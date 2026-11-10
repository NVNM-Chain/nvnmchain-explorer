//! Background indexer tuned for a sub-second chain.
//!
//! New heads arrive instantly via a WebSocket `newHeads` subscription (with a
//! polling fallback). Blocks are fetched in multi-block JSON-RPC HTTP batches
//! (receipts only when a block has transactions) and written by one serialized
//! SQLite writer. Tip catch-up is coalesced and windowed so the writer is not
//! left idle; backfill yields the RPC when the tip falls behind.

use std::collections::{HashMap, HashSet};
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

use crate::config::Settings;
use crate::contracts::RESERVED_TOKENS;
use crate::db::{self, Db};
use crate::decoder::{checksum_address, decode_event, DecodedEvent, TRANSFER_TOPIC};
use crate::models::{AnchoringEvent, BlockBundle, Transaction, TransferEvent};
use crate::parse::{parse_block, parse_transaction};
use crate::rpc::{decimal_amount, int_to_hex_str, parse_int_any, ChainRpc, RpcError};
use crate::summary::ZERO_ADDRESS;
use crate::tidx::Tidx;
use crate::tokens::{fetch_token_metadata, has_control_chars};

/// How many blocks share one JSON-RPC HTTP request. Sixteen empty blocks
/// (32 methods when receipts are included) measured at ~265ms against the
/// public RPC — one RTT for a range that used to be 16 separate connections.
const RPC_BLOCK_BATCH: u64 = 16;

/// How far the indexed tip may lag before backfill yields the RPC so catch-up
/// is not starved by history.
const TIP_YIELD_LAG: u64 = 16;

#[derive(Debug, Clone)]
pub struct IndexerConfig {
    pub poll: Duration,
    pub batch: u64,
    pub concurrency: usize,
    pub ws_url: String,
    pub index_ws: bool,
    pub stats_interval: Duration,
    pub tidx: Option<Tidx>,
}

impl IndexerConfig {
    pub fn from_settings(s: &Settings) -> Self {
        Self {
            poll: Duration::from_secs_f64(s.poll_seconds),
            batch: s.batch_size,
            concurrency: s.index_concurrency,
            ws_url: s.ws_url.clone(),
            index_ws: s.index_ws,
            stats_interval: Duration::from_secs_f64(s.stats_interval_seconds),
            tidx: s.tidx.clone(),
        }
    }
}

/// Fetch a block and everything attached to it: transactions, receipts when
/// the block has any, transfers, and fee-token metadata.
pub async fn fetch_block_bundle(rpc: &ChainRpc, block_num: u64) -> Result<Option<BlockBundle>> {
    let mut bundles = fetch_block_bundles(
        rpc,
        Arc::new(Mutex::new(HashSet::new())),
        block_num..=block_num,
    )
    .await?;
    Ok(bundles.pop())
}

/// Split `from..=to` into contiguous chunks of at most `batch` blocks.
fn block_chunks(from: u64, to: u64, batch: u64) -> impl Iterator<Item = RangeInclusive<u64>> {
    let batch = batch.max(1);
    std::iter::successors((from <= to).then_some(from), move |&start| {
        start.checked_add(batch).filter(|&next| next <= to)
    })
    .map(move |start| start..=start.saturating_add(batch - 1).min(to))
}

/// One non-null `eth_getBlockByNumber(full=true)` result plus optional
/// receipts. Token metadata is filled in later — this is pure CPU.
fn assemble_bundle(raw_block: &Value, receipts: Option<&Value>) -> BlockBundle {
    let block = parse_block(raw_block);
    let receipt_by_hash: HashMap<&str, &Value> = match receipts {
        Some(Value::Array(receipts)) => receipts
            .iter()
            .filter_map(|r| {
                r.get("transactionHash")
                    .and_then(Value::as_str)
                    .map(|h| (h, r))
            })
            .collect(),
        _ => HashMap::new(),
    };

    let raw_txs = raw_block
        .get("transactions")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let mut txs = Vec::with_capacity(raw_txs.len());
    let mut transfers = Vec::new();
    let mut anchoring = Vec::new();
    let mut next_log_index = 0u64;

    for tx_data in raw_txs {
        let tx_hash = tx_data.get("hash").and_then(Value::as_str).unwrap_or("");
        if tx_hash.is_empty() {
            continue;
        }
        let mut tx = parse_transaction(tx_data, &block);
        // Re-encode the RPC object with the official tempo primitives; the
        // block already carries the signed tx, so no extra per-tx RPC. Read in
        // place: a copy of every transaction object per block adds up.
        if let Ok(signed) = tempo_primitives::AASigned::deserialize(tx_data) {
            let mut buf = Vec::new();
            signed.eip2718_encode(&mut buf);
            tx.raw = Some(format!("0x{}", hex::encode(buf)));
        }
        if let Some(receipt) = receipt_by_hash.get(tx_hash) {
            apply_receipt(
                &mut tx,
                receipt,
                &mut transfers,
                &mut anchoring,
                &mut next_log_index,
            );
        }
        txs.push(tx);
    }

    BlockBundle {
        block,
        txs,
        transfers,
        anchoring,
        tokens: Vec::new(),
    }
}

/// Fetch `nums` in one HTTP request of `eth_getBlockByNumber`, then a second
/// request of `eth_getBlockReceipts` only for blocks that have transactions.
/// A range the node answers with one error, as reth does past its response size cap, is fetched
/// in halves; a request that fails outright is not retried.
async fn fetch_raw_blocks(
    rpc: &ChainRpc,
    nums: RangeInclusive<u64>,
) -> Result<Vec<(Value, Option<Value>)>> {
    if nums.is_empty() {
        return Ok(Vec::new());
    }
    let calls: Vec<(String, Value)> = nums
        .clone()
        .map(|n| {
            (
                "eth_getBlockByNumber".into(),
                json!([format!("0x{n:x}"), true]),
            )
        })
        .collect();
    let results = match rpc.batch_call(calls).await {
        Err(e) if e.is::<RpcError>() && nums.start() < nums.end() => {
            let (from, to) = nums.into_inner();
            let mid = from + (to - from) / 2;
            info!("blocks {from}..={to} refused ({e}); fetching in halves");
            let mut blocks = Box::pin(fetch_raw_blocks(rpc, from..=mid)).await?;
            blocks.extend(Box::pin(fetch_raw_blocks(rpc, mid + 1..=to)).await?);
            return Ok(blocks);
        }
        results => results?,
    };

    let mut blocks = Vec::with_capacity(results.len());
    let mut need_receipts = Vec::new();
    for (num, res) in nums.zip(results) {
        match res {
            Ok(v) if !v.is_null() => {
                if has_txs(&v) {
                    need_receipts.push((blocks.len(), num));
                }
                blocks.push((v, None));
            }
            Ok(_) => warn!("block {num} not found"),
            Err(e) => warn!("getBlock({num}) failed: {e}"),
        }
    }
    if need_receipts.is_empty() {
        return Ok(blocks);
    }

    let calls: Vec<(String, Value)> = need_receipts
        .iter()
        .map(|(_, n)| ("eth_getBlockReceipts".into(), json!([format!("0x{n:x}")])))
        .collect();
    match rpc.batch_call(calls).await {
        Ok(results) => {
            for ((idx, num), res) in need_receipts.iter().zip(results) {
                match res {
                    Ok(v) if !v.is_null() => blocks[*idx].1 = Some(v),
                    Ok(_) => warn!("getBlockReceipts({num}) answered null"),
                    Err(e) => warn!("getBlockReceipts({num}) failed: {e}"),
                }
            }
        }
        Err(e) => warn!("receipts batch failed: {e:#}"),
    }
    // A block short of its receipts would be written as if every transaction
    // succeeded and none paid a fee, and never revisited. One more try, a
    // receipt at a time for a node without eth_getBlockReceipts; a block still
    // short is left out, so the loops fetch it again.
    for (idx, num) in need_receipts {
        if blocks[idx].1.is_some() {
            continue;
        }
        let hashes = tx_hashes(&blocks[idx].0);
        let calls = hashes
            .iter()
            .map(|h| ("eth_getTransactionReceipt".into(), json!([h])))
            .collect();
        let receipts: Option<Vec<Value>> = match rpc.batch_call(calls).await {
            Ok(results) if results.len() == hashes.len() => results
                .into_iter()
                .map(|r| r.ok().filter(|v| !v.is_null()))
                .collect(),
            _ => None,
        };
        match receipts {
            Some(receipts) => blocks[idx].1 = Some(Value::Array(receipts)),
            None => warn!("receipts for block {num} unavailable; leaving it for the next fetch"),
        }
    }
    Ok(without_missing_receipts(blocks))
}

fn has_txs(block: &Value) -> bool {
    block
        .get("transactions")
        .and_then(Value::as_array)
        .is_some_and(|txs| !txs.is_empty())
}

fn tx_hashes(block: &Value) -> Vec<String> {
    block
        .get("transactions")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
        .iter()
        .filter_map(|t| t.get("hash").and_then(Value::as_str))
        .map(String::from)
        .collect()
}

/// The blocks that can be written: every block with transactions has its receipts.
fn without_missing_receipts(blocks: Vec<(Value, Option<Value>)>) -> Vec<(Value, Option<Value>)> {
    blocks
        .into_iter()
        .filter(|(block, receipts)| receipts.is_some() || !has_txs(block))
        .collect()
}

/// The bundles for `nums`; tokens in `known_tokens` are not described again.
pub async fn fetch_block_bundles(
    rpc: &ChainRpc,
    known_tokens: Arc<Mutex<HashSet<String>>>,
    nums: RangeInclusive<u64>,
) -> Result<Vec<BlockBundle>> {
    let mut bundles: Vec<BlockBundle> = fetch_raw_blocks(rpc, nums)
        .await?
        .iter()
        .map(|(raw, receipts)| assemble_bundle(raw, receipts.as_ref()))
        .collect();
    attach_token_metadata(rpc, &known_tokens, &mut bundles).await;
    Ok(bundles)
}

/// Tokens with no metadata row yet, and tokens a mint or burn here moved the supply of.
fn tokens_to_fetch(bundles: &[BlockBundle], known: &HashSet<String>) -> HashSet<String> {
    let mut out = HashSet::new();
    for bundle in bundles {
        out.extend(
            bundle_token_addrs(bundle)
                .filter(|addr| !known.contains(*addr))
                .map(String::from),
        );
        out.extend(
            bundle
                .transfers
                .iter()
                .filter(|t| t.from_addr == ZERO_ADDRESS || t.to_addr == ZERO_ADDRESS)
                .map(|t| t.token_addr.clone()),
        );
    }
    out
}

fn bundle_token_addrs(bundle: &BlockBundle) -> impl Iterator<Item = &str> {
    bundle
        .transfers
        .iter()
        .map(|t| t.token_addr.as_str())
        .chain(bundle.txs.iter().filter_map(|t| t.fee_token.as_deref()))
}

async fn attach_token_metadata(
    rpc: &ChainRpc,
    known_tokens: &Arc<Mutex<HashSet<String>>>,
    bundles: &mut [BlockBundle],
) {
    let wanted = tokens_to_fetch(
        bundles,
        &known_tokens.lock().unwrap_or_else(|e| e.into_inner()),
    );
    if wanted.is_empty() {
        return;
    }

    let metas: HashMap<_, _> = match fetch_token_metadata(rpc, wanted).await {
        Ok(metas) => metas.into_iter().map(|m| (m.address.clone(), m)).collect(),
        // Nothing stored and nothing remembered, so the next block that
        // mentions a token asks again.
        Err(e) => {
            warn!("token metadata unavailable: {e:#}");
            return;
        }
    };
    known_tokens
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .extend(metas.keys().cloned());

    for bundle in bundles.iter_mut() {
        // Deduped: one token backs every transfer of it in the block, and the
        // rows are upserted by address.
        let tokens: Vec<_> = bundle_token_addrs(bundle)
            .collect::<HashSet<_>>()
            .into_iter()
            .filter_map(|addr| metas.get(addr).cloned())
            .collect();
        bundle.tokens = tokens;
    }
}

/// Copy onto a transaction what only its receipt knows.
fn apply_receipt_fields(tx: &mut Transaction, receipt: &Value) {
    tx.receipt_data = Some(serde_json::to_string(receipt).unwrap_or_else(|_| "{}".into()));
    tx.status = receipt.get("status").map(parse_int_any).unwrap_or(1);
    // Some nodes fill receipt `to` with the first call's destination even when
    // the tx itself has no top-level `to` (tempo-style `calls` transactions).
    if tx.to_addr.is_none() {
        if let Some(to) = receipt.get("to").and_then(Value::as_str) {
            tx.to_addr = Some(to.to_string());
        }
    }
    tx.gas_used = receipt.get("gasUsed").map(parse_int_any).unwrap_or(0);
    if let Some(addr) = receipt.get("contractAddress").and_then(Value::as_str) {
        tx.contract_address = Some(checksum_address(addr));
    }
    if let Some(fee_token) = receipt.get("feeToken").and_then(Value::as_str) {
        tx.fee_token = Some(checksum_address(fee_token));
    }
    // A receipt that omits the fee — or reports it as neither a string nor a
    // number — leaves standing whatever the transaction object already said.
    if let Some(fee_amount) = receipt
        .get("feeAmount")
        .filter(|v| v.is_string() || v.is_number())
    {
        tx.fee_amount = decimal_amount(fee_amount);
    }
    if let Some(egp) = receipt.get("effectiveGasPrice") {
        tx.base_fee = int_to_hex_str(egp);
    }
}

/// The fee a receipt did not state, read off the transfer that settled it.
///
/// The fee is a TIP-20 transfer into the Fee Manager, so a receipt without a
/// `feeAmount` still says what was charged. Only the fee token's own transfer
/// counts: another token moving there is somebody's payment, not the fee.
fn derive_fee_from_transfer(tx: &mut Transaction, log: &Value, to: &str, amount: &str) {
    if amount.is_empty() || tx.fee_amount.parse::<i64>().map(|n| n > 0).unwrap_or(false) {
        return;
    }
    let emitter = log
        .get("address")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_lowercase();
    let is_fee_token = tx
        .fee_token
        .as_deref()
        .map(|f| f.to_lowercase() == emitter)
        .unwrap_or(false);
    if is_fee_token && to.eq_ignore_ascii_case(crate::decoder::FEE_MANAGER_ADDRESS) {
        tx.fee_amount = amount.to_string();
    }
}

/// Index one receipt log: an anchoring write lands in `anchoring`, a transfer in
/// `transfers`, and a transfer may also tell the transaction what it was charged.
fn index_log(
    tx: &mut Transaction,
    log: &Value,
    log_index: i64,
    transfers: &mut Vec<TransferEvent>,
    anchoring: &mut Vec<AnchoringEvent>,
) {
    let emitter = log.get("address").and_then(Value::as_str).unwrap_or("");
    let topic0 = log
        .pointer("/topics/0")
        .and_then(Value::as_str)
        .unwrap_or("");
    // Not `TransferWithMemo`: a memo transfer emits it beside its `Transfer`.
    let is_transfer = topic0.eq_ignore_ascii_case(TRANSFER_TOPIC.as_str());
    // Only the contract's own logs: any contract may emit under the same signature.
    let is_anchoring = emitter.eq_ignore_ascii_case(crate::anchoring::ADDRESS);
    if !is_transfer && !is_anchoring {
        return;
    }
    let Some(decoded) = decode_event(log) else {
        return;
    };
    if is_anchoring {
        anchoring.extend(anchoring_event(
            &decoded,
            &tx.hash,
            tx.block_number,
            log_index,
            tx.timestamp,
        ));
    }
    // Transfers, so the address and token transfer tabs have data.
    if !is_transfer {
        return;
    }
    let from = decoded.param("from").unwrap_or_default();
    let to = decoded.param("to").unwrap_or_default();
    let amount = decoded.param("amount").unwrap_or_default();
    derive_fee_from_transfer(tx, log, to, amount);
    // A log that does not carry all three is not a transfer, whatever its
    // topic0 says — a foreign contract may emit anything under it.
    if from.is_empty() || to.is_empty() || amount.is_empty() {
        return;
    }
    transfers.push(TransferEvent {
        id: 0,
        tx_hash: tx.hash.clone(),
        block_number: tx.block_number,
        log_index,
        token_addr: checksum_address(emitter),
        from_addr: from.to_string(),
        to_addr: to.to_string(),
        amount: amount.to_string(),
        timestamp: tx.timestamp,
        created_at: db::now_ts(),
    });
}

/// One anchoring row; `None` for a log that names no registry, whatever its topic0.
fn anchoring_event(
    decoded: &DecodedEvent,
    tx_hash: &str,
    block_number: i64,
    log_index: i64,
    timestamp: i64,
) -> Option<AnchoringEvent> {
    Some(AnchoringEvent {
        tx_hash: tx_hash.to_string(),
        block_number,
        log_index,
        timestamp,
        event: decoded.name.clone()?,
        registry_id: decoded.param("registryId")?.parse().ok()?,
        record_id: decoded
            .param("recordId")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
        caller: decoded.param("caller").unwrap_or_default().to_string(),
    })
}

fn apply_receipt(
    tx: &mut Transaction,
    receipt: &Value,
    transfers: &mut Vec<TransferEvent>,
    anchoring: &mut Vec<AnchoringEvent>,
    next_log_index: &mut u64,
) {
    apply_receipt_fields(tx, receipt);
    let logs = receipt
        .get("logs")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    for log in logs {
        // `logIndex` is the per-block unique half of the transfer_events key.
        // Prefer the node's value, but every log still advances the running
        // counter (undecodable logs occupy index slots too), so a missing or
        // unparsable index gets a unique fallback instead of colliding at 0.
        let log_index = log
            .get("logIndex")
            .map(parse_int_any)
            .filter(|n| *n >= 0)
            .unwrap_or(*next_log_index as i64);
        *next_log_index = (*next_log_index).max(log_index as u64 + 1);
        index_log(tx, log, log_index, transfers, anchoring);
    }
}

/// Fetch + persist one block (used by tests and simple callers).
pub async fn index_block(rpc: &ChainRpc, db: &Db, block_num: u64) -> Result<()> {
    let Some(bundle) = fetch_block_bundle(rpc, block_num).await? else {
        return Ok(());
    };
    db::save_block_bundle(db, &bundle)?;
    Ok(())
}

/// The height the anchoring backfill has read the node's logs up to.
const BACKFILL_KEY: &str = "anchoring_backfilled_to";

/// Blocks per `eth_getLogs`; halved while the node refuses the range.
const WINDOW: u64 = 50_000;

/// Fill `anchoring_events` for blocks indexed before the table existed, from the
/// node's logs rather than the blocks they sit in. Rows are keyed by (block, log
/// index), so overlap with the block path is free, and the watermark lets a
/// stopped run resume at its last window.
pub async fn backfill_anchoring(rpc: &ChainRpc, db: &Db) -> Result<()> {
    let done: u64 = db::get_kv(db, BACKFILL_KEY)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let head = rpc.eth_block_number().await.context("head for backfill")?;
    if done >= head {
        return Ok(());
    }
    info!("backfilling anchoring events from {done} to {head}");
    let (mut from, mut wrote, mut window) = (done, 0usize, WINDOW);
    let mut backoff = Duration::from_secs(1);
    while from <= head {
        let to = (from + window - 1).min(head);
        let logs = match rpc
            .eth_get_logs(json!({
                "address": crate::anchoring::ADDRESS,
                "fromBlock": format!("0x{from:x}"),
                "toBlock": format!("0x{to:x}"),
            }))
            .await
        {
            Ok(logs) => logs,
            // The node answered with an error: almost always its range cap.
            Err(e) if e.is::<RpcError>() => {
                if window == 1 {
                    return Err(e).with_context(|| format!("anchoring logs {from}..{to}"));
                }
                window /= 2;
                warn!("anchoring logs {from}..{to} refused ({e}); window now {window}");
                continue;
            }
            // The node did not answer: wait and retry the same range.
            Err(e) => {
                warn!("anchoring logs {from}..{to} failed ({e:#}); retrying in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        backoff = Duration::from_secs(1);
        // One transaction per window, watermark included, holding the shared
        // connection only to write.
        {
            let mut conn = db::lock(db);
            let txn = conn.transaction()?;
            for log in &logs {
                if let Some(event) = anchoring_event_from_log(&txn, log) {
                    wrote += usize::from(db::insert_anchoring(&txn, &event)?);
                }
            }
            db::set_kv(&txn, BACKFILL_KEY, &to.to_string())?;
            txn.commit()?;
        }
        from = to + 1;
        // Leave the node to the indexer between windows.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    info!("backfilled {wrote} anchoring event(s) to block {head}");
    Ok(())
}

/// One row from a raw log, stamped from the block the indexer already holds. A
/// block not indexed yet brings its own rows when it is.
fn anchoring_event_from_log(conn: &Connection, log: &Value) -> Option<AnchoringEvent> {
    let block_number = parse_int_any(log.get("blockNumber")?);
    let timestamp = db::get_block_timestamp(conn, block_number)?;
    let decoded = decode_event(log)?;
    anchoring_event(
        &decoded,
        decoded.transaction_hash.as_deref()?,
        block_number,
        parse_int_any(log.get("logIndex")?),
        timestamp,
    )
}

/// Re-fetch rows the database cannot be trusted for: a name or symbol carrying
/// control characters, from the pre-fix ABI string decoder, and the reserved
/// tokens, which an older build named from a table of its own. Run at startup
/// so a deployed database self-heals without manual intervention.
async fn repair_token_metadata(rpc: &ChainRpc, db: &Db) -> Result<()> {
    let stale: Vec<String> = db::get_all_token_metas(db)
        .into_iter()
        .filter(|m| {
            RESERVED_TOKENS.contains(&m.address.as_str())
                || has_control_chars(&m.name)
                || has_control_chars(&m.symbol)
        })
        .map(|m| m.address)
        .collect();
    if stale.is_empty() {
        return Ok(());
    }
    info!("repairing {} token metadata row(s)", stale.len());
    let (empty, repaired): (Vec<_>, Vec<_>) = fetch_token_metadata(rpc, &stale)
        .await?
        .into_iter()
        .partition(|meta| meta.name.is_empty() && meta.symbol.is_empty());
    // A token that answers to neither view has nothing to repair the row with;
    // leave it for the next start rather than blanking it.
    for meta in empty {
        warn!("nothing to repair {} with; leaving the row", meta.address)
    }
    db::save_token_metadata(db, &repaired)
}

/// Bring a database written by an older build back in line with what the read
/// paths assume. Every step is idempotent, so this runs on every start.
///
/// Each recompute is guarded rather than run unconditionally, and the lock is
/// taken per step: a rebuild reads whole tables, and holding the connection for
/// that long is long enough for page views to notice.
fn repair_derived_tables(db: &Db) {
    let (has_transfers, has_balances) = {
        let conn = db::lock(db);
        (
            db::table_has_rows(&conn, "transfer_events"),
            db::table_has_rows(&conn, "token_balances"),
        )
    };
    if has_transfers && !has_balances {
        // Transfers on record but no incremental balances: a database from
        // before they were maintained per block. Rebuild once, and holder
        // counts and holdings are correct from here on.
        let conn = db::lock(db);
        if let Err(e) = db::rebuild_token_balances(&conn) {
            warn!("token balance rebuild failed: {e:#}");
        }
    } else if has_balances {
        // Holder counts written before the BLOB-key fix are stale; recounting
        // them walks the balances' primary key and nothing else.
        let conn = db::lock(db);
        if let Err(e) = db::sync_holder_counts(&conn) {
            warn!("holder count sync failed: {e:#}");
        }
    }
}

/// Sleep for `dur` unless shutdown was requested, in which case return
/// immediately with `true`. Lets background loops stop promptly on Ctrl+C.
async fn sleep_or_shutdown(shutdown: &mut watch::Receiver<bool>, dur: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(dur) => false,
        r = shutdown.changed() => r.is_err() || *shutdown.borrow(),
    }
}

/// Shared state for the forward and backfill loops.
#[derive(Clone)]
struct Indexer {
    rpc: ChainRpc,
    db: Db,
    cfg: IndexerConfig,
    known_tokens: Arc<Mutex<HashSet<String>>>,
    bundle_tx: mpsc::Sender<BlockBundle>,
    shutdown: watch::Receiver<bool>,
    tip_lag: Arc<AtomicU64>,
    /// The newest head the node announced; the stats recompute stores it.
    chain_head: Arc<AtomicU64>,
}

impl Indexer {
    /// Fetch `from..=to` as multi-block RPC batches, returning bundles in
    /// number order so the caller can emit a gapless live feed.
    async fn fetch_range(&self, from: u64, to: u64) -> Vec<BlockBundle> {
        if from > to {
            return Vec::new();
        }
        let inflight = (self.cfg.concurrency.max(1) as u64).div_ceil(RPC_BLOCK_BATCH) as usize;
        let mut chunks = block_chunks(from, to, RPC_BLOCK_BATCH);
        let mut set = tokio::task::JoinSet::new();
        let mut out = Vec::with_capacity((to - from + 1).min(16384) as usize);
        loop {
            if *self.shutdown.borrow() {
                set.abort_all();
                break;
            }
            while set.len() < inflight {
                let Some(chunk) = chunks.next() else {
                    break;
                };
                let rpc = self.rpc.clone();
                let known = self.known_tokens.clone();
                set.spawn(async move { fetch_block_bundles(&rpc, known, chunk).await });
            }
            if set.is_empty() {
                break;
            }
            match set.join_next().await {
                Some(Ok(Ok(bundles))) => out.extend(bundles),
                Some(Ok(Err(e))) => warn!("index range fetch failed: {e:#}"),
                Some(Err(e)) => warn!("index range task failed: {e:#}"),
                None => break,
            }
        }
        out.sort_by_key(|b| b.block.number);
        out
    }

    async fn send_bundles(&self, bundles: impl IntoIterator<Item = BlockBundle>) -> bool {
        for b in bundles {
            if self.bundle_tx.send(b).await.is_err() {
                warn!("writer stopped");
                return false;
            }
        }
        true
    }
}

/// The bundles that run on from `from` with no hole, in order. Sorted input.
fn contiguous_from(bundles: Vec<BlockBundle>, from: u64) -> Vec<BlockBundle> {
    let mut expected = from;
    bundles
        .into_iter()
        .take_while(|b| {
            let hit = b.block.number as u64 == expected;
            expected += 1;
            hit
        })
        .collect()
}

/// The bundles that run down from `to` with no hole, still in ascending order. Sorted input.
fn contiguous_down_to(bundles: Vec<BlockBundle>, to: u64) -> Vec<BlockBundle> {
    let mut expected = to;
    let mut kept: Vec<BlockBundle> = bundles
        .into_iter()
        .rev()
        .take_while(|b| {
            let hit = b.block.number as u64 == expected;
            expected = expected.wrapping_sub(1);
            hit
        })
        .collect();
    kept.reverse();
    kept
}

/// Collapse every queued head into the highest one seen. The watcher polls and
/// subscribes concurrently, so a slow poll can deliver a stale head after a
/// newer one — taking the last would walk the tip backwards.
fn drain_heads(rx: &mut mpsc::Receiver<u64>, mut head: u64) -> u64 {
    while let Ok(n) = rx.try_recv() {
        head = head.max(n);
    }
    head
}

/// Fresh DB: wait for backfill to seed the head so this loop never tries to
/// index the whole history, and so the two loops don't fetch the same blocks.
async fn wait_for_seed(db: &Db) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(b) = db::get_latest_block(db) {
            return b.number as u64;
        }
        if tokio::time::Instant::now() >= deadline {
            return 0;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn forward_loop(mut ix: Indexer) {
    let (head_tx, mut head_rx) = mpsc::channel::<u64>(256);
    tokio::spawn(crate::ws::head_watcher(
        ix.rpc.clone(),
        ix.cfg.ws_url.clone(),
        ix.cfg.index_ws,
        ix.cfg.poll,
        head_tx,
        ix.shutdown.clone(),
    ));

    let mut sent = db::get_latest_block(&ix.db)
        .map(|b| b.number as u64)
        .unwrap_or(0);
    info!("forward loop started from block {sent}");

    while let Some(head) = head_rx.recv().await {
        if *ix.shutdown.borrow() {
            break;
        }
        // A fast head feed delivers every block; fold queued heads into one
        // range so we fetch a batch instead of one HTTP request per block.
        let mut head = drain_heads(&mut head_rx, head);
        ix.chain_head.store(head, Ordering::Relaxed);

        if sent == 0 && head > 0 {
            sent = wait_for_seed(&ix.db).await;
            if sent == 0 || sent >= head {
                continue;
            }
        }
        if head > sent + 1 {
            info!("new blocks: {sent} -> {head} (+{})", head - sent);
        }
        // Window the catch-up so the writer starts before a large gap has
        // finished fetching, and so memory stays bounded.
        while sent < head {
            if *ix.shutdown.borrow() {
                return;
            }
            head = drain_heads(&mut head_rx, head);
            ix.chain_head.store(head, Ordering::Relaxed);
            let end = sent.saturating_add(ix.cfg.batch).min(head);
            ix.tip_lag
                .store(head.saturating_sub(sent), Ordering::Relaxed);
            // Cut at the first hole: this loop never comes back for one.
            let bundles = contiguous_from(ix.fetch_range(sent + 1, end).await, sent + 1);
            // Nothing past `sent` came back: the fetch failed, or the node has
            // not caught up with the head it announced. Ask again after a
            // moment; stepping over the block would leave a hole for good.
            let Some(reached) = bundles.last().map(|b| b.block.number as u64) else {
                warn!("blocks {}..={end} not fetched; retrying", sent + 1);
                if sleep_or_shutdown(&mut ix.shutdown, ix.cfg.poll).await {
                    return;
                }
                continue;
            };
            if !ix.send_bundles(bundles).await {
                return;
            }
            sent = reached;
        }
        ix.tip_lag.store(0, Ordering::Relaxed);
    }
    warn!("forward loop stopped (head feed closed)");
}

async fn backfill_loop(mut ix: Indexer) {
    info!(
        "backfill loop started (batch {}, concurrency {})",
        ix.cfg.batch, ix.cfg.concurrency
    );

    // In-memory descending frontier: advancing this without waiting for the
    // writer lets the next fetch overlap the previous commit. Writes are
    // idempotent, so a crash just re-fetches the last in-flight batch.
    let mut frontier: Option<u64> = db::get_min_block_number(&ix.db).map(|n| n as u64);
    let mut consecutive_failures = 0u32;
    loop {
        if *ix.shutdown.borrow() {
            break;
        }
        if ix.tip_lag.load(Ordering::Relaxed) > TIP_YIELD_LAG {
            if sleep_or_shutdown(&mut ix.shutdown, Duration::from_millis(50)).await {
                break;
            }
            continue;
        }
        let target = match frontier {
            Some(min) if min > 1 => min,
            Some(_) => {
                if sleep_or_shutdown(&mut ix.shutdown, ix.cfg.poll).await {
                    break;
                }
                frontier = db::get_min_block_number(&ix.db).map(|n| n as u64);
                continue;
            }
            None => match ix.rpc.eth_block_number().await {
                Ok(head) => head + 1,
                Err(e) => {
                    warn!("backfill head fetch failed: {e:#}");
                    if sleep_or_shutdown(&mut ix.shutdown, ix.cfg.poll).await {
                        break;
                    }
                    continue;
                }
            },
        };
        let start = target.saturating_sub(ix.cfg.batch).max(1);
        // Cut at the first hole below the target: the frontier only descends,
        // so a block left above it would never be fetched again.
        let bundles = contiguous_down_to(ix.fetch_range(start, target - 1).await, target - 1);
        if bundles.is_empty() {
            consecutive_failures += 1;
            let backoff = Duration::from_millis(200 * consecutive_failures.min(10) as u64);
            if sleep_or_shutdown(&mut ix.shutdown, backoff).await {
                break;
            }
            continue;
        }
        let reached = bundles[0].block.number as u64;
        // Tip-first so the head lands in the DB as soon as possible (the
        // forward loop waits for it on a fresh database).
        if !ix.send_bundles(bundles.into_iter().rev()).await {
            return;
        }
        frontier = Some(reached);
        consecutive_failures = 0;
        info!("backfill: {} remaining", reached.saturating_sub(1));
    }
}

/// How long the writer waits before trying a failed write again.
const WRITE_RETRY: Duration = Duration::from_secs(5);

/// Persist what the fetch loops send, retrying a failed write: the loops never
/// come back for a batch.
async fn write_loop(
    db: Db,
    mut bundle_rx: mpsc::Receiver<BlockBundle>,
    index_transfers: bool,
    mut shutdown: watch::Receiver<bool>,
) {
    // Bundles one commit may carry. A writer that keeps up sees an empty queue
    // and commits per block; batching engages only once it falls behind.
    const MAX_BATCH: usize = 64;

    while let Some(bundle) = bundle_rx.recv().await {
        // On shutdown, stop draining the queue: in-flight bundles are
        // dropped and re-fetched on the next start.
        if *shutdown.borrow() {
            break;
        }
        // Fold in whatever is already queued; `try_recv` never waits, so
        // this adds no latency.
        let mut batch = vec![bundle];
        while batch.len() < MAX_BATCH {
            let Ok(next) = bundle_rx.try_recv() else {
                break;
            };
            batch.push(next);
        }
        if !index_transfers {
            for bundle in &mut batch {
                bundle.transfers.clear();
            }
        }
        while let Err(e) = db::save_block_bundles(&db, &batch) {
            let (first, last) = (batch[0].block.number, batch[batch.len() - 1].block.number);
            tracing::error!("blocks {first}..={last} not written, retrying: {e:#}");
            if sleep_or_shutdown(&mut shutdown, WRITE_RETRY).await {
                return;
            }
        }
    }
}

/// Run the indexer: one serialized DB writer plus forward and backfill loops.
pub async fn run_forever(
    rpc: ChainRpc,
    db: Db,
    cfg: IndexerConfig,
    shutdown: watch::Receiver<bool>,
) {
    let stats_interval = cfg.stats_interval;
    let chain_head = Arc::new(AtomicU64::new(0));
    // A tidx keeps the transfers and balances; see `tidx`.
    let own_transfers = cfg.tidx.is_none();
    tokio::spawn(stats_loop(
        db.clone(),
        stats_interval,
        chain_head.clone(),
        shutdown.clone(),
    ));
    if let Some(tidx) = cfg.tidx.clone() {
        tokio::spawn(holder_counts_loop(db.clone(), tidx, shutdown.clone()));
    }

    if own_transfers {
        tokio::spawn(genesis_loop(
            rpc.clone(),
            db.clone(),
            stats_interval,
            shutdown.clone(),
        ));
    }

    // Seed the token-metadata cache and repair balances on legacy databases.
    let known_tokens = Arc::new(Mutex::new(
        db::get_all_token_addresses(&db).into_iter().collect(),
    ));
    // Re-fetch token metadata that was stored by the pre-fix ABI string
    // decoder (its name/symbol are NUL-padded control-char garbage). Cheap
    // and best-effort: only rows with control characters are refetched.
    let repair_rpc = rpc.clone();
    let repair_db = db.clone();
    tokio::spawn(async move {
        if let Err(e) = repair_token_metadata(&repair_rpc, &repair_db).await {
            warn!("token metadata repair failed: {e:#}");
        }
    });
    if own_transfers {
        let rebuild_db = db.clone();
        tokio::spawn(async move { repair_derived_tables(&rebuild_db) });
    }
    // Minutes of work on a large database, so on a thread of its own.
    let pack_db = db.clone();
    tokio::task::spawn_blocking(move || {
        if let Err(e) = db::pack_legacy_transactions(&pack_db) {
            warn!("packing transaction rows failed: {e:#}");
        }
    });
    // Anchoring rows for blocks indexed before the table existed.
    let anchoring_rpc = rpc.clone();
    let anchoring_db = db.clone();
    tokio::spawn(async move {
        if let Err(e) = backfill_anchoring(&anchoring_rpc, &anchoring_db).await {
            warn!("anchoring backfill failed: {e:#}");
        }
    });

    let (bundle_tx, bundle_rx) = mpsc::channel::<BlockBundle>(1024);
    let writer = tokio::spawn(write_loop(
        db.clone(),
        bundle_rx,
        own_transfers,
        shutdown.clone(),
    ));

    let ix = Indexer {
        rpc,
        db,
        cfg,
        known_tokens,
        bundle_tx: bundle_tx.clone(),
        shutdown,
        tip_lag: Arc::new(AtomicU64::new(0)),
        chain_head,
    };
    let forward = tokio::spawn(forward_loop(ix.clone()));
    let backfill = tokio::spawn(backfill_loop(ix));
    drop(bundle_tx);

    tokio::select! {
        r = forward => { if let Err(e) = r { warn!("forward loop ended: {e:#}"); } }
        r = backfill => { if let Err(e) = r { warn!("backfill loop ended: {e:#}"); } }
    }
    let _ = writer.await;
}

// ---------------------------------------------------------------------------
// Precomputed network stats
// ---------------------------------------------------------------------------

/// Add each seen holder's balance at block 0, once: no Transfer carries what
/// genesis gave it, so a funded holder reads negative from its first send.
async fn genesis_loop(
    rpc: ChainRpc,
    db: Db,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if let Err(e) = add_genesis_balances(&rpc, &db).await {
            warn!("genesis balances: {e:#}");
        }
        if sleep_or_shutdown(&mut shutdown, interval).await {
            break;
        }
    }
}

/// The transfers indexed since the last pass, a page at a time.
async fn add_genesis_balances(rpc: &ChainRpc, db: &Db) -> Result<()> {
    while let Some((cursor, holders)) = db::holders_without_genesis_balance(db, 1000)? {
        let balances = crate::tokens::balances_at_genesis(rpc, &holders).await?;
        let rows: Vec<_> = holders.into_iter().zip(balances).collect();
        db::save_genesis_balances(db, &rows, cursor)?;
    }
    Ok(())
}

/// Each token's holder count as the tidx last counted it, written onto its
/// metadata row. Its snapshots refresh every quarter hour, so a minute's lag
/// shows nothing stale.
async fn holder_counts_loop(db: Db, tidx: Tidx, mut shutdown: watch::Receiver<bool>) {
    loop {
        let counts = tidx.clone().token_holder_counts().await;
        if let Err(e) = counts.and_then(|counts| db::set_holder_counts(&db, &counts)) {
            warn!("holder counts from tidx: {e:#}");
        }
        if sleep_or_shutdown(&mut shutdown, Duration::from_secs(60)).await {
            break;
        }
    }
}

/// Recompute the home-page stats blob into the `kv` table every interval so
/// the web layer never has to scan history at request time.
async fn stats_loop(
    db: Db,
    interval: Duration,
    chain_head: Arc<AtomicU64>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            break;
        }
        if let Err(e) = compute_and_store_stats(&db, chain_head.load(Ordering::Relaxed)) {
            warn!("stats recompute failed: {e:#}");
        }
        if sleep_or_shutdown(&mut shutdown, interval).await {
            break;
        }
    }
}

/// Minutes of activity the home page charts and averages.
const CHART_MINUTES: usize = 60;

fn compute_and_store_stats(db: &Db, head: u64) -> Result<Value> {
    let conn = db::lock(db);
    let now = db::now_ts();

    // The counters the writer keeps, and a sum over the blocks of the last day:
    // nothing here walks the transactions table.
    let total_blocks = db::counter(&conn, "blocks");
    let total_txns = db::counter(&conn, "transactions");
    let token_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM token_metadata", [], |r| r.get(0))?;
    let txns_24h: i64 = conn.query_row(
        "SELECT COALESCE(SUM(tx_count), 0) FROM blocks WHERE timestamp >= ?1",
        params![now - 86400],
        |r| r.get(0),
    )?;
    let blocks_24h: i64 = conn.query_row(
        "SELECT COUNT(*) FROM blocks WHERE timestamp >= ?1",
        params![now - 86400],
        |r| r.get(0),
    )?;

    let latest_block = conn.query_row("SELECT MAX(number) FROM blocks", [], |r| {
        r.get::<_, Option<i64>>(0)
    })?;

    // Index progress, so the home page and the stream never recount blocks.
    // Until the first head arrives, the newest indexed block stands in.
    let chain_head = (head as i64).max(latest_block.unwrap_or(0));
    let index_pct = if chain_head > 0 {
        (total_blocks as f64 / chain_head as f64 * 100.0).clamp(0.0, 100.0)
    } else {
        0.0
    };

    // Each minute of the hour before the newest block, oldest first: the charts
    // plot it and the tiles average it. A lagging index still has data.
    let mut tx_per_minute = vec![0_i64; CHART_MINUTES];
    let mut gas_per_minute = vec![0_i64; CHART_MINUTES];
    let (mut blocks, mut gas_limit, mut first_ms, mut last_ms) = (0, 0, i64::MAX, 0);
    let mut minutes = conn.prepare(
        "SELECT (timestamp - start - 1) / 60, SUM(tx_count), SUM(gas_used),
                COUNT(*), SUM(gas_limit), MIN(timestamp_ms), MAX(timestamp_ms)
         FROM blocks, (SELECT MAX(timestamp) - ?1 AS start FROM blocks)
         WHERE timestamp > start GROUP BY 1",
    )?;
    let mut rows = minutes.query(params![60 * CHART_MINUTES as i64])?;
    while let Some(r) = rows.next()? {
        let minute: usize = r.get(0)?;
        tx_per_minute[minute] = r.get(1)?;
        gas_per_minute[minute] = r.get(2)?;
        blocks += r.get::<_, i64>(3)?;
        gas_limit += r.get::<_, i64>(4)?;
        first_ms = first_ms.min(r.get(5)?);
        last_ms = last_ms.max(r.get(6)?);
    }
    // Millisecond timestamps, since sub-second blocks share a whole second.
    let span_ms = (last_ms - first_ms).max(0) as f64;
    let ratio = |n: f64, d: f64| if d > 0.0 { n / d } else { 0.0 };
    let avg_block_time_ms = ratio(span_ms, (blocks - 1) as f64);
    let tps = ratio(tx_per_minute.iter().sum::<i64>() as f64 * 1000.0, span_ms);
    let gas_util_pct = ratio(
        gas_per_minute.iter().sum::<i64>() as f64 * 100.0,
        gas_limit as f64,
    );

    let stats = serde_json::json!({
        "latest_block": latest_block,
        "total_blocks": total_blocks,
        "total_txns": total_txns,
        "token_count": token_count,
        "txns_24h": txns_24h,
        "blocks_24h": blocks_24h,
        "avg_block_time_ms": avg_block_time_ms,
        "tps": tps,
        "gas_util_pct": gas_util_pct,
        "chain_head": chain_head,
        "index_pct": index_pct,
        "tx_per_minute": tx_per_minute,
        "gas_per_minute": gas_per_minute,
        "updated_at": now,
    });
    // Still written to kv: it seeds the in-memory copy across a restart.
    db::set_kv(&conn, "stats", &stats.to_string())?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoder::TRANSFER_TOPIC;

    /// Each block counts in the minute it falls in, counted back from the newest.
    #[test]
    fn the_hour_before_the_newest_block_is_charted_and_averaged() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stats.db");
        let db: Db = Arc::new(Mutex::new(db::init_db(path.to_str().unwrap()).unwrap()));
        let end = 1_700_000_000;
        let blocks = [
            (1, end - 3600, 9),
            (2, end - 3599, 1),
            (3, end - 61, 2),
            (4, end - 30, 3),
            (5, end, 4),
        ];
        for (number, timestamp, txs) in blocks {
            db::lock(&db)
                .execute(
                    "INSERT INTO blocks (number, hash, parent_hash, timestamp, timestamp_ms,
                                         tx_count, gas_used, gas_limit)
                     VALUES (?1, ?1, ?1, ?2, ?2 * 1000, ?3, ?3 * 10, 1000)",
                    params![number, timestamp, txs],
                )
                .unwrap();
        }

        let stats = compute_and_store_stats(&db, 0).unwrap();
        let txs: Vec<i64> = serde_json::from_value(stats["tx_per_minute"].clone()).unwrap();
        assert_eq!(txs.len(), CHART_MINUTES);
        // The block an hour before the newest is outside the hour.
        assert_eq!((txs[0], txs[58], txs[59]), (1, 2, 7));
        assert_eq!(txs.iter().sum::<i64>(), 10);
        assert_eq!(stats["gas_per_minute"][59], 70);

        // The tiles average the same hour: 10 transactions and 100 of 4,000 gas
        // over the 3,599 s from the first block in it to the newest.
        let close = |k: &str, want: f64| (stats[k].as_f64().unwrap() - want).abs() < 1e-9;
        assert!(close("tps", 10.0 / 3599.0), "{stats}");
        assert!(close("gas_util_pct", 2.5));
        assert!(close("avg_block_time_ms", 3_599_000.0 / 3.0));
    }

    #[tokio::test]
    async fn a_refused_range_is_fetched_in_halves() {
        use axum::{routing::post, Json, Router};

        // Like reth past its response size cap: one error, not an array.
        let handler = |Json(body): Json<Value>| async move {
            let answer = |c: &Value| {
                let block = json!({"number": c["params"][0], "transactions": []});
                json!({"jsonrpc": "2.0", "id": c["id"], "result": block})
            };
            Json(match &body {
                Value::Array(calls) if calls.len() > 2 => {
                    json!({"jsonrpc": "2.0", "id": null, "error": {
                        "code": -32011, "message": "The batch response was too large",
                    }})
                }
                Value::Array(calls) => Value::Array(calls.iter().map(answer).collect()),
                call => answer(call),
            })
        };
        let rpc = ChainRpc::test_node(Router::new().route("/", post(handler))).await;

        let blocks = fetch_raw_blocks(&rpc, 1..=7).await.unwrap();
        let numbers: Vec<_> = blocks
            .iter()
            .map(|(b, _)| b["number"].as_str().unwrap())
            .collect();
        assert_eq!(numbers, ["0x1", "0x2", "0x3", "0x4", "0x5", "0x6", "0x7"]);
    }

    /// Halving a request the node did not answer would multiply the load on a node that is down.
    #[tokio::test]
    async fn a_failed_request_is_not_retried_in_halves() {
        use axum::{http::StatusCode, routing::post, Router};
        use std::sync::atomic::AtomicUsize;

        let hits = Arc::new(AtomicUsize::new(0));
        let counted = hits.clone();
        let handler = move || {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                StatusCode::BAD_GATEWAY
            }
        };
        let rpc = ChainRpc::test_node(Router::new().route("/", post(handler))).await;

        assert!(fetch_raw_blocks(&rpc, 1..=7).await.is_err());
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    fn chunks(from: u64, to: u64, batch: u64) -> Vec<RangeInclusive<u64>> {
        block_chunks(from, to, batch).collect()
    }

    /// A mint or burn moves a known token's supply, so its row is read again.
    #[test]
    fn supply_is_reread_after_a_mint_or_burn() {
        let (minted, burned, moved) = (
            "0x20C0000000000000000000000000000000000001",
            "0x20C0000000000000000000000000000000000002",
            "0x20C0000000000000000000000000000000000003",
        );
        let holder = "0x1111111111111111111111111111111111111111";
        let transfer = |token: &str, from: &str, to: &str| TransferEvent {
            id: 0,
            tx_hash: String::new(),
            block_number: 1,
            log_index: 0,
            token_addr: token.into(),
            from_addr: from.into(),
            to_addr: to.into(),
            amount: "1".into(),
            timestamp: 0,
            created_at: 0,
        };
        let mut bundle = assemble_bundle(&json!({"number": "0x1"}), None);
        bundle.transfers = vec![
            transfer(minted, ZERO_ADDRESS, holder),
            transfer(burned, holder, ZERO_ADDRESS),
            transfer(moved, holder, "0x2222222222222222222222222222222222222222"),
        ];
        let known = [minted, burned, moved].map(String::from).into();

        let wanted = tokens_to_fetch(&[bundle], &known);
        assert_eq!(wanted, [minted, burned].map(String::from).into());
    }

    #[test]
    fn block_chunks_splits_range() {
        assert_eq!(chunks(1, 40, 16), vec![1..=16, 17..=32, 33..=40]);
        assert_eq!(chunks(7, 7, 16), vec![7..=7]);
        assert_eq!(chunks(1, 3, 1), vec![1..=1, 2..=2, 3..=3]);
        assert!(chunks(5, 4, 16).is_empty());
        // A zero batch would otherwise chunk forever.
        assert_eq!(chunks(1, 2, 0), vec![1..=1, 2..=2]);
        // The final chunk stops at `to` rather than overflowing past it.
        assert_eq!(
            chunks(u64::MAX - 1, u64::MAX, 16),
            vec![u64::MAX - 1..=u64::MAX]
        );
    }

    /// A full disk holds the batch; writing resumes once there is room.
    #[tokio::test(start_paused = true)]
    async fn a_failed_write_is_held_until_it_lands() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("full.db");
        let db: Db = Arc::new(Mutex::new(db::init_db(path.to_str().unwrap()).unwrap()));
        // Clamps to the current size: no page can be added.
        db::lock(&db)
            .pragma_update(None, "max_page_count", 1)
            .unwrap();

        let (tx, rx) = mpsc::channel(8);
        let (_stop, shutdown) = watch::channel(false);
        let writer = tokio::spawn(write_loop(db.clone(), rx, true, shutdown));
        for n in [1, 2] {
            let raw = json!({"number": format!("0x{n:x}"), "hash": format!("0x{n:064x}")});
            let mut bundle = assemble_bundle(&raw, None);
            bundle.block.extra_data = "0".repeat(1 << 16);
            tx.send(bundle).await.unwrap();
        }

        tokio::time::sleep(WRITE_RETRY * 3).await;
        assert_eq!(db::get_min_block_number(&db), None, "the disk is full");

        db::lock(&db)
            .pragma_update(None, "max_page_count", 1 << 30)
            .unwrap();
        drop(tx);
        writer.await.unwrap();
        assert!(db::get_block_by_number(&db, 1).is_some());
        assert!(db::get_block_by_number(&db, 2).is_some());
    }

    /// With a tidx, the writer keeps the block and skips its transfers.
    #[tokio::test]
    async fn with_a_tidx_transfers_are_not_written() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tidx.db");
        let db: Db = Arc::new(Mutex::new(db::init_db(path.to_str().unwrap()).unwrap()));
        let (tx, rx) = mpsc::channel(8);
        let (_stop, shutdown) = watch::channel(false);
        let writer = tokio::spawn(write_loop(db.clone(), rx, false, shutdown));
        let holder = "0x1111111111111111111111111111111111111111";
        let raw = json!({"number": "0x1", "hash": format!("0x{:064x}", 1)});
        let mut bundle = assemble_bundle(&raw, None);
        bundle.transfers = vec![TransferEvent {
            id: 0,
            tx_hash: String::new(),
            block_number: 1,
            log_index: 0,
            token_addr: "0x20C0000000000000000000000000000000000000".into(),
            from_addr: ZERO_ADDRESS.into(),
            to_addr: holder.into(),
            amount: "1".into(),
            timestamp: 0,
            created_at: 0,
        }];
        tx.send(bundle).await.unwrap();
        drop(tx);
        writer.await.unwrap();
        assert!(db::get_block_by_number(&db, 1).is_some());
        assert_eq!(db::get_address_transfer_count(&db, holder), 0);
    }

    /// A block that did not come back must not be stepped over: what the forward
    /// loop sends stops below it, what backfill sends stops above it.
    #[test]
    fn a_hole_cuts_the_run_on_either_side() {
        let fetched = |nums: &[u64]| -> Vec<BlockBundle> {
            nums.iter()
                .map(|n| assemble_bundle(&json!({"number": format!("0x{n:x}")}), None))
                .collect()
        };
        let numbers = |bundles: &[BlockBundle]| -> Vec<i64> {
            bundles.iter().map(|b| b.block.number).collect()
        };
        assert_eq!(
            numbers(&contiguous_from(fetched(&[10, 11, 13]), 10)),
            [10, 11]
        );
        assert!(contiguous_from(fetched(&[11, 12]), 10).is_empty());
        assert_eq!(
            numbers(&contiguous_down_to(fetched(&[10, 11, 13]), 13)),
            [13]
        );
        assert!(contiguous_down_to(fetched(&[10, 11]), 13).is_empty());
        assert_eq!(
            numbers(&contiguous_down_to(fetched(&[10, 11, 12, 13]), 13)),
            [10, 11, 12, 13]
        );
    }

    /// A block with transactions but no receipts is not a block to write.
    #[test]
    fn a_block_short_of_its_receipts_is_left_out() {
        let with = json!({"number": "0x1", "transactions": [{"hash": "0xab"}]});
        let empty = json!({"number": "0x2", "transactions": []});
        let kept = without_missing_receipts(vec![
            (with.clone(), None),
            (empty.clone(), None),
            (with.clone(), Some(json!([{}]))),
        ]);
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].0, empty);
        assert_eq!(kept[1].0, with);
        assert_eq!(tx_hashes(&with), ["0xab"]);
    }

    #[test]
    fn drain_heads_keeps_the_highest() {
        let (tx, mut rx) = mpsc::channel::<u64>(8);
        // The watcher polls and subscribes concurrently, so a slow poll can
        // deliver a stale head after a newer subscription one.
        for n in [104u64, 107, 105] {
            tx.try_send(n).expect("queue head");
        }
        assert_eq!(drain_heads(&mut rx, 103), 107);
        // Nothing queued leaves the caller's head untouched.
        assert_eq!(drain_heads(&mut rx, 107), 107);
    }

    #[test]
    fn assemble_empty_block_without_receipts() {
        let raw = json!({
            "number": "0x10",
            "hash": format!("0x{}", "ab".repeat(32)),
            "parentHash": format!("0x{}", "cd".repeat(32)),
            "timestamp": "0x64",
            "transactions": [],
        });
        let bundle = assemble_bundle(&raw, None);
        assert_eq!(bundle.block.number, 16);
        assert_eq!(bundle.block.tx_count, 0);
        assert!(bundle.txs.is_empty());
        assert!(bundle.transfers.is_empty());
    }

    #[test]
    fn assemble_decodes_transfer_receipt() {
        let from = format!("0x{}", "1a".repeat(20));
        let to = format!("0x{}", "2b".repeat(20));
        let token = format!("0x{}", "aa".repeat(20));
        let tx_hash = format!("0x{}", "ff".repeat(32));
        let raw = json!({
            "number": "0x10",
            "hash": format!("0x{}", "ab".repeat(32)),
            "parentHash": format!("0x{}", "cd".repeat(32)),
            "timestamp": "0x64",
            "transactions": [{
                "hash": tx_hash,
                "blockNumber": "0x10",
                "transactionIndex": "0x0",
                "from": from,
                "to": token,
                "input": "0x",
            }],
        });
        let receipts = json!([{
            "transactionHash": tx_hash,
            "status": "0x1",
            "gasUsed": "0x5208",
            "logs": [{
                "address": token,
                "logIndex": "0x0",
                "topics": [
                    TRANSFER_TOPIC.as_str(),
                    format!("0x{}{}", "00".repeat(12), from.trim_start_matches("0x")),
                    format!("0x{}{}", "00".repeat(12), to.trim_start_matches("0x")),
                ],
                "data": format!("0x{:064x}", 100u64),
            }],
        }]);
        let bundle = assemble_bundle(&raw, Some(&receipts));
        assert_eq!(bundle.txs.len(), 1);
        assert_eq!(bundle.txs[0].status, 1);
        assert_eq!(bundle.transfers.len(), 1);
        assert_eq!(bundle.transfers[0].from_addr, checksum_address(&from));
        assert_eq!(bundle.transfers[0].to_addr, checksum_address(&to));
        assert_eq!(bundle.transfers[0].amount, "100");
        assert_eq!(bundle.transfers[0].token_addr, checksum_address(&token));
    }

    #[test]
    fn a_memo_transfer_counts_once() {
        let tx_hash = format!("0x{}", "ff".repeat(32));
        let topic = |byte: &str| format!("0x{}{}", "00".repeat(12), byte.repeat(20));
        let log = |index: &str, topics: Vec<String>| {
            json!({
                "address": format!("0x{}", "aa".repeat(20)),
                "logIndex": index,
                "topics": topics,
                "data": format!("0x{:064x}", 100u64),
            })
        };
        let raw = json!({"number": "0x10", "transactions": [{"hash": tx_hash}]});
        let receipts = json!([{
            "transactionHash": tx_hash,
            "logs": [
                log("0x0", vec![TRANSFER_TOPIC.clone(), topic("1a"), topic("2b")]),
                log("0x1", vec![
                    crate::decoder::TRANSFER_WITH_MEMO_TOPIC.clone(),
                    topic("1a"),
                    topic("2b"),
                    format!("0x{}", "ee".repeat(32)),
                ]),
            ],
        }]);
        let bundle = assemble_bundle(&raw, Some(&receipts));
        assert_eq!(bundle.transfers.len(), 1);
    }
}
