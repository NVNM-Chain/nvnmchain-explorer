//! Chain-derived writes, all on the writer session.
//!
//! A batch of blocks is set-based: one statement per table rather than one
//! per row, at most 14 round trips per batch (BEGIN, 11 statements, the
//! `writer_seq` bump, COMMIT) against about 642 for the per-row path.
//! Statements with nothing to do are skipped.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use num_bigint::BigInt;
use sqlx::PgConnection;

use super::plan::{apply_deltas, BalanceChanges, BatchPlan};
use super::q::{exec, fetch_all, get, PgQuery};
use super::shared::{bigint, hex_blob, without_nul};
use super::writer::{Budget, TxFuture};
use super::{DbError, PgDb};
use crate::db::{now_ts, Holder};
use crate::models::{AnchoringEvent, Block, BlockBundle, Transaction, TransferEvent};
use crate::tokens::TokenMeta;

/// The per-block counter semantics of the per-row writer: a block already
/// stored is not counted again, nor are the transactions it already holds.
const PROBE: &str = "SELECT COALESCE(SUM(GREATEST(u.n - (SELECT COUNT(*) FROM transactions t WHERE t.block_number = u.num), 0)), 0)::int8,
            (SELECT COUNT(*) FROM blocks WHERE number = ANY($1))
     FROM UNNEST($1::int8[], $2::int8[]) AS u(num, n)";

const UPSERT_BLOCKS: &str = "INSERT INTO blocks (number, hash, parent_hash, timestamp, timestamp_ms, gas_used, gas_limit,
                         base_fee, size, extra_data, epoch, view, proposer, miner, tx_count, created_at)
     SELECT * FROM UNNEST($1::int8[], $2::bytea[], $3::bytea[], $4::int8[], $5::int8[], $6::int8[], $7::int8[],
                          $8::text[], $9::int8[], $10::text[], $11::int8[], $12::int8[], $13::bytea[],
                          $14::bytea[], $15::int8[], $16::int8[])
     ON CONFLICT (number) DO UPDATE SET
         hash = excluded.hash, parent_hash = excluded.parent_hash, timestamp = excluded.timestamp,
         timestamp_ms = excluded.timestamp_ms, gas_used = excluded.gas_used, gas_limit = excluded.gas_limit,
         base_fee = excluded.base_fee, size = excluded.size, extra_data = excluded.extra_data,
         epoch = excluded.epoch, view = excluded.view, proposer = excluded.proposer,
         miner = excluded.miner, tx_count = excluded.tx_count";

/// A rewrite that carries nothing for a blob keeps what is stored: the trace a
/// page cached, the raw bytes a failed decode left out.
const UPSERT_TXS: &str = "INSERT INTO transactions (hash, block_number, position, from_addr, to_addr, status, gas_used,
                               base_fee, contract_address, fee_token, fee_amount, input, raw,
                               trace_data, receipt_data, timestamp, created_at)
     SELECT * FROM UNNEST($1::bytea[], $2::int8[], $3::int8[], $4::bytea[], $5::bytea[], $6::int8[], $7::int8[],
                          $8::text[], $9::bytea[], $10::bytea[], $11::text[], $12::text[], $13::bytea[],
                          $14::text[], $15::text[], $16::int8[], $17::int8[])
     ON CONFLICT (hash) DO UPDATE SET
         block_number = excluded.block_number, position = excluded.position,
         from_addr = excluded.from_addr, to_addr = excluded.to_addr, status = excluded.status,
         gas_used = excluded.gas_used, base_fee = excluded.base_fee,
         contract_address = excluded.contract_address, fee_token = excluded.fee_token,
         fee_amount = excluded.fee_amount, input = excluded.input,
         raw = COALESCE(excluded.raw, transactions.raw),
         trace_data = COALESCE(excluded.trace_data, transactions.trace_data),
         receipt_data = COALESCE(excluded.receipt_data, transactions.receipt_data),
         timestamp = excluded.timestamp";

/// Qualified: a bare `n` is ambiguous on Postgres.
const BUMP_COUNTERS: &str =
    "INSERT INTO counters (name, n) VALUES ('blocks', $1), ('transactions', $2)
     ON CONFLICT (name) DO UPDATE SET n = counters.n + excluded.n";

/// `RETURNING` gives exactly the rows the per-row writer reports as new.
const INSERT_TRANSFERS: &str = "INSERT INTO transfer_events (tx_hash, block_number, log_index, token_addr, from_addr, to_addr,
                                  amount, timestamp, created_at)
     SELECT * FROM UNNEST($1::bytea[], $2::int8[], $3::int8[], $4::bytea[], $5::bytea[], $6::bytea[],
                          $7::text[], $8::int8[], $9::int8[])
     ON CONFLICT (block_number, log_index) DO NOTHING RETURNING block_number, log_index";

const INSERT_ANCHORING: &str =
    "INSERT INTO anchoring_events (tx_hash, block_number, log_index, timestamp, event, registry_id,
                                   record_id, caller)
     SELECT * FROM UNNEST($1::bytea[], $2::int8[], $3::int8[], $4::int8[], $5::text[], $6::int8[],
                          $7::int8[], $8::bytea[])
     ON CONFLICT (block_number, log_index) DO NOTHING RETURNING 1";

/// A new row counts the holders its balances already give it, as
/// `upsert_token_meta` does.
const UPSERT_TOKENS: &str = "INSERT INTO token_metadata (address, name, symbol, decimals, currency, total_supply, logo_uri,
                                 holder_count, created_at, updated_at)
     SELECT u.a, u.n, u.s, u.d, u.c, u.t, '',
            (SELECT COUNT(*) FROM token_balances b WHERE b.token_addr = u.x AND b.balance NOT LIKE '-%'), $8, $8
     FROM UNNEST($1::bytea[], $2::text[], $3::text[], $4::int8[], $5::text[], $6::text[], $7::text[])
          AS u(a, n, s, d, c, t, x)
     ON CONFLICT (address) DO UPDATE SET name = excluded.name, symbol = excluded.symbol,
         decimals = excluded.decimals, currency = excluded.currency,
         total_supply = excluded.total_supply, updated_at = excluded.updated_at";

const READ_BALANCES: &str = "SELECT b.token_addr, b.holder_addr, b.balance FROM token_balances b
     JOIN UNNEST($1::text[], $2::text[]) AS u(t, h) ON b.token_addr = u.t AND b.holder_addr = u.h";

const UPSERT_BALANCES: &str =
    "INSERT INTO token_balances (token_addr, holder_addr, balance, updated_at)
     SELECT u.t, u.h, u.b, $4 FROM UNNEST($1::text[], $2::text[], $3::text[]) AS u(t, h, b)
     ON CONFLICT (token_addr, holder_addr) DO UPDATE SET
         balance = excluded.balance, updated_at = excluded.updated_at";

const DELETE_BALANCES: &str =
    "DELETE FROM token_balances b USING UNNEST($1::text[], $2::text[]) AS u(t, h)
     WHERE b.token_addr = u.t AND b.holder_addr = u.h";

const BUMP_HOLDERS: &str =
    "UPDATE token_metadata m SET holder_count = m.holder_count + u.by, updated_at = $3
     FROM UNNEST($1::bytea[], $2::int8[]) AS u(addr, by) WHERE m.address = u.addr";

const SET_KV: &str = "INSERT INTO kv (key, value, updated_at) VALUES ($1, $2, $3)
     ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at";

/// One column of `rows`, for an `UNNEST` parameter.
fn col<R, T>(rows: &[R], f: impl Fn(&R) -> T) -> Vec<T> {
    rows.iter().map(f).collect()
}

fn blocks_query(blocks: &[Block]) -> PgQuery {
    sqlx::query(UPSERT_BLOCKS)
        .bind(col(blocks, |b| b.number))
        .bind(col(blocks, |b| hex_blob(&b.hash)))
        .bind(col(blocks, |b| hex_blob(&b.parent_hash)))
        .bind(col(blocks, |b| b.timestamp))
        .bind(col(blocks, |b| b.timestamp_ms))
        .bind(col(blocks, |b| b.gas_used))
        .bind(col(blocks, |b| b.gas_limit))
        .bind(col(blocks, |b| without_nul(&b.base_fee)))
        .bind(col(blocks, |b| b.size))
        .bind(col(blocks, |b| without_nul(&b.extra_data)))
        .bind(col(blocks, |b| b.epoch))
        .bind(col(blocks, |b| b.view))
        .bind(col(blocks, |b| hex_blob(&b.proposer)))
        .bind(col(blocks, |b| hex_blob(&b.miner)))
        .bind(col(blocks, |b| b.tx_count))
        .bind(col(blocks, |b| b.created_at))
}

fn txs_query(txs: &[Transaction]) -> PgQuery {
    sqlx::query(UPSERT_TXS)
        .bind(col(txs, |t| hex_blob(&t.hash)))
        .bind(col(txs, |t| t.block_number))
        .bind(col(txs, |t| t.position))
        .bind(col(txs, |t| hex_blob(&t.from_addr)))
        .bind(col(txs, |t| t.to_addr.as_deref().map(hex_blob)))
        .bind(col(txs, |t| t.status))
        .bind(col(txs, |t| t.gas_used))
        .bind(col(txs, |t| without_nul(&t.base_fee)))
        .bind(col(txs, |t| t.contract_address.as_deref().map(hex_blob)))
        .bind(col(txs, |t| t.fee_token.as_deref().map(hex_blob)))
        .bind(col(txs, |t| without_nul(&t.fee_amount)))
        .bind(col(txs, |t| without_nul(&t.input)))
        .bind(col(txs, |t| t.raw.as_deref().map(hex_blob)))
        .bind(col(txs, |t| t.trace_data.as_deref().map(without_nul)))
        .bind(col(txs, |t| t.receipt_data.as_deref().map(without_nul)))
        .bind(col(txs, |t| t.timestamp))
        .bind(col(txs, |t| t.created_at))
}

fn transfers_query(ts: &[TransferEvent]) -> PgQuery {
    sqlx::query(INSERT_TRANSFERS)
        .bind(col(ts, |t| hex_blob(&t.tx_hash)))
        .bind(col(ts, |t| t.block_number))
        .bind(col(ts, |t| t.log_index))
        .bind(col(ts, |t| hex_blob(&t.token_addr)))
        .bind(col(ts, |t| hex_blob(&t.from_addr)))
        .bind(col(ts, |t| hex_blob(&t.to_addr)))
        .bind(col(ts, |t| without_nul(&t.amount)))
        .bind(col(ts, |t| t.timestamp))
        .bind(col(ts, |t| t.created_at))
}

fn tokens_query(tokens: &[TokenMeta]) -> PgQuery {
    sqlx::query(UPSERT_TOKENS)
        .bind(col(tokens, |m| hex_blob(&m.address)))
        .bind(col(tokens, |m| without_nul(&m.name)))
        .bind(col(tokens, |m| without_nul(&m.symbol)))
        .bind(col(tokens, |m| m.decimals))
        .bind(col(tokens, |m| without_nul(&m.currency)))
        .bind(col(tokens, |m| without_nul(&m.total_supply)))
        .bind(col(tokens, |m| m.address.clone()))
        .bind(now_ts())
}

fn anchoring_query(events: &[AnchoringEvent]) -> PgQuery {
    sqlx::query(INSERT_ANCHORING)
        .bind(col(events, |e| hex_blob(&e.tx_hash)))
        .bind(col(events, |e| e.block_number))
        .bind(col(events, |e| e.log_index))
        .bind(col(events, |e| e.timestamp))
        .bind(col(events, |e| without_nul(&e.event)))
        .bind(col(events, |e| e.registry_id))
        .bind(col(events, |e| e.record_id))
        .bind(col(events, |e| hex_blob(&e.caller)))
}

fn kv_query(key: &str, value: &str) -> PgQuery {
    sqlx::query(SET_KV)
        .bind(key.to_string())
        .bind(value.to_string())
        .bind(now_ts())
}

/// The stored balances of `keys`.
async fn read_balances(
    c: &mut PgConnection,
    keys: Vec<(String, String)>,
) -> Result<HashMap<(String, String), String>, DbError> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let (tokens, holders): (Vec<String>, Vec<String>) = keys.into_iter().unzip();
    let rows = fetch_all(
        c,
        "balances",
        sqlx::query(READ_BALANCES).bind(tokens).bind(holders),
    )
    .await?;
    let mut old = HashMap::with_capacity(rows.len());
    for row in &rows {
        old.insert(
            (get::<String>(row, 0)?, get::<String>(row, 1)?),
            get::<String>(row, 2)?,
        );
    }
    Ok(old)
}

/// Apply net balance deltas to the stored balances, and write the result.
async fn write_balances(
    c: &mut PgConnection,
    net: HashMap<(String, String), BigInt>,
) -> Result<(), DbError> {
    let old = read_balances(c, net.keys().cloned().collect()).await?;
    store_changes(c, apply_deltas(net, &old)).await
}

async fn store_changes(c: &mut PgConnection, out: BalanceChanges) -> Result<(), DbError> {
    let now = now_ts();
    if !out.upserts.is_empty() {
        let u = &out.upserts;
        exec(
            c,
            "upsert_bal",
            sqlx::query(UPSERT_BALANCES)
                .bind(col(u, |r| r.0.clone()))
                .bind(col(u, |r| r.1.clone()))
                .bind(col(u, |r| r.2.clone()))
                .bind(now),
        )
        .await?;
    }
    if !out.deletes.is_empty() {
        let (t, h): (Vec<String>, Vec<String>) = out.deletes.into_iter().unzip();
        exec(
            c,
            "delete_bal",
            sqlx::query(DELETE_BALANCES).bind(t).bind(h),
        )
        .await?;
    }
    if !out.holders.is_empty() {
        let h = &out.holders;
        exec(
            c,
            "holders",
            sqlx::query(BUMP_HOLDERS)
                .bind(col(h, |r| hex_blob(&r.0)))
                .bind(col(h, |r| r.1))
                .bind(now),
        )
        .await?;
    }
    Ok(())
}

/// Run one statement in a transaction of its own on the writer, building the
/// query afresh for each attempt.
async fn write_one(
    p: &PgDb,
    what: &'static str,
    query: impl Fn() -> PgQuery + Send + Sync + 'static,
) -> Result<()> {
    Ok(p.writer()?
        .write(Budget::Batch, move |c| -> TxFuture<'_, ()> {
            let q = query();
            Box::pin(async move { exec(c, what, q).await.map(drop) })
        })
        .await?)
}

/// One batch of blocks in one transaction on the writer session.
pub(crate) async fn save_block_bundles(p: &PgDb, bundles: &[BlockBundle]) -> Result<()> {
    if bundles.is_empty() {
        return Ok(());
    }
    let plan = Arc::new(BatchPlan::new(bundles));
    Ok(p.writer()?
        .write(Budget::Batch, move |c| -> TxFuture<'_, ()> {
            let plan = plan.clone();
            Box::pin(async move { write_plan(c, &plan).await })
        })
        .await?)
}

async fn write_plan(c: &mut PgConnection, plan: &BatchPlan) -> Result<(), DbError> {
    let (nums, counts) = plan.tx_counts();
    let probe = fetch_all(
        c,
        "probe",
        sqlx::query(PROBE).bind(nums.clone()).bind(counts),
    )
    .await?;
    let (new_txs, stored): (i64, i64) = (get(&probe[0], 0)?, get(&probe[0], 1)?);
    exec(c, "blocks", blocks_query(&plan.blocks)).await?;
    if !plan.txs.is_empty() {
        exec(c, "txs", txs_query(&plan.txs)).await?;
    }
    exec(
        c,
        "counters",
        sqlx::query(BUMP_COUNTERS)
            .bind(plan.blocks.len() as i64 - stored)
            .bind(new_txs),
    )
    .await?;
    let mut fresh = HashSet::new();
    if !plan.transfers.is_empty() {
        for row in fetch_all(c, "transfers", transfers_query(&plan.transfers)).await? {
            fresh.insert((get::<i64>(&row, 0)?, get::<i64>(&row, 1)?));
        }
    }
    if !plan.anchoring.is_empty() {
        exec(c, "anchoring", anchoring_query(&plan.anchoring)).await?;
    }
    // Metadata before balances, so a new token's count is seeded first.
    if !plan.tokens.is_empty() {
        exec(c, "tokens", tokens_query(&plan.tokens)).await?;
    }
    write_balances(c, plan.net(&fresh)).await
}

pub(crate) async fn save_block(p: &PgDb, block: &Block) -> Result<()> {
    let block = block.clone();
    write_one(p, "save_block", move || {
        blocks_query(std::slice::from_ref(&block))
    })
    .await
}

pub(crate) async fn save_transaction(p: &PgDb, tx: &Transaction) -> Result<()> {
    let tx = tx.clone();
    write_one(p, "save_transaction", move || {
        txs_query(std::slice::from_ref(&tx))
    })
    .await
}

pub(crate) async fn save_token_metadata(p: &PgDb, meta: &TokenMeta) -> Result<()> {
    let meta = meta.clone();
    write_one(p, "save_token_metadata", move || {
        tokens_query(std::slice::from_ref(&meta))
    })
    .await
}

/// The indexer's view of the chain head, for the progress bar.
pub(crate) async fn set_chain_head(p: &PgDb, head: i64) {
    if let Err(e) = set_kv(p, "chain_head", &head.to_string()).await {
        tracing::warn!("set_chain_head: {e:#}");
    }
}

pub(crate) async fn set_kv(p: &PgDb, key: &str, value: &str) -> Result<()> {
    let (key, value) = (key.to_string(), value.to_string());
    write_one(p, "set_kv", move || kv_query(&key, &value)).await
}

/// Two passes, since the timestamps must be read inside the writer's
/// transaction: first ask `events` which blocks it needs (it returns early
/// on a missing stamp, so this pass is cheap), then read those blocks and
/// build the events for real. `events` must be deterministic.
pub(crate) async fn save_anchoring_window(
    p: &PgDb,
    key: &str,
    value: &str,
    events: impl Fn(&dyn Fn(i64) -> Option<i64>) -> Vec<AnchoringEvent> + Send + 'static,
) -> Result<usize> {
    let wanted = std::cell::RefCell::new(Vec::new());
    let _ = events(&|n| {
        wanted.borrow_mut().push(n);
        None
    });
    let numbers = wanted.into_inner();
    let events = Arc::new(Mutex::new(events));
    let (key, value) = (key.to_string(), value.to_string());
    Ok(p.writer()?
        .write(Budget::Batch, move |c| -> TxFuture<'_, usize> {
            let (events, numbers, key, value) =
                (events.clone(), numbers.clone(), key.clone(), value.clone());
            Box::pin(async move {
                let rows = fetch_all(
                    c,
                    "anchoring stamps",
                    sqlx::query("SELECT number, timestamp FROM blocks WHERE number = ANY($1)")
                        .bind(numbers),
                )
                .await?;
                let mut stamps = HashMap::with_capacity(rows.len());
                for row in &rows {
                    stamps.insert(get::<i64>(row, 0)?, get::<i64>(row, 1)?);
                }
                let built = {
                    let events = events.lock().unwrap_or_else(|e| e.into_inner());
                    events(&|n| stamps.get(&n).copied())
                };
                // A repeated (block, log index) is skipped by `DO NOTHING`,
                // and only rows inserted are returned, as `INSERT OR IGNORE`
                // counts them.
                let wrote = if built.is_empty() {
                    0
                } else {
                    fetch_all(c, "anchoring", anchoring_query(&built))
                        .await?
                        .len()
                };
                exec(c, "watermark", kv_query(&key, &value)).await?;
                Ok(wrote)
            })
        })
        .await?)
}

/// Store the genesis balances not stored yet, add them to their holders',
/// and move the cursor, in one transaction. A repeated (token, holder) is
/// skipped by `DO NOTHING`, and only rows inserted are returned.
pub(crate) async fn save_genesis_balances(
    p: &PgDb,
    balances: &[(Holder, String)],
    cursor: i64,
) -> Result<()> {
    let balances = Arc::new(balances.to_vec());
    Ok(p.writer()?
        .write(Budget::Batch, move |c| -> TxFuture<'_, ()> {
            let balances = balances.clone();
            Box::pin(async move {
                let mut net: HashMap<(String, String), BigInt> = HashMap::new();
                if !balances.is_empty() {
                    let inserted = fetch_all(
                        c,
                        "genesis",
                        sqlx::query(
                            "INSERT INTO genesis_balances (token_addr, holder_addr, balance) \
                             SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[]) \
                             ON CONFLICT (token_addr, holder_addr) DO NOTHING \
                             RETURNING token_addr, holder_addr, balance",
                        )
                        .bind(col(&balances, |((t, _), _)| t.clone()))
                        .bind(col(&balances, |((_, h), _)| h.clone()))
                        .bind(col(&balances, |(_, b)| b.clone())),
                    )
                    .await?;
                    for row in &inserted {
                        let key = (get::<String>(row, 0)?, get::<String>(row, 1)?);
                        *net.entry(key).or_default() += bigint(&get::<String>(row, 2)?);
                    }
                }
                write_balances(c, net).await?;
                exec(
                    c,
                    "genesis cursor",
                    kv_query("genesis_balances_cursor", &cursor.to_string()),
                )
                .await?;
                Ok(())
            })
        })
        .await?)
}

/// The lease's heartbeat, on the writer session.
pub(crate) async fn keepalive(p: &PgDb) {
    if let Some(w) = &p.writer {
        w.keepalive().await;
    }
}
