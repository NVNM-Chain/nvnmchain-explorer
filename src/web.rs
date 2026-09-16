//! Axum web application: routes, JSON API, and Tera-rendered HTML.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::Json;
use axum::Router;
use chrono::{Local, TimeZone};
use ethers_core::abi::StateMutability;
use futures_util::stream::unfold;
use num_bigint::BigInt;
use serde_json::{json, Value};
use tera::Tera;
use tokio::sync::{broadcast, watch};
use tower_http::cors::CorsLayer;

use crate::anchoring;
use crate::config::Settings;
use crate::contracts::{
    abis_for_address, get_contract_name, get_precompile_name, identify_address, is_contract,
    is_tip20_token, search_named,
};
use crate::db::{self, Db, TxColumns};
use crate::decoder::{
    checksum_address, decode_event, decode_function_call, decode_revert, decode_with_signature,
    event_signature, extract_balance_changes, extract_calls, flatten_trace, function_signature,
    keccak_hex, revert_data_in, DecodedEvent, REGISTRY,
};
use crate::name_search;
use crate::rpc::ChainRpc;
use crate::signatures;
use crate::summary::{build_summary, known_events, Failure, TokenDisplay, Tokens};
use crate::tempo_address::parse_virtual;
use crate::tokens::{
    fetch_token_metadata, format_token_amount, format_token_amount_with_symbol, has_control_chars,
};

#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub rpc: ChainRpc,
    pub cfg: Settings,
    pub tera: Arc<Tera>,
    /// Live stream of indexed blocks, fed by the indexer writer task.
    pub block_events: broadcast::Sender<Value>,
    /// Home-page stats, published by the indexer's stats task — read here so
    /// a page view costs no aggregate queries.
    pub stats: Arc<RwLock<Value>>,
    /// Flipped on SIGINT/SIGTERM. The SSE stream ends when it flips; since
    /// this state owns a `block_events` sender, it never ends on its own.
    pub shutdown: watch::Receiver<bool>,
}

fn wants_json(headers: &HeaderMap, query: &HashMap<String, String>) -> bool {
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    accept.contains("application/json") || query.get("format").map(|f| f == "json").unwrap_or(false)
}

/// The columns a listing reads. A JSON answer carries the rows whole; a page shows
/// a few fields of each, and the blobs it never renders are three megabytes a page.
fn columns_for(headers: &HeaderMap, query: &HashMap<String, String>) -> TxColumns {
    if wants_json(headers, query) {
        TxColumns::Full
    } else {
        TxColumns::List
    }
}

fn render_html(tera: &Tera, template: &str, ctx: &Value) -> Response {
    match tera::Context::from_serialize(ctx) {
        Ok(tera_ctx) => match tera.render(template, &tera_ctx) {
            Ok(html) => Html(html).into_response(),
            Err(e) => {
                tracing::error!("template {template} render failed: {e:?}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Html("Internal server error"),
                )
                    .into_response()
            }
        },
        Err(e) => {
            tracing::error!("template context serialization failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html("Internal server error"),
            )
                .into_response()
        }
    }
}

fn json_response(ctx: &Value) -> Response {
    Json(ctx.clone()).into_response()
}

fn html_or_json(
    state: &AppState,
    headers: &HeaderMap,
    query: &HashMap<String, String>,
    template: &str,
    ctx: &Value,
) -> Response {
    if wants_json(headers, query) {
        json_response(ctx)
    } else {
        render_html(&state.tera, template, ctx)
    }
}

/// Common context keys every page needs. Every rendered template extends
/// `base.html`, so every one of these must be present or the render fails —
/// build page contexts through here rather than by hand.
fn page_ctx(state: &AppState, extra: Value) -> Value {
    page_ctx_for(state, db::get_latest_block(&state.db), extra)
}

/// [`page_ctx`] for handlers that have already read the tip, so the page does
/// not query for it twice.
fn page_ctx_for(
    state: &AppState,
    latest_block: Option<crate::models::Block>,
    extra: Value,
) -> Value {
    let mut map = serde_json::Map::new();
    map.insert(
        "latest_block".into(),
        serde_json::to_value(latest_block).unwrap_or(Value::Null),
    );
    map.insert("native_symbol".into(), json!(state.cfg.native_symbol));
    // The search box echoes it; only the search page has one to echo.
    map.insert("query".into(), json!(""));
    merge_into(&mut map, extra);
    Value::Object(map)
}

/// Fold one JSON object's keys into another, the incoming ones winning. A
/// non-object contributes nothing; every context merged here is a `json!({…})`.
fn merge_into(map: &mut serde_json::Map<String, Value>, extra: Value) {
    if let Value::Object(o) = extra {
        for (k, v) in o {
            map.insert(k, v);
        }
    }
}

/// The keys a transaction row draws, for the listings that share its markup.
fn tx_row(t: &crate::models::Transaction) -> Value {
    json!({
        "tx_hash": t.hash,
        "tx_from": t.from_addr,
        "tx_to": t.to_addr,
        "tx_timestamp": t.timestamp,
        "tx_status": t.status,
        "tx_block": t.block_number,
        "tx_method": tx_method_badge(&t.input),
    })
}

/// Rows per page, for every listing the explorer serves.
const PER_PAGE: u32 = 25;

/// The 1-based page a listing was asked for.
fn page_param(query: &HashMap<String, String>) -> u32 {
    query
        .get("page")
        .and_then(|p| p.parse().ok())
        .unwrap_or(1)
        .max(1)
}

/// Pages needed to show `total` rows, never fewer than one.
fn total_pages(total: i64, per_page: u32) -> u32 {
    (total.max(0) as u64).div_ceil(u64::from(per_page)).max(1) as u32
}

/// Compact method badge for a transaction: decoded name > signature > selector.
fn tx_method_badge(input: &str) -> Option<String> {
    if input.is_empty() || input == "0x" {
        return None;
    }
    decode_function_call(input).map(|d| d.name.or(d.signature).unwrap_or(d.selector))
}

/// Extra descriptors for the tx detail page, derived from the stored receipt
/// and the raw RPC object: (fee payer, signature type, failure reason).
pub fn tx_extras(
    tx: &crate::models::Transaction,
    receipt: Option<&Value>,
) -> (Option<String>, Option<String>, Option<String>) {
    let fee_payer = receipt
        .and_then(|r| r.get("feePayer").and_then(Value::as_str))
        .filter(|s| !s.is_empty())
        .map(String::from);
    let signature_type = tx
        .raw
        .as_deref()
        .map(crate::decoder::parse_raw_tx)
        .unwrap_or_default()
        .sig_type;
    let fail_reason = if tx.status == 0 {
        receipt
            .and_then(|r| {
                r.get("error")
                    .and_then(Value::as_str)
                    .or_else(|| r.get("revertReason").and_then(Value::as_str))
                    .or_else(|| r.get("outcome").and_then(Value::as_str))
            })
            .map(String::from)
    } else {
        None
    };
    (fee_payer, signature_type, fail_reason)
}

/// Replay every replayable top-level call of a transaction via `eth_call` at
/// its block, as one batched request. Returns the node's error message per
/// call (`None` = succeeded). This is the only per-call outcome source on
/// chains whose receipts record no reason and whose tracing is unsupported.
/// Entries are in `calls` order, skipping calls without a destination.
pub async fn replay_tx_calls(
    rpc: &ChainRpc,
    tx: &crate::models::Transaction,
) -> Vec<Option<String>> {
    let parsed = tx
        .raw
        .as_deref()
        .map(crate::decoder::parse_raw_tx)
        .unwrap_or_default();
    let calls = parsed.calls;
    if calls.is_empty() {
        return Vec::new();
    }
    let from = tx.from_addr.clone();
    let gas = format!("0x{:x}", parsed.gas_limit.unwrap_or(0).max(0));
    let block = format!("0x{:x}", tx.block_number.max(0));
    let batch: Vec<(String, Value)> = calls
        .iter()
        .filter_map(|call| {
            let to = call.get("to").and_then(Value::as_str)?;
            if to.is_empty() {
                return None;
            }
            let data = call
                .get("data")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .or_else(|| call.get("input").and_then(Value::as_str))
                .unwrap_or("0x")
                .to_string();
            Some((
                "eth_call".to_string(),
                json!([{"from": from, "to": to, "data": data, "gas": gas}, block]),
            ))
        })
        .collect();
    let Ok(results) = rpc.batch_call(batch).await else {
        return Vec::new();
    };
    results
        .into_iter()
        .map(|r| match r {
            Ok(_) => None,
            Err(e) => Some(e.message.clone()),
        })
        .collect()
}

/// The tokens a log is about: the contract that emitted it, plus any its
/// arguments name. Matched by address shape, since every event that names a
/// token names it differently.
fn tokens_mentioned(event: &DecodedEvent) -> Vec<String> {
    std::iter::once(event.contract.clone())
        .chain(
            event
                .params
                .iter()
                .filter(|p| p.ty == "address" && is_tip20_token(&p.value))
                .map(|p| p.value.clone()),
        )
        .collect()
}

/// Symbol and decimals per address, keyed lowercase: a log and the database
/// need not agree on checksum casing.
fn token_display_map(state: &AppState, addresses: impl Iterator<Item = String>) -> Tokens {
    let mut wanted: Vec<String> = addresses
        .map(|a| checksum_address(&a))
        .filter(|a| is_valid_address(a))
        .collect();
    wanted.sort();
    wanted.dedup();
    db::get_tokens_metadata(&state.db, &wanted)
        .into_iter()
        .map(|(address, meta)| {
            (
                address.to_lowercase(),
                TokenDisplay {
                    symbol: meta.symbol,
                    decimals: meta.decimals,
                },
            )
        })
        .collect()
}

/// A page view must not wait on the RPC's full budget for the bytecode.
const CODE_FETCH_TIMEOUT: Duration = Duration::from_secs(5);

/// The deployed bytecode at `address`, or `Null` when there is none.
///
/// Best-effort: a node that will not answer costs the page a panel, not the
/// load. Tempo's precompiles report a one-byte marker rather than bytecode,
/// which is worth saying instead of rendering as an empty box.
async fn contract_code(state: &AppState, address: &str) -> Value {
    let fetched = tokio::time::timeout(
        CODE_FETCH_TIMEOUT,
        state.rpc.eth_get_code(address, "latest"),
    )
    .await;
    let code = match fetched {
        Ok(Ok(code)) => code,
        Ok(Err(e)) => {
            tracing::warn!("eth_getCode for {address} failed: {e:#}");
            return Value::Null;
        }
        Err(_) => {
            tracing::warn!("eth_getCode for {address} timed out");
            return Value::Null;
        }
    };
    let hex = code.strip_prefix("0x").unwrap_or(&code);
    if hex.is_empty() {
        return Value::Null;
    }
    json!({
        "hex": code,
        "bytes": hex.len() / 2,
        // The marker a Tempo precompile reports in place of real bytecode.
        "is_precompile_marker": hex.len() <= 2,
    })
}

/// What an address exposes: the ABIs the explorer knows for it, split into
/// reads and writes, plus its events. Empty for an unknown address.
fn contract_interface(address: &str) -> Value {
    let names = abis_for_address(address);
    let (mut reads, mut writes, mut events) = (Vec::new(), Vec::new(), Vec::new());
    for name in names {
        let Some(contract) = REGISTRY.contract(name) else {
            continue;
        };
        for function in contract.functions() {
            // The signature carries the parameter types, so only the name and
            // the selector need spelling out beside it.
            let entry = json!({
                "name": function.name,
                "signature": function_signature(function),
                "selector": format!("0x{}", hex::encode(function.short_signature())),
            });
            // A view or pure function answers a question; anything else
            // changes something. That is the split a reader cares about.
            match function.state_mutability {
                StateMutability::View | StateMutability::Pure => reads.push(entry),
                _ => writes.push(entry),
            }
        }
        for event in contract.events() {
            let signature = event_signature(event);
            events.push(json!({
                "name": event.name,
                "topic0": keccak_hex(signature.as_bytes()),
                "signature": signature,
                // Indexedness is the one thing the signature does not say.
                "inputs": event
                    .inputs
                    .iter()
                    .map(|i| json!({
                        "type": i.kind.to_string(),
                        "name": i.name,
                        "indexed": i.indexed,
                    }))
                    .collect::<Vec<_>>(),
            }));
        }
    }
    // `Contract::functions()`/`events()` walk a map; sort so the page does not
    // reshuffle itself between views.
    let by_name = |a: &Value, b: &Value| {
        a["name"]
            .as_str()
            .unwrap_or("")
            .cmp(b["name"].as_str().unwrap_or(""))
    };
    reads.sort_by(by_name);
    writes.sort_by(by_name);
    events.sort_by(by_name);
    json!({
        "abis": names,
        "reads": reads,
        "writes": writes,
        "events": events,
    })
}

/// Name the calls whose selector no built-in ABI declares, from the signature
/// directory. A directory that is off, unreachable or ignorant leaves the bare
/// selector; one that answers also decodes the arguments, since the name alone
/// is half an answer.
async fn name_unknown_calls(state: &AppState, calls: &mut [Value]) {
    let unnamed: Vec<String> = calls
        .iter()
        .filter_map(|call| {
            let decoded = call.get("decoded")?;
            match decoded.get("name") {
                Some(Value::Null) => decoded
                    .get("selector")
                    .and_then(Value::as_str)
                    .map(String::from),
                _ => None,
            }
        })
        .collect();
    if unnamed.is_empty() {
        return;
    }

    let names = signatures::resolve(&state.db, &state.cfg, state.rpc.http_client(), &unnamed).await;
    if names.is_empty() {
        return;
    }
    for call in calls.iter_mut() {
        let Some(selector) = call
            .pointer("/decoded/selector")
            .and_then(Value::as_str)
            .map(|s| s.to_lowercase())
        else {
            continue;
        };
        let Some(signature) = names.get(&selector) else {
            continue;
        };
        let data = call.get("data").and_then(Value::as_str).unwrap_or("0x");
        if let Some(decoded) = decode_with_signature(data, signature) {
            call["decoded"] = decoded.to_json();
            // Say where the name came from: it is a stranger's, not the
            // chain's, and a reader should be able to tell the difference.
            call["decoded"]["source"] = json!("signature directory");
        }
    }
}

/// Say what each failed call reverted with, rather than showing the ABI blob
/// the node handed back.
fn decode_call_reverts(calls: &mut [Value]) {
    for call in calls.iter_mut() {
        if call.get("status").and_then(Value::as_str) != Some("failed") {
            continue;
        }
        let Some(revert) = call_revert_data(call).as_deref().and_then(decode_revert) else {
            continue;
        };
        call["revert"] = json!({
            "name": revert.name,
            "signature": revert.signature,
            "text": revert.reason().unwrap_or(&revert.call_form()).to_string(),
            "params": revert.params,
        });
    }
}

/// The revert data a failed call carries: its output, or hex embedded in its
/// error message.
fn call_revert_data(call: &Value) -> Option<String> {
    call.get("output")
        .and_then(Value::as_str)
        .filter(|s| s.len() > 2 && s.starts_with("0x"))
        .map(String::from)
        .or_else(|| {
            call.get("error")
                .and_then(Value::as_str)
                .and_then(revert_data_in)
        })
}

/// What the call that reverted says about why. The deepest failed call is the
/// one that objected — the ones above it only passed the failure up.
fn failure_of(calls: &[Value], reason: Option<&str>) -> Failure {
    let mut failure = Failure {
        reason: reason.map(String::from),
        ..Default::default()
    };
    let Some(call) = calls
        .iter()
        .filter(|c| c.get("status").and_then(Value::as_str) == Some("failed"))
        .max_by_key(|c| c.get("depth").and_then(Value::as_i64).unwrap_or(0))
    else {
        return failure;
    };

    failure.revert_data = call_revert_data(call);
    failure.reason = failure
        .reason
        .or_else(|| call.get("error").and_then(Value::as_str).map(String::from));
    failure.function = call
        .pointer("/decoded/name")
        .and_then(Value::as_str)
        .map(String::from);
    if let Some(to) = call.get("to").and_then(Value::as_str) {
        let to = checksum_address(to);
        failure.contract = Some(get_contract_name(&to).unwrap_or_else(|| truncate_hash(&to, 4, 4)));
        failure.token = is_tip20_token(&to).then_some(to);
    }
    failure
}

/// Where the fee went: what the gas cost, how much was burnt, what the
/// validator kept, and the TIP-20 amount charged. Big integers throughout —
/// wei products overflow an i64.
fn fee_breakdown(
    tx: &crate::models::Transaction,
    gas_used: i64,
    gas_price: i64,
    fee_token_meta: Option<&crate::models::TokenMetadata>,
) -> Value {
    let gas_used_big = BigInt::from(gas_used.max(0));
    let base_fee = BigInt::parse_bytes(tx.base_fee.trim_start_matches("0x").as_bytes(), 16)
        .or_else(|| BigInt::parse_bytes(tx.base_fee.as_bytes(), 10))
        .unwrap_or_else(|| BigInt::from(0));
    let total = BigInt::from(gas_price.max(0)) * &gas_used_big;
    let burnt = &base_fee * &gas_used_big;
    // A gas price below the base fee cannot happen on chain, but a receipt
    // that reports one must not produce a negative tip.
    let tip = if total > burnt {
        &total - &burnt
    } else {
        BigInt::from(0)
    };

    let charged = fee_token_meta
        .map(|meta| format_token_amount_with_symbol(&tx.fee_amount, meta.decimals, &meta.symbol));
    json!({
        "gas_used": gas_used,
        "gas_price": gas_price,
        "base_fee": base_fee.to_string(),
        "total_wei": total.to_string(),
        "burnt_wei": burnt.to_string(),
        "tip_wei": tip.to_string(),
        "charged": charged,
        "has_charge": tx.fee_amount != "0" && tx.fee_token.is_some(),
    })
}

/// What to call an address, in the few characters a tag allows — the most
/// specific name first: a precompile's own name, a token's symbol, an indexed
/// contract label, that an address is a TIP-1022 forwarding alias.
pub fn address_label(db: &Db, address: &str) -> Option<String> {
    let checksummed = checksum_address(address);
    if !is_valid_address(&checksummed) {
        return None;
    }
    if let Some(name) = get_precompile_name(&checksummed) {
        return Some(name);
    }
    if let Some(name) = crate::contracts::deployed_contract_name(&checksummed) {
        return Some(name);
    }
    if let Some(meta) = db::get_token_metadata(db, &checksummed) {
        if !meta.symbol.is_empty() {
            return Some(meta.symbol);
        }
        if !meta.name.is_empty() {
            return Some(meta.name);
        }
    }
    if let Some(parts) = parse_virtual(&checksummed) {
        return Some(format!("Virtual {}", parts.master_id));
    }
    if is_tip20_token(&checksummed) {
        return Some("TIP-20".into());
    }
    None
}

/// One page of a token's holders, with balances in the token's own units and
/// each share of supply. The share is percent with four decimals, divided in
/// big integers so a supply too large for an f64 still comes out exact.
fn token_holders(
    state: &AppState,
    token: &str,
    meta: &crate::models::TokenMetadata,
    page: u32,
    per_page: u32,
) -> Vec<Value> {
    let supply = BigInt::parse_bytes(meta.total_supply.as_bytes(), 10).unwrap_or_default();
    db::get_token_holders(&state.db, token, page, per_page)
        .into_iter()
        .enumerate()
        .map(|(i, (address, balance))| {
            let share = if supply > BigInt::from(0) {
                // balance/supply as percent ×10⁴, then the point re-inserted.
                let scaled = BigInt::parse_bytes(balance.as_bytes(), 10).unwrap_or_default()
                    * BigInt::from(1_000_000)
                    / &supply;
                let scaled: i64 = scaled.try_into().unwrap_or(0);
                format!("{}.{:04}", scaled / 10_000, scaled % 10_000)
            } else {
                String::new()
            };
            json!({
                "rank": db::page_offset(page, per_page) + i as i64 + 1,
                "address": address,
                "formatted": format_token_amount(&balance, meta.decimals),
                "balance": balance,
                "share": share,
            })
        })
        .collect()
}

/// Burnt fees for a block: base fee × gas used, as a wei decimal string.
fn burnt_fees_wei(base_fee: &str, gas_used: i64) -> String {
    let base = BigInt::parse_bytes(base_fee.as_bytes(), 10).unwrap_or_else(|| BigInt::from(0));
    (base * BigInt::from(gas_used.max(0))).to_string()
}

/// SVG polyline points for a 160×32 sparkline.
fn sparkline(values: &[f64]) -> String {
    if values.is_empty() {
        return String::new();
    }
    let max = values.iter().cloned().fold(0.0_f64, f64::max).max(1.0);
    let n = values.len();
    let pts: Vec<String> = values
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let x = if n == 1 {
                80.0
            } else {
                i as f64 / (n - 1) as f64 * 160.0
            };
            let y = 30.0 - (v / max * 26.0);
            format!("{x:.1},{y:.1}")
        })
        .collect();
    pts.join(" ")
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

pub async fn home(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let latest_block = db::get_latest_block(&state.db);
    let recent_blocks = db::get_recent_blocks(&state.db, state.cfg.recent_block_count);
    let recent_txs = db::get_transactions(&state.db, 1, state.cfg.recent_tx_count as u32);
    let latest_num = latest_block.as_ref().map(|b| b.number).unwrap_or(0);
    let mut stats = state
        .stats
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    if let Value::Object(ref mut o) = stats {
        let avg_ms = o
            .get("avg_block_time_ms")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let tps = o.get("tps").and_then(Value::as_f64).unwrap_or(0.0);
        let gas = o.get("gas_util_pct").and_then(Value::as_f64).unwrap_or(0.0);
        o.insert(
            "avg_block_time_display".into(),
            json!(format_block_time(avg_ms)),
        );
        o.insert("tps_display".into(), json!(format!("{tps:.2}")));
        o.insert("gas_util_display".into(), json!(format!("{gas:.1}%")));
    }
    let spark_tx = sparkline(
        &recent_blocks
            .iter()
            .map(|b| b.tx_count as f64)
            .collect::<Vec<_>>(),
    );
    let spark_gas = sparkline(
        &recent_blocks
            .iter()
            .map(|b| {
                if b.gas_limit > 0 {
                    b.gas_used as f64 / b.gas_limit as f64 * 100.0
                } else {
                    0.0
                }
            })
            .collect::<Vec<_>>(),
    );

    // Index progress rides in the same stats payload, so the tiles and the
    // bar cost no per-view aggregates either.
    let indexed_count = stats
        .get("total_blocks")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    // A blob written before the head rode along in it (an upgrade, seeded from
    // kv) has no chain_head; the kv row it came from does, so the bar shows
    // real progress rather than 0% until the first recompute.
    let chain_head = stats
        .get("chain_head")
        .and_then(Value::as_i64)
        .filter(|head| *head > 0)
        .or_else(|| db::get_kv(&state.db, "chain_head").and_then(|v| v.parse().ok()))
        .unwrap_or(0);
    let index_pct = if chain_head > 0 {
        (indexed_count as f64 / chain_head as f64 * 100.0).clamp(0.0, 100.0)
    } else {
        0.0
    };
    let ctx = page_ctx_for(
        &state,
        latest_block,
        json!({
            "stats": stats,
            "recent_blocks": recent_blocks,
            "recent_txs": recent_txs,
            "recent_block_count": state.cfg.recent_block_count,
            "recent_tx_count": state.cfg.recent_tx_count,
            "latest_num": latest_num,
            "chain_head": chain_head,
            "indexed_display": comma_num(indexed_count),
            "head_display": comma_num(chain_head),
            "index_pct_display": format!("{index_pct:.1}"),
            "spark_tx": spark_tx,
            "spark_gas": spark_gas,
        }),
    );
    html_or_json(&state, &headers, &query, "home.html", &ctx)
}

/// Server-Sent Events endpoint: pushes each newly indexed block to browsers
/// so the home page updates live without polling. Sends the current tip
/// immediately on connect, then every block as the indexer writes it (the
/// writer emits in number order, so the stream is gapless; if this subscriber
/// ever falls behind the broadcast, missed blocks are replayed from the DB).
pub async fn events(State(state): State<AppState>) -> Response {
    let rx = state.block_events.subscribe();
    let stream = unfold(
        SseState {
            rx,
            shutdown: state.shutdown.clone(),
            db: state.db.clone(),
            pending: std::collections::VecDeque::new(),
            last_num: -1,
            sent_initial: false,
        },
        sse_step,
    );
    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keepalive"),
        )
        .into_response()
}

struct SseState {
    rx: broadcast::Receiver<Value>,
    shutdown: watch::Receiver<bool>,
    db: Db,
    /// Catch-up events (blocks replayed after a broadcast lag).
    pending: std::collections::VecDeque<Value>,
    /// Highest block number already delivered to this client.
    last_num: i64,
    sent_initial: bool,
}

/// Upper bound on how many missed blocks a lagging client is replayed.
const SSE_MAX_REPLAY: usize = 4096;

fn sse_event(payload: &Value) -> Result<Event, std::convert::Infallible> {
    // Stats refreshes share the block channel; each payload names its own kind,
    // so a block that grows a `stats` field stays a block. A stats message lost
    // to broadcast lag is not replayed -- the next tick supersedes it.
    let name = payload
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("block");
    Ok(Event::default().event(name).data(payload.to_string()))
}

async fn sse_step(
    mut state: SseState,
) -> Option<(Result<Event, std::convert::Infallible>, SseState)> {
    // Ending the stream is what lets graceful shutdown finish: axum waits
    // for in-flight connections, and this one never ends on its own.
    if *state.shutdown.borrow() {
        return None;
    }
    // Initial event: the current tip (with its transactions), so the panels
    // are populated immediately on connect.
    if !state.sent_initial {
        state.sent_initial = true;
        if let Some(b) = db::get_latest_block(&state.db) {
            state.last_num = b.number;
            let txs = db::get_block_transactions(&state.db, b.number, TxColumns::List);
            let payload = crate::models::block_event_json(&b, &txs, crate::models::STREAM_TX_CAP);
            return Some((sse_event(&payload), state));
        }
        return Some((Ok(Event::default().event("block").data("null")), state));
    }
    // Drain replayed catch-up events before reading new ones.
    if let Some(v) = state.pending.pop_front() {
        return Some((sse_event(&v), state));
    }
    loop {
        let received = tokio::select! {
            biased; // shutdown wins a tie
            _ = state.shutdown.wait_for(|&stop| stop) => return None,
            received = state.rx.recv() => received,
        };
        match received {
            Ok(block) => {
                if let Some(n) = block.pointer("/block/number").and_then(Value::as_i64) {
                    // Blocks still in the channel after a replay have already
                    // been delivered from the DB; sending them again would
                    // duplicate rows and walk `last_num` backwards.
                    if n <= state.last_num {
                        continue;
                    }
                    state.last_num = n;
                }
                return Some((sse_event(&block), state));
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                // This client fell behind (browser throttled / connection
                // stalled). The writer emits in number order, so the missed
                // blocks are exactly `last_num + 1 ..= tip`; replay them from
                // the DB instead of skipping them. The dropped-message count is
                // no use as a bound -- stats refreshes share this channel and
                // are counted too -- so the tip bounds the window.
                let tip = db::get_latest_block(&state.db)
                    .map(|b| b.number)
                    .unwrap_or(state.last_num);
                let start = state.last_num + 1;
                let end = tip.min(start + SSE_MAX_REPLAY as i64 - 1);
                // Two range queries for the whole span, not two per height: a
                // client that fell far behind replays up to `SSE_MAX_REPLAY`.
                let blocks = db::get_blocks_in_range(&state.db, start, end);
                let txs = db::get_transactions_in_range(&state.db, start, end, TxColumns::List);
                // Oldest first, since the writer emits in number order.
                for block in blocks.into_iter().rev() {
                    let first = txs.partition_point(|t| t.block_number < block.number);
                    let past = txs.partition_point(|t| t.block_number <= block.number);
                    state.pending.push_back(crate::models::block_event_json(
                        &block,
                        &txs[first..past],
                        crate::models::STREAM_TX_CAP,
                    ));
                }
                state.last_num = state.last_num.max(end);
                if let Some(v) = state.pending.pop_front() {
                    return Some((sse_event(&v), state));
                }
                // Nothing replayed (blocks not indexed yet); keep reading.
            }
            Err(broadcast::error::RecvError::Closed) => return None,
        }
    }
}
pub async fn block_page(
    State(state): State<AppState>,
    Path(block_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let block = if block_id.chars().all(|c| c.is_ascii_digit()) && !block_id.is_empty() {
        block_id
            .parse::<i64>()
            .ok()
            .and_then(|n| db::get_block_by_number(&state.db, n))
    } else {
        db::get_block_by_hash(&state.db, &block_id).or_else(|| {
            block_id
                .strip_prefix("0x")
                .and_then(|h| u64::from_str_radix(h, 16).ok())
                .and_then(|n| db::get_block_by_number(&state.db, n as i64))
        })
    };
    let Some(block) = block else {
        return not_found(&state, &headers, &query, "Block", &block_id);
    };
    let transactions =
        db::get_block_transactions(&state.db, block.number, columns_for(&headers, &query));
    let gas_pct = block_pct(block.gas_used, block.gas_limit);
    let token_addrs: Vec<String> = transactions
        .iter()
        .filter_map(|t| t.fee_token.clone())
        .collect();
    let metas = db::get_tokens_metadata(&state.db, &token_addrs);
    let transactions: Vec<Value> = transactions
        .iter()
        .map(|t| {
            let mut v = serde_json::to_value(t).unwrap_or(Value::Null);
            if let Some(m) = tx_method_badge(&t.input) {
                v["method"] = json!(m);
            }
            if let Some(meta) = t.fee_token.as_deref().and_then(|f| metas.get(f)) {
                v["fee_token_meta"] = serde_json::to_value(meta).unwrap_or(Value::Null);
                v["fee_formatted"] = json!(format_token_amount_with_symbol(
                    &t.fee_amount,
                    meta.decimals,
                    &meta.symbol,
                ));
            }
            v
        })
        .collect();
    let burnt = burnt_fees_wei(&block.base_fee, block.gas_used);
    // Looked up rather than inferred from the tip: the index has gaps while it
    // backfills, so a number below the tip is not necessarily there to link to.
    let neighbour = |n: i64| db::get_block_by_number(&state.db, n).map(|b| b.number);
    let previous = (block.number > 0)
        .then(|| neighbour(block.number - 1))
        .flatten();
    let ctx = page_ctx(
        &state,
        json!({
            "block": block,
            "transactions": transactions,
            "gas_pct": gas_pct,
            "base_fee_gwei": format_token_amount(&block.base_fee, 9),
            "burnt_fees": burnt,
            "previous_block": previous,
            "next_block": neighbour(block.number + 1),
        }),
    );
    html_or_json(&state, &headers, &query, "block.html", &ctx)
}

pub async fn blocks_page(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let per_page = i64::from(PER_PAGE);
    // Bounded before the arithmetic below multiplies it.
    let page = i64::from(page_param(&query));
    let from: Option<i64> = query.get("from").and_then(|f| f.parse().ok());
    let latest = db::get_latest_block(&state.db);
    let latest_num = latest.as_ref().map(|b| b.number).unwrap_or(0);
    let end = from.unwrap_or_else(|| (latest_num - (page - 1) * per_page).max(0));
    let start = (end - per_page + 1).max(0);
    // One range query: the listing shows whichever of these heights are indexed.
    let blocks: Vec<Value> = db::get_blocks_in_range(&state.db, start, end)
        .into_iter()
        .map(|b| {
            let gas_pct = block_pct(b.gas_used, b.gas_limit);
            let mut v = serde_json::to_value(b).unwrap_or(Value::Null);
            v["gas_pct"] = json!(gas_pct);
            v
        })
        .collect();

    let ctx = page_ctx_for(
        &state,
        latest,
        json!({
            "blocks": blocks,
            "latest_num": latest_num,
            "total_blocks": latest_num + 1,
            "per_page": per_page,
            "page": page,
            "first_num": blocks.first().and_then(|b| b.get("number")).and_then(Value::as_i64),
            "last_num": blocks.last().and_then(|b| b.get("number")).and_then(Value::as_i64),
        }),
    );
    html_or_json(&state, &headers, &query, "blocks.html", &ctx)
}

/// Every transaction the indexer holds, newest first: what the home page's feed
/// of the latest few cannot page past.
pub async fn txs_page(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let page = page_param(&query);
    // The indexer recounts into the stats every few seconds; a request must not
    // hold the one connection for a count of its own, a quarter second cold on
    // a large database. Only a database the stats have not seen is counted here.
    let counted = state
        .stats
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get("total_txns")
        .and_then(Value::as_i64);
    let total = counted.unwrap_or_else(|| db::get_transaction_count(&state.db));
    let txs: Vec<Value> = db::get_transactions(&state.db, page, PER_PAGE)
        .iter()
        .map(tx_row)
        .collect();

    let ctx = page_ctx_for(
        &state,
        db::get_latest_block(&state.db),
        json!({
            "transactions": txs,
            "total_txns": total,
            "page": page,
            "per_page": PER_PAGE,
            "total_pages": total_pages(total, PER_PAGE),
        }),
    );
    html_or_json(&state, &headers, &query, "txs.html", &ctx)
}

/// Tripped when the node turns out to have no `debug_` namespace. Indexing
/// stopped tracing to keep up with the chain, so call trees are pulled per view
/// instead — on a node that cannot trace at all, that would be a doomed round
/// trip on every transaction page. One probe per process, then; a restart tries
/// again, so enabling tracing on the node needs no redeploy here.
static TRACING_UNAVAILABLE: AtomicBool = AtomicBool::new(false);

/// A page view must not wait on the RPC's full 30s budget for a call tree
/// nothing else on the page needs.
const TRACE_FETCH_TIMEOUT: Duration = Duration::from_secs(5);

/// Whether an RPC error means the node cannot trace *anything*, as opposed to
/// having nothing to say about one transaction. Only the former is worth
/// remembering: one pruned or unknown transaction must not disable call trees
/// for every other transaction the explorer serves.
fn is_method_unsupported(err: &anyhow::Error) -> bool {
    let Some(rpc) = err.downcast_ref::<crate::rpc::RpcError>() else {
        return false;
    };
    let message = rpc.message.to_lowercase();
    rpc.code == -32601
        || message.contains("method not found")
        || message.contains("does not exist")
        || message.contains("not available")
}

/// The call trace for a transaction indexed without one. Cached back onto the
/// row, so only the first view of a transaction pays for it.
async fn fetch_missing_trace(
    state: &AppState,
    tx: &crate::models::Transaction,
) -> Option<Vec<Value>> {
    if TRACING_UNAVAILABLE.load(Ordering::Relaxed) {
        return None;
    }
    let fetched = tokio::time::timeout(
        TRACE_FETCH_TIMEOUT,
        state.rpc.try_debug_trace_transaction(&tx.hash),
    )
    .await;
    let raw = match fetched {
        Ok(Ok(raw)) => raw,
        Ok(Err(e)) => {
            if is_method_unsupported(&e) {
                tracing::warn!("node cannot trace ({e}); call trees stay top-level");
                TRACING_UNAVAILABLE.store(true, Ordering::Relaxed);
            }
            return None;
        }
        // A slow node is not an untraceable one — keep trying on later views.
        Err(_) => {
            tracing::warn!("trace fetch for {} timed out", tx.hash);
            return None;
        }
    };
    let flat = flatten_trace(&raw);
    if flat.is_empty() {
        return None;
    }
    if let Ok(trace) = serde_json::to_string(&flat) {
        if let Err(e) = db::set_trace(&state.db, &tx.hash, &trace) {
            tracing::warn!("caching trace for {} failed: {e:#}", tx.hash);
        }
    }
    Some(flat)
}

/// The one call a transaction made, built from the transaction itself. Stands
/// in when there is neither a trace nor a `calls` array, so the page always
/// has a call tree to render.
fn top_level_call(tx: &crate::models::Transaction) -> Value {
    json!({
        "depth": 0,
        "type": "CALL",
        "to": tx.to_addr.clone(),
        "from": tx.from_addr.clone(),
        "data": tx.input.clone(),
        "decoded": decode_function_call(&tx.input).map(|d| d.to_json()).unwrap_or(Value::Null),
        "gas": "0",
        "gas_used": "0",
        "children": [],
    })
}

/// Attach `symbol`, `formatted` and `positive` to balance-change rows.
///
/// Every row gets all three whether or not its token is known: Tera errors on a
/// missing map key, so one unknown token would take the page down.
fn enrich_balance_changes(state: &AppState, changes: &mut [Value]) {
    let mut token_addrs: Vec<String> = changes
        .iter()
        .filter_map(|c| c.get("token").and_then(Value::as_str).map(String::from))
        .filter(|a| !a.is_empty())
        .collect();
    token_addrs.dedup();
    let metas = db::get_tokens_metadata(&state.db, &token_addrs);

    for change in changes.iter_mut() {
        let raw = change
            .get("change")
            .and_then(Value::as_str)
            .unwrap_or("0")
            .to_string();
        let display = change
            .get("token")
            .and_then(Value::as_str)
            .and_then(|token| metas.get(token))
            .map(|meta| (meta.symbol.clone(), meta.decimals));
        // What an unknown token's row shows instead: the raw signed integer.
        let unformatted = change.get("change").cloned().unwrap_or_else(|| json!(""));
        let Some(row) = change.as_object_mut() else {
            continue;
        };
        if let Some((symbol, decimals)) = display {
            let (sign, amount) = raw
                .strip_prefix('+')
                .map(|a| ("+", a))
                .or_else(|| raw.strip_prefix('-').map(|a| ("-", a)))
                .unwrap_or(("", raw.as_str()));
            row.insert("symbol".into(), json!(symbol));
            row.insert(
                "formatted".into(),
                json!(format!("{sign}{}", format_token_amount(amount, decimals))),
            );
        }
        row.insert("positive".into(), json!(raw.starts_with('+')));
        row.entry("symbol").or_insert_with(|| json!(""));
        row.entry("formatted").or_insert(unformatted);
    }
}

/// Mark the replayed top-level calls with what the replay said about each.
/// Outcomes are in `calls` order, skipping calls with no destination — exactly
/// the order [`replay_tx_calls`] batched them in — and every call after the one
/// that reverted never executed.
fn apply_replay_outcomes(calls: &mut [Value], outcomes: &[Option<String>]) {
    let mut outcomes = outcomes.iter();
    for call in calls.iter_mut() {
        if call.get("depth").and_then(Value::as_i64) != Some(0) {
            continue;
        }
        if call
            .get("to")
            .and_then(Value::as_str)
            .unwrap_or("")
            .is_empty()
        {
            continue;
        }
        let Some(outcome) = outcomes.next() else {
            break;
        };
        match outcome {
            Some(err) => {
                call["status"] = json!("failed");
                call["error"] = json!(err);
                break;
            }
            None => call["status"] = json!("success"),
        }
    }
}

/// Decide what each call did, and — when the transaction failed — why.
///
/// A trace-based chain carries the error on the failing trace node; a
/// tempo-style one records nothing, so the calls are replayed with one batched
/// `eth_call` to find which of them reverted. Best-effort throughout: an RPC
/// hiccup costs the page its per-call badges, not the page.
///
/// Returns the reason to show, which the replay may be the only source of.
async fn resolve_call_statuses(
    state: &AppState,
    tx: &crate::models::Transaction,
    calls: &mut [Value],
    mut fail_reason: Option<String>,
) -> Option<String> {
    if tx.status == 0 {
        if fail_reason.is_none() {
            fail_reason = calls
                .iter()
                .find_map(|c| c.get("error").and_then(Value::as_str))
                .map(String::from);
        }
        if fail_reason.is_none() {
            let outcomes = replay_tx_calls(&state.rpc, tx).await;
            fail_reason = outcomes.iter().find_map(|o| o.clone());
            apply_replay_outcomes(calls, &outcomes);
        }
    } else if calls
        .iter()
        .filter(|c| c.get("depth").and_then(Value::as_i64) == Some(0))
        .count()
        == 1
    {
        // Successful tx on an atomic chain: a lone top-level call executed.
        calls[0]["status"] = json!("success");
    }
    // A synthetic fallback call (no per-call data at all) mirrors the tx result.
    if let Some(first) = calls.first_mut() {
        if first.get("status").is_none() {
            first["status"] = json!(if tx.status == 1 { "success" } else { "failed" });
            if tx.status == 0 {
                if let Some(reason) = &fail_reason {
                    first["error"] = json!(reason);
                }
            }
        }
    }
    fail_reason
}

/// The gas, fee and identity fields of a transaction, as context keys.
///
/// All of it is parsed from the canonical RLP encoding at render time rather
/// than stored per column, so this is where the transaction page pays for the
/// columns the schema does not carry.
fn gas_and_fee_ctx(
    state: &AppState,
    tx: &crate::models::Transaction,
    receipt: Option<&Value>,
) -> Value {
    let parsed = tx
        .raw
        .as_deref()
        .map(crate::decoder::parse_raw_tx)
        .unwrap_or_default();
    let gas_price = receipt
        .and_then(|r| r.get("effectiveGasPrice").and_then(Value::as_str))
        .map(parse_hex_i64)
        .unwrap_or(0);
    let gas_used = tx.gas_used;
    let gas_limit = parsed.gas_limit.unwrap_or(0);
    let tx_type = parsed.tx_type.unwrap_or(0x76);
    let method_id = parsed
        .calls
        .first()
        .and_then(|c| c.get("data").and_then(Value::as_str))
        .and_then(|d| d.strip_prefix("0x"))
        .filter(|h| h.len() >= 8)
        .map(|h| format!("0x{}", &h[..8]))
        .unwrap_or_else(|| "0x".into());
    let gas_pct = if gas_limit > 0 {
        format!("{:.2}", gas_used as f64 / gas_limit as f64 * 100.0)
    } else {
        String::new()
    };
    let fee_token_meta = tx
        .fee_token
        .as_deref()
        .and_then(|f| db::get_token_metadata(&state.db, f));
    let fee_breakdown = fee_breakdown(tx, gas_used, gas_price, fee_token_meta.as_ref());
    json!({
        "gas_price": gas_price,
        "gas_used": gas_used,
        "gas_limit": gas_limit,
        "gas_pct": gas_pct,
        "max_fee": parsed.max_fee_per_gas.unwrap_or(0),
        "max_priority": parsed.max_priority_fee_per_gas.unwrap_or(0),
        "base_fee": parse_hex_i64(&tx.base_fee),
        "tx_type": tx_type,
        "tx_type_hex": format!("{tx_type:02x}"),
        "nonce": parsed.nonce.unwrap_or(0),
        "nonce_key": parsed.nonce_key,
        "method_id": method_id,
        "fee_token": tx.fee_token,
        "fee_amount": tx.fee_amount,
        "fee_breakdown": fee_breakdown,
        "fee_token_meta": fee_token_meta,
    })
}

pub async fn tx_page(
    State(state): State<AppState>,
    Path(tx_hash): Path<String>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let Some(mut tx) = db::get_transaction(&state.db, &tx_hash) else {
        return not_found(&state, &headers, &query, "Transaction", &tx_hash);
    };
    let receipt: Option<Value> = tx
        .receipt_data
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok());
    let trace: Option<Vec<Value>> = match tx
        .trace_data
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok())
    {
        Some(trace) => Some(trace),
        None => fetch_missing_trace(&state, &tx).await,
    };
    let block = db::get_block_by_number(&state.db, tx.block_number);

    // Tempo-style txs carry their destination in `calls[0].to`; fall back to
    // the receipt's `to` (nodes fill it with the first call's destination).
    if tx.to_addr.is_none() {
        if let Some(to) = receipt
            .as_ref()
            .and_then(|r| r.get("to"))
            .and_then(Value::as_str)
        {
            tx.to_addr = Some(to.to_string());
        }
    }

    let mut calls = extract_calls(&tx, trace.as_deref().unwrap_or(&[]));
    if calls.is_empty() {
        calls.push(top_level_call(&tx));
    }
    // Name the calls no built-in ABI explains, from the signature directory.
    name_unknown_calls(&state, &mut calls).await;
    // Indent each call by depth for the tree view.
    for call in calls.iter_mut() {
        let depth = call.get("depth").and_then(Value::as_i64).unwrap_or(0);
        call["indent"] = json!(depth * 20);
    }

    let decoded_events: Vec<DecodedEvent> = receipt
        .as_ref()
        .and_then(|r| r.get("logs"))
        .and_then(Value::as_array)
        .map(|logs| logs.iter().filter_map(decode_event).collect())
        .unwrap_or_default();

    let mut balance_changes = receipt
        .as_ref()
        .map(|r| extract_balance_changes(r, &tx))
        .unwrap_or_default();
    enrich_balance_changes(&state, &mut balance_changes);

    // Metadata for every token the page mentions, so amounts read in the
    // token's own units rather than as raw integers. One batched query.
    let token_display = token_display_map(
        &state,
        decoded_events
            .iter()
            .flat_map(tokens_mentioned)
            .chain(tx.fee_token.clone()),
    );

    let mut method = tx_method_badge(&tx.input);
    if method.is_none() {
        // Tempo-style txs have no top-level input; badge from the first call.
        method = calls
            .first()
            .and_then(|c| c.get("data").and_then(Value::as_str))
            .and_then(tx_method_badge);
    }

    let (fee_payer, signature_type, fail_reason) = tx_extras(&tx, receipt.as_ref());
    let fail_reason = resolve_call_statuses(&state, &tx, &mut calls, fail_reason).await;
    decode_call_reverts(&mut calls);

    let failed_calls = calls
        .iter()
        .filter(|c| c.get("status").and_then(Value::as_str) == Some("failed"))
        .count() as i64;

    // What the transaction did, in words. Built after the per-call statuses
    // are known, since a failure summary is named after the call that failed.
    let known = known_events(&decoded_events, &token_display, Some(&tx.from_addr));
    let failure = (tx.status == 0).then(|| failure_of(&calls, fail_reason.as_deref()));
    let summary = build_summary(tx.status == 1, &known, failure.as_ref(), &token_display);
    // Each log paired with its sentence, so the events tab can lead with the
    // reading and keep the decoded parameters underneath it.
    let events: Vec<Value> = decoded_events
        .iter()
        .enumerate()
        .map(|(i, decoded)| {
            let mut value = serde_json::to_value(decoded).unwrap_or(Value::Null);
            if let Some(said) = known.iter().find(|k| k.log_index == i) {
                value["known"] = serde_json::to_value(said).unwrap_or(Value::Null);
            }
            value
        })
        .collect();

    let mut extra = serde_json::Map::new();
    merge_into(&mut extra, gas_and_fee_ctx(&state, &tx, receipt.as_ref()));
    merge_into(
        &mut extra,
        json!({
            "tx": tx,
            "block": block,
            "receipt": receipt,
            "trace": trace,
            "calls": calls,
            "events": events,
            "summary": summary,
            "known_events": known,
            "balance_changes": balance_changes,
            "fee_payer": fee_payer,
            "signature_type": signature_type,
            "fail_reason": fail_reason,
            "failed_calls": failed_calls,
            "method": method,
            "active_tab": query.get("tab").cloned().unwrap_or_else(|| "overview".into()),
        }),
    );
    let ctx = page_ctx(&state, Value::Object(extra));
    html_or_json(&state, &headers, &query, "tx.html", &ctx)
}

pub async fn receipt_page(State(_state): State<AppState>, Path(tx_hash): Path<String>) -> Response {
    Redirect::to(&format!("/tx/{tx_hash}")).into_response()
}

pub async fn address_page(
    State(state): State<AppState>,
    Path(address): Path<String>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let checksummed = checksum_address(&address);
    if !is_valid_address(&checksummed) {
        return invalid_address(&state, &headers, &query, "Address", &address);
    }

    let tab = query
        .get("tab")
        .cloned()
        .unwrap_or_else(|| "transactions".into());
    let page = page_param(&query);
    let per_page = PER_PAGE;

    // Both totals on every tab: the header shows them side by side, and the
    // pager needs the total for whichever tab is open.
    let tx_count = db::get_address_transaction_count(&state.db, &checksummed);
    let transfer_count = db::get_address_transfer_count(&state.db, &checksummed);

    let (transactions, html_transactions) = if tab == "transfers" {
        let mut transfers = db::get_address_transfers(&state.db, &checksummed, page, per_page);
        enrich_transfers(&state, &mut transfers);
        (transfers.clone(), transfers)
    } else {
        let txs = db::get_address_transactions(
            &state.db,
            &checksummed,
            page,
            per_page,
            columns_for(&headers, &query),
        );
        let html_txs: Vec<Value> = txs.iter().map(tx_row).collect();
        (
            txs.into_iter()
                .map(|t| serde_json::to_value(t).unwrap_or(Value::Null))
                .collect::<Vec<Value>>(),
            html_txs,
        )
    };
    let total_pages = match tab.as_str() {
        "transfers" => total_pages(transfer_count, per_page),
        // Holdings is not paged: an address holds few enough tokens.
        "holdings" => 1,
        _ => total_pages(tx_count, per_page),
    };

    // The Contract tab: the interface the explorer knows, the TIP-20 metadata
    // when there is any, and the deployed bytecode. Only the code costs an RPC
    // round trip, and only when that tab is open.
    let interface = contract_interface(&checksummed);
    let has_interface = interface["abis"].as_array().is_some_and(|a| !a.is_empty());

    let addr_info = identify_address(&checksummed);
    let is_token_addr = db::get_token_metadata(&state.db, &checksummed).is_some()
        || crate::contracts::is_tip20_token(&checksummed);
    let kind = if addr_info.kind == "eoa" && (is_contract(&checksummed) || is_token_addr) {
        "contract"
    } else {
        addr_info.kind.as_str()
    };
    let label = addr_info.label.clone();

    let token_meta = db::get_token_metadata(&state.db, &checksummed);
    let code = if tab == "contract" {
        contract_code(&state, &checksummed).await
    } else {
        Value::Null
    };
    let virtual_address = parse_virtual(&checksummed);

    let ctx = page_ctx(
        &state,
        json!({
            "address": checksummed,
            "addr_info": addr_info,
            "type": kind,
            "label": label,
            "interface": interface,
            "has_interface": has_interface,
            "token_meta": token_meta,
            "code": code,
            "virtual_address": virtual_address,
            "transactions": transactions,
            "html_transactions": html_transactions,
            "holdings": db::get_address_holdings(&state.db, &checksummed),
            "tx_count": tx_count,
            "transfer_count": transfer_count,
            "page": page,
            "total_pages": total_pages,
            "per_page": per_page,
            "active_tab": tab,
        }),
    );
    html_or_json(&state, &headers, &query, "address.html", &ctx)
}

pub async fn token_page(
    State(state): State<AppState>,
    Path(address): Path<String>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let checksummed = checksum_address(&address);
    if !is_valid_address(&checksummed) {
        return invalid_address(&state, &headers, &query, "Token", &address);
    }

    // A corrupt row (NUL/control chars from the pre-fix decoder) is treated
    // as missing so the page re-fetches clean metadata on the spot.
    let meta = match db::get_token_metadata(&state.db, &checksummed) {
        Some(m) if !has_control_chars(&m.name) && !has_control_chars(&m.symbol) => m,
        _ => {
            let fetched = fetch_token_metadata(&state.rpc, &checksummed).await;
            let _ = db::save_token_metadata(&state.db, &fetched);
            db::get_token_metadata(&state.db, &checksummed).unwrap_or_else(|| {
                // Fall back to a minimal descriptor if the save failed.
                crate::models::TokenMetadata {
                    address: checksummed.clone(),
                    name: fetched.name,
                    symbol: fetched.symbol,
                    decimals: fetched.decimals,
                    currency: fetched.currency,
                    total_supply: fetched.total_supply,
                    logo_uri: String::new(),
                    holder_count: 0,
                    created_at: db::now_ts(),
                    updated_at: db::now_ts(),
                }
            })
        }
    };

    // The token page only has a Transfers tab (token.html), so default to it
    // instead of "transactions" — otherwise the home page renders an empty list.
    let tab = query
        .get("tab")
        .cloned()
        .unwrap_or_else(|| "transfers".into());
    let page = page_param(&query);
    let per_page = PER_PAGE;
    let transfers = if tab == "transfers" {
        db::get_token_transfers(&state.db, &checksummed, page, per_page)
    } else {
        Vec::new()
    };

    let holders = db::get_token_holder_count(&state.db, &checksummed);
    let transfer_count = db::get_token_transfer_count(&state.db, &checksummed);
    // The balances are already indexed — the page just never showed them.
    let holder_rows = if tab == "holders" {
        token_holders(&state, &checksummed, &meta, page, per_page)
    } else {
        Vec::new()
    };
    let total_pages = total_pages(
        if tab == "holders" {
            holders
        } else {
            transfer_count
        },
        per_page,
    );
    let ctx = page_ctx(
        &state,
        json!({
            "token": meta,
            "transfers": transfers,
            "holder_rows": holder_rows,
            "holders": holders,
            "transfer_count": transfer_count,
            "page": page,
            "total_pages": total_pages,
            "per_page": per_page,
            "active_tab": tab,
        }),
    );
    html_or_json(&state, &headers, &query, "token.html", &ctx)
}

pub async fn tokens_page(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let page = page_param(&query);
    let per_page = PER_PAGE;
    let tokens = db::get_all_tokens(&state.db, page, per_page);
    let total = db::get_token_count(&state.db);
    let total_pages = total_pages(total, per_page);
    let ctx = page_ctx(
        &state,
        json!({
            "tokens": tokens,
            "total": total,
            "page": page,
            "total_pages": total_pages,
            "per_page": per_page,
        }),
    );
    html_or_json(&state, &headers, &query, "tokens.html", &ctx)
}

// ---------------------------------------------------------------------------
// Anchoring
// ---------------------------------------------------------------------------

/// The anchoring contract's registries, newest first; or, given `q`, the registries with that
/// exact name and the records with that checksum. A browser asking for an id is sent to it.
pub async fn anchoring_page(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let q = query.get("q").map_or("", |q| q.trim());
    if !q.is_empty() && q.chars().all(|c| c.is_ascii_digit()) && !wants_json(&headers, &query) {
        return Redirect::to(&format!("/anchoring/{q}")).into_response();
    }
    let page = page_param(&query);
    let found = if q.is_empty() {
        anchoring::registries(&state.rpc, page.into(), PER_PAGE.into())
            .await
            .map(|(registries, total)| {
                json!({
                    "registries": registries,
                    "total": total,
                    "page": page,
                    "total_pages": total_pages(total as i64, PER_PAGE),
                })
            })
    } else {
        anchoring::lookup(&state.rpc, q)
            .await
            .map(|(named, records)| json!({"registries": named, "records": records}))
    };
    match found {
        Ok(mut extra) => {
            extra["address"] = json!(anchoring::ADDRESS);
            extra["q"] = json!(q);
            let ctx = page_ctx(&state, extra);
            html_or_json(&state, &headers, &query, "anchoring.html", &ctx)
        }
        Err(e) => node_failed(&state, &headers, &query, e),
    }
}

/// One registry, and the latest version of each of its records, newest first.
pub async fn anchoring_registry_page(
    State(state): State<AppState>,
    Path(registry_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let Ok(id) = registry_id.parse::<u64>() else {
        return not_found(&state, &headers, &query, "Registry", &registry_id);
    };
    let page = page_param(&query);
    match anchoring::registry(&state.rpc, id, page.into(), PER_PAGE.into()).await {
        Ok(Some((registry, records, total))) => {
            let ctx = page_ctx(
                &state,
                json!({
                    "registry": registry,
                    "records": records,
                    "total": total,
                    "page": page,
                    "total_pages": total_pages(total as i64, PER_PAGE),
                }),
            );
            html_or_json(&state, &headers, &query, "anchoring_registry.html", &ctx)
        }
        Ok(None) => not_found(&state, &headers, &query, "Registry", &registry_id),
        Err(e) => node_failed(&state, &headers, &query, e),
    }
}

/// One record: its latest version, and every version newest first.
pub async fn anchoring_record_page(
    State(state): State<AppState>,
    Path((registry_id, record_id)): Path<(String, String)>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let id = format!("{registry_id}/{record_id}");
    let (Ok(registry_id), Ok(record_id)) = (registry_id.parse::<u64>(), record_id.parse::<u64>())
    else {
        return not_found(&state, &headers, &query, "Record", &id);
    };
    let page = page_param(&query);
    match anchoring::record(
        &state.rpc,
        registry_id,
        record_id,
        page.into(),
        PER_PAGE.into(),
    )
    .await
    {
        Ok(Some((latest, versions))) => {
            let ctx = page_ctx(
                &state,
                json!({
                    "latest": latest,
                    "versions": versions,
                    "page": page,
                    "total_pages": total_pages(latest.index as i64, PER_PAGE),
                }),
            );
            html_or_json(&state, &headers, &query, "anchoring_record.html", &ctx)
        }
        Ok(None) => not_found(&state, &headers, &query, "Record", &id),
        Err(e) => node_failed(&state, &headers, &query, e),
    }
}

/// A page the node answers rather than the index, when the node did not.
fn node_failed(
    state: &AppState,
    headers: &HeaderMap,
    query: &HashMap<String, String>,
    err: anyhow::Error,
) -> Response {
    tracing::warn!("anchoring read failed: {err:#}");
    let page = if wants_json(headers, query) {
        Json(json!({"error": format!("{err:#}")})).into_response()
    } else {
        render_html(&state.tera, "500.html", &page_ctx(state, json!({})))
    };
    (StatusCode::BAD_GATEWAY, page).into_response()
}

/// An address-shaped path segment that is not an address — a token, an
/// account — said the way the client asked to hear it.
fn invalid_address(
    state: &AppState,
    headers: &HeaderMap,
    query: &HashMap<String, String>,
    kind: &str,
    address: &str,
) -> Response {
    if wants_json(headers, query) {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("Invalid address: {address}")})),
        )
            .into_response()
    } else {
        not_found_html(state, kind, address, "Invalid address")
    }
}

/// One destination the search box can send the reader to.
fn destination(kind: &str, id: &str, url: String) -> Value {
    json!({ "type": kind, "id": id, "url": url })
}

/// Where `q` should land, or `None` when nothing on the chain answers to it.
///
/// The candidates are tried in the order a reader means them. A block hash and
/// a transaction hash are the same shape, so only asking the index tells them
/// apart — and an address is answered whether or not anything has touched it,
/// since being told "no transactions" beats being told "not found".
fn resolve_search(db: &Db, q: &str) -> Option<Value> {
    let block = |number: i64| destination("block", &number.to_string(), format!("/block/{number}"));
    let token = |meta: crate::models::TokenMetadata| {
        destination("token", &meta.address, format!("/token/{}", meta.address))
    };
    // Digits only, so `parse` cannot take a sign the reader did not type.
    let height = (!q.is_empty() && q.chars().all(|c| c.is_ascii_digit()))
        .then(|| q.parse::<i64>().ok())
        .flatten();

    height
        .filter(|n| db::get_block_by_number(db, *n).is_some())
        // Spelled as the reader typed it, leading zeros and all.
        .map(|_| destination("block", q, format!("/block/{q}")))
        .or_else(|| {
            db::get_transaction(db, q).map(|_| destination("transaction", q, format!("/tx/{q}")))
        })
        .or_else(|| db::get_block_by_hash(db, q).map(|b| block(b.number)))
        .or_else(|| {
            let checksummed = checksum_address(q);
            is_valid_address(&checksummed)
                .then(|| destination("address", &checksummed, format!("/address/{checksummed}")))
        })
        .or_else(|| db::get_token_metadata(db, q).map(token))
        // Exact symbol or name first, then the best partial match — so pressing
        // Enter lands where the suggestions said it would.
        .or_else(|| db::get_token_by_symbol_or_name(db, q).map(token))
        .or_else(|| db::search_tokens(db, q, 1).into_iter().next().map(token))
}

pub async fn search_page(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let q = query.get("q").cloned().unwrap_or_default();
    let q = q.trim().to_string();
    if q.is_empty() {
        if wants_json(&headers, &query) {
            return Json(json!({"query": q, "match": Value::Null})).into_response();
        }
        return Redirect::to("/").into_response();
    }

    let found = match resolve_search(&state.db, &q) {
        found @ Some(_) => found,
        // A registry's name and a record's checksum are the contract's state, which no
        // index here holds, so they are only known by asking it.
        None => anchoring_hits(&state, &q, 1)
            .await
            .first()
            .map(|hit| destination(hit.kind, &hit.id, hit.url.clone())),
    };

    if wants_json(&headers, &query) {
        return Json(json!({"query": q, "match": found})).into_response();
    }
    if let Some(m) = found {
        let url = m
            .get("url")
            .and_then(Value::as_str)
            .unwrap_or("/")
            .to_string();
        return Redirect::to(&url).into_response();
    }
    let ctx = page_ctx(&state, json!({"query": q, "results": []}));
    render_html(&state.tera, "search.html", &ctx)
}

/// Enough suggestions to be useful, few enough to read without scrolling.
/// Somewhere the anchoring contract can send the reader.
struct Hit {
    kind: &'static str,
    id: String,
    url: String,
    label: String,
    sublabel: String,
}

impl Hit {
    fn registry(id: u64, name: String) -> Self {
        Self {
            kind: "registry",
            id: id.to_string(),
            url: format!("/anchoring/{id}"),
            label: name,
            sublabel: format!("Registry #{id}"),
        }
    }

    fn record(record: &anchoring::Record) -> Self {
        Self {
            kind: "record",
            id: record.checksum.clone(),
            url: format!("/anchoring/{}/{}", record.registryId, record.recordId),
            label: record.checksum.clone(),
            sublabel: format!("Record in registry #{}", record.registryId),
        }
    }
}

/// What the contract knows about `q`, best first: a registry by its whole name, a record by
/// its checksum, then registries whose name contains it, which only the index beside the
/// chain matches. Empty when neither knows it, or cannot be reached.
async fn anchoring_hits(state: &AppState, q: &str, limit: usize) -> Vec<Hit> {
    let mut hits = Vec::new();
    if let Ok((registries, records)) = anchoring::lookup(&state.rpc, q).await {
        hits.extend(
            registries
                .iter()
                .map(|r| Hit::registry(r.id, r.name.clone())),
        );
        hits.extend(records.iter().map(Hit::record));
    }
    if hits.len() < limit {
        if let Some(base) = state.cfg.name_search_url.as_deref() {
            for named in name_search::matching(state.rpc.http_client(), base, q, limit).await {
                let hit = Hit::registry(named.id, named.name);
                if !hits.iter().any(|seen| seen.url == hit.url) {
                    hits.push(hit);
                }
            }
        }
    }
    hits.truncate(limit);
    hits
}

const SUGGESTION_LIMIT: usize = 8;

/// Suggestions for what the reader is typing, for the search box.
///
/// Answered from the index, apart from the anchoring contract's registries and records,
/// which are its state: a name-shaped query asks the contract, and the name index when one
/// is configured.
/// Anything that is definitely a hash or an address is offered whether indexed
/// or not: being told "not found" on the page beats no suggestion at all.
pub async fn search_suggest(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let raw = query.get("q").cloned().unwrap_or_default();
    let q = raw.trim();
    if q.is_empty() {
        return suggest_response(&raw, Vec::new());
    }

    let mut results: Vec<Value> = Vec::new();
    // `type` is both the routing hint and the label on the row, so a
    // precompile says so rather than calling itself an address.
    let suggestion = |kind: &str, url: String, label: String, sublabel: String| json!({"type": kind, "url": url, "label": label, "sublabel": sublabel});

    // A block number, if the chain has reached it. The digit check keeps
    // `parse` from accepting a sign the reader did not mean as a number.
    let number = q.strip_prefix('#').unwrap_or(q);
    if number.chars().all(|c| c.is_ascii_digit()) {
        if let Some(block) = number
            .parse::<i64>()
            .ok()
            .and_then(|n| db::get_block_by_number(&state.db, n))
        {
            results.push(suggestion(
                "block",
                format!("/block/{}", block.number),
                format!("Block #{}", block.number),
                format!("{} transactions", block.tx_count),
            ));
        }
    }

    let checksummed = checksum_address(q);
    if is_valid_address(&checksummed) {
        let label = address_label(&state.db, &checksummed);
        // A token address goes to the token page, where the reader can
        // actually see the supply and the holders.
        if db::get_token_metadata(&state.db, &checksummed).is_some() {
            results.push(suggestion(
                "token",
                format!("/token/{checksummed}"),
                label.clone().unwrap_or_else(|| "Token".into()),
                checksummed.clone(),
            ));
        }
        let virtual_note = parse_virtual(&checksummed)
            .map(|parts| format!("Virtual address · user tag {}", parts.user_tag));
        results.push(suggestion(
            "address",
            format!("/address/{checksummed}"),
            label.unwrap_or_else(|| "Address".into()),
            virtual_note.unwrap_or_else(|| checksummed.clone()),
        ));
    } else if is_hash(q) {
        // 32 bytes: a transaction hash, or a block hash.
        if let Some(block) = db::get_block_by_hash(&state.db, q) {
            results.push(suggestion(
                "block",
                format!("/block/{}", block.number),
                format!("Block #{}", block.number),
                q.to_string(),
            ));
        } else {
            let indexed = db::get_transaction(&state.db, q);
            results.push(suggestion(
                "transaction",
                format!("/tx/{q}"),
                "Transaction".into(),
                match &indexed {
                    Some(tx) => format!("Block #{}", tx.block_number),
                    None => "Not indexed yet".into(),
                },
            ));
        }
    } else {
        // A name: tokens the index knows, then the built-in contracts.
        for meta in db::search_tokens(&state.db, q, SUGGESTION_LIMIT as u32) {
            results.push(suggestion(
                "token",
                format!("/token/{}", meta.address),
                if meta.name.is_empty() {
                    meta.symbol.clone()
                } else {
                    meta.name.clone()
                },
                format!("{} · {}", meta.symbol, truncate_hash(&meta.address, 8, 6)),
            ));
        }
        for hit in anchoring_hits(&state, q, SUGGESTION_LIMIT).await {
            results.push(suggestion(hit.kind, hit.url, hit.label, hit.sublabel));
        }
        for (address, name) in search_named(q, SUGGESTION_LIMIT) {
            results.push(suggestion(
                "precompile",
                format!("/address/{address}"),
                name,
                address,
            ));
        }
    }

    results.truncate(SUGGESTION_LIMIT);
    suggest_response(&raw, results)
}

/// The suggestions as JSON, cacheable for a moment: backspacing re-asks the
/// queries just asked, and the browser can answer those itself.
fn suggest_response(query: &str, results: Vec<Value>) -> Response {
    (
        [(header::CACHE_CONTROL, "private, max-age=15")],
        Json(json!({"query": query, "results": results})),
    )
        .into_response()
}

/// Whether `value` is 32 hex-encoded bytes — a transaction or block hash.
fn is_hash(value: &str) -> bool {
    value
        .strip_prefix("0x")
        .is_some_and(|hex| hex.len() == 64 && hex.chars().all(|c| c.is_ascii_hexdigit()))
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

fn not_found(
    state: &AppState,
    headers: &HeaderMap,
    query: &HashMap<String, String>,
    kind: &str,
    id: &str,
) -> Response {
    if wants_json(headers, query) {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("{kind} not found")})),
        )
            .into_response()
    } else {
        not_found_html(state, kind, id, &format!("{kind} not found"))
    }
}

fn not_found_html(state: &AppState, kind: &str, id: &str, message: &str) -> Response {
    // Through page_ctx like any other page: 404.html extends the layout, so a
    // hand-built context renders nothing and the reader gets bare text.
    let ctx = page_ctx(state, json!({"type": kind, "id": id, "message": message}));
    match tera::Context::from_serialize(&ctx) {
        Ok(tera_ctx) => match state.tera.render("404.html", &tera_ctx) {
            Ok(html) => (StatusCode::NOT_FOUND, Html(html)).into_response(),
            Err(e) => {
                tracing::error!("404 template render failed: {e}");
                (StatusCode::NOT_FOUND, Html(message.to_string())).into_response()
            }
        },
        Err(e) => {
            tracing::error!("404 context failed: {e}");
            (StatusCode::NOT_FOUND, Html(message.to_string())).into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub use crate::decoder::{is_valid_address, truncate_hash};

fn parse_hex_i64(s: &str) -> i64 {
    if let Some(h) = s.strip_prefix("0x") {
        i64::from_str_radix(h, 16).unwrap_or(0)
    } else {
        s.parse().unwrap_or(0)
    }
}

fn block_pct(gas_used: i64, gas_limit: i64) -> String {
    if gas_limit > 0 {
        format!("{:.1}", gas_used as f64 / gas_limit as f64 * 100.0)
    } else {
        "0".into()
    }
}

fn format_block_time(ms: f64) -> String {
    if ms >= 1000.0 {
        format!("{:.2}s", ms / 1000.0)
    } else {
        format!("{ms:.0} ms")
    }
}

/// `1234567` → `1,234,567` for display.
fn comma_num(n: i64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// Attach token symbol/decimals + formatted amounts to transfer rows.
fn enrich_transfers(state: &AppState, transfers: &mut [Value]) {
    let addrs: Vec<String> = transfers
        .iter()
        .filter_map(|t| {
            t.get("token_addr")
                .and_then(Value::as_str)
                .map(String::from)
        })
        .collect();
    let metas = db::get_tokens_metadata(&state.db, &addrs);
    for t in transfers.iter_mut() {
        let Some(addr) = t.get("token_addr").and_then(Value::as_str) else {
            continue;
        };
        let amount = t
            .get("amount")
            .and_then(Value::as_str)
            .unwrap_or("0")
            .to_string();
        let (symbol, decimals) = metas
            .get(addr)
            .map(|m| (m.symbol.clone(), m.decimals))
            .unwrap_or_else(|| (String::new(), 18));
        t["token_symbol"] = json!(symbol);
        t["token_decimals"] = json!(decimals);
        t["amount_formatted"] = json!(format_token_amount(&amount, decimals));
    }
}

// ---------------------------------------------------------------------------
// Tera setup
// ---------------------------------------------------------------------------

pub fn build_tera(db: Db) -> Result<Arc<Tera>> {
    let mut tera = Tera::new("templates/**/*.html").context("load templates")?;

    tera.register_filter(
        "to_checksum",
        |value: &Value, _: &HashMap<String, Value>| {
            Ok(Value::String(checksum_address(
                value.as_str().unwrap_or(""),
            )))
        },
    );
    tera.register_filter(
        "timestamp_to_date",
        |value: &Value, _: &HashMap<String, Value>| {
            let ts = value.as_i64().unwrap_or(0);
            let formatted = Local
                .timestamp_opt(ts, 0)
                .single()
                .map(|dt| dt.format("%m/%d/%y %H:%M:%S").to_string())
                .unwrap_or_else(|| "unknown".into());
            Ok(Value::String(formatted))
        },
    );
    // Use the fallback when the value is null or an empty string (Tera's
    // built-in `default` only handles null/undefined).
    tera.register_filter(
        "fallback",
        |value: &Value, args: &HashMap<String, Value>| {
            let empty = value.is_null() || value.as_str().map(|s| s.is_empty()).unwrap_or(false);
            if empty {
                Ok(args.get("value").cloned().unwrap_or(Value::Null))
            } else {
                Ok(value.clone())
            }
        },
    );

    // Thousands separators, matching what the live stats stream renders in the
    // browser: a tile must not change shape the first time a tick lands.
    tera.register_filter("comma", |value: &Value, _: &HashMap<String, Value>| {
        Ok(match value.as_i64() {
            Some(n) => Value::String(comma_num(n)),
            None => value.clone(),
        })
    });

    tera.register_function("truncate_hash", |args: &HashMap<String, Value>| {
        let h = args.get("h").and_then(Value::as_str).unwrap_or("");
        let prefix = args.get("prefix").and_then(Value::as_i64).unwrap_or(8) as usize;
        let suffix = args.get("suffix").and_then(Value::as_i64).unwrap_or(4) as usize;
        Ok(Value::String(truncate_hash(h, prefix, suffix)))
    });
    tera.register_function("format_time_ago", |args: &HashMap<String, Value>| {
        let ts = args.get("timestamp").and_then(Value::as_i64).unwrap_or(0);
        Ok(Value::String(format_time_ago(ts)))
    });
    let block_db = db.clone();
    tera.register_function("get_block_url", move |args: &HashMap<String, Value>| {
        let id = args.get("block_id").and_then(Value::as_str).unwrap_or("");
        let url = if id.chars().all(|c| c.is_ascii_digit()) && !id.is_empty() {
            format!("/block/{id}")
        } else {
            db::get_block_by_hash(&block_db, id)
                .map(|b| format!("/block/{}", b.number))
                .unwrap_or_else(|| format!("/block/{id}"))
        };
        Ok(Value::String(url))
    });
    tera.register_function("get_tx_url", |args: &HashMap<String, Value>| {
        let h = args.get("tx_hash").and_then(Value::as_str).unwrap_or("");
        Ok(Value::String(format!("/tx/{h}")))
    });
    tera.register_function("get_address_url", |args: &HashMap<String, Value>| {
        let a = args.get("address").and_then(Value::as_str).unwrap_or("");
        let url = if is_valid_address(&checksum_address(a)) {
            format!("/address/{}", checksum_address(a))
        } else {
            format!("/address/{a}")
        };
        Ok(Value::String(url))
    });
    // What the chain knows an address as, for the tag beside it.
    let label_db = db;
    tera.register_function("address_label", move |args: &HashMap<String, Value>| {
        let address = args.get("address").and_then(Value::as_str).unwrap_or("");
        Ok(match address_label(&label_db, address) {
            Some(label) => Value::String(label),
            None => Value::Null,
        })
    });
    tera.register_function("get_token_url", |args: &HashMap<String, Value>| {
        let a = args.get("address").and_then(Value::as_str).unwrap_or("");
        let url = if is_valid_address(&checksum_address(a)) {
            format!("/token/{}", checksum_address(a))
        } else {
            format!("/token/{a}")
        };
        Ok(Value::String(url))
    });
    tera.register_function("format_token_amount", |args: &HashMap<String, Value>| {
        let amount = args.get("amount").and_then(Value::as_str).unwrap_or("0");
        let decimals = args.get("decimals").and_then(Value::as_i64).unwrap_or(18);
        Ok(Value::String(format_token_amount(amount, decimals)))
    });
    tera.register_function(
        "format_token_amount_with_symbol",
        |args: &HashMap<String, Value>| {
            let amount = args.get("amount").and_then(Value::as_str).unwrap_or("0");
            let decimals = args.get("decimals").and_then(Value::as_i64).unwrap_or(18);
            let symbol = args.get("symbol").and_then(Value::as_str).unwrap_or("");
            Ok(Value::String(format_token_amount_with_symbol(
                amount, decimals, symbol,
            )))
        },
    );

    Ok(Arc::new(tera))
}

pub fn format_time_ago(ts: i64) -> String {
    let now = chrono::Utc::now().timestamp();
    let diff = now - ts;
    if diff < 0 {
        "just now".into()
    } else if diff < 60 {
        format!("{diff}s ago")
    } else if diff < 3600 {
        format!("{}m ago", diff / 60)
    } else if diff < 86400 {
        format!("{}h ago", diff / 3600)
    } else if diff < 2592000 {
        format!("{}d ago", diff / 86400)
    } else if diff < 31536000 {
        format!("{}mo ago", diff / 2592000)
    } else {
        format!("{}y ago", diff / 31536000)
    }
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/", get(home))
        .route("/api/events", get(events))
        .route("/block/{block_id}", get(block_page))
        .route("/blocks", get(blocks_page))
        .route("/txs", get(txs_page))
        .route("/tx/{tx_hash}", get(tx_page))
        .route("/receipt/{tx_hash}", get(receipt_page))
        .route("/address/{address}", get(address_page))
        .route("/token/{address}", get(token_page))
        .route("/tokens", get(tokens_page))
        .route("/anchoring", get(anchoring_page))
        .route("/anchoring/{registry_id}", get(anchoring_registry_page))
        .route(
            "/anchoring/{registry_id}/{record_id}",
            get(anchoring_record_page),
        )
        .route("/search", get(search_page))
        .route("/api/search", get(search_suggest))
        // Public explorer: allow cross-origin reads from any site (the wallet
        // is hosted on a different origin and needs `?format=json`).
        .layer(CorsLayer::permissive())
        .with_state(state)
}
