//! `/api/v1`: the pages' data as JSON, without the counts they take.

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use serde_json::{json, Value};

use super::{
    address_type, enrich_transfers, find_block, page_param, token_holders, AppState, PER_PAGE,
};
use crate::anchoring::{self, Record, Registry};
use crate::contracts::identify_address;
use crate::db::{self, TxColumns};
use crate::decoder::{checksum_address, decode_event, function_name, is_valid_address};
use crate::models::{Block, TokenMetadata, Transaction};

pub(super) fn routes() -> Router<AppState> {
    Router::new()
        .route("/blocks", get(blocks))
        .route("/blocks/{id}", get(block))
        .route("/txs", get(txs))
        .route("/txs/{hash}", get(tx))
        .route("/addresses/{address}", get(address))
        .route("/addresses/{address}/txs", get(address_txs))
        .route("/addresses/{address}/transfers", get(address_transfers))
        .route("/addresses/{address}/holdings", get(address_holdings))
        .route("/tokens", get(tokens))
        .route("/tokens/{address}", get(token))
        .route("/tokens/{address}/holders", get(holders))
        .route("/tokens/{address}/transfers", get(token_transfers))
        .route("/anchoring/registries", get(registries))
        .route("/anchoring/registries/{id}", get(registry))
        .route("/anchoring/registries/{id}/records", get(records))
        .route("/anchoring/registries/{id}/records/{record}", get(record))
        .route(
            "/anchoring/registries/{id}/records/{record}/versions",
            get(versions),
        )
}

#[derive(Serialize)]
struct List<T> {
    items: Vec<T>,
    next_page: Option<u32>,
}

/// One page of `PER_PAGE` rows: a full one may have another after it.
fn listing<T>(items: Vec<T>, page: u32) -> Json<List<T>> {
    let next_page = (items.len() == PER_PAGE as usize).then(|| page + 1);
    Json(List { items, next_page })
}

/// Answered as `{"error": message}` with its status.
struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}

/// A read the node did not answer.
impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        tracing::warn!("node read failed: {err:#}");
        ApiError(StatusCode::BAD_GATEWAY, format!("{err:#}"))
    }
}

fn not_found(kind: &str) -> ApiError {
    ApiError(StatusCode::NOT_FOUND, format!("{kind} not found"))
}

/// `address` checksummed, unless it is not an address.
fn checked(address: &str) -> Result<String, ApiError> {
    let checksummed = checksum_address(address);
    if is_valid_address(&checksummed) {
        Ok(checksummed)
    } else {
        let message = format!("Invalid address: {address}");
        Err(ApiError(StatusCode::BAD_REQUEST, message))
    }
}

/// A transaction less its stored blobs, with the method its input names.
fn tx_item(tx: &Transaction) -> Value {
    let mut item = json!(tx);
    if let Value::Object(fields) = &mut item {
        for blob in ["raw", "trace_data", "receipt_data"] {
            fields.remove(blob);
        }
    }
    item["method"] = json!(function_name(&tx.input));
    item
}

/// A page is 25 heights, so it holds fewer blocks where the index has gaps.
async fn blocks(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<List<Block>> {
    let page = page_param(&query);
    let per_page = i64::from(PER_PAGE);
    let end = db::get_max_block_number(&state.db).unwrap_or(-1) - i64::from(page - 1) * per_page;
    let start = (end - per_page + 1).max(0);
    let lowest = db::get_min_block_number(&state.db).unwrap_or(0);
    Json(List {
        items: db::get_blocks_in_range(&state.db, start, end),
        next_page: (start > lowest).then(|| page + 1),
    })
}

async fn block(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let block = find_block(&state.db, &id).ok_or_else(|| not_found("Block"))?;
    let txs = db::get_block_transactions(&state.db, block.number, TxColumns::List)
        .iter()
        .map(tx_item)
        .collect();
    let mut body = json!(block);
    body["transactions"] = Value::Array(txs);
    Ok(Json(body))
}

async fn txs(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<List<Value>> {
    let page = page_param(&query);
    let txs = db::get_transactions(&state.db, page, PER_PAGE)
        .iter()
        .map(tx_item)
        .collect();
    listing(txs, page)
}

/// No call tree: the page asks the node for that.
async fn tx(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let mut tx = db::get_transaction(&state.db, &hash).ok_or_else(|| not_found("Transaction"))?;
    // Taken out of the row first: `tx_item` would copy them only to drop them.
    let raw = tx.raw.take();
    let receipt: Value = tx
        .receipt_data
        .take()
        .and_then(|r| serde_json::from_str(&r).ok())
        .unwrap_or_default();
    tx.trace_data = None;
    let events: Vec<_> = receipt["logs"]
        .as_array()
        .map(|logs| logs.iter().filter_map(decode_event).collect())
        .unwrap_or_default();
    let mut body = tx_item(&tx);
    body["raw"] = raw.into();
    body["receipt"] = receipt;
    body["events"] = json!(events);
    Ok(Json(body))
}

async fn address(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let address = checked(&address)?;
    let info = identify_address(&address);
    let token = db::get_token_metadata(&state.db, &address);
    let (kind, _) = address_type(&state, &address, &info.kind, token.is_some(), false).await;
    let body = json!({"address": address, "type": kind, "label": info.label, "token": token});
    Ok(Json(body))
}

async fn address_txs(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<List<Value>>, ApiError> {
    let address = checked(&address)?;
    let page = page_param(&query);
    let txs = db::get_address_transactions(&state.db, &address, page, PER_PAGE, TxColumns::List)
        .iter()
        .map(tx_item)
        .collect();
    Ok(listing(txs, page))
}

async fn address_transfers(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<List<Value>>, ApiError> {
    let address = checked(&address)?;
    let page = page_param(&query);
    let mut transfers = db::get_address_transfers(&state.db, &address, page, PER_PAGE);
    enrich_transfers(&state, &mut transfers);
    Ok(listing(transfers, page))
}

/// Not paged: an address holds few enough tokens.
async fn address_holdings(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<List<Value>>, ApiError> {
    let items = db::get_address_holdings(&state.db, &checked(&address)?);
    Ok(Json(List {
        items,
        next_page: None,
    }))
}

async fn tokens(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<List<TokenMetadata>> {
    let page = page_param(&query);
    listing(db::get_all_tokens(&state.db, page, PER_PAGE), page)
}

fn indexed_token(state: &AppState, address: &str) -> Result<TokenMetadata, ApiError> {
    db::get_token_metadata(&state.db, &checked(address)?).ok_or_else(|| not_found("Token"))
}

async fn token(
    State(state): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<TokenMetadata>, ApiError> {
    Ok(Json(indexed_token(&state, &address)?))
}

async fn holders(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<List<Value>>, ApiError> {
    let meta = indexed_token(&state, &address)?;
    let page = page_param(&query);
    let holders = token_holders(&state, &meta.address, &meta, page, PER_PAGE);
    Ok(listing(holders, page))
}

async fn token_transfers(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<List<Value>>, ApiError> {
    let meta = indexed_token(&state, &address)?;
    let page = page_param(&query);
    let transfers = db::get_token_transfers(&state.db, &meta.address, page, PER_PAGE);
    Ok(listing(transfers, page))
}

/// The page after `page` while `total` rows run past it.
fn next_of(page: u32, total: u64) -> Option<u32> {
    (u64::from(page) * u64::from(PER_PAGE) < total).then(|| page + 1)
}

async fn registries(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<List<Registry>>, ApiError> {
    let page = page_param(&query);
    let (items, total) = anchoring::registries(&state.rpc, page.into(), PER_PAGE.into()).await?;
    Ok(Json(List {
        items,
        next_page: next_of(page, total),
    }))
}

async fn registry(
    State(state): State<AppState>,
    Path(registry_id): Path<u64>,
) -> Result<Json<Registry>, ApiError> {
    let (registry, _) = anchoring::find_registry(&state.rpc, registry_id)
        .await?
        .ok_or_else(|| not_found("Registry"))?;
    Ok(Json(registry))
}

async fn records(
    State(state): State<AppState>,
    Path(registry_id): Path<u64>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<List<Record>>, ApiError> {
    let page = page_param(&query);
    let (_, items, total) =
        anchoring::registry(&state.rpc, registry_id, page.into(), PER_PAGE.into())
            .await?
            .ok_or_else(|| not_found("Registry"))?;
    Ok(Json(List {
        items,
        next_page: next_of(page, total),
    }))
}

async fn record(
    State(state): State<AppState>,
    Path((registry_id, record_id)): Path<(u64, u64)>,
) -> Result<Json<Record>, ApiError> {
    let latest = anchoring::latest(&state.rpc, registry_id, record_id)
        .await?
        .ok_or_else(|| not_found("Record"))?;
    Ok(Json(latest))
}

async fn versions(
    State(state): State<AppState>,
    Path((registry_id, record_id)): Path<(u64, u64)>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<List<Record>>, ApiError> {
    let page = page_param(&query);
    let (latest, items) = anchoring::record(
        &state.rpc,
        registry_id,
        record_id,
        page.into(),
        PER_PAGE.into(),
    )
    .await?
    .ok_or_else(|| not_found("Record"))?;
    Ok(Json(List {
        items,
        next_page: next_of(page, latest.index),
    }))
}
