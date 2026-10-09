//! Token metadata, holders and holdings.

use std::collections::HashMap;

use serde_json::{json, Value};
use sqlx::postgres::PgRow;
use sqlx::Row;

use super::q;
use super::shared::{blob_addr, hex_blob, holding, token_cols};
use super::PgDb;
use crate::db::page_offset;
use crate::models::TokenMetadata;

pub(crate) fn token(r: &PgRow) -> Result<TokenMetadata, sqlx::Error> {
    Ok(TokenMetadata {
        address: blob_addr(&r.try_get::<Vec<u8>, _>(0)?),
        name: r.try_get(1)?,
        symbol: r.try_get(2)?,
        decimals: r.try_get(3)?,
        currency: r.try_get(4)?,
        total_supply: r.try_get(5)?,
        logo_uri: r.try_get(6)?,
        holder_count: r.try_get(7)?,
        created_at: r.try_get(8)?,
        updated_at: r.try_get(9)?,
    })
}

pub(crate) async fn get_token_metadata(p: &PgDb, address: &str) -> Option<TokenMetadata> {
    let address = hex_blob(address);
    q::query_opt(
        &p.read,
        "get_token_metadata",
        concat!(
            "SELECT ",
            token_cols!(),
            " FROM token_metadata WHERE address = $1"
        ),
        |q| q.bind(address),
        token,
    )
    .await
}

/// Largest holder counts first; ties by address, so pages are stable.
pub(crate) async fn get_all_tokens(p: &PgDb, page: u32, per_page: u32) -> Vec<TokenMetadata> {
    let offset = page_offset(page, per_page);
    q::query_rows(
        &p.read,
        "get_all_tokens",
        concat!(
            "SELECT ",
            token_cols!(),
            " FROM token_metadata ORDER BY holder_count DESC, address LIMIT $1 OFFSET $2"
        ),
        |q| q.bind(i64::from(per_page)).bind(offset),
        token,
    )
    .await
}

pub(crate) async fn get_all_token_metas(p: &PgDb) -> Vec<TokenMetadata> {
    q::query_rows(
        &p.read,
        "get_all_token_metas",
        concat!("SELECT ", token_cols!(), " FROM token_metadata"),
        |q| q,
        token,
    )
    .await
}

/// Every row, or the error: the label cache must not take a failed read for an
/// empty table.
pub(crate) async fn try_all_token_metas(p: &PgDb) -> anyhow::Result<Vec<TokenMetadata>> {
    q::try_query_rows(
        &p.read,
        "try_all_token_metas",
        concat!("SELECT ", token_cols!(), " FROM token_metadata"),
        |q| q,
        token,
    )
    .await
}

pub(crate) async fn get_token_count(p: &PgDb) -> i64 {
    q::query_count(
        &p.read,
        "get_token_count",
        "SELECT COUNT(*) FROM token_metadata",
        |q| q,
    )
    .await
}

pub(crate) async fn get_token_transfer_count(p: &PgDb, token_addr: &str) -> i64 {
    let token_addr = hex_blob(token_addr);
    q::query_count(
        &p.read,
        "get_token_transfer_count",
        "SELECT COUNT(*) FROM transfer_events WHERE token_addr = $1",
        |q| q.bind(token_addr),
    )
    .await
}

/// Exact symbol or name, ASCII case folded as SQLite's `lower()` folds it: the
/// term is compared under `"C"`, whatever the database's default collation.
pub(crate) async fn get_token_by_symbol_or_name(p: &PgDb, term: &str) -> Option<TokenMetadata> {
    if term.contains('\0') {
        return None;
    }
    let term = term.to_string();
    q::query_opt(
        &p.read,
        "get_token_by_symbol_or_name",
        concat!(
            "SELECT ",
            token_cols!(),
            " FROM token_metadata \
             WHERE lower(symbol) = lower($1::text COLLATE \"C\") \
                OR lower(name) = lower($1::text COLLATE \"C\") \
             ORDER BY holder_count DESC, address LIMIT 1"
        ),
        |q| q.bind(term),
        token,
    )
    .await
}

/// The term, then `LIKE` patterns matching it anywhere and as a prefix, with
/// its own `%`, `_` and `\` escaped, as on SQLite. `None` under two characters.
fn search_term(q: &str) -> Option<(String, String, String)> {
    if q.len() < 2 {
        return None;
    }
    let escaped = q
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    Some((q.to_string(), format!("%{escaped}%"), format!("{escaped}%")))
}

/// Best matches first, as on SQLite: exact symbol, symbol prefix, name prefix,
/// then the rest. `ILIKE` under the columns' `"C"` collation folds ASCII case
/// only, as SQLite's `LIKE` does; ties fall back to the address.
pub(crate) async fn search_tokens(p: &PgDb, term: &str, limit: u32) -> Vec<TokenMetadata> {
    if term.contains('\0') {
        return Vec::new();
    }
    let Some((exact, anywhere, prefix)) = search_term(term) else {
        return Vec::new();
    };
    q::query_rows(
        &p.read,
        "search_tokens",
        concat!(
            "SELECT ",
            token_cols!(),
            " FROM token_metadata \
             WHERE symbol ILIKE $2 ESCAPE '\\' OR name ILIKE $2 ESCAPE '\\' \
             ORDER BY \
                 CASE \
                     WHEN lower(symbol) = lower($1::text COLLATE \"C\") THEN 0 \
                     WHEN symbol ILIKE $3 ESCAPE '\\' THEN 1 \
                     WHEN name ILIKE $3 ESCAPE '\\' THEN 2 \
                     ELSE 3 \
                 END, \
                 holder_count DESC, symbol, address \
             LIMIT $4"
        ),
        |q| {
            q.bind(exact)
                .bind(anywhere)
                .bind(prefix)
                .bind(i64::from(limit))
        },
        token,
    )
    .await
}

pub(crate) async fn get_tokens_metadata(
    p: &PgDb,
    addresses: &[String],
) -> HashMap<String, TokenMetadata> {
    if addresses.is_empty() {
        return HashMap::new();
    }
    let keys: Vec<Vec<u8>> = addresses.iter().map(|a| hex_blob(a)).collect();
    q::query_rows(
        &p.read,
        "get_tokens_metadata",
        concat!(
            "SELECT ",
            token_cols!(),
            " FROM token_metadata WHERE address = ANY($1)"
        ),
        |q| q.bind(keys),
        token,
    )
    .await
    .into_iter()
    .map(|m| (m.address.clone(), m))
    .collect()
}

pub(crate) async fn get_all_token_addresses(p: &PgDb) -> Vec<String> {
    q::query_rows(
        &p.read,
        "get_all_token_addresses",
        "SELECT address FROM token_metadata",
        |q| q,
        |r| Ok(blob_addr(&r.try_get::<Vec<u8>, _>(0)?)),
    )
    .await
}

pub(crate) async fn get_token_holder_count(p: &PgDb, token_addr: &str) -> i64 {
    let token_addr = token_addr.to_string();
    q::query_count(
        &p.read,
        "get_token_holder_count",
        concat!(
            "SELECT COUNT(*) FROM token_balances WHERE token_addr = $1 AND ",
            holding!()
        ),
        |q| q.bind(token_addr),
    )
    .await
}

/// Largest first, by length then text, as on SQLite, with the holder as the
/// tie-break so equal balances page in a stable order. The predicate matches
/// `idx_tb_holding`'s, so the planner can use it.
pub(crate) async fn get_token_holders(
    p: &PgDb,
    token_addr: &str,
    page: u32,
    per_page: u32,
) -> Vec<(String, String)> {
    let (token_addr, offset) = (token_addr.to_string(), page_offset(page, per_page));
    q::query_rows(
        &p.read,
        "get_token_holders",
        concat!(
            "SELECT holder_addr, balance FROM token_balances WHERE token_addr = $1 AND ",
            holding!(),
            " ORDER BY LENGTH(balance) DESC, balance DESC, holder_addr LIMIT $2 OFFSET $3"
        ),
        |q| q.bind(token_addr).bind(i64::from(per_page)).bind(offset),
        |r| {
            Ok((
                crate::decoder::checksum_address(&r.try_get::<String, _>(0)?),
                r.try_get(1)?,
            ))
        },
    )
    .await
}

/// What `address` holds, negative balances left out as on the holders page.
pub(crate) async fn get_address_holdings(p: &PgDb, address: &str) -> Vec<Value> {
    let holder = address.to_string();
    let balances: Vec<(String, String)> = q::query_rows(
        &p.read,
        "get_address_holdings",
        concat!(
            "SELECT token_addr, balance FROM token_balances WHERE holder_addr = $1 AND ",
            holding!()
        ),
        |q| q.bind(holder),
        |r| Ok((r.try_get(0)?, r.try_get(1)?)),
    )
    .await;
    let token_addrs: Vec<String> = balances.iter().map(|(a, _)| a.clone()).collect();
    let metas = get_tokens_metadata(p, &token_addrs).await;
    let mut holdings = Vec::new();
    for (token_addr, balance) in &balances {
        if let Some(meta) = metas.get(token_addr) {
            holdings.push(json!({
                "token": meta.address,
                "name": meta.name,
                "symbol": meta.symbol,
                "decimals": meta.decimals,
                "balance": balance,
                "formatted": crate::tokens::format_token_amount(balance, meta.decimals),
            }));
        }
    }
    holdings
}

/// Token addresses a transfer or a fee references with no metadata row, for
/// the missing-metadata job's scan, or why they could not be read: a failed
/// scan is not "none missing".
pub(crate) async fn tokens_missing_metadata(p: &PgDb) -> anyhow::Result<Vec<String>> {
    q::try_query_rows(
        &p.read,
        "tokens_missing_metadata",
        "SELECT a FROM (
             SELECT DISTINCT token_addr AS a FROM transfer_events
             UNION
             SELECT DISTINCT fee_token FROM transactions WHERE fee_token IS NOT NULL
         ) used
         WHERE NOT EXISTS (SELECT 1 FROM token_metadata m WHERE m.address = used.a)",
        |q| q,
        |r| Ok(blob_addr(&r.try_get::<Vec<u8>, _>(0)?)),
    )
    .await
}
