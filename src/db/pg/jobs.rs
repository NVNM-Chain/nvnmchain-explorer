//! The indexer's jobs on Postgres: stats, the derived-table repair, and the
//! genesis-balance scan. Set-based here, where `sqlite.rs` walks rows.
//!
//! Postgres has no Keccak-256, so the checksummed text keys of
//! `token_balances` and `genesis_balances` are always made in Rust.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgConnection, Row};

use super::q;
use super::shared::blob_addr;
use super::writer::{Budget, TxFuture};
use super::{DbError, PgDb};
use crate::db::{now_ts, Holder};
use crate::summary::ZERO_ADDRESS;

const GENESIS_CURSOR: &str = "genesis_balances_cursor";

/// The home page's numbers, from the counters the writer keeps and the blocks
/// of the last day, stored in `kv` to seed the next start.
pub(crate) async fn compute_and_store_stats(p: &PgDb) -> Result<Value> {
    let now = now_ts();
    let totals = q::try_query_opt(
        &p.read,
        "stats",
        "SELECT COALESCE((SELECT n FROM counters WHERE name = 'blocks'), 0),
                COALESCE((SELECT n FROM counters WHERE name = 'transactions'), 0),
                (SELECT COUNT(*) FROM token_metadata),
                (SELECT COALESCE(SUM(tx_count), 0)::int8 FROM blocks WHERE timestamp >= $1),
                (SELECT COUNT(*) FROM blocks WHERE timestamp >= $1),
                (SELECT MAX(number) FROM blocks),
                (SELECT value FROM kv WHERE key = 'chain_head')",
        |q| q.bind(now - 86400),
        |r| {
            Ok((
                r.try_get::<i64, _>(0)?,
                r.try_get::<i64, _>(1)?,
                r.try_get::<i64, _>(2)?,
                r.try_get::<i64, _>(3)?,
                r.try_get::<i64, _>(4)?,
                r.try_get::<Option<i64>, _>(5)?,
                r.try_get::<Option<String>, _>(6)?,
            ))
        },
    )
    .await?
    .context("stats: no row")?;
    let (total_blocks, total_txns, token_count, txns_24h, blocks_24h, latest_block, head) = totals;
    // The newest 100 blocks, by their millisecond timestamps.
    let window = q::try_query_opt(
        &p.read,
        "stats window",
        "SELECT COALESCE(MIN(timestamp_ms), 0), COALESCE(MAX(timestamp_ms), 0),
                COALESCE(SUM(tx_count), 0)::int8,
                COALESCE(SUM(CASE WHEN gas_limit > 0 THEN gas_used::float8 / gas_limit ELSE 0 END), 0)::float8,
                COALESCE(SUM(CASE WHEN gas_limit > 0 THEN 1 ELSE 0 END), 0)::float8,
                COUNT(*)
         FROM (SELECT timestamp_ms, tx_count, gas_used, gas_limit
               FROM blocks ORDER BY number DESC LIMIT $1) w",
        |q| q.bind(100_i64),
        |r| {
            Ok((
                r.try_get::<i64, _>(0)?,
                r.try_get::<i64, _>(1)?,
                r.try_get::<i64, _>(2)?,
                r.try_get::<f64, _>(3)?,
                r.try_get::<f64, _>(4)?,
                r.try_get::<i64, _>(5)?,
            ))
        },
    )
    .await?
    .unwrap_or((0, 0, 0, 0.0, 0.0, 0));
    let (min_ms, max_ms, tx_sum, gas_sum, gas_den, n) = window;
    let span_ms = (max_ms - min_ms).max(1) as f64;
    let avg_block_time_ms = if n > 1 { span_ms / (n - 1) as f64 } else { 0.0 };
    let tps = tx_sum as f64 / span_ms * 1000.0;
    let gas_util_pct = if gas_den > 0.0 {
        gas_sum / gas_den * 100.0
    } else {
        0.0
    };
    let chain_head: i64 = head.and_then(|v| v.parse().ok()).unwrap_or(0);
    let index_pct = if chain_head > 0 {
        (total_blocks as f64 / chain_head as f64 * 100.0).clamp(0.0, 100.0)
    } else {
        0.0
    };
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
        "updated_at": now,
    });
    super::write::set_kv(p, "stats", &stats.to_string()).await?;
    Ok(stats)
}

/// Bring derived tables in line with what the reads assume, on the `Long`
/// budget: rebuild `token_balances` when transfers exist without it, else
/// recount the holders.
pub(crate) async fn repair_derived_tables(p: &PgDb) {
    let state = q::try_query_opt(
        &p.read,
        "repair_derived_tables",
        "SELECT EXISTS(SELECT 1 FROM transfer_events), EXISTS(SELECT 1 FROM token_balances)",
        |q| q,
        |r| Ok((r.try_get::<bool, _>(0)?, r.try_get::<bool, _>(1)?)),
    )
    .await;
    let (has_transfers, has_balances) = match state {
        Ok(Some(s)) => s,
        Ok(None) => return,
        Err(e) => {
            tracing::warn!("repair_derived_tables: {e:#}");
            return;
        }
    };
    let Ok(writer) = p.writer() else {
        return;
    };
    let result = if has_transfers && !has_balances {
        writer
            .write(Budget::Long, |c| -> TxFuture<'_, ()> {
                Box::pin(rebuild_token_balances(c))
            })
            .await
            .map_err(|e| anyhow::anyhow!("token balance rebuild failed: {e}"))
    } else if has_balances {
        writer
            .write(Budget::Long, |c| -> TxFuture<'_, ()> {
                Box::pin(sync_holder_counts(c))
            })
            .await
            .map_err(|e| anyhow::anyhow!("holder count sync failed: {e}"))
    } else {
        Ok(())
    };
    if let Err(e) = result {
        tracing::warn!("{e:#}");
    }
}

/// `token_balances` from the transfer history and the genesis balances, with
/// `adjust_balance`'s rule: a zero balance has no row, a negative one stays.
/// One aggregate keyed on bytes; the keys are checksummed here.
pub(crate) async fn rebuild_token_balances(c: &mut PgConnection) -> Result<(), DbError> {
    q::exec(c, "rebuild", sqlx::query("DELETE FROM token_balances")).await?;
    let rows = q::fetch_all(
        c,
        "rebuild",
        sqlx::query(
            "SELECT tok, holder, SUM(d)::text FROM (
               SELECT token_addr tok, from_addr holder, -(amount::numeric) d FROM transfer_events
               UNION ALL SELECT token_addr, to_addr, amount::numeric FROM transfer_events
               UNION ALL SELECT decode(substr(token_addr, 3), 'hex'), decode(substr(holder_addr, 3), 'hex'),
                                balance::numeric FROM genesis_balances) s
             GROUP BY tok, holder HAVING SUM(d) <> 0",
        ),
    )
    .await?;
    let mut holders: HashMap<Vec<u8>, i64> = HashMap::new();
    let (mut tokens, mut owners, mut balances) = (Vec::new(), Vec::new(), Vec::new());
    for row in &rows {
        let (tok, holder, balance): (Vec<u8>, Vec<u8>, String) =
            (q::get(row, 0)?, q::get(row, 1)?, q::get(row, 2)?);
        if !balance.starts_with('-') {
            *holders.entry(tok.clone()).or_default() += 1;
        }
        tokens.push(blob_addr(&tok));
        owners.push(blob_addr(&holder));
        balances.push(balance);
    }
    let now = now_ts();
    for ((t, h), b) in tokens
        .chunks(10_000)
        .zip(owners.chunks(10_000))
        .zip(balances.chunks(10_000))
    {
        q::exec(
            c,
            "rebuild insert",
            sqlx::query(
                "INSERT INTO token_balances (token_addr, holder_addr, balance, updated_at) \
                 SELECT u.t, u.h, u.b, $4 FROM UNNEST($1::text[], $2::text[], $3::text[]) AS u(t, h, b)",
            )
            .bind(t.to_vec())
            .bind(h.to_vec())
            .bind(b.to_vec())
            .bind(now),
        )
        .await?;
    }
    // Every token the history touches gets its count, zero included, as the
    // per-row rebuild recounts each token it touched.
    let touched = q::fetch_all(
        c,
        "rebuild tokens",
        sqlx::query(
            "SELECT DISTINCT token_addr FROM transfer_events
             UNION SELECT decode(substr(token_addr, 3), 'hex') FROM genesis_balances",
        ),
    )
    .await?;
    let mut addrs = Vec::with_capacity(touched.len());
    let mut counts = Vec::with_capacity(touched.len());
    for row in &touched {
        let tok: Vec<u8> = q::get(row, 0)?;
        counts.push(holders.get(&tok).copied().unwrap_or(0));
        addrs.push(tok);
    }
    q::exec(
        c,
        "rebuild counts",
        sqlx::query(
            "UPDATE token_metadata m SET holder_count = u.n, updated_at = $3 \
             FROM UNNEST($1::bytea[], $2::int8[]) AS u(a, n) WHERE m.address = u.a",
        )
        .bind(addrs)
        .bind(counts)
        .bind(now),
    )
    .await?;
    tracing::info!("rebuilt token balances: {} row(s)", rows.len());
    Ok(())
}

/// Recount the holders of every token that has balance rows, in one statement.
pub(crate) async fn sync_holder_counts(c: &mut PgConnection) -> Result<(), DbError> {
    q::exec(
        c,
        "sync_holder_counts",
        sqlx::query(
            "UPDATE token_metadata m SET holder_count = s.n, updated_at = $1
             FROM (SELECT decode(substr(token_addr, 3), 'hex') AS a,
                          COUNT(*) FILTER (WHERE balance NOT LIKE '-%') AS n
                   FROM token_balances GROUP BY token_addr) s
             WHERE m.address = s.a",
        )
        .bind(now_ts()),
    )
    .await?;
    Ok(())
}

/// The holders in the next `limit` transfers past the cursor that have no
/// genesis balance yet, and the id the cursor moves to; `None` once caught up.
/// A failed cursor read is an error, never a cursor of 0.
pub(crate) async fn holders_without_genesis_balance(
    p: &PgDb,
    limit: i64,
) -> Result<Option<(i64, Vec<Holder>)>> {
    let cursor: i64 = q::try_query_opt(
        &p.read,
        "genesis cursor",
        "SELECT value FROM kv WHERE key = $1",
        |q| q.bind(GENESIS_CURSOR),
        |r| r.try_get::<String, _>(0),
    )
    .await?
    .and_then(|v| v.parse().ok())
    .unwrap_or(0);
    let page = q::try_query_rows(
        &p.read,
        "genesis transfers",
        "SELECT id, token_addr, from_addr, to_addr FROM transfer_events WHERE id > $1 ORDER BY id LIMIT $2",
        |q| q.bind(cursor).bind(limit),
        |r| {
            Ok((
                r.try_get::<i64, _>(0)?,
                r.try_get::<Vec<u8>, _>(1)?,
                r.try_get::<Vec<u8>, _>(2)?,
                r.try_get::<Vec<u8>, _>(3)?,
            ))
        },
    )
    .await?;
    let Some(last) = page.last().map(|r| r.0) else {
        return Ok(None);
    };
    let mut seen = HashSet::new();
    let mut candidates: Vec<Holder> = Vec::new();
    for (_, token, from, to) in &page {
        let token = blob_addr(token);
        for holder in [from, to] {
            let holder = (token.clone(), blob_addr(holder));
            if holder.1 != ZERO_ADDRESS && seen.insert(holder.clone()) {
                candidates.push(holder);
            }
        }
    }
    if candidates.is_empty() {
        return Ok(Some((last, candidates)));
    }
    let (tokens, holders): (Vec<String>, Vec<String>) = candidates.into_iter().unzip();
    let missing = q::try_query_rows(
        &p.read,
        "genesis anti-join",
        "SELECT u.t, u.h FROM UNNEST($1::text[], $2::text[]) WITH ORDINALITY AS u(t, h, i)
         WHERE NOT EXISTS (SELECT 1 FROM genesis_balances g WHERE g.token_addr = u.t AND g.holder_addr = u.h)
         ORDER BY u.i",
        |q| q.bind(tokens).bind(holders),
        |r| Ok((r.try_get::<String, _>(0)?, r.try_get::<String, _>(1)?)),
    )
    .await?;
    Ok(Some((last, missing)))
}
