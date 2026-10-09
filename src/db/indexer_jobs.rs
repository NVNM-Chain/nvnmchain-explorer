//! Database work the indexer runs, kept beside the storage layer so nothing
//! outside `db` reaches the connection.

use anyhow::Result;
use rusqlite::params;
use serde_json::Value;
use tracing::warn;

use crate::db::sqlite::{self as db, Db};
use crate::models::AnchoringEvent;

/// Bring a database written by an older build back in line with what the read
/// paths assume. Every step is idempotent, so this runs on every start.
///
/// Each recompute is guarded rather than run unconditionally, and the lock is
/// taken per step: a rebuild reads whole tables, and holding the connection for
/// that long is long enough for page views to notice.
pub fn repair_derived_tables(db: &Db) {
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

pub fn compute_and_store_stats(db: &Db) -> Result<Value> {
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

    // Rolling window over the newest blocks (cheap PK scan, no history sweep).
    // Timestamps are ms-precision (`timestamp_ms`); the chain produces blocks
    // faster than once per second, so second-granularity timestamps would
    // quantize the block time to 1s.
    let window: i64 = 100;
    let (min_ms, max_ms, tx_sum, gas_sum, gas_den, n): (i64, i64, i64, f64, f64, i64) = conn
        .query_row(
            "SELECT MIN(timestamp_ms), MAX(timestamp_ms), SUM(tx_count),
                    SUM(CASE WHEN gas_limit > 0 THEN gas_used * 1.0 / gas_limit ELSE 0 END),
                    SUM(CASE WHEN gas_limit > 0 THEN 1 ELSE 0 END),
                    COUNT(*)
             FROM (SELECT timestamp_ms, tx_count, gas_used, gas_limit
                   FROM blocks ORDER BY number DESC LIMIT ?1)",
            params![window],
            |r| {
                Ok((
                    r.get::<_, Option<i64>>(0)?.unwrap_or(0),
                    r.get::<_, Option<i64>>(1)?.unwrap_or(0),
                    r.get::<_, Option<i64>>(2)?.unwrap_or(0),
                    r.get::<_, Option<f64>>(3)?.unwrap_or(0.0),
                    r.get::<_, Option<f64>>(4)?.unwrap_or(0.0),
                    r.get::<_, Option<i64>>(5)?.unwrap_or(0),
                ))
            },
        )
        .unwrap_or((0, 0, 0, 0.0, 0.0, 0));

    let span_ms = (max_ms - min_ms).max(1) as f64;
    let avg_block_time_ms = if n > 1 { span_ms / (n - 1) as f64 } else { 0.0 };
    let tps = if span_ms > 0.0 {
        tx_sum as f64 / span_ms * 1000.0
    } else {
        0.0
    };
    let gas_util_pct = if gas_den > 0.0 {
        gas_sum / gas_den * 100.0
    } else {
        0.0
    };
    let latest_block = conn.query_row("SELECT MAX(number) FROM blocks", [], |r| {
        r.get::<_, Option<i64>>(0)
    })?;

    // Index progress, so the home page and the stream never recount blocks.
    // Read through `conn`: a helper taking `&Db` would deadlock on the
    // connection lock this function already holds.
    let chain_head: i64 = conn
        .query_row("SELECT value FROM kv WHERE key='chain_head'", [], |r| {
            r.get::<_, String>(0)
        })
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
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
    // Still written to kv: it seeds the in-memory copy across a restart.
    db::set_kv(&conn, "stats", &stats.to_string())?;
    Ok(stats)
}

/// One backfill window, in one transaction: build the window's events with
/// `stamp` (a block's indexed timestamp, if it is indexed), insert them
/// (duplicates ignored), and advance the watermark at `key` to `value`, also
/// when there is nothing to insert. Returns the number of rows inserted.
pub fn save_anchoring_window(
    db: &Db,
    key: &str,
    value: &str,
    events: impl FnOnce(&dyn Fn(i64) -> Option<i64>) -> Vec<AnchoringEvent>,
) -> Result<usize> {
    let mut conn = db::lock(db);
    let txn = conn.transaction()?;
    let mut wrote = 0;
    for event in events(&|number| db::get_block_timestamp(&txn, number)) {
        wrote += usize::from(db::insert_anchoring(&txn, &event)?);
    }
    db::set_kv(&txn, key, value)?;
    txn.commit()?;
    Ok(wrote)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Block;

    fn temp_db() -> (tempfile::TempDir, Db) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = db::open(dir.path().join("jobs.db").to_str().unwrap()).expect("open");
        (dir, db)
    }

    fn block(number: i64, timestamp: i64) -> Block {
        Block {
            number,
            hash: format!("0x{number:064x}"),
            parent_hash: format!("0x{:064x}", number - 1),
            timestamp,
            timestamp_ms: timestamp * 1000,
            gas_used: 0,
            gas_limit: 0,
            base_fee: "0".into(),
            size: 0,
            extra_data: String::new(),
            epoch: 0,
            view: 0,
            proposer: format!("0x{:040x}", 0),
            miner: format!("0x{:040x}", 0),
            tx_count: 0,
            created_at: 0,
        }
    }

    fn event(block_number: i64, log_index: i64, timestamp: i64) -> AnchoringEvent {
        AnchoringEvent {
            tx_hash: format!("0x{block_number:064x}"),
            block_number,
            log_index,
            timestamp,
            event: "AddRecord".into(),
            registry_id: 1,
            record_id: 1,
            caller: format!("0x{:040x}", 1),
        }
    }

    /// The decoder's half of a window: one event per block the stamp knows.
    fn stamped(stamp: &dyn Fn(i64) -> Option<i64>, blocks: &[i64]) -> Vec<AnchoringEvent> {
        blocks
            .iter()
            .filter_map(|&n| Some(event(n, 0, stamp(n)?)))
            .collect()
    }

    #[test]
    fn a_window_stamps_from_indexed_blocks_and_skips_the_rest() {
        let (_dir, db) = temp_db();
        db::save_block(&db, &block(10, 1_700_000_000)).unwrap();

        let mut seen = Vec::new();
        let wrote = save_anchoring_window(&db, "watermark", "20", |stamp| {
            seen = vec![stamp(10), stamp(11)];
            stamped(stamp, &[10, 11])
        })
        .unwrap();

        assert_eq!(seen, vec![Some(1_700_000_000), None]);
        assert_eq!(wrote, 1);
        let rows = db::get_anchoring_events(&db, 1, 10);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].timestamp, 1_700_000_000);
        assert_eq!(db::get_kv(&db, "watermark").as_deref(), Some("20"));
    }

    #[test]
    fn a_repeated_window_inserts_nothing() {
        let (_dir, db) = temp_db();
        db::save_block(&db, &block(10, 1_700_000_000)).unwrap();
        let window = |stamp: &dyn Fn(i64) -> Option<i64>| stamped(stamp, &[10]);

        assert_eq!(
            save_anchoring_window(&db, "watermark", "20", window).unwrap(),
            1
        );
        assert_eq!(
            save_anchoring_window(&db, "watermark", "20", window).unwrap(),
            0
        );
        assert_eq!(db::get_anchoring_events(&db, 1, 10).len(), 1);
    }

    #[test]
    fn an_empty_window_still_advances_the_watermark() {
        let (_dir, db) = temp_db();

        let wrote = save_anchoring_window(&db, "watermark", "50000", |_| Vec::new()).unwrap();

        assert_eq!(wrote, 0);
        assert_eq!(db::get_kv(&db, "watermark").as_deref(), Some("50000"));
    }

    /// Production code reaches the database through `db`'s functions only, so
    /// a second backend is a change inside `db`. Upstream merges keep adding
    /// direct uses; this names each one.
    #[test]
    fn nothing_outside_db_touches_the_connection() {
        const LEAKS: [&str; 4] = ["rusqlite", "db::lock", "init_db", "Connection"];
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut found = Vec::new();
        let mut dirs = vec![src.clone()];
        while let Some(dir) = dirs.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path == src.join("db") {
                    continue;
                }
                if path.is_dir() {
                    dirs.push(path);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    let text = std::fs::read_to_string(&path).unwrap();
                    for (i, line) in text.lines().enumerate() {
                        if LEAKS.iter().any(|leak| line.contains(leak)) {
                            found.push(format!("{}:{}: {}", path.display(), i + 1, line.trim()));
                        }
                    }
                }
            }
        }
        assert!(
            found.is_empty(),
            "use a db:: function instead:\n{}",
            found.join("\n")
        );
    }
}
