//! SQLite queries added since the move to `db/sqlite.rs`. New queries go here
//! rather than into `sqlite.rs`, so the bodies teammates edit stay as they are.

use anyhow::{Context, Result};

use super::{blob_addr, get_min_block_number, lock, row_to_token, Db, TOKEN_COLS};
use crate::models::TokenMetadata;

/// `get_min_block_number` as a `Result`, for the backfill loop. SQLite has no
/// outage to tell apart from an empty table, so this never fails.
pub fn try_min_block_number(db: &Db) -> Result<Option<i64>> {
    Ok(get_min_block_number(db))
}

/// What a web replica's follower polls: the newest block, when the stats
/// were last written, and the schema version. One statement; an error is
/// returned, so the follower retries rather than taking it for "nothing new".
pub fn follow_point(db: &Db) -> Result<(Option<i64>, Option<i64>, Option<i64>)> {
    lock(db)
        .query_row(
            "SELECT (SELECT MAX(number) FROM blocks),
                    (SELECT updated_at FROM kv WHERE key = 'stats'),
                    (SELECT MAX(version) FROM schema_migrations)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .context("follow_point")
}

/// Token addresses a transfer or a fee names that have no metadata row, or
/// why they could not be read: a failed scan is not "none missing".
pub fn tokens_missing_metadata(db: &Db) -> Result<Vec<String>> {
    try_rows(
        db,
        "tokens_missing_metadata",
        "SELECT a FROM (
             SELECT token_addr AS a FROM transfer_events
             UNION
             SELECT fee_token FROM transactions WHERE fee_token IS NOT NULL
         ) used
         WHERE NOT EXISTS (SELECT 1 FROM token_metadata m WHERE m.address = used.a)",
        |r| Ok(blob_addr(&r.get::<_, Vec<u8>>(0)?)),
    )
}

/// Every token-metadata row, or why they could not be read. Unlike
/// `get_all_token_metas`, a failed read is an error, never an empty table.
pub fn try_all_token_metas(db: &Db) -> Result<Vec<TokenMetadata>> {
    try_rows(
        db,
        "try_all_token_metas",
        &format!("SELECT {TOKEN_COLS} FROM token_metadata"),
        row_to_token,
    )
}

/// Every row `sql` returns, mapped, or why the query failed. Undecodable rows
/// are dropped and logged, as `query_rows` does.
fn try_rows<T>(
    db: &Db,
    what: &'static str,
    sql: &str,
    map: impl FnMut(&rusqlite::Row) -> rusqlite::Result<T>,
) -> Result<Vec<T>> {
    let conn = lock(db);
    let mut stmt = conn.prepare(sql).context(what)?;
    let rows = stmt.query_map([], map).context(what)?;
    let mut out = Vec::new();
    let (mut dropped, mut first) = (0usize, None);
    for row in rows {
        match row {
            Ok(v) => out.push(v),
            Err(e) => {
                dropped += 1;
                first.get_or_insert_with(|| e.to_string());
            }
        }
    }
    if let Some(e) = first {
        tracing::warn!("{what}: dropped {dropped} undecodable row(s); first: {e}");
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::sqlite;

    fn temp_db() -> (tempfile::TempDir, Db) {
        let dir = tempfile::tempdir().unwrap();
        let db = sqlite::open(dir.path().join("extra.db").to_str().unwrap()).unwrap();
        (dir, db)
    }

    /// A token a transfer or a fee names, with no metadata row: the
    /// missing-metadata job's first pass.
    #[test]
    fn tokens_named_without_metadata_are_listed_once() {
        let (_dir, db) = temp_db();
        let named = crate::decoder::checksum_address("0x20c0000000000000000000000000000000000001");
        let fee = crate::decoder::checksum_address("0x20c0000000000000000000000000000000000002");
        let known = crate::decoder::checksum_address("0x20c0000000000000000000000000000000000003");
        let blob = |a: &str| hex::decode(&a[2..]).unwrap();
        let conn = sqlite::lock(&db);
        for (log_index, token) in [(0, &named), (1, &named), (2, &known)] {
            conn.execute(
                "INSERT INTO transfer_events (tx_hash, block_number, log_index, token_addr, from_addr, to_addr, amount)
                 VALUES (X'aa', 1, ?1, ?2, X'01', X'02', '1')",
                rusqlite::params![log_index, blob(token)],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO transactions (hash, block_number, from_addr, fee_token) VALUES (X'bb', 1, X'01', ?1)",
            [blob(&fee)],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO token_metadata (address) VALUES (?1)",
            [blob(&known)],
        )
        .unwrap();
        drop(conn);

        let mut missing = tokens_missing_metadata(&db).unwrap();
        missing.sort();
        let mut want = vec![named, fee];
        want.sort();
        assert_eq!(missing, want);
    }

    #[test]
    fn the_follow_point_is_the_newest_block_the_stats_time_and_the_version() {
        let (_dir, db) = temp_db();
        assert_eq!(
            follow_point(&db).unwrap(),
            (None, None, Some(crate::db::migrations::binary_version()))
        );
        sqlite::lock(&db)
            .execute_batch(
                "INSERT INTO blocks (number, hash, parent_hash, timestamp) VALUES (9, X'09', X'08', 0);
                 INSERT INTO kv (key, value, updated_at) VALUES ('stats', '{}', 123);",
            )
            .unwrap();
        let (block, stats, _) = follow_point(&db).unwrap();
        assert_eq!((block, stats), (Some(9), Some(123)));
    }

    /// The label cache seeds from this; an empty list would erase every label
    /// where a failed read must leave them be.
    #[test]
    fn a_failed_token_read_is_an_error_not_an_empty_list() {
        let (_dir, db) = temp_db();
        assert!(try_all_token_metas(&db).unwrap().is_empty());

        sqlite::lock(&db)
            .execute_batch("DROP TABLE token_metadata")
            .unwrap();
        assert!(try_all_token_metas(&db).is_err());
    }

    /// The missing-metadata job retries a scan that fails, so a failure must
    /// not read as "none missing".
    #[test]
    fn a_failed_missing_metadata_scan_is_an_error() {
        let (_dir, db) = temp_db();
        assert!(tokens_missing_metadata(&db).unwrap().is_empty());

        sqlite::lock(&db)
            .execute_batch("DROP TABLE token_metadata")
            .unwrap();
        assert!(tokens_missing_metadata(&db).is_err());
    }
}
