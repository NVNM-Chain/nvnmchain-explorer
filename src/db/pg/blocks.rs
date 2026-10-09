//! Blocks, the kv store and the selector cache: reads, and the cache writes
//! web pages make.

use std::collections::HashMap;

use sqlx::postgres::PgRow;
use sqlx::Row;

use super::q;
use super::shared::{blob_hex, block_cols, hex_blob, without_nul};
use super::PgDb;
use crate::models::Block;

pub(crate) fn block(r: &PgRow) -> Result<Block, sqlx::Error> {
    Ok(Block {
        number: r.try_get(0)?,
        hash: blob_hex(&r.try_get::<Vec<u8>, _>(1)?),
        parent_hash: blob_hex(&r.try_get::<Vec<u8>, _>(2)?),
        timestamp: r.try_get(3)?,
        timestamp_ms: r.try_get(4)?,
        gas_used: r.try_get(5)?,
        gas_limit: r.try_get(6)?,
        base_fee: r.try_get(7)?,
        size: r.try_get(8)?,
        extra_data: r.try_get(9)?,
        epoch: r.try_get(10)?,
        view: r.try_get(11)?,
        proposer: blob_hex(&r.try_get::<Vec<u8>, _>(12)?),
        miner: blob_hex(&r.try_get::<Vec<u8>, _>(13)?),
        tx_count: r.try_get(14)?,
        created_at: r.try_get(15)?,
    })
}

pub(crate) async fn get_block_by_number(p: &PgDb, number: i64) -> Option<Block> {
    q::query_opt(
        &p.read,
        "get_block_by_number",
        concat!("SELECT ", block_cols!(), " FROM blocks WHERE number = $1"),
        |q| q.bind(number),
        block,
    )
    .await
}

pub(crate) async fn get_block_by_hash(p: &PgDb, hash: &str) -> Option<Block> {
    let hash = hex_blob(hash);
    q::query_opt(
        &p.read,
        "get_block_by_hash",
        concat!("SELECT ", block_cols!(), " FROM blocks WHERE hash = $1"),
        |q| q.bind(hash),
        block,
    )
    .await
}

pub(crate) async fn get_latest_block(p: &PgDb) -> Option<Block> {
    q::query_opt(
        &p.read,
        "get_latest_block",
        concat!(
            "SELECT ",
            block_cols!(),
            " FROM blocks ORDER BY number DESC LIMIT 1"
        ),
        |q| q,
        block,
    )
    .await
}

pub(crate) async fn get_min_block_number(p: &PgDb) -> Option<i64> {
    q::query_opt(
        &p.read,
        "get_min_block_number",
        "SELECT MIN(number) FROM blocks",
        |q| q,
        |r| r.try_get::<Option<i64>, _>(0),
    )
    .await
    .flatten()
}

/// `get_min_block_number`, but a failed read is an error: backfill must not
/// take an outage for an empty table and re-walk the chain from the head.
pub(crate) async fn try_min_block_number(p: &PgDb) -> anyhow::Result<Option<i64>> {
    Ok(q::try_query_opt(
        &p.read,
        "try_min_block_number",
        "SELECT MIN(number) FROM blocks",
        |q| q,
        |r| r.try_get::<Option<i64>, _>(0),
    )
    .await?
    .flatten())
}

pub(crate) async fn get_blocks_in_range(p: &PgDb, from: i64, to: i64) -> Vec<Block> {
    q::query_rows(
        &p.read,
        "get_blocks_in_range",
        concat!(
            "SELECT ",
            block_cols!(),
            " FROM blocks WHERE number BETWEEN $1 AND $2 ORDER BY number DESC"
        ),
        |q| q.bind(from).bind(to),
        block,
    )
    .await
}

pub(crate) async fn get_recent_blocks(p: &PgDb, limit: usize) -> Vec<Block> {
    q::query_rows(
        &p.read,
        "get_recent_blocks",
        concat!(
            "SELECT ",
            block_cols!(),
            " FROM blocks ORDER BY number DESC LIMIT $1"
        ),
        |q| q.bind(limit as i64),
        block,
    )
    .await
}

/// The follower's one statement per tick: the newest block, when the stats
/// were last written, and the schema version.
pub(crate) async fn follow_point(
    p: &PgDb,
) -> anyhow::Result<(Option<i64>, Option<i64>, Option<i64>)> {
    q::try_query_opt(
        &p.read,
        "follow_point",
        "SELECT (SELECT MAX(number) FROM blocks),                 (SELECT updated_at FROM kv WHERE key = 'stats'),                 (SELECT MAX(version) FROM schema_migrations)",
        |q| q,
        |r| Ok((r.try_get(0)?, r.try_get(1)?, r.try_get(2)?)),
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("follow_point: no row"))
}

pub(crate) async fn get_kv(p: &PgDb, key: &str) -> Option<String> {
    let key = key.to_string();
    q::query_opt(
        &p.read,
        "get_kv",
        "SELECT value FROM kv WHERE key = $1",
        |q| q.bind(key),
        |r| r.try_get(0),
    )
    .await
}

/// Cached signatures by selector; a remembered miss is an empty string.
pub(crate) async fn get_selector_names(
    p: &PgDb,
    selectors: &[String],
    fresh_after: i64,
) -> HashMap<String, String> {
    if selectors.is_empty() {
        return HashMap::new();
    }
    let wanted: Vec<String> = selectors
        .iter()
        .map(|s| without_nul(&s.to_lowercase()))
        .collect();
    q::query_rows(
        &p.read,
        "get_selector_names",
        "SELECT selector, signature FROM selector_names \
         WHERE selector = ANY($1) AND fetched_at >= $2",
        |q| q.bind(wanted).bind(fresh_after),
        |r| Ok((r.try_get::<String, _>(0)?, r.try_get::<String, _>(1)?)),
    )
    .await
    .into_iter()
    .collect()
}

/// Remember what the directory said, misses included. A cache write: on the
/// cache pool, best effort, keys sorted so two replicas cannot deadlock.
pub(crate) async fn save_selector_names(
    p: &PgDb,
    answers: &[(String, String)],
) -> anyhow::Result<()> {
    if answers.is_empty() {
        return Ok(());
    }
    let mut rows: Vec<(String, String)> = answers
        .iter()
        .map(|(s, sig)| (without_nul(&s.to_lowercase()), without_nul(sig)))
        .collect();
    rows.sort();
    rows.dedup_by(|a, b| a.0 == b.0);
    let (selectors, signatures): (Vec<String>, Vec<String>) = rows.into_iter().unzip();
    let now = crate::db::now_ts();
    q::exec_best_effort(
        &p.cache,
        "save_selector_names",
        "INSERT INTO selector_names (selector, signature, fetched_at) \
         SELECT u.s, u.g, $3 FROM UNNEST($1::text[], $2::text[]) AS u(s, g) \
         ON CONFLICT (selector) DO UPDATE SET \
             signature = excluded.signature, fetched_at = excluded.fetched_at",
        |q| q.bind(selectors).bind(signatures).bind(now),
    )
    .await
}
