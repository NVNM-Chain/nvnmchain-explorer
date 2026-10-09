//! Data correctness against baseline databases.
//!
//! A baseline is an explorer database written by an earlier build. Its blocks
//! say which ranges it covers; the check re-indexes exactly those ranges from
//! the RPC with this build, into a fresh database, and compares every row the
//! indexer writes. Anything that differs is a change in what the explorer
//! stores, which a refactor, a migration or a new backend must not make.
//!
//! The baselines are `fixtures/baseline/*.db`, both from canary:
//!
//! - `canary-blocks.db`: 4649 empty blocks, as a deployed explorer wrote them
//!   before the database boundary was sealed (commit 4123460).
//! - `canary-rich.db`: `RICH_RANGES`, written by `build_baseline` at 4123460.
//!
//! `BASELINE_DBS` (comma-separated paths) checks other databases instead. A
//! baseline is opened read-only and immutable: nothing here can migrate or
//! repair it. To write a new one from the build checked out:
//!
//! ```text
//! BASELINE_BUILD=fixtures/baseline/<name>.db \
//!     cargo test --test baseline build_baseline -- --ignored --nocapture
//! ```

use std::collections::HashSet;
use std::ops::RangeInclusive;
use std::time::Duration;

use futures_util::{stream, StreamExt, TryStreamExt};
use nvnmchain_explorer::config::DEFAULT_RPC_URL;
use nvnmchain_explorer::db::{self, Db};
use nvnmchain_explorer::indexer::fetch_block_bundle;
use nvnmchain_explorer::models::BlockBundle;
use nvnmchain_explorer::rpc::ChainRpc;
use nvnmchain_explorer::tokens::balances_at_genesis;
use rusqlite::Connection;

#[allow(dead_code)]
mod common;
use common::baseline::{
    columns, diff_rows, fixture_paths, open_fixture, pg_rows, rows, tables, Rows, Spec, SPECS,
};

#[path = "common/backend.rs"]
mod backend;

/// The ranges `build_baseline` indexes by default, chosen so that one small
/// database exercises every write path: canary's first Transfer (102504); all
/// 26 anchoring writes, the failed tx at 105627 and three contract deploys; a
/// TIP-20 token's creation, role grant and first mint (1442891..1442924); and
/// legacy, EIP-1559 and 0x76 transactions side by side.
const RICH_RANGES: [RangeInclusive<u64>; 4] = [
    102_500..=102_720,
    105_400..=105_700,
    1_442_300..=1_442_999,
    1_445_400..=1_446_199,
];

/// Blocks fetched at once, and blocks per commit.
const CONCURRENCY: usize = 16;
const COMMIT: usize = 64;

/// Written by the live loops and page views rather than by indexing a block:
/// watermarks, the stats blob and the selector cache.
const NOT_INDEXED: &[&str] = &[
    "kv",
    "schema_migrations",
    "selector_names",
    "sqlite_sequence",
];

/// The most differences printed; the counts per table are always complete.
const SHOWN: usize = 25;

fn rpc() -> ChainRpc {
    let url = std::env::var("NVNM_RPC")
        .or_else(|_| std::env::var("TEMPO_RPC"))
        .unwrap_or_else(|_| DEFAULT_RPC_URL.to_string());
    ChainRpc::new(url).expect("rpc client")
}

/// The one line that differs between builds: how a database is opened.
async fn open_db(path: &str) -> Db {
    db::open(path).await.expect("open database")
}

/// The contiguous runs of block numbers a database holds.
fn block_runs(conn: &Connection) -> Vec<RangeInclusive<u64>> {
    let mut stmt = conn
        .prepare("SELECT number FROM blocks ORDER BY number")
        .unwrap();
    let numbers = stmt.query_map([], |r| r.get::<_, i64>(0)).unwrap();
    let mut runs: Vec<RangeInclusive<u64>> = Vec::new();
    for n in numbers {
        let n = n.unwrap() as u64;
        match runs.last_mut() {
            Some(run) if *run.end() + 1 == n => *run = *run.start()..=n,
            _ => runs.push(n..=n),
        }
    }
    runs
}

/// Whether every token the block mentions came with its metadata. The indexer
/// drops a token whose lookup failed, with a warning, and a token row missing
/// while its transfers are applied leaves its holder count behind: a dropped
/// request, not a difference in what the build stores.
fn has_all_tokens(bundle: &BlockBundle) -> bool {
    let have: HashSet<&str> = bundle.tokens.iter().map(|m| m.address.as_str()).collect();
    bundle
        .transfers
        .iter()
        .map(|t| t.token_addr.as_str())
        .chain(bundle.txs.iter().filter_map(|t| t.fee_token.as_deref()))
        .all(|addr| have.contains(addr))
}

/// One block's bundle, retried: a public node drops the odd request, a block
/// with transactions but no receipts yet comes back as `None`, and a failed
/// token lookup comes back without the token.
async fn fetch(rpc: &ChainRpc, number: u64) -> anyhow::Result<BlockBundle> {
    let mut wait = Duration::from_millis(250);
    for attempt in 1.. {
        match fetch_block_bundle(rpc, number).await {
            Ok(Some(bundle)) if has_all_tokens(&bundle) => return Ok(bundle),
            Ok(Some(_)) if attempt >= 6 => anyhow::bail!("block {number}: token metadata missing"),
            Ok(None) if attempt >= 6 => anyhow::bail!("block {number}: no bundle"),
            Err(e) if attempt >= 6 => return Err(e.context(format!("block {number}"))),
            _ => {}
        }
        tokio::time::sleep(wait).await;
        wait *= 2;
    }
    unreachable!()
}

/// Index `runs` into `db` the way a baseline is built: fetched concurrently,
/// written in ascending block order, then the genesis pass the explorer runs
/// alongside indexing, which fills `genesis_balances` and adds each holder's
/// block-0 balance to `token_balances`. Returns the most statements one batch
/// sent, for Postgres's round-trip budget.
async fn reindex(rpc: &ChainRpc, db: &Db, runs: &[RangeInclusive<u64>]) -> u64 {
    let numbers: Vec<u64> = runs.iter().cloned().flatten().collect();
    let mut bundles = stream::iter(numbers)
        .map(|n| fetch(rpc, n))
        .buffered(CONCURRENCY)
        .try_chunks(COMMIT);
    let mut worst = 0;
    while let Some(chunk) = bundles.next().await {
        let chunk = chunk.map_err(|e| e.1).expect("fetch");
        let before = db::statements();
        db::save_block_bundles(db, &chunk).await.expect("save");
        worst = worst.max(db::statements() - before);
    }
    add_genesis_balances(rpc, db).await;
    worst
}

/// The re-indexed database, on either backend.
enum Fresh {
    Sqlite(Connection),
    Postgres(sqlx::PgConnection),
}

impl Fresh {
    async fn tables(&mut self) -> Vec<String> {
        match self {
            Fresh::Sqlite(conn) => tables(conn),
            Fresh::Postgres(conn) => sqlx::query_scalar(
                "SELECT table_name::text FROM information_schema.tables
                 WHERE table_schema = current_schema() ORDER BY 1",
            )
            .fetch_all(conn)
            .await
            .unwrap(),
        }
    }

    async fn columns(&mut self, table: &str) -> Vec<String> {
        match self {
            Fresh::Sqlite(conn) => columns(conn, table),
            Fresh::Postgres(conn) => sqlx::query_scalar(
                "SELECT column_name::text FROM information_schema.columns
                 WHERE table_schema = current_schema() AND table_name = $1 ORDER BY ordinal_position",
            )
            .bind(table)
            .fetch_all(conn)
            .await
            .unwrap(),
        }
    }

    async fn rows(&mut self, spec: &Spec, cols: &[String]) -> Rows {
        match self {
            Fresh::Sqlite(conn) => rows(conn, spec, cols),
            Fresh::Postgres(conn) => pg_rows(conn, spec, cols).await,
        }
    }
}

/// `indexer::add_genesis_balances`, which is private: page through the
/// transfers' holders and store each one's balance at block 0. A page's
/// lookup is retried; the cursor only moves once its balances are stored.
async fn add_genesis_balances(rpc: &ChainRpc, db: &Db) {
    while let Some((cursor, holders)) = db::holders_without_genesis_balance(db, 1000)
        .await
        .expect("genesis holders")
    {
        let mut wait = Duration::from_millis(250);
        let balances = loop {
            match balances_at_genesis(rpc, &holders).await {
                Ok(balances) => break balances,
                Err(e) if wait > Duration::from_secs(8) => panic!("genesis balances: {e:#}"),
                Err(_) => tokio::time::sleep(wait).await,
            }
            wait *= 2;
        };
        let rows: Vec<_> = holders.into_iter().zip(balances).collect();
        db::save_genesis_balances(db, &rows, cursor)
            .await
            .expect("save genesis balances");
    }
}

/// Every difference between the baseline and the re-indexed database, and
/// the rows compared per table.
async fn compare(baseline: &Connection, fresh: &mut Fresh) -> (Vec<String>, Vec<(String, usize)>) {
    let mut diffs = Vec::new();
    let mut compared = Vec::new();
    let mut all_tables = tables(baseline);
    all_tables.extend(fresh.tables().await);
    all_tables.sort();
    all_tables.dedup();
    for table in all_tables {
        if !NOT_INDEXED.contains(&table.as_str()) && !SPECS.iter().any(|s| s.table == table) {
            diffs.push(format!(
                "{table}: no comparison spec; add it to SPECS or NOT_INDEXED in tests/baseline.rs"
            ));
        }
    }
    for spec in SPECS {
        // Both directions: a column the re-index no longer writes is lost
        // data, not a column to stop comparing.
        let compared_cols = |all: Vec<String>| -> Vec<String> {
            all.into_iter()
                .filter(|c| !spec.skip.contains(&c.as_str()))
                .collect()
        };
        let (base_cols, cols) = (
            compared_cols(columns(baseline, spec.table)),
            compared_cols(fresh.columns(spec.table).await),
        );
        let mut schema_diffs = Vec::new();
        for (side, present, other) in [
            ("baseline", &base_cols, &cols),
            ("re-index", &cols, &base_cols),
        ] {
            if present.is_empty() {
                schema_diffs.push(format!("{}: table missing from the {side}", spec.table));
            } else {
                for col in other.iter().filter(|c| !present.contains(c)) {
                    schema_diffs.push(format!(
                        "{}: column {col} missing from the {side}",
                        spec.table
                    ));
                }
            }
        }
        if !schema_diffs.is_empty() {
            diffs.extend(schema_diffs);
            continue;
        }
        let (base, new) = (rows(baseline, spec, &cols), fresh.rows(spec, &cols).await);
        compared.push((spec.table.to_string(), base.len()));
        diffs.extend(diff_rows(spec.table, &base, &new, ("baseline", "re-index")));
    }
    (diffs, compared)
}

/// `BASELINE_DBS` if set, else every database under `fixtures/baseline`.
fn baselines() -> Vec<String> {
    if let Ok(list) = std::env::var("BASELINE_DBS") {
        return list
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(String::from)
            .collect();
    }
    fixture_paths()
        .iter()
        .map(|p| p.display().to_string())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn reindexing_reproduces_each_baseline() {
    let rpc = rpc();
    let mut failed = Vec::new();
    for path in &baselines() {
        let baseline = open_fixture(path);
        let runs = block_runs(&baseline);
        assert!(!runs.is_empty(), "{path}: no blocks to re-index");
        eprintln!("{path}: re-indexing {runs:?}");

        let (mut fresh, _guard) = if backend::on_postgres() {
            let (scratch, url) = backend::scratch_schema().await;
            let db = backend::open(&backend::pg_config(&url, db::Role::All)).await;
            let worst = reindex(&rpc, &db, &runs).await;
            eprintln!("  at most {worst} round trip(s) per batch");
            assert!(
                worst <= 14,
                "{path}: a batch took {worst} round trips; the budget is 14"
            );
            use sqlx::Connection as _;
            let conn = sqlx::PgConnection::connect(&url).await.unwrap();
            (Fresh::Postgres(conn), backend::TempDb::Postgres(scratch))
        } else {
            let dir = tempfile::tempdir().expect("tempdir");
            let fresh_path = dir.path().join("reindex.db");
            reindex(&rpc, &open_db(fresh_path.to_str().unwrap()).await, &runs).await;
            let conn = Connection::open(&fresh_path).expect("open re-index");
            (Fresh::Sqlite(conn), backend::TempDb::Sqlite(dir))
        };
        let (diffs, compared) = compare(&baseline, &mut fresh).await;
        for (table, n) in &compared {
            eprintln!("  {table}: {n} row(s)");
        }
        if diffs.is_empty() {
            eprintln!("{path}: identical");
        } else {
            eprintln!("{path}: {} difference(s)", diffs.len());
            for d in diffs.iter().take(SHOWN) {
                eprintln!("  {d}");
            }
            failed.push(format!("{path}: {} difference(s)", diffs.len()));
        }
    }
    assert!(failed.is_empty(), "baselines differ: {failed:?}");
}

/// Write a new baseline at `BASELINE_BUILD` from the build checked out, over
/// `BASELINE_RANGES` (`from-to,from-to`) or `RICH_RANGES`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "writes a baseline database; run explicitly with BASELINE_BUILD set"]
async fn build_baseline() {
    let path = std::env::var("BASELINE_BUILD").expect("set BASELINE_BUILD to the file to write");
    assert!(
        !std::path::Path::new(&path).exists(),
        "{path} exists; a baseline is never overwritten"
    );
    let runs: Vec<RangeInclusive<u64>> = match std::env::var("BASELINE_RANGES") {
        Ok(spec) => spec
            .split(',')
            .map(|r| {
                let (a, b) = r.trim().split_once('-').expect("from-to");
                a.parse().unwrap()..=b.parse().unwrap()
            })
            .collect(),
        Err(_) => RICH_RANGES.to_vec(),
    };
    eprintln!("{path}: indexing {runs:?}");
    reindex(&rpc(), &open_db(&path).await, &runs).await;
    let conn = Connection::open(&path).unwrap();
    for table in [
        "blocks",
        "transactions",
        "transfer_events",
        "anchoring_events",
        "token_metadata",
        "token_balances",
        "genesis_balances",
    ] {
        let n: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        eprintln!("  {table}: {n}");
    }
}
