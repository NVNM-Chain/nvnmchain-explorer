//! The two backends against each other: the same bundles go into both,
//! shuffled, repeated and split into batches differently on each side, and
//! every table must come out the same.
//!
//! Needs a Postgres server: ignored unless run with `--include-ignored`, and
//! then failing without `PG_TEST_URL`.

use std::collections::BTreeMap;

use nvnmchain_explorer::db::{self, Role};
use nvnmchain_explorer::models::BlockBundle;
use rusqlite::Connection;
use sqlx::Connection as _;
use sqlx::PgConnection;

#[path = "common/backend.rs"]
mod backend;
#[allow(dead_code)]
mod common;
use common::baseline::{compared, diff_rows, open_fixture, pg_rows, report, rows, tables, Spec};

#[path = "common/bundles.rs"]
mod replay_bundles;

/// xorshift64*: deterministic, so a failure reproduces.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// The bundles in a random order, a tenth of them twice, cut into batches of
/// random sizes.
fn arrange(bundles: &[BlockBundle], rng: &mut Rng) -> Vec<Vec<BlockBundle>> {
    let mut all: Vec<BlockBundle> = bundles.to_vec();
    for _ in 0..bundles.len() / 10 {
        all.push(bundles[rng.below(bundles.len())].clone());
    }
    for i in (1..all.len()).rev() {
        all.swap(i, rng.below(i + 1));
    }
    let mut batches = Vec::new();
    while !all.is_empty() {
        let n = (1 + rng.below(64)).min(all.len());
        batches.push(all.drain(..n).collect());
    }
    batches
}

/// Row keys for every table, and the columns that differ by design.
fn spec_for(table: &str) -> Spec {
    let (key, skip): (&[&str], &[&str]) = match table {
        "blocks" => (&["number"], &["created_at"]),
        "transactions" => (&["hash"], &["created_at"]),
        "transfer_events" => (&["block_number", "log_index"], &["id", "created_at"]),
        "anchoring_events" => (&["block_number", "log_index"], &[]),
        "token_metadata" => (&["address"], &["created_at", "updated_at"]),
        "token_balances" => (&["token_addr", "holder_addr"], &["updated_at"]),
        "genesis_balances" => (&["token_addr", "holder_addr"], &[]),
        "counters" => (&["name"], &[]),
        "kv" => (&["key"], &["updated_at"]),
        "selector_names" => (&["selector"], &["fetched_at"]),
        // Checksums and who applied them differ by design.
        "schema_migrations" => (&["version"], &["checksum", "applied_at", "applied_by"]),
        other => panic!("{other}: no row key; add it to spec_for in tests/differential.rs"),
    };
    Spec {
        table: Box::leak(table.to_string().into_boxed_str()),
        key,
        skip,
    }
}

/// `kv` keys a run writes for itself.
const KV_SKIPPED: &[&str] = &[
    "genesis_balances_cursor",
    "stats",
    "chain_head",
    "writer_seq",
];

#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn both_backends_write_the_same_tables() {
    let fixture = open_fixture("fixtures/baseline/canary-rich.db");
    let bundles = replay_bundles::bundles(&fixture);
    let (genesis, cursor) = replay_bundles::genesis(&fixture);

    for seed in [1u64, 2, 3] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("differential.db");
        let sqlite = db::open(path.to_str().unwrap()).await.unwrap();
        let (_scratch, url) = backend::scratch_schema().await;
        let pg = backend::open(&backend::pg_config(&url, Role::All)).await;

        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        for (db, label) in [(&sqlite, "SQLite"), (&pg, "Postgres")] {
            for batch in arrange(&bundles, &mut rng) {
                db::save_block_bundles(db, &batch)
                    .await
                    .unwrap_or_else(|e| panic!("{label}: {e:#}"));
            }
            db::save_genesis_balances(db, &genesis, cursor)
                .await
                .unwrap();
            db::repair_derived_tables(db).await;
        }

        let lite = Connection::open(&path).unwrap();
        let mut conn = PgConnection::connect(&url).await.unwrap();
        let mut diffs = Vec::new();
        for table in tables(&lite) {
            if table == "sqlite_sequence" {
                continue;
            }
            let spec = spec_for(&table);
            let cols = compared(&lite, &spec);
            let mut a = rows(&lite, &spec, &cols);
            let mut b = pg_rows(&mut conn, &spec, &cols).await;
            if table == "kv" {
                let keep = |rows: &mut BTreeMap<_, BTreeMap<String, serde_json::Value>>| {
                    rows.retain(|_, r| !KV_SKIPPED.iter().any(|k| r["key"] == *k))
                };
                keep(&mut a);
                keep(&mut b);
            }
            diffs.extend(diff_rows(&table, &a, &b, ("SQLite", "Postgres")));
        }
        report(&format!("seed {seed}"), &diffs);
    }
}
