//! The restart drill: Postgres restarts mid-replay, and the final tables still
//! equal the fixture. No block is lost; the writer waits, re-acquires and
//! retries.
//!
//! It restarts the server every other test uses, so it lives alone and runs
//! only when `PG_CONTAINER` names the container (`docker compose ps`), e.g.
//! `PG_CONTAINER=nvnmchain-explorer-postgres-1`.

use std::time::Duration;

use nvnmchain_explorer::db::{self, Role};
use sqlx::{Connection as _, PgConnection};

#[path = "common/backend.rs"]
mod backend;
#[path = "common/bundles.rs"]
mod bundles;
#[allow(dead_code)]
mod common;
use common::baseline::{compared, diff_rows, open_fixture, pg_rows, report, rows, SPECS};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "restarts the Postgres container; set PG_CONTAINER and run alone"]
async fn a_database_restart_mid_replay_loses_nothing() {
    let container = std::env::var("PG_CONTAINER").expect("PG_CONTAINER");
    let fixture = open_fixture("fixtures/baseline/canary-rich.db");
    let (_scratch, url) = backend::scratch_schema().await;
    let mut cfg = backend::pg_config(&url, Role::Indexer);
    cfg.tuning.lost_after = Duration::from_secs(120);
    let db = backend::open(&cfg).await;

    let chunks: Vec<Vec<_>> = bundles::bundles(&fixture)
        .chunks(16)
        .map(<[_]>::to_vec)
        .collect();
    let restart_at = chunks.len() / 3;
    for (i, chunk) in chunks.iter().enumerate() {
        if i == restart_at {
            let status = std::process::Command::new("docker")
                .args(["restart", "-t", "1", &container])
                .status()
                .expect("docker restart");
            assert!(status.success());
        }
        db::save_block_bundles(&db, chunk).await.unwrap();
    }
    let (genesis, cursor) = bundles::genesis(&fixture);
    db::save_genesis_balances(&db, &genesis, cursor)
        .await
        .unwrap();

    let mut conn = PgConnection::connect(&url).await.unwrap();
    let mut diffs = Vec::new();
    for spec in SPECS {
        let cols = compared(&fixture, spec);
        let (base, new) = (
            rows(&fixture, spec, &cols),
            pg_rows(&mut conn, spec, &cols).await,
        );
        diffs.extend(diff_rows(spec.table, &base, &new, ("fixture", "Postgres")));
    }
    report("after the restart", &diffs);
}
