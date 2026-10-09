//! Versioned migrations, end to end.
//!
//! The SQLite tests need nothing but the fixtures. The Postgres ones are
//! ignored unless run with `--include-ignored`, and then fail unless
//! `PG_TEST_URL` names a server.

use std::collections::BTreeMap;
use std::path::Path;

use nvnmchain_explorer::db;
use rusqlite::types::Value;
use rusqlite::{Connection, OpenFlags};

#[allow(dead_code)]
mod common;
use common::baseline::{columns, quoted, tables};

/// A value as stored, its storage class included: a rewrite to equal-looking
/// text or JSON still shows.
fn exact(value: Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => format!("{f:?}"),
        Value::Text(s) => format!("{s:?}"),
        Value::Blob(b) => format!("x'{}'", hex::encode(b)),
    }
}

/// Every row of every table in `cols`, over its columns there (the ones the
/// legacy file had), sorted so that only the rows themselves are compared.
fn contents(
    conn: &Connection,
    cols: &BTreeMap<String, Vec<String>>,
) -> BTreeMap<String, Vec<String>> {
    let mut out = BTreeMap::new();
    for (table, cols) in cols {
        let mut stmt = conn
            .prepare(&format!("SELECT {} FROM \"{table}\"", quoted(cols)))
            .unwrap();
        let mut rows: Vec<String> = stmt
            .query_map([], |r| {
                (0..cols.len())
                    .map(|i| Ok(exact(r.get(i)?)))
                    .collect::<Result<Vec<_>, _>>()
            })
            .unwrap()
            .map(|row| format!("({})", row.unwrap().join(", ")))
            .collect();
        rows.sort();
        out.insert(table.clone(), rows);
    }
    out
}

/// What changed in each table, with a few rows to show it.
fn changes(
    before: &BTreeMap<String, Vec<String>>,
    after: &BTreeMap<String, Vec<String>>,
) -> Vec<String> {
    let mut out = Vec::new();
    for (table, was) in before {
        let now = &after[table];
        if now != was {
            let only = |a: &[String], b: &[String]| -> Vec<String> {
                a.iter()
                    .filter(|r| b.binary_search(r).is_err())
                    .take(3)
                    .cloned()
                    .collect()
            };
            out.push(format!(
                "{table}: {} rows before, {} after; gone {:?}; new {:?}",
                was.len(),
                now.len(),
                only(was, now),
                only(now, was)
            ));
        }
    }
    out
}

/// A deployed file written before versions existed is adopted as version 1
/// when the new binary opens it, and every later version applies over it with
/// its rows intact (the per-version data upgrade): the canaries stay legacy on
/// disk, so this runs on every build.
#[tokio::test]
async fn both_canaries_are_adopted_with_their_rows_intact() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/baseline");
    for name in ["canary-blocks.db", "canary-rich.db"] {
        let original = Connection::open_with_flags(
            root.join(name),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .unwrap();
        assert!(
            !tables(&original).contains(&"schema_migrations".to_string()),
            "{name} must stay a legacy file"
        );
        let cols: BTreeMap<String, Vec<String>> = tables(&original)
            .into_iter()
            .filter(|t| !t.starts_with("sqlite_"))
            .map(|t| {
                let cols = columns(&original, &t);
                (t, cols)
            })
            .collect();
        let before = contents(&original, &cols);

        let dir = tempfile::tempdir().unwrap();
        let copy = dir.path().join(name);
        original
            .execute("VACUUM INTO ?1", [copy.to_str().unwrap()])
            .unwrap();
        drop(original);
        let opened = db::open(copy.to_str().unwrap()).await.unwrap();
        drop(opened);

        let adopted = Connection::open(&copy).unwrap();
        let stamped: Vec<(i64, String)> = adopted
            .prepare("SELECT version, name FROM schema_migrations ORDER BY version")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(stamped[0], (1, "baseline".to_string()), "{name}");
        assert_eq!(
            stamped.len() as i64,
            db::migrations::binary_version(),
            "{name}"
        );
        let changed = changes(&before, &contents(&adopted, &cols));
        assert!(
            changed.is_empty(),
            "{name}: rows changed:\n{}",
            changed.join("\n")
        );
    }
}

// ---------------------------------------------------------------------------
// Postgres: what the preflight refuses, before it ever asks for the lock
// ---------------------------------------------------------------------------

#[path = "common/backend.rs"]
mod backend;

use std::time::Duration;

use nvnmchain_explorer::db::{DbConfig, Role, Status};
use sqlx::{Connection as _, PgConnection};

fn indexer(url: &str) -> DbConfig {
    let mut cfg = backend::pg_config(url, Role::Indexer);
    cfg.tuning.candidate_retry = Duration::from_millis(100);
    cfg
}

/// The open error, which must come within a few seconds: a refusal never
/// waits for the lock.
async fn refusal(cfg: &DbConfig) -> String {
    let open = db::open_with(
        cfg,
        tokio::sync::watch::channel(Status::starting(cfg.role)).0,
    );
    match tokio::time::timeout(Duration::from_secs(15), open).await {
        Ok(Ok(_)) => panic!("opened"),
        Ok(Err(e)) => format!("{e:#}"),
        Err(_) => panic!("no refusal within 15 s"),
    }
}

#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn an_edited_postgres_migration_is_refused() {
    let (_scratch, url) = backend::scratch_schema().await;
    drop(backend::open(&indexer(&url)).await);
    backend::exec(
        &url,
        "UPDATE schema_migrations SET checksum = 'x' WHERE version = 1",
    )
    .await;
    let err = refusal(&indexer(&url)).await;
    assert!(
        err.contains("migration 1 was edited after it was applied"),
        "{err}"
    );
}

/// D > B is refused at preflight, while another writer holds the lock.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn a_newer_postgres_database_is_refused_before_the_lock() {
    let (_scratch, url) = backend::scratch_schema().await;
    let leader = backend::open(&indexer(&url)).await;
    let newer = db::migrations::binary_version() + 1;
    backend::exec(
        &url,
        &format!("INSERT INTO schema_migrations VALUES ({newer}, 'later', 'x', 0, 'test')"),
    )
    .await;
    let err = refusal(&indexer(&url)).await;
    assert!(err.contains("newer than this binary"), "{err}");
    drop(leader);
}

#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn a_schema_applied_by_hand_is_refused() {
    let (_scratch, url) = backend::scratch_schema().await;
    backend::exec(
        &url,
        include_str!("../migrations/postgres/0001_baseline.sql"),
    )
    .await;
    let err = refusal(&indexer(&url)).await;
    assert!(err.contains("no schema_migrations"), "{err}");
}

/// Web replicas read everything and write only the two caches.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn the_web_role_reads_and_writes_only_the_caches() {
    let (_scratch, url) = backend::scratch_schema().await;
    let role = format!("t_web_{}", std::process::id());
    backend::exec(
        &url,
        &format!("DROP ROLE IF EXISTS {role}; CREATE ROLE {role} LOGIN PASSWORD 'pw'"),
    )
    .await;
    let mut cfg = indexer(&url);
    cfg.web_role = role.clone();
    let writer = backend::open(&cfg).await;
    let block = nvnmchain_explorer::models::Block {
        number: 1,
        hash: format!("0x{:064x}", 1),
        parent_hash: format!("0x{:064x}", 0),
        timestamp: 1,
        timestamp_ms: 0,
        gas_used: 0,
        gas_limit: 0,
        base_fee: "0".into(),
        size: 0,
        extra_data: String::new(),
        epoch: 0,
        view: 0,
        proposer: format!("0x{}", "33".repeat(20)),
        miner: format!("0x{}", "33".repeat(20)),
        tx_count: 0,
        created_at: 0,
    };
    db::save_block(&writer, &block).await.unwrap();

    let mut as_web = url::Url::parse(&url).unwrap();
    as_web.set_username(&role).unwrap();
    as_web.set_password(Some("pw")).unwrap();
    let web = backend::open(&backend::pg_config(as_web.as_str(), Role::Web)).await;
    assert_eq!(db::get_latest_block(&web).await.map(|b| b.number), Some(1));
    db::save_selector_names(&web, &[("0x12345678".into(), "f()".into())])
        .await
        .expect("selector cache");
    db::set_trace(&web, &format!("0x{:064x}", 9), "{}")
        .await
        .expect("trace cache");
    let err = db::save_block(&web, &block).await.unwrap_err();
    assert!(format!("{err:#}").contains("no writer"), "{err:#}");

    let mut conn = PgConnection::connect(as_web.as_str()).await.unwrap();
    let denied = sqlx::query(
        "INSERT INTO blocks (number, hash, parent_hash, timestamp) VALUES (2, '\\x02', '\\x01', 0)",
    )
    .execute(&mut conn)
    .await
    .unwrap_err();
    assert_eq!(
        denied.as_database_error().and_then(|d| d.code()).as_deref(),
        Some("42501")
    );
    // A cache write that fails is logged and never makes the page a 503.
    backend::exec(&url, &format!("REVOKE UPDATE ON transactions FROM {role}")).await;
    let (written, failed) =
        db::track_failures(db::set_trace(&web, &format!("0x{:064x}", 9), "{}")).await;
    assert!(written.is_err(), "the write was refused");
    assert!(!failed, "a cache write never sets the 503 flag");

    drop((web, writer));
    backend::exec(&url, &format!("DROP OWNED BY {role}; DROP ROLE {role}")).await;
}
