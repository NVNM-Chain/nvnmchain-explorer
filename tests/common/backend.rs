#![allow(dead_code)]
//! The database a test runs against: SQLite, or Postgres under
//! `TEST_DB=postgres`.
//!
//! Included by path (`#[path = "common/backend.rs"] mod backend;`) so a suite
//! that needs only this does not compile the baseline helpers too.
//!
//! On Postgres each test gets a schema of its own, `t_<pid>_<n>`, reached
//! through the connection's `search_path`. It is dropped when the test
//! passes, and kept to inspect when it fails; a sweep at the first open drops
//! the ones left by processes that are gone.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

use nvnmchain_explorer::db::{self, Db, DbConfig, DbUrl, Role, Status};
use sqlx::{AssertSqlSafe, Connection, PgConnection};

/// Keep this alive as long as the database is used.
pub enum TempDb {
    Sqlite(tempfile::TempDir),
    Postgres(Scratch),
}

pub struct Scratch {
    url: String,
    schema: String,
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("kept schema {} to inspect", self.schema);
            return;
        }
        let (url, schema) = (self.url.clone(), self.schema.clone());
        let _ = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(drop_schema(&url, &schema));
        })
        .join();
    }
}

async fn drop_schema(url: &str, schema: &str) {
    if let Ok(mut conn) = PgConnection::connect(url).await {
        let _ = sqlx::raw_sql(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE"
        )))
        .execute(&mut conn)
        .await;
        let _ = conn.close().await;
    }
}

pub fn on_postgres() -> bool {
    std::env::var("TEST_DB").is_ok_and(|v| v == "postgres")
}

/// The server, from `PG_TEST_URL`; a Postgres run without one fails rather
/// than passing on SQLite.
pub fn pg_url() -> String {
    std::env::var("PG_TEST_URL").unwrap_or_else(|_| {
        panic!(
            "TEST_DB=postgres needs PG_TEST_URL; e.g. after `docker compose up -d --wait`, \
             PG_TEST_URL=postgres://explorer:explorer@localhost:5432/explorer (see AGENTS.md)"
        )
    })
}

static SEQ: AtomicUsize = AtomicUsize::new(0);
static SWEPT: OnceLock<()> = OnceLock::new();

/// Drop `t_<pid>_<n>` schemas whose process is gone.
async fn sweep(url: &str) {
    let Ok(mut conn) = PgConnection::connect(url).await else {
        return;
    };
    let names: Vec<String> =
        sqlx::query_scalar("SELECT nspname FROM pg_namespace WHERE nspname ~ '^t_[0-9]+_[0-9]+$'")
            .fetch_all(&mut conn)
            .await
            .unwrap_or_default();
    for name in names {
        let pid = name.split('_').nth(1).unwrap_or("");
        let alive = std::process::Command::new("kill")
            .args(["-0", pid])
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|s| s.success());
        if !alive {
            let _ = sqlx::raw_sql(AssertSqlSafe(format!(
                "DROP SCHEMA IF EXISTS {name} CASCADE"
            )))
            .execute(&mut conn)
            .await;
        }
    }
}

/// A fresh schema, and a URL whose connections use it.
pub async fn scratch_schema() -> (Scratch, String) {
    let url = pg_url();
    if SWEPT.set(()).is_ok() {
        sweep(&url).await;
    }
    let schema = format!(
        "t_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    let mut conn = PgConnection::connect(&url)
        .await
        .unwrap_or_else(|e| panic!("connect to PG_TEST_URL: {e}"));
    sqlx::raw_sql(AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
    )))
    .execute(&mut conn)
    .await
    .unwrap_or_else(|e| panic!("create schema {schema}: {e}"));
    let _ = conn.close().await;
    let sep = if url.contains('?') { '&' } else { '?' };
    let scoped = format!("{url}{sep}options[search_path]={schema}");
    (Scratch { url, schema }, scoped)
}

/// A configuration for `url` as `role`, with small pools for parallel tests.
pub fn pg_config(url: &str, role: Role) -> DbConfig {
    let mut cfg = DbConfig::postgres(DbUrl(url.to_string()), role);
    cfg.tuning.pool_max = Some(2);
    cfg
}

/// `cfg` opened, its status published to nobody.
pub async fn open(cfg: &DbConfig) -> Db {
    db::open_with(
        cfg,
        tokio::sync::watch::channel(Status::starting(cfg.role)).0,
    )
    .await
    .unwrap_or_else(|e| panic!("open: {e:#}"))
}

/// Run `sql`, one statement or several, on a connection of its own.
pub async fn exec(url: &str, sql: &str) {
    let mut conn = PgConnection::connect(url)
        .await
        .unwrap_or_else(|e| panic!("connect: {e}"));
    sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .execute(&mut conn)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// A fresh database for one test. Keep the guard alive for as long as the
/// database is used; dropping it removes the database.
pub async fn temp_db(name: &str) -> (TempDb, Db) {
    if on_postgres() {
        let (scratch, url) = scratch_schema().await;
        return (
            TempDb::Postgres(scratch),
            open(&pg_config(&url, Role::All)).await,
        );
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(name);
    (
        TempDb::Sqlite(dir),
        db::open(path.to_str().unwrap()).await.expect("open db"),
    )
}
