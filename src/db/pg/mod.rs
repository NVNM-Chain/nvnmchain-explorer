//! The Postgres backend: hand-written twins of every `sqlite.rs` function,
//! read pools for pages, and one owned writer session that holds a
//! session-level advisory lock.
//!
//! Each connection carries its own settings in its startup options, because
//! role defaults apply to every connection a role opens and Cloud SQL has no
//! instance flags for them.

use std::cell::Cell;
use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection, PgPool};
use tokio::sync::watch;

use super::{migrations, DbConfig, Role, Status};

mod blocks;
mod error;
mod jobs;
pub(crate) mod migrate;
mod plan;
pub(crate) mod q;
pub(crate) mod shared;
mod tokens;
mod transfers;
mod txs;
mod write;
pub(crate) mod writer;

pub(crate) use blocks::*;
pub use error::DbError;
pub(crate) use jobs::*;
pub(crate) use tokens::*;
pub(crate) use transfers::*;
pub(crate) use txs::*;
pub(crate) use write::*;

tokio::task_local! {
    /// Set by a read that failed, so the response becomes a 503 rather than
    /// an empty page. Scoped per request by the 503 middleware.
    pub(crate) static DB_FAILED: Cell<bool>;
}

/// Note that a database read failed, if anything is listening. Reads outside
/// a request (the follower, the indexer) have no scope, and that is fine.
pub(crate) fn flag_failure() {
    let _ = DB_FAILED.try_with(|f| f.set(true));
}

/// PostgreSQL 15 is the oldest supported major: the design needs 14 for
/// `idle_session_timeout` and `client_connection_check_interval`, and 14
/// reaches end of life in November 2026.
pub(crate) const VERSION_FLOOR: u32 = 150_000;

pub(crate) fn check_version(conn: &PgConnection) -> Result<(), sqlx::Error> {
    match conn.server_version_num() {
        Some(v) if v < VERSION_FLOOR => Err(sqlx::Error::Configuration(
            format!(
                "PostgreSQL {} is too old; 15 or later is required",
                v / 10_000
            )
            .into(),
        )),
        _ => Ok(()),
    }
}

pub(crate) struct PgDb {
    /// Pages and the indexer's reads: read-only sessions, 5 s statements.
    pub(crate) read: PgPool,
    /// True caches web pages write (selector names, traces).
    pub(crate) cache: PgPool,
    /// The one writer, under `ROLE=indexer` and `all`.
    pub(crate) writer: Option<writer::Writer>,
}

impl PgDb {
    pub(crate) fn writer(&self) -> Result<&writer::Writer, DbError> {
        self.writer.as_ref().ok_or(DbError::NotWriter)
    }
}

/// `explorer-<role>-<pool>/<version>/b<B>`, so `pg_stat_activity` says who is who.
fn application_name(role: Role, what: &str) -> String {
    format!(
        "explorer-{role}-{what}/{}/b{}",
        env!("CARGO_PKG_VERSION"),
        migrations::binary_version()
    )
}

fn pool(opts: PgConnectOptions, max: u32, min: u32) -> PgPool {
    PgPoolOptions::new()
        .max_connections(max)
        .min_connections(min)
        .acquire_timeout(Duration::from_secs(3))
        // The default pings on every acquire: a round trip per query.
        .test_before_acquire(false)
        .idle_timeout(Duration::from_secs(300))
        .after_connect(|conn, _| Box::pin(async move { check_version(conn) }))
        .connect_lazy_with(opts)
}

/// Open the Postgres backend for `cfg.role`, publishing progress on `status`.
pub(crate) async fn open(cfg: &DbConfig, status: &watch::Sender<Status>) -> Result<PgDb> {
    let base = cfg.pg_options()?;
    let read_max = cfg
        .tuning
        .pool_max
        .unwrap_or(if cfg.role == Role::Indexer { 4 } else { 8 });
    let read = pool(
        base.clone()
            .application_name(&application_name(cfg.role, "read"))
            .options([
                ("statement_timeout", "5s"),
                ("idle_session_timeout", "0"),
                ("default_transaction_read_only", "on"),
                ("idle_in_transaction_session_timeout", "10s"),
            ]),
        read_max,
        1.min(read_max),
    );
    let cache = pool(
        base.clone()
            .application_name(&application_name(cfg.role, "cache"))
            .options([
                ("statement_timeout", "2s"),
                ("idle_session_timeout", "0"),
                ("idle_in_transaction_session_timeout", "5s"),
            ]),
        cfg.tuning.pool_max.unwrap_or(2).min(2),
        0,
    );
    let writer = match cfg.role {
        Role::Web => None,
        Role::Indexer | Role::All => {
            let opts = base.application_name(&application_name(cfg.role, "writer"));
            Some(writer::Writer::start(opts, read.clone(), cfg, status.clone()).await?)
        }
    };
    Ok(PgDb {
        read,
        cache,
        writer,
    })
}

/// D: the database's schema version, 0 before any migration ran. The table
/// is looked up on its own first: a statement that names a missing table
/// fails whatever its `WHERE` says.
pub(crate) async fn schema_version(read: &PgPool) -> Result<i64> {
    let versioned = q::try_query_opt(
        read,
        "schema_version",
        "SELECT to_regclass('schema_migrations') IS NOT NULL",
        |q| q,
        |r| sqlx::Row::try_get::<bool, _>(r, 0),
    )
    .await
    .context("look up schema_migrations")?;
    if versioned != Some(true) {
        return Ok(0);
    }
    let row = q::try_query_opt(
        read,
        "schema_version",
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
        |q| q,
        |r| sqlx::Row::try_get::<i64, _>(r, 0),
    )
    .await
    .context("read the schema version")?;
    Ok(row.unwrap_or(0))
}

/// One raw connection with `opts`, within 10 s.
pub(crate) async fn connect(opts: &PgConnectOptions) -> Result<PgConnection> {
    let conn = tokio::time::timeout(Duration::from_secs(10), PgConnection::connect_with(opts))
        .await
        .context("connect: no answer within 10 s")?
        .context("connect")?;
    check_version(&conn)?;
    Ok(conn)
}

#[cfg(test)]
mod tests {
    /// SQLite spellings that mean something else, or nothing, on Postgres:
    /// `?N` parameters, `OR IGNORE`, `NOCASE` and `GLOB`. And every upsert
    /// names its conflict target, since a bare column is ambiguous there.
    #[test]
    fn the_postgres_sql_has_no_sqlite_spellings() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db/pg");
        let mut found = Vec::new();
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            let text = std::fs::read_to_string(&path).unwrap();
            // The lint's own spellings live in this test.
            let code = text.split("#[cfg(test)]").next().unwrap_or("");
            for (i, line) in code.lines().enumerate() {
                let trimmed = line.trim_start();
                if trimmed.starts_with("//") {
                    continue;
                }
                let upper = line.to_uppercase();
                let sqlite_param = line
                    .match_indices('?')
                    .any(|(at, _)| line[at + 1..].starts_with(|c: char| c.is_ascii_digit()));
                let bad = sqlite_param
                    || upper.contains("OR IGNORE")
                    || upper.contains("NOCASE")
                    || upper.contains(" GLOB ")
                    || (upper.contains("ON CONFLICT DO UPDATE"));
                if bad {
                    found.push(format!("{}:{}: {}", path.display(), i + 1, trimmed));
                }
            }
        }
        assert!(
            found.is_empty(),
            "SQLite spellings in Postgres SQL:\n{}",
            found.join("\n")
        );
    }
}
