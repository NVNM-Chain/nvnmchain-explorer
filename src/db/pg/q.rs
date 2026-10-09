//! Every Postgres statement goes through here.
//!
//! Reads keep SQLite's rule: a failed query is logged and becomes "no data",
//! so a page degrades rather than erroring. Here a failure also sets
//! `DB_FAILED`, which the 503 middleware turns into a `503` rather than a false
//! empty page or 404. Each read runs on an explicitly acquired connection
//! under a client deadline, and a connection that times out is closed, never
//! returned to the pool. `try_*` variants return the error instead, for the
//! callers that must not mistake a failure for an empty table.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use sqlx::postgres::{PgArguments, PgRow};
use sqlx::{PgConnection, PgPool, Postgres};

use super::{flag_failure, DbError};

pub(crate) type PgQuery = sqlx::query::Query<'static, Postgres, PgArguments>;

/// A read's whole round trip: the server's `statement_timeout` (5 s) plus 2 s,
/// so the server cancels first and the error is clean.
pub(crate) const READ_DEADLINE: Duration = Duration::from_secs(7);
/// A cache write's round trip: `statement_timeout` (2 s) plus 2 s.
pub(crate) const CACHE_DEADLINE: Duration = Duration::from_secs(4);

tokio::task_local! {
    /// The client deadline a writer statement runs under, by budget.
    pub(crate) static DEADLINE: Option<Duration>;
}

static STATEMENTS: AtomicU64 = AtomicU64::new(0);

/// Statements sent so far by this process, for the round-trip budget tests.
pub fn statements() -> u64 {
    STATEMENTS.load(Relaxed)
}

pub(crate) fn count_statement() {
    STATEMENTS.fetch_add(1, Relaxed);
}

/// Run `$body` on a connection from `$pool` under `$deadline`, with the
/// connection bound to `$c`. A connection that misses the deadline may be
/// half-open, so it is closed rather than returned to the pool. A sqlx error
/// keeps only its own text, which already includes its source's.
macro_rules! on_pool {
    ($pool:expr, $deadline:expr, |$c:ident| $body:expr) => {{
        let mut conn = $pool.acquire().await.map_err(|e| anyhow::anyhow!("{e}"))?;
        count_statement();
        let $c = &mut *conn;
        match tokio::time::timeout($deadline, $body).await {
            Ok(r) => r.map_err(|e| anyhow::anyhow!("{e}")),
            Err(_) => {
                conn.close_on_drop();
                Err(anyhow::anyhow!("no answer within the client deadline"))
            }
        }
    }};
}

/// `read`, or no data when it fails: logged, and `DB_FAILED` set. Once a read
/// in this request has failed, the response will be a 503 whatever the rest
/// find, so they degrade at once rather than each waiting out the database.
async fn degrading<T: Default>(what: &str, read: impl Future<Output = anyhow::Result<T>>) -> T {
    if super::DB_FAILED.try_with(|f| f.get()).unwrap_or(false) {
        return T::default();
    }
    read.await.unwrap_or_else(|e| {
        tracing::warn!("{what}: {e}");
        flag_failure();
        T::default()
    })
}

/// Map every row, dropping (and logging) the ones that do not decode, as
/// `sqlite.rs`'s `query_rows` does.
fn map_rows<T>(
    what: &str,
    rows: Vec<PgRow>,
    map: impl Fn(&PgRow) -> Result<T, sqlx::Error>,
) -> Vec<T> {
    let mut out = Vec::with_capacity(rows.len());
    let (mut dropped, mut first) = (0usize, None);
    for row in &rows {
        match map(row) {
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
    out
}

pub(crate) async fn try_query_rows<T>(
    pool: &PgPool,
    what: &str,
    sql: &'static str,
    bind: impl FnOnce(PgQuery) -> PgQuery,
    map: impl Fn(&PgRow) -> Result<T, sqlx::Error>,
) -> anyhow::Result<Vec<T>> {
    let q = bind(sqlx::query(sql));
    let rows = on_pool!(pool, READ_DEADLINE, |c| q.fetch_all(c))?;
    Ok(map_rows(what, rows, map))
}

/// Every row, or none (logged, `DB_FAILED` set) when the query fails.
pub(crate) async fn query_rows<T>(
    pool: &PgPool,
    what: &str,
    sql: &'static str,
    bind: impl FnOnce(PgQuery) -> PgQuery,
    map: impl Fn(&PgRow) -> Result<T, sqlx::Error>,
) -> Vec<T> {
    degrading(what, try_query_rows(pool, what, sql, bind, map)).await
}

pub(crate) async fn try_query_opt<T>(
    pool: &PgPool,
    what: &str,
    sql: &'static str,
    bind: impl FnOnce(PgQuery) -> PgQuery,
    map: impl Fn(&PgRow) -> Result<T, sqlx::Error>,
) -> anyhow::Result<Option<T>> {
    let q = bind(sqlx::query(sql));
    let row = on_pool!(pool, READ_DEADLINE, |c| q.fetch_optional(c))?;
    Ok(map_rows(what, row.into_iter().collect(), map).pop())
}

/// The first row, or `None` when there is none or the query fails.
pub(crate) async fn query_opt<T>(
    pool: &PgPool,
    what: &str,
    sql: &'static str,
    bind: impl FnOnce(PgQuery) -> PgQuery,
    map: impl Fn(&PgRow) -> Result<T, sqlx::Error>,
) -> Option<T> {
    degrading(what, try_query_opt(pool, what, sql, bind, map)).await
}

/// A single `int8`, or 0 when the query fails.
pub(crate) async fn query_count(
    pool: &PgPool,
    what: &str,
    sql: &'static str,
    bind: impl FnOnce(PgQuery) -> PgQuery,
) -> i64 {
    query_opt(pool, what, sql, bind, |r| {
        sqlx::Row::try_get::<i64, _>(r, 0)
    })
    .await
    .unwrap_or(0)
}

/// A write to a true cache (selector names, traces), on the cache pool. A
/// failure is returned for the caller to log, and never sets `DB_FAILED`: the
/// page that asked has its answer either way.
pub(crate) async fn exec_best_effort(
    pool: &PgPool,
    what: &str,
    sql: &'static str,
    bind: impl FnOnce(PgQuery) -> PgQuery,
) -> anyhow::Result<()> {
    let q = bind(sqlx::query(sql));
    async { on_pool!(pool, CACHE_DEADLINE, |c| q.execute(c)) }
        .await
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("{what}: {e}"))
}

// Writer-side helpers: one statement on the writer's transaction, counted, with
// the error classified for the retry loop.

/// Run one writer statement: counted, under the attempt's client deadline
/// (`Batch` only), with its error classified.
pub(crate) async fn run<T>(
    what: &str,
    fut: impl Future<Output = Result<T, sqlx::Error>>,
) -> Result<T, DbError> {
    count_statement();
    let r = match DEADLINE.try_with(|d| *d).ok().flatten() {
        Some(limit) => tokio::time::timeout(limit, fut).await.map_err(|_| {
            DbError::Unavailable(anyhow::anyhow!("{what}: no answer within {limit:?}"))
        })?,
        None => fut.await,
    };
    r.map_err(|e| DbError::from_sqlx(what, e))
}

pub(crate) async fn exec(c: &mut PgConnection, what: &str, q: PgQuery) -> Result<u64, DbError> {
    run(what, q.execute(c)).await.map(|r| r.rows_affected())
}

pub(crate) async fn fetch_all(
    c: &mut PgConnection,
    what: &str,
    q: PgQuery,
) -> Result<Vec<PgRow>, DbError> {
    run(what, q.fetch_all(c)).await
}

/// A statement with no parameters, sent as a simple query.
pub(crate) async fn raw(
    c: &mut PgConnection,
    what: &str,
    sql: &'static str,
) -> Result<(), DbError> {
    run(what, sqlx::raw_sql(sql).execute(c)).await.map(|_| ())
}

/// Column `i` of a row a writer statement returned.
pub(crate) fn get<T: for<'r> sqlx::Decode<'r, Postgres> + sqlx::Type<Postgres>>(
    row: &PgRow,
    i: usize,
) -> Result<T, DbError> {
    sqlx::Row::try_get(row, i).map_err(|e| DbError::from_sqlx("decode", e))
}
