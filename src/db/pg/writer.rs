//! The one writer: an owned session that holds a session-level advisory lock.
//!
//! The session is the fence. A session-level lock ends only with an unlock or
//! with the session, and at backend exit Postgres aborts any open transaction
//! before it releases locks, so a new leader never overlaps an old session
//! that can still commit. That holds while every chain-derived write uses this
//! session and it is never pooled. To step down, the writer drops the session;
//! it never unlocks.
//!
//! Each write runs on a spawned task, so a dropped caller (a closed browser
//! tab, a request timeout) never cuts a transaction short. A failure that is
//! not the write's own content (`Unavailable`) drops the session and retries
//! the same idempotent write, re-acquiring the lock first, until it commits.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use sha3::{Digest, Sha3_256};
use sqlx::postgres::PgConnectOptions;
use sqlx::{Connection, PgConnection, PgPool, Row};
use tokio::sync::{watch, Mutex};

use super::q;
use super::{connect, migrate, DbError};
use crate::db::{migrations, now_ts, DbConfig, Preflight, Status, Tuning, WriterState};

/// The lock's first key: "NVNM".
pub(crate) const K1: i32 = 0x4E56_4E4D;

pub(crate) type TxFuture<'c, T> = Pin<Box<dyn Future<Output = Result<T, DbError>> + Send + 'c>>;

/// How long a write may take.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Budget {
    /// Normal writes: the server's 60 s `statement_timeout`, and a client
    /// deadline of 70 s per statement, so the server cancels first.
    Batch,
    /// Migrations, repairs and rebuilds: no statement timeout, supervised by a
    /// watchdog that checks the lock is still held.
    Long,
}

const BUMP_WRITER_SEQ: &str = "INSERT INTO kv (key, value, updated_at) VALUES ('writer_seq', $1 || ':1', $2)
     ON CONFLICT (key) DO UPDATE
       SET value = $1 || ':' || (split_part(kv.value, ':', 2)::bigint + 1), updated_at = excluded.updated_at
     RETURNING split_part(value, ':', 2)::int8";

#[derive(Clone)]
pub(crate) struct Writer(Arc<Inner>);

struct Inner {
    opts: PgConnectOptions,
    read: PgPool,
    slot: Mutex<Option<PgConnection>>,
    key: (i32, i32),
    /// This process's id in `writer_seq`.
    run_id: String,
    /// The newest `writer_seq` this process has committed; 0 before the first.
    last_seq: AtomicI64,
    /// The backend that last held the lock for this process.
    pid: AtomicI32,
    /// Unix time of the last commit or keepalive, for metrics.
    last_ok: AtomicI64,
    tuning: Tuning,
    web_role: String,
    status: watch::Sender<Status>,
    /// Run each time this process becomes leader again: the label re-seed.
    on_leader: OnceLock<Box<dyn Fn() + Send + Sync>>,
}

/// Why starting failed: a refusal ends the process; anything else is retried.
enum StartError {
    Refused(anyhow::Error),
    Retry(anyhow::Error),
}

impl Writer {
    /// Connect, pass the preflight, wait as a candidate until the lock is free,
    /// check the session, migrate and grant. Connection failures are retried
    /// with backoff while the process stays unready; a preflight refusal is an
    /// error, which ends the process.
    pub(crate) async fn start(
        opts: PgConnectOptions,
        read: PgPool,
        cfg: &DbConfig,
        status: watch::Sender<Status>,
    ) -> Result<Writer> {
        let tuning = cfg.tuning.clone();
        let opts = session_options(opts, &tuning);
        let mut backoff = Duration::from_secs(1);
        let (conn, key) = loop {
            match candidate(&opts, &tuning, &status).await {
                Ok(found) => break found,
                Err(StartError::Refused(e)) => return Err(e),
                Err(StartError::Retry(e)) => {
                    tracing::warn!("writer: {e:#}; retrying in {backoff:?}");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        };
        let writer = Writer(Arc::new(Inner {
            opts,
            read,
            slot: Mutex::new(None),
            key,
            run_id: run_id(),
            last_seq: AtomicI64::new(0),
            pid: AtomicI32::new(0),
            last_ok: AtomicI64::new(now_ts()),
            tuning,
            web_role: cfg.web_role.clone(),
            status,
            on_leader: OnceLock::new(),
        }));
        let mut conn = conn;
        match writer.lead(&mut conn).await {
            Ok(()) => {}
            Err(DbError::Unavailable(e)) => {
                // A fresh session and the lock again; that path leads too.
                tracing::warn!("writer: {e:#}; reconnecting");
                drop(conn);
                conn = writer
                    .reacquire_session()
                    .await
                    .map_err(|e| anyhow!("{e}"))?;
            }
            Err(e) => return Err(anyhow!("{e}")),
        }
        *writer.0.slot.lock().await = Some(conn);
        Ok(writer)
    }

    /// Run `f` once per attempt inside a transaction on the writer session, on
    /// a spawned task, until it commits or fails on its content.
    pub(crate) async fn write<T, F>(&self, budget: Budget, f: F) -> Result<T, DbError>
    where
        T: Send + 'static,
        F: for<'c> Fn(&'c mut PgConnection) -> TxFuture<'c, T> + Send + Sync + 'static,
    {
        let this = self.clone();
        match tokio::spawn(async move { this.write_retrying(budget, f).await }).await {
            Ok(r) => r,
            Err(e) => std::panic::resume_unwind(e.into_panic()),
        }
    }

    async fn write_retrying<T, F>(&self, mut budget: Budget, f: F) -> Result<T, DbError>
    where
        F: for<'c> Fn(&'c mut PgConnection) -> TxFuture<'c, T> + Send + Sync,
    {
        let mut backoff = Duration::from_secs(1);
        loop {
            let mut slot = self.0.slot.lock().await;
            let mut conn = match slot.take() {
                Some(conn) => conn,
                None => self.reacquire_session().await?,
            };
            let deadline = (budget == Budget::Batch).then_some(Duration::from_secs(70));
            let attempt = q::DEADLINE.scope(deadline, self.attempt(&mut conn, budget, &f));
            let r = if budget == Budget::Long {
                tokio::select! {
                    r = attempt => r,
                    e = self.watchdog() => Err(e),
                }
            } else {
                attempt.await
            };
            match r {
                Ok((v, seq)) => {
                    self.0.last_seq.fetch_max(seq, Relaxed);
                    self.0.last_ok.store(now_ts(), Relaxed);
                    *slot = Some(conn);
                    return Ok(v);
                }
                Err(e @ (DbError::Data(_) | DbError::NotWriter | DbError::Fatal(_))) => {
                    *slot = Some(conn);
                    return Err(e);
                }
                Err(DbError::Unavailable(e)) => {
                    // The session goes, and the lock with it; the next attempt
                    // has to win `try_lock` again.
                    drop(conn);
                    drop(slot);
                    if format!("{e:#}").contains("57014") {
                        budget = Budget::Long;
                    }
                    tracing::warn!("writer: {e:#}; retrying in {backoff:?}");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        }
    }

    /// One attempt: BEGIN, the work, the `writer_seq` bump, COMMIT. Dropping
    /// the transaction before COMMIT queues a ROLLBACK.
    async fn attempt<T, F>(
        &self,
        conn: &mut PgConnection,
        budget: Budget,
        f: &F,
    ) -> Result<(T, i64), DbError>
    where
        F: for<'c> Fn(&'c mut PgConnection) -> TxFuture<'c, T> + Send + Sync,
    {
        let mut tx = q::run("begin", conn.begin()).await?;
        if budget == Budget::Long {
            q::raw(&mut tx, "budget", "SET LOCAL statement_timeout = 0").await?;
        }
        let v = f(&mut tx).await?;
        let seq: i64 = q::run(
            "writer_seq",
            sqlx::query_scalar(BUMP_WRITER_SEQ)
                .bind(&self.0.run_id)
                .bind(now_ts())
                .fetch_one(&mut *tx),
        )
        .await?;
        q::run("commit", tx.commit()).await?;
        Ok((v, seq))
    }

    /// For `Long` work: every `watchdog` interval, check from the read pool that
    /// this process still holds the lock. Four misses in a row cancel the work.
    async fn watchdog(&self) -> DbError {
        let mut misses = 0;
        loop {
            tokio::time::sleep(self.0.tuning.watchdog).await;
            if self.holds_lock().await {
                misses = 0;
            } else {
                misses += 1;
                tracing::warn!("writer watchdog: lock not confirmed ({misses} of 4)");
                if misses >= 4 {
                    return DbError::Unavailable(anyhow!("the watchdog lost sight of the lock"));
                }
            }
        }
    }

    /// Whether `pg_locks` shows this process's backend holding the lock; a
    /// failed read is a no.
    async fn holds_lock(&self) -> bool {
        let pid = self.0.pid.load(Relaxed);
        let (k1, k2) = self.0.key;
        let held = q::try_query_opt(
            &self.0.read,
            "holds_lock",
            "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND classid::int8 = $1 \
             AND objid::int8 = $2 AND objsubid = 2 AND pid = $3 AND granted)",
            // The keys as `pg_locks` shows them: unsigned, in `oid` columns.
            |q| {
                q.bind(i64::from(k1 as u32))
                    .bind(i64::from(k2 as u32))
                    .bind(pid)
            },
            |r| r.try_get::<bool, _>(0),
        )
        .await;
        matches!(held, Ok(Some(true)))
    }

    /// The lease's heartbeat: `SELECT 1` on the writer session within 5 s. A
    /// session that does not answer is dropped; the next write re-acquires.
    pub(crate) async fn keepalive(&self) {
        let mut slot = self.0.slot.lock().await;
        let Some(conn) = slot.as_mut() else {
            return;
        };
        q::count_statement();
        let ok = tokio::time::timeout(
            Duration::from_secs(5),
            sqlx::query("SELECT 1").execute(conn),
        )
        .await
        .is_ok_and(|r| r.is_ok());
        if ok {
            self.0.last_ok.store(now_ts(), Relaxed);
        } else {
            tracing::warn!("writer keepalive failed; dropping the session");
            slot.take();
        }
    }

    /// The backend that last held the lock for this process.
    pub(crate) fn pid(&self) -> i32 {
        self.0.pid.load(Relaxed)
    }

    pub(crate) fn last_ok(&self) -> i64 {
        self.0.last_ok.load(Relaxed)
    }

    pub(crate) fn set_on_leader(&self, f: Box<dyn Fn() + Send + Sync>) {
        let _ = self.0.on_leader.set(f);
    }

    fn set_state(&self, state: WriterState) {
        self.0.status.send_modify(|s| s.writer = Some(state));
    }

    /// Exit with `code`, or report it, under `catch_exits`.
    fn fatal(&self, code: i32, why: String) -> DbError {
        tracing::error!("{why}; exiting with code {code}");
        if self.0.tuning.catch_exits {
            DbError::Fatal(code)
        } else {
            std::process::exit(code)
        }
    }

    /// A new session holding the lock, after a failure dropped the last one.
    /// An unreachable database is waited out however long it takes. Gives up
    /// once another session has kept the lock for `lost_after` (exit 3), exits
    /// 1 on a replica, and exits 4 when the database is not at this process's
    /// `writer_seq`.
    async fn reacquire_session(&self) -> Result<PgConnection, DbError> {
        self.set_state(WriterState::Reacquiring);
        let window = self.0.tuning.lost_after;
        let mut patience = Patience {
            window,
            busy_since: None,
        };
        let mut backoff = Duration::from_secs(1);
        loop {
            // Whether another session holds the lock.
            let busy = match self.try_reacquire().await {
                Ok(Some(mut conn)) => match self.check_seq(&mut conn).await {
                    Ok(()) => match self.lead(&mut conn).await {
                        Ok(()) => {
                            if let Some(f) = self.0.on_leader.get() {
                                f();
                            }
                            return Ok(conn);
                        }
                        Err(DbError::Unavailable(e)) => {
                            tracing::warn!("writer: {e:#}");
                            false
                        }
                        Err(e) => return Err(e),
                    },
                    Err(e @ DbError::Fatal(_)) => return Err(e),
                    Err(e) => {
                        tracing::warn!("writer re-acquire: {e:#}");
                        false
                    }
                },
                Ok(None) => true,
                Err(e) => {
                    tracing::warn!("writer re-acquire: {e:#}");
                    false
                }
            };
            if patience.lost(busy, Instant::now()) {
                return Err(self.fatal(
                    3,
                    format!("writer lost the lock: another session has held it for {window:?}"),
                ));
            }
            if busy {
                tokio::time::sleep(self.0.tuning.candidate_retry.min(backoff)).await;
                continue;
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    }

    /// One try: end the old backend if it still holds the lock, connect, and
    /// ask for the lock. `None` when another session has it.
    async fn try_reacquire(&self) -> Result<Option<PgConnection>> {
        let old = self.0.pid.load(Relaxed);
        if old != 0 && self.holds_lock().await {
            tracing::warn!("writer: terminating backend {old}, which still holds the lock");
            q::try_query_opt(
                &self.0.read,
                "terminate",
                "SELECT pg_terminate_backend($1)",
                |q| q.bind(old),
                |r| r.try_get::<bool, _>(0),
            )
            .await?;
        }
        let mut conn = connect(&self.0.opts).await?;
        check_lease(&mut conn, &self.0.tuning).await?;
        Ok(try_lock(&mut conn, self.0.key).await?.then_some(conn))
    }

    /// After a re-acquire: the database must be at this process's last commit,
    /// or one past it (a COMMIT that applied but whose acknowledgement was
    /// lost). Anything else means commits were lost, and a restart re-derives
    /// them from the database's truth.
    async fn check_seq(&self, conn: &mut PgConnection) -> Result<(), DbError> {
        let last = self.0.last_seq.load(Relaxed);
        if last == 0 {
            return Ok(());
        }
        let stored: Option<String> = q::run(
            "writer_seq",
            sqlx::query_scalar("SELECT value FROM kv WHERE key = 'writer_seq'")
                .fetch_optional(&mut *conn),
        )
        .await?;
        let (run, n) = stored
            .as_deref()
            .and_then(|v| v.split_once(':'))
            .map(|(r, n)| (r.to_string(), n.parse::<i64>().unwrap_or(-1)))
            .unwrap_or_default();
        if run == self.0.run_id && (n == last || n == last + 1) {
            self.0.last_seq.store(n, Relaxed);
            Ok(())
        } else {
            Err(self.fatal(
                4,
                format!(
                    "writer_seq is {stored:?}, not this run's {}:{last}; commits were lost",
                    self.0.run_id
                ),
            ))
        }
    }

    /// With the lock just won: check the session, record its pid, run the
    /// preflight again, migrate and grant, and say so. The candidate's
    /// preflight ran before a wait with no bound, and a re-acquire skips it, so
    /// a newer release may have migrated in between; under the lock, D cannot
    /// move. The migrations have no statement timeout, so like other `Long`
    /// work they run under the watchdog.
    async fn lead(&self, conn: &mut PgConnection) -> Result<(), DbError> {
        let pid = self.self_checks(conn).await?;
        self.0.pid.store(pid, Relaxed);
        let migrate = async {
            match migrate::preflight(conn).await {
                Ok(()) => {}
                Err(migrate::PreflightError::Refused(e)) => {
                    return Err(self.fatal(1, format!("{e:#}")))
                }
                Err(migrate::PreflightError::Unavailable(e)) => {
                    return Err(DbError::Unavailable(e))
                }
            }
            migrate::run(conn, &self.0.web_role).await
        };
        tokio::select! {
            r = migrate => r?,
            e = self.watchdog() => return Err(e),
        }
        self.0.status.send_modify(|s| {
            s.writer = Some(WriterState::Leader);
            s.schema.db = Some(migrations::binary_version());
        });
        self.0.last_ok.store(now_ts(), Relaxed);
        tracing::info!("writer: leader (backend {pid})");
        Ok(())
    }

    /// A primary, and exactly one granted lock row: ours. A replica (the
    /// Kubernetes `-ro` Service, a read-replica address) is a misconfiguration
    /// that waiting cannot fix, so it exits 1 at once.
    async fn self_checks(&self, conn: &mut PgConnection) -> Result<i32, DbError> {
        let (primary, pid, locks): (bool, i32, i64) = q::run(
            "self_checks",
            sqlx::query_as(
                "SELECT NOT pg_is_in_recovery(), pg_backend_pid(), \
                 (SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' \
                  AND pid = pg_backend_pid() AND objsubid = 2 AND granted)",
            )
            .fetch_one(&mut *conn),
        )
        .await?;
        if !primary {
            return Err(self.fatal(
                1,
                "the server is in recovery: DATABASE_URL points at a replica, such as the \
                 Kubernetes -ro Service, not the primary"
                    .to_string(),
            ));
        }
        if locks != 1 {
            return Err(DbError::Unavailable(anyhow!(
                "this session holds {locks} advisory locks, not 1"
            )));
        }
        Ok(pid)
    }
}

/// How long a re-acquire keeps trying. Only another session holding the lock
/// for the whole window gives up; an unreachable database is an outage,
/// waited out however long it takes, and starts the window over.
struct Patience {
    window: Duration,
    busy_since: Option<Instant>,
}

impl Patience {
    /// Whether the lock is lost, given whether this try found it held.
    fn lost(&mut self, busy: bool, now: Instant) -> bool {
        if !busy {
            self.busy_since = None;
            return false;
        }
        now.duration_since(*self.busy_since.get_or_insert(now)) > self.window
    }
}

/// A hash of the clock and the process: enough to tell two runs apart.
fn run_id() -> String {
    let seed = format!(
        "{:?}{}{:?}",
        std::time::SystemTime::now(),
        std::process::id(),
        std::thread::current().id()
    );
    hex::encode(&Sha3_256::digest(seed.as_bytes())[..8])
}

/// The writer's own settings, in its startup options.
fn session_options(opts: PgConnectOptions, tuning: &Tuning) -> PgConnectOptions {
    opts.options([
        ("statement_timeout", "60s".to_string()),
        ("lock_timeout", "5s".to_string()),
        ("idle_session_timeout", lease(tuning)),
        ("idle_in_transaction_session_timeout", "30s".to_string()),
        ("tcp_keepalives_idle", "10".to_string()),
        ("tcp_keepalives_interval", "5".to_string()),
        ("tcp_keepalives_count", "3".to_string()),
        ("tcp_user_timeout", "20000".to_string()),
        ("client_connection_check_interval", "10s".to_string()),
    ])
}

fn lease(tuning: &Tuning) -> String {
    format!("{}ms", tuning.lease.as_millis())
}

/// The lease must be this session's own: something that strips startup
/// options (a pooler) would leave the lock without one.
async fn check_lease(conn: &mut PgConnection, tuning: &Tuning) -> Result<()> {
    let ms: i64 = sqlx::query(
        "SELECT (EXTRACT(EPOCH FROM current_setting('idle_session_timeout')::interval) * 1000)::int8",
    )
    .fetch_one(&mut *conn)
    .await
    .context("read idle_session_timeout")?
    .try_get(0)?;
    if ms != tuning.lease.as_millis() as i64 {
        bail!(
            "the writer session's idle_session_timeout is {ms} ms, not its own {}; \
             is a pooler stripping startup options? Connect to the primary directly",
            lease(tuning)
        );
    }
    Ok(())
}

/// `K_WRITER`: the first four bytes of sha3-256("<schema>:writer"), so parallel
/// test schemas in one database never contend.
pub(crate) fn writer_key(schema: &str) -> i32 {
    let digest = Sha3_256::digest(format!("{schema}:writer").as_bytes());
    i32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]])
}

async fn try_lock(conn: &mut PgConnection, key: (i32, i32)) -> Result<bool> {
    q::count_statement();
    let row = tokio::time::timeout(
        Duration::from_secs(5),
        sqlx::query("SELECT pg_try_advisory_lock($1, $2)")
            .bind(key.0)
            .bind(key.1)
            .fetch_one(&mut *conn),
    )
    .await
    .context("try_lock: no answer within 5 s")??;
    Ok(row.try_get(0)?)
}

/// Connect, pass the preflight, and wait as a candidate until `try_lock`
/// succeeds, asking every `candidate_retry` on the same session so it never
/// idles past its lease.
async fn candidate(
    opts: &PgConnectOptions,
    tuning: &Tuning,
    status: &watch::Sender<Status>,
) -> Result<(PgConnection, (i32, i32)), StartError> {
    let mut conn = connect(opts).await.map_err(StartError::Retry)?;
    check_lease(&mut conn, tuning)
        .await
        .map_err(StartError::Refused)?;
    match migrate::preflight(&mut conn).await {
        Ok(()) => {}
        Err(migrate::PreflightError::Refused(e)) => return Err(StartError::Refused(e)),
        Err(migrate::PreflightError::Unavailable(e)) => return Err(StartError::Retry(e)),
    }
    let schema: String = sqlx::query("SELECT current_schema()")
        .fetch_one(&mut conn)
        .await
        .map_err(|e| StartError::Retry(e.into()))?
        .try_get(0)
        .map_err(|e| StartError::Retry(e.into()))?;
    let key = (K1, writer_key(&schema));
    status.send_modify(|s| {
        s.preflight = Some(Preflight::Passed);
        s.writer = Some(WriterState::Candidate);
    });
    loop {
        match try_lock(&mut conn, key).await {
            Ok(true) => return Ok((conn, key)),
            Ok(false) => tokio::time::sleep(tuning.candidate_retry).await,
            Err(e) => return Err(StartError::Retry(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(t0: Instant, secs: u64) -> Instant {
        t0 + Duration::from_secs(secs)
    }

    /// Only a lock another session keeps holding runs out the window. An
    /// outage, however long, is waited out, and starts the window over.
    #[test]
    fn a_held_lock_gives_up_after_the_window_and_an_outage_never_does() {
        let t0 = Instant::now();
        let mut p = Patience {
            window: Duration::from_secs(120),
            busy_since: None,
        };
        assert!(!p.lost(true, at(t0, 0)));
        assert!(!p.lost(true, at(t0, 119)));
        assert!(!p.lost(false, at(t0, 200)));
        assert!(!p.lost(false, at(t0, 10_000)));
        assert!(!p.lost(true, at(t0, 10_001)));
        assert!(p.lost(true, at(t0, 10_122)));
    }
}
