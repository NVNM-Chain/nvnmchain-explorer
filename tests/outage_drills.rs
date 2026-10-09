//! Half-open sessions: a proxy between the explorer and Postgres that, when
//! told to, keeps every socket open and passes nothing on, the way a dead
//! network path looks from the client.
//!
//! Needs a Postgres server: ignored unless run with `--include-ignored`, and
//! then failing without `PG_TEST_URL`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use nvnmchain_explorer::db::{self, Role, Status};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[path = "common/backend.rs"]
mod backend;

/// A TCP proxy to `upstream`; while `dark` is set, bytes are read and dropped.
async fn blackhole_proxy(upstream: String, dark: Arc<AtomicBool>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            let Ok(server) = TcpStream::connect(&upstream).await else {
                continue;
            };
            let (cr, cw) = client.into_split();
            let (sr, sw) = server.into_split();
            tokio::spawn(pipe(cr, sw, dark.clone()));
            tokio::spawn(pipe(sr, cw, dark.clone()));
        }
    });
    port
}

async fn pipe(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    dark: Arc<AtomicBool>,
) {
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => {
                if !dark.load(Ordering::Relaxed) && to.write_all(&buf[..n]).await.is_err() {
                    return;
                }
            }
        }
    }
}

/// A TCP proxy to `upstream` that passes everything until a client sends
/// `marker`, then goes dark on that one connection: nothing more passes either
/// way, and both its sockets stay open, as on a path whose resets are lost.
/// Connections made after it flow as normal.
async fn tripwire_proxy(upstream: String, marker: &'static [u8], tripped: Arc<AtomicBool>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            let Ok(server) = TcpStream::connect(&upstream).await else {
                continue;
            };
            let (cr, cw) = client.into_split();
            let (sr, sw) = server.into_split();
            let dark = Arc::new(AtomicBool::new(false));
            tokio::spawn(trip(cr, sw, dark.clone(), Some((marker, tripped.clone()))));
            tokio::spawn(trip(sr, cw, dark, None));
        }
    });
    port
}

async fn trip(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    dark: Arc<AtomicBool>,
    wire: Option<(&'static [u8], Arc<AtomicBool>)>,
) {
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let n = from.read(&mut buf).await.unwrap_or(0);
        if let Some((marker, tripped)) = &wire {
            if buf[..n].windows(marker.len()).any(|w| w == *marker)
                && tripped
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                dark.store(true, Ordering::SeqCst);
            }
        }
        if dark.load(Ordering::SeqCst) {
            if n == 0 {
                // Keep `to` open: no FIN reaches the other side.
                std::future::pending::<()>().await;
            }
            continue;
        }
        if n == 0 || to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

/// The migrations run under the `Long` watchdog too: a writer whose session
/// goes dark mid-migration drops it once the lock is out of sight, and leads
/// on a new one, rather than wait for an answer that never comes.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn the_watchdog_cancels_a_migration_it_cannot_see() {
    let (_scratch, url) = backend::scratch_schema().await;
    let tripped = Arc::new(AtomicBool::new(false));
    // The runner's first statement, which only the writer's session sends.
    let port = tripwire_proxy(
        upstream(&url),
        b"CREATE TABLE IF NOT EXISTS schema_migrations",
        tripped.clone(),
    )
    .await;
    let mut cfg = backend::pg_config(&via(&url, port), Role::Indexer);
    cfg.tuning.watchdog = Duration::from_secs(1);
    cfg.tuning.candidate_retry = Duration::from_millis(100);
    // The dark session's backend ends with its lease, which frees the lock.
    cfg.tuning.lease = Duration::from_secs(5);
    let (tx, status) = tokio::sync::watch::channel(Status::starting(Role::Indexer));
    let indexer = tokio::time::timeout(Duration::from_secs(60), db::open_with(&cfg, tx))
        .await
        .expect("the watchdog dropped the dark session, and a new one led")
        .unwrap();
    assert!(
        tripped.load(Ordering::SeqCst),
        "the writer's session went dark"
    );
    assert_eq!(status.borrow().writer, Some(db::WriterState::Leader));
    assert_eq!(
        db::schema_version(&indexer).await.unwrap(),
        db::migrations::binary_version()
    );
}

/// A TCP proxy to `upstream` that, while `refuse` is set, closes each new
/// connection at once, the way an unreachable server looks; connections
/// already made keep flowing.
async fn refusing_proxy(upstream: String, refuse: Arc<AtomicBool>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            if refuse.load(Ordering::Relaxed) {
                continue;
            }
            let Ok(server) = TcpStream::connect(&upstream).await else {
                continue;
            };
            let (cr, cw) = client.into_split();
            let (sr, sw) = server.into_split();
            let open = Arc::new(AtomicBool::new(false));
            tokio::spawn(pipe(cr, sw, open.clone()));
            tokio::spawn(pipe(sr, cw, open));
        }
    });
    port
}

/// The scratch schema's URL, sent through the proxy on `port`.
fn via(url: &str, port: u16) -> String {
    let mut u = url::Url::parse(url).unwrap();
    u.set_host(Some("127.0.0.1")).unwrap();
    u.set_port(Some(port)).unwrap();
    u.to_string()
}

fn upstream(url: &str) -> String {
    let u = url::Url::parse(url).unwrap();
    format!("{}:{}", u.host_str().unwrap(), u.port().unwrap_or(5432))
}

/// A web read on a half-open session fails within about the client deadline,
/// rather than hanging, and the pool recovers once the path does.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn a_half_open_read_fails_within_the_deadline_and_the_pool_recovers() {
    let (_scratch, url) = backend::scratch_schema().await;
    drop(backend::open(&backend::pg_config(&url, Role::Indexer)).await);
    let dark = Arc::new(AtomicBool::new(false));
    let port = blackhole_proxy(upstream(&url), dark.clone()).await;
    let web = backend::open(&backend::pg_config(&via(&url, port), Role::Web)).await;
    // The path goes dark under an idle pool. sqlx pings each connection as it
    // goes back to the pool, with no deadline, so let the last one land: caught
    // by the dark, it holds its slot.
    let (_, failed) = db::track_failures(db::get_latest_block(&web)).await;
    assert!(!failed);
    tokio::time::sleep(Duration::from_millis(200)).await;

    dark.store(true, Ordering::Relaxed);
    let started = Instant::now();
    let (_, failed) = db::track_failures(db::get_latest_block(&web)).await;
    let took = started.elapsed();
    assert!(failed, "a half-open read is a failure, for a 503");
    assert!(
        took < Duration::from_secs(9),
        "took {took:?}; the deadline is 7 s"
    );

    dark.store(false, Ordering::Relaxed);
    let (_, failed) = db::track_failures(db::get_latest_block(&web)).await;
    assert!(!failed, "the dead connection was closed, not pooled");
}

/// A `Long` job on a half-open session is cancelled by the watchdog once it
/// cannot see the lock: the session is dropped, and the work runs again on a
/// new one once the path is back.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn the_watchdog_cancels_long_work_it_cannot_see() {
    let (_scratch, url) = backend::scratch_schema().await;
    let dark = Arc::new(AtomicBool::new(false));
    let port = blackhole_proxy(upstream(&url), dark.clone()).await;
    let mut cfg = backend::pg_config(&via(&url, port), Role::Indexer);
    cfg.tuning.watchdog = Duration::from_secs(1);
    cfg.tuning.candidate_retry = Duration::from_millis(100);
    let indexer = backend::open(&cfg).await;
    let before = db::testing::writer_pid(&indexer).unwrap();
    let long = tokio::spawn({
        let indexer = indexer.clone();
        async move { db::testing::long_statement(&indexer, "SELECT pg_sleep(8)").await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    dark.store(true, Ordering::Relaxed);
    // Four missed checks, each up to the read deadline, cancel the work.
    tokio::time::sleep(Duration::from_secs(40)).await;
    dark.store(false, Ordering::Relaxed);
    tokio::time::timeout(Duration::from_secs(60), long)
        .await
        .expect("the work ran again once the path came back")
        .unwrap()
        .unwrap();
    assert_ne!(
        db::testing::writer_pid(&indexer).unwrap(),
        before,
        "the watchdog dropped the session it could not see"
    );
}

/// A database that cannot be reached is an outage, not a lost lock: the writer
/// waits in `Reacquiring` past `lost_after`, for as long as it takes, and the
/// interrupted write lands once the database is back. Only a lock another
/// session keeps holding gives up (exit 3).
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn an_unreachable_database_is_waited_out_not_taken_for_a_lost_lock() {
    let (_scratch, url) = backend::scratch_schema().await;
    let refuse = Arc::new(AtomicBool::new(false));
    let port = refusing_proxy(upstream(&url), refuse.clone()).await;
    let mut cfg = backend::pg_config(&via(&url, port), Role::Indexer);
    cfg.tuning.lost_after = Duration::from_secs(1);
    cfg.tuning.candidate_retry = Duration::from_millis(100);
    cfg.tuning.catch_exits = true;
    let (tx, status) = tokio::sync::watch::channel(Status::starting(Role::Indexer));
    let indexer = db::open_with(&cfg, tx).await.unwrap();
    let pid = db::testing::writer_pid(&indexer).unwrap();

    refuse.store(true, Ordering::Relaxed);
    backend::exec(&url, &format!("SELECT pg_terminate_backend({pid})")).await;
    let write = tokio::spawn({
        let indexer = indexer.clone();
        async move {
            let token = nvnmchain_explorer::tokens::TokenMeta {
                address: format!("0x{}", "20".repeat(20)),
                name: "Outage".into(),
                symbol: "OUT".into(),
                decimals: 6,
                currency: "USD".into(),
                total_supply: "1".into(),
            };
            db::save_token_metadata(&indexer, &token).await
        }
    });
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(!write.is_finished(), "still waiting, well past lost_after");
    assert_eq!(status.borrow().writer, Some(db::WriterState::Reacquiring));

    refuse.store(false, Ordering::Relaxed);
    tokio::time::timeout(Duration::from_secs(40), write)
        .await
        .expect("the write lands once the database is back")
        .unwrap()
        .unwrap();
}
