//! The writer's lock: one writer per database, and no block lost when its
//! session dies.
//!
//! Needs a Postgres server: ignored unless run with `--include-ignored`, and
//! then failing without `PG_TEST_URL`.

use std::time::Duration;

use nvnmchain_explorer::db::{self, Db, DbConfig, DbError, Role, Status, WriterState};
use nvnmchain_explorer::decoder::checksum_address;
use nvnmchain_explorer::models::{Block, BlockBundle};
use nvnmchain_explorer::tokens::TokenMeta;
use sqlx::{AssertSqlSafe, Connection as _, PgConnection, Row as _};
use tokio::sync::watch;

#[path = "common/backend.rs"]
mod backend;
use backend::exec;

fn fast(url: &str, role: Role) -> DbConfig {
    let mut cfg = backend::pg_config(url, role);
    cfg.tuning.candidate_retry = Duration::from_millis(100);
    cfg.tuning.lost_after = Duration::from_secs(20);
    cfg.tuning.catch_exits = true;
    cfg
}

async fn open(cfg: &DbConfig) -> (Db, watch::Receiver<Status>) {
    let (tx, rx) = watch::channel(Status::starting(cfg.role));
    (db::open_with(cfg, tx).await.expect("open"), rx)
}

fn bundle(number: i64) -> BlockBundle {
    BlockBundle {
        block: Block {
            number,
            hash: format!("0x{number:064x}"),
            parent_hash: format!("0x{:064x}", number - 1),
            timestamp: 1_700_000_000 + number,
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
        },
        txs: Vec::new(),
        transfers: Vec::new(),
        anchoring: Vec::new(),
        tokens: Vec::new(),
    }
}

async fn count(url: &str, sql: &str) -> i64 {
    let mut conn = PgConnection::connect(url).await.expect("connect");
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_one(&mut conn)
        .await
        .unwrap()
        .get(0)
}

async fn terminate(url: &str, pid: i32) {
    exec(url, &format!("SELECT pg_terminate_backend({pid})")).await;
}

#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn a_second_writer_waits_as_a_candidate_until_the_lock_is_free() {
    let (_scratch, url) = backend::scratch_schema().await;
    let (first, _) = open(&fast(&url, Role::Indexer)).await;

    let cfg = fast(&url, Role::Indexer);
    let (tx, mut status) = watch::channel(Status::starting(Role::Indexer));
    let second = tokio::spawn(async move { db::open_with(&cfg, tx).await });
    status
        .wait_for(|s| s.writer == Some(WriterState::Candidate))
        .await
        .unwrap();
    assert!(
        status.borrow().ready(),
        "a candidate is ready: it passed the preflight"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!second.is_finished(), "the lock is held");

    drop(first);
    let second = tokio::time::timeout(Duration::from_secs(10), second)
        .await
        .expect("the lock is free once the first writer closes")
        .unwrap()
        .unwrap();
    assert_eq!(status.borrow().writer, Some(WriterState::Leader));
    db::save_block_bundle(&second, &bundle(1)).await.unwrap();
}

/// A newer release may lead, and migrate, while a candidate waits: the
/// candidate re-runs the preflight once it holds the lock, and refuses rather
/// than run an older binary against the newer schema.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn a_candidate_refuses_a_database_migrated_while_it_waited() {
    let (_scratch, url) = backend::scratch_schema().await;
    let (first, _) = open(&fast(&url, Role::Indexer)).await;

    let cfg = fast(&url, Role::Indexer);
    let (tx, mut status) = watch::channel(Status::starting(Role::Indexer));
    let second = tokio::spawn(async move { db::open_with(&cfg, tx).await });
    status
        .wait_for(|s| s.writer == Some(WriterState::Candidate))
        .await
        .unwrap();
    // What a newer release leaves behind once it has led.
    let newer = db::migrations::binary_version() + 1;
    exec(
        &url,
        &format!("INSERT INTO schema_migrations VALUES ({newer}, 'later', 'x', 0, 'test')"),
    )
    .await;

    drop(first);
    let err = tokio::time::timeout(Duration::from_secs(10), second)
        .await
        .expect("the lock is free once the first writer closes")
        .expect("no panic: a newer database is refused under the lock")
        .err()
        .expect("refused");
    let err = format!("{err:#}");
    assert!(err.contains("exit 1"), "{err}");
    assert_eq!(
        count(&url, "SELECT MAX(version) FROM schema_migrations").await,
        newer
    );
}

#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn two_indexers_starting_together_migrate_once() {
    let (_scratch, url) = backend::scratch_schema().await;
    let (a, b) = (fast(&url, Role::Indexer), fast(&url, Role::Indexer));
    let mut first = tokio::spawn(async move { open(&a).await });
    let mut second = tokio::spawn(async move { open(&b).await });
    let (winner, loser) = tokio::select! {
        r = &mut first => (r.unwrap(), second),
        r = &mut second => (r.unwrap(), first),
    };
    assert_eq!(
        count(&url, "SELECT COUNT(*) FROM schema_migrations").await,
        db::migrations::binary_version()
    );
    assert!(!loser.is_finished());
    drop(winner);
    let _ = tokio::time::timeout(Duration::from_secs(10), loser)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        count(&url, "SELECT COUNT(*) FROM schema_migrations").await,
        db::migrations::binary_version()
    );
}

/// A killed session takes the lock with it; the next write wins it back and
/// writes, and nothing is lost.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn a_terminated_session_is_reacquired_without_losing_a_block() {
    let (_scratch, url) = backend::scratch_schema().await;
    let (db, status) = open(&fast(&url, Role::Indexer)).await;
    db::save_block_bundle(&db, &bundle(1)).await.unwrap();
    let pid = db::testing::writer_pid(&db).unwrap();

    terminate(&url, pid).await;
    db::save_block_bundle(&db, &bundle(2)).await.unwrap();

    assert_ne!(db::testing::writer_pid(&db).unwrap(), pid, "a new session");
    assert_eq!(status.borrow().writer, Some(WriterState::Leader));
    assert_eq!(count(&url, "SELECT COUNT(*) FROM blocks").await, 2);
}

/// A failure on the way back to leading, here the `writer_seq` read, is an
/// outage like any other: the write waits for it, and is never failed by it.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn a_failure_while_reacquiring_is_retried_not_returned() {
    let (_scratch, url) = backend::scratch_schema().await;
    let (db, _status) = open(&fast(&url, Role::Indexer)).await;
    db::save_block_bundle(&db, &bundle(1)).await.unwrap();
    let pid = db::testing::writer_pid(&db).unwrap();

    exec(&url, "ALTER TABLE kv RENAME TO kv_away").await;
    terminate(&url, pid).await;
    let write = tokio::spawn({
        let db = db.clone();
        async move { db::save_block_bundle(&db, &bundle(2)).await }
    });
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(!write.is_finished(), "the write waits rather than failing");

    exec(&url, "ALTER TABLE kv_away RENAME TO kv").await;
    tokio::time::timeout(Duration::from_secs(30), write)
        .await
        .expect("the write lands once the read succeeds")
        .unwrap()
        .unwrap();
    assert_eq!(count(&url, "SELECT COUNT(*) FROM blocks").await, 2);
}

/// A write runs on its own task, so a caller that goes away (a closed tab, a
/// timeout) cannot cut it short. Its tokens are still labelled: the label
/// cache is updated by the task that waits for the commit, not by the caller.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn a_dropped_caller_does_not_abort_its_batch() {
    let (_scratch, url) = backend::scratch_schema().await;
    let (db, _status) = open(&fast(&url, Role::Indexer)).await;
    db::save_block_bundle(&db, &bundle(1)).await.unwrap();
    let pid = db::testing::writer_pid(&db).unwrap();
    // Slow every block insert, so the caller is gone mid-batch.
    exec(
        &url,
        "CREATE FUNCTION slow() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.05); RETURN NEW; END $$;
         CREATE TRIGGER slow BEFORE INSERT ON blocks FOR EACH ROW EXECUTE FUNCTION slow();",
    )
    .await;
    let token = checksum_address(&format!("0x{}", "ab".repeat(20)));
    let mut batch: Vec<BlockBundle> = (10..40).map(bundle).collect();
    batch[0].tokens.push(TokenMeta {
        address: token.clone(),
        name: "Label Coin".into(),
        symbol: "LBL".into(),
        decimals: 6,
        currency: "USD".into(),
        total_supply: "0".into(),
    });
    let call = db::save_block_bundles(&db, &batch);
    assert!(tokio::time::timeout(Duration::from_millis(200), call)
        .await
        .is_err());

    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        count(&url, "SELECT COUNT(*) FROM blocks").await,
        31,
        "the batch committed"
    );
    assert_eq!(db::testing::writer_pid(&db), Some(pid), "the same session");
    assert_eq!(db::token_label(&db, &token).as_deref(), Some("LBL"));
}

/// The lease: a holder that stops talking loses its session to
/// `idle_session_timeout`, and the lock with it.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn an_idle_holder_loses_the_lock_to_its_lease() {
    let (_scratch, url) = backend::scratch_schema().await;
    let mut cfg = fast(&url, Role::Indexer);
    cfg.tuning.lease = Duration::from_secs(2);
    let (db, _status) = open(&cfg).await;
    let pid = db::testing::writer_pid(&db).unwrap();
    let held = format!("SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = {pid}");
    assert_eq!(count(&url, &held).await, 1);
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(count(&url, &held).await, 0, "the lease expired");
    // And the writer recovers on its next write.
    db::save_block_bundle(&db, &bundle(5)).await.unwrap();
}

/// `writer_seq`: after a re-acquire, the database must be at this process's
/// last commit. Here another writer committed in between, so this one must
/// stop (exit 4) and restart from the database's truth.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn a_writer_that_lost_commits_exits_4() {
    let (_scratch, url) = backend::scratch_schema().await;
    let (a, _) = open(&fast(&url, Role::Indexer)).await;
    db::save_block_bundle(&a, &bundle(1)).await.unwrap();
    db::save_block_bundle(&a, &bundle(2)).await.unwrap();
    let pid = db::testing::writer_pid(&a).unwrap();

    // A's session dies with its last batch; B leads and commits past A.
    terminate(&url, pid).await;
    exec(&url, "DELETE FROM blocks WHERE number = 2").await;
    let (b, _) = open(&fast(&url, Role::Indexer)).await;
    db::save_block_bundle(&b, &bundle(3)).await.unwrap();
    drop(b);

    let err = db::save_block_bundle(&a, &bundle(4)).await.unwrap_err();
    assert!(
        matches!(err.downcast_ref::<DbError>(), Some(DbError::Fatal(4))),
        "{err:#}"
    );
}

/// A suffix lost behind the writer's back (a restore, or a crash with
/// `synchronous_commit=off`) shows as a `writer_seq` behind this process's.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn a_lost_suffix_exits_4() {
    let (_scratch, url) = backend::scratch_schema().await;
    let (a, _) = open(&fast(&url, Role::Indexer)).await;
    for n in 1..=3 {
        db::save_block_bundle(&a, &bundle(n)).await.unwrap();
    }
    let pid = db::testing::writer_pid(&a).unwrap();
    terminate(&url, pid).await;
    exec(
        &url,
        "DELETE FROM blocks WHERE number = 3;
         UPDATE kv SET value = split_part(value, ':', 1) || ':' || (split_part(value, ':', 2)::int8 - 1)
         WHERE key = 'writer_seq'",
    )
    .await;

    let err = db::save_block_bundle(&a, &bundle(4)).await.unwrap_err();
    assert!(
        matches!(err.downcast_ref::<DbError>(), Some(DbError::Fatal(4))),
        "{err:#}"
    );
}

/// A database problem is retried, never returned: blocks written while the
/// server refuses (a full disk, a read-only server) all land once it stops.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn database_errors_never_drop_blocks() {
    for sqlstate in ["53100", "25006"] {
        let (_scratch, url) = backend::scratch_schema().await;
        let (db, _) = open(&fast(&url, Role::Indexer)).await;
        exec(
            &url,
            &format!(
                "CREATE TABLE refuse (on_ boolean);
                 INSERT INTO refuse VALUES (true);
                 CREATE FUNCTION refuse() RETURNS trigger LANGUAGE plpgsql AS $$
                 BEGIN
                     IF EXISTS (SELECT 1 FROM refuse) THEN
                         RAISE EXCEPTION 'injected' USING ERRCODE = '{sqlstate}';
                     END IF;
                     RETURN NEW;
                 END $$;
                 CREATE TRIGGER refuse BEFORE INSERT ON blocks FOR EACH ROW EXECUTE FUNCTION refuse();"
            ),
        )
        .await;
        let writer = {
            let db = db.clone();
            tokio::spawn(async move {
                for n in 1..=5 {
                    db::save_block_bundle(&db, &bundle(n)).await.unwrap();
                }
            })
        };
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(
            count(&url, "SELECT COUNT(*) FROM blocks").await,
            0,
            "{sqlstate}: refused"
        );
        exec(&url, "DELETE FROM refuse").await;
        tokio::time::timeout(Duration::from_secs(60), writer)
            .await
            .expect("the writer catches up")
            .unwrap();
        assert_eq!(
            count(&url, "SELECT COUNT(*) FROM blocks").await,
            5,
            "{sqlstate}"
        );
    }
}

/// A content error is the caller's: the indexer's per-bundle fallback isolates
/// the bundle that caused it.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn a_content_error_is_returned() {
    let (_scratch, url) = backend::scratch_schema().await;
    let (db, _) = open(&fast(&url, Role::Indexer)).await;
    exec(
        &url,
        "ALTER TABLE blocks ADD CONSTRAINT small CHECK (number < 100)",
    )
    .await;
    let err = db::save_block_bundle(&db, &bundle(500)).await.unwrap_err();
    assert!(
        matches!(err.downcast_ref::<DbError>(), Some(DbError::Data(_))),
        "{err:#}"
    );
    db::save_block_bundle(&db, &bundle(5)).await.unwrap();
}

/// Role defaults apply to every connection a role opens, so the pools set
/// their own: a role default `idle_session_timeout` must not reach them.
#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn role_defaults_do_not_leak_into_the_pools() {
    let (_scratch, url) = backend::scratch_schema().await;
    // Migrate as the owner first.
    drop(open(&fast(&url, Role::Indexer)).await);
    let role = format!("t_role_{}", std::process::id());
    let schema = url.rsplit("search_path]=").next().unwrap().to_string();
    exec(
        &url,
        &format!(
            "DROP ROLE IF EXISTS {role}; CREATE ROLE {role} LOGIN PASSWORD 'pw';
             GRANT USAGE ON SCHEMA {schema} TO {role}; GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {role};
             ALTER ROLE {role} SET idle_session_timeout = '2s';"
        ),
    )
    .await;
    let parsed = url::Url::parse(&url).unwrap();
    let mut as_role = parsed.clone();
    as_role.set_username(&role).unwrap();
    as_role.set_password(Some("pw")).unwrap();
    let (web, _) = open(&fast(as_role.as_str(), Role::Web)).await;
    assert!(db::get_latest_block(&web).await.is_none());
    tokio::time::sleep(Duration::from_secs(3)).await;
    let (_, failed) = db::track_failures(db::get_latest_block(&web)).await;
    assert!(
        !failed,
        "the read pool's connection outlived the role default"
    );
    drop(web);
    exec(&url, &format!("DROP OWNED BY {role}; DROP ROLE {role}")).await;
}
