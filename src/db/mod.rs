//! The explorer's database: one async API in front of SQLite and Postgres.
//!
//! Every function here dispatches on the backend. The SQLite arm calls the
//! rusqlite code in `sqlite.rs` inline, as before the API was async, so SQLite
//! behaves exactly as it did. The Postgres arm calls a hand-written twin in
//! `pg/`, which must exist or this does not compile. A forgotten `.await`
//! fails CI: `let _ = db::x()` trips `clippy::let_underscore_future`, and a
//! bare `db::x();` trips `unused_must_use`.

use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, Weak};
use std::time::Duration;

use anyhow::{bail, Result};
use rusqlite::Connection;
use serde_json::Value;
use tokio::sync::watch;

use crate::decoder::checksum_address;
use crate::models::{AnchoringEvent, Block, BlockBundle, TokenMetadata, Transaction};
use crate::tokens::TokenMeta;

mod config;
pub mod migrations;
pub(crate) mod pg;
mod sqlite;
mod status;

pub use config::{DbConfig, DbTarget, DbUrl, Role, Tuning};
pub use pg::DbError;
pub use status::{Preflight, SchemaVersions, Status, Sync, WriterState};

#[doc(hidden)]
pub use sqlite::{counter, get_block_timestamp, init_db, sync_holder_counts};
pub use sqlite::{now_ts, page_offset, Holder, TxColumns};

/// The explorer's database. Cheap to clone; every clone shares one backend.
#[derive(Clone)]
pub struct Db(Arc<Inner>);

struct Inner {
    backend: Backend,
    labels: LabelCache,
    status: watch::Sender<Status>,
    role: Role,
}

enum Backend {
    Sqlite(sqlite::Db),
    // Boxed: the pools and the writer dwarf the SQLite handle.
    Postgres(Box<pg::PgDb>),
}

/// Open a SQLite file as `ROLE=all`, created if needed, and bring its schema
/// up to date.
pub async fn open(path: &str) -> Result<Db> {
    let cfg = DbConfig::sqlite(path);
    open_with(&cfg, watch::channel(Status::starting(Role::All)).0).await
}

/// Open the configured database for `cfg.role`, publishing progress on
/// `status`, which `/readyz` reads.
///
/// Under `ROLE=indexer` or `all` on Postgres this returns once the process is
/// the writer: it connects (retrying while the database is unreachable), passes
/// the preflight, waits as a candidate for the lock, and migrates. A preflight
/// refusal is an error. Under `ROLE=web` the pools are lazy and this never
/// waits on the database.
pub async fn open_with(cfg: &DbConfig, status: watch::Sender<Status>) -> Result<Db> {
    let backend = match &cfg.target {
        DbTarget::Sqlite(path) => {
            let s = sqlite::open(path)?;
            // A file opens only once it is at this binary's version.
            status.send_modify(|s| s.schema.db = Some(migrations::binary_version()));
            Backend::Sqlite(s)
        }
        DbTarget::Postgres(_) => Backend::Postgres(Box::new(pg::open(cfg, &status).await?)),
    };
    let db = Db(Arc::new(Inner {
        backend,
        labels: LabelCache::new(cfg.role == Role::Indexer),
        status,
        role: cfg.role,
    }));
    if !reload_labels(&db).await {
        // Every later label change goes through this process's own writes, so
        // the seed is retried only until it lands.
        tokio::spawn(reseed_until_ok(Arc::downgrade(&db.0), true));
    }
    if let Backend::Postgres(p) = &db.0.backend {
        if let Some(writer) = &p.writer {
            // The previous leader kept writing while this process waited, so
            // every new leadership re-seeds.
            let weak = Arc::downgrade(&db.0);
            writer.set_on_leader(Box::new(move || {
                tokio::spawn(reseed_until_ok(weak.clone(), false));
            }));
        }
    }
    Ok(db)
}

/// Merge every stored token's label into the cache: the seed at open, and
/// what a web replica's follower calls to see the indexer's inserts and
/// repairs. On an error the cache is left as it was, and this says so.
pub async fn reload_labels(db: &Db) -> bool {
    match try_all_token_metas(db).await {
        Ok(rows) => {
            db.0.labels.merge(&rows);
            true
        }
        Err(e) => {
            tracing::warn!("token labels not loaded: {e:#}");
            false
        }
    }
}

const LABEL_RETRY: Duration = Duration::from_secs(30);

async fn reseed_until_ok(weak: Weak<Inner>, wait_first: bool) {
    let mut wait = wait_first;
    loop {
        if wait {
            tokio::time::sleep(LABEL_RETRY).await;
        }
        wait = true;
        let Some(inner) = weak.upgrade() else { return };
        if reload_labels(&Db(inner)).await {
            return;
        }
    }
}

/// Token labels for Tera's `address_label`, which is sync and cannot await a
/// query: one entry per `token_metadata` row, keyed by checksummed address.
/// Entries are only ever added or replaced, which is exact because nothing
/// deletes `token_metadata` rows.
///
/// Under `ROLE=indexer` it also notes the token addresses committed bundles
/// name with no entry, for the missing-metadata job. Whatever takes both locks
/// takes `labels` first.
struct LabelCache {
    labels: RwLock<HashMap<String, String>>,
    noted: Option<Mutex<HashSet<String>>>,
}

impl LabelCache {
    fn new(note: bool) -> Self {
        LabelCache {
            labels: RwLock::default(),
            noted: note.then(Mutex::default),
        }
    }

    /// The symbol, else the name, else empty.
    fn label(symbol: &str, name: &str) -> String {
        if symbol.is_empty() { name } else { symbol }.to_string()
    }

    fn put(&self, meta: &TokenMeta) {
        self.write().insert(
            checksum_address(&meta.address),
            Self::label(&meta.symbol, &meta.name),
        );
    }

    /// After a committed batch: each bundle's tokens in order, so the last
    /// metadata for an address wins, as it does in the write. Then, under
    /// `ROLE=indexer`, note what the bundles name and the cache lacks.
    fn committed(&self, bundles: &[BlockBundle]) {
        let mut map = self.write();
        for meta in bundles.iter().flat_map(|b| &b.tokens) {
            map.insert(
                checksum_address(&meta.address),
                Self::label(&meta.symbol, &meta.name),
            );
        }
        if let Some(noted) = &self.noted {
            let named = bundles.iter().flat_map(|b| {
                b.transfers
                    .iter()
                    .map(|t| t.token_addr.as_str())
                    .chain(b.txs.iter().filter_map(|t| t.fee_token.as_deref()))
            });
            let mut noted = noted.lock().unwrap_or_else(|e| e.into_inner());
            for address in named {
                let address = checksum_address(address);
                if !map.contains_key(&address) {
                    noted.insert(address);
                }
            }
        }
    }

    fn merge(&self, rows: &[TokenMetadata]) {
        let mut map = self.write();
        for row in rows {
            map.insert(
                checksum_address(&row.address),
                Self::label(&row.symbol, &row.name),
            );
        }
    }

    fn get(&self, address: &str) -> Option<String> {
        let map = self.labels.read().unwrap_or_else(|e| e.into_inner());
        map.get(address).filter(|l| !l.is_empty()).cloned()
    }

    fn take_noted(&self) -> Vec<String> {
        let Some(noted) = &self.noted else {
            return Vec::new();
        };
        // The labels before `noted`, as `committed` takes them: the other way
        // round deadlocks against a commit.
        let map = self.labels.read().unwrap_or_else(|e| e.into_inner());
        let mut noted = noted.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<String> = noted.drain().filter(|a| !map.contains_key(a)).collect();
        out.sort();
        out
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<String, String>> {
        self.labels.write().unwrap_or_else(|e| e.into_inner())
    }
}

/// What `/readyz` reports, kept current by the database layer.
pub fn status(db: &Db) -> watch::Receiver<Status> {
    db.0.status.subscribe()
}

/// Update the published status, for the follower and the indexer's sync report.
pub fn update_status(db: &Db, f: impl FnOnce(&mut Status)) {
    db.0.status.send_modify(f);
}

/// The role this database was opened for.
pub fn role(db: &Db) -> Role {
    db.0.role
}

/// Unix time of the writer's last commit or keepalive, where there is one.
pub fn writer_last_ok(db: &Db) -> Option<i64> {
    match &db.0.backend {
        Backend::Postgres(p) => p.writer.as_ref().map(|w| w.last_ok()),
        Backend::Sqlite(_) => None,
    }
}

/// Run `f`, and say whether any database read in it failed. The 503
/// middleware wraps each request in this.
pub async fn track_failures<F: Future>(f: F) -> (F::Output, bool) {
    pg::DB_FAILED
        .scope(Cell::new(false), async {
            let out = f.await;
            let failed = pg::DB_FAILED.with(Cell::get);
            (out, failed)
        })
        .await
}

/// Statements this process has sent to Postgres, for round-trip budgets.
pub fn statements() -> u64 {
    pg::q::statements()
}

/// The SQLite connection, for tests that inspect or tamper with rows directly.
/// Never hold the guard across an `.await`.
pub fn lock(db: &Db) -> MutexGuard<'_, Connection> {
    match &db.0.backend {
        Backend::Sqlite(s) => sqlite::lock(s),
        Backend::Postgres(_) => panic!("db::lock is a SQLite-only test hook"),
    }
}

/// The label a token's address goes by on other pages: its symbol, else its
/// name. Sync, for Tera's `address_label`, which cannot await a query.
/// `None` when the token has neither, so the caller's later rules apply.
pub fn token_label(db: &Db, address: &str) -> Option<String> {
    db.0.labels.get(address)
}

/// The token addresses committed bundles named that have no label, since the
/// last call: the missing-metadata job's work. Always empty outside
/// `ROLE=indexer`. An address with an empty label counts as known.
pub fn take_tokens_without_metadata(db: &Db) -> Vec<String> {
    db.0.labels.take_noted()
}

/// One `pub async fn` per line. `inline` calls the SQLite function of the
/// same name on the caller's task; `extra` calls one in `sqlite/extra.rs`;
/// `blocking` runs it on tokio's blocking pool. On Postgres each calls
/// `pg::` of the same name.
macro_rules! db_fn {
    (@call inline $s:ident $n:ident ($($a:ident),*)) => { sqlite::$n($s, $($a),*) };
    (@call extra $s:ident $n:ident ($($a:ident),*)) => { sqlite::extra::$n($s, $($a),*) };
    (@call blocking $s:ident $n:ident ()) => {{
        let h = $s.clone();
        match tokio::task::spawn_blocking(move || sqlite::$n(&h)).await {
            Ok(r) => r,
            Err(e) => std::panic::resume_unwind(e.into_panic()),
        }
    }};
    ($( $side:ident fn $n:ident($($a:ident: $t:ty),*) $(-> $r:ty)?; )*) => {
        $(
            pub async fn $n(db: &Db, $($a: $t),*) $(-> $r)? {
                #[cfg(feature = "db-coverage")]
                coverage::hit(stringify!($n));
                match &db.0.backend {
                    Backend::Sqlite(s) => db_fn!(@call $side s $n ($($a),*)),
                    Backend::Postgres(p) => pg::$n(p, $($a),*).await,
                }
            }
        )*

        /// Every `db_fn!` entry and hand-written wrapper, for the coverage gate.
        #[cfg(feature = "db-coverage")]
        pub const ALL_FNS: &[&str] = &[
            $(stringify!($n),)*
            "save_block_bundles",
            "save_block_bundle",
            "save_token_metadata",
            "save_anchoring_window",
        ];
    };
}

db_fn! {
    inline   fn get_kv(key: &str) -> Option<String>;
    inline   fn save_block(block: &Block) -> Result<()>;
    inline   fn get_block_by_number(number: i64) -> Option<Block>;
    inline   fn get_block_by_hash(hash: &str) -> Option<Block>;
    inline   fn get_latest_block() -> Option<Block>;
    inline   fn set_chain_head(head: i64);
    inline   fn get_min_block_number() -> Option<i64>;
    inline   fn get_blocks_in_range(from: i64, to: i64) -> Vec<Block>;
    inline   fn get_recent_blocks(limit: usize) -> Vec<Block>;
    inline   fn set_trace(hash: &str, trace: &str) -> Result<()>;
    inline   fn save_transaction(tx: &Transaction) -> Result<()>;
    inline   fn get_transaction(hash: &str) -> Option<Transaction>;
    inline   fn get_transactions_in_range(from: i64, to: i64, columns: TxColumns) -> Vec<Transaction>;
    inline   fn get_block_transactions(block_number: i64, columns: TxColumns) -> Vec<Transaction>;
    inline   fn get_address_transactions(address: &str, page: u32, per_page: u32, columns: TxColumns) -> Vec<Transaction>;
    inline   fn get_transactions(page: u32, per_page: u32) -> Vec<Transaction>;
    inline   fn get_transaction_count() -> i64;
    inline   fn get_address_transaction_count(address: &str) -> i64;
    inline   fn get_token_metadata(address: &str) -> Option<TokenMetadata>;
    inline   fn get_all_tokens(page: u32, per_page: u32) -> Vec<TokenMetadata>;
    inline   fn get_all_token_metas() -> Vec<TokenMetadata>;
    inline   fn get_token_count() -> i64;
    inline   fn get_token_transfer_count(token_addr: &str) -> i64;
    inline   fn get_token_by_symbol_or_name(q: &str) -> Option<TokenMetadata>;
    inline   fn search_tokens(q: &str, limit: u32) -> Vec<TokenMetadata>;
    inline   fn get_anchoring_events(registry_id: i64, limit: u32) -> Vec<AnchoringEvent>;
    inline   fn holders_without_genesis_balance(limit: i64) -> Result<Option<(i64, Vec<Holder>)>>;
    inline   fn save_genesis_balances(balances: &[(Holder, String)], cursor: i64) -> Result<()>;
    inline   fn get_token_transfers(token_addr: &str, page: u32, per_page: u32) -> Vec<Value>;
    inline   fn get_address_transfers(address: &str, page: u32, per_page: u32) -> Vec<Value>;
    inline   fn get_address_transfer_count(address: &str) -> i64;
    inline   fn get_token_holders(token_addr: &str, page: u32, per_page: u32) -> Vec<(String, String)>;
    inline   fn get_selector_names(selectors: &[String], fresh_after: i64) -> HashMap<String, String>;
    inline   fn save_selector_names(answers: &[(String, String)]) -> Result<()>;
    inline   fn get_tokens_metadata(addresses: &[String]) -> HashMap<String, TokenMetadata>;
    inline   fn get_all_token_addresses() -> Vec<String>;
    inline   fn get_token_holder_count(token_addr: &str) -> i64;
    inline   fn get_address_holdings(address: &str) -> Vec<Value>;
    inline   fn compute_and_store_stats() -> Result<Value>;
    blocking fn repair_derived_tables();
    extra    fn try_min_block_number() -> Result<Option<i64>>;
    extra    fn tokens_missing_metadata() -> Result<Vec<String>>;
    extra    fn try_all_token_metas() -> Result<Vec<TokenMetadata>>;
    extra    fn follow_point() -> Result<(Option<i64>, Option<i64>, Option<i64>)>;
}

// Hand-written, like save_anchoring_window: these three update the label
// cache after a successful commit, and db_fn! has no after-call step.

/// Persist several blocks in one transaction, in the order given.
pub async fn save_block_bundles(db: &Db, bundles: &[BlockBundle]) -> Result<()> {
    #[cfg(feature = "db-coverage")]
    coverage::hit("save_block_bundles");
    match &db.0.backend {
        Backend::Sqlite(s) => {
            sqlite::save_block_bundles(s, bundles)?;
            db.0.labels.committed(bundles);
            Ok(())
        }
        // A batch commits on in the writer's spawned task when its caller is
        // dropped, so the label update runs in a task of its own that waits
        // for the commit, as `save_token_metadata`'s does.
        Backend::Postgres(_) => {
            let (db, bundles) = (db.clone(), bundles.to_vec());
            let task = tokio::spawn(async move {
                let Backend::Postgres(p) = &db.0.backend else {
                    unreachable!()
                };
                pg::save_block_bundles(p, &bundles).await?;
                db.0.labels.committed(&bundles);
                anyhow::Ok(())
            });
            match task.await {
                Ok(r) => r,
                Err(e) => std::panic::resume_unwind(e.into_panic()),
            }
        }
    }
}

/// Persist one indexed block atomically.
pub async fn save_block_bundle(db: &Db, bundle: &BlockBundle) -> Result<()> {
    #[cfg(feature = "db-coverage")]
    coverage::hit("save_block_bundle");
    let one = std::slice::from_ref(bundle);
    match &db.0.backend {
        Backend::Sqlite(s) => {
            sqlite::save_block_bundle(s, bundle)?;
            db.0.labels.committed(one);
            Ok(())
        }
        // No singular twin: one bundle is a batch of one.
        Backend::Postgres(_) => save_block_bundles(db, one).await,
    }
}

pub async fn save_token_metadata(db: &Db, meta: &TokenMeta) -> Result<()> {
    #[cfg(feature = "db-coverage")]
    coverage::hit("save_token_metadata");
    match &db.0.backend {
        Backend::Sqlite(s) => {
            sqlite::save_token_metadata(s, meta)?;
            db.0.labels.put(meta);
            Ok(())
        }
        // A page view can be dropped mid-save (ROLE=all). The write runs on in
        // its spawned task, so the put must run in the same task.
        Backend::Postgres(_) => {
            let (db, meta) = (db.clone(), meta.clone());
            let task = tokio::spawn(async move {
                let Backend::Postgres(p) = &db.0.backend else {
                    unreachable!()
                };
                pg::save_token_metadata(p, &meta).await?;
                db.0.labels.put(&meta);
                anyhow::Ok(())
            });
            match task.await {
                Ok(r) => r,
                Err(e) => std::panic::resume_unwind(e.into_panic()),
            }
        }
    }
}

/// Store a window of anchoring events and advance its watermark, atomically.
/// `events` gets a block-timestamp lookup and must be deterministic: Postgres
/// calls it twice, once to learn which blocks it stamps.
pub async fn save_anchoring_window(
    db: &Db,
    key: &str,
    value: &str,
    events: impl Fn(&dyn Fn(i64) -> Option<i64>) -> Vec<AnchoringEvent> + Send + 'static,
) -> Result<usize> {
    #[cfg(feature = "db-coverage")]
    coverage::hit("save_anchoring_window");
    match &db.0.backend {
        Backend::Sqlite(s) => sqlite::save_anchoring_window(s, key, value, events),
        Backend::Postgres(p) => pg::save_anchoring_window(p, key, value, events).await,
    }
}

/// The writer's heartbeat: on Postgres, `SELECT 1` on the lock-holding
/// session within 5 s, which keeps its lease (`idle_session_timeout`) from
/// expiring. Nothing on SQLite.
pub async fn keepalive(db: &Db) {
    if let Backend::Postgres(p) = &db.0.backend {
        pg::keepalive(p).await;
    }
}

/// D: the database's schema version, read from the database. On SQLite, an
/// open file is always at this binary's version.
pub async fn schema_version(db: &Db) -> Result<i64> {
    match &db.0.backend {
        Backend::Sqlite(_) => Ok(migrations::binary_version()),
        Backend::Postgres(p) => pg::schema_version(&p.read).await,
    }
}

/// Hooks for tests that need a backend's internals, on either backend.
#[doc(hidden)]
pub mod testing {
    use super::*;

    /// Run raw SQL on a SQLite database, to tamper with it in a test.
    pub fn execute(db: &Db, sql: &str) -> Result<()> {
        Ok(lock(db).execute_batch(sql)?)
    }

    /// Run `sql` on the writer on the `Long` budget, as migrations and repairs
    /// run: no statement timeout, under the watchdog.
    pub async fn long_statement(db: &Db, sql: &'static str) -> Result<()> {
        let Backend::Postgres(p) = &db.0.backend else {
            bail!("Postgres only");
        };
        p.writer()?
            .write(
                pg::writer::Budget::Long,
                move |c| -> pg::writer::TxFuture<'_, ()> {
                    Box::pin(async move { pg::q::raw(c, "long", sql).await })
                },
            )
            .await
            .map_err(anyhow::Error::new)
    }

    /// The backend pid of the writer session, on Postgres.
    pub fn writer_pid(db: &Db) -> Option<i32> {
        match &db.0.backend {
            Backend::Postgres(p) => p.writer.as_ref().map(|w| w.pid()),
            Backend::Sqlite(_) => None,
        }
    }

    /// Rebuild `token_balances` and the holder counts from the transfer
    /// history and the genesis balances, as a repair on an older database does.
    pub async fn rebuild_token_balances(db: &Db) -> Result<()> {
        match &db.0.backend {
            Backend::Sqlite(_) => sqlite::rebuild_token_balances(&lock(db)),
            Backend::Postgres(p) => p
                .writer()?
                .write(
                    pg::writer::Budget::Long,
                    |c| -> pg::writer::TxFuture<'_, ()> { Box::pin(pg::rebuild_token_balances(c)) },
                )
                .await
                .map_err(anyhow::Error::new),
        }
    }
}

/// Which `db` functions ran, for the gate that every one runs on both backends.
#[cfg(feature = "db-coverage")]
pub mod coverage {
    use std::collections::BTreeSet;
    use std::sync::Mutex;

    static HIT: Mutex<BTreeSet<&'static str>> = Mutex::new(BTreeSet::new());

    pub fn hit(name: &'static str) {
        HIT.lock().unwrap_or_else(|e| e.into_inner()).insert(name);
    }

    pub fn hits() -> BTreeSet<&'static str> {
        HIT.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoder::checksum_address;

    /// `committed` holds the labels' write lock while it takes `noted`. Were
    /// `take_noted` to hold `noted` while it waits on the labels, a commit and
    /// the missing-metadata job would deadlock.
    #[test]
    fn take_noted_never_holds_noted_while_a_commit_holds_the_labels() {
        let cache = LabelCache::new(true);
        std::thread::scope(|s| {
            // Where `committed` stands before it takes `noted`.
            let labels = cache.write();
            let job = s.spawn(|| cache.take_noted());
            let noted = cache.noted.as_ref().unwrap();
            let until = std::time::Instant::now() + Duration::from_millis(200);
            while std::time::Instant::now() < until {
                assert!(
                    !matches!(noted.try_lock(), Err(std::sync::TryLockError::WouldBlock)),
                    "take_noted holds `noted` while it waits on the labels"
                );
                std::thread::yield_now();
            }
            drop(labels);
            assert!(job.join().unwrap().is_empty());
        });
    }

    const A: &str = "0x5555555555555555555555555555555555555501";
    const B: &str = "0x5555555555555555555555555555555555555502";

    fn meta(address: &str, symbol: &str) -> TokenMeta {
        TokenMeta {
            address: checksum_address(address),
            name: String::new(),
            symbol: symbol.into(),
            decimals: 6,
            currency: "USD".into(),
            total_supply: "1".into(),
        }
    }

    fn bundle(number: i64, tokens: Vec<TokenMeta>) -> BlockBundle {
        BlockBundle {
            block: Block {
                number,
                hash: format!("0x{number:064x}"),
                parent_hash: format!("0x{:064x}", number - 1),
                timestamp: 1_700_000_000,
                timestamp_ms: 1_700_000_000_000,
                gas_used: 0,
                gas_limit: 30_000_000,
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
            tokens,
        }
    }

    async fn temp_db() -> (tempfile::TempDir, Db) {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path().join("labels.db").to_str().unwrap())
            .await
            .unwrap();
        (dir, db)
    }

    #[tokio::test]
    async fn a_reopened_database_starts_with_its_labels() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("labels.db");
        let db = open(path.to_str().unwrap()).await.unwrap();
        save_token_metadata(&db, &meta(A, "ONE")).await.unwrap();
        drop(db);

        let db = open(path.to_str().unwrap()).await.unwrap();
        assert_eq!(
            token_label(&db, &checksum_address(A)).as_deref(),
            Some("ONE")
        );
    }

    #[tokio::test]
    async fn a_failed_write_labels_nothing() {
        let (_dir, db) = temp_db().await;
        lock(&db)
            .execute_batch("DROP TABLE token_metadata")
            .unwrap();

        assert!(save_token_metadata(&db, &meta(A, "ONE")).await.is_err());
        assert!(save_block_bundle(&db, &bundle(1, vec![meta(B, "TWO")]))
            .await
            .is_err());
        assert_eq!(token_label(&db, &checksum_address(A)), None);
        assert_eq!(token_label(&db, &checksum_address(B)), None);
    }

    /// The API is async all the way down. A sync bridge into a runtime panics
    /// on the current-thread runtime most tests use, and deadlocks under load,
    /// so neither form may come back.
    #[test]
    fn nothing_bridges_sync_code_into_the_runtime() {
        let banned = [
            concat!("block_", "in_place"),
            concat!("Handle::", "block_on"),
        ];
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut found = Vec::new();
        let mut dirs = vec![src];
        while let Some(dir) = dirs.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    dirs.push(path);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    let text = std::fs::read_to_string(&path).unwrap();
                    for (i, line) in text.lines().enumerate() {
                        if banned.iter().any(|b| line.contains(b)) {
                            found.push(format!("{}:{}: {}", path.display(), i + 1, line.trim()));
                        }
                    }
                }
            }
        }
        assert!(found.is_empty(), "await instead:\n{}", found.join("\n"));
    }

    /// As in the write itself, the last metadata for an address wins.
    #[tokio::test]
    async fn the_last_metadata_in_a_batch_wins() {
        let (_dir, db) = temp_db().await;
        let batch = [
            bundle(1, vec![meta(A, "OLD")]),
            bundle(2, vec![meta(A, "NEW")]),
        ];
        save_block_bundles(&db, &batch).await.unwrap();

        assert_eq!(
            token_label(&db, &checksum_address(A)).as_deref(),
            Some("NEW")
        );
        assert_eq!(
            get_token_metadata(&db, &checksum_address(A))
                .await
                .unwrap()
                .symbol,
            "NEW"
        );
    }
}
