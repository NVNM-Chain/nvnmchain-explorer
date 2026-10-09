# Phase 3: Postgres as a second permanent backend

Status: implemented
Date: 2026-10-03
Follows: `2026-10-01-db-boundary-design.md` (phase 1) and
`2026-10-02-schema-two-dialects-design.md` (phase 2)

## Why

The explorer runs on SQLite today. Production moves to Postgres: either
Cloud SQL for PostgreSQL in the same GCP region as the explorer, or a
Postgres running in Kubernetes. SQLite stays permanently, for development
and single-binary self-hosting.

Phase 1 sealed the database boundary. Phase 2 added a Postgres schema and
the tests that hold it to `init_db`. This phase adds:

- the Postgres backend;
- versioned migrations on both backends;
- a split web/indexer deployment;
- tests that compare the baseline fixtures on Postgres.

### What this solves

Today the explorer is one process with one SQLite file. The indexer and
every page share one connection, only one instance can serve, and every
release or restart takes the site down. This phase is for:

- **Availability and scale-out.** Several web replicas, releases with no
  downtime, and pages that keep serving while the indexer restarts. The
  database itself runs without HA (decision 2). While it is down for a
  restart or maintenance, pages return 503 with `Retry-After`, no block is
  lost, and no explorer process crashes or restarts (section 6).
- **Data growth.** The data can outgrow what one machine's disk and memory
  hold. Postgres on Cloud SQL or in Kubernetes grows with it.

It costs about 6,690–7,380 lines of code (section 11), a second backend
kept in step permanently, and the running cost of the Postgres target and
the GKE pods. Section 15 ties acceptance to both goals.

### Decisions this spec is built on

Made in brainstorming on 2026-10-03, and extended in review on
2026-10-05 and 2026-10-06:

1. **Where it runs.** On GCP, in the same region as the database. The
   deployment examples are for Kubernetes only. Production runs on
   Kubernetes today.
2. **Postgres targets.** Cloud SQL for PostgreSQL, or Postgres in
   Kubernetes (an operator such as CloudNativePG). The design must work on
   both. Either runs as a single primary, without HA, so a database restart
   or maintenance is downtime that the explorer waits out (section 6).
   Devops picks which one production uses, and section 12's measurements
   run on that one.
3. **Topology.** Split deployments built from one image:
   - one indexer process, the only writer, which holds a session-level
     advisory lock and runs migrations;
   - N web replicas that serve pages.

   `ROLE=all|web|indexer` picks the role at runtime. `all` stays the
   default, and is supported for development, CI and small self-hosting,
   on either backend. Production runs the split roles.
4. **One team, one main repo.** `NVNM-Chain/nvnmchain-explorer` is the
   main repo. This work merges there, as phases 1 and 2 did (#3 and #4),
   and migration numbers are assigned there only (section 5). Its parent,
   `yihuang/nvnmchain-explorer`, where teammates commit daily, is the same
   team, and PR #44 carries phases 1 and 2 on to it. A coordinated one-time
   change to `web.rs`, `indexer.rs` and the tests is acceptable, so the
   phase 1 and 2 rules that kept those files untouched are relaxed.
5. **The public `db::*` API becomes native async** (section 1).
6. **Both backends are permanent.** Every query and every schema change
   exists for both, and tests hold them together. Teammates have agreed to
   this recurring cost, not only to decision 4's one-time change: a
   Postgres twin and a parity-grid entry for every new `db` function, twin
   migration files under the commute rule, and a frozen `init_db`.
7. **Least change to existing SQLite code.** The rusqlite query bodies,
   `init_db`, `seed_counters`, `schema_check` and the per-row writer stay
   as they are: moved, not rewritten.
8. **Versioned migrations on both backends**, run by the indexer under the
   lock. Existing SQLite files are adopted.
9. **The first production Postgres is re-indexed from the chain, side by
   side.** New `explorer-indexer` and `explorer-web` deployments sync
   against Postgres while today's deployment keeps serving from SQLite.
   Devops switches traffic once the indexer reports synced and the
   go/no-go in section 10 passes. Each process
   connects to exactly one database, and there is no SQLite importer.
10. **Integration tests compare the baseline fixtures on Postgres too.**
11. **No TLS to the database** (owner, 2026-10-06). sqlx is built without
    a TLS feature, so every connection is plaintext, and the database is
    reachable only over a private network (section 7).

### What this phase delivers

- **One async API, two backends.** A native async `db::*` API in front of
  SQLite and Postgres. The SQLite arm calls today's code inline, so SQLite
  behaviour does not change.
- **A hand-written Postgres backend** on sqlx 0.9. Its writer is set-based:
  at most 14 round trips per 64-block batch, against about 642 statements
  per batch today for the `write_scale` block shape. It passed the stage
  4b gate, equal tables on `canary-rich`, so the per-row fallback did not
  ship (section 10).
- **One writer per database.** The writer is fenced by the session that
  holds a session-level advisory lock, and it survives a database restart
  without losing a block.
- **Versioned migrations on both backends.** On SQLite, migration 1 is the
  frozen `init_db`.
- **Split roles.** `ROLE=web` and `ROLE=indexer`, with a polling live feed
  for web replicas, lock-free readiness, and a 503 when the database is
  down.
- **Tests on both backends.** The existing suites run on both, the baseline
  fixtures are replayed and re-indexed into Postgres, and the two backends
  are compared with each other.
- **Operations.** Kubernetes manifests, a runbook, and a side-by-side
  cutover gated on the indexer's own "synced" report and a per-page p95
  bound.

### Known limitations

**A token with no transfers yet is not listed or searchable in the split
deployment.** Status: accepted for this phase (owner, 2026-10-05). The fix
is the next task after phase 3, in its own PR.

- **Today** a page view of a token saves its metadata (`web.rs:1567`). From
  then on the token is in `/tokens`, in search, and labels its address on
  other pages.
- **Under `ROLE=web`** the web replicas write no chain data (section 6).
  The indexer learns of a token only from a `Transfer` in a block or from a
  fee paid in it (`indexer.rs:270-295`). A mint counts, because it is a
  `Transfer` from the zero address.
- **So between a token's creation and its first mint, transfer or fee
  use,** its page still renders, from one RPC fetch per view, but it is
  missing from `/tokens`, from search and from labels on other pages.
  Opening its page does not change that.
- **`ROLE=all` keeps today's behaviour** on either backend, because its
  page views still save through the writer.
- **The fix** is to discover tokens from the TIP-20 factory's
  `TokenCreated` logs. It is the next task after this phase (section 14).
- **Users are told** in `README.md`, in stage 8 (section 10).

## 1. Sync vs async

**Decision: a native async `db::*` API. The SQLite arm calls the moved
rusqlite functions inline.**

Three options were compared against the tokio and sqlx sources and a
microbenchmark:

- **A.** Async API; every SQLite call goes through `spawn_blocking`.
- **B.** Sync API plus a `block_in_place` bridge into a private runtime.
  This was the phase 2 plan.
- **C1.** Async API; SQLite is called inline. This is the decision.

|                                               | A                                                               | B                                                                                                                                           | C1                                   |
|-----------------------------------------------|-----------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------|
| Tests vs production                           | Same path                                                       | `block_in_place` panics on a current-thread runtime. 45 of the 47 async DB tests run on one, so tests take a different path than production | Same path                            |
| Cancellation (timeouts, disconnects, SIGTERM) | A Postgres read cancels cleanly                                 | A call cannot be cancelled, and shutdown waits for it                                                                                       | As A                                 |
| How mistakes surface                          | Compile errors                                                  | Runtime panics or hangs (tokio#7892 deadlock; a runtime dropped inside async code)                                                          | Compile errors                       |
| SQLite cost per call                          | About 7 µs, `'static` argument copies, and one signature change | 0                                                                                                                                           | 0, with behaviour identical to today |
| Runtimes, for as long as both backends exist  | 1                                                               | 2, permanently                                                                                                                              | 1                                    |
| Caller churn                                  | About 360–400 lines, once                                       | 0                                                                                                                                           | About 360–400 lines, once            |

Why C1:

- **B's bridge would be permanent, because both backends are.** Its one
  advantage, no caller churn, mattered while upstream was treated as an
  outside party. An upstream-merge probe measured that tax: 59% of
  upstream commits conflicted, at about 30–45 minutes per sync. Under
  decision 4 the cost is paid once.
- **A taxes every SQLite call and buys nothing C1 lacks.** Any single
  function can still move to `spawn_blocking` later by changing one marker
  in `db/mod.rs`, without touching callers.
- **Speed is not the reason.** At about 1 ms to a same-region database, the
  number of round trips sets page time under any option. The token-label
  cache is needed under all three. The set-based writer only buys re-index
  headroom: real blocks are nearly empty, so a per-row writer needs about
  4 round trips per block and matches today's backfill rate at about 1 ms,
  and at the chain head the two cost nearly the same (7 round trips per
  block vs 6).
- **A forgotten `.await` fails CI.** `let _ = db::x()` trips
  `clippy::let_underscore_future`, and a bare `db::x();` trips
  `unused_must_use`. Both lints are on by default, and CI runs
  `-D warnings`.

**Once started, a write completes on both backends.**

- An inline SQLite call runs to completion, as it does today.
- A Postgres write runs its transaction on a spawned task (section 6), so a
  dropped caller (a disconnected client, a request timeout) never cancels a
  write halfway.
- Postgres reads can be cancelled.

**What would flip this decision:** if the team cannot take the conversion
into `yihuang/main` within about two weeks, this is a fork again, and B
becomes the fallback.

## 2. Module layout

| Path                                                      | Owns                                                                                                                                                                                                                                                                                                                   | New lines (est.) |
|-----------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
| `src/db/mod.rs`                                           | `Db`, `Backend`, the re-exports of `config.rs`, `open`, `open_with`, `status`, `keepalive`, the `db_fn!` list (43 entries), the hand-written `save_anchoring_window`, `save_block_bundle(s)` and `save_token_metadata`, the token-label cache, test hooks, the `db-coverage` recorder (`coverage::hit`), the seal test | ~450             |
| `src/db/config.rs`                                        | `Role`, `DbTarget`, `DbUrl` and `DbConfig`, read from the environment; `pg_options`, the connect options with the credentials applied and the `sslmode` rule (section 7)                                                                                                                                               | section 11       |
| `src/db/sqlite.rs`                                        | `git mv src/db.rs`: upstream's code                                                                                                                                                                                                                                                                                    | section 4        |
| `src/db/indexer_jobs.rs`, `src/db/schema_check.rs`        | Unchanged paths, loaded through `#[path]` from `sqlite.rs`                                                                                                                                                                                                                                                             | section 4        |
| `src/db/sqlite/migrate.rs`                                | SQLite runner (v1 = `init_db`), shape hash, `init_db` pins, commute guard                                                                                                                                                                                                                                              | ~160             |
| `src/db/sqlite/extra.rs`                                  | New SQLite queries, which never edit existing ones (e.g. `tokens_missing_metadata`)                                                                                                                                                                                                                                    | ~40              |
| `src/db/migrations.rs`                                    | Shared version list (`include_str!` of both dialects), checksums, the applied-version checks, the web schema gate                                                                                                                                                                                                      | ~150             |
| `src/db/pg/mod.rs`                                        | `PgDb`, pools, the version floor, session settings, `DbError` and its classifier, `DB_FAILED`, preflight                                                                                                                                                                                                               | ~350             |
| `src/db/pg/writer.rs`                                     | Candidate loop, lock and self-checks, spawned and cancel-safe `with_txn`, budgets, watchdog, lease, `writer_seq`, leadership watch                                                                                                                                                                                     | ~380             |
| `src/db/pg/q.rs`                                          | `query_rows`, `try_query_rows`, `query_opt`, `try_query_opt`, `query_count`, `exec_best_effort`, and the writer's `run`, `exec`, `fetch_all` and `raw`, all with client deadlines; statement counter                                                                                                                   | ~170             |
| `src/db/pg/shared.rs`                                     | Re-exports of `sqlite.rs`'s `hex_blob`, `blob_hex`, `blob_addr`, `bigint`; column-list macros; the `HOLDING` text                                                                                                                                                                                                      | ~70              |
| `src/db/pg/{blocks,txs,tokens,transfers,kv,selectors}.rs` | Read SQL and row mappers                                                                                                                                                                                                                                                                                               | ~1,200           |
| `src/db/pg/plan.rs`                                       | Pure batch planner: dedup, net balance deltas, holder deltas. Dropped if stage 4b falls back to the per-row writer                                                                                                                                                                                                     | ~200             |
| `src/db/pg/{write,jobs,migrate}.rs`                       | Set-based writes, jobs, the Postgres runner and grants                                                                                                                                                                                                                                                                 | ~900             |
| `src/follow.rs`                                           | The web-role polling follower: live blocks, stats, schema gate, label-cache refresh                                                                                                                                                                                                                                    | ~150             |
| `src/metrics.rs`                                          | The Prometheus recorder, the series in section 8 and the `/metrics` route                                                                                                                                                                                                                                              | ~100             |
| `migrations/postgres/0001_baseline.sql`                   | `src/db/schema_pg.sql`, moved; `idx_tb_holding` gains `holder_addr`                                                                                                                                                                                                                                                    | moved            |
| `migrations/{sqlite,postgres}/NNNN_name.sql`              | Schema changes from 0002 on                                                                                                                                                                                                                                                                                            | —                |
| `deploy/k8s/`                                             | Manifests, secrets template and README (section 8)                                                                                                                                                                                                                                                                     | —                |

The module root is `src/db/mod.rs`, not a new `src/db.rs`. Deleting
`src/db.rs` lets git pair the rename, so teammates' edits to `src/db.rs`
merge into `src/db/sqlite.rs`.

## 3. The async API and dispatch

```rust
#[derive(Clone)]
pub struct Db(Arc<Inner>);                    // Inner { backend: Backend, labels: LabelCache }
enum Backend { Sqlite(sqlite::Db), Postgres(Box<pg::PgDb>) } // Box: clippy large_enum_variant
#[derive(Clone, Copy)]
pub enum Role { All, Web, Indexer }

/// Every test's call text still works. A Postgres URL, with or without its
/// scheme (`localhost:5432` counts, section 7), picks Postgres; anything
/// else is a SQLite path. Role::All.
pub async fn open(path_or_url: &str) -> anyhow::Result<Db>;
/// main.rs: role, URL, credentials, and the status channel /readyz reads.
pub async fn open_with(cfg: &DbConfig, status: watch::Sender<Status>) -> anyhow::Result<Db>;

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
    ($( $side:ident fn $n:ident($($a:ident: $t:ty),*) -> $r:ty; )*) => {$(
        pub async fn $n(db: &Db, $($a: $t),*) -> $r {
            #[cfg(feature = "db-coverage")] coverage::hit(stringify!($n));
            match &db.0.backend {
                Backend::Sqlite(s) => db_fn!(@call $side s $n ($($a),*)),
                Backend::Postgres(p) => pg::$n(p, $($a),*).await,
            }
        }
    )*};
}

db_fn! {
    inline   fn get_block_by_number(number: i64) -> Option<Block>;
    inline   fn get_token_holders(token_addr: &str, page: u32, per_page: u32) -> Vec<(String, String)>;
    blocking fn repair_derived_tables() -> ();
    extra    fn try_min_block_number() -> anyhow::Result<Option<i64>>;
    // … 39 more, one line each; 43 in all: 39 inline, 1 blocking, 3 extra
}

// Hand-written, like save_anchoring_window: these three update the label
// cache after a successful commit, and db_fn! has no after-call step.
pub async fn save_block_bundles(db: &Db, bundles: &[BlockBundle]) -> Result<()> {
    #[cfg(feature = "db-coverage")] coverage::hit("save_block_bundles");
    match &db.0.backend {
        Backend::Sqlite(s) => sqlite::save_block_bundles(s, bundles)?,
        Backend::Postgres(p) => pg::save_block_bundles(p, bundles).await?,
    }
    db.0.labels.committed(bundles);
    Ok(())
}

pub async fn save_block_bundle(db: &Db, bundle: &BlockBundle) -> Result<()> {
    #[cfg(feature = "db-coverage")] coverage::hit("save_block_bundle");
    let one = std::slice::from_ref(bundle);
    match &db.0.backend {
        Backend::Sqlite(s) => sqlite::save_block_bundle(s, bundle)?, // unchanged, db.rs:505-507
        Backend::Postgres(p) => pg::save_block_bundles(p, one).await?, // no singular twin
    }
    db.0.labels.committed(one);
    Ok(())
}

pub async fn save_token_metadata(db: &Db, meta: &TokenMeta) -> Result<()> {
    #[cfg(feature = "db-coverage")] coverage::hit("save_token_metadata");
    match &db.0.backend {
        Backend::Sqlite(s) => {
            sqlite::save_token_metadata(s, meta)?;
            db.0.labels.put(meta);
            Ok(())
        }
        // A page view can be dropped mid-save (ROLE=all). The write runs on in
        // its spawned task (section 6), so the put must run in the same task.
        Backend::Postgres(_) => {
            let (db, meta) = (db.clone(), meta.clone());
            let task = tokio::spawn(async move {
                let Backend::Postgres(p) = &db.0.backend else { unreachable!() };
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
```

A missing Postgres twin fails to compile, because `pg::$name` must exist.
The hand-written wrappers name their twins too, so the same holds for them.

**Function classes**

| Class                                                                                                                | Change                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
|----------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `now_ts`, `page_offset`, `Holder`, `TxColumns` and the row types                                                     | `pub use sqlite::…`, unchanged                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 39 `&Db` I/O functions                                                                                               | `fn` → `async fn` through `db_fn!`, inline on SQLite, with the same parameters and return types                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `repair_derived_tables`                                                                                              | `db_fn!` with the `blocking` marker. Today it rebuilds whole tables on a runtime worker (`indexer.rs:985`)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `try_min_block_number` (new, `extra`)                                                                                | `-> anyhow::Result<Option<i64>>`. Its SQLite twin in `sqlite/extra.rs` is `Ok(get_min_block_number(s))`, so SQLite behaviour is unchanged; the Postgres arm returns the error instead of degrading (section 6)                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `tokens_missing_metadata` (new, `extra`)                                                                             | Token addresses referenced by `transfer_events.token_addr` or `transactions.fee_token` that have no `token_metadata` row. The SQLite twin lives in `sqlite/extra.rs`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `try_all_token_metas` (new, `extra`)                                                                                 | `-> anyhow::Result<Vec<TokenMetadata>>`: `get_all_token_metas`, but an error is returned rather than degraded to an empty list. The label cache seeds and reloads from it, so a failed read never looks like an empty table. The SQLite twin in `sqlite/extra.rs` reuses `TOKEN_COLS` and `row_to_token`                                                                                                                                                                                                                                                                                                                                                                       |
| `save_anchoring_window`                                                                                              | Hand-written wrapper. The public bound changes from `FnOnce` to `Fn + Send + 'static`, because pass 2 runs inside the spawned writer task (section 6). The one caller (`indexer.rs:576-580`) makes its closure a `move` closure that owns `logs`, and adds `.await`                                                                                                                                                                                                                                                                                                                                                                                                            |
| `save_block_bundle`, `save_block_bundles`, `save_token_metadata`                                                     | Hand-written wrappers with the same signatures. Each dispatches like `db_fn!`, then, only after the write returns `Ok`, updates the token-label cache. Under `ROLE=indexer`, `save_block_bundle(s)` also note the token addresses the bundles reference that the cache has no entry for, for the missing-metadata job (section 6). A failed write changes neither. On Postgres, `save_token_metadata` runs its write and its cache update in one spawned task, so a dropped page view cannot skip the update. `save_block_bundle` calls `pg::save_block_bundles` with one bundle, so Postgres has no singular twin, and the SQLite arm still calls the unchanged singular body |
| `keepalive` (new)                                                                                                    | Postgres: `SELECT 1` on the writer session, bounded to 5 s. SQLite: nothing                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `token_label(db, addr) -> Option<String>` (new, sync)                                                                | Reads the label cache. Used by the Tera `address_label` function (`web.rs:2284-2290`)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `take_tokens_without_metadata(db) -> Vec<String>` (new, sync)                                                        | In memory, not a `db_fn!` entry. Drains the addresses the bundle wrappers noted, for the missing-metadata job (section 6). An address with an empty-label cache entry counts as known, so this is not `token_label(..).is_none()`                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `#[doc(hidden)] pub use sqlite::{init_db, counter, get_block_timestamp, sync_holder_counts}`                         | Sync, SQLite-only test hooks, unchanged                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `pub fn lock(db: &Db) -> MutexGuard<'_, Connection>`                                                                 | Same text. SQLite: `sqlite::lock(s)`. Postgres: panics "SQLite-only test hook"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |

**The 7 `db::lock` call sites in tests stay byte-identical**
(`decoder.rs:685,772,859,1040`, `pages.rs:764`, `live_rpc.rs:435`,
`write_scale.rs:75`). Holding the guard across an `.await` would deadlock,
and `clippy::await_holding_lock` rejects that under `-D warnings`.

**The token-label cache** replaces the per-row `get_token_metadata` lookup
behind Tera's `address_label`, which is sync and cannot await. It is
`RwLock<HashMap<addr, label>>` in `Db` (plus, under `ROLE=indexer`, the set
of noted addresses below), with one entry per `token_metadata` row, keyed
by the checksummed address, which is how `address_label` looks it up. The
label is the symbol, else the name, else empty. `token_label` returns
`None` for an empty label, so `address_label` falls through to its later
rules as today (`web.rs:579-586`). The cache is:

- seeded at open from `try_all_token_metas`. A seed, a reload and a
  re-seed merge rows into the map and never remove an entry, which is exact
  because no code deletes `token_metadata` rows. On `Err` the cache stays
  as it was;
- updated by the three hand-written write wrappers above, once the write
  returns `Ok`. `save_token_metadata` puts its row. `save_block_bundle(s)`
  puts each bundle's `tokens` in order, so the last metadata for an address
  wins, as in the write. A failed write leaves the cache as it was. On
  Postgres, `save_token_metadata` runs its write and its put in one spawned
  task, so a page view dropped mid-save still updates the label.
  `ROLE=all` labels are therefore immediate, as today, with no change to
  the rusqlite bodies;
- under `ROLE=indexer`, the source of the missing-metadata job's work.
  After each commit, and after putting the bundles' `tokens`,
  `save_block_bundle(s)` note every token address the bundles reference
  (a transfer's `token_addr` or a transaction's `fee_token`) that has no
  cache entry, and `take_tokens_without_metadata` drains them (section 6).
  Other roles note nothing;
- reloaded from `try_all_token_metas` every 30 s under `ROLE=web`, by the
  follower (section 8), which is how web replicas see the indexer's inserts
  and repairs;
- under `ROLE=all` on SQLite, every label change goes through this
  process's own wrappers, so the reload runs only until a seed returns
  `Ok`, and SQLite gains no periodic scan;
- on Postgres under `ROLE=indexer` or `ROLE=all`, also re-seeded each time
  the writer becomes leader, after the runner finishes, retrying every 30 s
  until a re-seed returns `Ok`. The open-time
  seed predates the migrations, and the previous leader keeps writing while
  this pod waits as a candidate (section 8).

**Anchoring on Postgres takes two passes.** SQLite passes the closure to its
unchanged body. Postgres:

1. Calls `events` with a stamp that records each requested block number and
   returns `None`. `anchoring_event_from_log` returns at `stamp(..)?` before
   decoding (`indexer.rs:595-596`), so this pass is cheap.
2. Inside the writer transaction, runs
   `SELECT number, timestamp FROM blocks WHERE number = ANY($1)`.
3. Calls `events(&|n| map.get(&n).copied())`, inserts the events
   set-based, sets the watermark and commits.

The closure must be deterministic. The only caller's closure is pure over
`logs`.

### Worked example: `get_block_by_number`

SQLite is unchanged (`db.rs:583-591`). Postgres:

```rust
pub(crate) async fn get_block_by_number(p: &PgDb, number: i64) -> Option<Block> {
    q::query_opt(&p.read, "get_block_by_number",
        concat!("SELECT ", block_cols!(), " FROM blocks WHERE number = $1"),
        |q| q.bind(number), rows::block).await
}
```

- **`q::query_opt` keeps SQLite's degrade rule.** An error is logged and
  becomes `None`. It also sets `DB_FAILED` and has a client deadline
  (section 6).
- **`concat!` yields the `&'static str` that sqlx requires.** sqlx's
  `SqlSafeStr` takes only a static string (`sqlx-core-0.9.0/src/sql_str.rs:52`).
- **The column lists cannot drift.** A unit test inside `sqlite/migrate.rs`
  asserts `block_cols!() == BLOCK_COLS` (`db.rs:457`). The same holds for
  `TX_COLS`, `TX_LIST_COLS`, `TOKEN_COLS`, `TRANSFER_COLS` and `HOLDING`.
  As a child of `sqlite`, that module can read the private consts, so no
  SQLite visibility changes.

### Worked example: `get_token_holders`

```sql
-- SQLite: unchanged (db.rs:1524-1526). Ties come out in index order.
SELECT holder_addr, balance FROM token_balances WHERE token_addr=?1 AND balance NOT LIKE '-%'
ORDER BY LENGTH(balance) DESC, balance DESC LIMIT ?2 OFFSET ?3

-- Postgres (pg/tokens.rs): adds a tie-break so pages are stable.
SELECT holder_addr, balance FROM token_balances WHERE token_addr = $1 AND balance NOT LIKE '-%'
ORDER BY LENGTH(balance) DESC, balance DESC, holder_addr LIMIT $2 OFFSET $3

-- migrations/postgres/0001_baseline.sql
CREATE INDEX IF NOT EXISTS idx_tb_holding ON token_balances
  (token_addr, LENGTH(balance) DESC, balance DESC, holder_addr) WHERE balance NOT LIKE '-%';
```

- The predicate text matches the index predicate, so the planner can prove
  the partial index applies.
- `tests/postgres.rs`'s `TRANSLATED` pin is updated for the Postgres side.
- Tests compare tie groups across all pages (section 9).

### Worked example: `save_block_bundles`

**SQLite** is unchanged (`db.rs:512-560`): one transaction, per-row
`prepare_cached` statements, and `adjust_balance`.

**Postgres** is set-based, in one transaction on the session that holds the
lock:

```rust
pub(crate) async fn save_block_bundles(p: &PgDb, bundles: &[BlockBundle]) -> Result<()> {
    if bundles.is_empty() { return Ok(()); }
    let plan = Arc::new(plan::BatchPlan::new(bundles)); // blocks/txs/tokens last-wins; transfers first-wins; anchoring repeats left to DO NOTHING
    p.writer()?.write(Budget::Batch, move |c| { let plan = plan.clone(); Box::pin(async move {
        let (new_txs, stored): (i64, i64) = q::fetch_one(c, "probe", PROBE, plan.probe_binds()).await?;
        q::exec(c, "blocks", UPSERT_BLOCKS, plan.block_binds()).await?;
        q::exec(c, "txs", UPSERT_TXS, plan.tx_binds()).await?;
        q::exec(c, "counters", BUMP_COUNTERS, (plan.len() as i64 - stored, new_txs)).await?;
        let fresh: Vec<(i64, i64)> = q::fetch_all(c, "transfers", INSERT_TRANSFERS, plan.transfer_binds()).await?;
        q::exec(c, "anchoring", INSERT_ANCHORING, plan.anchoring_binds()).await?;
        q::exec(c, "tokens", UPSERT_TOKENS, plan.token_binds()).await?;   // metadata before balances
        let net = plan.net(&fresh);
        let old = q::fetch_all(c, "balances", READ_BALANCES, net.keys()).await?;
        let out = plan::apply_deltas(net, &old);  // same rule as adjust_balance (db.rs:1169-1206); 0 ⇒ delete
        q::exec(c, "upsert_bal", UPSERT_BALANCES, out.upserts()).await?;
        q::exec(c, "delete_bal", DELETE_BALANCES, out.deletes()).await?;
        q::exec(c, "holders", BUMP_HOLDERS, out.holder_deltas()).await?;
        Ok(())
    }) }).await
}
```

`write` runs the closure on a spawned task, once per attempt, so each
attempt takes its own clone of the `Arc`'d plan. It also adds the
`writer_seq` bump (section 6). Statements with nothing to do are skipped.

```sql
-- PROBE: the per-block counter semantics of db.rs:527-539
SELECT COALESCE(SUM(GREATEST(u.n - (SELECT COUNT(*) FROM transactions t WHERE t.block_number = u.num), 0)), 0)::int8,
       (SELECT COUNT(*) FROM blocks WHERE number = ANY($1))
FROM UNNEST($1::int8[], $2::int8[]) AS u(num, n);

-- UPSERT_BLOCKS: one INSERT … SELECT FROM UNNEST($1::int8[], $2::bytea[], …)
--   ON CONFLICT (number) DO UPDATE SET …, with the same columns as upsert_block.

-- BUMP_COUNTERS: qualify the target column; a bare `n` is ambiguous on Postgres (db.rs:400)
INSERT INTO counters (name, n) VALUES ('blocks', $1), ('transactions', $2)
ON CONFLICT (name) DO UPDATE SET n = counters.n + excluded.n;

-- INSERT_TRANSFERS: RETURNING gives exactly the rows insert_transfer reports as new
INSERT INTO transfer_events (tx_hash, block_number, log_index, token_addr, from_addr, to_addr, amount, timestamp, created_at)
SELECT * FROM UNNEST($1::bytea[], $2::int8[], $3::int8[], $4::bytea[], $5::bytea[], $6::bytea[], $7::text[], $8::int8[], $9::int8[])
ON CONFLICT (block_number, log_index) DO NOTHING RETURNING block_number, log_index;

-- UPSERT_TOKENS: holder_count seeded from balances, as upsert_token_meta does (db.rs:908-933)
INSERT INTO token_metadata (address, name, symbol, decimals, currency, total_supply, logo_uri, holder_count, created_at, updated_at)
SELECT u.a, u.n, u.s, u.d, u.c, u.t, '',
       (SELECT COUNT(*) FROM token_balances b WHERE b.token_addr = u.x AND b.balance NOT LIKE '-%'), $8, $8
FROM UNNEST($1::bytea[], $2::text[], $3::text[], $4::int8[], $5::text[], $6::text[], $7::text[]) AS u(a, n, s, d, c, t, x)
ON CONFLICT (address) DO UPDATE SET name = excluded.name, symbol = excluded.symbol, decimals = excluded.decimals,
  currency = excluded.currency, total_supply = excluded.total_supply, updated_at = excluded.updated_at;

-- BUMP_HOLDERS
UPDATE token_metadata m SET holder_count = m.holder_count + u.by, updated_at = $3
FROM UNNEST($1::bytea[], $2::int8[]) AS u(addr, by) WHERE m.address = u.addr;
```

**Cost per batch.** At most 14 round trips: BEGIN, 11 statements, the
`writer_seq` bump and COMMIT. Today's per-row path costs 642 for the
`write_scale` block shape.

**Equivalence with the per-row path.** Netting the deltas per (token,
holder) telescopes to the same final balances and holder counts. The
planner property test and the differential test pin this (section 9).

**The jobs become set-based on Postgres only.** Postgres has no
Keccak-256, so the EIP-55 checksummed text keys of `token_balances` and
`genesis_balances` are always made in Rust with `checksum_address`.

- **`rebuild_token_balances`** runs in one writer transaction on the `Long`
  budget:
  1. `DELETE FROM token_balances`.
  2. One aggregate keyed on bytes:
     ```sql
     SELECT tok, holder, SUM(d)::text FROM (
       SELECT token_addr tok, from_addr holder, -(amount::numeric) d FROM transfer_events
       UNION ALL SELECT token_addr, to_addr, amount::numeric FROM transfer_events
       UNION ALL SELECT decode(substr(token_addr, 3), 'hex'), decode(substr(holder_addr, 3), 'hex'),
                        balance::numeric FROM genesis_balances) s
     GROUP BY tok, holder HAVING SUM(d) <> 0
     ```
     This keeps `adjust_balance`'s rule: a zero balance has no row, and
     negative balances are kept.
  3. Checksum both keys of each row in Rust.
  4. Insert in chunks with `INSERT … SELECT FROM UNNEST($1::text[], $2::text[], $3::text[])`.
  5. Recount the holders with the set-based `sync_holder_counts`.
- **`sync_holder_counts`** becomes one `UPDATE … FROM (… GROUP BY)`.
- **`holders_without_genesis_balance`** takes two round trips instead of up
  to 2,000 `EXISTS` probes:
  1. Read the cursor and the transfer page (`id > $1 ORDER BY id LIMIT $2`)
     as bytes.
  2. Checksum and dedupe the addresses in Rust, dropping `ZERO_ADDRESS`.
  3. Anti-join on the primary key:
     `SELECT u.t, u.h FROM UNNEST($1::text[], $2::text[]) AS u(t, h) WHERE NOT EXISTS (SELECT 1 FROM genesis_balances g WHERE g.token_addr = u.t AND g.holder_addr = u.h)`.

  A failed cursor read is an error, never a cursor of 0.
- **`save_genesis_balances`** writes with `UNNEST`.

## 4. The SQLite path

**Changes to existing SQLite code: −3/+7 lines now.** These counts were
measured on a scratch copy, where all 26 moved unit tests pass.

| Stage | File:line                          | Change                                                                                                                                                                        | Δ      |
|-------|------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| 1     | `src/db.rs` → `src/db/sqlite.rs`   | `git mv`                                                                                                                                                                      | rename |
| 1     | `sqlite.rs` (was `db.rs:425-426`)  | `#[path = "indexer_jobs.rs"]` and `#[path = "schema_check.rs"]` on the existing `mod` lines, so both files stay in `src/db/` (`schema_check.rs:767` includes `../indexer.rs`) | +2     |
| 1     | `indexer_jobs.rs:10`               | `use crate::db::{self, Db};` → `use crate::db::sqlite::{self as db, Db};`                                                                                                     | −1/+1  |
| 1     | `schema_check.rs:419` (tests)      | `use crate::db;` → `use crate::db::sqlite as db;`                                                                                                                             | −1/+1  |
| 1     | `sqlite.rs`, after the `mod` lines | `pub(crate) mod extra;` (→ `src/db/sqlite/extra.rs`)                                                                                                                          | +1     |
| 2     | `sqlite.rs` (was `db.rs:421`)      | `schema_check::verify(&conn)…` → `migrate::run(&conn).with_context(\|\| format!("schema of {path}"))?;`                                                                       | −1/+1  |
| 2     | `sqlite.rs`, after the `mod` lines | `mod migrate;` (→ `src/db/sqlite/migrate.rs`)                                                                                                                                 | +1     |

**When the first SQLite 0002 lands**, `schema_check.rs` gains a version
bound, a few lines:

- `expected()` becomes `expected(upto)`: `init_db(":memory:")` plus the
  bodies 0002..`upto`.
- `verify` uses `expected(B)`.
- `explain` reads the file's own `MAX(version)` (1 when there is no table),
  clamps it to B, and compares against `expected(that)`.
- Pending versions are reported on their own line, never as drift with a
  recovery hint.

**Unchanged:**

- every SQL string, including the tie order;
- the pragmas (`db.rs:153-172`);
- `init_db`, its legacy DROPs, `seed_counters` and `HOLDING`;
- the per-row writer and `adjust_balance`;
- `save_anchoring_window`'s body;
- `schema_check`'s comparison and its recovery text;
- the seal test (`indexer_jobs.rs:268-297`, which skips `src/db/`).

**The runtime is unchanged too.** There is one `Mutex<Connection>`, called
inline, and no reader pool. Only `repair_derived_tables` runs on the
blocking pool.

**New SQLite queries go in `sqlite/extra.rs`.** For now that is
`try_min_block_number`, `tokens_missing_metadata` and
`try_all_token_metas`, so existing bodies are never edited. `db_fn!`'s
`extra` marker calls them.

**Postgres reuses the helpers it needs.** `pg/shared.rs` re-exports
`hex_blob`, `blob_hex`, `blob_addr` and `bigint`, which are `pub(crate)` in
`sqlite.rs`; their bodies are unchanged.

## 5. Migrations on both backends

### The model

- **One shared list.** `migrations.rs` declares
  `migrations!{ 1 => "baseline", … }`.
- **D and B.** Two numbers used throughout this section:
  - **D** is the database's version: the highest `version` in its
    `schema_migrations` table.
  - **B** is the binary's version: the last entry in the `migrations!`
    list compiled into the running image.
  - D < B means this release brings migrations to apply. D > B means a
    newer release already migrated this database, and this binary is older
    than the schema.
- **Version 1.**
  - On SQLite, version 1 **is** the frozen `init_db`: a Rust step with no
    DDL file.
  - On Postgres, it is `migrations/postgres/0001_baseline.sql`, which is
    phase 2's `schema_pg.sql` moved. Its only change is `holder_addr` in
    `idx_tb_holding`, and it seeds no rows.
- **From 0002 on, every version is a twin pair**
  (`migrations/sqlite/NNNN_name.sql` and `migrations/postgres/NNNN_name.sql`),
  with the same number and name.
- **A list test checks** contiguous, unique versions, and that every file
  on disk is in the list. The `migrations!` macro pairs the twins: a missing
  file does not compile.
- **No headers.** Files are plain SQL; a twin with nothing to do is empty.
  Stage 2 shipped `-- kind: expand|contract` with an expand-body check,
  `-- noop:` and `-- no-transaction` headers. No migration used them, so they
  were dropped; the expand/contract rule is left to review ("Version skew and
  deploy order" below).

**Table** (the same on both backends, with INTEGER types on SQLite):

```sql
CREATE TABLE IF NOT EXISTS schema_migrations (
  version BIGINT PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL,
  applied_at BIGINT NOT NULL, applied_by TEXT NOT NULL);
```

`schema_check` ignores tables that exist only in the file
(`schema_check.rs:16-18`), so this table is invisible to `verify` and
`explain`.

### Checksums and immutability

- **Checksum of a file:** sha3-256 of its `include_str!` text, with CRLF
  normalized, computed when the runner or the preflight checks it.
- **SQLite v1's checksum:** the constant `BASELINE_SHA3`. It is the sha3 of
  `SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name`
  over `init_db(":memory:")`.
- **At runtime:** the indexer refuses a mismatch with "migration N was
  edited after it was applied". Web replicas check versions only, never
  checksums.
- **In PRs:** a CI step fails when any path under `migrations/` is
  modified, deleted or renamed relative to the base branch. Merged files are
  immutable, so the fix is always a new version. The step uses
  `git diff --name-status origin/main...HEAD -- migrations/` with
  `fetch-depth: 0`. There is no committed checksum file to regenerate.

### Who assigns version numbers

A version number must mean the same file everywhere. If two repos each
merged a different `0002`, a database that ran one would refuse an image
built from the other: the checksums differ, and there is no override. So
`NVNM-Chain/nvnmchain-explorer`, the main repo (decision 4), owns the
numbers:

- **Migrations merge into NVNM-Chain's `main` first.**
  `yihuang/nvnmchain-explorer` and personal forks get migration files only
  by syncing from it and never add their own, so every repo's images carry
  the same files.
- **A PR takes the next free number when it is opened**, and renumbers its
  pair if another PR merges a migration first. Renaming an unmerged file is
  allowed: the CI immutability step protects only files already on the
  base branch.
- **NVNM-Chain's `main` requires a PR to be up to date before it merges,**
  with the list test as a required check, so that test always runs
  against the latest numbers. Without it, two PRs that each add a `0003`
  under different names pass their own CI and clash only on `main`, where
  `docker.yml` has already built an image from the clash. Today the org
  ruleset requires a review and squash merges but no status checks, so an
  org admin adds this in stage 2.
- **A migration merged into another repo first** is the mistake this
  prevents. Any deploy from yihuang's `main`, such as a `fly deploy` of
  the Fly app, would apply that migration to its SQLite file at once. If
  NVNM-Chain has used the number, the fix is renaming a merged file and
  rebuilding every database that ran it.

### Freezing `init_db`

Upstream changed `init_db` about 21 times in two months. From stage 2 on,
schema changes go into migration pairs, and two tests enforce that:

- **Pin A:** `shape_hash(init_db(":memory:")) == BASELINE_SHA3`. It catches
  DDL and `HOLDING` edits.
- **Pin B:** the sha3 of `init_db`'s whole body, read through
  `include_str!("../sqlite.rs")`, with the `pragma_update` and `cache_kib`
  lines filtered out. It catches edits to the DROP list and `seed_counters`,
  and still lets pragmas be tuned in place, as today.

Both fail with "init_db is migration 1 and frozen: add
`migrations/{sqlite,postgres}/NNNN_*.sql` (docs/database.md)".

### The SQLite runner

```mermaid
flowchart TD
    A["db::open(path)"] --> B["init_db(path)<br/>unchanged: pragmas, legacy DROPs, seed_counters"]
    B -->|error| X["schema_check::explain<br/>same message as today"]
    B --> C["BEGIN IMMEDIATE"]
    C --> D{"schema_migrations has v1?"}
    D -->|"no: fresh or legacy file"| E["create table, stamp v1 (BASELINE_SHA3)"]
    D -->|yes| F["verify versions and checksums"]
    E --> F
    F --> G{"D > B?"}
    G -->|yes| R["ROLLBACK, refuse"]
    G -->|no| H["apply pending 0002..B, one row each"]
    H --> I["schema_check::verify"]
    I -->|drift| R
    I -->|ok| J["COMMIT"]
```

- **`init_db` runs outside any transaction.** It opens its own connection
  (`db.rs:153`), so an outer transaction would make its DDL wait out the
  5 s `busy_timeout`. Also, SQLite silently ignores
  `PRAGMA journal_mode=WAL` inside a transaction on a fresh file.
- **Fresh, legacy and stamped files all take this one path.** On a legacy
  file that has drifted, `verify` fails inside the transaction, nothing is
  stamped, and the error text is today's.
- **Adoption is tested permanently.** The canary fixtures stay legacy, since
  they are opened read-only.
- **SQLite always refuses D > B.** It runs only as `ROLE=all` in one
  process, so it never sees a rolling deploy.
- **No file lock.** `BEGIN IMMEDIATE` serializes two processes that open at
  once. Two processes on one file are not supported today either.

### The commute rule

`init_db` keeps running on every open. That keeps today's pragmas, legacy
DROPs and seeding with no copied code. In exchange, every SQLite migration
must commute with `init_db`:

- it must not drop, by name, an object that `init_db` creates;
- it must not create a name that is on `init_db`'s DROP list
  (`db.rs:217-288`).

A guard test checks every N: temp file → `init_db` → 0002..N → hash →
`init_db` again → hash must be equal.

| Change                                 | SQLite twin                                                                                                                                                                                                                    | Postgres twin                                                                                                                                                                                                                       |
|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| New table, column or index             | As usual. `init_db`'s `IF NOT EXISTS` ignores objects it does not know                                                                                                                                                         | As usual, in one transaction                                                                                                                                                                                                        |
| Re-keyed index: two releases, new name | **Expand N:** `CREATE INDEX x_v2 ON t(new key)`; queries move to the new key. **Contract N+1:** `DROP INDEX x; CREATE INDEX x ON t(col) WHERE 0` (an empty stub that costs nothing per insert, and that `IF NOT EXISTS` skips) | **Expand N:** `CREATE INDEX x_v2 ON t(new key)`. **Contract N+1:** `DROP INDEX x`. Postgres never has a window without a usable index                                                                                               |
| Retired index                          | The stub, as above                                                                                                                                                                                                             | `DROP INDEX x`                                                                                                                                                                                                                      |
| Re-keyed derived table                 | `DROP TABLE t; CREATE TABLE t (…)`, recreate `init_db`'s index names on it (stubs where needed), and `DELETE FROM kv WHERE key = '…'` for its watermark                                                                        | The same, in one transaction                                                                                                                                                                                                        |
| Retired table                          | `DELETE FROM t`, plus stub indexes                                                                                                                                                                                             | `DROP TABLE t`                                                                                                                                                                                                                      |

- The parity test ignores `WHERE 0` stubs whose Postgres twin dropped the
  index.
- **Escape hatch.** If the rule ever becomes a burden, stamped files skip
  `init_db` and use a copied pragma block, plus a test that the two blocks
  are equal. That is the point at which SQLite code starts to change.

### The Postgres runner

**Where it runs.** On the writer session after it wins the lock and passes
the self-checks (section 6), and again after every re-acquire. Like other
`Long` work it runs under the watchdog, since its statements have no
timeout.

**What it refuses.** A lock-free **preflight**, run before the candidate
loop (section 8), catches all of these before the lock is taken. The
writer runs it again on its own session once it holds the lock, before the
runner, since a newer release may have migrated while it waited:

- a schema that has tables but no `schema_migrations`, i.e. one applied by
  hand. The remedy is to drop the schema and re-index;
- a version missing below the highest. The runner records versions in
  order, so a gap means rows edited by hand, and which files ran is
  unknown. The remedy is the same;
- a checksum mismatch;
- D > B. There is no override: rolling back across a migration means
  rolling forward.

Stage 4a also refused an index left INVALID. Only an interrupted concurrent
build leaves one, which no migration runs any more (below), so the refusal
was dropped with the no-transaction files.

**Every file** runs as one transaction:

1. `BEGIN; SET LOCAL statement_timeout = 0; SET LOCAL lock_timeout = '5s'`;
2. the body;
3. the version row;
4. `COMMIT`.

Any failure, a `55P03` (lock timeout) included, is `Unavailable`: the
writer drops the session and runs the preflight and the runner again on a
new one, backing off from 1 s to 30 s between tries. The rolled-back file
re-runs from the top.

**No concurrent index builds.** Stage 4a also ran `-- no-transaction` files
of `CREATE INDEX CONCURRENTLY`, a statement at a time. No migration used
them, so they were dropped; they come back with the first migration that
needs one. A plain `CREATE INDEX` blocks writes to its table but not reads,
so web replicas keep serving while it builds, and the writer runs no chain
writes until the runner finishes. A file must never pair a long index
build with a statement that takes `ACCESS EXCLUSIVE` (`DROP INDEX`,
`ALTER TABLE`, …), which would block reads for the whole build.

**Grants.** After migrations, the runner re-applies the web role's grants
on every start. GRANT is idempotent, so this also restores grants lost when
a migration recreates a table (section 7).

**Why not sqlx's `Migrator`.** It cannot drive rusqlite, its lock is
database-wide, and it returns without unlocking on error
(`sqlx-core-0.9.0/src/migrate/migrator.rs:237-293`).

### Version skew and deploy order

| Process | Database D vs binary B | Result               |
|---------|------------------------|----------------------|
| Indexer | D < B                  | migrates             |
| Indexer | D = B                  | runs                 |
| Indexer | D > B                  | refuses at preflight |
| Web     | any                    | ready when D ≥ B     |

- **What `expand` allows** is decided in PR review: it adds, and a new
  column is nullable or has a default. Anything that removes or rewrites
  what the code uses is a contract.
- **When a `contract` ships.** One release after the code stops using the
  object, and the PR names that release. The previous release's web
  replicas then never touch what the contract removes. A re-keyed derived
  table is a contract: either accept its rebuild window or use a new name
  plus a later contract. If the rule is broken, old web replicas return
  503 with `Retry-After` on the affected pages until web rolls.
- **The web gate is re-evaluated** by the follower whenever
  `MAX(version)` changes (section 8), never by the probe handler.
- **Deploy order, for every release:** roll the indexer first and wait
  until its `/readyz` reports `schema.db == B`, then roll web. A new web
  replica waits until the indexer has migrated (D ≥ B), and the old
  replicas keep serving meanwhile. This is written into the runbook and
  `deploy/k8s/README.md`.
- **Why there is no per-version gate.** A gate that kept old web replicas
  unready through a contract would only protect a web image rolled back two
  or more releases, which the roll-forward rule already excludes. When the
  one-release rule is broken, it would make every old replica unready at
  once, a whole-tier outage instead of 503s on a few pages (section 14).

### A teammate's schema change

A schema change is a PR to NVNM-Chain's `main`, numbered as in "Who
assigns version numbers" above. It touches:

- `migrations/sqlite/NNNN_name.sql`, written to the commute rule;
- `migrations/postgres/NNNN_name.sql`;
- one line in `migrations.rs`;
- the query edits in `sqlite.rs` (or `sqlite/extra.rs`) and in `pg/*`.

**Parity.** For every version N, the shape of SQLite after `init_db` and
0002..N must equal the shape of Postgres after 0001..N. This reuses phase
2's `sqlite_shape`, `pg_shape`, `ALLOWED`, `TRANSLATED` and `uncollated`,
and replaces `schema_pg_matches_init_db`.

## 6. Postgres connections, the writer and the session advisory lock

### Connections

Each connection carries its own settings in its startup `options` (sent as
`-c k=v`). Cloud SQL has no instance flags for `statement_timeout`,
`idle_session_timeout` or `synchronous_commit`.

| Connection      | Used by                                    | Settings                                                                                                                                                                                                                                                                                                                    |
|-----------------|--------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `read: PgPool`  | `all`; web (max 8); indexer (max 4); min 1 | `acquire_timeout` 3 s (sqlx default 30 s). `test_before_acquire(false)`, since the default pings on every acquire. `idle_timeout` 5 min. `statement_timeout=5s`, `idle_session_timeout=0`, `default_transaction_read_only=on`, `idle_in_transaction_session_timeout=10s`, `application_name=explorer-<role>/<version>/b<B>` |
| `cache: PgPool` | `all`, web (max 2)                         | `statement_timeout=2s`, `idle_session_timeout=0`, `idle_in_transaction_session_timeout=5s`                                                                                                                                                                                                                                  |
| **Writer**      | `all`, indexer                             | **One owned `PgConnection`, never pooled**, in `tokio::sync::Mutex<Option<PgConnection>>`. `statement_timeout=60s`, `lock_timeout=5s`, `idle_session_timeout=30s`, `idle_in_transaction_session_timeout=30s`, `tcp_keepalives_idle/interval/count=10/5/3`, `tcp_user_timeout=20000`, `client_connection_check_interval=10s` |

**Role defaults are only a backstop.** A role default applies to every
connection that role opens, so it cannot stand in for per-connection
values.

- **Never set as a role default:** `idle_session_timeout`, which is the
  writer's lease, and `default_transaction_read_only`.
- **Allowed as role defaults:** values that are safe for every connection
  of that role: `statement_timeout` (`explorer_web` 5 s, `explorer_indexer`
  60 s) and `synchronous_commit`.
- **The writer checks its own settings.** After connecting, it checks
  `current_setting('idle_session_timeout') = '30s'`. If anything stripped
  its startup options, it refuses to become a candidate.

**Client deadlines.**

- Every `q` read wraps its round trip in `tokio::time::timeout` on an
  explicitly acquired connection: 7 s for reads (statement timeout + 2 s),
  4 s for cache writes.
- On a timeout, the connection is closed (`close_on_drop`) and never
  returned to the pool to be pinged. Then `DB_FAILED` is set.
- A web read therefore fails within about 7 s on a half-open socket, and
  within 3 s when no connection can be had.
- The watchdog's and the re-acquire's queries use the same 5 s rule.

**`synchronous_commit` stays `on`.** If commit latency measures above 5 ms,
turning it off on the writer is acceptable, because the data is
re-derivable and `writer_seq` catches a lost suffix.

### The lock

- **Key:** `pg_try_advisory_lock(K1, K_WRITER)`. `K1 = 0x4E564E4D`
  ("NVNM"). `K_WRITER` is the first 4 bytes of
  `sha3_256("{current_schema}:writer")`.
  - Advisory locks are per database, and the key adds the schema, so
    parallel test schemas never contend.
  - The two-int key space cannot collide with sqlx's bigint migrator key.
  - `hashtext` is avoided because its output has changed between Postgres
    versions.
- **Raw SQL, not `PgAdvisoryLock`.** `PgAdvisoryLock::try_acquire` is not
  cancel-safe (`sqlx-postgres-0.9.0/src/advisory_lock.rs:257-261`).
- **Never unlocked.** To step down, the writer drops the session. That also
  avoids the trap where repeated locks stack and each needs its own unlock.

```mermaid
stateDiagram-v2
    [*] --> Preflight: open_with (ROLE=indexer or all)
    Preflight --> Candidate: schema, no gaps, checksums, D ≤ B
    Preflight --> [*]: refuse (exit 1, never Ready)
    Candidate --> Candidate: try_lock false, retry every 5 s on the same session
    Candidate --> Leader: try_lock true, self-checks, preflight, migrate, grants
    Leader --> Leader: batch committed (writer_seq bumped)
    Leader --> Reacquiring: Unavailable error, timeout, session lost
    Reacquiring --> Leader: reconnect, try_lock true, self-checks, writer_seq matches, preflight, migrate, grants
    Reacquiring --> SeqMismatch: writer_seq not this run's last or last+1
    Reacquiring --> Lost: try_lock false for 120 s
    SeqMismatch --> [*]: exit 4, restart from the database's truth
    Lost --> [*]: exit 3, restart as candidate
```

**Candidate.** Inside `open_with`, the process connects (10 s timeout) and
runs `SELECT pg_try_advisory_lock(K1, K_WRITER)` (5 s timeout).

- **On `false`:** it retries every 5 s on the same session, so the session
  never idles past `idle_session_timeout`. A standby spawns no jobs.
- **On `true`:** it runs the self-checks, records the pid, runs the
  preflight again, then the migrations and grants under the `Long`
  watchdog, and becomes Leader. A refusal there exits 1, as at the
  preflight; a grant that fails for any reason but a missing role is
  `Unavailable`. The self-checks are:
  - `SELECT NOT pg_is_in_recovery()`. A replica, such as the Kubernetes
    `-ro` Service or a read-replica address, is a misconfiguration that
    waiting cannot fix: the process exits with code 1 at once, with a
    message naming it;
  - `pg_locks` holds exactly one granted advisory row with
    `pid = pg_backend_pid()` and `objsubid = 2`.

**The session is the fence.**

- A session-level lock ends only with an unlock or with the session.
- At backend exit, Postgres aborts any open transaction before it releases
  locks: `AbortOutOfAnyTransaction` runs before `LockReleaseAll` (postgres
  REL_18_STABLE `postinit.c`). So a new leader never overlaps an old
  session that can still commit.
- This holds as long as every chain-derived write uses the lock-holding
  session, nothing runs `DISCARD ALL` or `pg_advisory_unlock_all()` on it,
  and the writer is never pooled.

**`write` runs on a spawned task; `with_txn` is cancel-safe.**

```rust
pub async fn write<T: Send + 'static>(&self, budget: Budget, f: impl TxFn<T>) -> Result<T, DbError> {
    let this = self.clone();
    // A dropped caller only drops the JoinHandle: the transaction runs to completion.
    match tokio::spawn(async move { this.write_retrying(budget, f).await }).await {
        Ok(r) => r,
        Err(e) => std::panic::resume_unwind(e.into_panic()), // never cancelled: nothing aborts it
    }
}

// inside write_retrying, per attempt:
let mut slot = self.slot.lock().await;
let mut conn = match slot.take() { Some(c) => c, None => self.reacquire().await? };
let r = async {
    let mut tx = conn.begin().await?;                 // counted as a round trip; drop queues ROLLBACK
    if budget == Budget::Long { q::exec(&mut tx, "budget", "SET LOCAL statement_timeout = 0", ()).await?; }
    let v = f(&mut tx).await?;
    let seq: i64 = q::fetch_one(&mut tx, "seq", BUMP_WRITER_SEQ, (&self.run_id, now_ts())).await?;
    tx.commit().await?;                               // counted as a round trip
    Ok((v, seq))
};
let r = if budget == Budget::Long { select! { r = r => r, e = self.watchdog() => Err(e) } } else { r.await };
match &r {
    Ok((_, seq)) => { self.last_seq.fetch_max(*seq, Relaxed); *slot = Some(conn); }
    Err(DbError::Data(_)) => { *slot = Some(conn); }
    Err(_) => {}                                      // Unavailable: the session is dropped, the lock goes with it
}
```

- **`TxFn<T>`** is
  `Fn(&mut Transaction<'_, Postgres>) -> BoxFuture<'_, Result<T, DbError>> + Send + 'static`.
  It is `Fn` because a retry calls it again, and `'static` because it
  moves into the spawned task. So callers own their data, as
  `save_block_bundles` does with its `Arc`'d plan.
- **Writes are never cancelled from outside.** `write` hands the
  transaction to a spawned task, so a dropped caller cannot cut it short.
  This matters under `ROLE=all`, where a page view can call
  `save_token_metadata`.
- **An abort inside the writer drops the session.** If the attempt itself
  is aborted (the watchdog fires, the process exits), `conn` is dropped.
  The socket closes, the server rolls back and frees the lock, and the next
  write has to win `try_lock` again.
- **Why not a raw `BEGIN`/`COMMIT`.** The next `COMMIT` would also commit
  the half-finished batch.
- **`last_seq` moves only after a commit succeeds.**

**Budgets:**

| Budget  | Used for                                                                             | Limits                                                                                                                                                                                                              |
|---------|--------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Batch` | Normal writes                                                                        | Each statement gets a client timeout of `statement_timeout + 10 s` = 70 s, so the server cancels first and the error is clean                                                                                       |
| `Long`  | Migrations, repairs, rebuilds, `sync_holder_counts`                                  | `statement_timeout = 0` and no client timeout. A watchdog checks every 15 s, from the read pool and with a 5 s deadline, that `pg_locks` still shows the writer pid holding `K_WRITER`. Four misses cancel the work |

**Error classes.** `Unavailable` is the default; only `Data` reaches the
caller.

| Condition                                                                                                                                                                                                                                        | Class         | What happens                                                                                                                                                                                                                                   |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| SQLSTATE classes `22` and `23`, `21000` (cardinality violation), sqlx `Error::Encode`                                                                                                                                                            | `Data`        | Returned to the caller. These depend on a bundle's content, so `indexer.rs:1024-1036`'s per-bundle fallback isolates the bad bundle                                                                                                            |
| **Everything else:** `08*`, `57P0x`, `55P03`, `57014` (escalates once to `Long`), `25006`, classes `42`, `53`, `54`, `58`, `XX`, I/O errors, pool and client timeouts, and every other sqlx error (`Protocol`, `Tls`, `Decode`, `PoolClosed`, …) | `Unavailable` | `Writer::write` drops the session and retries the same idempotent batch, with backoff from 1 s to 30 s. It logs a warning with the SQLSTATE on each retry. It never returns this class to the caller                                           |

A database problem, such as a timeout, a full disk, a missing privilege or
a restart, therefore never drops blocks. Dropped blocks would be a
permanent hole, because backfill only walks below `MIN(number)`. A unit
test pins the classifier.

**Lease, orphans and dead tasks:**

- **Heartbeat.** The writer loop in `indexer.rs` calls
  `db::keepalive(&db).await` on a 10 s idle tick. `idle_session_timeout=30s`
  then kills a frozen leader's session and frees the lock.
- **Dead tasks.** `run_forever` selects over the writer, forward and
  backfill tasks. If any of them ends while the process is not shutting
  down, the process exits with code 5, so the pod restarts.
- **Dead network.** `tcp_user_timeout` and the keepalives catch it.
- **Orphaned statements.** `client_connection_check_interval` aborts a
  running statement whose client is gone. Stage 4a checks that it can be
  set per session on the chosen target:
  `SELECT context FROM pg_settings WHERE name = 'client_connection_check_interval'`.
- **An old backend still holding the lock.** On re-acquire, if `pg_locks`
  shows the previously recorded pid still holding `K_WRITER`, the writer
  runs `pg_terminate_backend(pid)` from the read pool before `try_lock`.
  The same role may terminate its own sessions.
- **Lost.** If `try_lock` keeps returning `false` for 120 s, the state is
  `Lost` and the process exits with code 3.

**Detecting a lost suffix with `writer_seq`.**

Several events can silently drop the newest commits: a restore,
`synchronous_commit=off` (allowed above), or, if HA is added later, a
failover onto an asynchronous replica (a common Kubernetes setup). The
forward loop keeps its cursor in memory (`indexer.rs:810`), so lost blocks
would become a permanent hole.

- **The bump.** `open_with` draws a random `run_id` for the process. Every
  writer transaction runs:

  ```sql
  INSERT INTO kv (key, value, updated_at) VALUES ('writer_seq', $1 || ':1', $2)
  ON CONFLICT (key) DO UPDATE
    SET value = $1 || ':' || (split_part(kv.value, ':', 2)::bigint + 1), updated_at = excluded.updated_at
  RETURNING split_part(value, ':', 2)::int8
  ```

- **No seed row.** The upsert creates the row on first use, so migration
  0001 seeds nothing, and the phase 2 round-trip test is unaffected.
- **The check.** After any Reacquiring → Leader transition, and before the
  retried batch, the writer reads the row on the new session. It exits with
  code 4 unless the stored run id is its own and the counter is `last` or
  `last + 1`; `last + 1` covers a COMMIT that applied but whose
  acknowledgement was lost.
- **First leadership needs no check.** A fresh process reads its cursors
  from the database.
- **What a restart recovers.** The forward loop resumes from the database's
  max block, and backfill from its min. An asynchronous loss always drops a
  suffix of the commit history, and each loop's newest commits sit at its
  own frontier. The genesis and anchoring watermarks live in the same
  transactions. So a restart re-derives everything that was lost, with no
  change to `indexer.rs`.
- **No effect on the fixtures.** `kv` is outside the baseline comparison
  (`NOT_INDEXED`).

**Indexer control reads never treat an error as "empty".**

- `backfill_loop` reads its frontier through `try_min_block_number`, at
  start (`indexer.rs:882`) and on each poll (`:900`). On `Err` it logs,
  sleeps `poll`, and keeps the current frontier.
- Otherwise, one failed read during an outage would turn the frontier into
  `None`, and backfill would re-walk the whole chain from the head.
- The startup reads, such as the forward cursor and the anchoring
  watermark, run after the preflight has proven the database reachable.

**Restarts and maintenance.** The target runs without HA (decision 2), so
there is no failover: a crash, a restart or maintenance keeps the database
down until it is back. On Cloud SQL Enterprise Plus, planned maintenance
costs under about 1 s even on a standalone instance. A single-instance
CloudNativePG cluster is down while its pod restarts or is rescheduled. In
each case:

- Every session dies, and the in-memory locks go with them.
- The writer waits in `Reacquiring`, and the 1024-entry bundle channel
  holds back the fetchers.
- Web reads fail within about 7 s and return 503.
- The follower resumes on its next poll.
- The interrupted batch is retried, so no block is lost.
- If `synchronous_commit` is off and the database crashed, rather than
  restarting cleanly, the newest commits can be gone. The re-acquire's
  `writer_seq` check then exits 4, the indexer pod restarts, and the
  restart re-derives them (above).

### What web replicas write

- **No writer in the web role.** A chain-derived write returns
  `DbError::NotWriter`. `token_page` (`web.rs:1567`) already renders the
  fetched descriptor when the save fails.
- **Who owns `token_metadata`:**
  - The indexer creates rows for tokens it sees in a transfer or as a fee
    token (`attach_token_metadata`, `indexer.rs:268-295`).
  - Its new **missing-metadata job** runs only under `ROLE=indexer`. At
    startup it runs `tokens_missing_metadata` as a full scan. A failed scan
    is not "none missing": it runs again, backing off from one pass to five
    minutes, until one reads. Besides the scan, each pass on the stats
    interval checks only the addresses that `take_tokens_without_metadata`
    returns: token addresses that bundles committed since the last pass
    referenced and that the token-label cache has no entry for (section 3).
    So once the scan has read, no pass rescans `transactions` (`fee_token`
    has no index). The job fetches the missing tokens and saves them through
    the writer. An address whose fetch fails stays in the job's own retry
    set and is tried again on the next pass. That covers fetches that failed
    during indexing, which a page view used to repair. Under `ROLE=all`,
    page views still repair them, as today.
  - `repair_token_metadata` only re-fetches existing rows
    (`indexer.rs:611-620`).
- **Split-mode divergence (accepted; see "Known limitations").** Under
  `ROLE=web`, a token with no transfer and no fee use gets no row from a
  page view. Each view renders
  it from one RPC fetch, the same cost as an unknown address today. It is
  not listed or searchable until its first transfer is indexed. Discovering
  such tokens from TIP-20 factory `TokenCreated` logs is the next task
  after this phase.
- **Web writes only true caches,** through the `cache` pool and
  `q::exec_best_effort`. Failures are logged and never set `DB_FAILED`:
  - `save_selector_names`, with keys sorted, so two replicas cannot
    deadlock;
  - `set_trace`, which the writer preserves through `COALESCE`
    (`db.rs:689`).
- **`ROLE=all` behaves as today.**

## 7. Postgres targets: Cloud SQL and Kubernetes

**Configuration.** `DATABASE_URL` selects Postgres and carries the host,
port and database. The connection is plaintext ("No TLS" below). In
production it carries no credentials: the user and the
password are the separate variables `PGUSER` and `PGPASSWORD`, so the
password can be injected into the pod from Secret Manager without any URL
holding it. `ROLE` picks the role. `DB_PATH` stays the SQLite setting, and
`DATABASE_URL` wins when it is set. The Fly app, Render
and the systemd unit set only `DB_PATH` (`fly.toml:14`, `render.yaml:16`,
`deploy/nvnmchain-explorer.service:12`), and a test checks that `DB_PATH`
alone still opens the file.

**Credentials: `PGUSER` and `PGPASSWORD`.**

- **Read explicitly.** `DbConfig::from_env` reads `PGUSER` and `PGPASSWORD`
  through an injectable lookup, so unit tests pass a fake environment
  instead of mutating the process's. It applies each one to the connect
  options only where the URL has no user or no password. sqlx's
  `PgConnectOptions::from_str` reads the same variables as its defaults
  (`sqlx-postgres-0.9.0/src/options/parse.rs:9-10`, `mod.rs:56-98`), so the
  two agree. It reads `PGSSLMODE` the same way, for the `sslmode` rule
  ("No TLS" below).
- **Precedence.** A user or password written into the URL wins. The
  manifests never put one there. Local and CI runs may, so
  `postgres://explorer:explorer@localhost:5432/explorer` (`AGENTS.md`)
  keeps working. When the URL carries a password and `PGPASSWORD` is also
  set, startup logs a warning that the URL's password was used.
- **Never logged.** The password lives only in a redacting newtype inside
  `DbConfig` and in sqlx's connect options. It never enters `Settings`, so
  the startup log and `Debug` output cannot show it.
- **Missing or wrong.** Without `PGUSER`, sqlx falls back to the process's
  OS user, so authentication fails rather than connecting as someone else.
  A missing or wrong password fails authentication too: the indexer stays
  unready and retries, and web pages return 503 (section 8).
- **Rotation.** A container's environment is fixed when it starts. Open
  sessions survive a password change, but new connections (pool growth, a
  writer reacquire) use the password the pod started with. So change the
  password in the database and in Secret Manager, wait for the synced
  Secret, then `kubectl rollout restart` both deployments, indexer first.
- **Tests.** Unit tests cover the precedence with a fake environment: a
  credential-free URL plus both variables, a URL password plus
  `PGPASSWORD`, and neither. One integration test connects to the CI server
  with a credential-free `PG_TEST_URL` and the variables set.

**Telling a Postgres URL from a SQLite path.** One function,
`DbTarget::parse`, classifies the value of `DATABASE_URL`. `DB_PATH` is
always a file path and is never classified, and so is the argument of
`db::open`, which opens SQLite only; tests open Postgres through `open_with`
and a `DbConfig`.

- **`postgres://…` or `postgresql://…`** is Postgres, as given.
- **No scheme, in the form `[user[:password]@]host:port[/dbname][?params]`,**
  is Postgres too, and is normalized by prefixing `postgres://`. The host is
  a DNS name, an IPv4 address or a bracketed IPv6 address, and the port is
  1 to 5 digits. Examples: `localhost:5432`, `127.0.0.1:5432/explorer`,
  `[::1]:5432` and `explorer:explorer@localhost:5432/explorer`.
- **Anything else is a SQLite path:** `explorer.db`, `/data/explorer.db`,
  `:memory:`, `C:\data\explorer.db`, `localhost` (no port) and
  `file:x.db`.
- **Missing parts come from the environment.** The user is `PGUSER`, the
  password is `PGPASSWORD` (see "Credentials" above), and the `sslmode` is
  `PGSSLMODE` (see "No TLS" below). The database is
  `PGDATABASE` or the server's default
  (`sqlx-postgres-0.9.0/src/options/mod.rs:56-98`). So `localhost:5432`
  against docker-compose needs `PGUSER`, `PGPASSWORD` and `PGDATABASE`, or
  the credentials written into the string.
- **No silent fallback.** A string classified as Postgres that fails to
  parse is an error, never a SQLite file. A `DATABASE_URL` that classifies
  as a SQLite path is refused with "use `DB_PATH` for SQLite".
- **The `sslmode` rule still applies.** `localhost:5432` and
  `db.internal:5432` both connect in plaintext, and
  `db.internal:5432?sslmode=verify-full` is refused.
- **Tests and stage.** A unit test runs every example above in both
  directions, and `DbUrl` redacts the password in either form. The
  classifier lands in stage 3, with `open`'s Postgres arm.

|                           | Cloud SQL for PostgreSQL                                                                                                                                                                                                             | Postgres in Kubernetes (e.g. CloudNativePG)                                                                                                                                                                                                                                                                                                                                     |
|---------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Endpoint                  | The instance's PSA DNS name (from `dnsNames`, `…sql-psa.goog`), port 5432. Cloud SQL creates no record for PSA, so a Cloud DNS private-zone record maps that name to the private IP                                                  | The primary's read-write Service, `<cluster>-rw.<ns>.svc`, port 5432                                                                                                                                                                                                                                                                                                            |
| Not usable for the writer | Managed Connection Pooling and the Auth Proxy. Transaction mode forbids session locks, and through a proxy the keepalives cannot see a dead client                                                                                   | A PgBouncer `Pooler`, for the same reason; the `-ro` and `-r` Services. With `instances: 1`, `-ro` has no endpoints, so the connection fails and is retried as `Unavailable`, and `-r` reaches the primary only because it is the sole instance. Once replicas exist, the recovery self-check refuses a replica reached through either. `deploy/k8s/README.md` names `-rw` only |
| TLS                       | None: plaintext over the private IP only. Leave "Allow only SSL connections" off                                                                                                                                                     | None: plaintext over the ClusterIP Service                                                                                                                                                                                                                                                                                                                                      |
| Auth                      | Password users. `PGPASSWORD` comes from Secret Manager through a Kubernetes Secret synced from it (for example by External Secrets Operator), referenced with `secretKeyRef`. IAM can come later through `Pool::set_connect_options` | The operator-generated Secret: `PGUSER` and `PGPASSWORD` from its `username` and `password` keys                                                                                                                                                                                                                                                                                |
| HA                        | Not used (decision 2): a standalone instance. HA, if added later, fails over in about 60 s with no data loss, through the same DNS name                                                                                              | Not used (decision 2): `instances: 1`, so there is no replica to promote. Asynchronous replicas added later can lose the last commits on failover, which `writer_seq` catches                                                                                                                                                                                                   |
| `max_connections`         | Set by machine size (e.g. 500 at 15 GB)                                                                                                                                                                                              | The operator's setting (often 100)                                                                                                                                                                                                                                                                                                                                              |

**No TLS** (decision 11).

- **Plaintext only.** sqlx is built with `runtime-tokio` and `postgres`
  and no TLS feature, so every connection is plaintext. The database must
  be reachable only over a private network: Cloud SQL's private IP, or a
  ClusterIP Service.
- **The `sslmode` rule.** `DbConfig::pg_options` refuses at start any
  `sslmode` but `disable`, rather than quietly downgrading it. `prefer`
  and `allow` are refused too: they ask for TLS where the server offers
  it, and without a TLS feature sqlx connects them in plaintext without a
  word (`sqlx-postgres-0.9.0/src/connection/tls.rs:24-30`). The mode is
  the URL's (`sslmode` or `ssl-mode`, the last of several, as sqlx reads
  it), or else `PGSSLMODE`. Naming none is plaintext, whatever the host.
- **Test.** `connections_are_plaintext_and_a_url_asking_for_tls_is_refused`
  (`src/db/config.rs`, no network). No `sslmode`, or `disable` in any
  letter case, gets plaintext options, for a TCP host or a Unix socket.
  `allow`, `prefer`, `ssl-mode=prefer`, `sslmode=disable&sslmode=prefer`,
  `require`, `verify-ca` and `verify-full` are refused, and so is
  `PGSSLMODE=prefer` or `require` unless the URL says `disable`.
- **Adding TLS later** needs a sqlx TLS feature, a `pg_options` that
  accepts `verify-full` (today it refuses every mode but `disable` and
  sets `PgSslMode::Disable`), `sslmode=verify-full` and a CA bundle in the
  pods. Nothing weaker verifies the server in
  sqlx 0.9: `verify-ca` adds `sslrootcert` to the bundled public web roots
  and then accepts a certificate issued for any name
  (`sqlx-core-0.9.0/src/net/tls/tls_rustls.rs:139-160`), and `require`
  verifies nothing (`sqlx-postgres-0.9.0/src/connection/tls.rs:47-51`).

**Version floor: PostgreSQL 15, checked at open**
(`PgConnection::server_version_num()`, with no extra query).

- The design needs 14 or later, for `idle_session_timeout` and
  `client_connection_check_interval`. 14 reaches end of life in November
  2026, so 15 is the oldest supported major.
- CI runs 18 on every PR and 15 nightly.
- Cloud SQL defaults to 18.

**Database users and grants**

- **`explorer_indexer`** owns the schema and runs migrations.
- **`explorer_web`** (configurable as `DB_WEB_ROLE`) gets its rights two
  ways:
  - `ALTER DEFAULT PRIVILEGES IN SCHEMA <s> GRANT SELECT ON TABLES TO <web_role>`.
    With no `FOR ROLE`, it applies to tables the current user creates, so
    no role name is hard-coded;
  - re-applied by the runner after migrations on every start:
    `GRANT USAGE ON SCHEMA <s>`,
    `GRANT SELECT ON ALL TABLES IN SCHEMA <s>`,
    `GRANT INSERT, UPDATE ON selector_names` and
    `GRANT UPDATE (trace_data) ON transactions`.
- **A missing web role is not an error.** docker-compose and CI create only
  `explorer`. When `DB_WEB_ROLE` names no row in `pg_roles`, the runner logs
  a warning and skips the web grants. Otherwise every `TEST_DB=postgres`
  open would fail with `42704`, which is classed `Unavailable` and retried
  forever.
- **A test** creates the web role, connects as it, and checks that
  `save_selector_names` and `set_trace` succeed, and that an insert into
  `blocks` fails with `42501`.

**Credentials in logs.** A `DbUrl` newtype redacts any password written
into a URL, in `Display` and `Debug`. That covers `main.rs`'s startup log
(`main.rs:24-27`), the `Debug` output of `Settings`, and error contexts.
`PGPASSWORD` never enters `Settings` at all (see "Credentials" above).

## 8. Roles and the split deployment

| `ROLE`          | Runs                                                                                                                                                                                           | Allowed on SQLite          |
|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------|
| `all` (default) | Today's process: web, the indexer and the in-process broadcast                                                                                                                                 | Yes; the only role allowed |
| `indexer`       | Preflight → candidate → writer; migrations; the forward, backfill, writer, stats, genesis, repair, anchoring and missing-metadata loops. HTTP serves only `/healthz`, `/readyz` and `/metrics` | No                         |
| `web`           | Pages, SSE, the follower and cache writes. It never takes the lock                                                                                                                             | No                         |

One image runs as either deployment. Only the environment differs: `ROLE`,
`DATABASE_URL` and `PGUSER` as plain values, and `PGPASSWORD` from that
deployment's Secret. Each deployment has its own database user.

**Where each role is supported.** Production runs `ROLE=indexer` and
`ROLE=web` on Postgres. `ROLE=all` is supported for development, CI and
small self-hosting, on SQLite or Postgres. Under `ROLE=all` on Postgres, a
token page opened while the database is down hangs until the database is
back, plus up to 30 s of writer backoff, and then returns 503 anyway. Its
first read already set `DB_FAILED`, and its metadata save waits on the
writer (section 6). Each such view leaves one queued write that still runs
after the client leaves. That is accepted at this scale, and
`docs/database.md` says so.

```mermaid
flowchart LR
    users(["Users"]) --> ing["Ingress / Service"]
    ing --> web1["explorer-web pod<br/>ROLE=web"]
    ing --> web2["explorer-web pod<br/>ROLE=web"]
    rpc(["Chain RPC / WS"]) --> idx["explorer-indexer pod<br/>ROLE=indexer, replicas: 1"]
    subgraph pg["Postgres primary (Cloud SQL private IP, or the k8s -rw Service)"]
        lock[["session advisory lock<br/>(K1, K_WRITER)"]]
        tables[("tables + schema_migrations")]
    end
    idx -- "writer session: holds the lock,<br/>migrates, writes" --> pg
    idx -- "read pool" --> pg
    web1 -- "read pool (+ polling follower)<br/>and cache pool" --> pg
    web2 -- "read pool (+ polling follower)<br/>and cache pool" --> pg
```

### Process behaviour, the same on every platform

- **Bind first.** `main.rs` binds the port and serves `/healthz` before
  `open_with`.
- **Indexer startup.** `open_with` retries its connection and the preflight
  with backoff while the pod stays unready. A missing host or bad
  credentials never leave it Ready. A preflight refusal (section 5) exits
  non-zero, so a broken image never replaces a working leader.
- **Web startup.** `open_with` builds its pools lazily and never fails
  because Postgres is unreachable. The follower evaluates the schema gate
  from its first successful tick.
- **`/readyz`.** It reports
  `{role, schema: {db, binary}, writer, preflight}`, plus `sync`
  on the indexer (section 10), from a status `watch` created before
  `open_with`. It **never acquires a pool connection**. It returns 200
  when:
  - **indexer:** the preflight passed, whether the writer is a candidate,
    leader or reacquiring. Sync progress never affects readiness;
  - **web:** the schema gate (D ≥ B) passed.

  Database health shows in the JSON body and in metrics. Pool exhaustion
  and outages are handled by the 503 middleware, not by readiness.
- **Shutdown.**
  - On SIGINT or SIGTERM (Fly's default kill signal is SIGINT;
    `main.rs:131-146` handles both today), the process stops accepting,
    gives the indexer at most
    3 s, then calls `std::process::exit`. Today's sequence can take
    5 s + 10 s (`main.rs:118`, `:112`), and dropping the runtime waits on
    blocking tasks forever.
  - The dropped writer session frees the lock, and the batch replays
    idempotently.
  - Web relies on the manifest's `preStop` delay, because axum stops
    accepting the moment SIGTERM arrives.
- **Exit codes:**
  - 3: leadership lost;
  - 4: the database is not at this process's `writer_seq`;
  - 5: an indexer core task ended;
  - 1: the preflight refused.

### Kubernetes manifests (`deploy/k8s/`)

|             | `explorer-indexer`                                                                                                                                                                                                     | `explorer-web`                                                                                                                                                                                                                                                                   |
|-------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Kind        | Deployment, `replicas: 1`, RollingUpdate with `maxSurge: 1` and `maxUnavailable: 0`. The new pod becomes Ready after its preflight and waits as a candidate. Only then is the old pod terminated, which frees the lock | Deployment, Service and HorizontalPodAutoscaler; RollingUpdate with `maxUnavailable: 0`                                                                                                                                                                                          |
| Env         | `ROLE=indexer`; `DATABASE_URL` with no credentials and `PGUSER=explorer_indexer` as plain env; `PGPASSWORD` from the indexer Secret (`secretKeyRef`)                                                                   | `ROLE=web`; `DATABASE_URL` with no credentials and `PGUSER=explorer_web` as plain env; `PGPASSWORD` from the web Secret (`secretKeyRef`)                                                                                                                                         |
| Probes      | Startup and liveness: `/healthz`, with a generous startup budget. Readiness: `/readyz` (preflight passed). **Never gate on the lock**, or every deploy that carries a migration deadlocks                              | Startup and liveness: `/healthz`, with no DB check, so a database outage doesn't restart every replica. Readiness: `/readyz` (schema gate), `periodSeconds: 5`, `failureThreshold: 3`                                                                                            |
| Termination | `terminationGracePeriodSeconds: 30`                                                                                                                                                                                    | `lifecycle.preStop.sleep.seconds: 10`, so endpoints are removed before the drain starts; `terminationGracePeriodSeconds: 30`                                                                                                                                                     |
| Connections | About 6 (12 during a rollout)                                                                                                                                                                                          | About 10 per pod (read 8, cache 2). Cap the HPA maximum so that `pods × 10 + 12` stays within `max_connections`                                                                                                                                                                  |
| Alerts      | From `/metrics`. **Fetching behind:** `explorer_tip_lag_blocks > M` for T minutes. **Not leader:** `explorer_writer_state` is not 1 for 2 min. **No heartbeat:** `time() - explorer_writer_last_ok_seconds > 30`       | From `/metrics`. **Stale:** `time() - explorer_latest_block_timestamp_seconds > X` for T minutes. It still fires when the writer or the RPC node stalls, because the chain makes a block about every 0.48 s, empty or not. **503 rate:** `rate(explorer_http_503_total[5m]) > R` |

**Also shipped:**

- `secrets.example.yaml`, holding only each deployment's `PGPASSWORD`.
  The URL and the user are not secret;
- `deploy/k8s/README.md`, with the deploy order (indexer first) and a
  Cloud Run note. On Cloud Run, the same image runs with min = max = 1
  indexer instances and instance-based billing. The web startup probe there
  must be `/readyz`, because Cloud Run's startup probe gates revision
  traffic.

### Metrics

`src/metrics.rs` serves a Prometheus `/metrics` endpoint, through the
`metrics` and `metrics-exporter-prometheus` crates. It is for in-cluster
scraping (Google Managed Prometheus `PodMonitoring`, or any Prometheus);
the Ingress does not route it. It is served only under `ROLE=web` and
`ROLE=indexer`: `ROLE=all` runs on hosts with no Ingress (Fly, Render,
systemd), where it would be public. The alerts above use these series:

| Series                                    | Role    | Meaning                                                      |
|-------------------------------------------|---------|--------------------------------------------------------------|
| `explorer_writer_state`                   | indexer | 0 candidate, 1 leader, 2 reacquiring                         |
| `explorer_writer_last_ok_seconds`         | indexer | Unix time of the last committed write or keepalive           |
| `explorer_tip_lag_blocks`                 | indexer | How far the forward loop is behind the chain head            |
| `explorer_schema_version{of=…}`           | both    | D and B, one series each (`db`, `binary`)                    |
| `explorer_latest_block_timestamp_seconds` | web     | Timestamp of the newest block the follower has seen          |
| `explorer_http_503_total`                 | web     | Responses the 503 middleware produced                        |
| `explorer_http_request_duration_seconds`  | web     | Page latency by route, so p95 stays visible after the switch |

### Live feed: a polling follower

Under `ROLE=web`, `src/follow.rs` runs one task on the read pool:

- **Interval:** `FOLLOW_POLL_MS`, default 500 ms. The chain produces a
  block about every 0.48 s.
- **One statement per tick:**
  `SELECT (SELECT MAX(number) FROM blocks), (SELECT updated_at FROM kv WHERE key = 'stats'), (SELECT MAX(version) FROM schema_migrations)`.
- **Start.** The first tick that sees a block starts at the tip and
  reports its time, for the staleness alert; history is not replayed. Until
  the tip's row reads, the start stays unset. On an empty table it waits
  for a block: backfill fills a fresh database from the head down.
- **New blocks.** When the max is above `last`, the follower calls
  `get_blocks_in_range` and `get_transactions_in_range(last + 1, n)`,
  capped at 256 blocks, and broadcasts on the existing channel. Values at
  or below `last` are ignored, the same rule as `sse_step`. A block goes
  out only with its transactions: when either read fails, nothing is sent
  and `last` stays. A block the indexer never wrote (a bundle refused for
  its content) is stepped over, since blocks above the tip commit in order
  and it will not turn up later.
- **Stats.** When the stats `updated_at` changes, the follower reads
  `kv['stats']`, updates the stats cell and broadcasts `{"type":"stats"}`.
  Without this, split mode would lose live stats, which `stats_loop` sends
  only in its own process (`indexer.rs:1105-1111`).
- **Labels.** Every 30 s, on its own timer, the follower reloads the
  token-label cache from `try_all_token_metas`, merging (section 3).
- **Schema gate.** When `MAX(version)` changes, the follower re-evaluates
  the gate and updates the status watch.
- **Errors.** A DB error is logged and retried on the next tick. SSE
  streams stay open throughout.
- **`ROLE=all`.** The in-process broadcast still fires before commit
  (`indexer.rs:1001-1003`), as today.
- **LISTEN/NOTIFY is deferred.** It would need a dedicated connection, a
  notify in every batch and half-open handling. Revisit it only if
  commit-to-browser latency at the chosen poll interval measures
  unacceptable.

### When Postgres is down

- **Which errors set `DB_FAILED`.** The `q` read helpers set a
  `task_local! DB_FAILED` on every sqlx error except `RowNotFound` and
  per-row decode errors. Decode errors keep SQLite's "dropped N undecodable
  row(s)" rule.
- **Cache writes never set it.**
- **What the client sees.** Middleware turns a flagged response into
  **503 with `Retry-After: 30`**, never a false 404.
- **Everything else waits.** SSE streams stay open, the follower keeps
  polling, and the indexer waits in `Reacquiring`.

## 9. Tests

### Comparing baseline data on Postgres

1. **Fixture replay** (`tests/replay.rs`; network-free, every PR):
   1. Rebuild the `BlockBundle`s from each `fixtures/baseline/canary-*.db`.
      Addresses go through `checksum_address`.
   2. Write them with `save_block_bundles`, in chunks of 64, into **both**
      backends.
   3. Seed `genesis_balances`, `selector_names` and the kv watermarks from
      the fixture. Attach each token's metadata to the bundle of its first
      transfer.
   4. Run `repair_derived_tables`.
   5. Compare every bundle-derived table, plus `token_balances` and
      `holder_count`, with the fixture under `SPECS`.

   A second pass empties `token_balances` and runs `repair_derived_tables`
   again, so the Postgres rebuild (with its Rust checksumming) must
   reproduce the same rows and key spelling.
2. **Live re-index into Postgres.**
   - `tests/baseline.rs` runs under `TEST_DB=postgres`; its `open_db`
     (`tests/baseline.rs:71-73`) is already the single switch.
   - It re-indexes the canary ranges from the RPC into Postgres and
     compares the result with the same fixtures. `pg_rows` moves from
     `tests/postgres.rs:914` into `tests/common/baseline.rs`.
   - When the set-based writer ships, it enforces a budget of **at most 14
     round trips per `save_block_bundles` batch**, counted in `q`.
     `with_txn` counts BEGIN and COMMIT. With the per-row writer, it records
     the round trips per block instead.
3. **Cross-backend differential** (`tests/differential.rs`).
   - The same bundles go into both backends, shuffled, duplicated and split
     into batches differently.
   - Every table is compared between SQLite and Postgres. The comparison
     skips `id`, `created_at`, `updated_at`, and the kv keys
     `genesis_balances_cursor`, `stats`, `chain_head` and `writer_seq`.
   - It skips `sqlite_sequence`, which has no Postgres twin, and compares
     `schema_migrations` on `version` and `name` only. Its checksums and
     `applied_*` columns differ between backends by design.

### Holding the two backends together

**`TEST_DB=sqlite|postgres` matrix**

- **Picking the backend.** `tests/common/backend.rs::temp_db(name).await`
  does it. With `postgres` and no `PG_TEST_URL` it panics, so a skipped run
  can never pass.
- **Isolation.** Each Postgres test gets a schema `t_<pid>_<seq>` (63 bytes
  or fewer), set through `?options[search_path]=`
  (`sqlx-postgres-0.9.0/src/options/parse.rs:296-304`). It is dropped on
  pass, and a sweep removes leftover `t_%` schemas. Test pools are capped
  at 2, and CI starts Postgres with `-c max_connections=300`.
- **Suites.** decoder, pages and anchoring.
- **`indexer_jobs` stays SQLite-only.** Its unit tests call the SQLite
  module directly, and acceptance 3 keeps that file unchanged.
  `tests/jobs.rs` covers the same jobs (`save_anchoring_window`,
  `repair_derived_tables`, `compute_and_store_stats`) on both backends
  through the public API.
- **Rollout.** The three `temp_db` helpers (`decoder.rs:13`, `pages.rs:27`,
  `live_rpc.rs:19`) switch to the shared one in stage 1, SQLite only, and
  gain Postgres in stage 4c.
- **SQLite-only tests.** The `db::lock` tests in decoder open SQLite with
  `db::open` and a file path, and the tests that call `init_db` directly
  stay on SQLite too, so the `TEST_DB=postgres` job runs every test in its
  suites.
- **Backend-neutral rebuild.** The rebuild step of
  `pages.rs::a_genesis_balance_counts_once` gets a backend-neutral copy.

**API parity grid and coverage gate**

- **What runs.** Every `db_fn!` entry and hand-written wrapper runs on
  both backends, and the `db-coverage` feature fails if any name never
  ran, writes included. The macro and each hand-written wrapper call
  `coverage::hit` with their own name before dispatching (section 3), so
  the SQLite arm, which never goes through `q`, records too.
- **Data sets.** The fixtures, plus `pages.rs`'s `serve()` data: 30 tied
  holders across a 25-row page boundary.
- **Comparison rules:**
  - exact, where results are ordered by a unique key;
  - sorted, for the functions with no `ORDER BY` (`get_address_holdings`,
    `get_all_token_metas`, `try_all_token_metas`, `get_all_token_addresses`,
    `tokens_missing_metadata`);
  - **tie-aware across all pages** for `get_token_holders`,
    `get_all_tokens` and `search_tokens`. Each page's sequence of sort keys
    must be equal. Rows inside a run of equal keys compare as multisets.
    The runs at page boundaries must be subsets of the unpaged group. The
    union of the pages must equal the full set;
  - floats in stats compare with a 1e-9 relative tolerance.
- **Inputs** include non-ASCII terms, `%`, `_`, `\` and a NUL byte. Postgres
  `TEXT` rejects NUL, so the Postgres cache writes strip it.

**Planner property test** (`src/db/pg/plan.rs`, in `ci.yml`, no Postgres
needed; only when the set-based writer ships). Random transfer sequences run through the SQLite per-row writer,
which serves as the reference, and through `BatchPlan`. Balances and
`holder_count` must be equal.

**Classifier unit test.** `22xxx`, `23xxx`, `21000` and `Encode` are
`Data`. `42501`, `42P01`, `53100`, `25006`, `XX000`, `57014` and
`Protocol` are `Unavailable`.

**Postgres SQL lint** over `src/db/pg/**`:

- no `?N`, `OR IGNORE`, `NOCASE` or `GLOB`;
- `ORDER BY` only on `NOT NULL` keys, or with an explicit `NULLS` order;
- upsert targets are qualified.

**Migrations** (`tests/migrations.rs`, plus unit tests in
`sqlite/migrate.rs`):

- pins A and B, and the commute guard for every N;
- constant equality: `block_cols!` against `BLOCK_COLS`, and the same for
  the other column lists;
- both canaries adopted with their rows intact;
- checksum refusal, and D > B refusal by the preflight before `try_lock`;
- per-version shape parity, and a per-version data upgrade;
- the list test, and the web gate (D ≥ B);
- an unversioned Postgres schema refused;
- the web role's grants.

**Locks and outages** (`tests/locks.rs`):

- A second writer stays a candidate. The lock is free after close.
- `pg_terminate_backend` mid-batch, then re-acquire and retry: no block is
  lost.
- **A dropped caller does not abort its batch.** The batch commits, and the
  writer pid is unchanged.
- An `idle_session_timeout=2s` lease expires a paused holder.
- Two indexers start together, and exactly one migrates.
- The `pg_locks` and `pg_is_in_recovery` self-checks work.
- **`writer_seq`:**
  - Writer A commits; its session is killed and its last batch deleted;
    writer B takes the lock and commits past A's counter. A must exit 4 on
    re-acquire.
  - A suffix deleted behind the writer's back also gives exit 4.
- **Errors never drop blocks.** `53100` (from a trigger raising `disk_full`
  while a flag row exists) and `25006` (read-only role) are injected
  mid-replay. The final compare must equal the baseline.
- **Role defaults don't leak into pools.** A role default
  `idle_session_timeout=2s` leaves read-pool and cache-pool connections
  idle for 3 s, and both still answer.
- **No re-walk after an outage.** With backfill complete, the read-pool
  backends are terminated. The backfill must issue no fetch above the
  existing frontier.

**Outage drills**

- Restart the Postgres container mid-replay (`tests/restart_drill.rs`,
  stage 6). The final compare must equal the baseline.
- A blackhole TCP proxy (about 80 lines) covers half-open sessions:
  - a web read gets a 503 within about 7 s, and the pool recovers;
  - the watchdog cancels a `Long` statement within 4 × (15 s + 5 s).

**Follower**

- An advancing poll gives a gapless, ordered broadcast.
- Backfill below `last` is not broadcast.
- A DB error followed by recovery resumes with no gap.
- Stats changes are rebroadcast.

**Label cache.** A pages test calls `build_tera` and saves three tokens:
one with `save_token_metadata`, one in a `save_block_bundle` and one in a
`save_block_bundles` batch. It checks that all three labels render, and
that a token whose symbol and name are both empty renders no empty
label.

**Cache writes.** A failed cache write still returns 200, not 503.

### CI

- **`ci.yml`** keeps its jobs: fmt, clippy, the network-free suite and the
  live suite. It gains:
  - the planner property test (set-based writer only), the classifier test
    and the SQLite migration unit tests, none of which need a server;
  - the "merged migrations are immutable" step.
- **`postgres.yml`** runs three parallel jobs against `postgres:18.x`:
  1. the `TEST_DB=postgres` network-free suites;
  2. replay, differential, grid, migrations, locks and outages;
  3. the live baseline re-index into Postgres (about 105 s).
- **Nightly:** the same against `postgres:15.x`.
- **`Cargo.toml`** gains `rust-version = "1.94"`, because sqlx 0.9 requires
  it (`sqlx-core-0.9.0/Cargo.toml:14`). sqlx is pinned to `=0.9.0`, with
  only `runtime-tokio` and `postgres` (decision 11). From stage 1 it also
  declares `db-coverage = []` under `[features]`, so the coverage `cfg` is
  known and `unexpected_cfgs` stays quiet under `-D warnings`.

## 10. Rollout

Each stage is a PR that merges with CI green.

| Stage             | Contents                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Behaviour                                                                                                                                                                                                                                                                          |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 0 (upstream PRs)  | `block.html` links the parent through `parent_block` (the block below, when its hash is the parent hash) instead of `get_block_url`. `resolve_search` and the option-combinator closures become `if let`. The `let _ =` writes are logged (`web.rs:1567`, `tests/live_rpc.rs:152`). The shutdown budget                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | Preserved, except shutdown: on SIGINT or SIGTERM the process exits within 3 s                                                                                                                                                                                                      |
| 1 (coordinated)   | The `git mv` and section 4's stage-1 edits. `db/mod.rs` with `Backend::Sqlite` only, and the token-label cache with the three write wrappers that keep it current. `sqlite/extra.rs` starts here, with `try_all_token_metas` for the cache's seed. `Cargo.toml` declares `db-coverage = []`, and `db_fn!` and the hand-written wrappers carry their `coverage::hit` lines from here on; the gate that checks them lands in 4c. The cache replaces Tera's `address_label` lookups because Tera functions are sync. `.await` across `web.rs`, `indexer.rs`, `signatures.rs`, `main.rs` and every test that calls the API, `baseline.rs` included; the sync test hooks `init_db` (`tests/postgres.rs:755`) and `db::lock` (`write_scale.rs:75`) stay as they are. The shared `temp_db` helper. A seal test forbidding `block_in_place` and `Handle::block_on` | Identical; the baseline must match                                                                                                                                                                                                                                                 |
| 2 (coordinated)   | `migrations.rs`, `sqlite/migrate.rs`, and `try_min_block_number` and `tokens_missing_metadata` in `sqlite/extra.rs`, and section 4's stage-2 edits. The pins, the commute guard and the immutability CI step. An org admin makes the header test a required check on NVNM-Chain's `main`, with branches up to date before merging. `schema_pg.sql` → `migrations/postgres/0001_baseline.sql` (`holder_addr` added to `idx_tb_holding`), with `tests/postgres.rs` updated. `schema_migrations` joins `NOT_INDEXED` in `tests/baseline.rs`. `Dockerfile`: `COPY migrations ./migrations` before `cargo build`, since `migrations.rs` reads them with `include_str!`. sqlx promoted to `[dependencies]`; `rust-version`. `ROLE` parsing (`all` only), `/healthz`, and `/readyz` with the web schema gate                                                      | Files are adopted and stamped v1                                                                                                                                                                                                                                                   |
| 3                 | `src/db/pg/`: pools, plaintext connections (decision 11), the version floor, session settings, client deadlines, `q`, and every read but `holders_without_genesis_balance`. `config.rs`: `DATABASE_URL` (a bare `host:port` counts), `PGUSER`, `PGPASSWORD` and `PGSSLMODE`, the `sslmode` rule, and `DbUrl`, which never shows a password. `track_failures` and `DB_FAILED`. The cache writes (`set_trace`, `save_selector_names`) through `exec_best_effort` on the cache pool. The chain writes and `holders_without_genesis_balance` are stubs, most of which return an error. `db::open` and `open_with` accept Postgres, for the tests, and `main.rs` refuses `DATABASE_URL` and any `ROLE` but `all`. A test holds the Postgres column lists equal to SQLite's, and a lint keeps SQLite spellings out of the Postgres SQL                           | Not selectable in production                                                                                                                                                                                                                                                       |
| 4a                | The writer: preflight, candidate, self-checks, spawned and cancel-safe `write`, budgets, watchdog, error classes, `keepalive`, `writer_seq`, and exit codes 3 (another session holds the lock) and 4 (commits were lost). The Postgres runner and grants. Simple writes: blocks, transactions, token metadata, `kv` and the chain head. Lock and classifier tests, the Postgres migration refusals and grants, and the outage drills in `tests/outage_drills.rs`                                                                                                                                                                                                                                                                                                                                                                                           | —                                                                                                                                                                                                                                                                                  |
| 4b                | `plan.rs` and its property test. Set-based `save_block_bundle(s)`. Two-pass anchoring. Genesis balances, and the derived-table repair and rebuild (on the `Long` budget). Replay and differential, the live re-index into Postgres (`TEST_DB=postgres`), and the dropped-caller lock test. **Writer gate:** equal tables on `canary-rich`, within 14 round trips per batch (section 9). It was met, so the set-based writer shipped; the fallback was a per-row Postgres writer (a port of `write_block`, `db.rs:526-560`, inside `Writer::write`)                                                                                                                                                                                                                                                                                                         | —                                                                                                                                                                                                                                                                                  |
| 4c                | The home-page stats. `try_min_block_number` in `backfill_loop`. The missing-metadata job, with `take_tokens_without_metadata` and the bundle wrappers' noting; both stay idle until `ROLE=indexer` exists (stage 5). The parity grid (`tests/grid.rs`) and its coverage gate, the `indexer_pg` suite, and the `TEST_DB=postgres` job. Postgres becomes selectable                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | —                                                                                                                                                                                                                                                                                  |
| 5                 | `ROLE=web` and `ROLE=indexer`. The follower, which also re-reads the schema version for the web schema gate. The 503 middleware. In `indexer.rs`: the writer-loop `keepalive`, and `supervise`, after which `main` exits with code 5 when the writer, forward or backfill task ends. Bind-before-open and lock-free `/readyz`, with the indexer's `sync` status. `src/metrics.rs` and `/metrics`                                                                                                                                                                                                                                                                                                                                                                                                                                                           | `ROLE=all`: binds before open, and pages answer 503 until the database has opened. It exits with code 5 when the writer, forward or backfill task ends (today it keeps serving). On Postgres, a request whose reads failed answers 503 with `Retry-After: 30`. Otherwise unchanged |
| 6                 | `deploy/k8s/`. The runbook, with the cutover checklist. `docs/database.md`. In the Postgres workflow: the live Postgres baseline, the restart drill (`tests/restart_drill.rs`), and a nightly run of every suite on PG 15                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | —                                                                                                                                                                                                                                                                                  |
| 7                 | `.env`, for local development: `main` reads `ENV_FILE`, or `.env`, at startup; a variable already set wins. `.env.example` with local values                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Both backends: `.env` or `ENV_FILE` is read at startup; a line that does not parse is logged, and the file is read no further                                                                                                                                                      |
| 8                 | `README.md`, after stages 0–7 have merged, so it describes the code that shipped (checklist below)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | Docs only                                                                                                                                                                                                                                                                          |
| 9 (on a p95 miss) | `tokio::join!` of independent Postgres page queries                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | SQLite unchanged                                                                                                                                                                                                                                                                   |

**Coordinating with teammates**

- **Stage 1.** Announce a one-day merge window and land stage 0 first.
  Then regenerate the PR on the latest `main` with the codemod from the
  merge probe, rather than rebasing by hand. In-flight PRs then fail to
  compile at each missing `.await`, and each fix is one token.
- **Stage 2.** Announce that `init_db` is frozen, and that migrations
  merge into NVNM-Chain's `main` first, which assigns their numbers
  (section 5). Pins A and B point to `docs/database.md`. In-flight PRs that
  edit `init_db` are rewritten as migration pairs.

**Stage 8: `README.md`**

The last task, once every code stage has merged, so the README describes
what shipped rather than what was planned. It rewrites the sections that
phase 3 makes stale:

- **Intro** (`README.md:12-15`): "rusqlite (schema created on boot)"
  becomes SQLite or Postgres (sqlx 0.9), with versioned migrations.
- **Deploying to the cloud** (`README.md:31-75`): the single-process
  options (Fly, Railway, Render, a VPS) stay, as `ROLE=all` on SQLite. Add
  the split Kubernetes deployment on Postgres (Cloud SQL or CloudNativePG),
  pointing at `deploy/k8s/README.md` and the deploy order (indexer first).
- **Persistence & schema migrations** (`README.md:77-92`): replace the
  idempotent-DDL text with versioned migrations. On SQLite, migration 1 is
  the frozen `init_db`, existing files are adopted and stamped v1, a
  database newer than the binary (D > B) is refused, and the only way back
  is forward. Link `docs/database.md`.
- **Indexer** (`README.md:110-127` and `207-225`): one writer task writes
  to either backend. On Postgres (`ROLE=indexer`, or `ROLE=all` with
  `DATABASE_URL`) it holds the session advisory lock. Replace "Serialized
  SQLite writes ... ~200 blocks/s" with both backends' write paths. The
  Postgres re-index rate (section 12, item 7) is added once it has been
  measured on the chosen target.
- **Configuration** (`README.md:129-145`): add `DATABASE_URL`, `PGUSER`,
  `PGPASSWORD`, `PGSSLMODE`, `ROLE`, `DB_WEB_ROLE`, `FOLLOW_POLL_MS` and
  stage 7's `ENV_FILE`, plus the existing `DB_CACHE_KIB` (`db.rs:164`),
  `RECENT_BLOCK_COUNT` and `RECENT_TX_COUNT`, which the table lacks today.
  Correct the stale defaults: `INDEX_WS` is off, `INDEX_BATCH` is 32 and
  `NATIVE_SYMBOL` is `NVNM` (the Docker image sets `OM`), and nothing uses
  `CHAIN_ID`. `DB_PATH` stays the SQLite setting, and `DATABASE_URL` wins
  when it is set. Note that `DATABASE_URL` accepts `host:port` without a
  scheme, and that the connection is plaintext (section 7).
- **Routes** (`README.md:147-171`): add `/healthz`, `/readyz` and
  `/metrics` (the last under `ROLE=web` and `ROLE=indexer` only). The
  live-feed paragraph says the in-process feed is `ROLE=all`'s, and that
  web replicas get theirs from the polling follower.
- **Known limitations** (a new section): under the split deployment, a
  token with no mint, transfer or fee use yet is not listed or searchable,
  even after its page is opened, until its first transfer. The
  token-discovery PR that follows phase 3 removes it.
- **Tests** (`README.md:227-245`): add the `TEST_DB=postgres` runs with
  `docker compose` and `PG_TEST_URL`, as `AGENTS.md` has them.
- **Layout** (`README.md:247-271`): `src/db/` (`mod.rs`, `sqlite.rs`,
  `pg/`, `migrations.rs`), `migrations/`, `src/follow.rs`,
  `src/metrics.rs` and `deploy/k8s/`.

Before it merges, the reviewer checks that every variable in the
configuration table is read by the code and every route in the routes table
exists. Since then, the README leaves the Postgres test commands and the
migration details to `docs/database.md`.

**Cutover: side by side** (decision 9)

The Postgres stack is built next to production and synced from the chain,
then traffic moves to it. Every process connects to exactly one database,
and no tool reads both.

```mermaid
flowchart LR
    users(["Users"])
    rpc(["Chain RPC"])
    subgraph cur_dep["Current deployment, unchanged"]
        cur["today's image"] --> lite[("SQLite file")]
    end
    subgraph new_dep["New deployments"]
        idx["explorer-indexer<br/>ROLE=indexer"] --> pg[("Postgres")]
        web["explorer-web<br/>ROLE=web"] --> pg
    end
    users -->|"until the switch"| cur
    users -.->|"after the switch"| web
    rpc --> cur
    rpc --> idx
```

1. **Leave production as it is.** Production runs on Kubernetes, as one
   deployment of today's image on its SQLite file. It keeps its image,
   its SQLite file and all traffic. The new code reaches production only
   through the two new deployments, so it never opens the production
   SQLite file. The Fly app (`nvnmchain-explorer`, deployed by hand with
   `fly deploy`) is not production. It runs on SQLite, and the first deploy
   of stage 2 or later stamps its file v1.
2. **Deploy `explorer-indexer`** against an empty database. It passes the
   preflight, takes the lock, applies the migrations from v1, and
   re-indexes: forward from the head, backfill down to block 1, and the
   genesis, anchoring and missing-metadata jobs. Inferred: about 3 h for
   the roughly 2.3M blocks of 2026-10-05, limited by the RPC, plus about
   15 min for each later day (a block about every 0.48 s, about 180,000 a
   day, at ~200 blocks/s). That load lands on the node
   production also uses; point `NVNM_RPC` at a separate node if that one
   has little headroom. Any node used must be an archive node, as
   production's is: the re-index needs receipts back to block 1, `eth_call`
   at block 0 for the genesis balances, and `eth_getLogs` over the whole
   range.
3. **Deploy `explorer-web`** at any time after that. Its pods turn Ready
   once the indexer has applied the migrations (the schema gate), and get
   no public traffic until the switch. Pages show partial history until
   backfill completes. Reach them through an internal host (an internal
   load balancer or Ingress) to measure page p95, never through a
   `kubectl port-forward`, which adds its own latency.
4. **Wait until the indexer reports synced.** The indexer's `/readyz` body
   carries a `sync` object. Reading it never touches the database:

   ```json
   "sync": {"lowest_block": 1, "tip_lag": 0, "genesis": true, "anchoring": true, "complete": true}
   ```

   - `lowest_block`: the lowest committed block, as the backfill loop last
     read it from the database. The loop re-reads it there once it reaches
     block 1 (`indexer.rs:896-902`).
   - `tip_lag`: how far the forward loop is behind the chain head.
   - `genesis`: a genesis pass that started after `lowest_block` reached 1
     found no holder left.
   - `anchoring`: the anchoring backfill finished. It runs once per start
     (`indexer.rs:986-993`), so a failed run leaves this false until the
     indexer restarts, which surfaces a failure that today only logs a
     warning.
   - `complete`: `lowest_block` is 1, `tip_lag` is at most 16
     (`TIP_YIELD_LAG`, the lag at which backfill already yields to the
     head), and `genesis` and `anchoring` are true.
5. **Go/no-go,** checked by devops just before the switch:
   - `sync.complete` is true;
   - `blocks` has no holes. This returns true, run with the web user's
     credentials (the check only reads):
     `SELECT MIN(number) = 1 AND COUNT(*) = MAX(number) FROM blocks;`
     A hole means a block was dropped as a `Data` error and logged as
     "block N not written" (`indexer.rs:1033`). Investigate before
     switching;
   - for every URL in the page list below, the new deployment's p95 is at
     most today's p95 + 20 ms. Measure both deployments from one probe
     host, within the same hour, after `VACUUM (ANALYZE)` once sync
     completes and one warm-up pass (`oha -n 200 -c 1` per URL). On a
     miss, do not switch: apply stage 9's `join!`, then measure again;
   - a spot check: a few old blocks, transactions, addresses and tokens
     show the same data on both deployments.

   **The page list:** `/`, `/blocks`, `/txs`, `/tokens`, the block with the
   most transactions, a transaction on its second view (once its trace is
   stored), the address with the most transactions at page 1 and page 400
   plus its `?tab=transfers`, and the token with the most transfers plus
   its `?tab=holders`. Pick the heaviest address and token by SQL on
   Postgres.
6. **Switch.** Devops moves traffic (DNS or Ingress) to `explorer-web`.
   `trace_data` and `selector_names` start empty and refill on page views.
7. **Fallback.** Keep the current deployment running, and indexing, for
   7 days. Rolling back is the reverse switch, and no data moves in either
   direction. Then retire it with its SQLite file.

**No row-by-row comparison with production.** Nothing reads the production
SQLite file and Postgres together, so there is no snapshot, pinned height or
stop-at-height control. Correctness rests on section 9's tests (fixture
replay, the differential, the live canary re-index and the parity grid), the
hole check and the spot check, with the current deployment as the fallback.

**After the switch,** every release follows section 5's deploy order:
`explorer-indexer` first, then `explorer-web`.

## 11. Costs

**One-time** (estimates; the merge probe and the census measured the
caller and SQLite counts):

| Item                                                                              | Lines                                                            |
|-----------------------------------------------------------------------------------|------------------------------------------------------------------|
| Existing SQLite code                                                              | −3/+7, plus a few in `schema_check.rs` when SQLite 0002 lands    |
| Callers (`.await` codemod)                                                        | 360–400                                                          |
| Stage-0 prep                                                                      | 70–120                                                           |
| `db/mod.rs`, label cache, hooks, seal                                             | ~450                                                             |
| SQLite runner, `extra.rs`, pins, guard                                            | ~200                                                             |
| Shared migration list and gate                                                    | ~150                                                             |
| Postgres backend (`pg/*`: reads, writes, writer, runner, `mod.rs`, `q`, `shared`) | 3,000–3,300                                                      |
| Follower, `ROLE`, config, health and sync status, metrics, 503 middleware         | ~500                                                             |
| Tests                                                                             | 1,850–2,150                                                      |
| CI YAML                                                                           | ~100                                                             |
| **Total code**                                                                    | **~6,690–7,380**, plus ~400 lines of manifests, runbook and docs |

**Recurring:**

- **A new query.** The SQLite function as today, one `db_fn!` line, a
  Postgres twin of about 10–30 lines through `q`, and one grid entry. A
  missing twin does not compile.
- **A schema change.** Two new SQL files and one list line, plus a
  watermark reset when a derived table is re-keyed. A re-keyed index takes
  two releases.
- **A new page.** `.await` only.
- **Infrastructure.** The Postgres target (a single primary, without HA)
  and two GKE deployments, in place of today's single SQLite deployment.

**Build.** About 23 extra crates for sqlx with postgres and no TLS. Both
backends always compile, so clippy always checks both.

## 12. Risks and what to measure

| Risk                                                           | Containment                                                                                                                                                                                                                                                                                                                                                                                                     |
|----------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| The two SQL sets drift                                         | A missing twin does not compile; the parity grid and coverage gate; the differential; the `TEST_DB` matrix; the constant-equality test; the Postgres SQL lint                                                                                                                                                                                                                                                   |
| Set-based results differ from per-row                          | The planner property test, with SQLite as the reference; the differential with shuffled and duplicated batches; the rebuild pass in replay; fallback: the per-row writer (section 10, 4b)                                                                                                                                                                                                                       |
| A partial commit or a silent lock loss                         | Spawned, cancel-safe writes; the session fence; `try_lock` required to re-acquire; the self-checks; lock and cancel tests                                                                                                                                                                                                                                                                                       |
| Blocks lost on a database restart or error, or after a restore | `Unavailable` is the default class and is retried; `writer_seq` with run id and exit 4; fallible frontier reads; the container-restart and injected-error tests                                                                                                                                                                                                                                                 |
| The database is a single point of failure (no HA)              | Pages return 503 with `Retry-After` while it is down, and no block is lost (section 6). If the data itself is lost, a re-index rebuilds it at the rate item 7 measures: at ~200 blocks/s, about 3 h for the ~2.3M blocks of 2026-10-05, plus about 15 min for each day the chain grows. Pages show partial history until backfill completes (section 10). HA can be added later with no code change (section 7) |
| Orphan sessions holding the lock                               | The lease, `tcp_user_timeout`, `client_connection_check_interval`, terminating the recorded pid, and the watchdog                                                                                                                                                                                                                                                                                               |
| A deploy deadlock, a broken image, or version skew             | Probes never wait on the lock; the indexer preflight gates readiness; the one-release contract rule, checked in review; indexer-first deploys                                                                                                                                                                                                                                                                   |
| A divergence that only production data shows                   | Section 9's tests (replay, differential, the live canary re-index, the grid); the cutover's hole check and spot check; the current deployment kept for 7 days as the fallback                                                                                                                                                                                                                                   |
| Teammates edit `init_db` or a merged migration                 | Pins A and B; the CI immutability step; runtime checksums                                                                                                                                                                                                                                                                                                                                                       |
| Two repos assign the same migration number                     | Numbers are assigned on NVNM-Chain's `main` only, and other repos sync migrations from it (section 5); the list test as a required, up-to-date check; runtime checksums refuse a mismatch                                                                                                                                                                                                                       |
| The commute rule proves too restrictive                        | Stub patterns, two-release index changes, and the section 5 escape hatch                                                                                                                                                                                                                                                                                                                                        |
| sqlx 0.9.0 is new                                              | Pinned to `=0.9.0`; MSRV 1.94                                                                                                                                                                                                                                                                                                                                                                                   |

**What to measure.** Item 1 picked the writer; the rest tune the target
devops chose (decision 2). They do not decide whether Postgres ships. Items 2 and
4 need only a provisioned target and can run at any time; item 6 also needs
the indexer and web deployments.

1. The stage 4b writer gate: equal tables on `canary-rich`, within 14
   round trips per batch. It was met, so the set-based writer shipped
   (section 10). Separately,
   record the average Postgres lines per function (target 40 or fewer). It
   checks section 11's recurring cost and does not pick the writer.
2. RTT and commit latency from the explorer's pods to the chosen database
   (Cloud SQL private IP, or the in-cluster Service).
3. Postgres page p95 for the 9-query page, sequential vs `join!`.
4. That `lock_timeout`, `idle_session_timeout`, `tcp_user_timeout` and
   `client_connection_check_interval` behave as designed on the chosen
   target.
5. The inline facade's overhead on `write_scale` and the pages suite
   (target under 2%).
6. A restart drill on the chosen target: restart the database (a Cloud SQL
   restart, or deleting the CloudNativePG pod) under the indexer and a
   5 rps page probe. Run it after `sync.complete`, or use the range form of
   the hole check, `SELECT COUNT(*) = MAX(number) - MIN(number) + 1 FROM blocks`,
   which holds during backfill. Record the database's downtime and the
   probe's first-to-last 503 window. It passes when pages return 503 and
   then 200, the hole check finds no hole, no explorer pod's restart count
   changed, and `explorer_writer_state` returns to 1.
7. Re-index throughput into the chosen target, against today's ~200
   blocks/s backfill rate (measured on SQLite, `README.md:123`). The per-row writer needs about 4.0–4.4
   round trips per block (259–283 per 64-block batch), so it keeps pace only
   at an RTT of about 1.1 ms or less. Above that, a re-index runs longer.
   That delays the switch, and it lengthens any rebuild after data loss
   (the single-point-of-failure risk above).
8. The follower's commit-to-browser latency at 500 ms.

## 13. Corrections to earlier specs

- **Phase 2's "Recorded for phase 3: Sync vs async"** is superseded by
  section 1. The API is async, and there is no bridge.
- **Phase 2's "`sslmode=require` for non-local hosts"** and its "adds a
  rustls TLS feature" are superseded by decision 11: sqlx is built without
  TLS, and any `sslmode` but `disable` is refused (section 7).
- **Phase 2's "a writer pool of size 1"** is replaced by one owned session.
  A pool silently replaces its connection (`max_lifetime` defaults to
  30 min, `idle_timeout` to 10 min), and the session lock goes with it.
- **Phase 2's "a Postgres version of the shape check" at startup** is
  replaced by versioned migrations with checksums, the preflight and the
  per-version parity test.
- **The phase 1 and 2 merge rules** ("`init_db` off limits", "new code in
  new files") are relaxed by decision 4. `init_db` is now frozen as
  migration 1.
- **`docs/database.md`** ("There is no migration runner and no version
  number") and **`AGENTS.md`** (the test commands) are rewritten in stages 2
  and 6. **`README.md`** is rewritten last, in stage 8.

## 14. Out of scope

- A SQLite → Postgres data importer (decision 9).
- A SQLite reader pool, sqlx-sqlite, and any change to SQLite SQL, pragmas,
  the writer or `init_db`.
- An ORM or a query builder.
- Managed Connection Pooling, PgBouncer and the Auth Proxy; reads from
  replicas or read pools.
- LISTEN/NOTIFY (deferred; section 8).
- IAM database authentication (planned for later).
- Discovering factory-created tokens with no transfers. It is the next
  task after this phase, in its own PR (see "Known limitations").
- HA for the database (decision 2). The design already tolerates it, so it
  can be added later with no code change.
- Security hardening beyond section 7: TLS to the database (decision 11),
  a NetworkPolicy or authorized-networks rule, rate limits and request
  timeouts on public pages, and `cargo audit` or `cargo deny` in CI.
- Cloud Run manifests (a note in `deploy/k8s/README.md` only), and Fly.io
  or Render deployments of the Postgres mode.
- Provisioning Postgres: Terraform, or the operator's `Cluster` resource.
- Down migrations, a separate migration job, and image rollback across a
  migration (roll forward instead).
- A per-version web gate (`web-safe-from`). Revisit it only if web
  rollbacks of two or more releases become a practice. It must then ship
  at least one release before the contract it guards.
- Async Tera functions.
- Partitioning; multi-region.

## 15. Acceptance

The phase is done when:

1. `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` pass,
   and `rust-version = "1.94"` is set.
2. `cargo test --lib --test decoder --test anchoring --test pages` passes
   with no network, on SQLite, with the same results as before stage 1.
3. `git diff -M main -- src/db/sqlite.rs src/db/indexer_jobs.rs src/db/schema_check.rs`
   shows only the lines listed in section 4.
4. With `docker compose up -d --wait`, `TEST_DB=postgres` runs the same
   suites green. The replay, differential, grid, migrations, locks and
   outage suites pass. The `Postgres` workflow passes on PG 18 for every
   PR, and on PG 15 nightly.
5. The live baseline re-index into Postgres matches both canary fixtures,
   within the 14-round-trip budget when the set-based writer ships.
6. A deployed SQLite file opened by the new binary is adopted and stamped
   v1, with its rows and shape unchanged, and today's binary can still open
   it while the file is at v1. After a SQLite 0002, images with the runner
   open it only when B ≥ D. Today's binary still opens it when 0002 only
   adds tables (`schema_check` ignores tables only in the file), and
   refuses it when 0002 adds a column or an index to an existing table.
7. A full re-index into the chosen target reaches `sync.complete` with no
   holes in `blocks` (section 10).
8. **Availability.** Under a 5 rps probe over the section 10 page list, a
   rolling release of `explorer-web` and a kill of the `explorer-indexer`
   pod each produce zero non-2xx responses.
9. **Data growth.** On the fully re-indexed production data, every URL in
   the page list meets the section 10 p95 bound.
10. **`README.md` matches the shipped code** (stage 8). Its configuration
    table lists every variable the code reads, its routes table every
    route, and it states the known limitations.
