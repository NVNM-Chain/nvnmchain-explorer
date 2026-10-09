# Phase 1: seal the database boundary (SQLite only)

Status: implemented (merged as bb83764)
Date: 2026-10-01

## Why

The explorer should be able to run on a local SQLite file or a remote Postgres
server, chosen by `DB_PATH`. The work comes in three steps:

1. **This spec.** Production code outside `src/db.rs` (and its new child
   module) reaches the database only through `db::*` functions on an opaque
   `Db` handle. SQLite stays the only backend, and behaviour does not change.
2. Versioned schema migrations, on SQLite, designed for two dialects.
3. A Postgres backend behind the same `Db` handle.

## Constraint: the fork keeps merging upstream

This repo is a fork that regularly merges `yihuang/main`. Since 2026-08-01, 46
non-merge commits have touched `src/db.rs` or `src/indexer.rs`, and 33 have
touched `tests/decoder.rs`. Every line phase 1 changes in those files is a line
a later merge can conflict on. The rules:

- **Put new code in new files.** Code moved out of the indexer goes to a new
  child module, `src/db/indexer_jobs.rs`. Upstream never edits that file, and
  as a child of `db` it can still call `db.rs`'s private functions.
- **Move code byte-for-byte.** A moved function body keeps its `db::` prefixes
  and imports. That way an upstream hunk against the old `indexer.rs` copy
  applies to the new file with only a path rewrite (`git apply -3`).
- **Don't touch hot spots.** No reordering, no renames, and no SQL text
  changes in `db.rs`. `init_db` and the end of the file are both off limits:
  `init_db` changed in 21 upstream commits since 2026-08-01.
- **Enforce the seal with a test, not a convention.** Upstream will keep adding
  `db::lock(db)` calls in the indexer, so a check that runs on every
  `cargo test --lib` catches them after each merge.

## Design

### 1. `Db` becomes an opaque handle

```rust
/// The explorer's database. Cheap to clone; every clone shares one connection.
#[derive(Clone)]
pub struct Db(Arc<Mutex<Connection>>);
```

This replaces `pub type Db = Arc<Mutex<Connection>>`, and the field is private.
`Arc::new(Mutex::new(conn))` no longer produces a `Db`, so a `Db` can only come
from `db::open`.

The body of `lock` changes from `db.lock()` to `db.0.lock()`. Its signature and
every call site inside `db.rs` stay the same.

### 2. `db::open` is the only constructor

```rust
/// Open (creating if needed) the database at `path` and bring its schema up to date.
pub fn open(path: &str) -> Result<Db> {
    Ok(Db(Arc::new(Mutex::new(init_db(path)?))))
}
```

`open` sits directly after `lock`, which has been untouched since the
initial commit. **`init_db` itself does not change.**

`main.rs` changes to:

```rust
let db: Db = db::open(&cfg.db_path).context("initialize database")?;
```

The two lines it replaces were `let conn = db::init_db(..)` and
`let db: Db = Arc::new(Mutex::new(conn));`. `Mutex` is dropped from
`use std::sync::{Arc, Mutex};`, and `Arc` stays because `home_stats` uses it.

**This differs from the proposal you approved.** I said the DDL would move into
its own schema module. Both places where that split would cut `init_db` are
lines upstream keeps editing:

- between the pragmas and `execute_batch`, where the old Rust-side migration
  block lived;
- the tail around `seed_counters`.

A pre-DDL step added upstream could also merge silently into a
`configure_sqlite` helper that phase 2 runs on SQLite only. The migrations task
replaces this body anyway, so it does the split once. Phase 1 changes nothing
in `init_db`.

### 3. The indexer stops naming rusqlite

New file `src/db/indexer_jobs.rs`. It is declared directly after `open` in
`db.rs`:

```rust
mod indexer_jobs;
pub use indexer_jobs::{compute_and_store_stats, repair_derived_tables, save_anchoring_window};
```

Its imports copy what the moved bodies used in `indexer.rs`:
`use crate::db::{self, Db}`, `rusqlite::params`, `tracing::warn`,
`anyhow::Result` and `serde_json::Value`.

| Today in `indexer.rs`                                                                                                                             | After                                                                                                                     |
|---------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| `fn compute_and_store_stats(db: &Db) -> Result<Value>`: holds `db::lock`, runs 6 raw `query_row` calls, plus `db::counter` and `db::set_kv`       | Moves byte-for-byte into `indexer_jobs.rs`; `fn` becomes `pub fn`. `stats_loop` calls `db::compute_and_store_stats(&db)`. |
| `fn repair_derived_tables(db: &Db)`: three `db::lock` scopes calling `table_has_rows`, `rebuild_token_balances` and `sync_holder_counts`          | Moves byte-for-byte; `fn` becomes `pub fn`. The startup `tokio::spawn` calls `db::repair_derived_tables(&rebuild_db)`.    |
| `backfill_anchoring`'s per-window block: `conn.transaction()`, `anchoring_event_from_log(&txn, ..)`, `db::insert_anchoring`, `db::set_kv`, commit | Replaced by one call to `db::save_anchoring_window` (below).                                                              |
| `fn anchoring_event_from_log(conn: &Connection, log)`                                                                                             | Changes only its parameter type and one line (below).                                                                     |
| `use rusqlite::{params, Connection};`                                                                                                             | Deleted. `params!` was used only in `compute_and_store_stats`.                                                            |

**The anchoring window.** Decoding stays in the indexer, so `db` doesn't start
depending on the anchoring decoder. The database hands the decoder a timestamp
lookup inside the transaction:

```rust
/// One backfill window, in one transaction: build the window's events with
/// `stamp` (a block's indexed timestamp, if it is indexed), insert them
/// (duplicates ignored), and advance the watermark at `key` to `value`, also
/// when there is nothing to insert. Returns the number of rows inserted.
pub fn save_anchoring_window(
    db: &Db,
    key: &str,
    value: &str,
    events: impl FnOnce(&dyn Fn(i64) -> Option<i64>) -> Vec<AnchoringEvent>,
) -> Result<usize>
```

The body is today's block with the same steps:

1. lock;
2. `transaction()`;
3. call `events(&|n| db::get_block_timestamp(&txn, n))`;
4. `insert_anchoring` each event and sum the inserted rows;
5. `set_kv(&txn, key, value)`;
6. commit.

The indexer block becomes:

```rust
wrote += db::save_anchoring_window(db, BACKFILL_KEY, &to.to_string(), |stamp| {
    logs.iter().filter_map(|log| anchoring_event_from_log(stamp, log)).collect()
})?;
```

In `anchoring_event_from_log`, the parameter becomes
`stamp: &dyn Fn(i64) -> Option<i64>` and the timestamp line becomes
`let timestamp = stamp(block_number)?;`.

Reads and writes still share one transaction under one lock. The watermark
still advances on every window, including an empty one, so behaviour is
identical. The comment above the block ("One transaction per window, watermark
included") stays true.

### 4. Visibility

- **Becomes private:** `set_kv`, `insert_anchoring` and `table_has_rows`
  (`insert_anchoring` was `pub(crate)`). Their only outside callers were the
  moved code. Any upstream change that calls them from the indexer then fails
  to compile, which is the point.
- **Left byte-identical, still `pub`:** `lock`, `init_db`, `counter`,
  `get_block_timestamp`, `sync_holder_counts` and `rebuild_token_balances`.
  These are SQLite test hooks: tests use them to inspect the file. Adding
  attributes to them would only add diff lines next to hot signatures. The
  seal test in section 6, not their visibility, keeps production code off
  them.

### 5. Tests

These are the allowed edits to existing tests:

- **Constructors.** Every `init_db(..)` followed by `Arc::new(Mutex::new(conn))`
  becomes `db::open(..)`. That covers the `temp_db()` helpers in `decoder.rs`,
  `pages.rs` and `live_rpc.rs`, `fresh()` in `write_scale.rs`, and the inline
  copies in `anchoring.rs` and `decoder.rs`.
- **Mutex call.** `tests/pages.rs` calls `db.lock().unwrap()`, which was
  `Mutex::lock` on the old alias. It becomes `db::lock(&db)`.
- **Imports left unused.** Only the now-unused token goes; `Arc` stays where
  it is still used.

  | File | Line | Edit |
  |---|---|---|
  | `tests/decoder.rs` | 12 | drop `Arc` and `Mutex` |
  | `tests/decoder.rs` | 950, 1063 (function-local) | drop `Arc` and `Mutex` |
  | `tests/live_rpc.rs` | 7 | drop `Arc` and `Mutex` |
  | `tests/write_scale.rs` | 8 | drop `Arc` and `Mutex` |
  | `tests/anchoring.rs` | 4 | drop `Mutex` |
  | `tests/pages.rs` | 9 | drop `Mutex` |

  CI sets `RUSTFLAGS=-D warnings` for the test job, so a leftover unused import
  fails the build.

Tests that use `init_db` for a raw `Connection` don't change.

New tests go in a `#[cfg(test)] mod tests` inside `indexer_jobs.rs`, not in
`tests/decoder.rs`:

- `save_anchoring_window`:
  - a repeated window inserts 0 rows;
  - an empty window still writes the watermark;
  - `stamp` returns the timestamp for an indexed block and `None` for an
    unindexed one. The indexer's decoder skips the event on `None`.
- **The seal test** (see section 6).

### 6. The seal test

A unit test in `indexer_jobs.rs`, run by the `cargo test --lib` that CI already
runs:

- It walks `$CARGO_MANIFEST_DIR/src` and skips `src/db.rs` and `src/db/`.
- It fails if any other file contains `rusqlite`, `db::lock`, `init_db` or the
  word `Connection`, and it names the file and line.
- No CI workflow edit is needed.

### Post-merge recipe (every upstream sync)

Upstream will keep writing tests and indexer code against the old alias, so
expect a few compile errors after a merge rather than textual conflicts:

0. Sync through a pull request (added in phase 2):
   1. `git fetch upstream`.
   2. Merge `upstream/main` into a `sync/<date>` branch.
   3. Open a PR to `main`, and run steps 1-5 on that branch.
   4. Merge only when CI is green, including the fixture shape test and the
      `Postgres` workflow.

   Do not use GitHub's "Sync fork" button, which every earlier sync used
   (`4123460`, `84392e0`, `c3d4a1e`, `7ce6608`). It pushes straight to
   `main`, where `docker.yml` publishes `:latest` whether or not CI passes.
1. `rg -n 'Arc::new\(Mutex::new\(|db\.lock\(\)' src tests`: replace each hit
   with `db::open(..)` or `db::lock(&db)`.
2. If upstream edited `compute_and_store_stats` or `repair_derived_tables` in
   `indexer.rs`, port the hunk with `git diff A B -- src/indexer.rs`, rewrite
   the path to `src/db/indexer_jobs.rs`, then `git apply -3`.
3. `cargo test --lib`: the seal test names any new leak.
4. If `cargo test --test postgres -- --include-ignored` fails on parity, port
   the `init_db` change to `src/db/schema_pg.sql`, using the type rules at the
   top of that file.
5. If `every_baseline_fixture_opens` fails, the merge changed the columns or
   keys of an existing table, and every deployed database will refuse to
   start. Before merging the sync PR:
   1. Pick that table's recovery from the map in `src/db/schema_check.rs`
      (copied in `docs/database.md`). Add an entry if the table has none.
   2. Regenerate each affected fixture over its own block range.
      `build_baseline` never overwrites a file and defaults to `RICH_RANGES`,
      so delete the old file first:

      ```text
      rm fixtures/baseline/canary-rich.db
      BASELINE_BUILD=fixtures/baseline/canary-rich.db \
          cargo test --test baseline build_baseline -- --ignored --nocapture
      rm fixtures/baseline/canary-blocks.db
      BASELINE_BUILD=fixtures/baseline/canary-blocks.db BASELINE_RANGES=1579026-1583674 \
          cargo test --test baseline build_baseline -- --ignored --nocapture
      ```

   3. Update `canary-blocks.db`'s line in `tests/baseline.rs`'s doc comment:
      it is now rebuilt by `build_baseline` at `<commit>`, no longer the
      deployed copy, unless a fresh copy of the deployed database, taken
      after the recovery, replaces it.

## Known phase 2 constraints (recorded, not solved here)

- **Sync vs async.** Every `db::*` call is synchronous on tokio worker threads.
  That covers about 60 calls in `web.rs` handlers, `signatures::resolve`, the
  indexer, and two sync Tera callbacks (`get_block_url` and `address_label` in
  `build_tera`).
  - The sync `postgres` crate panics when called on a runtime thread.
  - `block_in_place` panics on `#[tokio::test]`'s default current-thread
    runtime, which `tests/pages.rs` and `tests/anchoring.rs` use.
  - So a Postgres backend stays inside `db.rs` only if it keeps the API sync,
    for example a client on a dedicated OS thread fed over a channel.
  - Going async instead changes about 95 call sites and the Tera callbacks.
- **Tests that reach through `db::lock(&Db)`** need a per-backend answer once
  `Db` holds an enum. There are 7 sites:
  - `tests/decoder.rs`, four sites: the counters-seeding test, the
    duplicate-bundle test, the `get_block_timestamp` asserts, and the stale
    holder count test;
  - `tests/live_rpc.rs` (anchoring row query);
  - `tests/pages.rs` (`rebuild_token_balances`);
  - `tests/write_scale.rs` (`pragma_update`).
- **Credentials in logs.** A Postgres URL in `DB_PATH` would leak through
  `main.rs`'s startup log, `Settings`' `Debug`, and `init_db`'s
  `open db {path}` error context.
- **Swallowed read errors.** Inside `save_anchoring_window`,
  `get_block_timestamp` turns a read error into `None`, so the log is skipped
  and the watermark still advances. That is harmless on SQLite. On a remote
  database it should fail the window instead, and that change stays inside
  `db`.

## Out of scope

- Postgres, a backend enum or trait, connection pooling, and the sync-vs-async
  decision.
- A migration runner, a version table, splitting the DDL out of `init_db`, the
  `token_balances` columns declared `BLOB` but holding TEXT, and the
  README's `PRAGMA user_version` claim.
- Reading `DB_CACHE_KIB` through `Settings`.

## Acceptance

1. `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` pass.
2. `cargo test --lib --test decoder --test anchoring --test pages` passes. That
   includes the seal test and the new `save_anchoring_window` tests.
   `cargo test --test live_rpc` passes where the RPC is reachable.
3. The moved bodies are byte-identical: the old and new versions of
   `compute_and_store_stats` and `repair_derived_tables`, extracted from
   `git show HEAD:src/indexer.rs` and from `src/db/indexer_jobs.rs`, differ
   only by the added `pub`.
4. `git diff` shows that:
   - `init_db` is unchanged;
   - no SQL string in `db.rs` changed;
   - `db.rs` changes only in the `Db` definition, `lock`'s body, the
     `open` / `mod` / `pub use` block after `lock`, and the three visibility
     downgrades.
5. Run against a copy of an existing `explorer.db` with the RPC unreachable,
   the old and new binaries leave the same things behind:
   - `sqlite3 .schema` output;
   - the `counters` rows;
   - `kv` keys and values, except `stats`;
   - the stats JSON, except `updated_at`, `txns_24h` and `blocks_24h`.
