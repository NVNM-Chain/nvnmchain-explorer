# Database schema

How the explorer's schema evolves on SQLite and Postgres, what the startup
checks refuse, and how to run the Postgres tests. The design is
`docs/superpowers/specs/2026-10-03-postgres-backend-design.md`.

## Versioned migrations

Both backends share one list of schema versions, `migrations!{}` in
`src/db/migrations.rs`. Each database records the versions it has applied in
`schema_migrations`, with a checksum of each file.

- **Version 1** is the baseline. On SQLite it **is** `init_db`
  (`src/db/sqlite.rs`), which is frozen. On Postgres it is
  `migrations/postgres/0001_baseline.sql`.
- **From 0002 on**, every version is a twin pair with the same number and
  name: `migrations/sqlite/NNNN_name.sql` and
  `migrations/postgres/NNNN_name.sql`, plus one line in `migrations!{}`.
- **D** is a database's version (its highest `schema_migrations.version`),
  **B** this binary's (the last entry in the list). A process applies
  versions D+1..B; a database newer than the binary (D > B) is refused, and
  the only way back is forward. So is one whose versions skip a number below
  D, on either backend: the runners record versions in order, so only hand
  edits leave a gap.
- **Merged files never change.** A running process refuses a database whose
  recorded checksum differs ("migration N was edited after it was applied"),
  and CI fails a pull request that modifies, deletes or renames anything under
  `migrations/`. Fix a mistake with a new version.

On SQLite, `db::open` runs `init_db` as it always has, then the runner
(`src/db/sqlite/migrate.rs`) does the rest in one `BEGIN IMMEDIATE`
transaction: it stamps a fresh or pre-versioning file as version 1, checks
checksums and D ≤ B, applies the pending files, and runs the shape check
below. Nothing is stamped unless the result verifies, so a drifted file keeps
today's error.

### `init_db` is frozen

Two tests pin it, and both fail with "init_db is migration 1 and frozen":

- **Pin A** hashes the schema `init_db(":memory:")` builds.
- **Pin B** hashes `init_db`'s source, which catches edits to the legacy DROP
  list and `seed_counters` that a fresh database cannot show. Statements that
  set a pragma or read `DB_CACHE_KIB` are left out, so pragmas can still be
  tuned in place.

### Adding a schema change

A schema change is a pull request to `NVNM-Chain/nvnmchain-explorer`'s
`main`, which assigns version numbers (see below). It touches:

- `migrations/sqlite/NNNN_name.sql`, written to the commute rule;
- `migrations/postgres/NNNN_name.sql`;
- one line in `migrations!{}`;
- the query edits in `src/db/sqlite.rs` (or `src/db/sqlite/extra.rs`) and in
  `src/db/pg/`;
- in `tests/postgres.rs`, a `TRANSLATED` pin for each expression or partial
  index it adds or changes, and an `ALLOWED` entry for each difference
  between the engines it means to make. Each entry holds from its version
  (`since`) on. None has an end version yet: the first migration that changes
  or drops what one describes adds one, and pins a changed definition in a new
  entry, so the parity test still holds every older version to its own.

Files are plain SQL, with no headers. A twin with nothing to do is empty or
holds only comments.

**Expand or contract.** Review enforces both rules: a new column is nullable
or has a default, and a change that removes or rewrites what the code uses (a
contract) ships one release after the code stops using it, naming that
release in its pull request.

**Postgres files.** Each runs in one transaction. A plain `CREATE INDEX`
blocks writes to its table but not reads, so pages keep serving while it
builds. Never put `DROP INDEX`, `ALTER TABLE` or anything else that takes an
`ACCESS EXCLUSIVE` lock in the same file as a `CREATE INDEX` on a big table
(`transactions`, `transfer_events`): reads of that table would wait for the
whole build.

**The commute rule (SQLite).** `init_db` still runs on every open, so a SQLite
migration must commute with it: it must not drop, by name, an object `init_db`
creates, nor create a name on `init_db`'s DROP list.
`every_migration_commutes_with_init_db` checks every version. The patterns:

| Change                        | SQLite twin                                                                                                     | Postgres twin                                                 |
|-------------------------------|-----------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------|
| New table, column or index    | As usual                                                                                                        | As usual                                                      |
| Re-keyed index (two releases) | Expand N: `CREATE INDEX x_v2 …`. Contract N+1: `DROP INDEX x; CREATE INDEX x ON t(col) WHERE 0` (an empty stub) | Expand N: `CREATE INDEX x_v2 …`. Contract N+1: `DROP INDEX x` |
| Retired index                 | The stub                                                                                                        | `DROP INDEX x`                                                |
| Re-keyed derived table        | `DROP TABLE t; CREATE TABLE t (…)`, `init_db`'s index names on it, and `DELETE FROM kv WHERE key = '…'`         | The same, in one transaction                                  |
| Retired table                 | `DELETE FROM t`, plus stub indexes                                                                              | `DROP TABLE t`                                                |

### Who assigns version numbers

A version number must mean the same file everywhere: a database that ran one
`0002` refuses an image whose `0002` differs. So numbers are assigned on
`NVNM-Chain/nvnmchain-explorer`'s `main` only:

- Migrations merge there first. `yihuang/nvnmchain-explorer` and personal
  forks get migration files only by syncing from it, and never add their own.
- A pull request takes the next free number when it opens, and renumbers its
  pair if another pull request merges a migration first. Renaming a file that
  has not merged is allowed.
- `main` requires a pull request to be up to date before it merges, with the
  test suite as a required check, so the migration list test always sees the
  latest numbers.

## The startup shape check

`db::open` runs `init_db`, then `src/db/schema_check.rs` compares the opened
database with what `init_db` builds in an empty in-memory database:

- every column's name, declared type, `NOT NULL` and default, and the column
  order;
- the primary key, with each key column's direction and collation;
  `AUTOINCREMENT`; and every `UNIQUE` constraint;
- every index written as `CREATE INDEX`: its key columns and direction,
  uniqueness, and its SQL (case and whitespace ignored), which alone shows a
  partial predicate or an expression.

A table only in the database is ignored: upstream retires tables with `DROP
TABLE IF EXISTS`, which has already run.

The collation of a column outside every key is not compared:
`pragma_table_info` does not report it, and no index carries it.

On any difference the explorer exits with code 1 and never serves a page; it
binds the port first, so only `/healthz` answers while it opens. On Fly (one
machine, replaced in place) and under systemd (`Restart=always`) the explorer
is then down until an operator either redeploys the previous image
(`fly deploy --image …:sha-<commit>`) or applies the recovery below and
restarts. There is no switch to skip the check: starting on a drifted schema
is the failure it prevents.

### Reading the error

```text
the database's tables differ from what this build creates, so it was not opened (see docs/database.md):
blocks: column finalized: missing
blocks: index idx_blocks_timestamp: definition differs
  recovery: no watermark exists, and the backfill only walks below MIN(blocks.number): stop the explorer and re-index from an empty database file.
  recovery for idx_blocks_timestamp: `DROP INDEX "idx_blocks_timestamp";` and restart; init_db rebuilds it from the existing rows.
```

Lines labelled `column …` or `table: …` are **table drift**: only re-deriving
the table fixes them. Lines labelled `index …` are **index drift**: drop the
index and restart, and `init_db` rebuilds it from the rows already there.

### Recovery per table

The map lives in `recovery()` in `src/db/schema_check.rs`; this is a copy.

| Table                                                               | Recovery                                                                                                                             |
|---------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| `blocks`, `transactions`, `transfer_events`, `token_metadata`, `kv` | No watermark exists, and backfill only walks below `MIN(blocks.number)`. Stop the explorer and re-index from an empty database file. |
| `token_balances`                                                    | Drop it. `repair_derived_tables` rebuilds it from `transfer_events` and `genesis_balances` on the next start.                        |
| `counters`                                                          | Drop it. `seed_counters` recounts it on open.                                                                                        |
| `anchoring_events`                                                  | Drop it and delete the `kv` key `anchoring_backfilled_to`.                                                                           |
| `genesis_balances`                                                  | Drop it and `token_balances`, and delete the `kv` key `genesis_balances_cursor`.                                                     |
| `selector_names`                                                    | Drop it. It is a cache of directory lookups.                                                                                         |
| any other table                                                     | No known recovery. Work it out before deploying, and add it to the map.                                                              |

Back up the database file before dropping anything.

### When `init_db` itself fails

The check runs after `init_db`, and an old table can fail `init_db` first:
an upstream index over a column the table lacks fails its `CREATE INDEX` with
`no such column`, and a `counters` table without `n` fails `seed_counters`.
`db::open` then reopens the file read-only and compares the tables both it
and `init_db` have. When any differ, the error starts with the same lines and
recoveries as above, under "init_db failed on a database whose tables differ
from what this build creates", and ends with `init_db`'s own error. Missing
tables and indexes are left out: `init_db` creates those, and may have stopped
before it reached them. With no table drift, `init_db`'s error is reported
unchanged.

This is a diagnosis after the failure, not a check before `init_db`. A check
first would refuse a legacy database that an older build's in-place fix in
`init_db` (a guarded `ALTER TABLE … ADD COLUMN`) was about to repair. `init_db`
is frozen now (pins A and B), so a schema change is a migration pair instead. The fixture test (below) catches these
cases on the sync PR too, because it opens copies of the fixtures the same
way.

## Upstream merges

Sync upstream through a pull request, not GitHub's "Sync fork" button, which
pushes straight to `main`, where `docker.yml` publishes `:latest` whether or
not CI passes. Three checks matter for the schema:

- **Pins A and B** (`src/db/sqlite/migrate.rs`) fail when a merge edits
  `init_db`. Rewrite the change as a migration pair (see "Adding a schema
  change").
- **`every_baseline_fixture_opens`** (in `cargo test --lib`) runs `db::open`
  on a copy of each `fixtures/baseline/*.db`, which adopts it as version 1 and
  applies the pending migrations. A merge that changes an existing table
  without a migration fails it.
- **`every_version_has_the_same_shape_on_both_backends`** (in
  `tests/postgres.rs`) holds every version's Postgres files to the shape of
  `init_db` plus the SQLite twins.

## Postgres

The Postgres backend (`src/db/pg/`) has a hand-written twin of every
`src/db/sqlite.rs` function; `db_fn!` in `src/db/mod.rs` fails to compile when
one is missing. Reads go through `src/db/pg/q.rs`, which keeps SQLite's rule
(a failed read is logged and returns no data) and also flags the request, so
the page becomes a `503` with `Retry-After: 30` rather than an empty page or a
false 404. Chain data is written only by the indexer's writer session, which
holds a session-level advisory lock (`src/db/pg/writer.rs`); a web replica
writes only the selector-name and trace caches.

Configuration: `DATABASE_URL`, `PGUSER`, `PGPASSWORD`, `PGSSLMODE`, `ROLE`,
`DB_WEB_ROLE` and `FOLLOW_POLL_MS`, read by `src/db/config.rs`; `README.md`'s
table has their defaults and meaning.

`ROLE=all` on Postgres is supported for development, CI and small
self-hosting. Production runs `ROLE=indexer` and `ROLE=web`
(`deploy/k8s/README.md`, `docs/runbook.md`). Under `ROLE=all` on Postgres, a
token page opened while the database is down hangs until it is back (plus up
to 30 s of writer backoff), then returns 503: its metadata save waits on the
writer, and runs on after the client leaves.

### Running the Postgres tests

```text
docker compose up -d --wait
export PG_TEST_URL=postgres://explorer:explorer@localhost:5432/explorer

# The usual suites, on Postgres.
TEST_DB=postgres cargo test --test decoder --test anchoring --test pages

# The Postgres suites: replay, the differential, the parity grid (with its
# coverage gate), migrations, locks and outages.
cargo test --lib -- --include-ignored
cargo test --features db-coverage --test postgres --test migrations --test replay \
    --test differential --test grid --test locks --test indexer_pg --test outage_drills \
    -- --include-ignored

# Restarts the container, so it runs alone.
PG_CONTAINER=nvnmchain-explorer-postgres-1 cargo test --test restart_drill -- --include-ignored

# The live re-index into Postgres (needs the chain's RPC).
TEST_DB=postgres cargo test --test baseline
```

`PG_PORT=5433 docker compose up -d --wait` uses another host port; change the
URL to match. The Postgres tests are `#[ignore]`d so plain `cargo test` needs
no server, and with `--include-ignored` but no `PG_TEST_URL` they fail rather
than skip. Each test works in a schema of its own, dropped when it passes and
kept to inspect when it fails. The next run sweeps the `t_<pid>_<n>` ones whose
process is gone. The compose file starts Postgres with `max_connections=300`,
since every test opens pools of its own.

CI runs the same in `.github/workflows/postgres.yml`, against PostgreSQL 18
on every pull request and 15 nightly. The image tag appears in that file and
in `docker-compose.yml`; change both together.
