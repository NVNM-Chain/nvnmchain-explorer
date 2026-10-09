# Coding conventions

Rust project. CI (`.github/workflows/ci.yml`) enforces the baseline:

- `cargo fmt --check`
- `cargo clippy --all-targets -- -D warnings`

## Errors

Application code returns `anyhow::Result` and uses `anyhow::Context` to attach
context when propagating errors (see `src/main.rs`). Never swallow errors —
log them with `tracing` or propagate them.

## Tests

- No network: `cargo test --lib --test decoder --test anchoring --test pages --test health --test migrations --test outage --test shutdown --test env_file`
- Against the live chain RPC: `cargo test --test live_rpc --test baseline`
- Against Postgres: `docker compose up -d --wait`, then with
  `PG_TEST_URL=postgres://explorer:explorer@localhost:5432/explorer`:
  - the usual suites on Postgres: `TEST_DB=postgres cargo test --test decoder --test anchoring --test pages`
  - the Postgres suites: `cargo test --lib -- --include-ignored` and `cargo test --features db-coverage --test postgres --test migrations --test replay --test differential --test grid --test locks --test indexer_pg --test outage_drills -- --include-ignored`
  - the live re-index into Postgres: `TEST_DB=postgres cargo test --test baseline`
  - the restart drill, which restarts the container, so run it alone: `PG_CONTAINER=nvnmchain-explorer-postgres-1 cargo test --test restart_drill -- --include-ignored`

`docs/database.md` has the details, and how to add a schema change.
