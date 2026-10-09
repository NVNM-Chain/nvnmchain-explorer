# Runbook: the explorer on Postgres

How to bring up, switch to, and look after the split deployment
(`deploy/k8s/`). The design is
`docs/superpowers/specs/2026-10-03-postgres-backend-design.md`.

## Cutover, side by side

The Postgres stack is built next to today's deployment and synced from the
chain; then traffic moves. Every process connects to exactly one database,
and nothing reads the old SQLite file and Postgres together.

1. **Leave production as it is.** Today's deployment keeps its image, its
   SQLite file and all traffic.
2. **Deploy `explorer-indexer`** against an empty database (see
   `deploy/k8s/README.md` for users, grants and the private network). It passes its preflight,
   takes the lock, applies the migrations and re-indexes: forward from the
   head, backfill down to block 1, and the genesis, anchoring and
   missing-metadata jobs. Expect about 3 h for the ~2.3M blocks of 2026-10-05
   at ~200 blocks/s, plus about 15 min per later day. The node must be an
   archive node: the re-index needs receipts back to block 1, `eth_call` at
   block 0 and `eth_getLogs` over the whole range.
3. **Deploy `explorer-web`** at any time after that. Its pods turn Ready once
   the indexer has migrated and get no public traffic until the switch. Reach
   them through an internal load balancer or Ingress, never a
   `kubectl port-forward`, which adds its own latency.
4. **Wait for the indexer to report synced** in its `/readyz` body (read from
   memory, never from the database):

   ```json
   "sync": {"lowest_block": 1, "tip_lag": 0, "genesis": true, "anchoring": true, "complete": true}
   ```

   `complete` needs `lowest_block` 1, `tip_lag` at most 16, a genesis pass
   that began after backfill reached block 1 and found nothing left, and the
   anchoring backfill finished. That backfill runs once per start: if it
   failed, `anchoring` stays false until the indexer restarts.
5. **Go/no-go**, just before the switch:
   - `sync.complete` is true;
   - no holes, with the web user's credentials (it only reads):
     `SELECT MIN(number) = 1 AND COUNT(*) = MAX(number) FROM blocks;`
     A hole is a block dropped as a content error and logged as "block N not
     written": investigate before switching;
   - `VACUUM (ANALYZE);` once, then one warm-up pass, then for every URL in
     the page list the new p95 is at most today's p95 + 20 ms, both measured
     from one probe host within the same hour (`oha -n 200 -c 1` per URL). On
     a miss, do not switch;
   - a spot check: a few old blocks, transactions, addresses and tokens show
     the same data on both deployments.

   **Page list:** `/`, `/blocks`, `/txs`, `/tokens`, the block with the most
   transactions, a transaction on its second view, the address with the most
   transactions at page 1 and page 400 plus its `?tab=transfers`, and the
   token with the most transfers plus its `?tab=holders`. Pick the heaviest
   address and token by SQL on Postgres.
6. **Switch** traffic (DNS or Ingress) to `explorer-web`. `trace_data` and
   `selector_names` start empty and refill as pages are viewed.
7. **Fallback:** keep the old deployment running, and indexing, for 7 days.
   Rolling back is the reverse switch; no data moves either way. Then retire
   it with its SQLite file.

After the switch, every release rolls `explorer-indexer` first, then
`explorer-web` (`deploy/k8s/README.md`).

## Exit codes

| Code | Meaning                                                    | What to do                                                                 |
|------|------------------------------------------------------------|----------------------------------------------------------------------------|
| 1    | The preflight refused the database, or the config is wrong, including a `DATABASE_URL` that points at a replica (the server is in recovery) | Read the log line; see "Preflight refusals" below                          |
| 3    | Leadership lost: another session held the lock for 120 s   | Another writer holds the lock. Normal during a rollout; otherwise look for a second indexer |
| 4    | The database is not at this process's `writer_seq`         | Commits were lost (a restore, or a crash with `synchronous_commit=off`). The restart re-derives them; nothing to do |
| 5    | An indexer core task ended                                 | The pod restarts; read the log for why                                     |

## Preflight refusals

The indexer checks these before it asks for the lock, and exits 1 (never
Ready, so a broken image never replaces a working writer). It checks them
again once it holds the lock, since a newer release may have migrated while
it waited, and exits 1 the same way:

- **"no schema_migrations"**: the schema was created by hand. Drop it and
  re-index.
- **"has version N but not M"**: rows of `schema_migrations` were deleted or
  written by hand, so which files ran is unknown. Drop the schema and
  re-index.
- **"migration N was edited after it was applied"**: the image carries a
  changed migration file. Deploy an image whose files match; merged migrations
  never change.
- **"newer than this binary"** (D > B): an older image against a migrated
  database. Roll forward.

## Database restarts and maintenance

There is no HA: while the database is down, pages return 503 with
`Retry-After: 30`, the indexer waits (`explorer_writer_state` 2), and no block
is lost. Cloud SQL Enterprise Plus maintenance costs under about a second; a
single-instance CloudNativePG cluster is down while its pod restarts.

**Restart drill** (on the chosen target, after `sync.complete`): restart the
database under the indexer and a 5 rps page probe. Record the database's
downtime and the probe's first-to-last 503 window. It passes when pages go
503 then 200, the hole check finds no hole, no explorer pod's restart count
changed, and `explorer_writer_state` returns to 1. During backfill use the
range form of the hole check,
`SELECT COUNT(*) = MAX(number) - MIN(number) + 1 FROM blocks`.

**If the data itself is lost**, re-index into an empty database (step 2
above): pages show partial history until backfill completes.

## Password rotation

A container's environment is fixed when it starts. Change the password in the
database and in Secret Manager, wait for the synced Secret, then
`kubectl rollout restart` both deployments, indexer first. Open sessions
survive the change; new ones use the password the pod started with.
