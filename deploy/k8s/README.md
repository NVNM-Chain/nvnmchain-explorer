# Kubernetes deployment

The explorer runs as two deployments of one image, against Postgres:

- **`explorer-indexer`** (`indexer.yaml`): `ROLE=indexer`, one replica. The
  only writer: it holds a session-level advisory lock, runs the migrations and
  indexes the chain. Its HTTP port serves only `/healthz`, `/readyz` and
  `/metrics`.
- **`explorer-web`** (`web.yaml`): `ROLE=web`, N replicas behind a Service and
  an HPA. Pages, the live feed (a polling follower) and two cache writes. It
  never takes the lock.

Only the environment differs: `ROLE`, `DATABASE_URL` and `PGUSER` as plain
values, and `PGPASSWORD` from each deployment's own Secret
(`secrets.example.yaml`). The operations guide, with the cutover checklist, is
`docs/runbook.md`.

## Deploy order

**On every release, roll `explorer-indexer` first.** Wait until its `/readyz`
reports `"schema": {"db": B, "binary": B}`, then roll `explorer-web`.

- A new web replica is Ready only once the database is at its image's schema
  version (D ≥ B), so it waits for the indexer to migrate. The old replicas
  keep serving meanwhile.
- A schema change that removes something (a `contract` migration) ships one
  release after the code stops using it, so the previous release's web
  replicas never touch what it removes.
- Rolling back across a migration is not supported: roll forward. A database
  newer than an image (D > B) is refused by that image's indexer, which exits
  and never turns Ready.

The indexer's rollout starts the new pod first (`maxSurge: 1`,
`maxUnavailable: 0`). It turns Ready once its preflight passes and waits as a
candidate for the lock; Kubernetes then stops the old pod, whose session, and
lock, go with it. **No probe waits on the lock**, or every release carrying a
migration would deadlock.

## The Postgres target

A single primary, without HA: Cloud SQL for PostgreSQL, or Postgres in the
cluster (an operator such as CloudNativePG). PostgreSQL 15 or later.

|                     | Cloud SQL                                                                                              | CloudNativePG                                                                                                  |
|---------------------|--------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `DATABASE_URL` host | The instance's PSA DNS name, mapped to its private IP by a Cloud DNS private-zone record               | The primary's read-write Service, `<cluster>-rw.<ns>.svc`. **Never `-ro` or `-r`**, nor a PgBouncer `Pooler`   |
| TLS                 | None: the explorer connects in plaintext, over the private IP only; leave "Allow only SSL connections" off | None: plaintext over the ClusterIP Service                                                                     |
| Credentials         | Password users; `PGPASSWORD` from Secret Manager through a synced Secret                               | The operator-generated Secret's `username` and `password`                                                      |
| Not usable          | Managed Connection Pooling and the Auth Proxy: transaction pooling forbids session locks, and a proxy hides a dead client | The same, for the same reasons                                                                                 |

The writer refuses a replica: a session that is in recovery makes the
indexer exit with code 1 at once, naming the likely misconfiguration. An
unreachable database is never a reason to exit: the indexer waits for it,
however long it takes.

**No TLS.** The explorer's Postgres client is built without TLS: every
connection is plaintext, so the database must be reachable only over a
private network (Cloud SQL's private IP, or a ClusterIP Service). Any
`sslmode` but `disable`, in `DATABASE_URL` or `PGSSLMODE`, is refused at
start rather than quietly downgraded. That includes `prefer` and `allow`:
both ask for TLS where the server offers it, and this build would connect
them in plaintext. The URL's own `sslmode` wins over `PGSSLMODE`; set
neither.

**Users and grants.** Create two users; the indexer's owns the schema:

```sql
CREATE ROLE explorer_indexer LOGIN PASSWORD '…';
CREATE ROLE explorer_web LOGIN PASSWORD '…';
CREATE SCHEMA explorer AUTHORIZATION explorer_indexer;
ALTER ROLE explorer_indexer SET search_path = explorer;
ALTER ROLE explorer_web SET search_path = explorer;
-- Safe as role defaults, unlike idle_session_timeout (the writer's lease):
ALTER ROLE explorer_web SET statement_timeout = '5s';
ALTER ROLE explorer_indexer SET statement_timeout = '60s';
```

The indexer grants the web user what it needs after every start (`SELECT` on
every table, `INSERT, UPDATE` on `selector_names`, `UPDATE (trace_data)` on
`transactions`), naming it by `DB_WEB_ROLE` (default `explorer_web`). Never set
`idle_session_timeout` or `default_transaction_read_only` as a role default:
each connection sets its own.

**Connections.** The indexer holds about 6 (12 during its rollout); each web
pod about 10. Keep the HPA's `maxReplicas × 10 + 12` within the server's
`max_connections`.

## Probes and shutdown

- **Liveness** is `/healthz`, which never touches the database, so an outage
  restarts nothing. The port answers before the database opens.
- **Readiness** is `/readyz`. It reads the status the process publishes and
  never takes a connection. The indexer is Ready once its preflight passed;
  a web pod once the schema gate passed. Its body says why:
  `{role, schema: {db, binary}, writer, preflight, sync}`.
- **Shutdown** takes at most 3 s after SIGTERM. Web pods sleep 10 s in
  `preStop` first, so the endpoints drop them before they stop accepting.

## Metrics and alerts

Both deployments serve `/metrics` for in-cluster scraping (Google Managed
Prometheus `PodMonitoring`, or any Prometheus). The Ingress must not route it.

| Alert            | Rule                                                                              |
|------------------|-----------------------------------------------------------------------------------|
| Fetching behind  | `explorer_tip_lag_blocks > M` for T minutes                                       |
| Not leader       | `explorer_writer_state` is not 1 for 2 min                                        |
| No heartbeat     | `time() - explorer_writer_last_ok_seconds > 30`                                   |
| Stale            | `time() - explorer_latest_block_timestamp_seconds > X` for T minutes (web)        |
| 503 rate         | `rate(explorer_http_503_total[5m]) > R` (web)                                     |

`explorer_http_request_duration_seconds` (by route) keeps page p95 visible;
`explorer_schema_version{of="db"|"binary"}` shows D and B.

## Cloud Run

The same image runs on Cloud Run with min = max = 1 indexer instance and
instance-based billing (the writer must not be throttled between requests).
The web service's startup probe must be `/readyz` there, since Cloud Run's
startup probe gates revision traffic. No Cloud Run manifests are shipped.
