# nvnmchain Explorer

Blockchain explorer for the nvnm chain — the Mantra canary EVM network
(chain id `0xc0316`) — with no native token (gas paid in ERC-20/TIP-20
tokens).

The UI is a dark, Blockscout-style dashboard: network stats with activity
sparklines, gas-utilization bars, method badges on transactions, token
holdings and holder counts, and decoded call/event views on transaction
pages.

Written in Rust with [axum](https://github.com/tokio-rs/axum) + [Tera](https://tera.netlify.app/),
SQLite (rusqlite) or Postgres (sqlx 0.9) with versioned migrations, an async
reqwest JSON-RPC client, a self-contained ABIv2 decoder with keccak, and a
tokio indexer (forward tip + backfill).

The default RPC is `https://rpc.nvnm.canary.mantrachain.dev` — the chain this
codebase is validated against. Point it anywhere with `NVNM_RPC` (the legacy
`TEMPO_RPC` variable is still accepted).

## Quick start

```bash
git submodule update --init contracts   # the anchoring ABI is compiled in from it
cargo run --release
```

Open http://localhost:8080. On first boot the indexer seeds from the chain
head, tracks new blocks continuously, and backfills history downward.

To run against a local node and the `docker compose` Postgres instead, copy
`.env.example` to `.env` first (see [Configuration](#configuration-env-vars)).

## Deploying to the cloud

On SQLite the explorer is a single long-running process (web server +
indexer, `ROLE=all`), so a good home is an always-on small VM or container with
a persistent disk. Platforms whose free tier sleeps (e.g. Render Free) would
pause the indexer and are unsuitable. The split deployment on Postgres, for
Kubernetes, is under [Kubernetes with Postgres](#kubernetes-with-postgres).

All of the options below give you a platform subdomain with TLS — no domain
registration needed. If you already own a domain you can point a subdomain at
any of them instead.

| Provider | Cost (approx.) | Subdomain | Notes |
|----------|----------------|-----------|-------|
| **Fly.io** (recommended) | ~$2/mo, free tier often covers it | `<app>.fly.dev` | Managed PaaS, 1 GB persistent volume, `fly.toml` included |
| **Railway** | $5/mo base (includes $5 usage) | `<app>.up.railway.app` | Same Dockerfile, volumes supported |
| **Render** | $7/mo Starter + ~$0.25/GB disk | `<app>.onrender.com` | `render.yaml` included; must be paid (always-on) |
| **Hetzner Cloud** | ~€4–5/mo VPS | your own or IP only | Full VM, real disk, `deploy/install.sh` + systemd included |
| **Oracle Cloud Always Free** | $0 | public IP only | 4-OCPU ARM VM; free forever but more setup + capacity limits |

### Managed PaaS (Fly.io)

```bash
fly auth login
fly launch --dockerfile Dockerfile --no-deploy   # creates the app
fly volumes create nvnm_data --size 5            # persistent disk for SQLite
fly deploy
```

The app listens on port 8080 and is served at `https://<app>.fly.dev`
(TLS automatic). Keep the machine always-on — `auto_stop_machines = false` is
already set in `fly.toml` because the indexer runs inside the web process.
To redeploy run `fly deploy`. SQLite data lives on the volume and survives
redeploys.

CI publishes the image to
`ghcr.io/yihuang/nvnmchain-explorer` (`latest` + `sha-<commit>` tags, and a
`<tag>` tag for `v*` releases). Managed platforms can run that image directly
instead of building from source.

Railway: create a service from this repo (Dockerfile), add a volume mounted at
`/data`, set `DB_PATH=/data/explorer.db`. Render: import `render.yaml`, pick
Starter, deploy.

### Kubernetes with Postgres

For availability and data that outgrows one machine, the same image runs as
two deployments against Postgres (Cloud SQL, or CloudNativePG in the cluster):
one `ROLE=indexer` writer and N `ROLE=web` replicas. Releases roll the indexer
first, then web. `deploy/k8s/README.md` has the manifests, the target setup and
the deploy order; `docs/runbook.md` has the side-by-side cutover from SQLite
and the operations guide.

### Persistence & schema migrations

- **Data survives redeploys** — SQLite lives on a persistent volume mounted at
  `/data` (`DB_PATH=/data/explorer.db`). Restarts and redeploys keep the
  database; the container entrypoint (`deploy/entrypoint.sh`) fixes volume
  ownership on boot so the app user can write to it regardless of how the
  provider mounts empty volumes.
- **Versioned migrations run automatically** — on boot, on both backends.
  The explorer refuses to start on a database newer than the binary, or on a
  drifted table, which it names. Legacy databases also get a one-time rebuild
  of the incremental token-balance table, and their anchoring events read
  back from the node's logs. See `docs/database.md`.
- **Volume sizing** — a full backfill of this chain is ~1.2 GB of raw block
  JSON before indexes and transactions. Use at least 2 GB; the examples use
  5 GB (~$0.75/mo on Fly), which leaves comfortable headroom.

### VPS (Hetzner, DigitalOcean, …)

```bash
sudo ./deploy/install.sh    # builds release binary + systemd service
```

Installs the binary to `/opt/nvnmchain-explorer`, creates a dedicated
`nvnmchain` user, and registers a hardened systemd unit
(`deploy/nvnmchain-explorer.service`) that restarts the process and keeps
SQLite under `/var/lib/nvnmchain-explorer`. Overrides live in
`/etc/nvnmchain-explorer.env` (see `deploy/nvnmchain-explorer.env.example`).

For the truly free option, Oracle's Always Free ARM VM (4 OCPU / 24 GB) runs
the same systemd setup — just remember the free public IP is the access point
unless you attach a domain.

The indexer is built for a sub-second chain:

- **Instant heads** — subscribes to `eth_subscribe("newHeads")` over WebSocket
  (`wss://ws.nvnm.canary.mantrachain.dev`); polling (`INDEX_POLL_SECONDS`)
  keeps the feed alive while the socket reconnects, so head detection never
  stalls. If the socket is unreachable (it currently does not answer from
  most networks), the indexer warns a few times, then retries silently on a
  long backoff while polling continues uninterrupted.
- **One RPC call per block** — receipts via `eth_getBlockReceipts` (with a
  per-transaction batch fallback), traces via `debug_traceBlockByNumber`, and
  blocks fetched concurrently (`INDEX_CONCURRENCY` in flight).
- **One writer** — every batch of blocks (block rows + txs + transfers +
  balances + token metadata) is persisted in one transaction by a single
  writer task. On SQLite that is a per-row write, measured at ~200 blocks/s
  while backfilling. On Postgres it is set-based, at most 14 round trips per
  64-block batch, on a session that holds an advisory lock, so only one
  process ever writes; a database restart is waited out and the batch
  retried, so no block is lost.
- **Cheap pages** — network stats (block time, TPS, gas utilization, 24h
  counts) are recomputed in the background into a `kv` row, and token
  balances/holder counts are maintained incrementally per block, so no page
  scans history at request time.

## Configuration (env vars)

For local development the explorer reads `.env` at startup; `ENV_FILE` names
another file, and an empty `ENV_FILE` reads none. A variable already set in the
environment wins over the file, and a value holding a `$` needs single quotes.
`.env.example` sets the variables for a local node and the `docker compose`
Postgres; copy it to `.env`, which git ignores. Deployments set the
environment themselves.

| Var | Default | Meaning |
|-----|---------|---------|
| `NVNM_RPC` | `https://rpc.nvnm.canary.mantrachain.dev` | JSON-RPC endpoint (legacy `TEMPO_RPC` also accepted) |
| `WS_URL` | `wss://ws.nvnm.canary.mantrachain.dev` | WebSocket endpoint for `newHeads` |
| `INDEX_WS` | `0` | `1` follows the WebSocket feed; unset or `0` polls only |
| `CHAIN_ID` | `787222` | Read, but nothing uses it yet |
| `DB_PATH` | `explorer.db` | SQLite database path |
| `DB_CACHE_KIB` | `524288` | SQLite page cache, in KiB |
| `DATABASE_URL` | unset | Postgres instead of SQLite; wins over `DB_PATH`. `postgres://…`, or `host:port[/db][?params]`; plaintext only, so keep the database on a private network |
| `PGUSER` / `PGPASSWORD` | unset | Postgres credentials, when the URL carries none; the password is never logged |
| `PGSSLMODE` | unset | Only `disable`, or unset; the URL's own `sslmode` wins over it. This build has no TLS |
| `ROLE` | `all` | `all` (web + indexer, one process), or `web` / `indexer` for the split deployment (Postgres only) |
| `DB_WEB_ROLE` | `explorer_web` | The web replicas' database user, which the indexer grants read and cache-write access |
| `FOLLOW_POLL_MS` | `500` | How often a `ROLE=web` replica polls for new blocks |
| `HOST` / `PORT` | `0.0.0.0` / `8080` | Bind address |
| `INDEX_POLL_SECONDS` | `1` | Poll interval when the WebSocket feed is unavailable |
| `INDEX_BATCH` | `32` | Blocks indexed per cycle (forward + backfill) |
| `INDEX_CONCURRENCY` | `32` | Blocks fetched in parallel |
| `NATIVE_SYMBOL` | `NVNM` | Symbol shown for native (burnt/gas) amounts; the Docker image sets `OM` |
| `STATS_INTERVAL_SECONDS` | `5` | How often the dashboard stats are recomputed |
| `RECENT_BLOCK_COUNT` | `15` | Rows in the dashboard's recent blocks |
| `RECENT_TX_COUNT` | `15` | Rows in the dashboard's recent transactions |
| `SIGNATURE_LOOKUP_URL` | OpenChain | Signature directory for selectors no built-in ABI declares; set empty to disable ([Decoding](#decoding)) |
| `RUST_LOG` | `nvnmchain_explorer=info` | Log verbosity |
| `ENV_FILE` | `.env` | The settings file read at startup; empty reads none, and a variable already set wins |

## Routes

| Path | Description |
|------|-------------|
| `/` | Dashboard (stats, recent blocks/txs) |
| `/block/{num\|hash}` | Block detail |
| `/blocks` | Block list |
| `/tx/{hash}` | Transaction detail (tabs: Overview/Balances/Calls/Events/Raw) |
| `/address/{addr}` | Address info (transactions, transfers, holdings, contract) |
| `/token/{addr}` | Token metadata, transfers, and holders |
| `/tokens` | Token list |
| `/anchoring` | Anchoring registries; `?q=` finds an id, a name or a checksum |
| `/anchoring/{registry}` | A registry and the latest version of each record |
| `/anchoring/{registry}/{record}` | A record's versions |
| `/search?q=...` | Smart redirect (block#/tx/address/token auto-detection) |
| `/api/search?q=...` | Suggestions for the search box, answered from the index |
| `/api/anchoring/search?q=...` | Suggestions for the anchoring page's field |
| `/api/events` | SSE live feed — pushes each newly indexed tip block (drives the home page's streaming "Latest Blocks" panel) |
| `/healthz` | Liveness; never touches the database |
| `/readyz` | Readiness, with the role, the schema versions, the writer's state and, on the indexer, its sync progress |
| `/metrics` | Prometheus metrics, under `ROLE=web` and `ROLE=indexer` only, for in-cluster scraping |

All data endpoints accept `?format=json` or `Accept: application/json`.

The home page subscribes to `/api/events` with `EventSource` and updates the
latest-blocks panel, the latest-block stat, and the block-time stat in real
time as blocks land — no client polling. Under `ROLE=all` the feed is
in-process: the indexer and the web server share one broadcast channel. Web
replicas (`ROLE=web`) get theirs from a polling follower, which reads new
blocks and stats from Postgres every `FOLLOW_POLL_MS`.

While Postgres is unreachable, pages answer `503` with `Retry-After: 30`,
never an empty page or a false "not found".

### Decoding

Calls, logs and reverts decode against the chain's own definitions: the
[tempo-contracts](https://github.com/NVNM-Chain/nvnmchain-tempo) bindings,
where `#[sol(abi)]` turns each `interface` into a JSON ABI at compile time —
an upstream change arrives with `cargo update`, and a rename fails to compile
rather than silently failing to decode. The few declarations with no binding
are Solidity signatures at the top of `src/decoder.rs`: a typo there does not
parse, and tests pin the selectors they hash to.

Multicall3, Permit2, CreateX and the anchoring contract at `0x…0a00` are not
Tempo's, so no binding carries them. The first three are vendored JSON under
`abi/`. Anchoring comes from the nvnmchain-contracts submodule at `contracts/`,
which generates `layout/anchoring.abi.json` beside the bytecode Tempo's genesis
uses, so the explorer cannot decode against an ABI the contract no longer has.

The anchoring pages read that contract over RPC, not the index: the corpus it
was seeded with at genesis emitted no events. So does the search box, for a whole
registry name or a record's checksum — all the contract matches. Any part of a name
matches only in the node's registry name index: start the node with
`--anchoring.name-index` and the box and the anchoring page ask it. A node without
it answers "method not found", which costs the box those rows and nothing else.

Each decoded log is also said in words, from the phrasing table in
`src/summary.rs`, and the transaction page leads with that sentence. Two tests
hold the table and the registry to each other, so a new event cannot land
unexplained.

A selector nothing declares is looked up once in a public signature directory
(OpenChain by default) and cached, misses included. An answer is believed only
when it hashes to the selector it was offered for, and is badged as the
stranger's name it is. Set `SIGNATURE_LOOKUP_URL=` (empty) and the explorer
talks to no third party.

## Known limitations

- **In the split deployment, a token with no mint, transfer or fee use yet is
  not listed or searchable**, even after its page is opened, until its first
  transfer is indexed. Its page still renders, from the node. Web replicas
  write no chain data, and the indexer learns of a token from its transfers.
  `ROLE=all` keeps saving a token when its page is viewed. The token-discovery
  change that follows reads the TIP-20 factory's `TokenCreated` logs and
  removes this.

## Indexer

Two background loops share a single writer task:

1. **Forward** — new blocks at the tip, driven by the WebSocket head feed (or
   polling fallback), indexed as soon as they appear.
2. **Backfill** — older blocks, descending (resumes from the lowest stored
   block after a restart, so an interrupted backfill is not abandoned).

For each block it stores the raw block plus indexed fields (base fee, size,
extra data, consensus epoch/view, proposer), every transaction (with receipt,
flattened call tree when tracing is available, and a method-id badge derived
from the call data), and decodes TIP-20 `Transfer` / `TransferWithMemo` events
into the `transfer_events` table so address and token transfer tabs have data.
Fees are derived from the Fee Manager transfer when the receipt omits
`feeAmount`. Token metadata (name, symbol, decimals, total supply) is fetched
via `eth_call` for fee tokens and transfer tokens alike (deduplicated, so each
token is fetched once), and token balances are applied incrementally so
holder counts and address holdings stay exact without rescanning history.

## Tests

```bash
# Unit and integration tests (no network)
cargo test --lib --test decoder --test anchoring --test pages \
    --test health --test migrations --test outage --test shutdown --test env_file

# Integration tests against the live chain RPC
cargo test --test live_rpc --test baseline
```

On Postgres, `docs/database.md` ("Running the Postgres tests") has the
commands for every suite: fixture replay into both backends, a differential
between them, a parity grid over every database function, migrations, locks,
and outage drills.

`tests/pages.rs` boots the HTTP API over a temp SQLite database and renders the
real templates, so a context key a handler stops sending fails a test rather
than a page view. Nothing in it reaches the network: the RPC points at a closed
port and the signature directory is stubbed. `tests/anchoring.rs` does the same
over a stub node that answers like the anchoring contract.

The live tests hit the RPC: they assert the chain id, fetch and index recent
blocks into a temp SQLite DB, and boot the HTTP API to verify the JSON
endpoints end to end.

## Layout

```
src/
  main.rs       entry point (server + indexer)
  config.rs     settings (RPC, WS, DB, indexer)
  rpc.rs        async JSON-RPC client
  ws.rs         WebSocket newHeads feed + polling fallback
  parse.rs      raw RPC → storage models
  db/           the async database API: mod.rs (dispatch, label cache),
                sqlite.rs, pg/ (Postgres), migrations.rs, config.rs
  follow.rs     the web role's polling live feed
  metrics.rs    Prometheus /metrics
  decoder.rs    ABI registry (from tempo-contracts) + decoder
  summary.rs    what a transaction did, in a sentence
  memo.rs       TIP-20 transfer memos
  signatures.rs names for selectors no built-in ABI declares
  name_search.rs  registry names from the node's name index, when it runs one
  tempo_address.rs  TIP-1022 virtual addresses
  contracts.rs  precompile / token labels
  anchoring.rs  the anchoring contract's views
  tokens.rs     token metadata + formatting
  indexer.rs    background indexing
  web.rs        axum routes + template helpers
abi/            ABIs for contracts no Tempo binding carries
migrations/     versioned schema changes, a SQLite and a Postgres twin per version
deploy/k8s/     the split deployment on Postgres
templates/      Tera templates
tests/          unit + page tests, and live RPC integration tests
```

## License

MIT
