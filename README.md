# nvnmchain Explorer

Blockchain explorer for the nvnm chain — the Mantra canary EVM network
(chain id `0xc0316`) — with no native token (gas paid in ERC-20/TIP-20
tokens).

The UI is a dark, Blockscout-style dashboard: network stats with activity
sparklines, gas-utilization bars, method badges on transactions, token
holdings and holder counts, and decoded call/event views on transaction
pages.

Written in Rust with [axum](https://github.com/tokio-rs/axum) + [Tera](https://tera.netlify.app/),
rusqlite (schema created on boot), an async reqwest JSON-RPC client, a
self-contained ABIv2 decoder with keccak, and a tokio indexer (forward tip +
backfill).

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

## Deploying to the cloud

The explorer is a single long-running process (web server + indexer) with a
SQLite database, so a good home is an always-on small VM or container with a
persistent disk. Platforms whose free tier sleeps (e.g. Render Free) would
pause the indexer and are unsuitable.

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

The app listens on port 8080 and is served at `https://nvnmchain-explorer.fly.dev`
(TLS automatic). Keep the machine always-on — `auto_stop_machines = false` is
already set in `fly.toml` because the indexer runs inside the web process.
Merging to `main` deploys automatically — CI builds the image on Fly's remote
builder and rolls it out (see `.github/workflows/docker.yml`). For a manual
redeploy run `fly deploy` (the token for CI lives in the repo secret
`FLY_API_TOKEN`, created with `fly tokens create deploy -x 2160h`). SQLite
data lives on the volume and survives redeploys.

CI also publishes the image to
`ghcr.io/yihuang/nvnmchain-explorer` (`latest` + `sha-<commit>` tags, and a
`<tag>` tag for `v*` releases). Managed platforms can run that image directly
instead of building from source.

Railway: create a service from this repo (Dockerfile), add a volume mounted at
`/data`, set `DB_PATH=/data/explorer.db`. Render: import `render.yaml`, pick
Starter, deploy.

### Persistence & schema migrations

- **Data survives redeploys** — SQLite lives on a persistent volume mounted at
  `/data` (`DB_PATH=/data/explorer.db`). Restarts and redeploys keep the
  database; the container entrypoint (`deploy/entrypoint.sh`) fixes volume
  ownership on boot so the app user can write to it regardless of how the
  provider mounts empty volumes.
- **Migrations run automatically** — on every boot `init_db` applies pending
  schema migrations tracked by SQLite's `PRAGMA user_version`. Deploying a new
  binary over an old database upgrades it in place (additive `ALTER TABLE`
  steps, logged at startup); legacy databases also get a one-time rebuild of
  the incremental token-balance table.
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
- **Serialized SQLite writes** — every block (block row + txs + transfers +
  balances + token metadata) is persisted in one transaction by a single
  writer task, measured at ~200 blocks/s while backfilling.
- **Cheap pages** — network stats (block time, TPS, gas utilization, 24h
  counts) are recomputed in the background into a `kv` row, and token
  balances/holder counts are maintained incrementally per block, so no page
  scans history at request time.

## Configuration (env vars)

| Var | Default | Meaning |
|-----|---------|---------|
| `NVNM_RPC` | `https://rpc.nvnm.canary.mantrachain.dev` | JSON-RPC endpoint (legacy `TEMPO_RPC` also accepted) |
| `WS_URL` | `wss://ws.nvnm.canary.mantrachain.dev` | WebSocket endpoint for `newHeads` |
| `INDEX_WS` | `1` | Set `0` to disable the WebSocket feed (pure polling) |
| `CHAIN_ID` | `787222` | Chain id shown in the UI |
| `DB_PATH` | `explorer.db` | SQLite database path |
| `HOST` / `PORT` | `0.0.0.0` / `8080` | Bind address |
| `INDEX_POLL_SECONDS` | `1` | Poll interval when the WebSocket feed is unavailable |
| `INDEX_BATCH` | `5` | Blocks indexed per cycle (forward + backfill) |
| `INDEX_CONCURRENCY` | `32` | Blocks fetched in parallel |
| `NATIVE_SYMBOL` | `OM` | Symbol shown for native (burnt/gas) amounts |
| `STATS_INTERVAL_SECONDS` | `5` | How often the dashboard stats are recomputed |
| `SIGNATURE_LOOKUP_URL` | OpenChain | Signature directory for selectors no built-in ABI declares; set empty to disable ([Decoding](#decoding)) |
| `NAME_SEARCH_URL` | unset | nvnmchain-anchoring's registry name index, so the search box takes any part of a name |
| `RUST_LOG` | `nvnmchain_explorer=info` | Log verbosity |

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
| `/anchoring` | Anchoring registries; `?q=` finds an id, an exact name or a checksum |
| `/anchoring/{registry}` | A registry and the latest version of each record |
| `/anchoring/{registry}/{record}` | A record's versions |
| `/search?q=...` | Smart redirect (block#/tx/address/token auto-detection) |
| `/api/search?q=...` | Suggestions for the search box, answered from the index |
| `/api/events` | SSE live feed — pushes each newly indexed tip block (drives the home page's streaming "Latest Blocks" panel) |

All data endpoints accept `?format=json` or `Accept: application/json`.

The home page subscribes to `/api/events` with `EventSource` and updates the
latest-blocks panel, the latest-block stat, and the block-time stat in real
time as blocks land — no client polling. The feed is in-process: run a single
instance (as the deploy configs do) so the indexer and the web server share
the same broadcast channel.

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
matches only in nvnmchain-anchoring's index: set `NAME_SEARCH_URL` and the box takes it
from there, at a request per keystroke.

Each decoded log is also said in words, from the phrasing table in
`src/summary.rs`, and the transaction page leads with that sentence. Two tests
hold the table and the registry to each other, so a new event cannot land
unexplained.

A selector nothing declares is looked up once in a public signature directory
(OpenChain by default) and cached, misses included. An answer is believed only
when it hashes to the selector it was offered for, and is badged as the
stranger's name it is. Set `SIGNATURE_LOOKUP_URL=` (empty) and the explorer
talks to no third party.

## Indexer

Two background loops share a single SQLite writer task:

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
cargo test --lib --test decoder --test anchoring --test pages

# Integration tests against the live chain RPC
cargo test --test live_rpc
```

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
  db.rs         SQLite layer
  decoder.rs    ABI registry (from tempo-contracts) + decoder
  summary.rs    what a transaction did, in a sentence
  memo.rs       TIP-20 transfer memos
  signatures.rs names for selectors no built-in ABI declares
  name_search.rs  registry names from nvnmchain-anchoring, when one is configured
  tempo_address.rs  TIP-1022 virtual addresses
  contracts.rs  precompile / token labels
  anchoring.rs  the anchoring contract's views
  tokens.rs     token metadata + formatting
  indexer.rs    background indexing
  web.rs        axum routes + template helpers
abi/            ABIs for contracts no Tempo binding carries
templates/      Tera templates
tests/          unit + page tests, and live RPC integration tests
```

## License

MIT
