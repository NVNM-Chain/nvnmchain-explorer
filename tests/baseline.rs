//! Checks that the explorer stores the same data as a saved baseline database.
//! It needs no network.
//!
//! There are two fixture files. `record_baseline` writes both from the live chain:
//!
//! - `fixtures/baseline/canary-rpc.json`: the recorded answers. It has every
//!   JSON-RPC answer the explorer got for the blocks in `RANGES`.
//! - `fixtures/baseline/canary.db`: the baseline database. It is what the
//!   explorer wrote from those answers.
//!
//! # How the test works
//!
//! `replay_reproduces_baseline` does these steps:
//!
//! 1. It starts a fake node on a local port. The fake node answers from the recorded answers。
//!    If the explorer asks for something that was not recorded, the test fails and names the request.
//! 2. It runs the explorer's writes into a new, empty database (see `index`).
//!    It saves the blocks in `RANGES` and runs the genesis balance pass.
//! 3. It checks that every table in the baseline has rows, and every column has at least one value
//!    that is not NULL. If not, the test fails, because an empty table or column checks nothing.
//!    The only exceptions are listed in `UNEXERCISED`, each with a reason.
//! 4. It compares every table in the 2 databases row by row (see `compare`). There is no list of
//!    tables to compare: it reads the table names from SQLite, so a new table is compared too.
//!    Columns in the `EXCLUDED_COLUMNS` are excluded because they are the clock at write time.
//!
//! If anything is different, the explorer now stores different data.
//! A refactor or a new database backend must not do that.
//!
//! # Table coverage
//!
//! The rows in the current baseline, and the code that writes them:
//!
//! | Table              | Rows | Written by                                       |
//! |--------------------|------|--------------------------------------------------|
//! | `blocks`           |  407 | saving blocks                                    |
//! | `transactions`     |   61 | saving blocks: legacy, EIP-1559 and 0x76 transactions; 1 failed, 3 contract deploys |
//! | `transfer_events`  |   86 | saving blocks: `Transfer` logs in receipts       |
//! | `anchoring_events` |   26 | saving blocks: logs from the anchoring contract  |
//! | `token_metadata`   |    3 | saving blocks: token name, symbol, supply, holder count |
//! | `token_balances`   |   30 | saving blocks, then the genesis balance pass     |
//! | `genesis_balances` |   29 | genesis balance pass: each holder's balance at block 0 |
//! | `counters`         |    2 | saving blocks: block and transaction counts      |
//! | `kv`               |    1 | genesis balance pass: `genesis_balances_cursor`  |
//! | `sqlite_sequence`  |    1 | SQLite, for the `transfer_events` ids            |
//!
//! Not covered:
//!
//! - `transactions.trace_data` and `selector_names`: see `UNEXERCISED`.
//! - The `kv` keys the live loops write (`stats`, `anchoring_backfilled_to`).
//!   Their values depend on the clock or on the chain's current head, so they
//!   change on every run.
//!
//! # Recording
//!
//! This command writes the baseline database again with the current code. It
//! keeps the answers already recorded and only records the missing ones:
//!
//! ```text
//! cargo test --test baseline record_baseline -- --ignored --nocapture
//! ```
//!
//! It only asks the chain if you delete `canary-rpc.json` first. `NVNM_RPC`
//! picks the node. Canary was reset on 2026-10-09, so the recorded blocks, and
//! the contract in `RANGES`, are no longer on the chain.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use futures_util::{stream, StreamExt, TryStreamExt};
use nvnmchain_explorer::config::DEFAULT_RPC_URL;
use nvnmchain_explorer::db::{self, Db};
use nvnmchain_explorer::indexer::fetch_block_bundle;
use nvnmchain_explorer::rpc::ChainRpc;
use nvnmchain_explorer::tokens::balances_at_genesis;
use rusqlite::types::Value as Sql;
use rusqlite::{Connection, OpenFlags};
use serde_json::{json, Value};

/// Canary's first Transfer (102504); 26 anchoring writes, the failed tx at
/// 105627 and three contract deploys; a TIP-20 token's creation, role grant
/// and first mint; legacy, EIP-1559 and 0x76 transactions side by side; and
/// every transaction touching contract 0xDF0555AFd6573Ad1426ACa5f834A3E1bfee71a49
/// up to block 3033001: its CREATE2 deploy and two 0x76 calls that emit its event.
const RANGES: [RangeInclusive<u64>; 7] = [
    102_500..=102_510,
    105_465..=105_640,
    1_419_680..=1_419_680,
    1_442_885..=1_442_930,
    1_445_775..=1_445_945,
    1_549_393..=1_549_393,
    1_561_424..=1_561_424,
];

/// Blocks fetched at once, and blocks per commit.
const CONCURRENCY: usize = 16;
const COMMIT: usize = 64;

/// Wall-clock write times: the only values two runs over the same answers
/// may disagree on.
const EXCLUDED_COLUMNS: &[&str] = &["created_at", "updated_at", "fetched_at"];

/// What the baseline leaves empty, and why: writes no recorded answer drives.
const UNEXERCISED: &[(&str, &str)] = &[
    (
        "transactions.trace_data",
        "a page keeps the trace it fetches in memory",
    ),
    (
        "selector_names",
        "a page keeps the names it looks up in memory",
    ),
];

/// JSON-RPC's code for a reverted call: the chain's answer, not a node failure.
const REVERT: i64 = 3;

/// The most differences printed; the count is always complete.
const SHOWN: usize = 25;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/baseline")
        .join(name)
}

/// The database under test. A Postgres backend opens here once `db` has one.
fn open_db(path: &str) -> Db {
    Arc::new(Mutex::new(db::init_db(path).expect("open database")))
}

/// What a recorded call is looked up by: its method and params, not its id.
fn call_key(call: &Value) -> String {
    format!(
        "{} {}",
        call["method"].as_str().unwrap_or(""),
        call["params"]
    )
}

/// Recorded answers, each a response without its id: `{"result": ...}` or
/// `{"error": ...}`.
type Tape = BTreeMap<String, Value>;

#[derive(Clone)]
struct Node {
    tape: Arc<Mutex<Tape>>,
    /// Recording: where calls missing from the tape are forwarded.
    upstream: Option<String>,
    /// Replaying: calls missing from the tape.
    missed: Arc<Mutex<BTreeSet<String>>>,
    client: reqwest::Client,
}

impl Node {
    /// Ask upstream for `calls`, retrying what a public node drops, and keep
    /// the first answer to each: a later run is given exactly what this one was.
    async fn record(&self, url: &str, calls: Vec<Value>) -> Result<(), StatusCode> {
        let mut wait = Duration::from_millis(250);
        for _ in 0..6 {
            let sent = self.client.post(url).json(&calls).send().await;
            if let Ok(resp) = sent.and_then(|r| r.error_for_status()) {
                if let Ok(Value::Array(items)) = resp.json::<Value>().await {
                    // All or nothing: a missing answer, or an error that is
                    // not a revert, is asked again.
                    let answers: Option<Vec<Value>> = (0..calls.len())
                        .map(|i| {
                            let item = items.iter().find(|r| r["id"] == json!(i))?;
                            match item.get("error").filter(|e| !e.is_null()) {
                                None => Some(json!({ "result": item["result"] })),
                                Some(e) if e["code"] == REVERT => Some(json!({ "error": e })),
                                Some(_) => None,
                            }
                        })
                        .collect();
                    if let Some(answers) = answers {
                        let mut tape = self.tape.lock().unwrap();
                        for (call, answer) in calls.iter().zip(answers) {
                            tape.entry(call_key(call)).or_insert(answer);
                        }
                        return Ok(());
                    }
                }
            }
            tokio::time::sleep(wait).await;
            wait *= 2;
        }
        Err(StatusCode::BAD_GATEWAY)
    }
}

async fn answer(
    State(node): State<Node>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let calls = match &body {
        Value::Array(calls) => calls.clone(),
        call => vec![call.clone()],
    };
    if let Some(url) = &node.upstream {
        let missing: Vec<Value> = {
            let tape = node.tape.lock().unwrap();
            calls
                .iter()
                .filter(|c| !tape.contains_key(&call_key(c)))
                .enumerate()
                .map(|(i, c)| json!({"jsonrpc": "2.0", "id": i, "method": c["method"], "params": c["params"]}))
                .collect()
        };
        if !missing.is_empty() {
            node.record(url, missing).await?;
        }
    }
    let tape = node.tape.lock().unwrap();
    let out: Vec<Value> = calls
        .iter()
        .map(|call| {
            let key = call_key(call);
            let mut response = tape.get(&key).cloned().unwrap_or_else(|| {
                node.missed.lock().unwrap().insert(key.clone());
                json!({ "error": { "code": -32000, "message": format!("not recorded: {key}") } })
            });
            response["jsonrpc"] = json!("2.0");
            response["id"] = call["id"].clone();
            response
        })
        .collect();
    Ok(Json(match body {
        Value::Array(_) => Value::Array(out),
        _ => out.into_iter().next().unwrap_or(Value::Null),
    }))
}

/// A node on a local port answering from `tape`, and forwarding what it lacks
/// upstream when recording. The chain is only asked for a tape started empty,
/// so one tape never mixes answers from two chains.
async fn serve(tape: Tape, recording: bool) -> (Node, String) {
    let upstream = std::env::var("NVNM_RPC").unwrap_or_else(|_| DEFAULT_RPC_URL.to_string());
    let node = Node {
        upstream: (recording && tape.is_empty()).then_some(upstream),
        tape: Arc::new(Mutex::new(tape)),
        missed: Arc::default(),
        client: reqwest::Client::new(),
    };
    let app = Router::new()
        .route("/", post(answer))
        .with_state(node.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    (node, url)
}

/// Write into `db` what the explorer writes for `RANGES`, from the node at
/// `url`: the blocks, fetched concurrently and written in ascending order; the
/// genesis pass, which fills `genesis_balances` and adds each holder's block-0
/// balance to `token_balances`.
async fn index(url: &str, db: &Db) -> anyhow::Result<()> {
    let rpc = ChainRpc::new(url)?;
    let numbers: Vec<u64> = RANGES.iter().cloned().flatten().collect();
    let mut bundles = stream::iter(numbers)
        .map(|n| {
            let rpc = &rpc;
            async move {
                let bundle = fetch_block_bundle(rpc, n).await?;
                bundle.with_context(|| format!("block {n}: no bundle"))
            }
        })
        .buffered(CONCURRENCY)
        .try_chunks(COMMIT);
    while let Some(chunk) = bundles.next().await {
        db::save_block_bundles(db, &chunk.map_err(|e| e.1)?)?;
    }
    // `indexer::add_genesis_balances`, which is private.
    while let Some((cursor, holders)) = db::holders_without_genesis_balance(db, 1000)? {
        let balances = balances_at_genesis(&rpc, &holders).await?;
        let rows: Vec<_> = holders.into_iter().zip(balances).collect();
        db::save_genesis_balances(db, &rows, cursor)?;
    }
    Ok(())
}

/// Every table's compared columns and rows, each row rendered and sorted.
fn dump(conn: &Connection) -> BTreeMap<String, (Vec<String>, Vec<String>)> {
    // Read the table names from SQLite instead of a fixed list, so a table
    // added to `init_db` is compared too.
    let tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let mut out = BTreeMap::new();
    for table in tables {
        let cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info(?1)")
            .unwrap()
            .query_map([&table], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .filter(|c: &String| !EXCLUDED_COLUMNS.contains(&c.as_str()))
            .collect();
        let list: Vec<String> = cols.iter().map(|c| format!("\"{c}\"")).collect();
        let mut stmt = conn
            .prepare(&format!("SELECT {} FROM \"{table}\"", list.join(", ")))
            .unwrap();
        let mut rows: Vec<String> = stmt
            .query_map([], |r| {
                let fields = cols
                    .iter()
                    .enumerate()
                    .map(|(i, col)| {
                        Ok(match r.get(i)? {
                            Sql::Blob(b) => format!("{col}=0x{}", hex::encode(b)),
                            value => format!("{col}={value:?}"),
                        })
                    })
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(fields.join(" "))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        rows.sort();
        out.insert(table, (cols, rows));
    }
    out
}

/// The baseline's empty tables and all-NULL columns `UNEXERCISED` does not
/// explain: what no write fills is not compared.
fn unexercised(conn: &Connection) -> Vec<String> {
    let mut out = Vec::new();
    for (table, (cols, rows)) in dump(conn) {
        let mut found = Vec::new();
        if rows.is_empty() {
            found.push(table.clone());
        } else {
            for col in cols {
                let set: i64 = conn
                    .query_row(
                        &format!("SELECT COUNT(*) FROM \"{table}\" WHERE \"{col}\" IS NOT NULL"),
                        [],
                        |r| r.get(0),
                    )
                    .unwrap();
                if set == 0 {
                    found.push(format!("{table}.{col}"));
                }
            }
        }
        out.extend(
            found
                .into_iter()
                .filter(|name| !UNEXERCISED.iter().any(|(n, _)| n == name)),
        );
    }
    out
}

/// Every difference between the baseline and the replayed database.
fn compare(baseline: &Connection, fresh: &Connection) -> Vec<String> {
    let (base, new) = (dump(baseline), dump(fresh));
    let mut diffs = Vec::new();
    let tables: BTreeSet<&String> = base.keys().chain(new.keys()).collect();
    for table in tables {
        let (Some((base_cols, base_rows)), Some((cols, rows))) = (base.get(table), new.get(table))
        else {
            let side = if base.contains_key(table) {
                "baseline"
            } else {
                "replay"
            };
            diffs.push(format!("{table}: only in the {side}"));
            continue;
        };
        if base_cols != cols {
            diffs.push(format!("{table}: columns were {base_cols:?}, now {cols:?}"));
            continue;
        }
        eprintln!("  {table}: {} row(s)", base_rows.len());
        let (base_set, set): (BTreeSet<_>, BTreeSet<_>) =
            (base_rows.iter().collect(), rows.iter().collect());
        diffs.extend(
            base_set
                .difference(&set)
                .map(|r| format!("{table}: only in the baseline: {r}")),
        );
        diffs.extend(
            set.difference(&base_set)
                .map(|r| format!("{table}: only in the replay: {r}")),
        );
    }
    diffs
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_reproduces_baseline() {
    let tape = std::fs::read(fixture("canary-rpc.json")).unwrap();
    let tape = serde_json::from_slice(&tape).expect("recorded answers");
    check_replay(tape, &fixture("canary.db")).await;
}

/// Replay `tape` into a fresh database and hold it to `baseline`.
async fn check_replay(tape: Tape, baseline: &Path) {
    let (node, url) = serve(tape, false).await;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("replay.db");
    let indexed = index(&url, &open_db(path.to_str().unwrap())).await;
    let missed = node.missed.lock().unwrap().clone();
    assert!(
        missed.is_empty(),
        "the indexer asked for answers never recorded; record the baseline again: {missed:?}"
    );
    indexed.expect("index the recorded answers");

    // Read-only and immutable: nothing here can migrate or repair the baseline.
    let baseline = Connection::open_with_flags(
        format!("file:{}?immutable=1", baseline.display()),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .expect("open baseline");
    let unfilled = unexercised(&baseline);
    assert!(
        unfilled.is_empty(),
        "the baseline leaves these empty; drive a write that fills them, or add them to UNEXERCISED with the reason: {unfilled:?}"
    );
    let diffs = compare(&baseline, &Connection::open(&path).unwrap());
    for d in diffs.iter().take(SHOWN) {
        eprintln!("  {d}");
    }
    assert!(
        diffs.is_empty(),
        "{} difference(s) from the baseline",
        diffs.len()
    );
}

/// Write `RANGES` through a recording stub that keeps the answers already on
/// the tape and asks upstream for the rest, then save the tape and the
/// database it wrote as the two fixtures.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "records the baseline from the live chain; run explicitly"]
async fn record_baseline() {
    let (tape_path, db_path) = (fixture("canary-rpc.json"), fixture("canary.db"));
    let tape: Tape = match std::fs::read(&tape_path) {
        Ok(bytes) => serde_json::from_slice(&bytes).expect("recorded answers"),
        Err(_) => Tape::new(),
    };
    eprintln!("recording {RANGES:?} onto {} answer(s)", tape.len());
    let (node, url) = serve(tape, true).await;

    // The database is derived from the tape: always written afresh, and
    // elsewhere first, so a recording that fails leaves the fixtures alone.
    let dir = tempfile::tempdir().unwrap();
    let recorded = dir.path().join("canary.db");
    let indexed = index(&url, &open_db(recorded.to_str().unwrap())).await;
    let missed = node.missed.lock().unwrap().clone();
    assert!(
        missed.is_empty(),
        "the chain is only asked for a tape started empty; delete it to record again: {missed:?}"
    );
    indexed.expect("index the recorded answers");
    // Self-contained: no WAL beside it, and no free pages.
    let conn = Connection::open(&recorded).unwrap();
    conn.pragma_update(None, "journal_mode", "DELETE").unwrap();
    conn.execute_batch("VACUUM").unwrap();
    for (table, (_, rows)) in dump(&conn) {
        eprintln!("  {table}: {} row(s)", rows.len());
    }
    eprintln!("  unexercised: {:?}", unexercised(&conn));

    // An answer that came too late for its caller is on the tape but not in
    // the database: the two must agree before they are kept.
    let tape = node.tape.lock().unwrap().clone();
    check_replay(tape.clone(), &recorded).await;

    // One answer per line, so a new recording diffs readably.
    let lines: Vec<String> = tape
        .iter()
        .map(|(key, answer)| format!("{}: {answer}", json!(key)))
        .collect();
    std::fs::create_dir_all(db_path.parent().unwrap()).unwrap();
    std::fs::write(&tape_path, format!("{{\n{}\n}}\n", lines.join(",\n"))).unwrap();
    std::fs::copy(&recorded, &db_path).unwrap();
    eprintln!("  {} answer(s) recorded", lines.len());
}
