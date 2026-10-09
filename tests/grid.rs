//! The API parity grid: every `db` function runs on both backends over the
//! same data, and the answers must agree.
//!
//! - **Exact**, where results are ordered by a unique key.
//! - **Sorted**, for the functions with no `ORDER BY`.
//! - **Tie-aware**, for the paged listings whose sort keys tie: each page's
//!   sequence of sort keys must be equal, and the pages together must hold the
//!   same rows. Postgres breaks ties by address; SQLite by index order.
//! - **Floats** in the stats within a relative 1e-9.
//!
//! With `--features db-coverage`, the run also fails if any function never
//! ran. Needs a Postgres server: ignored unless run with `--include-ignored`.

use std::collections::BTreeMap;

use nvnmchain_explorer::db::{self, Db, Role, TxColumns};
use nvnmchain_explorer::decoder::checksum_address;
use nvnmchain_explorer::models::{AnchoringEvent, Block, BlockBundle, Transaction, TransferEvent};
use nvnmchain_explorer::tokens::TokenMeta;
use rusqlite::Connection;
use serde_json::{json, Value};

#[path = "common/backend.rs"]
mod backend;
#[path = "common/bundles.rs"]
mod bundles;
#[allow(dead_code)]
mod common;
use common::baseline::open_fixture;

const TIED_TOKEN: &str = "0x20c0000000000000000000000000000000000abc";

fn holder(i: usize) -> String {
    checksum_address(&format!("0x{:040x}", 0x7000 + i))
}

fn meta(address: &str, symbol: &str, name: &str) -> TokenMeta {
    TokenMeta {
        address: checksum_address(address),
        name: name.into(),
        symbol: symbol.into(),
        decimals: 6,
        currency: "USD".into(),
        total_supply: "1".into(),
    }
}

/// 30 holders with equal balances of one token, across a 25-row page
/// boundary, and tokens whose holder counts tie, for the tie-aware rules.
fn tie_bundle() -> BlockBundle {
    let block = |number: i64| Block {
        number,
        hash: format!("0x{number:064x}"),
        parent_hash: format!("0x{:064x}", number - 1),
        timestamp: 1_800_000_000 + number,
        timestamp_ms: (1_800_000_000 + number) * 1000,
        gas_used: 21_000,
        gas_limit: 30_000_000,
        base_fee: "7".into(),
        size: 512,
        extra_data: String::new(),
        epoch: 1,
        view: 1,
        proposer: format!("0x{}", "33".repeat(20)),
        miner: format!("0x{}", "33".repeat(20)),
        tx_count: 1,
        created_at: 0,
    };
    let tx = |number: i64| Transaction {
        hash: format!("0x{:064x}", (1u64 << 50) + number as u64),
        block_number: number,
        position: 0,
        from_addr: holder(999),
        to_addr: Some(checksum_address(TIED_TOKEN)),
        status: 1,
        gas_used: 21_000,
        base_fee: "7".into(),
        contract_address: None,
        fee_token: Some(checksum_address(TIED_TOKEN)),
        fee_amount: "10".into(),
        input: "0xa9059cbb".into(),
        raw: Some("0x76aa".into()),
        trace_data: None,
        receipt_data: Some(r#"{"status":"0x1"}"#.into()),
        timestamp: 1_800_000_000 + number,
        created_at: 0,
    };
    let number = 9_000_000;
    let t = tx(number);
    let transfers = (0..30)
        .map(|i| TransferEvent {
            id: 0,
            tx_hash: t.hash.clone(),
            block_number: number,
            log_index: i as i64,
            token_addr: checksum_address(TIED_TOKEN),
            from_addr: "0x0000000000000000000000000000000000000000".into(),
            to_addr: holder(i),
            amount: "500".into(),
            timestamp: t.timestamp,
            created_at: 0,
        })
        .collect();
    BlockBundle {
        block: block(number),
        txs: vec![t],
        transfers,
        anchoring: vec![AnchoringEvent {
            tx_hash: format!("0x{:064x}", (1u64 << 50) + number as u64),
            block_number: number,
            log_index: 99,
            timestamp: 1_800_000_000 + number,
            event: "Anchored".into(),
            registry_id: 77,
            record_id: 1,
            caller: holder(999),
        }],
        tokens: vec![
            meta(TIED_TOKEN, "TIE", "Tied Token"),
            // Equal holder counts (0) for the token listing's ties, and the
            // characters a search must not take for patterns.
            meta(
                "0x20c0000000000000000000000000000000000ab1",
                "100%",
                "Percent_Token",
            ),
            meta(
                "0x20c0000000000000000000000000000000000ab2",
                "back\\slash",
                "Ümlaut Coin",
            ),
            meta(
                "0x20c0000000000000000000000000000000000ab3",
                "TIEX",
                "tie breaker",
            ),
        ],
    }
}

/// Results as JSON: compare exactly.
fn exact<T: serde::Serialize>(v: &T) -> Value {
    serde_json::to_value(v).unwrap()
}

/// Results with no order: compare as a sorted list.
fn sorted<T: serde::Serialize>(v: &[T]) -> Value {
    let mut items: Vec<String> = v
        .iter()
        .map(|x| without_clock(serde_json::to_value(x).unwrap()).to_string())
        .collect();
    items.sort();
    json!(items)
}

/// Pages whose sort keys tie: the key sequence of each page, and the rows of
/// all pages together as a sorted set.
fn tie_aware(pages: &[Vec<Value>], key: impl Fn(&Value) -> Value) -> Value {
    let keys: Vec<Vec<Value>> = pages.iter().map(|p| p.iter().map(&key).collect()).collect();
    let mut all: Vec<String> = pages.iter().flatten().map(|v| v.to_string()).collect();
    all.sort();
    json!({"keys": keys, "rows": all})
}

/// Wall-clock fields, which differ between any two writes, dropped wherever
/// they appear, inside serialized rows too.
fn without_clock(v: Value) -> Value {
    const CLOCK: [&str; 2] = ["created_at", "updated_at"];
    match v {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .filter(|(k, _)| !CLOCK.contains(&k.as_str()))
                .map(|(k, v)| (k, without_clock(v)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.into_iter().map(without_clock).collect()),
        Value::String(s) if s.starts_with('{') => match serde_json::from_str::<Value>(&s) {
            Ok(inner @ Value::Object(_)) => Value::String(without_clock(inner).to_string()),
            _ => Value::String(s),
        },
        other => other,
    }
}

/// What every function answers on one backend, by call.
async fn answers(db: &Db) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    let mut put = |k: &str, v: Value| {
        out.insert(k.to_string(), without_clock(v));
    };
    let latest = db::get_latest_block(db).await.expect("blocks");
    let tied = checksum_address(TIED_TOKEN);
    let first_holder = holder(0);
    let sender = holder(999);

    put(
        "get_kv",
        exact(&db::get_kv(db, "anchoring_backfilled_to").await),
    );
    put(
        "get_block_by_number",
        exact(&db::get_block_by_number(db, latest.number).await),
    );
    put(
        "get_block_by_hash",
        exact(&db::get_block_by_hash(db, &latest.hash).await),
    );
    put(
        "get_block_by_hash/upper",
        exact(&db::get_block_by_hash(db, &latest.hash.to_uppercase().replace("0X", "0x")).await),
    );
    put("get_latest_block", exact(&latest));
    put(
        "get_min_block_number",
        exact(&db::get_min_block_number(db).await),
    );
    put(
        "try_min_block_number",
        exact(&db::try_min_block_number(db).await.unwrap()),
    );
    put(
        "get_blocks_in_range",
        exact(&db::get_blocks_in_range(db, latest.number - 40, latest.number).await),
    );
    put(
        "get_recent_blocks",
        exact(&db::get_recent_blocks(db, 15).await),
    );
    let txs = db::get_transactions(db, 1, 100).await;
    put("get_transactions", exact(&txs));
    put(
        "get_transactions/2",
        exact(&db::get_transactions(db, 2, 7).await),
    );
    let some_tx = &txs[txs.len() / 2];
    put(
        "get_transaction",
        exact(&db::get_transaction(db, &some_tx.hash).await),
    );
    for cols in [TxColumns::List, TxColumns::Full] {
        let tag = matches!(cols, TxColumns::Full);
        put(
            &format!("get_transactions_in_range/{tag}"),
            exact(&db::get_transactions_in_range(db, 0, i64::MAX, cols).await),
        );
        put(
            &format!("get_block_transactions/{tag}"),
            exact(&db::get_block_transactions(db, some_tx.block_number, cols).await),
        );
        for page in 1..=3 {
            put(
                &format!("get_address_transactions/{tag}/{page}"),
                exact(&db::get_address_transactions(db, &some_tx.from_addr, page, 2, cols).await),
            );
        }
    }
    put(
        "get_transaction_count",
        exact(&db::get_transaction_count(db).await),
    );
    put(
        "get_address_transaction_count",
        exact(&db::get_address_transaction_count(db, &some_tx.from_addr).await),
    );
    put(
        "get_token_metadata",
        exact(&db::get_token_metadata(db, &tied).await),
    );
    let token_pages: Vec<Vec<Value>> = {
        let mut pages = Vec::new();
        for page in 1..=4 {
            pages.push(
                db::get_all_tokens(db, page, 2)
                    .await
                    .iter()
                    .map(exact)
                    .collect(),
            );
        }
        pages
    };
    put(
        "get_all_tokens",
        tie_aware(&token_pages, |t| t["holder_count"].clone()),
    );
    put(
        "get_all_token_metas",
        sorted(&db::get_all_token_metas(db).await),
    );
    put(
        "try_all_token_metas",
        sorted(&db::try_all_token_metas(db).await.unwrap()),
    );
    put("get_token_count", exact(&db::get_token_count(db).await));
    put(
        "get_token_transfer_count",
        exact(&db::get_token_transfer_count(db, &tied).await),
    );
    for q in [
        "tie",
        "TIE",
        "Tied Token",
        "percent_token",
        "ümlaut coin",
        "nothing",
        "a\0b",
    ] {
        put(
            &format!("get_token_by_symbol_or_name/{q:?}"),
            exact(&db::get_token_by_symbol_or_name(db, q).await),
        );
    }
    for q in [
        "ti",
        "TIE",
        "%",
        "100%",
        "_",
        "t_ken",
        "\\",
        "back\\slash",
        "ümlaut",
        "Ü",
        "a\0",
        "x",
    ] {
        let found: Vec<Value> = db::search_tokens(db, q, 10)
            .await
            .iter()
            .map(exact)
            .collect();
        put(
            &format!("search_tokens/{q:?}"),
            tie_aware(&[found], |t| json!([t["holder_count"], t["symbol"]])),
        );
    }
    put(
        "get_anchoring_events",
        exact(&db::get_anchoring_events(db, 77, 10).await),
    );
    put(
        "get_anchoring_events/fixture",
        exact(&db::get_anchoring_events(db, 1, 100).await),
    );
    put(
        "holders_without_genesis_balance",
        exact(&db::holders_without_genesis_balance(db, 50).await.unwrap()),
    );
    for page in 1..=2 {
        put(
            &format!("get_token_transfers/{page}"),
            exact(&db::get_token_transfers(db, &tied, page, 25).await),
        );
        put(
            &format!("get_address_transfers/{page}"),
            exact(&db::get_address_transfers(db, &first_holder, page, 25).await),
        );
    }
    put(
        "get_address_transfer_count",
        exact(&db::get_address_transfer_count(db, &first_holder).await),
    );
    let mut holder_pages = Vec::new();
    for page in 1..=3 {
        holder_pages.push(
            db::get_token_holders(db, &tied, page, 25)
                .await
                .iter()
                .map(|h| json!(h))
                .collect::<Vec<Value>>(),
        );
    }
    put(
        "get_token_holders",
        tie_aware(&holder_pages, |h| h[1].clone()),
    );
    put(
        "get_selector_names",
        exact(
            &db::get_selector_names(
                db,
                &[
                    "0xa9059cbb".into(),
                    "0xDEADBEEF".into(),
                    "0x00000000".into(),
                ],
                0,
            )
            .await,
        ),
    );
    put(
        "get_tokens_metadata",
        exact(&db::get_tokens_metadata(db, &[tied.clone(), sender.clone()]).await),
    );
    put(
        "get_all_token_addresses",
        sorted(&db::get_all_token_addresses(db).await),
    );
    put(
        "get_token_holder_count",
        exact(&db::get_token_holder_count(db, &tied).await),
    );
    put(
        "get_address_holdings",
        sorted(&db::get_address_holdings(db, &first_holder).await),
    );
    put(
        "tokens_missing_metadata",
        sorted(&db::tokens_missing_metadata(db).await.unwrap()),
    );
    let (newest, _stats_written_at, version) = db::follow_point(db).await.unwrap();
    put("follow_point", json!([newest, version]));
    put(
        "compute_and_store_stats",
        db::compute_and_store_stats(db).await.unwrap(),
    );
    out
}

/// The writes, the same on both backends, before the reads.
async fn load(db: &Db, fixture: &Connection) {
    for chunk in bundles::bundles(fixture).chunks(64) {
        db::save_block_bundles(db, chunk).await.unwrap();
    }
    let (genesis, cursor) = bundles::genesis(fixture);
    db::save_genesis_balances(db, &genesis, cursor)
        .await
        .unwrap();
    let tie = tie_bundle();
    db::save_block_bundle(db, &tie).await.unwrap();
    db::save_block(
        db,
        &Block {
            number: 9_000_100,
            hash: format!("0x{:064x}", 9_000_100),
            ..tie.block.clone()
        },
    )
    .await
    .unwrap();
    db::save_transaction(
        db,
        &Transaction {
            trace_data: None,
            ..tie.txs[0].clone()
        },
    )
    .await
    .unwrap();
    db::set_trace(db, &tie.txs[0].hash, "{\"calls\":[]}")
        .await
        .unwrap();
    db::set_chain_head(db, 9_000_200).await;
    db::save_selector_names(
        db,
        &[
            ("0xA9059CBB".into(), "transfer(address,uint256)".into()),
            ("0xdeadbeef".into(), String::new()),
        ],
    )
    .await
    .unwrap();
    db::save_token_metadata(
        db,
        &meta("0x20c0000000000000000000000000000000000ab4", "SOLO", "Solo"),
    )
    .await
    .unwrap();
    let wrote = db::save_anchoring_window(db, "anchoring_backfilled_to", "9000300", |stamp| {
        stamp(9_000_000)
            .map(|ts| AnchoringEvent {
                tx_hash: format!("0x{:064x}", 4242),
                block_number: 9_000_000,
                log_index: 100,
                timestamp: ts,
                event: "Anchored".into(),
                registry_id: 77,
                record_id: 2,
                caller: holder(999),
            })
            .into_iter()
            .collect()
    })
    .await
    .unwrap();
    assert_eq!(wrote, 1);
    db::repair_derived_tables(db).await;
    db::keepalive(db).await;
}

#[tokio::test]
#[ignore = "needs PG_TEST_URL; see AGENTS.md"]
async fn every_function_answers_the_same_on_both_backends() {
    let fixture = open_fixture("fixtures/baseline/canary-rich.db");
    let dir = tempfile::tempdir().unwrap();
    let sqlite = db::open(dir.path().join("grid.db").to_str().unwrap())
        .await
        .unwrap();
    let (_scratch, url) = backend::scratch_schema().await;
    let pg = backend::open(&backend::pg_config(&url, Role::All)).await;

    load(&sqlite, &fixture).await;
    load(&pg, &fixture).await;
    let (a, b) = (answers(&sqlite).await, answers(&pg).await);
    let mut differ = Vec::new();
    for (call, va) in &a {
        if b.get(call).is_none_or(|vb| !agree(va, vb)) {
            differ.push(format!(
                "{call}:\n    SQLite   {}\n    Postgres {}",
                truncate(va),
                b.get(call).map(truncate).unwrap_or_default()
            ));
        }
    }
    assert!(
        differ.is_empty(),
        "{} call(s) differ:\n{}",
        differ.len(),
        differ.join("\n")
    );

    #[cfg(feature = "db-coverage")]
    {
        let hit = db::coverage::hits();
        let missed: Vec<&&str> = db::ALL_FNS.iter().filter(|f| !hit.contains(**f)).collect();
        assert!(missed.is_empty(), "never ran on both backends: {missed:?}");
    }
}

/// Whether two answers agree: floats within a relative 1e-9, everything else
/// exactly. Only the stats carry floats, and SQLite's SUM compensates while
/// Postgres's does not, so `gas_util_pct` can differ in its last bits.
fn agree(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) if x.is_f64() && y.is_f64() => {
            let (x, y) = (x.as_f64().unwrap(), y.as_f64().unwrap());
            (x - y).abs() <= 1e-9 * x.abs().max(y.abs())
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(x, y)| agree(x, y))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| agree(v, w)))
        }
        _ => a == b,
    }
}

/// The grid's comparison, no database needed: a float within a relative 1e-9
/// agrees even across a rounding boundary, and nothing else is loosened.
#[test]
fn stats_agree_within_a_relative_1e_9() {
    let sqlite =
        without_clock(json!({"gas_util_pct": 1.00000000049, "total_blocks": 5, "updated_at": 1}));
    let pg = |gas: f64, blocks: i64| {
        without_clock(json!({"gas_util_pct": gas, "total_blocks": blocks, "updated_at": 2}))
    };
    // 2e-11 apart, but either side of the tenth significant digit.
    assert!(agree(&sqlite, &pg(1.00000000051, 5)));
    // 1.5e-9 apart: past the tolerance.
    assert!(!agree(&sqlite, &pg(1.000000002, 5)));
    // Integers stay exact.
    assert!(!agree(&sqlite, &pg(1.00000000049, 6)));
}

fn truncate(v: &Value) -> String {
    let s = v.to_string();
    if s.len() > 600 {
        format!("{}…", &s[..600])
    } else {
        s
    }
}
