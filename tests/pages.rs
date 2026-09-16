//! The pages, end to end.
//!
//! These render the real templates against a real database, so a context key a
//! handler stops sending fails a test rather than a page view. Nothing reaches
//! the network: fixtures carry their own traces and failure reasons, the RPC
//! points at a closed port, and the signature directory is stubbed.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use nvnmchain_explorer::config::Settings;
use nvnmchain_explorer::db::{self, Db};
use nvnmchain_explorer::decoder::{checksum_address, keccak256, keccak_hex, TRANSFER_TOPIC};
use nvnmchain_explorer::models::{Block, BlockBundle, Transaction, TransferEvent};
use nvnmchain_explorer::signatures;
use nvnmchain_explorer::tokens::TokenMeta;
use serde_json::{json, Value};

const TOKEN: &str = "0x20c0000000000000000000000000000000000000";
const SENDER: &str = "0x1111111111111111111111111111111111111111";
const RECIPIENT: &str = "0x2222222222222222222222222222222222222222";
const TX_HASH: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const FAILED_TX_HASH: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const UNKNOWN_LOG_TX_HASH: &str =
    "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

fn temp_db(name: &str) -> (tempfile::TempDir, Db) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(name);
    let conn = db::init_db(path.to_str().unwrap()).expect("init_db");
    (dir, Arc::new(Mutex::new(conn)))
}

fn topic(address: &str) -> String {
    format!("0x{}{}", "00".repeat(12), address.trim_start_matches("0x"))
}

/// `transfer(to, amount)` calldata — the failing call's input, so the summary
/// has a function name to blame.
fn transfer_calldata(to: &str, amount: u128) -> String {
    format!(
        "0xa9059cbb000000000000000000000000{}{amount:064x}",
        to.trim_start_matches("0x")
    )
}

fn block() -> Block {
    Block {
        number: 100,
        hash: format!("0x{}", "ab".repeat(32)),
        parent_hash: format!("0x{}", "cd".repeat(32)),
        timestamp: 1_700_000_000,
        timestamp_ms: 1_700_000_000_000,
        gas_used: 21_000,
        gas_limit: 30_000_000,
        base_fee: "1000000000".into(),
        size: 512,
        extra_data: String::new(),
        epoch: 1,
        view: 1,
        proposer: format!("0x{}", "33".repeat(20)),
        miner: format!("0x{}", "33".repeat(20)),
        tx_count: 2,
        created_at: 0,
    }
}

/// A transaction carrying `receipt` and no trace to fetch.
fn transaction(hash: &str, status: i64, receipt: Value) -> Transaction {
    Transaction {
        hash: hash.into(),
        block_number: 100,
        position: 0,
        from_addr: checksum_address(SENDER),
        to_addr: Some(checksum_address(TOKEN)),
        status,
        gas_used: 21_000,
        base_fee: "0x3b9aca00".into(),
        contract_address: None,
        fee_token: Some(checksum_address(TOKEN)),
        fee_amount: "2500".into(),
        input: transfer_calldata(RECIPIENT, 1_500_000),
        raw: None,
        // Empty rather than absent: a missing trace sends the handler to the RPC.
        trace_data: Some("[]".into()),
        receipt_data: Some(receipt.to_string()),
        timestamp: 1_700_000_000,
        created_at: 0,
    }
}

/// A receipt whose logs are a fee transfer and the payment it paid for, with a
/// memo — the shape a real TIP-20 payment has.
fn successful_receipt() -> Value {
    json!({
        "status": "0x1",
        "effectiveGasPrice": "0x4a817c800",
        "logs": [
            {
                "address": TOKEN,
                "topics": [TRANSFER_TOPIC.as_str(), topic(SENDER), topic("0xfeEC000000000000000000000000000000000000")],
                "data": format!("0x{:064x}", 2_500u128),
                "logIndex": "0x0",
            },
            {
                "address": TOKEN,
                "topics": [
                    nvnmchain_explorer::decoder::TRANSFER_WITH_MEMO_TOPIC.as_str(),
                    topic(SENDER),
                    topic(RECIPIENT),
                    format!("0x{}{}", hex::encode("invoice 42"), "00".repeat(22)),
                ],
                "data": format!("0x{:064x}", 1_500_000u128),
                "logIndex": "0x1",
            },
        ],
    })
}

/// A receipt for a transfer that ran out of money, with the revert data the
/// node embedded in its message.
fn failed_receipt() -> Value {
    let args = ethers_core::abi::encode(&[
        ethers_core::abi::Token::Uint(1_000_000u64.into()),
        ethers_core::abi::Token::Uint(2_500_000u64.into()),
        ethers_core::abi::Token::Address(TOKEN.parse().unwrap()),
    ]);
    let selector = &keccak256(b"InsufficientBalance(uint256,uint256,address)")[..4];
    json!({
        "status": "0x0",
        "effectiveGasPrice": "0x4a817c800",
        "revertReason": format!(
            "execution reverted: 0x{}{}",
            hex::encode(selector),
            hex::encode(args)
        ),
        "logs": [],
    })
}

/// A receipt whose only log no built-in ABI describes.
fn unknown_log_receipt() -> Value {
    json!({
        "status": "0x1",
        "effectiveGasPrice": "0x4a817c800",
        "logs": [{
            "address": TOKEN,
            "topics": [keccak_hex(b"SomethingNobodyDeclared(uint256)")],
            "data": format!("0x{:064x}", 7u64),
            "logIndex": "0x0",
        }],
    })
}

/// How many transfers the sender is party to — more than one page of 25, so
/// a count taken from the current page is visibly wrong.
const TRANSFER_COUNT: i64 = 30;

/// A block of transfers out of the sender, one unit to each recipient, which
/// makes the holders list and its shares easy to state exactly. The sender
/// was never funded, so its balance goes negative — a non-holding.
fn transfer_bundle() -> BlockBundle {
    let mut block = block();
    block.number = 101;
    block.hash = format!("0x{}", "ef".repeat(32));
    let transfers = (0..TRANSFER_COUNT)
        .map(|i| TransferEvent {
            id: 0,
            tx_hash: TX_HASH.into(),
            block_number: 101,
            log_index: i,
            token_addr: checksum_address(TOKEN),
            from_addr: checksum_address(SENDER),
            to_addr: checksum_address(&format!("0x{:040x}", i + 1)),
            amount: "1000000".into(),
            timestamp: 1_700_000_000,
            created_at: 0,
        })
        .collect();
    BlockBundle {
        block,
        txs: Vec::new(),
        transfers,
        tokens: Vec::new(),
    }
}

/// A server over the fixture, with `stats` as what the indexer would have left.
async fn serve(stats: Value) -> (tempfile::TempDir, String) {
    use nvnmchain_explorer::web::{self, AppState};

    let (dir, db) = temp_db("pages.db");
    db::save_block(&db, &block()).expect("block");
    db::save_transaction(&db, &transaction(TX_HASH, 1, successful_receipt())).expect("tx");
    db::save_transaction(&db, &transaction(FAILED_TX_HASH, 0, failed_receipt()))
        .expect("failed tx");
    db::save_transaction(
        &db,
        &transaction(UNKNOWN_LOG_TX_HASH, 1, unknown_log_receipt()),
    )
    .expect("unknown-log tx");
    db::save_token_metadata(
        &db,
        &TokenMeta {
            address: checksum_address(TOKEN),
            name: "pathUSD".into(),
            symbol: "pathUSD".into(),
            decimals: 6,
            currency: "USD".into(),
            total_supply: "1000000000".into(),
        },
    )
    .expect("token");
    // More transfers than fit on a page, so the counts and the pager have
    // something to be wrong about.
    db::save_block_bundle(&db, &transfer_bundle()).expect("transfers");

    let mut cfg = Settings::from_env();
    // Nothing here may reach the network. The signature directory is exercised
    // against a stub of its own; the RPC points at a closed local port, so any
    // handler that falls back to it fails at once instead of calling the live
    // chain — which would make these tests both slow and non-deterministic.
    cfg.signature_lookup_url = None;
    cfg.rpc_url = "http://127.0.0.1:1".into();
    let tera = web::build_tera(db.clone()).expect("templates");
    let state = AppState {
        db,
        rpc: nvnmchain_explorer::rpc::ChainRpc::from_settings(&cfg).expect("rpc"),
        cfg,
        tera,
        block_events: tokio::sync::broadcast::channel(16).0,
        stats: Arc::new(std::sync::RwLock::new(stats)),
        shutdown: tokio::sync::watch::channel(false).1,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, web::app(state)).await;
    });
    (dir, format!("http://{addr}"))
}

async fn get_json(base: &str, path: &str) -> Value {
    reqwest::get(format!("{base}{path}&format=json"))
        .await
        .unwrap_or_else(|e| panic!("GET {path}: {e}"))
        .json()
        .await
        .unwrap_or_else(|e| panic!("GET {path} json: {e}"))
}

/// A neighbour is offered only where one is indexed. The fixture holds 100 and
/// 101 but not 99, which is what a backfilling index looks like, and a link to 99
/// would 404.
#[tokio::test]
async fn a_block_links_only_to_neighbours_that_are_indexed() {
    let (_dir, base) = serve(Value::Null).await;
    let page = get_json(&base, "/block/100?").await;

    assert_eq!(page["next_block"], json!(101));
    assert_eq!(page["previous_block"], Value::Null, "99 is not indexed");

    let html = reqwest::get(format!("{base}/block/100"))
        .await
        .expect("GET /block/100")
        .text()
        .await
        .expect("block html");
    assert!(html.contains("href=\"/block/101\""), "a link forward");
    assert!(html.contains("step-off"), "and a dead end back");
}

/// Newest first, and only the heights the index holds: the fixture has 100 and
/// 101 but not 99, and a row for a missing block would link to a 404.
#[tokio::test]
async fn the_blocks_listing_descends_from_the_tip_over_the_gaps() {
    let (_dir, base) = serve(Value::Null).await;
    let page = get_json(&base, "/blocks?").await;

    let numbers: Vec<i64> = page["blocks"]
        .as_array()
        .expect("blocks")
        .iter()
        .map(|b| b["number"].as_i64().expect("number"))
        .collect();
    assert_eq!(numbers, [101, 100], "newest first, and nothing below 100");
    assert_eq!(page["first_num"], json!(101));
    assert_eq!(page["last_num"], json!(100));

    // Each row carries the gas percentage the bar is drawn from; the block row
    // itself has no such column.
    assert!(
        page["blocks"][0]["gas_pct"].is_string(),
        "gas_pct rides on the row"
    );

    // `from` starts the walk somewhere other than the tip, which is how the
    // "Older →" link pages backwards.
    let older = get_json(&base, "/blocks?from=100").await;
    let numbers: Vec<i64> = older["blocks"]
        .as_array()
        .expect("blocks")
        .iter()
        .map(|b| b["number"].as_i64().expect("number"))
        .collect();
    assert_eq!(numbers, [100], "101 is above the requested start");
}

/// The chain-wide listing, which the home page's feed of the latest few cannot
/// page past. Rows carry the keys `txs.html` draws, and an empty page renders
/// rather than 404s.
#[tokio::test]
async fn the_transactions_listing_holds_every_indexed_transaction() {
    let (_dir, base) = serve(Value::Null).await;
    let page = get_json(&base, "/txs?").await;

    assert_eq!(
        page["total_txns"],
        json!(3),
        "every saved transaction counted"
    );
    assert_eq!(page["total_pages"], json!(1));

    let rows = page["transactions"].as_array().expect("transactions");
    let hashes: Vec<&str> = rows
        .iter()
        .map(|t| t["tx_hash"].as_str().expect("hash"))
        .collect();
    for expected in [TX_HASH, FAILED_TX_HASH, UNKNOWN_LOG_TX_HASH] {
        assert!(
            hashes.contains(&expected),
            "{expected} missing from the listing"
        );
    }
    // The fixture's three all sit in block 100, so only the keys the row draws
    // can be asserted, not their order.
    for row in rows {
        assert_eq!(row["tx_block"], json!(100));
        assert!(row["tx_from"].is_string(), "from address for the row");
        assert!(row["tx_method"].is_string(), "method badge for the row");
    }
    assert!(
        rows.iter().any(|t| t["tx_status"] == json!(0)),
        "the failed transaction is listed too"
    );

    let empty = get_json(&base, "/txs?page=2").await;
    assert!(empty["transactions"]
        .as_array()
        .expect("transactions")
        .is_empty());

    // The template renders, which the JSON context alone does not prove.
    let html = reqwest::get(format!("{base}/txs"))
        .await
        .expect("GET /txs")
        .text()
        .await
        .expect("body");
    assert!(html.contains("Transactions"), "the page has its heading");
    assert!(html.contains(TX_HASH), "a row reaches the rendered page");

    // The address tab draws the same macro, so one broken row would break both.
    let address = reqwest::get(format!("{base}/address/{SENDER}?tab=transactions"))
        .await
        .expect("GET /address")
        .text()
        .await
        .expect("body");
    assert!(
        address.contains(TX_HASH),
        "the address tab draws the same rows"
    );

    // The total is the indexer's count from the stats, not a scan per request;
    // the fixture above, with no stats yet, was the one case that is counted.
    let (_dir, counted) = serve(json!({ "total_txns": 7 })).await;
    let page = get_json(&counted, "/txs?").await;
    assert_eq!(page["total_txns"], json!(7), "the stats count is trusted");
}

/// Pressing Enter in the search box resolves to one destination, and the
/// candidates are tried in the order a reader means them. A block hash and a
/// transaction hash are the same shape, so only asking the index tells them
/// apart.
#[tokio::test]
async fn search_resolves_each_kind_of_identifier() {
    let (_dir, base) = serve(Value::Null).await;
    let hit = |page: &Value| {
        let m = &page["match"];
        (
            m["type"].as_str().unwrap_or("").to_string(),
            m["url"].as_str().unwrap_or("").to_string(),
        )
    };

    let height = get_json(&base, "/search?q=100").await;
    assert_eq!(hit(&height), ("block".into(), "/block/100".into()));

    let block_hash = format!("0x{}", "ab".repeat(32));
    let by_hash = get_json(&base, &format!("/search?q={block_hash}")).await;
    assert_eq!(hit(&by_hash), ("block".into(), "/block/100".into()));

    let tx = get_json(&base, &format!("/search?q={TX_HASH}")).await;
    assert_eq!(hit(&tx), ("transaction".into(), format!("/tx/{TX_HASH}")));

    let address = get_json(&base, &format!("/search?q={SENDER}")).await;
    assert_eq!(
        hit(&address),
        (
            "address".into(),
            format!("/address/{}", checksum_address(SENDER))
        )
    );

    // A name, which is neither a height nor a hash nor an address.
    let symbol = get_json(&base, "/search?q=pathUSD").await;
    assert_eq!(
        hit(&symbol),
        (
            "token".into(),
            format!("/token/{}", checksum_address(TOKEN))
        )
    );

    let nothing = get_json(&base, "/search?q=matchesnothing").await;
    assert_eq!(nothing["match"], Value::Null);
}

#[tokio::test]
async fn a_successful_transaction_says_what_it_did() {
    let (_dir, base) = serve(Value::Null).await;
    let page = get_json(&base, &format!("/tx/{TX_HASH}?")).await;

    // The fee transfer came first in the receipt; the payment is the point.
    assert_eq!(
        page["summary"]["headline"],
        json!("Send 1.5 pathUSD to 0x2222…2222.")
    );
    assert_eq!(page["summary"]["tone"], json!("success"));

    let events = page["events"].as_array().expect("events");
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["known"]["kind"], json!("fee transfer"));
    assert_eq!(events[0]["known"]["is_fee"], json!(true));
    assert_eq!(
        events[1]["known"]["headline"],
        json!("Send 1.5 pathUSD to 0x2222…2222")
    );
    // The memo is the reason TransferWithMemo exists; it must reach the page.
    assert_eq!(events[1]["known"]["note"], json!("invoice 42"));
    // And the decoded parameters stay underneath the sentence.
    assert_eq!(events[1]["name"], json!("TransferWithMemo"));
    assert_eq!(events[1]["params"][2]["value"], json!("1500000"));
}

#[tokio::test]
async fn a_failed_transaction_says_why() {
    let (_dir, base) = serve(Value::Null).await;
    let page = get_json(&base, &format!("/tx/{FAILED_TX_HASH}?")).await;

    assert_eq!(page["summary"]["tone"], json!("failure"));
    assert_eq!(
        page["summary"]["headline"],
        json!(
            "Transfer failed: insufficient pathUSD balance. \
               Available 1 pathUSD, required 2.5 pathUSD."
        )
    );
    assert_eq!(
        page["summary"]["error"],
        json!("insufficient pathUSD balance")
    );
}

#[tokio::test]
async fn the_fee_breakdown_splits_the_gas_cost() {
    let (_dir, base) = serve(Value::Null).await;
    let page = get_json(&base, &format!("/tx/{TX_HASH}?")).await;
    let fee = &page["fee_breakdown"];

    // 21,000 gas at 20 gwei, of which 1 gwei per gas was burnt as base fee.
    assert_eq!(fee["total_wei"], json!("420000000000000"));
    assert_eq!(fee["burnt_wei"], json!("21000000000000"));
    assert_eq!(fee["tip_wei"], json!("399000000000000"));
    // What was actually charged is a TIP-20 amount, not wei.
    assert_eq!(fee["charged"], json!("0.0025 pathUSD"));
}

/// Every template the transaction page can render must render.
#[tokio::test]
async fn every_transaction_tab_renders() {
    let (_dir, base) = serve(Value::Null).await;
    for hash in [TX_HASH, FAILED_TX_HASH] {
        for tab in ["overview", "balances", "calls", "events", "raw"] {
            let path = format!("/tx/{hash}?tab={tab}");
            let response = reqwest::get(format!("{base}{path}"))
                .await
                .unwrap_or_else(|e| panic!("GET {path}: {e}"));
            assert_eq!(response.status(), 200, "GET {path}");
            let body = response.text().await.expect("body");
            if tab == "overview" {
                assert!(body.contains("summary-headline"), "GET {path}");
            }
            if hash == TX_HASH && tab == "events" {
                assert!(body.contains("invoice 42"), "the memo is shown");
            }
        }
    }
}

/// A log no ABI describes must still list, rather than disappearing from a
/// page that claims to show every event.
#[tokio::test]
async fn an_unknown_log_still_appears() {
    let (_dir, base) = serve(Value::Null).await;
    let page = get_json(&base, &format!("/tx/{UNKNOWN_LOG_TX_HASH}?")).await;

    let events = page["events"].as_array().expect("events");
    assert_eq!(events.len(), 1);
    assert!(events[0]["name"].is_null(), "no ABI names this event");
    assert!(
        events[0].get("known").is_none(),
        "and nothing to say about it"
    );
    assert_eq!(events[0]["params"][0]["name"], json!("data"));
    // With nothing interpretable, the summary says exactly that.
    assert_eq!(page["summary"]["headline"], json!("Transaction succeeded."));
}

// ---------------------------------------------------------------------------
// Address and token pages
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_address_counts_every_transfer_not_just_the_page() {
    let (_dir, base) = serve(Value::Null).await;
    let sender = checksum_address(SENDER);

    // The count is of the whole history; a page holds 25 of them.
    for tab in ["transactions", "transfers"] {
        let page = get_json(&base, &format!("/address/{sender}?tab={tab}")).await;
        assert_eq!(
            page["transfer_count"],
            json!(TRANSFER_COUNT),
            "transfer count on the {tab} tab"
        );
        assert_eq!(page["tx_count"], json!(3), "tx count on the {tab} tab");
    }

    // And the pager knows there is a second page to reach.
    let transfers = get_json(&base, &format!("/address/{sender}?tab=transfers")).await;
    assert_eq!(transfers["total_pages"], json!(2));
    assert_eq!(transfers["html_transactions"].as_array().unwrap().len(), 25);

    let second = get_json(&base, &format!("/address/{sender}?tab=transfers&page=2")).await;
    assert_eq!(
        second["html_transactions"].as_array().unwrap().len(),
        (TRANSFER_COUNT - 25) as usize
    );
}

/// The address page merges what an address sent with what it received, newest
/// first, and a page boundary neither repeats nor skips a transaction.
#[test]
fn an_address_page_merges_sent_and_received_in_block_order() {
    let (_dir, db) = temp_db("address-order.db");
    let me = checksum_address(RECIPIENT);
    let other = checksum_address(TOKEN);
    // Two blocks, each with: one sent, one received, one to itself, one not its.
    for (block_number, byte) in [(100, "a1"), (101, "b2")] {
        let mut block = block();
        block.number = block_number;
        block.hash = format!("0x{}", byte.repeat(32));
        let parties = [(&me, &other), (&other, &me), (&me, &me), (&other, &other)];
        let txs = parties
            .iter()
            .enumerate()
            .map(|(position, (from, to))| {
                let hash = format!("0x{}{:02x}", byte.repeat(31), position);
                let mut tx = transaction(&hash, 1, successful_receipt());
                tx.block_number = block_number;
                tx.position = position as i64;
                tx.from_addr = (*from).clone();
                tx.to_addr = Some((*to).clone());
                tx
            })
            .collect();
        let bundle = BlockBundle {
            block,
            txs,
            transfers: Vec::new(),
            tokens: Vec::new(),
        };
        db::save_block_bundle(&db, &bundle).expect("save");
    }

    let pages: Vec<Vec<(i64, i64)>> = (1..=4)
        .map(|page| {
            db::get_address_transactions(&db, &me, page, 2)
                .iter()
                .map(|tx| (tx.block_number, tx.position))
                .collect()
        })
        .collect();
    assert_eq!(
        pages,
        [
            vec![(101, 2), (101, 1)],
            vec![(101, 0), (100, 2)],
            vec![(100, 1), (100, 0)],
            vec![],
        ]
    );
    assert_eq!(db::get_address_transaction_count(&db, &me), 6);
}

/// The transfers tab merges the two sides the same way.
#[test]
fn an_address_transfer_page_merges_sent_and_received_in_block_order() {
    let (_dir, db) = temp_db("transfer-order.db");
    let me = checksum_address(RECIPIENT);
    let other = checksum_address(SENDER);
    for (block_number, byte) in [(100, "a1"), (101, "b2")] {
        let mut block = block();
        block.number = block_number;
        block.hash = format!("0x{}", byte.repeat(32));
        let parties = [(&me, &other), (&other, &me), (&me, &me), (&other, &other)];
        let transfers = parties
            .iter()
            .enumerate()
            .map(|(log_index, (from, to))| TransferEvent {
                id: 0,
                tx_hash: TX_HASH.into(),
                block_number,
                log_index: log_index as i64,
                token_addr: checksum_address(TOKEN),
                from_addr: (*from).clone(),
                to_addr: (*to).clone(),
                amount: "1".into(),
                timestamp: 1_700_000_000,
                created_at: 0,
            })
            .collect();
        let bundle = BlockBundle {
            block,
            txs: Vec::new(),
            transfers,
            tokens: Vec::new(),
        };
        db::save_block_bundle(&db, &bundle).expect("save");
    }

    let pages: Vec<Vec<(i64, i64)>> = (1..=4)
        .map(|page| {
            db::get_address_transfers(&db, &me, page, 2)
                .iter()
                .map(|row| {
                    (
                        row["block_number"].as_i64().unwrap(),
                        row["log_index"].as_i64().unwrap(),
                    )
                })
                .collect()
        })
        .collect();
    assert_eq!(
        pages,
        [
            vec![(101, 2), (101, 1)],
            vec![(101, 0), (100, 2)],
            vec![(100, 1), (100, 0)],
            vec![],
        ]
    );
    assert_eq!(db::get_address_transfer_count(&db, &me), 6);
}

#[tokio::test]
async fn a_token_lists_its_holders() {
    let (_dir, base) = serve(Value::Null).await;
    let token = checksum_address(TOKEN);
    let page = get_json(&base, &format!("/token/{token}?tab=holders")).await;

    // Every recipient holds one unit; the sender's balance is negative, and a
    // negative balance is not a holding, so the list is exactly the recipients.
    assert_eq!(page["holders"], json!(TRANSFER_COUNT));
    let rows = page["holder_rows"].as_array().expect("holder rows");
    assert_eq!(rows.len(), 25, "one page of holders");
    assert_eq!(rows[0]["rank"], json!(1));
    assert_eq!(rows[0]["formatted"], json!("1"));
    // 1,000,000 of a 1,000,000,000 supply.
    assert_eq!(rows[0]["share"], json!("0.1000"));
    assert_eq!(page["total_pages"], json!(2));

    let second = get_json(&base, &format!("/token/{token}?tab=holders&page=2")).await;
    assert_eq!(
        second["holder_rows"].as_array().unwrap()[0]["rank"],
        json!(26)
    );
}

/// Addresses are copied all day; every page that shows one must offer it.
#[tokio::test]
async fn addresses_are_copyable_and_labelled() {
    let (_dir, base) = serve(Value::Null).await;
    let token = checksum_address(TOKEN);

    let body = reqwest::get(format!("{base}/tx/{TX_HASH}"))
        .await
        .expect("tx page")
        .text()
        .await
        .expect("body");
    assert!(
        body.contains(&format!("data-copy=\"{TX_HASH}\"")),
        "the transaction hash is copyable"
    );
    assert!(
        body.contains(&format!("data-copy=\"{token}\"")),
        "the addresses on it are copyable"
    );
    // And an address the chain has a name for carries it.
    assert!(body.contains("addr-tag"), "known addresses are tagged");
    assert!(body.contains(">pathUSD<"), "the token is named");
}

/// A precompile is named by the built-in table rather than by the database.
#[tokio::test]
async fn precompiles_are_labelled_without_a_database_row() {
    use nvnmchain_explorer::web::address_label;

    let (_dir, db) = temp_db("labels.db");
    assert_eq!(
        address_label(&db, "0xfeEC000000000000000000000000000000000000").as_deref(),
        Some("Fee Manager")
    );
    // A TIP-1022 deposit address says so — nothing else on the page would.
    let deposit = nvnmchain_explorer::tempo_address::virtual_address(&[0xab; 4], &[1; 6]);
    assert_eq!(
        address_label(&db, &deposit).as_deref(),
        Some("Virtual 0xabababab")
    );
    assert_eq!(
        address_label(&db, "0x1111111111111111111111111111111111111111"),
        None
    );
}

// ---------------------------------------------------------------------------
// Search suggestions
// ---------------------------------------------------------------------------

async fn suggest(base: &str, query: &str) -> Vec<Value> {
    let body: Value = reqwest::get(format!("{base}/api/search?q={}", urlencoding_encode(query)))
        .await
        .unwrap_or_else(|e| panic!("suggest {query}: {e}"))
        .json()
        .await
        .unwrap_or_else(|e| panic!("suggest {query} json: {e}"));
    body["results"].as_array().cloned().unwrap_or_default()
}

/// Minimal percent-encoding for the few characters the fixtures use.
fn urlencoding_encode(value: &str) -> String {
    value.replace('#', "%23").replace(' ', "%20")
}

#[tokio::test]
async fn the_search_box_suggests_what_is_being_typed() {
    let (_dir, base) = serve(Value::Null).await;

    // A token by name, before the whole name is typed.
    let by_name = suggest(&base, "path").await;
    assert_eq!(by_name[0]["type"], json!("token"));
    assert_eq!(by_name[0]["label"], json!("pathUSD"));
    assert_eq!(
        by_name[0]["url"],
        json!(format!("/token/{}", checksum_address(TOKEN)))
    );

    // A precompile by name, which no database row describes. Matched as a
    // substring, so a partial second word still finds it.
    for term in ["fee", "fee man", "manager"] {
        let precompile = suggest(&base, term).await;
        assert!(
            precompile
                .iter()
                .any(|r| r["label"] == json!("Fee Manager") && r["type"] == json!("precompile")),
            "`{term}` got {precompile:#?}"
        );
    }

    // A block number, and only one the chain has reached.
    let block = suggest(&base, "100").await;
    assert_eq!(block[0]["type"], json!("block"));
    assert_eq!(block[0]["url"], json!("/block/100"));
    assert!(suggest(&base, "999999").await.is_empty());
    // The same, typed the way a reader refers to a block.
    assert_eq!(suggest(&base, "#100").await[0]["url"], json!("/block/100"));

    // A transaction hash resolves whether or not it is indexed — being told
    // "not found" on the page beats being offered nothing.
    let indexed = suggest(&base, TX_HASH).await;
    assert_eq!(indexed[0]["type"], json!("transaction"));
    assert_eq!(indexed[0]["sublabel"], json!("Block #100"));
    let unknown = suggest(&base, &format!("0x{}", "12".repeat(32))).await;
    assert_eq!(unknown[0]["sublabel"], json!("Not indexed yet"));

    // An address is offered as itself; a token address as both.
    let token_address = suggest(&base, TOKEN).await;
    assert_eq!(token_address[0]["type"], json!("token"));
    assert_eq!(token_address[1]["type"], json!("address"));
    assert_eq!(token_address[1]["label"], json!("pathUSD"));

    assert!(suggest(&base, "").await.is_empty());
}

/// A TIP-1022 deposit address is searchable, and says what it is — nothing
/// else in the explorer would tell the reader that.
#[tokio::test]
async fn a_virtual_address_is_recognised_in_search() {
    let (_dir, base) = serve(Value::Null).await;
    let address = nvnmchain_explorer::tempo_address::virtual_address(&[0xab; 4], &[0xcd; 6]);

    let results = suggest(&base, &address).await;
    assert_eq!(results[0]["type"], json!("address"));
    assert_eq!(results[0]["label"], json!("Virtual 0xabababab"));
    assert_eq!(
        results[0]["sublabel"],
        json!("Virtual address · user tag 0xcdcdcdcdcdcd")
    );
}

/// A search term is bound, never interpolated: a wildcard a reader types is a
/// character to match, not a pattern that matches everything.
#[tokio::test]
async fn search_terms_are_never_patterns() {
    let (_dir, base) = serve(Value::Null).await;
    // Leaked wildcards would make these match pathUSD; escaped, they cannot.
    assert!(suggest(&base, "%path%").await.is_empty());
    assert!(suggest(&base, "_ath").await.is_empty());
    assert!(suggest(&base, "' OR 1=1 --").await.is_empty());
    // And one character matches too much to rank at all.
    assert!(suggest(&base, "p").await.is_empty());
}

// ---------------------------------------------------------------------------
// Signature directory
// ---------------------------------------------------------------------------

/// A stub signature directory, and how many times it was asked.
async fn stub_directory(answer: Value) -> (String, Arc<AtomicUsize>) {
    use axum::{routing::get, Json, Router};

    let hits = Arc::new(AtomicUsize::new(0));
    let handler = {
        let hits = hits.clone();
        move || {
            hits.fetch_add(1, Ordering::SeqCst);
            async move { Json(answer) }
        }
    };
    let app = Router::new().route("/lookup", get(handler));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}/lookup"), hits)
}

/// A call to a contract nothing built-in describes gets its name from the
/// directory — once, and only if the answer is really that selector's.
#[tokio::test]
async fn an_unknown_selector_is_named_by_the_directory() {
    // `redeem(address,uint256)` — a selector no Tempo interface declares.
    let signature = "redeem(address,uint256)";
    let selector = format!("0x{}", hex::encode(&keccak256(signature.as_bytes())[..4]));
    let (url, hits) = stub_directory(json!({
        "ok": true,
        "result": { "function": { selector.clone(): [{ "name": signature }] } },
    }))
    .await;

    let (_dir, db) = temp_db("signatures.db");
    let mut cfg = Settings::from_env();
    cfg.signature_lookup_url = Some(url);
    let client = reqwest::Client::new();

    let names = signatures::resolve(&db, &cfg, &client, std::slice::from_ref(&selector)).await;
    assert_eq!(names.get(&selector).map(String::as_str), Some(signature));
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    // The answer is cached, so a second page view asks nobody.
    let again = signatures::resolve(&db, &cfg, &client, std::slice::from_ref(&selector)).await;
    assert_eq!(again, names);
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    // And the name is enough to decode the call's arguments.
    let call = nvnmchain_explorer::decoder::decode_with_signature(
        &format!(
            "{selector}{}{:064x}",
            topic(RECIPIENT).trim_start_matches("0x"),
            42
        ),
        signature,
    )
    .expect("decoded with the learned signature");
    assert_eq!(call.name.as_deref(), Some("redeem"));
    assert_eq!(call.params[0].value, checksum_address(RECIPIENT));
    assert_eq!(call.params[1].value, "42");
}

/// A directory that answers with the wrong function is not believed, and a
/// selector it does not know is not asked about twice.
#[tokio::test]
async fn a_wrong_or_missing_answer_is_remembered_as_a_miss() {
    let selector = "0xdeadbeef".to_string();
    let (url, hits) = stub_directory(json!({
        "ok": true,
        // The right shape for a different function — a name that does not
        // hash to the selector it is offered for.
        "result": { "function": { selector.clone(): [{ "name": "transfer(address,uint256)" }] } },
    }))
    .await;

    let (_dir, db) = temp_db("bad-signatures.db");
    let mut cfg = Settings::from_env();
    cfg.signature_lookup_url = Some(url);
    let client = reqwest::Client::new();

    assert!(
        signatures::resolve(&db, &cfg, &client, std::slice::from_ref(&selector))
            .await
            .is_empty(),
        "a signature that does not hash to the selector must be refused"
    );
    signatures::resolve(&db, &cfg, &client, std::slice::from_ref(&selector)).await;
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "a miss is remembered, not re-asked"
    );
}

/// With no directory configured, nothing is asked and nothing breaks.
#[tokio::test]
async fn the_directory_can_be_turned_off() {
    let (_dir, db) = temp_db("no-directory.db");
    let mut cfg = Settings::from_env();
    cfg.signature_lookup_url = None;
    let names = signatures::resolve(
        &db,
        &cfg,
        &reqwest::Client::new(),
        &["0xdeadbeef".to_string()],
    )
    .await;
    assert!(names.is_empty());
}

// ---------------------------------------------------------------------------
// Contract details
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_token_address_shows_the_interface_it_exposes() {
    let (_dir, base) = serve(Value::Null).await;
    let token = checksum_address(TOKEN);
    let page = get_json(&base, &format!("/address/{token}?tab=contract")).await;

    assert_eq!(page["has_interface"], json!(true));
    assert_eq!(
        page["interface"]["abis"],
        json!(["tip20", "tip20_roles_auth"])
    );

    // Reads and writes are split by what the function actually does.
    let reads = page["interface"]["reads"].as_array().expect("reads");
    let writes = page["interface"]["writes"].as_array().expect("writes");
    assert!(reads.iter().any(|f| f["name"] == json!("balanceOf")));
    assert!(writes
        .iter()
        .any(|f| f["signature"] == json!("transfer(address,uint256)")));
    assert!(
        !reads.iter().any(|f| f["name"] == json!("transfer")),
        "a state-changing function is not a read"
    );
    // And each entry carries the selector the chain identifies it by.
    let transfer = writes
        .iter()
        .find(|f| f["signature"] == json!("transfer(address,uint256)"))
        .expect("transfer");
    assert_eq!(transfer["selector"], json!("0xa9059cbb"));

    // Events come with the topic a log would carry.
    let events = page["interface"]["events"].as_array().expect("events");
    let transfer_event = events
        .iter()
        .find(|e| e["signature"] == json!("Transfer(address,address,uint256)"))
        .expect("Transfer");
    assert_eq!(transfer_event["topic0"], json!(TRANSFER_TOPIC.as_str()));
    assert_eq!(transfer_event["inputs"][0]["indexed"], json!(true));

    // The TIP-20 details come from the indexed metadata, not another RPC call.
    assert_eq!(page["token_meta"]["symbol"], json!("pathUSD"));
    assert_eq!(page["token_meta"]["decimals"], json!(6));
}

/// An ordinary account has no interface to show, and the tab does not claim
/// otherwise.
#[tokio::test]
async fn a_plain_account_has_no_interface() {
    let (_dir, base) = serve(Value::Null).await;
    let page = get_json(
        &base,
        &format!("/address/{}?tab=contract", checksum_address(SENDER)),
    )
    .await;
    assert_eq!(page["has_interface"], json!(false));
    assert_eq!(page["interface"]["abis"], json!([]));
    assert!(page["token_meta"].is_null());
}

/// The address page renders on every tab, including the ones added last.
#[tokio::test]
async fn every_address_tab_renders() {
    let (_dir, base) = serve(Value::Null).await;
    for address in [checksum_address(SENDER), checksum_address(TOKEN)] {
        for tab in ["transactions", "transfers", "holdings", "contract"] {
            let path = format!("/address/{address}?tab={tab}");
            let response = reqwest::get(format!("{base}{path}"))
                .await
                .unwrap_or_else(|e| panic!("GET {path}: {e}"));
            assert_eq!(response.status(), 200, "GET {path}");
        }
    }
}

/// Multicall3, Permit2, CreateX and the anchoring contract sit at fixed addresses
/// that no Tempo binding covers. Every path that names a contract must know them.
#[tokio::test]
async fn canonical_contracts_are_named_and_searchable() {
    let (_dir, base) = serve(Value::Null).await;
    let multicall = "0xcA11bde05977b3631167028862bE2a173976CA11";
    let anchoring = "0x0000000000000000000000000000000000000a00";

    let (dir, db) = temp_db("canonical.db");
    assert_eq!(
        nvnmchain_explorer::web::address_label(&db, multicall).as_deref(),
        Some("Multicall3")
    );
    assert_eq!(
        nvnmchain_explorer::web::address_label(&db, anchoring).as_deref(),
        Some("Anchoring")
    );
    let module_admin = "0x0582bfb2e8561d48636e78f0e6b139d5a842be8f";
    assert_eq!(
        nvnmchain_explorer::web::address_label(&db, module_admin).as_deref(),
        Some("Module Admin")
    );
    drop(dir);

    let page = get_json(&base, &format!("/address/{anchoring}?tab=contract")).await;
    assert_eq!(page["interface"]["abis"], json!(["anchoring"]));

    let hits = suggest(&base, "permit").await;
    assert!(
        hits.iter().any(|r| r["label"] == json!("Permit2")),
        "got {hits:#?}"
    );

    // And the interface is there to show, rather than a bare address page.
    let page = get_json(&base, &format!("/address/{multicall}?tab=contract")).await;
    assert_eq!(page["interface"]["abis"], json!(["multicall3"]));
    assert!(page["interface"]["writes"]
        .as_array()
        .expect("writes")
        .iter()
        .any(|f| f["name"] == json!("aggregate3")));
}
