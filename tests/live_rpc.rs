//! Integration tests against the live chain RPC.
//!
//! These exercise the real node at `NVNM_RPC` (default
//! `https://rpc.nvnm.canary.mantrachain.dev`): chain metadata, block and tx
//! fetching, indexing into SQLite, and the HTTP API end to end.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use nvnmchain_explorer::config::{DEFAULT_CHAIN_ID, DEFAULT_RPC_URL, DEFAULT_WS_URL};
use nvnmchain_explorer::db::{self, Db, TxColumns};
use nvnmchain_explorer::indexer::{fetch_block_bundle, index_block};
use nvnmchain_explorer::rpc::{parse_int_any, ChainRpc};
use nvnmchain_explorer::web::{self, AppState};
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tokio::sync::mpsc;

fn temp_db() -> (tempfile::TempDir, Db) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("explorer.db");
    let conn = db::init_db(path.to_str().unwrap()).expect("init db");
    (dir, Arc::new(Mutex::new(conn)))
}

fn rpc() -> ChainRpc {
    ChainRpc::new(DEFAULT_RPC_URL).expect("rpc client")
}

/// A recent block that has transactions, and their hashes in block order.
///
/// Found rather than pinned: a constant block is a fixture on one chain, and
/// this one has been reset out from under these tests once already.
async fn block_with_txs(rpc: &ChainRpc) -> Option<(u64, Vec<String>)> {
    let head = rpc.eth_block_number().await.ok()?;
    // In batches: most blocks on a quiet chain are empty, and 600 of them one
    // round trip at a time is a minute per test.
    for chunk in (head.saturating_sub(600)..=head)
        .rev()
        .collect::<Vec<_>>()
        .chunks(64)
    {
        let calls = chunk
            .iter()
            .map(|n| {
                (
                    "eth_getBlockByNumber".to_string(),
                    json!([format!("0x{n:x}"), false]),
                )
            })
            .collect();
        for (number, res) in chunk.iter().zip(rpc.batch_call(calls).await.ok()?) {
            let Ok(block) = res else { continue };
            let hashes: Vec<String> = block
                .get("transactions")
                .and_then(Value::as_array)
                .map(|txs| {
                    txs.iter()
                        .filter_map(|h| h.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            if !hashes.is_empty() {
                return Some((*number, hashes));
            }
        }
    }
    None
}

#[tokio::test]
async fn block_receipts_in_one_call() {
    let rpc = rpc();
    let Some((number, hashes)) = block_with_txs(&rpc).await else {
        eprintln!("skipping: no block with transactions near the head");
        return;
    };
    let receipts = rpc
        .eth_get_block_receipts(number)
        .await
        .expect("eth_getBlockReceipts")
        .expect("receipts");
    // One per transaction, in the block's own order.
    assert_eq!(
        receipts
            .iter()
            .filter_map(|r| r.get("transactionHash").and_then(Value::as_str))
            .collect::<Vec<_>>(),
        hashes.iter().map(String::as_str).collect::<Vec<_>>()
    );
    assert!(receipts[0].get("feeToken").is_some());
    assert!(receipts[0].get("status").and_then(Value::as_str).is_some());
}

#[tokio::test]
async fn batched_calls_return_in_order() {
    let rpc = rpc();
    let results = rpc
        .batch_call(vec![
            ("eth_chainId".into(), json!([])),
            ("eth_blockNumber".into(), json!([])),
            ("eth_gasPrice".into(), json!([])),
        ])
        .await
        .expect("batch");
    assert_eq!(results.len(), 3);
    let chain_id = results[0].as_ref().expect("chain id ok").as_str().unwrap();
    assert_eq!(
        u64::from_str_radix(chain_id.trim_start_matches("0x"), 16).unwrap(),
        DEFAULT_CHAIN_ID
    );
    assert!(results[1].as_ref().unwrap().as_str().is_some());
    assert!(results[2].as_ref().unwrap().as_str().is_some());
}

#[tokio::test]
async fn fetch_block_receipts_fallback() {
    let rpc = rpc();
    let Some((number, hashes)) = block_with_txs(&rpc).await else {
        eprintln!("skipping: no block with transactions near the head");
        return;
    };
    let receipts = rpc
        .fetch_block_receipts(number, &hashes)
        .await
        .expect("receipts")
        .expect("non-empty");
    assert_eq!(receipts.len(), hashes.len());
    assert_eq!(
        receipts[0].get("transactionHash").and_then(Value::as_str),
        Some(hashes[0].as_str())
    );
}

#[tokio::test]
async fn backfill_throughput() {
    let rpc = rpc();
    let (_dir, db) = temp_db();
    let head = rpc.eth_block_number().await.expect("head");
    let from = head.saturating_sub(300);
    let count = head - from + 1;

    let started = Instant::now();
    let mut set = tokio::task::JoinSet::new();
    let mut next = from;
    loop {
        while next <= head && set.len() < 32 {
            let rpc = rpc.clone();
            let db = db.clone();
            let num = next;
            set.spawn(async move {
                if let Ok(Some(bundle)) = fetch_block_bundle(&rpc, num).await {
                    let _ = db::save_block_bundle(&db, &bundle);
                }
            });
            next += 1;
        }
        if set.is_empty() {
            break;
        }
        let _ = set.join_next().await;
    }
    let elapsed = started.elapsed();
    let per_sec = count as f64 / elapsed.as_secs_f64();
    eprintln!("indexed {count} blocks in {elapsed:?} ({per_sec:.0} blocks/s)");

    assert_eq!(db::get_min_block_number(&db), Some(from as i64));
    assert_eq!(
        db::get_latest_block(&db).map(|b| b.number as u64),
        Some(head)
    );
    assert!(
        per_sec > 20.0,
        "backfill too slow for a sub-second chain: {per_sec:.1} blocks/s"
    );
}

#[tokio::test]
async fn head_feed_delivers_blocks() {
    let rpc = rpc();
    let (tx, mut rx) = mpsc::channel::<u64>(8);
    let (_, shutdown) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(async move {
        nvnmchain_explorer::ws::head_watcher(
            rpc,
            DEFAULT_WS_URL.to_string(),
            true,
            Duration::from_secs(1),
            tx,
            shutdown,
        )
        .await;
    });
    match tokio::time::timeout(Duration::from_secs(15), rx.recv()).await {
        Ok(Some(head)) => {
            assert!(head > 0, "head should be positive");
            eprintln!("head feed delivered block {head}");
        }
        Ok(None) => panic!("head feed closed"),
        Err(_) => {
            handle.abort();
            panic!("no head delivered within 15s");
        }
    }
}

#[tokio::test]
async fn chain_metadata_matches_mantra() {
    let rpc = rpc();
    let chain_id = rpc.eth_chain_id().await.expect("eth_chainId");
    assert_eq!(chain_id, DEFAULT_CHAIN_ID, "unexpected chain id");

    let head = rpc.eth_block_number().await.expect("eth_blockNumber");
    assert!(head > 0, "chain head should be above zero");

    let block = rpc
        .eth_get_block_by_number(head, true)
        .await
        .expect("block")
        .expect("block exists");
    assert!(block.get("hash").and_then(Value::as_str).is_some());
    assert!(block.get("transactions").is_some());
}

#[tokio::test]
async fn index_recent_blocks_into_sqlite() {
    let rpc = rpc();
    let (_dir, db) = temp_db();
    let head = rpc.eth_block_number().await.expect("head");

    // Index a small window at the tip.
    let start = head.saturating_sub(4);
    for n in start..=head {
        index_block(&rpc, &db, n).await.expect("index block");
    }

    for n in start..=head {
        let block = db::get_block_by_number(&db, n as i64).expect("stored block");
        assert_eq!(block.number as u64, n);
        assert!(!block.hash.is_empty());
        assert!(block.timestamp > 0);
    }
    let latest = db::get_latest_block(&db).expect("latest");
    assert_eq!(latest.number as u64, head);

    // Transactions that exist on the chain must be stored with receipts.
    for n in start..=head {
        let block = db::get_block_by_number(&db, n as i64).unwrap();
        let txs = db::get_block_transactions(&db, block.number, TxColumns::Full);
        assert_eq!(txs.len() as i64, block.tx_count);
        for tx in &txs {
            assert_eq!(tx.block_number, block.number);
            assert!(!tx.from_addr.is_empty());
        }
    }

    // If the tip block has transactions, spot-check a receipt + decoded call.
    let txs = db::get_block_transactions(&db, head as i64, TxColumns::Full);
    if let Some(tx) = txs.first() {
        if tx.receipt_data.is_some() {
            let receipt: Value = serde_json::from_str(tx.receipt_data.as_deref().unwrap()).unwrap();
            assert!(receipt.get("status").is_some());
        }
        if tx.input.len() > 10 {
            assert!(nvnmchain_explorer::decoder::decode_function_call(&tx.input).is_some());
        }
    }
}

#[tokio::test]
async fn web_api_serves_indexed_data() {
    let rpc = rpc();
    let (_dir, db) = temp_db();
    let head = rpc.eth_block_number().await.expect("head");
    index_block(&rpc, &db, head).await.expect("index tip");

    let cfg = nvnmchain_explorer::config::Settings {
        rpc_url: DEFAULT_RPC_URL.into(),
        ws_url: String::new(),
        index_ws: false,
        chain_id: DEFAULT_CHAIN_ID,
        host: "127.0.0.1".into(),
        port: 0,
        db_path: "unused".into(),
        recent_block_count: 5,
        recent_tx_count: 5,
        poll_seconds: 1.0,
        batch_size: 5,
        index_concurrency: 8,
        native_symbol: "OM".into(),
        stats_interval_seconds: 5.0,
        // The test asserts what this chain says; a third-party directory has
        // no part in that, and would be a network call per page view.
        signature_lookup_url: None,
    };
    let tera = web::build_tera(db.clone()).expect("tera");
    let (block_tx, _) = broadcast::channel::<Value>(64);
    let state = AppState {
        db: db.clone(),
        rpc,
        cfg,
        tera,
        block_events: block_tx,
        stats: std::sync::Arc::new(std::sync::RwLock::new(serde_json::Value::Null)),
        shutdown: tokio::sync::watch::channel(false).1,
    };
    let app = web::app(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let base = format!("http://{addr}");

    let client = reqwest::Client::new();

    // Home JSON.
    let home: Value = client
        .get(format!("{base}/"))
        .query(&[("format", "json")])
        .send()
        .await
        .expect("home")
        .json()
        .await
        .expect("home json");
    assert!(home
        .get("latest_block")
        .and_then(Value::as_object)
        .is_some());
    assert!(home
        .get("recent_blocks")
        .and_then(Value::as_array)
        .is_some());

    // Block JSON by number.
    let block_resp = client
        .get(format!("{base}/block/{head}"))
        .header("Accept", "application/json")
        .send()
        .await
        .expect("block");
    assert_eq!(block_resp.status(), 200);
    let block_json: Value = block_resp.json().await.expect("block json");
    assert_eq!(block_json["block"]["number"], json!(head));

    // Block HTML.
    let html = client
        .get(format!("{base}/block/{head}"))
        .send()
        .await
        .expect("block html");
    assert!(html.status().is_success());
    let body = html.text().await.expect("html body");
    assert!(body.contains("Block"));

    // Search by block number redirects.
    let search = client
        .get(format!("{base}/search"))
        .query(&[("q", head.to_string())])
        .send()
        .await
        .expect("search");
    assert!(search.status().is_redirection() || search.status().is_success());

    // A transaction page if the tip block has transactions.
    let txs = db::get_block_transactions(&db, head as i64, TxColumns::Full);
    if let Some(tx) = txs.first() {
        let tx_resp = client
            .get(format!("{base}/tx/{}", tx.hash))
            .query(&[("format", "json")])
            .send()
            .await
            .expect("tx");
        assert_eq!(tx_resp.status(), 200, "tx page should load for indexed tx");
        let tx_json: Value = tx_resp.json().await.expect("tx json");
        assert_eq!(tx_json["tx"]["hash"], json!(tx.hash));
        assert!(tx_json.get("calls").is_some());
    }

    // 404 for an unknown tx.
    let missing = client
        .get(format!("{base}/tx/0x{}", "00".repeat(32)))
        .query(&[("format", "json")])
        .send()
        .await
        .expect("missing");
    assert_eq!(missing.status(), 404);

    // The live feed sends the current tip immediately on connect.
    let sse = client
        .get(format!("{base}/api/events"))
        .send()
        .await
        .expect("sse");
    assert_eq!(sse.status(), 200);
    let mut chunks = sse.bytes_stream();
    let first = tokio::time::timeout(Duration::from_secs(5), chunks.next())
        .await
        .expect("sse first chunk within timeout")
        .expect("sse stream open")
        .expect("sse bytes");
    let text = String::from_utf8_lossy(&first);
    assert!(
        text.contains("event: block"),
        "SSE should send a block event: {text}"
    );
    assert!(text.contains("data:"), "SSE should carry JSON data: {text}");
}

/// A registry written after the load can name the transaction that wrote it.
/// The seeded ones cannot: they arrived in the dump, without an event.
#[tokio::test]
async fn anchoring_events_link_a_registry_to_its_tx() {
    let rpc = rpc();
    let (_dir, db) = temp_db();
    let head = rpc.eth_block_number().await.expect("head");
    // The latest write within the node's 100k-block log range.
    let logs = rpc
        .eth_get_logs(json!({
            "address": nvnmchain_explorer::anchoring::ADDRESS,
            "fromBlock": format!("0x{:x}", head.saturating_sub(99_999)),
            "toBlock": format!("0x{head:x}"),
        }))
        .await
        .expect("anchoring logs");
    let Some(block) = logs.last().and_then(|l| l.get("blockNumber")) else {
        eprintln!("skipping: no anchoring write in the last 100k blocks");
        return;
    };
    let block = parse_int_any(block);
    index_block(&rpc, &db, block as u64).await.expect("index");
    let id: i64 = db::lock(&db)
        .query_row(
            "SELECT registry_id FROM anchoring_events LIMIT 1",
            [],
            |r| r.get(0),
        )
        .expect("the block's anchoring row");
    let found = db::get_anchoring_events(&db, id, 25);
    let event = &found[0];
    assert_eq!(event.block_number, block);
    println!(
        "registry {} {} in tx {} block {}",
        event.registry_id, event.event, event.tx_hash, event.block_number
    );
    assert!(event.tx_hash.starts_with("0x") && event.tx_hash.len() == 66);
    assert!(event.caller.starts_with("0x"));
    assert!(event.registry_id > 0);
}
