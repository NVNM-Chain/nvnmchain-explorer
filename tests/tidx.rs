//! The pages over a tidx: its rows reach the JSON API in SQLite's shape, and a
//! tidx that fails is an error.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use nvnmchain_explorer::config::Settings;
use nvnmchain_explorer::db::{self, Db};
use nvnmchain_explorer::tidx::Tidx;
use nvnmchain_explorer::tokens::TokenMeta;
use nvnmchain_explorer::web::{self, AppState};
use serde_json::{json, Value};

const HOLDER: &str = "0x1111111111111111111111111111111111111111";
const OTHER: &str = "0x2222222222222222222222222222222222222222";
const TOKEN: &str = "0x20c0000000000000000000000000000000000000";
const TX: &str = "0x3333333333333333333333333333333333333333333333333333333333333333";

/// Canned rows for each query the explorer sends, in the column order it reads.
fn answer(sql: &str) -> Value {
    let rows = if sql.contains("count() FROM txs") {
        json!([[4, 2]])
    } else if sql.contains("count() FROM token_transfers") {
        json!([[9]])
    } else if sql.contains("FROM address_balances") {
        json!([[TOKEN, "5000000"]])
    } else if sql.contains("FROM token_balances_snapshot") {
        json!([[HOLDER, "250000000"]])
    } else if sql.contains("FROM token_holder_counts") {
        json!([[TOKEN, 3]])
    } else if sql.contains("SELECT block_num, idx, hash") {
        json!([[
            TX,
            7,
            0,
            HOLDER,
            TOKEN,
            1,
            21000,
            "2600000000",
            null,
            TOKEN,
            "478",
            "0xa9059cbb",
            1700000000
        ]])
    } else {
        json!([[TX, 7, 2, TOKEN, HOLDER, OTHER, "1500000", 1700000000, HOLDER, TOKEN, 1]])
    };
    json!({"columns": [], "rows": rows, "row_count": rows.as_array().map_or(0, Vec::len)})
}

async fn query(
    State(failing): State<Arc<AtomicBool>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Json<Value> {
    if failing.load(Ordering::Relaxed) {
        return Json(json!({"ok": false, "error": "ClickHouse not configured"}));
    }
    Json(answer(&params["sql"]))
}

/// The explorer over an empty database and a fake tidx; `failing` makes the
/// tidx answer every query with an error.
async fn serve() -> (tempfile::TempDir, String, Arc<AtomicBool>) {
    let failing = Arc::new(AtomicBool::new(false));
    let tidx = Router::new()
        .route("/query", get(query))
        .with_state(failing.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tidx_url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, tidx).await });

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tidx.db");
    let db: Db = Arc::new(Mutex::new(db::init_db(path.to_str().unwrap()).unwrap()));
    // Named here, so the holdings need not ask the node what the token is.
    db::save_token_metadata(
        &db,
        &[TokenMeta {
            address: nvnmchain_explorer::decoder::checksum_address(TOKEN),
            name: "pathUSD".into(),
            symbol: "pathUSD".into(),
            decimals: 6,
            currency: "USD".into(),
            total_supply: "1000000000".into(),
        }],
    )
    .unwrap();
    let mut cfg = Settings::from_env();
    cfg.signature_lookup_url = None;
    cfg.rpc_url = "http://127.0.0.1:1".into();
    cfg.tidx = Some(Tidx::new(&tidx_url, cfg.chain_id));
    let (_running, shutdown) = tokio::sync::watch::channel(false);
    let state = AppState {
        tera: web::build_tera(db.clone()).unwrap(),
        db,
        rpc: nvnmchain_explorer::rpc::ChainRpc::from_settings(&cfg).unwrap(),
        cfg,
        block_events: tokio::sync::broadcast::channel(16).0,
        stats: Arc::new(std::sync::RwLock::new(Value::Null)),
        signatures: Default::default(),
        shutdown,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, web::app(state)).await });
    (dir, base, failing)
}

async fn get_json(base: &str, path: &str) -> Value {
    reqwest::get(format!("{base}{path}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test]
async fn address_reads_come_from_tidx_in_sqlite_shape() {
    let (_dir, base, _) = serve().await;

    let txs = get_json(&base, &format!("/api/v1/addresses/{HOLDER}/txs")).await;
    let tx = &txs["items"][0];
    assert_eq!(tx["hash"], json!(TX));
    assert_eq!(tx["method"], json!("transfer"));
    assert_eq!(tx["fee_amount"], json!("478"));
    assert_eq!(
        tx["fee_token"],
        json!("0x20C0000000000000000000000000000000000000")
    );
    assert_eq!(tx["base_fee"], json!("0x9af8da00"));
    assert_eq!(tx["contract_address"], Value::Null);

    let transfers = get_json(&base, &format!("/api/v1/addresses/{HOLDER}/transfers")).await;
    let transfer = &transfers["items"][0];
    assert_eq!(
        transfer["tx_from"],
        json!("0x1111111111111111111111111111111111111111")
    );
    assert_eq!(
        transfer["tx_to"],
        json!("0x20C0000000000000000000000000000000000000")
    );
    assert_eq!(transfer["amount_formatted"], json!("1.5"));
    assert_eq!(transfer["token_symbol"], json!("pathUSD"));

    let holdings = get_json(&base, &format!("/api/v1/addresses/{HOLDER}/holdings")).await;
    assert_eq!(holdings["items"][0]["formatted"], json!("5"));
    assert_eq!(holdings["items"][0]["symbol"], json!("pathUSD"));

    let page = get_json(&base, &format!("/address/{HOLDER}?format=json")).await;
    assert_eq!(page["tx_count"], json!(4));
    assert_eq!(page["transfer_count"], json!(2));
}

#[tokio::test]
async fn token_reads_come_from_tidx() {
    let (_dir, base, _) = serve().await;

    let holders = get_json(&base, &format!("/api/v1/tokens/{TOKEN}/holders")).await;
    let holder = &holders["items"][0];
    assert_eq!(
        holder["address"],
        json!("0x1111111111111111111111111111111111111111")
    );
    assert_eq!(holder["formatted"], json!("250"));
    assert_eq!(holder["share"], json!("25.0000"));

    let transfers = get_json(&base, &format!("/api/v1/tokens/{TOKEN}/transfers")).await;
    assert_eq!(transfers["items"][0]["to_addr"], json!(OTHER));

    let page = get_json(&base, &format!("/token/{TOKEN}?format=json")).await;
    assert_eq!(page["transfer_count"], json!(9));
}

#[tokio::test]
async fn a_failing_tidx_is_an_error() {
    let (_dir, base, failing) = serve().await;
    failing.store(true, Ordering::Relaxed);

    for path in [
        format!("/api/v1/addresses/{HOLDER}/txs"),
        format!("/api/v1/tokens/{TOKEN}/holders"),
        format!("/address/{HOLDER}?format=json"),
        format!("/token/{TOKEN}?format=json"),
    ] {
        let response = reqwest::get(format!("{base}{path}")).await.unwrap();
        assert_eq!(response.status(), 502, "{path}");
        let body: Value = response.json().await.unwrap();
        let error = body["error"].as_str().unwrap_or_default();
        assert!(
            error.contains("ClickHouse not configured"),
            "{path}: {body}"
        );
    }
}
