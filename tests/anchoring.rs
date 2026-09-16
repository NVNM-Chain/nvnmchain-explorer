//! The anchoring pages, over a stub node that answers like the contract: 30 registries,
//! registry 1 with 27 records and its record 1 with 30 versions, so every listing pages.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use alloy_primitives::{B256, U256};
use alloy_sol_types::SolCall;
use axum::{
    extract::{Query, State},
    routing::post,
    Json, Router,
};
use nvnmchain_explorer::anchoring::{
    recordsCall, recordsReturn, registriesByNameCall, registriesByNameReturn, registriesCall,
    registriesReturn, PageResponse, Record, Registry,
};
use nvnmchain_explorer::config::Settings;
use nvnmchain_explorer::db;
use nvnmchain_explorer::web::{self, AppState};
use serde_json::{json, Value};

/// `_recordCount[1]` and `[2]`: `keccak256(abi.encode(id, 5))`, worked out apart from the code
/// under test.
const RECORD_COUNT_SLOTS: [(&str, u64); 2] = [
    (
        "0x1471eb6eb2c5e789fc3de43f8ce62938c7d1836ec861730447e2ada8fd81017b",
        27,
    ),
    (
        "0x89832631fb3c3307a103ba2c84ab569c64d6182a18893dcd163f0f1c2090733a",
        1,
    ),
];

fn registry(id: u64) -> Registry {
    Registry {
        id,
        // Two share a name, as names are not unique.
        name: if id == 2 || id == 3 {
            "twin".into()
        } else {
            format!("reg-{id}")
        },
        description: String::new(),
        creator: "nvnm1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqzctpdvkkn".into(),
        createdAt: "2025-09-09 00:00:00 +0000 UTC".into(),
        metadata: "{}".into(),
    }
}

fn versions_of(registry_id: u64, record_id: u64) -> u64 {
    if (registry_id, record_id) == (1, 1) {
        30
    } else {
        1
    }
}

/// Version `index` of a record; 0 is the latest. Registry 2's one record shares a checksum
/// with registry 1's first.
fn record(registry_id: u64, record_id: u64, index: u64) -> Record {
    let latest = versions_of(registry_id, record_id);
    let index = if index == 0 { latest } else { index };
    Record {
        uri: format!("https://ex.test/{registry_id}/{record_id}/{index}"),
        checksum: format!("sum-{record_id}"),
        checksumAlgo: "sha256".into(),
        metadata: "{}".into(),
        timestamp: "2025-09-09 00:00:00 +0000 UTC".into(),
        status: "Active".into(),
        recordId: record_id,
        index,
        isLatest: index == latest,
        registryId: registry_id,
    }
}

/// What the contract returns for the calls the pages make.
fn answer(data: &[u8], registries: u64) -> Vec<u8> {
    let page = PageResponse {
        nextKey: Default::default(),
        total: 0,
    };
    let selector: [u8; 4] = data[..4].try_into().unwrap();
    match selector {
        registriesCall::SELECTOR => {
            let call = registriesCall::abi_decode(data).unwrap();
            let rows = if call.registryId != 0 {
                vec![registry(call.registryId)]
            } else {
                assert!(call.pagination.reverse, "newest first");
                let p = call.pagination;
                (1..=registries)
                    .rev()
                    .skip(p.offset as usize)
                    .take(p.limit as usize)
                    .map(registry)
                    .collect()
            };
            registriesCall::abi_encode_returns(&registriesReturn {
                registriesOut: rows,
                paginationOut: page,
            })
        }
        registriesByNameCall::SELECTOR => {
            let call = registriesByNameCall::abi_decode(data).unwrap();
            assert_eq!(call.matchMode, 1, "exact");
            let rows = (1..=registries)
                .map(registry)
                .filter(|r| r.name == call.name)
                .collect();
            registriesByNameCall::abi_encode_returns(&registriesByNameReturn {
                registriesOut: rows,
                paginationOut: page,
            })
        }
        recordsCall::SELECTOR => {
            let c = recordsCall::abi_decode(data).unwrap();
            let rows = match (c.registryId, c.checksum.as_str(), c.recordId) {
                (0, "sum-1", 0) => vec![record(1, 1, 0), record(2, 1, 0)],
                (0, _, 0) => Vec::new(),
                (registry_id, "", 0) => {
                    let (offset, limit) = (c.pagination.offset, c.pagination.limit);
                    (offset + 1..=offset + limit)
                        .map(|id| record(registry_id, id, 0))
                        .collect()
                }
                (registry_id, "", record_id) => vec![record(registry_id, record_id, c.index)],
                _ => panic!("unexpected records call"),
            };
            recordsCall::abi_encode_returns(&recordsReturn {
                recordsOut: rows,
                paginationOut: page,
            })
        }
        _ => panic!("unexpected selector {}", hex::encode(selector)),
    }
}

fn respond(request: &Value, registries: u64) -> Value {
    let params = &request["params"];
    let result = match request["method"].as_str().unwrap() {
        "eth_getStorageAt" => {
            let slot = params[1].as_str().unwrap();
            let word = if slot.parse::<B256>().unwrap() == B256::with_last_byte(3) {
                U256::from(registries) << 160usize | U256::from(0xa0)
            } else {
                let (_, count) = RECORD_COUNT_SLOTS
                    .iter()
                    .find(|(s, _)| *s == slot)
                    .copied()
                    .unwrap_or_default();
                U256::from(count)
            };
            json!(B256::from(word).to_string())
        }
        "eth_call" => {
            let data =
                hex::decode(params[0]["data"].as_str().unwrap().trim_start_matches("0x")).unwrap();
            json!(format!("0x{}", hex::encode(answer(&data, registries))))
        }
        method => panic!("unexpected {method}"),
    };
    json!({"jsonrpc": "2.0", "id": request["id"], "result": result})
}

async fn stub_node(registries: u64) -> String {
    let handler = |State(registries): State<u64>, Json(body): Json<Value>| async move {
        Json(match body.as_array() {
            Some(batch) => json!(batch
                .iter()
                .map(|r| respond(r, registries))
                .collect::<Vec<_>>()),
            None => respond(&body, registries),
        })
    };
    let app = Router::new()
        .route("/", post(handler))
        .with_state(registries);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    format!("http://{addr}")
}

/// The explorer, reading `rpc_url`.
async fn serve(rpc_url: String) -> (tempfile::TempDir, String) {
    serve_with(rpc_url, None).await
}

/// The explorer over a stub node, and the name index beside it when there is one.
async fn serve_with(
    rpc_url: String,
    name_search_url: Option<String>,
) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let conn = db::init_db(dir.path().join("anchoring.db").to_str().unwrap()).unwrap();
    let db = Arc::new(Mutex::new(conn));
    let mut cfg = Settings::from_env();
    cfg.signature_lookup_url = None;
    cfg.rpc_url = rpc_url;
    cfg.name_search_url = name_search_url;
    let state = AppState {
        tera: web::build_tera(db.clone()).unwrap(),
        db,
        rpc: nvnmchain_explorer::rpc::ChainRpc::from_settings(&cfg).unwrap(),
        cfg,
        block_events: tokio::sync::broadcast::channel(16).0,
        stats: Arc::new(std::sync::RwLock::new(Value::Null)),
        shutdown: tokio::sync::watch::channel(false).1,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, web::app(state)).await });
    (dir, format!("http://{addr}"))
}

async fn get(base: &str, path: &str) -> (u16, Value) {
    let sep = if path.contains('?') { '&' } else { '?' };
    let response = reqwest::get(format!("{base}{path}{sep}format=json"))
        .await
        .unwrap();
    (response.status().as_u16(), response.json().await.unwrap())
}

fn ids(rows: &Value, key: &str) -> Vec<u64> {
    rows.as_array()
        .unwrap()
        .iter()
        .map(|r| r[key].as_u64().unwrap())
        .collect()
}

#[tokio::test]
async fn every_listing_is_newest_first_and_pages() {
    let (_dir, base) = serve(stub_node(30).await).await;

    let (_, first) = get(&base, "/anchoring").await;
    assert_eq!(
        (first["total"].clone(), first["total_pages"].clone()),
        (json!(30), json!(2))
    );
    assert_eq!(
        ids(&first["registries"], "id"),
        (6..=30).rev().collect::<Vec<_>>()
    );
    let (_, last) = get(&base, "/anchoring?page=2").await;
    assert_eq!(ids(&last["registries"], "id"), [5, 4, 3, 2, 1]);

    let (_, registry) = get(&base, "/anchoring/1?page=2").await;
    assert_eq!(registry["registry"]["name"], "reg-1");
    assert_eq!(
        (registry["total"].clone(), registry["total_pages"].clone()),
        (json!(27), json!(2))
    );
    assert_eq!(ids(&registry["records"], "recordId"), [2, 1]);
    assert_eq!(
        registry["records"][1]["index"], 30,
        "the latest version of each"
    );

    let (_, record) = get(&base, "/anchoring/1/1").await;
    assert_eq!(record["latest"]["index"], 30);
    assert_eq!(record["total_pages"], 2);
    assert_eq!(
        ids(&record["versions"], "index"),
        (6..=30).rev().collect::<Vec<_>>()
    );
    let (_, older) = get(&base, "/anchoring/1/1?page=2").await;
    assert_eq!(ids(&older["versions"], "index"), [5, 4, 3, 2, 1]);
    assert_eq!(older["versions"][0]["isLatest"], false);
}

#[tokio::test]
async fn a_lookup_finds_an_id_an_exact_name_or_a_checksum() {
    let (_dir, base) = serve(stub_node(30).await).await;

    let browser = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let response = browser
        .get(format!("{base}/anchoring?q=%207%20"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["location"], "/anchoring/7");

    let (_, named) = get(&base, "/anchoring?q=twin").await;
    assert_eq!(ids(&named["registries"], "id"), [2, 3]);
    assert_eq!(named["records"], json!([]));

    let (_, anchored) = get(&base, "/anchoring?q=sum-1").await;
    assert_eq!(anchored["registries"], json!([]));
    assert_eq!(ids(&anchored["records"], "registryId"), [1, 2]);
}

#[tokio::test]
async fn what_does_not_exist_is_not_found() {
    let (_dir, base) = serve(stub_node(30).await).await;
    for path in [
        "/anchoring/0",
        "/anchoring/31",
        "/anchoring/x",
        "/anchoring/1/28",
        "/anchoring/3/1",
        "/anchoring/1/x",
    ] {
        assert_eq!(get(&base, path).await.0, 404, "{path}");
    }
}

#[tokio::test]
async fn every_page_renders() {
    let (_dir, base) = serve(stub_node(30).await).await;
    for (path, expect) in [
        ("/anchoring", "reg-30"),
        ("/anchoring?page=2", "Page 2 of 2"),
        ("/anchoring?q=twin", "Registries named"),
        ("/anchoring?q=sum-1", "2 / 1"),
        ("/anchoring/1", "27 records"),
        ("/anchoring/1/1", "30 versions"),
        ("/anchoring/99", "Not Found"),
    ] {
        let html = reqwest::get(format!("{base}{path}"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(html.contains(expect), "{path} lacks {expect:?}");
    }
}

/// A chain without the contract has no registries, rather than a broken page.
#[tokio::test]
async fn no_contract_is_no_registries() {
    let (_dir, base) = serve(stub_node(0).await).await;
    let (status, page) = get(&base, "/anchoring").await;
    assert_eq!(
        (status, page["total"].clone(), page["registries"].clone()),
        (200, json!(0), json!([]))
    );
    assert_eq!(get(&base, "/anchoring/1").await.0, 404);
}

#[tokio::test]
async fn a_node_that_does_not_answer_is_a_bad_gateway() {
    let (_dir, base) = serve("http://127.0.0.1:1".into()).await;
    let (status, page) = get(&base, "/anchoring").await;
    assert_eq!(status, 502);
    assert!(
        page["error"].as_str().unwrap().contains("eth_getStorageAt"),
        "{page}"
    );
}

/// The search box, for what only the contract knows: the index holds no registry name
/// and no checksum.
#[tokio::test]
async fn the_search_box_finds_a_registry_by_name_and_a_record_by_checksum() {
    let (_dir, base) = serve(stub_node(30).await).await;

    let (status, named) = get(&base, "/search?q=reg-4").await;
    assert_eq!(status, 200);
    assert_eq!(named["match"]["type"], "registry");
    assert_eq!(named["match"]["url"], "/anchoring/4");

    let (_, anchored) = get(&base, "/search?q=sum-1").await;
    assert_eq!(anchored["match"]["type"], "record");
    assert_eq!(anchored["match"]["url"], "/anchoring/1/1");

    let (_, nothing) = get(&base, "/search?q=nothing-of-the-sort").await;
    assert_eq!(nothing["match"], Value::Null);

    let (_, suggestions) = get(&base, "/api/search?q=reg-4").await;
    let kinds: Vec<&str> = suggestions["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["type"].as_str().unwrap())
        .collect();
    assert!(kinds.contains(&"registry"), "{suggestions}");
}

/// A stub name index: the registries whose name starts with the query, in id order,
/// written as the service writes them -- proto JSON, so an id is a string.
async fn stub_name_index() -> String {
    async fn search(Query(params): Query<HashMap<String, String>>) -> Json<Value> {
        let name = params
            .get("name")
            .cloned()
            .unwrap_or_default()
            .to_lowercase();
        // Prefix is the only mode the box asks for; the explorer bounds the page itself.
        let rows: Vec<Value> = (1..=30)
            .map(registry)
            .filter(|r| r.name.to_lowercase().starts_with(&name))
            .map(|r| json!({"id": r.id.to_string(), "name": r.name}))
            .collect();
        Json(json!({"registries": rows, "pagination": null}))
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new().route(
        "/NVNM-Chain/nvnmchain/anchoring/v1/registries/search",
        axum::routing::get(search),
    );
    tokio::spawn(async move { axum::serve(listener, app).await });
    format!("http://{addr}")
}

/// Half a name matches only in the index beside the chain: the contract answers a whole
/// one and nothing less.
#[tokio::test]
async fn the_search_box_takes_half_a_name_from_the_index() {
    let (_dir, base) = serve_with(stub_node(30).await, Some(stub_name_index().await)).await;

    let (_, suggestions) = get(&base, "/api/search?q=reg-1").await;
    let rows: Vec<(&str, &str)> = suggestions["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| (row["type"].as_str().unwrap(), row["url"].as_str().unwrap()))
        .collect();
    assert!(
        rows.contains(&("registry", "/anchoring/1")),
        "{suggestions}"
    );
    assert!(
        rows.contains(&("registry", "/anchoring/10")),
        "{suggestions}"
    );
    assert_eq!(
        rows.iter()
            .filter(|(_, url)| *url == "/anchoring/1")
            .count(),
        1,
        "the contract's exact hit is not repeated by the index's: {suggestions}"
    );

    // Enter on half a name lands where the first suggestion pointed.
    let (_, found) = get(&base, "/search?q=reg-2").await;
    assert_eq!(found["match"]["url"], "/anchoring/20");

    // Without an index, the same half name finds nothing.
    let (_dir, bare) = serve(stub_node(30).await).await;
    let (_, none) = get(&bare, "/search?q=reg-2").await;
    assert_eq!(none["match"], Value::Null);
}
