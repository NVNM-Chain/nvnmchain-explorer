//! Async JSON-RPC client, mirroring `app/rpc.py`.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use num_bigint::BigInt;
use reqwest::header::{HeaderValue, RETRY_AFTER};
use reqwest::{Client, RequestBuilder, StatusCode};
use serde_json::{json, Value};
use tokio::time::Instant;

use crate::config::Settings;

pub struct ChainRpc {
    client: Client,
    url: String,
    /// `x-api-key` for JSON-RPC requests only. Set per request, not as a
    /// client default, so [`ChainRpc::http_client`] never carries it to a
    /// third party.
    api_key: Option<HeaderValue>,
    req_id: AtomicU64,
    pace: Option<Arc<BatchPace>>,
}

impl Clone for ChainRpc {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            url: self.url.clone(),
            api_key: self.api_key.clone(),
            req_id: AtomicU64::new(self.req_id.load(Ordering::Relaxed)),
            pace: self.pace.clone(),
        }
    }
}

/// Spaces batch requests `gap` apart across a client and its clones, for a
/// gateway that rate-limits them.
struct BatchPace {
    gap: Duration,
    next: tokio::sync::Mutex<Instant>,
}

impl BatchPace {
    async fn wait(&self) {
        let mut next = self.next.lock().await;
        tokio::time::sleep_until(*next).await;
        *next = Instant::now() + self.gap;
    }
}

/// What a 429 asks us to wait, a minute when it does not say.
fn retry_after(resp: &reqwest::Response) -> Duration {
    let secs = resp
        .headers()
        .get(RETRY_AFTER)
        .and_then(|v| v.to_str().ok()?.parse().ok());
    Duration::from_secs(secs.unwrap_or(60))
}

#[derive(Debug)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RPC error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for RpcError {}

impl RpcError {
    /// The error a JSON-RPC response carries, if any.
    fn of(response: &Value) -> Option<Self> {
        let err = response.get("error").filter(|e| !e.is_null())?;
        Some(Self {
            code: err.get("code").and_then(Value::as_i64).unwrap_or(0),
            message: err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            data: err.get("data").cloned(),
        })
    }
}

#[cfg(test)]
impl ChainRpc {
    /// A client to `app`, served on a port of its own for one test.
    pub(crate) async fn test_node(app: axum::Router) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await });
        Self::new(url).unwrap()
    }
}

impl ChainRpc {
    pub fn new(url: impl Into<String>) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self {
            client,
            url: url.into(),
            api_key: None,
            req_id: AtomicU64::new(0),
            pace: None,
        })
    }

    /// Space batch requests `gap` apart, clones included, and wait out a 429
    /// rather than fail on it.
    pub fn paced(mut self, gap: Duration) -> Self {
        self.pace = Some(Arc::new(BatchPace {
            gap,
            next: tokio::sync::Mutex::new(Instant::now()),
        }));
        self
    }

    /// This client on connections of its own, pace and key kept. A connection
    /// dies with the tokio runtime that opened it, so a client used from several
    /// runtimes, one per `#[tokio::test]`, needs a copy for each.
    pub fn with_own_connections(&self) -> Result<Self> {
        Ok(Self {
            client: Self::new(&self.url)?.client,
            ..self.clone()
        })
    }

    pub fn from_settings(settings: &Settings) -> Result<Self> {
        Ok(Self::new(&settings.rpc_url)?.with_api_key(settings.rpc_api_key.clone()))
    }

    /// Send `key` as `x-api-key` on every JSON-RPC request.
    pub fn with_api_key(mut self, key: Option<HeaderValue>) -> Self {
        self.api_key = key;
        self
    }

    /// The shared HTTP client, for the few requests that are not JSON-RPC —
    /// the signature directory. Borrowing it keeps one connection pool per
    /// process rather than one per subsystem.
    pub fn http_client(&self) -> &Client {
        &self.client
    }

    /// A JSON-RPC POST to the node, with the API key when there is one.
    fn post(&self) -> RequestBuilder {
        let req = self.client.post(&self.url);
        match &self.api_key {
            Some(key) => req.header("x-api-key", key.clone()),
            None => req,
        }
    }

    fn next_id(&self) -> u64 {
        self.req_id.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": self.next_id(),
            "method": method,
            "params": params,
        });
        let resp = self
            .post()
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("RPC request {method} failed"))?;
        let mut body: Value = resp
            .json()
            .await
            .with_context(|| format!("RPC response for {method} was not JSON"))?;
        if let Some(err) = RpcError::of(&body) {
            return Err(err.into());
        }
        Ok(body.get_mut("result").map_or(Value::Null, Value::take))
    }

    /// Send several JSON-RPC calls in a single HTTP request. Results come back
    /// in request order, each either `Ok(result)` or the per-call error. One
    /// call goes alone: no batch for a gateway that rate-limits them.
    pub async fn batch_call(
        &self,
        calls: Vec<(String, Value)>,
    ) -> Result<Vec<Result<Value, RpcError>>> {
        if calls.len() == 1 {
            let (method, params) = calls.into_iter().next().unwrap();
            return match self.call(&method, params).await {
                Ok(value) => Ok(vec![Ok(value)]),
                Err(e) => Ok(vec![Err(e.downcast::<RpcError>()?)]),
            };
        }
        let payload: Vec<Value> = calls
            .iter()
            .enumerate()
            .map(|(i, (method, params))| {
                json!({
                    "jsonrpc": "2.0",
                    "id": i as u64 + 1,
                    "method": method,
                    "params": params,
                })
            })
            .collect();
        let resp = loop {
            if let Some(pace) = &self.pace {
                pace.wait().await;
            }
            let resp = self.post().json(&payload).send().await.with_context(|| {
                let methods: BTreeSet<&str> = calls.iter().map(|(m, _)| m.as_str()).collect();
                let methods: Vec<&str> = methods.into_iter().collect();
                format!("batch RPC request ({}) failed", methods.join(", "))
            })?;
            if self.pace.is_none() || resp.status() != StatusCode::TOO_MANY_REQUESTS {
                break resp;
            }
            // Asking again before the gateway says only prolongs its block.
            tokio::time::sleep(retry_after(&resp)).await;
        };
        let body: Value = resp
            .json()
            .await
            .context("batch RPC response was not JSON")?;
        let items = match body {
            Value::Array(items) => items,
            // One error for the whole batch, as reth answers past its response size cap.
            other => match RpcError::of(&other) {
                Some(err) => return Err(err.into()),
                None => bail!("batch RPC response is not an array: {other}"),
            },
        };
        // Results are moved out, not copied: a batch of full blocks is large.
        let mut by_id: HashMap<u64, Value> = HashMap::with_capacity(items.len());
        for item in items {
            if let Some(id) = item.get("id").and_then(Value::as_u64) {
                by_id.insert(id, item);
            }
        }
        let mut out = Vec::with_capacity(calls.len());
        for i in 0..calls.len() {
            let id = i as u64 + 1;
            match by_id.remove(&id) {
                Some(mut item) => match RpcError::of(&item) {
                    Some(err) => out.push(Err(err)),
                    None => out.push(Ok(item.get_mut("result").map_or(Value::Null, Value::take))),
                },
                None => out.push(Err(RpcError {
                    code: -32603,
                    message: "no response for batched request".into(),
                    data: None,
                })),
            }
        }
        Ok(out)
    }

    pub async fn eth_block_number(&self) -> Result<u64> {
        let result = self.call("eth_blockNumber", json!([])).await?;
        hex_to_u64(&result).context("invalid eth_blockNumber result")
    }

    pub async fn eth_chain_id(&self) -> Result<u64> {
        let result = self.call("eth_chainId", json!([])).await?;
        hex_to_u64(&result).context("invalid eth_chainId result")
    }

    pub async fn eth_get_block_by_number(&self, num: u64, full: bool) -> Result<Option<Value>> {
        let result = self
            .call("eth_getBlockByNumber", json!([format!("0x{num:x}"), full]))
            .await?;
        if result.is_null() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }

    /// All receipts for a block in a single call (`eth_getBlockReceipts`).
    pub async fn eth_get_block_receipts(&self, num: u64) -> Result<Option<Vec<Value>>> {
        let result = self
            .call("eth_getBlockReceipts", json!([format!("0x{num:x}")]))
            .await?;
        if result.is_null() {
            Ok(None)
        } else {
            Ok(Some(result.as_array().cloned().unwrap_or_default()))
        }
    }

    /// Fetch receipts for a block: prefer the single `eth_getBlockReceipts`
    /// call, fall back to one batched request of per-transaction receipts.
    pub async fn fetch_block_receipts(
        &self,
        num: u64,
        tx_hashes: &[String],
    ) -> Result<Option<Vec<Value>>> {
        if tx_hashes.is_empty() {
            return Ok(Some(Vec::new()));
        }
        if let Ok(Some(receipts)) = self.eth_get_block_receipts(num).await {
            if !receipts.is_empty() {
                return Ok(Some(receipts));
            }
        }
        let calls: Vec<(String, Value)> = tx_hashes
            .iter()
            .map(|h| ("eth_getTransactionReceipt".into(), json!([h])))
            .collect();
        let results = self.batch_call(calls).await?;
        let receipts: Vec<Value> = results
            .into_iter()
            .filter_map(|r| r.ok().filter(|v| !v.is_null()))
            .collect();
        Ok(Some(receipts))
    }

    pub async fn eth_get_code(&self, address: &str, block: &str) -> Result<String> {
        let result = self.call("eth_getCode", json!([address, block])).await?;
        Ok(result
            .as_str()
            .filter(|s| !s.is_empty())
            .unwrap_or("0x")
            .to_string())
    }

    pub async fn eth_get_logs(&self, filter: Value) -> Result<Vec<Value>> {
        let result = self.call("eth_getLogs", json!([filter])).await?;
        Ok(result.as_array().cloned().unwrap_or_default())
    }

    pub async fn eth_get_storage_at(
        &self,
        address: &str,
        slot: &str,
        block: &str,
    ) -> Result<String> {
        let result = self
            .call("eth_getStorageAt", json!([address, slot, block]))
            .await?;
        Ok(result
            .as_str()
            .filter(|s| !s.is_empty())
            .unwrap_or("0x")
            .to_string())
    }

    /// Trace a transaction with the callTracer, keeping the node's error so a
    /// caller can tell "this node cannot trace" from "nothing to say about this
    /// transaction" — the two want different handling.
    pub async fn try_debug_trace_transaction(&self, tx_hash: &str) -> Result<Value> {
        self.call(
            "debug_traceTransaction",
            json!([tx_hash, {"tracer": "callTracer"}]),
        )
        .await
    }
}

/// Parse a `0x`-prefixed hex JSON value into a u64.
pub fn hex_to_u64(value: &Value) -> Result<u64> {
    let s = value.as_str().context("expected a hex string")?;
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.is_empty() {
        return Ok(0);
    }
    u64::from_str_radix(s, 16).with_context(|| format!("invalid hex number: {s}"))
}

/// An RPC amount as a decimal string, in whichever shape the node sent it:
/// `0x`-prefixed hex, a decimal string, or a JSON number.
///
/// Wei-scale amounts overflow every fixed-width integer, so the hex path goes
/// through a big integer and the result stays a string all the way to storage.
/// Anything that is neither a string nor a number is `"0"`.
pub fn decimal_amount(value: &Value) -> String {
    match value {
        Value::Number(n) => n.to_string(),
        Value::String(s) => match s.strip_prefix("0x") {
            // How a node spells zero once it has trimmed the leading digits.
            Some("") => "0".into(),
            Some(hex) => BigInt::parse_bytes(hex.as_bytes(), 16)
                .map(|n| n.to_string())
                .unwrap_or_else(|| s.clone()),
            // Already decimal — kept verbatim rather than guessed at.
            None => s.clone(),
        },
        _ => "0".into(),
    }
}

/// Parse a value that may be a `0x` hex string or a decimal number into a
/// signed 64-bit integer (0 on garbage).
pub fn parse_int_any(value: &Value) -> i64 {
    match value {
        Value::Number(n) => n.as_i64().unwrap_or(0),
        Value::String(s) => {
            let s = s.trim();
            if s.is_empty() {
                0
            } else if let Some(hex) = s.strip_prefix("0x") {
                u64::from_str_radix(hex, 16).map(|v| v as i64).unwrap_or(0)
            } else {
                s.parse().unwrap_or(0)
            }
        }
        _ => 0,
    }
}

/// Format an integer-like JSON value as a hex string; `0x`-prefixed string
/// values are returned unchanged.
pub fn int_to_hex_str(value: &Value) -> String {
    match value {
        Value::String(s) if s.starts_with("0x") => s.clone(),
        Value::String(s) => {
            if let Ok(n) = s.parse::<u128>() {
                format!("0x{n:x}")
            } else {
                s.clone()
            }
        }
        Value::Number(n) => {
            if let Some(i) = n.as_u128() {
                format!("0x{i:x}")
            } else {
                "0x0".into()
            }
        }
        _ => "0x0".into(),
    }
}

/// Get a string field from an RPC object with a default.
pub fn str_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Base fees and token fees pass through here on the way to storage, and
    /// nodes disagree about how to spell them.
    #[test]
    fn an_amount_is_read_however_the_node_spells_it() {
        assert_eq!(decimal_amount(&json!("0x1a")), "26");
        assert_eq!(decimal_amount(&json!("0x0")), "0");
        // How a node spells zero once it has trimmed the leading digits.
        assert_eq!(decimal_amount(&json!("0x")), "0");
        // Already decimal, or a JSON number: kept, not guessed at.
        assert_eq!(decimal_amount(&json!("1500000")), "1500000");
        assert_eq!(decimal_amount(&json!(1_500_000)), "1500000");
        // Neither a string nor a number says nothing about the amount.
        assert_eq!(decimal_amount(&Value::Null), "0");
        assert_eq!(decimal_amount(&json!({"wei": 1})), "0");
    }

    /// Wei-scale amounts run past every fixed-width integer, so the hex path
    /// has to be a big integer — this is the whole reason amounts stay strings.
    #[test]
    fn an_amount_larger_than_a_u128_survives() {
        let huge = "f".repeat(64);
        let expected = num_bigint::BigInt::parse_bytes(huge.as_bytes(), 16)
            .expect("parse")
            .to_string();
        assert_eq!(decimal_amount(&json!(format!("0x{huge}"))), expected);
        assert!(expected.len() > 70, "far past u128");
    }

    /// Undecodable hex is handed back rather than quietly becoming zero: a
    /// stored `0x…` reads as wrong, where a stored `0` reads as a real amount.
    #[test]
    fn undecodable_hex_is_not_mistaken_for_zero() {
        assert_eq!(decimal_amount(&json!("0xnot-a-number")), "0xnot-a-number");
    }

    #[test]
    fn an_integer_field_is_read_from_either_form() {
        assert_eq!(parse_int_any(&json!("0x5208")), 21_000);
        assert_eq!(parse_int_any(&json!("21000")), 21_000);
        assert_eq!(parse_int_any(&json!(21_000)), 21_000);
        assert_eq!(parse_int_any(&json!("")), 0);
        assert_eq!(parse_int_any(&json!("nonsense")), 0);
        assert_eq!(parse_int_any(&Value::Null), 0);
    }

    /// A node that answers every request with an empty JSON-RPC result and
    /// records the `x-api-key` each request carried.
    async fn key_recording_node() -> (
        String,
        std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>>,
    ) {
        use axum::{http::HeaderMap, routing::post, Json, Router};
        use std::sync::{Arc, Mutex};

        let seen = Arc::new(Mutex::new(Vec::new()));
        let record = seen.clone();
        let handler = move |headers: HeaderMap, Json(body): Json<Value>| {
            let key = headers
                .get("x-api-key")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            record.lock().unwrap_or_else(|e| e.into_inner()).push(key);
            let answer =
                |call: &Value| json!({"jsonrpc": "2.0", "id": call["id"], "result": "0x1"});
            let reply = match &body {
                Value::Array(calls) => Value::Array(calls.iter().map(answer).collect()),
                call => answer(call),
            };
            async move { Json(reply) }
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, Router::new().route("/", post(handler))).await;
        });
        (format!("http://{addr}"), seen)
    }

    /// The key goes on every JSON-RPC request, single and batched, and stays
    /// off the shared client the signature directory borrows.
    #[tokio::test]
    async fn the_api_key_rides_on_rpc_requests_only() {
        let (url, seen) = key_recording_node().await;
        let rpc = ChainRpc::new(&url)
            .expect("rpc")
            .with_api_key(Some(HeaderValue::from_static("s3cret")));

        rpc.call("eth_blockNumber", json!([])).await.expect("call");
        // Two calls: `batch_call` sends a lone call through `call`.
        rpc.batch_call(vec![
            ("eth_chainId".into(), json!([])),
            ("eth_blockNumber".into(), json!([])),
        ])
        .await
        .expect("batch");
        rpc.clone()
            .call("eth_blockNumber", json!([]))
            .await
            .expect("clone");
        rpc.http_client()
            .post(&url)
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "x", "params": []}))
            .send()
            .await
            .expect("plain");

        let seen = seen.lock().unwrap().clone();
        let key = Some("s3cret".to_string());
        assert_eq!(seen, [key.clone(), key.clone(), key, None]);
    }

    #[tokio::test]
    async fn no_api_key_sends_no_header() {
        let (url, seen) = key_recording_node().await;
        let rpc = ChainRpc::new(&url).expect("rpc");
        rpc.call("eth_blockNumber", json!([])).await.expect("call");
        assert_eq!(*seen.lock().unwrap(), [None]);
    }

    #[tokio::test]
    async fn a_paced_client_spaces_batches_and_waits_out_a_429() {
        use axum::response::IntoResponse;
        use axum::{routing::post, Json, Router};
        use std::sync::atomic::AtomicBool;

        // Turns the first batch away for a second, then answers.
        let turned_away = Arc::new(AtomicBool::new(false));
        let handler = move |Json(body): Json<Value>| async move {
            if !turned_away.swap(true, Ordering::SeqCst) {
                return (StatusCode::TOO_MANY_REQUESTS, [(RETRY_AFTER, "1")]).into_response();
            }
            let answers = body
                .as_array()
                .unwrap()
                .iter()
                .map(|c| json!({"jsonrpc": "2.0", "id": c["id"], "result": c["method"]}));
            Json(Value::Array(answers.collect())).into_response()
        };
        let rpc = ChainRpc::test_node(Router::new().route("/", post(handler)))
            .await
            .paced(Duration::from_millis(300));
        let started = Instant::now();
        // The second batch goes through a copy, which keeps the pace.
        for rpc in [rpc.clone(), rpc.with_own_connections().unwrap()] {
            let results = rpc
                .batch_call(vec![
                    ("eth_chainId".into(), json!([])),
                    ("eth_blockNumber".into(), json!([])),
                ])
                .await;
            assert_eq!(results.unwrap()[1].as_ref().unwrap(), "eth_blockNumber");
        }
        // A second for the 429, then one gap.
        assert!(started.elapsed() >= Duration::from_millis(1300));
    }
}
