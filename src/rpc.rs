//! Async JSON-RPC client, mirroring `app/rpc.py`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use num_bigint::BigInt;
use reqwest::Client;
use serde_json::{json, Value};

use crate::config::Settings;

pub struct ChainRpc {
    client: Client,
    url: String,
    req_id: AtomicU64,
}

impl Clone for ChainRpc {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            url: self.url.clone(),
            req_id: AtomicU64::new(self.req_id.load(Ordering::Relaxed)),
        }
    }
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

impl ChainRpc {
    pub fn new(url: impl Into<String>) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self {
            client,
            url: url.into(),
            req_id: AtomicU64::new(0),
        })
    }

    pub fn from_settings(settings: &Settings) -> Result<Self> {
        Self::new(&settings.rpc_url)
    }

    /// The shared HTTP client, for the few requests that are not JSON-RPC —
    /// the signature directory. Borrowing it keeps one connection pool per
    /// process rather than one per subsystem.
    pub fn http_client(&self) -> &Client {
        &self.client
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
            .client
            .post(&self.url)
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("RPC request {method} failed"))?;
        let body: Value = resp
            .json()
            .await
            .with_context(|| format!("RPC response for {method} was not JSON"))?;
        if let Some(err) = body.get("error").filter(|e| !e.is_null()) {
            let code = err.get("code").and_then(Value::as_i64).unwrap_or(0);
            let message = err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            let data = err.get("data").cloned();
            return Err(RpcError {
                code,
                message,
                data,
            }
            .into());
        }
        Ok(body.get("result").cloned().unwrap_or(Value::Null))
    }

    /// Send several JSON-RPC calls in a single HTTP request. Results come back
    /// in request order, each either `Ok(result)` or the per-call error.
    pub async fn batch_call(
        &self,
        calls: Vec<(String, Value)>,
    ) -> Result<Vec<Result<Value, RpcError>>> {
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
        let resp = self
            .client
            .post(&self.url)
            .json(&payload)
            .send()
            .await
            .context("batch RPC request failed")?;
        let body: Value = resp
            .json()
            .await
            .context("batch RPC response was not JSON")?;
        let items = body
            .as_array()
            .context("batch RPC response is not an array")?;
        let mut by_id: HashMap<u64, &Value> = HashMap::with_capacity(items.len());
        for item in items {
            if let Some(id) = item.get("id").and_then(Value::as_u64) {
                by_id.insert(id, item);
            }
        }
        let mut out = Vec::with_capacity(calls.len());
        for i in 0..calls.len() {
            let id = i as u64 + 1;
            match by_id.get(&id) {
                Some(item) => {
                    if let Some(err) = item.get("error").filter(|e| !e.is_null()) {
                        out.push(Err(RpcError {
                            code: err.get("code").and_then(Value::as_i64).unwrap_or(0),
                            message: err
                                .get("message")
                                .and_then(Value::as_str)
                                .unwrap_or("unknown")
                                .to_string(),
                            data: err.get("data").cloned(),
                        }));
                    } else {
                        out.push(Ok(item.get("result").cloned().unwrap_or(Value::Null)));
                    }
                }
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

    pub async fn eth_gas_price(&self) -> Result<u64> {
        let result = self.call("eth_gasPrice", json!([])).await?;
        hex_to_u64(&result).context("invalid eth_gasPrice result")
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

    pub async fn eth_get_block_by_hash(&self, hash: &str, full: bool) -> Result<Option<Value>> {
        let result = self.call("eth_getBlockByHash", json!([hash, full])).await?;
        if result.is_null() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }

    pub async fn eth_get_transaction_receipt(&self, tx_hash: &str) -> Result<Option<Value>> {
        let result = self
            .call("eth_getTransactionReceipt", json!([tx_hash]))
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

    pub async fn eth_get_transaction_by_hash(&self, tx_hash: &str) -> Result<Option<Value>> {
        let result = self
            .call("eth_getTransactionByHash", json!([tx_hash]))
            .await?;
        if result.is_null() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }

    pub async fn eth_get_balance(&self, address: &str, block: &str) -> Result<String> {
        let result = self.call("eth_getBalance", json!([address, block])).await?;
        Ok(decimal_amount(&result))
    }

    #[allow(dead_code)]
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

    pub async fn eth_get_transaction_count(&self, address: &str, block: &str) -> Result<u64> {
        let result = self
            .call("eth_getTransactionCount", json!([address, block]))
            .await?;
        hex_to_u64(&result).context("invalid eth_getTransactionCount result")
    }

    pub async fn eth_fee_history(
        &self,
        block_count: u64,
        newest_block: &str,
        reward_percentiles: Vec<f64>,
    ) -> Result<Value> {
        self.call(
            "eth_feeHistory",
            json!([
                format!("0x{block_count:x}"),
                newest_block,
                reward_percentiles
            ]),
        )
        .await
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

    /// Trace a transaction with the callTracer; `None` when tracing is
    /// unavailable. Not on the index path — kept for debugging.
    pub async fn debug_trace_transaction(&self, tx_hash: &str) -> Option<Value> {
        self.try_debug_trace_transaction(tx_hash)
            .await
            .ok()
            .filter(|v| !v.is_null())
    }

    /// Trace all transactions in a block with the callTracer. Not on the index
    /// path — kept for debugging.
    pub async fn debug_trace_block(&self, block_num: u64) -> Option<Value> {
        self.call(
            "debug_traceBlockByNumber",
            json!([format!("0x{block_num:x}"), {"tracer": "callTracer"}]),
        )
        .await
        .ok()
        .filter(|v| !v.is_null())
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

/// Resolve a block reference `"latest"`/number to a concrete height,
/// falling back to the head.
pub async fn resolve_block_ref(rpc: &ChainRpc, reference: &str) -> Result<Option<u64>> {
    if reference == "latest" || reference == "pending" || reference == "earliest" {
        return Ok(Some(rpc.eth_block_number().await?));
    }
    let s = reference.strip_prefix("0x").unwrap_or(reference);
    match u64::from_str_radix(s, 16) {
        Ok(n) => Ok(Some(n)),
        Err(_) => {
            bail!("invalid block reference: {reference}")
        }
    }
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
}
