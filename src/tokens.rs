//! TIP-20 / ERC-20 token metadata fetching and amount formatting,
//! mirroring `app/tokens.py`.

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};

use crate::decoder::checksum_address;
use crate::rpc::ChainRpc;

const NAME_CALL: &str = "0x06fdde03";
const SYMBOL_CALL: &str = "0x95d89b41";
const DECIMALS_CALL: &str = "0x313ce567";
const TOTAL_SUPPLY_CALL: &str = "0x18160ddd";

#[derive(Debug, Clone, Serialize)]
pub struct TokenMeta {
    pub address: String,
    pub name: String,
    pub symbol: String,
    pub decimals: i64,
    pub currency: String,
    pub total_supply: String,
}

/// Decode an ABI-encoded `string` return value (e.g. `name()` / `symbol()`).
///
/// A dynamic `string` is encoded with an offset indirection, even when it is
/// short:
///
/// ```text
/// word 0 = byte offset to the length word (0x20 for a single string)
/// word 1 = byte length of the UTF-8 payload
/// word 2+ = payload, right-padded to a 32-byte boundary
/// ```
///
/// A few non-standard contracts return the string in place (length in word 0),
/// so that shape is accepted as a fallback.
pub fn decode_string_result(raw: &str) -> String {
    if raw.is_empty() || raw == "0x" {
        return String::new();
    }
    let bytes = match hex::decode(raw.strip_prefix("0x").unwrap_or(raw)) {
        Ok(b) => b,
        Err(_) => return String::new(),
    };
    if bytes.len() < 32 {
        return String::new();
    }
    let offset = u64::from_be_bytes(bytes[24..32].try_into().unwrap_or([0; 8])) as usize;
    // Standard dynamic-string encoding: the first word is an offset into the
    // buffer pointing at the length word, followed by the payload.
    if offset >= 32 && offset.checked_add(32).is_some_and(|end| end <= bytes.len()) {
        let len = u64::from_be_bytes(bytes[offset + 24..offset + 32].try_into().unwrap_or([0; 8]))
            as usize;
        // The length word is the contract's to fill, so a length past the end of
        // the payload reads what there is rather than panicking.
        let payload = &bytes[offset + 32..];
        return String::from_utf8_lossy(&payload[..len.min(payload.len())]).into_owned();
    }
    // Fallback: short string encoded in place (length in word 0).
    let len = offset.min(bytes.len().saturating_sub(32));
    String::from_utf8_lossy(&bytes[32..32 + len]).into_owned()
}

fn decode_uint_result(raw: &str, default: i64) -> i64 {
    if raw.is_empty() || raw == "0x" {
        return default;
    }
    let bytes = match hex::decode(raw.strip_prefix("0x").unwrap_or(raw)) {
        Ok(b) => b,
        Err(_) => return default,
    };
    if bytes.len() < 32 {
        return default;
    }
    u64::from_be_bytes(bytes[24..32].try_into().unwrap_or([0; 8])) as i64
}

fn decode_uint256_result(raw: &str) -> String {
    if raw.is_empty() || raw == "0x" {
        return "0".into();
    }
    let bytes = match hex::decode(raw.strip_prefix("0x").unwrap_or(raw)) {
        Ok(b) => b,
        Err(_) => return "0".into(),
    };
    if bytes.len() < 32 {
        return "0".into();
    }
    num_bigint::BigInt::from_bytes_be(num_bigint::Sign::Plus, &bytes).to_string()
}

/// Clean a decoded `name()`/`symbol()` string. Real token metadata is
/// printable text; a mis-decoded ABI value (e.g. a length word read as the
/// payload) is full of NUL/control characters and should degrade to the
/// empty string rather than render as garbage.
pub fn sanitize_metadata_text(s: &str) -> String {
    let trimmed = s.trim_end_matches('\0');
    if trimmed.chars().any(|c| c.is_control()) {
        String::new()
    } else {
        trimmed.trim().to_string()
    }
}

/// Whether a stored name/symbol carries control characters — the signature of
/// a value written by the pre-fix string decoder — and therefore needs a
/// re-fetch.
pub fn has_control_chars(s: &str) -> bool {
    s.chars().any(|c| c.is_control())
}

/// Read several of a token's views at the chain head in one HTTP request. A view
/// the token lacks comes back as `"0x"`, which the decoders read as "no answer";
/// a request that never arrives is an error.
async fn view_calls<const N: usize>(
    rpc: &ChainRpc,
    address: &str,
    selectors: &[&str; N],
) -> Result<[String; N]> {
    let calls: Vec<(String, Value)> = selectors
        .iter()
        .map(|data| {
            (
                "eth_call".to_string(),
                json!([{"to": address, "data": data}, "latest"]),
            )
        })
        .collect();
    let results = rpc
        .batch_call(calls)
        .await
        .with_context(|| format!("reading token views for {address}"))?;
    Ok(std::array::from_fn(|i| {
        results
            .get(i)
            .and_then(|result| result.as_ref().ok())
            .and_then(Value::as_str)
            .filter(|hex| !hex.is_empty())
            .unwrap_or("0x")
            .to_string()
    }))
}

/// Fetch TIP-20 token metadata from the chain. An address answering to no view
/// has an empty name and symbol; an unreachable node is an error, so a token is
/// never stored from an answer the node never gave.
pub async fn fetch_token_metadata(rpc: &ChainRpc, address: &str) -> Result<TokenMeta> {
    let checksummed = checksum_address(address);

    // One request per token, reserved addresses included: a chain is free to
    // name the tokens genesis puts there whatever it likes.
    let [name, symbol, decimals_raw, supply_raw] = view_calls(
        rpc,
        &checksummed,
        &[NAME_CALL, SYMBOL_CALL, DECIMALS_CALL, TOTAL_SUPPLY_CALL],
    )
    .await?;

    // Guard against stale/bad decodes: a mis-decoded ABI word must not be
    // stored or rendered as a name/symbol.
    let name = sanitize_metadata_text(&decode_string_result(&name));
    let symbol = sanitize_metadata_text(&decode_string_result(&symbol));

    Ok(TokenMeta {
        address: checksummed,
        name,
        currency: infer_currency(&symbol),
        symbol,
        decimals: decode_uint_result(&decimals_raw, 18),
        total_supply: decode_uint256_result(&supply_raw),
    })
}

fn infer_currency(symbol: &str) -> String {
    let upper = symbol.to_uppercase();
    if upper.ends_with("USD") || matches!(upper.as_str(), "USDC" | "USDT" | "DAI" | "FRAX") {
        "USD".into()
    } else if upper.ends_with("EUR") {
        "EUR".into()
    } else {
        symbol.to_string()
    }
}

/// Format a token amount with up to six significant decimal places.
pub fn format_token_amount(amount: &str, decimals: i64) -> String {
    let amount = num_bigint::BigInt::parse_bytes(amount.as_bytes(), 10)
        .unwrap_or_else(|| num_bigint::BigInt::from(0));
    if amount.sign() == num_bigint::Sign::NoSign {
        return "0".into();
    }
    // The sign comes off before dividing: a quotient carries it only from a whole
    // token up, so -0.01 would divide to a zero that is not negative.
    let (sign, amount) = match amount.sign() {
        num_bigint::Sign::Minus => ("-", -amount),
        _ => ("", amount),
    };
    let decimals = decimals.clamp(0, 30) as u32;
    let divisor = num_bigint::BigInt::from(10u8).pow(decimals);
    let integer_part = &amount / &divisor;
    let mut frac = (&amount % &divisor).to_string();
    if (frac.len() as u32) < decimals {
        frac = format!("{}{}", "0".repeat(decimals as usize - frac.len()), frac);
    }
    while frac.ends_with('0') {
        frac.pop();
    }
    if frac.is_empty() {
        return format!("{sign}{integer_part}");
    }
    if frac.len() > 6 {
        frac.truncate(6);
    }
    format!("{sign}{integer_part}.{frac}")
}

pub fn format_token_amount_with_symbol(amount: &str, decimals: i64, symbol: &str) -> String {
    let formatted = format_token_amount(amount, decimals);
    if symbol.is_empty() {
        formatted
    } else {
        format!("{formatted} {symbol}")
    }
}

/// Convenience JSON wrapper for the token page.
pub fn token_to_json(meta: &TokenMeta) -> serde_json::Value {
    json!(meta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::RESERVED_TOKENS;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    /// A `name()`/`symbol()` return value in the ABI's dynamic-string encoding:
    /// an offset word, a length word, then the payload padded out to a word.
    fn abi_string(value: &str) -> String {
        let bytes = value.as_bytes();
        let mut padded = bytes.to_vec();
        padded.resize(bytes.len().div_ceil(32).max(1) * 32, 0);
        format!("0x{:064x}{:064x}{}", 32, bytes.len(), hex::encode(padded))
    }

    /// A `uint256` return value.
    fn abi_uint(value: u64) -> String {
        format!("0x{value:064x}")
    }

    /// A node that answers `eth_call` from a selector table, recording what it
    /// was asked and how many HTTP requests carried the questions.
    struct StubNode {
        url: String,
        requests: Arc<AtomicUsize>,
        asked: Arc<Mutex<Vec<String>>>,
    }

    impl StubNode {
        fn requests(&self) -> usize {
            self.requests.load(Ordering::SeqCst)
        }

        fn asked(&self) -> Vec<String> {
            self.asked.lock().unwrap_or_else(|e| e.into_inner()).clone()
        }
    }

    async fn stub_node(answers: Vec<(&'static str, String)>) -> StubNode {
        use axum::{routing::post, Json, Router};

        let requests = Arc::new(AtomicUsize::new(0));
        let asked: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let table = Arc::new(answers);

        let handler = {
            let (requests, asked, table) = (requests.clone(), asked.clone(), table.clone());
            move |Json(body): Json<Value>| {
                requests.fetch_add(1, Ordering::SeqCst);
                let mut out = Vec::new();
                for call in body.as_array().cloned().unwrap_or_default() {
                    let data = call
                        .pointer("/params/0/data")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    asked
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(data.clone());
                    // A view the token does not implement answers empty, which
                    // is what a node returns for a call to a missing selector.
                    let result = table
                        .iter()
                        .find(|(selector, _)| *selector == data)
                        .map(|(_, hex)| hex.clone())
                        .unwrap_or_else(|| "0x".into());
                    out.push(json!({
                        "jsonrpc": "2.0",
                        "id": call.get("id").cloned().unwrap_or(Value::Null),
                        "result": result,
                    }));
                }
                async move { Json(Value::Array(out)) }
            }
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, Router::new().route("/", post(handler))).await;
        });
        StubNode {
            url: format!("http://{addr}"),
            requests,
            asked,
        }
    }

    /// The indexer fetches metadata for every token a block introduces, so the
    /// cost of a token is the cost of one round trip, not four.
    #[tokio::test]
    async fn a_token_is_read_in_one_request() {
        let node = stub_node(vec![
            (NAME_CALL, abi_string("Test USD")),
            (SYMBOL_CALL, abi_string("TUSD")),
            (DECIMALS_CALL, abi_uint(6)),
            (TOTAL_SUPPLY_CALL, abi_uint(1_000_000)),
        ])
        .await;
        let rpc = ChainRpc::new(&node.url).expect("rpc");
        let token = "0x3333333333333333333333333333333333333333";

        let meta = fetch_token_metadata(&rpc, token).await.expect("fetched");
        assert_eq!(meta.address, checksum_address(token));
        assert_eq!(meta.name, "Test USD");
        assert_eq!(meta.symbol, "TUSD");
        assert_eq!(meta.decimals, 6);
        assert_eq!(meta.total_supply, "1000000");
        assert_eq!(meta.currency, "USD", "inferred from the symbol");
        assert_eq!(node.requests(), 1, "four views, one round trip");
        assert_eq!(node.asked().len(), 4);
    }

    /// A token at a reserved address is named by the chain, not by the
    /// explorer: a chain that renames it is reported under the new name.
    #[tokio::test]
    async fn a_reserved_token_is_named_by_the_chain() {
        let node = stub_node(vec![
            (NAME_CALL, abi_string("nUSD")),
            (SYMBOL_CALL, abi_string("nUSD")),
            (DECIMALS_CALL, abi_uint(6)),
            (TOTAL_SUPPLY_CALL, abi_uint(42)),
        ])
        .await;
        let rpc = ChainRpc::new(&node.url).expect("rpc");

        let meta = fetch_token_metadata(&rpc, RESERVED_TOKENS[0])
            .await
            .expect("fetched");
        assert_eq!(meta.symbol, "nUSD");
        assert_eq!(meta.decimals, 6);
        assert_eq!(meta.total_supply, "42");
        assert_eq!(node.requests(), 1, "four views, one round trip");
    }

    /// A token missing a view is still a token: whatever it does answer lands,
    /// and the rest take the ERC-20 defaults.
    #[tokio::test]
    async fn a_token_that_answers_only_some_views_still_gets_a_row() {
        let node = stub_node(vec![(SYMBOL_CALL, abi_string("ODD"))]).await;
        let rpc = ChainRpc::new(&node.url).expect("rpc");

        let meta = fetch_token_metadata(&rpc, "0x5555555555555555555555555555555555555555")
            .await
            .expect("fetched");
        assert_eq!(meta.symbol, "ODD");
        assert_eq!(meta.name, "");
        assert_eq!(meta.decimals, 18, "the ERC-20 default");
        assert_eq!(meta.total_supply, "0");
    }

    /// Under one whole token the quotient is zero, which carries no sign, so an
    /// amount owed would otherwise format as one held.
    #[test]
    fn an_amount_under_one_token_keeps_its_sign() {
        for (amount, decimals, want) in [
            ("-10000", 6, "-0.01"),
            ("-4000", 6, "-0.004"),
            ("-1003000", 6, "-1.003"),
            ("-5", 1, "-0.5"),
            ("-1000000", 6, "-1"),
            ("10000", 6, "0.01"),
        ] {
            assert_eq!(
                format_token_amount(amount, decimals),
                want,
                "{amount} at {decimals}"
            );
        }
    }

    /// A node that will not answer is an error, not a token with no name.
    #[tokio::test]
    async fn an_unreachable_node_is_an_error_not_a_nameless_token() {
        let rpc = ChainRpc::new("http://127.0.0.1:1").expect("rpc");
        let err = fetch_token_metadata(&rpc, "0x4444444444444444444444444444444444444444")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.starts_with("reading token views for 0x4444"), "{err}");
    }
}
