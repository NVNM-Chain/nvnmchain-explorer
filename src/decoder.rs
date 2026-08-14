//! ABI decoding for calls, events, traces, and balance changes.
//!
//! ABI type parsing and argument decoding are delegated to ethers-core
//! ([`HumanReadableParser`] + the ethabi decoder), with a thin formatter on
//! top that renders addresses checksummed and integers decimal. Built-in
//! TIP-20 / ERC-20 token metadata and labels round it out.

use std::{collections::HashMap, sync::LazyLock};

use ethers_core::abi::{
    decode as abi_decode, HumanReadableParser, ParamType, Token, Uint as EthersUint,
};
use num_bigint::{BigInt, Sign};
use serde::Serialize;
use serde_json::{json, Value};
use sha3::{Digest, Keccak256};

use crate::models::Transaction;

// ---------------------------------------------------------------------------
// Raw RLP transaction parsing (tempo primitives / alloy fork)
// ---------------------------------------------------------------------------

/// Fields parsed from the canonical RLP encoding of a transaction at runtime
/// (nothing redundant is stored per column). `None`/empty when the raw bytes
/// are missing or undecodable, so callers degrade gracefully.
#[derive(Debug, Default)]
pub struct ParsedTx {
    pub gas_limit: Option<i64>,
    pub max_fee_per_gas: Option<i64>,
    pub max_priority_fee_per_gas: Option<i64>,
    pub nonce: Option<i64>,
    pub nonce_key: Option<String>,
    pub chain_id: Option<i64>,
    pub tx_type: Option<i64>,
    pub sig_type: Option<String>,
    /// The tx's top-level calls in the Execution Trace shape (without `from`
    /// /`decoded`, which the caller fills in).
    pub calls: Vec<Value>,
}

/// Decode a raw RLP transaction (`0x…` hex) with the official tempo
/// primitives (the alloy fork that knows the typed 0x76 transaction).
pub fn parse_raw_tx(raw: &str) -> ParsedTx {
    use crate::tempo::TempoTxEnvelope;
    use alloy_eips::Decodable2718;
    use alloy_primitives::TxKind;

    let Ok(bytes) = hex::decode(raw.strip_prefix("0x").unwrap_or(raw)) else {
        return ParsedTx::default();
    };
    let Ok(envelope) = TempoTxEnvelope::decode_2718(&mut &bytes[..]) else {
        return ParsedTx::default();
    };

    let mut out = ParsedTx::default();
    let tx: &dyn alloy_consensus::Transaction = match &envelope {
        TempoTxEnvelope::AA(signed) => {
            let t = signed.tx();
            out.tx_type = Some(0x76);
            out.nonce_key = Some(t.nonce_key.to_string());
            out.sig_type = Some(format!("{:?}", signed.signature().signature_type()));
            out.calls = t
                .calls
                .iter()
                .map(|c| {
                    let to = match &c.to {
                        TxKind::Call(a) => a.to_string(),
                        TxKind::Create => String::new(),
                    };
                    json!({
                        "depth": 0,
                        "type": "CALL",
                        "to": to,
                        "value": c.value.to_string(),
                        "data": format!("0x{}", hex::encode(c.input.as_ref())),
                        "decoded": Value::Null,
                        "gas": "0",
                        "gas_used": "0",
                        "children": [],
                    })
                })
                .collect();
            t
        }
        TempoTxEnvelope::Legacy(s) => {
            out.tx_type = Some(0);
            s.tx()
        }
        TempoTxEnvelope::Eip2930(s) => {
            out.tx_type = Some(1);
            s.tx()
        }
        TempoTxEnvelope::Eip1559(s) => {
            out.tx_type = Some(2);
            s.tx()
        }
        TempoTxEnvelope::Eip7702(s) => {
            out.tx_type = Some(4);
            s.tx()
        }
    };
    out.gas_limit = Some(tx.gas_limit() as i64);
    out.max_fee_per_gas = Some(tx.max_fee_per_gas() as i64);
    out.max_priority_fee_per_gas = tx.max_priority_fee_per_gas().map(|v| v as i64);
    out.nonce = Some(tx.nonce() as i64);
    out.chain_id = tx.chain_id().map(|c| c as i64);
    out
}

// ---------------------------------------------------------------------------
// Keccak / checksum helpers
// ---------------------------------------------------------------------------

pub fn keccak256(input: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(input);
    hasher.finalize().into()
}

pub fn keccak_hex(input: &[u8]) -> String {
    format!("0x{}", hex::encode(keccak256(input)))
}

/// EIP-55 checksummed address from any 40-hex-digit input (with or without 0x).
pub fn checksum_address(addr: &str) -> String {
    let lower = addr.trim().to_lowercase();
    let lower = lower.strip_prefix("0x").unwrap_or(&lower);
    if lower.len() != 40 || !lower.chars().all(|c| c.is_ascii_hexdigit()) {
        return addr.trim().to_string();
    }
    let hash = keccak256(lower.as_bytes());
    let mut out = String::with_capacity(42);
    out.push_str("0x");
    for (i, c) in lower.chars().enumerate() {
        let nibble = hash[i / 2] >> (if i % 2 == 0 { 4 } else { 0 }) & 0x0f;
        if c.is_ascii_alphabetic() && nibble >= 8 {
            out.push(c.to_ascii_uppercase());
        } else {
            out.push(c);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Decoded models (serde-serialized for the JSON API)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct DecodedParam {
    #[serde(rename = "type")]
    pub ty: String,
    pub name: String,
    pub value: String,
    pub indexed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct DecodedCall {
    pub name: Option<String>,
    pub signature: Option<String>,
    pub params: Vec<DecodedParam>,
    pub selector: String,
    pub raw_args: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DecodedEvent {
    pub name: Option<String>,
    pub signature: Option<String>,
    pub contract: String,
    pub params: Vec<DecodedParam>,
    pub topic0: String,
    pub log_index: Option<String>,
    pub transaction_hash: Option<String>,
}

impl DecodedCall {
    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

// ---------------------------------------------------------------------------
// ABI type parsing + decoding (ethers-core / ethabi)
// ---------------------------------------------------------------------------

fn word(bytes: &[u8]) -> &[u8] {
    &bytes[..bytes.len().min(32)]
}

fn big_from_word(bytes: &[u8]) -> BigInt {
    let w = word(bytes);
    let mut buf = [0u8; 32];
    buf[..w.len()].copy_from_slice(w);
    BigInt::from_bytes_be(Sign::Plus, &buf)
}

fn hex_bytes(value: &[u8]) -> String {
    format!("0x{}", hex::encode(value))
}

/// Format an ethers-decoded `Token` the way the explorer displays values:
/// addresses EIP-55 checksummed, ints/uint decimal, bytes hex, arrays and
/// tuples as `[a, b]` / `(a, b)`.
fn format_token(ty: &ParamType, token: &Token) -> String {
    match (ty, token) {
        (ParamType::Address, Token::Address(addr)) => checksum_address(&format!("{addr:x}")),
        (ParamType::Bool, Token::Bool(b)) => b.to_string(),
        (ParamType::Uint(_), Token::Uint(n)) => n.to_string(),
        (ParamType::Int(bits), Token::Int(n)) => {
            // Canonical encoding sign-extends narrow ints across the word, so
            // mask to the declared width before the two's-complement conversion.
            let bits = *bits;
            let mask = EthersUint::MAX >> (256 - bits);
            let n = *n & mask;
            let sign_bit = EthersUint::from(1u64) << (bits - 1);
            if n < sign_bit {
                n.to_string()
            } else {
                format!("-{}", (n ^ mask) + EthersUint::from(1u64))
            }
        }
        (ParamType::FixedBytes(_), Token::FixedBytes(bytes))
        | (ParamType::Bytes, Token::Bytes(bytes)) => hex_bytes(bytes),
        (ParamType::String, Token::String(s)) => s.clone(),
        (ParamType::Array(elem), Token::Array(items))
        | (ParamType::FixedArray(elem, _), Token::FixedArray(items)) => {
            let parts: Vec<String> = items.iter().map(|t| format_token(elem, t)).collect();
            format!("[{}]", parts.join(", "))
        }
        (ParamType::Tuple(types), Token::Tuple(items)) => {
            let parts: Vec<String> = types
                .iter()
                .zip(items.iter())
                .map(|(t, tok)| format_token(t, tok))
                .collect();
            format!("({})", parts.join(", "))
        }
        _ => token.to_string(),
    }
}

/// Decode ABI-encoded arguments for `types` from raw calldata (after selector).
pub fn decode_abi_args(types: &[&str], data: &[u8]) -> Vec<String> {
    let params: Vec<ParamType> = types
        .iter()
        .filter_map(|t| HumanReadableParser::parse_type(t).ok())
        .collect();
    if params.len() != types.len() {
        return Vec::new();
    }
    match abi_decode(&params, data) {
        Ok(tokens) => params
            .iter()
            .zip(tokens.iter())
            .map(|(ty, tok)| format_token(ty, tok))
            .collect(),
        Err(_) => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Known selectors / signatures
// ---------------------------------------------------------------------------

struct FnDef {
    name: &'static str,
    canonical: &'static str,
    /// (type, name) input pairs.
    inputs: &'static [(&'static str, &'static str)],
}

fn tip20_fns() -> &'static [FnDef] {
    &[
        FnDef {
            name: "transfer",
            canonical: "transfer(address,uint256)",
            inputs: &[("address", "to"), ("uint256", "amount")],
        },
        FnDef {
            name: "transferWithMemo",
            canonical: "transferWithMemo(address,uint256,bytes32)",
            inputs: &[
                ("address", "to"),
                ("uint256", "amount"),
                ("bytes32", "memo"),
            ],
        },
        FnDef {
            name: "transferFrom",
            canonical: "transferFrom(address,address,uint256)",
            inputs: &[
                ("address", "sender"),
                ("address", "to"),
                ("uint256", "amount"),
            ],
        },
        FnDef {
            name: "transferFromWithMemo",
            canonical: "transferFromWithMemo(address,address,uint256,bytes32)",
            inputs: &[
                ("address", "sender"),
                ("address", "to"),
                ("uint256", "amount"),
                ("bytes32", "memo"),
            ],
        },
        FnDef {
            name: "approve",
            canonical: "approve(address,uint256)",
            inputs: &[("address", "spender"), ("uint256", "amount")],
        },
        FnDef {
            name: "mint",
            canonical: "mint(address,uint256)",
            inputs: &[("address", "to"), ("uint256", "amount")],
        },
        FnDef {
            name: "mintWithMemo",
            canonical: "mintWithMemo(address,uint256,bytes32)",
            inputs: &[
                ("address", "to"),
                ("uint256", "amount"),
                ("bytes32", "memo"),
            ],
        },
        FnDef {
            name: "burn",
            canonical: "burn(uint256)",
            inputs: &[("uint256", "amount")],
        },
        FnDef {
            name: "burnWithMemo",
            canonical: "burnWithMemo(uint256,bytes32)",
            inputs: &[("uint256", "amount"), ("bytes32", "memo")],
        },
    ]
}

/// Functions known by signature alone, in the named form the UI shows. The
/// selector is derived from this string when [`BY_SELECTOR`] is built, so
/// there is no hand-written selector to fall out of step.
fn additional_sigs() -> &'static [&'static str] {
    &[
        "balanceOf(address account)",
        "totalSupply()",
        "name()",
        "symbol()",
        "decimals()",
        "allowance(address owner, address spender)",
        // AccountKeychain. `KeyRestrictions` is spelled as the tuple it
        // expands to, since the selector hashes the expansion. `authorizeKey`
        // is overloaded three times, each overload with its own selector.
        "authorizeKey(address keyId, uint8 signatureType, uint64 expiry, bool enforceLimits, \
         (address,uint256)[] limits)",
        "authorizeKey(address keyId, uint8 signatureType, \
         (uint64,bool,(address,uint256,uint64)[],bool,(address,(bytes4,address[])[])[]) restrictions)",
        "authorizeKey(address keyId, uint8 signatureType, \
         (uint64,bool,(address,uint256,uint64)[],bool,(address,(bytes4,address[])[])[]) restrictions, \
         bytes32 witness)",
        "authorizeAdminKey(address keyId, uint8 signatureType, bytes32 witness)",
        "burnKeyAuthorizationWitness(bytes32 witness)",
        "revokeKey(address keyId)",
        "updateSpendingLimit(address keyId, address token, uint256 newLimit)",
        "setAllowedCalls(address keyId, (address,(bytes4,address[])[])[] scopes)",
        "removeAllowedCalls(address keyId, address target)",
    ]
}

fn selector_hex(sig: &str) -> String {
    let hash = keccak256(sig.as_bytes());
    format!("0x{}", hex::encode(&hash[..4]))
}

/// How a recognised selector is decoded: a TIP-20 entry, or a signature-list
/// entry parsed when the table is built.
enum Known {
    Tip20(&'static FnDef),
    Sig(SigEntry),
}

/// A signature-list entry, parsed once at table build.
struct SigEntry {
    name: String,
    canonical: String,
    inputs: Vec<(String, String)>,
}

impl Known {
    /// Name, canonical signature, and `(type, name)` inputs.
    fn parts(&self) -> (&str, &str, Vec<(&str, &str)>) {
        match self {
            Known::Tip20(def) => (
                def.name,
                def.canonical,
                def.inputs.iter().map(|&(t, n)| (t, n)).collect(),
            ),
            Known::Sig(entry) => (
                &entry.name,
                &entry.canonical,
                entry
                    .inputs
                    .iter()
                    .map(|(t, n)| (t.as_str(), n.as_str()))
                    .collect(),
            ),
        }
    }
}

/// Selector -> function, hashed once rather than on every decode. TIP-20
/// entries take precedence.
static BY_SELECTOR: LazyLock<HashMap<String, Known>> =
    LazyLock::new(|| build_table(tip20_fns(), additional_sigs()));

/// Split out so precedence can be tested against a deliberate collision.
fn build_table(tip20: &'static [FnDef], sigs: &'static [&'static str]) -> HashMap<String, Known> {
    let mut by_selector = HashMap::new();
    for def in tip20 {
        by_selector.insert(selector_hex(def.canonical), Known::Tip20(def));
    }
    for &sig in sigs {
        // Selector, canonical form, and parameter names all derive from the
        // one spelling; an unparsable signature is skipped (caught by test).
        let Ok(fun) = HumanReadableParser::parse_function(sig) else {
            continue;
        };
        let selector = format!("0x{}", hex::encode(fun.short_signature()));
        let types: Vec<String> = fun.inputs.iter().map(|p| p.kind.to_string()).collect();
        by_selector.entry(selector).or_insert_with(|| {
            Known::Sig(SigEntry {
                canonical: format!("{}({})", fun.name, types.join(",")),
                inputs: types
                    .into_iter()
                    .zip(fun.inputs.iter().map(|p| p.name.clone()))
                    .collect(),
                name: fun.name,
            })
        });
    }
    by_selector
}

/// Pair each declared input with its value; a missing one renders empty so
/// the call's shape stays visible.
fn decoded_params(inputs: &[(&str, &str)], values: &[String]) -> Vec<DecodedParam> {
    inputs
        .iter()
        .enumerate()
        .map(|(i, (ty, name))| DecodedParam {
            ty: ty.to_string(),
            name: if name.is_empty() {
                format!("arg{i}")
            } else {
                name.to_string()
            },
            value: values.get(i).cloned().unwrap_or_default(),
            indexed: false,
        })
        .collect()
}

/// Decode 0x-prefixed calldata into a call descriptor (None for empty/trivial).
pub fn decode_function_call(data: &str) -> Option<DecodedCall> {
    let data = data.strip_prefix("0x").unwrap_or(data);
    if data.is_empty() {
        return None;
    }
    let raw = hex::decode(data).ok()?;
    if raw.len() < 4 {
        return None;
    }
    let (selector, args) = raw.split_at(4);
    let sel_hex = format!("0x{}", hex::encode(selector));

    // Both tables report the canonical signature; the names are on `params`.
    let (name, signature, params) = match BY_SELECTOR.get(&sel_hex) {
        Some(known) => {
            let (name, canonical, inputs) = known.parts();
            let types: Vec<&str> = inputs.iter().map(|&(t, _)| t).collect();
            let values = decode_abi_args(&types, args);
            (
                Some(name.to_string()),
                Some(canonical.to_string()),
                decoded_params(&inputs, &values),
            )
        }
        None => (None, None, Vec::new()),
    };

    Some(DecodedCall {
        name,
        signature,
        params,
        selector: sel_hex,
        raw_args: format!("0x{}", hex::encode(args)),
    })
}

// ---------------------------------------------------------------------------
// Event decoding
// ---------------------------------------------------------------------------

pub const TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
pub const TRANSFER_WITH_MEMO_TOPIC: &str =
    "0xab2461e5dc8495f413774182e5eb0e9f0f30a81bf32c4b7a4a1d70c3c4e2f0a";
pub const APPROVAL_TOPIC: &str =
    "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925";

fn address_from_topic(topic: &str) -> String {
    let hexed = topic.strip_prefix("0x").unwrap_or(topic);
    let addr = &hexed[hexed.len().saturating_sub(40)..];
    checksum_address(addr)
}

fn uint256_from_data(data: &str, offset: usize) -> String {
    let bytes = hex::decode(data.strip_prefix("0x").unwrap_or(data)).unwrap_or_default();
    let chunk = bytes.get(offset * 32..offset * 32 + 32).unwrap_or(&[]);
    big_from_word(chunk).to_string()
}

fn bytes32_from_data(data: &str, offset: usize) -> String {
    let bytes = hex::decode(data.strip_prefix("0x").unwrap_or(data)).unwrap_or_default();
    let chunk = bytes.get(offset * 32..offset * 32 + 32).unwrap_or(&[]);
    hex_bytes(chunk)
}

pub fn decode_event(log: &Value) -> Option<DecodedEvent> {
    let topics = log.get("topics")?.as_array()?;
    if topics.is_empty() {
        return None;
    }
    let topic0 = topics[0].as_str().unwrap_or("").to_string();
    let contract = log
        .get("address")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let log_index = log
        .get("logIndex")
        .and_then(Value::as_str)
        .map(String::from);
    let transaction_hash = log
        .get("transactionHash")
        .and_then(Value::as_str)
        .map(String::from);
    let data = log
        .get("data")
        .and_then(Value::as_str)
        .unwrap_or("0x")
        .to_string();

    let make =
        |name: &'static str, signature: &'static str, params: Vec<DecodedParam>| DecodedEvent {
            name: Some(name.to_string()),
            signature: Some(signature.to_string()),
            contract: contract.clone(),
            params,
            topic0: topic0.clone(),
            log_index: log_index.clone(),
            transaction_hash: transaction_hash.clone(),
        };

    match topic0.as_str() {
        TRANSFER_TOPIC => {
            if topics.len() < 3 {
                return Some(DecodedEvent {
                    name: Some("Transfer".into()),
                    signature: Some("Transfer(address,address,uint256)".into()),
                    contract,
                    params: Vec::new(),
                    topic0,
                    log_index,
                    transaction_hash,
                });
            }
            let from = topics[1].as_str().unwrap_or("");
            let to = topics[2].as_str().unwrap_or("");
            Some(make(
                "Transfer",
                "Transfer(address,address,uint256)",
                vec![
                    DecodedParam {
                        ty: "address".into(),
                        name: "from".into(),
                        value: address_from_topic(from),
                        indexed: true,
                    },
                    DecodedParam {
                        ty: "address".into(),
                        name: "to".into(),
                        value: address_from_topic(to),
                        indexed: true,
                    },
                    DecodedParam {
                        ty: "uint256".into(),
                        name: "amount".into(),
                        value: uint256_from_data(&data, 0),
                        indexed: false,
                    },
                ],
            ))
        }
        TRANSFER_WITH_MEMO_TOPIC => {
            if topics.len() < 3 {
                return Some(DecodedEvent {
                    name: Some("TransferWithMemo".into()),
                    signature: Some("TransferWithMemo(address,address,uint256,bytes32)".into()),
                    contract,
                    params: Vec::new(),
                    topic0,
                    log_index,
                    transaction_hash,
                });
            }
            let from = topics[1].as_str().unwrap_or("");
            let to = topics[2].as_str().unwrap_or("");
            Some(make(
                "TransferWithMemo",
                "TransferWithMemo(address,address,uint256,bytes32)",
                vec![
                    DecodedParam {
                        ty: "address".into(),
                        name: "from".into(),
                        value: address_from_topic(from),
                        indexed: true,
                    },
                    DecodedParam {
                        ty: "address".into(),
                        name: "to".into(),
                        value: address_from_topic(to),
                        indexed: true,
                    },
                    DecodedParam {
                        ty: "uint256".into(),
                        name: "amount".into(),
                        value: uint256_from_data(&data, 0),
                        indexed: false,
                    },
                    DecodedParam {
                        ty: "bytes32".into(),
                        name: "memo".into(),
                        value: bytes32_from_data(&data, 1),
                        indexed: false,
                    },
                ],
            ))
        }
        APPROVAL_TOPIC => {
            if topics.len() < 3 {
                return Some(DecodedEvent {
                    name: Some("Approval".into()),
                    signature: Some("Approval(address,address,uint256)".into()),
                    contract,
                    params: Vec::new(),
                    topic0,
                    log_index,
                    transaction_hash,
                });
            }
            let owner = topics[1].as_str().unwrap_or("");
            let spender = topics[2].as_str().unwrap_or("");
            Some(make(
                "Approval",
                "Approval(address,address,uint256)",
                vec![
                    DecodedParam {
                        ty: "address".into(),
                        name: "owner".into(),
                        value: address_from_topic(owner),
                        indexed: true,
                    },
                    DecodedParam {
                        ty: "address".into(),
                        name: "spender".into(),
                        value: address_from_topic(spender),
                        indexed: true,
                    },
                    DecodedParam {
                        ty: "uint256".into(),
                        name: "amount".into(),
                        value: uint256_from_data(&data, 0),
                        indexed: false,
                    },
                ],
            ))
        }
        _ => {
            let value = if data.len() > 200 {
                format!("{}...", &data[..200])
            } else {
                data.clone()
            };
            Some(DecodedEvent {
                name: None,
                signature: None,
                contract,
                params: vec![DecodedParam {
                    ty: "bytes".into(),
                    name: "data".into(),
                    value,
                    indexed: false,
                }],
                topic0,
                log_index,
                transaction_hash,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Trace flattening
// ---------------------------------------------------------------------------

fn hex_to_dec(value: &Value, default: &str) -> String {
    match value {
        Value::String(s) => {
            let s = s.strip_prefix("0x").unwrap_or(s);
            if s.is_empty() {
                default.into()
            } else {
                BigInt::parse_bytes(s.as_bytes(), 16)
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| default.into())
            }
        }
        Value::Number(n) => n
            .as_i64()
            .map(|i| i.to_string())
            .unwrap_or_else(|| default.into()),
        _ => default.into(),
    }
}

fn walk_trace(node: &Value, depth: usize, result: &mut Vec<Value>) {
    let input = node.get("input").and_then(Value::as_str).unwrap_or("0x");
    let decoded = decode_function_call(input).map(|d| d.to_json());
    let error = node
        .get("error")
        .or_else(|| node.get("revertReason"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    let mut flat = json!({
        "depth": depth,
        "type": node.get("type").and_then(Value::as_str).unwrap_or("CALL"),
        "from": node.get("from").and_then(Value::as_str).unwrap_or(""),
        "to": node.get("to").and_then(Value::as_str).unwrap_or(""),
        "data": input,
        "output": node.get("output").and_then(Value::as_str).unwrap_or(""),
        "value": hex_to_dec(node.get("value").unwrap_or(&Value::Null), "0"),
        "gas": hex_to_dec(node.get("gas").unwrap_or(&Value::Null), "0"),
        "gas_used": hex_to_dec(node.get("gasUsed").unwrap_or(&Value::Null), "0"),
        "decoded": decoded.unwrap_or(Value::Null),
        "children": [],
    });
    if let Some(err) = error {
        flat["status"] = json!("failed");
        flat["error"] = json!(err);
    } else {
        flat["status"] = json!("success");
    }
    let idx = result.len();
    result.push(flat);
    let children = node
        .get("calls")
        .or_else(|| node.get("children"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut child_indices = Vec::new();
    for child in &children {
        child_indices.push(result.len());
        walk_trace(child, depth + 1, result);
    }
    if let Some(entry) = result.get_mut(idx) {
        entry["children"] = json!(child_indices);
    }
}

/// Flatten a nested callTracer tree into DFS order with depth + child indices.
pub fn flatten_trace(trace: &Value) -> Vec<Value> {
    let mut result = Vec::new();
    match trace {
        Value::Null => return result,
        Value::Array(nodes) => {
            for node in nodes {
                walk_trace(node, 0, &mut result);
            }
        }
        Value::Object(_) => walk_trace(trace, 0, &mut result),
        _ => {}
    }
    result
}

// ---------------------------------------------------------------------------
// Balance changes / calls extraction
// ---------------------------------------------------------------------------

pub const FEE_MANAGER_ADDRESS: &str = "0xfeEC000000000000000000000000000000000000";

pub fn extract_balance_changes(receipt: &Value, tx: &Transaction) -> Vec<Value> {
    let mut changes = Vec::new();
    let logs = receipt
        .get("logs")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for log in &logs {
        if let Some(decoded) = decode_event(log) {
            if decoded.name.as_deref() == Some("Transfer") {
                // Checksummed: the token metadata this is rendered with is
                // looked up by that spelling.
                let token =
                    checksum_address(log.get("address").and_then(Value::as_str).unwrap_or(""));
                let mut from = String::new();
                let mut to = String::new();
                let mut amount = String::new();
                for p in &decoded.params {
                    match p.name.as_str() {
                        "from" => from = p.value.clone(),
                        "to" => to = p.value.clone(),
                        "amount" => amount = p.value.clone(),
                        _ => {}
                    }
                }
                changes.push(json!({
                    "address": from,
                    "token": token,
                    "change": format!("-{amount}"),
                    "is_fee": false,
                }));
                changes.push(json!({
                    "address": to,
                    "token": token,
                    "change": format!("+{amount}"),
                    "is_fee": false,
                }));
            }
        }
    }

    if let Some(fee_token) = tx.fee_token.as_deref() {
        let fee_ok = tx.fee_amount.parse::<i64>().map(|n| n > 0).unwrap_or(false);
        if fee_ok {
            for c in changes.iter_mut() {
                let addr = c
                    .get("address")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_lowercase();
                let token = c
                    .get("token")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_lowercase();
                if addr == FEE_MANAGER_ADDRESS.to_lowercase() && token == fee_token.to_lowercase() {
                    c["is_fee"] = json!(true);
                    c["change_type"] = json!("fee");
                }
            }
        }
    }
    changes
}

/// Extract internal calls: use the flattened trace when available, otherwise
/// fall back to the top-level `calls` field of the raw transaction.
pub fn extract_calls(tx: &Transaction, trace: &[Value]) -> Vec<Value> {
    if !trace.is_empty() {
        let mut out = trace.to_vec();
        for call in out.iter_mut() {
            if call.get("input").is_some() && call.get("data").is_none() {
                call["data"] = call["input"].clone();
            }
        }
        return out;
    }

    // Tempo-style txs carry their calls in the raw RLP object; parse it with
    // the official tempo primitives.
    let mut calls = tx
        .raw
        .as_deref()
        .map(parse_raw_tx)
        .unwrap_or_default()
        .calls;
    for call in calls.iter_mut() {
        call["from"] = json!(tx.from_addr);
        let data = call.get("data").and_then(Value::as_str).unwrap_or("0x");
        call["decoded"] = decode_function_call(data)
            .map(|d| d.to_json())
            .unwrap_or(Value::Null);
    }
    calls
}

pub fn parse_decimal_or_hex(s: &str) -> i128 {
    if let Some(h) = s.strip_prefix("0x") {
        i128::from_str_radix(h, 16).unwrap_or(0)
    } else {
        s.parse().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethers_core::abi::encode as abi_encode;

    #[test]
    fn decode_abi_args_dynamic_string() {
        let data = abi_encode(&[Token::String("hello world".into())]);
        assert_eq!(decode_abi_args(&["string"], &data), vec!["hello world"]);
        // A string spanning multiple words.
        let long = "pathUSD-pathUSD-pathUSD-pathUSD-pathUSD-pathUSD";
        let data = abi_encode(&[Token::String(long.into())]);
        assert_eq!(decode_abi_args(&["string"], &data), vec![long]);
    }

    #[test]
    fn decode_abi_args_bytes() {
        let data = abi_encode(&[Token::Bytes(vec![0xde, 0xad, 0xbe, 0xef])]);
        assert_eq!(decode_abi_args(&["bytes"], &data), vec!["0xdeadbeef"]);
        let data = abi_encode(&[Token::FixedBytes(vec![0xaa; 8])]);
        assert_eq!(
            decode_abi_args(&["bytes8"], &data),
            vec!["0xaaaaaaaaaaaaaaaa"]
        );
    }

    #[test]
    fn decode_abi_args_arrays() {
        let data = abi_encode(&[Token::Array(vec![
            Token::Uint(1u64.into()),
            Token::Uint(2u64.into()),
            Token::Uint(3u64.into()),
        ])]);
        assert_eq!(decode_abi_args(&["uint256[]"], &data), vec!["[1, 2, 3]"]);

        let data = abi_encode(&[Token::FixedArray(vec![
            Token::Bool(true),
            Token::Bool(false),
        ])]);
        assert_eq!(decode_abi_args(&["bool[2]"], &data), vec!["[true, false]"]);
    }

    #[test]
    fn decode_abi_args_tuple_and_address() {
        let addr = "0x20c0000000000000000000000000000000000000";
        let data = abi_encode(&[Token::Tuple(vec![
            Token::Uint(7u64.into()),
            Token::Address(addr.parse().unwrap()),
        ])]);
        let values = decode_abi_args(&["(uint256,address)"], &data);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], format!("(7, {})", checksum_address(addr)));
    }

    #[test]
    fn decode_abi_args_signed_int() {
        // An int8 -1 arrives sign-extended as 32 bytes of 0xff; the formatter
        // masks to the declared width first.
        let data = abi_encode(&[Token::Int(EthersUint::MAX)]);
        assert_eq!(decode_abi_args(&["int256"], &data), vec!["-1"]);
        assert_eq!(decode_abi_args(&["int8"], &data), vec!["-1"]);
        // The most negative int8, sign-extended: 0xff…80.
        let data = abi_encode(&[Token::Int(EthersUint::MAX - EthersUint::from(0x7fu64))]);
        assert_eq!(decode_abi_args(&["int8"], &data), vec!["-128"]);
        let data = abi_encode(&[Token::Int(42u64.into())]);
        assert_eq!(decode_abi_args(&["int256"], &data), vec!["42"]);
        assert_eq!(decode_abi_args(&["int8"], &data), vec!["42"]);
    }

    #[test]
    fn decode_abi_args_rejects_unparsable_types() {
        // Any unparsable type makes the whole decode degrade to no values,
        // matching the pre-ethers behavior.
        assert!(decode_abi_args(&["not-a-type"], &[0u8; 32]).is_empty());
    }

    /// A signature-list entry must not shadow a TIP-20 entry; the live tables
    /// share no selector, so build one that collides.
    #[test]
    fn tip20_wins_when_both_tables_claim_a_selector() {
        static TIP20: [FnDef; 1] = [FnDef {
            name: "burn",
            canonical: "burn(uint256)",
            inputs: &[("uint256", "amount")],
        }];
        let table = build_table(&TIP20, &["burn(uint256 value)"]);
        match table.get(&selector_hex("burn(uint256)")) {
            Some(Known::Tip20(def)) => assert_eq!(def.inputs[0].1, "amount"),
            Some(Known::Sig(entry)) => {
                panic!(
                    "signature list shadowed the TIP-20 entry: {}",
                    entry.canonical
                )
            }
            None => panic!("burn(uint256) missing from the table"),
        }
    }

    /// The table build skips an unparsable signature, which would silently
    /// stop it from matching; catch that here.
    #[test]
    fn every_signature_parses() {
        for &sig in additional_sigs() {
            assert!(
                HumanReadableParser::parse_function(sig).is_ok(),
                "`{sig}` does not parse as a human-readable signature"
            );
        }
    }
}
