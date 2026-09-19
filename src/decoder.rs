//! ABI decoding for calls, events, reverts, traces, and balance changes.
//!
//! What a selector or `topic0` means comes from [`REGISTRY`], built from the
//! `tempo-contracts` bindings, where `#[sol(abi)]` turns each `interface` into
//! a JSON ABI at compile time — the Solidity the node is built from, not a
//! copy of it. The rest is the display layer over it: it decodes the arguments
//! and renders them the way the explorer shows values, with addresses EIP-55
//! checksummed, integers decimal and bytes hex.

use std::collections::HashMap;
use std::sync::LazyLock;

use alloy_json_abi::JsonAbi;
// `ethers_core::abi::AbiError` is ethers' own error enum; the ABI *error
// definition* is ethabi's, reachable only through the re-exported crate.
use ethers_core::abi::ethabi::AbiError;
use ethers_core::abi::{
    decode as abi_decode, ethabi::RawLog, Contract, Event, Function, HumanReadableParser, Param,
    ParamType, Token, Uint as EthersUint,
};
use num_bigint::BigInt;
use serde::Serialize;
use serde_json::{json, Value};
use sha3::{Digest, Keccak256};

use crate::models::Transaction;

// ---------------------------------------------------------------------------
// The ABI registry
// ---------------------------------------------------------------------------

/// What no binding declares: the errors every Solidity `revert` produces.
///
/// Written as signatures rather than hand-built ABIs. A signature is what the
/// selector hashes, so there is one spelling to get right and it reads like
/// Solidity; a mistyped type does not parse at all, which
/// `every_local_declaration_parses` catches, and `local_selectors_are_pinned`
/// pins what they hash to so a renamed argument cannot pass unnoticed.
const LOCAL: &[(&str, &[&str])] = &[(
    "solidity",
    &["error Error(string message)", "error Panic(uint256 code)"],
)];

/// Every Tempo precompile, from the chain's own `tempo-contracts` bindings.
///
/// `#[sol(abi)]` there turns each `interface` into a JSON ABI at compile time,
/// so these are the Solidity the node is built from rather than a copy of it:
/// an interface that changes upstream arrives with a `cargo update`, and one
/// that is renamed fails to compile here.
fn tempo_contracts() -> Vec<(&'static str, JsonAbi)> {
    use tempo_contracts::precompiles::*;
    vec![
        ("tip20", tip20::ITIP20::abi::contract()),
        ("tip20_roles_auth", tip20::IRolesAuth::abi::contract()),
        (
            "tip20_factory",
            tip20_factory::ITIP20Factory::abi::contract(),
        ),
        (
            "tip20_channel_reserve",
            tip20_channel_reserve::ITIP20ChannelReserve::abi::contract(),
        ),
        (
            "tip403_registry",
            tip403_registry::ITIP403Registry::abi::contract(),
        ),
        ("fee_manager", tip_fee_manager::IFeeManager::abi::contract()),
        ("fee_amm", tip_fee_manager::ITIPFeeAMM::abi::contract()),
        (
            "stablecoin_dex",
            stablecoin_dex::IStablecoinDEX::abi::contract(),
        ),
        (
            "account_keychain",
            account_keychain::IAccountKeychain::abi::contract(),
        ),
        ("nonce", nonce::INonce::abi::contract()),
        (
            "validator_config_v2",
            validator_config_v2::IValidatorConfigV2::abi::contract(),
        ),
        (
            "validator_config",
            validator_config::IValidatorConfig::abi::contract(),
        ),
        (
            "receive_policy_guard",
            receive_policy_guard::IReceivePolicyGuard::abi::contract(),
        ),
        (
            "storage_credits",
            storage_credits::IStorageCredits::abi::contract(),
        ),
        (
            "signature_verifier",
            signature_verifier::ISignatureVerifier::abi::contract(),
        ),
        (
            "address_registry",
            address_registry::IAddressRegistry::abi::contract(),
        ),
        (
            "current_committee",
            current_committee::ICurrentCommittee::abi::contract(),
        ),
    ]
}

/// Contracts that are not Tempo's, so no binding carries them; without these their calls decode to
/// a bare selector. The canonical deployments are vendored under `abi/`; anchoring comes from the
/// nvnmchain-contracts submodule, so it cannot drift from what it decodes.
const VENDORED: &[(&str, &str)] = &[
    ("multicall3", include_str!("../abi/multicall3.json")),
    ("permit2", include_str!("../abi/permit2.json")),
    ("createx", include_str!("../abi/createx.json")),
    (
        "anchoring",
        include_str!("../contracts/layout/anchoring.abi.json"),
    ),
];

/// One parsed ABI and the name it was registered under.
struct NamedContract {
    name: &'static str,
    contract: Contract,
}

/// Everything the registry answers, built once.
pub(crate) struct Registry {
    /// Registration order, so a lookup can say which ABI a match came from.
    contracts: Vec<NamedContract>,
    /// Selector -> (contract index, function).
    functions: HashMap<[u8; 4], (usize, Function)>,
    /// `topic0` -> (contract index, event).
    events: HashMap<[u8; 32], (usize, Event)>,
    /// Selector -> (contract index, error).
    errors: HashMap<[u8; 4], (usize, AbiError)>,
}

/// Canonical `name(type,type)` signature — what the selector/topic hashes.
fn signature_of(name: &str, inputs: impl Iterator<Item = ParamType>) -> String {
    let types: Vec<String> = inputs.map(|t| t.to_string()).collect();
    format!("{name}({})", types.join(","))
}

pub(crate) fn function_signature(f: &Function) -> String {
    signature_of(&f.name, f.inputs.iter().map(|p| p.kind.clone()))
}

pub(crate) fn event_signature(e: &Event) -> String {
    signature_of(&e.name, e.inputs.iter().map(|p| p.kind.clone()))
}

fn error_signature(e: &AbiError) -> String {
    signature_of(&e.name, e.inputs.iter().map(|p| p.kind.clone()))
}

/// One [`LOCAL`] group, parsed into the same `Contract` the bindings convert to.
///
/// `parse_abi` cannot express an anonymous tuple parameter; should a
/// declaration ever need one, parse it with `HumanReadableParser` instead.
fn local_contract(name: &str, declarations: &[&str]) -> Contract {
    ethers_core::abi::parse_abi(declarations)
        .map_err(|e| tracing::error!("the local ABI `{name}` failed to parse: {e}"))
        .unwrap_or_default()
}

/// Alloy's ABI type and ethabi's are the same wire format, so one serde hop
/// bridges the bindings into the `Contract` the rest of this module uses.
fn from_json_abi(abi: &JsonAbi) -> Result<Contract, serde_json::Error> {
    serde_json::from_str(&serde_json::to_string(abi)?)
}

fn selector(signature: &str) -> [u8; 4] {
    let hash = keccak256(signature.as_bytes());
    [hash[0], hash[1], hash[2], hash[3]]
}

impl Registry {
    fn build() -> Self {
        let mut registry = Registry {
            contracts: Vec::new(),
            functions: HashMap::new(),
            events: HashMap::new(),
            errors: HashMap::new(),
        };
        // Chain-local first: nothing upstream should shadow these.
        let parsed = LOCAL
            .iter()
            .map(|(name, declarations)| (*name, local_contract(name, declarations)))
            .chain(tempo_contracts().into_iter().filter_map(|(name, abi)| {
                from_json_abi(&abi)
                    .map_err(|e| tracing::error!("binding `{name}` did not convert: {e}"))
                    .ok()
                    .map(|contract| (name, contract))
            }))
            .chain(VENDORED.iter().filter_map(|(name, json)| {
                // A malformed asset is a build-time mistake: skip it loudly
                // rather than taking the process down on a page view.
                match serde_json::from_str(json) {
                    Ok(contract) => Some((*name, contract)),
                    Err(e) => {
                        tracing::error!("vendored ABI `{name}` failed to parse: {e}");
                        None
                    }
                }
            }));
        for (name, contract) in parsed {
            let index = registry.contracts.len();
            for function in contract.functions() {
                registry
                    .functions
                    .entry(selector(&function_signature(function)))
                    .or_insert_with(|| (index, function.clone()));
            }
            for event in contract.events() {
                if event.anonymous {
                    continue; // no topic0 to key on
                }
                registry
                    .events
                    .entry(keccak256(event_signature(event).as_bytes()))
                    .or_insert_with(|| (index, event.clone()));
            }
            for error in contract.errors() {
                registry
                    .errors
                    .entry(selector(&error_signature(error)))
                    .or_insert_with(|| (index, error.clone()));
            }
            registry.contracts.push(NamedContract { name, contract });
        }
        registry
    }

    /// Every event the registry can decode, in no particular order. Exists
    /// for the test that holds the phrasing table to it.
    #[cfg(test)]
    pub(crate) fn events(&self) -> impl Iterator<Item = &Event> {
        self.events.values().map(|(_, event)| event)
    }

    /// The whole ABI registered under `name`, for the pages that list what a
    /// contract exposes.
    pub(crate) fn contract(&self, name: &str) -> Option<&Contract> {
        self.contracts
            .iter()
            .find(|c| c.name == name)
            .map(|c| &c.contract)
    }

    fn contract_name(&self, index: usize) -> &'static str {
        self.contracts.get(index).map(|c| c.name).unwrap_or("")
    }

    fn function(&self, selector: &[u8; 4]) -> Option<(&'static str, &Function)> {
        self.functions
            .get(selector)
            .map(|(i, f)| (self.contract_name(*i), f))
    }

    pub(crate) fn event(&self, topic0: &[u8; 32]) -> Option<(&'static str, &Event)> {
        self.events
            .get(topic0)
            .map(|(i, e)| (self.contract_name(*i), e))
    }

    fn error(&self, selector: &[u8; 4]) -> Option<(&'static str, &AbiError)> {
        self.errors
            .get(selector)
            .map(|(i, e)| (self.contract_name(*i), e))
    }
}

pub(crate) static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::build);

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

/// `0x12345678…9abc` — a hash or address short enough to read in a line.
pub fn truncate_hash(h: &str, prefix: usize, suffix: usize) -> String {
    if h.is_empty() {
        return String::new();
    }
    if h.len() > prefix + suffix + 3 {
        format!("{}…{}", &h[..prefix + 2], &h[h.len() - suffix..])
    } else {
        h.to_string()
    }
}

/// EIP-55 checksummed address from any 40-hex-digit input (with or without 0x).
pub fn checksum_address(addr: &str) -> String {
    let lower = addr.trim().to_lowercase();
    let lower = lower.strip_prefix("0x").unwrap_or(&lower);
    if !is_valid_address(lower) {
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

impl DecodedEvent {
    /// One argument by the name the ABI gives it. A foreign contract may emit
    /// anything under a known `topic0`, so a declared argument can still be
    /// absent — the caller decides what that means.
    pub fn param(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|p| p.name == name)
            .map(|p| p.value.as_str())
    }
}

// ---------------------------------------------------------------------------
// ABI type parsing + decoding (ethers-core / ethabi)
// ---------------------------------------------------------------------------

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
// Calls
// ---------------------------------------------------------------------------

/// Pair each declared parameter with its decoded value. One the decoder could
/// not reach renders empty, so a malformed call still shows its shape.
fn decoded_params(inputs: &[Param], data: &[u8]) -> Vec<DecodedParam> {
    let types: Vec<ParamType> = inputs.iter().map(|p| p.kind.clone()).collect();
    let tokens = abi_decode(&types, data).unwrap_or_default();
    inputs
        .iter()
        .enumerate()
        .map(|(i, p)| DecodedParam {
            ty: p.kind.to_string(),
            name: param_name(&p.name, i),
            value: tokens
                .get(i)
                .map(|t| format_token(&p.kind, t))
                .unwrap_or_default(),
            indexed: false,
        })
        .collect()
}

/// The name a parameter is shown under; interfaces may leave it blank.
fn param_name(name: &str, position: usize) -> String {
    if name.is_empty() {
        format!("arg{position}")
    } else {
        name.to_string()
    }
}

/// Split `0x…` calldata into its 4-byte selector and its arguments.
fn split_calldata(data: &str) -> Option<([u8; 4], Vec<u8>)> {
    let data = data.strip_prefix("0x").unwrap_or(data);
    if data.is_empty() {
        return None;
    }
    let raw = hex::decode(data).ok()?;
    if raw.len() < 4 {
        return None;
    }
    let (selector, args) = raw.split_at(4);
    Some((selector.try_into().ok()?, args.to_vec()))
}

/// Decode 0x-prefixed calldata (None for empty/trivial). An unrecognised
/// selector still yields a descriptor: the selector and raw arguments are
/// worth showing, and the 4-byte cache can name it later.
pub fn decode_function_call(data: &str) -> Option<DecodedCall> {
    let (selector, args) = split_calldata(data)?;
    let sel_hex = format!("0x{}", hex::encode(selector));

    let (name, signature, params) = match REGISTRY.function(&selector) {
        Some((_, function)) => (
            Some(function.name.clone()),
            Some(function_signature(function)),
            decoded_params(&function.inputs, &args),
        ),
        None => (None, None, Vec::new()),
    };

    Some(DecodedCall {
        name,
        signature,
        params,
        selector: sel_hex,
        raw_args: format!("0x{}", hex::encode(&args)),
    })
}

/// Decode calldata against a signature learned at runtime — what the 4-byte
/// directory answers with. The signature is checked against the selector
/// first, so a directory that answers with the wrong function names nothing.
pub fn decode_with_signature(data: &str, signature: &str) -> Option<DecodedCall> {
    let (selector, args) = split_calldata(data)?;
    if !crate::signatures::hashes_to(signature, &hex::encode(selector)) {
        return None;
    }
    let function = HumanReadableParser::parse_function(signature).ok()?;
    Some(DecodedCall {
        name: Some(function.name.clone()),
        signature: Some(function_signature(&function)),
        params: decoded_params(&function.inputs, &args),
        selector: format!("0x{}", hex::encode(selector)),
        raw_args: format!("0x{}", hex::encode(&args)),
    })
}

// ---------------------------------------------------------------------------
// Reverts
// ---------------------------------------------------------------------------

/// A decoded revert: the custom error the call reverted with, or Solidity's
/// built-in `Error(string)` / `Panic(uint256)`.
#[derive(Debug, Clone, Serialize)]
pub struct DecodedError {
    pub name: String,
    pub signature: String,
    pub params: Vec<DecodedParam>,
    pub selector: String,
}

impl DecodedError {
    /// `InsufficientBalance(1, 2)` — the one-line form the UI shows.
    pub fn call_form(&self) -> String {
        let args: Vec<&str> = self.params.iter().map(|p| p.value.as_str()).collect();
        format!("{}({})", self.name, args.join(", "))
    }

    /// The revert message for `revert("…")`, which carries its reason as the
    /// sole argument of the built-in `Error(string)`.
    pub fn reason(&self) -> Option<&str> {
        if self.name != "Error" {
            return None;
        }
        self.params.first().map(|p| p.value.as_str())
    }
}

/// Decode ABI-encoded revert data (`0x…`) against the built-in error table.
pub fn decode_revert(data: &str) -> Option<DecodedError> {
    let (selector, args) = split_calldata(data)?;
    let (_, error) = REGISTRY.error(&selector)?;
    Some(DecodedError {
        name: error.name.clone(),
        signature: error_signature(error),
        params: decoded_params(&error.inputs, &args),
        selector: format!("0x{}", hex::encode(selector)),
    })
}

/// The first `0x…` blob in a node's error text: some report revert data
/// inside the message rather than as a field.
pub fn revert_data_in(message: &str) -> Option<String> {
    let bytes = message.as_bytes();
    let start = message.find("0x")?;
    let end = bytes[start + 2..]
        .iter()
        .position(|c| !c.is_ascii_hexdigit())
        .map(|n| start + 2 + n)
        .unwrap_or(bytes.len());
    let hex = &message[start..end];
    // A selector at minimum; anything shorter is an address fragment or noise.
    (hex.len() >= 10).then(|| hex.to_string())
}

// ---------------------------------------------------------------------------
// Event decoding
// ---------------------------------------------------------------------------

/// The signatures of the logs the explorer decodes.
pub const TRANSFER_SIGNATURE: &str = "Transfer(address,address,uint256)";
pub const TRANSFER_WITH_MEMO_SIGNATURE: &str = "TransferWithMemo(address,address,uint256,bytes32)";
pub const APPROVAL_SIGNATURE: &str = "Approval(address,address,uint256)";

/// `keccak256` of the signature beside it, in the lowercase `0x…` form the
/// chain reports `topic0` in.
///
/// Derived, never written out: a mistyped hash does not fail loudly, it
/// silently matches nothing. `TRANSFER_WITH_MEMO_TOPIC` was 63 hex digits, so
/// no memo transfer was ever indexed.
pub static TRANSFER_TOPIC: LazyLock<String> =
    LazyLock::new(|| keccak_hex(TRANSFER_SIGNATURE.as_bytes()));
pub static TRANSFER_WITH_MEMO_TOPIC: LazyLock<String> =
    LazyLock::new(|| keccak_hex(TRANSFER_WITH_MEMO_SIGNATURE.as_bytes()));
pub static APPROVAL_TOPIC: LazyLock<String> =
    LazyLock::new(|| keccak_hex(APPROVAL_SIGNATURE.as_bytes()));

/// Whether `addr` is 20 hex-encoded bytes — the shape, not the checksum.
pub fn is_valid_address(addr: &str) -> bool {
    let s = addr.strip_prefix("0x").unwrap_or(addr);
    s.len() == 40 && s.chars().all(|c| c.is_ascii_hexdigit())
}

pub fn address_from_topic(topic: &str) -> String {
    let hexed = topic.strip_prefix("0x").unwrap_or(topic);
    let addr = &hexed[hexed.len().saturating_sub(40)..];
    checksum_address(addr)
}

/// Format one decoded log argument. An indexed dynamic parameter (string,
/// bytes, array, tuple) is in the log only as its keccak hash, so that is what
/// it renders as.
fn format_log_token(param: &Param, indexed: bool, token: &Token) -> String {
    if indexed && is_dynamic(&param.kind) {
        if let Token::FixedBytes(bytes) = token {
            return hex_bytes(bytes);
        }
    }
    format_token(&param.kind, token)
}

/// Whether an indexed parameter of this type is stored hashed in its topic.
fn is_dynamic(kind: &ParamType) -> bool {
    matches!(
        kind,
        ParamType::String
            | ParamType::Bytes
            | ParamType::Array(_)
            | ParamType::FixedArray(_, _)
            | ParamType::Tuple(_)
    )
}

/// Decode a log's arguments against an event definition. `None` when the log
/// does not fit it — a foreign contract may emit anything under a known
/// `topic0`.
fn decode_log_params(event: &Event, topics: &[Value], data: &str) -> Option<Vec<DecodedParam>> {
    let topics: Vec<ethers_core::types::H256> = topics
        .iter()
        .filter_map(Value::as_str)
        .filter_map(|t| hex::decode(t.strip_prefix("0x").unwrap_or(t)).ok())
        .filter(|b| b.len() == 32)
        .map(|b| ethers_core::types::H256::from_slice(&b))
        .collect();
    let data = hex::decode(data.strip_prefix("0x").unwrap_or(data)).ok()?;
    let decoded = event.parse_log(RawLog { topics, data }).ok()?;
    // `parse_log` returns the params in declared order, so they zip with the
    // definition the names and indexed flags come from.
    Some(
        event
            .inputs
            .iter()
            .enumerate()
            .zip(decoded.params.iter())
            .map(|((i, input), got)| {
                let param = Param {
                    name: input.name.clone(),
                    kind: input.kind.clone(),
                    internal_type: None,
                };
                DecodedParam {
                    ty: input.kind.to_string(),
                    name: param_name(&input.name, i),
                    value: format_log_token(&param, input.indexed, &got.value),
                    indexed: input.indexed,
                }
            })
            .collect(),
    )
}

/// Decode one receipt log against the built-in ABI registry. An unknown
/// `topic0` still comes back, as an unnamed event carrying its raw data, so
/// the events list never silently drops a row.
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

    let known = hex::decode(topic0.strip_prefix("0x").unwrap_or(&topic0))
        .ok()
        .filter(|b| b.len() == 32)
        .and_then(|b| {
            let mut key = [0u8; 32];
            key.copy_from_slice(&b);
            REGISTRY.event(&key)
        });

    if let Some((_, event)) = known {
        return Some(DecodedEvent {
            name: Some(event.name.clone()),
            signature: Some(event_signature(event)),
            contract,
            // A log that does not fit the definition keeps the name the topic
            // identifies it by, with no arguments invented for it.
            params: decode_log_params(event, topics, &data).unwrap_or_default(),
            topic0,
            log_index,
            transaction_hash,
        });
    }

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
    let kind = node.get("type").and_then(Value::as_str).unwrap_or("CALL");
    // A CREATE's input is the contract's init code, not calldata: its first
    // four bytes are constructor prologue, not a selector to decode or name.
    let decoded = (!kind.starts_with("CREATE"))
        .then(|| decode_function_call(input).map(|d| d.to_json()))
        .flatten();
    let error = node
        .get("error")
        .or_else(|| node.get("revertReason"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    let mut flat = json!({
        "depth": depth,
        "type": kind,
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
                let amount = decoded.param("amount").unwrap_or_default();
                changes.push(json!({
                    "address": decoded.param("from").unwrap_or_default(),
                    "token": token,
                    "change": format!("-{amount}"),
                    "is_fee": false,
                }));
                changes.push(json!({
                    "address": decoded.param("to").unwrap_or_default(),
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

    /// A binding that fails to convert, or a vendored asset with a typo, would
    /// show up only as calls and logs quietly falling back to "unknown".
    #[test]
    fn every_source_loads() {
        for (name, abi) in tempo_contracts() {
            let converted = from_json_abi(&abi);
            assert!(converted.is_ok(), "binding `{name}`: {converted:?}");
            let converted = converted.unwrap();
            assert_eq!(
                converted.functions().count(),
                abi.functions().count(),
                "`{name}` lost functions crossing into ethabi"
            );
            assert_eq!(
                converted.events().count(),
                abi.events().count(),
                "`{name}` lost events"
            );
            assert_eq!(
                converted.errors().count(),
                abi.errors().count(),
                "`{name}` lost errors"
            );
        }
        for (name, json) in VENDORED {
            let parsed: Result<Contract, _> = serde_json::from_str(json);
            assert!(parsed.is_ok(), "`{name}` failed to parse: {parsed:?}");
        }
        assert_eq!(
            REGISTRY.contracts.len(),
            LOCAL.len() + tempo_contracts().len() + VENDORED.len()
        );
    }

    /// A collapse to a handful of entries means an asset stopped loading.
    #[test]
    fn registry_covers_the_tempo_surface() {
        let (functions, events, errors) = (
            REGISTRY.functions.len(),
            REGISTRY.events.len(),
            REGISTRY.errors.len(),
        );
        assert!(functions > 240, "only {functions} functions registered");
        assert!(events > 70, "only {events} events registered");
        assert!(errors > 120, "only {errors} errors registered");
    }

    /// The selector hashes the canonical signature, not ethabi's
    /// `Function::signature()`, which appends outputs.
    #[test]
    fn signatures_are_canonical() {
        let transfer = selector("transfer(address,uint256)");
        let (_, f) = REGISTRY.function(&transfer).expect("transfer registered");
        assert_eq!(function_signature(f), "transfer(address,uint256)");
        assert_eq!(f.short_signature(), transfer);
    }

    /// `TransferWithMemo`'s memo is indexed, so its topic0 differs from the
    /// shape a non-indexed memo would hash to. Pin the real one.
    #[test]
    fn transfer_with_memo_is_registered_with_an_indexed_memo() {
        let topic0 = keccak256(b"TransferWithMemo(address,address,uint256,bytes32)");
        let (_, event) = REGISTRY
            .event(&topic0)
            .expect("TransferWithMemo registered");
        assert_eq!(event.name, "TransferWithMemo");
        let memo = event.inputs.last().expect("memo param");
        assert!(memo.indexed, "memo is indexed in the TIP-20 ABI");
    }

    /// Solidity's built-in `revert("…")` must decode like any custom error.
    #[test]
    fn builtin_revert_errors_are_registered() {
        let (_, error) = REGISTRY
            .error(&selector("Error(string)"))
            .expect("Error(string) registered");
        assert_eq!(error.name, "Error");
        assert!(REGISTRY.error(&selector("Panic(uint256)")).is_some());
    }

    /// A declaration that does not parse registers nothing, which would show
    /// up only as calls quietly failing to decode. Counted per group, so a
    /// declaration lost to a typo cannot hide behind another group's total.
    #[test]
    fn every_local_declaration_parses() {
        for (name, declarations) in LOCAL {
            let contract = local_contract(name, declarations);
            let parsed = contract.functions().count()
                + contract.events().count()
                + contract.errors().count();
            assert_eq!(parsed, declarations.len(), "in `{name}`");
        }
    }

    /// Parsing proves the declarations are well formed, not that they are the
    /// right ones, so pin what they hash to: `Error`/`Panic` are the language's
    /// own constants.
    #[test]
    fn local_selectors_are_pinned() {
        for (signature, expected) in [
            ("Error(string)", "0x08c379a0"),
            ("Panic(uint256)", "0x4e487b71"),
        ] {
            let found = format!("0x{}", hex::encode(selector(signature)));
            assert_eq!(found, expected, "for {signature}");
        }
    }

    use ethers_core::abi::encode as abi_encode;

    /// Deriving stops a topic being mistyped; pinning the values stops a
    /// signature being edited without noticing.
    #[test]
    fn topics_are_what_the_chain_reports() {
        for (topic, expected) in [
            (
                &*TRANSFER_TOPIC,
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            ),
            (
                &*TRANSFER_WITH_MEMO_TOPIC,
                "0x57bc7354aa85aed339e000bccffabbc529466af35f0772c8f8ee1145927de7f0",
            ),
            (
                &*APPROVAL_TOPIC,
                "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
            ),
        ] {
            assert_eq!(topic, expected);
        }
    }

    /// The bug: a memo transfer decoded as an unknown log, so none was ever
    /// indexed.
    #[test]
    fn a_memo_transfer_decodes() {
        let address = |byte: &str| format!("0x{}{}", "00".repeat(12), byte.repeat(20));
        let memo = format!("0x{}", "ab".repeat(32));
        let log = json!({
            "address": format!("0x{}", "cc".repeat(20)),
            // `memo` is indexed, so a real log carries it as a fourth topic
            // and `data` holds `amount` alone.
            "topics": [
                TRANSFER_WITH_MEMO_TOPIC.as_str(),
                address("11"),
                address("22"),
                memo,
            ],
            "data": format!("0x{:064x}", 1234u64),
        });
        let event = decode_event(&log).expect("decoded");
        assert_eq!(event.name.as_deref(), Some("TransferWithMemo"));
        assert_eq!(event.params[2].value, "1234");
        assert_eq!(event.params[3].name, "memo");
        assert_eq!(event.params[3].value, memo);
        assert!(event.params[3].indexed);
    }

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

    /// `revert("boom")` arrives as `Error(string)`; the reason must come back
    /// out of it rather than being shown as hex.
    #[test]
    fn decodes_a_plain_revert_string() {
        let data = format!(
            "0x08c379a0{}",
            hex::encode(ethers_core::abi::encode(&[Token::String("boom".into())]))
        );
        let decoded = decode_revert(&data).expect("Error(string) decoded");
        assert_eq!(decoded.name, "Error");
        assert_eq!(decoded.reason(), Some("boom"));
    }

    /// A custom error is named and its arguments rendered, so a failed
    /// transaction says what the contract objected to.
    #[test]
    fn decodes_a_custom_error() {
        let selector = &keccak256(b"ContractPaused()")[..4];
        let decoded =
            decode_revert(&format!("0x{}", hex::encode(selector))).expect("custom error decoded");
        assert_eq!(decoded.name, "ContractPaused");
        assert_eq!(decoded.call_form(), "ContractPaused()");

        // The TIP-20 balance error carries what was available and what was
        // needed; both must survive into the rendered form.
        let token = "0x20c0000000000000000000000000000000000000";
        let args = ethers_core::abi::encode(&[
            Token::Uint(1u64.into()),
            Token::Uint(5u64.into()),
            Token::Address(token.parse().unwrap()),
        ]);
        let selector = &keccak256(b"InsufficientBalance(uint256,uint256,address)")[..4];
        let decoded = decode_revert(&format!("0x{}{}", hex::encode(selector), hex::encode(args)))
            .expect("InsufficientBalance decoded");
        assert_eq!(decoded.name, "InsufficientBalance");
        assert_eq!(
            decoded.call_form(),
            format!("InsufficientBalance(1, 5, {})", checksum_address(token))
        );
    }

    /// Revert data an unknown contract produced has no definition to decode
    /// against; that must be a `None`, not a wrong answer.
    #[test]
    fn unknown_revert_data_decodes_to_nothing() {
        assert!(decode_revert("0xdeadbeef").is_none());
        assert!(decode_revert("0x").is_none());
    }

    /// Some nodes report revert data inside the error message rather than as
    /// a field; pull the blob back out of the prose.
    #[test]
    fn finds_revert_data_inside_an_error_message() {
        assert_eq!(
            revert_data_in("execution reverted: 0x08c379a0abcd (some detail)").as_deref(),
            Some("0x08c379a0abcd")
        );
        // Too short to be a selector — an address fragment, not revert data.
        assert_eq!(revert_data_in("reverted at 0x1234"), None);
        assert_eq!(revert_data_in("out of gas"), None);
    }
}
