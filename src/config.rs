//! Runtime configuration, mirroring `app/config.py`.
//!
//! Defaults target the chain we validate against; override with env vars.

use std::env;

pub const DEFAULT_RPC_URL: &str = "https://rpc.nvnm.canary.mantrachain.dev";
pub const DEFAULT_WS_URL: &str = "wss://ws.nvnm.canary.mantrachain.dev";
/// Chain id reported by the RPC above (`eth_chainId` → 0xc0316).
pub const DEFAULT_CHAIN_ID: u64 = 787_222;

#[derive(Debug, Clone)]
pub struct Settings {
    pub rpc_url: String,
    pub ws_url: String,
    pub index_ws: bool,
    pub chain_id: u64,
    pub host: String,
    pub port: u16,
    pub db_path: String,
    pub recent_block_count: usize,
    pub recent_tx_count: usize,
    /// Seconds between poll cycles when the WebSocket feed is unavailable.
    pub poll_seconds: f64,
    /// Blocks indexed per poll cycle (forward and backfill each).
    pub batch_size: u64,
    /// Max blocks fetched in parallel by the indexer.
    pub index_concurrency: usize,
    /// Symbol shown for the native gas/currency token.
    pub native_symbol: String,
    /// Seconds between background recomputes of the home-page stats blob.
    pub stats_interval_seconds: f64,
    /// Signature directory consulted for selectors no built-in ABI declares.
    /// Answers are cached in the database, misses included. `None` disables it
    /// — the explorer then never talks to a third party.
    pub signature_lookup_url: Option<String>,
    /// nvnmchain-anchoring's registry name index, which is what matches half a name.
    pub name_search_url: Option<String>,
}

/// OpenChain's signature directory, queried at most once per selector per
/// [`SIGNATURE_TTL_SECONDS`].
pub const DEFAULT_SIGNATURE_LOOKUP_URL: &str =
    "https://api.openchain.xyz/signature-database/v1/lookup";

/// How long a cached answer — a miss included — stands before asking again.
pub const SIGNATURE_TTL_SECONDS: i64 = 7 * 24 * 60 * 60;

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Read `NVNM_RPC`, falling back to the legacy `TEMPO_RPC` variable.
fn rpc_url() -> String {
    env::var("NVNM_RPC")
        .or_else(|_| env::var("TEMPO_RPC"))
        .unwrap_or_else(|_| DEFAULT_RPC_URL.to_string())
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn env_f64(key: &str, default: f64) -> f64 {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

impl Settings {
    pub fn from_env() -> Self {
        Self {
            rpc_url: rpc_url(),
            ws_url: env_or("WS_URL", DEFAULT_WS_URL),
            index_ws: env::var("INDEX_WS")
                .map(|v| v != "0" && v.to_lowercase() != "false")
                .unwrap_or(false),
            chain_id: env_u64("CHAIN_ID", DEFAULT_CHAIN_ID),
            host: env_or("HOST", "0.0.0.0"),
            port: env_u64("PORT", 8080) as u16,
            db_path: env_or("DB_PATH", "explorer.db"),
            recent_block_count: env_usize("RECENT_BLOCK_COUNT", 15),
            recent_tx_count: env_usize("RECENT_TX_COUNT", 15),
            poll_seconds: env_f64("INDEX_POLL_SECONDS", 1.0),
            batch_size: env_u64("INDEX_BATCH", 32),
            index_concurrency: env_usize("INDEX_CONCURRENCY", 32),
            native_symbol: env_or("NATIVE_SYMBOL", "NVNM"),
            stats_interval_seconds: env_f64("STATS_INTERVAL_SECONDS", 5.0),
            signature_lookup_url: signature_lookup_url(),
            name_search_url: trimmed("NAME_SEARCH_URL"),
        }
    }
}

/// The signature directory to consult, or `None` when the operator has turned
/// the lookup off with an empty `SIGNATURE_LOOKUP_URL`.
/// An optional URL from the environment: unset or blank is `None`.
fn trimmed(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn signature_lookup_url() -> Option<String> {
    match env::var("SIGNATURE_LOOKUP_URL") {
        Ok(url) if url.trim().is_empty() => None,
        Ok(url) => Some(url.trim().to_string()),
        Err(_) => Some(DEFAULT_SIGNATURE_LOOKUP_URL.to_string()),
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self::from_env()
    }
}
