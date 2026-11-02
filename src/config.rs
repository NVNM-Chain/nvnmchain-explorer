//! Runtime configuration, mirroring `app/config.py`.
//!
//! Defaults target the chain we validate against; override with env vars.

use std::env;
use std::fmt;

use reqwest::header::HeaderValue;

pub const DEFAULT_RPC_URL: &str = "https://rpc.nvnm.canary.mantrachain.dev";
pub const DEFAULT_WS_URL: &str = "wss://rpc.nvnm.canary.mantrachain.dev";
/// Chain id reported by the RPC above (`eth_chainId` → 0xc0316).
pub const DEFAULT_CHAIN_ID: u64 = 787_222;
pub const DEFAULT_PORT: u16 = 8080;

/// Which parts of the explorer this process runs, read from `ROLE`.
///
/// Informational for now: every process still runs both the indexer and the
/// web server, whatever its role says.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Role {
    /// Indexer and web server in one process, read/write database.
    #[default]
    All,
    /// Indexing loops only, no server; read/write database.
    Indexer,
    /// Web server and live feed only; read-only database.
    Web,
}

impl Role {
    /// Parse a `ROLE` value, ignoring case and surrounding whitespace.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_uppercase().as_str() {
            "ALL" => Some(Self::All),
            "INDEXER" => Some(Self::Indexer),
            "WEB" => Some(Self::Web),
            _ => None,
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::All => "ALL",
            Self::Indexer => "INDEXER",
            Self::Web => "WEB",
        })
    }
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub role: Role,
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
    /// Seconds between background recomputes of the home-page stats blob.
    pub stats_interval_seconds: f64,
    /// Signature directory consulted for selectors no built-in ABI declares.
    /// Answers are cached in memory, misses included. `None` disables it
    /// — the explorer then never talks to a third party.
    pub signature_lookup_url: Option<String>,
    /// Sent as `x-api-key` on every JSON-RPC request, so the node's rate
    /// limiter can let this client through. Marked sensitive: `Debug` prints
    /// `Sensitive`, never the key.
    pub rpc_api_key: Option<HeaderValue>,
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

/// `key` parsed as a `T`, or `default` when it is unset or does not parse.
fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// A finite number of seconds; `inf` or `nan` would panic where a `Duration` is built.
fn env_f64(key: &str, default: f64) -> f64 {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .filter(|v: &f64| v.is_finite())
        .unwrap_or(default)
}

impl Settings {
    /// Every number is clamped to a usable range here, where it is read, so the
    /// rest of the explorer can build a `Duration` or a window from one as it is.
    pub fn from_env() -> Self {
        let port: u64 = env_parse("PORT", DEFAULT_PORT.into());
        Self {
            role: role(),
            rpc_url: rpc_url(),
            ws_url: env_or("WS_URL", DEFAULT_WS_URL),
            index_ws: env::var("INDEX_WS")
                .map(|v| v != "0" && v.to_lowercase() != "false")
                .unwrap_or(false),
            chain_id: env_parse("CHAIN_ID", DEFAULT_CHAIN_ID),
            host: env_or("HOST", "0.0.0.0"),
            port: u16::try_from(port).unwrap_or_else(|_| {
                tracing::warn!("PORT {port} is not a port number; listening on {DEFAULT_PORT}");
                DEFAULT_PORT
            }),
            db_path: env_or("DB_PATH", "explorer.db"),
            recent_block_count: env_parse("RECENT_BLOCK_COUNT", 15),
            recent_tx_count: env_parse("RECENT_TX_COUNT", 15),
            poll_seconds: env_f64("INDEX_POLL_SECONDS", 1.0).clamp(0.05, 3600.0),
            // A window is fetched whole before it is written, so this bounds memory too.
            batch_size: env_parse("INDEX_BATCH", 32).clamp(1, 1024),
            index_concurrency: env_parse("INDEX_CONCURRENCY", 32).clamp(1, 256),
            stats_interval_seconds: env_f64("STATS_INTERVAL_SECONDS", 5.0).clamp(1.0, 3600.0),
            signature_lookup_url: signature_lookup_url(),
            rpc_api_key: rpc_api_key(env::var("NVNM_RPC_API_KEY").ok()),
        }
    }
}

/// Read `ROLE`; unset or empty means [`Role::All`].
fn role() -> Role {
    match env::var("ROLE") {
        Ok(v) if v.trim().is_empty() => Role::default(),
        Ok(v) => Role::parse(&v).unwrap_or_else(|| {
            tracing::warn!(
                "ROLE {v:?} is not one of ALL, INDEXER, WEB; using {}",
                Role::default()
            );
            Role::default()
        }),
        Err(_) => Role::default(),
    }
}

/// The signature directory to consult, or `None` when the operator has turned
/// the lookup off with an empty `SIGNATURE_LOOKUP_URL`.
fn signature_lookup_url() -> Option<String> {
    match env::var("SIGNATURE_LOOKUP_URL") {
        Ok(url) if url.trim().is_empty() => None,
        Ok(url) => Some(url.trim().to_string()),
        Err(_) => Some(DEFAULT_SIGNATURE_LOOKUP_URL.to_string()),
    }
}

/// The `x-api-key` to send, from `NVNM_RPC_API_KEY`. Unset or empty means none:
/// a workflow secret GitHub withholds, as it does from fork PRs, arrives empty.
fn rpc_api_key(raw: Option<String>) -> Option<HeaderValue> {
    let raw = raw?;
    let key = raw.trim();
    if key.is_empty() {
        return None;
    }
    match HeaderValue::from_str(key) {
        Ok(mut value) => {
            value.set_sensitive(true);
            Some(value)
        }
        Err(_) => {
            tracing::warn!("NVNM_RPC_API_KEY is not a valid header value; sending no x-api-key");
            None
        }
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self::from_env()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_role_parses_in_any_case() {
        assert_eq!(Role::parse("ALL"), Some(Role::All));
        assert_eq!(Role::parse("indexer"), Some(Role::Indexer));
        assert_eq!(Role::parse(" Web\n"), Some(Role::Web));
    }

    #[test]
    fn an_unknown_role_is_rejected() {
        assert_eq!(Role::parse("both"), None);
        assert_eq!(Role::parse(""), None);
    }

    #[test]
    fn the_default_role_is_all() {
        assert_eq!(Role::default(), Role::All);
    }

    #[test]
    fn a_role_displays_as_its_env_value() {
        for role in [Role::All, Role::Indexer, Role::Web] {
            assert_eq!(Role::parse(&role.to_string()), Some(role));
        }
    }

    #[test]
    fn an_empty_api_key_is_no_api_key() {
        assert!(rpc_api_key(None).is_none());
        assert!(rpc_api_key(Some(String::new())).is_none());
        assert!(rpc_api_key(Some("  \n".into())).is_none());
    }

    #[test]
    fn an_api_key_is_trimmed_and_never_printed() {
        let key = rpc_api_key(Some(" s3cret\n".into())).expect("key");
        assert_eq!(key, "s3cret");
        assert!(key.is_sensitive());
        assert!(!format!("{key:?}").contains("s3cret"));
    }

    #[test]
    fn an_api_key_a_header_cannot_carry_is_dropped() {
        assert!(rpc_api_key(Some("bad\u{7f}key".into())).is_none());
    }
}
