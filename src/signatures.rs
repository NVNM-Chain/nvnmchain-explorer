//! Names for selectors no built-in ABI declares.
//!
//! [`crate::decoder`] covers the Tempo precompiles, but not a contract someone
//! deployed last week. Those are looked up once in a public signature
//! directory and cached, misses included.
//!
//! The lookup is a courtesy, never a dependency: an unreachable directory, a
//! slow one, or one an operator turned off all leave the bare selector.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use serde_json::Value;

use crate::config::{Settings, SIGNATURE_TTL_SECONDS};
use crate::db;
use crate::decoder::keccak256;

/// A page view must not wait on a third party for a nicety.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);

/// Selectors per request. More unknown calls than this on one page are not
/// worth a second round trip.
const MAX_LOOKUP: usize = 32;

/// Entries held before the cache starts over.
const MAX_CACHED: usize = 10_000;

/// What the directory said about each selector and when; a miss is an empty string.
pub type Cache = Mutex<HashMap<String, (String, i64)>>;

/// Signatures for `selectors`, from the cache and then the directory, keyed by
/// lowercase selector. One the directory does not know is absent from the
/// result and remembered as a miss.
pub async fn resolve(
    cache: &Cache,
    settings: &Settings,
    client: &reqwest::Client,
    selectors: &[String],
) -> HashMap<String, String> {
    let mut wanted: Vec<String> = selectors.iter().map(|s| s.to_lowercase()).collect();
    wanted.sort();
    wanted.dedup();
    if wanted.is_empty() {
        return HashMap::new();
    }

    let now = db::now_ts();
    let cached: HashMap<String, String> = {
        let held = cache.lock().unwrap_or_else(|e| e.into_inner());
        wanted
            .iter()
            .filter_map(|selector| {
                let (signature, asked) = held.get(selector)?;
                (*asked >= now - SIGNATURE_TTL_SECONDS)
                    .then(|| (selector.clone(), signature.clone()))
            })
            .collect()
    };
    let missing: Vec<String> = wanted
        .iter()
        .filter(|s| !cached.contains_key(*s))
        .take(MAX_LOOKUP)
        .cloned()
        .collect();

    // Cached misses are stored as empty strings; drop them from the answer.
    let mut resolved: HashMap<String, String> = cached
        .into_iter()
        .filter(|(_, signature)| !signature.is_empty())
        .collect();

    let Some(url) = settings.signature_lookup_url.as_deref() else {
        return resolved;
    };
    if missing.is_empty() {
        return resolved;
    }

    let fetched = fetch(client, url, &missing).await;
    // Everything asked about is remembered, so a selector the directory does
    // not know is not asked about again until the entry goes stale.
    let mut held = cache.lock().unwrap_or_else(|e| e.into_inner());
    if held.len() >= MAX_CACHED {
        held.clear();
    }
    held.extend(missing.into_iter().map(|selector| {
        let signature = fetched.get(&selector).cloned().unwrap_or_default();
        (selector, (signature, now))
    }));
    resolved.extend(fetched);
    resolved
}

/// Ask the directory about `selectors`, returning what it knew.
///
/// An answer is only believed when its signature hashes to the selector it was
/// returned for: the directory is a stranger, and a wrong name on a call is
/// worse than no name.
async fn fetch(
    client: &reqwest::Client,
    url: &str,
    selectors: &[String],
) -> HashMap<String, String> {
    let body: Value = match ask(client, url, selectors).await {
        Ok(body) => body,
        Err(e) => {
            tracing::warn!("signature lookup failed: {e}");
            return HashMap::new();
        }
    };

    let mut out = HashMap::new();
    let Some(function) = body.pointer("/result/function").and_then(Value::as_object) else {
        return out;
    };
    for (selector, entries) in function {
        let Some(name) = entries
            .as_array()
            .and_then(|list| list.first())
            .and_then(|entry| entry.get("name"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        if !hashes_to(name, selector) {
            tracing::warn!("signature directory offered `{name}` for {selector}; ignoring");
            continue;
        }
        out.insert(selector.to_lowercase(), name.to_string());
    }
    out
}

/// One GET to the directory. Unreachable, refusing and unparseable answers are
/// all the same to the caller: nothing to show.
async fn ask(
    client: &reqwest::Client,
    url: &str,
    selectors: &[String],
) -> Result<Value, reqwest::Error> {
    client
        .get(url)
        .query(&[("function", selectors.join(",")), ("filter", "true".into())])
        .timeout(LOOKUP_TIMEOUT)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
}

/// Whether `signature` is really the preimage of `selector`.
pub fn hashes_to(signature: &str, selector: &str) -> bool {
    let expected = selector
        .strip_prefix("0x")
        .unwrap_or(selector)
        .to_lowercase();
    hex::encode(&keccak256(signature.as_bytes())[..4]) == expected
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The check that makes a third party's answer safe to show.
    #[test]
    fn a_signature_is_only_believed_when_it_hashes_to_the_selector() {
        assert!(hashes_to("transfer(address,uint256)", "0xa9059cbb"));
        assert!(hashes_to("transfer(address,uint256)", "a9059cbb"));
        assert!(hashes_to("transfer(address,uint256)", "0xA9059CBB"));
        // The right shape, the wrong function.
        assert!(!hashes_to(
            "transferFrom(address,address,uint256)",
            "0xa9059cbb"
        ));
        assert!(!hashes_to("drainWallet(address)", "0xa9059cbb"));
        assert!(!hashes_to("transfer(address,uint256)", "not a selector"));
    }
}
