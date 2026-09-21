//! The registry name index, when the node is running one.
//!
//! The contract answers a whole name and nothing else, so any part of one matches only in
//! `anchoring_searchRegistriesByName`, which a node serves when started with
//! `--anchoring.name-index`. A courtesy, never a dependency: without it the box still finds
//! a whole name, from the contract.

use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::json;

use crate::rpc::ChainRpc;

/// A keystroke must not wait on a busy node. The contract half of a suggestion shares it.
pub const TIMEOUT: Duration = Duration::from_secs(2);

/// A reader remembers the middle of a name as often as its start.
const CONTAINS: &str = "contains";

/// Rows asked for per row shown.
const OVERFETCH: usize = 8;

/// A registry the index matched, read down to what a suggestion shows.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Named {
    pub id: u64,
    pub name: String,
}

/// The node's answer.
#[derive(Deserialize)]
struct Answer {
    #[serde(default)]
    registries: Vec<Named>,
}

/// Registries whose name contains `q`, best first, at most `limit`.
///
/// Empty for anything that goes wrong: the box is better without a row than broken by a
/// node that is down, slow, or not running the index.
pub async fn matching(rpc: &ChainRpc, q: &str, limit: usize) -> Vec<Named> {
    match ask(rpc, q, limit).await {
        Ok(named) => named,
        Err(e) => {
            tracing::debug!("name index: {e}");
            Vec::new()
        }
    }
}

/// The whole name first, then the names beginning with it, then the rest, each by id.
/// Cached: the key is otherwise lowercased at every comparison.
fn rank(named: &mut [Named], lower: &str) {
    named.sort_by_cached_key(|n| {
        let name = n.name.to_lowercase();
        let tier = if name == lower {
            0
        } else if name.starts_with(lower) {
            1
        } else {
            2
        };
        (tier, n.id)
    });
}

/// The index answers by id, so the name meant can sit behind longer ones that merely
/// contain it: ask for more than will be shown, then rank.
async fn ask(rpc: &ChainRpc, q: &str, limit: usize) -> Result<Vec<Named>> {
    let call = rpc.call(
        "anchoring_searchRegistriesByName",
        json!([{"name": q, "mode": CONTAINS, "limit": limit.saturating_mul(OVERFETCH)}]),
    );
    let answer = tokio::time::timeout(TIMEOUT, call)
        .await
        .context("timed out")??;
    let mut named = serde_json::from_value::<Answer>(answer)?.registries;
    rank(&mut named, &q.to_lowercase());
    named.truncate(limit);
    Ok(named)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(names: &[&str]) -> Vec<Named> {
        names
            .iter()
            .enumerate()
            .map(|(i, name)| Named {
                id: i as u64 + 1,
                name: (*name).to_string(),
            })
            .collect()
    }

    /// The index answers by id, which buries the name the reader typed under the longer
    /// ones that merely contain it.
    #[test]
    fn the_name_typed_outranks_the_names_that_contain_it() {
        let mut hits = named(&["us-nyoytermctwash", "us-washd", "us-wash", "us-washerr"]);
        rank(&mut hits, "us-wash");
        let order: Vec<_> = hits.iter().map(|n| n.name.as_str()).collect();
        assert_eq!(
            order,
            ["us-wash", "us-washd", "us-washerr", "us-nyoytermctwash"]
        );
    }

    /// Nothing whole and nothing started: id order, as the index gave them.
    #[test]
    fn a_middle_match_keeps_the_index_order() {
        let mut hits = named(&["us-nysupctwash", "us-gasuperctwashin"]);
        rank(&mut hits, "wash");
        let order: Vec<_> = hits.iter().map(|n| n.id).collect();
        assert_eq!(order, [1, 2]);
    }

    /// The node returns an id as a number and the fields the page never shows; taking the
    /// two that matter is what keeps a later field from breaking the box.
    #[test]
    fn a_row_is_read_from_the_nodes_answer() {
        let answer = json!({"registries": [
            {"id": 7, "name": "Fund Alpha", "description": "", "creator": "nvnm1…",
             "createdAt": "2026-09-21 00:00:00 +0000 UTC", "metadata": "{}"}
        ]});
        let parsed: Answer = serde_json::from_value(answer).unwrap();
        assert_eq!(
            parsed.registries,
            [Named {
                id: 7,
                name: "Fund Alpha".into()
            }]
        );
    }
}
