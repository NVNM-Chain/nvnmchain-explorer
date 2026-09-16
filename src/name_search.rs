//! The registry name index, when an operator runs one.
//!
//! The contract answers a whole name and nothing else, so any part of one matches only here:
//! nvnmchain-anchoring, on the route the Cosmos module served the search on. A courtesy,
//! never a dependency -- without it the box still finds a whole name, from the contract.

use std::time::Duration;

use serde::Deserialize;

/// A keystroke must not wait on another service.
const TIMEOUT: Duration = Duration::from_secs(2);

/// `RegistryNameMatchMode.REGISTRY_NAME_MATCH_MODE_CONTAINS`. A reader types the part of
/// the name they remember, which is as often the middle as the start.
const CONTAINS: &str = "4";

/// Rows asked for per row shown: the index answers by id, so the name meant can sit behind
/// longer ones that merely contain it.
const OVERFETCH: usize = 8;

const SEARCH_PATH: &str = "/NVNM-Chain/nvnmchain/anchoring/v1/registries/search";

/// A registry the index matched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Named {
    pub id: u64,
    pub name: String,
}

/// The service's answer. Proto JSON, so `id` is a string.
#[derive(Deserialize)]
struct Answer {
    registries: Option<Vec<Row>>,
}

#[derive(Deserialize)]
struct Row {
    id: String,
    name: String,
}

/// Registries whose name contains `q`, best first, at most `limit`.
///
/// Empty for anything that goes wrong: the box is better without a row than broken by a
/// service that is down, slow, or answering something else.
pub async fn matching(client: &reqwest::Client, base: &str, q: &str, limit: usize) -> Vec<Named> {
    match ask(client, base, q, limit.saturating_mul(OVERFETCH)).await {
        Ok(mut named) => {
            rank(&mut named, &q.to_lowercase());
            named.truncate(limit);
            named
        }
        Err(e) => {
            tracing::debug!("name index: {e}");
            Vec::new()
        }
    }
}

/// The whole name first, then the names beginning with it, then the rest, each by id.
/// Cached, or the key is lowercased again at every comparison.
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

async fn ask(
    client: &reqwest::Client,
    base: &str,
    q: &str,
    limit: usize,
) -> reqwest::Result<Vec<Named>> {
    let answer: Answer = client
        .get(format!("{}{SEARCH_PATH}", base.trim_end_matches('/')))
        .query(&[
            ("name", q),
            ("mode", CONTAINS),
            ("pagination.limit", &limit.to_string()),
        ])
        .timeout(TIMEOUT)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(answer
        .registries
        .unwrap_or_default()
        .into_iter()
        .filter_map(|row| {
            Some(Named {
                id: row.id.parse().ok()?,
                name: row.name,
            })
        })
        .collect())
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
}
