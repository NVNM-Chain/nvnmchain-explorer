//! Contract identification and labels, mirroring `app/contracts.py`.

use serde::Serialize;
use std::collections::HashMap;
use std::sync::OnceLock;

use crate::decoder::checksum_address;

fn precompile_labels() -> &'static HashMap<String, String> {
    static MAP: OnceLock<HashMap<String, String>> = OnceLock::new();
    MAP.get_or_init(|| {
        let pairs = [
            ("0xfeEC000000000000000000000000000000000000", "Fee Manager"),
            (
                "0x403c000000000000000000000000000000000000",
                "TIP-403 Registry",
            ),
            (
                "0x20Fc000000000000000000000000000000000000",
                "TIP-20 Factory",
            ),
            (
                "0x4D50500000000000000000000000000000000000",
                "TIP-20 Channel Reserve",
            ),
            (
                "0xDEc0000000000000000000000000000000000000",
                "Stablecoin DEX",
            ),
            (
                "0x4e4F4E4345000000000000000000000000000000",
                "Nonce Manager",
            ),
            (
                "0xCccCcCCC00000000000000000000000000000000",
                "Validator Config V1 (legacy)",
            ),
            (
                "0xCccCcCCC00000000000000000000000000000001",
                "Validator Config",
            ),
            (
                "0xaAAAaaAA00000000000000000000000000000000",
                "Account Keychain",
            ),
            (
                "0xFDC0000000000000000000000000000000000000",
                "Address Registry",
            ),
            (
                "0x5165300000000000000000000000000000000000",
                "Signature Verifier",
            ),
            (
                "0xB10C000000000000000000000000000000000000",
                "Receive Policy Guard",
            ),
            (
                "0x1060000000000000000000000000000000000000",
                "Storage Credits",
            ),
            (
                "0xC077e00000000000000000000000000000000000",
                "Current Committee",
            ),
            (crate::anchoring::ANCHORING_ADDRESS, "Anchoring"),
        ];
        pairs
            .iter()
            .map(|(a, label)| (checksum_address(a), label.to_string()))
            .collect()
    })
}

/// Contracts that are not Tempo's own but are deployed at canonical addresses
/// everywhere, including here. Kept apart from the precompiles: they are
/// ordinary contracts, and calling them precompiles would misreport what they
/// are on their own page.
fn deployed_contracts() -> &'static HashMap<String, String> {
    static MAP: OnceLock<HashMap<String, String>> = OnceLock::new();
    MAP.get_or_init(|| {
        [
            ("0xcA11bde05977b3631167028862bE2a173976CA11", "Multicall3"),
            ("0x000000000022D473030F116dDEE9F6B43aC78BA3", "Permit2"),
            ("0xba5Ed099633D3B313e4D5F7bdc1305d3c28ba5Ed", "CreateX"),
        ]
        .iter()
        .map(|(a, label)| (checksum_address(a), label.to_string()))
        .collect()
    })
}

/// Tokens genesis puts at reserved addresses, spelled as metadata rows store
/// them. Listed only so rows an older build named from a table can be refreshed.
pub const RESERVED_TOKENS: [&str; 4] = [
    "0x20C0000000000000000000000000000000000000",
    "0x20C0000000000000000000000000000000000001",
    "0x20C0000000000000000000000000000000000002",
    "0x20C0000000000000000000000000000000000003",
];

/// TIP-20 native token prefix: all StdTokens start with 0x20C000...
pub const TIP20_TOKEN_PREFIX: &str = "0x20c0000000000000000";

pub fn is_precompile_address(addr: &str) -> bool {
    precompile_labels().contains_key(&checksum_address(addr))
}

pub fn get_precompile_name(addr: &str) -> Option<String> {
    precompile_labels().get(&checksum_address(addr)).cloned()
}

pub fn is_tip20_token(addr: &str) -> bool {
    addr.trim().to_lowercase().starts_with(TIP20_TOKEN_PREFIX)
}

pub fn is_contract(addr: &str) -> bool {
    let checksummed = checksum_address(addr);
    precompile_labels().contains_key(&checksummed)
        || deployed_contracts().contains_key(&checksummed)
        || is_tip20_token(addr)
}

pub fn get_contract_name(addr: &str) -> Option<String> {
    let checksummed = checksum_address(addr);
    precompile_labels()
        .get(&checksummed)
        .or_else(|| deployed_contracts().get(&checksummed))
        .cloned()
}

pub fn is_eoa(addr: &str) -> bool {
    !is_contract(addr)
}

#[derive(Debug, Serialize)]
pub struct AddressInfo {
    #[serde(rename = "type")]
    pub kind: String,
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
}

pub fn identify_address(addr: &str) -> AddressInfo {
    let checksummed = checksum_address(addr);
    if let Some(label) = precompile_labels().get(&checksummed) {
        return AddressInfo {
            kind: "precompile".into(),
            label: Some(label.clone()),
            symbol: None,
        };
    }
    if let Some(label) = deployed_contracts().get(&checksummed) {
        return AddressInfo {
            kind: "contract".into(),
            label: Some(label.clone()),
            symbol: None,
        };
    }
    if is_tip20_token(&checksummed) {
        return AddressInfo {
            kind: "token".into(),
            label: None,
            symbol: None,
        };
    }
    AddressInfo {
        kind: "eoa".into(),
        label: None,
        symbol: None,
    }
}

/// The name of a canonical deployed contract (Multicall3, Permit2, CreateX).
pub fn deployed_contract_name(addr: &str) -> Option<String> {
    deployed_contracts().get(&checksum_address(addr)).cloned()
}

/// Which built-in ABIs describe the contract at `addr`, by the names the
/// decoder's registry loads them under. Some addresses need more than one:
/// the fee manager's interface says nothing about the AMM it runs.
pub fn abis_for_address(addr: &str) -> &'static [&'static str] {
    /// Addresses spelled lowercase, so the table cannot disagree with EIP-55
    /// over a character. Lookups normalize the same way.
    const BY_ADDRESS: &[(&str, &[&str])] = &[
        (
            "0xfeec000000000000000000000000000000000000",
            &["fee_manager", "fee_amm"],
        ),
        (
            "0x403c000000000000000000000000000000000000",
            &["tip403_registry"],
        ),
        (
            "0x20fc000000000000000000000000000000000000",
            &["tip20_factory"],
        ),
        (
            "0x4d50500000000000000000000000000000000000",
            &["tip20_channel_reserve"],
        ),
        (
            "0xdec0000000000000000000000000000000000000",
            &["stablecoin_dex"],
        ),
        ("0x4e4f4e4345000000000000000000000000000000", &["nonce"]),
        (
            "0xcccccccc00000000000000000000000000000000",
            &["validator_config"],
        ),
        (
            "0xcccccccc00000000000000000000000000000001",
            &["validator_config_v2"],
        ),
        (
            "0xaaaaaaaa00000000000000000000000000000000",
            &["account_keychain"],
        ),
        (
            "0xfdc0000000000000000000000000000000000000",
            &["address_registry"],
        ),
        (
            "0x5165300000000000000000000000000000000000",
            &["signature_verifier"],
        ),
        (
            "0xb10c000000000000000000000000000000000000",
            &["receive_policy_guard"],
        ),
        (
            "0x1060000000000000000000000000000000000000",
            &["storage_credits"],
        ),
        (
            "0xc077e00000000000000000000000000000000000",
            &["current_committee"],
        ),
        (
            "0xca11bde05977b3631167028862be2a173976ca11",
            &["multicall3"],
        ),
        ("0x000000000022d473030f116ddee9f6b43ac78ba3", &["permit2"]),
        ("0xba5ed099633d3b313e4d5f7bdc1305d3c28ba5ed", &["createx"]),
    ];

    let lowered = addr.trim().to_lowercase();
    if let Some((_, abis)) = BY_ADDRESS.iter().find(|(a, _)| *a == lowered) {
        return abis;
    }
    // Not in the table above because the address is a constant, not a literal.
    if lowered == crate::anchoring::ANCHORING_ADDRESS.to_lowercase() {
        return &["anchoring"];
    }
    // Every TIP-20 is the same interface at a different address.
    if is_tip20_token(&lowered) {
        return &["tip20", "tip20_roles_auth"];
    }
    &[]
}

/// Named contracts whose name contains `query`, as `(address, name)` pairs.
/// Sorted by match quality then alphabetically, so the same query always
/// produces the same list — a hash map's order is not one.
pub fn search_named(query: &str, limit: usize) -> Vec<(String, String)> {
    let query = query.trim().to_lowercase();
    if query.len() < 2 {
        return Vec::new();
    }
    let mut matches: Vec<(u8, String, String)> = precompile_labels()
        .iter()
        .chain(deployed_contracts())
        .filter_map(|(address, name)| {
            let lowered = name.to_lowercase();
            let rank = if lowered == query {
                0
            } else if lowered.starts_with(&query) {
                1
            } else if lowered.contains(&query) {
                2
            } else {
                return None;
            };
            Some((rank, name.clone(), address.clone()))
        })
        .collect();
    matches.sort();
    matches.truncate(limit);
    matches
        .into_iter()
        .map(|(_, name, address)| (address, name))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every labelled address must have an ABI to show, and every ABI it
    /// names must be one the registry actually loads.
    #[test]
    fn the_abi_table_agrees_with_the_label_table() {
        for label in precompile_labels()
            .keys()
            .chain(deployed_contracts().keys())
        {
            let abis = abis_for_address(label);
            assert!(
                !abis.is_empty(),
                "{label} is labelled but has no ABI to show"
            );
            for name in abis {
                assert!(
                    crate::decoder::REGISTRY.contract(name).is_some(),
                    "{label} names `{name}`, which the registry does not load"
                );
            }
        }
    }

    /// The lookup must not care how an address is spelled.
    #[test]
    fn abis_are_found_however_the_address_is_spelled() {
        let fee_manager = "0xfeEC000000000000000000000000000000000000";
        assert_eq!(abis_for_address(fee_manager), ["fee_manager", "fee_amm"]);
        assert_eq!(
            abis_for_address(&fee_manager.to_lowercase()),
            ["fee_manager", "fee_amm"]
        );
        assert_eq!(
            abis_for_address(&fee_manager.to_uppercase().replace("0X", "0x")),
            ["fee_manager", "fee_amm"]
        );
    }

    /// The precompile's own interface. The test above only asks that the ABI
    /// named exists, which passed while this address named the group holding
    /// the registry factory's event.
    #[test]
    fn the_anchoring_precompile_shows_its_own_interface() {
        let abis = abis_for_address(crate::anchoring::ANCHORING_ADDRESS);
        assert_eq!(abis, ["anchoring"]);
        let contract = crate::decoder::REGISTRY
            .contract("anchoring")
            .expect("anchoring registered");
        assert!(contract.functions().any(|f| f.name == "appendLeaf"));
        assert!(contract.events().any(|e| e.name == "LeafAppended"));
    }

    /// A TIP-20 is recognised by its prefix, not by an entry per token.
    #[test]
    fn every_tip20_gets_the_token_interface() {
        assert_eq!(
            abis_for_address("0x20c0000000000000000000000000000000000042"),
            ["tip20", "tip20_roles_auth"]
        );
        assert!(abis_for_address("0x1111111111111111111111111111111111111111").is_empty());
    }

    /// The repair matches these against stored addresses without normalizing.
    #[test]
    fn reserved_tokens_are_spelled_as_they_are_stored() {
        for address in RESERVED_TOKENS {
            assert_eq!(checksum_address(address), address);
        }
    }
}
