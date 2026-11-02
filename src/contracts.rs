//! Contract identification and labels, mirroring `app/contracts.py`.

use alloy_primitives::Address;
use serde::Serialize;
use tempo_primitives::TempoAddressExt as _;

use crate::decoder::checksum_address;

/// An address known by name, and the built-in ABIs that describe it. The fee
/// manager has two: its own interface says nothing about the AMM it runs.
struct Known {
    address: &'static str,
    label: &'static str,
    /// `precompile`, or `contract` for an ordinary deployment at a fixed address.
    kind: &'static str,
    abis: &'static [&'static str],
}

const fn precompile(
    address: &'static str,
    label: &'static str,
    abis: &'static [&'static str],
) -> Known {
    Known {
        address,
        label,
        kind: "precompile",
        abis,
    }
}

const fn contract(
    address: &'static str,
    label: &'static str,
    abis: &'static [&'static str],
) -> Known {
    Known {
        address,
        label,
        kind: "contract",
        abis,
    }
}

/// Tempo's precompiles, then ordinary contracts at fixed addresses. Matched
/// ignoring case.
const KNOWN: &[Known] = &[
    precompile(
        "0xfeEC000000000000000000000000000000000000",
        "Fee Manager",
        &["fee_manager", "fee_amm"],
    ),
    precompile(
        "0x403c000000000000000000000000000000000000",
        "TIP-403 Registry",
        &["tip403_registry"],
    ),
    precompile(
        "0x20Fc000000000000000000000000000000000000",
        "TIP-20 Factory",
        &["tip20_factory"],
    ),
    precompile(
        "0x4D50500000000000000000000000000000000000",
        "TIP-20 Channel Reserve",
        &["tip20_channel_reserve"],
    ),
    precompile(
        "0xDEc0000000000000000000000000000000000000",
        "Stablecoin DEX",
        &["stablecoin_dex"],
    ),
    precompile(
        "0x4e4F4E4345000000000000000000000000000000",
        "Nonce Manager",
        &["nonce"],
    ),
    precompile(
        "0xCccCcCCC00000000000000000000000000000000",
        "Validator Config V1 (legacy)",
        &["validator_config"],
    ),
    precompile(
        "0xCccCcCCC00000000000000000000000000000001",
        "Validator Config",
        &["validator_config_v2"],
    ),
    precompile(
        "0xaAAAaaAA00000000000000000000000000000000",
        "Account Keychain",
        &["account_keychain"],
    ),
    precompile(
        "0xFDC0000000000000000000000000000000000000",
        "Address Registry",
        &["address_registry"],
    ),
    precompile(
        "0x5165300000000000000000000000000000000000",
        "Signature Verifier",
        &["signature_verifier"],
    ),
    precompile(
        "0xB10C000000000000000000000000000000000000",
        "Receive Policy Guard",
        &["receive_policy_guard"],
    ),
    precompile(
        "0x1060000000000000000000000000000000000000",
        "Storage Credits",
        &["storage_credits"],
    ),
    precompile(
        "0xC077e00000000000000000000000000000000000",
        "Current Committee",
        &["current_committee"],
    ),
    precompile(
        "0x5AF2000000000000000000000000000000000000",
        "Zone Factory",
        &["zone_factory"],
    ),
    contract(
        "0xcA11bde05977b3631167028862bE2a173976CA11",
        "Multicall3",
        &["multicall3"],
    ),
    contract(
        "0x000000000022D473030F116dDEE9F6B43aC78BA3",
        "Permit2",
        &["permit2"],
    ),
    contract(
        "0xba5Ed099633D3B313e4D5F7bdc1305d3c28ba5Ed",
        "CreateX",
        &["createx"],
    ),
    contract(
        "0x0000000000000000000000000000000000000a00",
        "Anchoring",
        &["anchoring"],
    ),
    // What 0x…0a00 delegates to since T12, at the same CREATE2 address on every chain.
    contract(
        "0xF873bbD8B4ed0580425592a37b218BC46A628C70",
        "Anchoring Implementation",
        &["anchoring"],
    ),
    contract(
        "0x5AD1000000000000000000000000000000000000",
        "Zone Portal Implementation",
        &["zone_portal"],
    ),
];

fn known(addr: &str) -> Option<&'static Known> {
    let addr = addr.trim();
    KNOWN.iter().find(|k| k.address.eq_ignore_ascii_case(addr))
}

/// The zone a portal address numbers, if it is one. The prefix alone, zone id
/// 0, is no portal: ids start at 1.
fn zone_portal_id(addr: &str) -> Option<u64> {
    addr.trim().parse::<Address>().ok()?.zone_portal_id()
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
const TIP20_TOKEN_PREFIX: &str = "0x20c0000000000000000";

pub fn is_tip20_token(addr: &str) -> bool {
    addr.trim().to_lowercase().starts_with(TIP20_TOKEN_PREFIX)
}

/// The name of a contract at a fixed address, or of the zone portal an address numbers.
pub fn get_contract_name(addr: &str) -> Option<String> {
    if let Some(k) = known(addr) {
        return Some(k.label.to_string());
    }
    zone_portal_id(addr).map(|id| format!("Zone Portal #{id}"))
}

#[derive(Debug, Serialize)]
pub struct AddressInfo {
    #[serde(rename = "type")]
    pub kind: String,
    pub label: Option<String>,
}

pub fn identify_address(addr: &str) -> AddressInfo {
    let label = get_contract_name(addr);
    let kind = match known(addr) {
        Some(k) => k.kind,
        None if label.is_some() => "contract", // a zone portal
        None if is_tip20_token(addr) => "token",
        None => "eoa",
    };
    AddressInfo {
        kind: kind.into(),
        label,
    }
}

/// Which built-in ABIs describe the contract at `addr`, by the names the
/// decoder's registry loads them under.
pub fn abis_for_address(addr: &str) -> &'static [&'static str] {
    if let Some(k) = known(addr) {
        return k.abis;
    }
    // Every TIP-20 is the same interface at a different address.
    if is_tip20_token(addr) {
        return &["tip20", "tip20_roles_auth"];
    }
    if zone_portal_id(addr).is_some() {
        return &["zone_portal"];
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
    let mut matches: Vec<(u8, &str, &str)> = KNOWN
        .iter()
        .filter_map(|k| {
            let lowered = k.label.to_lowercase();
            let rank = if lowered == query {
                0
            } else if lowered.starts_with(&query) {
                1
            } else if lowered.contains(&query) {
                2
            } else {
                return None;
            };
            Some((rank, k.label, k.address))
        })
        .collect();
    matches.sort();
    matches.truncate(limit);
    matches
        .into_iter()
        .map(|(_, name, address)| (checksum_address(address), name.to_string()))
        .collect()
}

/// Where a proxy keeps the address it delegates to.
#[derive(Debug, PartialEq)]
pub enum ProxyTarget {
    /// In its own bytecode.
    Code(String),
    /// In this storage slot.
    Slot(&'static str),
}

/// The ERC-1967 implementation slot.
const IMPLEMENTATION_SLOT: &str =
    "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc";
/// The word a Safe proxy matches calldata against to answer `masterCopy()` from slot 0.
const SAFE_MASTER_COPY: &str = "a619486e00000000000000000000000000000000000000000000000000000000";
/// What an EIP-1167 clone's bytecode holds around the implementation's address.
const EIP1167_HEAD: &str = "363d3d373d3d3d363d73";
const EIP1167_TAIL: &str = "5af43d82803e903d91602b57fd5bf3";

/// The kind of proxy `code` is, if any, and where it keeps its implementation.
pub fn proxy(code: &str) -> Option<(&'static str, ProxyTarget)> {
    let hex = code.trim_start_matches("0x");
    if let Some(address) = hex
        .strip_prefix(EIP1167_HEAD)
        .and_then(|rest| rest.strip_suffix(EIP1167_TAIL))
        .filter(|address| address.len() == 40)
    {
        let implementation = checksum_address(&format!("0x{address}"));
        Some(("EIP-1167", ProxyTarget::Code(implementation)))
    } else if hex.contains(&IMPLEMENTATION_SLOT[2..]) {
        Some(("ERC-1967", ProxyTarget::Slot(IMPLEMENTATION_SLOT)))
    } else if hex.contains(SAFE_MASTER_COPY) {
        Some(("Safe", ProxyTarget::Slot("0x0")))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_proxy_is_told_by_its_code() {
        let portal = checksum_address("0x5ad1000000000000000000000000000000000000");
        let clone = format!(
            "0x{EIP1167_HEAD}{}{EIP1167_TAIL}",
            portal[2..].to_lowercase()
        );
        assert_eq!(proxy(&clone), Some(("EIP-1167", ProxyTarget::Code(portal))));
        // 0x…0a00 since T12.
        let erc1967 = "0x363d3d373d3d363d7f360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc545af43d6000803e6038573d6000fd5b3d6000f3";
        assert_eq!(
            proxy(erc1967),
            Some(("ERC-1967", ProxyTarget::Slot(IMPLEMENTATION_SLOT)))
        );
        // What canary's Safe proxy factory deploys, from its `proxyRuntimeCode()`.
        let safe = "0x608060405273ffffffffffffffffffffffffffffffffffffffff600054167fa619486e0000000000000000000000000000000000000000000000000000000060003514156050578060005260206000f35b3660008037600080366000845af43d6000803e60008114156070573d6000fd5b3d6000f3fea2646970667358221220d1429297349653a4918076d650332de1a1068c5f3e07c5c82360c277770b955264736f6c63430007060033";
        assert_eq!(proxy(safe), Some(("Safe", ProxyTarget::Slot("0x0"))));
        assert_eq!(proxy("0x6080604052"), None);
    }

    /// Every ABI the table names must be one the registry actually loads.
    #[test]
    fn every_named_abi_is_registered() {
        for k in KNOWN {
            assert!(!k.abis.is_empty(), "{} has no ABI to show", k.label);
            for name in k.abis {
                assert!(
                    crate::decoder::REGISTRY.contract(name).is_some(),
                    "{} names `{name}`, which the registry does not load",
                    k.label
                );
            }
        }
    }

    /// The label follows the deployment the vendored contracts describe.
    #[test]
    fn the_anchoring_implementation_is_where_the_contracts_deploy_it() {
        let deployment: serde_json::Value =
            serde_json::from_str(include_str!("../contracts/layout/deployment.json")).unwrap();
        let implementation = deployment["implementation"].as_str().unwrap();
        assert_eq!(
            get_contract_name(implementation).as_deref(),
            Some("Anchoring Implementation")
        );
        assert_eq!(abis_for_address(implementation), ["anchoring"]);
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
