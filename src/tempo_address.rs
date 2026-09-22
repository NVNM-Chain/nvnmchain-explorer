//! TIP-1022 virtual addresses.
//!
//! ```text
//! [4-byte masterId] [10-byte 0xFDFDFDFDFDFDFDFDFDFD] [6-byte userTag]
//! ```
//!
//! TIP-20 transfers to one are credited to the master wallet its `masterId` is
//! registered to, so the literal address never holds a balance. Recognising
//! the format is what lets a deposit address read as a forwarding alias rather
//! than an unknown EOA.

use crate::decoder::checksum_address;

/// The ten bytes that mark an address as virtual (TIP-1022).
pub const VIRTUAL_MAGIC: [u8; 10] = [0xfd; 10];

/// The two operator-chosen fields of a virtual address.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct VirtualAddress {
    /// Registry lookup key, derived from `(masterAddress, salt)`.
    pub master_id: String,
    /// Opaque per-user identifier, derived offchain by the operator.
    pub user_tag: String,
}

fn address_bytes(address: &str) -> Option<[u8; 20]> {
    let hex = address.trim();
    let hex = hex.strip_prefix("0x").unwrap_or(hex);
    let bytes = hex::decode(hex).ok()?;
    bytes.try_into().ok()
}

/// Whether `address` has the TIP-1022 virtual-address shape.
pub fn is_virtual(address: &str) -> bool {
    parse_virtual(address).is_some()
}

/// The `masterId` and `userTag` of a virtual address, or `None` for any other.
/// A check of the format only — whether the `masterId` is registered is the
/// chain's business.
pub fn parse_virtual(address: &str) -> Option<VirtualAddress> {
    let bytes = address_bytes(address)?;
    if bytes[4..14] != VIRTUAL_MAGIC {
        return None;
    }
    Some(VirtualAddress {
        master_id: format!("0x{}", hex::encode(&bytes[..4])),
        user_tag: format!("0x{}", hex::encode(&bytes[14..])),
    })
}

/// Build a virtual address from its two fields, as an operator does offchain.
pub fn virtual_address(master_id: &[u8; 4], user_tag: &[u8; 6]) -> String {
    let mut bytes = [0u8; 20];
    bytes[..4].copy_from_slice(master_id);
    bytes[4..14].copy_from_slice(&VIRTUAL_MAGIC);
    bytes[14..].copy_from_slice(user_tag);
    checksum_address(&format!("0x{}", hex::encode(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_virtual_address_reports_its_parts() {
        let address = virtual_address(&[0xde, 0xad, 0xbe, 0xef], &[1, 2, 3, 4, 5, 6]);
        let parsed = parse_virtual(&address).expect("virtual");
        assert_eq!(parsed.master_id, "0xdeadbeef");
        assert_eq!(parsed.user_tag, "0x010203040506");
        assert!(is_virtual(&address));
    }

    /// The magic sits in the middle, so an address that merely starts or ends
    /// with those bytes is an ordinary address.
    #[test]
    fn only_the_middle_bytes_decide() {
        assert!(!is_virtual(&format!(
            "0x{}",
            "fd".repeat(4) + &"11".repeat(16)
        )));
        assert!(!is_virtual(&format!("0x{}", "11".repeat(20))));
        assert!(!is_virtual("0xnot-an-address"));
        // One byte short of an address, magic in place — still not virtual.
        assert!(!is_virtual(&format!(
            "0x{}{}{}",
            "de".repeat(4),
            "fd".repeat(10),
            "01".repeat(5)
        )));
    }

    /// Case must not decide it: the same address in either spelling parses.
    #[test]
    fn parsing_ignores_checksum_case() {
        let address = virtual_address(&[0xAB; 4], &[0xCD; 6]);
        assert_eq!(
            parse_virtual(&address.to_lowercase()),
            parse_virtual(&address)
        );
    }
}
