//! TIP-20 transfer memos.
//!
//! A memo is a `bytes32` holding whatever the sender put there: usually a
//! short UTF-8 note, sometimes a binary payload, often nothing. Only the note
//! is worth showing, so anything that does not decode to readable text is
//! withheld rather than rendered as mojibake.

/// The MPP attribution memo's prefix — a machine payload, not a note.
const MPP_ATTRIBUTION_PREFIX: &str = "ef1ed712";

/// What a memo carries.
#[derive(Debug, PartialEq)]
pub enum Memo {
    /// A note worth showing.
    Note(String),
    /// The MPP attribution payload, which marks a payment but reads as noise.
    Attribution,
    /// Nothing displayable.
    Nothing,
}

/// Read a memo in the `0x…` form the decoder renders.
pub fn read(memo: &str) -> Memo {
    let Ok(bytes) = hex::decode(memo.strip_prefix("0x").unwrap_or(memo)) else {
        return Memo::Nothing;
    };
    if is_mpp_attribution(&bytes) {
        return Memo::Attribution;
    }
    decode_for_display(&bytes).map_or(Memo::Nothing, Memo::Note)
}

/// Whether a memo is an MPP attribution payload rather than a human note.
fn is_mpp_attribution(memo: &[u8]) -> bool {
    memo.len() == 32 && hex::encode(&memo[..4]) == MPP_ATTRIBUTION_PREFIX
}

/// The displayable text of a memo, or `None` when it carries none.
///
/// NUL padding is stripped from both ends (a short note in a fixed field is
/// padded on one side or the other) and whitespace collapsed. Control
/// characters are the signature of binary data, so a payload holding any is
/// refused.
fn decode_for_display(memo: &[u8]) -> Option<String> {
    let start = memo.iter().position(|&b| b != 0)?;
    let end = memo.iter().rposition(|&b| b != 0)? + 1;
    let text = std::str::from_utf8(&memo[start..end]).ok()?;

    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() || normalized.chars().any(is_control) {
        return None;
    }
    Some(normalized)
}

/// C0 and C1 control characters — never part of a note.
fn is_control(c: char) -> bool {
    let code = c as u32;
    code <= 0x1f || (0x7f..=0x9f).contains(&code)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A note padded into a 32-byte field, from either side.
    fn padded(text: &str, trailing: bool) -> Vec<u8> {
        let mut out = vec![0u8; 32];
        let bytes = text.as_bytes();
        let at = if trailing { 0 } else { 32 - bytes.len() };
        out[at..at + bytes.len()].copy_from_slice(bytes);
        out
    }

    #[test]
    fn a_padded_note_reads_back() {
        assert_eq!(
            decode_for_display(&padded("invoice 42", true)).as_deref(),
            Some("invoice 42")
        );
        assert_eq!(
            decode_for_display(&padded("invoice 42", false)).as_deref(),
            Some("invoice 42")
        );
    }

    #[test]
    fn whitespace_is_collapsed() {
        assert_eq!(
            decode_for_display(b"  rent   for\tmarch \n").as_deref(),
            Some("rent for march")
        );
    }

    /// The cases that must render nothing rather than gibberish.
    #[test]
    fn payloads_without_a_note_are_withheld() {
        assert_eq!(decode_for_display(&[0u8; 32]), None, "all-zero memo");
        assert_eq!(decode_for_display(&[]), None, "empty memo");
        assert_eq!(decode_for_display(&[0xff, 0xfe, 0xfd]), None, "not UTF-8");
        assert_eq!(
            decode_for_display(b"bell\x07here"),
            None,
            "control character"
        );
    }

    /// Attribution is machine data: readable or not, it is never a note.
    #[test]
    fn mpp_attribution_is_not_a_note() {
        let mut mpp = vec![0xef, 0x1e, 0xd7, 0x12];
        mpp.extend_from_slice(&[0x41; 28]);
        assert!(is_mpp_attribution(&mpp));
        // The same prefix at any other width is just a note that starts oddly.
        assert!(!is_mpp_attribution(&[0xef, 0x1e, 0xd7, 0x12]));
    }

    #[test]
    fn reads_the_hex_form() {
        let hex = format!("0x{}", hex::encode(padded("thanks", true)));
        assert_eq!(read(&hex), Memo::Note("thanks".into()));
        assert_eq!(read("not hex"), Memo::Nothing);
    }
}
