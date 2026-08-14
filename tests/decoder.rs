use nvnmchain_explorer::db::{self, Db};
use nvnmchain_explorer::decoder::{
    checksum_address, decode_abi_args, decode_event, decode_function_call, extract_balance_changes,
    extract_calls, flatten_trace, TRANSFER_TOPIC,
};
use nvnmchain_explorer::models::{BlockBundle, Transaction, TransferEvent};
use nvnmchain_explorer::parse::{parse_block, parse_transaction};
use nvnmchain_explorer::tokens::{
    decode_string_result, format_token_amount, has_control_chars, sanitize_metadata_text,
};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

fn temp_db(name: &str) -> (tempfile::TempDir, Db) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(name);
    let conn = db::init_db(path.to_str().unwrap()).expect("init_db");
    (dir, Arc::new(Mutex::new(conn)))
}

/// A block carrying one transaction, in the shape the RPC returns it.
///
/// Every address here has alphabetic nibbles on purpose: a fixture of digits
/// alone cannot tell a checksummed address from a lowercase one, and that is
/// exactly what the storage round-trip has to preserve.
fn sample_raw_block() -> Value {
    json!({
        "number": "0x10",
        "hash": format!("0x{}", "ab".repeat(32)),
        "parentHash": format!("0x{}", "cd".repeat(32)),
        "timestamp": "0x64",
        "gasUsed": "0x5208",
        "gasLimit": "0x1c9c380",
        "miner": format!("0x{}", "2b".repeat(20)),
        "consensusContext": {"epoch": 1, "view": 2, "proposer": format!("0x{}", "11".repeat(32))},
        "transactions": [{
            "hash": format!("0x{}", "ff".repeat(32)),
            "blockNumber": "0x10",
            "transactionIndex": "0x0",
            "from": format!("0x{}", "3c".repeat(20)),
            "to": format!("0x{}", "4d".repeat(20)),
            "gas": "0x5208",
            "gasPrice": "0x4a817c800",
            "value": "0x1",
            "nonce": "0x3",
            "chainId": "0x2b45",
            "type": "0x76",
            "signature": {"type": "webAuthn", "r": "0x01", "s": "0x02"},
            "calls": [{"to": format!("0x{}", "55".repeat(20)), "value": "0x0", "input": "0xa9059cbb"}],
        }]
    })
}

/// The token and the two parties of the sample transfer.
fn sample_parties() -> (String, String, String) {
    (
        checksum_address(&format!("0x{}", "aa".repeat(20))),
        checksum_address(&format!("0x{}", "1a".repeat(20))),
        checksum_address(&format!("0x{}", "2b".repeat(20))),
    )
}

fn sample_transfer(block: &nvnmchain_explorer::models::Block, tx: &Transaction) -> TransferEvent {
    let (token, from, to) = sample_parties();
    TransferEvent {
        id: 0,
        tx_hash: tx.hash.clone(),
        block_number: block.number,
        log_index: 0,
        token_addr: token,
        from_addr: from,
        to_addr: to,
        amount: "100".into(),
        timestamp: block.timestamp,
        created_at: 0,
    }
}

/// A fixture address from one repeated byte: `addr("22")` is `0x2222…2222`.
fn addr(byte: &str) -> String {
    checksum_address(&format!("0x{}", byte.repeat(20)))
}

fn transfer_calldata(to: &str, amount: u128) -> String {
    format!(
        "0xa9059cbb000000000000000000000000{}{:064x}",
        to.trim_start_matches("0x"),
        amount
    )
}

#[test]
fn checksum_is_stable_and_40_hex() {
    let a = checksum_address("0x20c0000000000000000000000000000000000000");
    assert_eq!(a.len(), 42);
    assert_eq!(checksum_address(&a), a);
    let b = checksum_address("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
    assert_eq!(b.len(), 42);
    assert_eq!(checksum_address(&b), b);
}

#[test]
fn decode_transfer_call() {
    let data = transfer_calldata("0x0000000000000000000000000000000000000001", 100);
    let call = decode_function_call(&data).expect("decoded");
    assert_eq!(call.name.as_deref(), Some("transfer"));
    assert_eq!(call.signature.as_deref(), Some("transfer(address,uint256)"));
    assert_eq!(call.selector, "0xa9059cbb");
    assert_eq!(call.params.len(), 2);
    assert_eq!(call.params[0].name, "to");
    assert_eq!(call.params[1].name, "amount");
    assert_eq!(call.params[1].value, "100");
}

#[test]
fn decode_approve_and_unknown() {
    let approve = format!(
        "0x095ea7b3{}{}{:064x}",
        "00".repeat(12),
        "ab".repeat(20),
        42u64
    );
    let call = decode_function_call(&approve).expect("decoded");
    assert_eq!(call.name.as_deref(), Some("approve"));
    assert_eq!(call.params[1].value, "42");

    let unknown = decode_function_call("0xdeadbeef1234").expect("unknown decoded");
    assert!(unknown.name.is_none());
    assert_eq!(unknown.selector, "0xdeadbeef");
    assert_eq!(unknown.raw_args, "0x1234");

    assert!(decode_function_call("0x").is_none());
    assert!(decode_function_call("0x1234").is_none());
}

#[test]
fn decode_transfer_event() {
    let from = format!("0x{}", "11".repeat(20));
    let to = format!("0x{}", "22".repeat(20));
    let log = json!({
        "address": format!("0x{}", "cc".repeat(20)),
        "topics": [
            TRANSFER_TOPIC,
            format!("0x{}{}", "00".repeat(12), from.trim_start_matches("0x")),
            format!("0x{}{}", "00".repeat(12), to.trim_start_matches("0x")),
        ],
        "data": format!("0x{:064x}", 1234u64),
        "logIndex": "0x0",
    });
    let event = decode_event(&log).expect("decoded event");
    assert_eq!(event.name.as_deref(), Some("Transfer"));
    assert_eq!(event.params.len(), 3);
    assert_eq!(event.params[0].name, "from");
    assert!(event.params[0].indexed);
    assert_eq!(event.params[2].value, "1234");
    assert_eq!(event.params[1].value, checksum_address(&to));
}

#[test]
fn flatten_trace_nested() {
    let trace = json!({
        "type": "CALL",
        "from": format!("0x{}", "aa".repeat(20)),
        "to": format!("0x{}", "bb".repeat(20)),
        "input": "0x",
        "value": "0x0",
        "gas": "0x186a0",
        "gasUsed": "0xcccc",
        "calls": [
            {"type": "STATICCALL", "from": format!("0x{}", "bb".repeat(20)), "to": format!("0x{}", "cc".repeat(20)), "input": "0x", "value": "0x0", "gas": "0x5208", "gasUsed": "0x100"},
            {"type": "CALL", "from": format!("0x{}", "bb".repeat(20)), "to": format!("0x{}", "dd".repeat(20)), "input": "0x", "value": "0x01", "gas": "0x7530", "gasUsed": "0x2000"}
        ]
    });
    let flat = flatten_trace(&trace);
    assert_eq!(flat.len(), 3);
    assert_eq!(flat[0]["depth"], 0);
    assert_eq!(flat[1]["depth"], 1);
    assert_eq!(flat[2]["depth"], 1);
    assert_eq!(flat[1]["type"], "STATICCALL");
    assert_eq!(flat[0]["children"], json!([1, 2]));
    assert_eq!(flat[1]["gas"], "21000");
    assert_eq!(flat[2]["value"], "1");
}

/// ABI-encode a `string` return value the standard dynamic way:
/// word 0 = offset (0x20), word 1 = length, word 2 = UTF-8 payload padded.
fn abi_string(s: &str) -> String {
    let payload = s.as_bytes();
    let mut words: Vec<u8> = Vec::new();
    let mut offset_word = [0u8; 32];
    offset_word[24..32].copy_from_slice(&32u64.to_be_bytes()); // offset = 32
    words.extend_from_slice(&offset_word);
    let mut len_word = [0u8; 32];
    len_word[24..32].copy_from_slice(&(payload.len() as u64).to_be_bytes()); // length
    words.extend_from_slice(&len_word);
    words.extend_from_slice(payload);
    let padded = payload.len().div_ceil(32) * 32;
    words.resize(64 + padded, 0);
    format!("0x{}", hex::encode(&words))
}

#[test]
fn decode_string_result_dynamic_encoding() {
    // Regression: the real `name()`/`symbol()` responses for a TIP-20 token
    // (e.g. 0x20C0…e37D → "TestUSD") use the offset-indirection form. The
    // old decoder read the offset word (32) as the length and returned the
    // raw length word (31 NULs + 0x07) instead of the payload.
    assert_eq!(decode_string_result(&abi_string("TestUSD")), "TestUSD");
    assert_eq!(decode_string_result(&abi_string("TESTUSD")), "TESTUSD");
    assert_eq!(decode_string_result(&abi_string("")), "");
    // A string long enough to span multiple words.
    let long = "pathUSD-pathUSD-pathUSD-pathUSD-pathUSD-pathUSD";
    assert_eq!(decode_string_result(&abi_string(long)), long);

    // Non-standard in-place short string (length in word 0) still decodes.
    let mut in_place = vec![0u8; 64];
    in_place[31] = 7;
    in_place[32..39].copy_from_slice(b"TestUSD");
    assert_eq!(
        decode_string_result(&format!("0x{}", hex::encode(&in_place))),
        "TestUSD"
    );

    // Empty / malformed inputs degrade to an empty string.
    assert_eq!(decode_string_result(""), "");
    assert_eq!(decode_string_result("0x"), "");
    assert_eq!(decode_string_result("0x12"), "");
}

#[test]
fn sanitize_metadata_text_rejects_control_chars() {
    // The pre-fix decoder produced this exact garbage for a 7-char name:
    // 31 NUL bytes followed by the length byte 0x07.
    let garbage = "\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{7}";
    assert!(has_control_chars(garbage));
    assert_eq!(sanitize_metadata_text(garbage), "");

    // Legit metadata passes through (trailing NUL padding trimmed).
    assert!(!has_control_chars("TestUSD"));
    assert_eq!(sanitize_metadata_text("TestUSD"), "TestUSD");
    assert_eq!(sanitize_metadata_text("TestUSD\u{0}\u{0}"), "TestUSD");
    assert_eq!(sanitize_metadata_text(""), "");
}

#[test]
fn format_amounts() {
    assert_eq!(format_token_amount("0", 18), "0");
    assert_eq!(format_token_amount("1000000000000000000", 18), "1");
    assert_eq!(format_token_amount("1234567890123456789", 18), "1.234567");
    assert_eq!(format_token_amount("5000000000000000000", 18), "5");
}

#[test]
fn parse_block_and_transaction() {
    let raw_block = json!({
        "number": "0x10",
        "hash": format!("0x{}", "ab".repeat(32)),
        "parentHash": format!("0x{}", "cd".repeat(32)),
        "timestamp": "0x64",
        "gasUsed": "0x5208",
        "gasLimit": "0x1c9c380",
        "miner": format!("0x{}", "ee".repeat(20)),
        "transactions": [
            {
                "hash": format!("0x{}", "ff".repeat(32)),
                "blockNumber": "0x10",
                "blockHash": format!("0x{}", "ab".repeat(32)),
                "transactionIndex": "0x0",
                "from": format!("0x{}", "11".repeat(20)),
                "to": format!("0x{}", "22".repeat(20)),
                "gas": "0x5208",
                "gasPrice": "0x4a817c800",
                "value": "0xde0b6b3a7640000",
                "nonce": "0x3",
                "type": "0x0",
                "input": "0x",
                "chainId": "0x2b45"
            }
        ]
    });
    let block = parse_block(&raw_block);
    assert_eq!(block.number, 16);
    assert_eq!(block.tx_count, 1);
    assert_eq!(block.timestamp, 100);

    let tx = parse_transaction(&raw_block["transactions"][0], &block);
    assert_eq!(tx.hash, format!("0x{}", "ff".repeat(32)));
    assert_eq!(tx.block_number, 16);
    assert_eq!(tx.timestamp, 100);
}

#[test]
fn balance_changes_from_receipt() {
    let from = format!("0x{}", "11".repeat(20));
    let to = format!("0x{}", "22".repeat(20));
    let receipt = json!({
        "logs": [{
            "address": format!("0x{}", "cc".repeat(20)),
            "topics": [
                TRANSFER_TOPIC,
                format!("0x{}{}", "00".repeat(12), from.trim_start_matches("0x")),
                format!("0x{}{}", "00".repeat(12), to.trim_start_matches("0x")),
            ],
            "data": format!("0x{:064x}", 500u64),
        }]
    });
    let tx = Transaction {
        hash: "0x".into(),
        block_number: 0,
        position: 0,
        from_addr: from.clone(),
        to_addr: None,
        status: 1,
        gas_used: 0,
        base_fee: "0x0".into(),
        contract_address: None,
        fee_token: None,
        fee_amount: "0".into(),
        input: "0x".into(),
        raw: None,
        trace_data: None,
        receipt_data: None,
        timestamp: 0,
        created_at: 0,
    };
    let changes = extract_balance_changes(&receipt, &tx);
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0]["change"], "-500");
    assert_eq!(changes[1]["change"], "+500");
    assert_eq!(changes[0]["is_fee"], false);

    let calls = extract_calls(&tx, &[]);
    assert!(calls.is_empty());
}

#[test]
fn fresh_schema_has_all_columns() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("fresh.db");
    let conn = nvnmchain_explorer::db::init_db(path.to_str().unwrap()).expect("init_db");

    // The base is reset between schema iterations, so init_db creates the
    // final shape directly — no migration ladder.
    for (table, expected) in [
        (
            "blocks",
            &[
                "number",
                "hash",
                "parent_hash",
                "timestamp",
                "timestamp_ms",
                "gas_used",
                "gas_limit",
                "miner",
                "tx_count",
                "base_fee",
                "size",
                "extra_data",
                "epoch",
                "view",
                "proposer",
                "created_at",
            ][..],
        ),
        (
            "transactions",
            &[
                "hash",
                "block_number",
                "position",
                "from_addr",
                "to_addr",
                "status",
                "gas_used",
                "base_fee",
                "contract_address",
                "fee_token",
                "fee_amount",
                "input",
                "raw",
                "trace_data",
                "receipt_data",
                "timestamp",
                "created_at",
            ][..],
        ),
    ] {
        let cols: Vec<String> = {
            let mut stmt = conn
                .prepare(&format!("PRAGMA table_info({table})"))
                .expect("pragma");
            let rows = stmt.query_map([], |r| r.get::<_, String>(1)).unwrap();
            rows.filter_map(|c| c.ok()).collect()
        };
        for column in expected {
            assert!(
                cols.iter().any(|c| c == column),
                "column {table}.{column} should exist in the fresh schema"
            );
        }
    }

    for table in [
        "blocks",
        "transactions",
        "token_metadata",
        "contract_labels",
        "transfer_events",
        "kv",
        "token_balances",
    ] {
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                [table],
                |r| r.get(0),
            )
            .expect("table lookup");
        assert_eq!(count, 1, "table {table} should exist");
    }
}

#[test]
fn blob_hex_round_trip() {
    // Block with one transaction (so the tx row is exercised too).
    let (_dir, db) = temp_db("blob.db");
    let raw_block = sample_raw_block();
    let hash = raw_block["hash"].as_str().unwrap().to_string();
    let parent = raw_block["parentHash"].as_str().unwrap().to_string();
    let proposer = raw_block["consensusContext"]["proposer"]
        .as_str()
        .unwrap()
        .to_string();
    let miner = raw_block["miner"].as_str().unwrap().to_string();
    let tx_hash = raw_block["transactions"][0]["hash"]
        .as_str()
        .unwrap()
        .to_string();
    let block = parse_block(&raw_block);
    // The indexer fills the canonical RLP encoding separately; embed the real
    // bytes of a known tempo 0x76 transaction (block 664125).
    let rlp = include_str!("../fixtures/tx_664125.rlp").trim();
    let mut tx = parse_transaction(&raw_block["transactions"][0], &block);
    tx.raw = Some(rlp.to_string());
    let bundle = BlockBundle {
        block,
        txs: vec![tx],
        transfers: vec![],
        tokens: vec![],
    };
    db::save_block_bundle(&db, &bundle).expect("save bundle");

    // Block reads back with identical hex, and hash lookup is case-insensitive
    // (binary storage normalizes hex case — a bonus over TEXT comparisons).
    let got = db::get_block_by_number(&db, 16).expect("block by number");
    assert_eq!(got.hash, hash);
    assert_eq!(got.parent_hash, parent);
    assert_eq!(got.proposer, proposer);
    assert_eq!(got.miner, miner);
    assert!(db::get_block_by_hash(&db, &hash).is_some());
    let upper = format!("0x{}", hash[2..].to_uppercase());
    assert!(
        db::get_block_by_hash(&db, &upper).is_some(),
        "hash lookup should be case-insensitive with BLOB storage"
    );

    // Transaction reads back: raw is the canonical RLP encoding, and the
    // display fields (calls, signature type, gas) are decoded from it at
    // runtime with the tempo primitives.
    let got_tx = db::get_transaction(&db, &tx_hash).expect("tx");
    assert_eq!(got_tx.raw.as_deref(), Some(rlp));
    let parsed = nvnmchain_explorer::decoder::parse_raw_tx(got_tx.raw.as_deref().unwrap());
    assert_eq!(parsed.sig_type.as_deref(), Some("WebAuthn"));
    assert_eq!(parsed.nonce, Some(0));
    assert_eq!(parsed.gas_limit, Some(319946));
    assert_eq!(parsed.max_fee_per_gas, Some(1200000000));
    assert_eq!(parsed.nonce_key.as_deref(), Some("0"));
    assert_eq!(parsed.calls.len(), 1);
    let call_to = parsed.calls[0]["to"].as_str().unwrap().to_lowercase();
    assert_eq!(call_to, "0x20c0000000000000000000000000000000000000");
}

#[test]
fn duplicate_bundle_is_idempotent() {
    let (_dir, db) = temp_db("dedup.db");
    let raw_block = sample_raw_block();
    let block = parse_block(&raw_block);
    let tx = parse_transaction(&raw_block["transactions"][0], &block);
    let (token, from, to) = sample_parties();
    let transfer = sample_transfer(&block, &tx);

    // The same block written twice (what the indexer's concurrent fetch/retry
    // races can produce). Blocks and txs upsert; transfers must dedupe.
    let bundle = BlockBundle {
        block,
        txs: vec![tx],
        transfers: vec![transfer],
        tokens: vec![],
    };
    db::save_block_bundle(&db, &bundle).expect("first save");
    db::save_block_bundle(&db, &bundle).expect("duplicate save");

    let conn = db::lock(&db);
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM transfer_events", [], |r| r.get(0))
        .expect("transfer count");
    assert_eq!(
        count, 1,
        "duplicate bundle must not duplicate transfer rows"
    );

    let unique: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_index_list('transfer_events') WHERE \"unique\"=1",
            [],
            |r| r.get(0),
        )
        .expect("unique index lookup");
    assert_eq!(
        unique, 1,
        "unique constraint on (block_number, log_index) should exist"
    );

    // Balance deltas are applied once per transfer, not once per write.
    let recipient: String = conn
        .query_row(
            "SELECT balance FROM token_balances WHERE token_addr=?1 AND holder_addr=?2",
            rusqlite::params![token, to],
            |r| r.get(0),
        )
        .expect("recipient balance");
    assert_eq!(recipient, "100", "balance must not be double-applied");
    let sender: String = conn
        .query_row(
            "SELECT balance FROM token_balances WHERE token_addr=?1 AND holder_addr=?2",
            rusqlite::params![token, from],
            |r| r.get(0),
        )
        .expect("sender balance");
    assert_eq!(sender, "-100", "sender delta applied exactly once");
}

/// A transfer written by the indexer must read back through every listing that
/// shows it. The columns are BLOBs; readers that bound TEXT against them
/// matched nothing, so these tabs were permanently empty while the counters
/// beside them reported rows.
#[test]
fn indexed_transfer_reads_back_through_every_listing() {
    let (_dir, db) = temp_db("listings.db");
    let raw_block = sample_raw_block();
    let block = parse_block(&raw_block);
    let tx = parse_transaction(&raw_block["transactions"][0], &block);
    let (token, from, to) = sample_parties();
    let transfer = sample_transfer(&block, &tx);
    let meta = nvnmchain_explorer::tokens::TokenMeta {
        address: token.clone(),
        name: "Probe".into(),
        symbol: "PRB".into(),
        decimals: 2,
        currency: "USD".into(),
        total_supply: "1000".into(),
    };
    let bundle = BlockBundle {
        block,
        txs: vec![tx.clone()],
        transfers: vec![transfer],
        tokens: vec![meta],
    };
    db::save_block_bundle(&db, &bundle).expect("save bundle");

    let by_token = db::get_token_transfers(&db, &token, 1, 25);
    assert_eq!(by_token.len(), 1, "token transfers");
    assert_eq!(by_token[0]["from_addr"], json!(from));
    assert_eq!(by_token[0]["to_addr"], json!(to));
    assert_eq!(by_token[0]["amount"], json!("100"));
    // Joined against the transaction, not just the log — and canonicalized, not
    // handed back in whatever case the node happened to report.
    assert_eq!(
        by_token[0]["tx_from"],
        json!(checksum_address(&tx.from_addr))
    );
    assert_ne!(tx.from_addr, checksum_address(&tx.from_addr));

    assert_eq!(
        db::get_address_transfers(&db, &to, 1, 25).len(),
        1,
        "recipient"
    );
    assert_eq!(
        db::get_address_transfers(&db, &from, 1, 25).len(),
        1,
        "sender"
    );
    assert_eq!(db::get_token_transfer_count(&db, &token), 1);

    // Holdings need the batch metadata lookup, which bound TEXT as well.
    assert_eq!(
        db::get_tokens_metadata(&db, std::slice::from_ref(&token)).len(),
        1
    );
    let holdings = db::get_address_holdings(&db, &to);
    assert_eq!(holdings.len(), 1, "recipient holdings");
    assert_eq!(holdings[0]["symbol"], json!("PRB"));
    assert_eq!(holdings[0]["formatted"], json!("1"));

    // And the tokens list reports the holders the transfer created.
    assert_eq!(db::get_token_holder_count(&db, &token), 2);
    let stored = db::get_token_metadata(&db, &token).expect("token metadata");
    assert_eq!(stored.holder_count, 2, "holder_count on the tokens list");
}

#[test]
fn blob_storage_queries_match_text_params() {
    // Regression: `transfer_events`/`token_metadata` store addresses as raw
    // bytes (BLOBs), so any query passing a plain TEXT address silently
    // matched nothing — empty transfers tabs, empty holdings, and holder
    // counts stuck at 0. All read/update paths must bind `hex_blob`.
    use nvnmchain_explorer::db::{self, Db};
    use nvnmchain_explorer::tokens::TokenMeta;
    use std::sync::{Arc, Mutex};

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("blob-queries.db");
    let conn = db::init_db(path.to_str().unwrap()).expect("init_db");
    let db: Db = Arc::new(Mutex::new(conn));

    let raw_block = json!({
        "number": "0x10",
        "hash": format!("0x{}", "ab".repeat(32)),
        "parentHash": format!("0x{}", "cd".repeat(32)),
        "timestamp": "0x64",
        "gasUsed": "0x5208",
        "gasLimit": "0x1c9c380",
        "miner": format!("0x{}", "22".repeat(20)),
        "consensusContext": {"epoch": 1, "view": 2, "proposer": format!("0x{}", "11".repeat(32))},
        "transactions": [{
            "hash": format!("0x{}", "ff".repeat(32)),
            "blockNumber": "0x10",
            "transactionIndex": "0x0",
            "from": format!("0x{}", "33".repeat(20)),
            "to": format!("0x{}", "44".repeat(20)),
            "gas": "0x5208",
            "gasPrice": "0x4a817c800",
            "value": "0x1",
            "nonce": "0x3",
            "chainId": "0x2b45",
            "type": "0x76",
            "signature": {"type": "webAuthn", "r": "0x01", "s": "0x02"},
            "calls": [{"to": format!("0x{}", "55".repeat(20)), "value": "0x0", "input": "0xa9059cbb"}],
        }]
    });
    let block = parse_block(&raw_block);
    let tx = parse_transaction(&raw_block["transactions"][0], &block);
    let token = checksum_address(&format!("0x{}", "aa".repeat(20)));
    let from = checksum_address(&format!("0x{}", "11".repeat(20)));
    let to = checksum_address(&format!("0x{}", "22".repeat(20)));
    let transfer = TransferEvent {
        id: 0,
        tx_hash: tx.hash.clone(),
        block_number: block.number,
        log_index: 0,
        token_addr: token.clone(),
        from_addr: from.clone(),
        to_addr: to.clone(),
        amount: "1000000".into(),
        timestamp: block.timestamp,
        created_at: 0,
    };
    let meta = TokenMeta {
        address: token.clone(),
        name: "Path USD".into(),
        symbol: "pathUSD".into(),
        decimals: 6,
        currency: "USD".into(),
        total_supply: "1000000".into(),
    };
    let bundle = BlockBundle {
        block,
        txs: vec![tx],
        transfers: vec![transfer],
        tokens: vec![meta],
    };
    db::save_block_bundle(&db, &bundle).expect("save bundle");

    // Address transfers tab: query binds the address as raw bytes.
    let addr_transfers = db::get_address_transfers(&db, &to, 1, 25);
    assert_eq!(addr_transfers.len(), 1, "address transfers must be found");

    // Token transfers list + count agree.
    let token_transfers = db::get_token_transfers(&db, &token, 1, 25);
    assert_eq!(token_transfers.len(), 1, "token transfers must be found");
    assert_eq!(db::get_token_transfer_count(&db, &token), 1);

    // Holdings: balance row exists (TEXT) and metadata resolves (BLOB via
    // hex_blob IN-clause), so the joined holding is returned.
    let holdings = db::get_address_holdings(&db, &to);
    assert_eq!(holdings.len(), 1, "holdings must be resolved");
    assert_eq!(holdings[0]["symbol"], "pathUSD");
    assert_eq!(holdings[0]["formatted"], "1");

    // Holder count: live count over token_balances plus the cached
    // token_metadata.holder_count refreshed with a hex_blob key. Both the
    // sender (negative balance) and the recipient hold a row, so it's 2.
    assert_eq!(db::get_token_holder_count(&db, &token), 2);
    let meta_back = db::get_token_metadata(&db, &token).expect("token metadata");
    assert_eq!(
        meta_back.holder_count, 2,
        "token_metadata.holder_count must be refreshed via BLOB key"
    );

    // Startup backfill repairs stale holder counts.
    {
        let conn = db::lock(&db);
        conn.execute(
            "UPDATE token_metadata SET holder_count=0 WHERE address=?1",
            rusqlite::params![token],
        )
        .expect("stale holder count");
        db::sync_holder_counts(&conn).expect("sync holder counts");
    }
    let meta_back = db::get_token_metadata(&db, &token).expect("token metadata");
    assert_eq!(
        meta_back.holder_count, 2,
        "sync_holder_counts must backfill stale counts"
    );
}

#[test]
fn huge_page_numbers_do_not_panic() {
    use nvnmchain_explorer::db::{self, Db};
    use std::sync::{Arc, Mutex};

    // `page` is user input; u32 offset math overflowed (a panic under
    // debug assertions, a garbage offset in release).
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("pages.db");
    let db: Db = Arc::new(Mutex::new(
        db::init_db(path.to_str().unwrap()).expect("init_db"),
    ));

    assert!(
        db::get_address_transactions(&db, &format!("0x{}", "11".repeat(20)), u32::MAX, 25)
            .is_empty()
    );
    assert!(
        db::get_token_transfers(&db, &format!("0x{}", "aa".repeat(20)), u32::MAX, 25).is_empty()
    );
    assert!(db::get_all_tokens(&db, u32::MAX, 25).is_empty());
}

/// `authorizeKey(keyId, WebAuthn, KeyRestrictions{expiry, enforceLimits, one
/// TokenLimit, allowAnyCalls: false, no CallScopes})`, encoded by `cast`.
/// One 32-byte argument word per line; `->` marks an offset.
const AUTHORIZE_KEY_CALLDATA: &str = concat!(
    "0x980a6025",
    "0000000000000000000000001111111111111111111111111111111111111111", // keyId
    "0000000000000000000000000000000000000000000000000000000000000002", // signatureType: WebAuthn
    "0000000000000000000000000000000000000000000000000000000000000060", // -> restrictions
    "0000000000000000000000000000000000000000000000000000000070dbd880", // restrictions.expiry
    "0000000000000000000000000000000000000000000000000000000000000001", // restrictions.enforceLimits
    "00000000000000000000000000000000000000000000000000000000000000a0", // -> limits
    "0000000000000000000000000000000000000000000000000000000000000000", // restrictions.allowAnyCalls
    "0000000000000000000000000000000000000000000000000000000000000120", // -> allowedCalls
    "0000000000000000000000000000000000000000000000000000000000000001", // limits.len
    "0000000000000000000000002222222222222222222222222222222222222222", // limits[0].token
    "00000000000000000000000000000000000000000000000000000000000003e8", // limits[0].amount
    "0000000000000000000000000000000000000000000000000000000000015180", // limits[0].period
    "0000000000000000000000000000000000000000000000000000000000000000", // allowedCalls.len
);

#[test]
fn revoke_key_decodes() {
    let revoke = format!("0x5ae7ab32{}{}", "00".repeat(12), "33".repeat(20));
    let call = decode_function_call(&revoke).expect("revokeKey");
    assert_eq!(call.name.as_deref(), Some("revokeKey"));
    assert_eq!(call.params.len(), 1);
    assert_eq!(call.params[0].name, "keyId");
    assert_eq!(call.params[0].value, addr("33"));
}

#[test]
fn authorize_key_restrictions_decode() {
    // The old bare-`tuple` spelling decoded this parameter to nothing at all.
    let call = decode_function_call(AUTHORIZE_KEY_CALLDATA).expect("authorizeKey");
    let names: Vec<&str> = call.params.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(names, ["keyId", "signatureType", "restrictions"]);
    assert_eq!(call.params[0].value, addr("11"));
    assert_eq!(call.params[1].value, "2");
    // expiry, enforceLimits, one TokenLimit, allowAnyCalls, no CallScopes.
    assert_eq!(
        call.params[2].value,
        format!(
            "(1893456000, true, [({}, 1000, 86400)], false, [])",
            addr("22")
        )
    );
}

/// The same authorization carrying two `CallScope`s, the first with a
/// `SelectorRule` that names a recipient — encoded by `cast`.
const SCOPES_CALLDATA: &str = concat!(
    "0x980a6025",
    "0000000000000000000000001111111111111111111111111111111111111111", // keyId
    "0000000000000000000000000000000000000000000000000000000000000002", // signatureType: WebAuthn
    "0000000000000000000000000000000000000000000000000000000000000060", // -> restrictions
    "0000000000000000000000000000000000000000000000000000000070dbd880", // restrictions.expiry
    "0000000000000000000000000000000000000000000000000000000000000001", // restrictions.enforceLimits
    "00000000000000000000000000000000000000000000000000000000000000a0", // -> limits
    "0000000000000000000000000000000000000000000000000000000000000000", // restrictions.allowAnyCalls
    "00000000000000000000000000000000000000000000000000000000000000c0", // -> allowedCalls
    "0000000000000000000000000000000000000000000000000000000000000000", // limits.len
    "0000000000000000000000000000000000000000000000000000000000000002", // allowedCalls.len
    "0000000000000000000000000000000000000000000000000000000000000040", // -> allowedCalls[0]
    "0000000000000000000000000000000000000000000000000000000000000140", // -> allowedCalls[1]
    "0000000000000000000000002222222222222222222222222222222222222222", // allowedCalls[0].target
    "0000000000000000000000000000000000000000000000000000000000000040", // -> [0].selectorRules
    "0000000000000000000000000000000000000000000000000000000000000001", // [0].selectorRules.len
    "0000000000000000000000000000000000000000000000000000000000000020", // -> [0].selectorRules[0]
    "a9059cbb00000000000000000000000000000000000000000000000000000000", // [0].rule[0].selector
    "0000000000000000000000000000000000000000000000000000000000000040", // -> [0].rule[0].recipients
    "0000000000000000000000000000000000000000000000000000000000000001", // [0].rule[0].recipients.len
    "0000000000000000000000003333333333333333333333333333333333333333", // [0].rule[0].recipients[0]
    "0000000000000000000000004444444444444444444444444444444444444444", // allowedCalls[1].target
    "0000000000000000000000000000000000000000000000000000000000000040", // -> [1].selectorRules
    "0000000000000000000000000000000000000000000000000000000000000001", // [1].selectorRules.len
    "0000000000000000000000000000000000000000000000000000000000000020", // -> [1].selectorRules[0]
    "095ea7b300000000000000000000000000000000000000000000000000000000", // [1].rule[0].selector
    "0000000000000000000000000000000000000000000000000000000000000040", // -> [1].rule[0].recipients
    "0000000000000000000000000000000000000000000000000000000000000000", // [1].rule[0].recipients.len
);

/// The TIP-1053 witness overload: `KeyRestrictions` sits *before* another
/// argument — encoded by `cast`.
const WITNESS_CALLDATA: &str = concat!(
    "0xe3c154d2",
    "0000000000000000000000001111111111111111111111111111111111111111", // keyId
    "0000000000000000000000000000000000000000000000000000000000000001", // signatureType: P256
    "0000000000000000000000000000000000000000000000000000000000000080", // -> restrictions
    "00000000000000000000000000000000000000000000000000000000000000ab", // witness
    "0000000000000000000000000000000000000000000000000000000070dbd880", // restrictions.expiry
    "0000000000000000000000000000000000000000000000000000000000000000", // restrictions.enforceLimits
    "00000000000000000000000000000000000000000000000000000000000000a0", // -> limits
    "0000000000000000000000000000000000000000000000000000000000000001", // restrictions.allowAnyCalls
    "0000000000000000000000000000000000000000000000000000000000000120", // -> allowedCalls
    "0000000000000000000000000000000000000000000000000000000000000001", // limits.len
    "0000000000000000000000002222222222222222222222222222222222222222", // limits[0].token
    "00000000000000000000000000000000000000000000000000000000000003e8", // limits[0].amount
    "0000000000000000000000000000000000000000000000000000000000015180", // limits[0].period
    "0000000000000000000000000000000000000000000000000000000000000000", // allowedCalls.len
);

#[test]
fn tip20_functions_report_their_named_inputs() {
    // mint/burn resolve from the TIP-20 table, not the signature list.
    let mint = format!("0x40c10f19{}{}{:064x}", "00".repeat(12), "11".repeat(20), 5);
    let call = decode_function_call(&mint).expect("mint");
    assert_eq!(call.signature.as_deref(), Some("mint(address,uint256)"));
    assert_eq!(call.params[1].value, "5");

    let burn = format!("0x42966c68{:064x}", 7);
    let call = decode_function_call(&burn).expect("burn");
    assert_eq!(call.signature.as_deref(), Some("burn(uint256)"));
    assert_eq!(call.params[0].name, "amount");
    assert_eq!(call.params[0].value, "7");
}

#[test]
fn signature_list_selectors_are_the_well_known_ones() {
    // Pin the derived selectors to their well-known 4 bytes; a bad derivation
    // stops matching silently.
    for (selector, name) in [
        ("0x70a08231", "balanceOf"),
        ("0x18160ddd", "totalSupply"),
        ("0x06fdde03", "name"),
        ("0x95d89b41", "symbol"),
        ("0x313ce567", "decimals"),
        ("0xdd62ed3e", "allowance"),
        ("0x54063a55", "authorizeKey"),
        ("0x980a6025", "authorizeKey"),
        ("0xe3c154d2", "authorizeKey"),
        ("0x9a424307", "authorizeAdminKey"),
        ("0xcff31c46", "burnKeyAuthorizationWitness"),
        ("0x5ae7ab32", "revokeKey"),
        ("0xcbbb4480", "updateSpendingLimit"),
        ("0xf5456703", "setAllowedCalls"),
        ("0xf3941811", "removeAllowedCalls"),
    ] {
        let call = decode_function_call(selector).expect(name);
        assert_eq!(call.name.as_deref(), Some(name), "selector {selector}");
    }
}

/// A dynamic array of dynamic tuples: one offset word per element, nested
/// arrays in each tail.
#[test]
fn multiple_call_scopes_decode() {
    let call = decode_function_call(SCOPES_CALLDATA).expect("authorizeKey");
    assert_eq!(
        call.params[2].value,
        format!(
            "(1893456000, true, [], false, [({}, [(0xa9059cbb, [{}])]), ({}, [(0x095ea7b3, [])])])",
            addr("22"),
            addr("33"),
            addr("44"),
        )
    );
}

/// A dynamic tuple occupies one head word, not one per field; the witness
/// after it catches a miscount.
#[test]
fn witness_overload_decodes_the_argument_after_the_tuple() {
    let call = decode_function_call(WITNESS_CALLDATA).expect("authorizeKey with witness");
    assert_eq!(call.name.as_deref(), Some("authorizeKey"));
    let names: Vec<&str> = call.params.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(names, ["keyId", "signatureType", "restrictions", "witness"]);
    assert_eq!(
        call.params[2].value,
        format!(
            "(1893456000, false, [({}, 1000, 86400)], true, [])",
            addr("22")
        )
    );
    assert_eq!(call.params[3].value, format!("0x{:064x}", 0xab));
}

/// Replace one argument word (counted after the selector), so a variant
/// fixture cannot drift from the base it claims to match.
fn with_arg_word(calldata: &str, index: usize, word: &str) -> String {
    let (selector, args) = calldata.split_at(10);
    let mut words: Vec<&str> = args
        .as_bytes()
        .chunks(64)
        .map(|c| std::str::from_utf8(c).unwrap())
        .collect();
    assert_eq!(word.len(), 64, "a replacement word is 32 bytes of hex");
    words[index] = word;
    format!("{selector}{}", words.concat())
}

/// A hostile length claim fails fast against the data instead of spinning.
#[test]
fn array_length_is_bounded_by_the_calldata() {
    // Argument word 8 is the `TokenLimit[]` length; one limit is encoded.
    let hostile = with_arg_word(
        AUTHORIZE_KEY_CALLDATA,
        8,
        "000000000000000000000000000000000000000000000000ffffffffffffffff",
    );
    let call = decode_function_call(&hostile).expect("authorizeKey");
    assert_eq!(call.name.as_deref(), Some("authorizeKey"));
    // All-or-nothing: the refused list keeps its names and types, values empty.
    let names: Vec<&str> = call.params.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(names, ["keyId", "signatureType", "restrictions"]);
    assert!(call.params.iter().all(|p| p.value.is_empty()));
}

/// A fixed-size array of dynamic elements has no length prefix, just offsets.
#[test]
fn fixed_size_array_of_dynamic_elements_has_no_length_prefix() {
    let calldata = [
        format!("{:064x}", 0x20),   // argument -> array
        format!("{:064x}", 0x40),   // element 0 -> "hi"
        format!("{:064x}", 0x80),   // element 1 -> "yo"
        format!("{:064x}", 2),      //
        format!("{:0<64}", "6869"), // "hi"
        format!("{:064x}", 2),      //
        format!("{:0<64}", "796f"), // "yo"
    ]
    .concat();
    let bytes = hex::decode(calldata).expect("fixture hex");
    assert_eq!(
        decode_abi_args(&["string[2]"], &bytes),
        vec!["[hi, yo]".to_string()]
    );
}

/// Dynamic `bytes` and `string`: offsets to a length word and padded payload.
#[test]
fn bytes_and_string_arguments_decode() {
    let calldata = [
        format!("{:064x}", 0x40),       // -> bytes
        format!("{:064x}", 0x80),       // -> string
        format!("{:064x}", 4),          //
        format!("{:0<64}", "deadbeef"), //
        format!("{:064x}", 2),          //
        format!("{:0<64}", "6869"),     // "hi"
    ]
    .concat();
    let bytes = hex::decode(calldata).expect("fixture hex");
    assert_eq!(
        decode_abi_args(&["bytes", "string"], &bytes),
        vec!["0xdeadbeef".to_string(), "hi".to_string()]
    );
}

/// A length the payload cannot back decodes to nothing rather than whatever
/// follows.
#[test]
fn a_length_longer_than_its_payload_decodes_nothing() {
    let calldata = [
        format!("{:064x}", 0x20),
        format!("{:064x}", 100),        // claims 100 bytes
        format!("{:0<64}", "deadbeef"), // 32 are present
    ]
    .concat();
    let bytes = hex::decode(calldata).expect("fixture hex");
    assert_eq!(decode_abi_args(&["bytes"], &bytes), Vec::<String>::new());
}

/// A static array sits inline at its element width — no offset, no length.
#[test]
fn fixed_size_array_of_static_elements_is_inline() {
    let calldata = [
        format!("{:064x}", 1),
        format!("{:064x}", 2),
        format!("{:064x}", 3),
    ]
    .concat();
    let bytes = hex::decode(calldata).expect("fixture hex");
    assert_eq!(
        decode_abi_args(&["uint256[3]"], &bytes),
        vec!["[1, 2, 3]".to_string()]
    );
}

/// Truncated calldata is refused outright rather than zero-padded.
#[test]
fn a_truncated_static_array_decodes_nothing() {
    let calldata = [format!("{:064x}", 1), format!("{:064x}", 2)].concat();
    let bytes = hex::decode(calldata).expect("fixture hex");
    assert_eq!(
        decode_abi_args(&["uint256[3]"], &bytes),
        Vec::<String>::new()
    );
}
