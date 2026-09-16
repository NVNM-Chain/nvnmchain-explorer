//! Normalize raw RPC JSON into storage models (port of the `parse_*`
//! functions in `app/rpc.py`).

use serde_json::Value;

use crate::db::now_ts;
use crate::models::{Block, Transaction};
use crate::rpc::{decimal_amount, parse_int_any, str_field};

pub fn parse_block(raw: &Value) -> Block {
    let number = raw.get("number").map(parse_int_any).unwrap_or(0);
    let tx_count = raw
        .get("transactions")
        .and_then(Value::as_array)
        .map(|a| a.len() as i64)
        .unwrap_or(0);
    let base_fee = raw
        .get("baseFeePerGas")
        .map(decimal_amount)
        .unwrap_or_else(|| "0".into());
    let consensus = raw.get("consensusContext");
    let proposer = consensus
        .and_then(|c| c.get("proposer"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    Block {
        number,
        hash: str_field(raw, "hash"),
        parent_hash: str_field(raw, "parentHash"),
        timestamp: raw.get("timestamp").map(parse_int_any).unwrap_or(0),
        timestamp_ms: raw
            .get("timestampMillis")
            .map(parse_int_any)
            .unwrap_or_else(|| raw.get("timestamp").map(parse_int_any).unwrap_or(0) * 1000),
        gas_used: raw.get("gasUsed").map(parse_int_any).unwrap_or(0),
        gas_limit: raw.get("gasLimit").map(parse_int_any).unwrap_or(0),
        base_fee,
        size: raw.get("size").map(parse_int_any).unwrap_or(0),
        extra_data: str_field(raw, "extraData"),
        epoch: consensus
            .and_then(|c| c.get("epoch"))
            .map(parse_int_any)
            .unwrap_or(0),
        view: consensus
            .and_then(|c| c.get("view"))
            .map(parse_int_any)
            .unwrap_or(0),
        proposer,
        miner: str_field(raw, "miner"),
        tx_count,
        created_at: now_ts(),
    }
}

/// Destination for a transaction. Tempo-style chains put every transfer in a
/// `calls` array instead of a top-level `to` (a true contract creation has
/// neither).
pub fn tx_to_addr(tx: &Value) -> Option<String> {
    tx.get("to")
        .and_then(Value::as_str)
        .map(String::from)
        .or_else(|| {
            tx.get("calls")
                .and_then(Value::as_array)
                .and_then(|c| c.first())
                .and_then(|c| c.get("to"))
                .and_then(Value::as_str)
                .map(String::from)
        })
}

pub fn parse_transaction(tx: &Value, block: &Block) -> Transaction {
    let to_addr = tx_to_addr(tx);
    let fee_amount = tx
        .get("feeAmount")
        .map(decimal_amount)
        .unwrap_or_else(|| "0".into());
    let input = {
        let input = str_field(tx, "input");
        let data = str_field(tx, "data");
        if input.is_empty() && !data.is_empty() {
            data
        } else {
            input
        }
    };
    Transaction {
        hash: str_field(tx, "hash"),
        block_number: tx.get("blockNumber").map(parse_int_any).unwrap_or(0),
        position: tx.get("transactionIndex").map(parse_int_any).unwrap_or(0),
        from_addr: str_field(tx, "from"),
        to_addr,
        status: 1,
        gas_used: 0,
        base_fee: "0x0".into(),
        contract_address: None,
        fee_token: None,
        fee_amount,
        input,
        // The canonical RLP bytes are re-encoded by the indexer from the
        // block's own transaction object and parsed at render time with the
        // tempo primitives — nothing else is stored per column.
        raw: None,
        trace_data: None,
        receipt_data: None,
        timestamp: block.timestamp,
        created_at: now_ts(),
    }
}
