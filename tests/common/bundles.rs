//! The bundles a baseline fixture's indexer wrote, rebuilt from its rows,
//! for the replay and differential tests.

use std::collections::HashMap;

use nvnmchain_explorer::decoder::checksum_address;
use nvnmchain_explorer::models::{AnchoringEvent, Block, BlockBundle, Transaction, TransferEvent};
use nvnmchain_explorer::tokens::TokenMeta;
use rusqlite::Connection;

fn hex(b: Vec<u8>) -> String {
    format!("0x{}", hex::encode(b))
}

fn addr(b: Vec<u8>) -> String {
    checksum_address(&hex(b))
}

/// The bundles the indexer wrote, rebuilt from the fixture's rows. Each
/// token's metadata rides with the bundle of its first transfer, or of the
/// first transaction that pays a fee in it.
pub fn bundles(conn: &Connection) -> Vec<BlockBundle> {
    let mut blocks: Vec<BlockBundle> = conn
        .prepare(
            "SELECT number, hash, parent_hash, timestamp, timestamp_ms, gas_used, gas_limit, base_fee, \
             size, extra_data, epoch, view, proposer, miner, tx_count, created_at FROM blocks ORDER BY number",
        )
        .unwrap()
        .query_map([], |r| {
            Ok(Block {
                number: r.get(0)?,
                hash: hex(r.get(1)?),
                parent_hash: hex(r.get(2)?),
                timestamp: r.get(3)?,
                timestamp_ms: r.get(4)?,
                gas_used: r.get(5)?,
                gas_limit: r.get(6)?,
                base_fee: r.get(7)?,
                size: r.get(8)?,
                extra_data: r.get(9)?,
                epoch: r.get(10)?,
                view: r.get(11)?,
                proposer: hex(r.get(12)?),
                miner: hex(r.get(13)?),
                tx_count: r.get(14)?,
                created_at: r.get(15)?,
            })
        })
        .unwrap()
        .map(|b| BlockBundle {
            block: b.unwrap(),
            txs: Vec::new(),
            transfers: Vec::new(),
            anchoring: Vec::new(),
            tokens: Vec::new(),
        })
        .collect();
    let at: HashMap<i64, usize> = blocks
        .iter()
        .enumerate()
        .map(|(i, b)| (b.block.number, i))
        .collect();

    let txs = conn
        .prepare(
            "SELECT hash, block_number, position, from_addr, to_addr, status, gas_used, base_fee, \
             contract_address, fee_token, fee_amount, input, raw, trace_data, receipt_data, timestamp, \
             created_at FROM transactions ORDER BY block_number, position",
        )
        .unwrap()
        .query_map([], |r| {
            Ok(Transaction {
                hash: hex(r.get(0)?),
                block_number: r.get(1)?,
                position: r.get(2)?,
                from_addr: addr(r.get(3)?),
                to_addr: r.get::<_, Option<Vec<u8>>>(4)?.map(addr),
                status: r.get(5)?,
                gas_used: r.get(6)?,
                base_fee: r.get(7)?,
                contract_address: r.get::<_, Option<Vec<u8>>>(8)?.map(addr),
                fee_token: r.get::<_, Option<Vec<u8>>>(9)?.map(addr),
                fee_amount: r.get(10)?,
                input: r.get(11)?,
                raw: r.get::<_, Option<Vec<u8>>>(12)?.map(hex),
                trace_data: r.get(13)?,
                receipt_data: r.get(14)?,
                timestamp: r.get(15)?,
                created_at: r.get(16)?,
            })
        })
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    for tx in txs {
        blocks[at[&tx.block_number]].txs.push(tx);
    }

    let transfers = conn
        .prepare(
            "SELECT id, tx_hash, block_number, log_index, token_addr, from_addr, to_addr, amount, \
             timestamp, created_at FROM transfer_events ORDER BY block_number, log_index",
        )
        .unwrap()
        .query_map([], |r| {
            Ok(TransferEvent {
                id: r.get(0)?,
                tx_hash: hex(r.get(1)?),
                block_number: r.get(2)?,
                log_index: r.get(3)?,
                token_addr: addr(r.get(4)?),
                from_addr: addr(r.get(5)?),
                to_addr: addr(r.get(6)?),
                amount: r.get(7)?,
                timestamp: r.get(8)?,
                created_at: r.get(9)?,
            })
        })
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    for t in transfers {
        blocks[at[&t.block_number]].transfers.push(t);
    }

    let anchoring = conn
        .prepare(
            "SELECT tx_hash, block_number, log_index, timestamp, event, registry_id, record_id, caller \
             FROM anchoring_events ORDER BY block_number, log_index",
        )
        .unwrap()
        .query_map([], |r| {
            Ok(AnchoringEvent {
                tx_hash: hex(r.get(0)?),
                block_number: r.get(1)?,
                log_index: r.get(2)?,
                timestamp: r.get(3)?,
                event: r.get(4)?,
                registry_id: r.get(5)?,
                record_id: r.get(6)?,
                caller: addr(r.get(7)?),
            })
        })
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    for a in anchoring {
        if let Some(&i) = at.get(&a.block_number) {
            blocks[i].anchoring.push(a);
        }
    }

    let tokens = conn
        .prepare(
            "SELECT address, name, symbol, decimals, currency, total_supply FROM token_metadata",
        )
        .unwrap()
        .query_map([], |r| {
            Ok(TokenMeta {
                address: addr(r.get(0)?),
                name: r.get(1)?,
                symbol: r.get(2)?,
                decimals: r.get(3)?,
                currency: r.get(4)?,
                total_supply: r.get(5)?,
            })
        })
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    for meta in tokens {
        let first = blocks
            .iter()
            .position(|b| {
                b.transfers.iter().any(|t| t.token_addr == meta.address)
                    || b.txs
                        .iter()
                        .any(|t| t.fee_token.as_deref() == Some(&meta.address))
            })
            .unwrap_or(0);
        blocks[first].tokens.push(meta);
    }
    blocks
}

/// The genesis balances and their cursor, as the genesis job stored them.
/// A genesis balance: (token, holder) and its amount.
pub type Genesis = ((String, String), String);

pub fn genesis(conn: &Connection) -> (Vec<Genesis>, i64) {
    let rows = conn
        .prepare("SELECT token_addr, holder_addr, balance FROM genesis_balances")
        .unwrap()
        .query_map([], |r| Ok(((r.get(0)?, r.get(1)?), r.get(2)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let cursor = conn
        .query_row(
            "SELECT value FROM kv WHERE key = 'genesis_balances_cursor'",
            [],
            |r| r.get::<_, String>(0),
        )
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    (rows, cursor)
}
