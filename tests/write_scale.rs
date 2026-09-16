//! What a commit costs, and what batching buys. Run explicitly; prints
//! timings rather than asserting them.

use std::time::Instant;

use nvnmchain_explorer::db::{self, save_block_bundle, save_block_bundles, Db};
use nvnmchain_explorer::models::{Block, BlockBundle, Transaction};
use std::sync::{Arc, Mutex};

const BLOCKS: u64 = 2_000;
const TXS_PER_BLOCK: usize = 5;

fn bundle(number: u64) -> BlockBundle {
    let block = Block {
        number: number as i64,
        hash: format!("0x{number:064x}"),
        parent_hash: format!("0x{:064x}", number.saturating_sub(1)),
        timestamp: number as i64,
        timestamp_ms: number as i64 * 1000,
        gas_used: 0,
        gas_limit: 0,
        miner: String::new(),
        tx_count: TXS_PER_BLOCK as i64,
        base_fee: "0".into(),
        size: 0,
        extra_data: String::new(),
        epoch: 0,
        view: 0,
        proposer: String::new(),
        created_at: 0,
    };
    let txs = (0..TXS_PER_BLOCK)
        .map(|i| Transaction {
            hash: format!("0x{:064x}", number * 100 + i as u64),
            block_number: number as i64,
            position: i as i64,
            from_addr: format!("0x{:040x}", number % 50),
            to_addr: Some(format!("0x{:040x}", number % 70)),
            status: 1,
            gas_used: 21_000,
            base_fee: "0x0".into(),
            contract_address: None,
            fee_token: None,
            fee_amount: "0".into(),
            input: String::new(),
            raw: None,
            trace_data: None,
            receipt_data: None,
            timestamp: number as i64,
            created_at: 0,
        })
        .collect();
    BlockBundle {
        block,
        txs,
        transfers: vec![],
        tokens: vec![],
    }
}

fn fresh() -> (tempfile::TempDir, Db) {
    let dir = tempfile::tempdir().unwrap();
    let conn = db::init_db(dir.path().join("explorer.db").to_str().unwrap()).unwrap();
    (dir, Arc::new(Mutex::new(conn)))
}

#[test]
#[ignore = "prints timings; run explicitly with --ignored --nocapture"]
fn write_scale() {
    println!("\n{BLOCKS} blocks x {TXS_PER_BLOCK} txs");

    // What the code did before: WAL's default `synchronous = FULL`, one commit
    // per block -- an fsync each.
    let (_dir, db) = fresh();
    db::lock(&db)
        .pragma_update(None, "synchronous", "FULL")
        .unwrap();
    let t = Instant::now();
    for n in 0..BLOCKS {
        save_block_bundle(&db, &bundle(n)).unwrap();
    }
    let full = t.elapsed();
    println!(
        "FULL,  per block   {:>12.1?}   ({:.0} blocks/s)",
        full,
        BLOCKS as f64 / full.as_secs_f64()
    );

    let (_dir, db) = fresh();
    let t = Instant::now();
    for n in 0..BLOCKS {
        save_block_bundle(&db, &bundle(n)).unwrap();
    }
    let per_block = t.elapsed();
    println!(
        "NORMAL, per block  {:>12.1?}   ({:.0} blocks/s, {:.1}x)",
        per_block,
        BLOCKS as f64 / per_block.as_secs_f64(),
        full.as_secs_f64() / per_block.as_secs_f64()
    );

    for batch in [8_u64, 64, 256] {
        let (_dir, db) = fresh();
        let t = Instant::now();
        let mut n = 0;
        while n < BLOCKS {
            let last = (n + batch - 1).min(BLOCKS - 1);
            let rows: Vec<_> = (n..=last).map(bundle).collect();
            save_block_bundles(&db, &rows).unwrap();
            n = last + 1;
        }
        let elapsed = t.elapsed();
        println!(
            "NORMAL, batch={batch:<4} {:>12.1?}   ({:.0} blocks/s, {:.1}x)",
            elapsed,
            BLOCKS as f64 / elapsed.as_secs_f64(),
            full.as_secs_f64() / elapsed.as_secs_f64()
        );
    }
}
