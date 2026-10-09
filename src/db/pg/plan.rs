//! The batch planner behind the set-based writer: what a batch of bundles
//! writes, deduplicated as the per-row writer would leave it, and the net
//! balance and holder changes its new transfers make.
//!
//! Pure, so the property test can hold it to SQLite's per-row writer.

use std::collections::{HashMap, HashSet};

use num_bigint::{BigInt, Sign};

use super::shared::bigint;
use crate::models::{AnchoringEvent, Block, BlockBundle, Transaction, TransferEvent};
use crate::tokens::TokenMeta;

/// One batch, as the per-row writer would leave it: blocks, transactions and
/// token metadata keep the last copy of a repeated key, as an upsert does, and
/// a set-based upsert must not touch a row twice. Transfers keep the first, as
/// `INSERT OR IGNORE` does, so the balances count each once. Anchoring events
/// keep every copy: their insert's `DO NOTHING` keeps the first.
pub(crate) struct BatchPlan {
    pub(super) blocks: Vec<Block>,
    pub(super) txs: Vec<Transaction>,
    pub(super) transfers: Vec<TransferEvent>,
    pub(super) anchoring: Vec<AnchoringEvent>,
    pub(super) tokens: Vec<TokenMeta>,
}

/// The balance rows a batch's new transfers leave, and the holders each token
/// gains or loses.
#[derive(Default)]
pub(crate) struct BalanceChanges {
    pub(crate) upserts: Vec<(String, String, String)>,
    pub(crate) deletes: Vec<(String, String)>,
    pub(crate) holders: Vec<(String, i64)>,
}

/// Keep one item per key, in first-seen order: the first copy, or the last.
fn dedup<T: Clone, K: std::hash::Hash + Eq>(
    items: impl Iterator<Item = T>,
    key: impl Fn(&T) -> K,
    last: bool,
) -> Vec<T> {
    let mut index: HashMap<K, usize> = HashMap::new();
    let mut out: Vec<T> = Vec::new();
    for item in items {
        match index.get(&key(&item)) {
            Some(&i) if last => out[i] = item,
            Some(_) => {}
            None => {
                index.insert(key(&item), out.len());
                out.push(item);
            }
        }
    }
    out
}

impl BatchPlan {
    pub(crate) fn new(bundles: &[BlockBundle]) -> Self {
        BatchPlan {
            blocks: dedup(bundles.iter().map(|b| b.block.clone()), |b| b.number, true),
            txs: dedup(
                bundles.iter().flat_map(|b| b.txs.iter().cloned()),
                |t| t.hash.to_lowercase(),
                true,
            ),
            transfers: dedup(
                bundles.iter().flat_map(|b| b.transfers.iter().cloned()),
                |t| (t.block_number, t.log_index),
                false,
            ),
            anchoring: bundles
                .iter()
                .flat_map(|b| b.anchoring.iter().cloned())
                .collect(),
            tokens: dedup(
                bundles.iter().flat_map(|b| b.tokens.iter().cloned()),
                |m| m.address.to_lowercase(),
                true,
            ),
        }
    }

    /// Each block with its transaction count in this batch, for the counter
    /// probe: a block's transactions already stored are not counted again.
    pub(crate) fn tx_counts(&self) -> (Vec<i64>, Vec<i64>) {
        let mut per_block: HashMap<i64, i64> = self.blocks.iter().map(|b| (b.number, 0)).collect();
        for tx in &self.txs {
            *per_block.entry(tx.block_number).or_default() += 1;
        }
        let mut pairs: Vec<(i64, i64)> = per_block.into_iter().collect();
        pairs.sort();
        pairs.into_iter().unzip()
    }

    /// Each (token, holder)'s net delta over the new transfers, the ones
    /// `fresh` names, keyed by the addresses as written, as `adjust_balance`
    /// keys them.
    pub(crate) fn net(&self, fresh: &HashSet<(i64, i64)>) -> HashMap<(String, String), BigInt> {
        let mut net: HashMap<(String, String), BigInt> = HashMap::new();
        for t in &self.transfers {
            if !fresh.contains(&(t.block_number, t.log_index)) {
                continue;
            }
            let amount = bigint(&t.amount);
            *net.entry((t.token_addr.clone(), t.from_addr.clone()))
                .or_default() -= &amount;
            *net.entry((t.token_addr.clone(), t.to_addr.clone()))
                .or_default() += &amount;
        }
        net
    }
}

/// Apply net balance deltas to the stored balances `old`, with
/// `adjust_balance`'s rules: a zero balance has no row, a negative one is kept
/// but is not a holding. The holder deltas telescope to what the per-row
/// writer's steps add up to.
pub(crate) fn apply_deltas(
    net: HashMap<(String, String), BigInt>,
    old: &HashMap<(String, String), String>,
) -> BalanceChanges {
    let mut changes = BalanceChanges::default();
    let mut holders: HashMap<String, i64> = HashMap::new();
    let mut keys: Vec<_> = net.into_iter().collect();
    keys.sort_by(|a, b| a.0.cmp(&b.0));
    for ((token, holder), delta) in keys {
        if delta.sign() == Sign::NoSign {
            continue;
        }
        let current = old.get(&(token.clone(), holder.clone()));
        let was = current.is_some_and(|b| !b.starts_with('-'));
        let new = bigint(current.map_or("0", String::as_str)) + delta;
        let is = new.sign() == Sign::Plus;
        if new.sign() == Sign::NoSign {
            if current.is_some() {
                changes.deletes.push((token.clone(), holder));
            }
        } else {
            changes
                .upserts
                .push((token.clone(), holder, new.to_string()));
        }
        *holders.entry(token).or_default() += i64::from(is) - i64::from(was);
    }
    let mut holders: Vec<(String, i64)> = holders.into_iter().filter(|(_, by)| *by != 0).collect();
    holders.sort();
    changes.holders = holders;
    changes
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    use crate::db::sqlite;
    use crate::models::{Block, BlockBundle, Transaction, TransferEvent};
    use crate::tokens::TokenMeta;

    /// xorshift64*: deterministic, so a failure reproduces.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    const PARTIES: usize = 5;
    const TOKENS: usize = 3;

    fn addr(prefix: u8, i: usize) -> String {
        crate::decoder::checksum_address(&format!("0x{:02x}{}{:02x}", prefix, "00".repeat(18), i))
    }

    fn bundle(number: i64, rng: &mut Rng) -> BlockBundle {
        let block = Block {
            number,
            hash: format!("0x{number:064x}"),
            parent_hash: format!("0x{:064x}", number - 1),
            timestamp: 1_700_000_000 + number,
            timestamp_ms: 0,
            gas_used: 0,
            gas_limit: 0,
            base_fee: "0".into(),
            size: 0,
            extra_data: String::new(),
            epoch: 0,
            view: 0,
            proposer: addr(0x33, 0),
            miner: addr(0x33, 0),
            tx_count: 1,
            created_at: 0,
        };
        let tx = Transaction {
            hash: format!("0x{:064x}", number + (1 << 40)),
            block_number: number,
            position: 0,
            from_addr: addr(0x11, 0),
            to_addr: None,
            status: 1,
            gas_used: 0,
            base_fee: "0".into(),
            contract_address: None,
            fee_token: None,
            fee_amount: "0".into(),
            input: "0x".into(),
            raw: None,
            trace_data: None,
            receipt_data: None,
            timestamp: block.timestamp,
            created_at: 0,
        };
        let transfers = (0..rng.below(5))
            .map(|log_index| TransferEvent {
                id: 0,
                tx_hash: tx.hash.clone(),
                block_number: number,
                log_index: log_index as i64,
                token_addr: addr(0x20, rng.below(TOKENS as u64) as usize),
                // Party 0 is the zero-ish minter; self-transfers happen too.
                from_addr: addr(0x44, rng.below(PARTIES as u64) as usize),
                to_addr: addr(0x44, rng.below(PARTIES as u64) as usize),
                amount: (rng.below(4) * 50).to_string(),
                timestamp: block.timestamp,
                created_at: 0,
            })
            .collect();
        // Metadata shows up now and then, sometimes after the token's transfers.
        let tokens = (0..TOKENS)
            .filter(|_| rng.below(4) == 0)
            .map(|i| TokenMeta {
                address: addr(0x20, i),
                name: format!("T{i}"),
                symbol: format!("T{i}"),
                decimals: 0,
                currency: String::new(),
                total_supply: "0".into(),
            })
            .collect();
        BlockBundle {
            block,
            txs: vec![tx],
            transfers,
            anchoring: Vec::new(),
            tokens,
        }
    }

    /// The Postgres pipeline over in-memory tables: insert transfers (first
    /// wins), seed new tokens' holder counts from the balances, apply the
    /// plan's changes.
    #[derive(Default)]
    struct Memory {
        transfers: HashSet<(i64, i64)>,
        balances: HashMap<(String, String), String>,
        holders: HashMap<String, i64>,
    }

    impl Memory {
        fn write(&mut self, bundles: &[BlockBundle]) {
            let plan = BatchPlan::new(bundles);
            let fresh: HashSet<(i64, i64)> = plan
                .transfers
                .iter()
                .map(|t| (t.block_number, t.log_index))
                .filter(|k| self.transfers.insert(*k))
                .collect();
            for meta in &plan.tokens {
                if !self.holders.contains_key(&meta.address) {
                    let n = self
                        .balances
                        .iter()
                        .filter(|((t, _), b)| *t == meta.address && !b.starts_with('-'))
                        .count();
                    self.holders.insert(meta.address.clone(), n as i64);
                }
            }
            let net = plan.net(&fresh);
            let old: HashMap<(String, String), String> = net
                .keys()
                .filter_map(|k| self.balances.get(k).map(|b| (k.clone(), b.clone())))
                .collect();
            let changes = apply_deltas(net, &old);
            for (token, holder, balance) in changes.upserts {
                self.balances.insert((token, holder), balance);
            }
            for (token, holder) in changes.deletes {
                self.balances.remove(&(token, holder));
            }
            for (token, by) in changes.holders {
                if let Some(n) = self.holders.get_mut(&token) {
                    *n += by;
                }
            }
        }
    }

    #[test]
    fn a_batch_plan_matches_the_per_row_writer() {
        for seed in 1..=40u64 {
            let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
            let dir = tempfile::tempdir().unwrap();
            let db = sqlite::open(dir.path().join("plan.db").to_str().unwrap()).unwrap();
            let mut memory = Memory::default();
            let mut number = 1;
            for _ in 0..8 {
                let mut batch: Vec<BlockBundle> = (0..1 + rng.below(6))
                    .map(|_| {
                        number += 1;
                        bundle(number, &mut rng)
                    })
                    .collect();
                // A repeated block, as a retried or overlapping fetch writes.
                if rng.below(3) == 0 {
                    let again = batch[rng.below(batch.len() as u64) as usize].clone();
                    batch.push(again);
                }
                sqlite::save_block_bundles(&db, &batch).unwrap();
                memory.write(&batch);
            }

            let conn = sqlite::lock(&db);
            let stored: HashMap<(String, String), String> = conn
                .prepare("SELECT token_addr, holder_addr, balance FROM token_balances")
                .unwrap()
                .query_map([], |r| Ok(((r.get(0)?, r.get(1)?), r.get(2)?)))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            assert_eq!(memory.balances, stored, "seed {seed}: balances");
            let counts: HashMap<String, i64> = conn
                .prepare("SELECT address, holder_count FROM token_metadata")
                .unwrap()
                .query_map([], |r| {
                    Ok((
                        crate::db::pg::shared::blob_addr(&r.get::<_, Vec<u8>>(0)?),
                        r.get(1)?,
                    ))
                })
                .unwrap()
                .map(Result::unwrap)
                .collect();
            assert_eq!(memory.holders, counts, "seed {seed}: holder counts");
        }
    }

    #[test]
    fn rows_are_deduplicated_as_the_per_row_writer_leaves_them() {
        let mut rng = Rng(7);
        let mut first = bundle(10, &mut rng);
        first.block.extra_data = "first".into();
        first.tokens = vec![TokenMeta {
            address: addr(0x20, 0),
            name: "old".into(),
            symbol: "OLD".into(),
            decimals: 0,
            currency: String::new(),
            total_supply: "0".into(),
        }];
        let mut second = first.clone();
        second.block.extra_data = "second".into();
        second.tokens[0].symbol = "NEW".into();
        if let Some(t) = second.transfers.first_mut() {
            t.amount = "999".into();
        }
        let plan = BatchPlan::new(&[first.clone(), second]);
        assert_eq!(plan.blocks.len(), 1);
        assert_eq!(plan.blocks[0].extra_data, "second", "blocks: last wins");
        assert_eq!(plan.tokens[0].symbol, "NEW", "tokens: last wins");
        if let Some(t) = first.transfers.first() {
            assert_eq!(plan.transfers[0].amount, t.amount, "transfers: first wins");
        }
    }
}
