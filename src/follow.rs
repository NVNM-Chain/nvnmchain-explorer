//! The web role's live feed: a polling follower.
//!
//! Under `ROLE=web` nothing in this process writes blocks, so nothing can
//! broadcast them as they commit. One task polls the read pool instead, with
//! one statement per tick, and feeds the same channel the in-process indexer
//! feeds under `ROLE=all`: new blocks in order, the stats when they change,
//! and the schema gate when the version moves. Every 30 s it also reloads the
//! token labels, which is how a replica sees the indexer's inserts.

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::{json, Value};
use tokio::sync::{broadcast, watch};

use crate::db::{self, Db, TxColumns};
use crate::models::block_events;

/// The most blocks one tick broadcasts; the rest come on the next.
const MAX_BLOCKS: i64 = 256;
/// How often the token labels are reloaded.
const LABELS_EVERY: Duration = Duration::from_secs(30);

pub struct Follower {
    db: Db,
    events: broadcast::Sender<Value>,
    stats: Arc<RwLock<Value>>,
    /// The newest block broadcast; `None` until a tick has read the tip.
    last: Option<i64>,
    /// The stats row's `updated_at` when last read.
    stats_at: Option<i64>,
    schema: Option<i64>,
    labels_at: Option<Instant>,
}

impl Follower {
    pub fn new(db: Db, events: broadcast::Sender<Value>, stats: Arc<RwLock<Value>>) -> Self {
        Follower {
            db,
            events,
            stats,
            last: None,
            stats_at: None,
            schema: None,
            labels_at: None,
        }
    }

    /// One poll. An error is the caller's to log; the next tick picks up
    /// where this one would have, so nothing is skipped.
    pub async fn tick(&mut self) -> Result<()> {
        let (newest, stats_at, version) = db::follow_point(&self.db).await?;
        if version != self.schema {
            self.schema = version;
            crate::metrics::schema_version("db", version.unwrap_or(0));
            db::update_status(&self.db, |s| s.schema.db = version);
        }
        match (self.last, newest) {
            // The first tick that sees a block starts at the tip: history is
            // not replayed, but the tip's time is reported, for the staleness
            // alert. An empty table is no tip: backfill fills a fresh database
            // from the head down, so following from 0 would wait for it to
            // reach the bottom, then replay the chain. A failed read of the tip
            // (logged where it failed) leaves the start unset, so the next
            // tick reads it again; a row that does not decode is not.
            (None, Some(tip)) => {
                let (block, failed) =
                    db::track_failures(db::get_block_by_number(&self.db, tip)).await;
                if let Some(block) = block {
                    crate::metrics::latest_block_timestamp(block.timestamp);
                }
                if !failed {
                    self.last = Some(tip);
                }
            }
            (Some(last), Some(newest)) if newest > last => self.broadcast(last, newest).await,
            _ => {}
        }
        if stats_at != self.stats_at {
            if let Some(stats) = db::get_kv(&self.db, "stats")
                .await
                .and_then(|s| serde_json::from_str::<Value>(&s).ok())
            {
                self.stats_at = stats_at;
                *self.stats.write().unwrap_or_else(|e| e.into_inner()) = stats.clone();
                let _ = self.events.send(json!({ "type": "stats", "stats": stats }));
            }
        }
        if self.labels_at.is_none_or(|at| at.elapsed() >= LABELS_EVERY)
            && db::reload_labels(&self.db).await
        {
            self.labels_at = Some(Instant::now());
        }
        Ok(())
    }

    /// Blocks above `last`, oldest first, at most `MAX_BLOCKS`. `last` moves
    /// only as far as what was read, so a failed read is retried, never
    /// skipped. A block missing from a range that read is one the indexer
    /// never wrote (a bundle refused for its content): blocks above the tip
    /// commit in order through the one writer, so it will not turn up later,
    /// and waiting for it would stop the feed.
    async fn broadcast(&mut self, last: i64, newest: i64) {
        let (from, to) = (last + 1, newest.min(last + MAX_BLOCKS));
        // A failed read comes back empty, as on a page; `track_failures` tells
        // the two apart, and the failure is logged where it happened.
        let db = &self.db;
        let ((blocks, txs), failed) = db::track_failures(async {
            (
                db::get_blocks_in_range(db, from, to).await,
                db::get_transactions_in_range(db, from, to, TxColumns::List).await,
            )
        })
        .await;
        if failed || blocks.is_empty() {
            return;
        }
        for event in block_events(&blocks, &txs) {
            let _ = self.events.send(event);
        }
        // Newest first, as the range read returns them.
        crate::metrics::latest_block_timestamp(blocks[0].timestamp);
        self.last = Some(blocks[0].number);
    }
}

/// Poll every `poll` until shutdown. Errors are logged and retried on the
/// next tick; SSE streams stay open throughout.
pub async fn run(mut follower: Follower, poll: Duration, mut shutdown: watch::Receiver<bool>) {
    loop {
        if let Err(e) = follower.tick().await {
            tracing::warn!("follower: {e:#}");
        }
        tokio::select! {
            _ = tokio::time::sleep(poll) => {}
            _ = shutdown.wait_for(|&stop| stop) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{self, Db};
    use crate::models::{Block, BlockBundle};

    fn bundle(number: i64) -> BlockBundle {
        BlockBundle {
            block: Block {
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
                proposer: format!("0x{}", "33".repeat(20)),
                miner: format!("0x{}", "33".repeat(20)),
                tx_count: 0,
                created_at: 0,
            },
            txs: Vec::new(),
            transfers: Vec::new(),
            anchoring: Vec::new(),
            tokens: Vec::new(),
        }
    }

    async fn setup() -> (tempfile::TempDir, Db, Follower, broadcast::Receiver<Value>) {
        let dir = tempfile::tempdir().unwrap();
        let db = db::open(dir.path().join("follow.db").to_str().unwrap())
            .await
            .unwrap();
        let (tx, rx) = broadcast::channel(64);
        let cell = Arc::new(RwLock::new(Value::Null));
        (dir, db.clone(), Follower::new(db, tx, cell), rx)
    }

    fn blocks(rx: &mut broadcast::Receiver<Value>) -> Vec<i64> {
        let mut out = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if let Some(n) = event["block"]["number"].as_i64() {
                out.push(n);
            }
        }
        out
    }

    #[tokio::test]
    async fn new_blocks_are_broadcast_in_order_without_gaps() {
        let (_dir, db, mut follower, mut rx) = setup().await;
        for n in 1..=3 {
            db::save_block_bundle(&db, &bundle(n)).await.unwrap();
        }
        follower.tick().await.unwrap();
        assert!(blocks(&mut rx).is_empty(), "history is not replayed");

        for n in [4, 5, 6] {
            db::save_block_bundle(&db, &bundle(n)).await.unwrap();
        }
        follower.tick().await.unwrap();
        assert_eq!(blocks(&mut rx), [4, 5, 6]);
        follower.tick().await.unwrap();
        assert!(blocks(&mut rx).is_empty(), "nothing twice");
    }

    /// The first tick reports the tip's time, so a replica that starts while
    /// the chain is stalled still has the series the staleness alert reads.
    #[tokio::test]
    async fn the_first_tick_reports_the_tips_time() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let (_dir, db, mut follower, _rx) = setup().await;
        db::save_block_bundle(&db, &bundle(5)).await.unwrap();
        follower.tick().await.unwrap();
        let text = handle.render();
        assert!(
            text.contains("explorer_latest_block_timestamp_seconds 1700000005"),
            "{text}"
        );
    }

    /// Backfill fills a fresh database from the head down. A follower that
    /// started on the empty table follows from the first tip it sees; from 0
    /// it would wait for backfill to reach the bottom, then replay the chain.
    #[tokio::test]
    async fn a_follower_started_on_an_empty_table_follows_from_the_first_tip() {
        let (_dir, db, mut follower, mut rx) = setup().await;
        follower.tick().await.unwrap();
        for n in [1000, 1001] {
            db::save_block_bundle(&db, &bundle(n)).await.unwrap();
        }
        follower.tick().await.unwrap();
        assert!(blocks(&mut rx).is_empty(), "history is not replayed");
        db::save_block_bundle(&db, &bundle(1002)).await.unwrap();
        follower.tick().await.unwrap();
        assert_eq!(blocks(&mut rx), [1002]);
    }

    /// A block the indexer never wrote (a bundle refused for its content) is
    /// stepped over: blocks above the tip commit in order, so it never turns
    /// up, and waiting for it would stop the feed.
    #[tokio::test]
    async fn a_block_never_written_does_not_stop_the_feed() {
        let (_dir, db, mut follower, mut rx) = setup().await;
        db::save_block_bundle(&db, &bundle(1)).await.unwrap();
        follower.tick().await.unwrap();
        for n in [2, 4] {
            db::save_block_bundle(&db, &bundle(n)).await.unwrap();
        }
        follower.tick().await.unwrap();
        assert_eq!(blocks(&mut rx), [2, 4]);
        db::save_block_bundle(&db, &bundle(5)).await.unwrap();
        follower.tick().await.unwrap();
        assert_eq!(blocks(&mut rx), [5]);
    }

    #[tokio::test]
    async fn backfill_below_the_last_block_is_not_broadcast() {
        let (_dir, db, mut follower, mut rx) = setup().await;
        db::save_block_bundle(&db, &bundle(10)).await.unwrap();
        follower.tick().await.unwrap();
        db::save_block_bundle(&db, &bundle(4)).await.unwrap();
        follower.tick().await.unwrap();
        assert!(blocks(&mut rx).is_empty());
    }

    #[tokio::test]
    async fn a_database_error_then_recovery_leaves_no_gap() {
        let (_dir, db, mut follower, mut rx) = setup().await;
        db::save_block_bundle(&db, &bundle(1)).await.unwrap();
        follower.tick().await.unwrap();

        db::save_block_bundle(&db, &bundle(2)).await.unwrap();
        db::testing::execute(&db, "ALTER TABLE blocks RENAME TO blocks_away").unwrap();
        assert!(follower.tick().await.is_err());
        db::testing::execute(&db, "ALTER TABLE blocks_away RENAME TO blocks").unwrap();
        db::save_block_bundle(&db, &bundle(3)).await.unwrap();
        follower.tick().await.unwrap();
        assert_eq!(blocks(&mut rx), [2, 3]);
    }

    #[tokio::test]
    async fn changed_stats_are_rebroadcast() {
        let (_dir, db, mut follower, mut rx) = setup().await;
        follower.tick().await.unwrap();
        db::compute_and_store_stats(&db).await.unwrap();
        follower.stats_at = Some(-1);
        follower.tick().await.unwrap();
        let event = rx.try_recv().expect("a stats event");
        assert_eq!(event["type"], "stats");
        assert!(follower.stats.read().unwrap().get("total_blocks").is_some());
        follower.tick().await.unwrap();
        assert!(rx.try_recv().is_err(), "unchanged stats are not sent again");
    }
}
