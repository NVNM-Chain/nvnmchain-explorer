//! WebSocket `newHeads` feed with a polling fallback.

use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::rpc::ChainRpc;

/// How long the watcher waits before retrying the socket, doubling up to the cap
/// while it stays unreachable and starting over once a subscription has run.
const RETRY_BASE: Duration = Duration::from_millis(500);
const RETRY_CAP: Duration = Duration::from_secs(300);

/// Poll the chain head once and forward it; returns false when the consumer
/// is gone (callers should stop).
async fn poll_once(rpc: &ChainRpc, tx: &mpsc::Sender<u64>) -> bool {
    match rpc.eth_block_number().await {
        Ok(head) => tx.send(head).await.is_ok(),
        Err(e) => {
            warn!("poll head failed: {e:#}");
            true
        }
    }
}

/// Forward every new chain head to `tx`.
///
/// Subscribes to `eth_subscribe("newHeads")` over WebSocket for instant block
/// notifications. When the socket is unreachable or drops, it falls back to
/// polling `eth_blockNumber` and keeps retrying the socket so the feed heals
/// on its own.
pub async fn head_watcher(
    rpc: ChainRpc,
    ws_url: String,
    index_ws: bool,
    poll: Duration,
    tx: mpsc::Sender<u64>,
    mut shutdown: watch::Receiver<bool>,
) {
    let poll = poll.max(Duration::from_millis(100));
    if !index_ws {
        // Pure polling mode.
        loop {
            if *shutdown.borrow() {
                return;
            }
            if !poll_once(&rpc, &tx).await {
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(poll) => {}
                r = shutdown.changed() => {
                    if r.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }

    let mut retry = RETRY_BASE;
    let mut consecutive_failures = 0u32;
    loop {
        if *shutdown.borrow() {
            return;
        }
        // Try the socket in the background while polling keeps the feed warm:
        let url = ws_url.clone();
        let tx2 = tx.clone();
        let ws_task = tokio::spawn(async move { subscribe_ws(&url, &tx2).await });
        let outcome = loop {
            if *shutdown.borrow() {
                ws_task.abort();
                return;
            }
            if !poll_once(&rpc, &tx).await {
                ws_task.abort();
                return;
            }
            if ws_task.is_finished() {
                break match ws_task.await {
                    Ok(res) => res,
                    Err(e) => Err(anyhow::anyhow!("websocket task failed: {e}")),
                };
            }
            tokio::select! {
                _ = tokio::time::sleep(poll) => {}
                r = shutdown.changed() => {
                    if r.is_err() || *shutdown.borrow() {
                        ws_task.abort();
                        return;
                    }
                }
            }
        };
        match outcome {
            Ok(()) => {
                consecutive_failures = 0;
                retry = RETRY_BASE;
                warn!("websocket subscription ended; reconnecting");
                // Keep polling during the brief reconnect pause.
                let until = tokio::time::Instant::now() + Duration::from_millis(250);
                while tokio::time::Instant::now() < until {
                    if *shutdown.borrow() {
                        return;
                    }
                    if !poll_once(&rpc, &tx).await {
                        return;
                    }
                    tokio::time::sleep(poll).await;
                }
            }
            Err(e) => {
                consecutive_failures += 1;
                if consecutive_failures <= 3 {
                    warn!(
                        "websocket head feed failed ({e:#}); continuing with polling \
                         (retry #{consecutive_failures}, then retries become silent)"
                    );
                } else {
                    debug!(
                        "websocket head feed still unreachable ({e:#}); next retry in {retry:?}"
                    );
                }
                // Poll continuously during the backoff so head detection never
                // stalls while the socket is unreachable.
                let until = tokio::time::Instant::now() + retry;
                while tokio::time::Instant::now() < until {
                    if *shutdown.borrow() {
                        return;
                    }
                    if !poll_once(&rpc, &tx).await {
                        return;
                    }
                    tokio::time::sleep(poll).await;
                }
                retry = (retry * 2).min(RETRY_CAP);
            }
        }
    }
}

async fn subscribe_ws(url: &str, tx: &mpsc::Sender<u64>) -> Result<()> {
    // A stalled handshake must not block the head feed forever: fall back to
    // polling if the socket doesn't come up quickly.
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(3),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .with_context(|| format!("connect timeout {url}"))?
    .with_context(|| format!("connect {url}"))?;
    ws.send(Message::Text(
        r#"{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}"#.into(),
    ))
    .await
    .context("send eth_subscribe")?;
    info!("subscribed to newHeads via {url}");

    while let Some(msg) = ws.next().await {
        let msg = msg.context("websocket read")?;
        match msg {
            Message::Text(text) => {
                if let Ok(value) = serde_json::from_str::<Value>(text.as_str()) {
                    // A refused subscription would otherwise leave the socket
                    // open and silent.
                    if let Some(error) = value.get("error") {
                        anyhow::bail!("eth_subscribe refused: {error}");
                    }
                    if let Some(number) = parse_head(&value) {
                        if tx.send(number).await.is_err() {
                            return Ok(()); // consumer gone; end subscription
                        }
                    }
                }
            }
            Message::Ping(payload) => {
                ws.send(Message::Pong(payload)).await.context("pong")?;
            }
            Message::Close(_) => return Ok(()),
            _ => {}
        }
    }
    Ok(())
}

/// Extract the new block number from an `eth_subscription` notification:
/// `{"method":"eth_subscription","params":{"subscription":"0x…","result":{"number":"0x…"}}}`
fn parse_head(value: &Value) -> Option<u64> {
    let number = value
        .get("params")?
        .get("result")?
        .get("number")?
        .as_str()?;
    u64::from_str_radix(number.strip_prefix("0x").unwrap_or(number), 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_eth_subscription_head() {
        let msg = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_subscription",
            "params": {
                "subscription": "0x1",
                "result": {"number": "0x8be58", "hash": "0xabc"}
            }
        });
        assert_eq!(parse_head(&msg), Some(0x8be58));
        assert_eq!(parse_head(&serde_json::json!({"x": 1})), None);
    }
}
