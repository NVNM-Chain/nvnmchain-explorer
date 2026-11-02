//! WebSocket `newHeads` feed with a polling fallback.

use std::future::Future;
use std::pin::pin;
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

/// Poll the head every `poll` until `stop` resolves, forwarding each to `tx`.
/// `None` when the consumer is gone or shutdown was requested.
async fn poll_until<T>(
    rpc: &ChainRpc,
    tx: &mpsc::Sender<u64>,
    poll: Duration,
    shutdown: &mut watch::Receiver<bool>,
    stop: impl Future<Output = T>,
) -> Option<T> {
    let mut stop = pin!(stop);
    loop {
        if *shutdown.borrow() {
            return None;
        }
        match rpc.eth_block_number().await {
            Ok(head) if tx.send(head).await.is_err() => return None,
            Ok(_) => {}
            Err(e) => warn!("poll head failed: {e:#}"),
        }
        tokio::select! {
            stopped = &mut stop => return Some(stopped),
            _ = tokio::time::sleep(poll) => {}
            r = shutdown.changed() => {
                if r.is_err() || *shutdown.borrow() {
                    return None;
                }
            }
        }
    }
}

/// Forward every new chain head to `tx`.
///
/// Subscribes to `eth_subscribe("newHeads")` over WebSocket and retries the
/// socket when it fails or drops; polling `eth_blockNumber` runs throughout,
/// so heads keep arriving meanwhile.
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
        poll_until(&rpc, &tx, poll, &mut shutdown, std::future::pending::<()>()).await;
        return;
    }

    let mut retry = RETRY_BASE;
    let mut consecutive_failures = 0u32;
    loop {
        let url = ws_url.clone();
        let tx2 = tx.clone();
        let mut ws_task = tokio::spawn(async move { subscribe_ws(&url, &tx2).await });
        let Some(joined) = poll_until(&rpc, &tx, poll, &mut shutdown, &mut ws_task).await else {
            ws_task.abort();
            return;
        };
        let outcome = joined.unwrap_or_else(|e| Err(anyhow::anyhow!("websocket task failed: {e}")));
        let pause = match outcome {
            Ok(()) => {
                consecutive_failures = 0;
                retry = RETRY_BASE;
                warn!("websocket subscription ended; reconnecting");
                Duration::from_millis(250)
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
                let pause = retry;
                retry = (retry * 2).min(RETRY_CAP);
                pause
            }
        };
        let pausing = tokio::time::sleep(pause);
        if poll_until(&rpc, &tx, poll, &mut shutdown, pausing)
            .await
            .is_none()
        {
            return;
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

    /// A node whose head is always 42.
    async fn node() -> ChainRpc {
        use axum::{routing::post, Json, Router};
        let handler = |Json(call): Json<Value>| async move {
            Json(serde_json::json!({"jsonrpc": "2.0", "id": call["id"], "result": "0x2a"}))
        };
        ChainRpc::test_node(Router::new().route("/", post(handler))).await
    }

    /// A closed port fails the subscription at once; polling stands in until shutdown.
    #[tokio::test]
    async fn polling_carries_the_feed_while_the_socket_is_down() {
        let (tx, mut rx) = mpsc::channel(8);
        let (stop, shutdown) = watch::channel(false);
        let watcher = tokio::spawn(head_watcher(
            node().await,
            "ws://127.0.0.1:1".into(),
            true,
            Duration::from_millis(100),
            tx,
            shutdown,
        ));
        assert_eq!(rx.recv().await, Some(42));
        assert_eq!(rx.recv().await, Some(42));
        stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), watcher)
            .await
            .expect("stopped on shutdown")
            .unwrap();
    }

    #[tokio::test]
    async fn pure_polling_stops_when_the_consumer_goes() {
        let (tx, mut rx) = mpsc::channel(8);
        let (_stop, shutdown) = watch::channel(false);
        let watcher = tokio::spawn(head_watcher(
            node().await,
            String::new(),
            false,
            Duration::from_millis(100),
            tx,
            shutdown,
        ));
        assert_eq!(rx.recv().await, Some(42));
        drop(rx);
        tokio::time::timeout(Duration::from_secs(2), watcher)
            .await
            .expect("stopped without a consumer")
            .unwrap();
    }

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
