use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, watch};
use tracing::{info, warn};

use nvnmchain_explorer::config::Settings;
use nvnmchain_explorer::db::{self, Db};
use nvnmchain_explorer::indexer::{self, IndexerConfig};
use nvnmchain_explorer::rpc::ChainRpc;
use nvnmchain_explorer::web;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nvnmchain_explorer=info".into()),
        )
        .init();

    let cfg = Settings::from_env();
    info!(
        "starting nvnmchain Explorer (rpc={}, db={})",
        cfg.rpc_url, cfg.db_path
    );

    let conn = db::init_db(&cfg.db_path).context("initialize database")?;
    let db: Db = Arc::new(Mutex::new(conn));
    let rpc = ChainRpc::from_settings(&cfg)?;
    let tera = web::build_tera(db.clone())?;

    // Background indexer: instant heads via WebSocket (poll fallback),
    // concurrent block fetching, serialized SQLite writes.
    let indexer_rpc = ChainRpc::from_settings(&cfg)?;
    let indexer_db = db.clone();
    let indexer_cfg = IndexerConfig::from_settings(&cfg);
    let ws_url = cfg.ws_url.clone();
    // Sized for ~an hour of sub-second blocks; combined with the writer's
    // in-order emission and the SSE lag-replay, live viewers never see gaps.
    let (block_tx, _) = broadcast::channel::<serde_json::Value>(8192);
    let indexer_block_tx = block_tx.clone();
    // Ctrl+C (or SIGTERM via the graceful-shutdown future) flips this watch;
    // every indexer loop checks it so the process stops promptly instead of
    // continuing to fetch/index for minutes.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let indexer_shutdown = shutdown_rx.clone();
    // Home-page stats live here; the stats task refreshes them, the web
    // handlers read them. Seeded from kv so a restart paints real numbers
    // before the first recompute.
    let home_stats = Arc::new(std::sync::RwLock::new(
        db::get_kv(&db, "stats")
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(serde_json::Value::Null),
    ));
    let indexer_stats = home_stats.clone();
    let indexer_task = tokio::spawn(async move {
        info!("indexer websocket feed: {ws_url}");
        indexer::run_forever(
            indexer_rpc,
            indexer_db,
            indexer_cfg,
            indexer_block_tx,
            indexer_stats,
            indexer_shutdown,
        )
        .await
    });

    let state = web::AppState {
        db,
        rpc,
        cfg: cfg.clone(),
        tera,
        block_events: block_tx,
        stats: home_stats,
        shutdown: shutdown_rx.clone(),
    };
    let app = web::app(state);

    let addr = format!("{}:{}", cfg.host, cfg.port);
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    info!("listening on http://{addr}");
    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        let sig = shutdown_signal().await;
        info!("received {sig}, shutting down (second one force quits)");
        let _ = shutdown_tx.send(true);
        // The installed handler replaced the default disposition for good, so
        // signals no longer kill the process; catch a second one ourselves.
        tokio::spawn(async {
            let sig = shutdown_signal().await;
            warn!("received {sig} again, exiting now");
            std::process::exit(130);
        });
    });

    tokio::select! {
        r = server => r.context("server error")?,
        // Backstop: graceful shutdown waits on in-flight connections, and a
        // streaming handler could hold it open indefinitely.
        _ = shutdown_deadline(shutdown_rx) => {
            warn!("connections still open {GRACE_SECS}s after shutdown; exiting anyway");
        }
    }

    // The indexer loops exit on the shutdown signal; give them a bounded
    // window to flush in-flight work, then exit regardless.
    info!("server stopped; waiting for indexer shutdown");
    let _ = tokio::time::timeout(Duration::from_secs(10), indexer_task).await;
    info!("shutdown complete");
    Ok(())
}

/// How long graceful shutdown may wait on in-flight connections.
const GRACE_SECS: u64 = 5;

/// Resolve `GRACE_SECS` after shutdown is requested; never if it isn't.
async fn shutdown_deadline(mut rx: watch::Receiver<bool>) {
    if rx.wait_for(|&stop| stop).await.is_ok() {
        tokio::time::sleep(Duration::from_secs(GRACE_SECS)).await;
    } else {
        // Sender gone without a signal: the server arm settles the race.
        std::future::pending::<()>().await
    }
}

/// Wait for SIGINT or SIGTERM, returning which arrived.
async fn shutdown_signal() -> &'static str {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => "SIGINT",
        _ = terminate => "SIGTERM",
    }
}
