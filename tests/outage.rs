//! A web replica while Postgres is down: pages are 503s with `Retry-After`,
//! never empty pages or false 404s, and liveness never notices.
//!
//! No server needed: the replica points at a port nothing listens on.

use std::time::{Duration, Instant};

use nvnmchain_explorer::config::Settings;
use nvnmchain_explorer::db::{self, DbConfig, DbUrl, Role, Status};
use nvnmchain_explorer::web::{self, AppState};
use tokio::sync::watch;

async fn serve_down() -> String {
    let cfg = DbConfig::postgres(
        DbUrl("postgres://explorer:explorer@127.0.0.1:1/explorer".into()),
        Role::Web,
    );
    let (tx, status) = watch::channel(Status::starting(Role::Web));
    let db = db::open_with(&cfg, tx)
        .await
        .expect("a web replica opens lazily");
    let mut settings = Settings::from_env();
    settings.signature_lookup_url = None;
    settings.rpc_url = "http://127.0.0.1:1".into();
    let state = AppState {
        tera: web::build_tera(db.clone()).unwrap(),
        db,
        rpc: nvnmchain_explorer::rpc::ChainRpc::from_settings(&settings).unwrap(),
        cfg: settings,
        block_events: tokio::sync::broadcast::channel(16).0,
        stats: std::sync::Arc::new(std::sync::RwLock::new(serde_json::Value::Null)),
        shutdown: watch::channel(false).1,
    };
    let app = web::app(state).merge(web::health(status));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    format!("http://{addr}")
}

#[tokio::test]
async fn pages_are_503s_while_the_database_is_down() {
    let base = serve_down().await;
    for path in [
        "/",
        "/blocks",
        "/block/5",
        "/tx/0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "/tokens",
    ] {
        let started = Instant::now();
        let resp = reqwest::get(format!("{base}{path}")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 503, "{path}");
        assert_eq!(
            resp.headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok()),
            Some("30"),
            "{path}"
        );
        // One failed read is enough to know; the rest do not each wait.
        assert!(
            started.elapsed() < Duration::from_secs(8),
            "{path} took {:?}",
            started.elapsed()
        );
    }
    let alive = reqwest::get(format!("{base}/healthz")).await.unwrap();
    assert_eq!(
        alive.status().as_u16(),
        200,
        "liveness never asks the database"
    );
    let ready = reqwest::get(format!("{base}/readyz")).await.unwrap();
    assert_eq!(
        ready.status().as_u16(),
        503,
        "the schema gate has not passed"
    );
}
