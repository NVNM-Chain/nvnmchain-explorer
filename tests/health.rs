//! `/readyz`: what an orchestrator's readiness probe sees. It reads the status
//! the database layer publishes, never a pool connection.
//!
//! Liveness, `/healthz`, never touches the database, so an outage does not
//! restart every pod: `tests/outage.rs` and `tests/shutdown.rs` check it.

use nvnmchain_explorer::db::{self, DbConfig, Role, Status};
use nvnmchain_explorer::web;
use serde_json::{json, Value};
use tokio::sync::watch;

async fn serve(status: watch::Receiver<Status>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, web::health(status)).await;
    });
    format!("http://{addr}")
}

async fn get(base: &str, path: &str) -> (u16, String) {
    let resp = reqwest::get(format!("{base}{path}")).await.expect("GET");
    (resp.status().as_u16(), resp.text().await.expect("body"))
}

#[tokio::test]
async fn not_ready_before_the_database_is_open() {
    let (_tx, status) = watch::channel(Status::starting(Role::All));
    let base = serve(status).await;
    let (code, body) = get(&base, "/readyz").await;
    assert_eq!(code, 503, "{body}");
    let body: Value = serde_json::from_str(&body).expect("json");
    assert_eq!(body["role"], json!("all"));
    assert_eq!(body["schema"]["db"], Value::Null);
}

#[tokio::test]
async fn ready_once_open_with_the_schema_versions() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = DbConfig::sqlite(dir.path().join("health.db").to_str().unwrap());
    let (tx, status) = watch::channel(Status::starting(Role::All));
    let _db = db::open_with(&cfg, tx).await.expect("open");
    let base = serve(status).await;

    let (code, body) = get(&base, "/readyz").await;
    assert_eq!(code, 200, "{body}");
    let body: Value = serde_json::from_str(&body).expect("json");
    let binary = db::migrations::binary_version();
    assert_eq!(body["role"], json!("all"));
    assert_eq!(body["schema"], json!({"db": binary, "binary": binary}));
}
