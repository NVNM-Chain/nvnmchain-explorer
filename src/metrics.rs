//! Prometheus metrics, served at `/metrics` under `ROLE=web` and
//! `ROLE=indexer` for in-cluster scraping. `ROLE=all` serves none: it runs on
//! hosts with no Ingress (Fly, Render, systemd), where it would be public.
//!
//! The recording functions are no-ops until `install` runs, so `ROLE=all` and
//! the tests pay nothing for them.

use std::time::Duration;

use anyhow::{Context, Result};
use axum::routing::get;
use axum::Router;
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use crate::db::{self, Db, WriterState};

/// Page latency buckets, in seconds: around the p95 bound the cutover gates on.
const BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 7.0, 15.0,
];

/// Install the process-wide recorder and return the route that renders it.
pub fn install() -> Result<Router> {
    let handle: PrometheusHandle = PrometheusBuilder::new()
        .set_buckets(BUCKETS)
        .context("metrics buckets")?
        .install_recorder()
        .context("install the metrics recorder")?;
    schema_version("binary", db::migrations::binary_version());
    Ok(Router::new().route("/metrics", get(move || std::future::ready(handle.render()))))
}

/// 0 candidate, 1 leader, 2 reacquiring.
pub fn writer_state(state: Option<WriterState>) {
    let value = match state {
        Some(WriterState::Candidate) | None => 0.0,
        Some(WriterState::Leader) => 1.0,
        Some(WriterState::Reacquiring) => 2.0,
    };
    gauge!("explorer_writer_state").set(value);
}

/// Unix time of the writer's last commit or keepalive.
pub fn writer_last_ok(at: i64) {
    gauge!("explorer_writer_last_ok_seconds").set(at as f64);
}

/// How far the forward loop is behind the chain head.
pub fn tip_lag(blocks: i64) {
    gauge!("explorer_tip_lag_blocks").set(blocks as f64);
}

/// D (`of="db"`) or B (`of="binary"`).
pub fn schema_version(of: &'static str, version: i64) {
    gauge!("explorer_schema_version", "of" => of).set(version as f64);
}

/// The newest block a web replica's follower has seen.
pub fn latest_block_timestamp(at: i64) {
    gauge!("explorer_latest_block_timestamp_seconds").set(at as f64);
}

/// A response the 503 middleware produced.
pub fn http_503() {
    counter!("explorer_http_503_total").increment(1);
}

pub fn request_duration(route: &str, seconds: f64) {
    histogram!("explorer_http_request_duration_seconds", "route" => route.to_string())
        .record(seconds);
}

/// Copy the indexer's status into the gauges every few seconds.
pub async fn watch_status(db: Db) {
    let status = db::status(&db);
    loop {
        let s = status.borrow().clone();
        writer_state(s.writer);
        if let Some(at) = db::writer_last_ok(&db) {
            writer_last_ok(at);
        }
        if let Some(lag) = s.sync.and_then(|sync| sync.tip_lag) {
            tip_lag(lag);
        }
        if let Some(d) = s.schema.db {
            schema_version("db", d);
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_series_the_alerts_use_is_rendered() {
        let recorder = PrometheusBuilder::new()
            .set_buckets(BUCKETS)
            .unwrap()
            .build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            writer_state(Some(crate::db::WriterState::Leader));
            writer_last_ok(1_700_000_000);
            tip_lag(3);
            schema_version("db", 1);
            schema_version("binary", 1);
            latest_block_timestamp(1_700_000_000);
            http_503();
            request_duration("/block/{block_id}", 0.012);
        });
        let text = handle.render();
        for series in [
            "explorer_writer_state 1",
            "explorer_writer_last_ok_seconds 1700000000",
            "explorer_tip_lag_blocks 3",
            "explorer_schema_version{of=\"db\"} 1",
            "explorer_schema_version{of=\"binary\"} 1",
            "explorer_latest_block_timestamp_seconds 1700000000",
            "explorer_http_503_total 1",
            "explorer_http_request_duration_seconds_bucket{route=\"/block/{block_id}\"",
        ] {
            assert!(text.contains(series), "{series} missing from:\n{text}");
        }
    }
}
