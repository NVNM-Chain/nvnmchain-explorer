//! Transfer listings and anchoring events.

use serde_json::{json, Value};
use sqlx::postgres::PgRow;
use sqlx::Row;

use super::q;
use super::shared::{blob_addr, blob_hex, hex_blob, transfer_cols};
use super::PgDb;
use crate::db::page_offset;
use crate::models::AnchoringEvent;

/// A transfer row plus the four fields of its transaction the listings show,
/// as `sqlite.rs`'s `row_to_transfer_json` builds it.
fn transfer_json(r: &PgRow) -> Result<Value, sqlx::Error> {
    let from_addr = blob_addr(&r.try_get::<Vec<u8>, _>(5)?);
    let to_addr = blob_addr(&r.try_get::<Vec<u8>, _>(6)?);
    let timestamp: i64 = r.try_get(8)?;
    let (tx_from, tx_to, tx_timestamp, tx_status) = match r.try_get::<Option<Vec<u8>>, _>(10)? {
        Some(from) => (
            blob_addr(&from),
            r.try_get::<Option<Vec<u8>>, _>(11)?.map(|b| blob_addr(&b)),
            r.try_get::<i64, _>(12)?,
            r.try_get::<i64, _>(13)?,
        ),
        None => (from_addr.clone(), Some(to_addr.clone()), timestamp, 1),
    };
    Ok(json!({
        "id": r.try_get::<i64, _>(0)?,
        "tx_hash": blob_hex(&r.try_get::<Vec<u8>, _>(1)?),
        "block_number": r.try_get::<i64, _>(2)?,
        "log_index": r.try_get::<i64, _>(3)?,
        "token_addr": blob_addr(&r.try_get::<Vec<u8>, _>(4)?),
        "from_addr": from_addr,
        "to_addr": to_addr,
        "amount": r.try_get::<String, _>(7)?,
        "timestamp": timestamp,
        "created_at": r.try_get::<i64, _>(9)?,
        "tx_from": tx_from,
        "tx_to": tx_to,
        "tx_timestamp": tx_timestamp,
        "tx_status": tx_status,
    }))
}

/// One page of transfers, joined to their transactions. `$keys` selects the
/// page's rows as `(i, b, l)`: id, block number, log index.
macro_rules! transfer_page {
    ($keys:expr) => {
        concat!(
            "SELECT ",
            transfer_cols!(),
            ", t.from_addr, t.to_addr, t.timestamp, t.status
             FROM (",
            $keys,
            ") page
             JOIN transfer_events e ON e.id = page.i
             LEFT JOIN transactions t ON t.hash = e.tx_hash
             ORDER BY page.b DESC, page.l DESC"
        )
    };
}

pub(crate) async fn get_token_transfers(
    p: &PgDb,
    token_addr: &str,
    page: u32,
    per_page: u32,
) -> Vec<Value> {
    let (token_addr, offset) = (hex_blob(token_addr), page_offset(page, per_page));
    q::query_rows(
        &p.read,
        "get_token_transfers",
        transfer_page!(
            "SELECT id AS i, block_number AS b, log_index AS l FROM transfer_events
             WHERE token_addr = $1 ORDER BY block_number DESC, log_index DESC LIMIT $2 OFFSET $3"
        ),
        |q| q.bind(token_addr).bind(i64::from(per_page)).bind(offset),
        transfer_json,
    )
    .await
}

/// Two walks merged, as `get_address_transactions` does; a transfer to itself
/// is read once.
pub(crate) async fn get_address_transfers(
    p: &PgDb,
    address: &str,
    page: u32,
    per_page: u32,
) -> Vec<Value> {
    let (limit, offset) = (i64::from(per_page), page_offset(page, per_page));
    let address = hex_blob(address);
    q::query_rows(
        &p.read,
        "get_address_transfers",
        transfer_page!(
            "SELECT i, b, l FROM (
                 (SELECT id AS i, block_number AS b, log_index AS l
                  FROM transfer_events WHERE from_addr = $1
                  ORDER BY block_number DESC, log_index DESC LIMIT $4)
                 UNION ALL
                 (SELECT id, block_number, log_index
                  FROM transfer_events WHERE to_addr = $1 AND from_addr <> $1
                  ORDER BY block_number DESC, log_index DESC LIMIT $4)
             ) u ORDER BY b DESC, l DESC LIMIT $2 OFFSET $3"
        ),
        |q| {
            q.bind(address)
                .bind(limit)
                .bind(offset)
                .bind(limit.saturating_add(offset))
        },
        transfer_json,
    )
    .await
}

pub(crate) async fn get_address_transfer_count(p: &PgDb, address: &str) -> i64 {
    let address = hex_blob(address);
    q::query_count(
        &p.read,
        "get_address_transfer_count",
        "SELECT COUNT(*) FROM transfer_events WHERE from_addr = $1 OR to_addr = $1",
        |q| q.bind(address),
    )
    .await
}

/// The latest `limit` writes to one registry, newest first.
pub(crate) async fn get_anchoring_events(
    p: &PgDb,
    registry_id: i64,
    limit: u32,
) -> Vec<AnchoringEvent> {
    q::query_rows(
        &p.read,
        "get_anchoring_events",
        "SELECT tx_hash, block_number, log_index, timestamp, event, registry_id, record_id, caller
         FROM anchoring_events WHERE registry_id = $1
         ORDER BY block_number DESC, log_index DESC
         LIMIT $2",
        |q| q.bind(registry_id).bind(i64::from(limit)),
        |r| {
            Ok(AnchoringEvent {
                tx_hash: blob_hex(&r.try_get::<Vec<u8>, _>(0)?),
                block_number: r.try_get(1)?,
                log_index: r.try_get(2)?,
                timestamp: r.try_get(3)?,
                event: r.try_get(4)?,
                registry_id: r.try_get(5)?,
                record_id: r.try_get(6)?,
                caller: blob_addr(&r.try_get::<Vec<u8>, _>(7)?),
            })
        },
    )
    .await
}
