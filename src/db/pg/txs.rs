//! Transactions: reads, and the trace cache web pages write.

use sqlx::postgres::PgRow;
use sqlx::Row;

use super::q;
use super::shared::{blob_addr, blob_hex, hex_blob, tx_cols, tx_list_cols, without_nul};
use super::PgDb;
use crate::db::{page_offset, TxColumns};
use crate::models::Transaction;

pub(crate) fn tx(r: &PgRow) -> Result<Transaction, sqlx::Error> {
    let addr = |i: usize| -> Result<Option<String>, sqlx::Error> {
        Ok(r.try_get::<Option<Vec<u8>>, _>(i)?.map(|b| blob_addr(&b)))
    };
    Ok(Transaction {
        hash: blob_hex(&r.try_get::<Vec<u8>, _>(0)?),
        block_number: r.try_get(1)?,
        position: r.try_get(2)?,
        from_addr: blob_addr(&r.try_get::<Vec<u8>, _>(3)?),
        to_addr: addr(4)?,
        status: r.try_get(5)?,
        gas_used: r.try_get(6)?,
        base_fee: r.try_get(7)?,
        contract_address: addr(8)?,
        fee_token: addr(9)?,
        fee_amount: r.try_get(10)?,
        input: r.try_get(11)?,
        raw: r
            .try_get::<Option<Vec<u8>>, _>(12)?
            .map(|b| format!("0x{}", hex::encode(b))),
        trace_data: r.try_get(13)?,
        receipt_data: r.try_get(14)?,
        timestamp: r.try_get(15)?,
        created_at: r.try_get(16)?,
    })
}

pub(crate) async fn get_transaction(p: &PgDb, hash: &str) -> Option<Transaction> {
    let hash = hex_blob(hash);
    q::query_opt(
        &p.read,
        "get_transaction",
        concat!("SELECT ", tx_cols!(), " FROM transactions WHERE hash = $1"),
        |q| q.bind(hash),
        tx,
    )
    .await
}

pub(crate) async fn get_transactions_in_range(
    p: &PgDb,
    from: i64,
    to: i64,
    columns: TxColumns,
) -> Vec<Transaction> {
    let sql = match columns {
        TxColumns::List => concat!(
            "SELECT ",
            tx_list_cols!(),
            " FROM transactions WHERE block_number BETWEEN $1 AND $2 ORDER BY block_number, position"
        ),
        TxColumns::Full => concat!(
            "SELECT ",
            tx_cols!(),
            " FROM transactions WHERE block_number BETWEEN $1 AND $2 ORDER BY block_number, position"
        ),
    };
    q::query_rows(
        &p.read,
        "get_transactions_in_range",
        sql,
        |q| q.bind(from).bind(to),
        tx,
    )
    .await
}

pub(crate) async fn get_block_transactions(
    p: &PgDb,
    block_number: i64,
    columns: TxColumns,
) -> Vec<Transaction> {
    let sql = match columns {
        TxColumns::List => concat!(
            "SELECT ",
            tx_list_cols!(),
            " FROM transactions WHERE block_number = $1 ORDER BY position"
        ),
        TxColumns::Full => concat!(
            "SELECT ",
            tx_cols!(),
            " FROM transactions WHERE block_number = $1 ORDER BY position"
        ),
    };
    q::query_rows(
        &p.read,
        "get_block_transactions",
        sql,
        |q| q.bind(block_number),
        tx,
    )
    .await
}

/// Two walks, one down each address index and each cut at the page's end,
/// then merged, as on SQLite. A transaction to itself is read once, as sent.
macro_rules! address_page {
    ($cols:expr) => {
        concat!(
            "SELECT ",
            $cols,
            " FROM (
                 SELECT h, b, p FROM (
                     (SELECT hash AS h, block_number AS b, position AS p
                      FROM transactions WHERE from_addr = $1
                      ORDER BY block_number DESC, position DESC LIMIT $4)
                     UNION ALL
                     (SELECT hash, block_number, position
                      FROM transactions WHERE to_addr = $1 AND from_addr <> $1
                      ORDER BY block_number DESC, position DESC LIMIT $4)
                 ) u ORDER BY b DESC, p DESC LIMIT $2 OFFSET $3
             ) page JOIN transactions ON hash = page.h
             ORDER BY page.b DESC, page.p DESC"
        )
    };
}

pub(crate) async fn get_address_transactions(
    p: &PgDb,
    address: &str,
    page: u32,
    per_page: u32,
    columns: TxColumns,
) -> Vec<Transaction> {
    let (limit, offset) = (i64::from(per_page), page_offset(page, per_page));
    let sql = match columns {
        TxColumns::List => address_page!(tx_list_cols!()),
        TxColumns::Full => address_page!(tx_cols!()),
    };
    let address = hex_blob(address);
    q::query_rows(
        &p.read,
        "get_address_transactions",
        sql,
        |q| {
            q.bind(address)
                .bind(limit)
                .bind(offset)
                .bind(limit.saturating_add(offset))
        },
        tx,
    )
    .await
}

pub(crate) async fn get_transactions(p: &PgDb, page: u32, per_page: u32) -> Vec<Transaction> {
    let (limit, offset) = (i64::from(per_page), page_offset(page, per_page));
    q::query_rows(
        &p.read,
        "get_transactions",
        concat!(
            "SELECT ",
            tx_list_cols!(),
            " FROM transactions ORDER BY block_number DESC, position DESC LIMIT $1 OFFSET $2"
        ),
        |q| q.bind(limit).bind(offset),
        tx,
    )
    .await
}

pub(crate) async fn get_transaction_count(p: &PgDb) -> i64 {
    q::query_count(
        &p.read,
        "get_transaction_count",
        "SELECT COUNT(*) FROM transactions",
        |q| q,
    )
    .await
}

pub(crate) async fn get_address_transaction_count(p: &PgDb, address: &str) -> i64 {
    let address = hex_blob(address);
    q::query_count(
        &p.read,
        "get_address_transaction_count",
        "SELECT COUNT(*) FROM transactions WHERE from_addr = $1 OR to_addr = $1",
        |q| q.bind(address),
    )
    .await
}

/// Cache a call trace onto its row: a cache write, best effort, on the cache
/// pool. The writer keeps it through `COALESCE` when it rewrites the row.
pub(crate) async fn set_trace(p: &PgDb, hash: &str, trace: &str) -> anyhow::Result<()> {
    let (hash, trace) = (hex_blob(hash), without_nul(trace));
    q::exec_best_effort(
        &p.cache,
        "set_trace",
        "UPDATE transactions SET trace_data = $1 WHERE hash = $2",
        |q| q.bind(trace).bind(hash),
    )
    .await
}
