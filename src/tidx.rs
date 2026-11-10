//! Transfers, balances and holder counts read from a tidx's `/query` instead of
//! SQLite. tidx skips block 0, so genesis balances show only once seeded into
//! it, and keeps no raw transaction, trace or receipt: the transaction page has
//! those. Holder lists and counts come from its snapshots, refreshed every 15
//! minutes; an address's own balances are live.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::db::page_offset;
use crate::decoder::{checksum_address, is_valid_address, FEE_MANAGER_ADDRESS};
use crate::models::Transaction;
use crate::rpc::parse_int_any;

/// A handle on one chain's tidx. Cheap to clone; the reads take it by value so
/// a caller can hand one to a future.
#[derive(Debug, Clone)]
pub struct Tidx {
    client: Client,
    url: String,
    chain_id: u64,
}

#[derive(Deserialize)]
struct Answer {
    error: Option<String>,
    #[serde(default)]
    rows: Vec<Vec<Value>>,
}

impl Tidx {
    pub fn new(url: &str, chain_id: u64) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("HTTP client"),
            url: format!("{}/query", url.trim_end_matches('/')),
            chain_id,
        }
    }

    async fn rows(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        let chain_id = self.chain_id.to_string();
        let answer: Answer = self
            .client
            .get(&self.url)
            .query(&[
                ("sql", sql),
                ("chainId", &chain_id),
                ("engine", "clickhouse"),
            ])
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .context("tidx query")?
            .json()
            .await
            .map_err(reqwest::Error::without_url)
            .context("tidx answer")?;
        if let Some(error) = answer.error {
            bail!("tidx: {error}");
        }
        Ok(answer.rows)
    }

    /// One page of the transactions `address` sent or received, newest first.
    pub async fn address_transactions(
        self,
        address: &str,
        page: u32,
        per_page: u32,
    ) -> Result<Vec<Transaction>> {
        let a = lower(address)?;
        let offset = page_offset(page, per_page);
        let fee_manager = FEE_MANAGER_ADDRESS.to_lowercase();
        // The fee is the transaction's last transfer into the Fee Manager: tidx
        // records no fee amount, and no fee token outside type-0x76 transactions.
        let sql = format!(
            r#"WITH page AS (
                   SELECT block_num, idx, hash, "from", "to", input,
                          toUnixTimestamp(block_timestamp) AS ts
                   FROM txs FINAL WHERE "from" = '{a}' OR "to" = '{a}'
                   ORDER BY block_num DESC, idx DESC LIMIT {per_page} OFFSET {offset})
               SELECT p.hash, p.block_num, p.idx, p."from", p."to", r.status, r.gas_used,
                      r.effective_gas_price, r.contract_address, f.token, toString(f.amount),
                      p.input, p.ts
               FROM page p
               LEFT JOIN (
                   SELECT block_num, tx_idx, status, gas_used, effective_gas_price,
                          contract_address
                   FROM receipts FINAL
                   WHERE (block_num, tx_idx) IN (SELECT block_num, idx FROM page)) r
               ON r.block_num = p.block_num AND r.tx_idx = p.idx
               LEFT JOIN (
                   SELECT block_num, tx_idx, argMax(token, log_idx) AS token,
                          argMax(amount, log_idx) AS amount
                   FROM token_transfers FINAL
                   WHERE "to" = '{fee_manager}'
                     AND (block_num, tx_idx) IN (SELECT block_num, idx FROM page)
                   GROUP BY block_num, tx_idx) f
               ON f.block_num = p.block_num AND f.tx_idx = p.idx
               ORDER BY p.block_num DESC, p.idx DESC"#
        );
        Ok(self.rows(&sql).await?.iter().map(|row| tx(row)).collect())
    }

    /// How many transactions and transfers `address` sent or received.
    pub async fn address_counts(self, address: &str) -> Result<(i64, i64)> {
        let a = lower(address)?;
        let sql = format!(
            r#"SELECT (SELECT count() FROM txs FINAL WHERE "from" = '{a}' OR "to" = '{a}'),
                      (SELECT count() FROM token_transfers FINAL
                       WHERE "from" = '{a}' OR "to" = '{a}')"#
        );
        let rows = self.rows(&sql).await?;
        let row = rows.first().context("tidx counts")?;
        Ok((parse_int_any(&row[0]), parse_int_any(&row[1])))
    }

    /// One page of the transfers `address` sent or received, newest first.
    pub async fn address_transfers(
        self,
        address: &str,
        page: u32,
        per_page: u32,
    ) -> Result<Vec<Value>> {
        let a = lower(address)?;
        self.transfers(
            &format!(r#""from" = '{a}' OR "to" = '{a}'"#),
            page,
            per_page,
        )
        .await
    }

    /// One page of `token`'s transfers, newest first.
    pub async fn token_transfers(
        self,
        token: &str,
        page: u32,
        per_page: u32,
    ) -> Result<Vec<Value>> {
        let t = lower(token)?;
        self.transfers(&format!("token = '{t}'"), page, per_page)
            .await
    }

    /// Transfers matching `filter`, in the shape `db::get_address_transfers`
    /// gives less its row `id`.
    async fn transfers(&self, filter: &str, page: u32, per_page: u32) -> Result<Vec<Value>> {
        let offset = page_offset(page, per_page);
        let sql = format!(
            r#"WITH page AS (
                   SELECT tx_hash, block_num, tx_idx, log_idx, token, "from", "to",
                          toString(amount) AS amount, toUnixTimestamp(block_timestamp) AS ts
                   FROM token_transfers FINAL WHERE {filter}
                   ORDER BY block_num DESC, log_idx DESC LIMIT {per_page} OFFSET {offset})
               SELECT p.tx_hash, p.block_num, p.log_idx, p.token, p."from", p."to", p.amount,
                      p.ts, if(t."from" = '', p."from", t."from"),
                      if(t."from" = '', p."to", t."to"), if(t."from" = '', 1, r.status)
               FROM page p
               LEFT JOIN (
                   SELECT block_num, idx, "from", "to" FROM txs FINAL
                   WHERE (block_num, idx) IN (SELECT block_num, tx_idx FROM page)) t
               ON t.block_num = p.block_num AND t.idx = p.tx_idx
               LEFT JOIN (
                   SELECT block_num, tx_idx, status FROM receipts FINAL
                   WHERE (block_num, tx_idx) IN (SELECT block_num, tx_idx FROM page)) r
               ON r.block_num = p.block_num AND r.tx_idx = p.tx_idx
               ORDER BY p.block_num DESC, p.log_idx DESC"#
        );
        let rows = self.rows(&sql).await?;
        let transfers = rows.iter().map(|row| {
            json!({
                "tx_hash": row[0],
                "block_number": row[1],
                "log_index": row[2],
                "token_addr": checksum_address(text(&row[3])),
                "from_addr": checksum_address(text(&row[4])),
                "to_addr": checksum_address(text(&row[5])),
                "amount": row[6],
                "timestamp": row[7],
                "tx_from": checksum_address(text(&row[8])),
                "tx_to": address_at(&row[9]),
                "tx_status": parse_int_any(&row[10]),
                "tx_timestamp": row[7],
            })
        });
        Ok(transfers.collect())
    }

    pub async fn token_transfer_count(self, token: &str) -> Result<i64> {
        let t = lower(token)?;
        let sql = format!("SELECT count() FROM token_transfers FINAL WHERE token = '{t}'");
        let rows = self.rows(&sql).await?;
        let row = rows.first().context("tidx count")?;
        Ok(parse_int_any(&row[0]))
    }

    /// `(token, balance)` for each token `address` holds, by token address.
    pub async fn address_balances(self, address: &str) -> Result<Vec<(String, String)>> {
        let a = lower(address)?;
        let sql = format!(
            "SELECT token, toString(balance) FROM address_balances
             WHERE holder = '{a}' ORDER BY token"
        );
        self.pairs(&sql).await
    }

    /// One page of `token`'s holders as `(address, balance)`, largest first.
    pub async fn token_holders(
        self,
        token: &str,
        page: u32,
        per_page: u32,
    ) -> Result<Vec<(String, String)>> {
        let t = lower(token)?;
        let offset = page_offset(page, per_page);
        let sql = format!(
            "SELECT holder, toString(balance) FROM token_balances_snapshot
             WHERE token = '{t}' ORDER BY balance DESC, holder LIMIT {per_page} OFFSET {offset}"
        );
        self.pairs(&sql).await
    }

    /// `(token, holders)` for every token with a holder.
    pub async fn token_holder_counts(self) -> Result<Vec<(String, i64)>> {
        let rows = self
            .rows("SELECT token, holder_count FROM token_holder_counts")
            .await?;
        Ok(rows
            .iter()
            .map(|row| (checksum_address(text(&row[0])), parse_int_any(&row[1])))
            .collect())
    }

    /// `(address, decimal)` rows.
    async fn pairs(&self, sql: &str) -> Result<Vec<(String, String)>> {
        let rows = self.rows(sql).await?;
        Ok(rows
            .iter()
            .map(|row| (checksum_address(text(&row[0])), text(&row[1]).to_string()))
            .collect())
    }
}

/// tidx keeps addresses lowercase. Checked here, as it goes into SQL.
fn lower(address: &str) -> Result<String> {
    if !is_valid_address(address) {
        bail!("not an address: {address}");
    }
    Ok(address.to_lowercase())
}

fn text(value: &Value) -> &str {
    value.as_str().unwrap_or_default()
}

fn address_at(value: &Value) -> Option<String> {
    value
        .as_str()
        .filter(|a| !a.is_empty())
        .map(checksum_address)
}

fn tx(row: &[Value]) -> Transaction {
    let price: u128 = text(&row[7]).parse().unwrap_or(0);
    Transaction {
        hash: text(&row[0]).to_string(),
        block_number: parse_int_any(&row[1]),
        position: parse_int_any(&row[2]),
        from_addr: checksum_address(text(&row[3])),
        to_addr: address_at(&row[4]),
        status: parse_int_any(&row[5]),
        gas_used: parse_int_any(&row[6]),
        base_fee: format!("{price:#x}"),
        contract_address: address_at(&row[8]),
        fee_token: address_at(&row[9]),
        fee_amount: text(&row[10]).to_string(),
        input: text(&row[11]).to_string(),
        raw: None,
        trace_data: None,
        receipt_data: None,
        timestamp: parse_int_any(&row[12]),
        created_at: 0,
    }
}
