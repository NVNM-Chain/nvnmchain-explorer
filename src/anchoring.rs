//! The anchoring contract at `0x…0a00`, read from the node rather than the index: the
//! registries and records it was seeded with at genesis emitted no events.

use std::ops::RangeInclusive;

use alloy_primitives::{keccak256, B256, U256};
use alloy_sol_types::{sol, SolCall, SolValue};
use anyhow::{Context, Result};
use serde_json::{json, Value};

use crate::rpc::ChainRpc;

pub const ADDRESS: &str = "0x0000000000000000000000000000000000000a00";

sol! {
    #[derive(Debug, serde::Serialize)]
    struct Record {
        string uri;
        string checksum;
        string checksumAlgo;
        string metadata;
        string timestamp;
        string status;
        uint64 recordId;
        uint64 index;
        bool isLatest;
        uint64 registryId;
    }

    #[derive(Debug, serde::Serialize)]
    struct Registry {
        uint64 id;
        string name;
        string description;
        string creator;
        string createdAt;
        string metadata;
    }

    #[derive(Default)]
    struct PageRequest {
        bytes key;
        uint64 offset;
        uint64 limit;
        bool countTotal;
        bool reverse;
    }

    struct PageResponse {
        bytes nextKey;
        uint64 total;
    }

    function records(uint64 registryId, string checksum, uint64 recordId, uint64 index, PageRequest pagination)
        external view returns (Record[] recordsOut, PageResponse paginationOut);

    function registries(uint64 registryId, PageRequest pagination)
        external view returns (Registry[] registriesOut, PageResponse paginationOut);

    function registriesByName(string name, uint8 matchMode, PageRequest pagination)
        external view returns (Registry[] registriesOut, PageResponse paginationOut);
}

fn eth_call<C: SolCall>(call: &C) -> Value {
    let data = format!("0x{}", hex::encode(call.abi_encode()));
    json!([{"to": ADDRESS, "data": data}, "latest"])
}

fn returns<C: SolCall>(result: Value) -> Result<C::Return> {
    let data = hex::decode(result.as_str().unwrap_or("").trim_start_matches("0x"))?;
    C::abi_decode_returns(&data).with_context(|| format!("decode {}", C::SIGNATURE))
}

async fn view<C: SolCall>(rpc: &ChainRpc, call: C) -> Result<C::Return> {
    returns::<C>(rpc.call("eth_call", eth_call(&call)).await?)
}

/// A storage word, for the two counts no view returns. The slots are the migration's layout,
/// which nvnmchain-contracts' `StorageLayout.t.sol` pins.
async fn word(rpc: &ChainRpc, slot: B256) -> Result<U256> {
    let word = rpc
        .eth_get_storage_at(ADDRESS, &slot.to_string(), "latest")
        .await?;
    word.parse().with_context(|| format!("storage word {word}"))
}

/// `_registryCount`, above `_moduleAdmin` in slot 3. Ids run `1..=count`.
async fn registry_count(rpc: &ChainRpc) -> Result<u64> {
    let slot = word(rpc, B256::with_last_byte(3)).await?;
    Ok((slot >> 160usize).saturating_to())
}

/// `_recordCount[registryId]`, the mapping at slot 5. Ids run `1..=count`.
async fn record_count(rpc: &ChainRpc, registry_id: u64) -> Result<u64> {
    let slot = keccak256((registry_id, 5u64).abi_encode());
    Ok(word(rpc, slot).await?.saturating_to())
}

/// The ids on 1-based page `page` of `1..=total` listed newest first, or `None` past the end.
fn newest_first(total: u64, page: u64, per_page: u64) -> Option<RangeInclusive<u64>> {
    let last = total
        .checked_sub((page - 1) * per_page)
        .filter(|&id| id > 0)?;
    Some(last.saturating_sub(per_page - 1).max(1)..=last)
}

fn version(registry_id: u64, record_id: u64, index: u64) -> recordsCall {
    recordsCall {
        registryId: registry_id,
        checksum: String::new(),
        recordId: record_id,
        index,
        pagination: PageRequest::default(),
    }
}

/// Registries newest first, and how many there are.
pub async fn registries(rpc: &ChainRpc, page: u64, per_page: u64) -> Result<(Vec<Registry>, u64)> {
    let total = registry_count(rpc).await?;
    if total == 0 {
        return Ok((Vec::new(), 0)); // nor may there be a contract to ask
    }
    let pagination = PageRequest {
        offset: (page - 1) * per_page,
        limit: per_page,
        reverse: true,
        ..Default::default()
    };
    let call = registriesCall {
        registryId: 0,
        pagination,
    };
    Ok((view(rpc, call).await?.registriesOut, total))
}

/// A registry, the latest version of each of its records newest first, and how many records
/// it has; `None` if there is no such registry.
pub async fn registry(
    rpc: &ChainRpc,
    registry_id: u64,
    page: u64,
    per_page: u64,
) -> Result<Option<(Registry, Vec<Record>, u64)>> {
    if !(1..=registry_count(rpc).await?).contains(&registry_id) {
        return Ok(None);
    }
    let call = registriesCall {
        registryId: registry_id,
        pagination: PageRequest::default(),
    };
    let registry = view(rpc, call)
        .await?
        .registriesOut
        .pop()
        .context("no registry")?;
    let total = record_count(rpc, registry_id).await?;
    let Some(ids) = newest_first(total, page, per_page) else {
        return Ok(Some((registry, Vec::new(), total)));
    };
    let pagination = PageRequest {
        offset: ids.start() - 1,
        limit: ids.end() - ids.start() + 1,
        ..Default::default()
    };
    let call = recordsCall {
        pagination,
        ..version(registry_id, 0, 0)
    };
    let mut records = view(rpc, call).await?.recordsOut;
    records.reverse();
    Ok(Some((registry, records, total)))
}

/// A record's latest version, and its versions newest first; `None` if there is no such
/// record. The latest's `index` is how many versions there are.
pub async fn record(
    rpc: &ChainRpc,
    registry_id: u64,
    record_id: u64,
    page: u64,
    per_page: u64,
) -> Result<Option<(Record, Vec<Record>)>> {
    if !(1..=record_count(rpc, registry_id).await?).contains(&record_id) {
        return Ok(None);
    }
    // Index 0 asks for the latest.
    let latest = view(rpc, version(registry_id, record_id, 0))
        .await?
        .recordsOut
        .pop()
        .context("no record")?;
    let Some(indexes) = newest_first(latest.index, page, per_page) else {
        return Ok(Some((latest, Vec::new())));
    };
    let calls = indexes
        .rev()
        .map(|index| {
            (
                "eth_call".to_string(),
                eth_call(&version(registry_id, record_id, index)),
            )
        })
        .collect();
    let mut versions = Vec::new();
    for result in rpc.batch_call(calls).await? {
        versions.extend(returns::<recordsCall>(result?)?.recordsOut);
    }
    Ok(Some((latest, versions)))
}

/// Registries named exactly `q`, and the latest version of the record with checksum `q` in
/// each registry that has one. The contract matches names exactly and nothing else.
pub async fn lookup(rpc: &ChainRpc, q: &str) -> Result<(Vec<Registry>, Vec<Record>)> {
    let call = registriesByNameCall {
        name: q.into(),
        matchMode: 1,
        pagination: PageRequest::default(),
    };
    let named = view(rpc, call).await?.registriesOut;
    let call = recordsCall {
        checksum: q.into(),
        ..version(0, 0, 0)
    };
    Ok((named, view(rpc, call).await?.recordsOut))
}
