//! The anchoring contract at `0x…0a00`, read from the node rather than the index: the
//! registries and records it was seeded with at genesis emitted no events.

use std::ops::RangeInclusive;

use alloy_primitives::{keccak256, B256, U256};
use alloy_sol_types::{sol, SolCall, SolValue};
use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};

use crate::rpc::{ChainRpc, RpcError};

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

    #[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
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

/// A view of the contract, for [`ask`].
fn view<C: SolCall>(call: &C) -> (String, Value) {
    let data = format!("0x{}", hex::encode(call.abi_encode()));
    let params = json!([{"to": ADDRESS, "data": data}, "latest"]);
    ("eth_call".into(), params)
}

fn returns<C: SolCall>(result: Value) -> Result<C::Return> {
    let data = hex::decode(result.as_str().unwrap_or("").trim_start_matches("0x"))?;
    C::abi_decode_returns(&data).with_context(|| format!("decode {}", C::SIGNATURE))
}

/// A storage word, for the two counts no view returns. The slots are the migration's layout,
/// which nvnmchain-contracts' `StorageLayout.t.sol` pins.
fn word(slot: B256) -> (String, Value) {
    let params = json!([ADDRESS, slot.to_string(), "latest"]);
    ("eth_getStorageAt".into(), params)
}

fn word_value(answer: Value) -> Result<U256> {
    let word = answer.as_str().unwrap_or("");
    word.parse().with_context(|| format!("storage word {word}"))
}

/// `_registryCount`, above `_formerAdmin` in slot 3. Ids run `1..=count`.
const REGISTRY_COUNT: B256 = B256::with_last_byte(3);

fn registry_total(answer: Value) -> Result<u64> {
    Ok((word_value(answer)? >> 160usize).saturating_to())
}

/// `_recordCount[registryId]`, the mapping at slot 5. Ids run `1..=count`.
fn record_count(registry_id: u64) -> B256 {
    keccak256((registry_id, 5u64).abi_encode())
}

/// `calls` in one request, answered in order.
async fn ask<const N: usize>(
    rpc: &ChainRpc,
    calls: [(String, Value); N],
) -> Result<[Result<Value, RpcError>; N]> {
    let answers = rpc.batch_call(calls.into()).await?;
    answers
        .try_into()
        .map_err(|_| anyhow!("an answer is missing"))
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
    let [total, listed] = ask(rpc, [word(REGISTRY_COUNT), view(&call)]).await?;
    let total = registry_total(total?)?;
    if total == 0 {
        return Ok((Vec::new(), 0)); // nor may there be a contract to answer
    }
    Ok((returns::<registriesCall>(listed?)?.registriesOut, total))
}

/// A registry by id, and how many records it has; `None` if there is no such registry.
pub async fn find_registry(rpc: &ChainRpc, registry_id: u64) -> Result<Option<(Registry, u64)>> {
    // Ids start at 1: the contract reads 0 as every registry.
    if registry_id == 0 {
        return Ok(None);
    }
    let by_id = registriesCall {
        registryId: registry_id,
        pagination: PageRequest::default(),
    };
    let [count, registry, records] = ask(
        rpc,
        [
            word(REGISTRY_COUNT),
            view(&by_id),
            word(record_count(registry_id)),
        ],
    )
    .await?;
    // The view reverts for a missing id, so the count decides first.
    if !(1..=registry_total(count?)?).contains(&registry_id) {
        return Ok(None);
    }
    let registry = returns::<registriesCall>(registry?)?.registriesOut.pop();
    let records = word_value(records?)?.saturating_to();
    Ok(Some((registry.context("no registry")?, records)))
}

/// A registry, the latest version of each of its records newest first, and how many records
/// it has; `None` if there is no such registry.
pub async fn registry(
    rpc: &ChainRpc,
    registry_id: u64,
    page: u64,
    per_page: u64,
) -> Result<Option<(Registry, Vec<Record>, u64)>> {
    let Some((registry, total)) = find_registry(rpc, registry_id).await? else {
        return Ok(None);
    };
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
    let [records] = ask(rpc, [view(&call)]).await?;
    let mut records = returns::<recordsCall>(records?)?.recordsOut;
    records.reverse();
    Ok(Some((registry, records, total)))
}

/// A record's latest version, whose `index` is how many versions there are; `None` if there is
/// no such record.
pub async fn latest(rpc: &ChainRpc, registry_id: u64, record_id: u64) -> Result<Option<Record>> {
    // Ids start at 1: the contract reads record 0 as a page of the registry.
    if record_id == 0 {
        return Ok(None);
    }
    // Index 0 asks for the latest.
    let [count, latest] = ask(
        rpc,
        [
            word(record_count(registry_id)),
            view(&version(registry_id, record_id, 0)),
        ],
    )
    .await?;
    // The view reverts for a missing id, so the count decides first.
    let count: u64 = word_value(count?)?.saturating_to();
    if !(1..=count).contains(&record_id) {
        return Ok(None);
    }
    let latest = returns::<recordsCall>(latest?)?.recordsOut.pop();
    Ok(Some(latest.context("no record")?))
}

/// A record's latest version, and its versions newest first; `None` if there is no such
/// record.
pub async fn record(
    rpc: &ChainRpc,
    registry_id: u64,
    record_id: u64,
    page: u64,
    per_page: u64,
) -> Result<Option<(Record, Vec<Record>)>> {
    let Some(latest) = latest(rpc, registry_id, record_id).await? else {
        return Ok(None);
    };
    let Some(indexes) = newest_first(latest.index, page, per_page) else {
        return Ok(Some((latest, Vec::new())));
    };
    let calls = indexes
        .rev()
        .map(|index| view(&version(registry_id, record_id, index)))
        .collect();
    let mut versions = Vec::new();
    for result in rpc.batch_call(calls).await? {
        versions.extend(returns::<recordsCall>(result?)?.recordsOut);
    }
    Ok(Some((latest, versions)))
}

/// Registries named exactly `q`, and the latest version of the record with checksum `q` in
/// each registry that has one. The contract matches names exactly and nothing else. Both in
/// one round trip, as a search box asks at every keystroke.
pub async fn lookup(rpc: &ChainRpc, q: &str) -> Result<(Vec<Registry>, Vec<Record>)> {
    let named = registriesByNameCall {
        name: q.into(),
        matchMode: 1,
        pagination: PageRequest::default(),
    };
    let anchored = recordsCall {
        checksum: q.into(),
        ..version(0, 0, 0)
    };
    let [named, anchored] = ask(rpc, [view(&named), view(&anchored)]).await?;
    Ok((
        returns::<registriesByNameCall>(named?)?.registriesOut,
        returns::<recordsCall>(anchored?)?.recordsOut,
    ))
}
