// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The read cost of **one open** of a range: becoming leader reads
//! each SST once, without re-scanning the same blocks for every derived table.
//!
//! On becoming leader, the engine rebuilds its RAM state from the derived zone: queues
//! (VO locks, `seq` counters), unfinished invocations, the outbox `seq` counter,
//! signals, inbound. Each full-table scan runs with a large read-ahead and **no**
//! block cache, so formerly each table was another pass GETting the same blocks of
//! every SST again (the same SST was read 6–8 times).

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    cairn_server::node::{self, NodeConfig, NodeMode, RunningNode},
    cairn_store::{
        testkit::{FaultyStore, Op, PathClass},
        ObjectStores, StoreLocation,
    },
    common::fake_sdk::{FakeSdk, Step},
    slatedb::object_store::ObjectStore,
    std::{sync::Arc, time::Duration},
};

async fn start_on(store: &Arc<FaultyStore>) -> Result<RunningNode> {
    let stores = ObjectStores::single(StoreLocation::from_store(store.clone(), "", "memory://"));
    let mut cfg = NodeConfig::new(stores, NodeMode::single(1));
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    node::start(cfg)
        .await
        .map_err(|e| anyhow!("build node: {e}"))
}

/// GETs of flushed SSTs (`compacted/`, not the WAL's SSTs), from every caller.
fn sst_gets(store: &FaultyStore) -> u64 {
    store
        .counts_by_source_object()
        .into_iter()
        .filter(|(_, op, class, kind, _)| {
            *op == Op::Get && *class == PathClass::Compacted && *kind == "sst"
        })
        .map(|(.., n)| n)
        .sum()
}

/// The number of flushed SSTs (`compacted/`) currently on the bucket.
async fn sst_count(store: &Arc<FaultyStore>) -> Result<usize> {
    use futures::TryStreamExt;
    let all: Vec<_> = store.list(None).try_collect().await?;
    Ok(all
        .iter()
        .filter(|m| {
            let p = m.location.as_ref();
            p.contains("/compacted/") && p.ends_with(".sst")
        })
        .count())
}

/// One workload round leaving every kind of derived row: completed invocations (status
/// index row + scheduled cleanup), fired timers (a tombstone in the timer index), long-sleeping
/// invocations (live timer, queue), cross-invocation calls (outbox, inbound).
async fn workload(url: &str, round: usize) -> Result<()> {
    for i in 0..40 {
        let (code, body) = common::send(url, "Echo", "now", None, "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let (code, body) = common::send(url, "Nap", "short", None, "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        if i % 4 == 0 {
            let (code, body) = common::send(url, "Nap", "long", None, "{}").await?;
            ensure!(code == 200 || code == 202, "{code} {body}");
            let key = format!("k{round}-{i}");
            let (code, body) = common::send(url, "Caller", "run", Some(&key), "{}").await?;
            ensure!(code == 200 || code == 202, "{code} {body}");
        }
    }
    Ok(())
}

/// GETs of flushed SSTs repeating a byte range this open **already** GOT: `(repeated
/// GETs, total GETs)`.
fn repeated_sst_reads(
    reads: &[(slatedb::object_store::path::Path, std::ops::Range<u64>)],
) -> (usize, usize) {
    let mut seen: std::collections::BTreeMap<String, Vec<std::ops::Range<u64>>> =
        Default::default();
    let (mut repeated, mut total) = (0, 0);
    for (path, r) in reads {
        let p = path.as_ref();
        if !(p.contains("/compacted/") && p.ends_with(".sst")) {
            continue;
        }
        total += 1;
        let prior = seen.entry(p.to_string()).or_default();
        if prior.iter().any(|q| q.start < r.end && r.start < q.end) {
            repeated += 1;
        }
        prior.push(r.clone());
    }
    (repeated, total)
}

/// Becoming leader on a range with history reads each SST block **once**, not
/// once per derived table.
///
/// 1. Three rounds: open a node on the same bucket, run the workload, shut down cleanly (the
///    seal flushes the memtable ⇒ each round leaves L0 SSTs carrying both the hot zone and
///    the derived zone).
/// 2. Open the node a fourth time, recording the byte range of every GET from the open until a
///    new call has been served and the recovery path has finished (plus 1 s).
///
/// Must see: no SST GET repeats a byte range already GOT in this open. Before
/// the fix: each full-table scan (`vqueue` three times, the status index,
/// outbox, signal, inbound) ran with a large read-ahead and no block cache, so it GOT
/// the same derived-zone blocks of every SST again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opening_a_range_reads_each_sst_block_once() -> Result<()> {
    let bucket = common::bucket();
    let store = FaultyStore::wrap(Arc::clone(&*bucket) as Arc<dyn ObjectStore>);
    let sdk = FakeSdk::builder()
        .service("Echo", "now", vec![Step::output("\"ok\"")])
        .service(
            "Nap",
            "short",
            vec![Step::Sleep(30), Step::output("\"awake\"")],
        )
        .service(
            "Nap",
            "long",
            vec![Step::Sleep(3_600_000), Step::output("\"awake\"")],
        )
        .service(
            "Caller",
            "run",
            vec![Step::call("Echo", "now", None), Step::EchoCallResult],
        )
        .spawn()
        .await?;

    for round in 0..3 {
        let node = start_on(&store).await?;
        let res = common::within(Duration::from_secs(60), "workload", async {
            if round == 0 {
                common::register(&node.url(), &sdk.url).await?;
            }
            workload(&node.url(), round).await?;
            // Let the short timers fire and the calls complete.
            tokio::time::sleep(Duration::from_millis(800)).await;
            Ok(())
        })
        .await;
        node.shutdown().await;
        res?;
    }

    let ssts = sst_count(&store).await?;
    let before = sst_gets(&store);
    store.record_reads();
    let node = start_on(&store).await?;
    let served = common::within(Duration::from_secs(30), "first call", async {
        let (code, body) = common::invoke(&node.url(), "Echo", "now", None, "{}").await?;
        ensure!(code == 200, "{code} {body}");
        Ok(())
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let gets = sst_gets(&store) - before;
    let (repeated, recorded) = repeated_sst_reads(&store.take_reads());
    node.shutdown().await;
    served?;
    eprintln!("open: {ssts} SSTs, {gets} SST GETs, {repeated} GETs re-reading blocks already read");
    ensure!(ssts >= 2, "too few SSTs to measure anything: {ssts}");
    ensure!(
        recorded as u64 == gets,
        "recorded byte ranges disagree with the counter: {recorded} ≠ {gets}"
    );
    ensure!(
        repeated == 0,
        "becoming leader GOT {repeated}/{gets} times an SST block already read — one read pass per derived table"
    );
    Ok(())
}
