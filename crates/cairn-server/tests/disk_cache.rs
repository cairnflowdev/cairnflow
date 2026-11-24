// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **The node's SST disk cache**: one `CachedObjectStore` per node
//! (`--disk-cache-dir`), shared by every range; off by default.

// The test runs on real time, network and processes; only `sim_cluster` must be deterministic.
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
        disk_cache::DiskCacheConfig,
        testkit::{FaultyStore, Op, PathClass},
        ObjectStores, StoreLocation,
    },
    common::fake_sdk::{FakeSdk, Step},
    serde_json::{json, Value},
    slatedb::object_store::ObjectStore,
    std::{path::PathBuf, sync::Arc, time::Duration},
};

/// A private temporary directory for one run (cleaned up afterwards).
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let p = std::env::temp_dir().join(format!(
            "cairn-disk-cache-{tag}-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&p).expect("create temp dir");
        TempDir(p)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn start_on(
    store: &Arc<FaultyStore>,
    partitions: u16,
    cache: Option<&TempDir>,
) -> Result<RunningNode> {
    let stores = ObjectStores::single(StoreLocation::from_store(store.clone(), "", "memory://"));
    let mut cfg = NodeConfig::new(stores, NodeMode::single(partitions));
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    cfg.disk_cache = cache.map(|d| DiskCacheConfig::new(&d.0));
    node::start(cfg)
        .await
        .map_err(|e| anyhow!("build node: {e}"))
}

/// GETs of flushed SSTs (`compacted/`) reaching the **object store** — a disk cache
/// hit does not get here.
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

async fn get_text(url: &str) -> Result<String> {
    Ok(reqwest::get(url).await?.text().await?)
}

async fn query(url: &str, sql: &str) -> Result<Value> {
    Ok(reqwest::Client::new()
        .post(format!("{url}/query"))
        .json(&json!({ "sql": sql }))
        .send()
        .await?
        .json()
        .await?)
}

/// **Where** in the directory ranges have cache (`ranges/<id>/…`).
fn ranges_in(dir: &std::path::Path) -> std::collections::BTreeSet<String> {
    std::fs::read_dir(dir.join("ranges"))
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect()
}

/// **Off by default** (the binary needs no disk): `NodeConfig::new` has no cache,
/// `/metrics` has no `cairnflow_disk_cache_*` line, `sys_node` returns NULL in its two
/// columns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_cache_off_by_default() -> Result<()> {
    let bucket = common::bucket();
    let store = FaultyStore::wrap(Arc::clone(&*bucket) as Arc<dyn ObjectStore>);
    let stores = ObjectStores::single(StoreLocation::from_store(store.clone(), "", "memory://"));
    let cfg = NodeConfig::new(stores, NodeMode::single(1));
    ensure!(cfg.disk_cache.is_none(), "NodeConfig::new turns cache on");
    let node = node::start(cfg).await.map_err(|e| anyhow!("{e}"))?;
    let url = node.url();
    let r = common::within(Duration::from_secs(30), "off by default", async {
        let m = get_text(&format!("{url}/metrics")).await?;
        ensure!(m.contains("cairnflow_invoke_requests_total"), "{m}");
        ensure!(!m.contains("cairnflow_disk_cache_"), "metric while off");
        let v = query(
            &url,
            "SELECT disk_cache_bytes, disk_cache_hit_ratio FROM sys_node",
        )
        .await?;
        ensure!(v["rows"] == json!([[null, null]]), "{v}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **One cache for the whole node, shared by every range; the first turn after a
/// leader change reads from disk:**
///
/// 1. Node A, two ranges, cache at `D`: load on both ranges, clean shutdown (the seal
///    flushes the memtable — new SSTs go into the cache, `cache_on_flush`).
/// 2. Node B, same bucket, same `D`: count SST GETs to the object store from opening
///    until the first call is served, plus 1 s (before the warm-up loop).
/// 3. Node C, same bucket, **without** cache: the same count.
///
/// Must see: `D` has SSTs of both ranges (one directory, one cache);
/// `cairnflow_disk_cache_hits_total` > 0 on B; B's GETs are at least 3× fewer than
/// C's (the threshold for turning it on by default); B's `sys_node` has cache bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_cache_is_shared_across_ranges() -> Result<()> {
    let bucket = common::bucket();
    let store = FaultyStore::wrap(Arc::clone(&*bucket) as Arc<dyn ObjectStore>);
    let dir = TempDir::new("shared");
    let sdk = FakeSdk::builder()
        .service("Echo", "now", vec![Step::output("\"ok\"")])
        .service(
            "Obj",
            "set",
            vec![
                Step::SetState {
                    key: "k".into(),
                    value: bytes::Bytes::from_static(b"\"v\""),
                },
                Step::output("\"ok\""),
            ],
        )
        .spawn()
        .await?;

    let a = start_on(&store, 2, Some(&dir)).await?;
    let res = common::within(Duration::from_secs(60), "load", async {
        common::register(&a.url(), &sdk.url).await?;
        for i in 0..60 {
            let key = format!("o{i}");
            let (code, body) = common::send(&a.url(), "Obj", "set", Some(&key), "{}").await?;
            ensure!(code == 200 || code == 202, "{code} {body}");
            let (code, body) = common::send(&a.url(), "Echo", "now", None, "{}").await?;
            ensure!(code == 200 || code == 202, "{code} {body}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        Ok(())
    })
    .await;
    a.shutdown().await;
    res?;
    let ranges = ranges_in(&dir.0);
    ensure!(ranges.len() >= 2, "cache has only ranges {ranges:?}");

    // The first turn after a leader change: with cache (B), then without (C).
    let mut gets = Vec::new();
    for cache in [Some(&dir), None] {
        let before = sst_gets(&store);
        let node = start_on(&store, 2, cache).await?;
        let url = node.url();
        let served = common::within(Duration::from_secs(30), "the first call", async {
            for i in 0..2 {
                let key = format!("o{i}");
                let (code, body) = common::invoke(&url, "Obj", "set", Some(&key), "{}").await?;
                ensure!(code == 200, "{code} {body}");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            if cache.is_some() {
                let m = get_text(&format!("{url}/metrics")).await?;
                let hits = m
                    .lines()
                    .find_map(|l| l.strip_prefix("cairnflow_disk_cache_hits_total "))
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .unwrap_or(0);
                ensure!(hits > 0, "no disk cache hit:\n{m}");
                let v = query(&url, "SELECT disk_cache_bytes FROM sys_node").await?;
                ensure!(
                    v["rows"][0][0].as_u64().is_some_and(|b| b > 0),
                    "sys_node: {v}"
                );
            }
            Ok(())
        })
        .await;
        gets.push(sst_gets(&store) - before);
        node.shutdown().await;
        served?;
    }
    let (warm, cold) = (gets[0], gets[1]);
    eprintln!("SST GETs to the store, open + first turn: with cache {warm}, without cache {cold}");
    ensure!(
        cold > 0,
        "no SST GET without cache — the measurement is meaningless"
    );
    ensure!(
        warm * 3 <= cold,
        "the disk cache does not cut GETs ≥ 3×: {warm} vs {cold}"
    );
    Ok(())
}
