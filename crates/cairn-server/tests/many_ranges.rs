// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Many ranges on one node run on a **fixed thread pool**.
//!
//! One OS thread per range is a practical cap on ranges per node (measured: a node with
//! 1024 idle partitions had 1038 threads). The engine is still `!Send` and lives entirely
//! on one thread, but one thread runs the lifecycle of many ranges.
//!
//! This file has only **one** test, on purpose: the thread count belongs to the whole
//! process, so another test running alongside in the same binary would be counted in.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::{bail, Result},
    cairn_server::node::{self, NodeConfig, NodeMode},
    cairn_store::{ObjectStores, StoreLocation},
    slatedb::object_store::memory::InMemory,
    std::{process::Command, sync::Arc, time::Duration},
};

/// The process's OS thread count (Linux: `/proc/self/task`; macOS: `ps -M`).
fn os_threads() -> usize {
    if let Ok(rd) = std::fs::read_dir("/proc/self/task") {
        return rd.count();
    }
    let pid = std::process::id().to_string();
    let out = Command::new("ps")
        .args(["-M", "-p", &pid])
        .output()
        .expect("ps -M");
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .count()
        .saturating_sub(1)
}

/// A 256-range node ⇒ the OS thread count grows by ≤ pool + a constant.
///
/// The default pool = the core count. The constant 16 leaves room for threads outside
/// the pool — the process's background compaction/GC runtime (cores/4) and tokio's
/// temporary blocking threads; it does not depend on the range count. Blocking threads
/// live 10 s past their last use, so the test waits up to 20 s for the count to settle.
/// Before the pool: one thread per range ⇒ ≥ 256.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_ranges_bounded_threads() -> Result<()> {
    const RANGES: u16 = 256;
    const SLACK: usize = 16;
    let pool = std::thread::available_parallelism().map_or(1, |n| n.get());

    let before = os_threads();
    let stores = ObjectStores::single(StoreLocation::from_store(
        Arc::new(InMemory::new()),
        "",
        "memory://",
    ));
    let node = node::start(NodeConfig::new(stores, NodeMode::single(RANGES)))
        .await
        .map_err(|e| anyhow::anyhow!("build node: {e}"))?;

    let bound = before + pool + SLACK;
    let t0 = std::time::Instant::now();
    let mut now = os_threads();
    while now > bound {
        if t0.elapsed() > Duration::from_secs(20) {
            node.shutdown().await;
            bail!(
                "node with {RANGES} ranges has {now} OS threads (before build: {before}, cap: \
                 {before} + pool {pool} + {SLACK} = {bound})"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        now = os_threads();
    }
    println!("{RANGES} ranges: {before} → {now} OS threads (pool {pool}, cap {bound})");
    node.shutdown().await;
    Ok(())
}
