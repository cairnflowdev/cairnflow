// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The cost of a partition **while idle**.
//!
//! Not a pass/fail test but a measurement, so it is `#[ignore]` and prints
//! numbers. Run by hand:
//!
//! ```sh
//! cargo test --release -p cairn-server --test scale_idle -- --ignored --nocapture
//! ```
//!
//! Each run builds a node `NodeMode::single(N)` with **the binary's defaults**
//! (background tick 200ms, flush tick 10ms) on `memory://` wrapped in
//! `FaultyStore` to count requests, leaves it idle a few seconds to settle, then
//! measures one window: requests/second by path kind, RSS, thread count, CPU.
//! `memory://` means the network cost or the S3 bill is **not** measured — only
//! how many requests the system issues and how many process resources it uses
//! when there is nothing to do.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::Result,
    cairn_server::node::{self, NodeConfig, NodeMode},
    cairn_store::{
        testkit::{FaultyStore, Op, PathClass},
        ObjectStores, StoreLocation,
    },
    slatedb::object_store::memory::InMemory,
    std::{process::Command, sync::Arc, time::Duration},
};

fn ps(field: &str) -> String {
    let pid = std::process::id().to_string();
    let out = Command::new("ps")
        .args(["-o", &format!("{field}="), "-p", &pid])
        .output()
        .expect("ps");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Thread count of the process (macOS: `ps -M`; Linux: `/proc/self/task`).
fn threads() -> usize {
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

/// CPU time spent, in seconds. `ps -o time=` prints `[[dd-]hh:]mm:ss.cc`.
fn cpu_secs() -> f64 {
    ps("time").split(':').fold(0.0, |acc, part| {
        acc * 60.0 + part.parse::<f64>().unwrap_or(0.0)
    })
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement, run by hand"]
async fn idle_cost_per_partition() -> Result<()> {
    const WARMUP: Duration = Duration::from_secs(3);
    const WINDOW: Duration = Duration::from_secs(10);
    println!(
        "\n  {:>5} | {:>8} | {:>8} | {:>9} | {:>8} | {:>9} | {:>7} | {:>6}",
        "N", "req/s", "wal/s", "manifest/s", "list/s", "RSS MiB", "threads", "CPU %"
    );
    for n in [16u16, 64, 256, 1024] {
        let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
        let stores =
            ObjectStores::single(StoreLocation::from_store(faulty.clone(), "", "memory://"));
        let node = node::start(NodeConfig::new(stores, NodeMode::single(n)))
            .await
            .map_err(|e| anyhow::anyhow!("node {n}: {e}"))?;
        tokio::time::sleep(WARMUP).await;

        let before = faulty.counts();
        let cpu0 = cpu_secs();
        tokio::time::sleep(WINDOW).await;
        let d = faulty.counts().since(&before);
        let cpu = (cpu_secs() - cpu0) / WINDOW.as_secs_f64() * 100.0;
        let secs = WINDOW.as_secs_f64();
        let rss_mib = ps("rss").parse::<f64>().unwrap_or(0.0) / 1024.0;
        println!(
            "  {:>5} | {:>8.1} | {:>8.1} | {:>9.1} | {:>8.1} | {:>9.0} | {:>7} | {:>6.1}",
            n,
            d.total() as f64 / secs,
            d.class(PathClass::Wal) as f64 / secs,
            d.class(PathClass::Manifest) as f64 / secs,
            d.op(Op::List) as f64 / secs,
            rss_mib,
            threads(),
            cpu,
        );
        if n == 16 {
            // Details for the smallest N: where the requests come from.
            for op in [Op::Get, Op::Head, Op::Put, Op::List, Op::Delete] {
                for class in [
                    PathClass::Wal,
                    PathClass::Manifest,
                    PathClass::Compacted,
                    PathClass::Lease,
                    PathClass::Other,
                ] {
                    let c = d.get(op, class);
                    if c > 0 {
                        println!(
                            "        {op:?} {class:?}: {:.2}/s per partition",
                            c as f64 / secs / n as f64
                        );
                    }
                }
            }
        }
        node.shutdown().await;
        // Let the engine threads of the node just shut down exit before the next run.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

/// Measure **idle requests by source**, for each set of SlateDB poll intervals
/// and both node modes. A measurement, run by hand:
///
/// ```sh
/// cargo test --release -p cairn-server --test scale_idle idle_requests_by_source -- --ignored --nocapture
/// ```
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement, run by hand"]
async fn idle_requests_by_source() -> Result<()> {
    use cairn_store::PollIntervals;
    const N: u16 = 16;
    const WARMUP: Duration = Duration::from_secs(5);
    let window = Duration::from_secs(
        std::env::var("IDLE_WINDOW_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30),
    );
    let s = Duration::from_secs;
    let variants = [
        ("slatedb", PollIntervals::SLATEDB),
        ("cairn", cairn_store::DEFAULT_POLLS),
        // Every interval divided by 720 (1 h → 5 s): the sources of long-term
        // cost show up within a few tens of seconds; multiply the measurement by
        // 1/720 to get the real cost.
        ("x720", cairn_store::DEFAULT_POLLS.scaled(720)),
        // cairn's defaults before F2 — kept for comparison.
        (
            "cairn-before-f2",
            PollIntervals {
                manifest: s(10),
                compactor: s(30),
                compaction_worker: s(30),
                ..cairn_store::DEFAULT_POLLS
            },
        ),
        (
            "60s",
            PollIntervals {
                manifest: s(60),
                compactor: s(60),
                compaction_worker: s(60),
                ..cairn_store::DEFAULT_POLLS
            },
        ),
        (
            "coord-only",
            PollIntervals {
                manifest: s(60),
                compactor: s(5),
                compaction_worker: s(60),
                ..cairn_store::DEFAULT_POLLS
            },
        ),
        (
            "worker-only",
            PollIntervals {
                manifest: s(60),
                compactor: s(60),
                compaction_worker: s(5),
                ..cairn_store::DEFAULT_POLLS
            },
        ),
    ];
    let only: Option<String> = std::env::var("IDLE_VARIANTS").ok();
    let modes: Vec<bool> = match std::env::var("IDLE_MODES").as_deref() {
        Ok("single") => vec![false],
        Ok("cluster") => vec![true],
        _ => vec![false, true],
    };
    for cluster in modes {
        for (name, polls) in variants {
            if only
                .as_deref()
                .is_some_and(|o| !o.split(',').any(|v| v == name))
            {
                continue;
            }
            let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
            let stores =
                ObjectStores::single(StoreLocation::from_store(faulty.clone(), "", "memory://"))
                    .with_polls(polls);
            let mode = if cluster {
                NodeMode {
                    partitions: N,
                    lease_ttl: Duration::from_secs(10),
                    node_id: Some("idle".into()),
                }
            } else {
                NodeMode::single(N)
            };
            let node = node::start(NodeConfig::new(stores, mode))
                .await
                .map_err(|e| anyhow::anyhow!("{name}: {e}"))?;
            tokio::time::sleep(WARMUP).await;
            let before = faulty.counts();
            tokio::time::sleep(window).await;
            let d = faulty.counts().since(&before);
            let per = |c: u64| c as f64 / window.as_secs_f64() / N as f64;
            let mut line = format!(
                "{:>7} {:>8}: {:6.3} req/s/partition |",
                if cluster { "cluster" } else { "1 process" },
                name,
                per(d.total())
            );
            for op in [Op::Get, Op::Head, Op::Put, Op::List, Op::Delete] {
                for class in PathClass::ALL {
                    let c = d.get(op, class);
                    if c > 0 {
                        line.push_str(&format!(" {op:?}/{class:?}={:.3}", per(c)));
                    }
                }
            }
            println!("{line}");
            node.shutdown().await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    Ok(())
}

/// The **lease layer** cost of an idle cluster, by node count × partition
/// count, at the default TTL of 10s. A measurement, run by hand:
///
/// ```sh
/// cargo test --release -p cairn-server --test scale_idle idle_cluster_by_nodes -- --ignored --nocapture
/// ```
///
/// Per-node leases (D20) promise: the "per partition" column ≈ 0, and the total
/// only grows with the node count.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement, run by hand"]
async fn idle_cluster_by_nodes() -> Result<()> {
    let window = Duration::from_secs(
        std::env::var("IDLE_WINDOW_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30),
    );
    let ttl = Duration::from_secs(10);
    println!(
        "\n  {:>5} | {:>4} | {:>8} | {:>7} | {:>7} | {:>7} | {:>14}",
        "node", "N", "lease/s", "PUT/s", "GET/s", "LIST/s", "/s per partition"
    );
    for nodes in [1usize, 2, 3] {
        for n in [16u16, 64] {
            let ids: Vec<String> = (0..nodes).map(|i| format!("do-{i}")).collect();
            let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
            let (_bucket, cluster) = common::cluster_nodes(&refs, n, ttl).await?;
            let probe: Vec<_> = cluster.iter().collect();
            common::wait_probe(
                &probe,
                Duration::from_secs(60),
                "every partition has a leader",
                |p| common::single_leaders(p).is_some(),
            )
            .await?;
            tokio::time::sleep(Duration::from_secs(5)).await;
            let before: Vec<_> = cluster.iter().map(|c| c.store.counts()).collect();
            tokio::time::sleep(window).await;
            let (mut put, mut get, mut list) = (0u64, 0u64, 0u64);
            for (c, b) in cluster.iter().zip(&before) {
                let d = c.store.counts().since(b);
                put += d.get(Op::Put, PathClass::Lease);
                get += d.get(Op::Get, PathClass::Lease) + d.get(Op::Head, PathClass::Lease);
                list += d.get(Op::List, PathClass::Lease);
            }
            let secs = window.as_secs_f64();
            let total = (put + get + list) as f64 / secs;
            println!(
                "  {:>5} | {:>4} | {:>8.2} | {:>7.2} | {:>7.2} | {:>7.2} | {:>14.3}",
                nodes,
                n,
                total,
                put as f64 / secs,
                get as f64 / secs,
                list as f64 / secs,
                total / f64::from(n)
            );
            for c in cluster {
                c.node.shutdown().await;
            }
        }
    }
    Ok(())
}

/// Source groups of the idle-cost table: scrubber, D-d, `owner_watch`, rule
/// book, SlateDB, shared WAL, session/lease, the rest.
fn source_group(src: &str) -> &'static str {
    match src {
        "scrub" => "scrubber",
        "gc.dbs" | "gc.orphans" => "D-d/orphans",
        "session.owner_watch" => "owner_watch",
        "rules" => "rule book",
        s if s.starts_with("slatedb.") => "SlateDB",
        s if s.starts_with("nodewal.") => "shared WAL",
        s if s.starts_with("session.") || s == "range.claim" => "session/lease",
        _ => "other",
    }
}

/// S3 billing class: A = PUT/LIST/COPY, B = GET/HEAD; DELETE is free.
fn op_class(op: Op) -> Option<usize> {
    match op {
        Op::Put | Op::PutMultipart | Op::List | Op::Copy | Op::Rename => Some(0),
        Op::Get | Op::Head => Some(1),
        Op::Delete => None,
    }
}

/// `(group, class) → request count` from `counts_by_source`.
fn by_group(store: &FaultyStore) -> std::collections::BTreeMap<(&'static str, usize), u64> {
    let mut out = std::collections::BTreeMap::new();
    for (src, op, _, c) in store.counts_by_source() {
        if let Some(class) = op_class(op) {
            *out.entry((source_group(src), class)).or_insert(0) += c;
        }
    }
    out
}

/// Like `idle_many_ranges::first_scrub_done`, with a longer limit for 1 024 ranges.
async fn first_scrub_done(store: &FaultyStore) -> Result<()> {
    let scrub = || -> u64 {
        store
            .counts_by_source()
            .into_iter()
            .filter(|(src, ..)| *src == "scrub")
            .map(|(.., c)| c)
            .sum()
    };
    let t0 = std::time::Instant::now();
    let mut last = scrub();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let now = scrub();
        if now > 0 && now == last {
            return Ok(());
        }
        anyhow::ensure!(
            t0.elapsed() < Duration::from_secs(600),
            "first scrub round not done after 600 s ({now} requests)"
        );
        last = now;
    }
}

/// **Idle cost by range count, by source** (the baseline for the idle cost per
/// range). A measurement, run by hand:
///
/// ```sh
/// IDLE_RANGES=64,256,1024 IDLE_WINDOW_S=120 \
///   cargo test --release -p cairn-server --test scale_idle \
///   idle_cost_by_source_per_range_count -- --ignored --nocapture
/// ```
///
/// For each N: a node `NodeMode::single(N)` with the binary's defaults on
/// `memory://` wrapped in `FaultyStore`. Wait until the scrubber's first round
/// is done (that round and the Db opens are a one-time cost, printed separately
/// in the "first" columns), then count requests by source
/// ([`cairn_store::io_source`]) over `IDLE_WINDOW_S` (default 120 s: covers
/// `owner_watch` 60 s and the rule book 30 s), scaled to per hour, by class A
/// (PUT/LIST) and B (GET/HEAD). Loops longer than the window (scrubber 600 s,
/// D-d 600 s) do not fall into the window: their "est/h" line = the cost of one
/// round (measured on the first round) × 3600 / interval. Also prints RSS,
/// thread count and live task count of the main runtime (engine threads have
/// their own runtime — not counted here).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement, run by hand"]
async fn idle_cost_by_source_per_range_count() -> Result<()> {
    let ns: Vec<u16> = std::env::var("IDLE_RANGES")
        .ok()
        .map(|v| v.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![64, 256, 1024]);
    let window = Duration::from_secs(
        std::env::var("IDLE_WINDOW_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(120),
    );
    let per_hour = |c: u64| c as f64 * 3600.0 / window.as_secs_f64();
    for n in ns {
        let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
        let stores =
            ObjectStores::single(StoreLocation::from_store(faulty.clone(), "", "memory://"));
        let t0 = std::time::Instant::now();
        let node = node::start(NodeConfig::new(stores, NodeMode::single(n)))
            .await
            .map_err(|e| anyhow::anyhow!("node {n}: {e}"))?;
        // `IDLE_DEPLOY=1`: register a deployment (idle, nobody calls it).
        // Without a deployment the range background tick does not run
        // (`spawn_background_tick` skips it when the registry is empty), and with
        // it the rule book poll loop — a real cell always has one.
        let _sdk = if std::env::var("IDLE_DEPLOY").is_ok_and(|v| v == "1") {
            let sdk = common::fake_sdk::FakeSdk::builder()
                .service("Echo", "run", vec![common::fake_sdk::Step::output("ok")])
                .spawn()
                .await?;
            common::register(&node.url(), &sdk.url).await?;
            Some(sdk)
        } else {
            None
        };
        first_scrub_done(&faulty).await?;
        let warm = t0.elapsed();
        let first = by_group(&faulty);
        tokio::time::sleep(Duration::from_secs(2)).await;
        let c0 = by_group(&faulty);
        tokio::time::sleep(window).await;
        let c1 = by_group(&faulty);
        let rss_mib = ps("rss").parse::<f64>().unwrap_or(0.0) / 1024.0;
        let tasks = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        println!(
            "\n  N = {n}: first (Db open + first scrub) {:.0} s, RSS {rss_mib:.0} MiB, {} threads, {tasks} live tasks (main runtime)",
            warm.as_secs_f64(),
            threads()
        );
        println!(
            "  {:>14} | {:>10} | {:>10} | {:>11} | {:>11} | {:>13} | {:>13}",
            "source", "A/h", "B/h", "A/h/range", "B/h/range", "first A", "first B"
        );
        let groups: std::collections::BTreeSet<&str> =
            c1.keys().chain(first.keys()).map(|(g, _)| *g).collect();
        let (mut ta, mut tb) = (0.0, 0.0);
        for g in groups {
            let get = |m: &std::collections::BTreeMap<(&str, usize), u64>, k: usize| {
                m.get(&(g, k)).copied().unwrap_or(0)
            };
            let a = per_hour(get(&c1, 0) - get(&c0, 0));
            let b = per_hour(get(&c1, 1) - get(&c0, 1));
            ta += a;
            tb += b;
            println!(
                "  {:>14} | {:>10.0} | {:>10.0} | {:>11.2} | {:>11.2} | {:>13} | {:>13}",
                g,
                a,
                b,
                a / f64::from(n),
                b / f64::from(n),
                get(&first, 0),
                get(&first, 1),
            );
        }
        println!(
            "  {:>14} | {:>10.0} | {:>10.0} | {:>11.2} | {:>11.2} |",
            "total (window)",
            ta,
            tb,
            ta / f64::from(n),
            tb / f64::from(n),
        );
        // Loops longer than the window: the cost of one round (first round) scaled to per hour.
        for (g, every) in [
            ("scrubber", cairn_server::scrub::DEFAULT_EVERY),
            ("D-d/orphans", cairn_server::gc::DEFAULT_EVERY),
        ] {
            let k = 3600.0 / every.as_secs_f64();
            let a = first.get(&(g, 0)).copied().unwrap_or(0) as f64 * k;
            let b = first.get(&(g, 1)).copied().unwrap_or(0) as f64 * k;
            println!(
                "  {:>14} | {:>10.0} | {:>10.0} | {:>11.2} | {:>11.2} | est/h: first round × 3600/{} s",
                g,
                a,
                b,
                a / f64::from(n),
                b / f64::from(n),
                every.as_secs()
            );
        }
        node.shutdown().await;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    Ok(())
}
