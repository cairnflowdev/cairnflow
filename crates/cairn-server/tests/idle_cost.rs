// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The cost of an **idle** partition is capped.
//!
//! Measured on an earlier scale run: every idle partition used to cost ~3.1 requests/s
//! just from SlateDB's default poll cadence (manifest every 1s, compactor and worker
//! every 5s); event-driven compaction brought it close to 0. In a cluster,
//! the lease layer used to add one renewal **per partition**, and every follower read
//! the lease of each partition it did not hold — cost grew with partitions ×
//! nodes. Per-node leases (D20) made that part depend only on the
//! node count. The tests here cap both, with margins wide enough not to flake
//! with the phase of the poll loops.
//!
//! The end of the file caps the requests **per source**
//! (`cairn_store::io_source`) of an idle cluster: renewal, peer observation, registry
//! polling, quarantine, owner watch, orphan-child cleanup — see [`CAPS_3`].

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{ensure, Result},
    cairn_server::node::{self, NodeConfig, NodeMode},
    cairn_store::{
        testkit::{FaultyStore, Op, PathClass},
        ObjectStores, StoreLocation,
    },
    slatedb::object_store::memory::InMemory,
    std::{sync::Arc, time::Duration},
};

const N: u16 = 4;
const WARMUP: Duration = Duration::from_secs(3);
const WINDOW: Duration = Duration::from_secs(10);

/// Waits for the state scrubber's first round to finish: it runs within
/// [`cairn_server::gc::RUNNER_CHECK_EVERY`] after the node becomes leader
/// (like every range safety net, it fires first when the Db opens) and reads every
/// range; the next round comes a full period (600 s) later — outside the measuring
/// window. Done = there are `scrub` source requests and no more arrive within 500 ms.
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
        tokio::time::sleep(Duration::from_millis(500)).await;
        let now = scrub();
        if now > 0 && now == last {
            return Ok(());
        }
        ensure!(
            t0.elapsed() < Duration::from_secs(60),
            "first scrub round not done after 60 s ({now} requests)"
        );
        last = now;
    }
}

/// Builds a `mode` node on a request-counting store, leaves it idle, then returns
/// the counts within the measuring window.
async fn idle_counts(mode: NodeMode) -> Result<cairn_store::testkit::StoreCounts> {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let stores = ObjectStores::single(StoreLocation::from_store(faulty.clone(), "", "memory://"));
    let node = node::start(NodeConfig::new(stores, mode))
        .await
        .map_err(|e| anyhow::anyhow!("build node: {e}"))?;
    tokio::time::sleep(WARMUP).await;
    first_scrub_done(&faulty).await?;
    let before = faulty.counts();
    tokio::time::sleep(WINDOW).await;
    let d = faulty.counts().since(&before);
    node.shutdown().await;
    Ok(d)
}

fn per_partition_per_s(c: u64) -> f64 {
    c as f64 / WINDOW.as_secs_f64() / f64::from(N)
}

/// An idle range **issues no request** within the measuring window.
///
/// **Marginal cost**: two nodes side by side, `N` and `N + 16` ranges, same window; the
/// difference divided by 16. The node's own share (LIST of the deployment registry every
/// 5 s, ~0.05 requests/s per range when divided over 4 ranges) does not scale with ranges
/// and cancels out — it was the only thing that forced the old cap (0.1/s spread evenly)
/// to be wide. Every range safety net fires first when the Db opens (the scrubber: at
/// leadership, [`first_scrub_done`]), and next at the earliest after 60 s, so within
/// the 10 s window after that the expectation is
/// **0**; the cap of 0.01 requests/s per range leaves room for late start-up work. The
/// long-run cost includes those nets: `idle_many_ranges.rs::idle_marginal_cost_per_range_long_run_is_near_zero`.
#[tokio::test(flavor = "multi_thread")]
async fn idle_single_process_partition_sends_almost_nothing() -> Result<()> {
    const MORE: u16 = 16;
    let (small, big) = tokio::join!(
        idle_counts(NodeMode::single(N)),
        idle_counts(NodeMode::single(N + MORE))
    );
    let (small, big) = (small?, big?);
    let marginal = |c: fn(&cairn_store::testkit::StoreCounts) -> u64| {
        (c(&big) as f64 - c(&small) as f64) / WINDOW.as_secs_f64() / f64::from(MORE)
    };
    let rate = marginal(|d| d.total());
    ensure!(
        rate <= 0.01,
        "marginal cost of an idle range {rate:.4} requests/s (cap 0.01): manifest {:.4}, other {:.4}",
        marginal(|d| d.class(PathClass::Manifest)),
        marginal(|d| d.class(PathClass::Other)),
    );
    Ok(())
}

/// **An idle single-node cluster, default TTL: the marginal cost of a range is ~0**
/// (a node without `--cluster` is about to take exactly this shape). As in the test
/// above, a 10 s window after warm-up and the first scrub round, two nodes with `N`
/// and `N + 16` ranges.
///
/// 1. Each node claims all its ranges (owner record: `Create`), then the keeper
///    rewrites the record (shared-WAL segment, `data=` of the range-tier migration step).
/// 2. The owner-record watch (layer 3 of LS-6) runs only on the claimer's notice and
///    on a 10-minute safety net — not once within the
///    window.
///
/// Must see: no per-range request. Formerly the watch GET every record again in its
/// first round after every claim (0.1 requests/s per range in this window); later it
/// LISTed `owners/` every TTL and compared version tags remembered from the keeper's
/// own writes. The conditional-GET safety net compares against the same tag, so when
/// it runs it is almost all `304`.
#[tokio::test(flavor = "multi_thread")]
async fn idle_one_node_cluster_range_sends_almost_nothing() -> Result<()> {
    const MORE: u16 = 16;
    let mode = |partitions| NodeMode {
        partitions,
        lease_ttl: Duration::from_secs(10),
        node_id: None,
    };
    let (small, big) = tokio::join!(idle_counts(mode(N)), idle_counts(mode(N + MORE)));
    let (small, big) = (small?, big?);
    let marginal = |c: fn(&cairn_store::testkit::StoreCounts) -> u64| {
        (c(&big) as f64 - c(&small) as f64) / WINDOW.as_secs_f64() / f64::from(MORE)
    };
    let rate = marginal(|d| d.total());
    ensure!(
        rate <= 0.01,
        "marginal cost of an idle range {rate:.4} requests/s (cap 0.01): lease {:.4}, manifest {:.4}, \
         other {:.4}",
        marginal(|d| d.class(PathClass::Lease)),
        marginal(|d| d.class(PathClass::Manifest)),
        marginal(|d| d.class(PathClass::Other)),
    );
    Ok(())
}

/// GETs of the rule book (`rules`) by a `mode` node from start-up to the end of the
/// measuring window (warm-up, first scrub round, [`WINDOW`]).
async fn rules_gets_from_start(mode: NodeMode) -> Result<u64> {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let stores = ObjectStores::single(StoreLocation::from_store(faulty.clone(), "", "memory://"));
    let node = node::start(NodeConfig::new(stores, mode))
        .await
        .map_err(|e| anyhow::anyhow!("build node: {e}"))?;
    tokio::time::sleep(WARMUP).await;
    first_scrub_done(&faulty).await?;
    tokio::time::sleep(WINDOW).await;
    let gets = faulty
        .counts_by_source_object()
        .into_iter()
        .filter(|(_, op, _, kind, _)| *op == Op::Get && *kind == "rules")
        .map(|(.., n)| n)
        .sum();
    node.shutdown().await;
    Ok(gets)
}

/// **The rule book is read once per node, not per range.**
///
/// Before: every range GET `rules` itself on becoming leader and every
/// `RULES_POLL_INTERVAL` (30 s) — a 256-range node made 256 GETs per tick for something
/// that changes a few times a day.
///
/// 1. Two nodes side by side, `N` and `N + 16` ranges, nobody sets a rule.
/// 2. Count each node's `rules` GETs from start-up to the end of the measuring window
///    (~15 s: the read on becoming leader of every range falls inside it).
///
/// Must see: the bigger node does not GET more (tolerance of one out-of-phase poll
/// round); no node makes more than two GETs. Before the fix: the bigger node made 16 more.
#[tokio::test(flavor = "multi_thread")]
async fn idle_rule_book_reads_do_not_scale_with_ranges() -> Result<()> {
    const MORE: u16 = 16;
    let (small, big) = tokio::join!(
        rules_gets_from_start(NodeMode::single(N)),
        rules_gets_from_start(NodeMode::single(N + MORE))
    );
    let (small, big) = (small?, big?);
    ensure!(
        big <= small + 1 && big <= 2,
        "rule book GETs grow with ranges: {small} with {N} ranges, {big} with {} ranges",
        N + MORE
    );
    Ok(())
}

/// An idle cluster: the routing table reads leases **once per TTL**, not once per TTL/5.
///
/// The lease write (renewal) is not capped here — its cadence is a trade-off against
/// failover time, not waste.
#[tokio::test(flavor = "multi_thread")]
async fn idle_cluster_reads_leases_once_per_ttl() -> Result<()> {
    let ttl = Duration::from_secs(2);
    let d = idle_counts(NodeMode {
        partitions: N,
        lease_ttl: ttl,
        node_id: Some("idle".into()),
    })
    .await?;
    let reads = per_partition_per_s(d.get(Op::Get, PathClass::Lease));
    // One read per TTL = 0.5/s with TTL 2s; the cap leaves room for one on-demand
    // refresh.
    ensure!(
        reads <= 1.0,
        "lease reads {reads:.2}/s per partition while idle (cap 1.0 with TTL {ttl:?})"
    );
    Ok(())
}

/// The lease layer's **writes** while idle must not grow with the partition count.
///
/// Per-partition leases renew partition by partition: 16 partitions at TTL 2s is
/// ~40 PUT/s. Per-node leases (D20) renew one session for the whole node.
#[tokio::test(flavor = "multi_thread")]
async fn idle_cluster_lease_writes_do_not_scale_with_partitions() -> Result<()> {
    let n = common::cluster_nodes(&["idle-writes"], 16, Duration::from_secs(2)).await?;
    let (_bucket, nodes) = n;
    let node = &nodes[0];
    tokio::time::sleep(Duration::from_secs(4)).await;
    let before = node.store.counts();
    tokio::time::sleep(WINDOW).await;
    let d = node.store.counts().since(&before);
    let puts = d.get(Op::Put, PathClass::Lease) as f64 / WINDOW.as_secs_f64();
    ensure!(
        puts <= 5.0,
        "lease layer writes {puts:.1} PUT/s with 16 idle partitions (cap 5)"
    );
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// The lease layer's **reads** while idle, in a two-node cluster, must not grow with
/// the partition count.
///
/// A follower used to ask for the lease of every partition it did not hold every TTL/5,
/// plus the routing table reading every lease every TTL — ~60 requests/s for 16 partitions.
#[tokio::test(flavor = "multi_thread")]
async fn idle_two_node_cluster_lease_reads_do_not_scale_with_partitions() -> Result<()> {
    let ttl = Duration::from_secs(2);
    let (_bucket, nodes) = common::cluster_nodes(&["idle-a", "idle-b"], 16, ttl).await?;
    let refs: Vec<_> = nodes.iter().collect();
    // The first node takes all 16 then releases them one by one, every ~2·TTL (the
    // release loop is slow on purpose) — so waiting for an even split takes about half a minute.
    common::wait_probe(&refs, Duration::from_secs(90), "split 8/8", |p| {
        common::single_leaders(p).is_some_and(|m| common::load_of(&m, 2) == [8, 8])
    })
    .await?;
    tokio::time::sleep(ttl).await;
    let before: Vec<_> = nodes.iter().map(|n| n.store.counts()).collect();
    tokio::time::sleep(WINDOW).await;
    let mut reads = 0;
    for (n, b) in nodes.iter().zip(&before) {
        let d = n.store.counts().since(b);
        reads += d.get(Op::Get, PathClass::Lease)
            + d.get(Op::Head, PathClass::Lease)
            + d.get(Op::List, PathClass::Lease);
    }
    let rate = reads as f64 / WINDOW.as_secs_f64();
    ensure!(
        rate <= 10.0,
        "lease layer reads {rate:.1} requests/s on a 2-node × 16-idle-partition cluster (cap 10)"
    );
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

// ─── **Idle** cost caps per source ───
//
// A cluster without load still reads and writes the bucket: session renewal, peer
// observation, registry polling, quarantine, owner watch, orphan-child cleanup. A
// measuring run on SeaweedFS showed that this is almost the whole request cost of an
// idle cluster. This test pins the request count of each source
// (`cairn_store::io_source`) in an idle window, so that a change that accidentally
// polls more often fails, and so that each change that cuts a source proves its
// saving with numbers.
//
// **Every later `fix:` lowers the cap of the source it fixes, in the same commit**
// (as happened for `gc.orphans`, `quarantine`, `node.refresh`,
// `session.owner_watch`, `session.tracker`, `cluster.follow`, the new
// `control.*` caps, and `session.renew`).
// A cap above the measured count is tolerance, not room for another source to grow.
//
// **Unit: occurrences per TTL, whole cluster.** The test runs the binary's default TTL
// (10 s) and every default cadence (quarantine 30 s, registry 30 s, orphan net 1 hour),
// not shortened: the ratio between fixed-seconds cadences and TTL cadences is what the
// caps measure, and any `fix:` that changes a default shows up at once. The price: one
// run takes ~`(K + 2)·TTL`.
//
// **Cap** = `⌈1.2 · measured per TTL · K⌉ + one round`: today's count plus 20%, plus
// one round of that source for phase skew (a `K·TTL` window cuts the registry's 30 s
// loop into 2 or 3 rounds depending on phase). The per-TTL numbers in [`CAPS_3`] and
// [`CAPS_1`] are the formula of today's cadence (commented per line); three runs on a
// debug build (two `cargo test`, one nextest) measured at or below it —
// `quarantine` 5.5 (phase skew), `session.owner_watch` 2.3 (one node did not hold a
// range for the whole window: ranges were still being split up after the last node
// joined). The test prints a table of every source (`--nocapture`), including sources
// without a cap.
//
// The counting store has no injected faults (`CAIRNFLOW_TEST_FAULTS` is ignored): an
// ambiguous write that is read back is an extra request, not idle cost.

/// The binary's default TTL (`--lease-ttl-ms 10000`).
const TTL: Duration = Duration::from_secs(10);
/// Length of the measuring window, in TTLs.
const K: u32 = 6;
/// The cluster's range count.
const PARTITIONS: u16 = 4;

/// The cap of one (source, op).
struct Cap {
    source: &'static str,
    op: Op,
    /// Today's measurement, per TTL, whole cluster.
    per_ttl: f64,
    /// One round of the source, whole cluster: tolerance for phase skew.
    round: u64,
}

impl Cap {
    fn limit(&self) -> u64 {
        (self.per_ttl * f64::from(K) * 1.2).ceil() as u64 + self.round
    }
}

/// A 3-node cluster × [`PARTITIONS`] ranges.
const CAPS_3: &[Cap] = &[
    // With the gate on and delegation on: nobody PUTs a renewal — the two delegated
    // nodes renew by ack, the coordinator in `solo` (every other node delegates to
    // it) renews by self-check GET. What remains is the `owns` CAS of ranges still being
    // split up within the window (keeping the mode) and the coordinator's CAS into
    // `solo` — leaving 8 (measured: 4). Before the coordinator went solo: only the
    // coordinator PUT, every TTL/5 (5/TTL). Before delegation: every node every TTL/5 (15/TTL).
    Cap {
        source: "session.renew",
        op: Op::Put,
        per_ttl: 0.0,
        round: 8,
    },
    // The self-check GET of **every** node every TTL/5 (3 × 5/TTL, `If-None-Match`, almost
    // always `304` — Class B): the bucket-reachability proof at delegated nodes, the
    // `solo` renewal at the coordinator; plus one gate read every 60 s. Before the
    // coordinator went solo: 10/TTL (the coordinator renewed by PUT). Before delegation: 0.
    Cap {
        source: "session.renew",
        op: Op::Get,
        per_ttl: 15.0,
        round: 3,
    },
    // The coordinator does not GET the sessions delegated to it while heartbeats
    // keep arriving (τ is when the heartbeat was received); a node listening to `view`
    // keeps a sparse tracker — each peer every `SPARSE_AFTER_TTLS` = 2 TTL (2 × 2 / 2 =
    // 2/TTL; formerly 6·TTL, ≈ 0.7/TTL); leave one sparse round (4 GETs). LIST `sessions/`
    // only at the coordinator, every TTL (a session without heartbeats reaches `view`
    // only through it; even while it is in `solo`: the runtime gate rule — delegate only
    // while every session in `view` knows the gate step — relies on that LIST); a node
    // listening to `view` has a 10-minute safety net. Formerly the coordinator GET each
    // peer every TTL/3 (7/TTL for the cluster); before that every node GET each peer every
    // TTL/3 (O(N²), 18/TTL), LIST every TTL (3/TTL).
    Cap {
        source: "session.tracker",
        op: Op::Get,
        per_ttl: 2.0,
        round: 4,
    },
    Cap {
        source: "session.tracker",
        op: Op::List,
        per_ttl: 1.0,
        round: 3,
    },
    // Each node makes one GET of `control/registry-gen` every 30 s (almost
    // always 304); LIST only in the 10-minute reconciliation — outside the window — so
    // there is no LIST at all, leaving a first round per node. Before: LIST every 5 s.
    Cap {
        source: "quarantine",
        op: Op::List,
        per_ttl: 0.0,
        round: 3,
    },
    // Only the coordinator GETs the registry generation every 30 s; a node listening to
    // `view` compares against the generation in it. Before: every node.
    Cap {
        source: "quarantine",
        op: Op::Get,
        per_ttl: 0.34,
        round: 1,
    },
    // As above. Formerly every node made 3 LISTs + 3 GETs every 30 s. Now a
    // listening node's `RefreshLoop` runs only when the generation in `view` differs from
    // the applied one — GETs remain only at the coordinator.
    Cap {
        source: "node.refresh",
        op: Op::List,
        per_ttl: 0.0,
        round: 3,
    },
    Cap {
        source: "node.refresh",
        op: Op::Get,
        per_ttl: 0.34,
        round: 1,
    },
    // Only the coordinator GETs `cluster.conf` every TTL; a node listening to `view`
    // compares the table version in it. Before: every node every TTL.
    Cap {
        source: "cluster.follow",
        op: Op::Get,
        per_ttl: 1.0,
        round: 1,
    },
    // The coordinator fence — two GETs per batch, one batch per tick (TTL/5)
    // thanks to the `next_in_ms` tick net, regardless of node count. Almost all `304`.
    Cap {
        source: "control.heartbeat",
        op: Op::Get,
        per_ttl: 10.0,
        round: 4,
    },
    // A node listening to `view` does not read `control/coordinator` while its ack is
    // fresh; the coordinator fences only per batch (source above). Leave room for a role claim.
    Cap {
        source: "control.coordinator",
        op: Op::Get,
        per_ttl: 0.0,
        round: 6,
    },
    // The orphan-child cleanup runs on nudges, with a one-hour net (0.003/TTL).
    // Each round is two LISTs (`owners/`, `ranges/`; range directories in the table are
    // no longer LISTed one by one): leave two rounds — the runner changes while ranges
    // are still being split up within the window (nudge (c)). Before: (2 + range dirs) LISTs every 15 s.
    Cap {
        source: "gc.orphans",
        op: Op::List,
        per_ttl: 0.0,
        round: 4,
    },
    // The owner-record watch no longer LISTs; the net is one conditional GET per
    // range (the fast path is the claimer's notice, and the coordinator's `view`;
    // a range re-split within the window goes through a yield, with no notice).
    // The 1-minute net = 6·TTL: one round (`PARTITIONS` GETs for the cluster) every
    // 6 TTL, leaving one round for phase skew (measured: 4 in the window). Before:
    // every node holding ranges LISTed `owners/` every TTL; later a 10-minute net
    // (outside the window).
    Cap {
        source: "session.owner_watch",
        op: Op::List,
        per_ttl: 0.0,
        round: 0,
    },
    Cap {
        source: "session.owner_watch",
        op: Op::Get,
        per_ttl: PARTITIONS as f64 / 6.0,
        round: PARTITIONS as u64,
    },
];

/// A lone node × [`PARTITIONS`] ranges, quiet-lease gate **on**.
const CAPS_1: &[Cap] = &[
    // A lone node in `mode=solo` renews by self-check GET (Class B,
    // almost always 304) — no PUT within the window. Before: one PUT every TTL/5.
    Cap {
        source: "session.renew",
        op: Op::Put,
        per_ttl: 0.0,
        round: 0,
    },
    // No LIST of `sessions/` every TTL; the 10-minute net falls outside the window.
    // Before: one LIST every TTL.
    Cap {
        source: "session.tracker",
        op: Op::List,
        per_ttl: 0.0,
        round: 0,
    },
    // An idle coordinator fences every 6·TTL (two GETs).
    Cap {
        source: "control.coordinator",
        op: Op::Get,
        per_ttl: 0.34,
        round: 4,
    },
    Cap {
        source: "quarantine",
        op: Op::List,
        per_ttl: 0.0,
        round: 1,
    },
    Cap {
        source: "quarantine",
        op: Op::Get,
        per_ttl: 0.34,
        round: 1,
    },
    Cap {
        source: "node.refresh",
        op: Op::List,
        per_ttl: 0.0,
        round: 3,
    },
    Cap {
        source: "node.refresh",
        op: Op::Get,
        per_ttl: 0.34,
        round: 1,
    },
    // As in the 3-node cluster; one node, becomes the runner once.
    Cap {
        source: "gc.orphans",
        op: Op::List,
        per_ttl: 0.0,
        round: 2,
    },
    // As in the 3-node cluster.
    Cap {
        source: "session.owner_watch",
        op: Op::List,
        per_ttl: 0.0,
        round: 0,
    },
    Cap {
        source: "session.owner_watch",
        op: Op::Get,
        per_ttl: PARTITIONS as f64 / 6.0,
        round: PARTITIONS as u64,
    },
];

/// Requests per (source, op — the `Debug` name of [`Op`]).
type Tally = std::collections::BTreeMap<(&'static str, String), u64>;

fn tally(nodes: &[common::ClusterNode]) -> Tally {
    let mut out = Tally::new();
    for n in nodes {
        for (src, op, _, count) in n.store.counts_by_source() {
            *out.entry((src, format!("{op:?}"))).or_default() += count;
        }
    }
    out
}

/// Builds a cluster of `ids` on a fresh bucket, waits for it to settle, then counts
/// requests per (source, op) over `K·TTL` of idleness.
async fn idle_window(
    ids: &[&str],
    quiet_lease: bool,
    register: bool,
) -> Result<(common::Bucket, Vec<common::ClusterNode>, Tally)> {
    let bucket = common::bucket();
    if quiet_lease {
        // Like `cairn-ctl cluster enable-quiet-lease --apply` on a fresh bucket:
        // every binary of the test knows the gate step.
        let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
        let on = cairn_store::quiet_lease::QuietLease {
            on: true,
            knows: cairn_store::migrate::QUIET_LEASE,
            by: "idle_cost".into(),
            at_ms: 0,
            delegation: true,
            ..Default::default()
        };
        ensure!(
            cairn_store::quiet_lease::write(&store, &Default::default(), &on, None).await?,
            "could not write the gate"
        );
    }
    let mut nodes = Vec::new();
    for id in ids {
        // Not through `start_cluster_node`: the counting store does not carry the
        // faults of `CAIRNFLOW_TEST_FAULTS`.
        let store = FaultyStore::wrap(bucket.clone());
        let node = common::start_cluster_node_on(&store, id, PARTITIONS, TTL, |_| {}).await?;
        nodes.push(common::ClusterNode { node, store });
    }
    let refs: Vec<_> = nodes.iter().collect();
    common::wait_probe(&refs, 6 * TTL, "one leader per range", |p| {
        common::single_leaders(p).is_some()
    })
    .await?;
    let t0 = tokio::time::Instant::now();
    while nodes
        .iter()
        .any(|n| n.node.session.tracker().live_nodes().len() < ids.len())
    {
        ensure!(
            t0.elapsed() < 3 * TTL,
            "nodes do not see each other in the session table yet"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // `register`: write the registry once (one deployment) and wait for every node to
    // see it — the path of a cluster in use. Without it: a fresh cluster never wrote the
    // registry, only generation 0 that the bucket creator made exists.
    let _sdk = if register {
        let sdk = common::fake_sdk::FakeSdk::builder()
            .object("Job", "run", vec![common::fake_sdk::Step::output("x")])
            .spawn()
            .await?;
        common::register(&nodes[0].node.url(), &sdk.url).await?;
        for n in &nodes {
            wait_deployments(&n.node.url(), 1, 3 * TTL).await?;
        }
        Some(sdk)
    } else {
        None
    };
    // Warm up for one TTL: the first pass of every background loop (opening ranges, first LIST) is done.
    tokio::time::sleep(TTL).await;
    let before = tally(&nodes);
    tokio::time::sleep(TTL * K).await;
    let after = tally(&nodes);
    let window = after
        .into_iter()
        .filter_map(|(k, n)| {
            let d = n - before.get(&k).copied().unwrap_or(0);
            (d > 0).then_some((k, d))
        })
        .collect();
    Ok((bucket, nodes, window))
}

async fn wait_deployments(url: &str, n: usize, within: Duration) -> Result<()> {
    let t0 = tokio::time::Instant::now();
    loop {
        let listed: serde_json::Value = reqwest::get(format!("{url}/deployments"))
            .await?
            .json()
            .await?;
        if listed.as_array().is_some_and(|v| v.len() >= n) {
            return Ok(());
        }
        ensure!(
            t0.elapsed() < within,
            "{url} does not list {n} deployments after {within:?}: {listed}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn check(what: &str, window: &Tally, caps: &[Cap]) -> Result<()> {
    println!("{what}: requests in {K}·TTL (TTL {TTL:?}), per source");
    for ((src, op), n) in window {
        println!(
            "  {src:<28} {op:<6} {n:>6}  ({:.1}/TTL)",
            *n as f64 / f64::from(K)
        );
    }
    let mut over = Vec::new();
    for cap in caps {
        let n = window
            .get(&(cap.source, format!("{:?}", cap.op)))
            .copied()
            .unwrap_or(0);
        if n > cap.limit() {
            over.push(format!(
                "{} {:?}: {n} > cap {} ({:.1}/TTL, today {:.1}/TTL)",
                cap.source,
                cap.op,
                cap.limit(),
                n as f64 / f64::from(K),
                cap.per_ttl
            ));
        }
    }
    ensure!(
        over.is_empty(),
        "{what}: idle cost cap exceeded:\n{}",
        over.join("\n")
    );
    Ok(())
}

/// **An idle 3-node cluster stays within the per-source request caps.**
///
/// 1. The `control/quiet-lease` gate is on before the nodes start (so the two
///    nodes other than the coordinator delegate renewal).
///    Three nodes `a`, `b`, `c`, [`PARTITIONS`] ranges, TTL 10 s, default cadences.
/// 2. Wait for one leader per range and for every node to see the others; warm up one TTL.
/// 3. Count the whole cluster's requests per (source, op) over `K·TTL` without load.
///
/// Must see: every source of [`CAPS_3`] is within its cap.
#[tokio::test(flavor = "multi_thread")]
async fn idle_three_node_cluster_stays_under_per_source_caps() -> Result<()> {
    let (_bucket, nodes, window) = idle_window(&["a", "b", "c"], true, true).await?;
    let verdict = check("3-node cluster", &window, CAPS_3);
    for n in nodes {
        n.node.shutdown().await;
    }
    verdict
}

/// **An idle lone node stays within the per-source request caps** — the goal of
/// solo mode: no renewal PUT, no LIST of `sessions/`.
///
/// 1. The `control/quiet-lease` gate is on before the node starts.
/// 2. One node `a`, [`PARTITIONS`] ranges, TTL 10 s, default cadences.
/// 3. As in the 3-node test.
///
/// Must see: every source of [`CAPS_1`] is within its cap.
#[tokio::test(flavor = "multi_thread")]
async fn idle_single_node_stays_under_per_source_caps() -> Result<()> {
    let (_bucket, nodes, window) = idle_window(&["a"], true, true).await?;
    let verdict = check("lone node", &window, CAPS_1);
    for n in nodes {
        n.node.shutdown().await;
    }
    verdict
}

/// **A fresh cluster that never wrote the registry does not run the full registry loop
/// every tick.** Before: `control/registry-gen` was absent until the first registry
/// write, readers treated absent as "unknown", so every node ran `RefreshLoop`
/// (3 LISTs and 3 GETs) and the quarantine loop (1 LIST) every 30 s forever — ~480
/// Class A/h per node. Now whoever creates a fresh bucket creates generation 0.
///
/// 1. Gate on as in the two tests above; three nodes `a`, `b`, `c` build an empty
///    bucket and do **not** write the registry.
/// 2. Wait to settle, warm up one TTL, count `K·TTL` of idleness.
///
/// Must see: the LISTs of `node.refresh` and `quarantine` are only the first round (cap
/// [`CAPS_3`], `per_ttl` 0) — the 10-minute reconciliation falls outside the window;
/// every other source of [`CAPS_3`] is within its cap too.
#[tokio::test(flavor = "multi_thread")]
async fn idle_cluster_that_never_wrote_the_registry_skips_full_rounds() -> Result<()> {
    let (_bucket, nodes, window) = idle_window(&["a", "b", "c"], true, false).await?;
    let verdict = check("3-node cluster, registry never written", &window, CAPS_3);
    for n in nodes {
        n.node.shutdown().await;
    }
    verdict
}

// ──────────────────────────── budgeted scrubber

/// Requests of the `scrub` source since the node was built.
fn scrub_requests(store: &FaultyStore) -> u64 {
    store
        .counts_by_source()
        .into_iter()
        .filter(|(src, ..)| *src == "scrub")
        .map(|(.., c)| c)
        .sum()
}

/// An observed scrub round: when it started (since the node was built) and its request count.
#[derive(Debug)]
struct SeenRound {
    at: Duration,
    requests: u64,
}

/// Builds a `NodeMode::single(n)` node (`n` ranges) with a scrubber of period `every` and
/// a budget of `budget` requests/hour, then observes `window`: a round is a run of
/// consecutive `scrub` requests, separated from the next by ≥ 250 ms of silence.
async fn scrub_rounds(
    n: u16,
    every: Duration,
    budget: u64,
    window: Duration,
) -> Result<(Vec<SeenRound>, u64)> {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let stores = ObjectStores::single(StoreLocation::from_store(faulty.clone(), "", "memory://"));
    let mut cfg = NodeConfig::new(stores, NodeMode::single(n));
    cfg.scrub_every = Some(every);
    cfg.scrub_requests_per_hour = Some(budget);
    let node = node::start(cfg)
        .await
        .map_err(|e| anyhow::anyhow!("build node: {e}"))?;
    let t0 = std::time::Instant::now();
    let mut rounds: Vec<SeenRound> = Vec::new();
    let (mut last, mut last_change) = (0u64, t0);
    let mut open: Option<(Duration, u64)> = None;
    while t0.elapsed() < window {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let now = scrub_requests(&faulty);
        if now != last {
            if open.is_none() {
                open = Some((t0.elapsed(), last));
            }
            last = now;
            last_change = std::time::Instant::now();
        } else if let Some((at, from)) = open {
            if last_change.elapsed() >= Duration::from_millis(250) {
                rounds.push(SeenRound {
                    at,
                    requests: now - from,
                });
                open = None;
            }
        }
    }
    let total = scrub_requests(&faulty);
    node.shutdown().await;
    Ok((rounds, total))
}

/// **The scrubber follows a request budget, not a fixed period.** The cost of a round
/// is proportional to the range count (manifest + checkpoint + owner record per
/// range); with a fixed period, the scrubber's requests/hour grow with ranges forever.
///
/// 1. A 16-range node, scrubber period 1 s, budget `BUDGET` requests/hour — above the
///    cost of 16 ranges at a 1 s period: the budget is not hit, rounds run every 1 s.
/// 2. A 128-range node, same period and budget: the cost of one round × 3600 exceeds the
///    budget many times over.
///
/// Must see: at 16 ranges the rounds are ~1 s apart (the budget does not slow a small
/// cluster); at 128 ranges every round is still complete (same request count per round)
/// but they are ≥ `round cost / budget` apart, and the total requests in the window do
/// not exceed one round plus the window's budget. On the old code: 128 ranges still scanned every 1 s.
#[tokio::test(flavor = "multi_thread")]
async fn scrub_requests_per_hour_are_capped_by_the_budget_as_ranges_grow() -> Result<()> {
    const EVERY: Duration = Duration::from_secs(1);
    // 400 requests/second.
    const BUDGET: u64 = 400 * 3600;
    const WINDOW: Duration = Duration::from_secs(16);
    let (small, big) = tokio::join!(
        scrub_rounds(16, EVERY, BUDGET, WINDOW),
        scrub_rounds(128, EVERY, BUDGET, WINDOW)
    );
    let ((small, small_total), (big, big_total)) = (small?, big?);
    println!("16 ranges: {small_total} requests, rounds {small:?}");
    println!("128 ranges: {big_total} requests, rounds {big:?}");
    ensure!(
        small.len() >= 6 && big.len() >= 2,
        "too few rounds to compare: 16 ranges {}, 128 ranges {}",
        small.len(),
        big.len()
    );
    let cost = |r: &[SeenRound]| r.iter().map(|r| r.requests).max().unwrap_or(0);
    let (c16, c128) = (cost(&small), cost(&big));
    ensure!(
        c16 * 3600 < BUDGET && c128 * 3600 > 2 * BUDGET,
        "test thresholds disagree with the measured round cost: 16 ranges {c16}, 128 ranges {c128} requests/round"
    );
    // 16 ranges: the budget is not hit — the gap between two rounds ≈ the period.
    for w in small.windows(2) {
        let gap = w[1].at - w[0].at;
        ensure!(
            gap < EVERY * 2,
            "16 ranges: two rounds {gap:?} apart (period {EVERY:?}) — the budget must not slow a small cluster"
        );
    }
    // 128 ranges, from the first complete round (at start-up ranges are still opening,
    // the first round may break into several pieces): every round is complete (same
    // cost ±10 %) and rounds are ≥ cost / budget apart.
    let full = |r: &SeenRound| r.requests * 10 >= c128 * 9;
    let first = big.iter().position(full).unwrap_or(0);
    let steady = &big[first..];
    ensure!(
        steady.len() >= 2,
        "128 ranges: fewer than two complete rounds: {big:?}"
    );
    for r in steady {
        ensure!(
            full(r),
            "128 ranges: round {r:?} incomplete (largest round {c128} requests)"
        );
    }
    let min_gap = Duration::from_secs_f64(c128 as f64 * 3600.0 / BUDGET as f64);
    for w in steady.windows(2) {
        let gap = w[1].at - w[0].at;
        ensure!(
            gap >= min_gap.mul_f64(0.9),
            "128 ranges: two rounds {gap:?} apart < round cost / budget = {min_gap:?} ({c128} requests/round)"
        );
    }
    let cap = c128 + (BUDGET as f64 * WINDOW.as_secs_f64() / 3600.0) as u64;
    ensure!(
        big_total <= cap,
        "128 ranges: {big_total} requests in {WINDOW:?} > one round + window budget = {cap}"
    );
    Ok(())
}
