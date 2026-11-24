// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A slow node gives up leadership by itself: a slowness score measured from
//! existing PUTs (session renewal, shared WAL), published in the session
//! record, and — when one node is clearly slower than the rest of the cluster —
//! that node releases all its ranges.
//!
//! The test's timings are shortened through [`cairn_server::slow::SlowConfig`]:
//! 1 s rounds instead of 10 s. A 2 s lease TTL (renewal every 400 ms) so that a
//! 300 ms delay on every PUT of one node makes that node *slow* without making
//! it *dead*: the renewal still comes back before the per-attempt limit.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, Result},
    cairn_server::slow::SlowConfig,
    common::{start_cluster_node_tweaked, ClusterNode},
    futures::TryStreamExt,
    slatedb::object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreExt},
    std::{sync::Arc, time::Duration},
};

const TTL: Duration = Duration::from_secs(2);

/// Slowness-score configuration shortened for tests.
fn fast_slow() -> SlowConfig {
    SlowConfig {
        round: Duration::from_secs(1),
        slow_put: Duration::from_millis(150),
        decay: Duration::from_secs(3),
        recover_for: Duration::from_secs(3),
        marker_ttl: Duration::from_secs(10),
        shed_gap: Duration::from_millis(300),
        ..SlowConfig::default()
    }
}

async fn node(bucket: &Arc<InMemory>, id: &str, partitions: u16) -> Result<ClusterNode> {
    start_cluster_node_tweaked(bucket, id, partitions, TTL, |cfg| cfg.slow = fast_slow()).await
}

/// Like [`node`], pinned to `direct` mode (`common::direct`) for the scoring and
/// shedding tests: the slowness score's PUT samples are an idle node's renewals.
/// In `solo` mode or under delegation an idle node does not PUT renewals — no
/// samples, no score (an idle delegated node drops out of the median —
/// `idle_delegated_peers_publish_no_stale_p99`).
async fn direct_node(bucket: &Arc<InMemory>, id: &str, partitions: u16) -> Result<ClusterNode> {
    start_cluster_node_tweaked(bucket, id, partitions, TTL, |cfg| {
        cfg.slow = fast_slow();
        common::direct(cfg);
    })
    .await
}

/// Content of the newest **live** session record of `node`.
async fn session_record(bucket: &Arc<InMemory>, node: &str) -> Result<String> {
    let dir = Path::from(format!("sessions/{node}"));
    let mut listed: Vec<_> = bucket.list(Some(&dir)).try_collect().await?;
    listed.sort_by_key(|m| m.location.to_string());
    let last = listed
        .last()
        .ok_or_else(|| anyhow!("{node} has no session yet"))?;
    let raw = bucket.get(&last.location).await?.bytes().await?;
    Ok(String::from_utf8_lossy(&raw).into_owned())
}

/// After one round, the node's session record carries `slow=` and
/// `p99_put_ms=`, and `/metrics` has `cairnflow_slow_score` and the PUT
/// histogram by kind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_record_publishes_slow_score_and_p99() -> Result<()> {
    let bucket = common::bucket();
    let a = direct_node(&bucket, "node-alpha", 2).await?;
    let t0 = std::time::Instant::now();
    let rec = loop {
        let rec = session_record(&bucket, "node-alpha").await?;
        if rec.contains("\nslow=") && rec.contains("\np99_put_ms=") {
            break rec;
        }
        anyhow::ensure!(
            t0.elapsed() < Duration::from_secs(10),
            "not published after 10 s: {rec}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    // A healthy node: a low score. Do not require exactly 1 — running in parallel
    // with other tests, a renewal can exceed the test's 150 ms threshold under CPU contention.
    let slow: u32 = rec
        .lines()
        .find_map(|l| l.strip_prefix("slow="))
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| anyhow!("slow= broken: {rec}"))?;
    assert!(slow < 100, "healthy node with score {slow}: {rec}");
    assert!(!rec.contains("draining="), "{rec}");
    let text = reqwest::get(format!("{}/metrics", a.node.url()))
        .await?
        .text()
        .await?;
    assert!(text.contains("cairnflow_slow_score "), "{text}");
    assert!(text.contains("cairnflow_draining 0\n"), "{text}");
    assert!(
        text.contains("cairnflow_object_put_seconds_count{kind=\"session\"}"),
        "{text}"
    );
    a.node.shutdown().await;
    Ok(())
}

/// Exactly one leader per range ⇒ the number of ranges each node leads, by node index.
fn loads(
    nodes: &[&ClusterNode],
    probe: &std::collections::BTreeMap<u32, Vec<usize>>,
) -> Option<Vec<usize>> {
    common::single_leaders(probe).map(|m| common::load_of(&m, nodes.len()))
}

/// Time limit for a node with uniformly slow PUTs to finish shedding: 1 → 100
/// takes seven rounds (doubling each round), plus `shed_rounds` rounds at 100,
/// plus the resign time (`shed_batch` ranges every `shed_gap`), plus two TTLs
/// for the other nodes to take over.
fn shed_bound(cfg: &SlowConfig, ranges: usize) -> Duration {
    let resign = cfg.shed_gap * ranges.div_ceil(cfg.shed_batch) as u32;
    cfg.round * (7 + cfg.shed_rounds + 1) + resign + TTL * 2
}

/// Shedding and recovery. Order:
///
/// 1. alpha, beta, gamma come up with 6 ranges ⇒ {2, 2, 2}.
/// 2. Every PUT of gamma gets 300 ms slower (renewal timeout threshold 150 ms) —
///    gamma still renews in time (limit 400 ms per attempt), so the lease does
///    not expire: a *grey* node.
/// 3. gamma's score rises to 100; its p99 (≥ 300 ms) > 2× the median of alpha
///    and beta (a few ms). gamma takes the `shedding` marker, sets `draining`,
///    and resigns two ranges at a time.
/// 4. Remove the delay. gamma's score slides below 10 (the `decay` window),
///    stays below it for `recover_for`, then gamma clears `draining` and
///    returns the marker.
///
/// Must see: after step 3, within [`shed_bound`], gamma leads no range, alpha
/// and beta hold all 6, the marker carries gamma's session and gamma's session
/// record has `draining=1`. After step 4, gamma gets its fair share back
/// ({2, 2, 2}) and the marker has been returned (`released=1`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_node_sheds_everything_then_takes_ranges_back() -> Result<()> {
    use cairn_store::testkit::{Action, OpSet, PathMatch, Rule};
    let bucket = common::bucket();
    let mut nodes = Vec::new();
    for id in ["node-alpha", "node-beta", "node-gamma"] {
        nodes.push(direct_node(&bucket, id, 6).await?);
    }
    let refs: Vec<_> = nodes.iter().collect();
    let balance_bound = cairn_server::balance::settle_for(TTL) + TTL * 10;
    common::wait_probe(&refs, balance_bound, "split 2/2/2", |p| {
        loads(&refs, p) == Some(vec![2, 2, 2])
    })
    .await?;

    let slow = nodes[2].store.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Any,
        Action::Delay(Duration::from_millis(300)),
    ));
    let cfg = fast_slow();
    common::wait_probe(
        &refs,
        shed_bound(&cfg, 2),
        "gamma sheds all, alpha+beta hold all 6",
        |p| loads(&refs, p) == Some(vec![3, 3, 0]),
    )
    .await?;
    let (marker, _) =
        cairn_store::health::read_shed(&(bucket.clone() as Arc<dyn ObjectStore>), &Path::default())
            .await?
            .ok_or_else(|| anyhow!("no shedding marker"))?;
    let marker = marker?;
    assert_eq!(marker.session.node, "node-gamma");
    assert!(!marker.released);
    let rec = session_record(&bucket, "node-gamma").await?;
    assert!(rec.contains("\ndraining=1\n"), "{rec}");

    // Remove the delay ⇒ recovery after decay + recover_for, then it takes its fair share back.
    assert!(nodes[2].store.remove_rule(slow));
    let recover_bound = cfg.decay + cfg.recover_for + cfg.round * 2 + balance_bound;
    common::wait_probe(
        &refs,
        recover_bound,
        "gamma takes its share back, 2/2/2",
        |p| loads(&refs, p) == Some(vec![2, 2, 2]),
    )
    .await?;
    let (marker, _) =
        cairn_store::health::read_shed(&(bucket.clone() as Arc<dyn ObjectStore>), &Path::default())
            .await?
            .ok_or_else(|| anyhow!("marker vanished"))?;
    assert!(marker?.released, "gamma recovered, marker kept");
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// The whole cluster is slow. Every PUT of **all three** nodes is equally slow:
/// everyone's score rises to 100, but nobody is slower than 2× the median of
/// the rest.
///
/// Must see: for the whole time limit in which a single slow node would have
/// finished shedding, the distribution stays at {2, 2, 2} and nobody takes the
/// marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uniformly_slow_cluster_never_sheds() -> Result<()> {
    use cairn_store::testkit::{Action, OpSet, PathMatch, Rule};
    let bucket = common::bucket();
    let mut nodes = Vec::new();
    for id in ["node-alpha", "node-beta", "node-gamma"] {
        nodes.push(direct_node(&bucket, id, 6).await?);
    }
    let refs: Vec<_> = nodes.iter().collect();
    let balance_bound = cairn_server::balance::settle_for(TTL) + TTL * 10;
    let first = common::wait_probe(&refs, balance_bound, "split 2/2/2", |p| {
        loads(&refs, p) == Some(vec![2, 2, 2])
    })
    .await?;
    let first = common::single_leaders(&first).expect("balanced");
    for n in &nodes {
        n.store.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Any,
            Action::Delay(Duration::from_millis(300)),
        ));
    }
    let t0 = std::time::Instant::now();
    while t0.elapsed() < shed_bound(&fast_slow(), 2) + fast_slow().round * 3 {
        let now = common::leaders_by_probe(&refs).await;
        anyhow::ensure!(
            common::single_leaders(&now).as_ref() == Some(&first),
            "distribution changed while the whole cluster is equally slow, after {:?}: {first:?} → {now:?}",
            t0.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let marker =
        cairn_store::health::read_shed(&(bucket.clone() as Arc<dyn ObjectStore>), &Path::default())
            .await?;
    assert!(marker.is_none(), "marker taken though all equally slow");
    let rec = session_record(&bucket, "node-alpha").await?;
    assert!(
        rec.contains("\nslow=100\n"),
        "the score must reach 100 — otherwise the test measures nothing: {rec}"
    );
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// A cluster with only one live node never sheds, however slow it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lone_node_never_sheds() -> Result<()> {
    use cairn_store::testkit::{Action, OpSet, PathMatch, Rule};
    let bucket = common::bucket();
    let a = direct_node(&bucket, "node-alpha", 2).await?;
    let refs = [&a];
    common::wait_probe(&refs, Duration::from_secs(10), "alpha holds both", |p| {
        loads(&refs, p) == Some(vec![2])
    })
    .await?;
    a.store.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Any,
        Action::Delay(Duration::from_millis(300)),
    ));
    let t0 = std::time::Instant::now();
    while t0.elapsed() < shed_bound(&fast_slow(), 2) {
        let now = common::leaders_by_probe(&refs).await;
        anyhow::ensure!(
            loads(&refs, &now) == Some(vec![2]),
            "the only node shed ranges: {now:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let rec = session_record(&bucket, "node-alpha").await?;
    assert!(
        rec.contains("\nslow=100\n"),
        "the score must reach 100 — otherwise the test measures nothing: {rec}"
    );
    a.node.shutdown().await;
    Ok(())
}

/// **An idle delegated node has no PUT samples, so it drops out of the
/// median**: it does not PUT renewals and does not write the WAL — each
/// slowness-score round has no samples at all. Its p99 must be **absent**
/// (carried by the heartbeat into `view`), not a stale p99 from when it still
/// renewed by PUT — a stale number skews the median that `slow.rs` compares
/// slow nodes against.
///
/// 1. Gate `control/quiet-lease` on; three nodes, 1 s slowness-score rounds;
///    wait until the coordinator `C` delegates the two other nodes.
/// 2. Send one invocation into each range: its leader PUTs the shared WAL —
///    every node has samples and publishes a p99.
/// 3. Wait three idle rounds.
///
/// Must see: the peer health `C` sees (`Membership::peer_health`) for the two
/// other nodes has no `p99_put_ms`. Before the fix: the p99 of the last round
/// that still had PUTs stayed forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_delegated_peers_publish_no_stale_p99() -> Result<()> {
    let bucket = common::bucket();
    {
        let store: Arc<dyn ObjectStore> = (*bucket).clone();
        let on = cairn_store::quiet_lease::QuietLease {
            on: true,
            knows: cairn_store::migrate::QUIET_LEASE,
            by: "slow_shed".into(),
            at_ms: 0,
            delegation: true,
            ..Default::default()
        };
        anyhow::ensure!(
            cairn_store::quiet_lease::write(&store, &Default::default(), &on, None).await?,
            "cannot write the gate"
        );
    }
    let mut nodes = Vec::new();
    for id in ["node-alpha", "node-beta", "node-gamma"] {
        nodes.push(node(&bucket, id, 3).await?);
    }
    let r = async {
        let t0 = std::time::Instant::now();
        let c = loop {
            if let Some(seen) = common::coordinator_of(&bucket)
                .await
                .filter(|s| !s.released)
            {
                if let Some(i) = nodes.iter().position(|n| {
                    n.node
                        .session
                        .current()
                        .is_some_and(|id| id.to_string() == seen.session)
                }) {
                    break i;
                }
            }
            anyhow::ensure!(t0.elapsed() < TTL * 5, "no coordinator yet");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        common::until_delegated(&bucket, &nodes, c, TTL * 10).await?;
        let sdk = common::fake_sdk::FakeSdk::builder()
            .object("Job", "run", vec![common::fake_sdk::Step::output("x")])
            .spawn()
            .await?;
        common::register(&nodes[0].node.url(), &sdk.url).await?;
        tokio::time::sleep(TTL).await;
        let client = reqwest::Client::new();
        for round in 0..3 {
            for p in 0..3u32 {
                let key = common::key_on_partition(&nodes[0].node, "Job", p, &format!("k{round}"));
                for n in &nodes {
                    let r = client
                        .post(format!("{}/invoke/Job/run?key={key}", n.node.url()))
                        .json(&serde_json::json!("x"))
                        .send()
                        .await;
                    if r.is_ok_and(|r| r.status().is_success()) {
                        break;
                    }
                }
            }
            tokio::time::sleep(fast_slow().round / 2).await;
        }
        tokio::time::sleep(fast_slow().round * 3).await;
        let health = nodes[c].node.session.membership().peer_health();
        for (i, n) in nodes.iter().enumerate() {
            if i == c {
                continue;
            }
            let h = health
                .get(&n.node.node_id)
                .ok_or_else(|| anyhow!("coordinator does not see {}", n.node.node_id))?;
            anyhow::ensure!(
                h.p99_put_ms.is_none(),
                "idle delegated node {} still publishes p99 {:?} (stale)",
                n.node.node_id,
                h.p99_put_ms
            );
        }
        Ok(())
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}
