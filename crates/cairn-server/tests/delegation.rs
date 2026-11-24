// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Delegated renewal** (LS-14/LS-15): nodes other than the coordinator stop PUTting
//! session renewals and renew their grants through acks on heartbeats; only the
//! coordinator expires a delegating node (an If-Match CAS after a silent TTL); other
//! observers yield.
//!
//! The tests look from outside: objects on the bucket, requests counted by source on
//! each node's store, whether an engine answers as leader. The `control/quiet-lease`
//! gate is turned on before the nodes start (like `cairn-ctl cluster
//! enable-quiet-lease --apply`). Tests needing the network filter live in
//! `net_partition.rs` (`--features testkit`), tests using failpoints in `failpoints.rs`.

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
    cairn_store::testkit::{Action, Op, OpSet, PathMatch, Rule},
    common::{
        coordinator_of, freeze, leaders_by_probe, single_leaders, start_cluster_node,
        start_cluster_node_tweaked, wait_probe, ClusterNode,
    },
    slatedb::object_store::{path::Path, ObjectStoreExt},
    std::time::{Duration, Instant},
};

const TTL: Duration = Duration::from_secs(1);

/// Turn on the `control/quiet-lease` gate, like `cairn-ctl cluster enable-quiet-lease --apply`.
async fn quiet_lease_on(bucket: &common::Bucket) -> Result<()> {
    let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = (**bucket).clone();
    let on = cairn_store::quiet_lease::QuietLease {
        on: true,
        knows: cairn_store::migrate::QUIET_LEASE,
        by: "delegation".into(),
        at_ms: 0,
        delegation: true,
        ..Default::default()
    };
    ensure!(
        cairn_store::quiet_lease::write(&store, &Default::default(), &on, None).await?,
        "could not write the gate"
    );
    Ok(())
}

/// The gate on **without** the delegation flag: lone-node mode yes, delegation no —
/// every node of a multi-node cluster stays `direct`.
async fn quiet_lease_on_without_delegation(bucket: &common::Bucket) -> Result<()> {
    let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = (**bucket).clone();
    let on = cairn_store::quiet_lease::QuietLease {
        on: true,
        knows: cairn_store::migrate::QUIET_LEASE,
        by: "delegation".into(),
        at_ms: 0,
        delegation: false,
        ..Default::default()
    };
    ensure!(
        cairn_store::quiet_lease::write(&store, &Default::default(), &on, None).await?,
        "could not write the gate"
    );
    Ok(())
}

/// Three nodes, four ranges, TTL 1 s, gate on; wait for one leader per range.
async fn three(prefix: &str) -> Result<(common::Bucket, Vec<ClusterNode>)> {
    let bucket = common::bucket();
    quiet_lease_on(&bucket).await?;
    let mut nodes = Vec::new();
    for id in ["a", "b", "c"] {
        nodes.push(start_cluster_node(&bucket, &format!("{prefix}-{id}"), 4, TTL).await?);
    }
    let refs: Vec<_> = nodes.iter().collect();
    wait_probe(
        &refs,
        Duration::from_secs(20),
        "one leader per range",
        |p| single_leaders(p).is_some(),
    )
    .await?;
    Ok((bucket, nodes))
}

/// Wait until the cluster has finished splitting the ranges (2/1/1), then 2·TTL more
/// without an owner change — a balancer yield is a valid `owns` CAS, not a renewal.
async fn balanced(nodes: &[ClusterNode]) -> Result<std::collections::BTreeMap<u32, usize>> {
    let refs: Vec<_> = nodes.iter().collect();
    let t0 = Instant::now();
    loop {
        let p = wait_probe(&refs, TTL * 10, "one leader per range", |p| {
            single_leaders(p).is_some()
        })
        .await?;
        let leaders = single_leaders(&p).expect("one leader per range");
        let mut counts = vec![0; nodes.len()];
        for i in leaders.values() {
            counts[*i] += 1;
        }
        counts.sort_unstable();
        if counts == [1, 1, 2] {
            tokio::time::sleep(TTL * 2).await;
            let again = common::leaders_by_probe(&refs).await;
            if single_leaders(&again).as_ref() == Some(&leaders) {
                return Ok(leaders);
            }
        }
        ensure!(t0.elapsed() < TTL * 30, "cluster not balanced: {p:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The index of the node holding `control/coordinator`.
async fn coordinator_index(bucket: &common::Bucket, nodes: &[ClusterNode]) -> Result<usize> {
    let t0 = Instant::now();
    loop {
        if let Some(seen) = coordinator_of(bucket).await.filter(|s| !s.released) {
            if let Some(i) = nodes.iter().position(|n| {
                n.node
                    .session
                    .current()
                    .is_some_and(|id| id.to_string() == seen.session)
            }) {
                return Ok(i);
            }
        }
        ensure!(t0.elapsed() < TTL * 10, "no coordinator yet");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn record_of(bucket: &common::Bucket, n: &ClusterNode) -> Option<String> {
    common::session_text(bucket, n).await
}

async fn delegated(bucket: &common::Bucket, n: &ClusterNode) -> bool {
    common::is_delegated(bucket, n).await
}

async fn direct(bucket: &common::Bucket, n: &ClusterNode) -> bool {
    common::is_direct(bucket, n).await
}

async fn until_delegated(
    bucket: &common::Bucket,
    nodes: &[ClusterNode],
    c: usize,
    within: Duration,
) -> Result<()> {
    common::until_delegated(bucket, nodes, c, within).await
}

/// No range has two leaders answering at once.
fn no_two_leaders(p: &std::collections::BTreeMap<u32, Vec<usize>>) -> Result<()> {
    for (r, v) in p {
        ensure!(v.len() <= 1, "range {r} has two leaders answering: {v:?}");
    }
    Ok(())
}

/// Three nodes that have finished delegating and splitting the ranges: (bucket,
/// nodes, the coordinator's index, the owner of each range).
async fn settled(
    prefix: &str,
) -> Result<(
    common::Bucket,
    Vec<ClusterNode>,
    usize,
    std::collections::BTreeMap<u32, usize>,
)> {
    let (bucket, nodes) = three(prefix).await?;
    let setup = async {
        let c = coordinator_index(&bucket, &nodes).await?;
        until_delegated(&bucket, &nodes, c, TTL * 10).await?;
        let leaders = balanced(&nodes).await?;
        until_delegated(&bucket, &nodes, c, TTL * 5).await?;
        Ok::<_, anyhow::Error>((c, leaders))
    }
    .await;
    match setup {
        Ok((c, leaders)) => Ok((bucket, nodes, c, leaders)),
        Err(e) => {
            shutdown(nodes).await;
            Err(e)
        }
    }
}

/// A node other than `c` holding at least one range.
fn delegator_with_ranges(leaders: &std::collections::BTreeMap<u32, usize>, c: usize) -> usize {
    *leaders
        .values()
        .find(|i| **i != c)
        .expect("a node other than the coordinator holds a range")
}

/// Wait until every range in `ranges` has a leader on a node other than `gone`,
/// recording when the **first** range changed owner. Checks for two leaders on every poll.
async fn until_moved(
    nodes: &[ClusterNode],
    gone: &[usize],
    ranges: &[u32],
    t0: Instant,
    within: Duration,
) -> Result<(Duration, Duration)> {
    let refs: Vec<_> = nodes.iter().collect();
    let mut first = None;
    loop {
        let p = leaders_by_probe(&refs).await;
        no_two_leaders(&p)?;
        let moved: Vec<bool> = ranges
            .iter()
            .map(|r| {
                p.get(r)
                    .is_some_and(|v| v.len() == 1 && !gone.contains(&v[0]))
            })
            .collect();
        if moved.iter().any(|m| *m) && first.is_none() {
            first = Some(t0.elapsed());
        }
        if moved.iter().all(|m| *m) {
            return Ok((first.unwrap_or_default(), t0.elapsed()));
        }
        ensure!(
            t0.elapsed() < within,
            "after {within:?} ranges {ranges:?} have not moved off {gone:?}: {p:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn source_count(n: &ClusterNode, source: &str, op: Op) -> u64 {
    n.store
        .counts_by_source()
        .into_iter()
        .filter(|(src, o, _, _)| *src == source && *o == op)
        .map(|(_, _, _, c)| c)
        .sum()
}

async fn shutdown(nodes: Vec<ClusterNode>) {
    for n in nodes {
        n.node.shutdown().await;
    }
}

/// **A delegating node PUTs no renewal while it is being acked** (LS-14).
///
/// 1. Three nodes, four ranges, TTL 1 s, gate on; wait for coordinator `C`, a
///    `mode=delegated` record on every other node, and the range split to finish.
/// 2. Count each node's `session.renew` PUTs over 6·TTL of idleness.
///
/// Must see: nodes other than `C` PUT no renewal (without delegation: 5 per TTL), keep
/// their incarnation, their records stay `mode=delegated`, and no range changes owner.
/// `C`: it enters `solo` once every other node delegates to it — at most one PUT (the
/// CAS into the mode, if it falls inside the window); before the `solo` mode it was
/// always `direct`, PUTting every TTL/5
/// (`coordinator_sends_no_session_put_while_every_peer_delegates` checks `C`'s part).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delegated_node_sends_no_session_put_while_acked() -> Result<()> {
    let (bucket, nodes, c, before) = settled("quiet").await?;
    let r = async {
        let incs: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();
        let puts: Vec<u64> = nodes
            .iter()
            .map(|n| source_count(n, "session.renew", Op::Put))
            .collect();
        tokio::time::sleep(TTL * 6).await;
        for (i, n) in nodes.iter().enumerate() {
            let sent = source_count(n, "session.renew", Op::Put) - puts[i];
            ensure!(
                n.node.session.current() == incs[i],
                "node {i} changed incarnation while idle"
            );
            if i == c {
                ensure!(
                    sent <= 1,
                    "the coordinator PUT a renewal {sent} times over 6·TTL: {:?}",
                    record_of(&bucket, n).await
                );
            } else {
                ensure!(
                    sent == 0 && delegated(&bucket, n).await,
                    "delegating node {i} PUT a renewal {sent} times over 6·TTL: {:?}",
                    record_of(&bucket, n).await
                );
            }
        }
        let refs: Vec<_> = nodes.iter().collect();
        let now = leaders_by_probe(&refs).await;
        ensure!(
            single_leaders(&now).as_ref() == Some(&before),
            "a range changed owner while idle: {before:?} → {now:?}"
        );
        Ok(())
    }
    .await;
    shutdown(nodes).await;
    r
}

/// Wait until `n`'s record becomes an unchallenged `solo` record.
async fn until_solo(bucket: &common::Bucket, n: &ClusterNode, within: Duration) -> Result<()> {
    let t0 = Instant::now();
    while !common::is_solo(bucket, n).await {
        ensure!(
            t0.elapsed() < within,
            "after {within:?} the record is not `solo` yet: {:?}",
            record_of(bucket, n).await
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

/// **The coordinator PUTs no renewal when every other node delegates to it, and the
/// delegating nodes do not challenge it.**
///
/// 1. Three nodes, finished delegating to `C` and splitting the ranges.
/// 2. Wait until `C`'s record becomes `mode=solo` (one `direct` renewal records the
///    mode once its preconditions hold).
/// 3. Count every node's `session.renew` PUTs over 6·TTL of idleness — ≥ 3 rounds of
///    the delegating nodes' sparse tracker (2·TTL), each round GETting `C`'s `solo`
///    record —, reading `C`'s record every 50 ms.
///
/// Must see: no node PUTs a renewal (without `solo`: `C` always `direct`, a PUT every
/// TTL/5); `C`'s record never carries a `challenged=` line (the deferral rule: a node
/// delegating to `C` does not challenge it); every node keeps its incarnation; no
/// range changes owner.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_sends_no_session_put_while_every_peer_delegates() -> Result<()> {
    let (bucket, nodes, c, before) = settled("csolo").await?;
    let r = async {
        until_solo(&bucket, &nodes[c], TTL * 5).await?;
        let incs: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();
        let puts: Vec<u64> = nodes
            .iter()
            .map(|n| source_count(n, "session.renew", Op::Put))
            .collect();
        let t0 = Instant::now();
        while t0.elapsed() < TTL * 6 {
            let rec = record_of(&bucket, &nodes[c]).await.unwrap_or_default();
            ensure!(
                !rec.lines().any(|l| l.starts_with("challenged=")),
                "the coordinator was challenged after {:?}: {rec}",
                t0.elapsed()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        for (i, n) in nodes.iter().enumerate() {
            let sent = source_count(n, "session.renew", Op::Put) - puts[i];
            ensure!(
                n.node.session.current() == incs[i],
                "node {i} changed incarnation while idle"
            );
            ensure!(
                sent == 0,
                "node {i} (coordinator: {}) PUT a renewal {sent} times over 6·TTL: {:?}",
                i == c,
                record_of(&bucket, n).await
            );
        }
        ensure!(
            common::is_solo(&bucket, &nodes[c]).await,
            "the coordinator left `solo`: {:?}",
            record_of(&bucket, &nodes[c]).await
        );
        let refs: Vec<_> = nodes.iter().collect();
        let now = leaders_by_probe(&refs).await;
        ensure!(
            single_leaders(&now).as_ref() == Some(&before),
            "a range changed owner while idle: {before:?} → {now:?}"
        );
        Ok(())
    }
    .await;
    shutdown(nodes).await;
    r
}

/// **A new node joins: the coordinator leaves `solo`, then re-enters once that node
/// delegates** (leaving `solo`, deferring challenges).
///
/// 1. Three nodes, finished delegating; `C`'s record is `solo`.
/// 2. A fourth node starts: a `direct` record, delegating to nobody. Its LIST sees
///    `C`'s `solo` record and challenges it at once; `C` also sees a session that
///    does not delegate (the first heartbeat, LIST). `C` goes back to `direct` — one CAS.
/// 3. The new node is permitted and delegates to `C`; every other session delegates
///    to `C` again, so `C` re-enters `solo`.
///
/// Must see: `C`'s record leaves `solo` within 3·TTL of the new node having a
/// session, then returns to `solo` (the new node `delegated`) within 10·TTL; none of
/// the first three nodes changes incarnation; no two leaders on any poll.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_leaves_solo_for_a_joining_node_and_returns_after_it_delegates() -> Result<()> {
    let (bucket, mut nodes, c, _) = settled("cjoin").await?;
    let r = async {
        until_solo(&bucket, &nodes[c], TTL * 5).await?;
        let incs: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();
        let joined = start_cluster_node(&bucket, "cjoin-d", 4, TTL).await?;
        nodes.push(joined);
        let t0 = Instant::now();
        let refs: Vec<_> = nodes.iter().collect();
        while common::is_solo(&bucket, &nodes[c]).await {
            no_two_leaders(&leaders_by_probe(&refs).await)?;
            ensure!(
                t0.elapsed() < TTL * 3,
                "the coordinator still `solo` {:?} after the new node joined: {:?}",
                t0.elapsed(),
                record_of(&bucket, &nodes[c]).await
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let d = nodes.len() - 1;
        loop {
            no_two_leaders(&leaders_by_probe(&refs).await)?;
            if delegated(&bucket, &nodes[d]).await && common::is_solo(&bucket, &nodes[c]).await {
                break;
            }
            ensure!(
                t0.elapsed() < TTL * 10,
                "after {:?} the coordinator has not returned to `solo`: C {:?}, new node {:?}",
                t0.elapsed(),
                record_of(&bucket, &nodes[c]).await,
                record_of(&bucket, &nodes[d]).await
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        for (i, inc) in incs.iter().enumerate() {
            ensure!(
                nodes[i].node.session.current() == *inc,
                "node {i} changed incarnation when the new node joined"
            );
        }
        Ok(())
    }
    .await;
    shutdown(nodes).await;
    r
}

/// **A frozen `solo` coordinator: its ranges move away after a challenge and one TTL,
/// within 2·TTL** (liveness — the `solo` version of
/// `coordinator_killed_ranges_move_within_two_ttl`).
///
/// 1. Three nodes, finished delegating; `C`'s record is `solo`, `C` holds at least one range.
/// 2. At `t0` freeze `C`'s store (the shape of a `kill -9` seen from the bucket).
/// 3. `C`'s fence hangs, so no heartbeat is acked any more; the other nodes fall back to
///    `direct` after ~TTL/3 — the deferral ends —, their trackers challenge `C`'s `solo`
///    record, one TTL after the challenge they CAS `expired` and claim the ranges.
///
/// Must see: `C`'s first range changes owner no earlier than ~TTL after `t0` (`C`'s
/// grant lives until its last self-check GET + ⅔·TTL; the challenger waits one TTL),
/// every range of `C` moves to another node within 2·TTL + a one-poll margin; the
/// other nodes keep their incarnation; no two leaders.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn solo_coordinator_frozen_ranges_move_after_challenge_within_two_ttl() -> Result<()> {
    let (bucket, nodes, c, leaders) = settled("cfrozen").await?;
    let r = async {
        until_solo(&bucket, &nodes[c], TTL * 5).await?;
        let incs: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();
        let mine: Vec<u32> = leaders
            .iter()
            .filter(|(_, i)| **i == c)
            .map(|(r, _)| *r)
            .collect();
        ensure!(
            !mine.is_empty(),
            "the coordinator holds no range: {leaders:?}"
        );
        freeze(&nodes[c].store);
        let t0 = Instant::now();
        let (first, all) = until_moved(&nodes, &[c], &mine, t0, TTL * 4).await?;
        ensure!(
            first >= TTL.mul_f32(0.9),
            "the frozen `solo` coordinator's ranges taken after {first:?} — earlier than one TTL"
        );
        ensure!(
            all <= TTL * 2 + TTL / 2,
            "the dead `solo` coordinator's ranges moved after {all:?} — over 2·TTL"
        );
        // By the incarnation at freeze time: `C` may already have `lose`d (past its bound).
        let cid = incs[c].clone().expect("C alive");
        let raw = bucket
            .get(&Path::from(format!("sessions/{}/{}", cid.node, cid.inc)))
            .await?
            .bytes()
            .await?;
        let rec = String::from_utf8_lossy(&raw);
        ensure!(
            rec.lines().any(|l| l.starts_with("challenged=")) && rec.contains("state=expired"),
            "the coordinator's record did not go through the challenge path: {rec}"
        );
        for (i, n) in nodes.iter().enumerate() {
            ensure!(
                i == c || n.node.session.current() == incs[i],
                "node {i} lost its session when the coordinator died"
            );
        }
        Ok(())
    }
    .await;
    shutdown(nodes).await;
    r
}

/// **A `solo` coordinator whose renewal loop died stops acking, is challenged and
/// loses its ranges** (the coordinator's fence in `solo`).
///
/// 1. Three nodes, finished delegating; `C`'s record is `solo`, `C` holds at least one range.
/// 2. At `t0`, `C`'s renewal loop and tracker die (`crash_handle`: the grant validity
///    pinned open), its control loop and HTTP keep running — its fence (a GET of its
///    still-`live` record) still passes.
/// 3. Without this rule: `C` acks forever, the delegating nodes defer the challenge
///    forever, nobody answers a challenge — `C`'s ranges are stuck. With the rule: once
///    the renewal loop has not woken for a TTL, `C`'s fence fails; the nodes fall back
///    after TTL/3, challenge, and one TTL later CAS `expired` and claim the ranges.
///
/// Must see: every range of `C` moves to another node within 4·TTL; the other nodes
/// keep their incarnation; no two leaders.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn solo_coordinator_with_a_dead_keeper_stops_acking_and_loses_its_ranges() -> Result<()> {
    let (bucket, nodes, c, leaders) = settled("ckeeper").await?;
    let r = async {
        until_solo(&bucket, &nodes[c], TTL * 5).await?;
        let incs: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();
        let mine: Vec<u32> = leaders
            .iter()
            .filter(|(_, i)| **i == c)
            .map(|(r, _)| *r)
            .collect();
        ensure!(
            !mine.is_empty(),
            "the coordinator holds no range: {leaders:?}"
        );
        nodes[c].node.session.crash_handle().crash();
        let t0 = Instant::now();
        until_moved(&nodes, &[c], &mine, t0, TTL * 4).await?;
        for (i, n) in nodes.iter().enumerate() {
            ensure!(
                i == c || n.node.session.current() == incs[i],
                "node {i} lost its session when the coordinator's renewal loop died"
            );
        }
        Ok(())
    }
    .await;
    shutdown(nodes).await;
    r
}

/// **A node cut off from the bucket but still reachable by the coordinator loses its
/// ranges.**
///
/// 1. Three nodes, finished delegating; `N` (other than `C`) holds at least one range.
/// 2. At `t0`, every request to `N`'s store fails at once (`Fail`); its HTTP keeps
///    running, heartbeats still reach `C`.
/// 3. `N`'s self-check GET fails: the heartbeats carry a growing bucket-touch age, `C`
///    stops acking once that age is ≥ TTL/3; `N`'s grant runs out; `C` expires `N` one
///    TTL after the last acked heartbeat; another node claims the ranges.
///
/// Must see: `N` stops answering as leader within `TTL/3 + ⅔·TTL` + a margin; every
/// range of `N` moves to another node within 4·TTL; no poll sees two leaders. Without
/// the bucket-touch rule (τ counting unacked heartbeats too): `N` is never expired.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_cut_from_bucket_but_not_coordinator_loses_its_ranges() -> Result<()> {
    let (_bucket, nodes, c, leaders) = settled("cut").await?;
    let r = async {
        let n = delegator_with_ranges(&leaders, c);
        let mine: Vec<u32> = leaders
            .iter()
            .filter(|(_, i)| **i == n)
            .map(|(r, _)| *r)
            .collect();
        nodes[n].store.add_rule(Rule::new(
            OpSet::ALL,
            PathMatch::Any,
            Action::Fail { times: None },
        ));
        let t0 = Instant::now();
        // `N`'s grant runs out by its own clock.
        loop {
            let p = leaders_by_probe(&[&nodes[n]]).await;
            if p.values().all(|v| v.is_empty()) {
                break;
            }
            ensure!(
                t0.elapsed() < TTL + TTL / 2,
                "a node cut off from the bucket still answers as leader after {:?}: {p:?}",
                t0.elapsed()
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        until_moved(&nodes, &[n], &mine, t0, TTL * 4).await?;
        Ok(())
    }
    .await;
    shutdown(nodes).await;
    r
}

/// **A frozen delegating node: its ranges move after one TTL, not earlier** (LS-3 —
/// the delegated version of
/// `cluster_lease.rs::frozen_node_partitions_move_after_ttl_not_before`).
///
/// 1. Three nodes, finished delegating; `N` (other than `C`) holds at least one range.
/// 2. At `t0` every request to `N`'s store hangs (`freeze`).
/// 3. `C` stops acking (bucket-touch age), then expires `N` after a silent TTL counted
///    from the last acked heartbeat; another node claims the ranges.
///
/// Must see: `N`'s first range changes owner no earlier than ~TTL after `t0`, and every
/// range of `N` within 4·TTL; no poll sees two leaders.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frozen_delegated_node_ranges_move_after_ttl_not_before() -> Result<()> {
    let (_bucket, nodes, c, leaders) = settled("frozen").await?;
    let r = async {
        let n = delegator_with_ranges(&leaders, c);
        let mine: Vec<u32> = leaders
            .iter()
            .filter(|(_, i)| **i == n)
            .map(|(r, _)| *r)
            .collect();
        freeze(&nodes[n].store);
        let t0 = Instant::now();
        let (first, _) = until_moved(&nodes, &[n], &mine, t0, TTL * 4).await?;
        ensure!(
            first >= TTL.mul_f32(0.9),
            "the frozen node's ranges taken after {first:?} — earlier than one TTL"
        );
        Ok(())
    }
    .await;
    shutdown(nodes).await;
    r
}

/// **A dead coordinator: its ranges move within 2·TTL** (who expires the coordinator
/// itself).
///
/// 1. Three nodes, finished delegating.
/// 2. At `t0` freeze `C`'s store (the shape of a `kill -9` seen from the bucket; its
///    fence cannot pass, so every heartbeat gets a `409`).
/// 3. The other nodes lose their acks and fall back to `direct` after ~TTL/3; their
///    trackers apply LS-1 to `C`'s `direct` record — or challenge its `solo` record
///    (a separate version: `solo_coordinator_frozen_ranges_move_after_challenge_within_two_ttl`)
///    —, CAS `expired`, claim the ranges.
///
/// Must see: every range of `C` moves to another node within 2·TTL + a one-poll
/// margin; the other nodes keep their incarnation; no two leaders.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_killed_ranges_move_within_two_ttl() -> Result<()> {
    let (_bucket, nodes, c, leaders) = settled("ckill").await?;
    let r = async {
        let incs: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();
        let mine: Vec<u32> = leaders
            .iter()
            .filter(|(_, i)| **i == c)
            .map(|(r, _)| *r)
            .collect();
        ensure!(
            !mine.is_empty(),
            "the coordinator holds no range: {leaders:?}"
        );
        freeze(&nodes[c].store);
        let t0 = Instant::now();
        let (_, all) = until_moved(&nodes, &[c], &mine, t0, TTL * 4).await?;
        ensure!(
            all <= TTL * 2 + TTL / 2,
            "the dead coordinator's ranges moved after {all:?} — over 2·TTL"
        );
        for (i, n) in nodes.iter().enumerate() {
            ensure!(
                i == c || n.node.session.current() == incs[i],
                "node {i} lost its session when the coordinator died"
            );
        }
        Ok(())
    }
    .await;
    shutdown(nodes).await;
    r
}

/// **The coordinator and a delegating node die together: that node's ranges move
/// within 3·TTL** (the yield rule LS-15).
///
/// 1. Three nodes, finished delegating; `N` (other than `C`) holds at least one range;
///    `M` is the remaining node.
/// 2. At `t0` freeze the stores of both `C` and `N`.
/// 3. `M` loses its ack, falls back to `direct`, expires `C` by LS-1 (~4/3·TTL); seeing
///    `C`'s role gone it **yields** one more TTL (counted from `max(when it saw the role
///    gone, when it saw this version)`) and then CASes `N`'s `delegated` record to
///    `expired`.
///
/// Must see: every range of `N` moves to `M` no earlier than ~2·TTL and within 3·TTL +
/// a margin; no two leaders.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_and_node_die_together_ranges_move_within_three_ttl() -> Result<()> {
    let (_bucket, nodes, c, leaders) = settled("both").await?;
    let r = async {
        let n = delegator_with_ranges(&leaders, c);
        let mine: Vec<u32> = leaders.iter().filter(|(_, i)| **i == n).map(|(r, _)| *r).collect();
        freeze(&nodes[c].store);
        freeze(&nodes[n].store);
        let t0 = Instant::now();
        let (first, all) = until_moved(&nodes, &[c, n], &mine, t0, TTL * 5).await?;
        ensure!(
            first >= TTL * 2 - TTL / 4,
            "the delegating node's ranges taken after {first:?} — before the coordinator's role had been gone a TTL"
        );
        ensure!(
            all <= TTL * 3 + TTL / 2,
            "the delegating node's ranges moved after {all:?} — over 3·TTL"
        );
        Ok(())
    }
    .await;
    shutdown(nodes).await;
    r
}

/// **A coordinator opening a new incarnation loses the role; the cluster re-delegates
/// or falls back, nobody loses a session.**
///
/// 1. Three nodes, finished delegating to `C` at term `t`.
/// 2. The test CASes `expired` onto `C`'s session record (like an observer reading a
///    stale copy): `C`'s next renewal fails, `C` loses its session and opens a new
///    incarnation; the term-`t` role does not move to the new incarnation.
/// 3. `C`'s fence sees the incarnation change: no more acks. The delegating nodes lose
///    their acks and fall back or re-delegate to a term > `t`.
///
/// Must see: within 6·TTL every node other than `C` has a `direct` record or one
/// `delegated` to a term > `t`; no node other than `C` changes incarnation; no two
/// leaders on any poll.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_reopening_incarnation_drops_the_role() -> Result<()> {
    let (bucket, nodes, c, _) = settled("reopen").await?;
    let r = async {
        let first = coordinator_of(&bucket).await.expect("a coordinator exists");
        let incs: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();
        let cid = incs[c].clone().expect("C alive");
        let path = Path::from(format!("sessions/{}/{}", cid.node, cid.inc));
        let got = bucket.get(&path).await?;
        let version = slatedb::object_store::UpdateVersion {
            e_tag: got.meta.e_tag.clone(),
            version: got.meta.version.clone(),
        };
        let text = String::from_utf8(got.bytes().await?.to_vec())?;
        let expired = text.replace("state=live", "state=expired");
        slatedb::object_store::ObjectStore::put_opts(
            &**bucket,
            &path,
            expired.into(),
            slatedb::object_store::PutMode::Update(version).into(),
        )
        .await?;
        let refs: Vec<_> = nodes.iter().collect();
        let t0 = Instant::now();
        loop {
            no_two_leaders(&leaders_by_probe(&refs).await)?;
            let mut ok = true;
            for (i, n) in nodes.iter().enumerate() {
                if i == c {
                    continue;
                }
                ensure!(
                    n.node.session.current() == incs[i],
                    "node {i} lost its session when the coordinator changed incarnation"
                );
                let rec = record_of(&bucket, n).await.unwrap_or_default();
                let old_term = rec
                    .lines()
                    .any(|l| l.starts_with("coord=") && l.ends_with(&format!(":{}", first.term)));
                ok &= !old_term;
            }
            if ok && n_coordinator_moved(&bucket, &first).await {
                break;
            }
            ensure!(
                t0.elapsed() < TTL * 6,
                "after 6·TTL some node still delegates to the old role (term {})",
                first.term
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    }
    .await;
    shutdown(nodes).await;
    r
}

/// `control/coordinator` has left the role `first` (a later term, or released).
async fn n_coordinator_moved(bucket: &common::Bucket, first: &common::CoordinatorSeen) -> bool {
    coordinator_of(bucket)
        .await
        .is_some_and(|s| s.term > first.term || s.released)
}

/// **A late (paused) coordinator renewal does not make the whole cluster fall back**
/// (the first ack rule: no demand for the coordinator's `alive()` — in the spirit of
/// KAFKA-15441).
///
/// 1. Three nodes, finished delegating.
/// 2. At `t0`, every PUT to `C`'s session record blocks at a gate; the gate opens at
///    `t0 + 0.9·TTL`. `C`'s grant expires ~`⅔·TTL` after the last renewal — **paused**,
///    not yet lost (bound `4/3·TTL`); after the gate opens, the next renewal wins its
///    CAS from the right previous version and revives it (LS-2).
/// 3. While paused `C` still fences (GET: its record still `live`) and acks.
///
/// Since `solo` exists `C` is usually already in `solo` when the test starts (no
/// renewal PUTs): then the test only checks that blocking `C`'s PUTs does not make the
/// cluster fall back — the pause path is not touched. A pause of `C` in `solo` is a
/// failed self-check GET, on the same object as its fence, so the fence fails too and
/// the cluster falls back — as with a failed fence in `direct`.
///
/// Must see: no delegating node PUTs a renewal (falls back) within 3·TTL; `C` keeps its
/// incarnation (revived, not lost).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_renewal_pause_does_not_fallback_the_cluster() -> Result<()> {
    let (_bucket, nodes, c, _) = settled("pause").await?;
    let r = async {
        let incs: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();
        let cid = incs[c].clone().expect("C alive");
        let puts: Vec<u64> = nodes
            .iter()
            .map(|n| source_count(n, "session.renew", Op::Put))
            .collect();
        let gate = cairn_store::testkit::Gate::closed();
        let held = nodes[c].store.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains(format!("sessions/{}/{}", cid.node, cid.inc)),
            Action::Gate(gate.clone()),
        ));
        tokio::time::sleep(TTL.mul_f32(0.9)).await;
        nodes[c].store.remove_rule(held);
        gate.open();
        tokio::time::sleep(TTL * 2).await;
        for (i, n) in nodes.iter().enumerate() {
            if i == c {
                continue;
            }
            let sent = source_count(n, "session.renew", Op::Put) - puts[i];
            ensure!(
                sent == 0,
                "node {i} fell back to direct ({sent} PUTs) while the coordinator only paused its renewal"
            );
        }
        ensure!(
            nodes[c].node.session.current() == incs[c],
            "the coordinator lost its session because of one renewal paused for 0.9·TTL"
        );
        Ok(())
    }
    .await;
    shutdown(nodes).await;
    r
}

/// **A slow coordinator releases the role, and does not take it back at once** (the
/// third expire rule).
///
/// 1. Three nodes (the slowness-score loop shortened: 1 s rounds), the gate on without
///    the delegation flag (see the test body for why), coordinator `C` at term `t`.
/// 2. Every PUT of `C` is slowed by 300 ms (> `slow_put` 150 ms): the slowness score
///    reaches its cap after a few rounds — `C` is "weak" (Lifeguard); weak for more
///    than a TTL, it releases the role.
///
/// Must see: `control/coordinator` moves to `released` or a term > `t` within 12 s (the
/// score doubles every round with a timeout: ~7 rounds to the cap, plus one weak TTL);
/// the new role (if any) is not an incarnation of `C` for
/// [`cairn_store::RETAKE_AFTER_TTLS`] TTLs after that; no node other than `C` loses its
/// session; no two leaders.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_coordinator_releases_and_is_not_retaken_at_once() -> Result<()> {
    let bucket = common::bucket();
    // The gate on **without** the delegation flag: once the cluster delegates
    // completely `C` is in `solo` — no renewal PUTs, so slow PUTs would not show while
    // idle (a known limitation). The rule of releasing the role when weak does not
    // depend on the mode; here every node is `direct`, so `C` still has PUT samples to
    // measure.
    quiet_lease_on_without_delegation(&bucket).await?;
    let mut nodes = Vec::new();
    for id in ["slowc-a", "slowc-b", "slowc-c"] {
        nodes.push(
            start_cluster_node_tweaked(&bucket, id, 4, TTL, |cfg| {
                cfg.slow = cairn_server::slow::SlowConfig {
                    round: Duration::from_secs(1),
                    slow_put: Duration::from_millis(150),
                    decay: Duration::from_secs(3),
                    recover_for: Duration::from_secs(3),
                    marker_ttl: Duration::from_secs(10),
                    shed_gap: Duration::from_millis(300),
                    ..Default::default()
                };
            })
            .await?,
        );
    }
    let r = async {
        let c = coordinator_index(&bucket, &nodes).await?;
        let first = coordinator_of(&bucket).await.expect("a coordinator exists");
        let incs: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();
        nodes[c].store.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Any,
            Action::Delay(Duration::from_millis(300)),
        ));
        let refs: Vec<_> = nodes.iter().collect();
        let t0 = Instant::now();
        let moved_at = loop {
            no_two_leaders(&leaders_by_probe(&refs).await)?;
            if n_coordinator_moved(&bucket, &first).await {
                break Instant::now();
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(12),
                "the slow coordinator did not release the role after 12 s"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        while moved_at.elapsed() < TTL * cairn_store::RETAKE_AFTER_TTLS - TTL / 2 {
            let now = coordinator_of(&bucket).await;
            ensure!(
                !now.as_ref().is_some_and(|s| !s.released
                    && s.term > first.term
                    && Some(s.session.clone()) == incs[c].as_ref().map(|i| i.to_string())),
                "the coordinator took back the role it had just released: {now:?}"
            );
            for (i, n) in nodes.iter().enumerate() {
                ensure!(
                    i == c || n.node.session.current() == incs[i],
                    "node {i} lost its session when the coordinator released the role"
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    }
    .await;
    shutdown(nodes).await;
    r
}

/// **A session of an old binary (`knows` < `lease/quiet`) slipping in moves the whole
/// cluster back to `direct`** (the gate's runtime rule).
///
/// 1. Three nodes, finished delegating.
/// 2. The test builds a `live` record `sessions/oldbin/1` with `knows=2` (like an old
///    binary that passed `min_binary` before the gate was turned on), renewing it every
///    TTL/5 (beat + 1) so that it is not expired.
/// 3. The coordinator's tracker LIST sees it (≤ one TTL) and GETs its `knows`.
///
/// Must see: within 3·TTL every node has a `direct` record, and keeps it for 2·TTL
/// after that; no node changes incarnation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_binary_session_forces_cluster_fallback() -> Result<()> {
    let (bucket, nodes, _, _) = settled("oldbin").await?;
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let fake = {
        let (bucket, stop) = (bucket.clone(), stop.clone());
        tokio::spawn(async move {
            let path = Path::from("sessions/oldbin/1");
            let mut beat = 1u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let body =
                    format!("cairnflow-session/1\nnode=oldbin\nbeat={beat}\nstate=live\nknows=2\n");
                let _ = bucket.put(&path, body.into()).await;
                beat += 1;
                tokio::time::sleep(TTL / 5).await;
            }
            let body =
                format!("cairnflow-session/1\nnode=oldbin\nbeat={beat}\nstate=closed\nknows=2\n");
            let _ = bucket.put(&path, body.into()).await;
        })
    };
    let r = async {
        let incs: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();
        let t0 = Instant::now();
        loop {
            let mut all = true;
            for n in &nodes {
                all &= direct(&bucket, n).await;
            }
            if all {
                break;
            }
            ensure!(
                t0.elapsed() < TTL * 3,
                "an old binary in the cluster yet nodes still delegate after 3·TTL"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let t1 = Instant::now();
        while t1.elapsed() < TTL * 2 {
            for (i, n) in nodes.iter().enumerate() {
                ensure!(
                    direct(&bucket, n).await,
                    "node {i} delegated again while the old binary is still alive"
                );
                ensure!(
                    n.node.session.current() == incs[i],
                    "node {i} changed incarnation"
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }
    .await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = fake.await;
    shutdown(nodes).await;
    r.map_err(|e: anyhow::Error| anyhow!("{e}"))
}

/// **The gate on without the `--delegation` flag ⇒ nobody delegates** (delegation can
/// only be turned on separately, once the gate's evidence is complete; the lone-node
/// mode can still be turned on).
///
/// 1. `control/quiet-lease` = `on` **without** a `delegation=on` line (like
///    `cairn-ctl cluster enable-quiet-lease --apply` without `--delegation`).
/// 2. Three nodes, four ranges, TTL 1 s; wait for one leader per range, then 3·TTL.
///
/// Must see: throughout the 3·TTL after that no session record has `mode=delegated`;
/// every node other than the coordinator still PUTs renewals.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gate_without_delegation_flag_keeps_the_cluster_direct() -> Result<()> {
    let bucket = common::bucket();
    bucket
        .put(
            &Path::from("control/quiet-lease"),
            "cairnflow-quiet-lease/1\nstate=on\nknows=3\nby=test at_ms=0\n".into(),
        )
        .await?;
    let mut nodes = Vec::new();
    for id in ["noflag-a", "noflag-b", "noflag-c"] {
        nodes.push(start_cluster_node(&bucket, id, 4, TTL).await?);
    }
    let r = async {
        let refs: Vec<_> = nodes.iter().collect();
        wait_probe(
            &refs,
            Duration::from_secs(20),
            "one leader per range",
            |p| single_leaders(p).is_some(),
        )
        .await?;
        tokio::time::sleep(TTL * 3).await;
        let t0 = Instant::now();
        while t0.elapsed() < TTL * 3 {
            for (i, n) in nodes.iter().enumerate() {
                ensure!(
                    !delegated(&bucket, n).await,
                    "node {i} delegates although the gate has no --delegation flag: {:?}",
                    record_of(&bucket, n).await
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    }
    .await;
    shutdown(nodes).await;
    r
}
