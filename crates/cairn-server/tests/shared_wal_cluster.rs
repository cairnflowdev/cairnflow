// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The per-node shared WAL on **real nodes**: two nodes on one bucket. Every
//! cluster test runs on the shared WAL (it is the only WAL); this file asserts
//! what is specific to the shared WAL: writes go into the node log, failover
//! replays from the dead node's log, and after a clean hand-off the receiver
//! does not read the old log.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{ensure, Result},
    cairn_store::{
        testkit::{Op, PathClass},
        PartitionId, WalKind,
    },
    common::{
        fake_sdk::{FakeSdk, Step},
        freeze, load_of, single_leaders, start_cluster_node, wait_probe, ClusterNode,
    },
    slatedb::object_store::{memory::InMemory, path::Path, ObjectStore},
    std::{collections::BTreeMap, sync::Arc, time::Duration},
};

const TTL: Duration = Duration::from_secs(1);
const PARTITIONS: u16 = 4;

async fn two_shared_nodes() -> Result<(common::Bucket, Vec<ClusterNode>)> {
    let bucket = common::bucket();
    let mut nodes = Vec::new();
    for id in ["node-alpha", "node-beta"] {
        nodes.push(start_cluster_node(&bucket, id, PARTITIONS, TTL).await?);
    }
    Ok((bucket, nodes))
}

/// One completed invocation per partition, called through its leader. Returns
/// the ids by partition.
async fn one_invocation_per_partition(
    nodes: &[ClusterNode],
    leaders: &BTreeMap<u32, usize>,
    sdk: &FakeSdk,
) -> Result<BTreeMap<u32, String>> {
    for n in nodes {
        common::register(&n.node.url(), &sdk.url).await?;
    }
    let mut ids = BTreeMap::new();
    for (p, i) in leaders {
        let leader = &nodes[*i].node;
        let key = common::key_on_partition(leader, "Counter", *p, "k");
        let (code, body) = common::invoke(&leader.url(), "Counter", "get", Some(&key), "x").await?;
        ensure!(
            code == 200 && body["output"] == "ok",
            "partition {p}: {code} {body}"
        );
        ids.insert(*p, body["id"].as_str().unwrap_or_default().to_string());
    }
    Ok(ids)
}

async fn owner(bucket: &Arc<InMemory>, p: u32) -> Result<cairn_store::OwnerRecord> {
    let store: Arc<dyn ObjectStore> = bucket.clone();
    cairn_store::read_owner(&store, &Path::from(""), PartitionId(p))
        .await?
        .ok_or_else(|| anyhow::anyhow!("partition {p} has no owner record"))
}

/// Writes go into the node log, not the root WAL; node beta dies ⇒ alpha seals
/// beta's log, replays it, and every completed invocation is still readable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failover_replays_from_dead_node_log() -> Result<()> {
    let (bucket, nodes) = two_shared_nodes().await?;
    let refs: Vec<_> = nodes.iter().collect();
    let leaders = wait_probe(&refs, Duration::from_secs(20), "balanced 2/2", |p| {
        single_leaders(p).is_some_and(|m| load_of(&m, 2) == [2, 2])
    })
    .await?;
    let leaders = single_leaders(&leaders).expect("leaders present");
    let sdk = FakeSdk::builder()
        .object("Counter", "get", vec![Step::output("ok")])
        .spawn()
        .await?;
    let before: Vec<_> = nodes.iter().map(|n| n.store.counts()).collect();
    let ids = one_invocation_per_partition(&nodes, &leaders, &sdk).await?;
    for (i, n) in nodes.iter().enumerate() {
        let d = n.store.counts().since(&before[i]);
        ensure!(
            d.get(Op::Put, PathClass::Wal) == 0,
            "node {i} wrote the root WAL: {d:?}"
        );
    }
    let nodewal_puts: u64 = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| {
            n.store
                .counts()
                .since(&before[i])
                .get(Op::Put, PathClass::NodeWal)
        })
        .sum();
    ensure!(nodewal_puts > 0, "no PUT into the node log");
    let beta_had: Vec<u32> = leaders
        .iter()
        .filter(|(_, i)| **i == 1)
        .map(|(p, _)| *p)
        .collect();
    ensure!(beta_had.len() == 2, "beta holds 2 ranges: {leaders:?}");
    for p in &beta_had {
        let rec = owner(&bucket, *p).await?;
        ensure!(
            rec.wal_kind == WalKind::Shared && !rec.wal_segments.is_empty(),
            "partition {p}: {rec:?}"
        );
    }

    freeze(&nodes[1].store);
    wait_probe(
        &refs[..1],
        TTL * 5 + Duration::from_secs(5),
        "alpha takes all 4",
        |probe| probe.values().all(|v| v == &[0]),
    )
    .await?;
    let alpha = &nodes[0].node;
    for (p, id) in &ids {
        let st = common::wait_status(&alpha.url(), id, "completed").await?;
        ensure!(st["output"] == "ok", "partition {p}: {st}");
    }
    for p in &beta_had {
        let rec = owner(&bucket, *p).await?;
        ensure!(
            rec.wal_segments.len() == 1 && rec.wal_segments[0].log.session.node == "node-alpha",
            "partition {p} still has beta's segment after alpha took it: {rec:?}"
        );
    }
    let mut nodes = nodes.into_iter();
    nodes.next().expect("alpha").node.shutdown().await;
    Ok(())
}

/// Beta shuts down cleanly ⇒ each of its ranges seals and then hands off
/// **cleanly** (no `wal_segment` left) ⇒ alpha takes over without reading any
/// object of the node log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_shutdown_hands_off_without_replay() -> Result<()> {
    let (bucket, nodes) = two_shared_nodes().await?;
    let refs: Vec<_> = nodes.iter().collect();
    let leaders = wait_probe(&refs, Duration::from_secs(20), "balanced 2/2", |p| {
        single_leaders(p).is_some_and(|m| load_of(&m, 2) == [2, 2])
    })
    .await?;
    let leaders = single_leaders(&leaders).expect("leaders present");
    let sdk = FakeSdk::builder()
        .object("Counter", "get", vec![Step::output("ok")])
        .spawn()
        .await?;
    let ids = one_invocation_per_partition(&nodes, &leaders, &sdk).await?;
    let beta_had: Vec<u32> = leaders
        .iter()
        .filter(|(_, i)| **i == 1)
        .map(|(p, _)| *p)
        .collect();
    ensure!(beta_had.len() == 2, "beta holds 2 ranges: {leaders:?}");

    let mut it = nodes.into_iter();
    let alpha = it.next().expect("alpha");
    let beta = it.next().expect("beta");
    let before = alpha.store.counts();
    beta.node.shutdown().await;
    // A **clean** hand-off: no WAL segment still points into beta's log. Alpha
    // may already have taken the range back right after the hand-off (the record
    // is already alpha's, a new term, not `released`) — reading the record races
    // with that claim, so `released` is only required while the record is still
    // beta's. The "no node log read" check below proves the rest.
    for p in &beta_had {
        let rec = owner(&bucket, *p).await?;
        let beta_log = rec
            .wal_segments
            .iter()
            .any(|s| s.log.session.node == "node-beta");
        ensure!(
            !beta_log && (rec.session.node != "node-beta" || rec.released),
            "partition {p}: {rec:?}"
        );
    }
    wait_probe(
        &[&alpha],
        TTL * 5 + Duration::from_secs(5),
        "alpha takes all 4",
        |probe| probe.values().all(|v| v == &[0]),
    )
    .await?;
    let d = alpha.store.counts().since(&before);
    ensure!(
        d.get(Op::Get, PathClass::NodeWal) + d.get(Op::Head, PathClass::NodeWal) == 0,
        "alpha read the node log after a clean hand-off: {d:?}"
    );
    for (p, id) in &ids {
        let st = common::wait_status(&alpha.node.url(), id, "completed").await?;
        ensure!(st["output"] == "ok", "partition {p}: {st}");
    }
    alpha.node.shutdown().await;
    Ok(())
}

/// A node on the shared WAL exposes its floor age on `/metrics`
/// (`cairnflow_nodewal_floor_age_seconds{node}`) — present even when no range
/// pins the floor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_expose_nodewal_floor_age() -> Result<()> {
    let (_bucket, nodes) = two_shared_nodes().await?;
    let refs: Vec<_> = nodes.iter().collect();
    wait_probe(&refs, Duration::from_secs(20), "balanced 2/2", |p| {
        single_leaders(p).is_some_and(|m| load_of(&m, 2) == [2, 2])
    })
    .await?;
    let http = reqwest::Client::new();
    for (n, id) in nodes.iter().zip(["node-alpha", "node-beta"]) {
        let want = format!("cairnflow_nodewal_floor_age_seconds{{node=\"{id}\"}} ");
        let mut text = String::new();
        for _ in 0..50 {
            text = http
                .get(format!("{}/metrics", n.node.url()))
                .send()
                .await?
                .text()
                .await?;
            if text.contains(&want) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        ensure!(text.contains(&want), "{id}: {want:?} missing from /metrics");
        // Only its own node — the harness runs two nodes in one process.
        let other = if id == "node-alpha" {
            "node-beta"
        } else {
            "node-alpha"
        };
        ensure!(
            !text.contains(&format!(
                "cairnflow_nodewal_floor_age_seconds{{node=\"{other}\"}}"
            )),
            "{id}: /metrics leaks the floor of {other}"
        );
    }
    Ok(())
}

/// **Split when the "clean record" CAS after a seal fails** — another path to
/// the same error `500 "Projection and/or union with WAL is not supported"`: the
/// store is flaky exactly at seal time (chaos `--failpoints`).
///
/// Interleaving:
///
/// 1. One node, shared WAL, a range P with one acked invocation: P's owner
///    record carries a `wal_segment` pointing into the node's log.
/// 2. The store fails exactly **one** next PUT to `owners/range-P`. Between
///    the ack and the split the keeper does not write the owner record at all,
///    so that PUT is `writer_sealed`'s CAS that drops the segment.
/// 3. `POST /ranges/P/split`: the coordinator CASes the intent, drains,
///    quiesces. P's seal completes, the CAS that drops `wal_segment` fails and
///    only gets a `warn`; quiesce still returns `Ok`.
/// 4. The clone asks `SharedWalAdmin::is_empty`, sees the owner record still
///    has a segment, and refuses the source.
///
/// Must see: the split returns `503` (retryable), not `500`; the table has no
/// intent left; calling it again completes the split; the acked invocation is
/// intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_when_clean_owner_cas_fails_after_seal_returns_503() -> Result<()> {
    use cairn_store::testkit::{Action, OpSet, PathMatch, Rule};
    let bucket = common::bucket();
    let node = start_cluster_node(&bucket, "node-solo", 1, TTL).await?;
    let result = common::within(Duration::from_secs(90), "split, clean CAS fails", async {
        wait_probe(
            &[&node],
            Duration::from_secs(20),
            "node leads range 0",
            |p| p.get(&0).is_some_and(|v| v == &[0]),
        )
        .await?;
        let sdk = FakeSdk::builder()
            .object("Counter", "get", vec![Step::output("ok")])
            .spawn()
            .await?;
        common::register(&node.node.url(), &sdk.url).await?;
        let (code, body) =
            common::invoke(&node.node.url(), "Counter", "get", Some("k"), "x").await?;
        ensure!(code == 200 && body["output"] == "ok", "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        let rec = owner(&bucket, 0).await?;
        ensure!(
            rec.wal_kind == WalKind::Shared && !rec.wal_segments.is_empty(),
            "range 0 must be writing into the node log — otherwise the test checks nothing: {rec:?}"
        );

        node.store.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains("owners/range-0".into()),
            Action::Fail { times: Some(1) },
        ));
        let http = reqwest::Client::new();
        let url = format!("{}/ranges/0/split", node.node.url());
        let resp = http.post(&url).send().await?;
        let code = resp.status().as_u16();
        let text = resp.text().await?;
        ensure!(
            code == 503,
            "split when the clean CAS fails must return 503: {code} {text}"
        );
        ensure!(
            node.node
                .cluster
                .table()
                .ranges()
                .iter()
                .all(|r| r.intent.is_none()),
            "intent still pending: {:?}",
            node.node.cluster.table()
        );

        let t0 = std::time::Instant::now();
        loop {
            let resp = http.post(&url).send().await?;
            let code = resp.status().as_u16();
            let text = resp.text().await?;
            if code == 200 {
                break;
            }
            ensure!(code == 503, "next split: {code} {text}");
            ensure!(
                t0.elapsed() < Duration::from_secs(30),
                "split did not complete: {code} {text}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        ensure!(
            node.node.cluster.table().ranges().len() == 2,
            "{:?}",
            node.node.cluster.table()
        );
        let st = common::wait_status(&node.node.url(), &id, "completed").await?;
        ensure!(st["output"] == "ok", "{st}");
        Ok(())
    })
    .await;
    node.node.shutdown().await;
    result
}

/// Poll until `ok` holds or `deadline` passes, every 50 ms.
async fn until(deadline: Duration, what: &str, mut ok: impl FnMut() -> bool) -> Result<()> {
    let t0 = std::time::Instant::now();
    while !ok() {
        ensure!(t0.elapsed() < deadline, "over {deadline:?}: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

/// Shared frame of the two stuck-clean-CAS tests: alpha hands off a range that
/// is writing into the node log while the "clean record" CAS of `writer_sealed`
/// (the **first** PUT to `owners/range-P` since the rule was set) is `stuck`;
/// every later PUT goes through.
///
/// Must see: the range has a leader again within a bound of a few seconds — not
/// held until that PUT returns (real S3 client: up to 180 s) — and no node
/// fail-stops.
async fn resign_while_clean_owner_cas_is_stuck(stuck: cairn_store::testkit::Action) -> Result<()> {
    use cairn_store::testkit::{Action, OpSet, PathMatch, Rule};
    let (bucket, nodes) = two_shared_nodes().await?;
    let refs: Vec<_> = nodes.iter().collect();
    let result = common::within(Duration::from_secs(90), "hand-off, clean CAS stuck", async {
        let leaders = wait_probe(&refs, Duration::from_secs(20), "balanced 2/2", |p| {
            single_leaders(p).is_some_and(|m| load_of(&m, 2) == [2, 2])
        })
        .await?;
        let leaders = single_leaders(&leaders).expect("leaders present");
        let sdk = FakeSdk::builder()
            .object("Counter", "get", vec![Step::output("ok")])
            .spawn()
            .await?;
        let ids = one_invocation_per_partition(&nodes, &leaders, &sdk).await?;
        let p = *leaders
            .iter()
            .find(|(_, i)| **i == 0)
            .expect("alpha holds a range")
            .0;
        let rec = owner(&bucket, p).await?;
        ensure!(
            !rec.wal_segments.is_empty(),
            "range {p} must be writing into the node log — otherwise the test checks nothing: {rec:?}"
        );
        let sessions: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();

        nodes[0].store.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains(format!("owners/range-{p}")),
            Action::EachOnce(Box::new(stuck)),
        ));
        let h = nodes[0]
            .node
            .partitions
            .get(PartitionId(p))
            .expect("p has an engine");
        let resign = tokio::spawn(async move { h.resign().await });
        wait_probe(
            &refs,
            TTL * 3 + Duration::from_secs(5),
            "range has a leader again",
            |probe| probe[&p].len() == 1,
        )
        .await?;
        until(Duration::from_secs(5), "Resign answers", || {
            resign.is_finished()
        })
        .await?;
        let rec = owner(&bucket, p).await?;
        ensure!(
            !rec.released,
            "range {p} has a leader yet the owner record is still `released`: {rec:?}"
        );
        let now: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();
        ensure!(now == sessions, "a node fail-stopped: {sessions:?} → {now:?}");
        let id = &ids[&p];
        for n in &nodes {
            if let Ok(st) = common::wait_status(&n.node.url(), id, "completed").await {
                ensure!(st["output"] == "ok", "range {p}: {st}");
                return Ok(());
            }
        }
        anyhow::bail!("acked invocation of range {p} no longer readable")
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    result
}

/// **The "clean record" CAS after a seal hangs.**
///
/// Interleaving:
/// 1. Two shared-WAL nodes, balanced 2/2; each range has one acked invocation.
/// 2. Alpha's next PUT to `owners/range-P` waits at a gate that never opens.
/// 3. `Resign` P: the seal completes, `writer_sealed` CASes the clean record and
///    hangs at the gate.
///
/// Must see: after a bound, the keeper abandons that CAS and hands off (with the
/// clean flag set) — P has a leader within a few seconds. Before the fix,
/// `step_down` waited for the CAS without a bound: the owner record still
/// pointed at alpha's live session, nobody challenged, and P had no leader for
/// as long as the PUT hung.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hung_clean_owner_cas_does_not_hold_resigned_range() -> Result<()> {
    use cairn_store::testkit::{Action, Gate};
    resign_while_clean_owner_cas_is_stuck(Action::Gate(Gate::closed())).await
}

/// **Stuck clean CAS, ambiguous variant:** the clean-record CAS **was written**
/// but the answer comes back late (past the bound). The keeper stops waiting with
/// the old version in hand; the hand-off after it misses its CAS on our own
/// record.
///
/// Must see: P has a leader within a few seconds. Here alpha usually takes P
/// back itself (it is within its fair share), so this test only keeps the
/// "bounded" part; the "the hand-off recognises our own record" part is kept by
/// `cairn-store` `session::tests::release_after_unseen_own_write_still_releases`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_reply_of_clean_owner_cas_does_not_strand_resigned_range() -> Result<()> {
    use cairn_store::testkit::Action;
    resign_while_clean_owner_cas_is_stuck(Action::DelayResponse(Duration::from_secs(60))).await
}

/// **The seal bound at hand-off must exceed the shared WAL's `stall_max` budget
/// plus one memtable flush.**
///
/// Interleaving:
/// 1. Two shared-WAL nodes, log PUT limit 6 s (`stall_max` = 18 s); balanced
///    2/2, each range has one acked invocation.
/// 2. From here on every log PUT of alpha is 5.5 s slow — below the PUT limit,
///    so the WAL stream tolerates it and does not fail-stop.
/// 3. `Resign` alpha's P: the seal writes a fence and waits until it is durable
///    (≥ 5.5 s), then flushes the memtable.
///
/// Must see: the seal completes, the hand-off is **clean**, P has a leader, and
/// no node changes incarnation. Before the fix, the 5 s `SEAL_TIMEOUT` ran out
/// before the fence was durable: the dirty hand-off kept a segment pointing at
/// alpha's **live** log, the receiver sealed it, and alpha's next PUT hit
/// `AlreadyExists` ⇒ the whole alpha node fail-stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_wal_put_within_budget_does_not_turn_resign_into_fail_stop() -> Result<()> {
    use cairn_store::testkit::{Action, Rule};
    let bucket = common::bucket();
    let mut nodes = Vec::new();
    for id in ["node-alpha", "node-beta"] {
        nodes.push(
            common::start_cluster_node_tweaked(&bucket, id, PARTITIONS, TTL, |cfg| {
                cfg.node_wal.put_timeout = Duration::from_secs(6);
            })
            .await?,
        );
    }
    let refs: Vec<_> = nodes.iter().collect();
    let result = common::within(Duration::from_secs(120), "hand-off, slow log PUT", async {
        let leaders = wait_probe(&refs, Duration::from_secs(20), "balanced 2/2", |p| {
            single_leaders(p).is_some_and(|m| load_of(&m, 2) == [2, 2])
        })
        .await?;
        let leaders = single_leaders(&leaders).expect("leaders present");
        let sdk = FakeSdk::builder()
            .object("Counter", "get", vec![Step::output("ok")])
            .spawn()
            .await?;
        let ids = one_invocation_per_partition(&nodes, &leaders, &sdk).await?;
        let p = *leaders
            .iter()
            .find(|(_, i)| **i == 0)
            .expect("alpha holds a range")
            .0;
        let sessions: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();

        let slow = nodes[0].store.add_rule(Rule::nodewal_put(
            Some("node-alpha"),
            Action::Delay(Duration::from_millis(5500)),
        ));
        let h = nodes[0]
            .node
            .partitions
            .get(PartitionId(p))
            .expect("p has an engine");
        let resigned = h.resign().await;
        nodes[0].store.remove_rule(slow);
        wait_probe(
            &refs,
            Duration::from_secs(20),
            "range has a leader again",
            |probe| probe[&p].len() == 1,
        )
        .await?;
        // A fail-stop follows the receiver's seal by one PUT beat: keep watching
        // a few more seconds.
        let t0 = std::time::Instant::now();
        while t0.elapsed() < Duration::from_secs(8) {
            let now: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();
            ensure!(
                now == sessions,
                "node fail-stopped after the hand-off (seal: {resigned:?}): {sessions:?} → {now:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        ensure!(
            resigned.is_ok(),
            "seal failed though log PUTs stayed within the limit: {resigned:?}"
        );
        for n in &nodes {
            if let Ok(st) = common::wait_status(&n.node.url(), &ids[&p], "completed").await {
                ensure!(st["output"] == "ok", "range {p}: {st}");
                return Ok(());
            }
        }
        anyhow::bail!("acked invocation of range {p} no longer readable")
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    result
}

/// **An ambiguous shared-WAL log PUT does not bring the node down** (the log
/// on an `atomic_put` store reads back and compares the bytes). Before that the
/// node fail-stopped (`cairnflow_failstop_total{cause="wal_fenced"|"wal_ambiguous"}`)
/// and every range of it had to be claimed again.
///
/// Interleaving:
/// 1. Two shared-WAL nodes, balanced 2/2, one acked invocation per range.
/// 2. Alpha's next log PUT reaches the store and then returns an ambiguous
///    answer: (a) an error; (b) `AlreadyExists` from object_store's own retry.
/// 3. A new invocation on a range alpha leads.
///
/// Must see: the new invocation completes and is readable; no node changes
/// incarnation in the 8 s after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ambiguous_wal_put_does_not_fail_stop_the_node() -> Result<()> {
    use cairn_store::testkit::{Action, Ambiguity, Rule};
    for reply in [Ambiguity::Error, Ambiguity::Retried] {
        let (bucket, nodes) = two_shared_nodes().await?;
        let refs: Vec<_> = nodes.iter().collect();
        let result = common::within(Duration::from_secs(90), "ambiguous log PUT", async {
            let leaders = wait_probe(&refs, Duration::from_secs(20), "balanced 2/2", |p| {
                single_leaders(p).is_some_and(|m| load_of(&m, 2) == [2, 2])
            })
            .await?;
            let leaders = single_leaders(&leaders).expect("leaders present");
            let sdk = FakeSdk::builder()
                .object("Counter", "get", vec![Step::output("ok")])
                .spawn()
                .await?;
            one_invocation_per_partition(&nodes, &leaders, &sdk).await?;
            let p = *leaders
                .iter()
                .find(|(_, i)| **i == 0)
                .expect("alpha holds a range")
                .0;
            let sessions: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();
            nodes[0].store.add_rule(Rule::nodewal_put(
                Some("node-alpha"),
                Action::Ambiguous {
                    reply,
                    skip: 0,
                    times: Some(1),
                },
            ));
            let alpha = &nodes[0].node;
            let key = common::key_on_partition(alpha, "Counter", p, "after-ambiguity");
            let (code, body) =
                common::invoke(&alpha.url(), "Counter", "get", Some(&key), "x").await?;
            ensure!(
                code == 200 && body["output"] == "ok",
                "{reply:?}: invocation on range {p} after an ambiguous PUT: {code} {body}"
            );
            let t0 = std::time::Instant::now();
            while t0.elapsed() < Duration::from_secs(8) {
                let now: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();
                ensure!(
                    now == sessions,
                    "{reply:?}: node fail-stopped on its own ambiguous log PUT: {sessions:?} → {now:?}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(())
        })
        .await;
        for n in nodes {
            n.node.shutdown().await;
        }
        drop(bucket);
        result?;
    }
    Ok(())
}

/// **A node whose shared-WAL PUTs are all slow does not fail-stop: its ranges
/// move to another node** (seen in chaos runs: 18 `wal_stall` in 15 minutes).
///
/// Interleaving:
/// 1. Two shared-WAL nodes, `stall_max` 900 ms (PUT limit 300 ms × 3); balanced
///    2/2, one acked invocation per range.
/// 2. From here on every `nodewal/` PUT of alpha is 1.5 s late (> `stall_max`),
///    and alpha's floor PUT (`/floor/`) hangs — the id claim for a new stream on
///    rotation does not complete.
/// 3. Each range alpha leads gets an invocation ⇒ its stream stalls past
///    `stall_max` ⇒ rotation; the id claim hangs.
/// 4. Remove the floor-hang rule.
///
/// Must see: alpha does **not** change incarnation during steps 3–4; in step 3
/// every range moves to beta (alpha backs off: it does not claim back into a
/// slot that has no stream yet); every invocation acked in step 1 is still
/// readable. Before, a failed claim ⇒ alpha fail-stopped with `wal_stall`, and
/// every range of it had to be claimed again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wholly_slow_wal_puts_move_ranges_without_fail_stop() -> Result<()> {
    use cairn_store::testkit::{Action, OpSet, PathMatch, Rule};
    let bucket = common::bucket();
    let mut nodes = Vec::new();
    for id in ["node-alpha", "node-beta"] {
        nodes.push(
            common::start_cluster_node_tweaked(&bucket, id, PARTITIONS, TTL, |cfg| {
                cfg.node_wal.put_timeout = Duration::from_millis(300);
                cfg.node_wal.put_attempts = 3;
            })
            .await?,
        );
    }
    let refs: Vec<_> = nodes.iter().collect();
    let result = common::within(
        Duration::from_secs(120),
        "node with all WAL PUTs slow",
        async {
            let leaders = wait_probe(&refs, Duration::from_secs(20), "balanced 2/2", |p| {
                single_leaders(p).is_some_and(|m| load_of(&m, 2) == [2, 2])
            })
            .await?;
            let leaders = single_leaders(&leaders).expect("leaders present");
            let sdk = FakeSdk::builder()
                .object("Counter", "get", vec![Step::output("ok")])
                .spawn()
                .await?;
            let ids = one_invocation_per_partition(&nodes, &leaders, &sdk).await?;
            let sessions: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();

            nodes[0].store.add_rule(Rule::nodewal_put(
                Some("node-alpha"),
                Action::Delay(Duration::from_millis(1500)),
            ));
            let floors = nodes[0].store.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Contains("/floor/".into()),
                Action::Hang,
            ));
            let alpha = &nodes[0].node;
            let mine: Vec<u32> = leaders
                .iter()
                .filter(|(_, i)| **i == 0)
                .map(|(p, _)| *p)
                .collect();
            let calls = mine.iter().map(|p| {
                let key = common::key_on_partition(alpha, "Counter", *p, "slow");
                let url = alpha.url();
                async move {
                    // The result asks nothing: the turn may be dropped when the stream rotates.
                    let _ = tokio::time::timeout(
                        Duration::from_secs(10),
                        common::invoke(&url, "Counter", "get", Some(&key), "x"),
                    )
                    .await;
                }
            });
            let unchanged = |what: &str| -> Result<()> {
                let now: Vec<_> = nodes.iter().map(|n| n.node.session.current()).collect();
                ensure!(
                    now == sessions,
                    "{what}: node changed incarnation: {sessions:?} → {now:?}"
                );
                Ok(())
            };
            futures::future::join_all(calls).await;
            unchanged("after the stream stalled")?;
            wait_probe(
                &refs,
                Duration::from_secs(30),
                "every range moves to beta",
                |p| single_leaders(p).is_some_and(|m| load_of(&m, 2) == [0, 4]),
            )
            .await?;
            unchanged("while beta holds every range")?;
            for (p, id) in &ids {
                let st = common::wait_status(&nodes[1].node.url(), id, "completed").await?;
                ensure!(st["output"] == "ok", "range {p}: {st}");
            }
            nodes[0].store.remove_rule(floors);
            let t0 = std::time::Instant::now();
            while t0.elapsed() < Duration::from_secs(5) {
                unchanged("after removing the floor-hang rule")?;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(())
        },
    )
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    result
}
