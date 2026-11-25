// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Behaviour of the **lease layer as seen from outside** — real nodes, a shared
//! bucket, the truth asked straight from the engine.
//!
//! The tests here do not know how leases are implemented (per partition or per
//! node): they only assert what an operator can see. Written before the switch to
//! per-node leases (D20) as a fence: they were green on the old design and must stay
//! green on the new one.

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
    cairn_store::testkit::{Action, OpSet, PathMatch, Rule},
    common::{cluster_nodes, freeze, load_of, single_leaders, start_cluster_node, wait_probe},
    std::time::{Duration, Instant},
};

const TTL: Duration = Duration::from_secs(1);

/// Every partition has exactly one leader and every node holds `want` partitions.
fn balanced(want: &[usize]) -> impl Fn(&std::collections::BTreeMap<u32, Vec<usize>>) -> bool + '_ {
    move |p| single_leaders(p).is_some_and(|m| load_of(&m, want.len()) == want)
}

/// A healthy cluster does not shuffle partitions: a balanced distribution stays put.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn healthy_cluster_never_moves_partitions() -> Result<()> {
    let (_bucket, nodes) = cluster_nodes(&["node-alpha", "node-beta"], 4, TTL).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let first = wait_probe(
        &refs,
        Duration::from_secs(20),
        "balanced 2/2",
        balanced(&[2, 2]),
    )
    .await?;
    let first = single_leaders(&first).expect("balanced");
    let t0 = Instant::now();
    while t0.elapsed() < TTL * 5 {
        let now = common::leaders_by_probe(&refs).await;
        ensure!(
            single_leaders(&now).as_ref() == Some(&first),
            "distribution changed in a healthy cluster after {:?}: {first:?} → {now:?}",
            t0.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// A frozen node: its partitions move to the other node — **no earlier** than the
/// silence the lease demands, and no later than a few TTLs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frozen_node_partitions_move_after_ttl_not_before() -> Result<()> {
    let (_bucket, nodes) = cluster_nodes(&["node-alpha", "node-beta"], 4, TTL).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let first = wait_probe(
        &refs,
        Duration::from_secs(20),
        "balanced 2/2",
        balanced(&[2, 2]),
    )
    .await?;
    let first = single_leaders(&first).expect("balanced");
    let beta_had: Vec<u32> = first
        .iter()
        .filter(|(_, n)| **n == 1)
        .map(|(p, _)| *p)
        .collect();

    let t0 = Instant::now();
    freeze(&nodes[1].store);
    let mut first_move = None;
    loop {
        let probe = common::leaders_by_probe(&refs[..1]).await;
        let moved = beta_had.iter().filter(|p| probe[p].contains(&0)).count();
        if moved > 0 && first_move.is_none() {
            first_move = Some(t0.elapsed());
        }
        if moved == beta_had.len() {
            break;
        }
        ensure!(
            t0.elapsed() < TTL * 3 + Duration::from_secs(4),
            "after {:?} alpha has taken only {moved}/{} of beta's partitions",
            t0.elapsed(),
            beta_had.len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // beta's last renewal may have been up to TTL/5 before the freeze, so the lower
    // bound is TTL minus the renewal tick, minus the measurement margin.
    let earliest = first_move.expect("a partition has moved");
    ensure!(
        earliest >= TTL.mul_f32(0.75),
        "beta's partitions taken after {earliest:?} — earlier than the lease's silence"
    );
    let mut nodes = nodes.into_iter();
    nodes.next().expect("alpha").node.shutdown().await;
    Ok(())
}

/// **A node cut off from the bucket but still reachable by its peers loses its
/// authority within ⅔·TTL, and its ranges move away** (LS-2, LS-3).
///
/// Pins the behaviour from before liveness went over the network: the peers can still
/// hear the node, but only the bucket decides "dead", and the node's own grant
/// expires by its own clock, without waiting for anyone to tell it.
///
/// Interleaving:
///
/// 1. Two nodes alpha, beta, four ranges, TTL 1 s; balanced 2/2.
/// 2. t0: every request of beta to the store fails at once (no hang — unlike
///    [`frozen_node_partitions_move_after_ttl_not_before`]); beta's HTTP and internal
///    port keep running, the network between the two nodes is open. beta's last
///    winning renewal was issued before t0.
/// 3. The test probes the engines of **both** nodes every 50 ms until alpha holds
///    all of beta's ranges.
///
/// Must see: (i) beta's session grant expires (`ValidityHandle`, probed every 5 ms)
/// no later than t0 + ⅔·TTL + 250 ms (a scheduling margin when the machine is busy),
/// and beta does not answer as the leader of any range after t0 + ⅔·TTL + 400 ms (one
/// probe is capped at 300 ms) — today it stops from the very first probe, because of
/// the bucket GET read fence; (ii) no probe sees a range with two leaders; (iii) alpha
/// takes all of beta's ranges no earlier than 0.75·TTL (the lease's silence, minus
/// the renewal tick) and no later than 3·TTL + 4 s.
///
/// Pins `direct`: ⅔·TTL is the bound of renewal by PUT. In delegated mode a grant
/// lives until the failed self-check plus its validity (< 1.5·TTL) — that version is
/// `delegation.rs::node_cut_from_bucket_but_not_coordinator_loses_its_ranges`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bucket_cut_node_loses_leadership_within_two_thirds_ttl_and_ranges_move() -> Result<()> {
    let (_bucket, nodes) =
        common::cluster_nodes_tweaked(&["node-alpha", "node-beta"], 4, TTL, common::direct).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let first = wait_probe(
        &refs,
        Duration::from_secs(20),
        "balanced 2/2",
        balanced(&[2, 2]),
    )
    .await?;
    let first = single_leaders(&first).expect("balanced");
    let beta_had: Vec<u32> = first
        .iter()
        .filter(|(_, n)| **n == 1)
        .map(|(p, _)| *p)
        .collect();

    let (_, beta_grant) = nodes[1]
        .node
        .session
        .live_changes()
        .borrow()
        .clone()
        .expect("beta has a live session");
    let t0 = Instant::now();
    nodes[1].store.add_rule(Rule::new(
        OpSet::ALL,
        PathMatch::Any,
        Action::Fail { times: None },
    ));
    let grant_end = tokio::spawn(async move {
        while beta_grant.is_valid() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        t0.elapsed()
    });
    let mut beta_last_answer = None;
    let mut first_move = None;
    loop {
        let probe = common::leaders_by_probe(&refs).await;
        let at = t0.elapsed();
        for (p, who) in &probe {
            ensure!(
                who.len() <= 1,
                "range {p} has two leaders after {at:?}: {probe:?}"
            );
        }
        if probe.values().any(|who| who.contains(&1)) {
            beta_last_answer = Some(at);
        }
        let moved = beta_had.iter().filter(|p| probe[p].contains(&0)).count();
        if moved > 0 && first_move.is_none() {
            first_move = Some(at);
        }
        if moved == beta_had.len() {
            break;
        }
        ensure!(
            at < TTL * 3 + Duration::from_secs(4),
            "after {at:?} alpha has taken only {moved}/{} of beta's ranges",
            beta_had.len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Probe one more TTL: beta does not come back.
    let until = t0.elapsed() + TTL;
    while t0.elapsed() < until {
        let probe = common::leaders_by_probe(&refs).await;
        ensure!(
            probe.values().all(|who| !who.contains(&1)),
            "beta answers as leader after {:?}, while cut off from the bucket: {probe:?}",
            t0.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let grant_end = grant_end.await?;
    ensure!(
        grant_end <= TTL.mul_f32(2.0 / 3.0) + Duration::from_millis(250),
        "beta's grant valid until {grant_end:?} after being cut off from the bucket (bound ⅔·TTL)"
    );
    let deadline = TTL.mul_f32(2.0 / 3.0) + Duration::from_millis(400);
    if let Some(last) = beta_last_answer {
        ensure!(
            last <= deadline,
            "beta still answers as leader after {last:?} (bound ⅔·TTL + margin = {deadline:?})"
        );
    }
    let earliest = first_move.expect("a range has moved");
    ensure!(
        earliest >= TTL.mul_f32(0.75),
        "beta's ranges taken after {earliest:?} — earlier than the lease's silence"
    );
    println!(
        "beta's grant ended at {grant_end:?}; beta last answered at {beta_last_answer:?}; \
         the first range moved at {earliest:?}"
    );
    // beta has no store any more: shutting it down may hang on the last flush — shut down
    // only alpha.
    let mut nodes = nodes.into_iter();
    nodes.next().expect("alpha").node.shutdown().await;
    Ok(())
}

/// A node's routing table sees **a third node's claim**, within a time bound — not
/// only what it did itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routing_sees_third_party_claims_within_bound() -> Result<()> {
    let ids = ["node-alpha", "node-beta", "node-gamma"];
    let (_bucket, nodes) = cluster_nodes(&ids, 6, TTL).await?;
    let refs: Vec<_> = nodes.iter().collect();
    wait_probe(
        &refs,
        Duration::from_secs(30),
        "balanced 2/2/2",
        balanced(&[2, 2, 2]),
    )
    .await?;

    freeze(&nodes[2].store);
    let gamma_url = nodes[2].node.url();
    let alive = &refs[..2];
    let truth = wait_probe(
        alive,
        Duration::from_secs(15),
        "alpha+beta take everything",
        |p| single_leaders(p).is_some(),
    )
    .await?;
    let truth = single_leaders(&truth).expect("one leader per partition");

    let t0 = Instant::now();
    loop {
        let table = nodes[0].node.routing.snapshot();
        let agrees = truth.iter().all(|(p, i)| {
            table
                .get(p)
                .is_some_and(|l| l.node_id == ids[*i] && l.address.as_deref() != Some(&gamma_url))
        });
        if agrees {
            break;
        }
        ensure!(
            t0.elapsed() < TTL + Duration::from_secs(2),
            "alpha's routing table after {:?} still differs from the truth {truth:?}: {table:?}",
            t0.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut nodes = nodes.into_iter();
    nodes.next().expect("alpha").node.shutdown().await;
    nodes.next().expect("beta").node.shutdown().await;
    Ok(())
}

/// A failed release write must not orphan a partition: slow is allowed, losing the
/// leader forever is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_release_does_not_strand_partition() -> Result<()> {
    let (_bucket, nodes) = cluster_nodes(&["node-alpha", "node-beta"], 4, TTL).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let first = wait_probe(
        &refs,
        Duration::from_secs(20),
        "balanced 2/2",
        balanced(&[2, 2]),
    )
    .await?;
    let first = single_leaders(&first).expect("balanced");
    let p = *first
        .iter()
        .find(|(_, n)| **n == 0)
        .expect("alpha holds a partition")
        .0;

    // `s/range-p` matches p's ownership record in the lease layer
    // (`owners/range-p`), not SlateDB's data (`ranges/p/wal/...`).
    nodes[0].store.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains(format!("s/range-{p}")),
        Action::Fail { times: Some(3) },
    ));
    nodes[0]
        .node
        .partitions
        .get(cairn_store::PartitionId(p))
        .expect("p has an engine")
        .resign()
        .await?;

    wait_probe(
        &refs,
        TTL * 3 + Duration::from_secs(3),
        "p has a leader again",
        |probe| probe[&p].len() == 1,
    )
    .await?;
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// A dead node's **torn** record (GETs flip-flopping the etag, conditional PUTs
/// always failing — exactly the shape local MinIO left behind in chaos runs) must not
/// imprison its partitions. The way out goes through the bucket: alpha overwrites the
/// record (an unconditional PUT heals a torn object), waits one TTL, then the
/// `expired` CAS wins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn torn_session_of_dead_node_does_not_strand_partitions() -> Result<()> {
    let (_bucket, nodes) = cluster_nodes(&["node-alpha", "node-beta"], 4, TTL).await?;
    let refs: Vec<_> = nodes.iter().collect();
    wait_probe(
        &refs,
        Duration::from_secs(20),
        "balanced 2/2",
        balanced(&[2, 2]),
    )
    .await?;

    freeze(&nodes[1].store);
    // Only the **session** record is torn — beta's shared WAL log (`nodewal/node-beta/…`)
    // must still be sealable so that alpha can take its ranges.
    let tear = cairn_store::testkit::Tear::new();
    nodes[0].store.add_rule(Rule::new(
        OpSet::GET | OpSet::HEAD | OpSet::PUT,
        PathMatch::Contains("sessions/node-beta".into()),
        Action::Tear(tear.clone()),
    ));

    wait_probe(
        &refs[..1],
        TTL * 5 + Duration::from_secs(3),
        "alpha takes all 4",
        |probe| probe.values().all(|v| v == &[0]),
    )
    .await?;
    assert!(tear.healed(), "ranges taken, torn record not healed");
    let mut nodes = nodes.into_iter();
    nodes.next().expect("alpha").node.shutdown().await;
    Ok(())
}

/// A node joining later gets its share, then the distribution stays put.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebalance_converges_and_stays() -> Result<()> {
    let (bucket, mut nodes) = cluster_nodes(&["node-alpha"], 4, TTL).await?;
    {
        let refs: Vec<_> = nodes.iter().collect();
        wait_probe(
            &refs,
            Duration::from_secs(10),
            "alpha holds all 4",
            balanced(&[4]),
        )
        .await?;
    }
    nodes.push(start_cluster_node(&bucket, "node-beta", 4, TTL).await?);
    let refs: Vec<_> = nodes.iter().collect();
    let settled = wait_probe(
        &refs,
        Duration::from_secs(20),
        "split 2/2",
        balanced(&[2, 2]),
    )
    .await?;
    let settled = single_leaders(&settled).expect("balanced");
    let t0 = Instant::now();
    while t0.elapsed() < TTL * 4 {
        let now = common::leaders_by_probe(&refs).await;
        ensure!(
            single_leaders(&now).as_ref() == Some(&settled),
            "distribution not settled after the division: {settled:?} → {now:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// The balancer does not leave a live node idle (found while merging the balancing
/// work). Interleaving:
///
/// 1. alpha and beta come up with 4 ranges ⇒ {2, 2}.
/// 2. gamma joins. The fair share becomes ⌈4/3⌉ = 2; alpha and beta are exactly at
///    the cap, nobody is *over*, so before the fix nobody released: {2, 2, 0} was an
///    equilibrium, gamma alive but leading nothing, forever.
/// 3. The test waits through `settle_for(TTL)` (gamma counts towards the fair share)
///    plus ten TTLs (two cycles of the release loop, one handover).
///
/// Must see: the distribution becomes {2, 1, 1} with gamma holding one range, then
/// stays put for ten TTLs — not a single change of owner after converging.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_node_gets_a_range_when_ranges_do_not_divide_evenly() -> Result<()> {
    let (bucket, mut nodes) = cluster_nodes(&["node-alpha", "node-beta"], 4, TTL).await?;
    {
        let refs: Vec<_> = nodes.iter().collect();
        wait_probe(
            &refs,
            Duration::from_secs(20),
            "split 2/2",
            balanced(&[2, 2]),
        )
        .await?;
    }
    nodes.push(start_cluster_node(&bucket, "node-gamma", 4, TTL).await?);
    let refs: Vec<_> = nodes.iter().collect();
    let bound = cairn_server::balance::settle_for(TTL) + TTL * 10;
    let spread = |p: &std::collections::BTreeMap<u32, Vec<usize>>| {
        single_leaders(p).is_some_and(|m| {
            let mut load = load_of(&m, 3);
            let gamma = load[2];
            load.sort_unstable();
            gamma == 1 && load == [1, 1, 2]
        })
    };
    let settled = wait_probe(&refs, bound, "split 2/1/1, gamma holds a range", spread).await?;
    let settled = single_leaders(&settled).expect("balanced");
    // Count owner changes after converging: must be 0.
    let mut last = settled.clone();
    let mut changes = 0;
    let t0 = Instant::now();
    while t0.elapsed() < TTL * 10 {
        let now = common::leaders_by_probe(&refs).await;
        if let Some(now) = single_leaders(&now) {
            changes += now.iter().filter(|(p, n)| last.get(p) != Some(n)).count();
            last = now;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    ensure!(
        changes == 0,
        "{changes} owner changes after converging: {settled:?} → {last:?}"
    );
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// **LS-10 — the balancer's release loop releases at most one range per cycle**
/// (cycle = TTL, over the share for two cycles in a row before releasing). Releasing
/// a whole bunch at once is an ownership storm — every owner change hands over every
/// unfinished invocation of the range.
///
/// Interleaving:
/// 1. alpha alone with 8 ranges ⇒ holds all 8.
/// 2. beta joins. After `settle_for(TTL)` alpha counts beta in its fair share ⌈8/2⌉ =
///    4: alpha is 4 ranges over.
/// 3. The test polls the number of ranges alpha leads every 25 ms, until alpha has
///    ≤ 4, then waits for a 4/4 split.
///
/// Must see: alpha's balancer release log (`Balancer::releases`, recorded at the
/// **decision**, one entry per range) has two consecutive releases ≥ 1.5 TTL apart
/// (two cycles of the loop). Measured on the log, not on the observed range count:
/// the moment a range leaves alpha also adds the `resign` time (seal, up to
/// `RESIGN_WAIT`), which fluctuates under full load, and a range lost to a broken
/// lease (the "ambiguous once per op" row) is not a release — both used to make the
/// test flaky (observed gaps of 0.57 s and 1.45 s in a full check run).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn balancer_releases_at_most_one_range_per_cycle() -> Result<()> {
    let (bucket, mut nodes) = cluster_nodes(&["node-alpha"], 8, TTL).await?;
    {
        let refs: Vec<_> = nodes.iter().collect();
        wait_probe(
            &refs,
            Duration::from_secs(20),
            "alpha holds all 8",
            balanced(&[8]),
        )
        .await?;
    }
    nodes.push(start_cluster_node(&bucket, "node-beta", 8, TTL).await?);
    let deadline = cairn_server::balance::settle_for(TTL) + TTL * 20;
    let r = common::within(deadline, "alpha releases down to 4", async {
        let alpha = [&nodes[0]];
        loop {
            let probe = common::leaders_by_probe(&alpha).await;
            if probe.values().filter(|v| v.contains(&0)).count() <= 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let refs: Vec<_> = nodes.iter().collect();
        wait_probe(&refs, TTL * 10, "split 4/4", balanced(&[4, 4])).await?;
        let releases = nodes[0].node.balancer.releases();
        ensure!(
            !releases.is_empty(),
            "alpha down to 4 but the balancer released nothing"
        );
        for w in releases.windows(2) {
            let gap = w[1].at - w[0].at;
            ensure!(
                gap >= TTL * 3 / 2,
                "two releases {gap:?} apart < 1.5 TTL: {releases:?}"
            );
        }
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// The term in range `r`'s owner record on the shared bucket (empty prefix).
async fn owner_term(
    bucket: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
    r: u32,
) -> Result<u64> {
    let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
    let rec = cairn_store::read_owner(
        &store,
        &slatedb::object_store::path::Path::default(),
        cairn_store::PartitionId(r),
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("range {r} has no owner record"))?;
    Ok(rec.term)
}

/// **Opening an engine rides out transient GET errors, and does not reclaim the
/// lease because of them.**
///
/// Under sustained 503s (`s3_slowdown`), a range that lost its leader could not be
/// reopened: opening the engine needs a run of GETs without errors, and every failure
/// dropped the keeper (a release PUT), rested 0.5 s, then claimed again from scratch
/// (a claim PUT, `term` + 1) — adding to the throttling itself.
///
/// Interleaving:
/// 1. Node A alone, one range; A leads at term `T`.
/// 2. **A's** store: the next two GETs to `cluster-version` fail.
/// 3. A resigns the range (`resign`) then claims it back: the claim writes term
///    `T + 1`, then opening the engine reads `cluster-version` (`ensure_known`) and
///    meets those two errors.
///
/// Must see: A leads again, and the owner record is at exactly term `T + 1` — one
/// claim. Before the fix: one resign + claim round per error, term `T + 3`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_open_rides_out_transient_get_errors_without_reclaiming() -> Result<()> {
    rides_out_transient_get_errors("cluster-version", "open-retry-a").await
}

/// Like [`engine_open_rides_out_transient_get_errors_without_reclaiming`], for the
/// **table reload** (`cluster.conf`, a `LeaseIo` error). Since RD1 an open does not
/// proceed when the table load fails, so this read must be retried in place —
/// otherwise every error is again one resign + claim round.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_open_rides_out_transient_table_reload_errors_without_reclaiming() -> Result<()> {
    rides_out_transient_get_errors("cluster.conf", "open-retry-table-a").await
}

/// The next two GETs to a path containing `path` fail right before A resigns and
/// claims back range 0: A must lead again at exactly term `T + 1`.
async fn rides_out_transient_get_errors(path: &str, node: &str) -> Result<()> {
    let bucket = common::bucket();
    let a = start_cluster_node(&bucket, node, 1, Duration::from_secs(2)).await?;
    wait_probe(
        &[&a],
        Duration::from_secs(20),
        "A is leading range 0",
        |p| p.get(&0) == Some(&vec![0]),
    )
    .await?;
    let before = owner_term(&bucket, 0).await?;

    a.store.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains(path.into()),
        Action::Fail { times: Some(2) },
    ));
    let h = a
        .node
        .partitions
        .get(cairn_store::PartitionId(0))
        .ok_or_else(|| anyhow::anyhow!("A has no engine for range 0"))?;
    h.resign()
        .await
        .map_err(|e| anyhow::anyhow!("resign: {e}"))?;
    wait_probe(
        &[&a],
        Duration::from_secs(20),
        "A leads range 0 again",
        |p| p.get(&0) == Some(&vec![0]),
    )
    .await?;
    let after = owner_term(&bucket, 0).await?;
    a.node.shutdown().await;
    ensure!(
        after == before + 1,
        "opening an engine must ride out failed GETs ({path}) without reclaiming the lease: term {before} → {after} (want {})",
        before + 1
    );
    Ok(())
}

/// **A renewal later than ⅔·TTL that then wins its CAS: the node keeps every range,
/// no replay.**
///
/// Interleaving:
/// 1. Node A alone, two ranges, TTL 2 s (renewal tick 0.4 s, validity ⅔·TTL =
///    1.33 s); A leads both at incarnation `I`, terms `T₀`, `T₁`.
/// 2. `t0`: every `sessions/` PUT **of A** stops at a gate — the renewal is issued
///    but has not reached the store (a network slow by ~one TTL).
/// 3. `t0 + 1.4 s`: the validity has run out (the last renewal was issued at
///    `t0 − 0.4 s` or later). A client asks range 0's `status` — a read at the leader.
/// 4. `t0 + 1.8 s` (< TTL since the last renewal an observer could have seen: nobody
///    can CAS `expired` yet — and there is no observer here at all): the gate opens,
///    the renewal CAS reaches the store from the right previous version ⇒ it wins.
///
/// Must see: A is still at incarnation `I`; every range still at its old term (no
/// resign, no reclaim, no replay); the command at (3) is not answered during the
/// pause — it is answered **after** the gate opens, and succeeds. On the old code:
/// past ⅔·TTL ⇒ `lose()` (writes `closed`), command (3) got `NotLeader` at once, every
/// range was reclaimed at a new incarnation (term + 1) and replayed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_renewal_that_wins_cas_keeps_every_range() -> Result<()> {
    use cairn_store::testkit::Gate;
    let ttl = Duration::from_secs(2);
    let bucket = common::bucket();
    // Pin renewal by PUT (`direct`): a lone node in `solo` does not PUT anything.
    let a = common::start_cluster_node_tweaked(&bucket, "revive-a", 2, ttl, common::direct).await?;
    wait_probe(
        &[&a],
        Duration::from_secs(20),
        "A leads both of its ranges",
        |p| p.len() == 2 && p.values().all(|v| v == &[0]),
    )
    .await?;
    let incarnation = a.node.session.current();
    let terms = [owner_term(&bucket, 0).await?, owner_term(&bucket, 1).await?];
    let h = a
        .node
        .partitions
        .get(cairn_store::PartitionId(0))
        .ok_or_else(|| anyhow::anyhow!("A has no engine for range 0"))?;
    let range = a
        .node
        .cluster
        .range(cairn_store::PartitionId(0))
        .ok_or_else(|| anyhow::anyhow!("range 0 is not in the table"))?;
    let probe = cairn_store::make_invocation_id(cairn_store::PartitionKey(range.start), [7; 16]);

    let gate = Gate::closed();
    let rule = a.store.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("sessions/".into()),
        Action::Gate(gate.clone()),
    ));
    let t0 = Instant::now();
    tokio::time::sleep(ttl.mul_f32(0.7)).await;
    let read = tokio::spawn(async move {
        let r = h.status(probe).await;
        (r.map(|_| ()).map_err(|e| e.to_string()), Instant::now())
    });
    tokio::time::sleep(ttl.mul_f32(0.9).saturating_sub(t0.elapsed())).await;
    ensure!(
        !read.is_finished(),
        "the read was answered during the pause: {:?}",
        read.await?
    );
    let opened = Instant::now();
    gate.open();
    a.store.remove_rule(rule);
    let (answer, at) = read.await?;

    // Let the renewal loop and the leadership loop run a few ticks after the revival.
    tokio::time::sleep(ttl).await;
    let after = [owner_term(&bucket, 0).await?, owner_term(&bucket, 1).await?];
    let now = a.node.session.current();
    let led = common::leaders_by_probe(&[&a]).await;
    a.node.shutdown().await;
    ensure!(
        answer.is_ok(),
        "the read during the pause must be answered after the revival, not with an error: {answer:?}"
    );
    ensure!(at >= opened, "the read was answered before the revival");
    ensure!(
        now == incarnation,
        "A dropped its incarnation although the renewal CAS won: {incarnation:?} → {now:?}"
    );
    ensure!(
        after == terms,
        "ranges reclaimed (replay) although the renewal CAS won: term {terms:?} → {after:?}"
    );
    ensure!(
        led.len() == 2 && led.values().all(|v| v == &[0]),
        "A no longer leads every range: {led:?}"
    );
    Ok(())
}

/// Whether the engine of range `r` on `n` is leading — asked through `flow`, answered
/// in place, not through the read fence (LS-5): the fence blocks the old owner's
/// read answers but does not make it step down, so a poll through the fence cannot
/// tell "has stepped down" from "still believes it leads".
async fn leads(n: &common::ClusterNode, r: cairn_store::PartitionId) -> bool {
    match n.node.partitions.get(r) {
        Some(h) => matches!(
            tokio::time::timeout(Duration::from_millis(500), h.flow()).await,
            Ok(Ok(_))
        ),
        None => false,
    }
}

/// **The old owner sees the range is lost at once, thanks to the claimer's notice**
/// (layer 3 of LS-6).
///
/// Interleaving:
/// 1. Nodes A, B, two ranges, TTL 1 s, the owner-record watch loop every **10
///    minutes** (the safety net behind the notice). Wait until each node leads one
///    range; `r` is A's range.
/// 2. A "freezes" (`crash_handle`): the renewal loop dies, the grant validity is
///    pinned open — layer 1 off; A writes nothing so it never touches `writer_epoch` —
///    layer 2 off. A's HTTP and watch loop keep running.
/// 3. After a silent TTL, B CASes A's session to `expired` and claims `r`.
///
/// Must see: within 3 s of B leading `r`, A's engine for `r` stops leading. Without
/// the notice: only the watch loop (every 10 minutes) sees the owner record moved to
/// B — A believes it leads for the whole window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_owner_steps_down_at_once_when_the_claimant_tells_it() -> Result<()> {
    const GRID: Duration = Duration::from_secs(600);
    let bucket = common::bucket();
    let watch = |cfg: &mut cairn_server::node::NodeConfig| cfg.owner_watch = Some(GRID);
    let a = common::start_cluster_node_tweaked(&bucket, "owner-a", 2, TTL, watch).await?;
    let b = common::start_cluster_node_tweaked(&bucket, "owner-b", 2, TTL, watch).await?;
    let r = async {
        let refs = [&a, &b];
        let seen = wait_probe(
            &refs,
            Duration::from_secs(30),
            "one range per node",
            balanced(&[1, 1]),
        )
        .await?;
        let leaders = single_leaders(&seen).expect("balanced");
        let r = cairn_store::PartitionId(
            *leaders
                .iter()
                .find(|(_, n)| **n == 0)
                .map(|(r, _)| r)
                .expect("A leads a range"),
        );
        ensure!(leads(&a, r).await, "A does not lead {r:?}");

        a.node.session.crash_handle().crash();
        let t0 = Instant::now();
        while !leads(&b, r).await {
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "B did not claim {r:?} after A froze"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let t1 = Instant::now();
        while leads(&a, r).await {
            ensure!(
                t1.elapsed() < Duration::from_secs(3),
                "3 s after B leads {r:?}, A still believes it leads — nobody told it"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    }
    .await;
    a.node.shutdown().await;
    b.node.shutdown().await;
    r
}

/// **The default owner-watch net is 1 minute.** The claimer's notice and the
/// coordinator's `view` are the fast paths; the net is what remains when both are
/// lost (`net_partition.rs::old_owner_cut_from_notice_and_coordinator_steps_down_at_the_grid`),
/// so it is the worst-case latency of LS-6 layer 3 in the default configuration — the
/// runbook `docs/runbooks/control-plane.md` quotes this number. It used to be 10 minutes.
#[test]
fn default_owner_watch_grid_is_one_minute() {
    assert_eq!(cairn_store::DEFAULT_OWNER_WATCH, Duration::from_secs(60));
}

// ─────────────────────────────── a lone node (LS-16)

/// Turn on the `control/quiet-lease` gate, like `cairn-ctl cluster enable-quiet-lease --apply`.
async fn quiet_lease_on(bucket: &common::Bucket) -> Result<()> {
    let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = (**bucket).clone();
    let on = cairn_store::quiet_lease::QuietLease {
        on: true,
        knows: cairn_store::migrate::QUIET_LEASE,
        by: "cluster_lease".into(),
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

/// Wait for the node to enter `mode=solo` (the CAS into the confirmed mode).
async fn until_solo(node: &cairn_server::node::RunningNode) -> Result<()> {
    let t0 = Instant::now();
    while node.session.mode() != cairn_store::SessionMode::Solo {
        ensure!(t0.elapsed() < TTL * 10, "lone node never entered mode=solo");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

/// **A lone node gets a joiner: becomes a cluster, splits the ranges, loses none**
/// (LS-16). Part of the "ambiguous once per op" row of `check.sh`: the CAS into the
/// mode, the challenge CAS and the answer CAS all meet an ambiguous answer the first
/// time.
///
/// 1. Gate on; alpha alone, four ranges, enters `mode=solo`.
/// 2. beta joins, challenges alpha's record; alpha answers (back to `direct`).
/// 3. The balancer splits 2/2.
///
/// Must see: no probe sees two leaders for one range; alpha keeps its incarnation (it
/// is not expired); the cluster balances 2/2; beta is not in `solo` (a node other than
/// the coordinator delegates), and alpha is in `solo` only when beta delegates to it
/// (a coordinator to which every other node delegates).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn solo_node_joined_by_another_forms_a_cluster_without_losing_a_range() -> Result<()> {
    let bucket = common::bucket();
    quiet_lease_on(&bucket).await?;
    let alpha = start_cluster_node(&bucket, "node-alpha", 4, TTL).await?;
    wait_probe(
        &[&alpha],
        Duration::from_secs(20),
        "alpha leads every range",
        |p| p.len() == 4 && p.values().all(|v| v == &[0]),
    )
    .await?;
    until_solo(&alpha.node).await?;
    let inc = alpha.node.session.current();
    let beta = start_cluster_node(&bucket, "node-beta", 4, TTL).await?;
    let refs = [&alpha, &beta];
    let t0 = Instant::now();
    loop {
        let p = common::leaders_by_probe(&refs).await;
        ensure!(p.values().all(|v| v.len() <= 1), "two leaders: {p:?}");
        ensure!(alpha.node.session.current() == inc, "incarnation changed");
        if balanced(&[2, 2])(&p) {
            break;
        }
        ensure!(t0.elapsed() < TTL * 30, "cluster not balanced 2/2: {p:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // A node other than the coordinator delegates its renewal (`mode=delegated`); the
    // coordinator is in `solo` when every other node delegates to it.
    let (am, bm) = (alpha.node.session.mode(), beta.node.session.mode());
    let alpha_quiet_ok = am != cairn_store::SessionMode::Solo
        || matches!(&bm, cairn_store::SessionMode::Delegated { coord, .. }
            if Some(coord) == alpha.node.session.current().as_ref());
    ensure!(
        bm != cairn_store::SessionMode::Solo && alpha_quiet_ok,
        "unexpected mode with two nodes in the cluster: {am:?}, {bm:?}"
    );
    alpha.node.shutdown().await;
    beta.node.shutdown().await;
    Ok(())
}

/// **A frozen lone node: its ranges move to the newcomer after a challenge + TTL, not
/// earlier** (LS-16). The silence rule LS-1 does not apply to a `solo` record (alpha
/// has been silent since entering the mode, for longer than a TTL).
///
/// 1. Gate on; alpha alone, four ranges, in `solo` for two more TTLs.
/// 2. alpha freezes (every request to the store hangs).
/// 3. beta joins at t0: challenges, waits a TTL, CASes `expired`, claims the ranges.
///
/// Must see: beta leads its first range no earlier than ~TTL after t0, and every
/// range within a few TTLs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frozen_solo_node_ranges_move_after_challenge_and_ttl() -> Result<()> {
    let bucket = common::bucket();
    quiet_lease_on(&bucket).await?;
    let alpha = start_cluster_node(&bucket, "node-alpha", 4, TTL).await?;
    wait_probe(
        &[&alpha],
        Duration::from_secs(20),
        "alpha leads every range",
        |p| p.len() == 4 && p.values().all(|v| v == &[0]),
    )
    .await?;
    until_solo(&alpha.node).await?;
    tokio::time::sleep(TTL * 2).await;
    freeze(&alpha.store);
    let t0 = Instant::now();
    let beta = start_cluster_node(&bucket, "node-beta", 4, TTL).await?;
    let mut first = None;
    loop {
        let p = common::leaders_by_probe(&[&beta]).await;
        let led = p.values().filter(|v| !v.is_empty()).count();
        if led > 0 && first.is_none() {
            first = Some(t0.elapsed());
        }
        if led == 4 {
            break;
        }
        ensure!(
            t0.elapsed() < TTL * 10,
            "beta holds only {led}/4 ranges after {:?}",
            t0.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let first = first.expect("a range has moved");
    ensure!(
        first >= TTL.mul_f32(0.9),
        "alpha's ranges taken after {first:?} — earlier than challenge + TTL"
    );
    beta.node.shutdown().await;
    Ok(())
}
