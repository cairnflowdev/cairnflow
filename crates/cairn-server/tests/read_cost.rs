// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Read cost: **when** a node reads the bucket.
//!
//! Each test here pins one GET source driven by time or by churn that a `chaos.ts --cost`
//! measuring run can separate by caller label (`cairn_store::io_source`),
//! and changes only when reads happen — never the right/wrong path.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{ensure, Context, Result},
    cairn_store::testkit::{FaultyStore, Op},
    common::{cluster_nodes, single_leaders, wait_probe},
    std::time::Duration,
};

/// GETs of session records by the observation loop (`session.tracker`) on this store.
fn tracker_gets(store: &FaultyStore) -> u64 {
    store
        .counts_by_source()
        .into_iter()
        .filter(|(src, op, ..)| *src == "session.tracker" && *op == Op::Get)
        .map(|(.., n)| n)
        .sum()
}

/// A `503 NotLeader` for a range whose leader the routing table already knows to be
/// **another** node does not trigger a session observation round.
///
/// Formerly every `NotLeader` asked for a table refresh, and that request woke the
/// tracker: a round GETting **every** other session, with a 200 ms floor. A client hitting
/// the wrong node (`chaos.ts`'s two-leader detector, or a load balancer unaware of ranges)
/// turned the TTL/5 observation cadence into 5 rounds a second — ~2.9 GETs/step in a
/// `--cost` run, the largest after reading dead logs when taking a range.
///
/// 1. Two nodes A, B (TTL 10 s ⇒ the tracker observes every 2 s); each range has exactly one
///    leader, and the other node's routing table already knows it.
/// 2. An idle 3 s window: count the tracker's session GETs on the other node.
/// 3. A loaded 3 s window: a client sends `cancel` for an id of that range to the other node,
///    20 times a second — that node answers 503 with the leader's address.
///
/// Must see: the tracker's GETs in the loaded window are no more than in the idle window plus 2 (one
/// out-of-phase round on each side).
#[tokio::test(flavor = "multi_thread")]
async fn not_leader_reply_does_not_wake_the_session_tracker() -> Result<()> {
    let (_shared, nodes) = cluster_nodes(&["a", "b"], 2, Duration::from_secs(10)).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let probe = wait_probe(
        &refs,
        Duration::from_secs(30),
        "one leader per range",
        |p| single_leaders(p).is_some(),
    )
    .await?;
    let leaders = single_leaders(&probe).context("one leader per range")?;
    let (&p, &leader) = leaders.iter().next().context("a range")?;
    let other = 1 - leader;
    let asked = &nodes[other];
    let leader_id = nodes[leader].node.node_id.clone();
    let t0 = std::time::Instant::now();
    while asked
        .node
        .routing
        .leader_of(cairn_store::PartitionId(p))
        .is_none_or(|l| l.node_id != leader_id)
    {
        ensure!(
            t0.elapsed() < Duration::from_secs(30),
            "the asked node's routing table does not know the leader yet"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let range = asked
        .node
        .cluster
        .range(cairn_store::PartitionId(p))
        .context("range in the table")?;
    let id = cairn_store::make_invocation_id(cairn_store::PartitionKey(range.start), [0x5a; 16]);
    let url = format!(
        "http://{}/invocations/{}/cancel",
        asked.node.addr,
        cairn_store::id_hex(&id)
    );
    const WINDOW: Duration = Duration::from_secs(3);

    let before = tracker_gets(&asked.store);
    tokio::time::sleep(WINDOW).await;
    let idle = tracker_gets(&asked.store) - before;

    let client = reqwest::Client::new();
    let before = tracker_gets(&asked.store);
    let t0 = std::time::Instant::now();
    let mut not_leader = 0;
    while t0.elapsed() < WINDOW {
        let r = client.post(&url).send().await?;
        if r.status().as_u16() == 503 {
            not_leader += 1;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let busy = tracker_gets(&asked.store) - before;
    ensure!(
        not_leader > 20,
        "the asked node must return 503 NotLeader, only saw {not_leader}"
    );
    ensure!(
        busy <= idle + 2,
        "{not_leader} 503 NotLeader answers made the tracker read {busy} sessions in {WINDOW:?} (idle: {idle})"
    );
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// The session observation round runs every TTL/3, not at the TTL/5 renewal cadence.
///
/// The tracker GETs the session of every other node each round: while idle that was 69% of the
/// read cost of a 3-node cluster in a SeaweedFS measuring run, and ~0.5 GETs/step under
/// chaos. Renewal stays at TTL/5 (three attempts before the deadline at which a node steps itself
/// down, ⅔ TTL); detecting a dead node gets slower by at most TTL/3 − TTL/5 (1.3 s at TTL 10 s).
///
/// 1. Two nodes, TTL 1.5 s (renewal 300 ms, observation 500 ms); one leader per range.
/// 2. An idle 6 s window: count the tracker's session GETs on node `a` (one peer ⇒ one
///    GET per round).
///
/// Must see: the GET count in `[6, 13]`. Each round is a little longer than the cadence (the
/// `MIN_REFRESH_GAP` floor plus the GET time), so TTL/3 measures 9–10 on a debug
/// build, TTL/5 measures 16.
#[tokio::test(flavor = "multi_thread")]
async fn session_tracker_observes_every_third_of_ttl() -> Result<()> {
    const TTL: Duration = Duration::from_millis(1500);
    const WINDOW: Duration = Duration::from_secs(6);
    // Pin the tracker cadence of `direct` mode: in delegated mode the coordinator
    // does not GET the sessions delegated to it while heartbeats keep arriving.
    let (_shared, nodes) =
        common::cluster_nodes_tweaked(&["a", "b"], 2, TTL, common::direct).await?;
    let refs: Vec<_> = nodes.iter().collect();
    wait_probe(
        &refs,
        Duration::from_secs(30),
        "one leader per range",
        |p| single_leaders(p).is_some(),
    )
    .await?;
    let before = tracker_gets(&nodes[0].store);
    tokio::time::sleep(WINDOW).await;
    let rounds = tracker_gets(&nodes[0].store) - before;
    ensure!(
        (6..=13).contains(&rounds),
        "tracker GET {rounds} sessions in {WINDOW:?} (TTL {TTL:?}): a TTL/3 cadence gives 9–10"
    );
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// A cluster-level registry change on one node reaches the other node **at once**, without waiting
/// for the registry poll. The poll is a safety net, not the propagation path:
/// each round is a few GETs + a few LISTs per node (`range-windows`, `auth/keys`,
/// `tenants`, `capture-requests/`, `deployments/`, `catalog/`) — at a 5 s cadence that is
/// ~13 000 requests/hour for an idle 3-node cluster, ~60% of the idle cost after the tracker.
///
/// 1. Two nodes `a`, `b`, registry refresh every 30 s; both past their first refresh round.
///    `a` may **not** yet see `b` in the session table (LIST every TTL) — the notice
///    must find its peers itself (this is why `s3_slowdown` once waited 30 s).
/// 2. Register a deployment on `a`.
///
/// Must see: `GET /deployments` on `b` lists it within 3 s.
#[tokio::test(flavor = "multi_thread")]
async fn registry_change_reaches_peer_before_the_poll() -> Result<()> {
    let shared = std::sync::Arc::new(slatedb::object_store::memory::InMemory::new());
    let mut nodes = Vec::new();
    for id in ["a", "b"] {
        nodes.push(
            common::start_cluster_node_tweaked(&shared, id, 2, Duration::from_secs(2), |cfg| {
                cfg.deploy_refresh = Duration::from_secs(30)
            })
            .await?,
        );
    }
    let (na, nb) = (&nodes[0].node, &nodes[1].node);
    let sdk = common::fake_sdk::FakeSdk::builder()
        .object("Job", "run", vec![common::fake_sdk::Step::output("x")])
        .spawn()
        .await?;
    common::register(&na.url(), &sdk.url).await?;
    let t0 = std::time::Instant::now();
    loop {
        let listed: serde_json::Value = reqwest::get(format!("{}/deployments", nb.url()))
            .await?
            .json()
            .await?;
        if listed.as_array().is_some_and(|v| !v.is_empty()) {
            break;
        }
        ensure!(
            t0.elapsed() < Duration::from_secs(3),
            "node b does not see the deployment registered on a after 3 s (poll every 30 s)"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// Like [`registry_change_reaches_peer_before_the_poll`], in **delegated mode**:
/// the gate on, three nodes, the two nodes
/// other than the coordinator delegating renewal — the registry notice still reaches peers
/// before the poll, even though peers no longer PUT renewals or poll the registry.
///
/// 1. The `control/quiet-lease` gate on; three nodes `a`, `b`, `c`, registry refresh every
///    30 s; wait for the two non-coordinator nodes to have records with `mode=delegated`.
/// 2. Register a deployment on `a`.
///
/// Must see: `GET /deployments` on `b` and `c` lists it within 3 s.
#[tokio::test(flavor = "multi_thread")]
async fn registry_change_reaches_a_delegated_peer_before_the_poll() -> Result<()> {
    let shared = std::sync::Arc::new(slatedb::object_store::memory::InMemory::new());
    {
        let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = shared.clone();
        let on = cairn_store::quiet_lease::QuietLease {
            on: true,
            knows: cairn_store::migrate::QUIET_LEASE,
            by: "read_cost".into(),
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
    for id in ["a", "b", "c"] {
        nodes.push(
            common::start_cluster_node_tweaked(&shared, id, 2, Duration::from_secs(1), |cfg| {
                cfg.deploy_refresh = Duration::from_secs(30)
            })
            .await?,
        );
    }
    let r = async {
        let t0 = std::time::Instant::now();
        let c = loop {
            let seen = common::coordinator_of(&shared)
                .await
                .filter(|s| !s.released);
            if let Some(i) = seen.and_then(|s| {
                nodes.iter().position(|n| {
                    n.node
                        .session
                        .current()
                        .is_some_and(|id| id.to_string() == s.session)
                })
            }) {
                break i;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "no coordinator yet"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        common::until_delegated(&shared, &nodes, c, Duration::from_secs(10)).await?;
        let sdk = common::fake_sdk::FakeSdk::builder()
            .object("Job", "run", vec![common::fake_sdk::Step::output("x")])
            .spawn()
            .await?;
        common::register(&nodes[0].node.url(), &sdk.url).await?;
        let t1 = std::time::Instant::now();
        for n in &nodes[1..] {
            loop {
                let listed: serde_json::Value =
                    reqwest::get(format!("{}/deployments", n.node.url()))
                        .await?
                        .json()
                        .await?;
                if listed.as_array().is_some_and(|v| !v.is_empty()) {
                    break;
                }
                ensure!(
                    t1.elapsed() < Duration::from_secs(3),
                    "node {} does not see the deployment registered on a after 3 s (poll every 30 s)",
                    n.node.node_id
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        Ok(())
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// Shared-WAL log GETs of the follower tailer on this store.
fn tailer_gets(store: &FaultyStore) -> u64 {
    store
        .counts_by_source()
        .into_iter()
        .filter(|(src, op, ..)| *src == "nodewal.tailer" && *op == Op::Get)
        .map(|(.., n)| n)
        .sum()
}

/// The follower reader of a range the node **has just become leader of** stops following the log —
/// after taking the range, the owner record points at the node's own log, and the tailer used to GET
/// its own log every poll tick until the reader was evicted for idleness (5 minutes).
///
/// 1. Two nodes `a`, `b` (follower poll 50 ms on `b`); range `p` led by `a`.
/// 2. Ask `b` for the status of an id in `p` ⇒ `b` opens a follower reader for `p`.
/// 3. `a` shuts down gracefully; `b` takes `p`.
/// 4. Wait 1 s for the reader to refresh its log list, then count the tailer's GETs on `b`
///    over 2 s.
///
/// Must see: ≤ 2 GETs (before the fix: ~40 — GETting `b`'s own log every 50 ms).
#[tokio::test(flavor = "multi_thread")]
async fn follower_reader_of_a_range_this_node_now_leads_stops_tailing() -> Result<()> {
    let shared = std::sync::Arc::new(slatedb::object_store::memory::InMemory::new());
    let a = common::start_cluster_node(&shared, "a", 1, Duration::from_secs(2)).await?;
    let b = common::start_cluster_node_tweaked(&shared, "b", 1, Duration::from_secs(2), |cfg| {
        cfg.reader_poll = Duration::from_millis(50);
    })
    .await?;
    let refs = [&a, &b];
    wait_probe(
        &refs,
        Duration::from_secs(30),
        "a is leader of range 0",
        |p| p.get(&0).is_some_and(|v| v == &vec![0]),
    )
    .await?;
    let range = b
        .node
        .cluster
        .range(cairn_store::PartitionId(0))
        .context("range 0")?;
    let id = cairn_store::make_invocation_id(cairn_store::PartitionKey(range.start), [0x33; 16]);
    let url = format!(
        "http://{}/invocations/{}",
        b.node.addr,
        cairn_store::id_hex(&id)
    );
    let t0 = std::time::Instant::now();
    while tailer_gets(&b.store) == 0 {
        let _ = reqwest::get(&url).await?;
        ensure!(
            t0.elapsed() < Duration::from_secs(10),
            "b has not opened a follower reader for range 0"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    a.node.shutdown().await;
    wait_probe(
        &[&b],
        Duration::from_secs(30),
        "b is leader of range 0",
        |p| p.get(&0).is_some_and(|v| v == &vec![0]),
    )
    .await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let before = tailer_gets(&b.store);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let gets = tailer_gets(&b.store) - before;
    ensure!(
        gets <= 2,
        "b leads range 0 yet the follower tailer still GOT {gets} times in 2 s"
    );
    b.node.shutdown().await;
    Ok(())
}

/// GETs of the `rules` object on this store (every caller).
fn rules_gets(store: &FaultyStore) -> u64 {
    store
        .counts_by_source_object()
        .into_iter()
        .filter(|(_, op, _, object, _)| *op == Op::Get && *object == "rules")
        .map(|(.., n)| n)
        .sum()
}

/// Each range's engine **does not re-ask for the rule book every 2 s**; a rule change on one
/// node reaches the engines on the other node through a notice.
/// In a `--cost` run `rules` was 0.35 GETs/step — one GET every 2 s per range with
/// turns, for something that changes a few times a day.
///
/// 1. Two nodes `a`, `b`, each leading a range; `a` already sees `b` in the session
///    table; each node has one deployment (the engine's background tick runs).
/// 2. A 5 s window with nobody changing rules: count `rules` GETs on `b`.
/// 3. Set a rule through `a`.
///
/// Must see: 0 `rules` GETs on `b` in the window (before the fix: ≥ 2 per range);
/// `b`'s `/scheduler` reports the new `rulesVersion` within 3 s.
#[tokio::test(flavor = "multi_thread")]
async fn rule_book_is_not_polled_every_two_seconds_and_changes_still_arrive() -> Result<()> {
    let (_shared, nodes) = cluster_nodes(&["a", "b"], 2, Duration::from_secs(2)).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let probe = wait_probe(
        &refs,
        Duration::from_secs(30),
        "each node leads one range",
        |p| {
            single_leaders(p)
                .is_some_and(|l| l.values().any(|&n| n == 0) && l.values().any(|&n| n == 1))
        },
    )
    .await?;
    single_leaders(&probe).context("one leader per range")?;
    let (na, nb) = (&nodes[0].node, &nodes[1].node);
    let t0 = std::time::Instant::now();
    while na.session.tracker().live_nodes().len() < 2 {
        ensure!(
            t0.elapsed() < Duration::from_secs(10),
            "a does not see b in the session table yet"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // The engine's background tick (where the rule book used to be re-read) runs only when the
    // registry has a deployment: register on both nodes, without waiting for propagation.
    let sdk = common::fake_sdk::FakeSdk::builder()
        .object("Job", "run", vec![common::fake_sdk::Step::output("x")])
        .spawn()
        .await?;
    common::register(&na.url(), &sdk.url).await?;
    common::register(&nb.url(), &sdk.url).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let before = rules_gets(&nodes[1].store);
    tokio::time::sleep(Duration::from_secs(5)).await;
    let polled = rules_gets(&nodes[1].store) - before;

    let http = reqwest::Client::new();
    let put = http
        .post(format!("{}/rules", na.url()))
        .json(&serde_json::json!({ "pattern": "acme", "concurrency": 5 }))
        .send()
        .await?;
    ensure!(
        put.status().is_success(),
        "set a rule through a: {}",
        put.status()
    );
    let t0 = std::time::Instant::now();
    loop {
        let views: serde_json::Value = http
            .get(format!("{}/scheduler", nb.url()))
            .send()
            .await?
            .json()
            .await?;
        let seen = views
            .as_array()
            .is_some_and(|v| !v.is_empty() && v.iter().all(|s| s["rulesVersion"] == 1));
        if seen {
            break;
        }
        ensure!(
            t0.elapsed() < Duration::from_secs(3),
            "the engine on b has not seen the new rule after 3 s: {views}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    ensure!(
        polled == 0,
        "b GOT the rule book {polled} times in 5 s while nobody changed a rule"
    );
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// The leader's read-fence GETs on this store.
fn read_fence_gets(store: &FaultyStore) -> u64 {
    store
        .counts_by_source()
        .into_iter()
        .filter(|(src, op, ..)| *src == "session.read_fence" && *op == Op::Get)
        .map(|(.., n)| n)
        .sum()
}

/// **The read fence is not paid per intermediate poll.**
///
/// A blocking `/invoke` polls the status every 5 → 50 ms until the invocation settles
/// (`ingress/invoke.rs:wait_for_settled`). Each "not done yet" answer is thrown away
/// — the caller never sees it —, but formerly every one went through the
/// fence: one `sessions/` GET per poll. In a chaos run with `--profile all` that was the
/// largest source of 26.8 fence GETs/s.
///
/// 1. A one-node cluster; the handler does `ctx.sleep(400 ms)` then returns a result — each call
///    polls ~10 times.
/// 2. Four concurrent loops, out of phase, each making 3 blocking calls back to back.
///
/// Must see: every call returns `200` with its result; fence GETs per call ≤ 2.5 —
/// only answers that **go out** need the fence: `invoke`'s reply and the final
/// "done" answer. Formerly: one GET per poll (~5–10 per call).
#[tokio::test(flavor = "multi_thread")]
async fn blocking_invoke_pays_the_read_fence_once_not_per_poll() -> Result<()> {
    const LOOPS: usize = 4;
    const CALLS: usize = 3;
    let (_shared, nodes) = cluster_nodes(&["a"], 2, Duration::from_secs(10)).await?;
    let node = &nodes[0];
    wait_probe(
        &[node],
        Duration::from_secs(30),
        "the node leads every range",
        |p| single_leaders(p).is_some(),
    )
    .await?;
    let sdk = common::fake_sdk::FakeSdk::builder()
        .service(
            "Nap",
            "run",
            vec![
                common::fake_sdk::Step::Sleep(400),
                common::fake_sdk::Step::output("woke-up"),
            ],
        )
        .spawn()
        .await?;
    common::register(&node.node.url(), &sdk.url).await?;
    let before = read_fence_gets(&node.store);
    let url = node.node.url();
    let mut loops = tokio::task::JoinSet::new();
    for i in 0..LOOPS {
        let url = url.clone();
        loops.spawn(async move {
            tokio::time::sleep(Duration::from_millis(17 * i as u64)).await;
            for _ in 0..CALLS {
                let (code, body) = common::invoke(&url, "Nap", "run", None, "\"x\"").await?;
                ensure!(
                    code == 200 && body.to_string().contains("woke-up"),
                    "blocking call returned {code}: {body}"
                );
            }
            anyhow::Ok(())
        });
    }
    while let Some(r) = loops.join_next().await {
        r??;
    }
    let calls = (LOOPS * CALLS) as f64;
    let per_call = (read_fence_gets(&node.store) - before) as f64 / calls;
    println!("fence GETs per blocking call: {per_call:.2}");
    ensure!(
        per_call <= 2.5,
        "{per_call:.2} fence GETs per acked call (cap 2.5): every intermediate poll pays a GET"
    );
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// **One external read, one fence GET.**
///
/// `GET /invocations/{id}` reads the status and then (while still running) the last
/// attempt's error — formerly two commands, two answers through the fence, two GETs. A
/// proof issued **after** the last read covers every earlier read in the same
/// incarnation (model M1: `proof_issued_after_the_read_keeps_reads_fresh_with_a_lying_clock`),
/// so reads combined in one turn need only one.
///
/// 1. A one-node cluster; one invocation waiting in `suspended`.
/// 2. 20 back-to-back `GET /invocations/{id}` (cannot be batched).
///
/// Must see: ≤ 1 fence GET per read (plus one for phase skew). Before: 2.
#[tokio::test(flavor = "multi_thread")]
async fn status_read_pays_one_read_fence() -> Result<()> {
    const READS: u64 = 20;
    let (_shared, nodes) = cluster_nodes(&["a"], 2, Duration::from_secs(10)).await?;
    let node = &nodes[0];
    wait_probe(
        &[node],
        Duration::from_secs(30),
        "the node leads every range",
        |p| single_leaders(p).is_some(),
    )
    .await?;
    let sdk = common::fake_sdk::FakeSdk::builder()
        .service("Hold", "run", vec![common::fake_sdk::Step::Suspend])
        .spawn()
        .await?;
    common::register(&node.node.url(), &sdk.url).await?;
    let url = node.node.url();
    let (code, body) = common::send(&url, "Hold", "run", None, "\"x\"").await?;
    ensure!(code < 300, "/send returned {code}: {body}");
    let id = body["id"]
        .as_str()
        .with_context(|| format!("no id in {body}"))?
        .to_string();
    common::within(
        Duration::from_secs(10),
        "invocation waits in suspended",
        common::wait_status(&url, &id, "suspended"),
    )
    .await?;
    let before = read_fence_gets(&node.store);
    for _ in 0..READS {
        let (code, st) = common::invocation(&url, &id).await?;
        ensure!(code == 200 && st["status"] == "suspended", "{code}: {st}");
    }
    let gets = read_fence_gets(&node.store) - before;
    println!("fence GETs for {READS} reads: {gets}");
    ensure!(
        gets <= READS + 1,
        "{gets} fence GETs for {READS} GET /invocations/{{id}}: each read command inside one read pays a GET"
    );
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// LISTs of `quarantine/` by the quarantine loop (`quarantine`) on this store.
fn quarantine_lists(store: &FaultyStore) -> u64 {
    store
        .counts_by_source()
        .into_iter()
        .filter(|(src, op, ..)| *src == "quarantine" && *op == Op::List)
        .map(|(.., n)| n)
        .sum()
}

/// A quarantine written on one node takes effect on the other node **at once**, and the quarantine
/// loop no longer re-reads `quarantine/` every 5 s.
///
/// The node writes quarantines itself (the detector, or a manual `DELETE /admin/quarantine`),
/// not through ingress's `registry_sync::on_change` layer: formerly there was
/// no notice at all, so the re-read cadence was pinned at 5 s
/// (`QUARANTINE_REFRESH_MAX`) — ~708 LISTs/h per idle node.
///
/// 1. Two nodes `a`, `b`, registry refresh every 30 s; one leader per range.
/// 2. An idle 12 s window: count the quarantine loop's `quarantine/` LISTs on `b`.
/// 3. Three rounds `k`: the test writes a `hold` entry for `Svc<k>/run` straight to the bucket
///    (no notice — like a node dying right after writing), then
///    `DELETE /admin/quarantine/default/Svc<k>/run` on `a`: `a` reads fresh and
///    rewrites the entry as `cleared`.
///
/// Must see: (2) ≤ 1 LIST (30 s cadence; before: 2–3 at a 5 s cadence); (3) `b`'s snapshot
/// carries the `cleared` entry of `Svc<k>/run` within 1 s of the `DELETE` answering,
/// in all three rounds (before: only when `b`'s 5 s tick happened to fall within that second).
#[tokio::test(flavor = "multi_thread")]
async fn quarantine_change_reaches_peer_before_the_poll() -> Result<()> {
    use cairn_server::{quarantine, stats::HandlerKey};
    const IDLE: Duration = Duration::from_secs(12);
    let shared = common::bucket();
    let mut nodes = Vec::new();
    for id in ["a", "b"] {
        nodes.push(
            common::start_cluster_node_tweaked(&shared, id, 2, Duration::from_secs(2), |cfg| {
                cfg.deploy_refresh = Duration::from_secs(30)
            })
            .await?,
        );
    }
    let refs: Vec<_> = nodes.iter().collect();
    wait_probe(
        &refs,
        Duration::from_secs(30),
        "one leader per range",
        |p| single_leaders(p).is_some(),
    )
    .await?;
    let (a, b) = (&nodes[0], &nodes[1]);

    let before = quarantine_lists(&b.store);
    tokio::time::sleep(IDLE).await;
    let idle = quarantine_lists(&b.store) - before;

    let raw: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = (*shared).clone();
    let book = quarantine::QuarantineStore::new(raw, &Default::default());
    let client = reqwest::Client::new();
    let mut late = Vec::new();
    for k in 0..3 {
        let service = format!("Svc{k}");
        let key = HandlerKey {
            tenant: "default".into(),
            service: service.clone(),
            handler: "run".into(),
            deployment: String::new(),
        };
        book.update("default", |entries, now| {
            entries.push(quarantine::Entry::new(
                &key,
                quarantine::Action::Hold,
                now,
                Duration::from_secs(600),
                "written directly by the test".into(),
            ));
            Some(())
        })
        .await?;
        let r = client
            .delete(format!(
                "{}/admin/quarantine/default/{service}/run",
                a.node.url()
            ))
            .send()
            .await?;
        ensure!(r.status().is_success(), "DELETE on a: {}", r.status());
        let t0 = std::time::Instant::now();
        loop {
            let now = cairn_store::entropy::now_ms();
            let seen = b
                .node
                .quarantine
                .snapshot()
                .find("default", &service, "run", "", now)
                .is_some_and(|e| e.action == quarantine::Action::Cleared);
            if seen {
                break;
            }
            if t0.elapsed() > Duration::from_secs(1) {
                late.push(service.clone());
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    for n in nodes {
        n.node.shutdown().await;
    }
    ensure!(
        idle <= 1 && late.is_empty(),
        "b's quarantine loop LISTed quarantine/ {idle} times in {IDLE:?} idle (a 30 s cadence gives ≤ 1); \
         b did not see the manual removal on a within 1 s in rounds {late:?}"
    );
    Ok(())
}

/// LISTs of the registry refresh loop (`node.refresh`) on this store.
fn refresh_lists(store: &FaultyStore) -> u64 {
    store
        .counts_by_source()
        .into_iter()
        .filter(|(src, op, ..)| *src == "node.refresh" && *op == Op::List)
        .map(|(.., n)| n)
        .sum()
}

/// Waits until `GET /deployments` on `url` lists all `n` deployments.
async fn wait_deployments(url: &str, n: usize, within: Duration) -> Result<()> {
    let t0 = std::time::Instant::now();
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

/// The registry refresh loop stops LISTing when the registry generation does not change.
///
/// Formerly every tick was a full round — LIST `capture-requests/`, `catalog/`,
/// `deployments/` plus GET `range-windows`, `auth/keys`, `tenants` — even with the registry
/// unchanged: 1 080 LISTs/h per idle 3-node cluster at a 30 s cadence.
/// Now each tick is one `If-None-Match` GET of `control/registry-gen`.
///
/// 1. Two nodes `a`, `b`, registry refresh every 200 ms (to be quick).
/// 2. Register a deployment on `a` — the first registry write; wait for `b` to list it.
/// 3. An idle 3 s window: count the LISTs of `node.refresh` and of the quarantine loop
///    (`quarantine`, same 200 ms cadence) on `b`.
///
/// Must see: ≤ 2 LISTs per source (before: 45 and 3 — a full round every tick, the
/// quarantine loop re-reading up to once a second at the detection cadence; after: 0 and 0).
#[tokio::test(flavor = "multi_thread")]
async fn registry_poll_skips_lists_while_the_generation_is_unchanged() -> Result<()> {
    const IDLE: Duration = Duration::from_secs(3);
    let shared = common::bucket();
    let mut nodes = Vec::new();
    for id in ["a", "b"] {
        nodes.push(
            common::start_cluster_node_tweaked(&shared, id, 2, Duration::from_secs(2), |cfg| {
                cfg.deploy_refresh = Duration::from_millis(200)
            })
            .await?,
        );
    }
    let (a, b) = (&nodes[0], &nodes[1]);
    let sdk = common::fake_sdk::FakeSdk::builder()
        .object("Job", "run", vec![common::fake_sdk::Step::output("x")])
        .spawn()
        .await?;
    common::register(&a.node.url(), &sdk.url).await?;
    wait_deployments(&b.node.url(), 1, Duration::from_secs(5)).await?;
    // A round in progress when `b` saw the deployment has fully finished.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let before = (refresh_lists(&b.store), quarantine_lists(&b.store));
    tokio::time::sleep(IDLE).await;
    let idle = refresh_lists(&b.store) - before.0;
    let quarantine = quarantine_lists(&b.store) - before.1;
    eprintln!("{IDLE:?} idle: node.refresh {idle} LIST, quarantine {quarantine} LIST");
    for n in nodes {
        n.node.shutdown().await;
    }
    ensure!(
        idle <= 2 && quarantine <= 2,
        "in {IDLE:?} idle with an unchanged registry, b LISTed {idle} times in the registry refresh loop and \
         {quarantine} times in the quarantine loop"
    );
    Ok(())
}

/// Copies tenant `default`'s deployment `src` to `id` straight onto the bucket —
/// through no node, without bumping the generation, notifying nobody.
async fn copy_deployment(store: &cairn_store::DeploymentStore, src: &str, id: &str) -> Result<()> {
    let tenant = cairn_store::TenantId::new("default")?;
    let raw = store.get(&tenant, src).await?.context("src deployment")?;
    let mut d: serde_json::Value = serde_json::from_slice(&raw)?;
    d["id"] = serde_json::Value::String(id.into());
    store.create(&tenant, id, &serde_json::to_vec(&d)?).await?;
    Ok(())
}

/// A registry change without a notice still reaches peers: through the registry
/// generation when the writer managed to bump it, through full reconciliation when it did not.
///
/// 1. Two nodes `a`, `b`, registry refresh every 200 ms, full reconciliation every 3 s. Register
///    deployment X on `a` (the registry generation exists from here); `b` lists X.
/// 2. Like a node whose notice was lost: the test copies X to Y straight onto the bucket then
///    bumps the generation.
/// 3. Like a node dying between the main write and the bump: the test copies X to Z
///    straight onto the bucket — no bump, no notice.
///
/// Must see: `b` lists Y within 1.5 s (the poll sees the generation change), Z within
/// one reconciliation (≤ 3 s + a tick).
#[tokio::test(flavor = "multi_thread")]
async fn registry_change_without_notice_arrives_by_generation_or_reconcile() -> Result<()> {
    let shared = common::bucket();
    let mut nodes = Vec::new();
    for id in ["a", "b"] {
        nodes.push(
            common::start_cluster_node_tweaked(&shared, id, 2, Duration::from_secs(2), |cfg| {
                cfg.deploy_refresh = Duration::from_millis(200);
                cfg.registry_reconcile = Duration::from_secs(3);
            })
            .await?,
        );
    }
    let (a, b) = (&nodes[0], &nodes[1]);
    let sdk = common::fake_sdk::FakeSdk::builder()
        .object("Job", "run", vec![common::fake_sdk::Step::output("x")])
        .spawn()
        .await?;
    common::register(&a.node.url(), &sdk.url).await?;
    wait_deployments(&b.node.url(), 1, Duration::from_secs(5)).await?;

    let raw: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = (*shared).clone();
    let store = cairn_store::DeploymentStore::new(raw.clone(), &Default::default());
    let x = store
        .list_ids()
        .await?
        .first()
        .context("deployment X")?
        .1
        .clone();

    copy_deployment(&store, &x, "dp_genbumped").await?;
    cairn_server::registry_gen::RegistryGen::new(raw, &Default::default(), None)
        .bump()
        .await?;
    wait_deployments(&b.node.url(), 2, Duration::from_millis(1500)).await?;

    copy_deployment(&store, &x, "dp_nobump").await?;
    wait_deployments(&b.node.url(), 3, Duration::from_secs(4)).await?;
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// **The cost of each poll cycle of a follower reader**: the
/// number the stream design (stream reads only at the leader, AP-13) rests on — "~10
/// requests per poll cycle". The test **measures**, prints, and caps it loosely.
///
/// 1. Two nodes: `a` leads the only range, `b` leads nothing (poll 200 ms).
/// 2. A baseline 3 s window on `b`: count requests by operation (no reader yet).
/// 3. One `GET /invocations/{id}` through `b` opens a follower reader for range 0.
/// 4. A measuring 3 s window: count again.
///
/// Must see: the gauge `cairnflow_follower_readers_open{reason="status"}` ≥ 1;
/// the difference per poll cycle ≤ 15 requests (GET + LIST + HEAD).
#[tokio::test(flavor = "multi_thread")]
async fn follower_reader_costs_a_bounded_number_of_requests_per_poll() -> Result<()> {
    use cairn_store::testkit::Op;
    const POLL: Duration = Duration::from_millis(200);
    const WINDOW: Duration = Duration::from_secs(3);
    let shared = std::sync::Arc::new(slatedb::object_store::memory::InMemory::new());
    let a = common::start_cluster_node(&shared, "a", 1, Duration::from_secs(2)).await?;
    let b = common::start_cluster_node_tweaked(&shared, "b", 1, Duration::from_secs(2), |cfg| {
        cfg.reader_poll = POLL;
    })
    .await?;
    wait_probe(
        &[&a, &b],
        Duration::from_secs(30),
        "a is leader of range 0",
        |p| p.get(&0).is_some_and(|v| v == &vec![0]),
    )
    .await?;
    let ops =
        |c: &cairn_store::testkit::StoreCounts| [Op::Get, Op::List, Op::Head].map(|o| c.op(o));
    tokio::time::sleep(Duration::from_secs(1)).await;
    let c0 = ops(&b.store.counts());
    tokio::time::sleep(WINDOW).await;
    let c1 = ops(&b.store.counts());
    let base: Vec<u64> = c1.iter().zip(&c0).map(|(x, y)| x - y).collect();

    let range = b
        .node
        .cluster
        .range(cairn_store::PartitionId(0))
        .context("range 0")?;
    let id = cairn_store::make_invocation_id(cairn_store::PartitionKey(range.start), [0x44; 16]);
    let url = format!(
        "http://{}/invocations/{}",
        b.node.addr,
        cairn_store::id_hex(&id)
    );
    let t0 = std::time::Instant::now();
    while cairn_server::metrics::agent::FOLLOWER_READERS_OPEN[0]
        .load(std::sync::atomic::Ordering::Relaxed)
        == 0
    {
        let _ = reqwest::get(&url).await?;
        ensure!(
            t0.elapsed() < Duration::from_secs(10),
            "b has not opened a follower reader for range 0"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let c2 = ops(&b.store.counts());
    tokio::time::sleep(WINDOW).await;
    let c3 = ops(&b.store.counts());
    let open: Vec<u64> = c3.iter().zip(&c2).map(|(x, y)| x - y).collect();
    let polls = (WINDOW.as_millis() / POLL.as_millis()) as f64;
    let per_poll: Vec<f64> = open
        .iter()
        .zip(&base)
        .map(|(o, b)| o.saturating_sub(*b) as f64 / polls)
        .collect();
    eprintln!(
        "follower reader per poll cycle: GET {:.2}, LIST {:.2}, HEAD {:.2} (baseline {base:?}, open {open:?}, {polls} cycles)",
        per_poll[0], per_poll[1], per_poll[2]
    );
    let total: f64 = per_poll.iter().sum();
    ensure!(
        total <= 15.0,
        "the follower reader costs {total:.2} requests per poll cycle (> 15)"
    );
    b.node.shutdown().await;
    a.node.shutdown().await;
    Ok(())
}
