// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **The cluster coordinator**: the `control/coordinator` record, heartbeats, and
//! listening to the `view` instead of polling.
//!
//! The tests here look from outside: they read the objects on the bucket directly,
//! count requests by source (`cairn_store::io_source`) on each node's store, and ask
//! the engines. Tests that need the network filter (cutting node ↔ coordinator) live
//! in `net_partition.rs` (`--features testkit`).

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
    common::{
        coordinator_of, freeze, single_leaders, start_cluster_node, wait_probe, ClusterNode,
        CoordinatorSeen as Seen,
    },
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
};

const TTL: Duration = Duration::from_secs(1);

/// The current incarnation of each node, as `<node>/<inc>`.
fn live_sessions(nodes: &[&ClusterNode]) -> Vec<String> {
    nodes
        .iter()
        .filter_map(|n| n.node.session.current().map(|id| id.to_string()))
        .collect()
}

/// Wait until `control/coordinator` points to a session (not released) satisfying `ok`.
async fn wait_coordinator(
    bucket: &Arc<slatedb::object_store::memory::InMemory>,
    within: Duration,
    what: &str,
    ok: impl Fn(&Seen) -> bool,
) -> Result<Seen> {
    let t0 = Instant::now();
    loop {
        let seen = coordinator_of(bucket).await;
        if let Some(s) = seen.as_ref().filter(|s| !s.released && ok(s)) {
            return Ok(s.clone());
        }
        ensure!(
            t0.elapsed() < within,
            "{what}: after {within:?} control/coordinator = {seen:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn three(prefix: &str) -> Result<(common::Bucket, Vec<ClusterNode>)> {
    three_with(prefix, |_| {}).await
}

/// Like [`three`], with each node's configuration further adjusted by `tweak`.
async fn three_with(
    prefix: &str,
    tweak: impl Fn(&mut cairn_server::node::NodeConfig),
) -> Result<(common::Bucket, Vec<ClusterNode>)> {
    let bucket = common::bucket();
    let mut nodes = Vec::new();
    for id in ["a", "b", "c"] {
        nodes.push(
            common::start_cluster_node_tweaked(&bucket, &format!("{prefix}-{id}"), 4, TTL, &tweak)
                .await?,
        );
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

fn index_of(nodes: &[ClusterNode], session: &str) -> Result<usize> {
    nodes
        .iter()
        .position(|n| {
            n.node
                .session
                .current()
                .is_some_and(|id| id.to_string() == session)
        })
        .ok_or_else(|| anyhow!("coordinator {session} is not any current incarnation"))
}

/// **The cluster has exactly one coordinator, pointing to a live session, and the
/// role is sticky.**
///
/// 1. Three nodes, four ranges, TTL 1 s. Wait for one leader per range.
/// 2. Within 5·TTL, `control/coordinator` appears, pointing to the current
///    incarnation of one of the three nodes.
/// 3. Watch for 4·TTL more without incidents.
///
/// Must see: the record does not change session or term throughout step 3 — the
/// role does not follow ranges, only the session. Before the coordinator existed:
/// the object did not exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_coordinator_names_a_live_session_and_sticks() -> Result<()> {
    let (bucket, nodes) = three("stick").await?;
    let r = async {
        let refs: Vec<_> = nodes.iter().collect();
        let first = wait_coordinator(&bucket, TTL * 5, "no coordinator yet", |s| {
            live_sessions(&refs).contains(&s.session)
        })
        .await?;
        let t0 = Instant::now();
        while t0.elapsed() < TTL * 4 {
            let now = coordinator_of(&bucket).await;
            ensure!(
                now.as_ref() == Some(&first),
                "a healthy cluster changed coordinator after {:?}: {first:?} → {now:?}",
                t0.elapsed()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **When the coordinator dies, a live node takes the role with the next term.**
///
/// 1. Three nodes as above; wait for coordinator `C` (term `t`).
/// 2. Freeze `C` (`freeze`: every request to its store hangs — the shape of a
///    `kill -9` seen from the bucket; its HTTP still listens).
/// 3. The two remaining nodes see `C`'s session silent for more than a TTL, CAS
///    `expired`; the record now points to a session that has ended on the bucket.
///
/// Must see: within 6·TTL, `control/coordinator` points to the incarnation of a
/// node still alive, term > `t`. Before the coordinator existed: no record at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_coordinator_is_replaced_with_a_later_term() -> Result<()> {
    let (bucket, nodes) = three("kill").await?;
    let refs: Vec<_> = nodes.iter().collect();
    let first = wait_coordinator(&bucket, TTL * 5, "no coordinator yet", |s| {
        live_sessions(&refs).contains(&s.session)
    })
    .await?;
    let c = index_of(&nodes, &first.session)?;
    freeze(&nodes[c].store);
    let alive: Vec<_> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != c)
        .map(|(_, n)| n)
        .collect();
    let next = wait_coordinator(
        &bucket,
        TTL * 6,
        "nobody replaced the dead coordinator",
        |s| s.term > first.term && live_sessions(&alive).contains(&s.session),
    )
    .await;
    // The frozen node cannot be shut down (its store hangs) — leave it behind, like the
    // other `freeze` tests.
    for (i, n) in nodes.into_iter().enumerate() {
        if i != c {
            n.node.shutdown().await;
        }
    }
    next.map(|_| ())
}

/// **A coordinator shutting down cleanly releases the role, another node takes it at
/// once** (the role is sticky until the session ends or it is released).
///
/// 1. Three nodes as above; wait for coordinator `C` (term `t`).
/// 2. `C` shuts down cleanly (`shutdown`): writes `released` before closing the
///    session, then tells its peers.
///
/// Must see: within 2·TTL, the record points to the incarnation of a node still
/// alive, term > `t`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drained_coordinator_hands_the_role_over() -> Result<()> {
    let (bucket, nodes) = three("drain").await?;
    let refs: Vec<_> = nodes.iter().collect();
    let first = wait_coordinator(&bucket, TTL * 5, "no coordinator yet", |s| {
        live_sessions(&refs).contains(&s.session)
    })
    .await?;
    let c = index_of(&nodes, &first.session)?;
    let mut nodes = nodes;
    let leaving = nodes.remove(c);
    leaving.node.shutdown().await;
    let refs: Vec<_> = nodes.iter().collect();
    let next = wait_coordinator(
        &bucket,
        TTL * 2,
        "nobody took over the released coordinator role",
        |s| s.term > first.term && live_sessions(&refs).contains(&s.session),
    )
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    next.map(|_| ())
}

/// **The role is bound to an incarnation, not to a node.**
///
/// 1. Three nodes as above; wait for coordinator `C` at incarnation `i` (term `t`).
/// 2. `C` fail-stops incarnation `i` (`Session::fail_stop`: writes `closed`, opens
///    incarnation `i' > i`). The record still says `C/i`.
///
/// Must see: within 4·TTL the record points to a **current** incarnation (possibly
/// `C/i'` itself), term > `t` — `C/i'` must not take the record of `C/i` as its own.
/// Before the coordinator existed: no record at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reopened_incarnation_does_not_inherit_the_role() -> Result<()> {
    let (bucket, nodes) = three("reopen").await?;
    let refs: Vec<_> = nodes.iter().collect();
    let first = wait_coordinator(&bucket, TTL * 5, "no coordinator yet", |s| {
        live_sessions(&refs).contains(&s.session)
    })
    .await?;
    let c = index_of(&nodes, &first.session)?;
    let old = nodes[c].node.session.current().expect("just seen");
    nodes[c]
        .node
        .session
        .fail_stop(&old, cairn_store::failstop::Cause::WalStall, "test")
        .await;
    let next = wait_coordinator(&bucket, TTL * 4, "role stuck on old incarnation", |s| {
        s.term > first.term
            && s.session != first.session
            && live_sessions(&refs).contains(&s.session)
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    next.map(|_| ())
}

/// The number of `sessions/` LISTs `n`'s tracker issued (source `session.tracker`).
fn tracker_lists(n: &ClusterNode) -> u64 {
    n.store
        .counts_by_source()
        .into_iter()
        .filter(|(src, op, _, _)| {
            *src == "session.tracker" && *op == cairn_store::testkit::Op::List
        })
        .map(|(_, _, _, c)| c)
        .sum()
}

/// **The coordinator learns of a new node through its first heartbeat, not through a
/// LIST** (a new node announces itself).
///
/// 1. Two nodes, TTL 1 s; wait for coordinator `C` and one stable TTL.
/// 2. Count the tracker's `sessions/` LISTs on `C` over 3·TTL of idleness.
/// 3. From here `C`'s store refuses every `sessions/` LIST. A third node `d` joins
///    the cluster.
///
/// Must see: step 2 has no more than one LIST per TTL (one node LISTs on behalf of
/// every node — the coordinator's LIST keeps the TTL pace for sessions that do not
/// heartbeat); within one TTL of `d` starting, `C`'s `Membership` lists `d` as a
/// live node — LIST is blocked, so only `d`'s heartbeat can tell it that.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_learns_a_joining_node_from_its_heartbeat_not_a_list() -> Result<()> {
    let bucket = common::bucket();
    let mut nodes = Vec::new();
    for id in ["hb-a", "hb-b"] {
        nodes.push(start_cluster_node(&bucket, id, 4, TTL).await?);
    }
    let r = async {
        let refs: Vec<_> = nodes.iter().collect();
        let first = wait_coordinator(&bucket, TTL * 5, "no coordinator yet", |s| {
            live_sessions(&refs).contains(&s.session)
        })
        .await?;
        let c = index_of(&nodes, &first.session)?;
        tokio::time::sleep(TTL).await;
        let before = tracker_lists(&nodes[c]);
        tokio::time::sleep(TTL * 3).await;
        let lists = tracker_lists(&nodes[c]) - before;
        ensure!(
            lists <= 4,
            "the coordinator LISTed sessions/ {lists} times over 3·TTL of idleness (one per TTL)"
        );
        {
            use cairn_store::testkit::{Action, OpSet, PathMatch, Rule};
            nodes[c].store.add_rule(Rule::new(
                OpSet::LIST,
                PathMatch::Contains("sessions".into()),
                Action::Fail { times: None },
            ));
        }
        let d = start_cluster_node(&bucket, "hb-d", 4, TTL).await?;
        let t0 = Instant::now();
        let seen = loop {
            if nodes[c]
                .node
                .session
                .membership()
                .live_nodes()
                .contains("hb-d")
            {
                break Ok(());
            }
            if t0.elapsed() > TTL {
                break Err(anyhow!(
                    "one TTL after hb-d started, the coordinator still does not see it: {:?}",
                    nodes[c].node.session.membership().live_nodes()
                ));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        nodes.push(d);
        seen
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// The number of requests (`op`) from source `source` that `n`'s store has seen.
fn source_count(n: &ClusterNode, source: &str, op: cairn_store::testkit::Op) -> u64 {
    n.store
        .counts_by_source()
        .into_iter()
        .filter(|(src, o, _, _)| *src == source && *o == op)
        .map(|(_, _, _, c)| c)
        .sum()
}

/// **A node listening to the coordinator's `view` stops polling the bucket**: no GET
/// of the peers' sessions every TTL/3, no `sessions/` LIST, no `cluster.conf` GET
/// every TTL — only the coordinator does that.
///
/// 1. Three nodes, four ranges, TTL 1 s; wait for coordinator `C` and 2·TTL of stability.
/// 2. Count each node's requests by source over 6·TTL of idleness.
///
/// Must see: each node other than `C` GETs peer sessions (`session.tracker`) no more
/// than 2 × (6 / `SPARSE_AFTER_TTLS` + 1) times (the sparse tracker: each peer every
/// `SPARSE_AFTER_TTLS` TTLs — 8 times with 2), no `sessions/` LIST, no
/// `cluster.conf` GET (`cluster.follow`); `C` still GETs each peer every TTL/3.
/// Before the `view`: every node GET each peer every TTL/3 (≈ 36 times), LISTed every
/// TTL, GET `cluster.conf` every TTL. Pins `direct` mode (`common::direct`): in
/// delegated mode `C` does not GET the session delegating to it while heartbeats keep
/// arriving — that part is pinned in `idle_cost.rs` (`CAPS_3`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nodes_listening_to_the_view_stop_polling_the_bucket() -> Result<()> {
    use cairn_store::testkit::Op;
    let (bucket, nodes) = three_with("listen", common::direct).await?;
    let r = async {
        let refs: Vec<_> = nodes.iter().collect();
        let first = wait_coordinator(&bucket, TTL * 5, "no coordinator yet", |s| {
            live_sessions(&refs).contains(&s.session)
        })
        .await?;
        let c = index_of(&nodes, &first.session)?;
        tokio::time::sleep(TTL * 2).await;
        let count = |n: &ClusterNode| {
            [
                source_count(n, "session.tracker", Op::Get),
                source_count(n, "session.tracker", Op::List),
                source_count(n, "cluster.follow", Op::Get),
            ]
        };
        let before: Vec<_> = nodes.iter().map(count).collect();
        tokio::time::sleep(TTL * 6).await;
        for (i, n) in nodes.iter().enumerate() {
            let now = count(n);
            let d: Vec<u64> = now.iter().zip(&before[i]).map(|(a, b)| a - b).collect();
            if i == c {
                ensure!(
                    d[0] >= 18,
                    "the coordinator GET a peer session only {} times in 6·TTL",
                    d[0]
                );
                continue;
            }
            let sparse = 2 * u64::from(6 / cairn_store::SPARSE_AFTER_TTLS + 1);
            ensure!(
                d[0] <= sparse && d[1] == 0 && d[2] == 0,
                "node {i} still polls the bucket over 6·TTL: GET session {}, LIST sessions/ {}, \
                 GET cluster.conf {}",
                d[0],
                d[1],
                d[2]
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

/// **A heartbeat carrying a term other than the current one is not acked** (the
/// coordinator's third ack rule: with delegated renewal an ack for an old term would
/// renew a grant thanks to a role that has changed hands).
///
/// 1. Three nodes; wait for coordinator `C` at term `t`; `F` is another node.
/// 2. Send `C` two heartbeats built from `F`'s session: term `t`, then term `t + 1`.
///
/// Must see: both are answered (`200`, the same `term = t`, with a `view`), the
/// first with `ack = true`, the second with `ack = false`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_does_not_ack_a_heartbeat_of_another_term() -> Result<()> {
    let (bucket, nodes) = three("term").await?;
    let r = async {
        let refs: Vec<_> = nodes.iter().collect();
        let first = wait_coordinator(&bucket, TTL * 5, "no coordinator yet", |s| {
            live_sessions(&refs).contains(&s.session)
        })
        .await?;
        let c = index_of(&nodes, &first.session)?;
        let f = (c + 1) % nodes.len();
        let me = nodes[f].node.session.self_report().expect("F alive");
        let client = reqwest::Client::new();
        let mut acks = Vec::new();
        for term in [first.term, first.term + 1] {
            let hb = cairn_server::control::Heartbeat {
                session: me.id.to_string(),
                term,
                beat: me.beat,
                etag: me.etag.clone(),
                address: me.address.clone(),
                ingress: me.ingress.clone(),
                owns: me.owns.clone(),
                slow: None,
                p99_put_ms: None,
                draining: false,
                knows: me.knows,
                registry_gen: None,
                table_version: 0,
                touched_ms: None,
                delegated: None,
            };
            let req = client
                .post(format!(
                    "{}/internal/heartbeat",
                    nodes[c].node.internal_url()
                ))
                .json(&hb);
            let resp = nodes[f].node.cluster_secret.send(&client, req).await?;
            ensure!(
                resp.status() == 200,
                "heartbeat term {term}: {}",
                resp.status()
            );
            let reply: cairn_server::control::HeartbeatReply = resp.json().await?;
            ensure!(
                reply.term == first.term && !reply.view.is_empty(),
                "{reply:?}"
            );
            acks.push(reply.ack);
        }
        ensure!(
            acks == [true, false],
            "ack by term {}, {}: {acks:?}",
            first.term,
            first.term + 1
        );
        Ok(())
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **The coordinator does not ack a heartbeat carrying an old term — nor the lease
/// answer** (the third ack rule; M1 `ack_for_an_old_term_breaks_ls3`). The
/// delegated-renewal version of
/// [`coordinator_does_not_ack_a_heartbeat_of_another_term`]: gate on, `F` has
/// delegated to `C`.
///
/// 1. Three nodes, gate on; wait for `C` at term `t` and for `F` (other than `C`) to
///    delegate to it.
/// 2. Send `C` two heartbeats built from `F`'s current session (beat, etag, age of
///    the bucket touch, delegation line as `F` reports them): term `t`, then term
///    `t − 1` (a heartbeat of the old role arriving late).
///
/// Must see: the first `ack = true`, the lease answer is `ack`; the second `ack =
/// false` and **no** lease answer at all (no ack, no permission).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_ignores_heartbeat_of_an_old_term() -> Result<()> {
    let bucket = common::bucket();
    {
        let store: Arc<dyn slatedb::object_store::ObjectStore> = (*bucket).clone();
        let on = cairn_store::quiet_lease::QuietLease {
            on: true,
            knows: cairn_store::migrate::QUIET_LEASE,
            by: "coordinator".into(),
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
    for id in ["old-a", "old-b", "old-c"] {
        nodes.push(start_cluster_node(&bucket, id, 4, TTL).await?);
    }
    let r = async {
        let refs: Vec<_> = nodes.iter().collect();
        let first = wait_coordinator(&bucket, TTL * 5, "no coordinator yet", |s| {
            live_sessions(&refs).contains(&s.session)
        })
        .await?;
        let c = index_of(&nodes, &first.session)?;
        common::until_delegated(&bucket, &nodes, c, TTL * 10).await?;
        let f = (c + 1) % nodes.len();
        // The old term: `t − 1` when there is one, otherwise a term that never existed (`t + 1`).
        let stale = if first.term > 1 {
            first.term - 1
        } else {
            first.term + 1
        };
        let client = reqwest::Client::new();
        let mut out = Vec::new();
        for term in [first.term, stale] {
            let s = &nodes[f].node.session;
            let me = s.self_report().expect("F alive");
            let hb = cairn_server::control::Heartbeat {
                session: me.id.to_string(),
                term,
                beat: me.beat,
                etag: me.etag.clone(),
                address: me.address.clone(),
                ingress: me.ingress.clone(),
                owns: me.owns.clone(),
                slow: None,
                p99_put_ms: None,
                draining: false,
                knows: me.knows,
                registry_gen: None,
                table_version: 0,
                touched_ms: s.touched_age().map(|a| a.as_millis() as u64),
                delegated: s.delegated_to().map(|(coord, t)| format!("{coord}:{t}")),
            };
            let req = client
                .post(format!(
                    "{}/internal/heartbeat",
                    nodes[c].node.internal_url()
                ))
                .json(&hb);
            let resp = nodes[f].node.cluster_secret.send(&client, req).await?;
            ensure!(
                resp.status() == 200,
                "heartbeat term {term}: {}",
                resp.status()
            );
            let reply: cairn_server::control::HeartbeatReply = resp.json().await?;
            out.push((reply.ack, reply.lease));
        }
        ensure!(
            out == [
                (true, Some(cairn_server::control::Lease::Ack)),
                (false, None)
            ],
            "answer per term {}, {stale}: {out:?}",
            first.term
        );
        Ok(())
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}
