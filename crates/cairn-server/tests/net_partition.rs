// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The network between nodes is cut, held, slowed — the network harness
//! (`NodeConfig::net_filter`, `tests/common/net.rs`).
//!
//! Built only with `--features testkit` (`required-features`). The shared bucket stays
//! reachable from every node: these are the "cut off from each other but still seeing the
//! bucket" scenarios that `FaultyStore` (which cuts *node ↔ bucket*) cannot build.
//!
//! Each race test opens with a doc comment describing the **interleaving** it reproduces
//! (see AGENTS.md).

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, bail, ensure, Context, Result},
    cairn_server::node::RunningNode,
    cairn_store::PartitionId,
    common::{
        fake_sdk::{FakeSdk, Step},
        net::{hold_then_release, isolate, net_cluster, partition, PathFilter, Verdict},
        single_leaders, wait_probe, ClusterNode,
    },
    std::time::{Duration, Instant},
};

/// A `POST` from node `from` to the internal port of node `to`, signed with `from`'s
/// cluster secret — going through `from`'s network filter, like a real node-to-node message.
async fn post_from(
    from: &RunningNode,
    to: &RunningNode,
    path: &str,
) -> reqwest::Result<reqwest::Response> {
    let client = reqwest::Client::new();
    let req = client
        .post(format!("{}{path}", to.internal_url()))
        .timeout(Duration::from_secs(5));
    from.cluster_secret.send(&client, req).await
}

async fn post_json(url: String) -> Result<(u16, serde_json::Value)> {
    let resp = reqwest::Client::new().post(url).send().await?;
    let code = resp.status().as_u16();
    let text = resp.text().await?;
    Ok((
        code,
        serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text)),
    ))
}

/// Waits for invocation `id` (asking every node, trusting whichever node returns 200) to
/// complete with result `want`.
async fn wait_completed(
    nodes: &[&ClusterNode],
    id: &str,
    want: &str,
    deadline: Duration,
) -> Result<()> {
    let t0 = Instant::now();
    let mut last = String::new();
    loop {
        for n in nodes {
            let (code, st) = common::invocation(&n.node.url(), id).await?;
            if code == 200 && st["status"] == "completed" {
                ensure!(st["output"] == want, "{id}: wrong result {st}");
                return Ok(());
            }
            last = format!("{code} {st}");
        }
        ensure!(
            t0.elapsed() < deadline,
            "{id} not completed after {deadline:?}: {last}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// `/invoke/.../send` to the node leading the key's range (found by probe),
/// retrying on `503`. Returns the acked id (`202` = the invocation is durable).
async fn send_acked(
    nodes: &[&ClusterNode],
    service: &str,
    handler: &str,
    key: &str,
) -> Result<String> {
    let t0 = Instant::now();
    loop {
        let p = nodes[0]
            .node
            .cluster
            .partition_of(&cairn_store::TenantId::default(), service, key);
        let probe = common::leaders_by_probe(nodes).await;
        if let Some(&[i]) = probe.get(&p.0).map(Vec::as_slice) {
            let (code, body) =
                common::send(&nodes[i].node.url(), service, handler, Some(key), "x").await?;
            if code == 202 || code == 200 {
                return body["id"]
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| anyhow!("ack without id: {body}"));
            }
            ensure!(code == 503, "{service}/{key}: {code} {body}");
        }
        ensure!(
            t0.elapsed() < Duration::from_secs(20),
            "could not send {service}/{key}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The deadline for the cluster to balance leadership. A new node counts toward the fair
/// share only after `settle_for(ttl)` (10·TTL, clamped to 5–30 s), then the surplus node
/// still needs two release ticks (one TTL each) and the receiver one acquire round — a
/// hard 20 s at TTL 2 s is too short from the start (flaky test).
fn balance_deadline(ttl: Duration) -> Duration {
    cairn_server::balance::settle_for(ttl) + ttl * 8 + Duration::from_secs(10)
}

/// Two nodes, one range each — returns the range index of each node.
async fn balanced_two(nodes: &[&ClusterNode], ttl: Duration) -> Result<(u32, u32)> {
    let probe = wait_probe(
        nodes,
        balance_deadline(ttl),
        "each node leads one range",
        |p| single_leaders(p).is_some_and(|m| common::load_of(&m, 2) == [1, 1]),
    )
    .await?;
    let leaders = single_leaders(&probe).expect("balanced");
    let of = |n: usize| {
        *leaders
            .iter()
            .find(|(_, i)| **i == n)
            .expect("node holds a range")
            .0
    };
    Ok((of(0), of(1)))
}

// ───────────────────────────────────────────── harness

/// The filter plugged into `ClusterSecret::send` really decides the fate of node-to-node
/// messages: `Drop` ⇒ the sender gets a network error **at once** (without waiting out the
/// timeout); `partition` cuts both directions and leaves messages within a group alone;
/// `Hold` ⇒ the sender waits until the test releases, then gets exactly the receiver's
/// response; `Delay` ⇒ the message arrives late by exactly that much.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn net_filter_decides_fate_of_node_to_node_requests() -> Result<()> {
    let (_bucket, net, nodes) =
        net_cluster(&["alpha", "beta", "gamma"], 2, Duration::from_secs(2)).await?;
    let (a, b, c) = (&nodes[0].node, &nodes[1].node, &nodes[2].node);
    let result = common::within(Duration::from_secs(30), "network harness", async {
        // `/internal/session/changed` returns `204` and changes nothing beyond one routing
        // table refresh — the cheapest probe message.
        const PATH: &str = "/internal/session/changed";
        ensure!(
            post_from(a, b, PATH).await?.status() == 204,
            "healthy network yet the message did not arrive"
        );

        let cut = net.add(partition(&["alpha"], &["beta"]));
        let t0 = Instant::now();
        let err = post_from(a, b, PATH).await.expect_err("message got across");
        ensure!(
            t0.elapsed() < Duration::from_secs(1),
            "Drop waited {:?}",
            t0.elapsed()
        );
        ensure!(err.is_timeout(), "Drop must look like a net error: {err}");
        ensure!(
            post_from(b, a, PATH).await.is_err(),
            "the partition cuts only one direction"
        );
        ensure!(
            post_from(a, c, PATH).await?.status() == 204,
            "the alpha|beta partition also cuts gamma"
        );
        ensure!(
            post_from(a, a, PATH).await?.status() == 204,
            "a message to itself was cut"
        );
        // Count only probe messages: background messages (a heartbeat every renewal tick)
        // crossing the partition in this gap are cut too, and that is not what is checked.
        let probes = net.dropped().iter().filter(|d| d.ends_with(PATH)).count();
        ensure!(probes == 2, "{:?}", net.dropped());
        net.remove(cut);

        let iso = net.add(isolate("gamma"));
        ensure!(post_from(c, a, PATH).await.is_err() && post_from(b, c, PATH).await.is_err());
        ensure!(post_from(a, b, PATH).await?.status() == 204);
        net.remove(iso);

        net.add(hold_then_release(
            PathFilter::new(PATH).from("alpha").to("beta").limit(1),
        ));
        let sent = tokio::spawn({
            let from = a.cluster_secret.clone();
            let to = b.internal_url();
            async move {
                let client = reqwest::Client::new();
                from.send(&client, client.post(format!("{to}{PATH}")))
                    .await
                    .map(|r| r.status().as_u16())
            }
        });
        net.wait_held(1, Duration::from_secs(5)).await?;
        ensure!(!sent.is_finished(), "sender ignores the held message");
        // `limit(1)`: a later message goes through while the first one is still held.
        ensure!(post_from(a, b, PATH).await?.status() == 204);
        let released = net.release_held().await;
        ensure!(
            released.len() == 1 && released[0].1 == Ok(204),
            "{released:?}"
        );
        ensure!(
            sent.await?? == 204,
            "the sender did not get the response to the released message"
        );
        net.heal();

        net.add(PathFilter::new(PATH).verdict(Verdict::Delay(Duration::from_millis(300))));
        let t0 = Instant::now();
        ensure!(post_from(a, b, PATH).await?.status() == 204);
        ensure!(
            t0.elapsed() >= Duration::from_millis(300),
            "Delay does not slow: {:?}",
            t0.elapsed()
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    result
}

// ───────────────────────────────────────────── a stale message after a split

/// **A cross-range message arrives late, after the target range has split.**
///
/// Interleaving:
///
/// 1. `Caller` (range P, node alpha) calls `Callee` keyed into range D (node
///    beta). P's outbox drain pass sends `StartCall` to
///    `beta/internal/inbound?range=D&table=v` — the filter **holds** that message on the
///    wire; the sender waits (not yet past `SEND_TIMEOUT`).
/// 2. Beta splits D into L, R (`POST /ranges/D/split`); the table moves to `v + 1`, D
///    retires. D's quiesce waits for nothing: the message has not reached D.
/// 3. The test releases the message. The receiver sees a message routed to range D, which
///    no longer exists (and an old table) ⇒ `409 wrong_range` with its own table. The sender
///    learns the table (or has already reloaded it), recomputes the target, resends to the
///    right child.
///
/// Must see: the message is processed **exactly once**, in the right child range — `Callee`
/// has exactly one invocation, `Caller` completes with its result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_inbound_after_split_is_processed_once_in_right_child() -> Result<()> {
    const TTL: Duration = Duration::from_secs(2);
    let (_bucket, net, nodes) = net_cluster(&["alpha", "beta"], 2, TTL).await?;
    let refs: Vec<&ClusterNode> = nodes.iter().collect();
    let result = common::within(Duration::from_secs(120), "stale msg after split", async {
        let (p, d) = balanced_two(&refs, TTL).await?;
        let callee_key = common::key_on_partition(&nodes[1].node, "Callee", d, "callee");
        let caller_key = common::key_on_partition(&nodes[0].node, "Caller", p, "caller");
        let sdk = FakeSdk::builder()
            .object(
                "Caller",
                "run",
                vec![
                    Step::call("Callee", "echo", Some(&callee_key)),
                    Step::EchoCallResult,
                ],
            )
            .object("Callee", "echo", vec![Step::output("pong")])
            .spawn()
            .await?;
        for n in &nodes {
            common::register(&n.node.url(), &sdk.url).await?;
        }

        // Hold exactly the first message to range D; every later message (including
        // resends) goes through.
        net.add(hold_then_release(
            PathFilter::new("/internal/inbound")
                .range(d)
                .to("beta")
                .limit(1),
        ));
        let caller = send_acked(&refs, "Caller", "run", &caller_key).await?;
        net.wait_held(1, Duration::from_secs(10)).await?;

        let (code, view) = post_json(format!("{}/ranges/{d}/split", nodes[1].node.url())).await?;
        ensure!(code == 200, "split: {code} {view}");
        ensure!(
            !nodes[1].node.cluster.table().contains(PartitionId(d)),
            "D still in the table after the split"
        );

        let held = net.take_held();
        ensure!(held.len() == 1, "{held:?}");
        ensure!(
            held[0].sender_waiting(),
            "the sender gave up before the release — wrong interleaving"
        );
        let status = held.into_iter().next().expect("one msg").release().await;
        ensure!(
            status == Ok(409),
            "a stale message to a split range must get 409 wrong_range: {status:?}"
        );

        wait_completed(&refs, &caller, "pong", Duration::from_secs(20)).await?;
        let starts = sdk.starts_of("Callee", "echo");
        let ids: std::collections::BTreeSet<String> = starts.iter().map(|s| s.id_hex()).collect();
        ensure!(
            ids.len() == 1,
            "Callee ran for {} invocations (must be exactly one): {starts:?}",
            ids.len()
        );
        let callee_range = nodes[1].node.cluster.partition_of(
            &cairn_store::TenantId::default(),
            "Callee",
            &callee_key,
        );
        ensure!(
            callee_range.0 != d,
            "Callee's key still falls into the split range"
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    result
}

/// **The ack of a cross-range message is lost, then the target range splits, then the
/// resend arrives.**
///
/// Interleaving:
///
/// 1. `Caller` (range P, alpha) calls `Callee` (range D, beta). The `StartCall` message reaches
///    D and is **written durably** (`200`), but the ack does not come back: the sender sees a
///    network error, P's outbox row stays. The completion beta → alpha is held, so that
///    `Caller` does not complete (once it completes its outbox row is dropped, no resend).
/// 2. Beta splits D into L, R — the child range carries the `Callee` invocation D had
///    accepted (cloned from the sealed D). Alpha's resend in the meantime is held.
/// 3. Release the resends (old table ⇒ `409` ⇒ learn the table ⇒ reach the right child),
///    then release the completion.
///
/// Must see: the child receives the duplicate **without** creating a second invocation — `Callee`
/// runs for exactly one invocation, `Caller` completes with its result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_inbound_after_lost_ack_and_split_is_deduplicated() -> Result<()> {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    const TTL: Duration = Duration::from_secs(2);
    let (_bucket, net, nodes) = net_cluster(&["alpha", "beta"], 2, TTL).await?;
    let refs: Vec<&ClusterNode> = nodes.iter().collect();
    let result = common::within(Duration::from_secs(120), "ack lost then split", async {
        let (p, d) = balanced_two(&refs, TTL).await?;
        let callee_key = common::key_on_partition(&nodes[1].node, "Callee", d, "callee");
        let caller_key = common::key_on_partition(&nodes[0].node, "Caller", p, "caller");
        let sdk = FakeSdk::builder()
            .object(
                "Caller",
                "run",
                vec![
                    Step::call("Callee", "echo", Some(&callee_key)),
                    Step::EchoCallResult,
                ],
            )
            .object("Callee", "echo", vec![Step::output("pong")])
            .spawn()
            .await?;
        for n in &nodes {
            common::register(&n.node.url(), &sdk.url).await?;
        }
        let to_callee = |m: &str| m.starts_with("alpha→beta /internal/inbound");
        let hold_starts = Arc::new(AtomicBool::new(true));
        let hold_replies = Arc::new(AtomicBool::new(true));
        net.add(
            PathFilter::new("/internal/inbound")
                .from("alpha")
                .to("beta")
                .when(hold_starts.clone())
                .hold(),
        );
        net.add(
            PathFilter::new("/internal/inbound")
                .from("beta")
                .to("alpha")
                .when(hold_replies.clone())
                .hold(),
        );
        let caller = send_acked(&refs, "Caller", "run", &caller_key).await?;
        net.wait_held(1, Duration::from_secs(10)).await?;
        let first = net.take_held().into_iter().next().expect("one message");
        ensure!(first.path().contains(&format!("range={d}")), "{first:?}");
        let status = first.release_lose_reply().await;
        ensure!(status == Ok(200), "D must accept message 1: {status:?}");

        let (code, view) = post_json(format!("{}/ranges/{d}/split", nodes[1].node.url())).await?;
        ensure!(code == 200, "split: {code} {view}");
        // Wait for alpha to resend (held) — the duplicate must really exist.
        let t0 = Instant::now();
        while net.seen().iter().filter(|m| to_callee(m)).count() < 2 {
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "alpha did not resend: {:?}",
                net.seen()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        hold_starts.store(false, Ordering::SeqCst);
        let mut stale = Vec::new();
        for h in net.take_held() {
            if h.from() == "alpha" {
                stale.push(h.release().await);
            } else {
                // The completion was held: put it back, release it last.
                net_hold_back(&net, h);
            }
        }
        // Wait for the duplicate to reach the child range (a send through the new table goes straight there).
        let t0 = Instant::now();
        while !net
            .seen()
            .iter()
            .any(|m| to_callee(m) && !m.contains(&format!("range={d}&")))
        {
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "the duplicate did not reach the child range: {stale:?} {:?}",
                net.seen()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        hold_replies.store(false, Ordering::SeqCst);
        let _ = net.release_held().await;

        wait_completed(&refs, &caller, "pong", Duration::from_secs(30)).await?;
        let starts = sdk.starts_of("Callee", "echo");
        let ids: std::collections::BTreeSet<String> = starts.iter().map(|s| s.id_hex()).collect();
        ensure!(
            ids.len() == 1,
            "the duplicate after the split created a second invocation: {starts:?}"
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    result
}

/// Returns a message taken with `take_held` to `net`'s hold queue.
fn net_hold_back(net: &common::net::Net, h: cairn_server::netfilter::Held) {
    use cairn_server::netfilter::NetFilter;
    net.hold(h);
}

// ───────────────────────────────────────────── merge while cut off

/// **A merge while the coordinator is cut off from the other range's leader.**
///
/// Interleaving:
///
/// 1. Alpha leads range M, beta leads range T. Alpha receives
///    `POST /ranges/merge?left=M&right=T`: CAS a `Merging` intent onto both ranges.
/// 2. Alpha asks beta to yield T (`/internal/ranges/T/resign`) — the filter **drops** the
///    message. Beta is still alive, still renewing its session on the bucket, so alpha cannot
///    claim T (`Claim::Held`) until `settle` runs out.
/// 3. Alpha rolls back: CAS away the intent, clear the merged range's Db and owner record.
///
/// Must see: the merge returns an error; the table has **no intent** right after (nothing
/// pending past the limit); M and T still have one leader each; the merged range leaves no
/// Db or owner record; invocations acked on both ranges are intact. Once the network heals
/// the merge succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_rolls_back_cleanly_when_resign_is_cut() -> Result<()> {
    const TTL: Duration = Duration::from_secs(1);
    let (bucket, net, nodes) = net_cluster(&["alpha", "beta"], 2, TTL).await?;
    let refs: Vec<&ClusterNode> = nodes.iter().collect();
    let result = common::within(Duration::from_secs(90), "merge while cut off", async {
        let (mine, theirs) = balanced_two(&refs, TTL).await?;
        let sdk = FakeSdk::builder()
            .object("Counter", "add", vec![Step::output("ok")])
            .spawn()
            .await?;
        for n in &nodes {
            common::register(&n.node.url(), &sdk.url).await?;
        }
        let mut acked = Vec::new();
        for (p, n) in [(mine, 0usize), (theirs, 1)] {
            let key = common::key_on_partition(&nodes[n].node, "Counter", p, "before-merge");
            acked.push(send_acked(&refs, "Counter", "add", &key).await?);
        }

        let cut = net.add(
            PathFilter::new(&format!("/internal/ranges/{theirs}/resign"))
                .from("alpha")
                .to("beta"),
        );
        let into = nodes[0].node.cluster.table().next_range_id();
        let t0 = Instant::now();
        let (code, view) = post_json(format!(
            "{}/ranges/merge?left={mine}&right={theirs}",
            nodes[0].node.url()
        ))
        .await?;
        ensure!(
            code != 200,
            "merge succeeded although the other range could not be gathered: {view}"
        );
        ensure!(
            net.dropped().iter().any(|d| d.contains("/resign")),
            "the yield request was never cut — the test checks nothing: {:?}",
            net.dropped()
        );
        tracing::info!(code = code, after = ?t0.elapsed(), "merge returned an error as expected");

        // No intent pending: the coordinator rolls back before answering. Give the
        // other node one tick to reload the table (TTL) to see the same.
        let deadline = Instant::now() + TTL * 3;
        loop {
            let clean = nodes.iter().all(|n| {
                let t = n.node.cluster.table();
                t.contains(PartitionId(mine))
                    && t.contains(PartitionId(theirs))
                    && t.ranges().iter().all(|r| r.intent.is_none())
            });
            if clean {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "intent still pending after {:?}: {:?}",
                TTL * 3,
                nodes[0].node.cluster.table()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
            bucket.clone(),
            "",
            "memory://",
        ));
        ensure!(
            cairn_store::read_owner(
                stores.main().store(),
                stores.main().prefix(),
                PartitionId(into)
            )
            .await?
            .is_none(),
            "owner record of merged range {into} still there after the rollback"
        );
        ensure!(
            !stores.range_db_exists(PartitionId(into)).await?,
            "Db of merged range {into} still there after the rollback"
        );
        let probe = wait_probe(
            &refs,
            Duration::from_secs(10),
            "one leader per range",
            |p| single_leaders(p).is_some_and(|m| m.len() == 2),
        )
        .await?;
        ensure!(probe[&theirs] == [1], "beta lost its range: {probe:?}");
        for id in &acked {
            wait_completed(&refs, id, "ok", Duration::from_secs(15)).await?;
        }

        net.remove(cut);
        let (code, view) = post_json(format!(
            "{}/ranges/merge?left={mine}&right={theirs}",
            nodes[0].node.url()
        ))
        .await?;
        ensure!(
            code == 200,
            "network healed yet the merge still fails: {code} {view}"
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    result
}

// ───────────────────────────────────── merge after kill -9

/// **A merge right after the leader of a source range died, with the shared WAL**
/// (first seen in a chaos run with `--failpoints`:
/// `500 "Projection and/or union with WAL is not supported"`).
///
/// Interleaving:
///
/// 1. Alpha leads range M, beta leads range T, both on the shared WAL. Each
///    range has one acked invocation: T's owner record carries a `wal_segment`
///    pointing into beta's node log.
/// 2. Beta "dies like `kill -9`": its store hangs (`freeze`), it is cut off from
///    every peer (`isolate`). Beta does not seal, does not yield.
/// 3. Right then alpha receives `POST /ranges/merge?left=M&right=T`. The coordinator
///    CASes the intent, sees it does not lead T yet, reads the owner record (beta's)
///    and asks beta to yield T — the test **holds** that yield request.
/// 4. In that gap, **alpha's own** balancer sees beta's session dead and T unowned past
///    the rescue grace: alpha's engine claims T (keeper K1, term t), seals beta's log,
///    replays, CASes its own segment, flushes, drops beta's segment, then becomes
///    leader of T. The test waits until alpha answers as T's leader.
/// 5. The test drops the held message (the coordinator gets a network error). The
///    coordinator enters its claim loop for T: the owner record already belongs to alpha's
///    session ⇒ `claim` rewrites it with term t+1. The engine still runs with K1, whose
///    `owner_version` is now stale.
/// 6. The coordinator flushes and quiesces both ranges. T's seal completes, but K1's
///    "clean record" CAS (`writer_sealed`) misses and is only `warn`ed:
///    alpha's `wal_segment` is still on the owner record. `union` asks
///    `SharedWalAdmin::is_empty`, sees a segment left ⇒ refuses the source.
///
/// Must see: the merge does **not** return `500`. More than that, it completes on the first
/// try (`200`): at step 5 the coordinator sees the owner record already belongs to its own
/// session and does not rewrite it (`Session::claim_from_others`), it only waits for its
/// engine to lead T. The table has no intent; invocations acked on both ranges are intact
/// after the merge. (The `503` net when a source still has a `wal_segment` has its own test:
/// `shared_wal_cluster::split_when_clean_owner_cas_fails_after_seal_returns_503`.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_right_after_source_leader_dies_never_returns_500() -> Result<()> {
    const TTL: Duration = Duration::from_secs(1);
    let (bucket, net, nodes) = common::net::net_cluster(&["alpha", "beta"], 2, TTL).await?;
    let refs: Vec<&ClusterNode> = nodes.iter().collect();
    let alpha = &nodes[0].node;
    let result = common::within(Duration::from_secs(120), "merge after kill -9", async {
        let (mine, theirs) = balanced_two(&refs, TTL).await?;
        let sdk = FakeSdk::builder()
            .object("Counter", "add", vec![Step::output("ok")])
            .spawn()
            .await?;
        for n in &nodes {
            common::register(&n.node.url(), &sdk.url).await?;
        }
        let mut acked = Vec::new();
        for (p, n) in [(mine, 0usize), (theirs, 1)] {
            let key = common::key_on_partition(&nodes[n].node, "Counter", p, "before-merge");
            acked.push(send_acked(&refs, "Counter", "add", &key).await?);
        }
        let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
        let root = slatedb::object_store::path::Path::from("");
        let rec = cairn_store::read_owner(&store, &root, PartitionId(theirs))
            .await?
            .context("T has no owner record")?;
        ensure!(
            rec.wal_kind == cairn_store::WalKind::Shared && !rec.wal_segments.is_empty(),
            "T must be writing into beta's node log — the test checks nothing: {rec:?}"
        );

        // 2. kill -9. The rule holding the yield request comes before `isolate`: the first
        // rule returning something other than `Pass` wins.
        net.add(
            PathFilter::new(&format!("/internal/ranges/{theirs}/resign"))
                .from("alpha")
                .to("beta")
                .hold(),
        );
        net.add(isolate("beta"));
        common::freeze(&nodes[1].store);

        // 3. Merge right away — the coordinator stops at the held yield request.
        let url = format!("{}/ranges/merge?left={mine}&right={theirs}", alpha.url());
        let first = tokio::spawn(post_json(url));
        net.wait_held(1, Duration::from_secs(10)).await?;

        // 4. Alpha's balancer claims T and its engine becomes leader.
        wait_probe(
            &refs[..1],
            Duration::from_secs(20),
            "alpha becomes leader of T",
            |p| p.get(&theirs).is_some_and(|v| v == &[0]),
        )
        .await?;
        let rec = cairn_store::read_owner(&store, &root, PartitionId(theirs))
            .await?
            .context("T has no owner record")?;
        ensure!(
            rec.session.node == "alpha",
            "alpha leads T yet the owner record is not alpha's: {rec:?}"
        );

        // 5. Release the coordinator: the yield request becomes a network error.
        let held = net.take_held();
        ensure!(held.len() == 1, "{held:?}");
        drop(held);

        let (code, merged) = first.await??;
        ensure!(code != 500, "merge just after kill -9 gave 500: {merged}");
        // Nothing in this ordering is transient: the range is already alpha's, alpha's
        // engine leads it with a keeper still valid. A `503` here means the coordinator
        // overwrote its own owner record again.
        ensure!(code == 200, "merge must pass first time: {code} {merged}");
        let ranges = merged["ranges"]
            .as_array()
            .ok_or_else(|| anyhow!("{merged}"))?;
        ensure!(ranges.len() == 1, "{merged}");
        ensure!(
            alpha
                .cluster
                .table()
                .ranges()
                .iter()
                .all(|r| r.intent.is_none()),
            "intent still pending: {:?}",
            alpha.cluster.table()
        );
        for id in &acked {
            wait_completed(&refs[..1], id, "ok", Duration::from_secs(30)).await?;
        }
        Ok(())
    })
    .await;
    // Beta has "died": its store hangs, shutting it down would hang too — shut down only alpha.
    let mut it = nodes.into_iter();
    it.next().expect("alpha").node.shutdown().await;
    result
}

// ───────────────────────────────────────────── asymmetric isolation

/// **Asymmetric isolation — a split brain the lease does not see.**
///
/// Interleaving:
///
/// 1. Three nodes, three ranges, each node leads exactly one range (the fair share
///    ⌈3/3⌉ = 1 forces that; with four ranges {2, 2, 0} is also balanced and the third
///    node might never get a range). Alpha is cut off
///    from **every** peer (both directions) but still sees the bucket: it still renews its
///    session, can still CAS owner records, so it is still the valid leader of its
///    range. No node sees alpha as "dead".
/// 2. While cut off, the client sends (to the right leader) calls crossing the partition:
///    from alpha's range to another node's range and back. Every call is
///    acked (`202`, durable) — only the cross-range outbox rows cannot go.
/// 3. The "keep leadership" phase: heal the network ⇒ both sides' outboxes drain.
/// 4. The "lose leadership" phase: cut again, send more, then **shut down** alpha while
///    still cut off (the shutdown notice to peers is cut too) — the other nodes learn only
///    through the bucket, take alpha's range and drain the outbox alpha left behind.
///
/// Must see, at every probe: no range has two answering leaders; and in the end every
/// acked invocation completes with the right result — including calls whose
/// `StartCall`/completion messages to alpha's range were cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_node_still_on_bucket_loses_no_acked_work() -> Result<()> {
    isolated_node_scenario(false).await
}

/// Like [`isolated_node_still_on_bucket_loses_no_acked_work`], in **delegated
/// mode**: the gate is on, nodes other than the
/// coordinator renew by ack. Alpha cut off from every peer loses its acks —
/// if it is delegating it falls back to `direct` after ~TTL/3 and renews on the bucket
/// as before, keeping leadership (LS-3: the network is not the source of truth). If alpha
/// is the coordinator the other nodes fall back, and nobody expires anybody.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_delegated_node_still_on_bucket_loses_no_acked_work() -> Result<()> {
    isolated_node_scenario(true).await
}

async fn isolated_node_scenario(quiet: bool) -> Result<()> {
    const TTL: Duration = Duration::from_secs(2);
    let (bucket, net, mut nodes) =
        common::net::net_cluster_with(&["alpha", "beta", "gamma"], 3, TTL, quiet).await?;
    let result = common::within(Duration::from_secs(120), "asymmetric isolation", async {
        if quiet {
            let c = coordinator_index(&bucket, &nodes, TTL * 5).await?;
            common::until_delegated(&bucket, &nodes, c, TTL * 10).await?;
        }
        let refs: Vec<&ClusterNode> = nodes.iter().collect();
        let probe = wait_probe(
            &refs,
            balance_deadline(TTL),
            "each node has at least one range",
            |p| single_leaders(p).is_some_and(|m| common::load_of(&m, 3).iter().all(|n| *n >= 1)),
        )
        .await?;
        let leaders = single_leaders(&probe).expect("balanced");
        let alpha_ranges: Vec<u32> = leaders
            .iter()
            .filter(|(_, n)| **n == 0)
            .map(|(p, _)| *p)
            .collect();
        let other_ranges: Vec<u32> = leaders
            .iter()
            .filter(|(_, n)| **n != 0)
            .map(|(p, _)| *p)
            .collect();

        // Keys for each direction across the partition: (caller in the source range, callee in
        // the target range). Each call has its own caller service (`CallerN`): its script calls
        // exactly its callee. Keys hash over the service name too, so look keys up by
        // exactly that name.
        let key =
            |svc: &str, p: u32, tag: &str| common::key_on_partition(&nodes[0].node, svc, p, tag);
        let mut plans = Vec::new();
        for (from, to) in [
            (&alpha_ranges, &other_ranges),
            (&other_ranges, &alpha_ranges),
        ] {
            for (src, dst) in from.iter().zip(to.iter().cycle()) {
                let svc = format!("Caller{}", plans.len());
                let callee = key("Callee", *dst, &format!("callee-{}", plans.len()));
                let caller = key(&svc, *src, "caller");
                plans.push((svc, caller, callee));
            }
        }
        let mut sdk = FakeSdk::builder().object("Callee", "echo", vec![Step::output("pong")]);
        for (svc, _, callee) in &plans {
            sdk = sdk.object(
                svc,
                "run",
                vec![
                    Step::call("Callee", "echo", Some(callee)),
                    Step::EchoCallResult,
                ],
            );
        }
        let sdk = sdk.spawn().await?;
        for n in &nodes {
            common::register(&n.node.url(), &sdk.url).await?;
        }

        let check_single = |probe: &std::collections::BTreeMap<u32, Vec<usize>>| -> Result<()> {
            for (p, v) in probe {
                ensure!(v.len() <= 1, "range {p} has two answering leaders: {v:?}");
            }
            Ok(())
        };

        // ── The keep-leadership phase.
        let cut = net.add(isolate("alpha"));
        let mut acked = Vec::new();
        for (svc, caller, _) in &plans {
            acked.push(send_acked(&refs, svc, "run", caller).await?);
        }
        net.wait_dropped(1, Duration::from_secs(10)).await?;
        for _ in 0..10 {
            check_single(&common::leaders_by_probe(&refs).await)?;
            tokio::time::sleep(TTL / 4).await;
        }
        let still = common::leaders_by_probe(&refs).await;
        for p in &alpha_ranges {
            ensure!(
                still.get(p).is_some_and(|v| v == &[0]),
                "alpha lost range {p} although it still sees the bucket: {still:?}"
            );
        }
        net.remove(cut);
        for id in &acked {
            wait_completed(&refs, id, "pong", Duration::from_secs(30)).await?;
        }

        // ── The lose-leadership phase: cut, send more, then shut alpha down while cut off.
        let cut = net.add(isolate("alpha"));
        let before = net.dropped().len();
        let mut acked2 = Vec::new();
        for (svc, caller, _) in &plans {
            acked2.push(send_acked(&refs, svc, "run", caller).await?);
        }
        net.wait_dropped(before + 1, Duration::from_secs(10))
            .await?;
        check_single(&common::leaders_by_probe(&refs).await)?;
        let alpha = nodes.remove(0);
        alpha.node.shutdown().await;
        let rest: Vec<&ClusterNode> = nodes.iter().collect();
        wait_probe(
            &rest,
            Duration::from_secs(30),
            "beta, gamma take every range",
            |p| single_leaders(p).is_some_and(|m| m.len() == 3),
        )
        .await?;
        net.remove(cut);
        for id in acked.iter().chain(&acked2) {
            wait_completed(&rest, id, "pong", Duration::from_secs(30)).await?;
        }
        check_single(&common::leaders_by_probe(&rest).await)?;
        Ok(())
    })
    .await
    .context("asymmetric isolation");
    for n in nodes {
        n.node.shutdown().await;
    }
    result
}

/// **Every node heartbeats to the coordinator every `renew_interval`**,
/// through `ClusterSecret::send` — i.e. through the network filter.
///
/// 1. Three nodes `a`, `b`, `c`, TTL 1 s (heartbeat tick TTL/5 = 200 ms); wait for
///    `control/coordinator` to point at one of the three (`C`) and one stable TTL.
/// 2. Count `POST /internal/heartbeat` messages over 5·TTL.
///
/// Must see: every node other than `C` sends `C` 15–40 heartbeats (≈ 25); `C` sends to
/// nobody; nobody sends a heartbeat to a node that is not the coordinator.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_node_heartbeats_the_coordinator_each_renew_interval() -> Result<()> {
    let ttl = Duration::from_secs(1);
    let (bucket, net, nodes) = net_cluster(&["a", "b", "c"], 4, ttl).await?;
    let r = async {
        let t0 = Instant::now();
        let c = loop {
            let seen = common::coordinator_of(&bucket).await;
            let found = seen.as_ref().filter(|s| !s.released).and_then(|s| {
                nodes.iter().position(|n| {
                    n.node
                        .session
                        .current()
                        .is_some_and(|id| id.to_string() == s.session)
                })
            });
            if let Some(i) = found {
                break i;
            }
            ensure!(t0.elapsed() < ttl * 5, "no coordinator yet: {seen:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let name = |i: usize| ["a", "b", "c"][i];
        tokio::time::sleep(ttl).await;
        let skip = net.seen().len();
        tokio::time::sleep(ttl * 5).await;
        let beats: Vec<String> = net.seen()[skip..]
            .iter()
            .filter(|m| m.ends_with(" /internal/heartbeat"))
            .cloned()
            .collect();
        for i in 0..3 {
            let sent = beats
                .iter()
                .filter(|m| m.starts_with(&format!("{}→", name(i))))
                .count();
            if i == c {
                ensure!(
                    sent == 0,
                    "coordinator {} sends {sent} heartbeats to itself",
                    name(c)
                );
                continue;
            }
            let to_c = beats
                .iter()
                .filter(|m| **m == format!("{}→{} /internal/heartbeat", name(i), name(c)))
                .count();
            ensure!(
                (15..=40).contains(&to_c) && to_c == sent,
                "{} sends {to_c} heartbeats to coordinator {} (total {sent}) in 5·TTL",
                name(i),
                name(c)
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

/// Waits for `control/coordinator` to point at the current incarnation of a node in
/// `nodes`; returns its index.
async fn coordinator_index(
    bucket: &std::sync::Arc<slatedb::object_store::memory::InMemory>,
    nodes: &[ClusterNode],
    within: Duration,
) -> Result<usize> {
    let t0 = Instant::now();
    loop {
        let seen = common::coordinator_of(bucket).await;
        let found = seen.as_ref().filter(|s| !s.released).and_then(|s| {
            nodes.iter().position(|n| {
                n.node
                    .session
                    .current()
                    .is_some_and(|id| id.to_string() == s.session)
            })
        });
        if let Some(i) = found {
            return Ok(i);
        }
        ensure!(t0.elapsed() < within, "no coordinator yet: {seen:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Peer-session GETs issued by `n`'s tracker (source `session.tracker`).
fn tracker_gets(n: &ClusterNode) -> u64 {
    n.store
        .counts_by_source()
        .into_iter()
        .filter(|(src, op, _, _)| *src == "session.tracker" && *op == cairn_store::testkit::Op::Get)
        .map(|(_, _, _, c)| c)
        .sum()
}

/// The tracker GET cap of a node listening to `view` with `peers` peers within `window`
/// (TTL 1 s): each peer every `SPARSE_AFTER_TTLS` TTL, plus one round for phase skew —
/// a full tracker (TTL/3) GETs each peer 6 times in 2·TTL.
fn sparse_gets(peers: u64, window: Duration) -> u64 {
    let every = Duration::from_secs(1) * cairn_store::SPARSE_AFTER_TTLS;
    peers * (window.as_millis() / every.as_millis()) as u64 + peers
}

/// Every node except `c` is listening to `view`: its tracker GETs no more than `most` times
/// within `window`.
async fn followers_quiet(
    nodes: &[ClusterNode],
    c: usize,
    window: Duration,
    most: u64,
) -> Result<()> {
    let before: Vec<u64> = nodes.iter().map(tracker_gets).collect();
    tokio::time::sleep(window).await;
    for (i, n) in nodes.iter().enumerate() {
        if i == c {
            continue;
        }
        let gets = tracker_gets(n) - before[i];
        ensure!(
            gets <= most,
            "node {i} not listening to view yet: tracker GET {gets} times in {window:?} (cap {most})"
        );
    }
    Ok(())
}

/// **The coordinator is unreachable ⇒ `view` goes stale ⇒ nodes return to their own
/// tracker within ≤ TTL/3 + one tick** (the fallback is the protocol from before
/// the coordinator existed).
///
/// 1. Three nodes `a`, `b`, `c`, TTL 1 s; wait for coordinator `C`; the other two listen to
///    `view` (their trackers barely GET — checked over 2·TTL).
/// 2. Cut `C` off from every peer (`isolate`): heartbeats do not arrive, acks do not return.
///    `C` still sees the bucket, still alive.
///
/// Must see: each other node GETs peer sessions again (full tracker) within
/// TTL/3 + TTL/5 + 200 ms from the cut — once the last ack is older than TTL/3
/// it stops trusting `view`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_view_puts_nodes_back_on_their_own_tracker() -> Result<()> {
    let ttl = Duration::from_secs(1);
    let (bucket, net, nodes) = net_cluster(&["a", "b", "c"], 4, ttl).await?;
    let r = async {
        let c = coordinator_index(&bucket, &nodes, ttl * 5).await?;
        tokio::time::sleep(ttl * 2).await;
        followers_quiet(&nodes, c, ttl * 2, sparse_gets(2, ttl * 2)).await?;
        let name = ["a", "b", "c"][c];
        let before: Vec<u64> = nodes.iter().map(tracker_gets).collect();
        let cut = Instant::now();
        let _rule = net.add(isolate(name));
        let bound = ttl / 3 + ttl / 5 + Duration::from_millis(200);
        let mut back = vec![false; nodes.len()];
        back[c] = true;
        while back.iter().any(|b| !b) {
            for (i, n) in nodes.iter().enumerate() {
                back[i] |= tracker_gets(n) > before[i];
            }
            ensure!(
                cut.elapsed() <= bound || back.iter().all(|b| *b),
                "{:?} after cutting off coordinator {name}, node not back on its tracker: {back:?}",
                cut.elapsed()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }
    .await;
    net.heal();
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **The coordinator still gets heartbeats but cannot read the session of a dead node:
/// that node is still expired** (the coordinator is not a single point of failure for
/// detection).
///
/// 1. Four nodes `a`, `b`, `c`, `d`, TTL 1 s; wait for coordinator `C` (not
///    `d` — the test runs only when `C ≠ d`, otherwise swap roles with another `d`); the
///    other three listen to `view` (trackers almost silent — checked over 2·TTL).
/// 2. `C`'s store refuses every GET of `sessions/<d>/…`: `C`'s tracker never sees
///    `d`'s beat stop, so it cannot expire it; `C`'s fence and heartbeats
///    keep running.
/// 3. `d` "dies": store frozen, network cut (no heartbeat, no renewal).
///
/// Must see: `d`'s session record becomes `expired` within 5·TTL — after
/// 2·TTL of silence `view` marks `d` suspect, and the nodes receiving `view` observe
/// it themselves. Without the suspect mark only the sparse tracker catches it
/// ([`dead_node_is_expired_by_the_sparse_tracker_without_suspects`]).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dead_node_is_expired_when_the_coordinator_cannot_read_it() -> Result<()> {
    unreadable_dead_node_scenario(false, true, Duration::from_secs(5)).await
}

/// Like [`dead_node_is_expired_when_the_coordinator_cannot_read_it`], **without suspect
/// marks** (`NodeConfig::view_suspects = false`): the only net left is the
/// sparse tracker of the nodes listening to `view` (LS-1).
///
/// 1–3. As in the test above; coordinator `C` never marks `d` suspect.
/// 4. Each node listening to `view` GETs `d` every `SPARSE_AFTER_TTLS` TTL. The first GET
///    after `d` dies usually sees a newer beat than the previous one (`d` renewed every
///    TTL/5 until it died) — it only resets the silence mark; the second GET sees a beat
///    stopped for ≥ TTL ⇒ CAS `expired`.
///
/// Must see: `d`'s record becomes `expired` within ≤ 2·`SPARSE_AFTER_TTLS`·TTL
/// plus two observation ticks (TTL/3) plus a margin — with `SPARSE_AFTER_TTLS` = 2 that is
/// ~4·TTL + a tick; with the former 6 it was ~12·TTL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dead_node_is_expired_by_the_sparse_tracker_without_suspects() -> Result<()> {
    let ttl = Duration::from_secs(1);
    let bound = ttl * 4 + (ttl / 3) * 2 + Duration::from_millis(300);
    unreadable_dead_node_scenario(false, false, bound).await
}

/// Like [`dead_node_is_expired_when_the_coordinator_cannot_read_it`], in **delegated
/// mode**: `d` delegates to `C`, so only `C` expires
/// it (the others yield — LS-15) — and `C` cannot read its record. `C`
/// CASes `expired` If-Match **the version it holds** (its last read before the GETs
/// failed) once τ is a TTL old (the expire rule), without reading again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delegated_dead_node_is_expired_when_the_coordinator_cannot_read_it() -> Result<()> {
    unreadable_dead_node_scenario(true, true, Duration::from_secs(5)).await
}

/// `quiet`: the quiet-lease gate on (delegation), otherwise an explicit `direct`;
/// `suspects`: the coordinator marks
/// suspects; `bound`: the limit from `d`'s death until its record is `expired`.
async fn unreadable_dead_node_scenario(quiet: bool, suspects: bool, bound: Duration) -> Result<()> {
    use {
        cairn_store::testkit::{Action, OpSet, PathMatch, Rule},
        slatedb::object_store::ObjectStoreExt,
    };
    let ttl = Duration::from_secs(1);
    let ids = ["a", "b", "c", "d"];
    let (bucket, net, nodes) = common::net::net_cluster_tweaked(&ids, 4, ttl, quiet, |cfg| {
        cfg.view_suspects = suspects;
        // The two `direct` variants (suspect marks / sparse tracker, LS-1) are the controls
        // for the delegated variant: explicitly turn off the gate that a node creating the
        // bucket turns on by default.
        if !quiet {
            common::direct(cfg);
        }
    })
    .await?;
    let r = async {
        let c = coordinator_index(&bucket, &nodes, ttl * 5).await?;
        // `d` is the last node that is not the coordinator.
        let d = (0..ids.len()).rev().find(|i| *i != c).expect("four nodes");
        if quiet {
            common::until_delegated(&bucket, &nodes, c, ttl * 10).await?;
        }
        tokio::time::sleep(ttl * 2).await;
        followers_quiet(&nodes, c, ttl * 2, sparse_gets(3, ttl * 2)).await?;
        let d_id = nodes[d].node.session.current().expect("d alive");
        nodes[c].store.add_rule(Rule::new(
            OpSet::GET,
            PathMatch::Contains(format!("sessions/{}/", ids[d])),
            Action::Fail { times: None },
        ));
        common::freeze(&nodes[d].store);
        let _cut = net.add(isolate(ids[d]));
        let killed = Instant::now();
        loop {
            let rec = bucket
                .get(&slatedb::object_store::path::Path::from(format!(
                    "sessions/{}/{}",
                    d_id.node, d_id.inc
                )))
                .await;
            let text = match rec {
                Ok(g) => String::from_utf8(g.bytes().await?.to_vec())?,
                Err(_) => String::new(),
            };
            if text.lines().any(|l| l == "state=expired") {
                break;
            }
            if killed.elapsed() >= bound {
                // Diagnostics (flaky under background load): is the coordinator
                // role still at `C`, and which mode is `d`'s record in.
                let coord = match bucket
                    .get(&slatedb::object_store::path::Path::from("control/coordinator"))
                    .await
                {
                    Ok(g) => String::from_utf8_lossy(&g.bytes().await?).into_owned(),
                    Err(e) => format!("<{e}>"),
                };
                bail!(
                    "{bound:?} after {} died, its session is still not expired (coordinator {} cannot read it, suspect marks {suspects}); record of {}: {text:?}; control/coordinator: {coord:?}",
                    ids[d],
                    ids[c],
                    ids[d]
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(d)
    }
    .await;
    net.heal();
    let dead = r.as_ref().ok().copied();
    for (i, n) in nodes.into_iter().enumerate() {
        if Some(i) != dead {
            n.node.shutdown().await;
        }
    }
    r.map(|_| ())
}

// ───────────────────────── owner watch through `view` (LS-6)

/// Whether the engine of range `r` on `n` is leading — asked with `flow`, answered
/// on the spot, without the read fence (LS-5): the fence blocks the old owner's read
/// answers but does not make it step down (see `cluster_lease.rs::leads`).
async fn leads(n: &ClusterNode, r: PartitionId) -> bool {
    match n.node.partitions.get(r) {
        Some(h) => matches!(
            tokio::time::timeout(Duration::from_millis(500), h.flow()).await,
            Ok(Ok(_))
        ),
        None => false,
    }
}

/// Three nodes `a`, `b`, `c` with a shared network filter, three ranges, owner-watch net
/// `grid`; waits for each node to lead exactly one range and for a coordinator. Returns
/// `(bucket, net, nodes, A, r)`: `A` is a node that is **not** the coordinator, `r` the
/// range it leads.
async fn owner_watch_cluster(
    ttl: Duration,
    grid: Duration,
) -> Result<(
    common::Bucket,
    std::sync::Arc<common::net::Net>,
    Vec<ClusterNode>,
    usize,
    PartitionId,
)> {
    let shared = common::bucket();
    let net = common::net::Net::new();
    let ids = ["a", "b", "c"];
    let mut nodes = Vec::new();
    for id in ids {
        let filter: std::sync::Arc<dyn cairn_server::netfilter::NetFilter> = net.clone();
        let n = common::start_cluster_node_tweaked(&shared, id, 3, ttl, |cfg| {
            cfg.net_filter = Some(filter);
            cfg.owner_watch = Some(grid);
        })
        .await?;
        net.register(id, &n.node);
        nodes.push(n);
    }
    let refs: Vec<_> = nodes.iter().collect();
    let seen = wait_probe(
        &refs,
        Duration::from_secs(30),
        "each node leads one range",
        |p| {
            single_leaders(p)
                .is_some_and(|l| (0..3).all(|i| l.values().filter(|n| **n == i).count() == 1))
        },
    )
    .await?;
    let leaders = single_leaders(&seen).expect("balanced");
    let c = coordinator_index(&shared, &nodes, ttl * 5).await?;
    let (r, a) = leaders
        .iter()
        .find(|(_, n)| **n != c)
        .map(|(r, n)| (PartitionId(*r), *n))
        .expect("a node that is not the coordinator");
    ensure!(leads(&nodes[a], r).await, "{} does not lead {r:?}", ids[a]);
    Ok((shared, net, nodes, a, r))
}

/// Freezes `A` then waits for another node to lead `r`; returns when the claimer was seen
/// leading and its index.
async fn freeze_and_wait_claim(
    nodes: &[ClusterNode],
    a: usize,
    r: PartitionId,
) -> Result<(Instant, usize)> {
    nodes[a].node.session.crash_handle().crash();
    let t0 = Instant::now();
    loop {
        for (i, n) in nodes.iter().enumerate() {
            if i != a && leads(n, r).await {
                return Ok((Instant::now(), i));
            }
        }
        ensure!(
            t0.elapsed() < Duration::from_secs(20),
            "nobody claimed {r:?} after node {a} froze"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// **The owner-change notice is lost: the old owner still steps down within
/// ≤ 2·`renew_interval` thanks to the coordinator's `view`** (layer 3 of LS-6).
///
/// Interleaving:
/// 1. Three nodes `a`, `b`, `c`, three ranges, TTL 1 s (`renew_interval` = TTL/5 =
///    200 ms), owner-watch net **10 minutes**. Each node leads one range; `A` is
///    a node that is not the coordinator, `r` its range.
/// 2. Block every `POST /internal/owner/changed` (the claimer's notice).
/// 3. `A` freezes (`crash_handle`): its renewal loop and tracker die, the pinned grant stays
///    open — layer 1 off; `A` does not write so layer 2 is off. `A`'s HTTP and control
///    loop (heartbeats to the coordinator) keep running.
/// 4. After one TTL of silence, another node CASes `A`'s session `expired` and claims
///    `r`; its notice to `A` is dropped.
/// 5. The claimer's next heartbeat carries `owns` with `r`; the coordinator's `view`
///    records the claimer holding `r`; `A` receives that `view` through the ack of its own
///    heartbeat.
///
/// Must see: within 2·`renew_interval` (+ 500 ms margin for the owner-record GET, the
/// engine's step-down and the `flow` probe) from when the claimer leads `r`, `A`'s engine
/// for `r` stops leading; the claimer's notice was really blocked.
/// Before `view` carried owners: only the 10-minute net saw it — `A` believed it led for the whole window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_owner_steps_down_from_the_coordinator_view_when_the_notice_is_lost() -> Result<()> {
    let ttl = Duration::from_secs(1);
    let renew_interval = ttl / 5;
    let (_bucket, net, nodes, a, r) = owner_watch_cluster(ttl, Duration::from_secs(600)).await?;
    let _cut = net.add(PathFilter::new("/internal/owner/changed"));
    let res = async {
        let (claimed, by) = freeze_and_wait_claim(&nodes, a, r).await?;
        let bound = renew_interval * 2 + Duration::from_millis(500);
        while leads(&nodes[a], r).await {
            ensure!(
                claimed.elapsed() < bound,
                "{:?} after node {by} leads {r:?} (notice blocked), node {a} still believes it \
                 leads — the coordinator's `view` did not make it recheck the owner record",
                claimed.elapsed()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let stepped = claimed.elapsed();
        net.wait_dropped(1, Duration::from_secs(5))
            .await
            .context("the claimer's notice was not blocked — the test cannot measure `view`")?;
        ensure!(
            net.dropped()
                .iter()
                .any(|m| m.ends_with(" /internal/owner/changed")),
            "the blocked message is not an owner-change notice: {:?}",
            net.dropped()
        );
        eprintln!("old owner stepped down {stepped:?} after the claimer led (cap {bound:?})");
        Ok(())
    }
    .await;
    net.heal();
    for n in nodes {
        n.node.shutdown().await;
    }
    res
}

/// **The notice is lost and `A` is cut off from the coordinator: the old owner steps down
/// at the owner-watch net** ("both lost: ≤ the net"; layer 3 of LS-6).
///
/// Interleaving as in
/// [`old_owner_steps_down_from_the_coordinator_view_when_the_notice_is_lost`],
/// but with a 3 s owner-watch net and `A` cut off from **every** peer (`isolate`) before
/// freezing: no notice, no `view`. `A` still sees the bucket.
///
/// Must see: `A`'s engine for `r` stops leading within the net + 1 s from when
/// the claimer leads `r` (the net GETs each range's owner record; LS-3: only the bucket
/// revokes a grant).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_owner_cut_from_notice_and_coordinator_steps_down_at_the_grid() -> Result<()> {
    let ttl = Duration::from_secs(1);
    let grid = Duration::from_secs(3);
    let (_bucket, net, nodes, a, r) = owner_watch_cluster(ttl, grid).await?;
    let _cut = net.add(isolate(["a", "b", "c"][a]));
    let res = async {
        let (claimed, by) = freeze_and_wait_claim(&nodes, a, r).await?;
        let bound = grid + Duration::from_secs(1);
        while leads(&nodes[a], r).await {
            ensure!(
                claimed.elapsed() < bound,
                "{:?} after node {by} leads {r:?}, node {a} (cut off from every peer) still believes it \
                 leads — the owner-watch net {grid:?} did not run",
                claimed.elapsed()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    }
    .await;
    net.heal();
    for n in nodes {
        n.node.shutdown().await;
    }
    res
}

// ─────────────────── delegated renewal (LS-14/LS-15)

/// A three-node cluster with delegation done and ranges spread (each node at least one range):
/// (bucket, network, nodes, coordinator index, owner of each range).
async fn delegated_cluster(
    ids: &[&str],
    ttl: Duration,
) -> Result<(
    std::sync::Arc<slatedb::object_store::memory::InMemory>,
    std::sync::Arc<common::net::Net>,
    Vec<ClusterNode>,
    usize,
    std::collections::BTreeMap<u32, usize>,
)> {
    let (bucket, net, nodes) =
        common::net::net_cluster_with(ids, ids.len() as u16, ttl, true).await?;
    let setup = async {
        let c = coordinator_index(&bucket, &nodes, ttl * 5).await?;
        common::until_delegated(&bucket, &nodes, c, ttl * 10).await?;
        let refs: Vec<&ClusterNode> = nodes.iter().collect();
        let probe = wait_probe(
            &refs,
            balance_deadline(ttl),
            "each node leads one range",
            |p| {
                single_leaders(p)
                    .is_some_and(|m| common::load_of(&m, ids.len()).iter().all(|n| *n >= 1))
            },
        )
        .await?;
        common::until_delegated(&bucket, &nodes, c, ttl * 5).await?;
        Ok::<_, anyhow::Error>((c, single_leaders(&probe).expect("balanced")))
    }
    .await;
    match setup {
        Ok((c, leaders)) => Ok((bucket, net, nodes, c, leaders)),
        Err(e) => {
            for n in nodes {
                n.node.shutdown().await;
            }
            Err(e)
        }
    }
}

/// At every probe within `window`: no range has two leaders, every node keeps exactly
/// incarnation `incs` (except `skip`), and every range of `keep` stays on it.
async fn steady(
    nodes: &[ClusterNode],
    incs: &[Option<cairn_store::SessionId>],
    keep: &std::collections::BTreeMap<u32, usize>,
    skip: Option<usize>,
    window: Duration,
) -> Result<()> {
    let refs: Vec<&ClusterNode> = nodes.iter().collect();
    let t0 = Instant::now();
    while t0.elapsed() < window {
        let p = common::leaders_by_probe(&refs).await;
        for (r, v) in &p {
            ensure!(v.len() <= 1, "range {r} has two answering leaders: {v:?}");
        }
        for (r, owner) in keep {
            ensure!(
                p.get(r).is_some_and(|v| v == &[*owner]),
                "range {r} left node {owner} after {:?}: {p:?}",
                t0.elapsed()
            );
        }
        for (i, n) in nodes.iter().enumerate() {
            ensure!(
                Some(i) == skip || n.node.session.current() == incs[i],
                "node {i} changed incarnation after {:?}",
                t0.elapsed()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

/// **A node cut off from the coordinator but still seeing the bucket keeps its leadership**
/// (LS-3: the network is not the source of truth).
///
/// 1. Three nodes `a`, `b`, `c` with delegation done; `N` other than coordinator `C`.
/// 2. Cut `N` ↔ `C` (only that pair, both directions): `N`'s heartbeats do not arrive, acks
///    do not return.
/// 3. Past TTL/3 + jitter from the last acked heartbeat, `N` CASes its record back to
///    `direct`, `beat+1` — one LS-2 renewal — then renews on the bucket as
///    before. `C` sees `N`'s beat rise: no expiry.
///
/// Must see: within one TTL `N`'s record is `direct`; throughout 4·TTL `N` keeps its
/// incarnation and all its ranges, no range has two leaders.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_cut_from_coordinator_but_on_bucket_keeps_leadership() -> Result<()> {
    let ttl = Duration::from_secs(1);
    let ids = ["a", "b", "c"];
    let (bucket, net, nodes, c, leaders) = delegated_cluster(&ids, ttl).await?;
    let r = async {
        let n = (c + 1) % ids.len();
        let incs: Vec<_> = nodes.iter().map(|x| x.node.session.current()).collect();
        let _cut = net.add(partition(&[ids[n]], &[ids[c]]));
        let t0 = Instant::now();
        while !common::is_direct(&bucket, &nodes[n]).await {
            ensure!(
                t0.elapsed() < ttl,
                "a node cut off from the coordinator did not fall back to direct after one TTL"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        steady(&nodes, &incs, &leaders, None, ttl * 4).await
    }
    .await;
    net.heal();
    for x in nodes {
        x.node.shutdown().await;
    }
    r
}

/// **A coordinator cut off from every peer (still seeing the bucket) expires nobody**
/// (network partition).
///
/// 1. Three nodes delegated to `C`.
/// 2. Isolate `C` from every peer. The other two lose their acks, fall back to `direct` (CAS
///    `beat+1`), renew on the bucket.
/// 3. `C`'s tracker reads their records: the beat rises ⇒ a new τ, the record is now
///    `direct` ⇒ LS-1 as before; `C` still renews on the bucket so they do not
///    expire `C`.
///
/// Must see: throughout 4·TTL no node changes incarnation, no range changes owner
/// or has two leaders; within one TTL the other two nodes have `direct` records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_cut_from_all_peers_expires_nobody() -> Result<()> {
    let ttl = Duration::from_secs(1);
    let ids = ["a", "b", "c"];
    let (bucket, net, nodes, c, leaders) = delegated_cluster(&ids, ttl).await?;
    let r = async {
        let incs: Vec<_> = nodes.iter().map(|x| x.node.session.current()).collect();
        let _cut = net.add(isolate(ids[c]));
        let t0 = Instant::now();
        loop {
            let mut all = true;
            for (i, x) in nodes.iter().enumerate() {
                all &= i == c || common::is_direct(&bucket, x).await;
            }
            if all {
                break;
            }
            ensure!(
                t0.elapsed() < ttl,
                "node did not fall back to direct one TTL after the coordinator was cut off"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        steady(&nodes, &incs, &leaders, None, ttl * 4).await
    }
    .await;
    net.heal();
    for x in nodes {
        x.node.shutdown().await;
    }
    r
}

/// **A node falling back to `direct` does not expire a peer that is still delegating**
/// (the yield rule LS-15: an observer other than the coordinator does not apply LS-1 to a
/// `delegated` record).
///
/// 1. Three nodes delegated to `C`; `N`, `P` are the other two.
/// 2. Cut `N` ↔ `C`: `N` falls back to `direct`, its `view` goes stale, its tracker returns to
///    full mode — GETting `P`'s record every TTL/3. `P` is still acked by `C` and writes
///    nothing: its beat stays still forever.
///
/// Must see: throughout 4·TTL the record of `P`'s incarnation is never
/// `expired`; nobody changes incarnation, no range changes owner. Before the yield
/// rule (LS-1 applied to every record): `N` expired `P` after one TTL.
///
/// `P` falling back to `direct` **on its own** is valid (an ack late past TTL/3 + jitter
/// — at TTL 1 s, one heartbeat ~200 ms late under background load is enough), not a
/// bug; but from then on its record no longer exercises the yield rule. Falling back before
/// [`YIELD_COVER_TTLS`] TTL from when `N` went `direct` ⇒ the run is inconclusive,
/// rebuild the cluster and run again (at most [`YIELD_ATTEMPTS`] runs).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_falling_back_never_expires_a_peer_still_delegated() -> Result<()> {
    let mut early = Vec::new();
    for _ in 0..YIELD_ATTEMPTS {
        match falling_back_scenario().await? {
            None => return Ok(()),
            Some(after) => early.push(after),
        }
    }
    bail!(
        "P fell back to direct on its own before {YIELD_COVER_TTLS}·TTL in all {YIELD_ATTEMPTS} runs (after {early:?}) — the yield rule was not exercised"
    )
}

/// `P`'s `delegated` record must stand this many TTLs from when `N` went
/// `direct` for the run to exercise the yield rule: without that rule, `N`'s full tracker
/// (GET every TTL/3) expires `P` at the first GET once the record has been silent for a
/// TTL — i.e. within TTL/3 + a margin.
const YIELD_COVER_TTLS: u32 = 2;

/// The maximum number of runs of [`node_falling_back_never_expires_a_peer_still_delegated`].
const YIELD_ATTEMPTS: usize = 3;

/// One run of [`node_falling_back_never_expires_a_peer_still_delegated`]:
/// `Ok(None)` — checked; `Ok(Some(t))` — `P` fell back to `direct` on its own after `t`, before
/// [`YIELD_COVER_TTLS`] TTL (inconclusive); `Err` — a violation.
async fn falling_back_scenario() -> Result<Option<Duration>> {
    use slatedb::object_store::{path::Path, ObjectStoreExt};
    let ttl = Duration::from_secs(1);
    let ids = ["a", "b", "c"];
    let (bucket, net, nodes, c, leaders) = delegated_cluster(&ids, ttl).await?;
    let r = async {
        let n = (c + 1) % ids.len();
        let p = (c + 2) % ids.len();
        let incs: Vec<_> = nodes.iter().map(|x| x.node.session.current()).collect();
        let p_id = incs[p].clone().expect("P alive");
        let p_path = Path::from(format!("sessions/{}/{}", p_id.node, p_id.inc));
        let _cut = net.add(partition(&[ids[n]], &[ids[c]]));
        let t0 = Instant::now();
        while !common::is_direct(&bucket, &nodes[n]).await {
            ensure!(
                t0.elapsed() < ttl,
                "a node cut off from the coordinator did not fall back to direct"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let watch = async {
            let t1 = Instant::now();
            let mut fell_back = None;
            while t1.elapsed() < ttl * 4 {
                let text = match bucket.get(&p_path).await {
                    Ok(g) => String::from_utf8(g.bytes().await?.to_vec())?,
                    Err(e) => bail!("P's record lost after {:?}: {e}", t1.elapsed()),
                };
                ensure!(
                    !text.lines().any(|l| l == "state=expired"),
                    "the record of a peer still delegating was expired after {:?} (P fell back to direct: {fell_back:?}): {text:?}",
                    t1.elapsed()
                );
                if fell_back.is_none() && !text.lines().any(|l| l == "mode=delegated") {
                    fell_back = Some(t1.elapsed());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok::<_, anyhow::Error>(fell_back)
        };
        let (a, b) = tokio::join!(watch, steady(&nodes, &incs, &leaders, None, ttl * 4));
        let fell_back = a?;
        b?;
        Ok(fell_back.filter(|t| *t < ttl * YIELD_COVER_TTLS))
    }
    .await;
    net.heal();
    for x in nodes {
        x.node.shutdown().await;
    }
    r
}

/// **A delegated node expired while frozen wakes up to no ack at all
/// and loses its session** (the ack rule).
///
/// 1. Three nodes with delegation done; `N` other than `C` holds at least one range.
/// 2. `N` "freezes": every request to its store blocks at a gate, and it is
///    isolated from every peer.
/// 3. `C` receives no heartbeat from `N`: one TTL after the last acked heartbeat
///    it CASes `N`'s record `expired`; another node claims `N`'s ranges.
/// 4. Open the gate, heal the network. `N`'s pending write misses its CAS; heartbeats (if
///    the old incarnation is still around) get `expired`, never `ack`.
///
/// Must see: the record of `N`'s old incarnation is `expired` and never comes back to
/// life (an unconditional `closed` write by `lose` on `N` may overwrite it — still ended);
/// `N` opens a new incarnation; no probe after it wakes sees two leaders.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_node_waking_gets_no_ack_and_loses() -> Result<()> {
    use cairn_store::testkit::{Action, Gate, OpSet, PathMatch, Rule};
    let ttl = Duration::from_secs(1);
    let ids = ["a", "b", "c"];
    let (bucket, net, nodes, c, leaders) = delegated_cluster(&ids, ttl).await?;
    let r = async {
        let n = *leaders
            .values()
            .find(|i| **i != c)
            .expect("a delegated node holds a range");
        let old = nodes[n].node.session.current().expect("N alive");
        let gate = Gate::closed();
        let frozen = nodes[n].store.add_rule(Rule::new(
            OpSet::ALL,
            PathMatch::Any,
            Action::Gate(gate.clone()),
        ));
        let cut = net.add(isolate(ids[n]));
        let path =
            slatedb::object_store::path::Path::from(format!("sessions/{}/{}", old.node, old.inc));
        // Ended on the bucket: the coordinator's `expired` (an unconditional `closed` write
        // by `lose` on N, waiting behind the gate, may overwrite it — still ended).
        let ended = |want_expired: bool| {
            let (bucket, path) = (bucket.clone(), path.clone());
            async move {
                use slatedb::object_store::ObjectStoreExt;
                match bucket.get(&path).await {
                    Ok(g) => g.bytes().await.is_ok_and(|b| {
                        String::from_utf8_lossy(&b)
                            .lines()
                            .any(|l| l == "state=expired" || (!want_expired && l == "state=closed"))
                    }),
                    // Cleared after `GC_AFTER_TTLS` TTL — only once ended.
                    Err(_) => !want_expired,
                }
            }
        };
        let t0 = Instant::now();
        while !ended(true).await {
            ensure!(
                t0.elapsed() < ttl * 4,
                "the coordinator did not expire the frozen node after 4·TTL"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let refs: Vec<&ClusterNode> = nodes.iter().collect();
        wait_probe(&refs, ttl * 3, "N's ranges move to another node", |p| {
            leaders
                .iter()
                .filter(|(_, i)| **i == n)
                .all(|(r, _)| p.get(r).is_some_and(|v| v.len() == 1 && v[0] != n))
        })
        .await?;
        nodes[n].store.remove_rule(frozen);
        gate.open();
        net.remove(cut);
        let t1 = Instant::now();
        while t1.elapsed() < ttl * 3 {
            let p = common::leaders_by_probe(&refs).await;
            for (r, v) in &p {
                ensure!(v.len() <= 1, "range {r}: two leaders after N woke: {v:?}");
            }
            ensure!(ended(false).await, "N's expired record came back to life");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        ensure!(
            nodes[n].node.session.current().is_some_and(|id| id != old),
            "N did not open a new incarnation after waking"
        );
        Ok(())
    }
    .await;
    net.heal();
    for x in nodes {
        x.node.shutdown().await;
    }
    r
}
