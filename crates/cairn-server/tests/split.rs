// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Operator-triggered split and merge (the split protocol of D24).
//!
//! Three questions:
//!
//! 1. **Death at any step of the protocol** ⇒ another node comes up, the table
//!    is consistent (the parent without an intent, *or* two children), exactly
//!    one side is served, no acked invocation is lost.
//! 2. **Split under load**, including calls crossing exactly the boundary being
//!    split ⇒ the client only sees latency (a retryable `503`), no call is lost.
//! 3. **Merge back after a split** ⇒ one range covers the whole key space, the
//!    data is intact.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Context, Result},
    cairn_store::{
        partition_key_for_target, read_owner,
        testkit::{Action, Gate, OpSet, PathMatch, Rule},
        ObjectStores, PartitionId, StoreLocation,
    },
    common::{
        cluster_nodes,
        fake_sdk::{FakeSdk, Step},
        freeze, single_leaders, wait_probe, ClusterNode,
    },
    std::{
        collections::BTreeSet,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    },
};

/// The default split point of a range covering all of `u64`: the middle.
const MID: u64 = 1 << 63;

/// An object key of `service` whose partition key falls into the left half
/// (`left`) or the right half of the key space.
fn key_in_half(service: &str, left: bool, prefix: &str, n: usize) -> String {
    (0..100_000)
        .map(|i| format!("{prefix}-{n}-{i}"))
        .find(|k| {
            let pk =
                partition_key_for_target(&cairn_store::TenantId::default(), None, service, k, None)
                    .expect("has a key")
                    .0;
            (pk < MID) == left
        })
        .expect("found a key")
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

/// `/invoke`, retrying on `503` (range changing — just latency). Returns the last
/// code and the number of retries needed.
async fn invoke_retrying(
    url: &str,
    service: &str,
    handler: &str,
    key: &str,
) -> Result<(u16, serde_json::Value, usize)> {
    let mut retries = 0;
    loop {
        let (code, body) = common::invoke(url, service, handler, Some(key), "x").await?;
        // `429 range_overloaded` is a temporary answer of the contract ("nothing
        // created, resend after Retry-After", `ErrorCode` in
        // `docs/api/openapi.json`): right after a merge, the merged range carries
        // the L0 of both children, so the L0 throttle may kick in.
        let overloaded = code == 429 && body["code"] == "range_overloaded";
        if !(code == 503 || overloaded) || retries >= 100 {
            return Ok((code, body, retries));
        }
        retries += 1;
        let wait_ms = if overloaded {
            body["retry_after_ms"]
                .as_u64()
                .unwrap_or(100)
                .clamp(20, 1_000)
        } else {
            20
        };
        tokio::time::sleep(Duration::from_millis(wait_ms)).await;
    }
}

/// Every id in `ids` must be `completed` (polling until `deadline`), with the right result.
async fn all_completed(url: &str, ids: &[(String, &str)], deadline: Duration) -> Result<()> {
    let t0 = Instant::now();
    for (id, want) in ids {
        loop {
            let (code, st) = common::invocation(url, id).await?;
            if code == 200 && st["status"] == "completed" {
                ensure!(st["output"] == *want, "{id}: {st}");
                break;
            }
            ensure!(
                t0.elapsed() < deadline,
                "{id} not done after {deadline:?}: {code} {st}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────── split under load

/// **Split under load** (one process, one range): continuous load on both
/// halves, plus `ctx.call` calls from the left half to the right half — crossing
/// exactly the boundary being split. `POST /ranges/{id}/split` midway ⇒ two
/// ranges; every accepted call completes with the right result; the client
/// sees nothing but a retryable `503`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_under_load_in_process() -> Result<()> {
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let callee_key = key_in_half("Callee", false, "callee", 0);
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("ok")])
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

    let result = common::within(Duration::from_secs(60), "split under load", async {
        common::register(&url, &sdk.url).await?;
        let accepted: Arc<Mutex<Vec<(String, &'static str)>>> = Arc::default();
        let bad: Arc<Mutex<Vec<String>>> = Arc::default();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let mut load = Vec::new();
        for worker in 0..4usize {
            let (url, accepted, bad, stop) =
                (url.clone(), accepted.clone(), bad.clone(), stop.clone());
            load.push(tokio::spawn(async move {
                let mut n = 0usize;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    n += 1;
                    let (service, handler, key, want) = if n.is_multiple_of(3) {
                        // The left half calls into the right half.
                        (
                            "Caller",
                            "run",
                            key_in_half("Caller", true, &format!("w{worker}"), n),
                            "pong",
                        )
                    } else {
                        (
                            "Counter",
                            "add",
                            key_in_half("Counter", n.is_multiple_of(2), &format!("w{worker}"), n),
                            "ok",
                        )
                    };
                    match invoke_retrying(&url, service, handler, &key).await {
                        Ok((200 | 202, body, _)) => accepted
                            .lock()
                            .unwrap()
                            .push((body["id"].as_str().unwrap_or_default().to_string(), want)),
                        Ok((code, body, retries)) => bad.lock().unwrap().push(format!(
                            "{service}/{key}: {code} after {retries} attempts: {body}"
                        )),
                        Err(e) => bad.lock().unwrap().push(format!("{service}/{key}: {e}")),
                    }
                }
            }));
        }

        tokio::time::sleep(Duration::from_millis(400)).await;
        let (code, view) = post_json(format!("{url}/ranges/0/split")).await?;
        ensure!(code == 200, "split: {code} {view}");
        tokio::time::sleep(Duration::from_millis(600)).await;
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for t in load {
            t.await?;
        }

        let bad = bad.lock().unwrap().clone();
        ensure!(bad.is_empty(), "calls failed around the split: {bad:#?}");
        let accepted = accepted.lock().unwrap().clone();
        ensure!(accepted.len() >= 20, "too little load: {}", accepted.len());
        all_completed(&url, &accepted, Duration::from_secs(20)).await?;

        let table = node.cluster.table();
        ensure!(table.partition_count() == 2, "{table:?}");
        ensure!(!table.contains(PartitionId(0)), "parent still in table");
        for p in table.partitions() {
            ensure!(node.partitions.contains(p), "node lacks range {}", p.0);
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    result
}

// ─────────────────────────────────────────────── merge after a split

/// **Merge back after a split**: one range ⇒ split ⇒ merge the two children ⇒
/// one range covering all of `u64`, `gen` = max of the two children + 1, a new
/// id. Every invocation written in any of the three periods reads back intact,
/// and new calls run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_after_split_restores_single_range() -> Result<()> {
    let cfg = common::solo_config(1);
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("ok")])
        .spawn()
        .await?;

    let result = common::within(Duration::from_secs(60), "split then merge", async {
        common::register(&url, &sdk.url).await?;
        let mut ids = Vec::new();
        let round = |tag: &'static str| {
            let url = url.clone();
            async move {
                let mut out = Vec::new();
                for (i, left) in [true, false, true, false].into_iter().enumerate() {
                    let key = key_in_half("Counter", left, tag, i);
                    let (code, body, _) = invoke_retrying(&url, "Counter", "add", &key).await?;
                    ensure!(code == 200, "{tag}/{key}: {code} {body}");
                    out.push((body["id"].as_str().unwrap_or_default().to_string(), "ok"));
                }
                Ok::<_, anyhow::Error>(out)
            }
        };
        ids.extend(round("before").await?);

        let (code, view) = post_json(format!("{url}/ranges/0/split")).await?;
        ensure!(code == 200, "split: {code} {view}");
        let ranges: Vec<u32> = view["ranges"]
            .as_array()
            .ok_or_else(|| anyhow!("no ranges: {view}"))?
            .iter()
            .map(|r| r["range"].as_u64().unwrap_or(0) as u32)
            .collect();
        ensure!(ranges.len() == 2, "{view}");
        ids.extend(round("during").await?);

        let (code, view) = post_json(format!(
            "{url}/ranges/merge?left={}&right={}",
            ranges[0], ranges[1]
        ))
        .await?;
        ensure!(code == 200, "merge: {code} {view}");
        let merged = view["ranges"].as_array().ok_or_else(|| anyhow!("{view}"))?;
        ensure!(merged.len() == 1, "{view}");
        ensure!(merged[0]["start"] == "0000000000000000", "{view}");
        ensure!(merged[0]["end"] == "ffffffffffffffff", "{view}");
        ensure!(
            merged[0]["gen"] == 2,
            "gen of the merged range = max(1, 1) + 1: {view}"
        );
        ensure!(
            !ranges.contains(&(merged[0]["range"].as_u64().unwrap_or(0) as u32)),
            "{view}"
        );
        ids.extend(round("after").await?);

        all_completed(&url, &ids, Duration::from_secs(20)).await?;
        let table = node.cluster.table();
        ensure!(table.partition_count() == 1, "{table:?}");

        // D-d: the three retired Dbs (parent, two children) are still
        // referenced by the merged range — the cleanup loop must delete none.
        let deleted = cairn_server::node::gc_retired_dbs_once(&stores, &node.cluster)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        ensure!(
            deleted == 0,
            "D-d deleted {deleted} Dbs still referenced by the merged range"
        );
        for r in [0, ranges[0], ranges[1]] {
            ensure!(
                stores.range_db_exists(cairn_store::PartitionId(r)).await?,
                "Db of range {r} vanished"
            );
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    result
}

// ─────────────────────────────────────────────── death at every step

fn stores_of(n: &ClusterNode) -> ObjectStores {
    ObjectStores::single(StoreLocation::from_store(n.store.clone(), "", "memory://"))
}

/// The protocol step whose object write is blocked — the coordinating node dies there.
#[derive(Clone, Copy, Debug)]
enum Stop {
    /// Table CAS writing the intent (step 1).
    IntentCas,
    /// Memtable flush / closing the parent Db (steps 2–3).
    Flush,
    /// Clone of the left child Db (step 5).
    CloneLeft,
    /// Owner record of the left child (step 6).
    OwnerLeft,
    /// Commit table CAS (step 7).
    Commit,
    /// The parent's tombstone, after the commit (step 9).
    Tombstone,
}

/// **Death at every step of the split protocol** (D24). A two-node cluster,
/// one range. Block each step's object write and kill the coordinating node
/// there ⇒ the other node comes up within a limit: the table is consistent —
/// the parent without an intent, *or* two children —, every range in the table
/// has a leader on a live node, the children of a rolled-back split have no
/// owner record left, and every acked invocation is readable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_crash_at_every_step() -> Result<()> {
    for stop in [
        Stop::IntentCas,
        Stop::Flush,
        Stop::CloneLeft,
        Stop::OwnerLeft,
        Stop::Commit,
        Stop::Tombstone,
    ] {
        split_crash_at(stop, false)
            .await
            .with_context(|| format!("died at {stop:?}"))?;
    }
    Ok(())
}

/// **Moving forward from `Prepared`**. The coordinator has cloned both
/// children, taken ownership of them, written the `Prepared` record, then died
/// right before the commit CAS.
///
/// Interleaving:
/// 1. A splits 0: intent, quiesce, `Closed` record, closed mark, clones 1 and 2,
///    takes ownership, `Prepared` record.
/// 2. A's second `cluster.conf` write (the commit CAS) is blocked; A dies (its
///    session stops renewing, every request to the store hangs).
/// 3. B takes range 0 after the TTL and sees an intent that no one on B
///    coordinates.
///
/// Must see: B **moves forward** — `Prepared` record, both children have a Db
/// and an owner record, the source is still closed at exactly the recorded epoch
/// ⇒ commit CAS; the table is the two children, the record is `Committed`, both
/// children have a leader on B (taken once A's session expires) and every acked
/// call is readable. Before the fix: B rolled back — the table went back to the
/// parent, the cloned work was thrown away.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_crash_before_commit_cas_rolls_forward() -> Result<()> {
    split_crash_at(Stop::Commit, true).await
}

async fn split_crash_at(stop: Stop, forward: bool) -> Result<()> {
    const TTL: Duration = Duration::from_secs(1);
    let (_bucket, nodes) = cluster_nodes(&["node-alpha", "node-beta"], 1, TTL).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let probe = wait_probe(&refs, Duration::from_secs(20), "range 0 is led", |p| {
        single_leaders(p).is_some()
    })
    .await?;
    let lead = single_leaders(&probe).expect("leader present")[&0];
    let (a, b) = (&nodes[lead], &nodes[1 - lead]);
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("ok")])
        .spawn()
        .await?;
    for n in &nodes {
        common::register(&n.node.url(), &sdk.url).await?;
    }
    let mut acked = Vec::new();
    for (i, left) in [true, false, true, false, true, false]
        .into_iter()
        .enumerate()
    {
        let key = key_in_half("Counter", left, &format!("{stop:?}"), i);
        let (code, body) = common::invoke(&a.node.url(), "Counter", "add", Some(&key), "x").await?;
        ensure!(code == 200, "{key}: {code} {body}");
        acked.push((body["id"].as_str().unwrap_or_default().to_string(), "ok"));
    }

    // The child ranges will be 1 and 2 (a new table's `next_range_id` is 1).
    let gate = Gate::closed();
    let put = |path: &str, action: Action| {
        Rule::new(OpSet::PUT, PathMatch::Contains(path.into()), action)
    };
    // Tombstone: the parent's owner record is still written **before** the
    // commit — with the shared WAL, the seal writes the clean record right at
    // quiesce. So block the tombstone with two gates: hold the commit CAS,
    // install the tombstone gate, and only then release the commit — after the
    // commit the only write to `owners/range-0` is the tombstone.
    let before_commit = Gate::closed();
    a.store.add_rule(match stop {
        Stop::IntentCas => put("cluster.conf", Action::Gate(gate.clone())),
        Stop::Flush => put("ranges/0/manifest", Action::Gate(gate.clone())),
        Stop::CloneLeft => put("ranges/1/", Action::Gate(gate.clone())),
        Stop::OwnerLeft => put("owners/range-1", Action::Gate(gate.clone())),
        Stop::Commit => put(
            "cluster.conf",
            Action::GateAfter {
                pass: 1,
                gate: gate.clone(),
            },
        ),
        Stop::Tombstone => put(
            "cluster.conf",
            Action::GateAfter {
                pass: 1,
                gate: before_commit.clone(),
            },
        ),
    });
    let split_url = format!("{}/ranges/0/split", a.node.url());
    let splitting = tokio::spawn(async move { post_json(split_url).await });
    let t0 = Instant::now();
    if matches!(stop, Stop::Tombstone) {
        while before_commit.blocked() == 0 {
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "the protocol did not reach the commit"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        a.store
            .add_rule(put("owners/range-0", Action::Gate(gate.clone())));
        before_commit.open();
    }
    while gate.blocked() == 0 {
        if splitting.is_finished() {
            let done = splitting.await?;
            return Err(anyhow!("split done before step {stop:?}: {done:?}"));
        }
        ensure!(
            t0.elapsed() < Duration::from_secs(10),
            "the protocol did not reach step {stop:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Kill the coordinating node right there: stop renewing the session, every
    // request to the store hangs.
    a.node.session.crash_handle().crash();
    freeze(&a.store);

    // The other node: the table is consistent, every range in the table has a leader on it.
    let stores = stores_of(b);
    let t0 = Instant::now();
    let bound = Duration::from_secs(25);
    let table = loop {
        let _ = b.node.cluster.reload().await;
        let t = b.node.cluster.table();
        let consistent = t.ranges().iter().all(|r| r.intent.is_none());
        let mut led = consistent;
        if consistent {
            for p in t.partitions() {
                let up = match b.node.partitions.get(p) {
                    Some(h) => {
                        let r = t.range(p).expect("in the table");
                        let probe = cairn_store::make_invocation_id(
                            cairn_store::PartitionKey(r.start),
                            [3; 16],
                        );
                        matches!(
                            tokio::time::timeout(Duration::from_millis(300), h.status(probe)).await,
                            Ok(Ok(_))
                        )
                    }
                    None => false,
                };
                led &= up;
            }
        }
        if led {
            break t;
        }
        ensure!(
            t0.elapsed() < bound,
            "after {bound:?} the live node is not consistent yet: {t:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    let ids: BTreeSet<u32> = table.partitions().map(|p| p.0).collect();
    ensure!(
        ids == BTreeSet::from([0]) || ids == BTreeSet::from([1, 2]),
        "the table is neither the parent *nor* two children: {table:?}"
    );
    if ids.contains(&0) {
        // Rolled back: no trace of the two children left.
        for child in [1u32, 2] {
            let rec = read_owner(
                stores.main().store(),
                stores.main().prefix(),
                PartitionId(child),
            )
            .await?;
            ensure!(
                rec.is_none(),
                "child {child} of a rolled-back split still has an owner record: {rec:?}"
            );
        }
    }
    if matches!(stop, Stop::Tombstone) {
        ensure!(
            ids == BTreeSet::from([1, 2]),
            "committed, so it must be two children: {table:?}"
        );
    }
    if forward {
        ensure!(
            ids == BTreeSet::from([1, 2]),
            "the op was Prepared but the live node rolled back instead of moving forward: {table:?}"
        );
        let op = cairn_store::ledger::read_op(stores.main().store(), stores.main().prefix(), 1)
            .await?
            .ok_or_else(|| anyhow!("no ops/1 record"))?;
        ensure!(
            op.record.phase == cairn_split::Phase::Committed,
            "record after moving forward: {:?}",
            op.record
        );
    }
    all_completed(&b.node.url(), &acked, Duration::from_secs(15)).await?;

    gate.open();
    let mut nodes = nodes;
    let survivor = nodes.remove(1 - lead);
    survivor.node.shutdown().await;
    Ok(())
}

// ─────────────────────────────────────────────── D-c: turn limit per node

/// **`--max-concurrent-turns` is a per-node limit** (D-c): split evenly between
/// the engines the node leads. Two ranges, limit 2 ⇒ at most 2 turns inside the
/// SDK at once — not 2 per range (4), which would make the total limit grow
/// after every split.
///
/// Interleaving (each step waits for a bounded event — AGENTS.md §6.3):
/// 1. One node, two ranges, limit 2. The SDK holds every turn at a gate.
/// 2. Two `/send`s per range (four different objects — no VO lock blocks).
/// 3. Wait: two turns reach the SDK, and the schedulers of the two ranges
///    together have exactly two turns holding a slot and two slot waiters.
/// 4. Release one turn: the third turn reaches the SDK — a waiter got the slot
///    just returned.
///
/// Must see: at (3) exactly 2 turns in the SDK (lower bound: the slots are
/// used; upper bound: no more than the node limit) and two waiters; at (4)
/// exactly 3; then all four complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_concurrent_turns_is_per_node() -> Result<()> {
    let mut cfg = common::solo_config(2);
    cfg.limits.max_concurrent_turns = 2;
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .gated_object("Slow", "run", vec![Step::output("done")])
        .spawn()
        .await?;
    let gate = sdk.gate();

    let result = common::within(Duration::from_secs(30), "turn limit per node", async {
        common::register(&url, &sdk.url).await?;
        let mut ids = Vec::new();
        for p in 0..2u32 {
            for i in 0..2 {
                let key = common::key_on_partition(&node, "Slow", p, &format!("p{p}-{i}"));
                let (code, body) = common::send(&url, "Slow", "run", Some(&key), "x").await?;
                ensure!(code == 200 || code == 202, "{code} {body}");
                ids.push((body["id"].as_str().unwrap_or_default().to_string(), "done"));
            }
        }
        // (3) Sum over both ranges: (turns holding a slot, slot waiters).
        let turns = || async {
            let (mut in_use, mut waiting) = (0, 0);
            for (_, h) in node.partitions.iter() {
                let f = h.flow().await.map_err(|e| anyhow!("{e}"))?;
                in_use += f.scheduler.turns_in_use;
                waiting += f.scheduler.waiting;
            }
            anyhow::Ok((in_use, waiting))
        };
        loop {
            let (in_use, waiting) = turns().await?;
            let inside = sdk.starts_of("Slow", "run").len();
            ensure!(
                inside <= 2,
                "{inside} turns inside the SDK at once with a limit of 2 per node"
            );
            if inside == 2 && in_use == 2 && waiting == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // (4) One turn completes ⇒ exactly one waiter gets the slot.
        gate.release(1);
        while sdk.starts_of("Slow", "run").len() < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let (in_use, _) = turns().await?;
        ensure!(in_use <= 2, "{in_use} turns in slots, limit 2 per node");
        ensure!(
            sdk.starts_of("Slow", "run").len() == 3,
            "one turn released but more than one waiter entered the SDK"
        );
        gate.open();
        all_completed(&url, &ids, Duration::from_secs(15)).await?;
        Ok(())
    })
    .await;
    gate.open();
    node.shutdown().await;
    result
}

// ─────────────────────────────────────────────── D-e: merging two ranges on two nodes

/// **Merging two ranges on two nodes** (D-e): the coordinating node leads one
/// range; the owner of the other range hands it off, the coordinating node
/// gathers it in, quiesces both, unions, commits. Afterwards one range covering
/// all of `u64` has exactly one leader, and invocations written in both earlier
/// ranges read back intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_gathers_range_from_other_node() -> Result<()> {
    const TTL: Duration = Duration::from_secs(1);
    let (_bucket, nodes) = cluster_nodes(&["node-alpha", "node-beta"], 2, TTL).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let probe = wait_probe(
        &refs,
        Duration::from_secs(20),
        "one range on each node",
        |p| single_leaders(p).is_some_and(|m| common::load_of(&m, 2) == [1, 1]),
    )
    .await?;
    let leaders = single_leaders(&probe).expect("balanced");
    let mine = *leaders
        .iter()
        .find(|(_, n)| **n == 0)
        .expect("alpha holds a range")
        .0;
    let theirs = *leaders
        .iter()
        .find(|(_, n)| **n == 1)
        .expect("beta holds a range")
        .0;
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("ok")])
        .spawn()
        .await?;
    for n in &nodes {
        common::register(&n.node.url(), &sdk.url).await?;
    }
    let result = async {
        let mut acked = Vec::new();
        for (p, n) in [(mine, 0usize), (theirs, 1)] {
            let key = common::key_on_partition(&nodes[n].node, "Counter", p, "before-merge");
            let (code, body) =
                common::invoke(&nodes[n].node.url(), "Counter", "add", Some(&key), "x").await?;
            ensure!(code == 200, "{code} {body}");
            acked.push((body["id"].as_str().unwrap_or_default().to_string(), "ok"));
        }

        let (code, view) = post_json(format!(
            "{}/ranges/merge?left={mine}&right={theirs}",
            nodes[0].node.url()
        ))
        .await?;
        ensure!(code == 200, "merge: {code} {view}");
        let merged = view["ranges"].as_array().ok_or_else(|| anyhow!("{view}"))?;
        ensure!(merged.len() == 1, "{view}");
        let into = merged[0]["range"].as_u64().unwrap_or(0) as u32;

        let probe = wait_probe(
            &refs,
            Duration::from_secs(20),
            "merged range has one leader",
            |p| p.get(&into).is_some_and(|v| v.len() == 1),
        )
        .await?;
        let leader = &nodes[probe[&into][0]].node;
        all_completed(&leader.url(), &acked, Duration::from_secs(15)).await?;
        Ok(())
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    result
}

// ─────────────────────────────────────────────── per-phase measurements

/// **Split reports per-phase measurements**: the response of
/// `POST /ranges/{id}/split` carries a `report` — the duration of each protocol
/// step, the write pause window (from quiesce until both children have a
/// leader), the parent's drain/seal part, and for each child: Db open, rebuild
/// of the derived region (rows per region, bytes), queue recovery. The rebuild
/// only scans the hot region: **0 bulk-region rows**, even though every
/// invocation has a journal and state there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_reports_phase_timings() -> Result<()> {
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .object(
            "Counter",
            "add",
            vec![
                Step::SetState {
                    key: "a".into(),
                    value: bytes::Bytes::from(vec![7u8; 256]),
                },
                Step::Sleep(3_600_000),
            ],
        )
        .spawn()
        .await?;
    let result = common::within(Duration::from_secs(60), "split with measurements", async {
        common::register(&url, &sdk.url).await?;
        // A few live invocations in each half: hot rows (status, timer) and bulk
        // rows (journal, state) in both children.
        let mut ids = Vec::new();
        for n in 0..8 {
            let key = key_in_half("Counter", n % 2 == 0, "live", n);
            let (code, body) = common::send(&url, "Counter", "add", Some(&key), "x").await?;
            ensure!(code == 200 || code == 202, "{code} {body}");
            ids.push(body["id"].as_str().unwrap_or_default().to_string());
        }
        for id in &ids {
            common::wait_status(&url, id, "suspended").await?;
        }

        let (code, view) = post_json(format!("{url}/ranges/0/split")).await?;
        ensure!(code == 200, "split: {code} {view}");
        let report = &view["report"];
        let phases: Vec<&str> = report["phases"]
            .as_array()
            .ok_or_else(|| anyhow!("missing report.phases: {view}"))?
            .iter()
            .map(|p| p["phase"].as_str().unwrap_or_default())
            .collect();
        ensure!(
            phases
                == [
                    "precondition",
                    "intentCas",
                    "flush",
                    "quiesce",
                    "cloneLeft",
                    "cloneRight",
                    "ownerClaim",
                    "commitCas",
                    "open",
                    "retire"
                ],
            "{phases:?}"
        );
        for p in report["phases"].as_array().into_iter().flatten() {
            ensure!(p["ms"].as_f64().is_some(), "phase without ms: {p}");
        }
        ensure!(report["windowMs"].as_f64().is_some(), "{report}");
        let parents = report["parents"]
            .as_array()
            .ok_or_else(|| anyhow!("{report}"))?;
        ensure!(parents.len() == 1, "{report}");
        let parent = &parents[0];
        ensure!(parent["range"] == 0, "{report}");
        ensure!(
            parent["drainMs"].as_f64().is_some() && parent["sealMs"].as_f64().is_some(),
            "{report}"
        );
        let children = report["children"]
            .as_array()
            .ok_or_else(|| anyhow!("{report}"))?;
        ensure!(children.len() == 2, "{report}");
        for c in children {
            ensure!(c["openDbMs"].as_f64().is_some(), "{c}");
            ensure!(c["restoreMs"].as_f64().is_some(), "{c}");
            let r = &c["rebuild"];
            ensure!(r["ms"].as_f64().is_some(), "{c}");
            ensure!(
                r["hotRows"].as_u64().unwrap_or(0) > 0,
                "child scanned no hot row: {c}"
            );
            ensure!(r["bulkRows"] == 0, "rebuild scanned the bulk region: {c}");
            ensure!(r["bytes"].as_u64().unwrap_or(0) > 0, "{c}");
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    result
}

// ─────────────────────────────────────── admin paths while a range is splitting

/// A one-range cluster node with a leader, and a (closed) gate for [`hold_split`].
async fn split_held() -> Result<(Vec<ClusterNode>, Gate)> {
    const TTL: Duration = Duration::from_secs(1);
    let (_bucket, nodes) = cluster_nodes(&["node-solo"], 1, TTL).await?;
    let refs: Vec<_> = nodes.iter().collect();
    wait_probe(&refs, Duration::from_secs(20), "range 0 is led", |p| {
        single_leaders(p).is_some()
    })
    .await?;
    Ok((nodes, Gate::closed()))
}

/// Start splitting range 0 and **hold** it after quiesce (a gate on the left
/// child's owner record): the parent is closed, the two children do not exist
/// yet. Returns the split task.
async fn hold_split(
    n: &ClusterNode,
    gate: &Gate,
) -> Result<tokio::task::JoinHandle<Result<(u16, serde_json::Value)>>> {
    n.store.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("owners/range-1".into()),
        Action::Gate(gate.clone()),
    ));
    let url = format!("{}/ranges/0/split", n.node.url());
    let splitting = tokio::spawn(async move { post_json(url).await });
    let t0 = Instant::now();
    while gate.blocked() == 0 {
        ensure!(
            t0.elapsed() < Duration::from_secs(10),
            "split did not reach the hold step"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(splitting)
}

/// **`/admin/tick` during a split must not lose its tick.** It used to return
/// `200 {"advanced":0}` when the parent range had quiesced and the children were
/// not open yet — the caller believed the tick happened, and a due timer never
/// fired. Now: wait for the table to settle, then tick (release the gate after
/// 500 ms ⇒ `200` and the timer fires), or `503` so the caller retries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_tick_during_split_is_not_lost() -> Result<()> {
    let (nodes, gate) = split_held().await?;
    let n = &nodes[0];
    let url = n.node.url();
    let sdk = FakeSdk::builder()
        .object(
            "Sleeper",
            "nap",
            vec![Step::Sleep(3_600_000), Step::output("woke")],
        )
        .spawn()
        .await?;
    let result = async {
        common::register(&url, &sdk.url).await?;
        let key = key_in_half("Sleeper", true, "sleep", 0);
        let (code, body) = common::send(&url, "Sleeper", "nap", Some(&key), "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &id, "suspended").await?;

        let splitting = hold_split(n, &gate).await?;
        let later = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64
            + 7_200_000;
        let tick_url = format!("{url}/admin/tick?now_ms={later}");
        let ticking = tokio::spawn(async move { post_json(tick_url).await });
        tokio::time::sleep(Duration::from_millis(500)).await;
        gate.open();
        let (code, body) = ticking.await??;
        ensure!(
            code == 503 || (code == 200 && body["advanced"].as_u64().unwrap_or(0) >= 1),
            "a tick during the split was swallowed: {code} {body}"
        );
        let (code, view) = splitting.await??;
        ensure!(code == 200, "split: {code} {view}");
        if code != 200 || body["advanced"].as_u64().unwrap_or(0) == 0 {
            let (code, body) = post_json(format!("{url}/admin/tick?now_ms={later}")).await?;
            ensure!(code == 200, "{code} {body}");
        }
        all_completed(&url, &[(id, "woke")], Duration::from_secs(10)).await
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    result
}

/// **Admin paths by id survive a split** (WrongRange/ask_range): an external
/// signal and a cooperative cancel sent to an invocation of the parent range
/// during the split must not get a final error ("id points at a foreign
/// partition", "engine stopped") — they wait for the table to change, then reach
/// the right child.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn id_routed_admin_calls_survive_split() -> Result<()> {
    let (nodes, gate) = split_held().await?;
    let n = &nodes[0];
    let url = n.node.url();
    let sdk = FakeSdk::builder()
        .object(
            "Waiter",
            "wait",
            vec![Step::Suspend, Step::output("received")],
        )
        .spawn()
        .await?;
    let result = async {
        common::register(&url, &sdk.url).await?;
        let mut ids = Vec::new();
        for (i, left) in [true, false, true, false].into_iter().enumerate() {
            let key = key_in_half("Waiter", left, "wait", i);
            let (code, body) = common::send(&url, "Waiter", "wait", Some(&key), "x").await?;
            ensure!(code == 200 || code == 202, "{code} {body}");
            let id = body["id"].as_str().unwrap_or_default().to_string();
            common::wait_status(&url, &id, "suspended").await?;
            ids.push(id);
        }

        let splitting = hold_split(n, &gate).await?;
        let client = reqwest::Client::new();
        let mut calls = Vec::new();
        for (i, id) in ids.iter().enumerate() {
            let req = if i < 2 {
                client
                    .post(format!("{url}/invocations/{id}/signal"))
                    .json(&serde_json::json!({ "signal": common::fake_sdk::HOLD_SIGNAL }))
            } else {
                client.post(format!("{url}/invocations/{id}/cancel?mode=signal"))
            };
            calls.push(tokio::spawn(async move {
                let resp = req.send().await?;
                let code = resp.status().as_u16();
                Ok::<_, anyhow::Error>((code, resp.text().await?))
            }));
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        gate.open();
        for (i, c) in calls.into_iter().enumerate() {
            let (code, text) = c.await??;
            ensure!(code == 200, "call {i} during the split: {code} {text}");
        }
        let (code, view) = splitting.await??;
        ensure!(code == 200, "split: {code} {view}");
        all_completed(
            &url,
            &[(ids[0].clone(), "received"), (ids[1].clone(), "received")],
            Duration::from_secs(10),
        )
        .await
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    result
}

/// **A synchronous `/invoke` waits across a split.** A call waiting for the
/// result (`invoke_wait`) of an invocation suspended on a short `sleep`; its
/// range splits midway. The wait loop used to ask exactly the old engine, and on
/// an error (range quiesced) returned `202` at once — the synchronous client got
/// "not done" though the invocation completed a few hundred milliseconds later
/// on a child range. Now it asks again by the table: `200` with the result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_invoke_waits_through_split() -> Result<()> {
    let (nodes, gate) = split_held().await?;
    let n = &nodes[0];
    let url = n.node.url();
    let sdk = FakeSdk::builder()
        .object(
            "Napper",
            "nap",
            vec![Step::Sleep(400), Step::output("woke up")],
        )
        .spawn()
        .await?;
    let result = async {
        common::register(&url, &sdk.url).await?;
        let key = key_in_half("Napper", true, "blink", 0);
        let invoke_url = url.clone();
        let invoking = tokio::spawn(async move {
            common::invoke(&invoke_url, "Napper", "nap", Some(&key), "x").await
        });
        while sdk.starts_of("Napper", "nap").is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let splitting = hold_split(n, &gate).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        gate.open();
        let (code, body) = invoking.await??;
        ensure!(
            code == 200 && body["output"] == "woke up",
            "synchronous call across a split: {code} {body}"
        );
        let (code, view) = splitting.await??;
        ensure!(code == 200, "split: {code} {view}");
        Ok(())
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    result
}

// ──────────────────── bounded waits while holding an intent

/// Split range 0 of `node` (one range, already led) while a PUT matching `path`
/// stands at a gate that never opens (under real SlateDB: infinite retries;
/// under the S3 client: 180 s), then assert:
///
/// - the split **answers** within `within` — `503` (temporary, rolled back),
///   does not hang;
/// - the table has no intent left;
/// - once the store heals, range 0 serves again.
async fn split_answers_while_stuck_on(path: &str, within: Duration) -> Result<()> {
    let node = alone("node-solo", 1).await?;
    let result = common::within(
        within + Duration::from_secs(30),
        "split while the store hangs",
        async {
            let sdk = FakeSdk::builder()
                .object("Counter", "get", vec![Step::output("ok")])
                .spawn()
                .await?;
            common::register(&node.node.url(), &sdk.url).await?;
            let (code, body) =
                common::invoke(&node.node.url(), "Counter", "get", Some("k"), "x").await?;
            ensure!(code == 200 && body["output"] == "ok", "{code} {body}");
            let gate = Gate::closed();
            node.store.add_rule(Rule::new(
                OpSet::WRITES,
                PathMatch::Contains(path.into()),
                Action::Gate(gate.clone()),
            ));
            let t0 = Instant::now();
            let r = common::within(
                within,
                "the split answers",
                post_json(format!("{}/ranges/0/split", node.node.url())),
            )
            .await;
            let took = t0.elapsed();
            let blocked = gate.blocked();
            gate.open();
            let (code, body) = r?;
            ensure!(
                blocked > 0 || took > Duration::from_secs(1),
                "gate {path} blocks nothing — the test checks nothing"
            );
            ensure!(
                code == 503,
                "split while {path} hangs: {code} {body} after {took:?}"
            );
            no_intent_left(&node, "after the split gave up").await?;
            let (code, body, _) = invoke_retrying(&node.node.url(), "Counter", "get", "k2").await?;
            ensure!(
                code == 200 && body["output"] == "ok",
                "range 0 does not serve again: {code} {body}"
            );
            Ok(())
        },
    )
    .await;
    node.node.shutdown().await;
    result
}

/// **A hung memtable flush holds the engine**: the coordinator asks for a
/// memtable flush before quiesce; the `FlushMemtable` command holds the engine
/// across the flush. The L0 SST of the flush hangs.
///
/// Must see: after a bound, the command returns a temporary error and releases
/// the engine; the split rolls back and returns `503`. Before the fix, every
/// turn of the range stood behind the held engine, and the split never
/// answered. The bound here is the flush bound plus the seal bound of the
/// `Resign` on the roll-back path (the seal also flushes the memtable — same
/// gate).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_gives_up_when_memtable_flush_hangs() -> Result<()> {
    split_answers_while_stuck_on("ranges/0/compacted", Duration::from_secs(40)).await
}

/// **A clone hangs while the source is quiesced**: range 0 has closed its Db,
/// holds the lease, and answers `WrongRange`; the left child's clone hangs on its
/// manifest.
///
/// Must see: after a bound, the split rolls back (the source reopens) and
/// returns `503`. Before the fix, the source stayed quiesced and the intent sat
/// on the table for as long as the PUT hung.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_gives_up_when_child_clone_hangs() -> Result<()> {
    split_answers_while_stuck_on("ranges/1/", Duration::from_secs(45)).await
}

/// **Taking a child hangs**: the CAS of the left child's owner record hangs (the
/// same place [`hold_split`] holds, but never released).
///
/// Must see: after a bound, the split rolls back and returns `503`. Before the
/// fix: it hung.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_gives_up_when_child_claim_hangs() -> Result<()> {
    split_answers_while_stuck_on("owners/range-1", Duration::from_secs(25)).await
}

// ─────────────────────────────── ambiguous table CAS (RD3)

/// A cluster node alone on a new bucket, `n` ranges, already leading every range.
async fn alone(id: &str, n: u16) -> Result<ClusterNode> {
    alone_tweaked(id, n, |_| {}).await
}

/// Like [`alone`], **without** the background orphan-child cleanup loop — so the
/// test runs the loop by hand and asserts on its result: the background loop
/// runs as soon as it is nudged (a roll-back, a commit), so it would compete
/// with the test's own round.
async fn alone_without_orphan_round(id: &str, n: u16) -> Result<ClusterNode> {
    alone_tweaked(id, n, |cfg| cfg.retire_orphans_every = None).await
}

async fn alone_tweaked(
    id: &str,
    n: u16,
    tweak: impl FnOnce(&mut cairn_server::node::NodeConfig),
) -> Result<ClusterNode> {
    let bucket = common::bucket();
    let node =
        common::start_cluster_node_tweaked(&bucket, id, n, Duration::from_secs(1), tweak).await?;
    wait_probe(
        &[&node],
        Duration::from_secs(20),
        "one node leads every range",
        |p| p.len() == n as usize && p.values().all(|v| v == &[0]),
    )
    .await?;
    Ok(node)
}

/// The table on the bucket (reloaded into the node's RAM) has no intent left.
async fn no_intent_left(n: &ClusterNode, what: &str) -> Result<cairn_store::PartitionTable> {
    n.node.cluster.reload().await?;
    let t = n.node.cluster.table();
    ensure!(
        t.ranges().iter().all(|r| r.intent.is_none()),
        "{what}: intent stuck on the bucket — auto split blocked cluster-wide: {t:?}"
    );
    Ok((*t).clone())
}

/// Rule: the node's `skip + 1`-th write of `cluster.conf` has an ambiguous result.
fn ambiguous_table_cas(reply: cairn_store::testkit::Ambiguity, skip: u32) -> Rule {
    Rule::new(
        OpSet::PUT,
        PathMatch::Contains("cluster.conf".into()),
        Action::Ambiguous {
            reply,
            skip,
            times: Some(1),
        },
    )
}

/// **An ambiguous intent CAS does not leave an intent stuck** (RD3).
///
/// Interleaving, for a split and then a merge, with both shapes of ambiguity:
/// 1. The coordinator CASes the intent (the first `cluster.conf` write).
/// 2. The store applies the write, then returns `Precondition` (object_store's
///    own retry of a 5xx) or `Generic` (5xx/timeout after the write).
///
/// Must see: the split/merge either succeeds, or fails **without** leaving any
/// intent on the bucket. Before the fix: `Conflict` (the "winner's" table was
/// our own) or `Err`, and the intent stayed — nobody rolled it back until the
/// next leader change, and auto split stopped cluster-wide.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ambiguous_intent_cas_never_strands_the_intent() -> Result<()> {
    use cairn_store::testkit::Ambiguity;
    for reply in [Ambiguity::Retried, Ambiguity::Error] {
        for merge in [false, true] {
            let what = format!("{} with {reply:?}", if merge { "merge" } else { "split" });
            let n = alone(&format!("amb-intent-{merge}-{reply:?}"), 2).await?;
            let r = async {
                n.store.add_rule(ambiguous_table_cas(reply, 0));
                let done = if merge {
                    n.node
                        .ranges
                        .merge(PartitionId(0), PartitionId(1))
                        .await
                        .map(|_| ())
                } else {
                    n.node.ranges.split(PartitionId(0), None).await.map(|_| ())
                };
                n.store.clear_rules();
                let t = no_intent_left(&n, &what).await?;
                if done.is_ok() {
                    ensure!(
                        !t.contains(PartitionId(0)),
                        "{what}: succeeded but range 0 is still in the table: {t:?}"
                    );
                }
                Ok(())
            }
            .await;
            n.node.shutdown().await;
            r.with_context(|| what.clone())?;
        }
    }
    // An error whose resolving read-back also fails: unknown whether the intent
    // landed. The coordinator reads the table back and rolls the intent back if it is its own.
    let n = alone("amb-intent-unresolved", 2).await?;
    let r = async {
        n.store.add_rule(ambiguous_table_cas(Ambiguity::Error, 0));
        // The second GET of `cluster.conf` (the read-back after the PUT) fails; the
        // first GET is `update`'s read before the CAS. `Ambiguous` on a GET is `Fail`.
        n.store.add_rule(Rule::new(
            OpSet::GET,
            PathMatch::Contains("cluster.conf".into()),
            Action::Ambiguous {
                reply: Ambiguity::Error,
                skip: 1,
                times: Some(1),
            },
        ));
        let done = n.node.ranges.split(PartitionId(0), None).await;
        n.store.clear_rules();
        ensure!(
            done.is_err(),
            "if the read-back fails the split must return an error: {:?}",
            done.map(|_| ())
        );
        no_intent_left(&n, "split, read-back failed").await?;
        Ok(())
    }
    .await;
    n.node.shutdown().await;
    r
}

/// **An ambiguous roll-back CAS still cleans up every child** (RD3).
///
/// Interleaving:
/// 1. Split range 0: intent, quiesce, clone L and R (final checkpoints on 0),
///    take L — then taking R fails (the PUT of `owners/range-R` errors). The
///    coordinator rolls back.
/// 2. The CAS dropping the intent (the second `cluster.conf` write) has an
///    ambiguous result.
///
/// Must see: the table has no intent left; L and R have no Db or owner record
/// left; range 0 has no final checkpoint left (which would pin a live range's
/// SSTs forever). Before the fix: `Retried` ⇒ `Conflict`, the loop saw no intent
/// left and returned `Ok` without cleaning up; `Error` ⇒ `Err`, no cleanup.
/// Nobody rolled back again — a permanent leak.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ambiguous_roll_back_cas_still_cleans_the_children() -> Result<()> {
    use cairn_store::testkit::Ambiguity;
    for reply in [Ambiguity::Retried, Ambiguity::Error] {
        let n = alone(&format!("amb-undo-{reply:?}"), 1).await?;
        let r = async {
            let t = n.node.cluster.table();
            let (left, right) = (t.next_range_id(), t.next_range_id() + 1);
            n.store.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Contains(format!("owners/range-{right}")),
                Action::Fail { times: None },
            ));
            n.store.add_rule(ambiguous_table_cas(reply, 1));
            let done = n.node.ranges.split(PartitionId(0), None).await;
            n.store.clear_rules();
            ensure!(done.is_err(), "split must fail at take-R");
            let t = no_intent_left(&n, "after the roll-back").await?;
            ensure!(t.contains(PartitionId(0)), "{t:?}");
            let stores = stores_of(&n);
            for c in [left, right] {
                ensure!(
                    !stores.range_db_exists(PartitionId(c)).await?,
                    "Db of orphan child {c} still there after the roll-back"
                );
                let rec = read_owner(
                    stores.main().store(),
                    stores.main().prefix(),
                    PartitionId(c),
                )
                .await?;
                ensure!(rec.is_none(), "owner record of orphan {c} left: {rec:?}");
            }
            let pinned: Vec<_> = stores
                .range_checkpoints(PartitionId(0))
                .await?
                .into_iter()
                .filter(|c| !c.expires)
                .collect();
            ensure!(
                pinned.is_empty(),
                "range 0 still has final checkpoints of orphan children: {pinned:?}"
            );
            Ok(())
        }
        .await;
        n.node.shutdown().await;
        r.with_context(|| format!("{reply:?}"))?;
    }
    Ok(())
}

// ─────────────────────── roll-back keeps the evidence until the cleanup is done (RD4)

/// Number of (non-expiring) final checkpoints on the Db of range `r`.
async fn pinned_on(stores: &ObjectStores, r: u32) -> Result<usize> {
    Ok(stores
        .range_checkpoints(PartitionId(r))
        .await?
        .into_iter()
        .filter(|c| !c.expires)
        .count())
}

/// **A backup taken mid split kept the clone's pin of a child the copy does not carry.**
/// Backup copies only the ranges of table V; with an intent in V the children are promised,
/// their Dbs are not copied, yet the clone's final checkpoint on the parent was kept. The
/// restored cluster can only roll that split back (forward needs the child Db), and the
/// roll-back removes the parent's pin only through the child's manifest — there is none, so
/// the pin stays for good (verify (f) after the roll-back: no healing round).
///
/// 1. Split range 0; hold the PUT of L's owner record (clone of L done, intent in V).
/// 2. `backup`, then `restore` into an empty store.
///
/// Must see: the restored range 0 carries no final checkpoint named by a child.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_mid_split_drops_the_pins_of_promised_children() -> Result<()> {
    let n = alone_without_orphan_round("mid-split-backup", 1).await?;
    let r = async {
        let left = n.node.cluster.table().next_range_id();
        let gate = Gate::closed();
        n.store.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains(format!("owners/range-{left}")),
            Action::Gate(gate.clone()),
        ));
        let ops = n.node.ranges.clone();
        let split = tokio::spawn(async move { ops.split(PartitionId(0), None).await.map(|_| ()) });
        let t0 = Instant::now();
        while gate.blocked() == 0 {
            ensure!(!split.is_finished(), "split done before L's owner record");
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "split did not reach L's owner record"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let stores = stores_of(&n);
        let pinned = stores
            .range_checkpoints(PartitionId(0))
            .await?
            .into_iter()
            .filter(|c| !c.expires && c.created_by.is_some())
            .count();
        ensure!(pinned > 0, "precondition: the clone of L pins range 0");
        let target = Arc::new(slatedb::object_store::memory::InMemory::new());
        let at = StoreLocation::from_store(target.clone(), "daily", "memory://r2/daily");
        let backed = cairn_server::ctl::Ctl::new(stores.clone())
            .backup(&at, "test@backup", "cairn-ctl backup")
            .await
            .map_err(|e| anyhow!("{e}"));
        gate.open();
        n.store.clear_rules();
        let _ = split.await;
        let rep = backed?;
        let restored = Arc::new(slatedb::object_store::memory::InMemory::new());
        let copy = ObjectStores::single(StoreLocation::from_store(restored, "", "memory://"));
        cairn_server::ctl::Ctl::new(copy.clone())
            .restore(&at, Some(&rep.id), "t", "c")
            .await
            .map_err(|e| anyhow!("{e}"))?;
        let pins: Vec<_> = copy
            .range_checkpoints(PartitionId(0))
            .await?
            .into_iter()
            .filter(|c| !c.expires && c.created_by.is_some())
            .collect();
        ensure!(
            pins.is_empty(),
            "the restored range 0 keeps the clone pins of children it does not carry: {pins:?}"
        );
        Ok(())
    }
    .await;
    n.node.shutdown().await;
    r
}

/// **A backup taken during a split roll-back left an orphan checkpoint no loop heals.**
/// Under chaos (40-minute run, backup cycle 5) a backup copied range 86 while two children
/// of a rolled-back split still had their Dbs: the clone's final checkpoints on 86 (named
/// by the child Db's path) were copied, the children were not (they are not in the table).
/// On the live cluster the orphan cleanup (SM-14) removes child and checkpoint; the restored
/// copy has no child Db, so nothing ever removes the checkpoint — verify (f) reports it with
/// no healing round.
///
/// 1. Split range 0; the take-R step fails, the roll-back cannot clean L's Db (GETs of its
///    manifest fail): L's Db and its checkpoint on range 0 stay (no orphan round).
/// 2. `backup`, then `restore` into an empty store.
///
/// Must see: verify on the restored copy finds no `orphan_checkpoint`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_during_a_roll_back_leaves_no_orphan_checkpoint() -> Result<()> {
    let n = alone_without_orphan_round("undo-backup", 1).await?;
    let r = async {
        let t = n.node.cluster.table();
        let (left, right) = (t.next_range_id(), t.next_range_id() + 1);
        let gate = Gate::closed();
        for action in [Action::Gate(gate.clone()), Action::Fail { times: None }] {
            n.store.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Contains(format!("owners/range-{right}")),
                action,
            ));
        }
        let ops = n.node.ranges.clone();
        let split = tokio::spawn(async move { ops.split(PartitionId(0), None).await.map(|_| ()) });
        let t0 = Instant::now();
        while gate.blocked() == 0 {
            ensure!(!split.is_finished(), "split done before the take-R step");
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "split did not reach the take-R step"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        n.store.add_rule(Rule::new(
            OpSet::GET | OpSet::LIST | OpSet::HEAD,
            PathMatch::Contains(format!("ranges/{left}/manifest")),
            Action::Precondition,
        ));
        gate.open();
        let done = split.await?;
        n.store.clear_rules();
        ensure!(done.is_err(), "the split must fail at the take-R step");
        no_intent_left(&n, "after the roll-back").await?;
        let stores = stores_of(&n);
        ensure!(
            stores.range_db_exists(PartitionId(left)).await?,
            "precondition: L's Db is still there"
        );

        let target = Arc::new(slatedb::object_store::memory::InMemory::new());
        let at = StoreLocation::from_store(target.clone(), "daily", "memory://r2/daily");
        let rep = cairn_server::ctl::Ctl::new(stores.clone())
            .backup(&at, "test@backup", "cairn-ctl backup")
            .await
            .map_err(|e| anyhow!("{e}"))?;
        let restored = Arc::new(slatedb::object_store::memory::InMemory::new());
        let copy = ObjectStores::single(StoreLocation::from_store(restored, "", "memory://"));
        cairn_server::ctl::Ctl::new(copy.clone())
            .restore(&at, Some(&rep.id), "t", "c")
            .await
            .map_err(|e| anyhow!("{e}"))?;
        let found = cairn_server::ctl::Ctl::new(copy)
            .verify(cairn_server::ctl::VerifyOptions {
                open_dbs: false,
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow!("{e}"))?;
        let orphans: Vec<_> = found
            .findings
            .iter()
            .filter(|f| f.check == "orphan_checkpoint")
            .collect();
        ensure!(
            orphans.is_empty(),
            "the restored copy keeps the clone checkpoint of child {left}: {orphans:?}"
        );
        Ok(())
    }
    .await;
    n.node.shutdown().await;
    r
}

/// **A roll-back that fails midway is finished by a later cleanup round**
/// (RD4).
///
/// Interleaving:
/// 1. One node, one range. Split 0: clone L, R (final checkpoints on 0), take
///    L; taking R stops at a gate, then fails.
/// 2. While it is stopped: reading L's manifest fails (an error not retried).
/// 3. Release the gate: the coordinator rolls back — CAS dropping the intent,
///    cleaning R succeeds, cleaning L fails at the step reading L's manifest.
/// 4. The store heals. One `OrphanRetirer` round (the D-d runner).
///
/// Must see: after step 3, L's owner record and Db are **still there** (the
/// evidence); after step 4, L and R have no Db or owner record left, and range
/// 0 has no final checkpoint left. Before the fix: step 3 deleted L's owner
/// record although its Db was still there, and the round in step 4 only LISTed
/// `owners/` so it did not see L — L's Db and its checkpoint on live range 0
/// leaked forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn half_done_roll_back_is_finished_by_a_later_round() -> Result<()> {
    let n = alone_without_orphan_round("undo-half", 1).await?;
    let r = async {
        let t = n.node.cluster.table();
        let (left, right) = (t.next_range_id(), t.next_range_id() + 1);
        let gate = Gate::closed();
        for action in [Action::Gate(gate.clone()), Action::Fail { times: None }] {
            n.store.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Contains(format!("owners/range-{right}")),
                action,
            ));
        }
        let ops = n.node.ranges.clone();
        let split = tokio::spawn(async move { ops.split(PartitionId(0), None).await.map(|_| ()) });
        let t0 = Instant::now();
        while gate.blocked() == 0 {
            ensure!(!split.is_finished(), "split done before the take-R step");
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "split did not reach the take-R step"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        n.store.add_rule(Rule::new(
            OpSet::GET | OpSet::LIST | OpSet::HEAD,
            PathMatch::Contains(format!("ranges/{left}/manifest")),
            Action::Precondition,
        ));
        gate.open();
        let done = split.await?;
        n.store.clear_rules();
        ensure!(done.is_err(), "the split must fail at the take-R step");
        no_intent_left(&n, "after the roll-back").await?;

        let stores = stores_of(&n);
        let (store, prefix) = (stores.main().store(), stores.main().prefix());
        ensure!(
            stores.range_db_exists(PartitionId(left)).await?,
            "L's Db must still be there: cleaning it failed"
        );
        ensure!(
            read_owner(store, prefix, PartitionId(left))
                .await?
                .is_some(),
            "L's owner record deleted although cleaning L's Db did not finish"
        );

        let verify = || async {
            cairn_server::ctl::Ctl::new(stores.clone())
                .verify(cairn_server::ctl::VerifyOptions {
                    open_dbs: false,
                    ..Default::default()
                })
                .await
                .map_err(|e| anyhow!("{e}"))
        };
        // `verify` sees L's checkpoint on live range 0 (name = path of L's Db).
        let rep = verify().await?;
        ensure!(
            rep.findings.iter().any(|f| f.check == "orphan_checkpoint"
                && f.range == Some(0)
                && f.detail.contains(&format!("orphan child {left} "))),
            "verify does not see L's orphan checkpoint on range 0: {:?}",
            rep.findings
        );

        let session = n.node.session.clone();
        let round = cairn_server::gc::OrphanRetirer::default()
            .run_once(&stores, &n.node.cluster, &session)
            .await?;
        ensure!(
            round.discarded.contains(&left),
            "the round did not clean L: {round:?}"
        );
        let rep = verify().await?;
        ensure!(rep.clean(), "after the cleanup round: {:?}", rep.findings);
        for c in [left, right] {
            ensure!(
                !stores.range_db_exists(PartitionId(c)).await?,
                "Db of orphan child {c} still there after the cleanup round"
            );
            let rec = read_owner(store, prefix, PartitionId(c)).await?;
            ensure!(
                rec.is_none(),
                "owner record of orphan child {c} still there after the cleanup round: {rec:?}"
            );
        }
        let pinned = pinned_on(&stores, 0).await?;
        ensure!(
            pinned == 0,
            "range 0 still has {pinned} final checkpoints of orphan children"
        );
        Ok(())
    }
    .await;
    n.node.shutdown().await;
    r
}

/// The safety net of the orphan-child cleanup loop in the nudge tests: one hour,
/// as the default — the test is done before the net fires, so the only thing
/// that can clean the orphan child is a nudge.
const ORPHAN_GRID: Duration = Duration::from_secs(3600);

/// **Nudge (a): the orphan child of an unfinished roll-back is cleaned within a
/// few seconds, without waiting for the net** (SM-14).
///
/// Interleaving:
/// 1. One node, one range, the orphan-child cleanup net [`ORPHAN_GRID`] (one
///    hour).
/// 2. Like `half_done_roll_back_is_finished_by_a_later_round`: split 0, taking
///    R stops at a gate; meanwhile reading L's manifest fails; release the gate
///    — the coordinator rolls back (the CAS dropping the intent wins), cleaning R
///    succeeds, cleaning L fails.
/// 3. The store heals.
///
/// Must see: within 15 s, L and R have no Db or owner record left and range 0
/// has no final checkpoint left — nobody calls `OrphanRetirer` by hand. Before
/// nudges: the loop only ran on its beat (with a one-hour net it had not run
/// yet), and L leaked until the next round.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orphan_of_a_half_done_roll_back_is_discarded_by_a_nudge() -> Result<()> {
    let bucket = common::bucket();
    let n = common::start_cluster_node_tweaked(
        &bucket,
        "undo-nudge",
        1,
        Duration::from_secs(1),
        |cfg| cfg.retire_orphans_every = Some(ORPHAN_GRID),
    )
    .await?;
    let r = async {
        wait_probe(
            &[&n],
            Duration::from_secs(20),
            "one node leads every range",
            |p| p.len() == 1 && p.values().all(|v| v == &[0]),
        )
        .await?;
        let t = n.node.cluster.table();
        let (left, right) = (t.next_range_id(), t.next_range_id() + 1);
        let gate = Gate::closed();
        for action in [Action::Gate(gate.clone()), Action::Fail { times: None }] {
            n.store.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Contains(format!("owners/range-{right}")),
                action,
            ));
        }
        let ops = n.node.ranges.clone();
        let split = tokio::spawn(async move { ops.split(PartitionId(0), None).await.map(|_| ()) });
        let t0 = Instant::now();
        while gate.blocked() == 0 {
            ensure!(!split.is_finished(), "split done before the take-R step");
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "split did not reach the take-R step"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        n.store.add_rule(Rule::new(
            OpSet::GET | OpSet::LIST | OpSet::HEAD,
            PathMatch::Contains(format!("ranges/{left}/manifest")),
            Action::Precondition,
        ));
        gate.open();
        let done = split.await?;
        n.store.clear_rules();
        ensure!(done.is_err(), "the split must fail at the take-R step");
        no_intent_left(&n, "after the roll-back").await?;

        let stores = stores_of(&n);
        let (store, prefix) = (stores.main().store(), stores.main().prefix());
        let t0 = Instant::now();
        loop {
            let mut left_over = Vec::new();
            for c in [left, right] {
                if stores.range_db_exists(PartitionId(c)).await? {
                    left_over.push(format!("Db of {c}"));
                }
                if read_owner(store, prefix, PartitionId(c)).await?.is_some() {
                    left_over.push(format!("owner record of {c}"));
                }
            }
            let pinned = pinned_on(&stores, 0).await?;
            if pinned > 0 {
                left_over.push(format!("{pinned} final checkpoints on range 0"));
            }
            if left_over.is_empty() {
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(15),
                "15 s after an unfinished roll-back, orphan children remain: {left_over:?} — no nudge"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }
    .await;
    n.node.shutdown().await;
    r
}

/// **A `Cleaned` record is positive evidence for the child cleanup path**
/// (found by M2 — the "not covered" item of SM-14).
///
/// Interleaving, one node:
/// 1. Split 0 into L, R: L gets taken (its owner record points at the node's
///    **live** session), taking R fails ⇒ roll back: the CAS dropping the intent
///    wins, the `ops/L` record = `Cleaned`.
/// 2. Cleaning L's Db fails (reading L's manifest misses): L's Db and owner
///    record stay (RD4).
/// 3. Split 0 again, this time with a commit: 0 leaves the table and retires.
/// 4. One orphan-child cleanup round.
///
/// Must see: the round cleans L. Before the fix: none of SM-14's evidence held
/// (L stands on no live range, L has an owner record), and the tombstone branch
/// kept `ownerLive` (the owner is a live node) — L leaked forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn claimed_orphan_is_discarded_by_the_ledger_after_its_source_retires() -> Result<()> {
    let n = alone_without_orphan_round("undo-claimed", 1).await?;
    let r = async {
        let t = n.node.cluster.table();
        let (left, right) = (t.next_range_id(), t.next_range_id() + 1);
        let gate = Gate::closed();
        for action in [Action::Gate(gate.clone()), Action::Fail { times: None }] {
            n.store.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Contains(format!("owners/range-{right}")),
                action,
            ));
        }
        let ops = n.node.ranges.clone();
        let split = tokio::spawn(async move { ops.split(PartitionId(0), None).await.map(|_| ()) });
        let t0 = Instant::now();
        while gate.blocked() == 0 {
            ensure!(!split.is_finished(), "split done before the take-R step");
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "split did not reach the take-R step"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        n.store.add_rule(Rule::new(
            OpSet::GET | OpSet::LIST | OpSet::HEAD,
            PathMatch::Contains(format!("ranges/{left}/manifest")),
            Action::Precondition,
        ));
        gate.open();
        ensure!(split.await?.is_err(), "split must fail at take-R");
        n.store.clear_rules();
        no_intent_left(&n, "after the roll-back").await?;
        let stores = stores_of(&n);
        let (store, prefix) = (stores.main().store(), stores.main().prefix());
        ensure!(
            stores.range_db_exists(PartitionId(left)).await?
                && read_owner(store, prefix, PartitionId(left))
                    .await?
                    .is_some(),
            "L must still have a Db and an owner record after the failed cleanup"
        );
        let op = cairn_store::ledger::read_op(store, prefix, left)
            .await?
            .ok_or_else(|| anyhow!("no ops/{left} record"))?;
        ensure!(
            op.record.phase == cairn_split::Phase::Cleaned,
            "{:?}",
            op.record
        );

        // 3. Split 0 again — commit: 0 retires, L no longer stands on a live range.
        // The source has just reopened after the roll-back: wait for a leader, then split.
        let t0 = Instant::now();
        loop {
            match n.node.ranges.split(PartitionId(0), None).await {
                Ok(_) => break,
                Err(e) => ensure!(t0.elapsed() < Duration::from_secs(20), "second split: {e}"),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let t = no_intent_left(&n, "after the second split").await?;
        ensure!(!t.contains(PartitionId(0)), "{t:?}");

        // 4. The orphan-child cleanup round.
        let session = n.node.session.clone();
        let round = cairn_server::gc::OrphanRetirer::default()
            .run_once(&stores, &n.node.cluster, &session)
            .await?;
        ensure!(
            round.discarded.contains(&left),
            "the round did not clean the taken orphan child L={left}: {round:?}"
        );
        ensure!(
            !stores.range_db_exists(PartitionId(left)).await?
                && read_owner(store, prefix, PartitionId(left))
                    .await?
                    .is_none(),
            "L still has a Db or an owner record after the cleanup round"
        );
        Ok(())
    }
    .await;
    n.node.shutdown().await;
    r
}

// ──────────── orphan intents while the leader is alive

/// Read the table back until the bucket has no intent left, within `deadline`.
/// A failed read (the store is having faults injected) is retried on the next beat.
async fn wait_no_intent(
    n: &ClusterNode,
    deadline: Duration,
    what: &str,
) -> Result<cairn_store::PartitionTable> {
    let t0 = Instant::now();
    loop {
        let read = n.node.cluster.reload().await;
        let t = n.node.cluster.table();
        if read.is_ok() && t.ranges().iter().all(|r| r.intent.is_none()) {
            return Ok((*t).clone());
        }
        ensure!(
            t0.elapsed() < deadline,
            "{what}: intent stuck on the bucket past {deadline:?} — auto split blocked cluster-wide: {t:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// **A torn intent CAS whose immediate read-backs all fail too does not leave
/// an intent stuck.** `undo_stray_intent` closes the case where the read-back
/// works; this one is the rest.
///
/// Interleaving:
/// 1. Node A alone, two ranges. A splits range 0.
/// 2. Intent CAS (the first `cluster.conf` write): the store applies the write,
///    then returns an error.
/// 3. The read-back resolving the ambiguity (`put_resolving`) fails, and
///    `undo_stray_intent`'s read-back fails too — a short burst of 503s on
///    `cluster.conf`.
/// 4. The store heals.
///
/// Must see: the split returns an error, and within the time limit the bucket
/// has no intent left; A still leads both ranges. Before the fix:
/// `undo_stray_intent` gave up after one failed read, range 0's leader was still
/// alive so no `factory` rolled back — the intent stayed until the leader
/// changed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn torn_intent_cas_with_failed_rereads_is_rolled_back_later() -> Result<()> {
    use cairn_store::testkit::Ambiguity;
    let n = alone("l3-torn-intent", 2).await?;
    let r = async {
        n.store.add_rule(ambiguous_table_cas(Ambiguity::Error, 0));
        // The first GET is `update`'s read before the CAS; later GETs fail.
        n.store.add_rule(Rule::new(
            OpSet::GET,
            PathMatch::Contains("cluster.conf".into()),
            Action::Ambiguous {
                reply: Ambiguity::Error,
                skip: 1,
                times: Some(6),
            },
        ));
        let done = n.node.ranges.split(PartitionId(0), None).await;
        ensure!(
            done.is_err(),
            "if the read-back fails the split must return an error: {:?}",
            done.map(|_| ())
        );
        // (4) The 503 burst passes after at most six more reads — do not clear the
        // rule: what must roll the intent back is the retry with backoff, not a lucky read.
        let t = wait_no_intent(
            &n,
            Duration::from_secs(20),
            "split, torn intent CAS, read-back failed",
        )
        .await?;
        n.store.clear_rules();
        ensure!(
            t.contains(PartitionId(0)),
            "the split failed but range 0 left the table: {t:?}"
        );
        wait_probe(
            &[&n],
            Duration::from_secs(20),
            "A still leads both ranges",
            |p| p.len() == 2 && p.values().all(|v| v == &[0]),
        )
        .await?;
        Ok(())
    }
    .await;
    n.node.shutdown().await;
    r
}

/// **The intent reconciliation loop rolls back an intent nobody coordinates.**
/// The last net for every path that leaves an intent on the table while the
/// source's leader is alive: a coordinator panicking before quiesce, its
/// roll-back giving up, or an old binary dying midway — no `factory` runs,
/// because nobody changes leader.
///
/// Interleaving:
/// 1. Node A alone, two ranges, leading both; the D-d runner's loop (proxy
///    tombstones, intent reconciliation) every 200 ms. A is the runner (it leads
///    range 0).
/// 2. A "coordinator" outside the node (a separate `ClusterConfig` on the same
///    bucket) writes a split intent on range 1 — allocating child ids for real —
///    then vanishes.
///
/// Must see: within the time limit, the bucket has no intent left; range 1 is
/// still in the table, the allocated child ids do not come back
/// (`next_range_id` unchanged), A still leads both ranges. Before the fix nobody
/// rolled back: the intent stayed until range 1's leader changed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_intent_without_coordinator_is_rolled_back_by_the_runner() -> Result<()> {
    let bucket = common::bucket();
    let n = common::start_cluster_node_tweaked(
        &bucket,
        "reconcile-a",
        2,
        Duration::from_secs(1),
        |cfg| {
            cfg.retire_orphans_every = Some(Duration::from_millis(200));
            cfg.reconcile_intents_every = Some(Duration::from_millis(200));
        },
    )
    .await?;
    let r = async {
        wait_probe(
            &[&n],
            Duration::from_secs(20),
            "A is leading every range",
            |p| p.len() == 2 && p.values().all(|v| v == &[0]),
        )
        .await?;
        // (2)
        let store: Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
        let root = slatedb::object_store::path::Path::default();
        let other = cairn_store::ClusterConfig::open(&store, &root, 2).await?;
        let t = other.table();
        let one = *t
            .range(PartitionId(1))
            .ok_or_else(|| anyhow!("no range 1: {t:?}"))?;
        let intent = cairn_store::RangeIntent::Splitting {
            at: one.start + (one.end - one.start) / 2 + 1,
            left: PartitionId(t.next_range_id()),
            right: PartitionId(t.next_range_id() + 1),
        };
        let marked = other
            .update(t.version(), |t| {
                let ranges = t
                    .ranges()
                    .iter()
                    .map(|r| {
                        if r.id == one.id {
                            cairn_store::RangeEntry {
                                intent: Some(intent),
                                ..*r
                            }
                        } else {
                            *r
                        }
                    })
                    .collect();
                cairn_store::PartitionTable::from_entries(
                    t.version() + 1,
                    t.next_range_id() + 2,
                    ranges,
                )
            })
            .await?;
        ensure!(
            matches!(marked, cairn_store::TableUpdate::Applied(_)),
            "cannot write the intent: {marked:?}"
        );
        let next = t.next_range_id() + 2;

        let after =
            wait_no_intent(&n, Duration::from_secs(15), "intent with no coordinator").await?;
        ensure!(after.contains(PartitionId(1)), "range 1 gone: {after:?}");
        ensure!(
            after.next_range_id() == next,
            "allocated ids came back: {after:?}"
        );
        wait_probe(
            &[&n],
            Duration::from_secs(20),
            "A still leads both ranges",
            |p| p.len() == 2 && p.values().all(|v| v == &[0]),
        )
        .await?;
        Ok(())
    }
    .await;
    n.node.shutdown().await;
    r
}

/// **Retirement by an in-RAM table that is not on the bucket.**
/// `Transport::send` applies the table in the body of a peer's `409 wrong_range`
/// without reading the bucket (D23); `reconcile` used to retire — writing a
/// tombstone, irreversibly — every range absent from the in-RAM table.
///
/// Interleaving:
/// 1. Node A alone, two ranges, leading both.
/// 2. A applies a **newer table that is not on the bucket** (as if learned from
///    the `409` body of a faulty peer): range 1 is replaced by two ranges with
///    new ids.
///
/// Must see: within 3 s, range 1's owner record carries no tombstone, and A
/// still holds range 1 (the owner record has not been handed off). A range only
/// retires when the table **read from the bucket** no longer has it either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_is_not_retired_by_a_table_that_is_not_on_the_bucket() -> Result<()> {
    let bucket = common::bucket();
    bucket.allow(
        "orphan_db",
        "the test's fake table makes A build engines for two ranges not on the bucket",
    );
    let n = common::start_cluster_node(&bucket, "bogus-a", 2, Duration::from_secs(1)).await?;
    let r = async {
        wait_probe(
            &[&n],
            Duration::from_secs(20),
            "A is leading every range",
            |p| p.len() == 2 && p.values().all(|v| v == &[0]),
        )
        .await?;
        // (2)
        let t = n.node.cluster.table();
        let one = *t
            .range(PartitionId(1))
            .ok_or_else(|| anyhow!("no range 1: {t:?}"))?;
        let mid = one.start + (one.end - one.start) / 2;
        let next = t.next_range_id();
        let mut ranges: Vec<cairn_store::RangeEntry> = t
            .ranges()
            .iter()
            .filter(|r| r.id != one.id)
            .copied()
            .collect();
        ranges.push(cairn_store::RangeEntry::new(
            one.start,
            mid,
            PartitionId(next),
        ));
        ranges.push(cairn_store::RangeEntry::new(
            mid + 1,
            one.end,
            PartitionId(next + 1),
        ));
        ranges.sort_by_key(|r| r.start);
        let bogus = cairn_store::PartitionTable::from_entries(t.version() + 10, next + 2, ranges)?;
        ensure!(
            n.node.cluster.apply(std::sync::Arc::new(bogus)),
            "A must apply the fake table"
        );

        let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
        let root = slatedb::object_store::path::Path::default();
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            let rec = read_owner(&store, &root, PartitionId(1)).await?;
            ensure!(
                !rec.as_ref().is_some_and(|r| r.retired),
                "range 1 retired by a table that is not on the bucket: {rec:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let rec = read_owner(&store, &root, PartitionId(1)).await?;
        ensure!(
            rec.as_ref()
                .is_some_and(|r| !r.released && r.session.node == "bogus-a"),
            "A must still hold range 1 (owner record not handed off): {rec:?}"
        );
        Ok(())
    }
    .await;
    n.node.shutdown().await;
    r
}

// ─────────────────────────── closed mark on the Db itself

/// **A stale closed mark on a live range removes itself on the next open**
/// (SM-17). The roll-back clears the source's `closed` flag **after** the CAS
/// dropping the intent; dying between the two steps leaves a range that is
/// alive again in the table while its Db is still closed.
///
/// Interleaving:
/// 1. Node A alone, one range. Write one invocation, A stops cleanly.
/// 2. Close range 0's Db by hand — the shape of a roll-back that died after the
///    CAS dropping the intent, before clearing the flag. Table: range 0, no
///    intent.
/// 3. Node B comes up on the same bucket.
///
/// Must see: B's first open meets the flag, reads a fresh table, sees a live
/// range with no intent, and clears the flag; B leads range 0 within 30 s, the
/// old invocation is readable, and range 0's Db no longer has the flag. Without
/// the self-clearing step range 0 would never have a leader again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_close_on_a_live_range_heals_on_next_open() -> Result<()> {
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .object("Obj", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let a = common::start_cluster_node(&bucket, "heal-a", 1, Duration::from_secs(1)).await?;
    wait_probe(
        &[&a],
        Duration::from_secs(20),
        "A is leading range 0",
        |p| p.get(&0).is_some_and(|v| v == &[0]),
    )
    .await?;
    common::register(&a.node.url(), &sdk.url).await?;
    let (code, body) = common::send(&a.node.url(), "Obj", "run", Some("k"), "\"x\"").await?;
    ensure!(code == 200 || code == 202, "send: {code} {body}");
    let id = body["id"]
        .as_str()
        .ok_or_else(|| anyhow!("no id: {body}"))?
        .to_string();
    a.node.shutdown().await;

    let stores = ObjectStores::single(StoreLocation::from_store(bucket.clone(), "", "memory://"));
    stores.close_range_db(PartitionId(0)).await?;
    ensure!(stores.range_db_closed(PartitionId(0)).await? == Some(true));

    let b = common::start_cluster_node(&bucket, "heal-b", 1, Duration::from_secs(1)).await?;
    let r = async {
        wait_probe(
            &[&b],
            Duration::from_secs(30),
            "B is leading range 0",
            |p| p.get(&0).is_some_and(|v| v == &[0]),
        )
        .await?;
        let (code, got) = common::invocation(&b.node.url(), &id).await?;
        ensure!(code == 200, "old invocation: {code} {got}");
        ensure!(
            stores.range_db_closed(PartitionId(0)).await? == Some(false),
            "the stale closed mark must be removed"
        );
        Ok(())
    }
    .await;
    b.node.shutdown().await;
    r
}

// ─────────────────────── losing `cluster.conf` does not produce a new table

/// **A bucket with range data but no `cluster.conf` ⇒ the node refuses to
/// start.** Owner records do not carry key ranges, so the table cannot be
/// rebuilt from the rest of the bucket; a new `equal` table would hand out ids
/// again that collide with live Db directories (breaking D22) and leave holes in
/// the key space where ranges had split.
///
/// Order:
/// 1. Node A alone, one range. Write one invocation, split range 0 ⇒ table v2
///    (ranges 1, 2; `next_range_id` 3). A stops cleanly.
/// 2. Delete `cluster.conf` from outside (an operator, a lifecycle rule, a
///    store error).
/// 3. Node B starts on the same bucket.
/// 4. Put back exactly the old bytes of `cluster.conf` (like bucket
///    versioning), node C starts.
///
/// Must see: (3) B refuses, the error points at the runbook
/// `docs/runbooks/cluster-conf-lost.md`, and the bucket still has no
/// `cluster.conf`; (4) C leads ranges 1 and 2, the old invocation is readable.
/// Before the fix B came up with an `equal` v1 table (range 0) on a bucket that
/// had ranges 1, 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_cluster_conf_on_a_bucket_with_ranges_refuses_to_start() -> Result<()> {
    use slatedb::object_store::{path::Path, ObjectStoreExt};
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .object("Obj", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let conf = Path::from("cluster.conf");

    // (1)
    let a = common::start_cluster_node(&bucket, "lost-a", 1, Duration::from_secs(1)).await?;
    let r = async {
        wait_probe(
            &[&a],
            Duration::from_secs(20),
            "A is leading range 0",
            |p| p.get(&0).is_some_and(|v| v == &[0]),
        )
        .await?;
        common::register(&a.node.url(), &sdk.url).await?;
        let (code, body) = common::send(&a.node.url(), "Obj", "run", Some("k"), "\"x\"").await?;
        ensure!(code == 200 || code == 202, "send: {code} {body}");
        let id = body["id"]
            .as_str()
            .ok_or_else(|| anyhow!("no id: {body}"))?
            .to_string();
        let (code, view) = post_json(format!("{}/ranges/0/split", a.node.url())).await?;
        ensure!(code == 200, "split: {code} {view}");
        Ok::<_, anyhow::Error>(id)
    }
    .await;
    a.node.shutdown().await;
    let id = r?;

    // (2)
    let saved = bucket.get(&conf).await?.bytes().await?;
    bucket.delete(&conf).await?;

    // (3)
    match common::start_cluster_node(&bucket, "lost-b", 1, Duration::from_secs(1)).await {
        Ok(b) => {
            let table = b.node.cluster.table();
            b.node.shutdown().await;
            // Put the real table back so `verify` while cleaning up the bucket does not hide the original error.
            bucket.put(&conf, saved.into()).await?;
            return Err(anyhow!(
                "B started on a bucket with range data but no cluster.conf, \
                 with a new table {table:?}"
            ));
        }
        Err(e) => {
            let msg = format!("{e:#}");
            ensure!(
                msg.contains("docs/runbooks/cluster-conf-lost.md"),
                "the error must point at the runbook: {msg}"
            );
        }
    }
    ensure!(
        bucket.head(&conf).await.is_err(),
        "the node refused but still wrote cluster.conf"
    );

    // (4)
    bucket.put(&conf, saved.into()).await?;
    let c = common::start_cluster_node(&bucket, "lost-c", 1, Duration::from_secs(1)).await?;
    let r = async {
        wait_probe(
            &[&c],
            Duration::from_secs(30),
            "C leads ranges 1 and 2",
            |p| p.len() == 2 && [1, 2].iter().all(|r| p.get(r).is_some_and(|v| v == &[0])),
        )
        .await?;
        let (code, got) = common::invocation(&c.node.url(), &id).await?;
        ensure!(code == 200, "old invocation: {code} {got}");
        Ok(())
    }
    .await;
    c.node.shutdown().await;
    r
}

/// Ctl on `bucket` (no node needs to run).
fn ctl_of(bucket: &common::Bucket) -> cairn_server::ctl::Ctl {
    cairn_server::ctl::Ctl::new(ObjectStores::single(StoreLocation::from_store(
        (*bucket).clone(),
        "",
        "memory://",
    )))
}

fn restore_opts(apply: bool) -> cairn_server::ctl::RepairOptions {
    cairn_server::ctl::RepairOptions {
        apply,
        force: false,
        who: "test@split".into(),
        command: "cairn-ctl table restore".into(),
        expect_version: None,
        now_ms: cairn_store::entropy::now_ms(),
    }
}

/// **`cairn-ctl table restore` rebuilds a lost table after many splits/merges**
/// (SM-22/SM-23).
///
/// Order:
/// 1. Node A alone, one range. Write one invocation, then `N` splits/merges
///    chosen by a seed (printed; `CAIRN_RESTORE_SEED` reruns exactly that
///    sequence): merge two adjacent ranges or split one range. A stops cleanly.
/// 2. Delete `cluster.conf` from outside.
/// 3. `table restore` dry run: the proof passes, nothing is written.
/// 4. `table restore --apply`: `cluster.conf` is exactly the bytes of the last
///    table.
/// 5. `verify` finds nothing; node B starts, leads every range of the table,
///    the old invocation is readable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn table_restore_rebuilds_the_last_table_after_random_splits_and_merges() -> Result<()> {
    use slatedb::object_store::{path::Path, ObjectStoreExt};
    const N: usize = 6;
    let seed: u64 = std::env::var("CAIRN_RESTORE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(7)
        });
    eprintln!("table_restore: CAIRN_RESTORE_SEED={seed}");
    let mut rng = seed | 1;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .object("Obj", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let conf = Path::from("cluster.conf");

    // (1)
    let a = common::start_cluster_node(&bucket, "restore-a", 1, Duration::from_secs(1)).await?;
    let r = async {
        wait_probe(
            &[&a],
            Duration::from_secs(20),
            "A is leading range 0",
            |p| p.get(&0).is_some_and(|v| v == &[0]),
        )
        .await?;
        common::register(&a.node.url(), &sdk.url).await?;
        let (code, body) = common::send(&a.node.url(), "Obj", "run", Some("k"), "\"x\"").await?;
        ensure!(code == 200 || code == 202, "send: {code} {body}");
        let id = body["id"]
            .as_str()
            .ok_or_else(|| anyhow!("no id: {body}"))?
            .to_string();
        let mut ops = Vec::new();
        for _ in 0..N {
            let t = a.node.cluster.table();
            let ranges: Vec<PartitionId> = t.ranges().iter().map(|r| r.id).collect();
            let pick = next() as usize;
            let (table, _) = if ranges.len() >= 2 && pick.is_multiple_of(2) {
                let i = (pick / 2) % (ranges.len() - 1);
                ops.push(format!("merge {}+{}", ranges[i].0, ranges[i + 1].0));
                a.node.ranges.merge(ranges[i], ranges[i + 1]).await
            } else {
                let p = ranges[(pick / 2) % ranges.len()];
                ops.push(format!("split {}", p.0));
                a.node.ranges.split(p, None).await
            }
            .map_err(|e| anyhow!("{ops:?}: {e}"))?;
            wait_probe(
                &[&a],
                Duration::from_secs(30),
                "A is leading every range",
                |p| {
                    p.len() == table.partition_count() as usize
                        && table
                            .partitions()
                            .all(|r| p.get(&r.0).is_some_and(|v| v == &[0]))
                },
            )
            .await?;
        }
        eprintln!("table_restore: {ops:?}");
        Ok::<_, anyhow::Error>(id)
    }
    .await;
    a.node.shutdown().await;
    let id = r?;

    // (2)
    let saved = bucket.get(&conf).await?.bytes().await?;
    bucket.delete(&conf).await?;
    let ctl = ctl_of(&bucket);

    // (3)
    let dry = ctl
        .table_restore(&restore_opts(false))
        .await
        .map_err(|e| anyhow!("dry run: {e}"))?;
    ensure!(!dry.applied, "{dry:?}");
    ensure!(
        bucket.head(&conf).await.is_err(),
        "dry run but cluster.conf was written"
    );
    let want = cairn_store::decode_table(&saved)?;
    ensure!(dry.version == want.version(), "{dry:?} / {want:?}");

    // (4)
    let applied = ctl
        .table_restore(&restore_opts(true))
        .await
        .map_err(|e| anyhow!("--apply: {e}"))?;
    ensure!(applied.applied && applied.audit.is_some(), "{applied:?}");
    let back = bucket.get(&conf).await?.bytes().await?;
    ensure!(back == saved, "rebuilt table differs from the last table");
    ensure!(
        matches!(
            ctl.table_restore(&restore_opts(true)).await,
            Err(cairn_server::ctl::CtlError::Refused(_))
        ),
        "cluster.conf already present but restore still acted"
    );

    // (5)
    let found = bucket.verify().await?;
    ensure!(found.is_empty(), "verify after restore: {found:?}");
    let b = common::start_cluster_node(&bucket, "restore-b", 1, Duration::from_secs(1)).await?;
    let r = async {
        wait_probe(
            &[&b],
            Duration::from_secs(30),
            "B is leading every range",
            |p| {
                p.len() == want.partition_count() as usize
                    && want
                        .partitions()
                        .all(|r| p.get(&r.0).is_some_and(|v| v == &[0]))
            },
        )
        .await?;
        let (code, got) = common::invocation(&b.node.url(), &id).await?;
        ensure!(code == 200, "old invocation: {code} {got}");
        Ok(())
    }
    .await;
    b.node.shutdown().await;
    r
}

// ─────────────────────────────── durable operation records

/// The `ops/<key>` record of the op whose first child is `key`.
async fn ledger_of(n: &ClusterNode, key: u32) -> Result<cairn_store::ledger::OpRecord> {
    let stores = stores_of(n);
    cairn_store::ledger::read_op(stores.main().store(), stores.main().prefix(), key)
        .await?
        .map(|o| o.record)
        .ok_or_else(|| anyhow!("no ops/{key} record"))
}

/// **Every split/merge leaves a durable operation record** (SM-19):
/// `ops/<first child>` is created at `Intent` right after the intent CAS wins,
/// goes through the phases by the transition rules, and ends at `Committed`
/// (won) or `Cleaned` (the roll-back CAS is done).
///
/// Order: (1) split 0 ⇒ `ops/1` `Committed`, carrying `sealed` = the epoch of
/// 0's just-quiesced writer; (2) merge 1 + 2 ⇒ `ops/3` `Committed`, `sealed` of
/// both; (3) split 3 where taking the left child fails ⇒ the coordinator rolls
/// back ⇒ `ops/4` `Cleaned`.
///
/// Must see: three records, right phases, right ops; the table has no intent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_op_leaves_a_ledger_ending_committed_or_cleaned() -> Result<()> {
    use cairn_split::{Op, Phase};
    let node = alone("node-ledger", 1).await?;
    let url = node.node.url();
    let result = common::within(Duration::from_secs(90), "operation records", async {
        let (code, view) = post_json(format!("{url}/ranges/0/split")).await?;
        ensure!(code == 200, "split: {code} {view}");
        let r = ledger_of(&node, 1).await?;
        ensure!(r.phase == Phase::Committed, "{r:?}");
        ensure!(
            r.op == Op::Split {
                source: 0,
                left: 1,
                right: 2
            },
            "{r:?}"
        );
        ensure!(r.sealed.len() == 1 && r.sealed[0].0 == 0, "{r:?}");

        let (code, view) = post_json(format!("{url}/ranges/merge?left=1&right=2")).await?;
        ensure!(code == 200, "merge: {code} {view}");
        let r = ledger_of(&node, 3).await?;
        ensure!(r.phase == Phase::Committed, "{r:?}");
        ensure!(
            r.op == Op::Merge {
                lo: 1,
                hi: 2,
                into: 3
            },
            "{r:?}"
        );
        ensure!(r.sealed.len() == 2, "{r:?}");

        // Taking the left child fails ⇒ roll back.
        node.store.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains("owners/range-4".into()),
            Action::Fail { times: Some(1) },
        ));
        let (code, view) = post_json(format!("{url}/ranges/3/split")).await?;
        ensure!(code >= 500, "the split must fail: {code} {view}");
        no_intent_left(&node, "after the failed split").await?;
        let r = ledger_of(&node, 4).await?;
        ensure!(r.phase == Phase::Cleaned, "{r:?}");
        Ok(())
    })
    .await;
    node.node.shutdown().await;
    result
}

/// **Settled records are cleaned up**: the record of an op at an outcome where
/// every range involved has settled — `Committed` with every source carrying a
/// tombstone, `Cleaned` with every child out of Db and owner record — leaves
/// `ops/` in the D-d round, after a minimum age.
///
/// 1. Split 0 ⇒ `ops/1` `Committed`; wait for 0's tombstone.
/// 2. Split 1 where taking the left child fails ⇒ the coordinator rolls back ⇒
///    `ops/3` `Cleaned`; children 3, 4 cleaned.
/// 3. A D-d round with the default `min_age` (30 minutes): both records just
///    reached their outcome.
/// 4. A D-d round with `min_age` 0.
///
/// Must see: after (3) both records are still there; after (4) no record is left
/// under `ops/`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settled_ledgers_are_reaped_by_the_dd_round() -> Result<()> {
    use cairn_split::Phase;
    let node = alone("node-ledger-reap", 1).await?;
    let url = node.node.url();
    let stores = stores_of(&node);
    let (store, prefix) = (
        stores.main().store().clone(),
        stores.main().prefix().clone(),
    );
    let result = common::within(Duration::from_secs(90), "clean op records", async {
        let (code, view) = post_json(format!("{url}/ranges/0/split")).await?;
        ensure!(code == 200, "split: {code} {view}");
        ensure!(ledger_of(&node, 1).await?.phase == Phase::Committed);
        let t0 = std::time::Instant::now();
        while !matches!(
            cairn_store::read_owner(&store, &prefix, PartitionId(0)).await?,
            Some(rec) if rec.retired
        ) {
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "range 0 has no tombstone yet"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        node.store.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains("owners/range-3".into()),
            Action::Fail { times: Some(1) },
        ));
        let (code, view) = post_json(format!("{url}/ranges/1/split")).await?;
        ensure!(code >= 500, "the split must fail: {code} {view}");
        no_intent_left(&node, "after the failed split").await?;
        ensure!(ledger_of(&node, 3).await?.phase == Phase::Cleaned);
        for c in [3, 4] {
            ensure!(
                !stores.range_db_exists(PartitionId(c)).await?
                    && cairn_store::read_owner(&store, &prefix, PartitionId(c))
                        .await?
                        .is_none(),
                "child {c} of the rolled-back split must be cleaned"
            );
        }

        let young = cairn_server::gc::GcConfig::default();
        cairn_server::gc::run_once(&stores, &node.node.cluster, &young)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        for key in [1, 3] {
            ensure!(
                cairn_store::ledger::read_op(&store, &prefix, key)
                    .await?
                    .is_some(),
                "record ops/{key} that just reached its outcome must survive a round with min_age"
            );
        }

        let now = cairn_server::gc::GcConfig {
            min_age: Duration::ZERO,
            ..cairn_server::gc::GcConfig::default()
        };
        cairn_server::gc::run_once(&stores, &node.node.cluster, &now)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        for key in [1, 3] {
            ensure!(
                cairn_store::ledger::read_op(&store, &prefix, key)
                    .await?
                    .is_none(),
                "settled record ops/{key} stays on the bucket forever"
            );
        }
        Ok(())
    })
    .await;
    node.node.shutdown().await;
    result
}

// ─────────────────────────────────────── server-side schedules across split/merge

/// A schedule name whose pk is in the left (`left`) or right half of the key space.
fn schedule_in_half(left: bool, prefix: &str) -> String {
    (0..100_000)
        .map(|i| format!("{prefix}-{i}"))
        .find(|n| {
            let pk =
                cairn_store::partition_key_for_schedule(&cairn_store::TenantId::default(), None, n)
                    .0;
            (pk < MID) == left
        })
        .expect("found a name")
}

/// `PUT /schedules/{name}` every minute ⇒ `(id, nextAt)`.
async fn put_schedule(url: &str, name: &str, target: serde_json::Value) -> Result<(String, u64)> {
    let r = reqwest::Client::new()
        .put(format!("{url}/schedules/{name}"))
        .json(&serde_json::json!({ "everyMs": 60_000, "target": target }))
        .send()
        .await?;
    let code = r.status().as_u16();
    let v: serde_json::Value = r.json().await?;
    ensure!(code == 200, "PUT {name}: {code} {v}");
    Ok((
        v["id"].as_str().unwrap_or_default().to_string(),
        v["nextAt"].as_u64().ok_or_else(|| anyhow!("nextAt {v}"))?,
    ))
}

/// Returns once at least `margin` is left before the next whole minute of the wall
/// clock (the marks of an `everyMs: 60000` schedule), waiting past the mark if not.
async fn clear_of_a_minute_mark(margin: Duration) -> Result<()> {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
    let into = Duration::from_millis((now.as_millis() % 60_000) as u64);
    let left = Duration::from_secs(60) - into;
    if left < margin {
        // A fixed wait on the wall clock, not on an event: the mark is the event.
        tokio::time::sleep(left + Duration::from_millis(100)).await;
    }
    Ok(())
}

async fn get_schedule(url: &str, name: &str) -> Result<serde_json::Value> {
    let r = reqwest::get(format!("{url}/schedules/{name}")).await?;
    let code = r.status().as_u16();
    let v: serde_json::Value = r.json().await?;
    ensure!(code == 200, "GET {name}: {code} {v}");
    Ok(v)
}

/// `/admin/tick?now_ms=` until `200` (during a split: `503`, retry).
async fn tick_at(url: &str, now_ms: u64) -> Result<()> {
    let t0 = Instant::now();
    loop {
        let (code, body) = post_json(format!("{url}/admin/tick?now_ms={now_ms}")).await?;
        if code == 200 {
            return Ok(());
        }
        ensure!(
            code == 503 && t0.elapsed() < Duration::from_secs(20),
            "tick: {code} {body}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// **CG-14 `schedule_due_during_split`**: a one-node cluster, one range; three
/// schedules every minute — two with unkeyed targets (schedules in the left
/// half and the right half), one with a virtual object target (fires in another
/// range through a `Fire` row).
///
/// 1. A tick at mark 1 ⇒ each schedule fires once.
/// 2. Start splitting range 0 and hold after quiesce; tick at mark 2 meanwhile
///    (waits for the table to settle, or `503`), then release.
/// 3. After the split: each schedule fires mark 2 **exactly once** (not lost —
///    the schedule row and the `ScheduleDue` index follow the pk to the child,
///    the index is rebuilt; not duplicated — the run id is deterministic). `GET`
///    sees `fired = 2`, `nextAt` = mark 3.
/// 4. Merge back; tick at mark 3, then tick again ⇒ each schedule fires exactly
///    once more.
/// 5. `GET /schedules` lists all three, not `partial`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedule_due_during_split() -> Result<()> {
    let (nodes, gate) = split_held().await?;
    let n = &nodes[0];
    let url = n.node.url();
    let sdk = FakeSdk::builder()
        .service("Report", "build", vec![Step::output("ok")])
        .object("Cart", "sweep", vec![Step::output("swept")])
        .spawn()
        .await?;
    let result = async {
        common::register(&url, &sdk.url).await?;
        // Each PUT takes its first mark from the wall clock: three PUTs on both sides of
        // a whole minute get marks a minute apart. Start them clear of one.
        clear_of_a_minute_mark(Duration::from_secs(5)).await?;
        let plain = serde_json::json!({ "service": "Report", "handler": "build" });
        let (left, right) = (
            schedule_in_half(true, "left"),
            schedule_in_half(false, "right"),
        );
        let keyed = schedule_in_half(true, "vo");
        let (_, at) = put_schedule(&url, &left, plain.clone()).await?;
        let (_, at_r) = put_schedule(&url, &right, plain).await?;
        let (_, at_k) = put_schedule(
            &url,
            &keyed,
            serde_json::json!({ "service": "Cart", "handler": "sweep", "key": "u1" }),
        )
        .await?;
        ensure!(at == at_r && at == at_k, "marks differ: {at} {at_r} {at_k}");
        let runs = |want: usize| {
            let sdk = &sdk;
            async move {
                let t0 = Instant::now();
                loop {
                    let got = sdk.starts_len();
                    if got == want {
                        return Ok::<_, anyhow::Error>(());
                    }
                    ensure!(got < want, "extra run: {got} > {want}");
                    ensure!(
                        t0.elapsed() < Duration::from_secs(20),
                        "missing run: {got} < {want}"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        };

        // 1.
        tick_at(&url, at).await?;
        runs(3).await?;

        // 2.
        let splitting = hold_split(n, &gate).await?;
        let at2 = at + 60_000;
        let tick_url = url.clone();
        let ticking = tokio::spawn(async move { tick_at(&tick_url, at2).await });
        tokio::time::sleep(Duration::from_millis(300)).await;
        gate.open();
        let (code, view) = splitting.await??;
        ensure!(code == 200, "split: {code} {view}");
        ticking.await??;
        // Tick again after the table settles: a mark that already fired does not fire
        // again; one that did not (the tick during the split reached the closed parent) fires now.
        tick_at(&url, at2).await?;

        // 3.
        runs(6).await?;
        for name in [&left, &right, &keyed] {
            let v = get_schedule(&url, name).await?;
            ensure!(
                v["fired"] == 2 && v["nextAt"] == at2 + 60_000,
                "{name}: {v}"
            );
        }
        let ranges: Vec<u32> = view["ranges"]
            .as_array()
            .ok_or_else(|| anyhow!("{view}"))?
            .iter()
            .map(|r| r["range"].as_u64().unwrap_or(0) as u32)
            .collect();
        ensure!(ranges.len() == 2, "{view}");

        // 4.
        let (code, view) = post_json(format!(
            "{url}/ranges/merge?left={}&right={}",
            ranges[0], ranges[1]
        ))
        .await?;
        ensure!(code == 200, "merge: {code} {view}");
        let at3 = at2 + 60_000;
        tick_at(&url, at3).await?;
        tick_at(&url, at3).await?;
        runs(9).await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        runs(9).await?;

        // 5.
        let l: serde_json::Value = reqwest::get(format!("{url}/schedules"))
            .await?
            .json()
            .await?;
        ensure!(
            l["partial"] == false && l["schedules"].as_array().map(Vec::len) == Some(3),
            "{l}"
        );
        Ok(())
    }
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    result
}

// ─────────────────────────────── children stand alone after a split

/// Split range `range` (in the middle) over HTTP, return the ids of the two
/// children (left, right) — the two ids just allocated (`ranges` is the whole
/// table after the commit).
async fn split_range(url: &str, range: u32) -> Result<(u32, u32)> {
    let (code, view) = post_json(format!("{url}/ranges/{range}/split")).await?;
    ensure!(code == 200, "split {range}: {code} {view}");
    let first_child = view["nextRangeId"].as_u64().unwrap_or(0).saturating_sub(2) as u32;
    let mut children: Vec<u32> = view["ranges"]
        .as_array()
        .ok_or_else(|| anyhow!("no ranges: {view}"))?
        .iter()
        .map(|r| r["range"].as_u64().unwrap_or(0) as u32)
        .filter(|r| *r >= first_child)
        .collect();
    children.sort_unstable();
    match children[..] {
        [.., a, b] => Ok((a, b)),
        _ => Err(anyhow!("two children not found: {view}")),
    }
}

/// Force a full compaction of range `range` (led by `node`): flush the
/// memtable, ask to compact every L0 and sorted run into the last run, wake the
/// compactor, wait until done. The range's data sits in a sorted run — which its
/// children inherit and do not rewrite by themselves.
async fn compact_fully(
    node: &cairn_server::node::RunningNode,
    stores: &ObjectStores,
    range: u32,
) -> Result<()> {
    let h = node
        .partitions
        .get(PartitionId(range))
        .ok_or_else(|| anyhow!("no engine for range {range}"))?;
    h.flush_memtable().await?;
    let Some(mut id) = stores.submit_full_compaction(range).await?.requested() else {
        return Ok(());
    };
    h.wake_compactor().await?;
    let t0 = Instant::now();
    loop {
        match stores.full_compaction_done(range, &mut id).await? {
            Some(true) => return Ok(()),
            Some(false) => return Err(anyhow!("full compaction of range {range} failed")),
            None => {}
        }
        ensure!(
            t0.elapsed() < Duration::from_secs(20),
            "full compaction of range {range} not done after 20 s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// **Cold children stand alone after two successive splits.**
///
/// 1. One range (0), write 12 invocations (8 in the left half, 4 in the right
///    half), then compact fully: the data is in a sorted run of 0.
/// 2. Split 0 ⇒ {L, R}; write three invocations into L; split L ⇒ {LL, LR}.
///    The children inherit 0's sorted run (through L) — SlateDB's own
///    compaction only rewrites L0, so a **cold** child (no writes) never
///    rewrites an inherited sorted run. (LL, LR may drop L by themselves after
///    compacting their own L0; not 0.)
/// 3. SlateDB's compactor and GC run fast (100–200 ms intervals, `min_age` 0,
///    compactor checkpoint 1 s); the stand-alone loop runs every 200 ms, and a
///    retired ancestor counts as old at once (`ancestor_age` 0 — 24 h in the
///    product).
///
/// Must see: within 60 s no live range stands on any ancestor (depth 0, so
/// ≤ 1); one D-d round (`min_age` 0) deletes the Dbs of 0 and of L; every
/// invocation written reads back. Before the fix: nobody compacted cold
/// children, R, LL, LR stood on 0 forever, and D-d could not delete 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_children_stop_standing_on_ancestors_after_nested_splits() -> Result<()> {
    let mut cfg = common::solo_config(1);
    cfg.stores = cfg
        .stores
        .clone()
        .with_polls(cairn_store::PollIntervals {
            manifest: Duration::from_millis(100),
            compactor: Duration::from_millis(100),
            compaction_worker: Duration::from_millis(100),
            gc: Duration::from_millis(200),
            gc_idle_skip_max: Duration::from_millis(200),
            gc_min_age: Duration::ZERO,
        })
        .with_compactor_checkpoint_lifetime(Duration::from_secs(1));
    cfg.lineage.every = Duration::from_millis(200);
    cfg.lineage.retry_after = Duration::from_secs(5);
    cfg.lineage.ancestor_age = Duration::ZERO;
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("ok")])
        .spawn()
        .await?;

    let result = common::within(Duration::from_secs(120), "children stand alone", async {
        common::register(&url, &sdk.url).await?;
        let mut ids = Vec::new();
        for i in 0..12 {
            let key = key_in_half("Counter", i % 3 != 2, "root", i);
            let (code, body, _) = invoke_retrying(&url, "Counter", "add", &key).await?;
            ensure!(code == 200, "{key}: {code} {body}");
            ids.push((body["id"].as_str().unwrap_or_default().to_string(), "ok"));
        }
        all_completed(&url, &ids, Duration::from_secs(20)).await?;
        compact_fully(&node, &stores, 0).await?;
        let (l, r) = split_range(&url, 0).await?;
        // L has its own data (L0) at split time: LL, LR stand on both L and 0.
        for i in 0..3 {
            let key = key_in_half("Counter", true, "left", i);
            let (code, body, _) = invoke_retrying(&url, "Counter", "add", &key).await?;
            ensure!(code == 200, "{key}: {code} {body}");
            ids.push((body["id"].as_str().unwrap_or_default().to_string(), "ok"));
        }
        all_completed(&url, &ids, Duration::from_secs(20)).await?;
        let (ll, lr) = split_range(&url, l).await?;
        let depth = |p: u32| {
            let stores = stores.clone();
            async move {
                stores
                    .range_external_depth(PartitionId(p))
                    .await?
                    .ok_or_else(|| anyhow!("Db of range {p} has no manifest yet"))
            }
        };
        println!(
            "lineage: depth right after two splits: LL {}, LR {}, R {}",
            depth(ll).await?,
            depth(lr).await?,
            depth(r).await?
        );
        let t0 = Instant::now();
        loop {
            let d = (depth(ll).await?, depth(lr).await?, depth(r).await?);
            if d == (0, 0, 0) {
                println!("lineage: all children detached after {:?}", t0.elapsed());
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "cold children still stand on ancestors after 60 s: depth (LL, LR, R) = {d:?}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let round = cairn_server::gc::run_once(
            &stores,
            &node.cluster,
            &cairn_server::gc::GcConfig {
                min_age: Duration::ZERO,
                ..cairn_server::gc::GcConfig::default()
            },
        )
        .await
        .map_err(|e| anyhow!("{e}"))?;
        let mut deleted = round.deleted.clone();
        deleted.sort_unstable();
        ensure!(
            deleted == vec![0, l],
            "D-d must delete the Dbs of 0 and L ({l}) — no child stands on them any more: {round:?}"
        );
        all_completed(&url, &ids, Duration::from_secs(20)).await?;
        Ok(())
    })
    .await;
    node.shutdown().await;
    result
}

/// An object key of `service` whose partition key is in `[lo, hi)`.
fn key_in_span(service: &str, lo: u64, hi: u64, prefix: &str, n: usize) -> String {
    (0..1_000_000)
        .map(|i| format!("{prefix}-{n}-{i}"))
        .find(|k| {
            let pk =
                partition_key_for_target(&cairn_store::TenantId::default(), None, service, k, None)
                    .expect("has a key")
                    .0;
            lo <= pk && pk < hi
        })
        .expect("found a key")
}

/// **The stand-alone loop's depth ≥ 2 rule fires by itself**: no old ancestor
/// needed.
///
/// 1. Range 0 has 12 invocations, fully compacted (data in a sorted run).
/// 2. Split 0 ⇒ {L, R}; write into L in both of its halves, then split L **at
///    once** — L's data is still in L0 (flushed at quiesce), not yet compacted
///    together with the sorted run inherited from 0. LL, LR stand on {L, 0}
///    (depth 2); R on {0} (depth 1).
/// 3. `ancestor_age` of one year: the old-ancestor rule never fires. SlateDB GC
///    every 5 s (spontaneous detach much slower than the loop's 200 ms beat), a
///    fast compactor.
///
/// Must see: LL and LR start at depth 2; the loop asks for full compactions
/// with reason `depth` (`cairnflow_lineage_compactions_submitted_total{reason="depth"}`
/// grows by ≥ 2 — the counter is process-wide, so the "for which reason" part
/// for **this node** is proven by the next two points); within 60 s LL and LR
/// get to depth 0; R (depth 1, only the old-ancestor rule could compact it)
/// still stands on 0. **Control** with the same scenario and a depth threshold
/// of 3 (the depth rule does not fire at 2): after 15 s (three GC beats) LL and
/// LR still stand on 0 — self-compaction does not bring them to 0, the depth
/// rule does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn depth_rule_alone_compacts_a_child_two_ancestors_deep() -> Result<()> {
    let fired = depth_scenario(2).await?;
    ensure!(fired.0 == (0, 0), "depth rule: (LL, LR) = {:?}", fired.0);
    let control = depth_scenario(3).await?;
    ensure!(
        control.0 .0 >= 1 && control.0 .1 >= 1,
        "control (threshold 3): LL, LR must not get to 0 when the depth rule does not fire: {:?}",
        control.0
    );
    Ok(())
}

/// The scenario of [`depth_rule_alone_compacts_a_child_two_ancestors_deep`] with
/// depth threshold `threshold`. Returns (final depth of (LL, LR), the process's
/// number of depth compactions during the run).
async fn depth_scenario(threshold: usize) -> Result<((usize, usize), u64)> {
    use {cairn_server::lineage::stats, std::sync::atomic::Ordering::Relaxed};
    let mut cfg = common::solo_config(1);
    cfg.stores = cfg
        .stores
        .clone()
        .with_polls(cairn_store::PollIntervals {
            manifest: Duration::from_millis(100),
            compactor: Duration::from_millis(100),
            compaction_worker: Duration::from_millis(100),
            gc: Duration::from_secs(5),
            gc_idle_skip_max: Duration::from_secs(5),
            gc_min_age: Duration::ZERO,
        })
        .with_compactor_checkpoint_lifetime(Duration::from_secs(1));
    cfg.lineage.depth = threshold;
    cfg.lineage.every = Duration::from_millis(200);
    cfg.lineage.retry_after = Duration::from_secs(30);
    cfg.lineage.ancestor_age = Duration::from_secs(365 * 24 * 3600);
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("ok")])
        .spawn()
        .await?;

    let result = common::within(Duration::from_secs(120), "depth rule", async {
        common::register(&url, &sdk.url).await?;
        let mut ids = Vec::new();
        let add = |key: String| {
            let url = url.clone();
            async move {
                let (code, body, _) = invoke_retrying(&url, "Counter", "add", &key).await?;
                ensure!(code == 200, "{key}: {code} {body}");
                Ok::<_, anyhow::Error>((body["id"].as_str().unwrap_or_default().to_string(), "ok"))
            }
        };
        for i in 0..12 {
            ids.push(add(key_in_half("Counter", i % 3 != 2, "root", i)).await?);
        }
        all_completed(&url, &ids, Duration::from_secs(20)).await?;
        compact_fully(&node, &stores, 0).await?;
        let (l, r) = split_range(&url, 0).await?;
        for (i, (lo, hi)) in [(0, MID / 2), (MID / 2, MID)].into_iter().enumerate() {
            for j in 0..2 {
                ids.push(add(key_in_span("Counter", lo, hi, "left", i * 2 + j)).await?);
            }
        }
        let depth_before = stats::SUBMITTED_DEPTH.load(Relaxed);
        let (ll, lr) = split_range(&url, l).await?;
        let depth = |p: u32| {
            let stores = stores.clone();
            async move {
                stores
                    .range_external_depth(PartitionId(p))
                    .await?
                    .ok_or_else(|| anyhow!("Db of range {p} has no manifest yet"))
            }
        };
        let start = (depth(ll).await?, depth(lr).await?, depth(r).await?);
        println!("lineage (threshold {threshold}): right after two splits (LL, LR, R) = {start:?}");
        ensure!(
            start.0 == 2 && start.1 == 2,
            "LL, LR must start at depth 2 (L and 0): {start:?}"
        );
        let t0 = Instant::now();
        // Rule fires: wait for 0 (limit 60 s). Control: watch three GC beats.
        let watch = if threshold <= 2 {
            Duration::from_secs(60)
        } else {
            Duration::from_secs(15)
        };
        let end = loop {
            let d = (depth(ll).await?, depth(lr).await?);
            let fired = stats::SUBMITTED_DEPTH.load(Relaxed) - depth_before;
            if threshold <= 2 && d == (0, 0) && fired >= 2 {
                println!(
                    "lineage (threshold {threshold}): LL, LR stopped standing on ancestors after {:?}, {fired} depth compactions",
                    t0.elapsed()
                );
                break (d, fired);
            }
            if t0.elapsed() >= watch {
                println!("lineage (threshold {threshold}): after {watch:?} (LL, LR) = {d:?}, {fired} depth compactions");
                break (d, fired);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        };
        if threshold <= 2 {
            let body = reqwest::get(format!("{url}/metrics")).await?.text().await?;
            ensure!(
                body.lines().any(|l| l
                    .strip_prefix(
                        "cairnflow_lineage_compactions_submitted_total{reason=\"depth\"} "
                    )
                    .and_then(|v| v.parse::<u64>().ok())
                    .is_some_and(|v| v >= 2)),
                "per-reason metric missing from /metrics"
            );
        }
        ensure!(
            depth(r).await? == 1,
            "R (depth 1, freshly retired ancestor) must not be compacted when the old-ancestor rule is off"
        );
        all_completed(&url, &ids, Duration::from_secs(20)).await?;
        Ok(end)
    })
    .await;
    node.shutdown().await;
    result
}

/// **The stand-alone loop detaches ancestors right after its full compaction,
/// without waiting for SlateDB's one-hour GC beat.** Detaching is a task of
/// SlateDB's GC, run every `polls.gc` (1 h by default) counted from the Db
/// open: before the fix, after the lineage loop's full compaction, the real
/// depth on the bucket only got to 0 on the next GC tick — up to more than 60
/// minutes (measured: ~55 minutes).
///
/// 1. Like [`depth_rule_alone_compacts_a_child_two_ancestors_deep`]: LL, LR
///    stand on {L, 0} (depth 2), the depth rule fires.
/// 2. SlateDB GC of every Db every **1 h** (first tick at Db open, before the
///    compaction): spontaneous GC detaches nothing during the test. The writer
///    polls its manifest every **1 h** as by default: the in-RAM manifest does
///    not see a detach written by someone else.
///
/// Must see: within 60 s LL and LR get to depth 0 on the bucket, and the
/// `cairnflow_range_external_depth` gauge of both ranges follows to 0 (manual
/// control: drop the intersection with the fresh copy in
/// `WriterHealth::external_paths` and the bucket gets to 0 while the gauge stays
/// at 2 for all 60 s).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lineage_detaches_right_after_its_full_compaction() -> Result<()> {
    let mut cfg = common::solo_config(1);
    cfg.stores = cfg
        .stores
        .clone()
        .with_polls(cairn_store::PollIntervals {
            manifest: Duration::from_secs(3600),
            compactor: Duration::from_millis(100),
            compaction_worker: Duration::from_millis(100),
            gc: Duration::from_secs(3600),
            gc_idle_skip_max: Duration::from_secs(3600),
            gc_min_age: Duration::ZERO,
        })
        .with_compactor_checkpoint_lifetime(Duration::from_secs(1));
    cfg.lineage.every = Duration::from_millis(200);
    cfg.lineage.retry_after = Duration::from_secs(600);
    cfg.lineage.ancestor_age = Duration::from_secs(365 * 24 * 3600);
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("ok")])
        .spawn()
        .await?;

    let result = common::within(Duration::from_secs(120), "detach at once", async {
        common::register(&url, &sdk.url).await?;
        let mut ids = Vec::new();
        let add = |key: String| {
            let url = url.clone();
            async move {
                let (code, body, _) = invoke_retrying(&url, "Counter", "add", &key).await?;
                ensure!(code == 200, "{key}: {code} {body}");
                Ok::<_, anyhow::Error>((body["id"].as_str().unwrap_or_default().to_string(), "ok"))
            }
        };
        for i in 0..12 {
            ids.push(add(key_in_half("Counter", i % 3 != 2, "root", i)).await?);
        }
        all_completed(&url, &ids, Duration::from_secs(20)).await?;
        compact_fully(&node, &stores, 0).await?;
        let (l, _r) = split_range(&url, 0).await?;
        for (i, (lo, hi)) in [(0, MID / 2), (MID / 2, MID)].into_iter().enumerate() {
            for j in 0..2 {
                ids.push(add(key_in_span("Counter", lo, hi, "left", i * 2 + j)).await?);
            }
        }
        let (ll, lr) = split_range(&url, l).await?;
        let depth = |p: u32| {
            let stores = stores.clone();
            async move {
                stores
                    .range_external_depth(PartitionId(p))
                    .await?
                    .ok_or_else(|| anyhow!("Db of range {p} has no manifest yet"))
            }
        };
        let start = (depth(ll).await?, depth(lr).await?);
        ensure!(start == (2, 2), "LL, LR must start at depth 2: {start:?}");
        let gauge = |p: u32| {
            let url = url.clone();
            async move {
                common::metric(
                    &url,
                    &format!("cairnflow_range_external_depth{{range=\"{p}\"}}"),
                )
                .await
            }
        };
        let t0 = Instant::now();
        loop {
            let d = (depth(ll).await?, depth(lr).await?);
            let g = (gauge(ll).await?, gauge(lr).await?);
            if d == (0, 0) && g == (0, 0) {
                println!(
                    "detach: LL, LR at depth 0 (bucket and gauge) after {:?}",
                    t0.elapsed()
                );
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(60),
                "after 60 s (SlateDB GC every 1 h): bucket depth (LL, LR) = {d:?}, gauge = {g:?}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        all_completed(&url, &ids, Duration::from_secs(20)).await?;
        Ok(())
    })
    .await;
    node.shutdown().await;
    result
}
