// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A **dead** destination partition must not stall the sender's outbox.
//!
//! A one-hour chaos run lost 20 acknowledged invocations. Two partitions lost their
//! leader for good (a lease torn in MinIO), and from then on **every** partition with
//! an outbox row addressed to them stalled completely: the drainer hit a send error on
//! the first row and abandoned the whole pass, the next pass scanned again from the
//! start and hit exactly that row — the rows behind it, including local rows and rows
//! to healthy partitions, never got their turn.
//!
//! The right promise is: rows to a dead destination **wait** (not lost, not reordered
//! relative to rows for the same destination), rows to other destinations **go on**.
//!
//! **The destination is killed with the network filter** (`--features testkit`), not
//! through the routing table. The old version injected a dead leader into the
//! in-RAM routing table; since single-process mode is a one-node cluster, the table's
//! `follow` loop repairs it by itself — a failed send asks for a refresh, `rebuild`
//! puts the range back on this node — so the "dead" destination came back to life
//! midway depending on timing, and the test was flaky under load. The filter drops
//! every `/internal/inbound?range=1` message while the flag is on: the destination is
//! dead **until the test says otherwise**, and nobody touches the routing table.

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
    cairn_engine::InterPartition,
    cairn_server::node::NodeMode,
    cairn_store::{make_invocation_id, ClusterConfig, PartitionId, PartitionKey},
    common::{
        fake_sdk::{FakeSdk, Step},
        net::{Net, PathFilter},
        TEST_DEADLINE,
    },
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
};

/// A single-process node with `n` ranges, every inter-range message to `dead` dropped
/// while the returned flag is on (the send fails at once, like a port refusing the connection).
async fn node_with_dead_range(
    n: u16,
    dead: u32,
) -> Result<(cairn_server::node::RunningNode, Arc<Net>, Arc<AtomicBool>)> {
    let net = Net::new();
    let cut = Arc::new(AtomicBool::new(false));
    net.add(
        PathFilter::new("/internal/inbound")
            .range(dead)
            .when(cut.clone()),
    );
    let mut cfg = common::test_config(NodeMode::single(n));
    cfg.net_filter = Some(net.clone());
    let node = common::start_led(cfg).await?;
    net.register("solo", &node);
    Ok((node, net, cut))
}

/// A single-process node with `n` ranges, every inter-range message to `range`
/// **held** by the filter (released by hand — `Net::take_held`).
async fn node_holding_range(
    n: u16,
    range: u32,
) -> Result<(cairn_server::node::RunningNode, Arc<Net>)> {
    let net = Net::new();
    net.add(PathFilter::new("/internal/inbound").range(range).hold());
    let mut cfg = common::test_config(NodeMode::single(n));
    cfg.net_filter = Some(net.clone());
    let node = common::start_led(cfg).await?;
    net.register("solo", &node);
    Ok((node, net))
}

/// The inter-range messages a held request carries, in order: one message
/// (`/internal/inbound`) or a batch (`/internal/inbound/batch`).
fn held_messages(h: &cairn_server::netfilter::Held) -> Result<Vec<InterPartition>> {
    let body = h
        .body()
        .ok_or_else(|| anyhow::anyhow!("held message has no body: {h:?}"))?;
    let body = bytes::Bytes::copy_from_slice(body);
    let path = h.path().split('?').next().unwrap_or_default();
    let decode = |b: bytes::Bytes| {
        InterPartition::decode(b)
            .map_err(|e| anyhow::anyhow!("held message body does not decode: {e}"))
    };
    if !path.ends_with("/internal/inbound/batch") {
        return Ok(vec![decode(body)?]);
    }
    // The batch frame read by hand per the wire contract, not through the code under
    // test: a `u32` message count, then per message a `u32` length + body (big-endian).
    let mut b = body;
    let u32_of = |b: &mut bytes::Bytes| -> Result<usize> {
        ensure!(b.len() >= 4, "truncated batch frame");
        let n = u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize;
        let _ = b.split_to(4);
        Ok(n)
    };
    let count = u32_of(&mut b)?;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let len = u32_of(&mut b)?;
        ensure!(b.len() >= len, "truncated batch frame");
        out.push(decode(b.split_to(len))?);
    }
    ensure!(b.is_empty(), "batch frame has extra bytes");
    Ok(out)
}

/// The only inter-range message a held request carries.
fn held_message(h: &cairn_server::netfilter::Held) -> Result<InterPartition> {
    let mut msgs = held_messages(h)?;
    ensure!(
        msgs.len() == 1,
        "held request carries {} messages, not one",
        msgs.len()
    );
    Ok(msgs.remove(0))
}

/// The object keys of the `StartCall` calls a held request carries, in their order in
/// the request.
fn held_start_call_keys(h: &cairn_server::netfilter::Held) -> Result<Vec<String>> {
    held_messages(h)?
        .into_iter()
        .map(|m| match m {
            InterPartition::StartCall(s) => Ok(s.object_key),
            other => Err(anyhow::anyhow!(
                "held message is not a StartCall: {other:?}"
            )),
        })
        .collect()
}

/// The object key of the only `StartCall` call a held request carries.
fn held_start_call_key(h: &cairn_server::netfilter::Held) -> Result<String> {
    let mut keys = held_start_call_keys(h)?;
    ensure!(
        keys.len() == 1,
        "held request carries {keys:?}, not one call"
    );
    Ok(keys.remove(0))
}

/// Take out exactly one held message — the `nth` one (counting from 1, total since
/// the start of the test) — once it has arrived.
async fn take_one_held(net: &Net, nth: usize) -> Result<cairn_server::netfilter::Held> {
    net.wait_held(nth, Duration::from_secs(20)).await?;
    let mut held = net.take_held();
    ensure!(
        held.len() == 1,
        "exactly one held message at a time (each destination sends sequentially): {held:?} {:?}",
        held.iter().map(held_messages).collect::<Vec<_>>()
    );
    Ok(held.remove(0))
}

/// The number of call ids (`CallInvocationIdCompletion`, `0x800E`) delivered into the
/// journal of `id`.
async fn delivered_call_ids(url: &str, id: &str) -> Result<usize> {
    let journal: serde_json::Value = reqwest::get(format!("{url}/invocations/{id}/journal"))
        .await?
        .json()
        .await?;
    Ok(journal
        .as_array()
        .map(|rows| rows.iter().filter(|r| r["ty"] == "0x800E").count())
        .unwrap_or(0))
}

/// An object key of `service` landing in `partition` in an evenly split table of `n`.
fn key_on(n: u16, service: &str, partition: u32, prefix: &str) -> String {
    let cluster = ClusterConfig::equal(n);
    (0..10_000)
        .map(|i| format!("{prefix}-{i}"))
        .find(|k| {
            cluster
                .partition_of(&cairn_store::TenantId::default(), service, k)
                .0
                == partition
        })
        .expect("found a key")
}

/// Two callers, both on partition 0. The first calls an object on partition 1 — a
/// partition the routing table points at a dead address. The second calls an object
/// on partition 2, healthy. The second must finish; the first must finish once the
/// path to partition 1 comes back to life.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dead_destination_does_not_stall_other_outbox_rows() -> Result<()> {
    const N: u16 = 4;
    let dead_key = key_on(N, "Echo", 1, "dead");
    let live_key = key_on(N, "Echo", 2, "live");
    let first = key_on(N, "Caller", 0, "first");
    let second = key_on(N, "Caller", 0, "second");

    let sdk = FakeSdk::builder()
        .object(
            "Caller",
            "toDead",
            vec![
                Step::call("Echo", "ping", Some(&dead_key)),
                Step::output("a"),
            ],
        )
        .object(
            "Caller",
            "toLive",
            vec![
                Step::call("Echo", "ping", Some(&live_key)),
                Step::output("b"),
            ],
        )
        .object("Echo", "ping", vec![Step::output("pong")])
        .spawn()
        .await?;
    let (node, net, cut) = node_with_dead_range(N, 1).await?;
    let url = node.url();

    let r = common::within(TEST_DEADLINE, "dead destination, live outbox", async {
        common::register(&url, &sdk.url).await?;
        cut.store(true, Ordering::SeqCst);

        // The first caller: a Call row to partition 1 sits in partition 0's outbox.
        let (code, a) = common::send(&url, "Caller", "toDead", Some(&first), "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {a}");
        let a = a["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &a, "suspended").await?;

        // The second caller: its Call row stands **behind** the stuck row above.
        let (code, b) = common::send(&url, "Caller", "toLive", Some(&second), "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {b}");
        let b = b["id"].as_str().unwrap_or_default().to_string();
        tokio::time::timeout(
            Duration::from_secs(10),
            common::wait_status(&url, &b, "completed"),
        )
        .await
        .map_err(|_| anyhow::anyhow!("a call to a healthy partition queued behind a dead one"))??;

        // The stuck row is not lost: once the path comes back the first caller finishes.
        let (_, st) = common::invocation(&url, &a).await?;
        ensure!(
            st["status"] == "suspended",
            "the first caller must still be waiting: {st}"
        );
        // The destination really was dead to the sender (not merely not its turn yet).
        ensure!(!net.dropped().is_empty(), "no message to range 1 dropped");
        // … and `/metrics` counts that send as `unreachable`. The counter is
        // process-wide, other tests only add to it: `≥ 1` is the right check under
        // both `cargo test` and nextest.
        let unreachable = common::metric(
            &url,
            "cairnflow_interrange_messages_total{kind=\"start_call\",outcome=\"unreachable\"}",
        )
        .await?;
        ensure!(
            unreachable >= 1,
            "sends to a dead destination not counted: {unreachable}"
        );
        cut.store(false, Ordering::SeqCst);
        common::wait_status(&url, &a, "completed").await?;
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// The same promise for **signals**: ≥ 64 signal rows to a dead partition standing in
/// front of a row to a healthy partition — that row must still go.
///
/// Formerly `drain_signals` read a fixed window of 64 rows from the head of the table
/// on every tick. 64 stuck rows filled the window, so the 65th row was never seen —
/// the same head-of-line disease already cured for the outbox.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signal_rows_to_dead_destination_do_not_block_others() -> Result<()> {
    const N: u16 = 4;
    const STUCK: usize = 64;
    let cluster = ClusterConfig::equal(N);
    // The fake destination on partition 1: it need not even exist — the send fails before
    // it gets there.
    let dead_ids: Vec<String> = (0..=u8::MAX as u64)
        .map(|i| PartitionKey(i << 56))
        .filter(|pk| cluster.partition_of_key(*pk) == PartitionId(1))
        .flat_map(|pk| (0..STUCK as u8).map(move |n| make_invocation_id(pk, [n; 16])))
        .take(STUCK)
        .map(|id| id.iter().map(|b| format!("{b:02x}")).collect())
        .collect();
    ensure!(dead_ids.len() == STUCK);
    let live = Arc::new(Mutex::new(String::new()));
    let mut steps: Vec<Step> = dead_ids
        .iter()
        .map(|id| Step::SendSignal(Arc::new(Mutex::new(id.clone()))))
        .collect();
    steps.push(Step::SendSignal(live.clone()));
    steps.push(Step::output("sent"));

    let waiter_key = key_on(N, "Waiter", 2, "live");
    let sender_key = key_on(N, "Sender", 0, "sender");
    let sdk = FakeSdk::builder()
        .object("Waiter", "hold", vec![Step::Suspend])
        .object("Sender", "fire", steps)
        .spawn()
        .await?;
    let (node, net, cut) = node_with_dead_range(N, 1).await?;
    let url = node.url();

    let r = common::within(
        TEST_DEADLINE,
        "a signal to a dead destination does not block other signals",
        async {
            common::register(&url, &sdk.url).await?;
            let (code, w) = common::send(&url, "Waiter", "hold", Some(&waiter_key), "{}").await?;
            ensure!(code == 200 || code == 202, "{code} {w}");
            let waiter = w["id"].as_str().unwrap_or_default().to_string();
            common::wait_status(&url, &waiter, "suspended").await?;
            *live.lock().unwrap() = waiter.clone();
            let woke_before = sdk.starts_of("Waiter", "hold").len();

            cut.store(true, Ordering::SeqCst);
            let (code, s) = common::send(&url, "Sender", "fire", Some(&sender_key), "{}").await?;
            ensure!(code == 200 || code == 202, "{code} {s}");
            let sender = s["id"].as_str().unwrap_or_default().to_string();
            common::wait_status(&url, &sender, "completed").await?;

            // The 65th signal, to a healthy partition, must wake the waiter.
            tokio::time::timeout(Duration::from_secs(10), async {
                while sdk.starts_of("Waiter", "hold").len() <= woke_before {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .map_err(|_| {
                anyhow::anyhow!("a signal to a healthy partition stood behind {STUCK} rows to a dead destination")
            })?;
            ensure!(!net.dropped().is_empty(), "no message to range 1 dropped");
            cut.store(false, Ordering::SeqCst);
            Ok(())
        },
    )
    .await;
    node.shutdown().await;
    r
}

/// **A hung destination does not hold up other destinations.**
///
/// Interleaving:
/// 1. `Caller` (range 0) issues in **one** turn 4 calls to object `Obj` on range 1,
///    then 4 calls to `Obj` on range 2 (`Step::fire`, not awaited), then suspends.
///    The drainer gathers all 8 rows in one pass.
/// 2. The filter **holds** every message to range 1: the first `StartCall` message
///    there stands waiting, its sender still waiting for the answer.
/// 3. The test cuts in **while** that message is still held.
///
/// Must see, while the message to range 1 is still held: the 4 callees on range 2
/// have run and the caller has received 4 ids (4 `0x800E` notifications in its
/// journal) — a finished destination is applied first, the total time is ≈ the max
/// of the two destinations rather than their sum; and exactly **one** request to
/// range 1 is being held (each destination sends sequentially, IR-3) — a batch
/// carrying all 4 calls, in order. Released, all 8 calls finish. Without
/// per-destination sending: the drainer sent row after row, so the first row (range
/// 1) held up every later row until `SEND_TIMEOUT` (10 s) ran out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hung_destination_does_not_hold_other_destinations() -> Result<()> {
    const N: u16 = 4;
    const PER_RANGE: usize = 4;
    let caller_key = key_on(N, "Caller", 0, "caller");
    let keys = |range: u32| -> Vec<String> {
        (0..PER_RANGE)
            .map(|i| key_on(N, "Obj", range, &format!("r{range}-{i}")))
            .collect()
    };
    let (slow, fast) = (keys(1), keys(2));
    let mut steps: Vec<Step> = slow
        .iter()
        .chain(&fast)
        .map(|k| Step::fire("Obj", "hold", Some(k)))
        .collect();
    steps.push(Step::Suspend);
    let sdk = FakeSdk::builder()
        .object("Caller", "fan", steps)
        .object("Obj", "hold", vec![Step::Suspend])
        .spawn()
        .await?;
    let (node, net) = node_holding_range(N, 1).await?;
    let url = node.url();

    let r = common::within(TEST_DEADLINE, "hung destination, others go", async {
        common::register(&url, &sdk.url).await?;
        let (code, c) = common::send(&url, "Caller", "fan", Some(&caller_key), "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {c}");
        let caller = c["id"].as_str().unwrap_or_default().to_string();

        net.wait_held(1, Duration::from_secs(20)).await?;
        // Well below `SEND_TIMEOUT` (10 s): the old path only moved on after the held
        // send had run out of time.
        let fast_done = tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                let started = sdk
                    .starts_of("Obj", "hold")
                    .iter()
                    .filter(|s| fast.contains(&s.key))
                    .count();
                if started == PER_RANGE && delivered_call_ids(&url, &caller).await? == PER_RANGE {
                    return anyhow::Ok(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        ensure!(
            matches!(fast_done, Ok(Ok(()))),
            "calls to range 2 stood behind a held message to range 1: {fast_done:?}"
        );
        let held = net.take_held();
        ensure!(
            held.len() == 1 && held[0].sender_waiting(),
            "exactly one request to range 1 held, its sender still waiting: {held:?}"
        );
        // The pass's four calls to range 1 travel in one batch, in order.
        ensure!(
            held_start_call_keys(&held[0])? == slow,
            "the held request must be the batch of four calls to range 1, in order: {:?}",
            held_start_call_keys(&held[0])
        );

        net.heal();
        for h in held {
            let status = h.release().await;
            ensure!(status == Ok(200), "release held message: {status:?}");
        }
        while delivered_call_ids(&url, &caller).await? < 2 * PER_RANGE {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        while sdk.starts_of("Obj", "hold").len() < 2 * PER_RANGE {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **A failed send leaves the failed row and every later row for the same destination
/// in place, order kept (IR-3); other destinations still go (IR-4).**
///
/// Interleaving:
/// 1. `Caller/slow` (range 0) issues in one turn 3 calls to range 1 (`a`, `b`, `c`);
///    then `Caller/fast` (range 0, another caller) issues 2 calls to range 2. The
///    filter holds every message to range 1.
/// 2. The first held request to range 1 must be the **batch** `[a, b, c]` (one pass,
///    one destination, one batch): **drop** it (the sender gets a network error) — the
///    destination has failed.
/// 3. The next drain pass resends.
///
/// Must see: the next held request is the batch `[a, b, c]` again, **in the same
/// order** — a failed batch lets none of its rows be deleted or go ahead of another;
/// the two calls to range 2 finish while the batch is still held, and the slow caller
/// has received no id yet (a batch is all or nothing); released, all 5 finish. Before
/// batching this test checked the same rule per message (`a` acked, `b` failed, `b`
/// resent before `c`); the order between the **requests** of one destination (the
/// next batch only goes once the previous one is back) is `Sender` and
/// `Transport::send_batch` — `transport.rs::tests`.
///
/// Two callers, not one: in an earlier design every row of **one** caller was tied to
/// the others through its lock — once id `a` was delivered the caller's turn grabbed
/// the lock, and its later rows were skipped (`busy`) until the next drain pass, rows
/// to other destinations included. `hung_destination_does_not_hold_other_destinations`
/// checks the one-caller case under the current design.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_send_keeps_later_rows_to_the_same_destination_in_order() -> Result<()> {
    const N: u16 = 4;
    let slow_caller = key_on(N, "Caller", 0, "slow");
    let fast_caller = key_on(N, "Caller", 0, "fast");
    let slow: Vec<String> = ["a", "b", "c"]
        .iter()
        .map(|p| key_on(N, "Obj", 1, p))
        .collect();
    let fast: Vec<String> = ["x", "y"].iter().map(|p| key_on(N, "Obj", 2, p)).collect();
    let fire = |keys: &[String]| -> Vec<Step> {
        let mut steps: Vec<Step> = keys
            .iter()
            .map(|k| Step::fire("Obj", "hold", Some(k)))
            .collect();
        steps.push(Step::Suspend);
        steps
    };
    let sdk = FakeSdk::builder()
        .object("Caller", "slow", fire(&slow))
        .object("Caller", "fast", fire(&fast))
        .object("Obj", "hold", vec![Step::Suspend])
        .spawn()
        .await?;
    let (node, net) = node_holding_range(N, 1).await?;
    let url = node.url();

    let r = common::within(TEST_DEADLINE, "order kept across a failure", async {
        common::register(&url, &sdk.url).await?;
        let mut callers = Vec::new();
        for (handler, key) in [("slow", &slow_caller), ("fast", &fast_caller)] {
            let (code, c) = common::send(&url, "Caller", handler, Some(key), "{}").await?;
            ensure!(code == 200 || code == 202, "{code} {c}");
            let id = c["id"].as_str().unwrap_or_default().to_string();
            common::wait_status(&url, &id, "suspended").await?;
            callers.push(id);
        }
        let (slow_id, fast_id) = (&callers[0], &callers[1]);

        let first = take_one_held(&net, 1).await?;
        ensure!(
            held_start_call_keys(&first)? == slow,
            "the first request to range 1 must be the batch [a, b, c]: {:?}",
            held_start_call_keys(&first)
        );
        // Drop without releasing: the sender gets a network error — the destination has failed.
        drop(first);

        let second = take_one_held(&net, 2).await?;
        ensure!(
            held_start_call_keys(&second)? == slow,
            "after the failure, the resent batch must be [a, b, c] in order: {:?}",
            held_start_call_keys(&second)
        );
        // The two calls to range 2 do not wait for range 1 (the batch still held, its sender
        // still waiting).
        while delivered_call_ids(&url, fast_id).await? < fast.len() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        ensure!(second.sender_waiting(), "batch held while range 2 done");
        ensure!(
            delivered_call_ids(&url, slow_id).await? == 0,
            "a failed batch delivers no id to the slow caller"
        );
        let status = second.release().await;
        ensure!(status == Ok(200), "release batch: {status:?}");

        while delivered_call_ids(&url, slow_id).await? < slow.len() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **A `Cancel` to a callee goes **after** its `StartCall` (IR-3).**
///
/// A `Cancel` row for callee `c` is only produced once the caller has `c`'s id
/// (`Engine::live_children` reads the `0x800E` notification), and that id is only
/// delivered after the receiver acks the `StartCall` — so the two rows are never in
/// the outbox together. What keeps the order is **the caller's invocation lock**: the
/// drainer holds it across the send, and `/cancel` needs exactly that lock.
///
/// Interleaving:
/// 1. `P` (range 0) `ctx.call`s object `c` on range 1, then waits. The filter holds
///    every message to range 1: `StartCall(c)` stands waiting.
/// 2. The test sends `/invocations/P/cancel` while `StartCall(c)` is still held.
/// 3. Release `StartCall(c)`.
///
/// Must see: the cancel does **not** finish while `StartCall(c)` is still held (it
/// waits for `P`'s lock); once released `P` is cancelled, the next message to range 1
/// is `Cancel(c)`, and `c` is cancelled along with it. If the drainer released the
/// caller's lock during the send, the cancel would run before `P` had `c`'s id, no
/// `Cancel` row would be produced, and `c` would run orphaned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_reaches_the_callee_after_its_start_call() -> Result<()> {
    const N: u16 = 4;
    let parent_key = key_on(N, "Parent", 0, "parent");
    let child_key = key_on(N, "Obj", 1, "child");
    let sdk = FakeSdk::builder()
        .object(
            "Parent",
            "run",
            vec![
                Step::call("Obj", "hold", Some(&child_key)),
                Step::output("done"),
            ],
        )
        .object("Obj", "hold", vec![Step::Suspend])
        .spawn()
        .await?;
    let (node, net) = node_holding_range(N, 1).await?;
    let url = node.url();

    let r = common::within(TEST_DEADLINE, "Cancel goes after StartCall", async {
        common::register(&url, &sdk.url).await?;
        let (code, p) = common::send(&url, "Parent", "run", Some(&parent_key), "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {p}");
        let parent = p["id"].as_str().unwrap_or_default().to_string();

        let start = take_one_held(&net, 1).await?;
        ensure!(
            held_start_call_key(&start)? == child_key,
            "the first message must be StartCall(c)"
        );

        let mut cancel = tokio::spawn({
            let url = url.clone();
            let parent = parent.clone();
            async move {
                let resp = reqwest::Client::new()
                    .post(format!("{url}/invocations/{parent}/cancel"))
                    .send()
                    .await?;
                anyhow::Ok(resp.json::<serde_json::Value>().await?)
            }
        });
        // For a wide interval, the cancel must still be waiting for `P`'s lock.
        ensure!(
            tokio::time::timeout(Duration::from_millis(500), &mut cancel)
                .await
                .is_err(),
            "the cancel finished while StartCall(c) was still held — the drainer released the caller's lock"
        );
        let (_, st) = common::invocation(&url, &parent).await?;
        ensure!(
            st["status"] != "failed",
            "P cancelled while StartCall(c) was still held — the drainer released the caller's lock: {st}"
        );
        let status = start.release().await;
        ensure!(status == Ok(200), "release StartCall(c): {status:?}");
        // `/cancel` answers after the drainer has drained the outbox rows it produced
        // (`sync_outbox`), so release `Cancel(c)` before waiting for the answer.
        let next = take_one_held(&net, 2).await?;
        let child = match held_message(&next)? {
            InterPartition::Cancel(c) => c.target,
            other => anyhow::bail!("the next message to range 1 must be Cancel(c): {other:?}"),
        };
        let status = next.release().await;
        ensure!(status == Ok(200), "release Cancel(c): {status:?}");
        let answer = cancel.await??;
        ensure!(answer["cancelled"] == true, "cancel P: {answer}");
        let child: String = child.iter().map(|b| format!("{b:02x}")).collect();
        common::wait_status(&url, &child, "failed").await?;
        common::wait_status(&url, &parent, "failed").await?;
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **A batch over the cap is split into several consecutive batches to the same
/// destination; a failed batch keeps its rows, batches acked before it are done
/// (IR-3).**
///
/// Interleaving:
/// 1. `Caller` (range 0) issues in one turn 130 calls to 130 objects on range 1
///    (`k0` … `k129`), then suspends. The filter holds every request to range 1.
/// 2. The drainer reads 32 rows per pass: the first pass hands `k0..k32` to range 1's
///    send task — the first request is held. Meanwhile later passes gather the rest
///    into the same destination's queue (not handed over yet: the destination is in
///    flight).
/// 3. The test releases request after request; the next task takes the whole queue
///    and cuts it at the 64-message cap, sending the next batch **only once the
///    previous batch has been acked**. The exact boundaries depend on when the SDK
///    commits rows, so the test checks invariants, not boundaries.
/// 4. Right after releasing a full batch (64 messages), the test **drops** the next
///    request (the sender gets a network error). The next drain pass resends.
///
/// Must see: exactly **one** request to range 1 held at any time; each request has
/// ≤ 64 messages and carries exactly the keys **right after** the request acked before
/// it (nothing skipped, repeated or reordered); at least one full batch; at the drop,
/// the caller has received exactly as many ids as the acked batches carried; the
/// request after the drop restarts exactly at the first key of the dropped batch;
/// releasing everything finishes all 130. Before batching: one request per call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_queue_goes_as_consecutive_batches_and_a_failed_one_keeps_its_rows() -> Result<()>
{
    const N: u16 = 4;
    const CALLS: usize = 130;
    const CAP: usize = 64;
    let caller_key = key_on(N, "Caller", 0, "caller");
    let keys: Vec<String> = (0..CALLS)
        .map(|i| key_on(N, "Obj", 1, &format!("k{i}")))
        .collect();
    let mut steps: Vec<Step> = keys
        .iter()
        .map(|k| Step::fire("Obj", "hold", Some(k)))
        .collect();
    steps.push(Step::Suspend);
    let sdk = FakeSdk::builder()
        .object("Caller", "fan", steps)
        .object("Obj", "hold", vec![Step::Suspend])
        .spawn()
        .await?;
    let (node, net) = node_holding_range(N, 1).await?;
    let url = node.url();

    let r = common::within(TEST_DEADLINE, "chained batches to one target", async {
        common::register(&url, &sdk.url).await?;
        let (code, c) = common::send(&url, "Caller", "fan", Some(&caller_key), "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {c}");
        let caller = c["id"].as_str().unwrap_or_default().to_string();
        let wait_delivered = |n: usize| {
            let (url, caller) = (url.clone(), caller.clone());
            async move {
                while delivered_call_ids(&url, &caller).await? < n {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                anyhow::Ok(())
            }
        };

        // `next`: the first key not yet acked. `full`: a full batch has been released.
        let (mut next, mut nth, mut full, mut dropped) = (0usize, 0usize, false, false);
        while next < CALLS {
            nth += 1;
            let held = take_one_held(&net, nth).await?;
            let got = held_start_call_keys(&held)?;
            ensure!(
                !got.is_empty()
                    && got.len() <= CAP
                    && next + got.len() <= CALLS
                    && got[..] == keys[next..next + got.len()],
                "request {nth} must be ≤ {CAP} keys right after k{next}: {} keys, first {:?}",
                got.len(),
                got.first()
            );
            if full && !dropped {
                let delivered = delivered_call_ids(&url, &caller).await?;
                ensure!(
                    delivered == next,
                    "before the drop, the caller must have exactly {next} ids: {delivered}"
                );
                // Drop without releasing: the sender gets a network error. `next` stays:
                // the next request must restart from here.
                drop(held);
                dropped = true;
                continue;
            }
            full |= got.len() == CAP;
            ensure!(held.release().await == Ok(200), "release request {nth}");
            next += got.len();
            wait_delivered(next).await?;
        }
        ensure!(full, "no batch reached the {CAP}-message cap");
        ensure!(dropped, "no request after a full batch to drop");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// Wait until the sender of held request `h` stops waiting (its send ran out of time);
/// returns the time waited since `since`.
async fn sender_gave_up(
    h: &cairn_server::netfilter::Held,
    since: std::time::Instant,
) -> Result<Duration> {
    tokio::time::timeout(Duration::from_secs(20), async {
        while h.sender_waiting() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("the held request's sender waited over 20 s"))?;
    Ok(since.elapsed())
}

/// **A hung destination does not hold up the sending range's inbound; the
/// per-destination circuit breaker survives across drain passes.**
///
/// Interleaving:
/// 1. `Caller` (range 0) issues one call to `Obj` on range 1, then suspends. The
///    filter **holds** every request to range 1: the first `StartCall` batch stands
///    until `SEND_TIMEOUT` (10 s) runs out — the test keeps that request (neither
///    released nor dropped) and waits for the sender to time out.
/// 2. Range 0's next drain pass (the background tick nudges every 50 ms) resends to
///    range 1: a second request is held. The test cuts in **as soon as** it arrives:
///    it sends `Far/go` (range 2), whose turn issues a call to `Near` on range 0 — a
///    `StartCall` message into the inbound of the very range sending to the hung
///    destination.
/// 3. The test measures when `Near` runs, and when the second request's sender stops
///    waiting.
/// 4. Release the network.
///
/// Must see: `Near` runs within `SEND_TIMEOUT / 2` of sending `Far` — range 0's inbound
/// does not stand behind the send to range 1 (the order within a pass: inbound →
/// signals → outbox); the second request is the circuit breaker's **probe batch** —
/// its sender gives up after `PROBE_TIMEOUT` (2 s) rather than `SEND_TIMEOUT`; once the
/// network is released the call to range 1 goes (the probe batch is acked, the breaker
/// clears). Before the breaker: the drainer did the outbox first, and the `down` set
/// lived for only one pass — every pass handed range 1 another 10 s, and range 0's
/// inbound waited along.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hung_destination_does_not_hold_the_senders_inbound() -> Result<()> {
    use cairn_server::transport::{PROBE_TIMEOUT, SEND_TIMEOUT};
    const N: u16 = 4;
    let caller_key = key_on(N, "Caller", 0, "caller");
    let dead_key = key_on(N, "Obj", 1, "dead");
    let far_key = key_on(N, "Far", 2, "far");
    let near_key = key_on(N, "Near", 0, "near");
    let sdk = FakeSdk::builder()
        .object(
            "Caller",
            "fan",
            vec![Step::fire("Obj", "hold", Some(&dead_key)), Step::Suspend],
        )
        .object("Obj", "hold", vec![Step::Suspend])
        .object(
            "Far",
            "go",
            // Suspend, never finish: a call from a caller that has stopped for good is
            // dropped at the drainer (`DropStale`).
            vec![Step::fire("Near", "run", Some(&near_key)), Step::Suspend],
        )
        .object("Near", "run", vec![Step::output("ok")])
        .spawn()
        .await?;
    let (node, net) = node_holding_range(N, 1).await?;
    let url = node.url();

    let r = common::within(
        TEST_DEADLINE + Duration::from_secs(30),
        "the sending range's inbound does not wait for a hung destination",
        async {
            common::register(&url, &sdk.url).await?;
            let (code, c) = common::send(&url, "Caller", "fan", Some(&caller_key), "{}").await?;
            ensure!(code == 200 || code == 202, "{code} {c}");

            // 1. The first send to range 1 runs out of time (request kept, not released).
            net.wait_held(1, Duration::from_secs(20)).await?;
            let first_at = std::time::Instant::now();
            let mut kept = net.take_held();
            ensure!(kept.len() == 1, "exactly one request to range 1: {kept:?}");
            let first = sender_gave_up(&kept[0], first_at).await?;
            ensure!(
                first + Duration::from_secs(1) >= SEND_TIMEOUT,
                "the first send to a hung destination must wait the whole SEND_TIMEOUT: {first:?}"
            );

            // 2. The next request to range 1 has just arrived: cut in with an inbound message
            // for range 0.
            net.wait_held(2, Duration::from_secs(20)).await?;
            let probe_at = std::time::Instant::now();
            let mut next = net.take_held();
            ensure!(next.len() == 1, "exactly one request to range 1: {next:?}");
            ensure!(
                held_start_call_keys(&next[0])? == [dead_key.clone()],
                "the next request to range 1 must be the batch of that same row: {:?}",
                held_start_call_keys(&next[0])
            );
            let sent = std::time::Instant::now();
            let (code, f) = common::send(&url, "Far", "go", Some(&far_key), "{}").await?;
            ensure!(code == 200 || code == 202, "{code} {f}");
            tokio::time::timeout(Duration::from_secs(30), async {
                while sdk.starts_of("Near", "run").is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .map_err(|_| anyhow::anyhow!("Near did not run within 30 s"))?;
            let inbound = sent.elapsed();
            let probe = sender_gave_up(&next[0], probe_at).await?;
            eprintln!(
                "first send {first:?}, next request {probe:?}, sending range's inbound {inbound:?}"
            );
            ensure!(
                inbound < SEND_TIMEOUT / 2,
                "range 0's inbound waited {inbound:?} after the send to a hung destination (SEND_TIMEOUT {SEND_TIMEOUT:?})"
            );
            ensure!(
                probe < PROBE_TIMEOUT + Duration::from_millis(1_500),
                "the next request to a destination that timed out must be a probe batch (PROBE_TIMEOUT {PROBE_TIMEOUT:?}): the sender waited {probe:?}"
            );

            // 4. Release the network: the next probe batch is acked, the call to range 1 goes.
            kept.append(&mut next);
            net.heal();
            for h in kept {
                let _ = h.release().await;
            }
            tokio::time::timeout(Duration::from_secs(20), async {
                while sdk.starts_of("Obj", "hold").is_empty() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .map_err(|_| anyhow::anyhow!("the call to range 1 did not go after the network was released"))?;
            Ok(())
        },
    )
    .await;
    node.shutdown().await;
    r
}
