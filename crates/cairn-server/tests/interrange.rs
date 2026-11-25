// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Cross-range calls: the metrics `cairnflow_calls_total`,
//! `cairnflow_interrange_messages_total` and the drainer's sync/send histograms.
//!
//! **The counters are process-wide** (`metrics::interrange`, like `metrics::sdk`),
//! so the tests read the **difference** before/after on `/metrics` and hold [`SERIAL`]
//! through the counting part: under `cargo test` the tests of this file share one
//! process. **Every** test of the file holds it — including tests that do not count,
//! since they produce messages into the same counters. Nextest runs each test in its
//! own process, so nothing more is needed there.

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
        fake_sdk::{FakeSdk, Step},
        TEST_DEADLINE,
    },
    std::{collections::BTreeMap, time::Duration},
};

/// Lock over the counting part of every test in the file — see the file's top doc.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Every sample of `cairnflow_calls_total`, `cairnflow_interrange_messages_total` and the
/// `_count` of the two sync/send histograms, keyed by the verbatim line before the value.
async fn samples(url: &str) -> Result<BTreeMap<String, u64>> {
    let text = reqwest::get(format!("{url}/metrics")).await?.text().await?;
    Ok(text
        .lines()
        .filter(|l| {
            l.starts_with("cairnflow_calls_total{")
                || l.starts_with("cairnflow_interrange_messages_total{")
                || l.starts_with("cairnflow_interrange_sync_seconds_count ")
                || l.starts_with("cairnflow_interrange_send_seconds_count ")
                || l.starts_with("cairnflow_interrange_batch_messages_count ")
                || l.starts_with("cairnflow_interrange_batch_messages_sum ")
        })
        .filter_map(|l| {
            let (name, v) = l.rsplit_once(' ')?;
            Some((name.to_string(), v.parse().ok()?))
        })
        .collect())
}

/// The outbox drainer counters (`cairnflow_outbox_*_total`).
async fn drain_counters(url: &str) -> Result<BTreeMap<String, u64>> {
    let text = reqwest::get(format!("{url}/metrics")).await?.text().await?;
    Ok(text
        .lines()
        .filter(|l| l.starts_with("cairnflow_outbox_"))
        .filter_map(|l| {
            let (name, v) = l.rsplit_once(' ')?;
            Some((name.to_string(), v.parse().ok()?))
        })
        .collect())
}

fn calls(placement: &str, class: &str) -> String {
    format!("cairnflow_calls_total{{placement=\"{placement}\",key_class=\"{class}\"}}")
}

fn messages(kind: &str, outcome: &str) -> String {
    format!("cairnflow_interrange_messages_total{{kind=\"{kind}\",outcome=\"{outcome}\"}}")
}

/// A parent on range 0 calls, one after another, 12 unkeyed callees, one object on the
/// same range, one object on range 2 and one callee with an idempotency key, on a
/// 4-range node (cross-range calls go over the real HTTP transport).
///
/// Where the unkeyed and idempotent callees land is decided by the hash,
/// so the test **computes** it itself from each callee's id (`/invocations/{id}`) and
/// compares with the counter differences. Must see:
/// - `cairnflow_calls_total{placement, key_class}` grows by exactly the call count of each
///   cell, and nothing in the `scoped`/`workflow` cells;
/// - every remote call has exactly one `start_call` `ok` and one `completion` `ok`, no
///   cross-range message fails;
/// - every message is one drainer `sync` and one `send`, and one single-message commit
///   at the receiver (`cairnflow_interrange_batch_messages`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calls_are_counted_by_placement_and_key_class() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let node = common::start_single_process(4).await?;
    let root_key = common::key_on_partition(&node, "Root", 0, "root");
    let near = common::key_on_partition(&node, "Obj", 0, "near");
    let far = common::key_on_partition(&node, "Obj", 2, "far");
    const UNKEYED: usize = 12;
    let mut steps: Vec<Step> = (0..UNKEYED)
        .map(|_| Step::call("Leaf", "echo", None))
        .collect();
    steps.push(Step::call("Obj", "echo", Some(&near)));
    steps.push(Step::call("Obj", "echo", Some(&far)));
    steps.push(Step::Call {
        service: "Idem".into(),
        handler: "echo".into(),
        key: None,
        idempotency_key: Some("interrange-0.1".into()),
        spread: false,
    });
    steps.push(Step::output("done"));
    let sdk = FakeSdk::builder()
        .object("Root", "run", steps)
        .service("Leaf", "echo", vec![Step::output("la")])
        .object("Obj", "echo", vec![Step::output("vat")])
        .service("Idem", "echo", vec![Step::output("once")])
        .spawn()
        .await?;
    let url = node.url();

    let result = common::within(TEST_DEADLINE, "count calls by placement", async {
        common::register(&url, &sdk.url).await?;
        let before = samples(&url).await?;
        let (code, body) = common::invoke(&url, "Root", "run", Some(&root_key), "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &id, "completed").await?;
        let home = common::partition_of(&url, &id).await?;
        ensure!(home == 0, "the parent must be on range 0, it is on {home}");

        // The real placement of each callee, by key class.
        let mut want: BTreeMap<String, u64> = BTreeMap::new();
        let mut remote = 0u64;
        for (service, class, n) in [
            ("Leaf", "unkeyed", UNKEYED),
            ("Obj", "object", 2),
            ("Idem", "idempotent", 1),
        ] {
            let starts = sdk.starts_of(service, "echo");
            ensure!(starts.len() == n, "{service}: {starts:?}");
            for s in starts {
                let p = common::partition_of(&url, &s.id_hex()).await?;
                let placement = if p == home { "local" } else { "remote" };
                remote += u64::from(p != home);
                *want.entry(calls(placement, class)).or_default() += 1;
            }
        }
        ensure!(
            want.get(&calls("local", "object")) == Some(&1)
                && want.get(&calls("remote", "object")) == Some(&1),
            "object on the same range and on another range: {want:?}"
        );
        want.insert(messages("start_call", "ok"), remote);
        want.insert(messages("completion", "ok"), remote);
        want.insert("cairnflow_interrange_sync_seconds_count".into(), 2 * remote);
        want.insert("cairnflow_interrange_send_seconds_count".into(), 2 * remote);
        // Sequential calls: each message is a one-message batch, one commit at the receiver.
        want.insert(
            "cairnflow_interrange_batch_messages_count".into(),
            2 * remote,
        );
        want.insert("cairnflow_interrange_batch_messages_sum".into(), 2 * remote);

        // The sender bumps the counter **after** the receiver acks, so the parent may finish
        // before the last bump: poll until it matches, with a limit.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let after = samples(&url).await?;
            let delta: BTreeMap<String, u64> = after
                .iter()
                .map(|(k, v)| (k.clone(), v - before.get(k).copied().unwrap_or(0)))
                .filter(|(_, d)| *d > 0)
                .collect();
            if delta == want {
                return Ok(());
            }
            if tokio::time::Instant::now() > deadline {
                return Err(anyhow!("counter difference {delta:?}\nmust be {want:?}"));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    node.shutdown().await;
    result
}

/// **Colocation gate on: an unkeyed call tree stays entirely on one range.**
///
/// A lone 4-range node on a fresh bucket (a fresh cluster has the gate on). A tree
/// `tree` of depth 3 and width 10: `Root` calls 10 `Mid`, each `Mid` calls 10 `Twig`, each
/// `Twig` calls 10 `Leaf` — 1 110 unkeyed calls, no scope.
///
/// Must see: every call is `placement=local` (`cairnflow_calls_total` grows by exactly
/// 1 110 in the `local/unkeyed` cell, nothing in `remote`), no cross-range message
/// at all, and every invocation of the tree keeps the root's high 32 bits — same range as
/// the root. Without the gate (the spread rule) ~75 % of calls go remote.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unkeyed_call_tree_stays_on_one_range_with_the_gate_on() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let node = common::start_single_process(4).await?;
    const WIDTH: usize = 10;
    let fan = |service: &str, handler: &str| -> Vec<Step> {
        let mut steps: Vec<Step> = (0..WIDTH)
            .map(|_| Step::call(service, handler, None))
            .collect();
        steps.push(Step::output("done"));
        steps
    };
    let sdk = FakeSdk::builder()
        .service("Root", "run", fan("Mid", "run"))
        .service("Mid", "run", fan("Twig", "run"))
        .service("Twig", "run", fan("Leaf", "echo"))
        .service("Leaf", "echo", vec![Step::output("la")])
        .spawn()
        .await?;
    let url = node.url();

    let result = common::within(Duration::from_secs(180), "3×10 call tree", async {
        common::register(&url, &sdk.url).await?;
        let before = samples(&url).await?;
        let (code, body) = common::send(&url, "Root", "run", None, "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let root = body["id"].as_str().unwrap_or_default().to_string();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(170);
        loop {
            let (_, st) = common::invocation(&url, &root).await?;
            if st["status"] == "completed" {
                break;
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "root not completed: {st}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // Range per the node's table (`/invocations` lists only 1 000 rows).
        let range_of = |id: &str| -> Result<u32> {
            let raw = cairn_store::id_from_hex(id).ok_or_else(|| anyhow!("bad id: {id}"))?;
            Ok(node.cluster.partition_of_id(&raw).0)
        };
        let home = range_of(&root)?;

        let calls_total = WIDTH + WIDTH * WIDTH + WIDTH * WIDTH * WIDTH;
        let mut seen = 0usize;
        for (service, handler) in [("Mid", "run"), ("Twig", "run"), ("Leaf", "echo")] {
            for s in sdk.starts_of(service, handler) {
                let id = s.id_hex();
                ensure!(
                    id[..8] == root[..8],
                    "{service} {id} does not keep the high 32 bits of root {root}"
                );
                seen += 1;
            }
        }
        ensure!(
            seen >= calls_total,
            "only saw {seen} turns of {calls_total} callees"
        );
        for (service, handler) in [("Mid", "run"), ("Twig", "run"), ("Leaf", "echo")] {
            for s in sdk.starts_of(service, handler) {
                let p = range_of(&s.id_hex())?;
                ensure!(
                    p == home,
                    "{service} {} on range {p}, root on range {home}",
                    s.id_hex()
                );
            }
        }

        let want: BTreeMap<String, u64> =
            BTreeMap::from([(calls("local", "unkeyed"), calls_total as u64)]);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let after = samples(&url).await?;
            let delta: BTreeMap<String, u64> = after
                .iter()
                .map(|(k, v)| (k.clone(), v - before.get(k).copied().unwrap_or(0)))
                .filter(|(_, d)| *d > 0)
                .collect();
            if delta == want {
                return Ok(());
            }
            if tokio::time::Instant::now() > deadline {
                return Err(anyhow!("counter difference {delta:?}\nmust be {want:?}"));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    node.shutdown().await;
    result
}

/// **`placement: "spread"` spreads on purpose even with the colocation gate on.**
///
/// A lone 4-range node on a fresh bucket (a fresh cluster has `calls/prefix` and
/// `calls/placement` on). `Root` calls, one after another, [`SPREAD`] unkeyed `Leaf`
/// callees with `placement: "spread"`, then [`NEAR`] unkeyed `Near` callees with the default.
///
/// Must see:
/// - every `Leaf` has **exactly** the id of the old rule `H(caller, completion)`
///   (`callee_partition_key` of the unkeyed class) — i.e. spread: ≈ (R−1)/R on a range
///   other than the root's; every `Near` keeps the root's high 32 bits (colocated);
/// - `cairnflow_calls_total` grows exactly per real placement, in the `unkeyed` cell.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spread_calls_fall_under_the_old_rule_with_the_gate_on() -> Result<()> {
    const SPREAD: u32 = 40;
    const NEAR: u32 = 4;
    let _serial = SERIAL.lock().await;
    let node = common::start_single_process(4).await?;
    let mut steps: Vec<Step> = (0..SPREAD)
        .map(|_| Step::call_spread("Leaf", "echo"))
        .collect();
    steps.extend((0..NEAR).map(|_| Step::call("Near", "echo", None)));
    steps.push(Step::output("done"));
    let sdk = FakeSdk::builder()
        .service("Root", "run", steps)
        .service("Leaf", "echo", vec![Step::output("la")])
        .service("Near", "echo", vec![Step::output("near")])
        .spawn()
        .await?;
    let url = node.url();

    let result = common::within(TEST_DEADLINE, "spread calls", async {
        common::register(&url, &sdk.url).await?;
        let before = samples(&url).await?;
        let (code, body) = common::send(&url, "Root", "run", None, "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let root = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &root, "completed").await?;
        let root_raw = cairn_store::id_from_hex(&root).ok_or_else(|| anyhow!("bad id: {root}"))?;
        let range_of = |id: &str| -> Result<u32> {
            let raw = cairn_store::id_from_hex(id).ok_or_else(|| anyhow!("bad id: {id}"))?;
            Ok(node.cluster.partition_of_id(&raw).0)
        };
        let home = range_of(&root)?;

        // The old rule, exactly `dispatch_call`'s computation for a v5 row: the completion
        // id of the i-th call is 2i + 1.
        let tenant = cairn_store::TenantId::default();
        let want_ids: std::collections::BTreeSet<String> = (0..SPREAD)
            .map(|i| {
                let cid = 2 * i + 1;
                let pk = cairn_store::callee_partition_key(
                    &tenant, None, "Leaf", "", None, &root_raw, cid,
                );
                cairn_store::id_hex(&cairn_store::callee_invocation_id(pk, &root_raw, cid))
            })
            .collect();
        let got_ids: std::collections::BTreeSet<String> = sdk
            .starts_of("Leaf", "echo")
            .iter()
            .map(|s| s.id_hex())
            .collect();
        ensure!(
            got_ids == want_ids,
            "spread callees must follow the old rule:\ngot  {got_ids:?}\nwant {want_ids:?}"
        );

        let mut want: BTreeMap<String, u64> = BTreeMap::new();
        let mut remote = 0u64;
        for id in &got_ids {
            let far = range_of(id)? != home;
            remote += u64::from(far);
            *want
                .entry(calls(if far { "remote" } else { "local" }, "unkeyed"))
                .or_default() += 1;
        }
        ensure!(
            remote >= u64::from(SPREAD) / 4,
            "spread over 4 ranges must spread (≈ 3/4 remote), only {remote}/{SPREAD}"
        );
        let near = sdk.starts_of("Near", "echo");
        ensure!(near.len() == NEAR as usize, "{near:?}");
        for s in &near {
            let id = s.id_hex();
            ensure!(
                id[..8] == root[..8] && range_of(&id)? == home,
                "Near {id} must be colocated with root {root}"
            );
        }
        *want.entry(calls("local", "unkeyed")).or_default() += u64::from(NEAR);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let after = samples(&url).await?;
            let delta: BTreeMap<String, u64> = after
                .iter()
                .filter(|(k, _)| k.starts_with("cairnflow_calls_total{"))
                .map(|(k, v)| (k.clone(), v - before.get(k).copied().unwrap_or(0)))
                .filter(|(_, d)| *d > 0)
                .collect();
            if delta == want {
                return Ok(());
            }
            if tokio::time::Instant::now() > deadline {
                return Err(anyhow!("counter difference {delta:?}\nmust be {want:?}"));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    node.shutdown().await;
    result
}

/// **One durability ticket per drain round, not per row.**
///
/// Order:
/// 1. A lone 4-range node. The deployment registry has only `Caller` (the first SDK).
/// 2. `Caller` (range 0) fires in **one** turn 24 calls without waiting
///    (`Step::fire`): 12 to object `Far` on range 1, 12 on range 2, interleaved; then waits.
///    `Far` is not yet in the node's registry, so the drainer leaves all 24 rows
///    (`dispatch_call` returns "cannot decide yet") — not sent, no `sync`.
/// 3. Register the second SDK (`Far`, a callee that waits forever — no `Reply` row produces
///    a `sync` on another range). The next drain pass sees all 24 rows in **one** round
///    (`BATCH` 32).
///
/// Must see: 24 `start_call` `ok` messages, and **exactly one** drainer `sync` for the
/// whole round. Formerly: one `sync` per row (24). With per-destination batching there
/// is also one send per destination (one batch) — `remote_rows_of_one_round_go_in_one_batch_…`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_rows_of_one_round_share_one_durability_wait() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let node = common::start_single_process(4).await?;
    const PER_RANGE: usize = 12;
    let caller_key = common::key_on_partition(&node, "Caller", 0, "caller");
    let mut steps = Vec::new();
    for i in 0..PER_RANGE {
        for range in [1, 2] {
            let key = common::key_on_partition(&node, "Far", range, &format!("far{range}-{i}"));
            steps.push(Step::fire("Far", "hold", Some(&key)));
        }
    }
    steps.push(Step::Suspend);
    let callers = FakeSdk::builder()
        .object("Caller", "fan", steps)
        .spawn()
        .await?;
    let far = FakeSdk::builder()
        .object("Far", "hold", vec![Step::Suspend])
        .spawn()
        .await?;
    let url = node.url();

    let result = common::within(TEST_DEADLINE, "one durability ticket per round", async {
        common::register(&url, &callers.url).await?;
        let (code, body) = common::send(&url, "Caller", "fan", Some(&caller_key), "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &id, "suspended").await?;
        let before = samples(&url).await?;

        common::register(&url, &far.url).await?;
        let total = 2 * PER_RANGE as u64;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let delta = loop {
            let after = samples(&url).await?;
            let delta: BTreeMap<String, u64> = after
                .iter()
                .map(|(k, v)| (k.clone(), v - before.get(k).copied().unwrap_or(0)))
                .filter(|(_, d)| *d > 0)
                .collect();
            if delta.get(&messages("start_call", "ok")) == Some(&total)
                && far.starts_of("Far", "hold").len() == total as usize
            {
                break delta;
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "not all {total} calls sent yet: {delta:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let get = |k: &str| delta.get(k).copied().unwrap_or(0);
        ensure!(
            get("cairnflow_interrange_sync_seconds_count") == 1,
            "24 rows of one round must share one durability wait: {delta:?}"
        );
        Ok(())
    })
    .await;

    node.shutdown().await;
    result
}

/// **Per-destination batches: k messages, one request, one commit.**
///
/// Order (same shape as the test above):
/// 1. A lone 4-range node. The deployment registry has only `Caller`.
/// 2. `Caller` (range 0) fires in one turn 24 calls without waiting: 12 to `Far`
///    on range 1, 12 on range 2, interleaved; then waits. All 24 rows stay.
/// 3. Register `Far` (waits forever). The next drain pass gathers all 24 rows in one round.
///
/// Must see:
/// - exactly **two** sends (`cairnflow_interrange_send_seconds_count`) — one batch
///   request per destination — and **two** inbound commits at the receiver carrying 24
///   messages (`cairnflow_interrange_batch_messages`: `_count` 2, `_sum` 24; each sample
///   is the message count of one commit, recorded once that commit is durable);
/// - 24 `start_call` `ok` messages, each `Far` runs exactly once;
/// - the sender has applied all 24 (`Forget`): one second later no message is resent.
///
/// Before batching: 24 sends, 24 commits, no `batch_messages` sample.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_rows_of_one_round_go_in_one_batch_per_destination() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let node = common::start_single_process(4).await?;
    const PER_RANGE: usize = 12;
    let caller_key = common::key_on_partition(&node, "Caller", 0, "caller");
    let mut steps = Vec::new();
    for i in 0..PER_RANGE {
        for range in [1, 2] {
            let key = common::key_on_partition(&node, "Far", range, &format!("far{range}-{i}"));
            steps.push(Step::fire("Far", "hold", Some(&key)));
        }
    }
    steps.push(Step::Suspend);
    let callers = FakeSdk::builder()
        .object("Caller", "fan", steps)
        .spawn()
        .await?;
    let far = FakeSdk::builder()
        .object("Far", "hold", vec![Step::Suspend])
        .spawn()
        .await?;
    let url = node.url();

    let result = common::within(TEST_DEADLINE, "per-destination batches", async {
        common::register(&url, &callers.url).await?;
        let (code, body) = common::send(&url, "Caller", "fan", Some(&caller_key), "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &id, "suspended").await?;
        let before = samples(&url).await?;
        let delta_of = |after: &BTreeMap<String, u64>| -> BTreeMap<String, u64> {
            after
                .iter()
                .map(|(k, v)| (k.clone(), v - before.get(k).copied().unwrap_or(0)))
                .filter(|(_, d)| *d > 0)
                .collect()
        };

        common::register(&url, &far.url).await?;
        let total = 2 * PER_RANGE as u64;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let delta = delta_of(&samples(&url).await?);
            if delta.get(&messages("start_call", "ok")) == Some(&total)
                && far.starts_of("Far", "hold").len() == total as usize
            {
                break;
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "not all {total} calls sent yet: {delta:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // Applied rows are not resent: give the drainer a few ticks then count again.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let delta = delta_of(&samples(&url).await?);
        let get = |k: &str| delta.get(k).copied().unwrap_or(0);
        ensure!(
            get(&messages("start_call", "ok")) == total,
            "messages resent after the ack: {delta:?}"
        );
        ensure!(
            get("cairnflow_interrange_send_seconds_count") == 2,
            "one batch request per destination: {delta:?}"
        );
        ensure!(
            get("cairnflow_interrange_batch_messages_count") == 2
                && get("cairnflow_interrange_batch_messages_sum") == total,
            "the receiver must write each batch in one commit: {delta:?}"
        );
        let mut started: Vec<String> = far
            .starts_of("Far", "hold")
            .iter()
            .map(|s| s.id_hex())
            .collect();
        started.sort();
        started.dedup();
        ensure!(
            started.len() == total as usize,
            "each Far exactly once: {}",
            started.len()
        );
        Ok(())
    })
    .await;

    node.shutdown().await;
    result
}

/// **Signals take the same colocation and batching path.**
///
/// Order: a lone 4-range node; `Sender` (range 0) sends in **one** turn 8
/// signals to 8 ids on range 1 (made-up ids: the receiver writes the message then drops it,
/// since the target does not exist — enough to walk the whole send path). The signal rows
/// are produced in the same end-of-turn commit, so the drainer reads all 8 in one round.
///
/// Must see: 8 `signal` `ok` messages, sent in **one** batch request and written in one
/// commit at the receiver (`cairnflow_interrange_batch_messages`: `_count` 1,
/// `_sum` 8), and only one `sync`. Before: one `sync`, one request and one commit per
/// signal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signals_of_one_round_go_in_one_batch() -> Result<()> {
    use {
        cairn_store::{make_invocation_id, PartitionKey},
        std::sync::{Arc, Mutex},
    };
    let _serial = SERIAL.lock().await;
    let node = common::start_single_process(4).await?;
    const N: usize = 8;
    let start = node.cluster.table().ranges()[1].start;
    let steps: Vec<Step> = (0..N)
        .map(|i| {
            let id = make_invocation_id(PartitionKey(start + i as u64), [i as u8; 16]);
            Step::SendSignal(Arc::new(Mutex::new(cairn_store::id_hex(&id))))
        })
        .chain([Step::output("sent")])
        .collect();
    let sender_key = common::key_on_partition(&node, "Sender", 0, "sender");
    let sdk = FakeSdk::builder()
        .object("Sender", "fire", steps)
        .spawn()
        .await?;
    let url = node.url();

    let result = common::within(TEST_DEADLINE, "batched signals", async {
        common::register(&url, &sdk.url).await?;
        let before = samples(&url).await?;
        let (code, body) = common::send(&url, "Sender", "fire", Some(&sender_key), "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &id, "completed").await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let after = samples(&url).await?;
            let delta: BTreeMap<String, u64> = after
                .iter()
                .map(|(k, v)| (k.clone(), v - before.get(k).copied().unwrap_or(0)))
                .filter(|(_, d)| *d > 0)
                .collect();
            let get = |k: &str| delta.get(k).copied().unwrap_or(0);
            if get(&messages("signal", "ok")) == N as u64 {
                ensure!(
                    get("cairnflow_interrange_batch_messages_count") == 1
                        && get("cairnflow_interrange_batch_messages_sum") == N as u64,
                    "8 signals of one round must be one batch, one commit: {delta:?}"
                );
                ensure!(
                    get("cairnflow_interrange_send_seconds_count") == 1
                        && get("cairnflow_interrange_sync_seconds_count") == 1,
                    "one durability wait, one request: {delta:?}"
                );
                return Ok(());
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "not all {N} signals sent yet: {delta:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    node.shutdown().await;
    result
}

/// **The drainer does not re-read the rows of a busy caller on every
/// pass.**
///
/// Order:
/// 1. A lone 4-range node. The deployment registry has `Caller` (a gated handler: each
///    turn takes one slot of the fake SDK) and `Ping`; no `Far` yet.
/// 2. `Caller` (range 0) fires in one turn `N` calls without waiting
///    (`Step::fire`) to object `Far` on ranges 1–3, then waits for a signal. `Far`
///    is not in the registry ⇒ all `N` `Call` rows stay.
/// 3. Signal `Caller`: its second turn stops at the fake SDK's gate — the `Caller`
///    lock is held throughout (the engine waits for the SDK to answer).
/// 4. Register `Far`. From now on `Caller`'s rows can be sent, they only wait for the lock.
/// 5. Measuring window: `K` `/invoke` calls of `Ping` (an object on range 0, no commands) —
///    each nudges the range-0 drainer for one pass. The `Caller` lock is busy throughout.
/// 6. Open the gate: `Caller` moves on to a long `ctx.sleep` and waits; the lock is
///    released, the `N` rows go.
///
/// Must see: within window 5 there are ≥ `K/2` drain passes while the rows the drainer
/// reads total ≤ `2N` — a blocked row is read once, not on every pass (O(N), not
/// O(N · passes)); `Caller` is still `running` at the end of the window; after 6, each
/// `Far` runs exactly once (no row skipped or sent twice). Before the fix: every pass
/// re-read all `N` rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn busy_callers_rows_are_not_reread_every_pass() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let node = common::start_single_process(4).await?;
    const N: usize = 48;
    const K: usize = 40;
    let caller_key = common::key_on_partition(&node, "Caller", 0, "caller");
    let ping_key = common::key_on_partition(&node, "Ping", 0, "ping");
    let mut far_keys = Vec::new();
    let mut steps = Vec::new();
    for i in 0..N {
        let range = 1 + (i % 3) as u32;
        let key = common::key_on_partition(&node, "Far", range, &format!("far{range}-{i}"));
        steps.push(Step::fire("Far", "hold", Some(&key)));
        far_keys.push(key);
    }
    steps.extend([Step::Suspend, Step::Sleep(600_000)]);
    let callers = FakeSdk::builder()
        .gated_object("Caller", "fan", steps)
        .object("Ping", "pong", vec![Step::output("\"pong\"")])
        .spawn()
        .await?;
    let far = FakeSdk::builder()
        .object("Far", "hold", vec![Step::Suspend])
        .spawn()
        .await?;
    let gate = callers.gate();
    let url = node.url();

    let result = common::within(TEST_DEADLINE, "rows of a busy caller", async {
        common::register(&url, &callers.url).await?;
        // 2.
        gate.release(1);
        let (code, body) = common::send(&url, "Caller", "fan", Some(&caller_key), "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &id, "suspended").await?;
        // 3. Sending the signal waits for the turn it wakes — run it separately.
        let signal = tokio::spawn({
            let url = url.clone();
            let id = id.clone();
            async move {
                reqwest::Client::new()
                    .post(format!("{url}/invocations/{id}/signal"))
                    .json(&serde_json::json!({ "signal": common::fake_sdk::HOLD_SIGNAL }))
                    .send()
                    .await
                    .map(|r| r.status())
            }
        });
        let t0 = tokio::time::Instant::now();
        while callers.starts_of("Caller", "fan").len() < 2 {
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "Caller's second turn has not reached the gate"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // 4.
        common::register(&url, &far.url).await?;
        // 5.
        let before = drain_counters(&url).await?;
        for _ in 0..K {
            let (code, body) = common::invoke(&url, "Ping", "pong", Some(&ping_key), "x").await?;
            ensure!(code == 200, "{code} {body}");
        }
        let after = drain_counters(&url).await?;
        let (code, st) = common::invocation(&url, &id).await?;
        ensure!(
            code == 200 && st["status"] == "running",
            "Caller must still be in the held turn throughout the window: {st}"
        );
        let d = |k: &str| after.get(k).copied().unwrap_or(0) - before.get(k).copied().unwrap_or(0);
        let passes = d("cairnflow_outbox_drain_passes_total");
        let read = d("cairnflow_outbox_rows_read_total");
        ensure!(
            passes >= (K / 2) as u64,
            "the window must have at least {} drain passes, has {passes}",
            K / 2
        );
        ensure!(
            read <= 2 * N as u64,
            "{passes} drain passes while the caller's lock was busy read {read} rows — \
             must be ≤ {} (each blocked row once), not all {N} rows on every pass",
            2 * N
        );
        // 6.
        gate.open();
        let t0 = tokio::time::Instant::now();
        loop {
            let starts = far.starts_of("Far", "hold");
            if starts.len() >= N {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let mut keys: Vec<String> = far
                    .starts_of("Far", "hold")
                    .into_iter()
                    .map(|s| s.key)
                    .collect();
                keys.sort();
                let mut want = far_keys.clone();
                want.sort();
                ensure!(keys == want, "each Far runs exactly once: {keys:?}");
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(30),
                "only {} / {N} Far ran",
                starts.len()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let status = signal.await??;
        ensure!(status.is_success(), "signal Caller: {status}");
        Ok(())
    })
    .await;

    node.shutdown().await;
    result
}

/// **When a caller releases its lock its rows go, while another caller that is still
/// busy keeps its rows** (the drainer's ledger reconsiders exactly the rows of the one
/// that just released, skipping none).
///
/// Order:
/// 1. A lone 4-range node. Two fake SDKs, each with a handler behind its own gate:
///    `CallerA` and `CallerB`, both on range 0; no `Far` yet.
/// 2. Each caller fires `N` calls without waiting to `Far` on ranges 1–3, then waits
///    for a signal; `Far` does not exist ⇒ `2N` rows stay.
/// 3. Signal both: each one's second turn stops at its SDK's gate — both
///    locks busy.
/// 4. Register `Far`; a few `/invoke` calls of `Ping` on range 0 make the drainer run a few
///    passes (both callers' rows enter the ledger, waiting for the lock).
/// 5. Open `B`'s gate: `B` moves on to a long `ctx.sleep` and waits — `B`'s lock is
///    released, `A`'s lock is still busy.
/// 6. Open `A`'s gate.
///
/// Must see: after 5, exactly `N` `Far` of `B` run, each once, no `Far`
/// of `A`, and `A` is still `running`; after 6, all `2N` `Far` run, each exactly
/// once. A debug build checks the ledger invariant at the end of every pass that still
/// has rows waiting for a lock (`turn::outbox::check_backlog`): a row of `B` dropping
/// out of the ledger while a row of `A` is still waiting is a panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn released_callers_rows_go_while_another_caller_stays_busy() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let node = common::start_single_process(4).await?;
    const N: usize = 24;
    let mut far_keys = BTreeMap::new();
    let mut sdks = Vec::new();
    for name in ["CallerA", "CallerB"] {
        let mut steps = Vec::new();
        let mut keys = Vec::new();
        for i in 0..N {
            let range = 1 + (i % 3) as u32;
            let key =
                common::key_on_partition(&node, "Far", range, &format!("{name}-far{range}-{i}"));
            steps.push(Step::fire("Far", "hold", Some(&key)));
            keys.push(key);
        }
        steps.extend([Step::Suspend, Step::Sleep(600_000)]);
        far_keys.insert(name, keys);
        let sdk = FakeSdk::builder()
            .gated_object(name, "fan", steps)
            .object(
                &format!("Ping{name}"),
                "pong",
                vec![Step::output("\"pong\"")],
            )
            .spawn()
            .await?;
        sdks.push((name, sdk));
    }
    let far = FakeSdk::builder()
        .object("Far", "hold", vec![Step::Suspend])
        .spawn()
        .await?;
    let ping_key = common::key_on_partition(&node, "PingCallerA", 0, "ping");
    let url = node.url();

    let started = |name: &str| -> Vec<String> {
        let want = &far_keys[name];
        let mut got: Vec<String> = far
            .starts_of("Far", "hold")
            .into_iter()
            .map(|s| s.key)
            .filter(|k| want.contains(k))
            .collect();
        got.sort();
        got
    };
    let result = common::within(
        TEST_DEADLINE,
        "one caller releases its lock, another stays busy",
        async {
            // 2.
            let mut ids = BTreeMap::new();
            for (name, sdk) in &sdks {
                common::register(&url, &sdk.url).await?;
                sdk.gate().release(1);
                let key = common::key_on_partition(&node, name, 0, "caller");
                let (code, body) = common::send(&url, name, "fan", Some(&key), "x").await?;
                ensure!(code == 200 || code == 202, "{code} {body}");
                let id = body["id"].as_str().unwrap_or_default().to_string();
                common::wait_status(&url, &id, "suspended").await?;
                ids.insert(*name, id);
            }
            // 3.
            let mut signals = Vec::new();
            for (name, sdk) in &sdks {
                let url = url.clone();
                let id = ids[name].clone();
                signals.push(tokio::spawn(async move {
                    reqwest::Client::new()
                        .post(format!("{url}/invocations/{id}/signal"))
                        .json(&serde_json::json!({ "signal": common::fake_sdk::HOLD_SIGNAL }))
                        .send()
                        .await
                        .map(|r| r.status())
                }));
                let t0 = tokio::time::Instant::now();
                while sdk.starts_of(name, "fan").len() < 2 {
                    ensure!(
                        t0.elapsed() < Duration::from_secs(10),
                        "{name}'s second turn has not reached the gate"
                    );
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            // 4.
            common::register(&url, &far.url).await?;
            for _ in 0..8 {
                let (code, body) =
                    common::invoke(&url, "PingCallerA", "pong", Some(&ping_key), "x").await?;
                ensure!(code == 200, "{code} {body}");
            }
            ensure!(
                started("CallerA").is_empty() && started("CallerB").is_empty(),
                "no caller has released its lock, yet a Far ran"
            );
            // 5.
            sdks[1].1.gate().open();
            let mut want_b = far_keys["CallerB"].clone();
            want_b.sort();
            let t0 = tokio::time::Instant::now();
            while started("CallerB").len() < N {
                ensure!(
                    t0.elapsed() < Duration::from_secs(30),
                    "only {} / {N} Far of B ran",
                    started("CallerB").len()
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            ensure!(started("CallerB") == want_b, "each Far of B exactly once");
            ensure!(
                started("CallerA").is_empty(),
                "a Far of A ran while A's lock was busy"
            );
            let (code, st) = common::invocation(&url, &ids["CallerA"]).await?;
            ensure!(
                code == 200 && st["status"] == "running",
                "A must still be in the held turn: {st}"
            );
            // 6.
            sdks[0].1.gate().open();
            let mut want_a = far_keys["CallerA"].clone();
            want_a.sort();
            let t0 = tokio::time::Instant::now();
            while started("CallerA").len() < N {
                ensure!(
                    t0.elapsed() < Duration::from_secs(30),
                    "only {} / {N} Far of A ran",
                    started("CallerA").len()
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            ensure!(started("CallerA") == want_a, "each Far of A exactly once");
            ensure!(started("CallerB") == want_b, "Far of B did not re-run");
            ensure!(
                far.starts_of("Far", "hold").len() == 2 * N,
                "exactly {} Far: {}",
                2 * N,
                far.starts_of("Far", "hold").len()
            );
            for s in signals {
                let status = s.await??;
                ensure!(status.is_success(), "signal: {status}");
            }
            Ok(())
        },
    )
    .await;

    node.shutdown().await;
    result
}

/// **The commit that receives a message and the callee's first `Running` share one WAL
/// object.**
///
/// Order: two nodes, one range each, WAL linger 50 ms (long enough for every commit
/// of the receiver within that window to land in one object), background tick 30 s. `Caller/run` on alpha calls
/// `Callee/echo` on beta; the callee's SDK holds the turn at the gate. Count PUTs of data
/// objects of beta's shared-WAL log from the send until the callee's turn reaches the
/// SDK — i.e. until its `Running` is durable.
///
/// Must see: **one** PUT. Before the fix, two: beta's drainer was nudged only after the
/// `200`, i.e. after the object holding the receiving commit had been PUT; `Running` went
/// into the next object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_message_and_first_running_share_one_wal_object() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let bucket = common::bucket();
    let mut nodes = Vec::new();
    for id in ["node-alpha", "node-beta"] {
        nodes.push(
            common::start_cluster_node_tweaked(&bucket, id, 2, Duration::from_secs(2), |cfg| {
                cfg.node_wal.linger = Duration::from_millis(50);
                // A sparse background tick: it also nudges the drainer (an existing
                // interleaving), and at a 50 ms tick it would land inside the linger
                // window — the test must see only the early nudge under test.
                cfg.tick_interval = Some(Duration::from_secs(30));
            })
            .await?,
        );
    }
    let refs: Vec<_> = nodes.iter().collect();
    let r = common::within(TEST_DEADLINE, "receive and Running share an object", async {
        let leaders = common::wait_probe(&refs, Duration::from_secs(20), "balanced 1/1", |p| {
            common::single_leaders(p).is_some_and(|m| common::load_of(&m, 2) == [1, 1])
        })
        .await?;
        let leaders = common::single_leaders(&leaders).expect("leaders exist");
        let on = |i: usize| *leaders.iter().find(|(_, n)| **n == i).expect("range exists").0;
        let (alpha, beta) = (&nodes[0], &nodes[1]);
        let caller_key = common::key_on_partition(&alpha.node, "Caller", on(0), "caller");
        let callee_key = common::key_on_partition(&alpha.node, "Callee", on(1), "callee");
        let sdk = FakeSdk::builder()
            .object(
                "Caller",
                "run",
                vec![
                    Step::call("Callee", "echo", Some(&callee_key)),
                    Step::EchoCallResult,
                ],
            )
            .gated_object("Callee", "echo", vec![Step::output("pong")])
            .spawn()
            .await?;
        for n in &nodes {
            common::register(&n.node.url(), &sdk.url).await?;
        }
        let url = alpha.node.url();
        // Precondition of the measurement: alpha's routing table has seen beta lead the
        // callee range. The balance probe asks the engine directly; the table follows the
        // session's `owns` hint, up to one tick slower — in delegated mode (the
        // default) that is one heartbeat + one re-read of beta's record by the
        // coordinator (~0.35 s measured). A message sent while the table has no leader
        // (`unreachable`) is resent only on the background tick — 30 s here — so without
        // waiting the test measures that race instead of the WAL object.
        let tb = std::time::Instant::now();
        while alpha
            .node
            .routing
            .leader_of(cairn_store::PartitionId(on(1)))
            .is_none_or(|l| l.node_id != "node-beta")
        {
            ensure!(
                tb.elapsed() < Duration::from_secs(10),
                "alpha's routing table has not seen beta lead the callee range"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        beta.store.record_writes();
        let (code, body) = common::send(&url, "Caller", "run", Some(&caller_key), "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let caller = body["id"].as_str().unwrap_or_default().to_string();
        let t0 = std::time::Instant::now();
        while sdk.starts_of("Callee", "echo").is_empty() {
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "the callee's turn has not reached the SDK"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let puts = beta
            .store
            .take_writes()
            .into_iter()
            .filter(|(op, p)| {
                *op == cairn_store::testkit::Op::Put
                    && p.as_ref().contains("nodewal/node-beta/")
                    && !p.as_ref().contains("/floor/")
            })
            .count();
        sdk.gate().open();
        let st = common::wait_status(&url, &caller, "completed").await?;
        ensure!(st["output"] == "pong", "{st}");
        ensure!(
            puts == 1,
            "beta PUT {puts} WAL log objects between receiving the message and the callee's Running being durable — want 1"
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **Security review — across nodes.** `Watch`/`DeliverSignal` messages from tenant
/// `alpha` to an invocation of tenant `bravo` on **another node** are answered as if
/// it does not exist (the same-node version, same or other range:
/// `tenant_isolation.rs::alpha_sdk_cannot_reach_bravo_by_id`).
///
/// Order:
/// 1. Two nodes, auth on, each node leads one range: P on node 0, Q on node 1.
/// 2. `bravo` runs object `Echo` (key on P) to completion and object `Hold` (key
///    on P) to `suspended`; snapshot `Hold`'s journal. `alpha` builds object
///    `Fence` (key on P) waiting for `HOLD_SIGNAL` — the fence.
/// 3. `alpha` runs object `Probe` (key on Q, running on node 1): `getOutput`/`attach`
///    to `bravo`'s `Echo` and `Hold`, `signal` idx 1, idx 17 and
///    `HOLD_SIGNAL` to `Hold`, then `HOLD_SIGNAL` to `Fence` (same sender,
///    same target range ⇒ arrives after every signal to `Hold`).
/// 4. Wait for `Probe` and `Fence` to complete.
///
/// Must see: the four results are exactly `404 invocation <id> not found`; `Hold`
/// is still `suspended`, its journal identical to step 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreign_tenant_watch_and_signal_across_nodes_answer_not_found() -> Result<()> {
    // This test reads no counter but **produces** `attach`/`peek`/`signal`/
    // `completion` messages into the process-wide counters: without holding the lock, a
    // counting test running alongside under `cargo test` sees them in its difference
    // (failed 1–3 times in 6 in `unkeyed_call_tree_stays_on_one_range_with_the_gate_on`).
    let _serial = SERIAL.lock().await;
    use {
        common::{load_of, single_leaders, start_cluster_node_tweaked, wait_probe},
        serde_json::{json, Value},
        std::sync::{Arc, Mutex},
    };
    const BOOT: &str = "cf_test_interrange_b1_bootstrap_0123456789";
    let shared = common::bucket();
    let authed = |cfg: &mut cairn_server::node::NodeConfig| {
        cfg.auth = cairn_server::auth::AuthConfig {
            required: true,
            admin_key: Some(BOOT.into()),
            cluster_secret: None,
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
    };
    let ttl = Duration::from_secs(1);
    let nodes = vec![
        start_cluster_node_tweaked(&shared, "node-p", 2, ttl, authed).await?,
        start_cluster_node_tweaked(&shared, "node-q", 2, ttl, authed).await?,
    ];
    let refs: Vec<_> = nodes.iter().collect();
    let probe = wait_probe(
        &refs,
        Duration::from_secs(20),
        "each node leads one range",
        |p| single_leaders(p).is_some_and(|m| load_of(&m, 2) == [1, 1]),
    )
    .await?;
    let leaders = single_leaders(&probe).expect("balanced");
    let range_led_by = |n: usize| *leaders.iter().find(|(_, i)| **i == n).expect("a range").0;
    let (p, q) = (range_led_by(0), range_led_by(1));
    let (np, nq) = (&nodes[0].node, &nodes[1].node);

    let http = reqwest::Client::new();
    let call = |url: String, key: String, method: reqwest::Method, body: Option<Value>| {
        let http = http.clone();
        async move {
            let mut rb = http.request(method, url).bearer_auth(key);
            if let Some(b) = body {
                rb = rb.json(&b);
            }
            let r = rb.send().await?;
            let code = r.status().as_u16();
            let text = r.text().await?;
            Ok::<_, anyhow::Error>((
                code,
                serde_json::from_str::<Value>(&text).unwrap_or(Value::String(text)),
            ))
        }
    };
    let new_key = |tenant: &'static str| {
        let call = &call;
        let url = np.url();
        async move {
            let (code, v) = call(
                format!("{url}/admin/keys"),
                BOOT.into(),
                reqwest::Method::POST,
                Some(json!({ "tenant": tenant, "role": "admin" })),
            )
            .await?;
            ensure!(code == 200 || code == 201, "key {tenant}: {code} {v}");
            Ok::<_, anyhow::Error>(v["secret"].as_str().unwrap_or_default().to_string())
        }
    };
    let key_on = |tenant: &str, service: &str, range: u32, prefix: &str| {
        let t = cairn_store::TenantId::new(tenant).expect("tenant");
        (0..10_000)
            .map(|i| format!("{prefix}-{i}"))
            .find(|k| np.cluster.partition_of(&t, service, k).0 == range)
            .expect("a key on the range")
    };

    let echo = "\"bravo-secret-out\"";
    let b_sdk = FakeSdk::builder()
        .object("Echo", "run", vec![Step::output(echo)])
        .object("Hold", "run", vec![Step::Suspend, Step::output(echo)])
        .spawn()
        .await?;
    let cell = || Arc::new(Mutex::new(String::new()));
    let (done, held, fence) = (cell(), cell(), cell());
    let a_sdk = FakeSdk::builder()
        .object(
            "Probe",
            "run",
            vec![
                Step::GetOutput(done.clone()),
                Step::Attach(done.clone()),
                Step::GetOutput(held.clone()),
                Step::Attach(held.clone()),
                Step::SignalIdx(held.clone(), 1),
                Step::SignalIdx(held.clone(), 17),
                Step::SendSignal(held.clone()),
                Step::SendSignal(fence.clone()),
                Step::OutputResults,
            ],
        )
        .object(
            "Fence",
            "run",
            vec![Step::Suspend, Step::output("\"fence\"")],
        )
        .spawn()
        .await?;

    let result = common::within(Duration::from_secs(120), "cross-node isolation", async {
        let (b_key, a_key) = (new_key("bravo").await?, new_key("alpha").await?);
        use reqwest::Method as M;
        for (key, sdk) in [(&b_key, &b_sdk.url), (&a_key, &a_sdk.url)] {
            let (code, v) = call(
                format!("{}/deployments", np.url()),
                key.clone(),
                M::POST,
                Some(json!({ "uri": sdk })),
            )
            .await?;
            ensure!(code < 300, "deployment: {code} {v}");
        }
        let send = |node: &cairn_server::node::RunningNode, key: &str, path: String| {
            let (url, key) = (node.url(), key.to_string());
            let call = &call;
            async move {
                let (code, v) =
                    call(format!("{url}{path}"), key, M::POST, Some(json!("x"))).await?;
                ensure!(code < 300, "{path}: {code} {v}");
                Ok::<_, anyhow::Error>(v["id"].as_str().unwrap_or_default().to_string())
            }
        };
        let status = |key: &str, id: &str| {
            let (url, key, id) = (np.url(), key.to_string(), id.to_string());
            let call = &call;
            async move {
                let (_, v) = call(format!("{url}/invocations/{id}"), key, M::GET, None).await?;
                Ok::<_, anyhow::Error>(v)
            }
        };
        let wait = |key: &str, id: &str, want: &'static str| {
            let (key, id) = (key.to_string(), id.to_string());
            let status = &status;
            async move {
                loop {
                    if status(&key, &id).await?["status"] == want {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        };
        // ── 2.
        let k = key_on("bravo", "Echo", p, "e");
        let done_id = send(np, &b_key, format!("/invoke/Echo/run/send?key={k}")).await?;
        wait(&b_key, &done_id, "completed").await?;
        let k = key_on("bravo", "Hold", p, "h");
        let held_id = send(np, &b_key, format!("/invoke/Hold/run/send?key={k}")).await?;
        wait(&b_key, &held_id, "suspended").await?;
        let journal = format!("{}/invocations/{held_id}/journal", np.url());
        let before = call(journal.clone(), b_key.clone(), M::GET, None).await?;
        let k = key_on("alpha", "Fence", p, "f");
        let fence_id = send(np, &a_key, format!("/invoke/Fence/run/send?key={k}")).await?;
        wait(&a_key, &fence_id, "suspended").await?;
        // ── 3.
        *done.lock().unwrap() = done_id.clone();
        *held.lock().unwrap() = held_id.clone();
        *fence.lock().unwrap() = fence_id.clone();
        let k = key_on("alpha", "Probe", q, "p");
        // Node 1 loads the key registry on its own cadence: wait for it to pick up alpha's key.
        while call(
            format!("{}/invocations", nq.url()),
            a_key.clone(),
            M::GET,
            None,
        )
        .await?
        .0 == 401
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let probe_id = send(nq, &a_key, format!("/invoke/Probe/run/send?key={k}")).await?;
        // ── 4.
        wait(&a_key, &probe_id, "completed").await?;
        wait(&a_key, &fence_id, "completed").await?;
        let v = status(&a_key, &probe_id).await?;
        let raw = match &v["output"] {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let out = serde_json::from_str::<String>(&raw).unwrap_or(raw);
        let want: Vec<String> = [&done_id, &done_id, &held_id, &held_id]
            .iter()
            .map(|id| format!("failure:404:invocation {id} not found"))
            .collect();
        ensure!(
            out.lines().collect::<Vec<_>>() == want,
            "cross-node results other than 404: {out:?}"
        );
        let st = status(&b_key, &held_id).await?;
        ensure!(
            st["status"] == "suspended",
            "bravo's Hold is no longer waiting: {st}"
        );
        let after = call(journal, b_key.clone(), M::GET, None).await?;
        ensure!(
            after == before,
            "Hold's journal changed: {before:?} → {after:?}"
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    result
}

/// **Narrow — the internal port does not cut the body at 2 MiB.**
///
/// axum's `Bytes` extractor has a default 2 MiB limit when the router does not declare
/// `DefaultBodyLimit`; the signature layer reads the body up to `MAX_INTERNAL_BODY` (256 MiB)
/// and rebuilds the `Body`, but does not lift that limit. A correctly signed
/// `POST /internal/inbound` carrying 3 MiB got `413` before the handler ran.
///
/// Must see: the handler runs — a 3 MiB junk body ⇒ `400 malformed message`, not
/// `413`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn internal_inbound_takes_bodies_over_two_mib() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let node = common::start_single_process(1).await?;
    let result = common::within(TEST_DEADLINE, "inbound 3 MiB", async {
        let resp = common::post_internal(&node, "/internal/inbound", vec![0xff; 3 << 20]).await?;
        let status = resp.status().as_u16();
        let body = resp.text().await?;
        ensure!(
            status == 400 && body.contains("malformed message"),
            "a correctly signed 3 MiB body must reach the handler (400 malformed), got {status}: {body}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    result
}

/// **Broad — a cross-range message > 2 MiB does not block the head of the queue.**
///
/// 1. A lone 4-range node; two callers `Caller` on range 0, two targets `Echo` on
///    range 1.
/// 2. `big` calls `Echo` with a 3 MiB argument ⇒ an outbox `Call` row, a `StartCall`
///    message of ~3 MiB that goes alone (larger than the 1 MiB batch limit).
/// 3. `small` calls `Echo` (another key, same range 1) with an empty argument — its row
///    stands **behind** `big`'s row in the per-destination queue.
///
/// Before the fix: the receiver answered `413` (the extractor's 2 MiB limit), the sender
/// treated 4xx as a fault of the message itself, kept the row, the target went `down` ⇒
/// `small`'s row never went; the next drain pass repeated exactly that.
///
/// Must see: both callers `completed` within 10 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_cross_range_call_does_not_block_the_queue() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let node = common::start_single_process(4).await?;
    let big_key = common::key_on_partition(&node, "Caller", 0, "big");
    let small_key = common::key_on_partition(&node, "Caller", 0, "small");
    let far_a = common::key_on_partition(&node, "Echo", 1, "far-a");
    let far_b = common::key_on_partition(&node, "Echo", 1, "far-b");
    let sdk = FakeSdk::builder()
        .object(
            "Caller",
            "big",
            vec![
                Step::CallWith {
                    service: "Echo".into(),
                    handler: "ping".into(),
                    key: Some(far_a.clone()),
                    parameter: bytes::Bytes::from(vec![b'x'; 3 << 20]),
                },
                Step::output("big"),
            ],
        )
        .object(
            "Caller",
            "small",
            vec![
                Step::call("Echo", "ping", Some(&far_b)),
                Step::output("small"),
            ],
        )
        .object("Echo", "ping", vec![Step::output("pong")])
        .spawn()
        .await?;
    let url = node.url();
    let result = common::within(TEST_DEADLINE, "3 MiB cross-range message", async {
        common::register(&url, &sdk.url).await?;
        let (code, big) = common::invoke(&url, "Caller", "big", Some(&big_key), "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {big}");
        let big = big["id"].as_str().unwrap_or_default().to_string();
        let (code, small) = common::invoke(&url, "Caller", "small", Some(&small_key), "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {small}");
        let small = small["id"].as_str().unwrap_or_default().to_string();
        for (what, id) in [("small", &small), ("big", &big)] {
            tokio::time::timeout(
                Duration::from_secs(10),
                common::wait_status(&url, id, "completed"),
            )
            .await
            .map_err(|_| {
                anyhow!("{what} not done in 10 s — outbox row stuck behind the 3 MiB message")
            })??;
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    result
}
