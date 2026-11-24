// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Delivering a completion to the caller does not replay its journal every
//! time: count how often the drainer reloads the caller's journal
//! (`cairnflow_settle_replays_total`) against the number of completions it
//! delivers (`cairnflow_settle_total`).
//!
//! **The counters are process-wide** (`metrics::settle`), so the tests read the
//! **difference** before/after on `/metrics` and hold [`SERIAL`] for the whole
//! counting part: under `cargo test` the tests of this file share one process.
//! Nextest runs each test in its own process.

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
    common::{
        fake_sdk::{FakeSdk, Step},
        TEST_DEADLINE,
    },
};

/// Lock for the counting part of every test in the file — see the file's doc.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// `(cairnflow_settle_total, cairnflow_settle_replays_total)`.
async fn counts(url: &str) -> Result<(u64, u64)> {
    Ok((
        common::metric(url, "cairnflow_settle_total").await?,
        common::metric(url, "cairnflow_settle_replays_total").await?,
    ))
}

/// **One inbound completion, one load of the caller's journal.**
///
/// Order:
/// 1. A lone node with 4 ranges. `Caller` (object, range 0) calls `Far`
///    (object, range 1) `K` times **in sequence**, each time waiting for the
///    result, then completes.
/// 2. Each call: range 0's drainer sends `StartCall`, and after the ack delivers
///    `CallInvocationId` to `Caller` (the outbox path, `settle_completion`);
///    `Far` completes, the result comes back to range 0 as an inbound message
///    (`deliver_inbound_completion`).
///
/// Must see: each delivered completion (2K) takes **exactly one** machine — a
/// journal load (`replays`) or a warm machine (`hit`, since the warm-machine
/// change; since only a satisfied wait tree wakes the caller, the id is not in
/// the wait tree so it does not wake the caller and the warm machine lives until
/// the result). Before the fix the inbound path loaded twice — once to check
/// `settled`, once in `settle_completion` — so `replays` = `settled` + K.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inbound_completion_replays_the_callers_journal_once() -> Result<()> {
    const K: usize = 5;
    let _serial = SERIAL.lock().await;
    let node = common::start_single_process(4).await?;
    let caller_key = common::key_on_partition(&node, "Caller", 0, "caller");
    let mut steps = Vec::new();
    for i in 0..K {
        let key = common::key_on_partition(&node, "Far", 1, &format!("far-{i}"));
        steps.push(Step::call("Far", "done", Some(&key)));
    }
    steps.push(Step::output("done"));
    let sdk = FakeSdk::builder()
        .object("Caller", "seq", steps)
        .object("Far", "done", vec![Step::output("ok")])
        .spawn()
        .await?;
    let url = node.url();

    let result = common::within(TEST_DEADLINE, "deliver inbound completion", async {
        common::register(&url, &sdk.url).await?;
        let before = counts(&url).await?;
        let hits_before = warm(&url).await?[0];
        let (code, body) = common::send(&url, "Caller", "seq", Some(&caller_key), "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &id, "completed").await?;
        let after = counts(&url).await?;
        let hits = warm(&url).await?[0] - hits_before;
        let (settled, replays) = (after.0 - before.0, after.1 - before.1);
        ensure!(
            settled == 2 * K as u64,
            "each call delivers exactly two completions (id, result): {settled}"
        );
        ensure!(
            replays + hits == settled,
            "each delivered completion must take exactly one caller machine: {replays} loads + \
             {hits} warm machines for {settled} completions"
        );
        Ok(())
    })
    .await;

    node.shutdown().await;
    result
}

/// **Warm machine: the completions of one drain round load the caller's journal
/// once.**
///
/// Order:
/// 1. A lone node with 4 ranges. `Caller` (object, range 0) emits, in one turn,
///    `K` calls without waiting to `Near` (object, the **same** range 0, one key
///    per call), then suspends waiting for a signal. All `K` outbox `Call` rows
///    are in one commit.
/// 2. The next drain pass goes through the `K` rows in one round: each row
///    creates `Near` in place and delivers `CallInvocationId` to `Caller`
///    (`deliver_local`) — the drainer holds the engine for the whole round, so
///    no `Caller` turn slips between two deliveries. `Near` suspends forever, so
///    no result completion comes back.
///
/// Must see: `K` completions delivered, **one** journal load (`cold`), `K − 1`
/// uses of the warm machine (`hit`), no `stale`. Before the fix: `K` loads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settles_of_one_drain_round_replay_the_callers_journal_once() -> Result<()> {
    const K: usize = 8;
    let _serial = SERIAL.lock().await;
    let node = common::start_single_process(4).await?;
    let caller_key = common::key_on_partition(&node, "Caller", 0, "caller");
    let mut steps = Vec::new();
    for i in 0..K {
        let key = common::key_on_partition(&node, "Near", 0, &format!("near-{i}"));
        steps.push(Step::fire("Near", "hold", Some(&key)));
    }
    steps.push(Step::Suspend);
    let sdk = FakeSdk::builder()
        .object("Caller", "fan", steps)
        .object("Near", "hold", vec![Step::Suspend])
        .spawn()
        .await?;
    let url = node.url();

    let result = common::within(TEST_DEADLINE, "warm machine in one drain round", async {
        common::register(&url, &sdk.url).await?;
        let before = counts(&url).await?;
        let warm_before = warm(&url).await?;
        let (code, body) = common::send(&url, "Caller", "fan", Some(&caller_key), "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            if counts(&url).await?.0 - before.0 >= K as u64
                && sdk.starts_of("Near", "hold").len() == K
            {
                break;
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "not all {K} ids delivered yet"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let after = counts(&url).await?;
        let warm_after = warm(&url).await?;
        let (settled, replays) = (after.0 - before.0, after.1 - before.1);
        let d: Vec<u64> = warm_after
            .iter()
            .zip(&warm_before)
            .map(|(a, b)| a - b)
            .collect();
        ensure!(settled == K as u64, "{K} ids delivered to the caller: {settled}");
        ensure!(
            replays == 1,
            "{K} completions of one drain round must load the caller's journal once: {replays} loads \
             (hit/cold/stale/evict = {d:?})"
        );
        ensure!(
            d[..3] == [K as u64 - 1, 1, 0],
            "hit/cold/stale/evict = {d:?}"
        );
        Ok(())
    })
    .await;

    node.shutdown().await;
    result
}

/// `cairnflow_warm_machine_total` theo `[hit, cold, stale, evict]`.
async fn warm(url: &str) -> Result<[u64; 4]> {
    let mut out = [0; 4];
    for (i, o) in ["hit", "cold", "stale", "evict"].iter().enumerate() {
        out[i] = common::metric(
            url,
            &format!("cairnflow_warm_machine_total{{outcome=\"{o}\"}}"),
        )
        .await?;
    }
    Ok(out)
}

/// Number of journal entries of `id` (`GET /invocations/{id}/journal`).
async fn journal_len(url: &str, id: &str) -> Result<usize> {
    let v: serde_json::Value = reqwest::get(format!("{url}/invocations/{id}/journal"))
        .await?
        .json()
        .await?;
    Ok(v.as_array().map_or(0, Vec::len))
}

/// **Wake the caller only when its wait tree is satisfied.**
///
/// Order:
/// 1. A lone node with 4 ranges. `Fan` (service) emits `N` calls without
///    waiting to `Hold` (object, keys spread over all four ranges — delivered
///    both in place and via inbound), then `ctx.all` on all `N` results (an
///    `ALL_COMPLETED` tree). Each `Hold` suspends waiting for a signal.
/// 2. The test signals each `Hold`, far enough apart for one `Fan` turn to
///    finish in between: the results arrive scattered, each while `Fan` is
///    suspended.
///
/// Must see: `Fan` completes with **≤ 3** SDK turns (the emitting turn; the last
/// turn when the tree is satisfied; at most one more if an id arrives before the
/// first turn has finished suspending). Before the fix every result arriving
/// while `Fan` was suspended was a turn (`N + 1`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fanout_parent_runs_at_most_three_turns() -> Result<()> {
    use {common::fake_sdk::HOLD_SIGNAL, std::time::Duration};
    const N: usize = 6;
    let _serial = SERIAL.lock().await;
    let node = common::start_single_process(4).await?;
    let mut steps = Vec::new();
    for i in 0..N {
        let key = common::key_on_partition(&node, "Hold", (i % 4) as u32, &format!("hold-{i}"));
        steps.push(Step::fire("Hold", "wait", Some(&key)));
    }
    steps.push(Step::AwaitFired);
    steps.push(Step::output("done"));
    let sdk = FakeSdk::builder()
        .service("Fan", "out", steps)
        .object("Hold", "wait", vec![Step::Suspend, Step::output("ok")])
        .spawn()
        .await?;
    let url = node.url();

    let result = common::within(TEST_DEADLINE, "parent fanout", async {
        common::register(&url, &sdk.url).await?;
        let (code, body) = common::send(&url, "Fan", "out", None, "x").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        let t0 = tokio::time::Instant::now();
        let holds = loop {
            let mut ids: Vec<String> = sdk
                .starts_of("Hold", "wait")
                .iter()
                .map(|s| s.id_hex())
                .collect();
            ids.sort();
            ids.dedup();
            if ids.len() == N {
                break ids;
            }
            ensure!(t0.elapsed() < Duration::from_secs(20), "{N} Holds not up");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        common::wait_status(&url, &id, "suspended").await?;
        for h in &holds {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let resp = reqwest::Client::new()
                .post(format!("{url}/invocations/{h}/signal"))
                .json(&serde_json::json!({ "signal": HOLD_SIGNAL }))
                .send()
                .await?;
            ensure!(
                resp.status().is_success(),
                "signal {h}: {}",
                resp.text().await?
            );
        }
        common::wait_status(&url, &id, "completed").await?;
        let turns = sdk.starts_of("Fan", "out").len();
        ensure!(
            turns <= 3,
            "parent with {N} children ran {turns} SDK turns — a completion that does not \
             satisfy the tree must not wake it"
        );
        Ok(())
    })
    .await;

    node.shutdown().await;
    result
}

/// **The wait tree is lost between two completions (leader change), the caller
/// still completes.**
///
/// Interleaving:
/// 1. Node A leads four ranges. `Pair` (service) emits two calls to `Hold`
///    (object, two keys), then `ctx.all` on both results. Both `Hold`s suspend
///    waiting for a signal.
/// 2. Signal the first `Hold` ⇒ its result comes back to `Pair`. A's engine
///    remembers the tree (`ALL` of two completions), knows for sure it is not
///    satisfied ⇒ records a notification, keeps `suspended`, does **not**
///    schedule a turn.
/// 3. A stops (RAM lost: wait tree, warm machine). Node B comes up and leads the
///    four ranges.
/// 4. Signal the second `Hold` ⇒ its result comes back to `Pair` on B: no tree
///    ⇒ "unknown" ⇒ wake it, as before the wait-tree check.
///
/// Must see: after step 2 `Pair` is still `suspended`, its journal grew, no new
/// SDK turn; after step 4 `Pair` completes (seeing both results). Before the
/// fix step 2 woke `Pair` for one turn (it suspended again).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parent_finishes_after_losing_its_waiting_tree_between_two_completions() -> Result<()> {
    use {common::fake_sdk::HOLD_SIGNAL, std::time::Duration};
    let _serial = SERIAL.lock().await;
    let bucket = common::bucket();
    let start = |id: &'static str| {
        let bucket = &bucket;
        async move {
            let n = common::start_cluster_node(bucket, id, 4, Duration::from_secs(2)).await?;
            common::wait_probe(
                &[&n],
                Duration::from_secs(20),
                "node leads every range",
                |p| p.len() == 4 && p.values().all(|v| v == &[0]),
            )
            .await?;
            Ok::<_, anyhow::Error>(n)
        }
    };
    let sdk = FakeSdk::builder()
        .service(
            "Pair",
            "both",
            vec![
                Step::fire("Hold", "wait", Some("k1")),
                Step::fire("Hold", "wait", Some("k2")),
                Step::AwaitFired,
                Step::output("done"),
            ],
        )
        .object("Hold", "wait", vec![Step::Suspend, Step::output("ok")])
        .spawn()
        .await?;
    let signal = |url: String, id: String| async move {
        let resp = reqwest::Client::new()
            .post(format!("{url}/invocations/{id}/signal"))
            .json(&serde_json::json!({ "signal": HOLD_SIGNAL }))
            .send()
            .await?;
        ensure!(
            resp.status().is_success(),
            "signal {id}: {}",
            resp.text().await?
        );
        Ok(())
    };
    let holds = |sdk: &FakeSdk| -> Vec<String> {
        let mut ids: Vec<String> = sdk
            .starts_of("Hold", "wait")
            .iter()
            .map(|s| s.id_hex())
            .collect();
        ids.sort();
        ids.dedup();
        ids
    };

    // 1–2.
    let a = start("node-a").await?;
    let url = a.node.url();
    let r = common::within(
        Duration::from_secs(60),
        "first result delivered to Pair on node A",
        async {
            common::register(&url, &sdk.url).await?;
            let (code, body) = common::send(&url, "Pair", "both", None, "x").await?;
            ensure!(code == 200 || code == 202, "{code} {body}");
            let pair = body["id"].as_str().unwrap_or_default().to_string();
            let t0 = tokio::time::Instant::now();
            while holds(&sdk).len() < 2 {
                ensure!(t0.elapsed() < Duration::from_secs(20), "both Holds not up");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            common::wait_status(&url, &pair, "suspended").await?;
            // Both ids are back (4 entries: input, two Calls, …): wait for the journal to settle.
            let t0 = tokio::time::Instant::now();
            while journal_len(&url, &pair).await? < 5 {
                ensure!(t0.elapsed() < Duration::from_secs(20), "ids not at Pair");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            common::wait_status(&url, &pair, "suspended").await?;
            let before = (
                journal_len(&url, &pair).await?,
                sdk.starts_of("Pair", "both").len(),
            );
            let first = holds(&sdk)[0].clone();
            signal(url.clone(), first).await?;
            let t0 = tokio::time::Instant::now();
            while journal_len(&url, &pair).await? <= before.0 {
                ensure!(
                    t0.elapsed() < Duration::from_secs(20),
                    "first result not back yet"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            let (_, st) = common::invocation(&url, &pair).await?;
            ensure!(
                st["status"] == "suspended",
                "tree not satisfied, yet Pair is not suspended: {st}"
            );
            let turns = sdk.starts_of("Pair", "both").len();
            ensure!(
                turns == before.1,
                "the first result does not satisfy the ALL tree, yet Pair ran {} more turns",
                turns - before.1
            );
            Ok(pair)
        },
    )
    .await;
    a.node.shutdown().await;
    let pair = r?;

    // 3–4.
    let b = start("node-b").await?;
    let url = b.node.url();
    let r = common::within(Duration::from_secs(60), "second result on B", async {
        let second = holds(&sdk)[1].clone();
        signal(url.clone(), second).await?;
        let st = common::wait_status(&url, &pair, "completed").await?;
        ensure!(st["output"] == "done", "{st}");
        Ok(())
    })
    .await;
    b.node.shutdown().await;
    r
}
