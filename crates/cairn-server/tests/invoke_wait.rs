// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `/invoke` waits for the outcome: the final answer comes back as soon as the outcome
//! commit lands, without waiting for the polling grid's tick; the waiter book does not leak.
//!
//! The polling grid is thinned out with `NodeConfig::invoke_poll_floor` (test only):
//! a 10 s grid, a 5 s wait limit ⇒ the grid's first poll falls exactly on the deadline. An
//! answer arriving before the deadline can only be thanks to the outcome notifier.

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
    cairn_server::node::RunningNode,
    common::fake_sdk::{FakeSdk, Step},
    std::time::{Duration, Instant},
};

/// The `/invoke` wait limit in this file.
const WAIT: Duration = Duration::from_secs(5);
/// The polling grid floor: sparser than the wait limit.
const GRID: Duration = Duration::from_secs(10);
/// An "early" answer must arrive before this mark — the SDK finishes after ~200 ms, the
/// deadline is at 5 s: a margin wide enough for a busy machine without being confused with the poll at the deadline.
const EARLY: Duration = Duration::from_secs(4);

/// A one-range node, sparse polling grid [`GRID`], wait limit [`WAIT`].
async fn node_with_sparse_grid(partitions: u16) -> Result<RunningNode> {
    let mut cfg = common::solo_config(partitions);
    cfg.invoke_wait = Some(WAIT);
    cfg.invoke_poll_floor = Some(GRID);
    common::start_led(cfg).await
}

/// The handler does `ctx.sleep(200)` then returns a result — the first turn suspends (the
/// client turn returns `Waiting`, `/invoke` enters its wait loop), the next turn is run by
/// the timer and completes; polling grid 10 s, wait limit 5 s.
///
/// Must see: `/invoke` returns `200` with the result **before** the deadline — the notice
/// of the outcome commit makes the poll happen at the right moment. Without the notifier
/// the first poll was the one at the deadline (5 s).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn final_answer_arrives_without_waiting_for_the_poll_grid() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let sdk = FakeSdk::builder()
        .service("Svc", "slow", vec![Step::Sleep(200), Step::output("done")])
        .spawn()
        .await?;
    let node = node_with_sparse_grid(1).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(30), "invoke with sparse grid", async {
        common::register(&url, &sdk.url).await?;
        let t0 = Instant::now();
        let (code, body) = common::invoke(&url, "Svc", "slow", None, "{}").await?;
        let took = t0.elapsed();
        ensure!(code == 200 && body["output"] == "done", "{code} {body}");
        ensure!(
            took < EARLY,
            "the answer came after {took:?} — waited for the polling grid/deadline {WAIT:?}, not the outcome notifier"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// The waiter gauge belongs to the whole process: the tests reading it run alone in
/// this binary (`cargo test` runs the tests of one binary in parallel).
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Waits (bounded) until no waiter is left: the node's gauge and the book of
/// every current range.
async fn waiters_drain(node: &RunningNode, what: &str) -> Result<()> {
    let t0 = Instant::now();
    loop {
        let gauge = cairn_server::metrics::floor::waiters();
        let books: usize = node.partitions.iter().map(|(_, h)| h.waiters().len()).sum();
        if gauge == 0 && books == 0 {
            return Ok(());
        }
        ensure!(
            t0.elapsed() < Duration::from_secs(10),
            "{what}: {gauge} waiters left (gauge), {books} entries in the range books — leak"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// `/invoke` for `n` parallel calls to `Hold/wait` (waits for a signal), one key per
/// call — returns the HTTP code of each call.
async fn hold_calls(url: &str, n: usize, client: reqwest::Client) -> Vec<Result<u16>> {
    let calls = (0..n).map(|i| {
        let client = client.clone();
        let url = format!("{url}/invoke/Hold/wait?key=k-{i}");
        async move {
            let r = client.post(url).body("{}").send().await?;
            Ok(r.status().as_u16())
        }
    });
    futures::future::join_all(calls).await
}

/// **Waiters do not leak on any exit path.** 50 calls of each kind:
/// completed; limit reached (`202`); client disconnects midway; range split while waiting.
///
/// Must see: after each kind, the `cairnflow_invoke_waiters` gauge and the book of every
/// range are back to 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiters_are_released_on_every_exit() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let sdk = FakeSdk::builder()
        .service("Svc", "quick", vec![Step::output("ok")])
        .object("Hold", "wait", vec![Step::Suspend, Step::output("freed")])
        .spawn()
        .await?;
    let mut cfg = common::solo_config(1);
    cfg.invoke_wait = Some(Duration::from_millis(500));
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(90), "waiters do not leak", async {
        common::register(&url, &sdk.url).await?;
        // Xong.
        let done = (0..50).map(|_| common::invoke(&url, "Svc", "quick", None, "{}"));
        for r in futures::future::join_all(done).await {
            let (code, body) = r?;
            ensure!(code == 200 && body["output"] == "ok", "{code} {body}");
        }
        waiters_drain(&node, "after 50 completed calls").await?;
        // Limit reached.
        for code in hold_calls(&url, 50, reqwest::Client::new()).await {
            ensure!(code? == 202, "held call: 202 at the deadline");
        }
        waiters_drain(&node, "after 50 calls hitting the limit").await?;
        // Client disconnects midway.
        let impatient = reqwest::Client::builder()
            .timeout(Duration::from_millis(150))
            .build()?;
        for r in hold_calls(&url, 50, impatient).await {
            ensure!(
                r.is_err(),
                "client disconnected after 150 ms yet got an answer: {r:?}"
            );
        }
        waiters_drain(&node, "after 50 client disconnects").await?;
        Ok(())
    })
    .await;
    node.shutdown().await;
    r?;

    // Range split while waiting: a longer limit so the split falls mid-wait.
    let mut cfg = common::solo_config(1);
    cfg.invoke_wait = Some(Duration::from_secs(3));
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(90), "waiters across a split", async {
        common::register(&url, &sdk.url).await?;
        let waiting = tokio::spawn({
            let url = url.clone();
            async move { hold_calls(&url, 50, reqwest::Client::new()).await }
        });
        let t0 = Instant::now();
        while cairn_server::metrics::floor::waiters() < 50 {
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "50 waiters not yet registered"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let split = reqwest::Client::new()
            .post(format!("{url}/ranges/0/split"))
            .send()
            .await?;
        ensure!(
            split.status().is_success(),
            "split: {}",
            split.text().await?
        );
        for code in waiting.await? {
            ensure!(code? == 202, "held call across split: 202 at deadline");
        }
        waiters_drain(&node, "after 50 calls across a split").await?;
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// Calls `/invoke` (or any `url` returning `InvokeResponse`) and requires the answer to
/// arrive **before** [`EARLY`] — i.e. through the notifier, not the grid (10 s) or the deadline (5 s).
async fn answered_early(req: reqwest::RequestBuilder, what: &str) -> Result<serde_json::Value> {
    let t0 = Instant::now();
    let resp = req.send().await?;
    let code = resp.status().as_u16();
    let body: serde_json::Value = resp.json().await?;
    let took = t0.elapsed();
    ensure!(
        took < EARLY,
        "{what}: the answer ({code} {body}) came after {took:?} — the outcome did not wake the waiter"
    );
    Ok(body)
}

/// **Every outcome path wakes the waiters** (grid 10 s, limit 5 s):
/// `failed` from running out of attempts with `Kill`, `paused` 575, admin cancel, cascading
/// cancel from the parent (the waiter is the `attach` of a child workflow), `continueAsNew`.
/// `completed` is covered by the first test. `paused` 577 (the rescue net) goes through the same
/// status write (`Mutations::set_invocation_status`) — the notification point is one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_ending_wakes_the_waiter() -> Result<()> {
    use cairn_server::discovery::{Contract, OnMaxAttempts};
    let _serial = SERIAL.lock().await;
    let once = |on_max| Contract {
        retry_policy_max_attempts: Some(1),
        retry_policy_on_max_attempts: Some(on_max),
        ..Default::default()
    };
    let sdk = FakeSdk::builder()
        .service("Fail", "kill", vec![Step::FailRetry])
        .contract("Fail", "kill", once(OnMaxAttempts::Kill))
        .service("Fail", "pause", vec![Step::FailRetry])
        .contract("Fail", "pause", once(OnMaxAttempts::Pause))
        .object("Hold", "wait", vec![Step::Suspend, Step::output("freed")])
        .object(
            "Parent",
            "run",
            vec![
                Step::call("Child", "run", Some("child")),
                Step::EchoCallResult,
            ],
        )
        .workflow("Child", "run", vec![Step::Suspend, Step::output("child")])
        .object(
            "Loop",
            "run",
            vec![
                Step::ContinueAsNewOnce(
                    std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                    bytes::Bytes::from_static(b"{}"),
                ),
                Step::output("round"),
            ],
        )
        .spawn()
        .await?;
    let node = node_with_sparse_grid(1).await?;
    let url = node.url();
    let http = reqwest::Client::new();
    let r = common::within(Duration::from_secs(90), "every outcome path", async {
        common::register(&url, &sdk.url).await?;
        // `Kill` when attempts run out ⇒ `failed`.
        let b = answered_early(
            http.post(format!("{url}/invoke/Fail/kill")).body("{}"),
            "Kill",
        )
        .await?;
        ensure!(b["status"] == "failed", "{b}");
        // `Pause` when attempts run out ⇒ `paused` 575.
        let b = answered_early(
            http.post(format!("{url}/invoke/Fail/pause")).body("{}"),
            "paused 575",
        )
        .await?;
        ensure!(b["status"] == "paused" && b["code"] == 575, "{b}");
        // Admin cancel.
        let waiting = tokio::spawn(answered_early(
            http.post(format!("{url}/invoke/Hold/wait?key=cancel"))
                .body("{}"),
            "admin cancel",
        ));
        let t0 = Instant::now();
        while sdk.starts_of("Hold", "wait").is_empty() {
            ensure!(t0.elapsed() < Duration::from_secs(3), "Hold/wait not run");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let id = sdk.starts_of("Hold", "wait")[0].id_hex();
        let c = http
            .post(format!("{url}/invocations/{id}/cancel"))
            .send()
            .await?;
        ensure!(c.status().is_success(), "cancel: {}", c.text().await?);
        let b = waiting.await??;
        ensure!(b["status"] == "failed", "{b}");
        // Cascading cancel: the parent calls a child workflow; the waiter is the child's `attach`.
        let (code, body) = common::send(&url, "Parent", "run", Some("parent"), "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let parent = body["id"].as_str().unwrap_or_default().to_string();
        let t0 = Instant::now();
        while sdk.starts_of("Child", "run").is_empty() {
            ensure!(
                t0.elapsed() < Duration::from_secs(3),
                "child workflow not running yet"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let attached = tokio::spawn(answered_early(
            http.post(format!("{url}/workflow/Child/child/attach")),
            "cascading cancel",
        ));
        let t0 = Instant::now();
        while cairn_server::metrics::floor::waiters() < 1 {
            ensure!(
                t0.elapsed() < Duration::from_secs(3),
                "attach has not registered its wait"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let c = http
            .post(format!("{url}/invocations/{parent}/cancel"))
            .send()
            .await?;
        ensure!(c.status().is_success(), "kill parent: {}", c.text().await?);
        let b = attached.await??;
        ensure!(b["status"] == "failed", "cascade cancels child: {b}");
        // `continueAsNew`: the old run ends.
        let b = answered_early(
            http.post(format!("{url}/invoke/Loop/run?key=v")).body("{}"),
            "continueAsNew",
        )
        .await?;
        ensure!(b["status"] != "waiting", "{b}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **A workflow's `attach` takes the same wait path.** The workflow runs 200 ms;
/// `attach` (grid 10 s, limit 5 s) returns `completed` before the deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflow_attach_wakes_on_completion() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let sdk = FakeSdk::builder()
        .workflow("Wf", "run", vec![Step::Stall(200), Step::output("wf")])
        .spawn()
        .await?;
    let node = node_with_sparse_grid(1).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(30), "attach", async {
        common::register(&url, &sdk.url).await?;
        let (code, body) = common::send(&url, "Wf", "run", Some("w"), "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let b = answered_early(
            reqwest::Client::new().post(format!("{url}/workflow/Wf/w/attach")),
            "attach",
        )
        .await?;
        ensure!(b["status"] == "completed" && b["output"] == "wf", "{b}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **Leadership lost while waiting**: when it stops serving, the leader wakes every
/// waiter of its book; they ask again instead of sleeping through the sparse grid tick.
///
/// 1. Two nodes, one range; alpha leads, beta waits. Sparse grid 10 s (floor
///    `invoke_poll_floor`), wait limit 20 s.
/// 2. `/invoke` to alpha: `Slow/go` sleeps 1.5 s then returns a result — the first turn
///    suspends, the request enters the wait loop in alpha's book.
/// 3. alpha `resign`s (answers after yielding); then every owner-record PUT
///    (`owners/`) of alpha fails — beta claims the range, alpha does not claim it back.
///
/// Must see: `/invoke` answers before 8 s — `202` (the leader moved to beta) or
/// `200` —, not waiting for the 10 s grid tick; the call completes on beta; the books of
/// every range on both nodes and the waiter gauge return to 0. Before this was fixed the
/// waiters slept through the 10 s tick.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiter_wakes_and_reasks_when_its_leader_steps_down() -> Result<()> {
    use cairn_store::testkit::{Action, OpSet, PathMatch, Rule};
    let _serial = SERIAL.lock().await;
    let sdk = FakeSdk::builder()
        .service("Slow", "go", vec![Step::Sleep(1500), Step::output("late")])
        .spawn()
        .await?;
    let shared = common::bucket();
    let tweak = |c: &mut cairn_server::node::NodeConfig| {
        c.invoke_wait = Some(Duration::from_secs(20));
        c.invoke_poll_floor = Some(GRID);
    };
    let ttl = Duration::from_secs(2);
    let alpha = common::start_cluster_node_tweaked(&shared, "node-alpha", 1, ttl, tweak).await?;
    let r = common::within(
        Duration::from_secs(90),
        "leadership lost while waiting",
        async {
            common::wait_probe(
                &[&alpha],
                Duration::from_secs(20),
                "alpha leads",
                |p| common::single_leaders(p).is_some(),
            )
            .await?;
            let beta =
                common::start_cluster_node_tweaked(&shared, "node-beta", 1, ttl, tweak).await?;
            for n in [&alpha, &beta] {
                common::register(&n.node.url(), &sdk.url).await?;
            }
            let url = alpha.node.url();
            let call = tokio::spawn(async move {
                let t0 = Instant::now();
                let r = common::invoke(&url, "Slow", "go", None, "{}").await;
                (t0.elapsed(), r)
            });
            // 2. Wait for the first turn to **finish answering** (invocation `suspended` in
            // `ctx.sleep`) — the request has entered the wait loop in alpha's book. Waiting
            // only for the waiter to register is not enough: the waiter registers **before**
            // the first question, so `resign` could land mid first turn, and then `/invoke`
            // returns `502 "engine stopped"` with the id per the contract (`admin-api.md`)
            // instead of going through the wait loop — the test flaked (≈ 5/9 on
            // `main`).
            let t0 = Instant::now();
            let id = loop {
                if let Some(s) = sdk.starts_of("Slow", "go").first() {
                    break s.id_hex();
                }
                ensure!(
                    t0.elapsed() < Duration::from_secs(10),
                    "first turn did not reach the SDK"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            common::within(
                Duration::from_secs(10),
                "first turn suspended in ctx.sleep",
                common::wait_status(&alpha.node.url(), &id, "suspended"),
            )
            .await?;
            ensure!(
                alpha
                    .node
                    .partitions
                    .iter()
                    .map(|(_, h)| h.waiters().len())
                    .sum::<usize>()
                    > 0,
                "the request did not enter the wait loop"
            );
            // 3.
            let (_, h) = alpha.node.partitions.iter().next().expect("a range");
            h.resign()
                .await
                .map_err(|e| anyhow::anyhow!("resign: {e}"))?;
            alpha.store.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Contains("owners/".into()),
                Action::Fail { times: None },
            ));
            let (took, r) = call.await?;
            let (code, body) = r?;
            ensure!(
                took < Duration::from_secs(8),
                "/invoke returned {code} after {took:?} — the waiter slept through the grid tick {GRID:?}"
            );
            ensure!(code == 200 || code == 202, "{code} {body}");
            if code == 202 {
                let id = body["id"].as_str().unwrap_or_default().to_string();
                let st = common::wait_status(&beta.node.url(), &id, "completed").await?;
                ensure!(st["output"] == "late", "{st}");
            } else {
                ensure!(body["output"] == "late", "{body}");
            }
            waiters_drain(&alpha.node, "alpha").await?;
            waiters_drain(&beta.node, "beta").await?;
            beta.node.shutdown().await;
            Ok(())
        },
    )
    .await;
    alpha.node.shutdown().await;
    r
}

/// **Pinned:** `GET /invocations/{id}` returns at once, never
/// long-polls — even with `?wait_ms=`. A `Hold/wait` invocation waits for a
/// signal (sitting in `suspended`); read its status with `wait_ms=5000`.
///
/// Must see: `200 suspended` within < 1 s. The wait-by-id door is attach;
/// reading the status stays an instantaneous question.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_read_never_long_polls() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let sdk = FakeSdk::builder()
        .object("Hold", "wait", vec![Step::Suspend, Step::output("freed")])
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let r = common::within(
        Duration::from_secs(30),
        "status read does not wait",
        async {
            common::register(&url, &sdk.url).await?;
            let (code, body) = common::send(&url, "Hold", "wait", Some("k"), "{}").await?;
            ensure!(code == 202, "{code} {body}");
            let id = body["id"].as_str().unwrap_or_default().to_string();
            common::wait_status(&url, &id, "suspended").await?;
            let t0 = Instant::now();
            let r = reqwest::get(format!("{url}/invocations/{id}?wait_ms=5000")).await?;
            let took = t0.elapsed();
            ensure!(r.status() == 200, "{}", r.status());
            let v: serde_json::Value = r.json().await?;
            ensure!(v["status"] == "suspended", "{v}");
            ensure!(
                took < Duration::from_secs(1),
                "GET /invocations/{{id}} waited {took:?}"
            );
            Ok(())
        },
    )
    .await;
    node.shutdown().await;
    r
}

/// `POST url` (body `{}`), returns the code, the `x-cairn-invocation-id` header, the JSON body, the time.
async fn post_timed(url: String) -> Result<(u16, Option<String>, serde_json::Value, Duration)> {
    let t0 = Instant::now();
    let r = reqwest::Client::new().post(url).body("{}").send().await?;
    let code = r.status().as_u16();
    let header = r
        .headers()
        .get("x-cairn-invocation-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let v: serde_json::Value = r.json().await.unwrap_or_default();
    Ok((code, header, v, t0.elapsed()))
}

/// **Attach by id (CG-2).** A `Hold/wait` invocation waits
/// for a signal.
///
/// 1. `attach?wait_ms=200` ⇒ `202 waiting` after ~200 ms (not the 5 s of
///    `--invoke-wait-ms`), header `x-cairn-invocation-id` = id.
/// 2. `attach?wait_ms=10000` is waiting; the signal arrives ⇒ the same request returns `200
///    completed` with the output, before the deadline.
/// 3. `attach` again ⇒ `200` at once. An unknown id ⇒ `404`.
///
/// Must see: attach returns exactly the durable result; `/invoke` and `/send` also carry
/// the `x-cairn-invocation-id` header.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn attach_by_id_waits_for_the_durable_result() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let sdk = FakeSdk::builder()
        .service("Svc", "quick", vec![Step::output("ok")])
        .object("Hold", "wait", vec![Step::Suspend, Step::output("freed")])
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "attach by id", async {
        common::register(&url, &sdk.url).await?;
        let (code, h, body, _) = post_timed(format!("{url}/invoke/Svc/quick")).await?;
        ensure!(
            code == 200 && h.as_deref() == body["id"].as_str(),
            "/invoke: {code} {h:?} {body}"
        );
        let (code, h, body, _) = post_timed(format!("{url}/invoke/Hold/wait/send?key=k")).await?;
        ensure!(
            code == 202 && h.as_deref() == body["id"].as_str(),
            "/send: {code} {h:?} {body}"
        );
        let id = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &id, "suspended").await?;

        // (1)
        let (code, h, v, took) =
            post_timed(format!("{url}/invocations/{id}/attach?wait_ms=200")).await?;
        ensure!(
            code == 202 && v["status"] == "waiting" && v["id"] == id.as_str(),
            "{code} {v}"
        );
        ensure!(h.as_deref() == Some(id.as_str()), "header {h:?}");
        ensure!(
            took >= Duration::from_millis(150) && took < Duration::from_secs(2),
            "attach wait_ms=200 took {took:?}"
        );
        // (2)
        let waiting = tokio::spawn(post_timed(format!(
            "{url}/invocations/{id}/attach?wait_ms=10000"
        )));
        tokio::time::sleep(Duration::from_millis(300)).await;
        let r = reqwest::Client::new()
            .post(format!("{url}/invocations/{id}/signal"))
            .json(&serde_json::json!({ "signal": common::fake_sdk::HOLD_SIGNAL }))
            .send()
            .await?;
        ensure!(r.status() == 200, "signal {}", r.status());
        let (code, _, v, took) = waiting.await??;
        ensure!(
            code == 200 && v["status"] == "completed" && v["output"] == "freed",
            "{code} {v}"
        );
        ensure!(
            took < Duration::from_secs(8),
            "attach did not wake on completion: {took:?}"
        );
        // (3)
        let (code, _, v, took) = post_timed(format!("{url}/invocations/{id}/attach")).await?;
        ensure!(
            code == 200 && v["output"] == "freed" && took < Duration::from_secs(1),
            "{code} {v}"
        );
        let unknown = "00000000000000000123456789abcdef0123456789abcdef";
        let (code, _, v, _) = post_timed(format!("{url}/invocations/{unknown}/attach")).await?;
        ensure!(code == 404, "unknown id: {code} {v}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **The `--attach-max-wait-ms` limit.** Limit 300 ms; a client
/// asks for `wait_ms=10000` ⇒ `202` after ~300 ms, not 10 s. `/workflow/…/attach`
/// accepts `wait_ms` too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn attach_wait_is_capped_by_the_node() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let sdk = FakeSdk::builder()
        .object("Hold", "wait", vec![Step::Suspend, Step::output("freed")])
        .workflow("Flow", "run", vec![Step::Suspend, Step::output("done")])
        .spawn()
        .await?;
    let mut cfg = common::solo_config(1);
    cfg.attach_max_wait = Duration::from_millis(300);
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "attach limit", async {
        common::register(&url, &sdk.url).await?;
        let (_, _, body, _) = post_timed(format!("{url}/invoke/Hold/wait/send?key=k")).await?;
        let id = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &id, "suspended").await?;
        let (code, _, v, took) =
            post_timed(format!("{url}/invocations/{id}/attach?wait_ms=10000")).await?;
        ensure!(code == 202, "{code} {v}");
        ensure!(took < Duration::from_secs(2), "300 ms cap, waited {took:?}");
        let (code, _, v, _) =
            post_timed(format!("{url}/invocations/{id}/attach?wait_ms=abc")).await?;
        ensure!(code == 400, "wrong wait_ms: {code} {v}");

        let (_, _, body, _) = post_timed(format!("{url}/invoke/Flow/run/send?key=w")).await?;
        let wid = body["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &wid, "suspended").await?;
        let (code, h, v, took) =
            post_timed(format!("{url}/workflow/Flow/w/attach?wait_ms=100")).await?;
        ensure!(
            code == 202 && took < Duration::from_secs(2),
            "{code} {v} {took:?}"
        );
        ensure!(h.as_deref() == Some(wid.as_str()), "header {h:?}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
