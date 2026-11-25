// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Quarantining a poison handler — the adversarial test and the
//! removal paths.
//!
//! The poison handler here is `Poison/boom`: every turn fails retryably
//! (`Step::FailRetry`), so its invocations never complete but keep retrying
//! with backoff. The healthy handler is `Healthy/ok` in the same tenant.

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
    cairn_server::node::{self, NodeConfig},
    common::{
        fake_sdk::{FakeSdk, Step},
        invoke, load_of, register, send, single_leaders, solo_config, start_cluster_node_tweaked,
        wait_probe, within, TEST_DEADLINE,
    },
    reqwest::Client,
    serde_json::Value,
    std::time::{Duration, Instant},
};

/// The code of an invocation held by quarantine (`hold`).
const QUARANTINED: u64 = 576;

fn quick(cfg: &mut NodeConfig) {
    // The deployment registry, tenants and the quarantine list spread between nodes at this cadence.
    cfg.deploy_refresh = Duration::from_millis(200);
}

async fn solo() -> Result<node::RunningNode> {
    let mut cfg = solo_config(2);
    quick(&mut cfg);
    common::start_led(cfg).await
}

async fn put_tenant(url: &str, body: Value) -> Result<()> {
    let r = Client::new()
        .put(format!("{url}/admin/tenants/default"))
        .json(&body)
        .send()
        .await?;
    let code = r.status();
    let text = r.text().await?;
    ensure!(
        code.is_success(),
        "PUT /admin/tenants/default {body}: {code} {text}"
    );
    Ok(())
}

/// Asks `GET /admin/tenants` on `url` until tenant `default` has quarantine mode `hold`.
/// That route re-reads the tenant settings from the bucket into the node's copy before
/// answering, so a `hold` here is the node's own view.
async fn wait_tenant_hold(url: &str) -> Result<()> {
    within(
        Duration::from_secs(40),
        "the node sees quarantine=hold",
        async {
            loop {
                let r = Client::new()
                    .get(format!("{url}/admin/tenants"))
                    .send()
                    .await?;
                let code = r.status();
                let v: Value = r.json().await?;
                ensure!(code.is_success(), "GET /admin/tenants: {code} {v}");
                if v["quarantine"]["default"]["mode"] == "hold" {
                    return Ok(());
                }
                // A polling tick inside a bounded loop (AGENTS.md §6.3).
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
    )
    .await
}

/// The entry of `Poison/boom` in `GET /admin/quarantine` (read fresh from the bucket).
async fn entry(url: &str) -> Result<Option<Value>> {
    let r = Client::new()
        .get(format!("{url}/admin/quarantine"))
        .send()
        .await?;
    let code = r.status();
    let v: Value = r.json().await.unwrap_or(Value::Null);
    ensure!(code.is_success(), "GET /admin/quarantine: {code} {v}");
    Ok(v["entries"].as_array().and_then(|a| {
        a.iter()
            .find(|e| e["service"] == "Poison" && e["handler"] == "boom")
            .cloned()
    }))
}

/// Waits until `Poison/boom` has an entry with action `action`.
async fn wait_entry(url: &str, action: &str) -> Result<Value> {
    loop {
        if let Some(e) = entry(url).await? {
            if e["action"] == action {
                return Ok(e);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Calls `/invoke` (or `/send`) like a client inside the cluster: a receiving node that does not
/// lead the id's range returns `503` with `leader` — call again there. An unkeyed service
/// gets a random partition key on every call, so the retried call may miss
/// again (½ each time with two nodes): try up to 32 times.
async fn call(url: &str, service: &str, handler: &str, send_only: bool) -> Result<(u16, Value)> {
    let mut url = url.to_string();
    for _ in 0..32 {
        let (code, r) = if send_only {
            send(&url, service, handler, None, "{}").await?
        } else {
            invoke(&url, service, handler, None, "{}").await?
        };
        match r["leader"].as_str() {
            Some(leader) if code == 503 => url = leader.to_string(),
            _ => return Ok((code, r)),
        }
    }
    Err(anyhow!("could not find a leader for {service}/{handler}"))
}

/// The status of `id` from whichever node answers `200` first (the leader, or a
/// follower that can read it).
async fn status_any(urls: &[String], id: &str) -> Result<Value> {
    loop {
        for u in urls {
            let (code, st) = common::invocation(u, id).await?;
            if code == 200 {
                return Ok(st);
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn send_poison(url: &str, n: usize) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    for _ in 0..n {
        let (code, r) = call(url, "Poison", "boom", true).await?;
        ensure!(code == 202 || code == 200, "send Poison: {code} {r}");
        ids.push(
            r["id"]
                .as_str()
                .ok_or_else(|| anyhow!("missing id: {r}"))?
                .to_string(),
        );
    }
    Ok(ids)
}

fn p99(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v.get(((v.len() as f64) * 0.99).ceil() as usize - 1)
        .copied()
        .unwrap_or_default()
}

async fn healthy_latencies(urls: &[String], n: usize) -> Result<Vec<Duration>> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let t0 = Instant::now();
        let (code, r) = call(&urls[i % urls.len()], "Healthy", "ok", false).await?;
        ensure!(
            code == 200 && r["status"] == "completed",
            "Healthy/ok: {code} {r}"
        );
        out.push(t0.elapsed());
    }
    Ok(out)
}

/// **The adversarial test.** Two nodes, four ranges; `Poison/boom` throws on every
/// turn; tenant `default` sets `quarantine=hold` (`hold` is enabled per tenant).
///
/// Order:
/// 1. measure `Healthy/ok`'s p99 while quiet (the baseline);
/// 2. send up to 50 `Poison/boom` invocations alternating between the two nodes — they land on
///    all four ranges, both nodes; every turn fails and retries with backoff. If the rule fires
///    midway (409/576), stop sending;
/// 3. in parallel, call `Healthy/ok` steadily.
///
/// Must see:
/// - `Poison/boom` enters the list with `hold` **within one rule window** (5 minutes;
///   in practice a few seconds);
/// - once both nodes have read the list, its **new** invocations through
///   **each** node return `paused` with code 576 — no SDK turn runs for them;
/// - **unfinished** invocations are neither cancelled nor `failed`;
/// - `/stats/handlers?scope=cluster` marks its row `quarantine: hold`;
/// - `Healthy/ok`'s p99 during the storm is no more than `3 × baseline + 100 ms`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poison_handler_is_held_across_two_nodes_without_hurting_others() -> Result<()> {
    let shared = common::bucket();
    let ttl = Duration::from_secs(2);
    let nodes = vec![
        start_cluster_node_tweaked(&shared, "q-alpha", 4, ttl, quick).await?,
        start_cluster_node_tweaked(&shared, "q-beta", 4, ttl, quick).await?,
    ];
    let refs: Vec<_> = nodes.iter().collect();
    wait_probe(
        &refs,
        Duration::from_secs(30),
        "each node leads two ranges",
        |p| single_leaders(p).is_some_and(|m| load_of(&m, 2) == [2, 2]),
    )
    .await?;
    let urls: Vec<String> = nodes.iter().map(|n| n.node.url()).collect();
    let sdk = FakeSdk::builder()
        .service("Poison", "boom", vec![Step::FailRetry])
        .service("Healthy", "ok", vec![Step::output("ok")])
        .spawn()
        .await?;
    let r = within(Duration::from_secs(120), "adversarial", async {
        register(&urls[0], &sdk.url).await?;
        put_tenant(&urls[0], serde_json::json!({ "quarantine": "hold" })).await?;
        // Registered and set on node 0 only: wait until both nodes know the deployment
        // and the tenant setting before sending to node 1.
        common::wait_services_known(&urls, &["Poison", "Healthy"]).await?;
        for url in &urls {
            wait_tenant_hold(url).await?;
        }

        let base = p99(healthy_latencies(&urls, 100).await?);

        let t0 = Instant::now();
        // The rule (20 failures over ≥ 5 invocations) may fire **while** still
        // sending — fast retries, a slow machine: a send meeting 409/576 is exactly the
        // behaviour to see for new invocations, so stop sending there. The test used to
        // require all 50 sends and failed on slow machines.
        let mut first = Vec::new();
        for i in 0..50 {
            let (code, r) = call(&urls[i % 2], "Poison", "boom", true).await?;
            if code == 409 && r["code"] == QUARANTINED {
                break;
            }
            ensure!(code == 202 || code == 200, "send Poison: {code} {r}");
            first.push(
                r["id"]
                    .as_str()
                    .ok_or_else(|| anyhow!("missing id: {r}"))?
                    .to_string(),
            );
        }
        ensure!(
            first.len() >= 5,
            "the rule needs ≥ 5 failing invocations to fire, only {} sent",
            first.len()
        );
        let loaded = {
            let urls = urls.clone();
            tokio::spawn(async move { healthy_latencies(&urls, 150).await })
        };
        let e = within(Duration::from_secs(60), "listed", wait_entry(&urls[1], "hold")).await?;
        let listed_after = t0.elapsed();
        ensure!(listed_after < Duration::from_secs(300), "listed after {listed_after:?} — over one window");
        ensure!(e["deployment"].as_str().is_some_and(|d| !d.is_empty()), "the identity carries the deployment: {e}");

        // Both nodes re-read the list at the refresh cadence (200 ms).
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let started_before = sdk.starts_of("Poison", "boom").len();
        let mut held = Vec::new();
        for i in 0..8 {
            let (code, r) = call(&urls[i % 2], "Poison", "boom", false).await?;
            ensure!(
                code == 409 && r["status"] == "paused" && r["code"] == QUARANTINED,
                "a new invocation through node {} must be paused/576: {code} {r}",
                i % 2
            );
            held.push(r["id"].as_str().unwrap_or_default().to_string());
        }
        for id in &held {
            let st = within(TEST_DEADLINE, "status", status_any(&urls, id)).await?;
            ensure!(st["status"] == "paused", "{st}");
        }
        // The 50 old invocations' turns still retry; the 8 new ones have none.
        let started_ids: std::collections::HashSet<String> =
            sdk.starts_of("Poison", "boom").iter().map(|s| s.id_hex()).collect();
        ensure!(held.iter().all(|id| !started_ids.contains(id)), "a held invocation must run no turn");
        ensure!(sdk.starts_of("Poison", "boom").len() >= started_before, "the turn count does not go back");
        for id in first.iter().take(5) {
            let st = within(TEST_DEADLINE, "status", status_any(&urls, id)).await?;
            ensure!(st["status"] != "failed" && st["status"] != "paused", "an unfinished invocation is not touched: {st}");
        }

        let stats: Value = Client::new()
            .get(format!("{}/stats/handlers?scope=cluster", urls[0]))
            .send()
            .await?
            .json()
            .await?;
        let row = stats["handlers"]
            .as_array()
            .and_then(|a| a.iter().find(|r| r["service"] == "Poison" && r["handler"] == "boom").cloned())
            .ok_or_else(|| anyhow!("no Poison/boom row in /stats/handlers: {stats}"))?;
        ensure!(row["quarantine"] == "hold", "/stats marks the quarantine: {row}");

        let loaded = loaded.await??;
        let loaded_p99 = p99(loaded);
        eprintln!("listed after {listed_after:?}; Healthy p99 {base:?} while quiet, {loaded_p99:?} during the storm");
        ensure!(
            loaded_p99 <= base * 3 + Duration::from_millis(100),
            "Healthy/ok p99 {loaded_p99:?} during the storm, {base:?} while quiet"
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **The default is `observe`.** Nothing set for the tenant: the poison handler enters
/// the list with `observe`, shows up in `/stats/handlers` and the metrics — but
/// its new invocations **still run**.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observe_is_the_default_and_changes_nothing() -> Result<()> {
    let node = solo().await?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .service("Poison", "boom", vec![Step::FailRetry])
        .spawn()
        .await?;
    let r = within(Duration::from_secs(60), "observe", async {
        register(&url, &sdk.url).await?;
        send_poison(&url, 10).await?;
        let e = within(TEST_DEADLINE, "listed", wait_entry(&url, "observe")).await?;
        ensure!(
            e["reason"].as_str().is_some_and(|r| !r.is_empty()),
            "has a reason: {e}"
        );

        let ids = send_poison(&url, 1).await?;
        within(TEST_DEADLINE, "turn of the new invocation", async {
            while !sdk
                .starts_of("Poison", "boom")
                .iter()
                .any(|s| s.id_hex() == ids[0])
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok(())
        })
        .await?;
        let (_, st) = common::invocation(&url, &ids[0]).await?;
        ensure!(st["status"] != "paused", "observe holds nothing: {st}");

        let stats: Value = Client::new()
            .get(format!("{url}/stats/handlers"))
            .send()
            .await?
            .json()
            .await?;
        let row = stats["handlers"]
            .as_array()
            .and_then(|a| a.iter().find(|r| r["handler"] == "boom").cloned())
            .ok_or_else(|| anyhow!("no boom row: {stats}"))?;
        ensure!(row["quarantine"] == "observe", "{row}");
        let metrics = Client::new()
            .get(format!("{url}/metrics"))
            .send()
            .await?
            .text()
            .await?;
        ensure!(
            metrics.contains("cairnflow_quarantine_detected_total"),
            "missing detection metric"
        );
        ensure!(
            metrics.contains("cairnflow_quarantine_entries{action=\"observe\"}"),
            "missing per-action gauge"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **Manual removal.** `DELETE /admin/quarantine/{tenant}/{service}/{handler}`:
/// new invocations run again at once, and the detector does **not** put the identity back
/// with the window's old counts. Invocations already held stay `paused` — only
/// `resume` lets them run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operator_delete_releases_and_does_not_flap_back() -> Result<()> {
    let node = solo().await?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .service("Poison", "boom", vec![Step::FailRetry])
        .spawn()
        .await?;
    let r = within(Duration::from_secs(60), "manual removal", async {
        register(&url, &sdk.url).await?;
        put_tenant(
            &url,
            serde_json::json!({ "quarantine": "hold", "quarantineRule": "4/2/60" }),
        )
        .await?;
        send_poison(&url, 4).await?;
        within(TEST_DEADLINE, "listed", wait_entry(&url, "hold")).await?;
        let (code, r) = invoke(&url, "Poison", "boom", None, "{}").await?;
        ensure!(code == 409 && r["code"] == QUARANTINED, "{code} {r}");
        let held = r["id"].as_str().unwrap_or_default().to_string();

        let del = Client::new()
            .delete(format!("{url}/admin/quarantine/default/Poison/boom"))
            .send()
            .await?;
        let code = del.status();
        let body: Value = del.json().await.unwrap_or(Value::Null);
        ensure!(
            code.is_success() && body["removed"] == 1,
            "DELETE: {code} {body}"
        );

        let ids = send_poison(&url, 1).await?;
        within(TEST_DEADLINE, "new invocation runs again", async {
            while !sdk
                .starts_of("Poison", "boom")
                .iter()
                .any(|s| s.id_hex() == ids[0])
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok(())
        })
        .await?;
        // Three detection ticks: not listed again.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let e = entry(&url).await?;
        ensure!(
            e.as_ref().is_none_or(|e| e["action"] == "cleared"),
            "after manual removal it must not be listed again from the old counts: {e:?}"
        );
        let (_, st) = common::invocation(&url, &held).await?;
        ensure!(
            st["status"] == "paused",
            "a held invocation waits for resume: {st}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **A new deployment, and expiry.** The identity carries the deployment: registering a fixed
/// version makes new invocations (bound to the new version) run and complete, even while the old
/// version's entry is still valid. An entry past its `ttl` removes itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_deployment_and_expiry_release_hold() -> Result<()> {
    let mut cfg = solo_config(2);
    quick(&mut cfg);
    cfg.quarantine.ttl = Duration::from_secs(4);
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let bad = FakeSdk::builder()
        .service("Poison", "boom", vec![Step::FailRetry])
        .spawn()
        .await?;
    let r = within(Duration::from_secs(60), "new deployment, expiry", async {
        register(&url, &bad.url).await?;
        put_tenant(
            &url,
            serde_json::json!({ "quarantine": "hold", "quarantineRule": "4/2/60" }),
        )
        .await?;
        send_poison(&url, 4).await?;
        let e = within(TEST_DEADLINE, "listed", wait_entry(&url, "hold")).await?;
        let (code, r) = invoke(&url, "Poison", "boom", None, "{}").await?;
        ensure!(code == 409 && r["code"] == QUARANTINED, "{code} {r}");

        // The fix: same service, the handler completes at once.
        let fixed = FakeSdk::builder()
            .service("Poison", "boom", vec![Step::output("fixed")])
            .spawn()
            .await?;
        register(&url, &fixed.url).await?;
        let (code, r) = invoke(&url, "Poison", "boom", None, "{}").await?;
        ensure!(
            code == 200 && r["status"] == "completed",
            "a new deployment is not held: {code} {r}"
        );

        // Expiry: this entry leaves the list after `ttl`. The old invocations (bound to the
        // broken version) still retry, so the detector may list the identity again —
        // but that is a **new** entry (different `sinceMs`), not this one.
        let since = e["sinceMs"].as_u64().unwrap_or(0);
        let expires = e["expiresMs"].as_u64().unwrap_or(0);
        ensure!(expires.saturating_sub(since) == 4_000, "node ttl: {e}");
        within(TEST_DEADLINE, "expiry", async {
            while entry(&url)
                .await?
                .is_some_and(|e| e["sinceMs"].as_u64() == Some(since))
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(())
        })
        .await?;
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **`cooldown`.** One range; each turn of `Poison/boom` stalls 300 ms then
/// fails; tenant `quarantine=cooldown`.
///
/// Order:
/// 1. six invocations arrive at once — their first turns run in parallel;
/// 2. the handler enters the list with `cooldown`;
/// 3. later retries ask for a slot through the `cooldown` layer of `poll_acquire`.
///
/// Must see: before listing there are two overlapping turns (the test means something);
/// after that **no** two turns of the handler overlap on the range (the gap between
/// two consecutive `Start`s ≥ 250 ms), and the handler **still runs** (≥ 3 turns) —
/// `cooldown` lowers the limit, it does not hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cooldown_caps_handler_to_one_turn_per_range() -> Result<()> {
    let mut cfg = solo_config(1);
    quick(&mut cfg);
    // A scheduled retry falling due while the invocation is still busy (the previous turn
    // stalling 300 ms, or waiting for a slot behind the cooldown layer) is deferred by one slot
    // wait limit (floor 1 s): a short limit so the handler still runs a few turns in the window.
    cfg.limits.queue_wait = Some(Duration::from_millis(500));
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let sdk = FakeSdk::builder()
        .service("Poison", "boom", vec![Step::Stall(300), Step::FailRetry])
        .spawn()
        .await?;
    let r = within(Duration::from_secs(60), "cooldown", async {
        register(&url, &sdk.url).await?;
        put_tenant(
            &url,
            serde_json::json!({ "quarantine": "cooldown", "quarantineRule": "4/2/60" }),
        )
        .await?;
        send_poison(&url, 6).await?;
        within(TEST_DEADLINE, "listed", wait_entry(&url, "cooldown")).await?;
        let listed = std::time::SystemTime::now();
        let mut before: Vec<_> = sdk
            .starts_of("Poison", "boom")
            .iter()
            .map(|s| s.at)
            .collect();
        before.sort();
        ensure!(
                before
                    .windows(2)
                    .any(|w| w[1].duration_since(w[0]).unwrap_or_default()
                        < Duration::from_millis(250)),
                "before quarantine there must be overlapping turns: {before:?}"
            );
        // A turn in flight at listing finishes; measure from after that.
        let from = listed + Duration::from_millis(400);
        tokio::time::sleep(Duration::from_secs(6)).await;
        let mut after: Vec<_> = sdk
            .starts_of("Poison", "boom")
            .iter()
            .map(|s| s.at)
            .filter(|t| *t >= from)
            .collect();
        after.sort();
        ensure!(
            after.len() >= 3,
            "cooldown does not hold: the handler must keep running ({} turns)",
            after.len()
        );
        for w in after.windows(2) {
            let gap = w[1].duration_since(w[0]).unwrap_or_default();
            ensure!(
                gap >= Duration::from_millis(250),
                "two turns overlap ({gap:?} apart): {after:?}"
            );
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
