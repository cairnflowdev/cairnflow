// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Rate throttling and fairness between tenants — adversarial tests.
//!
//! - Tenant A has `rate = 50` CU/s and sends 500 calls/s; tenant B has no
//!   limit, on the same node. A gets `429` with `Retry-After` and runs at about
//!   50 CU/s (±10 %); B's p99 latency does not change beyond a threshold.
//!
//! - Fairness by virtual time **while nobody has hit a rate limit**. A noisy
//!   tenant piling work onto many objects (many queues) is not ordered ahead of
//!   a quiet tenant; a tenant waking up after a long sleep does not take over
//!   everyone else's queue.
//!
//! One process, auth on: the tenant comes from the API key, as in production.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    cairn_server::node::{self, NodeMode},
    common::{
        fake_sdk::{FakeSdk, Step},
        test_config, within,
    },
    reqwest::{Client, StatusCode},
    serde_json::Value,
    std::{
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    },
};

const ADMIN_KEY: &str = "cf_test_tenant_rate_bootstrap_key_01234";

async fn authed_node(
    partitions: u16,
    max_turns: Option<usize>,
) -> Result<cairn_server::node::RunningNode> {
    let mut cfg = test_config(NodeMode::single(partitions));
    cfg.auth = cairn_server::auth::AuthConfig {
        required: true,
        admin_key: Some(ADMIN_KEY.into()),
        cluster_secret: None,
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    if let Some(n) = max_turns {
        cfg.limits.max_concurrent_turns = n;
    }
    node::start(cfg)
        .await
        .map_err(|e| anyhow!("cannot build the node: {e}"))
}

/// A client bound to one tenant's key.
#[derive(Clone)]
struct As {
    url: String,
    key: String,
    http: Client,
}

impl As {
    async fn tenant(url: &str, http: &Client, tenant: &str) -> Result<Self> {
        let r = http
            .post(format!("{url}/admin/keys"))
            .bearer_auth(ADMIN_KEY)
            .json(&serde_json::json!({ "tenant": tenant, "role": "admin" }))
            .send()
            .await?;
        ensure!(
            r.status() == StatusCode::OK,
            "create key {tenant}: {}",
            r.status()
        );
        let v: Value = r.json().await?;
        Ok(Self {
            url: url.to_string(),
            key: v["secret"].as_str().unwrap_or_default().to_string(),
            http: http.clone(),
        })
    }

    async fn register(&self, sdk: &str) -> Result<()> {
        let r = self
            .http
            .post(format!("{}/deployments", self.url))
            .bearer_auth(&self.key)
            .json(&serde_json::json!({ "uri": sdk }))
            .send()
            .await?;
        ensure!(r.status() == StatusCode::OK, "register: {}", r.status());
        Ok(())
    }

    /// Call `path`; returns the code, `Retry-After` (seconds) and the body.
    async fn invoke(&self, path: &str) -> Result<(StatusCode, Option<u64>, Value)> {
        let r = self
            .http
            .post(format!("{}/invoke/{path}", self.url))
            .bearer_auth(&self.key)
            .body("{}")
            .send()
            .await?;
        let code = r.status();
        let retry = r
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok()?.parse().ok());
        let text = r.text().await?;
        Ok((
            code,
            retry,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        ))
    }

    /// Call `path` and wait until the invocation **completes**; returns the time since sending.
    async fn invoke_done(&self, path: &str) -> Result<Duration> {
        self.invoke_done_key(path, None).await
    }

    /// Like [`Self::invoke_done`], for a virtual object with key `key`.
    async fn invoke_done_key(&self, path: &str, key: Option<&str>) -> Result<Duration> {
        let path = match key {
            Some(k) => format!("{path}?key={k}"),
            None => path.to_string(),
        };
        let path = path.as_str();
        let t0 = Instant::now();
        let (code, _, body) = self.invoke(path).await?;
        if code == StatusCode::OK {
            return Ok(t0.elapsed());
        }
        ensure!(code == StatusCode::ACCEPTED, "{path}: {code} {body}");
        let id = body["id"]
            .as_str()
            .ok_or_else(|| anyhow!("202 without an id: {body}"))?
            .to_string();
        loop {
            let r = self
                .http
                .get(format!("{}/invocations/{id}", self.url))
                .bearer_auth(&self.key)
                .send()
                .await?;
            let v: Value = r.json().await?;
            if v["status"] == "completed" {
                return Ok(t0.elapsed());
            }
            ensure!(v["status"] != "failed", "{path} failed: {v}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

async fn admin_post(http: &Client, url: &str, path: &str, body: Value) -> Result<Value> {
    let r = http
        .post(format!("{url}{path}"))
        .bearer_auth(ADMIN_KEY)
        .json(&body)
        .send()
        .await?;
    let code = r.status();
    let v: Value = r.json().await?;
    ensure!(code == StatusCode::OK, "{path}: {code} {v}");
    Ok(v)
}

/// `(turns, cu)` of one tenant from the per-handler summary.
async fn turns_and_cu(http: &Client, url: &str, tenant: &str) -> Result<(u64, u64)> {
    let v: Value = http
        .get(format!("{url}/stats/handlers?window=5m"))
        .bearer_auth(ADMIN_KEY)
        .send()
        .await?
        .json()
        .await?;
    let rows = v["handlers"].as_array().cloned().unwrap_or_default();
    Ok(rows
        .iter()
        .filter(|r| r["tenant"] == tenant)
        .fold((0, 0), |(t, c), r| {
            (
                t + r["turns"].as_u64().unwrap_or(0),
                c + r["cu"].as_u64().unwrap_or(0),
            )
        }))
}

fn p99(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v.get(((v.len() as f64) * 0.99).ceil() as usize - 1)
        .copied()
        .unwrap_or_default()
}

/// **Rate limit under attack.** Tenant A: a `tenant` line in the rule book,
/// `rate = 50` CU/s; sends 500 calls/s for 6 s. Tenant B: no limit, one
/// sequential calling loop.
///
/// Must see:
/// - A gets `429` (with `Retry-After` ≥ 1 s) — the invocation is still created,
///   with an id;
/// - within the measuring window (skipping the first 2 s — the bucket's burst),
///   A runs at 50 CU/s ±10 % (A's SDK turns × CU per turn from
///   `/stats/handlers`);
/// - B's p99 latency (send → completed) is at most `2 × p99 while A is quiet +
///   50 ms`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_over_rate_is_throttled_to_rate_and_other_tenant_unaffected() -> Result<()> {
    let node = authed_node(2, None).await?;
    let url = node.url();
    let http = Client::new();
    let sdk = FakeSdk::builder()
        .service("WorkA", "run", vec![Step::output("a")])
        .service("WorkB", "run", vec![Step::output("b")])
        .spawn()
        .await?;
    let r = within(Duration::from_secs(90), "rate adversarial", async {
        let a = As::tenant(&url, &http, "a").await?;
        let b = As::tenant(&url, &http, "b").await?;
        a.register(&sdk.url).await?;
        b.register(&sdk.url).await?;
        admin_post(&http, &url, "/rules", serde_json::json!({ "tenant": "a", "rate": 50, "description": "5.6" })).await?;

        // ── B alone: the latency baseline.
        let mut alone = Vec::new();
        for _ in 0..100 {
            alone.push(b.invoke_done("WorkB/run").await?);
        }
        let base = p99(alone);

        // ── A piles on 500/s for 6 s; B keeps calling in parallel.
        const RUN: Duration = Duration::from_secs(6);
        let throttled = Arc::new(AtomicU64::new(0));
        let retry_ok = Arc::new(AtomicU64::new(0));
        let sent = Arc::new(AtomicU64::new(0));
        let start = Instant::now();
        let flood = {
            let (a, throttled, retry_ok, sent) = (a.clone(), throttled.clone(), retry_ok.clone(), sent.clone());
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(2));
                let mut inflight = tokio::task::JoinSet::new();
                while start.elapsed() < RUN {
                    tick.tick().await;
                    let (a, throttled, retry_ok) = (a.clone(), throttled.clone(), retry_ok.clone());
                    sent.fetch_add(1, Ordering::Relaxed);
                    inflight.spawn(async move {
                        if let Ok((code, retry, body)) = a.invoke("WorkA/run").await {
                            if code == StatusCode::TOO_MANY_REQUESTS {
                                throttled.fetch_add(1, Ordering::Relaxed);
                                if retry.is_some_and(|s| s >= 1) && body["id"].is_string() {
                                    retry_ok.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    });
                }
                while inflight.join_next().await.is_some() {}
            })
        };
        // A's measuring window: [2 s, 6 s].
        tokio::time::sleep(Duration::from_secs(2)).await;
        let n0 = sdk.starts_of("WorkA", "run").len();
        let t0 = Instant::now();
        let mut loaded = Vec::new();
        while start.elapsed() < RUN {
            loaded.push(b.invoke_done("WorkB/run").await?);
        }
        let n1 = sdk.starts_of("WorkA", "run").len();
        let window = t0.elapsed().as_secs_f64();
        flood.await?;
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let (turns, cu) = turns_and_cu(&http, &url, "a").await?;
        ensure!(turns > 0, "A ran no turn");
        let cu_per_turn = cu as f64 / turns as f64;
        let rate = (n1 - n0) as f64 * cu_per_turn / window;
        let loaded_p99 = p99(loaded.clone());
        let (sent, throttled, retry_ok) =
            (sent.load(Ordering::Relaxed), throttled.load(Ordering::Relaxed), retry_ok.load(Ordering::Relaxed));
        eprintln!(
            "A: {sent} sent, {throttled} got 429 ({retry_ok} with Retry-After + id); {:.1} CU/s over {window:.1} s \
             ({} turns, {cu_per_turn:.2} CU/turn). B: p99 {base:?} alone, {loaded_p99:?} while A piles on ({} calls)",
            rate,
            n1 - n0,
            loaded.len()
        );
        ensure!(throttled > 0 && retry_ok == throttled, "A must get 429 with Retry-After and id: {throttled}/{retry_ok}");
        ensure!((45.0..=55.0).contains(&rate), "A runs {rate:.1} CU/s, must be 50 ±10 %");
        ensure!(
            loaded_p99 <= base * 2 + Duration::from_millis(50),
            "B's p99 {loaded_p99:?} while A piles on, {base:?} while A is quiet"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

// ─────────────────────────────── fairness by virtual time

/// The duration of one handler turn in the fairness tests.
const TURN_MS: u64 = 100;

/// `n` parallel calling loops of `who`, each loop on its own object (`<tag>-i`)
/// — one queue per object, so round-robin between queues saves nobody.
fn loud(
    who: &As,
    tag: &'static str,
    n: usize,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> tokio::task::JoinSet<()> {
    let mut set = tokio::task::JoinSet::new();
    for i in 0..n {
        let (who, stop) = (who.clone(), stop.clone());
        set.spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let _ = who
                    .invoke_done_key("Obj/run", Some(&format!("{tag}-{i}")))
                    .await;
            }
        });
    }
    set
}

async fn fairness_node() -> Result<(cairn_server::node::RunningNode, FakeSdk)> {
    // One range, turn limit 2: saturates fast, the serving order is the waiting order.
    let node = authed_node(1, Some(2)).await?;
    let sdk = FakeSdk::builder()
        .object("Obj", "run", vec![Step::Stall(TURN_MS), Step::output("ok")])
        .spawn()
        .await?;
    Ok((node, sdk))
}

/// Number of waiters for a turn slot on the node (summed over every range).
async fn waiting_turns(node: &cairn_server::node::RunningNode) -> Result<usize> {
    let mut n = 0;
    for (_, h) in node.partitions.iter() {
        n += h
            .flow()
            .await
            .map_err(|e| anyhow!("{e}"))?
            .scheduler
            .waiting;
    }
    Ok(n)
}

/// **A noisy tenant is not ordered ahead of a quiet one.** Tenant A piles on
/// 10× (20 calling loops on 20 objects, under every rate limit), tenant B runs
/// one sequential calling loop on one object. The node is saturated (turn
/// limit 2, 100 ms per turn).
///
/// Interleaving (measured by the **order** of `Start`s at the SDK, not by the
/// clock):
/// 1. B alone makes a few calls (B has a `vt` history).
/// 2. A piles on; wait until the node is saturated: ≥ 10 waiters for a turn
///    slot.
/// 3. B calls 15 more times. For each call, count the number of A's turns that
///    start at the SDK after B sends and before B's turn.
///
/// Must see: each of B's calls waits for at most 3 of A's turns — the same
/// tolerance as the earlier clock-based version (1.25 × baseline + two turns):
/// the next free slot goes to the waiting tenant with the smallest `vt`, but a
/// turn may have just been granted while B's call was still on its way, and
/// right after A arrives the two sides' `vt`s are still close (the slack). A
/// FIFO-by-queue wait order (the old version) put B behind ~19 of A's queues.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn loud_tenant_on_many_objects_does_not_queue_ahead_of_quiet_one() -> Result<()> {
    let (node, sdk) = fairness_node().await?;
    let url = node.url();
    let http = Client::new();
    let r = within(Duration::from_secs(90), "noisy tenant ordering", async {
        let a = As::tenant(&url, &http, "a").await?;
        let b = As::tenant(&url, &http, "b").await?;
        a.register(&sdk.url).await?;
        b.register(&sdk.url).await?;
        for _ in 0..5 {
            b.invoke_done_key("Obj/run", Some("b")).await?;
        }
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut flood = loud(&a, "a", 20, stop.clone());
        while waiting_turns(&node).await? < 10 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let mut ahead = Vec::new();
        for _ in 0..15 {
            let before = sdk.starts_len();
            b.invoke_done_key("Obj/run", Some("b")).await?;
            let starts = sdk.starts();
            let mine = starts[before..]
                .iter()
                .position(|s| s.key == "b")
                .ok_or_else(|| anyhow!("B's turn is not among the SDK's Starts"))?;
            ahead.push(
                starts[before..before + mine]
                    .iter()
                    .filter(|s| s.key.starts_with("a-"))
                    .count(),
            );
        }
        stop.store(true, Ordering::Relaxed);
        while flood.join_next().await.is_some() {}
        eprintln!("A's turns cutting in before each of B's calls: {ahead:?}");
        let worst = ahead.iter().copied().max().unwrap_or(0);
        ensure!(
            worst <= 3,
            "B is ordered behind the noisy tenant: up to {worst} of A's turns cut in ({ahead:?})"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **A tenant waking up does not starve the others.**
///
/// Interleaving:
/// 1. C and B run together for 1 s (C has history, C's `vt` level with B's).
/// 2. C sleeps 6 s; B keeps running (B's `vt` grows ~30 CU/s). Without pulling
///    C up, C would build up ~180 CU of "credit" — 60 turns ahead of B on
///    waking. These two durations are **inputs** of the scenario (the pull-back
///    is per second), not a "probably done" wait.
/// 3. C wakes: 20 calling loops on 20 objects. Measured by the **order** of
///    `Start`s at the SDK (not by the clock): within the first 60 turns after C
///    wakes (≈ 3 s with a limit of 2 × 100 ms), how many turns B gets.
///
/// Must see: B ≥ 8 in those 60 turns (fair ⇒ ~14 — B has only one sequential
/// calling loop; FIFO by queue ⇒ ~3; a WFQ that does not pull tenants back ⇒
/// ~0 — C stays ahead the whole time).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sleeping_tenant_wakes_without_starving_others() -> Result<()> {
    const WINDOW: usize = 60;
    let (node, sdk) = fairness_node().await?;
    let url = node.url();
    let http = Client::new();
    let r = within(Duration::from_secs(90), "tenant wakes up", async {
        let b = As::tenant(&url, &http, "b").await?;
        let c = As::tenant(&url, &http, "c").await?;
        b.register(&sdk.url).await?;
        c.register(&sdk.url).await?;
        let stop_b = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let steady = {
            let (b, stop_b) = (b.clone(), stop_b.clone());
            tokio::spawn(async move {
                while !stop_b.load(Ordering::Relaxed) {
                    let _ = b.invoke_done_key("Obj/run", Some("b")).await;
                }
            })
        };
        // 1. C runs together with B for one second.
        let t0 = Instant::now();
        while t0.elapsed() < Duration::from_secs(1) {
            c.invoke_done_key("Obj/run", Some("c-warm")).await?;
        }
        // 2. C sleeps.
        tokio::time::sleep(Duration::from_secs(6)).await;
        // 3. C wakes and piles on.
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let woke = sdk.starts_len();
        let mut burst = loud(&c, "c", 20, stop.clone());
        while sdk.starts_len() < woke + WINDOW {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        stop.store(true, Ordering::Relaxed);
        stop_b.store(true, Ordering::Relaxed);
        steady.await?;
        while burst.join_next().await.is_some() {}
        let starts = sdk.starts();
        let during = starts[woke..woke + WINDOW]
            .iter()
            .filter(|s| s.key == "b")
            .count();
        eprintln!("B has {during} in the first {WINDOW} turns after C wakes");
        ensure!(
            during >= 8,
            "a waking tenant takes over the queue: B only has {during} in {WINDOW} turns"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **`null` resets a limit to "not set".**
///
/// 1. Set rule `*` with `concurrency`, `rate`, `burst`, a description; and a
///    tenant line `a` with `rate`, `burst`.
/// 2. Send again only `{"pattern": "*", "concurrency": null}` and
///    `{"tenant": "a", "rate": null}`.
///
/// Must see: a field sent as `null` becomes **absent** (not set — the counter
/// takes the limit of a less specific rule), an absent field is kept (merged as
/// before), and `unlimited` remains an explicit choice quite different from
/// absent. Before the fix: `null` read as absent, so there was no way to remove
/// a limit once set other than deleting the rule and creating it again.
#[tokio::test]
async fn null_clears_cap_back_to_unset() -> Result<()> {
    let node = authed_node(1, None).await?;
    let url = node.url();
    let http = Client::new();
    let r = within(Duration::from_secs(30), "null resets a limit", async {
        admin_post(
            &http,
            &url,
            "/rules",
            serde_json::json!({ "pattern": "*", "concurrency": 5, "rate": 10, "burst": 3,
                "description": "keep" }),
        )
        .await?;
        admin_post(
            &http,
            &url,
            "/rules",
            serde_json::json!({ "tenant": "a", "rate": 50, "burst": 5 }),
        )
        .await?;
        let v = admin_post(
            &http,
            &url,
            "/rules",
            serde_json::json!({ "pattern": "*", "concurrency": null }),
        )
        .await?;
        let star = v["rules"]
            .as_array()
            .and_then(|r| r.iter().find(|r| r["pattern"] == "*"))
            .ok_or_else(|| anyhow!("{v}"))?;
        ensure!(star.get("concurrency").is_none(), "concurrency on: {star}");
        ensure!(
            star["rate"] == "10" && star["burst"] == 3 && star["description"] == "keep",
            "absent fields must be kept: {star}"
        );
        let v = admin_post(
            &http,
            &url,
            "/rules",
            serde_json::json!({ "tenant": "a", "rate": null }),
        )
        .await?;
        let a = v["tenants"]
            .as_array()
            .and_then(|t| t.iter().find(|t| t["tenant"] == "a"))
            .ok_or_else(|| anyhow!("{v}"))?;
        ensure!(a.get("rate").is_none() && a["burst"] == 5, "{a}");
        let v = admin_post(
            &http,
            &url,
            "/rules",
            serde_json::json!({ "pattern": "*", "rate": "unlimited" }),
        )
        .await?;
        let star = v["rules"]
            .as_array()
            .and_then(|r| r.iter().find(|r| r["pattern"] == "*"))
            .ok_or_else(|| anyhow!("{v}"))?;
        ensure!(star["rate"] == "unlimited", "{star}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
