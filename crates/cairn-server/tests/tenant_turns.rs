// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! One tenant cannot hold all of a node's turn slots, and the cell's SDK wait
//! limit clamps the `inactivityTimeout` declared in the contract.
//!
//! - **Per-tenant turn limit** (`TurnLimits::max_turns_per_tenant`, flag
//!   `--max-turns-per-tenant`, SaaS profile: ¼ of the node's slots): the running
//!   turns of **one** tenant across the node — every range — do not exceed the
//!   limit. A hanging endpoint (a long LLM step, or on purpose) only holds that
//!   tenant's share; other tenants still get slots.
//! - **Limit on `inactivityTimeout`** (`TurnLimits::sdk_timeout_max`, flag
//!   `--inactivity-timeout-max-ms`, SaaS profile: 15 minutes instead of 1 hour):
//!   if the contract declares longer, the turn is still cut at the cell's limit.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    cairn_server::auth::AuthConfig,
    common::{
        fake_sdk::{FakeSdk, Step},
        within, TEST_DEADLINE,
    },
    reqwest::{Client, StatusCode},
    serde_json::{json, Value},
    std::time::Duration,
};

const ADMIN_KEY: &str = "cf_test_tenant_turns_bootstrap_key_0123";
const SECRET: &str = "cf_test_cluster_secret_tenant_turns_0123456789";

async fn call(req: reqwest::RequestBuilder) -> Result<(StatusCode, Value)> {
    let r = req.send().await?;
    let code = r.status();
    let text = r.text().await?;
    Ok((
        code,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    ))
}

/// An `admin`-role key of `tenant` (retried until the node takes its lease).
async fn key(url: &str, tenant: &str) -> Result<String> {
    within(TEST_DEADLINE, "create a tenant key", async {
        loop {
            let (code, v) = call(
                Client::new()
                    .post(format!("{url}/admin/keys"))
                    .bearer_auth(ADMIN_KEY)
                    .json(&json!({ "tenant": tenant, "role": "admin" })),
            )
            .await?;
            if code == StatusCode::OK {
                return Ok(v["secret"].as_str().unwrap_or_default().to_string());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
}

async fn register(url: &str, key: &str, sdk: &str) -> Result<()> {
    let (code, v) = call(
        Client::new()
            .post(format!("{url}/deployments"))
            .bearer_auth(key)
            .json(&json!({ "uri": sdk })),
    )
    .await?;
    ensure!(code == StatusCode::OK, "register deployment: {code} {v}");
    Ok(())
}

async fn send(url: &str, key: &str, target: &str) -> Result<String> {
    let (code, v) = call(
        Client::new()
            .post(format!("{url}/invoke/{target}/send"))
            .bearer_auth(key)
            .json(&json!("x")),
    )
    .await?;
    ensure!(code.is_success(), "/send {target}: {code} {v}");
    v["id"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("no id: {v}"))
}

async fn wait_completed(url: &str, key: &str, id: &str) -> Result<Value> {
    within(TEST_DEADLINE, "invocation done", async {
        loop {
            let (code, v) = call(
                Client::new()
                    .get(format!("{url}/invocations/{id}"))
                    .bearer_auth(key),
            )
            .await?;
            if code == StatusCode::OK && v["status"] == "completed" {
                return Ok(v);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
}

fn auth() -> AuthConfig {
    AuthConfig {
        required: true,
        admin_key: Some(ADMIN_KEY.into()),
        cluster_secret: Some(SECRET.into()),
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    }
}

/// **A tenant with a hanging endpoint takes no more than its share of slots.**
///
/// 1. A one-node cluster, two ranges, 4 turn slots, a limit of 1 turn per
///    tenant.
/// 2. Tenant `hog` sends 3 invocations to `Hang/run` — the fake SDK holds every
///    request at a gate (a long step, or an endpoint hanging on purpose).
/// 3. Tenant `quiet` calls `Echo/hi`.
///
/// Must see: within 1.5 s the SDK receives exactly **one** `Start` of `Hang/run`
/// (the limit), and `quiet`'s `Echo/hi` completes meanwhile; opening the gate ⇒
/// `hog`'s three invocations complete one after another.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_tenant_never_holds_more_than_its_turn_share() -> Result<()> {
    let sdk = FakeSdk::builder()
        .gated_service("Hang", "run", vec![Step::output("\"ok\"")])
        .service("Echo", "hi", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node =
        common::start_cluster_node_tweaked(&bucket, "turns-a", 2, Duration::from_secs(2), |cfg| {
            cfg.auth = auth();
            cfg.limits.max_concurrent_turns = 4;
            cfg.limits.max_turns_per_tenant = Some(1);
            cfg.deploy_refresh = Duration::from_millis(100);
        })
        .await?;
    let url = node.node.url();
    let r = async {
        let hog = key(&url, "hog").await?;
        let quiet = key(&url, "quiet").await?;
        register(&url, &hog, &sdk.url).await?;
        register(&url, &quiet, &sdk.url).await?;
        let mut ids = Vec::new();
        for _ in 0..3 {
            ids.push(send(&url, &hog, "Hang/run").await?);
        }
        within(
            Duration::from_secs(10),
            "hog's first request reaches the SDK",
            async {
                while sdk.starts_of("Hang", "run").is_empty() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Ok(())
            },
        )
        .await?;
        let echo = send(&url, &quiet, "Echo/hi").await?;
        let t0 = std::time::Instant::now();
        while t0.elapsed() < Duration::from_millis(1_500) {
            let n = sdk.starts_of("Hang", "run").len();
            ensure!(n == 1, "hog holds {n} turns at once, limit 1 per tenant");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        wait_completed(&url, &quiet, &echo).await?;
        sdk.gate().open();
        for id in &ids {
            wait_completed(&url, &hog, id).await?;
        }
        Ok(())
    }
    .await;
    node.node.shutdown().await;
    r
}

/// **The cell's limit clamps the contract's `inactivityTimeout`** (SaaS profile:
/// 15 minutes instead of 1 hour).
///
/// 1. A node with an `sdk_timeout_max` limit of 1 s; handler `Slow/run` declares
///    an `inactivityTimeout` of 10 minutes and stalls 2.5 s mid-turn (`Stall`).
/// 2. `/send` once.
///
/// Must see: the turn is cut at 1 s and retried — the SDK receives ≥ 2 `Start`s
/// within 6 s — instead of waiting as the contract says.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cell_cap_clamps_the_declared_inactivity_timeout() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service(
            "Slow",
            "run",
            vec![Step::Stall(2_500), Step::output("\"ok\"")],
        )
        .contract(
            "Slow",
            "run",
            cairn_server::discovery::Contract {
                inactivity_timeout: Some(600_000),
                ..Default::default()
            },
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node =
        common::start_cluster_node_tweaked(&bucket, "turns-b", 1, Duration::from_secs(2), |cfg| {
            cfg.auth = auth();
            cfg.limits.sdk_timeout_max = Duration::from_secs(1);
            cfg.deploy_refresh = Duration::from_millis(100);
        })
        .await?;
    let url = node.node.url();
    let r = async {
        let k = key(&url, "acme").await?;
        register(&url, &k, &sdk.url).await?;
        send(&url, &k, "Slow/run").await?;
        within(
            Duration::from_secs(6),
            "turn cut at the cell's limit, then retried",
            async {
                while sdk.starts_of("Slow", "run").len() < 2 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Ok(())
            },
        )
        .await
    }
    .await;
    node.node.shutdown().await;
    r
}
