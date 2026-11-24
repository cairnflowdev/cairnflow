// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Query v1: `GET /invocations` has a `limit` cap and filters at the leader by service,
//! handler, `trace_id` — filtering **before** `limit`, inside the engine, like `status`.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Context, Result},
    cairn_server::node::{self, NodeMode},
    common::{
        fake_sdk::{FakeSdk, Step},
        register, send, start_single_process, test_config, wait_status, within, TEST_DEADLINE,
    },
    reqwest::Client,
};

const ADMIN_KEY: &str = "cf_test_query_bootstrap_0123456789abcdef";

async fn list(node: &str, q: &str) -> Result<serde_json::Value> {
    let r = reqwest::get(format!("{node}/invocations{q}")).await?;
    ensure!(r.status().is_success(), "{q}: {}", r.status());
    Ok(r.json().await?)
}

fn rows(v: &serde_json::Value) -> Vec<serde_json::Value> {
    v.as_array()
        .or_else(|| v["invocations"].as_array())
        .cloned()
        .unwrap_or_default()
}

/// **The `limit` cap.** A huge `?limit=` is clamped to the node's cap, and the
/// cluster-wide response states the `limit` actually used (honest about the scope).
///
/// Must see: `?scope=cluster&limit=999999999` returns `limit: 1000`;
/// `?limit=7` returns `limit: 7`.
#[tokio::test]
async fn list_limit_is_capped_and_reported() -> Result<()> {
    let node = start_single_process(1).await?;
    let v = list(&node.url(), "?scope=cluster&limit=999999999").await?;
    assert_eq!(v["limit"], 1000, "{v}");
    let v = list(&node.url(), "?scope=cluster&limit=7").await?;
    assert_eq!(v["limit"], 7, "{v}");
    node.shutdown().await;
    Ok(())
}

/// **Filtering at the leader.** Three invocations: `Mail/send` ×2, `Bill/charge` ×1, on a
/// **one-range** node — `limit` applies per range, so the check "filter before
/// `limit`" only means something when every row is in one range.
///
/// Must see: `?service=Mail` returns exactly two; `?service=Mail&handler=send&limit=1`
/// returns one (filtered before `limit`); `?handler=charge` returns `Bill`, even with
/// `limit=1`; `?trace_id=` (the first 32
/// hex digits of the id, the OTLP trace id) returns exactly that invocation; all three filters
/// work through `?scope=cluster`; a malformed `trace_id` ⇒ `400`.
#[tokio::test]
async fn list_filters_by_service_handler_and_trace_id() -> Result<()> {
    let node = start_single_process(1).await?;
    let sdk = FakeSdk::builder()
        .service("Mail", "send", vec![Step::output("1")])
        .service("Bill", "charge", vec![Step::output("2")])
        .spawn()
        .await?;
    register(&node.url(), &sdk.url).await?;
    let mut ids = Vec::new();
    for (svc, h) in [("Mail", "send"), ("Mail", "send"), ("Bill", "charge")] {
        let (code, r) = send(&node.url(), svc, h, None, "null").await?;
        ensure!(code < 300, "{code} {r}");
        let id = r["id"].as_str().context("id")?.to_string();
        within(
            TEST_DEADLINE,
            "completed",
            wait_status(&node.url(), &id, "completed"),
        )
        .await?;
        ids.push(id);
    }
    let mail = rows(&list(&node.url(), "?service=Mail").await?);
    assert_eq!(mail.len(), 2, "{mail:?}");
    assert!(mail.iter().all(|r| r["service"] == "Mail"));
    let one = rows(&list(&node.url(), "?service=Mail&handler=send&limit=1").await?);
    assert_eq!(one.len(), 1, "filter before limit: {one:?}");
    let bill = rows(&list(&node.url(), "?handler=charge").await?);
    assert_eq!(bill.len(), 1);
    let first = rows(&list(&node.url(), "?handler=charge&limit=1").await?);
    assert_eq!(first.len(), 1, "filter before limit: {first:?}");
    assert_eq!(first[0]["service"], "Bill");
    assert_eq!(bill[0]["service"], "Bill");
    let trace = &ids[2][..32];
    let by_trace = rows(&list(&node.url(), &format!("?trace_id={trace}")).await?);
    assert_eq!(by_trace.len(), 1, "{by_trace:?}");
    assert_eq!(by_trace[0]["id"], ids[2]);
    let cluster = rows(&list(&node.url(), "?scope=cluster&service=Mail").await?);
    assert_eq!(cluster.len(), 2, "{cluster:?}");
    let bad = reqwest::get(format!("{}/invocations?trace_id=xyz", node.url())).await?;
    assert_eq!(bad.status().as_u16(), 400);
    node.shutdown().await;
    Ok(())
}

/// **Filtering by tenant before `limit`, inside the engine.** A one-range node with auth;
/// tenant `globex` creates one invocation **first**, `acme` three after it.
///
/// The old bug: the engine cut `limit` (and sorted `created_desc`) over the rows of **every**
/// tenant, and ingress filtered the tenant afterwards: `globex` asking `?created_after=0&limit=1`
/// (sorted by descending time by default) got `[]` — the newest row is `acme`'s — with
/// `x-cairn-partial: false`, i.e. "complete".
///
/// Must see: `globex` gets exactly its invocation, both with `?created_after=0&limit=1`
/// and with `?service=Greeter&limit=1`; `acme` with `limit=2` gets two of its rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_filter_applies_before_limit() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Greeter", "hi", vec![Step::output("1")])
        .spawn()
        .await?;
    let mut cfg = test_config(NodeMode::single(1));
    cfg.auth = cairn_server::auth::AuthConfig {
        required: true,
        admin_key: Some(ADMIN_KEY.into()),
        cluster_secret: None,
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    let node = node::start(cfg)
        .await
        .map_err(|e| anyhow!("could not build the node: {e}"))?;
    let url = node.url();
    let http = Client::new();
    let key_for = |tenant: &'static str| {
        let (http, url) = (http.clone(), url.clone());
        async move {
            let r = http
                .post(format!("{url}/admin/keys"))
                .bearer_auth(ADMIN_KEY)
                .json(&serde_json::json!({ "tenant": tenant, "role": "admin" }))
                .send()
                .await?;
            ensure!(r.status().is_success(), "key {tenant}: {}", r.status());
            let v: serde_json::Value = r.json().await?;
            Ok::<String, anyhow::Error>(v["secret"].as_str().unwrap_or_default().to_string())
        }
    };
    let (acme, globex) = (key_for("acme").await?, key_for("globex").await?);
    let call = |key: &String, n: usize| {
        let (http, url, key, sdk_url) = (http.clone(), url.clone(), key.clone(), sdk.url.clone());
        async move {
            let r = http
                .post(format!("{url}/deployments"))
                .bearer_auth(&key)
                .json(&serde_json::json!({ "uri": sdk_url }))
                .send()
                .await?;
            ensure!(r.status().is_success(), "register: {}", r.status());
            let mut ids = Vec::new();
            for _ in 0..n {
                let r = http
                    .post(format!("{url}/invoke/Greeter/hi"))
                    .bearer_auth(&key)
                    .body("null")
                    .send()
                    .await?;
                ensure!(r.status().is_success(), "invoke: {}", r.status());
                let v: serde_json::Value = r.json().await?;
                ids.push(v["id"].as_str().context("id")?.to_string());
                // Millisecond timestamps: the next one is surely created later.
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            Ok::<Vec<String>, anyhow::Error>(ids)
        }
    };
    let mine = call(&globex, 1).await?;
    call(&acme, 3).await?;
    let list = |key: &String, q: &'static str| {
        let (http, url, key) = (http.clone(), url.clone(), key.clone());
        async move {
            let r = http
                .get(format!("{url}/invocations{q}"))
                .bearer_auth(&key)
                .send()
                .await?;
            ensure!(r.status().is_success(), "{q}: {}", r.status());
            let partial = r
                .headers()
                .get("x-cairn-partial")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            let v: serde_json::Value = r.json().await?;
            Ok::<_, anyhow::Error>((rows(&v), partial))
        }
    };
    let (newest, partial) = list(&globex, "?created_after=0&limit=1").await?;
    ensure!(
        newest.len() == 1 && newest[0]["id"] == mine[0],
        "globex lost its own row (partial = {partial:?}): {newest:?}"
    );
    let (by_service, _) = list(&globex, "?service=Greeter&limit=1").await?;
    ensure!(
        by_service.len() == 1 && by_service[0]["id"] == mine[0],
        "globex ?service=: {by_service:?}"
    );
    let (two, _) = list(&acme, "?limit=2").await?;
    ensure!(two.len() == 2, "acme limit=2: {two:?}");
    node.shutdown().await;
    Ok(())
}
