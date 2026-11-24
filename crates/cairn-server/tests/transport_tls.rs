// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Transport to an `https://` endpoint behind a PaaS edge: the edge terminates
//! TLS, speaks h2 to the runtime, speaks h1 to the app and buffers the whole
//! request body ([`common::tls_edge`]).
//!
//! `REQUEST_RESPONSE` gets through that edge (whole body, one request per
//! turn); `BIDI_STREAM` does not — the runtime keeps the request body open and
//! waits for the response headers, the edge waits for the end of the body, and
//! each side waits for the other.
//!
//! Needs a `testkit` build: the runtime trusts the edge's CA through
//! `EgressPolicy::tls_roots` (binary: `CAIRNFLOW_TESTKIT_TLS_ROOTS`).

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{ensure, Result},
    cairn_server::node::NodeMode,
    common::{
        fake_sdk::{FakeSdk, Step},
        tls_edge::{self, TlsEdge},
    },
    serde_json::{json, Value},
    std::{sync::atomic::Ordering, time::Duration},
};

async fn register(node: &str, uri: &str, transport: Option<&str>) -> Result<(u16, Value)> {
    let mut body = json!({ "uri": uri });
    if let Some(t) = transport {
        body["transport"] = t.into();
    }
    let r = reqwest::Client::new()
        .post(format!("{node}/deployments"))
        .json(&body)
        .send()
        .await?;
    Ok((r.status().as_u16(), r.json().await.unwrap_or(Value::Null)))
}

/// An in-process node that trusts the edge's CA.
fn node_config() -> cairn_server::node::NodeConfig {
    let mut cfg = common::test_config(NodeMode::single(1));
    cfg.egress.tls_roots = tls_edge::ca_roots();
    cfg
}

/// `REQUEST_RESPONSE` through a TLS h2 → h1 edge that buffers the body: the
/// registration sees `HTTP/2` (ALPN), the invocation completes; every request
/// to the app carries the whole body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_response_through_tls_edge() -> Result<()> {
    let node = common::start_led(node_config()).await?;
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"ok\"")])
        .spawn()
        .await?;
    let edge = TlsEdge::spawn(&sdk.url, None).await?;
    let r = common::within(Duration::from_secs(60), "RR over TLS", async {
        let (code, d) = register(&node.url(), &edge.url, None).await?;
        ensure!(code == 200, "{code} {d}");
        ensure!(d["protocolMode"] == "REQUEST_RESPONSE", "{d}");
        ensure!(d["transport"]["discoveredHttp"] == "HTTP/2", "{d}");
        let (code, out) = common::invoke(&node.url(), "Echo", "run", None, "null").await?;
        ensure!(
            code == 200 && out["status"] == "completed" && out["output"] == "\"ok\"",
            "invoke: {code} {out}"
        );
        ensure!(
            edge.forwarded.load(Ordering::SeqCst) >= 2,
            "the edge must forward /discover and /invoke"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

const ADMIN_KEY: &str = "cf_test_transport_tls_bootstrap_key_0123456789";

/// A failure to open a BIDI stream shows up in `lastFailure`.
const NO_HEADERS: &str = "bidi_no_response_headers";

/// Reproduces the transport failure of the SaaS profile:
///
/// 1. Binary `--profile saas` (strict egress, `https://` only), loopback
///    opened with `--egress-allow-cidr` for the fake edge.
/// 2. The SDK declares `BIDI_STREAM` (the default of the TS `serve()`) and
///    sits behind a TLS h2 → h1 edge that buffers the body; the registration
///    does **not** say `transport`.
/// 3. Call one invocation synchronously.
///
/// Before the fix: registration succeeded with `BIDI_STREAM`, the invocation
/// hung at the edge until `sdk_timeout` (60 s) and then retried forever. Must
/// see: the deployment settles on `REQUEST_RESPONSE` (an available value,
/// `transport.policy = declared`, the reason in `bidiProbe`), the invocation
/// completes within a few seconds; registering again with
/// `transport: declared` keeps `BIDI_STREAM` (the tenant chose it on purpose).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saas_profile_registers_https_bidi_as_request_response() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"ok\"")])
        .declare_bidi()
        .spawn()
        .await?;
    let edge = TlsEdge::spawn(&sdk.url, Some("v1")).await?;
    let roots = tls_edge::ca_path();
    let node = common::binary::Binary::spawn(
        &[
            "--profile",
            "saas",
            "--main-store",
            "memory://",
            "--port",
            "0",
            "--egress-deny-cidr",
            "203.0.113.7",
            "--egress-allow-cidr",
            "127.0.0.1/32",
        ],
        &[
            (
                "CAIRNFLOW_CLUSTER_SECRET",
                "cf_test_cluster_secret_transport_tls_0123456789",
            ),
            (
                "CAIRNFLOW_IDENTITY_MASTER",
                "cf_test_identity_master_transport_tls_01",
            ),
            ("CAIRNFLOW_ADMIN_KEY", ADMIN_KEY),
            (
                cairn_server::egress::TLS_ROOTS_ENV,
                roots.to_str().expect("UTF-8 path"),
            ),
        ],
    )?;
    let http = reqwest::Client::new();
    let register = |transport: Option<&'static str>| {
        let (http, url, edge) = (http.clone(), node.url.clone(), edge.url.clone());
        async move {
            let mut body = json!({ "uri": edge });
            if let Some(t) = transport {
                body["transport"] = t.into();
            }
            let r = http
                .post(format!("{url}/deployments"))
                .bearer_auth(ADMIN_KEY)
                .json(&body)
                .send()
                .await?;
            let code = r.status().as_u16();
            anyhow::Ok((code, r.json::<Value>().await.unwrap_or(Value::Null)))
        }
    };
    let (code, d) = register(None).await?;
    ensure!(code == 200, "{code} {d}\n{}", node.dump());
    // A 20 s cap < `sdk_timeout` 60 s: before the fix this call hung at the edge.
    let r = http
        .post(format!("{}/invoke/Echo/run", node.url))
        .bearer_auth(ADMIN_KEY)
        .header("content-type", "application/json")
        .body("null")
        .timeout(Duration::from_secs(20))
        .send()
        .await;
    let out: Value = match r {
        Ok(r) => r.json().await?,
        Err(e) => anyhow::bail!("invoke through the TLS edge hung: {e}\n{}", node.dump()),
    };
    ensure!(
        out["status"] == "completed" && out["output"] == "\"ok\"",
        "invoke: {out}"
    );
    ensure!(d["protocolMode"] == "REQUEST_RESPONSE", "{d}");
    ensure!(
        d["transport"]["policy"] == "declared" && d["transport"]["declared"] == "BIDI_STREAM",
        "{d}"
    );
    ensure!(
        d["transport"]["bidiProbe"]
            .as_str()
            .is_some_and(|p| p.contains("REQUEST_RESPONSE")),
        "the reason must be in bidiProbe: {d}"
    );
    let (code, d) = register(Some("declared")).await?;
    ensure!(
        code == 200 && d["protocolMode"] == "BIDI_STREAM",
        "transport: declared must keep BIDI_STREAM: {code} {d}"
    );
    ensure!(d["transport"].get("bidiProbe").is_none(), "{d}");
    Ok(())
}

/// Reports early when `BIDI_STREAM` gets no response headers:
///
/// 1. An in-process node, default `sdk_timeout` (60 s).
/// 2. The tenant chooses BIDI on purpose (`transport: declared`) for an SDK
///    behind a body-buffering TLS edge — the edge never returns headers.
/// 3. Send one invocation, read `lastFailure`.
///
/// Before the fix: the turn hung for the full 60 s and the error only said the
/// SDK wait cap ran out. Must see: within ~15 s `lastFailure` starts with the
/// stable code `bidi_no_response_headers`, suggests `transport: auto` /
/// `REQUEST_RESPONSE`, does not say "cleartext" (the URI is `https://`); the
/// invocation is not finished (retryable, no automatic mode switch).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bidi_without_response_headers_fails_early_with_code() -> Result<()> {
    let node = common::start_led(node_config()).await?;
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("\"ok\"")])
        .declare_bidi()
        .spawn()
        .await?;
    let edge = TlsEdge::spawn(&sdk.url, None).await?;
    let r = common::within(Duration::from_secs(90), "BIDI without headers", async {
        let (code, d) = register(&node.url(), &edge.url, Some("declared")).await?;
        ensure!(
            code == 200 && d["protocolMode"] == "BIDI_STREAM",
            "{code} {d}"
        );
        let (code, body) = common::send(&node.url(), "Echo", "run", None, "null").await?;
        ensure!(code == 202 || code == 200, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        let t0 = std::time::Instant::now();
        let msg = loop {
            let (_, st) = common::invocation(&node.url(), &id).await?;
            let msg = st["lastFailure"]["message"].as_str().unwrap_or("");
            if msg.contains(NO_HEADERS) {
                ensure!(st["status"] != "completed", "{st}");
                break msg.to_string();
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(40),
                "must report {NO_HEADERS} before sdk_timeout runs out: {st}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        };
        ensure!(msg.starts_with(NO_HEADERS), "code must lead: {msg}");
        ensure!(
            msg.contains("transport: auto") && msg.contains("REQUEST_RESPONSE"),
            "missing hint: {msg}"
        );
        ensure!(!msg.contains("cleartext"), "https URI, says h2c: {msg}");
        ensure!(
            edge.opened.load(Ordering::SeqCst) >= 1 && sdk.starts_len() == 0,
            "the edge must receive the stream while the app never sees Start"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
