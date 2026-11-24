// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Per-key request rate limit at ingress: one key (or one SSO user) cannot fire an
//! unbounded stream of requests — every write request is an engine turn, sometimes an
//! audit object. GCRA `N/S` per key (`--key-rate`, `NodeConfig::key_rate`; the SaaS
//! profile sets a default).
//!
//! Must see: a key at the limit gets `429` `code: "key_rate_limited"` with
//! `Retry-After`; another key (same tenant) is not blocked; open routes (`/health`)
//! do not count; with no limit set nothing is blocked.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{ensure, Result},
    cairn_server::{auth::AuthConfig, tenants::DeployRate},
    common::{within, ClusterNode, TEST_DEADLINE},
    reqwest::{Client, StatusCode},
    serde_json::{json, Value},
    std::time::Duration,
};

const ADMIN_KEY: &str = "cf_test_key_rate_bootstrap_key_0123456789";

async fn node(bucket: &common::Bucket, rate: Option<DeployRate>) -> Result<ClusterNode> {
    common::start_cluster_node_tweaked(bucket, "n1", 2, Duration::from_secs(2), |cfg| {
        cfg.auth = AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: None,
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
        cfg.key_rate = rate;
    })
    .await
}

async fn key(url: &str, tenant: &str) -> Result<String> {
    key_with(url, tenant, "admin").await
}

async fn key_with(url: &str, tenant: &str, role: &str) -> Result<String> {
    loop {
        let r = Client::new()
            .post(format!("{url}/admin/keys"))
            .bearer_auth(ADMIN_KEY)
            .json(&json!({ "tenant": tenant, "role": role }))
            .send()
            .await?;
        if r.status() == StatusCode::OK {
            let v: Value = r.json().await?;
            return Ok(v["secret"].as_str().unwrap_or_default().to_string());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `5/60`: a key's first five requests pass, the sixth is `429` with a code and
/// `Retry-After`; a second key of the same tenant still passes; `/health` does not count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_key_is_rate_limited() -> Result<()> {
    let bucket = common::bucket();
    let n = node(&bucket, DeployRate::parse("5/60")).await?;
    let url = n.node.url();
    within(TEST_DEADLINE, "per-key rate limit", async {
        let a = key(&url, "acme").await?;
        let b = key(&url, "acme").await?;
        let http = Client::new();
        for i in 0..5 {
            let r = http
                .get(format!("{url}/tenant"))
                .bearer_auth(&a)
                .send()
                .await?;
            ensure!(r.status() == StatusCode::OK, "request {i}: {}", r.status());
            let r = http.get(format!("{url}/health")).send().await?;
            ensure!(r.status().is_success(), "/health: {}", r.status());
        }
        let r = http
            .get(format!("{url}/tenant"))
            .bearer_auth(&a)
            .send()
            .await?;
        let code = r.status();
        let retry = r.headers().get(reqwest::header::RETRY_AFTER).cloned();
        let v: Value = r.json().await.unwrap_or(Value::Null);
        ensure!(
            code == StatusCode::TOO_MANY_REQUESTS
                && v["code"] == "key_rate_limited"
                && retry.is_some(),
            "sixth request: {code} {v} {retry:?}"
        );
        let r = http
            .get(format!("{url}/tenant"))
            .bearer_auth(&b)
            .send()
            .await?;
        ensure!(r.status() == StatusCode::OK, "other key: {}", r.status());
        Ok(())
    })
    .await
}

/// No limit set (the self-hosted default): nothing is blocked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_rate_no_limit() -> Result<()> {
    let bucket = common::bucket();
    let n = node(&bucket, None).await?;
    let url = n.node.url();
    within(TEST_DEADLINE, "no limit", async {
        let a = key(&url, "acme").await?;
        let http = Client::new();
        for i in 0..30 {
            let r = http
                .get(format!("{url}/tenant"))
                .bearer_auth(&a)
                .send()
                .await?;
            ensure!(r.status() == StatusCode::OK, "request {i}: {}", r.status());
        }
        Ok(())
    })
    .await
}

/// **A `403` request also spends the key's budget** (security review):
/// a `view` key fires `POST /deployments` (missing capability) — each one is a new
/// `Denials` aggregation key if the path changes, so it must not be exempt from the limit.
///
/// Must see: with `5/60`, the first five are `403`, the sixth `429`
/// `key_rate_limited`; after that even a route the key may use (`GET /tenant`) is `429`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forbidden_requests_spend_the_key_budget() -> Result<()> {
    let bucket = common::bucket();
    let n = node(&bucket, DeployRate::parse("5/60")).await?;
    let url = n.node.url();
    within(TEST_DEADLINE, "403 spends budget", async {
        let v = key_with(&url, "acme", "view").await?;
        let http = Client::new();
        for i in 0..5 {
            let r = http
                .post(format!("{url}/deployments/x{i}"))
                .bearer_auth(&v)
                .json(&json!({ "uri": "http://127.0.0.1:1" }))
                .send()
                .await?;
            ensure!(
                r.status() == StatusCode::FORBIDDEN,
                "request {i}: {}",
                r.status()
            );
        }
        let r = http
            .post(format!("{url}/deployments/x5"))
            .bearer_auth(&v)
            .json(&json!({ "uri": "http://127.0.0.1:1" }))
            .send()
            .await?;
        ensure!(
            r.status() == StatusCode::TOO_MANY_REQUESTS,
            "sixth request: {}",
            r.status()
        );
        let r = http
            .get(format!("{url}/tenant"))
            .bearer_auth(&v)
            .send()
            .await?;
        ensure!(
            r.status() == StatusCode::TOO_MANY_REQUESTS,
            "permitted route after the budget ran out: {}",
            r.status()
        );
        Ok(())
    })
    .await
}
