// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Tenants: the tenant comes **from the API key**, is part of `Target`, of the
//! partition key hash input and of every user-named key. Two tenants share one
//! cluster without seeing each other.
//!
//! Adversarial: tenant A knows (guesses correctly) tenant B's object key,
//! invocation id and idempotency key — and still cannot read anything of B's.

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
        test_config, within, TEST_DEADLINE,
    },
    reqwest::{Client, StatusCode},
    serde_json::Value,
};

const ADMIN_KEY: &str = "cf_test_tenants_bootstrap_key_0123456789";

/// A one-process node, auth on.
async fn authed_node(partitions: u16) -> Result<cairn_server::node::RunningNode> {
    let mut cfg = test_config(NodeMode::single(partitions));
    cfg.auth = cairn_server::auth::AuthConfig {
        required: true,
        admin_key: Some(ADMIN_KEY.into()),
        cluster_secret: None,
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
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
    /// Create an `admin`-role key for `tenant` with the bootstrap key.
    async fn tenant(url: &str, tenant: &str) -> Result<Self> {
        let http = Client::new();
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
            http,
        })
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> Result<(StatusCode, Value)> {
        let r = req.bearer_auth(&self.key).send().await?;
        let code = r.status();
        let text = r.text().await?;
        Ok((
            code,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        ))
    }

    async fn register(&self, sdk: &str) -> Result<()> {
        let (code, body) = self
            .send(
                self.http
                    .post(format!("{}/deployments", self.url))
                    .json(&serde_json::json!({ "uri": sdk })),
            )
            .await?;
        ensure!(code == StatusCode::OK, "register: {code} {body}");
        Ok(())
    }

    async fn invoke(&self, path: &str, idem: Option<&str>) -> Result<(StatusCode, Value)> {
        let mut req = self
            .http
            .post(format!("{}/invoke/{path}", self.url))
            .body("{}");
        if let Some(k) = idem {
            req = req.header("idempotency-key", k);
        }
        self.send(req).await
    }

    async fn get(&self, path: &str) -> Result<(StatusCode, Value)> {
        self.send(self.http.get(format!("{}{path}", self.url)))
            .await
    }
}

/// Two tenants register two different deployments under the **same** service
/// name: each tenant's call reaches its own deployment. A third tenant
/// (`default`, the bootstrap key) that has registered nothing has no such
/// service.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenants_do_not_share_service_names() -> Result<()> {
    let sdk_a = FakeSdk::builder()
        .service("Greeter", "hi", vec![Step::output("from-acme")])
        .spawn()
        .await?;
    let sdk_b = FakeSdk::builder()
        .service("Greeter", "hi", vec![Step::output("from-globex")])
        .spawn()
        .await?;
    let node = authed_node(4).await?;
    let url = node.url();
    let r = within(TEST_DEADLINE, "two tenants, one service name", async {
        let acme = As::tenant(&url, "acme").await?;
        let globex = As::tenant(&url, "globex").await?;
        acme.register(&sdk_a.url).await?;
        globex.register(&sdk_b.url).await?;

        let (code, body) = acme.invoke("Greeter/hi", None).await?;
        ensure!(
            code == StatusCode::OK && body["output"] == "from-acme",
            "acme: {code} {body}"
        );
        let (code, body) = globex.invoke("Greeter/hi", None).await?;
        ensure!(
            code == StatusCode::OK && body["output"] == "from-globex",
            "globex: {code} {body}"
        );

        // Each tenant's service registry only has its own deployment.
        let (_, services) = acme.get("/services").await?;
        ensure!(
            services.to_string().contains(&sdk_a.url) && !services.to_string().contains(&sdk_b.url),
            "acme's GET /services: {services}"
        );

        // Tenant `default` has registered nothing.
        let default = As {
            url: url.clone(),
            key: ADMIN_KEY.into(),
            http: Client::new(),
        };
        let (code, body) = default.invoke("Greeter/hi", None).await?;
        ensure!(
            code == StatusCode::NOT_FOUND,
            "tenant default sees another tenant's service: {code} {body}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// Adversarial: acme's key with globex's **correct** object key ⇒ globex's state
/// does not leak into acme's run; globex's invocation id ⇒ `404`; the same
/// idempotency key ⇒ two different invocations.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_cannot_read_other_tenants_state() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object(
            "Vault",
            "put",
            vec![
                Step::SetState {
                    key: "secret".into(),
                    value: bytes::Bytes::from_static(b"\"globex-own\""),
                },
                Step::output("ok"),
            ],
        )
        .object("Vault", "peek", vec![Step::output("peeked")])
        .spawn()
        .await?;
    let node = authed_node(4).await?;
    let url = node.url();
    let r = within(TEST_DEADLINE, "tenant snooping on state", async {
        let acme = As::tenant(&url, "acme").await?;
        let globex = As::tenant(&url, "globex").await?;
        acme.register(&sdk.url).await?;
        globex.register(&sdk.url).await?;

        let (code, body) = globex.invoke("Vault/put?key=k-secret", None).await?;
        ensure!(code == StatusCode::OK, "globex put: {code} {body}");
        let globex_id = body["id"].as_str().unwrap_or_default().to_string();

        let state_seen_by = |id: &str| -> Vec<(bytes::Bytes, bytes::Bytes)> {
            sdk.starts()
                .into_iter()
                .find(|s| s.id_hex() == id)
                .map(|s| s.state)
                .unwrap_or_default()
        };
        // Evidence for the other direction: globex itself sees its state.
        let (code, body) = globex.invoke("Vault/peek?key=k-secret", None).await?;
        ensure!(code == StatusCode::OK, "globex peek: {code} {body}");
        let own = state_seen_by(body["id"].as_str().unwrap_or_default());
        ensure!(
            own.iter().any(|(k, _)| k.as_ref() == b"secret"),
            "globex must see its own state: {own:?}"
        );

        // acme, same object key: sees nothing.
        let (code, body) = acme.invoke("Vault/peek?key=k-secret", None).await?;
        ensure!(code == StatusCode::OK, "acme peek: {code} {body}");
        let theirs = state_seen_by(body["id"].as_str().unwrap_or_default());
        ensure!(
            theirs.is_empty(),
            "acme could read globex's state: {theirs:?}"
        );

        // acme, globex's id: does not exist — even with the admin role.
        for path in [
            format!("/invocations/{globex_id}"),
            format!("/invocations/{globex_id}/journal"),
        ] {
            let (code, body) = acme.get(&path).await?;
            ensure!(
                code == StatusCode::NOT_FOUND,
                "acme GET {path}: {code} {body}"
            );
        }
        let (code, body) = acme
            .send(
                acme.http
                    .post(format!("{url}/invocations/{globex_id}/cancel")),
            )
            .await?;
        ensure!(
            code == StatusCode::NOT_FOUND,
            "acme cancelled globex's invocation: {code} {body}"
        );
        let (code, _) = globex.get(&format!("/invocations/{globex_id}")).await?;
        ensure!(
            code == StatusCode::OK,
            "globex reads its own invocation: {code}"
        );

        // Same idempotency key ⇒ two invocations, one per tenant.
        let (_, a) = acme
            .invoke("Vault/peek?key=k-shared", Some("idem-1"))
            .await?;
        let (_, g) = globex
            .invoke("Vault/peek?key=k-shared", Some("idem-1"))
            .await?;
        ensure!(
            a["id"].is_string() && a["id"] != g["id"],
            "same idempotency key, two tenants, one invocation: {a} / {g}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// A range split does not mix tenants: the state of `Vault/k` in two tenants
/// (two different owners) follows its owner into the right child range, and
/// each tenant reads back exactly its own state after the split.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_rows_follow_their_owner_across_split() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object(
            "Vault",
            "put",
            vec![
                Step::SetState {
                    key: "secret".into(),
                    value: bytes::Bytes::from_static(b"\"x\""),
                },
                Step::output("ok"),
            ],
        )
        .object("Vault", "peek", vec![Step::output("peeked")])
        .spawn()
        .await?;
    let node = authed_node(1).await?;
    let url = node.url();
    let r = within(TEST_DEADLINE, "split a range with two tenants", async {
        let tenants = [
            As::tenant(&url, "acme").await?,
            As::tenant(&url, "globex").await?,
        ];
        for t in &tenants {
            t.register(&sdk.url).await?;
            let (code, body) = t.invoke("Vault/put?key=k", None).await?;
            ensure!(code == StatusCode::OK, "put: {code} {body}");
        }
        let r = Client::new()
            .post(format!("{url}/ranges/0/split"))
            .bearer_auth(ADMIN_KEY)
            .send()
            .await?;
        ensure!(r.status() == StatusCode::OK, "split: {}", r.status());
        for t in &tenants {
            let (code, body) = t.invoke("Vault/peek?key=k", None).await?;
            ensure!(code == StatusCode::OK, "peek after split: {code} {body}");
            let id = body["id"].as_str().unwrap_or_default().to_string();
            let seen = sdk
                .starts()
                .into_iter()
                .find(|s| s.id_hex() == id)
                .map(|s| s.state)
                .unwrap_or_default();
            ensure!(
                seen.len() == 1 && seen[0].0.as_ref() == b"secret",
                "after the split, a tenant sees foreign state: {seen:?}"
            );
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// `ctx.call` stays within the caller's tenant: a callee with the same service
/// name and the same object key in another tenant is never the target — even
/// when the call crosses ranges (cross-range messages carry the tenant).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctx_call_stays_in_callers_tenant() -> Result<()> {
    let sdk_a = FakeSdk::builder()
        .object(
            "Caller",
            "run",
            vec![
                Step::call("Callee", "echo", Some("shared")),
                Step::EchoCallResult,
            ],
        )
        .object("Callee", "echo", vec![Step::output("callee-acme")])
        .spawn()
        .await?;
    let sdk_b = FakeSdk::builder()
        .object("Callee", "echo", vec![Step::output("callee-globex")])
        .spawn()
        .await?;
    let node = authed_node(4).await?;
    let url = node.url();
    let r = within(TEST_DEADLINE, "ctx.call within tenant", async {
        let acme = As::tenant(&url, "acme").await?;
        let globex = As::tenant(&url, "globex").await?;
        acme.register(&sdk_a.url).await?;
        globex.register(&sdk_b.url).await?;
        for i in 0..4 {
            let (code, body) = acme
                .invoke(&format!("Caller/run?key=call-{i}"), None)
                .await?;
            ensure!(
                code == StatusCode::OK && body["output"] == "callee-acme",
                "run {i}: {code} {body}"
            );
        }
        ensure!(
            sdk_b.starts_of("Callee", "echo").is_empty(),
            "globex's callee was called by acme: {:?}",
            sdk_b.starts_of("Callee", "echo")
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// `awakeable_id_of_other_tenant_rejected`: the awakeable id of a tenant globex
/// invocation — even byte-exact — sent with acme's key gives `404`; with
/// globex's key it gets through.
#[tokio::test]
async fn awakeable_id_of_other_tenant_rejected() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object("Hook", "wait", vec![Step::Suspend, Step::output("done")])
        .spawn()
        .await?;
    let node = authed_node(2).await?;
    let url = node.url();
    let r = within(TEST_DEADLINE, "cross-tenant awakeable", async {
        let acme = As::tenant(&url, "acme").await?;
        let globex = As::tenant(&url, "globex").await?;
        globex.register(&sdk.url).await?;
        let (code, body) = globex
            .send(
                globex
                    .http
                    .post(format!("{url}/invoke/Hook/wait/send?key=k")),
            )
            .await?;
        ensure!(code.is_success(), "globex send: {code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        let awk = format!("awk_{id}{:08x}", 17);
        let resolve = |who: &As| {
            who.http
                .post(format!("{url}/awakeables/{awk}/resolve"))
                .header("content-type", "application/json")
                .body("1")
        };
        let (code, body) = acme.send(resolve(&acme)).await?;
        ensure!(
            code == StatusCode::NOT_FOUND,
            "acme completed globex's awakeable: {code} {body}"
        );
        let (code, body) = globex.send(resolve(&globex)).await?;
        ensure!(
            code == StatusCode::OK,
            "globex completes its own awakeable: {code} {body}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// `metering_counts_steps_per_tenant`: metering is split by tenant — three
/// invocations (each with one durable step: `Output`) of acme, one of globex.
#[tokio::test]
async fn metering_counts_steps_per_tenant() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object("Meter", "go", vec![Step::output("ok")])
        .spawn()
        .await?;
    let mut cfg = test_config(NodeMode::single(2));
    cfg.auth = cairn_server::auth::AuthConfig {
        required: true,
        admin_key: Some(ADMIN_KEY.into()),
        cluster_secret: None,
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    cfg.metering_flush = std::time::Duration::from_millis(100);
    let node = node::start(cfg).await.map_err(|e| anyhow!("{e}"))?;
    let url = node.url();
    let r = within(TEST_DEADLINE, "metering by tenant", async {
        let acme = As::tenant(&url, "acme-meter").await?;
        let globex = As::tenant(&url, "globex-meter").await?;
        acme.register(&sdk.url).await?;
        globex.register(&sdk.url).await?;
        for k in ["a", "b", "c"] {
            let (code, body) = acme.invoke(&format!("Meter/go?key={k}"), None).await?;
            ensure!(code == StatusCode::OK, "acme: {code} {body}");
        }
        let (code, body) = globex.invoke("Meter/go?key=a", None).await?;
        ensure!(code == StatusCode::OK, "globex: {code} {body}");
        // `invocations` is counted on receipt (`turn/admit.rs`), `steps` when the
        // command arrives — one flush (100 ms) can fall between the two, so wait
        // for globex's step too before comparing. Bounded: on timeout, compare
        // against what is currently visible.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let (_, a) = acme.get("/metering").await?;
            let (_, g) = globex.get("/metering").await?;
            let landed = a["steps"].as_u64() >= Some(3) && g["steps"].as_u64() >= Some(1);
            if a["invocations"] == 3
                && g["invocations"] == 1
                && (landed || std::time::Instant::now() > deadline)
            {
                ensure!(a["steps"].as_u64() >= Some(3), "acme steps: {a}");
                ensure!(
                    g["steps"].as_u64() >= Some(1) && g["steps"].as_u64() < a["steps"].as_u64(),
                    "globex steps: {g}"
                );
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let (code, _) = acme.get("/metering?tenant=globex-meter").await?;
        ensure!(
            code == StatusCode::FORBIDDEN,
            "acme's admin reads globex's metering: {code}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
