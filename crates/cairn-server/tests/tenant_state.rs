// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Tenant states `limited`/`suspended`.
//!
//! | Source of work | `limited` | `suspended` |
//! |---|---|---|
//! | ingress (`/invoke`, `/send`, workflow, `restart`, manual schedule fire) | `429 tenant_limited` | `403 tenant_suspended` |
//! | schedules | the fire is skipped (`LastKind::Skipped`) | like `limited` |
//! | child calls, cross-range `StartCall` | created normally | created normally, not granted a slot |
//! | running work, timers, retries, completions | runs to its outcome | the scheduler grants no slot (`ResourceKind::Tenant`), reschedules after `suspended_retry`, `attempt` unchanged; a turn that already has a slot stops right before its next SDK call (`tests/failpoints.rs::suspended_tenant_*`) |
//! | `POST /deployments` | passes | `403 tenant_suspended` (discovery is a request to the tenant's endpoint) |
//!
//! The state only changes through `PUT /admin/tenants/{t}/state` (the
//! `Provision` capability), which returns `204` once every live session has
//! applied the book, `202 {pending}` otherwise (like key revocation); an
//! unknown value in the book ⇒ `suspended`.

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
        within, ClusterNode, TEST_DEADLINE,
    },
    reqwest::{Client, StatusCode},
    serde_json::{json, Value},
    slatedb::object_store::{path::Path, ObjectStoreExt, PutPayload},
    std::time::Duration,
};

const ADMIN_KEY: &str = "cf_test_tenant_state_bootstrap_key_01234";
const SECRET: &str = "cf_test_cluster_secret_tenant_state_0123456789";
const TTL: Duration = Duration::from_secs(2);
/// The reschedule beat of a suspended tenant's turns in tests (product: 15 minutes).
const SUSPENDED_RETRY: Duration = Duration::from_secs(3);

/// The tenant suspension tests run one at a time: they count a process-wide
/// metric (`cairnflow_flow_blocked_total{resource="tenant"}`).
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn authed(bucket: &common::Bucket, id: &str) -> Result<ClusterNode> {
    common::start_cluster_node_tweaked(bucket, id, 2, TTL, |cfg| {
        cfg.auth = AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: Some(SECRET.into()),
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
        cfg.limits.suspended_retry = SUSPENDED_RETRY;
        cfg.deploy_refresh = Duration::from_millis(100);
    })
    .await
}

/// A client bound to one tenant's key.
#[derive(Clone)]
struct As {
    url: String,
    key: String,
    http: Client,
}

impl As {
    fn admin(url: &str) -> Self {
        Self {
            url: url.into(),
            key: ADMIN_KEY.into(),
            http: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
        }
    }

    /// A `role` key for `tenant`, created with the bootstrap key (retried until
    /// the node takes its lease).
    async fn tenant(url: &str, tenant: &str, role: &str) -> Result<Self> {
        let admin = Self::admin(url);
        let t0 = std::time::Instant::now();
        loop {
            let (code, v) = admin
                .send(
                    admin
                        .http
                        .post(format!("{url}/admin/keys"))
                        .json(&json!({ "tenant": tenant, "role": role })),
                )
                .await?;
            if code == StatusCode::OK {
                return Ok(Self {
                    key: v["secret"].as_str().unwrap_or_default().to_string(),
                    ..admin
                });
            }
            ensure!(
                t0.elapsed() < TEST_DEADLINE,
                "create key {tenant}: {code} {v}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn at(&self, url: &str) -> Self {
        Self {
            url: url.into(),
            ..self.clone()
        }
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

    async fn register(&self, sdk: &str) -> Result<(StatusCode, Value)> {
        self.send(
            self.http
                .post(format!("{}/deployments", self.url))
                .json(&json!({ "uri": sdk })),
        )
        .await
    }

    async fn post(&self, path: &str, body: Value) -> Result<(StatusCode, Value)> {
        self.send(
            self.http
                .post(format!("{}{path}", self.url))
                .body(body.to_string()),
        )
        .await
    }

    /// `POST /admin/keys` for `tenant` with role `role` (no retry).
    async fn new_key(&self, tenant: &str, role: &str) -> Result<(StatusCode, Value)> {
        self.send(
            self.http
                .post(format!("{}/admin/keys", self.url))
                .json(&json!({ "tenant": tenant, "role": role })),
        )
        .await
    }

    async fn put(&self, path: &str, body: Value) -> Result<(StatusCode, Value)> {
        self.send(self.http.put(format!("{}{path}", self.url)).json(&body))
            .await
    }

    async fn get(&self, path: &str) -> Result<(StatusCode, Value)> {
        self.send(self.http.get(format!("{}{path}", self.url)))
            .await
    }

    /// `PUT /admin/tenants/{tenant}/state`.
    async fn set_state(&self, tenant: &str, state: &str) -> Result<(StatusCode, Value)> {
        self.put(
            &format!("/admin/tenants/{tenant}/state"),
            json!({ "state": state }),
        )
        .await
    }

    /// One `POST /query` statement; returns the rows.
    async fn query(&self, sql: &str) -> Result<Vec<Value>> {
        let (code, v) = self
            .send(
                self.http
                    .post(format!("{}/query", self.url))
                    .json(&json!({ "sql": sql })),
            )
            .await?;
        ensure!(code == StatusCode::OK, "query {sql}: {code} {v}");
        Ok(v["rows"].as_array().cloned().unwrap_or_default())
    }

    async fn blocked_tenant(&self) -> Result<u64> {
        let text = self
            .http
            .get(format!("{}/metrics", self.url))
            .bearer_auth(ADMIN_KEY)
            .send()
            .await?
            .text()
            .await?;
        Ok(text
            .lines()
            .find(|l| l.starts_with("cairnflow_flow_blocked_total{resource=\"tenant\"}"))
            .and_then(|l| l.rsplit(' ').next())
            .and_then(|n| n.parse().ok())
            .unwrap_or(0))
    }
}

/// Set the state and require `204` (every node has applied it).
async fn set_state(admin: &As, tenant: &str, state: &str) -> Result<()> {
    let (code, v) = admin.set_state(tenant, state).await?;
    ensure!(
        code == StatusCode::NO_CONTENT,
        "set {tenant}={state}: {code} {v}"
    );
    Ok(())
}

fn id_of(v: &Value) -> Result<String> {
    v["id"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("no id: {v}"))
}

/// Wait until `GET /invocations/{id}` shows `status`.
async fn wait_status(who: &As, id: &str, status: &str) -> Result<Value> {
    let t0 = std::time::Instant::now();
    loop {
        let (code, v) = who.get(&format!("/invocations/{id}")).await?;
        if code == StatusCode::OK && v["status"] == status {
            return Ok(v);
        }
        ensure!(
            t0.elapsed() < TEST_DEADLINE,
            "{id} not {status} yet: {code} {v}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// **New work is refused** according to the state, at every ingress entrance:
///
/// 1. `limited`: `/invoke`, `/send`, a workflow's `run` turn, `restart`, a
///    manual schedule fire ⇒ `429 tenant_limited`, no `id`, nothing created
///    (the SDK receives no new `Start`); `POST /deployments` still passes;
///    other tenants run normally; `GET /tenant` returns `state: limited`.
/// 2. `suspended`: the same entrances ⇒ `403 tenant_suspended`;
///    `POST /deployments` also `403` (discovery is a request to the tenant's
///    endpoint).
/// 3. `active` again ⇒ runs as usual.
/// 4. The tenant's own `admin` key cannot change the state (`403`); an unknown
///    value ⇒ `400`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_work_is_refused_by_state() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let sdk = FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .workflow("Flow", "run", vec![Step::output("ran")])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "refuse new work", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme", "admin").await?;
        let other = As::tenant(&url, "other", "admin").await?;
        acme.register(&sdk.url).await?;
        other.register(&sdk.url).await?;
        let (code, v) = acme.post("/invoke/Echo/hi", json!("x")).await?;
        ensure!(code == StatusCode::OK, "before suspending: {code} {v}");
        let done = id_of(&v)?;
        let (code, v) = acme
            .put(
                "/schedules/tick",
                json!({ "everyMs": 60_000, "target": { "service": "Echo", "handler": "hi" } }),
            )
            .await?;
        ensure!(code == StatusCode::OK, "set schedule: {code} {v}");

        let (code, v) = acme.set_state("acme", "suspended").await?;
        ensure!(
            code == StatusCode::FORBIDDEN,
            "admin changing its own state: {code} {v}"
        );
        let (code, v) = admin.set_state("acme", "paused").await?;
        ensure!(code == StatusCode::BAD_REQUEST, "unknown value: {code} {v}");

        for (state, want, wcode) in [
            ("limited", StatusCode::TOO_MANY_REQUESTS, "tenant_limited"),
            ("suspended", StatusCode::FORBIDDEN, "tenant_suspended"),
        ] {
            set_state(&admin, "acme", state).await?;
            let before = sdk.starts_len();
            for (path, body) in [
                ("/invoke/Echo/hi", json!("x")),
                ("/invoke/Echo/hi/send", json!("x")),
                ("/invoke/Flow/run?key=w1", json!("x")),
                (&*format!("/invocations/{done}/restart"), Value::Null),
                ("/schedules/tick/trigger", Value::Null),
            ] {
                let (code, v) = acme.post(path, body).await?;
                ensure!(
                    code == want && v["code"] == wcode && v.get("id").is_none(),
                    "{state} {path}: {code} {v}"
                );
            }
            let (code, v) = acme.register(&sdk.url).await?;
            let deploy_ok = state == "limited";
            ensure!(
                (code == StatusCode::OK) == deploy_ok
                    && (deploy_ok || v["code"] == "tenant_suspended"),
                "{state} POST /deployments: {code} {v}"
            );
            let (code, v) = acme.get("/tenant").await?;
            ensure!(code == StatusCode::OK && v["state"] == state, "{v}");
            let (code, v) = other.post("/invoke/Echo/hi", json!("x")).await?;
            ensure!(
                code == StatusCode::OK,
                "other tenant while acme is {state}: {code} {v}"
            );
            ensure!(
                sdk.starts_len() == before + 1,
                "{state}: SDK received a Start of acme's: {} ⇒ {}",
                before,
                sdk.starts_len()
            );
        }
        set_state(&admin, "acme", "active").await?;
        let (code, v) = acme.post("/invoke/Echo/hi", json!("x")).await?;
        ensure!(code == StatusCode::OK, "active again: {code} {v}");
        let (_, v) = acme.get("/tenant").await?;
        ensure!(v["state"] == "active", "{v}");
        Ok(())
    })
    .await
}

/// **Schedules do not fire** while the tenant is `limited`/`suspended`: a due
/// fire records a `skipped` entry in `last` (`LastKind::Skipped`), `skipped`
/// grows, `fired` stays still, there is no run; when the tenant is `active`
/// again the next mark fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schedules_skip_while_refused() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let sdk = FakeSdk::builder()
        .service("Report", "build", vec![Step::output("ok")])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "schedule skips its fire", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme", "admin").await?;
        acme.register(&sdk.url).await?;
        let (code, v) = acme
            .put(
                "/schedules/nightly",
                json!({ "everyMs": 60_000, "target": { "service": "Report", "handler": "build" } }),
            )
            .await?;
        ensure!(code == StatusCode::OK, "set schedule: {code} {v}");
        let mut at = v["nextAt"].as_u64().ok_or_else(|| anyhow!("nextAt {v}"))?;
        for (i, state) in ["limited", "suspended"].into_iter().enumerate() {
            set_state(&admin, "acme", state).await?;
            let (code, v) = admin
                .post(&format!("/admin/tick?now_ms={at}"), Value::Null)
                .await?;
            ensure!(code.is_success(), "tick: {code} {v}");
            let (_, g) = acme.get("/schedules/nightly").await?;
            ensure!(
                g["skipped"] == i as u64 + 1
                    && g["fired"] == 0
                    && newest(&g)["outcome"] == "skipped"
                    && g["nextAt"] == at + 60_000,
                "{state}: {g}"
            );
            at += 60_000;
        }
        ensure!(sdk.starts_len() == 0, "refused tenant's schedule runs");
        set_state(&admin, "acme", "active").await?;
        admin
            .post(&format!("/admin/tick?now_ms={at}"), Value::Null)
            .await?;
        let (_, g) = acme.get("/schedules/nightly").await?;
        ensure!(
            g["fired"] == 1 && newest(&g)["outcome"] == "fired",
            "active again: {g}"
        );
        let run = newest(&g)["id"].as_str().unwrap_or_default().to_string();
        wait_status(&acme, &run, "completed").await?;
        Ok(())
    })
    .await
}

/// The newest entry in a schedule's `last` (oldest first, newest last).
fn newest(g: &Value) -> Value {
    g["last"]
        .as_array()
        .and_then(|l| l.last().cloned())
        .unwrap_or_default()
}

/// **Suspension calls no endpoint and spends no commit; lifting it carries on.**
///
/// 1. `Retry/once` (a gate holding the turn): while the first turn is inside the
///    SDK, suspend `acme` (`204`); open the gate ⇒ that turn fails in a
///    retryable way (`attempt 0`). `Nap/once` is sleeping on a 1.5 s timer.
/// 2. While suspended (≥ 2 reschedule beats): the SDK receives **no** `Start`
///    of `acme`'s; the number of times the scheduler refuses a slot because of
///    the tenant (`blocked{tenant}`) is bounded by the reschedule beat — not by
///    the handler's backoff (the core rule `due = max(row.due, now +
///    after_ms)`); the `retry_at` of the failed invocation is at least half a
///    beat after the query time.
/// 3. Lift the suspension ⇒ both complete within one reschedule beat; the retry
///    turn carries `attempt 1` (a refused turn does not count as an attempt).
/// 4. Another tenant with the same handler runs normally throughout the
///    suspension.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn suspended_tenant_gets_no_turns() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let sdk = FakeSdk::builder()
        .gated_service(
            "Retry",
            "once",
            vec![Step::FailBelow(1), Step::output("ok")],
        )
        .service(
            "Nap",
            "once",
            vec![Step::Sleep(1_500), Step::output("woke")],
        )
        .service("Echo", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "suspension grants no slot", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme", "admin").await?;
        let other = As::tenant(&url, "other", "admin").await?;
        acme.register(&sdk.url).await?;
        other.register(&sdk.url).await?;
        let (code, v) = acme.post("/invoke/Retry/once/send", json!("x")).await?;
        ensure!(code.is_success(), "{code} {v}");
        let retry = id_of(&v)?;
        let (code, v) = acme.post("/invoke/Nap/once/send", json!("x")).await?;
        ensure!(code.is_success(), "{code} {v}");
        let nap = id_of(&v)?;
        // `Retry`'s first turn stands at the gate, `Nap` is asleep.
        let t0 = std::time::Instant::now();
        while sdk.starts_len() < 2 {
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "SDK has not received two Starts"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        wait_status(&acme, &nap, "suspended").await?;
        set_state(&admin, "acme", "suspended").await?;
        let blocked0 = admin.blocked_tenant().await?;
        let starts0 = sdk.starts_of("Retry", "once").len() + sdk.starts_of("Nap", "once").len();
        sdk.gate().release(1);

        let window = SUSPENDED_RETRY * 2 + Duration::from_millis(500);
        let t0 = std::time::Instant::now();
        while t0.elapsed() < window {
            let (code, v) = other.post("/invoke/Echo/hi", json!("x")).await?;
            ensure!(
                code == StatusCode::OK,
                "other tenant while acme is suspended: {code} {v}"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let starts = sdk.starts_of("Retry", "once").len() + sdk.starts_of("Nap", "once").len();
        ensure!(
            starts == starts0,
            "SDK received turns of a suspended tenant: {starts0} ⇒ {starts}"
        );
        let blocked = admin.blocked_tenant().await? - blocked0;
        // Two invocations, each ≤ once per reschedule beat (+ the first time).
        let cap = 2 * (window.as_millis() / SUSPENDED_RETRY.as_millis() + 2) as u64;
        ensure!(
            (1..=cap).contains(&blocked),
            "slot refused {blocked} times in {window:?} (limit {cap})"
        );
        // The refused turn's scheduled row lies ≥ half a reschedule beat ahead — not
        // a step of the handler's backoff (100 ms · 2^n).
        let far = admin
            .query(&format!(
                "SELECT count(*) FROM sys_invocation WHERE id = '{retry}' \
                 AND retry_at > now() + INTERVAL '1 second'"
            ))
            .await?;
        ensure!(
            far == [json!([1])],
            "retry_at of a refused turn too close (the row keeps the handler's backoff step): {far:?}"
        );

        set_state(&admin, "acme", "active").await?;
        sdk.gate().open();
        wait_status(&acme, &retry, "completed").await?;
        wait_status(&acme, &nap, "completed").await?;
        let attempts: Vec<u32> = sdk
            .starts_of("Retry", "once")
            .iter()
            .map(|s| s.attempt)
            .collect();
        ensure!(attempts == [0, 1], "attempts after lifting the suspension: {attempts:?}");
        Ok(())
    })
    .await
}

/// **Lifting a suspension runs the turns scheduled during it at once** (found
/// in a chaos run): the node's reschedule beat here is 10 minutes (close to the
/// product's 15 minutes), not 3 s.
///
/// 1. `Retry/once` stands at the gate, `Nap/once` sleeps on a 1.5 s timer;
///    suspend `acme`.
/// 2. Open the gate ⇒ the `Retry` turn fails in a retryable way; the retry turn
///    and `Nap`'s wake-up turn are refused a slot by the scheduler ⇒ rows
///    scheduled 10 minutes ahead.
/// 3. Lift the suspension (`204`).
///
/// Must see: both `completed` within 15 s of the `204`. With the old code: a
/// wait of the full 10 minutes (lifting only woke the in-RAM queue, it did not
/// pull the durable scheduled rows back).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsuspend_runs_parked_turns_at_once() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let sdk = FakeSdk::builder()
        .gated_service(
            "Retry",
            "once",
            vec![Step::FailBelow(1), Step::output("ok")],
        )
        .service(
            "Nap",
            "once",
            vec![Step::Sleep(1_500), Step::output("woke")],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = common::start_cluster_node_tweaked(&bucket, "n1", 2, TTL, |cfg| {
        cfg.auth = AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: Some(SECRET.into()),
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
        cfg.limits.suspended_retry = Duration::from_secs(600);
        cfg.deploy_refresh = Duration::from_millis(100);
    })
    .await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "lifting runs at once", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme", "admin").await?;
        acme.register(&sdk.url).await?;
        let (code, v) = acme.post("/invoke/Retry/once/send", json!("x")).await?;
        ensure!(code.is_success(), "{code} {v}");
        let retry = id_of(&v)?;
        let (code, v) = acme.post("/invoke/Nap/once/send", json!("x")).await?;
        ensure!(code.is_success(), "{code} {v}");
        let nap = id_of(&v)?;
        let t0 = std::time::Instant::now();
        while sdk.starts_len() < 2 {
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "SDK has not received two Starts"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        wait_status(&acme, &nap, "suspended").await?;
        set_state(&admin, "acme", "suspended").await?;
        let blocked0 = admin.blocked_tenant().await?;
        sdk.gate().release(1);
        // Both turns (`Retry`'s retry, `Nap`'s wake-up) have been refused a slot
        // and rescheduled.
        let t0 = std::time::Instant::now();
        while admin.blocked_tenant().await? < blocked0 + 2 {
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "the two turns have not been refused a slot yet"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        set_state(&admin, "acme", "active").await?;
        sdk.gate().open();
        let t0 = std::time::Instant::now();
        for id in [&retry, &nap] {
            loop {
                let (code, v) = acme.get(&format!("/invocations/{id}")).await?;
                if code == StatusCode::OK && v["status"] == "completed" {
                    break;
                }
                ensure!(
                    t0.elapsed() < Duration::from_secs(15),
                    "{id} not done 15 s after lifting (the row scheduled during the suspension still waits for the 10-minute beat): {code} {v}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        Ok(())
    })
    .await
}

/// A cluster node with auth and the suspended-tenant reschedule beat at **the
/// product default** (15 minutes), with lease TTL `ttl`.
async fn default_retry_node(
    bucket: &common::Bucket,
    id: &str,
    ttl: Duration,
) -> Result<ClusterNode> {
    common::start_cluster_node_tweaked(bucket, id, 2, ttl, |cfg| {
        cfg.auth = AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: Some(SECRET.into()),
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
        cfg.deploy_refresh = Duration::from_millis(100);
    })
    .await
}

/// `node` leads at least one range of the table (probed like `common::start_led`).
async fn leads_any(node: &cairn_server::node::RunningNode) -> bool {
    for r in node.cluster.table().ranges() {
        let probe = cairn_store::make_invocation_id(cairn_store::PartitionKey(r.start), [0; 16]);
        if let Some(h) = node.partitions.get(r.id) {
            if matches!(
                tokio::time::timeout(Duration::from_millis(300), h.status(probe)).await,
                Ok(Ok(_))
            ) {
                return true;
            }
        }
    }
    false
}

/// **Lifting a suspension during a leader change: a new leader that has never
/// seen the tenant suspended still runs the turns scheduled during the
/// suspension at once** (there was no deterministic test for this gap). The
/// reschedule beat is **the product default** (15 minutes), not lowered.
///
/// 1. `n1` leads both ranges. `Retry/once` stands at the gate, `Nap/once` sleeps
///    on a 1.5 s timer; suspend `acme`; open the gate ⇒ `Retry`'s retry turn and
///    `Nap`'s wake-up turn are refused a slot ⇒ two rows scheduled 15 minutes
///    ahead.
/// 2. Kill `n1` (`kill -9`: its lease stays valid until the 5 s TTL runs out).
/// 3. Build `n2`, lift the suspension **through `n2`** while it leads no range
///    yet (the test checks this gap right before the command — if `n2` already
///    leads, the gap is broken and the test goes red with a clear message, not a
///    false green).
/// 4. `n2` takes both ranges after the TTL, with `acme` `active` from its first
///    read of the book.
///
/// Must see: both `completed` within 60 s of the lift command. With the old
/// code: a wait of the full 15 minutes — lifting only pulled the scheduled rows
/// on a leader that **saw** the suspended set change (`Scheduler::set_refused`),
/// and the new leader saw no change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsuspend_across_leader_change_runs_parked_turns() -> Result<()> {
    let _serial = SERIAL.lock().await;
    const TTL5: Duration = Duration::from_secs(5);
    let sdk = FakeSdk::builder()
        .gated_service(
            "Retry",
            "once",
            vec![Step::FailBelow(1), Step::output("ok")],
        )
        .service(
            "Nap",
            "once",
            vec![Step::Sleep(1_500), Step::output("woke")],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let n1 = default_retry_node(&bucket, "n1", TTL5).await?;
    let url1 = n1.node.url();
    let (retry, nap, acme) = within(TEST_DEADLINE, "scheduled while suspended on n1", async {
        let admin = As::admin(&url1);
        let acme = As::tenant(&url1, "acme", "admin").await?;
        acme.register(&sdk.url).await?;
        let (code, v) = acme.post("/invoke/Retry/once/send", json!("x")).await?;
        ensure!(code.is_success(), "{code} {v}");
        let retry = id_of(&v)?;
        let (code, v) = acme.post("/invoke/Nap/once/send", json!("x")).await?;
        ensure!(code.is_success(), "{code} {v}");
        let nap = id_of(&v)?;
        let t0 = std::time::Instant::now();
        while sdk.starts_len() < 2 {
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "SDK has not received two Starts"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        wait_status(&acme, &nap, "suspended").await?;
        set_state(&admin, "acme", "suspended").await?;
        let blocked0 = admin.blocked_tenant().await?;
        sdk.gate().release(1);
        let t0 = std::time::Instant::now();
        while admin.blocked_tenant().await? < blocked0 + 2 {
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "the two turns have not been refused a slot yet"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok((retry, nap, acme))
    })
    .await?;
    // The retry turn after lifting the suspension runs to its result.
    sdk.gate().open();
    drop(n1);
    let n2 = default_retry_node(&bucket, "n2", TTL5).await?;
    let url2 = n2.node.url();
    within(Duration::from_secs(90), "lift on the new leader", async {
        let admin = As::admin(&url2);
        let acme = acme.at(&url2);
        // The gap is checked right before the command: the book is written within
        // milliseconds of it, while the answer waits for `n1`'s record to turn `expired`
        // (the request asks a dead peer again until its session ends) — by then `n2` may
        // already lead.
        ensure!(
            !leads_any(&n2.node).await,
            "the test's gap is broken: n2 led a range before the lift (TTL too short?)"
        );
        let (code, v) = admin.set_state("acme", "active").await?;
        ensure!(
            code == StatusCode::NO_CONTENT || code == StatusCode::ACCEPTED,
            "lift through n2: {code} {v}"
        );
        let t0 = std::time::Instant::now();
        for id in [&retry, &nap] {
            loop {
                let (code, v) = acme.get(&format!("/invocations/{id}")).await?;
                if code == StatusCode::OK && v["status"] == "completed" {
                    break;
                }
                ensure!(
                    t0.elapsed() < Duration::from_secs(60),
                    "{id} not done 60 s after the lift (the new leader keeps the row scheduled during the suspension until the 15-minute beat): {code} {v}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        Ok(())
    })
    .await
}

/// The ms value of flag `name` on line `line` of the `tenants` book.
fn mark_of(line: &str, name: &str) -> Option<u64> {
    line.split(' ')
        .find_map(|f| f.strip_prefix(name))
        .and_then(|v| v.parse().ok())
}

/// **Several suspend/lift rounds within one reschedule beat, a leader coming up
/// after both: the first round's scheduled rows run exactly once, and an
/// ordinary retry outside the window does not run early.** The product's
/// default reschedule beat (15 minutes).
///
/// 1. `n1` leads the range. `Slow/once` fails the first time with
///    `retryPolicyInitialInterval` = `retryPolicyMaxInterval` = 240 s ⇒ an
///    ordinary retry row due 240 s later. `Retry/once` stands at the gate,
///    `Nap/once` sleeps on a 1.5 s timer; suspend `acme` (mark `refused-ms=` =
///    S1); open the gate ⇒ `Retry`'s retry turn and `Nap`'s wake-up turn are
///    scheduled 15 minutes ahead.
/// 2. Kill `n1`; no node leads the range until step 4.
/// 3. Change the state the way `PUT …/state` does
///    (`TenantSettings::try_update` + `change_state`, no ACK since no node is
///    alive): lift; wait until the first round's scheduled time is more than
///    `LIFT_SLACK` (62 s) past; suspend again; lift. The book must keep
///    `refused-ms=` = S1 (the first round still has rows that may wait) and
///    `lifted-ms=` = the second lift. With the second round's `refused-ms=`,
///    the scan window would start after the due time of the first round's rows
///    — they would wait the full 15 minutes.
/// 4. Build `n2`.
///
/// Must see: `Retry` and `Nap` `completed` within 60 s of building `n2`, the SDK
/// receives exactly two `Start`s for each (no duplicate turn); `Slow` still has
/// exactly one `Start` (the ordinary retry row due in 240 s lies outside the
/// suspension round's window — it does not fire early).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_suspensions_in_one_window_are_swept_once_by_a_new_leader() -> Result<()> {
    let _serial = SERIAL.lock().await;
    const TTL5: Duration = Duration::from_secs(5);
    let sdk = FakeSdk::builder()
        .gated_service(
            "Retry",
            "once",
            vec![Step::FailBelow(1), Step::output("ok")],
        )
        .service(
            "Nap",
            "once",
            vec![Step::Sleep(1_500), Step::output("woke")],
        )
        .service(
            "Slow",
            "once",
            vec![Step::FailBelow(1), Step::output("late")],
        )
        .contract(
            "Slow",
            "once",
            cairn_server::discovery::Contract {
                retry_policy_initial_interval: Some(240_000),
                // The default limit of 60 s would clamp away the 240 s interval.
                retry_policy_max_interval: Some(240_000),
                ..Default::default()
            },
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let n1 = default_retry_node(&bucket, "n1", TTL5).await?;
    let url1 = n1.node.url();
    let acme_id = cairn_store::TenantId::new("acme")?;
    let (ids, acme, parked_at, s1) = within(TEST_DEADLINE, "parked on n1", async {
        let admin = As::admin(&url1);
        let acme = As::tenant(&url1, "acme", "admin").await?;
        acme.register(&sdk.url).await?;
        let mut ids = Vec::new();
        for svc in ["Slow", "Retry", "Nap"] {
            let (code, v) = acme
                .post(&format!("/invoke/{svc}/once/send"), json!("x"))
                .await?;
            ensure!(code.is_success(), "{svc}: {code} {v}");
            ids.push(id_of(&v)?);
        }
        let t0 = std::time::Instant::now();
        while sdk.starts_of("Slow", "once").is_empty() || sdk.starts_len() < 3 {
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "SDK has not received three Starts"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        wait_status(&acme, &ids[2], "suspended").await?;
        set_state(&admin, "acme", "suspended").await?;
        let s1 = mark_of(&book_line(&bucket, "acme").await?, "refused-ms=")
            .ok_or_else(|| anyhow!("suspension did not write refused-ms="))?;
        let blocked0 = admin.blocked_tenant().await?;
        sdk.gate().release(1);
        let t0 = std::time::Instant::now();
        while admin.blocked_tenant().await? < blocked0 + 2 {
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "the two turns have not been refused a slot yet"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok((ids, acme, std::time::Instant::now(), s1))
    })
    .await?;
    sdk.gate().open();
    drop(n1);

    // 3. Two lift/suspend/lift rounds while no node leads the range.
    let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
    let prefix = Path::from("");
    let settings = cairn_server::tenants::TenantSettings::new(Some((store, &prefix)));
    let change = |to: cairn_engine::TenantState| {
        let settings = settings.clone();
        let acme_id = acme_id.clone();
        async move {
            let now = cairn_store::entropy::now_ms();
            settings
                .try_update(&acme_id, |f| f.change_state(&acme_id, to, now).map(|_| ()))
                .await
                .map_err(|e| anyhow!("{e}"))?
                .map_err(|e| anyhow!("{e:?}"))?;
            anyhow::Ok(now)
        }
    };
    change(cairn_engine::TenantState::Active).await?;
    let gap = Duration::from_secs(62);
    if let Some(left) = gap.checked_sub(parked_at.elapsed()) {
        tokio::time::sleep(left).await;
    }
    change(cairn_engine::TenantState::Suspended).await?;
    let l2 = change(cairn_engine::TenantState::Active).await?;
    let line = book_line(&bucket, "acme").await?;
    ensure!(
        mark_of(&line, "refused-ms=") == Some(s1) && mark_of(&line, "lifted-ms=") == Some(l2),
        "marks after two rounds (must keep the first round's refused-ms={s1}, lifted-ms={l2}): {line:?}"
    );

    // 4.
    let n2 = default_retry_node(&bucket, "n2", TTL5).await?;
    let url2 = n2.node.url();
    within(Duration::from_secs(90), "new leader scans by the marks", async {
        let acme = acme.at(&url2);
        let t0 = std::time::Instant::now();
        for id in &ids[1..] {
            loop {
                let (code, v) = acme.get(&format!("/invocations/{id}")).await?;
                if code == StatusCode::OK && v["status"] == "completed" {
                    break;
                }
                ensure!(
                    t0.elapsed() < Duration::from_secs(60),
                    "{id} not done 60 s after n2 came up (the first suspension round's rows lie outside the scan window): {code} {v}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        for (svc, want) in [("Retry", 2), ("Nap", 2), ("Slow", 1)] {
            let n = sdk.starts_of(svc, "once").len();
            ensure!(
                n == want,
                "{svc}: SDK received {n} Starts, must be exactly {want} (a duplicate turn, or an ordinary retry firing early)"
            );
        }
        let (_, v) = acme.get(&format!("/invocations/{}", ids[0])).await?;
        ensure!(v["status"] == "running", "Slow must still wait for its 240 s retry: {v}");
        Ok(())
    })
    .await
}

/// **An unknown value ⇒ `suspended`** (fail safe): a book written by hand with
/// `state=archived` (a state of a newer binary) ⇒ the tenant counts as
/// suspended — `403`, `GET /tenant` returns `suspended` — not `active`; the book
/// is still readable (other lines are not broken); writing another field keeps
/// `state=archived` verbatim. (`deleting` used to be an unknown value here; it
/// is now a known state — [`deleting_refuses_access_and_reverts_until_purging`].)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn strange_state_is_suspended() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let sdk = FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "unknown value", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme", "admin").await?;
        let other = As::tenant(&url, "other", "admin").await?;
        acme.register(&sdk.url).await?;
        other.register(&sdk.url).await?;
        bucket
            .put(
                &Path::from("tenants"),
                PutPayload::from(
                    "cairnflow-tenants/1\nacme state=archived\nother slow-ms=10\n".to_string(),
                ),
            )
            .await?;
        // `GET /admin/tenants` reads the book again.
        let (code, v) = admin.get("/admin/tenants").await?;
        ensure!(code == StatusCode::OK, "{code} {v}");
        let (code, v) = acme.post("/invoke/Echo/hi", json!("x")).await?;
        ensure!(
            code == StatusCode::FORBIDDEN && v["code"] == "tenant_suspended",
            "state=archived: {code} {v}"
        );
        let (_, v) = acme.get("/tenant").await?;
        ensure!(v["state"] == "suspended", "{v}");
        let (code, v) = other.post("/invoke/Echo/hi", json!("x")).await?;
        ensure!(code == StatusCode::OK, "other lines readable: {code} {v}");
        let (code, v) = admin
            .put("/admin/tenants/acme", json!({ "slowMs": 50 }))
            .await?;
        ensure!(code == StatusCode::OK, "{code} {v}");
        let raw = bucket.get(&Path::from("tenants")).await?.bytes().await?;
        let raw = String::from_utf8_lossy(&raw);
        ensure!(
            raw.contains("acme state=archived"),
            "verbatim value lost: {raw}"
        );
        Ok(())
    })
    .await
}

/// Read the `tenant` line of the `tenants` book on the bucket.
async fn book_line(bucket: &common::Bucket, tenant: &str) -> Result<String> {
    let raw = bucket.get(&Path::from("tenants")).await?.bytes().await?;
    Ok(String::from_utf8_lossy(&raw)
        .lines()
        .find(|l| l.split(' ').next() == Some(tenant))
        .unwrap_or_default()
        .to_string())
}

/// Write the `tenants` book directly, then let the node read it again (`GET /admin/tenants`).
async fn write_book(bucket: &common::Bucket, admin: &As, body: &str) -> Result<()> {
    bucket
        .put(&Path::from("tenants"), PutPayload::from(body.to_string()))
        .await?;
    let (code, v) = admin.get("/admin/tenants").await?;
    ensure!(code == StatusCode::OK, "{code} {v}");
    Ok(())
}

/// **`deleting` — phase A of a tenant purge**:
///
/// 1. `PUT …/state deleting` (the `tenants/purge` gate is already on in a new
///    cluster) ⇒ `204`; the book carries `state=deleting deleting-ms=<that
///    time>`; sending again ⇒ `204`, the mark unchanged; tenant `default` ⇒
///    `400`.
/// 2. Every request with the tenant's key — new work, reading an invocation,
///    `GET /tenant` — ⇒ `403 tenant_deleting`; other tenants run normally; a
///    cluster key's `GET /admin/tenants` sees `deleting`; creating a new key
///    for the tenant ⇒ `409 tenant_deleting`.
/// 3. `PUT …/state suspended` (reverting before `purging-ms=` exists) ⇒ `204`,
///    the book no longer has `deleting-ms=`; `active` again ⇒ the old
///    invocation and schedule are intact, the old key works again.
/// 4. The book carries `purging-ms=` (phase B has started) ⇒ every `PUT …/state`
///    other than `deleting` ⇒ `409 tenant_purging`.
/// 5. The book carries `state=purged` (the tombstone) ⇒ every `PUT …/state` ⇒
///    `409 tenant_purged`, creating a key ⇒ `409 tenant_purged`, an old key ⇒
///    `403 tenant_deleting`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_refuses_access_and_reverts_until_purging() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let sdk = FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "deleting", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme", "admin").await?;
        let other = As::tenant(&url, "other", "admin").await?;
        acme.register(&sdk.url).await?;
        other.register(&sdk.url).await?;
        let (code, v) = acme.post("/invoke/Echo/hi", json!("x")).await?;
        ensure!(code == StatusCode::OK, "before deleting: {code} {v}");
        let done = id_of(&v)?;
        wait_status(&acme, &done, "completed").await?;
        let (code, v) = acme
            .put(
                "/schedules/tick",
                json!({ "everyMs": 60_000, "target": { "service": "Echo", "handler": "hi" } }),
            )
            .await?;
        ensure!(code == StatusCode::OK, "set schedule: {code} {v}");

        // 1.
        let (code, v) = admin.set_state("default", "deleting").await?;
        ensure!(code == StatusCode::BAD_REQUEST, "del default: {code} {v}");
        set_state(&admin, "acme", "deleting").await?;
        let line = book_line(&bucket, "acme").await?;
        let mark = line
            .split(' ')
            .find_map(|f| f.strip_prefix("deleting-ms="))
            .map(str::to_string);
        ensure!(
            line.contains("state=deleting") && mark.is_some(),
            "book after deleting: {line:?}"
        );
        set_state(&admin, "acme", "deleting").await?;
        let again = book_line(&bucket, "acme").await?;
        ensure!(again == line, "mark moved: {line:?} ⇒ {again:?}");

        // 2.
        for (what, r) in [
            ("invoke", acme.post("/invoke/Echo/hi", json!("x")).await?),
            ("read", acme.get(&format!("/invocations/{done}")).await?),
            ("tenant", acme.get("/tenant").await?),
        ] {
            ensure!(
                r.0 == StatusCode::FORBIDDEN && r.1["code"] == "tenant_deleting",
                "{what} while deleting: {} {}",
                r.0,
                r.1
            );
        }
        let (code, v) = other.post("/invoke/Echo/hi", json!("x")).await?;
        ensure!(code == StatusCode::OK, "other tenant: {code} {v}");
        let (code, v) = admin.get("/admin/tenants").await?;
        ensure!(
            code == StatusCode::OK && v["states"]["acme"] == "deleting",
            "{code} {v}"
        );
        let (code, v) = admin.new_key("acme", "admin").await?;
        ensure!(
            code == StatusCode::CONFLICT && v["code"] == "tenant_deleting",
            "create a key while deleting: {code} {v}"
        );

        // 3.
        set_state(&admin, "acme", "suspended").await?;
        let line = book_line(&bucket, "acme").await?;
        ensure!(
            line.contains("state=suspended") && !line.contains("deleting-ms="),
            "book after the reversal: {line:?}"
        );
        set_state(&admin, "acme", "active").await?;
        let (code, v) = acme.get(&format!("/invocations/{done}")).await?;
        ensure!(
            code == StatusCode::OK && v["status"] == "completed",
            "invocation after the reversal: {code} {v}"
        );
        let (code, v) = acme.get("/schedules/tick").await?;
        ensure!(code == StatusCode::OK, "schedule, reverted: {code} {v}");
        let (code, v) = acme.post("/invoke/Echo/hi", json!("x")).await?;
        ensure!(code == StatusCode::OK, "active again: {code} {v}");

        // 4.
        write_book(
            &bucket,
            &admin,
            "cairnflow-tenants/1\nacme state=deleting deleting-ms=1 purging-ms=2\n",
        )
        .await?;
        for state in ["suspended", "active", "limited"] {
            let (code, v) = admin.set_state("acme", state).await?;
            ensure!(
                code == StatusCode::CONFLICT && v["code"] == "tenant_purging",
                "{state} after purging-ms: {code} {v}"
            );
        }
        set_state(&admin, "acme", "deleting").await?;
        let line = book_line(&bucket, "acme").await?;
        ensure!(
            line.contains("deleting-ms=1") && line.contains("purging-ms=2"),
            "deleting again after purging-ms changed the mark: {line:?}"
        );

        // 5. A hand-written tombstone: the purge is "done" without deleting anything —
        // `verify` on teardown reports residue, as intended.
        bucket.allow(
            "tenant-residue",
            "a hand-written purged tombstone to test the state-change rules — no real purge loop",
        );
        write_book(
            &bucket,
            &admin,
            "cairnflow-tenants/1\nacme state=purged deleting-ms=1 purging-ms=2 purged-ms=3\n",
        )
        .await?;
        for state in ["deleting", "suspended", "active"] {
            let (code, v) = admin.set_state("acme", state).await?;
            ensure!(
                code == StatusCode::CONFLICT && v["code"] == "tenant_purged",
                "{state} while purged: {code} {v}"
            );
        }
        let (code, v) = admin.new_key("acme", "admin").await?;
        ensure!(
            code == StatusCode::CONFLICT && v["code"] == "tenant_purged",
            "create a key while purged: {code} {v}"
        );
        let (code, v) = acme.get("/tenant").await?;
        ensure!(
            code == StatusCode::FORBIDDEN && v["code"] == "tenant_deleting",
            "old key while purged: {code} {v}"
        );
        Ok(())
    })
    .await
}

/// **A synchronous ACK**: two nodes. `PUT …/state` on n1 returns `204` only once
/// n2 has read the book again — right after the `204`, new work of the tenant
/// sent to n2 is refused (no waiting for the 30 s poll beat).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn state_change_is_acked_by_every_node() -> Result<()> {
    let _serial = SERIAL.lock().await;
    let sdk = FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let n1 = authed(&bucket, "n1").await?;
    let n2 = authed(&bucket, "n2").await?;
    let (u1, u2) = (n1.node.url(), n2.node.url());
    within(TEST_DEADLINE, "ACK from every node", async {
        let admin = As::admin(&u1);
        let acme = As::tenant(&u1, "acme", "admin").await?;
        acme.register(&sdk.url).await?;
        // Both nodes have seen each other in the session table, and n2 knows the
        // deployment: ingress looks the service up before the tenant's state, so an n2
        // that has not read the catalog yet answers `404`, not the state's refusal.
        let t0 = std::time::Instant::now();
        loop {
            let (code, _) = acme.at(&u2).get("/tenant").await?;
            let (scode, services) = acme.at(&u2).get("/services").await?;
            let knows_echo = scode == StatusCode::OK
                && services
                    .as_array()
                    .is_some_and(|a| a.iter().any(|s| s["name"] == "Echo"));
            if code == StatusCode::OK && knows_echo {
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "n2 lacks the key or the deployment"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        for (state, want) in [
            ("suspended", StatusCode::FORBIDDEN),
            ("limited", StatusCode::TOO_MANY_REQUESTS),
        ] {
            set_state(&admin, "acme", state).await?;
            // No waiting: 204 means n2 has applied it.
            let (_, v) = acme.at(&u2).get("/tenant").await?;
            ensure!(v["state"] == state, "n2 after 204: {v}");
            // New work through n2 (forwarded to the range's leader if n2 is not it):
            // refused in every range, including those n2 leads.
            for _ in 0..8 {
                let (code, v) = acme
                    .at(&u2)
                    .send(
                        acme.http
                            .post(format!("{u2}/invoke/Echo/hi"))
                            .header("x-cairn-forward", "leader")
                            .body("\"x\""),
                    )
                    .await?;
                ensure!(code == want, "n2 after 204 ({state}): {code} {v}");
            }
        }
        Ok(())
    })
    .await
}

/// **No "empty book ⇒ every tenant active" window** (fail safe): a new node that
/// cannot read the `tenants` book yet does not start (takes no lease, does not
/// answer `/health`) — even when the book is broken by an unknown value of a
/// limit. A readable book ⇒ it starts, and the states are in force from the
/// first request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_does_not_start_before_reading_tenants() -> Result<()> {
    let bucket = common::bucket();
    bucket
        .put(
            &Path::from("tenants"),
            PutPayload::from("cairnflow-tenants/1\nacme max-payload=1kb\n".to_string()),
        )
        .await?;
    let started = tokio::time::timeout(Duration::from_secs(60), authed(&bucket, "n1")).await;
    match started {
        Ok(Ok(n)) => {
            n.node.shutdown().await;
            anyhow::bail!("node started before reading the tenants book");
        }
        Ok(Err(e)) => ensure!(e.to_string().contains("tenants"), "{e}"),
        Err(_) => {}
    }
    Ok(())
}
