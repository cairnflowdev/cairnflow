// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The data plane's audit log.
//!
//! | Kind | How it is written | Record |
//! |---|---|---|
//! | in-range action (cancel, purge, unstick, restart, migrate, signal, schedule, bulk) | a `Table::Audit` row **in the same commit** as the action; the range's export loop writes an object with a deterministic name | `r-<row key hex>.json` |
//! | object action (deployment, key, `tenants`, rule, snapshot, quarantine) | intent **before**, outcome **after** | `….intent`, `….done` |
//! | permission denial | grouped by `(tenant, actor, action)` per tick | `….denied` (with `count`) |
//! | in-range action that changes nothing (`not_found`, `noop`) | grouped by `(tenant, actor, action, outcome)` per tick, no row in the range | `….noop` (with `count`) |
//!
//! Reading: `GET /audit` (the key's tenant; `?tenant=` for provision/cluster-admin),
//! the `sys_audit` table.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
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
    std::time::Duration,
};

const ADMIN_KEY: &str = "cf_test_audit_bootstrap_key_0123456789ab";
const TTL: Duration = Duration::from_secs(2);

async fn authed(bucket: &common::Bucket, id: &str) -> Result<ClusterNode> {
    common::start_cluster_node_tweaked(bucket, id, 2, TTL, |cfg| {
        cfg.auth = AuthConfig {
            required: true,
            admin_key: Some(ADMIN_KEY.into()),
            cluster_secret: None,
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
        cfg.audit_denials_every = Duration::from_millis(200);
    })
    .await
}

#[derive(Clone)]
struct As {
    url: String,
    key: String,
    id: String,
    http: Client,
}

impl As {
    fn admin(url: &str) -> Self {
        Self {
            url: url.into(),
            key: ADMIN_KEY.into(),
            id: "bootstrap:cluster-admin".into(),
            http: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
        }
    }

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
                    id: v["id"].as_str().unwrap_or_default().to_string(),
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

    async fn send(&self, req: reqwest::RequestBuilder) -> Result<(StatusCode, Value)> {
        let r = req.bearer_auth(&self.key).send().await?;
        let code = r.status();
        let text = r.text().await?;
        Ok((
            code,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        ))
    }

    async fn post(&self, path: &str, body: Value) -> Result<(StatusCode, Value)> {
        self.send(self.http.post(format!("{}{path}", self.url)).json(&body))
            .await
    }

    async fn put(&self, path: &str, body: Value) -> Result<(StatusCode, Value)> {
        self.send(self.http.put(format!("{}{path}", self.url)).json(&body))
            .await
    }

    async fn delete(&self, path: &str) -> Result<(StatusCode, Value)> {
        self.send(self.http.delete(format!("{}{path}", self.url)))
            .await
    }

    async fn get(&self, path: &str) -> Result<(StatusCode, Value)> {
        self.send(self.http.get(format!("{}{path}", self.url)))
            .await
    }

    /// This key's tenant's audit records.
    async fn audit(&self, query: &str) -> Result<Vec<Value>> {
        let (code, v) = self.get(&format!("/audit{query}")).await?;
        ensure!(code == StatusCode::OK, "GET /audit: {code} {v}");
        Ok(v["records"].as_array().cloned().unwrap_or_default())
    }

    /// Wait until `pred` holds on the audit records (the export loop runs per tick).
    async fn until(&self, what: &str, pred: impl Fn(&[Value]) -> bool) -> Result<Vec<Value>> {
        let t0 = std::time::Instant::now();
        loop {
            let r = self.audit("").await?;
            if pred(&r) {
                return Ok(r);
            }
            ensure!(
                t0.elapsed() < TEST_DEADLINE / 2,
                "audit has no {what} yet: {r:#?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

fn has(r: &[Value], kind: &str, action: &str, outcome: &str) -> bool {
    r.iter()
        .any(|x| x["kind"] == kind && x["action"] == action && x["outcome"] == outcome)
}

fn id_of(v: &Value) -> Result<String> {
    v["id"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("no id: {v}"))
}

async fn settled(who: &As, id: &str, status: &str) -> Result<()> {
    let t0 = std::time::Instant::now();
    loop {
        let (code, v) = who.get(&format!("/invocations/{id}")).await?;
        if code == StatusCode::OK && v["status"] == status {
            return Ok(());
        }
        ensure!(
            t0.elapsed() < TEST_DEADLINE,
            "{id} not {status} yet: {code} {v}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// `(method, path)` of every `.route(` in `router()` — the same reader as
/// `tests/tenant_isolation.rs`.
fn routes() -> Vec<(String, String)> {
    let src = include_str!("../src/ingress/mod.rs");
    let start = src.find("pub fn router(state: AppState)").expect("router");
    let end = src.find("pub fn internal_router").expect("internal_router");
    let mut out = Vec::new();
    for part in src[start..end].split(".route(").skip(1) {
        let Some(path) = part
            .trim_start()
            .strip_prefix('"')
            .and_then(|p| p.split('"').next())
        else {
            continue;
        };
        let call: String = part
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        for m in ["get", "post", "put", "delete", "patch"] {
            let needle = format!("{m}(");
            let found = call.match_indices(&needle).any(|(i, _)| {
                i == 0 || matches!(call.as_bytes()[i - 1], b'(' | b'.' | b':' | b' ' | b'\n')
            });
            if found {
                out.push((m.to_ascii_uppercase(), path.to_string()));
            }
        }
    }
    out
}

/// **Every write is classified**: each non-`GET` route of `router()` has a line
/// in `cairn_server::audit::CLASSES` — audited in the range, on an object, or
/// exempt **with a reason** — and vice versa. A new write route does not slip
/// out of the audit unnoticed.
#[test]
fn every_mutation_is_audited() {
    let routes = routes();
    assert!(routes.len() > 100, "route reader broken: {routes:?}");
    for (m, p) in routes.iter().filter(|(m, _)| m != "GET") {
        assert!(
            cairn_server::audit::class_of(m, p).is_some(),
            "{m} {p} has no audit class in cairn_server::audit::CLASSES"
        );
        assert_eq!(
            cairn_server::audit::match_route(m, &p.replace(['{', '}', '*'], "x")).map(|(r, _)| r),
            Some(p.as_str()),
            "{m} {p}: route matcher of the audit class"
        );
    }
    for (m, p, _) in cairn_server::audit::CLASSES {
        assert!(
            routes.iter().any(|(mm, pp)| mm == m && pp == p),
            "CLASSES still has {m} {p}, which ingress does not"
        );
    }
}

/// **In-range actions are recorded in the same commit and exported as objects**:
/// cancel, restart, purge, signal, migrate (changes nothing ⇒ `noop`), schedules
/// (`put`/`pause`/`resume`/`trigger`/`delete`) and a `cancel` batch. Each action
/// is one `row` record carrying the actor (key id), the target and the outcome;
/// the object is named `r-<key hex>.json` under `audit/tenant/acme/<date>/`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_range_actions_are_audited_with_the_action() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service(
            "Nap",
            "once",
            vec![Step::Sleep(3_600_000), Step::output("woke")],
        )
        .service("Echo", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "in-range audit", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        let (code, v) = acme.post("/deployments", json!({ "uri": sdk.url })).await?;
        ensure!(code == StatusCode::OK, "{code} {v}");
        let (_, v) = acme.post("/invoke/Nap/once/send", json!("x")).await?;
        let nap = id_of(&v)?;
        let (_, v) = acme.post("/invoke/Nap/once/send", json!("x")).await?;
        let nap2 = id_of(&v)?;
        let (_, v) = acme.post("/invoke/Echo/hi", json!("x")).await?;
        let echo = id_of(&v)?;
        settled(&acme, &nap, "suspended").await?;
        settled(&acme, &nap2, "suspended").await?;

        let (code, v) = acme
            .post(
                &format!("/invocations/{nap}/signal"),
                json!({ "signal": "go", "value": 1 }),
            )
            .await?;
        ensure!(code.is_success(), "signal: {code} {v}");
        let (code, v) = acme
            .post(&format!("/invocations/{nap}/cancel"), json!(null))
            .await?;
        ensure!(code.is_success(), "cancel: {code} {v}");
        let (code, v) = acme
            .post(&format!("/invocations/{echo}/restart"), json!(null))
            .await?;
        ensure!(code.is_success(), "restart: {code} {v}");
        let (code, v) = acme
            .post(&format!("/invocations/{echo}/purge"), json!(null))
            .await?;
        ensure!(code.is_success(), "purge: {code} {v}");
        let (code, v) = acme
            .post("/invocations/batch/cancel", json!({ "ids": [nap2] }))
            .await?;
        ensure!(code.is_success(), "batch: {code} {v}");
        let sched = json!({ "everyMs": 60_000, "target": { "service": "Echo", "handler": "hi" } });
        for (m, path) in [
            ("PUT", "/schedules/s1"),
            ("POST", "/schedules/s1/pause"),
            ("POST", "/schedules/s1/resume"),
            ("POST", "/schedules/s1/trigger"),
            ("DELETE", "/schedules/s1"),
        ] {
            let (code, v) = match m {
                "PUT" => acme.put(path, sched.clone()).await?,
                "DELETE" => acme.delete(path).await?,
                _ => acme.post(path, json!(null)).await?,
            };
            ensure!(code.is_success(), "{m} {path}: {code} {v}");
        }

        let want = [
            "invocation.signal",
            "invocation.cancel",
            "invocation.restart",
            "invocation.purge",
            "schedule.put",
            "schedule.pause",
            "schedule.resume",
            "schedule.trigger",
            "schedule.delete",
        ];
        let r = acme
            .until("every in-range action", |r| {
                want.iter().all(|a| has(r, "row", a, "ok"))
                    && r.iter()
                        .filter(|x| x["kind"] == "row" && x["action"] == "invocation.cancel")
                        .count()
                        == 2
            })
            .await?;
        for x in r.iter().filter(|x| x["kind"] == "row") {
            ensure!(
                x["actor"]["id"] == acme.id.as_str()
                    && x["actor"]["type"] == "key"
                    && x["tenant"] == "acme"
                    && x["requestId"].as_str().is_some_and(|s| !s.is_empty())
                    && x["name"].as_str().is_some_and(|n| n.starts_with("r-")),
                "{x}"
            );
        }
        ensure!(
            r.iter()
                .any(|x| x["action"] == "invocation.cancel"
                    && x["target"]["invocation"] == nap.as_str()),
            "{r:#?}"
        );
        // **One** object per action.
        let rows = r.iter().filter(|x| x["kind"] == "row").count();
        ensure!(rows == want.len() + 1, "{rows} in-range records: {r:#?}");
        Ok(())
    })
    .await
}

/// **Object actions: intent before, outcome after**: register/remove a
/// deployment, create/revoke a key, change tenant settings and state, rules. Each
/// action is a pair `intent` (`pending`) + `done` with the same name; a failed
/// command still has its pair, outcome `error` with the HTTP code.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn object_actions_write_intent_then_outcome() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "object audit", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme", "admin").await?;
        let (code, v) = acme.post("/deployments", json!({ "uri": sdk.url })).await?;
        ensure!(code == StatusCode::OK, "{code} {v}");
        let first = id_of(&v)?;
        let (code, _) = acme.post("/deployments", json!({ "uri": sdk.url })).await?;
        ensure!(code == StatusCode::OK);
        let (code, v) = acme.delete(&format!("/deployments/{first}")).await?;
        ensure!(code.is_success(), "remove: {code} {v}");
        let (code, _) = acme
            .post(
                "/deployments",
                json!({ "uri": "http://127.0.0.1:1/missing" }),
            )
            .await?;
        ensure!(code.is_client_error() || code.is_server_error());
        let (code, v) = admin
            .post("/admin/keys", json!({ "tenant": "acme", "role": "invoke" }))
            .await?;
        ensure!(code == StatusCode::OK, "{code} {v}");
        let key = id_of(&v)?;
        let (code, v) = admin.delete(&format!("/admin/keys/{key}")).await?;
        ensure!(code.is_success(), "revoke: {code} {v}");
        let (code, v) = admin
            .put("/admin/tenants/acme", json!({ "slowMs": 100 }))
            .await?;
        ensure!(code == StatusCode::OK, "{code} {v}");
        let (code, v) = admin
            .put("/admin/tenants/acme/state", json!({ "state": "limited" }))
            .await?;
        ensure!(code == StatusCode::NO_CONTENT, "{code} {v}");

        let r = acme
            .until("intent/outcome pairs", |r| {
                [
                    ("deployment.register", "ok"),
                    ("deployment.remove", "ok"),
                    ("deployment.register", "error"),
                    ("key.create", "ok"),
                    ("key.revoke", "ok"),
                    ("tenant.settings", "ok"),
                    ("tenant.state", "ok"),
                ]
                .iter()
                .all(|(a, o)| has(r, "done", a, o))
            })
            .await?;
        // Every outcome has an intent with the same name.
        for d in r.iter().filter(|x| x["kind"] == "done") {
            let stem = d["name"]
                .as_str()
                .unwrap_or_default()
                .trim_end_matches(".done");
            ensure!(
                r.iter().any(|i| i["kind"] == "intent"
                    && i["outcome"] == "pending"
                    && i["name"] == format!("{stem}.intent").as_str()),
                "outcome without an intent: {d}"
            );
            ensure!(d["status"].as_u64().is_some(), "{d}");
        }
        ensure!(
            r.iter().any(|x| x["kind"] == "done"
                && x["action"] == "key.create"
                && x["actor"]["id"] == "bootstrap:cluster-admin"
                && x["target"]["role"] == "invoke"),
            "{r:#?}"
        );
        Ok(())
    })
    .await
}

/// **Permission denials are grouped**: 20 writes refused with `403` for a `view`
/// key become **one** `denied` record per grouping tick, with `count` adding up to
/// exactly 20 — not one object per request. On a busy machine the 20 commands can
/// straddle a flush (seen in a full check run: one record with `count = 6` before
/// the last command arrived), so the test waits for the total, not the first record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn denied_requests_are_aggregated() -> Result<()> {
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "grouped denials", async {
        let viewer = As::tenant(&url, "acme", "view").await?;
        let acme = As::tenant(&url, "acme", "admin").await?;
        for _ in 0..20 {
            let (code, _) = viewer
                .post("/deployments", json!({ "uri": "http://x" }))
                .await?;
            ensure!(code == StatusCode::FORBIDDEN);
        }
        let total = |r: &[Value]| -> u64 {
            r.iter()
                .filter(|x| x["kind"] == "denied")
                .map(|x| x["count"].as_u64().unwrap_or(0))
                .sum()
        };
        let r = acme.until("denied record", |r| total(r) >= 20).await?;
        let denied: Vec<&Value> = r.iter().filter(|x| x["kind"] == "denied").collect();
        ensure!(
            total(&r) == 20
                && denied.len() < 20
                && denied.iter().all(|d| d["actor"]["id"] == viewer.id.as_str()
                    && d["action"] == "POST /deployments"),
            "{denied:#?}"
        );
        Ok(())
    })
    .await
}

/// **No-op outcomes are grouped**: 20 `cancel` commands on an invocation that has
/// already finished (outcome `noop`: nothing changes) do not become 20 rows in the
/// range — each row is an object, one Class A PUT. Must see: no `row` record of
/// `invocation.cancel`; `noop` records carry `count`, totalling exactly 20, a few
/// records (one per grouping tick) rather than 20, with the right actor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noop_outcomes_are_aggregated() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "grouped no-ops", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        let (code, v) = acme.post("/deployments", json!({ "uri": sdk.url })).await?;
        ensure!(code == StatusCode::OK, "{code} {v}");
        let (_, v) = acme.post("/invoke/Echo/hi", json!("x")).await?;
        let echo = id_of(&v)?;
        settled(&acme, &echo, "completed").await?;
        for _ in 0..20 {
            let (code, v) = acme
                .post(&format!("/invocations/{echo}/cancel"), json!(null))
                .await?;
            ensure!(
                code.is_success() && v["cancelled"] == false,
                "cancel: {code} {v}"
            );
        }
        let noop = |r: &[Value]| -> u64 {
            r.iter()
                .filter(|x| x["kind"] == "noop" && x["action"] == "invocation.cancel")
                .map(|x| x["count"].as_u64().unwrap_or(0))
                .sum()
        };
        let r = acme.until("noop record", |r| noop(r) >= 20).await?;
        let rows: Vec<&Value> = r
            .iter()
            .filter(|x| x["kind"] == "row" && x["action"] == "invocation.cancel")
            .collect();
        ensure!(
            rows.is_empty(),
            "{} in-range no-op rows: {rows:#?}",
            rows.len()
        );
        let records: Vec<&Value> = r.iter().filter(|x| x["kind"] == "noop").collect();
        ensure!(
            noop(&r) == 20
                && records.len() <= 6
                && records
                    .iter()
                    .all(|x| x["outcome"] == "noop" && x["actor"]["id"] == acme.id.as_str()),
            "{records:#?}"
        );
        Ok(())
    })
    .await
}

/// **The target tenant is trusted only when the caller may pick a tenant** (found
/// in a security review): bravo's `admin` key sends `Object`-class writes with
/// `?tenant=acme` (`DELETE /deployments/{id}`, `POST /deployments`,
/// `PUT /services/{s}/revision`). The permission gate lets them through
/// (`Operate`), the handler answers `403` because a tenant key cannot pick a
/// tenant — but the intent/outcome were recorded before the handler. Must see:
/// the `intent`/`done` pair (`error`, `403`) is in **bravo's** audit, acme's audit
/// has no record from bravo's key — another tenant cannot write into acme's audit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreign_tenant_attempts_stay_in_the_callers_audit() -> Result<()> {
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "another tenant's attempt", async {
        let acme = As::tenant(&url, "acme", "admin").await?;
        let bravo = As::tenant(&url, "bravo", "admin").await?;
        let (code, _) = bravo.delete("/deployments/dp_x?tenant=acme").await?;
        ensure!(code == StatusCode::FORBIDDEN, "DELETE: {code}");
        let (code, _) = bravo
            .post("/deployments?tenant=acme", json!({ "uri": "http://x" }))
            .await?;
        ensure!(code == StatusCode::FORBIDDEN, "POST: {code}");
        let (code, _) = bravo
            .put(
                "/services/Echo/revision?tenant=acme",
                json!({ "deployment": "dp_x" }),
            )
            .await?;
        ensure!(code == StatusCode::FORBIDDEN, "PUT revision: {code}");
        let mine = bravo
            .until("three attempts in bravo's audit", |r| {
                [
                    "deployment.remove",
                    "deployment.register",
                    "service.revision",
                ]
                .iter()
                .all(|a| {
                    r.iter().any(|x| {
                        x["kind"] == "done"
                            && x["action"] == *a
                            && x["outcome"] == "error"
                            && x["status"] == 403
                    })
                })
            })
            .await?;
        ensure!(mine.iter().all(|x| x["tenant"] == "bravo"), "{mine:#?}");
        let theirs = acme.audit("").await?;
        ensure!(
            theirs.iter().all(|x| x["actor"]["id"] != bravo.id.as_str()),
            "bravo's key wrote into acme's audit: {theirs:#?}"
        );
        Ok(())
    })
    .await
}

/// **Reading audit per tenant**: bravo's key does not see acme's records and
/// cannot pick `?tenant=acme` (`403`); cluster-admin and a `provision` key can;
/// `sys_audit` returns the same records through `/query`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audit_reads_are_tenant_scoped() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "hi", vec![Step::output("ok")])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = authed(&bucket, "n1").await?;
    let url = node.node.url();
    within(TEST_DEADLINE, "read audit per tenant", async {
        let admin = As::admin(&url);
        let acme = As::tenant(&url, "acme", "admin").await?;
        let bravo = As::tenant(&url, "bravo", "admin").await?;
        let provision = As::tenant(&url, "default", "provision").await?;
        acme.post("/deployments", json!({ "uri": sdk.url })).await?;
        acme.until("acme's deployment", |r| has(r, "done", "deployment.register", "ok"))
            .await?;
        let mine = bravo.audit("").await?;
        ensure!(
            mine.iter().all(|x| x["tenant"] == "bravo"
                && x["action"] != "deployment.register"),
            "bravo sees acme's audit: {mine:#?}"
        );
        let (code, _) = bravo.get("/audit?tenant=acme").await?;
        ensure!(code == StatusCode::FORBIDDEN, "bravo picks a tenant: {code}");
        for who in [&admin, &provision] {
            let (code, v) = who.get("/audit?tenant=acme").await?;
            ensure!(
                code == StatusCode::OK
                    && v["records"]
                        .as_array()
                        .is_some_and(|r| has(r, "done", "deployment.register", "ok")),
                "{code} {v}"
            );
        }
        let (code, v) = acme
            .post(
                "/query",
                json!({ "sql": "SELECT action, outcome FROM sys_audit WHERE kind = 'done'" }),
            )
            .await?;
        ensure!(
            code == StatusCode::OK && v["rows"].to_string().contains("deployment.register"),
            "{code} {v}"
        );
        let (_, v) = bravo
            .post(
                "/query",
                json!({ "sql": "SELECT count(*) FROM sys_audit WHERE tenant <> 'bravo' OR action = 'deployment.register'" }),
            )
            .await?;
        ensure!(v["rows"] == json!([[0]]), "bravo's sys_audit: {v}");
        Ok(())
    })
    .await
}
