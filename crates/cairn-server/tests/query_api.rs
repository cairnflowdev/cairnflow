// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `POST /query`: roles, limits, errors.
//!
//! One node, auth on, two tenants (`acme`, `zeta`) with their own deployments and
//! invocations.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Context, Result},
    common::fake_sdk::{FakeSdk, Step},
    reqwest::Client,
    serde_json::{json, Value},
    std::time::Duration,
};

const ADMIN: &str = "cf_test_query_api_cluster_admin_key_0001";

async fn call(http: &Client, key: &str, req: reqwest::RequestBuilder) -> Result<(u16, Value)> {
    let r = req.bearer_auth(key).send().await?;
    let code = r.status().as_u16();
    let text = r.text().await?;
    let _ = http;
    Ok((
        code,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    ))
}

struct Fx {
    node: cairn_server::node::RunningNode,
    sdk: FakeSdk,
    url: String,
    http: Client,
    /// `(tenant, admin key, invoke key)`.
    keys: Vec<(String, String, String)>,
}

impl Fx {
    async fn get(&self, key: &str, path: &str) -> Result<(u16, Value)> {
        call(
            &self.http,
            key,
            self.http.get(format!("{}{path}", self.url)),
        )
        .await
    }

    async fn query(&self, key: &str, sql: &str) -> Result<(u16, Value)> {
        call(
            &self.http,
            key,
            self.http
                .post(format!("{}/query", self.url))
                .json(&json!({ "sql": sql })),
        )
        .await
    }

    /// Asks until there are `n` rows (the background index build and the `Remote` level may trail
    /// the commit by one tick).
    async fn query_until(&self, key: &str, sql: &str, n: usize) -> Result<Value> {
        let t0 = std::time::Instant::now();
        loop {
            let (code, v) = self.query(key, sql).await?;
            if code == 200 && v["rows"].as_array().map(Vec::len) == Some(n) && v["partial"] == false
            {
                return Ok(v);
            }
            if t0.elapsed() > Duration::from_secs(20) {
                return Err(anyhow!("{sql}: {code} {v}"));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

async fn fixture(tweak: impl FnOnce(&mut cairn_server::node::NodeConfig)) -> Result<Fx> {
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("ok")])
        // Waits until cancelled — the invocation stays bound to its deployment (UI questions 5, 8).
        .service("Hang", "run", vec![Step::Suspend])
        .spawn()
        .await?;
    let mut cfg = common::solo_config(2);
    // The asker's queries run in a child process like the default binary:
    // the whole `query_api` suite goes that way.
    cfg.query_worker = Some(common::query_worker());
    cfg.auth = cairn_server::auth::AuthConfig {
        required: true,
        admin_key: Some(ADMIN.into()),
        cluster_secret: None,
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    tweak(&mut cfg);
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let http = Client::new();
    let mut keys = Vec::new();
    for (tenant, n) in [("acme", 3), ("zeta", 2)] {
        let mut k = Vec::new();
        for role in ["admin", "invoke"] {
            let (code, v) = call(
                &http,
                ADMIN,
                http.post(format!("{url}/admin/keys"))
                    .json(&json!({ "tenant": tenant, "role": role })),
            )
            .await?;
            ensure!(code == 200, "key {tenant}/{role}: {code} {v}");
            k.push(v["secret"].as_str().unwrap_or_default().to_string());
        }
        let (code, v) = call(
            &http,
            &k[0],
            http.post(format!("{url}/deployments"))
                .json(&json!({ "uri": sdk.url })),
        )
        .await?;
        ensure!(code < 300, "deployment {tenant}: {code} {v}");
        for _ in 0..n {
            let (code, v) = call(
                &http,
                &k[1],
                http.post(format!("{url}/invoke/Echo/run")).body("{}"),
            )
            .await?;
            ensure!(code == 200, "invoke {tenant}: {code} {v}");
        }
        keys.push((tenant.to_string(), k[0].clone(), k[1].clone()));
    }
    Ok(Fx {
        node,
        sdk,
        url,
        http,
        keys,
    })
}

/// **Roles**: `acme`'s `admin` key sees only `acme`;
/// `cluster-admin` sees both; an `invoke` key cannot query.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_roles() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let r = common::within(Duration::from_secs(60), "/query roles", async {
        let sql =
            "SELECT tenant, count(*) AS n FROM sys_invocation GROUP BY tenant ORDER BY tenant";
        let v = fx.query_until(ADMIN, sql, 2).await?;
        ensure!(v["rows"] == json!([["acme", 3], ["zeta", 2]]), "{v}");
        let (_, acme_admin, acme_invoke) = &fx.keys[0];
        let v = fx.query_until(acme_admin, sql, 1).await?;
        ensure!(v["rows"] == json!([["acme", 3]]), "{v}");
        let v = fx
            .query_until(
                acme_admin,
                "SELECT tenant, sum(count) FROM sys_invocation_counts GROUP BY tenant",
                1,
            )
            .await?;
        ensure!(v["rows"] == json!([["acme", 3]]), "{v}");
        let (code, v) = fx.query(acme_invoke, sql).await?;
        ensure!(code == 403, "invoke key: {code} {v}");
        ensure!(v["columns"][0]["name"].is_null());
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// **The read limit**: a query reading past the limit ⇒ `422 query_budget`, not
/// `500`; a small query still runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_budget_is_enforced() -> Result<()> {
    // Five invocations, two ranges: a full scan reads 5 units; `LIMIT 1` reads at most one
    // row per stream = 2 ranges × (root, child) = 4. A limit of 4 separates the two queries exactly.
    let fx = fixture(|cfg| cfg.query_limits.read_budget = 4).await?;
    let r = common::within(Duration::from_secs(60), "/query limit", async {
        // Wait for the build to finish: the counters cost no read units.
        fx.query_until(ADMIN, "SELECT * FROM sys_invocation_counts", 2)
            .await?;
        let (code, v) = fx.query(ADMIN, "SELECT id FROM sys_invocation").await?;
        ensure!(code == 422, "{code} {v}");
        ensure!(
            v["error"]
                .as_str()
                .is_some_and(|e| e.contains("query_budget")),
            "{v}"
        );
        let (code, v) = fx
            .query(
                ADMIN,
                "SELECT id FROM sys_invocation ORDER BY created_at DESC, id LIMIT 1",
            )
            .await?;
        ensure!(code == 200, "{code} {v}");
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// **A token bucket per tenant**: once `acme` runs out of read units it gets `429`, while
/// `zeta` can still query — the buckets are per tenant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_tenant_bucket_throttles() -> Result<()> {
    let fx = fixture(|cfg| {
        cfg.query_tenant_budget = cairn_server::query::http::TenantBudget {
            capacity: 2,
            per_second: 0,
        }
    })
    .await?;
    let r = common::within(Duration::from_secs(60), "bucket /query", async {
        let (_, acme, _) = &fx.keys[0];
        let (_, zeta, _) = &fx.keys[1];
        let sql = "SELECT id FROM sys_invocation";
        // Wait for the build with the counters: 0 read units, no bucket charged.
        fx.query_until(acme, "SELECT * FROM sys_invocation_counts", 1)
            .await?;
        // The first query is accepted (bucket positive) then overdraws ⇒ the next one is `429`.
        let (code, v) = fx.query(acme, sql).await?;
        ensure!(code == 200, "{code} {v}");
        let (code, v) = fx.query(acme, sql).await?;
        ensure!(code == 429, "{code} {v}");
        ensure!(
            v["error"]
                .as_str()
                .is_some_and(|e| e.contains("query_budget")),
            "{v}"
        );
        let (code, v) = fx.query(zeta, sql).await?;
        ensure!(code == 200, "zeta blocked because of acme: {code} {v}");
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// **A run-time error does not become a truncated `200`** (the first batch is looked at
/// before the status is sent): division by zero, bad SQL, a missing table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_error_after_first_batch() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let r = common::within(Duration::from_secs(60), "/query errors", async {
        for (sql, want) in [
            ("SELECT 1 / (count(*) - count(*)) FROM sys_invocation", None),
            ("SELEC 1", Some(400)),
            ("SELECT * FROM sys_nonexistent", Some(400)),
            ("CREATE TABLE t AS SELECT 1", Some(400)),
        ] {
            let (code, v) = fx.query(ADMIN, sql).await?;
            ensure!(code != 200 && v["error"].is_string(), "{sql}: {code} {v}");
            if let Some(w) = want {
                ensure!(code == w, "{sql}: {code} {v}");
            }
        }
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// **A view table's declared columns match the REST JSON**: each key of
/// a read column must appear in at least one row of the endpoint — renaming a REST field
/// and forgetting the view table silently turns the column into `NULL`; this test catches it. Keys present only
/// when non-empty (`skip_serializing_if`) are in `OPTIONAL`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn views_match_rest() -> Result<()> {
    const OPTIONAL: &[&str] = &[
        "at",
        "error",
        "deployment",
        "scope",
        "limitKey",
        "objectKey",
        "blockedOn",
        "lockHolder",
        "limit",
        "available",
        "rule",
        "intent",
        "concurrency",
        "rate",
        "burst",
        "quarantine",
        "node",
        "nextAt",
        "ty",
        // Fields absent while they have no value (`skip_serializing_if`).
        "sizeBytes",
        "slowMs",
        "quarantineMode",
        "quarantineRule",
        "slow",
        "p99PutMs",
    ];
    let fx = fixture(|_| {}).await?;
    let r = common::within(Duration::from_secs(60), "views match REST", async {
        // A rule and a schedule so that `sys_rule`, `sys_schedule` have rows to compare
        // (the schedule belongs to the key's tenant: cluster-admin is the default tenant).
        let (code, v) = call(
            &fx.http,
            ADMIN,
            fx.http
                .post(format!("{}/deployments", fx.url))
                .json(&json!({ "uri": fx.sdk.url })),
        )
        .await?;
        ensure!(code < 300, "deployment: {code} {v}");
        let (code, v) = call(
            &fx.http,
            ADMIN,
            fx.http
                .post(format!("{}/rules", fx.url))
                .json(&json!({ "pattern": "acme", "concurrency": 5 })),
        )
        .await?;
        ensure!(code < 300, "rule: {code} {v}");
        let (code, v) = call(
            &fx.http,
            ADMIN,
            fx.http.put(format!("{}/schedules/nightly", fx.url)).json(
                &json!({ "everyMs": 60_000, "target": { "service": "Echo", "handler": "run" } }),
            ),
        )
        .await?;
        ensure!(code == 200, "schedule: {code} {v}");
        // The per-handler summary has rows once the stats window has recorded the turns.
        let t0 = std::time::Instant::now();
        loop {
            let (_, v) = call(
                &fx.http,
                ADMIN,
                fx.http
                    .get(format!("{}/stats/handlers?scope=cluster", fx.url)),
            )
            .await?;
            if v["handlers"].as_array().is_some_and(|a| !a.is_empty()) {
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "no summary rows yet: {v}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // Tenant settings, range 0's flush window (before the split) and an archive
        // of the default tenant (an invocation completed then purged) for `sys_tenant`,
        // `sys_range_window`, `sys_archive`.
        let (code, v) = call(
            &fx.http,
            ADMIN,
            fx.http
                .put(format!("{}/admin/tenants/acme", fx.url))
                .json(&json!({ "hashChain": true, "slowMs": 1234 })),
        )
        .await?;
        ensure!(code < 300, "tenant settings: {code} {v}");
        let (code, v) = call(
            &fx.http,
            ADMIN,
            fx.http
                .put(format!("{}/ranges/0/window", fx.url))
                .json(&json!({ "ms": 50 })),
        )
        .await?;
        ensure!(code < 300, "window: {code} {v}");
        let (code, v) = call(
            &fx.http,
            ADMIN,
            fx.http
                .post(format!("{}/invoke/Echo/run", fx.url))
                .body("{}"),
        )
        .await?;
        ensure!(code == 200, "invoke: {code} {v}");
        let done = v["id"].as_str().unwrap_or_default().to_string();
        let (code, v) = call(
            &fx.http,
            ADMIN,
            fx.http.post(format!("{}/invocations/{done}/purge", fx.url)),
        )
        .await?;
        ensure!(code < 300, "purge: {code} {v}");
        let today = cairn_store::archive::utc_date(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as u64,
        );
        // A capture request and a split so that `sys_capture`, `sys_range_op` have rows.
        let (code, v) = call(
            &fx.http,
            ADMIN,
            fx.http
                .post(format!("{}/admin/capture", fx.url))
                .json(&json!({ "service": "Echo", "handler": "run" })),
        )
        .await?;
        ensure!(code < 300, "capture: {code} {v}");
        let (code, v) = call(
            &fx.http,
            ADMIN,
            fx.http.post(format!("{}/ranges/0/split", fx.url)),
        )
        .await?;
        ensure!(code == 200, "split: {code} {v}");
        let mut seen = Vec::new();
        for spec in cairn_query::view::VIEWS {
            // Path parameters: sample values, and the same filter in the SQL query.
            let mut path = spec.path.to_string();
            let mut filter = String::new();
            for p in spec.params {
                let val = match *p {
                    "date" => today.clone(),
                    other => anyhow::bail!("{}: no sample value for {other} yet", spec.name),
                };
                path = path.replace(&format!("{{{p}}}"), &val);
                filter.push_str(if filter.is_empty() {
                    " WHERE "
                } else {
                    " AND "
                });
                filter.push_str(&format!("{p} = '{val}'"));
            }
            let (code, doc) =
                call(&fx.http, ADMIN, fx.http.get(format!("{}{}", fx.url, path))).await?;
            ensure!(code == 200, "{}: {code} {doc}", path);
            let parents: Vec<&Value> = match doc.pointer(spec.pointer) {
                Some(Value::Array(a)) => a.iter().collect(),
                Some(o) => vec![o],
                None => Vec::new(),
            };
            let mut items = parents.clone();
            if let Some(f) = spec.each {
                items = items
                    .iter()
                    .flat_map(|p| p[f].as_array().into_iter().flatten())
                    .collect();
            }
            if items.is_empty() {
                continue;
            }
            seen.push(spec.name);
            for c in spec.columns {
                let key = c.key.trim_start_matches('^');
                let present = if c.key.starts_with('^') {
                    parents.iter().any(|p| p.get(key).is_some())
                } else if key.starts_with('/') {
                    items.iter().any(|i| i.pointer(key).is_some())
                } else {
                    items.iter().any(|i| i.get(key).is_some())
                };
                ensure!(
                    present || OPTIONAL.contains(&key),
                    "{}.{}: key {key:?} not in GET {}: {doc}",
                    spec.name,
                    c.name,
                    spec.path
                );
            }
            // And readable through SQL, with the right row count.
            let (code, v) = fx
                .query(
                    ADMIN,
                    &format!("SELECT count(*) FROM {}{filter}", spec.name),
                )
                .await?;
            ensure!(
                code == 200 && v["rows"] == json!([[items.len()]]),
                "{}: {code} {v}",
                spec.name
            );
        }
        // Tables with no rows in this fixture are not compared — keep that list
        // short and deliberate (the concurrency limit needs a rule in effect on the range).
        let unseen: Vec<_> = cairn_query::view::VIEWS
            .iter()
            .map(|s| s.name)
            .filter(|n| !seen.contains(n))
            .collect();
        ensure!(
            unseen.iter().all(|n| [
                "sys_limit",
                "sys_vqueue",
                // Need real load: quarantine after a series of failed turns, hints after a hot range.
                "sys_quarantine",
                "sys_range_suggestion",
                // Default threshold 5 s: `slow_runs_are_queryable` lowers it.
                "sys_slow_run",
                // Need a background loop / the bucket: the scrubber (a clean round has no rows), the
                // trash, the shared WAL (this node uses the Db's WAL), the price table (only
                // `cairn-ctl pricing` writes it) — `ops_tables_*` checks the roles and shape.
                "sys_scrub",
                "sys_trash",
                "sys_nodewal",
                "sys_pricing",
                // Flushed once every 60 s: `handler_history_is_queryable` lowers the cadence.
                "sys_handler_history",
            ]
            .contains(n)),
            "not compared: {unseen:?}"
        );
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// **View tables carry the REST roles**: `acme`'s `admin` key reads
/// `sys_service`/`sys_handler` of its tenant; `sys_key` (`/admin/keys`, the business
/// of `cluster-admin`) is the asker's error. Joinable with `sys_invocation`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn views_follow_rest_roles() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let r = common::within(Duration::from_secs(60), "views by role", async {
        let (_, acme, _) = &fx.keys[0];
        let (code, v) = fx
            .query(
                acme,
                "SELECT service, name FROM sys_handler ORDER BY service, name",
            )
            .await?;
        ensure!(
            // `Hang/run`: the waiting service of UI questions 5 and 8, same deployment.
            code == 200 && v["rows"] == json!([["Echo", "run"], ["Hang", "run"]]),
            "{code} {v}"
        );
        let (code, v) = fx.query(acme, "SELECT * FROM sys_key").await?;
        ensure!(code == 400, "{code} {v}");
        let v = fx
            .query_until(
                acme,
                "SELECT h.service, count(i.id) FROM sys_handler h \
                 JOIN sys_invocation i ON i.service = h.service AND i.handler = h.name \
                 GROUP BY h.service",
                1,
            )
            .await?;
        ensure!(v["rows"] == json!([["Echo", 3]]), "{v}");
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

// ─────────────────────────────────────────── UI questions
//
// The questions that need view tables (the server's RAM/bucket). The questions on ranges are in
// `cairn-query/tests/sql.rs::ui_question_*`.

/// Asks until `ok`.
async fn until(fx: &Fx, key: &str, sql: &str, ok: impl Fn(&Value) -> bool) -> Result<Value> {
    let t0 = std::time::Instant::now();
    loop {
        let (code, v) = fx.query(key, sql).await?;
        if code == 200 && ok(&v) {
            return Ok(v);
        }
        ensure!(t0.elapsed() < Duration::from_secs(20), "{sql}: {code} {v}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Question 2: rate / success ratio per handler.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ui_question_2() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let r = common::within(Duration::from_secs(60), "question 2", async {
        let (_, acme, _) = &fx.keys[0];
        until(
            &fx,
            acme,
            "SELECT service, handler, sum(completed), sum(failed) FROM sys_handler_stats \
             GROUP BY service, handler",
            |v| v["rows"] == json!([["Echo", "run", 3, 0]]),
        )
        .await?;
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// **The `limit_key` column of `sys_invocation`**: a call with a scope and
/// `?limit-key=` ⇒ the column carries the limit key; a call without ⇒ `NULL`. The console uses it
/// to find the queue entry of the invocation itself (the "why" banner).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invocation_carries_limit_key() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let r = common::within(Duration::from_secs(60), "limit_key", async {
        let (_, acme, invoke) = &fx.keys[0];
        let res = fx
            .http
            .post(format!(
                "{}/scope/growth/invoke/Echo/run?limit-key=alice",
                fx.url
            ))
            .bearer_auth(invoke)
            .body("{}")
            .send()
            .await?;
        ensure!(res.status().is_success(), "invoke: {}", res.status());
        let id = res
            .headers()
            .get("x-cairn-invocation-id")
            .and_then(|v| v.to_str().ok())
            .context("missing id")?
            .to_string();
        let v = fx
            .query_until(
                acme,
                &format!("SELECT scope, limit_key FROM sys_invocation WHERE id = '{id}'"),
                1,
            )
            .await?;
        ensure!(v["rows"] == json!([["growth", "alice"]]), "{v}");
        let v = fx
            .query_until(
                acme,
                "SELECT limit_key FROM sys_invocation WHERE scope IS NULL LIMIT 1",
                1,
            )
            .await?;
        ensure!(v["rows"] == json!([[null]]), "{v}");
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// Question 9: success ratio per deployment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ui_question_9() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let r = common::within(Duration::from_secs(60), "question 9", async {
        let (_, acme, _) = &fx.keys[0];
        let v = until(
            &fx,
            acme,
            "SELECT s.deployment_id, sum(h.completed) FROM sys_handler_stats h \
             JOIN sys_service s ON s.name = h.service GROUP BY s.deployment_id",
            |v| v["rows"].as_array().is_some_and(|r| r.len() == 1),
        )
        .await?;
        ensure!(v["rows"][0][1] == 3, "{v}");
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// Question 6: which queues are long, each range's scheduler.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ui_question_6() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let r = common::within(Duration::from_secs(60), "question 6", async {
        let (code, v) = fx
            .query(
                ADMIN,
                "SELECT range, waiting, max_turns FROM sys_scheduler ORDER BY range",
            )
            .await?;
        ensure!(
            code == 200 && v["rows"].as_array().map(Vec::len) == Some(2),
            "{code} {v}"
        );
        let (code, v) = fx
            .query(
                ADMIN,
                "SELECT queue, waiting FROM sys_vqueue ORDER BY waiting DESC LIMIT 5",
            )
            .await?;
        ensure!(code == 200, "{code} {v}");
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// Question 7: how much a tenant uses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ui_question_7() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let r = common::within(Duration::from_secs(60), "question 7", async {
        let (_, acme, _) = &fx.keys[0];
        let (code, v) = fx
            .query(
                acme,
                "SELECT tenant, invocations, cost_micros FROM sys_metering",
            )
            .await?;
        ensure!(code == 200 && v["rows"][0][0] == "acme", "{code} {v}");
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// Question 43: schedules and their next fire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ui_question_43() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let r = common::within(Duration::from_secs(60), "question 43", async {
        let (_, acme, _) = &fx.keys[0];
        let (code, v) = call(
            &fx.http,
            acme,
            fx.http.put(format!("{}/schedules/nightly", fx.url)).json(
                &json!({ "everyMs": 60_000, "target": { "service": "Echo", "handler": "run" } }),
            ),
        )
        .await?;
        ensure!(code == 200, "{code} {v}");
        let (code, v) = fx
            .query(
                acme,
                "SELECT name, service, handler, next_at IS NOT NULL, paused FROM sys_schedule",
            )
            .await?;
        ensure!(
            code == 200 && v["rows"] == json!([["nightly", "Echo", "run", true, false]]),
            "{code} {v}"
        );
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// Question 44: rules, limits, schedulers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ui_question_44() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let r = common::within(Duration::from_secs(60), "question 44", async {
        let (code, v) = call(
            &fx.http,
            ADMIN,
            fx.http
                .post(format!("{}/rules", fx.url))
                .json(&json!({ "pattern": "acme", "concurrency": 5 })),
        )
        .await?;
        ensure!(code < 300, "{code} {v}");
        let v = until(
            &fx,
            ADMIN,
            "SELECT r.pattern, r.concurrency, max(s.rules_version) FROM sys_rule r \
             CROSS JOIN sys_scheduler s GROUP BY r.pattern, r.concurrency",
            |v| v["rows"].as_array().is_some_and(|r| r.len() == 1),
        )
        .await?;
        ensure!(v["rows"][0][0] == "acme" && v["rows"][0][1] == "5", "{v}");
        let (code, v) = fx.query(ADMIN, "SELECT count(*) FROM sys_limit").await?;
        ensure!(code == 200, "{code} {v}");
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// Each read endpoint of ingress and the query table answering the same question — or the reason
/// there is none. A `—` row is a deliberate exclusion or work
/// still to do.
const QUERY_ENDPOINTS: &[(&str, &str)] = &[
    ("/health", "— not data"),
    (
        "/values/v1/{token}",
        "— the bytes of one segment by a short-lived token, not a table",
    ),
    ("/auth/whoami", "— the key's identity, not data"),
    (
        "/identity",
        "— the tenant's public request-identity keys, not run data",
    ),
    (
        "/tenant",
        "— the tenant's own limits; cluster-admin sees them through /admin/tenants",
    ),
    ("/deployments", "sys_deployment"),
    ("/deployments/{id}", "sys_deployment"),
    ("/services", "sys_service"),
    (
        "/workflow/{service}/{key}/output",
        "— outputs do not enter the query tier",
    ),
    (
        "/scope/{scope}/workflow/{service}/{key}/output",
        "— outputs do not enter the query tier",
    ),
    (
        "/workflow/{service}/{key}/id",
        "— only computes a run id, not data",
    ),
    (
        "/scope/{scope}/workflow/{service}/{key}/id",
        "— only computes a run id, not data",
    ),
    (
        "/scope/{scope}/promises/{service}/{key}/{name}",
        "sys_promise",
    ),
    ("/rules", "sys_rule"),
    ("/rules/{*pattern}", "sys_rule"),
    ("/limits", "sys_limit"),
    ("/vqueues", "sys_vqueue"),
    ("/scheduler", "sys_scheduler"),
    ("/invocations", "sys_invocation"),
    ("/invocations/{id}", "sys_invocation"),
    ("/invocations/{id}/journal", "sys_journal"),
    ("/invocations/{id}/streams/{name}", "sys_stream_record"),
    ("/invocations/{id}/logs", "sys_log"),
    ("/invocations/{id}/spans", "sys_span"),
    ("/invocations/{id}/artifacts", "sys_artifact"),
    (
        "/invocations/{id}/artifacts/{sha}",
        "— artifact content is bytes, not rows",
    ),
    ("/schedules", "sys_schedule"),
    ("/schedules/{name}", "sys_schedule"),
    ("/scope/{scope}/schedules", "sys_schedule"),
    ("/scope/{scope}/schedules/{name}", "sys_schedule"),
    ("/metering", "sys_metering"),
    ("/audit", "sys_audit"),
    (
        "/metering/hours",
        "— hourly history for the Usage chart: views filter only by `=` on a column, \
         an hour range is not a column; the current hour is sys_metering",
    ),
    ("/ranges/windows", "sys_range_window"),
    ("/ranges/stats", "sys_range_load"),
    ("/ranges/suggestions", "sys_range_suggestion"),
    ("/promises/{service}/{key}/{name}", "sys_promise"),
    ("/cluster", "sys_range"),
    ("/metrics", "— Prometheus, not a table"),
    ("/ranges/ops", "sys_range_op"),
    ("/ranges/ops/{id}", "sys_range_op"),
    ("/admin/keys", "sys_key"),
    (
        "/admin/keys/{id}/revocation",
        "— the question \"has every node applied the revocation\" (re-reads the registry, requires acks), not data",
    ),
    (
        "/admin/oidc",
        "— a control plane's OIDC relying-party configuration, not customer data",
    ),
    ("/admin/tenants", "sys_tenant"),
    (
        "/admin/tenants/{tenant}/purge",
        "— a control plane's purge progress (phase, marks, object counts), not run data",
    ),
    ("/archive", "sys_archive"),
    (
        "/archive/{id}",
        "— one archive's details (journal, hash chain) are REST; the list by date is sys_archive",
    ),
    ("/stats/handlers", "sys_handler_stats"),
    ("/stats/slow-runs", "sys_slow_run"),
    ("/stats/history", "sys_handler_history"),
    ("/cluster/nodes", "sys_node"),
    ("/admin/nodewal", "sys_nodewal"),
    ("/admin/scrub", "sys_scrub"),
    ("/admin/trash", "sys_trash"),
    ("/metering/pricing", "sys_pricing"),
    ("/admin/capture", "sys_capture"),
    (
        "/admin/capture/{tenant}/{id}",
        "— a capture bundle is bytes; its metadata is in sys_capture",
    ),
    ("/admin/quarantine", "sys_quarantine"),
    ("/admin/cluster-version", "sys_cluster_version"),
    (
        "/admin/ops-reports",
        "— cairn-ctl operations reports (JSON on the bucket), not customer data",
    ),
    (
        "/admin/ops-reports/{id}",
        "— cairn-ctl operations reports (JSON on the bucket), not customer data",
    ),
    ("/admin/gc/plan", "— GC operations, not customer data"),
    (
        "/admin/gc/barriers",
        "— GC operations, not customer data",
    ),
    (
        "/admin/gc/barriers/{id}",
        "— GC operations, not customer data",
    ),
    ("/admin/failpoints", "— tests only"),
];

/// **Every read endpoint has a query table or a reason**: a new
/// `GET` route in ingress without a row in `QUERY_ENDPOINTS` is red; a row
/// pointing to a missing table is red too.
#[test]
fn every_read_endpoint_has_a_query_table() {
    let src = include_str!("../src/ingress/mod.rs");
    let start = src.find("pub fn router(state: AppState)").expect("router");
    let end = src.find("pub fn internal_router").expect("internal_router");
    let mut gets = Vec::new();
    for part in src[start..end].split(".route(").skip(1) {
        let Some(path) = part
            .trim_start()
            .strip_prefix('"')
            .and_then(|p| p.split('"').next())
        else {
            continue;
        };
        let call = part.split(".route(").next().unwrap_or(part);
        if call.contains("get(") {
            gets.push(path);
        }
    }
    let tables = cairn_query::tables();
    for p in &gets {
        let Some((_, how)) = QUERY_ENDPOINTS.iter().find(|(e, _)| e == p) else {
            panic!("GET {p} is not in QUERY_ENDPOINTS yet — add a query table or a reason");
        };
        assert!(
            how.starts_with('—') || tables.contains(how),
            "GET {p} points to table {how}, which does not exist"
        );
    }
    for (e, _) in QUERY_ENDPOINTS {
        assert!(
            gets.contains(e),
            "QUERY_ENDPOINTS still has {e} but ingress does not"
        );
    }
}

/// **`GET /invocations?cursor=` pages by key**: pages of
/// 2 rows chain to all three of `acme`'s invocations, without duplicates, newest first;
/// `zeta` is not seen; filtering by tag and class; a broken `cursor` is 400. Without `cursor`:
/// the old list (an array).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invocations_page_by_cursor() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let r = common::within(Duration::from_secs(60), "pages by cursor", async {
        let (_, acme, acme_invoke) = &fx.keys[0];
        let tagged = fx
            .http
            .post(format!("{}/invoke/Echo/run", fx.url))
            .bearer_auth(acme_invoke)
            .header("cairnflow-tag", "team=a")
            .body("{}")
            .send()
            .await?;
        ensure!(tagged.status() == 200, "{}", tagged.text().await?);
        fx.query_until(acme, "SELECT id FROM sys_invocation", 4)
            .await?;
        let mut seen: Vec<(i64, String)> = Vec::new();
        let mut cursor = String::new();
        for page in 0.. {
            ensure!(page < 5, "pages never end");
            let (code, v) = fx
                .get(acme, &format!("/invocations?limit=3&cursor={cursor}"))
                .await?;
            ensure!(code == 200 && v["partial"] == false, "{code} {v}");
            for i in v["invocations"].as_array().unwrap() {
                seen.push((
                    i["createdAt"].as_i64().unwrap(),
                    i["id"].as_str().unwrap().to_string(),
                ));
            }
            match v["nextCursor"].as_str() {
                Some(c) => cursor = c.to_string(),
                None => break,
            }
        }
        ensure!(seen.len() == 4, "{seen:?}");
        let mut sorted = seen.clone();
        sorted.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        sorted.dedup();
        ensure!(sorted == seen, "order/duplicates: {seen:?}");
        let (code, v) = fx.get(acme, "/invocations?cursor=&tag=team=a").await?;
        ensure!(
            code == 200 && v["invocations"].as_array().map(Vec::len) == Some(1),
            "{code} {v}"
        );
        ensure!(v["invocations"][0]["tags"] == json!({"team": "a"}), "{v}");
        let (code, v) = fx.get(acme, "/invocations?cursor=&class=failed").await?;
        ensure!(code == 200 && v["invocations"] == json!([]), "{code} {v}");
        let (code, v) = fx.get(acme, "/invocations?cursor=1.xyz").await?;
        ensure!(code == 400, "{code} {v}");
        let (code, v) = fx.get(acme, "/invocations").await?;
        ensure!(code == 200 && v.is_array(), "old list: {code} {v}");
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// **Slow runs are queryable**: a threshold of 0 ms ⇒ every run
/// is slow; `sys_slow_run` (cluster-wide, the node's ring buffer) has three runs
/// of `acme`, and the `acme` key does not see `zeta`'s runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_runs_are_queryable() -> Result<()> {
    let fx = fixture(|cfg| cfg.slow_log = Duration::ZERO).await?;
    let r = common::within(Duration::from_secs(60), "slow log is queryable", async {
        let (_, acme, _) = &fx.keys[0];
        let v = until(
            &fx,
            acme,
            "SELECT tenant, service, handler, outcome, count(*) FROM sys_slow_run \
             GROUP BY tenant, service, handler, outcome",
            |v| v["rows"].as_array().is_some_and(|r| !r.is_empty()),
        )
        .await?;
        ensure!(v["rows"] == json!([["acme", "Echo", "run", "completed", 3]]), "{v}");
        let (code, v) = fx
            .query(
                ADMIN,
                "SELECT count(DISTINCT tenant), max(total_ms) >= min(threshold_ms) FROM sys_slow_run",
            )
            .await?;
        ensure!(code == 200 && v["rows"] == json!([[2, true]]), "{code} {v}");
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// **The cluster operations tables belong to cluster-admin**: a tenant's `admin` key
/// is refused on `sys_node`, `sys_nodewal`, `sys_scrub`,
/// `sys_trash`, `sys_pricing`, `sys_tenant`, `sys_inbound` (the asker's error,
/// 4xx); cluster-admin can read them. `sys_archive` requires `date`. The scrubber runs
/// every second: its first round is recorded (`GET /admin/scrub` has the node's row).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ops_tables_are_cluster_admin() -> Result<()> {
    let fx = fixture(|cfg| cfg.scrub_every = Some(Duration::from_secs(1))).await?;
    let r = common::within(Duration::from_secs(60), "operations tables", async {
        let (_, acme, _) = &fx.keys[0];
        for t in [
            "sys_node",
            "sys_nodewal",
            "sys_scrub",
            "sys_trash",
            "sys_pricing",
            "sys_tenant",
            "sys_inbound",
            "sys_range_load",
            "sys_range_window",
        ] {
            let (code, v) = fx.query(acme, &format!("SELECT * FROM {t}")).await?;
            ensure!((400..500).contains(&code), "{t} with acme key: {code} {v}");
            let (code, v) = fx.query(ADMIN, &format!("SELECT * FROM {t}")).await?;
            ensure!(code == 200, "{t} with cluster-admin: {code} {v}");
        }
        let (code, v) = fx.query(acme, "SELECT * FROM sys_archive").await?;
        ensure!(
            code == 400 && v.to_string().contains("date"),
            "sys_archive without date: {code} {v}"
        );
        let (code, v) = fx
            .query(acme, "SELECT * FROM sys_archive WHERE date = '2026-01-01'")
            .await?;
        ensure!(code == 200 && v["rows"] == json!([]), "{code} {v}");
        let (code, v) = fx
            .query(acme, "SELECT * FROM sys_archive WHERE date = '../x'")
            .await?;
        ensure!(code == 400, "odd date: {code} {v}");
        let (code, v) = fx.query(ADMIN, "SELECT count(*) FROM sys_node").await?;
        ensure!(code == 200 && v["rows"] == json!([[1]]), "{code} {v}");
        let t0 = std::time::Instant::now();
        loop {
            let (code, v) = fx.get(ADMIN, "/admin/scrub?scope=cluster").await?;
            ensure!(code == 200, "{code} {v}");
            if v["rows"].as_array().is_some_and(|r| !r.is_empty()) {
                ensure!(v["rows"][0]["node"].is_string(), "{v}");
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(30),
                "no scrub round: {v}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// **Durable stats are queryable** (UI question 3): flush cadence
/// 200 ms; three runs of `acme` go into the day's `stats/` object and read back
/// through `sys_handler_history` (merged across nodes, by hour); `zeta` does not see `acme`'s
/// numbers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_history_is_queryable() -> Result<()> {
    let fx = fixture(|cfg| cfg.metering_flush = Duration::from_millis(200)).await?;
    let r = common::within(Duration::from_secs(60), "durable stats", async {
        let (_, acme, _) = &fx.keys[0];
        let v = until(
            &fx,
            acme,
            "SELECT tenant, service, handler, sum(turns) FROM sys_handler_history \
             GROUP BY tenant, service, handler",
            |v| v["rows"].as_array().is_some_and(|r| !r.is_empty()),
        )
        .await?;
        ensure!(v["rows"] == json!([["acme", "Echo", "run", 3]]), "{v}");
        let (_, zeta, _) = &fx.keys[1];
        let (code, v) = fx
            .query(zeta, "SELECT DISTINCT tenant FROM sys_handler_history")
            .await?;
        ensure!(
            code == 200
                && v["rows"]
                    .as_array()
                    .is_some_and(|r| r.iter().all(|x| x[0] == "zeta")),
            "{code} {v}"
        );
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// Question 3 (the 24 h – 30 day chart): `sys_handler_history` aggregated
/// by hour within a window `hour_ms >= mark`. A fixed cost — the view always reads
/// `/stats/history?days=30` (one `stats/` object per day per node), the filter applies afterwards;
/// a 24 h window or a 30-day one costs the same. The hour mark is the start of the hour, the tenant comes from the key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ui_question_3() -> Result<()> {
    let fx = fixture(|cfg| cfg.metering_flush = Duration::from_millis(200)).await?;
    let r = common::within(Duration::from_secs(60), "question 3", async {
        let (_, acme, _) = &fx.keys[0];
        let since = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as i64
            - 30 * 24 * 3_600_000;
        let sql = format!(
            "SELECT hour_ms, sum(turns) AS turns, sum(completed) AS completed, sum(failed) AS failed, \
             sum(cu) AS cu FROM sys_handler_history WHERE hour_ms >= {since} \
             GROUP BY hour_ms ORDER BY hour_ms"
        );
        let v = until(&fx, acme, &sql, |v| {
            v["rows"].as_array().is_some_and(|r| !r.is_empty())
        })
        .await?;
        let rows = v["rows"].as_array().ok_or_else(|| anyhow!("{v}"))?;
        let turns: i64 = rows.iter().filter_map(|r| r[1].as_i64()).sum();
        ensure!(turns == 3, "acme's turns: {v}");
        ensure!(
            rows.iter()
                .all(|r| r[0].as_i64().is_some_and(|h| h >= since && h % 3_600_000 == 0)),
            "hour mark: {v}"
        );
        // A window in the future ⇒ empty (the filter applies after reading, no error).
        let (code, v) = fx
            .query(
                acme,
                &format!("SELECT hour_ms FROM sys_handler_history WHERE hour_ms >= {}", i64::MAX / 2),
            )
            .await?;
        ensure!(code == 200 && v["rows"] == json!([]), "{code} {v}");
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// **`cairnflow_query_index_state`**: once the background build is done, the node's
/// `/metrics` (cluster-admin) counts every range it holds as `readable` for each descriptor,
/// `building` 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_index_state_metric() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let r = common::within(
        Duration::from_secs(60),
        "metric of the index states",
        async {
            let t0 = std::time::Instant::now();
            loop {
                let text = fx
                    .http
                    .get(format!("{}/metrics", fx.url))
                    .bearer_auth(ADMIN)
                    .send()
                    .await?
                    .text()
                    .await?;
                let line = |ix: &str, st: &str| {
                    let p =
                        format!("cairnflow_query_index_state{{index=\"{ix}\",state=\"{st}\"}} ");
                    text.lines()
                        .find_map(|l| l.strip_prefix(p.as_str()))
                        .and_then(|v| v.trim().parse::<u64>().ok())
                };
                let all = cairn_store::query_index::QUERY_INDEXES.iter().all(|ix| {
                    line(ix.name, "readable") == Some(2) && line(ix.name, "building") == Some(0)
                });
                if all {
                    return Ok(());
                }
                ensure!(
                    t0.elapsed() < Duration::from_secs(30),
                    "two readable ranges not seen yet:\n{}",
                    text.lines()
                        .filter(|l| l.contains("query_index_state"))
                        .collect::<Vec<_>>()
                        .join("\n")
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        },
    )
    .await;
    fx.node.shutdown().await;
    r
}

/// Sends `n` `Hang/run` calls of tenant `acme` (waiting, never completing) and
/// returns their ids.
async fn hang(fx: &Fx, n: usize) -> Result<Vec<String>> {
    let (_, _, invoke) = &fx.keys[0];
    let mut ids = Vec::new();
    for _ in 0..n {
        let (code, v) = call(
            &fx.http,
            invoke,
            fx.http
                .post(format!("{}/invoke/Hang/run/send", fx.url))
                .body("{}"),
        )
        .await?;
        ensure!(code < 300, "send: {code} {v}");
        ids.push(v["id"].as_str().unwrap_or_default().to_string());
    }
    Ok(ids)
}

async fn cancel_all(fx: &Fx, ids: &[String]) -> Result<()> {
    let (_, admin, _) = &fx.keys[0];
    for id in ids {
        let (code, v) = call(
            &fx.http,
            admin,
            fx.http.post(format!("{}/invocations/{id}/cancel", fx.url)),
        )
        .await?;
        ensure!(code < 300, "cancel {id}: {code} {v}");
    }
    Ok(())
}

/// **Question 5: which services cross a health threshold** — joining the per-class counters
/// (question 1) with the per-handler summary (question 2) in one SQL query.
///
/// 1. Fixture: `acme`'s `Echo/run` completes 3 times.
/// 2. Two `Hang/run` wait, then are cancelled (⇒ class `failed`).
///
/// Must see: a failed ratio ≥ 50 % selects only `Hang` (2/2), not `Echo`
/// (0/3); the summary's turns column joins by service.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ui_question_5() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let r = common::within(Duration::from_secs(60), "question 5", async {
        let ids = hang(&fx, 2).await?;
        cancel_all(&fx, &ids).await?;
        let (_, acme, _) = &fx.keys[0];
        let sql = "SELECT c.service, \
                   sum(CASE WHEN c.class = 'failed' THEN c.count ELSE 0 END) AS failed, \
                   sum(c.count) AS total, \
                   max(s.turns) AS turns \
                   FROM sys_invocation_counts c \
                   LEFT JOIN (SELECT service, sum(turns) AS turns FROM sys_handler_stats GROUP BY service) s \
                     ON s.service = c.service \
                   GROUP BY c.service \
                   HAVING sum(CASE WHEN c.class = 'failed' THEN c.count ELSE 0 END) * 2 >= sum(c.count) \
                   ORDER BY c.service";
        // The per-handler summary aggregates turns on a cadence: wait for the `turns` column too.
        let v = until(&fx, acme, sql, |v| {
            v["rows"]
                .as_array()
                .is_some_and(|r| r.len() == 1 && r[0][1] == 2 && !r[0][3].is_null())
        })
        .await?;
        ensure!(
            v["rows"][0][0] == "Hang" && v["rows"][0][2] == 2 && v["rows"][0][3] == 2,
            "{v}"
        );
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// **Question 8: deployments that still have bound invocations; drained deployments can be removed** —
/// `sys_deployment.pinned` (counted cluster-wide through `GET /deployments?pinned=true`).
///
/// 1. Two `acme` `Hang/run` wait ⇒ `acme`'s deployment has `pinned` = 2.
/// 2. Cancel both ⇒ `pinned` = 0: removable.
///
/// Must see: exactly those two values, and `zeta`'s deployment (nothing waiting)
/// does not mix into `acme`'s query.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ui_question_8() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let r = common::within(Duration::from_secs(60), "question 8", async {
        let ids = hang(&fx, 2).await?;
        let (_, acme, _) = &fx.keys[0];
        let sql = "SELECT id, pinned FROM sys_deployment";
        until(&fx, acme, sql, |v| {
            v["rows"]
                .as_array()
                .is_some_and(|r| r.len() == 1 && r[0][1] == 2)
        })
        .await?;
        cancel_all(&fx, &ids).await?;
        until(&fx, acme, sql, |v| {
            v["rows"]
                .as_array()
                .is_some_and(|r| r.len() == 1 && r[0][1] == 0)
        })
        .await?;
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// **`POST /query` takes `params`**: `$1` gives exactly the rows of the
/// literal query; an SQL injection string cannot escape; missing/extra parameters, floats, arrays
/// ⇒ `400`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_params_bind_values() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let r = common::within(Duration::from_secs(60), "params", async {
        let (_, acme, _) = &fx.keys[0];
        fx.query_until(acme, "SELECT * FROM sys_invocation_counts", 1)
            .await?;
        let (code, lit) = fx
            .query(
                acme,
                "SELECT id FROM sys_invocation WHERE service = 'Echo' ORDER BY id",
            )
            .await?;
        ensure!(
            code == 200 && lit["rows"].as_array().is_some_and(|r| !r.is_empty()),
            "{code} {lit}"
        );
        let post = |body: Value| {
            call(
                &fx.http,
                acme,
                fx.http.post(format!("{}/query", fx.url)).json(&body),
            )
        };
        let sql = "SELECT id FROM sys_invocation WHERE service = $1 ORDER BY id";
        let (code, v) = post(json!({ "sql": sql, "params": ["Echo"] })).await?;
        ensure!(code == 200 && v["rows"] == lit["rows"], "{code} {v}");
        let (code, v) = post(json!({ "sql": sql, "params": ["Echo' OR '1'='1"] })).await?;
        ensure!(
            code == 200 && v["rows"] == json!([]),
            "a parameter escaped its string: {code} {v}"
        );
        for params in [
            json!([]),
            json!(["a", "b"]),
            json!([1.5]),
            json!([["Echo"]]),
        ] {
            let (code, v) = post(json!({ "sql": sql, "params": params })).await?;
            ensure!(code == 400, "params {params}: {code} {v}");
        }
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// **Decoded journal**: `GET /invocations/{id}/journal?decode=1`
/// adds `entry` (the header part) and `body` (the body) to each entry; without the flag the
/// old shape is unchanged; the REST JSON matches the `entry_lite`/`body` columns of
/// `sys_journal` (the same decoder).
///
/// Must see: entry 0 is an `Input` with JSON body `{}`; `Output` has body `ok`
/// as `utf8`; REST and SQL give the same `entry_lite`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn journal_decodes_for_people() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let r = common::within(Duration::from_secs(60), "decoded journal", async {
        let (_, acme, _) = &fx.keys[0];
        let rows = fx
            .query_until(
                acme,
                "SELECT id FROM sys_invocation WHERE service = 'Echo'",
                3,
            )
            .await?;
        let id = rows["rows"][0][0].as_str().unwrap_or_default().to_string();
        let (code, plain) = fx.get(acme, &format!("/invocations/{id}/journal")).await?;
        ensure!(code == 200, "{code} {plain}");
        ensure!(
            plain[0].get("entry").is_none(),
            "no flag ⇒ old shape: {plain}"
        );
        let (code, j) = fx
            .get(acme, &format!("/invocations/{id}/journal?decode=1"))
            .await?;
        ensure!(code == 200, "{code} {j}");
        let entries = j.as_array().cloned().unwrap_or_default();
        ensure!(entries.len() >= 2, "{j}");
        ensure!(entries[0]["entry"]["kind"] == "Input", "{j}");
        ensure!(
            entries[0]["body"]["encoding"] == "json" && entries[0]["body"]["text"] == "{}",
            "{j}"
        );
        let out = entries
            .iter()
            .find(|e| e["entry"]["kind"] == "Output")
            .ok_or_else(|| anyhow!("no Output: {j}"))?;
        ensure!(
            out["body"]["encoding"] == "utf8" && out["body"]["text"] == "ok",
            "{out}"
        );
        let (code, sql) = call(
            &fx.http,
            acme,
            fx.http.post(format!("{}/query", fx.url)).json(&json!({
                "sql": "SELECT entry_lite FROM sys_journal WHERE id = $1 ORDER BY index",
                "params": [id],
            })),
        )
        .await?;
        ensure!(code == 200, "{code} {sql}");
        let from_sql: Vec<Value> = sql["rows"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .map(|r| serde_json::from_str(r[0].as_str().unwrap_or("null")).unwrap_or_default())
            .collect();
        let from_rest: Vec<Value> = entries.iter().map(|e| e["entry"].clone()).collect();
        ensure!(
            from_sql == from_rest,
            "SQL {from_sql:?} ≠ REST {from_rest:?}"
        );
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// **Bulk operations**: `POST /invocations/batch/{op}` runs
/// exactly the single-id handler for each id.
///
/// Must see: acme's two `Hang` are cancelled (`ok`, `cancelled`), zeta's id is
/// its own `404`, a bad id is its own `400`, the order is kept; an empty batch
/// or over 1 000 ids ⇒ `400`; an unknown op ⇒ `404`; an `invoke` key ⇒ `403`;
/// purging the whole batch after cancelling ⇒ `ok`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invocation_batch_runs_each_id_like_a_single_call() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let r = common::within(Duration::from_secs(60), "batch", async {
        let (_, acme, acme_invoke) = &fx.keys[0];
        let (_, _, zeta_invoke) = &fx.keys[1];
        let send = |key: String| {
            let (http, url) = (fx.http.clone(), fx.url.clone());
            async move {
                let r = http
                    .post(format!("{url}/invoke/Hang/run/send"))
                    .bearer_auth(&key)
                    .body("{}")
                    .send()
                    .await?;
                ensure!(r.status().as_u16() < 300, "send {}", r.status());
                let id = r.headers()["x-cairn-invocation-id"].to_str()?.to_string();
                Ok::<_, anyhow::Error>(id)
            }
        };
        let a1 = send(acme_invoke.clone()).await?;
        let a2 = send(acme_invoke.clone()).await?;
        let z = send(zeta_invoke.clone()).await?;
        let (code, v) = batch(&fx, acme, "cancel", json!({ "ids": [a1, z, "nope", a2] })).await?;
        ensure!(code == 200, "{code} {v}");
        let res = v["results"].as_array().cloned().unwrap_or_default();
        let ids: Vec<&str> = res.iter().map(|r| r["id"].as_str().unwrap_or("")).collect();
        ensure!(ids == [a1.as_str(), z.as_str(), "nope", a2.as_str()], "{v}");
        ensure!(
            res[0]["ok"] == true && res[0]["result"]["cancelled"] == true,
            "{v}"
        );
        ensure!(
            res[1]["ok"] == false && res[1]["status"] == 404,
            "other tenant: {v}"
        );
        ensure!(
            res[2]["ok"] == false && res[2]["status"] == 400,
            "wrong id: {v}"
        );
        ensure!(res[3]["ok"] == true, "{v}");

        let (code, v) = batch(&fx, acme, "purge", json!({ "ids": [a1, a2] })).await?;
        ensure!(code == 200, "{code} {v}");
        ensure!(
            v["results"]
                .as_array()
                .is_some_and(|r| r.iter().all(|x| x["ok"] == true)),
            "{v}"
        );

        let (code, _) = batch(&fx, acme, "cancel", json!({ "ids": [] })).await?;
        ensure!(code == 400, "empty: {code}");
        let many: Vec<String> = (0..1001).map(|i| format!("{i:048x}")).collect();
        let (code, _) = batch(&fx, acme, "cancel", json!({ "ids": many })).await?;
        ensure!(code == 400, "over the limit: {code}");
        let (code, _) = batch(&fx, acme, "explode", json!({ "ids": [a1] })).await?;
        ensure!(code == 404, "unknown op: {code}");
        let (code, _) = batch(&fx, acme_invoke, "cancel", json!({ "ids": [a1] })).await?;
        ensure!(code == 403, "invoke key: {code}");
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

async fn batch(fx: &Fx, key: &str, op: &str, body: Value) -> Result<(u16, Value)> {
    call(
        &fx.http,
        key,
        fx.http
            .post(format!("{}/invocations/batch/{op}", fx.url))
            .json(&body),
    )
    .await
}

/// The SQL console's sample queries (`ui/src/data/samples.json`) run
/// on a real node, with a cluster-admin key: changing a table/column and forgetting the
/// samples ⇒ red here, not in the user's hands. Each sample's question number is checked
/// on the console side (`samples.test.ts`: has a `ui_question_<n>`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_samples_run() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../ui/src/data/samples.json");
    let samples: Vec<Value> = serde_json::from_str(&std::fs::read_to_string(&path)?)?;
    let r = common::within(Duration::from_secs(120), "console sample queries", async {
        ensure!(samples.len() >= 40, "{} sample queries", samples.len());
        for s in &samples {
            let sql = s["sql"].as_str().ok_or_else(|| anyhow!("no sql in {s}"))?;
            let (code, v) = fx.query(ADMIN, sql).await?;
            ensure!(code == 200, "PQ#{}: {code} {v}\n{sql}", s["pq"]);
        }
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}

/// **An SQL query that is too long is refused before it runs.**
///
/// 1. `acme`'s `admin` key sends `SELECT 1+1+…` of ~1 MiB (under the JSON extractor's 2 MiB
///    limit) — formerly it went straight into DataFusion: synchronous planning,
///    a left-leaning tree 5·10⁵ deep, which could overflow the query thread's stack.
/// 2. Right after, an ordinary query with the same key.
///
/// Must see: (1) `400` with an English body naming the limit, within < 2 s; (2) `200` —
/// the node alive, the query slot returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_sql_is_400() -> Result<()> {
    let fx = fixture(|_| {}).await?;
    let r = common::within(Duration::from_secs(60), "1 MiB SQL query", async {
        let key = &fx.keys[0].1;
        let long = format!("SELECT 1{}", "+1".repeat(500_000));
        let started = std::time::Instant::now();
        let (code, v) = fx.query(key, &long).await?;
        let took = started.elapsed();
        ensure!(code == 400, "1 MiB query: {code} {v}");
        let msg = v["error"].as_str().unwrap_or_default();
        ensure!(
            msg.is_ascii() && msg.contains("bytes"),
            "the error body must name the length limit, in English: {v}"
        );
        ensure!(took < Duration::from_secs(2), "refusing took {took:?}");
        let (code, v) = fx.query(key, "SELECT 1").await?;
        ensure!(code == 200, "ordinary query after the long one: {code} {v}");
        Ok(())
    })
    .await;
    fx.node.shutdown().await;
    r
}
