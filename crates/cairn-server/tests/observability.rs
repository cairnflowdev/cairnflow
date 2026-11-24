// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Observability for operators: a per-handler summary, local and cluster-wide
//! `/stats/handlers`, the slow log, `/invocations?scope=cluster`, capturing the next run.
//!
//! Every assertion goes through HTTP — what the operator sees — never reading the node's
//! in-RAM structures.

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
    cairn_server::node::{self, NodeMode, RunningNode},
    common::{
        fake_sdk::{FakeSdk, Step},
        load_of, single_leaders, solo_config, start_cluster_node_tweaked, start_led, test_config,
        wait_probe, within, ClusterNode, TEST_DEADLINE,
    },
    reqwest::{Client, StatusCode},
    serde_json::Value,
    std::{collections::BTreeSet, time::Duration},
};

const TTL: Duration = Duration::from_secs(1);
const ADMIN_KEY: &str = "cf_test_observability_bootstrap_0123456789";

async fn get_json(url: &str) -> Result<(StatusCode, Value)> {
    let r = reqwest::get(url).await?;
    let code = r.status();
    let text = r.text().await?;
    Ok((
        code,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    ))
}

/// The rows of `(service, handler)` in a `/stats/handlers` response.
fn rows_of<'a>(body: &'a Value, service: &str, handler: &str) -> Vec<&'a Value> {
    body["handlers"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter(|r| r["service"] == service && r["handler"] == handler)
                .collect()
        })
        .unwrap_or_default()
}

fn sum(rows: &[&Value], field: &str) -> u64 {
    rows.iter().map(|r| r[field].as_u64().unwrap_or(0)).sum()
}

/// Asks `url` until `ok(body)` holds — the summary is aggregated in the background every second.
async fn poll(url: &str, what: &str, ok: impl Fn(&Value) -> bool) -> Result<Value> {
    let t0 = std::time::Instant::now();
    loop {
        let (code, body) = get_json(url).await?;
        if code == StatusCode::OK && ok(&body) {
            return Ok(body);
        }
        if t0.elapsed() > Duration::from_secs(20) {
            return Err(anyhow!(
                "over 20 s and still not reached: {what} — last {code} {body}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Three handlers: completes at once, fails forever (retryably), waits for a signal.
async fn three_handlers() -> Result<FakeSdk> {
    FakeSdk::builder()
        .service("Ok", "run", vec![Step::output("ok")])
        .service("Bad", "run", vec![Step::FailRetry])
        .service("Hold", "run", vec![Step::Suspend])
        .spawn()
        .await
}

/// The summary and local stats: turn count, outcomes by status, failed attempts,
/// suspensions, durable steps and duration percentiles — per `(tenant, service, handler,
/// deployment)`, with `node`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_summary_counts_turns_outcomes_and_failures() -> Result<()> {
    let sdk = three_handlers().await?;
    let node = start_led(solo_config(2)).await?;
    let url = node.url();
    let r = within(TEST_DEADLINE, "summary by handler", async {
        let dep = common::register(&url, &sdk.url).await?;
        let dep_id = dep["id"].as_str().unwrap_or_default().to_string();
        for _ in 0..3 {
            let (code, body) = common::invoke(&url, "Ok", "run", None, "x").await?;
            ensure!(
                code == 200 && body["output"] == "ok",
                "Ok/run: {code} {body}"
            );
        }
        common::send(&url, "Bad", "run", None, "x").await?;
        common::send(&url, "Hold", "run", None, "x").await?;

        let stats = format!("{url}/stats/handlers");
        let body = poll(&stats, "all three handlers", |b| {
            sum(&rows_of(b, "Ok", "run"), "completed") >= 3
                && sum(&rows_of(b, "Bad", "run"), "failedAttempts") >= 2
                && sum(&rows_of(b, "Hold", "run"), "suspensions") >= 1
        })
        .await?;
        ensure!(
            body["scope"] == "local" && body["partial"] == false,
            "{body}"
        );

        let ok = rows_of(&body, "Ok", "run");
        ensure!(ok.len() == 1, "Ok/run must be exactly one row: {body}");
        let ok = ok[0];
        ensure!(ok["node"] == node.node_id, "missing node: {ok}");
        ensure!(ok["tenant"] == "default", "tenant: {ok}");
        ensure!(
            ok["deployment"] == dep_id.as_str(),
            "deployment: {ok} (registered {dep_id})"
        );
        ensure!(ok["turns"].as_u64() == Some(3), "turns: {ok}");
        ensure!(
            ok["steps"].as_u64() >= Some(3),
            "steps (Output is one command): {ok}"
        );
        ensure!(ok["failed"] == 0 && ok["failedAttempts"] == 0, "{ok}");
        let (p50, p99, max) = (
            ok["p50Ms"].as_f64().unwrap_or(-1.0),
            ok["p99Ms"].as_f64().unwrap_or(-1.0),
            ok["maxMs"].as_f64().unwrap_or(-1.0),
        );
        ensure!(
            0.0 <= p50 && p50 <= p99 && p99 <= max * 1.25 + 1.0,
            "percentiles out of order: {ok}"
        );

        let bad = rows_of(&body, "Bad", "run");
        ensure!(
            sum(&bad, "completed") == 0 && sum(&bad, "turns") >= 2,
            "Bad/run: {body}"
        );
        let hold = rows_of(&body, "Hold", "run");
        ensure!(sum(&hold, "completed") == 0, "Hold/run: {body}");

        // A bad `?window=` ⇒ 400, not an empty list.
        let (code, _) = get_json(&format!("{stats}?window=abc")).await?;
        ensure!(code == StatusCode::BAD_REQUEST, "window=abc: {code}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

async fn authed_node() -> Result<RunningNode> {
    let mut cfg = test_config(NodeMode::single(2));
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
        .map_err(|e| anyhow!("could not build the node: {e}"))
}

async fn key_for(url: &str, tenant: &str, role: &str) -> Result<String> {
    let r = Client::new()
        .post(format!("{url}/admin/keys"))
        .bearer_auth(ADMIN_KEY)
        .json(&serde_json::json!({ "tenant": tenant, "role": role }))
        .send()
        .await?;
    ensure!(
        r.status() == StatusCode::OK,
        "create key {tenant}: {}",
        r.status()
    );
    let v: Value = r.json().await?;
    Ok(v["secret"].as_str().unwrap_or_default().to_string())
}

async fn authed(key: &str, req: reqwest::RequestBuilder) -> Result<(StatusCode, Value)> {
    let r = req.bearer_auth(key).send().await?;
    let code = r.status();
    let text = r.text().await?;
    Ok((
        code,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    ))
}

/// Stats: a tenant's key sees only that tenant; `?tenant=` is only for a
/// cluster admin; a cluster admin choosing no tenant sees every tenant.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_summary_is_scoped_by_tenant() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Greeter", "hi", vec![Step::output("hi")])
        .spawn()
        .await?;
    let node = authed_node().await?;
    let url = node.url();
    let r = within(TEST_DEADLINE, "summary by tenant", async {
        let http = Client::new();
        let acme = key_for(&url, "acme", "admin").await?;
        let globex = key_for(&url, "globex", "admin").await?;
        for key in [&acme, &globex] {
            let (code, body) = authed(
                key,
                http.post(format!("{url}/deployments"))
                    .json(&serde_json::json!({ "uri": sdk.url })),
            )
            .await?;
            ensure!(code == StatusCode::OK, "register: {code} {body}");
            let (code, body) = authed(
                key,
                http.post(format!("{url}/invoke/Greeter/hi")).body("{}"),
            )
            .await?;
            ensure!(code == StatusCode::OK, "invoke: {code} {body}");
        }
        // Cluster admin, no tenant chosen ⇒ both tenants.
        let t0 = std::time::Instant::now();
        let all = loop {
            let (code, body) = authed(ADMIN_KEY, http.get(format!("{url}/stats/handlers"))).await?;
            let tenants: BTreeSet<String> = body["handlers"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|r| r["tenant"].as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            if code == StatusCode::OK && tenants.contains("acme") && tenants.contains("globex") {
                break body;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "cluster admin does not see every tenant: {code} {body}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        ensure!(!rows_of(&all, "Greeter", "hi").is_empty(), "{all}");

        let (code, body) = authed(&acme, http.get(format!("{url}/stats/handlers"))).await?;
        ensure!(code == StatusCode::OK, "acme: {code} {body}");
        let rows = body["handlers"].as_array().cloned().unwrap_or_default();
        ensure!(
            !rows.is_empty() && rows.iter().all(|r| r["tenant"] == "acme"),
            "acme sees rows outside its tenant: {body}"
        );
        let (code, _) = authed(
            &acme,
            http.get(format!("{url}/stats/handlers?tenant=globex")),
        )
        .await?;
        ensure!(
            code == StatusCode::FORBIDDEN,
            "acme chooses another tenant: {code}"
        );

        let (code, body) = authed(
            ADMIN_KEY,
            http.get(format!("{url}/stats/handlers?tenant=globex")),
        )
        .await?;
        let rows = body["handlers"].as_array().cloned().unwrap_or_default();
        ensure!(
            code == StatusCode::OK
                && !rows.is_empty()
                && rows.iter().all(|r| r["tenant"] == "globex"),
            "cluster-admin ?tenant=globex: {code} {body}"
        );
        // An `invoke` key cannot read stats.
        let invoker = key_for(&url, "acme", "invoke").await?;
        let (code, _) = authed(&invoker, http.get(format!("{url}/stats/handlers"))).await?;
        ensure!(
            code == StatusCode::FORBIDDEN,
            "invoke key reads stats: {code}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// Two nodes on one bucket, each leading at least one range.
async fn two_nodes(secret_b: Option<&str>) -> Result<(common::Bucket, Vec<ClusterNode>)> {
    let bucket = common::bucket();
    let mut nodes = Vec::new();
    for (id, secret) in [("node-a", None), ("node-b", secret_b)] {
        nodes.push(
            start_cluster_node_tweaked(&bucket, id, 4, TTL, |cfg| {
                cfg.deploy_refresh = Duration::from_millis(200);
                if let Some(s) = secret {
                    cfg.auth.cluster_secret = Some(s.to_string());
                }
            })
            .await?,
        );
    }
    Ok((bucket, nodes))
}

/// One `Counter/get` invocation on each range, sent straight to the range's leader.
async fn one_per_partition(nodes: &[ClusterNode], sdk: &FakeSdk) -> Result<Vec<String>> {
    let refs: Vec<_> = nodes.iter().collect();
    let probe = wait_probe(&refs, Duration::from_secs(20), "balanced 2/2", |p| {
        single_leaders(p).is_some_and(|m| load_of(&m, 2) == [2, 2])
    })
    .await?;
    let leaders = single_leaders(&probe).expect("leaders exist");
    for n in nodes {
        common::register(&n.node.url(), &sdk.url).await?;
    }
    let mut ids = Vec::new();
    for (p, i) in &leaders {
        let leader = &nodes[*i].node;
        let key = common::key_on_partition(leader, "Counter", *p, "k");
        let (code, body) = common::invoke(&leader.url(), "Counter", "get", Some(&key), "x").await?;
        ensure!(
            code == 200 && body["output"] == "ok",
            "partition {p}: {code} {body}"
        );
        ids.push(body["id"].as_str().unwrap_or_default().to_string());
    }
    Ok(ids)
}

/// Cluster-wide stats: ask one node, get the rows of every node, each row carrying
/// `node`; with nobody missing, `partial = false`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_summary_cluster_scope_has_every_node() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object("Counter", "get", vec![Step::output("ok")])
        .spawn()
        .await?;
    let (_bucket, nodes) = two_nodes(None).await?;
    let r = within(Duration::from_secs(60), "cluster-wide summary", async {
        one_per_partition(&nodes, &sdk).await?;
        let url = format!("{}/stats/handlers?scope=cluster", nodes[0].node.url());
        let body = poll(&url, "all 4 invocations from both nodes", |b| {
            sum(&rows_of(b, "Counter", "get"), "completed") >= 4
        })
        .await?;
        ensure!(
            body["scope"] == "cluster" && body["partial"] == false,
            "{body}"
        );
        ensure!(
            body["missing"].as_array().is_some_and(|m| m.is_empty()),
            "missing must be empty: {body}"
        );
        let seen: BTreeSet<String> = rows_of(&body, "Counter", "get")
            .iter()
            .filter_map(|r| r["node"].as_str().map(String::from))
            .collect();
        ensure!(
            seen == BTreeSet::from(["node-a".to_string(), "node-b".to_string()]),
            "rows must come from both nodes: {body}"
        );
        ensure!(
            sum(&rows_of(&body, "Counter", "get"), "completed") == 4,
            "over/under-counted: {body}"
        );

        // Locally, only the asked node's share.
        let (_, local) = get_json(&format!("{}/stats/handlers", nodes[0].node.url())).await?;
        ensure!(
            rows_of(&local, "Counter", "get")
                .iter()
                .all(|r| r["node"] == "node-a"),
            "{local}"
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// Stats: a node that does not answer ⇒ `partial = true` and its name in `missing` — no
/// guessing. Here node-b uses a different cluster secret, so it refuses every signed
/// question from node-a.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_summary_cluster_scope_reports_missing_node() -> Result<()> {
    let (_bucket, nodes) = two_nodes(Some("cf_test_other_secret_of_node_b_0123456789")).await?;
    let r = within(Duration::from_secs(60), "missing node", async {
        let refs: Vec<_> = nodes.iter().collect();
        wait_probe(
            &refs,
            Duration::from_secs(20),
            "node-b leads at least one range",
            |p| p.values().any(|v| v == &vec![1]),
        )
        .await?;
        let url = format!("{}/stats/handlers?scope=cluster", nodes[0].node.url());
        let body = poll(&url, "node-b is in missing", |b| {
            b["missing"]
                .as_array()
                .is_some_and(|m| m.iter().any(|x| x == "node-b"))
        })
        .await?;
        ensure!(body["partial"] == true, "{body}");
        let (code, inv) = get_json(&format!(
            "{}/invocations?scope=cluster",
            nodes[0].node.url()
        ))
        .await?;
        ensure!(
            code == StatusCode::OK && inv["partial"] == true,
            "/invocations?scope=cluster: {code} {inv}"
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// `InvocationView` carries `deployment` and `attempt` (the number of consecutive failed
/// attempts pending).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invocations_show_deployment_and_attempt() -> Result<()> {
    let sdk = three_handlers().await?;
    let node = start_led(solo_config(2)).await?;
    let url = node.url();
    let r = within(TEST_DEADLINE, "deployment + attempt", async {
        let dep = common::register(&url, &sdk.url).await?;
        let dep_id = dep["id"].as_str().unwrap_or_default().to_string();
        let (_, sent) = common::send(&url, "Bad", "run", None, "x").await?;
        let bad = sent["id"].as_str().unwrap_or_default().to_string();
        let (_, done) = common::invoke(&url, "Ok", "run", None, "x").await?;
        let ok = done["id"].as_str().unwrap_or_default().to_string();
        let t0 = std::time::Instant::now();
        loop {
            let (code, rows) = get_json(&format!("{url}/invocations?limit=1000")).await?;
            ensure!(code == StatusCode::OK, "{code} {rows}");
            let find = |id: &str| {
                rows.as_array()
                    .and_then(|a| a.iter().find(|r| r["id"] == id))
                    .cloned()
            };
            if let (Some(b), Some(o)) = (find(&bad), find(&ok)) {
                if b["attempt"].as_u64() >= Some(2) {
                    ensure!(b["deployment"] == dep_id.as_str(), "Bad: {b}");
                    ensure!(
                        o["deployment"] == dep_id.as_str() && o["attempt"] == 0,
                        "Ok: {o}"
                    );
                    return Ok(());
                }
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "attempt has not reached 2: {rows}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    node.shutdown().await;
    r
}

/// `/invocations?scope=cluster` asks one node and sees the invocations of every
/// range, each row carrying `node`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invocations_cluster_scope_covers_every_range() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object("Counter", "get", vec![Step::output("ok")])
        .spawn()
        .await?;
    let (_bucket, nodes) = two_nodes(None).await?;
    let r = within(Duration::from_secs(60), "/invocations, cluster", async {
        let ids = one_per_partition(&nodes, &sdk).await?;
        let (code, body) = get_json(&format!(
            "{}/invocations?scope=cluster",
            nodes[1].node.url()
        ))
        .await?;
        ensure!(code == StatusCode::OK, "{code} {body}");
        ensure!(body["partial"] == false, "{body}");
        let items = body["invocations"].as_array().cloned().unwrap_or_default();
        for id in &ids {
            ensure!(
                items.iter().any(|r| r["id"] == id.as_str()),
                "missing {id}: {body}"
            );
        }
        let seen: BTreeSet<String> = items
            .iter()
            .filter_map(|r| r["node"].as_str().map(String::from))
            .collect();
        ensure!(seen.len() == 2, "rows must carry both nodes: {body}");
        // The local form is unchanged: an array.
        let (_, local) = get_json(&format!("{}/invocations", nodes[1].node.url())).await?;
        ensure!(local.is_array(), "{local}");
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **One transiently failing read-fence GET does not drop a range from the answer**
/// (found in a 4 h soak).
///
/// The read fence issues a GET of its own incarnation's session record before the
/// leader answers. Formerly that GET failing once (a 5xx left after the client's retry
/// loop — the `s3-standard-faults*` profiles inject ~0.3 %) made the fence say "no", the
/// answer became `LeaseLost`, and `/invocations?scope=cluster` treated the range as led by
/// another node: silently dropped, `partial`. One query needs one fence per range
/// (211 ranges at the end of the soak), so nearly half the queries missed a range and the
/// drain phase never saw two complete answers in a row.
///
/// 1. Two nodes, each leading two of four ranges, one invocation per range.
/// 2. `node-a`'s next GET of its own session record fails once.
/// 3. Ask `/invocations?scope=cluster` on `node-b` (`node-a`'s share goes through the
///    fan-out); repeat step 2 then read `GET /invocations/{id}` on `node-a`.
///
/// Must see: the list is complete (`partial = false`, no `missingRanges`, every id
/// present), and the status read returns `200` — the fence retries a transiently failed GET
/// within its limit instead of refusing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transient_read_fence_failure_does_not_drop_a_range_from_the_answer() -> Result<()> {
    use cairn_store::testkit::{Action, OpSet, PathMatch, Rule};
    let sdk = FakeSdk::builder()
        .object("Counter", "get", vec![Step::output("ok")])
        .spawn()
        .await?;
    let (_bucket, nodes) = two_nodes(None).await?;
    let fail_next_fence = || {
        nodes[0].store.add_rule(Rule::new(
            OpSet::GET,
            PathMatch::Contains("sessions/node-a/".into()),
            Action::Fail { times: Some(1) },
        ))
    };
    let r = within(Duration::from_secs(60), "read fence fails once", async {
        let ids = one_per_partition(&nodes, &sdk).await?;
        fail_next_fence();
        let (code, body) = get_json(&format!(
            "{}/invocations?scope=cluster",
            nodes[1].node.url()
        ))
        .await?;
        ensure!(code == StatusCode::OK, "{code} {body}");
        ensure!(
            body["partial"] == false,
            "one failed fence GET dropped a range: {body}"
        );
        let items = body["invocations"].as_array().cloned().unwrap_or_default();
        for id in &ids {
            ensure!(
                items.iter().any(|r| r["id"] == id.as_str()),
                "missing {id}: {body}"
            );
        }
        // One external read at the range's own leader.
        let url = nodes[0].node.url();
        let mut mine = None;
        for id in &ids {
            if common::invocation(&url, id).await?.0 == 200 {
                mine = Some(id.clone());
                break;
            }
        }
        let id = mine.ok_or_else(|| anyhow!("node-a leads no range"))?;
        fail_next_fence();
        let (code, st) = common::invocation(&url, &id).await?;
        ensure!(
            code == 200 && st["status"] == "completed",
            "status read while a fence GET fails once: {code} {st}"
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **A node leading many ranges still returns its share within the fan-out limit**
/// (found in a soak run).
///
/// `local_invocations` asks each range's engine **in turn**, each answer coming after a
/// read fence (a session-record GET) and a scan. In the soak one node
/// held ~193 ranges: its share took longer than the 5 s fan-out limit, and `/invocations?scope=cluster`
/// asked on another node always listed that node in `missing` — the drain phase never saw
/// a complete list.
///
/// 1. `node-a` starts alone with 32 ranges and leads all 32; `node-b` joins later
///    (the balancer yields ~1 range every 12 s, so `node-a` still holds nearly all).
/// 2. Every GET by `node-a` of its own session record is delayed 250 ms — each
///    read fence of an answer (sequentially: 32 × 250 ms = 8 s).
/// 3. Ask `/invocations?scope=cluster` on `node-b`.
///
/// Must see: `node-a` is not in `missing`, `partial = false`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_leading_many_ranges_answers_within_the_fan_out_bound() -> Result<()> {
    use cairn_store::testkit::{Action, OpSet, PathMatch, Rule};
    const RANGES: u16 = 32;
    // A long TTL: the read fence's per-attempt limit is TTL/6 — it must exceed the injected delay.
    let ttl = Duration::from_secs(6);
    let bucket = common::bucket();
    let a = start_cluster_node_tweaked(&bucket, "node-a", RANGES, ttl, |_| {}).await?;
    let r = within(
        Duration::from_secs(90),
        "a node with many ranges returns its share",
        async {
            wait_probe(
                &[&a],
                Duration::from_secs(30),
                "node-a leads every range",
                |p| single_leaders(p).is_some_and(|m| m.len() == usize::from(RANGES)),
            )
            .await?;
            let b = start_cluster_node_tweaked(&bucket, "node-b", RANGES, ttl, |_| {}).await?;
            let r = async {
                // `node-b` needs `node-a`'s address (routing table) before asking.
                within(Duration::from_secs(20), "node-b sees node-a", async {
                    loop {
                        let (code, body) =
                            get_json(&format!("{}/invocations?scope=cluster", b.node.url()))
                                .await?;
                        if code == StatusCode::OK
                            && body["nodes"].as_array().is_some_and(|n| n.len() == 2)
                        {
                            return Ok(());
                        }
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                })
                .await?;
                a.store.add_rule(Rule::new(
                    OpSet::GET,
                    PathMatch::Contains("sessions/node-a/".into()),
                    Action::Delay(Duration::from_millis(250)),
                ));
                let t0 = std::time::Instant::now();
                let (code, body) =
                    get_json(&format!("{}/invocations?scope=cluster", b.node.url())).await?;
                ensure!(code == StatusCode::OK, "{code} {body}");
                ensure!(
                    body["missing"].as_array().is_some_and(Vec::is_empty)
                        && body["partial"] == false,
                    "node-a's share did not come back within the fan-out limit ({:?}): {body}",
                    t0.elapsed()
                );
                Ok(())
            }
            .await;
            b.node.shutdown().await;
            r
        },
    )
    .await;
    a.node.shutdown().await;
    r
}

/// The value of an unlabelled counter in `/metrics`.
fn metric(text: &str, name: &str) -> Option<u64> {
    text.lines()
        .find_map(|l| l.strip_prefix(name).and_then(|rest| rest.strip_prefix(' ')))
        .and_then(|v| v.trim().parse().ok())
}

/// Slow log: a turn over the tenant's threshold ⇒ one slow-log line; a fast turn does not.
/// The threshold is set per tenant with `PUT /admin/tenants/{tenant} {"slowMs"}`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_turn_is_logged_with_tenant_threshold() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Slow", "run", vec![Step::Stall(400), Step::output("ok")])
        .service("Fast", "run", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = start_led(solo_config(2)).await?;
    let url = node.url();
    let r = within(TEST_DEADLINE, "slow log", async {
        common::register(&url, &sdk.url).await?;
        let r = Client::new()
            .put(format!("{url}/admin/tenants/default"))
            .json(&serde_json::json!({ "slowMs": 200 }))
            .send()
            .await?;
        ensure!(
            r.status() == StatusCode::OK,
            "set threshold: {} {}",
            r.status(),
            r.text().await?
        );
        let (_, listed) = get_json(&format!("{url}/admin/tenants")).await?;
        ensure!(
            listed["slowMs"]["default"] == 200,
            "/admin/tenants: {listed}"
        );

        let read = || async {
            let text = reqwest::get(format!("{url}/metrics")).await?.text().await?;
            Ok::<_, anyhow::Error>((
                metric(&text, "cairnflow_slow_log_total"),
                metric(&text, "cairnflow_slow_log_dropped_total"),
            ))
        };
        let (before, dropped) = read().await?;
        ensure!(
            before.is_some() && dropped.is_some(),
            "missing slow log metric"
        );
        let before = before.unwrap_or(0);
        for _ in 0..3 {
            common::invoke(&url, "Fast", "run", None, "x").await?;
        }
        ensure!(
            read().await?.0 == Some(before),
            "a fast turn was slow-logged"
        );
        let (code, body) = common::invoke(&url, "Slow", "run", None, "x").await?;
        ensure!(code == 200, "Slow/run: {code} {body}");
        ensure!(
            read().await?.0 == Some(before + 1),
            "a slow turn did not enter the slow log"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// Capture: `POST /admin/capture` ⇒ the **next** matching invocation (only `count` of them)
/// leaves a bundle `captures/<tenant>/<id>`, readable back through the API.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capture_on_next_run_writes_one_bundle() -> Result<()> {
    let sdk = three_handlers().await?;
    let node = start_led(solo_config(2)).await?;
    let url = node.url();
    let r = within(TEST_DEADLINE, "capture the next run", async {
        let dep = common::register(&url, &sdk.url).await?;
        let dep_id = dep["id"].as_str().unwrap_or_default().to_string();
        let http = Client::new();
        // Ran before the capture was set: not captured.
        let (_, before) = common::invoke(&url, "Ok", "run", None, "x").await?;
        let r = http
            .post(format!("{url}/admin/capture"))
            .json(&serde_json::json!({
                "tenant": "default", "service": "Ok", "handler": "run", "count": 1, "ttl": 600
            }))
            .send()
            .await?;
        ensure!(r.status() == StatusCode::OK, "set capture: {} {}", r.status(), r.text().await?);
        let spec: Value = r.json().await?;
        ensure!(spec["remaining"] == 1 && spec["id"].is_string(), "{spec}");
        let (code, body) = http
            .post(format!("{url}/admin/capture"))
            .json(&serde_json::json!({ "tenant": "default", "service": "Ok", "handler": "run", "count": 0, "ttl": 600 }))
            .send()
            .await
            .map(|r| (r.status(), r))?;
        ensure!(code == StatusCode::BAD_REQUEST, "count=0 must be 400: {code} {:?}", body.text().await);

        let (_, first) = common::invoke(&url, "Ok", "run", None, "x").await?;
        let (_, second) = common::invoke(&url, "Ok", "run", None, "x").await?;
        let first = first["id"].as_str().unwrap_or_default().to_string();
        let listing = poll(&format!("{url}/admin/capture?tenant=default"), "one bundle", |b| {
            b["bundles"].as_array().is_some_and(|a| !a.is_empty())
        })
        .await?;
        let bundles = listing["bundles"].as_array().cloned().unwrap_or_default();
        ensure!(bundles.len() == 1 && bundles[0] == first.as_str(), "only the next invocation: {listing}");
        ensure!(
            !bundles.iter().any(|b| *b == before["id"] || *b == second["id"]),
            "wrong capture: {listing}"
        );
        ensure!(
            listing["captures"].as_array().is_some_and(|a| a.is_empty()),
            "a used-up capture must disappear: {listing}"
        );

        let raw = http
            .get(format!("{url}/admin/capture/default/{first}"))
            .send()
            .await?;
        ensure!(raw.status() == StatusCode::OK, "download bundle: {}", raw.status());
        let bytes = raw.bytes().await?;
        ensure!(bytes.starts_with(b"cairnflow-capture/1\n"), "wrong bundle header");
        ensure!(
            bytes.windows(dep_id.len()).any(|w| w == dep_id.as_bytes()),
            "bundle does not carry deployment {dep_id}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
