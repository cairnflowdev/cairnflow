// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Forwarding to the leader: a request that asks for `x-cairn-forward: leader` and
//! reaches a node that does not lead the range runs on the leader, through the
//! internal port.
//!
//! Built only with `--features testkit` (the network filter of `tests/common/net.rs`).
//! Every test opens with the interleaving it reproduces (AGENTS.md §6).

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    cairn_server::auth::AuthConfig,
    common::{
        fake_sdk::{FakeSdk, Step, HOLD_SIGNAL},
        net::{net_cluster_tweaked, PathFilter, Verdict},
        single_leaders, wait_probe, ClusterNode,
    },
    reqwest::{header, Client, StatusCode},
    serde_json::{json, Value},
    std::time::{Duration, Instant},
};

const TTL: Duration = Duration::from_secs(2);
const BOOT: &str = "cf_test_forward_bootstrap_key_0123456789ab";
const SECRET: &str = "cf_test_cluster_secret_forward_32b";
const FORWARD: (&str, &str) = ("x-cairn-forward", "leader");
const BY: &str = "x-cairn-forwarded-by";

/// Cap on waiting for the cluster to balance — like `net_partition.rs::balance_deadline`.
fn balance_deadline(ttl: Duration) -> Duration {
    cairn_server::balance::settle_for(ttl) + ttl * 8 + Duration::from_secs(10)
}

/// Two nodes, one range each — returns alpha's range and beta's.
async fn balanced_two(nodes: &[&ClusterNode]) -> Result<(u32, u32)> {
    let probe = wait_probe(
        nodes,
        balance_deadline(TTL),
        "one range on each node",
        |p| single_leaders(p).is_some_and(|m| common::load_of(&m, 2) == [1, 1]),
    )
    .await?;
    let leaders = single_leaders(&probe).expect("balanced");
    let of = |n: usize| {
        *leaders
            .iter()
            .find(|(_, i)| **i == n)
            .expect("node holds one range")
            .0
    };
    Ok((of(0), of(1)))
}

/// `POST` with optional headers; returns (code, JSON, `x-cairn-forwarded-by`).
async fn post(
    url: String,
    headers: &[(&str, &str)],
    body: Value,
) -> Result<(StatusCode, Value, Option<String>)> {
    let mut rb = Client::new().post(url).json(&body);
    for (k, v) in headers {
        rb = rb.header(*k, *v);
    }
    let r = rb.send().await?;
    let code = r.status();
    let by = r
        .headers()
        .get(BY)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let v = r.json::<Value>().await.unwrap_or(Value::Null);
    Ok((code, v, by))
}

async fn register(node: &ClusterNode, sdk: &str, bearer: Option<&str>) -> Result<()> {
    let mut rb = Client::new()
        .post(format!("{}/deployments", node.node.url()))
        .json(&json!({ "uri": sdk }));
    if let Some(k) = bearer {
        rb = rb.bearer_auth(k);
    }
    let r = rb.send().await?;
    ensure!(r.status().is_success(), "register: {}", r.status());
    Ok(())
}

/// Waits for `id` to finish with `want`, asking every node.
async fn wait_completed(nodes: &[&ClusterNode], id: &str, want: &str) -> Result<()> {
    wait_completed_as(nodes, id, want, None).await
}

/// Like [`wait_completed`], asking with the `bearer` key (a cluster with auth).
async fn wait_completed_as(
    nodes: &[&ClusterNode],
    id: &str,
    want: &str,
    bearer: Option<&str>,
) -> Result<()> {
    let t0 = Instant::now();
    loop {
        for n in nodes {
            let mut rb = Client::new().get(format!("{}/invocations/{id}", n.node.url()));
            if let Some(k) = bearer {
                rb = rb.bearer_auth(k);
            }
            let r = rb.send().await?;
            let code = r.status().as_u16();
            let st: Value = r.json().await.unwrap_or(Value::Null);
            if code == 200 && st["status"] == "completed" {
                ensure!(st["output"] == want, "{id}: {st}");
                return Ok(());
            }
        }
        ensure!(t0.elapsed() < Duration::from_secs(20), "{id} not finished");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Waits for `id` to be suspended (`suspended`) on its leader.
async fn wait_suspended(nodes: &[&ClusterNode], id: &str) -> Result<()> {
    let t0 = Instant::now();
    loop {
        for n in nodes {
            let (code, st) = common::invocation(&n.node.url(), id).await?;
            if code == 200 && st["status"] == "suspended" {
                return Ok(());
            }
        }
        ensure!(t0.elapsed() < Duration::from_secs(20), "{id} not suspended");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn id_of(v: &Value) -> Result<String> {
    v["id"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("no id: {v}"))
}

/// **A request asking for forwarding runs on the leader, from a non-leading node.**
///
/// 1. Cluster alpha + beta, one range each; service `Hold/run` suspends waiting for a
///    signal.
/// 2. Send `send` for a key in beta's range **to alpha**: without asking ⇒ `503` with
///    `leader` as before; asking ⇒ `202`, `x-cairn-forwarded-by: beta`.
/// 3. Signal that invocation through alpha, asking ⇒ beta runs it, the invocation finishes.
/// 4. `batch/cancel` to alpha with one id per range ⇒ the id on alpha runs locally, the
///    id on beta runs on beta (a sub-batch), both `ok`, in order.
///
/// Must see: every call that asks succeeds from alpha; without asking the
/// `503 + leader` contract is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwarded_request_runs_on_leader() -> Result<()> {
    let (_bucket, _net, nodes) =
        net_cluster_tweaked(&["alpha", "beta"], 2, TTL, false, |_| {}).await?;
    let refs: Vec<&ClusterNode> = nodes.iter().collect();
    let r = common::within(Duration::from_secs(120), "forwarding", async {
        let (pa, pb) = balanced_two(&refs).await?;
        let sdk = FakeSdk::builder()
            .object("Hold", "run", vec![Step::Suspend, Step::output("done")])
            .spawn()
            .await?;
        for n in &nodes {
            register(n, &sdk.url, None).await?;
        }
        let alpha = nodes[0].node.url();
        let key_b = common::key_on_partition(&nodes[0].node, "Hold", pb, "b");
        let send_url = format!("{alpha}/invoke/Hold/run/send?key={key_b}");

        let (code, body, by) = post(send_url.clone(), &[], json!("x")).await?;
        ensure!(
            code == StatusCode::SERVICE_UNAVAILABLE && body["leader"].is_string() && by.is_none(),
            "without asking: must be 503 + leader as before, got {code} {body} {by:?}"
        );
        let (code, body, by) = post(send_url, &[FORWARD], json!("x")).await?;
        ensure!(
            code.is_success() && by.as_deref() == Some("beta"),
            "asking for forwarding: {code} {body} {by:?}"
        );
        let id = id_of(&body)?;
        wait_suspended(&refs, &id).await?;

        let (code, body, by) = post(
            format!("{alpha}/invocations/{id}/signal"),
            &[FORWARD],
            json!({ "signal": HOLD_SIGNAL }),
        )
        .await?;
        ensure!(
            code.is_success() && by.as_deref() == Some("beta"),
            "signal through forwarding: {code} {body} {by:?}"
        );
        wait_completed(&refs, &id, "done").await?;

        // A batch of two ids, one per range.
        let key_a = common::key_on_partition(&nodes[0].node, "Hold", pa, "a");
        let mut ids = Vec::new();
        for (node, key) in [(&nodes[0], &key_a), (&nodes[1], &key_b)] {
            let (code, body, _) = post(
                format!("{}/invoke/Hold/run/send?key={key}-2", node.node.url()),
                &[FORWARD],
                json!("x"),
            )
            .await?;
            ensure!(code.is_success(), "{code} {body}");
            ids.push(id_of(&body)?);
        }
        for id in &ids {
            wait_suspended(&refs, id).await?;
        }
        let (code, body, by) = post(
            format!("{alpha}/invocations/batch/cancel"),
            &[FORWARD],
            json!({ "ids": ids }),
        )
        .await?;
        ensure!(
            code == StatusCode::OK && by.is_none(),
            "{code} {body} {by:?}"
        );
        let results = body["results"]
            .as_array()
            .ok_or_else(|| anyhow!("{body}"))?;
        ensure!(results.len() == 2, "{body}");
        for (r, id) in results.iter().zip(&ids) {
            ensure!(r["id"] == id.as_str() && r["ok"] == true, "{body}");
        }
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **`--advertise-host`: bind every address, advertise a private one** (e.g. on a PaaS
/// private network: bind `0.0.0.0`/`::`, advertise the private IP).
///
/// 1. Cluster alpha + beta, each node binds `0.0.0.0` (port chosen by the OS) and
///    advertises `127.0.0.1`. The client only calls through `127.0.0.1:<real port>`.
/// 2. Wait for one range per node (balancing goes through node-to-node messages — to
///    the advertised address), register the SDK on both.
/// 3. Read the peer session on alpha; send `send` for a key in beta's range to alpha,
///    first without and then with a forwarding request.
///
/// Must see: beta's internal port address in the session is
/// `http://127.0.0.1:<beta's real internal port>` (not `0.0.0.0`); the `503` without
/// asking carries `leader` = `http://127.0.0.1:<beta's real ingress port>`; asking ⇒
/// success on beta (`x-cairn-forwarded-by: beta`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn advertise_host_is_what_peers_and_clients_see() -> Result<()> {
    let (_bucket, _net, nodes) = net_cluster_tweaked(&["alpha", "beta"], 2, TTL, false, |cfg| {
        cfg.host = "0.0.0.0".into();
        cfg.advertise_host = Some("127.0.0.1".into());
    })
    .await?;
    let refs: Vec<&ClusterNode> = nodes.iter().collect();
    let local = |n: &ClusterNode| format!("http://127.0.0.1:{}", n.node.addr.port());
    let r = common::within(Duration::from_secs(120), "advertise-host", async {
        for n in &nodes {
            ensure!(
                n.node.addr.ip().is_unspecified() && n.node.internal_addr.ip().is_unspecified(),
                "node must bind 0.0.0.0: {} {}",
                n.node.addr,
                n.node.internal_addr
            );
        }
        let (_, pb) = balanced_two(&refs).await?;
        let beta_internal = format!("http://127.0.0.1:{}", nodes[1].node.internal_addr.port());
        let peers = nodes[0].node.session.membership().peer_addresses();
        ensure!(
            peers == vec![beta_internal.clone()],
            "alpha sees peers {peers:?}, waiting for [{beta_internal}]"
        );
        let sdk = FakeSdk::builder()
            .object("Hold", "run", vec![Step::output("done")])
            .spawn()
            .await?;
        for n in &nodes {
            let r = Client::new()
                .post(format!("{}/deployments", local(n)))
                .json(&json!({ "uri": sdk.url }))
                .send()
                .await?;
            ensure!(r.status().is_success(), "register: {}", r.status());
        }
        let key_b = common::key_on_partition(&nodes[0].node, "Hold", pb, "b");
        let send_url = format!("{}/invoke/Hold/run/send?key={key_b}", local(&nodes[0]));
        let (code, body, by) = post(send_url.clone(), &[], json!("x")).await?;
        let beta_ingress = format!("http://127.0.0.1:{}", nodes[1].node.addr.port());
        ensure!(
            code == StatusCode::SERVICE_UNAVAILABLE
                && body["leader"].as_str().map(|l| l.trim_end_matches('/'))
                    == Some(beta_ingress.as_str())
                && by.is_none(),
            "without asking: expected 503 + leader {beta_ingress}, got {code} {body} {by:?}"
        );
        let (code, body, by) = post(send_url, &[FORWARD], json!("x")).await?;
        ensure!(
            code.is_success() && by.as_deref() == Some("beta"),
            "forwarding through the advertised address: {code} {body} {by:?}"
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **An unreachable leader: an ambiguous `504`, no hang, no silent `503`.**
///
/// 1. Cluster alpha + beta, one range each.
/// 2. The network filter drops every `/internal/forward` from alpha to beta (like a
///    network cut after sending: the sender sees a network error at once).
/// 3. Send a `send` that asks for forwarding, for a key in beta's range, to alpha.
///
/// Must see: a `504` within a few seconds, the body saying "may have run"; with the
/// filter removed, sending again (same idempotency key) succeeds and yields the
/// **same** id if the earlier attempt got through — never two invocations.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unreachable_leader_is_ambiguous_not_hung() -> Result<()> {
    let (_bucket, net, nodes) =
        net_cluster_tweaked(&["alpha", "beta"], 2, TTL, false, |_| {}).await?;
    let refs: Vec<&ClusterNode> = nodes.iter().collect();
    let r = common::within(Duration::from_secs(120), "leader unreachable", async {
        let (_, pb) = balanced_two(&refs).await?;
        let sdk = FakeSdk::builder()
            .object("Hold", "run", vec![Step::output("done")])
            .spawn()
            .await?;
        for n in &nodes {
            register(n, &sdk.url, None).await?;
        }
        let alpha = nodes[0].node.url();
        let key_b = common::key_on_partition(&nodes[0].node, "Hold", pb, "b");
        let url = format!("{alpha}/invoke/Hold/run/send?key={key_b}");
        let idem = ("idempotency-key", "forward-504");

        let cut = net.add(
            PathFilter::new("/internal/forward")
                .from("alpha")
                .to("beta")
                .verdict(Verdict::Drop),
        );
        let t0 = Instant::now();
        let (code, body, by) = post(url.clone(), &[FORWARD, idem], json!("x")).await?;
        ensure!(
            code == StatusCode::GATEWAY_TIMEOUT && by.is_none(),
            "{code} {body} {by:?}"
        );
        ensure!(
            t0.elapsed() < Duration::from_secs(10),
            "waiting {:?}",
            t0.elapsed()
        );
        net.remove(cut);
        let (code, body, by) = post(url, &[FORWARD, idem], json!("x")).await?;
        ensure!(
            code.is_success() && by.as_deref() == Some("beta"),
            "{code} {body} {by:?}"
        );
        wait_completed(&refs, &id_of(&body)?, "done").await
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **The leader re-authenticates from the original headers** (console session cookie,
/// origin).
///
/// 1. Cluster alpha + beta with keys (`BOOT`, a shared cluster secret); log into the
///    console on alpha to get a session cookie.
/// 2. Send a `send` that asks for forwarding, for a key in beta's range, to alpha, with
///    alpha's cookie + `Origin` ⇒ beta accepts the cookie (same cluster secret) and an
///    `Origin` matching the original `Host` (alpha).
/// 3. A cookie with one byte changed ⇒ `401` on alpha, goes nowhere. A correct cookie
///    with a foreign `Origin` ⇒ `403` on alpha.
///
/// Must see: (2) succeeds, `x-cairn-forwarded-by: beta`; (3) has no trace of
/// forwarding.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_reauthenticates_forwarded_request() -> Result<()> {
    let (_bucket, _net, nodes) = net_cluster_tweaked(&["alpha", "beta"], 2, TTL, false, |cfg| {
        cfg.auth = AuthConfig {
            required: true,
            admin_key: Some(BOOT.into()),
            cluster_secret: Some(SECRET.into()),
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
    })
    .await?;
    let refs: Vec<&ClusterNode> = nodes.iter().collect();
    let r = common::within(Duration::from_secs(120), "re-authentication", async {
        let (_, pb) = balanced_two(&refs).await?;
        let sdk = FakeSdk::builder()
            .object("Hold", "run", vec![Step::output("done")])
            .spawn()
            .await?;
        for n in &nodes {
            register(n, &sdk.url, Some(BOOT)).await?;
        }
        let alpha = nodes[0].node.url();
        let login = Client::new()
            .post(format!("{alpha}/ui/session"))
            .header(header::ORIGIN, alpha.to_string())
            .json(&json!({ "key": BOOT }))
            .send()
            .await?;
        ensure!(
            login.status() == StatusCode::OK,
            "login: {}",
            login.status()
        );
        let cookie = login
            .headers()
            .get(header::SET_COOKIE)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.split(';').next())
            .ok_or_else(|| anyhow!("no Set-Cookie"))?
            .to_string();
        let key_b = common::key_on_partition(&nodes[0].node, "Hold", pb, "b");
        let url = format!("{alpha}/invoke/Hold/run/send?key={key_b}");
        let origin = alpha.to_string();

        let (code, body, by) = post(
            url.clone(),
            &[FORWARD, ("cookie", &cookie), ("origin", &origin)],
            json!("x"),
        )
        .await?;
        ensure!(
            code.is_success() && by.as_deref() == Some("beta"),
            "cookie through forwarding: {code} {body} {by:?}"
        );
        wait_completed_as(&refs, &id_of(&body)?, "done", Some(BOOT)).await?;

        let mut bad = cookie.clone().into_bytes();
        let last = bad.len() - 1;
        bad[last] = if bad[last] == b'0' { b'1' } else { b'0' };
        let bad = String::from_utf8(bad)?;
        let (code, _, by) = post(
            url.clone(),
            &[FORWARD, ("cookie", &bad), ("origin", &origin)],
            json!("x"),
        )
        .await?;
        ensure!(
            code == StatusCode::UNAUTHORIZED && by.is_none(),
            "broken cookie: {code} {by:?}"
        );
        let (code, _, by) = post(
            url,
            &[
                FORWARD,
                ("cookie", &cookie),
                ("origin", "http://127.0.0.1:1"),
            ],
            json!("x"),
        )
        .await?;
        ensure!(
            code == StatusCode::FORBIDDEN && by.is_none(),
            "foreign origin: {code} {by:?}"
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **SSE goes through forwarding as a stream.**
///
/// 1. Cluster alpha + beta, SSE `:keepalive` every 200 ms; an invocation in beta's
///    range suspends waiting for a signal (its `$log` stream open, no record yet).
/// 2. `GET /invocations/{id}/logs?follow=1` to alpha: without asking ⇒ `503` +
///    `leader`; asking ⇒ alpha opens the SSE on beta through `/internal/forward` and
///    copies the body.
///
/// Must see: the head `200 text/event-stream` with `x-cairn-forwarded-by: beta`
/// comes back **at once** (without waiting for the body), then the first body chunk
/// (beta's keepalive) arrives through alpha while the connection is still open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sse_follows_through_forwarding() -> Result<()> {
    let (_bucket, _net, nodes) = net_cluster_tweaked(&["alpha", "beta"], 2, TTL, false, |cfg| {
        cfg.streams.keepalive = Duration::from_millis(200);
    })
    .await?;
    let refs: Vec<&ClusterNode> = nodes.iter().collect();
    let r = common::within(Duration::from_secs(120), "SSE through forwarding", async {
        let (_, pb) = balanced_two(&refs).await?;
        let sdk = FakeSdk::builder()
            .object("Hold", "run", vec![Step::Suspend, Step::output("done")])
            .spawn()
            .await?;
        for n in &nodes {
            register(n, &sdk.url, None).await?;
        }
        let alpha = nodes[0].node.url();
        let key_b = common::key_on_partition(&nodes[0].node, "Hold", pb, "b");
        let (code, body, _) = post(
            format!("{alpha}/invoke/Hold/run/send?key={key_b}"),
            &[FORWARD],
            json!("x"),
        )
        .await?;
        ensure!(code.is_success(), "{code} {body}");
        let id = id_of(&body)?;
        wait_suspended(&refs, &id).await?;

        let url = format!("{alpha}/invocations/{id}/logs?follow=1");
        let plain = Client::new().get(&url).send().await?;
        ensure!(
            plain.status() == StatusCode::SERVICE_UNAVAILABLE,
            "without asking: {}",
            plain.status()
        );
        let mut resp = tokio::time::timeout(
            Duration::from_secs(5),
            Client::new().get(&url).header(FORWARD.0, FORWARD.1).send(),
        )
        .await??;
        ensure!(resp.status() == StatusCode::OK, "{}", resp.status());
        ensure!(
            resp.headers().get(BY).and_then(|v| v.to_str().ok()) == Some("beta"),
            "{:?}",
            resp.headers()
        );
        ensure!(
            resp.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.starts_with("text/event-stream")),
            "{:?}",
            resp.headers()
        );
        let chunk = tokio::time::timeout(Duration::from_secs(5), resp.chunk())
            .await??
            .ok_or_else(|| anyhow!("SSE closed before the first chunk"))?;
        ensure!(
            String::from_utf8_lossy(&chunk).contains("keepalive"),
            "first chunk: {chunk:?}"
        );
        drop(resp);
        // Clean up: release the invocation through forwarding.
        let (code, body, _) = post(
            format!("{alpha}/invocations/{id}/signal"),
            &[FORWARD],
            json!({ "signal": HOLD_SIGNAL }),
        )
        .await?;
        ensure!(code.is_success(), "{code} {body}");
        wait_completed(&refs, &id, "done").await
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **`scope=cluster` gathers the part of every node.**
///
/// 1. Cluster alpha + beta, one range each; one suspended invocation in each range.
/// 2. `POST /deployments/{dp}/replay-check?service=Hold` to alpha: without `scope`
///    ⇒ only alpha's range; `scope=cluster` ⇒ alpha runs the same request on beta
///    (through `/internal/forward`) and adds up the reports.
/// 3. Beta splits its range; `GET /ranges/ops` to alpha: without `scope` ⇒ alpha's
///    log (empty); `scope=cluster` ⇒ includes beta's operation.
///
/// Must see: (2) `checked` 1 then 2, `ranges` with both ranges, `partial: false`;
/// (3) beta's split operation shows only with `scope=cluster`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_scope_gathers_every_node() -> Result<()> {
    let (_bucket, _net, nodes) =
        net_cluster_tweaked(&["alpha", "beta"], 2, TTL, false, |_| {}).await?;
    let refs: Vec<&ClusterNode> = nodes.iter().collect();
    let r = common::within(Duration::from_secs(120), "scope=cluster", async {
        let (pa, pb) = balanced_two(&refs).await?;
        let sdk = FakeSdk::builder()
            .object("Hold", "run", vec![Step::Suspend, Step::output("done")])
            .spawn()
            .await?;
        let mut dp = String::new();
        for n in &nodes {
            let r = Client::new()
                .post(format!("{}/deployments", n.node.url()))
                .json(&json!({ "uri": sdk.url }))
                .send()
                .await?;
            ensure!(r.status().is_success(), "register: {}", r.status());
            dp = r.json::<Value>().await?["id"]
                .as_str()
                .ok_or_else(|| anyhow!("no deployment id"))?
                .to_string();
        }
        let alpha = nodes[0].node.url();
        let mut ids = Vec::new();
        for (p, k) in [(pa, "a"), (pb, "b")] {
            let key = common::key_on_partition(&nodes[0].node, "Hold", p, k);
            let (code, body, _) = post(
                format!("{alpha}/invoke/Hold/run/send?key={key}"),
                &[FORWARD],
                json!("x"),
            )
            .await?;
            ensure!(code.is_success(), "{code} {body}");
            ids.push(id_of(&body)?);
        }
        for id in &ids {
            wait_suspended(&refs, id).await?;
        }
        let check = |scope: &'static str| {
            post(
                format!("{alpha}/deployments/{dp}/replay-check?service=Hold{scope}"),
                &[],
                Value::Null,
            )
        };
        let (code, local, _) = check("").await?;
        ensure!(
            code == StatusCode::OK && local["checked"] == 1,
            "{code} {local}"
        );
        let (code, all, _) = check("&scope=cluster").await?;
        ensure!(
            code == StatusCode::OK
                && all["checked"] == 2
                && all["partial"] == false
                && all["ranges"] == json!([pa.min(pb), pa.max(pb)]),
            "{code} {all}"
        );

        let (code, body, _) = post(
            format!("{}/ranges/{pb}/split", nodes[1].node.url()),
            &[],
            Value::Null,
        )
        .await?;
        ensure!(code.is_success(), "split: {code} {body}");
        let ops = |q: &'static str| {
            let url = format!("{alpha}/ranges/ops{q}");
            async move {
                let v: Value = Client::new().get(url).send().await?.json().await?;
                Ok::<_, anyhow::Error>(v)
            }
        };
        let mine = ops("").await?;
        ensure!(mine["ops"] == json!([]), "{mine}");
        // Right after the commit, alpha's routing table may not know yet who leads the
        // two children — beta is not yet "the leading node" to ask. Wait, with a cap.
        let t0 = Instant::now();
        loop {
            let all = ops("?scope=cluster").await?;
            let found = all["ops"].as_array().is_some_and(|o| {
                o.iter()
                    .any(|op| op["kind"] == "split" && op["ranges"][0] == pb)
            });
            if found && all["partial"] == false {
                break;
            }
            ensure!(t0.elapsed() < Duration::from_secs(15), "{all}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        for id in &ids {
            let _ = post(
                format!("{alpha}/invocations/{id}/signal"),
                &[FORWARD],
                json!({ "signal": HOLD_SIGNAL }),
            )
            .await?;
        }
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **`whoami` and `ops-reports` keep the identity through the forwarding layer.**
/// Two routes not bound to a range: the handler never answers "not the leader", so a
/// request that asks for forwarding runs **locally**.
///
/// 1. Cluster alpha + beta with keys; an `admin` key of tenant `acme`.
/// 2. On each node, with and without `x-cairn-forward: leader`: `GET /auth/whoami`
///    with the bootstrap key and the `acme` key; `GET /admin/ops-reports` with the
///    bootstrap key.
///
/// Must see: every answer `200`, no `x-cairn-forwarded-by`; the tenant, role and
/// `bootstrap` of `whoami` are the same on every node, asking or not; the report
/// lists are the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn whoami_and_ops_reports_keep_identity_under_forwarding() -> Result<()> {
    let (_bucket, _net, nodes) = net_cluster_tweaked(&["alpha", "beta"], 2, TTL, false, |cfg| {
        cfg.auth = AuthConfig {
            required: true,
            admin_key: Some(BOOT.into()),
            cluster_secret: Some(SECRET.into()),
            metrics_key: None,
            admin_key_sha256: None,
            provision_key_sha256: None,
        };
    })
    .await?;
    let refs: Vec<&ClusterNode> = nodes.iter().collect();
    let r = common::within(
        Duration::from_secs(120),
        "identity through forwarding",
        async {
            balanced_two(&refs).await?;
            let alpha = nodes[0].node.url();
            let created = Client::new()
                .post(format!("{alpha}/admin/keys"))
                .bearer_auth(BOOT)
                .json(&json!({ "tenant": "acme", "role": "admin" }))
                .send()
                .await?;
            ensure!(
                created.status() == StatusCode::OK,
                "create key: {}",
                created.status()
            );
            let acme = created.json::<Value>().await?["secret"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let get = |url: String, key: String, forward: bool| async move {
                let mut r = Client::new().get(url).bearer_auth(key);
                if forward {
                    r = r.header(FORWARD.0, FORWARD.1);
                }
                let r = r.send().await?;
                let code = r.status();
                let by = r
                    .headers()
                    .get(BY)
                    .map(|v| v.to_str().unwrap_or("?").to_string());
                anyhow::Ok((code, by, r.json::<Value>().await.unwrap_or(Value::Null)))
            };
            // Invariant across nodes: tenant, role, bootstrap of whoami; the whole ops-reports body.
            let shape = |v: &Value| json!([v["tenant"], v["role"], v["bootstrap"]]);
            for (key, path) in [
                (BOOT, "/auth/whoami"),
                (acme.as_str(), "/auth/whoami"),
                (BOOT, "/admin/ops-reports"),
            ] {
                let mut seen = Vec::new();
                for n in &nodes {
                    for forward in [false, true] {
                        let (code, by, v) =
                            get(format!("{}{path}", n.node.url()), key.to_string(), forward)
                                .await?;
                        ensure!(
                            code == StatusCode::OK && by.is_none(),
                            "{path} on {} (ask={forward}): {code} {by:?} {v}",
                            n.node.url()
                        );
                        seen.push(if path == "/auth/whoami" { shape(&v) } else { v });
                    }
                }
                ensure!(
                    seen.windows(2).all(|w| w[0] == w[1]),
                    "{path}: differs between nodes/forwarding: {seen:?}"
                );
            }
            Ok(())
        },
    )
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// The deployment id that `node`'s `GET /services` names as the current revision of
/// `service`.
async fn current_of(node: &ClusterNode, service: &str) -> Result<Option<String>> {
    let v: Value = Client::new()
        .get(format!("{}/services", node.node.url()))
        .send()
        .await?
        .json()
        .await?;
    Ok(v.as_array()
        .and_then(|a| a.iter().find(|s| s["name"] == service))
        .and_then(|s| s["deploymentId"].as_str())
        .map(str::to_string))
}

/// **A node with a stale deployment registry does not pin an invocation to a deployment
/// just removed** (the slow in-RAM registry case).
///
/// 1. Cluster alpha + beta, deployment registry refresh round every 10 minutes (only a
///    notification refreshes it). Register X; wait until beta knows X.
/// 2. Block `/internal/registry/changed` to beta. Register Y on alpha (catalog
///    `Hold → Y`) — beta still has `Hold → X`. `DELETE /deployments/X` on alpha.
/// 3. `send` `Hold` **on beta**.
///
/// Before the fix: `DELETE` returned `204` without asking any peer; beta created an
/// invocation pinned to X — a deployment no longer on the bucket (another node, or
/// beta after a restart, cannot run it).
///
/// Must see: either `DELETE` does not return `204`, or the invocation created on beta
/// is pinned to Y.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_peer_does_not_pin_a_removed_deployment() -> Result<()> {
    use std::sync::{atomic::AtomicBool, Arc};
    let (_bucket, net, nodes) = net_cluster_tweaked(&["alpha", "beta"], 2, TTL, false, |cfg| {
        cfg.deploy_refresh = Duration::from_secs(600);
    })
    .await?;
    let refs: Vec<&ClusterNode> = nodes.iter().collect();
    let r = common::within(Duration::from_secs(120), "slow in-RAM registry", async {
        balanced_two(&refs).await?;
        let sdk = FakeSdk::builder()
            .service("Hold", "run", vec![Step::output("done")])
            .spawn()
            .await?;
        register(&nodes[0], &sdk.url, None).await?;
        let x = current_of(&nodes[0], "Hold")
            .await?
            .ok_or_else(|| anyhow!("no X"))?;
        let t0 = Instant::now();
        while current_of(&nodes[1], "Hold").await?.as_deref() != Some(x.as_str()) {
            ensure!(t0.elapsed() < Duration::from_secs(20), "X unknown to beta");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let cut = Arc::new(AtomicBool::new(true));
        net.add(
            PathFilter::new("/internal/registry/changed")
                .to("beta")
                .when(cut.clone()),
        );
        register(&nodes[0], &sdk.url, None).await?;
        let y = current_of(&nodes[0], "Hold")
            .await?
            .ok_or_else(|| anyhow!("no Y"))?;
        ensure!(x != y, "two deployments");
        // A `503` (a range has not answered the pin count yet) is temporary: resend, with a cap.
        let t0 = Instant::now();
        let deleted = loop {
            let del = Client::new()
                .delete(format!("{}/deployments/{x}", nodes[0].node.url()))
                .send()
                .await?;
            let code = del.status();
            if code != StatusCode::SERVICE_UNAVAILABLE || t0.elapsed() > Duration::from_secs(10) {
                break code;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        };
        if deleted != StatusCode::NO_CONTENT {
            return Ok(());
        }
        // The range may be changing owner right then (a `503` runs nothing): resend.
        let t0 = Instant::now();
        let (code, body) = loop {
            let (code, body, _) = post(
                format!("{}/invoke/Hold/run/send", nodes[1].node.url()),
                &[FORWARD],
                json!("x"),
            )
            .await?;
            if code != StatusCode::SERVICE_UNAVAILABLE || t0.elapsed() > Duration::from_secs(10) {
                break (code, body);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        };
        ensure!(code.is_success(), "send on beta: {code} {body}");
        let id = id_of(&body)?;
        let sql = format!("SELECT deployment FROM sys_invocation WHERE id = '{id}'");
        let t0 = Instant::now();
        let pinned = loop {
            let v: Value = Client::new()
                .post(format!("{}/query", nodes[0].node.url()))
                .json(&json!({ "sql": sql, "allow_partial": true }))
                .send()
                .await?
                .json()
                .await?;
            if let Some(d) = v["rows"][0][0].as_str() {
                break d.to_string();
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "invocation not found: {v}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        ensure!(
            pinned == y,
            "beta pinned the invocation to {pinned} (removed: {x}), not {y}"
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **A write command that goes through forwarding carries the client IP into the
/// audit on the leader.**
///
/// 1. Cluster alpha + beta; `Hold/run` suspended in beta's range, created through alpha
///    with a forwarding request.
/// 2. `cancel` it **to alpha** with a forwarding request ⇒ beta runs it and writes audit.
///
/// Before the fix: `decode` built the request without `ConnectInfo` ⇒ the audit row
/// on the leader had `ip: null`. Must see: the `invocation.cancel` row carries
/// `ip = 127.0.0.1` (the test's client).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwarded_mutation_keeps_client_ip_in_audit() -> Result<()> {
    let (_bucket, _net, nodes) =
        net_cluster_tweaked(&["alpha", "beta"], 2, TTL, false, |_| {}).await?;
    let refs: Vec<&ClusterNode> = nodes.iter().collect();
    let r = common::within(Duration::from_secs(120), "audit via forwarding", async {
        let (_, pb) = balanced_two(&refs).await?;
        let sdk = FakeSdk::builder()
            .object("Hold", "run", vec![Step::Suspend, Step::output("done")])
            .spawn()
            .await?;
        for n in &nodes {
            register(n, &sdk.url, None).await?;
        }
        let alpha = nodes[0].node.url();
        let key_b = common::key_on_partition(&nodes[0].node, "Hold", pb, "audit");
        let (code, body, _) = post(
            format!("{alpha}/invoke/Hold/run/send?key={key_b}"),
            &[FORWARD],
            json!("x"),
        )
        .await?;
        ensure!(code.is_success(), "{code} {body}");
        let id = id_of(&body)?;
        wait_suspended(&refs, &id).await?;
        let (code, body, by) = post(
            format!("{alpha}/invocations/{id}/cancel"),
            &[FORWARD],
            json!({}),
        )
        .await?;
        ensure!(
            code.is_success() && by.as_deref() == Some("beta"),
            "cancel through forwarding: {code} {body} {by:?}"
        );
        let t0 = Instant::now();
        loop {
            let v: Value = Client::new()
                .get(format!("{alpha}/audit"))
                .send()
                .await?
                .json()
                .await?;
            let row = v["records"].as_array().and_then(|rs| {
                rs.iter()
                    .find(|r| r["action"] == "invocation.cancel" && r["kind"] != "intent")
                    .cloned()
            });
            if let Some(row) = row {
                ensure!(row["ip"] == "127.0.0.1", "audit row on the leader: {row}");
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "audit row not found: {v}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}
