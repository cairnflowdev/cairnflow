// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Egress with a policy: the engine never opens a connection to an internal address
//! on a tenant's behalf — not through an IP literal, DNS rebinding, proxy env or a
//! redirect — while `--egress-deny-private` is on; the cluster's internal paths
//! (forwarding to the leader) still work.
//!
//! The tests run on **real sockets**: layer 2 (the resolver) and layer 3 (the socket's
//! peer) do not exist in the deterministic simulation; layer 1 (the URL) is also
//! covered in `sim_cluster.rs`. The "internal destination" in these tests is a
//! listener on `127.0.0.1` counting the TCP connections it receives — which must be 0.
//!
//! Each test opens with the ordering it reproduces (AGENTS.md §6).

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
    cairn_server::{
        egress::{EgressPolicy, Resolver, TenantHttp, EGRESS_DENIED},
        node::{NodeConfig, NodeMode, RunningNode},
    },
    common::fake_sdk::{FakeSdk, Step},
    serde_json::{json, Value},
    std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
};

const TTL: Duration = Duration::from_secs(2);

fn deny_private() -> EgressPolicy {
    EgressPolicy {
        deny_private: true,
        ..Default::default()
    }
}

fn allow_loopback() -> EgressPolicy {
    EgressPolicy {
        deny_private: true,
        allow: vec!["127.0.0.1/32".parse().unwrap()],
        ..Default::default()
    }
}

async fn node_with(bucket: &common::Bucket, policy: EgressPolicy) -> Result<RunningNode> {
    let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
        (**bucket).clone(),
        "",
        "memory://",
    ));
    let mut cfg = NodeConfig::new(stores, NodeMode::single(1));
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    cfg.egress = policy;
    common::start_led(cfg).await
}

/// The test's own client: no proxy (the proxy test sets `HTTP_PROXY` for the whole
/// child process), bounded.
fn client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(20))
        .build()?)
}

/// `POST /invoke/{service}/{handler}` — (code, JSON body).
async fn invoke(node: &RunningNode, service: &str, handler: &str) -> Result<(u16, Value)> {
    let r = client()?
        .post(format!("{}/invoke/{service}/{handler}", node.url()))
        .body("\"x\"")
        .send()
        .await?;
    let code = r.status().as_u16();
    Ok((code, r.json().await.unwrap_or(Value::Null)))
}

/// `POST /deployments {uri}` — (code, JSON body).
async fn register(node: &RunningNode, uri: &str) -> Result<(u16, Value)> {
    let r = client()?
        .post(format!("{}/deployments", node.url()))
        .json(&json!({ "uri": uri }))
        .send()
        .await?;
    let code = r.status().as_u16();
    Ok((code, r.json().await.unwrap_or(Value::Null)))
}

/// The registration must be refused with `400 egress_denied`.
async fn expect_denied(node: &RunningNode, uri: &str) -> Result<()> {
    let (code, body) = register(node, uri).await?;
    ensure!(
        code == 400 && body["code"] == EGRESS_DENIED,
        "{uri}: expected 400 {EGRESS_DENIED}, saw {code} {body}"
    );
    Ok(())
}

/// A TCP listener on `127.0.0.1` that only counts connections and speaks no HTTP: an
/// "internal destination" no request may reach.
struct Canary {
    addr: SocketAddr,
    accepted: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Canary {
    async fn spawn() -> Result<Self> {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = l.local_addr()?;
        let accepted = Arc::new(AtomicUsize::new(0));
        let n = Arc::clone(&accepted);
        let task = tokio::spawn(async move {
            while let Ok((sock, _)) = l.accept().await {
                n.fetch_add(1, Ordering::SeqCst);
                drop(sock);
            }
        });
        Ok(Self {
            addr,
            accepted,
            task,
        })
    }

    fn port(&self) -> u16 {
        self.addr.port()
    }

    fn hits(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}

impl Drop for Canary {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// An HTTP server answering every request with `app`, counting the requests.
async fn http_server(app: axum::Router) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = l.local_addr()?;
    let task = tokio::spawn(async move {
        let _ = axum::serve(l, app).await;
    });
    Ok((format!("http://{addr}"), task))
}

/// A fake resolver: returns the elements of `answers` one after another (the last
/// element repeats), one per name resolution. Counts how often it was asked.
#[derive(Clone)]
struct Scripted {
    answers: Arc<Mutex<Vec<Vec<SocketAddr>>>>,
    asked: Arc<AtomicUsize>,
}

impl Scripted {
    fn new(answers: Vec<Vec<SocketAddr>>) -> Self {
        Self {
            answers: Arc::new(Mutex::new(answers)),
            asked: Arc::default(),
        }
    }

    fn set(&self, answers: Vec<Vec<SocketAddr>>) {
        *self.answers.lock().unwrap() = answers;
    }
}

impl reqwest::dns::Resolve for Scripted {
    fn resolve(&self, _name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        self.asked.fetch_add(1, Ordering::SeqCst);
        let mut a = self.answers.lock().unwrap();
        let next = if a.len() > 1 {
            a.remove(0)
        } else {
            a[0].clone()
        };
        Box::pin(async move { Ok(Box::new(next.into_iter()) as reqwest::dns::Addrs) })
    }
}

fn sock(ip: &str, port: u16) -> SocketAddr {
    SocketAddr::new(ip.parse().unwrap(), port)
}

/// **Every blocked range and every way of writing an IP literal is refused before a
/// socket is opened.**
///
/// 1. A node with `--egress-deny-private`; a listener counting connections on
///    `127.0.0.1:<p>`.
/// 2. Register a deployment at `127.0.0.1:<p>` written in every way `url` accepts:
///    plain, decimal (`2130706433`), hex, shortened (`127.1`), IPv4-mapped IPv6,
///    `localhost`; then one address in each of the other blocked ranges.
///
/// Must see: every attempt `400 egress_denied`, the listener receives **0**
/// connections, and the error body echoes nothing from the destination side.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn literal_and_private_targets_are_denied() -> Result<()> {
    let bucket = common::bucket();
    let canary = Canary::spawn().await?;
    let node = node_with(&bucket, deny_private()).await?;
    let p = canary.port();
    let r = common::within(Duration::from_secs(60), "internal destination", async {
        for uri in [
            format!("http://127.0.0.1:{p}"),
            format!("http://2130706433:{p}"),
            format!("http://0x7f000001:{p}"),
            format!("http://127.1:{p}"),
            format!("http://[::ffff:127.0.0.1]:{p}"),
            format!("http://[::ffff:7f00:1]:{p}"),
            format!("http://localhost:{p}"),
            format!("http://[::1]:{p}"),
            "http://169.254.169.254/latest/meta-data".to_string(),
            "http://2852039166/".to_string(),
            "http://[::ffff:a9fe:a9fe]/".to_string(),
            "http://10.0.0.1/".to_string(),
            "http://172.16.0.1/".to_string(),
            "http://192.168.1.1/".to_string(),
            "http://100.64.0.1/".to_string(),
            "http://0.0.0.0/".to_string(),
            "http://192.0.0.8/".to_string(),
            "http://198.18.0.1/".to_string(),
            "http://224.0.0.1/".to_string(),
            "http://240.0.0.1/".to_string(),
            "http://[fc00::1]/".to_string(),
            "http://[fe80::1]/".to_string(),
            "http://[64:ff9b::a9fe:a9fe]/".to_string(),
            "http://[2002:a9fe:a9fe::1]/".to_string(),
            "http://[100::1]/".to_string(),
        ] {
            expect_denied(&node, &uri).await?;
        }
        ensure!(
            canary.hits() == 0,
            "the internal destination received {} connections",
            canary.hits()
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **`--egress-allow-cidr` opens exactly the intended network, `--egress-deny-cidr`
/// beats it.**
///
/// 1. Node A: deny-private, no allow ⇒ a fake SDK at `127.0.0.1` is refused.
/// 2. Node B: deny-private + allow `127.0.0.1/32` ⇒ registration works, the invocation
///    runs to completion.
/// 3. Node C: like B, plus deny `127.0.0.1/32` ⇒ refused again.
///
/// Must see: A and C `400 egress_denied`, B `completed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn allow_cidr_opens_and_deny_cidr_wins() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("done")])
        .spawn()
        .await?;
    let r = common::within(Duration::from_secs(60), "allow/deny cidr", async {
        let a = common::bucket();
        let node = node_with(&a, deny_private()).await?;
        let got = expect_denied(&node, &sdk.url).await;
        node.shutdown().await;
        got?;

        let b = common::bucket();
        let node = node_with(&b, allow_loopback()).await?;
        let got = async {
            let (code, body) = register(&node, &sdk.url).await?;
            ensure!(code == 200, "allow-cidr: {code} {body}");
            let (code, body) = invoke(&node, "Echo", "run").await?;
            ensure!(
                code == 200 && body["status"] == "completed",
                "{code} {body}"
            );
            Ok(())
        }
        .await;
        node.shutdown().await;
        got?;

        let c = common::bucket();
        let mut both = allow_loopback();
        both.deny = vec!["127.0.0.1/32".parse().unwrap()];
        let node = node_with(&c, both).await?;
        let got = expect_denied(&node, &sdk.url).await;
        node.shutdown().await;
        got
    })
    .await;
    r
}

/// **DNS rebinding: a name allowed at registration, pointing inside at invoke time.**
///
/// 1. A node with deny-private + allow `127.0.0.1/32`; the fake resolver answers
///    `sdk.test` ⇒ `127.0.0.1:<sdk>` (a real fake SDK).
/// 2. Register `http://sdk.test:<sdk>` ⇒ accepted (discovery's resolution).
/// 3. Switch the resolver: `sdk.test` ⇒ `10.0.0.1`, then `169.254.169.254`.
/// 4. Send an invocation: the turn's client opens a new connection, resolving again.
///
/// Must see: the fake SDK receives no `Start` after step 3; the invocation does not
/// finish and its `lastFailure` says `egress_denied`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dns_rebinding_after_registration_is_denied_at_invoke() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("done")])
        .spawn()
        .await?;
    let port: u16 = sdk.url.rsplit(':').next().unwrap().parse()?;
    let dns = Scripted::new(vec![vec![sock("127.0.0.1", port)]]);
    let mut policy = allow_loopback();
    policy.resolver = Some(Arc::new(dns.clone()) as Resolver);
    let bucket = common::bucket();
    let node = node_with(&bucket, policy).await?;
    let r = common::within(Duration::from_secs(60), "rebinding", async {
        let (code, body) = register(&node, &format!("http://sdk.test:{port}")).await?;
        ensure!(code == 200, "register via an allowed name: {code} {body}");
        ensure!(
            dns.asked.load(Ordering::SeqCst) >= 1,
            "discovery did not ask the resolver"
        );
        dns.set(vec![vec![
            sock("10.0.0.1", port),
            sock("169.254.169.254", port),
        ]]);
        let (code, body) = common::send(&node.url(), "Echo", "run", None, "\"x\"").await?;
        ensure!(code == 202 || code == 200, "{code} {body}");
        let id = body["id"]
            .as_str()
            .ok_or_else(|| anyhow!("no id: {body}"))?
            .to_string();
        let t0 = std::time::Instant::now();
        loop {
            let (_, st) = common::invocation(&node.url(), &id).await?;
            let msg = st["lastFailure"]["message"].as_str().unwrap_or("");
            if msg.contains(EGRESS_DENIED) {
                ensure!(st["status"] != "completed", "{st}");
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "the invocation must fail with {EGRESS_DENIED}: {st}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        ensure!(
            sdk.starts_len() == 0,
            "the SDK received {} Starts after the rebinding",
            sdk.starts_len()
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **Layer 3 alone: the socket's real peer is checked, even when the resolver returns
/// a blocked address directly.**
///
/// 1. A tenant client with deny-private, a fake resolver **without filtering**
///    (`filter_dns = false`) answering `sdk.test` ⇒ `127.0.0.1:<p>` — an HTTP server
///    counting requests.
/// 2. `GET http://sdk.test:<p>/discover`.
///
/// Must see: the error carries `egress_denied` (the connector layer refuses the peer
/// `127.0.0.1`), the server **receives no HTTP request** (the TCP connection may have
/// opened and then been dropped before the first byte).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connector_checks_the_real_peer() -> Result<()> {
    let seen = Arc::new(AtomicUsize::new(0));
    let n = Arc::clone(&seen);
    let app = axum::Router::new().fallback(move || {
        let n = Arc::clone(&n);
        async move {
            n.fetch_add(1, Ordering::SeqCst);
            "ok"
        }
    });
    let (url, task) = http_server(app).await?;
    let port: u16 = url.rsplit(':').next().unwrap().parse()?;
    let dns = Scripted::new(vec![vec![sock("127.0.0.1", port)]]);
    let http = TenantHttp::h1_with_resolver(Arc::new(deny_private()), Arc::new(dns), false);
    let got = http
        .client()
        .get(format!("http://sdk.test:{port}/discover"))
        .send()
        .await;
    task.abort();
    let e = got
        .err()
        .ok_or_else(|| anyhow!("peer 127.0.0.1 must be blocked"))?;
    ensure!(
        cairn_server::egress::denied_in(&e).is_some(),
        "the error must carry egress_denied: {e:?}"
    );
    ensure!(seen.load(Ordering::SeqCst) == 0, "HTTP request reached it");
    Ok(())
}

/// **Redirects are not followed**, even with the policy off (self-hosting).
///
/// 1. Server A answers `/discover` with `302 Location: <fake SDK B>/discover`.
/// 2. A node (default policy) registers `A`.
///
/// Must see: the registration `400`, B receives no request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redirects_are_not_followed() -> Result<()> {
    let b_hits = Arc::new(AtomicUsize::new(0));
    let n = Arc::clone(&b_hits);
    let sdk_b = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("done")])
        .spawn()
        .await?;
    // The real B: counts requests before passing them to the fake SDK.
    let real = sdk_b.url.clone();
    let b = axum::Router::new().fallback(move |req: axum::extract::Request| {
        let (n, real) = (Arc::clone(&n), real.clone());
        async move {
            n.fetch_add(1, Ordering::SeqCst);
            let r = reqwest::get(format!("{real}{}", req.uri())).await.unwrap();
            (
                [(
                    axum::http::header::CONTENT_TYPE,
                    r.headers()[reqwest::header::CONTENT_TYPE]
                        .to_str()
                        .unwrap()
                        .to_string(),
                )],
                r.text().await.unwrap(),
            )
        }
    });
    let (b_url, b_task) = http_server(b).await?;
    let target = format!("{b_url}/discover");
    let a = axum::Router::new().fallback(move || {
        let target = target.clone();
        async move {
            (
                axum::http::StatusCode::FOUND,
                [(axum::http::header::LOCATION, target)],
            )
        }
    });
    let (a_url, a_task) = http_server(a).await?;
    let bucket = common::bucket();
    let node = node_with(&bucket, EgressPolicy::default()).await?;
    let r = async {
        let (code, body) = register(&node, &a_url).await?;
        ensure!(code == 400, "redirect must fail it: {code} {body}");
        ensure!(
            b_hits.load(Ordering::SeqCst) == 0,
            "the redirect target received requests"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    a_task.abort();
    b_task.abort();
    r
}

/// **A discovery body over 1 MiB is refused**, even if it is a valid manifest.
///
/// 1. A server returns a valid JSON manifest, padded with whitespace to 2 MiB.
/// 2. A node registers it.
///
/// Must see: `400`, the error message names the 1 MiB cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discovery_body_over_one_mib_is_refused() -> Result<()> {
    let manifest = json!({
        "protocolMode": "REQUEST_RESPONSE",
        "minProtocolVersion": 1,
        "maxProtocolVersion": 1,
        "services": [{"name": "Echo", "ty": "SERVICE", "handlers": [{"name": "run"}]}],
    })
    .to_string();
    let body = format!("{manifest}{}", " ".repeat(2 << 20));
    let app = axum::Router::new().route(
        "/discover",
        axum::routing::get(move || {
            let body = body.clone();
            async move {
                (
                    [(
                        axum::http::header::CONTENT_TYPE,
                        cairn_server::discovery::DISCOVERY_V1,
                    )],
                    body,
                )
            }
        }),
    );
    let (url, task) = http_server(app).await?;
    let bucket = common::bucket();
    let node = node_with(&bucket, EgressPolicy::default()).await?;
    let r = async {
        let (code, body) = register(&node, &url).await?;
        ensure!(
            code == 400 && body["error"].as_str().unwrap_or("").contains("1 MiB"),
            "a 2 MiB body must be refused: {code} {body}"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    task.abort();
    r
}

/// The environment variable marking the proxy test's child process.
const PROXY_CHILD: &str = "CAIRN_TEST_EGRESS_PROXY_CHILD";

/// **A preset `HTTP_PROXY` does not steer tenant requests.**
///
/// Environment variables belong to the whole process, and `cargo test` runs the tests
/// of one binary in a shared process: the test reruns itself in a child process whose
/// `HTTP_PROXY`/`ALL_PROXY` point to a listener counting connections.
///
/// 1. (child) A "proxy" listener on `127.0.0.1`; `HTTP_PROXY`, `http_proxy`,
///    `ALL_PROXY` point at it.
/// 2. (child) A node with the default policy registers the fake SDK, invokes.
///
/// Must see: registration and invoke succeed, the proxy receives **0** connections.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn proxy_env_does_not_steer_tenant_requests() -> Result<()> {
    if std::env::var(PROXY_CHILD).is_ok() {
        return proxy_child().await;
    }
    let proxy = std::net::TcpListener::bind("127.0.0.1:0")?;
    let addr = proxy.local_addr()?;
    drop(proxy);
    let out = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "proxy_env_does_not_steer_tenant_requests",
            "--nocapture",
        ])
        .env(PROXY_CHILD, addr.to_string())
        .env("CAIRNFLOW_TEST_VERIFY", "1")
        .output()?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    ensure!(
        out.status.success() && stdout.contains("1 passed"),
        "the child process must run exactly this test and pass:\n{stdout}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}

async fn proxy_child() -> Result<()> {
    let at = std::env::var(PROXY_CHILD)?;
    let proxy = tokio::net::TcpListener::bind(&at).await?;
    let hits = Arc::new(AtomicUsize::new(0));
    let n = Arc::clone(&hits);
    let task = tokio::spawn(async move {
        while let Ok((s, _)) = proxy.accept().await {
            n.fetch_add(1, Ordering::SeqCst);
            drop(s);
        }
    });
    let url = format!("http://{at}");
    // The test's SAFETY: the child process runs only this one test.
    unsafe {
        for k in ["HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"] {
            std::env::set_var(k, &url);
        }
        for k in ["NO_PROXY", "no_proxy"] {
            std::env::remove_var(k);
        }
    }
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("done")])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let node = node_with(&bucket, EgressPolicy::default()).await?;
    let r = common::within(Duration::from_secs(60), "proxy env", async {
        let (code, body) = register(&node, &sdk.url).await?;
        ensure!(code == 200, "register with HTTP_PROXY: {code} {body}");
        let (code, body) = invoke(&node, "Echo", "run").await?;
        ensure!(
            code == 200 && body["status"] == "completed",
            "{code} {body}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    task.abort();
    r?;
    ensure!(
        hits.load(Ordering::SeqCst) == 0,
        "the proxy received {} connections",
        hits.load(Ordering::SeqCst)
    );
    Ok(())
}

/// **The cluster's internal paths do not go through the tenant policy.**
///
/// 1. A cluster alpha + beta on `127.0.0.1`, both with deny-private (no allow):
///    registering a fake SDK at `127.0.0.1` is refused — the tenant client blocks
///    loopback.
/// 2. Ask **alpha** for the status of a workflow in beta's range, requesting
///    forwarding (`x-cairn-forward: leader`).
///
/// Must see: (1) `400 egress_denied`; (2) beta answers (`404`, no invocation yet) with
/// `x-cairn-forwarded-by: beta` — forwarding to an internal IP still works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_forwarding_still_reaches_internal_peers() -> Result<()> {
    let (_bucket, nodes) = common::cluster_nodes_tweaked(&["alpha", "beta"], 2, TTL, |cfg| {
        cfg.egress = deny_private();
    })
    .await?;
    let refs: Vec<&common::ClusterNode> = nodes.iter().collect();
    let r = common::within(Duration::from_secs(120), "forwarding", async {
        let probe = common::wait_probe(
            &refs,
            cairn_server::balance::settle_for(TTL) + TTL * 8 + Duration::from_secs(10),
            "one range per node",
            |p| common::single_leaders(p).is_some_and(|m| common::load_of(&m, 2) == [1, 1]),
        )
        .await?;
        let leaders = common::single_leaders(&probe).expect("balanced");
        let pb = *leaders
            .iter()
            .find(|(_, i)| **i == 1)
            .expect("beta holds a range")
            .0;
        let sdk = FakeSdk::builder()
            .workflow("Wf", "run", vec![Step::output("done")])
            .spawn()
            .await?;
        expect_denied(&nodes[0].node, &sdk.url).await?;

        let alpha = nodes[0].node.url();
        let key = common::key_on_partition(&nodes[0].node, "Wf", pb, "b");
        let id: Value = reqwest::get(format!("{alpha}/workflow/Wf/{key}/id"))
            .await?
            .json()
            .await?;
        let id = id["id"]
            .as_str()
            .ok_or_else(|| anyhow!("{id}"))?
            .to_string();
        let r = reqwest::Client::new()
            .get(format!("{alpha}/invocations/{id}"))
            .header("x-cairn-forward", "leader")
            .send()
            .await?;
        let by = r
            .headers()
            .get("x-cairn-forwarded-by")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        ensure!(
            r.status() == 404 && by.as_deref() == Some("beta"),
            "forwarding to beta: {} {by:?}",
            r.status()
        );
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}
