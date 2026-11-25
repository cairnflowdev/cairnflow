// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! HTTP serving on both ports.
//!
//! | Case | Must see |
//! |---|---|
//! | half a header then silence (slowloris) | both ports close the connection after `http_header_timeout` |
//! | HTTP/2 prior knowledge (h2c) | no port speaks HTTP/2 |
//! | keep-alive connection idle 15 s (default limit 75 s) | the next request on the same socket is still answered |
//! | SSE open longer than the header limit | still receives bytes (headers are read, the limit does not cut the body) |
//! | tenant opens more long connections than its limit (SSE + attach) | `429 too_many_open_streams`; another tenant can still open |
//! | graceful shutdown with a request in flight | the request gets its full answer; shutdown is bounded even with an SSE open |
//!
//! Real sockets (`tokio::net::TcpStream`), not `LocalNet` — the accept loop is
//! what is under test.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, bail, ensure, Context, Result},
    cairn_server::{auth::AuthConfig, node::NodeMode},
    common::{
        fake_sdk::{FakeSdk, Step},
        within, TEST_DEADLINE,
    },
    reqwest::{Client, StatusCode},
    serde_json::{json, Value},
    std::time::{Duration, Instant},
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    },
};

const ADMIN_KEY: &str = "cf_test_http_limits_bootstrap_key_012345";

fn host_port(url: &str) -> String {
    url.trim_start_matches("http://").to_string()
}

/// Sends half a header then goes silent; returns the time until the server closes (EOF or reset).
async fn half_header_closed_after(addr: &str, limit: Duration) -> Result<Duration> {
    let mut s = TcpStream::connect(addr).await?;
    s.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\n").await?;
    let t0 = Instant::now();
    let mut buf = [0u8; 1024];
    tokio::time::timeout(limit, async {
        loop {
            match s.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    })
    .await
    .map_err(|_| anyhow!("{addr}: half header still open after {limit:?}"))?;
    Ok(t0.elapsed())
}

/// Reads one HTTP/1.1 response with `Content-Length` on `s`; returns the status
/// line and the body.
async fn read_response(s: &mut TcpStream) -> Result<(String, String)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..end]).to_string();
            let len: usize = head
                .lines()
                .find_map(|l| {
                    let (k, v) = l.split_once(':')?;
                    k.eq_ignore_ascii_case("content-length")
                        .then(|| v.trim().parse().ok())?
                })
                .context("missing content-length")?;
            while buf.len() < end + 4 + len {
                let n = s.read(&mut chunk).await?;
                ensure!(n > 0, "server closed mid-body");
                buf.extend_from_slice(&chunk[..n]);
            }
            let status = head.lines().next().unwrap_or_default().to_string();
            let body = String::from_utf8_lossy(&buf[end + 4..end + 4 + len]).to_string();
            return Ok((status, body));
        }
        let n = s.read(&mut chunk).await?;
        ensure!(n > 0, "server closed before answering");
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// **Slowloris.** Header limit 2 s.
///
/// Must see: both the ingress port and the internal port close a half-header
/// connection within [1 s, 10 s] — not much earlier than the limit, never open forever.
#[tokio::test]
async fn half_header_is_closed_after_the_header_timeout() -> Result<()> {
    let mut cfg = common::test_config(NodeMode::single(1));
    cfg.http_header_timeout = Duration::from_secs(2);
    let node = common::start_led(cfg).await?;
    for url in [node.url(), node.internal_url()] {
        let took = half_header_closed_after(&host_port(&url), Duration::from_secs(10)).await?;
        ensure!(
            took >= Duration::from_secs(1),
            "{url}: closed after {took:?}, earlier than the 2 s limit"
        );
    }
    node.shutdown().await;
    Ok(())
}

/// Sends the full header of a request with `Content-Length` then goes silent; returns
/// the time until the server answers or closes.
async fn silent_body_answered_after(addr: &str, head: &str, limit: Duration) -> Result<Duration> {
    let mut s = TcpStream::connect(addr).await?;
    s.write_all(head.as_bytes()).await?;
    let t0 = Instant::now();
    let mut buf = [0u8; 1024];
    tokio::time::timeout(limit, async {
        // An answer (first byte) or a close (EOF/reset) both mean "the server stopped waiting".
        let _ = s.read(&mut buf).await;
    })
    .await
    .map_err(|_| anyhow!("{addr}: silent body still awaited after {limit:?}"))?;
    Ok(t0.elapsed())
}

/// **Silent request body** (security review). A full header with
/// `Content-Length: 1000`, then no body byte at all — three body-reading paths:
/// `POST /ui/session` (before authentication), a request asking to be forwarded
/// (`x-cairn-forward: leader`, with a key — the forwarding layer sits after
/// authentication), and the internal port. Header limit 2 s.
///
/// Must see: the server answers or closes within [1 s, 10 s] on all three — the
/// header limit does not cover the body, so a silence limit between body chunks is needed.
#[tokio::test]
async fn silent_body_is_cut_after_the_header_timeout() -> Result<()> {
    let mut cfg = common::test_config(NodeMode::single(1));
    cfg.http_header_timeout = Duration::from_secs(2);
    cfg.auth = AuthConfig {
        required: true,
        admin_key: Some(ADMIN_KEY.into()),
        cluster_secret: Some("cf_test_cluster_secret_http_limits_32b".into()),
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    let node = common::start_led(cfg).await?;
    let body = "Content-Length: 1000\r\nContent-Type: application/json\r\n\r\n";
    let ingress = host_port(&node.url());
    for (addr, head) in [
        (
            ingress.clone(),
            format!("POST /ui/session HTTP/1.1\r\nHost: {ingress}\r\nOrigin: http://{ingress}\r\n{body}"),
        ),
        (
            ingress.clone(),
            format!(
                "POST /invoke/Svc/run HTTP/1.1\r\nHost: {ingress}\r\nx-cairn-forward: leader\r\n\
                 Authorization: Bearer {ADMIN_KEY}\r\n{body}"
            ),
        ),
        (
            host_port(&node.internal_url()),
            format!("POST /internal/inbound HTTP/1.1\r\nHost: x\r\n{body}"),
        ),
    ] {
        let took = silent_body_answered_after(&addr, &head, Duration::from_secs(10)).await?;
        ensure!(
            took >= Duration::from_secs(1),
            "{addr}: answered after {took:?}, before the body could arrive"
        );
    }
    node.shutdown().await;
    Ok(())
}

/// A one-range node, auth on, with a cluster secret (the forwarding layer runs),
/// header limit `header`.
async fn authed_node(header: Duration) -> Result<cairn_server::node::RunningNode> {
    let mut cfg = common::test_config(NodeMode::single(1));
    cfg.http_header_timeout = header;
    cfg.auth = AuthConfig {
        required: true,
        admin_key: Some(ADMIN_KEY.into()),
        cluster_secret: Some("cf_test_cluster_secret_http_limits_32b".into()),
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    common::start_led(cfg).await
}

/// **An unauthenticated body is not read** (security review). The forwarding layer
/// used to sit **outside** the key layer: a request carrying `x-cairn-forward: leader`
/// had its body buffered (≤ 2 MiB) on the public listener before anyone asked for a key.
///
/// Order: the full header of a `POST` with `x-cairn-forward: leader`, **no** key,
/// `Content-Length` 2 MiB; send 1 KiB of body then go silent. Header limit 30 s.
///
/// Must see: `401` within 2 s — authentication answers before reading the body. Before
/// the fix: the server waited for the body up to the silence limit (30 s).
#[tokio::test]
async fn unauthenticated_forward_body_is_not_read() -> Result<()> {
    let node = authed_node(Duration::from_secs(30)).await?;
    let addr = host_port(&node.url());
    let mut s = TcpStream::connect(&addr).await?;
    s.write_all(
        format!(
            "POST /invoke/Svc/run HTTP/1.1\r\nHost: {addr}\r\nx-cairn-forward: leader\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            2 << 20
        )
        .as_bytes(),
    )
    .await?;
    s.write_all(&[b' '; 1024]).await?;
    let (status, _) = within(Duration::from_secs(2), "401 before body read", async {
        read_response(&mut s).await
    })
    .await?;
    ensure!(status.contains(" 401 "), "must be 401 at once: {status}");
    node.shutdown().await;
    Ok(())
}

/// **A trickling body has a total limit** (security review, defence in depth). The
/// body silence limit only counts the gap between two chunks: a client sending 1 byte
/// every half limit holds the request (and the connection slot) almost forever.
///
/// Order: a `POST` with a key, `Content-Length: 1000`, header limit 2 s; send 1 byte
/// of body every 500 ms.
///
/// Must see: the server answers or closes within [3 s, 10 s] — total limit 2 × the
/// header limit (4 s). Before the fix: it waited for all 1000 bytes (~500 s).
#[tokio::test]
async fn trickling_body_is_cut_after_twice_the_header_timeout() -> Result<()> {
    let node = authed_node(Duration::from_secs(2)).await?;
    let addr = host_port(&node.url());
    let mut s = TcpStream::connect(&addr).await?;
    s.write_all(
        format!(
            "POST /invoke/Svc/run HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {ADMIN_KEY}\r\n\
             Content-Type: application/json\r\nContent-Length: 1000\r\n\r\n"
        )
        .as_bytes(),
    )
    .await?;
    let t0 = Instant::now();
    let mut buf = [0u8; 1024];
    let cut = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                r = s.read(&mut buf) => return r.map(|_| ()),
                _ = tokio::time::sleep(Duration::from_millis(500)) => {
                    if s.write_all(b" ").await.is_err() {
                        return Ok(());
                    }
                }
            }
        }
    })
    .await;
    let took = t0.elapsed();
    ensure!(cut.is_ok(), "trickling body still awaited after 10 s");
    ensure!(
        took >= Duration::from_secs(3),
        "cut after {took:?}, earlier than the 4 s total limit"
    );
    node.shutdown().await;
    Ok(())
}

/// **h2c off.** An HTTP/2 prior-knowledge client calls both ports.
///
/// Must see: no answer is HTTP/2 (a connection error is fine too) — an h2
/// connection opens many streams and bypasses every HTTP/1 limit.
#[tokio::test]
async fn h2_prior_knowledge_is_not_served() -> Result<()> {
    let node = common::start_led(common::test_config(NodeMode::single(1))).await?;
    let h2 = Client::builder()
        .http2_prior_knowledge()
        .timeout(Duration::from_secs(5))
        .build()?;
    for url in [node.url(), node.internal_url()] {
        if let Ok(r) = h2.get(format!("{url}/health")).send().await {
            ensure!(
                r.version() != reqwest::Version::HTTP_2,
                "{url} answered HTTP/2 ({})",
                r.status()
            );
        }
    }
    node.shutdown().await;
    Ok(())
}

/// **Idle keep-alive under the default limit.** One socket, a request, idle 15 s,
/// another request.
///
/// Must see: the second request is answered on the same socket — the 75 s limit
/// (both the header limit and the idle limit) does not cut an idle connection that
/// clients and proxies still reuse.
#[tokio::test]
async fn idle_keep_alive_survives_15s_under_the_default_timeout() -> Result<()> {
    let node = common::start_led(common::test_config(NodeMode::single(1))).await?;
    let mut s = TcpStream::connect(host_port(&node.url())).await?;
    for round in 0..2 {
        if round == 1 {
            tokio::time::sleep(Duration::from_secs(15)).await;
        }
        s.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n")
            .await?;
        let (status, body) =
            within(TEST_DEADLINE, "answer to /health", read_response(&mut s)).await?;
        ensure!(status.contains(" 200 "), "round {round}: {status}");
        ensure!(body == "ok", "round {round}: {body}");
    }
    node.shutdown().await;
    Ok(())
}

/// An SDK whose `Hold/wait` waits for a signal nobody sends: the invocation lives forever.
async fn hold_sdk() -> Result<FakeSdk> {
    FakeSdk::builder()
        .object("Hold", "wait", vec![Step::Suspend, Step::output("freed")])
        .spawn()
        .await
}

/// A client bound to a key (absent ⇒ no auth).
#[derive(Clone)]
struct As {
    url: String,
    key: Option<String>,
    http: Client,
}

impl As {
    fn open(url: &str) -> Self {
        Self {
            url: url.into(),
            key: None,
            http: Client::new(),
        }
    }

    /// An `admin` role key of `tenant`, created with the bootstrap key.
    async fn tenant(url: &str, tenant: &str) -> Result<Self> {
        let admin = Self {
            key: Some(ADMIN_KEY.into()),
            ..Self::open(url)
        };
        let (code, v) = admin
            .send(
                admin
                    .http
                    .post(format!("{url}/admin/keys"))
                    .json(&json!({ "tenant": tenant, "role": "admin" })),
            )
            .await?;
        ensure!(code == StatusCode::OK, "create key {tenant}: {code} {v}");
        Ok(Self {
            key: v["secret"].as_str().map(str::to_string),
            ..admin
        })
    }

    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.key {
            Some(k) => req.bearer_auth(k),
            None => req,
        }
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> Result<(StatusCode, Value)> {
        let r = self.auth(req).send().await?;
        let code = r.status();
        let text = r.text().await?;
        Ok((
            code,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        ))
    }

    /// Registers `sdk` then starts `Hold/wait` with key `key`; returns the id.
    async fn held_invocation(&self, sdk: &str, key: &str) -> Result<String> {
        let (code, v) = self
            .send(
                self.http
                    .post(format!("{}/deployments", self.url))
                    .json(&json!({ "uri": sdk })),
            )
            .await?;
        ensure!(code.is_success(), "register: {code} {v}");
        let (code, v) = self
            .send(
                self.http
                    .post(format!("{}/invoke/Hold/wait/send?key={key}", self.url))
                    .body("{}"),
            )
            .await?;
        ensure!(code.is_success(), "send: {code} {v}");
        v["id"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| anyhow!("send returned no id: {v}"))
    }

    /// Opens the `ui` SSE of `id`: `Ok(resp)` on `200` (headers arrived — the slot is held),
    /// `Err((code, body))` otherwise. `404`/`425` while the invocation has not reached
    /// its range means wait and retry.
    async fn sse(&self, id: &str) -> Result<std::result::Result<reqwest::Response, (u16, Value)>> {
        within(TEST_DEADLINE, "open SSE", async {
            loop {
                let r = self
                    .auth(
                        self.http
                            .get(format!("{}/invocations/{id}/streams/ui", self.url))
                            .header("accept", "text/event-stream"),
                    )
                    .send()
                    .await?;
                let code = r.status().as_u16();
                if code == 200 {
                    return Ok(Ok(r));
                }
                let text = r.text().await.unwrap_or_default();
                if code == 404 || code == 425 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                return Ok(Err((
                    code,
                    serde_json::from_str(&text).unwrap_or(Value::String(text)),
                )));
            }
        })
        .await
    }
}

/// Waits for the node's `cairnflow_invoke_waiters` gauge to reach `n` (the attach has
/// entered its wait loop, i.e. holds a slot).
async fn waiters_at_least(me: &As, n: f64) -> Result<()> {
    loop {
        let m = me
            .auth(me.http.get(format!("{}/metrics", me.url)))
            .send()
            .await?
            .text()
            .await?;
        let waiting = m
            .lines()
            .find(|l| l.starts_with("cairnflow_invoke_waiters "))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);
        if waiting >= n {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// **SSE longer than the header limit.** Header limit 1 s, `:keepalive` every 200 ms;
/// one SSE on a held invocation, read for 3 s.
///
/// Must see: the SSE receives ≥ 5 byte chunks and is not closed — the limit applies
/// only while reading headers, not to a response body that is flowing.
#[tokio::test]
async fn sse_outlives_the_header_timeout() -> Result<()> {
    let mut cfg = common::test_config(NodeMode::single(1));
    cfg.http_header_timeout = Duration::from_secs(1);
    cfg.streams.keepalive = Duration::from_millis(200);
    let node = common::start_led(cfg).await?;
    let sdk = hold_sdk().await?;
    let me = As::open(&node.url());
    let id = me.held_invocation(&sdk.url, "k").await?;
    let mut resp = me.sse(&id).await?.map_err(|e| anyhow!("SSE: {e:?}"))?;
    let until = Instant::now() + Duration::from_secs(3);
    let mut chunks = 0;
    while Instant::now() < until {
        match tokio::time::timeout_at(until.into(), resp.chunk()).await {
            Ok(Ok(Some(_))) => chunks += 1,
            Ok(Ok(None)) => bail!("server closed the SSE after {chunks} chunks"),
            Ok(Err(e)) => bail!("SSE broke after {chunks} chunks: {e}"),
            Err(_) => break,
        }
    }
    ensure!(chunks >= 5, "SSE received only {chunks} chunks in 3 s");
    drop(resp);
    node.shutdown().await;
    Ok(())
}

/// **Long connections are capped per tenant.** Limit 3 per tenant; alpha and
/// bravo each have one held invocation.
///
/// 1. alpha opens 2 SSEs and 1 attach (`wait_ms` 20 s) — 3 slots; wait for the attach
///    to enter its wait loop (the waiter gauge).
/// 2. alpha opens a fourth SSE, then a fourth attach.
/// 3. bravo opens its own SSE.
/// 4. alpha closes one SSE and reopens it.
///
/// Must see: (2) both `429` with code `too_many_open_streams`; (3) `200` — the limit
/// is not one shared bucket; (4) `200` once the slot is returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_connections_are_capped_per_tenant() -> Result<()> {
    let mut cfg = common::test_config(NodeMode::single(1));
    cfg.auth = AuthConfig {
        required: true,
        admin_key: Some(ADMIN_KEY.into()),
        cluster_secret: None,
        metrics_key: None,
        admin_key_sha256: None,
        provision_key_sha256: None,
    };
    cfg.streams.tenant_max = 3;
    let node = common::start_led(cfg).await?;
    let (sdk_a, sdk_b) = (hold_sdk().await?, hold_sdk().await?);
    let alpha = As::tenant(&node.url(), "alpha").await?;
    let bravo = As::tenant(&node.url(), "bravo").await?;
    let id_a = alpha.held_invocation(&sdk_a.url, "a").await?;
    let id_b = bravo.held_invocation(&sdk_b.url, "b").await?;

    let mut held = Vec::new();
    for _ in 0..2 {
        held.push(
            alpha
                .sse(&id_a)
                .await?
                .map_err(|e| anyhow!("SSE alpha: {e:?}"))?,
        );
    }
    let attach = {
        let alpha = alpha.clone();
        let id = id_a.clone();
        tokio::spawn(async move {
            alpha
                .send(alpha.http.post(format!(
                    "{}/invocations/{id}/attach?wait_ms=20000",
                    alpha.url
                )))
                .await
        })
    };
    // The attach holds its slot once it enters the wait loop: the range's waiters go
    // up to 1. Do not probe with an SSE — a probing SSE takes the third slot just as the
    // attach arrives and the attach itself gets 429.
    // `/metrics` needs a cluster-level key: ask with the bootstrap key.
    let boot = As {
        key: Some(ADMIN_KEY.into()),
        ..As::open(&node.url())
    };
    within(
        TEST_DEADLINE,
        "alpha attach waiting",
        waiters_at_least(&boot, 1.0),
    )
    .await?;
    let over = alpha
        .sse(&id_a)
        .await?
        .err()
        .ok_or_else(|| anyhow!("alpha's fourth SSE opened"))?;
    ensure!(
        over.0 == 429 && over.1["code"] == "too_many_open_streams",
        "alpha's fourth SSE: {over:?}"
    );
    let (code, v) = alpha
        .send(alpha.http.post(format!(
            "{}/invocations/{id_a}/attach?wait_ms=20000",
            alpha.url
        )))
        .await?;
    ensure!(
        code == StatusCode::TOO_MANY_REQUESTS && v["code"] == "too_many_open_streams",
        "alpha's fourth attach: {code} {v}"
    );
    let b = bravo.sse(&id_b).await?;
    ensure!(b.is_ok(), "bravo blocked by alpha's limit: {:?}", b.err());
    drop(b);
    drop(held.pop());
    within(TEST_DEADLINE, "alpha's slot returns", async {
        loop {
            match alpha.sse(&id_a).await? {
                Ok(_) => return Ok(()),
                Err((429, _)) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err(e) => bail!("alpha SSE after the slot was returned: {e:?}"),
            }
        }
    })
    .await?;
    attach.abort();
    node.shutdown().await;
    Ok(())
}

/// **Graceful shutdown.** One attach waiting (`wait_ms` 3 s) and one SSE client that
/// never closes; shut the node down in the middle.
///
/// Must see: the attach receives a full HTTP answer (any code — not a connection
/// error); `shutdown` finishes within 20 s even with the SSE open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_shutdown_finishes_inflight_and_is_bounded() -> Result<()> {
    let node = common::start_led(common::test_config(NodeMode::single(1))).await?;
    let sdk = hold_sdk().await?;
    let me = As::open(&node.url());
    let id = me.held_invocation(&sdk.url, "k").await?;
    let sse = me.sse(&id).await?.map_err(|e| anyhow!("SSE: {e:?}"))?;
    let attach = {
        let me = me.clone();
        let id = id.clone();
        tokio::spawn(async move {
            let r = me
                .http
                .post(format!("{}/invocations/{id}/attach?wait_ms=3000", me.url))
                .send()
                .await?;
            let code = r.status();
            r.bytes().await?;
            anyhow::Ok(code)
        })
    };
    // The attach has entered its wait loop: the range's waiters go up to 1.
    within(TEST_DEADLINE, "attach waiting", waiters_at_least(&me, 1.0)).await?;
    tokio::time::timeout(Duration::from_secs(20), node.shutdown())
        .await
        .map_err(|_| anyhow!("shutdown took over 20 s with an SSE open"))?;
    let code = attach.await??;
    ensure!(
        !code.is_informational(),
        "the attach must receive a full answer: {code}"
    );
    drop(sse);
    Ok(())
}
