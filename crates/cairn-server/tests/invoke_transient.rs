// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Synchronous `POST /invoke` when **the first turn fails transiently**
//! (`docs/admin-api.md` §Calling services, "Infrastructure errors of a turn").
//!
//! Two classes of turn failure, **one** answer shape:
//!
//! - The handler reports a retryable error (`ErrorMessage`, behavior `RETRY`): the turn
//!   ends with `InvokeOutcome::Retry`, the `/invoke` door keeps waiting
//!   (`wait_for_settled`) and the client gets the **final result**.
//! - The node → SDK hop fails at the transport layer (connection reset, `error sending
//!   request`, `error decoding response body`, the SDK cannot be reached or returns
//!   5xx): `drive_one` returns `Err`, the turn leaves a scheduled retry
//!   (`failed_after_running`). The invocation was durably accepted, so the door also
//!   **waits through** the retry as above — up to `--invoke-wait-ms`; past the limit,
//!   `202 waiting` with the id, and `lastFailure` on `/invocations/{id}` says why.
//!   Formerly it returned `502` with the id at once.
//!
//! Unchanged: a turn that fails because **leadership was lost** (the answer cannot pass
//! `finish`'s durability ticket / read fence), and errors during **admission** (admit —
//! before the invocation surely exists; `tests/failpoints.rs`). Both modes,
//! `REQUEST_RESPONSE` (`FakeSdk`) and `BIDI_STREAM` (`RawSdk` below: hand-written h2c).
//! The node is built on a `FaultyStore` carrying the faults of `CAIRNFLOW_TEST_FAULTS`,
//! so the `check.sh` row "ambiguous once per op" can run this file.

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
    bytes::Bytes,
    cairn_protocol::{pb, Message},
    cairn_server::{
        discovery::{
            EndpointManifest, HandlerManifest, ProtocolMode, ServiceManifest, ServiceType,
            DISCOVERY_V1,
        },
        invoker::{encode_stream, INVOCATION_V1},
        node::{NodeConfig, NodeMode, RunningNode},
    },
    cairn_store::{
        testkit::{Action, FaultyStore, OpSet, PathMatch, Rule},
        ObjectStores, StoreLocation,
    },
    common::fake_sdk::{FakeSdk, Step},
    slatedb::object_store::memory::InMemory,
    std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::{Duration, Instant},
    },
    tokio::io::{AsyncReadExt, AsyncWriteExt},
};

/// A one-range node on `bucket`, `/invoke` wait limit = `wait`. The store is wrapped in
/// a `FaultyStore` carrying the faults of `CAIRNFLOW_TEST_FAULTS` (the `check.sh` row
/// "ambiguous once per op").
async fn start_on(bucket: &Arc<InMemory>, wait: Duration) -> Result<RunningNode> {
    let store = FaultyStore::wrap(bucket.clone());
    common::install_env_faults(&store);
    let stores = ObjectStores::single(StoreLocation::from_store(store, "", "memory://"));
    let mut cfg = NodeConfig::new(stores, NodeMode::single(1));
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    cfg.invoke_wait = Some(wait);
    common::start_led(cfg).await
}

/// The default wait limit of `--invoke-wait-ms`.
const WAIT: Duration = Duration::from_secs(5);

/// The 48-hex-character id of an `/invoke` answer.
fn id_of(body: &serde_json::Value) -> Result<String> {
    let id = body["id"]
        .as_str()
        .ok_or_else(|| anyhow!("the answer must carry an id: {body}"))?;
    ensure!(id.len() == 48, "id of 48 hex characters: {body}");
    Ok(id.to_owned())
}

/// **Transport error on the first turn (`REQUEST_RESPONSE`): `/invoke` waits through the
/// retry and returns `200` with the final result, side effect once.**
///
/// Interleaving:
/// 1. `Svc/first` completes; the runtime's SDK client keeps the h1 connection in its pool
///    (idle up to 4 s, `invoker::SDK_POOL_IDLE`).
/// 2. The connection exceeds the fake SDK's idle limit (`idle_close` 300 ms) — the SDK
///    server will drop the next request arriving on it, like `node:http` closing a socket
///    just as a request is in flight (same class as a mid-turn RST on h2c: `drive_one`
///    returns `Err`).
/// 3. `POST /invoke/Svc/second`: the first turn fails at the transport layer **before**
///    the handler runs; the turn leaves a scheduled retry (100 ms, `backoff_ms(0)`).
/// 4. The retry fires on a new connection, the handler runs once, the invocation is `completed`.
///
/// Must see: step 3 returns `200 completed` with step 4's output and the invocation id,
/// within the 5 s wait limit (not `502`); the handler runs exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_invoke_waits_out_a_transport_failure_of_the_first_turn() -> Result<()> {
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service("Svc", "first", vec![Step::output("1")])
        .service("Svc", "second", vec![Step::output("2")])
        .idle_close(Duration::from_millis(300))
        .spawn()
        .await?;
    let node = start_on(&bucket, WAIT).await?;
    let res = common::within(Duration::from_secs(30), "turn fails then completes", async {
        common::register(&node.url(), &sdk.url).await?;
        let (code, body) = common::invoke(&node.url(), "Svc", "first", None, "{}").await?;
        ensure!(code == 200, "{code} {body}");
        // The connection of the turn above exceeds the fake SDK's idle limit — the scenario itself.
        tokio::time::sleep(Duration::from_millis(600)).await;

        let asked = Instant::now();
        let (code, body) = common::invoke(&node.url(), "Svc", "second", None, "{}").await?;
        let answered_in = asked.elapsed();
        ensure!(
            code == 200 && body["status"] == "completed" && body["output"] == "2",
            "first turn failed at the transport: /invoke waits through the retry and returns the final result, \
             saw {code} {body}"
        );
        let id = id_of(&body)?;
        ensure!(
            answered_in < WAIT,
            "the final result arrives within the wait limit: {answered_in:?}"
        );
        // The returned result is exactly the durable state of the invocation.
        let (code, st) = common::invocation(&node.url(), &id).await?;
        ensure!(
            code == 200 && st["status"] == "completed" && st["output"] == "2",
            "durable state matches the answer: {code} {st}"
        );
        Ok(answered_in)
    })
    .await;
    node.shutdown().await;
    let answered_in = res?;
    ensure!(
        sdk.starts_of("Svc", "second").len() == 1,
        "the handler runs exactly once (the failed turn never reached the SDK): {:?}",
        sdk.starts_of("Svc", "second")
    );
    eprintln!("failed turn → 200 after {answered_in:?} (/invoke wait limit: {WAIT:?})");
    Ok(())
}

/// **Transport error mid first turn (`BIDI_STREAM`): `/invoke` waits through the retry
/// and returns `200` with the final result.**
///
/// Interleaving (the shape of the h2c SDK-hop failure measured on BIDI cross-calls):
/// 1. `POST /invoke/Svc/second`; the runtime opens an h2c stream to the SDK.
/// 2. The SDK returns a `200` header then **RST_STREAM(INTERNAL_ERROR)** on that stream —
///    the runtime hits a transport error (`error sending request` or `error decoding
///    response body`, depending on whether the RST arrives before or after the header is
///    read), `drive_one` returns `Err`, the turn leaves a scheduled retry.
/// 3. The retry opens a second stream on the same connection; the SDK returns `Output` + `End`.
///
/// Must see: step 1 returns `200 completed` with step 3's output within the wait limit;
/// the SDK sees exactly two streams and returns a result exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_invoke_waits_out_a_transport_failure_of_the_first_turn_bidi() -> Result<()> {
    let bucket = common::bucket();
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream, "second", true).await?;
    let node = start_on(&bucket, WAIT).await?;
    let res = common::within(Duration::from_secs(30), "stream fails, then done", async {
        common::register(&node.url(), &sdk.url).await?;
        let asked = Instant::now();
        let (code, body) = common::invoke(&node.url(), "Svc", "second", None, "{}").await?;
        let answered_in = asked.elapsed();
        ensure!(
            code == 200 && body["status"] == "completed" && body["output"] == "2",
            "first stream was reset: /invoke waits through the retry and returns the final result, \
             saw {code} {body}"
        );
        id_of(&body)?;
        ensure!(
            answered_in < WAIT,
            "the final result arrives within the wait limit: {answered_in:?}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    res?;
    ensure!(
        sdk.opened() == 2 && sdk.answered() == 1,
        "two streams (one reset), result returned once: opened {}, answered {}",
        sdk.opened(),
        sdk.answered()
    );
    Ok(())
}

/// **The SDK is dead for good: `/invoke` returns `202 waiting` with the id right after
/// the wait limit, and `/invocations/{id}` gives the last error (`lastFailure`).**
///
/// Order:
/// 1. Register the deployment, then kill the SDK — listener and every connection (`RawSdk`
///    answers discovery with `connection: close`, the runtime's pool keeps nothing).
/// 2. `POST /invoke/Svc/gone` with `--invoke-wait-ms` = 1.5 s: every turn fails at the
///    transport layer (connection refused), rescheduled by backoff.
///
/// Must see: `202 waiting` with the id, no earlier than the wait limit and no later than
/// the limit + 3 s; `GET /invocations/{id}` is `running`/`suspended` with
/// `lastFailure` (code, message, attempt).
async fn sdk_gone_scenario(mode: ProtocolMode) -> Result<()> {
    const SHORT: Duration = Duration::from_millis(1500);
    let bucket = common::bucket();
    let sdk = RawSdk::spawn(mode, "gone", false).await?;
    let node = start_on(&bucket, SHORT).await?;
    let res = common::within(Duration::from_secs(30), "SDK dead for good", async {
        common::register(&node.url(), &sdk.url).await?;
        sdk.kill();
        let asked = Instant::now();
        let (code, body) = common::invoke(&node.url(), "Svc", "gone", None, "{}").await?;
        let answered_in = asked.elapsed();
        ensure!(
            code == 202 && body["status"] == "waiting",
            "SDK dead for good: /invoke returns 202 waiting after the wait limit, saw {code} {body}"
        );
        let id = id_of(&body)?;
        ensure!(
            answered_in >= SHORT.mul_f32(0.95) && answered_in < SHORT + Duration::from_secs(3),
            "202 goes out at the wait limit {SHORT:?}, seen after {answered_in:?}"
        );
        let (code, st) = common::invocation(&node.url(), &id).await?;
        ensure!(
            code == 200 && (st["status"] == "running" || st["status"] == "suspended"),
            "invocation accepted, not completed: {code} {st}"
        );
        let last = &st["lastFailure"];
        ensure!(
            last["code"].is_u64() && last["message"].is_string() && last["attempt"].is_u64(),
            "lastFailure gives the error of the last attempt: {st}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    res?;
    ensure!(sdk.answered() == 0, "a dead SDK answers nothing");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_invoke_returns_202_after_the_wait_when_the_sdk_is_gone() -> Result<()> {
    sdk_gone_scenario(ProtocolMode::RequestResponse).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_invoke_returns_202_after_the_wait_when_the_sdk_is_gone_bidi() -> Result<()> {
    sdk_gone_scenario(ProtocolMode::BidiStream).await
}

/// **Control: a retryable handler error on the first turn — `/invoke` waits through the
/// retry and returns `200` with the final result.**
///
/// Order:
/// 1. `POST /invoke/Svc/flaky`: on attempt 0 the handler reports a `RETRY` error (`FailBelow(1)`).
/// 2. The turn ends with `InvokeOutcome::Retry`; the door waits (`wait_for_settled`).
/// 3. The retry (100 ms) fires, attempt 1 returns output.
///
/// Must see: `200 completed` with attempt 1's output; the handler is entered twice
/// (attempt 0, 1).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_invoke_waits_out_a_retryable_handler_error() -> Result<()> {
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service("Svc", "flaky", vec![Step::FailBelow(1), Step::output("ok")])
        .spawn()
        .await?;
    let node = start_on(&bucket, WAIT).await?;
    let res = common::within(Duration::from_secs(30), "handler error, then done", async {
        common::register(&node.url(), &sdk.url).await?;
        let (code, body) = common::invoke(&node.url(), "Svc", "flaky", None, "{}").await?;
        ensure!(
            code == 200 && body["status"] == "completed" && body["output"] == "ok",
            "retryable handler error: /invoke waits for the final result, saw {code} {body}"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    res?;
    let attempts: Vec<u32> = sdk
        .starts_of("Svc", "flaky")
        .into_iter()
        .map(|s| s.attempt)
        .collect();
    ensure!(attempts == [0, 1], "two attempts 0, 1: {attempts:?}");
    Ok(())
}

/// **An `/invoke` turn loses leadership midway: the answer is still the common error
/// body with the id — no `202`, no result.**
///
/// Interleaving:
/// 1. A two-node cluster, one range, TTL 1 s; one node leads the range (L).
/// 2. `POST /invoke/Svc/slow` to L; the handler stalls 2.5 s mid-turn.
/// 3. Once the SDK has received `Start`: every request of L to the store fails at once —
///    L's grant lapses within ⅔·TTL, the other node takes the range.
/// 4. The handler returns its result to L; L can no longer write or force anything durable.
///
/// Must see: step 2 returns the common error body with the id (no `status`) — `502 "engine
/// stopped"` when L steps down and cancels the turn's task (the answer channel drops,
/// measured), or `503` when the turn reaches `finish` with a fence error. Waiting through
/// retries does not change this branch: a cancelled task never reaches that code, and the
/// answer of a leader that lost its authority cannot pass `finish`'s durability ticket /
/// read fence, so `waiting` never goes out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_invoke_whose_turn_loses_leadership_keeps_its_error() -> Result<()> {
    let (_bucket, nodes) =
        common::cluster_nodes(&["inv-a", "inv-b"], 1, Duration::from_secs(1)).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let sdk = FakeSdk::builder()
        .service("Svc", "slow", vec![Step::Stall(2500), Step::output("late")])
        .spawn()
        .await?;
    let probe = common::wait_probe(&refs, Duration::from_secs(20), "one leader", |p| {
        p.get(&0).is_some_and(|who| who.len() == 1)
    })
    .await?;
    let l = probe[&0][0];
    let leader = &nodes[l];
    let url = leader.node.url();
    let res = common::within(Duration::from_secs(30), "turn loses authority", async {
        common::register(&url, &sdk.url).await?;
        let ask = tokio::spawn({
            let url = url.clone();
            async move { common::invoke(&url, "Svc", "slow", None, "{}").await }
        });
        common::within(Duration::from_secs(10), "SDK received Start", async {
            while sdk.starts_of("Svc", "slow").is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok(())
        })
        .await?;
        leader.store.add_rule(Rule::new(
            OpSet::ALL,
            PathMatch::Any,
            Action::Fail { times: None },
        ));
        let (code, body) = ask.await??;
        ensure!(
            (code == 502 || code == 503)
                && body.get("status").is_none()
                && body["error"].is_string(),
            "turn lost leadership: /invoke returns the common error body (502/503), saw {code} {body}"
        );
        id_of(&body)?;
        Ok(())
    })
    .await;
    // L has no store: shutting it down may hang on the last flush — shut down only the other node.
    let mut nodes = nodes.into_iter();
    let (a, b) = (nodes.next().expect("a"), nodes.next().expect("b"));
    let other = if l == 0 { b } else { a };
    other.node.shutdown().await;
    res
}

// ───────────────────────────────────────────── hand-written fake SDK (h1 + h2c)

/// A **hand-written** fake SDK over TCP: `GET /discover` over h1 with `connection:
/// close` (the runtime's pool keeps no connection), `POST /invoke/...` over h2c
/// for `BIDI_STREAM`. Two things [`FakeSdk`] cannot do: `BIDI_STREAM` mode,
/// and **dying for good** — [`RawSdk::kill`] drops the listener and every open connection
/// (axum keeps old connections alive after the server stops).
///
/// One service `Svc` with one handler; each h2 stream returns `Output("2")` + `End`
/// as soon as it opens, except the **first** stream when `reset_first`: a `200` header then
/// `RST_STREAM(INTERNAL_ERROR)`. `REQUEST_RESPONSE` mode is only used to register
/// and then kill — an h1 request other than `/discover` gets its connection closed without an answer.
struct RawSdk {
    url: String,
    shared: Arc<RawShared>,
}

struct RawShared {
    mode: ProtocolMode,
    handler: &'static str,
    reset_first: bool,
    opened: AtomicUsize,
    answered: AtomicUsize,
    tasks: Mutex<Vec<tokio::task::AbortHandle>>,
}

impl RawShared {
    fn track(&self, h: &tokio::task::JoinHandle<()>) {
        self.tasks.lock().unwrap().push(h.abort_handle());
    }
}

impl RawSdk {
    async fn spawn(mode: ProtocolMode, handler: &'static str, reset_first: bool) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let shared = Arc::new(RawShared {
            mode,
            handler,
            reset_first,
            opened: AtomicUsize::new(0),
            answered: AtomicUsize::new(0),
            tasks: Mutex::new(Vec::new()),
        });
        let sh = Arc::clone(&shared);
        let accept = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let h = tokio::spawn(serve_conn(socket, Arc::clone(&sh)));
                sh.track(&h);
            }
        });
        shared.track(&accept);
        Ok(Self { url, shared })
    }

    /// h2 invoke streams opened to the SDK.
    fn opened(&self) -> usize {
        self.shared.opened.load(Ordering::SeqCst)
    }

    /// Streams that got a result — the number of times "the handler ran".
    fn answered(&self) -> usize {
        self.shared.answered.load(Ordering::SeqCst)
    }

    /// Dead for good: the listener closes (new connections are refused), open connections are cut.
    fn kill(&self) {
        for h in self.shared.tasks.lock().unwrap().drain(..) {
            h.abort();
        }
    }
}

impl Drop for RawSdk {
    fn drop(&mut self) {
        self.kill();
    }
}

async fn serve_conn(socket: tokio::net::TcpStream, sh: Arc<RawShared>) {
    // h2c speaks first with the `PRI * HTTP/2.0` preface; h1 opens with a method name.
    let mut first = [0u8; 1];
    match socket.peek(&mut first).await {
        Ok(1) if first[0] == b'P' => serve_h2(socket, sh).await,
        Ok(1) => serve_h1(socket, &sh).await,
        _ => {}
    }
}

async fn serve_h1(mut socket: tokio::net::TcpStream, sh: &RawShared) {
    let mut head = Vec::new();
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        let mut chunk = [0u8; 1024];
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => head.extend_from_slice(&chunk[..n]),
        }
    }
    if !head.starts_with(b"GET /discover") {
        return;
    }
    let manifest = EndpointManifest {
        protocol_mode: sh.mode,
        min_protocol_version: 1,
        max_protocol_version: 1,
        services: vec![ServiceManifest {
            name: "Svc".into(),
            ty: ServiceType::Service,
            handlers: vec![HandlerManifest {
                name: sh.handler.into(),
                ty: None,
                contract: Default::default(),
            }],
            contract: Default::default(),
        }],
    };
    let body = serde_json::to_string(&manifest).expect("manifest serializes");
    let reply = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: {DISCOVERY_V1}\r\ncontent-length: {}\r\n\
         connection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = socket.write_all(reply.as_bytes()).await;
    let _ = socket.shutdown().await;
}

async fn serve_h2(socket: tokio::net::TcpStream, sh: Arc<RawShared>) {
    let Ok(mut conn) = h2::server::handshake(socket).await else {
        return;
    };
    // The connection must keep being polled for frames of new streams to flow.
    while let Some(Ok((req, respond))) = conn.accept().await {
        let n = sh.opened.fetch_add(1, Ordering::SeqCst);
        let h = tokio::spawn(serve_stream(req, respond, n, Arc::clone(&sh)));
        sh.track(&h);
    }
}

async fn serve_stream(
    req: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    n: usize,
    sh: Arc<RawShared>,
) {
    let resp = http::Response::builder()
        .status(200)
        .header("content-type", INVOCATION_V1)
        .body(())
        .expect("valid header");
    let Ok(mut send) = respond.send_response(resp, false) else {
        return;
    };
    if n == 0 && sh.reset_first {
        // The runtime hits a transport error in `send()` or while reading the response body.
        send.send_reset(h2::Reason::INTERNAL_ERROR);
        return;
    }
    sh.answered.fetch_add(1, Ordering::SeqCst);
    let out = encode_stream(&[
        Message::Output(pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from_static(b"2")),
            )),
            name: String::new(),
        }),
        Message::End(pb::EndMessage {}),
    ]);
    if send.send_data(out, true).is_err() {
        return;
    }
    // Drain the request body until the runtime closes its half (releasing the stream cleanly).
    let mut body = req.into_body();
    while let Some(Ok(chunk)) = body.data().await {
        let _ = body.flow_control().release_capacity(chunk.len());
    }
}
