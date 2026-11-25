// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The cancel bell of a running turn (`turn/live.rs`).
//!
//! A running turn holds the invocation lock until it stops: a whole `BIDI_STREAM`
//! session, or a chain of `REQUEST_RESPONSE` turns resolved in place (one turn per
//! `ctx.run`, up to `MAX_TURNS`). A cooperative cancel (`cancel?mode=signal`) that
//! only took the lock path would reach the SDK once the work is done. Here the fake
//! SDK **never stops on its own**: it only ends when it sees CANCEL — so CANCEL must
//! arrive mid-turn.
//!
//! A hand-written fake SDK: h1 for RR's `/discover` and `/invoke`, h2c for BIDI (as in
//! `streams.rs`).

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
    bytes::{Bytes, BytesMut},
    cairn_protocol::{pb, FrameDecoder, Message},
    cairn_server::{
        discovery::{
            EndpointManifest, HandlerManifest, ProtocolMode, ServiceManifest, ServiceType,
            DISCOVERY_V1,
        },
        invoker::{decode_stream, encode_stream, INVOCATION_V1},
    },
    common::{register, send, start_single_process, wait_status, within, TEST_DEADLINE},
    std::{
        sync::{Arc, Mutex},
        time::Duration,
    },
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        sync::mpsc,
    },
};

// ───────────────────────────────────────────────────────── Fake SDK

/// One SDK turn seen: the turn number (from 1), and whether its journal has a CANCEL yet.
#[derive(Debug, Clone, Copy)]
struct Seen {
    turn: usize,
    cancel: bool,
}

struct Sdk {
    url: String,
    shared: Arc<Shared>,
}

struct Shared {
    mode: ProtocolMode,
    /// RR: each permit lets one turn that has not seen CANCEL answer (closure "finished").
    release: tokio::sync::Semaphore,
    turns: Mutex<usize>,
    seen: mpsc::UnboundedSender<Seen>,
    tasks: Mutex<Vec<tokio::task::AbortHandle>>,
}

impl Sdk {
    async fn spawn(mode: ProtocolMode) -> Result<(Self, mpsc::UnboundedReceiver<Seen>)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let (tx, rx) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared {
            mode,
            release: tokio::sync::Semaphore::new(0),
            turns: Mutex::new(0),
            seen: tx,
            tasks: Mutex::new(Vec::new()),
        });
        let sh = Arc::clone(&shared);
        let accept = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let h = tokio::spawn(serve_conn(socket, Arc::clone(&sh)));
                sh.tasks.lock().unwrap().push(h.abort_handle());
            }
        });
        shared.tasks.lock().unwrap().push(accept.abort_handle());
        Ok((Self { url, shared }, rx))
    }

    fn release(&self) {
        self.shared.release.add_permits(1);
    }
}

impl Drop for Sdk {
    fn drop(&mut self) {
        for h in self.shared.tasks.lock().unwrap().drain(..) {
            h.abort();
        }
    }
}

impl Shared {
    fn saw(&self, cancel: bool) -> Seen {
        let mut n = self.turns.lock().unwrap();
        *n += 1;
        let s = Seen { turn: *n, cancel };
        let _ = self.seen.send(s);
        s
    }
}

fn manifest(mode: ProtocolMode) -> String {
    serde_json::to_string(&EndpointManifest {
        protocol_mode: mode,
        min_protocol_version: 1,
        max_protocol_version: 4,
        services: vec![ServiceManifest {
            name: "Agent".into(),
            ty: ServiceType::Service,
            handlers: vec![HandlerManifest {
                name: "loop".into(),
                ty: None,
                contract: Default::default(),
            }],
            contract: Default::default(),
        }],
    })
    .expect("manifest")
}

fn is_cancel(m: &Message) -> bool {
    matches!(
        m,
        Message::Signal(pb::SignalNotificationMessage {
            signal_id: Some(pb::signal_notification_message::SignalId::Idx(i)),
            ..
        }) if *i == pb::BuiltInSignal::Cancel as u32
    )
}

/// End like a real SDK does when cancelled: `Output` failure 409 then `End`.
fn cancelled() -> [Message; 2] {
    [
        Message::Output(pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Failure(pb::Failure {
                code: 409,
                message: "cancelled".into(),
                metadata: vec![],
            })),
            name: String::new(),
        }),
        Message::End(pb::EndMessage {}),
    ]
}

fn run(cid: u32) -> Message {
    Message::Run(pb::RunCommandMessage {
        result_completion_id: cid,
        name: "a".into(),
    })
}

/// RR: replay the journal; a CANCEL ⇒ end with 409. None yet ⇒ wait for a `release`
/// permit, then add a `ctx.run` that already has its result and suspend waiting for
/// it — which the runtime resolves in place, so the chain of turns never stops on its own.
async fn rr_turn(body: Bytes, sh: &Shared) -> Bytes {
    let msgs: Vec<Message> = decode_stream(body)
        .expect("valid frames")
        .iter()
        .map(|f| cairn_protocol::decode(f).expect("decodable"))
        .collect();
    let seen = sh.saw(msgs.iter().any(is_cancel));
    if seen.cancel {
        return encode_stream(&cancelled());
    }
    if let Ok(p) = sh.release.acquire().await {
        p.forget();
    }
    let cid = 1 + msgs.iter().filter(|m| matches!(m, Message::Run(_))).count() as u32;
    encode_stream(&[
        run(cid),
        Message::ProposeRunCompletion(pb::ProposeRunCompletionMessage {
            result_completion_id: cid,
            result: Some(pb::propose_run_completion_message::Result::Value(
                Bytes::from_static(b"1"),
            )),
        }),
        Message::Suspension(pb::SuspensionMessage {
            waiting_completions: vec![cid],
            ..Default::default()
        }),
    ])
}

async fn serve_conn(socket: tokio::net::TcpStream, sh: Arc<Shared>) {
    // h2c opens with `PRI * HTTP/2.0`; `POST` also starts with `P` — look at all 4 bytes.
    let mut first = [0u8; 4];
    let mut n = 0;
    while n < 4 {
        match socket.peek(&mut first).await {
            Ok(0) | Err(_) => return,
            Ok(k) => n = k,
        }
        if n < 4 {
            tokio::task::yield_now().await;
        }
    }
    if &first == b"PRI " {
        serve_h2(socket, sh).await
    } else {
        serve_h1(socket, &sh).await
    }
}

/// h1: `/discover`, and RR's `/invoke` (the whole body at once, `content-length`).
async fn serve_h1(mut socket: tokio::net::TcpStream, sh: &Shared) {
    let mut buf = Vec::new();
    let head_end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
        let mut chunk = [0u8; 4096];
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let (ctype, body) = if head.starts_with("GET /discover") {
        (DISCOVERY_V1, Bytes::from(manifest(sh.mode)))
    } else if head.starts_with("POST /invoke/Agent/loop") {
        let len: usize = head
            .lines()
            .find_map(|l| {
                l.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|v| v.trim().parse().unwrap_or(0))
            })
            .unwrap_or(0);
        while buf.len() < head_end + len {
            let mut chunk = [0u8; 4096];
            match socket.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
        let req = Bytes::copy_from_slice(&buf[head_end..head_end + len]);
        (INVOCATION_V1, rr_turn(req, sh).await)
    } else {
        return;
    };
    let reply = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: {ctype}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    let _ = socket.write_all(reply.as_bytes()).await;
    let _ = socket.write_all(&body).await;
    let _ = socket.shutdown().await;
}

async fn serve_h2(socket: tokio::net::TcpStream, sh: Arc<Shared>) {
    let Ok(mut conn) = h2::server::handshake(socket).await else {
        return;
    };
    while let Some(Ok((req, respond))) = conn.accept().await {
        let h = tokio::spawn(bidi_stream(req, respond, Arc::clone(&sh)));
        sh.tasks.lock().unwrap().push(h.abort_handle());
    }
}

/// BIDI: open `ctx.run("a")` (just `Run`, closure "running") then read the runtime's
/// stream; end only on seeing CANCEL.
async fn bidi_stream(
    req: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    sh: Arc<Shared>,
) {
    let resp = http::Response::builder()
        .status(200)
        .header("content-type", INVOCATION_V1)
        .body(())
        .expect("valid header");
    let Ok(mut out) = respond.send_response(resp, false) else {
        return;
    };
    if out.send_data(encode_stream(&[run(1)]), false).is_err() {
        return;
    }
    let mut body = req.into_body();
    let dec = FrameDecoder::new();
    let mut buf = BytesMut::new();
    // The first turn = the journal replay (`Start` + `Input`); every CANCEL arriving
    // later is a new `Seen`.
    sh.saw(false);
    while let Some(Ok(chunk)) = body.data().await {
        let _ = body.flow_control().release_capacity(chunk.len());
        buf.extend_from_slice(&chunk);
        while let Ok(Some(f)) = dec.decode(&mut buf) {
            let m = cairn_protocol::decode(&f).expect("decodable");
            if is_cancel(&m) {
                sh.saw(true);
                let _ = out.send_data(encode_stream(&cancelled()), true);
                return;
            }
        }
    }
}

// ───────────────────────────────────────────────────────── helpers

async fn journal_types(node: &str, id: &str) -> Result<Vec<String>> {
    let j: serde_json::Value = reqwest::get(format!("{node}/invocations/{id}/journal"))
        .await?
        .json()
        .await?;
    Ok(j.as_array()
        .ok_or_else(|| anyhow!("journal is not an array: {j}"))?
        .iter()
        .map(|e| e["ty"].as_str().unwrap_or_default().to_string())
        .collect())
}

/// `POST cancel?mode=signal` in a separate task: it takes the lock path, so it only
/// answers once the running turn has released.
fn cancel_signal(node: &str, id: &str) -> tokio::task::JoinHandle<Result<u16>> {
    let url = format!("{node}/invocations/{id}/cancel?mode=signal");
    tokio::spawn(async move {
        Ok(reqwest::Client::new()
            .post(url)
            .send()
            .await?
            .status()
            .as_u16())
    })
}

async fn next_seen(rx: &mut mpsc::UnboundedReceiver<Seen>, what: &str) -> Result<Seen> {
    within(TEST_DEADLINE, what, async {
        rx.recv()
            .await
            .ok_or_else(|| anyhow!("fake SDK closed the channel before {what}"))
    })
    .await
}

// ───────────────────────────────────────────────────────── test

/// **`BIDI_STREAM`: CANCEL reaches a running closure through the open stream.**
///
/// 1. The SDK opens `ctx.run("a")` (just `Run`) then reads the stream — the closure
///    is "running", the runtime waits for the next frame (`drive_bidi`), holding the
///    invocation lock.
/// 2. The test sends `cancel?mode=signal`: the bell rings, `drive_bidi` wakes in the
///    middle of its wait, writes CANCEL to the journal and pushes it down the stream.
/// 3. The SDK sees CANCEL, ends with `Output` failure 409.
///
/// Must see: the SDK receives CANCEL while the turn is still open (without the bell
/// the turn never stops, CANCEL never arrives); the invocation is `failed`; the
/// journal has **exactly one** CANCEL — the lock path's delivery, when the turn
/// releases, sees the identity already there.
#[tokio::test]
async fn bidi_cancel_reaches_the_open_stream() -> Result<()> {
    let node = start_single_process(1).await?;
    let (sdk, mut seen) = Sdk::spawn(ProtocolMode::BidiStream).await?;
    register(&node.url(), &sdk.url).await?;
    let (code, r) = send(&node.url(), "Agent", "loop", None, "null").await?;
    ensure!(code < 300, "{code} {r}");
    let id = r["id"].as_str().unwrap().to_string();

    let first = next_seen(&mut seen, "the SDK opens a turn").await?;
    ensure!(!first.cancel, "{first:?}");
    let asked = cancel_signal(&node.url(), &id);
    let got = next_seen(&mut seen, "CANCEL reaches the open stream").await?;
    ensure!(got.cancel, "{got:?}");

    within(
        TEST_DEADLINE,
        "failed",
        wait_status(&node.url(), &id, "failed"),
    )
    .await?;
    let code = within(TEST_DEADLINE, "cancel answers", async { asked.await? }).await?;
    ensure!((200..300).contains(&code), "cancel returned {code}");
    let types = journal_types(&node.url(), &id).await?;
    let cancels = types.iter().filter(|t| *t == "0xFBFF").count();
    assert_eq!(cancels, 1, "journal must have one CANCEL: {types:?}");
    drop(sdk);
    node.shutdown().await;
    Ok(())
}

/// **`REQUEST_RESPONSE`: CANCEL enters the journal before the next turn of the chain.**
///
/// 1. Every SDK turn adds a `ctx.run` that already has its result and suspends waiting
///    for it — the runtime resolves it in place and calls again at once (`drive_one`),
///    holding the lock for the whole chain. Each turn only answers when the test
///    releases a permit.
/// 2. Turn 1 waits; the test sends `cancel?mode=signal` (the bell rings).
/// 3. The test releases turn after turn, every 100 ms, at most 20 turns.
///
/// Must see: one of those 20 turns replays a journal with a CANCEL (without the bell
/// CANCEL would only enter when the chain hits `MAX_TURNS` = 256); the SDK ends with
/// 409, the invocation is `failed`; the journal has **exactly one** CANCEL.
#[tokio::test]
async fn rr_cancel_lands_before_the_next_turn_of_the_chain() -> Result<()> {
    let node = start_single_process(1).await?;
    let (sdk, mut seen) = Sdk::spawn(ProtocolMode::RequestResponse).await?;
    register(&node.url(), &sdk.url).await?;
    let (code, r) = send(&node.url(), "Agent", "loop", None, "null").await?;
    ensure!(code < 300, "{code} {r}");
    let id = r["id"].as_str().unwrap().to_string();

    let first = next_seen(&mut seen, "turn 1").await?;
    ensure!(!first.cancel, "{first:?}");
    let asked = cancel_signal(&node.url(), &id);
    let mut hit = None;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        sdk.release();
        let s = next_seen(&mut seen, "next turn").await?;
        if s.cancel {
            hit = Some(s);
            break;
        }
    }
    let hit = hit.ok_or_else(|| anyhow!("20 turns after the cancel, no CANCEL in the journal"))?;
    eprintln!("CANCEL arrived at turn {}", hit.turn);

    within(
        TEST_DEADLINE,
        "failed",
        wait_status(&node.url(), &id, "failed"),
    )
    .await?;
    let code = within(TEST_DEADLINE, "cancel answers", async { asked.await? }).await?;
    ensure!((200..300).contains(&code), "cancel returned {code}");
    let types = journal_types(&node.url(), &id).await?;
    let cancels = types.iter().filter(|t| *t == "0xFBFF").count();
    assert_eq!(cancels, 1, "journal must have one CANCEL: {types:?}");
    drop(sdk);
    node.shutdown().await;
    Ok(())
}
