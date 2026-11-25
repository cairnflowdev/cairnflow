// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! HTTP body bytes exchanged with the customer's endpoint, by mode —
//! `cairnflow_sdk_bytes_total`.
//!
//! One invocation with `N` `ctx.run`s, each result `PAYLOAD` bytes, in each
//! mode. A hand-written fake SDK (h1 for `/discover` and `REQUEST_RESPONSE`, h2c
//! for `BIDI_STREAM`, like `cancel_live.rs`) adds up **exactly** the body bytes
//! it receives and sends; the node's metric must equal those two numbers. A
//! single test in the binary: the metric is a process-wide counter, parallel
//! tests would add into each other.
//!
//! Must see: `to_sdk`/`from_sdk` of each mode match the bytes the SDK sees, and
//! `REQUEST_RESPONSE` sends much more down than `BIDI_STREAM` (the journal is
//! replayed every turn — exactly what this measures). Per-tenant metering
//! (`sdkBytesOut`/`sdkBytesIn`) adds up exactly to the metric's total.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{ensure, Result},
    bytes::{Bytes, BytesMut},
    cairn_protocol::{pb, FrameDecoder, Message},
    cairn_server::{
        discovery::{
            EndpointManifest, HandlerManifest, ProtocolMode, ServiceManifest, ServiceType,
            DISCOVERY_V1,
        },
        invoker::{decode_stream, encode_stream, INVOCATION_V1},
    },
    common::{invoke, metric, register, within, TEST_DEADLINE},
    std::{
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
    tokio::io::{AsyncReadExt, AsyncWriteExt},
};

/// Number of `ctx.run` per invocation.
const N: u32 = 5;
/// Result bytes of each `ctx.run`.
const PAYLOAD: usize = 1024;

// ───────────────────────────────────────────────────────── fake SDK

struct Sdk {
    url: String,
    shared: Arc<Shared>,
}

struct Shared {
    mode: ProtocolMode,
    service: &'static str,
    /// Body bytes the runtime sent down that the SDK has read.
    received: AtomicU64,
    /// Body bytes the SDK sent up.
    sent: AtomicU64,
    tasks: Mutex<Vec<tokio::task::AbortHandle>>,
}

impl Sdk {
    async fn spawn(mode: ProtocolMode, service: &'static str) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let shared = Arc::new(Shared {
            mode,
            service,
            received: AtomicU64::new(0),
            sent: AtomicU64::new(0),
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
        Ok(Self { url, shared })
    }

    fn seen(&self) -> (u64, u64) {
        (
            self.shared.received.load(Ordering::SeqCst),
            self.shared.sent.load(Ordering::SeqCst),
        )
    }
}

impl Drop for Sdk {
    fn drop(&mut self) {
        for h in self.shared.tasks.lock().unwrap().drain(..) {
            h.abort();
        }
    }
}

fn manifest(mode: ProtocolMode, service: &str) -> String {
    serde_json::to_string(&EndpointManifest {
        protocol_mode: mode,
        min_protocol_version: 1,
        max_protocol_version: 4,
        services: vec![ServiceManifest {
            name: service.into(),
            ty: ServiceType::Service,
            handlers: vec![HandlerManifest {
                name: "run".into(),
                ty: None,
                contract: Default::default(),
            }],
            contract: Default::default(),
        }],
    })
    .expect("manifest")
}

fn run_and_propose(cid: u32) -> [Message; 2] {
    [
        Message::Run(pb::RunCommandMessage {
            result_completion_id: cid,
            name: format!("r{cid}"),
        }),
        Message::ProposeRunCompletion(pb::ProposeRunCompletionMessage {
            result_completion_id: cid,
            result: Some(pb::propose_run_completion_message::Result::Value(
                Bytes::from(vec![b'x'; PAYLOAD]),
            )),
        }),
    ]
}

fn finish() -> [Message; 2] {
    [
        Message::Output(pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from_static(b"\"ok\"")),
            )),
            name: String::new(),
        }),
        Message::End(pb::EndMessage {}),
    ]
}

/// RR: the replayed journal has `k` `ctx.run` results ⇒ run the `k + 1`-th and
/// suspend waiting for it (the runtime resolves it in place, the next turn
/// carries a longer journal); all `N` ⇒ `Output` + `End`.
fn rr_turn(body: Bytes) -> Bytes {
    let msgs: Vec<Message> = decode_stream(body)
        .expect("valid message frames")
        .iter()
        .map(|f| cairn_protocol::decode(f).expect("decodable"))
        .collect();
    let done = msgs
        .iter()
        .filter(|m| matches!(m, Message::RunCompletion(_)))
        .count() as u32;
    if done >= N {
        return encode_stream(&finish());
    }
    let cid = done + 1;
    let mut out = run_and_propose(cid).to_vec();
    out.push(Message::Suspension(pb::SuspensionMessage {
        waiting_completions: vec![cid],
        ..Default::default()
    }));
    encode_stream(&out)
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
        (DISCOVERY_V1, Bytes::from(manifest(sh.mode, sh.service)))
    } else if head.starts_with(&format!("POST /invoke/{}/run", sh.service)) {
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
        sh.received.fetch_add(len as u64, Ordering::SeqCst);
        let reply = rr_turn(req);
        sh.sent.fetch_add(reply.len() as u64, Ordering::SeqCst);
        (INVOCATION_V1, reply)
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

/// BIDI: like a real SDK, each `ctx.run` sends `Run` + `ProposeRunCompletion`
/// and waits for its `RunCompletion` on the stream; all `N` ⇒ `Output` + `End`
/// and close its own half, then read the rest until the runtime closes its half.
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
    let mut body = req.into_body();
    let dec = FrameDecoder::new();
    let mut buf = BytesMut::new();
    let mut push = |msgs: &[Message], end: bool| {
        let b = encode_stream(msgs);
        sh.sent.fetch_add(b.len() as u64, Ordering::SeqCst);
        out.send_data(b, end).is_ok()
    };
    let mut completed = 0u32;
    let mut asked = 0u32;
    loop {
        // Run the next `ctx.run` once the previous one has its result.
        if asked == completed {
            if completed >= N {
                if !push(&finish(), true) {
                    return;
                }
                break;
            }
            asked += 1;
            if !push(&run_and_propose(asked), false) {
                return;
            }
        }
        let Some(Ok(chunk)) = body.data().await else {
            return;
        };
        let _ = body.flow_control().release_capacity(chunk.len());
        sh.received.fetch_add(chunk.len() as u64, Ordering::SeqCst);
        buf.extend_from_slice(&chunk);
        while let Ok(Some(f)) = dec.decode(&mut buf) {
            if let Message::RunCompletion(_) = cairn_protocol::decode(&f).expect("valid msg frame")
            {
                completed += 1;
            }
        }
    }
    while let Some(Ok(chunk)) = body.data().await {
        let _ = body.flow_control().release_capacity(chunk.len());
        sh.received.fetch_add(chunk.len() as u64, Ordering::SeqCst);
    }
}

// ───────────────────────────────────────────────────────── test

async fn sdk_bytes(node: &str, mode: &str) -> Result<(u64, u64)> {
    let sample = |dir: &str| format!("cairnflow_sdk_bytes_total{{mode=\"{mode}\",dir=\"{dir}\"}}");
    Ok((
        metric(node, &sample("to_sdk")).await?,
        metric(node, &sample("from_sdk")).await?,
    ))
}

/// Run one invocation on `sdk`, then wait (with a limit) until the mode's metric
/// has grown by exactly the bytes the SDK saw. Returns (to_sdk, from_sdk).
async fn one_mode(node: &str, sdk: &Sdk, mode: &str) -> Result<(u64, u64)> {
    let before = sdk_bytes(node, mode).await?;
    let (code, out) = invoke(node, sdk.shared.service, "run", None, "{}").await?;
    ensure!(code == 200, "{mode}: invoke {code} {out}");
    within(TEST_DEADLINE, "byte metric == SDK bytes", async {
        loop {
            let now = sdk_bytes(node, mode).await?;
            let got = (now.0 - before.0, now.1 - before.1);
            if got == sdk.seen() {
                return Ok(got);
            }
            // A polling beat inside a bounded loop: the last bytes may still be on their way.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_bytes_match_bodies_in_both_modes() -> Result<()> {
    let node = common::start_single_process(1).await?;
    let url = node.url();
    let r = async {
        let bidi = Sdk::spawn(ProtocolMode::BidiStream, "BytesBidi").await?;
        let rr = Sdk::spawn(ProtocolMode::RequestResponse, "BytesRr").await?;
        register(&url, &bidi.url).await?;
        register(&url, &rr.url).await?;

        let b = one_mode(&url, &bidi, "BIDI_STREAM").await?;
        let r = one_mode(&url, &rr, "REQUEST_RESPONSE").await?;
        // Each mode sees at least N results going up.
        let up = (N as u64) * PAYLOAD as u64;
        ensure!(
            b.1 >= up && r.1 >= up,
            "from_sdk short: bidi {b:?} rr {r:?}"
        );
        // BIDI sends each result down once (notification); RR replays the whole
        // journal every turn: 0 + 1 + … + N results ⇒ ≈ (N + 1) / 2 times BIDI.
        ensure!(
            r.0 > 2 * b.0,
            "RR does not replay the journal? bidi {b:?} rr {r:?}"
        );
        ensure!(b.0 >= up, "BIDI to_sdk lacks notifications: {b:?}");
        // Per-tenant metering = the process's metric total (one test per
        // binary: nobody else adds to it). BIDI is booked when the stream fully closes.
        let want = (b.0 + r.0, b.1 + r.1);
        within(TEST_DEADLINE, "SDK byte metering = metric", async {
            loop {
                let got = cairn_server::metering::snapshot()
                    .values()
                    .fold((0, 0), |(o, i), u| {
                        (o + u.sdk_bytes_out, i + u.sdk_bytes_in)
                    });
                if got == want {
                    return Ok(());
                }
                ensure!(
                    got.0 <= want.0 && got.1 <= want.1,
                    "metering exceeds the metric: {got:?} {want:?}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}
