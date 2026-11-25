// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! gzip compression of `REQUEST_RESPONSE` bodies, as in Restate's service protocol.
//!
//! A hand-written h1 fake SDK (like `sdk_bytes.rs`): it declares or does not
//! declare `requestCompression: "gzip"` in its manifest, records the headers of
//! every turn, decompresses the body when it has `content-encoding: gzip`, and
//! answers with a plain response, gzip when offered, gzip even when not offered,
//! or a "zip bomb".
//!
//! Must see:
//! - SDK declares it ⇒ every turn has `accept-encoding: gzip`; a body at or
//!   above the threshold goes compressed and the bytes on the wire are fewer than
//!   the plain body; the deployment records `rrCompression`.
//! - SDK does not declare it, or the node runs `--deploy-no-rr-compression` ⇒ no
//!   header at all, plain body (the behaviour before compression existed).
//! - gzip response when offered ⇒ decoded correctly, the invocation completes.
//! - gzip response when not offered ⇒ the turn fails, the message names
//!   `content-encoding`.
//! - A response that decompresses past the body limit ⇒ the turn fails with
//!   "exceeds the limit", without buffering it all in RAM.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    bytes::Bytes,
    cairn_protocol::{pb, Message},
    cairn_server::node::NodeMode,
    cairn_server::{
        discovery::{
            EndpointManifest, HandlerManifest, ProtocolMode, ServiceManifest, ServiceType,
            DISCOVERY_V1,
        },
        invoker::{decode_stream, encode_stream, INVOCATION_V1},
    },
    common::{invocation, invoke, register, send, within, TEST_DEADLINE},
    std::{
        io::{Read, Write},
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
    tokio::io::{AsyncReadExt, AsyncWriteExt},
};

/// Number of `ctx.run` per invocation.
const N: u32 = 4;
/// Result bytes of each `ctx.run` — compressible (one repeated character), and
/// from the second turn on the body exceeds the 8 KiB compression threshold.
const PAYLOAD: usize = 16 << 10;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Reply {
    /// Plain body, no header.
    Plain,
    /// gzip when the request has `accept-encoding: gzip`.
    GzipWhenOffered,
    /// gzip even when not offered.
    GzipAlways,
    /// 64 MiB of zeros compressed (≈ 64 KB on the wire), `content-encoding: gzip`.
    Bomb,
}

struct Sdk {
    url: String,
    shared: Arc<Shared>,
}

struct Shared {
    service: &'static str,
    declare: bool,
    reply: Reply,
    turns: AtomicU64,
    /// Turns with `accept-encoding: gzip`.
    offered: AtomicU64,
    /// Turns whose request body has `content-encoding: gzip`.
    compressed: AtomicU64,
    /// Request body bytes on the wire / after decompression.
    wire: AtomicU64,
    raw: AtomicU64,
    tasks: Mutex<Vec<tokio::task::AbortHandle>>,
}

impl Sdk {
    async fn spawn(service: &'static str, declare: bool, reply: Reply) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let shared = Arc::new(Shared {
            service,
            declare,
            reply,
            turns: AtomicU64::new(0),
            offered: AtomicU64::new(0),
            compressed: AtomicU64::new(0),
            wire: AtomicU64::new(0),
            raw: AtomicU64::new(0),
            tasks: Mutex::new(Vec::new()),
        });
        let sh = Arc::clone(&shared);
        let accept = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let h = tokio::spawn(serve_h1(socket, Arc::clone(&sh)));
                sh.tasks.lock().unwrap().push(h.abort_handle());
            }
        });
        shared.tasks.lock().unwrap().push(accept.abort_handle());
        Ok(Self { url, shared })
    }

    fn n(&self, a: &AtomicU64) -> u64 {
        a.load(Ordering::SeqCst)
    }
}

impl Drop for Sdk {
    fn drop(&mut self) {
        for h in self.shared.tasks.lock().unwrap().drain(..) {
            h.abort();
        }
    }
}

fn manifest(service: &str, declare: bool) -> String {
    let mut v = serde_json::to_value(EndpointManifest {
        protocol_mode: ProtocolMode::RequestResponse,
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
    .expect("manifest");
    if declare {
        v["requestCompression"] = "gzip".into();
    }
    v.to_string()
}

fn gzip(b: &[u8]) -> Vec<u8> {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    e.write_all(b).expect("compress into RAM");
    e.finish().expect("compress into RAM")
}

fn gunzip(b: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    flate2::read::MultiGzDecoder::new(b)
        .read_to_end(&mut out)
        .expect("valid gzip");
    out
}

/// Journal has `k` results ⇒ run the `k + 1`-th and suspend; all `N` ⇒ `Output`.
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
        return encode_stream(&[
            Message::Output(pb::OutputCommandMessage {
                result: Some(pb::output_command_message::Result::Value(
                    cairn_protocol::value::inline(Bytes::from_static(b"\"ok\"")),
                )),
                name: String::new(),
            }),
            Message::End(pb::EndMessage {}),
        ]);
    }
    let cid = done + 1;
    encode_stream(&[
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
        Message::Suspension(pb::SuspensionMessage {
            waiting_completions: vec![cid],
            ..Default::default()
        }),
    ])
}

fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.trim().eq_ignore_ascii_case(name).then(|| v.trim())
    })
}

async fn serve_h1(mut socket: tokio::net::TcpStream, sh: Arc<Shared>) {
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
    let mut extra = String::new();
    let (ctype, body) = if head.starts_with("GET /discover") {
        (DISCOVERY_V1, Bytes::from(manifest(sh.service, sh.declare)))
    } else if head.starts_with(&format!("POST /invoke/{}/run", sh.service)) {
        let len: usize = header(&head, "content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        while buf.len() < head_end + len {
            let mut chunk = [0u8; 4096];
            match socket.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
        let wire = &buf[head_end..head_end + len];
        sh.turns.fetch_add(1, Ordering::SeqCst);
        sh.wire.fetch_add(len as u64, Ordering::SeqCst);
        let offered = header(&head, "accept-encoding").is_some_and(|v| v.contains("gzip"));
        if offered {
            sh.offered.fetch_add(1, Ordering::SeqCst);
        }
        let raw = if header(&head, "content-encoding") == Some("gzip") {
            sh.compressed.fetch_add(1, Ordering::SeqCst);
            gunzip(wire)
        } else {
            wire.to_vec()
        };
        sh.raw.fetch_add(raw.len() as u64, Ordering::SeqCst);
        let reply = rr_turn(Bytes::from(raw));
        let reply = match sh.reply {
            Reply::Plain => reply,
            Reply::GzipWhenOffered if !offered => reply,
            Reply::GzipWhenOffered | Reply::GzipAlways => {
                extra.push_str("content-encoding: gzip\r\n");
                Bytes::from(gzip(&reply))
            }
            Reply::Bomb => {
                extra.push_str("content-encoding: gzip\r\n");
                Bytes::from(gzip(&vec![0u8; 64 << 20]))
            }
        };
        (INVOCATION_V1, reply)
    } else {
        return;
    };
    let reply = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: {ctype}\r\n{extra}content-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    let _ = socket.write_all(reply.as_bytes()).await;
    let _ = socket.write_all(&body).await;
    let _ = socket.shutdown().await;
}

/// Send one invocation and wait until `lastFailure.message` contains `want`.
async fn fails_with(node: &str, sdk: &Sdk, want: &str) -> Result<()> {
    let (code, v) = send(node, sdk.shared.service, "run", None, "{}").await?;
    ensure!(code == 200 || code == 202, "send: {code} {v}");
    let id = v["id"]
        .as_str()
        .or_else(|| v["invocationId"].as_str())
        .ok_or_else(|| anyhow!("no id: {v}"))?
        .to_string();
    within(TEST_DEADLINE, "turn failed for the right reason", async {
        loop {
            let (_, st) = invocation(node, &id).await?;
            let msg = st["lastFailure"]["message"].as_str().unwrap_or("");
            if msg.contains(want) {
                ensure!(st["status"] != "completed", "{st}");
                return Ok(());
            }
            ensure!(st["status"] != "completed", "completed, must fail: {st}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rr_bodies_are_gzipped_when_the_sdk_declares() -> Result<()> {
    let node = common::start_single_process(1).await?;
    let url = node.url();
    let r = async {
        let sdk = Sdk::spawn("Gz", true, Reply::GzipWhenOffered).await?;
        let dep = register(&url, &sdk.url).await?;
        ensure!(dep["rrCompression"] == "gzip", "deployment: {dep}");
        let (code, out) = invoke(&url, "Gz", "run", None, "{}").await?;
        ensure!(code == 200, "invoke {code} {out}");
        let sh = &sdk.shared;
        let turns = sdk.n(&sh.turns);
        ensure!(turns == u64::from(N) + 1, "turns: {turns}");
        ensure!(sdk.n(&sh.offered) == turns, "every turn offers gzip");
        // The first turn (journal has no result yet) is below the threshold; later ones exceed it.
        ensure!(
            sdk.n(&sh.compressed) == turns - 1,
            "compressed turns: {}",
            sdk.n(&sh.compressed)
        );
        let (wire, raw) = (sdk.n(&sh.wire), sdk.n(&sh.raw));
        ensure!(wire * 10 < raw, "bytes on the wire {wire} vs body {raw}");
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn undeclared_sdk_gets_plain_bodies() -> Result<()> {
    let node = common::start_single_process(1).await?;
    let url = node.url();
    let r = async {
        let sdk = Sdk::spawn("Plain", false, Reply::Plain).await?;
        let dep = register(&url, &sdk.url).await?;
        ensure!(dep.get("rrCompression").is_none(), "deployment: {dep}");
        let (code, out) = invoke(&url, "Plain", "run", None, "{}").await?;
        ensure!(code == 200, "invoke {code} {out}");
        let sh = &sdk.shared;
        ensure!(sdk.n(&sh.offered) == 0 && sdk.n(&sh.compressed) == 0);
        ensure!(sdk.n(&sh.wire) == sdk.n(&sh.raw));
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uninvited_gzip_reply_fails_the_turn() -> Result<()> {
    let node = common::start_single_process(1).await?;
    let url = node.url();
    let r = async {
        let sdk = Sdk::spawn("Rude", false, Reply::GzipAlways).await?;
        register(&url, &sdk.url).await?;
        fails_with(&url, &sdk, "content-encoding").await
    }
    .await;
    node.shutdown().await;
    r
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gzip_bomb_reply_is_capped() -> Result<()> {
    let node = common::start_single_process(1).await?;
    let url = node.url();
    let r = async {
        let sdk = Sdk::spawn("Bomb", true, Reply::Bomb).await?;
        register(&url, &sdk.url).await?;
        fails_with(&url, &sdk, "exceeds the limit").await
    }
    .await;
    node.shutdown().await;
    r
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operator_switch_keeps_bodies_plain() -> Result<()> {
    let mut cfg = common::test_config(NodeMode::single(1));
    cfg.deploy_no_rr_compression = true;
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let r = async {
        let sdk = Sdk::spawn("Off", true, Reply::GzipWhenOffered).await?;
        let dep = register(&url, &sdk.url).await?;
        ensure!(dep.get("rrCompression").is_none(), "deployment: {dep}");
        let (code, out) = invoke(&url, "Off", "run", None, "{}").await?;
        ensure!(code == 200, "invoke {code} {out}");
        let sh = &sdk.shared;
        ensure!(sdk.n(&sh.offered) == 0 && sdk.n(&sh.compressed) == 0);
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}
