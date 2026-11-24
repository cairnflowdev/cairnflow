// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Transient streams and SSE: wire v4's `StreamAppend` reaches the range's
//! `StreamHub`, read through `GET /invocations/{id}/streams/{name}` on the
//! leader only.
//!
//! A **hand-written** fake SDK ([`RawSdk`]): h1 for `/discover`, h2c for
//! `BIDI_STREAM` (like `invoke_transient.rs`), and axum h1 for
//! `REQUEST_RESPONSE`. Each handler sends a fixed frame script; `chatty` waits
//! for [`RawSdk::release`] mid-turn — the shape of a `ctx.run` generating tokens.
//!
//! SSE events (the read contract):
//!
//! | `event` | `data` |
//! |---|---|
//! | `record` | `{offset, durable, step, attempt, k, data}` — `id:` is the offset |
//! | `gap` | `{offset, dropped}` |
//! | `superseded` | `{offset, step, attempt}` |
//! | `end` | `{next}` — the stream is sealed and has been read to the end |
//! | `moved` | `{leader}` — the leader's term ended |
//! | `reset` | `{}` — a transient stream is lost with the term: clear what was shown |

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, bail, ensure, Context, Result},
    bytes::{Bytes, BytesMut},
    cairn_protocol::{encode_message, pb, Flags, MessageType},
    cairn_server::{
        discovery::{
            EndpointManifest, HandlerManifest, ProtocolMode, ServiceManifest, ServiceType,
            DISCOVERY_V1,
        },
        invoker::INVOCATION_V1,
    },
    cairn_store::testkit::Op,
    common::{register, send, start_single_process, wait_status, within, TEST_DEADLINE},
    std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
    tokio::io::{AsyncReadExt, AsyncWriteExt},
};

// ───────────────────────────────────────────────────────── SDK frames

fn append_frame(name: &str, step: u32, ks: std::ops::Range<u32>) -> Bytes {
    append_frame_as(name, false, step, ks, 0)
}

/// `StreamAppend` of stream `name`, optional `durable`, each record with `pad`
/// padding bytes in its payload.
fn append_frame_as(
    name: &str,
    durable: bool,
    step: u32,
    ks: std::ops::Range<u32>,
    pad: usize,
) -> Bytes {
    let fill = "x".repeat(pad);
    encode_message(
        MessageType::STREAM_APPEND,
        Flags::NONE,
        &pb::StreamAppendMessage {
            name: name.into(),
            durable,
            records: ks
                .map(|k| pb::StreamRecord {
                    step,
                    attempt: 0,
                    k,
                    payload: Bytes::from(if pad == 0 {
                        format!("{{\"tok\":{k}}}")
                    } else {
                        format!("{{\"tok\":{k},\"pad\":\"{fill}\"}}")
                    }),
                })
                .collect(),
            close: false,
        },
    )
}

/// Artifact content of handler `artifact`.
const ARTIFACT_BODY: &[u8] = b"a 2 MiB mock report";

/// sha256 hex of [`ARTIFACT_BODY`].
fn artifact_sha() -> String {
    use sha2::Digest;
    sha2::Sha256::digest(ARTIFACT_BODY)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// `ArtifactPut` (wire v4) of `content`, with a correct or deliberately wrong
/// sha.
fn artifact_frame(content: &[u8], honest: bool) -> Bytes {
    artifact_frame_typed(content, honest, "text/plain")
}

/// Like [`artifact_frame`], with a content-type the handler picks.
fn artifact_frame_typed(content: &[u8], honest: bool, content_type: &str) -> Bytes {
    use sha2::Digest;
    let mut sha: [u8; 32] = sha2::Sha256::digest(content).into();
    if !honest {
        sha[0] ^= 1;
    }
    encode_message(
        MessageType::ARTIFACT_PUT,
        Flags::NONE,
        &pb::ArtifactPutMessage {
            sha256: Bytes::copy_from_slice(&sha),
            content: Bytes::copy_from_slice(content),
            content_type: content_type.into(),
        },
    )
}

/// An HTML artifact with a script — something a tenant's handler can set freely.
const HTML_ARTIFACT: &[u8] = b"<script>fetch('/admin/keys',{method:'POST'})</script>";

fn output_end() -> Bytes {
    let mut b = BytesMut::new();
    b.extend_from_slice(&encode_message(
        MessageType::OUTPUT,
        Flags::NONE,
        &pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from_static(b"1")),
            )),
            name: "output".into(),
        },
    ));
    b.extend_from_slice(&encode_message(
        MessageType::END,
        Flags::NONE,
        &pb::EndMessage {},
    ));
    b.freeze()
}

/// Durable `StreamAppend` of stream `name`, with the given JSON payloads, `(step
/// 1, attempt 0, k)` in order.
fn append_json(name: &str, records: &[serde_json::Value]) -> Bytes {
    encode_message(
        MessageType::STREAM_APPEND,
        Flags::NONE,
        &pb::StreamAppendMessage {
            name: name.into(),
            durable: true,
            records: records
                .iter()
                .enumerate()
                .map(|(k, v)| pb::StreamRecord {
                    step: 1,
                    attempt: 0,
                    k: k as u32,
                    payload: Bytes::from(v.to_string()),
                })
                .collect(),
            close: false,
        },
    )
}

/// `$log` and `$trace` as the SDK writes them: two log lines, a `plan` span with
/// a `tool` child, and an `orphan` span with no `end` (abandoned).
/// The `$trace` of a run with retries: each `start` of the nested chain
/// `p → c → g` arrives twice (same `span_id` — `span_id` does not include
/// `attempt`), and two spans `x`/`y` point at each other as parents (a
/// `$trace` writer getting it wrong).
fn retried_trace_frames() -> Bytes {
    use serde_json::json;
    let start = |id: &str, parent: Option<&str>| json!({"kind": "start", "span_id": id, "parent_id": parent, "name": id, "ts": 1});
    let mut records = Vec::new();
    for _ in 0..2 {
        records.push(start("p", None));
        records.push(start("c", Some("p")));
        records.push(start("g", Some("c")));
    }
    records.push(start("x", Some("y")));
    records.push(start("y", Some("x")));
    append_json("$trace", &records)
}

/// `$trace` as after the runtime died mid-span: the `start` of `llm` was not
/// durable and was lost, the rerun does not send it again (that point is already
/// in the journal), only `end` remains — carrying `name`/`parent_id` as the SDK
/// writes them. `late` is a root with only an `end`.
fn lost_start_trace_frames() -> Bytes {
    use serde_json::json;
    append_json(
        "$trace",
        &[
            json!({"kind": "start", "span_id": "s0", "parent_id": null, "name": "turn", "ts": 20}),
            json!({"kind": "end", "span_id": "s1", "parent_id": "s0", "name": "llm", "ts": 22, "status": "ok", "attrs": {"input_tokens": 30, "output_tokens": 4}}),
            json!({"kind": "end", "span_id": "s0", "parent_id": null, "name": "turn", "ts": 23, "status": "ok"}),
            json!({"kind": "end", "span_id": "s2", "parent_id": null, "name": "late", "ts": 24, "status": "error", "error": "timed out"}),
        ],
    )
}

fn observed_frames() -> Bytes {
    use serde_json::json;
    let mut b = BytesMut::new();
    b.extend_from_slice(&append_json(
        "$log",
        &[
            json!({"ts": 1, "level": "info", "msg": "calling tool", "attrs": {"tool": "search"}}),
            json!({"ts": 2, "level": "debug", "msg": "details"}),
            json!({"ts": 3, "level": "error", "msg": "failed"}),
        ],
    ));
    b.extend_from_slice(&append_json(
        "$trace",
        &[
            json!({"kind": "start", "span_id": "p1", "parent_id": null, "name": "plan", "ts": 10}),
            json!({"kind": "start", "span_id": "t1", "parent_id": "p1", "name": "tool", "ts": 11, "attrs": {"tool": "search"}}),
            json!({"kind": "end", "span_id": "t1", "ts": 12, "status": "ok", "attrs": {"input_tokens": 100, "output_tokens": 20}}),
            json!({"kind": "end", "span_id": "p1", "ts": 13, "status": "error", "error": "boom", "attrs": {"input_tokens": 1000, "output_tokens": 50}}),
            json!({"kind": "start", "span_id": "o1", "parent_id": null, "name": "orphan", "ts": 14}),
        ],
    ));
    b.freeze()
}

/// `ctx.run("a")` with its result: `Run` + `ProposeRunCompletion` — one commit.
fn run_done(cid: u32) -> Bytes {
    let mut b = BytesMut::new();
    b.extend_from_slice(&encode_message(
        MessageType::RUN,
        Flags::NONE,
        &pb::RunCommandMessage {
            result_completion_id: cid,
            name: "a".into(),
        },
    ));
    b.extend_from_slice(&encode_message(
        MessageType::PROPOSE_RUN_COMPLETION,
        Flags::NONE,
        &pb::ProposeRunCompletionMessage {
            result_completion_id: cid,
            result: Some(pb::propose_run_completion_message::Result::Value(
                Bytes::from_static(b"1"),
            )),
        },
    ));
    b.freeze()
}

/// `ctx.sleep` for one hour, then suspend waiting for it (completion 1) — a real external wait.
fn sleep_and_suspend() -> Bytes {
    let mut b = BytesMut::new();
    b.extend_from_slice(&encode_message(
        MessageType::SLEEP,
        Flags::NONE,
        &pb::SleepCommandMessage {
            wake_up_time: cairn_store::entropy::now_ms() + 3_600_000,
            result_completion_id: 1,
            name: "sleep".into(),
        },
    ));
    b.extend_from_slice(&encode_message(
        MessageType::SUSPENSION,
        Flags::NONE,
        &pb::SuspensionMessage {
            waiting_completions: vec![1],
            ..Default::default()
        },
    ));
    b.freeze()
}

/// A `PAUSE` error (like the SDK's `PauseError`): the invocation is `paused`, waiting for `resume`.
fn pause_error() -> Bytes {
    encode_message(
        MessageType::ERROR,
        Flags::NONE,
        &pb::ErrorMessage {
            code: 500,
            message: "waiting for a human (test)".into(),
            behavior: pb::ErrorBehavior::Pause as i32,
            ..Default::default()
        },
    )
}

/// Number of `resumable` turns run (whole test process).
static RESUMABLE_TURNS: AtomicUsize = AtomicUsize::new(0);
/// Number of `logmove` turns run (whole test process).
static LOGMOVE_TURNS: AtomicUsize = AtomicUsize::new(0);

/// A retryable error with the next attempt far away (60 s): only the commit recording the error runs.
fn retryable_error() -> Bytes {
    encode_message(
        MessageType::ERROR,
        Flags::NONE,
        &pb::ErrorMessage {
            code: 500,
            message: "failed (test)".into(),
            next_retry_delay: Some(60_000),
            ..Default::default()
        },
    )
}

/// Number of `rtraceretry` turns run (whole test process).
static RTRACE_RETRY_TURNS: AtomicUsize = AtomicUsize::new(0);

/// Only the `Run` command of `ctx.run("a")` — the closure has not finished.
fn run_only(cid: u32) -> Bytes {
    encode_message(
        MessageType::RUN,
        Flags::NONE,
        &pb::RunCommandMessage {
            result_completion_id: cid,
            name: "a".into(),
        },
    )
}

/// Only `ProposeRunCompletion` — the `Run` command is in the journal from the previous attempt.
fn run_completion_only(cid: u32) -> Bytes {
    encode_message(
        MessageType::PROPOSE_RUN_COMPLETION,
        Flags::NONE,
        &pb::ProposeRunCompletionMessage {
            result_completion_id: cid,
            result: Some(pb::propose_run_completion_message::Result::Value(
                Bytes::from_static(b"1"),
            )),
        },
    )
}

/// A retryable error with the next attempt close (50 ms).
fn retryable_error_soon() -> Bytes {
    encode_message(
        MessageType::ERROR,
        Flags::NONE,
        &pb::ErrorMessage {
            code: 500,
            message: "transient failure (test)".into(),
            next_retry_delay: Some(50),
            ..Default::default()
        },
    )
}

/// A handler's script: the chunks it sends down, possibly with waits between them.
fn script(handler: &str) -> Vec<Part> {
    match handler {
        // Three tokens, wait (mid-`ctx.run`), then finish.
        "chatty" => vec![
            Part::Send(append_frame("ui", 1, 0..1)),
            Part::Send(append_frame("ui", 1, 1..2)),
            Part::Send(append_frame("ui", 1, 2..3)),
            Part::Wait,
            Part::Send(output_end()),
        ],
        // 20 back-to-back `StreamAppend` frames, then finish — measures PUTs (AP-4).
        "many" => {
            let mut v: Vec<Part> = (0..20)
                .map(|k| Part::Send(append_frame("ui", 1, k..k + 1)))
                .collect();
            v.push(Part::Send(output_end()));
            v
        }
        // Three durable `notes` records, wait (no commit carries them yet), one
        // `ctx.run` completes (a commit), wait, then finish.
        "logrun" => vec![
            Part::Send(append_frame_as("notes", true, 1, 0..3, 0)),
            Part::Wait,
            Part::Send(run_done(1)),
            Part::Wait,
            Part::Send(output_end()),
        ],
        // Three durable `notes` records, then wait — no commit carries them
        // until the term ends (AP-15).
        "logwait" => vec![
            Part::Send(append_frame_as("notes", true, 1, 0..3, 0)),
            Part::Wait,
            Part::Send(output_end()),
        ],
        // One `llm` span of model `m-1` with 1 M tokens in, 100 k tokens out.
        "billed" => vec![
            Part::Send(append_json(
                "$trace",
                &[
                    serde_json::json!({"kind": "start", "span_id": "00000000000000b1", "parent_id": null, "name": "llm", "ts": 1, "attrs": {"model": "m-1"}}),
                    serde_json::json!({"kind": "end", "span_id": "00000000000000b1", "ts": 2, "status": "ok", "attrs": {"input_tokens": 1_000_000, "output_tokens": 100_000}}),
                ],
            )),
            Part::Send(output_end()),
        ],
        // `$trace` nearly at its 1 MiB limit (one padding record), then an `llm`
        // span with 7 777 777 tokens in, 3 tokens out: the span is dropped
        // because of the limit. The model has no price — the price table and
        // `metering` are shared by the whole test process and must not touch the
        // amounts of `trace_tokens_are_metered_and_priced`.
        "billedfull" => vec![
            Part::Send(append_frame_as("$trace", true, 0, 0..1, 1_048_500)),
            Part::Send(append_json(
                "$trace",
                &[
                    serde_json::json!({"kind": "start", "span_id": "00000000000000b2", "parent_id": null, "name": "llm", "ts": 1, "attrs": {"model": "m-unpriced"}}),
                    serde_json::json!({"kind": "end", "span_id": "00000000000000b2", "ts": 2, "status": "ok", "attrs": {"input_tokens": 7_777_777, "output_tokens": 3}}),
                ],
            )),
            Part::Send(output_end()),
        ],
        // One `ctx.run` completes, then finish — the handler declares `trace:
        // runtime`.
        "rtrace" => vec![Part::Send(run_done(1)), Part::Send(output_end())],
        // Attempt 0: open `ctx.run("a")`, then a transient error (the closure has
        // not finished). Attempt 1: `Run` is in the journal, only send its result,
        // then finish.
        "rtraceretry" => {
            if RTRACE_RETRY_TURNS.fetch_add(1, Ordering::SeqCst) == 0 {
                vec![Part::Send(run_only(1)), Part::Send(retryable_error_soon())]
            } else {
                vec![Part::Send(run_completion_only(1)), Part::Send(output_end())]
            }
        }
        // `$log` + `$trace` as the SDK writes them, then finish.
        "observed" => vec![Part::Send(observed_frames()), Part::Send(output_end())],
        // Two `$log` lines, then a retryable error (AP-14).
        "logfail" => vec![
            Part::Send(append_frame_as("$log", true, 1, 0..2, 0)),
            Part::Send(retryable_error()),
        ],
        // 300 durable records of ~1 KiB, then wait: the non-durable part exceeds
        // 256 KiB ⇒ flush when full, with no commit of the turn.
        "logflood" => vec![
            Part::Send(append_frame_as("notes", true, 1, 0..300, 1000)),
            Part::Wait,
            Part::Send(output_end()),
        ],
        // Like `logwait`, but the handler declares `logFlushMs: 100`.
        "logflush" => vec![
            Part::Send(append_frame_as("notes", true, 1, 0..3, 0)),
            Part::Wait,
            Part::Send(output_end()),
        ],
        // First turn: two durable `notes` records at step 1, attempt 0, then
        // `PAUSE`. Next turn (`resume`): step 1 reruns from attempt 0 — the same
        // identity, different content (with `pad`) — then finishes.
        "resumable" => {
            if RESUMABLE_TURNS.fetch_add(1, Ordering::SeqCst) == 0 {
                vec![
                    Part::Send(append_frame_as("notes", true, 1, 0..2, 0)),
                    Part::Send(pause_error()),
                ]
            } else {
                vec![
                    Part::Send(append_frame_as("notes", true, 1, 0..2, 1)),
                    Part::Send(output_end()),
                ]
            }
        }
        // First turn: six times 100 `$log` lines of ~1 KiB, each time a `ctx.run`
        // carrying them — six chunks, ≈ 600 KiB, more than two of the reader's Db
        // pages — then wait. Next turn (new term): one more line at step 7 — the
        // hub seeds from the last chunk, starting at 600 — then wait, then finish.
        "logmove" => {
            if LOGMOVE_TURNS.fetch_add(1, Ordering::SeqCst) == 0 {
                let mut v = Vec::new();
                for step in 1..=6 {
                    v.push(Part::Send(append_frame_as(
                        "$log",
                        true,
                        step,
                        0..100,
                        1000,
                    )));
                    v.push(Part::Send(run_done(step)));
                }
                v.push(Part::Wait);
                v
            } else {
                vec![
                    Part::Send(append_frame_as("$log", true, 7, 0..1, 0)),
                    Part::Wait,
                    Part::Send(output_end()),
                ]
            }
        }
        // Three `ui` (transient) records at step 1, then `sleep` for an hour and suspend.
        "waitext" => vec![
            Part::Send(append_frame("ui", 1, 0..3)),
            Part::Send(sleep_and_suspend()),
        ],
        // `$trace` with repeated `start`s and a parent cycle (`/spans`).
        "retried" => vec![Part::Send(retried_trace_frames()), Part::Send(output_end())],
        // `$trace` with an `end` whose `start` was lost (`/spans`).
        "loststart" => vec![
            Part::Send(lost_start_trace_frames()),
            Part::Send(output_end()),
        ],
        // One artifact, then finish.
        "artifact" => vec![
            Part::Send(artifact_frame(ARTIFACT_BODY, true)),
            Part::Send(output_end()),
        ],
        // An artifact whose sha does not match its content: the turn fails, no object.
        "badsha" => vec![
            Part::Send(artifact_frame(ARTIFACT_BODY, false)),
            Part::Send(output_end()),
        ],
        "htmlart" => vec![
            Part::Send(artifact_frame_typed(HTML_ARTIFACT, true, "text/html")),
            Part::Send(output_end()),
        ],
        _ => vec![Part::Send(output_end())],
    }
}

#[derive(Clone)]
enum Part {
    Send(Bytes),
    Wait,
}

const HANDLERS: [&str; 21] = [
    "chatty",
    "many",
    "plain",
    "logrun",
    "logwait",
    "logfail",
    "logflood",
    "observed",
    "rtrace",
    "billed",
    "artifact",
    "badsha",
    "htmlart",
    "logflush",
    "resumable",
    "retried",
    "logmove",
    "billedfull",
    "waitext",
    "loststart",
    "rtraceretry",
];

fn manifest(mode: ProtocolMode) -> String {
    let mut v = serde_json::to_value(EndpointManifest {
        protocol_mode: mode,
        min_protocol_version: 1,
        max_protocol_version: 4,
        services: vec![ServiceManifest {
            name: "Agent".into(),
            ty: ServiceType::Service,
            handlers: HANDLERS
                .iter()
                .map(|h| HandlerManifest {
                    name: h.to_string(),
                    ty: None,
                    contract: Default::default(),
                })
                .collect(),
            contract: Default::default(),
        }],
    })
    .expect("manifest");
    // `trace: "runtime"` in `rtrace`'s contract — built as JSON so the manifest
    // does not depend on new fields of `Contract`.
    for h in v["services"][0]["handlers"]
        .as_array_mut()
        .expect("handlers")
    {
        if h["name"] == "rtrace" || h["name"] == "rtraceretry" {
            h["trace"] = "runtime".into();
        }
        if h["name"] == "logflush" {
            h["logFlushMs"] = 100.into();
        }
    }
    v.to_string()
}

// ───────────────────────────────────────────────── hand-written fake SDK (h1 + h2c)

struct RawSdk {
    url: String,
    shared: Arc<RawShared>,
}

struct RawShared {
    mode: ProtocolMode,
    /// One slot per `release`: two in a row do not swallow each other like `Notify`.
    release: tokio::sync::Semaphore,
    opened: AtomicUsize,
    tasks: Mutex<Vec<tokio::task::AbortHandle>>,
}

impl RawSdk {
    async fn spawn(mode: ProtocolMode) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let shared = Arc::new(RawShared {
            mode,
            release: tokio::sync::Semaphore::new(0),
            opened: AtomicUsize::new(0),
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

    /// Let a waiting `chatty` handler go on.
    fn release(&self) {
        self.shared.release.add_permits(1);
    }
}

impl Drop for RawSdk {
    fn drop(&mut self) {
        for h in self.shared.tasks.lock().unwrap().drain(..) {
            h.abort();
        }
    }
}

async fn serve_conn(socket: tokio::net::TcpStream, sh: Arc<RawShared>) {
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

/// h1: `/discover`, and the `REQUEST_RESPONSE` `/invoke` (the whole body at once).
async fn serve_h1(mut socket: tokio::net::TcpStream, sh: &RawShared) {
    let mut head = Vec::new();
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        let mut chunk = [0u8; 4096];
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => head.extend_from_slice(&chunk[..n]),
        }
    }
    let text = String::from_utf8_lossy(&head).to_string();
    let (ctype, body) = if text.starts_with("GET /discover") {
        (DISCOVERY_V1, Bytes::from(manifest(sh.mode)))
    } else if let Some(rest) = text.strip_prefix("POST /invoke/Agent/") {
        let handler = rest.split(' ').next().unwrap_or("");
        // Read the rest of the request body (content-length) to be tidy.
        let len: usize = text
            .lines()
            .find_map(|l| {
                l.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|v| v.trim().parse().unwrap_or(0))
            })
            .unwrap_or(0);
        let have = head.len() - text.find("\r\n\r\n").unwrap() - 4;
        let mut left = len.saturating_sub(have);
        while left > 0 {
            let mut chunk = [0u8; 4096];
            match socket.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => left = left.saturating_sub(n),
            }
        }
        let mut b = BytesMut::new();
        for p in script(handler) {
            if let Part::Send(f) = p {
                b.extend_from_slice(&f);
            }
        }
        (INVOCATION_V1, b.freeze())
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

async fn serve_h2(socket: tokio::net::TcpStream, sh: Arc<RawShared>) {
    let Ok(mut conn) = h2::server::handshake(socket).await else {
        return;
    };
    while let Some(Ok((req, respond))) = conn.accept().await {
        sh.opened.fetch_add(1, Ordering::SeqCst);
        let h = tokio::spawn(serve_stream(req, respond, Arc::clone(&sh)));
        sh.tasks.lock().unwrap().push(h.abort_handle());
    }
}

async fn serve_stream(
    req: http::Request<h2::RecvStream>,
    mut respond: h2::server::SendResponse<Bytes>,
    sh: Arc<RawShared>,
) {
    let handler = req
        .uri()
        .path()
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_string();
    let resp = http::Response::builder()
        .status(200)
        .header("content-type", INVOCATION_V1)
        .body(())
        .expect("valid header");
    let Ok(mut send) = respond.send_response(resp, false) else {
        return;
    };
    let parts = script(&handler);
    let last = parts.len() - 1;
    for (i, p) in parts.into_iter().enumerate() {
        match p {
            Part::Send(f) => {
                if send.send_data(f, i == last).is_err() {
                    return;
                }
            }
            Part::Wait => {
                if let Ok(p) = sh.release.acquire().await {
                    p.forget();
                }
            }
        }
    }
    let mut body = req.into_body();
    while let Some(Ok(chunk)) = body.data().await {
        let _ = body.flow_control().release_capacity(chunk.len());
    }
}

// ───────────────────────────────────────────────────────── SSE reading

#[derive(Debug, Clone)]
struct Event {
    event: String,
    id: Option<String>,
    data: serde_json::Value,
}

struct Sse {
    resp: reqwest::Response,
    buf: String,
}

impl Sse {
    async fn open(url: &str) -> Result<std::result::Result<Self, (u16, String)>> {
        let resp = reqwest::Client::new()
            .get(url)
            .header("accept", "text/event-stream")
            .send()
            .await?;
        let code = resp.status().as_u16();
        if code != 200 {
            return Ok(Err((code, resp.text().await.unwrap_or_default())));
        }
        Ok(Ok(Self {
            resp,
            buf: String::new(),
        }))
    }

    /// The next event (skipping the `:keepalive` comment lines). `None` = the server closed.
    async fn next(&mut self) -> Result<Option<Event>> {
        loop {
            if let Some(end) = self.buf.find("\n\n") {
                let block: String = self.buf.drain(..end + 2).collect();
                let mut ev = Event {
                    event: "message".into(),
                    id: None,
                    data: serde_json::Value::Null,
                };
                let mut data = String::new();
                let mut any = false;
                for line in block.lines() {
                    if let Some(v) = line.strip_prefix("event:") {
                        ev.event = v.trim().into();
                        any = true;
                    } else if let Some(v) = line.strip_prefix("id:") {
                        ev.id = Some(v.trim().into());
                    } else if let Some(v) = line.strip_prefix("data:") {
                        data.push_str(v.trim_start());
                        any = true;
                    }
                }
                if !any {
                    continue;
                }
                ev.data = serde_json::from_str(&data).unwrap_or(serde_json::Value::String(data));
                return Ok(Some(ev));
            }
            match self.resp.chunk().await? {
                Some(c) => self.buf.push_str(&String::from_utf8_lossy(&c)),
                None => return Ok(None),
            }
        }
    }

    async fn next_within(&mut self, what: &str) -> Result<Event> {
        within(TEST_DEADLINE, what, async {
            self.next()
                .await?
                .ok_or_else(|| anyhow!("server closed the SSE before {what}"))
        })
        .await
    }
}

fn stream_url(node: &str, id: &str, name: &str) -> String {
    format!("{node}/invocations/{id}/streams/{name}")
}

async fn invoke_send(node: &str, handler: &str) -> Result<String> {
    let resp = reqwest::Client::new()
        .post(format!("{node}/invoke/Agent/{handler}/send"))
        .header("content-type", "application/json")
        .body("null")
        .send()
        .await?;
    ensure!(resp.status().is_success(), "send: {}", resp.status());
    let v: serde_json::Value = resp.json().await?;
    Ok(v["id"].as_str().context("id")?.to_string())
}

// ───────────────────────────────────────────────────────── test

/// **`REQUEST_RESPONSE`: records applied at the end of the turn, read over SSE
/// on the leader, the stream sealed when the invocation ends.**
///
/// 1. The RR SDK returns `StreamAppend ui [0..3)` + `StreamAppend ui [1..2)`
///    (duplicate) + `Output` + `End` in one body — the `many` script trimmed down
///    with `chatty` in RR (no waiting: RR has no "mid-turn").
/// 2. Wait for `completed`, then open SSE `ui` from the start.
///
/// Must see: three `record`s at offsets 0, 1, 2 (`durable: false`, `step` 1,
/// `data` is the SDK's JSON), then `end {next: 3}`, then the server closes; the
/// journal is still only `[Input, Output]`.
#[tokio::test]
async fn rr_records_are_read_over_sse_and_end_after_seal() -> Result<()> {
    let node = start_single_process(1).await?;
    let sdk = RawSdk::spawn(ProtocolMode::RequestResponse).await?;
    let reg = register(&node.url(), &sdk.url).await?;
    assert_eq!(reg["wireVersion"], 4, "{reg}");
    let (code, r) = send(&node.url(), "Agent", "chatty", None, "null").await?;
    ensure!(code < 300, "{code} {r}");
    let id = r["id"].as_str().unwrap().to_string();
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&node.url(), &id, "completed"),
    )
    .await?;

    let mut sse = Sse::open(&stream_url(&node.url(), &id, "ui"))
        .await?
        .map_err(|(c, b)| anyhow!("SSE {c}: {b}"))?;
    for want in 0..3u64 {
        let ev = sse.next_within("record").await?;
        assert_eq!(ev.event, "record", "{ev:?}");
        assert_eq!(ev.id.as_deref(), Some(want.to_string().as_str()));
        assert_eq!(ev.data["offset"], want);
        assert_eq!(ev.data["durable"], false);
        assert_eq!(ev.data["step"], 1);
        assert_eq!(ev.data["data"]["tok"], want);
    }
    let end = sse.next_within("end").await?;
    assert_eq!(end.event, "end", "{end:?}");
    assert_eq!(end.data["next"], 3);
    ensure!(sse.next().await?.is_none(), "server must close after end");

    let journal: serde_json::Value =
        reqwest::get(format!("{}/invocations/{id}/journal", node.url()))
            .await?
            .json()
            .await?;
    let types: Vec<&str> = journal
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["ty"].as_str().unwrap())
        .collect();
    assert_eq!(types, ["0x0400", "0x0401"], "{journal}");
    node.shutdown().await;
    Ok(())
}

/// **`BIDI_STREAM`: records reach the reader mid-turn, `from` and
/// `Last-Event-ID` resume.**
///
/// 1. `chatty` sends three tokens, then waits (mid-`ctx.run`).
/// 2. While it waits: SSE reads three `record`s; a second connection with
///    `Last-Event-ID: 0` starts at offset 1; a connection with `?from=2` starts
///    at offset 2.
/// 3. `release`: the handler finishes; the first connection gets `end`.
///
/// Must see: the records arrive **before** `Output` (the invocation is still
/// `running`).
#[tokio::test]
async fn bidi_records_reach_readers_mid_run_and_resume_by_offset() -> Result<()> {
    let node = start_single_process(1).await?;
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&node.url(), &sdk.url).await?;
    let id = invoke_send(&node.url(), "chatty").await?;
    let url = stream_url(&node.url(), &id, "ui");
    let mut sse = within(TEST_DEADLINE, "open SSE", async {
        loop {
            match Sse::open(&url).await? {
                Ok(s) => return Ok(s),
                Err((404 | 425, _)) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err((c, b)) => bail!("SSE {c}: {b}"),
            }
        }
    })
    .await?;
    for want in 0..3u64 {
        let ev = sse.next_within("record mid-turn").await?;
        assert_eq!(
            (ev.event.as_str(), ev.data["offset"].as_u64()),
            ("record", Some(want))
        );
    }
    let (_, st) = common::invocation(&node.url(), &id).await?;
    assert_eq!(
        st["status"], "running",
        "records must arrive before Output: {st}"
    );

    let resumed = reqwest::Client::new()
        .get(&url)
        .header("last-event-id", "0")
        .send()
        .await?;
    let mut resumed = Sse {
        resp: resumed,
        buf: String::new(),
    };
    assert_eq!(resumed.next_within("resume").await?.data["offset"], 1);
    let mut from = Sse::open(&format!("{url}?from=2"))
        .await?
        .map_err(|(c, b)| anyhow!("{c} {b}"))?;
    assert_eq!(from.next_within("from=2").await?.data["offset"], 2);

    sdk.release();
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&node.url(), &id, "completed"),
    )
    .await?;
    let end = sse.next_within("end").await?;
    assert_eq!(end.event, "end", "{end:?}");
    node.shutdown().await;
    Ok(())
}

/// **AP-4: stream records add no commit to the turn.** Two `BIDI_STREAM`
/// invocations in turn on the same idle node: `plain` (only `Output`) and
/// `many` (20 `StreamAppend` frames, then `Output`).
///
/// Must see: the number of store PUTs of the `many` turn is at most that of the
/// `plain` turn plus 1 (an out-of-phase background tick); `many`'s journal is
/// only `[Input, Output]`.
#[tokio::test(flavor = "multi_thread")]
async fn appends_add_no_commit_to_the_turn() -> Result<()> {
    let shared = Arc::new(slatedb::object_store::memory::InMemory::new());
    let n = common::start_cluster_node(&shared, "a", 1, Duration::from_secs(60)).await?;
    common::wait_probe(
        &[&n],
        Duration::from_secs(30),
        "a is leading range 0",
        |p| p.get(&0).is_some_and(|v| v == &vec![0]),
    )
    .await?;
    let url = n.node.url();
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&url, &sdk.url).await?;
    // One warm-up turn (load the deployment, warm machine) before measuring.
    let warm = invoke_send(&url, "plain").await?;
    within(TEST_DEADLINE, "warm", wait_status(&url, &warm, "completed")).await?;
    let mut puts = Vec::new();
    let mut ids = Vec::new();
    for handler in ["plain", "many"] {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let before = n.store.counts().op(Op::Put);
        let id = invoke_send(&url, handler).await?;
        within(TEST_DEADLINE, handler, wait_status(&url, &id, "completed")).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        puts.push(n.store.counts().op(Op::Put) - before);
        ids.push(id);
    }
    ensure!(
        puts[1] <= puts[0] + 1,
        "20 StreamAppend frames added PUTs: plain {} many {}",
        puts[0],
        puts[1]
    );
    let mut sse = Sse::open(&stream_url(&url, &ids[1], "ui"))
        .await?
        .map_err(|(c, b)| anyhow!("{c} {b}"))?;
    for want in 0..20u64 {
        assert_eq!(sse.next_within("record").await?.data["offset"], want);
    }
    n.node.shutdown().await;
    Ok(())
}

/// **AP-13: a node that does not lead returns `503` with `leader`, and opens no
/// follower reader.**
///
/// Must see: `503`, the body has a non-empty `leader`; the gauge
/// `cairnflow_follower_readers_open` does not change.
#[tokio::test(flavor = "multi_thread")]
async fn non_leader_answers_503_with_leader_and_opens_no_reader() -> Result<()> {
    let (_shared, nodes) = common::cluster_nodes(&["a", "b"], 1, Duration::from_secs(10)).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let probe = common::wait_probe(&refs, Duration::from_secs(30), "one leader", |p| {
        common::single_leaders(p).is_some()
    })
    .await?;
    let leader = common::single_leaders(&probe).context("leader")?[&0];
    let (lead, other) = (&nodes[leader].node, &nodes[1 - leader].node);
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&lead.url(), &sdk.url).await?;
    let id = invoke_send(&lead.url(), "chatty").await?;
    let readers = || {
        cairn_server::metrics::agent::FOLLOWER_READERS_OPEN
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum::<u64>()
    };
    let before = readers();
    match Sse::open(&stream_url(&other.url(), &id, "ui")).await? {
        Err((503, body)) => {
            let v: serde_json::Value = serde_json::from_str(&body)?;
            ensure!(
                v["leader"].as_str().is_some_and(|l| !l.is_empty()),
                "503 without a leader: {body}"
            );
        }
        Err((c, b)) => bail!("a node that does not lead returned {c}: {b}"),
        Ok(_) => bail!("a node that does not lead served SSE"),
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(readers(), before, "reading must not open a follower reader");
    sdk.release();
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// **`410 stream_expired` and `404`.**
///
/// 1. `plain` ends without writing any stream: reading `ui` ⇒ `410`
///    `stream_expired` (a transient stream has nothing left after sealing).
/// 2. An id that does not exist ⇒ `404`.
#[tokio::test]
async fn expired_and_missing_streams() -> Result<()> {
    let node = start_single_process(1).await?;
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&node.url(), &sdk.url).await?;

    let plain = invoke_send(&node.url(), "plain").await?;
    within(
        TEST_DEADLINE,
        "plain",
        wait_status(&node.url(), &plain, "completed"),
    )
    .await?;
    match Sse::open(&stream_url(&node.url(), &plain, "ui")).await? {
        Err((410, body)) => ensure!(body.contains("stream_expired"), "{body}"),
        Err((c, b)) => bail!("stream of a completed invocation: {c} {b}"),
        Ok(_) => bail!("stream of a completed invocation must not be 200"),
    }
    let missing = "00".repeat(24);
    match Sse::open(&stream_url(&node.url(), &missing, "ui")).await? {
        Err((404, _)) => {}
        Err((c, b)) => bail!("id does not exist: {c} {b}"),
        Ok(_) => bail!("an id that does not exist must not be 200"),
    }
    node.shutdown().await;
    Ok(())
}

/// **Leader change: `moved`, then `reset` for a transient stream.**
///
/// 1. Two nodes; `chatty` runs on the leader, an SSE on the leader reads three
///    records.
/// 2. The leader hands the range off (`resign`).
///
/// Must see: `moved` (with a `leader` field), then `reset`, then the server
/// closes.
#[tokio::test(flavor = "multi_thread")]
async fn leader_change_sends_moved_then_reset() -> Result<()> {
    let (_shared, nodes) = common::cluster_nodes(&["a", "b"], 1, Duration::from_secs(10)).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let probe = common::wait_probe(&refs, Duration::from_secs(30), "one leader", |p| {
        common::single_leaders(p).is_some()
    })
    .await?;
    let leader = common::single_leaders(&probe).context("leader")?[&0];
    let lead = &nodes[leader].node;
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&lead.url(), &sdk.url).await?;
    let id = invoke_send(&lead.url(), "chatty").await?;
    let url = stream_url(&lead.url(), &id, "ui");
    let mut sse = within(TEST_DEADLINE, "open SSE", async {
        loop {
            match Sse::open(&url).await? {
                Ok(s) => return Ok(s),
                Err((404 | 425, _)) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err((c, b)) => bail!("SSE {c}: {b}"),
            }
        }
    })
    .await?;
    for _ in 0..3 {
        sse.next_within("record").await?;
    }
    lead.partitions
        .get(cairn_store::PartitionId(0))
        .context("engine of range 0")?
        .resign()
        .await?;
    let moved = sse.next_within("moved").await?;
    assert_eq!(moved.event, "moved", "{moved:?}");
    assert!(moved.data.get("leader").is_some(), "{moved:?}");
    let reset = sse.next_within("reset").await?;
    assert_eq!(reset.event, "reset", "{reset:?}");
    ensure!(sse.next().await?.is_none(), "server must close after reset");
    sdk.release();
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// **The node's SSE limit: `429` + `Retry-After`.** A node limited to 1
/// connection; `chatty` is running; the first connection gets `200`, the second
/// `429`. When the first connection closes the slot comes back.
#[tokio::test]
async fn sse_over_node_cap_gets_429_with_retry_after() -> Result<()> {
    let mut cfg = common::test_config(cairn_server::node::NodeMode::single(1));
    cfg.streams.sse_max = 1;
    let node = common::start_led(cfg).await?;
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&node.url(), &sdk.url).await?;
    let id = invoke_send(&node.url(), "chatty").await?;
    let url = stream_url(&node.url(), &id, "ui");
    let mut first = within(TEST_DEADLINE, "first SSE", async {
        loop {
            match Sse::open(&url).await? {
                Ok(s) => return Ok(s),
                Err((404 | 425, _)) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err((c, b)) => bail!("SSE {c}: {b}"),
            }
        }
    })
    .await?;
    first.next_within("record").await?;
    let resp = reqwest::get(&url).await?;
    assert_eq!(resp.status().as_u16(), 429, "2nd connection over limit");
    assert!(resp.headers().contains_key("retry-after"));
    drop(first);
    let again = within(TEST_DEADLINE, "slot comes back", async {
        loop {
            match Sse::open(&url).await? {
                Ok(s) => return Ok(s),
                Err((429, _)) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err((c, b)) => bail!("SSE {c}: {b}"),
            }
        }
    })
    .await;
    again?;
    sdk.release();
    node.shutdown().await;
    Ok(())
}

// ───────────────────────────────────────────── durable streams ride commits

/// Open SSE `name` from 0 and read `n` `record`s.
async fn read_records(url: &str, n: usize) -> Result<Vec<Event>> {
    let mut sse = within(TEST_DEADLINE, "open SSE", async {
        loop {
            match Sse::open(url).await? {
                Ok(s) => return Ok(s),
                Err((404 | 425, _)) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err((c, b)) => bail!("SSE {c}: {b}"),
            }
        }
    })
    .await?;
    let mut out = Vec::new();
    while out.len() < n {
        let ev = sse.next_within("record").await?;
        if ev.event == "record" {
            out.push(ev);
        }
    }
    Ok(out)
}

/// Wait until the first `n` records of the stream are all `durable: true`
/// (reopening the SSE each time — the flag is as of sending).
async fn until_durable(url: &str, n: usize) -> Result<Vec<Event>> {
    within(TEST_DEADLINE, "durable records", async {
        loop {
            let got = read_records(url, n).await?;
            if got.iter().all(|e| e.data["durable"] == true) {
                return Ok(got);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
}

/// **AP-12, AP-16 — durable streams ride existing commits.**
///
/// 1. Measure the PUTs of the idle range for 1 s (the baseline).
/// 2. `logrun` sends three durable `notes` records, then waits mid-`ctx.run`.
/// 3. SSE sees three records with `durable: false` — no commit carries them yet
///    (AP-12).
/// 4. During 1 s of waiting: PUTs ≤ baseline + 1 — no commit of its own for the
///    stream (AP-16).
/// 5. `release`: the completed `ctx.run` is one commit, carrying the three
///    records.
///
/// Must see: after 5, reading again from 0 shows three records with `durable:
/// true` and `data` as sent; the journal has no entry for the stream records.
/// `/metrics` counts at least one `Streams` row carried and **0** extra rows
/// (≤ 1 row per stream per commit).
#[tokio::test(flavor = "multi_thread")]
async fn durable_records_ride_the_next_commit_and_never_commit_alone() -> Result<()> {
    let shared = Arc::new(slatedb::object_store::memory::InMemory::new());
    let n = common::start_cluster_node(&shared, "a", 1, Duration::from_secs(60)).await?;
    common::wait_probe(
        &[&n],
        Duration::from_secs(30),
        "a is leading range 0",
        |p| p.get(&0).is_some_and(|v| v == &vec![0]),
    )
    .await?;
    let url = n.node.url();
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&url, &sdk.url).await?;
    let warm = invoke_send(&url, "plain").await?;
    within(TEST_DEADLINE, "warm", wait_status(&url, &warm, "completed")).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let before = n.store.counts().op(Op::Put);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let idle = n.store.counts().op(Op::Put) - before;

    let id = invoke_send(&url, "logrun").await?;
    let notes = stream_url(&url, &id, "notes");
    let early = read_records(&notes, 3).await?;
    for e in &early {
        assert_eq!(e.data["durable"], false, "no commit carries it yet: {e:?}");
    }
    let before = n.store.counts().op(Op::Put);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let waiting = n.store.counts().op(Op::Put) - before;
    ensure!(
        waiting <= idle + 1,
        "a durable stream committed by itself while waiting: idle {idle} PUT/s, waiting {waiting}"
    );

    sdk.release();
    let got = until_durable(&notes, 3).await?;
    for (k, e) in got.iter().enumerate() {
        assert_eq!(e.data["offset"], k as u64);
        assert_eq!(e.data["data"]["tok"], k as u64);
    }
    sdk.release();
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&url, &id, "completed"),
    )
    .await?;
    // The counters are process-wide (other tests in the same binary add to them):
    // only check the floor of carried rows, and extra rows — no test may make that non-zero.
    let carried = common::metric(&url, "cairnflow_stream_rows_carried_total").await?;
    ensure!(carried >= 1, "no Streams row was carried: {carried}");
    let extra = common::metric(&url, "cairnflow_stream_rows_extra_total").await?;
    ensure!(
        extra == 0,
        "{extra} extra Streams rows for the same stream in one commit"
    );
    n.node.shutdown().await;
    Ok(())
}

/// **AP-14 — a failing step while the node is alive loses no record.**
/// `logfail` sends two `$log` lines, then a retryable `Error` with the next
/// attempt in 60 s: the only commit after those two lines is the one recording
/// the error.
///
/// Must see: both `$log` records become `durable: true`.
#[tokio::test]
async fn failing_step_commit_carries_pending_records() -> Result<()> {
    let node = start_single_process(1).await?;
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&node.url(), &sdk.url).await?;
    let id = invoke_send(&node.url(), "logfail").await?;
    let got = until_durable(&stream_url(&node.url(), &id, "%24log"), 2).await?;
    assert_eq!(got[1].data["offset"], 1);
    let (_, st) = common::invocation(&node.url(), &id).await?;
    ensure!(st["status"] != "completed", "{st}");
    node.shutdown().await;
    Ok(())
}

/// **Flush when full.** `logflood` sends 300 durable records of ~1 KiB
/// (≈ 300 KiB not durable, more than 256 KiB), then waits mid-`ctx.run`.
///
/// Must see: the first record becomes `durable: true` while the handler is
/// still waiting — a commit with only `Streams` rows, which nobody waits for —
/// and the journal still only has `Input`.
#[tokio::test]
async fn pending_over_256_kib_flushes_without_a_turn_commit() -> Result<()> {
    let node = start_single_process(1).await?;
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&node.url(), &sdk.url).await?;
    let id = invoke_send(&node.url(), "logflood").await?;
    until_durable(&stream_url(&node.url(), &id, "notes"), 1).await?;
    let journal: serde_json::Value =
        reqwest::get(format!("{}/invocations/{id}/journal", node.url()))
            .await?
            .json()
            .await?;
    assert_eq!(journal.as_array().map(Vec::len), Some(1), "{journal}");
    sdk.release();
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&node.url(), &id, "completed"),
    )
    .await?;
    node.shutdown().await;
    Ok(())
}

// ─────────────────────────────── durable streams across a leader change: read the durable part from the Db

/// Wait for a new term of range 0 — any node may lead it (after `resign`, the
/// node that just handed off may take it back; its hub is still new). Returns
/// the leader's URL.
async fn new_leader(nodes: &[common::ClusterNode]) -> Result<String> {
    let refs: Vec<_> = nodes.iter().collect();
    let probe = common::wait_probe(&refs, Duration::from_secs(30), "new leader", |p| {
        common::single_leaders(p).is_some()
    })
    .await?;
    let i = common::single_leaders(&probe).context("leader")?[&0];
    Ok(nodes[i].node.url())
}

/// **AP-3 — the durable part of a durable stream survives a `kill -9` of the
/// leader.**
///
/// 1. Two nodes; `logrun` on leader L: three `notes` records, then a completed
///    `ctx.run` carries them; wait until they are `durable: true` on L.
/// 2. Freeze L (the shape of a `kill -9` as seen from the bucket); the other
///    node takes the range.
/// 3. SSE `notes` from 0 on the new leader — its hub has nothing yet (or only
///    resent records, dropped as duplicates).
///
/// Must see: three records at offsets 0..3, `durable: true`, the right `data`,
/// read from the Db.
#[tokio::test(flavor = "multi_thread")]
async fn durable_prefix_survives_leader_kill() -> Result<()> {
    let (_shared, mut nodes) =
        common::cluster_nodes(&["a", "b"], 1, Duration::from_secs(2)).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let probe = common::wait_probe(&refs, Duration::from_secs(30), "one leader", |p| {
        common::single_leaders(p).is_some()
    })
    .await?;
    let leader = common::single_leaders(&probe).context("leader")?[&0];
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    let lead_url = nodes[leader].node.url();
    register(&lead_url, &sdk.url).await?;
    let id = invoke_send(&lead_url, "logrun").await?;
    read_records(&stream_url(&lead_url, &id, "notes"), 3).await?;
    sdk.release();
    until_durable(&stream_url(&lead_url, &id, "notes"), 3).await?;

    common::freeze(&nodes[leader].store);
    let dead = nodes.remove(leader);
    let next = new_leader(&nodes).await?;
    let got = read_records(&stream_url(&next, &id, "notes"), 3).await?;
    for (k, e) in got.iter().enumerate() {
        assert_eq!(e.data["offset"], k as u64, "{e:?}");
        assert_eq!(e.data["durable"], true, "{e:?}");
        assert_eq!(e.data["data"]["tok"], k as u64, "{e:?}");
    }
    drop(dead.node);
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// **AP-15 — quiesce loses no durable stream record that was received.**
///
/// 1. Two nodes; `logwait` on leader L: three `notes` records, then wait — no
///    commit after them.
/// 2. L hands the range off (`resign`): the `seal` fence carries the three
///    records.
/// 3. The SSE reader on L gets `moved` **without** `reset` (a durable stream).
/// 4. SSE `notes` from 0 on the leader of the new term (any node).
///
/// Must see: three `durable: true` records on the new leader.
#[tokio::test(flavor = "multi_thread")]
async fn quiesce_keeps_pending_durable_records() -> Result<()> {
    let (_shared, nodes) = common::cluster_nodes(&["a", "b"], 1, Duration::from_secs(10)).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let probe = common::wait_probe(&refs, Duration::from_secs(30), "one leader", |p| {
        common::single_leaders(p).is_some()
    })
    .await?;
    let leader = common::single_leaders(&probe).context("leader")?[&0];
    let lead = &nodes[leader].node;
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&lead.url(), &sdk.url).await?;
    let id = invoke_send(&lead.url(), "logwait").await?;
    let url = stream_url(&lead.url(), &id, "notes");
    let mut sse = within(TEST_DEADLINE, "open SSE", async {
        loop {
            match Sse::open(&url).await? {
                Ok(s) => return Ok(s),
                Err((404 | 425, _)) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err((c, b)) => bail!("SSE {c}: {b}"),
            }
        }
    })
    .await?;
    for _ in 0..3 {
        let ev = sse.next_within("record").await?;
        assert_eq!(ev.data["durable"], false, "no commit yet: {ev:?}");
    }
    lead.partitions
        .get(cairn_store::PartitionId(0))
        .context("engine of range 0")?
        .resign()
        .await?;
    let moved = sse.next_within("moved").await?;
    assert_eq!(moved.event, "moved", "{moved:?}");
    ensure!(
        sse.next().await?.is_none(),
        "durable stream: no `reset` after `moved`"
    );
    let next = new_leader(&nodes).await?;
    let got = read_records(&stream_url(&next, &id, "notes"), 3).await?;
    for (k, e) in got.iter().enumerate() {
        assert_eq!(e.data["offset"], k as u64, "{e:?}");
        assert_eq!(e.data["durable"], true, "{e:?}");
    }
    sdk.release();
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

/// **The durable stream of a completed invocation, on a term that has never
/// seen it.**
///
/// 1. `logrun` runs to completion on leader L.
/// 2. L hands the range off; the new term (any node) has an empty hub.
///
/// Must see: SSE `notes` returns three durable records, then `end {next: 3}`,
/// not `410` (a durable stream lives until purge).
#[tokio::test(flavor = "multi_thread")]
async fn finished_durable_stream_is_read_from_db_then_ends() -> Result<()> {
    let (_shared, nodes) = common::cluster_nodes(&["a", "b"], 1, Duration::from_secs(10)).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let probe = common::wait_probe(&refs, Duration::from_secs(30), "one leader", |p| {
        common::single_leaders(p).is_some()
    })
    .await?;
    let leader = common::single_leaders(&probe).context("leader")?[&0];
    let lead = &nodes[leader].node;
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&lead.url(), &sdk.url).await?;
    let id = invoke_send(&lead.url(), "logrun").await?;
    sdk.release();
    sdk.release();
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&lead.url(), &id, "completed"),
    )
    .await?;
    lead.partitions
        .get(cairn_store::PartitionId(0))
        .context("engine of range 0")?
        .resign()
        .await?;
    let next = new_leader(&nodes).await?;
    let mut sse = Sse::open(&stream_url(&next, &id, "notes"))
        .await?
        .map_err(|(c, b)| anyhow!("SSE {c}: {b}"))?;
    for k in 0..3u64 {
        let ev = sse.next_within("record").await?;
        assert_eq!(
            (ev.event.as_str(), ev.data["offset"].as_u64()),
            ("record", Some(k))
        );
        assert_eq!(ev.data["durable"], true, "{ev:?}");
    }
    let end = sse.next_within("end").await?;
    assert_eq!(end.event, "end", "{end:?}");
    assert_eq!(end.data["next"], 3);
    for n in nodes {
        n.node.shutdown().await;
    }
    Ok(())
}

// ───────────────────────────────────── reading `$log` and `$trace`

/// **`GET /invocations/{id}/logs`**: a JSON page of `$log`, filtered by level
/// and attempt, resumed with `from`; `?follow=1` is the SSE of `$log`.
///
/// Must see: three lines by offset, each with `level`/`msg`/`step`/`attempt`
/// and `durable`; `level=info` drops `debug`; `from=2` only the last line;
/// `next = 3`; `follow=1` returns `text/event-stream`.
#[tokio::test]
async fn logs_endpoint_pages_filters_and_follows() -> Result<()> {
    let node = start_single_process(1).await?;
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&node.url(), &sdk.url).await?;
    let id = invoke_send(&node.url(), "observed").await?;
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&node.url(), &id, "completed"),
    )
    .await?;
    let get = |q: &str| {
        let url = format!("{}/invocations/{id}/logs{q}", node.url());
        async move {
            let r = reqwest::get(&url).await?;
            ensure!(r.status().is_success(), "{url}: {}", r.status());
            Ok::<serde_json::Value, anyhow::Error>(r.json().await?)
        }
    };
    let all = get("").await?;
    let msgs: Vec<&str> = all["records"]
        .as_array()
        .context("records")?
        .iter()
        .map(|r| r["msg"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(msgs, ["calling tool", "details", "failed"], "{all}");
    assert_eq!(all["records"][0]["level"], "info");
    assert_eq!(all["records"][0]["attrs"]["tool"], "search");
    assert_eq!(all["records"][0]["step"], 1);
    assert_eq!(all["records"][0]["attempt"], 0);
    assert_eq!(all["records"][0]["offset"], 0);
    assert!(all["records"][0]["durable"].is_boolean(), "{all}");
    assert_eq!(all["next"], 3);
    let info = get("?level=info").await?;
    assert_eq!(info["records"].as_array().map(Vec::len), Some(2), "{info}");
    let tail = get("?from=2").await?;
    assert_eq!(tail["records"][0]["msg"], "failed", "{tail}");
    assert_eq!(
        get("?attempt=1").await?["records"].as_array().map(Vec::len),
        Some(0)
    );
    let follow = reqwest::get(format!("{}/invocations/{id}/logs?follow=1", node.url())).await?;
    assert_eq!(follow.status().as_u16(), 200);
    assert!(
        follow
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("text/event-stream")),
        "{:?}",
        follow.headers()
    );
    node.shutdown().await;
    Ok(())
}

/// **`GET /invocations/{id}/spans`**: the span tree built from `$trace`,
/// abandoned spans (a `start`, no `end`), and the invocation's token total.
///
/// Must see: two roots `plan` (child `tool`) and `orphan` (`unfinished: true`);
/// `plan` carries `status: error`, `error`, start/end times; token total
/// `input` 1100, `output` 70.
#[tokio::test]
async fn spans_endpoint_builds_the_tree_with_unfinished_spans_and_token_totals() -> Result<()> {
    let node = start_single_process(1).await?;
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&node.url(), &sdk.url).await?;
    let id = invoke_send(&node.url(), "observed").await?;
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&node.url(), &id, "completed"),
    )
    .await?;
    let r = reqwest::get(format!("{}/invocations/{id}/spans", node.url())).await?;
    ensure!(r.status().is_success(), "{}", r.status());
    let v: serde_json::Value = r.json().await?;
    let roots = v["spans"].as_array().context("spans")?;
    let names: Vec<&str> = roots
        .iter()
        .map(|s| s["name"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(names, ["plan", "orphan"], "{v}");
    let plan = &roots[0];
    assert_eq!(plan["span_id"], "p1");
    assert_eq!(plan["status"], "error");
    assert_eq!(plan["error"], "boom");
    assert_eq!(
        (plan["start_ts"].as_u64(), plan["end_ts"].as_u64()),
        (Some(10), Some(13))
    );
    assert_eq!(plan["unfinished"], false);
    assert_eq!(plan["children"][0]["name"], "tool");
    assert_eq!(
        plan["children"][0]["attrs"]["tool"], "search",
        "start ∪ end: {v}"
    );
    assert_eq!(plan["children"][0]["attrs"]["input_tokens"], 100);
    assert_eq!(roots[1]["unfinished"], true);
    assert_eq!(roots[1]["attempt"], 0);
    assert_eq!(v["tokens"]["input"], 1100, "{v}");
    assert_eq!(v["tokens"]["output"], 70, "{v}");
    node.shutdown().await;
    Ok(())
}

/// **`trace: runtime` per handler.** Handler `rtrace` declares it; the runtime
/// writes into `$trace` one span per turn (`cairnflow.turn`) and one child span
/// per `ctx.run`. A handler that does not declare it (`observed`, the test
/// above) has no runtime span at all.
///
/// Must see: a closed root `cairnflow.turn`, `status: ok`, with a closed child
/// `a` (the name of the `ctx.run`).
#[tokio::test]
async fn runtime_trace_adds_turn_and_run_spans_when_declared() -> Result<()> {
    let node = start_single_process(1).await?;
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&node.url(), &sdk.url).await?;
    let id = invoke_send(&node.url(), "rtrace").await?;
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&node.url(), &id, "completed"),
    )
    .await?;
    let v: serde_json::Value = reqwest::get(format!("{}/invocations/{id}/spans", node.url()))
        .await?
        .json()
        .await?;
    let roots = v["spans"].as_array().context("spans")?;
    ensure!(!roots.is_empty(), "no runtime span at all: {v}");
    let turn = &roots[0];
    assert_eq!(turn["name"], "cairnflow.turn", "{v}");
    assert_eq!(turn["unfinished"], false, "{v}");
    assert_eq!(turn["status"], "ok", "{v}");
    let run = &turn["children"][0];
    assert_eq!(run["name"], "a", "{v}");
    assert_eq!(run["unfinished"], false, "{v}");
    node.shutdown().await;
    Ok(())
}

/// **Runtime spans across a retry.** Handler `rtraceretry`:
///
/// 1. Attempt 0 sends `Run("a")` (completion 1), then a transient error (retry
///    after 50 ms) — the closure has not finished, no `ProposeRunCompletion`.
/// 2. Attempt 1: `Run` is in the journal (entry 1), the SDK only sends
///    `ProposeRunCompletion(1)`, then `Output`.
///
/// Must see: the turn of attempt 0 closes with `error`, its run `a` closes with
/// `error` and `abandoned`, and `entry` = 1; the turn of attempt 1 closes with
/// `ok` and **has** run `a` with `start_estimated`, `entry` = 1.
#[tokio::test]
async fn runtime_trace_spans_survive_a_retry() -> Result<()> {
    let node = start_single_process(1).await?;
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&node.url(), &sdk.url).await?;
    let id = invoke_send(&node.url(), "rtraceretry").await?;
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&node.url(), &id, "completed"),
    )
    .await?;
    let v: serde_json::Value = reqwest::get(format!("{}/invocations/{id}/spans", node.url()))
        .await?
        .json()
        .await?;
    let roots = v["spans"].as_array().context("spans")?;
    let turn = |attempt: u64| {
        roots
            .iter()
            .find(|s| s["name"] == "cairnflow.turn" && s["attempt"] == attempt)
            .with_context(|| format!("no turn for attempt {attempt}: {v}"))
    };
    let t0 = turn(0)?;
    assert_eq!(t0["status"], "error", "attempt 0 failed transiently: {v}");
    let r0 = &t0["children"][0];
    assert_eq!(r0["name"], "a", "{v}");
    assert_eq!(r0["unfinished"], false, "unfinished run open: {v}");
    assert_eq!(r0["status"], "error", "{v}");
    assert_eq!(r0["attrs"]["abandoned"], true, "{v}");
    assert_eq!(r0["attrs"]["entry"], 1, "{v}");
    let t1 = turn(1)?;
    assert_eq!(t1["status"], "ok", "{v}");
    let r1 = &t1["children"][0];
    assert_eq!(r1["name"], "a", "a rerun must have a span: {v}");
    assert_eq!(r1["unfinished"], false, "{v}");
    assert_eq!(r1["status"], "ok", "{v}");
    assert_eq!(r1["attrs"]["start_estimated"], true, "{v}");
    assert_eq!(r1["attrs"]["entry"], 1, "{v}");
    node.shutdown().await;
    Ok(())
}

// ───────────────────────────────────────────── tokens and cost

/// **Tokens and cost from `$trace`.** The bucket has a `pricing` table (`m-1`:
/// 3 USD per million input tokens, 15 USD per million output tokens, in
/// micro-USD); handler `billed` writes one `llm` span of `m-1` with 1 000 000
/// input tokens and 100 000 output tokens.
///
/// Must see: the tenant's metering grows by ≥ 1 000 000 `input_tokens`,
/// ≥ 100 000 `output_tokens`, and `cost_micros` grows by exactly 4 500 000
/// (other spans have no priced model); `GET /metering` has the three new
/// fields.
#[tokio::test(flavor = "multi_thread")]
async fn trace_tokens_are_metered_and_priced() -> Result<()> {
    use slatedb::object_store::{path::Path as ObjPath, ObjectStoreExt, PutPayload};
    let shared = Arc::new(slatedb::object_store::memory::InMemory::new());
    shared
        .put(
            &ObjPath::from("pricing"),
            PutPayload::from("cairnflow-pricing/1\nversion=1\nm-1\t3000000\t15000000\n"),
        )
        .await?;
    let n = common::start_cluster_node(&shared, "a", 1, Duration::from_secs(60)).await?;
    common::wait_probe(
        &[&n],
        Duration::from_secs(30),
        "a is leading range 0",
        |p| p.get(&0).is_some_and(|v| v == &vec![0]),
    )
    .await?;
    let url = n.node.url();
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&url, &sdk.url).await?;
    let usage = || {
        let snap = serde_json::to_value(
            cairn_server::metering::snapshot()
                .into_iter()
                .filter(|((t, _), _)| t == "default")
                .map(|(_, u)| u)
                .collect::<Vec<_>>(),
        )
        .unwrap_or_default();
        let sum = |k: &str| {
            snap.as_array()
                .map(|a| a.iter().map(|u| u[k].as_u64().unwrap_or(0)).sum::<u64>())
                .unwrap_or(0)
        };
        (sum("inputTokens"), sum("outputTokens"), sum("costMicros"))
    };
    let before = usage();
    let id = invoke_send(&url, "billed").await?;
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&url, &id, "completed"),
    )
    .await?;
    let after = usage();
    ensure!(after.0 >= before.0 + 1_000_000, "{before:?} → {after:?}");
    ensure!(after.1 >= before.1 + 100_000, "{before:?} → {after:?}");
    assert_eq!(after.2 - before.2, 4_500_000, "{before:?} → {after:?}");
    let m: serde_json::Value = reqwest::get(format!("{url}/metering"))
        .await?
        .json()
        .await?;
    for k in ["inputTokens", "outputTokens", "costMicros"] {
        ensure!(m.get(k).is_some(), "GET /metering lacks {k}: {m}");
    }
    n.node.shutdown().await;
    Ok(())
}

// ───────────────────────────────────────────────────────── artifacts

/// Artifact sweep knobs for tests: sweep every 100 ms, `min_age` 0.
fn fast_artifact_sweep(cfg: &mut cairn_server::node::NodeConfig) {
    cfg.artifact_sweep = Some(Duration::from_millis(100));
    cfg.artifact_min_age = Duration::ZERO;
}

/// Paths of every object under the bucket's `artifacts/`.
async fn artifact_objects(shared: &slatedb::object_store::memory::InMemory) -> Result<Vec<String>> {
    use {futures::TryStreamExt, slatedb::object_store::ObjectStore};
    let root = slatedb::object_store::path::Path::from("artifacts");
    let listed: Vec<_> = shared.list(Some(&root)).try_collect().await?;
    Ok(listed.into_iter().map(|m| m.location.to_string()).collect())
}

async fn one_node(
    tweak: impl FnOnce(&mut cairn_server::node::NodeConfig),
) -> Result<(
    Arc<slatedb::object_store::memory::InMemory>,
    common::ClusterNode,
)> {
    let shared = Arc::new(slatedb::object_store::memory::InMemory::new());
    let n =
        common::start_cluster_node_tweaked(&shared, "a", 1, Duration::from_secs(60), tweak).await?;
    common::wait_probe(
        &[&n],
        Duration::from_secs(30),
        "a is leading range 0",
        |p| p.get(&0).is_some_and(|v| v == &vec![0]),
    )
    .await?;
    Ok((shared, n))
}

/// **`ArtifactPut` (AP-1).** On both transports, handler `artifact` pushes one
/// artifact and finishes; the first PUT is retried by object_store itself
/// (written, returning its own `AlreadyExists` — `Ambiguity::Retried`).
///
/// Must see: the invocation is `completed`; the object is at
/// `artifacts/default/<id>/<epoch>/<sha>`; `GET …/artifacts/{sha}` returns the
/// right bytes and content-type; `GET …/artifacts` lists
/// `{sha, size, contentType}`; an unknown sha ⇒ 404; handler `badsha` (sha
/// does not match) is a protocol violation: `failed` 571, no object.
#[tokio::test(flavor = "multi_thread")]
async fn artifact_put_lands_before_the_output_and_reads_back() -> Result<()> {
    use cairn_store::testkit::{Action, Ambiguity, OpSet, PathMatch, Rule};
    for mode in [ProtocolMode::RequestResponse, ProtocolMode::BidiStream] {
        let (shared, n) = one_node(|_| {}).await?;
        let url = n.node.url();
        let sdk = RawSdk::spawn(mode).await?;
        register(&url, &sdk.url).await?;
        n.store.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains("artifacts/".into()),
            Action::Ambiguous {
                reply: Ambiguity::Retried,
                skip: 0,
                times: Some(1),
            },
        ));
        let id = invoke_send(&url, "artifact").await?;
        within(
            TEST_DEADLINE,
            "completed",
            wait_status(&url, &id, "completed"),
        )
        .await?;
        let sha = artifact_sha();
        let objs = artifact_objects(&shared).await?;
        ensure!(
            objs.len() == 1
                && objs[0].starts_with(&format!("artifacts/default/{id}/"))
                && objs[0].ends_with(&sha),
            "{mode:?}: {objs:?}"
        );
        let got = reqwest::get(format!("{url}/invocations/{id}/artifacts/{sha}")).await?;
        ensure!(got.status() == 200, "{mode:?}: GET {}", got.status());
        assert_eq!(
            got.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("text/plain")
        );
        assert_eq!(&got.bytes().await?[..], ARTIFACT_BODY);
        let list: serde_json::Value = reqwest::get(format!("{url}/invocations/{id}/artifacts"))
            .await?
            .json()
            .await?;
        assert_eq!(list["artifacts"][0]["sha256"], sha.as_str(), "{list}");
        assert_eq!(list["artifacts"][0]["size"], ARTIFACT_BODY.len(), "{list}");
        assert_eq!(list["artifacts"][0]["contentType"], "text/plain", "{list}");
        let missing = reqwest::get(format!(
            "{url}/invocations/{id}/artifacts/{}",
            "0".repeat(64)
        ))
        .await?;
        assert_eq!(missing.status(), 404);

        let bad = invoke_send(&url, "badsha").await?;
        let st = within(
            TEST_DEADLINE,
            "badsha failed",
            wait_status(&url, &bad, "failed"),
        )
        .await?;
        ensure!(
            st.to_string().contains("571"),
            "a wrong sha is a protocol violation: {st}"
        );
        ensure!(
            artifact_objects(&shared).await?.len() == 1,
            "a wrong sha leaves no object"
        );
        n.node.shutdown().await;
    }
    Ok(())
}

/// **A transient store read error on `GET …/artifacts/{sha}`** (found during a
/// soak run): the store fails one read ⇒ `503` so the client retries, and the
/// body does **not** carry store details (object path, the S3 client's message —
/// the masking rule for store errors); the next read ⇒ `200` with the right
/// bytes. Before: `502` with the store error verbatim.
#[tokio::test(flavor = "multi_thread")]
async fn artifact_get_store_error_is_retryable_and_opaque() -> Result<()> {
    use cairn_store::testkit::{Action, OpSet, PathMatch, Rule};
    let (shared, first_node) = one_node(|_| {}).await?;
    let url = first_node.node.url();
    let sdk = RawSdk::spawn(ProtocolMode::RequestResponse).await?;
    register(&url, &sdk.url).await?;
    let id = invoke_send(&url, "artifact").await?;
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&url, &id, "completed"),
    )
    .await?;
    let sha = artifact_sha();
    // A new node on the same bucket: the range's artifact cache is empty ⇒ the
    // GET goes down to the store, as after a leader change.
    first_node.node.shutdown().await;
    let n = common::start_cluster_node_tweaked(&shared, "b", 1, Duration::from_secs(60), |_| {})
        .await?;
    common::wait_probe(
        &[&n],
        Duration::from_secs(30),
        "b is leading range 0",
        |p| p.get(&0).is_some_and(|v| v == &vec![0]),
    )
    .await?;
    let url = n.node.url();
    n.store.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("artifacts/".into()),
        Action::Fail { times: Some(1) },
    ));
    let first = reqwest::get(format!("{url}/invocations/{id}/artifacts/{sha}")).await?;
    let status = first.status();
    let body = first.text().await?;
    ensure!(status == 503, "transient read error: {status} {body}");
    ensure!(
        !body.contains("artifacts/") && !body.contains("FaultyStore"),
        "the body leaks store details: {body}"
    );
    let again = reqwest::get(format!("{url}/invocations/{id}/artifacts/{sha}")).await?;
    ensure!(again.status() == 200, "next read: {}", again.status());
    assert_eq!(&again.bytes().await?[..], ARTIFACT_BODY);
    n.node.shutdown().await;
    Ok(())
}

/// **An artifact belongs to one run** (pinned green). The SDK's
/// `ctx.artifact.get` reads `GET /invocations/{current id}/artifacts/{sha}`;
/// `docs/ai-sdk.md` §6 relies on this to advise **not** to move chat history
/// into artifacts.
///
/// 1. Invocation X runs `artifact` (pushes one artifact).
/// 2. Invocation Y (a later turn, pushing nothing) runs `rtrace`.
///
/// Must see: X's sha is readable through X, **not** through Y (`404`) — a later
/// turn cannot reach the artifact of an earlier one.
#[tokio::test(flavor = "multi_thread")]
async fn artifact_of_one_turn_is_not_readable_from_the_next() -> Result<()> {
    let (_shared, n) = one_node(|_| {}).await?;
    let url = n.node.url();
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&url, &sdk.url).await?;
    let x = invoke_send(&url, "artifact").await?;
    within(
        TEST_DEADLINE,
        "X completed",
        wait_status(&url, &x, "completed"),
    )
    .await?;
    let y = invoke_send(&url, "rtrace").await?;
    within(
        TEST_DEADLINE,
        "Y completed",
        wait_status(&url, &y, "completed"),
    )
    .await?;
    let sha = artifact_sha();
    let own = reqwest::get(format!("{url}/invocations/{x}/artifacts/{sha}")).await?;
    ensure!(own.status() == 200, "through X itself: {}", own.status());
    let next = reqwest::get(format!("{url}/invocations/{y}/artifacts/{sha}")).await?;
    ensure!(next.status() == 404, "via later Y: {}", next.status());
    n.node.shutdown().await;
    Ok(())
}

/// **Purge holds the artifact in place (AP-2).** Retention of 1 day, a fast
/// sweep.
///
/// 1. `artifact` completes; purge it.
/// 2. The sweep sees `(id, epoch)` has no row left ⇒ a `kind=artifacts` entry in
///    `trash/`.
///
/// Must see: `GET …/artifacts/{sha}` ⇒ 404 right after the purge; the
/// held-in-place entry appears; the object is still where it was (retention not
/// over yet).
#[tokio::test(flavor = "multi_thread")]
async fn purged_artifacts_are_held_in_place() -> Result<()> {
    let (shared, n) = one_node(|cfg| {
        fast_artifact_sweep(cfg);
        cfg.trash_keep = Some(Duration::from_secs(86_400));
    })
    .await?;
    let url = n.node.url();
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&url, &sdk.url).await?;
    let id = invoke_send(&url, "artifact").await?;
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&url, &id, "completed"),
    )
    .await?;
    let sha = artifact_sha();
    let purged = reqwest::Client::new()
        .post(format!("{url}/invocations/{id}/purge"))
        .send()
        .await?;
    ensure!(purged.status().is_success(), "purge {}", purged.status());
    let gone = reqwest::get(format!("{url}/invocations/{id}/artifacts/{sha}")).await?;
    assert_eq!(gone.status(), 404);
    within(TEST_DEADLINE, "held-in-place entry", async {
        loop {
            use {futures::TryStreamExt, slatedb::object_store::ObjectStore};
            let entries: Vec<_> = shared
                .list(Some(&slatedb::object_store::path::Path::from("trash")))
                .try_collect()
                .await?;
            for e in entries {
                if e.location.as_ref().ends_with("/entry") {
                    use slatedb::object_store::ObjectStoreExt;
                    let raw = shared.get(&e.location).await?.bytes().await?;
                    if String::from_utf8_lossy(&raw).contains("kind=artifacts") {
                        return Ok::<_, anyhow::Error>(());
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    ensure!(
        artifact_objects(&shared).await?.len() == 1,
        "held in place: the object is intact"
    );
    n.node.shutdown().await;
    Ok(())
}

/// **The artifact sweep must reach every directory, even past the 100 000th
/// object** (each round used to start over from the beginning and stop after
/// `MAX_PAGES` × `PAGE` = 100 000 objects; now the next round continues from a
/// cursor).
///
/// 1. Tenant `aaa` has 101 orphan directories × 1 000 objects (no rows) —
///    sorted first by path.
/// 2. Tenant `zzz` has one orphan directory with one object — sorted after.
/// 3. The sweep runs every 100 ms, `min_age` 0, retention 1 day.
///
/// Must see: `zzz`'s directory is held in place (a `kind=artifacts` entry with
/// its id). Before the cursor: never — every round ran out of pages in `aaa`'s
/// directories, **leaking space** (no wrong deletion).
#[tokio::test(flavor = "multi_thread")]
async fn artifact_sweep_reaches_past_one_hundred_thousand_objects() -> Result<()> {
    use slatedb::object_store::{path::Path, ObjectStoreExt, PutPayload};
    let (shared, n) = one_node(|cfg| {
        fast_artifact_sweep(cfg);
        cfg.trash_keep = Some(Duration::from_secs(86_400));
    })
    .await?;
    let dir = |tenant: &str, i: u8| {
        let mut unique = [0u8; 16];
        unique[0] = i;
        unique[1] = tenant.as_bytes()[0];
        let id = cairn_store::make_invocation_id(cairn_store::PartitionKey(u64::from(i)), unique);
        (
            cairn_store::id_hex(&id),
            format!(
                "artifacts/{tenant}/{}/{:016x}",
                cairn_store::id_hex(&id),
                1u64
            ),
        )
    };
    for i in 0..101u8 {
        let (_, d) = dir("aaa", i);
        for k in 0..1000u32 {
            let sha = format!("{k:064x}");
            shared
                .put(
                    &Path::from(format!("{d}/{sha}")),
                    PutPayload::from_static(b"x"),
                )
                .await?;
        }
    }
    let (late_id, late) = dir("zzz", 0);
    shared
        .put(
            &Path::from(format!("{late}/{:064x}", 7)),
            PutPayload::from_static(b"x"),
        )
        .await?;
    let held = |want: String| {
        let shared = Arc::clone(&shared);
        async move {
            use {futures::TryStreamExt, slatedb::object_store::ObjectStore};
            let entries: Vec<_> = shared
                .list(Some(&Path::from("trash")))
                .try_collect()
                .await?;
            for e in entries {
                if e.location.as_ref().ends_with("/entry") {
                    let raw = shared.get(&e.location).await?.bytes().await?;
                    let raw = String::from_utf8_lossy(&raw);
                    if raw.contains("kind=artifacts") && raw.contains(&want) {
                        return Ok::<_, anyhow::Error>(true);
                    }
                }
            }
            Ok(false)
        }
    };
    let (first_id, _) = dir("aaa", 0);
    within(
        TEST_DEADLINE,
        "the sweep runs (aaa gets held in place)",
        async {
            while !held(first_id.clone()).await? {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(())
        },
    )
    .await?;
    within(
        TEST_DEADLINE,
        "directory past object 100 000 is held",
        async {
            while !held(late_id.clone()).await? {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Ok(())
        },
    )
    .await?;
    n.node.shutdown().await;
    Ok(())
}

/// **The artifact sweep's cross-round cursor does not delete wrongly at the
/// boundary.**
///
/// 1. A node runs without the sweep: `artifact` completes ⇒ the directory
///    `artifacts/default/<X>/…` of a live run **still has rows**; stop the node.
/// 2. Tenant `aaa` has 100 orphan directories × 1 000 objects = exactly
///    `MAX_PAGES` × `PAGE` — the first round stops at the last object of `aaa`'s
///    100th directory, with the cursor right before X's directory.
/// 3. Tenant `zzz` has one orphan directory, sorted after X.
/// 4. Reopen the node with the sweep every 100 ms, `min_age` 0, retention 1 day:
///    the second round starts exactly at X's directory.
///
/// Must see: `zzz`'s directory is held in place (the second round went past X),
/// no held entry carries X even after many more rounds (the cursor has wrapped
/// to the start), and X's `GET …/artifacts/{sha}` still returns the bytes.
#[tokio::test(flavor = "multi_thread")]
async fn artifact_sweep_cursor_keeps_a_live_dir_right_after_the_boundary() -> Result<()> {
    use slatedb::object_store::{path::Path, ObjectStoreExt, PutPayload};
    let shared = Arc::new(slatedb::object_store::memory::InMemory::new());
    let first =
        common::start_cluster_node_tweaked(&shared, "a", 1, Duration::from_secs(60), |cfg| {
            cfg.artifact_sweep = None;
        })
        .await?;
    common::wait_probe(
        &[&first],
        Duration::from_secs(30),
        "a is leading range 0",
        |p| p.get(&0).is_some_and(|v| v == &vec![0]),
    )
    .await?;
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&first.node.url(), &sdk.url).await?;
    let live = invoke_send(&first.node.url(), "artifact").await?;
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&first.node.url(), &live, "completed"),
    )
    .await?;
    first.node.shutdown().await;

    let dir = |tenant: &str, i: u8| {
        let mut unique = [0u8; 16];
        unique[0] = i;
        unique[1] = tenant.as_bytes()[0];
        let id = cairn_store::make_invocation_id(cairn_store::PartitionKey(u64::from(i)), unique);
        (
            cairn_store::id_hex(&id),
            format!(
                "artifacts/{tenant}/{}/{:016x}",
                cairn_store::id_hex(&id),
                1u64
            ),
        )
    };
    for i in 0..100u8 {
        let (_, d) = dir("aaa", i);
        for k in 0..1000u32 {
            shared
                .put(
                    &Path::from(format!("{d}/{k:064x}")),
                    PutPayload::from_static(b"x"),
                )
                .await?;
        }
    }
    let (late_id, late) = dir("zzz", 0);
    shared
        .put(
            &Path::from(format!("{late}/{:064x}", 7)),
            PutPayload::from_static(b"x"),
        )
        .await?;
    let before = artifact_objects(&shared).await?;
    ensure!(
        before.len() == 100_002
            && before[100_000].starts_with(&format!("artifacts/default/{live}/")),
        "X's directory sits right after the 100 000th object: {:?}",
        before.get(100_000)
    );

    let n = common::start_cluster_node_tweaked(&shared, "a", 1, Duration::from_secs(60), |cfg| {
        fast_artifact_sweep(cfg);
        cfg.trash_keep = Some(Duration::from_secs(86_400));
    })
    .await?;
    let url = n.node.url();
    let held = |want: String| {
        let shared = Arc::clone(&shared);
        async move {
            use {futures::TryStreamExt, slatedb::object_store::ObjectStore};
            let entries: Vec<_> = shared
                .list(Some(&Path::from("trash")))
                .try_collect()
                .await?;
            for e in entries {
                if e.location.as_ref().ends_with("/entry") {
                    let raw = shared.get(&e.location).await?.bytes().await?;
                    let raw = String::from_utf8_lossy(&raw);
                    if raw.contains("kind=artifacts") && raw.contains(&want) {
                        return Ok::<_, anyhow::Error>(true);
                    }
                }
            }
            Ok(false)
        }
    };
    within(
        TEST_DEADLINE,
        "zzz's directory is held (second round went past X)",
        async {
            while !held(late_id.clone()).await? {
                ensure!(
                    !held(live.clone()).await?,
                    "wrongly held the directory of live run X"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Ok(())
        },
    )
    .await?;
    // A few more rounds: the cursor wraps to the start and crosses the boundary again.
    tokio::time::sleep(Duration::from_secs(3)).await;
    ensure!(
        !held(live.clone()).await?,
        "wrongly held the directory of live run X"
    );
    let sha = artifact_sha();
    let got = reqwest::get(format!("{url}/invocations/{live}/artifacts/{sha}")).await?;
    ensure!(got.status() == 200, "X's artifact: {}", got.status());
    assert_eq!(&got.bytes().await?[..], ARTIFACT_BODY);
    n.node.shutdown().await;
    Ok(())
}

/// **A recreated id keeps its artifacts (AP-8).** Retention 0, a fast sweep.
///
/// 1. `artifact` with idempotency key `k` ⇒ id X, epoch E1; purge X.
/// 2. Call again with key `k` ⇒ the same id X (a deterministic id), an artifact
///    with the same content ⇒ epoch E2.
/// 3. The sweep holds, then deletes directory E1.
///
/// Must see: exactly one directory left under `artifacts/default/X/` (E2), and
/// X's `GET …/artifacts/{sha}` still returns the bytes.
#[tokio::test(flavor = "multi_thread")]
async fn recreated_id_keeps_its_artifacts() -> Result<()> {
    let (shared, n) = one_node(|cfg| {
        fast_artifact_sweep(cfg);
        cfg.trash_keep = Some(Duration::ZERO);
    })
    .await?;
    let url = n.node.url();
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&url, &sdk.url).await?;
    let call = || async {
        let r: serde_json::Value = reqwest::Client::new()
            .post(format!("{url}/invoke/Agent/artifact/send"))
            .header("idempotency-key", "k")
            .body("null")
            .send()
            .await?
            .json()
            .await?;
        let id = r["id"].as_str().context("id")?.to_string();
        within(
            TEST_DEADLINE,
            "completed",
            wait_status(&url, &id, "completed"),
        )
        .await?;
        Ok::<_, anyhow::Error>(id)
    };
    let x = call().await?;
    let first = artifact_objects(&shared).await?;
    let purged = reqwest::Client::new()
        .post(format!("{url}/invocations/{x}/purge"))
        .send()
        .await?;
    ensure!(purged.status().is_success(), "purge {}", purged.status());
    let again = call().await?;
    assert_eq!(again, x, "same key ⇒ same id");
    within(TEST_DEADLINE, "directory E1 deleted", async {
        loop {
            let now = artifact_objects(&shared).await?;
            if now.len() == 1 && now != first {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    let got = reqwest::get(format!(
        "{url}/invocations/{x}/artifacts/{}",
        artifact_sha()
    ))
    .await?;
    ensure!(got.status() == 200, "GET {}", got.status());
    assert_eq!(&got.bytes().await?[..], ARTIFACT_BODY);
    n.node.shutdown().await;
    Ok(())
}

/// **`restart-as-new` copies artifacts to the new id before the restart commit
/// (AP-10).** Retention 0, a fast sweep.
///
/// 1. `artifact` completes at id X; the SDK shuts down (the new run can run no
///    turn, so it does not PUT again by itself).
/// 2. `POST /invocations/X/restart` ⇒ id Y (X's entry 0 copied to Y).
/// 3. Purge X; the sweep holds, then deletes X's directory.
///
/// Must see: `GET /invocations/Y/artifacts/{sha}` returns the right bytes right
/// after the restart and still does after X's directory was deleted; only Y's
/// directory is left under `artifacts/default/`.
#[tokio::test(flavor = "multi_thread")]
async fn purging_old_after_restart_keeps_new_readable() -> Result<()> {
    let (shared, n) = one_node(|cfg| {
        fast_artifact_sweep(cfg);
        cfg.trash_keep = Some(Duration::ZERO);
    })
    .await?;
    let url = n.node.url();
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&url, &sdk.url).await?;
    let x = invoke_send(&url, "artifact").await?;
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&url, &x, "completed"),
    )
    .await?;
    drop(sdk);
    let r: serde_json::Value = reqwest::Client::new()
        .post(format!("{url}/invocations/{x}/restart"))
        .send()
        .await?
        .json()
        .await?;
    let y = r["new"].as_str().context("new id")?.to_string();
    let sha = artifact_sha();
    let read = |id: String| {
        let url = url.clone();
        let sha = sha.clone();
        async move {
            let got = reqwest::get(format!("{url}/invocations/{id}/artifacts/{sha}")).await?;
            ensure!(got.status() == 200, "GET {id}: {}", got.status());
            ensure!(&got.bytes().await?[..] == ARTIFACT_BODY);
            Ok::<_, anyhow::Error>(())
        }
    };
    read(y.clone()).await?;
    let purged = reqwest::Client::new()
        .post(format!("{url}/invocations/{x}/purge"))
        .send()
        .await?;
    ensure!(purged.status().is_success(), "purge {}", purged.status());
    within(TEST_DEADLINE, "X's directory deleted", async {
        loop {
            let now = artifact_objects(&shared).await?;
            if now.iter().all(|p| !p.contains(&x)) && !now.is_empty() {
                return Ok::<_, anyhow::Error>(now);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    ensure!(
        artifact_objects(&shared)
            .await?
            .iter()
            .all(|p| p.starts_with(&format!("artifacts/default/{y}/"))),
        "only Y's directory is left"
    );
    read(y).await?;
    n.node.shutdown().await;
    Ok(())
}

// ──────────────────────────────── the `cairnflow-log-level` header

/// **The `cairnflow-log-level` header of `/invoke` goes into entry 0.**
///
/// Must see: `/send` with `cairnflow-log-level: debug` ⇒ the entry 0 payload
/// carries that header (the SDK reads it on every turn, overriding
/// `HandlerOptions.logLevel`); without it ⇒ entry 0 has no such header; an
/// unknown value (`loud`) ⇒ `400`, nothing created.
#[tokio::test]
async fn log_level_header_lands_in_entry_zero() -> Result<()> {
    let node = start_single_process(1).await?;
    let sdk = RawSdk::spawn(ProtocolMode::RequestResponse).await?;
    register(&node.url(), &sdk.url).await?;
    let hex = |s: &str| s.bytes().map(|b| format!("{b:02x}")).collect::<String>();
    let entry0 = |id: String| {
        let url = node.url();
        async move {
            let j: serde_json::Value = reqwest::get(format!("{url}/invocations/{id}/journal"))
                .await?
                .json()
                .await?;
            Ok::<_, anyhow::Error>(j[0]["payload"].as_str().unwrap_or_default().to_string())
        }
    };
    let send_with = |level: Option<&'static str>| {
        let url = node.url();
        async move {
            let mut req = reqwest::Client::new()
                .post(format!("{url}/invoke/Agent/plain/send"))
                .body("null");
            if let Some(l) = level {
                req = req.header("cairnflow-log-level", l);
            }
            let r = req.send().await?;
            let code = r.status().as_u16();
            let body: serde_json::Value = r.json().await.unwrap_or_default();
            Ok::<_, anyhow::Error>((code, body))
        }
    };
    let (code, r) = send_with(Some("debug")).await?;
    ensure!(code < 300, "{code} {r}");
    let with = entry0(r["id"].as_str().context("id")?.to_string()).await?;
    ensure!(
        with.contains(&hex("cairnflow-log-level")) && with.contains(&hex("debug")),
        "entry 0 lacks the header: {with}"
    );
    let (_, r) = send_with(None).await?;
    let without = entry0(r["id"].as_str().context("id")?.to_string()).await?;
    ensure!(!without.contains(&hex("cairnflow-log-level")), "{without}");
    let (code, r) = send_with(Some("loud")).await?;
    assert_eq!(code, 400, "{r}");
    node.shutdown().await;
    Ok(())
}

/// The `--archive-artifacts` knob for tests.
fn archive_artifacts_on(cfg: &mut cairn_server::node::NodeConfig) {
    cfg.archive_artifacts = true;
}

/// **Archive and artifacts.** Archive is on (the default). By default the archive
/// record only carries refs (the journal) — no bytes copied;
/// `--archive-artifacts` copies artifacts to
/// `archive/<tenant>/<day>/<id>.artifacts/<sha>` **before** the archive record.
///
/// Must see: after the purge, both nodes have an `archive/default/<day>/<id>`
/// record; only the node with the flag has `<id>.artifacts/<sha>` with the
/// right bytes.
#[tokio::test(flavor = "multi_thread")]
async fn archive_copies_artifacts_only_when_asked() -> Result<()> {
    use {
        futures::TryStreamExt,
        slatedb::object_store::{ObjectStore, ObjectStoreExt},
    };
    for copy in [false, true] {
        let (shared, n) = one_node(|cfg| {
            if copy {
                archive_artifacts_on(cfg);
            }
        })
        .await?;
        let url = n.node.url();
        let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
        register(&url, &sdk.url).await?;
        let id = invoke_send(&url, "artifact").await?;
        within(
            TEST_DEADLINE,
            "completed",
            wait_status(&url, &id, "completed"),
        )
        .await?;
        let purged = reqwest::Client::new()
            .post(format!("{url}/invocations/{id}/purge"))
            .send()
            .await?;
        ensure!(purged.status().is_success(), "purge {}", purged.status());
        let archived: Vec<String> = shared
            .list(Some(&slatedb::object_store::path::Path::from("archive")))
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|m| m.location.to_string())
            .collect();
        ensure!(
            archived.iter().any(|p| p.ends_with(&id)),
            "copy={copy}: archive record missing: {archived:?}"
        );
        let art = archived
            .iter()
            .find(|p| p.ends_with(&format!("{id}.artifacts/{}", artifact_sha())));
        if copy {
            let p = art.with_context(|| format!("no artifact in the archive: {archived:?}"))?;
            let raw = shared
                .get(&slatedb::object_store::path::Path::from(p.as_str()))
                .await?
                .bytes()
                .await?;
            ensure!(&raw[..] == ARTIFACT_BODY);
        } else {
            ensure!(art.is_none(), "default is refs only: {archived:?}");
        }
        n.node.shutdown().await;
    }
    Ok(())
}

/// **A run capture always copies artifacts.** The `captures/<tenant>/<id>`
/// bundle must be reproducible outside the cluster, so the artifact bytes come
/// along at `capture-artifacts/<tenant>/<id>/<sha>` — a separate tree, so the
/// bundle listing does not mix in sha names.
///
/// Must see: after a run matching a capture request, the bundle exists and the
/// artifact has the right bytes; `GET /admin/capture` still lists exactly one
/// bundle.
#[tokio::test(flavor = "multi_thread")]
async fn capture_bundles_carry_their_artifacts() -> Result<()> {
    use slatedb::object_store::ObjectStoreExt;
    let (shared, n) = one_node(|_| {}).await?;
    let url = n.node.url();
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&url, &sdk.url).await?;
    let r = reqwest::Client::new()
        .post(format!("{url}/admin/capture"))
        .json(&serde_json::json!({
            "tenant": "default", "service": "Agent", "handler": "artifact", "count": 1, "ttl": 600
        }))
        .send()
        .await?;
    ensure!(r.status().is_success(), "set capture: {}", r.status());
    let id = invoke_send(&url, "artifact").await?;
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&url, &id, "completed"),
    )
    .await?;
    let art = slatedb::object_store::path::Path::from(format!(
        "capture-artifacts/default/{id}/{}",
        artifact_sha()
    ));
    within(TEST_DEADLINE, "bundle and artifact", async {
        loop {
            let bundle = shared
                .head(&slatedb::object_store::path::Path::from(format!(
                    "captures/default/{id}"
                )))
                .await
                .is_ok();
            if bundle {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    let raw = shared
        .get(&art)
        .await
        .with_context(|| format!("missing {art}"))?
        .bytes()
        .await?;
    ensure!(&raw[..] == ARTIFACT_BODY);
    let listing: serde_json::Value = reqwest::get(format!("{url}/admin/capture?tenant=default"))
        .await?
        .json()
        .await?;
    assert_eq!(listing["bundles"], serde_json::json!([id]), "{listing}");
    n.node.shutdown().await;
    Ok(())
}

/// Number of artifact object GETs the node has sent to the store.
fn artifact_gets(n: &common::ClusterNode) -> u64 {
    n.store
        .counts_by_source_object()
        .into_iter()
        .filter(|(_, op, _, object, _)| *op == Op::Get && *object == "artifacts")
        .map(|(.., c)| c)
        .sum()
}

/// **Per-range artifact cache.** A freshly PUT artifact goes straight into the
/// range's cache; reading it back through `GET …/artifacts/{sha}` does not GET
/// the store. The cache is lost on a leader change (it lives in the range's
/// engine); the node-wide limit is 256 MiB.
///
/// Must see: two reads with the right bytes, **0** artifact object GETs.
#[tokio::test(flavor = "multi_thread")]
async fn artifact_reads_hit_the_range_cache() -> Result<()> {
    let (_shared, n) = one_node(|_| {}).await?;
    let url = n.node.url();
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&url, &sdk.url).await?;
    let id = invoke_send(&url, "artifact").await?;
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&url, &id, "completed"),
    )
    .await?;
    let before = artifact_gets(&n);
    for _ in 0..2 {
        let got = reqwest::get(format!(
            "{url}/invocations/{id}/artifacts/{}",
            artifact_sha()
        ))
        .await?;
        ensure!(
            got.status() == reqwest::StatusCode::OK,
            "GET {}",
            got.status()
        );
        ensure!(&got.bytes().await?[..] == ARTIFACT_BODY);
    }
    assert_eq!(
        artifact_gets(&n) - before,
        0,
        "the read must hit the range's cache"
    );
    n.node.shutdown().await;
    Ok(())
}

/// **`log_flush_ms`.** The handler declares `logFlushMs: 100`: durable stream
/// records that have not ridden any commit are flushed on the node's clock beat
/// — keeping the log when the node dies mid-`ctx.run`. It is the second path
/// (alongside flush-when-full) that may commit only to make a stream durable
/// (AP-16).
///
/// 1. `logflush` sends three durable `notes` records, then waits mid-`ctx.run`
///    — no commit of the turn carries them.
///
/// Must see: within 5 s, the three records become `durable: true` while the SDK
/// is still waiting.
#[tokio::test(flavor = "multi_thread")]
async fn log_flush_ms_makes_waiting_records_durable() -> Result<()> {
    let (_shared, n) = one_node(|_| {}).await?;
    let url = n.node.url();
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&url, &sdk.url).await?;
    let id = invoke_send(&url, "logflush").await?;
    let notes = stream_url(&url, &id, "notes");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let got = read_records(&notes, 3).await?;
            if got.iter().all(|e| e.data["durable"] == true) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("records not durable within 5 s despite logFlushMs = 100"))??;
    sdk.release();
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&url, &id, "completed"),
    )
    .await?;
    n.node.shutdown().await;
    Ok(())
}

/// The registry of a binary from **before** the `streams/table` gate step (like
/// `upgrade.rs`): set the cluster up with the gate off.
static BEFORE_STREAMS_TABLE: &[cairn_store::migrate::Migration] = cairn_store::migrate::MIGRATIONS
    .split_at(cairn_store::migrate::STREAMS_TABLE as usize - 1)
    .0;

/// **Gate `streams/table` off ⇒ no `Streams` (0x18) row is written**
/// (AGENTS.md §1 rule 2): durable streams live only in the leader's RAM, as the
/// doc of `Engine::streams` promises.
///
/// 1. An older binary sets the cluster up (gate off), then stops; this binary
///    comes up on the same bucket — it does not turn the gate on by itself.
/// 2. `logrun` (three durable `notes` records, one `ctx.run` commit, finish)
///    runs to completion.
/// 3. The leader hands the range off; the new term reads `notes` — the new hub
///    is empty, so only the Db can answer.
///
/// The old bug: the rider was attached unconditionally, `side_frame` only
/// checked the wire ⇒ records went into the Db ⇒ the new term read three
/// `durable: true` records (with an older binary as leader they leaked on
/// purge).
///
/// Must see: the gate is still off; the new term returns `410 stream_expired`.
#[tokio::test(flavor = "multi_thread")]
async fn streams_gate_off_writes_no_stream_rows() -> Result<()> {
    let shared = Arc::new(slatedb::object_store::memory::InMemory::new());
    let old = common::start_cluster_node_tweaked(&shared, "old", 1, Duration::from_secs(60), |c| {
        c.migrations = BEFORE_STREAMS_TABLE;
    })
    .await?;
    old.node.shutdown().await;
    let n = common::start_cluster_node_tweaked(&shared, "a", 1, Duration::from_secs(60), |_| {})
        .await?;
    common::wait_probe(
        &[&n],
        Duration::from_secs(30),
        "a is leading range 0",
        |p| p.get(&0).is_some_and(|v| v == &vec![0]),
    )
    .await?;
    let store: Arc<dyn slatedb::object_store::ObjectStore> = shared.clone();
    let cv = cairn_store::migrate::read_cluster_version(
        &store,
        &slatedb::object_store::path::Path::from(""),
    )
    .await?
    .map(|(cv, _)| cv)
    .unwrap_or_default();
    ensure!(!cv.streams_on(), "new binary turned the gate on: {cv:?}");

    let url = n.node.url();
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&url, &sdk.url).await?;
    let id = invoke_send(&url, "logrun").await?;
    sdk.release();
    sdk.release();
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&url, &id, "completed"),
    )
    .await?;
    n.node
        .partitions
        .get(cairn_store::PartitionId(0))
        .context("engine of range 0")?
        .resign()
        .await?;
    let next = new_leader(std::slice::from_ref(&n)).await?;
    match Sse::open(&stream_url(&next, &id, "notes")).await? {
        Err((410, body)) => ensure!(body.contains("stream_expired"), "{body}"),
        Err((c, b)) => bail!("stream of a completed invocation: {c} {b}"),
        Ok(mut sse) => {
            let ev = sse.next_within("record").await?;
            bail!("gate off, yet the new term read a durable stream from the Db: {ev:?}");
        }
    }
    n.node.shutdown().await;
    Ok(())
}

/// **`resume` does not make the rerun's records "duplicates".**
///
/// 1. `resumable` writes two durable `notes` records at step 1, attempt 0, then
///    `PAUSE` ⇒ `paused`.
/// 2. `POST /invocations/{id}/resume`: the lifecycle core resets `attempt` to 0
///    (`position()` with `turn.reset`), so step 1 reruns with the **same**
///    `(step, attempt, k)` — different content.
///
/// The old bug: the hub still remembered `(attempt, k)` of step 1 (this term, or
/// seeded again from the last chunk) ⇒ the two new records were silently dropped
/// as duplicates: no `gap`, no `superseded`, no metric — the `$log` of the run
/// after `resume` was lost.
///
/// Must see: four records (two old, two new with `pad`), step 1's `superseded`
/// between them, then `end`.
#[tokio::test(flavor = "multi_thread")]
async fn resumed_step_records_are_not_dropped_as_duplicates() -> Result<()> {
    let node = start_single_process(1).await?;
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&node.url(), &sdk.url).await?;
    let id = invoke_send(&node.url(), "resumable").await?;
    within(
        TEST_DEADLINE,
        "paused",
        wait_status(&node.url(), &id, "paused"),
    )
    .await?;
    let r = reqwest::Client::new()
        .post(format!("{}/invocations/{id}/resume", node.url()))
        .send()
        .await?;
    ensure!(
        r.status().is_success(),
        "resume: {} {}",
        r.status(),
        r.text().await?
    );
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&node.url(), &id, "completed"),
    )
    .await?;

    let mut sse = Sse::open(&stream_url(&node.url(), &id, "notes"))
        .await?
        .map_err(|(c, b)| anyhow!("SSE {c}: {b}"))?;
    let mut seen = Vec::new();
    loop {
        let ev = sse.next_within("event").await?;
        match ev.event.as_str() {
            "end" => break,
            "record" => seen.push(format!(
                "record k={} pad={}",
                ev.data["k"],
                ev.data["data"].get("pad").is_some()
            )),
            other => seen.push(format!("{other} {}", ev.data)),
        }
    }
    let records: Vec<_> = seen.iter().filter(|s| s.starts_with("record")).collect();
    ensure!(
        records
            == [
                "record k=0 pad=false",
                "record k=1 pad=false",
                "record k=0 pad=true",
                "record k=1 pad=true"
            ],
        "records after resume dropped: {seen:?}"
    );
    ensure!(
        seen.iter().any(|s| s.starts_with("superseded")),
        "no `superseded` for the rerun step: {seen:?}"
    );
    node.shutdown().await;
    Ok(())
}

/// **`/spans`: one node per span, even when its `start` arrives several times.**
///
/// `span_id` deliberately does not include `attempt`, so every retry resends
/// `start` with the same id. The old bug: each `start` added one more place in
/// the order and in the parent's `children` ⇒ a chain of depth d retried r times
/// drew r^d nodes (2^40 if a handler did it on purpose — one `GET` exhausting the
/// CPU/RAM of a shared node); two spans pointing at each other as parents
/// vanished from the tree.
///
/// Must see: `p`, `c`, `g`, `x`, `y` each exactly once in the tree.
#[tokio::test]
async fn spans_endpoint_renders_each_span_once() -> Result<()> {
    let node = start_single_process(1).await?;
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&node.url(), &sdk.url).await?;
    let id = invoke_send(&node.url(), "retried").await?;
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&node.url(), &id, "completed"),
    )
    .await?;
    let r = reqwest::get(format!("{}/invocations/{id}/spans", node.url())).await?;
    ensure!(r.status().is_success(), "{}", r.status());
    let v: serde_json::Value = r.json().await?;
    fn count(nodes: &serde_json::Value, into: &mut std::collections::BTreeMap<String, usize>) {
        for n in nodes.as_array().into_iter().flatten() {
            *into
                .entry(n["span_id"].as_str().unwrap_or_default().to_string())
                .or_default() += 1;
            count(&n["children"], into);
        }
    }
    let mut seen = std::collections::BTreeMap::new();
    count(&v["spans"], &mut seen);
    let want: std::collections::BTreeMap<String, usize> = ["c", "g", "p", "x", "y"]
        .iter()
        .map(|s| (s.to_string(), 1))
        .collect();
    ensure!(seen == want, "one node per span: {seen:?} — {v}");
    node.shutdown().await;
    Ok(())
}

/// **`/spans` draws spans that only have an `end`.**
///
/// 1. Turn 1 opens span `llm` inside span `turn`: the `start` of `llm` goes into
///    RAM, not yet durable.
/// 2. The runtime dies before the next commit; the `start` is lost with the RAM.
/// 3. The rerun replays up to `llm` — that point is already in the journal, so
///    the SDK does not resend `start` — then closes the span in live mode: only
///    the `end` arrives.
///
/// The test builds the `$trace` after step 3 directly (handler `loststart`). The
/// old bug: an `end` without a `start` created a node but did not enter the
/// drawing order ⇒ span `llm` (with its tokens) vanished from the tree, and so
/// did root `late`.
///
/// Must see: root `turn` has child `llm` (`start_lost: true`, `start_ts: null`,
/// `unfinished: false`, `end_ts` 22), root `late` (`start_lost: true`, `status:
/// error`); `turn` has `start_lost: false`; token total 30/4.
#[tokio::test]
async fn spans_endpoint_renders_a_span_whose_start_was_lost() -> Result<()> {
    let node = start_single_process(1).await?;
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&node.url(), &sdk.url).await?;
    let id = invoke_send(&node.url(), "loststart").await?;
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&node.url(), &id, "completed"),
    )
    .await?;
    let r = reqwest::get(format!("{}/invocations/{id}/spans", node.url())).await?;
    ensure!(r.status().is_success(), "{}", r.status());
    let v: serde_json::Value = r.json().await?;
    let roots = v["spans"].as_array().context("spans")?;
    let names: Vec<&str> = roots
        .iter()
        .map(|s| s["name"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(names, ["turn", "late"], "{v}");
    let turn = &roots[0];
    assert_eq!(turn["start_lost"], false, "{v}");
    assert_eq!(turn["start_ts"], 20);
    let llm = &turn["children"][0];
    assert_eq!(llm["name"], "llm", "{v}");
    assert_eq!(llm["parent_id"], "s0");
    assert_eq!(llm["start_lost"], true);
    assert_eq!(llm["start_ts"], serde_json::Value::Null);
    assert_eq!(llm["end_ts"], 22);
    assert_eq!(llm["unfinished"], false);
    assert_eq!(llm["attrs"]["input_tokens"], 30);
    let late = &roots[1];
    assert_eq!(late["start_lost"], true, "{v}");
    assert_eq!(late["status"], "error");
    assert_eq!(late["error"], "timed out");
    assert_eq!(v["tokens"]["input"], 30, "{v}");
    assert_eq!(v["tokens"]["output"], 4, "{v}");
    node.shutdown().await;
    Ok(())
}

/// **Reading `$log` misses no durable part that lies before the hub.**
///
/// 1. `logmove` writes six chunks of 100 `$log` lines of ~1 KiB (≈ 600 KiB);
///    wait until they are `durable: true`.
/// 2. The leader hands the range off; the rerun in the new term writes one line
///    at step 7 — the hub seeds from the last chunk, starting at offset 600
///    (`missed = 600` when reading from 0).
/// 3. `GET /logs?from=0&limit=100`, then SSE `$log` from 0.
///
/// The old bug: `/logs` stopped its Db read loop at `limit` and then jumped
/// `next` to where the hub starts (`next = 600`, so following `next` lost
/// 100..600); SSE read only **one** Db page (256 KiB) and reported a `gap` for
/// the rest — although it was durable and readable.
///
/// Must see: `/logs` returns offsets 0..100 with `next = 100`; SSE returns 601
/// records with contiguous offsets, no `gap`.
#[tokio::test(flavor = "multi_thread")]
async fn log_readers_see_every_durable_record_before_the_hub() -> Result<()> {
    let (_shared, n) = one_node(|_| {}).await?;
    let url = n.node.url();
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&url, &sdk.url).await?;
    let id = invoke_send(&url, "logmove").await?;
    until_durable(&stream_url(&url, &id, "$log"), 600).await?;
    n.node
        .partitions
        .get(cairn_store::PartitionId(0))
        .context("engine of range 0")?
        .resign()
        .await?;
    let next = new_leader(std::slice::from_ref(&n)).await?;
    let get = |q: String| {
        let url = format!("{next}/invocations/{id}/logs{q}");
        async move {
            let r = reqwest::get(&url).await?;
            ensure!(r.status().is_success(), "{url}: {}", r.status());
            Ok::<serde_json::Value, anyhow::Error>(r.json().await?)
        }
    };
    // The rerun has written its line at step 2 (the new term's hub has seeded).
    within(TEST_DEADLINE, "new term's line", async {
        loop {
            let tail = get("?from=600".into()).await?;
            if tail["records"].as_array().is_some_and(|r| !r.is_empty()) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;

    let page = get("?from=0&limit=100".into()).await?;
    let offsets: Vec<u64> = page["records"]
        .as_array()
        .context("records")?
        .iter()
        .filter_map(|r| r["offset"].as_u64())
        .collect();
    ensure!(
        offsets == (0..100).collect::<Vec<_>>(),
        "first page: {offsets:?}"
    );
    ensure!(
        page["next"] == 100,
        "next jumped over the unread part: {}",
        page["next"]
    );

    let mut sse = Sse::open(&stream_url(&next, &id, "$log"))
        .await?
        .map_err(|(c, b)| anyhow!("SSE {c}: {b}"))?;
    for want in 0..=600u64 {
        let ev = sse.next_within("record").await?;
        ensure!(
            ev.event == "record" && ev.data["offset"].as_u64() == Some(want),
            "offset {want}: {ev:?}"
        );
    }
    // One slot for the first turn (dead with the old term, still waiting), one
    // for the rerun.
    sdk.release();
    sdk.release();
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&next, &id, "completed"),
    )
    .await?;
    n.node.shutdown().await;
    Ok(())
}

/// **A rejected `restart` copies no artifact** (AP-10 only applies to a
/// successful restart).
///
/// 1. `artifact` completes: one object under `artifacts/default/<x>/…`.
/// 2. `POST /invocations/{x}/restart?from=999` — past the journal length ⇒
///    `400`.
///
/// The old bug: `copy_artifacts` ran **before** `restart_in`'s `from` check: it
/// COPYed objects into the directory of an id that would never exist and
/// committed an `Artifacts` row for that id — the sweep never cleans a
/// directory that still has rows, so it leaked forever (repeatable by a user).
///
/// Must see: `400`; under `artifacts/` there is still only one object, `x`'s.
#[tokio::test(flavor = "multi_thread")]
async fn refused_restart_copies_no_artifact() -> Result<()> {
    let (shared, n) = one_node(|_| {}).await?;
    let url = n.node.url();
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&url, &sdk.url).await?;
    let x = invoke_send(&url, "artifact").await?;
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&url, &x, "completed"),
    )
    .await?;
    let before = artifact_objects(&shared).await?;
    ensure!(before.len() == 1 && before[0].contains(&x), "{before:?}");
    let r = reqwest::Client::new()
        .post(format!("{url}/invocations/{x}/restart?from=999"))
        .send()
        .await?;
    ensure!(r.status() == 400, "restart from=999: {}", r.status());
    let after = artifact_objects(&shared).await?;
    ensure!(after == before, "rejected restart copied: {after:?}");
    n.node.shutdown().await;
    Ok(())
}

/// **Tokens are still metered when `$trace` has hit its limit**: the 1 MiB
/// limit bounds telemetry, not the bill.
///
/// `billedfull` pads `$trace` up to just below the limit, then sends an `llm`
/// span with 7 777 777 input tokens: both `start` and `end` are dropped because
/// of the limit.
///
/// The old bug: `tokens_of` only ran after the limit check (and the RAM limit),
/// so the LLM calls of a long agent running after `$trace` filled up never
/// reached `metering`.
///
/// Must see: `inputTokens` of tenant `default` grows by at least 7 777 777.
#[tokio::test]
async fn trace_tokens_are_metered_past_the_trace_cap() -> Result<()> {
    let node = start_single_process(1).await?;
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&node.url(), &sdk.url).await?;
    let input = || {
        cairn_server::metering::snapshot()
            .into_iter()
            .filter(|((t, _), _)| t == "default")
            .map(|(_, u)| {
                serde_json::to_value(u).unwrap_or_default()["inputTokens"]
                    .as_u64()
                    .unwrap_or(0)
            })
            .sum::<u64>()
    };
    let before = input();
    let id = invoke_send(&node.url(), "billedfull").await?;
    within(
        TEST_DEADLINE,
        "completed",
        wait_status(&node.url(), &id, "completed"),
    )
    .await?;
    let after = input();
    ensure!(
        after >= before + 7_777_777,
        "tokens of a span dropped because of the limit were not metered: {before} → {after}"
    );
    node.shutdown().await;
    Ok(())
}

/// **The transient stream of an invocation suspended on an external wait only
/// keeps its book**: a transient stream promises to keep **the running step**,
/// and while suspended no step is running — the durable part is read back from
/// durable storage.
///
/// 1. `idle_release` grace of 300 ms. `waitext` writes three `ui` records, then
///    suspends waiting on a one-hour `sleep` ⇒ `suspended`.
/// 2. Read `ui` from 0 after the grace period.
///
/// The old bug: the ring stayed in the node's RAM until the leader changed — a
/// few hundred agents waiting for approval were enough to use up the whole
/// `NodeRam` (256 MiB), and then **every** stream on the node dropped records.
///
/// Must see: `gap {offset: 0, dropped: 3}` — the offsets are kept (the book is
/// still there), the bytes were released.
#[tokio::test(flavor = "multi_thread")]
async fn idle_waiting_invocation_releases_ephemeral_records() -> Result<()> {
    let (_shared, n) = one_node(|cfg| {
        cfg.streams.idle_release = Duration::from_millis(300);
    })
    .await?;
    let url = n.node.url();
    let sdk = RawSdk::spawn(ProtocolMode::BidiStream).await?;
    register(&url, &sdk.url).await?;
    let id = invoke_send(&url, "waitext").await?;
    within(
        TEST_DEADLINE,
        "suspended",
        wait_status(&url, &id, "suspended"),
    )
    .await?;
    let first = within(Duration::from_secs(10), "records released", async {
        loop {
            let mut sse = Sse::open(&stream_url(&url, &id, "ui"))
                .await?
                .map_err(|(c, b)| anyhow!("SSE {c}: {b}"))?;
            let ev = sse.next_within("first event").await?;
            if ev.event == "gap" {
                return Ok::<_, anyhow::Error>(ev);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?;
    assert_eq!(first.data["offset"], 0, "{first:?}");
    assert_eq!(first.data["dropped"], 3, "{first:?}");
    n.node.shutdown().await;
    Ok(())
}

/// **Artifact content is inert** (a security review finding). The content-type
/// is whatever the handler sets; since the API accepts the console's session
/// cookie, a `text/html` artifact opened in a browser would run a script on the
/// API's origin **with the viewer's session** — e.g. `POST /admin/keys` with a
/// cluster-admin session.
///
/// Must see: every `GET …/artifacts/{sha}` has `nosniff` and a `sandbox` CSP
/// (the document gets an empty origin, no script); `text/html` becomes a file
/// download (`attachment`), `text/plain` is still shown inline; the
/// content-type is kept.
#[tokio::test(flavor = "multi_thread")]
async fn artifact_content_is_served_inert() -> Result<()> {
    let (_shared, n) = one_node(|_| {}).await?;
    let url = n.node.url();
    let sdk = RawSdk::spawn(ProtocolMode::RequestResponse).await?;
    register(&url, &sdk.url).await?;
    let r = async {
        for (handler, body, ct, attachment) in [
            ("htmlart", HTML_ARTIFACT, "text/html", true),
            ("artifact", ARTIFACT_BODY, "text/plain", false),
        ] {
            let id = invoke_send(&url, handler).await?;
            within(
                TEST_DEADLINE,
                "completed",
                wait_status(&url, &id, "completed"),
            )
            .await?;
            let sha: String = {
                use sha2::Digest;
                sha2::Sha256::digest(body)
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect()
            };
            let got = reqwest::get(format!("{url}/invocations/{id}/artifacts/{sha}")).await?;
            ensure!(got.status() == 200, "{handler}: {}", got.status());
            let h = |k: &str| {
                got.headers()
                    .get(k)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string()
            };
            ensure!(h("content-type") == ct, "{handler}: {}", h("content-type"));
            ensure!(
                h("x-content-type-options") == "nosniff",
                "{handler}: missing nosniff"
            );
            ensure!(
                h("content-security-policy").contains("sandbox"),
                "{handler}: CSP {:?}",
                h("content-security-policy")
            );
            ensure!(
                h("content-disposition").starts_with("attachment") == attachment,
                "{handler}: content-disposition {:?}",
                h("content-disposition")
            );
        }
        Ok(())
    }
    .await;
    n.node.shutdown().await;
    r
}
