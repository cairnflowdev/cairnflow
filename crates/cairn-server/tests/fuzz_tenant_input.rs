// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Tenant data does not crash the node, at the real-node level.
//!
//! Two sources of tenant-written bytes, on the same node:
//!
//! 1. **SDK response bodies** (`REQUEST_RESPONSE`, wire v4): every turn the fake SDK
//!    returns a randomly generated stream of frames — commands with edge values, side
//!    channels (`StreamAppend`, `ArtifactPut`, `Keepalive`, custom), near-protobuf
//!    frames, truncated tails. They go through `turn/sdk.rs` (`drive_rr`, `side_frame`,
//!    `artifact_frame`), the state machine, the engine, the commit.
//! 2. **HTTP requests to tenant routes**: every route in `ingress/mod.rs` that the
//!    `Invoke`/`Admin` roles can call (minus `/admin/*` and the cluster-admin
//!    routes), with odd path parameters, odd queries, random JSON bodies or garbage
//!    bytes.
//!
//! Must see: **no panic anywhere in the process** (counted by a panic hook — the node
//! runs in the same process as the test, so a panic in a tokio task or on the engine
//! thread is counted even though tokio swallows it), and the node still answers
//! `/health` after both phases.
//!
//! A fixed budget per `cargo test` run; `PROPTEST_CASES=<n>` overrides it. The
//! generators use `proptest` without shrinking: a failure prints the seed and the
//! input, to be turned into a dedicated test.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

mod common;

#[path = "../../cairn-protocol/tests/fuzz_gen/mod.rs"]
mod fuzz_gen;

use {
    anyhow::Result,
    axum::{
        extract::State,
        http::{header, HeaderMap, StatusCode},
        routing::{get, post},
        Router,
    },
    bytes::Bytes,
    cairn_protocol::{encode_frame, encode_message, pb, Flags, MessageType},
    cairn_server::{
        discovery::{
            EndpointManifest, HandlerManifest, HandlerType, ProtocolMode, ServiceManifest,
            ServiceType, DISCOVERY_V1,
        },
        invoker::INVOCATION_V4,
    },
    proptest::{
        prelude::*,
        strategy::ValueTree,
        test_runner::{RngAlgorithm, TestRng, TestRunner},
    },
    std::{
        collections::VecDeque,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex, OnceLock,
        },
        time::Duration,
    },
};

// ───────────────────────────────────────────────────────── panic counting

static PANICS: AtomicUsize = AtomicUsize::new(0);
static PANIC_LOG: Mutex<Vec<String>> = Mutex::new(Vec::new());
/// The tests of this file run one at a time: the panic counter belongs to the whole
/// process, so two tests running in parallel (`cargo test`) would count each other's panics.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Installs the hook (once) and resets the counter; hold the returned lock for the whole test.
async fn panic_counter() -> tokio::sync::MutexGuard<'static, ()> {
    let guard = SERIAL.lock().await;
    install_panic_counter();
    PANICS.store(0, Ordering::SeqCst);
    PANIC_LOG.lock().unwrap().clear();
    guard
}

fn install_panic_counter() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            PANICS.fetch_add(1, Ordering::SeqCst);
            PANIC_LOG.lock().unwrap().push(info.to_string());
            prev(info);
        }));
    });
}

fn assert_no_panics(phase: &str, seed: u64) {
    let n = PANICS.load(Ordering::SeqCst);
    // Copy out first: a panic of this very assert goes through the hook, which locks the log.
    let log = PANIC_LOG.lock().unwrap().clone();
    assert_eq!(
        n, 0,
        "{phase}: {n} panic in node (CAIRNFLOW_FUZZ_SEED={seed}): {log:#?}"
    );
}

// ───────────────────────────────────────────────────────── generators

fn rng_seed() -> u64 {
    std::env::var("CAIRNFLOW_FUZZ_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(cairn_store::entropy::now_ms)
}

fn runner(seed: u64) -> TestRunner {
    let mut s = [0u8; 32];
    s[..8].copy_from_slice(&seed.to_le_bytes());
    TestRunner::new_with_rng(
        fuzz_gen::budget(0),
        TestRng::from_seed(RngAlgorithm::ChaCha, &s),
    )
}

fn sample<S: Strategy>(r: &mut TestRunner, s: &S) -> S::Value {
    s.new_tree(r).expect("generator does not reject").current()
}

fn cases(default: u32) -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn text() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        "[a-z]{1,6}",
        ".{0,12}",
        "[\\x00-\\x1f/\\\\%]{1,6}",
        "[a-z]{200,300}",
    ]
}

fn bytes() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..48)
}

fn enc<M: prost::Message>(ty: MessageType, flags: Flags, m: &M) -> Vec<u8> {
    encode_message(ty, flags, m).to_vec()
}

/// One SDK → runtime frame, encoded.
fn sdk_frame() -> impl Strategy<Value = Vec<u8>> {
    let id = prop_oneof![4 => 1u32..100_000, 1 => fuzz_gen::number().prop_map(|n| n as u32)];
    let time = prop_oneof![Just(0u64), Just(u64::MAX), any::<u64>()];
    prop_oneof![
        // Side channels: streams (record, name, close) and artifacts (right or wrong sha).
        (
            text(),
            any::<bool>(),
            any::<bool>(),
            prop::collection::vec((any::<u32>(), any::<u32>(), any::<u32>(), bytes()), 0..4)
        )
            .prop_map(|(name, durable, close, recs)| enc(
                MessageType::STREAM_APPEND,
                Flags::NONE,
                &pb::StreamAppendMessage {
                    name,
                    durable,
                    close,
                    records: recs
                        .into_iter()
                        .map(|(step, attempt, k, payload)| pb::StreamRecord {
                            step,
                            attempt,
                            k,
                            payload: payload.into(),
                        })
                        .collect(),
                },
            )),
        (bytes(), bytes(), text(), any::<bool>()).prop_map(|(sha, content, ct, honest)| {
            let sha = if honest {
                use sha2::Digest;
                sha2::Sha256::digest(&content).to_vec()
            } else {
                sha
            };
            enc(
                MessageType::ARTIFACT_PUT,
                Flags::NONE,
                &pb::ArtifactPutMessage {
                    sha256: sha.into(),
                    content: content.into(),
                    content_type: ct,
                },
            )
        }),
        Just(enc(
            MessageType::KEEPALIVE,
            Flags::NONE,
            &pb::KeepaliveMessage::default()
        )),
        bytes().prop_map(|p| encode_frame(MessageType(0xFC01), Flags::NONE, &p).to_vec()),
        // Commands.
        (time.clone(), id.clone()).prop_map(|(t, id)| enc(
            MessageType::SLEEP,
            Flags::NONE,
            &pb::SleepCommandMessage {
                wake_up_time: t,
                result_completion_id: id,
                name: String::new()
            },
        )),
        (text(), text(), text(), id.clone(), id.clone()).prop_map(|(s, h, k, a, b)| enc(
            MessageType::CALL,
            Flags::NONE,
            &pb::CallCommandMessage {
                service_name: s,
                handler_name: h,
                key: k,
                invocation_id_notification_idx: a,
                result_completion_id: b,
                ..Default::default()
            },
        )),
        (text(), text(), time, id.clone()).prop_map(|(s, h, t, a)| enc(
            MessageType::ONE_WAY_CALL,
            Flags::NONE,
            &pb::OneWayCallCommandMessage {
                service_name: s,
                handler_name: h,
                invoke_time: t,
                invocation_id_notification_idx: a,
                ..Default::default()
            },
        )),
        (bytes(), prop::option::of(bytes())).prop_map(|(k, v)| enc(
            MessageType::SET_STATE,
            Flags::NONE,
            &pb::SetStateCommandMessage {
                key: k.into(),
                value: v.map(|c| cairn_protocol::value::inline(c.into())),
                name: String::new(),
            },
        )),
        (bytes(), id.clone()).prop_map(|(k, id)| enc(
            MessageType::GET_STATE,
            Flags::NONE,
            &pb::GetStateCommandMessage {
                key: k.into(),
                result_completion_id: id,
                name: String::new()
            },
        )),
        (id.clone(), bytes()).prop_map(|(id, v)| [
            enc(
                MessageType::RUN,
                Flags::NONE,
                &pb::RunCommandMessage {
                    result_completion_id: id,
                    name: String::new(),
                }
            ),
            enc(
                MessageType::PROPOSE_RUN_COMPLETION,
                Flags::REQUESTED_ACK,
                &pb::ProposeRunCompletionMessage {
                    result_completion_id: id,
                    result: Some(pb::propose_run_completion_message::Result::Value(v.into())),
                }
            ),
        ]
        .concat()),
        (text(), id).prop_map(|(t, id)| enc(
            MessageType::ATTACH_INVOCATION,
            Flags::NONE,
            &pb::AttachInvocationCommandMessage {
                invocation_id: t,
                result_completion_id: id,
                name: String::new(),
            },
        )),
        (any::<u32>(), text(), prop::option::of(any::<u64>())).prop_map(|(code, m, d)| enc(
            MessageType::ERROR,
            Flags::NONE,
            &pb::ErrorMessage {
                code,
                message: m,
                next_retry_delay: d,
                ..Default::default()
            },
        )),
        (
            prop::collection::vec(any::<u32>(), 0..3),
            prop::collection::vec(text(), 0..2)
        )
            .prop_map(|(c, n)| enc(
                MessageType::SUSPENSION,
                Flags::NONE,
                &pb::SuspensionMessage {
                    waiting_completions: c,
                    waiting_named_signals: n,
                    ..Default::default()
                },
            )),
        // Near-protobuf, any type code.
        (fuzz_gen::message_type(), fuzz_gen::payload()).prop_map(|(ty, p)| encode_frame(
            ty,
            Flags::NONE,
            &p
        )
        .to_vec()),
    ]
}

/// One response body: a few frames, usually ending in `Output` + `End`, sometimes truncated.
fn sdk_body() -> impl Strategy<Value = Vec<u8>> {
    (
        prop::collection::vec(sdk_frame(), 0..8),
        prop::option::of(bytes()),
        any::<bool>(),
        prop::option::of(any::<usize>()),
    )
        .prop_map(|(frames, output, end, cut)| {
            let mut out = frames.concat();
            if let Some(v) = output {
                out.extend(enc(
                    MessageType::OUTPUT,
                    Flags::NONE,
                    &pb::OutputCommandMessage {
                        result: Some(pb::output_command_message::Result::Value(
                            cairn_protocol::value::inline(v.into()),
                        )),
                        name: String::new(),
                    },
                ));
            }
            if end {
                out.extend(enc(MessageType::END, Flags::NONE, &pb::EndMessage {}));
            }
            if let Some(c) = cut.filter(|c| c % 5 == 0) {
                if !out.is_empty() {
                    let at = c % out.len();
                    out.truncate(at);
                }
            }
            out
        })
}

/// An arbitrary JSON value, up to 3 levels deep.
fn json() -> impl Strategy<Value = serde_json::Value> {
    use serde_json::Value as J;
    let leaf = prop_oneof![
        Just(J::Null),
        any::<bool>().prop_map(J::Bool),
        any::<i64>().prop_map(J::from),
        any::<u64>().prop_map(J::from),
        any::<f64>().prop_map(|f| serde_json::Number::from_f64(f).map_or(J::Null, J::Number)),
        text().prop_map(J::String),
    ];
    leaf.prop_recursive(3, 24, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..6).prop_map(J::Array),
            prop::collection::btree_map(
                prop_oneof![
                    // Real field names of the APIs, to get through serde into the logic.
                    prop::sample::select(vec![
                        "uri",
                        "sql",
                        "input",
                        "cron",
                        "every",
                        "at",
                        "service",
                        "handler",
                        "key",
                        "tz",
                        "value",
                        "failure",
                        "code",
                        "message",
                        "name",
                        "ids",
                        "filter",
                        "reason",
                        "deployment",
                        "revision",
                        "limit",
                        "state",
                        "paused",
                        "headers",
                        "delay_ms",
                        "idempotency_key",
                        "scope",
                    ])
                    .prop_map(str::to_string),
                    text(),
                ],
                inner,
                0..6
            )
            .prop_map(|m| J::Object(m.into_iter().collect())),
        ]
    })
}

fn http_body() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        4 => json().prop_map(|j| serde_json::to_vec(&j).unwrap()),
        1 => bytes(),
        1 => Just(Vec::new()),
        // Odd SQL for `/query`.
        1 => ".{0,40}".prop_map(|s| serde_json::to_vec(&serde_json::json!({ "sql": s })).unwrap()),
        1 => prop::sample::select(vec![
            "select * from sys_invocation where id = 'x' limit 18446744073709551615",
            "select count(*) from sys_journal group by 1 order by 1 desc limit -1",
            "select * from sys_invocation i join sys_journal j on true",
            "select 1/0",
            "select cast('9999999999999999999999' as bigint)",
            "with recursive r(n) as (select 1 union all select n+1 from r) select * from r",
        ])
        .prop_map(|s| serde_json::to_vec(&serde_json::json!({ "sql": s })).unwrap()),
    ]
}

/// A path segment: a tenant-written string, percent-encoded into a valid URL.
fn segment() -> impl Strategy<Value = String> {
    prop_oneof![
        3 => text(),
        1 => Just("inv_1abcdefghijklmnopqrstuvwxyz0123".to_string()),
        1 => Just("..".to_string()),
    ]
    .prop_map(|s| {
        s.bytes()
            .map(|b| {
                if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' {
                    (b as char).to_string()
                } else {
                    format!("%{b:02X}")
                }
            })
            .collect::<String>()
    })
}

fn query_string() -> impl Strategy<Value = String> {
    prop::collection::vec(
        (
            prop::sample::select(vec![
                "key",
                "limit",
                "cursor",
                "tenant",
                "status",
                "service",
                "from",
                "to",
                "after",
                "wait",
                "timeout_ms",
                "idempotency_key",
                "delay",
                "offset",
                "since",
                "tz",
            ]),
            segment(),
        ),
        0..4,
    )
    .prop_map(|kv| {
        kv.into_iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&")
    })
}

// ───────────────────────────────────────────────────────── tenant routes

/// `(method, path pattern)` of every route in `ingress::router` that the `Invoke`/`Admin`
/// roles can call — read from the source, like `ui_serving.rs` and `query_api.rs`, so a
/// new route joins the fuzz on its own.
fn tenant_routes() -> Vec<(String, String)> {
    let src = include_str!("../src/ingress/mod.rs");
    let body =
        &src[src.find("pub fn router").unwrap()..src.find("pub fn internal_router").unwrap()];
    // Join each `.route(` into one line.
    let mut out = Vec::new();
    for chunk in body.split(".route(").skip(1) {
        let chunk = &chunk[..chunk.find(")\n").unwrap_or(chunk.len())];
        let Some(path) = chunk.split('"').nth(1) else {
            continue;
        };
        if !path.starts_with('/') {
            continue;
        }
        let cluster_admin = [
            "/admin",
            "/ranges",
            "/cluster",
            "/metrics",
            "/rules",
            "/limits",
            "/scheduler",
            "/vqueues",
            "/health",
        ]
        .iter()
        .any(|p| path.starts_with(p));
        // Registering a deployment calls out to the URI in the body: no GET to a foreign host.
        if cluster_admin {
            continue;
        }
        for (needle, method) in [
            ("get(", "GET"),
            ("post(", "POST"),
            ("put(", "PUT"),
            ("delete(", "DELETE"),
        ] {
            if chunk.contains(needle) {
                if path == "/deployments" && method == "POST" {
                    continue;
                }
                out.push((method.to_string(), path.to_string()));
            }
        }
    }
    assert!(out.len() > 40, "route scan broken: only {}", out.len());
    out
}

fn fill(path: &str, r: &mut TestRunner) -> String {
    path.split('/')
        .map(|s| {
            if s.starts_with('{') {
                sample(r, &segment())
            } else {
                s.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

// ───────────────────────────────────────────────────────── fake SDK

#[derive(Default)]
struct Script {
    bodies: Mutex<VecDeque<Vec<u8>>>,
    turns: AtomicUsize,
}

async fn discover() -> ([(header::HeaderName, &'static str); 1], String) {
    let handler = |name: &str, ty: Option<HandlerType>| HandlerManifest {
        name: name.into(),
        ty,
        contract: Default::default(),
    };
    let manifest = EndpointManifest {
        protocol_mode: ProtocolMode::RequestResponse,
        min_protocol_version: 1,
        max_protocol_version: 4,
        services: vec![
            ServiceManifest {
                name: "Fuzz".into(),
                ty: ServiceType::Service,
                handlers: vec![handler("h", None)],
                contract: Default::default(),
            },
            ServiceManifest {
                name: "FuzzObj".into(),
                ty: ServiceType::VirtualObject,
                handlers: vec![handler("h", Some(HandlerType::Exclusive))],
                contract: Default::default(),
            },
        ],
    };
    (
        [(header::CONTENT_TYPE, DISCOVERY_V1)],
        serde_json::to_string(&manifest).unwrap(),
    )
}

/// Each turn returns the next body of the script; when the script runs out it ends
/// cleanly so the invocation does not retry forever.
async fn invoke_turn(
    State(st): State<Arc<Script>>,
    req: HeaderMap,
) -> (StatusCode, HeaderMap, Bytes) {
    st.turns.fetch_add(1, Ordering::SeqCst);
    let body = st.bodies.lock().unwrap().pop_front().unwrap_or_else(|| {
        [
            enc(
                MessageType::OUTPUT,
                Flags::NONE,
                &pb::OutputCommandMessage::default(),
            ),
            enc(MessageType::END, Flags::NONE, &pb::EndMessage {}),
        ]
        .concat()
    });
    let ct = req
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or(INVOCATION_V4)
        .to_string();
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, ct.parse().unwrap());
    (StatusCode::OK, headers, Bytes::from(body))
}

async fn spawn_sdk(script: Arc<Script>) -> Result<String> {
    let app = Router::new()
        .route("/discover", get(discover))
        .route("/invoke/{service}/{handler}", post(invoke_turn))
        .with_state(script);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move { axum::serve(listener, app).await });
    Ok(url)
}

// ───────────────────────────────────────────────────────── test

async fn healthy(client: &reqwest::Client, url: &str) {
    let r = client
        .get(format!("{url}/health"))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("node does not answer /health");
    assert!(r.status().is_success(), "/health returned {}", r.status());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_survives_tenant_bytes() -> Result<()> {
    let _serial = panic_counter().await;
    let seed = rng_seed();
    eprintln!("fuzz_tenant_input: CAIRNFLOW_FUZZ_SEED={seed}");
    let mut r = runner(seed);

    let bucket = common::bucket();
    let node = common::start_cluster_node(&bucket, "fuzz", 2, Duration::from_secs(2)).await?;
    let url = node.node.url();
    let script = Arc::new(Script::default());
    let sdk = spawn_sdk(script.clone()).await?;
    common::register(&url, &sdk).await?;
    let client = reqwest::Client::new();

    // ── Phase 1: SDK response bodies.
    let n = cases(48);
    let mut ids = Vec::new();
    for i in 0..n {
        let turns: Vec<Vec<u8>> = sample(&mut r, &prop::collection::vec(sdk_body(), 1..4));
        script.bodies.lock().unwrap().extend(turns);
        let input: Vec<u8> = sample(&mut r, &http_body());
        let target = if i % 2 == 0 {
            "Fuzz/h/send".to_string()
        } else {
            format!("FuzzObj/h/send?key=k{}", i % 5)
        };
        let resp = client
            .post(format!("{url}/invoke/{target}"))
            .body(input)
            .timeout(Duration::from_secs(10))
            .send()
            .await?;
        if let Ok(v) = resp.json::<serde_json::Value>().await {
            if let Some(id) = v.get("id").and_then(|x| x.as_str()) {
                ids.push(id.to_string());
            }
        }
    }
    // Wait until SDK turns have been still for 1.5 s (spare bodies of invocations that
    // already ended are never consumed), capped at 30 s.
    let t0 = std::time::Instant::now();
    let (mut seen, mut still) = (usize::MAX, std::time::Instant::now());
    while t0.elapsed() < Duration::from_secs(30) {
        let now = script.turns.load(Ordering::SeqCst);
        if now != seen {
            (seen, still) = (now, std::time::Instant::now());
        } else if still.elapsed() > Duration::from_millis(1500) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    script.bodies.lock().unwrap().clear();
    healthy(&client, &url).await;
    for id in ids.iter().take(8) {
        let _ = client
            .get(format!("{url}/invocations/{id}/journal"))
            .timeout(Duration::from_secs(5))
            .send()
            .await?;
    }
    assert_no_panics("phase SDK", seed);

    // ── Phase 2: requests to tenant routes.
    let routes = tenant_routes();
    let per_route = cases(48).div_ceil(4).max(4);
    let mut five_xx = Vec::new();
    for (method, path) in &routes {
        for _ in 0..per_route {
            let mut p = fill(path, &mut r);
            if let Some(id) = ids.first().filter(|_| p.contains("/invocations/")) {
                // Half of the time aim at a real id, to get past the id lookup.
                if sample(&mut r, &any::<bool>()) {
                    p = path.replace("{id}", id);
                    p = fill(&p, &mut r);
                }
            }
            let q: String = sample(&mut r, &query_string());
            let body: Vec<u8> = sample(&mut r, &http_body());
            let req = client
                .request(method.parse().unwrap(), format!("{url}{p}?{q}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(body)
                // SSE and long-poll hold the connection: cut early, not an error.
                .timeout(Duration::from_millis(400));
            match req.send().await {
                Ok(resp) if resp.status().is_server_error() => {
                    let s = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    five_xx.push(format!("{method} {path} → {s} {text:.120}"));
                }
                _ => {}
            }
        }
    }
    healthy(&client, &url).await;
    // A 5xx is not a crash, but is the sign of an unnamed bug: print it for inspection.
    for e in &five_xx {
        eprintln!("fuzz_tenant_input: 5xx {e}");
    }
    assert_no_panics("phase HTTP", seed);
    eprintln!(
        "fuzz_tenant_input: {n} invocations, {} SDK turns, {} routes × {per_route} requests, {} 5xx responses",
        script.turns.load(Ordering::SeqCst),
        routes.len(),
        five_xx.len()
    );
    // Garbage the fuzz leaves behind (streams, half-written artifacts) is not what `verify` guards.
    drop(node);
    Ok(())
}

// ───────────────────────────────────────────────────────── regressions

/// A single node + a fake SDK that returns `bodies` in turn (then `Output` + `End`);
/// sends one invocation to `target`, waits `wait`, returns the status JSON and the
/// number of SDK turns received. A panic meanwhile turns the test red.
async fn one_invocation(
    target: &str,
    bodies: Vec<Vec<u8>>,
    wait: Duration,
) -> Result<(serde_json::Value, usize)> {
    let bucket = common::bucket();
    let node = common::start_cluster_node(&bucket, "fuzz", 2, Duration::from_secs(2)).await?;
    let url = node.node.url();
    let script = Arc::new(Script::default());
    script.bodies.lock().unwrap().extend(bodies);
    let sdk = spawn_sdk(script.clone()).await?;
    common::register(&url, &sdk).await?;
    // If the engine thread dies of a panic the request hangs: a cap turns the test red instead.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let sent: serde_json::Value = client
        .post(format!("{url}/invoke/{target}"))
        .body("{}")
        .send()
        .await?
        .json()
        .await?;
    let id = sent["id"].as_str().expect("has an id").to_string();
    tokio::time::sleep(wait).await;
    let status: serde_json::Value = client
        .get(format!("{url}/invocations/{id}"))
        .send()
        .await?
        .json()
        .await?;
    assert_no_panics(target, 0);
    let turns = script.turns.load(Ordering::SeqCst);
    drop(node);
    Ok((status, turns))
}

fn error_frame(next_retry_delay: Option<u64>) -> Vec<u8> {
    enc(
        MessageType::ERROR,
        Flags::NONE,
        &pb::ErrorMessage {
            code: 500,
            message: "handler error".into(),
            next_retry_delay,
            ..Default::default()
        },
    )
}

/// **A — retry delay overflow.** The SDK returns `Error` with `next_retry_delay = u64::MAX`.
///
/// 1. Turn 1: the state machine reports `Retry { delay_ms: Some(u64::MAX) }`.
/// 2. The lifecycle core (`turn_ended`) computes the due time `now + delay`.
///
/// Must see: no panic (debug: arithmetic overflow on the engine thread), no second
/// turn (release: the due time wraps into the past ⇒ fires again every tick), the
/// invocation still alive with turn 1's `lastFailure`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sdk_retry_delay_max_does_not_overflow() -> Result<()> {
    let _serial = panic_counter().await;
    let (status, turns) = one_invocation(
        "Fuzz/h/send",
        vec![error_frame(Some(u64::MAX))],
        Duration::from_millis(1500),
    )
    .await?;
    assert_eq!(turns, 1, "retried: SDK asked to wait forever: {status}");
    assert_eq!(status["status"], "running", "{status}");
    assert_eq!(status["lastFailure"]["code"], 500, "{status}");
    Ok(())
}

/// **C — a frame after the turn's outcome.** A `REQUEST_RESPONSE` body with an `Error`
/// (retryable) followed by an `End`.
///
/// 1. `Error` ⇒ state machine `Closed`, outcome `Failed { Retry }` (in RAM).
/// 2. `End` reaches a machine already `Closed` ⇒ violation 571 ⇒ the engine commits
///    `failed` and drops the retry row — but the outcome in RAM is still the first one
///    (`Retry`).
/// 3. The shell reports `Retry` to the core ⇒ the core sets a retry row for an
///    invocation already `failed` (`EndedButScheduled`, a tripwire of
///    `cairn_lifecycle::valid`).
///
/// Must see: as with `BIDI_STREAM` (reading stops at the first outcome), a frame after
/// the outcome is ignored; turn 2 runs as the retry and ends `completed`; no panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frames_after_turn_outcome_are_ignored() -> Result<()> {
    let _serial = panic_counter().await;
    let end = enc(MessageType::END, Flags::NONE, &pb::EndMessage {});
    let (status, turns) = one_invocation(
        "Fuzz/h/send",
        vec![[error_frame(Some(50)), end].concat()],
        Duration::from_millis(1500),
    )
    .await?;
    assert_eq!(status["status"], "completed", "{status}");
    assert_eq!(turns, 2, "{status}");
    Ok(())
}

/// **B — a turn that ends without `End`/`Suspension`/`Error`.** An empty
/// `REQUEST_RESPONSE` body, or only `Output` (no `End`).
///
/// 1. The state machine has no outcome yet; `settle_turn` reports `Failed 571`.
/// 2. Nobody commits `failed` (the machine does not `Finish`), but the lifecycle core
///    treats the outcome as terminal: it removes the queue entry and releases the VO lock.
/// 3. The durable status stays `running`, with no queue entry (`LiveButUnqueued`, a
///    tripwire of `cairn_lifecycle::valid`) — another invocation on the same object runs
///    concurrently; the watchdog row runs it again.
///
/// Must see: the broken turn is one **retryable failed** attempt (like an SDK that makes
/// no progress), `lastFailure` 571, the queue unchanged; turn 2 ends `completed`; no
/// panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn turn_without_end_is_a_retryable_failure() -> Result<()> {
    let _serial = panic_counter().await;
    let output = enc(
        MessageType::OUTPUT,
        Flags::NONE,
        &pb::OutputCommandMessage::default(),
    );
    let keepalive = enc(
        MessageType::KEEPALIVE,
        Flags::NONE,
        &pb::KeepaliveMessage::default(),
    );
    for (what, body) in [
        ("empty body", vec![]),
        ("only Keepalive", keepalive),
        ("Output without End", output),
    ] {
        let (status, turns) = one_invocation(
            "FuzzObj/h/send?key=k",
            vec![body],
            Duration::from_millis(2500),
        )
        .await?;
        assert_eq!(status["status"], "completed", "{what}: {status}");
        assert_eq!(turns, 2, "{what}: {status}");
    }
    Ok(())
}

/// **B on `BIDI_STREAM`.** The SDK opens an h2 stream, returns a `200` header, then closes
/// its half **cleanly** (`END_STREAM`, no `RST`) without sending any frame.
///
/// 1. Turn 1: `drive_bidi` reads the whole stream, the machine has no outcome ⇒
///    `settle_turn` takes the `None` branch (the same branch as `REQUEST_RESPONSE`).
/// 2. Turn 2 (on the retry): the SDK returns `Output` + `End`.
///
/// Must see: turn 1 is a retryable failed attempt — exactly two streams are opened, the
/// invocation `completed`, no panic (a tripwire of `cairn_lifecycle::valid`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bidi_clean_close_without_end_is_retried() -> Result<()> {
    let _serial = panic_counter().await;
    let sdk = BidiSdk::spawn().await?;
    let bucket = common::bucket();
    let node = common::start_cluster_node(&bucket, "fuzz", 2, Duration::from_secs(2)).await?;
    let url = node.node.url();
    common::register(&url, &sdk.url).await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let sent: serde_json::Value = client
        .post(format!("{url}/invoke/Bidi/h/send"))
        .body("{}")
        .send()
        .await?
        .json()
        .await?;
    let id = sent["id"].as_str().expect("has an id").to_string();
    let t0 = std::time::Instant::now();
    let status = loop {
        let v: serde_json::Value = client
            .get(format!("{url}/invocations/{id}"))
            .send()
            .await?
            .json()
            .await?;
        if v["status"] == "completed" || t0.elapsed() > Duration::from_secs(10) {
            break v;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_no_panics("BIDI clean close", 0);
    assert_eq!(status["status"], "completed", "{status}");
    assert_eq!(sdk.opened.load(Ordering::SeqCst), 2, "{status}");
    drop(node);
    Ok(())
}

/// A minimal fake `BIDI_STREAM` SDK (the template of `invoke_transient.rs`): h1 for
/// `/discover`, h2c for `/invoke`. The first stream closes cleanly with no frame, the
/// following streams return `Output` + `End`.
struct BidiSdk {
    url: String,
    opened: Arc<AtomicUsize>,
    accept: tokio::task::JoinHandle<()>,
}

impl Drop for BidiSdk {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

impl BidiSdk {
    async fn spawn() -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let opened = Arc::new(AtomicUsize::new(0));
        let n = Arc::clone(&opened);
        let accept = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(bidi_conn(socket, Arc::clone(&n)));
            }
        });
        Ok(Self {
            url,
            opened,
            accept,
        })
    }
}

async fn bidi_conn(mut socket: tokio::net::TcpStream, opened: Arc<AtomicUsize>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut first = [0u8; 1];
    if socket.peek(&mut first).await.ok() != Some(1) {
        return;
    }
    if first[0] != b'P' {
        // h1: only `/discover`.
        let mut head = Vec::new();
        while !head.windows(4).any(|w| w == b"\r\n\r\n") {
            let mut chunk = [0u8; 1024];
            match socket.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => head.extend_from_slice(&chunk[..n]),
            }
        }
        let manifest = EndpointManifest {
            protocol_mode: ProtocolMode::BidiStream,
            min_protocol_version: 1,
            max_protocol_version: 1,
            services: vec![ServiceManifest {
                name: "Bidi".into(),
                ty: ServiceType::Service,
                handlers: vec![HandlerManifest {
                    name: "h".into(),
                    ty: None,
                    contract: Default::default(),
                }],
                contract: Default::default(),
            }],
        };
        let body = serde_json::to_string(&manifest).unwrap();
        let reply = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: {DISCOVERY_V1}\r\ncontent-length: {}\r\n\
             connection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = socket.write_all(reply.as_bytes()).await;
        let _ = socket.shutdown().await;
        return;
    }
    let Ok(mut conn) = h2::server::handshake(socket).await else {
        return;
    };
    while let Some(Ok((req, mut respond))) = conn.accept().await {
        let n = opened.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            let resp = http::Response::builder()
                .status(200)
                .header("content-type", cairn_server::invoker::INVOCATION_V1)
                .body(())
                .unwrap();
            let Ok(mut send) = respond.send_response(resp, false) else {
                return;
            };
            let out = if n == 0 {
                Bytes::new()
            } else {
                Bytes::from(
                    [
                        enc(
                            MessageType::OUTPUT,
                            Flags::NONE,
                            &pb::OutputCommandMessage::default(),
                        ),
                        enc(MessageType::END, Flags::NONE, &pb::EndMessage {}),
                    ]
                    .concat(),
                )
            };
            if send.send_data(out, true).is_err() {
                return;
            }
            let mut body = req.into_body();
            while let Some(Ok(chunk)) = body.data().await {
                let _ = body.flow_control().release_capacity(chunk.len());
            }
        });
    }
}
