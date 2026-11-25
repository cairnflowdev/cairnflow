// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! End-to-end over HTTP: a fake SDK really running on a port, the runtime calling down to it.
//!
//! This is the whole chain running exactly as production will run it — discovery,
//! version negotiation, invoke, suspending on `ctx.sleep`, the timer firing, resuming.
//! The fake SDK implements exactly the `REQUEST_RESPONSE` constraints of the protocol:
//! no `REQUESTED_ACK`, and every time it needs to wait it ends the turn with `Suspension`.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::Result,
    axum::{
        extract::State,
        http::{header, HeaderMap, StatusCode},
        routing::{get, post},
        Router,
    },
    bytes::Bytes,
    cairn_engine::Engine,
    cairn_protocol::{encode_message, pb, Flags, Message, MessageType},
    cairn_server::{
        actor::{EngineHandle, Leadership, Readiness},
        discovery::{
            EndpointManifest, HandlerManifest, HandlerType, ProtocolMode, ServiceManifest,
            ServiceType, DISCOVERY_V1,
        },
        ingress::AppState,
        invoker::{decode_stream, encode_stream, INVOCATION_V1},
        router,
    },
    cairn_store::{LeadershipGrant, PartitionId, PartitionWriter},
    slatedb::{
        config::Settings,
        object_store::{local::LocalFileSystem, ObjectStore},
        Db,
    },
    std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration},
};

const WAKE_AT: u64 = 1_700_000_000_000;

// ───────────────────────────────────────────────────── Fake SDK over HTTP

#[derive(Clone, Debug)]
enum Step {
    GetState {
        key: &'static str,
        id: u32,
    },
    Sleep {
        at: u64,
        id: u32,
    },
    Run {
        id: u32,
        result: &'static str,
    },
    SetState {
        key: &'static str,
        value: &'static str,
    },
    Output {
        value: &'static str,
    },
}

fn charge_program() -> Vec<Step> {
    vec![
        Step::GetState {
            key: "count",
            id: 1,
        },
        Step::Sleep { at: WAKE_AT, id: 2 },
        Step::Run {
            id: 3,
            result: "charged",
        },
        Step::SetState {
            key: "count",
            value: "1",
        },
        Step::Output { value: "done" },
    ]
}

fn awaited(s: &Step) -> Option<u32> {
    match s {
        Step::GetState { id, .. } | Step::Sleep { id, .. } | Step::Run { id, .. } => Some(*id),
        _ => None,
    }
}

fn msg_completion_id(m: &Message) -> Option<u32> {
    Some(match m {
        Message::GetStateCompletion(n) => n.completion_id,
        Message::SleepCompletion(n) => n.completion_id,
        Message::RunCompletion(n) => n.completion_id,
        _ => return None,
    })
}

#[derive(Clone)]
struct SdkState {
    min_version: u32,
    max_version: u32,
    /// The number of turns the runtime called down to the SDK. Counted **per fake
    /// SDK**, not as a global: tests run in parallel in the same process.
    calls: Arc<std::sync::atomic::AtomicUsize>,
    /// This many first turns fail with `503`, then no more.
    ///
    /// Simulates an SDK that crashes/chokes briefly: the runtime gets
    /// `InvokeError::Status`, i.e. an `Err` rather than an `InvokeOutcome` — exactly the
    /// branch `schedule_after_error` exists to handle.
    fail_first: Arc<std::sync::atomic::AtomicUsize>,
}

impl SdkState {
    fn v1() -> Self {
        Self::versions(1, 1)
    }

    fn versions(min_version: u32, max_version: u32) -> Self {
        Self {
            min_version,
            max_version,
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            fail_first: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// The SDK fails its first `n` turns, then recovers.
    fn fail_first_turns(self, n: usize) -> Self {
        self.fail_first
            .store(n, std::sync::atomic::Ordering::Relaxed);
        self
    }

    fn call_count(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::Relaxed)
    }
}

fn sdk_manifest(st: &SdkState) -> EndpointManifest {
    EndpointManifest {
        protocol_mode: ProtocolMode::RequestResponse,
        min_protocol_version: st.min_version,
        max_protocol_version: st.max_version,
        services: vec![ServiceManifest {
            name: "OrderService".into(),
            ty: ServiceType::VirtualObject,
            handlers: vec![HandlerManifest {
                name: "submit".into(),
                ty: Some(HandlerType::Exclusive),
                contract: Default::default(),
            }],
            contract: Default::default(),
        }],
    }
}

async fn sdk_discover(
    State(st): State<SdkState>,
) -> ([(header::HeaderName, &'static str); 1], String) {
    (
        [(header::CONTENT_TYPE, DISCOVERY_V1)],
        serde_json::to_string(&sdk_manifest(&st)).unwrap(),
    )
}

/// One SDK turn: rebuild the state from the replayed journal, then run on.
async fn sdk_invoke(State(st): State<SdkState>, body: Bytes) -> (StatusCode, HeaderMap, Bytes) {
    st.calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if st
        .fail_first
        .fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |n| n.checked_sub(1),
        )
        .is_ok()
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            HeaderMap::new(),
            Bytes::from_static(b"sdk is stalled"),
        );
    }
    let program = charge_program();
    let frames = decode_stream(body).expect("the runtime sends valid frames");

    let mut done: BTreeSet<u32> = BTreeSet::new();
    let mut journaled_commands = 0usize;
    for f in &frames {
        // REQUESTED_ACK must never come from the runtime.
        assert!(
            !f.flags.requested_ack(),
            "the runtime set REQUESTED_ACK — wrong direction"
        );
        let m = cairn_protocol::decode(f).expect("decodable");
        match &m {
            Message::Start(_) | Message::Input(_) => {}
            other => match msg_completion_id(other) {
                Some(id) => {
                    done.insert(id);
                }
                None => journaled_commands += 1,
            },
        }
    }

    let mut out: Vec<Message> = Vec::new();
    let mut cursor = 0usize;
    let mut suspended_on: Option<u32> = None;

    while cursor < program.len() {
        let step = program[cursor].clone();
        if cursor >= journaled_commands {
            out.push(command_of(&step));
        }
        // `ctx.run`: the SDK knows the result on the spot, so it proposes it and moves on
        // at once — exactly the protocol's REQUEST_RESPONSE constraint ("must end with a
        // suspension or an output within the same turn").
        if let Step::Run { id, result } = &step {
            if !done.contains(id) {
                out.push(Message::ProposeRunCompletion(
                    pb::ProposeRunCompletionMessage {
                        result_completion_id: *id,
                        result: Some(pb::propose_run_completion_message::Result::Value(
                            Bytes::from_static(result.as_bytes()),
                        )),
                    },
                ));
                done.insert(*id);
            }
        }
        match awaited(&step) {
            Some(id) if !done.contains(&id) => {
                suspended_on = Some(id);
                break;
            }
            _ => cursor += 1,
        }
    }

    if let Some(id) = suspended_on {
        out.push(Message::Suspension(pb::SuspensionMessage {
            waiting_completions: vec![id],
            ..Default::default()
        }));
    } else {
        out.push(Message::End(pb::EndMessage {}));
    }

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, INVOCATION_V1.parse().unwrap());
    (StatusCode::OK, headers, encode_stream(&out))
}

fn command_of(step: &Step) -> Message {
    match step {
        Step::GetState { key, id } => Message::GetState(pb::GetStateCommandMessage {
            key: Bytes::from_static(key.as_bytes()),
            result_completion_id: *id,
            name: String::new(),
        }),
        Step::Sleep { at, id } => Message::Sleep(pb::SleepCommandMessage {
            wake_up_time: *at,
            result_completion_id: *id,
            name: String::new(),
        }),
        Step::Run { id, .. } => Message::Run(pb::RunCommandMessage {
            result_completion_id: *id,
            name: String::new(),
        }),
        Step::SetState { key, value } => Message::SetState(pb::SetStateCommandMessage {
            key: Bytes::from_static(key.as_bytes()),
            value: Some(cairn_protocol::value::inline(Bytes::from_static(
                value.as_bytes(),
            ))),
            name: String::new(),
        }),
        Step::Output { value } => Message::Output(pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from_static(value.as_bytes())),
            )),
            name: String::new(),
        }),
    }
}

async fn spawn_sdk(state: SdkState) -> Result<String> {
    let app = Router::new()
        .route("/discover", get(sdk_discover))
        .route("/invoke/{service}/{handler}", post(sdk_invoke))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(format!("http://{addr}"))
}

// ───────────────────────────────────────────────────── runtime

async fn spawn_runtime(name: &str) -> Result<(String, EngineHandle)> {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;

    let registry = cairn_server::Registry::new();
    // No background loops: this test drives time through /admin/tick.
    let handle = EngineHandle::spawn(
        Box::new(move || {
            let root = root.clone();
            Box::pin(async move {
                let store: Arc<dyn ObjectStore> =
                    Arc::new(LocalFileSystem::new_with_prefix(&root).expect("open local fs"));
                let settings = Settings {
                    flush_interval: Some(Duration::from_millis(10)),
                    ..Default::default()
                };
                let db = Db::builder("partition-0", store)
                    .with_settings(settings)
                    .build()
                    .await
                    .expect("open SlateDB");
                let writer =
                    PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
                Ok(Leadership {
                    engine: Engine::open(writer).await?,
                    keeper: None,
                })
            })
        }),
        registry.clone(),
        None,
        Readiness::WhenLeader,
    )?;

    let app = router(AppState::new(handle.clone(), registry));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok((format!("http://{addr}"), handle))
}

// ───────────────────────────────────────────────────── test

/// The full chain: register → call → suspend on `ctx.sleep` → tick → done.
#[tokio::test]
async fn runs_over_http() -> Result<()> {
    let sdk_state = SdkState::v1();
    let sdk = spawn_sdk(sdk_state.clone()).await?;
    let (rt, handle) = spawn_runtime("e2e").await?;
    let http = reqwest::Client::new();

    // ── Register the deployment: discovery + version negotiation.
    let reg: serde_json::Value = http
        .post(format!("{rt}/deployments"))
        .json(&serde_json::json!({ "uri": sdk }))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(reg["wireVersion"], 1, "wrong version negotiation: {reg}");
    assert_eq!(reg["services"][0], "OrderService");

    // ── Call. The program has a ctx.sleep, so the first turn must return "waiting".
    let resp = http
        .post(format!("{rt}/invoke/OrderService/submit?key=order-42"))
        .body("req")
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        StatusCode::ACCEPTED,
        "an invocation with ctx.sleep must return 202, not block"
    );
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["status"], "waiting");
    let id = body["id"].as_str().unwrap().to_string();

    // ── Exactly **two** calls down to the SDK to reach the suspension point, not three.
    //
    // Turn 1: the SDK issues `GetState` and suspends. The runtime resolves it within
    //         that turn (reading state is its own job), so it must call again — the SDK
    //         has not seen the result, because it was recorded after the request was built.
    // Turn 2: the SDK issues `Sleep` and suspends. This time the runtime can resolve
    //         **nothing**: it is waiting for the clock. Calling again would only cost a
    //         round-trip to get exactly the same suspension.
    //
    // Telling those two cases apart must be measured by "is what the SDK waits for in
    // the journal yet", not by "did the journal grow" — the very `Sleep` command that
    // causes the suspension also grows the journal, so counting entries would make
    // every suspension cost an extra turn.
    assert_eq!(
        sdk_state.call_count(),
        2,
        "an extra call down to the SDK at the suspension point — every external wait costs a useless round-trip"
    );

    // ── Before the deadline a tick does nothing.
    let early: serde_json::Value = http
        .post(format!("{rt}/admin/tick?now_ms={}", WAKE_AT - 1))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(early["advanced"], 0, "a timer fired before its deadline");

    // ── At the deadline: the timer fires, the invocation runs on to the end.
    let tick: serde_json::Value = http
        .post(format!("{rt}/admin/tick?now_ms={WAKE_AT}"))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(tick["advanced"], 1);

    // A tick only **records** the wake-up and hands the invocation to the scheduler —
    // the turn runs behind this request, so ask until it is done.
    let mut st = serde_json::Value::Null;
    for _ in 0..250 {
        st = http
            .get(format!("{rt}/invocations/{id}"))
            .send()
            .await?
            .json()
            .await?;
        if st["status"] == "completed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(st["status"], "completed", "{st}");
    assert_eq!(st["output"], "done");

    // The timer firing costs exactly one more turn: loading the completion needs no
    // call to the SDK, only the turn after that asks — and that turn runs straight to `Output`.
    assert_eq!(
        sdk_state.call_count(),
        3,
        "total SDK calls for the whole invocation"
    );

    handle.shutdown().await?;
    Ok(())
}

/// No overlap in wire versions ⇒ refused **at registration**, not at invoke time. The
/// operator must know right at deploy time.
#[tokio::test]
async fn disjoint_versions_rejected_at_registration() -> Result<()> {
    // Above the runtime's maximum — computed from `WIRE_MAX` rather than a hard-coded
    // number, so that the next version bump does not turn this test falsely green.
    let above = cairn_server::discovery::WIRE_MAX + 1;
    let sdk = spawn_sdk(SdkState::versions(above, above + 2)).await?;
    let (rt, handle) = spawn_runtime("e2e-version").await?;
    let http = reqwest::Client::new();

    let resp = http
        .post(format!("{rt}/deployments"))
        .json(&serde_json::json!({ "uri": sdk }))
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await?;
    assert!(
        body["error"].as_str().unwrap().contains("wire version"),
        "the error must say why: {body}"
    );

    // And no deployment was recorded.
    let list: serde_json::Value = http
        .get(format!("{rt}/deployments"))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(list.as_array().unwrap().len(), 0);

    handle.shutdown().await?;
    Ok(())
}

/// Idempotency at the HTTP door: a client retrying with the same `Idempotency-Key`
/// attaches to the existing invocation instead of spawning a new one — and gets **the
/// same id**, because the id is derived from the key set, not generated per request.
#[tokio::test]
async fn idempotency_key_at_http_ingress() -> Result<()> {
    let sdk_state = SdkState::v1();
    let sdk = spawn_sdk(sdk_state.clone()).await?;
    let (rt, handle) = spawn_runtime("e2e-idem").await?;
    let http = reqwest::Client::new();

    http.post(format!("{rt}/deployments"))
        .json(&serde_json::json!({ "uri": sdk }))
        .send()
        .await?
        .error_for_status()?;

    let call_with = |idem: &str| {
        http.post(format!("{rt}/invoke/OrderService/submit?key=order-7"))
            .header("Idempotency-Key", idem.to_owned())
            .body("req")
            .send()
    };

    // The first turn creates the invocation, suspended on ctx.sleep.
    let body: serde_json::Value = call_with("txn-1").await?.json().await?;
    assert_eq!(body["status"], "waiting", "{body}");
    let id = body["id"].as_str().unwrap().to_string();
    let calls_after_first = sdk_state.call_count();

    // A retry with the same key: **the same id**, no new invocation, no extra SDK call.
    let repeated: serde_json::Value = call_with("txn-1").await?.json().await?;
    assert_eq!(repeated["status"], "waiting", "{repeated}");
    assert_eq!(
        repeated["id"].as_str().unwrap(),
        id,
        "a retry with the same Idempotency-Key must point at the same old invocation"
    );
    assert_eq!(
        sdk_state.call_count(),
        calls_after_first,
        "a duplicate call must not run any more SDK turns"
    );

    // Once done, a retry gets the result directly — not a 202 asking it to keep waiting.
    http.post(format!("{rt}/admin/tick?now_ms={WAKE_AT}"))
        .send()
        .await?
        .error_for_status()?;
    let finished = call_with("txn-1").await?;
    assert_eq!(finished.status(), StatusCode::OK);
    let finished: serde_json::Value = finished.json().await?;
    assert_eq!(finished["status"], "completed", "{finished}");
    assert_eq!(finished["id"].as_str().unwrap(), id);
    assert_eq!(finished["output"], "done");

    // Another key is another invocation. (Same object key, so it queues behind —
    // waiting — but the id must differ.)
    let other_key: serde_json::Value = call_with("txn-2").await?.json().await?;
    assert_ne!(
        other_key["id"].as_str().unwrap(),
        id,
        "a new key must give a new id"
    );

    handle.shutdown().await?;
    Ok(())
}

/// Calling a service that is not registered must give a clear 404, not an opaque lower-layer error.
#[tokio::test]
async fn calling_unregistered_service_returns_404() -> Result<()> {
    let (rt, handle) = spawn_runtime("e2e-404").await?;
    let http = reqwest::Client::new();

    let resp = http
        .post(format!("{rt}/invoke/NoSuchService/submit"))
        .body("x")
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    handle.shutdown().await?;
    Ok(())
}

/// The runtime MUST NOT set `REQUESTED_ACK` — that flag is the SDK asking the runtime,
/// not the other way round. The fake SDK asserts this on every frame it receives, so
/// every test going through it enforces the property; here it is asserted explicitly.
#[tokio::test]
async fn runtime_does_not_set_requested_ack() -> Result<()> {
    let msgs = vec![Message::Start(pb::StartMessage::default())];
    let frames = decode_stream(encode_stream(&msgs))?;
    assert!(!frames[0].flags.requested_ack());
    assert_eq!(frames[0].ty, MessageType::START);
    // And encode_message with Flags::NONE never sets that bit.
    let wire = encode_message(
        MessageType::RUN,
        Flags::NONE,
        &pb::RunCommandMessage::default(),
    );
    assert_eq!(wire[2] & 0x80, 0);
    Ok(())
}

/// **A turn that fails because of the SDK must leave a retry scheduled — otherwise the
/// invocation lies still.**
///
/// This is the case a load test caught at a rate of 1/20 000, and the worst a durable
/// execution engine can suffer: an acknowledged call that vanishes.
///
/// The bug's path: the turn throws an `Err` (not an `InvokeOutcome::Retry`), so
/// `schedule_if_retry` never looks at it. `settle_permit(.., None)` puts the vqueue
/// back to `Suspended`, releases the permit, and **deliberately keeps the VO lock**.
/// From then on nothing wakes it: `tick` only picks up due timers, `fire_retries` only
/// picks up retry rows, and no row exists. The invocation stays `Running` until the
/// next leadership change — which never comes on a single node. The VO lock stays
/// too, so every later call on the same object key dies with it.
///
/// This test guards the "must leave a retry scheduled" part. Remove
/// `schedule_after_error` and the last line turns red: the invocation stands in
/// `running` forever.
///
/// What `/invoke` answers: a turn failing **after** the invocation was durably
/// accepted answers at the door like a retryable handler error — this runtime has no
/// wait bound (`AppState::new`, like `--invoke-wait-ms 0`), so it is `202 waiting`
/// with the id.
#[tokio::test]
async fn transient_sdk_error_does_not_wedge_invocation() -> Result<()> {
    // Fail exactly the first turn, then recover — an SDK choking for a beat, a common thing.
    let sdk_state = SdkState::v1().fail_first_turns(1);
    let sdk = spawn_sdk(sdk_state.clone()).await?;
    let (rt, handle) = spawn_runtime("e2e-retry-after-error").await?;
    let http = reqwest::Client::new();

    http.post(format!("{rt}/deployments"))
        .json(&serde_json::json!({ "uri": sdk }))
        .send()
        .await?;

    // ── The first turn fails. The invocation is durably accepted and rescheduled: the
    // client gets `202 waiting` with the id — not an error, and not a result (nothing to
    // promise yet beyond "accepted").
    let resp = http
        .post(format!("{rt}/invoke/OrderService/submit?key=order-1"))
        .body("req")
        .send()
        .await?;
    let code = resp.status();
    // **It must carry the id.** The invocation is durable and will be rescheduled; not
    // naming it would make the caller lose track of work that is still running.
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(
        (code.as_u16(), body["status"].as_str()),
        (202, Some("waiting")),
        "the SDK returned 503 on the first turn: /invoke does not wait (bound off), returns 202 waiting: {body}"
    );
    let id = body["id"]
        .as_str()
        .unwrap_or_else(|| panic!("202 without an id, the client cannot look it up: {body}"))
        .to_string();
    assert_eq!(id.len(), 48, "the id must be 48 hex characters: {body}");
    assert_eq!(sdk_state.call_count(), 1);

    // ── The background tick runs. The scheduled retry must be here, otherwise there is
    // nothing to fire.
    //
    // `now_ms` is taken from the **real clock** plus one hour, not from `WAKE_AT`: the
    // retry row is scheduled with the process's `now_ms()`, while `WAKE_AT` is a fixed
    // mark in 2023 that the fake SDK sets for `ctx.sleep`. Ticking with a past mark
    // would fire the program's timer but not the retry — and the test would turn red
    // for exactly the wrong reason.
    let far_future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64
        + 3_600_000;
    // Ticks: one for the retry after the error, one for the program's `ctx.sleep`. A
    // tick only hands the invocation to the scheduler, so loop until it is done instead
    // of counting exactly two ticks.
    let mut all = serde_json::Value::Null;
    for _ in 0..100 {
        http.post(format!("{rt}/admin/tick?now_ms={far_future}"))
            .send()
            .await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        all = http
            .get(format!("{rt}/invocations"))
            .send()
            .await?
            .json()
            .await?;
        if all[0]["status"] == "completed" {
            break;
        }
    }

    assert!(
        sdk_state.call_count() > 1,
        "no scheduled retry fired — the invocation lies still after a transient error"
    );

    let rows = all.as_array().expect("invocation list");
    assert_eq!(rows.len(), 1, "there must be exactly one invocation: {all}");
    assert_eq!(
        rows[0]["status"], "completed",
        "the invocation lies still after a transient SDK error: {all}"
    );
    // And the id in the earlier error really is its id — otherwise the client could
    // look something up, just not the thing that failed.
    assert_eq!(rows[0]["id"], id, "the id in the error is not its id");

    handle.shutdown().await?;
    Ok(())
}
