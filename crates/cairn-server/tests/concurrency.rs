// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The engine thread no longer stands waiting for the SDK.
//!
//! Three properties, and they are the whole contract of moving SDK calls off the
//! engine thread:
//!
//! 1. **Introspection does not queue behind a slow turn.** This is the original
//!    symptom: `/invocations/{id}` could not answer exactly when the operator needed
//!    it most.
//! 2. **Two invocations run their turns interleaved.** Proven with a gate that
//!    requires *two* turns to be present at once before it opens — the old sequential
//!    loop would hang here, not slower but never finishing.
//! 3. **One invocation is still one turn at a time.** Parallelism stops exactly at
//!    that boundary: two requests with the same idempotency key arriving at the same
//!    time still run the handler only once.
//!
//! The fake SDK here is deliberately **slow and controllable**: it blocks mid-turn
//! until the test grants a permit. A fast SDK proves nothing — what needs measuring is
//! what happens while the SDK has *not yet* answered.

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
    cairn_engine::{Engine, InvocationStatus, Target},
    cairn_protocol::{pb, Message},
    cairn_server::{
        actor::{EngineHandle, InvokeOutcome, Leadership, Readiness, TurnLimits},
        discovery::{
            EndpointManifest, HandlerManifest, ProtocolMode, ServiceManifest, ServiceType,
            DISCOVERY_V1,
        },
        invoker::{decode_stream, encode_stream, INVOCATION_V1},
        Registry,
    },
    cairn_store::{LeadershipGrant, PartitionId, PartitionWriter},
    slatedb::{
        config::Settings,
        object_store::{local::LocalFileSystem, ObjectStore},
        Db,
    },
    std::{
        path::PathBuf,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    },
    tokio::sync::Semaphore,
};

/// The time bound for every wait in this file.
///
/// The tests here tell "can run in parallel" apart from "hangs", so they **must** be
/// bounded — without it a regression becomes a test suite hanging forever in CI, and
/// that is the hardest kind of failure to diagnose.
const TIMEOUT: Duration = Duration::from_secs(10);

// ───────────────────────────────────────────────────── Fake SDK, blockable

#[derive(Clone)]
struct SlowSdk {
    /// The number of turns that have *entered* the gate. Incremented before waiting,
    /// so it measures "how many turns are inside the SDK at once", exactly what
    /// property 2 needs.
    entered: Arc<AtomicUsize>,
    /// The gate: each turn consumes a permit before it is answered. The test grants
    /// permits to decide when the SDK is "done".
    gate: Arc<Semaphore>,
    /// The first turn suspends on a `ctx.sleep` instead of running straight through.
    ///
    /// To get a turn started by a **timer** — that is, a background path, where
    /// `Retry` used to be dropped. That case cannot be built with `invoke`.
    sleep_first: bool,
}

impl SlowSdk {
    fn new() -> Self {
        Self {
            entered: Arc::new(AtomicUsize::new(0)),
            gate: Arc::new(Semaphore::new(0)),
            sleep_first: false,
        }
    }

    fn sleeping_first() -> Self {
        Self {
            sleep_first: true,
            ..Self::new()
        }
    }

    fn entered(&self) -> usize {
        self.entered.load(Ordering::Relaxed)
    }

    /// Wait until `n` turns are inside the SDK at once.
    async fn wait_until_entered(&self, n: usize) -> Result<()> {
        tokio::time::timeout(TIMEOUT, async {
            while self.entered() < n {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "only {}/{n} turns got into the SDK — turns do not run in parallel",
                self.entered()
            )
        })
    }
}

fn manifest() -> EndpointManifest {
    EndpointManifest {
        protocol_mode: ProtocolMode::RequestResponse,
        min_protocol_version: 1,
        max_protocol_version: 1,
        services: vec![ServiceManifest {
            // `SERVICE`, not `VIRTUAL_OBJECT`: the `EXCLUSIVE` lock by object key would
            // also serialise the two invocations, and it would make property 2 hold for
            // the wrong reason.
            name: "Slow".into(),
            ty: ServiceType::Service,
            handlers: vec![HandlerManifest {
                name: "work".into(),
                ty: None,
                contract: Default::default(),
            }],
            contract: Default::default(),
        }],
    }
}

async fn sdk_discover() -> ([(header::HeaderName, &'static str); 1], String) {
    (
        [(header::CONTENT_TYPE, DISCOVERY_V1)],
        serde_json::to_string(&manifest()).unwrap(),
    )
}

/// One turn: enter, wait at the gate, then `Output` + `End`. Journals nothing else —
/// this test measures scheduling, not journal semantics.
async fn sdk_invoke(State(st): State<SlowSdk>, body: Bytes) -> (StatusCode, HeaderMap, Bytes) {
    let frames = decode_stream(body).expect("the runtime sends valid frames");

    // The first turn in `sleep_first` mode: suspends on a `ctx.sleep` due at once.
    // Recognised by "does the journal have the sleep's completion yet" rather than by
    // counting turns — a turn dropped for exceeding its bound runs again, and a
    // counter would get out of phase.
    let slept = frames
        .iter()
        .any(|f| matches!(cairn_protocol::decode(f), Ok(Message::SleepCompletion(_))));
    if st.sleep_first && !slept {
        let out = vec![
            Message::Sleep(pb::SleepCommandMessage {
                // Due at once: we need a turn started by a timer, not a measurement
                // of timer precision.
                wake_up_time: now_ms(),
                result_completion_id: 1,
                name: String::new(),
            }),
            Message::Suspension(pb::SuspensionMessage {
                waiting_completions: vec![1],
                ..Default::default()
            }),
        ];
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, INVOCATION_V1.parse().unwrap());
        return (StatusCode::OK, headers, encode_stream(&out));
    }

    st.entered.fetch_add(1, Ordering::Relaxed);
    let permit = st.gate.acquire().await.expect("the gate is not closed");
    permit.forget();

    let out = vec![
        Message::Output(pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from_static(b"done")),
            )),
            name: String::new(),
        }),
        Message::End(pb::EndMessage {}),
    ];
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, INVOCATION_V1.parse().unwrap());
    (StatusCode::OK, headers, encode_stream(&out))
}

async fn spawn_sdk(st: SlowSdk) -> Result<String> {
    let app = Router::new()
        .route("/discover", get(sdk_discover))
        .route("/invoke/{service}/{handler}", post(sdk_invoke))
        .with_state(st);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(format!("http://{addr}"))
}

// ───────────────────────────────────────────────────── runtime

async fn spawn_runtime(name: &str, endpoint: String) -> Result<EngineHandle> {
    // No time bound: the scheduling tests deliberately keep the SDK standing still
    // for a long time, and a bound cutting in would kill exactly what they measure.
    spawn_runtime_with(name, endpoint, None, None).await
}

async fn spawn_runtime_with(
    name: &str,
    endpoint: String,
    sdk_timeout: Option<Duration>,
    tick_interval: Option<Duration>,
) -> Result<EngineHandle> {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;

    let registry = Registry::new();
    registry.upsert(cairn_server::discovery::Deployment {
        // A fixed id: a test that rebuilds the registry after a "failover" must find
        // again exactly the deployment the old invocation is bound to.
        id: "dp_concurrency".into(),
        tenant: "default".into(),
        created_at_ms: 0,
        endpoint,
        wire_version: 1,
        protocol_mode: ProtocolMode::RequestResponse,
        request_identity: None,
        rr_compression: None,
        replay_cache: false,
        transport: None,
        services: manifest().services,
    });

    EngineHandle::spawn_with_transport(
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
        registry,
        tick_interval,
        Readiness::WhenLeader,
        cairn_server::transport::Transport::disconnected(),
        TurnLimits {
            sdk_timeout,
            ..TurnLimits::default()
        },
        None,
    )
    .map_err(Into::into)
}

fn target() -> Target {
    Target {
        tenant: Default::default(),
        scope: None,
        service: "Slow".into(),
        handler: "work".into(),
        object_key: String::new(),
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn id(n: u8) -> [u8; 24] {
    let mut b = [0u8; 24];
    b[15] = n;
    b
}

// ───────────────────────────────────────────────────── test

/// Property 1: a status query can be answered **while** a turn is waiting for the
/// SDK.
///
/// Before the split, `Status` sat behind `Invoke` in one and the same queue, so the
/// wait below would run out of time instead of answering.
#[tokio::test]
async fn introspection_does_not_queue_behind_a_slow_turn() -> Result<()> {
    let sdk = SlowSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let handle = spawn_runtime("conc-introspection", endpoint).await?;

    let h = handle.clone();
    let chay = tokio::spawn(async move {
        h.invoke(id(1), target(), None, None, Bytes::from_static(b"{}"))
            .await
    });

    // The SDK has entered and sits at the gate ⇒ the engine thread *is* in the middle of a turn.
    sdk.wait_until_entered(1).await?;

    let status = tokio::time::timeout(TIMEOUT, handle.status(id(1)))
        .await
        .map_err(|_| {
            anyhow::anyhow!("status() timed out — the engine thread is still held by the SDK call")
        })??;
    assert!(
        status.is_some(),
        "a running invocation must have a readable status, got None"
    );

    let ds = tokio::time::timeout(TIMEOUT, handle.list_invocations(16, None))
        .await
        .map_err(|_| anyhow::anyhow!("list_invocations() timed out"))??;
    assert_eq!(ds.len(), 1, "must see exactly the running invocation");
    // Time filters (CG-11) do not queue behind the turn either.
    let timed = cairn_engine::ListFilter {
        created_after: Some(0),
        created_desc: true,
        ..Default::default()
    };
    let ds = tokio::time::timeout(TIMEOUT, handle.list_invocations_where(16, timed))
        .await
        .map_err(|_| anyhow::anyhow!("list_invocations_where(time filter) timed out"))??;
    assert_eq!(
        ds.rows.len(),
        1,
        "the time filter must see the running invocation"
    );
    assert!(ds.rows[0].times.created_ms.is_some());

    // Open the gate and let the turn finish — a test ending halfway leaves an engine
    // thread waiting, and it would haunt other tests in the same process.
    sdk.gate.add_permits(1);
    let outcome = tokio::time::timeout(TIMEOUT, chay).await??;
    assert_eq!(
        outcome?,
        InvokeOutcome::Completed(Bytes::from_static(b"done").into())
    );
    handle.shutdown().await?;
    Ok(())
}

/// Property 2: two invocations inside the SDK at the same time.
///
/// The gate only opens after **both** have entered, so the old sequential loop hangs
/// here: the second turn never starts because the first has not answered.
#[tokio::test]
async fn two_invocations_interleave_turns() -> Result<()> {
    let sdk = SlowSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let handle = spawn_runtime("conc-parallel", endpoint).await?;

    let mut chay = Vec::new();
    for n in 1..=2u8 {
        let h = handle.clone();
        chay.push(tokio::spawn(async move {
            h.invoke(id(n), target(), None, None, Bytes::from_static(b"{}"))
                .await
        }));
    }

    sdk.wait_until_entered(2).await?;
    sdk.gate.add_permits(2);

    for t in chay {
        let outcome = tokio::time::timeout(TIMEOUT, t).await??;
        assert_eq!(
            outcome?,
            InvokeOutcome::Completed(Bytes::from_static(b"done").into())
        );
    }
    handle.shutdown().await?;
    Ok(())
}

/// Property 3: parallelism stops at the boundary of one invocation.
///
/// Two requests with the same idempotency key carry the same id (the id is derived
/// from the key set). Without a per-invocation lock both would see "absent" in the
/// `idempotency` table and both would create — the idempotency guarantee breaks, and
/// the user sees side effects run twice.
#[tokio::test]
async fn same_idempotency_key_runs_once() -> Result<()> {
    let sdk = SlowSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let handle = spawn_runtime("conc-idempotency", endpoint).await?;

    // The gate is open from the start: this test measures *how many times* the handler
    // runs, not scheduling.
    sdk.gate.add_permits(8);

    let mut chay = Vec::new();
    for _ in 0..2 {
        let h = handle.clone();
        chay.push(tokio::spawn(async move {
            h.invoke(
                id(7),
                target(),
                None,
                Some("shared-key".into()),
                Bytes::from_static(b"{}"),
            )
            .await
        }));
    }
    for t in chay {
        tokio::time::timeout(TIMEOUT, t).await???;
    }

    assert_eq!(
        sdk.entered(),
        1,
        "two requests with the same idempotency key ran the handler {} times — idempotency broken",
        sdk.entered()
    );
    assert!(
        matches!(
            handle.status(id(7)).await?,
            Some(InvocationStatus::Completed(_))
        ),
        "the invocation must end completed"
    );
    handle.shutdown().await?;
    Ok(())
}

/// Property 4: the SDK makes no progress ⇒ the turn is dropped, **the permit is
/// returned**.
///
/// Without a time bound a hung handler holds its permit forever; enough of them and
/// the concurrency cap runs dry and the partition stalls — slower than before the
/// split, but stalled all the same. What is measured here is *what follows* the
/// dropped turn: a second invocation must be able to run, i.e. the permit is back.
#[tokio::test]
async fn stalled_sdk_abandons_turn_and_returns_permit() -> Result<()> {
    let sdk = SlowSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    // A short bound so the test runs fast; the gate **never opens** for the first turn.
    let handle = spawn_runtime_with(
        "conc-timeout",
        endpoint,
        Some(Duration::from_millis(300)),
        None,
    )
    .await?;

    let outcome = tokio::time::timeout(
        TIMEOUT,
        handle.invoke(id(1), target(), None, None, Bytes::from_static(b"{}")),
    )
    .await
    .map_err(|_| anyhow::anyhow!("invoke did not return — the time bound did not fire"))??;
    assert!(
        matches!(outcome, InvokeOutcome::Retry { .. }),
        "running out of time must yield Retry (the same shape as an SDK reporting a retryable error), got {outcome:?}"
    );

    // The permit is back: open the gate and call another invocation, it must get through.
    sdk.gate.add_permits(4);
    let outcome = tokio::time::timeout(
        TIMEOUT,
        handle.invoke(id(2), target(), None, None, Bytes::from_static(b"{}")),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the next invocation is stuck — the permit was not returned"))??;
    assert_eq!(
        outcome,
        InvokeOutcome::Completed(Bytes::from_static(b"done").into())
    );

    handle.shutdown().await?;
    Ok(())
}

/// Property 5: after a turn is dropped **someone schedules a rerun**.
///
/// This is the second half of the feature, and the easy half to forget: returning
/// the permit without rescheduling trades "a hung handler" for "an invocation lying
/// still forever" — quieter, not better. Before this change, `Retry` from every
/// background path was dropped; only `run_invocation` and `fire_retries` handled it.
///
/// The measurement: let the first turn run out of time, then open the gate and let
/// the background tick run. No call again — the scheduled retry itself must bring the
/// invocation home.
#[tokio::test]
async fn abandoned_turn_reschedules_itself() -> Result<()> {
    let sdk = SlowSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let handle = spawn_runtime_with(
        "conc-retry-after-timeout",
        endpoint,
        Some(Duration::from_millis(300)),
        // Background tick on: it is exactly what must pick up the scheduled retry.
        Some(Duration::from_millis(50)),
    )
    .await?;

    let outcome = tokio::time::timeout(
        TIMEOUT,
        handle.invoke(id(3), target(), None, None, Bytes::from_static(b"{}")),
    )
    .await??;
    assert!(matches!(outcome, InvokeOutcome::Retry { .. }));

    // Open the gate. From here nobody calls anything — if the invocation gets home it
    // can only be thanks to the scheduled retry.
    sdk.gate.add_permits(8);
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if matches!(
                handle.status(id(3)).await,
                Ok(Some(InvocationStatus::Completed(_)))
            ) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("no rerun was scheduled — the invocation lies still"))?;

    handle.shutdown().await?;
    Ok(())
}

/// Property 6: a turn dropped on a **background path** is rescheduled too.
///
/// Property 5 goes through `run_invocation`, which already scheduled on its own — so
/// it cannot guard the part most prone to rot. Here the failing turn is started by a
/// **timer**, and before this change `tick` dropped `Retry`: the invocation stayed
/// `Running` with nothing to wake it, until the next leadership change.
///
/// Scenario: turn 1 suspends on a `ctx.sleep` (due at once) ⇒ the timer fires ⇒ turn 2
/// is started by the background tick and runs out of time ⇒ open the gate ⇒ only the
/// scheduled retry can bring it home.
#[tokio::test]
async fn abandoned_background_turn_is_rescheduled_too() -> Result<()> {
    let sdk = SlowSdk::sleeping_first();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let handle = spawn_runtime_with(
        "conc-retry-background",
        endpoint,
        Some(Duration::from_millis(300)),
        Some(Duration::from_millis(50)),
    )
    .await?;

    let outcome = tokio::time::timeout(
        TIMEOUT,
        handle.invoke(id(4), target(), None, None, Bytes::from_static(b"{}")),
    )
    .await??;
    assert_eq!(
        outcome,
        InvokeOutcome::Waiting,
        "the first turn must suspend on ctx.sleep"
    );

    // Wait for the background tick to fire the timer and take at least one timeout —
    // i.e. a turn has failed on a background path, exactly the case to guard.
    sdk.wait_until_entered(1).await?;
    tokio::time::sleep(Duration::from_millis(400)).await;

    sdk.gate.add_permits(8);
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if matches!(
                handle.status(id(4)).await,
                Ok(Some(InvocationStatus::Completed(_)))
            ) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!("a turn failed on a background path was not rescheduled — the invocation lies still in Running")
    })?;

    handle.shutdown().await?;
    Ok(())
}
