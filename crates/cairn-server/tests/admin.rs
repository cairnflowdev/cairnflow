// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Admin API: cancel, invocation listing, /services, /metrics.
//!
//! The hardest case is cancel × EXCLUSIVE: cancelling an invocation that **holds
//! the lock** must hand the lock to whoever is queued behind it — otherwise one
//! cancel turns the whole object key into a dead queue. And the timer of a
//! cancelled invocation **must not fire**: delivering a completion to a terminal
//! invocation means running it again.

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
    cairn_protocol::{pb, Message},
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
    std::{path::PathBuf, sync::Arc, time::Duration},
};

const WAKE_AT: u64 = 1_700_000_000_000;

/// Wait until a condition holds, for at most 5 seconds.
///
/// **Not there to hide a bug.** Since the scheduler exists, handing the lock to
/// the next in line is a scheduling tick, not a function call inside the cancel
/// — so "right after the cancel returns" is no longer the right moment to
/// assert. Asserting immediately here would assert an implementation detail
/// that is gone, not a promise of the system.
async fn eventually<F, Fut>(what: &str, mut f: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if f().await? {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            anyhow::bail!("waited 5 seconds in vain: {what}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ─────────────────────────────── Fake SDK: ctx.sleep(WAKE_AT) then Output

#[derive(Clone)]
struct SdkState {
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

async fn sdk_discover() -> ([(header::HeaderName, &'static str); 1], String) {
    let manifest = EndpointManifest {
        protocol_mode: ProtocolMode::RequestResponse,
        min_protocol_version: 1,
        max_protocol_version: 1,
        services: vec![ServiceManifest {
            name: "TicketOffice".into(),
            ty: ServiceType::VirtualObject,
            handlers: vec![HandlerManifest {
                name: "hold_seat".into(),
                ty: Some(HandlerType::Exclusive),
                contract: Default::default(),
            }],
            contract: Default::default(),
        }],
    };
    (
        [(header::CONTENT_TYPE, DISCOVERY_V1)],
        serde_json::to_string(&manifest).unwrap(),
    )
}

async fn sdk_invoke(State(st): State<SdkState>, body: Bytes) -> (StatusCode, HeaderMap, Bytes) {
    st.calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let frames = decode_stream(body).expect("valid frames");
    let mut slept = false;
    let mut journaled_sleep = false;
    for f in &frames {
        match cairn_protocol::decode(f).expect("decodable") {
            Message::Sleep(_) => journaled_sleep = true,
            Message::SleepCompletion(_) => slept = true,
            _ => {}
        }
    }
    let mut out = Vec::new();
    if slept {
        out.push(Message::Output(pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from_static(b"held for you")),
            )),
            name: String::new(),
        }));
        out.push(Message::End(pb::EndMessage {}));
    } else {
        if !journaled_sleep {
            out.push(Message::Sleep(pb::SleepCommandMessage {
                wake_up_time: WAKE_AT,
                result_completion_id: 1,
                name: String::new(),
            }));
        }
        out.push(Message::Suspension(pb::SuspensionMessage {
            waiting_completions: vec![1],
            ..Default::default()
        }));
    }
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, INVOCATION_V1.parse().unwrap());
    (StatusCode::OK, headers, encode_stream(&out))
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

async fn spawn_runtime(name: &str) -> Result<String> {
    spawn_runtime_with_retention(name, None).await
}

async fn spawn_runtime_with_retention(name: &str, retention_ms: Option<u64>) -> Result<String> {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;

    let registry = cairn_server::Registry::new();
    // No background loops: the test drives time through /admin/tick.
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
                    engine: Engine::open(writer).await?.with_retention(retention_ms),
                    keeper: None,
                })
            })
        }),
        registry.clone(),
        None,
        Readiness::WhenLeader,
    )?;

    let app = router(AppState::new(handle, registry));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(format!("http://{addr}"))
}

/// The whole admin path in one scenario: two invocations on the same object key
/// (EXCLUSIVE), cancel the one holding the lock, the queued one must go on and finish.
#[tokio::test]
async fn cancel_and_introspection() -> Result<()> {
    let sdk_state = SdkState {
        calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };
    let sdk = spawn_sdk(sdk_state.clone()).await?;
    let rt = spawn_runtime("admin-cancel").await?;
    let http = reqwest::Client::new();

    let resp = http
        .post(format!("{rt}/deployments"))
        .json(&serde_json::json!({ "uri": sdk }))
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::OK);

    // ── /services: the caller's view, with names + handler kinds.
    let services: serde_json::Value = http
        .get(format!("{rt}/services"))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(services[0]["name"], "TicketOffice");
    assert_eq!(services[0]["ty"], "virtualObject");
    assert_eq!(services[0]["handlers"][0]["name"], "hold_seat");
    assert_eq!(services[0]["handlers"][0]["ty"], "exclusive");

    // ── A holds the lock and hangs on a sleep; B queues behind it on the same object key.
    let a: serde_json::Value = http
        .post(format!("{rt}/invoke/TicketOffice/hold_seat?key=seat-7"))
        .body("A")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(a["status"], "waiting");
    let a_id = a["id"].as_str().unwrap().to_string();
    let b: serde_json::Value = http
        .post(format!("{rt}/invoke/TicketOffice/hold_seat?key=seat-7"))
        .body("B")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(b["status"], "waiting");
    let b_id = b["id"].as_str().unwrap().to_string();
    // B was never called down to the SDK — it is queued, that is what EXCLUSIVE means.
    assert_eq!(
        sdk_state.calls.load(std::sync::atomic::Ordering::Relaxed),
        1
    );

    // ── Human-readable journal: message type names, not just hex codes.
    let journal: serde_json::Value = http
        .get(format!("{rt}/invocations/{a_id}/journal"))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(journal[0]["name"], "Input");
    assert_eq!(journal[1]["name"], "Sleep");

    // ── Listing: both present, with the routing target read from entry 0.
    let list: serde_json::Value = http
        .get(format!("{rt}/invocations"))
        .send()
        .await?
        .json()
        .await?;
    let rows = list.as_array().unwrap();
    assert_eq!(rows.len(), 2, "{list}");
    for r in rows {
        assert_eq!(r["service"], "TicketOffice");
        assert_eq!(r["key"], "seat-7");
        assert_eq!(r["partition"], 0);
    }

    // ── Cancel A. B must get the lock and run to its own suspension point.
    let cancel: serde_json::Value = http
        .post(format!("{rt}/invocations/{a_id}/cancel"))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(cancel["cancelled"], true);
    assert_eq!(cancel["status"], "failed");
    // B must get the lock — but **no longer synchronously with the cancel**.
    //
    // Before flow control, the cancelled one itself drove the next in the queue,
    // so by the time the cancel returned B had already run. Now the scheduler is
    // what decides who runs next: the cancel releases the lock, and B's task —
    // waiting for its own slot — is woken. That changes the *mechanism*, not the
    // *promise*: the queue still moves on, only through a scheduling tick rather
    // than through the hands of the one that just died. Waiting here is waiting
    // for exactly what the new semantics says.
    eventually("B driven after A releases the lock", || async {
        Ok(sdk_state.calls.load(std::sync::atomic::Ordering::Relaxed) == 2)
    })
    .await?;
    // And it must run to **its own suspension point** before we advance time —
    // otherwise the `advanced` count below measures something else.
    eventually("B suspended on its own sleep", || async {
        let s: serde_json::Value = http
            .get(format!("{rt}/invocations/{b_id}"))
            .send()
            .await?
            .json()
            .await?;
        Ok(s["status"] == "suspended")
    })
    .await?;

    // A second cancel is a no-op, and says why.
    let again: serde_json::Value = http
        .post(format!("{rt}/invocations/{a_id}/cancel"))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(again["cancelled"], false);
    assert_eq!(again["status"], "failed");

    // Cancelling an id that does not exist → 404, not a pretend 200.
    let missing = http
        .post(format!("{rt}/invocations/{}/cancel", "00".repeat(24)))
        .send()
        .await?;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);

    // ── Tick past the wake time: A's timer (cancelled) must be CLEARED, not fire;
    // B's timer fires and B runs to completion.
    let tick: serde_json::Value = http
        .post(format!("{rt}/admin/tick?now_ms={}", WAKE_AT + 1))
        .send()
        .await?
        .json()
        .await?;
    // Only B advances. If A "advanced" too, the timer of a dead invocation still fires.
    assert_eq!(tick["advanced"], 1, "{tick}");

    let a_status: serde_json::Value = http
        .get(format!("{rt}/invocations/{a_id}"))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(a_status["status"], "failed", "A must stay failed");
    let b_status: serde_json::Value = http
        .get(format!("{rt}/invocations/{b_id}"))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(b_status["status"], "completed", "{b_status}");
    assert_eq!(b_status["output"], "held for you");

    // ── Filter by status.
    let failed: serde_json::Value = http
        .get(format!("{rt}/invocations?status=failed"))
        .send()
        .await?
        .json()
        .await?;
    let rows = failed.as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], a_id.as_str());

    // ── /metrics: Prometheus text, counting what just happened.
    let metrics = http
        .get(format!("{rt}/metrics"))
        .send()
        .await?
        .text()
        .await?;
    assert!(
        metrics.contains("cairnflow_invoke_requests_total 2"),
        "{metrics}"
    );
    assert!(metrics.contains("cairnflow_cancelled_total 1"), "{metrics}");
    assert!(
        metrics.contains("cairnflow_admin_ticks_total 1"),
        "{metrics}"
    );
    // The public router's metrics layer sees the route pattern (ack latency per
    // route) and counts responses per status class. Counters are per process
    // (other tests in the same binary add to them) ⇒ compare with ≥.
    let sample = |name: &str| -> u64 {
        metrics
            .lines()
            .find_map(|l| l.strip_prefix(name)?.trim().parse().ok())
            .unwrap_or_else(|| panic!("no {name}: {metrics}"))
    };
    assert!(sample("cairnflow_ack_seconds_count{route=\"invoke\"}") >= 2);
    assert!(sample("cairnflow_http_responses_total{class=\"2xx\"}") >= 4);
    assert!(metrics.contains("cairnflow_store_requests_total{op=\"put\"}"));
    Ok(())
}

/// Retention + purge: finished invocations are purged — by hand through the admin
/// API, and automatically once `--retention-ms` expires. After a purge the
/// idempotency key is free again: a call with the same key is a NEW invocation,
/// not the old result — the dedupe window is exactly the retention.
#[tokio::test]
async fn purge_admin_and_retention() -> Result<()> {
    let sdk_state = SdkState {
        calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };
    let sdk = spawn_sdk(sdk_state.clone()).await?;
    // Retention 50ms — short enough for a "future" tick to purge right inside the test.
    let rt = spawn_runtime_with_retention("admin-purge", Some(50)).await?;
    let http = reqwest::Client::new();
    http.post(format!("{rt}/deployments"))
        .json(&serde_json::json!({ "uri": sdk }))
        .send()
        .await?
        .error_for_status()?;

    // A hangs on a sleep. Purging a RUNNING invocation must be refused with 409 —
    // purging it would lose the suspended work.
    let a: serde_json::Value = http
        .post(format!("{rt}/invoke/TicketOffice/hold_seat?key=seat-1"))
        .header("Idempotency-Key", "ve-so-1")
        .body("A")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(a["status"], "waiting", "{a}");
    let a_id = a["id"].as_str().unwrap().to_string();
    let early = http
        .post(format!("{rt}/invocations/{a_id}/purge"))
        .send()
        .await?;
    assert_eq!(
        early.status(),
        StatusCode::CONFLICT,
        "a running invocation must not be purgeable (409)"
    );

    // Run to completion. A tick only fires the timer and then **hands** the turn
    // to the scheduler (D18) — the turn runs after the tick has answered, so wait
    // (bounded) rather than read once: under load, reading right after the tick sees `suspended`.
    http.post(format!("{rt}/admin/tick?now_ms={}", WAKE_AT + 1))
        .send()
        .await?
        .error_for_status()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let st = loop {
        let st: serde_json::Value = http
            .get(format!("{rt}/invocations/{a_id}"))
            .send()
            .await?
            .json()
            .await?;
        if st["status"] == "completed" || std::time::Instant::now() > deadline {
            break st;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(st["status"], "completed", "{st}");

    // Purge by hand: 200, then every trace is gone.
    let purged: serde_json::Value = http
        .post(format!("{rt}/invocations/{a_id}/purge"))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(purged["purged"], true, "{purged}");
    let gone = http.get(format!("{rt}/invocations/{a_id}")).send().await?;
    assert_eq!(
        gone.status(),
        StatusCode::NOT_FOUND,
        "status still there after the purge"
    );

    // The idempotency key is free again: same key ⇒ a NEW invocation (it hangs on
    // the sleep again instead of returning the old result). The id is STILL that
    // id — an id derived from the key set is deterministic, exactly as contracted.
    let reissued: serde_json::Value = http
        .post(format!("{rt}/invoke/TicketOffice/hold_seat?key=seat-1"))
        .header("Idempotency-Key", "ve-so-1")
        .body("A2")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(
        reissued["status"], "waiting",
        "must be a new invocation: {reissued}"
    );
    assert_eq!(
        reissued["id"].as_str().unwrap(),
        a_id,
        "the derived id must be stable"
    );

    // The automatic path: run to completion, then tick past the retention deadline.
    http.post(format!("{rt}/admin/tick?now_ms={}", WAKE_AT + 2))
        .send()
        .await?
        .error_for_status()?;
    let real_now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;
    // The purge deadline is recorded on the engine's real clock (+50ms), so the
    // tick must pass the real clock, not WAKE_AT.
    tokio::time::sleep(Duration::from_millis(60)).await;
    http.post(format!("{rt}/admin/tick?now_ms={}", real_now + 60_000))
        .send()
        .await?
        .error_for_status()?;
    let auto_gone = http.get(format!("{rt}/invocations/{a_id}")).send().await?;
    assert_eq!(
        auto_gone.status(),
        StatusCode::NOT_FOUND,
        "retention must purge a finished invocation on its own"
    );
    Ok(())
}
