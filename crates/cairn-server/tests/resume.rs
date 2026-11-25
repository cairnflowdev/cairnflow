// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `POST /invocations/{id}/resume` — takes an invocation out of `paused`.
//!
//! `FailureAction::Pause` presents itself as "waiting for manual intervention", but
//! before this endpoint **there was no way to intervene**: once `paused`, all that was
//! left was `cancel` or `purge` — the promise "keep it to inspect, then continue" kept
//! only its first half.
//!
//! The fake SDK here pauses on a switch: the simplest way to build this state that
//! can be released at once. The real case — `JOURNAL_MISMATCH` ⇒ `paused`, deploy the fix
//! then `resume` — is checked with the real SDK in `sdk/typescript/test/durability.test.ts`.

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
            EndpointManifest, HandlerManifest, ProtocolMode, ServiceManifest, ServiceType,
            DISCOVERY_V1,
        },
        ingress::AppState,
        invoker::{decode_stream, encode_stream, INVOCATION_V1},
        router, Registry,
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
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    },
};

#[derive(Clone)]
struct PausingSdk {
    /// While on, every turn ends with `Error(behavior = PAUSE)`.
    pause: Arc<AtomicBool>,
}

async fn sdk_discover() -> ([(header::HeaderName, &'static str); 1], String) {
    let manifest = EndpointManifest {
        protocol_mode: ProtocolMode::RequestResponse,
        min_protocol_version: 1,
        max_protocol_version: 1,
        services: vec![ServiceManifest {
            name: "Warehouse".into(),
            ty: ServiceType::Service,
            handlers: vec![HandlerManifest {
                name: "ship".into(),
                ty: None,
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

async fn sdk_invoke(State(st): State<PausingSdk>, body: Bytes) -> (StatusCode, HeaderMap, Bytes) {
    decode_stream(body).expect("valid frames");
    let out = if st.pause.load(Ordering::Relaxed) {
        vec![Message::Error(pb::ErrorMessage {
            code: 500,
            message: "out of stock — someone needs to restock".into(),
            behavior: pb::ErrorBehavior::Pause as i32,
            ..Default::default()
        })]
    } else {
        vec![
            Message::Output(pb::OutputCommandMessage {
                result: Some(pb::output_command_message::Result::Value(
                    cairn_protocol::value::inline(Bytes::from_static(b"shipped")),
                )),
                name: String::new(),
            }),
            Message::End(pb::EndMessage {}),
        ]
    };
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, INVOCATION_V1.parse().unwrap());
    (StatusCode::OK, headers, encode_stream(&out))
}

async fn spawn_sdk(st: PausingSdk) -> Result<String> {
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

async fn spawn_runtime(name: &str) -> Result<String> {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;

    let registry = Registry::new();
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

    let app = router(AppState::new(handle, registry));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(format!("http://{addr}"))
}

async fn body(r: reqwest::Response) -> (StatusCode, serde_json::Value) {
    let st = r.status();
    let text = r.text().await.unwrap_or_default();
    (
        st,
        serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text)),
    )
}

/// The full lifecycle: paused → resumed → continues to completion.
#[tokio::test]
async fn paused_can_be_lifted_and_continues() -> Result<()> {
    let sdk = PausingSdk {
        pause: Arc::new(AtomicBool::new(true)),
    };
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let rt = spawn_runtime("resume-e2e").await?;
    let http = reqwest::Client::new();

    http.post(format!("{rt}/deployments"))
        .json(&serde_json::json!({ "uri": endpoint }))
        .send()
        .await?;

    let (st, inv) = body(
        http.post(format!("{rt}/invoke/Warehouse/ship"))
            .body("{}")
            .send()
            .await?,
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT, "SDK asks for PAUSE ⇒ 409: {inv}");
    let id = inv["id"].as_str().expect("an id").to_string();

    // Resuming while the outside world is **not** fixed yet: it pauses again. That is the
    // honest answer, not an "ok" that leaves the operator guessing.
    let (st, r) = body(
        http.post(format!("{rt}/invocations/{id}/resume"))
            .send()
            .await?,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{r}");
    assert_eq!(r["resumed"], true, "{r}");
    assert_eq!(r["status"], "paused", "unfixed: must pause again: {r}");

    // The operator fixes it (restocks), then resumes again.
    sdk.pause.store(false, Ordering::Relaxed);
    let (st, r) = body(
        http.post(format!("{rt}/invocations/{id}/resume"))
            .send()
            .await?,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{r}");
    assert_eq!(r["status"], "completed", "resumed: must complete: {r}");

    let (_, s) = body(http.get(format!("{rt}/invocations/{id}")).send().await?).await;
    assert_eq!(s["status"], "completed", "{s}");
    assert_eq!(s["output"], "shipped", "{s}");
    Ok(())
}

/// Resuming an invocation that is **not** paused is a 409, not a silent no-op.
///
/// An operator who typed the wrong id needs to know; an "ok" here makes them believe
/// they resolved an incident that is still there.
#[tokio::test]
async fn resuming_non_suspended_is_409() -> Result<()> {
    let sdk = PausingSdk {
        pause: Arc::new(AtomicBool::new(false)),
    };
    let endpoint = spawn_sdk(sdk).await?;
    let rt = spawn_runtime("resume-409").await?;
    let http = reqwest::Client::new();

    http.post(format!("{rt}/deployments"))
        .json(&serde_json::json!({ "uri": endpoint }))
        .send()
        .await?;

    let (st, inv) = body(
        http.post(format!("{rt}/invoke/Warehouse/ship"))
            .body("{}")
            .send()
            .await?,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{inv}");
    let id = inv["id"].as_str().expect("an id").to_string();

    let (st, r) = body(
        http.post(format!("{rt}/invocations/{id}/resume"))
            .send()
            .await?,
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT, "{r}");

    // An id that does not exist ⇒ 404, clearly different from 409.
    let (st, _) = body(
        http.post(format!("{rt}/invocations/{}/resume", "0".repeat(48)))
            .send()
            .await?,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    Ok(())
}
