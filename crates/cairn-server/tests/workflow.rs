// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Workflows end to end over HTTP: `run` once per key, `SHARED` handlers poke
//! it through promises, and purging purges everything.
//!
//! The fake SDK here implements exactly the `REQUEST_RESPONSE` constraint, like
//! `end_to_end.rs`: every wait ends the turn with a `Suspension`.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    anyhow::Result,
    axum::{
        extract::Path as AxPath,
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
    std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration},
};

const PROMISE: &str = "approved";

// ───────────────────────────────────────────────────── fake SDK

/// One step of the fake program. `id` is the completion id it waits for.
#[derive(Clone, Debug)]
enum Step {
    SetState {
        key: &'static str,
        value: &'static str,
    },
    GetPromise {
        name: &'static str,
        id: u32,
    },
    CompletePromise {
        name: &'static str,
        value: &'static str,
        id: u32,
    },
    Output {
        value: &'static str,
    },
}

/// The program of each handler.
///
/// `run` writes state and then **waits** on a promise — that is, it hangs, and
/// that is what lets the test prove a `SHARED` handler runs *while* it hangs.
fn program(handler: &str) -> Vec<Step> {
    match handler {
        "run" => vec![
            Step::SetState {
                key: "stage",
                value: "waiting",
            },
            Step::GetPromise {
                name: PROMISE,
                id: 1,
            },
            Step::Output { value: "done" },
        ],
        "click" => vec![
            Step::CompletePromise {
                name: PROMISE,
                value: "yes",
                id: 1,
            },
            Step::Output { value: "clicked" },
        ],
        // A SHARED handler tries to write state — the runtime must block it.
        "hijack" => vec![
            Step::SetState {
                key: "stage",
                value: "hijacked",
            },
            Step::Output { value: "written" },
        ],
        other => panic!("the fake SDK has no handler {other:?}"),
    }
}

fn awaited(s: &Step) -> Option<u32> {
    match s {
        Step::GetPromise { id, .. } | Step::CompletePromise { id, .. } => Some(*id),
        _ => None,
    }
}

fn command_of(step: &Step) -> Message {
    match step {
        Step::SetState { key, value } => Message::SetState(pb::SetStateCommandMessage {
            key: Bytes::from_static(key.as_bytes()),
            value: Some(cairn_protocol::value::inline(Bytes::from_static(
                value.as_bytes(),
            ))),
            name: String::new(),
        }),
        Step::GetPromise { name, id } => Message::GetPromise(pb::GetPromiseCommandMessage {
            key: (*name).into(),
            result_completion_id: *id,
            name: String::new(),
        }),
        Step::CompletePromise { name, value, id } => {
            Message::CompletePromise(pb::CompletePromiseCommandMessage {
                key: (*name).into(),
                completion: Some(
                    pb::complete_promise_command_message::Completion::CompletionValue(
                        cairn_protocol::value::inline(Bytes::from_static(value.as_bytes())),
                    ),
                ),
                result_completion_id: *id,
                name: String::new(),
            })
        }
        Step::Output { value } => Message::Output(pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from_static(value.as_bytes())),
            )),
            name: String::new(),
        }),
    }
}

fn msg_completion_id(m: &Message) -> Option<u32> {
    Some(match m {
        Message::GetPromiseCompletion(n) => n.completion_id,
        Message::CompletePromiseCompletion(n) => n.completion_id,
        _ => return None,
    })
}

fn sdk_manifest() -> EndpointManifest {
    EndpointManifest {
        protocol_mode: ProtocolMode::RequestResponse,
        min_protocol_version: 1,
        max_protocol_version: 1,
        services: vec![ServiceManifest {
            name: "Signup".into(),
            ty: ServiceType::Workflow,
            handlers: vec![
                HandlerManifest {
                    name: "run".into(),
                    ty: Some(HandlerType::Workflow),
                    contract: Default::default(),
                },
                // No `ty` declared: a workflow defaults to SHARED.
                HandlerManifest {
                    name: "click".into(),
                    ty: None,
                    contract: Default::default(),
                },
                HandlerManifest {
                    name: "hijack".into(),
                    ty: Some(HandlerType::Shared),
                    contract: Default::default(),
                },
            ],
            contract: Default::default(),
        }],
    }
}

async fn sdk_discover() -> ([(header::HeaderName, &'static str); 1], String) {
    (
        [(header::CONTENT_TYPE, DISCOVERY_V1)],
        serde_json::to_string(&sdk_manifest()).unwrap(),
    )
}

async fn sdk_invoke(
    AxPath((_service, handler)): AxPath<(String, String)>,
    body: Bytes,
) -> (StatusCode, HeaderMap, Bytes) {
    let steps = program(&handler);
    let frames = decode_stream(body).expect("the runtime sends valid frames");

    let mut done: BTreeSet<u32> = BTreeSet::new();
    let mut journaled_commands = 0usize;
    for f in &frames {
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
    let mut suspended_on = None;
    for (i, step) in steps.iter().enumerate() {
        if i >= journaled_commands {
            out.push(command_of(step));
        }
        match awaited(step) {
            Some(id) if !done.contains(&id) => {
                suspended_on = Some(id);
                break;
            }
            _ => {}
        }
    }

    match suspended_on {
        Some(id) => out.push(Message::Suspension(pb::SuspensionMessage {
            waiting_completions: vec![id],
            ..Default::default()
        })),
        None => out.push(Message::End(pb::EndMessage {})),
    }

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, INVOCATION_V1.parse().unwrap());
    (StatusCode::OK, headers, encode_stream(&out))
}

async fn spawn_sdk() -> Result<String> {
    let app = Router::new()
        .route("/discover", get(sdk_discover))
        .route("/invoke/{service}/{handler}", post(sdk_invoke));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(format!("http://{addr}"))
}

// ───────────────────────────────────────────────────── runtime

/// A hand-built runtime: the ingress port (printed via `{rt}`) + the internal
/// port and the cluster secret, to send inter-range messages like another node.
struct Runtime {
    url: String,
    internal: String,
    secret: cairn_server::auth::ClusterSecret,
}

impl std::fmt::Display for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.url)
    }
}

impl Runtime {
    /// `POST /internal/inbound` signed with the cluster secret.
    async fn inbound(&self, http: &reqwest::Client, body: Bytes) -> Result<reqwest::Response> {
        let req = http
            .post(format!("{}/internal/inbound", self.internal))
            .body(body);
        Ok(self.secret.send(http, req).await?)
    }
}

async fn spawn_runtime(name: &str) -> Result<Runtime> {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;

    let registry = cairn_server::Registry::new();
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

    // `invoke_wait` as in the real binary (5s by default). Without it
    // `wait_for_settled` is a no-op, and `attach` — which means "wait until
    // this workflow has fully stopped" — returns at once whatever it sees on
    // the first tick.
    //
    // The test used to pass without it, but for a reason that no longer
    // holds: the drain loop ran the waiter's whole turn in place, so "done"
    // always arrived in time. That turn is now a separate task, and the test
    // must wait exactly as a real client waits.
    let secret = cairn_server::auth::ClusterSecret::random();
    let state = AppState::new(handle, registry)
        .with_invoke_wait(Some(Duration::from_secs(5)))
        .with_cluster_secret(secret.clone());
    let app = router(state.clone());
    let internal_app = cairn_server::ingress::internal_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let internal_addr = internal_listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    tokio::spawn(async move {
        let _ = axum::serve(internal_listener, internal_app).await;
    });
    Ok(Runtime {
        url: format!("http://{addr}"),
        internal: format!("http://{internal_addr}"),
        secret,
    })
}

async fn setup(name: &str) -> Result<(Runtime, reqwest::Client)> {
    let sdk = spawn_sdk().await?;
    let rt = spawn_runtime(name).await?;
    let http = reqwest::Client::new();
    let reg = http
        .post(format!("{rt}/deployments"))
        .json(&serde_json::json!({ "uri": sdk }))
        .send()
        .await?;
    assert_eq!(
        reg.status(),
        StatusCode::OK,
        "the workflow registration must be accepted"
    );
    Ok((rt, http))
}

// ───────────────────────────────────────────────────── test

/// The full lifecycle: `run` hangs on a promise, a `SHARED` handler completes
/// that promise **while `run` is still hanging**, and `run` continues to the end.
///
/// This is the property that sets a workflow apart from a virtual object: if
/// the queue were exclusive per key, `click` would have to queue behind `run`,
/// and `run` waits on that very `click` — a deadlock. It works *because* a
/// workflow does not lock per key.
#[tokio::test]
async fn shared_handler_unblocks_suspended_run() -> Result<()> {
    let (rt, http) = setup("wf-lifecycle").await?;

    let resp = http
        .post(format!("{rt}/invoke/Signup/run?key=u-1"))
        .body("{}")
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        StatusCode::ACCEPTED,
        "run waits on a promise ⇒ 202, not a result"
    );
    let run_id = resp.json::<serde_json::Value>().await?["id"]
        .as_str()
        .unwrap()
        .to_string();

    // `run` is not done ⇒ `/output` returns 202, **not** 404.
    let out = http
        .get(format!("{rt}/workflow/Signup/u-1/output"))
        .send()
        .await?;
    assert_eq!(out.status(), StatusCode::ACCEPTED);

    // The SHARED handler runs in parallel and pokes the promise.
    let click = http
        .post(format!("{rt}/invoke/Signup/click?key=u-1"))
        .body("{}")
        .send()
        .await?;
    assert_eq!(click.status(), StatusCode::OK, "click must run at once");

    // `attach` needs no id — it derives it from (service, key).
    let attached: serde_json::Value = http
        .post(format!("{rt}/workflow/Signup/u-1/attach"))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(attached["status"], "completed", "run finished: {attached}");
    assert_eq!(attached["output"], "done");
    assert_eq!(
        attached["id"], run_id,
        "the id derived from (service, key) must equal the id of the run call"
    );
    Ok(())
}

/// A second `run` call for the same key ⇒ `409`, **not** attached to the
/// running one.
///
/// Attaching is the wrong answer: the second call's input is dropped, so
/// returning the first run's result as if it were the second's is a lie.
#[tokio::test]
async fn second_run_rejected() -> Result<()> {
    let (rt, http) = setup("wf-duplicate").await?;

    let first = http
        .post(format!("{rt}/invoke/Signup/run?key=u-1"))
        .body("{}")
        .send()
        .await?;
    let first_id = first.json::<serde_json::Value>().await?["id"]
        .as_str()
        .unwrap()
        .to_string();

    let second = http
        .post(format!("{rt}/invoke/Signup/run?key=u-1"))
        .body("{}")
        .send()
        .await?;
    assert_eq!(second.status(), StatusCode::CONFLICT);
    let body: serde_json::Value = second.json().await?;
    assert_eq!(body["status"], "already_invoked");
    assert_eq!(
        body["id"], first_id,
        "the 409 must point to the run holding the key, so the client can attach"
    );

    // A **different** key runs normally: deduplication is per key, not per service.
    let other = http
        .post(format!("{rt}/invoke/Signup/run?key=u-2"))
        .body("{}")
        .send()
        .await?;
    assert_eq!(other.status(), StatusCode::ACCEPTED);
    Ok(())
}

/// Two parameters meaningless for `run` are **refused at the door**, not ignored.
#[tokio::test]
async fn run_rejects_missing_key_and_idempotency_key() -> Result<()> {
    let (rt, http) = setup("wf-badargs").await?;

    let no_key = http
        .post(format!("{rt}/invoke/Signup/run"))
        .body("{}")
        .send()
        .await?;
    assert_eq!(no_key.status(), StatusCode::BAD_REQUEST, "run, no key");

    let with_idem = http
        .post(format!("{rt}/invoke/Signup/run?key=u-1"))
        .header("idempotency-key", "k-1")
        .body("{}")
        .send()
        .await?;
    assert_eq!(
        with_idem.status(),
        StatusCode::BAD_REQUEST,
        "Idempotency-Key with run must be refused, not silently ignored"
    );

    // A SHARED handler still takes an idempotency key like any other service.
    let shared = http
        .post(format!("{rt}/invoke/Signup/click?key=u-1"))
        .header("idempotency-key", "k-1")
        .body("{}")
        .send()
        .await?;
    assert_eq!(shared.status(), StatusCode::OK);
    Ok(())
}

/// `attach`/`output` for a workflow nobody called ⇒ `404`, not `202`.
///
/// The two tell the client different things: `202` is "ask again", `404` is
/// "nobody started it, call `run`".
#[tokio::test]
async fn attach_to_workflow_not_yet_run() -> Result<()> {
    let (rt, http) = setup("wf-notfound").await?;
    let resp = http
        .post(format!("{rt}/workflow/Signup/missing/attach"))
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    Ok(())
}

/// A `SHARED` handler writing state ⇒ the invocation dies with
/// PROTOCOL_VIOLATION, and the state does **not** change.
///
/// It runs in parallel with `run`; letting it write would open exactly the
/// lost-update door that a virtual object closes with its `EXCLUSIVE` lock.
#[tokio::test]
async fn shared_handler_cannot_write_state() -> Result<()> {
    let (rt, http) = setup("wf-readonly").await?;

    http.post(format!("{rt}/invoke/Signup/run?key=u-1"))
        .body("{}")
        .send()
        .await?;

    let resp = http
        .post(format!("{rt}/invoke/Signup/hijack?key=u-1"))
        .body("{}")
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "a SHARED handler writing state must fail"
    );
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["status"], "failed");
    assert_eq!(
        body["code"],
        cairn_protocol::PROTOCOL_VIOLATION,
        "must be a protocol violation, not a business error: {body}"
    );
    Ok(())
}

/// A `StartCall` message from another partition: the duplicate check runs on
/// the **receiving side**.
///
/// The sender can only compute the id (it is deterministic); the workflow's
/// status is state of the destination partition, so only the destination
/// knows whether it already exists. This test goes straight to the
/// `/internal/inbound` door instead of building two real partitions — a
/// multi-partition cluster needs a store that can CAS (MinIO), while the code
/// branch to check sits entirely here.
#[tokio::test]
async fn cross_partition_start_call_also_blocks_duplicates() -> Result<()> {
    use cairn_engine::{InterPartition, StartCall};

    let (rt, http) = setup("wf-inbound").await?;
    let id = cairn_store::workflow_invocation_id(
        cairn_store::partition_key_for_target(
            &cairn_store::TenantId::default(),
            None,
            "Signup",
            "u-1",
            None,
        )
        .expect("has a key"),
        &cairn_store::TenantId::default(),
        None,
        "Signup",
        "u-1",
    );
    let caller = [9u8; 24];

    let start = |completion: u32| {
        InterPartition::StartCall(StartCall {
            callee: id,
            service: "Signup".into(),
            handler: "run".into(),
            object_key: "u-1".into(),
            parameter: Bytes::from_static(b"{}"),
            reply_to: Some((caller, completion)),
            idempotency_key: None,
            scope: None,
            limit_key: None,
            tenant: None,
            sender: (caller, completion),
            schedule: None,
            lineage: Default::default(),
        })
        .encode()
    };

    // First time: create.
    let r = rt.inbound(&http, start(1)).await?;
    assert_eq!(r.status(), StatusCode::OK, "message accepted and durable");

    // The drainer processes the message when it arrives and hands the first
    // turn to the scheduler — asynchronously to this request, so poll until
    // the run has hung.
    let mut status = serde_json::Value::Null;
    for _ in 0..250 {
        status = http
            .get(format!("{rt}/invocations/{}", hex(&id)))
            .send()
            .await?
            .json()
            .await?;
        if status["status"] == "suspended" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        status["status"], "suspended",
        "the run must exist at the right deterministic id: {status}"
    );

    // Second time, a **different** caller: nothing new is created, and the
    // running run is not overwritten. Its journal must keep its length.
    let before: Vec<serde_json::Value> = http
        .get(format!("{rt}/invocations/{}/journal", hex(&id)))
        .send()
        .await?
        .json()
        .await?;
    rt.inbound(&http, start(2)).await?;
    // Give the drainer time to process the second message (it arrives at once, but asynchronously).
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let after: Vec<serde_json::Value> = http
        .get(format!("{rt}/invocations/{}/journal", hex(&id)))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(
        before.len(),
        after.len(),
        "a duplicate StartCall message recreated the workflow — the journal was reset"
    );
    Ok(())
}

fn hex(id: &[u8; 24]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

/// Purging a workflow purges **both its state and its promises** — and the same
/// key can then run again from the start.
///
/// This is a necessary consequence of deterministic ids: after retention a
/// workflow key can be reused, and a new run that sees the old run's state is
/// a workflow starting in the middle.
#[tokio::test]
async fn purge_cleans_both_state_and_promises() -> Result<()> {
    let (rt, http) = setup("wf-purge").await?;

    let run = http
        .post(format!("{rt}/invoke/Signup/run?key=u-1"))
        .body("{}")
        .send()
        .await?;
    let id = run.json::<serde_json::Value>().await?["id"]
        .as_str()
        .unwrap()
        .to_string();
    http.post(format!("{rt}/invoke/Signup/click?key=u-1"))
        .body("{}")
        .send()
        .await?;

    // The promise has a result, readable from outside.
    let promise: serde_json::Value = http
        .get(format!("{rt}/promises/Signup/u-1/{PROMISE}"))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(
        promise["completed"], true,
        "the promise must have a result before the purge"
    );
    assert_eq!(promise["value"], "yes");

    let purge = http
        .post(format!("{rt}/invocations/{id}/purge"))
        .send()
        .await?;
    assert_eq!(purge.status(), StatusCode::OK, "purge a done workflow");

    let promise: serde_json::Value = http
        .get(format!("{rt}/promises/Signup/u-1/{PROMISE}"))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(
        promise["completed"], false,
        "the workflow's promises must go with it on purge: {promise}"
    );

    // And the key runs again: a new `run` hangs on a new promise, instead of
    // seeing the old result and running straight to the end.
    let again = http
        .post(format!("{rt}/invoke/Signup/run?key=u-1"))
        .body("{}")
        .send()
        .await?;
    assert_eq!(
        again.status(),
        StatusCode::ACCEPTED,
        "after the purge the same key must run again from the start"
    );
    Ok(())
}

/// `GET /workflow/{service}/{key}/id`: the console finds a workflow by key
/// without copying the tenant-aware hash function.
///
/// Must see: the id computed **before** `run` equals the id `run` returns (the
/// same function as `attach`); the id changes with the tenant (cluster-admin's
/// `?tenant=`) and with the scope; an empty key yields no id.
#[tokio::test]
async fn workflow_id_computed_before_running() -> Result<()> {
    let (rt, http) = setup("wf-id").await?;
    let id_of = |path: String| {
        let http = http.clone();
        async move {
            let r = http.get(path).send().await?;
            anyhow::ensure!(r.status() == StatusCode::OK, "{}", r.status());
            let v: serde_json::Value = r.json().await?;
            Ok::<_, anyhow::Error>(v["id"].as_str().unwrap_or_default().to_string())
        }
    };
    let before = id_of(format!("{rt}/workflow/Signup/u-9/id")).await?;
    assert_eq!(before.len(), 48, "{before}");

    let resp = http
        .post(format!("{rt}/invoke/Signup/run?key=u-9"))
        .body("{}")
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let run_id = resp.json::<serde_json::Value>().await?["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(before, run_id, "precomputed id must be the run id");

    let other_tenant = id_of(format!("{rt}/workflow/Signup/u-9/id?tenant=acme")).await?;
    assert_ne!(other_tenant, run_id, "the id must change with the tenant");
    let scoped = id_of(format!("{rt}/scope/team-a/workflow/Signup/u-9/id")).await?;
    assert_ne!(scoped, run_id, "the id must change with the scope");

    let empty = http.get(format!("{rt}/workflow/Signup//id")).send().await?;
    assert_ne!(empty.status(), StatusCode::OK, "an empty key has no id");
    Ok(())
}
