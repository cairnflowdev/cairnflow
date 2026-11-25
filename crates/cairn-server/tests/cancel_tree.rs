// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Cancellation spreads down the child tree.
//!
//! Cancelling an invocation while leaving alone what it has called abandons work
//! that is still running — and in the workloads cairn targets (agent pipelines
//! making model calls), running work costs money. Nobody reads their results any
//! more, so all they do is spend.
//!
//! The boundary is pinned here, and it is a choice, not a consequence:
//! **a `ctx.call` is cancelled along with its caller, a `ctx.send` is not.** A
//! `OneWayCall` declares no result completion, so the caller has never waited for
//! it — it is detached work, not a branch of the running tree.

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
            EndpointManifest, HandlerManifest, ProtocolMode, ServiceManifest, ServiceType,
            DISCOVERY_V1,
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

/// So far away that no tick in the test reaches it: the child must still be suspended
/// when the cancel arrives.
const WAKE_AT: u64 = 4_000_000_000_000;

async fn eventually<F, Fut>(what: &str, mut f: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    for _ in 0..200 {
        if f().await? {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    anyhow::bail!("timed out waiting for: {what}")
}

// ───────────────────────────────────────────────────── Fake SDK

async fn sdk_discover() -> ([(header::HeaderName, &'static str); 1], String) {
    let manifest = EndpointManifest {
        protocol_mode: ProtocolMode::RequestResponse,
        min_protocol_version: 1,
        max_protocol_version: 1,
        services: vec![
            ServiceManifest {
                name: "Parent".into(),
                ty: ServiceType::Service,
                handlers: vec![
                    HandlerManifest {
                        name: "run".into(),
                        ty: None,
                        contract: Default::default(),
                    },
                    HandlerManifest {
                        name: "fanout".into(),
                        ty: None,
                        contract: Default::default(),
                    },
                    HandlerManifest {
                        name: "late".into(),
                        ty: None,
                        contract: Default::default(),
                    },
                ],
                contract: Default::default(),
            },
            ServiceManifest {
                name: "Fire".into(),
                ty: ServiceType::Service,
                handlers: vec![HandlerManifest {
                    name: "fire".into(),
                    ty: None,
                    contract: Default::default(),
                }],
                contract: Default::default(),
            },
            ServiceManifest {
                name: "Probe".into(),
                ty: ServiceType::Service,
                handlers: vec![HandlerManifest {
                    name: "peek".into(),
                    ty: None,
                    contract: Default::default(),
                }],
                contract: Default::default(),
            },
            ServiceManifest {
                name: "Child".into(),
                ty: ServiceType::Service,
                handlers: vec![
                    HandlerManifest {
                        name: "call".into(),
                        ty: None,
                        contract: Default::default(),
                    },
                    HandlerManifest {
                        name: "send".into(),
                        ty: None,
                        contract: Default::default(),
                    },
                    HandlerManifest {
                        name: "slow".into(),
                        ty: None,
                        contract: Default::default(),
                    },
                ],
                contract: Default::default(),
            },
        ],
    };
    (
        [(header::CONTENT_TYPE, DISCOVERY_V1)],
        serde_json::to_string(&manifest).unwrap(),
    )
}

/// `Parent/run`: calls `Child/call` (awaiting the result) **and** fires `Child/send`
/// (not awaited), then suspends. `Parent/fanout`: calls `Child/call` then `Child/slow` —
/// two awaited calls, so that at cancel time the second one is still in the outbox.
/// `Child/*`: sleep, then suspend.
async fn sdk_invoke(
    AxPath((service, handler)): AxPath<(String, String)>,
    body: Bytes,
) -> (StatusCode, HeaderMap, Bytes) {
    // `Child/call` answers slowly to hold the outbox drain back — so that the row of
    // `Child/slow` has certainly **not been dispatched** when the cancel arrives.
    if service == "Child" && handler == "call" {
        tokio::time::sleep(Duration::from_millis(1_200)).await;
    }
    let frames = decode_stream(body).expect("valid frames");
    let mut journaled = false;
    let mut got_result = false;
    for f in &frames {
        match cairn_protocol::decode(f).expect("decodable") {
            Message::Call(_) | Message::Sleep(_) | Message::OneWayCall(_) => journaled = true,
            Message::CallCompletion(_) => got_result = true,
            _ => {}
        }
    }

    let mut out = Vec::new();
    if service == "Fire" && handler == "fire" {
        // Fire one-way and finish at once — awaiting nothing.
        if !journaled {
            out.push(Message::OneWayCall(pb::OneWayCallCommandMessage {
                service_name: "Probe".into(),
                handler_name: "peek".into(),
                invocation_id_notification_idx: 1,
                ..Default::default()
            }));
        }
        out.push(Message::Output(pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from_static(b"fired")),
            )),
            name: String::new(),
        }));
        out.push(Message::End(pb::EndMessage {}));
    } else if service == "Probe" {
        // The fastest possible handler: no commands, returns its result at once. It is
        // the yardstick for the partition's front door.
        out.push(Message::Output(pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from_static(b"ok")),
            )),
            name: String::new(),
        }));
        out.push(Message::End(pb::EndMessage {}));
    } else if service == "Parent" && (handler == "fanout" || handler == "late") {
        if !journaled {
            out.push(Message::Call(pb::CallCommandMessage {
                service_name: "Child".into(),
                handler_name: "call".into(),
                invocation_id_notification_idx: 1,
                result_completion_id: 2,
                ..Default::default()
            }));
            out.push(Message::Call(pb::CallCommandMessage {
                // `Parent/late` calls a service that this node **has no deployment
                // for yet**, so its row stays in the outbox instead of being
                // dispatched at once. That is the window the cancel-while-in-the-outbox
                // test needs, and it races nobody: "not in the registry yet" is a
                // state, not a moment.
                service_name: if handler == "late" { "Absent" } else { "Child" }.into(),
                handler_name: "slow".into(),
                invocation_id_notification_idx: 3,
                result_completion_id: 4,
                ..Default::default()
            }));
        }
        out.push(Message::Suspension(pb::SuspensionMessage {
            waiting_completions: vec![2, 4],
            ..Default::default()
        }));
    } else if service == "Parent" && handler == "run" {
        if got_result {
            out.push(Message::Output(pb::OutputCommandMessage {
                result: Some(pb::output_command_message::Result::Value(
                    cairn_protocol::value::inline(Bytes::from_static(b"done")),
                )),
                name: String::new(),
            }));
            out.push(Message::End(pb::EndMessage {}));
        } else {
            if !journaled {
                out.push(Message::Call(pb::CallCommandMessage {
                    service_name: "Child".into(),
                    handler_name: "call".into(),
                    invocation_id_notification_idx: 1,
                    result_completion_id: 2,
                    ..Default::default()
                }));
                out.push(Message::OneWayCall(pb::OneWayCallCommandMessage {
                    service_name: "Child".into(),
                    handler_name: "send".into(),
                    invocation_id_notification_idx: 3,
                    ..Default::default()
                }));
            }
            out.push(Message::Suspension(pb::SuspensionMessage {
                waiting_completions: vec![2],
                ..Default::default()
            }));
        }
    } else {
        // Both of `Child`'s handlers sleep for a very long time — they must still be
        // alive when the cancel arrives.
        if !journaled {
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

async fn spawn_runtime(name: &str) -> Result<String> {
    spawn_runtime_ticking(name, None).await
}

/// `tick`: turn the background loops on, exactly as the real binary runs. It is the
/// partition's **second drain pass**, and the only thing that creates contention on
/// the drain path.
async fn spawn_runtime_ticking(name: &str, tick: Option<Duration>) -> Result<String> {
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
        tick,
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

/// The status of the first invocation matching `(service, handler)`, if any.
/// Is there an invocation of `service/handler` yet? The general form of [`child_of`].
async fn invocation_of(rt: &str, service: &str, handler: &str) -> Result<Option<String>> {
    let http = reqwest::Client::new();
    let list: serde_json::Value = http
        .get(format!("{rt}/invocations?limit=100"))
        .send()
        .await?
        .json()
        .await?;
    Ok(list.as_array().and_then(|rows| {
        rows.iter()
            .find(|i| i["service"] == service && i["handler"] == handler)
            .map(|i| i["status"].as_str().unwrap_or_default().to_string())
    }))
}

/// A second endpoint, with only `Absent` — **registered late** on purpose.
///
/// It is how the test builds an outbox row that *stays put*: `dispatch_call` cannot
/// decide while the target is not yet in the deployment registry, so the row stays
/// where it is. Quite different from waiting for a slow handler — "not in the
/// registry yet" is a state that holds until we change it, not a time window to catch.
async fn spawn_absent_sdk() -> Result<String> {
    async fn discover() -> ([(header::HeaderName, &'static str); 1], String) {
        let manifest = EndpointManifest {
            protocol_mode: ProtocolMode::RequestResponse,
            min_protocol_version: 1,
            max_protocol_version: 1,
            services: vec![ServiceManifest {
                name: "Absent".into(),
                ty: ServiceType::Service,
                handlers: vec![HandlerManifest {
                    name: "slow".into(),
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
    let app = Router::new()
        .route("/discover", get(discover))
        .route("/invoke/{service}/{handler}", post(sdk_invoke));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(format!("http://{addr}"))
}

async fn child_of(rt: &str, handler: &str) -> Result<Option<(String, String)>> {
    let http = reqwest::Client::new();
    let list: serde_json::Value = http
        .get(format!("{rt}/invocations?limit=100"))
        .send()
        .await?
        .json()
        .await?;
    Ok(list.as_array().and_then(|rows| {
        rows.iter()
            .find(|i| i["service"] == "Child" && i["handler"] == handler)
            .map(|i| {
                (
                    i["id"].as_str().unwrap_or_default().to_string(),
                    i["status"].as_str().unwrap_or_default().to_string(),
                )
            })
    }))
}

#[tokio::test]
async fn cancelling_parent_cancels_called_children_but_not_sent_ones() -> Result<()> {
    let sdk = spawn_sdk().await?;
    let rt = spawn_runtime("cancel-tree").await?;
    let http = reqwest::Client::new();

    http.post(format!("{rt}/deployments"))
        .json(&serde_json::json!({ "uri": sdk }))
        .send()
        .await?
        .error_for_status()?;

    // The parent runs, spawns two children, then suspends waiting for the first.
    let started: serde_json::Value = http
        .post(format!("{rt}/invoke/Parent/run/send"))
        .body("{}")
        .send()
        .await?
        .json()
        .await?;
    let parent = started["id"].as_str().expect("has an id").to_string();

    eventually("both children have run and suspended", || async {
        let a = child_of(&rt, "call").await?;
        let b = child_of(&rt, "send").await?;
        Ok(matches!((&a, &b), (Some((_, sa)), Some((_, sb)))
            if sa == "suspended" && sb == "suspended"))
    })
    .await?;

    let (called_child, _) = child_of(&rt, "call").await?.expect("called child");
    let (sent_child, _) = child_of(&rt, "send").await?.expect("fired child");

    // Cancel the parent.
    let cancel: serde_json::Value = http
        .post(format!("{rt}/invocations/{parent}/cancel"))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(cancel["cancelled"], true);

    // The `ctx.call` child must die with it — this is what used not to happen.
    eventually("the ctx.call child is cancelled along", || async {
        let s: serde_json::Value = http
            .get(format!("{rt}/invocations/{called_child}"))
            .send()
            .await?
            .json()
            .await?;
        Ok(s["status"] == "failed")
    })
    .await?;
    // `failed` is enough evidence: `Child`'s program only sleeps then suspends, so the
    // only way for it to reach `failed` is to be cancelled. (`GET /invocations/{id}`
    // does not yet return the `code`/`message` of a failed invocation — that is a
    // separate introspection gap, not part of this test.)

    // The `ctx.send` child does **not**: it is detached from the call tree. Wait a
    // beat so that a mistaken cancel (if any) has time to arrive before asserting.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let s: serde_json::Value = http
        .get(format!("{rt}/invocations/{sent_child}"))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(
        s["status"], "suspended",
        "ctx.send is detached work, cancelling the parent must not touch it: {s}"
    );
    Ok(())
}

/// The other half, and the more expensive one: **a call still in the outbox at cancel
/// time**.
///
/// A fan-out issues N `Call` commands in one turn, but they are drained one at a
/// time. Cancelling the parent halfway would leave the not-yet-drained ones to be
/// created and run to the end anyway — each one a model call whose result nobody
/// reads. This case really happens with `ctx.all([...ctx.call])`, and it is the case
/// that "cancel the ones already created" alone does not reach.
#[tokio::test]
async fn cancelling_parent_suppresses_child_calls_still_in_outbox() -> Result<()> {
    let sdk = spawn_sdk().await?;
    let rt = spawn_runtime("cancel-tree-outbox").await?;
    let http = reqwest::Client::new();

    http.post(format!("{rt}/deployments"))
        .json(&serde_json::json!({ "uri": sdk }))
        .send()
        .await?
        .error_for_status()?;

    let started: serde_json::Value = http
        .post(format!("{rt}/invoke/Parent/late/send"))
        .body("{}")
        .send()
        .await?
        .json()
        .await?;
    let parent = started["id"].as_str().expect("has an id").to_string();

    // **This window is a state, not a moment.** `Parent/late` calls `Child/call` then
    // `Absent/slow`, and this node has no deployment for `Absent` yet — so the second row
    // *stays* in the outbox and stays there until we register that endpoint. There is
    // nothing to race.
    //
    // An earlier version relied on a slow handler holding the drain back. Moving the
    // child's SDK turn into its own task took that window away, and the first patch —
    // making the *parent's* replay turn sleep — only changed the race rather than
    // removing it: that turn holds the parent's invocation lock, and `/cancel` needs
    // exactly that lock, so the cancel only got in **after** the drain had moved on to
    // the second row.
    eventually("the first child has been created", || async {
        Ok(child_of(&rt, "call").await?.is_some())
    })
    .await?;
    assert!(
        invocation_of(&rt, "Absent", "slow").await?.is_none(),
        "the second one should not have been dispatched yet — the test no longer checks what it means to check"
    );

    let cancel: serde_json::Value = http
        .post(format!("{rt}/invocations/{parent}/cancel"))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(cancel["cancelled"], true);

    // Only now let `Absent` appear. From here the drain **can decide** the second row —
    // and the right answer is "drop it", because the caller is dead.
    let absent_uri = spawn_absent_sdk().await?;
    http.post(format!("{rt}/deployments"))
        .json(&serde_json::json!({ "uri": absent_uri }))
        .send()
        .await?
        .error_for_status()?;

    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert!(
        invocation_of(&rt, "Absent", "slow").await?.is_none(),
        "a call in the outbox of a cancelled invocation was still dispatched — \
         that is a model call whose result nobody reads"
    );
    Ok(())
}

/// A fan-out **must not close the front door of the whole partition**.
///
/// It used to be `#[ignore]` — a measured limitation, not yet a held invariant. Now
/// it is an invariant, so it runs with every other test.
///
/// **Why it used to be red.** The outbox drain ran a whole SDK turn for each child
/// right inside itself, and it held the drain lock for the whole pass — so every
/// other `/invoke`, other service, other object, entirely unrelated, had to queue
/// behind the whole brood. Measured on the ai-pipeline example: a 715ms call became
/// 10.3 seconds, and `/vqueues` reported `waiting=0` throughout, so it was not flow
/// control.
///
/// **Why it is green now, and why that is NOT because the drain got faster.** At
/// the time the drain still ran the child's whole SDK turn. What changed is *who has
/// to wait for it*: the drainer is now a separate task, and a caller only waits for
/// the mark to pass **the rows its own turn produced**. `Probe/peek` issues no
/// commands, so it produces no rows, so it has nothing to wait for.
///
/// That is also why the bound below measures the right thing: it does not ask "is
/// the fan-out fast yet" but "is an unrelated call held back by the fan-out".
///
/// Numbers on this yardstick, step by step: **1.41s → 94ms → 174ms** (idle ~56ms).
/// The last number is higher than the middle one, and that is not a regression: the
/// middle step still ran the SDK turn inside the drain, so there were fewer tasks,
/// while the last step turned sequential work into many tasks asking for the engine
/// at once. What is gone is the **head-of-line blocking**: 174ms is very far from the
/// 1.2s the child sleeps, so the front door no longer stands behind any SDK turn. What
/// remains is the known "one thread per partition" ceiling, not a lock.
///
/// The *structural* property is stronger than the number, and it is what is worth
/// keeping: **the drain no longer calls the SDK on any branch.** The 900ms bound
/// below catches every time an SDK call sneaks back in there, because the cheapest
/// turn in this suite already takes 1.2s.
///
/// Recording a direction already ruled out, so nobody walks it again: splitting the
/// drain lock (held across the scan, released across the run) plus an in-RAM "claim"
/// table also turns this yardstick green, but it **breaks** the semantics that
/// `ctx_send_same_partition_done_before_invoke_returns` pins below, and the patch
/// for that was timing-sensitive — e2e red 2/90, then green again once logging was
/// added. The path in use has no claim table to break: there is only one reader, so
/// the question "whose row is this" does not exist.
#[tokio::test]
async fn fan_out_must_not_close_the_partitions_ingress() -> Result<()> {
    let sdk = spawn_sdk().await?;
    let rt = spawn_runtime("drain-ingress").await?;
    let http = reqwest::Client::new();

    http.post(format!("{rt}/deployments"))
        .json(&serde_json::json!({ "uri": sdk }))
        .send()
        .await?
        .error_for_status()?;

    // Baseline: the front door while the partition is idle.
    let t = std::time::Instant::now();
    http.post(format!("{rt}/invoke/Probe/peek"))
        .body("{}")
        .send()
        .await?
        .error_for_status()?;
    let idle = t.elapsed();

    // `Parent/fanout` calls `Child/call` (the SDK sleeps 1.2s) then `Child/slow`. So the
    // drain is busy for at least 1.2 seconds.
    http.post(format!("{rt}/invoke/Parent/fanout/send"))
        .body("{}")
        .send()
        .await?
        .error_for_status()?;
    eventually("fan-out has started draining", || async {
        Ok(child_of(&rt, "call").await?.is_some())
    })
    .await?;

    // The same call, while the drain is running.
    let t = std::time::Instant::now();
    http.post(format!("{rt}/invoke/Probe/peek"))
        .body("{}")
        .send()
        .await?
        .error_for_status()?;
    let busy = t.elapsed();

    eprintln!("front door: idle {idle:?}, while the fan-out drains {busy:?}");
    // A generous bound: what we block is "standing behind the child's WHOLE 1.2s SDK
    // turn", not a few dozen ms of normal contention.
    assert!(
        busy < Duration::from_millis(900),
        "the fan-out closed the front door: idle {idle:?}, while draining {busy:?} — \
         the drain holds its lock across the child's SDK turn"
    );
    Ok(())
}

/// A `ctx.send` is **durable and dispatched** before `/invoke` answers — no more.
///
/// The old promise was stronger: "a same-partition `ctx.send` runs *in the caller's
/// turn*", i.e. the target had **finished** by the time `/invoke` answered. That
/// promise was dropped, and the reason is short: it branched on topology — green on
/// a one-partition dev setup, broken on an eight-partition production one — the same
/// disease already diagnosed and cured for HTTP status codes. "Run a same-partition
/// target right away" would be an *optimisation* not done, not a contract.
///
/// What **is** still kept, and what this test pins: when `/invoke` answers, the
/// calls that invocation produced are durable and dispatched — the target invocation
/// **exists** and can be looked up. A client reading right away does not see
/// "nothing yet"; it sees a running invocation. What is no longer guaranteed is
/// *finished*.
///
/// Background loops ON: they are the partition's second drain pass, and the only
/// thing that creates contention on the drain path.
#[tokio::test]
async fn ctx_send_durable_and_sent_before_invoke_returns() -> Result<()> {
    let sdk = spawn_sdk().await?;
    // Background loops ON: they compete for the drain path with `/invoke`'s turn, and
    // that is exactly the variable that would break this constraint if the drain lock
    // were split wrongly.
    let rt = spawn_runtime_ticking("drain-send-dong-bo", Some(Duration::from_millis(20))).await?;
    let http = reqwest::Client::new();

    http.post(format!("{rt}/deployments"))
        .json(&serde_json::json!({ "uri": sdk }))
        .send()
        .await?
        .error_for_status()?;

    for attempt in 0..5 {
        let r: serde_json::Value = http
            .post(format!("{rt}/invoke/Fire/fire"))
            .body("{}")
            .send()
            .await?
            .json()
            .await?;
        assert_eq!(r["status"], "completed", "attempt {attempt}: {r}");

        // NO waiting, no `eventually`: the target must already exist right now.
        let n = http
            .get(format!("{rt}/invocations?limit=200"))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?
            .as_array()
            .map(|rows| rows.iter().filter(|i| i["service"] == "Probe").count())
            .unwrap_or(0);
        assert_eq!(
            n,
            attempt + 1,
            "attempt {attempt}: when /invoke answered, the target invocation did not exist yet — \
             the call had not been dispatched, not merely not finished"
        );
    }
    Ok(())
}
