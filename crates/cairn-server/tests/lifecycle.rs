// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The invocation lifecycle: five suspicions, A–E.
//!
//! Each test pins the **real behaviour** of one suspicion, measured on a one-range
//! runtime (`EngineHandle`, like `flow_control.rs`) with a scripted fake SDK
//! (`common::fake_sdk`). Where a suspicion was a real bug the test was red, marked
//! `ignore` until the fix landed; where a suspicion is harmless or intended the test is
//! green and pins exactly the row of the valid-state table (`cairn_lifecycle::valid`)
//! that the lifecycle inventory recorded.
//!
//! Where a durable state is needed that no API path can build deterministically ("die
//! exactly between two commits", "the rescue net's `pause_with`"), the test stops the
//! runtime and calls **exactly** the `Engine` operation the product path calls
//! ([`with_engine`]) — the same way `flow_control.rs::orphan_entry_does_not_become_permanent_lock`
//! builds an orphan entry. Reopening the runtime on the same directory is a leader change.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    axum::{
        extract::State,
        http::{header, HeaderMap, StatusCode},
        routing::post,
        Router,
    },
    bytes::Bytes,
    cairn_engine::{CompletionPayload, Engine, InvocationStatus, SignalId, Target},
    cairn_protocol::{pb, Message},
    cairn_server::{
        actor::{EngineHandle, InvokeOutcome, Leadership, Readiness, TurnLimits},
        discovery::{
            Contract, Deployment, HandlerManifest, ProtocolMode, ServiceManifest, ServiceType,
            WIRE_MAX,
        },
        invoker::{decode_stream, encode_stream, INVOCATION_V2},
        Registry,
    },
    cairn_store::{LeadershipGrant, Limit, PartitionId, PartitionWriter, RuleBook, Scope, Stage},
    common::fake_sdk::{FakeSdk, Step, HOLD_SIGNAL},
    slatedb::{
        config::Settings,
        object_store::{local::LocalFileSystem, ObjectStore},
        Db,
    },
    std::{
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    },
};

/// The limit for every wait.
const TIMEOUT: Duration = Duration::from_secs(10);

// ───────────────────────────────────────────────────────────── runtime

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;
    Ok(root)
}

/// No SDK wait limit (the fake SDK does not hang mid-turn); `queue_wait` per test.
fn limits(queue_wait: Option<Duration>) -> TurnLimits {
    TurnLimits {
        sdk_timeout: None,
        queue_wait,
        ..TurnLimits::default()
    }
}

async fn open_db(root: &Path) -> Result<Db> {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(root)?);
    Ok(Db::builder("partition-0", store)
        .with_settings(Settings {
            flush_interval: Some(Duration::from_millis(10)),
            ..Default::default()
        })
        .build()
        .await?)
}

/// Opens an engine on `root`. Calling it again on the same `root` is a leader change:
/// the store stays intact, RAM does not.
fn spawn_runtime(root: &Path, registry: Registry, limits: TurnLimits) -> Result<EngineHandle> {
    let root = root.to_path_buf();
    EngineHandle::spawn_with_transport(
        Box::new(move || {
            let root = root.clone();
            Box::pin(async move {
                let db = open_db(&root).await.expect("open SlateDB");
                let writer =
                    PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
                Ok(Leadership {
                    engine: Engine::open(writer).await?,
                    keeper: None,
                })
            })
        }),
        registry,
        None,
        Readiness::WhenLeader,
        cairn_server::transport::Transport::disconnected(),
        limits,
        None,
    )
    .map_err(Into::into)
}

/// A new leader with `rules` **set before** the deployment registry has anything — so that
/// `recover` (which runs when the registry changes) delivers no turn under an empty rule book.
async fn start(
    root: &Path,
    deployment: &Deployment,
    rules: Option<RuleBook>,
    limits: TurnLimits,
) -> Result<EngineHandle> {
    let registry = Registry::new();
    let rt = spawn_runtime(root, registry.clone(), limits)?;
    if let Some(book) = rules {
        rt.set_rules(book).await?;
    }
    registry.upsert(deployment.clone());
    rt.deployments_changed().await?;
    Ok(rt)
}

/// A **stopped** runtime: open `Engine` directly on `root`, do `f`, seal.
/// Used to build a durable state with exactly the `Engine` operation the product path
/// calls — including "only the first commit of a two-commit sequence" (dying midway).
async fn with_engine<T>(root: &Path, f: impl AsyncFnOnce(&mut Engine) -> Result<T>) -> Result<T> {
    let db = open_db(root).await?;
    let writer = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
    let mut engine = Engine::open(writer).await?;
    let out = f(&mut engine).await?;
    engine.seal().await?;
    Ok(out)
}

async fn deployment_of(sdk: &FakeSdk) -> Result<Deployment> {
    let mut d = cairn_server::discovery::discover(
        &cairn_server::egress::TenantHttp::h1(Default::default()),
        &sdk.url,
        None,
    )
    .await
    .map_err(|e| anyhow!("discover fake SDK: {e:?}"))?;
    // A fixed id: invocations are bound to a deployment by id, and the next leader rebuilds the registry.
    d.id = "dp_lifecycle".into();
    Ok(d)
}

fn rules(entries: &[(&str, u32)], version: u64) -> RuleBook {
    let mut b = RuleBook::default();
    for (p, n) in entries {
        b.set(p, Some(Limit::Max(*n)), None, None, 0).unwrap();
    }
    b.version = version;
    b
}

fn target(service: &str, handler: &str, scope: Option<&str>, key: &str) -> Target {
    Target {
        tenant: Default::default(),
        scope: scope.map(|s| Scope::new(s).unwrap()),
        service: service.into(),
        handler: handler.into(),
        object_key: key.into(),
    }
}

fn id(n: u8) -> [u8; 24] {
    let mut b = [0u8; 24];
    b[15] = n;
    b
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Waits until the status of `id` satisfies `ok`; returns that status.
async fn wait_status(
    rt: &EngineHandle,
    id: [u8; 24],
    what: &str,
    ok: impl Fn(&InvocationStatus) -> bool,
) -> Result<InvocationStatus> {
    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        let s = rt.status(id).await?;
        if let Some(s) = s.as_ref().filter(|s| ok(s)) {
            return Ok(s.clone());
        }
        if std::time::Instant::now() > deadline {
            anyhow::bail!("{what}: after {TIMEOUT:?} still {s:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn is_completed(s: &InvocationStatus) -> bool {
    matches!(s, InvocationStatus::Completed(_))
}

fn is_suspended(s: &InvocationStatus) -> bool {
    matches!(s, InvocationStatus::Suspended)
}

/// The number of `Start`s the fake SDK received for invocation `id`.
fn starts_for(sdk: &FakeSdk, id: [u8; 24]) -> usize {
    sdk.starts()
        .iter()
        .filter(|s| s.id.as_ref() == id.as_slice())
        .count()
}

async fn hold_signal(rt: &EngineHandle, id: [u8; 24]) -> Result<()> {
    rt.external_signal(
        id,
        SignalId::Name(HOLD_SIGNAL.into()),
        CompletionPayload::Void,
    )
    .await?;
    Ok(())
}

// ─────────────────────────────────────── fake SDK "two timers at once" (suspicion A)

/// Handler `Timers/two`: the first turn issues **two** `ctx.sleep`s at once (due `w1`,
/// `w2`, waiting for either — `Promise.race` does not drop the loser), then once both
/// have arrived it waits for the signal [`HOLD_SIGNAL`] — the invocation stays alive, so an
/// extra turn after that **reaches the SDK** instead of stopping at the gate.
#[derive(Clone)]
struct TwoSleeps {
    starts: Arc<AtomicUsize>,
    w1: u64,
    w2: u64,
}

async fn two_sleeps(State(st): State<TwoSleeps>, body: Bytes) -> (StatusCode, HeaderMap, Bytes) {
    let frames = decode_stream(body).expect("the runtime sends valid frames");
    let mut journaled = 0usize;
    let mut done = std::collections::BTreeSet::new();
    for f in &frames {
        match cairn_protocol::decode(f).expect("frame decodes") {
            Message::Start(_) => {
                st.starts.fetch_add(1, Ordering::SeqCst);
            }
            Message::Input(_) => {}
            Message::SleepCompletion(n) => {
                done.insert(n.completion_id);
            }
            m if m.kind() == cairn_protocol::MessageKind::Command => journaled += 1,
            _ => {}
        }
    }
    let mut out = Vec::new();
    if journaled == 0 {
        for (cid, wake) in [(1, st.w1), (2, st.w2)] {
            out.push(Message::Sleep(pb::SleepCommandMessage {
                wake_up_time: wake,
                result_completion_id: cid,
                name: String::new(),
            }));
        }
    }
    let missing: Vec<u32> = [1, 2].into_iter().filter(|c| !done.contains(c)).collect();
    out.push(Message::Suspension(if missing.is_empty() {
        pb::SuspensionMessage {
            waiting_named_signals: vec![HOLD_SIGNAL.into()],
            ..Default::default()
        }
    } else {
        pb::SuspensionMessage {
            waiting_completions: missing,
            ..Default::default()
        }
    }));
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, INVOCATION_V2.parse().unwrap());
    (StatusCode::OK, headers, encode_stream(&out))
}

async fn spawn_two_sleeps(st: TwoSleeps) -> Result<Deployment> {
    let app = Router::new()
        .route("/invoke/{service}/{handler}", post(two_sleeps))
        .with_state(st);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(Deployment {
        id: "dp_two_sleeps".into(),
        tenant: "default".into(),
        created_at_ms: 0,
        endpoint: format!("http://{addr}"),
        wire_version: WIRE_MAX,
        protocol_mode: ProtocolMode::RequestResponse,
        request_identity: None,
        rr_compression: None,
        replay_cache: false,
        transport: None,
        services: vec![ServiceManifest {
            name: "Timers".into(),
            ty: ServiceType::Service,
            handlers: vec![HandlerManifest {
                name: "two".into(),
                ty: None,
                contract: Default::default(),
            }],
            contract: Default::default(),
        }],
    })
}

// ───────────────────────────────────────────────────────────── suspicion A

/// **Suspicion A — a timer delivers a duplicate turn to an invocation that already has a
/// delivered turn that has not run yet.**
///
/// Interleaving:
/// 1. `Timers/two` runs its first turn: two `ctx.sleep`s due `w1 < w2`, waiting for
///    either. The SDK sees 1 `Start`.
/// 2. One background tick at `now ≥ w2`: both timers are due in the **same** tick.
///    Timer 1: the invocation lock is free ⇒ the `Deliver` branch writes the completion then
///    `runner.queue(id)` — the turn is delivered, its task still waits for the engine (the
///    tick holds it), not yet holding the lock.
/// 3. Timer 2: the lock is still free (the task has not run) ⇒ `Deliver` writes completion 2,
///    and before the fix **delivered another** turn — `tick` only asked about the lock, not
///    `Runner::is_queued` as `fire_retries` does (FL-20).
/// 4. The first turn runs: the journal already has both completions, the SDK moves on and
///    waits for the signal (invocation `suspended`).
///
/// Must see: the SDK receives exactly **2** `Start`s (the first turn + one turn carrying both
/// completions). Before the fix: the duplicate turn from step 3 ran after the first turn, the
/// gate saw `suspended` and let it run, and the SDK was called a needless 3rd time — the same
/// shape as a turn delivered by any path (retry, outbox, recover) that had not taken the
/// lock when the timer fired.
#[tokio::test]
async fn two_timers_on_one_tick_deliver_one_turn() -> Result<()> {
    let base = now_ms();
    let st = TwoSleeps {
        starts: Arc::new(AtomicUsize::new(0)),
        w1: base + 3_600_000,
        w2: base + 3_601_000,
    };
    let d = spawn_two_sleeps(st.clone()).await?;
    let root = fresh_root("lifecycle-a-two-timers")?;
    let rt = start(&root, &d, None, limits(None)).await?;
    let starts = || st.starts.load(Ordering::SeqCst);

    let out = rt
        .submit(
            id(1),
            target("Timers", "two", None, ""),
            None,
            None,
            Bytes::new(),
        )
        .await?;
    ensure!(matches!(out, InvokeOutcome::Waiting), "{out:?}");
    wait_status(&rt, id(1), "first turn suspends", is_suspended).await?;
    ensure!(starts() == 1, "first turn: {} Start", starts());

    let advanced = rt.tick(base + 7_200_000).await?;
    ensure!(advanced == 2, "tick must deliver both timers: {advanced}");
    // The turn carrying both completions finishes and waits for the signal again.
    common::within(TIMEOUT, "turn after the two timers", async {
        while starts() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    })
    .await?;
    wait_status(&rt, id(1), "waits for the signal again", is_suspended).await?;
    // A duplicate turn (if any) runs right after the first; give it time to reach the SDK.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let n = starts();
    rt.shutdown().await?;
    ensure!(
        n == 2,
        "SDK received {n} Start — an extra turn from the second timer (delivered while a turn had not run)"
    );
    Ok(())
}

// ───────────────────────────────────────────────────────────── suspicion B

/// **Suspicion B — a throttled `Drive`-owned turn spends an attempt without the SDK
/// being called** (outside the scope of FL-19, which applies only to
/// `RetryOwner::Caller`).
///
/// Interleaving:
/// 1. Rule `acme = 0`, the handler declares `retryPolicyMaxAttempts = 1`. `id(1)` is queued
///    (`/send`); the leader steps down.
/// 2. New leader: `recover` delivers a background turn (`drive_now`, owner `Drive`); the turn
///    runs out of slot wait because of the budget, `Throttled`, and `park_for_retry` sets a
///    scheduled row with `attempt = 1` — one attempt already "counted", although the turn
///    never entered `Running`.
/// 3. Lift the limit (`acme = 1`), fire a background tick: `fire_retries` delivers a turn
///    carrying `attempt = 1`; preflight sees `retry_count 1 ≥ maxAttempts 1`.
///
/// Must see (the single-meaning attempt rule): `completed` after exactly **one** SDK call.
/// Before the fix: `paused` 575 "out of 1 attempt" with the SDK never called —
/// via the `park_for_retry` path (and `schedule_after_error` after an `ensure_queued`
/// error, the same meaning of `attempt` before `Running`).
#[tokio::test]
async fn parked_first_turn_does_not_spend_attempt() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Work", "run", vec![Step::output("\"done\"")])
        .contract(
            "Work",
            "run",
            Contract {
                retry_policy_max_attempts: Some(1),
                ..Default::default()
            },
        )
        .spawn()
        .await?;
    let d = deployment_of(&sdk).await?;
    let root = fresh_root("lifecycle-b-throttled-first-turn")?;
    let wait = limits(Some(Duration::from_millis(300)));
    let work = target("Work", "run", Some("acme"), "");

    {
        let rt = start(&root, &d, Some(rules(&[("acme", 0)], 1)), wait).await?;
        let out = rt
            .submit(id(1), work.clone(), None, None, Bytes::from_static(b"{}"))
            .await?;
        ensure!(matches!(out, InvokeOutcome::Waiting), "{out:?}");
        rt.shutdown().await?;
    }

    let rt = start(&root, &d, Some(rules(&[("acme", 0)], 1)), wait).await?;
    // `recover` delivers a background turn; wait for it to run out of slot wait and set a scheduled row.
    tokio::time::sleep(Duration::from_millis(800)).await;
    ensure!(
        sdk.starts().is_empty(),
        "the SDK was called: {:?}",
        sdk.starts()
    );

    rt.set_rules(rules(&[("acme", 1)], 2)).await?;
    let deadline = std::time::Instant::now() + TIMEOUT;
    let status = loop {
        let s = rt.status(id(1)).await?;
        if let Some(s) = s.filter(|s| {
            matches!(
                s,
                InvocationStatus::Completed(_)
                    | InvocationStatus::Failed { .. }
                    | InvocationStatus::Paused { .. }
            )
        }) {
            break s;
        }
        ensure!(
            std::time::Instant::now() < deadline,
            "id(1) not stopped after {TIMEOUT:?}"
        );
        rt.tick(now_ms() + 86_400_000).await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    rt.shutdown().await?;
    let calls = sdk.starts_of("Work", "run").len();
    ensure!(
        is_completed(&status) && calls == 1,
        "a turn throttled before `Running` spent an attempt: {status:?}, SDK called {calls} times"
    );
    Ok(())
}

// ───────────────────────────────────────────────────────────── suspicion C

/// **Suspicion C — status × stage drift apart across a leader
/// change.** Three mismatched pairs the product paths leave behind, each on its own object
/// key:
///
/// | key | pair | path that produces it |
/// |---|---|---|
/// | `k1` | `paused` 577 + stage `Suspended` | the rescue net's `pause_with` (`pause_stuck`) or a poison row (`poison_row`) — does not change the stage |
/// | `k2` | `running` + stage `Paused` | `Engine::resume` committed, the leader died before `begin_turn` |
/// | `k3` | `paused` 576 + stage `Inbox` | a quarantine hold (`CreateOptions.paused`), or `pause_with` for an invocation that never ran |
///
/// Order:
/// 1. Old leader: `P1`, `P2` (`k1`, `k2`) run their first turn and wait — holding the VO lock.
///    `H3` holds `k3`, `P3` queues behind it (`Inbox`). The leader steps down.
/// 2. Build the three pairs with exactly the `Engine` operations of the paths above.
/// 3. New leader: `restore_flow` rebuilds locks by **stage**, `recover` runs by
///    **status**.
///
/// Must see (valid-state table, all three harmless):
/// - 3 VO locks: `P1` (`Suspended`, currently `paused` — held like every `paused`, FL-4),
///   `P2` (`Paused`), `H3`; `P3` (`Inbox`) does **not** hold a lock.
/// - `recover` runs exactly `P2` (status `running`); `P1`, `P3` get no turn.
/// - `k3` is usable as soon as `H3` completes: a held `P3` does not block the object.
/// - `q1` on `k1` waits behind the `paused` `P1`; `resume` `P1` then waking it
///   completes both, in order.
/// - `resume` `P3` and it runs and completes.
#[tokio::test]
async fn status_stage_mismatch_across_leader_change_still_locks_and_runs() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object("Obj", "hold", vec![Step::Suspend, Step::output("\"held\"")])
        .object("Obj", "quick", vec![Step::output("\"quick\"")])
        .spawn()
        .await?;
    let d = deployment_of(&sdk).await?;
    let root = fresh_root("lifecycle-c-status-stage")?;
    let hold = |k: &str| target("Obj", "hold", None, k);
    let quick = |k: &str| target("Obj", "quick", None, k);
    let (p1, p2, h3, p3, q3, q1) = (id(1), id(2), id(3), id(4), id(5), id(6));

    {
        let rt = start(&root, &d, None, limits(None)).await?;
        for (i, k) in [(p1, "k1"), (p2, "k2"), (h3, "k3")] {
            rt.submit(i, hold(k), None, None, Bytes::from_static(b"{}"))
                .await?;
            wait_status(&rt, i, "first turn suspends", is_suspended).await?;
        }
        let out = rt
            .submit(p3, hold("k3"), None, None, Bytes::from_static(b"{}"))
            .await?;
        ensure!(matches!(out, InvokeOutcome::Waiting), "{out:?}");
        rt.shutdown().await?;
    }
    ensure!(
        starts_for(&sdk, p3) == 0,
        "P3 must not run while H3 holds k3"
    );

    with_engine(&root, async |e: &mut Engine| {
        // k1: the rescue net moves a lock-holding invocation to `paused` 577.
        e.pause_with(&p1, cairn_engine::INFRA_STUCK, "test: pause_stuck")
            .await?;
        // k2: `paused` (stage `Paused`, as with `Effect::Finish` + `end_turn`), then
        // `resume` commits and the leader dies before the turn.
        e.set_stage(&p2, Stage::Paused).await?;
        e.pause_with(&p2, 571, "test: pause").await?;
        ensure!(matches!(
            e.resume(&p2).await?,
            cairn_engine::ResumeOutcome::Resumed
        ));
        // k3: a quarantine hold on an invocation that never ran.
        e.pause_with(&p3, cairn_engine::QUARANTINED, "test: hold")
            .await?;
        let stage = async |i: &[u8; 24]| e.vqueue_of(i).await.map(|s| s.map(|s| s.stage));
        ensure!(stage(&p1).await? == Some(Stage::Suspended));
        ensure!(stage(&p2).await? == Some(Stage::Paused));
        ensure!(stage(&p3).await? == Some(Stage::Inbox));
        Ok(())
    })
    .await?;

    let rt = start(&root, &d, None, limits(None)).await?;
    // FL-8: the locks are rebuilt before the first slot grant, not before the first
    // command — wait for the chunked rebuild to finish.
    let flow = common::within(TIMEOUT, "lock rebuild finishes", async {
        loop {
            let flow = rt.flow().await?;
            if !flow.scheduler.restoring {
                return Ok(flow);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    ensure!(
        flow.scheduler.locks_held == 3,
        "rebuilt locks: {} (want 3: P1, P2, H3 — not P3)",
        flow.scheduler.locks_held
    );
    // `recover` runs P2 (running): the turn reaches the SDK then waits for the signal again.
    common::within(TIMEOUT, "recover runs P2", async {
        while starts_for(&sdk, p2) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    })
    .await?;
    wait_status(&rt, p2, "P2 waits again", is_suspended).await?;
    ensure!(starts_for(&sdk, p1) == 1, "P1 is paused yet still ran");
    ensure!(starts_for(&sdk, p3) == 0, "P3 is paused yet still ran");

    // k3: H3 completes ⇒ the object is free, a held P3 does not block `q3`.
    hold_signal(&rt, h3).await?;
    wait_status(&rt, h3, "H3 completes", is_completed).await?;
    rt.submit(q3, quick("k3"), None, None, Bytes::from_static(b"{}"))
        .await?;
    wait_status(&rt, q3, "q3 completes although P3 is paused", is_completed).await?;

    // k1: `q1` waits behind the paused P1 (holding the lock); resume then wake P1.
    rt.submit(q1, quick("k1"), None, None, Bytes::from_static(b"{}"))
        .await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    ensure!(
        starts_for(&sdk, q1) == 0,
        "q1 ran while the paused P1 still held lock k1"
    );
    ensure!(matches!(
        rt.resume(p1).await?,
        cairn_engine::ResumeOutcome::Resumed
    ));
    wait_status(&rt, p1, "P1 waits again after resume", is_suspended).await?;
    hold_signal(&rt, p1).await?;
    wait_status(&rt, p1, "P1 completes", is_completed).await?;
    wait_status(&rt, q1, "q1 completes after P1", is_completed).await?;

    // k3: resume P3 ⇒ it runs.
    ensure!(matches!(
        rt.resume(p3).await?,
        cairn_engine::ResumeOutcome::Resumed
    ));
    wait_status(&rt, p3, "P3 waits after resume", is_suspended).await?;
    hold_signal(&rt, p3).await?;
    wait_status(&rt, p3, "P3 completes", is_completed).await?;
    hold_signal(&rt, p2).await?;
    wait_status(&rt, p2, "P2 completes", is_completed).await?;
    rt.shutdown().await?;
    Ok(())
}

// ───────────────────────────────────────────────────────────── suspicion D

/// **Suspicion D, the admin-cancel part — dying between two commits
/// left an `Inbox` queue entry of an invocation that has stopped for good, forever.**
///
/// Formerly `turn::cancel` was two commits: `Engine::cancel` (status
/// `failed` 572, drop the retry row, answer the waiters) then `release_queue_slot`
/// (leave the queue). `restore_flow` clears orphan entries only in lock-holding stages
/// (FL-8); `Inbox` is skipped before the status read. Now the first commit
/// (`Engine::cancel`) leaves the queue too — "dying after the first commit" is dying after
/// the whole cancel.
///
/// Order:
/// 1. `A` holds the VO lock of `k` (waiting); `B` queues behind it (`Inbox`). The leader steps down.
/// 2. Admin-cancel `B`: only the first commit (`Engine::cancel`) — die before leaving the queue.
/// 3. New leader; `A` completes; `C` on `k` completes. The leader steps down.
///
/// Must see: no queue entry of `B` left (an invocation that has stopped for good is
/// not in the queue — a row of the valid-state table). Before the fix: `B`'s `Inbox` entry
/// survived the leader change, and no path cleared it (`restore_flow` skips
/// `Inbox`, `recover` only looks at `running`, `purge` does not leave the queue) short of
/// cancelling again by hand. Harmless for liveness (it never becomes a lock, does not block
/// `k`), but a leak and an invalid status × stage pair. The fix folds finishing into one
/// commit on every path.
#[tokio::test]
async fn admin_cancel_dying_between_two_commits_leaves_no_inbox_entry() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object("Obj", "hold", vec![Step::Suspend, Step::output("\"held\"")])
        .object("Obj", "quick", vec![Step::output("\"quick\"")])
        .spawn()
        .await?;
    let d = deployment_of(&sdk).await?;
    let root = fresh_root("lifecycle-d-cancel-between-commits")?;
    let (a, b, c) = (id(1), id(2), id(3));

    {
        let rt = start(&root, &d, None, limits(None)).await?;
        rt.submit(
            a,
            target("Obj", "hold", None, "k"),
            None,
            None,
            Bytes::new(),
        )
        .await?;
        wait_status(&rt, a, "A waits, holding the lock", is_suspended).await?;
        let out = rt
            .submit(
                b,
                target("Obj", "hold", None, "k"),
                None,
                None,
                Bytes::new(),
            )
            .await?;
        ensure!(matches!(out, InvokeOutcome::Waiting), "{out:?}");
        rt.shutdown().await?;
    }
    with_engine(&root, async |e: &mut Engine| {
        ensure!(matches!(
            e.cancel(&b, "test: admin cancel").await?,
            cairn_engine::CancelOutcome::Cancelled
        ));
        Ok(())
    })
    .await?;

    {
        let rt = start(&root, &d, None, limits(None)).await?;
        hold_signal(&rt, a).await?;
        wait_status(&rt, a, "A completes", is_completed).await?;
        rt.submit(
            c,
            target("Obj", "quick", None, "k"),
            None,
            None,
            Bytes::new(),
        )
        .await?;
        wait_status(&rt, c, "C completes", is_completed).await?;
        rt.shutdown().await?;
    }
    let left = with_engine(&root, async |e: &mut Engine| {
        Ok(e.vqueue_of(&b).await?.map(|s| s.stage))
    })
    .await?;
    ensure!(
        left.is_none(),
        "queue entry of B (cancelled) still at stage {left:?} after the leader change"
    );
    Ok(())
}

/// **Suspicion D, the `restart` part — the old run's scheduled row
/// survives the `restart` commit, but no turn runs for it.**
///
/// `Engine::restart` ends the old run (`completed` `{"continuedAsNew": …}`) and
/// leaves the queue in the same commit, but did not `drop_retry` (likewise for
/// `continue_as_new` inside a turn: the turn's watch row was not removed by `end_turn`
/// because the outcome is terminal). The stray row fires at its due time, `fire_retries`
/// reads the terminal status and cancels the row.
///
/// Order:
/// 1. `old` waits holding the VO lock; it has a durable scheduled retry row (from an
///    earlier failure — built with `schedule_retry`, due far away so the real background
///    tick does not touch it).
/// 2. `restart` `old` (keeping entry 0): `new` is created, delivered, run.
/// 3. A background tick after the stray row's due time.
///
/// Must see (a row of the transition table, harmless): the SDK never receives
/// a `Start` of `old` after `restart`; after the tick, `old` has no scheduled row;
/// `new` runs normally. Pinned both before and after the fix (removing the row in the same commit).
#[tokio::test]
async fn restart_does_not_rerun_old_version_via_leftover_schedule_row() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object("Obj", "hold", vec![Step::Suspend, Step::output("\"held\"")])
        .spawn()
        .await?;
    let d = deployment_of(&sdk).await?;
    let root = fresh_root("lifecycle-d-restart-leftover")?;
    let old = id(1);
    let due = now_ms() + 3_600_000;

    {
        let rt = start(&root, &d, None, limits(None)).await?;
        rt.submit(
            old,
            target("Obj", "hold", None, "k"),
            None,
            None,
            Bytes::new(),
        )
        .await?;
        wait_status(&rt, old, "old waits", is_suspended).await?;
        rt.shutdown().await?;
    }
    with_engine(&root, async |e: &mut Engine| {
        let len = e.journal_len(&old).await?;
        e.schedule_retry(&old, due, 1, len).await?;
        Ok(())
    })
    .await?;

    let rt = start(&root, &d, None, limits(None)).await?;
    let new = match rt.restart(old, 1, None).await? {
        cairn_engine::RestartOutcome::Restarted { new } => new,
        other => anyhow::bail!("restart: {other:?}"),
    };
    wait_status(&rt, old, "old finishes", is_completed).await?;
    wait_status(&rt, new, "new runs and waits", is_suspended).await?;
    rt.tick(due + 1_000).await?;
    // If the tick delivered anything to old, give it time to reach the SDK.
    tokio::time::sleep(Duration::from_millis(300)).await;
    hold_signal(&rt, new).await?;
    wait_status(&rt, new, "new completes", is_completed).await?;
    rt.shutdown().await?;
    ensure!(
        starts_for(&sdk, old) == 1,
        "SDK received a Start of old after restart: {}",
        starts_for(&sdk, old)
    );
    let left = with_engine(&root, async |e: &mut Engine| {
        Ok(e.pending_retry(&old).await?)
    })
    .await?;
    ensure!(left.is_none(), "old's retry row left after tick: {left:?}");
    Ok(())
}

// ───────────────────────────────────────────────────────────── suspicion E

/// **Suspicion E — `decide_retry` fires for `suspended`, while
/// `recover`/`decide_rescue` only look at `running`.** Intended: two different
/// questions.
///
/// - `recover` (and the rescue net) ask "does this invocation have work to run **now**
///   that nothing will wake": a `suspended` invocation waits for a durable event (timer,
///   outbox, inbound, promise) that the normal path delivers — re-running it on becoming
///   leader is a recovery storm.
/// - A retry row **is** a durable wake-up the invocation set for itself.
///   A `suspended` invocation that still has a retry row (a watch row left over because the
///   `end_turn` commit failed; a scheduled retry that `cancel_retry` missed after a failed
///   turn) — firing it is the safe side: one extra turn the SDK replays and suspends
///   again, better than dropping a durable wake-up.
///
/// Order:
/// 1. `P` waits for a signal. The leader steps down.
/// 2. `P` has a stray retry row (due far away).
/// 3. New leader: `recover` does **not** run `P`.
/// 4. A background tick after the due time: `fire_retries` runs **one** turn; `P` waits
///    again; the row is cancelled — the next tick fires nothing.
#[tokio::test]
async fn schedule_of_suspended_invocation_fires_once_but_recover_does_not() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object("Obj", "hold", vec![Step::Suspend, Step::output("\"held\"")])
        .spawn()
        .await?;
    let d = deployment_of(&sdk).await?;
    let root = fresh_root("lifecycle-e-suspended-retry")?;
    let p = id(1);
    let due = now_ms() + 3_600_000;

    {
        let rt = start(&root, &d, None, limits(None)).await?;
        rt.submit(
            p,
            target("Obj", "hold", None, "k"),
            None,
            None,
            Bytes::new(),
        )
        .await?;
        wait_status(&rt, p, "P waits", is_suspended).await?;
        rt.shutdown().await?;
    }
    with_engine(&root, async |e: &mut Engine| {
        let len = e.journal_len(&p).await?;
        e.schedule_retry(&p, due, 1, len).await?;
        Ok(())
    })
    .await?;

    let rt = start(&root, &d, None, limits(None)).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    ensure!(
        starts_for(&sdk, p) == 1,
        "recover ran a suspended invocation"
    );
    let fired = rt.tick(due + 1_000).await?;
    ensure!(fired == 1, "P's retry row must fire: {fired}");
    common::within(TIMEOUT, "turn from the retry row runs", async {
        while starts_for(&sdk, p) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    })
    .await?;
    wait_status(&rt, p, "P waits again", is_suspended).await?;
    let again = rt.tick(due + 3_600_000).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let n = starts_for(&sdk, p);
    rt.shutdown().await?;
    ensure!(again == 0, "retry row left after the turn: fired {again}");
    ensure!(n == 2, "SDK received {n} Start for P (want 2)");
    Ok(())
}

// ─────────────────────────────── the single-meaning attempt rule

/// The `ctx.attempt` numbers (`Start.retry_count_since_last_stored_entry`) the fake SDK
/// received for `id`, in order.
fn attempts_for(sdk: &FakeSdk, id: [u8; 24]) -> Vec<u32> {
    sdk.starts()
        .iter()
        .filter(|s| s.id.as_ref() == id.as_slice())
        .map(|s| s.attempt)
        .collect()
}

/// **The reverse of suspicion B** (core flag `uncounted_watch`) —
/// a turn without a pre-scheduled retry enters `Running` then dies: the attempt that called
/// the SDK is not counted.
///
/// Interleaving:
/// 1. `Work/run` stalls 30 s then completes. `/send` `id(1)`: the first turn (old owner
///    `Drive`) commits `Running` with the watch row then calls the SDK — the `Start` carries
///    `attempt = 0`.
/// 2. The leader steps down mid-turn (the runtime stops): the turn is cancelled at the SDK.
/// 3. New leader: `restore_flow` moves the entry out of `Running`, `recover` delivers
///    a background turn (the watch row is not a scheduled retry with a backoff step, so it
///    does not wait for it).
///
/// Must see (FL-19: "dying after the `Running` commit means the attempt counted"): the
/// second `Start` carries `attempt = 1`. Before the fix: 0 — the watch row carried `attempt` 0
/// and a turn without a pre-scheduled retry always ran with `ctx.attempt = 0`, so a handler
/// dying mid-turn (OOM, an infrastructure fault killing the process) never reached
/// `maxAttempts`.
#[tokio::test]
async fn turn_dying_after_running_still_counts_attempt() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service(
            "Work",
            "run",
            vec![Step::Stall(30_000), Step::output("\"done\"")],
        )
        .spawn()
        .await?;
    let d = deployment_of(&sdk).await?;
    let root = fresh_root("lifecycle-reverse-b-dead-turn")?;
    let work = target("Work", "run", None, "");

    {
        let rt = start(&root, &d, None, limits(None)).await?;
        rt.submit(id(1), work.clone(), None, None, Bytes::from_static(b"{}"))
            .await?;
        common::within(TIMEOUT, "first turn reaches the SDK", async {
            while starts_for(&sdk, id(1)) < 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok(())
        })
        .await?;
        rt.shutdown().await?;
    }

    let rt = start(&root, &d, None, limits(None)).await?;
    common::within(TIMEOUT, "new leader's turn reaches SDK", async {
        while starts_for(&sdk, id(1)) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    })
    .await?;
    let attempts = attempts_for(&sdk, id(1));
    rt.shutdown().await?;
    ensure!(
        attempts == [0, 1],
        "ctx.attempt of the two turns: {attempts:?} — a turn that died after `Running` must be counted"
    );
    Ok(())
}

/// Handler `Nap/once` for [`preset_schedule_recounted_when_journal_grows`]:
/// the first turn issues a `ctx.sleep` (due `wake`) then fails retryably — the sleep
/// command is in the journal, the invocation has a retry row **and** a timer. A turn that
/// sees the sleep's completion completes; a turn that does not fails retryably again.
#[derive(Clone)]
struct NapOnce {
    starts: Arc<std::sync::Mutex<Vec<u32>>>,
    wake: u64,
}

async fn nap_once(State(st): State<NapOnce>, body: Bytes) -> (StatusCode, HeaderMap, Bytes) {
    let frames = decode_stream(body).expect("the runtime sends valid frames");
    let mut journaled = 0usize;
    let mut woke = false;
    for f in &frames {
        match cairn_protocol::decode(f).expect("frame decodes") {
            Message::Start(s) => st
                .starts
                .lock()
                .unwrap()
                .push(s.retry_count_since_last_stored_entry),
            Message::Input(_) => {}
            Message::SleepCompletion(_) => woke = true,
            m if m.kind() == cairn_protocol::MessageKind::Command => journaled += 1,
            _ => {}
        }
    }
    let mut out = Vec::new();
    if journaled == 0 {
        out.push(Message::Sleep(pb::SleepCommandMessage {
            wake_up_time: st.wake,
            result_completion_id: 1,
            name: String::new(),
        }));
    }
    out.push(if woke {
        Message::Output(pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from_static(b"\"done\"")),
            )),
            ..Default::default()
        })
    } else {
        Message::Error(pb::ErrorMessage {
            code: 500,
            message: "transient failure".into(),
            behavior: pb::ErrorBehavior::Retry as i32,
            ..Default::default()
        })
    });
    if woke {
        out.push(Message::End(pb::EndMessage {}));
    }
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, INVOCATION_V2.parse().unwrap());
    (StatusCode::OK, headers, encode_stream(&out))
}

async fn spawn_nap_once(st: NapOnce) -> Result<Deployment> {
    let app = Router::new()
        .route("/invoke/{service}/{handler}", post(nap_once))
        .with_state(st);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(Deployment {
        id: "dp_nap_once".into(),
        tenant: "default".into(),
        created_at_ms: 0,
        endpoint: format!("http://{addr}"),
        wire_version: WIRE_MAX,
        protocol_mode: ProtocolMode::RequestResponse,
        request_identity: None,
        rr_compression: None,
        replay_cache: false,
        transport: None,
        services: vec![ServiceManifest {
            name: "Nap".into(),
            ty: ServiceType::Service,
            handlers: vec![HandlerManifest {
                name: "once".into(),
                ty: None,
                contract: Default::default(),
            }],
            contract: Default::default(),
        }],
    })
}

/// **A pre-scheduled retry keeps the old journal position** (found by M4; core flag
/// `planned_stale_position`).
///
/// Interleaving:
/// 1. `Nap/once` first turn: `ctx.sleep` (due `wake`) goes into the journal, then fails
///    retryably — a retry row with `attempt 1` at journal length `L`.
/// 2. Two ticks sent together, `t1` (after the retry's due time, before `wake`) then `t2`
///    (after `wake`). Tick `t1`: `fire_retries` sets the next scheduled retry and delivers a
///    turn carrying `attempt 1`, position `L` — its task waits for the engine **after** tick `t2`.
/// 3. Tick `t2`: the timer is due, the lock is free, a turn is already delivered (FL-20) ⇒
///    write the completion, deliver nothing more — the journal grows to `L + 1`.
/// 4. The delivered turn runs: the journal is longer than when the row fired.
///
/// Must see: that turn is the **first** attempt at the new position — `ctx.attempt = 0`
/// ("since last stored entry"). Before the fix: `attempt = 1` at the old position — an attempt
/// at the new position was counted with the old position's number (`maxAttempts` hit early).
#[tokio::test]
async fn preset_schedule_recounted_when_journal_grows() -> Result<()> {
    let base = now_ms();
    let st = NapOnce {
        starts: Arc::default(),
        wake: base + 3_600_000,
    };
    let d = spawn_nap_once(st.clone()).await?;
    let root = fresh_root("lifecycle-planned-stale-position")?;
    let rt = start(&root, &d, None, limits(None)).await?;
    let starts = || st.starts.lock().unwrap().clone();

    rt.submit(
        id(1),
        target("Nap", "once", None, ""),
        None,
        None,
        Bytes::new(),
    )
    .await?;
    common::within(TIMEOUT, "first turn fails retryably", async {
        while starts().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    })
    .await?;
    // The first turn ends (commits the retry row) before the ticks.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let (a, b) = tokio::join!(rt.tick(base + 60_000), rt.tick(base + 7_200_000));
    let (a, b) = (a?, b?);
    common::within(TIMEOUT, "delivered turn runs", async {
        while starts().len() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    })
    .await?;
    wait_status(&rt, id(1), "completes", is_completed).await?;
    let seen = starts();
    rt.shutdown().await?;
    ensure!(
        a == 1 && b >= 1 && seen.len() == 2,
        "interleaving not as built: tick t1 {a}, tick t2 {b}, Start {seen:?}"
    );
    ensure!(
        seen == [0, 0],
        "ctx.attempt: {seen:?} — a turn running at the new journal position must be the first attempt"
    );
    Ok(())
}

/// **A preflight `RetrySoon` kept the counted attempt** (found by reading the code; core
/// flag `retry_soon_counts`).
///
/// Order:
/// 1. The handler declares `retryPolicyMaxAttempts = 2`. `id(1)` is queued (rule
///    `acme = 0`, the turn waits for a slot); the leader steps down.
/// 2. The new leader has **not loaded** the deployment `id(1)` is bound to: `recover` delivers
///    a turn, the turn enters `Running` then preflight sees `NotLoaded` ⇒ reschedules after 1 s,
///    the SDK is not called. Three more background ticks, one such turn each.
/// 3. Load the deployment, background tick.
///
/// Must see: `completed` after exactly **one** SDK call. Before the fix: every
/// `RetrySoon` turn spent an attempt (the `Running` commit counted, preflight did not give it
/// back) ⇒ `paused` 575 with the SDK never called.
#[tokio::test]
async fn unloaded_deployment_does_not_spend_attempt() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Work", "run", vec![Step::output("\"done\"")])
        .contract(
            "Work",
            "run",
            Contract {
                retry_policy_max_attempts: Some(2),
                ..Default::default()
            },
        )
        .spawn()
        .await?;
    let d = deployment_of(&sdk).await?;
    let root = fresh_root("lifecycle-retry-soon-counts")?;
    let work = target("Work", "run", Some("acme"), "");

    {
        let rt = start(&root, &d, Some(rules(&[("acme", 0)], 1)), limits(None)).await?;
        rt.submit(id(1), work.clone(), None, None, Bytes::from_static(b"{}"))
            .await?;
        rt.shutdown().await?;
    }

    // Empty registry: the deployment `id(1)` is bound to is not loaded on this node.
    let registry = Registry::new();
    let rt = spawn_runtime(&root, registry.clone(), limits(None))?;
    rt.deployments_changed().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let base = now_ms();
    for k in 1..=3u64 {
        rt.tick(base + k * 86_400_000).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    ensure!(
        sdk.starts().is_empty(),
        "the SDK was called while the deployment was not loaded"
    );

    registry.upsert(d.clone());
    rt.deployments_changed().await?;
    let deadline = std::time::Instant::now() + TIMEOUT;
    let mut k = 4;
    let status = loop {
        let s = rt.status(id(1)).await?;
        if let Some(s) = s.filter(|s| !matches!(s, InvocationStatus::Running)) {
            break s;
        }
        ensure!(
            std::time::Instant::now() < deadline,
            "id(1) not stopped after {TIMEOUT:?}"
        );
        rt.tick(base + k * 86_400_000).await?;
        k += 1;
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    rt.shutdown().await?;
    let calls = sdk.starts_of("Work", "run").len();
    ensure!(
        is_completed(&status) && calls == 1,
        "a `RetrySoon` turn spent an attempt: {status:?}, SDK called {calls} times"
    );
    Ok(())
}

/// **A retry scheduled after an error ignored the handler's policy** (found by reading the
/// code; core flag `policy_ignored`).
///
/// Order:
/// 1. Handler `Work/run` declares `retryPolicyInitialInterval = 30 000` ms,
///    an endpoint nobody listens on: the turn enters `Running` then the SDK call fails
///    (infrastructure error, the `Err` branch — `retry_after_error`).
/// 2. Read the retry row of `id(1)`.
///
/// Must see: the scheduled due time ≥ the failure time + 30 s (the policy's first step).
/// Before the fix: `backoff_ms(0)` = 100 ms — the handler declared a backoff yet
/// infrastructure errors retried in a rush per the default table.
#[tokio::test]
async fn retry_after_error_follows_handler_policy() -> Result<()> {
    let dead = std::net::TcpListener::bind("127.0.0.1:0")?;
    let endpoint = format!("http://{}", dead.local_addr()?);
    drop(dead);
    let d = Deployment {
        id: "dp_policy".into(),
        tenant: "default".into(),
        created_at_ms: 0,
        endpoint,
        wire_version: WIRE_MAX,
        protocol_mode: ProtocolMode::RequestResponse,
        request_identity: None,
        rr_compression: None,
        replay_cache: false,
        transport: None,
        services: vec![ServiceManifest {
            name: "Work".into(),
            ty: ServiceType::Service,
            handlers: vec![HandlerManifest {
                name: "run".into(),
                ty: None,
                contract: Contract {
                    retry_policy_initial_interval: Some(30_000),
                    ..Default::default()
                },
            }],
            contract: Default::default(),
        }],
    };
    let root = fresh_root("lifecycle-policy-after-error")?;
    let rt = start(&root, &d, None, limits(None)).await?;
    let before = now_ms();
    rt.submit(
        id(1),
        target("Work", "run", None, ""),
        None,
        None,
        Bytes::new(),
    )
    .await?;
    // The turn fails at the SDK call then reschedules (no background tick runs in the test).
    tokio::time::sleep(Duration::from_millis(800)).await;
    rt.shutdown().await?;
    let row = with_engine(&root, async |e: &mut Engine| {
        Ok(e.retry_row(&id(1)).await?)
    })
    .await?;
    let row = row.ok_or_else(|| anyhow!("no retry row after the failed turn"))?;
    ensure!(
        !row.is_watch() && row.due >= before + 30_000,
        "retry after error: {row:?} (failed at ≥ {before}) — must follow retryPolicyInitialInterval"
    );
    Ok(())
}

// ─────────────────────────────────── one "busy" question

/// Handler `Waiter/wait` for [`drainer_does_not_redeliver_delivered_turn`]: the first turn
/// stalls `stall_ms` (holding the invocation lock) then waits for the signal
/// [`HOLD_SIGNAL`]; a turn that sees that signal goes on to wait for a signal nobody
/// sends — the invocation stays alive, so an extra turn after that **reaches the SDK**.
#[derive(Clone)]
struct Waiter {
    starts: Arc<AtomicUsize>,
    stall_ms: u64,
}

async fn waiter(State(st): State<Waiter>, body: Bytes) -> (StatusCode, HeaderMap, Bytes) {
    let frames = decode_stream(body).expect("the runtime sends valid frames");
    let mut signalled = false;
    for f in &frames {
        match cairn_protocol::decode(f).expect("frame decodes") {
            Message::Start(_) => {
                st.starts.fetch_add(1, Ordering::SeqCst);
            }
            Message::Signal(sig) => {
                if matches!(
                    &sig.signal_id,
                    Some(pb::signal_notification_message::SignalId::Name(n)) if n == HOLD_SIGNAL
                ) {
                    signalled = true;
                }
            }
            _ => {}
        }
    }
    if !signalled {
        tokio::time::sleep(Duration::from_millis(st.stall_ms)).await;
    }
    let out = vec![Message::Suspension(pb::SuspensionMessage {
        waiting_named_signals: vec![if signalled {
            "nobody-sent".into()
        } else {
            HOLD_SIGNAL.into()
        }],
        ..Default::default()
    })];
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, INVOCATION_V2.parse().unwrap());
    (StatusCode::OK, headers, encode_stream(&out))
}

async fn spawn_waiter(st: Waiter) -> Result<Deployment> {
    let app = Router::new()
        .route("/invoke/{service}/{handler}", post(waiter))
        .with_state(st);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(Deployment {
        id: "dp_waiter".into(),
        tenant: "default".into(),
        created_at_ms: 0,
        endpoint: format!("http://{addr}"),
        wire_version: WIRE_MAX,
        protocol_mode: ProtocolMode::RequestResponse,
        request_identity: None,
        rr_compression: None,
        replay_cache: false,
        transport: None,
        services: vec![ServiceManifest {
            name: "Waiter".into(),
            ty: ServiceType::Service,
            handlers: vec![HandlerManifest {
                name: "wait".into(),
                ty: None,
                contract: Default::default(),
            }],
            contract: Default::default(),
        }],
    })
}

/// **The drainer delivers a duplicate turn** (same shape as suspicion A; core flag
/// `drain_ignores_queued`) — the drainer's three "write then deliver" places
/// (`deliver_to_caller`, `settle_signal_local`, `deliver_inbound_completion`)
/// delivered a turn without asking `Runner::is_queued`.
///
/// Interleaving:
/// 1. `W` (`Waiter/wait`) runs its first turn and stalls at the SDK for 1.5 s — holding its lock.
/// 2. Two invocations `S1`, `S2` (`Send/go`) each send a `ctx.signal` to `W`
///    then complete: two signal rows await the drainer. The drainer finds `W` busy ⇒ leaves
///    both, adds `W` to its list of rows waiting for a lock.
/// 3. `W`'s first turn waits for the signal; releasing the lock ⇒ nudges the drainer.
/// 4. One drain pass: `S1`'s row — the lock is free ⇒ write the signal, deliver a turn (its
///    task waits for the engine, not yet holding the lock); `S2`'s row — the lock is still free ⇒
///    write the signal, and before the fix **delivered another** turn.
///
/// Must see: the SDK receives exactly **2** `Start`s for `W` (the first turn + one turn carrying
/// both signals). Before the fix: the duplicate turn ran afterwards, `W` was still waiting so
/// the gate let it through, and the SDK was called a needless 3rd time.
#[tokio::test]
async fn drainer_does_not_redeliver_delivered_turn() -> Result<()> {
    let st = Waiter {
        starts: Arc::new(AtomicUsize::new(0)),
        stall_ms: 1_500,
    };
    let w = id(1);
    let cell = Arc::new(std::sync::Mutex::new(hex_of(w)));
    let senders = FakeSdk::builder()
        .service(
            "Send",
            "go",
            vec![Step::SendSignal(cell.clone()), Step::output("\"sent\"")],
        )
        .spawn()
        .await?;
    let registry = Registry::new();
    registry.upsert(spawn_waiter(st.clone()).await?);
    let mut sd = cairn_server::discovery::discover(
        &cairn_server::egress::TenantHttp::h1(Default::default()),
        &senders.url,
        None,
    )
    .await
    .map_err(|e| anyhow!("discover fake SDK: {e:?}"))?;
    sd.id = "dp_senders".into();
    registry.upsert(sd);
    let root = fresh_root("lifecycle-drain-duplicate-turn")?;
    let rt = spawn_runtime(&root, registry, limits(None))?;
    rt.deployments_changed().await?;
    let starts = || st.starts.load(Ordering::SeqCst);

    rt.submit(
        w,
        target("Waiter", "wait", None, ""),
        None,
        None,
        Bytes::new(),
    )
    .await?;
    common::within(TIMEOUT, "W's first turn reaches the SDK", async {
        while starts() < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    })
    .await?;
    for n in [2u8, 3] {
        rt.submit(
            id(n),
            target("Send", "go", None, ""),
            None,
            None,
            Bytes::new(),
        )
        .await?;
    }
    for n in [2u8, 3] {
        wait_status(&rt, id(n), "sender completes", is_completed).await?;
    }
    common::within(TIMEOUT, "W's turn carrying the signals", async {
        while starts() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    })
    .await?;
    wait_status(&rt, w, "W waits again", is_suspended).await?;
    // A duplicate turn (if any) runs right after; give it time to reach the SDK.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let n = starts();
    rt.shutdown().await?;
    ensure!(
        n == 2,
        "SDK received {n} Start for W — an extra turn from the drainer (delivered while a turn had not run)"
    );
    Ok(())
}

fn hex_of(id: [u8; 24]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

// ─────────────────────────────── finishing in one commit

/// **Suspicion D (2) — `restart` left the old run's retry row alive past the commit that
/// finished it** (core flag `d2_restart_keeps_row`; harmless — the background tick cancels
/// the row when it fires — but a `completed` + retry row pair outside the valid set).
///
/// Order:
/// 1. `old` waits holding the VO lock and has a durable scheduled retry row (due far away —
///    built with `schedule_retry`, like an earlier failure).
/// 2. `restart` `old`. The leader steps down **at once**, no background tick runs.
///
/// Must see: `old` is `completed` and has **no** retry row left — the same commit
/// finishes it. Before the fix: the row stayed until the background tick fired it.
#[tokio::test]
async fn restart_deletes_old_versions_retry_row_in_same_commit() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object("Obj", "hold", vec![Step::Suspend, Step::output("\"held\"")])
        .spawn()
        .await?;
    let d = deployment_of(&sdk).await?;
    let root = fresh_root("lifecycle-d2-restart-drops-row")?;
    let old = id(1);
    let due = now_ms() + 3_600_000;

    {
        let rt = start(&root, &d, None, limits(None)).await?;
        rt.submit(
            old,
            target("Obj", "hold", None, "k"),
            None,
            None,
            Bytes::new(),
        )
        .await?;
        wait_status(&rt, old, "old waits", is_suspended).await?;
        rt.shutdown().await?;
    }
    with_engine(&root, async |e: &mut Engine| {
        let len = e.journal_len(&old).await?;
        e.schedule_retry(&old, due, 1, len).await?;
        Ok(())
    })
    .await?;

    let rt = start(&root, &d, None, limits(None)).await?;
    ensure!(
        matches!(
            rt.restart(old, 1, None).await?,
            cairn_engine::RestartOutcome::Restarted { .. }
        ),
        "restart did not happen"
    );
    rt.shutdown().await?;
    let (status, row) = with_engine(&root, async |e: &mut Engine| {
        Ok((e.status(&old).await?, e.retry_row(&old).await?))
    })
    .await?;
    ensure!(
        status.as_ref().is_some_and(is_completed),
        "old must be completed: {status:?}"
    );
    ensure!(
        row.is_none(),
        "old's retry row still there after the restart commit: {row:?}"
    );
    Ok(())
}

// ───────────────────────────────────────────── preflight NotFound

/// **Preflight `NotFound`**: when no deployment declares the service, the invocation
/// ends `failed` 404 in one commit — status, leaving the queue, deleting the retry row —
/// matching the 404 answer the caller gets.
///
/// Order:
/// 1. The runtime has a deployment declaring `Obj`; submit an invocation to `Ghost/run`
///    (nobody declares it — the invocation is created bound to no deployment).
/// 2. First turn: preflight finds no deployment ⇒ `NotFound`.
///
/// Must see: status `failed` 404 within the limit (before: it stayed `running`, the watch
/// row stayed and fired again after ~65 s, or it got stuck with nothing to wake it); after
/// that no queue entry, no retry row; the SDK never receives a `Start`.
#[tokio::test]
async fn preflight_notfound_ends_failed_404_in_one_commit() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object("Obj", "hold", vec![Step::output("\"x\"")])
        .spawn()
        .await?;
    let d = deployment_of(&sdk).await?;
    let root = fresh_root("lifecycle-f41-notfound")?;
    let ghost = id(41);
    {
        let rt = start(&root, &d, None, limits(None)).await?;
        rt.submit(
            ghost,
            target("Ghost", "run", None, ""),
            None,
            None,
            Bytes::new(),
        )
        .await?;
        let s = wait_status(&rt, ghost, "failed 404", |s| {
            matches!(s, InvocationStatus::Failed { code: 404, .. })
        })
        .await?;
        let InvocationStatus::Failed { message, .. } = s else {
            unreachable!()
        };
        ensure!(
            message.contains("Ghost"),
            "the 404 message must name the service: {message}"
        );
        rt.shutdown().await?;
    }
    let (row, slot) = with_engine(&root, async |e: &mut Engine| {
        Ok((e.retry_row(&ghost).await?, e.vqueue_of(&ghost).await?))
    })
    .await?;
    ensure!(row.is_none(), "retry row left after 404: {row:?}");
    ensure!(slot.is_none(), "queue entry left after 404: {slot:?}");
    ensure!(
        starts_for(&sdk, ghost) == 0,
        "SDK received a Start for a service nobody declares"
    );
    Ok(())
}

// ───────────────────────────────────────── which commit a failing step goes through

/// **Pinned: which commit a failing step goes through.** An attempt that
/// fails retryably (`ErrorMessage` behavior `RETRY`) ends with
/// **one** shell commit — `life::commit_noting` → `Engine::apply_lifecycle_noting`
/// — carrying the attempt's error (`LastFailure`) **and** the scheduled retry row.
/// This is the commit that the not-yet-durable `$log`/`$trace` records of that attempt
/// ride on (AP-14): it always exists, even when the attempt writes no journal
/// entry.
///
/// 1. `Svc/flaky` fails on every attempt below 100 (`FailBelow(100)`).
/// 2. Wait for the SDK to receive its first `Start`, then stop the runtime (the next attempt is
///    far away: the first backoff of the default policy).
///
/// Must see: the journal has only entry 0 (the attempt adds no command); there is a
/// `LastFailure` (attempt 0) **and** a retry row — both in the durable state
/// after the failed turn.
#[tokio::test]
async fn failing_attempt_ends_with_a_commit_carrying_failure_and_retry_row() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service(
            "Svc",
            "flaky",
            vec![Step::FailBelow(100), Step::output("\"ok\"")],
        )
        .spawn()
        .await?;
    let d = deployment_of(&sdk).await?;
    let root = fresh_root("agent-0-9-failing-step")?;
    let a = id(9);
    {
        let rt = start(&root, &d, None, limits(None)).await?;
        rt.submit(
            a,
            target("Svc", "flaky", None, ""),
            None,
            None,
            Bytes::new(),
        )
        .await?;
        let deadline = std::time::Instant::now() + TIMEOUT;
        while starts_for(&sdk, a) == 0 {
            ensure!(std::time::Instant::now() < deadline, "no Start at the SDK");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // The commit ending the failed turn runs right after the SDK returns the error.
        let deadline = std::time::Instant::now() + TIMEOUT;
        loop {
            if rt.status(a).await?.is_some() && starts_for(&sdk, a) >= 1 {
                tokio::time::sleep(Duration::from_millis(200)).await;
                break;
            }
            ensure!(
                std::time::Instant::now() < deadline,
                "invocation status not found"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        rt.shutdown().await?;
    }
    let (len, failure, retry) = with_engine(&root, async |e: &mut Engine| {
        Ok((
            e.journal_len(&a).await?,
            e.last_failure(&a).await?,
            e.pending_retry(&a).await?,
        ))
    })
    .await?;
    ensure!(len == 1, "the failed attempt wrote journal commands: {len}");
    let failure = failure.ok_or_else(|| anyhow!("no LastFailure after the failed attempt"))?;
    ensure!(failure.attempt == 0, "error of attempt 0: {failure:?}");
    ensure!(retry.is_some(), "no retry row after the failed attempt");
    Ok(())
}

// ──────────────────────── a timer limit per tick

/// **One tick delivers at most the timer limit; the rest go in later ticks, by due time,
/// none missed.** Before the fix `due_timers` had no limit: N timers due at once
/// (cron fan-out, a backlog after the store or the leader was away) meant holding the
/// engine for O(N).
///
/// 1. The runtime is stopped; write `N` = 600 orphan timers directly (the invocations have
///    no status row ⇒ the core chooses `Discard`, the tick deletes the row), due `base + i`.
/// 2. Reopen the runtime (empty deployment registry ⇒ no background tick); one tick at
///    `base + N`; stop the runtime and count the remaining timer index rows.
/// 3. Repeat step 2 until no rows are left.
///
/// Must see: after tick `k` exactly `N − 256·k` rows remain, and they are the rows with the
/// **latest** due times (delivered in due order); no rows left after `⌈N / 256⌉` ticks.
/// On the old code: the first tick deleted all 600.
#[tokio::test]
async fn tick_delivers_at_most_the_cap_per_hold_in_due_order() -> Result<()> {
    use cairn_store::keys::{self as keyspace, Index};
    const N: usize = 600;
    const CAP: usize = 256;
    let root = fresh_root("lifecycle-timer-cap")?;
    let base = now_ms() + 3_600_000;
    let tid = |i: usize| {
        let mut b = [0u8; 24];
        b[13..16].copy_from_slice(&(i as u32).to_be_bytes()[1..]);
        b
    };
    with_engine(&root, async |e: &mut Engine| {
        let mut muts = cairn_store::Mutations::new();
        for i in 0..N {
            muts.add_timer(base + i as u64, &tid(i), 1, &[]);
        }
        e.writer_mut().commit(muts).await?;
        Ok(())
    })
    .await?;
    let left = async |root: &Path| -> Result<Vec<u64>> {
        with_engine(root, async |e: &mut Engine| {
            Ok(e.writer()
                .rows_under(&keyspace::index_prefix(Index::Timer), false, usize::MAX)
                .await?
                .into_iter()
                .filter_map(|(k, _)| keyspace::parse_ix_timer(&k).map(|(due, ..)| due))
                .collect())
        })
        .await
    };
    ensure!(left(&root).await?.len() == N, "fewer than {N} timers");

    let now = base + N as u64;
    let mut ticks = 0;
    loop {
        let rt = spawn_runtime(&root, Registry::new(), limits(None))?;
        rt.tick(now).await?;
        rt.shutdown().await?;
        ticks += 1;
        let rest = left(&root).await?;
        let expect = N.saturating_sub(ticks * CAP);
        let want: Vec<u64> = (N - expect..N).map(|i| base + i as u64).collect();
        ensure!(
            rest == want,
            "after {ticks} ticks {} rows remain (want {expect}, exactly the latest due times)",
            rest.len()
        );
        if rest.is_empty() {
            break;
        }
        ensure!(ticks < N.div_ceil(CAP), "rows remain after {ticks} ticks");
    }
    ensure!(ticks == N.div_ceil(CAP), "no rows left after {ticks} ticks");
    Ok(())
}
