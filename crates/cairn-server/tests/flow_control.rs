// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Flow control end-to-end: concurrency limits, scope as identity, recovery.
//!
//! Four exit gates of the flow control design, each forcing something quite
//! different:
//!
//! 1. **The concurrency limit is real.** Rule `scope = 1` ⇒ two calls of the same
//!    scope are never inside the SDK at the same time. Without this test the whole
//!    design is just a counter table nobody reads.
//! 2. **Fairness.** A quiet tenant does not queue behind the whole burst of a noisy
//!    tenant.
//! 3. **Scope is identity.** Same object key, different scope ⇒ independent state,
//!    and **no locking each other** — two different virtual objects have no reason to
//!    wait for each other.
//! 4. **Recovery** (a hard gate). Killing the leader mid-turn ⇒ the new leader has
//!    counter = 0, and no invocation is lost or stuck.
//!
//! The fake SDK here **can block**: it reports "I am inside" and then waits for the
//! test to let it go. A fast SDK proves nothing about a concurrency limit — what has
//! to be measured is *how many turns are inside the SDK at once*, and that can only
//! be observed while they have not answered yet.

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
            EndpointManifest, HandlerManifest, HandlerType, ProtocolMode, ServiceManifest,
            ServiceType, DISCOVERY_V1, WIRE_MAX,
        },
        invoker::{decode_stream, encode_stream, INVOCATION_V2},
        router, Registry,
    },
    cairn_store::{
        LeadershipGrant, Limit, LimitKey, PartitionId, PartitionWriter, RuleBook, Scope, Stage,
    },
    slatedb::{
        config::Settings,
        object_store::{local::LocalFileSystem, ObjectStore},
        Db,
    },
    std::{
        collections::HashMap,
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
    tokio::sync::Semaphore,
};

/// Cap for every wait. Without it, a regression turns into a test suite that hangs
/// forever in CI — the hardest kind of failure to diagnose.
const TIMEOUT: Duration = Duration::from_secs(10);

/// A flow control snapshot **after** the term's VO lock rebuild has run to the end
/// (FL-8: the rebuild runs in chunks while the range serves commands — the first
/// command may arrive before it finishes).
async fn restored(rt: &EngineHandle) -> Result<cairn_server::flow::FlowSnapshot> {
    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        let flow = rt.flow().await?;
        if !flow.scheduler.restoring {
            return Ok(flow);
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "lock rebuild not finished after {TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

// ─────────────────────────────────────────────────── fake SDK that can block

#[derive(Clone)]
struct GateSdk {
    /// Number of turns **currently** inside the SDK. Incremented on entry, decremented
    /// on exit ⇒ it measures concurrency exactly, not total throughput.
    inside: Arc<AtomicUsize>,
    /// Peak of [`Self::inside`] — the number the concurrency limit has to cap.
    peak: Arc<AtomicUsize>,
    /// The order in which turns entered, by input. Used to measure fairness.
    order: Arc<Mutex<Vec<String>>>,
    /// The gate: each turn consumes a permit before it may answer.
    gate: Arc<Semaphore>,
    /// Every turn writes state `n` and returns the accumulated value.
    counting: bool,
    /// With `counting`: the **write** turn (the one that has read `n`) waits at the
    /// gate like a normal turn — to kill the leader exactly when a turn has read the
    /// state but not written it.
    hold_write: bool,
    /// Inputs whose **first** turn fails retryably (`ErrorMessage` behavior `RETRY`)
    /// at once, without waiting at the gate — the turn ends with a durable retry
    /// ([`retry_schedule_rows`]).
    fail_first: Arc<Mutex<std::collections::HashSet<String>>>,
    /// Gate and concurrency **per object**: an input of the form `"<object>-<name>"`
    /// whose `<object>` is here waits at that object's gate, and the object's
    /// `inside`/`peak` are counted separately.
    objects: Arc<Mutex<HashMap<String, ObjGate>>>,
    /// Inputs whose turn returns **one** `Output` frame larger than the 16 MiB frame
    /// cap — without waiting at the gate.
    oversized: Arc<Mutex<std::collections::HashSet<String>>>,
}

#[derive(Clone)]
struct ObjGate {
    gate: Arc<Semaphore>,
    inside: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl Default for ObjGate {
    fn default() -> Self {
        Self {
            gate: Arc::new(Semaphore::new(0)),
            inside: Arc::default(),
            peak: Arc::default(),
        }
    }
}

impl GateSdk {
    fn new() -> Self {
        Self {
            inside: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
            order: Arc::new(Mutex::new(Vec::new())),
            gate: Arc::new(Semaphore::new(0)),
            counting: false,
            hold_write: false,
            fail_first: Arc::default(),
            objects: Arc::default(),
            oversized: Arc::default(),
        }
    }

    /// The own gate of object `name` (created if missing).
    fn object(&self, name: &str) -> ObjGate {
        self.objects
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_default()
            .clone()
    }

    fn counting() -> Self {
        Self {
            counting: true,
            ..Self::new()
        }
    }

    fn counting_held() -> Self {
        Self {
            hold_write: true,
            ..Self::counting()
        }
    }

    /// Opens the gate without limit — for tests that do not need to block.
    fn open(self) -> Self {
        self.gate.add_permits(Semaphore::MAX_PERMITS);
        self
    }

    fn inside(&self) -> usize {
        self.inside.load(Ordering::SeqCst)
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    fn order(&self) -> Vec<String> {
        self.order.lock().unwrap().clone()
    }

    /// Forgets the old peak.
    ///
    /// Needed after killing the leader: the dead leader's HTTP requests are still
    /// inside the fake SDK (waiting at the gate), so its `inside` is **not** the
    /// concurrency of the new leader. Measuring the two together measures wrongly.
    fn reset_peak(&self) {
        self.peak.store(self.inside(), Ordering::SeqCst);
    }

    async fn wait_until_entered(&self, n: usize) -> Result<()> {
        wait_until(TIMEOUT, || self.inside() >= n)
            .await
            .map_err(|_| anyhow::anyhow!("only {}/{n} turns got into the SDK", self.inside()))
    }
}

async fn wait_until(budget: Duration, mut ok: impl FnMut() -> bool) -> Result<()> {
    tokio::time::timeout(budget, async {
        while !ok() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("waited {budget:?} in vain"))?;
    Ok(())
}

/// Waits until an invocation stops for good at `completed`.
///
/// An **async** predicate: asking for the status is a round trip through the actor
/// channel, and `block_in_place` cannot be used here (`#[tokio::test]` runs the
/// `current_thread` runtime). A dedicated function is far cheaper than generalizing
/// `wait_until`.
async fn wait_finished(rt: impl std::borrow::Borrow<EngineHandle>, id: [u8; 24]) -> Result<()> {
    let rt = rt.borrow();
    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        if matches!(rt.status(id).await?, Some(InvocationStatus::Completed(_))) {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            anyhow::bail!("invocation {} not finished after {TIMEOUT:?}", hex(&id));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn manifest() -> EndpointManifest {
    EndpointManifest {
        protocol_mode: ProtocolMode::RequestResponse,
        min_protocol_version: 1,
        max_protocol_version: WIRE_MAX,
        services: vec![
            // No object key: the concurrency limit must block *because of the counter*,
            // not because of the VO lock. Using a virtual object here would make the
            // test green for the wrong reason.
            ServiceManifest {
                name: "Work".into(),
                ty: ServiceType::Service,
                handlers: vec![HandlerManifest {
                    name: "run".into(),
                    ty: None,
                    contract: Default::default(),
                }],
                contract: Default::default(),
            },
            // With an object key: used for the "scope separates identity" part.
            ServiceManifest {
                name: "Cart".into(),
                ty: ServiceType::VirtualObject,
                handlers: vec![HandlerManifest {
                    name: "add".into(),
                    ty: Some(HandlerType::Exclusive),
                    contract: Default::default(),
                }],
                contract: Default::default(),
            },
        ],
    }
}

async fn sdk_discover() -> ([(header::HeaderName, &'static str); 1], String) {
    (
        [(header::CONTENT_TYPE, DISCOVERY_V1)],
        serde_json::to_string(&manifest()).unwrap(),
    )
}

fn reply(out: &[Message]) -> (StatusCode, HeaderMap, Bytes) {
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, INVOCATION_V2.parse().unwrap());
    (StatusCode::OK, headers, encode_stream(out))
}

/// Counts a turn leaving the fake SDK on drop — even when the handler is cancelled.
struct Leave(Arc<AtomicUsize>);

impl Drop for Leave {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// One turn: enter, wait at the gate, then `Output` + `End`.
async fn sdk_invoke(State(st): State<GateSdk>, body: Bytes) -> (StatusCode, HeaderMap, Bytes) {
    let frames = decode_stream(body).expect("runtime sends valid frames");
    let input = frames
        .iter()
        .find_map(|f| match cairn_protocol::decode(f) {
            Ok(Message::Input(i)) => i
                .value
                .map(|v| String::from_utf8_lossy(&v.content).into_owned()),
            _ => None,
        })
        .unwrap_or_default();

    // `counting`: accumulate into the object's state. A second turn is needed to read
    // and then write, so it is recognized by "is there a GetState completion yet".
    if st.counting {
        let got = frames.iter().find_map(|f| match cairn_protocol::decode(f) {
            Ok(Message::GetStateCompletion(g)) => Some(g),
            _ => None,
        });
        let Some(got) = got else {
            return reply(&[
                Message::GetState(pb::GetStateCommandMessage {
                    key: Bytes::from_static(b"n"),
                    result_completion_id: 1,
                    name: String::new(),
                }),
                Message::Suspension(pb::SuspensionMessage {
                    waiting_completions: vec![1],
                    ..Default::default()
                }),
            ]);
        };
        let cur: u64 = match got.result {
            Some(pb::get_state_completion_notification_message::Result::Value(v)) => {
                String::from_utf8_lossy(&v.content).parse().unwrap_or(0)
            }
            _ => 0,
        };
        let next = cur + input.parse::<u64>().unwrap_or(1);
        if st.hold_write {
            st.inside.fetch_add(1, Ordering::SeqCst);
            let leave = Leave(Arc::clone(&st.inside));
            let permit = st.gate.acquire().await.expect("gate not closed");
            permit.forget();
            drop(leave);
        }
        return reply(&[
            Message::SetState(pb::SetStateCommandMessage {
                key: Bytes::from_static(b"n"),
                value: Some(cairn_protocol::value::inline(Bytes::from(next.to_string()))),
                name: String::new(),
            }),
            Message::Output(pb::OutputCommandMessage {
                result: Some(pb::output_command_message::Result::Value(
                    cairn_protocol::value::inline(Bytes::from(next.to_string())),
                )),
                name: String::new(),
            }),
            Message::End(pb::EndMessage {}),
        ]);
    }

    if st.oversized.lock().unwrap().contains(&input) {
        st.order.lock().unwrap().push(input.clone());
        let big = Bytes::from(vec![
            b'x';
            cairn_protocol::DEFAULT_MAX_FRAME_LEN as usize + 1024
        ]);
        return reply(&[
            Message::Output(pb::OutputCommandMessage {
                result: Some(pb::output_command_message::Result::Value(
                    cairn_protocol::value::inline(big),
                )),
                name: String::new(),
            }),
            Message::End(pb::EndMessage {}),
        ]);
    }
    let now = st.inside.fetch_add(1, Ordering::SeqCst) + 1;
    st.peak.fetch_max(now, Ordering::SeqCst);
    st.order.lock().unwrap().push(input.clone());
    let obj = input
        .split_once('-')
        .and_then(|(o, _)| st.objects.lock().unwrap().get(o).cloned());
    if let Some(obj) = obj {
        let n = obj.inside.fetch_add(1, Ordering::SeqCst) + 1;
        obj.peak.fetch_max(n, Ordering::SeqCst);
        let _all = Leave(Arc::clone(&st.inside));
        let _mine = Leave(Arc::clone(&obj.inside));
        let permit = obj.gate.acquire().await.expect("gate not closed");
        permit.forget();
        drop((_all, _mine));
        return reply(&[
            Message::Output(pb::OutputCommandMessage {
                result: Some(pb::output_command_message::Result::Value(
                    cairn_protocol::value::inline(Bytes::from_static(b"done")),
                )),
                name: String::new(),
            }),
            Message::End(pb::EndMessage {}),
        ]);
    }
    if st.fail_first.lock().unwrap().remove(&input) {
        st.inside.fetch_sub(1, Ordering::SeqCst);
        return reply(&[Message::Error(pb::ErrorMessage {
            code: 500,
            message: "transient failure".into(),
            behavior: pb::ErrorBehavior::Retry as i32,
            ..Default::default()
        })]);
    }
    // Leaving the SDK = finished answering **or** cancelled: a leader stepping down
    // cancels turns in flight (and then only answers `Shutdown`), the client
    // disconnects, and axum drops the handler while it waits at the gate. Without
    // counting that exit `inside` stays stuck at the turn count of a dead leader.
    let leave = Leave(Arc::clone(&st.inside));
    let permit = st.gate.acquire().await.expect("gate not closed");
    permit.forget();
    drop(leave);

    reply(&[
        Message::Output(pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from_static(b"done")),
            )),
            name: String::new(),
        }),
        Message::End(pb::EndMessage {}),
    ])
}

async fn spawn_sdk(st: GateSdk) -> Result<String> {
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

// ───────────────────────────────────────────────────────────── runtime

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;
    Ok(root)
}

/// Opens an engine on `root`. Calling it again on the same `root` **simulates a
/// leader change**: the old store is intact, the RAM is not.
fn spawn_runtime(root: &Path, endpoint: String) -> Result<EngineHandle> {
    spawn_runtime_with(root, registry_for(endpoint), limits(None))
}

fn registry_for(endpoint: String) -> Registry {
    let registry = Registry::new();
    registry.upsert(deployment(endpoint));
    registry
}

fn deployment(endpoint: String) -> cairn_server::discovery::Deployment {
    cairn_server::discovery::Deployment {
        // Fixed id: a test that rebuilds the registry after a "failover" must find
        // again exactly the deployment the old invocations are pinned to.
        id: "dp_flowcontrol".into(),
        tenant: "default".into(),
        created_at_ms: 0,
        endpoint,
        wire_version: WIRE_MAX,
        protocol_mode: ProtocolMode::RequestResponse,
        request_identity: None,
        rr_compression: None,
        replay_cache: false,
        transport: None,
        services: manifest().services,
    }
}

/// The default limits of this file: no cap on waiting for the SDK (the tests keep
/// the SDK still on purpose) and no cap on waiting for a permit (what we measure is
/// *who gets to run*, not whether the system gives up at the right moment — that is
/// in `permit_wait_cap_...`).
fn limits(queue_wait: Option<Duration>) -> TurnLimits {
    TurnLimits {
        sdk_timeout: None,
        queue_wait,
        ..TurnLimits::default()
    }
}

fn spawn_runtime_with(root: &Path, registry: Registry, limits: TurnLimits) -> Result<EngineHandle> {
    spawn_runtime_stats(root, registry, limits, None)
}

/// [`spawn_runtime_with`] plus the writer's counters (`DbStats`: time and number of
/// engine holds per holder).
fn spawn_runtime_stats(
    root: &Path,
    registry: Registry,
    limits: TurnLimits,
    stats: Option<Arc<cairn_store::dbstats::DbStats>>,
) -> Result<EngineHandle> {
    let root = root.to_path_buf();
    EngineHandle::spawn_with_transport(
        Box::new(move || {
            let root = root.clone();
            let stats = stats.clone();
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
                    PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db)
                        .await?
                        .with_db_stats(stats);
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

fn rules(entries: &[(&str, u32)]) -> RuleBook {
    let mut b = RuleBook::default();
    for (p, n) in entries {
        b.set(p, Some(Limit::Max(*n)), None, None, 0).unwrap();
    }
    b.version = 1;
    b
}

fn work(scope: Option<&str>) -> Target {
    Target {
        tenant: Default::default(),
        scope: scope.map(|s| Scope::new(s).unwrap()),
        service: "Work".into(),
        handler: "run".into(),
        object_key: String::new(),
    }
}

fn cart(scope: Option<&str>, key: &str) -> Target {
    Target {
        tenant: Default::default(),
        scope: scope.map(|s| Scope::new(s).unwrap()),
        service: "Cart".into(),
        handler: "add".into(),
        object_key: key.into(),
    }
}

fn id(n: u8) -> [u8; 24] {
    let mut b = [0u8; 24];
    b[15] = n;
    b
}

// ───────────────────────────────────────────────────────────── the gates

/// **Gate 1.** A scope's concurrency limit is real: rule `acme = 2` ⇒ never more than
/// 2 turns inside the SDK at once, even though we fire 6 calls.
///
/// And it must **block because of the counter**, not because of a lock: the service
/// here has no object key, so there is no VO lock to confuse it with.
#[tokio::test]
async fn scope_concurrency_cap_is_real() -> Result<()> {
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-cap")?;
    let rt = spawn_runtime(&root, endpoint)?;
    rt.set_rules(rules(&[("acme", 2)])).await?;

    let mut chay = Vec::new();
    for i in 0..6u8 {
        let h = rt.clone();
        chay.push(tokio::spawn(async move {
            h.invoke(
                id(i),
                work(Some("acme")),
                None,
                None,
                Bytes::from(i.to_string()),
            )
            .await
        }));
    }

    // Two turns get in; the third must stay out.
    sdk.wait_until_entered(2).await?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        sdk.inside(),
        2,
        "limit acme=2 yet {} turns inside the SDK at once",
        sdk.inside()
    );

    // Open the gate: the peak never exceeds 2.
    //
    // The outcome of `/invoke` here is `Completed` (ran at once) or `Throttled`
    // (`acme`'s budget is exhausted), and **both are correct**: the `/invoke` gate
    // answers early if it has to queue — it does not hold an HTTP connection until its
    // turn — and then runs on behind the caller's back. What must hold is that *every*
    // invocation eventually finishes.
    sdk.gate.add_permits(6);
    for t in chay {
        let outcome = tokio::time::timeout(TIMEOUT, t).await???;
        assert!(
            matches!(
                outcome,
                InvokeOutcome::Completed(_) | InvokeOutcome::Throttled { .. }
            ),
            "{outcome:?}"
        );
    }
    for i in 0..6u8 {
        wait_finished(&rt, id(i)).await?;
    }
    assert_eq!(sdk.peak(), 2, "concurrency peak exceeded the limit");

    // And once the work is drained: no counter other than 0, no queue hanging.
    let flow = rt.flow().await?;
    assert!(flow.limits.is_empty(), "counter stuck after every turn");
    assert_eq!(flow.scheduler.turns_in_use, 0);
    assert_eq!(flow.scheduler.locks_held, 0);
    assert_eq!(flow.scheduler.waiting, 0);

    rt.shutdown().await?;
    Ok(())
}

/// The **unscoped** path is subject to no limit — a `*` rule does not touch it.
///
/// This is part of the flow control contract, and it breaks silently: a `*` rule
/// wrongly applied to unscoped invocations would throttle all existing traffic without
/// anyone being told.
#[tokio::test]
async fn without_scope_star_rule_does_not_apply() -> Result<()> {
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-no-scope")?;
    let rt = spawn_runtime(&root, endpoint)?;
    rt.set_rules(rules(&[("*", 1)])).await?;

    let mut chay = Vec::new();
    for i in 0..4u8 {
        let h = rt.clone();
        chay.push(tokio::spawn(async move {
            h.invoke(id(i), work(None), None, None, Bytes::from(i.to_string()))
                .await
        }));
    }
    sdk.wait_until_entered(4).await?;

    sdk.gate.add_permits(4);
    for t in chay {
        tokio::time::timeout(TIMEOUT, t).await???;
    }
    for i in 0..4u8 {
        wait_finished(&rt, id(i)).await?;
    }
    rt.shutdown().await?;
    Ok(())
}

/// **Gate 2 — fairness.** Two tenants, one sends a whole burst: the other tenant does
/// not queue behind that whole burst.
///
/// The partition's turn limit = 1, and neither tenant has **any rule** — so the only
/// arbiter is the round robin. Without it, the queue is one shared FIFO and the quiet
/// tenant waits until the very last turn.
#[tokio::test]
async fn noisy_tenant_does_not_starve_other_tenants() -> Result<()> {
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-fairness")?;
    // Turn limit = 1: without contention there is nothing to arbitrate, and the test
    // would go green without proving anything.
    let rt = spawn_runtime_with(
        &root,
        registry_for(endpoint),
        TurnLimits {
            max_concurrent_turns: 1,
            ..limits(None)
        },
    )?;

    const NOISY: u8 = 12;
    let mut chay = Vec::new();
    for i in 0..NOISY {
        let h = rt.clone();
        chay.push(tokio::spawn(async move {
            h.invoke(
                id(i),
                work(Some("onao")),
                None,
                None,
                Bytes::from(format!("onao-{i}")),
            )
            .await
        }));
    }
    // Wait for the noisy one to take its place first, then let the quiet tenant in —
    // that is the worst case, and the only case that proves anything.
    sdk.wait_until_entered(1).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let h = rt.clone();
    chay.push(tokio::spawn(async move {
        h.invoke(
            id(200),
            work(Some("imlang")),
            None,
            None,
            Bytes::from_static(b"imlang"),
        )
        .await
    }));
    // Give it time to register in the queue before we open the gate.
    wait_until(TIMEOUT, || !sdk.order().is_empty() && sdk.inside() >= 1).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    sdk.gate.add_permits(NOISY as usize + 1);
    for t in chay {
        tokio::time::timeout(TIMEOUT, t).await???;
    }
    for i in (0..NOISY).chain([200]) {
        wait_finished(&rt, id(i)).await?;
    }

    let order = sdk.order();
    let at = order
        .iter()
        .position(|s| s == "imlang")
        .expect("the quiet tenant must run");
    assert!(
        at <= 3,
        "the quiet tenant must get its turn early, not behind all {NOISY} calls of the noisy one: \
         entered at {at} in {order:?}"
    );
    rt.shutdown().await?;
    Ok(())
}

/// **Gate 3 — scope is identity.**
///
/// Same `Cart/u1`, two scopes: independent state, and **no locking each other**. Two
/// different virtual objects have no reason to wait for each other — if they did,
/// scope would be only a counting label, not a boundary.
#[tokio::test]
async fn same_object_key_different_scope_is_independent() -> Result<()> {
    let sdk = GateSdk::counting().open();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-scope-state")?;
    let rt = spawn_runtime(&root, endpoint)?;

    let mut totals = HashMap::new();
    for (n, (scope, amount)) in [
        (Some("bob"), 10u8),
        (Some("alice"), 3),
        (Some("bob"), 5),
        (None, 100),
    ]
    .into_iter()
    .enumerate()
    {
        let out = tokio::time::timeout(
            TIMEOUT,
            rt.invoke(
                id(n as u8),
                cart(scope, "u1"),
                None,
                None,
                Bytes::from(amount.to_string()),
            ),
        )
        .await??;
        let InvokeOutcome::Completed(v) = out else {
            anyhow::bail!("{out:?}");
        };
        totals.insert(
            scope.unwrap_or("<no scope>"),
            String::from_utf8(v.inline().map(|b| b.to_vec()).unwrap_or_default())?,
        );
    }

    assert_eq!(totals["bob"], "15", "bob's state must accumulate 10 + 5");
    assert_eq!(totals["alice"], "3", "alice sees bob's state");
    assert_eq!(
        totals["<no scope>"], "100",
        "the unscoped path sees a scope's state"
    );

    rt.shutdown().await?;
    Ok(())
}

/// Two scopes on the same object key **can run in parallel** — the VO lock is split
/// by scope, exactly as `QueueName::lock_name`.
#[tokio::test]
async fn two_scopes_on_same_object_key_do_not_lock_each_other() -> Result<()> {
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-scope-lock")?;
    let rt = spawn_runtime(&root, endpoint)?;

    let a = {
        let h = rt.clone();
        tokio::spawn(async move {
            h.invoke(
                id(1),
                cart(Some("bob"), "u1"),
                None,
                None,
                Bytes::from_static(b"a"),
            )
            .await
        })
    };
    let b = {
        let h = rt.clone();
        tokio::spawn(async move {
            h.invoke(
                id(2),
                cart(Some("alice"), "u1"),
                None,
                None,
                Bytes::from_static(b"b"),
            )
            .await
        })
    };
    // Both entered the SDK together ⇒ they do not lock each other.
    sdk.wait_until_entered(2).await?;

    // And a third invocation **with the same scope and key** must wait: the VO lock is
    // still intact within a scope.
    let c = {
        let h = rt.clone();
        tokio::spawn(async move {
            h.invoke(
                id(3),
                cart(Some("bob"), "u1"),
                None,
                None,
                Bytes::from_static(b"c"),
            )
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(sdk.inside(), 2, "third invocation slipped past the VO lock");

    sdk.gate.add_permits(3);
    for t in [a, b, c] {
        tokio::time::timeout(TIMEOUT, t).await???;
    }
    for i in 1..=3u8 {
        wait_finished(&rt, id(i)).await?;
    }
    rt.shutdown().await?;
    Ok(())
}

/// **Gate 4 — recovery, a hard gate.**
///
/// Kill the leader while several turns are running, open a new leader on the same store:
///
/// - the new counters **are 0** — the recovery invariant, and what keeps a counter
///   leak from accumulating across leader changes;
/// - no invocation is lost or stuck: everything in progress runs on to completion.
#[tokio::test]
async fn killing_leader_mid_turn_neither_leaks_nor_wedges() -> Result<()> {
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-recovery")?;

    const N: u8 = 6;
    let ids: Vec<[u8; 24]> = (0..N).map(id).collect();
    {
        let rt = spawn_runtime(&root, endpoint.clone())?;
        rt.set_rules(rules(&[("acme", 3)])).await?;
        for i in ids.iter().copied() {
            let h = rt.clone();
            // `submit`: answers at once, runs behind the caller's back — we need them
            // *running* when the leader dies, not their results.
            tokio::spawn(async move {
                let _ = h
                    .submit(i, work(Some("acme")), None, None, Bytes::from_static(b"x"))
                    .await;
            });
        }
        sdk.wait_until_entered(3).await?;
        // Exactly at the limit: three turns inside the SDK, three queued.
        let flow = rt.flow().await?;
        assert_eq!(flow.scheduler.turns_in_use, 3, "limit acme=3 not held");
        assert_eq!(
            flow.limits
                .iter()
                .find(|l| l.counter == "acme")
                .map(|l| l.used),
            Some(3),
            "the scope counter must count exactly the three running turns"
        );

        // Kill the leader **mid-turn**: do not open the gate, do not wait for anything.
        rt.shutdown().await?;
    }

    // The dead leader's three requests are still inside the fake SDK. Let them out
    // before measuring further — they belong to a process that no longer exists, and
    // counting them in the new leader's concurrency mixes two things up.
    sdk.gate.add_permits(3);
    wait_until(TIMEOUT, || sdk.inside() == 0).await?;
    sdk.reset_peak();

    // ── New leader on the same store, **with no deployment yet**.
    //
    // Empty deployment registry ⇒ `recover` has not run, so this is a snapshot of
    // exactly the moment "just became leader, has not taken work yet" — the only
    // place the recovery invariant can be observed.
    let registry = Registry::new();
    let rt = spawn_runtime_with(&root, registry.clone(), limits(None))?;
    let flow = restored(&rt).await?;
    assert!(
        flow.limits.is_empty(),
        "after becoming leader no counter may be non-zero — got {:?}",
        flow.limits
            .iter()
            .map(|l| (&l.counter, l.used))
            .collect::<Vec<_>>()
    );
    assert_eq!(flow.scheduler.turns_in_use, 0);
    assert_eq!(
        flow.scheduler.locks_held, 0,
        "a service without an object key has no VO lock to rebuild"
    );

    // ── Let the work run on: every invocation in progress must finish, none lost.
    sdk.gate.add_permits(6 * N as usize);
    rt.set_rules(rules(&[("acme", 3)])).await?;
    registry.upsert(deployment(endpoint));
    rt.deployments_changed().await?;
    for i in &ids {
        wait_finished(rt.clone(), *i).await?;
    }

    // ── And **new work** after the failover must not overwrite the slot of old work.
    //
    // `seq` is the position in the queue, and it is handed out by an in-RAM counter.
    // That counter must be re-seeded from the store on becoming leader; without that
    // the new leader hands out from 1 again — overwriting the entries of the
    // invocations just recovered above, and their index rows. Invocations lost, with
    // no error to see.
    let new_ids: Vec<[u8; 24]> = (100..100 + N).map(id).collect();
    for i in new_ids.iter().copied() {
        let h = rt.clone();
        tokio::spawn(async move {
            let _ = h
                .submit(i, work(Some("acme")), None, None, Bytes::from_static(b"y"))
                .await;
        });
    }
    for i in ids.iter().chain(new_ids.iter()) {
        wait_finished(rt.clone(), *i).await?;
    }

    // And the limit still holds **after** the failover: the peak never exceeds 3,
    // even in the rush right after becoming leader.
    //
    // `<=`, not `==`: recovery drives sequentially, so the *real* peak here is
    // usually 1. What this test enforces is "does not exceed", not "reaches exactly"
    // — enforcing equality would pin a detail of the recovery path, not a promise.
    assert!(
        sdk.peak() <= 3,
        "concurrency peak after failover is {} — above the limit acme=3",
        sdk.peak()
    );

    let flow = rt.flow().await?;
    assert!(flow.limits.is_empty(), "counter stuck after every turn");
    assert_eq!(flow.scheduler.waiting, 0);
    rt.shutdown().await?;
    Ok(())
}

/// Permit wait cap exhausted ⇒ `Throttled`, and the invocation **stays durable, stays
/// in the queue** — it just cannot run this turn yet.
#[tokio::test]
async fn permit_wait_cap_reschedules_instead_of_losing() -> Result<()> {
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-throttle")?;
    let rt = spawn_runtime_with(
        &root,
        registry_for(endpoint),
        limits(Some(Duration::from_millis(100))),
    )?;
    rt.set_rules(rules(&[("acme", 1)])).await?;

    // One turn takes the permit and stands still.
    let h = rt.clone();
    let first = tokio::spawn(async move {
        h.invoke(
            id(1),
            work(Some("acme")),
            None,
            None,
            Bytes::from_static(b"a"),
        )
        .await
    });
    sdk.wait_until_entered(1).await?;

    // The second one: `/send` answers as soon as it is **durable**, then runs behind the caller.
    let out = tokio::time::timeout(
        TIMEOUT,
        rt.submit(
            id(2),
            work(Some("acme")),
            None,
            None,
            Bytes::from_static(b"b"),
        ),
    )
    .await??;
    assert!(
        matches!(out, InvokeOutcome::Waiting),
        "/send answers right after it is durable: {out:?}"
    );

    // It still exists and **has not run** — the limit acme=1 is held by the first one.
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(matches!(
        rt.status(id(2)).await?,
        Some(InvocationStatus::Running)
    ));
    assert_eq!(sdk.inside(), 1, "the second slipped past the limit");

    // Open the gate: the first finishes, the second is woken and runs to completion
    // — **not lost**, even though its turn was dropped when the wait cap ran out.
    sdk.gate.add_permits(2);
    tokio::time::timeout(TIMEOUT, first).await???;
    wait_finished(&rt, id(2)).await?;
    rt.shutdown().await?;
    Ok(())
}

/// Changing a rule under load: raising the limit ⇒ waiters get to run, and **nobody
/// exceeds the limit** at any moment.
#[tokio::test]
async fn changing_rule_under_load() -> Result<()> {
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-change-rule")?;
    let rt = spawn_runtime(&root, endpoint)?;
    rt.set_rules(rules(&[("acme", 1)])).await?;

    let mut chay = Vec::new();
    for i in 0..4u8 {
        let h = rt.clone();
        chay.push(tokio::spawn(async move {
            h.invoke(
                id(i),
                work(Some("acme")),
                None,
                None,
                Bytes::from(i.to_string()),
            )
            .await
        }));
    }
    sdk.wait_until_entered(1).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(sdk.inside(), 1, "limit acme=1 not held");

    // Raise it to 3 ⇒ two waiters are woken at once, without anyone finishing.
    let mut b = rules(&[("acme", 3)]);
    b.version = 2;
    rt.set_rules(b).await?;
    sdk.wait_until_entered(3).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        sdk.inside(),
        3,
        "limit raised yet still only {} running",
        sdk.inside()
    );

    sdk.gate.add_permits(4);
    for t in chay {
        tokio::time::timeout(TIMEOUT, t).await???;
    }
    for i in 0..4u8 {
        wait_finished(&rt, id(i)).await?;
    }
    assert_eq!(sdk.peak(), 3, "limit 3 exceeded at some point");
    rt.shutdown().await?;
    Ok(())
}

/// A limit key subdivides the limit **within** a scope, and the effective limit is the
/// tightest one.
#[tokio::test]
async fn limit_key_subdivides_cap_within_scope() -> Result<()> {
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-limit-key")?;
    let rt = spawn_runtime(&root, endpoint)?;
    // Wide scope, narrow L1 level: two calls with the same `growth` must not run
    // together, but a `sales` call may.
    rt.set_rules(rules(&[("acme", 10), ("acme/growth", 1)]))
        .await?;

    let growth = LimitKey::parse("growth").unwrap();
    let sales = LimitKey::parse("sales").unwrap();

    let mut chay = Vec::new();
    for (n, lk) in [(1u8, &growth), (2, &growth), (3, &sales)] {
        let h = rt.clone();
        let lk = lk.clone();
        chay.push(tokio::spawn(async move {
            h.invoke(
                id(n),
                work(Some("acme")),
                Some(lk),
                None,
                Bytes::from(n.to_string()),
            )
            .await
        }));
    }
    // One `growth` + one `sales` run together; the second `growth` must wait.
    sdk.wait_until_entered(2).await?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(sdk.inside(), 2, "limit acme/growth=1 not held");

    sdk.gate.add_permits(3);
    for t in chay {
        tokio::time::timeout(TIMEOUT, t).await???;
    }
    for i in 1..=3u8 {
        wait_finished(&rt, id(i)).await?;
    }
    assert_eq!(sdk.peak(), 2);

    rt.shutdown().await?;
    Ok(())
}

fn hex(id: &[u8; 24]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

// ────────────────────────────────────────────── HTTP surface (admin API)

/// The `/scope/...` gate, `/rules`, `/limits`, `/vqueues`, `/scheduler` and `429`.
///
/// This is the only place that checks the **contract as seen from outside**: response
/// shapes, error codes, and the warnings that must be printed right where people type
/// the command. Lower-level tests prove the mechanism; this test proves users can
/// reach it.
#[tokio::test]
async fn flow_control_http_surface() -> Result<()> {
    use axum::http::StatusCode as S;

    let sdk = GateSdk::new().open();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-http")?;
    let handle = spawn_runtime(&root, endpoint.clone())?;

    let registry = Registry::new();
    registry.upsert(deployment(endpoint));
    // Rule book on an in-RAM object store: the same CAS mechanism, no disk needed.
    let rules = cairn_store::RuleStore::new(
        Arc::new(slatedb::object_store::memory::InMemory::new()),
        &slatedb::object_store::path::Path::default(),
    );
    let app = router(
        cairn_server::ingress::AppState::new(handle.clone(), registry).with_rules(Some(rules)),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let rt = format!("http://{addr}");
    let http = reqwest::Client::new();

    // ── The rule book is empty at first, and the version starts at 0.
    let r: serde_json::Value = http.get(format!("{rt}/rules")).send().await?.json().await?;
    assert_eq!(r["version"], 0);
    assert_eq!(r["rules"].as_array().unwrap().len(), 0);
    assert!(r["warning"].is_null(), "no `*` yet, so no warning");

    // ── Set a rule. `set` is idempotent and **merges**.
    let put = http
        .post(format!("{rt}/rules"))
        .json(&serde_json::json!({
            "pattern": "acme",
            "concurrency": 5,
            "description": "paying customer"
        }))
        .send()
        .await?;
    assert_eq!(put.status(), S::OK);
    let body: serde_json::Value = put.json().await?;
    assert_eq!(body["version"], 1);

    // Change only `disabled` — the description and the limit **must not be lost**.
    let body: serde_json::Value = http
        .post(format!("{rt}/rules"))
        .json(&serde_json::json!({ "pattern": "acme", "disabled": true }))
        .send()
        .await?
        .json()
        .await?;
    let rule = &body["rules"][0];
    assert_eq!(rule["concurrency"], "5", "limit lost on a disabled change");
    assert_eq!(rule["description"], "paying customer", "description lost");
    assert_eq!(rule["disabled"], true);
    assert_eq!(rule["level"], 1, "a 1-level rule hits the scope counter");

    // ── The `*` warning must be printed **right where people type the command**.
    let body: serde_json::Value = http
        .post(format!("{rt}/rules"))
        .json(&serde_json::json!({ "pattern": "*", "concurrency": 1000 }))
        .send()
        .await?
        .json()
        .await?;
    let warning = body["warning"].as_str().expect("a warning about `*`");
    assert!(
        warning.contains("NOT a shared pool"),
        "the warning does not name what people misunderstand: {warning}"
    );

    // ── `unlimited` is an explicit choice, quite different from "not set".
    let body: serde_json::Value = http
        .post(format!("{rt}/rules"))
        .json(&serde_json::json!({ "pattern": "acme/growth", "concurrency": "unlimited" }))
        .send()
        .await?
        .json()
        .await?;
    let r = body["rules"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["pattern"] == "acme/growth")
        .unwrap();
    assert_eq!(r["concurrency"], "unlimited");
    assert_eq!(r["level"], 2);

    // ── A broken pattern ⇒ 400, not 500.
    let bad = http
        .post(format!("{rt}/rules"))
        .json(&serde_json::json!({ "pattern": "a/b/c/d", "concurrency": 1 }))
        .send()
        .await?;
    assert_eq!(bad.status(), S::BAD_REQUEST);

    // ── Delete.
    assert_eq!(
        http.delete(format!("{rt}/rules/acme%2Fgrowth"))
            .send()
            .await?
            .status(),
        S::OK
    );
    assert_eq!(
        http.delete(format!("{rt}/rules/nonexistent"))
            .send()
            .await?
            .status(),
        S::NOT_FOUND,
        "deleting what does not exist is a 404, not a pretend 200"
    );

    // ── A limit key **without a scope** ⇒ 400. Silently ignoring it would let the
    //    operator believe they just set a limit.
    let e = http
        .post(format!("{rt}/invoke/Work/run?limit-key=growth"))
        .body("x")
        .send()
        .await?;
    assert_eq!(e.status(), S::BAD_REQUEST);
    let body: serde_json::Value = e.json().await?;
    assert!(
        body["error"].as_str().unwrap().contains("scope"),
        "the error must say why: {body}"
    );

    // ── A malformed scope ⇒ 400 right at the entry.
    assert_eq!(
        http.post(format!("{rt}/scope/has%20spaces/invoke/Work/run"))
            .body("x")
            .send()
            .await?
            .status(),
        S::BAD_REQUEST
    );

    // ── The scoped path runs, and returns an id that can be looked up.
    let inv: serde_json::Value = http
        .post(format!(
            "{rt}/scope/acme/invoke/Work/run?limit-key=growth/alice"
        ))
        .body("1")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(inv["status"], "completed", "{inv}");
    let id = inv["id"].as_str().unwrap();
    let st: serde_json::Value = http
        .get(format!("{rt}/invocations/{id}"))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(st["status"], "completed");

    // ── The three diagnostic endpoints answer, even when nothing is running.
    let limits: serde_json::Value = http
        .get(format!("{rt}/limits"))
        .send()
        .await?
        .json()
        .await?;
    assert!(limits.is_array(), "{limits}");
    let queues: serde_json::Value = http
        .get(format!("{rt}/vqueues"))
        .send()
        .await?
        .json()
        .await?;
    assert!(queues.is_array(), "{queues}");
    let sched: serde_json::Value = http
        .get(format!("{rt}/scheduler"))
        .send()
        .await?
        .json()
        .await?;
    let s = &sched[0];
    assert_eq!(s["partition"], 0);
    assert_eq!(s["turnsInUse"], 0);
    assert!(s["maxTurns"].as_u64().unwrap() >= 1);
    assert!(
        s["granted"].as_u64().unwrap() >= 1,
        "must count at least one granted permit: {sched}"
    );

    // ── Metrics have flow control lines.
    let text = http
        .get(format!("{rt}/metrics"))
        .send()
        .await?
        .text()
        .await?;
    for name in [
        "cairnflow_flow_granted_total",
        "cairnflow_flow_blocked_total",
        "cairnflow_flow_throttled_total",
        "cairnflow_invoke_throttled_total",
    ] {
        assert!(text.contains(name), "metric {name} missing");
    }

    handle.shutdown().await?;
    Ok(())
}

/// A tenant's budget exhausted ⇒ `429` with `Retry-After` **and** the id.
///
/// It differs from `202 waiting` in that it says *why*: the invocation has been
/// accepted, is durable, is in the queue — it just has no permit to run yet. Waiting
/// for an `EXCLUSIVE` lock does **not** give `429`: that is the normal serialization
/// of a virtual object, not a limit.
#[tokio::test]
async fn out_of_budget_is_429_waiting_for_lock_is_202() -> Result<()> {
    use axum::http::StatusCode as S;

    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-429")?;
    let handle = spawn_runtime(&root, endpoint.clone())?;
    handle.set_rules(rules(&[("acme", 1)])).await?;

    let registry = Registry::new();
    registry.upsert(deployment(endpoint));
    let app = router(cairn_server::ingress::AppState::new(
        handle.clone(),
        registry,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let rt = format!("http://{addr}");
    let http = reqwest::Client::new();

    // The first one takes `acme`'s only permit and stands still in the SDK.
    let one = {
        let (rt, http) = (rt.clone(), http.clone());
        tokio::spawn(async move {
            http.post(format!("{rt}/scope/acme/invoke/Work/run"))
                .body("1")
                .send()
                .await
        })
    };
    sdk.wait_until_entered(1).await?;

    // ── The second one with the same scope: budget exhausted ⇒ 429.
    let r = http
        .post(format!("{rt}/scope/acme/invoke/Work/run"))
        .body("2")
        .send()
        .await?;
    assert_eq!(r.status(), S::TOO_MANY_REQUESTS);
    let retry_after = r
        .headers()
        .get(axum::http::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .expect("429 must carry Retry-After — clients and load balancers read it");
    assert!(
        retry_after >= 1,
        "Retry-After = 0 turns the client into a busy loop"
    );
    let body: serde_json::Value = r.json().await?;
    assert_eq!(body["status"], "throttled");
    let queued_id = body["id"]
        .as_str()
        .expect("429 must still return an id to look up")
        .to_string();

    // It **still exists** — a 429 here is not "rejected" but "queued already, come
    // back later".
    let st = http
        .get(format!("{rt}/invocations/{queued_id}"))
        .send()
        .await?;
    assert_eq!(st.status(), S::OK);

    // ── Waiting for a **VO lock** is still 202: that is serialization, not a limit.
    let a = {
        let (rt, http) = (rt.clone(), http.clone());
        tokio::spawn(async move {
            http.post(format!("{rt}/invoke/Cart/add?key=u1"))
                .body("a")
                .send()
                .await
        })
    };
    sdk.wait_until_entered(2).await?;
    let b = http
        .post(format!("{rt}/invoke/Cart/add?key=u1"))
        .body("b")
        .send()
        .await?;
    assert_eq!(
        b.status(),
        S::ACCEPTED,
        "waiting for an EXCLUSIVE lock is not being throttled"
    );
    assert_eq!(b.json::<serde_json::Value>().await?["status"], "waiting");

    sdk.gate.add_permits(16);
    one.await??;
    a.await??;
    handle.shutdown().await?;
    Ok(())
}

/// `debug_id` carries the **scope prefix**.
///
/// `StartMessage` has no scope field — the SDK does not need to know. But someone
/// *reading the logs* does: a 48-character hex id does not tell which tenant this
/// invocation belongs to, and that is exactly the first question during a
/// multi-tenant incident.
#[tokio::test]
async fn debug_id_carries_scope_prefix() -> Result<()> {
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    // A minimal fake SDK: reads `debug_id` from `Start`, then returns `Output` at once.
    let st = seen.clone();
    let app = Router::new().route("/discover", get(sdk_discover)).route(
        "/invoke/{service}/{handler}",
        post(move |body: Bytes| {
            let st = st.clone();
            async move {
                let frames = decode_stream(body).expect("valid frames");
                if let Some(Message::Start(s)) =
                    frames.iter().find_map(|f| cairn_protocol::decode(f).ok())
                {
                    st.lock().unwrap().push(s.debug_id);
                }
                reply(&[
                    Message::Output(pb::OutputCommandMessage {
                        result: Some(pb::output_command_message::Result::Value(
                            cairn_protocol::value::inline(Bytes::from_static(b"ok")),
                        )),
                        name: String::new(),
                    }),
                    Message::End(pb::EndMessage {}),
                ])
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let root = fresh_root("flow-debug-id")?;
    let rt = spawn_runtime(&root, format!("http://{addr}"))?;
    tokio::time::timeout(
        TIMEOUT,
        rt.invoke(
            id(1),
            work(Some("acme")),
            None,
            None,
            Bytes::from_static(b"x"),
        ),
    )
    .await??;
    tokio::time::timeout(
        TIMEOUT,
        rt.invoke(id(2), work(None), None, None, Bytes::from_static(b"x")),
    )
    .await??;

    let ids = seen.lock().unwrap().clone();
    assert_eq!(ids.len(), 2, "{ids:?}");
    assert!(
        ids[0].starts_with("acme/"),
        "debug_id must carry the scope prefix: {:?}",
        ids[0]
    );
    // And the part after the `/` is still the full id — it can be looked up.
    assert_eq!(ids[0], format!("acme/{}", hex(&id(1))));
    assert_eq!(ids[1], hex(&id(2)), "no scope, no prefix");

    rt.shutdown().await?;
    Ok(())
}

/// An **orphaned** queue entry must not be rebuilt into a lock.
///
/// It can exist because removing from the queue and writing the terminal status are
/// two commits — a death right between them leaves the entry of an invocation that
/// has finished. Building a lock for it locks a virtual object **forever**: nobody is
/// left running to release it, and the only symptom is "this object does not run any
/// more".
#[tokio::test]
async fn orphan_entry_does_not_become_permanent_lock() -> Result<()> {
    let sdk = GateSdk::new().open();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-orphan")?;

    // One invocation runs to completion on `Cart/u1`...
    {
        let rt = spawn_runtime(&root, endpoint.clone())?;
        let out = tokio::time::timeout(
            TIMEOUT,
            rt.invoke(
                id(1),
                cart(Some("acme"), "u1"),
                None,
                None,
                Bytes::from_static(b"x"),
            ),
        )
        .await??;
        assert!(matches!(out, InvokeOutcome::Completed(_)), "{out:?}");

        rt.shutdown().await?;
    }

    // ...then we **rebuild by hand** exactly the entry a death between the two commits
    // would leave: a lock-holding stage, an invocation already terminal. Written
    // directly through `PartitionWriter` rather than opening a test door on
    // `EngineHandle`: no valid code path creates this state, so an API that produces it
    // would be an API whose only use is to lie.
    {
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&root)?);
        let db = Db::builder("partition-0", store)
            .with_settings(Settings {
                flush_interval: Some(Duration::from_millis(10)),
                ..Default::default()
            })
            .build()
            .await?;
        let mut w = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
        let q = cairn_store::QueueName {
            tenant: cairn_store::TenantId::default(),
            scope: Some(Scope::new("acme").unwrap()),
            limit: None,
            exclusive: true,
            service: "Cart".into(),
            object_key: "u1".into(),
        };
        let mut m = cairn_store::Mutations::new();
        m.push_vqueue(&q.encode(), Stage::Suspended, 1, &id(1));
        w.commit(m).await?;
        w.seal().await?;
    }

    // ── New leader: there must be no lock.
    let rt = spawn_runtime(&root, endpoint)?;
    let flow = restored(&rt).await?;
    assert_eq!(
        flow.scheduler.locks_held, 0,
        "a lock was built for an invocation that has stopped for good — this object is stuck forever"
    );

    // And the object is still usable.
    let out = tokio::time::timeout(
        TIMEOUT,
        rt.invoke(
            id(2),
            cart(Some("acme"), "u1"),
            None,
            None,
            Bytes::from_static(b"y"),
        ),
    )
    .await??;
    assert!(matches!(out, InvokeOutcome::Completed(_)), "{out:?}");
    rt.shutdown().await?;
    Ok(())
}

/// **FIFO order on one object key** — the only ordering guarantee cairn gives.
///
/// Each queue's waiting line is inserted by `seq`, not by arrival order
/// (`flow/scheduler.rs`, `Scheduler::acquire`), and `seq` is handed out when the
/// invocation is **created**, in the same commit as journal entry 0. So two sequential
/// calls to the same object key run in exactly the order they were accepted — no
/// matter who is faster, no matter who gets a permit first.
///
/// **Why this test deserves to exist.** This is what users really rely on ("do two
/// commands I send back to back run in order"), and before this file nothing pinned
/// it: `GateSdk::order` already existed but was only used to measure fairness between
/// tenants. A guarantee without a test is not a guarantee, only current behaviour —
/// and the machinery around it (`next_queue_seq` handed out before `Shared::finish`
/// answers, and `finish` now releases the engine lock during the durability wait) had
/// just been changed.
///
/// `submit`, not `invoke`: it answers as soon as the invocation is durable, so **the
/// acceptance order is deterministic** when we await sequentially. Firing `invoke` in
/// parallel would leave the `seq` order to tokio's scheduler, and the test would
/// measure tokio.
///
/// **A limit, stated so nobody reads it as stronger than it is.** This test pins the
/// **contract** ("sequential sends to one key run sequentially"), not the
/// **mechanism**. Changing the waiting line from an `insert` by `seq` to a `push_back`
/// keeps it green — that was actually tried and rerun, both here and in the `hot`
/// scenario of `harness/bench/load.ts` with 24 workers. The reason: sequential
/// acceptance means the order *of arriving at the waiting line* already matches the
/// `seq` order, and even under load the gap between handing out `seq` and calling
/// `acquire` is too short for the two orders to diverge.
///
/// The mechanism is pinned elsewhere, one layer down:
/// `flow::tests::queue_orders_by_seq_not_arrival_order` builds directly the case where
/// arrival order is the **reverse** of seq order — impossible to build through the
/// public API — and goes red when switched to `push_back`. The two tests complement
/// each other: that one guards the mechanism, this one guards the promise users
/// actually read.
#[tokio::test]
async fn same_object_key_runs_in_arrival_order() -> Result<()> {
    const N: u8 = 12;

    // Gate closed: no turn can run until we open it. That way all N calls are surely in
    // the queue before the first turn runs — so the test measures *queue order*, not
    // "first come first served".
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-fifo")?;
    let rt = spawn_runtime(&root, endpoint)?;

    // Accept **sequentially**: each call is sent only after the previous one is durable.
    for i in 0..N {
        let outcome = rt
            .submit(
                id(i),
                cart(None, "u1"),
                None,
                None,
                Bytes::from(i.to_string()),
            )
            .await?;
        assert!(
            matches!(outcome, InvokeOutcome::Waiting),
            "call {i} must only have been accepted, accepted: {outcome:?}"
        );
    }

    sdk.gate.add_permits(N as usize);
    for i in 0..N {
        wait_finished(&rt, id(i)).await?;
    }

    let seen_order = sdk.order();
    let expected_order: Vec<String> = (0..N).map(|i| i.to_string()).collect();
    assert_eq!(
        seen_order, expected_order,
        "same object key ran out of acceptance order:\n  seen     {seen_order:?}\n  expected {expected_order:?}"
    );
    rt.shutdown().await?;
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// First leader: each `id(n)`, `n` in `ids`, runs a background turn (`/send`) whose
/// first turn fails retryably in the SDK (`GateSdk::fail_first`): the turn ends with a
/// durable **retry** (one attempt counted — the SDK was called); then the leader steps
/// down. The new leader sees that row: `recover` does not dispatch a turn (the durable
/// retry is what continues the invocation), the background tick fires it when due.
/// Since background turns waiting for a permit are no longer throttled at the permit
/// request (no `park_for_retry`), this is how the test builds a retry row of an
/// unfinished invocation. Returns the number of turns that entered the SDK.
async fn retry_schedule_rows(
    sdk: &GateSdk,
    root: &Path,
    d: cairn_server::discovery::Deployment,
    ids: std::ops::RangeInclusive<u8>,
) -> Result<usize> {
    let input = |n: u8| format!("in-{n}");
    sdk.fail_first
        .lock()
        .unwrap()
        .extend(ids.clone().map(input));
    let registry = Registry::new();
    registry.upsert(d);
    let rt = spawn_runtime_with(root, registry, limits(None))?;
    rt.set_rules(rules(&[("acme", u32::from(*ids.end()))]))
        .await?;
    for n in ids.clone() {
        let out = rt
            .submit(id(n), work(Some("acme")), None, None, Bytes::from(input(n)))
            .await?;
        assert!(matches!(out, InvokeOutcome::Waiting), "{out:?}");
    }
    let k = ids.count();
    wait_until(TIMEOUT, || sdk.fail_first.lock().unwrap().is_empty()).await?;
    // The failed turn commits its end (the retry), then releases the permit.
    let deadline = std::time::Instant::now() + TIMEOUT;
    while rt.flow().await?.scheduler.turns_in_use > 0 {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "failed turn has not ended"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    rt.shutdown().await?;
    Ok(k)
}

/// **A due retry of a busy invocation must not be rescanned on every tick.**
///
/// Thousands of invocations waiting for a permit of the same scope: each waiter is a
/// dispatched turn that has not finished (a ticket parked in the scheduler's queue —
/// FL-20 counts it as busy), while its next retry (set before running, a few hundred
/// ms out) is already due again. Before the fix `fire_retries` skipped a busy
/// invocation — and the row stayed in place, so **every tick** (100 ms) rescanned all
/// of them, one read per row on the engine thread. Measured on a real cluster (a
/// billing workload under chaos): commits waited ~1.5 s, a hot object drained 0.1
/// commands/s instead of 50/s — a self-amplifying spiral.
///
/// Interleaving:
/// 1. First leader: the first turn of `N + 1` invocations fails retryably — one retry
///    per invocation ([`retry_schedule_rows`]).
/// 2. New leader, rule `acme = 1`. Tick A far past every due time: every row fires,
///    the next retry is set ~200 ms later; one turn gets the permit (enters the SDK,
///    gate closed), the other `N` park their tickets.
/// 3. Tick B at a `now` where every next retry is due: it meets `N` busy invocations.
/// 4. Tick C, at the **same** `now`.
/// 5. Open the gate.
///
/// Must see: (3) ≥ `N / 2` busy rows; (4) no busy row (their due time was moved); (5)
/// no invocation lost.
#[tokio::test]
async fn due_retry_of_busy_invocation_not_rescanned_every_tick() -> Result<()> {
    use cairn_server::metrics::flow::RETRY_BUSY;
    const N: u8 = 30;

    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-retry-busy")?;
    let wait = limits(Some(Duration::from_millis(800)));
    retry_schedule_rows(&sdk, &root, deployment(endpoint.clone()), 1..=N + 1).await?;

    let registry = Registry::new();
    let rt = spawn_runtime_with(&root, registry.clone(), wait)?;
    rt.set_rules(rules(&[("acme", 1)])).await?;
    registry.upsert(deployment(endpoint));
    rt.deployments_changed().await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Tick A: every retry is due ⇒ N + 1 turns dispatched; their next retries are due
    // ~200–400 ms later.
    let base = now_ms() + 600_000;
    let fired = rt.tick(base).await?;
    assert!(
        fired >= usize::from(N) / 2,
        "tick A fired only {fired} retries"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Tick B at a `now` where every next retry is due: it meets N busy rows.
    let before = RETRY_BUSY.load(Ordering::Relaxed);
    rt.tick(base + 1_000).await?;
    let first_pass = RETRY_BUSY.load(Ordering::Relaxed) - before;
    assert!(
        first_pass >= u64::from(N) / 2,
        "scenario not built: only {first_pass} busy rows at tick B"
    );

    // Tick C, same `now`: the busy rows were moved — no rescan.
    let before = RETRY_BUSY.load(Ordering::Relaxed);
    rt.tick(base + 1_000).await?;
    let second_pass = RETRY_BUSY.load(Ordering::Relaxed) - before;
    assert_eq!(
        second_pass, 0,
        "the next tick rescanned {second_pass} busy rows — a useless read round per tick"
    );

    // Release the permit: nobody is lost. A tick at a far-future `now` fires every
    // retry, including the ones that were moved.
    sdk.gate.add_permits(4 * usize::from(N) + 4);
    let deadline = std::time::Instant::now() + TIMEOUT;
    for n in 1..=N + 1 {
        loop {
            if matches!(
                rt.status(id(n)).await?,
                Some(InvocationStatus::Completed(_))
            ) {
                break;
            }
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "{} not finished",
                hex(&id(n))
            );
            rt.tick(now_ms() + 1_200_000).await?;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    rt.shutdown().await?;
    Ok(())
}

/// The second half of the same bug: a background turn **waiting for a VO lock** does
/// not give up after `queue_wait`.
///
/// The VO lock is the normal serialization of an object — waiting for it is not
/// "budget exhausted". Giving up and rescheduling here costs a retry commit + a refire
/// commit every `queue_wait` for **each** invocation in the line: a hot object with
/// 6 000 commands waiting on a real cluster gave 100–180 give-ups/s per node,
/// competing for the engine thread and the WAL with the very command that is running
/// — draining 0.1 commands/s instead of 50/s. Waiters keep waiting in the scheduler
/// (by `seq`, like the client path); only a block by a counter reschedules.
#[tokio::test]
async fn background_turn_waiting_for_object_lock_does_not_give_up_after_wait_cap() -> Result<()> {
    const N: u8 = 20;
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-lock-patient")?;
    let wait = limits(Some(Duration::from_millis(300)));

    // First leader: one turn holds the lock of object `k` and stands still; N more join
    // the same object's line. Then the leader steps down.
    {
        let rt = spawn_runtime_with(&root, registry_for(endpoint.clone()), wait)?;
        let h = rt.clone();
        tokio::spawn(async move {
            let _ = h
                .invoke(id(1), cart(None, "k"), None, None, Bytes::from_static(b"a"))
                .await;
        });
        sdk.wait_until_entered(1).await?;
        for n in 2..=N + 1 {
            let out = rt
                .submit(id(n), cart(None, "k"), None, None, Bytes::from_static(b"b"))
                .await?;
            assert!(matches!(out, InvokeOutcome::Waiting), "{out:?}");
        }
        rt.shutdown().await?;
    }

    // New leader: `recover` reruns the whole line on the background path. The first
    // holds the lock (SDK gate closed); the other N wait for the lock — five times as
    // long as the permit wait cap.
    let registry = Registry::new();
    let rt = spawn_runtime_with(&root, registry.clone(), wait)?;
    registry.upsert(deployment(endpoint));
    rt.deployments_changed().await?;
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    // Whoever gave up has written a retry; a tick in the far future fires them all.
    // Those still waiting hold the invocation lock — the tick cannot touch them.
    let fired = rt.tick(now_ms() + 120_000).await?;
    assert_eq!(
        fired, 0,
        "VO lock waiters gave up and rescheduled instead of waiting in the scheduler"
    );

    // Open the gate: the whole line runs, exactly once each.
    sdk.gate.add_permits(2 * usize::from(N) + 2);
    let deadline = std::time::Instant::now() + TIMEOUT;
    for n in 1..=N + 1 {
        loop {
            if matches!(
                rt.status(id(n)).await?,
                Some(InvocationStatus::Completed(_))
            ) {
                break;
            }
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "{} not finished",
                hex(&id(n))
            );
            rt.tick(now_ms() + 120_000).await?;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    rt.shutdown().await?;
    Ok(())
}

/// The `n`-th id of a line longer than 255 (`id(n)` has only one byte).
fn wide_id(n: u16) -> [u8; 24] {
    let mut b = [0u8; 24];
    b[14..16].copy_from_slice(&n.to_be_bytes());
    b
}

/// **A task waiting for a permit does not come back to the engine every slice.**
///
/// Interleaving:
/// 1. First leader: `wide_id(0)` holds the lock of object `k` and stands still in the
///    SDK (gate closed); `W` more join the same object's line (`submit`). The leader
///    steps down.
/// 2. New leader, permit wait slice 20 ms: `recover` hands the whole line to the
///    scheduler on the background path. The first wins the VO lock and enters the
///    SDK (gate still closed); the other `W` wait for the VO lock — not because of the
///    budget, so each waits across many slices, with **one** permit request for the
///    whole wait.
/// 3. The line stands still (half a second after the first entered the SDK), the test
///    counts the engine holds of holder `turn` (`DbStats`) in a 1 s window.
/// 4. Open the gate: the whole line runs.
///
/// Must see: in the window, the `turn` engine holds ≈ the permits granted — 0, gate
/// closed — below `W / 2`, not ≈ `W × 1 s / slice`; nobody gives up (a tick in the far
/// future fires no retry); with the gate open everybody finishes. Before the fix: every
/// waiter took the engine again after each slice just to read `blocked_on` (the
/// scheduler's RAM) — `W / slice` holds per second.
#[tokio::test]
async fn permit_waiter_does_not_keep_grabbing_engine() -> Result<()> {
    const W: u16 = 200;
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-permit-slices")?;
    let slice = limits(Some(Duration::from_millis(20)));

    {
        let rt = spawn_runtime_with(&root, registry_for(endpoint.clone()), limits(None))?;
        let h = rt.clone();
        tokio::spawn(async move {
            let _ = h
                .invoke(
                    wide_id(0),
                    cart(None, "k"),
                    None,
                    None,
                    Bytes::from_static(b"a"),
                )
                .await;
        });
        sdk.wait_until_entered(1).await?;
        for n in 1..=W {
            let out = rt
                .submit(
                    wide_id(n),
                    cart(None, "k"),
                    None,
                    None,
                    Bytes::from_static(b"b"),
                )
                .await?;
            assert!(matches!(out, InvokeOutcome::Waiting), "{out:?}");
        }
        rt.shutdown().await?;
    }

    let stats = cairn_store::dbstats::DbStats::new();
    let turn = cairn_store::dbstats::holder("turn");
    let registry = Registry::new();
    let rt = spawn_runtime_stats(&root, registry.clone(), slice, Some(stats.clone()))?;
    registry.upsert(deployment(endpoint));
    rt.deployments_changed().await?;
    // The first enters the SDK a second time (the first time was on the first leader).
    wait_until(TIMEOUT, || sdk.order().len() >= 2).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let before = stats.snapshot().holds[turn];
    tokio::time::sleep(Duration::from_secs(1)).await;
    let holds = stats.snapshot().holds[turn] - before;
    eprintln!("W = {W}, slice 20 ms — {holds} engine holds of `turn` in 1 s");
    assert!(
        holds < u64::from(W) / 2,
        "{holds} engine holds in 1 s with {W} VO lock waiters and no permit granted \
         — waiters come back to the engine every slice"
    );
    let fired = rt.tick(now_ms() + 120_000).await?;
    assert_eq!(fired, 0, "VO lock waiters gave up and rescheduled");

    sdk.gate.add_permits(2 * usize::from(W) + 4);
    let deadline = std::time::Instant::now() + 6 * TIMEOUT;
    for n in 0..=W {
        loop {
            if matches!(
                rt.status(wide_id(n)).await?,
                Some(InvocationStatus::Completed(_))
            ) {
                break;
            }
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "{} not finished",
                hex(&wide_id(n))
            );
            rt.tick(now_ms() + 120_000).await?;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    rt.shutdown().await?;
    Ok(())
}

/// RSS of the test process, MiB (`ps`; 0 if it cannot be read).
fn rss_mib() -> f64 {
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<f64>().ok())
        .map_or(0.0, |kib| kib / 1024.0)
}

/// **Permit queue bench** — not a check: prints numbers, run by hand only.
///
/// `CAIRN_PERMIT_W` (default 1 000) invocations wait for the VO lock of one object
/// behind a turn standing still in the SDK, on a leader that just took over (`recover`
/// hands the whole line over on the background path), with the default permit wait
/// cap (`DEFAULT_QUEUE_WAIT`, 10 s) or `CAIRN_PERMIT_SLICE_MS`. Prints: time until the
/// first enters the SDK, engine holds of `turn` per second while the line stands still
/// (3 s), RSS, then opens the gate and measures the time to drain the whole line
/// (turns/s, engine holds per turn).
///
/// `cargo test --release -p cairn-server --test flow_control -- --ignored bench_permit_queue --nocapture`
#[tokio::test]
#[ignore = "bench, run by hand"]
async fn bench_permit_queue() -> Result<()> {
    let w: u16 = std::env::var("CAIRN_PERMIT_W")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000);
    let slice = std::env::var("CAIRN_PERMIT_SLICE_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .map_or(
            cairn_server::turn::DEFAULT_QUEUE_WAIT,
            Duration::from_millis,
        );
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root(&format!("flow-permit-bench-{w}"))?;
    let rss0 = rss_mib();
    {
        let rt = spawn_runtime_with(&root, registry_for(endpoint.clone()), limits(None))?;
        let h = rt.clone();
        tokio::spawn(async move {
            let _ = h
                .invoke(
                    wide_id(0),
                    cart(None, "k"),
                    None,
                    None,
                    Bytes::from_static(b"a"),
                )
                .await;
        });
        sdk.wait_until_entered(1).await?;
        for n in 1..=w {
            rt.submit(
                wide_id(n),
                cart(None, "k"),
                None,
                None,
                Bytes::from_static(b"b"),
            )
            .await?;
        }
        rt.shutdown().await?;
    }

    let stats = cairn_store::dbstats::DbStats::new();
    let turn = cairn_store::dbstats::holder("turn");
    let registry = Registry::new();
    let rt = spawn_runtime_stats(
        &root,
        registry.clone(),
        limits(Some(slice)),
        Some(stats.clone()),
    )?;
    registry.upsert(deployment(endpoint));
    let up = std::time::Instant::now();
    rt.deployments_changed().await?;
    wait_until(Duration::from_secs(600), || sdk.order().len() >= 2).await?;
    let first = up.elapsed();
    // Let `recover` hand over the whole line and every waiter enter the scheduler.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let before = stats.snapshot().holds[turn];
    tokio::time::sleep(Duration::from_secs(3)).await;
    let idle_holds = (stats.snapshot().holds[turn] - before) as f64 / 3.0;
    let rss = rss_mib();

    sdk.gate.add_permits(Semaphore::MAX_PERMITS);
    let before = stats.snapshot().holds[turn];
    let drain = std::time::Instant::now();
    let deadline = drain + Duration::from_secs(1_800);
    while !matches!(
        rt.status(wide_id(w)).await?,
        Some(InvocationStatus::Completed(_))
    ) {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "line not drained after 30 minutes"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let drained = drain.elapsed();
    let drain_holds = stats.snapshot().holds[turn] - before;
    eprintln!(
        "permit bench: W={w} slice={slice:?} | first in SDK after {first:.2?} | `turn` engine holds while idle {idle_holds:.0}/s | RSS {rss:.0} MiB (start {rss0:.0}) | drained in {drained:.2?} = {:.0} turns/s, {:.1} holds/turn",
        f64::from(w) / drained.as_secs_f64(),
        drain_holds as f64 / f64::from(w)
    );
    rt.shutdown().await?;
    Ok(())
}

/// **A turn that read state and then died mid-way still holds the VO lock.**
///
/// On `BIDI_STREAM` (and on `REQUEST_RESPONSE` through a rerun turn), a turn's journal
/// is committed **incrementally**: the state read command and its completion are
/// durable before the SDK gets to write. A leader dying right then leaves the queue
/// entry at `Running`. A new leader that pushes it back to `Inbox` **without the lock**
/// leaves the object unlocked: a call arriving later runs first and writes state, then
/// the old turn reruns, replays exactly the value read from the journal and
/// **overwrites** — an update vanishes while both invocations are `completed`. Caught
/// in a billing workload under chaos: two `recordUsage` both read `apiCalls = 5030`.
///
/// Built deterministically: A reads `n = 0` and stops at the write turn; kill the
/// leader. The new leader comes up **without any deployment** (so `recover` has not
/// dispatched A yet), B arrives first — B may only run after A.
#[tokio::test]
async fn turn_that_read_state_then_died_midway_keeps_object_lock() -> Result<()> {
    let sdk = GateSdk::counting_held();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-lock-held-mid-turn")?;
    let (a, b) = (id(1), id(2));

    {
        let rt = spawn_runtime(&root, endpoint.clone())?;
        rt.submit(a, cart(None, "u1"), None, None, Bytes::from_static(b"1"))
            .await?;
        // A has read `n` (GetState + completion durable), and stands at the write turn.
        sdk.wait_until_entered(1).await?;
        rt.shutdown().await?;
    }
    // The write turn of the dead leader: let it out, nobody receives its answer any more.
    sdk.gate.add_permits(1);
    wait_until(TIMEOUT, || sdk.inside() == 0).await?;

    let registry = Registry::new();
    let rt = spawn_runtime_with(&root, registry.clone(), limits(None))?;
    let flow = restored(&rt).await?;
    assert_eq!(
        flow.scheduler.locks_held, 1,
        "A read Cart/u1's state before the leader died — the VO lock must be rebuilt"
    );

    // B arrives before A is dispatched to rerun.
    registry.upsert(deployment(endpoint));
    sdk.gate.add_permits(16);
    rt.submit(b, cart(None, "u1"), None, None, Bytes::from_static(b"10"))
        .await?;
    rt.deployments_changed().await?;
    wait_finished(&rt, a).await?;
    wait_finished(&rt, b).await?;

    let out = |s: Option<InvocationStatus>| match s {
        Some(InvocationStatus::Completed(v)) => {
            String::from_utf8_lossy(v.inline().map(|b| &b[..]).unwrap_or_default()).into_owned()
        }
        other => format!("{other:?}"),
    };
    let (va, vb) = (out(rt.status(a).await?), out(rt.status(b).await?));
    assert_eq!(
        (va.as_str(), vb.as_str()),
        ("1", "11"),
        "an update was lost: A saw {va}, B saw {vb} — B must run after A"
    );
    rt.shutdown().await?;
    Ok(())
}

/// **Resending the same Idempotency-Key does not wait for the object's turn.**
///
/// The first send answers at once ("accepted"), while the invocation's turn waits for
/// the VO lock behind the caller's back — and **holds the invocation lock** the whole
/// time. A client that lost the first answer (node died, timeout) resends with the
/// same key; the resend only needs an idempotency table lookup, but it looks up *under*
/// the invocation lock, so it waits until the other invocation runs — behind thousands
/// of commands of a hot object. The client times out, resends, times out... Caught in
/// a billing workload under chaos: 15 `recordUsage` commands that had been applied were
/// resent for 5 minutes without a single ack.
#[tokio::test]
async fn resending_same_idempotency_key_does_not_wait_for_object_turn() -> Result<()> {
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-resend-idempotency")?;
    let rt = spawn_runtime(&root, endpoint)?;

    // A holds Cart/u1's VO lock (stands in the SDK, gate closed); B queues behind A.
    rt.submit(
        id(1),
        cart(None, "u1"),
        None,
        None,
        Bytes::from_static(b"a"),
    )
    .await?;
    sdk.wait_until_entered(1).await?;
    let first = rt
        .submit(
            id(2),
            cart(None, "u1"),
            None,
            Some("ev-b".into()),
            Bytes::from_static(b"b"),
        )
        .await?;
    assert!(matches!(first, InvokeOutcome::Waiting), "{first:?}");

    // Resend B: it must answer at once that B exists and is waiting.
    let again = tokio::time::timeout(
        Duration::from_secs(2),
        rt.submit(
            id(2),
            cart(None, "u1"),
            None,
            Some("ev-b".into()),
            Bytes::from_static(b"b"),
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("resend with the same key waited for the object's turn"))??;
    assert!(matches!(again, InvokeOutcome::Waiting), "{again:?}");

    sdk.gate.add_permits(16);
    wait_finished(&rt, id(1)).await?;
    wait_finished(&rt, id(2)).await?;
    let done = rt
        .submit(
            id(2),
            cart(None, "u1"),
            None,
            Some("ev-b".into()),
            Bytes::from_static(b"b"),
        )
        .await?;
    assert!(matches!(done, InvokeOutcome::Completed(_)), "{done:?}");
    assert_eq!(
        sdk.order(),
        vec!["a", "b"],
        "the resend must not run B a second time"
    );
    rt.shutdown().await?;
    Ok(())
}

/// **A dispatched retry turn that has not yet won the lock still counts as "someone is
/// working on it".**
///
/// Interleaving:
/// 1. First leader: the first turn of `id(1)` fails retryably — one retry
///    ([`retry_schedule_rows`]).
/// 2. New leader, rule `acme = 0`. The test fires **two** background ticks at once,
///    at two `now`s past every backoff. Tick A gets the engine first; `fire_retries`
///    moves the retry row and dispatches a turn to the scheduler. That turn's task
///    queues for the engine **behind** tick B (the engine lock is FIFO), so it does
///    not hold the invocation lock yet.
/// 3. Tick B: the retry row is due, the invocation lock is free.
/// 4. Tick A's turn passes the gate and parks its ticket (`acme = 0`); raise the limit.
///
/// Must see: tick B moves the row (the invocation has a dispatched, unfinished turn,
/// as `recover` and the rescue net ask `is_attended`), the two ticks fire **once** in
/// total, and with the limit raised `id(1)` is `completed` after exactly one turn in
/// the SDK on the new leader. Must not see: tick B dispatching a duplicate turn.
#[tokio::test]
async fn delivered_turn_without_lock_is_not_delivered_twice() -> Result<()> {
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-queued-retry")?;
    let wait = limits(Some(Duration::from_millis(300)));
    let base = retry_schedule_rows(&sdk, &root, deployment(endpoint.clone()), 1..=1).await?;
    sdk.gate.add_permits(Semaphore::MAX_PERMITS);

    let registry = Registry::new();
    let rt = spawn_runtime_with(&root, registry.clone(), wait)?;
    rt.set_rules(rules(&[("acme", 0)])).await?;
    registry.upsert(deployment(endpoint));
    rt.deployments_changed().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let now = now_ms();
    let (a, b) = tokio::join!(rt.tick(now + 600_000), rt.tick(now + 1_200_000));
    let (a, b) = (a?, b?);
    assert_eq!(
        (a, b),
        (1, 0),
        "tick B must see tick A's dispatched turn (fired: A {a}, B {b})"
    );

    // The new rule version is newer than the running one: the same `version` is ignored.
    let mut open = rules(&[("acme", 1)]);
    open.version = 2;
    rt.set_rules(open).await?;
    let deadline = std::time::Instant::now() + TIMEOUT;
    while !matches!(
        rt.status(id(1)).await?,
        Some(InvocationStatus::Completed(_))
    ) {
        if std::time::Instant::now() > deadline {
            anyhow::bail!(
                "id(1) not finished after {TIMEOUT:?}: {:?}",
                rt.status(id(1)).await?
            );
        }
        rt.tick(now_ms() + 86_400_000).await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        sdk.order().len(),
        base + 1,
        "exactly one turn in the SDK on the new leader: {:?}",
        sdk.order()
    );
    rt.shutdown().await?;
    Ok(())
}

/// **A retry turn cancelled while waiting for a permit must not count as an attempt.**
///
/// Interleaving:
/// 1. First leader: the first turn of `id(1..=N)` fails retryably — one retry per
///    invocation, **one** attempt counted (the SDK was called,
///    [`retry_schedule_rows`]).
/// 2. In each following term, rule `acme = 0`: the test fires one background tick at
///    a `now` past every backoff: `fire_retries` moves the retry row to the next
///    attempt and dispatches a turn to the scheduler; the turn passes the gate and
///    **parks its ticket** (no permit).
/// 3. The leader steps down **at once** — the ticket is lost with the scheduler
///    (stepping down does not wait for turns without a permit), no outcome is
///    reported, no line is logged.
/// 4. Repeat 2–3 more than `maxAttempts` times (each one a range changing owner).
/// 5. Last leader: raise the limit.
///
/// Must see: every invocation `completed`. Must not see: `paused` with code 575
/// "N attempts exhausted" for an invocation whose SDK was called only once — the
/// shape seen in a billing workload's soak (53 `Invoice.run` exhausted 20 attempts on
/// a range that kept changing owner, the gate took no call, the log had no line).
#[tokio::test]
async fn retry_cancelled_while_waiting_for_permit_does_not_spend_attempt() -> Result<()> {
    const N: u8 = 3;
    const MAX_ATTEMPTS: u32 = 3;

    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-cancelled-attempts")?;
    let wait = limits(Some(Duration::from_millis(300)));
    let with_max = |endpoint: String| {
        let mut d = deployment(endpoint.clone());
        d.services[0].handlers[0].contract.retry_policy_max_attempts = Some(MAX_ATTEMPTS);
        d
    };
    let start = |acme: u32| {
        let registry = Registry::new();
        let rt = spawn_runtime_with(&root, registry.clone(), wait);
        let d = with_max(endpoint.clone());
        async move {
            let rt = rt?;
            rt.set_rules(rules(&[("acme", acme)])).await?;
            registry.upsert(d);
            rt.deployments_changed().await?;
            anyhow::Ok(rt)
        }
    };

    let base = retry_schedule_rows(&sdk, &root, with_max(endpoint.clone()), 1..=N).await?;
    sdk.gate.add_permits(Semaphore::MAX_PERMITS);

    let mut fired = 0;
    for k in 1..=2 * MAX_ATTEMPTS as u64 + 2 {
        let rt = start(0).await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        fired += rt.tick(now_ms() + k * 600_000).await?;
        // The turn just dispatched passes the gate and parks its ticket before the leader steps down.
        tokio::time::sleep(Duration::from_millis(200)).await;
        rt.shutdown().await?;
    }
    assert!(
        fired >= usize::from(N) * (2 * MAX_ATTEMPTS as usize),
        "scenario not built: only {fired} retries fired"
    );
    assert_eq!(
        sdk.order().len(),
        base,
        "the SDK was called on the blocked leader: {:?}",
        sdk.order()
    );

    let rt = start(u32::from(N)).await?;
    let deadline = std::time::Instant::now() + TIMEOUT;
    let mut last = Vec::new();
    loop {
        last.clear();
        for n in 1..=N {
            last.push((n, rt.status(id(n)).await?));
        }
        if last
            .iter()
            .all(|(_, s)| matches!(s, Some(InvocationStatus::Completed(_))))
        {
            break;
        }
        if std::time::Instant::now() > deadline {
            anyhow::bail!(
                "not finished after {TIMEOUT:?}: {last:?}\nSDK saw: {:?}",
                sdk.order()
            );
        }
        rt.tick(now_ms() + 86_400_000).await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    rt.shutdown().await?;
    Ok(())
}

/// A regression of the previous fix: **an attempt that is not counted must still grow
/// the backoff.**
///
/// Interleaving:
/// 1. First leader: the first turn of `id(1)` fails retryably — one retry
///    ([`retry_schedule_rows`]).
/// 2. In each following term, rule `acme = 0`: the test fires **one** background tick
///    at a fake `now` advancing steadily by 1 s per term; `fire_retries` moves the
///    retry row to the next attempt and dispatches the turn; the turn parks its ticket
///    and is lost when the leader steps down — it never enters `Running`, so it does
///    not consume an attempt (FL-19).
/// 3. Repeat for 12 terms.
///
/// Must see: the gap between two fires grows (100 ms · 2ⁿ), so most of the later ticks
/// 1 s apart do not fire. Must not see: every tick firing — the backoff step was keyed
/// on the attempt count, and the attempt count does not grow, so the retry row of every
/// blocked invocation fired again at the base backoff every time the range changed
/// owner. Under chaos that was a waiting line growing linearly to ~97k turns waiting
/// for a permit on one range, each waiting turn taking the engine again every wait
/// slice (the drain never finished, throughput collapsed from minute 20).
#[tokio::test]
async fn fire_not_counted_as_attempt_still_backs_off() -> Result<()> {
    const TERMS: u64 = 12;

    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-backoff-grows")?;
    let wait = limits(Some(Duration::from_millis(300)));
    let start = || {
        let registry = Registry::new();
        let rt = spawn_runtime_with(&root, registry.clone(), wait);
        let d = deployment(endpoint.clone());
        async move {
            let rt = rt?;
            rt.set_rules(rules(&[("acme", 0)])).await?;
            registry.upsert(d);
            rt.deployments_changed().await?;
            anyhow::Ok(rt)
        }
    };

    let seen = retry_schedule_rows(&sdk, &root, deployment(endpoint.clone()), 1..=1).await?;
    sdk.gate.add_permits(Semaphore::MAX_PERMITS);

    // The fake clock starts past every first step so that the first fire surely happens.
    let base = now_ms() + 600_000;
    let mut fired = Vec::new();
    for k in 0..TERMS {
        let rt = start().await?;
        fired.push(rt.tick(base + k * 1_000).await?);
        tokio::time::sleep(Duration::from_millis(100)).await;
        rt.shutdown().await?;
    }
    let total: usize = fired.iter().sum();
    assert!(
        fired[0] >= 1,
        "scenario not built: the first tick did not fire ({fired:?})"
    );
    assert_eq!(
        sdk.order().len(),
        seen,
        "the SDK was called: {:?}",
        sdk.order()
    );
    // Backing off: 200, 400, 800 ms fire every tick; 1.6 s skips one tick; 3.2 s
    // skips three; … — about 6 fires in 12 ticks. Staying at the base step fires all 12.
    assert!(
        total <= 8,
        "backoff does not grow: {total} fires in {TERMS} ticks 1 s apart ({fired:?})"
    );
    Ok(())
}

/// **A background turn waiting for a permit because of the budget must not count as
/// an attempt.**
///
/// Interleaving:
/// 1. Old leader: the turn of `id(1)` takes `acme`'s only permit and stands still in
///    the SDK; `id(2..)` join the line (`/send`). The leader steps down mid-turn of
///    `id(1)` (its watchdog retry row).
/// 2. New leader: `recover` reruns `id(2..)` on the background path. One takes the
///    permit (enters the SDK, gate closed), the others wait because of the budget —
///    formerly: `queue_wait` ran out ⇒ `Throttled`, `park_for_retry` set a retry row,
///    every background tick fired it again (`attempt + 1` written **before** running);
///    now: they park a ticket, do not give up, do not reschedule.
/// 3. The test fires the background tick several times, each at a `now` past every
///    backoff.
/// 4. Release the permit.
///
/// Must see: only `id(1)`'s watchdog retry row fires (once — after that it parks a
/// ticket, busy); every invocation `completed`. Must not see: `paused` with code 575
/// "N attempts exhausted" for an invocation whose SDK was never called again — the
/// shape seen in a billing workload's soak (19 attempts after a failover, the gate
/// took no call, the log had no line).
#[tokio::test]
async fn background_turn_parked_for_budget_does_not_spend_attempt() -> Result<()> {
    const N: u8 = 3;
    const MAX_ATTEMPTS: u32 = 3;

    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-throttle-attempts")?;
    let wait = limits(Some(Duration::from_millis(300)));
    let with_max = |endpoint: String| {
        let mut d = deployment(endpoint);
        d.services[0].handlers[0].contract.retry_policy_max_attempts = Some(MAX_ATTEMPTS);
        d
    };
    let input = |n: u8| Bytes::from(format!("in-{n}"));

    {
        let registry = Registry::new();
        registry.upsert(with_max(endpoint.clone()));
        let rt = spawn_runtime_with(&root, registry, wait)?;
        rt.set_rules(rules(&[("acme", 1)])).await?;
        let h = rt.clone();
        tokio::spawn(async move {
            let _ = h
                .invoke(id(1), work(Some("acme")), None, None, input(1))
                .await;
        });
        sdk.wait_until_entered(1).await?;
        for n in 2..=N + 1 {
            let out = rt
                .submit(id(n), work(Some("acme")), None, None, input(n))
                .await?;
            assert!(matches!(out, InvokeOutcome::Waiting), "{out:?}");
        }
        rt.shutdown().await?;
    }
    let seen_before = sdk.order().len();

    let registry = Registry::new();
    let rt = spawn_runtime_with(&root, registry.clone(), wait)?;
    rt.set_rules(rules(&[("acme", 1)])).await?;
    registry.upsert(with_max(endpoint));
    rt.deployments_changed().await?;
    // `recover` has dispatched background turns for all of them; `queue_wait` passed many times.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Each tick at a `now` past every backoff: every retry row is due.
    let mut fired = 0;
    for k in 1..=2 * MAX_ATTEMPTS as u64 + 2 {
        fired += rt.tick(now_ms() + k * 600_000).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        fired <= 1,
        "{fired} retries fired — budget waiters gave up and rescheduled (only `id(1)`'s \
         watchdog row may fire)"
    );

    sdk.gate.add_permits(4 * usize::from(N) + 4);
    let deadline = std::time::Instant::now() + TIMEOUT;
    let mut last = Vec::new();
    loop {
        last.clear();
        for n in 1..=N + 1 {
            last.push((n, rt.status(id(n)).await?));
        }
        if last
            .iter()
            .all(|(_, s)| matches!(s, Some(InvocationStatus::Completed(_))))
        {
            break;
        }
        if std::time::Instant::now() > deadline {
            let after: Vec<String> = sdk.order()[seen_before..].to_vec();
            anyhow::bail!(
                "not finished after {TIMEOUT:?}: {last:?}\nSDK saw on the new leader: {after:?}"
            );
        }
        rt.tick(now_ms() + 86_400_000).await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    rt.shutdown().await?;
    Ok(())
}

/// First leader: `wide_id(0)` enters the SDK and stands still (gate closed), `w` more
/// join the line `target(n)` (`submit` — durable at once); then it steps down. The new
/// leader (built by the caller) reruns the whole line on the background path (`recover`).
async fn build_queue(
    sdk: &GateSdk,
    endpoint: &str,
    root: &Path,
    book: Option<RuleBook>,
    w: u16,
    target: impl Fn(u16) -> Target,
) -> Result<()> {
    let rt = spawn_runtime_with(root, registry_for(endpoint.to_string()), limits(None))?;
    if let Some(b) = &book {
        rt.set_rules(b.clone()).await?;
    }
    let h = rt.clone();
    let first = target(0);
    tokio::spawn(async move {
        let _ = h
            .invoke(wide_id(0), first, None, None, Bytes::from_static(b"a"))
            .await;
    });
    sdk.wait_until_entered(1).await?;
    for n in 1..=w {
        let out = rt
            .submit(wide_id(n), target(n), None, None, Bytes::from_static(b"b"))
            .await?;
        assert!(matches!(out, InvokeOutcome::Waiting), "{out:?}");
    }
    rt.shutdown().await?;
    Ok(())
}

/// Opens the gate for the whole line `0..=w` and waits for everybody to be `Completed` — except `except`.
async fn drain_all(rt: &EngineHandle, sdk: &GateSdk, w: u16, except: &[u16]) -> Result<()> {
    sdk.gate.add_permits(2 * usize::from(w) + 4);
    let deadline = std::time::Instant::now() + 6 * TIMEOUT;
    for n in (0..=w).filter(|n| !except.contains(n)) {
        loop {
            if matches!(
                rt.status(wide_id(n)).await?,
                Some(InvocationStatus::Completed(_))
            ) {
                break;
            }
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "{} not finished",
                hex(&wide_id(n))
            );
            rt.tick(now_ms() + 120_000).await?;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    Ok(())
}

/// **A waiter for a permit because of the budget does not come back to the engine on
/// every tick, and does not reschedule through the retry row.**
///
/// Interleaving:
/// 1. First leader: `wide_id(0)` holds the only permit of scope `acme` (rule
///    `acme = 1`) and stands still in the SDK; `W` more join the line (`submit`, scope
///    `acme`). It steps down.
/// 2. New leader, permit wait slice 20 ms: `recover` hands the whole line over on the
///    background path. The first wins the permit and enters the SDK (gate still
///    closed); the other `W` are blocked by the **budget** (`concurrency`) — formerly:
///    at the end of a slice they gave up, rescheduled (`Event::Throttled`), the
///    background tick fired the retry row, a new task, the gate…
/// 3. The line stands still (half a second after the first entered the SDK): count
///    the engine holds of `turn` (`DbStats`) in a 1 s window with a background tick
///    every 50 ms, and the retries those ticks fire (plus one tick in the far future).
/// 4. Open the gate for `G` permits: count the engine holds of `turn` until `G` new
///    turns enter the SDK — a count, not a wall-clock measurement.
/// 5. Open the gate fully: the whole line finishes.
///
/// Must see: (3) below `W / 4` holds (not ≈ `W × 1 s / slice`) and no retry; (4) below
/// `10` holds per granted permit (not proportional to `W`); (5) nobody lost.
#[tokio::test]
async fn budget_waiter_neither_grabs_engine_nor_reschedules() -> Result<()> {
    const W: u16 = 200;
    const G: usize = 20;
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-budget-parked")?;
    let book = rules(&[("acme", 1)]);
    build_queue(&sdk, &endpoint, &root, Some(book.clone()), W, |_| {
        work(Some("acme"))
    })
    .await?;

    let stats = cairn_store::dbstats::DbStats::new();
    let turn = cairn_store::dbstats::holder("turn");
    let registry = Registry::new();
    let slice = limits(Some(Duration::from_millis(20)));
    let rt = spawn_runtime_stats(&root, registry.clone(), slice, Some(stats.clone()))?;
    rt.set_rules(book).await?;
    registry.upsert(deployment(endpoint));
    rt.deployments_changed().await?;
    wait_until(TIMEOUT, || sdk.order().len() >= 2).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // A 1 s window, a background tick every 50 ms as on a real node (the harness has no
    // background tick of its own).
    let before = stats.snapshot().holds[turn];
    let mut fired = 0;
    for _ in 0..20 {
        fired += rt.tick(now_ms()).await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let waiting = stats.snapshot().holds[turn] - before;
    fired += rt.tick(now_ms() + 120_000).await?;
    eprintln!(
        "W = {W}, slice 20 ms — {waiting} engine holds of `turn` in 1 s, {fired} retries fired"
    );
    assert!(
        waiting < u64::from(W) / 4,
        "{waiting} engine holds in 1 s with {W} budget waiters and no permit \
         granted — waiters come back to the engine on every tick"
    );
    assert_eq!(fired, 0, "budget waiters gave up and rescheduled");

    // Grant `G` permits: holds follow the permit count, not the waiter count.
    let base = sdk.order().len();
    let before = stats.snapshot().holds[turn];
    sdk.gate.add_permits(G + 2);
    wait_until(3 * TIMEOUT, || sdk.order().len() >= base + G).await?;
    let granted = stats.snapshot().holds[turn] - before;
    eprintln!("{granted} holds of `turn` for {G} granted permits");
    assert!(
        granted < 10 * G as u64,
        "{granted} engine holds for {G} permits — not proportional to the permits granted"
    );

    drain_all(&rt, &sdk, W, &[]).await?;
    rt.shutdown().await?;
    Ok(())
}

/// **No task for an invocation that has not been granted a permit.**
///
/// Interleaving:
/// 1. As step 1 of [`budget_waiter_neither_grabs_engine_nor_reschedules`], with `W`
///    larger than the scheduler's intake cap.
/// 2. New leader, default permit wait cap (10 s — formerly a task waited the whole
///    slice): `recover` hands the whole line over on the background path; the first
///    enters the SDK.
/// 3. The line stands still: read the scheduler snapshot (`EngineHandle::flow`).
/// 4. Open the gate fully.
///
/// Must see: (3) `W` waiters in the scheduler's line, at most 4 tasks running
/// background turns (the first is inside the SDK) — formerly: `W + 1` tasks, each
/// ~8.4 KB (FL-28); (4) nobody lost.
#[tokio::test]
async fn permit_waiter_is_not_a_task() -> Result<()> {
    const W: u16 = 300;
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-no-task-per-waiter")?;
    let book = rules(&[("acme", 1)]);
    build_queue(&sdk, &endpoint, &root, Some(book.clone()), W, |_| {
        work(Some("acme"))
    })
    .await?;

    let registry = Registry::new();
    let rt = spawn_runtime_with(&root, registry.clone(), TurnLimits::default())?;
    rt.set_rules(book).await?;
    registry.upsert(deployment(endpoint));
    rt.deployments_changed().await?;
    wait_until(TIMEOUT, || sdk.order().len() >= 2).await?;
    // The line is stable: every waiter has entered the scheduler.
    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        let s = rt.flow().await?.scheduler;
        if s.waiting >= usize::from(W) {
            break;
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "only {} / {W} waiters entered the scheduler",
            s.waiting
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let s = rt.flow().await?.scheduler;
    eprintln!(
        "W = {W} — {} waiters, {} tasks running background turns",
        s.waiting, s.turn_tasks
    );
    assert_eq!(s.waiting, usize::from(W), "waiters in the line");
    assert!(
        s.turn_tasks <= 4,
        "{} tasks running background turns for {W} permit waiters and one running turn — one \
         task per waiter",
        s.turn_tasks
    );

    drain_all(&rt, &sdk, W, &[]).await?;
    rt.shutdown().await?;
    Ok(())
}

/// **Cancelling an invocation waiting for a VO lock does not wait for its turn.**
///
/// Interleaving:
/// 1. First leader: `wide_id(0)` holds the lock of object `k` and stands still in the
///    SDK; `W` more join the same object's line. It steps down.
/// 2. New leader (permit wait slice 300 ms): `recover` hands the whole line over on
///    the background path; the first wins the VO lock and enters the SDK; the other
///    `W` wait for the VO lock — formerly: inside a task, **holding the invocation
///    lock**, never giving up.
/// 3. Admin cancel of `wide_id(2)` (needs its invocation lock).
/// 4. Open the gate fully.
///
/// Must see: (3) the cancel returns `Cancelled` within 2 s — a parked ticket does not
/// hold the invocation lock; (4) everybody else finishes, `wide_id(2)` is `failed` and
/// never enters the SDK.
#[tokio::test]
async fn cancelled_object_lock_waiter_does_not_wait_for_its_turn() -> Result<()> {
    const W: u16 = 5;
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-cancel-parked")?;
    build_queue(&sdk, &endpoint, &root, None, W, |_| cart(None, "k")).await?;

    let registry = Registry::new();
    let rt = spawn_runtime_with(
        &root,
        registry.clone(),
        limits(Some(Duration::from_millis(300))),
    )?;
    registry.upsert(deployment(endpoint));
    rt.deployments_changed().await?;
    wait_until(TIMEOUT, || sdk.order().len() >= 2).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let cancelled = tokio::time::timeout(Duration::from_secs(2), rt.cancel(wide_id(2))).await;
    assert!(
        matches!(cancelled, Ok(Ok(cairn_engine::CancelOutcome::Cancelled))),
        "cancelling a VO lock waiter must answer at once: {cancelled:?}"
    );

    let entered = sdk.order().len();
    drain_all(&rt, &sdk, W, &[2]).await?;
    assert!(matches!(
        rt.status(wide_id(2)).await?,
        Some(InvocationStatus::Failed { .. })
    ));
    assert_eq!(
        sdk.order().len(),
        entered + usize::from(W) - 1,
        "exactly one turn per remaining waiter, none for the cancelled one"
    );
    rt.shutdown().await?;
    Ok(())
}

/// **One entry per invocation in the line: a turn with a caller replaces the parked
/// ticket of the same invocation.**
///
/// Interleaving:
/// 1. First leader: `wide_id(0)` holds `acme`'s only permit and stands still in the
///    SDK; `wide_id(1)` joins the line (`/send`). It steps down.
/// 2. New leader: `recover` hands `wide_id(1)` over on the background path; the first
///    enters the SDK, `wide_id(1)` parks a ticket (budget).
/// 3. An external CANCEL signal to `wide_id(1)` (`cancel_signal`): it wins the
///    invocation lock (the ticket does not hold it), delivers the signal, then asks
///    for a permit — **withdrawing the parked ticket** (`Scheduler::adopt`) and
///    waiting in exactly its place.
/// 4. Open the gate.
///
/// Must see: (3) the scheduler's line has **one** waiter, no ticket; (4)
/// `cancel_signal` answers, `wide_id(1)` finishes after **exactly one** turn in the
/// SDK on the new leader — no second turn from the old ticket, no two tasks waiting
/// for each other.
#[tokio::test]
async fn turn_with_caller_replaces_parked_ticket_of_same_invocation() -> Result<()> {
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-adopt-ticket")?;
    let book = rules(&[("acme", 1)]);
    build_queue(&sdk, &endpoint, &root, Some(book.clone()), 1, |_| {
        work(Some("acme"))
    })
    .await?;

    let registry = Registry::new();
    let rt = spawn_runtime_with(&root, registry.clone(), limits(None))?;
    rt.set_rules(book).await?;
    registry.upsert(deployment(endpoint));
    rt.deployments_changed().await?;
    wait_until(TIMEOUT, || sdk.order().len() >= 2).await?;
    let deadline = std::time::Instant::now() + TIMEOUT;
    while rt.flow().await?.scheduler.parked != 1 {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "wide_id(1)'s ticket not parked yet"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let h = rt.clone();
    let signal = tokio::spawn(async move { h.cancel_signal(wide_id(1)).await });
    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        let s = rt.flow().await?.scheduler;
        if s.parked == 0 && s.waiting == 1 {
            break;
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the signal turn has not replaced the ticket: {} tickets, {} waiters",
            s.parked,
            s.waiting
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let entered = sdk.order().len();
    sdk.gate.add_permits(16);
    tokio::time::timeout(TIMEOUT, signal).await???;
    drain_all(&rt, &sdk, 1, &[]).await?;
    assert_eq!(
        sdk.order().len(),
        entered + 1,
        "exactly one turn of wide_id(1) on the new leader: {:?}",
        sdk.order()
    );
    rt.shutdown().await?;
    Ok(())
}

/// **A parked ticket with a retry is not moved by the background tick every time it
/// comes due.**
///
/// Interleaving:
/// 1. First leader: the first turn of `id(1..=N)` fails retryably — one retry per
///    invocation ([`retry_schedule_rows`]).
/// 2. New leader, rule `acme = 1`. Tick A far past every due time: every retry fires,
///    the next retry is set ~200 ms later; one turn gets the permit (enters the SDK,
///    gate closed), the other `N − 1` park tickets.
/// 3. Tick B, 60 s (fake) later: the next retry of every ticket is due, the
///    invocation is busy (a ticket) — the tick moves the row.
/// 4. Five ticks C, each 5 s (fake) after the previous one — past the defer cap of an
///    ordinary busy invocation (`busy_defer_ms`, 1 s here).
/// 5. Open the gate.
///
/// Must see: (3) ≥ `N / 2` busy rows; (4) fewer than `N / 2` moves over all five
/// ticks — a ticket's row is moved far once (the ticket's turn rewrites it when
/// granted), not on every due time; (5) every invocation finishes. Before the fix:
/// each tick C moved all `N − 1` rows again — one commit per ticket per
/// `busy_defer_ms`, proportional to the number of tickets.
#[tokio::test]
async fn parked_ticket_with_schedule_not_rescheduled_every_tick() -> Result<()> {
    use cairn_server::metrics::flow::RETRY_BUSY;
    const N: u8 = 200;

    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-parked-defer")?;
    retry_schedule_rows(&sdk, &root, deployment(endpoint.clone()), 1..=N).await?;

    let registry = Registry::new();
    let rt = spawn_runtime_with(
        &root,
        registry.clone(),
        limits(Some(Duration::from_millis(800))),
    )?;
    rt.set_rules(rules(&[("acme", 1)])).await?;
    registry.upsert(deployment(endpoint));
    rt.deployments_changed().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let base = now_ms() + 600_000;
    let fired = rt.tick(base).await?;
    assert!(
        fired >= usize::from(N) / 2,
        "tick A fired only {fired} retries"
    );
    let deadline = std::time::Instant::now() + TIMEOUT;
    while rt.flow().await?.scheduler.parked < usize::from(N) - 1 {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "not all tickets parked: {}",
            rt.flow().await?.scheduler.parked
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let before = RETRY_BUSY.load(Ordering::Relaxed);
    rt.tick(base + 60_000).await?;
    let first = RETRY_BUSY.load(Ordering::Relaxed) - before;
    assert!(
        first >= u64::from(N) / 2,
        "scenario not built: only {first} busy rows at tick B"
    );

    let before = RETRY_BUSY.load(Ordering::Relaxed);
    for k in 1..=5u64 {
        rt.tick(base + 60_000 + k * 5_000).await?;
    }
    let again = RETRY_BUSY.load(Ordering::Relaxed) - before;
    eprintln!("N = {N} tickets with a retry — tick B moved {first}, 5 later ticks moved {again}");
    assert!(
        again < u64::from(N) / 2,
        "five ticks moved {again} parked-ticket rows again — a commit per ticket per due time"
    );

    sdk.gate.add_permits(4 * usize::from(N) + 4);
    let deadline = std::time::Instant::now() + 3 * TIMEOUT;
    for n in 1..=N {
        loop {
            if matches!(
                rt.status(id(n)).await?,
                Some(InvocationStatus::Completed(_))
            ) {
                break;
            }
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "{} not finished: {:?}",
                hex(&id(n)),
                rt.status(id(n)).await?
            );
            rt.tick(now_ms() + 1_200_000).await?;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    rt.shutdown().await?;
    Ok(())
}

/// Invocation `id` has ended (finished or failed — a cooperative cancel ends `failed`).
async fn is_finished(rt: &EngineHandle, id: [u8; 24]) -> Result<bool> {
    Ok(matches!(
        rt.status(id).await?,
        Some(InvocationStatus::Completed(_) | InvocationStatus::Failed { .. })
    ))
}

/// **An external signal to an invocation queued on a hot object.** The integration
/// version of `flow::tests::ticket::{a_signal_adopting_a_granted_ticket_takes_its_permit,
/// a_nacked_exclusive_ticket_keeps_its_object_lock}`.
///
/// Interleaving (each object `o` a separate EXCLUSIVE `Cart`, with its own gate):
/// 1. First leader: W runs and stands in the SDK — holding VO lock `K`; A then B are
///    accepted sequentially (`submit`). It steps down.
/// 2. New leader: the lock rebuild gives `K` to W, `recover` hands all three over
///    through `Runner` — W re-enters the SDK (holds the lock ⇒ granted at once), A and
///    B **park tickets** behind `K`.
/// 3. One object at a time: open the gate for W; with a small offset different for
///    each object, send a cooperative cancel signal to A (`cancel_signal`: wins A's
///    invocation lock, then asks for a permit). W's release grants `K` to A's ticket;
///    depending on scheduling, the signal task asks for a permit **after** A's ticket
///    was granted but before its turn won the lock (a second permit for A), or the
///    task of A's ticket meets a busy invocation lock and nacks.
/// 4. Open the gate one permit at a time, only when exactly one turn of the object is
///    inside the SDK, until all three have ended.
///
/// Must see, on every object: never two of its turns inside the SDK at once, and A's
/// first turn on the new leader enters the SDK before B's first turn (FIFO on one
/// object key). Before the first fix: a second permit for A, the nack of A's ticket
/// released `K` ⇒ B ran in parallel with A. Before the second fix: the nack of A's
/// ticket released `K` ⇒ B ran before A.
///
/// **Limit:** the window depends on the `LocalSet`'s scheduling; on the code before
/// the fixes this test does **not** go red (3 × 24 objects, offsets 0–6 ms) — in this
/// harness the turn of a granted ticket always polls before the signal task gets to ask
/// for a permit. The deterministic red evidence of both races is the `Scheduler` unit
/// tests and M4; this test guards the contract (no parallelism, FIFO) under external
/// signals at the integration level.
#[tokio::test]
async fn signal_to_queued_invocation_does_not_run_concurrently_and_keeps_order() -> Result<()> {
    const OBJECTS: u8 = 24;
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-signal-queued")?;
    let ids = |o: u8| (id(3 * o), id(3 * o + 1), id(3 * o + 2));
    let input = |o: u8, s: &str| Bytes::from(format!("o{o}-{s}"));
    let gates: Vec<ObjGate> = (0..OBJECTS).map(|o| sdk.object(&format!("o{o}"))).collect();

    let rt = spawn_runtime(&root, endpoint.clone())?;
    restored(&rt).await?;
    for o in 0..OBJECTS {
        let (w, a, b) = ids(o);
        let key = format!("hot{o}");
        let h = rt.clone();
        let (k, inw) = (key.clone(), input(o, "w"));
        tokio::spawn(async move { h.invoke(w, cart(None, &k), None, None, inw).await });
        wait_until(TIMEOUT, || {
            gates[usize::from(o)].inside.load(Ordering::SeqCst) == 1
        })
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "W of object {o} did not enter the SDK ({e}): order {:?}, inside SDK {}",
                sdk.order(),
                sdk.inside()
            )
        })?;
        for (x, s) in [(a, "a"), (b, "b")] {
            let out = rt
                .submit(x, cart(None, &key), None, None, input(o, s))
                .await?;
            assert!(matches!(out, InvokeOutcome::Waiting), "{out:?}");
        }
    }
    rt.shutdown().await?;
    wait_until(TIMEOUT, || sdk.inside() == 0)
        .await
        .map_err(|e| anyhow::anyhow!("old leader's turns still in SDK ({}): {e}", sdk.inside()))?;

    let rt = spawn_runtime(&root, endpoint)?;
    restored(&rt).await?;
    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        let f = rt.flow().await?.scheduler;
        if f.parked == 2 * usize::from(OBJECTS) && sdk.inside() == usize::from(OBJECTS) {
            break;
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "new leader has not rebuilt the line: {} tickets, {} turns in the SDK",
            f.parked,
            sdk.inside()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let mark = sdk.order().len();
    for g in &gates {
        g.peak
            .store(g.inside.load(Ordering::SeqCst), Ordering::SeqCst);
    }

    let (mut concurrent, mut out_of_order) = (Vec::new(), Vec::new());
    for o in 0..OBJECTS {
        let (w, a, b) = ids(o);
        let g = &gates[usize::from(o)];
        g.gate.add_permits(1);
        tokio::time::sleep(Duration::from_micros(u64::from(o) * 250)).await;
        let h = rt.clone();
        let signal = tokio::spawn(async move { h.cancel_signal(a).await });
        let deadline = std::time::Instant::now() + 3 * TIMEOUT;
        loop {
            if is_finished(&rt, w).await?
                && is_finished(&rt, a).await?
                && is_finished(&rt, b).await?
            {
                break;
            }
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "object {o}: not all three ended"
            );
            if g.inside.load(Ordering::SeqCst) == 1 && g.gate.available_permits() == 0 {
                g.gate.add_permits(1);
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let _ = tokio::time::timeout(TIMEOUT, signal).await;
        if g.peak.load(Ordering::SeqCst) > 1 {
            concurrent.push(o);
        }
        let order = sdk.order();
        let first = |s: &str| {
            order[mark..]
                .iter()
                .position(|x| *x == format!("o{o}-{s}"))
                .unwrap_or(usize::MAX)
        };
        if first("b") < first("a") {
            out_of_order.push(o);
        }
    }
    eprintln!(
        "signal races: {OBJECTS} objects, parallel at {concurrent:?}, out of order at {out_of_order:?}"
    );
    anyhow::ensure!(
        concurrent.is_empty() && out_of_order.is_empty(),
        "two turns of one object inside the SDK at {concurrent:?}; B ran before A at {out_of_order:?}"
    );
    rt.shutdown().await?;
    Ok(())
}

/// The number of retry index rows left on the store of `root` (range 0) — read directly
/// from the Db after the runtime has shut down.
async fn read_retry_index(root: &Path) -> Result<Vec<(u64, [u8; 24])>> {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(root)?);
    let db = Db::builder("partition-0", store).build().await?;
    let out = cairn_store::testkit::retry_index_rows(&db).await?;
    db.close().await?;
    Ok(out)
}

/// **The `PermitGranted` of a turn with a pre-set retry deletes the retry index row at
/// the row's real due time.**
///
/// Interleaving:
/// 1. First leader: the first turn of `id(1..=3)` fails retryably — one retry per
///    invocation ([`retry_schedule_rows`]).
/// 2. New leader, rule `acme = 1`. Tick A: every retry fires (the next retry set in
///    advance), one turn enters the SDK (gate closed), the other two park tickets.
/// 3. Tick B, 60 s (fake) later: the next retry of the two tickets is due, the
///    invocation is busy (a ticket) ⇒ the background tick moves the row far into a
///    watchdog row — the entity row **and** the index row move to the new due time.
/// 4. Open the gate, drain everything: each ticket is granted, the `Running` commit
///    rewrites the row.
///
/// Must see: once every invocation has finished, no retry index row with a **far**
/// due time (more than an hour) is left on the store. Rows off by a near due time are
/// by design: the end commit drops the entity row without reading its due time
/// (`Mutations::drop_retry`), the background tick cleans the index row when it comes
/// due. Before the fix: the `Running` commit of a `planned` turn deleted the index row
/// at the fire's `next_due` (already moved), and the index row at the far-moved due
/// time (one day) stayed — one stray row per ticket, the whole cluster firing at once
/// exactly one day later.
#[tokio::test]
async fn index_row_of_postponed_retry_deleted_on_running() -> Result<()> {
    const N: u8 = 3;
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-stale-retry-index")?;
    retry_schedule_rows(&sdk, &root, deployment(endpoint.clone()), 1..=N).await?;

    let registry = Registry::new();
    let rt = spawn_runtime_with(&root, registry.clone(), limits(None))?;
    rt.set_rules(rules(&[("acme", 1)])).await?;
    registry.upsert(deployment(endpoint));
    rt.deployments_changed().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let base = now_ms() + 600_000;
    rt.tick(base).await?;
    let deadline = std::time::Instant::now() + TIMEOUT;
    while rt.flow().await?.scheduler.parked < usize::from(N) - 1 {
        anyhow::ensure!(std::time::Instant::now() < deadline, "tickets not parked");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    rt.tick(base + 60_000).await?;

    sdk.gate.add_permits(4 * usize::from(N) + 4);
    let deadline = std::time::Instant::now() + 3 * TIMEOUT;
    for n in 1..=N {
        while !matches!(
            rt.status(id(n)).await?,
            Some(InvocationStatus::Completed(_))
        ) {
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "{} not finished",
                hex(&id(n))
            );
            rt.tick(base + 61_000).await?;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    rt.shutdown().await?;
    let far = now_ms() + 3_600_000;
    let left: Vec<(u64, [u8; 24])> = read_retry_index(&root)
        .await?
        .into_iter()
        .filter(|(due, _)| *due > far)
        .collect();
    anyhow::ensure!(
        left.is_empty(),
        "retry index rows with a far stray due time left after every invocation finished: {:?}",
        left.iter()
            .map(|(due, id)| (*due, hex(id)))
            .collect::<Vec<_>>()
    );
    Ok(())
}

/// **A `recover` superseded at every chunk boundary still reaches the tail of the
/// index.** The integration version of
/// `actor::background::tests::superseded_recover_inherits_its_cursor`.
///
/// Interleaving:
/// 1. First leader: `N` `Work` invocations (unscoped) run and stand in the SDK or
///    wait for a permit; it steps down — in the store all `N` are still `running`.
/// 2. New leader, a reconciliation chunk of **one** index row
///    (`TurnLimits::recover_chunk`), and a task sending `DeploymentsChanged`
///    continuously: each command schedules a new `recover`, the scan in progress stops
///    at the next chunk boundary.
///
/// Must see: within the cap, all `N` invocations are dispatched again (entering the
/// SDK or parking a ticket behind the turn limit). Before the fix: each new scan
/// restarted from the beginning — only the first few index ids were dispatched while
/// the registry kept changing, the tail waited until the registry stopped changing.
#[tokio::test]
async fn recover_superseded_repeatedly_still_reaches_end_of_index() -> Result<()> {
    const N: u8 = 40;
    let sdk = GateSdk::new();
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-recover-superseded")?;
    let rt = spawn_runtime(&root, endpoint.clone())?;
    restored(&rt).await?;
    for n in 0..N {
        let out = rt
            .submit(id(n), work(None), None, None, Bytes::from(format!("r{n}")))
            .await?;
        assert!(matches!(out, InvokeOutcome::Waiting), "{out:?}");
    }
    wait_until(TIMEOUT, || sdk.inside() >= 8).await?;
    rt.shutdown().await?;
    wait_until(TIMEOUT, || sdk.inside() == 0).await?;

    let mark = sdk.order().len();
    let rt = spawn_runtime_with(
        &root,
        registry_for(endpoint),
        TurnLimits {
            recover_chunk: 1,
            ..limits(None)
        },
    )?;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hammer = {
        let (rt, stop) = (rt.clone(), Arc::clone(&stop));
        tokio::spawn(async move {
            while !stop.load(Ordering::SeqCst) {
                let _ = rt.deployments_changed().await;
            }
        })
    };
    // Dispatched again on the new leader = entered the SDK (gate closed) or parked a ticket behind the turn limit.
    let deadline = std::time::Instant::now() + TIMEOUT;
    let delivered = loop {
        let entered = sdk.order()[mark..]
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len();
        let n = entered + rt.flow().await?.scheduler.parked;
        if n >= usize::from(N) || std::time::Instant::now() > deadline {
            break n;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    stop.store(true, Ordering::SeqCst);
    let _ = hammer.await;
    eprintln!("recover cursor: {delivered}/{N} invocations dispatched again while the registry kept changing");
    anyhow::ensure!(
        delivered >= usize::from(N),
        "only {delivered}/{N} invocations dispatched again — the index tail was not reached"
    );
    sdk.gate.add_permits(4 * usize::from(N));
    for n in 0..N {
        wait_finished(&rt, id(n)).await?;
    }
    rt.shutdown().await?;
    Ok(())
}

/// **A frame from the SDK larger than the frame cap is a permanent error** (the
/// runtime part). The SDK returns a 16 MiB + 1 KiB `Output` frame (`ctx.run` or an
/// output that is too large, the SDK does not check it itself). A retry only reruns
/// the same code — side effects repeat — and meets exactly the same error.
///
/// Must see: the invocation `paused` with code 571 (`PROTOCOL_VIOLATION`) after
/// **exactly one** SDK call, and no further call. Before the fix: `FrameTooLarge` was
/// `InvokeError::Protocol` ⇒ an infrastructure failure of the turn ⇒ rescheduled with
/// backoff, one SDK turn each time.
#[tokio::test]
async fn oversize_frame_from_sdk_stops_invocation_without_retry() -> Result<()> {
    let sdk = GateSdk::new().open();
    sdk.oversized.lock().unwrap().insert("huge".into());
    let endpoint = spawn_sdk(sdk.clone()).await?;
    let root = fresh_root("flow-oversized-frame")?;
    let rt = spawn_runtime(&root, endpoint)?;
    restored(&rt).await?;
    let out = rt
        .submit(id(1), work(None), None, None, Bytes::from_static(b"huge"))
        .await?;
    assert!(matches!(out, InvokeOutcome::Waiting), "{out:?}");
    let deadline = std::time::Instant::now() + TIMEOUT;
    let status = loop {
        let st = rt.status(id(1)).await?;
        if matches!(st, Some(InvocationStatus::Paused { .. }))
            || std::time::Instant::now() > deadline
        {
            break st;
        }
        rt.tick(now_ms() + 600_000).await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    // The background tick keeps running: no further call.
    for _ in 0..10 {
        rt.tick(now_ms() + 3_600_000).await?;
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let calls = sdk.order().iter().filter(|o| *o == "huge").count();
    rt.shutdown().await?;
    anyhow::ensure!(
        matches!(status, Some(InvocationStatus::Paused { code: 571, .. })) && calls == 1,
        "frame over the cap: status {status:?}, {calls} SDK calls (expected paused 571, 1 call)"
    );
    Ok(())
}
