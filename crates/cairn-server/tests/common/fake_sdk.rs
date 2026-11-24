// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A scripted fake SDK, `REQUEST_RESPONSE` mode.
//!
//! Each handler is a **sequence of steps** ([`Step`]). On every turn the runtime
//! calls down, the fake SDK rebuilds its position from the replayed journal —
//! counting the commands recorded, gathering the completions that have arrived —
//! then runs on from there up to the first step that must wait, and ends the turn
//! with `Suspension` (`REQUEST_RESPONSE` has no channel to wait on mid-turn). The
//! wire mould is taken from `end_to_end.rs`.
//!
//! Two things for tests to inspect and control:
//!
//! - **The `Start` log**: every `Start` received, in order — id, debug id, random
//!   seed, key. Enough to check "which turn ran, for whom, with which seed".
//! - **The gate** ([`Gate`]): a handler marked `gated` must take a permit before
//!   answering, and the permit is **consumed** — `release(n)` lets exactly `n` turns
//!   through. That is how a turn is held in flight while the test does something else.

use {
    anyhow::Result,
    axum::{
        extract::{Path as AxPath, State},
        http::{header, HeaderMap, StatusCode},
        routing::{get, post},
        Router,
    },
    bytes::Bytes,
    cairn_protocol::{pb, Message},
    cairn_server::{
        discovery::{
            EndpointManifest, HandlerManifest, HandlerType, ProtocolMode, ServiceManifest,
            ServiceType, DISCOVERY_V1,
        },
        invoker::{decode_stream, encode_stream, INVOCATION_V1},
    },
    std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    },
    tokio::{sync::Semaphore, task::JoinHandle},
};

/// The signal name [`Step::Suspend`] waits for. Nobody sends it unless the test wants to.
pub const HOLD_SIGNAL: &str = "fake-sdk-hold";

/// A cell of refs `(sha, len, gen)` for [`Step::SetStateRefs`], read when the command is issued.
pub type RefSlot = Arc<Mutex<Vec<([u8; 32], u64, u64)>>>;

/// One step in a handler's script.
#[derive(Clone, Debug)]
pub enum Step {
    /// Return a result and finish the invocation.
    Output(Bytes),
    /// Return exactly the result of the latest `Call` (to check the completion came
    /// back to the right caller), then finish.
    EchoCallResult,
    /// `ctx.call` and **wait for the result**.
    Call {
        service: String,
        handler: String,
        key: Option<String>,
        idempotency_key: Option<String>,
        /// `placement: "spread"`.
        spread: bool,
    },
    /// Like [`Step::Call`] (optional key, waits for the result), the call carrying
    /// parameter `parameter` — a cross-range `StartCall` of the parameter's size.
    CallWith {
        service: String,
        handler: String,
        key: Option<String>,
        parameter: Bytes,
    },
    /// `ctx.sleep(ms)` counted from when the command was first issued.
    Sleep(u64),
    /// Sleep until the absolute mark `ms` (epoch) — every invocation of the handler
    /// has the same deadline, whenever it was created (a timer storm).
    SleepUntil(u64),
    /// `ctx.call` (unkeyed) **without waiting**: issue the command and move on — the
    /// shape of `const p = ctx.call(...); await ctx.sleep(...); await p`. The result
    /// is awaited in [`Step::AwaitSpawned`].
    Spawn {
        service: String,
        handler: String,
        /// `placement: "spread"`.
        spread: bool,
    },
    /// `ctx.call` **without waiting**, optional key: issue the command and move on,
    /// not kept for [`Step::AwaitSpawned`] — the shape of `Promise.all` at issue time.
    /// Several `Fire`s in a row are several outbox `Call` rows of the same turn.
    Fire {
        service: String,
        handler: String,
        key: Option<String>,
    },
    /// `ctx.call(service, handler, x, { tags })` unkeyed, **waiting** for the
    /// result.
    TaggedCall {
        service: String,
        handler: String,
        tags: Vec<(String, String)>,
    },
    /// `ctx.send(service, handler, x, { tags })` — one-way, moves on at once.
    Send {
        service: String,
        handler: String,
        tags: Vec<(String, String)>,
    },
    /// Wait for the result of the latest [`Step::Spawn`], then keep it for
    /// [`Step::EchoCallResult`].
    AwaitSpawned,
    /// Wait for the results of **every** [`Step::Fire`] before it — `ctx.all` over
    /// the issued calls: suspends with an `ALL_COMPLETED` tree over the result
    /// completions not yet arrived.
    AwaitFired,
    /// Suspend waiting for the signal [`HOLD_SIGNAL`] — the invocation lies still in
    /// `suspended`; once that signal arrives (through `SendSignal` or an external
    /// signal) it moves on.
    Suspend,
    /// Stand still for `ms` milliseconds mid-turn, sending nothing — the shape of a
    /// long `ctx.run` (an LLM call, a slow API) seen from the runtime.
    Stall(u64),
    /// Fail **retryably** (`ErrorMessage`, behavior `RETRY`) — the turn ends with an
    /// error, the runtime schedules the next attempt per its backoff.
    FailRetry,
    /// Like [`Step::FailRetry`] while `ctx.attempt` (`Start`'s
    /// `retry_count_since_last_stored_entry`) is below `n`; from attempt `n` on it
    /// moves on — a handler that fails `n` times then recovers. Not a journal command.
    FailBelow(u32),
    /// Like [`Step::FailBelow`], the error carrying `next_retry_delay` = `.1` ms — the
    /// next attempt is scheduled exactly that far ahead (the bench of a queue with retry rows).
    FailBelowDelay(u32, u64),
    /// `ctx.set(key, value)` — write a state row (block region). Not completable:
    /// moves on at once.
    SetState { key: String, value: Bytes },
    /// A **lazy** `ctx.get(key)` (`GetState`, waits for the completion), keeping the
    /// value for [`Step::EchoCallResult`]. A completion carrying `Value.segments` ⇒
    /// the fake SDK returns 400: an SDK ≤ v5 cannot read a segmented value.
    GetState { key: String },
    /// Like [`Step::SetState`], but a v6 SDK **reuses** the refs of the segmented
    /// value it just read in [`Step::GetState`] for the same key, within the same
    /// request: `value` starts with the bytes of `refs[0..k]` ⇒ send
    /// `Value{segments: refs[0..k], content: the rest}`. No known ref ⇒ inline.
    SetStateReuse { key: String, value: Bytes },
    /// `SetState` carrying a **made-up** ref (random sha) before `value` — a broken or
    /// malicious v6 SDK: the engine must refuse, writing nothing.
    SetStateForged { key: String, value: Bytes },
    /// `SetState` (v6 SDK) carrying exactly the refs `(sha, len, gen)` in the cell,
    /// read when the command is issued, then `tail` — a malicious SDK copying someone
    /// else's refs (same range). The engine must refuse as for a dangling ref; the
    /// shell must not read that ref's bytes.
    SetStateRefs {
        key: String,
        refs: RefSlot,
        tail: Bytes,
    },
    /// `ctx.set(key, old ‖ chunk)` — the append pattern of a chat history. `old` is
    /// the value read in [`Step::GetState`] for the same key or by the previous
    /// `AppendState` in the turn (absent ⇒ empty). A v6 SDK reuses refs like
    /// [`Step::SetStateReuse`] and, like the TS SDK (`handler.ts`), **remembers** the
    /// `refs[0..k]` just sent for the next `set` in the same turn.
    AppendState { key: String, chunk: Bytes },
    /// `ctx.signal(HOLD_SIGNAL)` sent to the hex id in the cell — the cell is read
    /// when the command is issued, so a test can set the target *after* building the
    /// script (the target id only exists once the target invocation has been
    /// created). Not completable: moves on at once.
    SendSignal(Arc<Mutex<String>>),
    /// `ctx.attach(id)` to the hex id in the cell (read when the command is issued,
    /// like [`Step::SendSignal`]) and **wait for the result**; the result goes into
    /// the turn's log ([`Step::OutputResults`]).
    Attach(Arc<Mutex<String>>),
    /// `ctx.getOutput(id)` (a look without waiting) to the hex id in the cell; the
    /// result goes into the turn's log.
    GetOutput(Arc<Mutex<String>>),
    /// `ctx.signal(idx)` — a **numbered** signal to the hex id in the cell: `1` is
    /// CANCEL, `17+` are awakeables. Not completable: moves on at once.
    SignalIdx(Arc<Mutex<String>>, u32),
    /// Return every [`Step::Attach`]/[`Step::GetOutput`] result that has arrived, one
    /// per line (`value:<content>`, `failure:<code>:<message>`, `void`), then finish.
    OutputResults,
    /// `ctx.continueAsNew(input)` (wire v3) — the **first** time the handler reaches
    /// this step (a flag shared by every invocation of the handler); later times move
    /// on, so the new run does not repeat forever.
    ContinueAsNewOnce(Arc<std::sync::atomic::AtomicBool>, Bytes),
    /// `ctx.run(() => v)`: `Run` then `ProposeRunCompletion` carrying `v`, suspending
    /// on its `RunCompletion` (RR has no mid-turn ack); the next turn sees the
    /// completion in the journal and moves on, keeping the result for
    /// [`Step::EchoCallResult`].
    Run(Bytes),
    /// Like [`Step::Run`], but the closure **fails retryably** while `ctx.attempt` is
    /// below `n`: `Run` is recorded, no result is proposed, the turn ends with an
    /// `Error` with behavior `RETRY` — the shape of a `ctx.run` throwing a transient error.
    RunFailBelow(u32, Bytes),
}

impl Step {
    pub fn output(v: &str) -> Self {
        Self::Output(Bytes::copy_from_slice(v.as_bytes()))
    }

    pub fn fire(service: &str, handler: &str, key: Option<&str>) -> Self {
        Self::Fire {
            service: service.into(),
            handler: handler.into(),
            key: key.map(Into::into),
        }
    }

    pub fn call(service: &str, handler: &str, key: Option<&str>) -> Self {
        Self::Call {
            service: service.into(),
            handler: handler.into(),
            key: key.map(Into::into),
            idempotency_key: None,
            spread: false,
        }
    }

    /// `ctx.call(service, handler, x, { placement: "spread" })` — unkeyed, waits for
    /// the result.
    pub fn call_spread(service: &str, handler: &str) -> Self {
        Self::Call {
            service: service.into(),
            handler: handler.into(),
            key: None,
            idempotency_key: None,
            spread: true,
        }
    }
}

fn placement_of(spread: bool) -> i32 {
    if spread {
        pb::CallPlacement::Spread as i32
    } else {
        pb::CallPlacement::Default as i32
    }
}

/// A `Start` the fake SDK has received.
#[derive(Clone, Debug)]
pub struct StartRecord {
    pub service: String,
    pub handler: String,
    pub id: Bytes,
    pub debug_id: String,
    pub random_seed: u64,
    pub key: String,
    /// `Start`'s `retry_count_since_last_stored_entry` — what a real SDK exposes as
    /// `ctx.attempt`.
    pub attempt: u32,
    /// The wall time when the fake SDK received `Start` — the same clock as
    /// `Step::Sleep`'s `wake_up_time`, to measure how late a timer fired against its deadline.
    pub at: std::time::SystemTime,
    /// The object's eager state sent with `Start` (`state_map`): `(key, value)`.
    pub state: Vec<(Bytes, Bytes)>,
}

impl StartRecord {
    /// `id` in hex — exactly the form the HTTP API takes at `/invocations/{id}`.
    pub fn id_hex(&self) -> String {
        self.id.iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// The gate holding turns. See the module docs.
#[derive(Clone)]
pub struct Gate(Arc<Semaphore>);

impl Gate {
    /// Let exactly `n` more turns through.
    pub fn release(&self, n: usize) {
        self.0.add_permits(n);
    }

    /// Open the gate for good.
    pub fn open(&self) {
        self.0.add_permits(Semaphore::MAX_PERMITS / 2);
    }
}

struct Handler {
    steps: Vec<Step>,
    gated: bool,
    contract: cairn_server::discovery::Contract,
    /// The type declared in the manifest; `None` = the service's default type
    /// (`EXCLUSIVE` for a virtual object, `WORKFLOW` for a workflow).
    ty: Option<HandlerType>,
}

struct Service {
    ty: ServiceType,
    handlers: BTreeMap<String, Handler>,
}

struct Inner {
    /// The maximum wire version the SDK declares (default 1). ≥ 6: reads segmented
    /// values through URLs.
    wire: u32,
    /// Bytes per `/invoke` request: (handler, down to the SDK, up to the runtime).
    traffic: Mutex<Vec<(String, usize, usize)>>,
    declare_bidi: bool,
    services: BTreeMap<String, Service>,
    starts: Mutex<Vec<StartRecord>>,
    /// The number of `Start`s per `(service, handler)` — counted without copying
    /// (benches load millions of invocations: copying `starts` on every poll is O(H)
    /// under the very lock the handler needs to record a `Start`).
    start_counts: Mutex<BTreeMap<(String, String), usize>>,
    gate: Gate,
}

/// Builds a [`FakeSdk`].
#[derive(Default)]
pub struct FakeSdkBuilder {
    services: BTreeMap<String, Service>,
    idle_close: Option<std::time::Duration>,
    /// The manifest declares `BIDI_STREAM` though the server only speaks h1 — an SDK
    /// behind an h1 proxy (Render, Railway, Heroku…). `/invoke` still answers
    /// `REQUEST_RESPONSE`, like the TS SDK's `serve()`.
    declare_bidi: bool,
    /// [`Inner::wire`]; 0 = default 1.
    wire: u32,
}

impl FakeSdkBuilder {
    /// Declare `maxProtocolVersion` = `v` (default 1).
    pub fn wire(mut self, v: u32) -> Self {
        self.wire = v;
        self
    }

    fn add(
        self,
        ty: ServiceType,
        service: &str,
        handler: &str,
        steps: Vec<Step>,
        gated: bool,
    ) -> Self {
        self.add_typed(ty, service, handler, steps, gated, None)
    }

    fn add_typed(
        mut self,
        ty: ServiceType,
        service: &str,
        handler: &str,
        steps: Vec<Step>,
        gated: bool,
        handler_ty: Option<HandlerType>,
    ) -> Self {
        let s = self
            .services
            .entry(service.into())
            .or_insert_with(|| Service {
                ty,
                handlers: BTreeMap::new(),
            });
        assert!(
            s.ty == ty,
            "service {service} is both a Service and a VirtualObject in the script"
        );
        s.handlers.insert(
            handler.into(),
            Handler {
                steps,
                gated,
                contract: Default::default(),
                ty: handler_ty,
            },
        );
        self
    }

    /// Declare a contract for a handler already added.
    pub fn contract(
        mut self,
        service: &str,
        handler: &str,
        contract: cairn_server::discovery::Contract,
    ) -> Self {
        self.services
            .get_mut(service)
            .and_then(|s| s.handlers.get_mut(handler))
            .expect("contract declared for a handler not added yet")
            .contract = contract;
        self
    }

    /// An `EXCLUSIVE` handler of a virtual object (called with `?key=`).
    pub fn object(self, service: &str, handler: &str, steps: Vec<Step>) -> Self {
        self.add(ServiceType::VirtualObject, service, handler, steps, false)
    }

    /// A `SHARED` handler of a virtual object (called with `?key=`): runs without
    /// holding the object lock, in parallel with an `EXCLUSIVE` handler on the same key.
    pub fn shared_object(self, service: &str, handler: &str, steps: Vec<Step>) -> Self {
        self.add_typed(
            ServiceType::VirtualObject,
            service,
            handler,
            steps,
            false,
            Some(HandlerType::Shared),
        )
    }

    /// A workflow's main run (`/invoke/{service}/{handler}?key=…`, then
    /// `/workflow/{service}/{key}/attach`) — one handler per service.
    pub fn workflow(self, service: &str, handler: &str, steps: Vec<Step>) -> Self {
        self.add(ServiceType::Workflow, service, handler, steps, false)
    }

    /// A handler of an unkeyed service.
    pub fn service(self, service: &str, handler: &str, steps: Vec<Step>) -> Self {
        self.add(ServiceType::Service, service, handler, steps, false)
    }

    /// Like [`Self::object`], but every turn must pass the [`Gate`].
    pub fn gated_object(self, service: &str, handler: &str, steps: Vec<Step>) -> Self {
        self.add(ServiceType::VirtualObject, service, handler, steps, true)
    }

    /// Like [`Self::service`], but every turn must pass the [`Gate`].
    pub fn gated_service(self, service: &str, handler: &str, steps: Vec<Step>) -> Self {
        self.add(ServiceType::Service, service, handler, steps, true)
    }

    /// The manifest declares `BIDI_STREAM` (the server still speaks only h1) — see the field.
    pub fn declare_bidi(mut self) -> Self {
        self.declare_bidi = true;
        self
    }

    /// The server **closes idle connections** like `node:http` (`keepAliveTimeout`,
    /// default 5 s), in the worst shape of the race: a request arriving on a connection
    /// idle for ≥ `idle` is dropped and the connection closed, without an answer — like
    /// a server closing the socket just as a request is in flight. The client gets
    /// `connection closed before message completed`. See [`IdleClosing`].
    pub fn idle_close(mut self, idle: std::time::Duration) -> Self {
        self.idle_close = Some(idle);
        self
    }

    pub async fn spawn(self) -> Result<FakeSdk> {
        let inner = Arc::new(Inner {
            wire: self.wire.max(1),
            traffic: Mutex::new(Vec::new()),
            declare_bidi: self.declare_bidi,
            services: self.services,
            starts: Mutex::new(Vec::new()),
            start_counts: Mutex::new(BTreeMap::new()),
            gate: Gate(Arc::new(Semaphore::new(0))),
        });
        let app = Router::new()
            .route("/discover", get(discover))
            .route("/invoke/{service}/{handler}", post(invoke))
            // The journal assembled for an SDK ≤ v5 carries the whole segmented value
            // (a 2 MiB `ctx.run` result) — axum's default 2 MiB cap would block it
            // before the fake SDK gets to read it.
            .layer(axum::extract::DefaultBodyLimit::max(64 << 20))
            .with_state(Arc::clone(&inner));
        // Deterministic simulation: register with the thread's in-process network (if
        // any), under an address the network assigns — OS-chosen ports differ between
        // runs, and the SDK's URI sits in the deployment registry — and do **not** open
        // a real TCP port: stray requests from another process would leak into the
        // sim's runtime (like the node's listener, `node/setup.rs::bind_listeners`).
        #[cfg(feature = "testkit")]
        if let Some((addr, _)) = cairn_server::netfilter::LocalNet::allocate_pair() {
            cairn_server::netfilter::LocalNet::serve(&addr.to_string(), app);
            return Ok(FakeSdk {
                url: format!("http://{addr}"),
                inner,
                server: tokio::spawn(std::future::pending::<()>()),
            });
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = match self.idle_close {
            // BIDI declared behind an "h1 proxy": a connection opening with the h2c preface
            // is closed.
            None if self.declare_bidi => tokio::spawn(async move {
                let _ = axum::serve(H1OnlyListener(listener), app).await;
            }),
            None => tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            }),
            Some(idle) => tokio::spawn(async move {
                let _ = axum::serve(IdleClosingListener { listener, idle }, app).await;
            }),
        };
        Ok(FakeSdk {
            url: format!("http://{addr}"),
            inner,
            server,
        })
    }
}

/// A running fake SDK. Drop ⇒ the server stops.
pub struct FakeSdk {
    pub url: String,
    inner: Arc<Inner>,
    server: JoinHandle<()>,
}

impl Drop for FakeSdk {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl FakeSdk {
    pub fn builder() -> FakeSdkBuilder {
        FakeSdkBuilder::default()
    }

    /// Every `Start` received, in order of arrival.
    pub fn starts(&self) -> Vec<StartRecord> {
        self.inner.starts.lock().unwrap().clone()
    }

    /// The number of `Start`s received — without copying the whole list (benches load
    /// hundreds of thousands).
    pub fn starts_len(&self) -> usize {
        self.inner.starts.lock().unwrap().len()
    }

    /// The number of `Start`s of one handler — no list copy, O(log number of handlers).
    /// Benches' poll loops use this, not `starts_of(..).len()`.
    pub fn starts_count_of(&self, service: &str, handler: &str) -> usize {
        self.inner
            .start_counts
            .lock()
            .unwrap()
            .get(&(service.to_string(), handler.to_string()))
            .copied()
            .unwrap_or(0)
    }

    /// The `Start`s of one handler.
    pub fn starts_of(&self, service: &str, handler: &str) -> Vec<StartRecord> {
        self.starts()
            .into_iter()
            .filter(|s| s.service == service && s.handler == handler)
            .collect()
    }

    /// The bytes of every `/invoke` request to `handler`: (down to the SDK, up to the runtime).
    pub fn traffic_of(&self, handler: &str) -> Vec<(usize, usize)> {
        self.inner
            .traffic
            .lock()
            .unwrap()
            .iter()
            .filter(|(h, _, _)| h == handler)
            .map(|(_, d, u)| (*d, *u))
            .collect()
    }

    /// The gate shared by every `gated` handler.
    pub fn gate(&self) -> Gate {
        self.inner.gate.clone()
    }
}

async fn discover(
    State(st): State<Arc<Inner>>,
) -> ([(header::HeaderName, &'static str); 1], String) {
    let manifest = EndpointManifest {
        protocol_mode: if st.declare_bidi {
            ProtocolMode::BidiStream
        } else {
            ProtocolMode::RequestResponse
        },
        min_protocol_version: 1,
        max_protocol_version: st.wire,
        services: st
            .services
            .iter()
            .map(|(name, s)| ServiceManifest {
                name: name.clone(),
                ty: s.ty,
                handlers: s
                    .handlers
                    .iter()
                    .map(|(h, def)| HandlerManifest {
                        name: h.clone(),
                        ty: def.ty.or(match s.ty {
                            ServiceType::VirtualObject => Some(HandlerType::Exclusive),
                            ServiceType::Workflow => Some(HandlerType::Workflow),
                            ServiceType::Service => None,
                        }),
                        contract: def.contract.clone(),
                    })
                    .collect(),
                contract: Default::default(),
            })
            .collect(),
    };
    (
        [(header::CONTENT_TYPE, DISCOVERY_V1)],
        serde_json::to_string(&manifest).unwrap(),
    )
}

/// The fake SDK's wall time: through `entropy` — outside the sim it is
/// `SystemTime::now()`, inside the sim it is virtual time, the same clock as the node's.
fn now_ms() -> u64 {
    cairn_store::entropy::now_ms()
}

fn fail(status: StatusCode, msg: String) -> (StatusCode, HeaderMap, Bytes) {
    (status, HeaderMap::new(), Bytes::from(msg))
}

/// [`invoke_turn`] with a byte log of the request ([`FakeSdk::traffic_of`]).
async fn invoke(
    State(st): State<Arc<Inner>>,
    AxPath((service, handler)): AxPath<(String, String)>,
    body: Bytes,
) -> (StatusCode, HeaderMap, Bytes) {
    let down = body.len();
    let r = invoke_turn(
        State(Arc::clone(&st)),
        AxPath((service, handler.clone())),
        body,
    )
    .await;
    st.traffic.lock().unwrap().push((handler, down, r.2.len()));
    r
}

/// Assemble a segmented `Value` (v6 SDK): GET each ref through its URL, check the
/// length and the sha256.
async fn fetch_segmented(
    v: &pb::Value,
    urls: &BTreeMap<(Vec<u8>, u64), String>,
) -> Result<Bytes, String> {
    let mut full = Vec::new();
    for r in &v.segments {
        let Some(url) = urls.get(&(r.sha256.to_vec(), r.gen)) else {
            return Err(format!("no URL for segment gen {}", r.gen));
        };
        let resp = reqwest::get(url).await.map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!("segment GET {}", resp.status()));
        }
        let b = resp.bytes().await.map_err(|e| e.to_string())?;
        if b.len() as u64 != r.len || cairn_store::artifact::sha256(&b)[..] != r.sha256[..] {
            return Err("segment bytes do not match the ref".into());
        }
        full.extend_from_slice(&b);
    }
    full.extend_from_slice(&v.content);
    Ok(Bytes::from(full))
}

/// One turn: rebuild the position from the replayed journal, run on up to where it must wait.
async fn invoke_turn(
    State(st): State<Arc<Inner>>,
    AxPath((service, handler)): AxPath<(String, String)>,
    body: Bytes,
) -> (StatusCode, HeaderMap, Bytes) {
    let Some(h) = st
        .services
        .get(&service)
        .and_then(|s| s.handlers.get(&handler))
    else {
        return fail(
            StatusCode::NOT_FOUND,
            format!("the script has no {service}/{handler}"),
        );
    };
    let frames = match decode_stream(body) {
        Ok(f) => f,
        Err(e) => return fail(StatusCode::BAD_REQUEST, format!("broken frames: {e}")),
    };

    // Commands already in the journal (not counting `Input`), and completions that have arrived.
    let mut journaled = 0usize;
    let mut done: BTreeMap<u32, Option<Bytes>> = BTreeMap::new();
    // Results of `Attach`/`GetOutput` that have arrived, by completion id.
    let mut outcomes: BTreeMap<u32, String> = BTreeMap::new();
    // The `HOLD_SIGNAL` signal has arrived ⇒ `Step::Suspend` moves on instead of suspending again.
    let mut held_released = false;
    let mut attempt = 0u32;
    // v6: read URLs by `(sha, gen)` and the refs of segmented completions.
    let mut urls: BTreeMap<(Vec<u8>, u64), String> = BTreeMap::new();
    let mut done_refs: BTreeMap<u32, Vec<pb::SegmentRef>> = BTreeMap::new();
    for f in &frames {
        let m = match cairn_protocol::decode(f) {
            Ok(m) => m,
            Err(e) => return fail(StatusCode::BAD_REQUEST, format!("cannot decode: {e}")),
        };
        match m {
            Message::Start(s) => {
                for u in &s.segment_urls {
                    urls.insert((u.sha256.to_vec(), u.gen), u.url.clone());
                }
                st.starts.lock().unwrap().push(StartRecord {
                    attempt: {
                        attempt = s.retry_count_since_last_stored_entry;
                        attempt
                    },
                    service: service.clone(),
                    handler: handler.clone(),
                    id: s.id.clone(),
                    debug_id: s.debug_id.clone(),
                    random_seed: s.random_seed,
                    key: s.key.clone(),
                    at: cairn_store::entropy::now(),
                    state: s
                        .state_map
                        .iter()
                        .map(|e| (e.key.clone(), e.value.clone()))
                        .collect(),
                });
                // After recording `Start`: whoever sees count `n` sees all `n` records.
                *st.start_counts
                    .lock()
                    .unwrap()
                    .entry((service.clone(), handler.clone()))
                    .or_default() += 1;
            }
            Message::Input(_) => {}
            Message::CallCompletion(n) => {
                let v = match n.result {
                    // An SDK ≤ v5 cannot read a segmented value: the shell must assemble
                    // it — a completion carrying refs is a 400.
                    Some(pb::call_completion_notification_message::Result::Value(v))
                        if !v.segments.is_empty() =>
                    {
                        return fail(
                            StatusCode::BAD_REQUEST,
                            "runtime sent a segmented call result to a wire ≤ v5 SDK".to_string(),
                        )
                    }
                    Some(pb::call_completion_notification_message::Result::Value(v)) => {
                        Some(v.content)
                    }
                    _ => None,
                };
                done.insert(n.completion_id, v);
            }
            Message::CallInvocationIdCompletion(n) => {
                done.insert(n.completion_id, None);
            }
            Message::GetStateCompletion(n) => {
                use pb::get_state_completion_notification_message::Result as R;
                let v = match n.result {
                    Some(R::Value(v)) if !v.segments.is_empty() && st.wire >= 6 => {
                        match fetch_segmented(&v, &urls).await {
                            Ok(b) => {
                                done_refs.insert(n.completion_id, v.segments.clone());
                                Some(b)
                            }
                            Err(e) => return fail(StatusCode::BAD_GATEWAY, e),
                        }
                    }
                    Some(R::Value(v)) if !v.segments.is_empty() => {
                        return fail(
                            StatusCode::BAD_REQUEST,
                            "runtime sent a segmented value to a wire ≤ v5 SDK".to_string(),
                        )
                    }
                    Some(R::Value(v)) => Some(v.content),
                    _ => None,
                };
                done.insert(n.completion_id, v);
            }
            Message::SleepCompletion(n) => {
                done.insert(n.completion_id, None);
            }
            Message::RunCompletion(n) => {
                let v = match n.result {
                    Some(pb::run_completion_notification_message::Result::Value(v))
                        if !v.segments.is_empty() =>
                    {
                        return fail(
                            StatusCode::BAD_REQUEST,
                            "runtime sent a segmented run result to a wire ≤ v5 SDK".to_string(),
                        )
                    }
                    Some(pb::run_completion_notification_message::Result::Value(v)) => {
                        Some(v.content)
                    }
                    _ => None,
                };
                done.insert(n.completion_id, v);
            }
            Message::AttachInvocationCompletion(n) => {
                use pb::attach_invocation_completion_notification_message::Result as R;
                let d = match n.result {
                    Some(R::Value(v)) => describe_value(&v.content),
                    Some(R::Failure(f)) => describe_failure(&f),
                    None => "none".into(),
                };
                outcomes.insert(n.completion_id, d);
            }
            Message::GetInvocationOutputCompletion(n) => {
                use pb::get_invocation_output_completion_notification_message::Result as R;
                let d = match n.result {
                    Some(R::Void(_)) => "void".into(),
                    Some(R::Value(v)) => describe_value(&v.content),
                    Some(R::Failure(f)) => describe_failure(&f),
                    None => "none".into(),
                };
                outcomes.insert(n.completion_id, d);
            }
            Message::Signal(sig) => {
                if matches!(
                    &sig.signal_id,
                    Some(pb::signal_notification_message::SignalId::Name(n)) if n == HOLD_SIGNAL
                ) {
                    held_released = true;
                }
            }
            m if m.kind() == cairn_protocol::MessageKind::Command => journaled += 1,
            _ => {}
        }
    }

    if h.gated {
        match st.gate.0.acquire().await {
            Ok(p) => p.forget(),
            Err(_) => return fail(StatusCode::SERVICE_UNAVAILABLE, "gate shut for good".into()),
        }
    }

    let mut out: Vec<Message> = Vec::new();
    let mut cmd = 0usize;
    let mut next_id = 1u32;
    let mut last_call: Option<Bytes> = None;
    let mut spawned: Option<u32> = None;
    let mut fired: Vec<u32> = Vec::new();
    let mut results: Vec<String> = Vec::new();
    let mut ended = false;
    let mut errored = false;
    // Refs + bytes of the segmented values read, by key (v6, `SetStateReuse`).
    let mut known: BTreeMap<String, (Vec<pb::SegmentRef>, Bytes)> = BTreeMap::new();
    // The current value per key as far as the turn knows (`GetState`, `AppendState`).
    let mut vals: BTreeMap<String, Bytes> = BTreeMap::new();
    // Issue a command only when it is **not yet** in the journal.
    let emit = |cmd: &mut usize, out: &mut Vec<Message>, m: Message| {
        if *cmd >= journaled {
            out.push(m);
        }
        *cmd += 1;
    };
    for step in &h.steps {
        match step {
            Step::Output(v) => {
                emit(&mut cmd, &mut out, output(v.clone()));
                ended = true;
                break;
            }
            Step::EchoCallResult => {
                emit(
                    &mut cmd,
                    &mut out,
                    output(last_call.clone().unwrap_or_default()),
                );
                ended = true;
                break;
            }
            Step::Call {
                service,
                handler,
                key,
                idempotency_key,
                spread,
            } => {
                let (id_idx, result_idx) = (next_id, next_id + 1);
                next_id += 2;
                emit(
                    &mut cmd,
                    &mut out,
                    Message::Call(pb::CallCommandMessage {
                        service_name: service.clone(),
                        handler_name: handler.clone(),
                        key: key.clone().unwrap_or_default(),
                        idempotency_key: idempotency_key.clone(),
                        invocation_id_notification_idx: id_idx,
                        result_completion_id: result_idx,
                        placement: placement_of(*spread),
                        ..Default::default()
                    }),
                );
                match done.get(&result_idx) {
                    Some(v) => last_call = v.clone(),
                    None => {
                        out.push(suspend_on(result_idx));
                        break;
                    }
                }
            }
            Step::CallWith {
                service,
                handler,
                key,
                parameter,
            } => {
                let (id_idx, result_idx) = (next_id, next_id + 1);
                next_id += 2;
                emit(
                    &mut cmd,
                    &mut out,
                    Message::Call(pb::CallCommandMessage {
                        service_name: service.clone(),
                        handler_name: handler.clone(),
                        key: key.clone().unwrap_or_default(),
                        parameter: parameter.clone(),
                        invocation_id_notification_idx: id_idx,
                        result_completion_id: result_idx,
                        ..Default::default()
                    }),
                );
                match done.get(&result_idx) {
                    Some(v) => last_call = v.clone(),
                    None => {
                        out.push(suspend_on(result_idx));
                        break;
                    }
                }
            }
            Step::TaggedCall {
                service,
                handler,
                tags,
            } => {
                let (id_idx, result_idx) = (next_id, next_id + 1);
                next_id += 2;
                emit(
                    &mut cmd,
                    &mut out,
                    Message::Call(pb::CallCommandMessage {
                        service_name: service.clone(),
                        handler_name: handler.clone(),
                        invocation_id_notification_idx: id_idx,
                        result_completion_id: result_idx,
                        tags: tags
                            .iter()
                            .map(|(k, v)| pb::Header {
                                key: k.clone(),
                                value: v.clone(),
                            })
                            .collect(),
                        ..Default::default()
                    }),
                );
                match done.get(&result_idx) {
                    Some(v) => last_call = v.clone(),
                    None => {
                        out.push(suspend_on(result_idx));
                        break;
                    }
                }
            }
            Step::Send {
                service,
                handler,
                tags,
            } => {
                let id_idx = next_id;
                next_id += 1;
                emit(
                    &mut cmd,
                    &mut out,
                    Message::OneWayCall(pb::OneWayCallCommandMessage {
                        service_name: service.clone(),
                        handler_name: handler.clone(),
                        invocation_id_notification_idx: id_idx,
                        tags: tags
                            .iter()
                            .map(|(k, v)| pb::Header {
                                key: k.clone(),
                                value: v.clone(),
                            })
                            .collect(),
                        ..Default::default()
                    }),
                );
            }
            Step::Spawn {
                service,
                handler,
                spread,
            } => {
                let (id_idx, result_idx) = (next_id, next_id + 1);
                next_id += 2;
                emit(
                    &mut cmd,
                    &mut out,
                    Message::Call(pb::CallCommandMessage {
                        service_name: service.clone(),
                        handler_name: handler.clone(),
                        invocation_id_notification_idx: id_idx,
                        result_completion_id: result_idx,
                        placement: placement_of(*spread),
                        ..Default::default()
                    }),
                );
                spawned = Some(result_idx);
            }
            Step::Fire {
                service,
                handler,
                key,
            } => {
                let (id_idx, result_idx) = (next_id, next_id + 1);
                next_id += 2;
                emit(
                    &mut cmd,
                    &mut out,
                    Message::Call(pb::CallCommandMessage {
                        service_name: service.clone(),
                        handler_name: handler.clone(),
                        key: key.clone().unwrap_or_default(),
                        invocation_id_notification_idx: id_idx,
                        result_completion_id: result_idx,
                        ..Default::default()
                    }),
                );
                fired.push(result_idx);
            }
            Step::AwaitFired => {
                let missing: Vec<u32> = fired
                    .iter()
                    .copied()
                    .filter(|i| !done.contains_key(i))
                    .collect();
                if !missing.is_empty() {
                    out.push(Message::Suspension(pb::SuspensionMessage {
                        waiting_completions: missing,
                        combinator_type: pb::CombinatorType::AllCompleted as i32,
                        ..Default::default()
                    }));
                    break;
                }
            }
            Step::AwaitSpawned => {
                let idx = spawned.expect("AwaitSpawned after a Spawn");
                match done.get(&idx) {
                    Some(v) => last_call = v.clone(),
                    None => {
                        out.push(suspend_on(idx));
                        break;
                    }
                }
            }
            Step::Sleep(_) | Step::SleepUntil(_) => {
                let id = next_id;
                next_id += 1;
                let wake_up_time = match step {
                    Step::SleepUntil(at) => *at,
                    Step::Sleep(ms) => now_ms() + ms,
                    _ => unreachable!(),
                };
                emit(
                    &mut cmd,
                    &mut out,
                    Message::Sleep(pb::SleepCommandMessage {
                        wake_up_time,
                        result_completion_id: id,
                        name: String::new(),
                    }),
                );
                if !done.contains_key(&id) {
                    out.push(suspend_on(id));
                    break;
                }
            }
            Step::SendSignal(target) => {
                let target = target.lock().unwrap().clone();
                emit(
                    &mut cmd,
                    &mut out,
                    Message::SendSignal(pb::SendSignalCommandMessage {
                        target_invocation_id: target,
                        signal_id: Some(pb::send_signal_command_message::SignalId::Name(
                            HOLD_SIGNAL.into(),
                        )),
                        result: Some(pb::send_signal_command_message::Result::Void(pb::Void {})),
                        entry_name: String::new(),
                    }),
                );
            }
            Step::Attach(target) | Step::GetOutput(target) => {
                let id = next_id;
                next_id += 1;
                let target = target.lock().unwrap().clone();
                let m = match step {
                    Step::Attach(_) => {
                        Message::AttachInvocation(pb::AttachInvocationCommandMessage {
                            invocation_id: target,
                            result_completion_id: id,
                            name: String::new(),
                        })
                    }
                    _ => Message::GetInvocationOutput(pb::GetInvocationOutputCommandMessage {
                        invocation_id: target,
                        result_completion_id: id,
                        name: String::new(),
                    }),
                };
                emit(&mut cmd, &mut out, m);
                match outcomes.get(&id) {
                    Some(d) => results.push(d.clone()),
                    None => {
                        out.push(suspend_on(id));
                        break;
                    }
                }
            }
            Step::SignalIdx(target, idx) => {
                let target = target.lock().unwrap().clone();
                emit(
                    &mut cmd,
                    &mut out,
                    Message::SendSignal(pb::SendSignalCommandMessage {
                        target_invocation_id: target,
                        signal_id: Some(pb::send_signal_command_message::SignalId::Idx(*idx)),
                        result: Some(pb::send_signal_command_message::Result::Void(pb::Void {})),
                        entry_name: String::new(),
                    }),
                );
            }
            Step::OutputResults => {
                emit(
                    &mut cmd,
                    &mut out,
                    output(Bytes::from(
                        serde_json::to_vec(&results.join("\n")).unwrap(),
                    )),
                );
                ended = true;
                break;
            }
            Step::Stall(ms) => {
                tokio::time::sleep(std::time::Duration::from_millis(*ms)).await;
            }
            Step::FailBelow(n) | Step::FailBelowDelay(n, _) if attempt >= *n => {}
            Step::FailBelowDelay(_, delay) => {
                out.push(Message::Error(pb::ErrorMessage {
                    code: 500,
                    message: format!("transient failure (fake SDK, attempt {attempt})"),
                    behavior: pb::ErrorBehavior::Retry as i32,
                    next_retry_delay: Some(*delay),
                    ..Default::default()
                }));
                errored = true;
                break;
            }
            Step::FailRetry | Step::FailBelow(_) => {
                out.push(Message::Error(pb::ErrorMessage {
                    code: 500,
                    message: format!("transient failure (fake SDK, attempt {attempt})"),
                    behavior: pb::ErrorBehavior::Retry as i32,
                    ..Default::default()
                }));
                errored = true;
                break;
            }
            Step::GetState { key } => {
                let idx = next_id;
                next_id += 1;
                emit(
                    &mut cmd,
                    &mut out,
                    Message::GetState(pb::GetStateCommandMessage {
                        key: Bytes::from(key.clone().into_bytes()),
                        result_completion_id: idx,
                        name: String::new(),
                    }),
                );
                match done.get(&idx) {
                    Some(v) => {
                        if let (Some(refs), Some(b)) = (done_refs.get(&idx), v) {
                            known.insert(key.clone(), (refs.clone(), b.clone()));
                        }
                        if let Some(b) = v {
                            vals.insert(key.clone(), b.clone());
                        }
                        last_call = v.clone()
                    }
                    None => {
                        out.push(suspend_on(idx));
                        break;
                    }
                }
            }
            Step::SetStateReuse { key, value } => {
                let mut reuse = Vec::new();
                let mut off = 0usize;
                if let Some((refs, full)) = known.get(key) {
                    for r in refs {
                        let end = off + r.len as usize;
                        if end > value.len()
                            || end > full.len()
                            || value[off..end] != full[off..end]
                        {
                            break;
                        }
                        reuse.push(r.clone());
                        off = end;
                    }
                }
                let v = if reuse.is_empty() {
                    cairn_protocol::value::inline(value.clone())
                } else {
                    pb::Value {
                        content: value.slice(off..),
                        segments: reuse,
                        len: value.len() as u64,
                    }
                };
                emit(
                    &mut cmd,
                    &mut out,
                    Message::SetState(pb::SetStateCommandMessage {
                        key: Bytes::from(key.clone().into_bytes()),
                        value: Some(v),
                        name: String::new(),
                    }),
                );
            }
            Step::AppendState { key, chunk } => {
                let mut full = vals.get(key).map(|b| b.to_vec()).unwrap_or_default();
                full.extend_from_slice(chunk);
                let value = Bytes::from(full);
                let mut reuse = Vec::new();
                let mut off = 0usize;
                if st.wire >= 6 {
                    if let Some((refs, old)) = known.get(key) {
                        for r in refs {
                            let end = off + r.len as usize;
                            if end > value.len()
                                || end > old.len()
                                || value[off..end] != old[off..end]
                            {
                                break;
                            }
                            reuse.push(r.clone());
                            off = end;
                        }
                    }
                }
                let v = if reuse.is_empty() {
                    cairn_protocol::value::inline(value.clone())
                } else {
                    pb::Value {
                        content: value.slice(off..),
                        segments: reuse.clone(),
                        len: value.len() as u64,
                    }
                };
                let fresh = cmd >= journaled;
                emit(
                    &mut cmd,
                    &mut out,
                    Message::SetState(pb::SetStateCommandMessage {
                        key: Bytes::from(key.clone().into_bytes()),
                        value: Some(v),
                        name: String::new(),
                    }),
                );
                // Like the TS SDK: after a new `set`, the key's known refs are `refs[0..k]`;
                // after a replayed `set` no ref is known.
                if fresh && !reuse.is_empty() {
                    known.insert(key.clone(), (reuse, value.clone()));
                } else {
                    known.remove(key);
                }
                vals.insert(key.clone(), value);
            }
            Step::SetStateRefs { key, refs, tail } => {
                let refs: Vec<pb::SegmentRef> = refs
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(sha, len, gen)| pb::SegmentRef {
                        sha256: Bytes::copy_from_slice(sha),
                        len: *len,
                        gen: *gen,
                    })
                    .collect();
                let len = refs.iter().map(|r| r.len).sum::<u64>() + tail.len() as u64;
                emit(
                    &mut cmd,
                    &mut out,
                    Message::SetState(pb::SetStateCommandMessage {
                        key: Bytes::from(key.clone().into_bytes()),
                        value: Some(pb::Value {
                            content: tail.clone(),
                            segments: refs,
                            len,
                        }),
                        name: String::new(),
                    }),
                );
            }
            Step::SetStateForged { key, value } => {
                let forged = pb::SegmentRef {
                    sha256: Bytes::from(vec![0xAB; 32]),
                    len: 1,
                    gen: 7,
                };
                emit(
                    &mut cmd,
                    &mut out,
                    Message::SetState(pb::SetStateCommandMessage {
                        key: Bytes::from(key.clone().into_bytes()),
                        value: Some(pb::Value {
                            content: value.clone(),
                            segments: vec![forged],
                            len: value.len() as u64 + 1,
                        }),
                        name: String::new(),
                    }),
                );
            }
            Step::SetState { key, value } => {
                emit(
                    &mut cmd,
                    &mut out,
                    Message::SetState(pb::SetStateCommandMessage {
                        key: Bytes::from(key.clone().into_bytes()),
                        value: Some(cairn_protocol::value::inline(value.clone())),
                        name: String::new(),
                    }),
                );
            }
            Step::ContinueAsNewOnce(done, input) => {
                if !done.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    out.push(Message::ContinueAsNew(pb::ContinueAsNewMessage {
                        input: input.clone(),
                    }));
                    // End of the turn: no `End` after it.
                    errored = true;
                    break;
                }
            }
            Step::Run(v) | Step::RunFailBelow(_, v) => {
                let id = next_id;
                next_id += 1;
                emit(
                    &mut cmd,
                    &mut out,
                    Message::Run(pb::RunCommandMessage {
                        result_completion_id: id,
                        name: format!("r{id}"),
                    }),
                );
                // The `ctx.run` result is back (journal): keep it for `EchoCallResult`.
                if let Some(Some(v)) = done.get(&id) {
                    last_call = Some(v.clone());
                }
                if !done.contains_key(&id) {
                    if let Step::RunFailBelow(n, _) = step {
                        if attempt < *n {
                            out.push(Message::Error(pb::ErrorMessage {
                                code: 500,
                                message: format!("ctx.run failed (fake SDK, attempt {attempt})"),
                                behavior: pb::ErrorBehavior::Retry as i32,
                                ..Default::default()
                            }));
                            errored = true;
                            break;
                        }
                    }
                    out.push(Message::ProposeRunCompletion(
                        pb::ProposeRunCompletionMessage {
                            result_completion_id: id,
                            result: Some(pb::propose_run_completion_message::Result::Value(
                                v.clone(),
                            )),
                        },
                    ));
                    out.push(suspend_on(id));
                    break;
                }
            }
            Step::Suspend if held_released => {}
            Step::Suspend => {
                out.push(Message::Suspension(pb::SuspensionMessage {
                    waiting_named_signals: vec![HOLD_SIGNAL.into()],
                    ..Default::default()
                }));
                break;
            }
        }
    }
    let suspended = matches!(out.last(), Some(Message::Suspension(_)));
    if !errored && (ended || !suspended) {
        out.push(Message::End(pb::EndMessage {}));
    }

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, INVOCATION_V1.parse().unwrap());
    (StatusCode::OK, headers, encode_stream(&out))
}

fn output(v: Bytes) -> Message {
    Message::Output(pb::OutputCommandMessage {
        result: Some(pb::output_command_message::Result::Value(
            cairn_protocol::value::inline(v),
        )),
        name: String::new(),
    })
}

/// One `Attach`/`GetOutput` result as a line of [`Step::OutputResults`].
fn describe_value(v: &[u8]) -> String {
    format!("value:{}", String::from_utf8_lossy(v))
}

fn describe_failure(f: &pb::Failure) -> String {
    format!("failure:{}:{}", f.code, f.message)
}

fn suspend_on(id: u32) -> Message {
    Message::Suspension(pb::SuspensionMessage {
        waiting_completions: vec![id],
        ..Default::default()
    })
}

/// A listener that accepts only h1, like a proxy that downgrades everything to
/// HTTP/1.1 in front of the app: a connection opening with the h2c preface (`PRI `)
/// is closed at once. `axum::serve` speaks h2c by itself when axum's `http2` feature
/// is on anywhere in the workspace — without this layer the fake SDK would not be
/// "h1 only".
struct H1OnlyListener(tokio::net::TcpListener);

impl axum::serve::Listener for H1OnlyListener {
    type Io = tokio::net::TcpStream;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let Ok((stream, addr)) = self.0.accept().await else {
                continue;
            };
            // `POST` also opens with `P`: decide once 4 bytes are in (bounded).
            let sniff = async {
                let mut first = [0u8; 4];
                loop {
                    match stream.peek(&mut first).await {
                        Ok(4) => return Some(first),
                        Ok(0) | Err(_) => return None,
                        Ok(_) => tokio::task::yield_now().await,
                    }
                }
            };
            match tokio::time::timeout(std::time::Duration::from_secs(5), sniff).await {
                Ok(Some(first)) if &first != b"PRI " => return (stream, addr),
                _ => drop(stream),
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.0.local_addr()
    }
}

/// The listener of [`FakeSdkBuilder::idle_close`].
struct IdleClosingListener {
    listener: tokio::net::TcpListener,
    idle: std::time::Duration,
}

impl axum::serve::Listener for IdleClosingListener {
    type Io = IdleClosing;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            if let Ok((stream, addr)) = self.listener.accept().await {
                let io = IdleClosing {
                    stream,
                    idle: self.idle,
                    answered_at: None,
                    closed: false,
                };
                return (io, addr);
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

/// A connection of [`IdleClosingListener`]: the first byte arriving after an idle
/// stretch ≥ `idle` since the previous answer ⇒ drop that byte and report EOF — the
/// HTTP server closes the connection without reading the request.
struct IdleClosing {
    stream: tokio::net::TcpStream,
    idle: std::time::Duration,
    /// When the last answer byte was written — the connection's idleness counts from here.
    answered_at: Option<tokio::time::Instant>,
    closed: bool,
}

impl tokio::io::AsyncRead for IdleClosing {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.closed {
            return std::task::Poll::Ready(Ok(()));
        }
        let before = buf.filled().len();
        let res = std::pin::Pin::new(&mut self.stream).poll_read(cx, buf);
        let stale = self.answered_at.is_some_and(|t| t.elapsed() >= self.idle);
        if matches!(res, std::task::Poll::Ready(Ok(()))) && buf.filled().len() > before && stale {
            buf.set_filled(before);
            self.closed = true;
        }
        res
    }
}

impl tokio::io::AsyncWrite for IdleClosing {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let res = std::pin::Pin::new(&mut self.stream).poll_write(cx, data);
        if matches!(res, std::task::Poll::Ready(Ok(n)) if n > 0) {
            self.answered_at = Some(tokio::time::Instant::now());
        }
        res
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
