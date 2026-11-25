// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The runtime side of a trace: the **real** state machine
//! (`InvocationMachine`), a deterministic driver standing in for the engine,
//! and `cairn-server`'s transport rules (`turn/sdk.rs`: `drive_rr`,
//! `drive_bidi`) rewritten for one process.
//!
//! The runtime → SDK frames in a trace are exactly what the machine emits; the
//! SDK → runtime frames are the reference SDK's ([`super::sdk`]) and have all
//! gone through the machine. A trace is therefore a conversation both ends
//! accept: the drift check (`conformance_gen.rs`) holds the runtime end, the
//! SDK's runner holds the other.

use {
    super::sdk::{rr_turn, BidiSdk, Env, Op, SigKey, Transport},
    bytes::Bytes,
    cairn_protocol::{pb, Flags, Frame, Message, MessageType},
    cairn_runtime::{
        suspension_satisfied, Completion, CompletionResult, Effect, Event, FailureAction,
        InvocationMachine, Journal, JournalEntry, Outcome, Phase, SettledSignals, StartParams,
        Task,
    },
    std::collections::{BTreeMap, BTreeSet, VecDeque},
};

pub const INV_ID: [u8; 24] = [0x11; 24];

/// A scheduled delivery of a deferred task's completion (timer, call).
#[derive(Clone, Debug)]
pub struct Fire {
    /// Delivered right after SDK frame n (counted across the trace, from 1) —
    /// i.e. **in the middle of** a turn, while the stream is open. `None` =
    /// delivered when both ends are idle.
    pub after_sdk_frames: Option<usize>,
    pub id: u32,
}

/// A signal from outside (`POST /invocations/{id}/signal`, resolving an
/// awakeable). Delivered when both ends are idle, **before** every deferred
/// completion (timers not fired yet), in declaration order.
#[derive(Clone, Debug)]
pub struct SignalFire {
    pub id: SigKey,
    /// The JSON payload.
    pub json: &'static str,
}

/// A runtime → SDK side-channel frame, inserted after SDK frame n.
#[derive(Clone, Debug)]
pub struct Inject {
    pub after_sdk_frames: usize,
    pub ty: u16,
    pub payload: &'static [u8],
}

#[derive(Clone, Debug)]
pub struct Scenario {
    pub name: String,
    pub description: String,
    pub source: Vec<String>,
    pub spec: Vec<&'static str>,
    /// The cells of the section 10 matrix (`protocol-v1.md`) this trace covers.
    pub cells: Vec<u32>,
    pub wire: u32,
    pub transport: Transport,
    pub handler: &'static str,
    pub ops: Vec<Op>,
    pub input: &'static str,
    /// The object's state on the driver side (JSON values).
    pub state: Vec<(&'static str, &'static str)>,
    /// `Start` carries `state` as the eager snapshot (`state_map`).
    pub eager_state: bool,
    pub keepalive_ms: Option<u64>,
    pub replay_only: bool,
    /// The journal durable before the trace (a new leader picks it up). `None` = only `Input`.
    pub initial: Option<Vec<JournalEntry>>,
    pub fire: Vec<Fire>,
    pub signals: Vec<SignalFire>,
    pub inject: Vec<Inject>,
    /// The result (JSON) every `ctx.call` receives.
    pub call_result: &'static str,
    /// When done, open one more connection replaying the whole journal: the SDK
    /// may only return `End` (section 10 — `End` is not an entry, `Output` is).
    pub verify_replay: bool,
    /// Wire v5 (section 9.3): the SDK keeps a replay cache, the runtime sends deltas per the hint.
    pub replay_cache: bool,
    /// Connection n (counted from 0) carries a `prefix_digest` with one bit
    /// flipped — the SDK must report a miss (the digest disagrees with its cache).
    pub tamper_digest_at: Option<usize>,
    /// The SDK restarts (losing its cache) right before connection n.
    pub sdk_restart_at: Option<usize>,
    /// Connection n lands on an **old** replica of the SDK — its cache is the
    /// cache from before the last turn (two replicas without affinity).
    pub stale_sdk_at: Option<usize>,
    /// Round 2 (section 9.3): a miss carrying `cairn-replay-held` ⇒ the runtime
    /// resends from `k`. Off in the v5 traces from before round 2 (their runtime
    /// resends whole — still valid).
    pub resend_from_held: bool,
    /// The wire version cap the SDK declares (`maxProtocolVersion`), **below**
    /// `wire`: the deployment settled on a new version, the SDK process was
    /// downgraded to an old one.
    pub sdk_max_wire: Option<u32>,
    /// Wire v6: sealed state — the key, one segment (in [`SEGMENTS`]) and the
    /// inline tail.
    pub seg_state: Vec<(&'static str, &'static SegDef, &'static str)>,
}

/// An immutable segment of a v6 trace: bytes, sha256 (computed by the shell
/// when sealing) and `gen` (issued by the shell at every seal). The runner
/// serves the bytes at [`SegDef::url`].
#[derive(Debug)]
pub struct SegDef {
    pub bytes: &'static [u8],
    pub sha256: [u8; 32],
    pub gen: u64,
}

impl SegDef {
    pub fn seg_ref(&self) -> cairn_protocol::value::SegmentRef {
        cairn_protocol::value::SegmentRef {
            sha256: self.sha256,
            len: self.bytes.len() as u64,
            gen: self.gen,
        }
    }

    /// The segment's read URL in the trace — a constant, unsigned (the shell
    /// signs with `value_urls.rs`; the runner serves exactly this string
    /// through the `fetch` hook).
    pub fn url(&self) -> String {
        format!(
            "https://segments.conformance.invalid/v1/{}-{}",
            super::json::hex(&self.sha256),
            self.gen
        )
    }
}

/// The segment table of every v6 trace (the runner serves by URL).
pub static SEGMENTS: &[SegDef] = &[SegDef {
    // `"` + 39 × `a` — the 40-byte prefix of the JSON string `"a…a"`.
    bytes: b"\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    sha256: [
        0x00, 0xa6, 0xee, 0xd3, 0x3d, 0x40, 0xdc, 0x58, 0x18, 0x62, 0x0d, 0xc4, 0xf4, 0xae, 0x4d,
        0x5d, 0xb7, 0xf0, 0x2f, 0xe9, 0x47, 0x51, 0x99, 0xac, 0x43, 0x5b, 0xab, 0x7c, 0x04, 0xa7,
        0x9f, 0x94,
    ],
    gen: 7,
}];

/// The table's segment with this `(sha, gen)`.
pub fn segment(r: &cairn_protocol::value::SegmentRef) -> Option<&'static SegDef> {
    SEGMENTS
        .iter()
        .find(|d| d.sha256 == r.sha256 && d.gen == r.gen)
}

/// The logical bytes of a `Value` (segments from the table ‖ tail).
pub fn logical_bytes(l: &cairn_protocol::value::Logical) -> Bytes {
    let mut out = Vec::new();
    for r in l.refs() {
        out.extend_from_slice(segment(r).expect("the segment is in the table").bytes);
    }
    out.extend_from_slice(l.carried());
    out.into()
}

pub const CLOCK_MS: u64 = 1_800_000_000_000;

impl Scenario {
    pub fn new(name: &str, handler: &'static str, transport: Transport, wire: u32) -> Self {
        Self {
            name: name.into(),
            description: String::new(),
            source: vec![],
            spec: vec![],
            cells: vec![],
            wire,
            transport,
            handler,
            ops: vec![],
            input: "null",
            state: vec![],
            eager_state: false,
            keepalive_ms: None,
            replay_only: false,
            initial: None,
            fire: vec![],
            signals: vec![],
            inject: vec![],
            call_result: "null",
            verify_replay: true,
            replay_cache: false,
            tamper_digest_at: None,
            sdk_restart_at: None,
            stale_sdk_at: None,
            resend_from_held: false,
            sdk_max_wire: None,
            seg_state: vec![],
        }
    }

    pub fn env(&self) -> Env {
        Env {
            wire: self.wire,
            transport: self.transport,
            clock_ms: CLOCK_MS,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum From {
    Runtime,
    Sdk,
}

#[derive(Clone, Debug)]
pub struct TraceFrame {
    pub from: From,
    pub frame: Frame,
    pub wire: Bytes,
}

impl TraceFrame {
    pub fn new(from: From, frame: Frame) -> Self {
        let wire = cairn_protocol::encode_frame(frame.ty, frame.flags, &frame.payload);
        Self { from, frame, wire }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Close {
    /// The SDK finishes (Output+End, Error, ReplayReport) — or an RR turn.
    Sdk,
    /// The runtime closes the stream: what the SDK waits for only the outside world can answer.
    Runtime,
}

#[derive(Clone, Debug)]
pub struct Conn {
    pub frames: Vec<TraceFrame>,
    pub close: Close,
    pub verify: bool,
    /// The SDK must refuse the connection with this HTTP code, no frames
    /// (`415` when the runtime speaks a version above the cap the SDK
    /// declares). `None` in every other trace.
    pub status: Option<u16>,
    /// Wire v5 (section 9.3) — `None` in every pre-v5 trace (the JSON does not change).
    pub replay: Option<ConnReplay>,
}

/// The v5 part of a `REQUEST_RESPONSE` connection.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConnReplay {
    /// The SDK must return `409` + `cairn-replay: miss`, an empty body.
    pub miss: bool,
    /// The `cairn-replay-cache` the SDK must return (200). `None` = not kept.
    pub cache: Option<u32>,
    /// The runner restarts the SDK (dropping its cache) before this connection.
    pub sdk_restart: bool,
    /// A miss (round 2): the `cairn-replay-held` the SDK must return — `k` and `h_k`.
    pub held: Option<(u32, cairn_runtime::replay::Digest)>,
    /// The runner puts the SDK back on an **old** cache (another replica) before this connection.
    pub stale_sdk: bool,
}

/// The reference SDK's replay cache: the whole frames of entries `[0, k)` and `h₀ … h_k`.
#[derive(Clone, Debug)]
struct SdkCache {
    frames: Vec<Bytes>,
    chain: Vec<cairn_runtime::replay::Digest>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Final {
    Completed(pb::OutputCommandMessage),
    Failed {
        code: u32,
        action: FailureAction,
    },
    ReplayReport(pb::ReplayReportMessage),
    /// The SDK refuses every connection — the invocation makes no progress.
    Refused,
}

pub struct Trace {
    pub scenario: Scenario,
    pub initial_len: usize,
    pub conns: Vec<Conn>,
    pub journal: Vec<JournalEntry>,
    pub outcome: Final,
}

/// A fake engine: state read from a map, every task answered deterministically.
struct Driver {
    state: BTreeMap<Bytes, cairn_protocol::value::Logical>,
    promises: BTreeMap<String, Bytes>,
    /// Deferred completions, in the order the tasks produced them.
    deferred: Vec<Completion>,
    seen: BTreeSet<u32>,
    plan: Vec<Fire>,
    signals: VecDeque<SignalFire>,
    call_result: Bytes,
}

/// What the driver delivers when both ends are idle.
enum Pending {
    Completion(Completion),
    Signal(SignalFire),
}

impl Driver {
    /// A completion answered **in place** (same step, like the engine reading state).
    fn on_task(&mut self, task: Task) -> Vec<Completion> {
        // The machine re-issues the tasks of pending completions at every
        // rebuild — an id already seen is already taken care of.
        let fresh = |seen: &mut BTreeSet<u32>, id: u32| seen.insert(id);
        match task {
            Task::ReadState { completion_id, key } => {
                if !fresh(&mut self.seen, completion_id) {
                    return vec![];
                }
                vec![Completion {
                    id: completion_id,
                    result: CompletionResult::State(self.state.get(&key).cloned()),
                }]
            }
            Task::ReadStateKeys { completion_id } => {
                if !fresh(&mut self.seen, completion_id) {
                    return vec![];
                }
                vec![Completion {
                    id: completion_id,
                    result: CompletionResult::StateKeys(self.state.keys().cloned().collect()),
                }]
            }
            Task::WriteState { key, value } => {
                match value {
                    // v6: a `SetState` carrying a reused ref goes through
                    // `Event::Sealed` (the shell), the segmented value enters
                    // state as ref + tail.
                    Some(v) => self.state.insert(key, v),
                    None => self.state.remove(&key),
                };
                vec![]
            }
            Task::ClearAllState => {
                self.state.clear();
                vec![]
            }
            Task::PromiseComplete {
                completion_id,
                key,
                result,
            } => {
                if !fresh(&mut self.seen, completion_id) {
                    return vec![];
                }
                if let pb::complete_promise_command_message::Completion::CompletionValue(v) = result
                {
                    self.promises.entry(key).or_insert(v.content);
                }
                vec![Completion {
                    id: completion_id,
                    result: CompletionResult::Void,
                }]
            }
            Task::PromiseGet { completion_id, key } => {
                if !fresh(&mut self.seen, completion_id) {
                    return vec![];
                }
                let v = self
                    .promises
                    .get(&key)
                    .cloned()
                    .expect("promise not completed yet");
                vec![Completion {
                    id: completion_id,
                    result: CompletionResult::CallValue(v),
                }]
            }
            Task::ScheduleTimer { completion_id, .. } => {
                if fresh(&mut self.seen, completion_id) {
                    self.deferred.push(Completion {
                        id: completion_id,
                        result: CompletionResult::Timer,
                    });
                }
                vec![]
            }
            Task::Invoke {
                result_completion_id,
                invocation_id_completion_id,
                ..
            } => {
                if fresh(&mut self.seen, invocation_id_completion_id) {
                    self.deferred.push(Completion {
                        id: invocation_id_completion_id,
                        result: CompletionResult::CallInvocationId("ab".repeat(24)),
                    });
                }
                if let Some(rc) = result_completion_id {
                    if fresh(&mut self.seen, rc) {
                        self.deferred.push(Completion {
                            id: rc,
                            result: CompletionResult::CallValue(self.call_result.clone()),
                        });
                    }
                }
                vec![]
            }
            other => panic!("the trace driver does not support task {other:?} yet"),
        }
    }

    fn take(&mut self, id: u32) -> Option<Completion> {
        let i = self.deferred.iter().position(|c| c.id == id)?;
        Some(self.deferred.remove(i))
    }

    /// A completion scheduled for mid-turn delivery, due after SDK frame `n`.
    fn due(&mut self, n: usize) -> Vec<Completion> {
        let ids: Vec<u32> = self
            .plan
            .iter()
            .filter(|f| f.after_sdk_frames.is_some_and(|a| a <= n))
            .map(|f| f.id)
            .collect();
        ids.into_iter().filter_map(|id| self.take(id)).collect()
    }

    /// The next thing when both ends are idle: signals scheduled first, then
    /// completions per `plan`, then in task order.
    fn idle(&mut self) -> Option<Pending> {
        if let Some(s) = self.signals.pop_front() {
            return Some(Pending::Signal(s));
        }
        let planned: Vec<u32> = self.plan.iter().map(|f| f.id).collect();
        for id in planned {
            if let Some(c) = self.take(id) {
                return Some(Pending::Completion(c));
            }
        }
        (!self.deferred.is_empty()).then(|| Pending::Completion(self.deferred.remove(0)))
    }
}

/// The target of runtime → SDK frames while applying effects.
struct Sink<'a> {
    rec: Option<(&'a mut Conn, &'a mut VecDeque<Frame>)>,
    /// Skip the first n frames sent (the replayed part the SDK already has, `drive_bidi`).
    skip: usize,
}

impl Sink<'_> {
    fn discard() -> Sink<'static> {
        Sink { rec: None, skip: 0 }
    }

    fn send(&mut self, msg: &Message) {
        if self.skip > 0 {
            self.skip -= 1;
            return;
        }
        if let Some((conn, inbox)) = self.rec.as_mut() {
            let f = to_frame(msg);
            conn.frames.push(TraceFrame::new(From::Runtime, f.clone()));
            inbox.push_back(f);
        }
    }
}

fn split(body: &[u8]) -> Vec<Frame> {
    let dec = cairn_protocol::FrameDecoder::new();
    let mut buf = bytes::BytesMut::from(body);
    let mut out = Vec::new();
    while let Some(f) = dec.decode(&mut buf).unwrap() {
        out.push(f);
    }
    assert!(buf.is_empty());
    out
}

fn start_info_known(f: &Frame) -> u32 {
    match cairn_protocol::decode(f) {
        Ok(Message::Start(s)) => s.known_entries,
        _ => panic!("the first frame is not Start"),
    }
}

fn to_frame(msg: &Message) -> Frame {
    let wire = msg.encode(Flags::NONE);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    cairn_protocol::FrameDecoder::new()
        .decode(&mut buf)
        .unwrap()
        .unwrap()
}

struct Sim {
    sc: Scenario,
    journal: Vec<JournalEntry>,
    driver: Driver,
    conns: Vec<Conn>,
    sdk_frames: usize,
    outcome: Option<Outcome>,
    last_spec: Option<pb::SuspensionMessage>,
    fin: Option<Final>,
    /// The runtime's hint (section 9.3): `B` for the next turn.
    hint: Option<u32>,
    sdk_cache: Option<SdkCache>,
    /// The SDK's cache before the last turn — the old replica of `stale_sdk_at`.
    prev_cache: Option<SdkCache>,
}

/// The initial journal of a new invocation: entry 0 is `Input`, created by the runtime.
pub fn input_entry(json: &str) -> JournalEntry {
    let msg = Message::Input(pb::InputCommandMessage {
        headers: vec![],
        value: Some(cairn_protocol::value::inline(Bytes::copy_from_slice(
            json.as_bytes(),
        ))),
        name: String::new(),
    });
    JournalEntry::new(
        MessageType::INPUT,
        msg.encode(Flags::NONE).slice(cairn_protocol::HEADER_LEN..),
    )
}

impl Sim {
    fn machine(&self) -> InvocationMachine {
        let params = StartParams {
            state_map: if self.sc.eager_state {
                self.driver
                    .state
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            v.clone()
                                .into_inline()
                                .expect("an eager snapshot is inline only"),
                        )
                    })
                    .collect()
            } else {
                vec![]
            },
            ..Default::default()
        };
        let mut m =
            InvocationMachine::resume(INV_ID, params, Journal::from_entries(self.journal.clone()));
        // The runtime does **not** send v3 parameters to a deployment settled below v3.
        if self.sc.wire >= 3 {
            m.set_wire_v3(self.sc.keepalive_ms, self.sc.replay_only);
        }
        m
    }

    fn apply(&mut self, m: &mut InvocationMachine, fx: Vec<Effect>, sink: &mut Sink<'_>) {
        let mut queue: VecDeque<Effect> = fx.into();
        while let Some(e) = queue.pop_front() {
            match e {
                Effect::Persist { entry_index, entry } => {
                    assert_eq!(
                        entry_index as usize,
                        self.journal.len(),
                        "persist at the wrong position"
                    );
                    self.journal.push(entry);
                }
                Effect::Send(msg) => sink.send(&msg),
                Effect::Task(t) => {
                    // A task answered in place: its notification goes right
                    // after, before the rest of the step's effects — as the
                    // engine applies tasks in order.
                    let mut now = Vec::new();
                    for c in self.driver.on_task(t) {
                        now.extend(m.handle(Event::Completion(c)).expect("journal not broken"));
                    }
                    for e in now.into_iter().rev() {
                        queue.push_front(e);
                    }
                }
                // Spec section 5: forward the raw bytes, no journal. `BIDI_STREAM`
                // has a path (the stream is open), `REQUEST_RESPONSE` does not —
                // the RR turn's `discard` sink is exactly that.
                Effect::ForwardCustom { ty, payload } => {
                    sink.send(&Message::Custom { ty, payload });
                }
                Effect::Finish(o) => self.outcome = self.outcome.take().or(Some(o)),
            }
        }
    }

    fn deliver(&mut self, m: &mut InvocationMachine, c: Completion, sink: &mut Sink<'_>) {
        assert_ne!(
            m.phase(),
            Phase::Closed,
            "completion {} reached a closed machine",
            c.id
        );
        let fx = m.handle(Event::Completion(c)).expect("journal not broken");
        self.apply(m, fx, sink);
    }

    fn deliver_pending(&mut self, m: &mut InvocationMachine, p: Pending, sink: &mut Sink<'_>) {
        match p {
            Pending::Completion(c) => self.deliver(m, c, sink),
            Pending::Signal(s) => {
                use pb::signal_notification_message::Result as R;
                let fx = m
                    .handle(Event::Signal(pb::SignalNotificationMessage {
                        signal_id: Some(s.id.id()),
                        result: Some(R::Value(cairn_protocol::value::inline(Bytes::from_static(
                            s.json.as_bytes(),
                        )))),
                    }))
                    .expect("journal not broken");
                self.apply(m, fx, sink);
            }
        }
    }

    fn satisfied(&self, s: &pb::SuspensionMessage, m: &InvocationMachine) -> bool {
        suspension_satisfied(
            s,
            &m.settled_completions(),
            SettledSignals {
                signals: m.settled_signals(),
            },
        )
    }

    /// An SDK → runtime frame. Three frames stop at the transport layer and do
    /// not enter the machine: `Keepalive` (side channel — the silence cap has
    /// restarted), `StreamAppend` at wire ≥ 4 (side channel — into `StreamHub`,
    /// `turn/sdk.rs::side_frame`) and `ReplayReport` (a trial replay turn,
    /// `replay.rs`).
    fn on_sdk_frame(&mut self, m: &mut InvocationMachine, f: Frame, sink: &mut Sink<'_>) {
        self.sdk_frames += 1;
        if f.ty == MessageType::KEEPALIVE
            || (f.ty == MessageType::STREAM_APPEND && self.sc.wire >= 4)
        {
            return;
        }
        if f.ty == MessageType::REPLAY_REPORT {
            let Ok(Message::ReplayReport(r)) = cairn_protocol::decode(&f) else {
                unreachable!()
            };
            self.fin = Some(Final::ReplayReport(r));
            return;
        }
        // Wire v6: a frame carrying a reused ref (`SetState`, `Output`, a
        // `ctx.run` result) goes through `Event::Sealed` — the shell keeps the
        // ref, a small tail is not sealed further (`turn/values.rs::seal_frame`,
        // `seal_output`).
        let sealed = self.sc.wire >= 6
            && cairn_protocol::decode(&f).is_ok_and(|m| {
                cairn_protocol::value::values(&m)
                    .iter()
                    .any(|v| !v.segments.is_empty())
            });
        let ev = if sealed {
            Event::Sealed(f)
        } else {
            Event::FromSdk(f)
        };
        let fx = m.handle(ev).expect("journal not broken");
        self.apply(m, fx, sink);
    }

    fn settle(&mut self, o: Outcome) {
        self.fin = Some(match o {
            Outcome::Completed(out) => Final::Completed(out),
            Outcome::Failed { code, action, .. } => Final::Failed { code, action },
            Outcome::Suspended(_) => unreachable!(),
        });
    }

    fn bidi_conn(&mut self, verify: bool) -> Close {
        let mut conn = Conn {
            frames: vec![],
            close: Close::Sdk,
            verify,
            replay: None,
            status: None,
        };
        let mut inbox = VecDeque::new();
        let mut m = self.machine();
        let fx = m.handle(Event::Start).expect("journal not broken");
        self.outcome = None;
        self.apply(
            &mut m,
            fx,
            &mut Sink {
                rec: Some((&mut conn, &mut inbox)),
                skip: 0,
            },
        );
        let mut sdk = BidiSdk::new(self.sc.ops.clone(), self.sc.env());
        let close = loop {
            if let Some(f) = sdk.outbox.pop_front() {
                conn.frames.push(TraceFrame::new(From::Sdk, f.clone()));
                let mut sink = Sink {
                    rec: Some((&mut conn, &mut inbox)),
                    skip: 0,
                };
                self.on_sdk_frame(&mut m, f, &mut sink);
                match self.outcome.take() {
                    None => {}
                    Some(Outcome::Suspended(s)) => {
                        self.last_spec = Some(s.clone());
                        if !self.satisfied(&s, &m) {
                            break Close::Runtime;
                        }
                        // Resolvable in place: rebuild the machine, **keep** the
                        // stream, and send only the part **after** the replay the
                        // SDK already has.
                        m = self.machine();
                        let replayed = 1 + self.journal.len();
                        let fx = m.handle(Event::Start).expect("journal not broken");
                        self.apply(
                            &mut m,
                            fx,
                            &mut Sink {
                                rec: Some((&mut conn, &mut inbox)),
                                skip: replayed,
                            },
                        );
                    }
                    Some(o) => self.settle(o),
                }
                let n = self.sdk_frames;
                for c in self.driver.due(n) {
                    let mut sink = Sink {
                        rec: Some((&mut conn, &mut inbox)),
                        skip: 0,
                    };
                    self.deliver(&mut m, c, &mut sink);
                }
                for inj in self.sc.inject.iter().filter(|i| i.after_sdk_frames == n) {
                    let f = Frame::new(
                        MessageType(inj.ty),
                        Flags::NONE,
                        Bytes::from_static(inj.payload),
                    );
                    conn.frames.push(TraceFrame::new(From::Runtime, f.clone()));
                    inbox.push_back(f);
                }
                continue;
            }
            if !inbox.is_empty() && !sdk.done() {
                sdk.step(&mut inbox);
                continue;
            }
            if self.fin.is_some() || sdk.done() {
                break Close::Sdk;
            }
            if let Some(p) = self.driver.idle() {
                let mut sink = Sink {
                    rec: Some((&mut conn, &mut inbox)),
                    skip: 0,
                };
                self.deliver_pending(&mut m, p, &mut sink);
                continue;
            }
            panic!("{}: both ends are waiting for each other", self.sc.name);
        };
        conn.close = close;
        self.conns.push(conn);
        close
    }

    fn rr_conn(&mut self, verify: bool) {
        let mut conn = Conn {
            frames: vec![],
            close: Close::Sdk,
            verify,
            replay: None,
            status: None,
        };
        let mut body = VecDeque::new();
        let mut m = self.machine();
        let fx = m.handle(Event::Start).expect("journal not broken");
        self.outcome = None;
        self.apply(
            &mut m,
            fx,
            &mut Sink {
                rec: Some((&mut conn, &mut body)),
                skip: 0,
            },
        );
        let mut request: Vec<Frame> = body.into_iter().collect();
        if self.sc.wire >= 6 {
            // `prepare_down(Down::Opening)`: the read URL of every ref in the
            // batch goes into `Start.segment_urls`, in order of appearance,
            // without duplicate `(sha, gen)` — like `value_urls.rs::for_messages`.
            let mut seen = BTreeSet::new();
            let mut urls = Vec::new();
            for f in &request {
                let m = cairn_protocol::decode(f).unwrap();
                for r in cairn_protocol::value::refs_in(&m).unwrap() {
                    if seen.insert((r.sha256, r.gen)) {
                        let d = segment(&r).expect("the segment is in the table");
                        urls.push(pb::SegmentUrl {
                            sha256: Bytes::copy_from_slice(&r.sha256),
                            gen: r.gen,
                            url: d.url(),
                        });
                    }
                }
            }
            if !urls.is_empty() {
                let Ok(Message::Start(mut st)) = cairn_protocol::decode(&request[0]) else {
                    unreachable!("the turn-opening batch starts with Start")
                };
                st.segment_urls = urls;
                request[0] = to_frame(&Message::Start(st));
                conn.frames[0] = TraceFrame::new(From::Runtime, request[0].clone());
            }
        }
        if self.sc.replay_cache {
            self.rr_replay(&mut conn, &request);
        }
        let out = rr_turn(&self.sc.ops, &request, self.sc.env());
        if self.sc.replay_cache {
            self.rr_keep(&mut conn, &request, &out);
        }
        for f in out {
            conn.frames.push(TraceFrame::new(From::Sdk, f.clone()));
            // The response body has been read in full: there is no path to send anything down to
            // the SDK.
            self.on_sdk_frame(&mut m, f, &mut Sink::discard());
            let n = self.sdk_frames;
            for c in self.driver.due(n) {
                self.deliver(&mut m, c, &mut Sink::discard());
            }
        }
        match self.outcome.take() {
            Some(Outcome::Suspended(s)) => self.last_spec = Some(s),
            Some(o) => self.settle(o),
            None => assert!(
                self.fin.is_some(),
                "{}: the RR turn does not end",
                self.sc.name
            ),
        }
        self.conns.push(conn);
    }

    /// Wire v5, before the turn: with a hint the runtime sends a delta
    /// (`replay::Opening`, exactly the function of `turn/sdk.rs`). The reference
    /// SDK checks its cache: a hit ⇒ the connection carries the delta body and
    /// the SDK runs on the spliced copy (equal to `request` — the check
    /// `delta_rejoins_to_full_replay_on_every_rr_trace`); a miss ⇒ one `409`
    /// connection without SDK frames, then the runtime resends whole within
    /// `conn`.
    fn rr_replay(&mut self, conn: &mut Conn, request: &[Frame]) {
        use cairn_runtime::replay::Opening;
        let at = self.conns.len();
        let restart = self.sc.sdk_restart_at == Some(at);
        if restart {
            self.sdk_cache = None;
        }
        let stale = self.sc.stale_sdk_at == Some(at);
        if stale {
            self.sdk_cache = self.prev_cache.clone();
        }
        let msgs: Vec<Message> = request
            .iter()
            .map(|f| cairn_protocol::decode(f).unwrap())
            .collect();
        let opening = Opening::new(&msgs).expect("a turn opening");
        let Some(b) = self.hint.filter(|&b| b > 0 && b <= opening.known_entries()) else {
            conn.replay = Some(ConnReplay {
                sdk_restart: restart,
                ..Default::default()
            });
            return;
        };
        let mut body = opening.body(b);
        if self.sc.tamper_digest_at == Some(at) {
            let mut frames = split(&body);
            let Ok(Message::Start(mut s)) = cairn_protocol::decode(&frames[0]) else {
                unreachable!()
            };
            let mut d = s.prefix_digest.to_vec();
            d[0] ^= 1;
            s.prefix_digest = d.into();
            frames[0] = to_frame(&Message::Start(s));
            body = frames
                .iter()
                .flat_map(|f| cairn_protocol::encode_frame(f.ty, f.flags, &f.payload).to_vec())
                .collect::<Vec<u8>>()
                .into();
        }
        let delta = split(&body);
        let Ok(Message::Start(ds)) = cairn_protocol::decode(&delta[0]) else {
            unreachable!()
        };
        let n = opening.known_entries();
        let (hit, held) = self.sdk_check(b, &ds.prefix_digest, n);
        let delta_conn = |frames: Vec<Frame>, hit: bool, held, restart, stale| Conn {
            frames: frames
                .into_iter()
                .map(|f| TraceFrame::new(From::Runtime, f))
                .collect(),
            close: Close::Sdk,
            verify: conn.verify,
            status: None,
            replay: Some(ConnReplay {
                miss: !hit,
                cache: None,
                sdk_restart: restart,
                held,
                stale_sdk: stale,
            }),
        };
        if hit {
            // The SDK runs on the spliced copy — exactly `request`; the connection records the
            // delta body.
            *conn = delta_conn(delta, true, None, restart, stale);
            return;
        }
        // A miss: the SDK runs nothing.
        let held = held.filter(|_| self.sc.resend_from_held);
        self.conns
            .push(delta_conn(delta, false, held, restart, stale));
        // Round 2: a valid `k` (0 < k ≤ N, k ≠ B, h_k matches the current
        // journal) ⇒ resend **once** from `k`; otherwise (or on another miss)
        // whole — `conn`.
        if let Some((k, h)) = held {
            if k > 0 && k <= n && k != b && opening.digest(k) == Some(&h) {
                let again = split(&opening.body(k));
                let (hit, _) = self.sdk_check(k, &h, n);
                if hit {
                    *conn = delta_conn(again, true, None, false, false);
                    return;
                }
                self.conns
                    .push(delta_conn(again, false, None, false, false));
            }
        }
        conn.replay = Some(ConnReplay::default());
    }

    /// The reference SDK at a `replay_from = b` turn: whether it hits, and on a
    /// miss the part it still holds `(k = min(held, N), h_k)`.
    fn sdk_check(
        &self,
        b: u32,
        digest: &[u8],
        n: u32,
    ) -> (bool, Option<(u32, cairn_runtime::replay::Digest)>) {
        let Some(c) = self.sdk_cache.as_ref() else {
            return (false, None);
        };
        if c.frames.len() >= b as usize && c.chain[b as usize][..] == digest[..] {
            return (true, None);
        }
        let k = (c.frames.len() as u32).min(n);
        (false, (k > 0).then(|| (k, c.chain[k as usize])))
    }

    /// Wire v5, after the turn: the SDK keeps the entries `[0, N)` it received
    /// (dropped when the turn ends the invocation — `End`/`Error`), the runtime
    /// records the hint `B = min(k, N)`.
    fn rr_keep(&mut self, conn: &mut Conn, request: &[Frame], out: &[Frame]) {
        let n = start_info_known(&request[0]);
        let ended = out
            .iter()
            .any(|f| f.ty == MessageType::END || f.ty == MessageType::ERROR);
        let r = conn.replay.get_or_insert_with(ConnReplay::default);
        if ended || n == 0 {
            self.sdk_cache = None;
            self.hint = None;
            r.cache = None;
            return;
        }
        let frames: Vec<Bytes> = request[1..=n as usize]
            .iter()
            .map(|f| cairn_protocol::encode_frame(f.ty, f.flags, &f.payload))
            .collect();
        let Ok(Message::Start(s)) = cairn_protocol::decode(&request[0]) else {
            unreachable!()
        };
        let chain = cairn_runtime::replay::chain(&s.id, &frames);
        self.prev_cache = self.sdk_cache.take();
        self.sdk_cache = Some(SdkCache { frames, chain });
        self.hint = Some(n);
        r.cache = Some(n);
    }

    /// Outside a turn: completions arrive and the engine journals them until
    /// what the SDK waits for is complete.
    fn wait_until_satisfied(&mut self) {
        let spec = self.last_spec.clone().expect("suspended without a spec");
        loop {
            let mut m = self.machine();
            let fx = m.handle(Event::Start).expect("journal not broken");
            self.apply(&mut m, fx, &mut Sink::discard());
            if self.satisfied(&spec, &m) {
                return;
            }
            let p = self
                .driver
                .idle()
                .unwrap_or_else(|| panic!("{}: suspended forever", self.sc.name));
            self.deliver_pending(&mut m, p, &mut Sink::discard());
        }
    }
}

pub fn run(sc: Scenario) -> Trace {
    let mut journal = sc
        .initial
        .clone()
        .unwrap_or_else(|| vec![input_entry(sc.input)]);
    if journal.is_empty() {
        journal.push(input_entry(sc.input));
    }
    let initial_len = journal.len();
    let driver = Driver {
        state: sc
            .state
            .iter()
            .map(|(k, v)| {
                (
                    Bytes::from_static(k.as_bytes()),
                    cairn_protocol::value::Logical::Inline(Bytes::from_static(v.as_bytes())),
                )
            })
            .chain(sc.seg_state.iter().map(|(k, d, tail)| {
                let tail = Bytes::from_static(tail.as_bytes());
                (
                    Bytes::from_static(k.as_bytes()),
                    cairn_protocol::value::Logical::Segmented {
                        len: d.bytes.len() as u64 + tail.len() as u64,
                        refs: vec![d.seg_ref()],
                        tail,
                    },
                )
            }))
            .collect(),
        promises: BTreeMap::new(),
        deferred: vec![],
        seen: BTreeSet::new(),
        plan: sc.fire.clone(),
        signals: sc.signals.iter().cloned().collect(),
        call_result: Bytes::from_static(sc.call_result.as_bytes()),
    };
    let mut sim = Sim {
        sc,
        journal,
        driver,
        conns: vec![],
        sdk_frames: 0,
        outcome: None,
        last_spec: None,
        fin: None,
        hint: None,
        sdk_cache: None,
        prev_cache: None,
    };

    for _ in 0..64 {
        match sim.sc.transport {
            Transport::Bidi => {
                sim.bidi_conn(false);
            }
            Transport::Rr => sim.rr_conn(false),
        }
        if sim.fin.is_some() {
            break;
        }
        // Suspended: wait outside the turn until what the SDK waits for is
        // complete (returns at once if already complete — an RR turn suspended
        // on a notification it just recorded itself).
        sim.wait_until_satisfied();
    }
    let outcome = sim
        .fin
        .clone()
        .expect("the trace exceeded 64 connections without ending");

    if sim.sc.verify_replay && matches!(outcome, Final::Completed(_)) {
        let before = sim.journal.clone();
        sim.fin = None;
        match sim.sc.transport {
            Transport::Bidi => {
                sim.bidi_conn(true);
            }
            Transport::Rr => sim.rr_conn(true),
        }
        assert_eq!(
            sim.journal, before,
            "{}: replaying the whole journal made it longer",
            sim.sc.name
        );
        let last = sim.conns.last().unwrap();
        let sdk: Vec<MessageType> = last
            .frames
            .iter()
            .filter(|f| f.from == From::Sdk)
            .map(|f| f.frame.ty)
            .collect();
        assert_eq!(
            sdk,
            vec![MessageType::END],
            "{}: replaying the whole journal",
            sim.sc.name
        );
        assert_eq!(sim.fin, Some(outcome.clone()));
    }

    Trace {
        initial_len,
        conns: sim.conns,
        journal: sim.journal,
        outcome,
        scenario: sim.sc,
    }
}
