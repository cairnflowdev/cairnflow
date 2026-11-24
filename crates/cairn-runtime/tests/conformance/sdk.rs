// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The reference SDK — the SDK side of the conformance traces.
//!
//! It differs from `harness::ScriptedSdk` at the core: the harness's fake SDK
//! is enough to check **the runtime** (it never suspends, never sends
//! `Suspension`), while this SDK must emit **exactly the bytes** a real SDK
//! would emit, because the traces produced from it are what the runner of every
//! language compares their SDK against. So it copies the TypeScript SDK's model
//! (`sdk/typescript/src/handler.ts`): rerun the handler **from the start** on
//! every turn on a local journal, allocate completion ids in call order,
//! suspend with `Suspension`, keep a `ctx.run` result outside the journal until
//! the ack comes back.
//!
//! The bytes are encoded by `prost` — the canonical form of the `.proto`
//! (fields in tag order, packed `repeated` numbers). The runner compares the
//! real SDK with exactly these bytes, so a hand-written codec that deviates from
//! the canonical form shows up here.
//!
//! Each `Program` has a handler of the same name in the runner (the
//! `conformance/` README).

use {
    bytes::Bytes,
    cairn_protocol::value::SegmentRef,
    cairn_protocol::{
        decode, encode_message, pb, Flags, Frame, FrameDecoder, Message, MessageType,
    },
    std::collections::{BTreeMap, BTreeSet, VecDeque},
};

/// One statement of a handler, in the SDK's API (`ctx.*`).
#[derive(Clone, Debug)]
pub enum Op {
    /// `const f = ctx.get(key)` — a future; eager when `Start` carries a state snapshot.
    Get(&'static str),
    /// `ctx.set(key, <json>)`.
    Set(&'static str, &'static str),
    /// `const f = ctx.sleep(ms)` — future.
    Sleep(u64),
    /// `const f = ctx.getKeys()` — a future (the lazy path).
    GetKeys,
    /// `await ctx.promise(name).resolve(<json>)`.
    PromiseResolve(&'static str, &'static str),
    /// `const f = ctx.promise(name).get()` — a future.
    PromiseGet(&'static str),
    /// `const f = ctx.scoped(scope, limitKey).call(service, handler, <json>)` —
    /// a future. Wire < 2 ⇒ a terminal error (the SDK refuses to send a scope to
    /// an old runtime).
    ScopedCall {
        scope: &'static str,
        limit_key: &'static str,
        service: &'static str,
        handler: &'static str,
        input: &'static str,
    },
    /// `const f = ctx.call(service, handler, <json>, { placement: "spread" })` —
    /// a future, every wire version (field 13, no wire bump).
    SpreadCall {
        service: &'static str,
        handler: &'static str,
        input: &'static str,
    },
    /// `const f = ctx.call(service, handler, <json>)` — a future.
    Call {
        service: &'static str,
        handler: &'static str,
        input: &'static str,
    },
    /// `await Promise.all(branches.map(async b => { for (const [name, json] of b)
    /// await ctx.run(name, () => json) }))` — parallel branches, synchronous
    /// closures: results are delivered in journal order, a branch waiting for its
    /// proposal does not block the other branch.
    Branches(Vec<Vec<(&'static str, &'static str)>>),
    /// `await` future number n (counted from 0, in creation order).
    Await(usize),
    /// `await ctx.all([...])` over futures number n.
    AwaitAll(Vec<usize>),
    /// `await ctx.run(name, () => <json>)`. `slow` = the closure runs longer
    /// than the keepalive interval (the runner holds it until it sees a
    /// `Keepalive`). `tokens` = the records (JSON) the closure appends to the
    /// ephemeral stream `ui` (`ctx.stream("ui")`, wire v4) before returning.
    Run {
        name: &'static str,
        json: &'static str,
        slow: bool,
        tokens: &'static [&'static str],
    },
    /// `return <json>`.
    Return(&'static str),
    /// `const f = ctx.signal(id)` — a future; no command goes into the journal.
    Signal(SigKey),
    /// `const { promise: f } = ctx.awakeable()` — a future on a numbered signal
    /// allocated from 17.
    Awakeable,
    /// `await ctx.race([...])` over futures number n: the winner is the ready
    /// future whose leaf stands **earliest** in the journal (CG-3).
    Race(Vec<usize>),
    /// `return` the value of the winner of the last `Race`: the JSON payload of
    /// a signal leaf, or `<json>` when the winner is a completion (`sleep`).
    ReturnWinner(&'static str),
    /// `return` the JSON payload of signal future number n.
    ReturnValueOf(usize),
    /// Wire v6: `ctx.set(key, (await f) + suffix)` on `get` future number n —
    /// the value is a JSON string; reuses the ref of the prefix read
    /// (`reusablePrefix`).
    SetExtended {
        key: &'static str,
        from: usize,
        suffix: &'static str,
    },
    /// Wire v6: `await ctx.run(name, () => (await f) + suffix)` — the result
    /// reuses the ref of the byte prefix of a segmented value assembled during
    /// the turn (`reuseFor`).
    RunExtended {
        name: &'static str,
        from: usize,
        suffix: &'static str,
    },
    /// `return await f` — returns the whole value (JSON) of `get` future number n.
    ReturnOf(usize),
}

/// A signal's identity: numbered or named.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SigKey {
    Idx(u32),
    Name(&'static str),
}

impl SigKey {
    pub fn id(&self) -> pb::signal_notification_message::SignalId {
        use pb::signal_notification_message::SignalId as S;
        match self {
            SigKey::Idx(i) => S::Idx(*i),
            SigKey::Name(n) => S::Name(n.to_string()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    Bidi,
    Rr,
}

impl Transport {
    pub fn mode(self) -> &'static str {
        match self {
            Transport::Bidi => "BIDI_STREAM",
            Transport::Rr => "REQUEST_RESPONSE",
        }
    }
}

/// What the SDK waits for when it suspends: a one-level tree — completion leaves
/// and signal leaves. `First`/`All` without signal leaves encode exactly as
/// before signal leaves existed.
#[derive(Clone, Debug, PartialEq)]
pub enum Spec {
    First(Vec<u32>),
    All(Vec<u32>),
    Tree {
        all: bool,
        completions: Vec<u32>,
        signals: Vec<SigKey>,
    },
}

impl Spec {
    fn parts(&self) -> (bool, Vec<u32>, Vec<SigKey>) {
        match self {
            Spec::First(ids) => (false, ids.clone(), vec![]),
            Spec::All(ids) => (true, ids.clone(), vec![]),
            Spec::Tree {
                all,
                completions,
                signals,
            } => (*all, completions.clone(), signals.clone()),
        }
    }

    fn message(&self) -> pb::SuspensionMessage {
        let (all, completions, signals) = self.parts();
        let node = pb::SuspensionMessage {
            waiting_completions: completions,
            waiting_signals: signals
                .iter()
                .filter_map(|s| match s {
                    SigKey::Idx(i) => Some(*i),
                    SigKey::Name(_) => None,
                })
                .collect(),
            waiting_named_signals: signals
                .iter()
                .filter_map(|s| match s {
                    SigKey::Name(n) => Some(n.to_string()),
                    SigKey::Idx(_) => None,
                })
                .collect(),
            ..Default::default()
        };
        if !all {
            return node;
        }
        // `ALL` wrapped in ONE child node — the root (FIRST) is satisfied when the child is.
        pb::SuspensionMessage {
            nested_futures: vec![pb::SuspensionMessage {
                combinator_type: pb::CombinatorType::AllCompleted as i32,
                ..node
            }],
            ..Default::default()
        }
    }

    fn satisfied(&self, entries: &[Frame]) -> bool {
        let done: BTreeSet<u32> = entries.iter().filter_map(notification_id).collect();
        let sigs: Vec<SigKey> = entries.iter().filter_map(signal_key).collect();
        let (all, completions, signals) = self.parts();
        let leaves: Vec<bool> = completions
            .iter()
            .map(|i| done.contains(i))
            .chain(signals.iter().map(|s| sigs.contains(s)))
            .collect();
        if all {
            leaves.iter().all(|x| *x)
        } else {
            leaves.iter().any(|x| *x)
        }
    }
}

/// The identity of a signal entry (`0xFBFF`), if it is one.
fn signal_key(f: &Frame) -> Option<SigKey> {
    use pb::signal_notification_message::SignalId as S;
    if f.ty != MessageType::SIGNAL {
        return None;
    }
    let Ok(Message::Signal(n)) = decode(f) else {
        return None;
    };
    match n.signal_id? {
        S::Idx(i) => Some(SigKey::Idx(i)),
        // `&'static`: the trace's signal names are constants of the scenario.
        S::Name(n) => Some(SigKey::Name(Box::leak(n.into_boxed_str()))),
    }
}

/// The JSON payload of a signal entry.
fn signal_value(f: &Frame) -> Option<Bytes> {
    use pb::signal_notification_message::Result as R;
    let Ok(Message::Signal(n)) = decode(f) else {
        return None;
    };
    match n.result? {
        R::Value(v) => Some(v.content),
        _ => None,
    }
}

pub fn frame<M: prost::Message>(ty: MessageType, flags: Flags, m: &M) -> Frame {
    let wire = encode_message(ty, flags, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

/// Whether the frame takes a place in the journal (a command or a notification).
pub fn is_journal_entry(f: &Frame) -> bool {
    matches!(f.ty.raw(), 0x0400..=0x04FF | 0x8000..=0xFBFF)
}

fn notification_id(f: &Frame) -> Option<u32> {
    if !matches!(f.ty.raw(), 0x8000..=0xFBFE) {
        return None;
    }
    Some(match decode(f).ok()? {
        Message::GetStateCompletion(n) => n.completion_id,
        Message::GetStateKeysCompletion(n) => n.completion_id,
        Message::GetPromiseCompletion(n) => n.completion_id,
        Message::PeekPromiseCompletion(n) => n.completion_id,
        Message::CompletePromiseCompletion(n) => n.completion_id,
        Message::SleepCompletion(n) => n.completion_id,
        Message::CallCompletion(n) => n.completion_id,
        Message::CallInvocationIdCompletion(n) => n.completion_id,
        Message::RunCompletion(n) => n.completion_id,
        Message::AttachInvocationCompletion(n) => n.completion_id,
        Message::GetInvocationOutputCompletion(n) => n.completion_id,
        _ => return None,
    })
}

/// Whether two commands of the same kind are **one** command — the TS SDK's
/// `commandDiff` rule: every field except the wake-up time of `Sleep`, the value
/// of `SetState` and the embedded result of `GetEagerState` (not deterministic,
/// or the runtime shell may seal them).
fn same_command(journaled: &Frame, built: &Frame) -> bool {
    match (decode(journaled), decode(built)) {
        (Ok(Message::Sleep(a)), Ok(Message::Sleep(b))) => {
            a.result_completion_id == b.result_completion_id && a.name == b.name
        }
        (Ok(Message::SetState(a)), Ok(Message::SetState(b))) => a.key == b.key && a.name == b.name,
        (Ok(Message::GetEagerState(a)), Ok(Message::GetEagerState(b))) => {
            a.key == b.key && a.name == b.name
        }
        _ => journaled.payload == built.payload,
    }
}

/// The message type name as the TS SDK prints it in a journal-mismatch message (`typeName`).
fn type_name(ty: MessageType) -> &'static str {
    match ty.raw() {
        0x0401 => "OUTPUT",
        0x0402 => "GET_STATE",
        0x0403 => "SET_STATE",
        0x0406 => "GET_STATE_KEYS",
        0x0407 => "GET_EAGER_STATE",
        0x0409 => "GET_PROMISE",
        0x040B => "COMPLETE_PROMISE",
        0x040C => "SLEEP",
        0x040D => "CALL",
        0x0411 => "RUN",
        _ => "?",
    }
}

/// The part of `Start` the SDK reads.
#[derive(Clone, Debug)]
struct StartInfo {
    known: u32,
    state_map: Option<BTreeMap<Bytes, Bytes>>,
    partial: bool,
    keepalive_ms: u64,
    replay_only: bool,
    /// `retry_count_since_last_stored_entry` — the `attempt` of stream records.
    retry: u32,
}

fn start_info(f: &Frame) -> StartInfo {
    let Ok(Message::Start(s)) = decode(f) else {
        panic!("the first frame is not Start");
    };
    // An empty map + partial=false is indistinguishable from runtime v1 on the wire.
    let state_map = (!s.state_map.is_empty() || s.partial_state).then(|| {
        s.state_map
            .iter()
            .map(|e| (e.key.clone(), e.value.clone()))
            .collect()
    });
    StartInfo {
        known: s.known_entries,
        state_map,
        partial: s.partial_state,
        keepalive_ms: s.keepalive_interval_ms.unwrap_or(0),
        replay_only: s.replay_only,
        retry: s.retry_count_since_last_stored_entry,
    }
}

/// Why a turn stops in the middle of the handler.
enum Stop {
    Suspend,
    Mismatch,
    BeyondJournal(u32),
    Terminal(u32, String),
}

/// One run of the handler on the local journal — the TS SDK's `Turn` + `runOnce`.
struct Turn {
    journaled: Vec<Frame>,
    completions: BTreeSet<u32>,
    /// The journal position of each leaf's first appearance — completions and
    /// signals on one sequence (the TS SDK's `Turn.orderOf`).
    completion_at: BTreeMap<u32, usize>,
    signal_at: Vec<(SigKey, usize, Option<Bytes>)>,
    next_awakeable: u32,
    cursor: usize,
    next_id: u32,
    out: Vec<Frame>,
    pending_runs: BTreeMap<u32, Frame>,
    suspended: Option<Spec>,
    mismatch: Option<(u32, String)>,
    replay_only: bool,
    live: bool,
    /// Records of the `ui` stream appended but not on the wire yet — the TS
    /// SDK's `flushStreams` flushes them before every turn-ending frame.
    stream: Vec<pb::StreamRecord>,
    /// The values (logical bytes, refs) of completions that have a value — wire
    /// v6 assembles them from the trace's segment table.
    values: BTreeMap<u32, (Vec<SegmentRef>, Bytes)>,
    /// Every segmented value assembled in the turn, in journal order — the
    /// source of reusable refs for `Output`/`ctx.run` (the TS SDK's
    /// `Segments.all`).
    assembled: Vec<(Vec<SegmentRef>, Bytes)>,
}

impl Turn {
    fn new(entries: &[Frame]) -> Self {
        let mut t = Turn {
            journaled: Vec::new(),
            completions: BTreeSet::new(),
            completion_at: BTreeMap::new(),
            signal_at: Vec::new(),
            next_awakeable: 17,
            cursor: 0,
            next_id: 1,
            out: Vec::new(),
            pending_runs: BTreeMap::new(),
            suspended: None,
            mismatch: None,
            replay_only: false,
            live: false,
            stream: Vec::new(),
            values: BTreeMap::new(),
            assembled: Vec::new(),
        };
        for (at, e) in entries.iter().enumerate() {
            match e.ty.raw() {
                0x0400 => {} // Input: the runtime's entry
                0x0401..=0x04FF => t.journaled.push(e.clone()),
                _ => {
                    if let (Some(id), Ok(m)) = (notification_id(e), decode(e)) {
                        if let Some(v) = cairn_protocol::value::values(&m).first() {
                            let l = cairn_protocol::value::Logical::of(Some(v))
                                .expect("a valid Value layout");
                            let full = super::sim::logical_bytes(&l);
                            if !l.refs().is_empty() {
                                t.assembled.push((l.refs().to_vec(), full.clone()));
                            }
                            t.values.insert(id, (l.refs().to_vec(), full));
                        }
                    }
                    if let Some(id) = notification_id(e) {
                        t.completions.insert(id);
                        t.completion_at.entry(id).or_insert(at);
                    }
                    if let Some(k) = signal_key(e) {
                        if !t.signal_at.iter().any(|(s, _, _)| *s == k) {
                            t.signal_at.push((k, at, signal_value(e)));
                        }
                    }
                }
            }
        }
        t
    }

    fn alloc(&mut self) -> u32 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn peek(&self) -> Option<MessageType> {
        self.journaled.get(self.cursor).map(|f| f.ty)
    }

    /// `true` = the command is already in the journal (not emitted again).
    fn command(&mut self, ty: MessageType, build: impl FnOnce() -> Frame) -> Result<bool, Stop> {
        self.command_named(ty, None, build)
    }

    /// [`command`](Self::command), plus: when `name` is given, the name of the
    /// journaled entry must equal it — `ctx.run`.
    fn command_named(
        &mut self,
        ty: MessageType,
        name: Option<&str>,
        build: impl FnOnce() -> Frame,
    ) -> Result<bool, Stop> {
        if self.suspended.is_some() {
            return Err(Stop::Suspend);
        }
        let at = self.cursor;
        self.cursor += 1;
        let built = build();
        if let Some(j) = self.journaled.get(at) {
            if j.ty != ty {
                self.mismatch = Some((
                    at as u32,
                    format!(
                        "entry {at}: journal has {}, current code produced {}",
                        type_name(j.ty),
                        type_name(ty)
                    ),
                ));
                return Err(Stop::Mismatch);
            }
            if let Some(name) = name {
                let was = <pb::RunCommandMessage as prost::Message>::decode(j.payload.clone())
                    .expect("the journaled Run decodes")
                    .name;
                if was != name {
                    let t = type_name(ty);
                    self.mismatch = Some((
                        at as u32,
                        format!(
                            "entry {at}: journal has {t} {was:?}, current code produced {t} {name:?}"
                        ),
                    ));
                    return Err(Stop::Mismatch);
                }
            }
            // The target, key, parameter and completion id must match too —
            // `same_command` skips the fields the TS SDK does not compare.
            if j.ty == ty && !same_command(j, &built) {
                let t = type_name(ty);
                self.mismatch = Some((
                    at as u32,
                    format!(
                        "entry {at}: journal has {t}, current code produced {t} (fields differ)"
                    ),
                ));
                return Err(Stop::Mismatch);
            }
            return Ok(true);
        }
        if self.replay_only {
            return Err(Stop::BeyondJournal(at as u32));
        }
        self.out.push(built);
        Ok(false)
    }

    fn demand(&mut self, id: u32) -> Result<(), Stop> {
        if self.completions.contains(&id) {
            return Ok(());
        }
        self.suspend(Spec::First(vec![id]))
    }

    fn suspend(&mut self, spec: Spec) -> Result<(), Stop> {
        self.suspended = Some(spec);
        Err(Stop::Suspend)
    }

    fn known(&self, id: u32) -> bool {
        self.completions.contains(&id)
    }

    fn signal(&self, k: &SigKey) -> Option<&(SigKey, usize, Option<Bytes>)> {
        self.signal_at.iter().find(|(s, _, _)| s == k)
    }

    /// A future is ready (every leaf has arrived), with the earliest position of its leaves.
    fn ready(&self, f: &Fut) -> Option<usize> {
        let mut at = usize::MAX;
        for id in &f.ids {
            at = at.min(*self.completion_at.get(id)?);
        }
        for k in &f.signals {
            at = at.min(self.signal(k)?.1);
        }
        Some(at)
    }

    /// `Output` (if not durable yet) then `End` — the TS SDK's `terminate`.
    fn terminate(&mut self, output: pb::OutputCommandMessage) {
        let at = self.cursor;
        self.cursor += 1;
        match self.journaled.get(at) {
            Some(j) if j.ty == MessageType::OUTPUT => {}
            Some(j) => {
                let why = format!(
                    "entry {at}: journal has {}, current code produced OUTPUT",
                    type_name(j.ty)
                );
                self.out
                    .push(error_frame(cairn_protocol::JOURNAL_MISMATCH, &why));
                return;
            }
            None => self
                .out
                .push(frame(MessageType::OUTPUT, Flags::NONE, &output)),
        }
        self.out
            .push(frame(MessageType::END, Flags::NONE, &pb::EndMessage {}));
    }
}

/// A handler future: completion leaves and signal leaves.
#[derive(Clone, Debug)]
struct Fut {
    ids: Vec<u32>,
    signals: Vec<SigKey>,
}

impl Fut {
    fn done() -> Self {
        Fut {
            ids: vec![],
            signals: vec![],
        }
    }

    fn on(id: u32) -> Self {
        Fut {
            ids: vec![id],
            signals: vec![],
        }
    }

    fn signal(k: SigKey) -> Self {
        Fut {
            ids: vec![],
            signals: vec![k],
        }
    }
}

/// The TS SDK's `reusablePrefix`: the number of leading refs of `refs` whose
/// exact bytes `next` starts with (`known` is the logical bytes of the value
/// carrying `refs`).
fn reusable_prefix(refs: &[SegmentRef], known: &[u8], next: &[u8]) -> usize {
    let mut k = 0;
    let mut off = 0usize;
    for r in refs {
        let end = off + r.len as usize;
        if end > next.len() || end > known.len() {
            break;
        }
        if next[off..end] != known[off..end] {
            return k;
        }
        k += 1;
        off = end;
    }
    k
}

/// The TS SDK's `Segments.reuseFor`: the longest prefix of `next` matching a
/// segmented value assembled during the turn.
fn reuse_for(assembled: &[(Vec<SegmentRef>, Bytes)], next: &[u8]) -> Vec<SegmentRef> {
    let mut best: Vec<SegmentRef> = vec![];
    let mut best_len = 0u64;
    for (refs, bytes) in assembled {
        let k = reusable_prefix(refs, bytes, next);
        let len: u64 = refs[..k].iter().map(|r| r.len).sum();
        if len > best_len {
            best = refs[..k].to_vec();
            best_len = len;
        }
    }
    best
}

/// The TS SDK's `writeValue`: reused refs + the rest; no ref ⇒ inline.
fn value_with(next: &[u8], reuse: &[SegmentRef]) -> pb::Value {
    if reuse.is_empty() {
        return value_bytes(next);
    }
    let off: u64 = reuse.iter().map(|r| r.len).sum();
    cairn_protocol::value::Logical::Segmented {
        refs: reuse.to_vec(),
        tail: Bytes::copy_from_slice(&next[off as usize..]),
        len: next.len() as u64,
    }
    .encode()
}

fn value_bytes(b: &[u8]) -> pb::Value {
    cairn_protocol::value::inline(Bytes::copy_from_slice(b))
}

/// A JSON string `"…"` with `suffix` appended (no characters needing escapes) —
/// the TS SDK's `h + suffix` then `JSON.stringify`.
fn json_append(json: &[u8], suffix: &str) -> Vec<u8> {
    assert!(json.len() >= 2 && json[0] == b'"' && json[json.len() - 1] == b'"');
    let mut out = json[..json.len() - 1].to_vec();
    out.extend_from_slice(suffix.as_bytes());
    out.push(b'"');
    out
}

/// A JSON payload (absent ⇒ `null`) as a `&'static str` for `Return`.
fn leak(v: Option<Bytes>) -> &'static str {
    match v {
        Some(b) => Box::leak(
            String::from_utf8(b.to_vec())
                .expect("JSON utf-8")
                .into_boxed_str(),
        ),
        None => "null",
    }
}

fn error_frame(code: u32, message: &str) -> Frame {
    frame(
        MessageType::ERROR,
        Flags::NONE,
        &pb::ErrorMessage {
            code,
            message: message.to_string(),
            behavior: pb::ErrorBehavior::Pause as i32,
            ..Default::default()
        },
    )
}

fn value(json: &str) -> pb::Value {
    cairn_protocol::value::inline(Bytes::copy_from_slice(json.as_bytes()))
}

fn bytes(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

/// The SDK's environment constants in a trace.
#[derive(Clone, Copy, Debug)]
pub struct Env {
    pub wire: u32,
    pub transport: Transport,
    /// The SDK's clock (`Date.now()`), fixed for the whole trace — `ctx.sleep(ms)`
    /// journals `clock_ms + ms`.
    pub clock_ms: u64,
}

/// Advances branch `i` of [`branches_once`] until it waits; returns a proposal
/// (the closure that just ran) if any.
fn advance(
    t: &mut Turn,
    branches: &[Vec<(&'static str, &'static str)>],
    i: usize,
    step: &mut [usize],
    waiting: &mut [Option<u32>],
    delivered: &BTreeSet<u32>,
) -> Result<Option<(u32, &'static str)>, Stop> {
    loop {
        let Some((name, json)) = branches[i].get(step[i]).copied() else {
            return Ok(None);
        };
        step[i] += 1;
        let id = t.alloc();
        t.command_named(MessageType::RUN, Some(name), || {
            frame(
                MessageType::RUN,
                Flags::NONE,
                &pb::RunCommandMessage {
                    result_completion_id: id,
                    name: name.to_string(),
                },
            )
        })?;
        if t.known(id) {
            if delivered.contains(&id) {
                continue;
            }
            waiting[i] = Some(id);
            return Ok(None);
        }
        return Ok(Some((id, json)));
    }
}

/// `Op::Branches` for synchronous closures:
///
/// 1. The first synchronous pass: each branch in order emits the `Run` of its
///    first step; a result in the journal ⇒ wait for the delivery; none ⇒ the
///    closure runs, the proposals go out **after** the synchronous pass (one
///    microtask), in branch order, then the branch waits outside.
/// 2. Deliver results in journal order; a woken branch moves on at once (one
///    drain): a next step already delivered goes on, one in the journal waits,
///    one not there runs its closure and proposes right after its command.
/// 3. Deliveries exhausted while branches still wait outside ⇒ suspend on
///    `FIRST` of every proposed id.
fn branches_once(
    t: &mut Turn,
    entries: &[Frame],
    branches: &[Vec<(&'static str, &'static str)>],
    env: Env,
) -> Result<(), Stop> {
    // Leaves in journal order (position of first appearance).
    let mut order: Vec<(usize, u32)> = t.completion_at.iter().map(|(id, at)| (*at, *id)).collect();
    order.sort();
    let mut delivered: BTreeSet<u32> = BTreeSet::new();
    let n = branches.len();
    let mut step = vec![0usize; n];
    // The branch waiting for the delivery of this completion.
    let mut waiting: Vec<Option<u32>> = vec![None; n];
    let mut outside: BTreeSet<u32> = BTreeSet::new();
    let flags = match env.transport {
        Transport::Bidi => Flags::REQUESTED_ACK,
        Transport::Rr => Flags::NONE,
    };
    let _ = entries;

    let propose = |t: &mut Turn, id: u32, json: &'static str| {
        use pb::propose_run_completion_message::Result as P;
        t.out.push(frame(
            MessageType::PROPOSE_RUN_COMPLETION,
            flags,
            &pb::ProposeRunCompletionMessage {
                result_completion_id: id,
                result: Some(P::Value(bytes(json))),
            },
        ));
        use pb::run_completion_notification_message::Result as R;
        t.pending_runs.insert(
            id,
            frame(
                MessageType::RUN_COMPLETION,
                Flags::NONE,
                &pb::RunCompletionNotificationMessage {
                    completion_id: id,
                    result: Some(R::Value(value(json))),
                },
            ),
        );
    };

    let mut first: Vec<(u32, &'static str)> = Vec::new();
    for i in 0..n {
        if let Some(p) = advance(t, branches, i, &mut step, &mut waiting, &delivered)? {
            first.push(p);
        }
    }
    for (id, json) in first {
        propose(t, id, json);
        outside.insert(id);
    }
    for (_, id) in order {
        delivered.insert(id);
        for i in 0..n {
            if waiting[i] != Some(id) {
                continue;
            }
            waiting[i] = None;
            if let Some((p, json)) = advance(t, branches, i, &mut step, &mut waiting, &delivered)? {
                propose(t, p, json);
                outside.insert(p);
            }
        }
    }
    let done =
        (0..n).all(|i| step[i] == branches[i].len() && waiting[i].is_none()) && outside.is_empty();
    if done {
        return Ok(());
    }
    t.suspend(Spec::First(outside.into_iter().collect()))
}

fn run_once(ops: &[Op], start: &StartInfo, entries: &[Frame], env: Env) -> Turn {
    let mut t = Turn::new(entries);
    t.replay_only = env.wire >= 3 && start.replay_only;
    t.live = env.transport == Transport::Bidi && env.wire >= 3 && start.keepalive_ms > 0;
    let mut state_map = start.state_map.clone();
    let mut futures: Vec<Fut> = Vec::new();
    let mut winner: Option<usize> = None;

    let res = (|| -> Result<&'static str, Stop> {
        for op in ops {
            match op {
                Op::Get(key) => {
                    let journaled = t.peek();
                    let eager = journaled == Some(MessageType::GET_EAGER_STATE)
                        || (journaled.is_none()
                            && state_map
                                .as_ref()
                                .is_some_and(|m| !start.partial || m.contains_key(key.as_bytes())));
                    if eager {
                        let v = state_map
                            .as_ref()
                            .and_then(|m| m.get(key.as_bytes()).cloned());
                        t.command(MessageType::GET_EAGER_STATE, || {
                            use pb::get_eager_state_command_message::Result as R;
                            frame(
                                MessageType::GET_EAGER_STATE,
                                Flags::NONE,
                                &pb::GetEagerStateCommandMessage {
                                    key: bytes(key),
                                    name: key.to_string(),
                                    result: Some(match v {
                                        Some(content) => {
                                            R::Value(cairn_protocol::value::inline(content))
                                        }
                                        None => R::Void(pb::Void {}),
                                    }),
                                },
                            )
                        })?;
                        futures.push(Fut::done());
                    } else {
                        let id = t.alloc();
                        t.command(MessageType::GET_STATE, || {
                            frame(
                                MessageType::GET_STATE,
                                Flags::NONE,
                                &pb::GetStateCommandMessage {
                                    key: bytes(key),
                                    result_completion_id: id,
                                    name: key.to_string(),
                                },
                            )
                        })?;
                        futures.push(Fut::on(id));
                    }
                }
                Op::Set(key, json) => {
                    t.command(MessageType::SET_STATE, || {
                        frame(
                            MessageType::SET_STATE,
                            Flags::NONE,
                            &pb::SetStateCommandMessage {
                                key: bytes(key),
                                value: Some(value(json)),
                                name: key.to_string(),
                            },
                        )
                    })?;
                    if let Some(m) = state_map.as_mut() {
                        m.insert(bytes(key), bytes(json));
                    }
                }
                Op::Sleep(ms) => {
                    let id = t.alloc();
                    let at = env.clock_ms + ms;
                    t.command(MessageType::SLEEP, || {
                        frame(
                            MessageType::SLEEP,
                            Flags::NONE,
                            &pb::SleepCommandMessage {
                                wake_up_time: at,
                                result_completion_id: id,
                                name: "sleep".into(),
                            },
                        )
                    })?;
                    futures.push(Fut::on(id));
                }
                Op::GetKeys => {
                    let id = t.alloc();
                    t.command(MessageType::GET_STATE_KEYS, || {
                        frame(
                            MessageType::GET_STATE_KEYS,
                            Flags::NONE,
                            &pb::GetStateKeysCommandMessage {
                                result_completion_id: id,
                                name: "getKeys".into(),
                            },
                        )
                    })?;
                    futures.push(Fut::on(id));
                }
                Op::PromiseResolve(name, json) => {
                    let id = t.alloc();
                    t.command(MessageType::COMPLETE_PROMISE, || {
                        use pb::complete_promise_command_message::Completion as C;
                        frame(
                            MessageType::COMPLETE_PROMISE,
                            Flags::NONE,
                            &pb::CompletePromiseCommandMessage {
                                key: name.to_string(),
                                completion: Some(C::CompletionValue(value(json))),
                                result_completion_id: id,
                                name: format!("promise/{name}"),
                            },
                        )
                    })?;
                    t.demand(id)?;
                }
                Op::PromiseGet(name) => {
                    let id = t.alloc();
                    t.command(MessageType::GET_PROMISE, || {
                        frame(
                            MessageType::GET_PROMISE,
                            Flags::NONE,
                            &pb::GetPromiseCommandMessage {
                                key: name.to_string(),
                                result_completion_id: id,
                                name: format!("promise/{name}"),
                            },
                        )
                    })?;
                    futures.push(Fut::on(id));
                }
                Op::ScopedCall {
                    scope,
                    limit_key,
                    service,
                    handler,
                    input,
                } => {
                    if env.wire < 2 {
                        return Err(Stop::Terminal(
                            500,
                            format!(
                                "ctx.scoped needs wire v2; the other end speaks v{}",
                                env.wire
                            ),
                        ));
                    }
                    let idc = t.alloc();
                    let rc = t.alloc();
                    t.command(MessageType::CALL, || {
                        frame(
                            MessageType::CALL,
                            Flags::NONE,
                            &pb::CallCommandMessage {
                                service_name: service.to_string(),
                                handler_name: handler.to_string(),
                                parameter: bytes(input),
                                scope: Some(scope.to_string()),
                                limit_key: Some(limit_key.to_string()),
                                invocation_id_notification_idx: idc,
                                result_completion_id: rc,
                                name: format!("{service}/{handler}"),
                                ..Default::default()
                            },
                        )
                    })?;
                    futures.push(Fut::on(rc));
                }
                Op::SpreadCall {
                    service,
                    handler,
                    input,
                } => {
                    let idc = t.alloc();
                    let rc = t.alloc();
                    t.command(MessageType::CALL, || {
                        frame(
                            MessageType::CALL,
                            Flags::NONE,
                            &pb::CallCommandMessage {
                                service_name: service.to_string(),
                                handler_name: handler.to_string(),
                                parameter: bytes(input),
                                invocation_id_notification_idx: idc,
                                result_completion_id: rc,
                                name: format!("{service}/{handler}"),
                                placement: pb::CallPlacement::Spread as i32,
                                ..Default::default()
                            },
                        )
                    })?;
                    futures.push(Fut::on(rc));
                }
                Op::Call {
                    service,
                    handler,
                    input,
                } => {
                    let idc = t.alloc();
                    let rc = t.alloc();
                    t.command(MessageType::CALL, || {
                        frame(
                            MessageType::CALL,
                            Flags::NONE,
                            &pb::CallCommandMessage {
                                service_name: service.to_string(),
                                handler_name: handler.to_string(),
                                parameter: bytes(input),
                                invocation_id_notification_idx: idc,
                                result_completion_id: rc,
                                name: format!("{service}/{handler}"),
                                ..Default::default()
                            },
                        )
                    })?;
                    futures.push(Fut::on(rc));
                }
                Op::Branches(branches) => branches_once(&mut t, entries, branches, env)?,
                Op::Await(n) => {
                    for id in futures[*n].ids.clone() {
                        t.demand(id)?;
                    }
                    for k in futures[*n].signals.clone() {
                        if t.signal(&k).is_none() {
                            t.suspend(Spec::Tree {
                                all: false,
                                completions: vec![],
                                signals: vec![k],
                            })?;
                        }
                    }
                }
                Op::AwaitAll(ns) => {
                    // The TS SDK's `ctx.all`: the leaves of every future **not** ready yet.
                    let pending: Vec<&Fut> = ns
                        .iter()
                        .map(|n| &futures[*n])
                        .filter(|f| t.ready(f).is_none())
                        .collect();
                    if !pending.is_empty() {
                        let completions: Vec<u32> =
                            pending.iter().flat_map(|f| f.ids.clone()).collect();
                        let signals: Vec<SigKey> =
                            pending.iter().flat_map(|f| f.signals.clone()).collect();
                        t.suspend(if signals.is_empty() {
                            Spec::All(completions)
                        } else {
                            Spec::Tree {
                                all: true,
                                completions,
                                signals,
                            }
                        })?;
                    }
                }
                Op::Signal(k) => futures.push(Fut::signal(k.clone())),
                Op::Awakeable => {
                    let idx = t.next_awakeable;
                    t.next_awakeable += 1;
                    futures.push(Fut::signal(SigKey::Idx(idx)));
                }
                Op::Race(ns) => {
                    let best = ns
                        .iter()
                        .filter_map(|n| t.ready(&futures[*n]).map(|at| (at, *n)))
                        .min();
                    match best {
                        Some((_, n)) => winner = Some(n),
                        None => {
                            let completions: Vec<u32> =
                                ns.iter().flat_map(|n| futures[*n].ids.clone()).collect();
                            let signals: Vec<SigKey> = ns
                                .iter()
                                .flat_map(|n| futures[*n].signals.clone())
                                .collect();
                            t.suspend(if signals.is_empty() {
                                Spec::First(completions)
                            } else {
                                Spec::Tree {
                                    all: false,
                                    completions,
                                    signals,
                                }
                            })?;
                        }
                    }
                }
                Op::ReturnWinner(fallback) => {
                    let f = &futures[winner.expect("ReturnWinner after Race")];
                    return Ok(match f.signals.first() {
                        Some(k) => leak(t.signal(k).and_then(|s| s.2.clone())),
                        None => fallback,
                    });
                }
                Op::ReturnValueOf(n) => {
                    let k = futures[*n].signals.first().expect("future signal").clone();
                    return Ok(leak(t.signal(&k).and_then(|s| s.2.clone())));
                }
                Op::Run {
                    name,
                    json,
                    slow,
                    tokens,
                } => {
                    let id = t.alloc();
                    t.command_named(MessageType::RUN, Some(name), || {
                        frame(
                            MessageType::RUN,
                            Flags::NONE,
                            &pb::RunCommandMessage {
                                result_completion_id: id,
                                name: name.to_string(),
                            },
                        )
                    })?;
                    if t.known(id) {
                        continue;
                    }
                    // The closure runs: the stream records (wire v4) carry the
                    // identity `(position of this very Run command, attempt, k)`;
                    // nothing is sent while replaying.
                    if env.wire >= 4 && !t.replay_only && t.cursor >= t.journaled.len() {
                        for (k, tok) in tokens.iter().enumerate() {
                            t.stream.push(pb::StreamRecord {
                                step: (t.cursor - 1) as u32,
                                attempt: start.retry,
                                k: k as u32,
                                payload: bytes(tok),
                            });
                        }
                    }
                    // A long closure on a live channel: the `Run` command is on
                    // the wire, then a `Keepalive` every interval until the
                    // closure finishes (side channel).
                    if t.live && *slow {
                        t.out.push(frame(
                            MessageType::KEEPALIVE,
                            Flags::NONE,
                            &pb::KeepaliveMessage {},
                        ));
                    }
                    // `REQUESTED_ACK` only on `BIDI_STREAM` — section 4:
                    // `REQUEST_RESPONSE` has no channel for the ack to come back on.
                    let flags = match env.transport {
                        Transport::Bidi => Flags::REQUESTED_ACK,
                        Transport::Rr => Flags::NONE,
                    };
                    use pb::propose_run_completion_message::Result as P;
                    t.out.push(frame(
                        MessageType::PROPOSE_RUN_COMPLETION,
                        flags,
                        &pb::ProposeRunCompletionMessage {
                            result_completion_id: id,
                            result: Some(P::Value(bytes(json))),
                        },
                    ));
                    use pb::run_completion_notification_message::Result as R;
                    t.pending_runs.insert(
                        id,
                        frame(
                            MessageType::RUN_COMPLETION,
                            Flags::NONE,
                            &pb::RunCompletionNotificationMessage {
                                completion_id: id,
                                result: Some(R::Value(value(json))),
                            },
                        ),
                    );
                    t.suspend(Spec::First(vec![id]))?;
                }
                Op::SetExtended { key, from, suffix } => {
                    let id = futures[*from].ids[0];
                    let (refs, known) = t
                        .values
                        .get(&id)
                        .cloned()
                        .expect("the get already has a result");
                    let next = json_append(&known, suffix);
                    let reuse = if env.wire >= 6 {
                        refs[..reusable_prefix(&refs, &known, &next)].to_vec()
                    } else {
                        vec![]
                    };
                    t.command(MessageType::SET_STATE, || {
                        frame(
                            MessageType::SET_STATE,
                            Flags::NONE,
                            &pb::SetStateCommandMessage {
                                key: bytes(key),
                                value: Some(value_with(&next, &reuse)),
                                name: key.to_string(),
                            },
                        )
                    })?;
                }
                Op::RunExtended { name, from, suffix } => {
                    let id = t.alloc();
                    t.command_named(MessageType::RUN, Some(name), || {
                        frame(
                            MessageType::RUN,
                            Flags::NONE,
                            &pb::RunCommandMessage {
                                result_completion_id: id,
                                name: name.to_string(),
                            },
                        )
                    })?;
                    if t.known(id) {
                        continue;
                    }
                    let src = futures[*from].ids[0];
                    let (_, known) = t
                        .values
                        .get(&src)
                        .cloned()
                        .expect("the get already has a result");
                    let next = json_append(&known, suffix);
                    let reuse = reuse_for(&t.assembled, &next);
                    use pb::propose_run_completion_message::Result as P;
                    let result = if reuse.is_empty() {
                        P::Value(Bytes::from(next.clone()))
                    } else {
                        P::SegmentedValue(value_with(&next, &reuse))
                    };
                    let flags = match env.transport {
                        Transport::Bidi => Flags::REQUESTED_ACK,
                        Transport::Rr => Flags::NONE,
                    };
                    t.out.push(frame(
                        MessageType::PROPOSE_RUN_COMPLETION,
                        flags,
                        &pb::ProposeRunCompletionMessage {
                            result_completion_id: id,
                            result: Some(result),
                        },
                    ));
                    t.suspend(Spec::First(vec![id]))?;
                }
                Op::ReturnOf(n) => {
                    let id = futures[*n].ids[0];
                    let (_, v) = t
                        .values
                        .get(&id)
                        .cloned()
                        .expect("the get already has a result");
                    return Ok(leak(Some(v)));
                }
                Op::Return(json) => return Ok(json),
            }
        }
        Ok("null")
    })();

    // The TS SDK's `flushStreams`: the batched records go on the wire before the
    // turn-ending frame (`Output`/`End`, `Suspension`, `Error`).
    if !t.stream.is_empty() {
        t.out.push(frame(
            MessageType::STREAM_APPEND,
            Flags::NONE,
            &pb::StreamAppendMessage {
                name: "ui".into(),
                durable: false,
                records: std::mem::take(&mut t.stream),
                close: false,
            },
        ));
    }
    use pb::output_command_message::Result as O;
    match res {
        Ok(json) => match t.suspended.clone() {
            Some(spec) => t
                .out
                .push(frame(MessageType::SUSPENSION, Flags::NONE, &spec.message())),
            None => {
                // Wire v6: `Output` reuses the ref of the prefix of an assembled
                // value (`turn.reuseFor`).
                let reuse = if env.wire >= 6 {
                    reuse_for(&t.assembled, json.as_bytes())
                } else {
                    vec![]
                };
                t.terminate(pb::OutputCommandMessage {
                    result: Some(O::Value(value_with(json.as_bytes(), &reuse))),
                    name: "output".into(),
                })
            }
        },
        Err(stop) => {
            if t.replay_only {
                let report = match (&stop, &t.mismatch) {
                    (_, Some((at, why))) => Some(pb::ReplayReportMessage {
                        matched: *at,
                        mismatch: true,
                        message: why.clone(),
                        ..Default::default()
                    }),
                    (Stop::BeyondJournal(at), None) => Some(pb::ReplayReportMessage {
                        matched: *at,
                        ..Default::default()
                    }),
                    _ => None,
                };
                if let Some(r) = report {
                    t.out
                        .push(frame(MessageType::REPLAY_REPORT, Flags::NONE, &r));
                    return t;
                }
            }
            if let Some((_, why)) = t.mismatch.clone() {
                t.out
                    .push(error_frame(cairn_protocol::JOURNAL_MISMATCH, &why));
            } else if let Some(spec) = t.suspended.clone() {
                t.out
                    .push(frame(MessageType::SUSPENSION, Flags::NONE, &spec.message()));
            } else if let Stop::Terminal(code, message) = stop {
                t.terminate(pb::OutputCommandMessage {
                    result: Some(O::Failure(pb::Failure {
                        code,
                        message,
                        metadata: vec![],
                    })),
                    name: "output".into(),
                });
            } else {
                unreachable!("a stop reason not in the model");
            }
        }
    }
    t
}

/// One `REQUEST_RESPONSE` turn: the whole request body in, the whole response body out.
pub fn rr_turn(ops: &[Op], request: &[Frame], env: Env) -> Vec<Frame> {
    let start = start_info(&request[0]);
    run_once(ops, &start, &request[1..], env).out
}

#[derive(Debug, PartialEq)]
enum Phase {
    AwaitStart,
    Replay(u32),
    Waiting,
    Done,
}

/// The SDK end of a `BIDI_STREAM` stream — the TS SDK's `runBidi`.
pub struct BidiSdk {
    ops: Vec<Op>,
    env: Env,
    phase: Phase,
    start: Option<StartInfo>,
    entries: Vec<Frame>,
    pending_runs: BTreeMap<u32, Frame>,
    spec: Option<Spec>,
    pub outbox: VecDeque<Frame>,
}

impl BidiSdk {
    pub fn new(ops: Vec<Op>, env: Env) -> Self {
        Self {
            ops,
            env,
            phase: Phase::AwaitStart,
            start: None,
            entries: Vec::new(),
            pending_runs: BTreeMap::new(),
            spec: None,
            outbox: VecDeque::new(),
        }
    }

    pub fn done(&self) -> bool {
        self.phase == Phase::Done
    }

    /// Reads frames from the runtime as the TS SDK does: the replayed part, exactly
    /// `known_entries` of them, then **everything that has arrived** (`buffered()`);
    /// when suspended, one at a time until what it waits for is complete — the
    /// rest waits for the next suspension.
    pub fn step(&mut self, inbox: &mut VecDeque<Frame>) {
        loop {
            match self.phase {
                Phase::AwaitStart => {
                    let Some(f) = inbox.pop_front() else { return };
                    let s = start_info(&f);
                    self.phase = Phase::Replay(s.known);
                    self.start = Some(s);
                }
                Phase::Replay(0) => {
                    for f in inbox.drain(..) {
                        if is_journal_entry(&f) {
                            self.entries.push(f);
                        }
                    }
                    self.run();
                    return;
                }
                Phase::Replay(n) => {
                    let Some(f) = inbox.pop_front() else { return };
                    if is_journal_entry(&f) {
                        self.entries.push(f);
                    }
                    self.phase = Phase::Replay(n - 1);
                }
                Phase::Waiting => {
                    let Some(f) = inbox.pop_front() else { return };
                    if f.ty == MessageType::PROPOSE_RUN_COMPLETION_ACK {
                        let Ok(Message::ProposeRunCompletionAck(a)) = decode(&f) else {
                            unreachable!()
                        };
                        if let Some(settled) = self.pending_runs.get(&a.completion_id) {
                            self.entries.push(settled.clone());
                        }
                    }
                    if is_journal_entry(&f) {
                        self.entries.push(f);
                    }
                    // The next turn runs only once **every** proposal of this
                    // turn is durable — the TS SDK's `runBidi` (parallel branches
                    // propose several).
                    let acked = self
                        .pending_runs
                        .keys()
                        .all(|id| self.entries.iter().any(|e| notification_id(e) == Some(*id)));
                    if acked && self.spec.as_ref().unwrap().satisfied(&self.entries) {
                        self.run();
                        return;
                    }
                }
                Phase::Done => return,
            }
        }
    }

    fn run(&mut self) {
        let t = run_once(
            &self.ops,
            self.start.as_ref().unwrap(),
            &self.entries,
            self.env,
        );
        for f in &t.out {
            if is_journal_entry(f) {
                self.entries.push(f.clone());
            }
        }
        self.outbox.extend(t.out.iter().cloned());
        let ends_suspended = t.out.last().map(|f| f.ty) == Some(MessageType::SUSPENSION);
        match t.suspended {
            Some(spec) if ends_suspended => {
                self.spec = Some(spec);
                self.pending_runs = t.pending_runs;
                self.phase = Phase::Waiting;
            }
            _ => self.phase = Phase::Done,
        }
    }
}
