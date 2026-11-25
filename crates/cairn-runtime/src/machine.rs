// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The invocation state machine — sections 6 and 8 of `protocol-v1.md`.
//!
//! **This machine is pure: no I/O, no clock, no randomness.** It takes
//! [`Event`]s and returns [`Effect`]s; the driver is what touches storage, the
//! network, timers.
//!
//! That is not an architectural taste but a condition for testability. Mandatory
//! test number 1 of section 10 — *"kill at every command index, replay, confirm
//! the journal and output are identical"* — only runs if rebuilding the machine
//! from a journal is cheap and deterministic. Put storage in here and that test
//! becomes an infrastructure exercise, and nobody will run it at every index.
//!
//! **The order of effects is a contract.** [`Effect::Persist`] comes before the
//! [`Effect::Send`] of the same step, and the driver MUST finish committing the
//! persist part before sending. Sending before durability promises the SDK
//! something not yet certain — the same kind of mistake as acking before
//! `await_durable` (INV-5).

use {
    crate::journal::{Journal, JournalEntry},
    bytes::Bytes,
    cairn_protocol::{
        decode, pb, Flags, Frame, Message, MessageKind, MessageType, JOURNAL_MISMATCH,
        PROTOCOL_VIOLATION,
    },
    std::collections::BTreeMap,
};

/// An invocation id — 24 bytes, must match `cairn_store::ID_LEN` (the engine
/// checks it at compile time; this crate does not depend on the store).
pub type InvocationId = [u8; 24];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Replaying stored entries. The SDK reruns the user code.
    Replay,
    /// The SDK sends new commands, the runtime stores them and returns notifications.
    Processing,
    Closed,
}

/// Work the runtime must do outside the state machine. The result comes back
/// through [`Event::Completion`].
///
/// **Every task must be idempotent.** When an invocation restarts, the machine
/// re-issues tasks for the pending completions — it has no way of knowing how
/// far the old task got. For `Invoke`, idempotency comes from `idempotency_key`
/// (spec section 8.6).
#[derive(Clone, Debug, PartialEq)]
pub enum Task {
    ReadState {
        completion_id: u32,
        key: Bytes,
    },
    /// Read the list of the object's state keys (`GetStateKeys`).
    ReadStateKeys {
        completion_id: u32,
    },
    /// `value = None` means delete. Segmented only when the frame arrives
    /// through [`Event::Sealed`] (the shell has sealed it).
    WriteState {
        key: Bytes,
        value: Option<cairn_protocol::value::Logical>,
    },
    /// Delete ALL of the object's state (`ClearAllState`).
    ClearAllState,
    /// Wait for a durable promise to be completed (`GetPromise`).
    PromiseGet {
        completion_id: u32,
        key: String,
    },
    /// Look at a promise without waiting (`PeekPromise`).
    PromisePeek {
        completion_id: u32,
        key: String,
    },
    /// Complete a promise (`CompletePromise`). Idempotent per
    /// `(invocation, completion_id)`: rerunning after a crash must not see
    /// itself as "the one who came later".
    PromiseComplete {
        completion_id: u32,
        key: String,
        result: pb::complete_promise_command_message::Completion,
    },
    /// Wait for the result of another invocation (`AttachInvocation`).
    Attach {
        completion_id: u32,
        target_invocation_id: String,
    },
    /// Look at the result of another invocation without waiting (`GetInvocationOutput`).
    PeekOutput {
        completion_id: u32,
        target_invocation_id: String,
    },
    ScheduleTimer {
        completion_id: u32,
        wake_up_time: u64,
    },
    Invoke {
        /// `None` for `OneWayCall` — it does not wait for a result.
        result_completion_id: Option<u32>,
        invocation_id_completion_id: u32,
        service: String,
        handler: String,
        key: String,
        parameter: Bytes,
        idempotency_key: Option<String>,
        /// An absolute instant; 0 = as soon as possible.
        invoke_time: u64,
        /// `CallCommandMessage.scope` (field 7) — flow control v2.
        ///
        /// The state machine **does not interpret** the two fields below: it
        /// only carries them from the command to the task. Interpreting them
        /// (checking the format, building the `QueueName`) is the driver's job,
        /// and keeping it in exactly one place is the condition for replay never
        /// depending on the current rules — a scope valid yesterday but refused
        /// today must break *a new call*, not an old journal.
        scope: Option<String>,
        /// `CallCommandMessage.limit_key` (field 8).
        limit_key: Option<String>,
        /// `placement = CALL_PLACEMENT_SPREAD` (field 13). Like `scope`: the
        /// state machine only carries it, the engine decides (`callee_pk_for`)
        /// by the caller's entry-0 mark. An unknown enum value (a newer SDK) ⇒
        /// `false`: this binary's default.
        spread: bool,
        /// `CallCommandMessage.tags` (field 14) — carried verbatim; the engine
        /// checks them and drops tags that break the rules.
        tags: Vec<(String, String)>,
    },
    SendSignal {
        target_invocation_id: String,
        signal: pb::send_signal_command_message::SignalId,
        /// The result that comes with the signal (void/value/failure), handed to the target intact.
        result: Option<pb::send_signal_command_message::Result>,
        /// The index of this very command in the sender's journal. Together
        /// with the sender's id it is the deterministic identity of the send —
        /// the driver uses it as the queue key so a replay overwrites rather
        /// than sends again. (SendSignal has no completion id — it is not
        /// completable — so it cannot use an `(invocation, completion)` key like
        /// `Invoke`.)
        entry_index: u32,
    },
}

/// The result of a completion, supplied by the driver.
#[derive(Clone, Debug, PartialEq)]
pub enum CompletionResult {
    /// `None` = the key does not exist. Quite unlike `Some(empty)` = it exists,
    /// with an empty value. Segmented ⇒ the notification carries ref + tail; the
    /// shell assembles it before sending it down to an SDK ≤ v5.
    State(Option<cairn_protocol::value::Logical>),
    /// The list of the object's state keys (`GetStateKeys`).
    StateKeys(Vec<Bytes>),
    Timer,
    /// The notification template's `Void`: "nothing" — a promise not completed
    /// yet (`PeekPromise`), an invocation not finished yet
    /// (`GetInvocationOutput`), or completing a promise won (`CompletePromise`).
    Void,
    /// A success value. Used for `Call`, promises, attach, get-output — the
    /// concrete notification is decided by the kind of completion promised.
    CallValue(Bytes),
    /// Like [`Self::CallValue`] for a segmented result that the shell has
    /// COPYed to the receiver's owner: the notification carries ref + tail. Only
    /// `Call`, attach and get-output — promises stay inline.
    CallSegmented(cairn_protocol::value::Logical),
    CallFailure(pb::Failure),
    CallInvocationId(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Completion {
    pub id: u32,
    pub result: CompletionResult,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    /// Open or reopen the invocation: send `Start`, then replay the journal.
    Start,
    FromSdk(Frame),
    /// An SDK frame that **the shell** has rewritten into a segmented value
    /// (`SetState`, `GetEagerState`). Like [`Event::FromSdk`], differing only in
    /// the `Value` check: `FromSdk` carrying `segments` is 571, this frame may
    /// (the refs are issued by the runtime; the engine checks they exist).
    Sealed(Frame),
    Completion(Completion),
    /// A signal from outside (another invocation, or an admin CANCEL). It
    /// differs from [`Event::Completion`] in identity: a signal is identified by
    /// idx/name, not by completion id, and a duplicate is ignored rather than a
    /// violation.
    Signal(pb::SignalNotificationMessage),
}

/// What the runtime does when an invocation fails (spec section 8.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureAction {
    Retry {
        delay_ms: Option<u64>,
    },
    /// Suspend, waiting for manual intervention.
    Pause,
    Fail,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    Completed(pb::OutputCommandMessage),
    Suspended(pb::SuspensionMessage),
    Failed {
        code: u32,
        message: String,
        action: FailureAction,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum Effect {
    Persist {
        entry_index: u32,
        entry: JournalEntry,
    },
    Send(Message),
    Task(Task),
    /// `0xFC00+` — the runtime forwards the raw bytes, does not parse (spec section 5).
    ForwardCustom {
        ty: MessageType,
        payload: Bytes,
    },
    Finish(Outcome),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MachineError {
    #[error("journal corrupt at entry {entry_index}: {reason}")]
    CorruptJournal { entry_index: u32, reason: String },
}

/// The `StartMessage` parameters the machine cannot produce itself (spec
/// section 8.4: every source of non-determinism must have a name).
#[derive(Clone, Debug, Default)]
pub struct StartParams {
    pub debug_id: String,
    pub key: String,
    pub random_seed: u64,
    pub idempotency_key: Option<String>,
    pub retry_count_since_last_stored_entry: u32,
    pub duration_since_last_stored_entry: u64,
    /// Eager state: all (or part) of the object's state, captured at load time.
    pub state_map: Vec<(Bytes, Bytes)>,
    /// `true` = `state_map` is only part of it; absent keys must be read lazily.
    pub partial_state: bool,
    /// This handler may **only read** state.
    ///
    /// Turned on in two places: a workflow's `SHARED` handler (the engine, from
    /// the `InvocationKind` of entry 0), and a virtual object's `SHARED` handler
    /// (the turn layer, from the pinned deployment, through
    /// [`InvocationMachine::forbid_state_writes`] — only for invocations carrying
    /// the `cairnflow-state-lock` mark, D-W5). Only a VO `EXCLUSIVE` handler and
    /// a workflow's `run` may write state; for an unkeyed Service the engine
    /// refuses separately (574).
    ///
    /// Why it must be the runtime and not the SDK's types: a `SHARED` handler
    /// runs **in parallel** with `run` (workflow) or with an `EXCLUSIVE` handler
    /// (virtual object) on the same key, holding no lock. Both sides
    /// read-modify-writing one key means lost updates. A promise kept only by
    /// TypeScript can be broken by Python — and the TS SDK's `handlerTypes` does
    /// not change the type of `ctx` by itself.
    pub state_read_only: bool,
    /// Wire v3: the keepalive interval announced to the SDK. `None` = do not send.
    pub keepalive_ms: Option<u64>,
    /// Wire v3: a trial replay turn.
    pub replay_only: bool,
}

/// The failure reason when a read-only handler writes state
/// ([`StartParams::state_read_only`]). It reaches the user verbatim (`failed`
/// 571), so it is written in English.
pub const STATE_READ_ONLY_MESSAGE: &str = "state writes are not allowed in a SHARED handler: it \
     runs concurrently with other handlers on the same key without the object lock (the \
     workflow's run handler, or the virtual object's EXCLUSIVE handlers), so it may only read \
     state. Move the write to an EXCLUSIVE handler (virtual object) or to run (workflow)";

/// Does this command write state?
fn writes_state(msg: &Message) -> bool {
    matches!(
        msg,
        Message::SetState(_) | Message::ClearState(_) | Message::ClearAllState(_)
    )
}

pub struct InvocationMachine {
    id: InvocationId,
    params: StartParams,
    journal: Journal,
    phase: Phase,
    /// Completion ids promised but whose notification has not been sent yet,
    /// with the kind of notification to send. Spec section 8.1: the runtime MUST
    /// send one for every declared id.
    pending: BTreeMap<u32, MessageType>,
    /// Ids whose notification has been sent. Spec section 8.1: an id MUST NOT
    /// receive two notifications.
    settled: BTreeMap<u32, MessageType>,
    /// Signals received, by identity (idx or name). Signals come from outside
    /// with an at-least-once guarantee, so **duplicates are normal** — the
    /// machine dedupes by identity instead of treating them as a violation like
    /// completions (section 8.1 only speaks of completion ids, which this very
    /// SDK allocates).
    signals_seen: Vec<pb::signal_notification_message::SignalId>,
    output: Option<pb::OutputCommandMessage>,
    /// The number of journal entries decoded at the last `Event::Start` —
    /// replay plus building tasks for pending completions. Only for measuring
    /// and for tests pinning the slope; affects no effect.
    replay_decodes: u64,
}

impl InvocationMachine {
    /// Sets the two v3 parameters of `Start` — known only after the deployment
    /// has been chosen, i.e. after the machine was built. Call before
    /// `Event::Start`.
    pub fn set_wire_v3(&mut self, keepalive_ms: Option<u64>, replay_only: bool) {
        self.params.keepalive_ms = keepalive_ms;
        self.params.replay_only = replay_only;
    }

    /// From here on the machine refuses `SetState`/`ClearState`/`ClearAllState`
    /// (see [`StartParams::state_read_only`]). One way: there is no path to turn
    /// write permission back on. Commands already in the journal are not
    /// rechecked — the machine checks only **new** SDK commands, so calling it
    /// before or after `Event::Start` is the same.
    pub fn forbid_state_writes(&mut self) {
        self.params.state_read_only = true;
    }

    /// The machine refuses state-writing commands ([`Self::forbid_state_writes`]).
    pub fn state_writes_forbidden(&self) -> bool {
        self.params.state_read_only
    }

    /// Rebuilds from the durable journal. An empty journal = a new invocation.
    pub fn resume(id: InvocationId, params: StartParams, journal: Journal) -> Self {
        Self {
            id,
            params,
            journal,
            phase: Phase::Replay,
            pending: BTreeMap::new(),
            settled: BTreeMap::new(),
            signals_seen: Vec::new(),
            output: None,
            replay_decodes: 0,
        }
    }

    pub fn phase(&self) -> Phase {
        self.phase
    }

    pub fn journal(&self) -> &Journal {
        &self.journal
    }

    /// Pending completion ids — exactly the work a new leader must pick up again.
    pub fn pending_completions(&self) -> Vec<u32> {
        self.pending.keys().copied().collect()
    }

    /// The number of pending completions waiting for a notification of kind
    /// `ty` (e.g. `SLEEP_COMPLETION`: timers waiting — the tenant's
    /// `max-timers=` cap).
    pub fn pending_of(&self, ty: MessageType) -> usize {
        self.pending.values().filter(|t| **t == ty).count()
    }

    /// Completion ids that **already** have a notification in the journal.
    ///
    /// Differs from `pending_completions` in use: this is what the next call
    /// turn can replay to the SDK at once, without waiting for anyone. The
    /// driver needs it to tell "the SDK suspended because it had not seen the
    /// result yet" from "the SDK suspended waiting for the outside world" — the
    /// two demand quite different actions.
    pub fn settled_completions(&self) -> Vec<u32> {
        self.settled.keys().copied().collect()
    }

    /// Whether completion `id` already has a notification in the journal —
    /// like [`Self::settled_completions`] without building an array.
    pub fn is_settled(&self, id: u32) -> bool {
        self.settled.contains_key(&id)
    }

    /// The number of journal entries decoded at the last `Event::Start` (replay
    /// and building tasks for pending completions). The engine replays the
    /// journal at **every** completion delivery (`settle_completion`), so this
    /// number times the number of completions is the CPU cost on the engine
    /// thread.
    pub fn replay_decodes(&self) -> u64 {
        self.replay_decodes
    }

    /// Signals received (already in the journal), by identity.
    pub fn settled_signals(&self) -> &[pb::signal_notification_message::SignalId] {
        &self.signals_seen
    }

    /// The first place where two machines differ in the state that **the
    /// journal decides** — id, journal, phase, pending and settled completions
    /// (with kinds), signals seen, output — or `None` if they match. Does not
    /// compare `params` (a snapshot at load time, only going into the `Start`
    /// sent to the SDK) or the `replay_decodes` counter.
    ///
    /// The engine's warm machine is a machine that has replayed and then
    /// received more completions; the lemma `tests/warm_lemma.rs` pins that it
    /// matches a machine replaying the grown journal, and the engine's
    /// cross-check mode compares with this function.
    pub fn divergence(&self, other: &Self) -> Option<String> {
        if self.id != other.id {
            return Some("id".into());
        }
        if self.journal != other.journal {
            let (a, b) = (self.journal.entries(), other.journal.entries());
            let at = a.iter().zip(b).position(|(x, y)| x != y);
            return Some(format!(
                "journal: {} ↔ {} entries, first difference at {at:?}",
                a.len(),
                b.len()
            ));
        }
        if self.phase != other.phase {
            return Some(format!("phase: {:?} ↔ {:?}", self.phase, other.phase));
        }
        if self.pending != other.pending {
            return Some(format!(
                "pending completions: {:?} ↔ {:?}",
                self.pending, other.pending
            ));
        }
        if self.settled != other.settled {
            return Some(format!(
                "settled completions: {:?} ↔ {:?}",
                self.settled, other.settled
            ));
        }
        if self.signals_seen != other.signals_seen {
            return Some(format!(
                "signals seen: {:?} ↔ {:?}",
                self.signals_seen, other.signals_seen
            ));
        }
        if self.output != other.output {
            return Some("output".into());
        }
        None
    }

    pub fn handle(&mut self, event: Event) -> Result<Vec<Effect>, MachineError> {
        if self.phase == Phase::Closed {
            return Ok(match event {
                // Spec section 8.3: after finishing, the SDK MUST NOT send more messages.
                Event::FromSdk(_) | Event::Sealed(_) => {
                    vec![self.violation("SDK sent a message after the invocation ended")]
                }
                // A completion is DIFFERENT: it comes from the runtime's own
                // background task, not from the SDK. A scheduled timer still fires
                // after the invocation ended with an error or suspension — that is
                // a normal race, not a protocol violation. Ignore it silently.
                Event::Completion(_) => vec![],
                // A signal, by the same reasoning as a completion: it comes from
                // outside, and a finished target is a normal race — ignore it.
                Event::Signal(_) => vec![],
                Event::Start => vec![],
            });
        }
        match event {
            Event::Start => self.on_start(),
            Event::FromSdk(frame) => self.on_sdk(frame, false),
            Event::Sealed(frame) => self.on_sdk(frame, true),
            Event::Completion(c) => Ok(self.on_completion(c)),
            Event::Signal(s) => Ok(self.on_signal(s)),
        }
    }

    // ────────────────────────────────────────────────────────── replay

    fn on_start(&mut self) -> Result<Vec<Effect>, MachineError> {
        let mut fx = vec![Effect::Send(Message::Start(pb::StartMessage {
            id: Bytes::copy_from_slice(&self.id),
            debug_id: self.params.debug_id.clone(),
            known_entries: self.journal.len(),
            state_map: self
                .params
                .state_map
                .iter()
                .map(|(k, v)| pb::start_message::StateEntry {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect(),
            partial_state: self.params.partial_state,
            key: self.params.key.clone(),
            retry_count_since_last_stored_entry: self.params.retry_count_since_last_stored_entry,
            duration_since_last_stored_entry: self.params.duration_since_last_stored_entry,
            random_seed: self.params.random_seed,
            idempotency_key: self.params.idempotency_key.clone(),
            keepalive_interval_ms: self.params.keepalive_ms,
            replay_only: self.params.replay_only,
            // v5: the machine always emits a full replay; cutting the delta is
            // the job of `replay::Opening` at the transport layer.
            replay_from: 0,
            prefix_digest: Bytes::new(),
            // v6: the shell fills it in before sending, not the machine.
            segment_urls: vec![],
        }))];

        self.replay_decodes = 0;
        // The command declaring each replayable completion: `(id, kind)` → the
        // position in `fx` of the **first** entry declaring it (see
        // [`replay_key`]).
        let mut origin: BTreeMap<(u32, MessageType), usize> = BTreeMap::new();
        // Replay verbatim, in exactly the entry-index order. This is where
        // "bit-exact" is kept: we resend the very bytes stored, not a
        // reconstruction from semantics.
        for i in 0..self.journal.len() {
            let entry = self.journal.get(i).expect("i < len");
            self.replay_decodes += 1;
            let msg = self.decode_entry(i, entry)?;
            self.track_entry(i, &msg)?;
            if let Some(key) = replay_key(&msg) {
                origin.entry(key).or_insert(fx.len());
            }
            fx.push(Effect::Send(msg));
        }

        // Completions still pending after replay: nobody has returned a result,
        // the task must be re-issued. This is the recovery path after failover.
        //
        // Tasks come from the index built in the loop above, **without**
        // rescanning the journal per completion: the engine replays the journal
        // at every completion delivery (`settle_completion`), so a rescan is
        // O(pending completions × entries) each time — a `fanout` of 1 000
        // children decoded ~1.9 million entries for **one** completion
        // delivered (`tests/replay_cost.rs`).
        // The task order is unchanged: by id, like the loop over `pending`.
        let tasks: Vec<Effect> = self
            .pending
            .iter()
            .filter_map(|(&id, &ty)| match fx.get(*origin.get(&(id, ty))?) {
                Some(Effect::Send(msg)) => replay_task(msg).map(Effect::Task),
                _ => None,
            })
            .collect();
        fx.extend(tasks);

        self.phase = Phase::Processing;
        Ok(fx)
    }

    fn decode_entry(
        &self,
        entry_index: u32,
        entry: &JournalEntry,
    ) -> Result<Message, MachineError> {
        let frame = Frame::new(entry.ty, Flags::NONE, entry.payload.clone());
        decode(&frame).map_err(|e| MachineError::CorruptJournal {
            entry_index,
            reason: e.to_string(),
        })
    }

    /// Updates the completion ledger for one replayed entry.
    ///
    /// A durable journal containing a duplicate id is a **broken** journal, not
    /// one to handle gently: it was written by an earlier run, so continuing
    /// the replay only reproduces exactly the silent hang that
    /// [`Self::duplicate_completion_id`] exists to prevent. Stop here and say
    /// which entry.
    fn track_entry(&mut self, entry_index: u32, msg: &Message) -> Result<(), MachineError> {
        match msg.kind() {
            MessageKind::Command => {
                if let Some(dup) = self.duplicate_completion_id(msg) {
                    return Err(MachineError::CorruptJournal {
                        entry_index,
                        reason: format!("completion id {dup} allocated twice"),
                    });
                }
                self.promise_completions(msg)
            }
            MessageKind::Notification => {
                if let Some(id) = notification_completion_id(msg) {
                    if let Some(ty) = self.pending.remove(&id) {
                        self.settled.insert(id, ty);
                    }
                }
                // A signal journaled by an earlier run: remember its identity so
                // a redelivery (at-least-once) is deduped, and so a suspension
                // knows it has arrived.
                if let Message::Signal(s) = msg {
                    if let Some(identity) = s.signal_id.clone() {
                        if !self.signals_seen.contains(&identity) {
                            self.signals_seen.push(identity);
                        }
                    }
                }
            }
            _ => {}
        }
        if let Message::Output(o) = msg {
            self.output = Some(o.clone());
        }
        Ok(())
    }

    // ────────────────────────────────────────────────────────── SDK → runtime

    fn on_sdk(&mut self, frame: Frame, sealed: bool) -> Result<Vec<Effect>, MachineError> {
        let requested_ack = frame.flags.requested_ack();
        let msg = match decode(&frame) {
            Ok(m) => m,
            Err(e) => return Ok(vec![self.violation(&e.to_string())]),
        };

        if let Message::Custom { ty, payload } = msg {
            return Ok(vec![Effect::ForwardCustom { ty, payload }]);
        }

        // A `Value` from the SDK is always inline before wire v6: `segments`
        // would be a ref to a segment the runtime has not checked.
        let checked = if sealed {
            cairn_protocol::value::sealed_values_ok(&msg)
        } else {
            cairn_protocol::value::sdk_values_inline(&msg)
        };
        if let Err(e) = checked {
            return Ok(vec![self.violation(&e.to_string())]);
        }

        // Notifications go from the runtime down to the SDK, never the other way.
        if msg.kind() == MessageKind::Notification {
            return Ok(vec![self.violation(&format!(
                "SDK sent notification {}",
                msg.message_type()
            ))]);
        }

        match &msg {
            // ── Control
            Message::ProposeRunCompletion(p) => Ok(self.on_propose_run(p, requested_ack)),
            Message::Suspension(s) => Ok(self.on_suspension(s)),
            Message::Error(e) => Ok(self.on_error(e)),
            Message::End(_) => Ok(self.on_end()),
            Message::Start(_)
            | Message::CommandAck(_)
            | Message::ProposeRunCompletionAck(_)
            | Message::SegmentUrls(_) => Ok(vec![self.violation(&format!(
                "{} only flows from the runtime to the SDK",
                msg.message_type()
            ))]),

            // ── Side channel (spec section 9.2): no entry, no ack, no work for
            // the machine. The turn layer has used it (the silence cap restarts
            // at every frame) before it gets here; if it slips through, drop it
            // — the journal must not know about it.
            Message::Keepalive(_) => Ok(vec![]),
            // Turn-ending frames that only the turn layer understands (trial
            // replay, running again from the start). Reaching the machine means
            // the driver misused it — and recording it as an entry would make
            // every later replay send a control frame down to the SDK as if it
            // were the SDK's own command.
            //
            // `StreamAppend`/`ArtifactPut` (v4) likewise: the turn layer hands
            // them to `StreamHub` or PUTs the artifact when the deployment
            // settled on v4; reaching the machine means below v4 (the SDK sent
            // something it may not send) or the turn layer has no path for it
            // yet — 571, nothing recorded.
            Message::ReplayReport(_)
            | Message::ContinueAsNew(_)
            | Message::StreamAppend(_)
            | Message::ArtifactPut(_) => Ok(vec![self.violation(&format!(
                "{} is valid only at the turn layer",
                msg.message_type()
            ))]),

            // ── Input is created by the runtime; the SDK must not send it.
            Message::Input(_) => {
                Ok(vec![self.violation(
                    "SDK sent Input — this entry is created by the runtime",
                )])
            }

            // ── Command
            _ => Ok(self.on_command(msg, requested_ack)),
        }
    }

    /// Writes the SDK's command into the journal, then produces the work to do.
    fn on_command(&mut self, msg: Message, requested_ack: bool) -> Vec<Effect> {
        // Check BEFORE appending: a command allocating a duplicate completion id
        // is a broken command, and writing it into the journal means every later
        // replay trips over exactly it again.
        if let Some(dup) = self.duplicate_completion_id(&msg) {
            return vec![self.violation(&format!(
                "completion id {dup} is already allocated to another command of this invocation"
            ))];
        }

        // Writing state in a handler without write permission is a broken
        // command too, for the same reason: journaling it means every later
        // replay trips over it again. See [`StartParams::state_read_only`].
        // English message: it reaches the user verbatim as the failure reason.
        if self.params.state_read_only && writes_state(&msg) {
            return vec![self.violation(STATE_READ_ONLY_MESSAGE)];
        }

        let mut fx = Vec::new();
        let appended = self.append(&msg, &mut fx);

        // Ack AFTER persisting — this order is the contract with the driver.
        if requested_ack {
            if let Some(command_index) = appended.command_index {
                fx.push(Effect::Send(Message::CommandAck(pb::CommandAckMessage {
                    command_index,
                })));
            }
        }

        self.promise_completions(&msg);

        match &msg {
            Message::GetState(g) => fx.push(Effect::Task(Task::ReadState {
                completion_id: g.result_completion_id,
                key: g.key.clone(),
            })),
            // Inline: `on_sdk` already refused `segments` (`value::sdk_values_inline`).
            // `on_sdk` already checked: inline from the SDK, or segmented sealed by the shell.
            Message::SetState(s) => match cairn_protocol::value::Logical::of(s.value.as_ref()) {
                Ok(v) => fx.push(Effect::Task(Task::WriteState {
                    key: s.key.clone(),
                    value: Some(v),
                })),
                Err(e) => fx.push(self.violation(&e.to_string())),
            },
            Message::ClearState(c) => fx.push(Effect::Task(Task::WriteState {
                key: c.key.clone(),
                value: None,
            })),
            Message::ClearAllState(_) => fx.push(Effect::Task(Task::ClearAllState)),
            Message::GetStateKeys(g) => fx.push(Effect::Task(Task::ReadStateKeys {
                completion_id: g.result_completion_id,
            })),
            // Eager state: the result is embedded in the command — the SDK read
            // it from `state_map`. Journal only, nothing more to do.
            Message::GetEagerState(_) | Message::GetEagerStateKeys(_) => {}
            Message::GetPromise(p) => fx.push(Effect::Task(Task::PromiseGet {
                completion_id: p.result_completion_id,
                key: p.key.clone(),
            })),
            Message::PeekPromise(p) => fx.push(Effect::Task(Task::PromisePeek {
                completion_id: p.result_completion_id,
                key: p.key.clone(),
            })),
            Message::CompletePromise(p) => {
                let Some(result) = p.completion.clone() else {
                    return vec![self.violation("CompletePromise carries no result")];
                };
                fx.push(Effect::Task(Task::PromiseComplete {
                    completion_id: p.result_completion_id,
                    key: p.key.clone(),
                    result,
                }));
            }
            Message::AttachInvocation(a) => fx.push(Effect::Task(Task::Attach {
                completion_id: a.result_completion_id,
                target_invocation_id: a.invocation_id.clone(),
            })),
            Message::GetInvocationOutput(g) => fx.push(Effect::Task(Task::PeekOutput {
                completion_id: g.result_completion_id,
                target_invocation_id: g.invocation_id.clone(),
            })),
            Message::Sleep(s) => fx.push(Effect::Task(Task::ScheduleTimer {
                completion_id: s.result_completion_id,
                wake_up_time: s.wake_up_time,
            })),
            Message::Call(c) => fx.push(Effect::Task(call_task(c))),
            Message::OneWayCall(c) => fx.push(Effect::Task(one_way_task(c))),
            Message::SendSignal(s) => {
                if let Some(signal) = s.signal_id.clone() {
                    fx.push(Effect::Task(Task::SendSignal {
                        target_invocation_id: s.target_invocation_id.clone(),
                        signal,
                        result: s.result.clone(),
                        entry_index: appended.entry_index,
                    }));
                }
            }
            Message::Output(o) => self.output = Some(o.clone()),
            // `Run` produces no task: the result is proposed by the SDK through
            // ProposeRunCompletion.
            _ => {}
        }
        fx
    }

    /// Spec section 8.2 — the subtlest point of the whole protocol.
    ///
    /// The journal always stores `RunCompletionNotification`. What goes *onto
    /// the wire* differs by phase: in PROCESSING an ack is sent (if the SDK
    /// asked), in REPLAY that very notification is sent. Since both occupy **the
    /// same journal position**, the notification stream on replay lines up
    /// exactly with the first run — without any separate ordering rule.
    fn on_propose_run(
        &mut self,
        p: &pb::ProposeRunCompletionMessage,
        requested_ack: bool,
    ) -> Vec<Effect> {
        let id = p.result_completion_id;
        if self.settled.contains_key(&id) {
            return vec![self.violation(&format!("completion {id} already has a notification"))];
        }
        if !self.pending.contains_key(&id) {
            return vec![self.violation(&format!("completion {id} was never declared"))];
        }

        let notification = Message::RunCompletion(pb::RunCompletionNotificationMessage {
            completion_id: id,
            result: match &p.result {
                Some(pb::propose_run_completion_message::Result::Value(v)) => {
                    Some(pb::run_completion_notification_message::Result::Value(
                        cairn_protocol::value::inline(v.clone()),
                    ))
                }
                Some(pb::propose_run_completion_message::Result::Failure(f)) => Some(
                    pb::run_completion_notification_message::Result::Failure(f.clone()),
                ),
                // A sealed frame (the shell) or an SDK v6: ref + tail go into
                // the journal as they are — the shell assembles them before
                // sending down to an SDK ≤ v5.
                Some(pb::propose_run_completion_message::Result::SegmentedValue(v)) => Some(
                    pb::run_completion_notification_message::Result::Value(v.clone()),
                ),
                None => None,
            },
        });

        let mut fx = Vec::new();
        self.append(&notification, &mut fx);
        self.settle(id);

        fx.push(Effect::Send(if requested_ack {
            Message::ProposeRunCompletionAck(pb::ProposeRunCompletionAckMessage {
                completion_id: id,
            })
        } else {
            notification
        }));
        fx
    }

    fn on_suspension(&mut self, s: &pb::SuspensionMessage) -> Vec<Effect> {
        // Spec section 8.3: the wait list MUST NOT be empty — with a tree, there must be at least
        // one leaf.
        if !suspension_has_leaves(s) {
            return vec![self.violation("Suspension with an empty wait list")];
        }
        self.phase = Phase::Closed;
        vec![Effect::Finish(Outcome::Suspended(s.clone()))]
    }

    fn on_error(&mut self, e: &pb::ErrorMessage) -> Vec<Effect> {
        self.phase = Phase::Closed;
        let action = if e.code == JOURNAL_MISMATCH {
            // Spec section 8.5: JOURNAL_MISMATCH is NOT RETRYABLE, whatever the
            // behavior — a retry only reproduces the same error.
            //
            // And **always PAUSE, never FAIL** — even when the SDK asks for FAIL.
            // A journal mismatch is the sign of a bad deploy, not of a broken
            // invocation: it is fixed by deploying the right version and then
            // `resume`. FAIL here would permanently lose every in-flight
            // invocation because of one deploy. The rule lives in the runtime,
            // not only in the SDK, so old SDKs (sending FAIL for 570) are saved
            // too.
            FailureAction::Pause
        } else {
            match e.behavior() {
                pb::ErrorBehavior::Retry => FailureAction::Retry {
                    delay_ms: e.next_retry_delay,
                },
                pb::ErrorBehavior::Pause => FailureAction::Pause,
                pb::ErrorBehavior::Fail => FailureAction::Fail,
            }
        };
        vec![Effect::Finish(Outcome::Failed {
            code: e.code,
            message: e.message.clone(),
            action,
        })]
    }

    fn on_end(&mut self) -> Vec<Effect> {
        self.phase = Phase::Closed;
        match self.output.clone() {
            Some(o) => vec![Effect::Finish(Outcome::Completed(o))],
            None => vec![self.violation("End without Output")],
        }
    }

    // ────────────────────────────────────────────────────── completion → SDK

    fn on_completion(&mut self, c: Completion) -> Vec<Effect> {
        let Some(&expected) = self.pending.get(&c.id) else {
            // Spec section 8.1: an id MUST NOT receive two notifications.
            let why = if self.settled.contains_key(&c.id) {
                format!("completion {} already has a notification", c.id)
            } else {
                format!("completion {} was never declared", c.id)
            };
            return vec![self.violation(&why)];
        };

        let Some(msg) = build_notification(expected, c.id, &c.result) else {
            return vec![
                self.violation(&format!("result {:?} does not match {expected}", c.result))
            ];
        };

        let mut fx = Vec::new();
        self.append(&msg, &mut fx);
        self.settle(c.id);
        fx.push(Effect::Send(msg));
        fx
    }

    /// A signal from outside. Journal it + send it down to the SDK; a duplicate
    /// is silently ignored — the source is at-least-once, so "already seen" is
    /// not an error.
    fn on_signal(&mut self, s: pb::SignalNotificationMessage) -> Vec<Effect> {
        let Some(identity) = s.signal_id.clone() else {
            // A signal without an identity cannot be deduped and the SDK cannot
            // match it to anything either. Drop it, loudly.
            return vec![self.violation("signal carries neither an index nor a name")];
        };
        if self.signals_seen.contains(&identity) {
            return Vec::new();
        }
        self.signals_seen.push(identity);
        let msg = Message::Signal(s);
        let mut fx = Vec::new();
        self.append(&msg, &mut fx);
        fx.push(Effect::Send(msg));
        fx
    }

    // ────────────────────────────────────────────────────────── helpers

    fn append(&mut self, msg: &Message, fx: &mut Vec<Effect>) -> crate::journal::Appended {
        let entry = JournalEntry::new(msg.message_type(), payload_of(msg));
        let appended = self.journal.append(entry.clone());
        fx.push(Effect::Persist {
            entry_index: appended.entry_index,
            entry,
        });
        appended
    }

    /// Records the completions a command promises to receive (spec section 8.1).
    fn promise_completions(&mut self, msg: &Message) {
        for (id, expected) in promised_completions(msg) {
            self.pending.insert(id, expected);
        }
    }

    /// A completion id that `msg` means to allocate but that **already has an
    /// owner** (spec section 8.1).
    ///
    /// Section 8.1: ids are allocated by the SDK and MUST be unique within one
    /// invocation. Silently ignoring a duplicate id is the worst way to fail:
    /// the runtime does not promise that completion, so it never sends a
    /// notification; the SDK waits forever; and the invocation sits in
    /// `Suspended` **with no error to see**. Section 8.1 requires the runtime to
    /// either send a notification for every declared id, or end the invocation
    /// with `Error` — silence is neither.
    ///
    /// `settled` also counts as "owned": allocating an id that has already
    /// received a notification is a duplicate allocation too.
    fn duplicate_completion_id(&self, msg: &Message) -> Option<u32> {
        let promised = promised_completions(msg);
        promised.iter().enumerate().find_map(|(i, (id, _))| {
            let taken = self.pending.contains_key(id)
                || self.settled.contains_key(id)
                // `Call` allocates TWO ids in one command; they must differ too.
                || promised[..i].iter().any(|(prev, _)| prev == id);
            taken.then_some(*id)
        })
    }

    fn settle(&mut self, id: u32) {
        if let Some(ty) = self.pending.remove(&id) {
            self.settled.insert(id, ty);
        }
    }

    fn violation(&mut self, why: &str) -> Effect {
        self.fail(PROTOCOL_VIOLATION, why)
    }

    /// Closes the invocation with a **terminal error decided by the runtime**,
    /// not reported by the SDK — e.g. a command the runtime refuses to carry
    /// out. No retry: an error of this kind does not go away on a rerun.
    pub fn fail(&mut self, code: u32, message: &str) -> Effect {
        self.phase = Phase::Closed;
        Effect::Finish(Outcome::Failed {
            code,
            message: message.to_string(),
            action: FailureAction::Fail,
        })
    }

    /// Like [`Self::fail`], but the invocation is **`paused`** (waiting for
    /// intervention: raise the cap, then `resume`) instead of `failed` — for
    /// caps enforced by the runtime where continuing makes sense once the
    /// operator changes the configuration (`max-journal-bytes=`).
    pub fn pause(&mut self, code: u32, message: &str) -> Effect {
        self.phase = Phase::Closed;
        Effect::Finish(Outcome::Failed {
            code,
            message: message.to_string(),
            action: FailureAction::Pause,
        })
    }
}

/// The completion that `msg` declares **and that can be replayed** after a
/// restart, with its notification kind: the key of the replay index in
/// `on_start`.
///
/// Each command variant maps to **exactly one** replayable completion kind, so
/// "the first entry with `replay_key == (id, kind)`" is exactly the entry the
/// old scan (scanning the journal from the start, stopping at the first
/// matching branch) found. [`replay_task`] builds the task for the same
/// variant; the two functions must go together.
///
/// - `ReadState` cannot be replayed from here: the machine does not keep the
///   key of a `GetState` command once the entry has passed. The driver gets it
///   back from the journal — that is the source of truth, not the machine's
///   memory.
/// - The task sending a call is tied to the **invocation id** completion, not
///   the result completion. This is easy to get wrong and costly: tied to the
///   result completion, every replay — and the caller replays on *every* turn
///   while waiting — would send one more call. The invocation id completion is
///   the receipt "already sent"; once it is in the journal it must not be sent
///   again. The result comes back on its own later.
/// - `Run` cannot be replayed: the result comes from the SDK through
///   ProposeRunCompletion, not from the runtime.
fn replay_key(msg: &Message) -> Option<(u32, MessageType)> {
    Some(match msg {
        Message::GetState(g) => (g.result_completion_id, MessageType::GET_STATE_COMPLETION),
        Message::GetStateKeys(g) => (
            g.result_completion_id,
            MessageType::GET_STATE_KEYS_COMPLETION,
        ),
        Message::GetPromise(p) => (p.result_completion_id, MessageType::GET_PROMISE_COMPLETION),
        Message::PeekPromise(p) => (p.result_completion_id, MessageType::PEEK_PROMISE_COMPLETION),
        Message::CompletePromise(p) => (
            p.result_completion_id,
            MessageType::COMPLETE_PROMISE_COMPLETION,
        ),
        Message::AttachInvocation(a) => (
            a.result_completion_id,
            MessageType::ATTACH_INVOCATION_COMPLETION,
        ),
        Message::GetInvocationOutput(g) => (
            g.result_completion_id,
            MessageType::GET_INVOCATION_OUTPUT_COMPLETION,
        ),
        Message::Sleep(s) => (s.result_completion_id, MessageType::SLEEP_COMPLETION),
        Message::Call(c) => (
            c.invocation_id_notification_idx,
            MessageType::CALL_INVOCATION_ID_COMPLETION,
        ),
        Message::OneWayCall(c) => (
            c.invocation_id_notification_idx,
            MessageType::CALL_INVOCATION_ID_COMPLETION,
        ),
        _ => return None,
    })
}

/// The replay task for the completion `msg` declares ([`replay_key`]). `None`
/// when the command has no replay task — including a `CompletePromise` without
/// a result: the first matching entry decides, the search does not go on.
fn replay_task(msg: &Message) -> Option<Task> {
    match msg {
        Message::GetState(g) => Some(Task::ReadState {
            completion_id: g.result_completion_id,
            key: g.key.clone(),
        }),
        Message::GetStateKeys(g) => Some(Task::ReadStateKeys {
            completion_id: g.result_completion_id,
        }),
        Message::GetPromise(p) => Some(Task::PromiseGet {
            completion_id: p.result_completion_id,
            key: p.key.clone(),
        }),
        Message::PeekPromise(p) => Some(Task::PromisePeek {
            completion_id: p.result_completion_id,
            key: p.key.clone(),
        }),
        // Replayable because the engine makes it idempotent per completer: if
        // the earlier run wrote it, this one sees itself.
        Message::CompletePromise(p) => p.completion.clone().map(|result| Task::PromiseComplete {
            completion_id: p.result_completion_id,
            key: p.key.clone(),
            result,
        }),
        Message::AttachInvocation(a) => Some(Task::Attach {
            completion_id: a.result_completion_id,
            target_invocation_id: a.invocation_id.clone(),
        }),
        Message::GetInvocationOutput(g) => Some(Task::PeekOutput {
            completion_id: g.result_completion_id,
            target_invocation_id: g.invocation_id.clone(),
        }),
        Message::Sleep(s) => Some(Task::ScheduleTimer {
            completion_id: s.result_completion_id,
            wake_up_time: s.wake_up_time,
        }),
        Message::Call(c) => Some(call_task(c)),
        Message::OneWayCall(c) => Some(one_way_task(c)),
        _ => None,
    }
}

fn call_task(c: &pb::CallCommandMessage) -> Task {
    Task::Invoke {
        result_completion_id: Some(c.result_completion_id),
        invocation_id_completion_id: c.invocation_id_notification_idx,
        service: c.service_name.clone(),
        handler: c.handler_name.clone(),
        key: c.key.clone(),
        parameter: c.parameter.clone(),
        idempotency_key: c.idempotency_key.clone(),
        invoke_time: 0,
        scope: c.scope.clone(),
        limit_key: c.limit_key.clone(),
        spread: c.placement == pb::CallPlacement::Spread as i32,
        tags: c
            .tags
            .iter()
            .map(|h| (h.key.clone(), h.value.clone()))
            .collect(),
    }
}

fn one_way_task(c: &pb::OneWayCallCommandMessage) -> Task {
    Task::Invoke {
        result_completion_id: None,
        invocation_id_completion_id: c.invocation_id_notification_idx,
        service: c.service_name.clone(),
        handler: c.handler_name.clone(),
        key: c.key.clone(),
        parameter: c.parameter.clone(),
        idempotency_key: c.idempotency_key.clone(),
        invoke_time: c.invoke_time,
        scope: c.scope.clone(),
        limit_key: c.limit_key.clone(),
        spread: c.placement == pb::CallPlacement::Spread as i32,
        tags: c
            .tags
            .iter()
            .map(|h| (h.key.clone(), h.value.clone()))
            .collect(),
    }
}

/// The completions a command promises to receive, with the notification kind
/// (spec section 8.1).
///
/// `Call` is the only command returning two elements: the invocation id
/// arrives at once, the result later.
fn promised_completions(msg: &Message) -> Vec<(u32, MessageType)> {
    let ty = msg.message_type();
    let mut out = Vec::with_capacity(ty.completions().len());
    for expected in ty.completions() {
        let id = match (msg, *expected) {
            (Message::GetState(g), _) => g.result_completion_id,
            (Message::GetStateKeys(g), _) => g.result_completion_id,
            (Message::GetPromise(p), _) => p.result_completion_id,
            (Message::PeekPromise(p), _) => p.result_completion_id,
            (Message::CompletePromise(p), _) => p.result_completion_id,
            (Message::Sleep(s), _) => s.result_completion_id,
            (Message::Run(r), _) => r.result_completion_id,
            (Message::AttachInvocation(a), _) => a.result_completion_id,
            (Message::GetInvocationOutput(g), _) => g.result_completion_id,
            (Message::Call(c), MessageType::CALL_COMPLETION) => c.result_completion_id,
            (Message::Call(c), MessageType::CALL_INVOCATION_ID_COMPLETION) => {
                c.invocation_id_notification_idx
            }
            (Message::OneWayCall(c), _) => c.invocation_id_notification_idx,
            _ => continue,
        };
        out.push((id, *expected));
    }
    out
}

fn notification_completion_id(msg: &Message) -> Option<u32> {
    Some(match msg {
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

fn build_notification(expected: MessageType, id: u32, r: &CompletionResult) -> Option<Message> {
    use pb::get_state_completion_notification_message as gs;
    Some(match (expected, r) {
        (MessageType::GET_STATE_COMPLETION, CompletionResult::State(v)) => {
            Message::GetStateCompletion(pb::GetStateCompletionNotificationMessage {
                completion_id: id,
                result: Some(match v {
                    // Void = the key does not exist. An empty Value = it exists, empty.
                    None => gs::Result::Void(pb::Void {}),
                    Some(v) => gs::Result::Value(v.encode()),
                }),
            })
        }
        (MessageType::SLEEP_COMPLETION, CompletionResult::Timer) => {
            Message::SleepCompletion(pb::SleepCompletionNotificationMessage {
                completion_id: id,
                void: Some(pb::Void {}),
            })
        }
        (MessageType::CALL_COMPLETION, CompletionResult::CallValue(v)) => {
            Message::CallCompletion(pb::CallCompletionNotificationMessage {
                completion_id: id,
                result: Some(pb::call_completion_notification_message::Result::Value(
                    cairn_protocol::value::inline(v.clone()),
                )),
            })
        }
        (MessageType::CALL_COMPLETION, CompletionResult::CallSegmented(v)) => {
            Message::CallCompletion(pb::CallCompletionNotificationMessage {
                completion_id: id,
                result: Some(pb::call_completion_notification_message::Result::Value(
                    v.encode(),
                )),
            })
        }
        (MessageType::CALL_COMPLETION, CompletionResult::CallFailure(f)) => {
            Message::CallCompletion(pb::CallCompletionNotificationMessage {
                completion_id: id,
                result: Some(pb::call_completion_notification_message::Result::Failure(
                    f.clone(),
                )),
            })
        }
        (MessageType::CALL_INVOCATION_ID_COMPLETION, CompletionResult::CallInvocationId(s)) => {
            Message::CallInvocationIdCompletion(pb::CallInvocationIdCompletionNotificationMessage {
                completion_id: id,
                invocation_id: s.clone(),
            })
        }
        (MessageType::GET_STATE_KEYS_COMPLETION, CompletionResult::StateKeys(keys)) => {
            Message::GetStateKeysCompletion(pb::GetStateKeysCompletionNotificationMessage {
                completion_id: id,
                state_keys: Some(pb::StateKeys { keys: keys.clone() }),
            })
        }
        (MessageType::GET_PROMISE_COMPLETION, r) => {
            use pb::get_promise_completion_notification_message::Result as R;
            Message::GetPromiseCompletion(pb::GetPromiseCompletionNotificationMessage {
                completion_id: id,
                result: Some(match r {
                    CompletionResult::CallValue(v) => {
                        R::Value(cairn_protocol::value::inline(v.clone()))
                    }
                    CompletionResult::CallFailure(f) => R::Failure(f.clone()),
                    _ => return None,
                }),
            })
        }
        (MessageType::PEEK_PROMISE_COMPLETION, r) => {
            use pb::peek_promise_completion_notification_message::Result as R;
            Message::PeekPromiseCompletion(pb::PeekPromiseCompletionNotificationMessage {
                completion_id: id,
                result: Some(match r {
                    CompletionResult::Void => R::Void(pb::Void {}),
                    CompletionResult::CallValue(v) => {
                        R::Value(cairn_protocol::value::inline(v.clone()))
                    }
                    CompletionResult::CallFailure(f) => R::Failure(f.clone()),
                    _ => return None,
                }),
            })
        }
        (MessageType::COMPLETE_PROMISE_COMPLETION, r) => {
            use pb::complete_promise_completion_notification_message::Result as R;
            Message::CompletePromiseCompletion(pb::CompletePromiseCompletionNotificationMessage {
                completion_id: id,
                result: Some(match r {
                    CompletionResult::Void => R::Void(pb::Void {}),
                    CompletionResult::CallFailure(f) => R::Failure(f.clone()),
                    _ => return None,
                }),
            })
        }
        (MessageType::ATTACH_INVOCATION_COMPLETION, r) => {
            use pb::attach_invocation_completion_notification_message::Result as R;
            Message::AttachInvocationCompletion(pb::AttachInvocationCompletionNotificationMessage {
                completion_id: id,
                result: Some(match r {
                    CompletionResult::CallValue(v) => {
                        R::Value(cairn_protocol::value::inline(v.clone()))
                    }
                    CompletionResult::CallSegmented(v) => R::Value(v.encode()),
                    CompletionResult::CallFailure(f) => R::Failure(f.clone()),
                    _ => return None,
                }),
            })
        }
        (MessageType::GET_INVOCATION_OUTPUT_COMPLETION, r) => {
            use pb::get_invocation_output_completion_notification_message::Result as R;
            Message::GetInvocationOutputCompletion(
                pb::GetInvocationOutputCompletionNotificationMessage {
                    completion_id: id,
                    result: Some(match r {
                        CompletionResult::Void => R::Void(pb::Void {}),
                        CompletionResult::CallValue(v) => {
                            R::Value(cairn_protocol::value::inline(v.clone()))
                        }
                        CompletionResult::CallSegmented(v) => R::Value(v.encode()),
                        CompletionResult::CallFailure(f) => R::Failure(f.clone()),
                        _ => return None,
                    }),
                },
            )
        }
        _ => return None,
    })
}

/// Whether the suspension tree has any leaf (spec section 8.3: the wait list MUST NOT be empty).
pub fn suspension_has_leaves(s: &pb::SuspensionMessage) -> bool {
    !s.waiting_completions.is_empty()
        || !s.waiting_signals.is_empty()
        || !s.waiting_named_signals.is_empty()
        || s.nested_futures.iter().any(suspension_has_leaves)
}

/// The set of signals that have arrived, to match against the suspension tree's signal leaves.
#[derive(Clone, Copy, Debug, Default)]
pub struct SettledSignals<'a> {
    pub signals: &'a [pb::signal_notification_message::SignalId],
}

impl SettledSignals<'_> {
    fn has_idx(&self, idx: u32) -> bool {
        use pb::signal_notification_message::SignalId as S;
        self.signals
            .iter()
            .any(|s| matches!(s, S::Idx(i) if *i == idx))
    }

    fn has_name(&self, name: &str) -> bool {
        use pb::signal_notification_message::SignalId as S;
        self.signals
            .iter()
            .any(|s| matches!(s, S::Name(n) if n == name))
    }
}

/// Whether the suspension tree is satisfied, given the set of completions that
/// have a notification and the set of signals that have arrived.
///
/// A `FIRST_COMPLETED` node (including the `UNKNOWN = 0` of a v1 message) is
/// satisfied when **any** leaf or child node is; `ALL_COMPLETED` when **all**
/// are. A signal leaf is satisfied when a signal of the same identity is in the
/// journal.
pub fn suspension_satisfied(
    s: &pb::SuspensionMessage,
    settled: &[u32],
    signals: SettledSignals<'_>,
) -> bool {
    suspension_satisfied_by(s, &|id| settled.contains(&id), signals)
}

/// [`suspension_satisfied`] with the question "does completion `id` have a
/// notification yet" supplied by the caller — e.g. looking up the machine's
/// `settled` ledger ([`InvocationMachine::is_settled`], O(log n)) instead of
/// searching an array at each leaf: the drainer evaluates a thousand-leaf
/// `ctx.all` tree on every completion, and searching an array is O(leaves ×
/// completions) each time.
pub fn suspension_satisfied_by(
    s: &pb::SuspensionMessage,
    done: &dyn Fn(u32) -> bool,
    signals: SettledSignals<'_>,
) -> bool {
    match s.combinator_type() {
        pb::CombinatorType::AllCompleted => {
            s.waiting_completions.iter().all(|id| done(*id))
                && s.waiting_signals.iter().all(|i| signals.has_idx(*i))
                && s.waiting_named_signals.iter().all(|n| signals.has_name(n))
                && s.nested_futures
                    .iter()
                    .all(|n| suspension_satisfied_by(n, done, signals))
        }
        // UNKNOWN and FIRST_COMPLETED: any.
        _ => {
            s.waiting_completions.iter().any(|id| done(*id))
                || s.waiting_signals.iter().any(|i| signals.has_idx(*i))
                || s.waiting_named_signals.iter().any(|n| signals.has_name(n))
                || s.nested_futures
                    .iter()
                    .any(|n| suspension_satisfied_by(n, done, signals))
        }
    }
}

/// The protobuf bytes of a message, without the header.
fn payload_of(msg: &Message) -> Bytes {
    let wire = msg.encode(Flags::NONE);
    wire.slice(cairn_protocol::HEADER_LEN..)
}

#[cfg(test)]
mod tests {
    use {super::*, crate::journal::JournalEntry, cairn_protocol::HEADER_LEN};

    /// The **old** scan, kept verbatim as an oracle: scan the journal from the
    /// start for one completion, stopping at the first matching branch.
    fn scan_replay_task_for(journal: &Journal, id: u32, ty: MessageType) -> Option<Task> {
        for entry in journal.entries() {
            let frame = Frame::new(entry.ty, Flags::NONE, entry.payload.clone());
            let Ok(msg) = decode(&frame) else { continue };
            match (&msg, ty) {
                (Message::GetState(g), MessageType::GET_STATE_COMPLETION)
                    if g.result_completion_id == id =>
                {
                    return Some(Task::ReadState {
                        completion_id: id,
                        key: g.key.clone(),
                    })
                }
                (Message::GetStateKeys(g), MessageType::GET_STATE_KEYS_COMPLETION)
                    if g.result_completion_id == id =>
                {
                    return Some(Task::ReadStateKeys { completion_id: id })
                }
                (Message::GetPromise(p), MessageType::GET_PROMISE_COMPLETION)
                    if p.result_completion_id == id =>
                {
                    return Some(Task::PromiseGet {
                        completion_id: id,
                        key: p.key.clone(),
                    })
                }
                (Message::PeekPromise(p), MessageType::PEEK_PROMISE_COMPLETION)
                    if p.result_completion_id == id =>
                {
                    return Some(Task::PromisePeek {
                        completion_id: id,
                        key: p.key.clone(),
                    })
                }
                // Replayable because the engine makes it idempotent per
                // completer: if the earlier run wrote it, this one sees itself.
                (Message::CompletePromise(p), MessageType::COMPLETE_PROMISE_COMPLETION)
                    if p.result_completion_id == id =>
                {
                    return p.completion.clone().map(|result| Task::PromiseComplete {
                        completion_id: id,
                        key: p.key.clone(),
                        result,
                    })
                }
                (Message::AttachInvocation(a), MessageType::ATTACH_INVOCATION_COMPLETION)
                    if a.result_completion_id == id =>
                {
                    return Some(Task::Attach {
                        completion_id: id,
                        target_invocation_id: a.invocation_id.clone(),
                    })
                }
                (
                    Message::GetInvocationOutput(g),
                    MessageType::GET_INVOCATION_OUTPUT_COMPLETION,
                ) if g.result_completion_id == id => {
                    return Some(Task::PeekOutput {
                        completion_id: id,
                        target_invocation_id: g.invocation_id.clone(),
                    })
                }
                (Message::Sleep(s), MessageType::SLEEP_COMPLETION)
                    if s.result_completion_id == id =>
                {
                    return Some(Task::ScheduleTimer {
                        completion_id: id,
                        wake_up_time: s.wake_up_time,
                    })
                }
                // The task sending a call is tied to the **invocation id**
                // completion, not the result completion.
                //
                // This is easy to get wrong and costly: tied to the result
                // completion, every replay — and the caller replays on *every*
                // turn while waiting — would send one more call. The invocation
                // id completion is the receipt "already sent"; once it is in the
                // journal it must not be sent again. The result comes back on
                // its own later.
                (Message::Call(c), MessageType::CALL_INVOCATION_ID_COMPLETION)
                    if c.invocation_id_notification_idx == id =>
                {
                    return Some(call_task(c));
                }
                (Message::OneWayCall(c), MessageType::CALL_INVOCATION_ID_COMPLETION)
                    if c.invocation_id_notification_idx == id =>
                {
                    return Some(one_way_task(c));
                }
                // `Run` cannot be replayed: the result comes from the SDK
                // through ProposeRunCompletion, not from the runtime.
                _ => {}
            }
        }
        None
    }

    fn entry(msg: Message) -> JournalEntry {
        let wire = msg.encode(Flags::NONE);
        JournalEntry::new(msg.message_type(), wire.slice(HEADER_LEN..))
    }

    /// Every command with a completion, interleaved with commands without one,
    /// half of them already notified: the replay tasks from `on_start`'s index
    /// must match the old scan element by element, in order, for every pending
    /// completion.
    #[test]
    fn indexed_replay_matches_the_old_scan() {
        use pb::complete_promise_command_message::Completion as Done;
        let mut entries = vec![entry(Message::Input(pb::InputCommandMessage::default()))];
        let mut notify = Vec::new();
        for round in 0..3u32 {
            let base = round * 100;
            let cmds = vec![
                Message::GetState(pb::GetStateCommandMessage {
                    key: Bytes::from(format!("k{round}")),
                    result_completion_id: base + 1,
                    ..Default::default()
                }),
                Message::SetState(pb::SetStateCommandMessage::default()),
                Message::GetStateKeys(pb::GetStateKeysCommandMessage {
                    result_completion_id: base + 2,
                    ..Default::default()
                }),
                Message::GetPromise(pb::GetPromiseCommandMessage {
                    key: format!("p{round}"),
                    result_completion_id: base + 3,
                    ..Default::default()
                }),
                Message::PeekPromise(pb::PeekPromiseCommandMessage {
                    key: format!("q{round}"),
                    result_completion_id: base + 4,
                    ..Default::default()
                }),
                Message::CompletePromise(pb::CompletePromiseCommandMessage {
                    key: format!("c{round}"),
                    result_completion_id: base + 5,
                    completion: Some(Done::CompletionValue(cairn_protocol::value::inline(
                        Bytes::from_static(b"v"),
                    ))),
                    ..Default::default()
                }),
                // No result ⇒ no replay task.
                Message::CompletePromise(pb::CompletePromiseCommandMessage {
                    key: format!("d{round}"),
                    result_completion_id: base + 6,
                    completion: None,
                    ..Default::default()
                }),
                Message::AttachInvocation(pb::AttachInvocationCommandMessage {
                    invocation_id: format!("a{round}"),
                    result_completion_id: base + 7,
                    ..Default::default()
                }),
                Message::GetInvocationOutput(pb::GetInvocationOutputCommandMessage {
                    invocation_id: format!("o{round}"),
                    result_completion_id: base + 8,
                    ..Default::default()
                }),
                Message::Sleep(pb::SleepCommandMessage {
                    wake_up_time: 1_000 + u64::from(round),
                    result_completion_id: base + 9,
                    ..Default::default()
                }),
                Message::Call(pb::CallCommandMessage {
                    service_name: "S".into(),
                    handler_name: format!("h{round}"),
                    invocation_id_notification_idx: base + 10,
                    result_completion_id: base + 11,
                    ..Default::default()
                }),
                Message::OneWayCall(pb::OneWayCallCommandMessage {
                    service_name: "S".into(),
                    handler_name: format!("w{round}"),
                    invoke_time: u64::from(round),
                    invocation_id_notification_idx: base + 12,
                    ..Default::default()
                }),
                Message::Run(pb::RunCommandMessage {
                    result_completion_id: base + 13,
                    ..Default::default()
                }),
            ];
            entries.extend(cmds.into_iter().map(entry));
            // Middle round: the call's invocation id has come back, and a sleep has fired.
            if round == 1 {
                notify.push(entry(Message::CallInvocationIdCompletion(
                    pb::CallInvocationIdCompletionNotificationMessage {
                        completion_id: base + 10,
                        invocation_id: "callee".into(),
                    },
                )));
                notify.push(entry(Message::SleepCompletion(
                    pb::SleepCompletionNotificationMessage {
                        completion_id: base + 9,
                        ..Default::default()
                    },
                )));
            }
        }
        entries.extend(notify);
        let journal = Journal::from_entries(entries);

        let mut m = InvocationMachine::resume([7; 24], StartParams::default(), journal.clone());
        let got: Vec<Task> = m
            .handle(Event::Start)
            .unwrap()
            .into_iter()
            .filter_map(|e| match e {
                Effect::Task(t) => Some(t),
                _ => None,
            })
            .collect();
        let want: Vec<Task> = m
            .pending
            .iter()
            .filter_map(|(&id, &ty)| scan_replay_task_for(&journal, id, ty))
            .collect();
        assert!(!want.is_empty());
        assert_eq!(got, want);
        // No task for a completion that already has a notification.
        assert!(!got.iter().any(|t| matches!(
            t,
            Task::Invoke {
                invocation_id_completion_id: 110,
                ..
            } | Task::ScheduleTimer {
                completion_id: 109,
                ..
            }
        )));
        // Each entry decoded exactly once.
        assert_eq!(m.replay_decodes(), u64::from(journal.len()));
    }
}
