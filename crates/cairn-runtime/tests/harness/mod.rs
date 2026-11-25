// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A deterministic fake SDK + driver, enough to run the four mandatory tests of
//! spec section 10.
//!
//! The fake SDK runs a "user program" that is a fixed list of steps. It does
//! exactly what a real SDK must do and what section 10 requires checking:
//! matching entries on replay, matching notifications **by id**, not rerunning
//! a side effect whose result is already in the journal, and reporting 570 when
//! the journal does not match the code.

use {
    bytes::Bytes,
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, Message, MessageType},
    cairn_runtime::{
        Completion, CompletionResult, Effect, Event, InvocationMachine, Journal, JournalEntry,
        Outcome, StartParams, Task,
    },
    std::collections::{BTreeMap, BTreeSet, VecDeque},
};

pub const INV_ID: [u8; 24] = [0x11; 24];

/// One step of the "user code".
#[derive(Clone, Debug, PartialEq)]
pub enum Step {
    GetState {
        key: &'static str,
        id: u32,
    },
    SetState {
        key: &'static str,
        value: &'static str,
    },
    Sleep {
        at: u64,
        id: u32,
    },
    /// `ctx.run` — has a side effect, its result must be durable so it does not run again.
    Run {
        id: u32,
        result: &'static str,
    },
    Output {
        value: &'static str,
    },
}

impl Step {
    fn command_type(&self) -> MessageType {
        match self {
            Step::GetState { .. } => MessageType::GET_STATE,
            Step::SetState { .. } => MessageType::SET_STATE,
            Step::Sleep { .. } => MessageType::SLEEP,
            Step::Run { .. } => MessageType::RUN,
            Step::Output { .. } => MessageType::OUTPUT,
        }
    }

    fn awaited_completion(&self) -> Option<u32> {
        match self {
            Step::GetState { id, .. } | Step::Sleep { id, .. } | Step::Run { id, .. } => Some(*id),
            _ => None,
        }
    }
}

fn frame<M: prost::Message>(ty: MessageType, flags: Flags, m: &M) -> Frame {
    let wire = encode_message(ty, flags, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

pub struct ScriptedSdk {
    program: Vec<Step>,
    cursor: usize,
    emitted: BTreeSet<usize>,
    /// `Run` steps that proposed a result in *this run*.
    proposed: BTreeSet<usize>,
    /// Whether `End` has been sent on this connection.
    ended: bool,
    completions: BTreeMap<u32, ()>,
    known_entries: u32,
    seen_entries: u32,
    replayed_commands: usize,
    /// Counts how many times a `ctx.run` side effect actually ran.
    pub side_effect_runs: u32,
    mismatch: bool,
}

impl ScriptedSdk {
    pub fn new(program: Vec<Step>) -> Self {
        Self {
            program,
            cursor: 0,
            emitted: BTreeSet::new(),
            proposed: BTreeSet::new(),
            ended: false,
            completions: BTreeMap::new(),
            known_entries: 0,
            seen_entries: 0,
            replayed_commands: 0,
            side_effect_runs: 0,
            mismatch: false,
        }
    }

    /// Receives one message from the runtime, returns the frames sent back up.
    pub fn on_message(&mut self, msg: &Message) -> Vec<Frame> {
        if self.mismatch {
            return vec![];
        }
        if let Message::Start(s) = msg {
            self.known_entries = s.known_entries;
            self.seen_entries = 0;
            return if self.known_entries == 0 {
                self.drain()
            } else {
                vec![]
            };
        }

        if self.seen_entries < self.known_entries {
            self.seen_entries += 1;
            if let Some(err) = self.on_replay_entry(msg) {
                self.mismatch = true;
                return vec![err];
            }
            if self.seen_entries == self.known_entries {
                return self.drain();
            }
            return vec![];
        }

        // The PROCESSING phase: acks and notifications.
        match msg {
            Message::CommandAck(_) => vec![],
            Message::ProposeRunCompletionAck(a) => {
                self.completions.insert(a.completion_id, ());
                self.drain()
            }
            other => {
                if let Some(id) = completion_id(other) {
                    self.completions.insert(id, ());
                }
                self.drain()
            }
        }
    }

    /// Matches a replayed entry against the program. `Some(frame)` = error 570.
    fn on_replay_entry(&mut self, msg: &Message) -> Option<Frame> {
        // Entry 0 is Input, created by the runtime — not part of the user program.
        if matches!(msg, Message::Input(_)) {
            return None;
        }
        if let Some(id) = completion_id(msg) {
            self.completions.insert(id, ());
            return None;
        }

        let pos = self.replayed_commands;
        self.replayed_commands += 1;
        let Some(step) = self.program.get(pos) else {
            return Some(self.journal_mismatch(&format!(
                "the journal has entry {pos} but the code has only {} steps left",
                self.program.len()
            )));
        };
        if msg.message_type() != step.command_type() {
            return Some(self.journal_mismatch(&format!(
                "entry {pos}: the journal has {}, the code produced {}",
                msg.message_type(),
                step.command_type()
            )));
        }
        // Do not rerun the side effect: this step is already in the journal.
        self.emitted.insert(pos);
        None
    }

    fn journal_mismatch(&self, why: &str) -> Frame {
        frame(
            MessageType::ERROR,
            Flags::NONE,
            &pb::ErrorMessage {
                code: cairn_protocol::JOURNAL_MISMATCH,
                message: why.to_string(),
                ..Default::default()
            },
        )
    }

    fn drain(&mut self) -> Vec<Frame> {
        let mut out = Vec::new();
        while self.cursor < self.program.len() {
            let step = self.program[self.cursor].clone();
            if !self.emitted.contains(&self.cursor) {
                self.emit_command(&step, &mut out);
                self.emitted.insert(self.cursor);
            }

            // `ctx.run` differs from every other command: the result comes from
            // the **SDK**, not the runtime. So if the journal has the command but
            // no result yet — the process died right between the two — the SDK
            // MUST run the side effect again. That is ctx.run's at-least-once
            // semantics, and also why section 8.4 forces all outside I/O through
            // it.
            if let Step::Run { id, result } = &step {
                if !self.completions.contains_key(id) && !self.proposed.contains(&self.cursor) {
                    self.side_effect_runs += 1;
                    self.proposed.insert(self.cursor);
                    out.push(frame(
                        MessageType::PROPOSE_RUN_COMPLETION,
                        Flags::REQUESTED_ACK,
                        &pb::ProposeRunCompletionMessage {
                            result_completion_id: *id,
                            result: Some(pb::propose_run_completion_message::Result::Value(
                                Bytes::from_static(result.as_bytes()),
                            )),
                        },
                    ));
                }
            }

            match step.awaited_completion() {
                // Wait for the completion — matched by ID, not by position (section 8.1).
                Some(id) if !self.completions.contains_key(&id) => break,
                _ => self.cursor += 1,
            }
        }

        // `End` is NOT a journal entry — it is the signal "the user code has
        // returned", scoped to one connection. So if the process dies after
        // `Output` is durable but before `End` is sent, the next replay MUST send
        // it again. Treating `End` as something recorded in the journal would
        // leave the invocation hanging forever at exactly the last step.
        if self.cursor >= self.program.len() && !self.ended {
            self.ended = true;
            out.push(frame(MessageType::END, Flags::NONE, &pb::EndMessage {}));
        }
        out
    }

    fn emit_command(&mut self, step: &Step, out: &mut Vec<Frame>) {
        match step {
            Step::GetState { key, id } => out.push(frame(
                MessageType::GET_STATE,
                Flags::NONE,
                &pb::GetStateCommandMessage {
                    key: Bytes::from_static(key.as_bytes()),
                    result_completion_id: *id,
                    name: String::new(),
                },
            )),
            Step::SetState { key, value } => out.push(frame(
                MessageType::SET_STATE,
                Flags::NONE,
                &pb::SetStateCommandMessage {
                    key: Bytes::from_static(key.as_bytes()),
                    value: Some(cairn_protocol::value::inline(Bytes::from_static(
                        value.as_bytes(),
                    ))),
                    name: String::new(),
                },
            )),
            Step::Sleep { at, id } => out.push(frame(
                MessageType::SLEEP,
                Flags::NONE,
                &pb::SleepCommandMessage {
                    wake_up_time: *at,
                    result_completion_id: *id,
                    name: String::new(),
                },
            )),
            // Only emit the command. Running the side effect and proposing the
            // result happen in `drain`, since that depends on whether the journal
            // already has the result.
            Step::Run { id, .. } => out.push(frame(
                MessageType::RUN,
                Flags::NONE,
                &pb::RunCommandMessage {
                    result_completion_id: *id,
                    name: String::new(),
                },
            )),
            // Only emit `Output`; `End` is sent by `drain` when the program runs out of steps.
            Step::Output { value } => out.push(frame(
                MessageType::OUTPUT,
                Flags::NONE,
                &pb::OutputCommandMessage {
                    result: Some(pb::output_command_message::Result::Value(
                        cairn_protocol::value::inline(Bytes::from_static(value.as_bytes())),
                    )),
                    name: String::new(),
                },
            )),
        }
    }
}

fn completion_id(msg: &Message) -> Option<u32> {
    Some(match msg {
        Message::GetStateCompletion(n) => n.completion_id,
        Message::SleepCompletion(n) => n.completion_id,
        Message::CallCompletion(n) => n.completion_id,
        Message::CallInvocationIdCompletion(n) => n.completion_id,
        Message::RunCompletion(n) => n.completion_id,
        _ => return None,
    })
}

/// The result of one run.
#[allow(dead_code)]
pub struct RunResult {
    pub journal: Vec<JournalEntry>,
    pub outcome: Option<Outcome>,
    /// The messages the runtime sent down to the SDK, in order — used to check section 8.2.
    pub sent: Vec<Message>,
    pub side_effect_runs: u32,
    pub killed: bool,
}

/// The initial journal of a new invocation: entry 0 is `Input`, created by the
/// runtime when it receives the request (not produced by the state machine).
pub fn initial_journal(input: &'static str) -> Journal {
    let msg = Message::Input(pb::InputCommandMessage {
        headers: vec![],
        value: Some(cairn_protocol::value::inline(Bytes::from_static(
            input.as_bytes(),
        ))),
        name: String::new(),
    });
    let wire = msg.encode(Flags::NONE);
    Journal::from_entries(vec![JournalEntry::new(
        MessageType::INPUT,
        wire.slice(cairn_protocol::HEADER_LEN..),
    )])
}

/// Runs an invocation until it finishes, or stops right after persist number
/// `kill_after_persists` (simulating a process death).
pub fn run(
    program: Vec<Step>,
    journal: Journal,
    state: &BTreeMap<String, Bytes>,
    kill_after_persists: Option<u32>,
) -> RunResult {
    let mut machine = InvocationMachine::resume(INV_ID, StartParams::default(), journal.clone());
    let mut sdk = ScriptedSdk::new(program);
    let mut persisted: Vec<JournalEntry> = journal.entries().to_vec();
    let mut sent = Vec::new();
    let mut outcome = None;
    let mut persists = 0u32;
    let mut killed = false;

    let mut events: VecDeque<Event> = VecDeque::from([Event::Start]);
    'outer: while let Some(ev) = events.pop_front() {
        let effects = machine.handle(ev).expect("the journal is not broken");
        for fx in effects {
            match fx {
                Effect::Persist { entry_index, entry } => {
                    // The journal is append-only: the index must match the next position.
                    assert_eq!(
                        entry_index as usize,
                        persisted.len(),
                        "persist at the wrong position"
                    );
                    persisted.push(entry);
                    persists += 1;
                    if Some(persists) == kill_after_persists {
                        killed = true;
                        break 'outer;
                    }
                }
                Effect::Send(msg) => {
                    sent.push(msg.clone());
                    for f in sdk.on_message(&msg) {
                        events.push_back(Event::FromSdk(f));
                    }
                }
                Effect::Task(t) => {
                    if let Some(c) = resolve(&t, state) {
                        events.push_back(Event::Completion(c));
                    }
                }
                Effect::ForwardCustom { .. } => {}
                // The first outcome wins: a later effect must not overwrite the
                // real reason the invocation stopped.
                Effect::Finish(o) => outcome = outcome.or(Some(o)),
            }
        }
    }

    RunResult {
        journal: persisted,
        outcome,
        sent,
        side_effect_runs: sdk.side_effect_runs,
        killed,
    }
}

/// A deterministic driver: state read from a map, timers fire at once.
fn resolve(task: &Task, state: &BTreeMap<String, Bytes>) -> Option<Completion> {
    match task {
        Task::ReadState { completion_id, key } => Some(Completion {
            id: *completion_id,
            result: CompletionResult::State(
                state
                    .get(std::str::from_utf8(key).unwrap())
                    .cloned()
                    .map(cairn_protocol::value::Logical::Inline),
            ),
        }),
        Task::ScheduleTimer { completion_id, .. } => Some(Completion {
            id: *completion_id,
            result: CompletionResult::Timer,
        }),
        _ => None,
    }
}
