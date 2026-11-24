// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The warm-machine lemma: a machine that has replayed journal `J` and then
//! received a completion `c` **equals** the machine replaying `J ++ notif(c)`.
//!
//! The engine keeps the caller's machine in RAM between the drainer's
//! completion deliveries instead of reloading and replaying the whole journal
//! each time. The condition for that to be correct is this lemma, at every
//! prefix: the state machine is a function of the journal — `pending`,
//! `settled` (with kinds), signals seen, output and phase rebuilt from the
//! journal exactly as for the machine that lived through the events producing
//! that journal.
//!
//! The test randomly generates (fixed seeds) the event sequences a machine in
//! the `Processing` phase receives — SDK commands declaring completions (`Call`,
//! `Sleep`, `GetState`, `GetPromise`, `AttachInvocation`), completions in any
//! order, signals (including duplicates) — and after **every** event compares
//! the live machine with the machine replaying its journal
//! ([`InvocationMachine::divergence`]).

use {
    bytes::Bytes,
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, Message, MessageType},
    cairn_runtime::{
        Completion, CompletionResult, Effect, Event, InvocationMachine, Journal, JournalEntry,
        StartParams,
    },
};

const ID: [u8; 24] = [0x3b; 24];

fn frame<M: prost::Message>(ty: MessageType, m: &M) -> Frame {
    let wire = encode_message(ty, Flags::NONE, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

fn input_journal() -> Journal {
    let msg = Message::Input(pb::InputCommandMessage {
        headers: vec![],
        value: Some(cairn_protocol::value::inline(Bytes::from_static(b"{}"))),
        name: String::new(),
    });
    let wire = msg.encode(Flags::NONE);
    Journal::from_entries(vec![JournalEntry::new(
        MessageType::INPUT,
        wire.slice(cairn_protocol::HEADER_LEN..),
    )])
}

/// The machine replaying journal `j`, as the engine loads it cold (`load` + `Event::Start`).
fn cold(j: &Journal) -> InvocationMachine {
    let mut m = InvocationMachine::resume(ID, StartParams::default(), j.clone());
    m.handle(Event::Start).expect("replay");
    m
}

/// xorshift64 — enough for a test, deterministic per seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// A pending completion whose kind the test knows, to build a valid result.
#[derive(Clone, Copy)]
enum Want {
    CallId,
    CallResult,
    Timer,
    State,
    Promise,
    Attach,
}

fn result_for(w: Want, rng: &mut Rng) -> CompletionResult {
    match w {
        Want::CallId => CompletionResult::CallInvocationId(format!("callee-{}", rng.next(1 << 20))),
        Want::CallResult | Want::Promise | Want::Attach => match rng.next(3) {
            0 => CompletionResult::CallFailure(pb::Failure {
                code: 500,
                message: "broken".into(),
                metadata: vec![],
            }),
            _ => CompletionResult::CallValue(Bytes::from(format!("v{}", rng.next(100)))),
        },
        Want::Timer => CompletionResult::Timer,
        Want::State => match rng.next(2) {
            0 => CompletionResult::State(None),
            _ => CompletionResult::State(Some(cairn_protocol::value::Logical::Inline(
                Bytes::from_static(b"s"),
            ))),
        },
    }
}

/// A new SDK command declaring completions `next..`; returns the completions it declares.
fn command(rng: &mut Rng, next: &mut u32) -> (Frame, Vec<(u32, Want)>) {
    let id = *next;
    match rng.next(5) {
        0 => {
            *next += 2;
            (
                frame(
                    MessageType::CALL,
                    &pb::CallCommandMessage {
                        service_name: "S".into(),
                        handler_name: "h".into(),
                        parameter: Bytes::from(format!("{id}")),
                        invocation_id_notification_idx: id,
                        result_completion_id: id + 1,
                        ..Default::default()
                    },
                ),
                vec![(id, Want::CallId), (id + 1, Want::CallResult)],
            )
        }
        1 => {
            *next += 1;
            (
                frame(
                    MessageType::SLEEP,
                    &pb::SleepCommandMessage {
                        wake_up_time: 1_000 + u64::from(id),
                        result_completion_id: id,
                        name: String::new(),
                    },
                ),
                vec![(id, Want::Timer)],
            )
        }
        2 => {
            *next += 1;
            (
                frame(
                    MessageType::GET_STATE,
                    &pb::GetStateCommandMessage {
                        key: Bytes::from_static(b"k"),
                        result_completion_id: id,
                        name: String::new(),
                    },
                ),
                vec![(id, Want::State)],
            )
        }
        3 => {
            *next += 1;
            (
                frame(
                    MessageType::GET_PROMISE,
                    &pb::GetPromiseCommandMessage {
                        key: format!("p{id}"),
                        result_completion_id: id,
                        name: String::new(),
                    },
                ),
                vec![(id, Want::Promise)],
            )
        }
        _ => {
            *next += 1;
            (
                frame(
                    MessageType::ATTACH_INVOCATION,
                    &pb::AttachInvocationCommandMessage {
                        invocation_id: format!("target-{id}"),
                        result_completion_id: id,
                        name: String::new(),
                    },
                ),
                vec![(id, Want::Attach)],
            )
        }
    }
}

fn signal(idx: u32) -> pb::SignalNotificationMessage {
    use pb::signal_notification_message::{Result as R, SignalId as S};
    pb::SignalNotificationMessage {
        signal_id: Some(if idx.is_multiple_of(2) {
            S::Idx(idx)
        } else {
            S::Name(format!("s{idx}"))
        }),
        result: Some(R::Void(pb::Void {})),
    }
}

/// **The lemma, at every prefix.** A machine living through a random event
/// sequence equals the machine replaying its journal after **every** event; a
/// completion delivered to the live machine produces exactly one `Persist` at
/// index `J.len()`.
///
/// Must see: 400 seeds × 80 events, no divergence.
#[test]
fn completion_on_a_replayed_machine_equals_replaying_the_longer_journal() {
    for seed in 1..=400u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let mut warm = cold(&input_journal());
        let mut pending: Vec<(u32, Want)> = Vec::new();
        let mut next = 1u32;
        for step in 0..80 {
            let len = warm.journal().len();
            let what = match rng.next(10) {
                0..=3 => {
                    let (f, declared) = command(&mut rng, &mut next);
                    warm.handle(Event::FromSdk(f)).expect("command");
                    pending.extend(declared);
                    "command"
                }
                4..=7 if !pending.is_empty() => {
                    let (id, want) = pending.remove(rng.next(pending.len() as u64) as usize);
                    let fx = warm
                        .handle(Event::Completion(Completion {
                            id,
                            result: result_for(want, &mut rng),
                        }))
                        .expect("completion");
                    let persisted: Vec<u32> = fx
                        .iter()
                        .filter_map(|e| match e {
                            Effect::Persist { entry_index, .. } => Some(*entry_index),
                            _ => None,
                        })
                        .collect();
                    assert_eq!(
                        persisted,
                        vec![len],
                        "seed {seed} step {step}: completion {id} must write exactly one entry at \
                         {len}"
                    );
                    "completion"
                }
                _ => {
                    warm.handle(Event::Signal(signal(rng.next(6) as u32)))
                        .expect("signal");
                    "signal"
                }
            };
            let replayed = cold(warm.journal());
            if let Some(d) = warm.divergence(&replayed) {
                panic!(
                    "seed {seed} step {step} ({what}): the live machine diverges from the \
                        replaying machine — {d}"
                );
            }
        }
    }
}

/// `divergence` sees a real divergence: two machines with different journals,
/// or the same journal with one machine closed.
#[test]
fn divergence_sees_a_different_journal_and_a_closed_machine() {
    let a = cold(&input_journal());
    let mut b = cold(&input_journal());
    assert_eq!(a.divergence(&b), None);
    let mut next = 1;
    let mut rng = Rng(7);
    let (f, _) = command(&mut rng, &mut next);
    b.handle(Event::FromSdk(f)).expect("command");
    assert!(a.divergence(&b).is_some_and(|d| d.starts_with("journal")));

    let mut c = cold(&input_journal());
    // A completion for an undeclared id ⇒ a protocol violation, the machine
    // closes; the journal does not change.
    c.handle(Event::Completion(Completion {
        id: 99,
        result: CompletionResult::Timer,
    }))
    .expect("an unknown completion");
    assert!(a.divergence(&c).is_some_and(|d| d.starts_with("phase")));
}

/// `suspension_satisfied_by` (the lookup supplied by the caller — the drainer
/// looks up the machine's ledger) returns exactly what `suspension_satisfied`
/// returns on a slice, for random nested ALL/FIRST trees and random completion
/// sets.
#[test]
fn satisfied_by_a_lookup_equals_satisfied_on_a_slice() {
    use cairn_runtime::{suspension_satisfied, suspension_satisfied_by, SettledSignals};
    fn tree(rng: &mut Rng, depth: u32) -> pb::SuspensionMessage {
        pb::SuspensionMessage {
            waiting_completions: (0..rng.next(4)).map(|_| rng.next(12) as u32).collect(),
            nested_futures: if depth == 0 {
                vec![]
            } else {
                (0..rng.next(3)).map(|_| tree(rng, depth - 1)).collect()
            },
            combinator_type: rng.next(3) as i32,
            ..Default::default()
        }
    }
    let none = SettledSignals { signals: &[] };
    for seed in 1..=2_000u64 {
        let mut rng = Rng(seed.wrapping_mul(0x2545_f491_4f6c_dd1d) | 1);
        let t = tree(&mut rng, 3);
        let settled: Vec<u32> = (0..12u32).filter(|_| rng.next(2) == 0).collect();
        assert_eq!(
            suspension_satisfied_by(&t, &|id| settled.contains(&id), none),
            suspension_satisfied(&t, &settled, none),
            "seed {seed}: {t:?} {settled:?}"
        );
    }
}
