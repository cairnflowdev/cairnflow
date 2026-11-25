// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Phase 4 — eager state, state keys, promises, attach, suspension trees.
//!
//! Each test points at one sentence of `docs/protocol-v1.md` (sections 5, 7,
//! 8.1, 8.3).

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    bytes::Bytes,
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, Message, MessageType},
    cairn_runtime::{
        machine::{suspension_has_leaves, suspension_satisfied, SettledSignals},
        Completion, CompletionResult, Effect, Event, InvocationMachine, Journal, JournalEntry,
        Outcome, StartParams, Task,
    },
};

const ID: [u8; 24] = [0x22; 24];

fn frame<M: prost::Message>(ty: MessageType, m: &M) -> Frame {
    let wire = encode_message(ty, Flags::NONE, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

fn fresh() -> InvocationMachine {
    let mut m =
        InvocationMachine::resume(ID, StartParams::default(), Journal::from_entries(vec![]));
    m.handle(Event::Start).unwrap();
    m
}

fn tasks(fx: &[Effect]) -> Vec<&Task> {
    fx.iter()
        .filter_map(|f| match f {
            Effect::Task(t) => Some(t),
            _ => None,
        })
        .collect()
}

fn sends(fx: &[Effect]) -> Vec<&Message> {
    fx.iter()
        .filter_map(|f| match f {
            Effect::Send(m) => Some(m),
            _ => None,
        })
        .collect()
}

fn persisted(fx: &[Effect]) -> Vec<MessageType> {
    fx.iter()
        .filter_map(|f| match f {
            Effect::Persist { entry, .. } => Some(entry.ty),
            _ => None,
        })
        .collect()
}

// ───────────────────────────────────────────────── eager state

/// Spec section 7: `StartMessage` carries `state_map` and `partial_state` from `StartParams`.
#[test]
fn start_carries_state_map() {
    let params = StartParams {
        state_map: vec![(Bytes::from_static(b"k"), Bytes::from_static(b"v"))],
        partial_state: true,
        ..Default::default()
    };
    let mut m = InvocationMachine::resume(ID, params, Journal::from_entries(vec![]));
    let fx = m.handle(Event::Start).unwrap();
    let Some(Message::Start(s)) = sends(&fx).first().copied().cloned() else {
        panic!("the first message must be Start");
    };
    assert_eq!(s.state_map.len(), 1);
    assert_eq!(s.state_map[0].key, Bytes::from_static(b"k"));
    assert!(s.partial_state);
}

/// Spec section 5: `GetEagerState` is NOT completable — journal only, no task,
/// no completion promised.
#[test]
fn eager_state_journal_only() {
    let mut m = fresh();
    let cmd = pb::GetEagerStateCommandMessage {
        key: Bytes::from_static(b"k"),
        result: Some(pb::get_eager_state_command_message::Result::Value(
            cairn_protocol::value::inline(Bytes::from_static(b"v")),
        )),
        name: "k".into(),
    };
    let fx = m
        .handle(Event::FromSdk(frame(MessageType::GET_EAGER_STATE, &cmd)))
        .unwrap();
    assert_eq!(persisted(&fx), vec![MessageType::GET_EAGER_STATE]);
    assert!(tasks(&fx).is_empty(), "eager state produces no task");
    assert!(m.pending_completions().is_empty(), "no completion promised");
}

// ───────────────────────────────────────── ClearAllState / GetStateKeys

#[test]
fn clear_all_state_yields_task() {
    let mut m = fresh();
    let fx = m
        .handle(Event::FromSdk(frame(
            MessageType::CLEAR_ALL_STATE,
            &pb::ClearAllStateCommandMessage {
                name: "clearAll".into(),
            },
        )))
        .unwrap();
    assert_eq!(persisted(&fx), vec![MessageType::CLEAR_ALL_STATE]);
    assert_eq!(tasks(&fx), vec![&Task::ClearAllState]);
}

/// Spec section 8.1: `GetStateKeys` promises a completion; the `StateKeys`
/// result becomes notification `0x8006` — journaled, then sent.
#[test]
fn get_state_keys_full_lifecycle() {
    let mut m = fresh();
    let fx = m
        .handle(Event::FromSdk(frame(
            MessageType::GET_STATE_KEYS,
            &pb::GetStateKeysCommandMessage {
                result_completion_id: 1,
                name: "getKeys".into(),
            },
        )))
        .unwrap();
    assert_eq!(tasks(&fx), vec![&Task::ReadStateKeys { completion_id: 1 }]);
    assert_eq!(m.pending_completions(), vec![1]);

    let fx = m
        .handle(Event::Completion(Completion {
            id: 1,
            result: CompletionResult::StateKeys(vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ]),
        }))
        .unwrap();
    assert_eq!(persisted(&fx), vec![MessageType::GET_STATE_KEYS_COMPLETION]);
    let Some(Message::GetStateKeysCompletion(n)) = sends(&fx).first().copied().cloned() else {
        panic!("GetStateKeysCompletion must be sent");
    };
    assert_eq!(n.completion_id, 1);
    assert_eq!(n.state_keys.unwrap().keys.len(), 2);
    assert_eq!(m.settled_completions(), vec![1]);
}

// ───────────────────────────────────────────────── promise

#[test]
fn promise_get_and_complete_yield_tasks() {
    let mut m = fresh();
    let fx = m
        .handle(Event::FromSdk(frame(
            MessageType::GET_PROMISE,
            &pb::GetPromiseCommandMessage {
                key: "approve".into(),
                result_completion_id: 1,
                name: "promise/approve".into(),
            },
        )))
        .unwrap();
    assert_eq!(
        tasks(&fx),
        vec![&Task::PromiseGet {
            completion_id: 1,
            key: "approve".into()
        }]
    );

    let fx = m
        .handle(Event::FromSdk(frame(
            MessageType::COMPLETE_PROMISE,
            &pb::CompletePromiseCommandMessage {
                key: "approve".into(),
                completion: Some(
                    pb::complete_promise_command_message::Completion::CompletionValue(
                        cairn_protocol::value::inline(Bytes::from_static(b"\"ok\"")),
                    ),
                ),
                result_completion_id: 2,
                name: "promise/approve".into(),
            },
        )))
        .unwrap();
    assert!(matches!(
        tasks(&fx)[..],
        [Task::PromiseComplete {
            completion_id: 2,
            ..
        }]
    ));

    // The GetPromise result comes back as a value → notification 0x8009.
    let fx = m
        .handle(Event::Completion(Completion {
            id: 1,
            result: CompletionResult::CallValue(Bytes::from_static(b"\"ok\"")),
        }))
        .unwrap();
    assert_eq!(persisted(&fx), vec![MessageType::GET_PROMISE_COMPLETION]);

    // CompletePromise wins → void → notification 0x800B.
    let fx = m
        .handle(Event::Completion(Completion {
            id: 2,
            result: CompletionResult::Void,
        }))
        .unwrap();
    assert_eq!(
        persisted(&fx),
        vec![MessageType::COMPLETE_PROMISE_COMPLETION]
    );
}

/// A `CompletePromise` without a result is a broken command — 571, not a silent hang.
#[test]
fn completing_empty_promise_is_violation() {
    let mut m = fresh();
    let fx = m
        .handle(Event::FromSdk(frame(
            MessageType::COMPLETE_PROMISE,
            &pb::CompletePromiseCommandMessage {
                key: "x".into(),
                completion: None,
                result_completion_id: 1,
                name: String::new(),
            },
        )))
        .unwrap();
    assert!(matches!(
        fx.last(),
        Some(Effect::Finish(Outcome::Failed { code: 571, .. }))
    ));
}

/// A restart midway: a pending promise completion must re-issue its task — the
/// recovery path after failover, the same pattern as `GetState`/`Sleep`.
#[test]
fn pending_promise_is_replayed_after_restart() {
    let mut m = fresh();
    m.handle(Event::FromSdk(frame(
        MessageType::GET_PROMISE,
        &pb::GetPromiseCommandMessage {
            key: "approve".into(),
            result_completion_id: 1,
            name: String::new(),
        },
    )))
    .unwrap();

    // Rebuild from the journal (simulating a new leader).
    let entries: Vec<JournalEntry> = m.journal().entries().to_vec();
    let mut m2 =
        InvocationMachine::resume(ID, StartParams::default(), Journal::from_entries(entries));
    let fx = m2.handle(Event::Start).unwrap();
    assert!(
        tasks(&fx).contains(&&Task::PromiseGet {
            completion_id: 1,
            key: "approve".into()
        }),
        "the promise task must be re-issued"
    );
}

// ───────────────────────────────────────────── attach / get output

#[test]
fn attach_and_peek_produce_task_and_notification() {
    let mut m = fresh();
    let fx = m
        .handle(Event::FromSdk(frame(
            MessageType::ATTACH_INVOCATION,
            &pb::AttachInvocationCommandMessage {
                invocation_id: "aa".repeat(24),
                result_completion_id: 1,
                name: String::new(),
            },
        )))
        .unwrap();
    assert!(matches!(
        tasks(&fx)[..],
        [Task::Attach {
            completion_id: 1,
            ..
        }]
    ));

    let fx = m
        .handle(Event::FromSdk(frame(
            MessageType::GET_INVOCATION_OUTPUT,
            &pb::GetInvocationOutputCommandMessage {
                invocation_id: "bb".repeat(24),
                result_completion_id: 2,
                name: String::new(),
            },
        )))
        .unwrap();
    assert!(matches!(
        tasks(&fx)[..],
        [Task::PeekOutput {
            completion_id: 2,
            ..
        }]
    ));

    // Attach done → value → 0x8012. Peek not done → void → 0x8013.
    let fx = m
        .handle(Event::Completion(Completion {
            id: 1,
            result: CompletionResult::CallValue(Bytes::from_static(b"1")),
        }))
        .unwrap();
    assert_eq!(
        persisted(&fx),
        vec![MessageType::ATTACH_INVOCATION_COMPLETION]
    );

    let fx = m
        .handle(Event::Completion(Completion {
            id: 2,
            result: CompletionResult::Void,
        }))
        .unwrap();
    assert_eq!(
        persisted(&fx),
        vec![MessageType::GET_INVOCATION_OUTPUT_COMPLETION]
    );
}

// ───────────────────────────────────────────── suspension trees

/// Spec section 8.3: the wait list MUST NOT be empty — with a tree, "empty"
/// means no leaf at any level.
#[test]
fn empty_suspension_tree_is_violation_leaf_tree_is_not() {
    // A tree of only nested empty nodes: still empty.
    let empty_tree = pb::SuspensionMessage {
        nested_futures: vec![pb::SuspensionMessage::default()],
        ..Default::default()
    };
    assert!(!suspension_has_leaves(&empty_tree));
    let mut m = fresh();
    let fx = m
        .handle(Event::FromSdk(frame(MessageType::SUSPENSION, &empty_tree)))
        .unwrap();
    assert!(matches!(
        fx.last(),
        Some(Effect::Finish(Outcome::Failed { code: 571, .. }))
    ));

    // A leaf deep inside a child node: valid.
    let deep_tree = pb::SuspensionMessage {
        nested_futures: vec![pb::SuspensionMessage {
            waiting_completions: vec![7],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(suspension_has_leaves(&deep_tree));
    let mut m = fresh();
    let fx = m
        .handle(Event::FromSdk(frame(MessageType::SUSPENSION, &deep_tree)))
        .unwrap();
    assert!(matches!(
        fx.last(),
        Some(Effect::Finish(Outcome::Suspended(_)))
    ));
}

/// Tree semantics: FIRST is satisfied by any, ALL by all — and
/// COMBINATOR_TYPE_UNKNOWN (a v1 message) is FIRST.
#[test]
fn suspension_satisfied_follows_combinator() {
    // v1 flat: any.
    let v1 = pb::SuspensionMessage {
        waiting_completions: vec![1, 2],
        ..Default::default()
    };
    let no_signals = SettledSignals::default();
    assert!(suspension_satisfied(&v1, &[2], no_signals));
    assert!(!suspension_satisfied(&v1, &[3], no_signals));

    // ALL wrapped in a child node (the form the SDK emits for ctx.all).
    let all = pb::SuspensionMessage {
        nested_futures: vec![pb::SuspensionMessage {
            waiting_completions: vec![1, 2],
            combinator_type: pb::CombinatorType::AllCompleted as i32,
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(
        !suspension_satisfied(&all, &[1], no_signals),
        "ALL needs both"
    );
    assert!(suspension_satisfied(&all, &[1, 2], no_signals));

    // Signal leaves: ALL waiting for a signal that has not arrived is not
    // satisfied; once the signal arrives (same identity, idx or name) it is.
    // Before, a signal leaf was NEVER satisfied and an ALL node containing a
    // signal hung forever.
    let all_signal = pb::SuspensionMessage {
        nested_futures: vec![pb::SuspensionMessage {
            waiting_completions: vec![1],
            waiting_signals: vec![1],
            waiting_named_signals: vec!["approve".into()],
            combinator_type: pb::CombinatorType::AllCompleted as i32,
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(!suspension_satisfied(&all_signal, &[1], no_signals));
    use pb::signal_notification_message::SignalId as S;
    let missing_name = [S::Idx(1)];
    assert!(!suspension_satisfied(
        &all_signal,
        &[1],
        SettledSignals {
            signals: &missing_name
        }
    ));
    let enough = [S::Idx(1), S::Name("approve".into())];
    let enough = SettledSignals { signals: &enough };
    assert!(suspension_satisfied(&all_signal, &[1], enough));

    // FIRST: one signal arriving is enough to wake, no completion needed.
    let first_signal = pb::SuspensionMessage {
        waiting_completions: vec![9],
        waiting_named_signals: vec!["approve".into()],
        ..Default::default()
    };
    assert!(suspension_satisfied(&first_signal, &[], enough));
}
