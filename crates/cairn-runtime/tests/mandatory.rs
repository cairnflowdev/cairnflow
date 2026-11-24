// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The four **mandatory** tests of `protocol-v1.md` section 10.
//!
//! The spec says it plainly: until these four pass, it is not done. This is
//! their runtime part.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

mod harness;

use {
    bytes::Bytes,
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, MessageType},
    cairn_runtime::{
        Completion, CompletionResult, Effect, Event, FailureAction, InvocationMachine, Journal,
        Outcome, StartParams,
    },
    harness::{initial_journal, run, Step, INV_ID},
    std::collections::BTreeMap,
};

fn sample_program() -> Vec<Step> {
    vec![
        Step::GetState {
            key: "count",
            id: 1,
        },
        Step::Sleep { at: 1_000, id: 2 },
        Step::Run {
            id: 3,
            result: "charged",
        },
        Step::SetState {
            key: "count",
            value: "1",
        },
        Step::Output { value: "done" },
    ]
}

fn state() -> BTreeMap<String, Bytes> {
    BTreeMap::from([("count".to_string(), Bytes::from_static(b"0"))])
}

/// ## Mandatory test 1 — bit-exact replay
///
/// *"Run an invocation, kill it midway at every command index, replay, confirm
/// the journal and output are identical."*
///
/// This is the most expensive test and also the only one that catches a whole
/// class of bugs: journal written in the wrong order, entries missing on
/// replay, a durable side effect run again. It can run thanks to the pure state
/// machine — rebuilding from a journal is just one call.
#[test]
fn replay_is_bit_exact_at_every_kill_point() {
    let state = state();
    let full_run = run(sample_program(), initial_journal("req"), &state, None);
    assert!(!full_run.killed);
    let reference_journal = full_run.journal.clone();
    let reference_output = full_run.outcome.clone();
    assert!(
        matches!(reference_output, Some(Outcome::Completed(_))),
        "the reference run must complete, got: {reference_output:?}"
    );

    let persist_count = reference_journal.len() as u32 - 1; // minus the existing Input entry
    assert!(
        persist_count >= 5,
        "the program is too short for the test to mean anything"
    );

    for k in 1..=persist_count {
        // Die right after persist number k.
        let killed_run = run(sample_program(), initial_journal("req"), &state, Some(k));
        assert!(killed_run.killed, "k={k} should have been killed");
        assert!(
            killed_run.outcome.is_none(),
            "k={k}: killed, yet the invocation still finished"
        );

        // A new leader rebuilds from exactly what is durable.
        let resumed = run(
            sample_program(),
            Journal::from_entries(killed_run.journal.clone()),
            &state,
            None,
        );

        assert_eq!(
            resumed.journal, reference_journal,
            "k={k}: the journal after replay differs from the reference journal"
        );
        match (&resumed.outcome, &reference_output) {
            (Some(Outcome::Completed(a)), Some(Outcome::Completed(b))) => {
                assert_eq!(a, b, "k={k}: the output differs")
            }
            other => panic!("k={k}: not completed after replay: {other:?}"),
        }
    }
}

/// A direct consequence of durable execution: a side effect whose result is in
/// the journal **must not run again**.
#[test]
fn durable_side_effect_does_not_rerun_on_replay() {
    let state = state();
    let full_run = run(sample_program(), initial_journal("req"), &state, None);
    assert_eq!(
        full_run.side_effect_runs, 1,
        "the first run must run exactly once"
    );

    // Replay from the full journal: it must not run again even once.
    let replayed = run(
        sample_program(),
        Journal::from_entries(full_run.journal.clone()),
        &state,
        None,
    );
    assert_eq!(
        replayed.side_effect_runs, 0,
        "reran a durable side effect — ctx.run loses its correctness"
    );
    assert_eq!(
        replayed.journal, full_run.journal,
        "the replay made the journal grow"
    );
}

/// ## Mandatory test 2 — notifications arriving out of order
///
/// Spec section 8.1: *"Notifications MAY arrive out of order. The SDK MUST match
/// them by id, MUST NOT match them by position."*
///
/// Checked at the runtime layer: two pending completions, returned in the
/// **reverse** of declaration order. Each notification must carry exactly its
/// own id, and the journal records them in the real arrival order — not the
/// declaration order.
#[test]
fn out_of_order_notifications() {
    let mut m = InvocationMachine::resume(INV_ID, StartParams::default(), initial_journal("req"));
    m.handle(Event::Start).unwrap();

    // Two sleep commands emitted back to back, none finished yet.
    for (at, id) in [(1_000u64, 1u32), (2_000, 2)] {
        let f = frame(
            MessageType::SLEEP,
            Flags::NONE,
            &pb::SleepCommandMessage {
                wake_up_time: at,
                result_completion_id: id,
                name: String::new(),
            },
        );
        m.handle(Event::FromSdk(f)).unwrap();
    }
    assert_eq!(m.pending_completions(), vec![1, 2]);

    // Return them in REVERSE order.
    let mut ids = Vec::new();
    for id in [2u32, 1] {
        let fx = m
            .handle(Event::Completion(Completion {
                id,
                result: CompletionResult::Timer,
            }))
            .unwrap();
        for e in fx {
            if let Effect::Send(msg) = e {
                ids.push(completion_id_of(&msg));
            }
        }
    }
    assert_eq!(ids, vec![2, 1], "the runtime reordered notifications");
    assert!(m.pending_completions().is_empty());

    // The journal records the real arrival order — replay will reproduce exactly that order (P3).
    let order: Vec<u32> = m
        .journal()
        .entries()
        .iter()
        .filter(|e| e.is_notification())
        .map(|e| {
            let f = Frame::new(e.ty, Flags::NONE, e.payload.clone());
            completion_id_of(&cairn_protocol::decode(&f).unwrap())
        })
        .collect();
    assert_eq!(
        order,
        vec![2, 1],
        "the journal does not keep the real arrival order"
    );
}

/// Spec section 8.1: *"One completion id MUST NOT receive two notifications."*
#[test]
fn one_completion_id_never_gets_two_notifications() {
    let mut m = InvocationMachine::resume(INV_ID, StartParams::default(), initial_journal("req"));
    m.handle(Event::Start).unwrap();
    m.handle(Event::FromSdk(frame(
        MessageType::SLEEP,
        Flags::NONE,
        &pb::SleepCommandMessage {
            wake_up_time: 1,
            result_completion_id: 1,
            name: String::new(),
        },
    )))
    .unwrap();

    let ok = Completion {
        id: 1,
        result: CompletionResult::Timer,
    };
    m.handle(Event::Completion(ok.clone())).unwrap();
    let fx = m.handle(Event::Completion(ok)).unwrap();
    assert!(
        matches!(
            fx.as_slice(),
            [Effect::Finish(Outcome::Failed { code, .. })] if *code == cairn_protocol::PROTOCOL_VIOLATION
        ),
        "a second notification for the same id slipped through: {fx:?}"
    );
}

/// ## Mandatory test 3 — run/ack order (spec section 8.2)
///
/// Exactly the scenario the spec gives:
///
/// ```text
/// First run:      Sleep(1),  ProposeRunCompletionAck(3),  Sleep(2)
/// Every replay:   Sleep(1),  RunCompletion(3),            Sleep(2)
/// ```
///
/// The ack must take **exactly the position** `RunCompletion` will take on replay.
#[test]
fn run_ack_order_is_preserved_when_the_journal_is_replayed() {
    let mut m = InvocationMachine::resume(INV_ID, StartParams::default(), initial_journal("req"));
    m.handle(Event::Start).unwrap();

    // Three waiting commands: sleep 1, run 3, sleep 2.
    for (at, id) in [(1_000u64, 1u32), (2_000, 2)] {
        m.handle(Event::FromSdk(frame(
            MessageType::SLEEP,
            Flags::NONE,
            &pb::SleepCommandMessage {
                wake_up_time: at,
                result_completion_id: id,
                name: String::new(),
            },
        )))
        .unwrap();
    }
    m.handle(Event::FromSdk(frame(
        MessageType::RUN,
        Flags::NONE,
        &pb::RunCommandMessage {
            result_completion_id: 3,
            name: String::new(),
        },
    )))
    .unwrap();

    // The notification stream of the first run: sleep(1), ack(3), sleep(2).
    let mut first_run = Vec::new();
    let thu = |fx: Vec<Effect>, out: &mut Vec<(MessageType, u32)>| {
        for e in fx {
            if let Effect::Send(msg) = e {
                out.push((msg.message_type(), completion_id_of(&msg)));
            }
        }
    };
    thu(
        m.handle(Event::Completion(Completion {
            id: 1,
            result: CompletionResult::Timer,
        }))
        .unwrap(),
        &mut first_run,
    );
    thu(
        m.handle(Event::FromSdk(frame(
            MessageType::PROPOSE_RUN_COMPLETION,
            Flags::REQUESTED_ACK, // ← asks for an ack, exactly the section 8.2 branch
            &pb::ProposeRunCompletionMessage {
                result_completion_id: 3,
                result: Some(pb::propose_run_completion_message::Result::Value(
                    Bytes::from_static(b"result"),
                )),
            },
        )))
        .unwrap(),
        &mut first_run,
    );
    thu(
        m.handle(Event::Completion(Completion {
            id: 2,
            result: CompletionResult::Timer,
        }))
        .unwrap(),
        &mut first_run,
    );

    assert_eq!(
        first_run,
        vec![
            (MessageType::SLEEP_COMPLETION, 1),
            (MessageType::PROPOSE_RUN_COMPLETION_ACK, 3),
            (MessageType::SLEEP_COMPLETION, 2),
        ],
        "the first run's notification stream is wrong"
    );

    // Replay from the journal just produced.
    let mut m2 = InvocationMachine::resume(
        INV_ID,
        StartParams::default(),
        Journal::from_entries(m.journal().entries().to_vec()),
    );
    let on_replay: Vec<(MessageType, u32)> = m2
        .handle(Event::Start)
        .unwrap()
        .into_iter()
        .filter_map(|e| match e {
            Effect::Send(msg) if msg.kind() == cairn_protocol::MessageKind::Notification => {
                Some((msg.message_type(), completion_id_of(&msg)))
            }
            _ => None,
        })
        .collect();

    assert_eq!(
        on_replay,
        vec![
            (MessageType::SLEEP_COMPLETION, 1),
            (MessageType::RUN_COMPLETION, 3), // ← takes exactly the ack's place
            (MessageType::SLEEP_COMPLETION, 2),
        ],
        "the replay does not reproduce the exact position — spec section 8.2 broken"
    );

    // The completion id sequences must match exactly between the two.
    assert_eq!(
        first_run.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
        on_replay.iter().map(|(_, id)| *id).collect::<Vec<_>>()
    );

    // Spec section 8.2: the ack is sent ONLY in the PROCESSING phase, NEVER on replay.
    assert!(
        !on_replay
            .iter()
            .any(|(ty, _)| *ty == MessageType::PROPOSE_RUN_COMPLETION_ACK),
        "sent an ack in the replay phase"
    );
}

/// Without an ack request the runtime sends the notification directly — the same ordering
/// guarantee.
#[test]
fn without_ack_request_notification_is_sent_directly() {
    let mut m = InvocationMachine::resume(INV_ID, StartParams::default(), initial_journal("req"));
    m.handle(Event::Start).unwrap();
    m.handle(Event::FromSdk(frame(
        MessageType::RUN,
        Flags::NONE,
        &pb::RunCommandMessage {
            result_completion_id: 3,
            name: String::new(),
        },
    )))
    .unwrap();

    let fx = m
        .handle(Event::FromSdk(frame(
            MessageType::PROPOSE_RUN_COMPLETION,
            Flags::NONE, // ← no ack requested
            &pb::ProposeRunCompletionMessage {
                result_completion_id: 3,
                result: Some(pb::propose_run_completion_message::Result::Value(
                    Bytes::from_static(b"x"),
                )),
            },
        )))
        .unwrap();

    let sent: Vec<MessageType> = fx
        .iter()
        .filter_map(|e| match e {
            Effect::Send(m) => Some(m.message_type()),
            _ => None,
        })
        .collect();
    assert_eq!(sent, vec![MessageType::RUN_COMPLETION]);
}

/// ## Mandatory test 4 — journal mismatch
///
/// *"Change the user code between two runs, confirm 570 is reported and there is
/// no infinite retry."*
#[test]
fn journal_mismatch_reports_570_without_retry() {
    let state = state();
    // First run: run until there are a few entries, then die.
    let killed_run = run(sample_program(), initial_journal("req"), &state, Some(3));
    assert!(killed_run.killed);

    // Second run: the user code HAS CHANGED — the second step is now SetState,
    // not Sleep.
    let new_code = vec![
        Step::GetState {
            key: "count",
            id: 1,
        },
        Step::SetState {
            key: "count",
            value: "9",
        },
        Step::Output { value: "other" },
    ];
    let run_result = run(
        new_code,
        Journal::from_entries(killed_run.journal.clone()),
        &state,
        None,
    );

    match run_result.outcome {
        Some(Outcome::Failed { code, action, .. }) => {
            assert_eq!(code, cairn_protocol::JOURNAL_MISMATCH, "must be 570");
            assert!(
                !matches!(action, FailureAction::Retry { .. }),
                "570 yet still retried — a retry only reproduces the same error, an infinite loop"
            );
        }
        other => panic!("journal mismatch not detected: {other:?}"),
    }
}

/// 570 is not retried **whatever** `behavior` the SDK sends (spec section 8.5).
#[test]
fn journal_mismatch_not_retried_even_if_sdk_asks() {
    for behavior in [
        pb::ErrorBehavior::Retry,
        pb::ErrorBehavior::Pause,
        pb::ErrorBehavior::Fail,
    ] {
        let mut m =
            InvocationMachine::resume(INV_ID, StartParams::default(), initial_journal("req"));
        m.handle(Event::Start).unwrap();
        let fx = m
            .handle(Event::FromSdk(frame(
                MessageType::ERROR,
                Flags::NONE,
                &pb::ErrorMessage {
                    code: cairn_protocol::JOURNAL_MISMATCH,
                    message: "other".into(),
                    behavior: behavior as i32,
                    next_retry_delay: Some(10),
                    ..Default::default()
                },
            )))
            .unwrap();
        match fx.as_slice() {
            [Effect::Finish(Outcome::Failed { action, .. })] => assert!(
                !matches!(action, FailureAction::Retry { .. }),
                "behavior={behavior:?} still gave Retry"
            ),
            other => panic!("{other:?}"),
        }
    }
}

/// 570 is always `Pause`, **never** `Fail` — even when the SDK asks for `FAIL`.
/// A journal mismatch is the sign of a bad deploy: it is fixed by deploying the
/// right version and then `resume`; `Fail` would lose the work for good. Old
/// SDKs (sending `ERROR_FAIL` for 570) are still saved thanks to the rule on
/// the runtime side.
#[test]
fn journal_mismatch_always_pauses_regardless_of_behavior() {
    for behavior in [
        pb::ErrorBehavior::Retry,
        pb::ErrorBehavior::Pause,
        pb::ErrorBehavior::Fail,
    ] {
        let mut m =
            InvocationMachine::resume(INV_ID, StartParams::default(), initial_journal("req"));
        m.handle(Event::Start).unwrap();
        let fx = m
            .handle(Event::FromSdk(frame(
                MessageType::ERROR,
                Flags::NONE,
                &pb::ErrorMessage {
                    code: cairn_protocol::JOURNAL_MISMATCH,
                    message: "other".into(),
                    behavior: behavior as i32,
                    ..Default::default()
                },
            )))
            .unwrap();
        match fx.as_slice() {
            [Effect::Finish(Outcome::Failed { action, .. })] => assert!(
                matches!(action, FailureAction::Pause),
                "behavior={behavior:?} gave {action:?}, must be Pause"
            ),
            other => panic!("{other:?}"),
        }
    }
}

/// For ordinary errors `behavior` is respected, including `next_retry_delay` (spec section 8.5).
#[test]
fn ordinary_error_respects_behavior() {
    let mut m = InvocationMachine::resume(INV_ID, StartParams::default(), initial_journal("req"));
    m.handle(Event::Start).unwrap();
    let fx = m
        .handle(Event::FromSdk(frame(
            MessageType::ERROR,
            Flags::NONE,
            &pb::ErrorMessage {
                code: 500,
                message: "boom".into(),
                behavior: pb::ErrorBehavior::Retry as i32,
                next_retry_delay: Some(250),
                ..Default::default()
            },
        )))
        .unwrap();
    assert!(matches!(
        fx.as_slice(),
        [Effect::Finish(Outcome::Failed {
            action: FailureAction::Retry {
                delay_ms: Some(250)
            },
            ..
        })]
    ));
}

// ─────────────────────────────────────────────────────────────── helpers

fn frame<M: prost::Message>(ty: MessageType, flags: Flags, m: &M) -> Frame {
    let wire = encode_message(ty, flags, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

fn completion_id_of(msg: &cairn_protocol::Message) -> u32 {
    use cairn_protocol::Message as M;
    match msg {
        M::GetStateCompletion(n) => n.completion_id,
        M::SleepCompletion(n) => n.completion_id,
        M::CallCompletion(n) => n.completion_id,
        M::CallInvocationIdCompletion(n) => n.completion_id,
        M::RunCompletion(n) => n.completion_id,
        M::ProposeRunCompletionAck(n) => n.completion_id,
        other => panic!("not a notification: {other:?}"),
    }
}

/// An outgoing call **must not be resent** once there is a receipt.
///
/// The caller replays on *every* turn while waiting for the result. If the task
/// sending the call were tied to the *result* completion, every replay would
/// send one more call — and every call is a run of the target service. The
/// right receipt is the **invocation id** completion: once it is in the
/// journal, the call has been sent.
#[test]
fn call_not_resent_after_receipt() {
    let call = |m: &mut InvocationMachine| {
        m.handle(Event::FromSdk(frame(
            MessageType::CALL,
            Flags::NONE,
            &pb::CallCommandMessage {
                service_name: "PaymentService".into(),
                handler_name: "charge".into(),
                invocation_id_notification_idx: 1,
                result_completion_id: 2,
                ..Default::default()
            },
        )))
        .unwrap()
    };

    let mut m = InvocationMachine::resume(INV_ID, StartParams::default(), initial_journal("req"));
    m.handle(Event::Start).unwrap();
    call(&mut m);

    // No receipt yet: the replay MUST send the call.
    let journal_before = Journal::from_entries(m.journal().entries().to_vec());
    let mut m1 = InvocationMachine::resume(INV_ID, StartParams::default(), journal_before);
    assert_eq!(
        m1.handle(Event::Start)
            .unwrap()
            .iter()
            .filter(|e| matches!(e, Effect::Task(cairn_runtime::Task::Invoke { .. })))
            .count(),
        1,
        "the call was not sent yet and the replay did not send it"
    );

    // The receipt arrives (the invocation id), the result not yet.
    m.handle(Event::Completion(Completion {
        id: 1,
        result: CompletionResult::CallInvocationId("callee-1".into()),
    }))
    .unwrap();

    let journal_after = Journal::from_entries(m.journal().entries().to_vec());
    let mut m2 = InvocationMachine::resume(INV_ID, StartParams::default(), journal_after);
    let fx = m2.handle(Event::Start).unwrap();
    assert_eq!(
        fx.iter()
            .filter(|e| matches!(e, Effect::Task(cairn_runtime::Task::Invoke { .. })))
            .count(),
        0,
        "resent the call despite the receipt — the target service will run several times"
    );
    // And the result completion is still pending, as it should be.
    assert_eq!(m2.pending_completions(), vec![2]);
}

/// Spec section 8.1 — a duplicate completion id must **blow up**, not be
/// swallowed.
///
/// This is the other side of `one_completion_id_never_gets_two_notifications`:
/// there the runtime refuses to send a second notification; here it refuses to
/// *promise* an id that already has an owner.
///
/// Silently ignoring it is the worst way to fail. The runtime does not promise
/// that completion so it never sends a notification; the SDK waits forever; the
/// invocation sits in `Suspended` and **there is no error to see**. Section 8.1
/// allows exactly two options — send a notification for every declared id, or
/// end with `Error` — and silence is neither.
#[test]
fn duplicate_completion_id_is_protocol_violation() {
    let sleep = |id: u32| {
        frame(
            MessageType::SLEEP,
            Flags::NONE,
            &pb::SleepCommandMessage {
                wake_up_time: 1_000,
                result_completion_id: id,
                name: String::new(),
            },
        )
    };

    let mut m = InvocationMachine::resume(INV_ID, StartParams::default(), initial_journal("req"));
    m.handle(Event::Start).unwrap();
    m.handle(Event::FromSdk(sleep(7))).unwrap();

    let fx = m.handle(Event::FromSdk(sleep(7))).unwrap();
    assert!(
        matches!(
            fx.as_slice(),
            [Effect::Finish(Outcome::Failed { code, .. })]
                if *code == cairn_protocol::PROTOCOL_VIOLATION
        ),
        "a duplicate id slipped through — the SDK would wait forever for a notification that never \
         comes: {fx:?}"
    );
    assert!(
        !fx.iter().any(|e| matches!(e, Effect::Persist { .. })),
        "the broken command was still journaled — every later replay would trip over exactly it"
    );
}

/// `Call` allocates **two** ids in one command; they must differ too.
///
/// This case is not caught by the comparison with `pending`/`settled`, since at
/// check time neither of the two ids has been recorded yet.
#[test]
fn call_must_not_use_one_id_for_both_completions() {
    let mut m = InvocationMachine::resume(INV_ID, StartParams::default(), initial_journal("req"));
    m.handle(Event::Start).unwrap();

    let fx = m
        .handle(Event::FromSdk(frame(
            MessageType::CALL,
            Flags::NONE,
            &pb::CallCommandMessage {
                service_name: "PaymentService".into(),
                handler_name: "charge".into(),
                invocation_id_notification_idx: 5,
                result_completion_id: 5, // duplicates itself
                ..Default::default()
            },
        )))
        .unwrap();
    assert!(
        matches!(
            fx.as_slice(),
            [Effect::Finish(Outcome::Failed { code, .. })]
                if *code == cairn_protocol::PROTOCOL_VIOLATION
        ),
        "a Call using one id for both the receipt and the result passed: {fx:?}"
    );
}

/// A durable journal that already contains a duplicate id ⇒ a broken journal,
/// stop and say which entry.
///
/// Continuing the replay only reproduces exactly the silent hang the check
/// above exists to prevent, so "handling it gently" here prolongs a known bug.
#[test]
fn journal_with_duplicate_ids_rejected_on_replay() {
    use cairn_runtime::JournalEntry;

    let sleep_entry = |id: u32| {
        let payload = encode_message(
            MessageType::SLEEP,
            Flags::NONE,
            &pb::SleepCommandMessage {
                wake_up_time: 1_000,
                result_completion_id: id,
                name: String::new(),
            },
        )
        .slice(cairn_protocol::HEADER_LEN..);
        JournalEntry::new(MessageType::SLEEP, payload)
    };

    let mut entries = initial_journal("req").entries().to_vec();
    entries.push(sleep_entry(7));
    entries.push(sleep_entry(7));

    let mut m = InvocationMachine::resume(
        INV_ID,
        StartParams::default(),
        Journal::from_entries(entries),
    );
    let err = m.handle(Event::Start).unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains("entry 2"),
        "must name exactly the broken entry: {text}"
    );
    assert!(
        text.contains('7'),
        "must say which id is duplicated: {text}"
    );
}
