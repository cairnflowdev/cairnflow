// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The cost of one journal replay by the number of calls waiting.
//!
//! The engine replays the caller's whole journal at **every** completion
//! delivery (`turn::dispatch::settle_completion`: `engine.load` +
//! `Event::Start`). A parent fanning out `n` children receives `2n` completions
//! (the invocation id, then the result), so the cost of one replay times `2n` is
//! the CPU of the parent range's engine thread. A `fanout` of 1 000 children was
//! measured at 508 s, superlinear; the profile pointed at
//! `InvocationMachine::replay_task_for`, a function rescanning **the whole
//! journal** for **every** pending completion — i.e. O(P × J) per replay.
//!
//! The test counts decoded entries (`replay_decodes`), it does not measure time.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    bytes::Bytes,
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, Message, MessageType},
    cairn_runtime::{
        Completion, CompletionResult, Effect, Event, InvocationMachine, Journal, JournalEntry,
        StartParams, Task,
    },
};

const ID: [u8; 24] = [0x3f; 24];

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

/// The journal of a parent that has issued `n` `ctx.call`s and received the
/// invocation id of the first `delivered` children (none has returned a result
/// yet) — exactly the shape of a `fanout` journal midway.
fn fanout_journal(n: u32, delivered: u32) -> Journal {
    let mut m = InvocationMachine::resume(ID, StartParams::default(), input_journal());
    m.handle(Event::Start).unwrap();
    for i in 0..n {
        m.handle(Event::FromSdk(frame(
            MessageType::CALL,
            &pb::CallCommandMessage {
                service_name: "InterService".into(),
                handler_name: "leaf".into(),
                parameter: Bytes::from(format!("{{\"tag\":\"fanout/0.{i}\"}}")),
                invocation_id_notification_idx: 2 * i + 1,
                result_completion_id: 2 * i + 2,
                ..Default::default()
            },
        )))
        .unwrap();
    }
    for i in 0..delivered {
        m.handle(Event::Completion(Completion {
            id: 2 * i + 1,
            result: CompletionResult::CallInvocationId(format!("callee-{i}")),
        }))
        .unwrap();
    }
    Journal::from_entries(m.journal().entries().to_vec())
}

/// One replay decodes **each entry once**, plus at most once per pending
/// completion — it does not rescan the whole journal per completion.
///
/// Before the fix, `replay_task_for` scanned the journal from the start for each
/// pending completion; a `Call`'s result completion matched no branch so the
/// scan ran to the end. With `n` = 1 000 and half the ids delivered: J = 1 501
/// entries, 1 500 pending completions ⇒ 1 878 251 decodes for **one** completion
/// delivered.
///
/// Must see: `replay_decodes ≤ J + P` for every `n`, and the replay tasks exactly
/// as before (one `Invoke` per call without an id, none for calls with an id).
#[test]
fn replay_decodes_each_entry_once_regardless_of_pending_calls() {
    let mut over = Vec::new();
    for n in [10u32, 100, 1_000] {
        let delivered = n / 2;
        let journal = fanout_journal(n, delivered);
        let entries = u64::from(journal.len());
        let mut m = InvocationMachine::resume(ID, StartParams::default(), journal);
        let fx = m.handle(Event::Start).unwrap();

        let pending = m.pending_completions().len() as u64;
        assert_eq!(pending, u64::from(2 * n - delivered), "n = {n}");
        let invokes: Vec<u32> = fx
            .iter()
            .filter_map(|e| match e {
                Effect::Task(Task::Invoke {
                    invocation_id_completion_id,
                    ..
                }) => Some(*invocation_id_completion_id),
                _ => None,
            })
            .collect();
        let want: Vec<u32> = (delivered..n).map(|i| 2 * i + 1).collect();
        assert_eq!(
            invokes, want,
            "n = {n}: the replay tasks must be exactly as before"
        );

        let decodes = m.replay_decodes();
        if decodes > entries + pending {
            over.push(format!(
                "n = {n}: decoded {decodes} entries for a journal of {entries} entries and \
                 {pending} pending completions (cap {})",
                entries + pending
            ));
        }
    }
    assert!(
        over.is_empty(),
        "one replay rescanned the journal for each pending completion:\n{}",
        over.join("\n")
    );
}
