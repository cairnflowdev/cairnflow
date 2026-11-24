// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **A `Value` from the SDK is always inline before wire v6.**
//!
//! An SDK ≤ v5 has no valid way to send `Value.segments`: an SDK that sends
//! them points at a segment the runtime never checked (sha, holder). The
//! machine refuses in `on_sdk`, before the command becomes a journal entry.
//!
//! | Case | Must see |
//! |---|---|
//! | `SetState` carrying `segments` | 571, no entry, no `WriteState` |
//! | `Output` carrying `segments` | 571, no entry |
//! | inline `Output` with a matching `len` | runs as before |

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

#[allow(
    dead_code,
    reason = "only uses initial_journal and INV_ID of the shared harness"
)]
mod harness;

use {
    bytes::Bytes,
    cairn_protocol::{
        encode_message, pb,
        value::{inline, Logical, SegmentRef},
        Flags, Frame, FrameDecoder, MessageType,
    },
    cairn_runtime::{Effect, Event, InvocationMachine, Outcome, StartParams, Task},
    harness::{initial_journal, INV_ID},
};

fn frame<M: prost::Message>(ty: MessageType, m: &M) -> Frame {
    let wire = encode_message(ty, Flags::NONE, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

fn segmented() -> pb::Value {
    Logical::Segmented {
        refs: vec![SegmentRef {
            sha256: [7; 32],
            len: 100,
            gen: 1,
        }],
        tail: Bytes::from_static(b"tail"),
        len: 104,
    }
    .encode()
}

fn violated(fx: &[Effect]) -> bool {
    matches!(
        fx,
        [Effect::Finish(Outcome::Failed { code, .. })] if *code == cairn_protocol::PROTOCOL_VIOLATION
    )
}

fn started() -> InvocationMachine {
    let mut m = InvocationMachine::resume(INV_ID, StartParams::default(), initial_journal("req"));
    m.handle(Event::Start).unwrap();
    m
}

#[test]
fn set_state_with_segments_is_a_protocol_violation() {
    let mut m = started();
    let before = m.journal().len();
    let fx = m
        .handle(Event::FromSdk(frame(
            MessageType::SET_STATE,
            &pb::SetStateCommandMessage {
                key: Bytes::from_static(b"messages"),
                value: Some(segmented()),
                name: String::new(),
            },
        )))
        .unwrap();
    assert!(
        violated(&fx),
        "SetState carrying segments slipped through: {fx:?}"
    );
    assert!(
        !fx.iter()
            .any(|e| matches!(e, Effect::Task(Task::WriteState { .. }))),
        "WriteState issued for an unchecked value: {fx:?}"
    );
    assert_eq!(
        m.journal().len(),
        before,
        "the command became a journal entry"
    );
}

#[test]
fn output_with_segments_is_a_protocol_violation() {
    let mut m = started();
    let before = m.journal().len();
    let fx = m
        .handle(Event::FromSdk(frame(
            MessageType::OUTPUT,
            &pb::OutputCommandMessage {
                result: Some(pb::output_command_message::Result::Value(segmented())),
                ..Default::default()
            },
        )))
        .unwrap();
    assert!(
        violated(&fx),
        "Output carrying segments slipped through: {fx:?}"
    );
    assert_eq!(
        m.journal().len(),
        before,
        "the command became a journal entry"
    );
}

#[test]
fn inline_output_with_matching_len_still_runs() {
    let mut m = started();
    let mut v = inline(Bytes::from_static(b"\"ok\""));
    v.len = 4;
    let fx = m
        .handle(Event::FromSdk(frame(
            MessageType::OUTPUT,
            &pb::OutputCommandMessage {
                result: Some(pb::output_command_message::Result::Value(v)),
                ..Default::default()
            },
        )))
        .unwrap();
    assert!(!violated(&fx), "a valid inline Output was refused: {fx:?}");
}
