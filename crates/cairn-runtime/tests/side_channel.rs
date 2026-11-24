// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Unknown frames and the side channel in the state machine —
//! `protocol-v1.md` section 9.2.
//!
//! The side channel is frames that travel on the connection but **never**
//! become journal entries, wait for no durable ack, and shift no entry index
//! wherever they are inserted. Today there are `Keepalive` (`0x0008`, v3) and
//! the custom range `0xFC00+`; the token streams' `StreamAppend` (v4) will be
//! the third kind. Every other code the machine does not know is a protocol
//! violation (571), **at every wire version** — the wire version only changes
//! what the SDK may *send*, not how the machine treats what it does not
//! understand. The turn layer (`cairn-server/src/turn/sdk.rs`) filters
//! `Keepalive` and `ContinueAsNew` before they get here; this test pins what the
//! machine does if another driver (or a change to the turn layer) lets them
//! through.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    bytes::Bytes,
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, MessageType},
    cairn_runtime::{
        Effect, Event, InvocationMachine, Journal, JournalEntry, Outcome, StartParams,
    },
};

const ID: [u8; 24] = [0x33; 24];

fn frame<M: prost::Message>(ty: MessageType, m: &M) -> Frame {
    let wire = encode_message(ty, Flags::NONE, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

fn raw(ty: u16, payload: &'static [u8]) -> Frame {
    Frame::new(MessageType(ty), Flags::NONE, Bytes::from_static(payload))
}

fn input() -> JournalEntry {
    let payload = encode_message(
        MessageType::INPUT,
        Flags::NONE,
        &pb::InputCommandMessage {
            value: Some(cairn_protocol::value::inline(Bytes::from_static(b"null"))),
            ..Default::default()
        },
    )
    .slice(cairn_protocol::HEADER_LEN..);
    JournalEntry::new(MessageType::INPUT, payload)
}

fn fresh() -> InvocationMachine {
    let mut m = InvocationMachine::resume(
        ID,
        StartParams::default(),
        Journal::from_entries(vec![input()]),
    );
    m.handle(Event::Start).unwrap();
    m
}

fn sleep(id: u32) -> Frame {
    frame(
        MessageType::SLEEP,
        &pb::SleepCommandMessage {
            wake_up_time: 1_000,
            result_completion_id: id,
            name: String::new(),
        },
    )
}

fn persisted(fx: &[Effect]) -> usize {
    fx.iter()
        .filter(|e| matches!(e, Effect::Persist { .. }))
        .count()
}

fn violation(fx: &[Effect]) -> bool {
    matches!(
        fx,
        [Effect::Finish(Outcome::Failed { code, .. })] if *code == cairn_protocol::PROTOCOL_VIOLATION
    )
}

/// Inserts a side-channel frame between two commands: the next command still
/// lands in exactly entry 2 — the journal does not know the frame ever existed.
fn assert_side(f: Frame) -> Vec<Effect> {
    let mut m = fresh();
    m.handle(Event::FromSdk(sleep(1))).unwrap();
    let fx = m.handle(Event::FromSdk(f)).unwrap();
    assert_eq!(
        persisted(&fx),
        0,
        "a side-channel frame became a journal entry: {fx:?}"
    );
    assert!(
        !fx.iter().any(|e| matches!(e, Effect::Finish(_))),
        "a side-channel frame ended the invocation: {fx:?}"
    );
    let next = m.handle(Event::FromSdk(sleep(2))).unwrap();
    assert!(
        matches!(next.first(), Some(Effect::Persist { entry_index: 2, .. })),
        "the command after a side-channel frame has a shifted index: {next:?}"
    );
    assert_eq!(m.journal().len(), 3);
    fx
}

#[test]
fn custom_is_side_channel_forwarding_raw_bytes() {
    let fx = assert_side(raw(0xFC01, b"\x00\xffnot-protobuf"));
    assert!(
        matches!(
            fx.as_slice(),
            [Effect::ForwardCustom { ty, payload }]
                if ty.raw() == 0xFC01 && payload.as_ref() == b"\x00\xffnot-protobuf"
        ),
        "{fx:?}"
    );
}

#[test]
fn keepalive_is_side_channel_outside_journal() {
    let fx = assert_side(frame(MessageType::KEEPALIVE, &pb::KeepaliveMessage {}));
    assert!(
        fx.is_empty(),
        "Keepalive has nothing to do in the machine: {fx:?}"
    );
}

/// Control frames only the turn layer understands (`ReplayReport` of a trial
/// replay turn, `ContinueAsNew`): reaching the machine means the driver misused
/// them, and recording them as entries would make every later replay send a
/// control frame to the SDK as if it were the SDK's own command.
#[test]
fn turn_layer_frame_reaching_machine_is_violation() {
    for f in [
        frame(
            MessageType::REPLAY_REPORT,
            &pb::ReplayReportMessage {
                matched: 1,
                ..Default::default()
            },
        ),
        frame(
            MessageType::CONTINUE_AS_NEW,
            &pb::ContinueAsNewMessage {
                input: Bytes::from_static(b"1"),
            },
        ),
    ] {
        let mut m = fresh();
        let fx = m.handle(Event::FromSdk(f.clone())).unwrap();
        assert!(violation(&fx), "{}: {fx:?}", f.ty);
        assert_eq!(m.journal().len(), 1, "{} became a journal entry", f.ty);
    }
}

/// Unknown codes — outside every range, in a command range not allocated yet,
/// or a reserved code — are 571 and record nothing. The wire version does not
/// change this.
#[test]
fn unknown_and_reserved_codes_are_violations_not_journaled() {
    for ty in [0x0300u16, 0x0420, 0x04FF, 0x000F, 0x0006, 0x040F, 0x0414] {
        let mut m = fresh();
        let fx = m.handle(Event::FromSdk(raw(ty, b""))).unwrap();
        assert!(violation(&fx), "0x{ty:04X}: {fx:?}");
        assert_eq!(m.journal().len(), 1, "0x{ty:04X} became a journal entry");
    }
}

/// Notifications go only runtime → SDK; an SDK sending one is a violation
/// (including notification codes not allocated yet).
#[test]
fn sdk_sending_notification_is_violation() {
    for ty in [0x800Cu16, 0x8099] {
        let mut m = fresh();
        let fx = m.handle(Event::FromSdk(raw(ty, b""))).unwrap();
        assert!(violation(&fx), "0x{ty:04X}: {fx:?}");
        assert_eq!(m.journal().len(), 1);
    }
}
