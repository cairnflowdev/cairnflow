// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Codec tests. Each test points at one sentence of `docs/protocol-v1.md`.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    bytes::{Bytes, BytesMut},
    cairn_protocol::{
        decode, encode_frame, encode_message, pb, Flags, Frame, FrameDecoder, Message, MessageKind,
        MessageType, ProtocolError,
    },
};

fn roundtrip(msg: Message, flags: Flags) -> Message {
    let wire = msg.encode(flags);
    let mut buf = BytesMut::from(&wire[..]);
    let frame = FrameDecoder::new()
        .decode(&mut buf)
        .expect("a valid frame")
        .expect("enough bytes");
    assert!(buf.is_empty(), "bytes left over after splitting the frame");
    assert_eq!(frame.ty, msg.message_type());
    decode(&frame).expect("decodable")
}

/// **The dispatch table must have no holes.**
///
/// Sweeps the whole `u16` range: every code that `is_known()` accepts as v1
/// MUST be handled by `decode()`, and must return exactly that kind. A missing
/// `match` arm is a bug that only shows when a real message passes through —
/// that is, in production.
#[test]
fn every_known_message_type_has_dispatch_arm() {
    let mut checked = 0;
    for raw in 0u32..=u16::MAX as u32 {
        let ty = MessageType(raw as u16);
        if !ty.is_known() {
            continue;
        }
        // proto3: every field has a default, so an empty payload is always valid.
        let frame = Frame::new(ty, Flags::NONE, Bytes::new());
        let msg = decode(&frame).unwrap_or_else(|e| panic!("{ty} has no dispatch arm: {e}"));
        assert_eq!(msg.message_type(), ty, "{ty} dispatched to the wrong kind");
        checked += 1;
    }
    // 23 of v1 + 15 opened v2 codes (eager state, state keys, promise, attach)
    // + 3 of wire v3 (`Keepalive`, `ReplayReport`, `ContinueAsNew`)
    // + 2 of wire v4 (`StreamAppend`, `ArtifactPut`)
    // + 1 of wire v6 (`SegmentUrls`).
    assert_eq!(
        checked, 44,
        "the number of message types changed — update spec section 5, then this test"
    );
}

#[test]
fn roundtrip_preserves_content() {
    let m = roundtrip(
        Message::Call(pb::CallCommandMessage {
            service_name: "OrderService".into(),
            handler_name: "submit".into(),
            parameter: Bytes::from_static(b"{\"id\":1}"),
            headers: vec![pb::Header {
                key: "x-trace".into(),
                value: "abc".into(),
            }],
            key: "order-42".into(),
            idempotency_key: Some("idem-1".into()),
            scope: Some("acme".into()),
            limit_key: Some("growth/alice".into()),
            invocation_id_notification_idx: 3,
            result_completion_id: 4,
            name: "call payment".into(),
            placement: 0,
            tags: vec![pb::Header {
                key: "team".into(),
                value: "pay".into(),
            }],
        }),
        Flags::REQUESTED_ACK,
    );
    let Message::Call(c) = m else {
        panic!("wrong kind")
    };
    assert_eq!(c.service_name, "OrderService");
    assert_eq!(c.idempotency_key.as_deref(), Some("idem-1"));
    assert_eq!(c.invocation_id_notification_idx, 3);
    assert_eq!(c.result_completion_id, 4);
}

/// Spec section 5: the runtime forwards the `0xFC00+` range **as raw bytes, without parsing**.
#[test]
fn custom_is_not_parsed() {
    let junk = Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]); // not valid protobuf
    let frame = Frame::new(MessageType(0xFC42), Flags::NONE, junk.clone());
    match decode(&frame).expect("custom must not fail") {
        Message::Custom { ty, payload } => {
            assert_eq!(ty, MessageType(0xFC42));
            assert_eq!(payload, junk);
        }
        other => panic!("custom was parsed as {other:?}"),
    }
}

/// A `reserved` code (kept for v2, spec section 9) is distinguishable from an
/// unknown code. Both are errors, but the causes differ so the diagnosis must
/// differ.
#[test]
fn reserved_and_unknown_are_distinct_errors() {
    let reserved = Frame::new(MessageType(0x040F), Flags::NONE, Bytes::new());
    assert!(matches!(
        decode(&reserved),
        Err(ProtocolError::ReservedMessageType { .. })
    ));

    let unknown = Frame::new(MessageType(0x0299), Flags::NONE, Bytes::new());
    assert!(matches!(
        decode(&unknown),
        Err(ProtocolError::UnknownMessageType { .. })
    ));

    // Both report PROTOCOL_VIOLATION outward.
    assert_eq!(
        decode(&reserved).unwrap_err().code(),
        cairn_protocol::PROTOCOL_VIOLATION
    );
}

/// Spec section 7: `ErrorBehavior` value 0 is **deliberate** — an unset field must be read as
/// RETRY.
#[test]
fn unset_error_behavior_means_retry() {
    let frame_bytes = encode_message(
        MessageType::ERROR,
        Flags::NONE,
        &pb::ErrorMessage {
            code: 500,
            message: "boom".into(),
            ..Default::default()
        },
    );
    let mut buf = BytesMut::from(&frame_bytes[..]);
    let frame = FrameDecoder::new().decode(&mut buf).unwrap().unwrap();
    let Message::Error(e) = decode(&frame).unwrap() else {
        panic!("wrong kind")
    };
    assert_eq!(e.behavior(), pb::ErrorBehavior::Retry);
}

/// Spec section 7: for `GetStateCompletion`, `Void` = the key **does not
/// exist**, while an empty `Value` = the key exists with an empty value.
/// Merging the two is a classic bug: the user sets state to an empty string
/// and reads back "not set".
#[test]
fn empty_value_differs_from_void() {
    use pb::get_state_completion_notification_message::Result as R;

    let absent = roundtrip(
        Message::GetStateCompletion(pb::GetStateCompletionNotificationMessage {
            completion_id: 1,
            result: Some(R::Void(pb::Void {})),
        }),
        Flags::NONE,
    );
    let present_empty = roundtrip(
        Message::GetStateCompletion(pb::GetStateCompletionNotificationMessage {
            completion_id: 1,
            result: Some(R::Value(cairn_protocol::value::inline(Bytes::new()))),
        }),
        Flags::NONE,
    );
    assert_ne!(
        absent, present_empty,
        "\"does not exist\" and \"empty value\" encode the same"
    );

    let Message::GetStateCompletion(a) = absent else {
        panic!()
    };
    assert!(matches!(a.result, Some(R::Void(_))));
}

/// Spec section 8.1: notifications MAY arrive out of order; they are matched by
/// **id**, not by position. The codec must keep the ids across a shuffled byte
/// stream.
#[test]
fn out_of_order_notifications_keep_their_ids() {
    let mut wire = BytesMut::new();
    for id in [7u32, 2, 5] {
        wire.extend_from_slice(&encode_message(
            MessageType::SLEEP_COMPLETION,
            Flags::NONE,
            &pb::SleepCompletionNotificationMessage {
                completion_id: id,
                void: Some(pb::Void {}),
            },
        ));
    }

    let dec = FrameDecoder::new();
    let mut ids = Vec::new();
    while let Some(f) = dec.decode(&mut wire).unwrap() {
        let Message::SleepCompletion(n) = decode(&f).unwrap() else {
            panic!("wrong kind")
        };
        ids.push(n.completion_id);
    }
    assert_eq!(
        ids,
        vec![7, 2, 5],
        "the codec reordered notifications — not allowed"
    );
}

/// Spec section 8.2 — the subtlest constraint of the whole protocol, expressed
/// at the encoding level.
///
/// The codec **cannot** enforce the order (that is the runtime's job). What it
/// must guarantee: `ProposeRunCompletionAck` and `RunCompletion` are **two
/// different codes** carrying **the same completion id**, so the runtime can
/// substitute one for the other at the right position on replay. This test
/// locks that contract down in runnable form.
#[test]
fn run_ack_and_completion_share_id_not_type() {
    const RUN_ID: u32 = 3;

    // The first run (PROCESSING): sleep(1), ack(3), sleep(2).
    let first_run = [
        encode_message(
            MessageType::SLEEP_COMPLETION,
            Flags::NONE,
            &pb::SleepCompletionNotificationMessage {
                completion_id: 1,
                void: Some(pb::Void {}),
            },
        ),
        encode_message(
            MessageType::PROPOSE_RUN_COMPLETION_ACK,
            Flags::NONE,
            &pb::ProposeRunCompletionAckMessage {
                completion_id: RUN_ID,
            },
        ),
        encode_message(
            MessageType::SLEEP_COMPLETION,
            Flags::NONE,
            &pb::SleepCompletionNotificationMessage {
                completion_id: 2,
                void: Some(pb::Void {}),
            },
        ),
    ];

    // Replay: at exactly that position, the ack is replaced by RunCompletion — same id 3.
    let on_replay = [
        first_run[0].clone(),
        encode_message(
            MessageType::RUN_COMPLETION,
            Flags::NONE,
            &pb::RunCompletionNotificationMessage {
                completion_id: RUN_ID,
                result: Some(pb::run_completion_notification_message::Result::Value(
                    cairn_protocol::value::inline(Bytes::from_static(b"result")),
                )),
            },
        ),
        first_run[2].clone(),
    ];

    let doc = |frames: &[bytes::Bytes]| -> Vec<(MessageType, u32)> {
        let dec = FrameDecoder::new();
        let mut buf = BytesMut::new();
        for f in frames {
            buf.extend_from_slice(f);
        }
        let mut out = Vec::new();
        while let Some(f) = dec.decode(&mut buf).unwrap() {
            let id = match decode(&f).unwrap() {
                Message::SleepCompletion(n) => n.completion_id,
                Message::ProposeRunCompletionAck(n) => n.completion_id,
                Message::RunCompletion(n) => n.completion_id,
                other => panic!("unexpected: {other:?}"),
            };
            out.push((f.ty, id));
        }
        out
    };

    let a = doc(&first_run);
    let b = doc(&on_replay);

    // The id sequences must be identical — that is "the same position in the notification stream".
    assert_eq!(
        a.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
        b.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
        "replay changed the order of completion ids — spec section 8.2 broken"
    );
    // But exactly one position changes code, and it is the run's position.
    assert_eq!(a[1].0, MessageType::PROPOSE_RUN_COMPLETION_ACK);
    assert_eq!(b[1].0, MessageType::RUN_COMPLETION);
    assert_eq!(a[0].0, b[0].0);
    assert_eq!(a[2].0, b[2].0);
}

/// Spec section 4: `REQUESTED_ACK` on `ProposeRunCompletion` is valid; on
/// `ProposeRunCompletionAck` it is not (that is the other direction).
#[test]
fn ack_valid_only_in_its_direction() {
    let ok = encode_frame(
        MessageType::PROPOSE_RUN_COMPLETION,
        Flags::REQUESTED_ACK,
        b"",
    );
    assert!(FrameDecoder::new()
        .decode(&mut BytesMut::from(&ok[..]))
        .is_ok());

    let sai = encode_frame(
        MessageType::PROPOSE_RUN_COMPLETION_ACK,
        Flags::REQUESTED_ACK,
        b"",
    );
    assert!(matches!(
        FrameDecoder::new().decode(&mut BytesMut::from(&sai[..])),
        Err(ProtocolError::AckNotAllowed { .. })
    ));
}

/// The group of every known code must match the spec section 5 table.
#[test]
fn grouping_matches_spec_table() {
    for (ty, kind) in [
        (MessageType::START, MessageKind::Control),
        (
            MessageType::PROPOSE_RUN_COMPLETION_ACK,
            MessageKind::Control,
        ),
        (MessageType::INPUT, MessageKind::Command),
        (MessageType::RUN, MessageKind::Command),
        (MessageType::GET_STATE_COMPLETION, MessageKind::Notification),
        (MessageType::SIGNAL, MessageKind::Notification),
    ] {
        assert_eq!(ty.kind(), kind, "{ty} in the wrong group");
    }
}

/// **Wire v4:** the two new side-channel codes in the control range —
/// `StreamAppend` `0x000B`, `ArtifactPut` `0x000C` — are **known** codes, not
/// reserved, not completable.
#[test]
fn wire_v4_side_channel_codes_are_known_control_frames() {
    for (code, name) in [(0x000B, "StreamAppend"), (0x000C, "ArtifactPut")] {
        let ty = MessageType(code);
        assert_eq!(ty.name(), Some(name));
        assert_eq!(ty.kind(), MessageKind::Control);
        assert!(ty.is_known() && !ty.is_reserved() && !ty.is_completable());
    }
}
