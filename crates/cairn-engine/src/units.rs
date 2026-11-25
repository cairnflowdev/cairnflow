// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Billing units by entry kind.**
//!
//! Counting rules (the counter and the "count only once durable" rule live in
//! [`cairn_store::units`]):
//!
//! - Every **durable command** — a journal entry proposed by the SDK or written
//!   by the engine — counts `max(1, ⌈payload / 64 KiB⌉)` units
//!   ([`cairn_store::units::of_payload`]), **once**, at the commit that writes
//!   the entry ([`Engine::step`](crate::Engine::step), `Effect::Persist`).
//!   Replays and retry attempts do not rewrite an existing entry, so they do
//!   not count again.
//! - Every invocation adds 2: 1 at the creating commit (new status,
//!   `stage_status`), 1 at the commit that enters an ending status. The
//!   invocation's result counts as the payload of the ending: the part of
//!   `Output` beyond 64 KiB is added at the `Output` entry.
//! - Every **new** artifact row `(id, sha)`: `of_payload(size)`.
//!
//! The payload of each kind ([`message_units`]) — an **exhaustive** `match` on
//! [`Message`], so a new command kind does not compile until it is classified
//! here.

use {
    cairn_protocol::{pb, Flags, Frame, Message},
    cairn_runtime::JournalEntry,
    cairn_store::units::{of_payload, surplus},
};

/// Units of a journal entry just written. An entry that does not decode
/// (unreachable: the machine builds the entries) counts by its raw size.
pub fn entry_units(entry: &JournalEntry) -> u64 {
    entry_units_with(entry, 0)
}

/// Like [`entry_units`], plus `new_segment_bytes` — the bytes of segments just
/// sealed **from the customer's bytes** for this entry: units follow new bytes
/// = tail + new segments, not logical bytes. Only added to kinds that count by
/// the value's payload (`SetState`, `Output`, `RunCompletion`,
/// `CompletePromise`, signal); other kinds ignore it.
pub fn entry_units_with(entry: &JournalEntry, new_segment_bytes: usize) -> u64 {
    let frame = Frame::new(entry.ty, Flags::NONE, entry.payload.clone());
    match cairn_protocol::decode(&frame) {
        Ok(m) => message_units_with(&m, new_segment_bytes),
        Err(_) => of_payload(entry.payload.len()),
    }
}

fn value_len(v: &Option<pb::Value>) -> usize {
    v.as_ref().map_or(0, carried_len)
}

/// The bytes a `Value` carries in the entry: the whole value when inline, the
/// tail when segmented (units follow new bytes — a sealed segment counts when
/// it is sealed). A broken layout (unreachable: the machine checked it on
/// receipt) counts by its encoded size.
fn carried_len(v: &pb::Value) -> usize {
    use cairn_protocol::value::{encoded_len, Logical};
    Logical::of(Some(v)).map_or(encoded_len(v), |l| l.carried().len())
}

fn failure_len(f: &pb::Failure) -> usize {
    f.message.len()
}

/// Units of a message **when it is a journal entry**. Messages that are never
/// entries (control, side channel) count 0.
pub fn message_units(m: &Message) -> u64 {
    message_units_with(m, 0)
}

/// Like [`message_units`], with `new` new segment bytes added to the value's
/// payload (see [`entry_units_with`]).
pub fn message_units_with(m: &Message, new: usize) -> u64 {
    use pb::{
        complete_promise_command_message::Completion as Cp, output_command_message::Result as Out,
        run_completion_notification_message::Result as Run,
        send_signal_command_message::Result as Send, signal_notification_message::Result as Sig,
    };
    match m {
        // ── Never an entry: turn control, the v4 side channel (streams go into
        // the streams table; artifacts count at the `Artifacts` row), the
        // `0xFC00+` extensions.
        Message::Start(_)
        | Message::Suspension(_)
        | Message::Error(_)
        | Message::End(_)
        | Message::CommandAck(_)
        | Message::ProposeRunCompletion(_)
        | Message::ProposeRunCompletionAck(_)
        | Message::Keepalive(_)
        | Message::ReplayReport(_)
        | Message::ContinueAsNew(_)
        | Message::StreamAppend(_)
        | Message::ArtifactPut(_)
        | Message::SegmentUrls(_)
        | Message::Custom { .. } => 0,

        // ── Invocation lifecycle: 1 at creation and 1 at the end, at the
        // status write (`stage_status`) — every creating/ending path, including
        // paths without an entry (`cancel`, `kill`). `Output` carries only the
        // excess.
        Message::Input(_) => 0,
        Message::Output(o) => surplus(match &o.result {
            Some(Out::Value(v)) => carried_len(v) + new,
            Some(Out::Failure(f)) => failure_len(f),
            None => 0,
        }),

        // ── State. A `get` served from eager state (`GetEagerState*`) does not
        // count — a deliberate choice; a lazy `get` is one unit.
        Message::GetEagerState(_) | Message::GetEagerStateKeys(_) => 0,
        Message::GetState(_) | Message::GetStateKeys(_) => 1,
        Message::SetState(s) => of_payload(value_len(&s.value) + new),
        Message::ClearState(_) | Message::ClearAllState(_) => 1,

        // ── Promise (workflow): payload = the resolved value.
        Message::GetPromise(_) | Message::PeekPromise(_) => 1,
        Message::CompletePromise(c) => of_payload(match &c.completion {
            Some(Cp::CompletionValue(v)) => carried_len(v) + new,
            Some(Cp::CompletionFailure(f)) => failure_len(f),
            None => 0,
        }),

        // ── Time and calls: payload = the parameters.
        Message::Sleep(_) => 1,
        Message::Call(c) => of_payload(c.parameter.len()),
        Message::OneWayCall(c) => of_payload(c.parameter.len()),
        Message::AttachInvocation(_) | Message::GetInvocationOutput(_) => 1,

        // ── Signal/awakeable: payload = the resolved value, on the sending
        // side (`SendSignal`) and on the receiving side (the `Signal`
        // notification — including signals from outside, CANCEL): resolve and
        // await each count as one action, one on each side.
        Message::SendSignal(s) => of_payload(match &s.result {
            Some(Send::Value(v)) => carried_len(v) + new,
            Some(Send::Failure(f)) => failure_len(f),
            Some(Send::Void(_)) | None => 0,
        }),
        Message::Signal(s) => of_payload(match &s.result {
            Some(Sig::Value(v)) => carried_len(v),
            Some(Sig::Failure(f)) => failure_len(f),
            Some(Sig::Void(_)) | None => 0,
        }),

        // ── `ctx.run`: the `Run` entry carries nothing; the result is durable
        // in `RunCompletion` — exactly once per `ctx.run`, even when the
        // closure retries many times (a transient failure writes no
        // completion).
        Message::Run(_) => 0,
        Message::RunCompletion(r) => of_payload(match &r.result {
            Some(Run::Value(v)) => carried_len(v) + new,
            Some(Run::Failure(f)) => failure_len(f),
            None => 0,
        }),

        // ── Completions of commands counted above (a `call` result counts at
        // the callee's end): not counted again.
        Message::GetStateCompletion(_)
        | Message::GetStateKeysCompletion(_)
        | Message::GetPromiseCompletion(_)
        | Message::PeekPromiseCompletion(_)
        | Message::CompletePromiseCompletion(_)
        | Message::SleepCompletion(_)
        | Message::CallCompletion(_)
        | Message::CallInvocationIdCompletion(_)
        | Message::AttachInvocationCompletion(_)
        | Message::GetInvocationOutputCompletion(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use {super::*, bytes::Bytes};

    const K64: usize = 64 * 1024;

    fn val(n: usize) -> pb::Value {
        cairn_protocol::value::inline(Bytes::from(vec![b'x'; n]))
    }

    /// **One row per message kind**, payload 0 / 64 KiB / 64 KiB + 1 / 1 MiB
    /// for kinds carrying a payload. A new `Message` row must go here (the
    /// match above is exhaustive — adding a variant makes `message_units` fail
    /// to compile).
    #[test]
    fn every_message_kind_has_its_units() {
        use pb::*;
        let sizes = [(0usize, 1u64), (K64, 1), (K64 + 1, 2), (1024 * 1024, 16)];
        for (n, want) in sizes {
            let weighted: Vec<(&str, Message)> = vec![
                (
                    "SetState",
                    Message::SetState(SetStateCommandMessage {
                        value: Some(val(n)),
                        ..Default::default()
                    }),
                ),
                (
                    "CompletePromise",
                    Message::CompletePromise(CompletePromiseCommandMessage {
                        completion: Some(
                            complete_promise_command_message::Completion::CompletionValue(val(n)),
                        ),
                        ..Default::default()
                    }),
                ),
                (
                    "Call",
                    Message::Call(CallCommandMessage {
                        parameter: val(n).content,
                        ..Default::default()
                    }),
                ),
                (
                    "OneWayCall",
                    Message::OneWayCall(OneWayCallCommandMessage {
                        parameter: val(n).content,
                        ..Default::default()
                    }),
                ),
                (
                    "SendSignal",
                    Message::SendSignal(SendSignalCommandMessage {
                        result: Some(send_signal_command_message::Result::Value(val(n))),
                        ..Default::default()
                    }),
                ),
                (
                    "Signal",
                    Message::Signal(SignalNotificationMessage {
                        result: Some(signal_notification_message::Result::Value(val(n))),
                        ..Default::default()
                    }),
                ),
                (
                    "RunCompletion",
                    Message::RunCompletion(RunCompletionNotificationMessage {
                        result: Some(run_completion_notification_message::Result::Value(val(n))),
                        ..Default::default()
                    }),
                ),
            ];
            for (name, m) in &weighted {
                assert_eq!(message_units(m), want, "{name} {n} B");
                // Through the journal path: encode as an entry, then classify again.
                let wire = m.encode(Flags::NONE);
                let entry =
                    JournalEntry::new(m.message_type(), wire.slice(cairn_protocol::HEADER_LEN..));
                assert_eq!(entry_units(&entry), want, "entry {name} {n} B");
            }
            let out = Message::Output(OutputCommandMessage {
                result: Some(output_command_message::Result::Value(val(n))),
                ..Default::default()
            });
            assert_eq!(message_units(&out), want - 1, "Output {n} B: the excess");
        }
        let one: Vec<(&str, Message)> = vec![
            ("GetState", Message::GetState(Default::default())),
            ("GetStateKeys", Message::GetStateKeys(Default::default())),
            ("ClearState", Message::ClearState(Default::default())),
            ("ClearAllState", Message::ClearAllState(Default::default())),
            ("GetPromise", Message::GetPromise(Default::default())),
            ("PeekPromise", Message::PeekPromise(Default::default())),
            ("Sleep", Message::Sleep(Default::default())),
            (
                "AttachInvocation",
                Message::AttachInvocation(Default::default()),
            ),
            (
                "GetInvocationOutput",
                Message::GetInvocationOutput(Default::default()),
            ),
            ("SendSignal void", Message::SendSignal(Default::default())),
            ("Signal void", Message::Signal(Default::default())),
            (
                "empty RunCompletion",
                Message::RunCompletion(Default::default()),
            ),
            (
                "empty CompletePromise",
                Message::CompletePromise(Default::default()),
            ),
        ];
        for (name, m) in &one {
            assert_eq!(message_units(m), 1, "{name}");
        }
        let zero: Vec<(&str, Message)> = vec![
            ("Input", Message::Input(Default::default())),
            ("empty Output", Message::Output(Default::default())),
            ("Run", Message::Run(Default::default())),
            ("GetEagerState", Message::GetEagerState(Default::default())),
            (
                "GetEagerStateKeys",
                Message::GetEagerStateKeys(Default::default()),
            ),
            (
                "GetStateCompletion",
                Message::GetStateCompletion(Default::default()),
            ),
            (
                "GetStateKeysCompletion",
                Message::GetStateKeysCompletion(Default::default()),
            ),
            (
                "GetPromiseCompletion",
                Message::GetPromiseCompletion(Default::default()),
            ),
            (
                "PeekPromiseCompletion",
                Message::PeekPromiseCompletion(Default::default()),
            ),
            (
                "CompletePromiseCompletion",
                Message::CompletePromiseCompletion(Default::default()),
            ),
            (
                "SleepCompletion",
                Message::SleepCompletion(Default::default()),
            ),
            (
                "CallCompletion",
                Message::CallCompletion(Default::default()),
            ),
            (
                "CallInvocationIdCompletion",
                Message::CallInvocationIdCompletion(Default::default()),
            ),
            (
                "AttachInvocationCompletion",
                Message::AttachInvocationCompletion(Default::default()),
            ),
            (
                "GetInvocationOutputCompletion",
                Message::GetInvocationOutputCompletion(Default::default()),
            ),
            ("Start", Message::Start(Default::default())),
            ("Suspension", Message::Suspension(Default::default())),
            ("Error", Message::Error(Default::default())),
            ("End", Message::End(Default::default())),
            ("CommandAck", Message::CommandAck(Default::default())),
            (
                "ProposeRunCompletion",
                Message::ProposeRunCompletion(Default::default()),
            ),
            (
                "ProposeRunCompletionAck",
                Message::ProposeRunCompletionAck(Default::default()),
            ),
            ("Keepalive", Message::Keepalive(Default::default())),
            ("ReplayReport", Message::ReplayReport(Default::default())),
            ("ContinueAsNew", Message::ContinueAsNew(Default::default())),
            ("StreamAppend", Message::StreamAppend(Default::default())),
            ("ArtifactPut", Message::ArtifactPut(Default::default())),
            ("SegmentUrls", Message::SegmentUrls(Default::default())),
        ];
        for (name, m) in &zero {
            assert_eq!(message_units(m), 0, "{name}");
        }
        // A failure carries a message: the payload is the message.
        let failed = Message::Output(OutputCommandMessage {
            result: Some(output_command_message::Result::Failure(Failure {
                code: 500,
                message: "x".repeat(K64 + 1),
                metadata: vec![],
            })),
            ..Default::default()
        });
        assert_eq!(message_units(&failed), 1);
    }

    /// Units follow new bytes: the entry carries only ref + tail, the segment
    /// just sealed is added to the payload — a 1 MiB `SetState` sealed (4 KiB
    /// tail) counts like 1 MiB + 4 KiB inline; a reused (not new) ref does not
    /// count again.
    #[test]
    fn sealed_values_count_their_new_bytes() {
        use cairn_protocol::value::{Logical, SegmentRef};
        let v = Logical::Segmented {
            refs: vec![SegmentRef {
                sha256: [1; 32],
                len: 1 << 20,
                gen: 1,
            }],
            tail: Bytes::from(vec![b'x'; 4096]),
            len: (1 << 20) + 4096,
        }
        .encode();
        let set = Message::SetState(pb::SetStateCommandMessage {
            key: Bytes::from_static(b"k"),
            value: Some(v.clone()),
            name: String::new(),
        });
        assert_eq!(message_units(&set), 1, "reused: tail only");
        assert_eq!(
            message_units_with(&set, 1 << 20),
            17,
            "sealed: tail + new segment"
        );
        let out = Message::Output(pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(v)),
            name: String::new(),
        });
        assert_eq!(
            message_units_with(&out, 1 << 20),
            16,
            "Output: the excess over 64 KiB"
        );
        let get = Message::GetEagerState(pb::GetEagerStateCommandMessage {
            key: Bytes::from_static(b"k"),
            ..Default::default()
        });
        assert_eq!(message_units_with(&get, 1 << 20), 0, "eager read is free");
    }
}
