// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The message type table — section 5 of `protocol-v1.md`.
//!
//! The type is a newtype over `u16`, not a closed enum, for two reasons that
//! come straight from the spec: the `0xFC00+` range must be **forwarded as raw
//! bytes without parsing**, and the `reserved` codes must be distinguishable
//! from entirely unknown codes — the former means "the peer speaks a future
//! language", the latter "the peer is wrong".

/// The four groups of spec section 5, plus a fifth group for codes in no range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageKind {
    Control,
    Command,
    Notification,
    /// `0xFC00+` — the runtime forwards the raw bytes, does not parse.
    Custom,
    Unknown,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageType(pub u16);

impl MessageType {
    // ── Control 0x0000–0x000F
    pub const START: Self = Self(0x0000);
    pub const SUSPENSION: Self = Self(0x0001);
    pub const ERROR: Self = Self(0x0002);
    pub const END: Self = Self(0x0003);
    pub const COMMAND_ACK: Self = Self(0x0004);
    pub const PROPOSE_RUN_COMPLETION: Self = Self(0x0005);
    pub const PROPOSE_RUN_COMPLETION_ACK: Self = Self(0x0007);
    /// Wire v3: SDK → runtime, empty payload — "the `ctx.run` closure is still
    /// running". Not in the journal, not through the state machine; the runtime
    /// only uses it to restart the silence cap.
    pub const KEEPALIVE: Self = Self(0x0008);
    /// Wire v3: SDK → runtime, ends a `replay_only` turn — see
    /// `ReplayReportMessage` in `protocol.proto`.
    pub const REPLAY_REPORT: Self = Self(0x0009);
    /// Wire v3: SDK → runtime, `ctx.continueAsNew(input)`.
    pub const CONTINUE_AS_NEW: Self = Self(0x000A);
    /// Wire v4: SDK → runtime, **side channel** — a stream record
    /// (`StreamAppendMessage`). Not in the journal, not through the state
    /// machine; the turn layer hands it to the range's `StreamHub`.
    pub const STREAM_APPEND: Self = Self(0x000B);
    /// Wire v4: SDK → runtime, side channel — an artifact
    /// (`ArtifactPutMessage`) that the turn layer PUTs before the next frame.
    pub const ARTIFACT_PUT: Self = Self(0x000C);
    /// Wire v6: side channel runtime → SDK — the read URLs of the segment refs
    /// in the next message (BIDI).
    pub const SEGMENT_URLS: Self = Self(0x000D);

    // ── Command 0x0400+
    pub const INPUT: Self = Self(0x0400);
    pub const OUTPUT: Self = Self(0x0401);
    pub const GET_STATE: Self = Self(0x0402);
    pub const SET_STATE: Self = Self(0x0403);
    pub const CLEAR_STATE: Self = Self(0x0404);
    pub const CLEAR_ALL_STATE: Self = Self(0x0405);
    pub const GET_STATE_KEYS: Self = Self(0x0406);
    pub const GET_EAGER_STATE: Self = Self(0x0407);
    pub const GET_EAGER_STATE_KEYS: Self = Self(0x0408);
    pub const GET_PROMISE: Self = Self(0x0409);
    pub const PEEK_PROMISE: Self = Self(0x040A);
    pub const COMPLETE_PROMISE: Self = Self(0x040B);
    pub const SLEEP: Self = Self(0x040C);
    pub const CALL: Self = Self(0x040D);
    pub const ONE_WAY_CALL: Self = Self(0x040E);
    pub const SEND_SIGNAL: Self = Self(0x0410);
    pub const RUN: Self = Self(0x0411);
    pub const ATTACH_INVOCATION: Self = Self(0x0412);
    pub const GET_INVOCATION_OUTPUT: Self = Self(0x0413);

    // ── Notification 0x8000+ and 0xFBFF
    pub const GET_STATE_COMPLETION: Self = Self(0x8002);
    pub const GET_STATE_KEYS_COMPLETION: Self = Self(0x8006);
    pub const GET_PROMISE_COMPLETION: Self = Self(0x8009);
    pub const PEEK_PROMISE_COMPLETION: Self = Self(0x800A);
    pub const COMPLETE_PROMISE_COMPLETION: Self = Self(0x800B);
    pub const SLEEP_COMPLETION: Self = Self(0x800C);
    pub const CALL_COMPLETION: Self = Self(0x800D);
    pub const CALL_INVOCATION_ID_COMPLETION: Self = Self(0x800E);
    pub const RUN_COMPLETION: Self = Self(0x8011);
    pub const ATTACH_INVOCATION_COMPLETION: Self = Self(0x8012);
    pub const GET_INVOCATION_OUTPUT_COMPLETION: Self = Self(0x8013);
    pub const SIGNAL: Self = Self(0xFBFF);

    pub const fn raw(self) -> u16 {
        self.0
    }

    pub const fn kind(self) -> MessageKind {
        match self.0 {
            // The custom range must be checked BEFORE the 0x8000 bit, since 0xFC00+ has that bit
            // too.
            0xFC00..=0xFFFF => MessageKind::Custom,
            0x8000..=0xFBFF => MessageKind::Notification,
            0x0000..=0x000F => MessageKind::Control,
            0x0400..=0x04FF => MessageKind::Command,
            _ => MessageKind::Unknown,
        }
    }

    /// A code kept for v2 (spec section 9). Receiving it from a v1 peer is a
    /// protocol error, but a different kind from an unknown code: here we *know*
    /// what it will mean later.
    pub const fn is_reserved(self) -> bool {
        matches!(self.0, 0x0006 | 0x040F | 0x0414)
    }

    /// Whether the `REQUESTED_ACK` flag is valid on this message — the `ack`
    /// column of spec section 5. Set elsewhere ⇒ `PROTOCOL_VIOLATION`.
    pub const fn allows_requested_ack(self) -> bool {
        matches!(
            self.0,
            0x0005 // ProposeRunCompletion
                | 0x0400..=0x040E // commands, including the newly opened v2 codes
                | 0x0410 // SendSignal
                | 0x0411 // Run
                | 0x0412..=0x0413 // AttachInvocation, GetInvocationOutput
        )
    }

    /// The notifications this command will receive.
    ///
    /// The spec section 5 convention: the completion of `0x04XX` has code
    /// `0x80XX`. `Call` is the only exception — it receives **two**: the
    /// invocation id arrives at once, the result later.
    pub const fn completions(self) -> &'static [MessageType] {
        match self.0 {
            0x0402 => &[Self::GET_STATE_COMPLETION],
            0x0406 => &[Self::GET_STATE_KEYS_COMPLETION],
            0x0409 => &[Self::GET_PROMISE_COMPLETION],
            0x040A => &[Self::PEEK_PROMISE_COMPLETION],
            0x040B => &[Self::COMPLETE_PROMISE_COMPLETION],
            0x040C => &[Self::SLEEP_COMPLETION],
            0x040D => &[Self::CALL_COMPLETION, Self::CALL_INVOCATION_ID_COMPLETION],
            0x040E => &[Self::CALL_INVOCATION_ID_COMPLETION],
            0x0411 => &[Self::RUN_COMPLETION],
            0x0412 => &[Self::ATTACH_INVOCATION_COMPLETION],
            0x0413 => &[Self::GET_INVOCATION_OUTPUT_COMPLETION],
            _ => &[],
        }
    }

    /// Whether this command waits for a completion (spec section 7, "Completable").
    pub const fn is_completable(self) -> bool {
        !self.completions().is_empty()
    }

    pub const fn name(self) -> Option<&'static str> {
        Some(match self.0 {
            0x0000 => "Start",
            0x0001 => "Suspension",
            0x0002 => "Error",
            0x0003 => "End",
            0x0004 => "CommandAck",
            0x0005 => "ProposeRunCompletion",
            0x0007 => "ProposeRunCompletionAck",
            0x0008 => "Keepalive",
            0x0009 => "ReplayReport",
            0x000A => "ContinueAsNew",
            0x000B => "StreamAppend",
            0x000C => "ArtifactPut",
            0x000D => "SegmentUrls",
            0x0400 => "Input",
            0x0401 => "Output",
            0x0402 => "GetState",
            0x0403 => "SetState",
            0x0404 => "ClearState",
            0x0405 => "ClearAllState",
            0x0406 => "GetStateKeys",
            0x0407 => "GetEagerState",
            0x0408 => "GetEagerStateKeys",
            0x0409 => "GetPromise",
            0x040A => "PeekPromise",
            0x040B => "CompletePromise",
            0x040C => "Sleep",
            0x040D => "Call",
            0x040E => "OneWayCall",
            0x0410 => "SendSignal",
            0x0411 => "Run",
            0x0412 => "AttachInvocation",
            0x0413 => "GetInvocationOutput",
            0x8002 => "GetStateCompletion",
            0x8006 => "GetStateKeysCompletion",
            0x8009 => "GetPromiseCompletion",
            0x800A => "PeekPromiseCompletion",
            0x800B => "CompletePromiseCompletion",
            0x800C => "SleepCompletion",
            0x800D => "CallCompletion",
            0x800E => "CallInvocationIdCompletion",
            0x8011 => "RunCompletion",
            0x8012 => "AttachInvocationCompletion",
            0x8013 => "GetInvocationOutputCompletion",
            0xFBFF => "Signal",
            _ => return None,
        })
    }

    /// The message types v1 knows how to parse. Neither custom nor reserved is one.
    pub const fn is_known(self) -> bool {
        self.name().is_some()
    }
}

impl std::fmt::Debug for MessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.name() {
            Some(n) => write!(f, "{n}(0x{:04X})", self.0),
            None if self.is_reserved() => write!(f, "reserved(0x{:04X})", self.0),
            None => write!(f, "0x{:04X}", self.0),
        }
    }
}

impl std::fmt::Display for MessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl From<u16> for MessageType {
    fn from(v: u16) -> Self {
        Self(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_range_not_mistaken_for_notification() {
        // 0xFC00+ has the 0x8000 bit too. Checking in the wrong order parses
        // third-party data as a notification by mistake.
        assert_eq!(MessageType(0xFC00).kind(), MessageKind::Custom);
        assert_eq!(MessageType(0xFFFF).kind(), MessageKind::Custom);
        assert_eq!(MessageType(0xFBFF).kind(), MessageKind::Notification);
    }

    #[test]
    fn grouping_follows_spec_table() {
        assert_eq!(MessageType::START.kind(), MessageKind::Control);
        assert_eq!(MessageType::RUN.kind(), MessageKind::Command);
        assert_eq!(
            MessageType::RUN_COMPLETION.kind(),
            MessageKind::Notification
        );
        assert_eq!(MessageType(0x0200).kind(), MessageKind::Unknown);
    }

    #[test]
    fn convention_04xx_maps_to_80xx() {
        // The spec: the completion of command 0x04XX has code 0x80XX — keeping
        // the LOW BYTE, not an OR of bit 0x8000 (0x0402 → 0x8002, not 0x8402).
        for cmd in [MessageType::GET_STATE, MessageType::SLEEP, MessageType::RUN] {
            let expected = MessageType(0x8000 | (cmd.raw() & 0x00FF));
            assert_eq!(cmd.completions(), &[expected], "wrong convention for {cmd}");
        }
    }

    #[test]
    fn call_gets_two_notifications() {
        assert_eq!(MessageType::CALL.completions().len(), 2);
        assert_eq!(MessageType::ONE_WAY_CALL.completions().len(), 1);
        assert!(!MessageType::SET_STATE.is_completable());
    }

    #[test]
    fn ack_valid_only_where_spec_allows() {
        for t in [
            MessageType::INPUT,
            MessageType::OUTPUT,
            MessageType::GET_STATE,
            MessageType::SET_STATE,
            MessageType::CLEAR_STATE,
            MessageType::SLEEP,
            MessageType::CALL,
            MessageType::ONE_WAY_CALL,
            MessageType::SEND_SIGNAL,
            MessageType::RUN,
            MessageType::PROPOSE_RUN_COMPLETION,
        ] {
            assert!(t.allows_requested_ack(), "{t} must allow ack");
        }
        for t in [
            MessageType::START,
            MessageType::SUSPENSION,
            MessageType::ERROR,
            MessageType::END,
            MessageType::COMMAND_ACK,
            MessageType::PROPOSE_RUN_COMPLETION_ACK,
            MessageType::RUN_COMPLETION,
            MessageType::SIGNAL,
        ] {
            assert!(!t.allows_requested_ack(), "{t} must NOT allow ack");
        }
    }

    #[test]
    fn reserved_code_distinct_from_unknown_code() {
        assert!(MessageType(0x0006).is_reserved()); // AwaitingOn, v2
        assert!(MessageType(0x040F).is_reserved());
        assert!(MessageType(0x0414).is_reserved()); // Restate's CompleteAwakeable — not opened yet
        assert!(!MessageType(0x040F).is_known());
        assert!(
            !MessageType(0x0299).is_reserved(),
            "an unknown code is not reserved"
        );
    }

    #[test]
    fn opened_v2_codes_no_longer_reserved() {
        for t in [
            MessageType::CLEAR_ALL_STATE,
            MessageType::GET_STATE_KEYS,
            MessageType::GET_EAGER_STATE,
            MessageType::GET_EAGER_STATE_KEYS,
            MessageType::GET_PROMISE,
            MessageType::PEEK_PROMISE,
            MessageType::COMPLETE_PROMISE,
            MessageType::ATTACH_INVOCATION,
            MessageType::GET_INVOCATION_OUTPUT,
            MessageType::GET_STATE_KEYS_COMPLETION,
        ] {
            assert!(!t.is_reserved(), "{t} is open, no longer reserved");
            assert!(t.is_known(), "{t} must have a name");
        }
        // The 0x04XX → 0x80XX convention holds for the new codes.
        for cmd in [
            MessageType::GET_STATE_KEYS,
            MessageType::GET_PROMISE,
            MessageType::PEEK_PROMISE,
            MessageType::COMPLETE_PROMISE,
            MessageType::ATTACH_INVOCATION,
            MessageType::GET_INVOCATION_OUTPUT,
        ] {
            let expected = MessageType(0x8000 | (cmd.raw() & 0x00FF));
            assert_eq!(cmd.completions(), &[expected], "wrong convention for {cmd}");
        }
        // Eager state is NOT completable — the result is embedded in the command.
        assert!(!MessageType::GET_EAGER_STATE.is_completable());
        assert!(!MessageType::GET_EAGER_STATE_KEYS.is_completable());
        assert!(!MessageType::CLEAR_ALL_STATE.is_completable());
    }
}
