// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Cairnflow Service Protocol v1 — framing, the message type table, codec.
//!
//! Implements `docs/protocol-v1.md`. The document is the source of truth; this
//! crate must follow it, and each test here points at exactly one sentence of
//! it.
//!
//! The crate deliberately knows **nothing** about the runtime, storage, or
//! HTTP. It only turns a byte stream into messages and back. The reason: D9 —
//! the protocol cannot be changed once there are SDKs and customers, so it must
//! stand alone, be testable alone, and carry no decision of the layers above.
//!
//! ```
//! use cairn_protocol::{encode_message, decode, Flags, FrameDecoder, MessageType, Message, pb};
//! use bytes::BytesMut;
//!
//! let wire = encode_message(
//!     MessageType::SLEEP,
//!     Flags::REQUESTED_ACK,
//!     &pb::SleepCommandMessage {
//!         wake_up_time: 1_700_000_000_000,
//!         result_completion_id: 1,
//!         name: "await payment".into(),
//!     },
//! );
//!
//! let mut buf = BytesMut::from(&wire[..]);
//! let frame = FrameDecoder::new().decode(&mut buf).unwrap().unwrap();
//! assert!(frame.flags.requested_ack());
//! match decode(&frame).unwrap() {
//!     Message::Sleep(m) => assert_eq!(m.wake_up_time, 1_700_000_000_000),
//!     other => panic!("wrong message: {other:?}"),
//! }
//! ```

mod error;
mod frame;
/// The journal projection for readers (console, `sys_journal`).
pub mod journal_view;
mod message;
/// The logical value of a `pb::Value`: inline or segmented.
pub mod value;

/// Types generated from `proto/protocol.proto` (spec section 7).
pub mod pb {
    include!(concat!(
        env!("OUT_DIR"),
        "/dev.cairnflow.service.protocol.rs"
    ));
}

pub use {
    error::{ProtocolError, Result, JOURNAL_MISMATCH, PROTOCOL_VIOLATION},
    frame::{
        encode_frame, encode_message, Flags, Frame, FrameDecoder, Header, DEFAULT_MAX_FRAME_LEN,
        HEADER_LEN,
    },
    message::{MessageKind, MessageType},
};

use bytes::Bytes;

/// A decoded message.
///
/// `Custom` keeps the bytes as they are: the `0xFC00+` range is for third-party
/// extensions and the runtime only forwards it, without parsing (spec section 5).
#[derive(Clone, Debug, PartialEq)]
pub enum Message {
    // Control
    Start(pb::StartMessage),
    Suspension(pb::SuspensionMessage),
    Error(pb::ErrorMessage),
    End(pb::EndMessage),
    CommandAck(pb::CommandAckMessage),
    ProposeRunCompletion(pb::ProposeRunCompletionMessage),
    ProposeRunCompletionAck(pb::ProposeRunCompletionAckMessage),
    /// v3 — not fed to the state machine; the runtime filters it out first.
    Keepalive(pb::KeepaliveMessage),
    /// v3 — ends a `replay_only` turn.
    ReplayReport(pb::ReplayReportMessage),
    /// v3 — ends the turn with a new invocation of the same target.
    ContinueAsNew(pb::ContinueAsNewMessage),
    StreamAppend(pb::StreamAppendMessage),
    ArtifactPut(pb::ArtifactPutMessage),
    SegmentUrls(pb::SegmentUrlsMessage),

    // Command
    Input(pb::InputCommandMessage),
    Output(pb::OutputCommandMessage),
    GetState(pb::GetStateCommandMessage),
    SetState(pb::SetStateCommandMessage),
    ClearState(pb::ClearStateCommandMessage),
    ClearAllState(pb::ClearAllStateCommandMessage),
    GetStateKeys(pb::GetStateKeysCommandMessage),
    GetEagerState(pb::GetEagerStateCommandMessage),
    GetEagerStateKeys(pb::GetEagerStateKeysCommandMessage),
    GetPromise(pb::GetPromiseCommandMessage),
    PeekPromise(pb::PeekPromiseCommandMessage),
    CompletePromise(pb::CompletePromiseCommandMessage),
    Sleep(pb::SleepCommandMessage),
    Call(pb::CallCommandMessage),
    OneWayCall(pb::OneWayCallCommandMessage),
    SendSignal(pb::SendSignalCommandMessage),
    Run(pb::RunCommandMessage),
    AttachInvocation(pb::AttachInvocationCommandMessage),
    GetInvocationOutput(pb::GetInvocationOutputCommandMessage),

    // Notification
    GetStateCompletion(pb::GetStateCompletionNotificationMessage),
    GetStateKeysCompletion(pb::GetStateKeysCompletionNotificationMessage),
    GetPromiseCompletion(pb::GetPromiseCompletionNotificationMessage),
    PeekPromiseCompletion(pb::PeekPromiseCompletionNotificationMessage),
    CompletePromiseCompletion(pb::CompletePromiseCompletionNotificationMessage),
    SleepCompletion(pb::SleepCompletionNotificationMessage),
    CallCompletion(pb::CallCompletionNotificationMessage),
    CallInvocationIdCompletion(pb::CallInvocationIdCompletionNotificationMessage),
    RunCompletion(pb::RunCompletionNotificationMessage),
    AttachInvocationCompletion(pb::AttachInvocationCompletionNotificationMessage),
    GetInvocationOutputCompletion(pb::GetInvocationOutputCompletionNotificationMessage),
    Signal(pb::SignalNotificationMessage),

    /// `0xFC00+` — forwarded as raw bytes.
    Custom {
        ty: MessageType,
        payload: Bytes,
    },
}

impl Message {
    pub fn message_type(&self) -> MessageType {
        match self {
            Self::Start(_) => MessageType::START,
            Self::Suspension(_) => MessageType::SUSPENSION,
            Self::Error(_) => MessageType::ERROR,
            Self::End(_) => MessageType::END,
            Self::CommandAck(_) => MessageType::COMMAND_ACK,
            Self::ProposeRunCompletion(_) => MessageType::PROPOSE_RUN_COMPLETION,
            Self::ProposeRunCompletionAck(_) => MessageType::PROPOSE_RUN_COMPLETION_ACK,
            Self::Keepalive(_) => MessageType::KEEPALIVE,
            Self::ReplayReport(_) => MessageType::REPLAY_REPORT,
            Self::ContinueAsNew(_) => MessageType::CONTINUE_AS_NEW,
            Self::StreamAppend(_) => MessageType::STREAM_APPEND,
            Self::ArtifactPut(_) => MessageType::ARTIFACT_PUT,
            Self::SegmentUrls(_) => MessageType::SEGMENT_URLS,
            Self::Input(_) => MessageType::INPUT,
            Self::Output(_) => MessageType::OUTPUT,
            Self::GetState(_) => MessageType::GET_STATE,
            Self::SetState(_) => MessageType::SET_STATE,
            Self::ClearState(_) => MessageType::CLEAR_STATE,
            Self::ClearAllState(_) => MessageType::CLEAR_ALL_STATE,
            Self::GetStateKeys(_) => MessageType::GET_STATE_KEYS,
            Self::GetEagerState(_) => MessageType::GET_EAGER_STATE,
            Self::GetEagerStateKeys(_) => MessageType::GET_EAGER_STATE_KEYS,
            Self::GetPromise(_) => MessageType::GET_PROMISE,
            Self::PeekPromise(_) => MessageType::PEEK_PROMISE,
            Self::CompletePromise(_) => MessageType::COMPLETE_PROMISE,
            Self::Sleep(_) => MessageType::SLEEP,
            Self::Call(_) => MessageType::CALL,
            Self::OneWayCall(_) => MessageType::ONE_WAY_CALL,
            Self::SendSignal(_) => MessageType::SEND_SIGNAL,
            Self::Run(_) => MessageType::RUN,
            Self::AttachInvocation(_) => MessageType::ATTACH_INVOCATION,
            Self::GetInvocationOutput(_) => MessageType::GET_INVOCATION_OUTPUT,
            Self::GetStateCompletion(_) => MessageType::GET_STATE_COMPLETION,
            Self::GetStateKeysCompletion(_) => MessageType::GET_STATE_KEYS_COMPLETION,
            Self::GetPromiseCompletion(_) => MessageType::GET_PROMISE_COMPLETION,
            Self::PeekPromiseCompletion(_) => MessageType::PEEK_PROMISE_COMPLETION,
            Self::CompletePromiseCompletion(_) => MessageType::COMPLETE_PROMISE_COMPLETION,
            Self::SleepCompletion(_) => MessageType::SLEEP_COMPLETION,
            Self::CallCompletion(_) => MessageType::CALL_COMPLETION,
            Self::CallInvocationIdCompletion(_) => MessageType::CALL_INVOCATION_ID_COMPLETION,
            Self::RunCompletion(_) => MessageType::RUN_COMPLETION,
            Self::AttachInvocationCompletion(_) => MessageType::ATTACH_INVOCATION_COMPLETION,
            Self::GetInvocationOutputCompletion(_) => MessageType::GET_INVOCATION_OUTPUT_COMPLETION,
            Self::Signal(_) => MessageType::SIGNAL,
            Self::Custom { ty, .. } => *ty,
        }
    }

    pub fn kind(&self) -> MessageKind {
        self.message_type().kind()
    }

    /// Encodes back into wire bytes.
    pub fn encode(&self, flags: Flags) -> Bytes {
        let ty = self.message_type();
        macro_rules! enc {
            ($m:expr) => {
                encode_message(ty, flags, $m)
            };
        }
        match self {
            Self::Start(m) => enc!(m),
            Self::Suspension(m) => enc!(m),
            Self::Error(m) => enc!(m),
            Self::End(m) => enc!(m),
            Self::CommandAck(m) => enc!(m),
            Self::ProposeRunCompletion(m) => enc!(m),
            Self::ProposeRunCompletionAck(m) => enc!(m),
            Self::Keepalive(m) => enc!(m),
            Self::ReplayReport(m) => enc!(m),
            Self::ContinueAsNew(m) => enc!(m),
            Self::StreamAppend(m) => enc!(m),
            Self::ArtifactPut(m) => enc!(m),
            Self::SegmentUrls(m) => enc!(m),
            Self::Input(m) => enc!(m),
            Self::Output(m) => enc!(m),
            Self::GetState(m) => enc!(m),
            Self::SetState(m) => enc!(m),
            Self::ClearState(m) => enc!(m),
            Self::ClearAllState(m) => enc!(m),
            Self::GetStateKeys(m) => enc!(m),
            Self::GetEagerState(m) => enc!(m),
            Self::GetEagerStateKeys(m) => enc!(m),
            Self::GetPromise(m) => enc!(m),
            Self::PeekPromise(m) => enc!(m),
            Self::CompletePromise(m) => enc!(m),
            Self::Sleep(m) => enc!(m),
            Self::Call(m) => enc!(m),
            Self::OneWayCall(m) => enc!(m),
            Self::SendSignal(m) => enc!(m),
            Self::Run(m) => enc!(m),
            Self::AttachInvocation(m) => enc!(m),
            Self::GetInvocationOutput(m) => enc!(m),
            Self::GetStateCompletion(m) => enc!(m),
            Self::GetStateKeysCompletion(m) => enc!(m),
            Self::GetPromiseCompletion(m) => enc!(m),
            Self::PeekPromiseCompletion(m) => enc!(m),
            Self::CompletePromiseCompletion(m) => enc!(m),
            Self::SleepCompletion(m) => enc!(m),
            Self::CallCompletion(m) => enc!(m),
            Self::CallInvocationIdCompletion(m) => enc!(m),
            Self::RunCompletion(m) => enc!(m),
            Self::AttachInvocationCompletion(m) => enc!(m),
            Self::GetInvocationOutputCompletion(m) => enc!(m),
            Self::Signal(m) => enc!(m),
            Self::Custom { payload, .. } => encode_frame(ty, flags, payload),
        }
    }
}

/// The dispatch table by message type (spec section 10, the runtime checklist).
///
/// Three branches are not a "valid message", and telling them apart matters:
/// - **Custom** (`0xFC00+`) — forward, do not parse.
/// - **Reserved** — a code kept for v2. A v1 peer sending it is wrong, but we
///   *know* what it will mean; it is logged differently from an unknown code.
/// - **Unknown** — the peer is speaking complete nonsense.
pub fn decode(frame: &Frame) -> Result<Message> {
    let ty = frame.ty;
    if ty.kind() == MessageKind::Custom {
        return Ok(Message::Custom {
            ty,
            payload: frame.payload.clone(),
        });
    }
    if ty.is_reserved() {
        return Err(ProtocolError::ReservedMessageType { ty });
    }

    macro_rules! dec {
        ($variant:ident, $msg:ty) => {{
            let m = <$msg as prost::Message>::decode(frame.payload.clone())
                .map_err(|source| ProtocolError::Decode { ty, source })?;
            Ok(Message::$variant(m))
        }};
    }

    match ty {
        MessageType::START => dec!(Start, pb::StartMessage),
        MessageType::SUSPENSION => dec!(Suspension, pb::SuspensionMessage),
        MessageType::ERROR => dec!(Error, pb::ErrorMessage),
        MessageType::END => dec!(End, pb::EndMessage),
        MessageType::COMMAND_ACK => dec!(CommandAck, pb::CommandAckMessage),
        MessageType::PROPOSE_RUN_COMPLETION => {
            dec!(ProposeRunCompletion, pb::ProposeRunCompletionMessage)
        }
        MessageType::PROPOSE_RUN_COMPLETION_ACK => {
            dec!(ProposeRunCompletionAck, pb::ProposeRunCompletionAckMessage)
        }
        MessageType::KEEPALIVE => dec!(Keepalive, pb::KeepaliveMessage),
        MessageType::REPLAY_REPORT => dec!(ReplayReport, pb::ReplayReportMessage),
        MessageType::CONTINUE_AS_NEW => dec!(ContinueAsNew, pb::ContinueAsNewMessage),
        MessageType::STREAM_APPEND => dec!(StreamAppend, pb::StreamAppendMessage),
        MessageType::ARTIFACT_PUT => dec!(ArtifactPut, pb::ArtifactPutMessage),
        MessageType::SEGMENT_URLS => dec!(SegmentUrls, pb::SegmentUrlsMessage),
        MessageType::INPUT => dec!(Input, pb::InputCommandMessage),
        MessageType::OUTPUT => dec!(Output, pb::OutputCommandMessage),
        MessageType::GET_STATE => dec!(GetState, pb::GetStateCommandMessage),
        MessageType::SET_STATE => dec!(SetState, pb::SetStateCommandMessage),
        MessageType::CLEAR_STATE => dec!(ClearState, pb::ClearStateCommandMessage),
        MessageType::CLEAR_ALL_STATE => dec!(ClearAllState, pb::ClearAllStateCommandMessage),
        MessageType::GET_STATE_KEYS => dec!(GetStateKeys, pb::GetStateKeysCommandMessage),
        MessageType::GET_EAGER_STATE => dec!(GetEagerState, pb::GetEagerStateCommandMessage),
        MessageType::GET_EAGER_STATE_KEYS => {
            dec!(GetEagerStateKeys, pb::GetEagerStateKeysCommandMessage)
        }
        MessageType::GET_PROMISE => dec!(GetPromise, pb::GetPromiseCommandMessage),
        MessageType::PEEK_PROMISE => dec!(PeekPromise, pb::PeekPromiseCommandMessage),
        MessageType::COMPLETE_PROMISE => dec!(CompletePromise, pb::CompletePromiseCommandMessage),
        MessageType::SLEEP => dec!(Sleep, pb::SleepCommandMessage),
        MessageType::CALL => dec!(Call, pb::CallCommandMessage),
        MessageType::ONE_WAY_CALL => dec!(OneWayCall, pb::OneWayCallCommandMessage),
        MessageType::SEND_SIGNAL => dec!(SendSignal, pb::SendSignalCommandMessage),
        MessageType::RUN => dec!(Run, pb::RunCommandMessage),
        MessageType::ATTACH_INVOCATION => {
            dec!(AttachInvocation, pb::AttachInvocationCommandMessage)
        }
        MessageType::GET_INVOCATION_OUTPUT => {
            dec!(GetInvocationOutput, pb::GetInvocationOutputCommandMessage)
        }
        MessageType::GET_STATE_COMPLETION => {
            dec!(
                GetStateCompletion,
                pb::GetStateCompletionNotificationMessage
            )
        }
        MessageType::GET_STATE_KEYS_COMPLETION => dec!(
            GetStateKeysCompletion,
            pb::GetStateKeysCompletionNotificationMessage
        ),
        MessageType::GET_PROMISE_COMPLETION => dec!(
            GetPromiseCompletion,
            pb::GetPromiseCompletionNotificationMessage
        ),
        MessageType::PEEK_PROMISE_COMPLETION => dec!(
            PeekPromiseCompletion,
            pb::PeekPromiseCompletionNotificationMessage
        ),
        MessageType::COMPLETE_PROMISE_COMPLETION => dec!(
            CompletePromiseCompletion,
            pb::CompletePromiseCompletionNotificationMessage
        ),
        MessageType::SLEEP_COMPLETION => {
            dec!(SleepCompletion, pb::SleepCompletionNotificationMessage)
        }
        MessageType::CALL_COMPLETION => {
            dec!(CallCompletion, pb::CallCompletionNotificationMessage)
        }
        MessageType::CALL_INVOCATION_ID_COMPLETION => dec!(
            CallInvocationIdCompletion,
            pb::CallInvocationIdCompletionNotificationMessage
        ),
        MessageType::RUN_COMPLETION => dec!(RunCompletion, pb::RunCompletionNotificationMessage),
        MessageType::ATTACH_INVOCATION_COMPLETION => dec!(
            AttachInvocationCompletion,
            pb::AttachInvocationCompletionNotificationMessage
        ),
        MessageType::GET_INVOCATION_OUTPUT_COMPLETION => dec!(
            GetInvocationOutputCompletion,
            pb::GetInvocationOutputCompletionNotificationMessage
        ),
        MessageType::SIGNAL => dec!(Signal, pb::SignalNotificationMessage),
        _ => Err(ProtocolError::UnknownMessageType { ty }),
    }
}
