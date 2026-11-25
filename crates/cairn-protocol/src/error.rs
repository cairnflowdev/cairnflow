// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

use crate::message::MessageType;

/// The journal does not match the current code (spec section 8.5).
///
/// **Never retry** — a retry only reproduces the same error. This signals that
/// the code changed incompatibly with the old journal, and a human must handle
/// it.
pub const JOURNAL_MISMATCH: u32 = 570;

/// Received a message or variant that is invalid in the current state.
pub const PROTOCOL_VIOLATION: u32 = 571;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("reserved flag bits must be 0, got 0x{flags:04X}")]
    ReservedFlagsSet { flags: u16 },

    #[error("the REQUESTED_ACK flag is not allowed on {ty}")]
    AckNotAllowed { ty: MessageType },

    #[error("payload of {len} bytes exceeds the limit of {max} bytes")]
    FrameTooLarge { len: u32, max: u32 },

    #[error("message type {ty} is reserved for v2; a v1 peer must not send it")]
    ReservedMessageType { ty: MessageType },

    #[error("message type {ty} is not in any v1 range")]
    UnknownMessageType { ty: MessageType },

    #[error("cannot decode the payload of {ty}: {source}")]
    Decode {
        ty: MessageType,
        #[source]
        source: prost::DecodeError,
    },
}

impl ProtocolError {
    /// The code to put into `ErrorMessage.code`.
    ///
    /// Every framing-layer error is `PROTOCOL_VIOLATION`: it says the peer sent
    /// something invalid *by the protocol*, quite unlike a journal mismatch —
    /// which can only be detected by a layer above, comparing the journal with
    /// the user's code.
    pub fn code(&self) -> u32 {
        PROTOCOL_VIOLATION
    }
}

pub type Result<T> = std::result::Result<T, ProtocolError>;
