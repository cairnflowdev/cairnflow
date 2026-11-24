// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Framing — section 4 of `protocol-v1.md`.
//!
//! ```text
//!  63          48 47                    32 31                  0
//! ┌──────────────┬────────────────────────┬────────────────────┐
//! │  type (u16)  │         flags          │    length (u32)    │
//! └──────────────┴────────────────────────┴────────────────────┘
//! ```
//!
//! `length` is the number of payload bytes, **not** counting the header.

use {
    crate::{
        error::{ProtocolError, Result},
        message::MessageType,
    },
    bytes::{Buf, BufMut, Bytes, BytesMut},
};

pub const HEADER_LEN: usize = 8;

/// The default payload length cap.
///
/// **Spec section 4 defines no cap** — `length` is a `u32`, so a broken or
/// malicious peer declares 4GiB and we would sit allocating. This is a hole in
/// the spec, not in the implementation: every implementation has to pick a
/// number of its own, and if two sides pick differently, a message valid for one
/// is refused by the other. This cap should go into section 4.
pub const DEFAULT_MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;

/// The flags in bits 47..32 of the header.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct Flags(u16);

impl Flags {
    pub const NONE: Self = Self(0);
    /// Bit 47 — the highest bit of the flags field.
    pub const REQUESTED_ACK: Self = Self(0x8000);

    /// Bits 46..32 — MUST be 0.
    const RESERVED_MASK: u16 = 0x7FFF;

    pub const fn requested_ack(self) -> bool {
        self.0 & Self::REQUESTED_ACK.0 != 0
    }

    pub const fn raw(self) -> u16 {
        self.0
    }

    pub const fn from_raw(v: u16) -> Self {
        Self(v)
    }
}

impl std::fmt::Debug for Flags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.requested_ack() {
            write!(f, "REQUESTED_ACK")
        } else {
            write!(f, "—")
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub ty: MessageType,
    pub flags: Flags,
    pub len: u32,
}

impl Header {
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let raw =
            ((self.ty.raw() as u64) << 48) | ((self.flags.raw() as u64) << 32) | (self.len as u64);
        raw.to_be_bytes()
    }

    /// Reads and **validates** the header.
    ///
    /// Three checks, each one a sentence of the spec: the reserved bits must be
    /// 0; `REQUESTED_ACK` is valid only on messages with the `ack` column; the
    /// length does not exceed the cap.
    pub fn decode(bytes: [u8; HEADER_LEN], max_frame_len: u32) -> Result<Self> {
        let raw = u64::from_be_bytes(bytes);
        let ty = MessageType((raw >> 48) as u16);
        let flags = Flags::from_raw(((raw >> 32) & 0xFFFF) as u16);
        let len = raw as u32;

        if flags.raw() & Flags::RESERVED_MASK != 0 {
            return Err(ProtocolError::ReservedFlagsSet { flags: flags.raw() });
        }
        if flags.requested_ack() && !ty.allows_requested_ack() {
            return Err(ProtocolError::AckNotAllowed { ty });
        }
        if len > max_frame_len {
            return Err(ProtocolError::FrameTooLarge {
                len,
                max: max_frame_len,
            });
        }
        Ok(Self { ty, flags, len })
    }
}

/// A message split off the byte stream. The payload is **not** parsed yet — the
/// `0xFC00+` range must pass through intact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub ty: MessageType,
    pub flags: Flags,
    pub payload: Bytes,
}

impl Frame {
    pub fn new(ty: MessageType, flags: Flags, payload: Bytes) -> Self {
        Self { ty, flags, payload }
    }

    pub fn encode(&self) -> Bytes {
        encode_frame(self.ty, self.flags, &self.payload)
    }
}

pub fn encode_frame(ty: MessageType, flags: Flags, payload: &[u8]) -> Bytes {
    let header = Header {
        ty,
        flags,
        len: payload.len() as u32,
    };
    let mut out = BytesMut::with_capacity(HEADER_LEN + payload.len());
    out.put_slice(&header.encode());
    out.put_slice(payload);
    out.freeze()
}

pub fn encode_message<M: prost::Message>(ty: MessageType, flags: Flags, msg: &M) -> Bytes {
    let mut payload = BytesMut::with_capacity(msg.encoded_len());
    msg.encode(&mut payload)
        .expect("BytesMut never runs out of room");
    encode_frame(ty, flags, &payload)
}

/// Splits frames off a byte stream arriving in pieces.
///
/// The `BIDI_STREAM` transport is HTTP/2, so messages arrive in arbitrary
/// pieces: one piece can hold half a header, or three and a half messages. The
/// decoder returns `Ok(None)` when it needs more data and consumes **nothing**
/// in that case.
#[derive(Clone, Copy, Debug)]
pub struct FrameDecoder {
    max_frame_len: u32,
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self {
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
        }
    }
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_frame_len(max_frame_len: u32) -> Self {
        Self { max_frame_len }
    }

    /// `Ok(None)` = more bytes needed. `Err` = the stream is broken, the connection must be closed.
    pub fn decode(&self, buf: &mut BytesMut) -> Result<Option<Frame>> {
        if buf.len() < HEADER_LEN {
            return Ok(None);
        }
        let mut head = [0u8; HEADER_LEN];
        head.copy_from_slice(&buf[..HEADER_LEN]);

        // Check the header BEFORE waiting for the whole payload. Waiting first
        // and checking the cap afterwards would let a lying `length` make us hold
        // a huge buffer.
        let header = Header::decode(head, self.max_frame_len)?;

        let total = HEADER_LEN + header.len as usize;
        if buf.len() < total {
            return Ok(None);
        }
        buf.advance(HEADER_LEN);
        let payload = buf.split_to(header.len as usize).freeze();
        Ok(Some(Frame {
            ty: header.ty,
            flags: header.flags,
            payload,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hdr(ty: MessageType, flags: Flags, len: u32) -> [u8; 8] {
        Header { ty, flags, len }.encode()
    }

    #[test]
    fn header_roundtrips_in_big_endian_layout() {
        let h = Header {
            ty: MessageType::CALL,
            flags: Flags::REQUESTED_ACK,
            len: 0x0000_1234,
        };
        let bytes = h.encode();
        // type in bytes 0-1, flags in bytes 2-3, length in bytes 4-7 — all big-endian.
        assert_eq!(&bytes[0..2], &[0x04, 0x0D]);
        assert_eq!(&bytes[2..4], &[0x80, 0x00]);
        assert_eq!(&bytes[4..8], &[0x00, 0x00, 0x12, 0x34]);
        assert_eq!(Header::decode(bytes, DEFAULT_MAX_FRAME_LEN).unwrap(), h);
    }

    #[test]
    fn requested_ack_uses_bit_47() {
        // The spec: bit 47 = 0x8000_0000_0000 in the 64-bit header.
        let bytes = hdr(MessageType::RUN, Flags::REQUESTED_ACK, 0);
        let raw = u64::from_be_bytes(bytes);
        assert_eq!(raw & 0x8000_0000_0000, 0x8000_0000_0000);
    }

    #[test]
    fn nonzero_reserved_bits_are_rejected() {
        for bit in 0..15 {
            let flags = Flags::from_raw(1 << bit);
            let bytes = hdr(MessageType::RUN, flags, 0);
            let err = Header::decode(bytes, DEFAULT_MAX_FRAME_LEN).unwrap_err();
            assert!(
                matches!(err, ProtocolError::ReservedFlagsSet { .. }),
                "reserved bit {bit} slipped through"
            );
        }
    }

    #[test]
    fn ack_flag_where_not_allowed_is_rejected() {
        // Spec section 4: "Valid only on messages with the ack column. Set elsewhere ⇒
        // PROTOCOL_VIOLATION."
        let bytes = hdr(MessageType::START, Flags::REQUESTED_ACK, 0);
        assert_eq!(
            Header::decode(bytes, DEFAULT_MAX_FRAME_LEN).unwrap_err(),
            ProtocolError::AckNotAllowed {
                ty: MessageType::START
            }
        );
        // And on a message where it is allowed it passes.
        let ok = hdr(MessageType::RUN, Flags::REQUESTED_ACK, 0);
        assert!(Header::decode(ok, DEFAULT_MAX_FRAME_LEN).is_ok());
    }

    #[test]
    fn oversized_length_is_rejected_at_header() {
        let bytes = hdr(MessageType::INPUT, Flags::NONE, 64);
        let err = FrameDecoder::with_max_frame_len(32)
            .decode(&mut BytesMut::from(&bytes[..]))
            .unwrap_err();
        assert_eq!(err, ProtocolError::FrameTooLarge { len: 64, max: 32 });
    }

    #[test]
    fn empty_payload_is_valid() {
        // EndMessage{} encodes to 0 bytes — the decoder must not take that as "missing data".
        let mut buf = BytesMut::from(&encode_frame(MessageType::END, Flags::NONE, b"")[..]);
        let frame = FrameDecoder::new().decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.ty, MessageType::END);
        assert!(frame.payload.is_empty());
        assert!(buf.is_empty());
    }

    #[test]
    fn byte_by_byte_stream_still_reassembles() {
        // The worst case of HTTP/2: only 1 byte added at a time.
        let wire = encode_frame(MessageType::INPUT, Flags::NONE, b"hello there");
        let dec = FrameDecoder::new();
        let mut buf = BytesMut::new();
        for (i, b) in wire.iter().enumerate() {
            buf.extend_from_slice(&[*b]);
            let got = dec.decode(&mut buf).unwrap();
            if i + 1 < wire.len() {
                assert!(got.is_none(), "returned a frame with only {} bytes", i + 1);
            } else {
                let f = got.expect("the last byte must complete the frame");
                assert_eq!(f.payload.as_ref(), b"hello there");
            }
        }
    }

    #[test]
    fn many_frames_in_one_chunk() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&encode_frame(MessageType::END, Flags::NONE, b""));
        buf.extend_from_slice(&encode_frame(MessageType::INPUT, Flags::NONE, b"a"));
        buf.extend_from_slice(&encode_frame(MessageType::RUN, Flags::REQUESTED_ACK, b"bb"));
        // Half of a fourth frame.
        buf.extend_from_slice(&encode_frame(MessageType::OUTPUT, Flags::NONE, b"ccc")[..5]);

        let dec = FrameDecoder::new();
        let mut tys = Vec::new();
        while let Some(f) = dec.decode(&mut buf).unwrap() {
            tys.push(f.ty);
        }
        assert_eq!(
            tys,
            vec![MessageType::END, MessageType::INPUT, MessageType::RUN]
        );
        assert_eq!(
            buf.len(),
            5,
            "the partial part must be kept in the buffer as is"
        );
    }

    #[test]
    fn consumes_nothing_on_partial_data() {
        let wire = encode_frame(MessageType::INPUT, Flags::NONE, b"abcdef");
        let mut buf = BytesMut::from(&wire[..wire.len() - 1]);
        let before = buf.len();
        assert!(FrameDecoder::new().decode(&mut buf).unwrap().is_none());
        assert_eq!(
            buf.len(),
            before,
            "the decoder ate bytes before the frame was complete"
        );
    }

    #[test]
    fn custom_passes_raw_bytes() {
        // 0xFC00+ : the runtime forwards it, does not parse it. A payload that is
        // not valid protobuf must pass too.
        let junk = &[0xFFu8, 0x00, 0x13, 0x37];
        let mut buf = BytesMut::from(&encode_frame(MessageType(0xFC01), Flags::NONE, junk)[..]);
        let f = FrameDecoder::new().decode(&mut buf).unwrap().unwrap();
        assert_eq!(f.ty.kind(), crate::MessageKind::Custom);
        assert_eq!(f.payload.as_ref(), junk);
    }
}
