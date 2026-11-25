// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Fuzzing the SDK protocol's frame decoder.
//!
//! The bytes an SDK sends back are tenant data: in a shared cell, a panic on
//! the decoding path takes down the node of every customer. Three layers, each
//! taking arbitrary bytes:
//!
//! 1. byte stream → [`FrameDecoder`] (arbitrary cuts, arbitrary length cap);
//! 2. frame → [`decode`] (any message code, any or near-protobuf payload);
//! 3. frame → [`journal_view::view`] (the console reading a customer's journal).
//!
//! `proptest` with a fixed budget was chosen over `cargo fuzz`: the dev machine
//! is macOS arm64 without nightly, and the fuzzing must run in `cargo test` as
//! well as under `check.sh`'s nextest so nobody forgets to run it. The default
//! budget is in [`fuzz_gen::budget`]; `PROPTEST_CASES=<n>` overrides it for
//! long runs. Failures found are not stored as regression files: each one
//! becomes a test of its own.

mod fuzz_gen;

use {
    bytes::{Bytes, BytesMut},
    cairn_protocol::{
        decode, encode_frame, journal_view, Flags, Frame, FrameDecoder, DEFAULT_MAX_FRAME_LEN,
        HEADER_LEN,
    },
    fuzz_gen::{budget, message_type, payload},
    proptest::prelude::*,
};

/// Layers 2 and 3 on one frame: no panic; if it decodes, re-encoding and
/// decoding again must give exactly that message.
fn check_frame(frame: &Frame) {
    let _ = journal_view::view(frame.ty, &frame.payload);
    if let Ok(msg) = decode(frame) {
        assert_eq!(msg.message_type(), frame.ty);
        let wire = msg.encode(Flags::NONE);
        let mut buf = BytesMut::from(&wire[..]);
        let again = FrameDecoder::new()
            .decode(&mut buf)
            .expect("a self-encoded frame must be valid")
            .expect("enough bytes");
        assert!(buf.is_empty());
        assert_eq!(decode(&again).expect("decode again"), msg);
    }
}

/// Feeds `wire` into the decoder in the pieces `cuts`, checking the consumption contract.
fn feed(dec: FrameDecoder, wire: &[u8], cuts: &[usize]) -> Vec<Frame> {
    let mut buf = BytesMut::new();
    let mut frames = Vec::new();
    let mut at = 0;
    let mut cuts = cuts.iter().copied();
    while at < wire.len() {
        let step = cuts.next().map_or(wire.len() - at, |c| 1 + c % 64);
        let end = (at + step).min(wire.len());
        buf.extend_from_slice(&wire[at..end]);
        at = end;
        loop {
            let before = buf.len();
            match dec.decode(&mut buf) {
                Ok(None) => {
                    assert_eq!(buf.len(), before, "Ok(None) must not eat bytes");
                    break;
                }
                Ok(Some(f)) => {
                    assert_eq!(before - buf.len(), HEADER_LEN + f.payload.len());
                    frames.push(f);
                }
                // A broken stream: the caller closes the connection and reads no further.
                Err(_) => return frames,
            }
        }
    }
    frames
}

proptest! {
    #![proptest_config(budget(4096))]

    /// Arbitrary bytes, arbitrary cuts, arbitrary cap: the decoder does not
    /// panic, does not eat bytes when short, and each frame goes on through
    /// layers 2 and 3.
    #[test]
    fn raw_stream_never_panics(
        wire in prop::collection::vec(any::<u8>(), 0..2048),
        cuts in prop::collection::vec(any::<usize>(), 0..64),
        max in prop_oneof![Just(DEFAULT_MAX_FRAME_LEN), Just(u32::MAX), 0u32..512],
    ) {
        for f in feed(FrameDecoder::with_max_frame_len(max), &wire, &cuts) {
            check_frame(&f);
        }
    }

    /// A stream of frames with valid headers (any code and flags) and
    /// near-protobuf payloads, plus a junk tail: every complete frame before
    /// the first break must come out exactly as sent.
    #[test]
    fn framed_stream_round_trips(
        frames in prop::collection::vec((message_type(), any::<u16>(), payload()), 0..16),
        tail in prop::collection::vec(any::<u8>(), 0..12),
        cuts in prop::collection::vec(any::<usize>(), 0..64),
    ) {
        let mut wire = Vec::new();
        for (ty, flags, p) in &frames {
            wire.extend_from_slice(&encode_frame(*ty, Flags::from_raw(*flags), p));
        }
        wire.extend_from_slice(&tail);
        let got = feed(FrameDecoder::new(), &wire, &cuts);
        for (f, (ty, _, p)) in got.iter().zip(&frames) {
            prop_assert_eq!(f.ty, *ty);
            prop_assert_eq!(f.payload.as_ref(), &p[..]);
            check_frame(f);
        }
    }

    /// Layers 2 and 3 directly (skipping the header checks), so codes that
    /// forbid `REQUESTED_ACK` or reserved flags still reach prost.
    #[test]
    fn decode_any_frame(ty in message_type(), flags in any::<u16>(), p in payload()) {
        check_frame(&Frame::new(ty, Flags::from_raw(flags), Bytes::from(p)));
    }
}
