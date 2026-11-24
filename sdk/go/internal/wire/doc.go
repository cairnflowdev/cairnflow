// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

// Package wire is the byte layer of the Cairnflow service protocol (docs/protocol-v1.md
// sections 4, 5, 7 and 9): frame headers, splitting a byte stream into frames, the message type
// table, and a hand-written protobuf codec for every message the protocol defines, up to wire v6.
//
// # Frames
//
// A frame is an 8-byte big-endian header (type, flags, payload length) followed by a protobuf
// payload. [ParseHeader] checks a header the moment its 8 bytes are available: reserved flag
// bits must be zero and the length must not exceed [MaxFrameLen]. Three ways to get frames out
// of bytes:
//
//   - [DecodeFrames] splits a whole body (REQUEST_RESPONSE): bytes left over at the end are an
//     error, the body has ended;
//   - [Splitter] is fed chunks of a stream (BIDI_STREAM DATA frames arrive at arbitrary
//     boundaries): bytes left over mean "wait for more";
//   - [FrameReader] pulls frames from an [io.Reader].
//
// [AppendFrame] and [EncodeFrame] write a frame; [EncodeMessage] writes a message as a frame.
//
// # Messages
//
// Every protobuf message of section 7 (with the fields added by wire v2 to v6) has a Go struct.
// Messages that travel as frames implement [Message]; [Decode] turns a frame's type and payload
// into the matching struct and [Marshal] writes it back.
//
// Encoding is proto3 canonical form, the form the conformance traces compare byte for byte
// (conformance/README.md, "Canonical encoding"): fields in ascending tag order, numeric repeated
// fields packed, default values not written except for members of a oneof, and a present nested
// message written even when empty. Decoding accepts every valid form: any field order, packed
// or unpacked repeated numbers, duplicate fields (the last scalar wins, nested messages merge),
// and unknown fields, which are skipped. A known field carrying the wrong wire type is an error,
// as in prost.
//
// Presence follows proto3:
//
//   - a scalar written only when it is not the default is a plain Go value;
//   - a proto3 optional scalar and a scalar member of a oneof is a pointer (nil = absent);
//   - a nested message is a pointer (nil = absent); the empty message Void is a bool;
//   - the one bytes member of a oneof (ProposeRunCompletion.value) has a HasValue flag.
//
// When more than one member of a oneof is set, Marshal writes only the member with the highest
// field number: the one a decoder would keep if all were written.
//
// Decoded byte slices and strings are copies: holding on to a small value never pins the frame
// it came from. String fields are not checked for valid UTF-8; their bytes are kept as they are.
package wire
