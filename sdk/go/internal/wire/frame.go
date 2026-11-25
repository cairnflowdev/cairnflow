// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// HeaderLen is the size of a frame header.
const HeaderLen = 8

// readerInitialPayload is the most a FrameReader allocates for a payload before its bytes
// arrive; past it the buffer grows as they do.
const readerInitialPayload = 64 << 10

// MaxFrameLen is the payload length limit, 16 MiB (docs/protocol-v1.md section 4, "Length
// limit"). The number is normative: both sides must refuse the same frames. A frame above it
// is refused as soon as its header is read, before waiting for the payload.
const MaxFrameLen = 16 << 20

// FlagRequestedAck is bit 47 of the header, the high bit of the 16-bit flags field: on a
// command, the runtime sends a CommandAck once the entry is durable; on ProposeRunCompletion,
// it answers with ProposeRunCompletionAck. Bits 46..32 are reserved and must be 0.
const FlagRequestedAck uint16 = 0x8000

// Errors reported while splitting frames. Each one is a protocol violation (code 571).
var (
	// ErrReservedFlags: a header with a reserved flag bit set.
	ErrReservedFlags = errors.New("reserved flag bits must be 0")
	// ErrFrameTooLarge: a header declaring a payload above MaxFrameLen.
	ErrFrameTooLarge = errors.New("frame exceeds the length limit")
	// ErrTruncated: the byte stream ended in the middle of a frame.
	ErrTruncated = errors.New("truncated byte stream")
)

// FrameHeader is a decoded 8-byte frame header.
type FrameHeader struct {
	Type   MessageType
	Flags  uint16
	Length uint32 // payload bytes, not counting the header
}

// RequestedAck reports whether the REQUESTED_ACK flag is set.
func (h FrameHeader) RequestedAck() bool { return h.Flags&FlagRequestedAck != 0 }

// ParseHeader decodes the first HeaderLen bytes of b and checks them: reserved flag bits must
// be 0 and the length must not exceed MaxFrameLen. b shorter than HeaderLen is ErrTruncated.
func ParseHeader(b []byte) (FrameHeader, error) {
	if len(b) < HeaderLen {
		return FrameHeader{}, fmt.Errorf("%w: %d bytes do not form a frame header", ErrTruncated, len(b))
	}
	h := FrameHeader{
		Type:   MessageType(binary.BigEndian.Uint16(b[0:2])),
		Flags:  binary.BigEndian.Uint16(b[2:4]),
		Length: binary.BigEndian.Uint32(b[4:8]),
	}
	if h.Flags&^FlagRequestedAck != 0 {
		return h, fmt.Errorf("%w, got 0x%04x", ErrReservedFlags, h.Flags)
	}
	if h.Length > MaxFrameLen {
		return h, fmt.Errorf("%w: payload of %d bytes, the limit is %d", ErrFrameTooLarge, h.Length, MaxFrameLen)
	}
	return h, nil
}

// AppendHeader appends the 8-byte encoding of h.
func AppendHeader(dst []byte, h FrameHeader) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(h.Type))
	dst = binary.BigEndian.AppendUint16(dst, h.Flags)
	return binary.BigEndian.AppendUint32(dst, h.Length)
}

// Frame is one message on the wire: its type, flags and protobuf payload.
type Frame struct {
	Type    MessageType
	Flags   uint16
	Payload []byte
}

// RequestedAck reports whether the REQUESTED_ACK flag is set.
func (f Frame) RequestedAck() bool { return f.Flags&FlagRequestedAck != 0 }

// Len is the size of the frame on the wire, header included.
func (f Frame) Len() int { return HeaderLen + len(f.Payload) }

// AppendTo appends the frame (header and payload) to dst.
func (f Frame) AppendTo(dst []byte) []byte { return AppendFrame(dst, f.Type, f.Flags, f.Payload) }

// AppendFrame appends a frame to dst. It does not check the length limit; a payload above
// MaxFrameLen would be refused by the receiver.
func AppendFrame(dst []byte, t MessageType, flags uint16, payload []byte) []byte {
	dst = AppendHeader(dst, FrameHeader{Type: t, Flags: flags, Length: uint32(len(payload))})
	return append(dst, payload...)
}

// EncodeFrame returns a new frame.
func EncodeFrame(t MessageType, flags uint16, payload []byte) []byte {
	return AppendFrame(make([]byte, 0, HeaderLen+len(payload)), t, flags, payload)
}

// DecodeFrames splits a whole body into frames (REQUEST_RESPONSE: the body has ended, so
// bytes left over that do not form a frame are ErrTruncated, not "wait for more"). The
// payloads alias b.
func DecodeFrames(b []byte) ([]Frame, error) {
	var out []Frame
	at := 0
	for len(b)-at >= HeaderLen {
		h, err := ParseHeader(b[at:])
		if err != nil {
			return out, err
		}
		end := at + HeaderLen + int(h.Length)
		if end > len(b) {
			break
		}
		out = append(out, Frame{Type: h.Type, Flags: h.Flags, Payload: b[at+HeaderLen : end : end]})
		at = end
	}
	if at != len(b) {
		return out, fmt.Errorf("%w: %d trailing bytes do not form a frame", ErrTruncated, len(b)-at)
	}
	return out, nil
}

// Splitter splits frames out of a byte stream that arrives in chunks of any size (BIDI_STREAM:
// HTTP/2 DATA frames do not line up with protocol frames). Bytes left over after the last
// complete frame are kept for the next chunk; Pending reports how many. A header is checked as
// soon as its 8 bytes have arrived, before the payload is waited for, so a lying length never
// makes the splitter buffer it. After an error the splitter is broken: every later Push returns
// the same error.
//
// The zero value is ready to use. A Splitter is not safe for concurrent use.
type Splitter struct {
	buf []byte
	err error
}

// Push feeds one chunk and returns the frames that became complete, in order. Their payloads
// are fresh copies: chunk is not retained.
func (s *Splitter) Push(chunk []byte) ([]Frame, error) {
	if s.err != nil {
		return nil, s.err
	}
	var out []Frame
	data := chunk
	if len(s.buf) > 0 {
		s.buf = append(s.buf, chunk...)
		data = s.buf
	}
	at := 0
	for len(data)-at >= HeaderLen {
		h, err := ParseHeader(data[at:])
		if err != nil {
			s.err = err
			s.buf = nil
			return out, err
		}
		end := at + HeaderLen + int(h.Length)
		if end > len(data) {
			break
		}
		payload := append([]byte{}, data[at+HeaderLen:end]...)
		out = append(out, Frame{Type: h.Type, Flags: h.Flags, Payload: payload})
		at = end
	}
	rest := data[at:]
	switch {
	case len(rest) == 0:
		s.buf = s.buf[:0]
	case len(s.buf) == 0:
		// The leftover still lives in the caller's chunk: copy it.
		s.buf = append(s.buf, rest...)
	default:
		s.buf = s.buf[:copy(s.buf, rest)]
	}
	// Make room for the whole frame at once when its header is known: no repeated growth
	// while a large payload trickles in.
	if len(s.buf) >= HeaderLen {
		need := HeaderLen + int(binary.BigEndian.Uint32(s.buf[4:8]))
		if cap(s.buf) < need {
			grown := make([]byte, len(s.buf), need)
			copy(grown, s.buf)
			s.buf = grown
		}
	}
	return out, nil
}

// Pending is the number of bytes received that do not form a complete frame yet. Non-zero when
// the stream closes means the stream was cut in the middle of a frame.
func (s *Splitter) Pending() int { return len(s.buf) }

// Close reports ErrTruncated when the stream ended in the middle of a frame, or the error
// that broke the splitter.
func (s *Splitter) Close() error {
	if s.err != nil {
		return s.err
	}
	if len(s.buf) > 0 {
		return fmt.Errorf("%w: the stream ended %d bytes into a frame", ErrTruncated, len(s.buf))
	}
	return nil
}

// FrameReader reads frames from an io.Reader one at a time.
type FrameReader struct {
	r   io.Reader
	hdr [HeaderLen]byte
}

// NewFrameReader returns a reader of frames from r.
func NewFrameReader(r io.Reader) *FrameReader { return &FrameReader{r: r} }

// Next reads the next frame. It returns io.EOF when r ends cleanly between two frames, and an
// error wrapping ErrTruncated when r ends inside a frame. The header is checked before the
// payload is read.
func (fr *FrameReader) Next() (Frame, error) {
	n, err := io.ReadFull(fr.r, fr.hdr[:])
	if err != nil {
		if err == io.EOF {
			return Frame{}, io.EOF
		}
		if err == io.ErrUnexpectedEOF {
			return Frame{}, fmt.Errorf("%w: the stream ended %d bytes into a frame header", ErrTruncated, n)
		}
		return Frame{}, err
	}
	h, err := ParseHeader(fr.hdr[:])
	if err != nil {
		return Frame{}, err
	}
	// The buffer grows with the bytes received, not with the length the header declares:
	// a peer that sends headers only must not make the reader hold MaxFrameLen each.
	var buf bytes.Buffer
	buf.Grow(int(min(h.Length, readerInitialPayload)))
	if n, err := io.CopyN(&buf, fr.r, int64(h.Length)); err != nil {
		if err == io.EOF {
			return Frame{}, fmt.Errorf("%w: the stream ended %d bytes into a payload of %d", ErrTruncated, n, h.Length)
		}
		return Frame{}, err
	}
	return Frame{Type: h.Type, Flags: h.Flags, Payload: buf.Bytes()}, nil
}
