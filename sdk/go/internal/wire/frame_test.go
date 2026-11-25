// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math/rand/v2"
	"runtime"
	"testing"
)

func TestHeaderLayout(t *testing.T) {
	b := EncodeFrame(TypeRun, FlagRequestedAck, []byte{0x58, 0x01})
	want := []byte{0x04, 0x11, 0x80, 0x00, 0, 0, 0, 2, 0x58, 0x01}
	if !bytes.Equal(b, want) {
		t.Fatalf("frame %x, want %x", b, want)
	}
	h, err := ParseHeader(b)
	if err != nil {
		t.Fatal(err)
	}
	if h != (FrameHeader{Type: TypeRun, Flags: FlagRequestedAck, Length: 2}) || !h.RequestedAck() {
		t.Fatalf("header %+v", h)
	}
}

func TestHeaderReservedBitsAreRefused(t *testing.T) {
	for bit := 0; bit < 15; bit++ {
		b := EncodeFrame(TypeEnd, 1<<bit, nil)
		if _, err := ParseHeader(b); !errors.Is(err, ErrReservedFlags) {
			t.Errorf("flag bit %d: %v, want ErrReservedFlags", bit+32, err)
		}
		if _, err := DecodeFrames(b); !errors.Is(err, ErrReservedFlags) {
			t.Errorf("DecodeFrames, flag bit %d: %v", bit+32, err)
		}
		var s Splitter
		if _, err := s.Push(b); !errors.Is(err, ErrReservedFlags) {
			t.Errorf("Splitter, flag bit %d: %v", bit+32, err)
		}
	}
}

func overLimitHeader() []byte {
	return AppendHeader(nil, FrameHeader{Type: TypeInput, Length: MaxFrameLen + 1})
}

func TestLengthLimitIsCheckedAtTheHeader(t *testing.T) {
	if _, err := ParseHeader(AppendHeader(nil, FrameHeader{Type: TypeInput, Length: MaxFrameLen})); err != nil {
		t.Fatalf("exactly the limit: %v", err)
	}
	if MaxFrameLen != 16*1024*1024 {
		t.Fatalf("MaxFrameLen = %d, the specification says 16 MiB", MaxFrameLen)
	}
	hdr := overLimitHeader()
	if _, err := ParseHeader(hdr); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("over the limit: %v", err)
	}
	// Refused with only the 8 header bytes, without waiting for any payload.
	if _, err := DecodeFrames(hdr); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("DecodeFrames: %v", err)
	}
	var s Splitter
	if _, err := s.Push(hdr); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("Splitter: %v", err)
	}
	if _, err := NewFrameReader(bytes.NewReader(hdr)).Next(); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("FrameReader: %v", err)
	}
	huge := AppendHeader(nil, FrameHeader{Type: TypeInput, Length: 0xFFFFFFFF})
	if _, err := ParseHeader(huge); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("4 GiB: %v", err)
	}
}

func TestDecodeFramesRefusesTrailingBytes(t *testing.T) {
	whole := append(EncodeFrame(TypeEnd, 0, nil), EncodeFrame(TypeRun, 0, []byte{0x58, 1})...)
	for cut := 1; cut < len(whole); cut++ {
		if cut == HeaderLen {
			continue // a whole End frame
		}
		frames, err := DecodeFrames(whole[:cut])
		if !errors.Is(err, ErrTruncated) {
			t.Errorf("cut at %d: %d frames, %v, want ErrTruncated", cut, len(frames), err)
		}
	}
	frames, err := DecodeFrames(whole)
	if err != nil || len(frames) != 2 {
		t.Fatalf("%d frames, %v", len(frames), err)
	}
	if frames, err := DecodeFrames(nil); err != nil || len(frames) != 0 {
		t.Fatalf("empty body: %d frames, %v", len(frames), err)
	}
	if _, err := ParseHeader(whole[:7]); !errors.Is(err, ErrTruncated) {
		t.Fatalf("7-byte header: %v", err)
	}
}

// sampleStream is a stream of frames of varied sizes, including an empty payload and one whose
// length needs a multi-byte varint inside.
func sampleStream() ([]byte, []Frame) {
	frames := []Frame{
		{Type: TypeStart, Payload: Marshal(&Start{ID: bytes.Repeat([]byte{0x11}, 24), KnownEntries: 3})},
		{Type: TypeInput, Payload: Marshal(&Input{Value: InlineValue([]byte(`"req"`))})},
		{Type: TypeEnd},
		{Type: TypeRun, Flags: FlagRequestedAck, Payload: Marshal(&Run{ResultCompletionID: 1, Name: "charge"})},
		{Type: TypeSetState, Payload: Marshal(&SetState{Key: []byte("k"), Value: InlineValue(bytes.Repeat([]byte("v"), 70000))})},
		{Type: 0xFC01, Payload: []byte("opaque")},
		{Type: TypeKeepalive},
	}
	var body []byte
	for _, f := range frames {
		body = f.AppendTo(body)
	}
	return body, frames
}

func equalFrames(t *testing.T, got, want []Frame) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%d frames, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Type != want[i].Type || got[i].Flags != want[i].Flags || !bytes.Equal(got[i].Payload, want[i].Payload) {
			t.Fatalf("frame %d: %s/%04x/%d bytes, want %s/%04x/%d bytes", i,
				got[i].Type, got[i].Flags, len(got[i].Payload), want[i].Type, want[i].Flags, len(want[i].Payload))
		}
	}
}

func TestSplitterByteByByte(t *testing.T) {
	body, want := sampleStream()
	var s Splitter
	var got []Frame
	for i := range body {
		fs, err := s.Push(body[i : i+1])
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, fs...)
		if len(fs) == 0 && s.Pending() == 0 && i != len(body)-1 {
			t.Fatalf("byte %d: nothing pending and no frame", i)
		}
	}
	equalFrames(t, got, want)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSplitterRandomChunks(t *testing.T) {
	body, want := sampleStream()
	rng := rand.New(rand.NewPCG(1, 2))
	for round := 0; round < 200; round++ {
		var s Splitter
		var got []Frame
		for at := 0; at < len(body); {
			n := 1 + rng.IntN(1+rng.IntN(len(body)))
			n = min(n, len(body)-at)
			// The splitter must not keep the caller's buffer: scribble over it after Push.
			chunk := append([]byte{}, body[at:at+n]...)
			fs, err := s.Push(chunk)
			if err != nil {
				t.Fatal(err)
			}
			clear(chunk)
			got = append(got, fs...)
			at += n
		}
		equalFrames(t, got, want)
		if s.Pending() != 0 {
			t.Fatalf("round %d: %d pending", round, s.Pending())
		}
	}
}

func TestSplitterReportsACutStream(t *testing.T) {
	body, _ := sampleStream()
	var s Splitter
	if _, err := s.Push(body[:len(body)-3]); err != nil {
		t.Fatal(err)
	}
	if s.Pending() == 0 {
		t.Fatal("nothing pending in the middle of a frame")
	}
	if err := s.Close(); !errors.Is(err, ErrTruncated) {
		t.Fatalf("Close: %v", err)
	}
}

func TestSplitterErrorIsSticky(t *testing.T) {
	var s Splitter
	if _, err := s.Push(EncodeFrame(TypeEnd, 0x0001, nil)); !errors.Is(err, ErrReservedFlags) {
		t.Fatal(err)
	}
	if _, err := s.Push(EncodeFrame(TypeEnd, 0, nil)); !errors.Is(err, ErrReservedFlags) {
		t.Fatalf("after an error: %v", err)
	}
	if err := s.Close(); !errors.Is(err, ErrReservedFlags) {
		t.Fatalf("Close: %v", err)
	}
}

func TestSplitterKeepsFramesBeforeABadHeader(t *testing.T) {
	good := EncodeFrame(TypeEnd, 0, nil)
	var s Splitter
	frames, err := s.Push(append(append([]byte{}, good...), overLimitHeader()...))
	if !errors.Is(err, ErrFrameTooLarge) || len(frames) != 1 {
		t.Fatalf("%d frames, %v", len(frames), err)
	}
}

// oneByteReader returns at most one byte per Read.
type oneByteReader struct{ r io.Reader }

func (o oneByteReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return o.r.Read(p)
}

func TestFrameReader(t *testing.T) {
	body, want := sampleStream()
	for _, r := range []io.Reader{bytes.NewReader(body), oneByteReader{bytes.NewReader(body)}} {
		fr := NewFrameReader(r)
		var got []Frame
		for {
			f, err := fr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, f)
		}
		equalFrames(t, got, want)
	}
	for _, cut := range []int{3, HeaderLen + 1, len(body) - 1} {
		fr := NewFrameReader(bytes.NewReader(body[:cut]))
		var err error
		for err == nil {
			_, err = fr.Next()
		}
		if !errors.Is(err, ErrTruncated) {
			t.Errorf("cut at %d: %v, want ErrTruncated", cut, err)
		}
	}
}

func TestEncodeMessageWritesTheLength(t *testing.T) {
	m := &SetState{Key: []byte("k"), Value: InlineValue(bytes.Repeat([]byte("x"), 300))}
	b := EncodeMessage(m, 0)
	h, err := ParseHeader(b)
	if err != nil || h.Type != TypeSetState || int(h.Length) != len(b)-HeaderLen {
		t.Fatalf("%+v %v", h, err)
	}
	prefix := []byte("prefix")
	b2 := AppendMessage(append([]byte{}, prefix...), m, FlagRequestedAck)
	if !bytes.Equal(b2[:len(prefix)], prefix) || !bytes.Equal(b2[len(prefix)+4:], b[4:]) {
		t.Fatal("AppendMessage does not append the same frame")
	}
}

// The reader's memory grows with the payload bytes received, not with the length the
// header declares: 64 streams each sending only a header that declares 16 MiB used to
// hold 1 GiB.
//
// Must see: a header declaring MaxFrameLen followed by 10 bytes and the end of the stream
// allocates well under 1 MiB, and is a truncated frame.
func TestFrameReaderDoesNotAllocateTheDeclaredLength(t *testing.T) {
	in := binary.BigEndian.AppendUint32([]byte{0, 0, 0, 0}, MaxFrameLen) // Start, no flags
	in = append(in, make([]byte, 10)...)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := NewFrameReader(bytes.NewReader(in)).Next()
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("error %v, want ErrTruncated", err)
	}
	if got := after.TotalAlloc - before.TotalAlloc; got > 1<<20 {
		t.Fatalf("a header declaring %d bytes and 10 bytes of payload allocated %d bytes", MaxFrameLen, got)
	}
	// A whole frame still reads back the same, at the largest length too.
	big := EncodeFrame(TypeRun, 0, bytes.Repeat([]byte{7}, MaxFrameLen))
	f, err := NewFrameReader(bytes.NewReader(big)).Next()
	if err != nil || len(f.Payload) != MaxFrameLen || f.Payload[MaxFrameLen-1] != 7 {
		t.Fatalf("a %d-byte frame: %v, %d bytes", MaxFrameLen, err, len(f.Payload))
	}
}
