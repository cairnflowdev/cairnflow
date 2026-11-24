// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

// seedFrames adds every frame of every conformance trace as a seed.
func seedFrames(f *testing.F, add func(raw []byte)) {
	for _, tr := range loadTraces(f) {
		for _, conn := range tr.Connections {
			var body []byte
			for _, fr := range conn.Frames {
				raw := unhex(f, fr.Hex)
				add(raw)
				body = append(body, raw...)
			}
			add(body)
		}
	}
}

// FuzzSplitter feeds arbitrary bytes to a Splitter in chunks whose sizes come from the input,
// and checks it agrees with DecodeFrames on the same bytes: same frames, same error class.
func FuzzSplitter(f *testing.F) {
	seedFrames(f, func(raw []byte) { f.Add(raw, []byte{1, 7, 3}) })
	f.Add(overLimitHeader(), []byte{3})
	f.Add(EncodeFrame(TypeEnd, 0x0001, nil), []byte{})
	f.Fuzz(func(t *testing.T, data, sizes []byte) {
		want, wantErr := DecodeFrames(data)
		var s Splitter
		var got []Frame
		var gotErr error
		for at, i := 0, 0; at < len(data); i++ {
			n := len(data) - at
			if len(sizes) > 0 {
				n = min(n, 1+int(sizes[i%len(sizes)]))
			}
			fs, err := s.Push(data[at : at+n])
			got = append(got, fs...)
			at += n
			if err != nil {
				gotErr = err
				break
			}
		}
		if gotErr == nil {
			gotErr = s.Close()
		}
		if len(got) != len(want) {
			t.Fatalf("splitter: %d frames (%v), DecodeFrames: %d frames (%v)", len(got), gotErr, len(want), wantErr)
		}
		for i := range want {
			if got[i].Type != want[i].Type || got[i].Flags != want[i].Flags || !bytes.Equal(got[i].Payload, want[i].Payload) {
				t.Fatalf("frame %d differs", i)
			}
		}
		for _, sentinel := range []error{ErrReservedFlags, ErrFrameTooLarge, ErrTruncated} {
			if errors.Is(gotErr, sentinel) != errors.Is(wantErr, sentinel) {
				t.Fatalf("splitter error %v, DecodeFrames error %v", gotErr, wantErr)
			}
		}
		if (gotErr == nil) != (wantErr == nil) {
			t.Fatalf("splitter error %v, DecodeFrames error %v", gotErr, wantErr)
		}
	})
}

// FuzzDecode decodes arbitrary payloads as every message type: decoding never panics, and
// decode → encode → decode is stable (the second encoding equals the first, the second decode
// equals the first).
func FuzzDecode(f *testing.F) {
	seedFrames(f, func(raw []byte) {
		frames, err := DecodeFrames(raw)
		if err != nil {
			return
		}
		for _, fr := range frames {
			f.Add(uint16(fr.Type), fr.Payload)
		}
	})
	f.Add(uint16(TypeSuspension), []byte{0x22, 0x02, 0x22, 0x00})
	f.Fuzz(func(t *testing.T, code uint16, payload []byte) {
		m1, err := Decode(MessageType(code), payload)
		if err != nil {
			if !errors.Is(err, ErrMalformed) && !errors.Is(err, ErrUnknownType) {
				t.Fatalf("unexpected error class: %v", err)
			}
			return
		}
		b1 := Marshal(m1)
		m2, err := Decode(MessageType(code), b1)
		if err != nil {
			t.Fatalf("re-encoded %x does not decode: %v", b1, err)
		}
		b2 := Marshal(m2)
		if !bytes.Equal(b1, b2) {
			t.Fatalf("unstable encoding: %x then %x", b1, b2)
		}
		// m1 may differ from m2 (an empty field written in a non-canonical input decodes to an
		// empty slice, its canonical re-encoding to nil); from canonical bytes on, decoding is
		// a fixed point.
		m3, err := Decode(MessageType(code), b2)
		if err != nil || !reflect.DeepEqual(m2, m3) {
			t.Fatalf("unstable decoding (%v):\n%#v\n%#v", err, m2, m3)
		}
	})
}

// FuzzDecodeValue decodes arbitrary bytes as a standalone Value: never panics, stable round
// trip, and Check never panics either.
func FuzzDecodeValue(f *testing.F) {
	f.Add(MarshalValue(&Value{Content: []byte("x"), Segments: []SegmentRef{{Sha256: sha32(), Len: 3, Gen: 9}}, Len: 4}))
	f.Add([]byte{0x0a, 0x00})
	f.Fuzz(func(t *testing.T, b []byte) {
		v, err := UnmarshalValue(b)
		if err != nil {
			return
		}
		_ = v.Check()
		b1 := MarshalValue(v)
		v2, err := UnmarshalValue(b1)
		if err != nil || !bytes.Equal(MarshalValue(v2), b1) {
			t.Fatalf("unstable: %x, %v", b1, err)
		}
	})
}
