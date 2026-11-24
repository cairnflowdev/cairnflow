// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// tracesDir holds the language-neutral conformance traces (conformance/README.md).
const tracesDir = "../../../../conformance/traces"

type traceFrame struct {
	From    string `json:"from"`
	Channel string `json:"channel"`
	Type    string `json:"type"`
	Name    string `json:"name"`
	Flags   string `json:"flags"`
	Hex     string `json:"hex"`
}

type traceEntry struct {
	Index      int    `json:"index"`
	Type       string `json:"type"`
	Name       string `json:"name"`
	PayloadHex string `json:"payload_hex"`
}

type trace struct {
	Name        string `json:"name"`
	Connections []struct {
		Frames []traceFrame `json:"frames"`
	} `json:"connections"`
	Journal []traceEntry `json:"journal"`
}

func loadTraces(t testing.TB) map[string]trace {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(tracesDir, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatalf("no trace in %s", tracesDir)
	}
	out := make(map[string]trace, len(files))
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var tr trace
		if err := json.Unmarshal(raw, &tr); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		out[filepath.Base(f)] = tr
	}
	return out
}

func parseCode(t testing.TB, s string) uint16 {
	t.Helper()
	v, err := strconv.ParseUint(s, 0, 16)
	if err != nil {
		t.Fatalf("code %q: %v", s, err)
	}
	return uint16(v)
}

func unhex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("hex %q: %v", s, err)
	}
	return b
}

// roundTrip decodes payload as t, re-encodes it and requires the same bytes.
func roundTrip(t *testing.T, where string, mt MessageType, payload []byte) {
	t.Helper()
	m, err := Decode(mt, payload)
	if err != nil {
		t.Errorf("%s: decode %s: %v", where, mt, err)
		return
	}
	if m.Type() != mt {
		t.Errorf("%s: decoded %T reports type %s, want %s", where, m, m.Type(), mt)
	}
	if got := Marshal(m); !bytes.Equal(got, payload) {
		t.Errorf("%s: %s re-encodes to\n  %x\nwant\n  %x", where, mt, got, payload)
	}
}

// Every frame and every journal entry of every trace decodes into its struct and re-encodes to
// exactly the same bytes. Runtime frames come from prost and SDK frames from the reference SDK,
// both in canonical form, so both directions must round-trip.
func TestTracesRoundTrip(t *testing.T) {
	frames, entries := 0, 0
	seen := map[MessageType]bool{}
	for file, tr := range loadTraces(t) {
		for ci, conn := range tr.Connections {
			for fi, f := range conn.Frames {
				where := fmt.Sprintf("%s conn %d frame %d", file, ci, fi)
				raw := unhex(t, f.Hex)
				h, err := ParseHeader(raw)
				if err != nil {
					t.Errorf("%s: header: %v", where, err)
					continue
				}
				if want := MessageType(parseCode(t, f.Type)); h.Type != want {
					t.Errorf("%s: header type %s, trace says %s", where, h.Type, want)
				}
				if want := parseCode(t, f.Flags); h.Flags != want {
					t.Errorf("%s: header flags 0x%04x, trace says 0x%04x", where, h.Flags, want)
				}
				if h.Type.String() != f.Name {
					t.Errorf("%s: name %q, trace says %q", where, h.Type.String(), f.Name)
				}
				if h.RequestedAck() && !h.Type.MayRequestAck() {
					t.Errorf("%s: REQUESTED_ACK on %s", where, h.Type)
				}
				if (f.Channel == "side") != h.Type.IsSideChannel() {
					t.Errorf("%s: channel %q but IsSideChannel = %v", where, f.Channel, h.Type.IsSideChannel())
				}
				got, err := DecodeFrames(raw)
				if err != nil || len(got) != 1 {
					t.Errorf("%s: DecodeFrames = %d frames, %v", where, len(got), err)
					continue
				}
				roundTrip(t, where, h.Type, got[0].Payload)
				m, err := DecodeFrame(got[0])
				if err == nil {
					if again := EncodeMessage(m, h.Flags); !bytes.Equal(again, raw) {
						t.Errorf("%s: frame re-encodes to %x, want %x", where, again, raw)
					}
				}
				seen[h.Type] = true
				frames++
			}
		}
		for _, e := range tr.Journal {
			where := fmt.Sprintf("%s journal %d", file, e.Index)
			mt := MessageType(parseCode(t, e.Type))
			if mt.String() != e.Name {
				t.Errorf("%s: name %q, trace says %q", where, mt.String(), e.Name)
			}
			roundTrip(t, where, mt, unhex(t, e.PayloadHex))
			entries++
		}
	}
	t.Logf("%d frames, %d journal entries, %d message types", frames, entries, len(seen))
}

// The suspension trees of conformance/suspension-vectors.json decode and re-encode to the same
// bytes.
func TestSuspensionVectorsRoundTrip(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(tracesDir, "..", "suspension-vectors.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Vectors []struct {
			Name string `json:"name"`
			Hex  string `json:"hex"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Vectors) == 0 {
		t.Fatal("no vector")
	}
	for _, v := range doc.Vectors {
		roundTrip(t, v.Name, TypeSuspension, unhex(t, v.Hex))
	}
}

// A whole connection's runtime frames, concatenated, split back into the same frames, through
// DecodeFrames and through a Splitter fed one byte at a time.
func TestTraceBodiesSplit(t *testing.T) {
	for file, tr := range loadTraces(t) {
		for ci, conn := range tr.Connections {
			var body []byte
			var want [][]byte
			for _, f := range conn.Frames {
				if f.From != "runtime" {
					continue
				}
				raw := unhex(t, f.Hex)
				body = append(body, raw...)
				want = append(want, raw)
			}
			got, err := DecodeFrames(body)
			if err != nil || len(got) != len(want) {
				t.Fatalf("%s conn %d: DecodeFrames = %d frames, %v; want %d", file, ci, len(got), err, len(want))
			}
			var s Splitter
			var split []Frame
			for i := range body {
				fs, err := s.Push(body[i : i+1])
				if err != nil {
					t.Fatalf("%s conn %d: %v", file, ci, err)
				}
				split = append(split, fs...)
			}
			if s.Pending() != 0 || s.Close() != nil || len(split) != len(want) {
				t.Fatalf("%s conn %d: splitter gave %d frames, %d pending", file, ci, len(split), s.Pending())
			}
			for i := range want {
				if !bytes.Equal(got[i].AppendTo(nil), want[i]) || !bytes.Equal(split[i].AppendTo(nil), want[i]) {
					t.Errorf("%s conn %d frame %d differs", file, ci, i)
				}
			}
		}
	}
}
