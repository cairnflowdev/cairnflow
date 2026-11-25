// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

// Wire v5 and v6 together, on the real runtime, in the verified configuration (both need
// it: IdentityKeys, and SegmentOrigins naming the node): the replay cache is on (REQUEST_RESPONSE)
// and the invocation carries a segmented value (a Run result larger than the sealing
// threshold, stored as refs to segments) across many turns. Once the entry with the refs
// is in the prefix the SDK holds, a turn carries only the entries after it (a delta); the
// SDK must assemble the refs of its cached raw frames from the read URLs the runtime
// signs for the whole journal. The segment cache is off, so every turn downloads what it
// assembles, and the downloads of such a delta turn prove it read the cached refs.

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	cairn "github.com/cairnflowdev/cairnflow/sdk/go"
	"github.com/cairnflowdev/cairnflow/sdk/go/client"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

// noise is a deterministic string that does not compress away.
func noise(n int, seed uint32) string {
	x := seed
	var b strings.Builder
	b.Grow(n)
	for range n {
		x = x*1103515245 + 12345
		b.WriteByte(byte('a' + (x>>16)%26))
	}
	return b.String()
}

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

type digest struct {
	Len int    `json:"len"`
	SHA string `json:"sha"`
}

const historyTicks = 5

// history fetches a large value in a Run, then goes through several more turns (each Run
// result needs the runtime's answer: a new REQUEST_RESPONSE turn) before using it.
func history(ctx cairn.ObjectContext, n int) (digest, error) {
	big, err := cairn.Run(ctx, "fetch", func(cairn.RunContext) (string, error) { return noise(n, 6), nil })
	if err != nil {
		return digest{}, err
	}
	for i := range historyTicks {
		if _, err := cairn.Run(ctx, fmt.Sprintf("tick-%d", i), func(cairn.RunContext) (int, error) { return i, nil }); err != nil {
			return digest{}, err
		}
	}
	if err := cairn.Set(ctx, "last", big[len(big)-16:]); err != nil {
		return digest{}, err
	}
	return digest{Len: len(big), SHA: sha256Hex(big)}, nil
}

var histories = cairn.NewObject("GoHistory", cairn.Exclusive("chat", history))

// turnRecord is what one /invoke request carried and how it was answered.
type turnRecord struct {
	replayFrom uint32
	// segmented are the journal indexes of the entries in the body that hold a
	// segmented value.
	segmented []int
	status    int
	cacheHdr  string
	downloads int64
}

// turnRecorder wraps the endpoint and records every /invoke request.
type turnRecorder struct {
	inner     http.Handler
	downloads *atomic.Int64
	mu        sync.Mutex
	turns     []turnRecord
	err       error
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func (r *turnRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !strings.Contains(req.URL.Path, "/invoke/") {
		r.inner.ServeHTTP(w, req)
		return
	}
	raw, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req.Body = io.NopCloser(bytes.NewReader(raw))
	rec, perr := inspect(raw, req.Header.Get("Content-Encoding") == "gzip")
	before := r.downloads.Load()
	sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
	r.inner.ServeHTTP(sw, req)
	rec.status = sw.status
	rec.cacheHdr = sw.Header().Get("cairn-replay-cache")
	rec.downloads = r.downloads.Load() - before
	r.mu.Lock()
	defer r.mu.Unlock()
	if perr != nil && r.err == nil {
		r.err = perr
	}
	r.turns = append(r.turns, rec)
}

// inspect decodes the Start of a turn and finds the entries holding a segmented value.
func inspect(body []byte, gzipped bool) (turnRecord, error) {
	var rec turnRecord
	if gzipped {
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return rec, err
		}
		if body, err = io.ReadAll(zr); err != nil {
			return rec, err
		}
	}
	frames, err := wire.DecodeFrames(body)
	if err != nil || len(frames) == 0 {
		return rec, fmt.Errorf("undecodable turn body: %v", err)
	}
	m, err := wire.DecodeFrame(frames[0])
	if err != nil {
		return rec, err
	}
	start, ok := m.(*wire.Start)
	if !ok {
		return rec, fmt.Errorf("the first frame is %T, not Start", m)
	}
	rec.replayFrom = start.ReplayFrom
	for i, f := range frames[1:] {
		m, err := wire.DecodeFrame(f)
		if err != nil {
			return rec, err
		}
		var v *wire.Value
		switch e := m.(type) {
		case *wire.RunCompletion:
			v = e.Value
		case *wire.GetStateCompletion:
			v = e.Value
		}
		if v != nil && v.IsSegmented() {
			rec.segmented = append(rec.segmented, int(start.ReplayFrom)+i)
		}
	}
	return rec, nil
}

// countingTransport counts the segment downloads.
type countingTransport struct{ n *atomic.Int64 }

func (c countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.n.Add(1)
	return http.DefaultTransport.RoundTrip(r)
}

func TestReplayCacheWithSegmentedValues(t *testing.T) {
	n := startNode(t, "--value-read-path", "proxy")
	var downloads atomic.Int64
	rec := &turnRecorder{downloads: &downloads}
	rec.inner = cairn.HTTPHandler(append(n.verified(t), histories,
		cairn.ValueCacheBytes(0), // every turn downloads what it assembles
		cairn.WithSegmentTransport(countingTransport{&downloads}))...)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	reg := n.register(t, serveOn(t, ln, rec))
	if reg.ProtocolMode != rr || reg.WireVersion != 6 {
		t.Fatalf("settled on %s v%d, want %s v6", reg.ProtocolMode, reg.WireVersion, rr)
	}
	const size = 600 << 10
	got, err := client.Call[digest](bounded(t), n.api.Object("GoHistory", "h-"+randomHex(3)), "chat", size)
	if err != nil {
		t.Fatal(err)
	}
	want := noise(size, 6)
	if got != (digest{Len: len(want), SHA: sha256Hex(want)}) {
		t.Fatalf("result %+v, want len %d sha %s", got, len(want), sha256Hex(want))
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.err != nil {
		t.Fatal(rec.err)
	}
	var log []string
	for _, tr := range rec.turns {
		log = append(log, fmt.Sprintf("{from %d, segmented %v, status %d, cache %q, downloads %d}",
			tr.replayFrom, tr.segmented, tr.status, tr.cacheHdr, tr.downloads))
	}
	t.Logf("turns: %s", strings.Join(log, " "))
	// The first entry holding a segmented value, seen in some turn.
	sealed := -1
	for _, tr := range rec.turns {
		if len(tr.segmented) > 0 {
			sealed = tr.segmented[0]
			break
		}
	}
	if sealed < 0 {
		t.Fatal("no turn carried a segmented value: the Run result was not sealed")
	}
	// A delta turn whose cached prefix holds that entry, answered, assembling by download.
	proven := 0
	for _, tr := range rec.turns {
		if int(tr.replayFrom) > sealed && tr.status == http.StatusOK {
			if tr.downloads == 0 {
				t.Fatalf("a delta turn from %d (refs at entry %d in the cached prefix) downloaded nothing", tr.replayFrom, sealed)
			}
			proven++
		}
	}
	if proven < 2 {
		t.Fatalf("%d delta turns had the refs in their cached prefix, want at least 2", proven)
	}
}
