// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

// Segmented values (wire v6) end to end through the endpoint, for what the conformance
// traces do not cover: limits, failures, the rules for reusing refs, BIDI_STREAM.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/bidi"
	machine "github.com/cairnflowdev/cairnflow/sdk/go/internal/turn"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/values"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

// readURL is the shape of a signed read URL: its query is the capability.
const readURL = "https://values.example.com/v1/seg?X-Amz-Signature=SECRET-CAPABILITY"

// history is a JSON string value split into one sealed segment and a tail.
var history = []byte(`"` + strings.Repeat("a", 39) + strings.Repeat("a", 10) + `"`)

func historySegment() ([]byte, wire.SegmentRef) {
	seg := history[:40]
	sum := sha256.Sum256(seg)
	return seg, wire.SegmentRef{Sha256: sum[:], Len: 40, Gen: 7}
}

func historyValue() *wire.Value {
	_, r := historySegment()
	return &wire.Value{Content: history[40:], Segments: []wire.SegmentRef{r}, Len: uint64(len(history))}
}

// segmentStore serves segments by URL in process and counts the downloads.
type segmentStore struct {
	mu    sync.Mutex
	byURL map[string][]byte
	gets  atomic.Int64
}

func newSegmentStore() *segmentStore {
	seg, _ := historySegment()
	return &segmentStore{byURL: map[string][]byte{readURL: seg}}
}

func (s *segmentStore) RoundTrip(r *http.Request) (*http.Response, error) {
	s.gets.Add(1)
	s.mu.Lock()
	b, ok := s.byURL[r.URL.String()]
	s.mu.Unlock()
	if !ok {
		return &http.Response{StatusCode: http.StatusForbidden, Body: io.NopCloser(bytes.NewReader(nil)), Request: r}, nil
	}
	return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(b)), ContentLength: int64(len(b)), Request: r}, nil
}

func historyURLs() []wire.SegmentURL {
	_, r := historySegment()
	return []wire.SegmentURL{{Sha256: r.Sha256, Gen: r.Gen, URL: readURL}}
}

// readState: Start (with the read URL), Input, GetState(history) and its segmented
// completion; then more entries.
func readState(more ...wire.Message) []wire.Message {
	return append([]wire.Message{
		input(),
		&wire.GetState{Key: []byte("history"), ResultCompletionID: 1, Name: "history"},
		&wire.GetStateCompletion{ID: 1, Result: wire.Result{Value: historyValue()}},
	}, more...)
}

// invokeV posts one REQUEST_RESPONSE turn at wire v and decodes the response.
func invokeV(t *testing.T, s *testServer, handler string, v int, start *wire.Start, entries ...wire.Message) []wire.Message {
	t.Helper()
	if start == nil {
		start = &wire.Start{ID: []byte{0xca, 0x1f}, SegmentURLs: historyURLs()}
	}
	start.KnownEntries = uint32(len(entries))
	body := wire.EncodeMessage(start, 0)
	for _, e := range entries {
		body = wire.AppendMessage(body, e, 0)
	}
	res, out := s.do(t, http.MethodPost, "/invoke/Values/"+handler, body, invokeHeaders(v))
	if res.StatusCode != 200 {
		t.Fatalf("status %d: %s", res.StatusCode, out)
	}
	return decodeFrames(t, out)
}

var ranValues atomic.Int64

func valuesService() *ServiceDef {
	appendTo := func(ctx ObjectContext, suffix string, between func(ObjectContext) error) (string, error) {
		ranValues.Add(1)
		h, _, err := Get[string](ctx, "history")
		if err != nil {
			return "", err
		}
		if between != nil {
			if err := between(ctx); err != nil {
				return "", err
			}
		}
		if err := Set(ctx, "history", h+suffix); err != nil {
			return "", err
		}
		return "ok", nil
	}
	return NewObject("Values",
		Handler("echo", NoInput(func(ctx ObjectContext) (string, error) {
			ranValues.Add(1)
			h, _, err := Get[string](ctx, "history")
			return h, err
		})),
		Handler("extend", NoInput(func(ctx ObjectContext) (string, error) { return appendTo(ctx, "b", nil) })),
		Handler("extendTwice", NoInput(func(ctx ObjectContext) (string, error) {
			if _, err := appendTo(ctx, "b", nil); err != nil {
				return "", err
			}
			// The second write starts with the first: the refs it reuses are the ones the
			// first write sent.
			return "ok", Set(ctx, "history", strings.Repeat("a", 49)+"bc")
		})),
		Handler("clearBetween", NoInput(func(ctx ObjectContext) (string, error) {
			return appendTo(ctx, "b", func(ctx ObjectContext) error { return Clear(ctx, "history") })
		})),
		Handler("replace", NoInput(func(ctx ObjectContext) (string, error) {
			if _, _, err := Get[string](ctx, "history"); err != nil {
				return "", err
			}
			return "ok", Set(ctx, "history", "other")
		})),
		Handler("signal", NoInput(func(ctx ObjectContext) (string, error) {
			return Signal[string](ctx, "s").Result()
		})),
	)
}

func valuesHandler(t *testing.T, name string) *HandlerDef {
	h, ok := valuesService().Handler(name)
	if !ok {
		t.Fatalf("no handler %s", name)
	}
	return h
}

// newValuesServer serves valuesService reading segments from readURL's origin only (an
// endpoint without request identity serves wire v6 only with SegmentOrigins).
func newValuesServer(t *testing.T, opts ...ServeOption) (*testServer, *segmentStore) {
	return newBareValuesServer(t, append([]ServeOption{SegmentOrigins("https://values.example.com")}, opts...)...)
}

// newBareValuesServer is newValuesServer without SegmentOrigins.
func newBareValuesServer(t *testing.T, opts ...ServeOption) (*testServer, *segmentStore) {
	store := newSegmentStore()
	return newTestServer(t, HTTPHandler(append([]ServeOption{valuesService(), WithSegmentTransport(store)}, opts...)...)), store
}

func setStateOf(t *testing.T, m wire.Message) *wire.Value {
	t.Helper()
	s, ok := m.(*wire.SetState)
	if !ok {
		t.Fatalf("got %T, want SetState", m)
	}
	return s.Value
}

func TestSegmentedSetReusesTheRefsOfTheValueRead(t *testing.T) {
	s, store := newValuesServer(t)
	msgs := invokeV(t, s, "extend", 6, nil, readState()...)
	wantKinds(t, msgs, "SetState Output End")
	v := setStateOf(t, msgs[0])
	_, r := historySegment()
	if len(v.Segments) != 1 || !bytes.Equal(v.Segments[0].Sha256, r.Sha256) || v.Segments[0].Gen != 7 ||
		string(v.Content) != string(history[40:len(history)-1])+`b"` || v.Len != uint64(len(history)+1) {
		t.Fatalf("SetState value %+v", v)
	}
	if store.gets.Load() != 1 {
		t.Fatalf("%d downloads, want 1", store.gets.Load())
	}
	// The next turn of the endpoint finds the segment in its cache.
	invokeV(t, s, "extend", 6, nil, readState()...)
	if store.gets.Load() != 1 {
		t.Fatalf("%d downloads after a second turn, want 1 (cached)", store.gets.Load())
	}

	// The second write of the turn reuses the refs the first one sent.
	msgs = invokeV(t, s, "extendTwice", 6, nil, readState()...)
	wantKinds(t, msgs, "SetState SetState Output End")
	if v := setStateOf(t, msgs[1]); len(v.Segments) != 1 || string(v.Content) != string(history[40:len(history)-1])+`bc"` {
		t.Fatalf("second SetState %+v", v)
	}
}

func TestSegmentedSetGoesInlineWhenNothingMatches(t *testing.T) {
	s, _ := newValuesServer(t)
	// A write of the key between the read and the set: the refs read are not the row's.
	msgs := invokeV(t, s, "clearBetween", 6, nil, readState()...)
	wantKinds(t, msgs, "ClearState SetState Output End")
	if v := setStateOf(t, msgs[1]); v.IsSegmented() || string(v.Content) != string(history[:len(history)-1])+`b"` {
		t.Fatalf("SetState %+v", v)
	}
	// A value that does not start with the bytes read.
	msgs = invokeV(t, s, "replace", 6, nil, readState()...)
	if v := setStateOf(t, msgs[0]); v.IsSegmented() || string(v.Content) != `"other"` {
		t.Fatalf("SetState %+v", v)
	}
	// A journaled SetState: which refs the earlier run sent is unknown, so the next write
	// goes inline.
	journaled := &wire.SetState{Key: []byte("history"), Name: "history", Value: historyValue()}
	msgs = invokeV(t, s, "extendTwice", 6, nil, readState(journaled)...)
	wantKinds(t, msgs, "SetState Output End")
	if v := setStateOf(t, msgs[0]); v.IsSegmented() {
		t.Fatalf("SetState after a replayed one %+v", v)
	}
	// Settled below v6 (assembled all the same): never refs.
	msgs = invokeV(t, s, "extend", 5, nil, readState()...)
	if v := setStateOf(t, msgs[0]); v.IsSegmented() {
		t.Fatalf("SetState at v5 %+v", v)
	}
	msgs = invokeV(t, s, "echo", 5, nil, readState()...)
	if o := msgs[0].(*wire.Output); o.Result.Value.IsSegmented() {
		t.Fatalf("Output at v5 %+v", o.Result.Value)
	}
}

func TestSegmentedEagerStateIsAssembledButNotReused(t *testing.T) {
	s, _ := newValuesServer(t)
	eager := &wire.GetEagerState{Key: []byte("history"), Name: "history", Result: wire.Result{Value: historyValue()}}
	start := &wire.Start{ID: []byte{0xca, 0x1f}, SegmentURLs: historyURLs(), StateMap: []wire.StateEntry{}, PartialState: true}
	msgs := invokeV(t, s, "extend", 6, start, input(), eager)
	wantKinds(t, msgs, "SetState Output End")
	if v := setStateOf(t, msgs[0]); v.IsSegmented() || string(v.Content) != string(history[:len(history)-1])+`b"` {
		t.Fatalf("SetState after a journaled GetEagerState %+v", v)
	}
}

// captureLogs sends every slog record of the test to a buffer.
func captureLogs(t *testing.T) *bytes.Buffer {
	var buf bytes.Buffer
	var mu sync.Mutex
	before := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	}), &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(before) })
	return &buf
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestSegmentedValueFailuresAreRetriedAndNeverShowTheURL(t *testing.T) {
	logs := captureLogs(t)
	var messages []string
	for name, tc := range map[string]struct {
		bare     bool // no SegmentOrigins of readURL's origin
		opts     []ServeOption
		urls     []wire.SegmentURL
		fragment string
	}{
		"expired URL":     {urls: []wire.SegmentURL{{Sha256: historyURLs()[0].Sha256, Gen: 7, URL: readURL + "-expired"}}, fragment: "a segment could not be read (fetch)"},
		"no URL":          {urls: nil, fragment: "has no read URL"},
		"origin":          {bare: true, opts: []ServeOption{SegmentOrigins("https://other.example.com")}, urls: historyURLs(), fragment: "not an allowed http(s) origin"},
		"value limit":     {opts: []ServeOption{MaxValueBytes(10)}, urls: historyURLs(), fragment: "over the 10-byte value limit"},
		"turn limit":      {opts: []ServeOption{MaxTurnValueBytes(10)}, urls: historyURLs(), fragment: "per-turn assembled-bytes limit"},
		"wrong gen":       {urls: []wire.SegmentURL{{Sha256: historyURLs()[0].Sha256, Gen: 8, URL: readURL}}, fragment: "has no read URL"},
		"origin, allowed": {opts: []ServeOption{SegmentOrigins("https://values.example.com")}, urls: historyURLs()},
	} {
		t.Run(name, func(t *testing.T) {
			newServer := newValuesServer
			if tc.bare {
				newServer = newBareValuesServer
			}
			s, _ := newServer(t, tc.opts...)
			before := ranValues.Load()
			start := &wire.Start{ID: []byte{0xca, 0x1f}, SegmentURLs: tc.urls}
			msgs := invokeV(t, s, "echo", 6, start, readState()...)
			if tc.fragment == "" {
				wantKinds(t, msgs, "Output End")
				return
			}
			wantKinds(t, msgs, "Error")
			e := wantError(t, msgs[0], 500, wire.BehaviorRetry)
			if !strings.HasPrefix(e.Message, "segmented value: ") || !strings.Contains(e.Message, tc.fragment) {
				t.Fatalf("message %q, want %q", e.Message, tc.fragment)
			}
			if ranValues.Load() != before {
				t.Fatal("the handler ran on a value that was not assembled")
			}
			messages = append(messages, e.Message)
		})
	}
	for _, m := range append(messages, logs.String()) {
		if strings.Contains(m, "SECRET") || strings.Contains(m, "values.example.com/v1") {
			t.Fatalf("a read URL leaked: %q", m)
		}
	}
}

func TestSegmentedSignalValueIsNotReadAsItsTail(t *testing.T) {
	s, _ := newValuesServer(t)
	sig := &wire.SignalNotification{Signal: wire.SignalName("s"), Result: wire.Result{Value: historyValue()}}
	msgs := invokeV(t, s, "signal", 6, nil, input(), sig)
	wantKinds(t, msgs, "Error")
	e := wantError(t, msgs[0], 500, wire.BehaviorRetry)
	if !strings.Contains(e.Message, values.ErrNotAssembled) {
		t.Fatalf("message %q", e.Message)
	}
	// Seen by user code, the error is a *SegmentedValueError.
	var got error
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		_, got = Signal[string](ctx, "s").Result()
		return "", got
	}))
	rrTurn(t, h, 6, nil, input(), sig)
	var sve *SegmentedValueError
	if !errors.As(got, &sve) || IsTerminalError(got) {
		t.Fatalf("error %v (%T), want a *SegmentedValueError", got, got)
	}
}

// openValuesStream is openStream with a segmented-value session; check, when set, is the
// turn-opening body gate.
func openValuesStream(t *testing.T, h *HandlerDef, store http.RoundTripper, check func([]byte) bool) (*fakeStream, chan error) {
	t.Helper()
	pr, pw := io.Pipe()
	s := &fakeStream{t: t, body: pw, frames: make(chan wire.Frame, 256), result: make(chan error, 1)}
	cfg := bidi.Config{Turn: machine.Config{
		Wire:   6,
		Now:    func() int64 { return testClock },
		Values: values.NewSession(values.NewCache(1<<20), values.NewFetcher(store, nil), values.Limits{}),
	}}
	if check != nil {
		cfg.CheckOpening, cfg.MaxOpening = check, 1<<20
	}
	var sp wire.Splitter
	done := make(chan error, 1)
	go func() {
		err := bidi.Serve(h.body, pr, func(b []byte) error {
			fs, err := sp.Push(b)
			for _, f := range fs {
				s.frames <- f
			}
			return err
		}, cfg)
		close(s.frames)
		s.result <- err
		done <- err
	}()
	return s, done
}

func TestBidiAssemblesMidTurnSegments(t *testing.T) {
	store := newSegmentStore()
	h := valuesHandler(t, "extend")
	s, _ := openValuesStream(t, h, store, nil)
	s.send(&wire.Start{ID: []byte{1}, KnownEntries: 1}, input())
	s.expect(wire.TypeGetState, wire.TypeSuspension)
	// Mid-turn: the URLs travel in a SegmentUrls frame at the head of the batch.
	s.send(&wire.SegmentUrls{URLs: historyURLs()}, &wire.GetStateCompletion{ID: 1, Result: wire.Result{Value: historyValue()}})
	fs := s.expect(wire.TypeSetState, wire.TypeOutput, wire.TypeEnd)
	v := decodeAs[wire.SetState](t, fs[0]).Value
	if len(v.Segments) != 1 || string(v.Content) != string(history[40:len(history)-1])+`b"` {
		t.Fatalf("SetState %+v", v)
	}
	s.ends()
	if store.gets.Load() != 1 {
		t.Fatalf("%d downloads", store.gets.Load())
	}
}

func TestBidiFailsTheStreamWhenASegmentIsMissing(t *testing.T) {
	logs := captureLogs(t)
	store := newSegmentStore()
	h := valuesHandler(t, "extend")
	s, _ := openValuesStream(t, h, store, nil)
	s.send(&wire.Start{ID: []byte{1}, KnownEntries: 1}, input())
	s.expect(wire.TypeGetState, wire.TypeSuspension)
	s.send(&wire.SegmentUrls{URLs: []wire.SegmentURL{{Sha256: historyURLs()[0].Sha256, Gen: 7, URL: readURL + "-gone"}}},
		&wire.GetStateCompletion{ID: 1, Result: wire.Result{Value: historyValue()}})
	f := s.expect(wire.TypeError)[0]
	e := decodeAs[wire.ErrorMessage](t, f)
	if e.Code != 500 || e.Behavior != wire.BehaviorRetry || strings.Contains(e.Message, "SECRET") {
		t.Fatalf("Error %+v", e)
	}
	s.ends()
	if strings.Contains(logs.String(), "SECRET") {
		t.Fatalf("a read URL leaked into the logs: %s", logs)
	}
}

func TestBidiFetchesNothingBeforeTheOpeningIsVerified(t *testing.T) {
	store := newSegmentStore()
	h := valuesHandler(t, "echo")
	s, done := openValuesStream(t, h, store, func([]byte) bool { return false })
	entries := readState()
	s.send(append([]wire.Message{&wire.Start{ID: []byte{1}, KnownEntries: uint32(len(entries)), SegmentURLs: historyURLs()}}, entries...)...)
	if err := <-done; !errors.Is(err, bidi.ErrRejected) {
		t.Fatalf("Serve: %v, want ErrRejected", err)
	}
	_ = s.body.Close()
	if store.gets.Load() != 0 {
		t.Fatal("a segment was fetched for an unverified opening")
	}
	// The opening that passes is assembled from Start.segment_urls.
	s, _ = openValuesStream(t, h, store, func([]byte) bool { return true })
	s.send(append([]wire.Message{&wire.Start{ID: []byte{1}, KnownEntries: uint32(len(entries)), SegmentURLs: historyURLs()}}, entries...)...)
	fs := s.expect(wire.TypeOutput, wire.TypeEnd)
	if o := decodeAs[wire.Output](t, fs[0]); len(o.Result.Value.Segments) != 1 {
		t.Fatalf("Output %+v", o.Result.Value)
	}
	s.ends()
}

// An endpoint that neither verifies request identity nor lists SegmentOrigins does not
// serve wire v6 and never fetches a segment: whoever reaches it would choose the URLs.
//
// Must see: maxProtocolVersion 5 (the runtime assembles values inline), a v6 turn refused
// with 415, a v5 turn carrying refs fails without any download, MaxProtocolVersion(6) is a
// configuration error; SegmentOrigins or IdentityKeys bring v6 back.
func TestUnverifiedEndpointFetchesNoSegment(t *testing.T) {
	s, store := newBareValuesServer(t)
	if m := manifestOf(t, s); m["maxProtocolVersion"] != float64(5) {
		t.Fatalf("maxProtocolVersion %v, want 5", m["maxProtocolVersion"])
	}
	body := wire.EncodeMessage(&wire.Start{ID: []byte{0xca, 0x1f}, KnownEntries: 3, SegmentURLs: historyURLs()}, 0)
	for _, e := range readState() {
		body = wire.AppendMessage(body, e, 0)
	}
	if res, out := s.do(t, http.MethodPost, "/invoke/Values/echo", body, invokeHeaders(6)); res.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("a v6 turn: %d %s", res.StatusCode, out)
	}
	msgs := invokeV(t, s, "echo", 5, nil, readState()...)
	wantKinds(t, msgs, "Error")
	if store.gets.Load() != 0 {
		t.Fatalf("%d segment downloads on an unverified endpoint without SegmentOrigins", store.gets.Load())
	}
	opts := []ServeOption{valuesService(), MaxProtocolVersion(6)}
	if _, err := newEndpoint(opts); err == nil || !strings.Contains(err.Error(), "SegmentOrigins") {
		t.Fatalf("newEndpoint: %v", err)
	}
	if err := Serve(context.Background(), "127.0.0.1:0", opts...); err == nil {
		t.Fatal("Serve accepted MaxProtocolVersion(6)")
	}
	if _, err := LambdaHandler(opts...); err == nil {
		t.Fatal("LambdaHandler accepted MaxProtocolVersion(6)")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("HTTPHandler accepted MaxProtocolVersion(6)")
			}
		}()
		HTTPHandler(opts...)
	}()
	for name, extra := range map[string]ServeOption{
		"SegmentOrigins": SegmentOrigins("https://values.example.com"),
		"IdentityKeys":   IdentityKeys(newSigner(t).kid),
	} {
		e, err := newEndpoint([]ServeOption{valuesService(), MaxProtocolVersion(6), extra})
		if err != nil || !strings.Contains(string(e.manifest), `"maxProtocolVersion":6`) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

// The reply to a failed segment read is the same whatever the target answered, and the
// detail goes to the local log, without the URL.
//
// Must see: one message for a body of the wrong length, a 403 and a closed port; the log
// says what happened; no URL in either.
func TestSegmentFailuresTellTheCallerNothing(t *testing.T) {
	logs := captureLogs(t)
	internal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admin" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = io.WriteString(w, "internal-secret-data-of-length-38-byte")
	}))
	defer internal.Close()
	closed := httptest.NewServer(http.NotFoundHandler())
	closedURL := closed.URL
	closed.Close()
	s := newTestServer(t, HTTPHandler(valuesService(), SegmentOrigins(internal.URL, closedURL)))
	var messages []string
	for _, u := range []string{internal.URL + "/meta?SECRET", internal.URL + "/admin?SECRET", closedURL + "/x?SECRET"} {
		sha := make([]byte, 32)
		val := &wire.Value{Segments: []wire.SegmentRef{{Sha256: sha, Len: 1 << 20, Gen: 1}}, Len: 1 << 20}
		start := &wire.Start{ID: []byte{0xca, 0x1f}, SegmentURLs: []wire.SegmentURL{{Sha256: sha, Gen: 1, URL: u}}}
		msgs := invokeV(t, s, "echo", 6, start, input(),
			&wire.GetState{Key: []byte("history"), ResultCompletionID: 1, Name: "history"},
			&wire.GetStateCompletion{ID: 1, Result: wire.Result{Value: val}})
		wantKinds(t, msgs, "Error")
		messages = append(messages, wantError(t, msgs[0], 500, wire.BehaviorRetry).Message)
	}
	for _, m := range messages {
		if m != messages[0] || strings.Contains(m, "403") || strings.Contains(m, "38") {
			t.Fatalf("the replies tell the targets apart: %q", messages)
		}
	}
	got := logs.String()
	if !strings.Contains(got, "403") || strings.Contains(got, "SECRET") || strings.Contains(got, "127.0.0.1") {
		t.Fatalf("the log must say what happened, without the URL: %s", got)
	}
}

// A turn whose Input carries 1 MiB of empty segment refs (two bytes each) costs the
// endpoint about the size of the body, not 75 times it: the decoder refuses the first ref
// without a 32-byte sha256. The body is kept small on purpose; a 15 MiB one used to reach
// over 1 GiB of heap.
//
// Must see: the whole request allocates less than 16 times the body.
func TestDecodingAnInputOfEmptyRefsStaysSmall(t *testing.T) {
	s := newTestServer(t, HTTPHandler(echoService))
	var v []byte
	for range 1 << 19 {
		v = append(v, 0x12, 0x00) // Value.segments: SegmentRef{}
	}
	in := binary.AppendUvarint([]byte{0x72}, uint64(len(v))) // Input.value
	in = append(in, v...)
	body := wire.EncodeMessage(&wire.Start{ID: []byte{1}, KnownEntries: 1}, 0)
	body = wire.AppendFrame(body, wire.TypeInput, 0, in)
	var peak atomic.Uint64
	stop, sampled := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(sampled)
		var m runtime.MemStats
		for {
			runtime.ReadMemStats(&m)
			peak.Store(max(peak.Load(), m.HeapAlloc))
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	res, out := s.do(t, http.MethodPost, "/invoke/Echo/hello", body, invokeHeaders(5))
	runtime.ReadMemStats(&after)
	close(stop)
	<-sampled
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("body %d KiB: status %d, allocated %d KiB, peak heap %d KiB over a base of %d KiB",
		len(body)>>10, res.StatusCode, allocated>>10, (peak.Load()-min(peak.Load(), before.HeapAlloc))>>10, before.HeapAlloc>>10)
	// The handler takes no input, so whether the turn fails is not the point here.
	decodeFrames(t, out)
	if allocated > 16*uint64(len(body)) {
		t.Fatalf("a %d-byte body allocated %d bytes", len(body), allocated)
	}
}
