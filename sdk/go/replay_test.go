// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

// Incremental replay of REQUEST_RESPONSE over HTTP (wire v5, docs/protocol-v1.md §9.3).
//
// Must see:
//   - a delta turn that hits answers byte for byte what a full replay answers on a fresh
//     endpoint, with `cairn-replay-cache: N`;
//   - a miss (no entry, a mismatching digest, B past the cache, a missing frame) is 409 +
//     `cairn-replay: miss` with an empty body, and the handler does not run;
//   - the identity check comes before the cache;
//   - a turn that ends the invocation drops the entry; a runtime on v4 gets no header;
//   - the defaults and limits of the options, and FLY_MACHINE_ID affinity;
//   - many requests for one invocation at once.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/identity"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/replay"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

var replayID = bytes.Repeat([]byte{0x11}, 24)

// replayInput is Input("req"), the frame of the golden vector.
var replayInput = func() []byte {
	b, _ := hex.DecodeString("040000000000000972070a052272657122")
	return b
}()

// replayService: nap sleeps once then returns, done returns at once; runs counts the
// times a handler body starts.
func replayService(runs *atomic.Int32) *ServiceDef {
	return NewService("R",
		Handler("nap", NoInput(func(ctx Context) (string, error) {
			runs.Add(1)
			if err := Sleep(ctx, time.Second); err != nil {
				return "", err
			}
			return "ok", nil
		})),
		Handler("done", NoInput(func(Context) (string, error) {
			runs.Add(1)
			return "ok", nil
		})),
	)
}

// replayKey signs the requests of post: the replay cache needs request identity.
var replayKey = sync.OnceValue(func() signer {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	return signer{priv: priv, kid: identity.KID(pub)}
})

// replayEndpoint verifies request identity with replayKey, so the replay cache is on.
func replayEndpoint(t *testing.T, runs *atomic.Int32, opts ...ServeOption) *endpoint {
	t.Helper()
	base := []ServeOption{replayService(runs), withClock(func() int64 { return testClock }), IdentityKeys(replayKey().kid)}
	e, err := newEndpoint(append(base, opts...))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func replayBody(n, b uint32, digest []byte, frames ...[]byte) []byte {
	body := wire.EncodeMessage(&wire.Start{ID: replayID, KnownEntries: n, ReplayFrom: b, PrefixDigest: digest}, 0)
	for _, f := range frames {
		body = append(body, f...)
	}
	return body
}

// post sends one turn signed with replayKey, as the runtime signs it.
func post(t *testing.T, h http.Handler, handler string, body []byte, v int) *httptest.ResponseRecorder {
	t.Helper()
	frames, err := wire.DecodeFrames(body)
	if err != nil || len(frames) == 0 {
		t.Fatalf("body %x: %v", body, err)
	}
	var st wire.Start
	if err := st.Unmarshal(frames[0].Payload); err != nil {
		t.Fatal(err)
	}
	key := replayKey()
	jwt := key.jwt("/invoke/R/"+handler, identity.ExpectedJTI(st.ID, st.RetryCountSinceLastStoredEntry), body, "")
	return postRaw(t, h, handler, body, v, identity.SchemeHeader, identity.SchemeV1, identity.JWTHeader, jwt)
}

// postRaw sends one turn with only the given headers (name, value pairs) added.
func postRaw(t *testing.T, h http.Handler, handler string, body []byte, v int, header ...string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/invoke/R/"+handler, bytes.NewReader(body))
	for k, val := range invokeHeaders(v) {
		req.Header.Set(k, val)
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

// sleepOf is the Sleep command of a response, as the runtime journals it.
func sleepOf(t *testing.T, body []byte) []byte {
	t.Helper()
	frames, err := wire.DecodeFrames(body)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range frames {
		if f.Type == wire.TypeSleep {
			return wire.EncodeFrame(f.Type, 0, f.Payload)
		}
	}
	t.Fatalf("no Sleep in %x", body)
	return nil
}

func chainOf(frames ...[]byte) []replay.Digest {
	return replay.Extend([]replay.Digest{replay.Root(replayID)}, frames)
}

func wantMiss(t *testing.T, w *httptest.ResponseRecorder, held, why string) {
	t.Helper()
	if w.Code != http.StatusConflict || w.Header().Get("cairn-replay") != "miss" || w.Body.Len() != 0 {
		t.Fatalf("%s: status %d, cairn-replay %q, %d body bytes; want a miss", why, w.Code, w.Header().Get("cairn-replay"), w.Body.Len())
	}
	if got := w.Header().Get("cairn-replay-held"); got != held {
		t.Fatalf("%s: cairn-replay-held %q, want %q", why, got, held)
	}
}

func TestReplayDeltaHitMatchesFullReplay(t *testing.T) {
	var runs atomic.Int32
	e := replayEndpoint(t, &runs)
	first := post(t, e, "nap", replayBody(1, 0, nil, replayInput), 5)
	if first.Code != 200 || first.Header().Get("cairn-replay-cache") != "1" || e.replay.Len() != 1 {
		t.Fatalf("turn 1: %d, cairn-replay-cache %q", first.Code, first.Header().Get("cairn-replay-cache"))
	}
	sleep := sleepOf(t, first.Body.Bytes())
	h := chainOf(replayInput, sleep)
	// Turn 2: the journal is Input + Sleep; the runtime sends only Sleep.
	delta := post(t, e, "nap", replayBody(2, 1, h[1][:], sleep), 5)
	full := post(t, replayEndpoint(t, &runs), "nap", replayBody(2, 0, nil, replayInput, sleep), 5)
	if delta.Code != 200 || !bytes.Equal(delta.Body.Bytes(), full.Body.Bytes()) {
		t.Fatalf("delta %d %x, full replay %x", delta.Code, delta.Body.Bytes(), full.Body.Bytes())
	}
	if delta.Header().Get("cairn-replay-cache") != "2" {
		t.Fatalf("turn 2: cairn-replay-cache %q", delta.Header().Get("cairn-replay-cache"))
	}
	// Turn 3: the timer fired; the turn ends, the entry is dropped and no header returned.
	fired := wire.EncodeMessage(&wire.SleepCompletion{ID: 1, Void: true}, 0)
	last := post(t, e, "nap", replayBody(3, 2, h[2][:], fired), 5)
	if got := kinds(decodeFrames(t, last.Body.Bytes())); last.Code != 200 || got != "Output End" {
		t.Fatalf("turn 3: %d %s", last.Code, got)
	}
	if _, has := last.Header()["Cairn-Replay-Cache"]; has || e.replay.Len() != 0 {
		t.Fatalf("End must drop the entry: header %v, len %d", last.Header(), e.replay.Len())
	}
}

func TestReplayMissDoesNotRunTheHandler(t *testing.T) {
	var runs atomic.Int32
	e := replayEndpoint(t, &runs)
	post(t, e, "nap", replayBody(1, 0, nil, replayInput), 5)
	h := chainOf(replayInput)
	held := "1:" + hex.EncodeToString(h[1][:])
	bad := h[1]
	bad[0] ^= 1
	before := runs.Load()
	for _, c := range []struct {
		why  string
		e    *endpoint
		body []byte
		held string
	}{
		{"digest mismatch", e, replayBody(2, 1, bad[:], replayInput), held},
		{"no entry", replayEndpoint(t, &runs), replayBody(2, 1, h[1][:], replayInput), ""},
		{"B beyond the cache", e, replayBody(3, 2, h[1][:], replayInput), held},
		{"missing frame", e, replayBody(3, 1, h[1][:], replayInput), held},
		{"cache off", replayEndpoint(t, &runs, WithoutReplayCache()), replayBody(2, 1, h[1][:], replayInput), ""},
	} {
		wantMiss(t, post(t, c.e, "nap", c.body, 5), c.held, c.why)
	}
	if runs.Load() != before {
		t.Fatalf("the handler ran %d times on miss turns", runs.Load()-before)
	}
}

// The second round of a miss: what is still held, k = min(held, N); resending from k hits.
func TestReplayMissReportsWhatIsHeld(t *testing.T) {
	var runs atomic.Int32
	e := replayEndpoint(t, &runs)
	first := post(t, e, "nap", replayBody(1, 0, nil, replayInput), 5)
	sleep := sleepOf(t, first.Body.Bytes())
	h := chainOf(replayInput, sleep)
	post(t, e, "nap", replayBody(2, 1, h[1][:], sleep), 5)
	var bad [32]byte
	wantMiss(t, post(t, e, "nap", replayBody(3, 2, bad[:], replayInput), 5), "2:"+hex.EncodeToString(h[2][:]), "N past what is held")
	wantMiss(t, post(t, e, "nap", replayBody(1, 1, bad[:]), 5), "1:"+hex.EncodeToString(h[1][:]), "N below what is held")
	if again := post(t, e, "nap", replayBody(3, 2, h[2][:], replayInput), 5); again.Code != 200 {
		t.Fatalf("resent from k: %d", again.Code)
	}
}

func TestReplayIdentityComesFirst(t *testing.T) {
	key := newSigner(t)
	var runs atomic.Int32
	e := replayEndpoint(t, &runs, IdentityKeys(key.kid))
	body := replayBody(1, 0, nil, replayInput)
	if w := postRaw(t, e, "nap", body, 5); w.Code != 401 || e.replay.Len() != 0 {
		t.Fatalf("unsigned: %d, %d held", w.Code, e.replay.Len())
	}
	// A signed delta for an invocation the cache does not hold: a miss, after the check.
	h := chainOf(replayInput)
	delta := replayBody(2, 1, h[1][:], replayInput)
	jwt := key.jwt("/invoke/R/nap", identity.ExpectedJTI(replayID, 0), delta, "example.com")
	w := postRaw(t, e, "nap", delta, 5, identity.SchemeHeader, identity.SchemeV1, identity.JWTHeader, jwt)
	wantMiss(t, w, "", "signed delta")
	if runs.Load() != 0 {
		t.Fatal("the handler ran")
	}
}

func TestReplayOldRuntimeAndEndingTurns(t *testing.T) {
	var runs atomic.Int32
	e := replayEndpoint(t, &runs)
	post(t, e, "nap", replayBody(1, 0, nil, replayInput), 5)
	if e.replay.Len() != 1 {
		t.Fatal("not held")
	}
	if w := post(t, e, "done", replayBody(1, 0, nil, replayInput), 5); w.Header().Get("cairn-replay-cache") != "" || e.replay.Len() != 0 {
		t.Fatalf("End: header %q, %d held", w.Header().Get("cairn-replay-cache"), e.replay.Len())
	}
	// A runtime on wire v4: no header, nothing held.
	w := post(t, e, "nap", replayBody(1, 0, nil, replayInput), 4)
	if w.Code != 200 || w.Header().Get("cairn-replay-cache") != "" || e.replay.Len() != 0 {
		t.Fatalf("v4: %d, header %q, %d held", w.Code, w.Header().Get("cairn-replay-cache"), e.replay.Len())
	}
}

func TestReplayOptions(t *testing.T) {
	var runs atomic.Int32
	declared := func(opts ...ServeOption) bool {
		return strings.Contains(string(replayEndpoint(t, &runs, opts...).manifest), `"replayCache":true`)
	}
	for _, c := range []struct {
		name string
		opts []ServeOption
		want bool
	}{
		{"default", nil, true},
		{"pinned to v4", []ServeOption{MaxProtocolVersion(4)}, false},
		{"off", []ServeOption{WithoutReplayCache()}, false},
		{"off then on", []ServeOption{WithoutReplayCache(), ReplayCache(ReplayCacheOptions{})}, true},
		{"on then off", []ServeOption{ReplayCache(ReplayCacheOptions{}), WithoutReplayCache()}, false},
		{"bidi", []ServeOption{Bidi()}, false},
	} {
		if got := declared(c.opts...); got != c.want {
			t.Errorf("%s: replayCache declared %v, want %v", c.name, got, c.want)
		}
	}
	// Off: a full turn runs with no header.
	off := replayEndpoint(t, &runs, WithoutReplayCache())
	if w := post(t, off, "nap", replayBody(1, 0, nil, replayInput), 5); w.Code != 200 || w.Header().Get("cairn-replay-cache") != "" {
		t.Fatalf("off: %d %q", w.Code, w.Header().Get("cairn-replay-cache"))
	}
	// An invocation larger than the cap is not held: no header.
	tiny := replayEndpoint(t, &runs, ReplayCache(ReplayCacheOptions{MaxBytes: 8}))
	if w := post(t, tiny, "nap", replayBody(1, 0, nil, replayInput), 5); w.Code != 200 || w.Header().Get("cairn-replay-cache") != "" {
		t.Fatalf("tiny: %d %q", w.Code, w.Header().Get("cairn-replay-cache"))
	}
}

func TestReplayAffinity(t *testing.T) {
	var runs atomic.Int32
	t.Setenv("FLY_MACHINE_ID", "0123456789abcd")
	if w := post(t, replayEndpoint(t, &runs), "nap", replayBody(1, 0, nil, replayInput), 5); w.Header().Get("cairn-affinity") != "fly=0123456789abcd" {
		t.Fatalf("cairn-affinity %q", w.Header().Get("cairn-affinity"))
	}
	t.Setenv("FLY_MACHINE_ID", "bad id")
	if w := post(t, replayEndpoint(t, &runs), "nap", replayBody(1, 0, nil, replayInput), 5); w.Header().Get("cairn-affinity") != "" {
		t.Fatalf("cairn-affinity %q", w.Header().Get("cairn-affinity"))
	}
}

// Several requests for one invocation at once (a runtime resending after a timeout,
// two replicas' worth of traffic on one), under the race detector: every hit answers
// exactly the full replay, every other answer is a clean miss.
func TestReplayConcurrentRequests(t *testing.T) {
	var runs atomic.Int32
	e := replayEndpoint(t, &runs)
	first := post(t, e, "nap", replayBody(1, 0, nil, replayInput), 5)
	sleep := sleepOf(t, first.Body.Bytes())
	h := chainOf(replayInput, sleep)
	want := post(t, replayEndpoint(t, &runs), "nap", replayBody(2, 0, nil, replayInput, sleep), 5).Body.Bytes()
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 20 {
				var w *httptest.ResponseRecorder
				switch (g + i) % 3 {
				case 0:
					w = post(t, e, "nap", replayBody(1, 0, nil, replayInput), 5)
				case 1:
					w = post(t, e, "nap", replayBody(2, 1, h[1][:], sleep), 5)
				default:
					w = post(t, e, "nap", replayBody(2, 2, h[2][:]), 5)
				}
				switch {
				case w.Code == http.StatusConflict:
				case w.Code != 200:
					errs <- fmt.Errorf("status %d", w.Code)
					return
				case (g+i)%3 != 0 && !bytes.Equal(w.Body.Bytes(), want):
					errs <- fmt.Errorf("a hit answered %x", w.Body.Bytes())
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// An endpoint that does not verify request identity keeps no journal: whoever knows a
// running invocation's id could otherwise learn the digest of its cached prefix from a
// miss (`cairn-replay-held`), send a delta from there, and run the handler on that
// invocation's real input and results.
//
//  1. The runtime runs a turn of invocation V; the handler suspends on a signal.
//  2. A stranger sends a delta for V with a wrong digest and reads `cairn-replay-held`.
//  3. The stranger sends a delta from that prefix with its own signal.
//
// Must see: no `replayCache` in the manifest, no `cairn-replay-cache` header, the probe
// is a miss without `cairn-replay-held`, the handler never runs on V's input; and an
// explicit ReplayCache without IdentityKeys is a configuration error.
func TestReplayCacheNeedsRequestIdentity(t *testing.T) {
	var leaked atomic.Value
	svc := NewService("Pay", Handler("pay", func(ctx Context, card string) (string, error) {
		ok, err := Signal[string](ctx, "approve").Result()
		if err != nil {
			return "", err
		}
		leaked.Store(card)
		return "charged " + card + " " + ok, nil
	}))
	e, err := newEndpoint([]ServeOption{svc})
	if err != nil {
		t.Fatal(err)
	}
	if e.replay != nil || strings.Contains(string(e.manifest), "replayCache") {
		t.Fatalf("an unverified endpoint declares the replay cache: %s", e.manifest)
	}
	postPay := func(body []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/invoke/Pay/pay", bytes.NewReader(body))
		for k, v := range invokeHeaders(5) {
			req.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		e.ServeHTTP(w, req)
		return w
	}
	victim := bytes.Repeat([]byte{0x77}, 16)
	in := wire.AppendMessage(nil, &wire.Input{Value: &wire.Value{Content: []byte(`"4111-VICTIM-CARD"`)}}, 0)
	// 1. The runtime's turn.
	w := postPay(append(wire.EncodeMessage(&wire.Start{ID: victim, KnownEntries: 1}, 0), in...))
	if w.Code != 200 || w.Header().Get("cairn-replay-cache") != "" {
		t.Fatalf("runtime turn: %d, cairn-replay-cache %q", w.Code, w.Header().Get("cairn-replay-cache"))
	}
	// 2. The probe.
	w = postPay(wire.EncodeMessage(&wire.Start{ID: victim, KnownEntries: 1, ReplayFrom: 1, PrefixDigest: make([]byte, 32)}, 0))
	wantMiss(t, w, "", "probe")
	// 3. The delta from the prefix the endpoint would have held.
	h := replay.Extend([]replay.Digest{replay.Root(victim)}, [][]byte{in})
	sig := wire.AppendMessage(nil, &wire.SignalNotification{Signal: wire.SignalName("approve"), Result: wire.Result{Value: &wire.Value{Content: []byte(`"yes"`)}}}, 0)
	w = postPay(append(wire.EncodeMessage(&wire.Start{ID: victim, KnownEntries: 2, ReplayFrom: 1, PrefixDigest: h[1][:]}, 0), sig...))
	wantMiss(t, w, "", "delta from the victim's prefix")
	if leaked.Load() != nil {
		t.Fatalf("the handler ran on the victim's input %v", leaked.Load())
	}

	// An explicit ReplayCache without IdentityKeys is refused, whatever the entry point.
	opts := []ServeOption{svc, ReplayCache(ReplayCacheOptions{})}
	if _, err := newEndpoint(opts); err == nil || !strings.Contains(err.Error(), "IdentityKeys") {
		t.Fatalf("newEndpoint: %v", err)
	}
	if err := Serve(context.Background(), "127.0.0.1:0", opts...); err == nil {
		t.Fatal("Serve accepted ReplayCache without IdentityKeys")
	}
	if _, err := LambdaHandler(opts...); err == nil {
		t.Fatal("LambdaHandler accepted ReplayCache without IdentityKeys")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("HTTPHandler accepted ReplayCache without IdentityKeys")
			}
		}()
		HTTPHandler(opts...)
	}()
	// With IdentityKeys the cache is on by default, and ReplayCache is accepted.
	key := newSigner(t)
	for _, extra := range [][]ServeOption{nil, {ReplayCache(ReplayCacheOptions{})}} {
		e, err := newEndpoint(append([]ServeOption{svc, IdentityKeys(key.kid)}, extra...))
		if err != nil || e.replay == nil || !strings.Contains(string(e.manifest), `"replayCache":true`) {
			t.Fatalf("with IdentityKeys: %v, %+v", err, e)
		}
	}
}
