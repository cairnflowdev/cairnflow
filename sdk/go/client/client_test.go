// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cairn "github.com/cairnflowdev/cairnflow/sdk/go"
)

var uuidRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

var bg = context.Background()

func TestCallWaitsAndSendsTheContractHeaders(t *testing.T) {
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		var in any
		_ = json.Unmarshal(req.Body, &in)
		reply(w, 200, completed(map[string]any{"ok": true, "n": in}, testID), "X-Cairn-Invocation-Id", testID)
	})
	c := newClient(t, f.URL, WithAPIKey("cf_key"), WithHeader("X-Extra", "1"))
	type out struct {
		OK bool           `json:"ok"`
		N  map[string]int `json:"n"`
	}
	got, err := Call[out](bg, c.Service("Orders"), "submit", map[string]int{"amount": 5},
		LimitKey("t1"), LogLevel("debug"), Tag("team", "a"), Tag("env", "dev"), Header("X-Call", "2"))
	if err != nil {
		t.Fatal(err)
	}
	equal(t, got.OK, true)
	equal(t, got.N["amount"], 5)
	r := f.last()
	equal(t, r.Method, "POST")
	equal(t, r.Target, "/invoke/Orders/submit")
	equal(t, r.Header.Get("Authorization"), "Bearer cf_key")
	equal(t, r.Header.Get("X-Extra"), "1")
	equal(t, r.Header.Get("X-Call"), "2")
	equal(t, r.Header.Get("Content-Type"), "application/json")
	equal(t, r.Header.Get("X-Cairn-Limit-Key"), "t1")
	equal(t, r.Header.Get("Cairnflow-Log-Level"), "debug")
	equalStrings(t, r.Header.Values("Cairnflow-Tag"), []string{"team=a", "env=dev"})
	if !uuidRe.MatchString(r.Header.Get("Idempotency-Key")) {
		t.Fatalf("idempotency key %q is not a UUID", r.Header.Get("Idempotency-Key"))
	}
}

func TestPathsKeysScopesAndBodies(t *testing.T) {
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		reply(w, 200, map[string]any{"id": testID, "status": "completed", "output": ""})
	})
	c := newClient(t, f.URL)
	v, err := Call[any](bg, c.Object("Cart", "a/b c+d&e=\u00e9", InScope("acme")), "add", nil)
	if err != nil || v != nil {
		t.Fatalf("got %v, %v", v, err)
	}
	for _, in := range []any{json.RawMessage("null"), "<a&b>", []byte(`{"raw":1}`)} {
		if _, err := Call[any](bg, c.Service("S"), "h", in); err != nil {
			t.Fatal(err)
		}
	}
	seen := f.requests()
	equal(t, seen[0].Target, "/scope/acme/invoke/Cart/add?key=a%2Fb%20c%2Bd%26e%3D%C3%A9")
	equal(t, string(seen[0].Body), "")
	equal(t, seen[0].Header.Get("Content-Type"), "")
	equal(t, seen[0].Header.Get("Authorization"), "")
	equal(t, string(seen[1].Body), "null")
	equal(t, string(seen[2].Body), `"<a&b>"`) // no HTML escaping
	equal(t, string(seen[3].Body), `{"raw":1}`)
}

func TestEncodeURIComponent(t *testing.T) {
	equal(t, enc("A-z_0.9!~*'()"), "A-z_0.9!~*'()")
	equal(t, enc("a b/c?d#e%f:g@h$i,j;k"), "a%20b%2Fc%3Fd%23e%25f%3Ag%40h%24i%2Cj%3Bk")
	equal(t, enc("\u00e9"), "%C3%A9")
}

func TestOutputDecoding(t *testing.T) {
	type pair struct {
		A int `json:"a"`
	}
	p, err := decodeOutput[pair](`{"a":1}`)
	equal(t, err, nil)
	equal(t, p.A, 1)
	s, err := decodeOutput[string](`"x"`)
	equal(t, err, nil)
	equal(t, s, "x")
	s, err = decodeOutput[string](`a@b:123`) // not JSON: the text itself
	equal(t, err, nil)
	equal(t, s, "a@b:123")
	a, err := decodeOutput[any](`not json`)
	equal(t, err, nil)
	equal(t, a.(string), "not json")
	raw, err := decodeOutput[json.RawMessage](`{"a": 1}`)
	equal(t, err, nil)
	equal(t, string(raw), `{"a": 1}`)
	z, err := decodeOutput[pair]("")
	equal(t, err, nil)
	equal(t, z.A, 0)
	if _, err := decodeOutput[int](`"text"`); err == nil {
		t.Fatal("decoding a string into an int must fail")
	}
}

func TestIdempotencyKeyOffExplicitAndWorkflowRun(t *testing.T) {
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		reply(w, 200, completed("ok", testID), "X-Cairn-Invocation-Id", testID)
	})
	c := newClient(t, f.URL)
	wf := c.Workflow("W", "k")
	calls := []func() error{
		func() error { _, err := Call[string](bg, c.Service("S"), "h", 1, NoIdempotencyKey()); return err },
		func() error { _, err := Call[string](bg, c.Service("S"), "h", 1, IdempotencyKey("mine")); return err },
		func() error { _, err := RunWorkflow[string](bg, wf, 1); return err },
		func() error { _, err := Call[string](bg, wf, "click", 1); return err },
		func() error { _, err := RunWorkflow[string](bg, wf, 1, IdempotencyKey("r1")); return err },
		func() error { _, err := Send(bg, wf, "run", 1); return err },
	}
	for _, call := range calls {
		if err := call(); err != nil {
			t.Fatal(err)
		}
	}
	seen := f.requests()
	keys := make([]string, len(seen))
	for i, r := range seen {
		keys[i] = r.Header.Get("Idempotency-Key")
	}
	equal(t, keys[0], "")
	equal(t, keys[1], "mine")
	equal(t, keys[2], "")
	if !uuidRe.MatchString(keys[3]) {
		t.Fatalf("a workflow's shared handler got key %q", keys[3])
	}
	equal(t, keys[4], "r1")
	equal(t, keys[5], "")
	equal(t, seen[2].Target, "/invoke/W/run?key=k")
	equal(t, seen[5].Target, "/invoke/W/run/send?key=k")
}

func TestTheKeyIsKeptAcross503AndNetworkErrors(t *testing.T) {
	fakeSleep(t)
	var n atomic.Int32
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		switch n.Add(1) {
		case 1:
			reply(w, 503, map[string]any{"error": "electing"})
		case 2:
			abort(w) // the response is lost
		default:
			reply(w, 200, completed(3, testID))
		}
	})
	got, err := Call[int](bg, newClient(t, f.URL).Service("S"), "h", 1)
	if err != nil {
		t.Fatal(err)
	}
	equal(t, got, 3)
	seen := f.requests()
	equal(t, len(seen), 3)
	for _, r := range seen {
		equal(t, r.Header.Get("Idempotency-Key"), seen[0].Header.Get("Idempotency-Key"))
	}
}

func Test503LeaderIsFollowedWithTheSameKey(t *testing.T) {
	b := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		reply(w, 200, completed("from b", testID))
	})
	a := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		reply(w, 503, map[string]any{"error": "not the leader", "leader": b.hostPort()})
	})
	got, err := Call[string](bg, newClient(t, a.URL).Service("S"), "h", 1)
	if err != nil {
		t.Fatal(err)
	}
	equal(t, got, "from b")
	equal(t, len(a.requests()), 1)
	equal(t, len(b.requests()), 1)
	equal(t, a.last().Header.Get("Idempotency-Key"), b.last().Header.Get("Idempotency-Key"))
}

func TestALeaderWithAnotherSchemeIsNotFollowed(t *testing.T) {
	fakeSleep(t)
	var n atomic.Int32
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		if n.Add(1) == 1 {
			reply(w, 503, map[string]any{"error": "x", "leader": "https://127.0.0.1:1"})
			return
		}
		reply(w, 200, completed("home", testID))
	})
	got, err := Call[string](bg, newClient(t, f.URL).Service("S"), "h", nil)
	if err != nil {
		t.Fatal(err)
	}
	equal(t, got, "home")
	equal(t, len(f.requests()), 2)
}

func Test429WithoutIDWaitsRetryAfter(t *testing.T) {
	waits := fakeSleep(t)
	var n atomic.Int32
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		if n.Add(1) == 1 {
			reply(w, 429, map[string]any{"error": "overloaded", "code": "range_overloaded"}, "Retry-After", "3")
			return
		}
		reply(w, 200, completed("ok", testID))
	})
	got, err := Call[string](bg, newClient(t, f.URL).Service("S"), "h", nil)
	if err != nil {
		t.Fatal(err)
	}
	equal(t, got, "ok")
	seen := f.requests()
	equal(t, len(seen), 2)
	equal(t, seen[0].Header.Get("Idempotency-Key"), seen[1].Header.Get("Idempotency-Key"))
	w := waits()
	equal(t, len(w), 1)
	equal(t, w[0], 3*time.Second)
}

func Test202And429ThrottledAttachByID(t *testing.T) {
	waits := fakeSleep(t)
	var attaches atomic.Int32
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		switch p := req.path(); {
		case strings.HasPrefix(p, "/invoke/S/slow"):
			reply(w, 202, map[string]any{"id": testID, "status": "waiting"}, "X-Cairn-Invocation-Id", testID)
		case strings.HasPrefix(p, "/invoke/S/throttled"):
			reply(w, 429, map[string]any{"id": testID, "status": "throttled", "retry_after_ms": 5}, "Retry-After", "1")
		case p == "/invocations/"+testID+"/attach":
			if attaches.Add(1)%2 == 1 {
				reply(w, 202, map[string]any{"id": testID, "status": "waiting"})
			} else {
				reply(w, 200, completed("late", testID))
			}
		}
	})
	c := newClient(t, f.URL, WithAttachWait(1500*time.Millisecond))
	got, err := Call[string](bg, c.Service("S"), "slow", nil)
	if err != nil {
		t.Fatal(err)
	}
	equal(t, got, "late")
	seen := f.requests()
	equal(t, seen[1].Target, "/invocations/"+testID+"/attach?wait_ms=1500")
	equal(t, seen[1].Method, "POST")
	equal(t, seen[1].Header.Get("Idempotency-Key"), "")
	got, err = Call[string](bg, c.Service("S"), "throttled", nil)
	if err != nil {
		t.Fatal(err)
	}
	equal(t, got, "late")
	equal(t, len(f.requests()), 6)
	equal(t, len(waits()), 0) // a 429 with an id is not resent: it is attached to
}

func TestAttachTimeout(t *testing.T) {
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		wait, _ := strconv.Atoi(req.query("wait_ms"))
		time.Sleep(time.Duration(min(wait, 50)) * time.Millisecond)
		reply(w, 202, map[string]any{"id": testID, "status": "waiting"}, "X-Cairn-Invocation-Id", testID)
	})
	c := newClient(t, f.URL)
	_, err := Call[any](bg, c.Service("S"), "h", nil, Timeout(200*time.Millisecond))
	var at *AttachTimeout
	if !errors.As(err, &at) {
		t.Fatalf("got %v, want *AttachTimeout", err)
	}
	equal(t, at.InvocationID, testID)
	for _, r := range f.requests()[1:] {
		if w, _ := strconv.Atoi(r.query("wait_ms")); w > 200 {
			t.Fatalf("wait_ms %d is above the timeout", w)
		}
	}
	_, err = Attach[any](bg, c.Invocation(testID), Timeout(100*time.Millisecond))
	if !errors.As(err, &at) {
		t.Fatalf("got %v, want *AttachTimeout", err)
	}
}

func TestAttachRetries503And429WithoutID(t *testing.T) {
	waits := fakeSleep(t)
	var n atomic.Int32
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		switch n.Add(1) {
		case 1:
			reply(w, 429, map[string]any{"error": "too many", "code": "too_many_open_streams"}, "Retry-After", "4")
		case 2:
			reply(w, 503, map[string]any{"error": "x"})
		default:
			reply(w, 200, completed(7, testID))
		}
	})
	c := newClient(t, f.URL, WithMaxAttempts(1))
	got, err := Attach[int](bg, c.Invocation(testID))
	if err != nil {
		t.Fatal(err)
	}
	equal(t, got, 7)
	equal(t, len(f.requests()), 3)
	w := waits()
	equal(t, w[0], 4*time.Second) // Retry-After wins over the 100 ms backoff
	equal(t, w[1], 200*time.Millisecond)
}

func TestAttachGivesUpWithTheInvocationID(t *testing.T) {
	fakeSleep(t)
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		reply(w, 503, map[string]any{"error": "electing"})
	})
	_, err := Attach[any](bg, newClient(t, f.URL, WithMaxAttempts(1)).Invocation(testID))
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("got %v, want *HTTPError", err)
	}
	equal(t, he.Status, 503)
	equal(t, he.InvocationID, testID)
	equal(t, len(f.requests()), 11)
}

func TestErrorMapping(t *testing.T) {
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		hdr := []string{"X-Cairn-Invocation-Id", testID}
		switch req.path() {
		case "/invoke/S/failed":
			reply(w, 500, map[string]any{"id": testID, "status": "failed", "code": 422, "message": "bad amount"}, hdr...)
		case "/invoke/S/paused":
			reply(w, 409, map[string]any{"id": testID, "status": "paused", "code": 503, "message": "out of attempts"}, hdr...)
		case "/invoke/W/run":
			reply(w, 409, map[string]any{"id": testID, "status": "already_invoked"}, hdr...)
		case "/invoke/S/bad":
			reply(w, 400, map[string]any{"error": "bad input", "code": "bad_request"})
		case "/invoke/S/text":
			w.WriteHeader(403)
			_, _ = w.Write([]byte("nope"))
		default:
			reply(w, 404, map[string]any{"message": "no such thing"})
		}
	})
	c := newClient(t, f.URL)
	_, err := Call[any](bg, c.Service("S"), "failed", nil)
	var failed *InvocationFailed
	if !errors.As(err, &failed) {
		t.Fatalf("got %v", err)
	}
	equal(t, *failed, InvocationFailed{InvocationID: testID, Code: 422, Message: "bad amount"})
	if term, ok := cairn.AsTerminalError(err); !ok || term.Code() != 422 || term.Error() != "bad amount" {
		t.Fatalf("a failed invocation is a terminal error: %v %v", term, ok)
	}
	_, err = Call[any](bg, c.Service("S"), "paused", nil)
	var paused *InvocationPaused
	if !errors.As(err, &paused) {
		t.Fatalf("got %v", err)
	}
	equal(t, *paused, InvocationPaused{InvocationID: testID, Code: 503, Message: "out of attempts"})
	_, err = RunWorkflow[any](bg, c.Workflow("W", "k"), map[string]any{})
	var already *AlreadyInvoked
	if !errors.As(err, &already) {
		t.Fatalf("got %v", err)
	}
	equal(t, already.InvocationID, testID)
	_, err = Call[any](bg, c.Service("S"), "bad", nil)
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("got %v", err)
	}
	equal(t, he.Status, 400)
	equal(t, he.Code, "bad_request")
	equal(t, he.Message, "bad input")
	equal(t, he.Error(), "HTTP 400: bad input")
	_, err = Call[any](bg, c.Service("S"), "text", nil)
	if !errors.As(err, &he) {
		t.Fatalf("got %v", err)
	}
	equal(t, he.Status, 403)
	equal(t, he.Code, "")
	equal(t, string(he.Body), "nope")
	equal(t, he.Error(), "HTTP 403")
	_, err = c.Invocation(testID).Status(bg)
	if !errors.As(err, &he) {
		t.Fatalf("got %v", err)
	}
	equal(t, he.Status, 404)
	equal(t, he.Message, "no such thing")
}

// A workflow's run carries no key: the first send reached the node but its response was
// lost, so the resend gets 409 already_invoked. That run is the caller's own: wait for it
// instead of returning *AlreadyInvoked.
func TestAResentWorkflowRunWaitsForItsOwnRun(t *testing.T) {
	fakeSleep(t)
	var n atomic.Int32
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		switch {
		case n.Add(1) == 1:
			abort(w)
		case req.path() == "/invoke/W/run":
			reply(w, 409, map[string]any{"id": testID, "status": "already_invoked"}, "X-Cairn-Invocation-Id", testID)
		default:
			reply(w, 200, completed("mine", testID))
		}
	})
	got, err := RunWorkflow[string](bg, newClient(t, f.URL).Workflow("W", "k"), map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	equal(t, got, "mine")
	equal(t, f.last().rawPath(), "/invocations/"+testID+"/attach")
}

func TestSendHandle(t *testing.T) {
	var done atomic.Bool
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		hdr := []string{"X-Cairn-Invocation-Id", testID}
		switch p := req.path(); p {
		case "/invoke/S/slow/send":
			reply(w, 202, map[string]any{"id": testID, "status": "waiting"}, hdr...)
		case "/invoke/W/run/send":
			reply(w, 409, map[string]any{"id": testID, "status": "already_invoked"}, hdr...)
		case "/invoke/S/paused/send":
			reply(w, 409, map[string]any{"id": testID, "status": "paused"}, hdr...)
		case "/invoke/S/nope/send":
			reply(w, 400, map[string]any{"error": "bad"})
		case "/invocations/" + testID + "/attach":
			if done.Load() {
				reply(w, 200, completed("late", testID))
			} else {
				reply(w, 202, map[string]any{"id": testID, "status": "waiting"})
			}
		case "/invocations/" + testID:
			reply(w, 200, map[string]any{"id": testID, "status": "completed", "output": `"late"`, "createdAt": 1, "endedAt": 2})
		case "/invocations/" + testID + "/signal":
			reply(w, 200, map[string]any{"id": testID, "status": "running"})
		case "/invocations/" + testID + "/cancel":
			reply(w, 200, map[string]any{"id": testID, "cancelled": true, "status": "running"})
		}
	})
	c := newClient(t, f.URL)
	h, err := Send(bg, c.Service("S"), "slow", 800, Timeout(time.Nanosecond))
	if err != nil {
		t.Fatal(err)
	}
	equal(t, f.last().Target, "/invoke/S/slow/send")
	equal(t, string(f.last().Body), "800")
	equal(t, h.ID(), testID)
	v, ready, err := Output[string](bg, h)
	equal(t, err, nil)
	equal(t, ready, false)
	equal(t, v, "")
	equal(t, f.last().Target, "/invocations/"+testID+"/attach?wait_ms=0")
	done.Store(true)
	v, ready, err = Output[string](bg, h)
	equal(t, err, nil)
	equal(t, ready, true)
	equal(t, v, "late")
	v, err = Attach[string](bg, h)
	equal(t, err, nil)
	equal(t, v, "late")
	st, err := h.Status(bg)
	if err != nil {
		t.Fatal(err)
	}
	equal(t, st.Status, "completed")
	equal(t, st.CreatedAt, int64(1))
	equal(t, *st.Output, `"late"`)
	equal(t, f.last().Method, "GET")
	ack, err := h.Signal(bg, "approve", map[string]string{"by": "boss"})
	if err != nil {
		t.Fatal(err)
	}
	equal(t, ack.Status, "running")
	equalJSON(t, f.last().json(t), `{"signal":"approve","value":{"by":"boss"}}`)
	if _, err := h.SignalIndex(bg, 2, nil); err != nil {
		t.Fatal(err)
	}
	equal(t, string(f.last().Body), `{"signal":2}`)
	if _, err := h.Signal(bg, "s", json.RawMessage("null")); err != nil {
		t.Fatal(err)
	}
	equal(t, string(f.last().Body), `{"signal":"s","value":null}`)
	cancel, err := h.Cancel(bg)
	if err != nil {
		t.Fatal(err)
	}
	equal(t, cancel.Cancelled, true)
	equal(t, f.last().Target, "/invocations/"+testID+"/cancel?mode=signal")
	if _, err := h.CancelHard(bg); err != nil {
		t.Fatal(err)
	}
	equal(t, f.last().Target, "/invocations/"+testID+"/cancel")
	p, err := Send(bg, c.Service("S"), "paused", nil)
	if err != nil {
		t.Fatal(err)
	}
	equal(t, p.ID(), testID)
	_, err = Send(bg, c.Workflow("W", "k"), "run", map[string]any{})
	var already *AlreadyInvoked
	if !errors.As(err, &already) {
		t.Fatalf("got %v, want *AlreadyInvoked", err)
	}
	_, err = Send(bg, c.Service("S"), "nope", nil)
	var he *HTTPError
	if !errors.As(err, &he) || he.Status != 400 {
		t.Fatalf("got %v, want a 400 *HTTPError", err)
	}
}

func TestWorkflowAttachOutputAwakeablesPromises(t *testing.T) {
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		p := req.path()
		switch {
		case strings.HasSuffix(p, "/attach"):
			reply(w, 200, map[string]any{"id": "wf", "status": "completed", "output": "a@b:123"})
		case strings.HasPrefix(p, "/awakeables/"):
			reply(w, 200, map[string]any{"id": "awk", "status": "running"})
		case req.Method == "GET":
			reply(w, 200, map[string]any{"service": "Signup", "key": "u1", "name": "email", "completed": false})
		default:
			reply(w, 200, map[string]any{"service": "Signup", "key": "u1", "name": "email", "completed": true, "woken": 1})
		}
	})
	c := newClient(t, f.URL)
	wf := c.Workflow("Signup", "u 1", InScope("acme"))
	v, err := Attach[string](bg, wf, Timeout(5*time.Second))
	equal(t, err, nil)
	equal(t, v, "a@b:123")
	// wait_ms is what is left of the 5 s timeout, capped at the attach wait.
	if !regexp.MustCompile(`^/scope/acme/workflow/Signup/u%201/attach\?wait_ms=(4[0-9]{3}|5000)$`).MatchString(f.last().Target) {
		t.Fatalf("attach target %q", f.last().Target)
	}
	v, ready, err := Output[string](bg, wf)
	equal(t, err, nil)
	equal(t, ready, true)
	equal(t, v, "a@b:123")
	equal(t, f.last().Target, "/scope/acme/workflow/Signup/u%201/attach?wait_ms=0")

	if _, err := c.Awakeable("sign_1/x").Resolve(bg, map[string]bool{"ok": true}); err != nil {
		t.Fatal(err)
	}
	equal(t, f.last().rawPath(), "/awakeables/sign_1%2Fx/resolve")
	equal(t, string(f.last().Body), `{"ok":true}`)
	if _, err := c.Awakeable("a").Resolve(bg, nil); err != nil {
		t.Fatal(err)
	}
	equal(t, string(f.last().Body), "")
	if _, err := c.Awakeable("a").Reject(bg, "denied", 403); err != nil {
		t.Fatal(err)
	}
	equal(t, string(f.last().Body), `{"message":"denied","code":403}`)
	if _, err := c.Awakeable("a").Reject(bg, "denied", 0); err != nil {
		t.Fatal(err)
	}
	equal(t, string(f.last().Body), `{"message":"denied"}`)

	p := c.Promise("Signup", "u1", "email")
	view, err := p.Peek(bg)
	equal(t, err, nil)
	equal(t, view.Completed, false)
	equal(t, f.last().Method, "GET")
	equal(t, f.last().Target, "/promises/Signup/u1/email")
	ack, err := p.Resolve(bg, "456")
	equal(t, err, nil)
	equal(t, ack.Completed, true)
	equal(t, ack.Woken, 1)
	equal(t, f.last().Target, "/promises/Signup/u1/email/resolve")
	equal(t, string(f.last().Body), `"456"`)
	if _, err := c.Promise("Signup", "u1", "email", InScope("acme")).Reject(bg, "broken", 400); err != nil {
		t.Fatal(err)
	}
	equal(t, f.last().Target, "/scope/acme/promises/Signup/u1/email/reject")
	equal(t, string(f.last().Body), `{"message":"broken","code":400}`)
}

func TestSchedules(t *testing.T) {
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		p := req.path()
		switch {
		case req.Method == "PUT":
			var spec map[string]any
			_ = json.Unmarshal(req.Body, &spec)
			spec["name"], spec["created"] = "nightly", true
			reply(w, 200, spec)
		case strings.HasSuffix(p, "/trigger"):
			reply(w, 200, map[string]any{"id": testID, "created": true})
		case strings.HasSuffix(p, "/gone"):
			reply(w, 404, map[string]any{"error": "no such schedule"})
		case strings.HasSuffix(p, "/pause"), strings.HasSuffix(p, "/resume"):
			reply(w, 200, map[string]any{"name": "nightly", "paused": strings.HasSuffix(p, "/pause"), "nextAt": nil})
		case req.Method == "DELETE":
			reply(w, 200, map[string]any{"name": "nightly", "deleted": true})
		case strings.HasSuffix(p, "/schedules"):
			reply(w, 200, map[string]any{"schedules": []any{}, "partial": false, "missing": []any{}, "missingRanges": []any{}})
		default:
			reply(w, 200, map[string]any{"name": "nightly", "fired": 1, "nextAt": 1700000000000})
		}
	})
	s := newClient(t, f.URL).Schedules()
	v, err := s.Put(bg, "nightly", ScheduleDefinition{
		Every:  time.Minute,
		Target: ScheduleTarget{Service: "Report", Handler: "build"},
		Input:  map[string]bool{"full": true},
		Paused: true,
		Jitter: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	equal(t, v.Created, true)
	equal(t, f.last().Method, "PUT")
	equal(t, f.last().Target, "/schedules/nightly")
	equal(t, f.last().Header.Get("Content-Type"), "application/json")
	equal(t, string(f.last().Body), `{"everyMs":60000,"target":{"service":"Report","handler":"build"},"input":{"full":true},"paused":true,"jitterMs":500}`)
	if _, err := s.Put(bg, "vo", ScheduleDefinition{
		Cron:     "0 2 * * *",
		TimeZone: "Europe/Berlin",
		Target:   ScheduleTarget{Service: "CronObject", Handler: "tick", Key: "k1"},
		LimitKey: "l",
		Overlap:  "allow",
	}, InScope("acme")); err != nil {
		t.Fatal(err)
	}
	equal(t, f.last().Target, "/scope/acme/schedules/vo")
	equal(t, string(f.last().Body), `{"cron":"0 2 * * *","timeZone":"Europe/Berlin","target":{"service":"CronObject","handler":"tick","key":"k1"},"limitKey":"l","overlap":"allow"}`)
	got, err := s.Get(bg, "nightly")
	if err != nil || got == nil {
		t.Fatalf("got %v, %v", got, err)
	}
	equal(t, got.Fired, int64(1))
	equal(t, *got.NextAt, int64(1700000000000))
	gone, err := s.Get(bg, "gone")
	equal(t, err, nil)
	if gone != nil {
		t.Fatalf("a missing schedule: got %+v", gone)
	}
	deleted, err := s.Delete(bg, "nightly")
	equal(t, err, nil)
	equal(t, deleted, true)
	equal(t, f.last().Method, "DELETE")
	deleted, err = s.Delete(bg, "gone")
	equal(t, err, nil)
	equal(t, deleted, false)
	pv, err := s.Pause(bg, "nightly")
	equal(t, err, nil)
	equal(t, pv.Paused, true)
	if pv.NextAt != nil {
		t.Fatal("a paused schedule has no nextAt")
	}
	rv, err := s.Resume(bg, "nightly")
	equal(t, err, nil)
	equal(t, rv.Paused, false)
	equal(t, f.last().Target, "/schedules/nightly/resume")
	equal(t, f.last().Method, "POST")
	run, err := s.Trigger(bg, "nightly", IdempotencyKey("r1"))
	equal(t, err, nil)
	equal(t, run.ID(), testID)
	equal(t, f.last().Header.Get("Idempotency-Key"), "r1")
	if _, err := s.Trigger(bg, "nightly", InScope("acme")); err != nil {
		t.Fatal(err)
	}
	equal(t, f.last().Target, "/scope/acme/schedules/nightly/trigger")
	if !uuidRe.MatchString(f.last().Header.Get("Idempotency-Key")) {
		t.Fatalf("trigger key %q", f.last().Header.Get("Idempotency-Key"))
	}
	list, err := s.List(bg)
	equal(t, err, nil)
	equal(t, list.Partial, false)
	if _, err := s.List(bg, InScope("acme")); err != nil {
		t.Fatal(err)
	}
	equal(t, f.last().Method, "GET")
	equal(t, f.last().Target, "/scope/acme/schedules")
}

// The trigger key is generated once and kept across every attempt: a retry after a lost
// response lands on the run already created.
func TestTriggerKeyIsKeptAcrossRetries(t *testing.T) {
	fakeSleep(t)
	var n atomic.Int32
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		if n.Add(1) == 1 {
			abort(w)
			return
		}
		reply(w, 200, map[string]any{"id": testID, "created": false})
	})
	if _, err := newClient(t, f.URL).Schedules().Trigger(bg, "nightly"); err != nil {
		t.Fatal(err)
	}
	seen := f.requests()
	equal(t, len(seen), 2)
	equal(t, seen[0].Header.Get("Idempotency-Key"), seen[1].Header.Get("Idempotency-Key"))
}

func TestCancellingTheContextStopsACall(t *testing.T) {
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		if strings.HasPrefix(req.path(), "/invoke/") {
			reply(w, 202, map[string]any{"id": testID, "status": "waiting"}, "X-Cairn-Invocation-Id", testID)
			return
		}
		// The attach long-polls until the client goes away.
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	})
	ctx, cancel := context.WithCancel(bg)
	time.AfterFunc(50*time.Millisecond, cancel)
	started := time.Now()
	_, err := Call[any](ctx, newClient(t, f.URL).Service("S"), "h", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatalf("cancelling took %v", time.Since(started))
	}
}

func TestCancellingTheContextStopsTheBackoff(t *testing.T) {
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		reply(w, 429, map[string]any{"error": "overloaded"}, "Retry-After", "30")
	})
	ctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
	defer cancel()
	_, err := Call[any](ctx, newClient(t, f.URL).Service("S"), "h", nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
}

func TestRequestTimeoutIsANetworkError(t *testing.T) {
	fakeSleep(t)
	var n atomic.Int32
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		if n.Add(1) == 1 {
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
			return
		}
		reply(w, 200, completed("second", testID))
	})
	got, err := Call[string](bg, newClient(t, f.URL, WithRequestTimeout(50*time.Millisecond)).Service("S"), "h", nil)
	if err != nil {
		t.Fatal(err)
	}
	equal(t, got, "second")
	seen := f.requests()
	equal(t, seen[0].Header.Get("Idempotency-Key"), seen[1].Header.Get("Idempotency-Key"))
}

func TestARefusedConnectionIsANetworkError(t *testing.T) {
	fakeSleep(t)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	_, err = Call[any](bg, newClient(t, addr, WithMaxAttempts(2)).Service("S"), "h", nil)
	var ne *NetworkError
	if !errors.As(err, &ne) {
		t.Fatalf("got %v, want *NetworkError", err)
	}
}
