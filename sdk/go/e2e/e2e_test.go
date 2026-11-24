// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

// The Rust runtime runs Go services end to end, in both transports. Two independent
// implementations of the protocol in two languages: the check that catches drift in the
// header, the message numbers or the protobuf layout that the conformance traces miss.

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cairn "github.com/cairnflowdev/cairnflow/sdk/go"
	"github.com/cairnflowdev/cairnflow/sdk/go/client"
)

// e2eEnv is one runtime with the e2e endpoint registered in one transport.
type e2eEnv struct {
	*node
	mode string
	// run makes keys unique to this test run.
	run string
}

func (e *e2eEnv) key(name string) string { return e.mode[:4] + "-" + e.run + "-" + name }

// TestEndToEnd runs every case in both transports and two configurations: verified
// (IdentityKeys set to the runtime's kid and SegmentOrigins to its value read origin:
// wire v6, and the replay cache in REQUEST_RESPONSE) and the default unverified one (wire
// v5, no replay cache, no segment download).
func TestEndToEnd(t *testing.T) {
	for _, verified := range []bool{true, false} {
		for _, mode := range modes {
			name := mode + "/unverified"
			if verified {
				name = mode + "/verified"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				n := startNode(t)
				opts := append(modeOptions(mode), allServices...)
				wantWire := 5
				if verified {
					opts = append(opts, n.verified(t)...)
					wantWire = 6
				}
				var cached atomic.Int64
				h := cairn.HTTPHandler(opts...)
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				uri := serveOn(t, ln, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					h.ServeHTTP(w, r)
					if w.Header().Get("cairn-replay-cache") != "" {
						cached.Add(1)
					}
				}))
				reg := n.register(t, uri)
				if reg.ProtocolMode != mode {
					t.Fatalf("the runtime settled on %s, want %s", reg.ProtocolMode, mode)
				}
				if reg.WireVersion != wantWire {
					t.Fatalf("negotiated wire v%d, want v%d", reg.WireVersion, wantWire)
				}
				got := slices.Sorted(slices.Values(reg.Services))
				if want := []string{"GoLoops", "GoOrders", "GoPayments", "GoSignup"}; !slices.Equal(got, want) {
					t.Fatalf("registered services %v, want %v", got, want)
				}
				e := &e2eEnv{node: n, mode: mode, run: randomHex(3)}
				t.Run("cases", func(t *testing.T) {
					for _, c := range e2eCases {
						t.Run(c.name, func(t *testing.T) {
							t.Parallel()
							c.run(t, e)
						})
					}
				})
				// The replay cache: held turns only for a verified REQUEST_RESPONSE endpoint.
				switch held := cached.Load(); {
				case verified && mode == rr && held == 0:
					t.Fatal("a verified REQUEST_RESPONSE endpoint never held a journal in its replay cache")
				case (!verified || mode == bidi) && held != 0:
					t.Fatalf("%d turns were held in a replay cache that must be off", held)
				}
			})
		}
	}
}

type e2eCase struct {
	name string
	run  func(t *testing.T, e *e2eEnv)
}

var e2eCases = []e2eCase{
	{"state, durable sleep, Run exactly once", func(t *testing.T, e *e2eEnv) {
		key := e.key("o1")
		obj := e.api.Object("GoOrders", key)
		r, err := client.Call[receipt](bounded(t), obj, "submit", order{Amount: 100})
		if err != nil {
			t.Fatal(err)
		}
		if want := (receipt{Receipt: "receipt-100", Total: 100, Key: key}); r != want {
			t.Fatalf("submit: %+v, want %+v", r, want)
		}
		r, err = client.Call[receipt](bounded(t), obj, "submit", order{Amount: 50})
		if err != nil {
			t.Fatal(err)
		}
		if r.Total != 150 {
			t.Fatalf("second submit total %d, want 150", r.Total)
		}
		if n := counter("charge/" + key).Load(); n != 2 {
			t.Fatalf("the side effect ran %d times for two invocations, want 2", n)
		}
		p, err := client.Call[peekResult](bounded(t), obj, "peek", nil)
		if err != nil {
			t.Fatal(err)
		}
		if p.Total != 150 || !slices.Equal(p.Keys, []string{"total"}) {
			t.Fatalf("peek: %+v", p)
		}
		// Eager state keys, a set and a sleep: the replay after the sleep matches.
		k, err := client.Call[keysResult](bounded(t), obj, "keysAcrossSleep", nil)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(k.Keys, []string{"total"}) || !slices.Equal(k.After, []string{"b", "total"}) {
			t.Fatalf("keysAcrossSleep: %+v", k)
		}
	}},
	{"clear and clear all", func(t *testing.T, e *e2eEnv) {
		r, err := client.Call[clearResult](bounded(t), e.api.Object("GoOrders", e.key("clear")), "clearing", nil)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(r.AfterClear, []string{"y"}) || len(r.AfterClearAll) != 0 || !r.Gone {
			t.Fatalf("clearing: %+v", r)
		}
	}},
	{"deterministic random across a suspension", func(t *testing.T, e *e2eEnv) {
		r, err := client.Call[randomResult](bounded(t), e.api.Object("GoOrders", e.key("rand")), "sameRandom", nil)
		if err != nil {
			t.Fatal(err)
		}
		if !r.Same || len(r.UUID) != 36 {
			t.Fatalf("sameRandom: %+v", r)
		}
	}},
	{"call and send", func(t *testing.T, e *e2eEnv) {
		key := e.key("o2")
		paid, err := client.Call[string](bounded(t), e.api.Object("GoOrders", key), "checkout", order{Amount: 250})
		if err != nil {
			t.Fatal(err)
		}
		if paid != "paid-250" {
			t.Fatalf("checkout: %q", paid)
		}
		select {
		case got := <-mailbox("note/" + key):
			if got != "paid-250" {
				t.Fatalf("note %q", got)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("the one-way note did not arrive")
		}
	}},
	{"a delayed send runs later", func(t *testing.T, e *e2eEnv) {
		tag := e.key("later")
		started := time.Now()
		if _, err := client.Call[cairn.Void](bounded(t), e.api.Service("GoPayments"), "remindLater", tag); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-mailbox("note/" + tag):
			if got != "later" {
				t.Fatalf("note %q", got)
			}
			if d := time.Since(started); d < 300*time.Millisecond {
				t.Fatalf("the delayed send ran after %v, before its 300 ms delay", d)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("the delayed note did not arrive")
		}
	}},
	{"a callee's TerminalError fails the caller with its code", func(t *testing.T, e *e2eEnv) {
		_, err := client.Call[string](bounded(t), e.api.Object("GoOrders", e.key("o3")), "checkout", order{Amount: 5000})
		var f *client.InvocationFailed
		if !errors.As(err, &f) {
			t.Fatalf("want *client.InvocationFailed, got %v", err)
		}
		if f.Code != 402 || !strings.Contains(f.Message, "over quota") {
			t.Fatalf("failure %d %q, want 402 over quota", f.Code, f.Message)
		}
		var te *cairn.TerminalError
		if !errors.As(err, &te) || te.Code() != 402 {
			t.Fatalf("InvocationFailed must read as a cairn.TerminalError 402: %v", err)
		}
	}},
	{"parallel RunAsync and All", func(t *testing.T, e *e2eEnv) {
		out, err := client.Call[[]int](bounded(t), e.api.Service("GoPayments"), "fanout", 4)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(out, []int{0, 1, 4, 9}) {
			t.Fatalf("fanout: %v", out)
		}
	}},
	{"workflow: a shared handler resolves the run's promise", func(t *testing.T, e *e2eEnv) {
		wf := e.api.Workflow("GoSignup", e.key("u1"))
		inv, err := client.Send(bounded(t), wf, "run", "alice")
		if err != nil {
			t.Fatal(err)
		}
		if !within(20*time.Second, func() bool {
			s, err := client.Call[string](bounded(t), wf, "stage", nil)
			return err == nil && s == "waiting"
		}) {
			t.Fatal("the workflow never reached the waiting stage")
		}
		if _, err := client.Call[cairn.Void](bounded(t), wf, "confirm", "A@Example.com"); err != nil {
			t.Fatal(err)
		}
		got, err := client.Attach[signupResult](bounded(t), inv)
		if err != nil {
			t.Fatal(err)
		}
		if got != (signupResult{User: "alice", Email: "a@example.com"}) {
			t.Fatalf("workflow result %+v", got)
		}
		// The run of a workflow happens once per key.
		_, err = client.RunWorkflow[signupResult](bounded(t), wf, "bob")
		var already *client.AlreadyInvoked
		if !errors.As(err, &already) {
			t.Fatalf("a second run for the same key: want *client.AlreadyInvoked, got %v", err)
		}
	}},
	{"workflow: the promise resolved from outside", func(t *testing.T, e *e2eEnv) {
		key := e.key("u2")
		wf := e.api.Workflow("GoSignup", key)
		inv, err := client.Send(bounded(t), wf, "run", "carol")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.api.Promise("GoSignup", key, "email").Resolve(bounded(t), "c@example.com"); err != nil {
			t.Fatal(err)
		}
		got, err := client.Attach[signupResult](bounded(t), inv)
		if err != nil {
			t.Fatal(err)
		}
		if got != (signupResult{User: "carol", Email: "c@example.com"}) {
			t.Fatalf("workflow result %+v", got)
		}
		byKey, err := client.Attach[signupResult](bounded(t), wf)
		if err != nil || byKey != got {
			t.Fatalf("attach by workflow key: %+v %v", byKey, err)
		}
	}},
	{"awakeable resolved from outside", func(t *testing.T, e *e2eEnv) {
		tag := e.key("awk")
		inv, err := client.Send(bounded(t), e.api.Service("GoPayments"), "awaitHuman", tag)
		if err != nil {
			t.Fatal(err)
		}
		var id string
		select {
		case id = <-mailbox("awakeable/" + tag):
		case <-time.After(20 * time.Second):
			t.Fatal("the handler never published its awakeable")
		}
		if _, err := e.api.Awakeable(id).Resolve(bounded(t), map[string]bool{"ok": true}); err != nil {
			t.Fatal(err)
		}
		got, err := client.Attach[map[string]bool](bounded(t), inv)
		if err != nil {
			t.Fatal(err)
		}
		if !got["ok"] {
			t.Fatalf("awakeable result %v", got)
		}
	}},
	{"signals: from outside, from another handler, and a timer that wins", func(t *testing.T, e *e2eEnv) {
		svc := e.api.Service("GoPayments")
		inv, err := client.Send(bounded(t), svc, "raceSignal", 3_600_000)
		if err != nil {
			t.Fatal(err)
		}
		e.waitStatus(t, inv.ID(), "suspended")
		if _, err := inv.Signal(bounded(t), "go", "from-outside"); err != nil {
			t.Fatal(err)
		}
		if got, err := client.Attach[string](bounded(t), inv); err != nil || got != "from-outside" {
			t.Fatalf("race after an outside signal: %q %v", got, err)
		}

		inv, err = client.Send(bounded(t), svc, "raceSignal", 3_600_000)
		if err != nil {
			t.Fatal(err)
		}
		e.waitStatus(t, inv.ID(), "suspended")
		if _, err := client.Call[cairn.Void](bounded(t), svc, "signalOther", signalRequest{Target: inv.ID(), Value: "from-go"}); err != nil {
			t.Fatal(err)
		}
		if got, err := client.Attach[string](bounded(t), inv); err != nil || got != "from-go" {
			t.Fatalf("race after a handler's signal: %q %v", got, err)
		}

		if got, err := client.Call[string](bounded(t), svc, "raceSignal", 200); err != nil || got != "timeout" {
			t.Fatalf("race with no signal: %q %v", got, err)
		}
	}},
	{"attach and get-output of another invocation", func(t *testing.T, e *e2eEnv) {
		svc := e.api.Service("GoPayments")
		target, err := client.Send(bounded(t), svc, "gate", nil)
		if err != nil {
			t.Fatal(err)
		}
		e.waitStatus(t, target.ID(), "suspended")
		p, err := client.Call[peekedOutput](bounded(t), svc, "peekOutput", target.ID())
		if err != nil {
			t.Fatal(err)
		}
		if p.Done {
			t.Fatalf("get-output of a running invocation: %+v", p)
		}
		waiter, err := client.Send(bounded(t), svc, "attachTo", target.ID())
		if err != nil {
			t.Fatal(err)
		}
		e.waitStatus(t, waiter.ID(), "suspended")
		if _, err := target.Signal(bounded(t), "release", "now"); err != nil {
			t.Fatal(err)
		}
		if got, err := client.Attach[string](bounded(t), waiter); err != nil || got != "released-now" {
			t.Fatalf("attach: %q %v", got, err)
		}
		p, err = client.Call[peekedOutput](bounded(t), svc, "peekOutput", target.ID())
		if err != nil {
			t.Fatal(err)
		}
		if !p.Done || p.Value != "released-now" {
			t.Fatalf("get-output of a finished invocation: %+v", p)
		}
	}},
	{"streams, durable log and trace", func(t *testing.T, e *e2eEnv) {
		tag := e.key("chat")
		inv, err := client.Send(bounded(t), e.api.Service("GoPayments"), "chatter", tag)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := client.Attach[string](bounded(t), inv); err != nil || got != "streamed" {
			t.Fatalf("chatter: %q %v", got, err)
		}
		var toks []string
		for ev, err := range e.api.ReadStream(bounded(t), inv.ID(), "tokens") {
			if err != nil {
				t.Fatalf("reading the stream: %v", err)
			}
			if ev.Kind == client.EventRecord {
				var s string
				if err := json.Unmarshal(ev.Data, &s); err != nil {
					t.Fatalf("record %s: %v", ev.Data, err)
				}
				toks = append(toks, s)
			}
			if ev.Kind == client.EventEnd {
				break
			}
		}
		if !slices.Equal(toks, []string{"a", "b", "c"}) {
			t.Fatalf("stream records %v", toks)
		}

		var logs struct {
			Records []struct {
				Msg   string         `json:"msg"`
				Level string         `json:"level"`
				Attrs map[string]any `json:"attrs"`
			} `json:"records"`
		}
		var msgs []string
		if !within(10*time.Second, func() bool {
			st, body := e.must(t, "GET", "/invocations/"+inv.ID()+"/logs", nil)
			if st != http.StatusOK || json.Unmarshal(body, &logs) != nil {
				return false
			}
			msgs = msgs[:0]
			for _, r := range logs.Records {
				msgs = append(msgs, r.Msg)
			}
			return slices.Contains(msgs, "handler log") && slices.Contains(msgs, "closure log")
		}) {
			t.Fatalf("durable log records %v, want the handler's and the closure's", msgs)
		}

		var spans struct {
			Spans []struct {
				Name   string         `json:"name"`
				Status string         `json:"status"`
				Attrs  map[string]any `json:"attrs"`
			} `json:"spans"`
		}
		st, body := e.must(t, "GET", "/invocations/"+inv.ID()+"/spans", nil)
		if st != http.StatusOK {
			t.Fatalf("spans: %d %s", st, body)
		}
		if err := json.Unmarshal(body, &spans); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, s := range spans.Spans {
			if s.Name == "work" && s.Status == "ok" && s.Attrs["tag"] == tag && s.Attrs["tokens"] == float64(3) {
				found = true
			}
		}
		if !found {
			t.Fatalf("no finished span \"work\" with its attributes: %s", body)
		}
	}},
	{"artifacts", func(t *testing.T, e *e2eEnv) {
		text := "artifact body " + e.key("art")
		inv, err := client.Send(bounded(t), e.api.Service("GoPayments"), "artifact", text)
		if err != nil {
			t.Fatal(err)
		}
		got, err := client.Attach[artifactResult](bounded(t), inv)
		if err != nil {
			t.Fatal(err)
		}
		if got.Body != text || got.Size != len(text) || len(got.SHA256) != 64 {
			t.Fatalf("artifact result %+v", got)
		}
		st, body := e.must(t, "GET", "/invocations/"+inv.ID()+"/artifacts", nil)
		if st != http.StatusOK || !strings.Contains(string(body), got.SHA256) {
			t.Fatalf("artifact list: %d %s", st, body)
		}
		st, body = e.must(t, "GET", "/invocations/"+inv.ID()+"/artifacts/"+got.SHA256, nil)
		if st != http.StatusOK || string(body) != text {
			t.Fatalf("artifact bytes: %d %q", st, body)
		}
	}},
	{"continue-as-new: a call gets the end of the chain", func(t *testing.T, e *e2eEnv) {
		got, err := client.Call[loopResult](bounded(t), e.api.Service("GoLoops"), "viaCall", 23)
		if err != nil {
			t.Fatal(err)
		}
		if got.Done != 23 {
			t.Fatalf("loop result %+v", got)
		}
	}},
	{"PauseError pauses; resume after the fix completes it", func(t *testing.T, e *e2eEnv) {
		tag := e.key("stock")
		inv, err := client.Send(bounded(t), e.api.Service("GoPayments"), "shipStock", tag)
		if err != nil {
			t.Fatal(err)
		}
		e.waitStatus(t, inv.ID(), "paused")
		// Resumed before the fix: it pauses again.
		st, body := e.must(t, "POST", "/invocations/"+inv.ID()+"/resume", nil)
		if st != http.StatusOK || !strings.Contains(string(body), `"paused"`) {
			t.Fatalf("resume before the fix: %d %s", st, body)
		}
		flags.Store("restocked/"+tag, true)
		// Known runtime behaviour: two resumes of one paused invocation within the same
		// second, with no journal growth between them, carry the same request identity JWT
		// (same jti, same iat: Ed25519 signs it to the same bytes), and a verifying
		// BIDI_STREAM endpoint refuses a JWT that already opened a stream (401, the resume
		// answers 500). Resume in the next second.
		resumed := time.Now().Unix()
		within(2*time.Second, func() bool { return time.Now().Unix() > resumed })
		st, body = e.must(t, "POST", "/invocations/"+inv.ID()+"/resume", nil)
		if st != http.StatusOK {
			t.Fatalf("resume: %d %s", st, body)
		}
		var got shipped
		output(t, e.settle(t, inv.ID()), &got)
		if got.Shipped != 10 {
			t.Fatalf("shipStock: %+v", got)
		}
	}},
	{"cooperative cancel: the handler cleans up, the invocation fails", func(t *testing.T, e *e2eEnv) {
		tag := e.key("cancel")
		inv, err := client.Send(bounded(t), e.api.Service("GoPayments"), "waitCancel", tag)
		if err != nil {
			t.Fatal(err)
		}
		e.waitStatus(t, inv.ID(), "suspended")
		if _, err := inv.Cancel(bounded(t)); err != nil {
			t.Fatal(err)
		}
		st := e.settle(t, inv.ID())
		if st.Status != "failed" {
			t.Fatalf("a cancelled invocation is %s, want failed", st.Status)
		}
		if n := counter("cleanup/" + tag).Load(); n != 1 {
			t.Fatalf("the cleanup ran %d times, want 1", n)
		}
	}},
}
