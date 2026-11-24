// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

// Turns run through the REQUEST_RESPONSE turn machine directly (no HTTP), for the
// behaviour the conformance traces only cover in BIDI_STREAM or not at all.

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	machine "github.com/cairnflowdev/cairnflow/sdk/go/internal/turn"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

const testClock = 1_800_000_000_000

func input() wire.Message { return &wire.Input{Value: &wire.Value{Content: []byte("null")}} }

func value(s string) *wire.Value { return &wire.Value{Content: []byte(s)} }

// rrTurn runs one turn of h at wire version v with start (nil: a plain Start) and the
// entries after it, checks no goroutine outlives it, and returns the decoded response.
func rrTurn(t *testing.T, h *HandlerDef, v int, start *wire.Start, entries ...wire.Message) []wire.Message {
	t.Helper()
	if start == nil {
		start = &wire.Start{ID: []byte{0xca, 0x1f}, KnownEntries: uint32(len(entries))}
	}
	body := wire.EncodeMessage(start, 0)
	for _, e := range entries {
		body = wire.AppendMessage(body, e, 0)
	}
	before := runtime.NumGoroutine()
	out := machine.RequestResponse(h.body, body, machine.Config{Wire: v, Now: func() int64 { return testClock }})
	noLeak(t, before)
	frames, err := wire.DecodeFrames(out)
	if err != nil {
		t.Fatal(err)
	}
	msgs := make([]wire.Message, len(frames))
	for i, f := range frames {
		if f.Type == wire.TypeProposeRunCompletion && f.RequestedAck() {
			t.Fatalf("REQUEST_RESPONSE must not request acks: %v", f)
		}
		if msgs[i], err = wire.DecodeFrame(f); err != nil {
			t.Fatal(err)
		}
	}
	return msgs
}

func kinds(msgs []wire.Message) string {
	var b []string
	for _, m := range msgs {
		b = append(b, m.Type().String())
	}
	return strings.Join(b, " ")
}

func wantKinds(t *testing.T, msgs []wire.Message, want string) {
	t.Helper()
	if got := kinds(msgs); got != want {
		t.Fatalf("frames %q, want %q", got, want)
	}
}

func wantError(t *testing.T, m wire.Message, code uint32, behavior wire.ErrorBehavior) *wire.ErrorMessage {
	t.Helper()
	e, ok := m.(*wire.ErrorMessage)
	if !ok || e.Code != code || e.Behavior != behavior {
		t.Fatalf("got %#v, want Error code %d behavior %d", m, code, behavior)
	}
	return e
}

func wantOutputFailure(t *testing.T, m wire.Message, code uint32) *wire.Failure {
	t.Helper()
	o, ok := m.(*wire.Output)
	if !ok || o.Failure == nil || o.Failure.Code != code || o.Name != "output" {
		t.Fatalf("got %#v, want a failed Output %d", m, code)
	}
	return o.Failure
}

func wantSuspension(t *testing.T, m wire.Message, want *wire.Suspension) {
	t.Helper()
	s, ok := m.(*wire.Suspension)
	if !ok || string(wire.Marshal(s)) != string(wire.Marshal(want)) {
		t.Fatalf("got %#v, want suspension %#v", m, want)
	}
}

func object(handlers ...*HandlerDef) map[string]*HandlerDef {
	s := NewObject("T", handlers...)
	out := map[string]*HandlerDef{}
	for _, h := range s.handlers {
		out[h.name] = h
	}
	return out
}

var handlers = object(conformance().handlers...)

// The eager snapshot answers `get` in place (GetEagerState carries the value); on replay
// the journaled value wins over the snapshot, and Output is sent once.
func TestRREagerState(t *testing.T) {
	start := &wire.Start{ID: []byte{1}, KnownEntries: 1, StateMap: []wire.StateEntry{{Key: []byte("count"), Value: []byte("1")}}}
	msgs := rrTurn(t, handlers["eagerCount"], 1, start, input())
	wantKinds(t, msgs, "GetEagerState SetState Output End")
	if g := msgs[0].(*wire.GetEagerState); string(g.Key) != "count" || g.Name != "count" || string(g.Value.Content) != "1" {
		t.Fatalf("GetEagerState %#v", g)
	}
	if s := msgs[1].(*wire.SetState); string(s.Value.Content) != "2" {
		t.Fatalf("SetState %#v", s)
	}
	// Replay: the journal read 5 (the snapshot now says 7); the code sets 6 again.
	start.StateMap[0].Value = []byte("7")
	msgs = rrTurn(t, handlers["eagerCount"], 1, start, input(),
		&wire.GetEagerState{Key: []byte("count"), Name: "count", Result: wire.Result{Value: value("5")}},
		&wire.SetState{Key: []byte("count"), Value: value("6"), Name: "count"},
		&wire.Output{Name: "output", Result: wire.Result{Value: value(`"done"`)}})
	wantKinds(t, msgs, "End")
	// An absent key in a complete snapshot reads as absent, in place.
	start.StateMap = []wire.StateEntry{{Key: []byte("other"), Value: []byte("1")}}
	msgs = rrTurn(t, handlers["eagerCount"], 1, start, input())
	if g := msgs[0].(*wire.GetEagerState); !g.Void {
		t.Fatalf("GetEagerState %#v", g)
	}
	// A partial snapshot without the key reads it lazily.
	start.PartialState = true
	msgs = rrTurn(t, handlers["eagerCount"], 1, start, input())
	wantKinds(t, msgs, "GetState Suspension")
}

// A lazy GetState in the journal stays lazy even when the runtime now sends a snapshot.
func TestRRJournalDecidesEagerOrLazy(t *testing.T) {
	start := &wire.Start{ID: []byte{1}, KnownEntries: 3, StateMap: []wire.StateEntry{{Key: []byte("count"), Value: []byte("4")}}}
	msgs := rrTurn(t, handlers["eagerCount"], 1, start, input(),
		&wire.GetState{Key: []byte("count"), ResultCompletionID: 1, Name: "count"},
		&wire.GetStateCompletion{ID: 1, Result: wire.Result{Value: value("2")}})
	wantKinds(t, msgs, "SetState Output End")
	if s := msgs[0].(*wire.SetState); string(s.Value.Content) != "3" {
		t.Fatalf("SetState %#v", s)
	}
}

func TestRRSuspensionTrees(t *testing.T) {
	// all: one ALL_COMPLETED child.
	msgs := rrTurn(t, handlers["allOf"], 1, nil, input())
	wantKinds(t, msgs, "Sleep Sleep Suspension")
	wantSuspension(t, msgs[2], &wire.Suspension{NestedFutures: []wire.Suspension{{WaitingCompletions: []uint32{1, 2}, CombinatorType: wire.CombinatorAllCompleted}}})
	// all, one leaf delivered: the leaf left alone.
	msgs = rrTurn(t, handlers["allSignal"], 1, nil, input(),
		&wire.Sleep{WakeUpTime: testClock + 1000, ResultCompletionID: 1, Name: "sleep"},
		&wire.SleepCompletion{ID: 1, Void: true})
	wantKinds(t, msgs, "Suspension")
	wantSuspension(t, msgs[0], &wire.Suspension{WaitingSignals: []uint32{4}})
	// race: the awakeable (signal 17) and the sleep on one FIRST node.
	msgs = rrTurn(t, handlers["raceAwakeable"], 1, nil, input())
	wantKinds(t, msgs, "Sleep Suspension")
	wantSuspension(t, msgs[1], &wire.Suspension{WaitingCompletions: []uint32{1}, WaitingSignals: []uint32{17}})
	// The signal arrives: it wins, with its value.
	msgs = rrTurn(t, handlers["allSignal"], 1, nil, input(),
		&wire.Sleep{WakeUpTime: testClock + 1000, ResultCompletionID: 1, Name: "sleep"},
		&wire.SignalNotification{Signal: wire.SignalIdx(4), Result: wire.Result{Value: value("42")}},
		&wire.SleepCompletion{ID: 1, Void: true})
	wantKinds(t, msgs, "Output End")
	if o := msgs[0].(*wire.Output); string(o.Value.Content) != "42" {
		t.Fatalf("output %#v", o)
	}
}

func TestRRPromise(t *testing.T) {
	msgs := rrTurn(t, handlers["promise"], 1, nil, input())
	wantKinds(t, msgs, "CompletePromise Suspension")
	c := msgs[0].(*wire.CompletePromise)
	if c.Key != "approve" || c.Name != "promise/approve" || string(c.CompletionValue.Content) != `"ok"` || c.ResultCompletionID != 1 {
		t.Fatalf("CompletePromise %#v", c)
	}
	msgs = rrTurn(t, handlers["promise"], 1, nil, input(), c,
		&wire.CompletePromiseCompletion{ID: 1, Result: wire.Result{Void: true}})
	wantKinds(t, msgs, "GetPromise Suspension")
	// The promise already had a result: a terminal error.
	msgs = rrTurn(t, handlers["promise"], 1, nil, input(), c,
		&wire.CompletePromiseCompletion{ID: 1, Result: wire.Result{Failure: &wire.Failure{Code: 409, Message: "already completed"}}})
	wantKinds(t, msgs, "Output End")
	wantOutputFailure(t, msgs[0], 409)
}

// A PauseError returned from a Run closure pauses the invocation, as one returned by the
// handler does (and as the other SDKs do): nothing is journaled, so a resume runs the
// closure again and sees the fixed world. Treated as a transient failure instead, the
// invocation is retried forever and never waits for the human.
func TestRRPauseErrorFromRunPauses(t *testing.T) {
	h := Handler("h", NoInput(func(ctx Context) (int, error) {
		return Run(ctx, "read stock", func(RunContext) (int, error) {
			return 0, PauseErrorf("out of stock: restock needed")
		})
	}))
	msgs := rrTurn(t, h, 1, nil, input())
	wantKinds(t, msgs, "Run Error")
	if e := wantError(t, msgs[1], 500, wire.BehaviorPause); e.Message != "out of stock: restock needed" {
		t.Fatalf("pause message %q", e.Message)
	}
	// With a code, and through a wrapping error.
	h = Handler("h", NoInput(func(ctx Context) (int, error) {
		return Run(ctx, "read stock", func(RunContext) (int, error) {
			return 0, fmt.Errorf("stock service: %w", ToPauseError(errors.New("empty"), WithCode(503)))
		})
	}))
	msgs = rrTurn(t, h, 1, nil, input())
	wantKinds(t, msgs, "Run Error")
	wantError(t, msgs[1], 503, wire.BehaviorPause)
}

func TestRRHandlerErrors(t *testing.T) {
	h := func(err error) *HandlerDef {
		return Handler("h", NoInput(func(Context) (Void, error) { return Void{}, err }))
	}
	msgs := rrTurn(t, h(TerminalErrorf(400, "bad order")), 1, nil, input())
	wantKinds(t, msgs, "Output End")
	if f := wantOutputFailure(t, msgs[0], 400); f.Message != "bad order" {
		t.Fatalf("failure %#v", f)
	}
	msgs = rrTurn(t, h(PauseErrorf("needs a human")), 1, nil, input())
	wantKinds(t, msgs, "Error")
	wantError(t, msgs[0], 500, wire.BehaviorPause)
	msgs = rrTurn(t, h(errors.New("flaky")), 1, nil, input())
	wantError(t, msgs[0], 500, wire.BehaviorRetry)
	msgs = rrTurn(t, h(RetryableErrorf(503, "busy")), 1, nil, input())
	if e := wantError(t, msgs[0], 503, wire.BehaviorRetry); e.NextRetryDelay != nil {
		t.Fatalf("no retry delay was asked: %#v", e)
	}
	msgs = rrTurn(t, h(ToRetryableError(errors.New("busy"), WithRetryAfter(1500*time.Millisecond))), 1, nil, input())
	if e := wantError(t, msgs[0], 500, wire.BehaviorRetry); e.NextRetryDelay == nil || *e.NextRetryDelay != 1500 {
		t.Fatalf("retry delay %#v", e)
	}
	panicky := Handler("p", NoInput(func(Context) (Void, error) { panic("boom") }))
	msgs = rrTurn(t, panicky, 1, nil, input())
	if e := wantError(t, msgs[0], 500, wire.BehaviorRetry); !strings.Contains(e.Message, "panic: boom") {
		t.Fatalf("error %#v", e)
	}
}

func TestRRRunOutcomes(t *testing.T) {
	transient := Handler("h", NoInput(func(ctx Context) (string, error) {
		return Run(ctx, "step", func(RunContext) (string, error) { return "", errors.New("flaky") })
	}))
	msgs := rrTurn(t, transient, 1, nil, input())
	wantKinds(t, msgs, "Run Error")
	wantError(t, msgs[1], 500, wire.BehaviorRetry)

	terminal := Handler("h", NoInput(func(ctx Context) (string, error) {
		return Run(ctx, "step", func(RunContext) (string, error) { return "", TerminalErrorf(422, "no such card") })
	}))
	msgs = rrTurn(t, terminal, 1, nil, input())
	wantKinds(t, msgs, "Run ProposeRunCompletion Suspension")
	if p := msgs[1].(*wire.ProposeRunCompletion); p.Failure == nil || p.Failure.Code != 422 || p.Failure.Message != "no such card" {
		t.Fatalf("proposal %#v", p)
	}
	// Replayed: the closure does not run again, the same terminal error comes back.
	msgs = rrTurn(t, terminal, 1, nil, input(),
		&wire.Run{ResultCompletionID: 1, Name: "step"},
		&wire.RunCompletion{ID: 1, Result: wire.Result{Failure: &wire.Failure{Code: 422, Message: "no such card"}}})
	wantKinds(t, msgs, "Output End")
	wantOutputFailure(t, msgs[0], 422)

	// A run nobody waits for still suspends the turn on its proposal.
	unawaited := Handler("h", NoInput(func(ctx Context) (string, error) {
		RunAsync(ctx, "fire", constant(1))
		return "done", nil
	}))
	msgs = rrTurn(t, unawaited, 1, nil, input())
	wantKinds(t, msgs, "Run ProposeRunCompletion Suspension")
	wantSuspension(t, msgs[2], &wire.Suspension{WaitingCompletions: []uint32{1}})
}

// Proposals sit right after the commands of the delivery they were started in, in command
// order, whatever order the closures finish in.
func TestRRProposalsInCommandOrder(t *testing.T) {
	release := make(chan struct{})
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		slow := RunAsync(ctx, "slow", func(RunContext) (string, error) { <-release; return "s", nil })
		fast := RunAsync(ctx, "fast", func(RunContext) (string, error) { defer close(release); return "f", nil })
		if err := All(ctx, slow, fast); err != nil {
			return "", err
		}
		return "both", nil
	}))
	msgs := rrTurn(t, h, 1, nil, input())
	wantKinds(t, msgs, "Run Run ProposeRunCompletion ProposeRunCompletion Suspension")
	if a, b := msgs[2].(*wire.ProposeRunCompletion), msgs[3].(*wire.ProposeRunCompletion); a.ResultCompletionID != 1 || b.ResultCompletionID != 2 {
		t.Fatalf("proposals %d then %d", a.ResultCompletionID, b.ResultCompletionID)
	}
	// Both runs proposed and the ALL wait: a FIRST root with the runs and the ALL child.
	wantSuspension(t, msgs[4], &wire.Suspension{
		WaitingCompletions: []uint32{1, 2},
		NestedFutures:      []wire.Suspension{{WaitingCompletions: []uint32{1, 2}, CombinatorType: wire.CombinatorAllCompleted}},
	})
}

func TestRRJournalMismatchOnOutput(t *testing.T) {
	h := Handler("h", NoInput(func(Context) (string, error) { return "x", nil }))
	msgs := rrTurn(t, h, 1, nil, input(), &wire.Sleep{WakeUpTime: 1, ResultCompletionID: 1, Name: "sleep"})
	wantKinds(t, msgs, "Error")
	if e := wantError(t, msgs[0], wire.JournalMismatch, wire.BehaviorPause); !strings.Contains(e.Message, "journal has SLEEP, current code produced OUTPUT") {
		t.Fatalf("message %q", e.Message)
	}
}

func TestRRReplayOnly(t *testing.T) {
	start := &wire.Start{ID: []byte{1}, KnownEntries: 2, ReplayOnly: true}
	// A mismatch ends with a report, not an Error.
	msgs := rrTurn(t, handlers["counterChanged"], 3, start, input(),
		&wire.GetState{Key: []byte("count"), ResultCompletionID: 1, Name: "count"},
		&wire.GetStateCompletion{ID: 1, Result: wire.Result{Value: value("0")}},
		&wire.Sleep{WakeUpTime: testClock + 1000, ResultCompletionID: 2, Name: "sleep"})
	wantKinds(t, msgs, "ReplayReport")
	if r := msgs[0].(*wire.ReplayReport); !r.Mismatch || r.Matched != 1 || r.Expected != "SLEEP" || r.Got != "SET_STATE" {
		t.Fatalf("report %#v", r)
	}
	// Below v3 the flag is not read: the turn runs for real.
	msgs = rrTurn(t, handlers["counterChanged"], 2, start, input())
	wantKinds(t, msgs, "GetState Suspension")
	// A journaled Run without its result: the closure does not run in a trial.
	ran := false
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		return Run(ctx, "step", func(RunContext) (string, error) { ran = true; return "", nil })
	}))
	msgs = rrTurn(t, h, 3, start, input(), &wire.Run{ResultCompletionID: 1, Name: "step"})
	wantKinds(t, msgs, "Suspension")
	if ran {
		t.Fatal("a trial replay ran a closure")
	}
}

func TestRRCancelled(t *testing.T) {
	cancel := &wire.SignalNotification{Signal: wire.SignalIdx(wire.SignalCancel), Result: wire.Result{Void: true}}
	msgs := rrTurn(t, handlers["outOfOrder"], 1, nil, input(), cancel)
	wantKinds(t, msgs, "Sleep Sleep Output End")
	wantOutputFailure(t, msgs[2], CancelledCode)
	// A cancellable run without a result does not run; a plain one does.
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		return Run(ctx, "step", constant("x"), Cancellable())
	}))
	msgs = rrTurn(t, h, 1, nil, input(), cancel)
	wantKinds(t, msgs, "Run Output End")
	wantOutputFailure(t, msgs[1], CancelledCode)
	msgs = rrTurn(t, handlers["sleepRunSleep"], 1, nil, input(),
		&wire.Sleep{WakeUpTime: testClock + 1000, ResultCompletionID: 1, Name: "sleep"},
		&wire.Sleep{WakeUpTime: testClock + 2000, ResultCompletionID: 2, Name: "sleep"},
		cancel)
	wantKinds(t, msgs, "Run ProposeRunCompletion Suspension")
}

func TestRRContinueAsNew(t *testing.T) {
	h := Handler("h", func(ctx Context, n int) (Void, error) {
		return Void{}, ContinueAsNew(ctx, n+1)
	})
	msgs := rrTurn(t, h, 3, nil, &wire.Input{Value: value("1")})
	wantKinds(t, msgs, "ContinueAsNew")
	if c := msgs[0].(*wire.ContinueAsNew); string(c.Input) != "2" {
		t.Fatalf("ContinueAsNew %#v", c)
	}
	msgs = rrTurn(t, h, 2, nil, &wire.Input{Value: value("1")})
	wantKinds(t, msgs, "Output End")
	wantOutputFailure(t, msgs[0], 500)
}

func TestRRCommandFields(t *testing.T) {
	h := Handler("h", NoInput(func(ctx Context) (Void, error) {
		scoped, err := ctx.Scoped("tenant-a", "k1")
		if err != nil {
			return Void{}, err
		}
		if err := Object[Void](scoped, "Cart", "c1", "add").Send(2, WithIdempotencyKey(""), WithDelay(1500*time.Microsecond)); err != nil {
			return Void{}, err
		}
		if err := Service[Void](ctx, "Mail", "send").Send("hi", WithTags(map[string]string{"b": "2", "a": "1"})); err != nil {
			return Void{}, err
		}
		if err := SendSignal(ctx, "inv1", "approve", nil); err != nil {
			return Void{}, err
		}
		if err := SendSignalIndex(ctx, "inv1", 17, "ok"); err != nil {
			return Void{}, err
		}
		Attach[Void](ctx, "inv2")
		_, _, err = GetOutput[Void](ctx, "inv3")
		return Void{}, err
	}))
	msgs := rrTurn(t, h, 2, nil, input())
	wantKinds(t, msgs, "OneWayCall OneWayCall SendSignal SendSignal AttachInvocation GetInvocationOutput Suspension")
	a := msgs[0].(*wire.OneWayCall)
	if a.Scope == nil || *a.Scope != "tenant-a" || a.LimitKey == nil || *a.LimitKey != "k1" || a.IdempotencyKey == nil ||
		*a.IdempotencyKey != "" || a.Key != "c1" || a.InvokeTime != testClock+2 || a.Name != "Cart/add" || a.InvocationIDNotificationIdx != 1 {
		t.Fatalf("OneWayCall %#v", a)
	}
	b := msgs[1].(*wire.OneWayCall)
	if b.Scope != nil || b.LimitKey != nil || b.IdempotencyKey != nil || b.InvokeTime != 0 || len(b.Tags) != 2 || b.Tags[0].Key != "a" {
		t.Fatalf("OneWayCall %#v", b)
	}
	if s := msgs[2].(*wire.SendSignal); s.Name != "signal/approve" || !s.Void || s.Signal.Name == nil || *s.Signal.Name != "approve" {
		t.Fatalf("SendSignal %#v", s)
	}
	if s := msgs[3].(*wire.SendSignal); s.Name != "signal/17" || s.Signal.Idx == nil || *s.Signal.Idx != 17 || string(s.Value.Content) != `"ok"` {
		t.Fatalf("SendSignal %#v", s)
	}
	if m := msgs[4].(*wire.AttachInvocation); m.InvocationID != "inv2" || m.Name != "attach" || m.ResultCompletionID != 3 {
		t.Fatalf("AttachInvocation %#v", m)
	}
	if m := msgs[5].(*wire.GetInvocationOutput); m.InvocationID != "inv3" || m.Name != "getOutput" || m.ResultCompletionID != 4 {
		t.Fatalf("GetInvocationOutput %#v", m)
	}
}

func TestRRArtifacts(t *testing.T) {
	h := Handler("h", NoInput(func(ctx Context) (ArtifactRef, error) {
		return Run(ctx, "fetch", func(rc RunContext) (ArtifactRef, error) {
			return rc.Artifacts().Put([]byte("big bytes"), "text/plain")
		})
	}))
	msgs := rrTurn(t, h, 4, nil, input())
	wantKinds(t, msgs, "Run ArtifactPut ProposeRunCompletion Suspension")
	if p := msgs[1].(*wire.ArtifactPut); string(p.Content) != "big bytes" || p.ContentType != "text/plain" {
		t.Fatalf("ArtifactPut %#v", p)
	}
	// Below v4 the put fails the closure (retried), it is never dropped silently.
	msgs = rrTurn(t, h, 3, nil, input())
	wantKinds(t, msgs, "Run Error")
}

func TestRRBadRequests(t *testing.T) {
	h := handlers["counter"]
	for name, body := range map[string][]byte{
		"empty":       nil,
		"not a frame": {1, 2, 3},
		"no Start":    wire.EncodeMessage(input(), 0),
	} {
		out := machine.RequestResponse(h.body, body, machine.Config{Wire: 1})
		frames, err := wire.DecodeFrames(out)
		if err != nil || len(frames) != 1 {
			t.Fatalf("%s: %v %v", name, frames, err)
		}
		m, _ := wire.DecodeFrame(frames[0])
		wantError(t, m, wire.ProtocolViolation, wire.BehaviorFail)
	}
}
