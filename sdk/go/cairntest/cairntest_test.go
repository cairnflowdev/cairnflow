// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairntest_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cairn "github.com/cairnflowdev/cairnflow/sdk/go"
	"github.com/cairnflowdev/cairnflow/sdk/go/cairntest"
)

func run(t *testing.T, inv *cairntest.Invocation, want cairntest.Status) {
	t.Helper()
	st, err := inv.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if st != want {
		t.Fatalf("status %s, want %s (pending %+v)", st, want, inv.Pending())
	}
}

func output[T any](t *testing.T, inv *cairntest.Invocation) T {
	t.Helper()
	var v T
	if err := inv.Output(&v); err != nil {
		t.Fatalf("Output: %v", err)
	}
	return v
}

// noLeak checks, within a bound, that no goroutine outlives the test's invocations.
func noLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	t.Cleanup(func() {
		deadline := time.Now().Add(5 * time.Second)
		for runtime.NumGoroutine() > before {
			if time.Now().After(deadline) {
				t.Errorf("%d goroutines outlived the invocation", runtime.NumGoroutine()-before)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

func TestReplayRunsEachSideEffectOnce(t *testing.T) {
	noLeak(t)
	var effects atomic.Int64
	svc := cairn.NewObject("Counter", cairn.Exclusive("bump", func(ctx cairn.ObjectContext, n int) (int, error) {
		v, _, err := cairn.Get[int](ctx, "v")
		if err != nil {
			return 0, err
		}
		for i := range n {
			if _, err := cairn.Run(ctx, fmt.Sprintf("step-%d", i), func(cairn.RunContext) (int, error) {
				return int(effects.Add(1)), nil
			}); err != nil {
				return 0, err
			}
		}
		keys, err := cairn.StateKeys(ctx)
		if err != nil || len(keys) != 0 {
			return 0, fmt.Errorf("keys %v %v", keys, err)
		}
		return v + n, cairn.Set(ctx, "v", v+n)
	}))
	inv := cairntest.New(svc, "bump", 3, cairntest.Key("k"))
	run(t, inv, cairntest.Completed)
	if got := output[int](t, inv); got != 3 {
		t.Fatalf("output %d", got)
	}
	if effects.Load() != 3 {
		t.Fatalf("%d side effects for 3 steps", effects.Load())
	}
	if inv.Turns() < 4 {
		t.Fatalf("%d turns: each Run result needs the runtime's answer", inv.Turns())
	}
	var v int
	if ok, err := inv.Get("v", &v); !ok || err != nil || v != 3 {
		t.Fatalf("state v = %d %v %v", v, ok, err)
	}
	// Status is stable; Run again does nothing.
	run(t, inv, cairntest.Completed)
}

func TestNondeterminismIsAJournalMismatch(t *testing.T) {
	var turns atomic.Int64
	svc := cairn.NewService("Flaky", cairn.Handler("go", cairn.NoInput(func(ctx cairn.Context) (string, error) {
		// A branch on something outside the journal: different on the replay.
		name := "first"
		if turns.Add(1) > 1 {
			name = "second"
		}
		if _, err := cairn.Run(ctx, name, func(cairn.RunContext) (int, error) { return 1, nil }); err != nil {
			return "", err
		}
		return "done", nil
	})))
	_, err := cairntest.New(svc, "go", nil).Run()
	if err == nil || !strings.Contains(err.Error(), "journal mismatch") {
		t.Fatalf("want a journal mismatch, got %v", err)
	}
}

func TestTransientFailuresAreRetried(t *testing.T) {
	var attempts atomic.Int64
	svc := cairn.NewService("Retry", cairn.Handler("go", cairn.NoInput(func(ctx cairn.Context) (int, error) {
		return cairn.Run(ctx, "call", func(rc cairn.RunContext) (int, error) {
			if n := attempts.Add(1); n < 3 {
				return 0, fmt.Errorf("transient %d", n)
			}
			return rc.Attempt(), nil
		})
	})))
	inv := cairntest.New(svc, "go", nil)
	run(t, inv, cairntest.Completed)
	if got := output[int](t, inv); got != 2 {
		t.Fatalf("the closure saw attempt %d, want 2", got)
	}

	attempts.Store(-100)
	_, err := cairntest.New(svc, "go", nil, cairntest.MaxAttempts(2)).Run()
	if err == nil || !strings.Contains(err.Error(), "transient") {
		t.Fatalf("want the last transient failure, got %v", err)
	}
}

func TestPauseAndResume(t *testing.T) {
	var stocked atomic.Bool
	svc := cairn.NewService("Stock", cairn.Handler("ship", cairn.NoInput(func(ctx cairn.Context) (int, error) {
		return cairn.Run(ctx, "read stock", func(cairn.RunContext) (int, error) {
			if !stocked.Load() {
				return 0, cairn.PauseErrorf("restock needed")
			}
			return 10, nil
		})
	})))
	inv := cairntest.New(svc, "ship", nil)
	run(t, inv, cairntest.Paused)
	var pe *cairn.PauseError
	if err := inv.Output(nil); !errors.As(err, &pe) || pe.Error() != "restock needed" {
		t.Fatalf("paused output: %v", err)
	}
	stocked.Store(true)
	run(t, inv, cairntest.Completed) // the resume
	if got := output[int](t, inv); got != 10 {
		t.Fatalf("output %d", got)
	}
}

func TestCancel(t *testing.T) {
	var cleaned atomic.Bool
	svc := cairn.NewService("Long", cairn.Handler("wait", cairn.NoInput(func(ctx cairn.Context) (string, error) {
		_, err := cairn.Signal[string](ctx, "never").Result()
		if err != nil {
			_, _ = cairn.Run(ctx, "cleanup", func(cairn.RunContext) (bool, error) { cleaned.Store(true); return true, nil })
		}
		return "", err
	})))
	inv := cairntest.New(svc, "wait", nil)
	run(t, inv, cairntest.Waiting)
	if p := inv.Pending(); len(p) != 1 || p[0].Kind != cairntest.SignalKind || p[0].Name != "never" {
		t.Fatalf("pending %+v", p)
	}
	inv.Cancel()
	run(t, inv, cairntest.Failed)
	te, ok := cairn.AsTerminalError(inv.Output(nil))
	if !ok || te.Code() != cairn.CancelledCode || !cleaned.Load() {
		t.Fatalf("cancelled: %v, cleanup %v", te, cleaned.Load())
	}
}

func TestSignalsPromisesAttachAndOutput(t *testing.T) {
	type result struct {
		Signal  string `json:"signal"`
		Promise string `json:"promise"`
		Peeked  bool   `json:"peeked"`
		Other   string `json:"other"`
		Late    bool   `json:"late"`
	}
	wf := cairn.NewWorkflow("Flow", cairn.Main("run", func(ctx cairn.WorkflowContext, id string) (result, error) {
		var r result
		var err error
		if r.Signal, err = cairn.Signal[string](ctx, "go").Result(); err != nil {
			return r, err
		}
		if _, r.Peeked, err = cairn.Promise[string](ctx, "own").Peek(); err != nil {
			return r, err
		}
		if err := cairn.Promise[string](ctx, "own").Resolve("mine"); err != nil {
			return r, err
		}
		if r.Promise, err = cairn.Promise[string](ctx, "own").Get(); err != nil {
			return r, err
		}
		if _, r.Late, err = cairn.GetOutput[string](ctx, id); err != nil {
			return r, err
		}
		r.Other, err = cairn.Attach[string](ctx, id).Result()
		return r, err
	}))
	inv := cairntest.New(wf, "run", "inv-2", cairntest.Key("w-1"))
	if err := inv.Signal("go", "early"); err != nil { // before the handler waits
		t.Fatal(err)
	}
	run(t, inv, cairntest.Waiting)
	p := inv.Pending()
	if len(p) != 1 || p[0].Kind != cairntest.OutputKind || p[0].Name != "inv-2" {
		t.Fatalf("pending %+v", p)
	}
	if err := inv.Resolve(p[0], nil); err != nil { // not finished
		t.Fatal(err)
	}
	run(t, inv, cairntest.Waiting)
	p = inv.Pending()
	if len(p) != 1 || p[0].Kind != cairntest.AttachKind {
		t.Fatalf("pending %+v", p)
	}
	if err := inv.Resolve(p[0], "theirs"); err != nil {
		t.Fatal(err)
	}
	if err := inv.Resolve(p[0], "again"); err == nil {
		t.Fatal("resolving twice must fail")
	}
	run(t, inv, cairntest.Completed)
	want := result{Signal: "early", Promise: "mine", Other: "theirs"}
	if got := output[result](t, inv); got != want {
		t.Fatalf("result %+v, want %+v", got, want)
	}
}

func TestPromiseResolvedByTheTest(t *testing.T) {
	wf := cairn.NewWorkflow("Signup", cairn.Main("run", func(ctx cairn.WorkflowContext, _ cairn.Void) (string, error) {
		return cairn.Promise[string](ctx, "email").Get()
	}))
	inv := cairntest.New(wf, "run", nil, cairntest.Key("u"))
	run(t, inv, cairntest.Waiting)
	p := inv.Pending()
	if len(p) != 1 || p[0].Kind != cairntest.PromiseKind || p[0].Name != "email" {
		t.Fatalf("pending %+v", p)
	}
	if err := inv.Reject(p[0], 403, "no email"); err != nil {
		t.Fatal(err)
	}
	run(t, inv, cairntest.Failed)
	if te, ok := cairn.AsTerminalError(inv.Output(nil)); !ok || te.Code() != 403 {
		t.Fatalf("output %v", inv.Output(nil))
	}
}

func TestTimersSendsAndStreams(t *testing.T) {
	start := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	svc := cairn.NewService("Mailer", cairn.Handler("go", cairn.NoInput(func(ctx cairn.Context) (int, error) {
		s, err := ctx.Stream("progress")
		if err != nil {
			return 0, err
		}
		s.Append(map[string]int{"step": 1})
		ctx.Log().Info("hello", "who", "test")
		if err := cairn.Sleep(ctx, 24*time.Hour); err != nil {
			return 0, err
		}
		if err := cairn.Service[cairn.Void](ctx, "Mail", "send").Send("hi", cairn.WithDelay(time.Minute)); err != nil {
			return 0, err
		}
		futs := []cairn.Awaitable{}
		var all []*cairn.Future[int]
		for i := range 3 {
			f := cairn.RunAsync(ctx, fmt.Sprintf("part-%d", i), func(cairn.RunContext) (int, error) { return i * 10, nil })
			all = append(all, f)
			futs = append(futs, f)
		}
		if err := cairn.All(ctx, futs...); err != nil {
			return 0, err
		}
		sum := 0
		for _, f := range all {
			v, _ := f.Result()
			sum += v
		}
		return sum, nil
	})))
	inv := cairntest.New(svc, "go", nil, cairntest.Clock(start))
	run(t, inv, cairntest.Completed)
	if got := output[int](t, inv); got != 30 {
		t.Fatalf("sum %d", got)
	}
	sends := inv.Sends()
	wantAt := start.Add(24*time.Hour + time.Minute)
	if len(sends) != 1 || sends[0].Service != "Mail" || string(sends[0].Input) != `"hi"` || !sends[0].At.Equal(wantAt) {
		t.Fatalf("sends %+v, want one at %v", sends, wantAt)
	}
	if recs := inv.Stream("progress"); len(recs) == 0 || string(recs[0]) != `{"step":1}` {
		t.Fatalf("stream records %s", recs)
	}
	var logs []string
	for _, r := range inv.Stream("$log") {
		var rec struct {
			Msg string `json:"msg"`
		}
		_ = json.Unmarshal(r, &rec)
		logs = append(logs, rec.Msg)
	}
	if !slices.Contains(logs, "hello") {
		t.Fatalf("log records %v", logs)
	}
	if j := inv.Journal(); j[0] != "Input" || j[len(j)-1] != "Output" {
		t.Fatalf("journal %v", j)
	}
}

func TestContinueAsNew(t *testing.T) {
	svc := cairn.NewService("Loop", cairn.Handler("go", func(ctx cairn.Context, i int) (int, error) {
		if i < 3 {
			return 0, cairn.ContinueAsNew(ctx, i+1)
		}
		return i, nil
	}))
	inv := cairntest.New(svc, "go", 1)
	run(t, inv, cairntest.ContinuedAsNew)
	in, ok := inv.ContinuedWith()
	if !ok || string(in) != "2" {
		t.Fatalf("continued with %s %v", in, ok)
	}
}

func TestStubFailureAndCallPending(t *testing.T) {
	svc := cairn.NewService("Caller", cairn.Handler("go", cairn.NoInput(func(ctx cairn.Context) (string, error) {
		a, err := cairn.Service[string](ctx, "A", "x").Request(1)
		if err != nil {
			return "", err
		}
		b, err := cairn.Object[string](ctx, "B", "key-1", "y").Request("in")
		return a + b, err
	})))
	inv := cairntest.New(svc, "go", nil, cairntest.Stub("A", "x", func(json.RawMessage) (any, error) { return "a", nil }))
	run(t, inv, cairntest.Waiting)
	p := inv.Pending()
	if len(p) != 1 || p[0].Kind != cairntest.CallKind || p[0].Service != "B" || p[0].Key != "key-1" || string(p[0].Input) != `"in"` {
		t.Fatalf("pending %+v", p)
	}
	if err := inv.Resolve(p[0], "b"); err != nil {
		t.Fatal(err)
	}
	run(t, inv, cairntest.Completed)
	if got := output[string](t, inv); got != "ab" {
		t.Fatalf("output %q", got)
	}

	inv = cairntest.New(svc, "go", nil, cairntest.Stub("A", "x", func(json.RawMessage) (any, error) { return nil, errors.New("down") }))
	run(t, inv, cairntest.Failed)
	if te, ok := cairn.AsTerminalError(inv.Output(nil)); !ok || te.Code() != 500 || te.Error() != "down" {
		t.Fatalf("output %v", inv.Output(nil))
	}
}

func TestNewPanicsOnAMistakeInTheTest(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("an unknown handler must panic")
		}
	}()
	cairntest.New(orders, "nope", nil)
}
