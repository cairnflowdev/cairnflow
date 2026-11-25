// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/scripted"
)

// ── stopping the turn ────────────────────────────────────────────────────

// A suspension unwinds the handler goroutine: defers run, recover sees nothing.
func TestStopRunsDefersAndCannotBeRecovered(t *testing.T) {
	var deferRan, afterWait bool
	var recovered any = "not called"
	d := scripted.New()
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		defer func() {
			deferRan = true
			recovered = recover()
		}()
		err := Sleep(ctx, time.Second)
		afterWait = true
		return "", err
	}))
	wantStop(t, turn(t, d, h, ""), core.StopSuspended)
	if !deferRan || afterWait || recovered != nil {
		t.Fatalf("deferRan=%v afterWait=%v recovered=%v", deferRan, afterWait, recovered)
	}
}

// A ctx call from a defer while the turn is stopping exits again at once; the outer
// defers still run, and nothing is emitted.
func TestCtxCallInDeferExitsAgain(t *testing.T) {
	var steps []string
	d := scripted.New()
	h := Handler("h", NoInput(func(ctx ObjectContext) (string, error) {
		defer func() { steps = append(steps, "outer") }()
		defer func() {
			steps = append(steps, "inner")
			_ = Set(ctx, "k", 1)
			steps = append(steps, "after set")
		}()
		return "", Sleep(ctx, time.Second)
	}))
	wantStop(t, turn(t, d, h, ""), core.StopSuspended)
	if strings.Join(steps, ",") != "inner,outer" {
		t.Fatalf("steps %v", steps)
	}
	wantCommands(t, d, "Sleep sleep #1")
}

// A user defer that panics while the turn stops does not turn the stop into a failure.
func TestPanicWhileStoppingIsStillAStop(t *testing.T) {
	d := scripted.New()
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		defer func() { panic("cleanup failed") }()
		return "", Sleep(ctx, time.Second)
	}))
	wantStop(t, turn(t, d, h, ""), core.StopSuspended)
}

// A stop reached from a defer while the handler panics wins over the panic (as the
// suspended flag wins in the other SDKs).
func TestStopDuringPanicWins(t *testing.T) {
	d := scripted.New()
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		defer func() { _ = Sleep(ctx, time.Second) }()
		panic("boom")
	}))
	wantStop(t, turn(t, d, h, ""), core.StopSuspended)
}

// A journal mismatch reported at a command stops the turn the same way.
func TestMismatchAtCommandStops(t *testing.T) {
	d := scripted.New()
	d.StopAtCommand = 2
	reached := false
	h := Handler("h", NoInput(func(ctx ObjectContext) (string, error) {
		_ = Set(ctx, "a", 1)
		_ = Set(ctx, "b", 2) // the journal has something else here
		reached = true
		return "x", nil
	}))
	wantStop(t, turn(t, d, h, ""), core.StopMismatch)
	if reached {
		t.Fatal("code after the mismatch ran")
	}
}

// Stopping inside a range-over-func loop of Wait unwinds through the iterator.
func TestStopInsideWaitLoop(t *testing.T) {
	d := scripted.New(scripted.Value(2, `2`))
	var seen []int
	h := Handler("h", NoInput(func(ctx Context) (Void, error) {
		a := RunAsync(ctx, "a", constant(1))
		b := RunAsync(ctx, "b", constant(2))
		for f, err := range Wait(ctx, a, b) {
			if err != nil {
				return Void{}, err
			}
			v, _ := f.(*Future[int]).Result()
			seen = append(seen, v)
		}
		return Void{}, nil
	}))
	wantStop(t, turn(t, d, h, ""), core.StopSuspended)
	if len(seen) != 1 || seen[0] != 2 {
		t.Fatalf("seen %v", seen)
	}
}

func TestPanicIsRetryable(t *testing.T) {
	d := scripted.New()
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		var m map[string]int
		//lint:ignore SA5000 the test needs a runtime panic raised by the Go runtime itself
		m["x"] = 1 // nil map write
		return "", nil
	}))
	out := turn(t, d, h, "")
	if out.Kind != core.Panicked || out.Failure.Kind != core.RetryFailure || out.Failure.Code != 500 {
		t.Fatalf("outcome %s %+v", out.Kind, out.Failure)
	}
	if !strings.Contains(out.Failure.Message, "assignment to entry in nil map") || !strings.Contains(out.Failure.Stack, "exec_test.go") {
		t.Fatalf("failure %q, stack:\n%s", out.Failure.Message, out.Failure.Stack)
	}
}

func TestUserGoexitIsRetryable(t *testing.T) {
	d := scripted.New()
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		runtime.Goexit()
		return "", nil
	}))
	out := turn(t, d, h, "")
	if out.Kind != core.Panicked || !strings.Contains(out.Failure.Message, "runtime.Goexit") {
		t.Fatalf("outcome %s %+v", out.Kind, out.Failure)
	}
}

func TestReturnedErrorsAreClassified(t *testing.T) {
	cases := []struct {
		err  error
		kind core.FailureKind
		code uint32
	}{
		{TerminalErrorf(400, "bad %s", "input"), core.TerminalFailure, 400},
		{fmt.Errorf("wrapped: %w", TerminalErrorf(404, "missing")), core.TerminalFailure, 404},
		{PauseErrorf("restock needed"), core.PauseFailure, 500},
		{ToPauseError(errors.New("x"), WithCode(503)), core.PauseFailure, 503},
		{RetryableErrorf(429, "slow down"), core.RetryFailure, 429},
		{errors.New("plain"), core.RetryFailure, 500},
	}
	for _, c := range cases {
		d := scripted.New()
		out := turn(t, d, Handler("h", NoInput(func(ctx Context) (Void, error) { return Void{}, c.err })), "")
		if out.Kind != core.Failed || out.Failure.Kind != c.kind || out.Failure.Code != c.code || out.Failure.Message != c.err.Error() {
			t.Errorf("%v: outcome %s %+v", c.err, out.Kind, out.Failure)
		}
	}
	d := scripted.New()
	out := turn(t, d, Handler("h", NoInput(func(ctx Context) (Void, error) {
		return Void{}, ToRetryableError(errors.New("later"), WithRetryAfter(3*time.Second))
	})), "")
	if out.Failure.RetryAfter != 3*time.Second {
		t.Fatalf("retry after %v", out.Failure.RetryAfter)
	}
}

func TestContinueAsNew(t *testing.T) {
	d := scripted.New()
	var deferRan bool
	h := Handler("h", func(ctx Context, n int) (int, error) {
		defer func() { deferRan = true }()
		if err := ContinueAsNew(ctx, map[string]int{"n": n + 1}); err != nil {
			return 0, err
		}
		return 0, errors.New("unreachable")
	})
	out := turn(t, d, h, "41")
	if out.Kind != core.ContinuedAsNew || string(out.ContinueInput) != `{"n":42}` || !deferRan {
		t.Fatalf("outcome %s %q deferRan=%v", out.Kind, out.ContinueInput, deferRan)
	}
	d = scripted.New()
	d.InfoValue.WireVersion = 2
	out = turn(t, d, h, "1")
	if out.Kind != core.Failed || out.Failure.Kind != core.TerminalFailure || !strings.Contains(out.Failure.Message, "wire v3") {
		t.Fatalf("outcome %s %+v", out.Kind, out.Failure)
	}
}

// ── the goroutine guard ──────────────────────────────────────────────────

// A ctx call from a goroutine the user started stops that goroutine (its defers run,
// recover sees nothing, the process lives on) and fails the turn as retryable, even
// when the handler then returns normally. Nothing reaches the driver.
func TestCtxFromAnotherGoroutineFailsTheTurn(t *testing.T) {
	for name, misuse := range map[string]func(ObjectContext){
		"Set":  func(ctx ObjectContext) { _ = Set(ctx, "k", 1) },
		"Rand": func(ctx ObjectContext) { ctx.Rand() },
	} {
		t.Run(name, func(t *testing.T) {
			d := scripted.New()
			var after, deferRan atomic.Bool
			var recovered atomic.Value
			h := Handler("h", NoInput(func(ctx ObjectContext) (string, error) {
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() {
						deferRan.Store(true)
						if r := recover(); r != nil {
							recovered.Store(fmt.Sprint(r))
						}
					}()
					misuse(ctx)
					after.Store(true)
				}()
				wg.Wait()
				// Immutable accessors are fine from anywhere.
				done := make(chan string)
				go func() { done <- ctx.InvocationID() }()
				return <-done, nil
			}))
			out := turn(t, d, h, "")
			if out.Kind != core.Panicked || out.Failure.Kind != core.RetryFailure ||
				!strings.Contains(out.Failure.Message, "goroutine other than the handler's") {
				t.Fatalf("outcome %s %+v", out.Kind, out.Failure)
			}
			if after.Load() || !deferRan.Load() || recovered.Load() != nil {
				t.Fatalf("after=%v deferRan=%v recovered=%v", after.Load(), deferRan.Load(), recovered.Load())
			}
			wantCommands(t, d)
		})
	}
}

// Once another goroutine misused the context, the handler's next ctx call fails the
// turn at once instead of reaching the driver.
func TestCtxAfterMisuseFailsAtOnce(t *testing.T) {
	d := scripted.New()
	reached := false
	h := Handler("h", NoInput(func(ctx ObjectContext) (string, error) {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = Set(ctx, "a", 1)
		}()
		wg.Wait()
		_ = Set(ctx, "b", 2)
		reached = true
		return "x", nil
	}))
	out := turn(t, d, h, "")
	if out.Kind != core.Panicked || reached {
		t.Fatalf("outcome %s reached=%v", out.Kind, reached)
	}
	wantCommands(t, d)
}

func TestCtxInsideRunClosureFailsTheTurn(t *testing.T) {
	d := scripted.New()
	h := Handler("h", NoInput(func(ctx ObjectContext) (int, error) {
		return Run(ctx, "bad", func(RunContext) (int, error) {
			_ = Set(ctx, "k", 1)
			return 1, nil
		})
	}))
	out := turn(t, d, h, "")
	wantStop(t, out, core.StopFailed)
	if !strings.Contains(out.Stop.Cause.Error(), "inside a Run closure") {
		t.Fatalf("cause %v", out.Stop.Cause)
	}
}

// ── Run closures ─────────────────────────────────────────────────────────

// Two closures meet each other: they run at the same time, on their own goroutines.
func TestRunClosuresRunConcurrently(t *testing.T) {
	d := scripted.New()
	d.Ack = true
	aIn, bIn := make(chan struct{}), make(chan struct{})
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		a := RunAsync(ctx, "a", func(RunContext) (string, error) {
			close(aIn)
			<-bIn
			return "a", nil
		})
		b := RunAsync(ctx, "b", func(RunContext) (string, error) {
			close(bIn)
			<-aIn
			return "b", nil
		})
		if err := All(ctx, a, b); err != nil {
			return "", err
		}
		va, _ := a.Result()
		vb, _ := b.Result()
		return va + vb, nil
	}))
	wantOutput(t, turn(t, d, h, ""), `"ab"`)
}

// The handler returning does not abandon a closure nobody waited for: Execute returns
// only once it has ended, and the driver has its proposal.
func TestExecuteWaitsForUnawaitedClosures(t *testing.T) {
	d := scripted.New()
	var finished atomic.Bool
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		RunAsync(ctx, "background", func(RunContext) (int, error) {
			defer finished.Store(true)
			time.Sleep(20 * time.Millisecond) // a side effect taking a while
			return 1, nil
		})
		return "returned", nil
	}))
	out := turn(t, d, h, "")
	wantOutput(t, out, `"returned"`)
	if !finished.Load() {
		t.Fatal("Execute returned before the closure ended")
	}
	d.Await(core.Wait{Leaves: []core.Leaf{{Kind: core.CompletionLeaf, ID: 1}}}) // collects the proposal
	if len(d.Proposals) != 1 || string(d.Proposals[0].Outcome.Value) != "1" {
		t.Fatalf("proposals %+v", d.Proposals)
	}
}

func TestRunJournaledDoesNotExecute(t *testing.T) {
	d := scripted.New(scripted.Value(1, `{"n":7}`))
	h := Handler("h", NoInput(func(ctx Context) (int, error) {
		v, err := Run(ctx, "step", func(RunContext) (struct{ N int }, error) {
			panic("must not run")
		})
		return v.N, err
	}))
	wantOutput(t, turn(t, d, h, ""), `7`)
	if len(d.Proposals) != 0 {
		t.Fatal("closure ran")
	}
}

func TestRunTerminalErrorIsADurableResult(t *testing.T) {
	d := scripted.New()
	d.Ack = true
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		_, err := Run(ctx, "charge", func(RunContext) (string, error) {
			return "", TerminalErrorf(402, "card declined")
		})
		te, ok := AsTerminalError(err)
		if !ok {
			return "", fmt.Errorf("want terminal, got %v", err)
		}
		return fmt.Sprintf("%d %s", te.Code(), te.Error()), nil
	}))
	wantOutput(t, turn(t, d, h, ""), `"402 card declined"`)
	p := d.Proposals[0].Outcome
	if p.Failure == nil || p.Failure.Kind != core.TerminalFailure || p.Failure.Code != 402 {
		t.Fatalf("proposal %+v", p)
	}
}

func TestRunPanicAndErrorAreTransient(t *testing.T) {
	for name, fn := range map[string]func(RunContext) (int, error){
		"panic": func(RunContext) (int, error) { panic("boom") },
		"error": func(RunContext) (int, error) { return 0, errors.New("connection reset") },
		"goexit": func(RunContext) (int, error) {
			runtime.Goexit()
			return 0, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			d := scripted.New()
			d.Ack = true
			h := Handler("h", NoInput(func(ctx Context) (int, error) {
				v, err := Run(ctx, "step", fn)
				return v, err
			}))
			out := turn(t, d, h, "")
			wantStop(t, out, core.StopFailed)
			f := d.Proposals[0].Outcome.Failure
			if f == nil || f.Kind != core.RetryFailure {
				t.Fatalf("proposal %+v", d.Proposals[0].Outcome)
			}
			if name == "panic" && (f.Message != "panic: boom" || !strings.Contains(f.Stack, "exec_test.go")) {
				t.Fatalf("failure %q\n%s", f.Message, f.Stack)
			}
		})
	}
}

// A CANCEL arriving while a cancellable closure runs cancels its RunContext; its error
// after that is the cancellation (409), not a result.
func TestCancellableRunIsCancelled(t *testing.T) {
	d := scripted.New()
	d.Ack = true
	started := make(chan struct{})
	go func() {
		<-started
		d.CancelRuns()
	}()
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		_, err := Run(ctx, "llm", func(rc RunContext) (string, error) {
			close(started)
			<-rc.Done()
			return "", rc.Err()
		}, Cancellable())
		te, ok := AsTerminalError(err)
		if !ok || te.Code() != CancelledCode {
			return "", fmt.Errorf("want 409, got %v", err)
		}
		return "cancelled", nil
	}))
	wantOutput(t, turn(t, d, h, ""), `"cancelled"`)
	if !d.Proposals[0].Outcome.Cancelled {
		t.Fatalf("proposal %+v", d.Proposals[0].Outcome)
	}
}

func TestCancellableRunOnCancelledInvocationDoesNotRun(t *testing.T) {
	d := scripted.New()
	d.Cancelled = true
	h := Handler("h", NoInput(func(ctx Context) (int, error) {
		_, err := Run(ctx, "forward", func(RunContext) (int, error) { panic("must not run") }, Cancellable())
		if te, ok := AsTerminalError(err); !ok || te.Code() != CancelledCode {
			return 0, fmt.Errorf("want 409, got %v", err)
		}
		return 1, nil
	}))
	wantOutput(t, turn(t, d, h, ""), `1`)
	wantCommands(t, d, "Run forward #1")
}

func TestRunContextIsAContext(t *testing.T) {
	d := scripted.New()
	d.Ack = true
	h := Handler("h", NoInput(func(ctx Context) (bool, error) {
		return Run(ctx, "io", func(rc RunContext) (bool, error) {
			var c context.Context = rc
			sub, cancel := context.WithTimeout(c, time.Minute)
			defer cancel()
			_, has := sub.Deadline()
			return has && rc.Err() == nil, nil
		})
	}))
	wantOutput(t, turn(t, d, h, ""), `true`)
}
