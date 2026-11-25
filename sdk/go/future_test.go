// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/scripted"
)

// A race is won by the result earliest in the journal, not by argument order.
func TestWaitFirstPicksJournalOrder(t *testing.T) {
	// Sleep #1; Call declares #2 (id) and #3 (result). The call's result is first.
	d := scripted.New(scripted.Value(3, `"called"`), scripted.Void(1))
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		timer := After(ctx, time.Hour)
		call := Service[string](ctx, "Svc", "h").RequestFuture(nil)
		w, err := WaitFirst(ctx, timer, call)
		if err != nil {
			return "", err
		}
		if w != Awaitable(call) {
			return "timer won", nil
		}
		if timer.Done() {
			return "timer delivered too early", nil
		}
		v, err := call.Result()
		if err != nil {
			return "", err
		}
		if _, err := timer.Result(); err != nil {
			return "", err
		}
		return v, nil
	}))
	wantOutput(t, turn(t, d, h, ""), `"called"`)
	wantCommands(t, d, "Sleep sleep #1", "Call Svc/h #3")
}

// Two futures delivered while waiting on a third: the earlier journal position wins,
// whatever the argument order.
func TestWaitFirstAmongDeliveredUsesPositions(t *testing.T) {
	d := scripted.New(scripted.Void(1), scripted.Value(3, `"c"`), scripted.Value(4, `"r"`))
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		timer := After(ctx, time.Minute)
		call := Service[string](ctx, "Svc", "h").RequestFuture(1)
		r, err := RunAsync(ctx, "r", constant("never runs")).Result()
		if err != nil {
			return "", err
		}
		w, err := WaitFirst(ctx, call, timer)
		if err != nil {
			return "", err
		}
		if w == Awaitable(timer) {
			return "timer first, then " + r, nil
		}
		return "call first", nil
	}))
	wantOutput(t, turn(t, d, h, ""), `"timer first, then r"`)
}

// Wait yields in journal order; Race returns the winner's value.
func TestWaitIteratesInJournalOrder(t *testing.T) {
	d := scripted.New(scripted.Value(2, `"b"`), scripted.Value(3, `"c"`), scripted.Value(1, `"a"`))
	h := Handler("h", NoInput(func(ctx Context) ([]string, error) {
		futs := []*Future[string]{
			RunAsync(ctx, "a", constant("x")),
			RunAsync(ctx, "b", constant("x")),
			RunAsync(ctx, "c", constant("x")),
		}
		var got []string
		for f, err := range Wait(ctx, futs[0], futs[1], futs[2]) {
			if err != nil {
				return nil, err
			}
			v, err := f.(*Future[string]).Result()
			if err != nil {
				return nil, err
			}
			got = append(got, v)
		}
		first, err := Race(ctx, futs...)
		if err != nil {
			return nil, err
		}
		return append(got, first), nil
	}))
	wantOutput(t, turn(t, d, h, ""), `["b","c","a","b"]`)
}

func TestWaitIterStopsOnBreakAndSuspendsOnRest(t *testing.T) {
	d := scripted.New(scripted.Value(2, `2`))
	h := Handler("h", NoInput(func(ctx Context) (int, error) {
		a := RunAsync(ctx, "a", constant(1))
		b := RunAsync(ctx, "b", constant(2))
		it := WaitIter(ctx, a, b)
		if !it.Next() || it.Value() != Awaitable(b) {
			return 0, fmt.Errorf("b should come first")
		}
		it.Next() // a has no result in this turn: the turn suspends here
		return 0, fmt.Errorf("unreachable")
	}))
	out := turn(t, d, h, "")
	wantStop(t, out, core.StopSuspended)
	if d.Suspended == nil || !slices.Equal(d.Suspended.Leaves, []core.Leaf{{Kind: core.CompletionLeaf, ID: 1}}) || d.Suspended.Mode != core.WaitFirst {
		t.Fatalf("suspended on %+v", d.Suspended)
	}
	// Closure a ran (its result is not in the journal); b did not.
	if len(d.Proposals) != 1 || d.Proposals[0].ID != 1 || string(d.Proposals[0].Outcome.Value) != "1" {
		t.Fatalf("proposals %+v", d.Proposals)
	}
}

// All returns the failure earliest in the journal, even if a later one is listed first.
func TestAllReturnsEarliestFailure(t *testing.T) {
	d := scripted.New(
		scripted.Value(1, `1`),
		scripted.Failure(3, 410, "second listed, first in journal"),
		scripted.Failure(2, 400, "first listed"),
	)
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		a := RunAsync(ctx, "a", constant(0))
		b := RunAsync(ctx, "b", constant(0))
		c := RunAsync(ctx, "c", constant(0))
		err := All(ctx, a, b, c)
		te, ok := AsTerminalError(err)
		if !ok {
			return "", fmt.Errorf("want a terminal error, got %v", err)
		}
		return fmt.Sprintf("%d %s", te.Code(), te.Error()), nil
	}))
	wantOutput(t, turn(t, d, h, ""), `"410 second listed, first in journal"`)
}

func TestAllSuspendsWithAllCompletedTree(t *testing.T) {
	d := scripted.New(scripted.Value(2, `2`))
	h := Handler("h", NoInput(func(ctx Context) (Void, error) {
		a := RunAsync(ctx, "a", constant(1))
		b := RunAsync(ctx, "b", constant(2))
		return Void{}, All(ctx, a, b)
	}))
	wantStop(t, turn(t, d, h, ""), core.StopSuspended)
	if d.Suspended.Mode != core.WaitAll || len(d.Suspended.Leaves) != 2 {
		t.Fatalf("suspended on %+v", d.Suspended)
	}
}

// A future that failed before its command was emitted is ready at once but comes last
// in a race (as a future with no journal position in the TypeScript SDK).
func TestFailedFutureLosesToDeliveredOne(t *testing.T) {
	d := scripted.New(scripted.Value(1, `"r"`))
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		r := RunAsync(ctx, "r", constant("x"))
		if _, err := r.Result(); err != nil {
			return "", err
		}
		bad := Service[string](ctx, "Svc", "h").RequestFuture(make(chan int))
		w, err := WaitFirst(ctx, bad, r)
		if err != nil {
			return "", err
		}
		if w != Awaitable(r) {
			return "failed future won", nil
		}
		_, err = bad.Result()
		if err == nil || !strings.Contains(err.Error(), "cannot encode") {
			return "", fmt.Errorf("bad future: %v", err)
		}
		return "ok", nil
	}))
	wantOutput(t, turn(t, d, h, ""), `"ok"`)
	wantCommands(t, d, "Run r #1")
}

// A wait that cannot be satisfied from the journal of a cancelled invocation fails with
// 409; results already in the journal are still returned.
func TestCancelledWaitFails(t *testing.T) {
	d := scripted.New(scripted.Void(1))
	d.Cancelled = true
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		if err := Sleep(ctx, time.Second); err != nil {
			return "", fmt.Errorf("journaled sleep: %w", err)
		}
		err := Sleep(ctx, time.Second)
		te, ok := AsTerminalError(err)
		if !ok || te.Code() != CancelledCode {
			return "", fmt.Errorf("want 409, got %v", err)
		}
		return te.Error(), nil
	}))
	wantOutput(t, turn(t, d, h, ""), `"invocation was cancelled (CANCEL signal)"`)
}

// The shape of the conformance handler `parallelBranches`, with futures: each branch's
// next step starts when its previous result is delivered, in journal order.
func TestParallelBranchesWithFutures(t *testing.T) {
	branches := func(ctx Context) ([][]string, error) {
		type branch struct {
			name string
			step int
			got  []string
			fut  *Future[string]
		}
		bs := []*branch{{name: "a"}, {name: "b"}}
		for _, b := range bs {
			n := b.name + "1"
			b.fut = RunAsync(ctx, n, constant(n))
		}
		for {
			var open []Awaitable
			for _, b := range bs {
				if b.fut != nil {
					open = append(open, b.fut)
				}
			}
			if len(open) == 0 {
				break
			}
			w, err := WaitFirst(ctx, open...)
			if err != nil {
				return nil, err
			}
			for _, b := range bs {
				if b.fut == nil || Awaitable(b.fut) != w {
					continue
				}
				v, err := b.fut.Result()
				if err != nil {
					return nil, err
				}
				b.got = append(b.got, v)
				b.step++
				b.fut = nil
				if b.step < 2 {
					n := fmt.Sprintf("%s%d", b.name, b.step+1)
					b.fut = RunAsync(ctx, n, constant(n))
				}
			}
		}
		return [][]string{bs[0].got, bs[1].got}, nil
	}
	// b1 is durable before a1, so b2 is emitted before a2.
	d := scripted.New(
		scripted.Value(2, `"b1"`), scripted.Value(1, `"a1"`),
		scripted.Value(3, `"b2"`), scripted.Value(4, `"a2"`),
	)
	h := Handler("h", NoInput(branches))
	wantOutput(t, turn(t, d, h, ""), `[["a1","a2"],["b1","b2"]]`)
	wantCommands(t, d, "Run a1 #1", "Run b1 #2", "Run b2 #3", "Run a2 #4")
	if len(d.Proposals) != 0 {
		t.Fatalf("journaled closures ran: %+v", d.Proposals)
	}

	// First turn on an empty journal: both closures run, the turn suspends on both.
	d = scripted.New()
	wantStop(t, turn(t, d, h, ""), core.StopSuspended)
	wantCommands(t, d, "Run a1 #1", "Run b1 #2")
	if len(d.Proposals) != 2 {
		t.Fatalf("proposals %+v", d.Proposals)
	}
	// With acks (BIDI_STREAM), the whole program runs in one turn.
	d = scripted.New()
	d.Ack = true
	out := turn(t, d, h, "")
	wantOutput(t, out, `[["a1","a2"],["b1","b2"]]`)
}

func TestFutureOfAnotherInvocationPanics(t *testing.T) {
	var other *Future[Void]
	d1 := scripted.New(scripted.Void(1))
	turn(t, d1, Handler("h", NoInput(func(ctx Context) (Void, error) {
		other = After(ctx, time.Second)
		return other.Result()
	})), "")
	d2 := scripted.New()
	out := turn(t, d2, Handler("h", NoInput(func(ctx Context) (Void, error) {
		_, err := WaitFirst(ctx, other)
		return Void{}, err
	})), "")
	if out.Kind != core.Panicked || !strings.Contains(out.Failure.Message, "another invocation") {
		t.Fatalf("outcome %s %+v", out.Kind, out.Failure)
	}
}
