// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/scripted"
)

var _ core.Driver = (*scripted.Driver)(nil)

// turn runs one turn of h on the scripted driver and checks that no goroutine outlives
// it.
func turn(t *testing.T, d *scripted.Driver, h *HandlerDef, input string) core.Outcome {
	t.Helper()
	before := runtime.NumGoroutine()
	out := core.Execute(d, h.body, []byte(input))
	noLeak(t, before)
	return out
}

// noLeak waits (bounded, with backoff) until the goroutine count is back to before:
// goroutines that already returned may take a moment to be accounted for.
func noLeak(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	wait := time.Millisecond
	for {
		n := runtime.NumGoroutine()
		if n <= before {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			buf = buf[:runtime.Stack(buf, true)]
			t.Fatalf("%d goroutines leaked:\n%s", n-before, buf)
		}
		time.Sleep(wait)
		wait = min(wait*2, 100*time.Millisecond)
	}
}

func wantOutput(t *testing.T, out core.Outcome, want string) {
	t.Helper()
	if out.Kind != core.Returned {
		t.Fatalf("outcome %s (failure %+v, stop %v), want output %s", out.Kind, out.Failure, out.Stop, want)
	}
	if string(out.Output) != want {
		t.Fatalf("output %s, want %s", out.Output, want)
	}
}

func wantStop(t *testing.T, out core.Outcome, reason core.StopReason) {
	t.Helper()
	if out.Kind != core.Stopped || out.Stop.Reason != reason {
		t.Fatalf("outcome %s (failure %+v, stop %v), want stop %s", out.Kind, out.Failure, out.Stop, reason)
	}
}

func wantCommands(t *testing.T, d *scripted.Driver, want ...string) {
	t.Helper()
	if got := d.Kinds(); !slices.Equal(got, want) {
		t.Fatalf("commands:\n  got  %s\n  want %s", strings.Join(got, " | "), strings.Join(want, " | "))
	}
}

// constant returns a Run closure returning v.
func constant[T any](v T) func(RunContext) (T, error) {
	return func(RunContext) (T, error) { return v, nil }
}
