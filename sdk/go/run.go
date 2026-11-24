// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
)

// After starts a durable timer and returns its future, for use with [WaitFirst] and the
// other combinators. The wake-up time is absolute, computed once and journaled; a replay
// does not restart the timer.
func After(ctx Context, d time.Duration, opts ...SleepOption) *Future[Void] {
	b := ctx.base()
	o := sleepOptions{name: "sleep"}
	for _, opt := range opts {
		opt.applySleep(&o)
	}
	dr := b.inv.Begin()
	t, err := dr.Sleep(d, o.name)
	if err = b.inv.Check(err); err != nil {
		return failedFuture[Void](b, fromDriver(err))
	}
	return newFuture(b, t, func(r core.Result) (Void, error) {
		if r.Kind == core.FailureResult {
			return Void{}, failureResult(r)
		}
		return Void{}, nil
	})
}

// Sleep sleeps durably for d. The error is a *TerminalError (a cancellation point).
func Sleep(ctx Context, d time.Duration, opts ...SleepOption) error {
	_, err := After(ctx, d, opts...).Result()
	return err
}

// RunContext is what a [Run] closure gets: a context.Context for its I/O — cancelled
// when a [Cancellable] run is cancelled, and carrying the deadline the transport gives
// the turn — plus the parts of the handler context a closure may use. It is safe to use
// from goroutines the closure starts, until the closure returns.
type RunContext interface {
	context.Context
	// Log is the invocation's durable log, recorded at this step.
	Log() *slog.Logger
	// Stream is a stream of the invocation (see [Context.Stream]), recorded at this step.
	Stream(name string, opts ...StreamOption) (*Stream, error)
	// Artifacts keeps large bytes out of the journal: return the [ArtifactRef] from the
	// closure instead of the bytes.
	Artifacts() *Artifacts
	// Attempt is [Context.Attempt].
	Attempt() int
}

type runContext struct {
	context.Context
	b     *ctxBase
	scope core.Leaf
	log   *slog.Logger
}

func (r *runContext) Log() *slog.Logger { return r.log }
func (r *runContext) Attempt() int      { return int(r.b.info.Attempt) }

func (r *runContext) Stream(name string, opts ...StreamOption) (*Stream, error) {
	return newStream(r.b, name, opts, &r.scope)
}

func (r *runContext) Artifacts() *Artifacts { return &Artifacts{b: r.b, scope: &r.scope} }

// RunAsync runs a side effect exactly once per durable result and returns its future.
// The closure starts on its own goroutine at once (unless the result is already in the
// journal: then it does not run at all). Its result is proposed to the runtime and
// becomes the future's value once durable — in the next turn, in journal order. Dying
// while the closure runs reruns it, so keep it idempotent.
//
// The closure's result: a value is the step's result; a [TerminalError] is a durable
// failed result (returned on every replay, the closure does not run again); a
// [PauseError] pauses the invocation without recording anything, so the closure runs
// again after `resume` and sees the fixed world; any other error or a panic is
// transient: nothing is recorded, the turn fails and the retry reruns the closure. The
// closure must not use the handler's ctx (it panics); it gets a [RunContext].
func RunAsync[T any](ctx Context, name string, fn func(RunContext) (T, error), opts ...RunOption) *Future[T] {
	b := ctx.base()
	var o runOptions
	for _, opt := range opts {
		opt.applyRun(&o)
	}
	name = wellFormed(name)
	b.inv.Begin()
	closure := func(c context.Context, scope core.Leaf) core.RunOutcome {
		rc := &runContext{Context: c, b: b, scope: scope}
		rc.log = slog.New(&logHandler{b: b, scope: scope})
		v, err := fn(rc)
		if err != nil {
			if t, ok := AsTerminalError(err); ok {
				return core.RunOutcome{Failure: &core.Failure{Kind: core.TerminalFailure, Code: uint32(t.code), Message: err.Error(), Err: err}}
			}
			// A transient failure, or a pause: neither is proposed, so the step runs again
			// on the retry or after the resume; the turn ends with the pause (the
			// invocation waits for a human) or the retry.
			return core.RunOutcome{Failure: classify(err)}
		}
		raw, err := encode(v)
		if err != nil {
			return core.RunOutcome{Failure: &core.Failure{Kind: core.RetryFailure, Code: 500, Message: err.Error(), Err: err}}
		}
		return core.RunOutcome{Value: raw}
	}
	t, err := b.inv.Run(name, o.cancellable, closure)
	if err != nil {
		return failedFuture[T](b, fromDriver(err))
	}
	what := "result of Run " + strconv.Quote(name)
	return newFuture(b, t, func(r core.Result) (T, error) {
		var zero T
		switch r.Kind {
		case core.ValueResult:
			return decode[T](r.Value, what)
		case core.FailureResult:
			return zero, failureResult(r)
		case core.VoidResult:
			return zero, nil
		default:
			return zero, invalidResult(r, "Run")
		}
	})
}

// Run is [RunAsync] followed by waiting for its result.
func Run[T any](ctx Context, name string, fn func(RunContext) (T, error), opts ...RunOption) (T, error) {
	return RunAsync(ctx, name, fn, opts...).Result()
}

// ContinueAsNew ends this invocation and starts a new one with the same target and
// input, with an empty journal: for long loops, so each journal stays bounded. The new
// invocation runs on the service's current deployment. The caller and the idempotency
// key move to the new one. It does not return, except with an error when it cannot run
// (the runtime speaks wire below v3, or input does not encode).
func ContinueAsNew(ctx Context, input any) error {
	b := ctx.base()
	if v := b.info.WireVersion; v < keepaliveVersion {
		return &TerminalError{code: 500, message: "ContinueAsNew needs a runtime speaking wire v" +
			strconv.Itoa(keepaliveVersion) + "; the peer speaks v" + strconv.Itoa(v)}
	}
	raw, err := encode(input)
	if err != nil {
		return err
	}
	b.inv.Begin()
	b.inv.ContinueAsNew(raw)
	return errors.New("unreachable")
}

// keepaliveVersion is the first wire version with keepalives, the replay report and
// continue-as-new.
const keepaliveVersion = 3
