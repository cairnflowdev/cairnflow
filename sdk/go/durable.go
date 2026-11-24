// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"fmt"
	"strconv"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
)

// ── signals ──────────────────────────────────────────────────────────────

// Signal is the future of the named signal sent to this invocation (by [SendSignal] or
// `POST /invocations/{id}/signals/{name}`). It carries the signal's value (zero when it
// carries none); a signal carrying an error (a rejection) is a *TerminalError.
//
// A signal that has arrived stays satisfied (docs/protocol-v1.md §8.7): a later Signal
// with the same name resolves at once with the old value. To wait for a new one, use a
// new name.
func Signal[T any](ctx Context, name string) *Future[T] {
	return signalFuture[T](ctx.base(), core.SignalID{Name: wellFormed(name)})
}

// SignalIndex is the future of a numbered signal (the numbered range is the system's:
// built-in signals and awakeables). Most code uses [Signal] or [Awakeable].
func SignalIndex[T any](ctx Context, index uint32) *Future[T] {
	return signalFuture[T](ctx.base(), core.SignalID{Index: index, ByIndex: true})
}

func signalFuture[T any](b *ctxBase, sig core.SignalID) *Future[T] {
	b.inv.Begin()
	what := "signal " + sig.String()
	return newFuture(b, core.Ticket{Leaf: sig.Leaf()}, func(r core.Result) (T, error) {
		var zero T
		switch r.Kind {
		case core.ValueResult:
			return decode[T](r.Value, what)
		case core.FailureResult:
			return zero, failureResult(r)
		default:
			return zero, nil
		}
	})
}

// SendSignal sends a named signal to another invocation. One-way: it does not wait, and
// is a no-op if the target has ended. A nil value sends a signal without a value.
func SendSignal(ctx Context, targetInvocationID, name string, value any) error {
	return sendSignal(ctx.base(), targetInvocationID, core.SignalID{Name: wellFormed(name)}, value)
}

// SendSignalIndex sends a numbered signal to another invocation; see [SignalIndex].
func SendSignalIndex(ctx Context, targetInvocationID string, index uint32, value any) error {
	return sendSignal(ctx.base(), targetInvocationID, core.SignalID{Index: index, ByIndex: true}, value)
}

func sendSignal(b *ctxBase, target string, sig core.SignalID, value any) error {
	var raw []byte
	if value != nil {
		var err error
		if raw, err = encode(value); err != nil {
			return err
		}
	}
	d := b.inv.Begin()
	return fromDriver(b.inv.Check(d.SendSignal(target, sig, raw)))
}

// ── awakeables ───────────────────────────────────────────────────────────

// AwakeableFuture is the future of an awakeable, with the id an external system uses to
// complete it.
type AwakeableFuture[T any] struct {
	*Future[T]
	id string
}

// ID is what the external system calls `POST /awakeables/{id}/resolve` (or `/reject`)
// with, using an invoke key of the same tenant.
func (a *AwakeableFuture[T]) ID() string { return a.id }

// Awakeable creates a wait point an external system completes by id, without knowing
// the invocation: send the id out from a [Run] closure, then wait on the future.
func Awakeable[T any](ctx Context) *AwakeableFuture[T] {
	b := ctx.base()
	d := b.inv.Begin()
	idx := d.AllocAwakeable()
	return &AwakeableFuture[T]{
		Future: signalFuture[T](b, core.SignalID{Index: idx, ByIndex: true}),
		id:     fmt.Sprintf("awk_%s%08x", b.InvocationID(), idx),
	}
}

// ── durable promises ─────────────────────────────────────────────────────

// DurablePromise is a durable promise by name belonging to this object or workflow: one
// handler waits on it, another (of the same key) completes it. See [Promise].
type DurablePromise[T any] struct {
	b    *ctxBase
	name string
}

// Promise is the durable promise name of this object or workflow.
func Promise[T any](ctx ObjectSharedContext, name string) DurablePromise[T] {
	return DurablePromise[T]{b: ctx.base(), name: wellFormed(name)}
}

func (p DurablePromise[T]) what() string { return "promise " + strconv.Quote(p.name) }

// GetFuture is the future of the promise's value (waits until it is completed).
func (p DurablePromise[T]) GetFuture() *Future[T] {
	d := p.b.inv.Begin()
	t, err := d.GetPromise(p.name)
	if err = p.b.inv.Check(err); err != nil {
		return failedFuture[T](p.b, fromDriver(err))
	}
	return newFuture(p.b, t, func(r core.Result) (T, error) {
		var zero T
		switch r.Kind {
		case core.ValueResult:
			return decode[T](r.Value, p.what())
		case core.FailureResult:
			return zero, failureResult(r)
		default:
			return zero, invalidResult(r, "GetPromise")
		}
	})
}

// Get waits until the promise is completed and returns its value; a rejection is a
// *TerminalError.
func (p DurablePromise[T]) Get() (T, error) { return p.GetFuture().Result() }

// Peek looks without waiting: (value, true, nil) when completed with a value, (zero,
// false, nil) when not completed yet.
func (p DurablePromise[T]) Peek() (T, bool, error) {
	var zero T
	d := p.b.inv.Begin()
	t, err := d.PeekPromise(p.name)
	if err = p.b.inv.Check(err); err != nil {
		return zero, false, fromDriver(err)
	}
	found := false
	v, err := newFuture(p.b, t, func(r core.Result) (T, error) {
		switch r.Kind {
		case core.VoidResult:
			return zero, nil
		case core.ValueResult:
			found = true
			return decode[T](r.Value, p.what())
		case core.FailureResult:
			return zero, failureResult(r)
		default:
			return zero, invalidResult(r, "PeekPromise")
		}
	}).Result()
	return v, found, err
}

// Resolve completes the promise with a value. It returns a *TerminalError if the promise
// already has a result.
func (p DurablePromise[T]) Resolve(value T) error {
	raw, err := encode(value)
	if err != nil {
		return err
	}
	return p.complete(raw, nil)
}

// Reject completes the promise with a failure (a waiter gets a *TerminalError with this
// code and message). It returns a *TerminalError if the promise already has a result.
func (p DurablePromise[T]) Reject(code int, message string) error {
	return p.complete(nil, &core.Failure{Kind: core.TerminalFailure, Code: uint32(wireCode(code)), Message: message})
}

func (p DurablePromise[T]) complete(raw []byte, failure *core.Failure) error {
	d := p.b.inv.Begin()
	t, err := d.CompletePromise(p.name, raw, failure)
	if err = p.b.inv.Check(err); err != nil {
		return fromDriver(err)
	}
	_, err = newFuture(p.b, t, func(r core.Result) (Void, error) {
		if r.Kind == core.FailureResult {
			return Void{}, failureResult(r)
		}
		return Void{}, nil
	}).Result()
	return err
}

// ── other invocations ────────────────────────────────────────────────────

// Attach waits for the result of another invocation by id; its failure is a
// *TerminalError.
func Attach[T any](ctx Context, invocationID string, opts ...AttachOption) *Future[T] {
	b := ctx.base()
	o := attachOptions{name: "attach"}
	for _, opt := range opts {
		opt.applyAttach(&o)
	}
	d := b.inv.Begin()
	t, err := d.Attach(invocationID, o.name)
	if err = b.inv.Check(err); err != nil {
		return failedFuture[T](b, fromDriver(err))
	}
	what := "result of " + invocationID
	return newFuture(b, t, func(r core.Result) (T, error) {
		var zero T
		switch r.Kind {
		case core.ValueResult:
			return decode[T](r.Value, what)
		case core.FailureResult:
			return zero, failureResult(r)
		default:
			return zero, invalidResult(r, "Attach")
		}
	})
}

// GetOutput peeks at another invocation's result without waiting for it to finish:
// (value, true, nil) when it finished with a value, (zero, false, nil) while it runs; a
// failure is a *TerminalError.
func GetOutput[T any](ctx Context, invocationID string, opts ...AttachOption) (T, bool, error) {
	b := ctx.base()
	o := attachOptions{name: "getOutput"}
	for _, opt := range opts {
		opt.applyAttach(&o)
	}
	var zero T
	d := b.inv.Begin()
	t, err := d.GetOutput(invocationID, o.name)
	if err = b.inv.Check(err); err != nil {
		return zero, false, fromDriver(err)
	}
	found := false
	what := "result of " + invocationID
	v, err := newFuture(b, t, func(r core.Result) (T, error) {
		switch r.Kind {
		case core.VoidResult:
			return zero, nil
		case core.ValueResult:
			found = true
			return decode[T](r.Value, what)
		case core.FailureResult:
			return zero, failureResult(r)
		default:
			return zero, invalidResult(r, "GetInvocationOutput")
		}
	}).Result()
	return v, found, err
}
