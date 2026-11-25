// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"iter"
	"math"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
)

// Awaitable is any future: what [WaitFirst], [Wait], [WaitIter] and [All] take. Every
// *Future[T] is one.
type Awaitable interface {
	pending() *pending
}

// pending is the untyped part of a future: where its result comes from.
type pending struct {
	b *ctxBase
	// leaf is the wait target; zero when the result is known in place (known) or the
	// future failed before its command was emitted (err).
	leaf  core.Leaf
	known core.Result
	err   error
	// resolve decodes the result (once) and reports its error.
	resolve func() error
}

func (p *pending) ready(d core.Driver) bool {
	return p.leaf.IsZero() || d.Delivered(p.leaf)
}

// order is the journal position of the result: the earliest wins a race. A future with
// no leaf comes last, as in the TypeScript SDK (its position is +Inf there).
func (p *pending) order(d core.Driver) int {
	if p.leaf.IsZero() {
		return math.MaxInt
	}
	return d.Order(p.leaf)
}

// result is the raw result of a ready future.
func (p *pending) result(d core.Driver) (core.Result, error) {
	if p.err != nil {
		return core.Result{}, p.err
	}
	if p.leaf.IsZero() {
		return p.known, nil
	}
	return d.Result(p.leaf)
}

// Future is a durable value to come: the result of a call, a timer, a [RunAsync], a
// signal, a promise. The command is journaled when the future is created; waiting only
// happens in [Future.Result] (or in [WaitFirst], [Wait], [All], [Race]) — which is what
// lets several futures run at once.
type Future[T any] struct {
	p      pending
	decode func(core.Result) (T, error)
	done   bool
	value  T
	fail   error
}

func (f *Future[T]) pending() *pending { return &f.p }

// Result waits for the future and returns its value. It can be called again (it returns
// the same thing). Handler goroutine only.
func (f *Future[T]) Result() (T, error) {
	d := f.p.b.inv.Begin()
	if !f.p.ready(d) {
		if err := f.p.b.inv.Check(d.Await(core.Wait{Leaves: []core.Leaf{f.p.leaf}, Mode: core.WaitFirst})); err != nil {
			var zero T
			return zero, fromDriver(err)
		}
	}
	f.take(d)
	return f.value, f.fail
}

// Done reports whether the result is available without waiting.
func (f *Future[T]) Done() bool {
	d := f.p.b.inv.Begin()
	return f.p.ready(d)
}

// take decodes the result of a ready future (once).
func (f *Future[T]) take(d core.Driver) {
	if f.done {
		return
	}
	f.done = true
	r, err := f.p.result(d)
	if err != nil {
		f.fail = fromDriver(err)
		return
	}
	f.value, f.fail = f.decode(r)
}

func newFuture[T any](b *ctxBase, t core.Ticket, decode func(core.Result) (T, error)) *Future[T] {
	f := &Future[T]{p: pending{b: b, leaf: t.Leaf, known: t.Known}, decode: decode}
	f.p.resolve = func() error {
		f.take(b.inv.Driver())
		return f.fail
	}
	return f
}

// failedFuture is a future whose command could not be emitted: ready, with err.
func failedFuture[T any](b *ctxBase, err error) *Future[T] {
	f := &Future[T]{p: pending{b: b, err: err}, decode: func(core.Result) (T, error) { var zero T; return zero, err }}
	f.p.resolve = func() error {
		f.take(b.inv.Driver())
		return f.fail
	}
	return f
}

// ── combinators ──────────────────────────────────────────────────────────

// gather checks the futures belong to ctx's invocation and returns their leaves.
func gather(b *ctxBase, futs []Awaitable, what string) ([]*pending, []core.Leaf) {
	if len(futs) == 0 {
		panic("cairn: " + what + " needs at least one future")
	}
	ps := make([]*pending, len(futs))
	var leaves []core.Leaf
	for i, f := range futs {
		p := f.pending()
		if p.b.inv != b.inv {
			panic("cairn: " + what + " got a future of another invocation")
		}
		ps[i] = p
		if !p.leaf.IsZero() {
			leaves = append(leaves, p.leaf)
		}
	}
	return ps, leaves
}

// first returns the index of the ready future earliest in journal order, waiting as
// needed; -1 with the error of a cancellation point.
func first(b *ctxBase, ps []*pending, leaves []core.Leaf) (int, error) {
	d := b.inv.Begin()
	for {
		best := -1
		for i, p := range ps {
			if p.ready(d) && (best < 0 || p.order(d) < ps[best].order(d)) {
				best = i
			}
		}
		if best >= 0 {
			return best, nil
		}
		if err := b.inv.Check(d.Await(core.Wait{Leaves: leaves, Mode: core.WaitFirst})); err != nil {
			return -1, fromDriver(err)
		}
	}
}

// WaitFirst waits until one of the futures has its result and returns it: when several
// are ready, the one whose result stands earliest in the journal wins, never the one
// that arrived first in time — so a replay picks the same winner. Read the winner's
// value with its Result method. The error is the cancellation point's (409), never a
// future's own error.
func WaitFirst(ctx Context, futs ...Awaitable) (Awaitable, error) {
	b := ctx.base()
	ps, leaves := gather(b, futs, "WaitFirst")
	i, err := first(b, ps, leaves)
	if err != nil {
		return nil, err
	}
	return futs[i], nil
}

// Race waits for the first of futures of one type (as [WaitFirst]) and returns its result.
func Race[T any](ctx Context, futs ...*Future[T]) (T, error) {
	as := make([]Awaitable, len(futs))
	for i, f := range futs {
		as[i] = f
	}
	w, err := WaitFirst(ctx, as...)
	if err != nil {
		var zero T
		return zero, err
	}
	return w.(*Future[T]).Result()
}

// Wait yields the futures one by one as their results arrive, in journal order:
//
//	for fut, err := range cairn.Wait(ctx, a, b, c) {
//		if err != nil {
//			return err // cancelled
//		}
//		switch fut {
//		case a: …
//		}
//	}
//
// A non-nil error ends the sequence.
func Wait(ctx Context, futs ...Awaitable) iter.Seq2[Awaitable, error] {
	return func(yield func(Awaitable, error) bool) {
		it := WaitIter(ctx, futs...)
		for it.Next() {
			if !yield(it.Value(), nil) {
				return
			}
		}
		if err := it.Err(); err != nil {
			yield(nil, err)
		}
	}
}

// WaitIterator walks futures in journal order; see [WaitIter].
type WaitIterator struct {
	b      *ctxBase
	ps     []*pending
	futs   []Awaitable
	leaves []core.Leaf
	cur    Awaitable
	err    error
}

// WaitIter is [Wait] as an iterator object:
//
//	it := cairn.WaitIter(ctx, a, b)
//	for it.Next() {
//		fut := it.Value()
//	}
//	if err := it.Err(); err != nil { … }
func WaitIter(ctx Context, futs ...Awaitable) *WaitIterator {
	b := ctx.base()
	ps, leaves := gather(b, futs, "WaitIter")
	return &WaitIterator{b: b, ps: ps, futs: append([]Awaitable(nil), futs...), leaves: leaves}
}

// Next waits for the next future; false when every future was returned or on error.
func (it *WaitIterator) Next() bool {
	it.cur = nil
	if it.err != nil || len(it.ps) == 0 {
		return false
	}
	i, err := first(it.b, it.ps, it.leaves)
	if err != nil {
		it.err = err
		return false
	}
	it.cur = it.futs[i]
	done := it.ps[i]
	it.ps = append(it.ps[:i], it.ps[i+1:]...)
	it.futs = append(it.futs[:i], it.futs[i+1:]...)
	if !done.leaf.IsZero() {
		for j, l := range it.leaves {
			if l == done.leaf {
				it.leaves = append(it.leaves[:j], it.leaves[j+1:]...)
				break
			}
		}
	}
	return true
}

// Value is the future Next just returned.
func (it *WaitIterator) Value() Awaitable { return it.cur }

// Err is the error that stopped the iteration (a cancellation point), if any.
func (it *WaitIterator) Err() error { return it.err }

// All waits for every future. If one fails, the error returns at once; "first" is in
// journal order, so a replay after another future also failed returns the same error.
// Read each value with its Result method afterwards. When the turn has to wait, it
// suspends with an ALL_COMPLETED tree: the runtime wakes it once every result is there.
func All(ctx Context, futs ...Awaitable) error {
	b := ctx.base()
	ps, leaves := gather(b, futs, "All")
	d := b.inv.Begin()
	for {
		failed := -1
		var failure error
		complete := true
		for i, p := range ps {
			if !p.ready(d) {
				complete = false
				continue
			}
			if err := p.resolve(); err != nil && (failed < 0 || p.order(d) < ps[failed].order(d)) {
				failed, failure = i, err
			}
		}
		if failed >= 0 {
			return failure
		}
		if complete {
			return nil
		}
		if err := b.inv.Check(d.Await(core.Wait{Leaves: leaves, Mode: core.WaitAll})); err != nil {
			return fromDriver(err)
		}
	}
}
