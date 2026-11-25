// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"maps"
	"slices"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
)

// CallOption adjusts a call or a send.
type CallOption interface{ applyCall(*callOptions) }

// SleepOption adjusts a timer ([Sleep], [After]).
type SleepOption interface{ applySleep(*sleepOptions) }

// RunOption adjusts a [Run] or [RunAsync].
type RunOption interface{ applyRun(*runOptions) }

// AttachOption adjusts [Attach] and [GetOutput].
type AttachOption interface{ applyAttach(*attachOptions) }

// StreamOption adjusts [Context.Stream].
type StreamOption interface{ applyStream(*streamOptions) }

type callOptions struct {
	idempotencyKey    string
	hasIdempotencyKey bool
	name              string
	scope             string
	limitKey          string
	spread            bool
	tags              map[string]string
	delay             time.Duration
}

type sleepOptions struct{ name string }

type runOptions struct{ cancellable bool }

type attachOptions struct{ name string }

type streamOptions struct{ durable bool }

type nameOption string

// WithName sets the command's display name in the journal: of a call or send (default
// "<service>/<handler>"), a timer (default "sleep"), an attach (default "attach") or a
// GetOutput (default "getOutput").
func WithName(name string) interface {
	CallOption
	SleepOption
	AttachOption
} {
	return nameOption(name)
}

func (n nameOption) applyCall(o *callOptions)     { o.name = string(n) }
func (n nameOption) applySleep(o *sleepOptions)   { o.name = string(n) }
func (n nameOption) applyAttach(o *attachOptions) { o.name = string(n) }

type callOptionFunc func(*callOptions)

func (f callOptionFunc) applyCall(o *callOptions) { f(o) }

// WithIdempotencyKey guarantees at most one invocation for the same target and key
// (docs/protocol-v1.md §8.6): a duplicate call gets the same result. With no scope and
// no object key, it decides the callee's range.
func WithIdempotencyKey(key string) CallOption {
	return callOptionFunc(func(o *callOptions) {
		o.idempotencyKey = key
		o.hasIdempotencyKey = true
	})
}

// WithScope sets the call's flow-control scope (see [Context.Scoped]); it beats the
// scope of a [ScopedContext].
func WithScope(scope string) CallOption {
	return callOptionFunc(func(o *callOptions) { o.scope = scope })
}

// WithLimitKey sets the call's limit key ("l1" or "l1/l2"); it needs a scope.
func WithLimitKey(limitKey string) CallOption {
	return callOptionFunc(func(o *callOptions) { o.limitKey = limitKey })
}

// WithSpread places an unkeyed callee deliberately away from the caller's range
// (`placement: spread`) instead of colocating the call tree; for CPU-heavy fan-out. It
// is ignored for keyed calls (scope, object key, idempotency key). See
// docs/key-design.md §8.
func WithSpread() CallOption {
	return callOptionFunc(func(o *callOptions) { o.spread = true })
}

// WithTags sets the callee's user tags (filterable in `POST /query`). Keys
// `[a-z0-9_.-]{1,32}`, values up to 64 bytes, at most 8 tags; the runtime drops tags
// breaking the rules.
func WithTags(tags map[string]string) CallOption {
	return callOptionFunc(func(o *callOptions) { o.tags = maps.Clone(tags) })
}

// WithDelay delays a send by d (the absolute time is computed once and journaled). It is
// ignored by Request and RequestFuture, which wait for the result.
func WithDelay(d time.Duration) CallOption {
	return callOptionFunc(func(o *callOptions) { o.delay = d })
}

type runOptionFunc func(*runOptions)

func (f runOptionFunc) applyRun(o *runOptions) { f(o) }

// Cancellable makes a Run a cancellation point. By default a Run is not one: a saga's
// compensation steps are Runs and must still run after a CANCEL. A cancellable Run with
// no result yet, on a cancelled invocation, returns the cancellation error (409) without
// running; a CANCEL arriving while it runs cancels its [RunContext], and an error the
// closure returns after that is the cancellation, not a result.
func Cancellable() RunOption {
	return runOptionFunc(func(o *runOptions) { o.cancellable = true })
}

type streamOptionFunc func(*streamOptions)

func (f streamOptionFunc) applyStream(o *streamOptions) { f(o) }

// Durable makes a stream durable: its records are kept with the invocation and survive
// a node dying, instead of living only in the leader's memory.
func Durable() StreamOption {
	return streamOptionFunc(func(o *streamOptions) { o.durable = true })
}

// sortedTags orders tags by key, so the command is the same on every replay.
func sortedTags(tags map[string]string) []core.Tag {
	if len(tags) == 0 {
		return nil
	}
	keys := slices.Sorted(maps.Keys(tags))
	out := make([]core.Tag, len(keys))
	for i, k := range keys {
		out[i] = core.Tag{Key: k, Value: tags[k]}
	}
	return out
}
