// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"strconv"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
)

// Get reads the state key: (value, true, nil) when it exists, (zero, false, nil) when it
// does not. With an eager state snapshot it answers in place; without one it asks the
// runtime and waits. The error is a *TerminalError when the runtime refused the read, or
// a retryable error when the stored value does not decode into T.
func Get[T any](ctx ObjectSharedContext, key string) (T, bool, error) {
	b := ctx.base()
	key = wellFormed(key)
	var zero T
	d := b.inv.Begin()
	t, err := d.GetState(key)
	if err = b.inv.Check(err); err != nil {
		return zero, false, fromDriver(err)
	}
	what := "state " + strconv.Quote(key)
	found := false
	f := newFuture(b, t, func(r core.Result) (T, error) {
		switch r.Kind {
		case core.VoidResult:
			return zero, nil
		case core.ValueResult:
			found = true
			return decode[T](r.Value, what)
		case core.FailureResult:
			return zero, failureResult(r)
		default:
			return zero, invalidResult(r, "GetState")
		}
	})
	v, err := f.Result()
	return v, found, err
}

// Set writes the state key. It does not wait (the command has no completion). The error
// is an encoding failure, or a *TerminalError when the value exceeds the frame limit.
func Set[T any](ctx ObjectContext, key string, value T) error {
	b := ctx.base()
	raw, err := encode(value)
	if err != nil {
		return err
	}
	d := b.inv.Begin()
	return fromDriver(b.inv.Check(d.SetState(wellFormed(key), raw)))
}

// Clear deletes the state key.
func Clear(ctx ObjectContext, key string) error {
	b := ctx.base()
	d := b.inv.Begin()
	return fromDriver(b.inv.Check(d.ClearState(wellFormed(key))))
}

// ClearAll deletes all of the object's state.
func ClearAll(ctx ObjectContext) error {
	b := ctx.base()
	d := b.inv.Begin()
	return fromDriver(b.inv.Check(d.ClearAllState()))
}

// StateKeys returns the object's state keys.
func StateKeys(ctx ObjectSharedContext) ([]string, error) {
	b := ctx.base()
	d := b.inv.Begin()
	t, err := d.StateKeys()
	if err = b.inv.Check(err); err != nil {
		return nil, fromDriver(err)
	}
	return newFuture(b, t, func(r core.Result) ([]string, error) {
		switch r.Kind {
		case core.StateKeysResult:
			return r.Keys, nil
		case core.FailureResult:
			return nil, failureResult(r)
		default:
			return nil, invalidResult(r, "GetStateKeys")
		}
	}).Result()
}
