// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"fmt"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
)

// CallClient calls one handler of another service, object or workflow; O is the type of
// its result. Build one with [Service], [Object] or [Workflow].
type CallClient[O any] struct {
	c       callerScope
	service string
	key     string
	handler string
	opts    []CallOption
}

// Service calls a handler of a plain service.
//
//	r, err := cairn.Service[Quote](ctx, "Pricing", "quote").Request(req)
func Service[O any](c Caller, service, handler string, opts ...CallOption) CallClient[O] {
	return CallClient[O]{c: c.caller(), service: service, handler: handler, opts: opts}
}

// Object calls a handler of the virtual object with the given key.
func Object[O any](c Caller, service, key, handler string, opts ...CallOption) CallClient[O] {
	return CallClient[O]{c: c.caller(), service: service, key: key, handler: handler, opts: opts}
}

// Workflow calls a handler of the workflow with the given id (its key). Calling the main
// handler again for the same id is refused (409); the run happens once per id.
func Workflow[O any](c Caller, service, workflowID, handler string, opts ...CallOption) CallClient[O] {
	return CallClient[O]{c: c.caller(), service: service, key: workflowID, handler: handler, opts: opts}
}

// spec builds the call from the client, the view's scope and the options; the options of
// the request beat the client's, which beat the view's.
func (c CallClient[O]) spec(input any, opts []CallOption) (core.CallSpec, time.Duration, error) {
	o := callOptions{scope: c.c.scope, limitKey: c.c.limitKey}
	for _, opt := range c.opts {
		opt.applyCall(&o)
	}
	for _, opt := range opts {
		opt.applyCall(&o)
	}
	b := c.c.b
	if o.scope != "" || o.limitKey != "" {
		if err := b.needScope(); err != nil {
			return core.CallSpec{}, 0, err
		}
		if o.scope == "" {
			return core.CallSpec{}, 0, fmt.Errorf("cairn: limit key %q needs a scope (cairn.WithScope or ctx.Scoped)", o.limitKey)
		}
		if err := checkComponent("scope", o.scope); err != nil {
			return core.CallSpec{}, 0, err
		}
		if o.limitKey != "" {
			if err := checkLimitKey(o.limitKey); err != nil {
				return core.CallSpec{}, 0, err
			}
		}
	}
	raw, err := encode(input)
	if err != nil {
		return core.CallSpec{}, 0, err
	}
	name := o.name
	if name == "" {
		name = c.service + "/" + c.handler
	}
	return core.CallSpec{
		Service:           c.service,
		Handler:           c.handler,
		Key:               c.key,
		Input:             raw,
		IdempotencyKey:    o.idempotencyKey,
		HasIdempotencyKey: o.hasIdempotencyKey,
		Name:              name,
		Scope:             o.scope,
		LimitKey:          o.limitKey,
		Spread:            o.spread,
		Tags:              sortedTags(o.tags),
	}, o.delay, nil
}

// RequestFuture calls the handler and returns the future of its result. A failure of
// the callee is a *TerminalError (its outcome is already decided; retrying here cannot
// change it).
func (c CallClient[O]) RequestFuture(input any, opts ...CallOption) *Future[O] {
	b := c.c.b
	spec, _, err := c.spec(input, opts)
	if err != nil {
		return failedFuture[O](b, err)
	}
	d := b.inv.Begin()
	t, err := d.Call(spec)
	if err = b.inv.Check(err); err != nil {
		return failedFuture[O](b, fromDriver(err))
	}
	what := "result of " + c.service + "/" + c.handler
	return newFuture(b, t, func(r core.Result) (O, error) {
		var zero O
		switch r.Kind {
		case core.ValueResult:
			return decode[O](r.Value, what)
		case core.FailureResult:
			return zero, failureResult(r)
		case core.VoidResult:
			return zero, nil
		default:
			return zero, invalidResult(r, "Call")
		}
	})
}

// Request calls the handler and waits for its result.
func (c CallClient[O]) Request(input any, opts ...CallOption) (O, error) {
	return c.RequestFuture(input, opts...).Result()
}

// Send calls the handler without waiting for its result; [WithDelay] schedules it later.
func (c CallClient[O]) Send(input any, opts ...CallOption) error {
	b := c.c.b
	spec, delay, err := c.spec(input, opts)
	if err != nil {
		return err
	}
	d := b.inv.Begin()
	return fromDriver(b.inv.Check(d.Send(spec, delay)))
}
