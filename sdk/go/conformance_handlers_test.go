// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"encoding/json"
	"sync"
	"time"
)

// slowGate holds the closure of the slowRun handler until the runner reads the first
// Keepalive (armSlow, one gate per trace run).
var slowGate struct {
	sync.Mutex
	ch chan struct{}
}

// armSlow sets a new gate and returns the function that opens it (idempotent).
func armSlow() (release func()) {
	ch := make(chan struct{})
	slowGate.Lock()
	slowGate.ch = ch
	slowGate.Unlock()
	var once sync.Once
	return func() { once.Do(func() { close(ch) }) }
}

func slowReleased() <-chan struct{} {
	slowGate.Lock()
	defer slowGate.Unlock()
	return slowGate.ch
}

// conformance is the service of conformance/README.md, section "Handlers": the same names
// and the same behaviour as the reference SDK of the trace generator. Values are JSON;
// completion ids are assigned in call order, from 1.
func conformance() *ServiceDef {
	return NewObject("Conformance",
		Handler("counter", NoInput(func(ctx ObjectContext) (string, error) { return counter(ctx, "charge") })),
		Handler("counterRenamed", NoInput(func(ctx ObjectContext) (string, error) { return counter(ctx, "charge-v2") })),
		Handler("counterChanged", NoInput(func(ctx ObjectContext) (string, error) {
			if _, _, err := Get[int](ctx, "count"); err != nil {
				return "", err
			}
			if err := Set(ctx, "count", 9); err != nil {
				return "", err
			}
			return "other", nil
		})),
		Handler("outOfOrder", NoInput(func(ctx ObjectContext) (string, error) {
			a := After(ctx, time.Second)
			b := After(ctx, 2*time.Second)
			if _, err := a.Result(); err != nil {
				return "", err
			}
			if _, err := b.Result(); err != nil {
				return "", err
			}
			return "both", nil
		})),
		Handler("sleepRunSleep", NoInput(func(ctx ObjectContext) (string, error) {
			a := After(ctx, time.Second)
			b := After(ctx, 2*time.Second)
			if _, err := Run(ctx, "pay", constant("result")); err != nil {
				return "", err
			}
			if _, err := a.Result(); err != nil {
				return "", err
			}
			if _, err := b.Result(); err != nil {
				return "", err
			}
			return "done", nil
		})),
		Handler("eagerCount", NoInput(func(ctx ObjectContext) (string, error) {
			n, _, err := Get[int](ctx, "count")
			if err != nil {
				return "", err
			}
			if err := Set(ctx, "count", n+1); err != nil {
				return "", err
			}
			return "done", nil
		})),
		Handler("stateKeys", NoInput(func(ctx ObjectContext) ([]string, error) { return StateKeys(ctx) })),
		Handler("promise", NoInput(func(ctx ObjectContext) (string, error) {
			if err := Promise[string](ctx, "approve").Resolve("ok"); err != nil {
				return "", err
			}
			return Promise[string](ctx, "approve").Get()
		})),
		Handler("allOf", NoInput(func(ctx ObjectContext) (string, error) {
			a := After(ctx, time.Second)
			b := After(ctx, 2*time.Second)
			if err := All(ctx, a, b); err != nil {
				return "", err
			}
			return "both", nil
		})),
		Handler("raceSignal", NoInput(func(ctx ObjectContext) (string, error) {
			sleep := After(ctx, 60*time.Second)
			signal := Signal[string](ctx, "approve")
			won, err := WaitFirst(ctx, sleep, signal)
			if err != nil {
				return "", err
			}
			if won == signal {
				return signal.Result()
			}
			return "sleep", nil
		})),
		Handler("raceAwakeable", NoInput(func(ctx ObjectContext) (string, error) {
			awk := Awakeable[string](ctx)
			sleep := After(ctx, 60*time.Second)
			won, err := WaitFirst(ctx, awk.Future, sleep)
			if err != nil {
				return "", err
			}
			if won == Awaitable(awk.Future) {
				return awk.Result()
			}
			return "sleep", nil
		})),
		Handler("allSignal", NoInput(func(ctx ObjectContext) (int, error) {
			sleep := After(ctx, time.Second)
			signal := SignalIndex[int](ctx, 4)
			if err := All(ctx, sleep, signal); err != nil {
				return 0, err
			}
			return signal.Result()
		})),
		Handler("scopedCall", NoInput(func(ctx ObjectContext) (int, error) {
			scoped, err := ctx.Scoped("tenant-a", "k1")
			if err != nil {
				return 0, err
			}
			return Service[int](scoped, "Callee", "work").Request(1)
		})),
		Handler("spreadCall", NoInput(func(ctx ObjectContext) (int, error) {
			return Service[int](ctx, "Callee", "work", WithSpread()).Request(1)
		})),
		Handler("parallelBranches", NoInput(parallelBranches)),
		Handler("callRetargeted", NoInput(func(ctx ObjectContext) (string, error) {
			if _, err := Service[Void](ctx, "Callee", "b").Request(1); err != nil {
				return "", err
			}
			return "x", nil
		})),
		Handler("stateKeyChanged", NoInput(func(ctx ObjectContext) (string, error) {
			if _, _, err := Get[json.RawMessage](ctx, "total"); err != nil {
				return "", err
			}
			return "x", nil
		})),
		Handler("segmentedGet", NoInput(func(ctx ObjectContext) (string, error) {
			h, _, err := Get[string](ctx, "history")
			return h, err
		})),
		Handler("segmentedSetReuseTail", NoInput(func(ctx ObjectContext) (string, error) {
			h, _, err := Get[string](ctx, "history")
			if err != nil {
				return "", err
			}
			if err := Set(ctx, "history", h+"b"); err != nil {
				return "", err
			}
			return "ok", nil
		})),
		Handler("segmentedRunResult", NoInput(func(ctx ObjectContext) (string, error) {
			h, _, err := Get[string](ctx, "history")
			if err != nil {
				return "", err
			}
			if _, err := Run(ctx, "extend", constant(h+"c")); err != nil {
				return "", err
			}
			return "ok", nil
		})),
		Handler("slowRun", NoInput(func(ctx ObjectContext) (string, error) {
			return Run(ctx, "slow", func(rc RunContext) (string, error) {
				select {
				case <-slowReleased():
					return "done", nil
				case <-rc.Done():
					return "", rc.Err()
				}
			})
		})),
		Handler("streamRun", NoInput(func(ctx ObjectContext) (string, error) {
			ui, err := ctx.Stream("ui")
			if err != nil {
				return "", err
			}
			if _, err := Run(ctx, "llm", func(RunContext) (string, error) {
				ui.Append(map[string]string{"tok": "a"})
				ui.Append(map[string]string{"tok": "b"})
				return "ab", nil
			}); err != nil {
				return "", err
			}
			return "done", nil
		})),
	)
}

func counter(ctx ObjectContext, step string) (string, error) {
	n, _, err := Get[int](ctx, "count")
	if err != nil {
		return "", err
	}
	if err := Sleep(ctx, time.Second); err != nil {
		return "", err
	}
	if _, err := Run(ctx, step, constant("charged")); err != nil {
		return "", err
	}
	if err := Set(ctx, "count", n+1); err != nil {
		return "", err
	}
	return "done", nil
}

// parallelBranches runs two branches of two steps each, "a" and "b": each branch's second
// step starts once its first result is delivered, and results are taken in journal order
// (WaitFirst), so a branch waiting for its proposal does not hold the other back.
func parallelBranches(ctx ObjectContext) ([][]string, error) {
	type branch struct {
		name  string
		steps []string
		fut   *Future[string]
	}
	branches := []*branch{{name: "a"}, {name: "b"}}
	start := func(b *branch) {
		step := b.name + string(rune('1'+len(b.steps)))
		b.fut = RunAsync(ctx, step, constant(step))
	}
	for _, b := range branches {
		start(b)
	}
	for {
		var waiting []Awaitable
		for _, b := range branches {
			if b.fut != nil {
				waiting = append(waiting, b.fut)
			}
		}
		if len(waiting) == 0 {
			break
		}
		won, err := WaitFirst(ctx, waiting...)
		if err != nil {
			return nil, err
		}
		for _, b := range branches {
			if Awaitable(b.fut) != won {
				continue
			}
			v, err := b.fut.Result()
			if err != nil {
				return nil, err
			}
			b.steps = append(b.steps, v)
			b.fut = nil
			if len(b.steps) < 2 {
				start(b)
			}
			break
		}
	}
	return [][]string{branches[0].steps, branches[1].steps}, nil
}
