// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

// The services under test: together they use the whole Context surface. Side effects
// are counted per object key or per tag, so tests running at the same time (and the two
// transports) never see each other's counts.

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cairn "github.com/cairnflowdev/cairnflow/sdk/go"
)

// counters counts side effects by name.
var counters sync.Map // string -> *atomic.Int64

func counter(name string) *atomic.Int64 {
	c, _ := counters.LoadOrStore(name, new(atomic.Int64))
	return c.(*atomic.Int64)
}

// mailboxes carry what a handler hands to the test (an awakeable id, a note).
var mailboxes sync.Map // string -> chan string

func mailbox(tag string) chan string {
	c, _ := mailboxes.LoadOrStore(tag, make(chan string, 16))
	return c.(chan string)
}

// flags are switches the test flips for the outside world (stock restocked).
var flags sync.Map // string -> bool

// ---------------------------------------------------------------- GoOrders (object)

type order struct {
	Amount int `json:"amount"`
}

type receipt struct {
	Receipt string `json:"receipt"`
	Total   int    `json:"total"`
	Key     string `json:"key"`
}

// submit: state, a durable sleep, a side effect exactly once.
func submit(ctx cairn.ObjectContext, o order) (receipt, error) {
	total, _, err := cairn.Get[int](ctx, "total")
	if err != nil {
		return receipt{}, err
	}
	if err := cairn.Sleep(ctx, 50*time.Millisecond); err != nil {
		return receipt{}, err
	}
	key := ctx.Key()
	r, err := cairn.Run(ctx, "charge", func(cairn.RunContext) (string, error) {
		counter("charge/" + key).Add(1)
		return fmt.Sprintf("receipt-%d", o.Amount), nil
	})
	if err != nil {
		return receipt{}, err
	}
	total += o.Amount
	if err := cairn.Set(ctx, "total", total); err != nil {
		return receipt{}, err
	}
	return receipt{Receipt: r, Total: total, Key: key}, nil
}

type peekResult struct {
	Total int      `json:"total"`
	Keys  []string `json:"keys"`
}

func peek(ctx cairn.ObjectSharedContext) (peekResult, error) {
	total, _, err := cairn.Get[int](ctx, "total")
	if err != nil {
		return peekResult{}, err
	}
	keys, err := cairn.StateKeys(ctx)
	return peekResult{Total: total, Keys: keys}, err
}

type keysResult struct {
	Keys  []string `json:"keys"`
	After []string `json:"after"`
}

// keysAcrossSleep: eager state keys, a new key, then a suspension; the turn after the
// sleep replays the journaled keys without a journal mismatch.
func keysAcrossSleep(ctx cairn.ObjectContext) (keysResult, error) {
	keys, err := cairn.StateKeys(ctx)
	if err != nil {
		return keysResult{}, err
	}
	if err := cairn.Set(ctx, "b", 2); err != nil {
		return keysResult{}, err
	}
	if err := cairn.Sleep(ctx, 10*time.Millisecond); err != nil {
		return keysResult{}, err
	}
	after, err := cairn.StateKeys(ctx)
	return keysResult{Keys: keys, After: after}, err
}

type clearResult struct {
	AfterClear    []string `json:"afterClear"`
	AfterClearAll []string `json:"afterClearAll"`
	Gone          bool     `json:"gone"`
}

func clearing(ctx cairn.ObjectContext) (clearResult, error) {
	var r clearResult
	if err := cairn.Set(ctx, "x", 1); err != nil {
		return r, err
	}
	if err := cairn.Set(ctx, "y", 2); err != nil {
		return r, err
	}
	if err := cairn.Clear(ctx, "x"); err != nil {
		return r, err
	}
	var err error
	if r.AfterClear, err = cairn.StateKeys(ctx); err != nil {
		return r, err
	}
	if err := cairn.ClearAll(ctx); err != nil {
		return r, err
	}
	if r.AfterClearAll, err = cairn.StateKeys(ctx); err != nil {
		return r, err
	}
	_, found, err := cairn.Get[int](ctx, "y")
	r.Gone = !found
	return r, err
}

type note struct {
	Tag  string `json:"tag"`
	Text string `json:"text"`
}

// checkout calls a service and waits for it, then sends it a one-way note.
func checkout(ctx cairn.ObjectContext, o order) (string, error) {
	paid, err := cairn.Service[string](ctx, "GoPayments", "pay").Request(o.Amount)
	if err != nil {
		return "", err
	}
	if err := cairn.Service[cairn.Void](ctx, "GoPayments", "note").Send(note{Tag: ctx.Key(), Text: paid}); err != nil {
		return "", err
	}
	return paid, nil
}

type randomResult struct {
	Same bool   `json:"same"`
	UUID string `json:"uuid"`
}

// sameRandom draws from the deterministic RNG before a suspension: the replay after it
// draws the same.
func sameRandom(ctx cairn.ObjectContext) (randomResult, error) {
	u := cairn.UUID(ctx)
	if err := cairn.Set(ctx, "u", u); err != nil {
		return randomResult{}, err
	}
	if err := cairn.Sleep(ctx, 10*time.Millisecond); err != nil {
		return randomResult{}, err
	}
	stored, _, err := cairn.Get[string](ctx, "u")
	return randomResult{Same: stored == u, UUID: u}, err
}

var orders = cairn.NewObject("GoOrders",
	cairn.Exclusive("submit", submit),
	cairn.Shared("peek", cairn.NoInput(peek)),
	cairn.Exclusive("keysAcrossSleep", cairn.NoInput(keysAcrossSleep)),
	cairn.Exclusive("clearing", cairn.NoInput(clearing)),
	cairn.Exclusive("checkout", checkout),
	cairn.Exclusive("sameRandom", cairn.NoInput(sameRandom)),
)

// ---------------------------------------------------------------- GoPayments (service)

func pay(ctx cairn.Context, amount int) (string, error) {
	if amount > 1000 {
		return "", cairn.TerminalErrorf(402, "over quota")
	}
	return cairn.Run(ctx, "pay", func(cairn.RunContext) (string, error) {
		return fmt.Sprintf("paid-%d", amount), nil
	})
}

func takeNote(ctx cairn.Context, n note) (cairn.Void, error) {
	return cairn.Run(ctx, "note", func(cairn.RunContext) (cairn.Void, error) {
		mailbox("note/" + n.Tag) <- n.Text
		return cairn.Void{}, nil
	})
}

// remindLater sends a note with a delay.
func remindLater(ctx cairn.Context, tag string) (cairn.Void, error) {
	err := cairn.Service[cairn.Void](ctx, "GoPayments", "note").Send(note{Tag: tag, Text: "later"}, cairn.WithDelay(300*time.Millisecond))
	return cairn.Void{}, err
}

// fanout runs n side effects at once and waits for all of them.
func fanout(ctx cairn.Context, n int) ([]int, error) {
	futs := make([]*cairn.Future[int], n)
	all := make([]cairn.Awaitable, n)
	for i := range n {
		futs[i] = cairn.RunAsync(ctx, fmt.Sprintf("step-%d", i), func(cairn.RunContext) (int, error) { return i * i, nil })
		all[i] = futs[i]
	}
	if err := cairn.All(ctx, all...); err != nil {
		return nil, err
	}
	out := make([]int, n)
	for i, f := range futs {
		v, err := f.Result()
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// awaitHuman hands an awakeable id to the outside world and waits for it.
func awaitHuman(ctx cairn.Context, tag string) (json.RawMessage, error) {
	awk := cairn.Awakeable[json.RawMessage](ctx)
	id := awk.ID()
	if _, err := cairn.Run(ctx, "publish", func(cairn.RunContext) (bool, error) {
		mailbox("awakeable/" + tag) <- id
		return true, nil
	}); err != nil {
		return nil, err
	}
	return awk.Result()
}

// raceSignal waits for the signal "go" or a timer, whichever the journal holds first.
func raceSignal(ctx cairn.Context, ms int) (string, error) {
	sig := cairn.Signal[string](ctx, "go")
	timer := cairn.After(ctx, time.Duration(ms)*time.Millisecond)
	first, err := cairn.WaitFirst(ctx, sig, timer)
	if err != nil {
		return "", err
	}
	if first == cairn.Awaitable(timer) {
		return "timeout", nil
	}
	return sig.Result()
}

type signalRequest struct {
	Target string `json:"target"`
	Value  string `json:"value"`
}

func signalOther(ctx cairn.Context, r signalRequest) (cairn.Void, error) {
	return cairn.Void{}, cairn.SendSignal(ctx, r.Target, "go", r.Value)
}

// gate waits for the signal "release".
func gate(ctx cairn.Context) (string, error) {
	v, err := cairn.Signal[string](ctx, "release").Result()
	return "released-" + v, err
}

func attachTo(ctx cairn.Context, id string) (string, error) {
	return cairn.Attach[string](ctx, id).Result()
}

type peekedOutput struct {
	Done  bool   `json:"done"`
	Value string `json:"value"`
}

func peekOutput(ctx cairn.Context, id string) (peekedOutput, error) {
	v, done, err := cairn.GetOutput[string](ctx, id)
	return peekedOutput{Done: done, Value: v}, err
}

// chatter writes a user stream, the durable log and a span with a Run inside.
func chatter(ctx cairn.Context, tag string) (string, error) {
	s, err := ctx.Stream("tokens")
	if err != nil {
		return "", err
	}
	for _, tok := range []string{"a", "b", "c"} {
		s.Append(tok)
	}
	s.Close()
	ctx.Log().Info("handler log", "tag", tag)
	return cairn.WithSpan(ctx, "work", func(sp *cairn.Span) (string, error) {
		sp.Set("tokens", 3)
		return cairn.Run(ctx, "inside", func(rc cairn.RunContext) (string, error) {
			rc.Log().Info("closure log", "tag", tag)
			return "streamed", nil
		})
	}, "tag", tag)
}

type artifactResult struct {
	SHA256 string `json:"sha256"`
	Size   int    `json:"size"`
	Body   string `json:"body"`
}

// artifact keeps bytes out of the journal and reads them back after a suspension.
func artifact(ctx cairn.Context, text string) (artifactResult, error) {
	ref, err := cairn.Run(ctx, "make", func(rc cairn.RunContext) (cairn.ArtifactRef, error) {
		return rc.Artifacts().Put([]byte(text), "text/plain")
	})
	if err != nil {
		return artifactResult{}, err
	}
	if err := cairn.Sleep(ctx, 10*time.Millisecond); err != nil {
		return artifactResult{}, err
	}
	body, err := cairn.Run(ctx, "read", func(rc cairn.RunContext) (string, error) {
		b, err := rc.Artifacts().Get(ref.SHA256)
		return string(b), err
	})
	return artifactResult{SHA256: ref.SHA256, Size: ref.Size, Body: body}, err
}

type shipped struct {
	Shipped int `json:"shipped"`
}

// shipStock pauses, waiting for a human, until the stock is restocked: the PauseError
// comes from inside the Run, so nothing is journaled and a resume reads the stock again.
func shipStock(ctx cairn.Context, tag string) (shipped, error) {
	stock, err := cairn.Run(ctx, "read stock", func(cairn.RunContext) (int, error) {
		counter("stock/" + tag).Add(1)
		if v, _ := flags.Load("restocked/" + tag); v != true {
			return 0, cairn.PauseErrorf("out of stock: restock needed")
		}
		return 10, nil
	})
	return shipped{Shipped: stock}, err
}

// waitCancel sleeps for an hour; a cooperative cancel ends the sleep with an error, the
// handler cleans up and fails.
func waitCancel(ctx cairn.Context, tag string) (string, error) {
	err := cairn.Sleep(ctx, time.Hour)
	if err == nil {
		return "slept", nil
	}
	if _, cerr := cairn.Run(ctx, "cleanup", func(cairn.RunContext) (bool, error) {
		counter("cleanup/" + tag).Add(1)
		return true, nil
	}); cerr != nil {
		return "", cerr
	}
	return "", err
}

var payments = cairn.NewService("GoPayments",
	cairn.Handler("pay", pay),
	cairn.Handler("note", takeNote),
	cairn.Handler("remindLater", remindLater),
	cairn.Handler("fanout", fanout),
	cairn.Handler("awaitHuman", awaitHuman),
	cairn.Handler("raceSignal", raceSignal),
	cairn.Handler("signalOther", signalOther),
	cairn.Handler("gate", cairn.NoInput(gate)),
	cairn.Handler("attachTo", attachTo),
	cairn.Handler("peekOutput", peekOutput),
	cairn.Handler("chatter", chatter),
	cairn.Handler("artifact", artifact),
	cairn.Handler("shipStock", shipStock),
	cairn.Handler("waitCancel", waitCancel),
)

// ---------------------------------------------------------------- GoLoops (continue-as-new)

type loopState struct {
	I int `json:"i"`
	N int `json:"n"`
}

type loopResult struct {
	Done int `json:"done"`
}

// loop does five steps per invocation, then continues as a new one.
func loop(ctx cairn.Context, s loopState) (loopResult, error) {
	i := s.I
	for k := 0; k < 5 && i < s.N; k++ {
		next, err := cairn.Run(ctx, fmt.Sprintf("step %d", i), func(cairn.RunContext) (int, error) { return i + 1, nil })
		if err != nil {
			return loopResult{}, err
		}
		i = next
	}
	if i < s.N {
		return loopResult{}, cairn.ContinueAsNew(ctx, loopState{I: i, N: s.N})
	}
	return loopResult{Done: i}, nil
}

// loopCaller waits for the whole chain through a call.
func loopCaller(ctx cairn.Context, n int) (loopResult, error) {
	return cairn.Service[loopResult](ctx, "GoLoops", "loop").Request(loopState{N: n})
}

var loops = cairn.NewService("GoLoops",
	cairn.Handler("loop", loop),
	cairn.Handler("viaCall", loopCaller),
)

// ---------------------------------------------------------------- GoSignup (workflow)

type signupResult struct {
	User  string `json:"user"`
	Email string `json:"email"`
}

func signupRun(ctx cairn.WorkflowContext, user string) (signupResult, error) {
	if err := cairn.Set(ctx, "stage", "waiting"); err != nil {
		return signupResult{}, err
	}
	email, err := cairn.Promise[string](ctx, "email").Get()
	if err != nil {
		return signupResult{}, err
	}
	if err := cairn.Set(ctx, "stage", "done"); err != nil {
		return signupResult{}, err
	}
	return signupResult{User: user, Email: strings.ToLower(email)}, nil
}

func signupConfirm(ctx cairn.WorkflowSharedContext, email string) (cairn.Void, error) {
	return cairn.Void{}, cairn.Promise[string](ctx, "email").Resolve(email)
}

func signupStage(ctx cairn.WorkflowSharedContext) (string, error) {
	s, _, err := cairn.Get[string](ctx, "stage")
	return s, err
}

var signup = cairn.NewWorkflow("GoSignup",
	cairn.Main("run", signupRun),
	cairn.Shared("confirm", signupConfirm),
	cairn.Shared("stage", cairn.NoInput(signupStage)),
)

// allServices is what the e2e endpoint serves.
var allServices = []cairn.ServeOption{orders, payments, loops, signup}
