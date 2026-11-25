// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// Body is a handler as the core runs it: the input bytes in, the output bytes or a
// classified failure out. The public package builds it from the user's typed function.
type Body func(inv *Invocation, input []byte) (output []byte, failure *Failure)

// OutcomeKind says how the handler goroutine ended.
type OutcomeKind uint8

const (
	// Returned: the handler returned a value (Outcome.Output).
	Returned OutcomeKind = iota + 1
	// Failed: the handler returned an error (Outcome.Failure, any kind).
	Failed
	// Panicked: the handler panicked, or left its goroutine with runtime.Goexit on its
	// own; Outcome.Failure is a RetryFailure carrying the panic and its stack.
	Panicked
	// Stopped: the driver ended the turn (Outcome.Stop).
	Stopped
	// ContinuedAsNew: the handler called continue-as-new (Outcome.ContinueInput).
	ContinuedAsNew
)

func (k OutcomeKind) String() string {
	switch k {
	case Returned:
		return "returned"
	case Failed:
		return "failed"
	case Panicked:
		return "panicked"
	case Stopped:
		return "stopped"
	case ContinuedAsNew:
		return "continued as new"
	default:
		return fmt.Sprintf("outcome(%d)", uint8(k))
	}
}

// Outcome is how one turn of user code ended. By the time [Execute] returns it, the
// handler goroutine and every `ctx.run` closure goroutine it started have ended.
type Outcome struct {
	Kind          OutcomeKind
	Output        []byte
	Failure       *Failure
	Stop          *Stop
	ContinueInput []byte
}

// Invocation is the core state of one turn of user code: the driver, the handler
// goroutine's identity, the deterministic RNG and the running closures.
type Invocation struct {
	driver Driver
	info   Info
	// gid is the handler goroutine's id, set before user code runs.
	gid uint64
	// exiting is set before the handler goroutine is unwound; any later ctx call on the
	// handler goroutine Goexits again at once.
	exiting atomic.Bool
	// stop / continueInput record why (handler goroutine only).
	stop          *Stop
	continueInput []byte
	continued     bool

	src  *Xorshift
	rand *rand.Rand

	// misuse is set when a goroutine other than the handler's (and not a closure) called
	// a ctx method: that goroutine was stopped and the turn fails, whatever else happened.
	misuse atomic.Pointer[string]

	closures sync.WaitGroup
	// scopes maps the goroutine id of a running closure to its scope leaf.
	scopes sync.Map
}

// Execute runs body on a new goroutine (the handler goroutine) and blocks until it has
// ended and every closure it started has returned. It never lets a panic of user code
// escape.
func Execute(d Driver, body Body, input []byte) Outcome {
	inv := &Invocation{driver: d, info: d.Info()}
	inv.src = NewXorshift(inv.info.RandomSeed)
	inv.rand = rand.New(inv.src)

	var out Outcome
	done := make(chan struct{})
	go func() {
		defer close(done)
		inv.gid = goid()
		returned := false
		defer func() {
			r := recover()
			switch {
			case inv.stop != nil:
				// The stop wins over everything that happened while unwinding (a user defer
				// that panicked, or recovered nothing).
				out = Outcome{Kind: Stopped, Stop: inv.stop}
			case inv.continued:
				out = Outcome{Kind: ContinuedAsNew, ContinueInput: inv.continueInput}
			case r != nil:
				out = Outcome{Kind: Panicked, Failure: panicFailure(r, debug.Stack())}
			case !returned:
				out = Outcome{Kind: Panicked, Failure: &Failure{
					Kind:    RetryFailure,
					Code:    500,
					Message: "the handler goroutine exited without returning (runtime.Goexit called by user code)",
				}}
			}
		}()
		output, failure := body(inv, input)
		returned = true
		if failure != nil {
			out = Outcome{Kind: Failed, Failure: failure}
		} else {
			out = Outcome{Kind: Returned, Output: output}
		}
	}()
	<-done
	inv.closures.Wait()
	if m := inv.misuse.Load(); m != nil {
		// A ctx call was lost on a foreign goroutine: nothing this turn produced can be
		// trusted to replay the same way.
		out = Outcome{Kind: Panicked, Failure: &Failure{Kind: RetryFailure, Code: 500, Message: *m}}
	}
	return out
}

func panicFailure(r any, stack []byte) *Failure {
	msg := fmt.Sprint(r)
	var err error
	if e, ok := r.(error); ok {
		err = e
	}
	return &Failure{Kind: RetryFailure, Code: 500, Message: "panic: " + msg, Stack: string(stack), Err: err}
}

// Info returns the invocation's description. Safe from any goroutine.
func (inv *Invocation) Info() Info { return inv.info }

// Driver returns the driver without any check: for the methods documented as safe from
// any goroutine (Info, Now, Append, PutArtifact, FetchArtifact).
func (inv *Invocation) Driver() Driver { return inv.driver }

// Begin is called at the start of every ctx operation that touches the journal. Called
// from a goroutine other than the handler goroutine it does not return (see guard); it
// Goexits at once when the handler goroutine is already unwinding (a ctx call from a
// user defer after the turn stopped), and fails the turn once another goroutine has
// misused the context. It returns the driver.
func (inv *Invocation) Begin() Driver {
	inv.guard()
	if inv.exiting.Load() {
		runtime.Goexit()
	}
	if m := inv.misuse.Load(); m != nil {
		panic(*m)
	}
	return inv.driver
}

func (inv *Invocation) guard() {
	g := goid()
	if g == inv.gid {
		return
	}
	if _, ok := inv.scopes.Load(g); ok {
		panic("cairn: a ctx method was called inside a Run closure; a closure may only use its RunContext " +
			"(the result of the closure is how it talks to the journal)")
	}
	// A goroutine the user started. A panic here would have no recover above it and
	// would take the whole process down with every other invocation, and a recover in
	// user code could swallow it and go on. So: mark the turn as failed and stop only
	// this goroutine (its defers run; nothing reaches the driver).
	msg := "cairn: a ctx method was called from a goroutine other than the handler's; that goroutine was " +
		"stopped and the turn fails. Handler code must stay on one goroutine (use futures and " +
		"cairn.Wait/WaitFirst/All for parallel work, and cairn.Run for I/O)"
	inv.misuse.CompareAndSwap(nil, &msg)
	runtime.Goexit()
}

// Check passes err through, except a [*Stop], which ends the turn: the handler goroutine
// is unwound with runtime.Goexit and Check does not return.
func (inv *Invocation) Check(err error) error {
	var stop *Stop
	if errors.As(err, &stop) {
		inv.stop = stop
		inv.exiting.Store(true)
		runtime.Goexit()
	}
	return err
}

// ContinueAsNew ends the turn with continue-as-new: the handler goroutine is unwound
// with runtime.Goexit. Handler goroutine only (after Begin).
func (inv *Invocation) ContinueAsNew(input []byte) {
	inv.continued = true
	inv.continueInput = input
	inv.exiting.Store(true)
	runtime.Goexit()
}

// Exiting reports whether the handler goroutine is being unwound (handler goroutine
// only; a user defer can use it to tell a stop from a panic).
func (inv *Invocation) Exiting() bool { return inv.exiting.Load() }

// Scope tells who is calling: the handler goroutine (zero leaf, true), a running
// closure (its scope, true), or another goroutine (false).
func (inv *Invocation) Scope() (Leaf, bool) {
	g := goid()
	if g == inv.gid {
		return Leaf{}, true
	}
	if v, ok := inv.scopes.Load(g); ok {
		return v.(Leaf), true
	}
	return Leaf{}, false
}

// Source is the invocation's xorshift64* source (handler goroutine only).
func (inv *Invocation) Source() *Xorshift { return inv.src }

// Rand is the invocation's deterministic *rand.Rand over [Invocation.Source].
func (inv *Invocation) Rand() *rand.Rand { return inv.rand }

// RunClosure is the closure of a `ctx.run` as the core runs it: the context, the
// closure's scope; returns its outcome. The public package builds it from the user's
// typed function and maps errors; the core adds panic safety and cancellation.
type RunClosure func(ctx context.Context, scope Leaf) RunOutcome

// Run emits `Run` through the driver with an Exec that runs fn on its own goroutine.
// Handler goroutine only (after Begin); a *Stop from the driver ends the turn.
func (inv *Invocation) Run(name string, cancellable bool, fn RunClosure) (Ticket, error) {
	spec := RunSpec{
		Name:        name,
		Cancellable: cancellable,
		Exec: func(parent context.Context, scope Leaf, done func(RunOutcome)) context.CancelFunc {
			return inv.exec(parent, scope, cancellable, fn, done)
		},
	}
	t, err := inv.driver.Run(spec)
	return t, inv.Check(err)
}

func (inv *Invocation) exec(parent context.Context, scope Leaf, cancellable bool, fn RunClosure, done func(RunOutcome)) context.CancelFunc {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	var aborted atomic.Bool
	abort := func() {
		aborted.Store(true)
		cancel()
	}
	inv.closures.Add(1)
	go func() {
		defer inv.closures.Done()
		g := goid()
		inv.scopes.Store(g, scope)
		var outcome RunOutcome
		finished := false
		defer func() {
			r := recover()
			inv.scopes.Delete(g)
			cancel()
			switch {
			case r != nil:
				outcome = RunOutcome{Failure: panicFailure(r, debug.Stack())}
			case !finished:
				outcome = RunOutcome{Failure: &Failure{
					Kind:    RetryFailure,
					Code:    500,
					Message: "the Run closure exited without returning (runtime.Goexit called by user code)",
				}}
			}
			if cancellable && aborted.Load() && (outcome.Failure != nil || outcome.Cancelled) {
				// Aborted by CANCEL: whatever the closure did after that is a cancel.
				outcome = RunOutcome{Cancelled: true}
			}
			done(outcome)
		}()
		outcome = fn(ctx, scope)
		finished = true
	}()
	return abort
}

// goid returns the current goroutine's id, read from the first line of its stack
// ("goroutine 123 [running]:"). It costs about a microsecond; it is only called on ctx
// operations, which talk to the journal anyway.
func goid() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	b := buf[:n]
	const prefix = "goroutine "
	if len(b) < len(prefix) {
		panic("cairn: unexpected goroutine stack header")
	}
	b = b[len(prefix):]
	var id uint64
	for _, c := range b {
		if c < '0' || c > '9' {
			break
		}
		id = id*10 + uint64(c-'0')
	}
	return id
}
