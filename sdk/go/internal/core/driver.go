// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

// Package core is the execution core of the Go SDK: it runs one handler turn on its own
// goroutine, guards the handler context against use from other goroutines, runs
// `ctx.run` closures, and ends the turn with runtime.Goexit when the turn machine says
// so. Everything that touches the journal goes through the [Driver] interface, which the
// turn state machine implements (docs/protocol-v1.md §6 and §8).
//
// The core never encodes a frame and never decides what the journal means: it only
// knows commands (as typed requests), leaves (what a wait can be satisfied by) and
// results. That keeps the public API independent of the wire.
package core

import (
	"context"
	"fmt"
	"time"
)

// LeafKind tells what a [Leaf] waits for.
type LeafKind uint8

const (
	// NoLeaf is the zero Leaf: nothing to wait for (a result known in place).
	NoLeaf LeafKind = iota
	// CompletionLeaf waits for the notification of a completion id (Leaf.ID).
	CompletionLeaf
	// SignalIndexLeaf waits for a signal by index (Leaf.ID): awakeables, built-in signals.
	SignalIndexLeaf
	// SignalNameLeaf waits for a signal by name (Leaf.Name).
	SignalNameLeaf
)

// Leaf is one thing a wait can be satisfied by: a completion id or a signal identity —
// the leaves of the suspension tree (docs/protocol-v1.md §7 `Suspension`, §8.7). Leaves
// are comparable and two equal leaves are the same wait target.
type Leaf struct {
	Kind LeafKind
	// ID is the completion id (CompletionLeaf) or the signal index (SignalIndexLeaf).
	ID uint32
	// Name is the signal name (SignalNameLeaf).
	Name string
}

// IsZero reports whether the leaf is the zero Leaf (no wait).
func (l Leaf) IsZero() bool { return l.Kind == NoLeaf }

func (l Leaf) String() string {
	switch l.Kind {
	case CompletionLeaf:
		return fmt.Sprintf("c%d", l.ID)
	case SignalIndexLeaf:
		return fmt.Sprintf("i%d", l.ID)
	case SignalNameLeaf:
		return "n" + l.Name
	default:
		return "none"
	}
}

// ResultKind is the shape of a delivered result.
type ResultKind uint8

const (
	// VoidResult: the operation completed without a value (absent state key, a sleep that
	// fired, an unfinished invocation for GetOutput, a signal carrying no value).
	VoidResult ResultKind = iota
	// ValueResult: Result.Value holds the value bytes (JSON).
	ValueResult
	// FailureResult: Result.Code and Result.Message describe a terminal failure.
	FailureResult
	// StateKeysResult: Result.Keys holds the state keys.
	StateKeysResult
)

// Result is a delivered result as the public layer reads it. Segmented values (wire v6)
// are already assembled by the turn machine; a value it could not assemble is reported
// by [Driver.Result] as an error, never as a partial value.
type Result struct {
	Kind    ResultKind
	Value   []byte
	Keys    []string
	Code    uint32
	Message string
}

// Ticket is what an asynchronous command hands back: the leaf to wait on, or — when
// Leaf is zero — a result known in place (an eager state read answered from the
// snapshot, `GetEagerState`).
type Ticket struct {
	Leaf  Leaf
	Known Result
}

// WaitMode shapes the suspension tree a wait contributes if the turn ends while it is
// pending (docs/protocol-v1.md §7, §8.7).
type WaitMode uint8

const (
	// WaitFirst is satisfied by any one leaf (combinator FIRST_COMPLETED).
	WaitFirst WaitMode = iota
	// WaitAll needs every leaf (combinator ALL_COMPLETED).
	WaitAll
)

// Wait is one blocking wait of user code: every leaf of the futures involved, already
// delivered ones included, and the combinator it represents.
type Wait struct {
	Leaves []Leaf
	Mode   WaitMode
}

// CallSpec describes a `Call` or `OneWayCall` command.
type CallSpec struct {
	Service string
	Handler string
	// Key is the target object or workflow key; empty for a plain service.
	Key   string
	Input []byte
	// IdempotencyKey is sent when HasIdempotencyKey is set (an empty key is a key).
	IdempotencyKey    string
	HasIdempotencyKey bool
	// Name is the command's display name in the journal ("<service>/<handler>" by default).
	Name string
	// Scope and LimitKey are the flow-control fields (wire v2); empty means absent.
	Scope    string
	LimitKey string
	// Spread asks for `placement: spread` of an unkeyed callee.
	Spread bool
	// Tags are the callee's user tags, sorted by key so the command is deterministic.
	Tags []Tag
}

// Tag is one user tag of a call.
type Tag struct{ Key, Value string }

// SignalID identifies a signal: by index (ByIndex) or by name.
type SignalID struct {
	Index   uint32
	Name    string
	ByIndex bool
}

// Leaf returns the leaf a wait on this signal waits for.
func (s SignalID) Leaf() Leaf {
	if s.ByIndex {
		return Leaf{Kind: SignalIndexLeaf, ID: s.Index}
	}
	return Leaf{Kind: SignalNameLeaf, Name: s.Name}
}

func (s SignalID) String() string {
	if s.ByIndex {
		return fmt.Sprint(s.Index)
	}
	return s.Name
}

// RunSpec describes a `Run` command (`ctx.run`).
type RunSpec struct {
	Name        string
	Cancellable bool
	// Exec starts the closure on a new goroutine and returns at once. The driver calls it
	// at most once, only when the result is not in the journal, passing the leaf it
	// returns in the Ticket as scope (it identifies the closure's stream records and
	// artifacts). parent carries what the driver wants the closure's context to inherit
	// (a deadline, a transport's lifetime); the returned cancel aborts a cancellable
	// closure (a CANCEL signal arriving mid-turn). done is called exactly once, on the
	// closure's goroutine, and must not block.
	Exec func(parent context.Context, scope Leaf, done func(RunOutcome)) (cancel context.CancelFunc)
}

// RunOutcome is how a `ctx.run` closure ended.
type RunOutcome struct {
	// Value is the encoded result when Failure is nil and Cancelled is false.
	Value []byte
	// Failure is a durable failed result to propose when its Kind is TerminalFailure. Any
	// other kind (a transient error, a panic) is not proposed: the turn fails with it and
	// the retry reruns the closure.
	Failure *Failure
	// Cancelled: a cancellable closure aborted by its cancel function returned an error.
	// Nothing is proposed; the waiter gets the cancellation error (409).
	Cancelled bool
}

// Ident is the identity of a stream record: (step, attempt, k) of the `StreamAppend`
// side channel (docs/protocol-v1.md §9.2).
type Ident struct {
	Step    uint32
	Attempt uint32
	K       uint32
}

// Append is one stream record (`ctx.stream`, `ctx.log`, spans), or the closing of a
// stream when Build is nil.
type Append struct {
	Stream  string
	Durable bool
	// Build returns the record's bytes for its identity. It is called at most once, and
	// only when the record is really sent.
	Build func(Ident) []byte
	// Closure is the scope of the `ctx.run` closure appending (the leaf given to
	// RunSpec.Exec); zero when the handler goroutine appends.
	Closure Leaf
}

// Schedule is the server-side schedule a run belongs to (the `cairnflow-schedule`
// header of the input entry).
type Schedule struct {
	Name string
	AtMs int64
}

// Info is what the turn knows about the invocation before user code runs. It does not
// change during the turn.
type Info struct {
	// ID is `StartMessage.id`; the invocation id users see is its hex form.
	ID      []byte
	DebugID string
	// Key is the object or workflow key; empty for a plain service.
	Key string
	// Attempt is `retry_count_since_last_stored_entry`.
	Attempt uint32
	// RandomSeed is `StartMessage.random_seed`.
	RandomSeed        uint64
	IdempotencyKey    string
	HasIdempotencyKey bool
	// WireVersion is the version the runtime settled on for this request.
	WireVersion int
	// Schedule is set when a server-side schedule fired the run.
	Schedule *Schedule
	// LogLevel is the minimum level of `ctx.log` for this invocation ("debug", "info",
	// "warn", "error"), already resolved from the handler's option and the
	// `cairnflow-log-level` header. Empty means "info".
	LogLevel string
}

// Driver is the turn state machine as the execution core sees it. One Driver serves one
// turn (one run of the handler from the start).
//
// Concurrency: the core calls every method from the handler goroutine, one call at a
// time, except Info, Now, Append, PutArtifact and FetchArtifact, which a `ctx.run`
// closure may call from its own goroutine while the handler goroutine runs; those must
// be safe for concurrent use.
//
// Ending the turn: any method returning an error may return a [*Stop] instead. The core
// then unwinds the handler goroutine with runtime.Goexit (user defers run, user recover
// cannot catch it) and reports the Stop in the [Outcome]. A Stop is never shown to user
// code. Errors of type [*Failure] with Kind TerminalFailure reach user code as the
// public terminal error; any other error reaches user code as it is (retryable).
type Driver interface {
	// Info describes the invocation.
	Info() Info
	// Now is the SDK's wall clock in Unix milliseconds (fixed by the conformance runner).
	Now() int64

	// GetState emits `GetState` or `GetEagerState` (the journal decides which).
	GetState(key string) (Ticket, error)
	// SetState emits `SetState`.
	SetState(key string, value []byte) error
	// ClearState emits `ClearState`.
	ClearState(key string) error
	// ClearAllState emits `ClearAllState`.
	ClearAllState() error
	// StateKeys emits `GetStateKeys` or `GetEagerStateKeys`.
	StateKeys() (Ticket, error)

	// Sleep emits `Sleep`; the wake-up time (Now + d) is computed only when the command
	// is new, never on replay.
	Sleep(d time.Duration, name string) (Ticket, error)
	// Call emits `Call`; the ticket waits for the callee's result.
	Call(c CallSpec) (Ticket, error)
	// Send emits `OneWayCall`; delay > 0 schedules it at Now + delay (computed once).
	Send(c CallSpec, delay time.Duration) error

	// SendSignal emits `SendSignal`; value nil means the signal carries no value.
	SendSignal(target string, signal SignalID, value []byte) error
	// AllocAwakeable allocates the signal index of the next awakeable (17 and up).
	AllocAwakeable() uint32

	// GetPromise emits `GetPromise` (waits until the promise is completed).
	GetPromise(name string) (Ticket, error)
	// PeekPromise emits `PeekPromise` (void when not completed).
	PeekPromise(name string) (Ticket, error)
	// CompletePromise emits `CompletePromise` with a value, or with failure when it is
	// not nil (a rejection); the ticket fails if the promise already had a result.
	CompletePromise(name string, value []byte, failure *Failure) (Ticket, error)

	// Attach emits `AttachInvocation`.
	Attach(invocationID, name string) (Ticket, error)
	// GetOutput emits `GetInvocationOutput` (void while the target runs).
	GetOutput(invocationID, name string) (Ticket, error)

	// Run emits `Run`. If the result is in the journal the ticket waits for it in journal
	// order and the closure does not run; otherwise the driver calls spec.Exec (now or
	// later) and the ticket waits for the durable result of the proposal. When the
	// invocation was cancelled and the closure is cancellable with no result yet, Run
	// returns the cancellation failure (409) after matching the command.
	Run(spec RunSpec) (Ticket, error)

	// Await blocks until the wait makes progress. Precondition: at least one leaf of w is
	// not delivered. It delivers results in journal order, and returns nil right after
	// the first delivery of a leaf of w. It returns the cancellation failure (409) when the
	// invocation has been cancelled and w cannot be satisfied from the journal (WaitFirst:
	// no leaf present; WaitAll: some leaf absent), or a *Stop when the turn ends: nothing
	// left to deliver (suspension), a journal mismatch, a failed closure, the end of a
	// BIDI_STREAM turn.
	Await(w Wait) error
	// Delivered reports whether the leaf's result has been delivered to user code.
	Delivered(l Leaf) bool
	// Order is the journal position of the leaf's result: the earliest delivered leaf
	// wins a race. Only meaningful for delivered leaves.
	Order(l Leaf) int
	// Result returns a delivered leaf's result. An error is retryable (a segmented value
	// that could not be assembled).
	Result(l Leaf) (Result, error)

	// Append sends a stream record (or closes the stream). It never fails: below wire v4
	// or once the turn has an outcome the record is dropped. It returns the record's
	// identity and true when an identity was allocated (live or replayed), which spans
	// use for their id.
	Append(a Append) (Ident, bool)
	// PutArtifact sends `ArtifactPut` (wire v4); scope is the appending closure's leaf
	// or zero. Replay-aware: a point already in the journal does not resend.
	PutArtifact(scope Leaf, sha256 [32]byte, content []byte, contentType string) error
	// FetchArtifact downloads an artifact that is not in this process's memory.
	FetchArtifact(ctx context.Context, sha256 string) ([]byte, error)
}

// StopReason says why the turn machine ended the turn.
type StopReason uint8

const (
	// StopSuspended: nothing left to deliver; the turn ends with `Suspension`.
	StopSuspended StopReason = iota + 1
	// StopMismatch: the code diverged from the journal (570).
	StopMismatch
	// StopFailed: a `ctx.run` closure failed transiently (or another retryable failure of
	// the turn itself); the turn ends with a retryable `Error`.
	StopFailed
	// StopEndOfTurn: a BIDI_STREAM turn ends to rerun the handler from the start.
	StopEndOfTurn
	// StopReplayed: a `replay_only` turn reached the first command outside the journal.
	StopReplayed
)

func (r StopReason) String() string {
	switch r {
	case StopSuspended:
		return "suspended"
	case StopMismatch:
		return "journal mismatch"
	case StopFailed:
		return "failed"
	case StopEndOfTurn:
		return "end of turn"
	case StopReplayed:
		return "replay stopped"
	default:
		return fmt.Sprintf("stop(%d)", uint8(r))
	}
}

// Stop is the error a Driver method returns to end the turn now. Cause is optional and
// only travels back to the driver in the [Outcome].
type Stop struct {
	Reason StopReason
	Cause  error
}

func (s *Stop) Error() string {
	if s.Cause != nil {
		return "cairn: turn stopped (" + s.Reason.String() + "): " + s.Cause.Error()
	}
	return "cairn: turn stopped (" + s.Reason.String() + ")"
}

func (s *Stop) Unwrap() error { return s.Cause }

// FailureKind classifies how a handler or closure failed.
type FailureKind uint8

const (
	// RetryFailure: retried by the runtime (`ErrorBehavior.RETRY`).
	RetryFailure FailureKind = iota
	// TerminalFailure: the invocation (or the `ctx.run` step) fails for good.
	TerminalFailure
	// PauseFailure: the invocation pauses, waiting for `resume` (`ErrorBehavior.PAUSE`).
	PauseFailure
)

// Failure is a classified error: a handler's returned error or panic, a closure's
// outcome, or a terminal failure the driver hands to user code.
type Failure struct {
	Kind    FailureKind
	Code    uint32
	Message string
	// Stack is the goroutine stack of a panic, empty otherwise.
	Stack string
	// RetryAfter overrides the retry policy for the next attempt (RetryFailure only;
	// `ErrorMessage.next_retry_delay`); zero means the policy.
	RetryAfter time.Duration
	// Err is the original error, if any.
	Err error
}

func (f *Failure) Error() string { return f.Message }

func (f *Failure) Unwrap() error { return f.Err }

// CancelledCode is the code of every cancellation point (as in Restate).
const CancelledCode = 409

// Cancelled is the failure of a cancellation point.
func Cancelled() *Failure {
	return &Failure{Kind: TerminalFailure, Code: CancelledCode, Message: "invocation was cancelled (CANCEL signal)"}
}
