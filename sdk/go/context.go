// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"regexp"
	"strings"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
)

// Context is the context of one handler run: everything the handler does durably goes
// through it. Every handler gets one; the kind of handler decides which of the narrower
// interfaces it gets ([ObjectContext], [ObjectSharedContext], [WorkflowContext],
// [WorkflowSharedContext]); a plain service handler gets this one, with no state.
//
// Three rules for handler code (docs/protocol-v1.md §8.4) that the SDK cannot fully
// enforce:
//
//  1. Stay on the handler's goroutine. Parallel work is futures plus [WaitFirst], [Wait],
//     [WaitIter], [All] or [Race]; a ctx method called from another goroutine stops that
//     goroutine and fails the turn (retried).
//     Do not use `go`, channels, `sync` or `time.Sleep` to coordinate durable work.
//  2. No randomness or clock of your own: use [Context.Rand], [Context.Random] or [UUID],
//     and read the clock inside [Run].
//  3. I/O only inside [Run]: that is why Context does not carry a context.Context, and
//     why a Run closure does (its [RunContext]).
//
// When the turn must end (the handler suspends to wait, the journal does not match the
// code), the SDK unwinds the handler's goroutine with runtime.Goexit: deferred functions
// run, recover cannot catch it, and a ctx call from a deferred function exits at once.
// Keep deferred functions free of durable work.
type Context interface {
	// Key is the key of the virtual object or workflow; empty for a plain service.
	Key() string
	// InvocationID is the id the runtime accepts everywhere over HTTP (the hex form of
	// `StartMessage.id`). Treat it as an opaque string.
	InvocationID() string
	// DebugID is a label for people reading logs; not an id.
	DebugID() string
	// Attempt counts retries since the journal last grew (0, 1, 0, 1, … for a ten-step
	// workflow where every step fails transiently once). Use it to give up deliberately
	// inside a [Run] closure.
	Attempt() int
	// IdempotencyKey is the key the invocation was started with, if any.
	IdempotencyKey() (string, bool)
	// Schedule is the server-side schedule time this run belongs to, if a schedule
	// fired it.
	Schedule() (ScheduleMark, bool)

	// Rand is a deterministic *rand.Rand seeded from the invocation (identical on every
	// replay). Do not share it with other goroutines.
	Rand() *rand.Rand
	// Random returns a number in [0, 1) from the same source as Rand: exactly the number
	// the TypeScript SDK's ctx.random() returns for the same draw.
	Random() float64

	// Log is the invocation's durable log: the stream `$log`, tied to the step and the
	// attempt, read with `GET /invocations/{id}/logs`. Records below the minimum level
	// (default info; [WithLogLevel]; the invocation's `cairnflow-log-level` header) are
	// dropped at once. It does not wait, does not fail, and is not resent on replay.
	Log() *slog.Logger
	// Stream is a stream of this invocation (tokens, progress) that a UI reads over SSE
	// while the handler runs; ephemeral unless [Durable]. Needs wire v4 (records are
	// dropped below). The name is 1 to 128 bytes and must not start with "$".
	Stream(name string, opts ...StreamOption) (*Stream, error)
	// Artifacts keeps large bytes out of the journal (wire v4).
	Artifacts() *Artifacts

	// Scoped returns a view whose calls and sends carry a flow-control scope (and an
	// optional limit key "l1" or "l1/l2"). The scope decides the callee's range, ahead of
	// the key and the idempotency key, and the callee's whole subtree inherits it; it is
	// bounded by the capacity of one range (docs/key-design.md). Needs wire v2: an older
	// runtime would silently ignore the scope, so Scoped returns an error instead.
	Scoped(scope string, limitKey ...string) (ScopedContext, error)

	base() *ctxBase
	caller() callerScope
}

// ObjectSharedContext is the context of a SHARED handler of a virtual object: it reads
// state and uses durable promises, but cannot write state (it runs in parallel with the
// EXCLUSIVE handlers of the same key, holding no lock; a write there would be a lost
// update, and the runtime rejects it).
type ObjectSharedContext interface {
	Context
	readState()
}

// ObjectContext is the context of an EXCLUSIVE handler of a virtual object: it reads and
// writes the object's state; handlers of one key run one at a time.
type ObjectContext interface {
	ObjectSharedContext
	writeState()
}

// WorkflowSharedContext is the context of a workflow's SHARED handlers: they read state,
// signal the run and complete its durable promises.
type WorkflowSharedContext interface {
	ObjectSharedContext
	workflow()
}

// WorkflowContext is the context of a workflow's main handler (it runs exactly once per
// workflow id, the key): it reads and writes state.
type WorkflowContext interface {
	WorkflowSharedContext
	writeState()
}

// ScheduleMark is the server-side schedule time a run belongs to.
type ScheduleMark struct {
	// Name is the schedule's name.
	Name string
	// At is the nominal time of the fire (for a manual fire, the time of firing).
	At time.Time
}

// ctxBase is the state shared by every context kind of one handler run.
type ctxBase struct {
	inv  *core.Invocation
	info core.Info
	// spans is the stack of open spans (handler goroutine only).
	spans []string
	log   *slog.Logger
}

func newBase(inv *core.Invocation) *ctxBase {
	b := &ctxBase{inv: inv, info: inv.Info()}
	b.log = slog.New(&logHandler{b: b, dynamic: true})
	return b
}

func (b *ctxBase) base() *ctxBase       { return b }
func (b *ctxBase) caller() callerScope  { return callerScope{b: b} }
func (b *ctxBase) Key() string          { return b.info.Key }
func (b *ctxBase) InvocationID() string { return hex.EncodeToString(b.info.ID) }
func (b *ctxBase) DebugID() string      { return b.info.DebugID }
func (b *ctxBase) Attempt() int         { return int(b.info.Attempt) }

func (b *ctxBase) IdempotencyKey() (string, bool) {
	return b.info.IdempotencyKey, b.info.HasIdempotencyKey
}

func (b *ctxBase) Schedule() (ScheduleMark, bool) {
	s := b.info.Schedule
	if s == nil {
		return ScheduleMark{}, false
	}
	return ScheduleMark{Name: s.Name, At: time.UnixMilli(s.AtMs)}, true
}

func (b *ctxBase) Rand() *rand.Rand {
	b.inv.Begin()
	return b.inv.Rand()
}

func (b *ctxBase) Random() float64 {
	b.inv.Begin()
	return b.inv.Source().Float64()
}

func (b *ctxBase) Log() *slog.Logger { return b.log }

// The concrete context of each handler kind: the marker methods decide which interfaces
// it satisfies, so a type assertion cannot widen a narrow context.
type (
	serviceContext        struct{ *ctxBase }
	objectSharedContext   struct{ *ctxBase }
	objectContext         struct{ *ctxBase }
	workflowSharedContext struct{ *ctxBase }
	workflowContext       struct{ *ctxBase }
)

func (objectSharedContext) readState()   {}
func (objectContext) readState()         {}
func (objectContext) writeState()        {}
func (workflowSharedContext) readState() {}
func (workflowSharedContext) workflow()  {}
func (workflowContext) readState()       {}
func (workflowContext) writeState()      {}
func (workflowContext) workflow()        {}

// ctxKind is the kind of handler, read from the context type of its function.
type ctxKind uint8

const (
	serviceKind ctxKind = iota + 1
	objectKind
	objectSharedKind
	workflowKind
	workflowSharedKind
)

func (k ctxKind) String() string {
	switch k {
	case serviceKind:
		return "cairn.Context"
	case objectKind:
		return "cairn.ObjectContext"
	case objectSharedKind:
		return "cairn.ObjectSharedContext"
	case workflowKind:
		return "cairn.WorkflowContext"
	case workflowSharedKind:
		return "cairn.WorkflowSharedContext"
	default:
		return "unknown context"
	}
}

// kindOf reads the kind from a context type parameter; 0 when C is not one of the five
// context interfaces.
func kindOf[C any]() ctxKind {
	switch any((*C)(nil)).(type) {
	case *Context:
		return serviceKind
	case *ObjectContext:
		return objectKind
	case *ObjectSharedContext:
		return objectSharedKind
	case *WorkflowContext:
		return workflowKind
	case *WorkflowSharedContext:
		return workflowSharedKind
	default:
		return 0
	}
}

// newContext builds the concrete context of a kind.
func newContext(kind ctxKind, inv *core.Invocation) Context {
	b := newBase(inv)
	switch kind {
	case objectKind:
		return objectContext{b}
	case objectSharedKind:
		return objectSharedContext{b}
	case workflowKind:
		return workflowContext{b}
	case workflowSharedKind:
		return workflowSharedContext{b}
	default:
		return serviceContext{b}
	}
}

// UUID returns a version 4 UUID drawn from the invocation's deterministic RNG: the same
// on every replay of the same point.
func UUID(ctx Context) string {
	r := ctx.Rand()
	var u [16]byte
	binary.BigEndian.PutUint64(u[:8], r.Uint64())
	binary.BigEndian.PutUint64(u[8:], r.Uint64())
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	h := hex.EncodeToString(u[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// ── scope ────────────────────────────────────────────────────────────────

// flowControlVersion is the first wire version that carries `scope`/`limit_key`.
const flowControlVersion = 2

// maxComponent is the length cap of one scope or limit-key component.
const maxComponent = 36

var componentRE = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

func checkComponent(what, value string) error {
	if value == "" {
		return fmt.Errorf("cairn: %s is empty; omit it instead of passing an empty string", what)
	}
	if len(value) > maxComponent {
		return fmt.Errorf("cairn: %s is %d characters long; the limit is %d", what, len(value), maxComponent)
	}
	if !componentRE.MatchString(value) {
		return fmt.Errorf("cairn: %s %q has invalid characters; only [a-zA-Z0-9_.-] is allowed", what, value)
	}
	return nil
}

func checkLimitKey(limitKey string) error {
	parts := strings.Split(limitKey, "/")
	if len(parts) > 2 {
		return fmt.Errorf("cairn: limit key %q has %d levels; only 1 or 2 are allowed (l1 or l1/l2)", limitKey, len(parts))
	}
	for _, p := range parts {
		if err := checkComponent("limit key", p); err != nil {
			return err
		}
	}
	return nil
}

func (b *ctxBase) needScope() error {
	if v := b.info.WireVersion; v < flowControlVersion {
		return &TerminalError{code: 500, message: fmt.Sprintf(
			"the runtime speaks wire version %d; a scope needs v%d. An older runtime silently IGNORES the scope, "+
				"so the call would run with no limit at all; upgrade the runtime, or remove the scope from this deployment.",
			v, flowControlVersion)}
	}
	return nil
}

func (b *ctxBase) Scoped(scope string, limitKey ...string) (ScopedContext, error) {
	if err := b.needScope(); err != nil {
		return ScopedContext{}, err
	}
	if err := checkComponent("scope", scope); err != nil {
		return ScopedContext{}, err
	}
	if len(limitKey) > 1 {
		return ScopedContext{}, fmt.Errorf("cairn: Scoped takes at most one limit key, got %d", len(limitKey))
	}
	s := ScopedContext{s: callerScope{b: b, scope: scope}}
	if len(limitKey) == 1 {
		if err := checkLimitKey(limitKey[0]); err != nil {
			return ScopedContext{}, err
		}
		s.s.limitKey = limitKey[0]
	}
	return s, nil
}

// Caller is what can start calls and sends: a [Context], or a [ScopedContext] view of one.
type Caller interface {
	caller() callerScope
}

type callerScope struct {
	b        *ctxBase
	scope    string
	limitKey string
}

// ScopedContext is a view of a [Context] whose calls and sends carry a scope; see
// [Context.Scoped]. It only starts calls: the handler's own state stays in its own scope.
// A [WithScope] or [WithLimitKey] option on a call beats the view's.
type ScopedContext struct{ s callerScope }

func (s ScopedContext) caller() callerScope { return s.s }
