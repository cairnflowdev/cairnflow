// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
)

// ServiceType is the kind of a service in the manifest.
type ServiceType string

// The service kinds (docs/protocol-v1.md §3).
const (
	ServiceTypeService       ServiceType = "SERVICE"
	ServiceTypeVirtualObject ServiceType = "VIRTUAL_OBJECT"
	ServiceTypeWorkflow      ServiceType = "WORKFLOW"
)

// HandlerType is the kind of a handler in the manifest.
type HandlerType string

// The handler kinds (docs/protocol-v1.md §3).
const (
	HandlerTypeExclusive HandlerType = "EXCLUSIVE"
	HandlerTypeShared    HandlerType = "SHARED"
	HandlerTypeWorkflow  HandlerType = "WORKFLOW"
)

// OnMaxAttempts is what happens when a retry policy's attempts run out.
type OnMaxAttempts string

const (
	// PauseOnMaxAttempts keeps the invocation waiting for `resume`.
	PauseOnMaxAttempts OnMaxAttempts = "PAUSE"
	// KillOnMaxAttempts ends the invocation with an error.
	KillOnMaxAttempts OnMaxAttempts = "KILL"
)

// RetryPolicy is the runtime's retry policy for transient errors. A zero field is not
// declared (the runtime's default applies).
type RetryPolicy struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	// ExponentiationFactor multiplies the interval between two attempts.
	ExponentiationFactor float64
	// MaxAttempts is the total number of attempts, the first included; 0 is unlimited.
	MaxAttempts   int
	OnMaxAttempts OnMaxAttempts
}

// HandlerOptions is the contract a service or handler declares in the manifest; a
// handler's declaration overrides its service's, and when neither declares the runtime
// uses its default. The values are captured at registration: changing them means
// registering a new deployment. Durations are sent in whole milliseconds; a zero field
// is not declared.
type HandlerOptions struct {
	// InactivityTimeout caps how long the SDK may stay silent within a turn. A service
	// declares 15 minutes when it sets nothing (a REQUEST_RESPONSE turn running a long
	// Run would otherwise be cut at the node's default).
	InactivityTimeout time.Duration
	RetryPolicy       *RetryPolicy
	// JournalRetention is how long the journal of a finished invocation is kept.
	JournalRetention time.Duration
	// LogLevel is the minimum level of the durable log (SDK side only, not in the
	// manifest); the invocation's `cairnflow-log-level` header overrides it.
	LogLevel *slog.Level
	// RuntimeTrace makes the runtime write one span per turn and per Run into `$trace`.
	RuntimeTrace bool
	// LogFlush flushes durable stream records that have not ridden along with a commit at
	// this interval (BIDI_STREAM only), so logs survive a node dying in a long Run.
	LogFlush time.Duration
}

// HandlerOption sets one field of [HandlerOptions].
type HandlerOption func(*HandlerOptions)

// WithInactivityTimeout sets [HandlerOptions.InactivityTimeout].
func WithInactivityTimeout(d time.Duration) HandlerOption {
	return func(o *HandlerOptions) { o.InactivityTimeout = d }
}

// WithRetryPolicy sets [HandlerOptions.RetryPolicy].
func WithRetryPolicy(p RetryPolicy) HandlerOption {
	return func(o *HandlerOptions) { o.RetryPolicy = &p }
}

// WithJournalRetention sets [HandlerOptions.JournalRetention].
func WithJournalRetention(d time.Duration) HandlerOption {
	return func(o *HandlerOptions) { o.JournalRetention = d }
}

// WithLogLevel sets [HandlerOptions.LogLevel].
func WithLogLevel(l slog.Level) HandlerOption {
	return func(o *HandlerOptions) { o.LogLevel = &l }
}

// WithRuntimeTrace sets [HandlerOptions.RuntimeTrace].
func WithRuntimeTrace() HandlerOption {
	return func(o *HandlerOptions) { o.RuntimeTrace = true }
}

// WithLogFlush sets [HandlerOptions.LogFlush].
func WithLogFlush(d time.Duration) HandlerOption {
	return func(o *HandlerOptions) { o.LogFlush = d }
}

func buildOptions(opts []HandlerOption) *HandlerOptions {
	if len(opts) == 0 {
		return nil
	}
	o := &HandlerOptions{}
	for _, f := range opts {
		f(o)
	}
	return o
}

// HandlerDef is one handler, made by [Handler], [Exclusive], [Shared] or [Main] and
// registered with [NewService], [NewObject] or [NewWorkflow].
type HandlerDef struct {
	name string
	kind ctxKind
	// ty is the declared handler type; empty when not declared.
	ty      HandlerType
	options *HandlerOptions
	body    core.Body
}

// Name is the handler's name.
func (h *HandlerDef) Name() string { return h.name }

// handlerOf adapts a typed function to the core.
func handlerOf[C Context, I, O any](name string, kind ctxKind, ty HandlerType, fn func(C, I) (O, error), opts []HandlerOption) *HandlerDef {
	if name == "" {
		panic("cairn: a handler needs a name")
	}
	if fn == nil {
		panic(fmt.Sprintf("cairn: handler %q has a nil function", name))
	}
	body := func(inv *core.Invocation, raw []byte) ([]byte, *core.Failure) {
		ctx := newContext(kind, inv).(C)
		in, err := decodeInput[I](raw)
		if err != nil {
			return nil, classify(err)
		}
		out, err := fn(ctx, in)
		if err != nil {
			return nil, classify(err)
		}
		b, err := encode(out)
		if err != nil {
			return nil, &core.Failure{Kind: core.RetryFailure, Code: 500, Message: err.Error(), Err: err}
		}
		return b, nil
	}
	return &HandlerDef{name: name, kind: kind, ty: ty, options: buildOptions(opts), body: body}
}

// Handler declares a handler; its kind follows the context type of fn:
//
//   - [Context]: a plain service handler.
//   - [ObjectContext]: an EXCLUSIVE object handler (its type is left undeclared in the
//     manifest, which means EXCLUSIVE; [Exclusive] declares it).
//   - [ObjectSharedContext]: a SHARED object handler.
//   - [WorkflowContext]: the workflow's main handler ([Main]).
//   - [WorkflowSharedContext]: a SHARED workflow handler.
//
// For a handler without input, wrap fn with [NoInput]; for one without output, return
// [Void].
func Handler[C Context, I, O any](name string, fn func(C, I) (O, error), opts ...HandlerOption) *HandlerDef {
	kind := kindOf[C]()
	if kind == 0 {
		var c C
		panic(fmt.Sprintf("cairn: handler %q takes %T; the context must be one of cairn.Context, cairn.ObjectContext, "+
			"cairn.ObjectSharedContext, cairn.WorkflowContext, cairn.WorkflowSharedContext", name, c))
	}
	var ty HandlerType
	switch kind {
	case objectSharedKind, workflowSharedKind:
		ty = HandlerTypeShared
	case workflowKind:
		ty = HandlerTypeWorkflow
	}
	return handlerOf(name, kind, ty, fn, opts)
}

// Exclusive declares an EXCLUSIVE handler of a virtual object: it reads and writes
// state, and handlers of one key run one at a time.
func Exclusive[I, O any](name string, fn func(ObjectContext, I) (O, error), opts ...HandlerOption) *HandlerDef {
	return handlerOf(name, objectKind, HandlerTypeExclusive, fn, opts)
}

// Shared declares a SHARED handler of a virtual object (C is [ObjectSharedContext]) or a
// workflow (C is [WorkflowSharedContext]): it runs in parallel with the others and reads
// state without writing it.
func Shared[C ObjectSharedContext, I, O any](name string, fn func(C, I) (O, error), opts ...HandlerOption) *HandlerDef {
	kind := kindOf[C]()
	if kind != objectSharedKind && kind != workflowSharedKind {
		panic(fmt.Sprintf("cairn: shared handler %q takes %s; use cairn.ObjectSharedContext or cairn.WorkflowSharedContext", name, kind))
	}
	return handlerOf(name, kind, HandlerTypeShared, fn, opts)
}

// Main declares a workflow's main handler: it runs exactly once per workflow id (the
// key), with a [WorkflowContext]. The other SDKs name it "run".
func Main[I, O any](name string, fn func(WorkflowContext, I) (O, error), opts ...HandlerOption) *HandlerDef {
	return handlerOf(name, workflowKind, HandlerTypeWorkflow, fn, opts)
}

// NoInput adapts a handler that takes no input: its input is ignored.
//
//	cairn.Shared("total", cairn.NoInput(total))
func NoInput[C Context, O any](fn func(C) (O, error)) func(C, Void) (O, error) {
	return func(c C, _ Void) (O, error) { return fn(c) }
}

// ServiceDef is a declared service, virtual object or workflow.
type ServiceDef struct {
	name     string
	ty       ServiceType
	options  *HandlerOptions
	handlers []*HandlerDef
}

// Name is the service's name.
func (s *ServiceDef) Name() string { return s.name }

// Type is the service's kind.
func (s *ServiceDef) Type() ServiceType { return s.ty }

// With sets the contract shared by every handler of the service (a handler's own
// options override it).
func (s *ServiceDef) With(opts ...HandlerOption) *ServiceDef {
	if s.options == nil {
		s.options = &HandlerOptions{}
	}
	for _, f := range opts {
		f(s.options)
	}
	return s
}

func newServiceDef(name string, ty ServiceType, handlers []*HandlerDef, allowed ...ctxKind) *ServiceDef {
	if name == "" {
		panic("cairn: a service needs a name")
	}
	if len(handlers) == 0 {
		panic(fmt.Sprintf("cairn: service %q has no handlers", name))
	}
	seen := map[string]bool{}
	for _, h := range handlers {
		if h == nil {
			panic(fmt.Sprintf("cairn: service %q has a nil handler", name))
		}
		if seen[h.name] {
			panic(fmt.Sprintf("cairn: service %q has two handlers named %q", name, h.name))
		}
		seen[h.name] = true
		ok := false
		for _, k := range allowed {
			ok = ok || h.kind == k
		}
		if !ok {
			panic(fmt.Sprintf("cairn: handler %q of %s %q takes %s, which this kind of service does not run", h.name, ty, name, h.kind))
		}
	}
	return &ServiceDef{name: name, ty: ty, handlers: handlers}
}

// NewService declares a plain service: stateless handlers taking a [Context], run in
// parallel, without a key.
func NewService(name string, handlers ...*HandlerDef) *ServiceDef {
	return newServiceDef(name, ServiceTypeService, handlers, serviceKind)
}

// NewObject declares a virtual object: keyed, with durable state; handlers take an
// [ObjectContext] (EXCLUSIVE) or an [ObjectSharedContext] (SHARED).
func NewObject(name string, handlers ...*HandlerDef) *ServiceDef {
	return newServiceDef(name, ServiceTypeVirtualObject, handlers, objectKind, objectSharedKind)
}

// NewWorkflow declares a workflow: exactly one [Main] handler (it runs once per
// workflow id) and any number of SHARED handlers taking a [WorkflowSharedContext].
func NewWorkflow(name string, handlers ...*HandlerDef) *ServiceDef {
	s := newServiceDef(name, ServiceTypeWorkflow, handlers, workflowKind, workflowSharedKind)
	mains := 0
	for _, h := range handlers {
		if h.kind == workflowKind {
			mains++
		}
	}
	if mains != 1 {
		panic(fmt.Sprintf("cairn: workflow %q has %d main handlers; it needs exactly one (cairn.Main)", name, mains))
	}
	return s
}

// Handlers lists the service's handlers in declaration order.
func (s *ServiceDef) Handlers() []*HandlerDef { return append([]*HandlerDef(nil), s.handlers...) }

// Handler returns the handler with this name.
func (s *ServiceDef) Handler(name string) (*HandlerDef, bool) {
	for _, h := range s.handlers {
		if h.name == name {
			return h, true
		}
	}
	return nil, false
}

// logLevel is the handler's minimum durable log level ("" when neither the handler nor
// its service declares one); the invocation's header still overrides it.
func (s *ServiceDef) logLevel(h *HandlerDef) string {
	switch {
	case h.options != nil && h.options.LogLevel != nil:
		return levelName(*h.options.LogLevel)
	case s.options != nil && s.options.LogLevel != nil:
		return levelName(*s.options.LogLevel)
	default:
		return ""
	}
}
