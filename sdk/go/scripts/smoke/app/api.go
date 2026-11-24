// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package main

// The public API as a user's code sees it, pinned by assignment: a changed signature
// (a parameter, a result, a type made unexported) stops this file from compiling, in the
// SDK's own `go vet ./...` and in scripts/smoke.sh, where it builds outside the repository
// against the module through a replace directive. A deliberate API change updates this
// file in the same commit.

import (
	"context"
	"encoding/json"
	"iter"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"

	cairn "github.com/cairnflowdev/cairnflow/sdk/go"
	"github.com/cairnflowdev/cairnflow/sdk/go/cairntest"
	"github.com/cairnflowdev/cairnflow/sdk/go/client"
)

type in struct{ N int }
type out struct{ M int }

var (
	// Declaring services and handlers.
	_ func(string, ...*cairn.HandlerDef) *cairn.ServiceDef = cairn.NewService
	_ func(string, ...*cairn.HandlerDef) *cairn.ServiceDef = cairn.NewObject
	_ func(string, ...*cairn.HandlerDef) *cairn.ServiceDef = cairn.NewWorkflow

	_ func(string, func(cairn.Context, in) (out, error), ...cairn.HandlerOption) *cairn.HandlerDef               = cairn.Handler[cairn.Context, in, out]
	_ func(string, func(cairn.ObjectContext, in) (out, error), ...cairn.HandlerOption) *cairn.HandlerDef         = cairn.Exclusive[in, out]
	_ func(string, func(cairn.ObjectSharedContext, in) (out, error), ...cairn.HandlerOption) *cairn.HandlerDef   = cairn.Shared[cairn.ObjectSharedContext, in, out]
	_ func(string, func(cairn.WorkflowSharedContext, in) (out, error), ...cairn.HandlerOption) *cairn.HandlerDef = cairn.Shared[cairn.WorkflowSharedContext, in, out]
	_ func(string, func(cairn.WorkflowContext, in) (out, error), ...cairn.HandlerOption) *cairn.HandlerDef       = cairn.Main[in, out]
	_ func(func(cairn.Context) (out, error)) func(cairn.Context, cairn.Void) (out, error)                        = cairn.NoInput[cairn.Context, out]
	_ func(time.Duration) cairn.HandlerOption                                                                    = cairn.WithInactivityTimeout
	_ func(cairn.RetryPolicy) cairn.HandlerOption                                                                = cairn.WithRetryPolicy
	_ func(time.Duration) cairn.HandlerOption                                                                    = cairn.WithJournalRetention
	_ func(slog.Level) cairn.HandlerOption                                                                       = cairn.WithLogLevel
	_ func() cairn.HandlerOption                                                                                 = cairn.WithRuntimeTrace
	_ func(time.Duration) cairn.HandlerOption                                                                    = cairn.WithLogFlush
	_ func(*cairn.ServiceDef, ...cairn.HandlerOption) *cairn.ServiceDef                                          = (*cairn.ServiceDef).With
	_                                                                                                            = cairn.RetryPolicy{InitialInterval: time.Second, MaxInterval: time.Minute, ExponentiationFactor: 2, MaxAttempts: 3, OnMaxAttempts: cairn.PauseOnMaxAttempts}

	// Serving.
	_ func(context.Context, string, ...cairn.ServeOption) error   = cairn.Serve
	_ func(...cairn.ServeOption) http.Handler                     = cairn.HTTPHandler
	_ func(...cairn.ServeOption) (cairn.LambdaHandlerFunc, error) = cairn.LambdaHandler
	_ func() cairn.ServeOption                                    = cairn.Bidi
	_ func(...string) cairn.ServeOption                           = cairn.IdentityKeys
	_ func(...string) cairn.ServeOption                           = cairn.IdentityHosts
	_ func(...string) cairn.ServeOption                           = cairn.SegmentOrigins
	_ func(int) cairn.ServeOption                                 = cairn.MaxProtocolVersion
	_ func(cairn.ReplayCacheOptions) cairn.ServeOption            = cairn.ReplayCache
	_ func() cairn.ServeOption                                    = cairn.WithoutReplayCache
	_ func() cairn.ServeOption                                    = cairn.WithoutCompression
	_ func(int64) cairn.ServeOption                               = cairn.ValueCacheBytes
	_ func(int64) cairn.ServeOption                               = cairn.MaxValueBytes
	_ func(int64) cairn.ServeOption                               = cairn.MaxTurnValueBytes
	_ func(http.RoundTripper) cairn.ServeOption                   = cairn.WithSegmentTransport
	_ func(string, http.Header) cairn.ServeOption                 = cairn.WithArtifactSource
	_ cairn.ServeOption                                           = (*cairn.ServiceDef)(nil)

	// State.
	_ func(cairn.ObjectSharedContext, string) (out, bool, error) = cairn.Get[out]
	_ func(cairn.ObjectContext, string, out) error               = cairn.Set[out]
	_ func(cairn.ObjectContext, string) error                    = cairn.Clear
	_ func(cairn.ObjectContext) error                            = cairn.ClearAll
	_ func(cairn.ObjectSharedContext) ([]string, error)          = cairn.StateKeys

	// Durable steps and futures.
	_ func(cairn.Context, time.Duration, ...cairn.SleepOption) error                                          = cairn.Sleep
	_ func(cairn.Context, time.Duration, ...cairn.SleepOption) *cairn.Future[cairn.Void]                      = cairn.After
	_ func(cairn.Context, string, func(cairn.RunContext) (out, error), ...cairn.RunOption) (out, error)       = cairn.Run[out]
	_ func(cairn.Context, string, func(cairn.RunContext) (out, error), ...cairn.RunOption) *cairn.Future[out] = cairn.RunAsync[out]
	_ func() cairn.RunOption                                                                                  = cairn.Cancellable
	_ func(*cairn.Future[out]) (out, error)                                                                   = (*cairn.Future[out]).Result
	_ func(*cairn.Future[out]) bool                                                                           = (*cairn.Future[out]).Done
	_ func(cairn.Context, ...cairn.Awaitable) (cairn.Awaitable, error)                                        = cairn.WaitFirst
	_ func(cairn.Context, ...cairn.Awaitable) iter.Seq2[cairn.Awaitable, error]                               = cairn.Wait
	_ func(cairn.Context, ...cairn.Awaitable) *cairn.WaitIterator                                             = cairn.WaitIter
	_ func(cairn.Context, ...cairn.Awaitable) error                                                           = cairn.All
	_ func(cairn.Context, ...*cairn.Future[out]) (out, error)                                                 = cairn.Race[out]
	_ func(cairn.Context) string                                                                              = cairn.UUID
	_ func(cairn.Context, any) error                                                                          = cairn.ContinueAsNew
	_ func(cairn.Context, string, func(*cairn.Span) (out, error), ...any) (out, error)                        = cairn.WithSpan[out]

	// Calls.
	_ func(cairn.Caller, string, string, ...cairn.CallOption) cairn.CallClient[out]         = cairn.Service[out]
	_ func(cairn.Caller, string, string, string, ...cairn.CallOption) cairn.CallClient[out] = cairn.Object[out]
	_ func(cairn.Caller, string, string, string, ...cairn.CallOption) cairn.CallClient[out] = cairn.Workflow[out]
	_ func(cairn.CallClient[out], any, ...cairn.CallOption) (out, error)                    = cairn.CallClient[out].Request
	_ func(cairn.CallClient[out], any, ...cairn.CallOption) *cairn.Future[out]              = cairn.CallClient[out].RequestFuture
	_ func(cairn.CallClient[out], any, ...cairn.CallOption) error                           = cairn.CallClient[out].Send
	_ func(time.Duration) cairn.CallOption                                                  = cairn.WithDelay
	_ func(string) cairn.CallOption                                                         = cairn.WithIdempotencyKey
	_ func(string) cairn.CallOption                                                         = cairn.WithScope
	_ func(string) cairn.CallOption                                                         = cairn.WithLimitKey
	_ func() cairn.CallOption                                                               = cairn.WithSpread
	_ func(map[string]string) cairn.CallOption                                              = cairn.WithTags
	_ cairn.CallOption                                                                      = cairn.WithName("x")
	_ cairn.SleepOption                                                                     = cairn.WithName("x")
	_ cairn.AttachOption                                                                    = cairn.WithName("x")

	// The outside world.
	_ func(cairn.Context, string) *cairn.Future[out]                        = cairn.Signal[out]
	_ func(cairn.Context, uint32) *cairn.Future[out]                        = cairn.SignalIndex[out]
	_ func(cairn.Context, string, string, any) error                        = cairn.SendSignal
	_ func(cairn.Context) *cairn.AwakeableFuture[out]                       = cairn.Awakeable[out]
	_ func(*cairn.AwakeableFuture[out]) string                              = (*cairn.AwakeableFuture[out]).ID
	_ func(cairn.ObjectSharedContext, string) cairn.DurablePromise[out]     = cairn.Promise[out]
	_ func(cairn.DurablePromise[out]) (out, error)                          = cairn.DurablePromise[out].Get
	_ func(cairn.DurablePromise[out]) (out, bool, error)                    = cairn.DurablePromise[out].Peek
	_ func(cairn.DurablePromise[out], out) error                            = cairn.DurablePromise[out].Resolve
	_ func(cairn.DurablePromise[out], int, string) error                    = cairn.DurablePromise[out].Reject
	_ func(cairn.Context, string, ...cairn.AttachOption) *cairn.Future[out] = cairn.Attach[out]
	_ func(cairn.Context, string, ...cairn.AttachOption) (out, bool, error) = cairn.GetOutput[out]

	// Errors.
	_ func(int, string, ...any) error          = cairn.TerminalErrorf
	_ func(int, string, ...any) error          = cairn.RetryableErrorf
	_ func(string, ...any) error               = cairn.PauseErrorf
	_ func(error, ...cairn.ErrorOption) error  = cairn.ToTerminalError
	_ func(error, ...cairn.ErrorOption) error  = cairn.ToRetryableError
	_ func(error, ...cairn.ErrorOption) error  = cairn.ToPauseError
	_ func(int) cairn.ErrorOption              = cairn.WithCode
	_ func(time.Duration) cairn.ErrorOption    = cairn.WithRetryAfter
	_ func(error) (*cairn.TerminalError, bool) = cairn.AsTerminalError
	_ func(error) bool                         = cairn.IsTerminalError
	_ func(*cairn.TerminalError) int           = (*cairn.TerminalError).Code
	_ int                                      = cairn.CancelledCode

	// The client.
	_ func(string, ...client.Option) *client.Client                                                                      = client.Connect
	_ func(string) client.ConnectionOption                                                                               = client.WithAPIKey
	_ func(context.Context, client.Callee, string, any, ...client.CallOption) (out, error)                               = client.Call[out]
	_ func(context.Context, client.Callee, string, any, ...client.CallOption) (*client.Invocation, error)                = client.Send
	_ func(context.Context, client.Attachable, ...client.AttachOption) (out, error)                                      = client.Attach[out]
	_ func(context.Context, client.Attachable) (out, bool, error)                                                        = client.Output[out]
	_ func(context.Context, client.Workflow, any, ...client.CallOption) (out, error)                                     = client.RunWorkflow[out]
	_ func(*client.Client, string, string, ...client.TargetOption) client.Workflow                                       = (*client.Client).Workflow
	_ func(*client.Client, string, ...client.TargetOption) client.Target                                                 = (*client.Client).Service
	_ func(*client.Client, string, string, ...client.TargetOption) client.Target                                         = (*client.Client).Object
	_ func(*client.Client, context.Context, string, string, ...client.StreamOption) iter.Seq2[client.StreamEvent, error] = (*client.Client).ReadStream

	// Testing handlers without a server.
	_ func(*cairn.ServiceDef, string, any, ...cairntest.Option) *cairntest.Invocation = cairntest.New
	_ func(string) cairntest.Option                                                   = cairntest.Key
	_ func(string, any) cairntest.Option                                              = cairntest.State
	_ func(string, string, func(json.RawMessage) (any, error)) cairntest.Option       = cairntest.Stub
	_ func(time.Time) cairntest.Option                                                = cairntest.Clock
	_ func(int) cairntest.Option                                                      = cairntest.MaxAttempts
	_ func(*cairntest.Invocation) (cairntest.Status, error)                           = (*cairntest.Invocation).Run
	_ func(*cairntest.Invocation, any) error                                          = (*cairntest.Invocation).Output
	_ func(*cairntest.Invocation) []cairntest.Pending                                 = (*cairntest.Invocation).Pending
	_ func(*cairntest.Invocation, cairntest.Pending, any) error                       = (*cairntest.Invocation).Resolve
	_ func(*cairntest.Invocation, cairntest.Pending, int, string) error               = (*cairntest.Invocation).Reject
	_ func(*cairntest.Invocation, string, any) error                                  = (*cairntest.Invocation).Signal
	_ func(*cairntest.Invocation)                                                     = (*cairntest.Invocation).Cancel
	_ func(*cairntest.Invocation, time.Duration)                                      = (*cairntest.Invocation).Advance
	_ func(*cairntest.Invocation, string, any) (bool, error)                          = (*cairntest.Invocation).Get
)

// The context interfaces nest: an exclusive object handler can do what a shared one can,
// and every handler context is a Context.
var (
	_ cairn.ObjectSharedContext   = cairn.ObjectContext(nil)
	_ cairn.ObjectSharedContext   = cairn.WorkflowSharedContext(nil)
	_ cairn.WorkflowSharedContext = cairn.WorkflowContext(nil)
	_ cairn.Caller                = cairn.Context(nil)
	_ context.Context             = cairn.RunContext(nil)
)

// Context methods a handler calls.
func contextSurface(ctx cairn.Context) {
	var (
		_ string                     = ctx.Key()
		_ string                     = ctx.InvocationID()
		_ int                        = ctx.Attempt()
		_ *rand.Rand                 = ctx.Rand()
		_ float64                    = ctx.Random()
		_ *slog.Logger               = ctx.Log()
		_ *cairn.Artifacts           = ctx.Artifacts()
		_ func(string) (bool, error) = nil
	)
	_, _ = ctx.IdempotencyKey()
	_, _ = ctx.Schedule()
	s, _ := ctx.Stream("tokens", cairn.Durable())
	s.Append("x")
	s.Close()
	_, _ = ctx.Scoped("scope", "l1")
	ref, _ := ctx.Artifacts().Put([]byte("x"), "text/plain")
	_, _ = ctx.Artifacts().Get(ref.SHA256)
}

var _ = contextSurface
