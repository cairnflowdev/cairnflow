// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/values"
)

// CancelledCode is the code of the error every cancellation point returns once the
// invocation was cancelled (a CANCEL signal), as in Restate.
const CancelledCode = core.CancelledCode

// TerminalError is a business error: returned from a handler it ends the invocation as
// failed, with no retry (retrying cannot change it). Returned from a [Run] closure it is
// the step's durable result: the closure does not run again, and [Run] returns the same
// error (same code, same message) on every replay.
//
// Operations whose only failure is a terminal one (a callee that failed, a rejected
// promise or awakeable, a cancellation point) return a *TerminalError.
type TerminalError struct {
	code    int
	message string
	cause   error
}

// Error returns the message.
func (e *TerminalError) Error() string { return e.message }

// Code is the error code (an HTTP-like status; 500 by default).
func (e *TerminalError) Code() int { return e.code }

// Unwrap returns the error it was made from, if any.
func (e *TerminalError) Unwrap() error { return e.cause }

// TerminalErrorf returns a *TerminalError with a code and a formatted message.
func TerminalErrorf(code int, format string, args ...any) error {
	return &TerminalError{code: wireCode(code), message: fmt.Sprintf(format, args...)}
}

// ErrorOption adjusts an error made by [ToTerminalError], [ToRetryableError] or
// [ToPauseError].
type ErrorOption func(*errorOptions)

type errorOptions struct {
	code       int
	hasCode    bool
	retryAfter time.Duration
}

// WithCode sets the error's code.
func WithCode(code int) ErrorOption {
	return func(o *errorOptions) {
		o.code = code
		o.hasCode = true
	}
}

// WithRetryAfter makes the runtime wait this long before the next attempt, overriding
// the retry policy for that one attempt ([RetryableError] only).
func WithRetryAfter(d time.Duration) ErrorOption {
	return func(o *errorOptions) { o.retryAfter = d }
}

func applyErrorOptions(opts []ErrorOption) errorOptions {
	var o errorOptions
	for _, f := range opts {
		f(&o)
	}
	return o
}

// ToTerminalError marks err as terminal. It returns nil for a nil err. A *TerminalError
// already in err's chain keeps its code unless WithCode is given; otherwise the code is
// 500.
func ToTerminalError(err error, opts ...ErrorOption) error {
	if err == nil {
		return nil
	}
	o := applyErrorOptions(opts)
	code := 500
	if t, ok := AsTerminalError(err); ok {
		code = t.code
	}
	if o.hasCode {
		code = o.code
	}
	return &TerminalError{code: wireCode(code), message: err.Error(), cause: err}
}

// IsTerminalError reports whether err's chain holds a *TerminalError.
func IsTerminalError(err error) bool {
	_, ok := AsTerminalError(err)
	return ok
}

// AsTerminalError returns the *TerminalError in err's chain.
func AsTerminalError(err error) (*TerminalError, bool) {
	var t *TerminalError
	if errors.As(err, &t) {
		return t, true
	}
	return nil, false
}

// RetryableError is a transient failure with a code: the runtime retries the invocation
// per its retry policy. Any error that is not a *TerminalError or a *PauseError is
// retried the same way (code 500); this type only adds a code and [WithRetryAfter].
type RetryableError struct {
	code       int
	message    string
	retryAfter time.Duration
	cause      error
}

// Error returns the message.
func (e *RetryableError) Error() string { return e.message }

// Code is the error code.
func (e *RetryableError) Code() int { return e.code }

// RetryAfter is the delay before the next attempt; zero means the retry policy.
func (e *RetryableError) RetryAfter() time.Duration { return e.retryAfter }

// Unwrap returns the error it was made from, if any.
func (e *RetryableError) Unwrap() error { return e.cause }

// RetryableErrorf returns a *RetryableError with a code and a formatted message.
func RetryableErrorf(code int, format string, args ...any) error {
	return &RetryableError{code: wireCode(code), message: fmt.Sprintf(format, args...)}
}

// ToRetryableError wraps err as a *RetryableError (code 500 unless WithCode). It returns
// nil for a nil err.
func ToRetryableError(err error, opts ...ErrorOption) error {
	if err == nil {
		return nil
	}
	o := applyErrorOptions(opts)
	code := 500
	if o.hasCode {
		code = o.code
	}
	return &RetryableError{code: wireCode(code), message: err.Error(), retryAfter: o.retryAfter, cause: err}
}

// SegmentedValueError reports a large value (wire v6: refs to segments plus a tail) that
// the SDK could not read: a segment it could not download or check, a value over a limit.
// It is not a business error: returned from a handler, the attempt fails and is retried.
// Its message never carries a segment's read URL.
type SegmentedValueError struct{ message string }

// Error returns the message.
func (e *SegmentedValueError) Error() string { return e.message }

// PauseError pauses the invocation, waiting for a human (docs/protocol-v1.md §8.5).
//
// An ordinary error is retried with backoff; a [TerminalError] fails the invocation for
// good and throws the work done so far away. A PauseError stops and keeps the journal as
// it is: the operator fixes the cause, then `POST /invocations/{id}/resume`, and the
// invocation continues from exactly where it stopped. A paused invocation of a virtual
// object keeps its place in the exclusive queue.
type PauseError struct {
	code    int
	message string
	cause   error
}

// Error returns the message.
func (e *PauseError) Error() string { return e.message }

// Code is the error code.
func (e *PauseError) Code() int { return e.code }

// Unwrap returns the error it was made from, if any.
func (e *PauseError) Unwrap() error { return e.cause }

// PauseErrorf returns a *PauseError with code 500 and a formatted message.
func PauseErrorf(format string, args ...any) error {
	return &PauseError{code: 500, message: fmt.Sprintf(format, args...)}
}

// ToPauseError wraps err as a *PauseError (code 500 unless WithCode). It returns nil
// for a nil err.
func ToPauseError(err error, opts ...ErrorOption) error {
	if err == nil {
		return nil
	}
	o := applyErrorOptions(opts)
	code := 500
	if o.hasCode {
		code = o.code
	}
	return &PauseError{code: wireCode(code), message: err.Error(), cause: err}
}

// wireCode keeps a code that can travel on the wire (uint32); anything else becomes 500.
func wireCode(code int) int {
	if code < 0 || int64(code) > math.MaxUint32 {
		return 500
	}
	return code
}

// classify turns an error returned by user code into the core's failure: a
// *TerminalError anywhere in the chain wins, then a *PauseError, then a *RetryableError;
// anything else is a retryable 500.
func classify(err error) *core.Failure {
	var (
		t *TerminalError
		p *PauseError
		r *RetryableError
	)
	switch {
	case errors.As(err, &t):
		return &core.Failure{Kind: core.TerminalFailure, Code: uint32(t.code), Message: err.Error(), Err: err}
	case errors.As(err, &p):
		return &core.Failure{Kind: core.PauseFailure, Code: uint32(p.code), Message: err.Error(), Err: err}
	case errors.As(err, &r):
		return &core.Failure{Kind: core.RetryFailure, Code: uint32(r.code), Message: err.Error(), RetryAfter: r.retryAfter, Err: err}
	default:
		return &core.Failure{Kind: core.RetryFailure, Code: 500, Message: err.Error(), Err: err}
	}
}

// fromDriver turns a driver error into what user code sees: a terminal failure becomes
// a *TerminalError; anything else passes through (retryable).
func fromDriver(err error) error {
	var ve *values.Error
	if errors.As(err, &ve) {
		return &SegmentedValueError{message: ve.Message}
	}
	var f *core.Failure
	if errors.As(err, &f) && f.Kind == core.TerminalFailure {
		return &TerminalError{code: int(f.Code), message: f.Message, cause: f.Err}
	}
	return err
}

// failureResult turns a FailureResult into a *TerminalError.
func failureResult(r core.Result) error {
	return &TerminalError{code: int(r.Code), message: r.Message}
}

// invalidResult is the error of a result whose kind the operation cannot take.
func invalidResult(r core.Result, op string) error {
	return &TerminalError{code: 500, message: fmt.Sprintf("result %s is not valid for %s", resultName(r.Kind), op)}
}

func resultName(k core.ResultKind) string {
	switch k {
	case core.VoidResult:
		return "void"
	case core.ValueResult:
		return "value"
	case core.FailureResult:
		return "failure"
	case core.StateKeysResult:
		return "stateKeys"
	default:
		return fmt.Sprintf("kind %d", k)
	}
}
