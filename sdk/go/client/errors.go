// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"fmt"
	"strconv"

	cairn "github.com/cairnflowdev/cairnflow/sdk/go"
)

// InvocationFailed reports an invocation that ended failed with a terminal error: Code and
// Message are the handler's terminal error.
type InvocationFailed struct {
	InvocationID string
	Code         int
	Message      string
}

func (e *InvocationFailed) Error() string {
	return fmt.Sprintf("invocation %s failed with code %d: %s", e.InvocationID, e.Code, e.Message)
}

// As makes a failed invocation a terminal error, as in the other SDKs: errors.As finds a
// *cairn.TerminalError with the handler's code and message. Returned from a handler or a
// Run closure, it fails that step for good too (the callee's outcome is decided; a retry
// cannot change it).
func (e *InvocationFailed) As(target any) bool {
	t, ok := target.(**cairn.TerminalError)
	if !ok {
		return false
	}
	terminal, _ := cairn.AsTerminalError(cairn.TerminalErrorf(e.Code, "%s", e.Message))
	*t = terminal
	return true
}

// InvocationPaused reports an invocation that is paused (out of attempts, waiting for a
// human). Resume it with POST /invocations/{id}/resume.
type InvocationPaused struct {
	InvocationID string
	Code         int
	Message      string
}

func (e *InvocationPaused) Error() string {
	return fmt.Sprintf("invocation %s is paused with code %d: %s", e.InvocationID, e.Code, e.Message)
}

// AlreadyInvoked reports a 409 already_invoked: the workflow key already has a run, and
// InvocationID is that run. Wait for it with Attach on the workflow.
type AlreadyInvoked struct {
	InvocationID string
}

func (e *AlreadyInvoked) Error() string {
	return "workflow already has a run: " + e.InvocationID
}

// AttachTimeout reports that the Timeout of a call or an attach ran out before the
// invocation stopped for good. It is still running: attach again by id.
type AttachTimeout struct {
	InvocationID string
}

func (e *AttachTimeout) Error() string {
	return "invocation " + e.InvocationID + " did not finish before the timeout"
}

// HTTPError is an HTTP error outside the invoke contract (400, 403, 404, a 503 past the
// retry cap, ...).
type HTTPError struct {
	Status int
	// Code is the body's machine-readable code (docs/api/openapi.json ErrorCode), "" if none.
	Code string
	// Message is the body's error (or message) sentence, "" if the body has none.
	Message string
	// Body is the raw response body.
	Body []byte
	// InvocationID is set when the invocation was already created and the error happened
	// while waiting for it (attach): do not call again, attach by id to keep waiting.
	InvocationID string
}

func (e *HTTPError) Error() string {
	if e.Message == "" {
		return "HTTP " + strconv.Itoa(e.Status)
	}
	return "HTTP " + strconv.Itoa(e.Status) + ": " + e.Message
}

// StreamError is a stream error that cannot be continued past: 410 stream_expired, 404,
// 403, or a 503/429 past the retry cap.
type StreamError struct {
	Status  int
	Code    string
	Message string
}

func (e *StreamError) Error() string {
	if e.Message == "" {
		return "stream: HTTP " + strconv.Itoa(e.Status)
	}
	return "stream: HTTP " + strconv.Itoa(e.Status) + ": " + e.Message
}

func httpError(res *response) *HTTPError {
	obj := res.object()
	msg, ok := stringField(obj, "error")
	if !ok {
		msg, _ = stringField(obj, "message")
	}
	code, _ := stringField(obj, "code")
	return &HTTPError{Status: res.status, Code: code, Message: msg, Body: res.body}
}

func streamError(res *response) *StreamError {
	obj := res.object()
	msg, _ := stringField(obj, "error")
	code, _ := stringField(obj, "code")
	return &StreamError{Status: res.status, Code: code, Message: msg}
}
