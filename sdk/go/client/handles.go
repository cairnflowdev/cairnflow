// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"encoding/json"
	"net/http"
)

// The views below are the server's JSON (docs/api/openapi.json), with field names as the
// server sends them.

// InvocationStatus is GET /invocations/{id} (InvocationStatus in docs/api/openapi.json).
type InvocationStatus struct {
	ID string `json:"id"`
	// Status is running, suspended, completed, failed or paused.
	Status string `json:"status"`
	// Output holds the output (a string holding JSON) when completed.
	Output *string `json:"output,omitempty"`
	// Code and Message are set when failed or paused.
	Code    int    `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
	// LastFailure is present while running, suspended or paused once an attempt has
	// failed, and only when read from the leader.
	LastFailure *LastFailure `json:"lastFailure,omitempty"`
	CreatedAt   int64        `json:"createdAt,omitempty"`
	ModifiedAt  int64        `json:"modifiedAt,omitempty"`
	EndedAt     int64        `json:"endedAt,omitempty"`
}

// LastFailure is the error of the most recent failed attempt of a still-running invocation.
type LastFailure struct {
	Code int `json:"code"`
	// Message is cut at 1 KiB.
	Message string `json:"message"`
	// Attempt is the attempt number of the failed attempt (0 = the first).
	Attempt int `json:"attempt"`
	// AtMs is the wall-clock time (epoch ms) when that attempt ended.
	AtMs int64 `json:"atMs,omitempty"`
	// NextAttemptAtMs is when the next attempt is due, only while backing off.
	NextAttemptAtMs int64 `json:"nextAttemptAtMs,omitempty"`
}

// SignalAck is the reply to a signal and to resolving or rejecting an awakeable.
type SignalAck struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// CancelAck is the reply to a cancel.
type CancelAck struct {
	ID        string `json:"id"`
	Cancelled bool   `json:"cancelled"`
	Status    string `json:"status"`
}

// PromiseAck is the reply to resolving or rejecting a promise; Completed false means it
// already had a result.
type PromiseAck struct {
	Service   string `json:"service"`
	Key       string `json:"key"`
	Name      string `json:"name"`
	Completed bool   `json:"completed"`
	// Woken is the number of waiting invocations given the result (only when completed).
	Woken int `json:"woken,omitempty"`
}

// PromiseView is the state of a promise.
type PromiseView struct {
	Service   string `json:"service"`
	Key       string `json:"key"`
	Name      string `json:"name"`
	Completed bool   `json:"completed"`
	// Value is the value (a string holding JSON) when resolved.
	Value *string `json:"value,omitempty"`
	// Failure is "<code>: <message>" when rejected.
	Failure string `json:"failure,omitempty"`
}

// ---------------------------------------------------------------------------
// Invocation
// ---------------------------------------------------------------------------

// Invocation is an invocation by id. Wait for it with Attach and Output.
type Invocation struct {
	c  *Client
	id string
}

// Invocation is the invocation with this id.
func (c *Client) Invocation(id string) *Invocation { return &Invocation{c: c, id: id} }

// ID is the invocation id.
func (i *Invocation) ID() string { return i.id }

func (i *Invocation) base() string { return "/invocations/" + enc(i.id) }

func (i *Invocation) attachTarget() (*Client, string, string) {
	return i.c, i.id, i.base() + "/attach"
}

// Status reads the invocation's state.
func (i *Invocation) Status(ctx context.Context) (*InvocationStatus, error) {
	var v InvocationStatus
	if err := i.c.doJSON(ctx, http.MethodGet, i.base(), nil, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

type signalRequest struct {
	Signal any             `json:"signal"`
	Value  json.RawMessage `json:"value,omitempty"`
}

func (i *Invocation) signal(ctx context.Context, signal any, value any) (*SignalAck, error) {
	data, err := encodeValue(value)
	if err != nil {
		return nil, err
	}
	var v SignalAck
	if err := i.c.doJSON(ctx, http.MethodPost, i.base()+"/signal", signalRequest{signal, data}, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// Signal delivers the signal named name with value (nil: Void; see Call for how values are
// encoded).
func (i *Invocation) Signal(ctx context.Context, name string, value any) (*SignalAck, error) {
	return i.signal(ctx, name, value)
}

// SignalIndex delivers a built-in signal by index, with value (nil: Void).
func (i *Invocation) SignalIndex(ctx context.Context, index uint32, value any) (*SignalAck, error) {
	return i.signal(ctx, index, value)
}

// Cancel cancels cooperatively: it delivers the cancel signal and the handler cleans up
// itself (an invoke key is enough).
func (i *Invocation) Cancel(ctx context.Context) (*CancelAck, error) {
	var v CancelAck
	if err := i.c.doJSON(ctx, http.MethodPost, i.base()+"/cancel?mode=signal", nil, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// CancelHard ends the invocation at once (failed 572). It needs an admin key.
func (i *Invocation) CancelHard(ctx context.Context) (*CancelAck, error) {
	var v CancelAck
	if err := i.c.doJSON(ctx, http.MethodPost, i.base()+"/cancel", nil, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// ---------------------------------------------------------------------------
// Awakeables and promises
// ---------------------------------------------------------------------------

type rejectRequest struct {
	Message string `json:"message"`
	Code    int    `json:"code,omitempty"`
}

// Awakeable completes an awakeable by id.
type Awakeable struct {
	c  *Client
	id string
}

// Awakeable is the awakeable with this id.
func (c *Client) Awakeable(id string) Awakeable { return Awakeable{c: c, id: id} }

// Resolve completes the awakeable with value (nil: no value; see Call for how values are
// encoded).
func (a Awakeable) Resolve(ctx context.Context, value any) (*SignalAck, error) {
	var v SignalAck
	if err := a.c.doJSON(ctx, http.MethodPost, "/awakeables/"+enc(a.id)+"/resolve", value, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// Reject completes the awakeable with a terminal error. code 0 sends no code (the server
// uses 500).
func (a Awakeable) Reject(ctx context.Context, message string, code int) (*SignalAck, error) {
	var v SignalAck
	if err := a.c.doJSON(ctx, http.MethodPost, "/awakeables/"+enc(a.id)+"/reject", rejectRequest{message, code}, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// Promise is a durable promise of a workflow key.
type Promise struct {
	c    *Client
	base string
}

// Promise is the promise name of workflow service's key.
func (c *Client) Promise(service, key, name string, opts ...TargetOption) Promise {
	return Promise{c: c, base: scopePrefix(targetConfigOf(opts).scope) + "/promises/" +
		enc(service) + "/" + enc(key) + "/" + enc(name)}
}

// Resolve completes the promise with value (nil: no value).
func (p Promise) Resolve(ctx context.Context, value any) (*PromiseAck, error) {
	var v PromiseAck
	if err := p.c.doJSON(ctx, http.MethodPost, p.base+"/resolve", value, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// Reject completes the promise with a terminal error. code 0 sends no code (the server uses
// 500).
func (p Promise) Reject(ctx context.Context, message string, code int) (*PromiseAck, error) {
	var v PromiseAck
	if err := p.c.doJSON(ctx, http.MethodPost, p.base+"/reject", rejectRequest{message, code}, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// Peek reads whether the promise has a result yet.
func (p Promise) Peek(ctx context.Context) (*PromiseView, error) {
	var v PromiseView
	if err := p.c.doJSON(ctx, http.MethodGet, p.base, nil, &v); err != nil {
		return nil, err
	}
	return &v, nil
}
