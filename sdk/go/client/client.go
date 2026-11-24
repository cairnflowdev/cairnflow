// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"
	"time"
)

// Client calls a cluster through any node's ingress port. It is safe for concurrent use.
type Client struct {
	tr         *transport
	attachWait time.Duration
}

// Connect makes a client for the ingress port at baseURL (http://host:port, https://...,
// or host:port). It opens no connection: errors surface on the first call.
func Connect(baseURL string, opts ...Option) *Client {
	c := &config{
		header:       http.Header{},
		hc:           http.DefaultClient,
		followLeader: true,
		maxAttempts:  8,
		maxElapsed:   30 * time.Second,
		attachWait:   25 * time.Second,
	}
	for _, o := range opts {
		if o != nil {
			o.applyConnect(c)
		}
	}
	if c.hc == nil {
		c.hc = http.DefaultClient
	}
	return &Client{
		tr: &transport{
			base:         normalizeURL(baseURL),
			header:       c.header,
			followLeader: c.followLeader,
			maxAttempts:  max(1, c.maxAttempts),
			maxElapsed:   c.maxElapsed,
			retryNetwork: true,
			hc:           c.hc,
			timeout:      c.timeout,
		},
		attachWait: max(0, c.attachWait),
	}
}

// URL is the normalized base URL of the client.
func (c *Client) URL() string { return c.tr.base }

func (c *Client) send(ctx context.Context, method, path string, body []byte, header http.Header) (*sent, error) {
	if body != nil {
		if header == nil {
			header = http.Header{}
		}
		header.Set("Content-Type", "application/json")
	}
	return c.tr.request(ctx, request{method: method, path: path, header: header, body: body})
}

func (c *Client) do(ctx context.Context, method, path string, body []byte) (*response, error) {
	s, err := c.send(ctx, method, path, body, nil)
	if err != nil {
		return nil, err
	}
	return s.res, nil
}

// doJSON sends a request and decodes its 2xx JSON answer into v.
func (c *Client) doJSON(ctx context.Context, method, path string, body any, v any) error {
	data, err := encodeValue(body)
	if err != nil {
		return err
	}
	res, err := c.do(ctx, method, path, data)
	if err != nil {
		return err
	}
	return decodeJSON(res, v)
}

// ---------------------------------------------------------------------------
// Targets
// ---------------------------------------------------------------------------

// Target is a service, a virtual object key or a workflow key to call with Call and Send.
type Target struct {
	c        *Client
	service  string
	key      string
	keyed    bool
	scope    string
	workflow bool
}

// Callee is what Call and Send accept: a Target or a Workflow.
type Callee interface {
	callee() Target
}

func (t Target) callee() Target { return t }

// Service is the service's name.
func (t Target) Service() string { return t.service }

// Key is the object or workflow key ("" for a plain service).
func (t Target) Key() string { return t.key }

// Service is a plain service (no key).
func (c *Client) Service(name string, opts ...TargetOption) Target {
	return Target{c: c, service: name, scope: targetConfigOf(opts).scope}
}

// Object is one key of a virtual object.
func (c *Client) Object(name, key string, opts ...TargetOption) Target {
	return Target{c: c, service: name, key: key, keyed: true, scope: targetConfigOf(opts).scope}
}

// Workflow is one key of a workflow: its run (RunWorkflow, or Call/Send of "run") and its
// shared handlers, plus Attach and Output of the run by key.
type Workflow struct {
	Target
}

// Workflow is one key of a workflow.
func (c *Client) Workflow(name, key string, opts ...TargetOption) Workflow {
	return Workflow{Target{c: c, service: name, key: key, keyed: true, scope: targetConfigOf(opts).scope, workflow: true}}
}

func (w Workflow) attachTarget() (*Client, string, string) {
	return w.c, w.service + "/" + w.key,
		scopePrefix(w.scope) + "/workflow/" + enc(w.service) + "/" + enc(w.key) + "/attach"
}

// invokePath is /invoke/{svc}/{h}[/send][?key=].
func (t Target) invokePath(handler string, send bool) string {
	p := scopePrefix(t.scope) + "/invoke/" + enc(t.service) + "/" + enc(handler)
	if send {
		p += "/send"
	}
	if t.keyed {
		p += "?key=" + enc(t.key)
	}
	return p
}

// headers are the headers of a call, and whether the idempotency key is generated (auto).
// A workflow's run sends no generated key: it is already once per key.
func (t Target) headers(handler string, o *callConfig) (http.Header, bool, error) {
	auto := !(t.workflow && handler == "run")
	h := o.header.Clone()
	switch o.keyMode {
	case keyGiven:
		h.Set("Idempotency-Key", o.key)
	case keyAuto:
		if auto {
			key, err := newKey()
			if err != nil {
				return nil, false, err
			}
			h.Set("Idempotency-Key", key)
		}
	}
	if o.limitKey != "" {
		h.Set("X-Cairn-Limit-Key", o.limitKey)
	}
	if o.logLevel != "" {
		h.Set("Cairnflow-Log-Level", o.logLevel)
	}
	return h, auto, nil
}

// newKey is a random UUID (version 4), like crypto.randomUUID.
func newKey() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	var s [36]byte
	hex.Encode(s[0:8], b[0:4])
	s[8] = '-'
	hex.Encode(s[9:13], b[4:6])
	s[13] = '-'
	hex.Encode(s[14:18], b[6:8])
	s[18] = '-'
	hex.Encode(s[19:23], b[8:10])
	s[23] = '-'
	hex.Encode(s[24:], b[10:])
	return string(s[:]), nil
}

// call sends /invoke and waits for the result; it returns the raw output.
func (t Target) call(ctx context.Context, handler string, input any, o *callConfig) (string, error) {
	body, err := encodeValue(input)
	if err != nil {
		return "", err
	}
	h, auto, err := t.headers(handler, o)
	if err != nil {
		return "", err
	}
	s, err := t.c.send(ctx, http.MethodPost, t.invokePath(handler, false), body, h)
	if err != nil {
		return "", err
	}
	st, err := settle(s.res)
	if err != nil {
		// A workflow's run has no key: an earlier send that failed with a network error may
		// have created this very invocation. Wait for it instead of reporting a duplicate.
		var ai *AlreadyInvoked
		if !(errors.As(err, &ai) && s.resent && !auto) {
			return "", err
		}
		st = settled{id: ai.InvocationID}
	}
	if st.done {
		return st.output, nil
	}
	return t.c.attachLoop(ctx, st.id, "/invocations/"+enc(st.id)+"/attach", o.timeout, o.hasTimeout)
}

// Call calls handler on to with input and waits for the result.
//
// input nil sends no body (the handler sees no input); json.RawMessage and []byte are sent
// as they are; anything else is encoded as JSON. To send JSON null, pass
// json.RawMessage("null").
//
// The result is the handler's output decoded into O; json.RawMessage or []byte receive the
// raw bytes. A 202 never surfaces: the client attaches by id until done or until Timeout
// runs out (*AttachTimeout). Errors: *InvocationFailed, *InvocationPaused, *AlreadyInvoked (a
// workflow key that already has a run), *HTTPError, *NetworkError, or ctx's error.
//
// Every call carries a generated Idempotency-Key kept across its attempts (see
// IdempotencyKey and NoIdempotencyKey); a workflow's run carries none unless given.
func Call[O any](ctx context.Context, to Callee, handler string, input any, opts ...CallOption) (O, error) {
	raw, err := to.callee().call(ctx, handler, input, callConfigOf(opts))
	if err != nil {
		var zero O
		return zero, err
	}
	return decodeOutput[O](raw)
}

// RunWorkflow starts the workflow's run and waits for its result: Call of "run". It sends no
// idempotency key unless one is given; a second run is *AlreadyInvoked.
func RunWorkflow[O any](ctx context.Context, w Workflow, input any, opts ...CallOption) (O, error) {
	return Call[O](ctx, w, "run", input, opts...)
}

// Send starts handler on to with input and returns once the invocation is durable, without
// waiting for its run. Options and input as Call (Timeout is ignored).
func Send(ctx context.Context, to Callee, handler string, input any, opts ...CallOption) (*Invocation, error) {
	t := to.callee()
	o := callConfigOf(opts)
	body, err := encodeValue(input)
	if err != nil {
		return nil, err
	}
	h, auto, err := t.headers(handler, o)
	if err != nil {
		return nil, err
	}
	s, err := t.c.send(ctx, http.MethodPost, t.invokePath(handler, true), body, h)
	if err != nil {
		return nil, err
	}
	res := s.res
	// /send answers 202 once durable; 200, 500 and 409 when a call with the same
	// idempotency key already has an outcome: still the same invocation.
	if ids := res.header.Values("X-Cairn-Invocation-Id"); len(ids) > 0 {
		switch res.status {
		case 200, 202, 409, 429, 500:
			// A 409 paused with the same key is still a handle; only an already_invoked of
			// someone else (a workflow run, not our own resend) is an error.
			if res.status == 409 && !(auto || s.resent) {
				if status, _ := stringField(res.object(), "status"); status == "already_invoked" {
					if _, err := settle(res); err != nil {
						return nil, err
					}
				}
			}
			return t.c.Invocation(ids[0]), nil
		}
	}
	return nil, httpError(res)
}

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

type settled struct {
	done   bool
	output string
	id     string
}

// settle reads the body of /invoke, /send and every attach (docs/api/openapi.json, the
// Invoke* schemas): 200 done, 202 or 429 throttled still running (durable), 500 failed,
// 409 paused or already_invoked. Anything else is an *HTTPError.
func settle(res *response) (settled, error) {
	obj := res.object()
	var id string
	hasID := false
	if ids := res.header.Values("X-Cairn-Invocation-Id"); len(ids) > 0 {
		id, hasID = ids[0], true
	} else {
		id, hasID = stringField(obj, "id")
	}
	status, _ := stringField(obj, "status")
	code := 500
	if n, ok := intField(obj, "code"); ok {
		code = int(n)
	}
	message, _ := stringField(obj, "message")
	if hasID {
		switch {
		case res.status == 200 && status == "completed":
			out, _ := stringField(obj, "output")
			return settled{done: true, output: out, id: id}, nil
		case res.status == 202 && status == "waiting", res.status == 429 && status == "throttled":
			return settled{id: id}, nil
		case res.status == 500 && status == "failed":
			return settled{}, &InvocationFailed{InvocationID: id, Code: code, Message: message}
		case res.status == 409 && status == "paused":
			return settled{}, &InvocationPaused{InvocationID: id, Code: code, Message: message}
		case res.status == 409 && status == "already_invoked":
			return settled{}, &AlreadyInvoked{InvocationID: id}
		}
	}
	return settled{}, httpError(res)
}

// carriesID: the response carries an invocation id (header or body), as the 429 throttled
// of a durable invocation does.
func carriesID(res *response) bool {
	if len(res.header.Values("X-Cairn-Invocation-Id")) > 0 {
		return true
	}
	_, ok := stringField(res.object(), "id")
	return ok
}

// attachLoop waits through attach (path takes ?wait_ms=) until done or until the timeout runs
// out. Attach is a read: a 503 past the transport's cap, a 429 without an id or a network
// error means ask again, with a cap on consecutive failures. Raising those would make the
// caller call again, that is, a second invocation.
func (c *Client) attachLoop(ctx context.Context, id, path string, timeout time.Duration, hasTimeout bool) (string, error) {
	deadline := time.Now().Add(timeout)
	failures := 0
	for {
		wait := c.attachWait
		if hasTimeout {
			left := time.Until(deadline)
			if left <= 0 {
				return "", &AttachTimeout{InvocationID: id}
			}
			wait = min(wait, left)
		}
		res, err := c.do(ctx, http.MethodPost, path+"?wait_ms="+strconv.FormatInt(wait.Milliseconds(), 10), nil)
		if err != nil {
			var ne *NetworkError
			if ctx.Err() != nil || !errors.As(err, &ne) {
				return "", err
			}
			failures++
			if failures > 10 {
				return "", err
			}
			if err := sleep(ctx, backoff(failures)); err != nil {
				return "", err
			}
			continue
		}
		if res.status == http.StatusServiceUnavailable || (res.status == http.StatusTooManyRequests && !carriesID(res)) {
			failures++
			if failures > 10 {
				e := httpError(res)
				e.InvocationID = id
				return "", e
			}
			if err := sleep(ctx, max(backoff(failures), retryAfter(res.header, 0))); err != nil {
				return "", err
			}
			continue
		}
		failures = 0
		st, err := settle(res)
		if err != nil {
			return "", err
		}
		if st.done {
			return st.output, nil
		}
	}
}

// Attachable is what Attach and Output accept: an *Invocation or a Workflow.
type Attachable interface {
	attachTarget() (c *Client, label, path string)
}

// Attach waits until a stops for good and returns its output decoded into O (see Call). It
// long-polls one attach after another; Timeout caps the whole wait (*AttachTimeout).
func Attach[O any](ctx context.Context, a Attachable, opts ...AttachOption) (O, error) {
	o := &attachConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt.applyAttach(o)
		}
	}
	c, label, path := a.attachTarget()
	raw, err := c.attachLoop(ctx, label, path, o.timeout, o.hasTimeout)
	if err != nil {
		var zero O
		return zero, err
	}
	return decodeOutput[O](raw)
}

// Output returns a's output if it is done, without waiting: (value, true, nil) when done,
// (zero, false, nil) while it runs. Failed and paused are errors, as in Call.
func Output[O any](ctx context.Context, a Attachable) (O, bool, error) {
	var zero O
	c, _, path := a.attachTarget()
	res, err := c.do(ctx, http.MethodPost, path+"?wait_ms=0", nil)
	if err != nil {
		return zero, false, err
	}
	st, err := settle(res)
	if err != nil || !st.done {
		return zero, false, err
	}
	v, err := decodeOutput[O](st.output)
	if err != nil {
		return zero, false, err
	}
	return v, true, nil
}
