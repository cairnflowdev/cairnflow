// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// The retry layer every request of this package goes through. It implements the cluster's
// error contract (docs/admin-api.md, the error contract section):
//
//   - 503 with leader: ask again at that leader, one hop per attempt, only when it has the
//     same scheme as the original URL (an API key does not go to http:// from an https://);
//     can be turned off with WithFollowLeader(false);
//   - 503 without leader (the cluster is electing, a range is changing): capped exponential
//     backoff, then ask the original node again;
//   - 429 without id (range_overloaded, sse_limit: nothing created yet): wait Retry-After and
//     resend; 429 with id (throttled: already durable): return it to the caller, resending
//     is its job (usually an attach by id);
//   - a network error at a followed node: back off and return to the original node, and do
//     not follow that node again in this call (the original node keeps pointing at the old
//     leader until its lease expires, so following again would burn every attempt);
//   - a cap on attempts and a cap on time; past the cap, return the last response or the
//     last network error.
//
// A request that creates an invocation must carry an Idempotency-Key so that a resend points
// at that same invocation. The transport does not add one; the call paths of Client do.

// NetworkError reports a request that did not get a complete response: refused, reset, timed
// out or cut midway. The request may still have reached the server.
type NetworkError struct {
	Err error
}

func (e *NetworkError) Error() string { return "cairnflow client: network error: " + e.Err.Error() }

func (e *NetworkError) Unwrap() error { return e.Err }

// sleep waits d or until ctx is done. Tests replace it to run without waiting.
var sleep = func(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// backoff is the wait before attempt n+1 (n from 1): 100 ms, 200 ms, ... capped at 2 s.
func backoff(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	if n > 6 {
		return 2 * time.Second
	}
	return min(2*time.Second, 100*time.Millisecond<<(n-1))
}

// retryAfter reads a Retry-After header in seconds: def when absent or not a finite number,
// 0 when empty, never negative.
func retryAfter(h http.Header, def time.Duration) time.Duration {
	vs := h.Values("Retry-After")
	if len(vs) == 0 {
		return def
	}
	text := strings.TrimSpace(vs[0])
	if text == "" {
		return 0
	}
	v, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsInf(v, 0) || math.IsNaN(v) {
		return def
	}
	if v <= 0 {
		return 0
	}
	return time.Duration(v * float64(time.Second))
}

// normalizeURL turns host:port into http://host:port and drops trailing slashes.
func normalizeURL(node string) string {
	n := strings.TrimRight(node, "/")
	if strings.HasPrefix(n, "http://") || strings.HasPrefix(n, "https://") {
		return n
	}
	return "http://" + n
}

func scheme(url string) string {
	if i := strings.IndexByte(url, ':'); i >= 0 {
		return url[:i]
	}
	return ""
}

// response is one HTTP response. A buffered response holds its whole body in body; a
// streaming one (stream mode and a 2xx status) is read from stream and must be closed.
type response struct {
	status int
	header http.Header
	body   []byte
	stream io.ReadCloser
}

func (r *response) ok() bool { return r.status >= 200 && r.status < 300 }

func (r *response) close() {
	if r.stream != nil {
		_ = r.stream.Close()
	}
}

// object is the body as a JSON object, nil when it is not one.
func (r *response) object() map[string]json.RawMessage {
	var m map[string]json.RawMessage
	if json.Unmarshal(r.body, &m) != nil {
		return nil
	}
	return m
}

// sent is the outcome of one request: the last response and the node that answered it.
type sent struct {
	res  *response
	node string
	// resent: an earlier send of the same request failed with a network error and was
	// resent. That one may have reached the server, so a 409 already_invoked may be the
	// caller's own.
	resent bool
}

type request struct {
	method string
	path   string // with the query
	header http.Header
	body   []byte // nil: no body
	node   string // "": the original URL
	stream bool
}

type transport struct {
	base         string
	header       http.Header
	followLeader bool
	maxAttempts  int
	maxElapsed   time.Duration
	retryNetwork bool
	hc           *http.Client
	// timeout caps one exchange (a buffered response) or the wait for the response head
	// (stream mode); 0 means no cap.
	timeout time.Duration
}

// cancelOnClose releases the request's context when the caller closes the stream.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

// fetch sends one attempt to node at. Errors are *NetworkError, ctx.Err() or a request that
// cannot be built.
func (t *transport) fetch(ctx context.Context, r request, at string) (*response, error) {
	actx, cancel := context.WithCancel(ctx)
	var timer *time.Timer
	if t.timeout > 0 {
		timer = time.AfterFunc(t.timeout, cancel)
	}
	stop := func() {
		if timer != nil {
			timer.Stop()
		}
	}
	var body io.Reader
	if r.body != nil {
		body = bytes.NewReader(r.body)
	}
	req, err := http.NewRequestWithContext(actx, r.method, at+r.path, body)
	if err != nil {
		stop()
		cancel()
		return nil, err
	}
	for k, vs := range t.header {
		req.Header[k] = append([]string(nil), vs...)
	}
	for k, vs := range r.header {
		req.Header[k] = append([]string(nil), vs...)
	}
	res, err := t.hc.Do(req)
	if err != nil {
		stop()
		cancel()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &NetworkError{Err: err}
	}
	if r.stream && res.StatusCode >= 200 && res.StatusCode < 300 && res.StatusCode != http.StatusNoContent {
		stop() // the timeout caps the wait for the head only
		return &response{status: res.StatusCode, header: res.Header, stream: &cancelOnClose{res.Body, cancel}}, nil
	}
	data, err := io.ReadAll(res.Body)
	_ = res.Body.Close()
	stop()
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &NetworkError{Err: err}
	}
	return &response{status: res.StatusCode, header: res.Header, body: data}, nil
}

// request sends r under the error contract at the top of this file.
func (t *transport) request(ctx context.Context, r request) (*sent, error) {
	started := time.Now()
	at := t.base
	if r.node != "" {
		at = normalizeURL(r.node)
	}
	followed := false
	// Followed leaders that did not answer: not followed again in this call.
	dead := map[string]bool{}
	resent := false
	for attempt := 1; ; attempt++ {
		last := attempt >= t.maxAttempts || time.Since(started) >= t.maxElapsed
		res, err := t.fetch(ctx, r, at)
		if err != nil {
			var ne *NetworkError
			if ctx.Err() != nil || !errors.As(err, &ne) {
				return nil, err
			}
			if at != t.base {
				// A followed node did not answer: back off and return to the original
				// node; it learns the new leader when the old lease expires.
				dead[at] = true
				at = t.base
				followed = false
				if last {
					return nil, err
				}
				if err := t.wait(ctx, backoff(attempt), started); err != nil {
					return nil, err
				}
				continue
			}
			if !t.retryNetwork || last {
				return nil, err
			}
			resent = true
			if err := t.wait(ctx, backoff(attempt), started); err != nil {
				return nil, err
			}
			continue
		}
		if res.status == http.StatusServiceUnavailable {
			leader, _ := stringField(res.object(), "leader")
			to := normalizeURL(leader)
			if t.followLeader && !followed && leader != "" && scheme(to) == scheme(t.base) &&
				to != at && !dead[to] && !last {
				res.close()
				at = to
				followed = true
				continue
			}
			if last {
				return &sent{res: res, node: at, resent: resent}, nil
			}
			res.close()
			if err := t.wait(ctx, backoff(attempt), started); err != nil {
				return nil, err
			}
			at = t.base
			followed = false
			continue
		}
		if res.status == http.StatusTooManyRequests && !hasField(res.object(), "id") && !last {
			after := retryAfter(res.header, time.Second)
			res.close()
			if err := t.wait(ctx, after, started); err != nil {
				return nil, err
			}
			continue
		}
		return &sent{res: res, node: at, resent: resent}, nil
	}
}

// wait waits d, no longer than what is left of the time cap.
func (t *transport) wait(ctx context.Context, d time.Duration, started time.Time) error {
	left := t.maxElapsed - time.Since(started)
	return sleep(ctx, max(0, min(d, left)))
}

func hasField(m map[string]json.RawMessage, name string) bool {
	_, ok := m[name]
	return ok
}

func stringField(m map[string]json.RawMessage, name string) (string, bool) {
	raw, ok := m[name]
	if !ok {
		return "", false
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return "", false
	}
	return s, true
}

// intField reads an integral JSON number (1 and 1.0, not true or "1").
func intField(m map[string]json.RawMessage, name string) (int64, bool) {
	raw, ok := m[name]
	if !ok {
		return 0, false
	}
	// json.Number also accepts a quoted number: only a bare number counts here.
	if t := bytes.TrimSpace(raw); len(t) == 0 || (t[0] != '-' && (t[0] < '0' || t[0] > '9')) {
		return 0, false
	}
	var n json.Number
	if json.Unmarshal(raw, &n) != nil {
		return 0, false
	}
	if i, err := n.Int64(); err == nil {
		return i, true
	}
	f, err := n.Float64()
	if err != nil || f != math.Trunc(f) || math.Abs(f) > 1<<53 {
		return 0, false
	}
	return int64(f), true
}
