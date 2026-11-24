// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

const testID = "abababababababababababababababababababababababab"

// seenRequest is one request a fake node received.
type seenRequest struct {
	Method string
	// Target is the request target as sent: escaped path and query.
	Target string
	Header http.Header
	Body   []byte
}

func (r seenRequest) path() string {
	p, _, _ := strings.Cut(r.Target, "?")
	u, err := url.PathUnescape(p)
	if err != nil {
		return p
	}
	return u
}

func (r seenRequest) rawPath() string {
	p, _, _ := strings.Cut(r.Target, "?")
	return p
}

func (r seenRequest) query(name string) string {
	_, q, _ := strings.Cut(r.Target, "?")
	v, _ := url.ParseQuery(q)
	return v.Get(name)
}

func (r seenRequest) json(t *testing.T) any {
	t.Helper()
	if len(r.Body) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(r.Body, &v); err != nil {
		t.Fatalf("request body is not JSON: %q", r.Body)
	}
	return v
}

// fake is one fake node: every request is recorded, then handed to the handler.
type fake struct {
	*httptest.Server
	mu   sync.Mutex
	seen []seenRequest
}

func newFake(t *testing.T, h func(w http.ResponseWriter, r *http.Request, req seenRequest)) *fake {
	t.Helper()
	f := &fake{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		req := seenRequest{Method: r.Method, Target: r.RequestURI, Header: r.Header.Clone(), Body: body}
		f.mu.Lock()
		f.seen = append(f.seen, req)
		f.mu.Unlock()
		h(w, r, req)
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fake) requests() []seenRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]seenRequest(nil), f.seen...)
}

func (f *fake) last() seenRequest {
	s := f.requests()
	return s[len(s)-1]
}

func (f *fake) hostPort() string { return strings.TrimPrefix(f.URL, "http://") }

func reply(w http.ResponseWriter, status int, body any, headers ...string) {
	for i := 0; i+1 < len(headers); i += 2 {
		w.Header().Set(headers[i], headers[i+1])
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body != nil {
		_ = json.NewEncoder(w).Encode(body)
	}
}

func completed(output any, id string) map[string]any {
	b, _ := json.Marshal(output)
	return map[string]any{"id": id, "status": "completed", "output": string(b)}
}

// abort closes the connection without an answer: the response is lost.
func abort(w http.ResponseWriter) {
	conn, _, err := http.NewResponseController(w).Hijack()
	if err == nil {
		_ = conn.Close()
	}
}

type ev struct {
	kind string
	data any
	id   int // -1: no id line
}

func rec(offset int, data any) ev {
	return ev{"record", data, offset}
}

func sseHead(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, ": keepalive\n\n")
}

// sse writes an SSE response; the body ends when the handler returns.
func sse(w http.ResponseWriter, events ...ev) {
	sseHead(w)
	for _, e := range events {
		b, _ := json.Marshal(e.data)
		id := ""
		if e.id >= 0 {
			id = fmt.Sprintf("id: %d\n", e.id)
		}
		_, _ = fmt.Fprintf(w, "event: %s\n%sdata: %s\n\n", e.kind, id, b)
	}
	_ = http.NewResponseController(w).Flush()
}

// die breaks the connection like a node under kill -9: no terminating chunk.
func die(w http.ResponseWriter, events ...ev) {
	sse(w, events...)
	time.Sleep(10 * time.Millisecond)
	abort(w)
}

func other(kind string, data any) ev { return ev{kind, data, -1} }

// newClient connects to url with an HTTP client of its own (no connection shared between
// tests).
func newClient(t *testing.T, url string, opts ...Option) *Client {
	t.Helper()
	tr := &http.Transport{}
	t.Cleanup(tr.CloseIdleConnections)
	return Connect(url, append([]Option{WithHTTPClient(&http.Client{Transport: tr})}, opts...)...)
}

// fakeSleep makes every backoff and Retry-After wait return at once and records it.
func fakeSleep(t *testing.T) func() []time.Duration {
	t.Helper()
	var mu sync.Mutex
	var waits []time.Duration
	old := sleep
	sleep = func(ctx context.Context, d time.Duration) error {
		mu.Lock()
		waits = append(waits, d)
		mu.Unlock()
		return ctx.Err()
	}
	t.Cleanup(func() { sleep = old })
	return func() []time.Duration {
		mu.Lock()
		defer mu.Unlock()
		return append([]time.Duration(nil), waits...)
	}
}

func collect(t *testing.T, seq func(func(StreamEvent, error) bool), limit int) ([]StreamEvent, error) {
	t.Helper()
	var out []StreamEvent
	for e, err := range seq {
		if err != nil {
			return out, err
		}
		out = append(out, e)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func kinds(evs []StreamEvent) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = string(e.Kind)
	}
	return out
}

func equal[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func equalJSON(t *testing.T, got any, want string) {
	t.Helper()
	g, _ := json.Marshal(got)
	var w any
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("bad want %q: %v", want, err)
	}
	wb, _ := json.Marshal(w)
	if string(g) != string(wb) {
		t.Fatalf("got %s, want %s", g, wb)
	}
}

func equalStrings(t *testing.T, got, want []string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") || len(got) != len(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}
