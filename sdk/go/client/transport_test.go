// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// scripted is an http.RoundTripper that answers from a list: a *http.Response or an error.
type scripted struct {
	mu      sync.Mutex
	replies []any
	calls   []string
	headers []http.Header
}

func (s *scripted) RoundTrip(r *http.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, r.URL.String())
	s.headers = append(s.headers, r.Header.Clone())
	if len(s.replies) == 0 {
		return nil, errors.New("out of scripted replies")
	}
	next := s.replies[0]
	s.replies = s.replies[1:]
	if err, ok := next.(error); ok {
		return nil, err
	}
	return next.(*http.Response), nil
}

func jres(status int, body any, headers ...string) *http.Response {
	b, _ := json.Marshal(body)
	h := http.Header{}
	for i := 0; i+1 < len(headers); i += 2 {
		h.Set(headers[i], headers[i+1])
	}
	return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader(string(b)))}
}

func newTransport(base string, s *scripted) *transport {
	return &transport{
		base:         normalizeURL(base),
		header:       http.Header{},
		followLeader: true,
		maxAttempts:  8,
		maxElapsed:   30 * time.Second,
		retryNetwork: true,
		hc:           &http.Client{Transport: s},
	}
}

func get(t *testing.T, tr *transport, path string) *sent {
	t.Helper()
	s, err := tr.request(context.Background(), request{method: http.MethodGet, path: path})
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	return s
}

func TestTransportFollowsTheLeaderOneHop(t *testing.T) {
	fakeSleep(t)
	s := &scripted{replies: []any{jres(503, map[string]any{"error": "x", "leader": "http://b:2"}), jres(200, map[string]any{"ok": 1})}}
	got := get(t, newTransport("a:1", s), "/invoke/S/h")
	equal(t, got.res.status, 200)
	equal(t, got.node, "http://b:2")
	equalStrings(t, s.calls, []string{"http://a:1/invoke/S/h", "http://b:2/invoke/S/h"})
}

func TestTransportDoesNotFollowAnotherScheme(t *testing.T) {
	fakeSleep(t)
	s := &scripted{replies: []any{jres(503, map[string]any{"error": "x", "leader": "http://b:2"}), jres(200, map[string]any{})}}
	tr := newTransport("https://a", s)
	tr.maxAttempts = 2
	equal(t, get(t, tr, "/x").res.status, 200)
	equalStrings(t, s.calls, []string{"https://a/x", "https://a/x"})
}

func TestTransportFollowLeaderOff(t *testing.T) {
	fakeSleep(t)
	s := &scripted{replies: []any{jres(503, map[string]any{"error": "x", "leader": "http://b:2"}), jres(200, map[string]any{})}}
	tr := newTransport("http://a", s)
	tr.followLeader = false
	get(t, tr, "/x")
	equalStrings(t, s.calls, []string{"http://a/x", "http://a/x"})
}

func TestTransportNoSecondHop(t *testing.T) {
	fakeSleep(t)
	s := &scripted{replies: []any{
		jres(503, map[string]any{"error": "x", "leader": "http://b"}),
		jres(503, map[string]any{"leader": "http://c"}),
		jres(200, map[string]any{}),
	}}
	get(t, newTransport("http://a", s), "/x")
	equalStrings(t, s.calls, []string{"http://a/x", "http://b/x", "http://a/x"})
}

func TestTransport503UntilTheCapReturnsTheLast(t *testing.T) {
	fakeSleep(t)
	s := &scripted{}
	for range 3 {
		s.replies = append(s.replies, jres(503, map[string]any{"error": "x"}))
	}
	tr := newTransport("http://a", s)
	tr.maxAttempts = 3
	equal(t, get(t, tr, "/x").res.status, 503)
	equal(t, len(s.calls), 3)
}

func TestTransportBackoffDoubles(t *testing.T) {
	waits := fakeSleep(t)
	s := &scripted{}
	for range 8 {
		s.replies = append(s.replies, jres(503, map[string]any{"error": "x"}))
	}
	get(t, newTransport("http://a", s), "/x")
	var got []string
	for _, w := range waits() {
		got = append(got, w.String())
	}
	equalStrings(t, got, []string{"100ms", "200ms", "400ms", "800ms", "1.6s", "2s", "2s"})
}

func TestTransportTimeCap(t *testing.T) {
	s := &scripted{}
	for range 20 {
		s.replies = append(s.replies, jres(503, map[string]any{"error": "x"}))
	}
	tr := newTransport("http://a", s)
	tr.maxElapsed = 250 * time.Millisecond
	equal(t, get(t, tr, "/x").res.status, 503)
	if len(s.calls) >= 6 {
		t.Fatalf("%d calls within a 250 ms cap", len(s.calls))
	}
}

func Test429WithoutIDWaitsRetryAfterAndWithIDReturns(t *testing.T) {
	waits := fakeSleep(t)
	s := &scripted{replies: []any{
		jres(429, map[string]any{"code": "range_overloaded"}, "Retry-After", "3"),
		jres(429, map[string]any{"code": "range_overloaded"}),
		jres(429, map[string]any{"code": "range_overloaded"}, "Retry-After", "soon"),
		jres(200, map[string]any{}),
	}}
	equal(t, get(t, newTransport("http://a", s), "/x").res.status, 200)
	equal(t, len(s.calls), 4)
	got := waits()
	equal(t, len(got), 3)
	equal(t, got[0], 3*time.Second)
	equal(t, got[1], time.Second) // absent: 1 s
	equal(t, got[2], time.Second) // not a number: 1 s

	g := &scripted{replies: []any{jres(429, map[string]any{"status": "throttled", "id": "ab"}, "Retry-After", "1")}}
	equal(t, get(t, newTransport("http://a", g), "/x").res.status, 429)
	equal(t, len(g.calls), 1)
}

func TestTransportNetworkErrors(t *testing.T) {
	fakeSleep(t)
	down := errors.New("down")
	s := &scripted{replies: []any{jres(503, map[string]any{"leader": "http://b"}), down, jres(200, map[string]any{})}}
	got := get(t, newTransport("http://a", s), "/x")
	equal(t, got.res.status, 200)
	equal(t, got.resent, false) // the lost request went to the followed leader
	equalStrings(t, s.calls, []string{"http://a/x", "http://b/x", "http://a/x"})

	g := &scripted{replies: []any{down, jres(200, map[string]any{})}}
	got = get(t, newTransport("http://a", g), "/x")
	equal(t, got.res.status, 200)
	equal(t, got.resent, true)

	h := &scripted{replies: []any{down}}
	tr := newTransport("http://a", h)
	tr.retryNetwork = false
	_, err := tr.request(context.Background(), request{method: http.MethodGet, path: "/x"})
	var ne *NetworkError
	if !errors.As(err, &ne) {
		t.Fatalf("got %v, want a *NetworkError", err)
	}
}

func TestTransportDeadLeaderIsNotFollowedAgain(t *testing.T) {
	fakeSleep(t)
	s := &scripted{replies: []any{
		jres(503, map[string]any{"leader": "http://b"}),
		errors.New("down"),
		jres(503, map[string]any{"leader": "http://b"}),
		jres(503, map[string]any{"leader": "http://c"}),
		jres(200, map[string]any{}),
	}}
	equal(t, get(t, newTransport("http://a", s), "/x").res.status, 200)
	equalStrings(t, s.calls, []string{"http://a/x", "http://b/x", "http://a/x", "http://a/x", "http://c/x"})
}

func TestTransportCancellingStopsTheWait(t *testing.T) {
	s := &scripted{}
	for range 8 {
		s.replies = append(s.replies, jres(503, map[string]any{"error": "x"}))
	}
	tr := newTransport("http://a", s)
	tr.header.Set("Authorization", "Bearer k")
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	started := time.Now()
	_, err := tr.request(ctx, request{method: http.MethodPost, path: "/x", header: http.Header{"Idempotency-Key": {"i"}}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if time.Since(started) > time.Second {
		t.Fatalf("cancelling took %v", time.Since(started))
	}
	equal(t, s.headers[0].Get("Authorization"), "Bearer k")
	equal(t, s.headers[0].Get("Idempotency-Key"), "i")
}

func TestRetryAfterParsing(t *testing.T) {
	for _, c := range []struct {
		value string
		set   bool
		want  time.Duration
	}{
		{"", false, 7 * time.Second},
		{"", true, 0},
		{"2", true, 2 * time.Second},
		{" 0.5 ", true, 500 * time.Millisecond},
		{"-3", true, 0},
		{"Wed, 21 Oct 2015 07:28:00 GMT", true, 7 * time.Second},
		{"Inf", true, 7 * time.Second},
	} {
		h := http.Header{}
		if c.set {
			h["Retry-After"] = []string{c.value}
		}
		equal(t, retryAfter(h, 7*time.Second), c.want)
	}
}

func TestNormalizeURL(t *testing.T) {
	equal(t, normalizeURL("a:1"), "http://a:1")
	equal(t, normalizeURL("https://a/"), "https://a")
	equal(t, normalizeURL("http://a//"), "http://a")
}
