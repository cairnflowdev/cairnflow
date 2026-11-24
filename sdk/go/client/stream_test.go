// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func recAt(offset, attempt int, data string) ev {
	return rec(offset, map[string]any{"offset": offset, "durable": false, "step": 3, "attempt": attempt, "k": offset, "data": data})
}

func fromOf(req seenRequest) int {
	n, _ := strconv.Atoi(req.query("from"))
	return n
}

// 503 with leader: ask the leader. moved then reset: the temporary stream is read again
// from 0 at the new leader.
func TestStream503LeaderThenMovedAndReset(t *testing.T) {
	fakeSleep(t)
	var calls atomic.Int32
	var b *fake
	b = newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		if calls.Add(1) == 1 {
			sse(w,
				rec(0, map[string]any{"offset": 0, "durable": false, "data": map[string]any{"tok": 0}}),
				rec(1, map[string]any{"offset": 1, "durable": false, "data": map[string]any{"tok": 1}}),
				other("moved", map[string]any{"leader": b.URL}),
				other("reset", map[string]any{}))
			return
		}
		sse(w, rec(0, map[string]any{"offset": 0, "durable": false, "data": map[string]any{"tok": "a"}}), other("end", map[string]any{"next": 1}))
	})
	a := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		reply(w, 503, map[string]any{"error": "not the leader", "leader": b.hostPort()})
	})
	got, err := collect(t, ReadStream(bg, a.URL, testID, "ui"), 50)
	if err != nil {
		t.Fatal(err)
	}
	equalStrings(t, kinds(got), []string{"record", "record", "moved", "reset", "record", "end"})
	equal(t, string(got[4].Data), `{"tok":"a"}`)
	equal(t, got[1].Offset, int64(1))
	equal(t, got[0].Durable, false)
	equal(t, len(got[0].Fields), 3)
	equal(t, got[5].Next, int64(1))
	equal(t, len(a.requests()), 1)
	equal(t, fromOf(b.requests()[1]), 0)
	equal(t, a.last().Header.Get("Accept"), "text/event-stream")
}

// A durable stream: moved without reset reconnects from the next offset, and so does a
// connection closed without end (the maximum lifetime of an SSE connection).
func TestDurableStreamReconnectsFromTheNextOffset(t *testing.T) {
	fakeSleep(t)
	var calls atomic.Int32
	var s *fake
	s = newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		switch calls.Add(1) {
		case 1:
			sse(w, rec(0, map[string]any{"offset": 0, "durable": true, "data": 1}), other("moved", map[string]any{"leader": s.URL}))
		case 2:
			sse(w, rec(1, map[string]any{"offset": 1, "durable": true, "data": 2}))
		default:
			sse(w, rec(2, map[string]any{"offset": 2, "durable": true, "data": 3}), other("end", map[string]any{"next": 3}))
		}
	})
	got, err := collect(t, ReadStream(bg, s.URL, testID, "$log", WithAPIKey("k")), 50)
	if err != nil {
		t.Fatal(err)
	}
	var offsets []string
	for _, e := range got {
		if e.Kind == EventRecord {
			offsets = append(offsets, strconv.FormatInt(e.Offset, 10))
			equal(t, e.Durable, true)
		}
	}
	equalStrings(t, offsets, []string{"0", "1", "2"})
	equal(t, got[len(got)-1].Kind, EventEnd)
	equal(t, got[len(got)-1].Next, int64(3))
	seen := s.requests()
	equal(t, fromOf(seen[1]), 1)
	equal(t, fromOf(seen[2]), 2)
	equal(t, seen[0].Target, "/invocations/"+testID+"/streams/%24log?from=0")
	equal(t, seen[0].Header.Get("Authorization"), "Bearer k")
}

func TestStream410StreamExpired(t *testing.T) {
	s := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		reply(w, 410, map[string]any{"error": "expired", "code": "stream_expired"})
	})
	_, err := collect(t, ReadStream(bg, s.URL, testID, "ui"), 50)
	var se *StreamError
	if !errors.As(err, &se) {
		t.Fatalf("got %v, want *StreamError", err)
	}
	equal(t, *se, StreamError{Status: 410, Code: "stream_expired", Message: "expired"})
}

// The connection breaks midway (the node died): reconnect, compare the last record; the
// same generation means no duplicate records.
func TestBrokenConnectionSameGenerationNoDuplicates(t *testing.T) {
	fakeSleep(t)
	var calls atomic.Int32
	recs := []ev{recAt(0, 0, "a"), recAt(1, 0, "b"), recAt(2, 0, "c")}
	s := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		if calls.Add(1) == 1 {
			die(w, recs[:2]...)
			return
		}
		sse(w, append(append([]ev{}, recs[fromOf(req):]...), other("end", map[string]any{"next": 3}))...)
	})
	got, err := collect(t, ReadStream(bg, s.URL, testID, "ui"), 50)
	if err != nil {
		t.Fatal(err)
	}
	equalStrings(t, kinds(got), []string{"record", "record", "record", "end"})
	var data []string
	for _, e := range got[:3] {
		data = append(data, string(e.Data))
	}
	equalStrings(t, data, []string{`"a"`, `"b"`, `"c"`})
	equal(t, fromOf(s.requests()[1]), 1)
}

// The node restarted midway (a new hub, a new generation of the temporary stream): reset,
// then read again from 0.
func TestNewGenerationAfterABreakResets(t *testing.T) {
	fakeSleep(t)
	var calls atomic.Int32
	s := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		if calls.Add(1) == 1 {
			die(w, recAt(0, 0, "a"), recAt(1, 0, "b"))
			return
		}
		fresh := []ev{recAt(0, 1, "x"), recAt(1, 1, "y")}
		sse(w, append(append([]ev{}, fresh[fromOf(req):]...), other("end", map[string]any{"next": 2}))...)
	})
	got, err := collect(t, ReadStream(bg, s.URL, testID, "ui"), 50)
	if err != nil {
		t.Fatal(err)
	}
	equalStrings(t, kinds(got), []string{"record", "record", "reset", "record", "record", "end"})
	equal(t, string(got[3].Data), `"x"`)
	equal(t, string(got[4].Data), `"y"`)
}

// The leader is dead for good: go back to the origin node to ask for the new leader, never
// keep knocking on a dead address.
func TestDeadLeaderGoesBackToTheOriginNode(t *testing.T) {
	fakeSleep(t)
	rec2 := func(i int, d string) ev {
		return rec(i, map[string]any{"offset": i, "durable": true, "step": 1, "attempt": 0, "k": i, "data": d})
	}
	c := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		all := []ev{rec2(0, "a"), rec2(1, "b")}
		sse(w, append(append([]ev{}, all[fromOf(req):]...), other("end", map[string]any{"next": 2}))...)
	})
	var b *fake
	b = newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		_ = b.Listener.Close() // nothing answers at b any more
		die(w, rec2(0, "a"))
	})
	var asked atomic.Int32
	a := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		to := c
		if asked.Add(1) == 1 {
			to = b
		}
		reply(w, 503, map[string]any{"error": "not the leader", "leader": to.hostPort()})
	})
	got, err := collect(t, ReadStream(bg, a.URL, testID, "$log"), 50)
	if err != nil {
		t.Fatal(err)
	}
	equalStrings(t, kinds(got), []string{"record", "record", "end"})
	equal(t, asked.Load(), int32(2)) // back to the origin node once more
}

// A moved to another scheme is dropped: no connection to it, reading continues at the origin
// node (an API key never goes to an address with another scheme).
func TestMovedToAnotherSchemeIsNotFollowed(t *testing.T) {
	fakeSleep(t)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var foreign atomic.Int32
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			foreign.Add(1)
			_ = conn.Close()
		}
	}()
	defer l.Close()
	var calls atomic.Int32
	home := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		if calls.Add(1) == 1 {
			sse(w, other("moved", map[string]any{"leader": "https://" + l.Addr().String()}))
			return
		}
		sse(w, other("end", map[string]any{"next": 0}))
	})
	got, err := collect(t, ReadStream(bg, home.URL, testID, "ui"), 50)
	if err != nil {
		t.Fatal(err)
	}
	equalStrings(t, kinds(got), []string{"moved", "end"})
	equal(t, foreign.Load(), int32(0))
}

// Reconnecting after a break when the last record has left the buffer: the server sends a
// gap covering it, then a later record. That proves no new generation.
// Must see: no reset; the gap reaches the reader; reading continues.
func TestGapOverTheLastRecordIsNotAReset(t *testing.T) {
	fakeSleep(t)
	var calls atomic.Int32
	s := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		if calls.Add(1) == 1 {
			die(w, recAt(0, 0, "a"), recAt(1, 0, "b"))
			return
		}
		sse(w, other("gap", map[string]any{"offset": 1, "dropped": 3}), recAt(4, 0, "e"), other("end", map[string]any{"next": 5}))
	})
	got, err := collect(t, ReadStream(bg, s.URL, testID, "ui"), 50)
	if err != nil {
		t.Fatal(err)
	}
	equalStrings(t, kinds(got), []string{"record", "record", "gap", "record", "end"})
	equal(t, got[2].Dropped, int64(3))
	equal(t, got[3].Offset, int64(4))
}

func TestStreamGivesUpAfterRetries(t *testing.T) {
	fakeSleep(t)
	s := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		reply(w, 503, map[string]any{"error": "electing"})
	})
	_, err := collect(t, ReadStream(bg, s.URL, testID, "ui", Retries(1)), 50)
	var se *StreamError
	if !errors.As(err, &se) {
		t.Fatalf("got %v, want *StreamError", err)
	}
	equal(t, se.Status, 503)
	// Two reader attempts, each one capped at Retries+1 transport attempts.
	equal(t, len(s.requests()), 4)
}

func TestBreakingOutClosesTheConnection(t *testing.T) {
	closed := make(chan struct{})
	s := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		sse(w, rec(0, map[string]any{"offset": 0, "data": 1}))
		select {
		case <-r.Context().Done():
			close(closed)
		case <-time.After(5 * time.Second):
		}
	})
	got, err := collect(t, ReadStream(bg, s.URL, testID, "ui"), 1)
	equal(t, err, nil)
	equal(t, len(got), 1)
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("the reader did not close the connection")
	}
}

func TestCancellingTheContextStopsTheReader(t *testing.T) {
	s := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		sse(w, rec(0, map[string]any{"offset": 0, "data": 1}))
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	})
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	var got []StreamEvent
	var last error
	for e, err := range ReadStream(ctx, s.URL, testID, "ui") {
		if err != nil {
			last = err
			break
		}
		got = append(got, e)
		cancel()
	}
	equal(t, len(got), 1)
	if !errors.Is(last, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", last)
	}
}

func TestClientReadStreamUsesTheClientsKey(t *testing.T) {
	s := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		sse(w, other("end", map[string]any{"next": 0}))
	})
	got, err := collect(t, newClient(t, s.URL, WithAPIKey("cf_x")).ReadStream(bg, "id/1", "ui", From(7)), 50)
	equal(t, err, nil)
	equalStrings(t, kinds(got), []string{"end"})
	equal(t, s.last().Header.Get("Authorization"), "Bearer cf_x")
	equal(t, s.last().Target, "/invocations/id%2F1/streams/ui?from=7")
}

func TestParseSSE(t *testing.T) {
	body := ": keepalive\r\n\r\nevent: record\r\nid: 4\r\ndata: {\"a\":\r\ndata: 1}\r\n\r\ndata: x\n\nevent: end\ndata: {}"
	var got []rawEvent
	err := parseSSE(strings.NewReader(body), func(e rawEvent) bool {
		got = append(got, e)
		return true
	})
	equal(t, err, nil)
	equal(t, len(got), 2) // the unterminated last block is dropped
	equal(t, got[0], rawEvent{event: "record", id: "4", hasID: true, data: "{\"a\":\n1}"})
	equal(t, got[1], rawEvent{event: "message", data: "x"})
	e := toEvent(got[0])
	equal(t, e.Kind, EventRecord)
	equal(t, string(e.Fields["a"]), "1")
}
