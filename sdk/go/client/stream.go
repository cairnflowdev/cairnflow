// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// EventKind is the kind of a stream event.
type EventKind string

// The kinds of stream events (GET /invocations/{id}/streams/{name} in docs/api/openapi.json).
const (
	// EventRecord is a record: Offset, Durable, Step, Attempt, K, Data.
	EventRecord EventKind = "record"
	// EventGap reports Dropped records dropped or overwritten from Offset on.
	EventGap EventKind = "gap"
	// EventSuperseded reports a record superseded by a later attempt.
	EventSuperseded EventKind = "superseded"
	// EventEnd: the stream is sealed and fully read; Next is its number of records.
	EventEnd EventKind = "end"
	// EventMoved: the leader's term ended; Leader is the new leader's ingress port, if known.
	EventMoved EventKind = "moved"
	// EventReset: the stream is read again from 0 (a temporary stream lost with the term,
	// or a new generation after the node died); clear what was shown.
	EventReset EventKind = "reset"
)

// StreamEvent is one event of an invocation's stream. Fields holds every field of the
// event's JSON as sent; use it to tell an absent field from a zero one.
type StreamEvent struct {
	Kind    EventKind
	Offset  int64
	Durable bool
	Step    int64
	Attempt int64
	K       int64
	// Data is the writer's JSON payload of a record.
	Data    json.RawMessage
	Dropped int64
	Leader  string
	Next    int64
	Fields  map[string]json.RawMessage
}

func (e StreamEvent) int(name string) (int64, bool) { return intField(e.Fields, name) }

// key identifies a record across reconnects: step, attempt, k and data.
func (e StreamEvent) key() string {
	var b strings.Builder
	for _, name := range [...]string{"step", "attempt", "k", "data"} {
		raw, ok := e.Fields[name]
		var c bytes.Buffer
		if !ok || json.Compact(&c, raw) != nil {
			c.Reset()
			c.WriteString("null")
		}
		b.Write(c.Bytes())
		b.WriteByte(0)
	}
	return b.String()
}

type rawEvent struct {
	event string
	id    string
	hasID bool
	data  string
}

// parseSSE splits an SSE body into events and hands each to emit until emit returns false.
// Comment lines (": keepalive") are dropped. It returns nil when the body ends cleanly (or
// emit stopped) and the read error when the body breaks.
func parseSSE(body io.Reader, emit func(rawEvent) bool) error {
	r := bufio.NewReader(body)
	ev := rawEvent{event: "message"}
	has := false
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil // an unterminated last block is dropped
			}
			return err
		}
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if line == "" {
			if has && !emit(ev) {
				return nil
			}
			ev = rawEvent{event: "message"}
			has = false
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, found := strings.Cut(line, ":")
		if found {
			value = strings.TrimPrefix(value, " ")
		}
		switch field {
		case "event":
			ev.event = value
			has = true
		case "id":
			ev.id, ev.hasID = value, true
		case "data":
			if ev.data != "" {
				ev.data += "\n" + value
			} else {
				ev.data = value
			}
			has = true
		}
	}
}

func toEvent(raw rawEvent) StreamEvent {
	var d map[string]json.RawMessage
	if json.Unmarshal([]byte(raw.data), &d) != nil {
		d = nil // not a JSON object: keep the kind, drop the fields
	}
	if d == nil {
		d = map[string]json.RawMessage{}
	}
	e := StreamEvent{Kind: EventKind(raw.event), Fields: d}
	e.Offset, _ = intField(d, "offset")
	e.Step, _ = intField(d, "step")
	e.Attempt, _ = intField(d, "attempt")
	e.K, _ = intField(d, "k")
	e.Dropped, _ = intField(d, "dropped")
	e.Next, _ = intField(d, "next")
	e.Leader, _ = stringField(d, "leader")
	if v, ok := d["durable"]; ok {
		_ = json.Unmarshal(v, &e.Durable)
	}
	if v, ok := d["data"]; ok {
		e.Data = v
	}
	return e
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// ReadStream reads the stream name of this client's invocation id, with the client's
// headers, API key and HTTP client. See the package-level ReadStream.
func (c *Client) ReadStream(ctx context.Context, invocationID, name string, opts ...StreamOption) iter.Seq2[StreamEvent, error] {
	cfg := &streamConfig{header: c.tr.header.Clone(), hc: c.tr.hc, followLeader: c.tr.followLeader, retries: 10}
	return readStream(ctx, c.tr.base, invocationID, name, cfg, opts)
}

// ReadStream reads the stream name of invocation invocationID from node base (URL or
// host:port of the ingress port). Streams can only be read at the range's leader;
// ReadStream follows on its own:
//
//   - 503 with leader: ask again at that leader (no leader: wait, then ask again);
//   - 429: wait Retry-After, then ask again;
//   - a moved event: the leader's term ended; reconnect at the new leader, from the next
//     offset (durable stream), or from 0 if a reset follows (a temporary stream is lost with
//     the term, the new attempt regenerates it from the start);
//   - the connection closes without an end (the maximum lifetime of an SSE connection):
//     reconnect from the next offset;
//   - the connection breaks midway (the node died): reconnect from the last record and
//     compare; if the stream is a new generation, yield a reset and read again from 0; a
//     followed leader that does not answer means ask the original node again;
//   - end: stop; 410 stream_expired, 404, ...: yield a *StreamError and stop.
//
// Every event, moved and reset included, is yielded: a UI needs reset to clear what it has
// shown. An error is yielded once, last. Breaking out of the loop closes the connection;
// cancelling ctx stops the reader with ctx's error.
func ReadStream(ctx context.Context, base, invocationID, name string, opts ...StreamOption) iter.Seq2[StreamEvent, error] {
	cfg := &streamConfig{header: http.Header{}, hc: http.DefaultClient, followLeader: true, retries: 10}
	return readStream(ctx, normalizeURL(base), invocationID, name, cfg, opts)
}

func readStream(ctx context.Context, home, id, name string, cfg *streamConfig, opts []StreamOption) iter.Seq2[StreamEvent, error] {
	for _, o := range opts {
		if o != nil {
			o.applyStream(cfg)
		}
	}
	if cfg.hc == nil {
		cfg.hc = http.DefaultClient
	}
	return func(yield func(StreamEvent, error) bool) {
		header := cfg.header.Clone()
		header.Set("Accept", "text/event-stream")
		// 503, 429 and network errors of one connection attempt: the shared transport. The
		// loop below handles the stream's part: reconnects, moved, new generations.
		tr := &transport{
			base:         home,
			header:       header,
			followLeader: cfg.followLeader,
			maxAttempts:  max(1, cfg.retries+1),
			maxElapsed:   30 * time.Second,
			retryNetwork: true,
			hc:           cfg.hc,
		}
		node := home
		next := cfg.from
		failures := 0
		// The last record yielded, for reconnecting after the connection breaks: if the node
		// died and was rebuilt (or the range moved), a temporary stream is a new generation
		// (offsets from 0 again, the rerun regenerates steps from the start), and asking
		// from=next would lose its start. So reconnect from the last record itself and
		// compare: a match means the same generation; anything else (or a shorter stream)
		// yields reset and reads again from 0. A clean close needs no comparison.
		var last *struct {
			offset int64
			key    string
		}
		verify := false
		fail := func(err error) bool {
			failures++
			if failures > cfg.retries {
				yield(StreamEvent{}, err)
				return false
			}
			if err := sleep(ctx, backoff(failures)); err != nil {
				yield(StreamEvent{}, err)
				return false
			}
			return true
		}
		for {
			path := "/invocations/" + enc(id) + "/streams/" + enc(name) + "?from=" + strconv.FormatInt(next, 10)
			s, err := tr.request(ctx, request{method: http.MethodGet, path: path, node: node, stream: true})
			if err != nil {
				var ne *NetworkError
				if ctx.Err() != nil || !errors.As(err, &ne) {
					yield(StreamEvent{}, err)
					return
				}
				node = home
				if !fail(err) {
					return
				}
				continue
			}
			res := s.res
			node = s.node
			// The transport already followed leader and waited Retry-After up to its cap.
			if res.status == http.StatusServiceUnavailable || res.status == http.StatusTooManyRequests {
				res.close()
				if !fail(streamError(res)) {
					return
				}
				continue
			}
			if !res.ok() || res.stream == nil {
				res.close()
				yield(StreamEvent{}, streamError(res))
				return
			}
			moved := ""
			reborn := false
			stopped := false
			perr := parseSSE(res.stream, func(raw rawEvent) bool {
				failures = 0
				e := toEvent(raw)
				// The last record has left the buffer: a gap covers it. Generations cannot be
				// compared and nothing proves a new generation: treat it as the same
				// generation; the reader still sees the gap.
				if verify && last != nil && e.Kind == EventGap {
					if off, ok := e.int("offset"); ok {
						dropped, _ := e.int("dropped")
						if off <= last.offset && last.offset < off+dropped {
							verify = false
						}
					}
				}
				if verify && last != nil && (e.Kind == EventRecord || e.Kind == EventEnd) {
					verify = false
					if off, ok := e.int("offset"); ok && e.Kind == EventRecord && off == last.offset && e.key() == last.key {
						next = last.offset + 1
						return true // overlap: already yielded
					}
					reborn = true
					return false
				}
				if raw.hasID && isDigits(raw.id) {
					if n, err := strconv.ParseInt(raw.id, 10, 64); err == nil {
						next = n + 1
					}
				} else if off, ok := e.int("offset"); ok && e.Kind == EventGap {
					dropped, _ := e.int("dropped")
					next = off + dropped
				}
				if off, ok := e.int("offset"); ok && e.Kind == EventRecord {
					last = &struct {
						offset int64
						key    string
					}{off, e.key()}
				}
				if !yield(e, nil) {
					stopped = true
					return false
				}
				switch e.Kind {
				case EventEnd:
					stopped = true
					return false
				case EventMoved:
					moved = e.Leader
				case EventReset:
					next = 0
					last = nil
				}
				return true
			})
			res.close()
			if stopped {
				return
			}
			if perr != nil {
				if ctx.Err() != nil {
					yield(StreamEvent{}, ctx.Err())
					return
				}
				// The body broke midway (the node died): reconnect from the last record and
				// compare generations.
				if last != nil {
					verify = true
					next = last.offset
				}
				if !fail(&NetworkError{Err: perr}) {
					return
				}
				continue
			}
			if reborn {
				if !yield(StreamEvent{Kind: EventReset, Fields: map[string]json.RawMessage{}}, nil) {
					return
				}
				next = 0
				last = nil
				continue
			}
			// Same rule as the transport's 503 with leader: do not follow a leader with
			// another scheme (an API key does not go over http:// to the internal address the
			// leader advertises when the client speaks https://). Skipped: keep reading at
			// the original node.
			if moved != "" && cfg.followLeader {
				to := normalizeURL(moved)
				if scheme(to) == scheme(home) {
					node = to
				} else {
					node = home
				}
			}
			// Closed without end (maximum lifetime, or a leader change): reconnect from next.
		}
	}
}
