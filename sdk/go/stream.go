// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
)

// The system streams of the durable log and the trace.
const (
	logStream   = "$log"
	traceStream = "$trace"
	// maxStreamName matches the runtime's cap on a stream name, in bytes.
	maxStreamName = 128
)

// Stream is a stream of this invocation; see [Context.Stream].
type Stream struct {
	b       *ctxBase
	name    string
	durable bool
	// scope is fixed for a stream made from a RunContext; nil: decided by the calling
	// goroutine (the handler's, or a running closure's).
	scope *core.Leaf
}

func newStream(b *ctxBase, name string, opts []StreamOption, scope *core.Leaf) (*Stream, error) {
	if name == "" || len(name) > maxStreamName || strings.HasPrefix(name, "$") {
		return nil, fmt.Errorf("cairn: invalid stream name %q (1-%d bytes, must not start with \"$\")", name, maxStreamName)
	}
	var o streamOptions
	for _, opt := range opts {
		opt.applyStream(&o)
	}
	return &Stream{b: b, name: wellFormed(name), durable: o.durable, scope: scope}, nil
}

func (b *ctxBase) Stream(name string, opts ...StreamOption) (*Stream, error) {
	return newStream(b, name, opts, nil)
}

// Append adds a record (JSON). It does not wait and does not fail: a value that cannot
// be encoded becomes {"unserializable": "<error>"}; records are dropped on replay and
// below wire v4.
func (s *Stream) Append(record any) {
	payload := core.StreamJSON(record)
	s.b.append(s.scope, core.Append{Stream: s.name, Durable: s.durable, Build: func(core.Ident) []byte { return payload }})
}

// Close closes the stream: no record after that.
func (s *Stream) Close() {
	s.b.append(s.scope, core.Append{Stream: s.name, Durable: s.durable})
}

// append sends a record from the calling goroutine's scope (or the fixed one).
func (b *ctxBase) append(fixed *core.Leaf, a core.Append) (core.Ident, bool) {
	if fixed != nil {
		a.Closure = *fixed
		return b.inv.Driver().Append(a)
	}
	scope, ok := b.inv.Scope()
	if !ok {
		b.inv.Begin() // panics with the goroutine message
	}
	if scope.IsZero() {
		b.inv.Begin() // a record from a defer after the turn stopped exits again
	}
	a.Closure = scope
	return b.inv.Driver().Append(a)
}

// ── log ──────────────────────────────────────────────────────────────────

// logLevels orders the level names of the durable log.
var logLevels = map[string]slog.Level{"debug": slog.LevelDebug, "info": slog.LevelInfo, "warn": slog.LevelWarn, "error": slog.LevelError}

func levelName(l slog.Level) string {
	switch {
	case l < slog.LevelInfo:
		return "debug"
	case l < slog.LevelWarn:
		return "info"
	case l < slog.LevelError:
		return "warn"
	default:
		return "error"
	}
}

// logHandler is the slog.Handler of the durable log: each record goes to the `$log`
// stream as {"ts", "level", "msg", "attrs"} (the layout of the other SDKs).
type logHandler struct {
	b *ctxBase
	// dynamic: the scope is the calling goroutine's; otherwise scope is fixed.
	dynamic bool
	scope   core.Leaf
	// frames are the open groups, the root first (its group is ""); each holds the
	// attributes added while it was the innermost.
	frames []logFrame
}

type logFrame struct {
	group string
	attrs []slog.Attr
}

func (h *logHandler) Enabled(_ context.Context, l slog.Level) bool {
	min, ok := logLevels[h.b.info.LogLevel]
	if !ok {
		min = slog.LevelInfo
	}
	return l >= min
}

func (h *logHandler) Handle(_ context.Context, r slog.Record) error {
	var cur []slog.Attr
	r.Attrs(func(a slog.Attr) bool {
		cur = append(cur, a)
		return true
	})
	// The record's attributes belong to the innermost group; wrap outwards.
	for i := len(h.frames) - 1; i >= 0; i-- {
		f := h.frames[i]
		all := append(append([]slog.Attr(nil), f.attrs...), cur...)
		switch {
		case f.group == "":
			cur = all
		case len(all) == 0:
			cur = nil
		default:
			cur = []slog.Attr{{Key: f.group, Value: slog.GroupValue(all...)}}
		}
	}
	var attrs core.Object
	writeAttrs(&attrs, cur)
	ts, level, msg := h.b.inv.Driver().Now(), levelName(r.Level), r.Message
	build := func(core.Ident) []byte {
		var rec core.Object
		rec.Field("ts", ts)
		rec.Field("level", level)
		rec.Field("msg", msg)
		if attrs.Len() > 0 {
			rec.Raw("attrs", attrs.Bytes())
		}
		return rec.Bytes()
	}
	var fixed *core.Leaf
	if !h.dynamic {
		fixed = &h.scope
	}
	h.b.append(fixed, core.Append{Stream: logStream, Durable: true, Build: build})
	return nil
}

func (h *logHandler) clone() *logHandler {
	c := *h
	c.frames = append([]logFrame(nil), h.frames...)
	if len(c.frames) == 0 {
		c.frames = []logFrame{{}}
	}
	return &c
}

func (h *logHandler) WithAttrs(as []slog.Attr) slog.Handler {
	if len(as) == 0 {
		return h
	}
	c := h.clone()
	last := &c.frames[len(c.frames)-1]
	last.attrs = append(append([]slog.Attr(nil), last.attrs...), as...)
	return c
}

func (h *logHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	c := h.clone()
	c.frames = append(c.frames, logFrame{group: name})
	return c
}

// writeAttrs writes slog attributes as JSON fields, groups as nested objects.
func writeAttrs(o *core.Object, as []slog.Attr) {
	for _, a := range as {
		v := a.Value.Resolve()
		if a.Equal(slog.Attr{}) {
			continue
		}
		if v.Kind() == slog.KindGroup {
			if a.Key == "" {
				writeAttrs(o, v.Group())
				continue
			}
			var inner core.Object
			writeAttrs(&inner, v.Group())
			o.Raw(a.Key, inner.Bytes())
			continue
		}
		o.Field(a.Key, attrValue(v))
	}
}

func attrValue(v slog.Value) any {
	switch v.Kind() {
	case slog.KindString:
		return v.String()
	case slog.KindInt64:
		return v.Int64()
	case slog.KindUint64:
		return v.Uint64()
	case slog.KindFloat64:
		return v.Float64()
	case slog.KindBool:
		return v.Bool()
	case slog.KindDuration:
		return v.Duration().String()
	case slog.KindTime:
		return v.Time().UTC().Format(time.RFC3339Nano)
	default:
		a := v.Any()
		if e, ok := a.(error); ok {
			return e.Error()
		}
		return a
	}
}

// ── trace ────────────────────────────────────────────────────────────────

// Span is an open span; see [WithSpan].
type Span struct {
	attrs core.Object
}

// Set adds attributes (key-value pairs, as slog); they go with the span's end record.
func (s *Span) Set(args ...any) {
	writeAttrs(&s.attrs, argsToAttrs(args))
}

func argsToAttrs(args []any) []slog.Attr {
	var r slog.Record
	r.Add(args...)
	var out []slog.Attr
	r.Attrs(func(a slog.Attr) bool {
		out = append(out, a)
		return true
	})
	return out
}

// spanID is the first 16 hex digits of sha256("<invocation>:<step>:<k>"): the same id in
// cairn and in an OTLP backend, independent of the attempt so that an end sent in a later
// attempt matches the start already sent.
func spanID(invocation string, id core.Ident) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s:%d:%d", invocation, id.Step, id.K))
	return hex.EncodeToString(sum[:])[:16]
}

// WithSpan runs fn inside a span of the invocation's trace: two records on the durable
// stream `$trace`, start{span_id, parent_id, name, ts, attrs} then end{…, status, error?}.
// Spans nest by call: a span opened inside fn is a child. If fn returns an error (or
// panics) the span ends with status "error". When the turn stops inside fn (it suspends,
// or the journal does not match) the span stays open: the next turn replays up to here
// and closes it with the same id. Handler goroutine only. args are key-value pairs, as
// slog.
func WithSpan[T any](ctx Context, name string, fn func(*Span) (T, error), args ...any) (T, error) {
	b := ctx.base()
	b.inv.Begin()
	var parent any // a JSON null when there is no parent
	if n := len(b.spans); n > 0 && b.spans[n-1] != "" {
		parent = b.spans[n-1]
	}
	var startAttrs core.Object
	writeAttrs(&startAttrs, argsToAttrs(args))
	invocation := b.InvocationID()
	ts := b.inv.Driver().Now()
	id, opened := b.inv.Driver().Append(core.Append{Stream: traceStream, Durable: true, Build: func(id core.Ident) []byte {
		var rec core.Object
		rec.Field("kind", "start")
		rec.Field("span_id", spanID(invocation, id))
		rec.Field("parent_id", parent)
		rec.Field("name", name)
		rec.Field("ts", ts)
		if len(args) > 0 {
			rec.Raw("attrs", startAttrs.Bytes())
		}
		return rec.Bytes()
	}})
	sid := ""
	if opened {
		sid = spanID(invocation, id)
	}
	// Below wire v4 (no identity) the span's children get its parent.
	scope := sid
	if scope == "" && parent != nil {
		scope = parent.(string)
	}
	b.spans = append(b.spans, scope)
	span := &Span{}
	end := func(status string, failure error) {
		if sid == "" {
			return
		}
		ts := b.inv.Driver().Now()
		b.append(nil, core.Append{Stream: traceStream, Durable: true, Build: func(core.Ident) []byte {
			var rec core.Object
			rec.Field("kind", "end")
			rec.Field("span_id", sid)
			rec.Field("parent_id", parent)
			rec.Field("name", name)
			rec.Field("ts", ts)
			rec.Field("status", status)
			if span.attrs.Len() > 0 {
				rec.Raw("attrs", span.attrs.Bytes())
			}
			if failure != nil {
				rec.Field("error", failure.Error())
			}
			return rec.Bytes()
		}})
	}
	finished := false
	defer func() {
		b.spans = b.spans[:len(b.spans)-1]
		if finished || b.inv.Exiting() {
			// Returned normally (ended below), or the turn stopped: leave it open.
			return
		}
		if r := recover(); r != nil {
			end("error", fmt.Errorf("panic: %v", r))
			panic(r)
		}
	}()
	v, err := fn(span)
	finished = true
	if err != nil {
		end("error", err)
	} else {
		end("ok", nil)
	}
	return v, err
}

// ── artifacts ────────────────────────────────────────────────────────────

// ArtifactRef is the ref of an artifact: what goes into the journal instead of the
// bytes. Its JSON is the same as the other SDKs'.
type ArtifactRef struct {
	SHA256      string `json:"sha256"`
	Size        int    `json:"size"`
	ContentType string `json:"contentType"`
}

// ArtifactMaxBytes caps one artifact: 16 MiB minus 1 KiB.
const ArtifactMaxBytes = core.ArtifactMaxBytes

// Artifacts keeps large bytes out of the journal (wire v4); see [Context.Artifacts].
type Artifacts struct {
	b     *ctxBase
	scope *core.Leaf
}

func (b *ctxBase) Artifacts() *Artifacts { return &Artifacts{b: b} }

// Put sends the bytes to the runtime (which stores them before processing the next
// command) and returns their ref, computed in place: return the ref from a [Run]
// closure, and the journal carries a few dozen bytes. Replay-aware: a point already in
// the journal does not resend. contentType defaults to application/octet-stream.
func (a *Artifacts) Put(content []byte, contentType string) (ArtifactRef, error) {
	if len(content) > ArtifactMaxBytes {
		return ArtifactRef{}, fmt.Errorf("cairn: artifact of %d bytes exceeds the limit of %d", len(content), ArtifactMaxBytes)
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	var scope core.Leaf
	if a.scope != nil {
		scope = *a.scope
	} else {
		s, ok := a.b.inv.Scope()
		if !ok || s.IsZero() {
			a.b.inv.Begin()
		}
		scope = s
	}
	sum := sha256.Sum256(content)
	content = append([]byte(nil), content...)
	if err := a.b.inv.Driver().PutArtifact(scope, sum, content, contentType); err != nil {
		return ArtifactRef{}, err
	}
	sha := hex.EncodeToString(sum[:])
	core.Artifacts.Remember(sha, content)
	return ArtifactRef{SHA256: sha, Size: len(content), ContentType: contentType}, nil
}

// Get returns an artifact's bytes: from this process's memory, otherwise downloaded
// from the runtime. On the handler goroutine prefer reading artifacts inside a [Run]
// closure (a download is I/O).
func (a *Artifacts) Get(sha256Hex string) ([]byte, error) {
	if b, ok := core.Artifacts.Get(sha256Hex); ok {
		return b, nil
	}
	if sha256Hex == "" {
		return nil, errors.New("cairn: empty artifact sha256")
	}
	ctx := context.Background()
	if a.scope == nil {
		if s, ok := a.b.inv.Scope(); !ok || s.IsZero() {
			a.b.inv.Begin()
		}
	}
	body, err := a.b.inv.Driver().FetchArtifact(ctx, sha256Hex)
	if err != nil {
		return nil, err
	}
	core.Artifacts.Remember(sha256Hex, body)
	return body, nil
}
