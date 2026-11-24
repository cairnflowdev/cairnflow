// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

// Package bidi is the SDK end of a BIDI_STREAM stream (docs/protocol-v1.md §4, "Turns on
// one stream"): the turn loop that keeps a local journal, runs the handler on it, and
// after a `Suspension` reads frame by frame until what the turn waits for has arrived and
// every proposal of the turn is acked, then reruns the handler from the start. It is the
// Go port of `runBidi` in the TypeScript SDK (sdk/typescript/src/handler.ts).
//
// The package knows nothing about HTTP/2: frames come in from an io.ReadCloser (the
// request body) and go out through a write function (the response body, flushed after
// every write). A turn is the same internal/turn machine REQUEST_RESPONSE uses, so both
// modes have one replay semantics.
package bidi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/turn"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/values"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

// ErrRejected is returned when a gate of Config refused the stream. Nothing was written:
// the transport closes the stream without a frame (docs/protocol-v1.md §4, "Request
// identity").
var ErrRejected = errors.New("cairn: the stream does not match its request identity")

// Config is what one stream needs.
type Config struct {
	// Turn configures every turn of the stream; RequestAck and Live are set by Serve.
	Turn turn.Config
	// CheckStart, when set, is asked about `Start` before anything else; false rejects the
	// stream (a jti that does not match the request identity).
	CheckStart func(*wire.Start) bool
	// CheckOpening, when set, is asked about the turn-opening body — `Start` and the
	// known_entries frames after it, as received — before the handler runs; false rejects
	// the stream (a body that does not match the signed hash).
	CheckOpening func(opening []byte) bool
	// MaxOpening caps the turn-opening body CheckOpening collects; past it the stream is
	// rejected.
	MaxOpening int
}

// stallWarning is how long a suspension may wait on a stream that is still open before
// it is suspicious: on a wait for the outside world the runtime closes the stream, so an
// open stream this long after a suspension means the runtime considers the suspension
// satisfied while the SDK does not.
const stallWarning = 60 * time.Second

var warnCustom sync.Once

// Serve runs the turn loop of one stream until it is over: the runtime closed the request
// body, the turn ended with something else than a suspension, or the stream broke. It
// closes in and returns only once every goroutine it started has ended and every frame
// went to write; write is not called after Serve returns.
func Serve(body core.Body, in io.ReadCloser, write func([]byte) error, cfg Config) error {
	ib := newInbox(in)
	out := newSink(write)
	defer func() {
		_ = in.Close()
		<-ib.done
		out.close()
	}()
	return loop(body, ib, out, cfg)
}

func loop(body core.Body, ib *inbox, out *sink, cfg Config) error {
	first, ok := ib.next()
	if !ok {
		return nil // the runtime gave up before saying anything
	}
	if first.Type != wire.TypeStart {
		if cfg.CheckStart != nil {
			return ErrRejected
		}
		out.put(turn.ProtocolViolation("the first message must be Start"))
		return nil
	}
	var start wire.Start
	if err := start.Unmarshal(first.Payload); err != nil {
		if cfg.CheckStart != nil {
			return ErrRejected
		}
		out.put(turn.ProtocolViolation("invalid Start: " + err.Error()))
		return nil
	}
	if cfg.CheckStart != nil && !cfg.CheckStart(&start) {
		return ErrRejected
	}

	// The local journal. The runtime does not send it again on later turns of the stream
	// (that is the saving), so the SDK keeps it: the replayed entries, the entries the SDK
	// emits, the notifications that come later.
	j := journal{values: cfg.Turn.Values, ctx: cfg.Turn.Context}
	var opening []byte
	if cfg.CheckOpening != nil {
		opening = first.AppendTo(nil)
	}
	// The turn-opening frames are held until the body hash is checked: admitting them
	// assembles segmented values, which fetches URLs the body names (wire v6).
	var held []wire.Frame
	for left := int64(start.KnownEntries); left > 0; {
		f, ok := ib.next()
		if !ok {
			if cfg.CheckOpening != nil {
				return ErrRejected
			}
			return nil // the stream broke during the replay
		}
		if cfg.CheckOpening != nil {
			opening = f.AppendTo(opening)
			if len(opening) > cfg.MaxOpening {
				return ErrRejected
			}
		}
		// A side-channel frame takes no slot among the replayed entries.
		if !f.Type.IsSideChannel() {
			left--
		}
		held = append(held, f)
	}
	if cfg.CheckOpening != nil && !cfg.CheckOpening(opening) {
		return ErrRejected
	}
	if j.values != nil {
		j.values.AddURLs(start.SegmentURLs)
	}
	for _, f := range append(held, ib.buffered()...) {
		if !j.take(f, out) {
			return nil
		}
	}

	tc := cfg.Turn
	tc.RequestAck = true
	live := &turn.Live{Write: out.put}
	if tc.Wire >= turn.KeepaliveVersion && start.KeepaliveIntervalMs != nil {
		live.Keepalive = time.Duration(*start.KeepaliveIntervalMs) * time.Millisecond
	}
	tc.Live = live
	for {
		t, input := turn.New(&start, j.entries, tc)
		// Frames arriving while the turn runs are read at once: a CANCEL must reach the
		// running closures without waiting for the turn to suspend.
		ib.current.Store(t)
		o := core.Execute(t, body, input)
		ib.current.Store(nil)
		out.put(t.Finish(o))
		for _, c := range t.Commands() {
			j.admit(c)
		}

		// A turn ends with exactly one of three (docs/protocol-v1.md §6); two of them end
		// the stream.
		tree, suspended := t.Suspension()
		if !suspended {
			return nil
		}
		// Suspended, and not closed: the runtime either pushes what the turn waits for, or
		// closes the stream because only the outside world can answer.
		if !wait(ib, &j, out, tree, t.Proposed()) {
			return nil
		}
	}
}

// wait reads frame by frame until the tree is satisfied and every proposal of the turn is
// durable (its ack arrived); false when the stream ended first, or a segmented value could
// not be assembled.
func wait(ib *inbox, j *journal, out *sink, tree turn.Tree, proposed map[uint32]wire.Frame) bool {
	stall := time.AfterFunc(stallWarning, func() {
		slog.Warn("cairn: a suspension has waited over a minute on a stream that is still open: the runtime may "+
			"consider it satisfied while the SDK does not (do the two suspension_satisfied implementations disagree?)",
			"code", "CAIRNFLOW_SUSPENSION_STALL")
	})
	defer stall.Stop()
	for {
		f, ok := ib.next()
		if !ok {
			return false
		}
		// An ack is not an entry: it is the receipt for an entry the turn built. Only now
		// does that `RunCompletion` join the local journal; from here a rerun may trust it,
		// because the runtime wrote it before acking. A runtime that sends the whole
		// `RunCompletion` instead goes through admit like any notification: both copies
		// have one completion id, a turn keeps the first.
		if f.Type == wire.TypeProposeRunCompletionAck {
			var a wire.ProposeRunCompletionAck
			if a.Unmarshal(f.Payload) == nil {
				if rc, ok := proposed[a.CompletionID]; ok {
					j.admit(rc)
				}
			}
		}
		if !j.take(f, out) {
			return false
		}
		if j.satisfies(tree) && j.durable(proposed) {
			return true
		}
	}
}

// journal is the local journal of a stream and what it holds, for the satisfied rule.
type journal struct {
	entries []wire.Frame
	done    map[uint32]bool
	idx     map[uint32]bool
	names   map[string]bool
	// values is the stream's segmented-value state (wire v6): the URL table of
	// `Start.segment_urls` and of every `SegmentUrls` frame, and the assembled copies.
	values *values.Session
	ctx    context.Context
}

// take admits a frame from the runtime after assembling the segmented value it carries
// (a `SegmentUrls` frame loads its URLs). When assembly fails it ends the turn with a
// transient error (retried) and returns false: the stream is over.
func (j *journal) take(f wire.Frame, out *sink) bool {
	if j.values != nil {
		ctx := j.ctx
		if ctx == nil {
			ctx = context.Background()
		}
		if err := turn.AdmitFrame(ctx, j.values, f); err != nil {
			out.put([]wire.Frame{turn.ValueFailure(err)})
			return false
		}
	}
	j.admit(f)
	return true
}

// admit takes a frame from the runtime, or a command the SDK emitted, into the local
// journal. Only commands and notifications are entries: acks, `Start`, side-channel and
// custom frames are not, and counting them would shift every later entry
// (docs/protocol-v1.md §9.2).
func (j *journal) admit(f wire.Frame) {
	switch {
	case f.Type.IsCommand():
		j.entries = append(j.entries, f)
	case f.Type == wire.TypeSignal:
		j.entries = append(j.entries, f)
		var s wire.SignalNotification
		if s.Unmarshal(f.Payload) != nil {
			return
		}
		if s.Signal.Idx != nil {
			set(&j.idx, *s.Signal.Idx)
		}
		if s.Signal.Name != nil {
			set(&j.names, *s.Signal.Name)
		}
	case f.Type.IsNotification():
		j.entries = append(j.entries, f)
		if m, err := wire.DecodeFrame(f); err == nil {
			if c, ok := m.(wire.Completion); ok {
				set(&j.done, c.CompletionID())
			}
		}
	case f.Type.IsCustom():
		warnCustom.Do(func() {
			slog.Warn("cairn: a custom message (0xFC00 and up) was dropped: this SDK has no API for them",
				"type", f.Type.String())
		})
	}
}

func set[K comparable](m *map[K]bool, k K) {
	if *m == nil {
		*m = map[K]bool{}
	}
	(*m)[k] = true
}

// satisfies is the runtime's rule (turn.Satisfied, the one conformance/
// suspension-vectors.json checks) over the local journal.
func (j *journal) satisfies(t turn.Tree) bool {
	return turn.Satisfied(t, func(c uint32) bool { return j.done[c] }, func(i uint32) bool { return j.idx[i] },
		func(n string) bool { return j.names[n] })
}

// durable reports whether every proposal has its `RunCompletion` in the local journal.
func (j *journal) durable(proposed map[uint32]wire.Frame) bool {
	for id := range proposed {
		if !j.done[id] {
			return false
		}
	}
	return true
}

// inbox reads the request body on its own goroutine, frame by frame, into a queue the
// loop takes from in order. It passes a CANCEL signal to the running turn at once.
type inbox struct {
	in      io.Reader
	mu      sync.Mutex
	queue   []wire.Frame
	ended   bool
	signal  chan struct{}
	done    chan struct{}
	current atomic.Pointer[turn.Turn]
}

func newInbox(in io.Reader) *inbox {
	ib := &inbox{in: in, signal: make(chan struct{}, 1), done: make(chan struct{})}
	go ib.read()
	return ib
}

func (ib *inbox) read() {
	defer close(ib.done)
	fr := wire.NewFrameReader(ib.in)
	for {
		// Any error ends the stream: the end of the body, a broken connection, or a byte
		// stream where no frame boundary can be found again.
		f, err := fr.Next()
		if err != nil {
			ib.mu.Lock()
			ib.ended = true
			ib.mu.Unlock()
			ib.wake()
			return
		}
		if t := ib.current.Load(); t != nil && isCancel(f) {
			t.Cancel()
		}
		ib.mu.Lock()
		ib.queue = append(ib.queue, f)
		ib.mu.Unlock()
		ib.wake()
	}
}

func (ib *inbox) wake() {
	select {
	case ib.signal <- struct{}{}:
	default:
	}
}

// next returns the next frame, waiting for it; false once the body has ended.
func (ib *inbox) next() (wire.Frame, bool) {
	for {
		ib.mu.Lock()
		if len(ib.queue) > 0 {
			f := ib.queue[0]
			ib.queue = ib.queue[1:]
			ib.mu.Unlock()
			return f, true
		}
		if ib.ended {
			ib.mu.Unlock()
			return wire.Frame{}, false
		}
		ib.mu.Unlock()
		<-ib.signal
	}
}

// buffered takes every frame that has arrived, without waiting.
func (ib *inbox) buffered() []wire.Frame {
	ib.mu.Lock()
	defer ib.mu.Unlock()
	out := ib.queue
	ib.queue = nil
	return out
}

func isCancel(f wire.Frame) bool {
	if f.Type != wire.TypeSignal {
		return false
	}
	var s wire.SignalNotification
	return s.Unmarshal(f.Payload) == nil && s.Signal.Idx != nil && *s.Signal.Idx == wire.SignalCancel
}

// sink writes frames on its own goroutine, in the order they were put, so that no writer
// ever blocks on the network (a turn's handler goroutine, its live loop). After a failed
// write the rest is dropped: the stream is gone, and the reader sees it end.
type sink struct {
	write  func([]byte) error
	mu     sync.Mutex
	queue  []byte
	closed bool
	signal chan struct{}
	done   chan struct{}
}

func newSink(write func([]byte) error) *sink {
	s := &sink{write: write, signal: make(chan struct{}, 1), done: make(chan struct{})}
	go s.run()
	return s
}

// put queues frames; it never blocks.
func (s *sink) put(frames []wire.Frame) {
	if len(frames) == 0 {
		return
	}
	s.mu.Lock()
	for _, f := range frames {
		s.queue = f.AppendTo(s.queue)
	}
	s.mu.Unlock()
	select {
	case s.signal <- struct{}{}:
	default:
	}
}

func (s *sink) run() {
	defer close(s.done)
	failed := false
	for {
		s.mu.Lock()
		b, closed := s.queue, s.closed
		s.queue = nil
		s.mu.Unlock()
		if len(b) > 0 && !failed {
			failed = s.write(b) != nil
		}
		if len(b) == 0 {
			if closed {
				return
			}
			<-s.signal
		}
	}
}

// close writes what is queued, then stops the writer and waits for it.
func (s *sink) close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	select {
	case s.signal <- struct{}{}:
	default:
	}
	<-s.done
}
