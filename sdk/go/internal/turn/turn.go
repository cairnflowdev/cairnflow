// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

// Package turn is the SDK side of the protocol's state machine (docs/protocol-v1.md
// sections 6 and 8): one [Turn] is one run of the handler from the start over the journal
// the runtime sent, each command matching the next journal entry, results delivered to
// user code in journal order. It is the Go port of `Turn` in the TypeScript SDK
// (sdk/typescript/src/handler.ts); the conformance traces check the ports against each
// other byte for byte.
//
// A Turn implements [core.Driver]; the execution core (internal/core) runs the handler
// goroutine and calls it. User code cannot tell the replayed part of the journal from the
// new part: commands already in the journal are matched instead of emitted, and results
// come from the journal instead of from the outside world.
package turn

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/values"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

// Wire versions that change what a turn may do.
const (
	// FlowControlVersion is the first wire version with `scope` and `limit_key`.
	FlowControlVersion = 2
	// KeepaliveVersion is the first wire version with keepalives, the replay report and
	// continue-as-new.
	KeepaliveVersion = 3
	// StreamsVersion is the first wire version with the `StreamAppend` and `ArtifactPut`
	// side channel.
	StreamsVersion = 4
)

// Input headers the SDK reads.
const (
	logLevelHeader = "cairnflow-log-level"
	scheduleHeader = "cairnflow-schedule"
)

// streamBatchBytes flushes the batched stream records into a frame once they reach this
// many payload bytes (the same batching as the other SDKs).
const streamBatchBytes = 1024

// Config is what a turn needs from the transport.
type Config struct {
	// Wire is the settled wire version (the runtime's, clamped to the SDK's ceiling).
	Wire int
	// Now is the SDK's wall clock in Unix milliseconds; nil means the system clock.
	Now func() int64
	// LogLevel is the handler's own minimum durable log level ("" = info); the
	// invocation's `cairnflow-log-level` header overrides it.
	LogLevel string
	// Context is the parent of every `ctx.run` closure's context (the request's lifetime).
	Context context.Context
	// RequestAck sets REQUESTED_ACK on `ProposeRunCompletion` (BIDI_STREAM only;
	// REQUEST_RESPONSE must not, docs/protocol-v1.md section 4).
	RequestAck bool
	// FetchArtifact downloads an artifact of the invocation that is not in memory; nil
	// means downloading is not configured.
	FetchArtifact func(ctx context.Context, invocationID, sha256 string) ([]byte, error)
	// Live is the mid-turn write path of a BIDI_STREAM turn (keepalives, stream records,
	// the commands ahead of them); nil in REQUEST_RESPONSE, where every frame goes in the
	// response at the end of the turn.
	Live *Live
	// Values holds the assembled copies of the segmented values of the entries (wire v6),
	// admitted before the turn; nil when nothing is assembled (a segmented value read then
	// is a transient error, never its tail).
	Values *values.Session
}

// stored is a result in the journal.
type stored struct {
	res core.Result
	// err is set for a result the SDK cannot read (a segmented value it cannot assemble).
	err error
	// segmented is the assembled segmented value the result came from (wire v6).
	segmented *values.Assembled
}

// stateRead is a lazy state read waiting to be read by user code: the key, and the
// writes of that key at the time of the read (see Turn.known).
type stateRead struct {
	key   string
	stamp writeStamp
}

// writeStamp counts the writes of one key and the ClearAllState commands.
type writeStamp struct{ writes, clears int }

// outFrame is a frame of the turn's response with the delivery epoch it was produced in.
type outFrame struct {
	frame wire.Frame
	epoch int
}

// proposal is the outcome of a `ctx.run` closure, placed after the commands of the epoch
// its `Run` was matched in (see Finish).
type proposal struct {
	id     uint32
	epoch  int
	frames []wire.Frame // ArtifactPut frames of the closure, then the proposal (if any)
}

// finished is a closure that returned, waiting to be processed on the handler goroutine.
type finished struct {
	id      uint32
	outcome core.RunOutcome
}

// mismatchAt describes a journal mismatch for the replay report.
type mismatchAt struct {
	at            int
	expected, got string
}

// pendingStream is the batch of one stream not yet put into a frame.
type pendingStream struct {
	name    string
	durable bool
	records []wire.StreamRecord
	close   bool
}

type streamKey struct {
	step uint32
	name string
}

// Turn is one run of the handler. Everything except the fields under mu and wmu belongs to
// the handler goroutine (or to the transport before and after core.Execute). The handler
// goroutine writes the fields under wmu with wmu held and may read them without it.
type Turn struct {
	cfg  Config
	info core.Info
	now  func() int64

	// journaled holds the stored command entries, not counting the runtime's Input.
	journaled []wire.Frame
	results   map[core.Leaf]stored
	// positions is the journal position of each leaf's first appearance: completions and
	// signals on one sequence, so a race picks its winner in exactly the recorded order.
	positions map[core.Leaf]int
	// deliveries is the delivery schedule: leaves in journal order, one list per position.
	deliveries   [][]core.Leaf
	nextDelivery int
	delivered    map[core.Leaf]int
	// cancelled: a CANCEL signal is in the journal, or arrived mid-turn (Cancel).
	cancelled atomic.Bool

	// state is the eager state snapshot; nil when the runtime sent none.
	state   map[string][]byte
	partial bool

	// known is, per key, the segmented value user code most recently read in this turn
	// (wire v6): SetState reuses the refs of a byte-identical prefix. Any other read of the
	// key (inline, absent, a journaled GetEagerState) or any write of it replaces or deletes
	// the entry. reads are the lazy reads not read yet; writes and clears stamp them, so a
	// read only counts when no write of its key came between the command and the result.
	known  map[string]*values.Assembled
	reads  map[core.Leaf]stateRead
	writes map[string]int
	clears int

	cursor  int
	nextID  uint32
	nextAwk uint32

	// wmu guards what the write path reads: the frames of the turn in their order, and
	// where the handler goroutine stands (see take).
	wmu sync.Mutex
	// epoch counts the times user code resumed from a wait.
	epoch int
	out   []outFrame
	// proposals not on the wire yet.
	proposals []proposal
	// undrained counts, by epoch, the closures started in it whose outcome is not
	// processed yet.
	undrained map[int]int
	// parked: the handler goroutine is blocked in Await, so its epoch gets no new command.
	parked bool
	// ended: the handler goroutine is over (Finish), every epoch is closed.
	ended bool
	// wOut is the next frame of out to write, wEpoch the epoch being written.
	wOut, wEpoch int
	// liveClosed: the turn's last frames went to the transport; the live path is off.
	liveClosed bool

	// ownRuns are the closures started in this turn, by leaf, with their epoch.
	ownRuns      map[core.Leaf]int
	pendingRuns  map[uint32]wire.Frame
	cancelledRun map[core.Leaf]bool

	mismatch    string
	mismatchAt  *mismatchAt
	failedWith  *core.Failure
	suspendedOn *Tree
	replayOnly  bool
	replayStop  int // the position of the first command outside the journal, -1 = none
	// endFrames holds the frames that ended the turn (Finish).
	endFrames []wire.Frame

	mu      sync.Mutex
	wake    chan struct{}
	running int
	done    []finished
	runStep map[core.Leaf]uint32
	// closurePuts are the ArtifactPut frames of running closures, by scope.
	closurePuts  map[core.Leaf][]wire.Frame
	streams      []*pendingStream
	streamBytes  int
	streamFrames []wire.Frame
	streamK      map[streamKey]uint32
	// cancels abort the running cancellable closures, by scope.
	cancels map[core.Leaf]context.CancelFunc
	// streamWanted: a live write of stream records waits for the handler goroutine to
	// process a closure that returned (so its proposal goes first).
	streamWanted bool

	// The live loop of a BIDI_STREAM turn (live.go).
	kick     chan struct{}
	liveStop chan struct{}
	liveDone chan struct{}
}

// New builds a turn from the Start message and the entries that follow it (the replayed
// journal and the notifications of the turn). It returns the input bytes of the Input
// entry.
func New(start *wire.Start, entries []wire.Frame, cfg Config) (*Turn, []byte) {
	t := &Turn{
		cfg:          cfg,
		now:          cfg.Now,
		results:      map[core.Leaf]stored{},
		positions:    map[core.Leaf]int{},
		delivered:    map[core.Leaf]int{},
		nextID:       1,
		nextAwk:      17,
		ownRuns:      map[core.Leaf]int{},
		pendingRuns:  map[uint32]wire.Frame{},
		cancelledRun: map[core.Leaf]bool{},
		replayStop:   -1,
		wake:         make(chan struct{}, 1),
		runStep:      map[core.Leaf]uint32{},
		closurePuts:  map[core.Leaf][]wire.Frame{},
		streamK:      map[streamKey]uint32{},
		undrained:    map[int]int{},
		cancels:      map[core.Leaf]context.CancelFunc{},
		known:        map[string]*values.Assembled{},
		reads:        map[core.Leaf]stateRead{},
		writes:       map[string]int{},
	}
	if t.now == nil {
		t.now = func() int64 { return time.Now().UnixMilli() }
	}
	if t.cfg.Context == nil {
		t.cfg.Context = context.Background()
	}
	t.replayOnly = cfg.Wire >= KeepaliveVersion && start.ReplayOnly
	t.info = core.Info{
		ID:          start.ID,
		DebugID:     start.DebugID,
		Key:         start.Key,
		Attempt:     start.RetryCountSinceLastStoredEntry,
		RandomSeed:  start.RandomSeed,
		WireVersion: cfg.Wire,
		LogLevel:    cfg.LogLevel,
	}
	if start.IdempotencyKey != nil {
		t.info.IdempotencyKey, t.info.HasIdempotencyKey = *start.IdempotencyKey, true
	}
	if len(start.StateMap) > 0 || start.PartialState {
		t.state = make(map[string][]byte, len(start.StateMap))
		for _, e := range start.StateMap {
			t.state[string(e.Key)] = e.Value
		}
		t.partial = start.PartialState
	}

	var input []byte
	seenInput := false
	last := -1
	for at, e := range entries {
		switch {
		case e.Type == wire.TypeInput:
			if seenInput {
				continue
			}
			seenInput = true
			var in wire.Input
			if in.Unmarshal(e.Payload) == nil {
				if in.Value != nil {
					input = in.Value.Content
				}
				t.readHeaders(in.Headers)
			}
		case e.Type == wire.TypeSignal:
			var s wire.SignalNotification
			if s.Unmarshal(e.Payload) != nil {
				continue
			}
			res := t.result(&s.Result, e)
			if s.Signal.Idx != nil {
				l := core.Leaf{Kind: core.SignalIndexLeaf, ID: *s.Signal.Idx}
				last = t.seen(l, res, at, last)
				if *s.Signal.Idx == wire.SignalCancel {
					t.cancelled.Store(true)
				}
			}
			if s.Signal.Name != nil {
				last = t.seen(core.Leaf{Kind: core.SignalNameLeaf, Name: *s.Signal.Name}, res, at, last)
			}
		case e.Type.IsCommand():
			t.journaled = append(t.journaled, e)
		case e.Type.IsNotification():
			m, err := wire.DecodeFrame(e)
			if err != nil {
				continue
			}
			c, ok := m.(wire.Completion)
			if !ok {
				continue
			}
			last = t.seen(core.Leaf{Kind: core.CompletionLeaf, ID: c.CompletionID()}, t.notificationResult(m, e), at, last)
		}
	}
	if cfg.Live != nil {
		t.startLive()
	}
	return t, input
}

func (t *Turn) readHeaders(hs []wire.Header) {
	for _, h := range hs {
		switch h.Key {
		case logLevelHeader:
			switch h.Value {
			case "debug", "info", "warn", "error":
				t.info.LogLevel = h.Value
			}
		case scheduleHeader:
			i := strings.LastIndexByte(h.Value, '@')
			if i <= 0 {
				continue
			}
			ms, err := strconv.ParseInt(h.Value[i+1:], 10, 64)
			if err != nil {
				continue
			}
			t.info.Schedule = &core.Schedule{Name: h.Value[:i], AtMs: ms}
		}
	}
}

// seen records the first appearance of a leaf at journal position at; leaves at the same
// position are delivered together.
func (t *Turn) seen(l core.Leaf, r stored, at, last int) int {
	if _, ok := t.positions[l]; ok {
		return last
	}
	t.positions[l] = at
	t.results[l] = r
	if at == last && len(t.deliveries) > 0 {
		t.deliveries[len(t.deliveries)-1] = append(t.deliveries[len(t.deliveries)-1], l)
	} else {
		t.deliveries = append(t.deliveries, []core.Leaf{l})
	}
	return at
}

// value turns the wire value of frame f into its bytes: a segmented value is read from its
// assembled copy (wire v6), never from its tail.
func (t *Turn) value(v *wire.Value, f wire.Frame) ([]byte, *values.Assembled, error) {
	if v.IsSegmented() {
		if t.cfg.Values != nil {
			if a, ok := t.cfg.Values.Of(f); ok {
				return a.Bytes, a, nil
			}
		}
		return nil, nil, &values.Error{Message: values.ErrNotAssembled}
	}
	if v.Content == nil {
		return []byte{}, nil, nil
	}
	return v.Content, nil, nil
}

// result reads the result oneof of a notification (frame f).
func (t *Turn) result(r *wire.Result, f wire.Frame) stored {
	switch {
	case r.Failure != nil:
		return stored{res: core.Result{Kind: core.FailureResult, Code: r.Failure.Code, Message: r.Failure.Message}}
	case r.Value != nil:
		b, a, err := t.value(r.Value, f)
		return stored{res: core.Result{Kind: core.ValueResult, Value: b}, err: err, segmented: a}
	default:
		return stored{res: core.Result{Kind: core.VoidResult}}
	}
}

func (t *Turn) notificationResult(m wire.Message, f wire.Frame) stored {
	switch n := m.(type) {
	case *wire.GetStateCompletion:
		return t.result(&n.Result, f)
	case *wire.GetStateKeysCompletion:
		r := core.Result{Kind: core.StateKeysResult, Keys: []string{}}
		if n.StateKeys != nil {
			for _, k := range n.StateKeys.Keys {
				r.Keys = append(r.Keys, strings.ToValidUTF8(string(k), "�"))
			}
		}
		return stored{res: r}
	case *wire.GetPromiseCompletion:
		return t.result(&n.Result, f)
	case *wire.PeekPromiseCompletion:
		return t.result(&n.Result, f)
	case *wire.CompletePromiseCompletion:
		return t.result(&n.Result, f)
	case *wire.SleepCompletion:
		return stored{res: core.Result{Kind: core.VoidResult}}
	case *wire.CallCompletion:
		return t.result(&n.Result, f)
	case *wire.CallInvocationIDCompletion:
		return stored{res: core.Result{Kind: core.ValueResult, Value: []byte(n.InvocationID)}}
	case *wire.RunCompletion:
		return t.result(&n.Result, f)
	case *wire.AttachInvocationCompletion:
		return t.result(&n.Result, f)
	case *wire.GetInvocationOutputCompletion:
		return t.result(&n.Result, f)
	default:
		return stored{res: core.Result{Kind: core.VoidResult}}
	}
}

// ── core.Driver: identity and clock ─────────────────────────────────────

// Info describes the invocation.
func (t *Turn) Info() core.Info { return t.info }

// Now is the SDK's wall clock.
func (t *Turn) Now() int64 { return t.now() }

// ── commands ──────────────────────────────────────────────────────────────

func (t *Turn) alloc() uint32 {
	n := t.nextID
	t.nextID++
	return n
}

// AllocAwakeable allocates the signal index of the next awakeable (1-16 are built-in).
func (t *Turn) AllocAwakeable() uint32 {
	n := t.nextAwk
	t.nextAwk++
	return n
}

// peek is the type of the entry journaled at the next position (0, false at the end).
func (t *Turn) peek() (wire.MessageType, bool) {
	if t.cursor < len(t.journaled) {
		return t.journaled[t.cursor].Type, true
	}
	return 0, false
}

// replaying reports whether the next command is already in the journal.
func (t *Turn) replaying() bool {
	return t.replayOnly || t.cursor < len(t.journaled)
}

// dead reports whether the turn already has an outcome: code still running on the
// handler goroutine is a ghost (it can only be inside a user defer).
func (t *Turn) dead() bool {
	return t.suspendedOn != nil || t.failedWith != nil || t.mismatch != "" || t.replayStop >= 0
}

// stop is the Stop that ends the turn once it has an outcome.
func (t *Turn) stop() error {
	switch {
	case t.mismatch != "":
		return &core.Stop{Reason: core.StopMismatch}
	case t.failedWith != nil:
		return &core.Stop{Reason: core.StopFailed, Cause: t.failedWith}
	case t.replayStop >= 0:
		return &core.Stop{Reason: core.StopReplayed}
	case t.suspendedOn != nil:
		return &core.Stop{Reason: core.StopSuspended}
	}
	return nil
}

func jsString(s string) string {
	b, err := core.Marshal(s)
	if err != nil {
		return strconv.Quote(s)
	}
	return string(b)
}

func (t *Turn) diverged(at int, expected, got, message string) error {
	t.mismatchAt = &mismatchAt{at: at, expected: expected, got: got}
	t.mismatch = message
	return &core.Stop{Reason: core.StopMismatch}
}

// command is one command of user code. It returns the journaled frame when the command is
// already in the journal (and matches it on every field), nil when it was just emitted.
// build is called only when needed. checkName compares the names first (a renamed step is a
// different step, docs/protocol-v1.md section 6).
func (t *Turn) command(ty wire.MessageType, build func() wire.Message, checkName *string) (*wire.Frame, error) {
	if err := t.stop(); err != nil {
		return nil, err
	}
	at := t.cursor
	t.cursor++
	if at < len(t.journaled) {
		j := &t.journaled[at]
		if j.Type != ty {
			return nil, t.diverged(at, j.Type.UpperName(), ty.UpperName(),
				fmt.Sprintf("entry %d: journal has %s, current code produced %s", at, j.Type.UpperName(), ty.UpperName()))
		}
		if checkName != nil {
			if was := wire.EntryNameOf(j.Payload); was != *checkName {
				expected := ty.UpperName() + " " + jsString(was)
				got := ty.UpperName() + " " + jsString(*checkName)
				return nil, t.diverged(at, expected, got,
					fmt.Sprintf("entry %d: journal has %s, current code produced %s", at, expected, got))
			}
		}
		current := wire.Marshal(build())
		if f, differs := wire.CommandDiff(ty, j.Payload, current); differs {
			expected := ty.UpperName() + " " + jsString(wire.EntryNameOf(j.Payload))
			got := ty.UpperName() + " " + jsString(wire.EntryNameOf(current))
			return nil, t.diverged(at, expected, got,
				fmt.Sprintf("entry %d: journal has %s, current code produced %s (field %d differs)", at, expected, got, f))
		}
		return j, nil
	}
	// A trial replay stops before the new command is built, and before any closure.
	if t.replayOnly {
		t.replayStop = at
		return nil, &core.Stop{Reason: core.StopReplayed}
	}
	payload := wire.Marshal(build())
	if len(payload) > wire.MaxFrameLen {
		return nil, frameTooLarge(ty.String(), len(payload))
	}
	t.emit(wire.Frame{Type: ty, Payload: payload})
	return nil, nil
}

func (t *Turn) emit(f wire.Frame) {
	t.wmu.Lock()
	t.out = append(t.out, outFrame{frame: f, epoch: t.epoch})
	t.wmu.Unlock()
}

func frameTooLarge(what string, size int) *core.Failure {
	return &core.Failure{Kind: core.TerminalFailure, Code: 413, Message: fmt.Sprintf(
		"%s of %d bytes exceeds the %d-byte frame limit; keep large data out of the journal (artifacts)",
		what, size, wire.MaxFrameLen)}
}

func completion(id uint32) core.Leaf { return core.Leaf{Kind: core.CompletionLeaf, ID: id} }

func inline(b []byte) *wire.Value { return &wire.Value{Content: b} }

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// GetState emits `GetState`, or `GetEagerState` when the journal has one here or, for a
// new command, when the eager snapshot answers the key. The journal decides, not the
// current capabilities: an invocation that lived through an upgrade still replays.
func (t *Turn) GetState(key string) (core.Ticket, error) {
	journaled, has := t.peek()
	eager := (has && journaled == wire.TypeGetEagerState) || (!has && t.state != nil && (!t.partial || t.hasState(key)))
	if eager {
		snapshot, found := t.state[key]
		j, err := t.command(wire.TypeGetEagerState, func() wire.Message {
			m := &wire.GetEagerState{Key: []byte(key), Name: key}
			if found {
				m.Value = inline(snapshot)
			} else {
				m.Void = true
			}
			return m
		}, nil)
		if err != nil {
			return core.Ticket{}, err
		}
		if j == nil {
			if found {
				return core.Ticket{Known: core.Result{Kind: core.ValueResult, Value: snapshot}}, nil
			}
			return core.Ticket{Known: core.Result{Kind: core.VoidResult}}, nil
		}
		// A journaled GetEagerState is a read with no reusable refs: the runtime seals that
		// entry itself (from the SDK's snapshot), so the part past the prefix matching the
		// state row is new segments held by the invocation, not by the row, and a SetState
		// reusing them would be refused on every retry. A read answered from the snapshot
		// in place (above) keeps what is known: that value is the latest SetState's.
		t.forget(key)
		var m wire.GetEagerState
		if err := m.Unmarshal(j.Payload); err != nil {
			return core.Ticket{}, err
		}
		if m.Value == nil {
			return core.Ticket{Known: core.Result{Kind: core.VoidResult}}, nil
		}
		b, _, err := t.value(m.Value, *j)
		if err != nil {
			return core.Ticket{}, err
		}
		return core.Ticket{Known: core.Result{Kind: core.ValueResult, Value: b}}, nil
	}
	id := t.alloc()
	_, err := t.command(wire.TypeGetState, func() wire.Message {
		return &wire.GetState{Key: []byte(key), ResultCompletionID: id, Name: key}
	}, nil)
	if err == nil {
		t.reads[completion(id)] = stateRead{key: key, stamp: t.stamp(key)}
	}
	return core.Ticket{Leaf: completion(id)}, err
}

// stamp is the write stamp of key now.
func (t *Turn) stamp(key string) writeStamp {
	return writeStamp{writes: t.writes[key], clears: t.clears}
}

// remember records what user code read for key: a segmented value keeps its refs for the
// next SetState of the key; anything else forgets them. A read whose key was written
// between the command and the result does not count (its refs are those of an
// overwritten value).
func (t *Turn) remember(r stateRead, s stored) {
	if r.stamp != t.stamp(r.key) {
		return
	}
	switch {
	case s.err != nil || s.res.Kind == core.FailureResult:
		// Nothing was read.
	case s.segmented != nil:
		t.known[r.key] = s.segmented
	default:
		t.forget(r.key)
	}
}

func (t *Turn) forget(key string) { delete(t.known, key) }

// segmentedFor is the value to send for a write-once value b (`Output`, a `ctx.run`
// result): at wire v6 it reuses the refs of the longest prefix of b matching a segmented
// value assembled for this invocation.
func (t *Turn) segmentedFor(b []byte) *wire.Value {
	if t.cfg.Wire < values.WireSegmented || t.cfg.Values == nil {
		return inline(b)
	}
	return values.Segmented(b, t.cfg.Values.ReuseFor(b))
}

func (t *Turn) hasState(key string) bool {
	_, ok := t.state[key]
	return ok
}

// SetState emits `SetState`. At wire v6, when v starts with the bytes of the segments
// user code just read for this key, the command carries the refs of that prefix and only
// the rest: the whole value is not sent again every turn.
func (t *Turn) SetState(key string, v []byte) error {
	var reuse []wire.SegmentRef
	if known := t.known[key]; known != nil && t.cfg.Wire >= values.WireSegmented {
		reuse = known.Refs[:values.ReusablePrefix(known, v)]
	}
	j, err := t.command(wire.TypeSetState, func() wire.Message {
		return &wire.SetState{Key: []byte(key), Value: values.Segmented(v, reuse), Name: key}
	}, nil)
	if err != nil {
		return err
	}
	t.writes[key]++
	// Replayed: which refs the earlier run sent is unknown, so nothing is assumed (the next
	// SetState goes inline). Emitted: once committed, the row starts with exactly the refs
	// just sent. The bytes are copied: user code may reuse its buffer.
	if j == nil && len(reuse) > 0 {
		t.known[key] = &values.Assembled{Bytes: slices.Clone(v), Refs: reuse}
	} else {
		t.forget(key)
	}
	if t.state != nil {
		t.state[key] = v
	}
	return nil
}

// ClearState emits `ClearState`.
func (t *Turn) ClearState(key string) error {
	if _, err := t.command(wire.TypeClearState, func() wire.Message {
		return &wire.ClearState{Key: []byte(key), Name: key}
	}, nil); err != nil {
		return err
	}
	t.writes[key]++
	t.forget(key)
	if t.state != nil {
		delete(t.state, key)
	}
	return nil
}

// ClearAllState emits `ClearAllState`.
func (t *Turn) ClearAllState() error {
	if _, err := t.command(wire.TypeClearAllState, func() wire.Message {
		return &wire.ClearAllState{Name: "clearAll"}
	}, nil); err != nil {
		return err
	}
	t.clears++
	clear(t.known)
	// After deleting everything the snapshot is complete, and empty.
	t.state = map[string][]byte{}
	t.partial = false
	return nil
}

// StateKeys emits `GetStateKeys`, or `GetEagerStateKeys` when the journal has one here
// or, for a new command, when the snapshot is complete (keys sorted as the other SDKs do).
func (t *Turn) StateKeys() (core.Ticket, error) {
	journaled, has := t.peek()
	eager := (has && journaled == wire.TypeGetEagerStateKeys) || (!has && t.state != nil && !t.partial)
	if eager {
		keys := make([]string, 0, len(t.state))
		for k := range t.state {
			keys = append(keys, k)
		}
		slices.SortFunc(keys, compareUTF16)
		j, err := t.command(wire.TypeGetEagerStateKeys, func() wire.Message {
			sk := &wire.StateKeys{Keys: [][]byte{}}
			for _, k := range keys {
				sk.Keys = append(sk.Keys, []byte(k))
			}
			return &wire.GetEagerStateKeys{Name: "getKeys", Value: sk}
		}, nil)
		if err != nil {
			return core.Ticket{}, err
		}
		if j != nil {
			var m wire.GetEagerStateKeys
			if err := m.Unmarshal(j.Payload); err != nil {
				return core.Ticket{}, err
			}
			keys = []string{}
			if m.Value != nil {
				for _, k := range m.Value.Keys {
					keys = append(keys, strings.ToValidUTF8(string(k), "�"))
				}
			}
		}
		return core.Ticket{Known: core.Result{Kind: core.StateKeysResult, Keys: keys}}, nil
	}
	id := t.alloc()
	_, err := t.command(wire.TypeGetStateKeys, func() wire.Message {
		return &wire.GetStateKeys{ResultCompletionID: id, Name: "getKeys"}
	}, nil)
	return core.Ticket{Leaf: completion(id)}, err
}

// Sleep emits `Sleep` with an absolute wake-up time (docs/protocol-v1.md section 8.4).
func (t *Turn) Sleep(d time.Duration, name string) (core.Ticket, error) {
	id := t.alloc()
	_, err := t.command(wire.TypeSleep, func() wire.Message {
		return &wire.Sleep{WakeUpTime: uint64(max(0, t.now()+d.Milliseconds())), ResultCompletionID: id, Name: name}
	}, nil)
	return core.Ticket{Leaf: completion(id)}, err
}

func tags(c core.CallSpec) []wire.Header {
	if len(c.Tags) == 0 {
		return nil
	}
	out := make([]wire.Header, len(c.Tags))
	for i, tg := range c.Tags {
		out[i] = wire.Header{Key: tg.Key, Value: tg.Value}
	}
	return out
}

func placement(c core.CallSpec) wire.CallPlacement {
	if c.Spread {
		return wire.PlacementSpread
	}
	return wire.PlacementDefault
}

func idempotency(c core.CallSpec) *string {
	if !c.HasIdempotencyKey {
		return nil
	}
	k := c.IdempotencyKey
	return &k
}

// Call emits `Call`: the first id gets the callee's invocation id, the second its result.
func (t *Turn) Call(c core.CallSpec) (core.Ticket, error) {
	idc := t.alloc()
	res := t.alloc()
	_, err := t.command(wire.TypeCall, func() wire.Message {
		return &wire.Call{
			ServiceName:                 c.Service,
			HandlerName:                 c.Handler,
			Parameter:                   c.Input,
			Key:                         c.Key,
			IdempotencyKey:              idempotency(c),
			Scope:                       optional(c.Scope),
			LimitKey:                    optional(c.LimitKey),
			InvocationIDNotificationIdx: idc,
			ResultCompletionID:          res,
			Name:                        c.Name,
			Placement:                   placement(c),
			Tags:                        tags(c),
		}
	}, nil)
	return core.Ticket{Leaf: completion(res)}, err
}

// Send emits `OneWayCall`; a delay is turned into an absolute time once.
func (t *Turn) Send(c core.CallSpec, delay time.Duration) error {
	idc := t.alloc()
	_, err := t.command(wire.TypeOneWayCall, func() wire.Message {
		var at uint64
		if delay > 0 {
			ms := (delay + time.Millisecond - 1) / time.Millisecond
			at = uint64(max(0, t.now()+int64(ms)))
		}
		return &wire.OneWayCall{
			ServiceName:                 c.Service,
			HandlerName:                 c.Handler,
			Parameter:                   c.Input,
			InvokeTime:                  at,
			Key:                         c.Key,
			IdempotencyKey:              idempotency(c),
			Scope:                       optional(c.Scope),
			LimitKey:                    optional(c.LimitKey),
			InvocationIDNotificationIdx: idc,
			Name:                        c.Name,
			Placement:                   placement(c),
			Tags:                        tags(c),
		}
	}, nil)
	return err
}

// SendSignal emits `SendSignal`.
func (t *Turn) SendSignal(target string, signal core.SignalID, v []byte) error {
	_, err := t.command(wire.TypeSendSignal, func() wire.Message {
		m := &wire.SendSignal{TargetInvocationID: target, Name: "signal/" + signal.String()}
		if signal.ByIndex {
			m.Signal = wire.SignalIdx(signal.Index)
		} else {
			m.Signal = wire.SignalName(signal.Name)
		}
		if v == nil {
			m.Void = true
		} else {
			m.Value = inline(v)
		}
		return m
	}, nil)
	return err
}

// GetPromise emits `GetPromise`.
func (t *Turn) GetPromise(name string) (core.Ticket, error) {
	id := t.alloc()
	_, err := t.command(wire.TypeGetPromise, func() wire.Message {
		return &wire.GetPromise{Key: name, ResultCompletionID: id, Name: "promise/" + name}
	}, nil)
	return core.Ticket{Leaf: completion(id)}, err
}

// PeekPromise emits `PeekPromise`.
func (t *Turn) PeekPromise(name string) (core.Ticket, error) {
	id := t.alloc()
	_, err := t.command(wire.TypePeekPromise, func() wire.Message {
		return &wire.PeekPromise{Key: name, ResultCompletionID: id, Name: "promise/" + name}
	}, nil)
	return core.Ticket{Leaf: completion(id)}, err
}

// CompletePromise emits `CompletePromise` with a value or a failure.
func (t *Turn) CompletePromise(name string, v []byte, failure *core.Failure) (core.Ticket, error) {
	id := t.alloc()
	_, err := t.command(wire.TypeCompletePromise, func() wire.Message {
		m := &wire.CompletePromise{Key: name, ResultCompletionID: id, Name: "promise/" + name}
		if failure != nil {
			m.CompletionFailure = &wire.Failure{Code: failure.Code, Message: failure.Message}
		} else {
			m.CompletionValue = inline(v)
		}
		return m
	}, nil)
	return core.Ticket{Leaf: completion(id)}, err
}

// Attach emits `AttachInvocation`.
func (t *Turn) Attach(invocationID, name string) (core.Ticket, error) {
	id := t.alloc()
	_, err := t.command(wire.TypeAttachInvocation, func() wire.Message {
		return &wire.AttachInvocation{InvocationID: invocationID, ResultCompletionID: id, Name: name}
	}, nil)
	return core.Ticket{Leaf: completion(id)}, err
}

// GetOutput emits `GetInvocationOutput`.
func (t *Turn) GetOutput(invocationID, name string) (core.Ticket, error) {
	id := t.alloc()
	_, err := t.command(wire.TypeGetInvocationOutput, func() wire.Message {
		return &wire.GetInvocationOutput{InvocationID: invocationID, ResultCompletionID: id, Name: name}
	}, nil)
	return core.Ticket{Leaf: completion(id)}, err
}

// Run emits `Run`. A result in the journal is delivered in journal order and the closure
// does not run; otherwise the closure starts now, and when it returns its result is
// proposed. The proposal is not durable in this turn: the run's leaf joins the suspension
// tree and the next turn delivers the result in journal order.
func (t *Turn) Run(spec core.RunSpec) (core.Ticket, error) {
	id := t.alloc()
	step := uint32(t.cursor)
	name := spec.Name
	if _, err := t.command(wire.TypeRun, func() wire.Message {
		return &wire.Run{ResultCompletionID: id, Name: name}
	}, &name); err != nil {
		return core.Ticket{}, err
	}
	leaf := completion(id)
	if _, ok := t.positions[leaf]; ok {
		return core.Ticket{Leaf: leaf}, nil
	}
	// No result yet and the invocation was cancelled: a cancellable step does not run.
	if spec.Cancellable && t.cancelled.Load() {
		return core.Ticket{}, core.Cancelled()
	}
	// A trial replay never runs a closure: the run stays a wait, and the turn ends having
	// matched the whole journal.
	if t.replayOnly {
		return core.Ticket{Leaf: leaf}, nil
	}
	t.ownRuns[leaf] = t.epoch
	t.wmu.Lock()
	t.undrained[t.epoch]++
	t.wmu.Unlock()
	t.mu.Lock()
	t.runStep[leaf] = step
	t.running++
	t.mu.Unlock()
	cancel := spec.Exec(t.cfg.Context, leaf, func(o core.RunOutcome) {
		t.mu.Lock()
		t.done = append(t.done, finished{id: id, outcome: o})
		t.running--
		delete(t.cancels, leaf)
		t.mu.Unlock()
		t.poke()
		t.kickLive()
	})
	t.kickLive()
	// A CANCEL arriving mid-turn (only BIDI_STREAM can deliver one, see Cancel) aborts a
	// cancellable closure; in REQUEST_RESPONSE the closure runs to its end.
	if spec.Cancellable && cancel != nil {
		t.mu.Lock()
		t.cancels[leaf] = cancel
		t.mu.Unlock()
		if t.cancelled.Load() {
			cancel()
		}
	}
	return core.Ticket{Leaf: leaf}, nil
}

// poke wakes the handler goroutine if it waits in Await.
func (t *Turn) poke() {
	select {
	case t.wake <- struct{}{}:
	default:
	}
}

// drain processes the closures that returned since the last call (handler goroutine, or
// the transport once the handler goroutine ended). Proposals are built here; a transient
// failure fails the turn.
func (t *Turn) drain() {
	// wmu first: a live write never sees a closure that has left done while its proposal
	// is not built yet.
	t.wmu.Lock()
	t.mu.Lock()
	done := t.done
	t.done = nil
	t.mu.Unlock()
	t.propose(done)
	t.wmu.Unlock()
	t.liveAfterDrain()
}

// propose builds the proposals of the closures that returned (under wmu).
func (t *Turn) propose(done []finished) {
	slices.SortFunc(done, func(a, b finished) int { return int(a.id) - int(b.id) })
	for _, f := range done {
		leaf := completion(f.id)
		p := proposal{id: f.id, epoch: t.ownRuns[leaf]}
		t.undrained[p.epoch]--
		t.mu.Lock()
		p.frames = t.closurePuts[leaf]
		delete(t.closurePuts, leaf)
		t.mu.Unlock()
		o := f.outcome
		flags := uint16(0)
		if t.cfg.RequestAck {
			flags = wire.FlagRequestedAck
		}
		var propose *wire.ProposeRunCompletion
		var completed *wire.RunCompletion
		switch {
		case o.Cancelled:
			t.cancelledRun[leaf] = true
		case o.Failure != nil && o.Failure.Kind != core.TerminalFailure:
			// Not proposed: the retry reruns the side effect. The turn fails here even if user
			// code swallows the error.
			if t.failedWith == nil {
				t.failedWith = o.Failure
			}
		case o.Failure != nil:
			fl := &wire.Failure{Code: o.Failure.Code, Message: o.Failure.Message}
			propose = wire.NewProposeFailure(f.id, fl)
			completed = &wire.RunCompletion{ID: f.id, Result: wire.Result{Failure: fl}}
		default:
			if v := t.segmentedFor(o.Value); v.IsSegmented() {
				propose = wire.NewProposeSegmented(f.id, v)
			} else {
				propose = wire.NewProposeValue(f.id, o.Value)
			}
			completed = &wire.RunCompletion{ID: f.id, Result: wire.Result{Value: inline(o.Value)}}
			if len(wire.Marshal(propose)) > wire.MaxFrameLen {
				// Over the frame cap: a terminal, durable failure of the step, never a frame the
				// runtime would refuse and retry forever.
				e := frameTooLarge("Run result", len(o.Value))
				fl := &wire.Failure{Code: e.Code, Message: e.Message}
				propose = wire.NewProposeFailure(f.id, fl)
				completed = &wire.RunCompletion{ID: f.id, Result: wire.Result{Failure: fl}}
			}
		}
		if propose != nil {
			p.frames = append(p.frames, wire.Frame{Type: wire.TypeProposeRunCompletion, Flags: flags, Payload: wire.Marshal(propose)})
			t.pendingRuns[f.id] = wire.Frame{Type: wire.TypeRunCompletion, Payload: wire.Marshal(completed)}
		}
		if len(p.frames) > 0 {
			t.proposals = append(t.proposals, p)
		}
	}
}

// closureState says whether every closure of the turn has returned and been processed.
type closureState uint8

const (
	closuresSettled closureState = iota
	closuresRunning
	closuresReturned
)

// settled reads the running count and the unprocessed returns together, so the turn never
// suspends between a closure returning and its proposal joining the tree.
func (t *Turn) settled() closureState {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch {
	case len(t.done) > 0:
		return closuresReturned
	case t.running > 0:
		return closuresRunning
	default:
		return closuresSettled
	}
}
