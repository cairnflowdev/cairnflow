// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package turn

import (
	"context"
	"slices"
	"strconv"
	"strings"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/values"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

// RequestResponse runs one REQUEST_RESPONSE turn: the runtime's request body in, the
// response body out (docs/protocol-v1.md section 4). It touches no network.
func RequestResponse(body core.Body, req []byte, cfg Config) []byte {
	frames, err := wire.DecodeFrames(req)
	if err != nil {
		return Encode(ProtocolViolation("invalid request body: " + err.Error()))
	}
	if len(frames) == 0 || frames[0].Type != wire.TypeStart {
		return Encode(ProtocolViolation("the first message must be Start"))
	}
	var start wire.Start
	if err := start.Unmarshal(frames[0].Payload); err != nil {
		return Encode(ProtocolViolation("invalid Start: " + err.Error()))
	}
	// Section 4: REQUEST_RESPONSE does not ask for acks.
	cfg.RequestAck = false
	if err := Admit(cfg, &start, frames[1:]); err != nil {
		return Encode([]wire.Frame{ValueFailure(err)})
	}
	t, input := New(&start, Entries(frames[1:]), cfg)
	return Encode(t.Finish(core.Execute(t, body, input)))
}

// Admit assembles every segmented value of a turn's frames before user code runs (wire
// v6): it loads the read URLs of `Start.segment_urls` and of every `SegmentUrls` frame,
// then assembles each entry carrying refs into cfg.Values. The frames themselves do not
// change. Assembly does not depend on the settled version: the runtime only sends refs to
// a deployment settled on v6, so a ref means assemble, never read the tail as the value.
// Nothing to do when cfg.Values is nil.
func Admit(cfg Config, start *wire.Start, frames []wire.Frame) error {
	if cfg.Values == nil {
		return nil
	}
	if start != nil {
		cfg.Values.AddURLs(start.SegmentURLs)
	}
	ctx := cfg.Context
	if ctx == nil {
		ctx = context.Background()
	}
	for _, f := range frames {
		if err := AdmitFrame(ctx, cfg.Values, f); err != nil {
			return err
		}
	}
	return nil
}

// AdmitFrame takes one frame from the runtime into s: a `SegmentUrls` frame loads its
// URLs, an entry carrying refs is assembled.
func AdmitFrame(ctx context.Context, s *values.Session, f wire.Frame) error {
	if f.Type == wire.TypeSegmentUrls {
		var m wire.SegmentUrls
		if err := m.Unmarshal(f.Payload); err != nil {
			return &values.Error{Message: "invalid SegmentUrls frame"}
		}
		s.AddURLs(m.URLs)
		return nil
	}
	if f.Type.IsSideChannel() {
		return nil
	}
	return s.Admit(ctx, f)
}

// ValueFailure is the reply when a segmented value cannot be assembled (a missing
// segment, an expired URL, a wrong sha256): a transient turn error, retried; not user
// code's, not a journal mismatch. Its message never carries a read URL.
func ValueFailure(err error) wire.Frame {
	return errorFrame(500, "segmented value: "+err.Error(), wire.BehaviorRetry, 0)
}

// Entries keeps the frames that are journal entries: acks and side-channel frames are not
// (docs/protocol-v1.md section 9.2), and counting them would shift every later entry.
func Entries(frames []wire.Frame) []wire.Frame {
	out := make([]wire.Frame, 0, len(frames))
	for _, f := range frames {
		switch {
		case f.Type.IsSideChannel(), f.Type == wire.TypeCommandAck, f.Type == wire.TypeProposeRunCompletionAck:
			continue
		}
		out = append(out, f)
	}
	return out
}

// Encode writes frames back to back.
func Encode(frames []wire.Frame) []byte {
	n := 0
	for _, f := range frames {
		n += f.Len()
	}
	b := make([]byte, 0, n)
	for _, f := range frames {
		b = f.AppendTo(b)
	}
	return b
}

// ProtocolViolation is the reply to a request the SDK cannot read: one Error 571 (FAIL).
func ProtocolViolation(message string) []wire.Frame {
	return []wire.Frame{errorFrame(wire.ProtocolViolation, message, wire.BehaviorFail, 0)}
}

func errorFrame(code uint32, message string, behavior wire.ErrorBehavior, retryAfterMs uint64) wire.Frame {
	m := &wire.ErrorMessage{Code: code, Message: message, Behavior: behavior}
	if retryAfterMs > 0 {
		m.NextRetryDelay = &retryAfterMs
	}
	return wire.Frame{Type: wire.TypeError, Payload: wire.Marshal(m)}
}

func failureError(f *core.Failure, behavior wire.ErrorBehavior) wire.Frame {
	msg := f.Message
	if f.Stack != "" {
		msg = strings.TrimSpace(msg + "\n" + f.Stack)
	}
	code := f.Code
	if code == 0 {
		code = 500
	}
	return errorFrame(code, msg, behavior, uint64(f.RetryAfter.Milliseconds()))
}

// Finish ends the turn once core.Execute has returned (the handler goroutine and every
// closure have ended) and returns every frame of the response not written yet, in order:
// the commands and proposals of the turn, the batched stream records, then the frame that
// ends the turn. In BIDI_STREAM some of them may already be on the wire (Live); Finish
// returns the rest and turns the live path off.
//
// Where proposals go: a `ProposeRunCompletion` sits right after the commands of the epoch
// its `Run` was matched in (an epoch ends each time user code resumes from a wait), the
// proposals of one epoch in completion-id order. That is the order the other SDKs produce
// when closures return at once, made deterministic here so the journal (where the runtime
// puts the `RunCompletion` of each proposal) does not depend on how long a closure takes,
// in either mode; the conformance traces `parallel-branches-*`, `run-ack-order-bidi-v3`
// and `keepalive-side-bidi-v3` pin it. The one exception is a live write in BIDI_STREAM
// that must put a command on the wire while a closure of an earlier epoch still runs (see
// take): that proposal then goes as soon as it is built.
func (t *Turn) Finish(o core.Outcome) []wire.Frame {
	t.stopLive()
	t.drain()
	// Proposals of this turn are not durable yet: suspend on them, even when the handler
	// returned (an unawaited run, the losing branch of a race).
	if t.suspendedOn == nil && len(t.pendingRuns) > 0 {
		tree := t.waitTree(nil)
		t.suspendedOn = &tree
	}
	t.endFrames = t.ending(o)
	t.wmu.Lock()
	t.ended = true
	out := t.take(false)
	t.wmu.Unlock()
	out = append(out, t.flushStreams()...)
	return append(out, t.endFrames...)
}

// take returns the frames that may go on the wire now and moves the write position past
// them (under wmu). It walks the epochs in order: the commands of an epoch, then — once
// the epoch is closed (user code has left it, or is parked in Await, or has ended) and
// every closure started in it has been processed — its proposals in id order, then the
// next epoch. force goes past an epoch whose closures still run, so that every command
// emitted so far goes out (stream records must follow the command of their step); the
// proposals left behind go first in a later take.
func (t *Turn) take(force bool) []wire.Frame {
	var out []wire.Frame
	slices.SortStableFunc(t.proposals, func(a, b proposal) int {
		if a.epoch != b.epoch {
			return a.epoch - b.epoch
		}
		return int(a.id) - int(b.id)
	})
	n := 0
	for n < len(t.proposals) && t.proposals[n].epoch < t.wEpoch {
		out = append(out, t.proposals[n].frames...)
		n++
	}
	t.proposals = t.proposals[n:]
	for {
		for t.wOut < len(t.out) && t.out[t.wOut].epoch == t.wEpoch {
			out = append(out, t.out[t.wOut].frame)
			t.wOut++
		}
		closed := t.wEpoch < t.epoch || t.parked || t.ended
		if !closed || (t.undrained[t.wEpoch] > 0 && !force) {
			return out
		}
		n = 0
		for n < len(t.proposals) && t.proposals[n].epoch == t.wEpoch {
			out = append(out, t.proposals[n].frames...)
			n++
		}
		t.proposals = t.proposals[n:]
		if t.wEpoch >= t.epoch {
			return out
		}
		t.wEpoch++
	}
}

// Commands returns the journal entries this turn emitted, in order (BIDI_STREAM keeps
// them in its local journal: the runtime does not send them back). Valid after Finish.
func (t *Turn) Commands() []wire.Frame {
	var out []wire.Frame
	for _, f := range t.out {
		if f.frame.Type.IsCommand() {
			out = append(out, f.frame)
		}
	}
	for _, f := range t.endFrames {
		if f.Type.IsCommand() {
			out = append(out, f)
		}
	}
	return out
}

// Suspension returns the tree the turn suspended on, and whether the turn ended with
// `Suspension`. Valid after Finish.
func (t *Turn) Suspension() (Tree, bool) {
	if t.suspendedOn == nil || len(t.endFrames) == 0 || t.endFrames[len(t.endFrames)-1].Type != wire.TypeSuspension {
		return Tree{}, false
	}
	return *t.suspendedOn, true
}

// Proposed returns the `RunCompletion` entries of the proposals of this turn, by
// completion id: in BIDI_STREAM each goes into the local journal when its
// `ProposeRunCompletionAck` arrives (docs/protocol-v1.md §4, "Turns on one stream").
// Valid after Finish.
func (t *Turn) Proposed() map[uint32]wire.Frame { return t.pendingRuns }

// ending decides the frames that end the turn. A journal mismatch wins over everything; a
// closure's transient failure over everything else; then the suspension; then what the
// handler did (docs/protocol-v1.md section 10, "End is not a journal entry").
func (t *Turn) ending(o core.Outcome) []wire.Frame {
	if f := t.failedWith; f != nil && t.mismatch == "" && !t.replayOnly {
		if f.Kind == core.PauseFailure {
			// A closure asked for a human: the invocation pauses, the journal stays.
			return []wire.Frame{failureError(&core.Failure{Code: f.Code, Message: f.Message}, wire.BehaviorPause)}
		}
		return []wire.Frame{failureError(f, wire.BehaviorRetry)}
	}
	if o.Kind == core.ContinuedAsNew && t.mismatch == "" && !t.replayOnly {
		return []wire.Frame{{Type: wire.TypeContinueAsNew, Payload: wire.Marshal(&wire.ContinueAsNew{Input: o.ContinueInput})}}
	}
	if t.replayOnly {
		// The replay report replaces every other ending: the trial leaves nothing behind.
		if t.mismatch != "" {
			r := &wire.ReplayReport{Mismatch: true, Message: t.mismatch}
			if m := t.mismatchAt; m != nil {
				r.Matched, r.Expected, r.Got = uint32(m.at), m.expected, m.got
			}
			return []wire.Frame{{Type: wire.TypeReplayReport, Payload: wire.Marshal(r)}}
		}
		if t.replayStop >= 0 {
			return []wire.Frame{{Type: wire.TypeReplayReport, Payload: wire.Marshal(&wire.ReplayReport{Matched: uint32(t.replayStop)})}}
		}
		if o.Kind == core.ContinuedAsNew {
			return []wire.Frame{{Type: wire.TypeReplayReport, Payload: wire.Marshal(&wire.ReplayReport{Matched: uint32(t.cursor)})}}
		}
	}
	if t.mismatch != "" {
		return []wire.Frame{errorFrame(wire.JournalMismatch, t.mismatch, wire.BehaviorPause, 0)}
	}
	if t.suspendedOn != nil {
		return []wire.Frame{{Type: wire.TypeSuspension, Payload: wire.Marshal(t.suspendedOn.Message())}}
	}
	switch o.Kind {
	case core.Returned:
		return t.terminate(func() *wire.Output {
			m := &wire.Output{Name: "output", Result: wire.Result{Value: t.segmentedFor(o.Output)}}
			if len(wire.Marshal(m)) > wire.MaxFrameLen {
				e := frameTooLarge("handler output", len(o.Output))
				m.Result = wire.Result{Failure: &wire.Failure{Code: e.Code, Message: e.Message}}
			}
			return m
		})
	case core.Failed, core.Panicked:
		f := o.Failure
		switch f.Kind {
		case core.TerminalFailure:
			return t.terminate(func() *wire.Output {
				return &wire.Output{Name: "output", Result: wire.Result{Failure: &wire.Failure{Code: f.Code, Message: f.Message}}}
			})
		case core.PauseFailure:
			// Waiting for a human: not an Output, the journal stays so resume continues here.
			return []wire.Frame{failureError(&core.Failure{Code: f.Code, Message: f.Message}, wire.BehaviorPause)}
		default:
			return []wire.Frame{failureError(f, wire.BehaviorRetry)}
		}
	default:
		// A stop with no recorded cause cannot happen; fail the attempt rather than guess.
		return []wire.Frame{errorFrame(500, "cairn: the turn ended without an outcome ("+o.Kind.String()+")", wire.BehaviorRetry, 0)}
	}
}

// terminate ends the invocation: `Output` unless it is already durable, then `End`, which
// is sent every time (it is not a journal entry: a process that died between the two must
// still be able to end the invocation).
func (t *Turn) terminate(build func() *wire.Output) []wire.Frame {
	at := t.cursor
	t.cursor++
	if at < len(t.journaled) {
		j := t.journaled[at]
		if j.Type != wire.TypeOutput {
			t.mismatch = "entry " + strconv.Itoa(at) + ": journal has " + j.Type.UpperName() + ", current code produced " +
				wire.TypeOutput.UpperName()
			return []wire.Frame{errorFrame(wire.JournalMismatch, t.mismatch, wire.BehaviorPause, 0)}
		}
		return []wire.Frame{{Type: wire.TypeEnd}}
	}
	if t.replayOnly {
		// The output is a command outside the journal: the trial stops before it.
		return []wire.Frame{{Type: wire.TypeReplayReport, Payload: wire.Marshal(&wire.ReplayReport{Matched: uint32(at)})}}
	}
	return []wire.Frame{
		{Type: wire.TypeOutput, Payload: wire.Marshal(build())},
		{Type: wire.TypeEnd},
	}
}
