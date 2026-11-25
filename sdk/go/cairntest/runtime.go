// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairntest

// The fake runtime: it builds each REQUEST_RESPONSE turn (Start, then the whole journal),
// runs it on the SDK's turn machine, and journals what comes back the way the runtime
// does (docs/protocol-v1.md sections 6 to 8).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	cairn "github.com/cairnflowdev/cairnflow/sdk/go"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/hook"
	machine "github.com/cairnflowdev/cairnflow/sdk/go/internal/turn"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

// wireVersion is the wire version the fake runtime speaks.
const wireVersion = 6

// maxTurns bounds one Run: a handler that never stops making progress.
const maxTurns = 10_000

const (
	cancelSignal = wire.SignalCancel
	// firstAwakeable is the first signal index an awakeable gets (1 to 16 are built-in).
	firstAwakeable = 17
)

type leafKind uint8

const (
	completionLeaf leafKind = iota
	indexLeaf
	namedLeaf
)

// leaf is one thing a suspension can wait for.
type leaf struct {
	kind leafKind
	id   uint32
	name string
}

// result is a completion's result.
type result struct {
	void    bool
	value   []byte
	failed  bool
	code    int
	message string
}

func (r result) wire() wire.Result {
	switch {
	case r.failed:
		return wire.Result{Failure: &wire.Failure{Code: uint32(r.code), Message: r.message}}
	case r.void:
		return wire.Result{Void: true}
	default:
		return wire.Result{Value: wire.InlineValue(slices.Clone(r.value))}
	}
}

type failure struct {
	code    int
	message string
}

// Invocation is one invocation of a handler, run by the fake runtime. Use it from one
// goroutine.
type Invocation struct {
	service string
	handler hook.Handler
	cfg     config
	id      []byte
	clock   int64 // Unix ms

	state   map[string][]byte
	journal []wire.Frame
	// done holds the leaves whose result is in the journal.
	done map[leaf]bool

	pending      map[leaf]*Pending
	pendingOrder []leaf
	// timers are the pending timers' wake-up times (Unix ms), by completion id.
	timers  map[uint32]int64
	promise map[string]result
	sends   []Send
	streams map[string][]json.RawMessage

	status        Status
	output        []byte
	failure       failure
	continueInput []byte
	suspended     *wire.Suspension
	turns         int
	// attempts counts the turns that failed transiently since the journal last grew.
	attempts int
	lastErr  string
}

// Run runs turns until the invocation completes, fails, pauses, continues as new, or
// waits for something only the test provides (Waiting). Run again after resolving a
// pending wait, after a signal, after Advance, or to resume a paused invocation.
//
// The error is not the handler's result (see [Invocation.Output]) but a run that cannot
// go on: the journal does not match the code (non-deterministic handler code), the SDK
// reported a protocol violation, or the attempts in a row that failed transiently reached
// [MaxAttempts] (the error carries the last failure).
func (inv *Invocation) Run() (Status, error) {
	switch inv.status {
	case Completed, Failed, ContinuedAsNew:
		return inv.status, nil
	}
	for range maxTurns {
		retry, err := inv.turn()
		if err != nil {
			return inv.status, err
		}
		switch inv.status {
		case Completed, Failed, ContinuedAsNew, Paused:
			return inv.status, nil
		}
		if retry {
			if inv.attempts >= inv.cfg.maxAttempts {
				return inv.status, fmt.Errorf("cairntest: %d attempts in a row failed; the last: %s", inv.attempts, inv.lastErr)
			}
			continue
		}
		if inv.suspended == nil {
			return inv.status, errors.New("cairntest: the turn ended without a result, a suspension or an error")
		}
		if inv.satisfied(inv.suspended) {
			continue
		}
		if inv.timersAlone(inv.suspended) {
			inv.fireEarliest()
			continue
		}
		inv.status = Waiting
		return Waiting, nil
	}
	return inv.status, fmt.Errorf("cairntest: no end after %d turns", maxTurns)
}

// turn runs one turn and journals its outcome; retry reports a transient failure.
func (inv *Invocation) turn() (retry bool, err error) {
	start := &wire.Start{
		ID:                             inv.id,
		DebugID:                        "inv_cairntest",
		KnownEntries:                   uint32(len(inv.journal)),
		Key:                            inv.cfg.key,
		RetryCountSinceLastStoredEntry: uint32(inv.attempts),
		RandomSeed:                     0x5eed_ca1f,
	}
	keys := inv.StateKeys()
	for _, k := range keys {
		start.StateMap = append(start.StateMap, wire.StateEntry{Key: []byte(k), Value: slices.Clone(inv.state[k])})
	}
	body := wire.EncodeMessage(start, 0)
	for _, f := range inv.journal {
		body = f.AppendTo(body)
	}
	out := machine.RequestResponse(inv.handler.Body, body, machine.Config{
		Wire:     wireVersion,
		Now:      func() int64 { return inv.clock },
		LogLevel: inv.handler.LogLevel,
		Context:  context.Background(),
	})
	inv.turns++
	frames, err := wire.DecodeFrames(out)
	if err != nil {
		return false, fmt.Errorf("cairntest: the SDK answered frames it cannot decode: %w", err)
	}
	grew := len(inv.journal)
	inv.suspended = nil
	inv.status = ""
	for _, f := range frames {
		m, err := wire.DecodeFrame(f)
		if err != nil {
			return false, fmt.Errorf("cairntest: %s: %w", f.Type, err)
		}
		switch m := m.(type) {
		case *wire.Suspension:
			inv.suspended = m
			inv.noteWaits(m)
		case *wire.ErrorMessage:
			switch {
			case m.Code == wire.JournalMismatch:
				return false, fmt.Errorf("cairntest: journal mismatch (%d), the handler is not deterministic: %s", m.Code, m.Message)
			case m.Code == wire.ProtocolViolation:
				return false, fmt.Errorf("cairntest: protocol violation (%d): %s", m.Code, m.Message)
			case m.Behavior == wire.BehaviorPause:
				inv.status, inv.failure = Paused, failure{code: int(m.Code), message: m.Message}
			case m.Behavior == wire.BehaviorFail:
				inv.status, inv.failure = Failed, failure{code: int(m.Code), message: m.Message}
			default:
				retry = true
				inv.lastErr = fmt.Sprintf("%d %s", m.Code, m.Message)
			}
		case *wire.End:
		case *wire.ContinueAsNew:
			inv.status, inv.continueInput = ContinuedAsNew, slices.Clone(m.Input)
		case *wire.ProposeRunCompletion:
			r := wire.Result{Value: wire.InlineValue(slices.Clone(m.Value))}
			switch {
			case m.Failure != nil:
				r = wire.Result{Failure: m.Failure}
			case m.SegmentedValue != nil:
				return false, errors.New("cairntest: a segmented Run result (the fake runtime does not serve wire v6 refs)")
			}
			inv.notify(&wire.RunCompletion{ID: m.ResultCompletionID, Result: r}, leaf{id: m.ResultCompletionID})
		case *wire.StreamAppend:
			for _, r := range m.Records {
				inv.streams[m.Name] = append(inv.streams[m.Name], slices.Clone(r.Payload))
			}
		case *wire.ArtifactPut, *wire.Keepalive, *wire.ReplayReport:
		default:
			if !f.Type.IsCommand() {
				return false, fmt.Errorf("cairntest: unexpected %s from the SDK", f.Type)
			}
			inv.journal = append(inv.journal, f)
			if err := inv.command(m); err != nil {
				return false, err
			}
		}
	}
	if len(inv.journal) > grew {
		inv.attempts = 0
	}
	if retry {
		inv.attempts++
	}
	return retry, nil
}

// notify journals a notification and marks its leaf done.
func (inv *Invocation) notify(m wire.Message, l leaf) {
	inv.journal = append(inv.journal, wire.Frame{Type: m.Type(), Payload: wire.Marshal(m)})
	inv.done[l] = true
	delete(inv.pending, l)
	if l.kind == completionLeaf {
		delete(inv.timers, l.id)
	}
}

func (inv *Invocation) wait(l leaf, p *Pending) {
	if inv.done[l] {
		return
	}
	if _, ok := inv.pending[l]; !ok {
		inv.pendingOrder = append(inv.pendingOrder, l)
	}
	p.leaf = l
	inv.pending[l] = p
}

// command journals the runtime's side of one new command.
func (inv *Invocation) command(m wire.Message) error {
	switch m := m.(type) {
	case *wire.Output:
		if f := m.Failure; f != nil {
			inv.status, inv.failure = Failed, failure{code: int(f.Code), message: f.Message}
		} else {
			inv.status = Completed
			if m.Value != nil {
				inv.output = slices.Clone(m.Value.Content)
			}
		}
	case *wire.GetState:
		r := result{void: true}
		if v, ok := inv.state[string(m.Key)]; ok {
			r = result{value: v}
		}
		inv.notify(&wire.GetStateCompletion{ID: m.ResultCompletionID, Result: r.wire()}, leaf{id: m.ResultCompletionID})
	case *wire.SetState:
		var v []byte
		if m.Value != nil {
			if m.Value.IsSegmented() {
				return errors.New("cairntest: a segmented state value (the fake runtime does not serve wire v6 refs)")
			}
			v = slices.Clone(m.Value.Content)
		}
		inv.state[string(m.Key)] = v
	case *wire.ClearState:
		delete(inv.state, string(m.Key))
	case *wire.ClearAllState:
		clear(inv.state)
	case *wire.GetStateKeys:
		sk := &wire.StateKeys{}
		for _, k := range inv.StateKeys() {
			sk.Keys = append(sk.Keys, []byte(k))
		}
		inv.notify(&wire.GetStateKeysCompletion{ID: m.ResultCompletionID, StateKeys: sk}, leaf{id: m.ResultCompletionID})
	case *wire.GetPromise:
		l := leaf{id: m.ResultCompletionID}
		if r, ok := inv.promise[m.Key]; ok {
			inv.notify(&wire.GetPromiseCompletion{ID: m.ResultCompletionID, Result: r.wire()}, l)
		} else {
			inv.wait(l, &Pending{Kind: PromiseKind, Name: m.Key})
		}
	case *wire.PeekPromise:
		r, ok := inv.promise[m.Key]
		if !ok {
			r = result{void: true}
		}
		inv.notify(&wire.PeekPromiseCompletion{ID: m.ResultCompletionID, Result: r.wire()}, leaf{id: m.ResultCompletionID})
	case *wire.CompletePromise:
		l := leaf{id: m.ResultCompletionID}
		if _, ok := inv.promise[m.Key]; ok {
			inv.notify(&wire.CompletePromiseCompletion{ID: m.ResultCompletionID, Result: result{failed: true, code: 409, message: "promise already completed"}.wire()}, l)
			return nil
		}
		r := result{}
		switch {
		case m.CompletionFailure != nil:
			r = result{failed: true, code: int(m.CompletionFailure.Code), message: m.CompletionFailure.Message}
		case m.CompletionValue != nil:
			r.value = slices.Clone(m.CompletionValue.Content)
		default:
			r.void = true
		}
		inv.notify(&wire.CompletePromiseCompletion{ID: m.ResultCompletionID, Result: wire.Result{Void: true}}, l)
		inv.completePromise(m.Key, r)
	case *wire.Sleep:
		l := leaf{id: m.ResultCompletionID}
		inv.timers[m.ResultCompletionID] = int64(m.WakeUpTime)
		inv.wait(l, &Pending{Kind: SleepKind, Name: m.Name, WakeAt: time.UnixMilli(int64(m.WakeUpTime))})
	case *wire.Call:
		inv.notify(&wire.CallInvocationIDCompletion{ID: m.InvocationIDNotificationIdx, InvocationID: inv.childID(m.InvocationIDNotificationIdx)},
			leaf{id: m.InvocationIDNotificationIdx})
		l := leaf{id: m.ResultCompletionID}
		if stub, ok := inv.cfg.stubs[m.ServiceName+"/"+m.HandlerName]; ok {
			inv.notify(&wire.CallCompletion{ID: m.ResultCompletionID, Result: stubResult(stub, m.Parameter).wire()}, l)
			return nil
		}
		inv.wait(l, &Pending{Kind: CallKind, Name: m.Name, Service: m.ServiceName, Handler: m.HandlerName, Key: m.Key, Input: slices.Clone(m.Parameter)})
	case *wire.OneWayCall:
		inv.notify(&wire.CallInvocationIDCompletion{ID: m.InvocationIDNotificationIdx, InvocationID: inv.childID(m.InvocationIDNotificationIdx)},
			leaf{id: m.InvocationIDNotificationIdx})
		at := inv.clock
		if int64(m.InvokeTime) > at {
			at = int64(m.InvokeTime)
		}
		inv.sends = append(inv.sends, Send{Service: m.ServiceName, Handler: m.HandlerName, Key: m.Key, Input: slices.Clone(m.Parameter), At: time.UnixMilli(at)})
	case *wire.AttachInvocation:
		inv.wait(leaf{id: m.ResultCompletionID}, &Pending{Kind: AttachKind, Name: m.InvocationID})
	case *wire.GetInvocationOutput:
		inv.wait(leaf{id: m.ResultCompletionID}, &Pending{Kind: OutputKind, Name: m.InvocationID})
	}
	// GetEagerState, GetEagerStateKeys, Run, SendSignal: nothing to answer.
	return nil
}

// stubResult runs a stub and turns its answer into a call result.
func stubResult(stub func(json.RawMessage) (any, error), input []byte) (r result) {
	defer func() {
		if p := recover(); p != nil {
			r = result{failed: true, code: 500, message: fmt.Sprintf("stub panicked: %v", p)}
		}
	}()
	v, err := stub(slices.Clone(input))
	if err != nil {
		code := 500
		if t, ok := cairn.AsTerminalError(err); ok {
			code = t.Code()
		}
		return result{failed: true, code: code, message: err.Error()}
	}
	b, err := encode(v)
	if err != nil {
		return result{failed: true, code: 500, message: "stub result: " + err.Error()}
	}
	return result{value: b}
}

// completePromise stores a promise's result and answers its waiters.
func (inv *Invocation) completePromise(name string, r result) {
	inv.promise[name] = r
	for _, l := range slices.Clone(inv.pendingOrder) {
		if p, ok := inv.pending[l]; ok && p.Kind == PromiseKind && p.Name == name {
			inv.notify(&wire.GetPromiseCompletion{ID: l.id, Result: r.wire()}, l)
		}
	}
}

// noteWaits adds the signals and awakeables a suspension waits for to the pending list.
func (inv *Invocation) noteWaits(s *wire.Suspension) {
	for _, idx := range s.WaitingSignals {
		l := leaf{kind: indexLeaf, id: idx}
		if idx >= firstAwakeable {
			inv.wait(l, &Pending{Kind: AwakeableKind, Name: inv.awakeableID(idx)})
		} else if idx != cancelSignal {
			inv.wait(l, &Pending{Kind: SignalKind, Name: fmt.Sprintf("#%d", idx)})
		}
	}
	for _, name := range s.WaitingNamedSignals {
		inv.wait(leaf{kind: namedLeaf, name: name}, &Pending{Kind: SignalKind, Name: name})
	}
	for i := range s.NestedFutures {
		inv.noteWaits(&s.NestedFutures[i])
	}
}

// signal journals a signal (once per identity: a signal that arrived stays).
func (inv *Invocation) signal(l leaf, r result) {
	if inv.done[l] {
		return
	}
	n := &wire.SignalNotification{Result: r.wire()}
	if l.kind == namedLeaf {
		name := l.name
		n.Signal.Name = &name
	} else {
		idx := l.id
		n.Signal.Idx = &idx
	}
	inv.notify(n, l)
}

// complete answers one pending wait.
func (inv *Invocation) complete(p Pending, r result) error {
	l := p.leaf
	if _, ok := inv.pending[l]; !ok || inv.done[l] {
		return fmt.Errorf("cairntest: %s %q is not pending", p.Kind, p.Name)
	}
	switch p.Kind {
	case CallKind:
		inv.notify(&wire.CallCompletion{ID: l.id, Result: r.wire()}, l)
	case SleepKind:
		inv.notify(&wire.SleepCompletion{ID: l.id, Void: true}, l)
	case PromiseKind:
		inv.completePromise(p.Name, r)
	case AttachKind:
		inv.notify(&wire.AttachInvocationCompletion{ID: l.id, Result: r.wire()}, l)
	case OutputKind:
		inv.notify(&wire.GetInvocationOutputCompletion{ID: l.id, Result: r.wire()}, l)
	case SignalKind, AwakeableKind:
		inv.signal(l, r)
	}
	return nil
}

// satisfied evaluates a suspension tree against the journal.
func (inv *Invocation) satisfied(s *wire.Suspension) bool {
	var got []bool
	for _, id := range s.WaitingCompletions {
		got = append(got, inv.done[leaf{id: id}])
	}
	for _, idx := range s.WaitingSignals {
		got = append(got, inv.done[leaf{kind: indexLeaf, id: idx}])
	}
	for _, name := range s.WaitingNamedSignals {
		got = append(got, inv.done[leaf{kind: namedLeaf, name: name}])
	}
	for i := range s.NestedFutures {
		got = append(got, inv.satisfied(&s.NestedFutures[i]))
	}
	if s.CombinatorType == wire.CombinatorAllCompleted {
		return !slices.Contains(got, false)
	}
	return slices.Contains(got, true)
}

// timersAlone reports whether everything the suspension waits for that is not done yet
// is a timer: then nothing but time can move the handler, and time moves.
func (inv *Invocation) timersAlone(s *wire.Suspension) bool {
	timer := false
	var walk func(s *wire.Suspension) bool
	walk = func(s *wire.Suspension) bool {
		if len(s.WaitingSignals) > 0 || len(s.WaitingNamedSignals) > 0 {
			return false
		}
		for _, id := range s.WaitingCompletions {
			if inv.done[leaf{id: id}] {
				continue
			}
			if _, ok := inv.timers[id]; !ok {
				return false
			}
			timer = true
		}
		for i := range s.NestedFutures {
			if !walk(&s.NestedFutures[i]) {
				return false
			}
		}
		return true
	}
	return walk(s) && timer
}

// fireEarliest moves the clock to the earliest pending timer and fires every timer due.
func (inv *Invocation) fireEarliest() {
	ids := make([]uint32, 0, len(inv.timers))
	for id := range inv.timers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return inv.timers[ids[i]] < inv.timers[ids[j]] })
	if len(ids) > 0 && inv.timers[ids[0]] > inv.clock {
		inv.clock = inv.timers[ids[0]]
	}
	inv.fireDue()
}

// fireDue fires every timer due by the clock, earliest first.
func (inv *Invocation) fireDue() {
	ids := make([]uint32, 0, len(inv.timers))
	for id, at := range inv.timers {
		if at <= inv.clock {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		if inv.timers[ids[i]] != inv.timers[ids[j]] {
			return inv.timers[ids[i]] < inv.timers[ids[j]]
		}
		return ids[i] < ids[j]
	})
	for _, id := range ids {
		inv.notify(&wire.SleepCompletion{ID: id, Void: true}, leaf{id: id})
	}
}

func inputEntry(raw []byte) wire.Frame {
	m := &wire.Input{Value: wire.InlineValue(raw)}
	return wire.Frame{Type: m.Type(), Payload: wire.Marshal(m)}
}
