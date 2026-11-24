// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

// Package cairntest runs a handler in the test process, with no runtime and no server,
// for unit tests of handler code.
//
// An [Invocation] plays the runtime's part over the SDK's real turn machine, as in
// REQUEST_RESPONSE: every turn runs the handler from the start over the whole journal so
// far, so a test exercises replay too. The fake runtime answers what a runtime answers at
// once (state, the results of Run closures, promises of the same invocation, calls to a
// [Stub]); everything else (a call without a stub, an awakeable, a signal, an attach) is
// [Pending] until the test provides it with [Invocation.Resolve] or [Invocation.Reject].
// Durable timers fire by themselves when the handler waits on timers alone, and the
// clock jumps to their wake-up time; [Invocation.Advance] moves the clock by hand.
//
//	inv := cairntest.New(orders, "submit", Order{Amount: 100}, cairntest.Key("o-1"))
//	if st, err := inv.Run(); err != nil || st != cairntest.Completed { ... }
//	var r Receipt
//	err := inv.Output(&r)
//
// Code that is not deterministic (it reads the clock or a random source outside Run, or
// ranges over a map before a durable step) makes a later turn produce a different
// command than the journal holds: Run returns that journal mismatch as an error, as the
// runtime would pause the invocation.
//
// The fake runtime runs one invocation: the calls and sends a handler makes do not run
// other handlers (use [Stub] for a call's result, and [Invocation.Sends] to see the
// sends). Values are JSON, as on the wire.
package cairntest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	cairn "github.com/cairnflowdev/cairnflow/sdk/go"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/hook"
)

// Status is where an invocation stands after [Invocation.Run].
type Status string

const (
	// Waiting: the handler waits for something only the test can provide; see
	// [Invocation.Pending].
	Waiting Status = "waiting"
	// Completed: the handler returned a value; see [Invocation.Output].
	Completed Status = "completed"
	// Failed: the handler returned a [cairn.TerminalError]; see [Invocation.Output].
	Failed Status = "failed"
	// Paused: the handler returned a [cairn.PauseError] (or one came from a Run closure).
	// Fix the cause, then call [Invocation.Run] again: that is a resume.
	Paused Status = "paused"
	// ContinuedAsNew: the handler called [cairn.ContinueAsNew]; see
	// [Invocation.ContinuedWith].
	ContinuedAsNew Status = "continued as new"
)

// Kind is what a [Pending] wait is.
type Kind string

const (
	// CallKind: the result of a call to a handler that has no [Stub].
	CallKind Kind = "call"
	// SleepKind: a durable timer.
	SleepKind Kind = "sleep"
	// SignalKind: a named signal ([cairn.Signal]).
	SignalKind Kind = "signal"
	// AwakeableKind: an awakeable ([cairn.Awakeable]); Name is its id.
	AwakeableKind Kind = "awakeable"
	// PromiseKind: a durable promise no handler has completed yet.
	PromiseKind Kind = "promise"
	// AttachKind: the result of another invocation ([cairn.Attach]).
	AttachKind Kind = "attach"
	// OutputKind: a peek at another invocation's result ([cairn.GetOutput]); Resolve with
	// nil means "not finished".
	OutputKind Kind = "output"
)

// Pending is one thing the handler waits for that the fake runtime does not answer by
// itself.
type Pending struct {
	Kind Kind
	// Name is the call's "<service>/<handler>", the timer's name, the signal's or the
	// promise's name, the awakeable's id, or the other invocation's id.
	Name string
	// Service, Handler, Key and Input describe a call.
	Service, Handler, Key string
	Input                 json.RawMessage
	// WakeAt is a timer's wake-up time.
	WakeAt time.Time

	leaf leaf
}

// Send is a one-way call the handler made ([cairn.CallClient.Send]).
type Send struct {
	Service, Handler, Key string
	Input                 json.RawMessage
	// At is when the call is to run (the clock at the send, plus its delay).
	At time.Time
}

// Option configures [New].
type Option func(*config)

type config struct {
	key         string
	state       map[string][]byte
	stubs       map[string]func(json.RawMessage) (any, error)
	clock       time.Time
	maxAttempts int
	err         error
}

// Key sets the key of a virtual object or a workflow.
func Key(key string) Option { return func(c *config) { c.key = key } }

// State sets a state key before the first turn (encoded as JSON, like [cairn.Set]).
func State(key string, value any) Option {
	return func(c *config) {
		b, err := encode(value)
		if err != nil {
			c.err = fmt.Errorf("cairntest: state %q: %w", key, err)
			return
		}
		c.state[key] = b
	}
}

// Stub answers every call to service/handler at once: fn gets the call's input and
// returns its result. An error is the call's failure, with the code of a
// [cairn.TerminalError] in its chain (500 otherwise): the caller sees a
// [cairn.TerminalError], as it would see a callee that failed.
func Stub(service, handler string, fn func(input json.RawMessage) (any, error)) Option {
	return func(c *config) { c.stubs[service+"/"+handler] = fn }
}

// Clock sets the time of the first turn (default: the time New was called). The clock
// only moves when timers fire or with [Invocation.Advance].
func Clock(t time.Time) Option { return func(c *config) { c.clock = t } }

// MaxAttempts is how many attempts in a row may fail transiently (an ordinary error from
// the handler or a Run closure) before Run gives up and returns the error; default 3.
// Each retry runs the handler again, as the runtime's retry policy would.
func MaxAttempts(n int) Option { return func(c *config) { c.maxAttempts = n } }

// New prepares an invocation of handler of service with input (JSON, like a call's
// input; nil is no input). It panics when the service has no such handler or the input
// does not encode: a mistake in the test, not in the code under test.
func New(service *cairn.ServiceDef, handler string, input any, opts ...Option) *Invocation {
	cfg := config{state: map[string][]byte{}, stubs: map[string]func(json.RawMessage) (any, error){}, clock: time.Now(), maxAttempts: 3}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.err != nil {
		panic(cfg.err)
	}
	if hook.Lookup == nil {
		panic("cairntest: package cairn is not initialised")
	}
	h, ok := hook.Lookup(service, handler)
	if !ok {
		panic(fmt.Sprintf("cairntest: service %q has no handler %q", service.Name(), handler))
	}
	var raw []byte
	if input != nil {
		var err error
		if raw, err = encode(input); err != nil {
			panic(fmt.Sprintf("cairntest: input: %v", err))
		}
	}
	id := sha256.Sum256([]byte(service.Name() + "/" + handler + "/" + cfg.key))
	inv := &Invocation{
		service: service.Name(),
		handler: h,
		cfg:     cfg,
		id:      id[:16],
		clock:   cfg.clock.UnixMilli(),
		state:   cfg.state,
		pending: map[leaf]*Pending{},
		done:    map[leaf]bool{},
		timers:  map[uint32]int64{},
		promise: map[string]result{},
		streams: map[string][]json.RawMessage{},
	}
	inv.journal = append(inv.journal, inputEntry(raw))
	return inv
}

// Output decodes the result of a completed invocation into v (as [encoding/json]; a
// json.RawMessage or []byte gets the bytes). For a failed invocation it returns the
// failure as a [*cairn.TerminalError]; for a paused one, a [*cairn.PauseError].
func (inv *Invocation) Output(v any) error {
	switch inv.status {
	case Completed:
		return decode(inv.output, v)
	case Failed:
		return cairn.TerminalErrorf(inv.failure.code, "%s", inv.failure.message)
	case Paused:
		return cairn.ToPauseError(fmt.Errorf("%s", inv.failure.message), cairn.WithCode(inv.failure.code))
	case "":
		return fmt.Errorf("cairntest: the invocation has not run yet")
	default:
		return fmt.Errorf("cairntest: the invocation is %s, not finished", inv.status)
	}
}

// ContinuedWith returns the input of the new invocation [cairn.ContinueAsNew] started,
// when the status is ContinuedAsNew.
func (inv *Invocation) ContinuedWith() (json.RawMessage, bool) {
	return inv.continueInput, inv.status == ContinuedAsNew
}

// Status is where the invocation stands.
func (inv *Invocation) Status() Status { return inv.status }

// Turns counts the turns run so far (every turn runs the handler from the start).
func (inv *Invocation) Turns() int { return inv.turns }

// Pending lists what the handler waits for and the fake runtime does not answer by
// itself, in the order the handler asked.
func (inv *Invocation) Pending() []Pending {
	out := make([]Pending, 0, len(inv.pendingOrder))
	for _, l := range inv.pendingOrder {
		if p, ok := inv.pending[l]; ok {
			out = append(out, *p)
		}
	}
	return out
}

// Resolve completes a pending wait with value (JSON). A promise keeps the value for
// later readers; a signal or an awakeable without a value takes nil. Call Run to go on.
func (inv *Invocation) Resolve(p Pending, value any) error {
	r := result{}
	if value == nil && (p.Kind == SignalKind || p.Kind == AwakeableKind || p.Kind == OutputKind) {
		r.void = true
	} else {
		b, err := encode(value)
		if err != nil {
			return fmt.Errorf("cairntest: %w", err)
		}
		r.value = b
	}
	return inv.complete(p, r)
}

// Reject completes a pending wait with a failure: the handler sees a
// [cairn.TerminalError] with this code and message. Call Run to go on.
func (inv *Invocation) Reject(p Pending, code int, message string) error {
	return inv.complete(p, result{failed: true, code: code, message: message})
}

// Signal sends the named signal to the invocation (nil: a signal without a value). It
// may arrive before the handler waits for it. Call Run to go on.
func (inv *Invocation) Signal(name string, value any) error {
	r := result{void: value == nil}
	if value != nil {
		b, err := encode(value)
		if err != nil {
			return fmt.Errorf("cairntest: %w", err)
		}
		r.value = b
	}
	inv.signal(leaf{kind: namedLeaf, name: name}, r)
	return nil
}

// Cancel cancels the invocation cooperatively, as `POST /invocations/{id}/cancel
// ?mode=signal` does: waits that are cancellation points end with a 409
// [cairn.TerminalError]. Call Run to go on.
func (inv *Invocation) Cancel() {
	inv.signal(leaf{kind: indexLeaf, id: cancelSignal}, result{void: true})
}

// Advance moves the clock forward by d and fires every timer due by then. Call Run to go
// on.
func (inv *Invocation) Advance(d time.Duration) {
	inv.clock += d.Milliseconds()
	inv.fireDue()
}

// Get decodes the state key into v; it reports whether the key exists.
func (inv *Invocation) Get(key string, v any) (bool, error) {
	b, ok := inv.state[key]
	if !ok {
		return false, nil
	}
	return true, decode(b, v)
}

// StateKeys lists the state keys, sorted.
func (inv *Invocation) StateKeys() []string {
	keys := make([]string, 0, len(inv.state))
	for k := range inv.state {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// Sends lists the one-way calls the handler made, in order.
func (inv *Invocation) Sends() []Send { return slices.Clone(inv.sends) }

// Stream returns the records written to a stream of the invocation, in order: a user
// stream ([cairn.Context.Stream]), the durable log "$log" ([cairn.Context.Log]: objects
// with ts, level, msg and attrs) or the trace "$trace" ([cairn.WithSpan]). Records from
// every attempt are kept.
func (inv *Invocation) Stream(name string) []json.RawMessage {
	return slices.Clone(inv.streams[name])
}

// Journal lists the journal's entries by message name ("Input", "Run",
// "RunCompletion", ...): what a turn replays.
func (inv *Invocation) Journal() []string {
	out := make([]string, len(inv.journal))
	for i, f := range inv.journal {
		out[i] = f.Type.String()
	}
	return out
}

// encode is JSON without HTML escaping (the SDK's encoding); json.RawMessage and []byte
// pass through.
func encode(v any) ([]byte, error) {
	switch b := v.(type) {
	case json.RawMessage:
		return slices.Clone([]byte(b)), nil
	case []byte:
		return slices.Clone(b), nil
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func decode(b []byte, v any) error {
	switch p := v.(type) {
	case *json.RawMessage:
		*p = slices.Clone(b)
		return nil
	case *[]byte:
		*p = slices.Clone(b)
		return nil
	}
	return json.Unmarshal(b, v)
}

// awakeableID is the id the SDK gives the awakeable with this signal index.
func (inv *Invocation) awakeableID(idx uint32) string {
	return fmt.Sprintf("awk_%s%08x", hex.EncodeToString(inv.id), idx)
}

// childID is the id the fake runtime gives the invocation a call or a send started.
func (inv *Invocation) childID(idx uint32) string {
	h := sha256.Sum256(fmt.Appendf(nil, "%x/%d", inv.id, idx))
	return hex.EncodeToString(h[:16])
}
