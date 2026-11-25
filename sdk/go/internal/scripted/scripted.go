// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

// Package scripted is a [core.Driver] over a scripted journal, for tests of the public
// API and the execution core without a wire: results become available in a given journal
// order, every command is recorded, and the turn suspends when user code waits for
// something the script does not have. It models the delivery rules of the turn machine
// (results in journal order, one position at a time; `ctx.run` closures executed when
// the command is new; proposals not delivered in the same turn unless Ack is set) but
// not the wire: it never compares commands against a journal.
package scripted

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
)

// Delivery is one result in the journal: what it satisfies and the result itself.
type Delivery struct {
	Leaf   core.Leaf
	Result core.Result
}

// Value is a ValueResult delivery for a completion id.
func Value(id uint32, json string) Delivery {
	return Delivery{Leaf: core.Leaf{Kind: core.CompletionLeaf, ID: id}, Result: core.Result{Kind: core.ValueResult, Value: []byte(json)}}
}

// Void is a VoidResult delivery for a completion id.
func Void(id uint32) Delivery {
	return Delivery{Leaf: core.Leaf{Kind: core.CompletionLeaf, ID: id}, Result: core.Result{Kind: core.VoidResult}}
}

// Failure is a FailureResult delivery for a completion id.
func Failure(id uint32, code uint32, message string) Delivery {
	return Delivery{Leaf: core.Leaf{Kind: core.CompletionLeaf, ID: id}, Result: core.Result{Kind: core.FailureResult, Code: code, Message: message}}
}

// Keys is a StateKeysResult delivery for a completion id.
func Keys(id uint32, keys ...string) Delivery {
	return Delivery{Leaf: core.Leaf{Kind: core.CompletionLeaf, ID: id}, Result: core.Result{Kind: core.StateKeysResult, Keys: keys}}
}

// Signal is a named signal delivery carrying a JSON value.
func Signal(name, json string) Delivery {
	return Delivery{Leaf: core.Leaf{Kind: core.SignalNameLeaf, Name: name}, Result: core.Result{Kind: core.ValueResult, Value: []byte(json)}}
}

// IndexSignal is an indexed signal delivery carrying a JSON value.
func IndexSignal(idx uint32, json string) Delivery {
	return Delivery{Leaf: core.Leaf{Kind: core.SignalIndexLeaf, ID: idx}, Result: core.Result{Kind: core.ValueResult, Value: []byte(json)}}
}

// Command is one recorded command.
type Command struct {
	// Kind is the message name ("Run", "Sleep", "Call", "SetState", ...).
	Kind string
	// Name is the command's name (state key, run name, "sleep", "<svc>/<handler>", ...).
	Name string
	// ID is the completion id the command declared (0 when none).
	ID uint32
	// Value is the value bytes the command carries (SetState, Call input, ...).
	Value []byte
	// Call is set for Call and OneWayCall.
	Call *core.CallSpec
	// Delay is the delay of a Sleep or a OneWayCall.
	Delay time.Duration
}

func (c Command) String() string {
	if c.ID != 0 {
		return fmt.Sprintf("%s %s #%d", c.Kind, c.Name, c.ID)
	}
	return c.Kind + " " + c.Name
}

// Proposal is a `ctx.run` closure outcome the driver received.
type Proposal struct {
	ID      uint32
	Outcome core.RunOutcome
}

// Record is one stream record (or close) the driver received.
type Record struct {
	Stream  string
	Durable bool
	Ident   core.Ident
	Payload []byte
	Close   bool
}

// Artifact is one artifact put.
type Artifact struct {
	SHA         string
	Content     []byte
	ContentType string
	Scope       core.Leaf
}

// Driver is the scripted driver. Configure the exported fields before the turn; read
// the recorded ones after core.Execute returned.
type Driver struct {
	// Info is returned by Info (WireVersion defaults to 6).
	InfoValue core.Info
	// Clock is Now() in ms.
	Clock int64
	// Journal is every result, in journal order. A result is delivered when user code
	// waits and every result before it has been delivered.
	Journal []Delivery
	// Ack makes a closure's proposal delivered in the same turn (as a BIDI_STREAM ack
	// does): its result is appended to the journal when the closure ends.
	Ack bool
	// StopAtCommand stops the turn with a journal mismatch at this command index
	// (1-based; 0 = never).
	StopAtCommand int
	// Cancelled marks the invocation as cancelled (a CANCEL signal in the journal).
	Cancelled bool
	// Eager answers GetState from this snapshot in place (nil = always lazy).
	Eager map[string][]byte
	// Remote holds artifacts FetchArtifact can download.
	Remote map[string][]byte

	mu        sync.Mutex
	nextID    uint32
	nextAwk   uint32
	cursor    int
	delivered map[core.Leaf]int
	runs      map[uint32]bool // completion id → closure still running
	cancels   map[uint32]context.CancelFunc
	doneCh    chan Proposal
	streamK   map[string]uint32

	// Commands are the commands emitted, in order.
	Commands []Command
	// Proposals are the closure outcomes, in the order they arrived.
	Proposals []Proposal
	// Records are the stream records, in the order they arrived.
	Records []Record
	// Artifacts are the artifacts put.
	Artifacts []Artifact
	// Suspended is the last wait when the turn suspended.
	Suspended *core.Wait
	// Waits counts Await calls.
	Waits int
}

// New returns a driver over the journal.
func New(journal ...Delivery) *Driver {
	return &Driver{Journal: journal}
}

func (d *Driver) init() {
	if d.delivered == nil {
		d.delivered = map[core.Leaf]int{}
		d.runs = map[uint32]bool{}
		d.cancels = map[uint32]context.CancelFunc{}
		d.doneCh = make(chan Proposal, 64)
		d.streamK = map[string]uint32{}
		d.nextAwk = 17
	}
}

func (d *Driver) Info() core.Info {
	i := d.InfoValue
	if i.WireVersion == 0 {
		i.WireVersion = 6
	}
	if i.ID == nil {
		i.ID = []byte{0xca, 0x1f, 0x00, 0x01}
	}
	return i
}

func (d *Driver) Now() int64 { return d.Clock }

func (d *Driver) alloc() uint32 {
	d.nextID++
	return d.nextID
}

func (d *Driver) command(c Command) error {
	d.mu.Lock()
	d.init()
	d.Commands = append(d.Commands, c)
	n := len(d.Commands)
	d.mu.Unlock()
	if d.StopAtCommand != 0 && n == d.StopAtCommand {
		return &core.Stop{Reason: core.StopMismatch, Cause: fmt.Errorf("entry %d: %s does not match the journal", n-1, c)}
	}
	return nil
}

func leaf(id uint32) core.Leaf { return core.Leaf{Kind: core.CompletionLeaf, ID: id} }

func (d *Driver) GetState(key string) (core.Ticket, error) {
	if v, ok := d.Eager[key]; ok {
		return core.Ticket{Known: core.Result{Kind: core.ValueResult, Value: v}}, d.command(Command{Kind: "GetEagerState", Name: key})
	}
	if d.Eager != nil {
		return core.Ticket{Known: core.Result{Kind: core.VoidResult}}, d.command(Command{Kind: "GetEagerState", Name: key})
	}
	id := d.alloc()
	return core.Ticket{Leaf: leaf(id)}, d.command(Command{Kind: "GetState", Name: key, ID: id})
}

func (d *Driver) SetState(key string, value []byte) error {
	return d.command(Command{Kind: "SetState", Name: key, Value: value})
}

func (d *Driver) ClearState(key string) error {
	return d.command(Command{Kind: "ClearState", Name: key})
}

func (d *Driver) ClearAllState() error {
	return d.command(Command{Kind: "ClearAllState", Name: "clearAll"})
}

func (d *Driver) StateKeys() (core.Ticket, error) {
	id := d.alloc()
	return core.Ticket{Leaf: leaf(id)}, d.command(Command{Kind: "GetStateKeys", Name: "getKeys", ID: id})
}

func (d *Driver) Sleep(delay time.Duration, name string) (core.Ticket, error) {
	id := d.alloc()
	return core.Ticket{Leaf: leaf(id)}, d.command(Command{Kind: "Sleep", Name: name, ID: id, Delay: delay})
}

func (d *Driver) Call(c core.CallSpec) (core.Ticket, error) {
	d.alloc() // the invocation id notification
	id := d.alloc()
	return core.Ticket{Leaf: leaf(id)}, d.command(Command{Kind: "Call", Name: c.Name, ID: id, Value: c.Input, Call: &c})
}

func (d *Driver) Send(c core.CallSpec, delay time.Duration) error {
	d.alloc()
	return d.command(Command{Kind: "OneWayCall", Name: c.Name, Value: c.Input, Call: &c, Delay: delay})
}

func (d *Driver) SendSignal(target string, signal core.SignalID, value []byte) error {
	return d.command(Command{Kind: "SendSignal", Name: target + "/" + signal.String(), Value: value})
}

func (d *Driver) AllocAwakeable() uint32 {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.init()
	n := d.nextAwk
	d.nextAwk++
	return n
}

func (d *Driver) GetPromise(name string) (core.Ticket, error) {
	id := d.alloc()
	return core.Ticket{Leaf: leaf(id)}, d.command(Command{Kind: "GetPromise", Name: "promise/" + name, ID: id})
}

func (d *Driver) PeekPromise(name string) (core.Ticket, error) {
	id := d.alloc()
	return core.Ticket{Leaf: leaf(id)}, d.command(Command{Kind: "PeekPromise", Name: "promise/" + name, ID: id})
}

func (d *Driver) CompletePromise(name string, value []byte, failure *core.Failure) (core.Ticket, error) {
	id := d.alloc()
	if failure != nil {
		value = []byte(fmt.Sprintf("failure %d %s", failure.Code, failure.Message))
	}
	return core.Ticket{Leaf: leaf(id)}, d.command(Command{Kind: "CompletePromise", Name: "promise/" + name, ID: id, Value: value})
}

func (d *Driver) Attach(invocationID, name string) (core.Ticket, error) {
	id := d.alloc()
	return core.Ticket{Leaf: leaf(id)}, d.command(Command{Kind: "AttachInvocation", Name: name, ID: id, Value: []byte(invocationID)})
}

func (d *Driver) GetOutput(invocationID, name string) (core.Ticket, error) {
	id := d.alloc()
	return core.Ticket{Leaf: leaf(id)}, d.command(Command{Kind: "GetInvocationOutput", Name: name, ID: id, Value: []byte(invocationID)})
}

func (d *Driver) inJournal(l core.Leaf) bool {
	for _, e := range d.Journal {
		if e.Leaf == l {
			return true
		}
	}
	return false
}

func (d *Driver) Run(spec core.RunSpec) (core.Ticket, error) {
	id := d.alloc()
	if err := d.command(Command{Kind: "Run", Name: spec.Name, ID: id}); err != nil {
		return core.Ticket{}, err
	}
	d.mu.Lock()
	present := d.inJournal(leaf(id))
	d.mu.Unlock()
	if present {
		return core.Ticket{Leaf: leaf(id)}, nil
	}
	if spec.Cancellable && d.Cancelled {
		return core.Ticket{}, core.Cancelled()
	}
	d.mu.Lock()
	d.runs[id] = true
	d.mu.Unlock()
	cancel := spec.Exec(context.Background(), leaf(id), func(o core.RunOutcome) {
		d.doneCh <- Proposal{ID: id, Outcome: o}
	})
	d.mu.Lock()
	d.cancels[id] = cancel
	// A CANCEL that arrived while Exec was starting the closure found no cancel
	// function to call: deliver it now.
	if d.Cancelled {
		cancel()
	}
	d.mu.Unlock()
	return core.Ticket{Leaf: leaf(id)}, nil
}

// CancelRuns aborts every running closure (a CANCEL arriving mid-turn).
func (d *Driver) CancelRuns() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Cancelled = true
	for id, c := range d.cancels {
		if d.runs[id] {
			c()
		}
	}
}

// proposed records a closure outcome; with Ack its result joins the journal.
func (d *Driver) proposed(p Proposal) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.runs[p.ID] = false
	d.Proposals = append(d.Proposals, p)
	o := p.Outcome
	switch {
	case o.Failure != nil && o.Failure.Kind != core.TerminalFailure:
		return &core.Stop{Reason: core.StopFailed, Cause: o.Failure}
	case !d.Ack:
		return nil
	case o.Cancelled:
		f := core.Cancelled()
		d.Journal = append(d.Journal, Failure(p.ID, f.Code, f.Message))
	case o.Failure != nil:
		d.Journal = append(d.Journal, Failure(p.ID, o.Failure.Code, o.Failure.Message))
	default:
		d.Journal = append(d.Journal, Value(p.ID, string(o.Value)))
	}
	return nil
}

func (d *Driver) running() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, r := range d.runs {
		if r {
			return true
		}
	}
	return false
}

func (d *Driver) Await(w core.Wait) error {
	d.mu.Lock()
	d.init()
	d.Waits++
	if d.Cancelled {
		// A leaf whose closure is still running counts as coming: the cancelled
		// closure proposes its outcome, which then joins the journal.
		present := 0
		for _, l := range w.Leaves {
			if d.inJournal(l) || (l.Kind == core.CompletionLeaf && d.runs[l.ID]) {
				present++
			}
		}
		if (w.Mode == core.WaitFirst && present == 0) || (w.Mode == core.WaitAll && present < len(w.Leaves)) {
			d.mu.Unlock()
			return core.Cancelled()
		}
	}
	d.mu.Unlock()
	for {
		// Closures finished so far propose first (their results may join the journal).
		for drained := false; !drained; {
			select {
			case p := <-d.doneCh:
				if err := d.proposed(p); err != nil {
					return err
				}
			default:
				drained = true
			}
		}
		d.mu.Lock()
		for d.cursor < len(d.Journal) {
			e := d.Journal[d.cursor]
			at := d.cursor
			d.cursor++
			if _, ok := d.delivered[e.Leaf]; ok {
				continue // a duplicate signal identity
			}
			d.delivered[e.Leaf] = at
			if slices.Contains(w.Leaves, e.Leaf) {
				d.mu.Unlock()
				return nil
			}
		}
		d.mu.Unlock()
		if !d.running() {
			wait := w
			d.Suspended = &wait
			return &core.Stop{Reason: core.StopSuspended}
		}
		p := <-d.doneCh
		if err := d.proposed(p); err != nil {
			return err
		}
	}
}

func (d *Driver) Delivered(l core.Leaf) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.init()
	_, ok := d.delivered[l]
	return ok
}

func (d *Driver) Order(l core.Leaf) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	if at, ok := d.delivered[l]; ok {
		return at
	}
	return 1 << 62
}

func (d *Driver) Result(l core.Leaf) (core.Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, e := range d.Journal {
		if e.Leaf == l {
			return e.Result, nil
		}
	}
	return core.Result{}, fmt.Errorf("scripted: %s read before it was delivered", l)
}

func (d *Driver) Append(a core.Append) (core.Ident, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.init()
	step := uint32(len(d.Commands))
	if a.Closure.Kind == core.CompletionLeaf {
		for i, c := range d.Commands {
			if c.Kind == "Run" && c.ID == a.Closure.ID {
				step = uint32(i)
			}
		}
	}
	if a.Build == nil {
		d.Records = append(d.Records, Record{Stream: a.Stream, Durable: a.Durable, Close: true})
		return core.Ident{}, false
	}
	key := fmt.Sprintf("%d/%s", step, a.Stream)
	k := d.streamK[key]
	d.streamK[key] = k + 1
	id := core.Ident{Step: step, Attempt: d.InfoValue.Attempt, K: k}
	d.Records = append(d.Records, Record{Stream: a.Stream, Durable: a.Durable, Ident: id, Payload: a.Build(id)})
	return id, true
}

func (d *Driver) PutArtifact(scope core.Leaf, sha [32]byte, content []byte, contentType string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Artifacts = append(d.Artifacts, Artifact{SHA: hex.EncodeToString(sha[:]), Content: content, ContentType: contentType, Scope: scope})
	return nil
}

func (d *Driver) FetchArtifact(_ context.Context, sha string) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if b, ok := d.Remote[sha]; ok {
		return b, nil
	}
	return nil, fmt.Errorf("artifact %s: HTTP 404", sha)
}

// Kinds returns the recorded commands as strings ("Run a1 #1"), for compact assertions.
func (d *Driver) Kinds() []string {
	out := make([]string, len(d.Commands))
	for i, c := range d.Commands {
		out[i] = c.String()
	}
	return out
}
