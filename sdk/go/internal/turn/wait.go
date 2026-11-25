// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package turn

import (
	"fmt"
	"slices"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
)

// notDelivered is the order of a leaf that has not been delivered: after everything.
const notDelivered = 1 << 62

// Await blocks until the wait makes progress: it delivers results in journal order, one
// position at a time, and returns right after a delivery that touches a leaf of w. While
// `ctx.run` closures run it waits for them; a closure returning only adds a proposal (in
// REQUEST_RESPONSE a proposal is never durable within the turn). With nothing left to
// deliver and no closure running, the turn suspends on the tree of w merged with the runs
// proposed in this turn.
func (t *Turn) Await(w core.Wait) error {
	if err := t.stop(); err != nil {
		return err
	}
	if t.cancelled.Load() && !t.satisfiable(w) {
		return core.Cancelled()
	}
	parked := false
	for {
		t.drain()
		if t.failedWith != nil {
			return &core.Stop{Reason: core.StopFailed, Cause: t.failedWith}
		}
		for _, l := range w.Leaves {
			if t.cancelledRun[l] && !t.Delivered(l) {
				if parked {
					// A live write may have closed the epoch while user code was parked:
					// what user code does next belongs to a new one.
					t.nextEpoch()
				}
				return core.Cancelled()
			}
		}
		for t.nextDelivery < len(t.deliveries) {
			batch := t.deliveries[t.nextDelivery]
			t.nextDelivery++
			woke := false
			for _, l := range batch {
				t.delivered[l] = t.positions[l]
				woke = woke || slices.Contains(w.Leaves, l)
			}
			if woke {
				// User code resumes: commands from here on belong to a new epoch, after the
				// proposals of the closures started before.
				t.nextEpoch()
				return nil
			}
		}
		switch t.settled() {
		case closuresRunning:
			t.park(true)
			<-t.wake
			t.park(false)
			parked = true
			continue
		case closuresReturned:
			continue // a closure returned after the drain above: process it first
		}
		tree := t.waitTree(&w)
		t.suspendedOn = &tree
		return &core.Stop{Reason: core.StopSuspended}
	}
}

// satisfiable reports whether a wait can still be satisfied from the journal once the
// invocation is cancelled: a FIRST wait needs one leaf present, an ALL wait every leaf.
// Waiting on one run of this turn is not a cancellation point: it waits for durability.
func (t *Turn) satisfiable(w core.Wait) bool {
	present := 0
	for _, l := range w.Leaves {
		if _, ok := t.positions[l]; ok {
			present++
		}
	}
	if len(w.Leaves) == 1 && present == 0 {
		_, own := t.ownRuns[w.Leaves[0]]
		return own && !t.cancelledRun[w.Leaves[0]]
	}
	if w.Mode == core.WaitAll {
		return present == len(w.Leaves)
	}
	return present > 0
}

// waitTree is the suspension tree of the turn: the user wait (nil when the handler is not
// waiting) and every run proposed in this turn, merged (see merge).
func (t *Turn) waitTree(w *core.Wait) Tree {
	var nodes []Tree
	if w != nil {
		var n Tree
		left := 0
		for _, l := range w.Leaves {
			if _, ok := t.delivered[l]; ok {
				continue
			}
			left++
			switch l.Kind {
			case core.CompletionLeaf:
				n.Completions = append(n.Completions, l.ID)
			case core.SignalIndexLeaf:
				n.Signals = append(n.Signals, l.ID)
			case core.SignalNameLeaf:
				n.Names = append(n.Names, l.Name)
			}
		}
		n.All = w.Mode == core.WaitAll && left > 1
		if left > 0 {
			nodes = append(nodes, n)
		}
	}
	ids := make([]uint32, 0, len(t.pendingRuns))
	for id := range t.pendingRuns {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		if _, ok := t.delivered[completion(id)]; !ok {
			nodes = append(nodes, Tree{Completions: []uint32{id}})
		}
	}
	if len(nodes) == 0 {
		return Tree{}
	}
	return merge(nodes)
}

// nextEpoch starts a new epoch: user code resumes from a wait.
func (t *Turn) nextEpoch() {
	t.wmu.Lock()
	t.epoch++
	t.wmu.Unlock()
}

// park records that the handler goroutine blocks in Await (or no longer does): while it
// is parked its epoch gets no new command, so a live write may close the epoch.
func (t *Turn) park(on bool) {
	t.wmu.Lock()
	t.parked = on
	t.wmu.Unlock()
}

// Delivered reports whether the leaf's result has reached user code.
func (t *Turn) Delivered(l core.Leaf) bool {
	_, ok := t.delivered[l]
	return ok
}

// Order is the journal position of a delivered leaf.
func (t *Turn) Order(l core.Leaf) int {
	if at, ok := t.delivered[l]; ok {
		return at
	}
	return notDelivered
}

// Result returns a delivered leaf's result.
func (t *Turn) Result(l core.Leaf) (core.Result, error) {
	if t.cancelledRun[l] {
		f := core.Cancelled()
		return core.Result{Kind: core.FailureResult, Code: f.Code, Message: f.Message}, nil
	}
	if _, ok := t.delivered[l]; !ok {
		return core.Result{}, fmt.Errorf("cairn: internal error: %s read before it was delivered", l)
	}
	r := t.results[l]
	if rd, ok := t.reads[l]; ok {
		delete(t.reads, l)
		t.remember(rd, r)
	}
	if r.err != nil {
		return core.Result{}, r.err
	}
	return r.res, nil
}
