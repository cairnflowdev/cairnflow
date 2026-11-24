// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package turn

import (
	"context"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

// Live is the mid-turn write path of a BIDI_STREAM turn. Without it every frame waits
// for the end of the turn (REQUEST_RESPONSE).
type Live struct {
	// Write puts frames on the wire after every frame written before, in call order. It
	// must not block.
	Write func([]wire.Frame)
	// Keepalive is `Start.keepalive_interval_ms` (wire v3); zero sends no keepalive.
	Keepalive time.Duration
}

// Timings of the live path, the same as the other SDKs'.
const (
	// runEarlyFlush: a `ctx.run` closure still running after this long (or after the
	// keepalive interval, whichever is shorter) has the frames emitted before it flushed,
	// then a `Keepalive` every interval. A shorter closure flushes nothing: its `Run`
	// goes with its proposal in one write, and the runtime waits for durability once.
	runEarlyFlush = 20 * time.Millisecond
	// streamBatchWait: stream records wait this long for others to share their frame.
	streamBatchWait = 50 * time.Millisecond
)

// startLive starts the live loop of a BIDI_STREAM turn; Finish stops it.
func (t *Turn) startLive() {
	t.kick = make(chan struct{}, 1)
	t.liveStop = make(chan struct{})
	t.liveDone = make(chan struct{})
	go t.liveLoop()
}

// stopLive stops the live loop and waits for it: no live write happens after it returns.
func (t *Turn) stopLive() {
	t.wmu.Lock()
	t.liveClosed = true
	t.wmu.Unlock()
	if t.liveStop == nil {
		return
	}
	close(t.liveStop)
	<-t.liveDone
}

// kickLive makes the live loop look at the turn again (a closure started or returned,
// stream records were batched). It never blocks.
func (t *Turn) kickLive() {
	if t.kick == nil {
		return
	}
	select {
	case t.kick <- struct{}{}:
	default:
	}
}

// liveLoop sends keepalives while `ctx.run` closures run (docs/protocol-v1.md §9.1,
// "Keepalive (v3)") and puts stream records on the wire while the turn runs ("Streams
// (v4)"): batched for streamBatchWait, or at once when a batch is full or a stream closes.
func (t *Turn) liveLoop() {
	defer close(t.liveDone)
	interval := t.cfg.Live.Keepalive
	var early, batch *time.Timer
	var beat *time.Ticker
	var earlyC, beatC, batchC <-chan time.Time
	stopKeepalive := func() {
		if early != nil {
			early.Stop()
			early, earlyC = nil, nil
		}
		if beat != nil {
			beat.Stop()
			beat, beatC = nil, nil
		}
	}
	defer func() {
		stopKeepalive()
		if batch != nil {
			batch.Stop()
		}
	}()
	for {
		select {
		case <-t.liveStop:
			return
		case <-t.kick:
		case <-earlyC:
			early, earlyC = nil, nil
			t.flushLive()
			beat = time.NewTicker(interval)
			beatC = beat.C
		case <-beatC:
			t.writeLive(wire.Frame{Type: wire.TypeKeepalive})
		case <-batchC:
			batch, batchC = nil, nil
			t.flushStreamsLive()
		}
		t.mu.Lock()
		running := t.running > 0
		full := len(t.streamFrames) > 0
		pending := len(t.streams) > 0
		t.mu.Unlock()
		switch {
		case interval <= 0:
		case !running:
			stopKeepalive()
		case earlyC == nil && beatC == nil:
			early = time.NewTimer(min(runEarlyFlush, interval))
			earlyC = early.C
		}
		switch {
		case full:
			if batch != nil {
				batch.Stop()
				batch, batchC = nil, nil
			}
			t.flushStreamsLive()
		case pending && batchC == nil:
			batch = time.NewTimer(streamBatchWait)
			batchC = batch.C
		}
	}
}

// writeLive writes frames that need nothing ahead of them (a keepalive).
func (t *Turn) writeLive(frames ...wire.Frame) {
	t.wmu.Lock()
	defer t.wmu.Unlock()
	if !t.liveClosed {
		t.cfg.Live.Write(frames)
	}
}

// flushLive writes what may go on the wire now (see take), so the `Run` of a closure
// that takes long reaches the runtime before the closure ends.
func (t *Turn) flushLive() {
	t.wmu.Lock()
	defer t.wmu.Unlock()
	if t.liveClosed {
		return
	}
	if head := t.take(false); len(head) > 0 {
		t.cfg.Live.Write(head)
	}
}

// flushStreamsLive writes the batched stream records, after every command emitted so far
// (program order: the `Run` of a closure goes before the records the closure appends).
// When a closure has returned but its outcome is not processed yet, the write waits for
// the handler goroutine to process it (liveAfterDrain): its proposal goes first, in the
// same order as when the records are written at the end of the turn.
func (t *Turn) flushStreamsLive() {
	t.wmu.Lock()
	defer t.wmu.Unlock()
	if t.liveClosed {
		return
	}
	t.mu.Lock()
	if len(t.done) > 0 {
		t.streamWanted = true
		t.mu.Unlock()
		t.poke()
		return
	}
	t.streamWanted = false
	t.batchStreams()
	frames := t.streamFrames
	t.streamFrames = nil
	t.mu.Unlock()
	if len(frames) == 0 {
		return
	}
	t.cfg.Live.Write(append(t.take(true), frames...))
}

// liveAfterDrain writes the stream records that waited for drain.
func (t *Turn) liveAfterDrain() {
	if t.cfg.Live == nil {
		return
	}
	t.mu.Lock()
	wanted := t.streamWanted
	t.mu.Unlock()
	if wanted {
		t.flushStreamsLive()
	}
}

// Cancel is a CANCEL signal arriving mid-turn on an open BIDI_STREAM stream
// (docs/protocol-v1.md §8.7). From here the turn behaves like one built on a journal that
// already has CANCEL: a wait that the journal cannot satisfy fails with the cancellation
// error, a cancellable `ctx.run` without a result does not run, and the running
// cancellable closures are aborted. The frame itself joins the local journal before the
// next turn. Safe from any goroutine.
func (t *Turn) Cancel() {
	if t.cancelled.Swap(true) {
		return
	}
	t.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(t.cancels))
	for _, c := range t.cancels {
		cancels = append(cancels, c)
	}
	t.mu.Unlock()
	for _, c := range cancels {
		c()
	}
}

var _ core.Driver = (*Turn)(nil)
