// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package turn

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

// Append batches a stream record (or the closing of a stream) for the `StreamAppend`
// side channel (wire v4, docs/protocol-v1.md section 9.1). The record's identity is
// (step, attempt, k): step is the position of the appending closure's `Run` command, or the
// handler's current command position; k counts within the step. Replay-aware: a record
// appended where the next command is already in the journal gets its identity but is not
// sent (the earlier run sent it). Never fails: below wire v4, or once the turn has an
// outcome, the record is dropped.
func (t *Turn) Append(a core.Append) (core.Ident, bool) {
	if t.cfg.Wire < StreamsVersion {
		return core.Ident{}, false
	}
	inClosure := !a.Closure.IsZero()
	if !inClosure {
		// The handler goroutine owns the cursor and the outcome.
		if t.replaying() {
			if a.Build == nil {
				return core.Ident{}, false
			}
			t.mu.Lock()
			id := t.nextIdent(uint32(t.cursor), a.Stream)
			t.mu.Unlock()
			return id, true
		}
		if t.dead() {
			return core.Ident{}, false
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// BIDI_STREAM: the live loop puts the batch on the wire (live.go).
	defer t.kickLive()
	step := uint32(t.cursor)
	if inClosure {
		step = t.runStep[a.Closure]
	}
	var s *pendingStream
	for _, p := range t.streams {
		if p.name == a.Stream {
			s = p
			break
		}
	}
	if s == nil {
		s = &pendingStream{name: a.Stream, durable: a.Durable}
		t.streams = append(t.streams, s)
	}
	if a.Build == nil {
		s.close = true
		t.batchStreams()
		return core.Ident{}, false
	}
	id := t.nextIdent(step, a.Stream)
	payload := a.Build(id)
	s.records = append(s.records, wire.StreamRecord{Step: id.Step, Attempt: id.Attempt, K: id.K, Payload: payload})
	t.streamBytes += len(payload)
	if t.streamBytes >= streamBatchBytes {
		t.batchStreams()
	}
	return id, true
}

// nextIdent allocates the next identity of a stream at a step (under mu).
func (t *Turn) nextIdent(step uint32, name string) core.Ident {
	key := streamKey{step: step, name: name}
	k := t.streamK[key]
	t.streamK[key] = k + 1
	return core.Ident{Step: step, Attempt: t.info.Attempt, K: k}
}

// batchStreams turns the batched records into one frame per stream (under mu).
func (t *Turn) batchStreams() {
	for _, s := range t.streams {
		m := &wire.StreamAppend{Name: s.name, Durable: s.durable, Records: s.records, Close: s.close}
		t.streamFrames = append(t.streamFrames, wire.Frame{Type: wire.TypeStreamAppend, Payload: wire.Marshal(m)})
	}
	t.streams = nil
	t.streamBytes = 0
}

// flushStreams returns every stream frame not yet sent; the transport puts them before
// the frame that ends the turn (the runtime stops reading after the outcome).
func (t *Turn) flushStreams() []wire.Frame {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.batchStreams()
	out := t.streamFrames
	t.streamFrames = nil
	return out
}

// PutArtifact sends `ArtifactPut` (wire v4). From the handler goroutine the frame goes at
// the current position (not at all when the point is already in the journal); from a
// closure it goes right before the closure's proposal, so the runtime stores the artifact
// before the result that refers to it.
func (t *Turn) PutArtifact(scope core.Leaf, sha [32]byte, content []byte, contentType string) error {
	if t.cfg.Wire < StreamsVersion {
		return fmt.Errorf("cairn: artifacts need a runtime speaking wire v%d; the peer speaks v%d", StreamsVersion, t.cfg.Wire)
	}
	f := wire.Frame{Type: wire.TypeArtifactPut, Payload: wire.Marshal(&wire.ArtifactPut{Sha256: sha[:], Content: content, ContentType: contentType})}
	if scope.IsZero() {
		if t.replaying() || t.dead() {
			return nil
		}
		t.emit(f)
		return nil
	}
	t.mu.Lock()
	t.closurePuts[scope] = append(t.closurePuts[scope], f)
	t.mu.Unlock()
	return nil
}

// FetchArtifact downloads an artifact that is not in this process's memory.
func (t *Turn) FetchArtifact(ctx context.Context, sha256 string) ([]byte, error) {
	if t.cfg.FetchArtifact == nil {
		return nil, errors.New("cairn: artifact " + sha256 + " is not in this process's memory, and reading artifacts " +
			"from the runtime is not configured (cairn.WithArtifactSource)")
	}
	return t.cfg.FetchArtifact(ctx, hex.EncodeToString(t.info.ID), sha256)
}
