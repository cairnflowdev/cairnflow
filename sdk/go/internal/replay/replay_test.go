// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

// id is the invocation id of the golden vector.
var id = bytes.Repeat([]byte{0x11}, 24)

// inputFrame is Input("req"), a whole frame as the conformance traces carry it.
var inputFrame = mustHex("040000000000000972070a052272657122")

func mustHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

// The chain must match the runtime's golden vector byte for byte (golden_vector_is_stable
// in crates/cairn-runtime/src/replay.rs, copied in sdk/typescript/test/replay.test.ts).
func TestGoldenVector(t *testing.T) {
	h0 := Root(id)
	if got := hex.EncodeToString(h0[:]); got != "1f64388c3fc28f0042a5c57665b4b9a00b95f335a30260a54323acffdd41bc74" {
		t.Fatalf("h0 = %s", got)
	}
	h1 := Step(h0, inputFrame)
	if got := hex.EncodeToString(h1[:]); got != "caedb1a740dbe983c6a7f0a4be6a7f94e8f0b48fd0324664f1b51c6d3994367a" {
		t.Fatalf("h1 = %s", got)
	}
	if c := Extend([]Digest{h0}, [][]byte{inputFrame}); len(c) != 2 || c[1] != h1 {
		t.Fatalf("Extend: %x", c)
	}
}

// The chain depends on the id, the order and every byte, and frame boundaries are inside
// the hash.
func TestChainBindsIDOrderAndBytes(t *testing.T) {
	chain := func(id []byte, frames ...string) []Digest {
		fs := make([][]byte, len(frames))
		for i, f := range frames {
			fs[i] = []byte(f)
		}
		return Extend([]Digest{Root(id)}, fs)
	}
	a := chain([]byte{1}, "x", "y")
	for name, b := range map[string][]Digest{
		"id":       chain([]byte{2}, "x", "y"),
		"order":    chain([]byte{1}, "y", "x"),
		"bytes":    chain([]byte{1}, "x", "z"),
		"boundary": chain([]byte{1}, "xy", ""),
	} {
		if a[2] == b[2] {
			t.Errorf("%s does not change the chain", name)
		}
	}
}

// fakeClock is a settable clock for the TTL.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func withClock(c *Cache) *fakeClock {
	clock := &fakeClock{now: time.Unix(1_800_000_000, 0)}
	c.now = clock.Now
	return clock
}

// entry is one held frame of n bytes and its chain.
func entry(n int) ([][]byte, []Digest) {
	f := make([]byte, n)
	return [][]byte{f}, Extend([]Digest{Root([]byte("id"))}, [][]byte{f})
}

// Eviction is least-recently-used by bytes; a use moves an entry to the front; an entry
// over the cap is not held; past the TTL an entry is gone.
func TestEvictionByBytesAndTTL(t *testing.T) {
	c := New(Limits{MaxBytes: 1000, TTL: time.Minute})
	clock := withClock(c)
	f, ch := entry(400)
	if !c.Put("a", f, ch) || !c.Put("b", f, ch) {
		t.Fatal("put")
	}
	if _, _, ok := c.Take("a", 1, ch[1][:]); !ok {
		t.Fatal("take a")
	}
	if !c.Put("c", f, ch) { // b is now the oldest
		t.Fatal("put c")
	}
	if _, _, ok := c.Take("b", 1, ch[1][:]); ok {
		t.Fatal("b must be evicted")
	}
	if c.Len() != 2 || c.Bytes() > 1000 {
		t.Fatalf("len %d, bytes %d", c.Len(), c.Bytes())
	}
	big, bigChain := entry(2000)
	if c.Put("big", big, bigChain) {
		t.Fatal("an entry over the cap must not be held")
	}
	// Put over the cap still drops what was held for that id.
	if c.Put("a", big, bigChain) || c.Len() != 1 {
		t.Fatalf("a over the cap: len %d", c.Len())
	}
	clock.Add(time.Minute + time.Second)
	if _, _, ok := c.Held("c", 5); ok {
		t.Fatal("Held past the TTL")
	}
	if _, _, ok := c.Take("c", 1, ch[1][:]); ok || c.Len() != 0 || c.Bytes() != 0 {
		t.Fatalf("past the TTL: len %d, bytes %d", c.Len(), c.Bytes())
	}
	if d := New(Limits{}); d.maxBytes != DefaultMaxBytes || d.ttl != DefaultTTL {
		t.Fatalf("defaults %d %s", d.maxBytes, d.ttl)
	}
}

// Take misses on a wrong digest, too few entries, or b = 0; what it returns is clipped,
// so appending to it never writes into the cache.
func TestTakeAndHeld(t *testing.T) {
	c := New(Limits{})
	frames := [][]byte{[]byte("e0"), []byte("e1"), []byte("e2")}
	chain := Extend([]Digest{Root(id)}, frames)
	c.Put("x", frames, chain)
	bad := chain[2]
	bad[0] ^= 1
	for name, ok := range map[string]bool{
		"wrong digest": take(c, "x", 2, bad[:]),
		"short digest": take(c, "x", 2, chain[2][:31]),
		"too few":      take(c, "x", 4, chain[3][:]),
		"b = 0":        take(c, "x", 0, chain[0][:]),
		"no entry":     take(c, "y", 1, chain[1][:]),
	} {
		if ok {
			t.Errorf("%s: hit", name)
		}
	}
	got, gotChain, ok := c.Take("x", 2, chain[2][:])
	if !ok || len(got) != 2 || len(gotChain) != 3 || gotChain[2] != chain[2] {
		t.Fatalf("hit: %q %x %v", got, gotChain, ok)
	}
	_ = append(got, []byte("intruder"))
	_ = append(gotChain, Digest{})
	if again, againChain, _ := c.Take("x", 3, chain[3][:]); string(again[2]) != "e2" || againChain[3] != chain[3] {
		t.Fatal("appending to a taken prefix wrote into the cache")
	}
	for _, tc := range []struct{ n, k int }{{5, 3}, {3, 3}, {2, 2}, {1, 1}} {
		k, d, ok := c.Held("x", tc.n)
		if !ok || k != tc.k || d != chain[tc.k] {
			t.Errorf("Held(n=%d) = %d %v, want %d", tc.n, k, ok, tc.k)
		}
	}
	if _, _, ok := c.Held("x", 0); ok {
		t.Error("Held with n = 0")
	}
	if _, _, ok := c.Held("y", 3); ok {
		t.Error("Held without an entry")
	}
}

func take(c *Cache, id string, b int, digest []byte) bool {
	_, _, ok := c.Take(id, b, digest)
	return ok
}

// body is a REQUEST_RESPONSE body: Start, then frames.
func body(start *wire.Start, frames ...[]byte) []byte {
	b := wire.EncodeMessage(start, 0)
	for _, f := range frames {
		b = append(b, f...)
	}
	return b
}

var (
	sleepFrame = wire.EncodeMessage(&wire.Sleep{WakeUpTime: 5, ResultCompletionID: 1, Name: "sleep"}, 0)
	doneFrame  = wire.EncodeMessage(&wire.SleepCompletion{ID: 1, Void: true}, 0)
	endFrame   = wire.EncodeMessage(&wire.End{}, 0)
	suspend    = wire.EncodeMessage(&wire.Suspension{WaitingCompletions: []uint32{1}}, 0)
)

// A full replay holds entries [0, N); a delta turn that hits runs exactly the bytes of the
// full replay; the entries grow turn by turn; an ending response drops them.
func TestRejoin(t *testing.T) {
	c := New(Limits{})
	full := body(&wire.Start{ID: id, KnownEntries: 1}, inputFrame)
	t1 := c.Rejoin(full)
	if t1.Miss || !bytes.Equal(t1.Body, full) {
		t.Fatalf("full replay: %+v", t1)
	}
	if k := c.Keep(t1, suspend); k != 1 {
		t.Fatalf("kept %d, want 1", k)
	}
	h1 := Step(Root(id), inputFrame)
	delta := body(&wire.Start{ID: id, KnownEntries: 3, ReplayFrom: 1, PrefixDigest: h1[:]}, sleepFrame, doneFrame)
	t2 := c.Rejoin(delta)
	want := body(&wire.Start{ID: id, KnownEntries: 3, ReplayFrom: 1, PrefixDigest: h1[:]}, inputFrame, sleepFrame, doneFrame)
	if t2.Miss || !bytes.Equal(t2.Body, want) {
		t.Fatalf("delta: %+v", t2)
	}
	if k := c.Keep(t2, suspend); k != 3 {
		t.Fatalf("kept %d, want 3", k)
	}
	if k, d, _ := c.Held(string(id), 3); k != 3 || d != Extend([]Digest{h1}, [][]byte{sleepFrame, doneFrame})[2] {
		t.Fatalf("held %d %x", k, d)
	}
	// The response ends the invocation: the entry is dropped, no header.
	h3 := Extend([]Digest{h1}, [][]byte{sleepFrame, doneFrame})[2]
	t3 := c.Rejoin(body(&wire.Start{ID: id, KnownEntries: 3, ReplayFrom: 3, PrefixDigest: h3[:]}))
	if t3.Miss {
		t.Fatal("B = N must hit")
	}
	if k := c.Keep(t3, endFrame); k != 0 || c.Len() != 0 {
		t.Fatalf("End: kept %d, len %d", k, c.Len())
	}
}

// Every doubt is a miss, and a miss reports what is still held (k = min(held, N)).
func TestRejoinMisses(t *testing.T) {
	c := New(Limits{})
	c.Keep(c.Rejoin(body(&wire.Start{ID: id, KnownEntries: 2}, inputFrame, sleepFrame)), suspend)
	chain := Extend([]Digest{Root(id)}, [][]byte{inputFrame, sleepFrame})
	bad := chain[1]
	bad[31] ^= 1
	for _, tc := range []struct {
		name  string
		start wire.Start
		rest  [][]byte
		held  string
	}{
		{"digest mismatch", wire.Start{KnownEntries: 2, ReplayFrom: 1, PrefixDigest: bad[:]}, [][]byte{sleepFrame}, "2:" + hex.EncodeToString(chain[2][:])},
		{"B beyond the cache", wire.Start{KnownEntries: 4, ReplayFrom: 3, PrefixDigest: chain[2][:]}, [][]byte{doneFrame}, "2:" + hex.EncodeToString(chain[2][:])},
		{"B beyond N", wire.Start{KnownEntries: 1, ReplayFrom: 2, PrefixDigest: chain[2][:]}, nil, "1:" + hex.EncodeToString(chain[1][:])},
		{"missing frame", wire.Start{KnownEntries: 3, ReplayFrom: 1, PrefixDigest: chain[1][:]}, [][]byte{sleepFrame}, "2:" + hex.EncodeToString(chain[2][:])},
		{"no digest", wire.Start{KnownEntries: 2, ReplayFrom: 1}, [][]byte{sleepFrame}, "2:" + hex.EncodeToString(chain[2][:])},
	} {
		tc.start.ID = id
		got := c.Rejoin(body(&tc.start, tc.rest...))
		if !got.Miss || got.Body != nil || got.HeldHeader() != tc.held {
			t.Errorf("%s: miss %v, held %q, want %q", tc.name, got.Miss, got.HeldHeader(), tc.held)
		}
	}
	// No entry (a restart, another replica): a miss without the held part.
	other := New(Limits{}).Rejoin(body(&wire.Start{ID: id, KnownEntries: 2, ReplayFrom: 1, PrefixDigest: chain[1][:]}, sleepFrame))
	if !other.Miss || other.HeldHeader() != "" {
		t.Fatalf("no entry: %+v", other)
	}
	// The cache off: a delta is a miss, a full replay runs and nothing is held.
	var off *Cache
	if got := off.Rejoin(body(&wire.Start{ID: id, KnownEntries: 2, ReplayFrom: 1, PrefixDigest: chain[1][:]}, sleepFrame)); !got.Miss {
		t.Fatal("a delta to a cache that is off must miss")
	}
	full := off.Rejoin(body(&wire.Start{ID: id, KnownEntries: 1}, inputFrame))
	if full.Miss || off.Keep(full, suspend) != 0 {
		t.Fatal("cache off: a full replay runs, nothing held")
	}
}

// A body it cannot read runs as it is (the turn machine answers it) and holds nothing;
// so does a full replay with fewer frames than known_entries, and an unreadable response.
func TestRejoinUnreadable(t *testing.T) {
	c := New(Limits{})
	for name, b := range map[string][]byte{
		"empty":         nil,
		"truncated":     wire.EncodeMessage(&wire.Start{ID: id}, 0)[:5],
		"not Start":     inputFrame,
		"bad Start":     append(wire.EncodeFrame(wire.TypeStart, 0, []byte{0xff}), inputFrame...),
		"short journal": body(&wire.Start{ID: id, KnownEntries: 2}, inputFrame),
	} {
		got := c.Rejoin(b)
		if got.Miss || !bytes.Equal(got.Body, b) || c.Keep(got, suspend) != 0 {
			t.Errorf("%s: %+v", name, got)
		}
	}
	ok := c.Rejoin(body(&wire.Start{ID: id, KnownEntries: 1}, inputFrame))
	if c.Keep(ok, []byte{1, 2, 3}) != 0 || c.Len() != 0 {
		t.Fatal("an unreadable response must hold nothing")
	}
}

// A held entry is a copy: it does not keep the request body alive, and writing the body
// afterwards does not change it.
func TestHeldFramesAreCopies(t *testing.T) {
	c := New(Limits{})
	b := body(&wire.Start{ID: id, KnownEntries: 1}, inputFrame)
	c.Keep(c.Rejoin(b), suspend)
	clear(b)
	h1 := Step(Root(id), inputFrame)
	got, _, ok := c.Take(string(id), 1, h1[:])
	if !ok || !bytes.Equal(got[0], inputFrame) {
		t.Fatalf("held %x", got)
	}
}

// Many turns at once, on one invocation and on many, under the race detector: every hit
// runs a body equal to the full replay, and what is held always matches its chain.
func TestConcurrentTurns(t *testing.T) {
	c := New(Limits{MaxBytes: 4 << 10})
	full := body(&wire.Start{ID: id, KnownEntries: 2}, inputFrame, sleepFrame)
	chain := Extend([]Digest{Root(id)}, [][]byte{inputFrame, sleepFrame})
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			other := []byte(fmt.Sprintf("other-%d", g))
			for i := range 200 {
				switch i % 4 {
				case 0:
					c.Keep(c.Rejoin(full), suspend)
				case 1:
					delta := body(&wire.Start{ID: id, KnownEntries: 2, ReplayFrom: 1, PrefixDigest: chain[1][:]}, sleepFrame)
					got := c.Rejoin(delta)
					if !got.Miss {
						want := body(&wire.Start{ID: id, KnownEntries: 2, ReplayFrom: 1, PrefixDigest: chain[1][:]}, inputFrame, sleepFrame)
						if !bytes.Equal(got.Body, want) {
							errs <- fmt.Errorf("a hit ran %x", got.Body)
							return
						}
						c.Keep(got, suspend)
					}
				case 2:
					c.Keep(c.Rejoin(body(&wire.Start{ID: other, KnownEntries: 1}, inputFrame)), suspend)
					if k, d, ok := c.Held(string(id), 2); ok && d != chain[k] {
						errs <- fmt.Errorf("held %d does not match its chain", k)
						return
					}
				case 3:
					if g%4 == 0 {
						c.Delete(string(id))
					}
					_ = c.Len() + c.Bytes()
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if c.Bytes() > 4<<10 {
		t.Fatalf("bytes %d over the cap", c.Bytes())
	}
}
