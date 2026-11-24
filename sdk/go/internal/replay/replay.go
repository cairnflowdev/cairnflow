// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

// Package replay is the REQUEST_RESPONSE replay cache of wire v5
// (docs/protocol-v1.md §9.3, "Incremental replay for REQUEST_RESPONSE").
//
// In REQUEST_RESPONSE the runtime resends the whole journal every turn. With this cache
// the SDK keeps in memory the journal frames it already received for each running
// invocation; on the next turn the runtime sends only the new entries, with
// Start.replay_from = B and Start.prefix_digest = h_B. The SDK compares h_B with its own
// chain: on a match it joins the cached prefix to the body and runs the turn exactly like
// a full replay; on anything else it answers a miss (409) and the runtime resends
// everything. Correctness never depends on the cache: any doubt is a miss.
package replay

import (
	"container/list"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"sync"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

// domain separates the chain's root; it must match the runtime's.
const domain = "cairn-replay-v1"

// Digest is one link of the hash chain.
type Digest = [sha256.Size]byte

// Root is h0 = SHA-256("cairn-replay-v1" || u32be(len(id)) || id).
func Root(id []byte) Digest {
	h := sha256.New()
	h.Write([]byte(domain))
	h.Write(binary.BigEndian.AppendUint32(nil, uint32(len(id))))
	h.Write(id)
	return Digest(h.Sum(nil))
}

// Step is h(i+1) = SHA-256(h(i) || u32be(len(frame)) || frame), over the whole frame
// (8-byte header and payload).
func Step(prev Digest, frame []byte) Digest {
	h := sha256.New()
	h.Write(prev[:])
	h.Write(binary.BigEndian.AppendUint32(nil, uint32(len(frame))))
	h.Write(frame)
	return Digest(h.Sum(nil))
}

// Extend appends the links of frames to chain (currently h0 ... hk) and returns it.
func Extend(chain []Digest, frames [][]byte) []Digest {
	for _, f := range frames {
		chain = append(chain, Step(chain[len(chain)-1], f))
	}
	return chain
}

// Defaults of [Limits].
const (
	DefaultMaxBytes = 64 << 20
	DefaultTTL      = 15 * time.Minute
)

// Limits bound a [Cache]. Zero fields take the defaults.
type Limits struct {
	// MaxBytes caps the memory held (frames and chain); when full, the least recently
	// used entry is evicted. An entry larger than the cap is not held at all.
	MaxBytes int
	// TTL evicts an entry unused for longer than this.
	TTL time.Duration
}

// Cache holds the received journal of running invocations, keyed by invocation id. It is
// safe for concurrent use; the frames and chains it hands out are never written again.
type Cache struct {
	maxBytes int
	ttl      time.Duration
	// now is the monotonic clock of the TTL (tests replace it).
	now func() time.Time

	mu    sync.Mutex
	items map[string]*list.Element // of *item
	// lru: front is the most recently used.
	lru   list.List
	total int
}

type item struct {
	id string
	// frames are the whole frames of entries [0, k).
	frames [][]byte
	// chain is h0 ... hk.
	chain  []Digest
	bytes  int
	usedAt time.Time
}

// New returns an empty cache.
func New(l Limits) *Cache {
	c := &Cache{maxBytes: l.MaxBytes, ttl: l.TTL, now: time.Now, items: map[string]*list.Element{}}
	if c.maxBytes <= 0 {
		c.maxBytes = DefaultMaxBytes
	}
	if c.ttl <= 0 {
		c.ttl = DefaultTTL
	}
	return c
}

// Len is the number of invocations held.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// Bytes is the memory accounted for.
func (c *Cache) Bytes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.total
}

// live returns the entry of id, evicting it past the TTL. c.mu is held.
func (c *Cache) live(id string, now time.Time) *list.Element {
	e, ok := c.items[id]
	if !ok {
		return nil
	}
	if now.Sub(e.Value.(*item).usedAt) > c.ttl {
		c.remove(e)
		return nil
	}
	return e
}

func (c *Cache) remove(e *list.Element) {
	it := e.Value.(*item)
	c.lru.Remove(e)
	delete(c.items, it.id)
	c.total -= it.bytes
}

// Take returns the prefix [0, b) of id and its chain h0 ... hb when the cache holds at
// least b entries and hb equals digest; ok is false otherwise (a miss). The slices are
// clipped: appending to them never writes into the cache.
func (c *Cache) Take(id string, b int, digest []byte) (frames [][]byte, chain []Digest, ok bool) {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.live(id, now)
	if e == nil {
		return nil, nil, false
	}
	it := e.Value.(*item)
	if b <= 0 || len(it.frames) < b || subtle.ConstantTimeCompare(it.chain[b][:], digest) != 1 {
		return nil, nil, false
	}
	it.usedAt = now
	c.lru.MoveToFront(e)
	return it.frames[:b:b], it.chain[: b+1 : b+1], true
}

// Held is what is still held for id, reported with a miss (header cairn-replay-held):
// k = min(entries held, n) and hk, taken from the stored chain. ok is false when there is
// no entry, it is past the TTL, or k is 0. It does not change the eviction order.
func (c *Cache) Held(id string, n int) (k int, digest Digest, ok bool) {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.live(id, now)
	if e == nil {
		return 0, Digest{}, false
	}
	it := e.Value.(*item)
	k = min(len(it.frames), n)
	if k <= 0 {
		return 0, Digest{}, false
	}
	return k, it.chain[k], true
}

// Put holds entries [0, k) of id (frames) and h0 ... hk (chain), replacing what was held,
// and evicts the least recently used entries past the cap. An entry larger than the cap
// is not held: Put reports false. The caller must not write frames or chain afterwards.
func (c *Cache) Put(id string, frames [][]byte, chain []Digest) bool {
	bytes := size(frames) + len(chain)*sha256.Size + len(id)
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[id]; ok {
		c.remove(e)
	}
	if bytes > c.maxBytes {
		return false
	}
	c.items[id] = c.lru.PushFront(&item{id: id, frames: frames, chain: chain, bytes: bytes, usedAt: now})
	c.total += bytes
	for c.total > c.maxBytes {
		c.remove(c.lru.Back())
	}
	return true
}

// Delete drops what is held for id.
func (c *Cache) Delete(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[id]; ok {
		c.remove(e)
	}
}

// Turn is a request body after [Cache.Rejoin].
type Turn struct {
	// Body is what the turn machine runs: the request itself, or on a delta Start, the
	// cached prefix and the rest of the request. Nil on a miss.
	Body []byte
	// Miss: answer 409 with `cairn-replay: miss`, without running the handler.
	Miss bool
	// HeldK and HeldDigest (when HeldK > 0) go with a miss as `cairn-replay-held`.
	HeldK      int
	HeldDigest Digest

	// keep is what to hold after the turn, unless it ends the invocation.
	keep *keep
}

type keep struct {
	id     string
	frames [][]byte
	chain  []Digest
}

// HeldHeader is the value of `cairn-replay-held` for a miss: "<k>:<64 hex of hk>"; empty
// when nothing is held.
func (t Turn) HeldHeader() string {
	if !t.Miss || t.HeldK <= 0 {
		return ""
	}
	return strconv.Itoa(t.HeldK) + ":" + hex.EncodeToString(t.HeldDigest[:])
}

// rawFrames splits a body into whole frames (header and payload), aliasing body.
func rawFrames(body []byte) ([][]byte, error) {
	var out [][]byte
	for at := 0; at < len(body); {
		h, err := wire.ParseHeader(body[at:])
		if err != nil {
			return nil, err
		}
		end := at + wire.HeaderLen + int(h.Length)
		if end > len(body) {
			return nil, wire.ErrTruncated
		}
		out = append(out, body[at:end:end])
		at = end
	}
	return out, nil
}

// copyFrames copies frames into one new buffer, so a held entry does not keep the whole
// request body alive.
func copyFrames(frames [][]byte) [][]byte {
	buf := make([]byte, 0, size(frames))
	out := make([][]byte, len(frames))
	for i, f := range frames {
		at := len(buf)
		buf = append(buf, f...)
		out[i] = buf[at:len(buf):len(buf)]
	}
	return out
}

// Rejoin reads the Start of a request body (docs/protocol-v1.md §9.3). With
// replay_from = B > 0 it answers a miss unless the cache holds at least B entries of the
// invocation, hB equals prefix_digest, B <= N and the body still has at least N - B frames
// after Start; on a hit the body becomes Start || cached [0, B) || the rest of the body.
// With B = 0 the body runs as it is, and entries [0, N) are held after the turn. A body
// it cannot read runs as it is (the turn machine reports it) and holds nothing. c may be
// nil (the cache is off): then a delta Start is a miss and nothing is held.
func (c *Cache) Rejoin(body []byte) Turn {
	frames, err := rawFrames(body)
	if err != nil || len(frames) == 0 {
		return Turn{Body: body}
	}
	h, _ := wire.ParseHeader(frames[0])
	if h.Type != wire.TypeStart {
		return Turn{Body: body}
	}
	var start wire.Start
	if start.Unmarshal(frames[0][wire.HeaderLen:]) != nil {
		return Turn{Body: body}
	}
	b, n := int(start.ReplayFrom), int(start.KnownEntries)
	rest := frames[1:]
	if c == nil {
		if b > 0 {
			// A delta to an endpoint that keeps nothing: running it would run the handler
			// on a journal missing its first B entries.
			return Turn{Miss: true}
		}
		return Turn{Body: body}
	}
	id := string(start.ID)
	if b == 0 {
		// Full replay. Fewer frames than known_entries fails the turn by itself, as in v4:
		// hold nothing.
		if len(rest) < n {
			return Turn{Body: body}
		}
		entries := copyFrames(rest[:n])
		chain := make([]Digest, 1, n+1)
		chain[0] = Root(start.ID)
		chain = Extend(chain, entries)
		return Turn{Body: body, keep: &keep{id: id, frames: entries, chain: chain}}
	}
	var held [][]byte
	var heldChain []Digest
	ok := false
	if b <= n && len(rest) >= n-b {
		held, heldChain, ok = c.Take(id, b, start.PrefixDigest)
	}
	if !ok {
		t := Turn{Miss: true}
		if k, d, has := c.Held(id, n); has {
			t.HeldK, t.HeldDigest = k, d
		}
		return t
	}
	delta := copyFrames(rest[:n-b])
	entries := append(held, delta...)
	chain := Extend(heldChain, delta)
	start0 := frames[0]
	afterStart := body[len(start0):]
	joined := make([]byte, 0, len(body)+size(held))
	joined = append(joined, start0...)
	for _, f := range held {
		joined = append(joined, f...)
	}
	joined = append(joined, afterStart...)
	return Turn{Body: joined, keep: &keep{id: id, frames: entries, chain: chain}}
}

func size(frames [][]byte) int {
	n := 0
	for _, f := range frames {
		n += len(f)
	}
	return n
}

// Keep runs after the turn with its response out: a response that ends the invocation
// (End, or an Error) drops the entry; otherwise entries [0, N) are held. It returns the
// value of `cairn-replay-cache`: N when held, 0 when nothing is (no header then).
func (c *Cache) Keep(t Turn, out []byte) int {
	if c == nil || t.keep == nil {
		return 0
	}
	k := t.keep
	if ends(out) || len(k.frames) == 0 {
		c.Delete(k.id)
		return 0
	}
	if !c.Put(k.id, k.frames, k.chain) {
		return 0
	}
	return len(k.frames)
}

// ends reports whether a response carries End or Error. A response it cannot read counts
// as ending: nothing is held.
func ends(out []byte) bool {
	frames, err := wire.DecodeFrames(out)
	if err != nil {
		return true
	}
	for _, f := range frames {
		if f.Type == wire.TypeEnd || f.Type == wire.TypeError {
			return true
		}
	}
	return false
}
