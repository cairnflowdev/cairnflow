// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

// Package values is the SDK side of segmented values (wire v6, docs/protocol-v1.md
// section 9.1): a large state value or result travels as refs to immutable segments plus an
// inline tail, and the read URL of each ref travels outside the entry (`Start.segment_urls`,
// the `SegmentUrls` frame).
//
// The SDK assembles every segmented value before user code runs: segment bytes come from
// the cache (by sha256) or from their read URL, checked by length and sha256; a mismatch is
// a transient turn error, never a guess. When user code writes back a value that starts
// with the bytes of a value it read, the writer sends the refs of that prefix and only the
// rest. The raw frames are never modified: the assembled copy sits beside them.
//
// A read URL is a capability: this package never logs one and never puts one in an error
// message. It is the Go port of sdk/typescript/src/values.ts.
package values

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

// Limits and defaults, the same numbers as the other SDKs.
const (
	// WireSegmented is the first wire version with segmented values.
	WireSegmented = 6
	// DefaultCacheBytes is the default cap of a [Cache].
	DefaultCacheBytes = 64 << 20
	// MaxSegmentBytes caps one segment: the runtime seals from one frame (at most 16 MiB),
	// so a larger ref is a forged ref, refused before fetching.
	MaxSegmentBytes = 16 << 20
	// DefaultMaxValueBytes caps the logical size of one assembled value: the tenant's
	// default `max-value-bytes` and the runtime's assembly cap. A cluster that raises
	// `max-value-bytes=` raises the SDK's cap with it.
	DefaultMaxValueBytes = 64 << 20
	// DefaultMaxTurnValueBytes caps the bytes assembled in one turn (in BIDI_STREAM, one
	// stream): the sum of the logical sizes, segments found in the cache included (the
	// assembly buffer is allocated at the logical size even when nothing is fetched).
	DefaultMaxTurnValueBytes = 256 << 20
	// MaxTurnSegmentRefs caps the refs assembled in one turn (in BIDI_STREAM, one stream),
	// cache hits included: two values at the per-value cap, [wire.MaxSegmentRefs]. Every
	// distinct ref missing from the cache is a GET, and the byte budget does not bound
	// them (a ref may declare one byte). A legitimate turn stays far below: at the
	// runtime's smallest segment (60 KiB) the 256 MiB turn budget holds about 4400 refs.
	MaxTurnSegmentRefs = 2 * wire.MaxSegmentRefs
	// MaxConcurrentFetches caps the segment GETs in flight across the process, shared by
	// every turn and every endpoint: eight values fetching at once at their own cap of
	// fetchConcurrency. More requests wait for a slot.
	MaxConcurrentFetches = 8 * fetchConcurrency

	fetchConcurrency = 8
	fetchTimeout     = 60 * time.Second
)

// fetchSlots holds one token per segment GET in flight in the process.
var fetchSlots = make(chan struct{}, MaxConcurrentFetches)

// Error is a segmented value that cannot be assembled or checked: a missing read URL, an
// HTTP error, a wrong length or sha256, a value over a limit. It is a transient turn error
// (retried), not a terminal error and not a journal mismatch. Neither field ever carries a
// read URL.
type Error struct {
	// Message goes back to whoever sent the turn. When a fetch fails it says only that
	// (`a segment could not be read (fetch)`): on an endpoint without request identity the
	// sender may be a stranger probing the endpoint's network, and a status code, a body
	// length or a network error would answer the probe.
	Message string
	// Detail says what happened, for the local log only; empty when Message says it all.
	Detail string
}

func (e *Error) Error() string { return e.Message }

func errorf(format string, args ...any) *Error { return &Error{Message: fmt.Sprintf(format, args...)} }

// fetchFailed is a failed read of a segment: a generic message, and the detail for the
// local log.
func fetchFailed(format string, args ...any) *Error {
	return &Error{Message: "a segment could not be read (fetch)", Detail: fmt.Sprintf(format, args...)}
}

// ErrNotAssembled is the message of a segmented value read before it was assembled (the
// runtime sent refs on a frame the SDK does not assemble, such as a signal).
const ErrNotAssembled = "segmented value was not assembled before the turn"

// ── cache ────────────────────────────────────────────────────────────────

// Cache holds segment bytes by sha256 (hex), least recently used out first, capped by
// total bytes. Content is identified by its sha256, so `gen` is not part of the key: two
// seals of the same bytes are one entry. An entry larger than the cap is not held; a cap of
// 0 holds nothing. Safe for concurrent use; the bytes it returns must not be modified.
type Cache struct {
	mu    sync.Mutex
	max   int64
	total int64
	order *list.List // front = most recently used
	items map[string]*list.Element
}

type cacheEntry struct {
	sha   string
	bytes []byte
}

// NewCache returns an empty cache holding at most limit bytes (a negative limit is 0).
func NewCache(limit int64) *Cache {
	return &Cache{max: max(limit, 0), order: list.New(), items: map[string]*list.Element{}}
}

// Get returns the bytes cached under sha and marks them recently used.
func (c *Cache) Get(sha string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[sha]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(e)
	return e.Value.(*cacheEntry).bytes, true
}

// Put holds b (sha already checked) under sha, evicting least recently used entries until
// the total fits the cap.
func (c *Cache) Put(sha string, b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[sha]; ok {
		c.total -= int64(len(e.Value.(*cacheEntry).bytes))
		c.order.Remove(e)
		delete(c.items, sha)
	}
	if int64(len(b)) > c.max {
		return
	}
	c.items[sha] = c.order.PushFront(&cacheEntry{sha: sha, bytes: b})
	c.total += int64(len(b))
	for c.total > c.max {
		last := c.order.Back()
		e := last.Value.(*cacheEntry)
		c.order.Remove(last)
		delete(c.items, e.sha)
		c.total -= int64(len(e.bytes))
	}
}

// Bytes is the number of bytes held.
func (c *Cache) Bytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.total
}

// Len is the number of segments held.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// ── origins ──────────────────────────────────────────────────────────────

// Origin normalizes an allowed origin ("https://values.example.com"): an http or https URL
// with a host and nothing after it but an optional "/". The result is the scheme and host,
// lower case, without the scheme's default port.
func Origin(s string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(s))
	if err != nil {
		return "", fmt.Errorf("segment origin %q: %w", s, err)
	}
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.User != nil || u.Opaque != "" {
		return "", fmt.Errorf("segment origin %q must be scheme://host[:port] with nothing after it", s)
	}
	o, ok := originOf(u)
	if !ok {
		return "", fmt.Errorf("segment origin %q is not an http(s) origin", s)
	}
	return o, nil
}

// originOf is the origin of an http(s) URL with a host.
func originOf(u *url.URL) (string, bool) {
	scheme := strings.ToLower(u.Scheme)
	if (scheme != "http" && scheme != "https") || u.Hostname() == "" {
		return "", false
	}
	host := strings.ToLower(u.Hostname())
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	if p := u.Port(); p != "" && !(scheme == "https" && p == "443") && !(scheme == "http" && p == "80") {
		host += ":" + p
	}
	return scheme + "://" + host, true
}

// allowed reports whether a read URL may be fetched: http or https only, and with origins
// set, only those origins. `segment_urls` is supplied by the caller: on an endpoint that
// does not verify request identity, fetching any URL would be a blind SSRF.
func allowed(raw string, origins []string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	o, ok := originOf(u)
	if !ok {
		return false
	}
	if len(origins) == 0 {
		return true
	}
	for _, a := range origins {
		if a == o {
			return true
		}
	}
	return false
}

// ── fetching ─────────────────────────────────────────────────────────────

// Fetcher reads segments over HTTP.
type Fetcher struct {
	// Client sends the GET requests. It must not follow redirects (the origin check covers
	// the first URL only); NewFetcher builds one that does not.
	Client *http.Client
	// Origins are the allowed origins, normalized with [Origin]; empty allows any http(s)
	// origin.
	Origins []string
}

// NewFetcher returns a Fetcher sending requests through rt (http.DefaultTransport when
// nil), never following a redirect.
func NewFetcher(rt http.RoundTripper, origins []string) *Fetcher {
	if rt == nil {
		rt = http.DefaultTransport
	}
	return &Fetcher{
		Client: &http.Client{
			Transport:     rt,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		Origins: origins,
	}
}

// describe names a fetch error without its text: the errors of net/http carry the URL.
func describe(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	var op *net.OpError
	if errors.As(err, &op) {
		return "network error (" + op.Op + ")"
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return "name resolution failed"
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return "connection closed early"
	}
	for {
		u := errors.Unwrap(err)
		if u == nil {
			break
		}
		err = u
	}
	return fmt.Sprintf("%T", err)
}

// fetch reads one segment from its read URL: at most ref.Len bytes, checked by length and
// sha256. Every failure that depends on what the target answered (a status, a length, a
// checksum, a network error) is the one generic [fetchFailed] message; the checks made
// before any request keep their own message.
func (f *Fetcher) fetch(ctx context.Context, index int, ref wire.SegmentRef, raw string) ([]byte, error) {
	what := "segment " + strconv.Itoa(index) + " (sha256 " + hex.EncodeToString(ref.Sha256) + ")"
	if ref.Len > MaxSegmentBytes {
		return nil, errorf("%s: the ref declares %d bytes, over the %d-byte segment limit", what, ref.Len, MaxSegmentBytes)
	}
	if !allowed(raw, f.Origins) {
		return nil, errorf("%s: read URL is not an allowed http(s) origin (SegmentOrigins)", what)
	}
	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return nil, errorf("%s: invalid read URL", what)
	}
	res, err := f.Client.Do(req)
	if err != nil {
		return nil, fetchFailed("%s: fetch failed: %s", what, describe(err))
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return nil, fetchFailed("%s: read returned HTTP %d", what, res.StatusCode)
	}
	n := int64(ref.Len)
	if res.ContentLength > n {
		return nil, fetchFailed("%s: got more than the %d bytes the ref says", what, ref.Len)
	}
	// The buffer grows with the bytes received, not with the length the ref claims.
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, io.LimitReader(res.Body, n+1)); err != nil {
		return nil, fetchFailed("%s: reading the body failed: %s", what, describe(err))
	}
	b := buf.Bytes()
	if int64(len(b)) > n {
		return nil, fetchFailed("%s: got more than the %d bytes the ref says", what, ref.Len)
	}
	if int64(len(b)) != n {
		return nil, fetchFailed("%s: got %d bytes, the ref says %d", what, len(b), ref.Len)
	}
	sum := sha256.Sum256(b)
	if subtle.ConstantTimeCompare(sum[:], ref.Sha256) != 1 {
		return nil, fetchFailed("%s: content does not match its sha256", what)
	}
	return b, nil
}

// ── assembling ───────────────────────────────────────────────────────────

// Limits caps the assembly of one session.
type Limits struct {
	// MaxValueBytes caps the logical size of one value (DefaultMaxValueBytes when 0).
	MaxValueBytes int64
	// MaxTurnValueBytes caps the bytes assembled in the session (DefaultMaxTurnValueBytes
	// when 0).
	MaxTurnValueBytes int64
}

// Assembled is a segmented value read from the runtime: its whole bytes and its refs (the
// refs are a prefix of the bytes).
type Assembled struct {
	Bytes []byte
	Refs  []wire.SegmentRef
}

type urlKey struct {
	sha string
	gen uint64
}

// Session is the segmented-value state of one invocation on one request (REQUEST_RESPONSE)
// or one stream (BIDI_STREAM): the URL table `(sha256, gen) → url`, filled from
// `Start.segment_urls` and every `SegmentUrls` frame, and the assembled copy of each frame.
// Safe for concurrent use.
type Session struct {
	cache    *Cache
	fetcher  *Fetcher
	maxValue uint64

	mu        sync.Mutex
	urls      map[urlKey]string
	assembled map[*byte]*Assembled
	all       []*Assembled
	budget    uint64
	// refBudget is the number of refs the session may still assemble.
	refBudget int
}

// NewSession returns an empty session. A nil cache holds nothing across sessions; a nil
// fetcher cannot fetch (every segment must then be in the cache).
func NewSession(cache *Cache, fetcher *Fetcher, limits Limits) *Session {
	if cache == nil {
		cache = NewCache(0)
	}
	maxValue, budget := limits.MaxValueBytes, limits.MaxTurnValueBytes
	if maxValue <= 0 {
		maxValue = DefaultMaxValueBytes
	}
	if budget <= 0 {
		budget = DefaultMaxTurnValueBytes
	}
	return &Session{
		cache:     cache,
		fetcher:   fetcher,
		maxValue:  uint64(maxValue),
		budget:    uint64(budget),
		refBudget: MaxTurnSegmentRefs,
		urls:      map[urlKey]string{},
		assembled: map[*byte]*Assembled{},
	}
}

// AddURLs loads read URLs into the table.
func (s *Session) AddURLs(urls []wire.SegmentURL) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, u := range urls {
		s.urls[urlKey{string(u.Sha256), u.Gen}] = u.URL
	}
}

func (s *Session) url(ref wire.SegmentRef) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.urls[urlKey{string(ref.Sha256), ref.Gen}]
	return u, ok
}

// frameKey identifies a frame by its payload's first byte: the copies of a frame share
// the payload, and two frames never share a first byte. A frame carrying a segmented value
// never has an empty payload.
func frameKey(f wire.Frame) *byte {
	if len(f.Payload) == 0 {
		return nil
	}
	return &f.Payload[0]
}

// SegmentedValueOf returns the segmented value a frame carries, if any: the embedded
// result of `GetEagerState` and the result value of a completion (not of a signal, whose
// read site only takes inline values). A frame that does not decode carries none (the turn
// machine reports it).
func SegmentedValueOf(f wire.Frame) *wire.Value {
	var v *wire.Value
	switch {
	case f.Type == wire.TypeGetEagerState:
		var m wire.GetEagerState
		if m.Unmarshal(f.Payload) == nil {
			v = m.Value
		}
	case f.Type.IsNotification() && f.Type != wire.TypeSignal:
		m, err := wire.DecodeFrame(f)
		if err != nil {
			return nil
		}
		if r := resultOf(m); r != nil {
			v = r.Value
		}
	}
	if v == nil || !v.IsSegmented() {
		return nil
	}
	return v
}

func resultOf(m wire.Message) *wire.Result {
	switch n := m.(type) {
	case *wire.GetStateCompletion:
		return &n.Result
	case *wire.GetPromiseCompletion:
		return &n.Result
	case *wire.PeekPromiseCompletion:
		return &n.Result
	case *wire.CompletePromiseCompletion:
		return &n.Result
	case *wire.CallCompletion:
		return &n.Result
	case *wire.RunCompletion:
		return &n.Result
	case *wire.AttachInvocationCompletion:
		return &n.Result
	case *wire.GetInvocationOutputCompletion:
		return &n.Result
	}
	return nil
}

// Admit assembles the segmented value a frame carries, if any, before user code sees it.
// An `Input` carrying refs is refused: an input is always inline, and its tail is never
// read as the whole value.
func (s *Session) Admit(ctx context.Context, f wire.Frame) error {
	if f.Type == wire.TypeInput {
		var in wire.Input
		if in.Unmarshal(f.Payload) == nil && in.Value != nil && in.Value.IsSegmented() {
			return errorf("the input carries segment refs; an input is always inline")
		}
		return nil
	}
	v := SegmentedValueOf(f)
	if v == nil {
		return nil
	}
	k := frameKey(f)
	s.mu.Lock()
	_, done := s.assembled[k]
	s.mu.Unlock()
	if done {
		return nil
	}
	b, err := s.assemble(ctx, v)
	if err != nil {
		var ve *Error
		if errors.As(err, &ve) && ve.Detail != "" {
			// The reply says only that a read failed; the log says what happened (never
			// the URL: Detail does not carry it).
			slog.Warn("cairn: a segment of a large value could not be read; the turn will be retried",
				"detail", ve.Detail)
		}
		return err
	}
	a := &Assembled{Bytes: b, Refs: v.Segments}
	s.mu.Lock()
	s.assembled[k] = a
	s.all = append(s.all, a)
	s.mu.Unlock()
	return nil
}

// Of returns the assembled copy of a frame admitted earlier.
func (s *Session) Of(f wire.Frame) (*Assembled, bool) {
	k := frameKey(f)
	if k == nil {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.assembled[k]
	return a, ok
}

// assemble returns the whole bytes of v: its segments in order, then its content (the
// tail). Every limit is checked before any fetch and any allocation.
func (s *Session) assemble(ctx context.Context, v *wire.Value) ([]byte, error) {
	if v.Len > s.maxValue {
		return nil, errorf("segmented value declares %d bytes, over the %d-byte value limit (MaxValueBytes)", v.Len, s.maxValue)
	}
	// The number of refs is the sender's choice and each missing one is a GET: capped per
	// value (the decoder refuses more too) and per turn, before any fetch.
	if len(v.Segments) > wire.MaxSegmentRefs {
		return nil, errorf("segmented value carries %d refs, over the %d-ref value limit", len(v.Segments), wire.MaxSegmentRefs)
	}
	s.mu.Lock()
	if v.Len > s.budget {
		s.mu.Unlock()
		return nil, errorf("segmented values of this turn exceed the per-turn assembled-bytes limit (MaxTurnValueBytes)")
	}
	if len(v.Segments) > s.refBudget {
		s.mu.Unlock()
		return nil, errorf("segmented values of this turn exceed the per-turn limit of %d refs", MaxTurnSegmentRefs)
	}
	s.budget -= v.Len
	s.refBudget -= len(v.Segments)
	s.mu.Unlock()
	if err := v.Check(); err != nil {
		return nil, errorf("%s", strings.TrimPrefix(err.Error(), wire.ErrValueLayout.Error()+": "))
	}

	parts := make([][]byte, len(v.Segments))
	// The refs that miss the cache, by sha: two refs with the same bytes are fetched once.
	type job struct {
		at  []int
		ref wire.SegmentRef
		url string
	}
	var jobs []*job
	bySha := map[string]*job{}
	for i, r := range v.Segments {
		sha := hex.EncodeToString(r.Sha256)
		if hit, ok := s.cache.Get(sha); ok && uint64(len(hit)) == r.Len {
			parts[i] = hit
			continue
		}
		if j, ok := bySha[sha]; ok {
			j.at = append(j.at, i)
			continue
		}
		u, ok := s.url(r)
		if !ok {
			return nil, errorf("segment %d (sha256 %s) is not cached and has no read URL", i, sha)
		}
		j := &job{at: []int{i}, ref: r, url: u}
		bySha[sha] = j
		jobs = append(jobs, j)
	}
	if len(jobs) > 0 {
		if s.fetcher == nil {
			return nil, errorf("segment %d (sha256 %s) is not cached and fetching is off", jobs[0].at[0], hex.EncodeToString(jobs[0].ref.Sha256))
		}
		// A few workers, not one goroutine per ref: the number of refs is the sender's choice.
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		var (
			wg    sync.WaitGroup
			next  atomic.Int64
			once  sync.Once
			first error
		)
		for range min(fetchConcurrency, len(jobs)) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					n := int(next.Add(1)) - 1
					if n >= len(jobs) || ctx.Err() != nil {
						return
					}
					j := jobs[n]
					// A slot of the process-wide cap, or the end of the turn.
					select {
					case fetchSlots <- struct{}{}:
					case <-ctx.Done():
						return
					}
					b, err := s.fetcher.fetch(ctx, j.at[0], j.ref, j.url)
					<-fetchSlots
					if err != nil {
						// The failure that stops the others, not the cancellation it causes.
						once.Do(func() { first = err })
						cancel()
						return
					}
					s.cache.Put(hex.EncodeToString(j.ref.Sha256), b)
					for _, i := range j.at {
						parts[i] = b
					}
				}
			}()
		}
		wg.Wait()
		if first != nil {
			return nil, first
		}
		if err := ctx.Err(); err != nil {
			return nil, errorf("fetching segments failed: %s", describe(err))
		}
	}
	out := make([]byte, 0, v.Len)
	for _, p := range parts {
		out = append(out, p...)
	}
	return append(out, v.Content...), nil
}

// ReusablePrefix is the largest k such that next starts with the bytes of known's first k
// refs; 0 when none can be reused. Compared byte by byte: the SDK has the bytes.
func ReusablePrefix(known *Assembled, next []byte) int {
	k, off := 0, uint64(0)
	for _, r := range known.Refs {
		end := off + r.Len
		if end < off || end > uint64(len(next)) || end > uint64(len(known.Bytes)) {
			break
		}
		if !bytes.Equal(next[off:end], known.Bytes[off:end]) {
			return k
		}
		k++
		off = end
	}
	return k
}

// PrefixLen is the number of bytes the refs cover.
func PrefixLen(refs []wire.SegmentRef) uint64 {
	var n uint64
	for _, r := range refs {
		n += r.Len
	}
	return n
}

// ReuseFor returns the reusable refs for a write-once value (`Output`, a `ctx.run`
// result): the longest prefix of next matching the bytes of a segmented value assembled in
// this session. Every such value comes from a journal entry of this invocation, which is
// where the runtime accepts those refs. Nothing matches: nil (the value goes inline and the
// runtime seals it itself).
func (s *Session) ReuseFor(next []byte) []wire.SegmentRef {
	s.mu.Lock()
	all := s.all
	s.mu.Unlock()
	var best []wire.SegmentRef
	var bestLen uint64
	for _, a := range all {
		refs := a.Refs[:ReusablePrefix(a, next)]
		if n := PrefixLen(refs); n > bestLen {
			best, bestLen = refs, n
		}
	}
	return best
}

// Segmented builds the value to send for b reusing refs: refs, then the rest of b as the
// tail, with the total length. No refs: an inline value.
func Segmented(b []byte, refs []wire.SegmentRef) *wire.Value {
	if len(refs) == 0 {
		return &wire.Value{Content: b}
	}
	n := PrefixLen(refs)
	return &wire.Value{Content: b[n:], Segments: refs, Len: uint64(len(b))}
}
