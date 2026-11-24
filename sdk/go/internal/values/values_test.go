// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package values

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

// secretURL is the shape of a signed read URL: its path and query are the capability.
const secretURL = "https://values.example.com/v1/seg?sig=SECRET-CAPABILITY"

// fakeStore serves segment bytes by URL, in process, and counts the requests.
type fakeStore struct {
	mu       sync.Mutex
	byURL    map[string]*served
	requests atomic.Int64
}

type served struct {
	body     []byte
	declared int64 // Content-Length; -1 = unknown
}

func (s *fakeStore) RoundTrip(r *http.Request) (*http.Response, error) {
	s.requests.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	sv, ok := s.byURL[r.URL.String()]
	if !ok {
		return &http.Response{StatusCode: 404, Body: io.NopCloser(bytes.NewReader(nil)), Request: r}, nil
	}
	return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(sv.body)), ContentLength: sv.declared, Request: r}, nil
}

// serve answers url with body (status 200, Content-Length set).
func (s *fakeStore) serve(url string, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.byURL == nil {
		s.byURL = map[string]*served{}
	}
	s.byURL[url] = &served{body: body, declared: int64(len(body))}
}

func ref(b []byte, gen uint64) wire.SegmentRef {
	sum := sha256.Sum256(b)
	return wire.SegmentRef{Sha256: sum[:], Len: uint64(len(b)), Gen: gen}
}

// completion is a GetStateCompletion frame carrying v.
func completion(id uint32, v *wire.Value) wire.Frame {
	m := &wire.GetStateCompletion{ID: id, Result: wire.Result{Value: v}}
	return wire.Frame{Type: m.Type(), Payload: wire.Marshal(m)}
}

func segmented(tail []byte, refs ...wire.SegmentRef) *wire.Value {
	n := uint64(len(tail))
	for _, r := range refs {
		n += r.Len
	}
	return &wire.Value{Content: tail, Segments: refs, Len: n}
}

func session(store http.RoundTripper, cache *Cache, origins ...string) *Session {
	return NewSession(cache, NewFetcher(store, origins), Limits{})
}

func wantError(t *testing.T, err error, fragment string) {
	t.Helper()
	var ve *Error
	if !errors.As(err, &ve) {
		t.Fatalf("error %v (%T), want a *values.Error containing %q", err, err, fragment)
	}
	if !strings.Contains(ve.Message, fragment) && !strings.Contains(ve.Detail, fragment) {
		t.Fatalf("error %q (detail %q), want it to contain %q", ve.Message, ve.Detail, fragment)
	}
	for _, s := range []string{ve.Message, ve.Detail} {
		if strings.Contains(s, "SECRET") || strings.Contains(s, "values.example.com") || strings.Contains(s, "127.0.0.1") {
			t.Fatalf("the error carries the read URL: %q", s)
		}
	}
}

// wantFetchFailed: the error is the generic reply to a failed read, with the detail kept
// for the log.
func wantFetchFailed(t *testing.T, err error, detail string) {
	t.Helper()
	wantError(t, err, detail)
	if err.Error() != "a segment could not be read (fetch)" {
		t.Fatalf("error %q, want the generic reply", err)
	}
}

func TestAssembleSegmentsAndTail(t *testing.T) {
	a, b := bytes.Repeat([]byte("a"), 40), []byte("bbbb")
	store := &fakeStore{}
	store.serve(secretURL+"&n=1", a)
	store.serve(secretURL+"&n=2", b)
	s := session(store, NewCache(1<<20))
	ra, rb := ref(a, 7), ref(b, 8)
	s.AddURLs([]wire.SegmentURL{{Sha256: ra.Sha256, Gen: 7, URL: secretURL + "&n=1"}, {Sha256: rb.Sha256, Gen: 8, URL: secretURL + "&n=2"}})
	f := completion(1, segmented([]byte("tail"), ra, rb))
	if err := s.Admit(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Of(f)
	if !ok || string(got.Bytes) != string(a)+"bbbbtail" || len(got.Refs) != 2 {
		t.Fatalf("assembled %q %v", got.Bytes, ok)
	}
	// A copy of the frame is the same frame; the frame's bytes are untouched.
	cp := wire.Frame{Type: f.Type, Payload: f.Payload}
	if _, ok := s.Of(cp); !ok {
		t.Fatal("a copy of the frame is not found")
	}
	if err := s.Admit(context.Background(), f); err != nil || store.requests.Load() != 2 {
		t.Fatalf("admitting again fetched again: %v, %d requests", err, store.requests.Load())
	}
}

func TestAssembleChecksLengthAndSha(t *testing.T) {
	good := []byte("0123456789")
	r := ref(good, 1)
	for name, tc := range map[string]struct {
		body     []byte
		declared int64
		fragment string
	}{
		"short":             {[]byte("01234"), -1, "got 5 bytes, the ref says 10"},
		"long":              {[]byte("0123456789x"), -1, "got more than the 10 bytes the ref says"},
		"long by header":    {good, 11, "got more than the 10 bytes the ref says"},
		"same length, sha":  {[]byte("9876543210"), -1, "content does not match its sha256"},
		"empty":             {nil, -1, "got 0 bytes, the ref says 10"},
		"unknown length ok": {good, -1, ""},
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{}
			store.serve(secretURL, tc.body)
			store.byURL[secretURL].declared = tc.declared
			s := session(store, NewCache(1<<20))
			s.AddURLs([]wire.SegmentURL{{Sha256: r.Sha256, Gen: 1, URL: secretURL}})
			err := s.Admit(context.Background(), completion(1, segmented(nil, r)))
			if tc.fragment == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			wantFetchFailed(t, err, tc.fragment)
		})
	}
}

func TestAssembleRefusesOriginsAndSchemes(t *testing.T) {
	seg := []byte("segment")
	r := ref(seg, 1)
	for name, tc := range map[string]struct {
		url     string
		origins []string
	}{
		"other origin":    {secretURL, []string{"https://values.other.com"}},
		"other port":      {"https://values.example.com:8443/x?sig=SECRET", []string{"https://values.example.com"}},
		"other scheme":    {"http://values.example.com/x?sig=SECRET", []string{"https://values.example.com"}},
		"file scheme":     {"file:///etc/passwd?SECRET", nil},
		"not a URL":       {"%zz SECRET", nil},
		"no host":         {"https:///x?sig=SECRET", nil},
		"gopher, no list": {"gopher://values.example.com/SECRET", nil},
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{}
			store.serve(tc.url, seg)
			var origins []string
			for _, o := range tc.origins {
				n, err := Origin(o)
				if err != nil {
					t.Fatal(err)
				}
				origins = append(origins, n)
			}
			s := session(store, NewCache(1<<20), origins...)
			s.AddURLs([]wire.SegmentURL{{Sha256: r.Sha256, Gen: 1, URL: tc.url}})
			err := s.Admit(context.Background(), completion(1, segmented(nil, r)))
			wantError(t, err, "not an allowed http(s) origin")
			if store.requests.Load() != 0 {
				t.Fatal("a refused URL was fetched")
			}
		})
	}
	// The same origin, written with its default port, is allowed.
	store := &fakeStore{}
	store.serve("https://values.example.com:443/x?sig=SECRET", seg)
	o, _ := Origin("HTTPS://Values.Example.com/")
	s := session(store, NewCache(1<<20), o)
	s.AddURLs([]wire.SegmentURL{{Sha256: r.Sha256, Gen: 1, URL: "https://values.example.com:443/x?sig=SECRET"}})
	if err := s.Admit(context.Background(), completion(1, segmented(nil, r))); err != nil {
		t.Fatal(err)
	}
}

func TestOrigin(t *testing.T) {
	for in, want := range map[string]string{
		"https://values.example.com":      "https://values.example.com",
		"https://Values.Example.com:443/": "https://values.example.com",
		"http://127.0.0.1:8080":           "http://127.0.0.1:8080",
		"http://[::1]:80":                 "http://[::1]",
	} {
		if got, err := Origin(in); err != nil || got != want {
			t.Fatalf("Origin(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"values.example.com", "ftp://x", "https://x/path", "https://x?q", "https://u@x", "https://"} {
		if _, err := Origin(bad); err == nil {
			t.Fatalf("Origin(%q) accepted", bad)
		}
	}
}

func TestAssembleOverHTTPDoesNotFollowRedirects(t *testing.T) {
	seg := []byte("segment bytes")
	r := ref(seg, 3)
	var other atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		other.Add(1)
		_, _ = w.Write(seg)
	}))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/ok":
			w.Header().Set("Content-Length", strconv.Itoa(len(seg)))
			_, _ = w.Write(seg)
		case "/moved":
			http.Redirect(w, req, target.URL+"/elsewhere", http.StatusFound)
		default:
			http.Error(w, "expired", http.StatusForbidden)
		}
	}))
	defer srv.Close()
	origin, err := Origin(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	for path, fragment := range map[string]string{"/ok": "", "/moved": "read returned HTTP 302", "/gone": "read returned HTTP 403"} {
		s := NewSession(NewCache(1<<20), NewFetcher(nil, []string{origin}), Limits{})
		s.AddURLs([]wire.SegmentURL{{Sha256: r.Sha256, Gen: 3, URL: srv.URL + path + "?sig=SECRET"}})
		err := s.Admit(context.Background(), completion(1, segmented([]byte("!"), r)))
		if fragment == "" {
			if err != nil {
				t.Fatal(err)
			}
			continue
		}
		wantFetchFailed(t, err, fragment)
	}
	if other.Load() != 0 {
		t.Fatal("a redirect was followed")
	}
	// A connection that fails names no address.
	srv.Close()
	s := NewSession(NewCache(0), NewFetcher(nil, nil), Limits{})
	s.AddURLs([]wire.SegmentURL{{Sha256: r.Sha256, Gen: 3, URL: srv.URL + "/ok?sig=SECRET"}})
	wantFetchFailed(t, s.Admit(context.Background(), completion(1, segmented(nil, r))), "fetch failed")
}

func TestAssembleLimits(t *testing.T) {
	seg := bytes.Repeat([]byte("s"), 100)
	r := ref(seg, 1)
	newSession := func(l Limits) (*Session, *fakeStore) {
		store := &fakeStore{}
		store.serve(secretURL, seg)
		s := NewSession(NewCache(1<<20), NewFetcher(store, nil), l)
		s.AddURLs([]wire.SegmentURL{{Sha256: r.Sha256, Gen: 1, URL: secretURL}})
		return s, store
	}
	// Over the value cap: refused before any fetch.
	s, store := newSession(Limits{MaxValueBytes: 100})
	wantError(t, s.Admit(context.Background(), completion(1, segmented([]byte("x"), r))), "over the 100-byte value limit")
	if store.requests.Load() != 0 {
		t.Fatal("fetched a value over the limit")
	}
	// The turn budget counts cache hits too.
	s, _ = newSession(Limits{MaxTurnValueBytes: 250})
	if err := s.Admit(context.Background(), completion(1, segmented(nil, r))); err != nil {
		t.Fatal(err)
	}
	if err := s.Admit(context.Background(), completion(2, segmented([]byte("x"), r))); err != nil {
		t.Fatal(err)
	}
	wantError(t, s.Admit(context.Background(), completion(3, segmented(nil, r))), "per-turn assembled-bytes limit")
	// A ref over the segment cap is a forged ref.
	big := wire.SegmentRef{Sha256: r.Sha256, Len: MaxSegmentBytes + 1, Gen: 1}
	s, store = newSession(Limits{MaxValueBytes: 1 << 30})
	wantError(t, s.Admit(context.Background(), completion(1, segmented(nil, big))), "segment limit")
	if store.requests.Load() != 0 {
		t.Fatal("fetched a forged ref")
	}
	// A length that is not the sum of the parts.
	s, _ = newSession(Limits{})
	v := segmented([]byte("x"), r)
	v.Len++
	wantError(t, s.Admit(context.Background(), completion(1, v)), "parts sum to")
	// A ref without a URL and not in the cache.
	s = NewSession(NewCache(1<<20), NewFetcher(&fakeStore{}, nil), Limits{})
	wantError(t, s.Admit(context.Background(), completion(1, segmented(nil, r))), "no read URL")
	// An input never carries refs.
	in := &wire.Input{Value: segmented(nil, r)}
	wantError(t, s.Admit(context.Background(), wire.Frame{Type: in.Type(), Payload: wire.Marshal(in)}), "input")
}

func TestAssembleUsesTheCacheAndFetchesEachShaOnce(t *testing.T) {
	seg := []byte("shared segment")
	r1, r2 := ref(seg, 1), ref(seg, 2) // the same bytes sealed twice
	store := &fakeStore{}
	store.serve(secretURL, seg)
	cache := NewCache(1 << 20)
	s := session(store, cache)
	s.AddURLs([]wire.SegmentURL{{Sha256: r1.Sha256, Gen: 1, URL: secretURL}, {Sha256: r2.Sha256, Gen: 2, URL: secretURL + "&b"}})
	f := completion(1, segmented(nil, r1, r2, r1))
	if err := s.Admit(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	if a, _ := s.Of(f); string(a.Bytes) != strings.Repeat(string(seg), 3) {
		t.Fatalf("assembled %q", a.Bytes)
	}
	if n := store.requests.Load(); n != 1 {
		t.Fatalf("%d requests, want 1", n)
	}
	// Another session of the same endpoint (the next turn) finds it in the cache, with no URL.
	s2 := session(&fakeStore{}, cache)
	if err := s2.Admit(context.Background(), completion(1, segmented([]byte("t"), r2))); err != nil {
		t.Fatal(err)
	}
}

func TestCacheEvictsLeastRecentlyUsed(t *testing.T) {
	c := NewCache(10)
	c.Put("a", []byte("aaaa"))
	c.Put("b", []byte("bbbb"))
	if _, ok := c.Get("a"); !ok { // a is now the most recently used
		t.Fatal("a missing")
	}
	c.Put("c", []byte("cccc")) // 12 > 10: b goes
	if _, ok := c.Get("b"); ok {
		t.Fatal("b was kept")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a was evicted")
	}
	if c.Bytes() != 8 || c.Len() != 2 {
		t.Fatalf("bytes %d len %d", c.Bytes(), c.Len())
	}
	c.Put("big", make([]byte, 11)) // larger than the cap: not held
	if _, ok := c.Get("big"); ok || c.Bytes() != 8 {
		t.Fatal("an entry over the cap was held")
	}
	c.Put("a", []byte("a")) // replacing an entry adjusts the total
	if c.Bytes() != 5 {
		t.Fatalf("bytes %d after replace", c.Bytes())
	}
	z := NewCache(0)
	z.Put("a", []byte("a"))
	if z.Len() != 0 {
		t.Fatal("a zero cache held something")
	}
}

func TestReusablePrefix(t *testing.T) {
	a, b := []byte("aaaa"), []byte("bbbb")
	known := &Assembled{Bytes: []byte("aaaabbbbtail"), Refs: []wire.SegmentRef{ref(a, 1), ref(b, 2)}}
	for next, want := range map[string]int{
		"aaaabbbbtail-more": 2,
		"aaaabbbb":          2,
		"aaaabbb":           1,
		"aaaaXbbb":          1,
		"aaa":               0,
		"Xaaabbbb":          0,
		"":                  0,
	} {
		if got := ReusablePrefix(known, []byte(next)); got != want {
			t.Fatalf("ReusablePrefix(%q) = %d, want %d", next, got, want)
		}
	}
	v := Segmented([]byte("aaaabbbbXY"), known.Refs)
	if string(v.Content) != "XY" || v.Len != 10 || len(v.Segments) != 2 {
		t.Fatalf("Segmented %+v", v)
	}
	if v := Segmented([]byte("xy"), nil); v.IsSegmented() || string(v.Content) != "xy" || v.Len != 0 {
		t.Fatalf("Segmented inline %+v", v)
	}
}

func TestReuseForPicksTheLongestPrefix(t *testing.T) {
	short, long1, long2 := []byte("0123"), []byte("abcdefgh"), []byte("ijklmnop")
	store := &fakeStore{}
	rs, r1, r2 := ref(short, 1), ref(long1, 2), ref(long2, 3)
	store.serve("https://v.example.com/s", short)
	store.serve("https://v.example.com/1", long1)
	store.serve("https://v.example.com/2", long2)
	s := session(store, NewCache(1<<20))
	s.AddURLs([]wire.SegmentURL{
		{Sha256: rs.Sha256, Gen: 1, URL: "https://v.example.com/s"},
		{Sha256: r1.Sha256, Gen: 2, URL: "https://v.example.com/1"},
		{Sha256: r2.Sha256, Gen: 3, URL: "https://v.example.com/2"},
	})
	for i, v := range []*wire.Value{segmented([]byte("!"), rs), segmented([]byte("?"), r1, r2)} {
		if err := s.Admit(context.Background(), completion(uint32(i+1), v)); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.ReuseFor([]byte("abcdefghijklmnop and more")); len(got) != 2 || got[1].Gen != 3 {
		t.Fatalf("ReuseFor %+v", got)
	}
	if got := s.ReuseFor([]byte("0123abc")); len(got) != 1 || got[0].Gen != 1 {
		t.Fatalf("ReuseFor %+v", got)
	}
	if got := s.ReuseFor([]byte("nothing")); got != nil {
		t.Fatalf("ReuseFor %+v", got)
	}
}

func TestSegmentedValueOfSkipsSignalsAndInline(t *testing.T) {
	r := ref([]byte("x"), 1)
	sig := &wire.SignalNotification{Signal: wire.SignalName("s"), Result: wire.Result{Value: segmented(nil, r)}}
	if SegmentedValueOf(wire.Frame{Type: sig.Type(), Payload: wire.Marshal(sig)}) != nil {
		t.Fatal("a signal's value is assembled")
	}
	if SegmentedValueOf(completion(1, &wire.Value{Content: []byte("inline")})) != nil {
		t.Fatal("an inline value is segmented")
	}
	eager := &wire.GetEagerState{Key: []byte("k"), Name: "k", Result: wire.Result{Value: segmented(nil, r)}}
	if SegmentedValueOf(wire.Frame{Type: eager.Type(), Payload: wire.Marshal(eager)}) == nil {
		t.Fatal("a GetEagerState value is not assembled")
	}
}

// FuzzAdmit feeds untrusted payload bytes as a completion carrying a value: assembly must
// never panic, and what it accepts has exactly the declared length and the served bytes.
func FuzzAdmit(f *testing.F) {
	seg := []byte("fuzz segment")
	r := ref(seg, 9)
	f.Add(completion(1, segmented([]byte("tail"), r)).Payload)
	f.Add(completion(1, segmented(nil, r, r)).Payload)
	f.Add(completion(1, &wire.Value{Content: []byte("x")}).Payload)
	f.Fuzz(func(t *testing.T, payload []byte) {
		store := &fakeStore{}
		store.serve("https://v.example.com/s", seg)
		s := NewSession(NewCache(1<<10), NewFetcher(store, nil), Limits{MaxValueBytes: 1 << 16})
		s.AddURLs([]wire.SegmentURL{{Sha256: r.Sha256, Gen: 9, URL: "https://v.example.com/s"}})
		fr := wire.Frame{Type: wire.TypeGetStateCompletion, Payload: payload}
		if err := s.Admit(context.Background(), fr); err != nil {
			return
		}
		v := SegmentedValueOf(fr)
		if v == nil {
			return
		}
		a, ok := s.Of(fr)
		if !ok || uint64(len(a.Bytes)) != v.Len || !bytes.HasSuffix(a.Bytes, v.Content) {
			t.Fatalf("assembled %d bytes for a declared %d", len(a.Bytes), v.Len)
		}
		for i := 0; i < len(v.Segments); i++ {
			if !bytes.Equal(a.Bytes[i*len(seg):(i+1)*len(seg)], seg) {
				t.Fatalf("segment %d is not the served bytes", i)
			}
		}
	})
}

// countingStore is a fakeStore that records how many requests run at once.
type countingStore struct {
	fakeStore
	inFlight, most atomic.Int64
}

func (c *countingStore) RoundTrip(r *http.Request) (*http.Response, error) {
	n := c.inFlight.Add(1)
	defer c.inFlight.Add(-1)
	for {
		m := c.most.Load()
		if n <= m || c.most.CompareAndSwap(m, n) {
			break
		}
	}
	return c.fakeStore.RoundTrip(r)
}

func TestAssembleManyRefsWithFewWorkers(t *testing.T) {
	store := &countingStore{}
	var refs []wire.SegmentRef
	var urls []wire.SegmentURL
	var want []byte
	for i := range 200 {
		seg := []byte("segment number " + strconv.Itoa(i))
		r := ref(seg, uint64(i))
		u := "https://v.example.com/" + strconv.Itoa(i)
		if i != 150 {
			store.serve(u, seg)
		}
		refs = append(refs, r)
		urls = append(urls, wire.SegmentURL{Sha256: r.Sha256, Gen: uint64(i), URL: u})
		want = append(want, seg...)
	}
	s := session(store, NewCache(1<<20))
	s.AddURLs(urls)
	// Segment 150 is gone: that is the error reported, not the cancellation of the others.
	wantFetchFailed(t, s.Admit(context.Background(), completion(1, segmented(nil, refs...))), "segment 150 ")
	if m := store.most.Load(); m > fetchConcurrency {
		t.Fatalf("%d requests at once, want at most %d", m, fetchConcurrency)
	}
	store.serve("https://v.example.com/150", []byte("segment number 150"))
	f := completion(2, segmented(nil, refs...))
	if err := s.Admit(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	if a, _ := s.Of(f); !bytes.Equal(a.Bytes, want) {
		t.Fatal("assembled bytes differ")
	}
}

// A failed fetch answers the same whatever the target did: the error goes back to whoever
// sent the turn, and on an endpoint without request identity that may be a stranger using
// the endpoint as a port and size scanner of its network.
//
// Must see: one message for a body of the wrong length, an HTTP error status and a closed
// port, with no status code, length or network detail in it.
func TestFetchFailuresTellNothingAboutTheTarget(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admin" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = io.WriteString(w, "internal-secret-data-of-length-38-byte")
	}))
	defer srv.Close()
	closed := httptest.NewServer(http.NotFoundHandler())
	closedURL := closed.URL
	closed.Close()
	var origins []string
	for _, u := range []string{srv.URL, closedURL} {
		o, err := Origin(u)
		if err != nil {
			t.Fatal(err)
		}
		origins = append(origins, o)
	}
	sha := make([]byte, 32)
	r := wire.SegmentRef{Sha256: sha, Len: 1 << 20, Gen: 1}
	var messages []string
	for _, u := range []string{srv.URL + "/meta", srv.URL + "/admin", closedURL + "/x"} {
		s := NewSession(NewCache(0), NewFetcher(nil, origins), Limits{})
		s.AddURLs([]wire.SegmentURL{{Sha256: sha, Gen: 1, URL: u}})
		err := s.Admit(context.Background(), completion(1, segmented(nil, r)))
		if err == nil {
			t.Fatalf("%s: assembled", u)
		}
		messages = append(messages, err.Error())
	}
	for _, m := range messages {
		if m != messages[0] || strings.Contains(m, "403") || strings.Contains(m, "38") || strings.Contains(m, "dial") {
			t.Fatalf("the messages tell the targets apart: %q", messages)
		}
	}
}

// The number of refs is the sender's choice, and every distinct ref is a GET: a value may
// carry at most 4096 refs and a turn 8192, checked before any fetch.
func TestAssembleCapsTheNumberOfRefs(t *testing.T) {
	store := &fakeStore{}
	cache := NewCache(1 << 30)
	var refs []wire.SegmentRef
	var urls []wire.SegmentURL
	for i := range 4097 {
		seg := []byte("s" + strconv.Itoa(i))
		r := ref(seg, 1)
		u := "https://v.example.com/" + strconv.Itoa(i)
		store.serve(u, seg)
		refs = append(refs, r)
		urls = append(urls, wire.SegmentURL{Sha256: r.Sha256, Gen: 1, URL: u})
	}
	s := session(store, cache)
	s.AddURLs(urls)
	// The decoder refuses such a frame (it carries no value to assemble); assembling the
	// value itself is refused too.
	if err := s.Admit(context.Background(), completion(1, segmented(nil, refs...))); err != nil {
		t.Fatal(err)
	}
	_, err := s.assemble(context.Background(), segmented(nil, refs...))
	wantError(t, err, "limit")
	if store.requests.Load() != 0 {
		t.Fatalf("%d fetches for a value over the ref limit", store.requests.Load())
	}
	// 3 x 3000 refs, every one in the cache: the third value is over the turn's refs.
	s = session(store, cache)
	s.AddURLs(urls)
	for i := uint32(1); i <= 2; i++ {
		if err := s.Admit(context.Background(), completion(i, segmented(nil, refs[:3000]...))); err != nil {
			t.Fatal(err)
		}
	}
	before := store.requests.Load()
	wantError(t, s.Admit(context.Background(), completion(3, segmented(nil, refs[:3000]...))), "limit")
	if store.requests.Load() != before {
		t.Fatal("fetched for a turn over the ref limit")
	}
}

// blockingStore holds every request until release is closed and records the most
// requests in flight at once.
type blockingStore struct {
	countingStore
	release chan struct{}
}

func (b *blockingStore) RoundTrip(r *http.Request) (*http.Response, error) {
	n := b.inFlight.Add(1)
	for {
		m := b.most.Load()
		if n <= m || b.most.CompareAndSwap(m, n) {
			break
		}
	}
	<-b.release
	b.inFlight.Add(-1)
	return b.fakeStore.RoundTrip(r)
}

// Segment fetches are capped across the process, not only per value: 16 turns at once
// with 8 refs each would otherwise open 128 requests.
//
//  1. 16 sessions each assemble a value of 8 distinct refs; the store holds every request.
//  2. The test waits until 64 requests are in flight, then for more to arrive.
//  3. The store releases them all.
//
// Must see: never more than 64 requests in flight, and every value assembled.
func TestFetchesAreCappedAcrossTheProcess(t *testing.T) {
	const sessions, perValue, limit = 16, 8, 64
	store := &blockingStore{release: make(chan struct{})}
	var wg sync.WaitGroup
	errs := make(chan error, sessions)
	for i := range sessions {
		var refs []wire.SegmentRef
		var urls []wire.SegmentURL
		for j := range perValue {
			seg := []byte("p" + strconv.Itoa(i) + "-" + strconv.Itoa(j))
			r := ref(seg, 1)
			u := "https://v.example.com/" + strconv.Itoa(i) + "/" + strconv.Itoa(j)
			store.serve(u, seg)
			refs = append(refs, r)
			urls = append(urls, wire.SegmentURL{Sha256: r.Sha256, Gen: 1, URL: u})
		}
		s := session(store, NewCache(0))
		s.AddURLs(urls)
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.Admit(context.Background(), completion(1, segmented(nil, refs...)))
		}()
	}
	deadline := time.Now().Add(10 * time.Second)
	for store.inFlight.Load() < limit && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// More requests would arrive within this window if nothing held them back.
	for range 100 {
		if store.inFlight.Load() > limit {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(store.release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if m := store.most.Load(); m != limit {
		t.Fatalf("%d requests in flight at most, want %d", m, limit)
	}
}
