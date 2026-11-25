// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

// The conformance runner: a fake runtime that replays the golden traces of
// conformance/traces into this SDK over real HTTP and compares every frame the SDK sends,
// following the runner rules of conformance/README.md. Frames are read here by hand, not
// with the SDK's codec, so the codec is not checked against itself.
//
// Every trace with a wire version below the SDK's ceiling runs twice: against the SDK as
// it is (a runtime older than the SDK) and against an SDK pinned with MaxProtocolVersion
// at the trace's version (an SDK older than the runtime).

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/identity"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

var tracesDir = filepath.Join("..", "..", "conformance", "traces")

type traceFrame struct {
	From          string `json:"from"`
	Channel       string `json:"channel"`
	Type          string `json:"type"`
	Name          string `json:"name"`
	Flags         string `json:"flags"`
	Hex           string `json:"hex"`
	Match         string `json:"match"`
	ErrorCode     *int   `json:"error_code"`
	ErrorBehavior *int   `json:"error_behavior"`
	Summary       string `json:"summary"`
}

type traceConnection struct {
	Verify bool         `json:"verify"`
	Close  string       `json:"close"`
	Status *int         `json:"status"`
	Replay *traceReplay `json:"replay"`
	Frames []traceFrame `json:"frames"`
}

// traceReplay is the `replay` field of a wire v5 connection (conformance/README.md,
// "Incremental replay").
type traceReplay struct {
	SDKRestart bool   `json:"sdk_restart"`
	Miss       bool   `json:"miss"`
	Cache      *int   `json:"cache"`
	Held       *int   `json:"held"`
	HeldDigest string `json:"held_digest"`
	StaleSDK   bool   `json:"stale_sdk"`
}

type journalEntry struct {
	Index      int    `json:"index"`
	Type       string `json:"type"`
	Name       string `json:"name"`
	PayloadHex string `json:"payload_hex"`
	Match      string `json:"match"`
}

type trace struct {
	Name                  string                         `json:"name"`
	WireVersion           int                            `json:"wire_version"`
	Mode                  string                         `json:"mode"`
	Handler               struct{ Service, Name string } `json:"handler"`
	ClockMs               int64                          `json:"clock_ms"`
	InitialJournalLen     int                            `json:"initial_journal_len"`
	SDKMaxProtocolVersion *int                           `json:"sdk_max_protocol_version"`
	Connections           []traceConnection              `json:"connections"`
	Journal               []journalEntry                 `json:"journal"`
	// Segments are the segment bytes the runner serves at their read URLs (wire v6).
	Segments []traceSegment `json:"segments"`
}

type traceSegment struct {
	URL string `json:"url"`
	Hex string `json:"hex"`
}

func loadTraces(t *testing.T) []trace {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(tracesDir, "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no traces in %s: %v", tracesDir, err)
	}
	sort.Strings(files)
	out := make([]trace, 0, len(files))
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var tr trace
		if err := json.Unmarshal(raw, &tr); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		if tr.Name+".json" != filepath.Base(f) {
			t.Fatalf("trace %s is in file %s", tr.Name, filepath.Base(f))
		}
		out = append(out, tr)
	}
	return out
}

// usesReplayCache reports whether a trace exercises the replay cache of wire v5.
func usesReplayCache(tr trace) bool {
	for _, c := range tr.Connections {
		if c.Replay != nil {
			return true
		}
	}
	return false
}

// skipReason says why this SDK does not run a trace; empty when it does.
func skipReason(tr trace) string {
	if tr.WireVersion >= 7 {
		return "wire versions above 6 are not served by this SDK"
	}
	return ""
}

// TestConformance replays every trace (conformance/README.md, "Runner rules"). The
// REQUEST_RESPONSE traces run once more through the AWS Lambda adapter, event in, result
// out, as the Python runner does.
func TestConformance(t *testing.T) {
	for _, tr := range loadTraces(t) {
		t.Run(tr.Name, func(t *testing.T) {
			if why := skipReason(tr); why != "" {
				t.Skip(why)
			}
			pins := []int{0}
			if pin := tr.SDKMaxProtocolVersion; pin != nil {
				pins = []int{*pin}
			} else if tr.WireVersion < servedProtocolVersion {
				pins = append(pins, tr.WireVersion)
			}
			for i, pin := range pins {
				name := ""
				if i > 0 {
					name = "pinned"
				}
				if name == "" {
					runTrace(t, tr, pin, false)
				} else {
					t.Run(name, func(t *testing.T) { runTrace(t, tr, pin, false) })
				}
				// The replay cache is off on Lambda (a turn may land on a fresh instance), so
				// the incremental replay traces do not apply there.
				if tr.Mode == "REQUEST_RESPONSE" && !usesReplayCache(tr) {
					t.Run(strings.TrimPrefix(name+"/lambda", "/"), func(t *testing.T) { runTrace(t, tr, pin, true) })
				}
			}
		})
	}
}

// lambdaTransport turns each runner request into a Lambda event (payload format 2.0,
// base64 body) for the SDK's LambdaHandler, and its result back into a response.
type lambdaTransport struct{ h LambdaHandlerFunc }

func (l lambdaTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	var body []byte
	if r.Body != nil {
		b, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			return nil, err
		}
		body = b
	}
	ev := LambdaEvent{
		RawPath:         r.URL.EscapedPath(),
		RawQueryString:  r.URL.RawQuery,
		Headers:         map[string]string{"host": r.URL.Host},
		Body:            base64.StdEncoding.EncodeToString(body),
		IsBase64Encoded: true,
	}
	ev.RequestContext.HTTP.Method = r.Method
	for k, vs := range r.Header {
		ev.Headers[strings.ToLower(k)] = strings.Join(vs, ",")
	}
	// Through JSON, as Lambda delivers it.
	raw, err := json.Marshal(ev)
	if err != nil {
		return nil, err
	}
	var in LambdaEvent
	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, err
	}
	res, err := l.h(r.Context(), in)
	if err != nil {
		return nil, err
	}
	if !res.IsBase64Encoded {
		return nil, fmt.Errorf("the Lambda result body is not base64")
	}
	out, err := base64.StdEncoding.DecodeString(res.Body)
	if err != nil {
		return nil, err
	}
	h := http.Header{}
	for k, v := range res.Headers {
		h.Set(k, v)
	}
	return &http.Response{
		StatusCode:    res.StatusCode,
		Header:        h,
		Body:          io.NopCloser(bytes.NewReader(out)),
		ContentLength: int64(len(out)),
		Request:       r,
	}, nil
}

// signingTransport signs each runner request with request identity (docs/protocol-v1.md
// section 4), as the runtime does: aud is the path, jti binds the request to the Start of
// its body (the SDK's codec reads the Start here only to sign; it checks nothing the SDK
// sends), bsh to the body.
type signingTransport struct {
	base http.RoundTripper
	key  signer
}

func (s signingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	var body []byte
	jti := identity.DiscoverJTI
	if r.Body != nil {
		b, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			return nil, err
		}
		body = b
		frames, err := wire.DecodeFrames(body)
		if err != nil || len(frames) == 0 {
			return nil, fmt.Errorf("signing: the body has no Start: %v", err)
		}
		var st wire.Start
		if err := st.Unmarshal(frames[0].Payload); err != nil {
			return nil, err
		}
		jti = identity.ExpectedJTI(st.ID, st.RetryCountSinceLastStoredEntry)
	}
	r = r.Clone(r.Context())
	if body != nil {
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
	}
	for k, v := range s.key.headers(nil, s.key.jwt(r.URL.EscapedPath(), jti, body, "")) {
		r.Header.Set(k, v)
	}
	return s.base.RoundTrip(r)
}

// conformanceSegmentOrigin is the origin of every read URL of the traces
// (conformance/README.md, "Wire v6").
const conformanceSegmentOrigin = "https://segments.conformance.invalid"

// segmentServer serves the segments of a trace at their read URLs, in process: the SDK's
// segment downloads never reach the network (conformance/README.md, "Wire v6").
type segmentServer struct {
	t       *testing.T
	byURL   map[string][]byte
	fetches atomic.Int64
}

func newSegmentServer(t *testing.T, tr trace) *segmentServer {
	s := &segmentServer{t: t, byURL: map[string][]byte{}}
	for _, seg := range tr.Segments {
		s.byURL[seg.URL] = unhex(t, seg.Hex)
	}
	return s
}

func (s *segmentServer) RoundTrip(r *http.Request) (*http.Response, error) {
	b, ok := s.byURL[r.URL.String()]
	if !ok || r.Method != http.MethodGet {
		s.t.Errorf("the SDK fetched %s %s, which the trace does not serve", r.Method, r.URL)
		return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(bytes.NewReader(nil)), Request: r}, nil
	}
	s.fetches.Add(1)
	return &http.Response{
		StatusCode:    http.StatusOK,
		Body:          io.NopCloser(bytes.NewReader(b)),
		ContentLength: int64(len(b)),
		Request:       r,
	}, nil
}

// runTrace runs one trace against a fresh endpoint; pin > 0 sets MaxProtocolVersion;
// lambda serves it through LambdaHandler instead of an HTTP server.
func runTrace(t *testing.T, tr trace, pin int, lambda bool) {
	before := runtime.NumGoroutine()
	segments := newSegmentServer(t, tr)
	// A fresh endpoint has its own segment cache, so every v6 trace really downloads.
	// The segments' origin is listed: the runner sends no request identity, and an
	// unverified endpoint serves wire v6 only with SegmentOrigins.
	opts := []ServeOption{conformance(), withClock(func() int64 { return tr.ClockMs }), WithSegmentTransport(segments),
		SegmentOrigins(conformanceSegmentOrigin)}
	if pin > 0 {
		opts = append(opts, MaxProtocolVersion(pin))
	}
	bidiMode := tr.Mode == "BIDI_STREAM"
	if bidiMode {
		opts = append(opts, Bidi())
	}
	// fresh starts another replica of the SDK: the replay cache of wire v5 lives in it.
	var servers []*httptest.Server
	fresh := func() string {
		srv := httptest.NewUnstartedServer(HTTPHandler(opts...))
		srv.Config.Protocols = serverProtocols()
		srv.Start()
		servers = append(servers, srv)
		return srv.URL
	}
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	if usesReplayCache(tr) {
		// The replay cache is only kept by an endpoint that verifies request identity: the
		// runner signs every request as the runtime does.
		key := newSigner(t)
		opts = append(opts, IdentityKeys(key.kid))
		client.Transport = signingTransport{base: client.Transport, key: key}
	}
	var base string
	if lambda {
		h, err := LambdaHandler(opts...)
		if err != nil {
			t.Fatal(err)
		}
		// A mount prefix, as an API Gateway stage adds one.
		client = &http.Client{Transport: lambdaTransport{h}}
		base = "https://fn.lambda-url.invalid/stage"
	}
	// h2c with prior knowledge, no HTTP/1.1 (conformance/README.md, runner rule 3).
	var h2c http.Protocols
	h2c.SetUnencryptedHTTP2(true)
	streams := &http.Client{Transport: &http.Transport{Protocols: &h2c, DisableCompression: true}}
	release := armSlow()
	defer func() {
		release()
		client.CloseIdleConnections()
		streams.CloseIdleConnections()
		for _, srv := range servers {
			srv.Close()
		}
		noLeak(t, before)
	}()
	if !lambda {
		base = fresh()
	}
	negotiate(t, client, base, tr)
	var sent [][]byte
	for i, c := range tr.Connections {
		at := fmt.Sprintf("connection %d", i)
		if c.Verify {
			at += " (verify)"
		}
		if c.Replay != nil && c.Replay.SDKRestart {
			// The SDK restarts and loses its replay cache.
			base = fresh()
		}
		if c.Replay != nil && c.Replay.StaleSDK {
			// A second replica that received every earlier connection except the last
			// one answered 200: the stale replica behind a load balancer without affinity.
			base = fresh()
			last := -1
			for j := range i {
				if r := tr.Connections[j].Replay; r == nil || !r.Miss {
					last = j
				}
			}
			for j := range i {
				if r := tr.Connections[j].Replay; j == last || (r != nil && r.Miss) {
					continue
				}
				requestResponse(t, client, base, tr, tr.Connections[j], fmt.Sprintf("stale replica, connection %d", j))
			}
		}
		var got []rawFrame
		if bidiMode {
			got = bidiStream(t, streams, base, tr, c, at, release)
		} else {
			got = requestResponse(t, client, base, tr, c, at)
		}
		if !c.Verify {
			for _, f := range got {
				if f.typ >= 0x0400 && f.typ < 0x8000 {
					sent = append(sent, f.bytes)
				}
			}
		}
	}
	checkJournal(t, tr, sent)
	if len(tr.Segments) > 0 && segments.fetches.Load() == 0 {
		t.Fatalf("no segment download at all")
	}
}

// checkJournal: the commands the SDK sent outside verify connections, in order, are the
// command part of the journal after its durable prefix (conformance/README.md, rule 6).
func checkJournal(t *testing.T, tr trace, sent [][]byte) {
	t.Helper()
	var want []journalEntry
	for _, e := range tr.Journal[min(tr.InitialJournalLen, len(tr.Journal)):] {
		if strings.HasPrefix(e.Type, "0x04") {
			want = append(want, e)
		}
	}
	if len(sent) != len(want) {
		t.Fatalf("the SDK sent %d commands, the journal has %d after its first %d entries", len(sent), len(want), tr.InitialJournalLen)
	}
	for i, f := range sent {
		e := want[i]
		typ := fmt.Sprintf("0x%04x", binary.BigEndian.Uint16(f[:2]))
		if typ != e.Type || ((e.Match == "" || e.Match == "bytes") && hex.EncodeToString(f[8:]) != e.PayloadHex) {
			t.Fatalf("command %d: the SDK sent %x, the journal has entry %d %s %s", i, f, e.Index, e.Type, e.PayloadHex)
		}
	}
}

// negotiate: the runner speaks [1, wire_version]; the chosen version must be the trace's
// (conformance/README.md, runner rule 1).
func negotiate(t *testing.T, client *http.Client, base string, tr trace) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, base+"/discover", nil)
	req.Header.Set("Accept", discoveryV1)
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 || res.Header.Get("Content-Type") != discoveryV1 {
		t.Fatalf("discover: %d %s", res.StatusCode, res.Header.Get("Content-Type"))
	}
	var m struct {
		ProtocolMode       string `json:"protocolMode"`
		MinProtocolVersion int    `json:"minProtocolVersion"`
		MaxProtocolVersion int    `json:"maxProtocolVersion"`
	}
	if err := json.NewDecoder(res.Body).Decode(&m); err != nil {
		t.Fatal(err)
	}
	if m.ProtocolMode != tr.Mode {
		t.Fatalf("protocolMode %s, the trace is %s", m.ProtocolMode, tr.Mode)
	}
	if pin := tr.SDKMaxProtocolVersion; pin != nil {
		if m.MaxProtocolVersion != *pin {
			t.Fatalf("maxProtocolVersion %d, the trace pins the SDK at %d", m.MaxProtocolVersion, *pin)
		}
		return
	}
	chosen := min(m.MaxProtocolVersion, tr.WireVersion)
	if chosen < max(1, m.MinProtocolVersion) || chosen != tr.WireVersion {
		t.Fatalf("negotiated v%d (SDK [%d, %d]), the trace is v%d", chosen, m.MinProtocolVersion, m.MaxProtocolVersion, tr.WireVersion)
	}
}

var invocationContentType = regexp.MustCompile(`^application/vnd\.cairnflow\.invocation\.v(\d+)$`)

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// requestResponse sends every runtime frame of the connection as one body and compares
// the response with the SDK frames of the trace.
func requestResponse(t *testing.T, client *http.Client, base string, tr trace, c traceConnection, at string) []rawFrame {
	t.Helper()
	var body []byte
	for _, f := range c.Frames {
		if f.From == "runtime" {
			body = append(body, unhex(t, f.Hex)...)
		}
	}
	ct := "application/vnd.cairnflow.invocation.v" + strconv.Itoa(tr.WireVersion)
	req, _ := http.NewRequest(http.MethodPost, base+"/invoke/"+tr.Handler.Service+"/"+tr.Handler.Name, bytes.NewReader(body))
	req.Header.Set("Content-Type", ct)
	req.Header.Set("Accept", ct)
	res, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s: %v", at, err)
	}
	got, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		t.Fatalf("%s: %v", at, err)
	}
	if c.Status != nil {
		// Version too new: the SDK refuses, no frames.
		if res.StatusCode != *c.Status {
			t.Fatalf("%s: status %d, want %d (%s)", at, res.StatusCode, *c.Status, got)
		}
		if invocationContentType.MatchString(res.Header.Get("Content-Type")) {
			t.Fatalf("%s: a refusal must not carry frames (content-type %s)", at, res.Header.Get("Content-Type"))
		}
		return nil
	}
	if r := c.Replay; r != nil && r.Miss {
		// Wire v5 miss: 409, `cairn-replay: miss`, an empty body, and with round 2 the part
		// still held.
		if res.StatusCode != http.StatusConflict || res.Header.Get("cairn-replay") != "miss" || len(got) != 0 {
			t.Fatalf("%s: must miss: status %d, cairn-replay %q, %d body bytes", at, res.StatusCode, res.Header.Get("cairn-replay"), len(got))
		}
		if r.Held != nil {
			if want := fmt.Sprintf("%d:%s", *r.Held, r.HeldDigest); res.Header.Get("cairn-replay-held") != want {
				t.Fatalf("%s: cairn-replay-held %q, want %q", at, res.Header.Get("cairn-replay-held"), want)
			}
		}
		return nil
	}
	if res.StatusCode != 200 {
		t.Fatalf("%s: status %d: %s", at, res.StatusCode, got)
	}
	if r := c.Replay; r != nil {
		// The `cairn-replay-cache` header exactly as the trace records it; absent there,
		// absent here.
		v, has := res.Header["Cairn-Replay-Cache"]
		switch {
		case r.Cache == nil && has:
			t.Fatalf("%s: cairn-replay-cache %q, the trace has none", at, v)
		case r.Cache != nil && (!has || len(v) != 1 || v[0] != strconv.Itoa(*r.Cache)):
			t.Fatalf("%s: cairn-replay-cache %q, want %d", at, v, *r.Cache)
		}
	}
	m := invocationContentType.FindStringSubmatch(strings.TrimSpace(strings.SplitN(res.Header.Get("Content-Type"), ";", 2)[0]))
	if m == nil {
		t.Fatalf("%s: response content-type %q", at, res.Header.Get("Content-Type"))
	}
	if v, _ := strconv.Atoi(m[1]); v > tr.WireVersion {
		t.Fatalf("%s: response content-type %q is above v%d", at, res.Header.Get("Content-Type"), tr.WireVersion)
	}
	frames, rest := splitFrames(got)
	if len(rest) != 0 {
		t.Fatalf("%s: %d bytes after the last whole frame", at, len(rest))
	}
	all := frames
	next := func(string) (rawFrame, bool) {
		if len(frames) == 0 {
			return rawFrame{}, false
		}
		f := frames[0]
		frames = frames[1:]
		return f, true
	}
	for _, want := range c.Frames {
		if want.From == "sdk" {
			expectSDK(t, next, want, at)
		}
	}
	noJournalFrame(t, frames, at+": the SDK sent an extra frame")
	return all
}

// expectSDK reads the next SDK frame and compares it with want; a side-channel frame the
// trace does not list (the number of Keepalives depends on timing) is skipped
// (conformance/README.md, runner rule 5).
func expectSDK(t *testing.T, next func(what string) (rawFrame, bool), want traceFrame, at string) rawFrame {
	t.Helper()
	for {
		f, ok := next(want.Name)
		if !ok {
			t.Fatalf("%s: the SDK closed, the trace still expects %s: %s", at, want.Name, want.Summary)
		}
		why := frameMismatch(want, f)
		if why == "" {
			return f
		}
		wantType, _ := strconv.ParseUint(strings.TrimPrefix(want.Type, "0x"), 16, 16)
		if isSide(f.typ) && !(want.Channel == "side" && uint64(f.typ) == wantType) {
			continue // a side frame the trace does not list
		}
		t.Fatalf("%s: %s\n  trace: %s %s\n         %s\n  SDK:   %s", at, why, want.Name, want.Hex, want.Summary, f)
	}
}

// noJournalFrame fails on any frame that is not on the side channel.
func noJournalFrame(t *testing.T, frames []rawFrame, what string) {
	t.Helper()
	for _, f := range frames {
		if !isSide(f.typ) {
			t.Fatalf("%s: %s", what, f)
		}
	}
}

// sdkSilence bounds every wait of the BIDI runner for the SDK.
const sdkSilence = 5 * time.Second

// bidiStream plays one connection over an h2c stream (conformance/README.md, runner rule
// 3): a run of consecutive runtime frames is one write; at each SDK frame of the trace the
// runner reads the next frame the SDK sends. At the end, close "runtime" means the runner
// ends its side and the SDK must end too; "sdk" means the SDK ends on its own. Either way
// nothing but side-channel frames may follow. It returns every frame the SDK sent.
func bidiStream(t *testing.T, client *http.Client, base string, tr trace, c traceConnection, at string, release func()) []rawFrame {
	t.Helper()
	pr, pw := io.Pipe()
	ct := "application/vnd.cairnflow.invocation.v" + strconv.Itoa(tr.WireVersion)
	req, _ := http.NewRequest(http.MethodPost, base+"/invoke/"+tr.Handler.Service+"/"+tr.Handler.Name, pr)
	req.Header.Set("Content-Type", ct)
	req.Header.Set("Accept", ct)
	type response struct {
		res *http.Response
		err error
	}
	responses := make(chan response, 1)
	go func() {
		res, err := client.Do(req)
		responses <- response{res, err}
	}()

	// The runtime's writes go through their own goroutine: a write blocked by a
	// misbehaving SDK never blocks the runner past its deadlines.
	writes := make(chan []byte, len(c.Frames))
	written := make(chan struct{})
	go func() {
		defer close(written)
		for b := range writes {
			if _, err := pw.Write(b); err != nil {
				break
			}
		}
		_ = pw.Close() // END_STREAM
		for range writes {
		}
	}()
	ended := false
	end := func() {
		if !ended {
			ended = true
			close(writes)
		}
	}

	var res *http.Response
	frames := make(chan rawFrame, 64)
	var readErr error
	readDone := make(chan struct{})
	defer func() {
		end()
		_ = pw.CloseWithError(io.ErrClosedPipe)
		<-written
		if res == nil {
			select {
			case r := <-responses:
				res = r.res
			case <-time.After(sdkSilence):
			}
			if res != nil {
				res.Body.Close()
			}
			return
		}
		res.Body.Close()
		<-readDone
	}()
	open := func() *http.Response {
		if res != nil {
			return res
		}
		select {
		case r := <-responses:
			if r.err != nil {
				t.Fatalf("%s: %v", at, r.err)
			}
			res = r.res
		case <-time.After(sdkSilence):
			t.Fatalf("%s: no response headers after %s", at, sdkSilence)
		}
		go func() {
			defer close(readDone)
			defer close(frames)
			var hdr [8]byte
			for {
				if _, err := io.ReadFull(res.Body, hdr[:]); err != nil {
					if err != io.EOF {
						readErr = err
					}
					return
				}
				b := make([]byte, 8+int(binary.BigEndian.Uint32(hdr[4:8])))
				copy(b, hdr[:])
				if _, err := io.ReadFull(res.Body, b[8:]); err != nil {
					readErr = fmt.Errorf("half a frame left at the end of the SDK stream: %w", err)
					return
				}
				frames <- rawFrame{typ: binary.BigEndian.Uint16(b[0:2]), flags: binary.BigEndian.Uint16(b[2:4]), bytes: b}
			}
		}()
		return res
	}
	var all []rawFrame
	next := func(what string) (rawFrame, bool) {
		select {
		case f, ok := <-frames:
			if ok {
				all = append(all, f)
			}
			return f, ok
		case <-time.After(sdkSilence):
			t.Fatalf("%s: the SDK was silent for %s while the runner waits for %s", at, sdkSilence, what)
			return rawFrame{}, false
		}
	}
	rest := func() []rawFrame {
		var out []rawFrame
		for {
			f, ok := next("the SDK closing its side")
			if !ok {
				<-readDone
				if readErr != nil {
					t.Fatalf("%s: %v", at, readErr)
				}
				return out
			}
			out = append(out, f)
		}
	}

	if c.Status != nil {
		// Version too new: the SDK refuses before reading the body, no frames.
		for _, f := range c.Frames {
			if f.From == "runtime" {
				writes <- unhex(t, f.Hex)
			}
		}
		select {
		case r := <-responses:
			if r.err != nil {
				t.Fatalf("%s: %v", at, r.err)
			}
			res = r.res
		case <-time.After(sdkSilence):
			t.Fatalf("%s: no response headers after %s", at, sdkSilence)
		}
		close(readDone)
		if res.StatusCode != *c.Status {
			t.Fatalf("%s: status %d, want %d", at, res.StatusCode, *c.Status)
		}
		// The body is a JSON error, not frames.
		if invocationContentType.MatchString(strings.TrimSpace(strings.SplitN(res.Header.Get("Content-Type"), ";", 2)[0])) {
			t.Fatalf("%s: a refusal must not carry frames (content-type %s)", at, res.Header.Get("Content-Type"))
		}
		end()
		_, _ = io.Copy(io.Discard, res.Body)
		return nil
	}

	var batch []byte
	for _, f := range c.Frames {
		if f.From == "runtime" {
			batch = append(batch, unhex(t, f.Hex)...)
			continue
		}
		if len(batch) > 0 {
			writes <- batch
			batch = nil
		}
		open()
		got := expectSDK(t, next, f, at)
		// slowRun: the closure is held until the first Keepalive arrives.
		if got.typ == 0x0008 {
			release()
		}
	}
	if len(batch) > 0 {
		writes <- batch
	}
	r := open()
	if r.StatusCode != http.StatusOK {
		t.Fatalf("%s: status %d", at, r.StatusCode)
	}
	m := invocationContentType.FindStringSubmatch(strings.TrimSpace(strings.SplitN(r.Header.Get("Content-Type"), ";", 2)[0]))
	if m == nil {
		t.Fatalf("%s: response content-type %q", at, r.Header.Get("Content-Type"))
	}
	if v, _ := strconv.Atoi(m[1]); v > tr.WireVersion {
		t.Fatalf("%s: response content-type %q is above v%d", at, r.Header.Get("Content-Type"), tr.WireVersion)
	}
	switch c.Close {
	case "runtime":
		// The runtime ends its side: the SDK must end too, without another journal frame.
		end()
		noJournalFrame(t, rest(), at+": the SDK kept sending after the runtime closed")
	default:
		noJournalFrame(t, rest(), at+": the SDK sent an extra frame")
		end()
	}
	return all
}

type rawFrame struct {
	typ, flags uint16
	bytes      []byte // the whole frame
}

func (f rawFrame) String() string {
	return fmt.Sprintf("0x%04x flags=0x%x %x", f.typ, f.flags, f.bytes)
}

// splitFrames reads 8-byte big-endian headers (type u16, flags u16, length u32).
func splitFrames(b []byte) ([]rawFrame, []byte) {
	var out []rawFrame
	for len(b) >= 8 {
		n := int(binary.BigEndian.Uint32(b[4:8]))
		if len(b) < 8+n {
			break
		}
		out = append(out, rawFrame{typ: binary.BigEndian.Uint16(b[0:2]), flags: binary.BigEndian.Uint16(b[2:4]), bytes: b[:8+n]})
		b = b[8+n:]
	}
	return out, b
}

// isSide: Keepalive, StreamAppend, ArtifactPut and the custom range (docs/protocol-v1.md
// section 9.2).
func isSide(typ uint16) bool {
	return typ == 0x0008 || typ == 0x000b || typ == 0x000c || typ >= 0xfc00
}

// varintField reads the varint of top-level field n of a protobuf payload (0 if absent).
func varintField(p []byte, n int) uint64 {
	var found uint64
	i := 0
	varint := func() uint64 {
		var v uint64
		for shift := uint(0); i < len(p); shift += 7 {
			c := p[i]
			i++
			v |= uint64(c&0x7f) << shift
			if c < 0x80 {
				break
			}
		}
		return v
	}
	for i < len(p) {
		key := varint()
		switch key & 7 {
		case 0:
			if v := varint(); int(key>>3) == n {
				found = v
			}
		case 1:
			i += 8
		case 2:
			l := int(varint())
			i += l
		case 5:
			i += 4
		default:
			return found
		}
	}
	return found
}

// frameMismatch compares an SDK frame with the trace per its match; empty means equal.
func frameMismatch(want traceFrame, got rawFrame) string {
	head := hex.EncodeToString(got.bytes[:4])
	switch want.Match {
	case "", "bytes":
		if hex.EncodeToString(got.bytes) != want.Hex {
			return "bytes differ"
		}
	case "type":
		if head != want.Hex[:8] {
			return "type/flags differ"
		}
	case "error":
		if head != want.Hex[:8] {
			return "type/flags differ"
		}
		code, behavior := varintField(got.bytes[8:], 1), varintField(got.bytes[8:], 9)
		if want.ErrorCode == nil || want.ErrorBehavior == nil || code != uint64(*want.ErrorCode) || behavior != uint64(*want.ErrorBehavior) {
			return fmt.Sprintf("ErrorMessage code=%d behavior=%d, the trace demands code=%v behavior=%v", code, behavior,
				deref(want.ErrorCode), deref(want.ErrorBehavior))
		}
	default:
		return "unknown match " + want.Match
	}
	return ""
}

func deref(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}

// TestConformanceTracesAreConsistent: the commands the SDK sends outside verify
// connections are exactly the command part of the journal after its durable prefix, and
// the verify connection replays the whole journal (conformance/README.md, rule 6).
func TestConformanceTracesAreConsistent(t *testing.T) {
	for _, tr := range loadTraces(t) {
		var sent []traceFrame
		for _, c := range tr.Connections {
			if c.Verify {
				continue
			}
			for _, f := range c.Frames {
				if f.From == "sdk" && strings.HasPrefix(f.Type, "0x04") {
					sent = append(sent, f)
				}
			}
		}
		var commands []journalEntry
		for _, e := range tr.Journal[min(tr.InitialJournalLen, len(tr.Journal)):] {
			if strings.HasPrefix(e.Type, "0x04") {
				commands = append(commands, e)
			}
		}
		if len(sent) != len(commands) {
			t.Fatalf("%s: %d commands sent, %d in the journal", tr.Name, len(sent), len(commands))
		}
		for i, f := range sent {
			e := commands[i]
			if f.Type != e.Type || ((e.Match == "" || e.Match == "bytes") && f.Hex[16:] != e.PayloadHex) {
				t.Fatalf("%s: command %d differs from the journal", tr.Name, i)
			}
		}
		for _, c := range tr.Connections {
			if !c.Verify {
				continue
			}
			var replayed []string
			for _, f := range c.Frames {
				if f.From == "runtime" {
					replayed = append(replayed, f.Hex[16:])
				}
			}
			var journal []string
			for _, e := range tr.Journal {
				journal = append(journal, e.PayloadHex)
			}
			if len(replayed) == 0 || !slices.Equal(replayed[1:], journal) {
				t.Fatalf("%s: the verify connection does not replay the journal exactly", tr.Name)
			}
		}
	}
}
