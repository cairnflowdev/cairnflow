// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/identity"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

// startBody is a REQUEST_RESPONSE body: Start (id, retry count) then Input.
func startBody(id []byte, retry uint32) []byte {
	b := wire.EncodeMessage(&wire.Start{ID: id, KnownEntries: 1, RetryCountSinceLastStoredEntry: retry}, 0)
	return wire.AppendMessage(b, input(), 0)
}

var echoService = NewService("Echo",
	Handler("hello", NoInput(func(Context) (string, error) { return "hi", nil })),
	Handler("big", NoInput(func(Context) (string, error) { return strings.Repeat("x", 20_000), nil })),
)

type testServer struct {
	*httptest.Server
	client *http.Client
}

func newTestServer(t *testing.T, h http.Handler) *testServer {
	t.Helper()
	s := &testServer{Server: httptest.NewServer(h), client: &http.Client{Transport: &http.Transport{DisableCompression: true}}}
	t.Cleanup(func() {
		s.client.CloseIdleConnections()
		s.Close()
	})
	return s
}

func (s *testServer) do(t *testing.T, method, path string, body []byte, header map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, s.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	res, err := s.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res, b
}

func invokeHeaders(v int) map[string]string {
	ct := "application/vnd.cairnflow.invocation.v" + string(rune('0'+v))
	return map[string]string{"Content-Type": ct, "Accept": ct}
}

func manifestOf(t *testing.T, s *testServer) map[string]any {
	t.Helper()
	res, b := s.do(t, http.MethodGet, "/discover", nil, map[string]string{"Accept": discoveryV1})
	if res.StatusCode != 200 || res.Header.Get("Content-Type") != discoveryV1 {
		t.Fatalf("discover: %d %s %s", res.StatusCode, res.Header.Get("Content-Type"), b)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestEndpointDiscoverAndRoutes(t *testing.T) {
	// No IdentityKeys and no SegmentOrigins: wire v6 is not served (no segment downloads).
	if m := manifestOf(t, newTestServer(t, HTTPHandler(echoService))); m["maxProtocolVersion"] != float64(5) {
		t.Fatalf("unverified manifest %v", m)
	}
	s := newTestServer(t, HTTPHandler(echoService, SegmentOrigins("https://values.example.com")))
	m := manifestOf(t, s)
	if m["protocolMode"] != "REQUEST_RESPONSE" || m["maxProtocolVersion"] != float64(servedProtocolVersion) ||
		m["requestCompression"] != "gzip" || m["requestIdentity"] != nil {
		t.Fatalf("manifest %v", m)
	}
	pinned := newTestServer(t, HTTPHandler(echoService, MaxProtocolVersion(2), WithoutCompression()))
	if m := manifestOf(t, pinned); m["maxProtocolVersion"] != float64(2) || m["requestCompression"] != nil {
		t.Fatalf("pinned manifest %v", m)
	}

	res, b := s.do(t, http.MethodPost, "/invoke/Echo/hello", startBody([]byte{1}, 0), invokeHeaders(1))
	if res.StatusCode != 200 || res.Header.Get("Content-Type") != invocationV1 {
		t.Fatalf("invoke: %d %s", res.StatusCode, b)
	}
	if got := kinds(decodeFrames(t, b)); got != "Output End" {
		t.Fatalf("frames %s", got)
	}
	for _, c := range []struct{ method, path string }{
		{http.MethodPost, "/invoke/Echo/nope"},
		{http.MethodPost, "/invoke/Nope/hello"},
		{http.MethodGet, "/invoke/Echo/hello"},
		{http.MethodPost, "/invoke/Echo/hello/extra"},
		{http.MethodPost, "/discover"},
	} {
		if res, _ := s.do(t, c.method, c.path, nil, invokeHeaders(1)); res.StatusCode != 404 {
			t.Fatalf("%s %s: %d", c.method, c.path, res.StatusCode)
		}
	}
	// The runtime speaks a version above the ceiling: 415, the handler does not run.
	res, b = pinned.do(t, http.MethodPost, "/invoke/Echo/hello", startBody([]byte{1}, 0), invokeHeaders(3))
	if res.StatusCode != 415 || !strings.Contains(string(b), "register the deployment again") {
		t.Fatalf("too new: %d %s", res.StatusCode, b)
	}
	// An encoding that is not enabled.
	h := invokeHeaders(1)
	h["Content-Encoding"] = "gzip"
	if res, _ := pinned.do(t, http.MethodPost, "/invoke/Echo/hello", startBody([]byte{1}, 0), h); res.StatusCode != 415 {
		t.Fatalf("gzip without compression: %d", res.StatusCode)
	}
}

func decodeFrames(t *testing.T, b []byte) []wire.Message {
	t.Helper()
	frames, err := wire.DecodeFrames(b)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]wire.Message, len(frames))
	for i, f := range frames {
		if out[i], err = wire.DecodeFrame(f); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func gzipped(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestEndpointCompression(t *testing.T) {
	s := newTestServer(t, HTTPHandler(echoService))
	h := invokeHeaders(1)
	h["Content-Encoding"] = "gzip"
	h["Accept-Encoding"] = "br, gzip;q=0.5"
	res, b := s.do(t, http.MethodPost, "/invoke/Echo/big", gzipped(t, startBody([]byte{1}, 0)), h)
	if res.StatusCode != 200 || res.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("big: %d %q", res.StatusCode, res.Header.Get("Content-Encoding"))
	}
	plain, err := gunzip(b, maxRequestBody)
	if err != nil {
		t.Fatal(err)
	}
	msgs := decodeFrames(t, plain)
	if kinds(msgs) != "Output End" || len(msgs[0].(*wire.Output).Value.Content) != 20_002 {
		t.Fatalf("frames %s", kinds(msgs))
	}
	// One gzip member per frame.
	zr, _ := gzip.NewReader(bytes.NewReader(b))
	zr.Multistream(false)
	first, _ := io.ReadAll(zr)
	if len(first) != len(wire.EncodeMessage(msgs[0], 0)) {
		t.Fatalf("first member holds %d bytes", len(first))
	}
	// Small responses, or no Accept-Encoding: plain.
	delete(h, "Accept-Encoding")
	if res, _ := s.do(t, http.MethodPost, "/invoke/Echo/big", gzipped(t, startBody([]byte{1}, 0)), h); res.Header.Get("Content-Encoding") != "" {
		t.Fatal("compressed without Accept-Encoding")
	}
	// A body already decompressed by a platform (header kept) is read as it is.
	if res, b := s.do(t, http.MethodPost, "/invoke/Echo/hello", startBody([]byte{1}, 0), h); res.StatusCode != 200 || kinds(decodeFrames(t, b)) != "Output End" {
		t.Fatalf("plain body under gzip header: %d", res.StatusCode)
	}
	bad := append([]byte{0x1f, 0x8b}, bytes.Repeat([]byte{0}, 20)...)
	if res, _ := s.do(t, http.MethodPost, "/invoke/Echo/hello", bad, h); res.StatusCode != 400 {
		t.Fatalf("corrupt gzip: %d", res.StatusCode)
	}
	big := invokeHeaders(1)
	req, _ := http.NewRequest(http.MethodPost, s.URL+"/invoke/Echo/hello", strings.NewReader("x"))
	for k, v := range big {
		req.Header.Set(k, v)
	}
	req.ContentLength = maxRequestBody + 1
	req.Body = io.NopCloser(io.LimitReader(zeroes{}, maxRequestBody+1))
	if res, err := s.client.Do(req); err != nil || res.StatusCode != 413 {
		t.Fatalf("too large: %v %v", res, err)
	} else {
		res.Body.Close()
	}
}

type zeroes struct{}

func (zeroes) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// signer signs requests as a runtime with an identity master does.
type signer struct {
	priv ed25519.PrivateKey
	kid  string
}

func newSigner(t *testing.T) signer {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return signer{priv: priv, kid: identity.KID(pub)}
}

func (s signer) jwt(aud, jti string, body []byte, host string) string {
	b64 := base64.RawURLEncoding.EncodeToString
	head, _ := json.Marshal(map[string]string{"alg": "EdDSA", "typ": "JWT", "kid": s.kid})
	now := time.Now().Unix()
	claims := map[string]any{"aud": aud, "sub": "t1", "iat": now, "nbf": now - 60, "exp": now + 60, "jti": jti}
	if body != nil {
		sum := sha256.Sum256(body)
		claims["bsh"] = b64(sum[:])
	}
	if host != "" {
		claims["host"] = host
	}
	c, _ := json.Marshal(claims)
	msg := b64(head) + "." + b64(c)
	return msg + "." + b64(ed25519.Sign(s.priv, []byte(msg)))
}

func (s signer) headers(base map[string]string, token string) map[string]string {
	out := map[string]string{identity.SchemeHeader: identity.SchemeV1, identity.JWTHeader: token}
	for k, v := range base {
		out[k] = v
	}
	return out
}

func TestEndpointRequestIdentity(t *testing.T) {
	key := newSigner(t)
	var ran atomic.Int32
	svc := NewService("Echo", Handler("hello", NoInput(func(Context) (string, error) {
		ran.Add(1)
		return "hi", nil
	})))
	mux := http.NewServeMux()
	mux.Handle("/cairn/", http.StripPrefix("/cairn", HTTPHandler(svc, IdentityKeys(key.kid))))
	s := newTestServer(t, mux)
	host := strings.TrimPrefix(s.URL, "http://")

	if res, _ := s.do(t, http.MethodGet, "/cairn/discover", nil, nil); res.StatusCode != 401 {
		t.Fatalf("unsigned discover: %d", res.StatusCode)
	}
	res, b := s.do(t, http.MethodGet, "/cairn/discover", nil, key.headers(nil, key.jwt("/cairn/discover", identity.DiscoverJTI, nil, host)))
	if res.StatusCode != 200 || !strings.Contains(string(b), `"requestIdentity":"v1"`) {
		t.Fatalf("signed discover: %d %s", res.StatusCode, b)
	}

	id := []byte{0xab, 0xcd}
	body := startBody(id, 2)
	jti := identity.ExpectedJTI(id, 2)
	cases := []struct {
		name  string
		token string
		want  int
	}{
		{"unsigned", "", 401},
		{"another invocation", key.jwt("/cairn/invoke/Echo/hello", identity.ExpectedJTI([]byte{9}, 2), body, host), 401},
		{"another attempt", key.jwt("/cairn/invoke/Echo/hello", identity.ExpectedJTI(id, 1), body, host), 401},
		{"another body", key.jwt("/cairn/invoke/Echo/hello", jti, startBody(id, 3), host), 401},
		{"another path", key.jwt("/invoke/Echo/hello", jti, body, host), 401},
		{"another host", key.jwt("/cairn/invoke/Echo/hello", jti, body, "elsewhere.example"), 401},
		{"another key", newSigner(t).jwt("/cairn/invoke/Echo/hello", jti, body, host), 401},
		{"signed", key.jwt("/cairn/invoke/Echo/hello", jti, body, host), 200},
	}
	for _, c := range cases {
		h := invokeHeaders(1)
		if c.token != "" {
			h = key.headers(h, c.token)
		}
		res, b := s.do(t, http.MethodPost, "/cairn/invoke/Echo/hello", body, h)
		if res.StatusCode != c.want {
			t.Fatalf("%s: %d %s", c.name, res.StatusCode, b)
		}
	}
	if ran.Load() != 1 {
		t.Fatalf("the handler ran %d times, want once (only for the signed request)", ran.Load())
	}
}

func TestEndpointConfiguration(t *testing.T) {
	for name, opts := range map[string][]ServeOption{
		"no service":      nil,
		"twice":           {echoService, echoService},
		"version 0 kept":  {echoService, MaxProtocolVersion(-1)},
		"version too new": {echoService, MaxProtocolVersion(servedProtocolVersion + 1)},
		"bad origin":      {echoService, SegmentOrigins("https://values.example.com/path")},
		"ftp origin":      {echoService, SegmentOrigins("ftp://values.example.com")},
		"negative cache":  {echoService, ValueCacheBytes(-1)},
		"bad kid":         {echoService, IdentityKeys("publickeyv1_nope")},
		"no kid":          {echoService, IdentityKeys()},
		"nil":             {echoService, nil},
	} {
		if _, err := newEndpoint(opts); err == nil {
			t.Fatalf("%s: no error", name)
		}
		if err := Serve(context.Background(), "127.0.0.1:0", opts...); err == nil {
			t.Fatalf("%s: Serve did not fail", name)
		}
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s: HTTPHandler did not panic", name)
				}
			}()
			HTTPHandler(opts...)
		}()
	}
}

// A REQUEST_RESPONSE body must arrive within bodyReadTimeout: the server sets no
// ReadTimeout (a BIDI stream lives long), so without it a client trickling a body holds
// the connection and the handler goroutine forever.
//
//  1. A client sends headers announcing 100 body bytes, then 10 bytes, then nothing.
//  2. The endpoint waits for the rest.
//
// Must see: the endpoint answers (or closes) within the shortened bound; then a normal
// turn on a new connection still runs.
func TestRequestBodyHasAReadDeadline(t *testing.T) {
	old := bodyReadTimeout
	bodyReadTimeout = 200 * time.Millisecond
	t.Cleanup(func() { bodyReadTimeout = old })
	s := newTestServer(t, HTTPHandler(echoService))
	conn, err := net.Dial("tcp", strings.TrimPrefix(s.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	head := "POST /invoke/Echo/hello HTTP/1.1\r\nHost: x\r\nContent-Type: application/vnd.cairnflow.invocation.v1\r\n" +
		"Content-Length: 100\r\n\r\n0123456789"
	if _, err := io.WriteString(conn, head); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	b := make([]byte, 512)
	n, err := conn.Read(b)
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatal("the endpoint still waits for the body after 5 s")
	}
	if n > 0 && !strings.HasPrefix(string(b[:n]), "HTTP/1.1 400") {
		t.Fatalf("answer %q", b[:n])
	}
	if res, out := s.do(t, http.MethodPost, "/invoke/Echo/hello", startBody([]byte{1}, 0), invokeHeaders(1)); res.StatusCode != 200 {
		t.Fatalf("a normal turn: %d %s", res.StatusCode, out)
	}
}

func TestServeShutsDownWithItsContext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e, err := newEndpoint([]ServeOption{echoService})
	if err != nil {
		t.Fatal(err)
	}
	srv := newServer(ln.Addr().String(), e)
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- waitServer(ctx, srv, errc) }()

	client := &http.Client{Transport: &http.Transport{}}
	res, err := client.Get("http://" + ln.Addr().String() + "/discover")
	if err != nil || res.StatusCode != 200 {
		t.Fatalf("discover: %v %v", res, err)
	}
	res.Body.Close()
	client.CloseIdleConnections()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after its context ended")
	}
	// The address is busy: Serve returns the listen error at once.
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	err = Serve(context.Background(), busy.Addr().String(), echoService)
	var op *net.OpError
	if !errors.As(err, &op) {
		t.Fatalf("Serve on a busy address: %v", err)
	}
}
