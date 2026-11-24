// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

// Artifacts end to end through the endpoint: ArtifactPut on the wire, and Get reading
// from the runtime (WithArtifactSource) what this process does not hold.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

func shaHex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// artifactIngress is a fake runtime ingress serving `GET /invocations/{id}/artifacts/{sha}`.
type artifactIngress struct {
	*httptest.Server
	bodies    map[string][]byte // by path
	requests  atomic.Int64
	redirects atomic.Int64
	header    atomic.Value // the Authorization header of the last request
}

func newArtifactIngress(t *testing.T, bodies map[string][]byte) *artifactIngress {
	a := &artifactIngress{bodies: bodies}
	a.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/elsewhere" {
			a.redirects.Add(1)
			_, _ = w.Write([]byte("followed"))
			return
		}
		a.requests.Add(1)
		a.header.Store(r.Header.Get("Authorization"))
		if strings.HasSuffix(r.URL.Path, "/moved") {
			http.Redirect(w, r, "/elsewhere", http.StatusFound)
			return
		}
		b, ok := a.bodies[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(b)
	}))
	t.Cleanup(a.Close)
	return a
}

// artifactGetter returns the artifact sha (or the error) as its output.
var artifactGetter = NewService("Artifacts",
	Handler("get", func(ctx Context, sha string) (string, error) {
		b, err := ctx.Artifacts().Get(sha)
		if err != nil {
			return "error: " + err.Error(), nil
		}
		return string(b), nil
	}),
	Handler("put", NoInput(func(ctx Context) (ArtifactRef, error) {
		ref, err := ctx.Artifacts().Put([]byte("put on the handler goroutine"), "")
		if err != nil {
			return ArtifactRef{}, err
		}
		_, err = Run(ctx, "after", constant(1))
		return ref, err
	})),
)

// getArtifact runs the get handler for sha and returns its output.
func getArtifact(t *testing.T, s *testServer, sha string) string {
	t.Helper()
	b := wire.EncodeMessage(&wire.Start{ID: []byte{0xca, 0x1f}, KnownEntries: 1}, 0)
	b = wire.AppendMessage(b, &wire.Input{Value: &wire.Value{Content: []byte(`"` + sha + `"`)}}, 0)
	res, out := s.do(t, http.MethodPost, "/invoke/Artifacts/get", b, invokeHeaders(4))
	if res.StatusCode != 200 {
		t.Fatalf("status %d", res.StatusCode)
	}
	msgs := decodeFrames(t, out)
	o, ok := msgs[0].(*wire.Output)
	if !ok || o.Result.Value == nil {
		t.Fatalf("frames %s", kinds(msgs))
	}
	return strings.Trim(string(o.Result.Value.Content), `"`)
}

func TestArtifactGetReadsFromTheRuntime(t *testing.T) {
	good := []byte("artifact bytes from the runtime " + t.Name())
	sha := shaHex(good)
	ingress := newArtifactIngress(t, map[string][]byte{"/invocations/ca1f/artifacts/" + sha: good})
	s := newTestServer(t, HTTPHandler(artifactGetter, WithArtifactSource(ingress.URL+"/", http.Header{"Authorization": {"Bearer key"}})))
	if got := getArtifact(t, s, sha); got != string(good) {
		t.Fatalf("got %q", got)
	}
	if ingress.header.Load() != "Bearer key" {
		t.Fatalf("Authorization %v", ingress.header.Load())
	}
	// Downloaded once: the process now holds it.
	if got := getArtifact(t, s, sha); got != string(good) || ingress.requests.Load() != 1 {
		t.Fatalf("got %q after %d requests", got, ingress.requests.Load())
	}
	// Not there, or redirected elsewhere: an error, and a redirect is not followed (the
	// credentials never reach another origin).
	if got := getArtifact(t, s, shaHex([]byte("absent "+t.Name()))); !strings.Contains(got, "HTTP 404") {
		t.Fatalf("got %q", got)
	}
	if got := getArtifact(t, s, "moved"); !strings.HasPrefix(got, "error:") || ingress.redirects.Load() != 0 {
		t.Fatalf("got %q, %d redirects followed", got, ingress.redirects.Load())
	}
	// Without a source, only what the process holds.
	plain := newTestServer(t, HTTPHandler(artifactGetter))
	if got := getArtifact(t, plain, shaHex([]byte("elsewhere "+t.Name()))); !strings.Contains(got, "WithArtifactSource") {
		t.Fatalf("got %q", got)
	}
}

func TestArtifactGetChecksTheDownload(t *testing.T) {
	want := []byte("the real artifact " + t.Name())
	sha := shaHex(want)
	big := bytes.Repeat([]byte("x"), core.ArtifactMaxBytes+1)
	bigSha := shaHex(big)
	ingress := newArtifactIngress(t, map[string][]byte{
		"/invocations/ca1f/artifacts/" + sha:    []byte("a proxy error page"),
		"/invocations/ca1f/artifacts/" + bigSha: big,
	})
	s := newTestServer(t, HTTPHandler(artifactGetter, WithArtifactSource(ingress.URL, nil)))
	for range 2 {
		// Bytes that are not the artifact are refused, and not kept for the next Get.
		if got := getArtifact(t, s, sha); !strings.Contains(got, "does not match its sha256") {
			t.Fatalf("got %q", got)
		}
	}
	if ingress.requests.Load() != 2 {
		t.Fatalf("%d requests, want 2 (nothing cached)", ingress.requests.Load())
	}
	if got := getArtifact(t, s, bigSha); !strings.Contains(got, "exceeds the limit") {
		t.Fatalf("got %.80q", got)
	}
}

func TestArtifactPutOnTheHandlerGoroutine(t *testing.T) {
	s := newTestServer(t, HTTPHandler(artifactGetter))
	body := wire.EncodeMessage(&wire.Start{ID: []byte{0xca, 0x1f}, KnownEntries: 1}, 0)
	body = wire.AppendMessage(body, input(), 0)
	_, out := s.do(t, http.MethodPost, "/invoke/Artifacts/put", body, invokeHeaders(4))
	msgs := decodeFrames(t, out)
	// The bytes go before the command after them, so the runtime has stored them first.
	wantKinds(t, msgs, "ArtifactPut Run ProposeRunCompletion Suspension")
	p := msgs[0].(*wire.ArtifactPut)
	if string(p.Content) != "put on the handler goroutine" || p.ContentType != "application/octet-stream" ||
		hex.EncodeToString(p.Sha256) != shaHex(p.Content) {
		t.Fatalf("ArtifactPut %+v", p)
	}
	// Replayed: the point is in the journal, the bytes are not sent again.
	body = wire.EncodeMessage(&wire.Start{ID: []byte{0xca, 0x1f}, KnownEntries: 3}, 0)
	body = wire.AppendMessage(body, input(), 0)
	body = wire.AppendMessage(body, msgs[1], 0)
	body = wire.AppendMessage(body, &wire.RunCompletion{ID: 1, Result: wire.Result{Value: &wire.Value{Content: []byte("1")}}}, 0)
	_, out = s.do(t, http.MethodPost, "/invoke/Artifacts/put", body, invokeHeaders(4))
	wantKinds(t, decodeFrames(t, out), "Output End")
	// Below wire v4 there is no ArtifactPut: the put fails.
	_, out = s.do(t, http.MethodPost, "/invoke/Artifacts/put", startBody([]byte{0xca, 0x1f}, 0), invokeHeaders(3))
	wantKinds(t, decodeFrames(t, out), "Error")
}
