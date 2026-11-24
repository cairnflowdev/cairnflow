// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/bidi"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/identity"
	machine "github.com/cairnflowdev/cairnflow/sdk/go/internal/turn"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

// stream serves one BIDI_STREAM invocation: an HTTP/2 stream that stays open across turns
// (docs/protocol-v1.md §4, "Turns on one stream").
func (e *endpoint) stream(w http.ResponseWriter, r *http.Request, svc *ServiceDef, h *HandlerDef, wireVersion int) {
	// Request identity: the headers now (signature, path, expiry, host). One JWT opens one
	// stream, keyed by its signature: only the turn-opening body is bound to it, so a
	// second stream with the same JWT could carry a forged continuation. The jti and the
	// body hash are checked when Start and the replayed entries arrive, after the response
	// headers went out.
	var claims *identity.Claims
	if e.verifier != nil {
		c, err := identity.Check(e.verifier, r.Header.Get, signedPath(r), func(string) bool { return true }, e.bound(r), time.Now())
		if err != nil {
			writeError(w, http.StatusUnauthorized, err.Error())
			return
		}
		token := r.Header.Get(identity.JWTHeader)
		if !e.used.Claim(token[strings.LastIndexByte(token, '.')+1:], c.Exp, time.Now()) {
			writeError(w, http.StatusUnauthorized, "this request identity token has already opened a stream")
			return
		}
		claims = c
	}
	gzipped := false
	if enc := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding"))); enc != "" && enc != "identity" {
		if enc != "gzip" || e.cfg.noCompression {
			writeError(w, http.StatusUnsupportedMediaType, "content-encoding "+enc+" is not enabled (requestCompression)")
			return
		}
		gzipped = true
	}

	// The response headers go out before any byte of the body is read: the runtime takes
	// them as the sign that the stream is open, and waiting for Start first would make
	// both sides wait for each other.
	rc := http.NewResponseController(w)
	_ = rc.EnableFullDuplex() // HTTP/2 is always full duplex; this matters for HTTP/1 only
	w.Header().Set("Content-Type", invocationV1)
	w.WriteHeader(http.StatusOK)
	if rc.Flush() != nil {
		return
	}

	var in io.ReadCloser = r.Body
	if gzipped {
		in = &gunzipBody{body: r.Body, left: maxRequestBody}
	}
	cfg := bidi.Config{Turn: machine.Config{
		Wire:     wireVersion,
		Now:      e.now,
		LogLevel: svc.logLevel(h),
		Context:  r.Context(),
		Values:   e.valueSession(),
	}}
	if e.artifact != nil {
		cfg.Turn.FetchArtifact = e.fetchArtifact
	}
	if claims != nil {
		cfg.CheckStart = func(st *wire.Start) bool {
			return identity.ExpectedJTI(st.ID, st.RetryCountSinceLastStoredEntry) == claims.JTI
		}
		if claims.BSH != nil {
			want := *claims.BSH
			cfg.CheckOpening = func(opening []byte) bool { return identity.BodyHash(opening) == want }
			cfg.MaxOpening = maxRequestBody
		}
	}
	err := bidi.Serve(h.body, in, func(b []byte) error {
		if _, err := w.Write(b); err != nil {
			return err
		}
		return rc.Flush()
	}, cfg)
	if errors.Is(err, bidi.ErrRejected) {
		// Close the stream without a frame (RST_STREAM); net/http does not log this panic.
		panic(http.ErrAbortHandler)
	}
}

// gunzipBody decompresses a gzip request body as it arrives, at most left bytes.
type gunzipBody struct {
	body io.ReadCloser
	zr   *gzip.Reader
	left int64
}

func (g *gunzipBody) Read(p []byte) (int, error) {
	if g.zr == nil {
		zr, err := gzip.NewReader(g.body)
		if err != nil {
			return 0, err
		}
		g.zr = zr
	}
	if g.left <= 0 {
		return 0, fmt.Errorf("cairn: the stream exceeds %d bytes after decompression", maxRequestBody)
	}
	if int64(len(p)) > g.left {
		p = p[:g.left]
	}
	n, err := g.zr.Read(p)
	g.left -= int64(n)
	return n, err
}

func (g *gunzipBody) Close() error { return g.body.Close() }
