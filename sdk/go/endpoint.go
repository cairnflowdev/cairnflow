// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/identity"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/replay"
	machine "github.com/cairnflowdev/cairnflow/sdk/go/internal/turn"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/values"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

// Content types of the protocol (docs/protocol-v1.md section 2).
const (
	discoveryV1  = "application/vnd.cairnflow.endpointmanifest.v1+json"
	invocationV1 = "application/vnd.cairnflow.invocation.v1"
)

// servedProtocolVersion is the highest wire version this SDK serves today.
const servedProtocolVersion = 6

// maxRequestBody caps a request body, after decompression too (a larger one gets 413).
const maxRequestBody = 256 << 20

// compressMin is the response size from which a response is gzip-compressed when the
// runtime accepts it.
const compressMin = 8 << 10

// shutdownGrace is how long Serve waits for running turns when its context ends.
const shutdownGrace = 30 * time.Second

// bodyReadTimeout bounds the read of a REQUEST_RESPONSE request body, as Node's default
// requestTimeout does: a client must not hold a turn open by trickling its body. A
// variable so tests can shorten it.
var bodyReadTimeout = 5 * time.Minute

// ServeOption configures [HTTPHandler] and [Serve]. A *[ServiceDef] is one: passing it
// serves that service.
type ServeOption interface{ applyServe(*endpointConfig) }

type endpointConfig struct {
	services []*ServiceDef
	// bidi declares BIDI_STREAM: invocations arrive as HTTP/2 streams.
	bidi bool
	// maxWire is the highest wire version declared (servedProtocolVersion when 0).
	maxWire int
	// identityOn: IdentityKeys was given (an empty list is an error, not "off").
	identityOn    bool
	identityKeys  []string
	identityHosts []string
	noCompression bool
	artifactBase  string
	artifactHead  http.Header
	// Segmented values (wire v6): see SegmentOrigins and the options after it.
	segmentOrigins    []string
	valueCacheBytes   int64
	hasValueCache     bool
	maxValueBytes     int64
	maxTurnValueBytes int64
	segmentTransport  http.RoundTripper
	// lambda: the endpoint serves AWS Lambda ([LambdaHandler]), one request per turn and no
	// process kept between turns.
	lambda bool
	// replayCache is the replay cache of wire v5 as the options set it: replayDefault (on
	// only with request identity), replayOn ([ReplayCache]) or replayOff
	// ([WithoutReplayCache]); replayLimits bound it.
	replayCache  replayChoice
	replayLimits replay.Limits
	// now is the SDK's wall clock (tests fix it); nil = the system clock.
	now func() int64
}

func (s *ServiceDef) applyServe(c *endpointConfig) { c.services = append(c.services, s) }

type serveOptionFunc func(*endpointConfig)

func (f serveOptionFunc) applyServe(c *endpointConfig) { f(c) }

// Bidi makes the endpoint declare `protocolMode: BIDI_STREAM` (docs/protocol-v1.md §4): each
// invocation is one HTTP/2 stream without TLS (h2c, prior knowledge) that stays open across
// turns, so a suspension the runtime can answer at once (a state read, the ack of a [Run])
// costs no new request and no journal resent. [Serve] serves HTTP/1.1 (discovery) and h2c on
// one port. The default is REQUEST_RESPONSE, one HTTP/1.1 request per turn: keep it when a
// proxy between the runtime and the endpoint only speaks HTTP/1.1.
//
// A server of your own around [HTTPHandler] must accept h2c and must not cut long streams:
//
//	var p http.Protocols
//	p.SetHTTP1(true)
//	p.SetUnencryptedHTTP2(true)
//	srv := &http.Server{Addr: ":9080", Handler: h, Protocols: &p, ReadHeaderTimeout: 10 * time.Second}
//
// with no ReadTimeout or WriteTimeout (they bound the whole stream, which lives as long as
// the invocation keeps resolving its waits in place). An HTTP/1.1 request to /invoke on such
// an endpoint is still served, as one REQUEST_RESPONSE turn.
func Bidi() ServeOption {
	return serveOptionFunc(func(c *endpointConfig) { c.bidi = true })
}

// MaxProtocolVersion pins the highest wire version the endpoint declares (1 to 6), for a
// deployment that must keep talking to an older runtime the same way. The default is the
// highest version this SDK serves (6) when the endpoint has [IdentityKeys] or
// [SegmentOrigins], and 5 otherwise: wire v6 makes the endpoint download segment URLs, which
// an endpoint that verifies no request identity must only read from listed origins (the
// runtime then assembles large values inline, as for any v5 SDK). Pinning 6 without either
// option is a configuration error.
func MaxProtocolVersion(v int) ServeOption {
	return serveOptionFunc(func(c *endpointConfig) { c.maxWire = v })
}

// IdentityKeys turns on request identity verification (docs/protocol-v1.md section 4,
// "Request identity"): every request must carry a JWT signed by one of these keys, the kid
// values listed at the runtime's `GET /identity`; any other request gets 401 before a
// handler runs. Without it, anyone who can reach the endpoint can run handlers on a forged
// journal (the endpoint warns once). It also turns on the replay cache of REQUEST_RESPONSE
// ([ReplayCache]), which an unverified endpoint never keeps, and wire v6 (large values read
// from segment URLs, see [SegmentOrigins]), which an unverified endpoint only serves with
// SegmentOrigins.
func IdentityKeys(kids ...string) ServeOption {
	return serveOptionFunc(func(c *endpointConfig) {
		c.identityOn = true
		c.identityKeys = append(c.identityKeys, kids...)
	})
}

// IdentityHosts lists the public hosts a signed `host` claim may name, for an endpoint
// behind a proxy that rewrites Host; by default the request's own Host.
func IdentityHosts(hosts ...string) ServeOption {
	return serveOptionFunc(func(c *endpointConfig) { c.identityHosts = append(c.identityHosts, hosts...) })
}

// WithoutCompression stops declaring and accepting gzip-compressed request bodies, and
// stops compressing large responses.
func WithoutCompression() ServeOption {
	return serveOptionFunc(func(c *endpointConfig) { c.noCompression = true })
}

// WithArtifactSource lets [Artifacts.Get] download artifacts this process does not hold
// from the runtime: baseURL is the runtime's ingress, header carries its credentials (an
// API key). Without it, Get only reads the artifacts this process put.
func WithArtifactSource(baseURL string, header http.Header) ServeOption {
	return serveOptionFunc(func(c *endpointConfig) {
		c.artifactBase = strings.TrimRight(baseURL, "/")
		c.artifactHead = header.Clone()
	})
}

// SegmentOrigins lists the origins the endpoint may read segments of large values from
// ("https://values.example.com"), the runtime's value read origins (wire v6). The runtime
// sends a large state value or result as refs to segments plus the read URL of each, and
// the SDK downloads the segments before the handler runs. Without this option, an endpoint
// with [IdentityKeys] reads any http or https URL (only the runtime can send a turn), and
// an endpoint without IdentityKeys does not serve wire v6 at all and never downloads a
// segment (the runtime assembles large values inline): there, whoever can reach the
// endpoint chooses the URLs. A failed download answers the runtime only "a segment could
// not be read (fetch)"; the detail (status, length, network error, never the URL) goes to
// the slog logger. At most 4096 refs per value and 8192 per turn are read, and at most 64
// downloads run at once in the process.
func SegmentOrigins(origins ...string) ServeOption {
	return serveOptionFunc(func(c *endpointConfig) { c.segmentOrigins = append(c.segmentOrigins, origins...) })
}

// ValueCacheBytes caps the memory of the segment cache (wire v6), shared by every turn of
// the endpoint: segments already read are not downloaded again. The default is 64 MiB; 0
// keeps nothing between turns. On AWS Lambda the cache only lives as long as the instance.
func ValueCacheBytes(n int64) ServeOption {
	return serveOptionFunc(func(c *endpointConfig) { c.valueCacheBytes, c.hasValueCache = n, true })
}

// MaxValueBytes caps the logical size of one segmented value the SDK assembles (wire v6).
// The default, 64 MiB, is the tenant's default `max-value-bytes`: a cluster that raises it
// raises this cap with it.
func MaxValueBytes(n int64) ServeOption {
	return serveOptionFunc(func(c *endpointConfig) { c.maxValueBytes = n })
}

// MaxTurnValueBytes caps the bytes of segmented values assembled in one turn (in
// BIDI_STREAM, one stream), segments found in the cache included. The default is 256 MiB.
func MaxTurnValueBytes(n int64) ServeOption {
	return serveOptionFunc(func(c *endpointConfig) { c.maxTurnValueBytes = n })
}

// WithSegmentTransport sends the segment downloads of large values (wire v6) through rt
// instead of http.DefaultTransport: a proxy, a private root CA, a test. Redirects are never
// followed, whatever rt does.
func WithSegmentTransport(rt http.RoundTripper) ServeOption {
	return serveOptionFunc(func(c *endpointConfig) { c.segmentTransport = rt })
}

// ReplayCacheOptions bound the replay cache of [ReplayCache]. Zero fields take the
// defaults.
type ReplayCacheOptions struct {
	// MaxBytes caps the memory the cache holds, journal frames and digests; when it is
	// full the least recently used invocation is dropped, and an invocation larger than
	// the cap is not held at all. Default 64 MiB.
	MaxBytes int
	// TTL drops an invocation not used for this long. Default 15 minutes.
	TTL time.Duration
}

// replayChoice is how the options set the replay cache.
type replayChoice int8

const (
	replayDefault replayChoice = iota
	replayOn
	replayOff
)

// ReplayCache sets the limits of the replay cache and turns it on. With [IdentityKeys],
// the cache is on by default for REQUEST_RESPONSE: the endpoint keeps the journal of each
// running invocation in memory and declares `replayCache`, so a runtime speaking wire v5
// sends each turn only the entries after a prefix the endpoint proves it holds
// (docs/protocol-v1.md §9.3, "Incremental replay for REQUEST_RESPONSE") instead of the
// whole journal. Anything the cache cannot prove (a restart, another replica behind a load
// balancer, an eviction) costs one round trip: the endpoint answers 409 and the runtime
// resends everything. It has no effect with [Bidi] or below wire v5. The last of
// ReplayCache and [WithoutReplayCache] wins.
//
// Without [IdentityKeys] the cache is off, and ReplayCache is a configuration error: an
// endpoint that does not verify who sends a turn must not keep other invocations'
// journals, which a stranger who knows an invocation id could join and run the handler on.
func ReplayCache(o ReplayCacheOptions) ServeOption {
	return serveOptionFunc(func(c *endpointConfig) {
		c.replayCache = replayOn
		c.replayLimits = replay.Limits{MaxBytes: o.MaxBytes, TTL: o.TTL}
	})
}

// WithoutReplayCache turns the replay cache off (see [ReplayCache]): every turn carries
// the whole journal, as below wire v5. The last of ReplayCache and WithoutReplayCache
// wins. Without [IdentityKeys] the cache is off already.
func WithoutReplayCache() ServeOption {
	return serveOptionFunc(func(c *endpointConfig) { c.replayCache = replayOff })
}

// withClock fixes the SDK's wall clock (the conformance runner sets `clock_ms`).
func withClock(now func() int64) ServeOption {
	return serveOptionFunc(func(c *endpointConfig) { c.now = now })
}

// endpoint is the HTTP surface of a deployment: `GET /discover` and
// `POST /invoke/{service}/{handler}` (docs/protocol-v1.md sections 3 and 4).
type endpoint struct {
	cfg      endpointConfig
	byName   map[string]*ServiceDef
	verifier *identity.Verifier
	// used: the identity tokens that opened a BIDI_STREAM stream (one stream each).
	used     *identity.UsedTokens
	manifest []byte
	artifact *http.Client
	// Segmented values (wire v6): the segment cache shared by every turn, and the reader.
	segments *values.Cache
	fetcher  *values.Fetcher
	// replay is the replay cache of wire v5; nil when off.
	replay *replay.Cache
}

var warnUnverified sync.Once

func newEndpoint(opts []ServeOption) (*endpoint, error) {
	var cfg endpointConfig
	for _, o := range opts {
		if o == nil {
			return nil, errors.New("cairn: a nil option or service")
		}
		o.applyServe(&cfg)
	}
	if len(cfg.services) == 0 {
		return nil, errors.New("cairn: no service to serve")
	}
	// Segment downloads fail closed: without request identity, whoever reaches the
	// endpoint chooses the read URLs, so only the origins listed may be read. With
	// neither, the endpoint stays on wire v5 (the runtime assembles values inline) and
	// fetches nothing.
	segmentsOK := cfg.identityOn || len(cfg.segmentOrigins) > 0
	if cfg.maxWire == 0 {
		cfg.maxWire = servedProtocolVersion
		if !segmentsOK {
			cfg.maxWire = values.WireSegmented - 1
		}
	} else if cfg.maxWire >= values.WireSegmented && cfg.maxWire <= servedProtocolVersion && !segmentsOK {
		return nil, fmt.Errorf("cairn: MaxProtocolVersion(%d) needs IdentityKeys or SegmentOrigins: wire v6 makes the "+
			"endpoint download segment URLs, which on an endpoint that verifies no request identity whoever reaches it "+
			"chooses; set cairn.IdentityKeys (or cairn.SegmentOrigins to the runtime's value read origins), or pin "+
			"MaxProtocolVersion(5)", cfg.maxWire)
	}
	if cfg.replayCache == replayOn && !cfg.identityOn {
		return nil, errors.New("cairn: ReplayCache needs IdentityKeys: an endpoint that does not verify request " +
			"identity must not keep other invocations' journals (whoever knows an invocation id could join one); " +
			"set cairn.IdentityKeys or remove cairn.ReplayCache")
	}
	if cfg.lambda && cfg.bidi {
		return nil, errors.New("cairn: AWS Lambda serves REQUEST_RESPONSE only (no long-lived streams); remove cairn.Bidi()")
	}
	if cfg.maxWire < minProtocolVersion || cfg.maxWire > servedProtocolVersion {
		return nil, fmt.Errorf("cairn: MaxProtocolVersion(%d) is outside [%d, %d]", cfg.maxWire, minProtocolVersion, servedProtocolVersion)
	}
	for _, n := range []int64{cfg.valueCacheBytes, cfg.maxValueBytes, cfg.maxTurnValueBytes} {
		if n < 0 {
			return nil, fmt.Errorf("cairn: a value byte limit of %d is negative", n)
		}
	}
	origins := make([]string, 0, len(cfg.segmentOrigins))
	for _, o := range cfg.segmentOrigins {
		n, err := values.Origin(o)
		if err != nil {
			return nil, fmt.Errorf("cairn: %w", err)
		}
		origins = append(origins, n)
	}
	cacheBytes := int64(values.DefaultCacheBytes)
	if cfg.hasValueCache {
		cacheBytes = cfg.valueCacheBytes
	}
	e := &endpoint{
		cfg:      cfg,
		byName:   map[string]*ServiceDef{},
		segments: values.NewCache(cacheBytes),
	}
	if segmentsOK {
		// Without it no fetcher at all: a turn that still carries refs (a caller sending
		// refs at v5) fails without a download.
		e.fetcher = values.NewFetcher(cfg.segmentTransport, origins)
	}
	for _, s := range cfg.services {
		if _, dup := e.byName[s.name]; dup {
			return nil, fmt.Errorf("cairn: service %q is served twice", s.name)
		}
		e.byName[s.name] = s
	}
	if cfg.identityOn {
		v, err := identity.NewVerifier(cfg.identityKeys)
		if err != nil {
			return nil, fmt.Errorf("cairn: %w", err)
		}
		e.verifier = v
		e.used = identity.NewUsedTokens(0)
	} else {
		warnUnverified.Do(func() {
			slog.Warn("cairn: the endpoint has no IdentityKeys: anyone who can reach it can run your handlers on a forged "+
				"journal, input and state (real side effects on fake data). The replay cache stays off (every turn "+
				"carries the whole journal), and without cairn.SegmentOrigins the endpoint serves wire v5 only "+
				"(large values arrive inline; no segment URL is ever fetched). Set cairn.IdentityKeys to the kid "+
				"listed at the runtime's GET /identity (and cairn.SegmentOrigins to the runtime's value read "+
				"origins)", "code", "CAIRNFLOW_UNVERIFIED")
		})
	}
	if cfg.artifactBase != "" {
		// No redirects: the configured credentials never reach another origin.
		e.artifact = &http.Client{
			Timeout:       60 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	// Only REQUEST_RESPONSE at wire v5 or later has deltas: BIDI_STREAM keeps the journal
	// on the stream. Only with request identity: an unverified endpoint keeps no journal.
	replayWanted := cfg.replayCache == replayOn || (cfg.replayCache == replayDefault && cfg.identityOn)
	if replayWanted && e.verifier != nil && !cfg.bidi && cfg.maxWire >= replayDeltaVersion {
		e.replay = replay.New(cfg.replayLimits)
	}
	e.manifest = manifestJSON(manifestOptions{
		bidi:            cfg.bidi,
		maxWire:         cfg.maxWire,
		requestIdentity: e.verifier != nil,
		compression:     !cfg.noCompression,
		replayCache:     e.replay != nil,
	}, cfg.services...)
	return e, nil
}

// HTTPHandler serves the services passed among opts, for mounting in a server of your own:
//
//	mux.Handle("/cairn/", http.StripPrefix("/cairn", cairn.HTTPHandler(orders, cairn.IdentityKeys(kid))))
//
// It serves REQUEST_RESPONSE over HTTP/1.1, and with [Bidi] BIDI_STREAM over h2c (see
// [Bidi] for what the server needs). The JWT of request identity signs the path the runtime
// sent, prefix included. A server you build yourself must not set a WriteTimeout shorter
// than your longest turn (a turn waits for its [Run] closures), nor any ReadTimeout or
// WriteTimeout with [Bidi]; the handler itself gives a REQUEST_RESPONSE body 5 minutes to
// arrive (a read deadline on the connection, cleared after the body). It panics when opts
// are inconsistent (no service, the same
// service twice, a version or key it cannot use, [ReplayCache] without [IdentityKeys],
// MaxProtocolVersion(6) with neither IdentityKeys nor [SegmentOrigins]); [Serve] returns
// the error instead.
func HTTPHandler(opts ...ServeOption) http.Handler {
	e, err := newEndpoint(opts)
	if err != nil {
		panic(err.Error())
	}
	return e
}

// Serve listens on addr and serves the services passed among opts until ctx ends, then
// shuts down gracefully (running turns get a grace period). It serves HTTP/1.1 and h2c
// (unencrypted HTTP/2 with prior knowledge) on the one port: discovery and
// REQUEST_RESPONSE over HTTP/1.1, BIDI_STREAM ([Bidi]) over h2c. The server reads request
// headers with a timeout, and a REQUEST_RESPONSE body must arrive within 5 minutes; it sets
// no write timeout and does not bound a BIDI_STREAM stream, which a long turn and a
// long-lived stream need.
func Serve(ctx context.Context, addr string, opts ...ServeOption) error {
	e, err := newEndpoint(opts)
	if err != nil {
		return err
	}
	srv := newServer(addr, e)
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	return waitServer(ctx, srv, errc)
}

// newServer is the http.Server of Serve: HTTP/1.1 and unencrypted HTTP/2 with prior
// knowledge on the same port (the server tells them apart by the HTTP/2 preface,
// docs/protocol-v1.md §4, "BIDI_STREAM is h2c prior-knowledge"). No ReadTimeout or
// WriteTimeout: a BIDI_STREAM stream lives as long as its invocation keeps resolving waits
// in place.
func newServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		Protocols:         serverProtocols(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
	}
}

// serverProtocols is HTTP/1.1 plus h2c.
func serverProtocols() *http.Protocols {
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	return &p
}

// waitServer returns the server's error, or shuts it down when ctx ends.
func waitServer(ctx context.Context, srv *http.Server, errc <-chan error) error {
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shut, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	err := srv.Shutdown(shut)
	if err != nil {
		_ = srv.Close()
	}
	if served := <-errc; served != nil && !errors.Is(served, http.ErrServerClosed) && err == nil {
		err = served
	}
	return err
}

func (e *endpoint) now() int64 {
	if e.cfg.now != nil {
		return e.cfg.now()
	}
	return time.Now().UnixMilli()
}

var invocationType = regexp.MustCompile(`^application/vnd\.cairnflow\.invocation\.v(\d+)$`)

// wireVersionOf is the wire version a content type asks for; unreadable means v1, the safe
// side (a scope is refused rather than sent to a runtime that might ignore it).
func wireVersionOf(contentType string) int {
	media := strings.ToLower(strings.TrimSpace(strings.SplitN(contentType, ";", 2)[0]))
	m := invocationType.FindStringSubmatch(media)
	if m == nil {
		return minProtocolVersion
	}
	v, err := strconv.Atoi(m[1])
	if err != nil {
		return maxProtocolVersion + 1
	}
	return v
}

func writeJSON(w http.ResponseWriter, status int, contentType string, body []byte) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	b, _ := json.Marshal(map[string]string{"error": message})
	writeJSON(w, status, "application/json", b)
}

// signedPath is the path the runtime sent (the JWT's aud), mount prefix included.
func signedPath(r *http.Request) string {
	if r.RequestURI != "" && strings.HasPrefix(r.RequestURI, "/") {
		p, _, _ := strings.Cut(r.RequestURI, "?")
		return p
	}
	return r.URL.EscapedPath()
}

func (e *endpoint) bound(r *http.Request) identity.Bound {
	return identity.Bound{Host: r.Host, Hosts: e.cfg.identityHosts}
}

// ServeHTTP answers `GET /discover` and `POST /invoke/{service}/{handler}`.
func (e *endpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.EscapedPath()
	if r.Method == http.MethodGet && path == "/discover" {
		if _, err := identity.Check(e.verifier, r.Header.Get, signedPath(r), identity.JTI(identity.DiscoverJTI), e.bound(r), time.Now()); err != nil {
			writeError(w, http.StatusUnauthorized, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, discoveryV1, e.manifest)
		return
	}
	if r.Method == http.MethodPost {
		if svc, h, ok := e.route(path); ok {
			e.invoke(w, r, svc, h)
			return
		}
	}
	writeError(w, http.StatusNotFound, "no route for "+r.Method+" "+path)
}

func (e *endpoint) route(path string) (*ServiceDef, *HandlerDef, bool) {
	rest, ok := strings.CutPrefix(path, "/invoke/")
	if !ok {
		return nil, nil, false
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, nil, false
	}
	svcName, err1 := url.PathUnescape(parts[0])
	hName, err2 := url.PathUnescape(parts[1])
	if err1 != nil || err2 != nil {
		return nil, nil, false
	}
	svc, ok := e.byName[svcName]
	if !ok {
		return nil, nil, false
	}
	h, ok := svc.Handler(hName)
	return svc, h, ok
}

// invoke runs one REQUEST_RESPONSE turn.
func (e *endpoint) invoke(w http.ResponseWriter, r *http.Request, svc *ServiceDef, h *HandlerDef) {
	asked := wireVersionOf(r.Header.Get("Content-Type"))
	if asked > e.cfg.maxWire {
		// The deployment settled on a version this build does not speak (the process was
		// downgraded without registering again): reading the journal in another version's
		// layout would be wrong, so the handler does not run.
		writeError(w, http.StatusUnsupportedMediaType, fmt.Sprintf(
			"the runtime speaks wire v%d, this SDK supports up to v%d; register the deployment again", asked, e.cfg.maxWire))
		return
	}
	if e.cfg.bidi && r.ProtoMajor >= 2 {
		e.stream(w, r, svc, h, min(asked, e.cfg.maxWire))
		return
	}
	signed := signedPath(r)
	// The header signature before reading the body: a stranger cannot push a body of any
	// size into memory. The jti and bsh are checked against the body afterwards.
	if _, err := identity.Check(e.verifier, r.Header.Get, signed, func(string) bool { return true }, e.bound(r), time.Now()); err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if r.ContentLength > maxRequestBody {
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("body exceeds %d bytes", maxRequestBody))
		return
	}
	// The body must arrive within bodyReadTimeout (the server has no ReadTimeout, which
	// would cut BIDI streams); the deadline is cleared once it is read. A writer without
	// deadlines (a test recorder, the Lambda adapter) has nothing to bound.
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Now().Add(bodyReadTimeout))
	data, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
	if err != nil {
		// The deadline stays: net/http reads the rest of a small unread body before it
		// answers, and the connection is not reused.
		w.Header().Set("Connection", "close")
		writeError(w, http.StatusBadRequest, "cannot read the request body: "+err.Error())
		return
	}
	_ = rc.SetReadDeadline(time.Time{})
	if len(data) > maxRequestBody {
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("body exceeds %d bytes", maxRequestBody))
		return
	}
	if enc := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding"))); enc != "" && enc != "identity" {
		if enc != "gzip" || e.cfg.noCompression {
			writeError(w, http.StatusUnsupportedMediaType, "content-encoding "+enc+" is not enabled (requestCompression)")
			return
		}
		// A platform that already decompressed the body leaves the header as it was.
		if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
			raw, err := gunzip(data, maxRequestBody)
			switch {
			case errors.Is(err, errTooLarge):
				writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("body exceeds %d bytes after decompression", maxRequestBody))
				return
			case err != nil:
				writeError(w, http.StatusBadRequest, "corrupt gzip body")
				return
			}
			data = raw
		}
	}
	if e.verifier != nil {
		// jti binds the JWT to the Start of this very body (another invocation's or another
		// attempt's JWT does not pass); bsh binds it to the body itself.
		sameStart := func(claimed string) bool {
			frames, err := wire.DecodeFrames(data)
			if err != nil || len(frames) == 0 || frames[0].Type != wire.TypeStart {
				return false
			}
			var st wire.Start
			if st.Unmarshal(frames[0].Payload) != nil {
				return false
			}
			return identity.ExpectedJTI(st.ID, st.RetryCountSinceLastStoredEntry) == claimed
		}
		b := e.bound(r)
		b.Body, b.HasBody = data, true
		if _, err := identity.Check(e.verifier, r.Header.Get, signed, sameStart, b, time.Now()); err != nil {
			writeError(w, http.StatusUnauthorized, err.Error())
			return
		}
	}
	wireVersion := min(asked, e.cfg.maxWire)
	// Wire v5 (docs/protocol-v1.md §9.3): only after the request passed the identity check.
	turn := replay.Turn{Body: data}
	if wireVersion >= replayDeltaVersion {
		turn = e.replay.Rejoin(data)
	}
	if turn.Miss {
		// Nothing runs. The part still held lets the runtime resend from there.
		if held := turn.HeldHeader(); held != "" {
			w.Header().Set("cairn-replay-held", held)
		}
		w.Header().Set("cairn-replay", "miss")
		w.Header().Set("Content-Type", invocationV1)
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusConflict)
		return
	}
	cfg := machine.Config{
		Wire:     wireVersion,
		Now:      e.now,
		LogLevel: svc.logLevel(h),
		Context:  r.Context(),
		Values:   e.valueSession(),
	}
	if e.artifact != nil {
		cfg.FetchArtifact = e.fetchArtifact
	}
	out := machine.RequestResponse(h.body, turn.Body, cfg)
	if k := e.replay.Keep(turn, out); k > 0 {
		w.Header().Set("cairn-replay-cache", strconv.Itoa(k))
		if fly := flyMachineID(); fly != "" {
			w.Header().Set("cairn-affinity", "fly="+fly)
		}
	}
	if !e.cfg.noCompression && len(out) >= compressMin && acceptsGzip(r.Header.Get("Accept-Encoding")) {
		if z := gzipFrames(out); z != nil && len(z) < len(out) {
			w.Header().Set("Content-Encoding", "gzip")
			writeJSON(w, http.StatusOK, invocationV1, z)
			return
		}
	}
	writeJSON(w, http.StatusOK, invocationV1, out)
}

// valueSession is the segmented-value state of one request or stream (wire v6).
func (e *endpoint) valueSession() *values.Session {
	return values.NewSession(e.segments, e.fetcher, values.Limits{
		MaxValueBytes:     e.cfg.maxValueBytes,
		MaxTurnValueBytes: e.cfg.maxTurnValueBytes,
	})
}

func (e *endpoint) fetchArtifact(ctx context.Context, invocationID, sha string) ([]byte, error) {
	u := e.cfg.artifactBase + "/invocations/" + url.PathEscape(invocationID) + "/artifacts/" + url.PathEscape(sha)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	for k, vs := range e.cfg.artifactHead {
		req.Header[k] = append([]string(nil), vs...)
	}
	res, err := e.artifact.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cairn: artifact %s: %w", sha, err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cairn: artifact %s: HTTP %d", sha, res.StatusCode)
	}
	// The bytes are kept in the process under sha: anything else (a proxy's error page, a
	// cut download, more than an artifact can hold) is refused, never cached.
	body, err := io.ReadAll(io.LimitReader(res.Body, core.ArtifactMaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("cairn: artifact %s: reading the body: %w", sha, err)
	}
	if len(body) > core.ArtifactMaxBytes {
		return nil, fmt.Errorf("cairn: artifact %s: the download exceeds the limit of %d bytes", sha, core.ArtifactMaxBytes)
	}
	sum := sha256.Sum256(body)
	if !strings.EqualFold(hex.EncodeToString(sum[:]), sha) {
		return nil, fmt.Errorf("cairn: artifact %s: the download does not match its sha256", sha)
	}
	return body, nil
}

var flyID = regexp.MustCompile(`^[A-Za-z0-9]{1,64}$`)

// flyMachineID is FLY_MACHINE_ID when it is a valid machine id: the runtime then pins the
// invocation's next turns to this machine (`cairn-affinity`, docs/protocol-v1.md §9.3),
// where the cache holds its journal.
func flyMachineID() string {
	if id := os.Getenv("FLY_MACHINE_ID"); flyID.MatchString(id) {
		return id
	}
	return ""
}

// acceptsGzip reports whether an Accept-Encoding header invites gzip.
func acceptsGzip(v string) bool {
	for _, p := range strings.Split(v, ",") {
		token, _, _ := strings.Cut(p, ";")
		if strings.EqualFold(strings.TrimSpace(token), "gzip") {
			return true
		}
	}
	return false
}

var errTooLarge = errors.New("too large")

// gunzip decompresses every gzip member of data; errTooLarge past limit bytes.
func gunzip(data []byte, limit int64) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	out, err := io.ReadAll(io.LimitReader(zr, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(out)) > limit {
		return nil, errTooLarge
	}
	return out, nil
}

// gzipFrames compresses a response one gzip member per frame, concatenated (RFC 1952
// allows several members; the runtime reads them as one stream), as the other SDKs do.
// nil when the body does not split into whole frames.
func gzipFrames(data []byte) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	for at := 0; at < len(data); {
		if len(data)-at < wire.HeaderLen {
			return nil
		}
		end := at + wire.HeaderLen + int(binary.BigEndian.Uint32(data[at+4:at+8]))
		if end > len(data) {
			return nil
		}
		zw.Reset(&buf)
		if _, err := zw.Write(data[at:end]); err != nil {
			return nil
		}
		if err := zw.Close(); err != nil {
			return nil
		}
		at = end
	}
	return buf.Bytes()
}
