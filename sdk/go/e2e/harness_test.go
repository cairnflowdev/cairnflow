// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

// The plumbing: start the runtime binary, serve an endpoint in process, talk to the
// admin API with bounded retries.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	cairn "github.com/cairnflowdev/cairnflow/sdk/go"
	"github.com/cairnflowdev/cairnflow/sdk/go/client"
)

// The two transports every suite runs in.
const (
	rr   = "REQUEST_RESPONSE"
	bidi = "BIDI_STREAM"
)

var modes = []string{rr, bidi}

// adminKey is the runtime's bootstrap key: the suite runs with auth on.
var adminKey = "cf_e2e_" + randomHex(16)

// identityMaster makes the runtime sign its requests to the SDK (request identity), so
// endpoints run in the verified configuration: IdentityKeys set to the runtime's kid.
var identityMaster = "go-e2e-identity-master-" + randomHex(16)

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// repoRoot is the repository root: this package lives in sdk/go/e2e.
func repoRoot(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "..")
}

// binary returns the runtime binary, or skips (fails under CAIRNFLOW_E2E_REQUIRE=1) when
// it is missing.
func binary(t testing.TB) string {
	t.Helper()
	p := os.Getenv("CAIRNFLOW_BINARY")
	if p == "" {
		p = filepath.Join(repoRoot(t), "target", "debug", "cairnflow")
	}
	if _, err := os.Stat(p); err != nil {
		msg := fmt.Sprintf("missing runtime binary %s: run `cargo build --bin cairnflow` (or set CAIRNFLOW_BINARY)", p)
		if os.Getenv("CAIRNFLOW_E2E_REQUIRE") == "1" {
			t.Fatal(msg)
		}
		t.Skip(msg)
	}
	return p
}

// within polls cond every tick until it holds or the time runs out; it reports whether
// it held. A bounded poll, never a sleep standing in for synchronisation.
func within(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// logTail keeps the last lines a process printed, for the failure report.
type logTail struct {
	mu    sync.Mutex
	lines []string
}

func (l *logTail) add(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, s)
	if len(l.lines) > 400 {
		l.lines = slices.Clone(l.lines[len(l.lines)-300:])
	}
}

func (l *logTail) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

// node is one runtime process.
type node struct {
	url     string
	cmd     *exec.Cmd
	cluster bool
	log     *logTail
	http    *http.Client
	// api is the Go client, connected with the admin key.
	api *client.Client
}

// nodeArgs are the runtime's arguments: CAIRNFLOW_E2E_ARGS when set (another shape, for
// example a 4-partition cluster), else one node on a fresh data directory.
func nodeArgs(t testing.TB) []string {
	extra := strings.Fields(os.Getenv("CAIRNFLOW_E2E_ARGS"))
	if len(extra) == 0 {
		return []string{"--data-dir", t.TempDir(), "--tick-interval-ms", "50"}
	}
	// A shared bucket: each runtime gets its own prefix, never another run's leases.
	out := make([]string, len(extra))
	for i, a := range extra {
		if strings.HasPrefix(a, "s3://") {
			a = strings.TrimSuffix(a, "/") + "/" + randomHex(4)
		}
		out[i] = a
	}
	return out
}

var listening = regexp.MustCompile(`listening on (http://[0-9.]+:[0-9]+)`)

// startNode starts the runtime with nodeArgs plus more, waits until it serves (and, on a
// cluster, until every partition has a leader), and kills it when the test ends.
func startNode(t testing.TB, more ...string) *node {
	t.Helper()
	return startNodeArgs(t, append(nodeArgs(t), more...))
}

func startNodeArgs(t testing.TB, args []string) *node {
	t.Helper()
	bin := binary(t)
	args = append(slices.Clone(args), "--port", "0", "--node-id", "go-e2e-"+randomHex(3))
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), "CAIRNFLOW_ADMIN_KEY="+adminKey, "CAIRNFLOW_IDENTITY_MASTER="+identityMaster)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	n := &node{cmd: cmd, cluster: slices.Contains(args, "--cluster"), log: &logTail{}, http: &http.Client{Timeout: 60 * time.Second}}
	urlCh := make(chan string, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		sent := false
		for sc.Scan() {
			line := sc.Text()
			n.log.add(line)
			if m := listening.FindStringSubmatch(line); m != nil && !sent {
				urlCh <- m[1]
				sent = true
			}
		}
		_, _ = io.Copy(io.Discard, stdout)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		<-done
		if t.Failed() {
			t.Logf("runtime %s log (tail):\n%s", n.url, n.log)
		}
	})
	select {
	case n.url = <-urlCh:
	case <-done:
		t.Fatalf("runtime exited before it listened:\n%s", n.log)
	case <-time.After(30 * time.Second):
		t.Fatalf("runtime did not report its address within 30 s:\n%s", n.log)
	}
	if !within(30*time.Second, func() bool {
		r, err := n.http.Get(n.url + "/health")
		if err != nil {
			return false
		}
		r.Body.Close()
		return r.StatusCode == http.StatusOK
	}) {
		t.Fatalf("%s/health did not come up", n.url)
	}
	if n.cluster {
		// /health only says HTTP is up; wait until every partition has a leader.
		var last string
		if !within(30*time.Second, func() bool {
			_, body := n.must(t, "GET", "/cluster", nil)
			last = string(body)
			var c struct {
				Partitions []struct {
					Leader any `json:"leader"`
				} `json:"partitions"`
			}
			if json.Unmarshal(body, &c) != nil || len(c.Partitions) == 0 {
				return false
			}
			for _, p := range c.Partitions {
				if p.Leader == nil || p.Leader == "" {
					return false
				}
			}
			return true
		}) {
			t.Fatalf("a partition has no leader after 30 s: %s", last)
		}
	}
	n.api = client.Connect(n.url, client.WithAPIKey(adminKey))
	return n
}

// do sends one admin request with the admin key. It follows a 503 that names the leader,
// backs off on a 503 without one (a lease changing hands) and on a 429 that created
// nothing, all within 20 s; anything else is returned as is.
func (n *node) do(method, path string, body []byte) (int, []byte, http.Header, error) {
	base := n.url
	deadline := time.Now().Add(20 * time.Second)
	for {
		var rd io.Reader
		if body != nil {
			rd = bytes.NewReader(body)
		}
		req, err := http.NewRequest(method, base+path, rd)
		if err != nil {
			return 0, nil, nil, err
		}
		req.Header.Set("Authorization", "Bearer "+adminKey)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		res, err := n.http.Do(req)
		if err != nil {
			if time.Now().After(deadline) {
				return 0, nil, nil, err
			}
			base = n.url
			time.Sleep(100 * time.Millisecond)
			continue
		}
		out, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			return 0, nil, nil, err
		}
		retry := false
		switch res.StatusCode {
		case http.StatusServiceUnavailable:
			var peek struct {
				Leader string `json:"leader"`
			}
			_ = json.Unmarshal(out, &peek)
			if peek.Leader != "" {
				base = strings.TrimSuffix(peek.Leader, "/")
			} else {
				time.Sleep(150 * time.Millisecond)
			}
			retry = true
		case http.StatusTooManyRequests:
			var peek struct {
				Code string `json:"code"`
			}
			_ = json.Unmarshal(out, &peek)
			if peek.Code == "range_overloaded" {
				time.Sleep(500 * time.Millisecond)
				retry = true
			}
		}
		if !retry || time.Now().After(deadline) {
			return res.StatusCode, out, res.Header, nil
		}
	}
}

// must is do that fails the test on a transport error.
func (n *node) must(t testing.TB, method, path string, body []byte) (int, []byte) {
	t.Helper()
	st, out, _, err := n.do(method, path, body)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return st, out
}

// deployment is the answer of POST /deployments.
type deployment struct {
	ID           string   `json:"id"`
	Services     []string `json:"services"`
	ProtocolMode string   `json:"protocolMode"`
	WireVersion  int      `json:"wireVersion"`
}

// register registers the endpoint at uri.
func (n *node) register(t testing.TB, uri string) deployment {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"uri": uri})
	st, out := n.must(t, "POST", "/deployments", body)
	if st != http.StatusOK {
		t.Fatalf("POST /deployments %s: %d %s", uri, st, out)
	}
	var d deployment
	if err := json.Unmarshal(out, &d); err != nil {
		t.Fatalf("POST /deployments: %v: %s", err, out)
	}
	return d
}

// waitStatus polls the invocation until its status is one of want (within 30 s) and
// returns it.
func (n *node) waitStatus(t testing.TB, id string, want ...string) *client.InvocationStatus {
	t.Helper()
	var last *client.InvocationStatus
	if !within(30*time.Second, func() bool {
		st, err := n.api.Invocation(id).Status(context.Background())
		if err != nil {
			return false
		}
		last = st
		return slices.Contains(want, st.Status)
	}) {
		t.Fatalf("invocation %s did not reach %v within 30 s: %+v", id, want, last)
	}
	return last
}

// settle waits until the invocation stops (completed, failed or paused).
func (n *node) settle(t testing.TB, id string) *client.InvocationStatus {
	t.Helper()
	return n.waitStatus(t, id, "completed", "failed", "paused")
}

// output decodes a completed invocation's output into v.
func output(t testing.TB, st *client.InvocationStatus, v any) {
	t.Helper()
	if st.Status != "completed" || st.Output == nil {
		t.Fatalf("invocation %s is %s (code %d %s), want completed", st.ID, st.Status, st.Code, st.Message)
	}
	if err := json.Unmarshal([]byte(*st.Output), v); err != nil {
		t.Fatalf("output of %s: %v: %s", st.ID, err, *st.Output)
	}
}

// journalRow is one entry of GET /invocations/{id}/journal.
type journalRow struct {
	Index   int    `json:"index"`
	Ty      string `json:"ty"`
	Name    string `json:"name"`
	Payload string `json:"payload"`
}

func (n *node) journal(t testing.TB, id string) []journalRow {
	t.Helper()
	st, out := n.must(t, "GET", "/invocations/"+id+"/journal", nil)
	if st != http.StatusOK {
		t.Fatalf("journal of %s: %d %s", id, st, out)
	}
	var rows []journalRow
	if err := json.Unmarshal(out, &rows); err != nil {
		t.Fatalf("journal of %s: %v", id, err)
	}
	return rows
}

// names is the journal's sequence of entry names.
func names(rows []journalRow) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.Name
		if out[i] == "" {
			out[i] = r.Ty
		}
	}
	return out
}

// serveEndpoint serves the services on a fresh local port (HTTP/1.1 and h2c, as
// cairn.Serve does) and returns its URL; the server stops when the test ends.
func serveEndpoint(t testing.TB, opts ...cairn.ServeOption) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return serveOn(t, ln, cairn.HTTPHandler(opts...))
}

func serveOn(t testing.TB, ln net.Listener, h http.Handler) string {
	t.Helper()
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	srv := &http.Server{Handler: h, Protocols: &p, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if srv.Shutdown(ctx) != nil {
			_ = srv.Close()
		}
	})
	return "http://" + ln.Addr().String()
}

// modeOptions are the serve options of a transport.
func modeOptions(mode string) []cairn.ServeOption {
	if mode == bidi {
		return []cairn.ServeOption{cairn.Bidi()}
	}
	return nil
}

// kid is the runtime's active request-identity key (GET /identity), what an endpoint
// passes to cairn.IdentityKeys.
func (n *node) kid(t testing.TB) string {
	t.Helper()
	st, body := n.must(t, "GET", "/identity", nil)
	if st != http.StatusOK {
		t.Fatalf("GET /identity: %d %s", st, body)
	}
	var id struct {
		Signing bool `json:"signing"`
		Keys    []struct {
			Kid    string `json:"kid"`
			Active bool   `json:"active"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(body, &id); err != nil {
		t.Fatalf("GET /identity: %v: %s", err, body)
	}
	for _, k := range id.Keys {
		if k.Active {
			return k.Kid
		}
	}
	t.Fatalf("GET /identity lists no active key: %s", body)
	return ""
}

// verified are the serve options of the secure configuration: requests must carry the
// runtime's signature, and segments are read only from the runtime's own origin (the
// node's `proxy` value read path). The replay cache (wire v5) and segmented values
// (wire v6) are on.
func (n *node) verified(t testing.TB) []cairn.ServeOption {
	return []cairn.ServeOption{cairn.IdentityKeys(n.kid(t)), cairn.SegmentOrigins(n.url)}
}

// bounded is a context for one client call.
func bounded(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}
