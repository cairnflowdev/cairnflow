// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

// Same program, two transports, the same journal byte for byte: what lets an invocation
// start in one mode, lose its node, and continue on a deployment that speaks the other.
// A suite green in both modes only says the results are the same; this compares entries.

import (
	"fmt"
	"net"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"testing"
	"time"

	cairn "github.com/cairnflowdev/cairnflow/sdk/go"
	"github.com/cairnflowdev/cairnflow/sdk/go/client"
)

type walkResult struct {
	A       string   `json:"a"`
	B       string   `json:"b"`
	Missing bool     `json:"missing"`
	Keys    []string `json:"keys"`
}

// walk produces every kind of entry a turn can, from no nondeterministic source: no
// sleep (its wake-up time is absolute) and no call (the callee id derives from the
// caller's). Run and lazy state reads are where the transports differ most: the runtime
// answers them at once, on the open stream in BIDI_STREAM, in a new request in
// REQUEST_RESPONSE.
func walk(ctx cairn.ObjectContext, tag string) (walkResult, error) {
	var r walkResult
	var err error
	if r.A, err = cairn.Run(ctx, "a", func(cairn.RunContext) (string, error) { return tag + "-a", nil }); err != nil {
		return r, err
	}
	_, found, err := cairn.Get[int](ctx, "not-set")
	if err != nil {
		return r, err
	}
	r.Missing = !found
	if err := cairn.Set(ctx, "k", 1); err != nil {
		return r, err
	}
	if r.Keys, err = cairn.StateKeys(ctx); err != nil {
		return r, err
	}
	r.B, err = cairn.Run(ctx, "b", func(cairn.RunContext) (string, error) { return tag + "-b", nil })
	return r, err
}

type slowResult struct {
	A string `json:"a"`
	B string `json:"b"`
}

// slowly suspends on something only the outside world answers (a long timer), with a
// Run on each side: the first half is journaled in one transport, the second replays it
// in the other.
func slowly(ctx cairn.ObjectContext) (slowResult, error) {
	key := ctx.Key()
	step := func(name string) (string, error) {
		return cairn.Run(ctx, name, func(cairn.RunContext) (string, error) {
			return fmt.Sprintf("%s-%d", name, counter("slowly/"+key).Add(1)), nil
		})
	}
	a, err := step("a")
	if err != nil {
		return slowResult{}, err
	}
	if err := cairn.Sleep(ctx, time.Hour, cairn.WithName("wait")); err != nil {
		return slowResult{}, err
	}
	b, err := step("b")
	return slowResult{A: a, B: b}, err
}

var shapes = cairn.NewObject("GoShapes",
	cairn.Exclusive("walk", walk),
	cairn.Exclusive("slowly", cairn.NoInput(slowly)),
)

// deploymentHeader is entry 0's `cairnflow-deployment` header (Header{key, value "dp_" +
// 28 hex}), present on some runtime shapes (a cluster): the id of each runtime's
// deployment, rightly different on the two sides.
var deploymentHeader = regexp.MustCompile(`0a370a14636169726e666c6f772d6465706c6f796d656e74121f[0-9a-f]{62}`)

func withoutDeployment(rows []journalRow) []journalRow {
	out := slices.Clone(rows)
	if len(out) > 0 && out[0].Index == 0 {
		out[0].Payload = deploymentHeader.ReplaceAllString(out[0].Payload, "")
	}
	return out
}

func TestJournalParity(t *testing.T) {
	journals := map[string][]journalRow{}
	outputs := map[string]walkResult{}
	for _, mode := range modes {
		n := startNode(t)
		uri := serveEndpoint(t, append(append(modeOptions(mode), shapes), n.verified(t)...)...)
		if reg := n.register(t, uri); reg.ProtocolMode != mode {
			t.Fatalf("the runtime settled on %s, want %s", reg.ProtocolMode, mode)
		}
		// No generated idempotency key: entry 0 carries it, and it would differ.
		inv, err := client.Send(bounded(t), n.api.Object("GoShapes", "parity"), "walk", "t", client.NoIdempotencyKey())
		if err != nil {
			t.Fatal(err)
		}
		var out walkResult
		output(t, n.settle(t, inv.ID()), &out)
		outputs[mode] = out
		journals[mode] = withoutDeployment(n.journal(t, inv.ID()))
	}
	want := walkResult{A: "t-a", B: "t-b", Missing: true, Keys: []string{"k"}}
	for mode, out := range outputs {
		if out.A != want.A || out.B != want.B || out.Missing != want.Missing || !slices.Equal(out.Keys, want.Keys) {
			t.Fatalf("%s: result %+v, want %+v", mode, out, want)
		}
	}
	shape := []string{"Input", "Run", "RunCompletion", "GetState", "GetStateCompletion", "SetState",
		"GetStateKeys", "GetStateKeysCompletion", "Run", "RunCompletion", "Output"}
	if got := names(journals[bidi]); !slices.Equal(got, shape) {
		t.Fatalf("journal shape %v, want %v", got, shape)
	}
	a, b := journals[bidi], journals[rr]
	if len(a) != len(b) {
		t.Fatalf("BIDI_STREAM wrote %d entries, REQUEST_RESPONSE %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Errorf("entry %d differs:\n  BIDI_STREAM      %+v\n  REQUEST_RESPONSE %+v", i, a[i], b[i])
		}
	}
}

// An invocation started under BIDI_STREAM continues under REQUEST_RESPONSE: the endpoint
// is replaced on the same port by one declaring the other transport, registered as a new
// deployment, and the invocation migrated to it. The Run before the suspension keeps its
// durable value: it does not run again.
func TestTransportSwitchMidway(t *testing.T) {
	n := startNode(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	stop := serveUntil(t, ln, cairn.HTTPHandler(append(n.verified(t), cairn.Bidi(), shapes)...))
	if reg := n.register(t, "http://"+addr); reg.ProtocolMode != bidi {
		t.Fatalf("settled on %s", reg.ProtocolMode)
	}
	key := "switch-" + randomHex(3)
	inv, err := client.Send(bounded(t), n.api.Object("GoShapes", key), "slowly", nil)
	if err != nil {
		t.Fatal(err)
	}
	n.waitStatus(t, inv.ID(), "suspended")
	if c := counter("slowly/" + key).Load(); c != 1 {
		t.Fatalf("the Run before the suspension ran %d times, want 1", c)
	}

	stop()
	ln, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listening again on %s: %v", addr, err)
	}
	serveOn(t, ln, cairn.HTTPHandler(append(n.verified(t), shapes)...))
	reg := n.register(t, "http://"+addr)
	if reg.ProtocolMode != rr {
		t.Fatalf("settled on %s, want %s", reg.ProtocolMode, rr)
	}
	st, body := n.must(t, "POST", "/invocations/"+inv.ID()+"/migrate?deployment="+reg.ID, nil)
	if st != http.StatusOK {
		t.Fatalf("migrate: %d %s", st, body)
	}
	now := strconv.FormatInt(time.Now().Add(2*time.Hour).UnixMilli(), 10)
	if st, body := n.must(t, "POST", "/admin/tick?now_ms="+now, nil); st != http.StatusOK {
		t.Fatalf("tick: %d %s", st, body)
	}
	var got slowResult
	output(t, n.settle(t, inv.ID()), &got)
	if got != (slowResult{A: "a-1", B: "b-2"}) {
		t.Fatalf("result after the switch %+v, want a-1 and b-2 (a must not run again)", got)
	}
}

// serveUntil is serveOn with a function that stops the server early (closing every
// connection, as a process that goes away would).
func serveUntil(t testing.TB, ln net.Listener, h http.Handler) func() {
	t.Helper()
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	srv := &http.Server{Handler: h, Protocols: &p, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	stop := func() { _ = srv.Close() }
	t.Cleanup(stop)
	return stop
}
