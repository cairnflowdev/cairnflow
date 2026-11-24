// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/scripted"
)

// ── state ────────────────────────────────────────────────────────────────

func TestState(t *testing.T) {
	d := scripted.New(scripted.Value(1, `5`), scripted.Void(2), scripted.Keys(3, "a", "total"))
	h := Exclusive("add", func(ctx ObjectContext, n int) (int, error) {
		total, found, err := Get[int](ctx, "total")
		if err != nil || !found {
			return 0, fmt.Errorf("total: %v %v", found, err)
		}
		_, found, err = Get[string](ctx, "missing")
		if err != nil || found {
			return 0, fmt.Errorf("missing: %v %v", found, err)
		}
		keys, err := StateKeys(ctx)
		if err != nil || strings.Join(keys, ",") != "a,total" {
			return 0, fmt.Errorf("keys: %v %v", keys, err)
		}
		if err := Set(ctx, "total", total+n); err != nil {
			return 0, err
		}
		if err := Clear(ctx, "a"); err != nil {
			return 0, err
		}
		if err := ClearAll(ctx); err != nil {
			return 0, err
		}
		if err := Set(ctx, "bad", make(chan int)); err == nil {
			return 0, errors.New("encoding a channel must fail")
		}
		return total + n, nil
	})
	wantOutput(t, turn(t, d, h, "2"), `7`)
	wantCommands(t, d, "GetState total #1", "GetState missing #2", "GetStateKeys getKeys #3",
		"SetState total", "ClearState a", "ClearAllState clearAll")
	if string(d.Commands[3].Value) != "7" {
		t.Fatalf("SetState value %s", d.Commands[3].Value)
	}
}

func TestEagerStateAnswersInPlace(t *testing.T) {
	d := scripted.New()
	d.Eager = map[string][]byte{"name": []byte(`"ada"`)}
	h := Shared("get", NoInput(func(ctx ObjectSharedContext) (string, error) {
		v, found, err := Get[string](ctx, "name")
		if err != nil || !found {
			return "", fmt.Errorf("%v %v", found, err)
		}
		if _, found, _ := Get[string](ctx, "other"); found {
			return "", errors.New("other should be absent")
		}
		return v, nil
	}))
	wantOutput(t, turn(t, d, h, ""), `"ada"`)
	if d.Waits != 0 {
		t.Fatalf("an eager read waited %d times", d.Waits)
	}
}

func TestGetFailureAndBadValue(t *testing.T) {
	d := scripted.New(scripted.Failure(1, 574, "no object key"), scripted.Value(2, `"text"`), scripted.Value(3, ``))
	h := Exclusive("h", NoInput(func(ctx ObjectContext) (string, error) {
		_, _, err := Get[int](ctx, "a")
		if te, ok := AsTerminalError(err); !ok || te.Code() != 574 {
			return "", fmt.Errorf("a: %v", err)
		}
		_, _, err = Get[int](ctx, "b")
		if err == nil || IsTerminalError(err) {
			return "", fmt.Errorf("b: a value of the wrong type is retryable, got %v", err)
		}
		_, _, err = Get[int](ctx, "c")
		if te, ok := AsTerminalError(err); !ok || te.Error() != `state "c" is empty, not valid JSON` {
			return "", fmt.Errorf("c: %v", err)
		}
		return "ok", nil
	}))
	wantOutput(t, turn(t, d, h, ""), `"ok"`)
}

// ── calls ────────────────────────────────────────────────────────────────

func TestCalls(t *testing.T) {
	d := scripted.New(scripted.Value(2, `{"price":3}`), scripted.Failure(4, 404, "no such cart"), scripted.Void(6))
	type quote struct {
		Price int `json:"price"`
	}
	h := Handler("h", NoInput(func(ctx Context) (int, error) {
		q, err := Service[quote](ctx, "Pricing", "quote").Request(map[string]string{"sku": "a<b"},
			WithIdempotencyKey("idem-1"), WithTags(map[string]string{"z": "1", "a": "2"}), WithSpread())
		if err != nil {
			return 0, err
		}
		_, err = Object[quote](ctx, "Cart", "alice", "get").Request(nil, WithName("cart lookup"))
		if te, ok := AsTerminalError(err); !ok || te.Code() != 404 {
			return 0, fmt.Errorf("cart: %v", err)
		}
		if _, err := Workflow[Void](ctx, "Signup", "wf-1", "run").Request(Void{}); err != nil {
			return 0, err
		}
		if err := Service[Void](ctx, "Mailer", "send").Send("hi", WithDelay(90*time.Second)); err != nil {
			return 0, err
		}
		return q.Price, nil
	}))
	wantOutput(t, turn(t, d, h, ""), `3`)
	wantCommands(t, d, "Call Pricing/quote #2", "Call cart lookup #4", "Call Signup/run #6", "OneWayCall Mailer/send")
	c := d.Commands[0].Call
	if string(c.Input) != `{"sku":"a<b"}` || !c.HasIdempotencyKey || c.IdempotencyKey != "idem-1" || !c.Spread ||
		!reflect.DeepEqual(c.Tags, []core.Tag{{Key: "a", Value: "2"}, {Key: "z", Value: "1"}}) {
		t.Fatalf("call %+v", c)
	}
	if k := d.Commands[1].Call.Key; k != "alice" {
		t.Fatalf("object key %q", k)
	}
	if string(d.Commands[1].Call.Input) != "null" || string(d.Commands[2].Call.Input) != "null" {
		t.Fatalf("void inputs %s %s", d.Commands[1].Call.Input, d.Commands[2].Call.Input)
	}
	if d.Commands[3].Delay != 90*time.Second || string(d.Commands[3].Value) != `"hi"` {
		t.Fatalf("send %+v", d.Commands[3])
	}
}

func TestScoped(t *testing.T) {
	d := scripted.New()
	h := Handler("h", NoInput(func(ctx Context) (Void, error) {
		if _, err := ctx.Scoped("bad scope"); err == nil {
			return Void{}, errors.New("a space is not allowed")
		}
		if _, err := ctx.Scoped("acme", "a/b/c"); err == nil {
			return Void{}, errors.New("three levels are not allowed")
		}
		s, err := ctx.Scoped("acme", "growth/alice")
		if err != nil {
			return Void{}, err
		}
		if err := Service[Void](s, "Svc", "a").Send(1); err != nil {
			return Void{}, err
		}
		if err := Service[Void](s, "Svc", "b").Send(1, WithScope("other")); err != nil {
			return Void{}, err
		}
		if err := Service[Void](ctx, "Svc", "c").Send(1, WithLimitKey("x")); err == nil {
			return Void{}, errors.New("a limit key needs a scope")
		}
		return Void{}, nil
	}))
	wantOutput(t, turn(t, d, h, ""), `null`)
	a, b := d.Commands[0].Call, d.Commands[1].Call
	if a.Scope != "acme" || a.LimitKey != "growth/alice" || b.Scope != "other" || b.LimitKey != "growth/alice" {
		t.Fatalf("scopes %+v %+v", a, b)
	}

	d = scripted.New()
	d.InfoValue.WireVersion = 1
	out := turn(t, d, Handler("h", NoInput(func(ctx Context) (Void, error) {
		_, err := ctx.Scoped("acme")
		return Void{}, err
	})), "")
	if out.Kind != core.Failed || out.Failure.Kind != core.TerminalFailure || !strings.Contains(out.Failure.Message, "silently IGNORES") {
		t.Fatalf("outcome %s %+v", out.Kind, out.Failure)
	}
}

// ── signals, awakeables, promises, other invocations ─────────────────────

func TestSignalsAndAwakeables(t *testing.T) {
	d := scripted.New(
		scripted.Signal("approve", `{"by":"bob"}`),
		scripted.IndexSignal(17, `"paid"`),
	)
	d.InfoValue.ID = []byte{0xab, 0xcd}
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		type approval struct{ By string }
		timer := After(ctx, 24*time.Hour)
		approve := Signal[approval](ctx, "approve")
		w, err := WaitFirst(ctx, approve, timer)
		if err != nil || w != Awaitable(approve) {
			return "", fmt.Errorf("approve should win: %v", err)
		}
		a, _ := approve.Result()
		// Once arrived, satisfied forever.
		again, err := Signal[approval](ctx, "approve").Result()
		if err != nil || again != a {
			return "", fmt.Errorf("again: %v %v", again, err)
		}
		awk := Awakeable[string](ctx)
		paid, err := awk.Result()
		if err != nil {
			return "", err
		}
		if err := SendSignal(ctx, "target", "go", nil); err != nil {
			return "", err
		}
		return a.By + " " + paid + " " + awk.ID(), nil
	}))
	wantOutput(t, turn(t, d, h, ""), `"bob paid awk_abcd00000011"`)
	wantCommands(t, d, "Sleep sleep #1", "SendSignal target/go")
	if d.Commands[1].Value != nil {
		t.Fatalf("a signal without a value carries %q", d.Commands[1].Value)
	}
}

func TestPromisesAttachOutput(t *testing.T) {
	d := scripted.New(
		scripted.Value(1, `"yes"`),
		scripted.Void(2),
		scripted.Void(3),
		scripted.Failure(4, 409, "promise already completed"),
		scripted.Value(5, `42`),
		scripted.Void(6),
	)
	h := Handler("h", NoInput(func(ctx WorkflowContext) (string, error) {
		p := Promise[string](ctx, "email-clicked")
		v, err := p.Get()
		if err != nil {
			return "", err
		}
		if _, ok, err := Promise[string](ctx, "other").Peek(); ok || err != nil {
			return "", fmt.Errorf("peek: %v %v", ok, err)
		}
		if err := p.Resolve("again"); err != nil {
			return "", err
		}
		if err := p.Reject(400, "no"); !IsTerminalError(err) {
			return "", fmt.Errorf("reject: %v", err)
		}
		n, err := Attach[int](ctx, "inv-1").Result()
		if err != nil {
			return "", err
		}
		if _, done, err := GetOutput[int](ctx, "inv-2"); done || err != nil {
			return "", fmt.Errorf("output: %v %v", done, err)
		}
		return fmt.Sprintf("%s %d", v, n), nil
	}))
	wantOutput(t, turn(t, d, h, ""), `"yes 42"`)
	wantCommands(t, d, "GetPromise promise/email-clicked #1", "PeekPromise promise/other #2",
		"CompletePromise promise/email-clicked #3", "CompletePromise promise/email-clicked #4",
		"AttachInvocation attach #5", "GetInvocationOutput getOutput #6")
	if string(d.Commands[3].Value) != "failure 400 no" {
		t.Fatalf("reject %s", d.Commands[3].Value)
	}
}

// ── streams, log, trace, artifacts ───────────────────────────────────────

func TestStreamsAndLog(t *testing.T) {
	d := scripted.New()
	d.Ack = true
	d.Clock = 1700000000000
	d.InfoValue.Attempt = 2
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		if _, err := ctx.Stream("$bad"); err == nil {
			return "", errors.New("a name starting with $ is refused")
		}
		ui, err := ctx.Stream("ui")
		if err != nil {
			return "", err
		}
		ui.Append(map[string]string{"tok": "<"})
		ctx.Log().Debug("dropped below info")
		ctx.Log().WithGroup("req").With("id", 7).Info("hello", "user", "ada", slog.Group("g", "x", 1))
		v, err := Run(ctx, "llm", func(rc RunContext) (string, error) {
			ui.Append(map[string]string{"tok": "a"}) // the handler's stream, from the closure
			rc.Log().Warn("in closure")
			s, _ := rc.Stream("progress", Durable())
			s.Append(1)
			s.Close()
			return "ab", nil
		})
		return v, err
	}))
	wantOutput(t, turn(t, d, h, ""), `"ab"`)
	var got []string
	for _, r := range d.Records {
		got = append(got, fmt.Sprintf("%s@%d/%d/%d:%s:%v:%v", r.Stream, r.Ident.Step, r.Ident.Attempt, r.Ident.K, r.Payload, r.Durable, r.Close))
	}
	want := []string{
		`ui@0/2/0:{"tok":"<"}:false:false`,
		`$log@0/2/0:{"ts":1700000000000,"level":"info","msg":"hello","attrs":{"req":{"id":7,"user":"ada","g":{"x":1}}}}:true:false`,
		`ui@0/2/1:{"tok":"a"}:false:false`,
		`$log@0/2/1:{"ts":1700000000000,"level":"warn","msg":"in closure"}:true:false`,
		`progress@0/2/0:1:true:false`,
		`progress@0/0/0::true:true`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("records:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestLogLevelFromInfo(t *testing.T) {
	d := scripted.New()
	d.InfoValue.LogLevel = "debug"
	turn(t, d, Handler("h", NoInput(func(ctx Context) (Void, error) {
		ctx.Log().Debug("kept")
		return Void{}, nil
	})), "")
	if len(d.Records) != 1 || !strings.Contains(string(d.Records[0].Payload), `"level":"debug"`) {
		t.Fatalf("records %+v", d.Records)
	}
}

func TestSpans(t *testing.T) {
	d := scripted.New(scripted.Value(1, `"r"`))
	d.InfoValue.ID = []byte{1}
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		return WithSpan(ctx, "plan", func(s *Span) (string, error) {
			s.Set("tokens", 12)
			_, err := WithSpan(ctx, "inner", func(*Span) (int, error) {
				return 0, errors.New("tool failed")
			})
			if err == nil {
				return "", errors.New("inner error lost")
			}
			return Run(ctx, "llm", constant("x"))
		}, "model", "m")
	}))
	wantOutput(t, turn(t, d, h, ""), `"r"`)
	var payloads []string
	for _, r := range d.Records {
		if r.Stream != "$trace" {
			t.Fatalf("stream %s", r.Stream)
		}
		payloads = append(payloads, string(r.Payload))
	}
	re := regexp.MustCompile(`"span_id":"([0-9a-f]{16})"`)
	outer := re.FindStringSubmatch(payloads[0])[1]
	inner := re.FindStringSubmatch(payloads[1])[1]
	want := []string{
		`{"kind":"start","span_id":"` + outer + `","parent_id":null,"name":"plan","ts":0,"attrs":{"model":"m"}}`,
		`{"kind":"start","span_id":"` + inner + `","parent_id":"` + outer + `","name":"inner","ts":0}`,
		`{"kind":"end","span_id":"` + inner + `","parent_id":"` + outer + `","name":"inner","ts":0,"status":"error","error":"tool failed"}`,
		`{"kind":"end","span_id":"` + outer + `","parent_id":null,"name":"plan","ts":0,"status":"ok","attrs":{"tokens":12}}`,
	}
	if strings.Join(payloads, "\n") != strings.Join(want, "\n") {
		t.Fatalf("trace:\n%s\nwant:\n%s", strings.Join(payloads, "\n"), strings.Join(want, "\n"))
	}
	if outer != spanID("01", core.Ident{Step: 0, K: 0}) || outer == inner {
		t.Fatalf("span ids %s %s", outer, inner)
	}
}

// A span the turn stops inside stays open (no end record): the next turn closes it.
func TestSpanLeftOpenWhenTurnStops(t *testing.T) {
	d := scripted.New()
	h := Handler("h", NoInput(func(ctx Context) (Void, error) {
		return WithSpan(ctx, "wait", func(*Span) (Void, error) {
			return Void{}, Sleep(ctx, time.Hour)
		})
	}))
	wantStop(t, turn(t, d, h, ""), core.StopSuspended)
	if len(d.Records) != 1 || !strings.Contains(string(d.Records[0].Payload), `"kind":"start"`) {
		t.Fatalf("records %+v", d.Records)
	}
}

func TestArtifacts(t *testing.T) {
	d := scripted.New()
	d.Ack = true
	d.Remote = map[string][]byte{"feed": []byte("remote bytes")}
	h := Handler("h", NoInput(func(ctx Context) (string, error) {
		ref, err := Run(ctx, "render", func(rc RunContext) (ArtifactRef, error) {
			return rc.Artifacts().Put([]byte("report"), "text/plain")
		})
		if err != nil {
			return "", err
		}
		local, err := ctx.Artifacts().Get(ref.SHA256)
		if err != nil {
			return "", err
		}
		remote, err := ctx.Artifacts().Get("feed")
		if err != nil {
			return "", err
		}
		raw, _ := json.Marshal(ref)
		return string(local) + "|" + string(remote) + "|" + string(raw), nil
	}))
	const sha = "845e91831319e89c4d656bdb80c278ac09a7230d61e5dfd2e1b1fbb436ac8917" // sha256("report")
	out := turn(t, d, h, "")
	if out.Kind != core.Returned {
		t.Fatalf("outcome %s %+v %v", out.Kind, out.Failure, out.Stop)
	}
	var s string
	_ = json.Unmarshal(out.Output, &s)
	parts := strings.Split(s, "|")
	if parts[0] != "report" || parts[1] != "remote bytes" || !strings.Contains(parts[2], `"size":6,"contentType":"text/plain"`) {
		t.Fatalf("output %s", s)
	}
	if len(d.Artifacts) != 1 || d.Artifacts[0].Scope != (core.Leaf{Kind: core.CompletionLeaf, ID: 1}) || d.Artifacts[0].SHA != sha {
		t.Fatalf("artifacts %+v", d.Artifacts)
	}
}

// ── randomness ───────────────────────────────────────────────────────────

func TestRandomMatchesTypeScript(t *testing.T) {
	// The first draws of the TypeScript SDK's random() for seed 42 (testdata/golden.ts).
	want := []float64{0.33908526400192196, 0.7822558479199243, 0.7901370452687786}
	d := scripted.New()
	d.InfoValue.RandomSeed = 42
	var got []float64
	var uuid string
	turn(t, d, Handler("h", NoInput(func(ctx Context) (Void, error) {
		for range want {
			got = append(got, ctx.Random())
		}
		uuid = UUID(ctx)
		return Void{}, nil
	})), "")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(uuid) {
		t.Fatalf("uuid %s", uuid)
	}
	// The same seed gives the same UUID on replay.
	d2 := scripted.New()
	d2.InfoValue.RandomSeed = 42
	var again string
	turn(t, d2, Handler("h", NoInput(func(ctx Context) (Void, error) {
		for range want {
			ctx.Random()
		}
		again = UUID(ctx)
		return Void{}, nil
	})), "")
	if again != uuid {
		t.Fatalf("uuid %s then %s", uuid, again)
	}
}

// ── values ───────────────────────────────────────────────────────────────

func TestInputAndOutputForms(t *testing.T) {
	type in struct {
		Name string `json:"name"`
	}
	cases := []struct {
		h     *HandlerDef
		input string
		want  string
	}{
		{Handler("s", func(_ Context, s string) (string, error) { return s, nil }), `"json"`, `"json"`},
		{Handler("s", func(_ Context, s string) (string, error) { return s, nil }), `not json`, `"not json"`},
		{Handler("s", func(_ Context, s string) (string, error) { return s, nil }), ``, `""`},
		{Handler("b", func(_ Context, b []byte) ([]byte, error) { return b, nil }), `{"raw": 1}`, `{"raw": 1}`},
		{Handler("r", func(_ Context, r json.RawMessage) (json.RawMessage, error) { return r, nil }), `[1,2]`, `[1,2]`},
		{Handler("v", NoInput(func(Context) (Void, error) { return Void{}, nil })), `ignored`, `null`},
		{Handler("st", func(_ Context, v in) (in, error) { return v, nil }), `{"name":"<ada>"}`, `{"name":"<ada>"}`},
		{Handler("st", func(_ Context, v in) (*in, error) { return nil, nil }), ``, `null`},
	}
	for _, c := range cases {
		wantOutput(t, turn(t, scripted.New(), c.h, c.input), c.want)
	}
	out := turn(t, scripted.New(), Handler("st", func(_ Context, v in) (in, error) { return v, nil }), `not json`)
	if out.Kind != core.Failed || out.Failure.Kind != core.TerminalFailure || out.Failure.Code != 400 {
		t.Fatalf("invalid input: %s %+v", out.Kind, out.Failure)
	}
	out = turn(t, scripted.New(), Handler("o", NoInput(func(Context) (chan int, error) { return nil, nil })), ``)
	if out.Kind != core.Failed || out.Failure.Kind != core.RetryFailure {
		t.Fatalf("unencodable output: %s %+v", out.Kind, out.Failure)
	}
}

// ── errors ───────────────────────────────────────────────────────────────

func TestErrorHelpers(t *testing.T) {
	if ToTerminalError(nil) != nil || ToRetryableError(nil) != nil || ToPauseError(nil) != nil {
		t.Fatal("nil in, nil out")
	}
	base := errors.New("disk full")
	te := ToTerminalError(base)
	if !IsTerminalError(te) || !errors.Is(te, base) || te.Error() != "disk full" {
		t.Fatalf("%v", te)
	}
	if got, _ := AsTerminalError(te); got.Code() != 500 {
		t.Fatalf("code %d", got.Code())
	}
	wrapped := fmt.Errorf("ctx: %w", TerminalErrorf(422, "bad"))
	if got, _ := AsTerminalError(ToTerminalError(wrapped)); got.Code() != 422 {
		t.Fatalf("kept code %d", got.Code())
	}
	if got, _ := AsTerminalError(ToTerminalError(wrapped, WithCode(409))); got.Code() != 409 {
		t.Fatalf("override code %d", got.Code())
	}
	if got, _ := AsTerminalError(TerminalErrorf(-1, "x")); got.Code() != 500 {
		t.Fatalf("invalid code becomes 500, got %d", got.Code())
	}
	var re *RetryableError
	if !errors.As(RetryableErrorf(503, "busy"), &re) || re.Code() != 503 || re.RetryAfter() != 0 {
		t.Fatalf("%+v", re)
	}
	var pe *PauseError
	if !errors.As(PauseErrorf("restock %s", "sku"), &pe) || pe.Code() != 500 || pe.Error() != "restock sku" {
		t.Fatalf("%+v", pe)
	}
}

// ── registration ─────────────────────────────────────────────────────────

func TestHandlerKindsFollowTheContextType(t *testing.T) {
	cases := []struct {
		h    *HandlerDef
		kind ctxKind
		ty   HandlerType
	}{
		{Handler("a", NoInput(func(Context) (Void, error) { return Void{}, nil })), serviceKind, ""},
		{Handler("b", NoInput(func(ObjectContext) (Void, error) { return Void{}, nil })), objectKind, ""},
		{Exclusive("c", NoInput(func(ObjectContext) (Void, error) { return Void{}, nil })), objectKind, HandlerTypeExclusive},
		{Handler("d", NoInput(func(ObjectSharedContext) (Void, error) { return Void{}, nil })), objectSharedKind, HandlerTypeShared},
		{Shared("e", NoInput(func(WorkflowSharedContext) (Void, error) { return Void{}, nil })), workflowSharedKind, HandlerTypeShared},
		{Main("run", NoInput(func(WorkflowContext) (Void, error) { return Void{}, nil })), workflowKind, HandlerTypeWorkflow},
	}
	for _, c := range cases {
		if c.h.kind != c.kind || c.h.ty != c.ty {
			t.Errorf("%s: kind %s ty %q, want %s %q", c.h.name, c.h.kind, c.h.ty, c.kind, c.ty)
		}
	}
	mustPanic(t, "shared over an exclusive context", func() {
		Shared("x", NoInput(func(ObjectContext) (Void, error) { return Void{}, nil }))
	})
	mustPanic(t, "service handler in an object", func() {
		NewObject("O", Handler("x", NoInput(func(Context) (Void, error) { return Void{}, nil })))
	})
	mustPanic(t, "workflow without main", func() {
		NewWorkflow("W", Shared("x", NoInput(func(WorkflowSharedContext) (Void, error) { return Void{}, nil })))
	})
	mustPanic(t, "duplicate names", func() {
		h := Handler("x", NoInput(func(Context) (Void, error) { return Void{}, nil }))
		NewService("S", h, h)
	})
	mustPanic(t, "no handlers", func() { NewService("S") })
}

// A narrow context cannot be widened with a type assertion.
func TestSharedContextCannotBeWidened(t *testing.T) {
	d := scripted.New()
	h := Shared("peek", NoInput(func(ctx ObjectSharedContext) (bool, error) {
		_, writable := ctx.(ObjectContext)
		return writable, nil
	}))
	wantOutput(t, turn(t, d, h, ""), `false`)
}

func TestLogLevelOption(t *testing.T) {
	h := Handler("x", NoInput(func(Context) (Void, error) { return Void{}, nil }), WithLogLevel(slog.LevelDebug))
	g := Handler("y", NoInput(func(Context) (Void, error) { return Void{}, nil }))
	s := NewService("S", h, g).With(WithLogLevel(slog.LevelWarn))
	if s.logLevel(h) != "debug" || s.logLevel(g) != "warn" {
		t.Fatalf("%q %q", s.logLevel(h), s.logLevel(g))
	}
	if l, ok := s.Handler("y"); !ok || l != g || len(s.Handlers()) != 2 || s.Type() != ServiceTypeService {
		t.Fatal("lookup")
	}
}

func mustPanic(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s: no panic", what)
		}
	}()
	f()
}

// The manifest is byte for byte what the TypeScript SDK's manifest() produces for the
// same definitions (testdata/manifest.json, from testdata/golden.ts).
func TestManifestMatchesTypeScript(t *testing.T) {
	raw, err := os.ReadFile("testdata/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var want struct{ RR, Bidi string }
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	nop := NoInput(func(Context) (Void, error) { return Void{}, nil })
	greeter := NewService("Greeter",
		Handler("greet", nop,
			WithJournalRetention(24*time.Hour), WithRuntimeTrace(), WithLogFlush(500*time.Millisecond),
			WithRetryPolicy(RetryPolicy{InitialInterval: 100 * time.Millisecond, MaxInterval: 10 * time.Second, ExponentiationFactor: 1.5})),
		Handler("hello", nop),
	).With(WithInactivityTimeout(time.Minute), WithRetryPolicy(RetryPolicy{MaxAttempts: 5, OnMaxAttempts: PauseOnMaxAttempts}))
	objNop := NoInput(func(ObjectContext) (Void, error) { return Void{}, nil })
	counter := NewObject("Counter",
		Handler("add", objNop),
		Exclusive("inc", objNop),
		Shared("peek", NoInput(func(ObjectSharedContext) (Void, error) { return Void{}, nil })),
	)
	signup := NewWorkflow("Signup",
		Main("run", NoInput(func(WorkflowContext) (Void, error) { return Void{}, nil })),
		Shared("click", NoInput(func(WorkflowSharedContext) (Void, error) { return Void{}, nil })),
	).With(WithJournalRetention(time.Hour))

	if got := string(manifestJSON(manifestOptions{}, greeter, counter, signup)); got != want.RR {
		t.Errorf("REQUEST_RESPONSE manifest:\n  got  %s\n  want %s", got, want.RR)
	}
	got := string(manifestJSON(manifestOptions{bidi: true, maxWire: 6, requestIdentity: true, compression: true, replayCache: true}, greeter))
	if got != want.Bidi {
		t.Errorf("BIDI_STREAM manifest:\n  got  %s\n  want %s", got, want.Bidi)
	}
	if got := string(manifestJSON(manifestOptions{maxWire: 4, replayCache: true}, greeter)); strings.Contains(got, "replayCache") {
		t.Errorf("replayCache needs wire v5: %s", got)
	}
}
