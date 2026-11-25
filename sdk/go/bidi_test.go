// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/bidi"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/identity"
	machine "github.com/cairnflowdev/cairnflow/sdk/go/internal/turn"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

// fakeStream is the runtime end of a BIDI_STREAM stream, in memory: what the test sends
// is the request body, what the SDK writes arrives frame by frame.
type fakeStream struct {
	t      *testing.T
	body   *io.PipeWriter
	frames chan wire.Frame
	result chan error
}

func openStream(t *testing.T, h *HandlerDef, wireVersion int) *fakeStream {
	t.Helper()
	pr, pw := io.Pipe()
	s := &fakeStream{t: t, body: pw, frames: make(chan wire.Frame, 256), result: make(chan error, 1)}
	cfg := bidi.Config{Turn: machine.Config{Wire: wireVersion, Now: func() int64 { return testClock }}}
	var sp wire.Splitter
	go func() {
		err := bidi.Serve(h.body, pr, func(b []byte) error {
			fs, err := sp.Push(b)
			for _, f := range fs {
				s.frames <- f
			}
			return err
		}, cfg)
		close(s.frames)
		s.result <- err
	}()
	return s
}

func (s *fakeStream) send(msgs ...wire.Message) {
	s.t.Helper()
	var b []byte
	for _, m := range msgs {
		b = wire.AppendMessage(b, m, 0)
	}
	if _, err := s.body.Write(b); err != nil {
		s.t.Fatalf("send: %v", err)
	}
}

// expect reads the next frames, skipping keepalives, and checks their types.
func (s *fakeStream) expect(types ...wire.MessageType) []wire.Frame {
	s.t.Helper()
	var out []wire.Frame
	for _, ty := range types {
		for {
			f, ok := <-s.frames
			if !ok {
				s.t.Fatalf("the SDK ended the stream, want %s", ty)
			}
			if f.Type == wire.TypeKeepalive && ty != wire.TypeKeepalive {
				continue
			}
			if f.Type != ty {
				s.t.Fatalf("the SDK sent %s, want %s", f.Type, ty)
			}
			out = append(out, f)
			break
		}
	}
	return out
}

// ends: the SDK ends the stream on its own, with no further frame but keepalives.
func (s *fakeStream) ends() {
	s.t.Helper()
	for f := range s.frames {
		if f.Type != wire.TypeKeepalive {
			s.t.Fatalf("the SDK sent %s, want the end of the stream", f.Type)
		}
	}
	if err := <-s.result; err != nil {
		s.t.Fatalf("Serve: %v", err)
	}
	_ = s.body.Close()
}

func ka(ms uint64) *uint64 { return &ms }

func decodeAs[M any, P interface {
	*M
	Unmarshal([]byte) error
}](t *testing.T, f wire.Frame) *M {
	t.Helper()
	m := P(new(M))
	if err := m.Unmarshal(f.Payload); err != nil {
		t.Fatal(err)
	}
	return m
}

// A closure longer than the early-flush delay has its Run flushed, then a Keepalive every
// interval until it returns (docs/protocol-v1.md §9.1, "Keepalive (v3)"); the proposal
// asks for an ack, and the next turn runs only once the ack is in.
func TestBidiKeepaliveWhileARunIsSlow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		h := Handler("slow", NoInput(func(ctx Context) (string, error) {
			return Run(ctx, "slow", func(RunContext) (string, error) {
				<-release
				return "done", nil
			})
		}))
		s := openStream(t, h, 3)
		begin := time.Now()
		s.send(&wire.Start{ID: []byte{1}, KnownEntries: 1, KeepaliveIntervalMs: ka(100)}, input())
		s.expect(wire.TypeRun)
		if d := time.Since(begin); d != runEarlyFlushForTest {
			t.Fatalf("Run flushed after %s, want %s", d, runEarlyFlushForTest)
		}
		for i := 1; i <= 3; i++ {
			s.expect(wire.TypeKeepalive)
			if d, want := time.Since(begin), runEarlyFlushForTest+time.Duration(i)*100*time.Millisecond; d != want {
				t.Fatalf("keepalive %d after %s, want %s", i, d, want)
			}
		}
		close(release)
		p := s.expect(wire.TypeProposeRunCompletion, wire.TypeSuspension)[0]
		if !p.RequestedAck() {
			t.Fatal("BIDI_STREAM asks for the ack of a proposal")
		}
		synctest.Wait()
		select {
		case f := <-s.frames:
			t.Fatalf("the SDK sent %s before the ack", f.Type)
		default:
		}
		s.send(&wire.ProposeRunCompletionAck{CompletionID: 1})
		out := s.expect(wire.TypeOutput, wire.TypeEnd)
		if o := decodeAs[wire.Output](t, out[0]); string(o.Result.Value.Content) != `"done"` {
			t.Fatalf("output %s", o.Result.Value.Content)
		}
		s.ends()
	})
}

// runEarlyFlushForTest mirrors the turn machine's early-flush delay.
const runEarlyFlushForTest = 20 * time.Millisecond

// A short closure flushes nothing early: its Run goes with its proposal, and no keepalive
// is sent.
func TestBidiShortRunSendsNoKeepalive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := Handler("quick", NoInput(func(ctx Context) (string, error) {
			return Run(ctx, "quick", constant("ok"))
		}))
		s := openStream(t, h, 3)
		s.send(&wire.Start{ID: []byte{1}, KnownEntries: 1, KeepaliveIntervalMs: ka(5)}, input())
		for _, want := range []wire.MessageType{wire.TypeRun, wire.TypeProposeRunCompletion, wire.TypeSuspension} {
			if f := <-s.frames; f.Type != want {
				t.Fatalf("the SDK sent %s, want %s", f.Type, want)
			}
		}
		s.send(&wire.ProposeRunCompletionAck{CompletionID: 1})
		s.expect(wire.TypeOutput, wire.TypeEnd)
		s.ends()
	})
}

// Two closures of one epoch propose in completion-id order, whichever returns first, even
// when a live flush happens in between (Turn.Finish: where proposals go).
func TestBidiProposalsKeepTheirOrderAcrossLiveWrites(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		h := Handler("two", NoInput(func(ctx Context) (string, error) {
			a := RunAsync(ctx, "a", func(RunContext) (string, error) {
				<-release
				return "a", nil
			})
			b := RunAsync(ctx, "b", constant("b"))
			if err := All(ctx, a, b); err != nil {
				return "", err
			}
			return "both", nil
		}))
		s := openStream(t, h, 3)
		s.send(&wire.Start{ID: []byte{1}, KnownEntries: 1, KeepaliveIntervalMs: ka(10)}, input())
		s.expect(wire.TypeRun, wire.TypeRun, wire.TypeKeepalive)
		close(release)
		ps := s.expect(wire.TypeProposeRunCompletion, wire.TypeProposeRunCompletion, wire.TypeSuspension)
		for i, p := range ps[:2] {
			if id := decodeAs[wire.ProposeRunCompletion](t, p).ResultCompletionID; id != uint32(i+1) {
				t.Fatalf("proposal %d is for completion %d", i, id)
			}
		}
		s.send(&wire.ProposeRunCompletionAck{CompletionID: 1}, &wire.ProposeRunCompletionAck{CompletionID: 2})
		s.expect(wire.TypeOutput, wire.TypeEnd)
		s.ends()
	})
}

// Stream records of a running closure go on the wire while it runs, after its Run command
// (docs/protocol-v1.md §9.1, "Streams (v4)") — even when that means putting the commands
// of a later epoch out while a closure of an earlier one still runs: its proposal then goes
// as soon as it is built, after them (the one exception to the proposal order).
func TestBidiStreamRecordsDoNotWaitForASlowClosure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		h := Handler("talk", NoInput(func(ctx Context) (string, error) {
			timer := After(ctx, time.Second)
			slow := RunAsync(ctx, "slow", func(RunContext) (string, error) {
				<-release
				return "slow", nil
			})
			if _, err := timer.Result(); err != nil { // delivered from the journal: a new epoch
				return "", err
			}
			ui, err := ctx.Stream("ui")
			if err != nil {
				return "", err
			}
			talk := RunAsync(ctx, "talk", func(RunContext) (string, error) {
				ui.Append("hello")
				return "said", nil
			})
			if err := All(ctx, slow, talk); err != nil {
				return "", err
			}
			return "done", nil
		}))
		s := openStream(t, h, 4)
		sleep := &wire.Sleep{WakeUpTime: testClock + 1000, ResultCompletionID: 1, Name: "sleep"}
		s.send(&wire.Start{ID: []byte{1}, KnownEntries: 3}, input(), sleep, &wire.SleepCompletion{ID: 1})
		frames := s.expect(wire.TypeRun, wire.TypeRun, wire.TypeProposeRunCompletion, wire.TypeStreamAppend)
		if names := []string{decodeAs[wire.Run](t, frames[0]).Name, decodeAs[wire.Run](t, frames[1]).Name}; !slices.Equal(names, []string{"slow", "talk"}) {
			t.Fatalf("runs %v", names)
		}
		if id := decodeAs[wire.ProposeRunCompletion](t, frames[2]).ResultCompletionID; id != 3 {
			t.Fatalf("the first proposal is for %d, want 3 (talk)", id)
		}
		close(release)
		p := s.expect(wire.TypeProposeRunCompletion, wire.TypeSuspension)[0]
		if id := decodeAs[wire.ProposeRunCompletion](t, p).ResultCompletionID; id != 2 {
			t.Fatalf("the late proposal is for %d, want 2 (slow)", id)
		}
		s.send(&wire.ProposeRunCompletionAck{CompletionID: 3}, &wire.ProposeRunCompletionAck{CompletionID: 2})
		s.expect(wire.TypeOutput, wire.TypeEnd)
		s.ends()
	})
}

// A CANCEL arriving mid-turn aborts a running cancellable closure: nothing is proposed and
// the handler sees the cancellation (docs/protocol-v1.md §8.7).
func TestBidiCancelMidTurnAbortsCancellableRuns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		var sawCancel atomic.Bool
		h := Handler("wait", NoInput(func(ctx Context) (string, error) {
			_, err := Run(ctx, "wait", func(rc RunContext) (string, error) {
				close(started)
				<-rc.Done()
				sawCancel.Store(true)
				return "", rc.Err()
			}, Cancellable())
			return "", err
		}))
		s := openStream(t, h, 3)
		s.send(&wire.Start{ID: []byte{1}, KnownEntries: 1}, input())
		<-started
		s.send(&wire.SignalNotification{Signal: wire.SignalIdx(wire.SignalCancel), Result: wire.Result{Void: true}})
		frames := s.expect(wire.TypeRun, wire.TypeOutput, wire.TypeEnd)
		if !sawCancel.Load() {
			t.Fatal("the closure was not cancelled")
		}
		o := decodeAs[wire.Output](t, frames[1])
		if o.Result.Failure == nil || o.Result.Failure.Code != 409 {
			t.Fatalf("output %+v, want the cancellation failure 409", o.Result)
		}
		s.ends()
	})
}

// The runtime closing its side ends the stream at once, after a suspension it cannot
// answer in place.
func TestBidiRuntimeCloseEndsTheStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := Handler("sleepy", NoInput(func(ctx Context) (string, error) {
			return "", Sleep(ctx, time.Minute)
		}))
		s := openStream(t, h, 1)
		s.send(&wire.Start{ID: []byte{1}, KnownEntries: 1}, input())
		s.expect(wire.TypeSleep, wire.TypeSuspension)
		_ = s.body.Close()
		s.ends()
	})
}

// h2 is a real endpoint served with Serve's server settings (HTTP/1.1 and h2c on one
// port) and a prior-knowledge h2c client.
type h2 struct {
	srv    *httptest.Server
	client *http.Client
}

func newH2(t *testing.T, h http.Handler) *h2 {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	srv.Config = newServer("", h)
	srv.Start()
	var p http.Protocols
	p.SetUnencryptedHTTP2(true)
	s := &h2{srv: srv, client: &http.Client{Transport: &http.Transport{Protocols: &p}}}
	t.Cleanup(func() {
		s.client.CloseIdleConnections()
		srv.Close()
	})
	return s
}

// open starts a stream; the request body is the returned pipe.
func (s *h2) open(t *testing.T, ctx context.Context, path string, header map[string]string) (*io.PipeWriter, *http.Response) {
	t.Helper()
	pr, pw := io.Pipe()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.srv.URL+path, pr)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range invokeHeaders(4) {
		req.Header.Set(k, v)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	res, err := s.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if res.ProtoMajor != 2 {
		t.Fatalf("the stream is %s, want HTTP/2", res.Proto)
	}
	return pw, res
}

func readFrame(t *testing.T, r io.Reader) wire.Frame {
	t.Helper()
	f, err := wire.NewFrameReader(r).Next()
	if err != nil {
		t.Fatalf("read a frame: %v", err)
	}
	return f
}

// The runtime going away in the middle of a turn (the stream reset, the connection gone)
// leaves no goroutine behind: the closure's context ends, the turn ends, the loop ends.
func TestBidiRuntimeDisconnectsMidTurn(t *testing.T) {
	for _, how := range []string{"reset", "connection"} {
		t.Run(how, func(t *testing.T) {
			before := runtime.NumGoroutine()
			started := make(chan struct{}, 1)
			var ended atomic.Bool
			svc := NewService("Svc", Handler("hang", NoInput(func(ctx Context) (string, error) {
				return Run(ctx, "hang", func(rc RunContext) (string, error) {
					started <- struct{}{}
					<-rc.Done()
					ended.Store(true)
					return "", rc.Err()
				})
			})))
			s := newH2(t, HTTPHandler(svc, Bidi()))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			pw, res := s.open(t, ctx, "/invoke/Svc/hang", nil)
			_, _ = pw.Write(wire.AppendMessage(wire.EncodeMessage(&wire.Start{ID: []byte{7}, KnownEntries: 1}, 0), input(), 0))
			<-started
			switch how {
			case "reset":
				// RST_STREAM: the request body fails, which the client transport only
				// notices when it reads it.
				cancel()
				_ = pw.CloseWithError(errors.New("the runtime went away"))
			case "connection":
				s.client.CloseIdleConnections()
				s.srv.CloseClientConnections()
			}
			_, _ = io.Copy(io.Discard, res.Body)
			res.Body.Close()
			_ = pw.Close()
			s.client.CloseIdleConnections()
			s.srv.Close()
			noLeak(t, before)
			if !ended.Load() {
				t.Fatal("the closure did not see its context end")
			}
		})
	}
}

// The endpoint declares BIDI_STREAM, answers h2c streams turn after turn, and still serves
// an HTTP/1.1 request to /invoke as one REQUEST_RESPONSE turn.
func TestBidiEndpoint(t *testing.T) {
	before := runtime.NumGoroutine()
	svc := NewService("Svc", Handler("count", NoInput(func(ctx Context) (string, error) {
		if err := Sleep(ctx, time.Second); err != nil {
			return "", err
		}
		return Run(ctx, "step", constant("ok"))
	})))
	func() {
		s := newH2(t, HTTPHandler(svc, Bidi(), withClock(func() int64 { return testClock })))
		h1 := &http.Client{Transport: &http.Transport{}}
		defer h1.CloseIdleConnections()
		res, err := h1.Get(s.srv.URL + "/discover")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.ProtoMajor != 1 || !strings.Contains(string(b), `"protocolMode":"BIDI_STREAM"`) {
			t.Fatalf("discover over %s: %s", res.Proto, b)
		}

		pw, res := s.open(t, context.Background(), "/invoke/Svc/count", nil)
		defer res.Body.Close()
		if ct := res.Header.Get("Content-Type"); ct != invocationV1 {
			t.Fatalf("content-type %s", ct)
		}
		_, _ = pw.Write(wire.AppendMessage(wire.EncodeMessage(&wire.Start{ID: []byte{7}, KnownEntries: 1}, 0), input(), 0))
		for _, want := range []wire.MessageType{wire.TypeSleep, wire.TypeSuspension} {
			if f := readFrame(t, res.Body); f.Type != want {
				t.Fatalf("got %s, want %s", f.Type, want)
			}
		}
		_, _ = pw.Write(wire.EncodeMessage(&wire.SleepCompletion{ID: 1}, 0))
		for _, want := range []wire.MessageType{wire.TypeRun, wire.TypeProposeRunCompletion, wire.TypeSuspension} {
			if f := readFrame(t, res.Body); f.Type != want {
				t.Fatalf("got %s, want %s", f.Type, want)
			}
		}
		_, _ = pw.Write(wire.EncodeMessage(&wire.ProposeRunCompletionAck{CompletionID: 2}, 0))
		for _, want := range []wire.MessageType{wire.TypeOutput, wire.TypeEnd} {
			if f := readFrame(t, res.Body); f.Type != want {
				t.Fatalf("got %s, want %s", f.Type, want)
			}
		}
		if rest, err := io.ReadAll(res.Body); err != nil || len(rest) != 0 {
			t.Fatalf("after End: %d bytes, %v", len(rest), err)
		}
		_ = pw.Close()

		// HTTP/1.1: one REQUEST_RESPONSE turn.
		body := wire.AppendMessage(wire.EncodeMessage(&wire.Start{ID: []byte{8}, KnownEntries: 1}, 0), input(), 0)
		req, _ := http.NewRequest(http.MethodPost, s.srv.URL+"/invoke/Svc/count", strings.NewReader(string(body)))
		for k, v := range invokeHeaders(4) {
			req.Header.Set(k, v)
		}
		res1, err := h1.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ = io.ReadAll(res1.Body)
		res1.Body.Close()
		frames, err := wire.DecodeFrames(b)
		if err != nil || res1.ProtoMajor != 1 || len(frames) != 2 || frames[0].Type != wire.TypeSleep || frames[1].Type != wire.TypeSuspension {
			t.Fatalf("HTTP/1.1 invoke: %s %v %v", res1.Proto, frames, err)
		}
		h1.CloseIdleConnections()
		s.client.CloseIdleConnections()
		s.srv.Close()
	}()
	noLeak(t, before)
}

// Request identity on a stream (docs/protocol-v1.md §4, "Request identity"): the headers
// before anything, one stream per JWT, then the jti against Start and the body hash
// against the turn-opening body; a mismatch there closes the stream without a frame and
// the handler does not run.
func TestBidiRequestIdentity(t *testing.T) {
	key := newSigner(t)
	var ran atomic.Int32
	svc := NewService("Echo", Handler("hello", NoInput(func(Context) (string, error) {
		ran.Add(1)
		return "hi", nil
	})))
	s := newH2(t, HTTPHandler(svc, Bidi(), IdentityKeys(key.kid)))
	host := strings.TrimPrefix(s.srv.URL, "http://")
	id := []byte{0xab, 0xcd}
	opening := startBody(id, 2)
	jti := identity.ExpectedJTI(id, 2)

	// play opens a stream with token, sends the opening body and reads to the end; it
	// returns the status, the frames and whether the stream was reset.
	play := func(token string, body []byte) (int, []wire.Frame, bool) {
		t.Helper()
		h := map[string]string{}
		if token != "" {
			h = key.headers(nil, token)
		}
		pw, res := s.open(t, context.Background(), "/invoke/Echo/hello", h)
		defer res.Body.Close()
		defer pw.Close()
		if res.StatusCode != http.StatusOK {
			return res.StatusCode, nil, false
		}
		_, _ = pw.Write(body)
		var frames []wire.Frame
		fr := wire.NewFrameReader(res.Body)
		for {
			f, err := fr.Next()
			if errors.Is(err, io.EOF) {
				return res.StatusCode, frames, false
			}
			if err != nil {
				return res.StatusCode, frames, true
			}
			frames = append(frames, f)
		}
	}

	if status, _, _ := play("", opening); status != http.StatusUnauthorized {
		t.Fatalf("unsigned: %d", status)
	}
	good := key.jwt("/invoke/Echo/hello", jti, opening, host)
	status, frames, reset := play(good, opening)
	if status != 200 || reset || len(frames) != 2 || frames[0].Type != wire.TypeOutput || frames[1].Type != wire.TypeEnd {
		t.Fatalf("signed: %d %v reset=%v", status, frames, reset)
	}
	if status, _, _ := play(good, opening); status != http.StatusUnauthorized {
		t.Fatalf("the same JWT opened a second stream: %d", status)
	}
	for name, c := range map[string]struct {
		token string
		body  []byte
	}{
		"another attempt": {key.jwt("/invoke/Echo/hello", identity.ExpectedJTI(id, 1), opening, host), opening},
		"another body":    {key.jwt("/invoke/Echo/hello", jti, startBody(id, 3), host), opening},
		"not a Start":     {key.jwt("/invoke/Echo/hello", jti, nil, host), wire.EncodeMessage(input(), 0)},
	} {
		status, frames, reset := play(c.token, c.body)
		if status != 200 || !reset || len(frames) != 0 {
			t.Fatalf("%s: %d %v reset=%v, want the stream closed without a frame", name, status, frames, reset)
		}
	}
	if ran.Load() != 1 {
		t.Fatalf("the handler ran %d times, want once", ran.Load())
	}
}

// Serve's own server speaks h2c on the port it listens on.
func TestServeSpeaksH2C(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e, err := newEndpoint([]ServeOption{echoService, Bidi()})
	if err != nil {
		t.Fatal(err)
	}
	srv := newServer(ln.Addr().String(), e)
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()
	var p http.Protocols
	p.SetUnencryptedHTTP2(true)
	client := &http.Client{Transport: &http.Transport{Protocols: &p}}
	defer client.CloseIdleConnections()
	body := wire.AppendMessage(wire.EncodeMessage(&wire.Start{ID: []byte{1}, KnownEntries: 1}, 0), input(), 0)
	req, _ := http.NewRequest(http.MethodPost, "http://"+ln.Addr().String()+"/invoke/Echo/hello", strings.NewReader(string(body)))
	for k, v := range invokeHeaders(4) {
		req.Header.Set(k, v)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	frames, err := wire.DecodeFrames(b)
	if err != nil || res.ProtoMajor != 2 || len(frames) != 2 || frames[0].Type != wire.TypeOutput {
		t.Fatalf("%s: %v %v", res.Proto, frames, err)
	}
}
