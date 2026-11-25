// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

// Command sdkproc is a Go SDK endpoint in its own process, for the durability test of the
// end-to-end suite: it can kill itself with SIGKILL at a chosen turn, or between the
// Output frame and the End frame of the last turn, so the test can restart it and check
// that the invocation ends the same way and its journal does not grow.
//
//	sdkproc -addr 127.0.0.1:9080 -mode bidi -kid publickeyv1_... -kill-at-turn 3 -log /tmp/events
//
// Every handler execution (one per turn: a turn runs the handler from the start), every
// Run closure execution and every Output or End frame of the pipeline appends a line to -log
// ("turn", "effect <name>", "frame Output", "frame End"); the process prints "ready" once
// it listens.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	cairn "github.com/cairnflowdev/cairnflow/sdk/go"
	"github.com/cairnflowdev/cairnflow/sdk/go/internal/wire"
)

var (
	killAtTurn = flag.Int("kill-at-turn", 0, "SIGKILL this process when the handler starts its n-th execution (0: never)")
	killAtEnd  = flag.Bool("kill-before-end", false, "SIGKILL this process after writing the pipeline's Output frame, before its End frame")
	eventLog   = flag.String("log", "", "file the turns and Run executions are appended to")
	kid        = flag.String("kid", "", "the runtime's request-identity key (cairn.IdentityKeys); empty: unverified")
	origin     = flag.String("segment-origin", "", "the runtime's value read origin (cairn.SegmentOrigins)")
)

var (
	turns  atomic.Int64
	logMu  sync.Mutex
	logOut *os.File
)

func event(line string) {
	logMu.Lock()
	defer logMu.Unlock()
	if logOut == nil {
		return
	}
	if _, err := fmt.Fprintln(logOut, line); err != nil {
		log.Fatal(err)
	}
	if err := logOut.Sync(); err != nil {
		log.Fatal(err)
	}
}

func die() {
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {} // SIGKILL cannot be caught; never go on
}

type pipelineInput struct {
	N int `json:"n"`
}

type pipelineResult struct {
	A       int `json:"a"`
	B       int `json:"b"`
	Doubled int `json:"doubled"`
	Seen    int `json:"seen"`
}

// pipeline is long enough to have many points to die at: a state read, two Runs around a
// durable sleep, a call, a state write.
func pipeline(ctx cairn.ObjectContext, in pipelineInput) (pipelineResult, error) {
	event("turn")
	if n := turns.Add(1); n == int64(*killAtTurn) {
		die()
	}
	seen, _, err := cairn.Get[int](ctx, "seen")
	if err != nil {
		return pipelineResult{}, err
	}
	step := func(name string, v int) (int, error) {
		return cairn.Run(ctx, name, func(cairn.RunContext) (int, error) {
			event("effect " + name)
			return v, nil
		})
	}
	a, err := step("step-1", in.N*2)
	if err != nil {
		return pipelineResult{}, err
	}
	if err := cairn.Sleep(ctx, 50*time.Millisecond, cairn.WithName("rest")); err != nil {
		return pipelineResult{}, err
	}
	b, err := step("step-2", a+1)
	if err != nil {
		return pipelineResult{}, err
	}
	d, err := cairn.Service[int](ctx, "GoWorkHelper", "double").Request(b)
	if err != nil {
		return pipelineResult{}, err
	}
	if err := cairn.Set(ctx, "seen", seen+1); err != nil {
		return pipelineResult{}, err
	}
	return pipelineResult{A: a, B: b, Doubled: d, Seen: seen + 1}, nil
}

func double(_ cairn.Context, n int) (int, error) { return 2 * n, nil }

// fast retries: the runtime calls again soon after the process comes back.
var retry = cairn.WithRetryPolicy(cairn.RetryPolicy{InitialInterval: 50 * time.Millisecond, MaxInterval: 300 * time.Millisecond, ExponentiationFactor: 2})

var (
	work   = cairn.NewObject("GoWork", cairn.Exclusive("pipeline", pipeline, retry))
	helper = cairn.NewService("GoWorkHelper", cairn.Handler("double", double, retry))
)

// frameWatch passes a response of GoWork/pipeline through frame by frame, logging each
// Output and End frame ("frame Output", "frame End"); with kill set it kills the process
// when the frame after an Output frame is End: the runtime has the Output but never sees
// the End.
type frameWatch struct {
	http.ResponseWriter
	kill      bool
	buf       []byte
	sawOutput bool
}

func (k *frameWatch) Unwrap() http.ResponseWriter { return k.ResponseWriter }

func (k *frameWatch) Flush() {
	if f, ok := k.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (k *frameWatch) Write(p []byte) (int, error) {
	k.buf = append(k.buf, p...)
	for len(k.buf) >= wire.HeaderLen {
		h, err := wire.ParseHeader(k.buf)
		if err != nil {
			return 0, err
		}
		size := wire.HeaderLen + int(h.Length)
		if len(k.buf) < size {
			break
		}
		switch h.Type {
		case wire.TypeEnd:
			if k.kill && k.sawOutput {
				k.Flush()
				die()
			}
			event("frame End")
		case wire.TypeOutput:
			k.sawOutput = true
			event("frame Output")
		}
		if _, err := k.ResponseWriter.Write(k.buf[:size]); err != nil {
			return 0, err
		}
		k.buf = k.buf[size:]
	}
	return len(p), nil
}

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "address to listen on")
	mode := flag.String("mode", "rr", "rr (REQUEST_RESPONSE) or bidi (BIDI_STREAM)")
	flag.Parse()
	if *eventLog != "" {
		f, err := os.OpenFile(*eventLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Fatal(err)
		}
		logOut = f
	}
	opts := []cairn.ServeOption{work, helper}
	switch *mode {
	case "rr":
	case "bidi":
		opts = append(opts, cairn.Bidi())
	default:
		log.Fatalf("unknown mode %q", *mode)
	}
	if *kid != "" {
		opts = append(opts, cairn.IdentityKeys(*kid))
	}
	if *origin != "" {
		opts = append(opts, cairn.SegmentOrigins(*origin))
	}
	// Frames must stay readable on the way out.
	opts = append(opts, cairn.WithoutCompression())
	inner := cairn.HTTPHandler(opts...)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/invoke/GoWork/pipeline") {
			inner.ServeHTTP(w, r) // discovery (JSON, not frames) and the helper
			return
		}
		inner.ServeHTTP(&frameWatch{ResponseWriter: w, kill: *killAtEnd}, r)
	})
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	srv := &http.Server{Handler: h, Protocols: &p, ReadHeaderTimeout: 10 * time.Second}
	fmt.Println("ready")
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
