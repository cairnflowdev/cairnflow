// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

// The SDK process dies at every turn of a multi-step invocation, with SIGKILL, and comes
// back: the invocation ends with the same result and the same journal as a run nobody
// killed. One more death between the Output frame and the End frame of the last turn:
// in BIDI_STREAM the runtime already has the Output, so the restarted SDK must send End
// again and not a second Output.

import (
	"bufio"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/client"
)

// buildSDKProc builds the sdkproc command (internal/sdkproc) into a temporary directory.
func buildSDKProc(t *testing.T) string {
	t.Helper()
	// `go test` puts the go tool of its own GOROOT first on PATH.
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Fatal("no go tool to build the SDK process")
	}
	_, file, _, _ := runtime.Caller(0)
	out := filepath.Join(t.TempDir(), "sdkproc")
	cmd := exec.Command(goBin, "build", "-o", out, "./internal/sdkproc")
	cmd.Dir = filepath.Dir(file)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building sdkproc: %v\n%s", err, b)
	}
	return out
}

// freeAddr returns a local address nothing listens on (the port the SDK process keeps
// across its restarts, so the registered deployment stays valid).
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// sdkProc is one run of the SDK process.
type sdkProc struct {
	cmd  *exec.Cmd
	done chan struct{}
	out  *logTail
}

func startSDK(t *testing.T, bin, addr, mode, eventLog string, more ...string) *sdkProc {
	t.Helper()
	args := append([]string{"-addr", addr, "-mode", mode, "-log", eventLog}, more...)
	cmd := exec.Command(bin, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &sdkProc{cmd: cmd, done: make(chan struct{}), out: &logTail{}}
	ready := make(chan struct{})
	go func() {
		sc := bufio.NewScanner(stdout)
		once := false
		for sc.Scan() {
			p.out.add(sc.Text())
			if sc.Text() == "ready" && !once {
				close(ready)
				once = true
			}
		}
		_ = cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(p.stop)
	select {
	case <-ready:
	case <-p.done:
		t.Fatalf("the SDK process exited before it was ready:\n%s", p.out)
	case <-time.After(20 * time.Second):
		t.Fatalf("the SDK process was not ready within 20 s:\n%s", p.out)
	}
	return p
}

// exited waits (bounded) for the process to end by itself.
func (p *sdkProc) exited(d time.Duration) bool {
	select {
	case <-p.done:
		return true
	case <-time.After(d):
		return false
	}
}

func (p *sdkProc) stop() {
	_ = p.cmd.Process.Kill()
	<-p.done
}

// events counts the lines of the event log that start with prefix.
func events(t *testing.T, path, prefix string) int {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, prefix) {
			n++
		}
	}
	return n
}

// pipelineResult is the output of GoWork/pipeline (internal/sdkproc).
type pipelineResult struct {
	A       int `json:"a"`
	B       int `json:"b"`
	Doubled int `json:"doubled"`
	Seen    int `json:"seen"`
}

func TestDurableAcrossSDKDeath(t *testing.T) {
	bin := buildSDKProc(t)
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			flag := map[string]string{rr: "rr", bidi: "bidi"}[mode]
			n := startNode(t)
			addr := freeAddr(t)
			dir := t.TempDir()
			start := func(key string) string {
				t.Helper()
				inv, err := client.Send(bounded(t), n.api.Object("GoWork", key), "pipeline", map[string]int{"n": 21})
				if err != nil {
					t.Fatal(err)
				}
				return inv.ID()
			}

			// The reference: nobody dies.
			// The verified configuration: a restarted process has an empty replay cache,
			// so its first turn after the restart is a replay miss (409) the runtime
			// answers with the whole journal.
			verified := []string{"-kid", n.kid(t), "-segment-origin", n.url}
			refLog := filepath.Join(dir, "ref")
			p := startSDK(t, bin, addr, flag, refLog, verified...)
			if reg := n.register(t, "http://"+addr); reg.ProtocolMode != mode {
				t.Fatalf("the runtime settled on %s, want %s", reg.ProtocolMode, mode)
			}
			id := start("ref")
			var want pipelineResult
			output(t, n.settle(t, id), &want)
			if want != (pipelineResult{A: 42, B: 43, Doubled: 86, Seen: 1}) {
				t.Fatalf("reference result %+v", want)
			}
			refShape := names(n.journal(t, id))
			total := events(t, refLog, "turn")
			p.stop()
			if total < 3 {
				t.Fatalf("the program is too short to mean anything: %d turns", total)
			}
			t.Logf("%s: %d turns, journal %v", mode, total, refShape)

			deaths := make([][]string, 0, total+1)
			for k := 1; k <= total; k++ {
				deaths = append(deaths, []string{"-kill-at-turn", strconv.Itoa(k)})
			}
			deaths = append(deaths, []string{"-kill-before-end"})
			for i, death := range deaths {
				what := strings.Join(death, " ")
				log := filepath.Join(dir, "run-"+strconv.Itoa(i))
				p := startSDK(t, bin, addr, flag, log, append(death, verified...)...)
				id := start("death-" + strconv.Itoa(i))
				if !p.exited(30 * time.Second) {
					t.Fatalf("%s: the SDK process did not die (turns so far: %d)", what, events(t, log, "turn"))
				}
				// Died between Output and End: once the runtime has journaled the Output (on a
				// stream it reads frame by frame; a REQUEST_RESPONSE body cut short is dropped
				// whole), the restarted SDK sends End again but not a second Output.
				outputDurable := false
				if death[0] == "-kill-before-end" {
					wait := time.Duration(0) // REQUEST_RESPONSE: one look
					if mode == bidi {
						wait = 5 * time.Second
					}
					outputDurable = within(wait, func() bool {
						j := names(n.journal(t, id))
						return len(j) > 0 && j[len(j)-1] == "Output"
					})
					if mode == bidi && !outputDurable {
						t.Fatalf("%s: the runtime never journaled the Output sent before the death", what)
					}
				}
				outputsBefore, endsBefore := events(t, log, "frame Output"), events(t, log, "frame End")
				p = startSDK(t, bin, addr, flag, log, verified...)
				var got pipelineResult
				output(t, n.settle(t, id), &got)
				p.stop()
				if outputDurable {
					outputs, ends := events(t, log, "frame Output")-outputsBefore, events(t, log, "frame End")-endsBefore
					if outputs != 0 || ends != 1 {
						t.Fatalf("%s: after the restart the SDK sent %d Output and %d End frames, want 0 and 1", what, outputs, ends)
					}
				}
				if got != want {
					t.Fatalf("%s: result %+v after the restart, want %+v", what, got, want)
				}
				if shape := names(n.journal(t, id)); !slices.Equal(shape, refShape) {
					t.Fatalf("%s: journal %v after the restart, want %v (the journal must not grow)", what, shape, refShape)
				}
				// A Run reruns at most once for each death that came before its result was durable.
				for _, step := range []string{"step-1", "step-2"} {
					if c := events(t, log, "effect "+step); c < 1 || c > 2 {
						t.Fatalf("%s: %s ran %d times", what, step, c)
					}
				}
			}
		})
	}
}
