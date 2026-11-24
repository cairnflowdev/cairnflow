// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"encoding/json"
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

// openAPIPaths reads docs/api/openapi.json: path template => methods (upper case).
func openAPIPaths(t *testing.T) map[string]map[string]bool {
	t.Helper()
	data, err := os.ReadFile("../../../docs/api/openapi.json")
	if err != nil {
		t.Fatalf("read the OpenAPI document: %v", err)
	}
	var doc struct {
		Paths map[string]map[string]json.RawMessage `json:"paths"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse the OpenAPI document: %v", err)
	}
	out := map[string]map[string]bool{}
	for p, ops := range doc.Paths {
		out[p] = map[string]bool{}
		for m := range ops {
			out[p][strings.ToUpper(m)] = true
		}
	}
	return out
}

// matches: every {param} segment of the template matches one non-empty segment of the
// escaped path; the other segments match exactly.
func matches(template, path string) bool {
	ts := strings.Split(template, "/")
	ps := strings.Split(path, "/")
	if len(ts) != len(ps) {
		return false
	}
	for i := range ts {
		if strings.HasPrefix(ts[i], "{") && strings.HasSuffix(ts[i], "}") {
			if ps[i] == "" {
				return false
			}
			continue
		}
		if ts[i] != ps[i] {
			return false
		}
	}
	return true
}

// Every request the client can send goes to a path and a method of docs/api/openapi.json.
func TestEveryPathTheClientCallsIsInOpenAPI(t *testing.T) {
	spec := openAPIPaths(t)
	f := newFake(t, func(w http.ResponseWriter, r *http.Request, req seenRequest) {
		if strings.Contains(req.path(), "/streams/") {
			sse(w, other("end", map[string]any{"next": 0}))
			return
		}
		reply(w, 200, map[string]any{"id": testID, "status": "completed", "output": "1", "nextAt": nil},
			"X-Cairn-Invocation-Id", testID)
	})
	c := newClient(t, f.URL)
	scoped := InScope("acme")
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, opts := range [][]TargetOption{nil, {scoped}} {
		_, err := Call[int](bg, c.Service("S", opts...), "h", 1)
		check(err)
		_, err = Call[int](bg, c.Object("O", "k", opts...), "h", 1)
		check(err)
		_, err = Send(bg, c.Service("S", opts...), "h", 1)
		check(err)
		wf := c.Workflow("W", "k", opts...)
		_, err = RunWorkflow[int](bg, wf, 1)
		check(err)
		_, err = Attach[int](bg, wf, Timeout(time.Second))
		check(err)
		_, _, err = Output[int](bg, wf)
		check(err)
		p := c.Promise("W", "k", "p", opts...)
		_, err = p.Resolve(bg, 1)
		check(err)
		_, err = p.Reject(bg, "no", 400)
		check(err)
		_, err = p.Peek(bg)
		check(err)
		s := c.Schedules()
		_, err = s.Put(bg, "n", ScheduleDefinition{Every: time.Minute, Target: ScheduleTarget{Service: "S", Handler: "h"}}, opts...)
		check(err)
		_, err = s.Get(bg, "n", opts...)
		check(err)
		_, err = s.Delete(bg, "n", opts...)
		check(err)
		_, err = s.Pause(bg, "n", opts...)
		check(err)
		_, err = s.Resume(bg, "n", opts...)
		check(err)
		trigger := []TriggerOption{}
		if opts != nil {
			trigger = append(trigger, scoped)
		}
		_, err = s.Trigger(bg, "n", trigger...)
		check(err)
		_, err = s.List(bg, opts...)
		check(err)
	}
	h := c.Invocation(testID)
	_, err := Attach[int](bg, h)
	check(err)
	_, _, err = Output[int](bg, h)
	check(err)
	_, err = h.Status(bg)
	check(err)
	_, err = h.Signal(bg, "s", 1)
	check(err)
	_, err = h.SignalIndex(bg, 1, nil)
	check(err)
	_, err = h.Cancel(bg)
	check(err)
	_, err = h.CancelHard(bg)
	check(err)
	_, err = c.Awakeable("a").Resolve(bg, 1)
	check(err)
	_, err = c.Awakeable("a").Reject(bg, "no", 0)
	check(err)
	_, err = collect(t, c.ReadStream(bg, testID, "ui"), 10)
	check(err)

	used := map[string]bool{}
	for _, r := range f.requests() {
		path := r.rawPath()
		found := false
		for template, methods := range spec {
			if matches(template, path) && methods[r.Method] {
				used[r.Method+" "+template] = true
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s %s is not in docs/api/openapi.json", r.Method, path)
		}
	}
	var list []string
	for k := range used {
		list = append(list, k)
	}
	sort.Strings(list)
	t.Logf("operations used (%d):\n%s", len(list), strings.Join(list, "\n"))
	if len(list) < 30 {
		t.Fatalf("only %d operations exercised", len(list))
	}
}
