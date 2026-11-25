// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import "testing"

func TestCommandDiff(t *testing.T) {
	call := func(handler string, id uint32, key *string) []byte {
		return Marshal(&Call{ServiceName: "Callee", HandlerName: handler, Parameter: []byte("1"), IdempotencyKey: key,
			InvocationIDNotificationIdx: id, ResultCompletionID: id + 1, Name: "Callee/" + handler})
	}
	empty := ""
	for _, c := range []struct {
		name    string
		t       MessageType
		a, b    []byte
		field   int
		differs bool
	}{
		{"same call", TypeCall, call("a", 1, nil), call("a", 1, nil), 0, false},
		{"retargeted", TypeCall, call("a", 1, nil), call("b", 1, nil), 2, true},
		{"other ids", TypeCall, call("a", 1, nil), call("a", 3, nil), 10, true},
		{"absent vs empty key", TypeCall, call("a", 1, nil), call("a", 1, &empty), 6, true},
		{"wake-up time", TypeSleep, Marshal(&Sleep{WakeUpTime: 1, ResultCompletionID: 1, Name: "sleep"}),
			Marshal(&Sleep{WakeUpTime: 2, ResultCompletionID: 1, Name: "sleep"}), 0, false},
		{"sleep id", TypeSleep, Marshal(&Sleep{WakeUpTime: 1, ResultCompletionID: 1, Name: "sleep"}),
			Marshal(&Sleep{WakeUpTime: 1, ResultCompletionID: 2, Name: "sleep"}), 11, true},
		{"set value", TypeSetState, Marshal(&SetState{Key: []byte("k"), Value: InlineValue([]byte("1")), Name: "k"}),
			Marshal(&SetState{Key: []byte("k"), Value: InlineValue([]byte("2")), Name: "k"}), 0, false},
		{"set key", TypeSetState, Marshal(&SetState{Key: []byte("k"), Name: "k"}), Marshal(&SetState{Key: []byte("j"), Name: "k"}), 1, true},
		{"send time", TypeOneWayCall, Marshal(&OneWayCall{ServiceName: "S", InvokeTime: 5}), Marshal(&OneWayCall{ServiceName: "S", InvokeTime: 9}), 0, false},
		{"eager value", TypeGetEagerState, Marshal(&GetEagerState{Key: []byte("k"), Result: Result{Void: true}}),
			Marshal(&GetEagerState{Key: []byte("k"), Result: Result{Value: InlineValue([]byte("3"))}}), 0, false},
		{"eager keys", TypeGetEagerStateKeys, Marshal(&GetEagerStateKeys{Name: "getKeys", Value: &StateKeys{Keys: [][]byte{[]byte("a")}}}),
			Marshal(&GetEagerStateKeys{Name: "getKeys", Value: &StateKeys{}}), 0, false},
		{"broken journal", TypeRun, []byte{0x0a}, Marshal(&Run{ResultCompletionID: 1}), 0, true},
	} {
		field, differs := CommandDiff(c.t, c.a, c.b)
		if field != c.field || differs != c.differs {
			t.Errorf("%s: field %d differs %v, want %d %v", c.name, field, differs, c.field, c.differs)
		}
	}
}

func TestUpperNameAndEntryName(t *testing.T) {
	for typ, want := range map[MessageType]string{
		TypeSetState: "SET_STATE", TypeOneWayCall: "ONE_WAY_CALL", TypeOutput: "OUTPUT",
		TypeGetEagerStateKeys: "GET_EAGER_STATE_KEYS", MessageType(0x0414): "0x0414",
	} {
		if got := typ.UpperName(); got != want {
			t.Errorf("%v: %s, want %s", typ, got, want)
		}
	}
	if got := EntryNameOf(Marshal(&Run{ResultCompletionID: 3, Name: "charge"})); got != "charge" {
		t.Fatalf("entry name %q", got)
	}
	if got := EntryNameOf([]byte{0xff}); got != "" {
		t.Fatalf("entry name of garbage %q", got)
	}
}
