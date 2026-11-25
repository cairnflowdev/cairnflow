// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"runtime"
	"testing"
)

func ptr[T any](v T) *T { return &v }

func sha32() []byte {
	b := make([]byte, 32)
	for i := range b {
		b[i] = byte(i + 1)
	}
	return b
}

// Messages no conformance trace carries, encoded by the TypeScript SDK's encoders
// (sdk/typescript/src/wire.ts) with the same inputs. Same bytes ⇒ same canonical form.
func TestEncodingMatchesTheTypeScriptSDK(t *testing.T) {
	cases := []struct {
		name string
		m    Message
		hex  string
	}{
		{"ClearState", &ClearState{Key: []byte("k"), Name: "n"}, "0a016b62016e"},
		{"ClearAllState", &ClearAllState{Name: "n"}, "62016e"},
		{"GetEagerStateKeys", &GetEagerStateKeys{Name: "keys", Value: &StateKeys{Keys: [][]byte{[]byte("a"), {}, []byte("bc")}}},
			"62046b65797372090a01610a000a026263"},
		{"GetEagerState void", &GetEagerState{Key: []byte("missing"), Name: "g", Result: Result{Void: true}},
			"0a076d697373696e676201676a00"},
		{"PeekPromise", &PeekPromise{Key: "p", ResultCompletionID: 3, Name: "peek"}, "0a0170580362047065656b"},
		{"CompletePromise failure", &CompletePromise{Key: "p", CompletionFailure: &Failure{Code: 409, Message: "taken"}, ResultCompletionID: 4},
			"0a01701a0a089903120574616b656e5804"},
		{"AttachInvocation", &AttachInvocation{InvocationID: "inv_1", ResultCompletionID: 5, Name: "att"}, "0a05696e765f3158056203617474"},
		{"GetInvocationOutput", &GetInvocationOutput{InvocationID: "inv_1", ResultCompletionID: 6}, "0a05696e765f315806"},
		{"OneWayCall", &OneWayCall{
			ServiceName: "S", HandlerName: "h", Parameter: []byte("{}"), InvokeTime: 1800000000000, Key: "k",
			Scope: ptr("tenant"), LimitKey: ptr("lk"), InvocationIDNotificationIdx: 7, Name: "send",
			Placement: PlacementSpread, Tags: []Header{{"a", "1"}, {"b", ""}},
		}, "0a01531201681a027b7d2080a0f1c2b13432016b420674656e616e744a026c6b5007620473656e64680172060a016112013172030a0162"},
		{"SendSignal numbered 0", &SendSignal{TargetInvocationID: "inv_2", Signal: SignalIdx(0), Result: Result{Void: true}},
			"0a05696e765f3210002200"},
		{"SendSignal named", &SendSignal{TargetInvocationID: "inv_2", Signal: SignalName("go"), Result: Result{Value: InlineValue([]byte("1"))}, Name: "sig"},
			"0a05696e765f321a02676f2a030a01316203736967"},
		{"ProposeRunCompletion segmented", NewProposeSegmented(9, &Value{
			Content: []byte("ef"), Segments: []SegmentRef{{Sha256: sha32(), Len: 4, Gen: 0x0102030405060708}}, Len: 6,
		}), "08098201350a026566122d0a200102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f2010041908070605040302011806"},
		{"ProposeRunCompletion empty value", NewProposeValue(9, nil), "08097200"},
		{"ReplayReport", &ReplayReport{Matched: 2, Mismatch: true, Expected: "Sleep", Got: "Run", Message: "m"},
			"080210011a05536c656570220352756e2a016d"},
		{"ArtifactPut", &ArtifactPut{Sha256: sha32(), Content: []byte("data"), ContentType: "text/plain"},
			"0a200102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f201204646174611a0a746578742f706c61696e"},
		{"StreamAppend", &StreamAppend{Name: "s", Durable: true, Records: []StreamRecord{
			{Step: 0, Attempt: 1, K: 2, Payload: []byte("x")}, {Step: 3},
		}, Close: true}, "0a017310011a07100118022201781a0208032001"},
		{"ContinueAsNew", &ContinueAsNew{Input: []byte("{}")}, "0a027b7d"},
		{"ErrorMessage", &ErrorMessage{Code: 500, Message: "boom", Behavior: BehaviorPause}, "08f4031204626f6f6d4801"},
		{"SegmentUrls", &SegmentUrls{URLs: []SegmentURL{{Sha256: sha32(), Gen: 5, URL: "https://s.example/a"}}},
			"0a400a200102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f201105000000000000001a1368747470733a2f2f732e6578616d706c652f61"},
		{"Suspension with an ALL child", &Suspension{
			WaitingCompletions: []uint32{1}, WaitingSignals: []uint32{17}, WaitingNamedSignals: []string{"x"},
			NestedFutures: []Suspension{{WaitingCompletions: []uint32{2, 300}, CombinatorType: CombinatorAllCompleted}},
		}, "0a01011201111a017822070a0302ac022802"},
	}
	for _, c := range cases {
		want, _ := hex.DecodeString(c.hex)
		if got := Marshal(c.m); !bytes.Equal(got, want) {
			t.Errorf("%s: %x\n want %x", c.name, got, want)
			continue
		}
		back, err := Decode(c.m.Type(), want)
		if err != nil {
			t.Errorf("%s: decode: %v", c.name, err)
			continue
		}
		if !bytes.Equal(Marshal(back), want) {
			t.Errorf("%s: decode → encode differs", c.name)
		}
	}
}

// Every message type with every field set: encode → decode gives the same struct back, and the
// encoding is canonical (ascending tags).
func TestEveryMessageRoundTrips(t *testing.T) {
	val := &Value{Content: []byte("tail"), Segments: []SegmentRef{{Sha256: sha32(), Len: 10, Gen: 1 << 63}}, Len: 14}
	fail := &Failure{Code: 409, Message: "no", Metadata: []FailureMetadata{{"k", "v"}, {"", ""}}}
	msgs := []Message{
		&Start{
			ID: []byte{1, 2}, DebugID: "inv_x", KnownEntries: 4,
			StateMap:     []StateEntry{{Key: []byte("a"), Value: []byte("1")}, {Key: []byte("empty")}},
			PartialState: true, Key: "obj", RetryCountSinceLastStoredEntry: 2, DurationSinceLastStoredEntry: 1500,
			RandomSeed: 1<<64 - 1, IdempotencyKey: ptr(""), KeepaliveIntervalMs: ptr(uint64(0)), ReplayOnly: true,
			ReplayFrom: 3, PrefixDigest: sha32(), SegmentURLs: []SegmentURL{{Sha256: sha32(), Gen: 9, URL: "u"}},
		},
		&Suspension{WaitingCompletions: []uint32{1, 2}, WaitingSignals: []uint32{1}, WaitingNamedSignals: []string{"", "n"},
			NestedFutures:  []Suspension{{WaitingCompletions: []uint32{5}, CombinatorType: CombinatorAllCompleted}, {}},
			CombinatorType: CombinatorFirstCompleted},
		&ErrorMessage{Code: 570, Message: "m", Stacktrace: "s", RelatedCommandIndex: ptr(uint32(0)),
			RelatedCommandName: ptr(""), RelatedCommandType: ptr(uint32(0x0411)), NextRetryDelay: ptr(uint64(0)), Behavior: BehaviorFail},
		&End{}, &Keepalive{},
		&CommandAck{CommandIndex: 7},
		NewProposeValue(3, []byte("v")), NewProposeFailure(3, fail), NewProposeSegmented(3, val),
		&ProposeRunCompletionAck{CompletionID: 3},
		&ReplayReport{Matched: 1, Mismatch: true, Expected: "e", Got: "g", Message: "m"},
		&ContinueAsNew{Input: []byte("in")},
		&StreamAppend{Name: "s", Durable: true, Records: []StreamRecord{{1, 2, 3, []byte("p")}}, Close: true},
		&ArtifactPut{Sha256: sha32(), Content: []byte("c"), ContentType: "t"},
		&SegmentUrls{URLs: []SegmentURL{{Sha256: sha32(), Gen: 1, URL: "a"}, {Sha256: sha32()}}},
		&Input{Headers: []Header{{"h", "v"}, {}}, Name: "n", Value: &Value{}},
		&Output{Name: "n", Result: Result{Value: val}},
		&Output{Result: Result{Failure: fail}},
		&GetState{Key: []byte("k"), ResultCompletionID: 1, Name: "n"},
		&SetState{Key: []byte("k"), Value: val, Name: "n"},
		&ClearState{Key: []byte("k"), Name: "n"},
		&ClearAllState{Name: "n"},
		&GetStateKeys{ResultCompletionID: 2, Name: "n"},
		&GetEagerState{Key: []byte("k"), Name: "n", Result: Result{Value: &Value{}}},
		&GetEagerStateKeys{Name: "n", Value: &StateKeys{Keys: [][]byte{{}, []byte("a")}}},
		&GetPromise{Key: "p", ResultCompletionID: 1, Name: "n"},
		&PeekPromise{Key: "p", ResultCompletionID: 1, Name: "n"},
		&CompletePromise{Key: "p", CompletionValue: &Value{Content: []byte("v")}, ResultCompletionID: 1, Name: "n"},
		&Sleep{WakeUpTime: 1800000001000, ResultCompletionID: 2, Name: "n"},
		&Call{ServiceName: "S", HandlerName: "h", Parameter: []byte("p"), Headers: []Header{{"a", "b"}}, Key: "k",
			IdempotencyKey: ptr("ik"), Scope: ptr(""), LimitKey: ptr("l"), InvocationIDNotificationIdx: 1,
			ResultCompletionID: 2, Name: "n", Placement: PlacementSpread, Tags: []Header{{"t", "1"}}},
		&OneWayCall{ServiceName: "S", HandlerName: "h", InvokeTime: 1, Headers: []Header{{}}, Key: "k",
			IdempotencyKey: ptr(""), InvocationIDNotificationIdx: 1, Name: "n", Tags: []Header{{"t", ""}}},
		&SendSignal{TargetInvocationID: "i", Signal: SignalIdx(1), Result: Result{Failure: fail}, Name: "n"},
		&SendSignal{TargetInvocationID: "i", Signal: SignalName(""), Result: Result{Void: true}},
		&Run{ResultCompletionID: 1, Name: "n"},
		&AttachInvocation{InvocationID: "i", ResultCompletionID: 1, Name: "n"},
		&GetInvocationOutput{InvocationID: "i", ResultCompletionID: 1, Name: "n"},
		&GetStateCompletion{ID: 1, Result: Result{Void: true}},
		&GetStateCompletion{ID: 1, Result: Result{Value: val}},
		&GetStateKeysCompletion{ID: 1, StateKeys: &StateKeys{Keys: [][]byte{[]byte("a")}}},
		&GetStateKeysCompletion{ID: 1, StateKeys: &StateKeys{}},
		&GetPromiseCompletion{ID: 1, Result: Result{Failure: fail}},
		&PeekPromiseCompletion{ID: 1, Result: Result{Void: true}},
		&CompletePromiseCompletion{ID: 1, Result: Result{Failure: &Failure{}}},
		&SleepCompletion{ID: 1, Void: true},
		&CallCompletion{ID: 1, Result: Result{Value: &Value{}}},
		&CallInvocationIDCompletion{ID: 1, InvocationID: "inv"},
		&RunCompletion{ID: 1, Result: Result{Value: val}},
		&AttachInvocationCompletion{ID: 1, Result: Result{Failure: fail}},
		&GetInvocationOutputCompletion{ID: 1, Result: Result{Void: true}},
		&SignalNotification{Signal: SignalIdx(SignalCancel), Result: Result{Void: true}},
		&SignalNotification{Signal: SignalName("approve"), Result: Result{Value: InlineValue([]byte("ok"))}},
		&Custom{Code: 0xFC07, Payload: []byte{1, 2, 3}},
	}
	covered := map[MessageType]bool{}
	for _, m := range msgs {
		b := Marshal(m)
		assertCanonicalOrder(t, m.Type(), b)
		back, err := Decode(m.Type(), b)
		if err != nil {
			t.Errorf("%s: %v", m.Type(), err)
			continue
		}
		if !reflect.DeepEqual(back, m) {
			t.Errorf("%s: round trip\n got %#v\nwant %#v", m.Type(), back, m)
		}
		covered[m.Type()] = true
	}
	for code := range typeNames {
		if !covered[code] {
			t.Errorf("no round-trip case for %s", code)
		}
	}
}

// assertCanonicalOrder checks that the top-level fields of b come in ascending tag order
// (a repeated field repeats its tag).
func assertCanonicalOrder(t *testing.T, mt MessageType, b []byte) {
	t.Helper()
	if mt.IsCustom() {
		return
	}
	last := 0
	err := walk(b, 0, func(r *reader, field, wt int) (bool, error) {
		if field < last {
			t.Errorf("%s: field %d after field %d", mt, field, last)
		}
		last = field
		return false, nil
	})
	if err != nil {
		t.Errorf("%s: %v", mt, err)
	}
}

func TestDecodeAcceptsNonCanonicalInput(t *testing.T) {
	// Suspension with unpacked repeated numbers, fields in reverse order and an unknown
	// field of every wire type (varint, fixed64, bytes, group, fixed32).
	b := []byte{
		0x28, 0x02, // combinator_type = ALL
		0x10, 0x05, // waiting_signals = 5, unpacked
		0x08, 0x03, // waiting_completions = 3, unpacked
		0x0a, 0x02, 0x04, 0x05, // waiting_completions packed [4, 5]
		0x08, 0x06, // waiting_completions = 6, unpacked again
		0xf8, 0x07, 0x01, // field 127, varint
		0xf9, 0x07, 1, 2, 3, 4, 5, 6, 7, 8, // field 127, fixed64
		0xfa, 0x07, 0x01, 0xff, // field 127, bytes
		0xfb, 0x07, 0x08, 0x01, 0xfc, 0x07, // field 127, group containing field 1
		0xfd, 0x07, 1, 2, 3, 4, // field 127, fixed32
	}
	var s Suspension
	if err := s.Unmarshal(b); err != nil {
		t.Fatal(err)
	}
	want := Suspension{WaitingCompletions: []uint32{3, 4, 5, 6}, WaitingSignals: []uint32{5}, CombinatorType: CombinatorAllCompleted}
	if !reflect.DeepEqual(s, want) {
		t.Fatalf("got %+v", s)
	}
	canonical := []byte{0x0a, 0x04, 3, 4, 5, 6, 0x12, 0x01, 5, 0x28, 0x02}
	if got := Marshal(&s); !bytes.Equal(got, canonical) {
		t.Fatalf("re-encoded %x, want %x", got, canonical)
	}

	// Start fields in reverse order, a non-minimal varint, and the scalar given twice (the last
	// one wins).
	b = []byte{
		0x48, 0x80, 0x00, // random_seed = 0, spelled with two bytes
		0x18, 0x01, // known_entries = 1
		0x18, 0x02, // known_entries = 2
		0x0a, 0x01, 0xaa, // id
	}
	var st Start
	if err := st.Unmarshal(b); err != nil {
		t.Fatal(err)
	}
	if st.KnownEntries != 2 || !bytes.Equal(st.ID, []byte{0xaa}) || st.RandomSeed != 0 {
		t.Fatalf("got %+v", st)
	}
	if got, want := Marshal(&st), []byte{0x0a, 0x01, 0xaa, 0x18, 0x02}; !bytes.Equal(got, want) {
		t.Fatalf("re-encoded %x, want %x", got, want)
	}
}

func TestDecodeSkipsFieldsOutsideTheMessage(t *testing.T) {
	// GetStateCompletion has no failure (field 6 is reserved there): it is skipped like an
	// unknown field, and the value (5) is kept.
	b := []byte{0x08, 0x01, 0x2a, 0x00, 0x32, 0x02, 0x08, 0x01}
	var m GetStateCompletion
	if err := m.Unmarshal(b); err != nil {
		t.Fatal(err)
	}
	if m.ID != 1 || m.Value == nil || m.Failure != nil {
		t.Fatalf("got %+v", m)
	}
	if got := Marshal(&m); !bytes.Equal(got, []byte{0x08, 0x01, 0x2a, 0x00}) {
		t.Fatalf("re-encoded %x", got)
	}
}

func TestOneofLastMemberWins(t *testing.T) {
	// value, then failure: failure wins.
	b := []byte{0x08, 0x01, 0x2a, 0x03, 0x0a, 0x01, 'x', 0x32, 0x02, 0x08, 0x07}
	var m RunCompletion
	if err := m.Unmarshal(b); err != nil {
		t.Fatal(err)
	}
	if m.Value != nil || m.Failure == nil || m.Failure.Code != 7 {
		t.Fatalf("got %+v", m)
	}
	// The same nested member twice merges: content from the first, len from the second.
	b = []byte{0x08, 0x01, 0x2a, 0x03, 0x0a, 0x01, 'x', 0x2a, 0x02, 0x18, 0x01}
	m = RunCompletion{}
	if err := m.Unmarshal(b); err != nil {
		t.Fatal(err)
	}
	if m.Value == nil || string(m.Value.Content) != "x" || m.Value.Len != 1 {
		t.Fatalf("got %+v", m.Value)
	}
	// ProposeRunCompletion: failure then a bytes value: the value wins.
	b = []byte{0x08, 0x02, 0x7a, 0x00, 0x72, 0x00}
	var p ProposeRunCompletion
	if err := p.Unmarshal(b); err != nil {
		t.Fatal(err)
	}
	if !p.HasValue || p.Failure != nil || len(p.Value) != 0 {
		t.Fatalf("got %+v", p)
	}
	// SignalNotification: numbered then named: named wins.
	b = []byte{0x10, 0x01, 0x1a, 0x01, 'n', 0x22, 0x00}
	var s SignalNotification
	if err := s.Unmarshal(b); err != nil {
		t.Fatal(err)
	}
	if s.Signal.Idx != nil || s.Signal.Name == nil || *s.Signal.Name != "n" || !s.Void {
		t.Fatalf("got %+v", s)
	}
	// Marshal of a message with several members set writes only the highest field.
	both := &RunCompletion{ID: 1, Result: Result{Value: &Value{}, Failure: &Failure{Code: 1}}}
	if got := Marshal(both); !bytes.Equal(got, []byte{0x08, 0x01, 0x32, 0x02, 0x08, 0x01}) {
		t.Fatalf("%x", got)
	}
}

func TestDecodeErrors(t *testing.T) {
	cases := []struct {
		name string
		t    MessageType
		b    []byte
	}{
		{"truncated varint", TypeCommandAck, []byte{0x08, 0x80}},
		{"varint over 64 bits", TypeCommandAck, []byte{0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02}},
		{"length past the end", TypeStart, []byte{0x0a, 0x05, 1, 2}},
		{"field number 0", TypeStart, []byte{0x00, 0x01}},
		{"known field with the wrong wire type", TypeStart, []byte{0x0d, 1, 2, 3, 4}},
		{"wrong wire type in a nested message", TypeInput, []byte{0x72, 0x02, 0x08, 0x01}},
		{"repeated number with fixed64 wire type", TypeSuspension, []byte{0x09, 1, 2, 3, 4, 5, 6, 7, 8}},
		{"group without end", TypeEnd, []byte{0x0b, 0x08, 0x01}},
		{"end group without start", TypeEnd, []byte{0x0c}},
		{"mismatched end group", TypeEnd, []byte{0x0b, 0x14}},
		{"wire type 6", TypeEnd, []byte{0x0e}},
		{"truncated fixed64", TypeSuspension, []byte{0x31, 1, 2}},
		{"bad bytes inside Void", TypeSleepCompletion, []byte{0x22, 0x01, 0x08}},
		{"field key over 32 bits", TypeEnd, []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x01}},
	}
	for _, c := range cases {
		if _, err := Decode(c.t, c.b); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v, want ErrMalformed", c.name, err)
		}
	}
	for _, code := range []MessageType{0x0006, 0x040F, 0x0414, 0x000E, 0x0500, 0x8000, 0x8003, 0xFBFE} {
		if _, err := Decode(code, nil); !errors.Is(err, ErrUnknownType) {
			t.Errorf("%s: %v, want ErrUnknownType", code, err)
		}
	}
}

func TestNestingDepthIsBounded(t *testing.T) {
	// 200 levels of Suspension.nested_futures.
	var b []byte
	for range 200 {
		b = appendBytesAlways(nil, 4, b)
	}
	if _, err := Decode(TypeSuspension, b); !errors.Is(err, ErrMalformed) {
		t.Fatalf("200 levels: %v", err)
	}
	b = nil
	for range 50 {
		b = appendBytesAlways(nil, 4, b)
	}
	if _, err := Decode(TypeSuspension, b); err != nil {
		t.Fatalf("50 levels: %v", err)
	}
}

func TestCustomFramesAreOpaque(t *testing.T) {
	m, err := Decode(0xFC01, []byte{0xff, 0xff})
	if err != nil {
		t.Fatal(err)
	}
	c, ok := m.(*Custom)
	if !ok || c.Code != 0xFC01 || !bytes.Equal(c.Payload, []byte{0xff, 0xff}) {
		t.Fatalf("%#v", m)
	}
	if !MessageType(0xFFFF).IsCustom() || MessageType(0xFBFF).IsCustom() || !MessageType(0xFC00).IsSideChannel() {
		t.Fatal("custom range")
	}
}

func TestTypeClasses(t *testing.T) {
	for code, name := range typeNames {
		if code.String() != name {
			t.Errorf("%04x: %q", uint16(code), code.String())
		}
		classes := 0
		for _, in := range []bool{code.IsControl(), code.IsCommand(), code.IsNotification(), code.IsCustom()} {
			if in {
				classes++
			}
		}
		if classes != 1 {
			t.Errorf("%s is in %d classes", code, classes)
		}
		if code.IsCommand() != code.MayRequestAck() && code != TypeProposeRunCompletion {
			t.Errorf("%s: MayRequestAck = %v", code, code.MayRequestAck())
		}
		if _, isCmd := New(code).(Command); isCmd != code.IsCommand() {
			t.Errorf("%s: implements Command = %v", code, isCmd)
		}
		if _, isNote := New(code).(Notification); isNote != code.IsNotification() {
			t.Errorf("%s: implements Notification = %v", code, isNote)
		}
		if _, isCompletion := New(code).(Completion); isCompletion != (code.IsNotification() && code != TypeSignal) {
			t.Errorf("%s: implements Completion = %v", code, isCompletion)
		}
	}
	if MessageType(0x0006).String() != "0x0006" || MessageType(0xFC01).String() != "0xFC01" {
		t.Fatal("names of codes without a message")
	}
	if TypeSleep.CompletionType() != TypeSleepCompletion || TypeOneWayCall.CompletionType() != TypeCallInvocationIDCompletion {
		t.Fatal("CompletionType")
	}
	if TypeEnd.MayRequestAck() || !TypeProposeRunCompletion.MayRequestAck() || MessageType(0x040F).MayRequestAck() {
		t.Fatal("MayRequestAck")
	}
}

func TestValueCheck(t *testing.T) {
	ok := []*Value{
		{},
		{Content: []byte("abc")},
		{Content: []byte("abc"), Len: 3},
		{Content: []byte("ef"), Segments: []SegmentRef{{Sha256: sha32(), Len: 4}}, Len: 6},
		{Segments: []SegmentRef{{Sha256: sha32(), Len: 4}, {Sha256: sha32(), Len: 0}}, Len: 4},
	}
	for i, v := range ok {
		if err := v.Check(); err != nil {
			t.Errorf("ok %d: %v", i, err)
		}
	}
	bad := []*Value{
		{Content: []byte("abc"), Len: 4},
		{Content: []byte("ef"), Segments: []SegmentRef{{Sha256: sha32(), Len: 4}}, Len: 7},
		{Content: []byte("ef"), Segments: []SegmentRef{{Sha256: sha32()[:31], Len: 4}}, Len: 6},
		{Segments: []SegmentRef{{Sha256: sha32(), Len: 1 << 63}, {Sha256: sha32(), Len: 1 << 63}}, Len: 0},
	}
	for i, v := range bad {
		if err := v.Check(); !errors.Is(err, ErrValueLayout) {
			t.Errorf("bad %d: %v", i, err)
		}
	}
	v := &Value{Content: []byte("x"), Segments: []SegmentRef{{Sha256: sha32(), Len: 1, Gen: 2}}, Len: 2}
	back, err := UnmarshalValue(MarshalValue(v))
	if err != nil || !reflect.DeepEqual(back, v) {
		t.Fatalf("%+v %v", back, err)
	}
}

func TestDecodedBytesDoNotAliasThePayload(t *testing.T) {
	payload := Marshal(&SetState{Key: []byte("key"), Value: InlineValue([]byte("value"))})
	m, err := Decode(TypeSetState, payload)
	if err != nil {
		t.Fatal(err)
	}
	clear(payload)
	s := m.(*SetState)
	if string(s.Key) != "key" || string(s.Value.Content) != "value" {
		t.Fatalf("decoded fields changed with the payload: %+v", s)
	}
}

// emptyRefs is a Value payload of n empty SegmentRefs: two bytes each on the wire.
func emptyRefs(n int) []byte {
	b := make([]byte, 0, 2*n)
	for range n {
		b = append(b, 0x12, 0x00)
	}
	return b
}

// A ref without its 32-byte sha256 is refused while decoding, and so are more refs than a
// value may carry: a 2-byte empty ref used to become a 40-byte struct plus a reader, about
// 75 times its size on the wire, before Value.Check ran.
//
// Must see: an error for a ref whose sha256 is not 32 bytes, for MaxSegmentRefs+1 refs, for
// a read URL whose sha256 is not 32 bytes and for more read URLs than a turn may use; and
// decoding 10000 empty refs allocates less than 8 times their size.
func TestDecodingRefusesBadSegmentRefs(t *testing.T) {
	payload := emptyRefs(10000)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, _ = UnmarshalValue(payload)
	runtime.ReadMemStats(&after)
	if got := after.TotalAlloc - before.TotalAlloc; got > 8*uint64(len(payload)) {
		t.Fatalf("decoding %d bytes of empty refs allocated %d bytes", len(payload), got)
	}

	for _, sha := range [][]byte{nil, sha32()[:31], append(sha32(), 0)} {
		v := &Value{Segments: []SegmentRef{{Sha256: sha, Len: 1}}, Len: 1}
		if _, err := UnmarshalValue(MarshalValue(v)); !errors.Is(err, ErrMalformed) {
			t.Fatalf("a %d-byte sha256: %v", len(sha), err)
		}
	}
	refs := make([]SegmentRef, MaxSegmentRefs+1)
	for i := range refs {
		refs[i] = SegmentRef{Sha256: sha32(), Len: 1}
	}
	if _, err := UnmarshalValue(MarshalValue(&Value{Segments: refs[:MaxSegmentRefs], Len: MaxSegmentRefs})); err != nil {
		t.Fatalf("%d refs: %v", MaxSegmentRefs, err)
	}
	if _, err := UnmarshalValue(MarshalValue(&Value{Segments: refs, Len: MaxSegmentRefs + 1})); !errors.Is(err, ErrMalformed) {
		t.Fatalf("%d refs: %v", MaxSegmentRefs+1, err)
	}
	var m SegmentUrls
	if err := m.Unmarshal(Marshal(&SegmentUrls{URLs: []SegmentURL{{Sha256: sha32()[:4], URL: "u"}}})); !errors.Is(err, ErrMalformed) {
		t.Fatalf("a short read URL sha256: %v", err)
	}
	urls := make([]SegmentURL, 2*MaxSegmentRefs+1)
	for i := range urls {
		urls[i] = SegmentURL{Sha256: sha32(), URL: "u"}
	}
	if err := m.Unmarshal(Marshal(&SegmentUrls{URLs: urls})); !errors.Is(err, ErrMalformed) {
		t.Fatalf("%d read URLs: %v", len(urls), err)
	}
	var st Start
	if err := st.Unmarshal(Marshal(&Start{ID: []byte{1}, SegmentURLs: urls})); !errors.Is(err, ErrMalformed) {
		t.Fatalf("%d read URLs in Start: %v", len(urls), err)
	}
}
