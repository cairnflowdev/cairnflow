// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"strings"
	"unicode"
)

// replayIgnored lists, per command type, the fields a replay does not compare: values
// computed once and then recorded (the wake-up time of Sleep, the send time of
// OneWayCall), and values the runtime may have rewritten (SetState.value, the embedded
// result of GetEagerState and GetEagerStateKeys).
var replayIgnored = map[MessageType][]int{
	TypeSleep:             {1},
	TypeOneWayCall:        {4},
	TypeSetState:          {3},
	TypeGetEagerState:     {13, 14},
	TypeGetEagerStateKeys: {14},
}

type rawField struct {
	wt int
	v  []byte // the raw bytes of a length-delimited field, or the varint re-encoded
}

// fieldsOf collects the varint and length-delimited fields of a payload, in order, by
// field number; ignored fields and other wire types are left out. A payload that does not
// parse yields the fields read before the error, plus a marker under field 0.
func fieldsOf(payload []byte, ignored []int) map[int][]rawField {
	out := map[int][]rawField{}
	err := walk(payload, 0, func(r *reader, field, wt int) (bool, error) {
		for _, f := range ignored {
			if f == field {
				return false, nil
			}
		}
		switch wt {
		case wireVarint:
			v, err := r.varint()
			if err != nil {
				return true, err
			}
			out[field] = append(out[field], rawField{wt, appendVarint(nil, v)})
		case wireBytes:
			v, err := r.raw()
			if err != nil {
				return true, err
			}
			out[field] = append(out[field], rawField{wt, v})
		default:
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		out[0] = append(out[0], rawField{-1, []byte(err.Error())})
	}
	return out
}

// CommandDiff compares a journaled command with the command the current code builds at
// the same position, field by field: target, key, parameters, completion id (every field
// except the ones a replay leaves out, see replayIgnored). It returns the lowest field
// number that differs, and false when the two match (docs/protocol-v1.md section 6).
func CommandDiff(t MessageType, journaled, current []byte) (int, bool) {
	ignored := replayIgnored[t]
	a := fieldsOf(journaled, ignored)
	b := fieldsOf(current, ignored)
	lowest, found := 0, false
	check := func(f int) {
		x, y := a[f], b[f]
		same := len(x) == len(y)
		for i := 0; same && i < len(x); i++ {
			same = x[i].wt == y[i].wt && bytes.Equal(x[i].v, y[i].v)
		}
		if !same && (!found || f < lowest) {
			lowest, found = f, true
		}
	}
	for f := range a {
		check(f)
	}
	for f := range b {
		check(f)
	}
	return lowest, found
}

// EntryNameOf returns the name (field 12) of a command payload; empty when absent or the
// payload does not parse.
func EntryNameOf(payload []byte) string {
	var name string
	_ = walk(payload, 0, func(r *reader, field, wt int) (bool, error) {
		if field != 12 || wt != wireBytes {
			return false, nil
		}
		v, err := r.raw()
		name = string(v)
		return true, err
	})
	return name
}

// UpperName is the message name in the constant style of the other SDKs' type tables
// ("GET_STATE", "ONE_WAY_CALL"), used in journal mismatch messages; "0xNNNN" for a code
// without a message.
func (t MessageType) UpperName() string {
	n, ok := typeNames[t]
	if !ok {
		return t.String()
	}
	var b strings.Builder
	for i, r := range n {
		if i > 0 && unicode.IsUpper(r) {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToUpper(r))
	}
	return b.String()
}
