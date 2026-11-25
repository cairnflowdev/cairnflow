// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
)

// Void is the type of "no value": a handler without input or output, a timer, a call
// whose result is ignored. It encodes as JSON `null` (what the other SDKs write for a
// handler returning nothing) and decodes from any value.
type Void struct{}

// MarshalJSON writes `null`.
func (Void) MarshalJSON() ([]byte, error) { return []byte("null"), nil }

// UnmarshalJSON accepts anything.
func (*Void) UnmarshalJSON([]byte) error { return nil }

// encode is the JSON of a value as it goes into the journal (see core.Marshal: the bytes
// match the TypeScript SDK's JSON.stringify; []byte and json.RawMessage pass through).
func encode(v any) ([]byte, error) {
	b, err := core.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("cairn: cannot encode %T as JSON: %w", v, err)
	}
	return b, nil
}

// decode reads value bytes into a T. Empty bytes are not valid JSON (a terminal error,
// as in the other SDKs), except for Void, []byte and json.RawMessage. A value that does
// not decode into T is a retryable error: a fixed deployment can read it.
func decode[T any](raw []byte, what string) (T, error) {
	var v T
	switch any(&v).(type) {
	case *Void:
		return v, nil
	case *[]byte, *json.RawMessage:
		return v, core.Unmarshal(raw, &v)
	}
	if len(raw) == 0 {
		return v, &TerminalError{code: 500, message: what + " is empty, not valid JSON"}
	}
	if err := core.Unmarshal(raw, &v); err != nil {
		return v, fmt.Errorf("cairn: cannot decode %s into %T: %w", what, v, err)
	}
	return v, nil
}

// decodeInput reads a handler's input: empty means the zero value; input that is not
// JSON is passed as the raw string when T is a string (ingress accepts any body, as the
// other SDKs do); otherwise it is a terminal 400 (retrying cannot fix the input).
func decodeInput[T any](raw []byte) (T, error) {
	var v T
	switch p := any(&v).(type) {
	case *Void:
		return v, nil
	case *[]byte, *json.RawMessage:
		return v, core.Unmarshal(raw, &v)
	case *string:
		if len(raw) == 0 {
			return v, nil
		}
		if err := json.Unmarshal(raw, p); err != nil {
			*p = strings.ToValidUTF8(string(raw), "�")
		}
		return v, nil
	}
	if len(raw) == 0 {
		return v, nil
	}
	if err := core.Unmarshal(raw, &v); err != nil {
		return v, &TerminalError{code: 400, message: fmt.Sprintf("invalid input for %T: %v", v, err), cause: err}
	}
	return v, nil
}

// wellFormed is a string exactly as it travels on the wire: invalid UTF-8 becomes
// U+FFFD, so names and keys compare equal on replay and two Go strings never share one
// durable key by accident.
func wellFormed(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	return strings.ToValidUTF8(s, "�")
}
