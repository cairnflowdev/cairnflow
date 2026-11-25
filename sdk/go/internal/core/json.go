// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"bytes"
	"encoding/json"
	"strconv"
)

// Marshal encodes a value the way every Cairnflow SDK does: compact JSON with the bytes
// `JSON.stringify` produces in the TypeScript SDK for the same value — no HTML escaping,
// U+2028/U+2029 left as they are, `-0` written as `0`, no trailing newline.
//
// []byte and json.RawMessage pass through untouched (they are the value's bytes, not a
// base64 string); a nil one encodes as `null`.
//
// What stays different from JavaScript: Go writes map keys sorted (JavaScript keeps
// insertion order; use a struct for a fixed order), and integers beyond 2^53 keep every
// digit.
func Marshal(v any) ([]byte, error) {
	switch b := v.(type) {
	case []byte:
		if b == nil {
			return []byte("null"), nil
		}
		return bytes.Clone(b), nil
	case json.RawMessage:
		if b == nil {
			return []byte("null"), nil
		}
		return bytes.Clone(b), nil
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	out := buf.Bytes()
	out = out[:len(out)-1] // Encode adds a newline
	return jsCompatible(out), nil
}

// jsCompatible rewrites the two places where encoding/json differs from JSON.stringify
// on the same value: encoding/json always escapes U+2028 and U+2029 (JavaScript writes
// them raw), and writes negative zero as `-0` (JavaScript writes `0`).
func jsCompatible(b []byte) []byte {
	if !bytes.Contains(b, []byte(`\u202`)) && !bytes.Contains(b, []byte("-0")) {
		return b
	}
	out := make([]byte, 0, len(b))
	inString := false
	for i := 0; i < len(b); i++ {
		c := b[i]
		if inString {
			switch c {
			case '"':
				inString = false
				out = append(out, c)
			case '\\':
				if i+5 < len(b) && b[i+1] == 'u' && b[i+2] == '2' && b[i+3] == '0' && b[i+4] == '2' && (b[i+5] == '8' || b[i+5] == '9') {
					if b[i+5] == '8' {
						out = append(out, " "...)
					} else {
						out = append(out, " "...)
					}
					i += 5
					continue
				}
				// Copy the escape pair as it is, so an escaped backslash is not read again.
				out = append(out, c)
				if i+1 < len(b) {
					out = append(out, b[i+1])
					i++
				}
			default:
				out = append(out, c)
			}
			continue
		}
		switch {
		case c == '"':
			inString = true
			out = append(out, c)
		case c == '-' && i+1 < len(b) && b[i+1] == '0' && negativeZero(b[i+1:]):
			// Drop the sign of a negative zero.
		default:
			out = append(out, c)
		}
	}
	return out
}

// negativeZero reports whether the number starting at b (just after a '-') is a zero:
// "0", "0.0…", "0e…".
func negativeZero(b []byte) bool {
	end := 0
	for end < len(b) && (b[end] == '.' || b[end] == 'e' || b[end] == 'E' || b[end] == '+' || b[end] == '-' || (b[end] >= '0' && b[end] <= '9')) {
		end++
	}
	f, err := strconv.ParseFloat(string(b[:end]), 64)
	return err == nil && f == 0
}

// Unmarshal decodes value bytes into v. For *[]byte and *json.RawMessage the bytes are
// copied untouched.
func Unmarshal(raw []byte, v any) error {
	switch p := v.(type) {
	case *[]byte:
		*p = bytes.Clone(raw)
		return nil
	case *json.RawMessage:
		*p = bytes.Clone(raw)
		return nil
	}
	return json.Unmarshal(raw, v)
}

// StreamJSON is the JSON of a stream record: it never fails, since a stream must not
// break the invocation. A value that cannot be encoded becomes {"unserializable": "<error>"}.
func StreamJSON(v any) []byte {
	b, err := Marshal(v)
	if err != nil {
		b, _ = Marshal(map[string]string{"unserializable": err.Error()})
	}
	return b
}

// Object writes a JSON object with its keys in insertion order (log and trace records:
// the same layout as the other SDKs' records).
type Object struct {
	buf   []byte
	count int
}

// Field appends one key and its value (encoded with [StreamJSON]).
func (o *Object) Field(key string, value any) {
	o.Raw(key, StreamJSON(value))
}

// Raw appends one key and its already encoded value.
func (o *Object) Raw(key string, value []byte) {
	if o.count == 0 {
		o.buf = append(o.buf, '{')
	} else {
		o.buf = append(o.buf, ',')
	}
	o.count++
	k, _ := Marshal(key)
	o.buf = append(o.buf, k...)
	o.buf = append(o.buf, ':')
	o.buf = append(o.buf, value...)
}

// Len is the number of fields written.
func (o *Object) Len() int { return o.count }

// Bytes returns the object; `{}` when empty.
func (o *Object) Bytes() []byte {
	if o.count == 0 {
		return []byte("{}")
	}
	return append(bytes.Clone(o.buf), '}')
}
