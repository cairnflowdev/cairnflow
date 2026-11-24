// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// encodeValue turns an input or a value into a request body. nil means no body (the handler
// sees no input, a signal carries Void); json.RawMessage and []byte go out as they are;
// anything else is JSON from encoding/json without HTML escaping, so the bytes match the
// TypeScript and Python clients.
func encodeValue(v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case json.RawMessage:
		if len(x) == 0 {
			return nil, nil
		}
		return x, nil
	case []byte:
		if len(x) == 0 {
			return nil, nil
		}
		return x, nil
	}
	return marshal(v)
}

// marshal is json.Marshal without HTML escaping and without the trailing newline.
func marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("cairnflow client: encode a value: %w", err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// decodeOutput turns the output of an invocation (a string holding the handler's JSON) into
// an O. An absent or empty output gives the zero O. json.RawMessage and []byte receive the
// bytes as they are. Output that is not JSON goes into a string or an any as the text itself,
// like the TypeScript and Python clients.
func decodeOutput[O any](raw string) (O, error) {
	var out O
	switch p := any(&out).(type) {
	case *json.RawMessage:
		if raw != "" {
			*p = json.RawMessage(raw)
		}
		return out, nil
	case *[]byte:
		if raw != "" {
			*p = []byte(raw)
		}
		return out, nil
	}
	if raw == "" {
		return out, nil
	}
	err := json.Unmarshal([]byte(raw), &out)
	if err == nil {
		return out, nil
	}
	if !json.Valid([]byte(raw)) {
		switch p := any(&out).(type) {
		case *string:
			*p = raw
			return out, nil
		case *any:
			*p = raw
			return out, nil
		}
	}
	var zero O
	return zero, fmt.Errorf("cairnflow client: decode the output into %T: %w", zero, err)
}

// decodeJSON decodes a 2xx JSON response into v; anything else is an *HTTPError.
func decodeJSON(res *response, v any) error {
	if !res.ok() {
		return httpError(res)
	}
	if err := json.Unmarshal(res.body, v); err != nil {
		return fmt.Errorf("cairnflow client: decode the response (HTTP %d): %w", res.status, err)
	}
	return nil
}

// enc is JavaScript's encodeURIComponent: everything but A-Z a-z 0-9 - _ . ! ~ * ' ( ) is
// percent-encoded, byte by byte.
func enc(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' ||
			strings.IndexByte("-_.!~*'()", c) >= 0 {
			b.WriteByte(c)
			continue
		}
		const hex = "0123456789ABCDEF"
		b.WriteByte('%')
		b.WriteByte(hex[c>>4])
		b.WriteByte(hex[c&15])
	}
	return b.String()
}

func scopePrefix(scope string) string {
	if scope == "" {
		return ""
	}
	return "/scope/" + enc(scope)
}
