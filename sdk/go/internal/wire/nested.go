// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"errors"
	"fmt"
)

// Value is a user value: `Value { bytes content = 1; repeated SegmentRef segments = 2;
// uint64 len = 3; }`.
//
// No segments: inline, Content is the whole value and Len is 0 (the bytes are identical to the
// layout before wire v6). With segments (wire v6): the value is the segments' bytes in order
// followed by Content (the tail), and Len is the total. Never read Content as the whole value
// when Segments is not empty; Check validates the layout.
type Value struct {
	Content  []byte
	Segments []SegmentRef
	Len      uint64
}

// InlineValue returns an inline Value holding b.
func InlineValue(b []byte) *Value { return &Value{Content: b} }

// IsSegmented reports whether the value carries segment refs.
func (v *Value) IsSegmented() bool { return len(v.Segments) > 0 }

// ErrValueLayout is wrapped by the errors of Value.Check.
var ErrValueLayout = errors.New("invalid value layout")

// ShaLen is the length of a segment's SHA-256.
const ShaLen = 32

// MaxSegmentRefs caps the refs of one segmented value. The runtime seals at least 60 KiB
// at a time (its smallest sealing threshold, 64 KiB, less the 4 KiB tail it keeps) and
// merges older segments in tiers of four, so a real value carries a few tens of refs; even
// with no merge at all, 4096 refs of 60 KiB hold 240 MiB, almost four times the default
// `max-value-bytes` (64 MiB). The refs are the sender's choice, and each distinct ref may
// cost a download. The decoder refuses a value with more.
const MaxSegmentRefs = 4096

// MaxSegmentURLs caps the read URLs of one message (`Start`, `SegmentUrls`): one per ref a
// turn may assemble, two values at MaxSegmentRefs. The decoder refuses a message with more.
const MaxSegmentURLs = 2 * MaxSegmentRefs

// Check validates the layout: inline ⇒ Len is 0 or exactly len(Content); segmented ⇒ every
// segment carries a 32-byte sha256 and Len is the sum of the segment lengths plus
// len(Content). It does not fetch or verify segment bytes.
func (v *Value) Check() error {
	if len(v.Segments) == 0 {
		if v.Len != 0 && v.Len != uint64(len(v.Content)) {
			return fmt.Errorf("%w: inline value declares length %d but carries %d bytes", ErrValueLayout, v.Len, len(v.Content))
		}
		return nil
	}
	sum := uint64(len(v.Content))
	for i, s := range v.Segments {
		if len(s.Sha256) != ShaLen {
			return fmt.Errorf("%w: segment %d has a %d-byte sha256, expected %d", ErrValueLayout, i, len(s.Sha256), ShaLen)
		}
		next := sum + s.Len
		if next < sum {
			return fmt.Errorf("%w: segment lengths overflow", ErrValueLayout)
		}
		sum = next
	}
	if sum != v.Len {
		return fmt.Errorf("%w: segmented value declares length %d but its parts sum to %d", ErrValueLayout, v.Len, sum)
	}
	return nil
}

func (v *Value) appendProto(b []byte) []byte {
	b = appendBytes(b, 1, v.Content)
	for i := range v.Segments {
		b = appendMessage(b, 2, &v.Segments[i])
	}
	return appendUint64(b, 3, v.Len)
}

func (v *Value) unmarshalProto(b []byte, depth int) error {
	return walk(b, depth, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			v.Content, err = r.bytesField(field, wt)
		case 2:
			// Checked before decoding the ref: the count and the sha256 are refused here,
			// not after the whole list is built (Value.Check), which a body of empty refs
			// would make about 75 times its size.
			if len(v.Segments) >= MaxSegmentRefs {
				return true, malformed("a value carries more than %d segment refs", MaxSegmentRefs)
			}
			var s SegmentRef
			if err = r.nested(field, wt, &s); err == nil {
				v.Segments = append(v.Segments, s)
			}
		case 3:
			v.Len, err = r.uint64Field(field, wt)
		default:
			return false, nil
		}
		return true, err
	})
}

// SegmentRef points at one segment of a segmented value: `SegmentRef { bytes sha256 = 1;
// uint64 len = 2; fixed64 gen = 3; }`. Gen is issued anew every time the runtime seals.
type SegmentRef struct {
	Sha256 []byte
	Len    uint64
	Gen    uint64
}

func (s *SegmentRef) appendProto(b []byte) []byte {
	b = appendBytes(b, 1, s.Sha256)
	b = appendUint64(b, 2, s.Len)
	return appendFixed64(b, 3, s.Gen)
}

func (s *SegmentRef) unmarshalProto(b []byte, depth int) error {
	err := walk(b, depth, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			s.Sha256, err = r.bytesField(field, wt)
		case 2:
			s.Len, err = r.uint64Field(field, wt)
		case 3:
			s.Gen, err = r.fixed64Field(field, wt)
		default:
			return false, nil
		}
		return true, err
	})
	if err == nil && len(s.Sha256) != ShaLen {
		err = malformed("a segment ref has a %d-byte sha256, expected %d", len(s.Sha256), ShaLen)
	}
	return err
}

// appendSegmentURL decodes one read URL (field, wt just read) onto list, refusing more
// than MaxSegmentURLs.
func appendSegmentURL(list []SegmentURL, r *reader, field, wt int) ([]SegmentURL, error) {
	if len(list) >= MaxSegmentURLs {
		return list, malformed("more than %d segment read URLs", MaxSegmentURLs)
	}
	var u SegmentURL
	if err := r.nested(field, wt, &u); err != nil {
		return list, err
	}
	return append(list, u), nil
}

// SegmentURL is the read URL of one segment (wire v6): `SegmentUrl { bytes sha256 = 1;
// fixed64 gen = 2; string url = 3; }`. A read URL is a capability: never log it.
type SegmentURL struct {
	Sha256 []byte
	Gen    uint64
	URL    string
}

func (s *SegmentURL) appendProto(b []byte) []byte {
	b = appendBytes(b, 1, s.Sha256)
	b = appendFixed64(b, 2, s.Gen)
	return appendString(b, 3, s.URL)
}

func (s *SegmentURL) unmarshalProto(b []byte, depth int) error {
	err := walk(b, depth, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			s.Sha256, err = r.bytesField(field, wt)
		case 2:
			s.Gen, err = r.fixed64Field(field, wt)
		case 3:
			s.URL, err = r.stringField(field, wt)
		default:
			return false, nil
		}
		return true, err
	})
	if err == nil && len(s.Sha256) != ShaLen {
		err = malformed("a segment read URL has a %d-byte sha256, expected %d", len(s.Sha256), ShaLen)
	}
	return err
}

// StateEntry is one key of the eager state map of Start: `StateEntry { bytes key = 1; bytes
// value = 2; }`. An empty Value means an empty value, not a missing key.
type StateEntry struct {
	Key   []byte
	Value []byte
}

func (e *StateEntry) appendProto(b []byte) []byte {
	b = appendBytes(b, 1, e.Key)
	return appendBytes(b, 2, e.Value)
}

func (e *StateEntry) unmarshalProto(b []byte, depth int) error {
	return walk(b, depth, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			e.Key, err = r.bytesField(field, wt)
		case 2:
			e.Value, err = r.bytesField(field, wt)
		default:
			return false, nil
		}
		return true, err
	})
}

// StateKeys is a list of state keys: `StateKeys { repeated bytes keys = 1; }`.
type StateKeys struct {
	Keys [][]byte
}

func (k *StateKeys) appendProto(b []byte) []byte {
	for _, key := range k.Keys {
		b = appendBytesAlways(b, 1, key)
	}
	return b
}

func (k *StateKeys) unmarshalProto(b []byte, depth int) error {
	return walk(b, depth, func(r *reader, field, wt int) (bool, error) {
		if field != 1 {
			return false, nil
		}
		key, err := r.bytesField(field, wt)
		if err == nil {
			k.Keys = append(k.Keys, key)
		}
		return true, err
	})
}

// Header is a key/value pair carried by Input (the request headers) and by calls (headers and
// tags): `Header { string key = 1; string value = 2; }`.
type Header struct {
	Key   string
	Value string
}

func (h *Header) appendProto(b []byte) []byte {
	b = appendString(b, 1, h.Key)
	return appendString(b, 2, h.Value)
}

func (h *Header) unmarshalProto(b []byte, depth int) error {
	return walk(b, depth, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			h.Key, err = r.stringField(field, wt)
		case 2:
			h.Value, err = r.stringField(field, wt)
		default:
			return false, nil
		}
		return true, err
	})
}

// Failure is an error result: `Failure { uint32 code = 1; string message = 2; repeated
// FailureMetadata metadata = 3; }`.
type Failure struct {
	Code     uint32
	Message  string
	Metadata []FailureMetadata
}

func (f *Failure) appendProto(b []byte) []byte {
	b = appendUint32(b, 1, f.Code)
	b = appendString(b, 2, f.Message)
	for i := range f.Metadata {
		b = appendMessage(b, 3, &f.Metadata[i])
	}
	return b
}

func (f *Failure) unmarshalProto(b []byte, depth int) error {
	return walk(b, depth, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			f.Code, err = r.uint32Field(field, wt)
		case 2:
			f.Message, err = r.stringField(field, wt)
		case 3:
			var m FailureMetadata
			if err = r.nested(field, wt, &m); err == nil {
				f.Metadata = append(f.Metadata, m)
			}
		default:
			return false, nil
		}
		return true, err
	})
}

// FailureMetadata is one key/value of a Failure: `FailureMetadata { string key = 1; string
// value = 2; }`.
type FailureMetadata struct {
	Key   string
	Value string
}

func (m *FailureMetadata) appendProto(b []byte) []byte {
	b = appendString(b, 1, m.Key)
	return appendString(b, 2, m.Value)
}

func (m *FailureMetadata) unmarshalProto(b []byte, depth int) error {
	return walk(b, depth, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.Key, err = r.stringField(field, wt)
		case 2:
			m.Value, err = r.stringField(field, wt)
		default:
			return false, nil
		}
		return true, err
	})
}

// StreamRecord is one record of a stream (wire v4): `StreamRecord { uint32 step = 1; uint32
// attempt = 2; uint32 k = 3; bytes payload = 4; }`. (Step, Attempt, K) identifies the record
// and strictly increases within one stream.
type StreamRecord struct {
	Step    uint32
	Attempt uint32
	K       uint32
	Payload []byte
}

func (s *StreamRecord) appendProto(b []byte) []byte {
	b = appendUint32(b, 1, s.Step)
	b = appendUint32(b, 2, s.Attempt)
	b = appendUint32(b, 3, s.K)
	return appendBytes(b, 4, s.Payload)
}

func (s *StreamRecord) unmarshalProto(b []byte, depth int) error {
	return walk(b, depth, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			s.Step, err = r.uint32Field(field, wt)
		case 2:
			s.Attempt, err = r.uint32Field(field, wt)
		case 3:
			s.K, err = r.uint32Field(field, wt)
		case 4:
			s.Payload, err = r.bytesField(field, wt)
		default:
			return false, nil
		}
		return true, err
	})
}

// Result is the result oneof shared by notifications and some commands: Void (field 4 in
// notifications), Value (5) or Failure (6). At most one member is set; which members a given
// message allows is fixed by its definition, and Marshal writes only those.
type Result struct {
	Void    bool
	Value   *Value
	Failure *Failure
}

// resultTags gives the field numbers of the Result members in one message; 0 = not a member.
type resultTags struct{ void, value, failure int }

var notificationResult = resultTags{4, 5, 6}

// appendResult writes the one member the message allows. When several are set it writes the
// highest field number (the one a decoder would keep); in every message the members are
// numbered void < value < failure.
func appendResult(b []byte, r *Result, t resultTags) []byte {
	switch {
	case t.failure != 0 && r.Failure != nil:
		return appendMessage(b, t.failure, r.Failure)
	case t.value != 0 && r.Value != nil:
		return appendMessage(b, t.value, r.Value)
	case t.void != 0 && r.Void:
		return appendVoid(b, t.void, true)
	}
	return b
}

// readResult reads field into r when it is one of the members t allows. The last member read
// wins; reading the same member twice merges, as in protobuf.
func readResult(rd *reader, field, wt int, r *Result, t resultTags) (bool, error) {
	switch {
	case field == t.void && t.void != 0:
		if err := rd.void(field, wt); err != nil {
			return true, err
		}
		*r = Result{Void: true}
	case field == t.value && t.value != 0:
		v := r.Value
		if v == nil {
			v = new(Value)
		}
		if err := rd.nested(field, wt, v); err != nil {
			return true, err
		}
		*r = Result{Value: v}
	case field == t.failure && t.failure != 0:
		f := r.Failure
		if f == nil {
			f = new(Failure)
		}
		if err := rd.nested(field, wt, f); err != nil {
			return true, err
		}
		*r = Result{Failure: f}
	default:
		return false, nil
	}
	return true, nil
}
