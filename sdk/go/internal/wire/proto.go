// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Protobuf wire types.
const (
	wireVarint  = 0
	wireFixed64 = 1
	wireBytes   = 2
	wireStart   = 3 // deprecated group start, only skipped
	wireEnd     = 4 // deprecated group end, only skipped
	wireFixed32 = 5
)

// maxDepth bounds nesting (messages inside messages, groups inside groups), as prost does.
const maxDepth = 100

// ErrMalformed is wrapped by every error that reports a payload which is not valid protobuf
// for the message it is decoded as.
var ErrMalformed = errors.New("malformed protobuf")

func malformed(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrMalformed, fmt.Sprintf(format, args...))
}

// ─────────────────────────────────────────────────────────── encoding

func appendVarint(b []byte, v uint64) []byte {
	return binary.AppendUvarint(b, v)
}

func appendTag(b []byte, field, wt int) []byte {
	return appendVarint(b, uint64(field)<<3|uint64(wt))
}

// appendUint32 writes a uint32 field, skipped when 0 (proto3 default).
func appendUint32(b []byte, field int, v uint32) []byte {
	if v == 0 {
		return b
	}
	return appendVarint(appendTag(b, field, wireVarint), uint64(v))
}

// appendUint64 writes a uint64 field, skipped when 0.
func appendUint64(b []byte, field int, v uint64) []byte {
	if v == 0 {
		return b
	}
	return appendVarint(appendTag(b, field, wireVarint), v)
}

// appendEnum writes an enum (int32 on the wire, sign-extended to 64 bits), skipped when 0.
func appendEnum(b []byte, field int, v int32) []byte {
	if v == 0 {
		return b
	}
	return appendVarint(appendTag(b, field, wireVarint), uint64(int64(v)))
}

// appendBool writes a bool field, skipped when false.
func appendBool(b []byte, field int, v bool) []byte {
	if !v {
		return b
	}
	return append(appendTag(b, field, wireVarint), 1)
}

// appendFixed64 writes a fixed64 field (8 bytes little-endian), skipped when 0.
func appendFixed64(b []byte, field int, v uint64) []byte {
	if v == 0 {
		return b
	}
	return binary.LittleEndian.AppendUint64(appendTag(b, field, wireFixed64), v)
}

// appendBytes writes a bytes field, skipped when empty.
func appendBytes(b []byte, field int, v []byte) []byte {
	if len(v) == 0 {
		return b
	}
	return appendBytesAlways(b, field, v)
}

// appendBytesAlways writes a bytes field even when empty (repeated elements, oneof members).
func appendBytesAlways(b []byte, field int, v []byte) []byte {
	b = appendVarint(appendTag(b, field, wireBytes), uint64(len(v)))
	return append(b, v...)
}

// appendString writes a string field, skipped when empty.
func appendString(b []byte, field int, v string) []byte {
	if v == "" {
		return b
	}
	return appendStringAlways(b, field, v)
}

// appendStringAlways writes a string field even when empty.
func appendStringAlways(b []byte, field int, v string) []byte {
	b = appendVarint(appendTag(b, field, wireBytes), uint64(len(v)))
	return append(b, v...)
}

// appendOptUint32 writes a proto3 optional (or oneof) uint32: present means written, even if 0.
func appendOptUint32(b []byte, field int, v *uint32) []byte {
	if v == nil {
		return b
	}
	return appendVarint(appendTag(b, field, wireVarint), uint64(*v))
}

// appendOptUint64 writes a proto3 optional uint64.
func appendOptUint64(b []byte, field int, v *uint64) []byte {
	if v == nil {
		return b
	}
	return appendVarint(appendTag(b, field, wireVarint), *v)
}

// appendOptString writes a proto3 optional (or oneof) string.
func appendOptString(b []byte, field int, v *string) []byte {
	if v == nil {
		return b
	}
	return appendStringAlways(b, field, *v)
}

// appendPacked writes a repeated uint32 field in packed form; nothing when empty.
func appendPacked(b []byte, field int, vs []uint32) []byte {
	if len(vs) == 0 {
		return b
	}
	n := 0
	for _, v := range vs {
		n += varintLen(uint64(v))
	}
	b = appendVarint(appendTag(b, field, wireBytes), uint64(n))
	for _, v := range vs {
		b = appendVarint(b, uint64(v))
	}
	return b
}

// appendVoid writes an empty nested message (Void) when present.
func appendVoid(b []byte, field int, present bool) []byte {
	if !present {
		return b
	}
	return append(appendTag(b, field, wireBytes), 0)
}

// encoder is implemented by every struct of this package that is a protobuf message.
type encoder interface {
	appendProto(b []byte) []byte
}

// appendMessage writes a nested message, even when it encodes to nothing: presence is
// information (an empty Value is not an absent Value).
func appendMessage(b []byte, field int, m encoder) []byte {
	b = appendTag(b, field, wireBytes)
	// Reserve one byte for the length, the common case; shift when it needs more.
	at := len(b)
	b = append(b, 0)
	b = m.appendProto(b)
	n := len(b) - at - 1
	if n < 0x80 {
		b[at] = byte(n)
		return b
	}
	extra := varintLen(uint64(n)) - 1
	b = append(b, make([]byte, extra)...)
	copy(b[at+1+extra:], b[at+1:len(b)-extra])
	binary.PutUvarint(b[at:], uint64(n))
	return b
}

func varintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

// ─────────────────────────────────────────────────────────── decoding

// reader walks the fields of one message.
type reader struct {
	b     []byte
	i     int
	depth int
}

func newReader(b []byte, depth int) (*reader, error) {
	if depth > maxDepth {
		return nil, malformed("nesting deeper than %d", maxDepth)
	}
	return &reader{b: b, depth: depth}, nil
}

func (r *reader) done() bool { return r.i >= len(r.b) }

func (r *reader) varint() (uint64, error) {
	var v uint64
	for shift := uint(0); ; shift += 7 {
		if r.i >= len(r.b) {
			return 0, malformed("truncated varint")
		}
		c := r.b[r.i]
		r.i++
		// The tenth byte only has room for bit 63.
		if shift == 63 && c > 1 {
			return 0, malformed("varint overflows 64 bits")
		}
		v |= uint64(c&0x7f) << shift
		if c < 0x80 {
			return v, nil
		}
	}
}

// tag reads a field key.
func (r *reader) tag() (field, wt int, err error) {
	k, err := r.varint()
	if err != nil {
		return 0, 0, err
	}
	if k > math.MaxUint32 {
		return 0, 0, malformed("field key %d does not fit in 32 bits", k)
	}
	field, wt = int(k>>3), int(k&7)
	if field == 0 {
		return 0, 0, malformed("field number 0")
	}
	return field, wt, nil
}

// raw reads a length-delimited field and returns a view into the buffer.
func (r *reader) raw() ([]byte, error) {
	n, err := r.varint()
	if err != nil {
		return nil, err
	}
	if n > uint64(len(r.b)-r.i) {
		return nil, malformed("length %d runs past the end of the message", n)
	}
	v := r.b[r.i : r.i+int(n)]
	r.i += int(n)
	return v, nil
}

// bytes reads a length-delimited field and returns a copy.
func (r *reader) bytes() ([]byte, error) {
	v, err := r.raw()
	if err != nil {
		return nil, err
	}
	return append([]byte{}, v...), nil
}

func (r *reader) string() (string, error) {
	v, err := r.raw()
	if err != nil {
		return "", err
	}
	return string(v), nil
}

func (r *reader) fixed64() (uint64, error) {
	if len(r.b)-r.i < 8 {
		return 0, malformed("truncated fixed64")
	}
	v := binary.LittleEndian.Uint64(r.b[r.i:])
	r.i += 8
	return v, nil
}

// skip skips one field of wire type wt whose key was just read.
func (r *reader) skip(field, wt int) error {
	switch wt {
	case wireVarint:
		_, err := r.varint()
		return err
	case wireFixed64:
		if len(r.b)-r.i < 8 {
			return malformed("truncated fixed64")
		}
		r.i += 8
		return nil
	case wireBytes:
		_, err := r.raw()
		return err
	case wireFixed32:
		if len(r.b)-r.i < 4 {
			return malformed("truncated fixed32")
		}
		r.i += 4
		return nil
	case wireStart:
		return r.skipGroup(field, r.depth+1)
	default:
		return malformed("unexpected wire type %d", wt)
	}
}

// skipGroup skips a deprecated group up to its matching end tag.
func (r *reader) skipGroup(field, depth int) error {
	if depth > maxDepth {
		return malformed("nesting deeper than %d", maxDepth)
	}
	for {
		if r.done() {
			return malformed("group %d has no end", field)
		}
		f, wt, err := r.tag()
		if err != nil {
			return err
		}
		if wt == wireEnd {
			if f != field {
				return malformed("group %d ends with field %d", field, f)
			}
			return nil
		}
		if wt == wireStart {
			if err := r.skipGroup(f, depth+1); err != nil {
				return err
			}
			continue
		}
		if err := r.skip(f, wt); err != nil {
			return err
		}
	}
}

// want checks the wire type of a known field.
func want(field, got, expected int) error {
	if got != expected {
		return malformed("field %d has wire type %d, expected %d", field, got, expected)
	}
	return nil
}

func (r *reader) uint32Field(field, wt int) (uint32, error) {
	if err := want(field, wt, wireVarint); err != nil {
		return 0, err
	}
	v, err := r.varint()
	return uint32(v), err // proto: a uint32 read from a wider varint is truncated
}

func (r *reader) uint64Field(field, wt int) (uint64, error) {
	if err := want(field, wt, wireVarint); err != nil {
		return 0, err
	}
	return r.varint()
}

func (r *reader) enumField(field, wt int) (int32, error) {
	if err := want(field, wt, wireVarint); err != nil {
		return 0, err
	}
	v, err := r.varint()
	return int32(v), err
}

func (r *reader) boolField(field, wt int) (bool, error) {
	if err := want(field, wt, wireVarint); err != nil {
		return false, err
	}
	v, err := r.varint()
	return v != 0, err
}

func (r *reader) fixed64Field(field, wt int) (uint64, error) {
	if err := want(field, wt, wireFixed64); err != nil {
		return 0, err
	}
	return r.fixed64()
}

func (r *reader) bytesField(field, wt int) ([]byte, error) {
	if err := want(field, wt, wireBytes); err != nil {
		return nil, err
	}
	return r.bytes()
}

func (r *reader) stringField(field, wt int) (string, error) {
	if err := want(field, wt, wireBytes); err != nil {
		return "", err
	}
	return r.string()
}

// packedUint32 reads one occurrence of a repeated uint32 field, packed or not, and appends it.
func (r *reader) packedUint32(dst []uint32, field, wt int) ([]uint32, error) {
	switch wt {
	case wireVarint:
		v, err := r.varint()
		return append(dst, uint32(v)), err
	case wireBytes:
		raw, err := r.raw()
		if err != nil {
			return dst, err
		}
		sub := &reader{b: raw}
		for !sub.done() {
			v, err := sub.varint()
			if err != nil {
				return dst, err
			}
			dst = append(dst, uint32(v))
		}
		return dst, nil
	default:
		return dst, malformed("field %d has wire type %d, expected 0 or 2", field, wt)
	}
}

// decoder is implemented by every struct of this package that is a protobuf message. Decoding
// into a struct that already holds values merges, as protobuf does for a repeated nested
// message field.
type decoder interface {
	unmarshalProto(b []byte, depth int) error
}

// nested reads a nested message field into m (merging into what m already holds).
func (r *reader) nested(field, wt int, m decoder) error {
	if err := want(field, wt, wireBytes); err != nil {
		return err
	}
	raw, err := r.raw()
	if err != nil {
		return err
	}
	return m.unmarshalProto(raw, r.depth+1)
}

// void reads an empty nested message (Void): its content must still be valid protobuf.
func (r *reader) void(field, wt int) error {
	return r.nested(field, wt, voidMessage{})
}

// voidMessage decodes a message with no known field: every field is skipped, a malformed
// field is still an error.
type voidMessage struct{}

func (voidMessage) unmarshalProto(b []byte, depth int) error {
	return walk(b, depth, func(*reader, int, int) (bool, error) { return false, nil })
}

func (voidMessage) appendProto(b []byte) []byte { return b }

// walk calls visit for every field of b. visit reports whether it consumed the field; an
// unconsumed field is skipped.
func walk(b []byte, depth int, visit func(r *reader, field, wt int) (bool, error)) error {
	r, err := newReader(b, depth)
	if err != nil {
		return err
	}
	for !r.done() {
		field, wt, err := r.tag()
		if err != nil {
			return err
		}
		if wt == wireEnd {
			return malformed("unexpected end of group %d", field)
		}
		ok, err := visit(r, field, wt)
		if err != nil {
			return err
		}
		if !ok {
			if err := r.skip(field, wt); err != nil {
				return err
			}
		}
	}
	return nil
}
