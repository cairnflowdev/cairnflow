# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""Minimal protobuf: just enough for ``docs/protocol-v1.md`` section 7.

Hand-written instead of depending on ``protobuf``: the surface is fixed and small
(varint, length-delimited, fixed64 and about twenty messages), and an SDK with no
dependencies installs anywhere. The Python twin of ``sdk/typescript/src/protobuf.ts``.

**Canonical form.** :class:`Writer` writes in exactly the order it is called, so callers
write fields in ascending tag order, skip default values (except ``oneof`` members, written
with the ``*_always`` methods) and pack numeric ``repeated`` fields. That is what every
official protobuf library produces, and what the conformance traces compare byte by byte.
"""

from __future__ import annotations

from collections.abc import Iterator

WIRE_VARINT = 0
WIRE_FIXED64 = 1
WIRE_LEN = 2
WIRE_FIXED32 = 5

_U64 = (1 << 64) - 1


class DecodeError(ValueError):
    """A truncated or malformed protobuf message."""


def _varint(out: bytearray, value: int) -> None:
    if value < 0:
        raise ValueError("negative varints are not supported")
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return


class Writer:
    """Writes a protobuf message. Call in increasing tag order."""

    __slots__ = ("_buf",)

    def __init__(self) -> None:
        self._buf = bytearray()

    def _tag(self, field: int, wire: int) -> None:
        _varint(self._buf, (field << 3) | wire)

    def uint32(self, field: int, value: int) -> Writer:
        if value:
            self._tag(field, WIRE_VARINT)
            _varint(self._buf, value)
        return self

    uint64 = uint32

    def uint32_always(self, field: int, value: int) -> Writer:
        """A ``oneof`` member: written even when it is the default value."""
        self._tag(field, WIRE_VARINT)
        _varint(self._buf, value)
        return self

    def boolean(self, field: int, value: bool) -> Writer:
        return self.uint32(field, 1 if value else 0)

    def fixed64(self, field: int, value: int) -> Writer:
        if value:
            self._tag(field, WIRE_FIXED64)
            self._buf += (value & _U64).to_bytes(8, "little")
        return self

    def packed_uint32(self, field: int, values: list[int] | tuple[int, ...]) -> Writer:
        if not values:
            return self
        inner = bytearray()
        for v in values:
            _varint(inner, v)
        return self.bytes_always(field, bytes(inner))

    def bytes(self, field: int, value: bytes) -> Writer:
        if not value:
            return self
        return self.bytes_always(field, value)

    def bytes_always(self, field: int, value: bytes) -> Writer:
        self._tag(field, WIRE_LEN)
        _varint(self._buf, len(value))
        self._buf += value
        return self

    def string(self, field: int, value: str) -> Writer:
        return self.bytes(field, value.encode("utf-8"))

    def message(self, field: int, sub: Writer) -> Writer:
        """A sub-message: always written, even when empty (presence matters)."""
        return self.bytes_always(field, sub.finish())

    def finish(self) -> bytes:
        return bytes(self._buf)


def read_varint(buf: bytes, pos: int) -> tuple[int, int]:
    """Reads one varint at ``pos``; returns ``(value, next position)``."""
    result = 0
    shift = 0
    n = len(buf)
    while True:
        if pos >= n:
            raise DecodeError("truncated protobuf")
        b = buf[pos]
        pos += 1
        # The 10th byte only has room for bit 63; larger overflows 64 bits (prost rejects it).
        if shift == 63 and b > 1:
            raise DecodeError("varint overflows 64 bits")
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, pos
        shift += 7
        if shift > 63:
            raise DecodeError("varint longer than 10 bytes")


def fields(buf: bytes) -> Iterator[tuple[int, int, int | bytes]]:
    """Yields ``(field, wire type, value)`` for every field of a message.

    The value is an ``int`` for varint, fixed64 and fixed32 fields and ``bytes`` for
    length-delimited ones. Unknown fields are the caller's to ignore.
    """
    pos = 0
    n = len(buf)
    while pos < n:
        key, pos = read_varint(buf, pos)
        field, wire = key >> 3, key & 7
        if wire == WIRE_VARINT:
            value, pos = read_varint(buf, pos)
            yield field, wire, value
        elif wire == WIRE_LEN:
            size, pos = read_varint(buf, pos)
            if pos + size > n:
                raise DecodeError("truncated protobuf")
            yield field, wire, bytes(buf[pos : pos + size])
            pos += size
        elif wire == WIRE_FIXED64:
            if pos + 8 > n:
                raise DecodeError("truncated protobuf")
            yield field, wire, int.from_bytes(buf[pos : pos + 8], "little")
            pos += 8
        elif wire == WIRE_FIXED32:
            if pos + 4 > n:
                raise DecodeError("truncated protobuf")
            yield field, wire, int.from_bytes(buf[pos : pos + 4], "little")
            pos += 4
        else:
            raise DecodeError(f"unsupported wire type {wire}")


def unpack_uint32(value: int | bytes) -> list[int]:
    """A ``repeated uint32``: packed (bytes) or one element per tag (int)."""
    if isinstance(value, int):
        return [value]
    out: list[int] = []
    pos = 0
    while pos < len(value):
        v, pos = read_varint(value, pos)
        out.append(v)
    return out
