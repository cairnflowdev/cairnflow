# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""Framing and messages: sections 4, 5 and 7 of ``docs/protocol-v1.md``.

The Python twin of ``sdk/typescript/src/wire.ts`` (itself the twin of
``crates/cairn-protocol``). Every constant is copied from the specification; every
encoder writes fields in ascending tag order (canonical form), because the conformance
traces compare the SDK's frames byte by byte.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from collections.abc import Callable
from typing import NamedTuple

from ._pb import WIRE_FIXED64, WIRE_LEN, WIRE_VARINT, Writer, fields, unpack_uint32


class MT:
    """Message type codes (section 5)."""

    START = 0x0000
    SUSPENSION = 0x0001
    ERROR = 0x0002
    END = 0x0003
    COMMAND_ACK = 0x0004
    PROPOSE_RUN_COMPLETION = 0x0005
    PROPOSE_RUN_COMPLETION_ACK = 0x0007
    KEEPALIVE = 0x0008
    REPLAY_REPORT = 0x0009
    CONTINUE_AS_NEW = 0x000A
    STREAM_APPEND = 0x000B
    ARTIFACT_PUT = 0x000C
    SEGMENT_URLS = 0x000D

    INPUT = 0x0400
    OUTPUT = 0x0401
    GET_STATE = 0x0402
    SET_STATE = 0x0403
    CLEAR_STATE = 0x0404
    CLEAR_ALL_STATE = 0x0405
    GET_STATE_KEYS = 0x0406
    GET_EAGER_STATE = 0x0407
    GET_EAGER_STATE_KEYS = 0x0408
    GET_PROMISE = 0x0409
    PEEK_PROMISE = 0x040A
    COMPLETE_PROMISE = 0x040B
    SLEEP = 0x040C
    CALL = 0x040D
    ONE_WAY_CALL = 0x040E
    SEND_SIGNAL = 0x0410
    RUN = 0x0411
    ATTACH_INVOCATION = 0x0412
    GET_INVOCATION_OUTPUT = 0x0413

    GET_STATE_COMPLETION = 0x8002
    GET_STATE_KEYS_COMPLETION = 0x8006
    GET_PROMISE_COMPLETION = 0x8009
    PEEK_PROMISE_COMPLETION = 0x800A
    COMPLETE_PROMISE_COMPLETION = 0x800B
    SLEEP_COMPLETION = 0x800C
    CALL_COMPLETION = 0x800D
    CALL_INVOCATION_ID_COMPLETION = 0x800E
    RUN_COMPLETION = 0x8011
    ATTACH_INVOCATION_COMPLETION = 0x8012
    GET_INVOCATION_OUTPUT_COMPLETION = 0x8013
    SIGNAL = 0xFBFF


_NAMES = {v: k for k, v in vars(MT).items() if k.isupper()}

JOURNAL_MISMATCH = 570
PROTOCOL_VIOLATION = 571

#: Bit 47 of the 64-bit header: the high bit of the 16-bit flags field.
FLAG_REQUESTED_ACK = 0x8000
HEADER_LEN = 8
#: Payload length cap (section 4). Normative: both sides must use the same number.
MAX_FRAME_LEN = 16 * 1024 * 1024

ERROR_RETRY = 0
ERROR_PAUSE = 1
ERROR_FAIL = 2

COMBINATOR_FIRST = 1
COMBINATOR_ALL = 2

#: ``BuiltInSignal.CANCEL``: a cooperative cancel from the admin API.
CANCEL_SIGNAL = 1
#: ``CallPlacement.CALL_PLACEMENT_SPREAD``.
CALL_PLACEMENT_SPREAD = 1


def is_notification(ty: int) -> bool:
    return 0x8000 <= ty <= 0xFBFF


def is_command(ty: int) -> bool:
    return 0x0400 <= ty <= 0x04FF


def type_name(ty: int) -> str:
    return _NAMES.get(ty, f"0x{ty:04x}")


# ─────────────────────────────────────────────────────────── frames


class Frame(NamedTuple):
    type: int
    flags: int
    payload: bytes


class FrameError(ValueError):
    """A byte stream that does not split into valid frames."""


_HEADER = struct.Struct(">HHI")


def encode_frame(ty: int, flags: int, payload: bytes) -> bytes:
    return _HEADER.pack(ty, flags, len(payload)) + payload


def frames_to_bytes(frames: list[Frame]) -> bytes:
    return b"".join(encode_frame(f.type, f.flags, f.payload) for f in frames)


def decode_frames(buf: bytes) -> list[Frame]:
    """Splits a whole byte stream into frames.

    Trailing bytes are an **error**, not "wait for more": in ``REQUEST_RESPONSE`` the
    body has ended, so there is nothing left to wait for.
    """
    out: list[Frame] = []
    at = 0
    n = len(buf)
    while at + HEADER_LEN <= n:
        ty, flags, size = _HEADER.unpack_from(buf, at)
        # Check the header before waiting for the payload (section 4, length limit).
        if flags & ~FLAG_REQUESTED_ACK:
            raise FrameError(f"reserved flag bits must be 0, got 0x{flags:x}")
        if size > MAX_FRAME_LEN:
            raise FrameError(f"payload of {size} bytes exceeds the limit of {MAX_FRAME_LEN}")
        if at + HEADER_LEN + size > n:
            break
        out.append(Frame(ty, flags, bytes(buf[at + HEADER_LEN : at + HEADER_LEN + size])))
        at += HEADER_LEN + size
    if at != n:
        raise FrameError(f"truncated byte stream: {n - at} trailing bytes do not form a frame")
    return out


# ─────────────────────────────────────────────────────────── decoding


class ValueLayoutError(ValueError):
    """A ``Value`` with an invalid layout (length, segments)."""


@dataclass(frozen=True)
class SegmentRef:
    """``SegmentRef { bytes sha256 = 1; uint64 len = 2; fixed64 gen = 3; }``."""

    sha256: bytes
    len: int
    gen: int


@dataclass(frozen=True)
class SegmentUrl:
    """``SegmentUrl { bytes sha256 = 1; fixed64 gen = 2; string url = 3; }`` (wire v6)."""

    sha256: bytes
    gen: int
    url: str


@dataclass(frozen=True)
class WireValue:
    """A ``Value`` with its full layout. ``content`` is the **tail**: the logical value is
    the segments in order, then ``content``. Never read ``content`` alone as the value
    when ``segments`` is not empty."""

    content: bytes
    segments: tuple[SegmentRef, ...]
    len: int


def decode_segment_ref(payload: bytes) -> SegmentRef:
    sha, length, gen = b"", 0, 0
    for f, wire, v in fields(payload):
        if f == 1 and wire == WIRE_LEN:
            sha = _b(v)
        elif f == 2 and wire == WIRE_VARINT:
            length = int(v)
        elif f == 3 and wire == WIRE_FIXED64:
            gen = int(v)
    return SegmentRef(sha, length, gen)


def decode_segment_url(payload: bytes) -> SegmentUrl:
    sha, gen, url = b"", 0, ""
    for f, wire, v in fields(payload):
        if f == 1 and wire == WIRE_LEN:
            sha = _b(v)
        elif f == 2 and wire == WIRE_FIXED64:
            gen = int(v)
        elif f == 3 and wire == WIRE_LEN:
            url = _text(v)
    return SegmentUrl(sha, gen, url)


def decode_segment_urls(payload: bytes) -> list[SegmentUrl]:
    """``SegmentUrlsMessage { repeated SegmentUrl urls = 1; }`` (0x000D, wire v6)."""
    return [decode_segment_url(_b(v)) for f, wire, v in fields(payload) if f == 1 and wire == WIRE_LEN]


def encode_segment_urls(urls: list[SegmentUrl]) -> bytes:
    w = Writer()
    for u in urls:
        w.message(1, Writer().bytes(1, u.sha256).fixed64(2, u.gen).string(3, u.url))
    return w.finish()


def decode_wire_value(payload: bytes) -> WireValue:
    """A ``Value`` checked like the runtime's ``Logical::of``: with segments,
    ``len = sum(segments) + len(content)`` and 32-byte shas; inline, ``len`` is 0 or
    exactly ``len(content)``."""
    content = b""
    segments: list[SegmentRef] = []
    length = 0
    for f, wire, v in fields(payload):
        if f == 1 and wire == WIRE_LEN:
            content = _b(v)
        elif f == 2 and wire == WIRE_LEN:
            segments.append(decode_segment_ref(_b(v)))
        elif f == 3 and wire == WIRE_VARINT:
            length = int(v)
    if not segments:
        if length not in (0, len(content)):
            raise ValueLayoutError(f"inline value declares length {length} but carries {len(content)} bytes")
        return WireValue(content, (), len(content))
    total = len(content)
    for i, r in enumerate(segments):
        if len(r.sha256) != 32:
            raise ValueLayoutError(f"segment {i} has a {len(r.sha256)}-byte sha256, expected 32")
        total += r.len
    if total != length:
        raise ValueLayoutError(f"segmented value declares length {length} but its parts sum to {total}")
    return WireValue(content, tuple(segments), length)


@dataclass(frozen=True)
class StartInfo:
    id: bytes = b""
    debug_id: str = ""
    known_entries: int = 0
    key: str = ""
    random_seed: int = 0
    #: Eager state: ``(key, value)`` pairs sent along to answer ``ctx.get`` in place.
    state_map: tuple[tuple[bytes, bytes], ...] = ()
    #: ``True`` = ``state_map`` is only part of the state; absent keys are read lazily.
    partial_state: bool = False
    retry_count: int = 0
    keepalive_ms: int = 0
    replay_only: bool = False
    replay_from: int = 0
    prefix_digest: bytes = b""
    idempotency_key: str | None = None
    #: Wire v6: read URLs of every segment ref in the messages that open the turn.
    segment_urls: tuple[SegmentUrl, ...] = ()


# Wire types of the known fields of StartMessage: a mismatch is a decode error, as in prost.
_START_WIRE = {1: 2, 2: 2, 3: 0, 4: 2, 5: 0, 6: 2, 7: 0, 9: 0, 10: 2, 16: 0, 17: 0, 18: 0, 19: 2, 20: 2}


def decode_start(payload: bytes) -> StartInfo:
    kw: dict[str, object] = {}
    state: list[tuple[bytes, bytes]] = []
    urls: list[SegmentUrl] = []
    for f, wire, v in fields(payload):
        want = _START_WIRE.get(f)
        if want is not None and want != wire:
            raise ValueError(f"Start field {f}: wire type {wire}, expected {want}")
        if f == 1:
            kw["id"] = v
        elif f == 2:
            kw["debug_id"] = _text(v)
        elif f == 3:
            kw["known_entries"] = v
        elif f == 4:
            k, val = b"", b""
            for g, _w, x in fields(_b(v)):
                if g == 1:
                    k = _b(x)
                elif g == 2:
                    val = _b(x)
            state.append((k, val))
        elif f == 5:
            kw["partial_state"] = v != 0
        elif f == 6:
            kw["key"] = _text(v)
        elif f == 7:
            kw["retry_count"] = v
        elif f == 9:
            kw["random_seed"] = v
        elif f == 10:
            kw["idempotency_key"] = _text(v)
        elif f == 16:
            kw["keepalive_ms"] = v
        elif f == 17:
            kw["replay_only"] = v != 0
        elif f == 18:
            kw["replay_from"] = v
        elif f == 19:
            kw["prefix_digest"] = v
        elif f == 20:
            urls.append(decode_segment_url(_b(v)))
    kw["state_map"] = tuple(state)
    kw["segment_urls"] = tuple(urls)
    return StartInfo(**kw)  # type: ignore[arg-type]


def _b(v: int | bytes) -> bytes:
    if not isinstance(v, bytes):
        raise ValueError("expected a length-delimited field")
    return v


def _text(v: int | bytes) -> str:
    return _b(v).decode("utf-8", errors="replace")


def decode_value(payload: bytes) -> bytes:
    """A ``Value`` whose read site only accepts inline (input, signal, call result): the
    runtime only sends refs for state values and run results."""
    v = decode_wire_value(payload)
    if v.segments:
        raise ValueLayoutError("segmented value where only an inline value is allowed")
    return v.content


def write_value(tail: bytes, refs: tuple[SegmentRef, ...] = (), length: int = 0) -> Writer:
    """A ``Value`` going onto the wire: inline when ``refs`` is empty (only ``content``,
    bytes identical to the old layout), otherwise ``segments = refs``, ``content`` = the
    tail and ``len`` = the logical length (wire v6)."""
    w = Writer().bytes(1, tail)
    if not refs:
        return w
    for r in refs:
        w.message(2, Writer().bytes(1, r.sha256).uint64(2, r.len).fixed64(3, r.gen))
    return w.uint64(3, length)


def _split_reuse(value: bytes, reuse: tuple[SegmentRef, ...]) -> Writer:
    if not reuse:
        return write_value(value)
    off = sum(r.len for r in reuse)
    return write_value(value[off:], reuse, len(value))


def decode_input(payload: bytes) -> bytes:
    for f, _w, v in fields(payload):
        if f == 14:
            return decode_value(_b(v))
    return b""


def decode_input_headers(payload: bytes) -> dict[str, str]:
    out: dict[str, str] = {}
    for f, _w, v in fields(payload):
        if f != 1:
            continue
        key = value = ""
        for g, _x, s in fields(_b(v)):
            if g == 1:
                key = _text(s)
            elif g == 2:
                value = _text(s)
        out[key] = value
    return out


@dataclass(frozen=True)
class Result:
    """The result of a completion. ``void`` (key absent) and an empty ``value`` differ."""

    kind: str  # "void" | "value" | "segmented" | "failure" | "state_keys" | "invocation_id"
    value: bytes = b""
    #: Wire v6: the refs of an assembled segmented ``value``.
    refs: tuple[SegmentRef, ...] = ()
    #: Wire v6: a segmented value not assembled yet (``kind == "segmented"``).
    wire: WireValue | None = None
    code: int = 0
    message: str = ""
    keys: tuple[bytes, ...] = ()
    invocation_id: str = ""


VOID = Result("void")


def _decode_failure(payload: bytes) -> Result:
    code, message = 0, ""
    for f, _w, v in fields(payload):
        if f == 1:
            code = int(v)
        elif f == 2:
            message = _text(v)
    return Result("failure", code=code, message=message)


def decode_state_keys(payload: bytes) -> tuple[bytes, ...]:
    return tuple(_b(v) for f, _w, v in fields(payload) if f == 1)


def decode_notification(payload: bytes) -> tuple[int, Result]:
    """Every notification shares one field layout (``NotificationTemplate``, section 7)."""
    completion_id = 0
    result = VOID
    for f, _w, v in fields(payload):
        if f == 1:
            completion_id = int(v)
        elif f == 4:
            result = VOID
        elif f == 5:
            wv = decode_wire_value(_b(v))
            result = Result("segmented", wire=wv) if wv.segments else Result("value", value=wv.content)
        elif f == 6:
            result = _decode_failure(_b(v))
        elif f == 16:
            result = Result("invocation_id", invocation_id=_text(v))
        elif f == 17:
            result = Result("state_keys", keys=decode_state_keys(_b(v)))
    return completion_id, result


@dataclass(frozen=True)
class Signal:
    idx: int | None
    name: str | None
    result: Result


def decode_signal(payload: bytes) -> Signal:
    idx: int | None = None
    name: str | None = None
    result = VOID
    for f, _w, v in fields(payload):
        if f == 2:
            idx = int(v)
        elif f == 3:
            name = _text(v)
        elif f == 4:
            result = VOID
        elif f == 5:
            result = Result("value", value=decode_value(_b(v)))
        elif f == 6:
            result = _decode_failure(_b(v))
    return Signal(idx, name, result)


def decode_command_name(payload: bytes) -> str:
    """``name`` (field 12) of a journaled command; absent is the empty string."""
    for f, wire, v in fields(payload):
        if f == 12 and wire == WIRE_LEN:
            return _text(v)
    return ""


def decode_eager_state_value(payload: bytes) -> WireValue | None:
    """The result embedded in a journaled ``GetEagerState``: ``None`` = key absent."""
    value: WireValue | None = None
    for f, _w, v in fields(payload):
        if f == 13:
            value = None
        elif f == 14:
            value = decode_wire_value(_b(v))
    return value


def decode_eager_state_keys(payload: bytes) -> tuple[bytes, ...]:
    for f, _w, v in fields(payload):
        if f == 14:
            return decode_state_keys(_b(v))
    return ()


def decode_error(payload: bytes) -> tuple[int, str, int]:
    """``(code, message, behavior)`` of an ``ErrorMessage``: for tests and runners."""
    code, message, behavior = 0, "", 0
    for f, _w, v in fields(payload):
        if f == 1:
            code = int(v)
        elif f == 2:
            message = _text(v)
        elif f == 9:
            behavior = int(v)
    return code, message, behavior


# Fields NOT compared on replay: values computed once then recorded (the wake-up time of
# Sleep, the send time of OneWayCall), and values the runtime may have rewritten
# (SetState.value, the embedded result of GetEagerState*).
_REPLAY_IGNORED: dict[int, tuple[int, ...]] = {
    MT.SLEEP: (1,),
    MT.ONE_WAY_CALL: (4,),
    MT.SET_STATE: (3,),
    MT.GET_EAGER_STATE: (13, 14),
    MT.GET_EAGER_STATE_KEYS: (14,),
}


def _fields_of(payload: bytes, ignored: tuple[int, ...]) -> dict[int, list[object]]:
    out: dict[int, list[object]] = {}
    for f, wire, v in fields(payload):
        if f in ignored or wire not in (WIRE_VARINT, WIRE_LEN):
            continue
        out.setdefault(f, []).append((wire, v))
    return out


def command_diff(ty: int, journaled: bytes, current: bytes) -> int | None:
    """The first field that differs between a journaled command and the rebuilt one."""
    ignored = _REPLAY_IGNORED.get(ty, ())
    a = _fields_of(journaled, ignored)
    b = _fields_of(current, ignored)
    for f in sorted(set(a) | set(b)):
        if a.get(f, []) != b.get(f, []):
            return f
    return None


# ─────────────────────────────────────────────────────────── encoding


def _key(key: str) -> bytes:
    return key.encode("utf-8")


def encode_get_state(key: str, completion_id: int, name: str) -> bytes:
    return Writer().bytes(1, _key(key)).uint32(11, completion_id).string(12, name).finish()


def encode_set_state(key: str, value: bytes, name: str, reuse: tuple[SegmentRef, ...] = ()) -> bytes:
    """``reuse`` (wire v6) = refs of a prefix the SDK just read for this same key."""
    return Writer().bytes(1, _key(key)).message(3, _split_reuse(value, reuse)).string(12, name).finish()


def encode_clear_state(key: str, name: str) -> bytes:
    return Writer().bytes(1, _key(key)).string(12, name).finish()


def encode_clear_all_state(name: str) -> bytes:
    return Writer().string(12, name).finish()


def encode_get_state_keys(completion_id: int, name: str) -> bytes:
    return Writer().uint32(11, completion_id).string(12, name).finish()


def encode_get_eager_state(key: str, value: bytes | None, name: str) -> bytes:
    w = Writer().bytes(1, _key(key)).string(12, name)
    if value is None:
        w.message(13, Writer())
    else:
        w.message(14, write_value(value))
    return w.finish()


def encode_get_eager_state_keys(keys: list[bytes], name: str) -> bytes:
    sk = Writer()
    for k in keys:
        sk.bytes_always(1, k)
    return Writer().string(12, name).message(14, sk).finish()


def encode_get_promise(key: str, completion_id: int, name: str) -> bytes:
    return Writer().string(1, key).uint32(11, completion_id).string(12, name).finish()


encode_peek_promise = encode_get_promise


def _failure(code: int, message: str) -> Writer:
    return Writer().uint32(1, code).string(2, message)


def encode_complete_promise(
    key: str, value: bytes | None, failure: tuple[int, str] | None, completion_id: int, name: str
) -> bytes:
    w = Writer().string(1, key)
    if failure is None:
        w.message(2, write_value(value or b""))
    else:
        w.message(3, _failure(*failure))
    return w.uint32(11, completion_id).string(12, name).finish()


def encode_attach_invocation(invocation_id: str, completion_id: int, name: str) -> bytes:
    return Writer().string(1, invocation_id).uint32(11, completion_id).string(12, name).finish()


encode_get_invocation_output = encode_attach_invocation


def encode_sleep(wake_up_time: int, completion_id: int, name: str) -> bytes:
    return Writer().uint64(1, wake_up_time).uint32(11, completion_id).string(12, name).finish()


@dataclass(frozen=True)
class CallFields:
    service: str
    handler: str
    parameter: bytes
    key: str
    id_completion: int
    name: str
    idempotency_key: str | None = None
    #: Flow control (wire v2). ``None`` = inherit the caller's scope, not "no scope".
    scope: str | None = None
    limit_key: str | None = None
    #: ``"spread"`` or ``None`` (default placement, not written).
    placement: str | None = None
    #: The callee's user tags (field 14).
    tags: tuple[tuple[str, str], ...] = field(default_factory=tuple)


def _placement(f: CallFields) -> int:
    return CALL_PLACEMENT_SPREAD if f.placement == "spread" else 0


def _tags(w: Writer, f: CallFields) -> Writer:
    for k, v in f.tags:
        w.bytes(14, Writer().string(1, k).string(2, v).finish())
    return w


def encode_call(f: CallFields, result_completion: int) -> bytes:
    w = Writer().string(1, f.service).string(2, f.handler).bytes(3, f.parameter).string(5, f.key)
    if f.idempotency_key is not None:
        w.string(6, f.idempotency_key)
    if f.scope is not None:
        w.string(7, f.scope)
    if f.limit_key is not None:
        w.string(8, f.limit_key)
    w.uint32(10, f.id_completion).uint32(11, result_completion).string(12, f.name).uint32(13, _placement(f))
    return _tags(w, f).finish()


def encode_one_way_call(f: CallFields, invoke_time: int) -> bytes:
    w = Writer().string(1, f.service).string(2, f.handler).bytes(3, f.parameter)
    w.uint64(4, invoke_time).string(6, f.key)
    if f.idempotency_key is not None:
        w.string(7, f.idempotency_key)
    if f.scope is not None:
        w.string(8, f.scope)
    if f.limit_key is not None:
        w.string(9, f.limit_key)
    w.uint32(10, f.id_completion).string(12, f.name).uint32(13, _placement(f))
    return _tags(w, f).finish()


def encode_run(completion_id: int, name: str) -> bytes:
    return Writer().uint32(11, completion_id).string(12, name).finish()


def encode_propose_run_completion(completion_id: int, value: bytes, reuse: tuple[SegmentRef, ...] = ()) -> bytes:
    if not reuse:
        # Field 14 is a oneof member: written even when the value is empty.
        return Writer().uint32(1, completion_id).bytes_always(14, value).finish()
    # Wire v6: refs of a value already in this invocation's journal + the rest (field 16).
    return Writer().uint32(1, completion_id).message(16, _split_reuse(value, reuse)).finish()


def encode_propose_run_failure(completion_id: int, code: int, message: str) -> bytes:
    return Writer().uint32(1, completion_id).message(15, _failure(code, message)).finish()


def encode_run_completion(completion_id: int, value: bytes) -> bytes:
    return Writer().uint32(1, completion_id).message(5, write_value(value)).finish()


def encode_run_completion_failure(completion_id: int, code: int, message: str) -> bytes:
    return Writer().uint32(1, completion_id).message(6, _failure(code, message)).finish()


def encode_output(value: bytes, name: str, reuse: tuple[SegmentRef, ...] = ()) -> bytes:
    return Writer().string(12, name).message(14, _split_reuse(value, reuse)).finish()


def encode_output_failure(code: int, message: str, name: str) -> bytes:
    return Writer().string(12, name).message(15, _failure(code, message)).finish()


def encode_end() -> bytes:
    return b""


@dataclass(frozen=True)
class SuspensionTree:
    """Completion leaves (field 1), numbered signals (2), named signals (3), child
    nodes (4), combinator (5). Child nodes only occur under a ``first`` root."""

    combinator: str  # "first" | "all"
    completions: tuple[int, ...] = ()
    signals: tuple[int, ...] = ()
    names: tuple[str, ...] = ()
    nested: tuple[SuspensionTree, ...] = ()


def _leaves(w: Writer, n: SuspensionTree) -> Writer:
    w.packed_uint32(1, n.completions).packed_uint32(2, n.signals)
    for x in n.names:
        w.string(3, x)
    return w


def _all_node(n: SuspensionTree) -> Writer:
    return _leaves(Writer(), n).uint32_always(5, COMBINATOR_ALL)


def encode_suspension(t: SuspensionTree) -> bytes:
    """A ``first`` tree writes leaves at the root (no field 5); an ``all`` tree wraps
    its leaves in ONE ``ALL_COMPLETED`` child node."""
    if t.combinator == "first":
        w = _leaves(Writer(), t)
        for n in t.nested:
            w.message(4, _all_node(n) if n.combinator == "all" else _leaves(Writer(), n))
        return w.finish()
    return Writer().message(4, _all_node(t)).finish()


def decode_suspension(payload: bytes) -> SuspensionTree:
    """Inverse of :func:`encode_suspension`, for tests and runners."""
    comps: list[int] = []
    sigs: list[int] = []
    names: list[str] = []
    nested: list[SuspensionTree] = []
    combinator = "first"
    for f, _w, v in fields(payload):
        if f == 1:
            comps += unpack_uint32(v)
        elif f == 2:
            sigs += unpack_uint32(v)
        elif f == 3:
            names.append(_text(v))
        elif f == 4:
            nested.append(decode_suspension(_b(v)))
        elif f == 5:
            combinator = "all" if v == COMBINATOR_ALL else "first"
    return SuspensionTree(combinator, tuple(comps), tuple(sigs), tuple(names), tuple(nested))


def encode_send_signal(target: str, signal: int | str, value: bytes | None, name: str) -> bytes:
    w = Writer().string(1, target)
    if isinstance(signal, int):
        w.uint32_always(2, signal)
    else:
        w.string(3, signal)
    if value is None:
        w.message(4, Writer())
    else:
        w.message(5, write_value(value))
    return w.string(12, name).finish()


def encode_replay_report(
    matched: int, mismatch: bool, expected: str = "", got: str = "", message: str = ""
) -> bytes:
    w = Writer().uint32(1, matched).boolean(2, mismatch)
    return w.string(3, expected).string(4, got).string(5, message).finish()


@dataclass(frozen=True)
class StreamRecord:
    step: int
    attempt: int
    k: int
    payload: bytes


def encode_stream_append(name: str, durable: bool, records: list[StreamRecord], close: bool) -> bytes:
    w = Writer().string(1, name).boolean(2, durable)
    for r in records:
        w.message(3, Writer().uint32(1, r.step).uint32(2, r.attempt).uint32(3, r.k).bytes(4, r.payload))
    return w.boolean(4, close).finish()


def encode_continue_as_new(input: bytes) -> bytes:
    return Writer().bytes_always(1, input).finish()


def encode_error(code: int, message: str, behavior: int) -> bytes:
    return Writer().uint32(1, code).string(2, message).uint32(9, behavior).finish()



def encode_artifact_put(sha256: bytes, content: bytes, content_type: str) -> bytes:
    """``ArtifactPutMessage`` (wire v4, side channel)."""
    return Writer().bytes(1, sha256).bytes(2, content).string(3, content_type).finish()


def tree_satisfied(
    t: SuspensionTree,
    done: Callable[[int], bool],
    has_idx: Callable[[int], bool],
    has_name: Callable[[str], bool],
) -> bool:
    """The runtime's ``suspension_satisfied_by`` rule: ``first`` = any leaf, ``all`` =
    every leaf. ``conformance/suspension-vectors.json`` keeps the two copies in agreement."""
    leaves = (
        [done(c) for c in t.completions]
        + [has_idx(i) for i in t.signals]
        + [has_name(n) for n in t.names]
        + [tree_satisfied(n, done, has_idx, has_name) for n in t.nested]
    )
    return all(leaves) if t.combinator == "all" else any(leaves)
