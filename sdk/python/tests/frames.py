# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""Building runtime-side frames for tests: a fake runtime in a few helpers."""

from __future__ import annotations

import json
from typing import Any

import _path  # noqa: F401  (src/ on sys.path)
from cairnflow._pb import Writer
from cairnflow._wire import (
    MT,
    Frame,
    decode_frames,
    decode_error,
    encode_frame,
    write_value,
)

ID = bytes([0x11] * 24)


def start(
    known: int,
    *,
    key: str = "",
    seed: int = 7,
    retry: int = 0,
    state: dict[str, Any] | None = None,
    partial: bool = False,
    replay_only: bool = False,
    replay_from: int = 0,
    digest: bytes = b"",
    keepalive_ms: int = 0,
) -> bytes:
    w = Writer().bytes(1, ID).uint32(3, known)
    for k, v in (state or {}).items():
        w.message(4, Writer().bytes(1, k.encode()).bytes(2, json.dumps(v).encode()))
    w.boolean(5, partial).string(6, key).uint32(7, retry).uint64(9, seed)
    w.uint32(16, keepalive_ms).boolean(17, replay_only).uint32(18, replay_from).bytes(19, digest)
    return encode_frame(MT.START, 0, w.finish())


def input_(value: Any = None, headers: dict[str, str] | None = None) -> bytes:
    w = Writer()
    for k, v in (headers or {}).items():
        w.message(1, Writer().string(1, k).string(2, v))
    raw = b"" if value is None else json.dumps(value, separators=(",", ":")).encode()
    w.message(14, write_value(raw))
    return encode_frame(MT.INPUT, 0, w.finish())


def completion(ty: int, cid: int, value: Any = None, *, void: bool = False, failure: tuple[int, str] | None = None) -> bytes:
    w = Writer().uint32(1, cid)
    if failure is not None:
        w.message(6, Writer().uint32(1, failure[0]).string(2, failure[1]))
    elif void:
        w.message(4, Writer())
    else:
        w.message(5, write_value(json.dumps(value, separators=(",", ":")).encode()))
    return encode_frame(ty, 0, w.finish())


def signal(*, idx: int | None = None, name: str | None = None, value: Any = None) -> bytes:
    w = Writer()
    if idx is not None:
        w.uint32_always(2, idx)
    if name is not None:
        w.string(3, name)
    if value is None:
        w.message(4, Writer())
    else:
        w.message(5, write_value(json.dumps(value).encode()))
    return encode_frame(MT.SIGNAL, 0, w.finish())


def frames(out: bytes) -> list[Frame]:
    return decode_frames(out)


def types(out: bytes) -> list[int]:
    return [f.type for f in decode_frames(out)]


def error_of(out: bytes) -> tuple[int, str, int]:
    errs = [f for f in decode_frames(out) if f.type == MT.ERROR]
    assert errs, f"no Error frame in {[hex(t) for t in types(out)]}"
    return decode_error(errs[0].payload)
