# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""JSON on the wire: the same bytes ``JSON.stringify`` produces for the same value.

Values are JSON everywhere (input, output, state, ``ctx.run`` results, call parameters).
Dataclasses and Pydantic models are accepted on the way out; on the way in, a handler
whose input parameter is annotated with a dataclass or a Pydantic model receives one.
"""

from __future__ import annotations

import dataclasses
import json
from typing import Any

from .errors import TerminalError


def _default(value: Any) -> Any:
    dump = getattr(value, "model_dump", None)
    if callable(dump):  # Pydantic v2
        return dump(mode="json")
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    if isinstance(value, (set, frozenset, tuple)):
        return list(value)
    if isinstance(value, (bytes, bytearray)):
        raise TypeError("bytes are not JSON; encode them (base64) or use ctx.artifact")
    raise TypeError(f"{type(value).__name__} is not JSON serializable")


def to_json(value: Any) -> bytes:
    """Compact JSON (``JSON.stringify`` layout). ``NaN``/``Infinity`` are refused: they are
    not JSON, and a runtime or another SDK would fail to read them back."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False, default=_default).encode(
        "utf-8"
    )


def stream_json(value: Any) -> bytes:
    """JSON of a stream record: never raises (a stream must not break the invocation)."""
    try:
        return to_json(value)
    except (TypeError, ValueError) as e:
        return to_json({"unserializable": str(e)})


def from_json(raw: bytes, what: str) -> Any:
    text = raw.decode("utf-8", errors="replace")
    if not text:
        raise TerminalError(f"{what} is empty, not valid JSON")
    return json.loads(text)


def input_json(raw: bytes) -> Any:
    """Handler input: JSON, or the raw string when it is not JSON (ingress accepts any body)."""
    if not raw:
        return None
    text = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except ValueError:
        return text


def coerce(value: Any, annotation: Any) -> Any:
    """Turns parsed JSON into the annotated type when it is a dataclass or a Pydantic model."""
    if annotation is None or value is None or isinstance(annotation, str):
        return value
    validate = getattr(annotation, "model_validate", None)
    if callable(validate):
        return validate(value)
    if dataclasses.is_dataclass(annotation) and isinstance(annotation, type) and isinstance(value, dict):
        return annotation(**value)
    return value
