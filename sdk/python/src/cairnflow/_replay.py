# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""Incremental replay for ``REQUEST_RESPONSE`` (wire v5, section 9.3 of
``docs/protocol-v1.md``): the SDK keeps the journal of a running invocation in memory,
and the runtime sends only what comes after a prefix **proven** by a digest.

Every ambiguity falls back to a full replay; a hint is never used to skip a comparison.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from collections import OrderedDict
from dataclasses import dataclass, field

_DOMAIN = b"cairn-replay-v1"


def chain_root(invocation_id: bytes) -> bytes:
    """``h0 = SHA-256("cairn-replay-v1" || u32be(len(id)) || id)``."""
    return hashlib.sha256(_DOMAIN + len(invocation_id).to_bytes(4, "big") + invocation_id).digest()


def chain_step(h: bytes, frame: bytes) -> bytes:
    """``h(i+1) = SHA-256(h(i) || u32be(len(frame)) || frame)``, over the whole frame."""
    return hashlib.sha256(h + len(frame).to_bytes(4, "big") + frame).digest()


def extend_chain(chain: list[bytes], frames: list[bytes]) -> None:
    for f in frames:
        chain.append(chain_step(chain[-1], f))


@dataclass
class _Item:
    frames: list[bytes]
    chain: list[bytes]
    size: int
    used_at: float = field(default_factory=time.monotonic)


class ReplayCache:
    """Journals of running invocations, by invocation id, bounded by bytes and idle time."""

    def __init__(self, max_bytes: int = 64 << 20, ttl_s: float = 15 * 60) -> None:
        self.max_bytes = max_bytes
        self.ttl_s = ttl_s
        self._items: OrderedDict[str, _Item] = OrderedDict()
        self.total = 0

    def __len__(self) -> int:
        return len(self._items)

    def _live(self, key: str) -> _Item | None:
        it = self._items.get(key)
        if it is None:
            return None
        if time.monotonic() - it.used_at > self.ttl_s:
            self.delete(key)
            return None
        return it

    def take(self, key: str, b: int, digest: bytes) -> tuple[list[bytes], list[bytes]] | None:
        """The first ``b`` entries and their chain, if held and ``h_b == digest``."""
        it = self._live(key)
        if it is None or b <= 0 or len(it.frames) < b or not hmac.compare_digest(it.chain[b], digest):
            return None
        it.used_at = time.monotonic()
        self._items.move_to_end(key)
        return it.frames[:b], it.chain[: b + 1]

    def held(self, key: str, n: int) -> tuple[int, bytes] | None:
        """``(k, h_k)`` of what is still held, ``k = min(held, n)``: the second round of a miss."""
        it = self._live(key)
        if it is None:
            return None
        k = min(len(it.frames), n)
        return (k, it.chain[k]) if k > 0 else None

    def put(self, key: str, frames: list[bytes], chain: list[bytes]) -> bool:
        self.delete(key)
        size = sum(len(f) for f in frames) + len(chain) * 32 + len(key)
        if size > self.max_bytes:
            return False
        self._items[key] = _Item(frames, chain, size)
        self.total += size
        while self.total > self.max_bytes and self._items:
            _k, old = self._items.popitem(last=False)
            self.total -= old.size
        return True

    def delete(self, key: str) -> None:
        it = self._items.pop(key, None)
        if it is not None:
            self.total -= it.size
