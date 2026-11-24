# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""Segmented values (wire v6): a large state value or run result travels as refs to
immutable segments plus an inline tail.

The SDK assembles every segmented value **before** user code runs (segment bytes come
from the read URLs in ``Start.segment_urls`` / ``SegmentUrls``, checked by sha256 and
length, cached by sha), and when user code writes back a value that starts with the
bytes of a value it just read, it sends the refs of that prefix and only the rest.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import threading
import urllib.parse
import urllib.request
from collections import OrderedDict, deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass

from ._wire import MT, Frame, SegmentRef, SegmentUrl, WireValue, decode_eager_state_value, decode_notification, is_notification
from .errors import SegmentedValueError

WIRE_SEGMENTED = 6
DEFAULT_VALUE_CACHE_BYTES = 64 << 20
FETCH_CONCURRENCY = 8
FETCH_TIMEOUT_S = 60.0
#: The runtime never seals a segment larger than this.
MAX_SEGMENT_BYTES = 16 << 20
DEFAULT_MAX_VALUE_BYTES = 64 << 20
DEFAULT_MAX_TURN_VALUE_BYTES = 256 << 20
#: Segment fetches in flight across the whole process: each assembly has its own
#: ``FETCH_CONCURRENCY`` workers, and how many turns assemble at once is the callers' choice.
MAX_PROCESS_FETCHES = 32
#: The runtime never seals a segment below 64 KiB (merges only grow them), so a value of
#: ``max_value_bytes`` holds at most ``max_value_bytes / 64 KiB`` refs; more is a forged value,
#: refused before fetching (each ref costs a request).
MIN_SEGMENT_BYTES = 64 << 10
#: The one text of a segment read that failed after the request went out.
_READ_FAILED = "could not be read, or did not match its ref"


def _refs_for(max_bytes: int) -> int:
    return max(1, -(-max_bytes // MIN_SEGMENT_BYTES))


class _FetchSlots:
    """Process-wide fetch slots, usable from any event loop (serve, ASGI workers and the
    Lambda adapter may each run their own): a counter under a lock and FIFO waiters woken
    on their own loop."""

    def __init__(self, n: int) -> None:
        self._lock = threading.Lock()
        self._free = n
        self._waiters: deque[tuple[asyncio.AbstractEventLoop, asyncio.Future[None]]] = deque()

    async def acquire(self) -> None:
        loop = asyncio.get_running_loop()
        with self._lock:
            if self._free > 0:
                self._free -= 1
                return
            fut: asyncio.Future[None] = loop.create_future()
            entry = (loop, fut)
            self._waiters.append(entry)
        try:
            await fut
        except asyncio.CancelledError:
            with self._lock:
                if entry in self._waiters:
                    self._waiters.remove(entry)
                    raise
            # The slot was already handed over: pass it on.
            if fut.done() and not fut.cancelled():
                self.release()
            raise

    def release(self) -> None:
        with self._lock:
            if not self._waiters:
                self._free += 1
                return
            loop, fut = self._waiters.popleft()
        loop.call_soon_threadsafe(self._grant, fut)

    def _grant(self, fut: asyncio.Future[None]) -> None:
        if fut.cancelled():
            self.release()  # its task gave up waiting: the next one gets the slot
        else:
            fut.set_result(None)


_fetch_slots = _FetchSlots(MAX_PROCESS_FETCHES)

#: ``fetch(url, max_bytes) -> body``: raises on failure, returns at most ``max_bytes + 1``.
Fetch = Callable[[str, int], Awaitable[bytes]]


class SegmentCache:
    """Segment bytes by sha256 (hex), least recently used out first."""

    def __init__(self, max_bytes: int = DEFAULT_VALUE_CACHE_BYTES) -> None:
        if max_bytes < 0:
            raise ValueError(f"value cache bytes {max_bytes} must be non-negative")
        self.max_bytes = max_bytes
        self._items: OrderedDict[str, bytes] = OrderedDict()
        self.total = 0

    def __len__(self) -> int:
        return len(self._items)

    def get(self, sha_hex: str) -> bytes | None:
        b = self._items.get(sha_hex)
        if b is not None:
            self._items.move_to_end(sha_hex)
        return b

    def put(self, sha_hex: str, data: bytes) -> None:
        self.delete(sha_hex)
        if len(data) > self.max_bytes:
            return
        self._items[sha_hex] = data
        self.total += len(data)
        while self.total > self.max_bytes and self._items:
            _k, old = self._items.popitem(last=False)
            self.total -= len(old)

    def delete(self, sha_hex: str) -> None:
        b = self._items.pop(sha_hex, None)
        if b is not None:
            self.total -= len(b)


_shared_cache: SegmentCache | None = None


def default_segment_cache() -> SegmentCache:
    global _shared_cache
    if _shared_cache is None:
        _shared_cache = SegmentCache()
    return _shared_cache


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: object, **kwargs: object) -> None:  # type: ignore[override]
        return None


_opener = urllib.request.build_opener(_NoRedirect)


async def http_fetch(url: str, max_bytes: int) -> bytes:
    """The default segment reader: one GET, no redirects, at most ``max_bytes + 1`` read."""

    def get() -> bytes:
        with _opener.open(url, timeout=FETCH_TIMEOUT_S) as res:  # noqa: S310 - checked origin
            return bytes(res.read(max_bytes + 1))

    return await asyncio.get_running_loop().run_in_executor(None, get)


def _allowed(url: str, origins: Iterable[str] | None) -> bool:
    u = urllib.parse.urlsplit(url)
    if u.scheme not in ("https", "http") or not u.netloc:
        return False
    allowed = list(origins or [])
    if not allowed:
        return True
    origin = f"{u.scheme}://{u.netloc}".lower()
    return any(origin == o.rstrip("/").lower() for o in allowed)


def url_key(sha256: bytes, gen: int) -> str:
    return f"{sha256.hex()}:{gen & ((1 << 64) - 1):x}"


async def _fetch_segment(i: int, ref: SegmentRef, url: str, fetch: Fetch, origins: Iterable[str] | None) -> bytes:
    what = f"segment {i} (sha256 {ref.sha256.hex()})"
    if ref.len < 0 or ref.len > MAX_SEGMENT_BYTES:
        raise SegmentedValueError(f"{what}: the ref declares {ref.len} bytes, over the {MAX_SEGMENT_BYTES}-byte segment limit")
    if not _allowed(url, origins):
        # Never the URL itself in the message: a read URL is a capability.
        raise SegmentedValueError(f"{what}: read URL is not an allowed http(s) origin (segment_origins)")
    # From here on the request goes out: every failure is the same text. It returns to the
    # runtime and, on an endpoint without identity_keys, to whoever sent the turn -- a
    # status, a byte count or a dial error would tell them what is behind the URL.
    await _fetch_slots.acquire()
    try:
        data = await fetch(url, ref.len)
    except Exception:  # noqa: BLE001
        raise SegmentedValueError(f"{what}: {_READ_FAILED}") from None
    finally:
        _fetch_slots.release()
    if len(data) != ref.len or not hmac.compare_digest(hashlib.sha256(data).digest(), ref.sha256):
        raise SegmentedValueError(f"{what}: {_READ_FAILED}")
    return data


@dataclass
class ValueOptions:
    cache: SegmentCache | None = None
    fetch: Fetch | None = None
    #: Allowed origins of read URLs; empty = any http(s) origin.
    origins: tuple[str, ...] = ()
    max_value_bytes: int = DEFAULT_MAX_VALUE_BYTES
    max_turn_value_bytes: int = DEFAULT_MAX_TURN_VALUE_BYTES


@dataclass(frozen=True)
class Assembled:
    bytes: bytes
    refs: tuple[SegmentRef, ...]


def reusable_prefix(known: Assembled, nxt: bytes) -> int:
    """How many leading refs of ``known`` ``nxt`` starts with, byte for byte."""
    k = 0
    off = 0
    for r in known.refs:
        end = off + r.len
        if end > len(nxt) or end > len(known.bytes):
            break
        if nxt[off:end] != known.bytes[off:end]:
            return k
        k += 1
        off = end
    return k


class Segments:
    """The URL table of one stream or turn, and the assembled copy of each frame."""

    def __init__(self, opts: ValueOptions | None = None, wire: int = WIRE_SEGMENTED) -> None:
        """``wire`` is the version the turn runs at, already clamped to the endpoint's cap.
        Below v6 the runtime never sends refs, so URLs are ignored and a ref is refused
        without a fetch: an endpoint that does not serve v6 (no identity_keys, no
        segment_origins) must not fetch URLs a caller put in a turn labelled lower."""
        o = opts or ValueOptions()
        self._wire = wire
        self._urls: dict[str, str] = {}
        self._assembled: dict[Frame, Assembled] = {}
        self._all: list[Assembled] = []
        self._cache = o.cache if o.cache is not None else default_segment_cache()
        self._fetch = o.fetch or http_fetch
        self._origins = o.origins
        self._max_value = o.max_value_bytes
        self._budget = o.max_turn_value_bytes
        self._ref_budget = _refs_for(o.max_turn_value_bytes)

    def add_urls(self, urls: Iterable[SegmentUrl]) -> None:
        if self._wire < WIRE_SEGMENTED:
            return
        for u in urls:
            self._urls[url_key(u.sha256, u.gen)] = u.url

    async def admit(self, frame: Frame) -> None:
        """Assembles the segmented value a frame carries, if any, before user code runs."""
        v: WireValue | None = None
        if frame.type == MT.GET_EAGER_STATE:
            v = decode_eager_state_value(frame.payload)
        elif is_notification(frame.type) and frame.type != MT.SIGNAL:
            r = decode_notification(frame.payload)[1]
            if r.kind == "segmented":
                v = r.wire
        if v is None or not v.segments or frame in self._assembled:
            return
        if self._wire < WIRE_SEGMENTED:
            raise SegmentedValueError(f"a segmented value arrived on wire v{self._wire}; segment refs need wire v{WIRE_SEGMENTED}")
        data = await self._assemble(v)
        a = Assembled(data, v.segments)
        self._assembled[frame] = a
        self._all.append(a)

    async def _assemble(self, v: WireValue) -> bytes:
        if v.len > self._max_value:
            raise SegmentedValueError(
                f"segmented value declares {v.len} bytes, over the {self._max_value}-byte value limit (max_value_bytes)"
            )
        max_refs = _refs_for(self._max_value)
        if len(v.segments) > max_refs:
            raise SegmentedValueError(
                f"segmented value has {len(v.segments)} segment refs, over the {max_refs}-ref limit of max_value_bytes"
            )
        if v.len > self._budget:
            raise SegmentedValueError(
                "segmented values of this turn exceed the per-turn assembled-bytes limit (max_turn_value_bytes)"
            )
        if len(v.segments) > self._ref_budget:
            raise SegmentedValueError(
                "segmented values of this turn exceed the per-turn segment-ref limit of max_turn_value_bytes"
            )
        self._budget -= v.len
        self._ref_budget -= len(v.segments)
        total = len(v.content) + sum(r.len for r in v.segments)
        if total != v.len:
            raise SegmentedValueError(f"segmented value declares length {v.len} but its parts sum to {total}")
        parts: list[bytes | None] = []
        for r in v.segments:
            hit = self._cache.get(r.sha256.hex())
            parts.append(hit if hit is not None and len(hit) == r.len else None)
        misses: dict[str, list[int]] = {}
        for i, p in enumerate(parts):
            if p is None:
                misses.setdefault(v.segments[i].sha256.hex(), []).append(i)
        jobs: list[tuple[list[int], SegmentRef, str]] = []
        for at in misses.values():
            ref = v.segments[at[0]]
            url = self._urls.get(url_key(ref.sha256, ref.gen))
            if url is None:
                raise SegmentedValueError(f"segment {at[0]} (sha256 {ref.sha256.hex()}) is not cached and has no read URL")
            jobs.append((at, ref, url))
        sem = asyncio.Semaphore(FETCH_CONCURRENCY)

        async def job(at: list[int], ref: SegmentRef, url: str) -> None:
            async with sem:
                data = await _fetch_segment(at[0], ref, url, self._fetch, self._origins)
            self._cache.put(ref.sha256.hex(), data)
            for i in at:
                parts[i] = data

        await asyncio.gather(*(job(*j) for j in jobs))
        return b"".join(p or b"" for p in parts) + v.content

    def reuse_for(self, nxt: bytes) -> tuple[SegmentRef, ...]:
        """The longest run of refs of an assembled value that ``nxt`` starts with."""
        best: tuple[SegmentRef, ...] = ()
        best_len = 0
        for a in self._all:
            k = reusable_prefix(a, nxt)
            size = sum(r.len for r in a.refs[:k])
            if size > best_len:
                best, best_len = a.refs[:k], size
        return best

    def of(self, frame: Frame) -> Assembled | None:
        return self._assembled.get(frame)
