# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""The handler context: everything user code does durably goes through ``ctx``.

Three rules for user code (protocol-v1 section 8.4) that the SDK cannot fully enforce:

1. Do not use ``random`` directly: use ``ctx.random()``.
2. Do not read the clock directly: wrap it in ``ctx.run``.
3. Do I/O only inside ``ctx.run`` (the handler loop refuses sockets on purpose).
"""

from __future__ import annotations

import asyncio
import contextvars
import hashlib
import inspect
import re
import urllib.request
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Generic, TypeVar

from . import _serde
from ._turn import (
    KEEPALIVE_VERSION,
    STREAMS_VERSION,
    Turn,
    Waiter,
    cancelled_error,
    closure_scope,
    frame_too_large,
)
from ._wire import (
    FLAG_REQUESTED_ACK,
    MAX_FRAME_LEN,
    MT,
    CallFields,
    Frame,
    StartInfo,
    decode_eager_state_keys,
    encode_artifact_put,
    encode_attach_invocation,
    encode_call,
    encode_clear_all_state,
    encode_clear_state,
    encode_complete_promise,
    encode_get_eager_state,
    encode_get_eager_state_keys,
    encode_get_invocation_output,
    encode_get_promise,
    encode_get_state,
    encode_get_state_keys,
    encode_one_way_call,
    encode_peek_promise,
    encode_propose_run_completion,
    encode_propose_run_failure,
    encode_run,
    encode_run_completion,
    encode_run_completion_failure,
    encode_send_signal,
    encode_set_state,
    encode_sleep,
)
from ._values import WIRE_SEGMENTED, Assembled, reusable_prefix
from ._wire import SegmentRef
from .errors import RunFailure, TerminalError

T = TypeVar("T")

#: Wire v2: ``scope``/``limit_key`` on calls.
FLOW_CONTROL_VERSION = 2
MAX_COMPONENT = 36
_COMPONENT_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
MAX_STREAM_NAME = 128
LOG_STREAM = "$log"
TRACE_STREAM = "$trace"
LOG_ORDER = {"debug": 0, "info": 1, "warn": 2, "error": 3}
#: At most this many bytes per artifact (16 MiB minus room for the frame overhead).
ARTIFACT_MAX_BYTES = (16 << 20) - 1024

_MISSING: Any = object()

_span_scope: contextvars.ContextVar[str | None] = contextvars.ContextVar("cairnflow_span", default=None)


def well_formed(s: str) -> str:
    """A string exactly as it travels on the wire: a lone surrogate becomes U+FFFD."""
    return s.encode("utf-16-le", "surrogatepass").decode("utf-16-le", "replace")


def _spawn(turn: Turn, coro: Awaitable[T]) -> asyncio.Task[T]:
    """Runs a continuation on the handler loop whether or not user code awaits it."""
    return turn.loop.create_task(coro)  # type: ignore[arg-type]


def _ms(delay: timedelta | float) -> float:
    """A delay as milliseconds: a ``timedelta``, or a number of **seconds** (as asyncio)."""
    if isinstance(delay, timedelta):
        return delay.total_seconds() * 1000
    return float(delay) * 1000


class _Random:
    """xorshift64*, seeded from ``StartMessage.random_seed``: identical to the TS SDK."""

    _MASK = (1 << 64) - 1

    def __init__(self, seed: int) -> None:
        self._s = seed if seed else 0x9E3779B97F4A7C15

    def __call__(self) -> float:
        s = self._s
        s ^= (s >> 12) & self._MASK
        s = (s ^ ((s << 25) & self._MASK)) & self._MASK
        s ^= (s >> 27) & self._MASK
        self._s = s
        v = (s * 0x2545F4914F6CDD1D) & self._MASK
        return (v >> 11) / 2**53


class DurableFuture(Generic[T]):
    """A durable value to come: the result of ``ctx.call``, ``ctx.sleep``, ``ctx.get``...

    The command is journaled the moment the future is **created**; waiting only happens on
    ``await`` (or in ``ctx.all`` / ``ctx.race``). Separating the two is what makes
    combinators possible.
    """

    __slots__ = ("_turn", "ids", "signals", "_take")

    def __init__(
        self, turn: Turn, ids: list[int], take: Callable[[], T], signals: tuple[int | str, ...] = ()
    ) -> None:
        self._turn = turn
        self.ids = ids
        self.signals = signals
        self._take = take

    def keys(self) -> tuple[str, ...]:
        return tuple(f"c{i}" for i in self.ids) + tuple(
            f"i{s}" if isinstance(s, int) else f"n{s}" for s in self.signals
        )

    def ready(self) -> bool:
        return all(self._turn.is_delivered(k) for k in self.keys())

    def resolve(self) -> T:
        return self._take()

    def order(self) -> int:
        orders = [self._turn.order_of(i) for i in self.ids] + [self._turn.order_of_signal(s) for s in self.signals]
        return min(orders) if orders else -1

    def __await__(self) -> Any:
        return self._wait().__await__()

    async def _wait(self) -> T:
        await self._turn.when(self.keys())
        return self._take()


class DurablePromise(Generic[T]):
    """A workflow's durable promise; see :meth:`Context.promise`."""

    def __init__(self, turn: Turn, name: str) -> None:
        self._turn = turn
        self._name = name

    def get(self) -> DurableFuture[T]:
        """Waits until the promise is completed."""
        turn, name = self._turn, self._name
        cid = turn.alloc()
        turn.command(MT.GET_PROMISE, lambda: encode_get_promise(name, cid, f"promise/{name}"))

        def take() -> Any:
            r = turn.demand(cid)
            if r.kind == "value":
                return _serde.from_json(r.value, f"promise {name!r}")
            if r.kind == "failure":
                raise TerminalError(r.message, r.code)
            raise TerminalError(f"result {r.kind} is not valid for GetPromise")

        return DurableFuture(turn, [cid], take)

    def peek(self) -> DurableFuture[T | None]:
        """Looks without waiting: ``None`` if not completed yet."""
        turn, name = self._turn, self._name
        cid = turn.alloc()
        turn.command(MT.PEEK_PROMISE, lambda: encode_peek_promise(name, cid, f"promise/{name}"))

        def take() -> Any:
            r = turn.demand(cid)
            if r.kind == "void":
                return None
            if r.kind == "value":
                return _serde.from_json(r.value, f"promise {name!r}")
            if r.kind == "failure":
                raise TerminalError(r.message, r.code)
            raise TerminalError(f"result {r.kind} is not valid for PeekPromise")

        return DurableFuture(turn, [cid], take)

    def resolve(self, value: T) -> asyncio.Task[None]:
        """Completes with a value. Raises ``TerminalError`` if it already has a result."""
        raw = _serde.to_json(value)
        return self._complete(lambda cid: encode_complete_promise(self._name, raw, None, cid, f"promise/{self._name}"))

    def reject(self, message: str, code: int = 500) -> asyncio.Task[None]:
        """Completes with a failure. Raises ``TerminalError`` if it already has a result."""
        return self._complete(
            lambda cid: encode_complete_promise(self._name, None, (code, message), cid, f"promise/{self._name}")
        )

    def _complete(self, build: Callable[[int], bytes]) -> asyncio.Task[None]:
        turn = self._turn
        cid = turn.alloc()
        turn.command(MT.COMPLETE_PROMISE, lambda: build(cid))
        waited = turn.when((f"c{cid}",))

        async def rest() -> None:
            await waited
            r = turn.demand(cid)
            if r.kind == "failure":
                raise TerminalError(r.message, r.code)

        return _spawn(turn, rest())


class Stream:
    """A stream of this invocation (``ctx.stream``): ``append`` never waits nor raises."""

    def __init__(self, turn: Turn, name: str, durable: bool) -> None:
        self._turn = turn
        self._name = name
        self._durable = durable

    def append(self, record: Any) -> None:
        payload = _serde.stream_json(record)
        self._turn.append_with(self._name, self._durable, lambda *_: payload)

    def close(self) -> None:
        self._turn.append_with(self._name, self._durable, None)


class Log:
    """The invocation's log: the durable stream ``$log``. Levels below the minimum
    (default ``info``; the handler's ``log_level``; the invocation's
    ``cairnflow-log-level`` header) are dropped at once."""

    def __init__(self, turn: Turn) -> None:
        self._turn = turn

    def _emit(self, level: str, msg: str, attrs: dict[str, Any] | None) -> None:
        turn = self._turn
        if LOG_ORDER[level] < LOG_ORDER.get(turn.min_log_level, 1):
            return

        def build(*_: int) -> bytes:
            rec: dict[str, Any] = {"ts": turn.now(), "level": level, "msg": msg}
            if attrs is not None:
                rec["attrs"] = attrs
            return _serde.stream_json(rec)

        turn.append_with(LOG_STREAM, True, build)

    def debug(self, msg: str, attrs: dict[str, Any] | None = None) -> None:
        self._emit("debug", msg, attrs)

    def info(self, msg: str, attrs: dict[str, Any] | None = None) -> None:
        self._emit("info", msg, attrs)

    def warn(self, msg: str, attrs: dict[str, Any] | None = None) -> None:
        self._emit("warn", msg, attrs)

    def error(self, msg: str, attrs: dict[str, Any] | None = None) -> None:
        self._emit("error", msg, attrs)


def _span_id(invocation: str, step: int, k: int) -> str:
    return hashlib.sha256(f"{invocation}:{step}:{k}".encode()).hexdigest()[:16]


class Span:
    def __init__(self) -> None:
        self.attrs: dict[str, Any] = {}

    def set(self, attrs: dict[str, Any]) -> None:
        """Adds attributes; they go with the ``end`` record."""
        self.attrs.update(attrs)


class _SpanScope:
    def __init__(self, trace: Trace, name: str, attrs: dict[str, Any] | None) -> None:
        self._trace = trace
        self._name = name
        self._attrs = attrs
        self._span = Span()
        self._parent: str | None = None
        self._id: str | None = None
        self._token: contextvars.Token[str | None] | None = None

    async def __aenter__(self) -> Span:
        turn, invocation = self._trace._turn, self._trace._invocation
        self._parent = _span_scope.get()
        parent, name, attrs = self._parent, self._name, self._attrs

        def build(step: int, _attempt: int, k: int) -> bytes:
            rec: dict[str, Any] = {
                "kind": "start",
                "span_id": _span_id(invocation, step, k),
                "parent_id": parent,
                "name": name,
                "ts": turn.now(),
            }
            if attrs is not None:
                rec["attrs"] = attrs
            return _serde.stream_json(rec)

        opened = turn.append_with(TRACE_STREAM, True, build)
        if opened is not None:
            self._id = _span_id(invocation, opened[0], opened[2])
        self._token = _span_scope.set(self._id if self._id is not None else parent)
        return self._span

    async def __aexit__(self, ty: Any, exc: BaseException | None, tb: Any) -> None:
        from .errors import JournalMismatch, Suspend

        if self._token is not None:
            _span_scope.reset(self._token)
        if self._id is None:
            return
        # Suspending or a journal mismatch is the turn's control flow, not a span error:
        # leave it open; the next turn replays up to here and closes it with the same id.
        if exc is not None and (isinstance(exc, (Suspend, JournalMismatch, asyncio.CancelledError))):
            return
        turn, span_id, parent, name, extra = self._trace._turn, self._id, self._parent, self._name, self._span.attrs

        def build(*_: int) -> bytes:
            rec: dict[str, Any] = {
                "kind": "end",
                "span_id": span_id,
                "parent_id": parent,
                "name": name,
                "ts": turn.now(),
                "status": "ok" if exc is None else "error",
            }
            if extra:
                rec["attrs"] = extra
            if exc is not None:
                rec["error"] = str(exc)
            return _serde.stream_json(rec)

        turn.append_with(TRACE_STREAM, True, build)


class Trace:
    """The invocation's trace (durable stream ``$trace``)::

        async with ctx.trace.span("plan", {"model": "m"}) as s:
            s.set({"input_tokens": 1200})
            await ctx.run("llm", call_model)
    """

    def __init__(self, turn: Turn, invocation: str) -> None:
        self._turn = turn
        self._invocation = invocation

    def span(self, name: str, attrs: dict[str, Any] | None = None) -> _SpanScope:
        return _SpanScope(self, name, attrs)


@dataclass(frozen=True)
class ArtifactRef:
    sha256: str
    size: int
    content_type: str


#: A sha256 in hex, as ``put`` returns it.
_SHA256_HEX = re.compile(r"[0-9a-f]{64}")
_artifact_cache: dict[str, bytes] = {}
_artifact_cache_order: list[str] = []
_ARTIFACT_CACHE_BYTES = 64 << 20
_artifact_source: dict[str, Any] = {}


def configure_artifacts(base_url: str, headers: dict[str, str] | None = None) -> None:
    """Where ``ctx.artifact.get`` reads artifacts that are not in this process's memory."""
    _artifact_source["base_url"] = base_url.rstrip("/")
    _artifact_source["headers"] = dict(headers or {})


def _remember_artifact(sha: str, content: bytes) -> None:
    if sha in _artifact_cache:
        return
    _artifact_cache[sha] = content
    _artifact_cache_order.append(sha)
    total = sum(len(v) for v in _artifact_cache.values())
    while total > _ARTIFACT_CACHE_BYTES and _artifact_cache_order:
        old = _artifact_cache_order.pop(0)
        total -= len(_artifact_cache.pop(old, b""))


class Artifacts:
    """Large bytes kept out of the journal (wire v4). ``put`` returns a small ref that a
    ``ctx.run`` can return; ``get`` reads from memory, else from :func:`configure_artifacts`."""

    def __init__(self, turn: Turn, invocation: str) -> None:
        self._turn = turn
        self._invocation = invocation

    def put(self, content: bytes | str, content_type: str = "application/octet-stream") -> ArtifactRef:
        data = content.encode("utf-8") if isinstance(content, str) else bytes(content)
        if len(data) > ARTIFACT_MAX_BYTES:
            raise ValueError(f"artifact of {len(data)} bytes exceeds the limit of {ARTIFACT_MAX_BYTES}")
        digest = hashlib.sha256(data).digest()
        if not self._turn_put(digest, data, content_type):
            raise RuntimeError(
                f"ctx.artifact needs a runtime speaking wire v{STREAMS_VERSION}; "
                f"the peer speaks v{self._turn.wire_version}"
            )
        sha = digest.hex()
        _remember_artifact(sha, data)
        return ArtifactRef(sha, len(data), content_type)

    def _turn_put(self, sha: bytes, data: bytes, content_type: str) -> bool:
        turn = self._turn
        if turn.wire_version < STREAMS_VERSION:
            return False
        if turn.replaying():
            return True
        if turn.dead() and closure_scope.get() is None:
            return True
        turn.flush_streams()
        frame = Frame(MT.ARTIFACT_PUT, 0, encode_artifact_put(sha, data, content_type))
        if turn.wire is None:
            turn.out.append(frame)
            return True
        head = turn.out[turn.flushed :]
        turn.flushed = len(turn.out)
        turn.wire([*head, frame])
        return True

    async def get(self, ref: ArtifactRef | str) -> bytes:
        sha = ref if isinstance(ref, str) else ref.sha256
        # The sha goes into the source's URL path, with the tenant's key: only a sha256.
        if not isinstance(sha, str) or _SHA256_HEX.fullmatch(sha) is None:
            raise ValueError("artifact ref is not a sha256 (64 lowercase hex digits)")
        size = None if isinstance(ref, str) else ref.size
        if size is not None and not (isinstance(size, int) and 0 <= size <= ARTIFACT_MAX_BYTES):
            raise ValueError(f"artifact {sha}: the ref's size {size} is outside [0, {ARTIFACT_MAX_BYTES}]")
        hit = _artifact_cache.get(sha)
        if hit is not None and (size is None or len(hit) == size):
            return hit
        if "base_url" not in _artifact_source:
            raise RuntimeError(
                f"artifact {sha} is not in this process's memory; call configure_artifacts(base_url) "
                "to read it from cairnflow"
            )
        url = f"{_artifact_source['base_url']}/invocations/{self._invocation}/artifacts/{sha}"
        headers = _artifact_source["headers"]

        def fetch() -> bytes:
            from ._values import _opener

            req = urllib.request.Request(url, headers=headers)
            # No redirects: the configured headers (an API key) never reach another origin.
            # At most the ref's size (or the artifact limit) plus one byte is read.
            with _opener.open(req, timeout=60) as res:  # noqa: S310 - operator-configured URL
                return bytes(res.read(limit + 1))

        limit = ARTIFACT_MAX_BYTES if size is None else size

        async def download() -> bytes:
            body = await asyncio.get_running_loop().run_in_executor(None, fetch)
            if len(body) > limit:
                raise ValueError(f"artifact {sha}: the download is larger than {limit} bytes")
            if size is not None and len(body) != size:
                raise ValueError(f"artifact {sha}: got {len(body)} bytes, the ref says {size}")
            if hashlib.sha256(body).hexdigest() != sha:
                raise ValueError(f"artifact {sha}: the download does not match its sha256")
            _remember_artifact(sha, body)
            return body

        turn = self._turn
        task = turn.outer.create_task(download())
        return await turn.busy_with(task)


def _check_component(what: str, value: str) -> None:
    if not value:
        raise TypeError(f"{what} is empty; omit the argument instead of passing an empty string")
    if len(value) > MAX_COMPONENT:
        raise TypeError(f"{what} is {len(value)} characters long; the limit is {MAX_COMPONENT}")
    if not _COMPONENT_RE.match(value):
        raise TypeError(f"{what} {value!r} has invalid characters; only [a-zA-Z0-9_.-] is allowed")


class Context:
    """The context of one handler run. See the module doc for the three rules."""

    def __init__(self, turn: Turn, start: StartInfo, wire_version: int) -> None:
        self._turn = turn
        self._start = start
        self._wire_version = wire_version
        self.random: Callable[[], float] = _Random(start.random_seed)
        # An empty map + partial=false cannot be told apart from a v1 runtime on the wire,
        # so the snapshot is only trusted when it is not empty.
        self._state: dict[str, bytes] | None
        if start.state_map or start.partial_state:
            self._state = {k.decode("utf-8", "replace"): v for k, v in start.state_map}
            self._partial = start.partial_state
        else:
            self._state = None
            self._partial = False
        # Wire v6: the most recently read segmented value of each key in this turn, so
        # `set` can reuse the refs of a matching prefix; reads only count if no write of
        # that key came in between (``_writes``/``_clears``).
        self._known: dict[str, Assembled] = {}
        self._writes: dict[str, int] = {}
        self._clears = 0

    def _stamp(self, key: str) -> tuple[int, int]:
        return self._writes.get(key, 0), self._clears

    def _wrote(self, key: str) -> None:
        self._writes[key] = self._writes.get(key, 0) + 1

    def _remember(self, key: str, stamp: tuple[int, int], value: bytes | None, refs: tuple[SegmentRef, ...]) -> None:
        if stamp != self._stamp(key):
            return
        if value is not None and refs:
            self._known[key] = Assembled(value, refs)
        else:
            self._known.pop(key, None)

    # ── identity ─────────────────────────────────────────────────────────

    @property
    def key(self) -> str:
        """Key of the virtual object or workflow; empty for a plain service."""
        return self._start.key

    @property
    def invocation_id(self) -> str:
        """The id the runtime accepts everywhere over HTTP (hex of ``Start.id``)."""
        return self._start.id.hex()

    @property
    def debug_id(self) -> str:
        """A label for people reading logs; not an id."""
        return self._start.debug_id

    @property
    def attempt(self) -> int:
        """Retries **since the journal last grew** (0, 1, 0, 1 for a step failing once)."""
        return self._start.retry_count

    @property
    def schedule(self) -> tuple[str, int] | None:
        """``(name, time ms)`` when the run was fired by a server-side schedule."""
        return self._turn.schedule

    @property
    def idempotency_key(self) -> str | None:
        return self._start.idempotency_key

    # ── state ────────────────────────────────────────────────────────────

    def get(self, key: str) -> DurableFuture[Any]:
        """Reads state; ``None`` means the key does not exist.

        With an eager snapshot it answers in place (journaling ``GetEagerState``); without
        one it emits ``GetState`` and waits. The journal decides the path, not current
        capabilities, so an invocation that lived through an upgrade still replays.
        """
        key = well_formed(key)
        turn = self._turn
        journaled = turn.peek()
        eager = journaled == MT.GET_EAGER_STATE or (
            journaled is None and self._state is not None and (not self._partial or key in self._state)
        )
        if eager:
            snapshot = None if self._state is None else self._state.get(key)
            frame = turn.command(MT.GET_EAGER_STATE, lambda: encode_get_eager_state(key, snapshot, key))
            # The refs of a journaled GetEagerState are NOT remembered: the runtime sealed
            # that entry itself, so its refs are held by the invocation, not the state row.
            if frame is not None:
                raw = turn.eager_value(frame)[0]
                self._remember(key, self._stamp(key), raw, ())
            else:
                raw = snapshot
            return DurableFuture(turn, [], lambda: None if raw is None else _serde.from_json(raw, f"state {key!r}"))
        cid = turn.alloc()
        turn.command(MT.GET_STATE, lambda: encode_get_state(key, cid, key))
        stamp = self._stamp(key)

        def take() -> Any:
            r = turn.demand(cid)
            if r.kind == "void":
                self._remember(key, stamp, None, ())
                return None
            if r.kind == "value":
                self._remember(key, stamp, r.value, r.refs)
                return _serde.from_json(r.value, f"state {key!r}")
            if r.kind == "failure":
                raise TerminalError(r.message, r.code)
            raise TerminalError(f"result {r.kind} is not valid for GetState")

        return DurableFuture(turn, [cid], take)

    def set(self, key: str, value: Any) -> None:
        """Writes state. Does not wait (this command has no completion)."""
        key = well_formed(key)
        raw = _serde.to_json(value)
        known = self._known.get(key) if self._wire_version >= WIRE_SEGMENTED else None
        reuse: list[tuple[SegmentRef, ...]] = [()]

        def build() -> bytes:
            if known is not None:
                reuse[0] = known.refs[: reusable_prefix(known, raw)]
            return encode_set_state(key, raw, key, reuse[0])

        journaled = self._turn.command(MT.SET_STATE, build)
        self._wrote(key)
        # Replayed: which refs the earlier run sent is unknown, so assume none. Newly
        # emitted: the row after commit starts with exactly the refs just sent.
        if journaled is None and reuse[0]:
            self._known[key] = Assembled(raw, reuse[0])
        else:
            self._known.pop(key, None)
        if self._state is not None:
            self._state[key] = raw

    def clear(self, key: str) -> None:
        key = well_formed(key)
        self._turn.command(MT.CLEAR_STATE, lambda: encode_clear_state(key, key))
        self._wrote(key)
        self._known.pop(key, None)
        if self._state is not None:
            self._state.pop(key, None)

    def clear_all(self) -> None:
        """Deletes ALL of the object's state."""
        self._turn.command(MT.CLEAR_ALL_STATE, lambda: encode_clear_all_state("clearAll"))
        self._clears += 1
        self._known.clear()
        # After deleting everything, the snapshot is complete and empty.
        self._state = {}
        self._partial = False

    def state_keys(self) -> DurableFuture[list[str]]:
        """The object's state keys (sorted when answered from a complete snapshot)."""
        turn = self._turn
        journaled = turn.peek()
        eager = journaled == MT.GET_EAGER_STATE_KEYS or (
            journaled is None and self._state is not None and not self._partial
        )
        if eager:
            keys = sorted(self._state or {}, key=lambda k: list(k.encode("utf-16-be")))
            frame = turn.command(
                MT.GET_EAGER_STATE_KEYS, lambda: encode_get_eager_state_keys([k.encode() for k in keys], "getKeys")
            )
            got = [k.decode("utf-8", "replace") for k in decode_eager_state_keys(frame.payload)] if frame else keys
            return DurableFuture(turn, [], lambda: got)
        cid = turn.alloc()
        turn.command(MT.GET_STATE_KEYS, lambda: encode_get_state_keys(cid, "getKeys"))

        def take() -> list[str]:
            r = turn.demand(cid)
            if r.kind == "state_keys":
                return [k.decode("utf-8", "replace") for k in r.keys]
            if r.kind == "failure":
                raise TerminalError(r.message, r.code)
            raise TerminalError(f"result {r.kind} is not valid for GetStateKeys")

        return DurableFuture(turn, [cid], take)

    # ── timers ───────────────────────────────────────────────────────────

    def sleep(self, delay: timedelta | float, name: str = "sleep") -> DurableFuture[None]:
        """Sleeps durably. ``delay`` is a ``timedelta`` or a number of **seconds**.

        The wake-up time is absolute, computed once and journaled (section 8.4)."""
        ms = _ms(delay)
        turn = self._turn
        cid = turn.alloc()
        turn.command(MT.SLEEP, lambda: encode_sleep(max(0, int(turn.now() + ms)), cid, name))

        def take() -> None:
            r = turn.demand(cid)
            if r.kind == "failure":
                raise TerminalError(r.message, r.code)

        return DurableFuture(turn, [cid], take)

    # ── calls ────────────────────────────────────────────────────────────

    def _fields(
        self,
        service: str,
        handler: str,
        input: Any,
        id_completion: int,
        key: str | None,
        idempotency_key: str | None,
        name: str | None,
        scope: str | None,
        limit_key: str | None,
        placement: str | None,
        tags: dict[str, str] | None,
    ) -> CallFields:
        return CallFields(
            service=service,
            handler=handler,
            parameter=_serde.to_json(input),
            key=key or "",
            id_completion=id_completion,
            name=name if name is not None else f"{service}/{handler}",
            idempotency_key=idempotency_key,
            scope=scope,
            limit_key=limit_key,
            placement=placement,
            tags=tuple((tags or {}).items()),
        )

    def call(
        self,
        service: str,
        handler: str,
        input: Any = None,
        *,
        key: str | None = None,
        idempotency_key: str | None = None,
        name: str | None = None,
        scope: str | None = None,
        limit_key: str | None = None,
        placement: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> DurableFuture[Any]:
        """Calls another handler; ``await`` waits for its result. A callee failure
        surfaces as ``TerminalError``."""
        _check_placement(placement)
        turn = self._turn
        id_completion = turn.alloc()
        result_completion = turn.alloc()
        f = self._fields(
            service, handler, input, id_completion, key, idempotency_key, name, scope, limit_key, placement, tags
        )
        turn.command(MT.CALL, lambda: encode_call(f, result_completion))

        def take() -> Any:
            r = turn.demand(result_completion)
            if r.kind == "value":
                return _serde.from_json(r.value, f"result of {service}/{handler}")
            if r.kind == "failure":
                raise TerminalError(r.message, r.code)
            if r.kind == "void":
                return None
            raise TerminalError(f"result {r.kind} is not valid for Call")

        return DurableFuture(turn, [result_completion], take)

    def send(
        self,
        service: str,
        handler: str,
        input: Any = None,
        *,
        key: str | None = None,
        idempotency_key: str | None = None,
        name: str | None = None,
        scope: str | None = None,
        limit_key: str | None = None,
        placement: str | None = None,
        tags: dict[str, str] | None = None,
        delay: timedelta | float | None = None,
    ) -> None:
        """Calls another handler without waiting for its result; ``delay`` (seconds or a
        ``timedelta``) schedules it later. The absolute time is computed once."""
        _check_placement(placement)
        turn = self._turn
        id_completion = turn.alloc()
        f = self._fields(
            service, handler, input, id_completion, key, idempotency_key, name, scope, limit_key, placement, tags
        )
        ms = 0.0 if delay is None else _ms(delay)

        def build() -> bytes:
            at = turn.now() + _ceil(ms) if ms > 0 else 0
            return encode_one_way_call(f, at)

        turn.command(MT.ONE_WAY_CALL, build)

    def scoped(self, scope: str, limit_key: str | None = None) -> ScopedContext:
        """A view whose every ``call``/``send`` carries the scope (and limit key).

        The scope decides the callee's range (ahead of ``key`` and ``idempotency_key``)
        and is inherited by its whole subtree. Needs wire v2."""
        if self._wire_version < FLOW_CONTROL_VERSION:
            raise TerminalError(
                f"the runtime speaks wire version {self._wire_version}; ctx.scoped(...) needs "
                f"v{FLOW_CONTROL_VERSION}. An older runtime silently IGNORES the scope, so the call would run "
                "with no limit at all; upgrade the runtime, or remove ctx.scoped from this deployment."
            )
        _check_component("scope", scope)
        if limit_key is not None:
            parts = limit_key.split("/")
            if len(parts) > 2:
                raise TypeError(
                    f"limit key {limit_key!r} has {len(parts)} levels; only 1 or 2 are allowed (l1 or l1/l2)"
                )
            for p in parts:
                _check_component("limit key", p)
        return ScopedContext(self, scope, limit_key)

    # ── signals and promises ─────────────────────────────────────────────

    def send_signal(self, target_invocation_id: str, signal: int | str, value: Any = _MISSING) -> None:
        """Sends a signal to another invocation. One-way; a no-op if the target has ended."""
        raw = None if value is _MISSING else _serde.to_json(value)
        self._turn.command(
            MT.SEND_SIGNAL, lambda: encode_send_signal(target_invocation_id, signal, raw, f"signal/{signal}")
        )

    def signal(self, signal: int | str) -> DurableFuture[Any]:
        """A signal by identity, as a future (composable in ``race``/``all``).

        A signal that has arrived stays satisfied: a later ``ctx.signal`` with the same
        identity resolves at once with the old value. Use a new name to wait again."""
        if isinstance(signal, str):
            signal = well_formed(signal)
        turn = self._turn
        sig = signal

        def take() -> Any:
            seen = turn.signal_seen(sig)
            if seen is None:
                raise RuntimeError(f"internal: signal {sig} read before it was delivered")
            if seen.result.kind == "value":
                return _serde.from_json(seen.result.value, f"signal {sig}")
            if seen.result.kind == "failure":
                raise TerminalError(seen.result.message, seen.result.code)
            return None

        return DurableFuture(turn, [], take, (sig,))

    def awakeable(self) -> tuple[str, DurableFuture[Any]]:
        """A wait point an external system completes by id
        (``POST /awakeables/{id}/resolve`` or ``/reject``). Returns ``(id, future)``."""
        idx = self._turn.alloc_awakeable()
        return f"awk_{self.invocation_id}{idx:08x}", self.signal(idx)

    def promise(self, name: str) -> DurablePromise[Any]:
        """A durable promise by name, belonging to this object or workflow."""
        return DurablePromise(self._turn, name)

    def attach(self, invocation_id: str, name: str = "attach") -> DurableFuture[Any]:
        """Waits for the result of another invocation by id."""
        turn = self._turn
        cid = turn.alloc()
        turn.command(MT.ATTACH_INVOCATION, lambda: encode_attach_invocation(invocation_id, cid, name))

        def take() -> Any:
            r = turn.demand(cid)
            if r.kind == "value":
                return _serde.from_json(r.value, f"result of {invocation_id}")
            if r.kind == "failure":
                raise TerminalError(r.message, r.code)
            raise TerminalError(f"result {r.kind} is not valid for Attach")

        return DurableFuture(turn, [cid], take)

    def get_output(self, invocation_id: str, name: str = "getOutput") -> DurableFuture[Any]:
        """Peeks at another invocation's result without waiting: ``None`` if unfinished."""
        turn = self._turn
        cid = turn.alloc()
        turn.command(MT.GET_INVOCATION_OUTPUT, lambda: encode_get_invocation_output(invocation_id, cid, name))

        def take() -> Any:
            r = turn.demand(cid)
            if r.kind == "void":
                return None
            if r.kind == "value":
                return _serde.from_json(r.value, f"result of {invocation_id}")
            if r.kind == "failure":
                raise TerminalError(r.message, r.code)
            raise TerminalError(f"result {r.kind} is not valid for GetInvocationOutput")

        return DurableFuture(turn, [cid], take)

    # ── combinators ──────────────────────────────────────────────────────

    def all(self, futures: Iterable[DurableFuture[Any]]) -> asyncio.Future[list[Any]]:
        """Waits for all, suspending durably with an ``ALL_COMPLETED`` tree. A failure
        surfaces at once; "first" failure is in journal order, so replay agrees."""
        futs = list(futures)
        turn = self._turn
        out: asyncio.Future[list[Any]] = turn.loop.create_future()
        keys = tuple(k for f in futs for k in f.keys())
        if turn.cancelled and any(not turn.present(k) for k in keys):
            out.set_exception(cancelled_error())
            return out
        got: list[tuple[bool, Any] | None] = [None] * len(futs)

        def check() -> bool:
            for i, f in enumerate(futs):
                if got[i] is not None or not f.ready():
                    continue
                try:
                    got[i] = (True, f.resolve())
                except Exception as e:
                    got[i] = (False, e)
            failed = -1
            for i, f in enumerate(futs):
                g = got[i]
                if g is None or g[0]:
                    continue
                if failed < 0 or f.order() < futs[failed].order():
                    failed = i
            if failed >= 0:
                if not out.done():
                    out.set_exception(got[failed][1])  # type: ignore[index]
                return True
            if any(g is None for g in got):
                return False
            if not out.done():
                out.set_result([g[1] for g in got])  # type: ignore[index]
            return True

        turn.wait(Waiter(keys, "all", check))
        return out

    def race(self, futures: Iterable[DurableFuture[Any]]) -> asyncio.Future[Any]:
        """Waits for whichever finishes first, decided by the order in the journal (so
        replay picks the same winner)."""
        futs = list(futures)
        turn = self._turn
        out: asyncio.Future[Any] = turn.loop.create_future()
        keys = tuple(k for f in futs for k in f.keys())
        if turn.cancelled and not any(all(turn.present(k) for k in f.keys()) for f in futs):
            out.set_exception(cancelled_error())
            return out

        def check() -> bool:
            winner: DurableFuture[Any] | None = None
            for f in futs:
                if f.ready() and (winner is None or f.order() < winner.order()):
                    winner = f
            if winner is None:
                return False
            if not out.done():
                try:
                    out.set_result(winner.resolve())
                except Exception as e:
                    out.set_exception(e)
            return True

        turn.wait(Waiter(keys, "first", check))
        return out

    # ── side effects ─────────────────────────────────────────────────────

    def run(
        self,
        name: str,
        fn: Callable[..., Any],
        *args: Any,
        cancellable: bool = False,
        **kwargs: Any,
    ) -> asyncio.Task[Any]:
        """Runs a side effect **exactly once** per durable result: ``fn(*args, **kwargs)``,
        sync or async, on the server's real event loop.

        Proposes the result, then waits for it to be durable; the result reaches user code
        in the next turn, in journal order. Dying while ``fn`` runs reruns it, so keep it
        idempotent. ``RunFailure``/``TerminalError`` raised by ``fn`` is a durable failed
        result; any other exception is retried (the closure reruns). ``cancellable=True``
        makes it a cancellation point (409) once the invocation was cancelled.
        """
        name = well_formed(name)
        turn = self._turn
        cid = turn.alloc()
        step = turn.position
        turn.command(MT.RUN, lambda: encode_run(cid, name), name)

        if turn.present(f"c{cid}"):
            waited = turn.when((f"c{cid}",))

            async def journaled() -> Any:
                await waited
                r = turn.known(cid)
                assert r is not None
                if r.kind == "value":
                    return _serde.from_json(r.value, f"result of ctx.run {name!r}")
                if r.kind == "failure":
                    raise TerminalError(r.message, r.code)
                if r.kind == "void":
                    return None
                raise TerminalError(f"result {r.kind} is not valid for Run")

            return _spawn(turn, journaled())

        if cancellable and turn.cancelled:
            fut: asyncio.Future[Any] = turn.loop.create_future()
            fut.set_exception(cancelled_error())

            async def refused() -> Any:
                return await fut

            return _spawn(turn, refused())

        task = turn.outer.create_task(self._execute(cid, step, fn, args, kwargs, cancellable))
        bridge = turn.track(task)

        async def after() -> Any:
            await bridge
            return await turn.await_proposal(cid)

        return _spawn(turn, after())

    async def _execute(
        self,
        cid: int,
        step: int,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        cancellable: bool,
    ) -> None:
        turn = self._turn
        closure_scope.set(step)
        me = asyncio.current_task()
        if cancellable and me is not None:
            turn.running[me] = None
            if turn.on_cancellable is not None:
                turn.on_cancellable()
        flags = FLAG_REQUESTED_ACK if turn.request_ack else 0
        stop = turn_keep_alive(turn)
        try:
            value = fn(*args, **kwargs)
            if inspect.isawaitable(value):
                value = await value
        except asyncio.CancelledError:
            if cancellable and turn.cancelled:
                raise cancelled_error() from None
            raise
        except (RunFailure, TerminalError) as e:
            if cancellable and turn.cancelled:
                raise cancelled_error() from None
            # A failure is a result too: proposed like a success, replayed as TerminalError.
            turn.emit(MT.PROPOSE_RUN_COMPLETION, encode_propose_run_failure(cid, e.code, e.message), flags)
            turn.propose_run(cid, MT.RUN_COMPLETION, encode_run_completion_failure(cid, e.code, e.message))
            return
        except Exception as e:
            if cancellable and turn.cancelled:
                raise cancelled_error() from None
            # Not proposed, so the retry reruns the side effect; the turn dies here even
            # if the caller swallows the error.
            if turn.failed_with is None:
                turn.failed_with = e
            raise
        finally:
            stop()
            if me is not None:
                turn.running.pop(me, None)
        raw = _serde.to_json(value)
        reuse = turn.reuse_for(raw)
        payload = encode_propose_run_completion(cid, raw, reuse) if reuse or len(raw) <= MAX_FRAME_LEN else b""
        if not payload or len(payload) > MAX_FRAME_LEN:
            # Over the frame cap: a terminal, durable failure of the step, never a frame
            # the runtime would reject and retry forever.
            err = frame_too_large("ctx.run result", len(raw))
            turn.emit(MT.PROPOSE_RUN_COMPLETION, encode_propose_run_failure(cid, err.code, err.message), flags)
            turn.propose_run(cid, MT.RUN_COMPLETION, encode_run_completion_failure(cid, err.code, err.message))
            return
        turn.emit(MT.PROPOSE_RUN_COMPLETION, payload, flags)
        turn.propose_run(cid, MT.RUN_COMPLETION, encode_run_completion(cid, raw))

    # ── streams, log, trace, artifacts ───────────────────────────────────

    def stream(self, name: str, durable: bool = False) -> Stream:
        """A stream of this invocation (tokens, progress) a UI reads over SSE while the
        handler runs. Ephemeral by default. Needs wire v4 (records dropped below)."""
        if not name or len(name) > MAX_STREAM_NAME or name.startswith("$"):
            raise TypeError(f'invalid stream name: {name!r} (1-{MAX_STREAM_NAME} bytes, must not start with "$")')
        return Stream(self._turn, name, durable)

    @property
    def log(self) -> Log:
        return Log(self._turn)

    @property
    def trace(self) -> Trace:
        return Trace(self._turn, self.invocation_id)

    @property
    def artifact(self) -> Artifacts:
        return Artifacts(self._turn, self.invocation_id)

    def continue_as_new(self, input: Any) -> None:
        """Ends this invocation and starts a new one with the same target and ``input``
        and an empty journal. Does not return. Needs wire v3."""
        if self._wire_version < KEEPALIVE_VERSION:
            raise TerminalError(
                f"ctx.continue_as_new needs a runtime speaking wire v{KEEPALIVE_VERSION}; "
                f"the peer speaks v{self._wire_version}"
            )
        raise ContinueAsNew(input)


class ContinueAsNew(Exception):
    """Raised by ``ctx.continue_as_new`` to end the turn."""

    def __init__(self, input: Any) -> None:
        super().__init__("continue_as_new")
        self.input = input


class ScopedContext:
    """A ``ctx`` view with a scope attached (:meth:`Context.scoped`): only ``call`` and
    ``send``, the two commands that create a new invocation. An explicit ``scope=``
    argument beats the view's scope."""

    def __init__(self, ctx: Context, scope: str, limit_key: str | None) -> None:
        self._ctx = ctx
        self._scope = scope
        self._limit_key = limit_key

    def _merge(self, kw: dict[str, Any]) -> dict[str, Any]:
        if kw.get("scope") is None:
            kw["scope"] = self._scope
        if kw.get("limit_key") is None and self._limit_key is not None:
            kw["limit_key"] = self._limit_key
        return kw

    def call(self, service: str, handler: str, input: Any = None, **kw: Any) -> DurableFuture[Any]:
        return self._ctx.call(service, handler, input, **self._merge(kw))

    def send(self, service: str, handler: str, input: Any = None, **kw: Any) -> None:
        self._ctx.send(service, handler, input, **self._merge(kw))


def _check_placement(placement: str | None) -> None:
    if placement is not None and placement != "spread":
        raise TypeError(f'invalid placement {placement!r}; the only value is "spread" (or omit the argument)')


def _ceil(ms: float) -> int:
    i = int(ms)
    return i if i == ms else i + 1


def turn_keep_alive(turn: Turn) -> Callable[[], None]:
    """While a closure runs (``BIDI_STREAM`` with a keepalive interval): after a short
    delay flush the emitted frames (so ``Run`` reaches the runtime before the closure
    ends), then send ``Keepalive`` every interval until the returned stop is called."""
    from ._turn import RUN_EARLY_FLUSH_S
    from ._wire import MT as _MT

    live = turn.live
    if live is None or live.keepalive_ms <= 0:
        return lambda: None
    loop = turn.outer
    beat = Frame(_MT.KEEPALIVE, 0, b"")
    handles: list[asyncio.TimerHandle] = []
    interval = live.keepalive_ms / 1000

    def tick() -> None:
        live.write([beat])
        handles[0] = loop.call_later(interval, tick)

    def early() -> None:
        if len(turn.out) > turn.flushed:
            live.write(turn.out[turn.flushed :])
            turn.flushed = len(turn.out)
        handles[0] = loop.call_later(interval, tick)

    handles.append(loop.call_later(min(RUN_EARLY_FLUSH_S, interval), early))

    def stop() -> None:
        handles[0].cancel()

    return stop
