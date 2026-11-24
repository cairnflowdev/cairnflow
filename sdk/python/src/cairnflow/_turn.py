# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""The SDK-side state machine: sections 6 and 8 of ``docs/protocol-v1.md``.

A **turn** = one run of user code from the start, each command matching the next entry
in the journal; no match is a 570. The Python port of ``Turn`` in
``sdk/typescript/src/handler.ts``; the concepts and names are kept so the two can be
reviewed side by side. The conformance traces (generated from that same model) check
the two byte by byte.

User code must not see the replay/processing boundary (section 6), and it does not: the
code runs identically in both phases; commands already in the journal are not emitted
again, and values come from the journal instead of from the outside world.
"""

from __future__ import annotations

import asyncio
import contextvars
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ._loop import DeterministicLoop
from ._wire import (
    CANCEL_SIGNAL,
    MAX_FRAME_LEN,
    MT,
    Frame,
    Result,
    Signal,
    StreamRecord,
    SuspensionTree,
    command_diff,
    decode_command_name,
    decode_notification,
    decode_signal,
    encode_stream_append,
    is_command,
    type_name,
)
from ._values import Segments
from ._wire import SegmentRef, decode_eager_state_value
from .errors import JournalMismatch, SegmentedValueError, Suspend, TerminalError, TurnFailed

#: Wire v3: keepalive, replay report, continue-as-new.
KEEPALIVE_VERSION = 3
#: Wire v4: the ``StreamAppend`` side channel.
STREAMS_VERSION = 4
#: Batching of stream records: flush after this many seconds or this many bytes.
STREAM_BATCH_S = 0.05
STREAM_BATCH_BYTES = 1024
#: A ``ctx.run`` closure running longer than this flushes ``Run`` and starts keepalives.
RUN_EARLY_FLUSH_S = 0.02

#: The position of the running ``ctx.run`` closure's ``Run`` command; ``None`` outside one.
closure_scope: contextvars.ContextVar[int | None] = contextvars.ContextVar("cairnflow_closure", default=None)


def cancelled_error() -> TerminalError:
    """The error of every cancellation point: 409, as in Restate."""
    return TerminalError("invocation was cancelled (CANCEL signal)", 409)


def frame_too_large(what: str, size: int) -> TerminalError:
    return TerminalError(
        f"{what} of {size} bytes exceeds the {MAX_FRAME_LEN}-byte frame limit; "
        "keep large data out of the journal (ctx.artifact)",
        413,
    )


class ReplayStop(Exception):
    """A ``replay_only`` turn reached the first command outside the journal."""

    def __init__(self, at: int) -> None:
        super().__init__(f"replay stopped at command {at}")
        self.at = at


@dataclass(eq=False)
class Waiter:
    """A wait point of user code. ``check`` settles it and returns ``True`` when done."""

    keys: tuple[str, ...]
    mode: str  # "first" | "all"
    check: Callable[[], bool]


@dataclass
class MismatchAt:
    at: int
    expected: str
    got: str


@dataclass
class Live:
    """The mid-turn write path (``BIDI_STREAM`` only)."""

    write: Callable[[list[Frame]], None]
    keepalive_ms: int


class Turn:
    def __init__(self, entries: list[Frame], loop: asyncio.AbstractEventLoop, segments: Segments | None = None) -> None:
        self.outer = loop
        #: Assembled copies of segmented values (wire v6); ``None`` when the turn has none.
        self.segments = segments
        self._wakeup = asyncio.Event()
        self.loop = DeterministicLoop(loop, self.poke)
        #: Stored command entries (whole frames), not counting the runtime's ``Input``.
        self.journaled: list[Frame] = []
        self.completions: dict[int, Result] = {}
        #: Signals received, in journal order.
        self.signals: list[Signal] = []
        #: Journal position of each leaf's first appearance: completions (``c<id>``) and
        #: signals (``i<idx>``/``n<name>``) on ONE sequence, so ``race`` picks the winner
        #: among mixed leaves in exactly the recorded order.
        self.positions: dict[str, int] = {}
        #: Delivery schedule: leaves in journal order, one list per position.
        self.deliveries: list[list[str]] = []
        self.next_delivery = 0
        #: Delivered leaves: user code may see their results.
        self.delivered: set[str] = set()
        #: Pending waits, by the undelivered leaf they need (dicts as ordered sets).
        self.waiters_by_key: dict[str, dict[Waiter, None]] = {}
        #: Every pending wait: becomes the ``Suspension`` tree at the end of the turn.
        self.pending: dict[Waiter, None] = {}
        #: CANCEL received: every later external wait throws 409.
        self.cancelled = False
        #: Cancel callbacks of the cancellable ``ctx.run`` closures currently running.
        self.running: dict[asyncio.Task[Any], None] = {}
        self.on_cancellable: Callable[[], None] | None = None
        self.cursor = 0
        self.next_id = 1
        self.next_awakeable = 17
        #: Frames to send to the runtime, in emission order.
        self.out: list[Frame] = []
        #: ``ctx.run`` results proposed but not yet durable (the pre-built ``RunCompletion``).
        self.pending_runs: dict[int, Frame] = {}
        #: ``ctx.run`` closures of this turn still running (tasks on the real loop).
        self.inflight: dict[asyncio.Task[Any], None] = {}
        #: The SDK's own async work outside closures (``ctx.artifact.get`` downloads): the
        #: drain waits for it, because user code wakes after it in the same order on replay.
        self.busy: dict[asyncio.Task[Any], None] = {}
        self.suspended_on: SuspensionTree | None = None
        #: A transient error of a ``ctx.run`` closure: wins like the suspended flag.
        self.failed_with: BaseException | None = None
        self.mismatch: str | None = None
        self.mismatch_at: MismatchAt | None = None
        self.live: Live | None = None
        self.flushed = 0
        self.replay_only = False
        #: Set ``REQUESTED_ACK`` on ``ProposeRunCompletion``: only in ``BIDI_STREAM``.
        self.request_ack = True
        self.wire: Callable[[list[Frame]], None] | None = None
        self.wire_version = 0
        self.attempt = 0
        self.min_log_level = "info"
        self.schedule: tuple[str, int] | None = None
        #: The SDK's wall clock (ms). Fixed by the conformance runner.
        self.now: Callable[[], int] = lambda: int(time.time() * 1000)
        self._pending_streams: dict[str, tuple[bool, list[StreamRecord], list[bool]]] = {}
        self._stream_k: dict[tuple[int, str], int] = {}
        self._pending_stream_bytes = 0
        self._stream_timer: asyncio.TimerHandle | None = None
        self._warned_streams = False

        last_at = -1
        for at, e in enumerate(entries):
            if e.type == MT.INPUT:
                continue  # the runtime's entry, not user code's
            if e.type == MT.SIGNAL:
                # Signals are identified by idx/name, not by completion id.
                s = decode_signal(e.payload)
                self.signals.append(s)
                if s.idx is not None:
                    last_at = self._seen(f"i{s.idx}", at, last_at)
                if s.name is not None:
                    last_at = self._seen(f"n{s.name}", at, last_at)
                if s.idx == CANCEL_SIGNAL:
                    self.cancelled = True
                continue
            if is_command(e.type):
                self.journaled.append(e)
            else:
                cid, result = decode_notification(e.payload)
                self.completions[cid] = self._assembled_result(e, result)
                last_at = self._seen(f"c{cid}", at, last_at)

    def _assembled_result(self, frame: Frame, r: Result) -> Result:
        """Segmented values replaced by the bytes assembled before the turn ran. Not
        assembled: left as is, so reading it is a turn error, never the tail as the value."""
        if r.kind != "segmented":
            return r
        a = self.segments.of(frame) if self.segments is not None else None
        if a is None:
            return r
        return Result("value", value=a.bytes, refs=a.refs)

    def eager_value(self, frame: Frame) -> tuple[bytes | None, tuple[SegmentRef, ...]]:
        """The value embedded in a journaled ``GetEagerState`` (assembled if segmented)."""
        v = decode_eager_state_value(frame.payload)
        if v is None:
            return None, ()
        if not v.segments:
            return v.content, ()
        a = self.segments.of(frame) if self.segments is not None else None
        if a is None:
            raise SegmentedValueError("segmented value was not assembled before the turn")
        return a.bytes, a.refs

    def reuse_for(self, nxt: bytes) -> tuple[SegmentRef, ...]:
        return self.segments.reuse_for(nxt) if self.segments is not None else ()

    def _seen(self, key: str, at: int, last_at: int) -> int:
        if key in self.positions:
            return last_at
        self.positions[key] = at
        if at == last_at:
            self.deliveries[-1].append(key)
        else:
            self.deliveries.append([key])
        return at

    # ── ids ──────────────────────────────────────────────────────────────

    def alloc(self) -> int:
        """Allocates a completion id. Identical on every replay only for deterministic code."""
        n = self.next_id
        self.next_id += 1
        return n

    def alloc_awakeable(self) -> int:
        """Signal index for the next awakeable: from 17 (1-16 are built-in signals)."""
        n = self.next_awakeable
        self.next_awakeable += 1
        return n

    @property
    def position(self) -> int:
        return self.cursor

    def peek(self) -> int | None:
        """Kind of the entry journaled at the next position, without moving the cursor."""
        return self.journaled[self.cursor].type if self.cursor < len(self.journaled) else None

    def replaying(self) -> bool:
        """The next command is already in the journal (never inside a running closure)."""
        if self.replay_only:
            return True
        return closure_scope.get() is None and self.cursor < len(self.journaled)

    def dead(self) -> bool:
        """The turn already has an outcome; code still running outside closures is a ghost."""
        return self.suspended_on is not None or self.failed_with is not None or self.mismatch is not None

    # ── commands ─────────────────────────────────────────────────────────

    def _diverged(self, at: int, expected: str, got: str, message: str) -> JournalMismatch:
        self.mismatch_at = MismatchAt(at, expected, got)
        self.mismatch = message
        self.poke()
        return JournalMismatch(message)

    def command(self, ty: int, build: Callable[[], bytes], name: str | None = None) -> Frame | None:
        """One user-code command. Returns the journaled frame if it is already in the
        journal (not emitted again), ``None`` if it was just emitted. ``build`` is only
        called when needed, so a non-deterministic value (a wake-up time) is computed once.
        """
        if self.mismatch is not None:
            raise JournalMismatch(self.mismatch)
        if self.failed_with is not None:
            raise TurnFailed(self.failed_with)
        if self.suspended_on is not None:
            raise Suspend("the turn has suspended")
        at = self.cursor
        self.cursor += 1
        if at < len(self.journaled):
            journaled = self.journaled[at]
            if journaled.type != ty:
                raise self._diverged(
                    at,
                    type_name(journaled.type),
                    type_name(ty),
                    f"entry {at}: journal has {type_name(journaled.type)}, current code produced {type_name(ty)}",
                )
            if name is not None:
                was = decode_command_name(journaled.payload)
                if was != name:
                    expected = f"{type_name(ty)} {_js(was)}"
                    got = f"{type_name(ty)} {_js(name)}"
                    raise self._diverged(at, expected, got, f"entry {at}: journal has {expected}, current code produced {got}")
            # Same kind and name is not enough: target, key, parameters, completion id too.
            current = build()
            diff = command_diff(ty, journaled.payload, current)
            if diff is not None:
                expected = f"{type_name(ty)} {_js(decode_command_name(journaled.payload))}"
                got = f"{type_name(ty)} {_js(decode_command_name(current))}"
                raise self._diverged(
                    at, expected, got, f"entry {at}: journal has {expected}, current code produced {got} (field {diff} differs)"
                )
            return journaled
        # Trial replay: stop before build() and before any closure.
        if self.replay_only:
            raise ReplayStop(at)
        payload = build()
        if len(payload) > MAX_FRAME_LEN:
            raise frame_too_large(type_name(ty), len(payload))
        self.out.append(Frame(ty, 0, payload))
        return None

    def emit(self, ty: int, payload: bytes, flags: int = 0) -> None:
        self.out.append(Frame(ty, flags, payload))

    def output(self, build: Callable[[], bytes]) -> bool:
        """The ending command ``Output``: a journal entry (``End`` is not). Not re-emitted
        when already durable; ``False`` = the journal has something else here (570)."""
        at = self.cursor
        self.cursor += 1
        if at < len(self.journaled):
            journaled = self.journaled[at]
            if journaled.type == MT.OUTPUT:
                return True
            self.mismatch = (
                f"entry {at}: journal has {type_name(journaled.type)}, current code produced {type_name(MT.OUTPUT)}"
            )
            return False
        self.out.append(Frame(MT.OUTPUT, 0, build()))
        return True

    def propose_run(self, cid: int, ty: int, payload: bytes) -> None:
        """Holds a ``ctx.run`` result back until its ack."""
        self.pending_runs[cid] = Frame(ty, 0, payload)

    # ── results and waits ────────────────────────────────────────────────

    def known(self, cid: int) -> Result | None:
        return self.completions.get(cid) if f"c{cid}" in self.delivered else None

    def present(self, key: str) -> bool:
        return key in self.positions

    def is_delivered(self, key: str) -> bool:
        return key in self.delivered

    def demand(self, cid: int) -> Result:
        r = self.known(cid)
        if r is not None and r.kind == "segmented":
            # Retried, not terminal: a segment unreadable in this turn may read in the next.
            raise SegmentedValueError("segmented value was not assembled before the turn")
        if r is None:
            raise RuntimeError(f"internal: completion {cid} read before it was delivered")
        return r

    def wait(self, w: Waiter) -> None:
        if w.check():
            return
        self.pending[w] = None
        for k in w.keys:
            if k in self.delivered:
                continue
            self.waiters_by_key.setdefault(k, {})[w] = None
        self.poke()

    def when(self, keys: tuple[str, ...]) -> asyncio.Future[None]:
        """A future done once every leaf is delivered. A leaf not in the journal is an
        external wait: pending for the rest of the turn (after CANCEL: a 409)."""
        fut: asyncio.Future[None] = self.loop.create_future()
        if all(k in self.delivered for k in keys):
            fut.set_result(None)
            return fut
        if self.cancelled and any(k not in self.positions for k in keys):
            fut.set_exception(cancelled_error())
            return fut

        def check() -> bool:
            if not all(k in self.delivered for k in keys):
                return False
            if not fut.done():
                fut.set_result(None)
            return True

        self.wait(Waiter(keys, "first" if len(keys) == 1 else "all", check))
        return fut

    def await_proposal(self, cid: int) -> asyncio.Future[Any]:
        """Waits for the durability of a ``ctx.run`` proposal: never in this turn; it puts
        the id into the suspension tree. Not a cancellation point."""
        fut: asyncio.Future[Any] = self.loop.create_future()
        self.wait(Waiter((f"c{cid}",), "first", lambda: False))
        return fut

    def track(self, task: asyncio.Task[Any]) -> asyncio.Future[Any]:
        """Records a ``ctx.run`` closure (a task on the real loop) until it finishes;
        returns a future of the handler loop that settles with it."""
        return self._bridge(task, self.inflight)

    def busy_with(self, task: asyncio.Task[Any]) -> asyncio.Future[Any]:
        """Records the SDK's own async work outside closures; see ``busy``."""
        return self._bridge(task, self.busy)

    def _bridge(self, task: asyncio.Task[Any], into: dict[asyncio.Task[Any], None]) -> asyncio.Future[Any]:
        into[task] = None
        bridge: asyncio.Future[Any] = self.loop.create_future()

        def done(t: asyncio.Task[Any]) -> None:
            into.pop(t, None)
            if not bridge.done():
                if t.cancelled():
                    bridge.set_exception(asyncio.CancelledError())
                elif t.exception() is not None:
                    bridge.set_exception(t.exception())  # type: ignore[arg-type]
                else:
                    bridge.set_result(t.result())
            self.poke()

        task.add_done_callback(done)
        self.poke()
        return bridge

    async def settle(self) -> None:
        """Waits for every unfinished ``ctx.run`` closure of the turn."""
        while self.inflight:
            await asyncio.wait(list(self.inflight))

    def poke(self) -> None:
        self._wakeup.set()

    async def _drain(self) -> None:
        while True:
            while self.busy:
                await asyncio.wait(list(self.busy))
            self._wakeup.clear()
            await self.loop.step()
            if not self.busy:
                return

    def _deliver_next(self) -> bool:
        """Delivers the next leaves in journal order until a delivery wakes someone."""
        while self.next_delivery < len(self.deliveries):
            woke = False
            batch = self.deliveries[self.next_delivery]
            self.next_delivery += 1
            for k in batch:
                self.delivered.add(k)
                ws = self.waiters_by_key.pop(k, None)
                if ws is None:
                    continue
                for w in ws:
                    if w not in self.pending or not w.check():
                        continue
                    del self.pending[w]
                    woke = True
            if woke:
                return True
        return False

    async def drive(self, running: asyncio.Task[Any]) -> None:
        """The loop of a turn: drain, deliver the next leaf, drain, ...

        User code only wakes at a delivery (in journal order, each after a full drain);
        a closure finishing only adds a proposal. Stops when the handler finishes, when
        the turn fails, or when quiescent: then ``suspended_on`` is the tree of every
        pending external wait.
        """
        while True:
            await self._drain()
            if running.done() or self.failed_with is not None or self.mismatch is not None:
                return
            if self._deliver_next():
                continue
            if self.inflight or not self.pending:
                # A closure still running, or user code waiting on something outside the
                # SDK (a timer): wait until something wakes the handler loop.
                if not self._wakeup.is_set() and not self.loop.has_ready():
                    await self._wakeup.wait()
                continue
            self.suspended_on = self.wait_tree()
            return

    def wait_tree(self) -> SuspensionTree:
        """The ``Suspension`` tree from every pending wait: one wait = exactly its tree;
        several = a ``first`` root merging leaves, each multi-leaf ``all`` wait a child."""

        def node(w: Waiter) -> SuspensionTree:
            left = [k for k in w.keys if k not in self.delivered]
            return SuspensionTree(
                "all" if w.mode == "all" and len(left) > 1 else "first",
                tuple(int(k[1:]) for k in left if k[0] == "c"),
                tuple(int(k[1:]) for k in left if k[0] == "i"),
                tuple(k[1:] for k in left if k[0] == "n"),
            )

        nodes = [node(w) for w in self.pending]
        if len(nodes) == 1:
            return nodes[0]
        comps: set[int] = set()
        sigs: set[int] = set()
        names: set[str] = set()
        nested: list[SuspensionTree] = []
        for n in nodes:
            if n.combinator == "all":
                nested.append(n)
                continue
            comps.update(n.completions)
            sigs.update(n.signals)
            names.update(n.names)
        return SuspensionTree(
            "first", tuple(sorted(comps)), tuple(sorted(sigs)), tuple(sorted(names, key=_utf16_key)), tuple(nested)
        )

    def cancel(self) -> None:
        """CANCEL arrived mid-turn (``BIDI_STREAM``): cancellable closures are aborted."""
        if self.cancelled:
            return
        self.cancelled = True
        for task in list(self.running):
            task.cancel()

    def signal_seen(self, signal: int | str) -> Signal | None:
        for s in self.signals:
            if (s.idx == signal) if isinstance(signal, int) else (s.name == signal):
                return s
        return None

    def order_of(self, cid: int) -> int:
        return self.positions.get(f"c{cid}", 1 << 62)

    def order_of_signal(self, signal: int | str) -> int:
        key = f"i{signal}" if isinstance(signal, int) else f"n{signal}"
        return self.positions.get(key, 1 << 62)

    # ── streams (wire v4 side channel) ───────────────────────────────────

    def append_with(
        self, name: str, durable: bool, build: Callable[[int, int, int], bytes] | None
    ) -> tuple[int, int, int, bool] | None:
        """Appends a record built from its identity ``(step, attempt, k)``, or closes the
        stream (``build`` is ``None``). Replay still allocates identities without sending,
        so a later live record repeats the identity of the one already sent (the runtime
        drops the duplicate). Never raises: a stream must not break the invocation."""
        if self.wire_version < STREAMS_VERSION:
            if not self._warned_streams:
                self._warned_streams = True
                import warnings

                warnings.warn(
                    f"ctx.stream needs a runtime speaking wire v{STREAMS_VERSION}; "
                    f"the peer speaks v{self.wire_version}, record dropped",
                    RuntimeWarning,
                    stacklevel=3,
                )
            return None
        if self.replaying():
            if build is None:
                return None
            step, attempt, k = self._next_ident(name)
            return step, attempt, k, False
        # Once the turn has an outcome, code still running outside closures is a ghost: the
        # rerun goes through this stretch again and must own these identities.
        if self.dead() and closure_scope.get() is None:
            return None
        p = self._pending_streams.get(name)
        if p is None:
            p = (durable, [], [False])
            self._pending_streams[name] = p
        ident: tuple[int, int, int] | None = None
        if build is None:
            p[2][0] = True
        else:
            ident = self._next_ident(name)
            payload = build(*ident)
            p[1].append(StreamRecord(ident[0], ident[1], ident[2], payload))
            self._pending_stream_bytes += len(payload)
        if self._pending_stream_bytes >= STREAM_BATCH_BYTES or build is None:
            self.flush_streams()
        elif self.wire is not None and self._stream_timer is None:
            self._stream_timer = self.outer.call_later(STREAM_BATCH_S, self._timer_flush)
        return None if ident is None else (*ident, True)

    def _timer_flush(self) -> None:
        self._stream_timer = None
        self.flush_streams()

    def _next_ident(self, name: str) -> tuple[int, int, int]:
        step = closure_scope.get()
        if step is None:
            step = self.cursor
        key = (step, name)
        k = self._stream_k.get(key, 0)
        self._stream_k[key] = k + 1
        return step, self.attempt, k

    def flush_streams(self) -> None:
        """Puts every batched record on the wire (BIDI) or into ``out`` (RR). Called
        before every turn-ending frame: the runtime stops reading after the outcome."""
        if self._stream_timer is not None:
            self._stream_timer.cancel()
            self._stream_timer = None
        if not self._pending_streams:
            return
        frames = [
            Frame(MT.STREAM_APPEND, 0, encode_stream_append(name, durable, records, close[0]))
            for name, (durable, records, close) in self._pending_streams.items()
        ]
        self._pending_streams.clear()
        self._pending_stream_bytes = 0
        if self.wire is None:
            self.out.extend(frames)
            return
        # Keep program order: commands already emitted go before records produced after them.
        head = self.out[self.flushed :]
        self.flushed = len(self.out)
        self.wire(head + frames)

    def close(self) -> None:
        """The whole turn is on the wire: no write path may be used any more."""
        self.flushed = len(self.out)
        self.wire = None
        self.live = None
        if self._stream_timer is not None:
            self._stream_timer.cancel()
            self._stream_timer = None
        self.loop.close()


def _js(s: str) -> str:
    """``JSON.stringify`` of a string: the form the TS SDK puts in mismatch messages."""
    import json

    return json.dumps(s, ensure_ascii=False)


def _utf16_key(s: str) -> list[int]:
    """Sort key matching JavaScript's default string order (UTF-16 code units)."""
    return list(s.encode("utf-16-be"))
