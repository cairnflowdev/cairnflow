# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""The turn loop on a ``BIDI_STREAM`` connection (section 4, "Turns on one stream").

::

    runtime                                   SDK
    Start + journal replay      ------>
                                              runs the handler
                                <------       commands, then Suspension
    resolvable in place? push notifications ->
                                              reruns the handler from the start on the
                                              local journal: no round trip
    external wait? close the stream           turn over, as in REQUEST_RESPONSE

Rerunning from the start (instead of resuming the coroutine) is deliberate: it is the
same replay model as ``REQUEST_RESPONSE``, so there is no second code path to drift.
"""

from __future__ import annotations

import asyncio
import logging
import warnings
from collections.abc import Callable
from typing import Any, Protocol

from ._invoke import Handler, bad_first_message, run_once, value_failure
from ._values import Segments, ValueOptions
from .errors import SegmentedValueError
from ._turn import Turn
from ._wire import (
    CANCEL_SIGNAL,
    FLAG_REQUESTED_ACK,
    HEADER_LEN,
    MAX_FRAME_LEN,
    MT,
    Frame,
    FrameError,
    SuspensionTree,
    ValueLayoutError,
    decode_notification,
    decode_segment_urls,
    decode_signal,
    decode_start,
    frames_to_bytes,
    is_command,
    is_notification,
    tree_satisfied,
)

log = logging.getLogger("cairnflow")

#: A suspension on a still-open stream this long is suspicious (see :func:`run_bidi`).
SUSPEND_STALL_S = 60.0


class FrameStream(Protocol):
    """Both directions of an open stream, as frames in and bytes out."""

    async def next(self) -> Frame | None:
        """The next frame from the runtime; ``None`` = the runtime closed the stream."""
        ...

    def buffered(self) -> list[Frame]:
        """Frames that have arrived and are waiting: takes them all without blocking."""
        ...

    def write(self, data: bytes) -> None: ...

    def end(self) -> None:
        """Ends the SDK side. The turn is over."""
        ...


class FrameSplitter:
    """Splits frames out of a byte stream arriving in chunks. Trailing bytes mean "wait
    for more" (the body has not ended), unlike ``decode_frames``."""

    def __init__(self) -> None:
        self._buf = bytearray()

    def push(self, chunk: bytes) -> list[Frame]:
        self._buf += chunk
        out: list[Frame] = []
        at = 0
        buf = self._buf
        while len(buf) - at >= HEADER_LEN:
            ty = int.from_bytes(buf[at : at + 2], "big")
            flags = int.from_bytes(buf[at + 2 : at + 4], "big")
            size = int.from_bytes(buf[at + 4 : at + 8], "big")
            # Check the header before waiting for a payload of a lying length.
            if flags & ~FLAG_REQUESTED_ACK:
                raise FrameError(f"reserved flag bits must be 0, got 0x{flags:x}")
            if size > MAX_FRAME_LEN:
                raise FrameError(f"payload of {size} bytes exceeds the limit of {MAX_FRAME_LEN}")
            if len(buf) - at < HEADER_LEN + size:
                break
            out.append(Frame(ty, flags, bytes(buf[at + HEADER_LEN : at + HEADER_LEN + size])))
            at += HEADER_LEN + size
        if at:
            del self._buf[:at]
        return out

    @property
    def pending(self) -> int:
        return len(self._buf)


class QueueStream:
    """A :class:`FrameStream` fed by a transport: ``offer`` frames, ``finish`` on close.

    ``watch`` sees every frame the moment it arrives (a mid-turn CANCEL must reach a
    running closure at once); the frame still stays queued, in order, for the wait loop.
    """

    def __init__(self, write: Callable[[bytes], None], end: Callable[[], None]) -> None:
        self._queue: list[Frame] = []
        self._closed = False
        self._waiter: asyncio.Future[None] | None = None
        self._write = write
        self._end = end
        self.watcher: Callable[[Frame], None] | None = None

    def offer(self, f: Frame) -> None:
        self._queue.append(f)
        if self.watcher is not None:
            self.watcher(f)
        self._wake()

    def finish(self) -> None:
        self._closed = True
        self._wake()

    def _wake(self) -> None:
        w = self._waiter
        if w is not None and not w.done():
            w.set_result(None)

    async def next(self) -> Frame | None:
        while not self._queue:
            if self._closed:
                return None
            self._waiter = asyncio.get_running_loop().create_future()
            await self._waiter
            self._waiter = None
        return self._queue.pop(0)

    def buffered(self) -> list[Frame]:
        out, self._queue = self._queue, []
        return out

    def write(self, data: bytes) -> None:
        if data:
            self._write(data)

    def end(self) -> None:
        self._end()


def is_journal_entry(ty: int) -> bool:
    """Acks and ``0xFC00+`` take no journal slot: putting them in the local journal would
    shift the index of every later entry."""
    return is_command(ty) or is_notification(ty)


def _note_custom_dropped(f: Frame) -> None:
    if f.type >= 0xFC00:
        warnings.warn(
            f"custom message 0x{f.type:x} dropped: the SDK has no API for 0xFC00+ messages yet",
            RuntimeWarning,
            stacklevel=2,
        )


def suspension_satisfied(t: SuspensionTree, entries: list[Frame]) -> bool:
    """Whether what the SDK awaits is already in the local journal: the same rule as the
    runtime's ``suspension_satisfied``, so the loop reads until *enough* has arrived."""
    done: set[int] = set()
    idx: set[int] = set()
    names: set[str] = set()
    for e in entries:
        if e.type == MT.SIGNAL:
            s = decode_signal(e.payload)
            if s.idx is not None:
                idx.add(s.idx)
            if s.name is not None:
                names.add(s.name)
        elif is_notification(e.type):
            done.add(decode_notification(e.payload)[0])
    return tree_satisfied(t, done.__contains__, idx.__contains__, names.__contains__)


async def run_bidi(
    handler: Handler, io: QueueStream, wire_version: int, values: ValueOptions | None = None, **opts: Any
) -> None:
    first = await io.next()
    if first is None:
        return  # the runtime gave up before saying anything
    if first.type != MT.START:
        io.write(bad_first_message())
        io.end()
        return
    start = decode_start(first.payload)

    # The local journal. The runtime does not resend it in later turns on the same stream
    # (that is the saving), so the SDK keeps it: the replayed entries, the entries it emits,
    # the notifications that come later.
    entries: list[Frame] = []
    # Wire v6: the stream's URL table and each frame's assembled copy.
    segments = Segments(values, wire_version)
    segments.add_urls(start.segment_urls)

    async def admit(f: Frame) -> bool:
        """Takes a frame into the local journal, assembling a segmented value first;
        assembly failing ends the stream with a retried Error."""
        if f.type == MT.SEGMENT_URLS:
            segments.add_urls(decode_segment_urls(f.payload))
            return True
        if not is_journal_entry(f.type):
            return True
        try:
            await segments.admit(f)
        except (SegmentedValueError, ValueLayoutError) as e:
            io.write(frames_to_bytes([value_failure(e)]))
            io.end()
            return False
        entries.append(f)
        return True

    i = 0
    while i < start.known_entries:
        f = await io.next()
        if f is None:
            return  # the stream broke during the replay
        if f.type != MT.SEGMENT_URLS:
            i += 1
        if not await admit(f):
            return
    for f in io.buffered():
        _note_custom_dropped(f)
        if not await admit(f):
            return

    loop = asyncio.get_running_loop()
    while True:
        current: list[Turn] = []

        def watch(f: Frame) -> None:
            if f.type == MT.SIGNAL and decode_signal(f.payload).idx == CANCEL_SIGNAL and current:
                current[0].cancel()

        io.watcher = watch
        turn = await run_once(
            handler,
            start,
            list(entries),
            wire_version,
            live=lambda frames: io.write(frames_to_bytes(frames)),
            request_ack=True,
            attach=current.append,
            segments=segments,
            **opts,
        )
        io.watcher = None
        io.write(frames_to_bytes(turn.out[turn.flushed :]))
        turn.close()
        for f in turn.out:
            if is_journal_entry(f.type):
                entries.append(f)

        # A turn ends with exactly one of three (section 6); two of them stop for good.
        spec = turn.suspended_on
        if spec is None or not turn.out or turn.out[-1].type != MT.SUSPENSION:
            io.end()
            return

        # Suspended and NOT closed: the runtime either pushes what we await, or closes the
        # stream because only the outside world can answer. The timer plays no role in the
        # protocol; it is a warning light for the two copies of the satisfaction rule
        # disagreeing (both sides would wait for each other, silently).
        stalled = loop.call_later(
            SUSPEND_STALL_S,
            lambda: log.warning(
                "suspension has waited over %ss on a stream that is still open: the runtime may consider it "
                "satisfied while the SDK does not (do the two suspension_satisfied implementations disagree?)",
                int(SUSPEND_STALL_S),
            ),
        )
        try:
            proposals = SuspensionTree("all", tuple(turn.pending_runs))
            while True:
                woken = await io.next()
                if woken is None:
                    io.end()
                    return
                _note_custom_dropped(woken)
                # An ack is the receipt for an entry we already built: only now does the
                # pre-built RunCompletion go into the local journal.
                if woken.type == MT.PROPOSE_RUN_COMPLETION_ACK:
                    cid = decode_notification(woken.payload)[0]
                    settled = turn.pending_runs.get(cid)
                    if settled is not None:
                        entries.append(settled)
                if not await admit(woken):
                    return
                # The next turn runs only when every proposal of this turn is durable.
                if suspension_satisfied(spec, entries) and suspension_satisfied(proposals, entries):
                    break
        finally:
            stalled.cancel()

