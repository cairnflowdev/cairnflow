# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""Running one turn of a handler, and the turn's outcome.

``REQUEST_RESPONSE`` (:func:`run_turn`) runs :func:`run_once` once per HTTP request on
the journal the runtime sent; ``BIDI_STREAM`` runs it again on a local journal each time
a frame arrives. One kernel for both, so there is no separate path whose semantics drift.
"""

from __future__ import annotations

import asyncio
import inspect
import traceback
from collections.abc import Callable
from typing import Any

from . import _serde
from ._turn import KEEPALIVE_VERSION, STREAMS_VERSION, Live, ReplayStop, Turn
from ._wire import (
    ERROR_FAIL,
    ERROR_PAUSE,
    ERROR_RETRY,
    JOURNAL_MISMATCH,
    MAX_FRAME_LEN,
    MT,
    PROTOCOL_VIOLATION,
    Frame,
    FrameError,
    StartInfo,
    SuspensionTree,
    decode_frames,
    decode_input,
    decode_input_headers,
    decode_start,
    encode_continue_as_new,
    encode_end,
    encode_error,
    encode_output,
    encode_output_failure,
    encode_replay_report,
    encode_suspension,
    frames_to_bytes,
)
from ._values import Segments, ValueOptions
from ._wire import ValueLayoutError, decode_segment_urls
from .context import LOG_ORDER, Context, ContinueAsNew
from .errors import JournalMismatch, PauseError, SegmentedValueError, Suspend, TerminalError

#: The wire version this SDK speaks at most.
MAX_PROTOCOL_VERSION = 6
MIN_PROTOCOL_VERSION = 1

LOG_LEVEL_HEADER = "cairnflow-log-level"
SCHEDULE_HEADER = "cairnflow-schedule"

Handler = Callable[..., Any]


def _parse_schedule(v: str | None) -> tuple[str, int] | None:
    if v is None:
        return None
    i = v.rfind("@")
    if i <= 0:
        return None
    try:
        return v[:i], int(v[i + 1 :])
    except ValueError:
        return None


async def run_once(
    handler: Handler,
    start: StartInfo,
    entries: list[Frame],
    wire_version: int = MAX_PROTOCOL_VERSION,
    *,
    live: Callable[[list[Frame]], None] | None = None,
    request_ack: bool = True,
    attach: Callable[[Turn], None] | None = None,
    clock: Callable[[], int] | None = None,
    log_level: str | None = None,
    input_type: Any = None,
    takes_input: bool = True,
    segments: Segments | None = None,
) -> Turn:
    loop = asyncio.get_running_loop()
    turn = Turn(entries, loop, segments)
    if clock is not None:
        turn.now = clock
    if attach is not None:
        attach(turn)
    turn.request_ack = request_ack
    turn.replay_only = wire_version >= KEEPALIVE_VERSION and start.replay_only
    turn.wire_version = wire_version
    turn.attempt = start.retry_count
    if live is not None and wire_version >= STREAMS_VERSION:
        turn.wire = live
    if live is not None and wire_version >= KEEPALIVE_VERSION and start.keepalive_ms > 0:
        turn.live = Live(live, start.keepalive_ms)
    if log_level is not None:
        turn.min_log_level = log_level

    value: Any = None
    for e in entries:
        if e.type == MT.INPUT:
            value = _serde.input_json(decode_input(e.payload))
            headers = decode_input_headers(e.payload)
            level = headers.get(LOG_LEVEL_HEADER)
            if level in LOG_ORDER:
                turn.min_log_level = level
            turn.schedule = _parse_schedule(headers.get(SCHEDULE_HEADER))
            break

    ctx = Context(turn, start, wire_version)
    outcome: list[tuple[bool, Any]] = []

    async def body() -> None:
        try:
            if takes_input:
                r = handler(ctx, _serde.coerce(value, input_type))
            else:
                r = handler(ctx)
            if inspect.isawaitable(r):
                r = await r
            outcome.append((True, r))
        except BaseException as e:  # noqa: BLE001 - the turn's outcome, decided below
            outcome.append((False, e))

    running = turn.loop.create_task(body())
    try:
        # Deliver results in journal order until the handler finishes, the turn fails, or
        # it is quiescent.
        await turn.drive(running)
        # Closures still running decide the outcome only once they finish.
        await turn.settle()
        # Proposals of this turn not durable yet: suspend on them, even when the handler
        # already returned (an unawaited ctx.run, the losing branch of a race).
        if turn.suspended_on is None and turn.pending_runs:
            turn.suspended_on = SuspensionTree("first", tuple(sorted(turn.pending_runs)))
        if not outcome:
            fail(turn, turn.failed_with or Suspend("waiting"))
        elif outcome[0][0]:
            finish(turn, outcome[0][1])
        else:
            fail(turn, outcome[0][1])
    finally:
        turn.loop.close()
    return turn


def finish(turn: Turn, output: Any) -> None:  # noqa: C901 - mirrors the TS SDK
    turn.flush_streams()
    if turn.mismatch is not None:
        fail(turn, JournalMismatch(turn.mismatch))
        return
    if turn.failed_with is not None:
        fail(turn, turn.failed_with)
        return
    # The suspended flag wins: user code swallowing a Suspend still suspends correctly.
    if turn.suspended_on is not None:
        turn.emit(MT.SUSPENSION, encode_suspension(turn.suspended_on))
        return

    def build() -> bytes:
        try:
            raw = _serde.to_json(output)
        except (TypeError, ValueError) as e:
            return encode_output_failure(500, f"handler output is not JSON: {e}", "output")
        reuse = turn.reuse_for(raw)
        if reuse or len(raw) <= MAX_FRAME_LEN:
            payload = encode_output(raw, "output", reuse)
            if len(payload) <= MAX_FRAME_LEN:
                return payload
        from ._turn import frame_too_large

        err = frame_too_large("handler output", len(raw))
        return encode_output_failure(err.code, err.message, "output")

    terminate(turn, build)


def terminate(turn: Turn, build: Callable[[], bytes]) -> None:
    """``Output`` (if not durable yet), then ``End``: sent every time, even when
    ``Output`` is already journaled (section 10, ``End`` is not a journal entry)."""
    if not turn.output(build):
        # 570 is never retried, and it is PAUSE, not FAIL: deploy the fix, then resume.
        turn.emit(MT.ERROR, encode_error(JOURNAL_MISMATCH, turn.mismatch or "", ERROR_PAUSE))
        return
    turn.emit(MT.END, encode_end())


def fail(turn: Turn, e: BaseException) -> None:
    turn.flush_streams()
    # A closure's transient error wins over every outcome except a journal mismatch.
    if turn.failed_with is not None and turn.mismatch is None and not turn.replay_only:
        e = turn.failed_with
    if isinstance(e, ContinueAsNew) and turn.mismatch is None and not turn.replay_only:
        turn.emit(MT.CONTINUE_AS_NEW, encode_continue_as_new(_serde.to_json(e.input)))
        return
    if turn.replay_only:
        # The replay report replaces every other ending.
        if turn.mismatch is not None:
            m = turn.mismatch_at
            turn.emit(
                MT.REPLAY_REPORT,
                encode_replay_report(
                    m.at if m else 0, True, m.expected if m else "", m.got if m else "", turn.mismatch
                ),
            )
            return
        if isinstance(e, ReplayStop):
            turn.emit(MT.REPLAY_REPORT, encode_replay_report(e.at, False))
            return
    if turn.mismatch is not None:
        turn.emit(MT.ERROR, encode_error(JOURNAL_MISMATCH, turn.mismatch, ERROR_PAUSE))
        return
    if turn.suspended_on is not None and turn.failed_with is None:
        turn.emit(MT.SUSPENSION, encode_suspension(turn.suspended_on))
        return
    if isinstance(e, TerminalError):
        terminate(turn, lambda: encode_output_failure(e.code, e.message, "output"))
        return
    if isinstance(e, PauseError):
        # Waiting for a human: not an Output, the journal stays so /resume continues here.
        turn.emit(MT.ERROR, encode_error(e.code, e.message, ERROR_PAUSE))
        return
    message = f"{type(e).__name__}: {e}"
    stack = "".join(traceback.format_exception(e))
    turn.emit(MT.ERROR, encode_error(500, f"{message}\n{stack}".strip(), ERROR_RETRY))


def bad_first_message() -> bytes:
    return frames_to_bytes(
        [Frame(MT.ERROR, 0, encode_error(PROTOCOL_VIOLATION, "the first message must be Start", ERROR_FAIL))]
    )


async def run_turn(
    handler: Handler,
    body: bytes,
    wire_version: int = MAX_PROTOCOL_VERSION,
    values: ValueOptions | None = None,
    **opts: Any,
) -> bytes:
    """One ``REQUEST_RESPONSE`` turn: the runtime's bytes in, the reply's bytes out.
    Touches no network, so tests can build every replay situation without a server."""
    try:
        frames = decode_frames(body)
    except FrameError as e:
        return frames_to_bytes([Frame(MT.ERROR, 0, encode_error(PROTOCOL_VIOLATION, str(e), ERROR_FAIL))])
    if not frames or frames[0].type != MT.START:
        return bad_first_message()
    start = decode_start(frames[0].payload)
    # Wire v6: assemble every segmented value BEFORE user code runs. The runtime only sends
    # refs at v6: below it a ref is refused without fetching (an endpoint capped at v5 must
    # not fetch URLs a caller put in the turn), and the tail is never read as the value.
    segments = Segments(values, wire_version)
    segments.add_urls(start.segment_urls)
    entries: list[Frame] = []
    try:
        for f in frames[1:]:
            if f.type == MT.SEGMENT_URLS:
                segments.add_urls(decode_segment_urls(f.payload))
                continue
            await segments.admit(f)
            entries.append(f)
    except (SegmentedValueError, ValueLayoutError) as e:
        return frames_to_bytes([value_failure(e)])
    # Section 4: REQUEST_RESPONSE does not ask for acks.
    turn = await run_once(handler, start, entries, wire_version, request_ack=False, segments=segments, **opts)
    return frames_to_bytes(turn.out)


def value_failure(e: BaseException) -> Frame:
    """A retried ``Error`` when assembling a segmented value fails (never the URL)."""
    return Frame(MT.ERROR, 0, encode_error(500, f"segmented value: {e}", ERROR_RETRY))
