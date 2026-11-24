# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""A deterministic event loop for handler code.

The turn driver must know when user code has run as far as it can, so it can deliver the
next journaled result **in journal order** (see ``_turn.Turn.drive``). asyncio has no
public "the ready queue is empty" API, so handler code (the handler task and every task
it creates: ``asyncio.gather``, ``asyncio.create_task``) runs on this loop instead, which
the driver steps until quiescent. The same technique as the Temporal Python SDK's
workflow event loop.

``ctx.run`` closures (real I/O) do **not** run here: they run on the server's real loop.
Timers (``asyncio.sleep`` in handler code, outside ``ctx.run``) are forwarded to the real
loop and count as "waiting on something outside the SDK".

Stepping happens inside a plain callback of the real loop, never inside one of its
tasks: asyncio refuses to enter a task while another task of the same thread is running.
Network I/O on this loop is refused on purpose: I/O belongs in ``ctx.run``.
"""

from __future__ import annotations

import asyncio
import collections
import contextvars
import logging
from collections.abc import Callable
from typing import Any

log = logging.getLogger("cairnflow")

_IO_MESSAGE = "I/O in handler code must go through ctx.run (this loop runs replayable code only)"


class DeterministicLoop(asyncio.AbstractEventLoop):
    def __init__(self, outer: asyncio.AbstractEventLoop, wake: Callable[[], None]) -> None:
        self._outer = outer
        self._wake = wake
        self._ready: collections.deque[asyncio.Handle] = collections.deque()
        self._timers: dict[asyncio.TimerHandle, asyncio.TimerHandle] = {}
        self._closed = False
        self._stepping = False
        self._exception_handler: Any = None

    # ── stepping ─────────────────────────────────────────────────────────

    def run_until_quiescent(self) -> None:
        """Runs every ready callback (and those they schedule) until none is left.

        Must be called from a plain callback of the real loop (see :func:`step`)."""
        prev = asyncio.events._get_running_loop()
        asyncio.events._set_running_loop(self)
        self._stepping = True
        try:
            while self._ready:
                handle = self._ready.popleft()
                if not handle._cancelled:
                    handle._run()
        finally:
            self._stepping = False
            asyncio.events._set_running_loop(prev)

    async def step(self) -> None:
        """Runs :meth:`run_until_quiescent` from a plain callback of the real loop."""
        done = self._outer.create_future()

        def run() -> None:
            try:
                self.run_until_quiescent()
            except BaseException as e:  # pragma: no cover - handles never raise
                done.set_exception(e)
            else:
                done.set_result(None)

        self._outer.call_soon(run)
        await done

    def has_ready(self) -> bool:
        return bool(self._ready)

    # ── scheduling ───────────────────────────────────────────────────────

    def call_soon(
        self, callback: Callable[..., object], *args: Any, context: contextvars.Context | None = None
    ) -> asyncio.Handle:
        handle = asyncio.Handle(callback, args, self, context)
        self._ready.append(handle)
        if not self._stepping:
            self._wake()
        return handle

    def call_soon_threadsafe(
        self, callback: Callable[..., object], *args: Any, context: contextvars.Context | None = None
    ) -> asyncio.Handle:
        handle = asyncio.Handle(callback, args, self, context)

        def enqueue() -> None:
            self._ready.append(handle)
            self._wake()

        self._outer.call_soon_threadsafe(enqueue)
        return handle

    def call_later(
        self, delay: float, callback: Callable[..., object], *args: Any, context: contextvars.Context | None = None
    ) -> asyncio.TimerHandle:
        return self.call_at(self.time() + delay, callback, *args, context=context)

    def call_at(
        self, when: float, callback: Callable[..., object], *args: Any, context: contextvars.Context | None = None
    ) -> asyncio.TimerHandle:
        timer = asyncio.TimerHandle(when, callback, args, self, context)
        timer._scheduled = True

        def fire() -> None:
            self._timers.pop(timer, None)
            if not timer._cancelled:
                self._ready.append(timer)
                self._wake()

        self._timers[timer] = self._outer.call_at(when, fire)
        return timer

    def _timer_handle_cancelled(self, handle: asyncio.TimerHandle) -> None:
        outer = self._timers.pop(handle, None)
        if outer is not None:
            outer.cancel()

    def time(self) -> float:
        return self._outer.time()

    def create_future(self) -> asyncio.Future[Any]:
        return asyncio.Future(loop=self)

    def create_task(self, coro: Any, *, name: str | None = None, context: contextvars.Context | None = None, **_: Any) -> asyncio.Task[Any]:
        return asyncio.Task(coro, loop=self, name=name, context=context)

    # ── state ────────────────────────────────────────────────────────────

    def get_debug(self) -> bool:
        return False

    def set_debug(self, enabled: bool) -> None:
        pass

    def is_running(self) -> bool:
        return not self._closed

    def is_closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        """Drops everything still scheduled; abandoned handler tasks are not resumed."""
        self._closed = True
        self._ready.clear()
        for outer in self._timers.values():
            outer.cancel()
        self._timers.clear()

    def get_exception_handler(self) -> Any:
        return self._exception_handler

    def set_exception_handler(self, handler: Any) -> None:
        self._exception_handler = handler

    def default_exception_handler(self, context: dict[str, Any]) -> None:
        # A turn that suspended abandons the handler task where it waits: that is the
        # protocol (the next turn reruns the handler from the start), not a leak.
        message = context.get("message", "")
        if message == "Task was destroyed but it is pending!":
            return
        exc = context.get("exception")
        if message.endswith("exception was never retrieved"):
            # A turn that ended early (a failed closure, a mismatch) leaves the futures
            # of code it will rerun anyway; worth a trace for debugging, not an error.
            log.debug("cairnflow handler loop: %s", message, exc_info=exc)
            return
        log.error("cairnflow handler loop: %s", context.get("message"), exc_info=exc)

    def call_exception_handler(self, context: dict[str, Any]) -> None:
        if self._exception_handler is not None:
            self._exception_handler(self, context)
        else:
            self.default_exception_handler(context)

    def get_task_factory(self) -> None:
        return None

    # ── refused: I/O belongs in ctx.run ──────────────────────────────────

    def _refuse(self, *_: Any, **__: Any) -> Any:
        raise RuntimeError(_IO_MESSAGE)

    run_in_executor = _refuse  # pyright: ignore[reportAssignmentType]
    getaddrinfo = _refuse
    getnameinfo = _refuse
    create_connection = _refuse
    create_server = _refuse
    create_datagram_endpoint = _refuse
    create_unix_connection = _refuse
    create_unix_server = _refuse
    sock_recv = _refuse
    sock_sendall = _refuse
    sock_connect = _refuse
    sock_accept = _refuse
    add_reader = _refuse  # pyright: ignore[reportAssignmentType]
    add_writer = _refuse  # pyright: ignore[reportAssignmentType]
    subprocess_exec = _refuse
    subprocess_shell = _refuse
    run_forever = _refuse
    run_until_complete = _refuse
