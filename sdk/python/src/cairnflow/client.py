# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""The cairnflow client: call services from an app or a script through the HTTP ingress.

::

    from cairnflow.client import connect, InvocationFailed

    c = connect("https://cairn.example.com", api_key=os.environ["CAIRN_KEY"])
    await c.service("Orders").call("submit", {"amount": 5})        # waits for the result
    h = await c.service("Orders").send("submit", {"amount": 5})    # fire and forget
    await h.attach(timeout=60)                                     # or h.output(), h.status()
    await h.signal("approve", {"ok": True}); await h.cancel()      # cooperative cancel
    wf = c.workflow("Signup", "u1"); await wf.run(user); await wf.attach()
    await c.awakeable(awk_id).resolve(v); await c.promise("Signup", "u1", "email").resolve(v)

Two parts:

- :func:`connect`: calls to services, objects and workflows; waits for results through
  attach by id; signals, cancel, awakeables, promises, server-side schedules.
- :func:`read_stream`: reads the streams of an invocation (the viewer's side: UIs, CLIs,
  adapters).

Rules shared by every call:

- A ``202`` never surfaces: the client attaches by id
  (``POST /invocations/{id}/attach?wait_ms=25000``) until done or until ``timeout`` runs
  out (:class:`AttachTimeout`; the invocation keeps running).
- Every call carries a generated ``Idempotency-Key``, kept across every attempt, so a
  resend after a ``503`` or a network error does not create a second invocation. Pass
  ``idempotency_key=None`` to send no key; a workflow's ``run`` sends none by default (it
  is already once per key).
- The transport follows the ``leader`` of a ``503`` with the same scheme, waits for the
  ``Retry-After`` of a ``429`` that created nothing, and caps attempts and time
  (:mod:`cairnflow._http`).
- Input and values: leaving ``input``/``value`` out sends no body (the handler sees no
  input); ``None`` sends JSON ``null``.
- Durations are seconds (``float``) or :class:`~datetime.timedelta`.

Streams can only be read at the range's **leader**. :func:`read_stream` follows on its own:

- ``503`` with ``leader`` => ask again at that leader (no ``leader`` => wait, then ask again);
- ``429`` => wait ``Retry-After``, then ask again;
- a ``moved`` event => the leader's term ended: reconnect at the new leader, from the next
  offset (durable stream), or from 0 if a ``reset`` follows (an ephemeral stream is lost with
  the term; the new attempt regenerates it from the start);
- the connection closes without an ``end`` (the maximum lifetime of an SSE connection) =>
  reconnect from the next offset;
- the connection **breaks** midway (the node died) => reconnect from the last record and
  compare: if the stream is a new generation, return ``reset`` and read again from 0; a
  followed ``leader`` node that does not answer => ask the original node again;
- ``end`` => stop; ``410 stream_expired``, ``404``, ... => raise :class:`StreamError`.

Every event, including ``moved`` and ``reset``, is returned to the caller: a UI needs
``reset`` to clear what it has shown.
"""

from __future__ import annotations

import asyncio
import codecs
import json
import math
import re
import ssl
import time
import uuid
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Final, Literal, NotRequired, TypedDict
from urllib.parse import quote, urlsplit

from ._http import (
    Fetch,
    NetworkError,
    Response,
    Sent,
    Transport,
    backoff_ms,
    normalize_url,
    retry_after_s,
    seconds,
)
from .errors import TerminalError

__all__ = [
    "AUTO",
    "AlreadyInvoked",
    "AttachTimeout",
    "AwakeableHandle",
    "CairnHttpError",
    "Client",
    "InvocationFailed",
    "InvocationHandle",
    "InvocationPaused",
    "NetworkError",
    "OutputResult",
    "PromiseHandle",
    "ScheduleTarget",
    "SchedulesClient",
    "ServiceClient",
    "StreamError",
    "StreamEvent",
    "WorkflowClient",
    "connect",
    "read_stream",
]


# ---------------------------------------------------------------------------
# Sentinels
# ---------------------------------------------------------------------------


class _Auto:
    """The default of ``idempotency_key``: generate a random key for this call."""

    def __repr__(self) -> str:
        return "AUTO"


class _Missing:
    """An omitted ``input``/``value``: no request body."""

    def __repr__(self) -> str:
        return "<missing>"


#: ``idempotency_key=AUTO`` (the default): generate a random key, kept across every attempt
#: of the call. ``idempotency_key=None``: send no key. A string: that key.
AUTO: Final = _Auto()
_MISSING: Final = _Missing()

IdempotencyKey = str | None | _Auto
Duration = float | timedelta
LogLevel = Literal["debug", "info", "warn", "error"]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class InvocationFailed(TerminalError):
    """The invocation ended ``failed``. Still a :class:`~cairnflow.errors.TerminalError`
    (``code``, ``message``)."""

    def __init__(self, invocation_id: str, message: str, code: int) -> None:
        super().__init__(message, code)
        self.invocation_id = invocation_id


class InvocationPaused(Exception):
    """The invocation is ``paused`` (out of attempts, waiting for a human): resume it with
    ``POST /invocations/{id}/resume``."""

    def __init__(self, invocation_id: str, message: str, code: int) -> None:
        super().__init__(message)
        self.invocation_id = invocation_id
        self.message = message
        self.code = code


class AlreadyInvoked(Exception):
    """``409 already_invoked``: this workflow key already has a run, ``invocation_id``."""

    def __init__(self, invocation_id: str) -> None:
        super().__init__(f"workflow already has a run: {invocation_id}")
        self.invocation_id = invocation_id


class AttachTimeout(Exception):
    """``timeout`` ran out before the invocation stopped for good. It is still running;
    attach again by id."""

    def __init__(self, invocation_id: str) -> None:
        super().__init__(f"invocation {invocation_id} did not finish before the timeout")
        self.invocation_id = invocation_id


class CairnHttpError(Exception):
    """An HTTP error outside the invoke contract (``400``, ``403``, ``404``, ``503`` past
    the cap, ...)."""

    def __init__(self, message: str, status: int, code: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.status = status
        self.code = code
        #: Set when the invocation was already created and the error happened while
        #: **waiting** for it (attach): do not call again, ``client.invocation(id).attach()``
        #: is how to keep waiting.
        self.invocation_id: str | None = None


class StreamError(Exception):
    """A stream error that cannot be continued past: ``410 stream_expired``, ``404``,
    ``403``, ..."""

    def __init__(self, message: str, status: int, code: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.status = status
        self.code = code


# ---------------------------------------------------------------------------
# Views (the server's JSON, field names as the server sends them)
# ---------------------------------------------------------------------------


class LastFailure(TypedDict):
    """The error of the most recent failed attempt of a still-running invocation."""

    code: int
    #: Cut at 1 KiB.
    message: str
    #: ``ctx.attempt`` of the failed attempt (0 = the first).
    attempt: int
    #: Wall-clock time (epoch ms) when that attempt ended.
    atMs: NotRequired[int]


class InvocationStatusView(TypedDict):
    """``GET /invocations/{id}`` (``InvocationStatus`` in ``docs/api/openapi.json``)."""

    id: str
    #: ``pending``, ``running``, ``suspended``, ``completed``, ``failed``, ``paused``, ...
    status: str
    #: Output bytes (a string holding JSON) when ``completed``.
    output: NotRequired[str]
    code: NotRequired[int]
    message: NotRequired[str]
    #: Present while ``running``, ``suspended`` or ``paused`` once an attempt has failed,
    #: and only when read from the leader.
    lastFailure: NotRequired[LastFailure]
    createdAt: NotRequired[int]
    modifiedAt: NotRequired[int]
    endedAt: NotRequired[int]


class SignalAck(TypedDict):
    """Reply to a signal and to resolving/rejecting an awakeable."""

    id: str
    status: str


class CancelAck(TypedDict):
    id: str
    cancelled: bool
    status: str


class PromiseAck(TypedDict):
    """Reply to resolving/rejecting a promise; ``completed: False`` => it already had a result."""

    service: str
    key: str
    name: str
    completed: bool
    woken: NotRequired[int]


class PromiseView(TypedDict):
    service: str
    key: str
    name: str
    completed: bool
    value: NotRequired[str]
    failure: NotRequired[str]


class ScheduleTargetView(TypedDict):
    service: str
    handler: str
    key: NotRequired[str]


class ScheduleLast(TypedDict):
    """A recent firing. ``status`` only when the run is on the same range as the schedule."""

    at: int
    id: str
    #: ``fired`` or ``skipped``.
    outcome: str
    status: NotRequired[str]


class ScheduleView(TypedDict):
    """A schedule as the server returns it (``Schedule`` in ``docs/api/openapi.json``)."""

    name: str
    id: str
    scope: NotRequired[str]
    cron: NotRequired[str]
    timeZone: NotRequired[str]
    everyMs: NotRequired[int]
    target: ScheduleTargetView
    input: NotRequired[Any]
    limitKey: NotRequired[str]
    #: ``skip`` or ``allow``.
    overlap: str
    paused: bool
    jitterMs: int
    #: The next nominal firing time (ms); ``None`` when paused.
    nextAt: int | None
    #: The actual firing deadline of ``nextAt`` (jitter added).
    fireAt: NotRequired[int]
    #: The next 10 firing times, ``get`` only.
    upcoming: NotRequired[list[int]]
    last: list[ScheduleLast]
    fired: int
    missed: int
    skipped: int
    createdAt: int
    updatedAt: int
    #: ``put``: a new schedule.
    created: NotRequired[bool]
    #: ``put``: the definition changed (``False`` => no-op, ``nextAt`` not moved).
    changed: NotRequired[bool]


class ScheduleList(TypedDict):
    schedules: list[ScheduleView]
    #: The listing is incomplete; ``missing``/``missingRanges``/``truncatedRanges`` say where.
    partial: bool
    missing: list[str]
    missingRanges: list[int]
    #: Ranges whose listing hit a limit (too many schedules, or the read budget ran out).
    truncatedRanges: NotRequired[list[int]]


@dataclass(frozen=True)
class OutputResult:
    """Result of ``output()``, which does not wait: ``ready`` and, when ready, ``value``."""

    ready: bool
    value: Any = None


@dataclass(frozen=True)
class ScheduleTarget:
    """Target of a schedule: service/handler, plus a key for a virtual object."""

    service: str
    handler: str
    key: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _enc(s: str) -> str:
    """``encodeURIComponent``."""
    return quote(s, safe="!~*'()")


def _ms(d: Duration) -> int:
    return int(round(seconds(d) * 1000))


def _decode_output(raw: Any) -> Any:
    """Raw output (a string) => a value: JSON if it parses, otherwise the string itself;
    absent or empty => ``None``."""
    if not isinstance(raw, str) or raw == "":
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return raw


def _int_code(v: Any) -> int | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float) and v.is_integer():
        return int(v)
    return None


def _http_error(res: Response) -> CairnHttpError:
    msg = f"HTTP {res.status}"
    code: str | None = None
    body = res.json_object()
    if isinstance(body.get("error"), str):
        msg = body["error"]
    elif isinstance(body.get("message"), str):
        msg = body["message"]
    if isinstance(body.get("code"), str):
        code = body["code"]
    return CairnHttpError(msg, res.status, code)


@dataclass(frozen=True)
class _Settled:
    done: bool
    value: Any = None
    id: str = ""


def _settle(res: Response) -> _Settled:
    """The body of ``/invoke``, ``/send`` and every attach (``InvokeResponse``): ``200``
    done, ``202``/``429 throttled`` still running (durable), ``500`` failed, ``409`` paused
    or already_invoked. Anything else => :class:`CairnHttpError`."""
    body = res.json_object()
    header = res.header("x-cairn-invocation-id")
    inv_id = header if header is not None else body.get("id") if isinstance(body.get("id"), str) else None
    status = body.get("status")
    code = _int_code(body.get("code"))
    code = 500 if code is None else code
    message = body["message"] if isinstance(body.get("message"), str) else ""
    if inv_id is not None:
        if res.status == 200 and status == "completed":
            return _Settled(True, _decode_output(body.get("output")))
        if (res.status == 202 and status == "waiting") or (res.status == 429 and status == "throttled"):
            return _Settled(False, id=inv_id)
        if res.status == 500 and status == "failed":
            raise InvocationFailed(inv_id, message, code)
        if res.status == 409 and status == "paused":
            raise InvocationPaused(inv_id, message, code)
        if res.status == 409 and status == "already_invoked":
            raise AlreadyInvoked(inv_id)
    raise _http_error(res)


def _carries_id(res: Response) -> bool:
    """The response carries an invocation id (header or body): the ``429 throttled`` of a
    durable invocation."""
    if res.header("x-cairn-invocation-id") is not None:
        return True
    return isinstance(res.json_object().get("id"), str)


def _json(res: Response) -> Any:
    if not res.ok:
        raise _http_error(res)
    return res.json()


def _scope_prefix(scope: str | None) -> str:
    return "" if scope is None else f"/scope/{_enc(scope)}"


def _body(value: Any) -> bytes | None:
    return None if value is _MISSING else json.dumps(value, separators=(",", ":")).encode("utf-8")


# ---------------------------------------------------------------------------
# The client
# ---------------------------------------------------------------------------


class _Core:
    """What every handle shares: the transport and the attach loop."""

    def __init__(self, transport: Transport, attach_wait_ms: int) -> None:
        self.transport = transport
        self.attach_wait_ms = attach_wait_ms

    async def send(
        self,
        path: str,
        method: str = "POST",
        body: Any = _MISSING,
        headers: Mapping[str, str] | None = None,
    ) -> Sent:
        h = dict(headers or {})
        data = _body(body)
        if data is not None:
            h["content-type"] = "application/json"
        return await self.transport.request(path, method=method, headers=h, body=data)

    async def post(self, path: str, body: Any = _MISSING, headers: Mapping[str, str] | None = None) -> Response:
        return (await self.send(path, "POST", body, headers)).res

    async def get(self, path: str) -> Response:
        return (await self.transport.request(path)).res

    async def attach_loop(self, inv_id: str, path: str, timeout: Duration | None) -> Any:
        """Waits through attach (``path`` takes ``?wait_ms=``) until done or until
        ``timeout`` runs out. Attach is a read: a ``503`` past the transport's cap or a
        network error means ask again, with a cap on consecutive failures."""
        deadline = math.inf if timeout is None else time.monotonic() + seconds(timeout)
        failures = 0
        while True:
            left_ms = (deadline - time.monotonic()) * 1000
            if left_ms <= 0:
                raise AttachTimeout(inv_id)
            wait = max(0, math.floor(min(self.attach_wait_ms, left_ms)))
            try:
                res = await self.post(f"{path}?wait_ms={wait}")
            except NetworkError:
                failures += 1
                if failures > 10:
                    raise
                await asyncio.sleep(backoff_ms(failures) / 1000)
                continue
            # Attach is a read: `503` (no leader yet) and a `429` without an id (the
            # per-tenant cap on long-lived connections) both mean "ask again later", not
            # an outcome. Raising here would make the caller call again, i.e. a second
            # invocation.
            if res.status == 503 or (res.status == 429 and not _carries_id(res)):
                failures += 1
                if failures > 10:
                    e = _http_error(res)
                    e.invocation_id = inv_id
                    raise e
                after = retry_after_s(res.header("retry-after"), 0.0)
                await asyncio.sleep(max(backoff_ms(failures), after * 1000) / 1000)
                continue
            failures = 0
            s = _settle(res)
            if s.done:
                return s.value

    def invocation(self, inv_id: str) -> InvocationHandle:
        return InvocationHandle(self, inv_id)


class InvocationHandle:
    """An invocation by id."""

    def __init__(self, core: _Core, inv_id: str) -> None:
        self._core = core
        self.id = inv_id
        self._base = f"/invocations/{_enc(inv_id)}"

    def __repr__(self) -> str:
        return f"InvocationHandle({self.id!r})"

    async def attach(self, *, timeout: Duration | None = None) -> Any:
        """Waits until it stops for good: attach by id, long-polling ``attach_wait`` each
        time. Raises :class:`AttachTimeout` when ``timeout`` runs out."""
        return await self._core.attach_loop(self.id, f"{self._base}/attach", timeout)

    async def output(self) -> OutputResult:
        """The result if done, without waiting (``failed`` and ``paused`` raise)."""
        s = _settle(await self._core.post(f"{self._base}/attach?wait_ms=0"))
        return OutputResult(True, s.value) if s.done else OutputResult(False)

    async def status(self) -> InvocationStatusView:
        v: InvocationStatusView = _json(await self._core.get(self._base))
        return v

    async def signal(self, name: str | int, value: Any = _MISSING) -> SignalAck:
        """Delivers a signal by name (``str``) or index (``int``); an omitted ``value`` is Void."""
        body: dict[str, Any] = {"signal": name}
        if value is not _MISSING:
            body["value"] = value
        v: SignalAck = _json(await self._core.post(f"{self._base}/signal", body))
        return v

    async def cancel(self, *, mode: Literal["signal", "hard"] = "signal") -> CancelAck:
        """Cancels. The default is **cooperative** (``mode="signal"``, ``invoke`` key);
        ``"hard"`` needs ``admin``."""
        q = "?mode=signal" if mode == "signal" else ""
        v: CancelAck = _json(await self._core.post(f"{self._base}/cancel{q}"))
        return v


class ServiceClient:
    """Client of a service or of one virtual object key."""

    def __init__(self, core: _Core, name: str, key: str | None, scope: str | None, workflow: bool) -> None:
        self._core = core
        self.name = name
        self.key = key
        self._prefix = _scope_prefix(scope)
        self._workflow = workflow

    def _path(self, handler: str, send: bool) -> str:
        q = "" if self.key is None else f"?key={_enc(self.key)}"
        s = "/send" if send else ""
        return f"{self._prefix}/invoke/{_enc(self.name)}/{_enc(handler)}{s}{q}"

    def _headers(
        self,
        handler: str,
        idempotency_key: IdempotencyKey,
        limit_key: str | None,
        log_level: LogLevel | None,
        headers: Mapping[str, str] | None,
    ) -> tuple[dict[str, str], bool]:
        """Headers of a call, and whether the key is generated (``auto``). The key is
        generated once and kept across every attempt."""
        auto = not (self._workflow and handler == "run")
        h = {k.lower(): v for k, v in (headers or {}).items()}
        if isinstance(idempotency_key, _Auto):
            key = str(uuid.uuid4()) if auto else None
        else:
            key = idempotency_key
        if key is not None:
            h["idempotency-key"] = key
        if limit_key is not None:
            h["x-cairn-limit-key"] = limit_key
        if log_level is not None:
            h["cairnflow-log-level"] = log_level
        return h, auto

    async def call(
        self,
        handler: str,
        input: Any = _MISSING,
        *,
        idempotency_key: IdempotencyKey = AUTO,
        timeout: Duration | None = None,
        limit_key: str | None = None,
        log_level: LogLevel | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        """Calls ``handler`` and waits for its result.

        :param input: the handler's input; omitted => no body, ``None`` => JSON ``null``.
        :param idempotency_key: :data:`AUTO` (default) generates a key kept across every
            attempt; ``None`` sends no key; a string is used as is.
        :param timeout: cap on waiting for the result (default: wait until done);
            exceeded => :class:`AttachTimeout`.
        :param limit_key: ``x-cairn-limit-key`` (flow control).
        :param log_level: the invocation's ``cairnflow-log-level``.
        :raises InvocationFailed: the invocation failed (a ``TerminalError``).
        :raises InvocationPaused: the invocation is paused.
        :raises AlreadyInvoked: a workflow key that already has a run.
        :raises CairnHttpError: any other HTTP error.
        """
        h, auto = self._headers(handler, idempotency_key, limit_key, log_level, headers)
        sent = await self._core.send(self._path(handler, False), "POST", input, h)
        try:
            s = _settle(sent.res)
        except AlreadyInvoked as e:
            # A workflow's `run` has no key: an earlier send that failed with a network
            # error may have created this very invocation; wait for it instead of
            # reporting a duplicate.
            if not (sent.resent and not auto):
                raise
            s = _Settled(False, id=e.invocation_id)
        if s.done:
            return s.value
        return await self._core.attach_loop(s.id, f"/invocations/{_enc(s.id)}/attach", timeout)

    async def send(
        self,
        handler: str,
        input: Any = _MISSING,
        *,
        idempotency_key: IdempotencyKey = AUTO,
        limit_key: str | None = None,
        log_level: LogLevel | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> InvocationHandle:
        """Fire and forget: returns the handle of the durable invocation. Options as
        :meth:`call` (no ``timeout``)."""
        h, auto = self._headers(handler, idempotency_key, limit_key, log_level, headers)
        sent = await self._core.send(self._path(handler, True), "POST", input, h)
        res = sent.res
        inv_id = res.header("x-cairn-invocation-id")
        # `/send` answers `202` once durable; `200`/`500`/`409` when a call with the same
        # idempotency key already has an outcome: still the same invocation.
        if inv_id is not None and res.status in (200, 202, 409, 429, 500):
            # A `409 paused` with the same key is still a handle; only an
            # `already_invoked` of someone else (a workflow run, not our own resend) raises.
            if res.status == 409 and not (auto or sent.resent):
                if res.json_object().get("status") == "already_invoked":
                    _settle(res)
            return InvocationHandle(self._core, inv_id)
        raise _http_error(res)


class WorkflowClient(ServiceClient):
    """Client of one workflow key: ``run`` and the shared handlers, plus
    :meth:`attach`/:meth:`output` of the run by key."""

    def __init__(self, core: _Core, name: str, key: str, scope: str | None) -> None:
        super().__init__(core, name, key, scope, True)
        self._attach = f"{self._prefix}/workflow/{_enc(name)}/{_enc(key)}/attach"

    async def run(
        self,
        input: Any = _MISSING,
        *,
        idempotency_key: IdempotencyKey = AUTO,
        timeout: Duration | None = None,
        limit_key: str | None = None,
        log_level: LogLevel | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        """Starts the run and waits for its result: :meth:`call` of ``run``. Sends no
        idempotency key unless one is given. A second run => :class:`AlreadyInvoked`."""
        return await self.call(
            "run",
            input,
            idempotency_key=idempotency_key,
            timeout=timeout,
            limit_key=limit_key,
            log_level=log_level,
            headers=headers,
        )

    async def attach(self, *, timeout: Duration | None = None) -> Any:
        """Waits until this key's ``run`` stops for good."""
        return await self._core.attach_loop(f"{self.name}/{self.key}", self._attach, timeout)

    async def output(self) -> OutputResult:
        """Result of the ``run``, without waiting."""
        s = _settle(await self._core.post(f"{self._attach}?wait_ms=0"))
        return OutputResult(True, s.value) if s.done else OutputResult(False)


class AwakeableHandle:
    def __init__(self, core: _Core, awk_id: str) -> None:
        self._core = core
        self.id = awk_id
        self._base = f"/awakeables/{_enc(awk_id)}"

    async def resolve(self, value: Any = _MISSING) -> SignalAck:
        v: SignalAck = _json(await self._core.post(f"{self._base}/resolve", value))
        return v

    async def reject(self, message: str, code: int | None = None) -> SignalAck:
        body: dict[str, Any] = {"message": message}
        if code is not None:
            body["code"] = code
        v: SignalAck = _json(await self._core.post(f"{self._base}/reject", body))
        return v


class PromiseHandle:
    def __init__(self, core: _Core, base: str) -> None:
        self._core = core
        self._base = base

    async def resolve(self, value: Any = _MISSING) -> PromiseAck:
        v: PromiseAck = _json(await self._core.post(f"{self._base}/resolve", value))
        return v

    async def reject(self, message: str, code: int | None = None) -> PromiseAck:
        body: dict[str, Any] = {"message": message}
        if code is not None:
            body["code"] = code
        v: PromiseAck = _json(await self._core.post(f"{self._base}/reject", body))
        return v

    async def peek(self) -> PromiseView:
        v: PromiseView = _json(await self._core.get(self._base))
        return v


class SchedulesClient:
    """``client.schedules``: the server-side schedules of the key's tenant."""

    def __init__(self, core: _Core) -> None:
        self._core = core

    @staticmethod
    def _base(scope: str | None) -> str:
        return f"{_scope_prefix(scope)}/schedules"

    def _one(self, name: str, scope: str | None) -> str:
        return f"{self._base(scope)}/{_enc(name)}"

    async def put(
        self,
        name: str,
        *,
        target: ScheduleTarget | Mapping[str, str],
        cron: str | None = None,
        time_zone: str | None = None,
        every: Duration | None = None,
        input: Any = _MISSING,
        limit_key: str | None = None,
        overlap: Literal["skip", "allow"] | None = None,
        paused: bool | None = None,
        jitter: Duration | None = None,
        scope: str | None = None,
    ) -> ScheduleView:
        """Creates or updates; the same definition => no-op (safe to call at app startup).

        Exactly one of ``cron`` (5 fields, IANA ``time_zone``, default UTC) and ``every``
        (anchored at the epoch). ``overlap``: ``"skip"`` (default for an unkeyed target) or
        ``"allow"`` (the only option for a keyed or scoped target).
        """
        if isinstance(target, ScheduleTarget):
            t: dict[str, str] = {"service": target.service, "handler": target.handler}
            if target.key is not None:
                t["key"] = target.key
        else:
            t = dict(target)
        spec: dict[str, Any] = {}
        if cron is not None:
            spec["cron"] = cron
        if time_zone is not None:
            spec["timeZone"] = time_zone
        if every is not None:
            spec["everyMs"] = _ms(every)
        spec["target"] = t
        if input is not _MISSING:
            spec["input"] = input
        if limit_key is not None:
            spec["limitKey"] = limit_key
        if overlap is not None:
            spec["overlap"] = overlap
        if paused is not None:
            spec["paused"] = paused
        if jitter is not None:
            spec["jitterMs"] = _ms(jitter)
        v: ScheduleView = _json((await self._core.send(self._one(name, scope), "PUT", spec)).res)
        return v

    async def get(self, name: str, *, scope: str | None = None) -> ScheduleView | None:
        """``None`` when absent."""
        res = (await self._core.send(self._one(name, scope), "GET")).res
        if res.status == 404:
            return None
        v: ScheduleView = _json(res)
        return v

    async def delete(self, name: str, *, scope: str | None = None) -> bool:
        """``False`` when absent."""
        res = (await self._core.send(self._one(name, scope), "DELETE")).res
        if res.status == 404:
            return False
        _json(res)
        return True

    async def pause(self, name: str, *, scope: str | None = None) -> ScheduleView:
        v: ScheduleView = _json(await self._core.post(f"{self._one(name, scope)}/pause"))
        return v

    async def resume(self, name: str, *, scope: str | None = None) -> ScheduleView:
        """The next firing is computed from the current time: no catch-up firing."""
        v: ScheduleView = _json(await self._core.post(f"{self._one(name, scope)}/resume"))
        return v

    async def trigger(
        self, name: str, *, idempotency_key: str | None = None, scope: str | None = None
    ) -> InvocationHandle:
        """Fires once right away (ignores ``paused`` and ``overlap``; does not move
        ``nextAt``). The same ``idempotency_key`` => the same run; ``None`` (default)
        generates a key, kept across every attempt."""
        key = idempotency_key if idempotency_key is not None else str(uuid.uuid4())
        r = _json(await self._core.post(f"{self._one(name, scope)}/trigger", headers={"idempotency-key": key}))
        return self._core.invocation(r["id"])

    async def list(self, *, scope: str | None = None) -> ScheduleList:
        v: ScheduleList = _json(await self._core.get(self._base(scope)))
        return v


class Client:
    """Created by :func:`connect`."""

    def __init__(self, transport: Transport, attach_wait_ms: int) -> None:
        self._core = _Core(transport, attach_wait_ms)
        #: Server-side schedules.
        self.schedules = SchedulesClient(self._core)

    @property
    def url(self) -> str:
        return self._core.transport.url

    def service(self, name: str, *, scope: str | None = None) -> ServiceClient:
        """A plain service (no key). ``scope``: the ``/scope/{scope}/...`` path (flow control)."""
        return ServiceClient(self._core, name, None, scope, False)

    def object(self, name: str, key: str, *, scope: str | None = None) -> ServiceClient:
        """A virtual object by key."""
        return ServiceClient(self._core, name, key, scope, False)

    def workflow(self, name: str, key: str, *, scope: str | None = None) -> WorkflowClient:
        """A workflow by key."""
        return WorkflowClient(self._core, name, key, scope)

    def invocation(self, inv_id: str) -> InvocationHandle:
        return self._core.invocation(inv_id)

    def awakeable(self, awk_id: str) -> AwakeableHandle:
        return AwakeableHandle(self._core, awk_id)

    def promise(self, service: str, key: str, name: str, *, scope: str | None = None) -> PromiseHandle:
        base = f"{_scope_prefix(scope)}/promises/{'/'.join(_enc(x) for x in (service, key, name))}"
        return PromiseHandle(self._core, base)


def connect(
    url: str,
    api_key: str | None = None,
    *,
    headers: Mapping[str, str] | None = None,
    follow_leader: bool = True,
    max_attempts: int = 8,
    max_elapsed: Duration = 30.0,
    attach_wait: Duration = 25.0,
    timeout: Duration | None = None,
    connect_timeout: Duration | None = 10.0,
    ssl_context: ssl.SSLContext | None = None,
    fetch: Fetch | None = None,
) -> Client:
    """Creates a client.

    :param url: ingress port of any node (``http://host:port`` or ``host:port``).
    :param api_key: API key (``cf_...``); the tenant is the key's tenant.
    :param headers: added to every request.
    :param follow_leader: follow the ``leader`` of a ``503``.
    :param max_attempts: cap on the attempts of one request.
    :param max_elapsed: time cap of one request, waits included.
    :param attach_wait: ``wait_ms`` of each attach (default 25 s): below the 30 s idle cap
        of common load balancers; the server clamps it to ``--attach-max-wait-ms``.
    :param timeout: cap on one HTTP exchange (default: none; an attach holds its request
        open for up to ``attach_wait``).
    :param connect_timeout: cap on opening one connection.
    :param ssl_context: TLS settings for ``https://`` (default: the system's trust store).
    :param fetch: replacement for the HTTP client (tests).
    """
    h = {k.lower(): v for k, v in (headers or {}).items()}
    if api_key is not None:
        h["authorization"] = f"Bearer {api_key}"
    transport = Transport(
        url,
        headers=h,
        follow_leader=follow_leader,
        max_attempts=max_attempts,
        max_elapsed=max_elapsed,
        timeout=timeout,
        connect_timeout=connect_timeout,
        ssl_context=ssl_context,
        fetch=fetch,
    )
    return Client(transport, _ms(attach_wait))


# ---------------------------------------------------------------------------
# read_stream
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StreamEvent:
    """A stream event: the SSE contract of ``GET /invocations/{id}/streams/{name}``."""

    #: ``record``, ``gap``, ``superseded``, ``end``, ``moved`` or ``reset``.
    kind: str
    offset: int | None = None
    #: ``record``: a durable record.
    durable: bool | None = None
    step: int | None = None
    attempt: int | None = None
    k: int | None = None
    #: ``record``: the writer's JSON payload.
    data: Any = None
    #: ``gap``: number of records dropped or overwritten.
    dropped: int | None = None
    #: ``moved``: the new leader's ingress port, if known.
    leader: str | None = None
    #: ``end``: the next offset (the stream's number of records).
    next: int | None = None
    #: Every field of the event's JSON, as sent.
    fields: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)


@dataclass
class _RawEvent:
    event: str
    id: str | None
    data: str


async def _parse_sse(res: Response) -> AsyncIterator[_RawEvent]:
    """Splits an SSE body into events; drops comment lines (``: keepalive``)."""
    dec = codecs.getincrementaldecoder("utf-8")(errors="replace")
    buf = ""
    async for chunk in res.iter_chunks():
        buf = (buf + dec.decode(chunk)).replace("\r\n", "\n")
        while (at := buf.find("\n\n")) >= 0:
            block, buf = buf[:at], buf[at + 2 :]
            ev = _RawEvent("message", None, "")
            any_ = False
            for line in block.split("\n"):
                if line.startswith(":"):
                    continue
                name, colon, value = line.partition(":")
                if colon and value.startswith(" "):
                    value = value[1:]
                if name == "event":
                    ev.event = value
                    any_ = True
                elif name == "id":
                    ev.id = value
                elif name == "data":
                    ev.data += f"\n{value}" if ev.data else value
                    any_ = True
            if any_:
                yield ev


def _opt_int(v: Any) -> int | None:
    return v if isinstance(v, int) and not isinstance(v, bool) else None


def _to_event(raw: _RawEvent) -> StreamEvent:
    d: dict[str, Any] = {}
    try:
        v = json.loads(raw.data)
        if isinstance(v, dict):
            d = v
    except ValueError:
        pass  # Not JSON: keep `kind`, drop the fields.
    durable = d.get("durable")
    leader = d.get("leader")
    return StreamEvent(
        kind=raw.event,
        offset=_opt_int(d.get("offset")),
        durable=durable if isinstance(durable, bool) else None,
        step=_opt_int(d.get("step")),
        attempt=_opt_int(d.get("attempt")),
        k=_opt_int(d.get("k")),
        data=d.get("data"),
        dropped=_opt_int(d.get("dropped")),
        leader=leader if isinstance(leader, str) else None,
        next=_opt_int(d.get("next")),
        fields=d,
    )


def _stream_error(res: Response) -> StreamError:
    msg = f"HTTP {res.status}"
    code: str | None = None
    body = res.json_object()
    if isinstance(body.get("error"), str):
        msg = body["error"]
    if isinstance(body.get("code"), str):
        code = body["code"]
    return StreamError(msg, res.status, code)


def _key_of(e: StreamEvent) -> str:
    return json.dumps([e.step, e.attempt, e.k, e.data], separators=(",", ":"))


async def read_stream(
    base: str,
    invocation_id: str,
    name: str,
    *,
    offset: int = 0,
    headers: Mapping[str, str] | None = None,
    retries: int = 10,
    fetch: Fetch | None = None,
) -> AsyncIterator[StreamEvent]:
    """Reads stream ``name`` of invocation ``invocation_id`` from node ``base`` (URL or
    ``host:port`` of the ingress port); see the module doc for how it follows the leader.

    :param offset: starting offset.
    :param headers: extra headers, e.g. ``authorization: Bearer <invoke key>``.
    :param retries: maximum consecutive failures before giving up.

    An async generator: stop early with ``break`` inside
    ``async with contextlib.aclosing(read_stream(...)) as events``, or cancel the task.
    """
    home = normalize_url(base)
    node = home
    # `503`/`429`/network errors of one connection attempt: the shared transport layer.
    # The loop below handles the stream's part: reconnects, `moved`, new generations.
    transport = Transport(
        home,
        headers={"accept": "text/event-stream", **{k.lower(): v for k, v in (headers or {}).items()}},
        max_attempts=retries + 1,
        fetch=fetch,
    )
    nxt = offset
    failures = 0
    # The last record returned, for reconnecting after the connection **breaks**: if the
    # node died and was rebuilt (or the range moved to another node), an ephemeral stream
    # is a new generation (offsets from 0 again, the rerun regenerates steps from the
    # start), and asking `from=next` would lose the start of the new generation. So
    # reconnect from the last record itself and compare: match => same generation, carry
    # on; different (or the stream is shorter) => return `reset` and read again from 0.
    # A clean close (the connection's maximum lifetime) needs no comparison.
    last: tuple[int, str] | None = None
    verify = False

    async def failed(e: BaseException) -> None:
        nonlocal failures
        failures += 1
        if failures > retries:
            raise e
        await asyncio.sleep(backoff_ms(failures) / 1000)

    while True:
        path = f"/invocations/{_enc(invocation_id)}/streams/{_enc(name)}?from={nxt}"
        try:
            sent = await transport.request(path, node=node, stream=True)
        except NetworkError as e:
            node = home
            await failed(e)
            continue
        res = sent.res
        node = sent.node
        # The transport already followed `leader` and waited `Retry-After` up to its cap.
        if res.status in (503, 429):
            await res.aclose()
            await failed(_stream_error(res))
            continue
        if not res.ok:
            await res.aclose()
            raise _stream_error(res)
        moved: str | None = None
        reborn = False
        try:
            async for raw in _parse_sse(res):
                failures = 0
                e = _to_event(raw)
                # The last record has left the buffer: a `gap` covers it. Generations
                # cannot be compared and nothing proves a new generation: treat it as the
                # same generation; the reader still sees the `gap`.
                if (
                    verify
                    and last is not None
                    and e.kind == "gap"
                    and e.offset is not None
                    and e.offset <= last[0] < e.offset + (e.dropped or 0)
                ):
                    verify = False
                if verify and last is not None and e.kind in ("record", "end"):
                    verify = False
                    if e.kind == "record" and e.offset == last[0] and _key_of(e) == last[1]:
                        nxt = last[0] + 1
                        continue  # overlap: already returned
                    reborn = True
                    break
                if raw.id is not None and re.fullmatch(r"[0-9]+", raw.id):
                    nxt = int(raw.id) + 1
                elif e.kind == "gap" and e.offset is not None:
                    nxt = e.offset + (e.dropped or 0)
                if e.kind == "record" and e.offset is not None:
                    last = (e.offset, _key_of(e))
                yield e
                if e.kind == "end":
                    return
                if e.kind == "moved":
                    moved = e.leader
                if e.kind == "reset":
                    nxt = 0
                    last = None
        except NetworkError as err:
            # The SSE body broke midway (the node died): reconnect from the last record
            # and compare generations.
            if last is not None:
                verify = True
                nxt = last[0]
            await failed(err)
            continue
        finally:
            await res.aclose()
        if reborn:
            yield StreamEvent(kind="reset")
            nxt = 0
            last = None
            continue
        # Same rule as the transport's `503 + leader`: do not follow a leader with a
        # different scheme (a Bearer key does not go over `http://` to the internal address
        # the leader advertises when the client speaks `https://`). Skip => keep reading at
        # the original node.
        if moved:
            to = normalize_url(moved)
            node = to if urlsplit(to).scheme == urlsplit(home).scheme else home
        # Closed without `end` (maximum lifetime, or a leader change): reconnect from `next`.
