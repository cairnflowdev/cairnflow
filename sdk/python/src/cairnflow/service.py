# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""Service definitions: ``Service``, ``VirtualObject``, ``Workflow``.

::

    counter = VirtualObject("Counter")

    @counter.handler()
    async def add(ctx: Context, n: int) -> int: ...

    @counter.handler(kind="shared")
    async def peek(ctx: Context) -> int: ...
"""

from __future__ import annotations

import inspect
import typing
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Literal, TypeVar

ServiceType = str  # "SERVICE" | "VIRTUAL_OBJECT" | "WORKFLOW"

#: A handler function; the decorators return it unchanged, so its type is kept.
H = TypeVar("H", bound=Callable[..., Any])

#: The runtime's default silence cap within a turn; declared explicitly in the manifest.
DEFAULT_INACTIVITY_TIMEOUT_MS = 15 * 60 * 1000


def _ms(v: timedelta | float | int) -> int:
    """Durations are a ``timedelta`` or a number of **seconds**; the manifest wants ms."""
    if isinstance(v, timedelta):
        return int(v.total_seconds() * 1000)
    return int(v * 1000)


@dataclass(frozen=True)
class RetryPolicy:
    """The runtime's retry policy for transient errors."""

    initial_interval: timedelta | float | None = None
    max_interval: timedelta | float | None = None
    #: Backoff multiplier between two attempts.
    exponentiation_factor: float | None = None
    #: Total number of attempts, including the first. ``None`` = unlimited.
    max_attempts: int | None = None
    #: ``"PAUSE"`` or ``"KILL"`` when ``max_attempts`` runs out.
    on_max_attempts: str | None = None


@dataclass(frozen=True)
class HandlerOptions:
    """The contract a service or handler declares in the manifest."""

    #: Cap on how long the SDK may stay silent within a turn.
    inactivity_timeout: timedelta | float | None = None
    retry_policy: RetryPolicy | None = None
    #: How long to keep the journal of a finished invocation.
    journal_retention: timedelta | float | None = None
    #: Minimum level of ``ctx.log`` (``"debug"``, ``"info"``, ``"warn"``, ``"error"``).
    log_level: str | None = None
    #: ``"runtime"`` or ``"off"``.
    trace: str | None = None
    log_flush: timedelta | float | None = None

    def contract(self) -> dict[str, Any]:
        out: dict[str, Any] = {}

        def put(k: str, v: Any) -> None:
            if v is not None:
                out[k] = v

        put("inactivityTimeout", None if self.inactivity_timeout is None else _ms(self.inactivity_timeout))
        put("journalRetention", None if self.journal_retention is None else _ms(self.journal_retention))
        put("trace", self.trace)
        put("logFlushMs", None if self.log_flush is None else _ms(self.log_flush))
        r = self.retry_policy
        if r is not None:
            put("retryPolicyInitialInterval", None if r.initial_interval is None else _ms(r.initial_interval))
            put("retryPolicyMaxInterval", None if r.max_interval is None else _ms(r.max_interval))
            put("retryPolicyExponentiationFactor", r.exponentiation_factor)
            put("retryPolicyMaxAttempts", r.max_attempts)
            put("retryPolicyOnMaxAttempts", r.on_max_attempts)
        return out


@dataclass
class HandlerDef:
    name: str
    fn: Callable[..., Any]
    #: ``EXCLUSIVE`` / ``SHARED`` / ``WORKFLOW``; ``None`` = not declared.
    ty: str | None
    options: HandlerOptions | None
    takes_input: bool
    input_type: Any


def _inspect(fn: Callable[..., Any]) -> tuple[bool, Any]:
    params = [
        p
        for p in inspect.signature(fn).parameters.values()
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    ]
    if len(params) < 1:
        raise TypeError(f"handler {fn.__name__} must take the context as its first parameter")
    if len(params) < 2:
        return False, None
    try:
        hints = typing.get_type_hints(fn)
    except Exception:  # noqa: BLE001 - unresolvable annotations: plain JSON
        hints = {}
    return True, hints.get(params[1].name)


@dataclass
class _Service:
    name: str
    type: ServiceType
    options: HandlerOptions | None = None
    handlers: dict[str, HandlerDef] = field(default_factory=dict)

    def _add(self, fn: Callable[..., Any], name: str | None, ty: str | None, options: HandlerOptions | None) -> None:
        hname = name or fn.__name__
        if hname in self.handlers:
            raise ValueError(f"service {self.name!r} already has a handler {hname!r}")
        takes_input, input_type = _inspect(fn)
        self.handlers[hname] = HandlerDef(hname, fn, ty, options, takes_input, input_type)

    def log_level(self, handler: str) -> str | None:
        h = self.handlers.get(handler)
        if h is not None and h.options is not None and h.options.log_level is not None:
            return h.options.log_level
        return self.options.log_level if self.options is not None else None


class Service(_Service):
    """A stateless service: handlers run in parallel, no key, no state."""

    def __init__(self, name: str, *, options: HandlerOptions | None = None) -> None:
        super().__init__(name, "SERVICE", options)

    def handler(
        self, name: str | None = None, *, options: HandlerOptions | None = None
    ) -> Callable[[H], H]:
        def register(fn: H) -> H:
            self._add(fn, name, None, options)
            return fn

        return register


_KINDS = {"exclusive": "EXCLUSIVE", "shared": "SHARED"}


class VirtualObject(_Service):
    """A keyed object with durable state. ``EXCLUSIVE`` handlers (the default) run one at
    a time per key; ``kind="shared"`` handlers run in parallel and must not write state."""

    def __init__(self, name: str, *, options: HandlerOptions | None = None) -> None:
        super().__init__(name, "VIRTUAL_OBJECT", options)

    def handler(
        self,
        name: str | None = None,
        *,
        kind: Literal["exclusive", "shared"] | None = None,
        options: HandlerOptions | None = None,
    ) -> Callable[[H], H]:
        if kind is not None and kind not in _KINDS:
            raise ValueError(f"kind must be 'exclusive' or 'shared', got {kind!r}")

        def register(fn: H) -> H:
            self._add(fn, name, None if kind is None else _KINDS[kind], options)
            return fn

        return register


class Workflow(_Service):
    """A workflow: exactly one ``main`` handler (runs once per key) plus ``SHARED``
    handlers that read state, signal it or complete its durable promises."""

    def __init__(self, name: str, *, options: HandlerOptions | None = None) -> None:
        super().__init__(name, "WORKFLOW", options)

    def main(
        self, name: str = "run", *, options: HandlerOptions | None = None
    ) -> Callable[[H], H]:
        def register(fn: H) -> H:
            if any(h.ty == "WORKFLOW" for h in self.handlers.values()):
                raise ValueError(f"workflow {self.name!r} already has a main handler")
            self._add(fn, name, "WORKFLOW", options)
            return fn

        return register

    def handler(
        self, name: str | None = None, *, options: HandlerOptions | None = None
    ) -> Callable[[H], H]:
        def register(fn: H) -> H:
            self._add(fn, name, "SHARED", options)
            return fn

        return register


AnyService = _Service
