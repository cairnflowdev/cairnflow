# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""Python SDK for Cairnflow, a durable execution engine.

::

    from cairnflow import Context, VirtualObject, serve

    orders = VirtualObject("OrderService")

    @orders.handler()
    async def submit(ctx: Context, amount: int) -> dict:
        seen = (await ctx.get("count")) or 0
        await ctx.sleep(1)
        receipt = await ctx.run("charge", charge, amount)
        ctx.set("count", seen + 1)
        return {"receipt": receipt}

    asyncio.run(serve([orders], port=9080))

No runtime dependencies (``BIDI_STREAM`` needs the ``bidi`` extra: ``h2``). Implements
``docs/protocol-v1.md`` on the side of whoever writes the code; the conformance traces in
``conformance/`` check it byte by byte.
"""

from ._invoke import MAX_PROTOCOL_VERSION, MIN_PROTOCOL_VERSION, run_once, run_turn
from .asgi import app
from .client import (
    AlreadyInvoked,
    AttachTimeout,
    AwakeableHandle,
    CairnHttpError,
    CancelAck,
    Client,
    InvocationFailed,
    InvocationHandle,
    InvocationPaused,
    InvocationStatusView,
    LastFailure,
    OutputResult,
    PromiseAck,
    PromiseHandle,
    PromiseView,
    ScheduleLast,
    ScheduleList,
    SchedulesClient,
    ScheduleTarget,
    ScheduleTargetView,
    ScheduleView,
    ServiceClient,
    SignalAck,
    StreamError,
    StreamEvent,
    WorkflowClient,
    connect,
    read_stream,
)
from .context import (
    ArtifactRef,
    Context,
    DurableFuture,
    DurablePromise,
    ScopedContext,
    Span,
    Stream,
    configure_artifacts,
)
from .endpoint import DISCOVERY_V1, INVOCATION_V1, Endpoint
from .identity import IdentityError, IdentityVerifier
from .errors import (
    JournalMismatch,
    PauseError,
    RunFailure,
    SegmentedValueError,
    Suspend,
    TerminalError,
    TurnFailed,
)
from .serve import serve, start
from .service import HandlerOptions, RetryPolicy, Service, VirtualObject, Workflow

__version__ = "0.1.0b0"

__all__ = [
    "AlreadyInvoked",
    "AttachTimeout",
    "AwakeableHandle",
    "CairnHttpError",
    "CancelAck",
    "Client",
    "InvocationFailed",
    "InvocationHandle",
    "InvocationPaused",
    "InvocationStatusView",
    "LastFailure",
    "OutputResult",
    "PromiseAck",
    "PromiseHandle",
    "PromiseView",
    "ScheduleLast",
    "ScheduleList",
    "SchedulesClient",
    "ScheduleTarget",
    "ScheduleTargetView",
    "ScheduleView",
    "ServiceClient",
    "SignalAck",
    "StreamError",
    "StreamEvent",
    "WorkflowClient",
    "connect",
    "read_stream",
    "IdentityError",
    "IdentityVerifier",
    "DISCOVERY_V1",
    "INVOCATION_V1",
    "MAX_PROTOCOL_VERSION",
    "MIN_PROTOCOL_VERSION",
    "ArtifactRef",
    "Context",
    "DurableFuture",
    "DurablePromise",
    "Endpoint",
    "HandlerOptions",
    "JournalMismatch",
    "PauseError",
    "RetryPolicy",
    "RunFailure",
    "ScopedContext",
    "SegmentedValueError",
    "Service",
    "Span",
    "Stream",
    "Suspend",
    "TerminalError",
    "TurnFailed",
    "VirtualObject",
    "Workflow",
    "app",
    "configure_artifacts",
    "run_once",
    "run_turn",
    "serve",
    "start",
]
