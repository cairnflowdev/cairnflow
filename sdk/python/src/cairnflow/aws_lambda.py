# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""AWS Lambda adapter: a Function URL or an API Gateway HTTP API (payload v2) serving an
endpoint in ``REQUEST_RESPONSE`` mode.

::

    from cairnflow.aws_lambda import lambda_handler
    handler = lambda_handler([greeter], identity_keys=["publickeyv1_..."])

The replay cache is off by default: Lambda keeps no process (and no affinity) across
turns, so a cache would only miss.
"""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Callable, Coroutine, Iterable
from typing import Any

from .endpoint import Endpoint
from .service import AnyService

#: Response headers the runtime reads; everything else is dropped.
_PASS = ("content-encoding", "cairn-replay", "cairn-replay-cache", "cairn-replay-held", "cairn-affinity")


def async_lambda_handler(
    services: Iterable[AnyService] | Endpoint, **endpoint_options: Any
) -> Callable[[dict[str, Any], Any], Coroutine[Any, Any, dict[str, Any]]]:
    endpoint_options.setdefault("replay_cache", False)
    endpoint = services if isinstance(services, Endpoint) else Endpoint(services, **endpoint_options)
    if endpoint.verifier is None:
        from .serve import warn_unverified

        warn_unverified("lambda_handler()")
    if endpoint.mode != "REQUEST_RESPONSE":
        raise ValueError("Lambda serves REQUEST_RESPONSE only (no long-lived streams)")

    async def handle(event: dict[str, Any], _context: Any = None) -> dict[str, Any]:
        headers = {str(k).lower(): str(v) for k, v in (event.get("headers") or {}).items() if v is not None}
        method = event["requestContext"]["http"]["method"]
        raw = event.get("body")
        if raw is None or method in ("GET", "HEAD"):
            data = b""
        elif event.get("isBase64Encoded"):
            data = base64.b64decode(raw)
        else:
            data = str(raw).encode("utf-8")

        async def body() -> bytes:
            return data

        path = event.get("rawPath") or "/"
        answer = await endpoint.answer(method, path, headers, body)
        out = {"content-type": answer.content_type}
        out.update({k: v for k, v in answer.headers.items() if k in _PASS})
        return {
            "statusCode": answer.status,
            "headers": out,
            "body": base64.b64encode(answer.body).decode("ascii"),
            "isBase64Encoded": True,
        }

    return handle


def lambda_handler(
    services: Iterable[AnyService] | Endpoint, **endpoint_options: Any
) -> Callable[[dict[str, Any], Any], dict[str, Any]]:
    """A synchronous Lambda handler (the Python runtime's default calling convention)."""
    handle = async_lambda_handler(services, **endpoint_options)

    def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
        return asyncio.run(handle(event, context))

    return handler
