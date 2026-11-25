# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""A small HTTP/1.1 server on ``asyncio.start_server``: run an endpoint with no
dependency at all. For production behind your own stack, use the ASGI app
(``cairnflow.app``) with uvicorn or hypercorn instead.

::

    import asyncio, cairnflow
    asyncio.run(cairnflow.serve([greeter], port=9080))
"""

from __future__ import annotations

import asyncio
import logging
import warnings
from collections.abc import Iterable
from typing import Any

from .endpoint import MAX_REQUEST_BODY, Answer, BodyTooLarge, Endpoint
from .service import AnyService

log = logging.getLogger("cairnflow")

_MAX_HEADER = 64 << 10
_REASONS = {200: "OK", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found", 409: "Conflict",
            413: "Payload Too Large", 415: "Unsupported Media Type", 500: "Internal Server Error"}


_warned = False


def warn_unverified(who: str) -> None:
    """Said once per process: an endpoint without ``identity_keys`` runs anyone's journal."""
    global _warned
    if _warned:
        return
    _warned = True
    warnings.warn(
        f"{who} has no identity_keys: anyone who can reach this endpoint can run your handlers on a forged "
        "journal, input and state (real side effects on fake data). Without them the REQUEST_RESPONSE replay "
        "cache stays off, and segmented values (wire v6, which make the endpoint fetch segment URLs from "
        "Start.segment_urls) are only served when segment_origins lists the runtime's value read origins. "
        "Set identity_keys to the kid listed at the runtime's GET /identity",
        UserWarning,
        stacklevel=3,
    )


class _BadRequest(Exception):
    pass


async def _read_head(reader: asyncio.StreamReader) -> tuple[str, str, dict[str, str]] | None:
    try:
        raw = await reader.readuntil(b"\r\n\r\n")
    except asyncio.IncompleteReadError as e:
        if e.partial.strip():
            raise _BadRequest("truncated request head") from None
        return None
    except asyncio.LimitOverrunError:
        raise _BadRequest("request head too large") from None
    lines = raw.decode("latin-1").split("\r\n")
    parts = lines[0].split(" ")
    if len(parts) != 3 or not parts[2].startswith("HTTP/1."):
        raise _BadRequest("malformed request line")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if not line:
            continue
        name, sep, value = line.partition(":")
        if not sep:
            raise _BadRequest("malformed header line")
        key = name.strip().lower()
        headers[key] = f"{headers[key]}, {value.strip()}" if key in headers else value.strip()
    return parts[0].upper(), parts[1], headers


async def _read_body(reader: asyncio.StreamReader, headers: dict[str, str]) -> bytes:
    if "chunked" in headers.get("transfer-encoding", "").lower():
        out = bytearray()
        while True:
            size_line = await reader.readuntil(b"\r\n")
            size = int(size_line.split(b";")[0].strip(), 16)
            if size == 0:
                await reader.readuntil(b"\r\n")  # trailer end (no trailers expected)
                return bytes(out)
            if len(out) + size > MAX_REQUEST_BODY:
                raise BodyTooLarge()
            out += await reader.readexactly(size)
            await reader.readexactly(2)
    length = int(headers.get("content-length", "0") or "0")
    if length > MAX_REQUEST_BODY:
        raise BodyTooLarge()
    return await reader.readexactly(length) if length else b""


def _has_body(headers: dict[str, str]) -> bool:
    """Whether the request carries a body (any ``transfer-encoding``, or a length that is not 0)."""
    if "transfer-encoding" in headers:
        return True
    return headers.get("content-length", "").strip() not in ("", "0")


def _response(answer: Answer, keep_alive: bool) -> bytes:
    head = [
        f"HTTP/1.1 {answer.status} {_REASONS.get(answer.status, 'Unknown')}",
        f"content-type: {answer.content_type}",
        f"content-length: {len(answer.body)}",
        f"connection: {'keep-alive' if keep_alive else 'close'}",
    ]
    head += [f"{k}: {v}" for k, v in answer.headers.items()]
    return ("\r\n".join(head) + "\r\n\r\n").encode("latin-1") + answer.body


async def _connection(endpoint: Endpoint, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while True:
            try:
                head = await _read_head(reader)
            except _BadRequest as e:
                writer.write(_response(Answer(400, "text/plain", str(e).encode()), False))
                break
            if head is None:
                break
            method, target, headers = head
            path = target.split("?", 1)[0]
            consumed: list[bool] = []

            async def body() -> bytes:
                consumed.append(True)
                return await _read_body(reader, headers)

            try:
                answer = await endpoint.answer(method, path, headers, body)
            except Exception as e:  # noqa: BLE001 - one request's failure, not the server's
                log.exception("cairnflow: request %s %s failed", method, path)
                answer = Answer(500, "application/json", f'{{"error":"{type(e).__name__}"}}'.encode())
            keep = headers.get("connection", "").lower() != "close"
            if not consumed and _has_body(headers):
                # The body was not read (an early 404/415): close the connection rather than
                # read up to MAX_REQUEST_BODY of a request nothing has verified.
                keep = False
            writer.write(_response(answer, keep))
            await writer.drain()
            if not keep:
                break
    except (ConnectionError, asyncio.IncompleteReadError):
        pass
    finally:
        writer.close()


async def start(
    services: Iterable[AnyService] | Endpoint,
    *,
    host: str = "127.0.0.1",
    port: int = 9080,
    **endpoint_options: Any,
) -> asyncio.Server:
    """Starts serving and returns the ``asyncio.Server`` (``port=0`` picks a free port)."""
    endpoint = services if isinstance(services, Endpoint) else Endpoint(services, **endpoint_options)
    if endpoint.verifier is None:
        warn_unverified("serve()")
    if endpoint.mode != "REQUEST_RESPONSE":
        from ._h2 import start_bidi

        return await start_bidi(endpoint, host=host, port=port)
    return await asyncio.start_server(
        lambda r, w: _connection(endpoint, r, w), host, port, limit=_MAX_HEADER
    )


async def serve(
    services: Iterable[AnyService] | Endpoint,
    *,
    host: str = "127.0.0.1",
    port: int = 9080,
    **endpoint_options: Any,
) -> None:
    """Serves forever."""
    server = await start(services, host=host, port=port, **endpoint_options)
    addr = server.sockets[0].getsockname() if server.sockets else (host, port)
    log.info("cairnflow endpoint listening on %s:%s", addr[0], addr[1])
    async with server:
        await server.serve_forever()
