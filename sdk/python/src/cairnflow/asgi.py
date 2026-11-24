# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""An ASGI application: run an endpoint under uvicorn, hypercorn or any ASGI server.

::

    app = cairnflow.app([greeter])          # uvicorn module:app --port 9080

``REQUEST_RESPONSE`` works on any ASGI server. ``BIDI_STREAM`` needs a server that speaks
h2c with prior knowledge on the same port as HTTP/1.1 and streams both bodies at once:
hypercorn does (``hypercorn module:app --bind 127.0.0.1:9080``); uvicorn has no HTTP/2.
"""

from __future__ import annotations

import asyncio
import json
import logging
import zlib
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from ._bidi import FrameSplitter, QueueStream, run_bidi
from ._wire import FrameError
from .endpoint import INVOCATION_V1, MAX_REQUEST_BODY, BodyTooLarge, Endpoint
from .service import AnyService

log = logging.getLogger("cairnflow")

Scope = dict[str, Any]
Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]


#: ``BIDI_STREAM``: how long the SDK keeps its half open after the turn, waiting for the
#: runtime to end the request body first (below the runtime's 2 s close grace).
BIDI_CLOSE_WAIT_S = 1.5


def app(
    services: Iterable[AnyService] | Endpoint, *, bidi_close_wait: float = BIDI_CLOSE_WAIT_S, **endpoint_options: Any
) -> Callable[[Scope, Receive, Send], Awaitable[None]]:
    """``bidi_close_wait``: see :data:`BIDI_CLOSE_WAIT_S`. Some ASGI servers (hypercorn 0.18)
    drop the whole HTTP/2 connection when request data arrives on a stream whose response
    already ended; the runtime ends its side right after reading the turn's last frame, so
    waiting for that first keeps the connection (and its other streams) alive."""
    endpoint = services if isinstance(services, Endpoint) else Endpoint(services, **endpoint_options)
    if endpoint.verifier is None:
        from .serve import warn_unverified

        warn_unverified("app()")

    async def asgi(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            while True:
                msg = await receive()
                if msg["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif msg["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        if scope["type"] != "http":
            return
        headers = {}
        for k, v in scope.get("headers", []):
            key = k.decode("latin-1").lower()
            val = v.decode("latin-1")
            headers[key] = f"{headers[key]}, {val}" if key in headers else val
        root = scope.get("root_path", "")
        path = scope["path"]
        if root and path.startswith(root):
            path = path[len(root) :] or "/"

        if endpoint.mode == "BIDI_STREAM" and scope["method"] == "POST" and endpoint.route("POST", path) is not None:
            await _bidi(endpoint, path, scope["path"], headers, receive, send, bidi_close_wait)
            return

        async def body() -> bytes:
            out = bytearray()
            while True:
                msg = await receive()
                if msg["type"] == "http.disconnect":
                    break
                out += msg.get("body", b"")
                if len(out) > MAX_REQUEST_BODY:
                    raise BodyTooLarge()
                if not msg.get("more_body", False):
                    break
            return bytes(out)

        answer = await endpoint.answer(scope["method"], path, headers, body, signed_path=scope["path"])
        out_headers = [
            (b"content-type", answer.content_type.encode()),
            (b"content-length", str(len(answer.body)).encode()),
        ]
        out_headers += [(k.encode(), v.encode()) for k, v in answer.headers.items()]
        await send({"type": "http.response.start", "status": answer.status, "headers": out_headers})
        await send({"type": "http.response.body", "body": answer.body})

    return asgi


async def _bidi(
    endpoint: Endpoint,
    path: str,
    signed_path: str,
    headers: dict[str, str],
    receive: Receive,
    send: Send,
    close_wait: float,
) -> None:
    """One ``BIDI_STREAM`` invocation over ASGI: response headers at once, then the request
    body (runtime frames) and the response body (SDK frames) both stream, in parallel."""
    routed = endpoint.route("POST", path)
    assert routed is not None
    svc, h = routed
    opened = endpoint.open_bidi(headers, signed_path)
    if isinstance(opened, tuple):
        status, message = opened
        body = json.dumps({"error": message}).encode()
        hs = [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]
        await send({"type": "http.response.start", "status": status, "headers": hs})
        await send({"type": "http.response.body", "body": body})
        return
    # Headers before reading any byte of the body: the runtime takes them as "the stream is open".
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", INVOCATION_V1.encode())]})
    outbox: asyncio.Queue[bytes | None] = asyncio.Queue()
    ended = [False]

    def write(data: bytes) -> None:
        if not ended[0]:
            outbox.put_nowait(data)

    def end() -> None:
        if not ended[0]:
            ended[0] = True
            outbox.put_nowait(None)

    io = QueueStream(write, end)
    gunzip = zlib.decompressobj(wbits=31) if headers.get("content-encoding", "").strip().lower() == "gzip" else None

    async def read() -> None:
        split = FrameSplitter()
        total = 0
        try:
            while True:
                msg = await receive()
                if msg["type"] == "http.disconnect":
                    return
                data = msg.get("body", b"")
                if gunzip is not None:
                    data = gunzip.decompress(data)
                    total += len(data)
                    if total > MAX_REQUEST_BODY:
                        return
                for f in split.push(data):
                    io.offer(f)
                if not msg.get("more_body", False):
                    return
        except (FrameError, zlib.error):
            return  # a broken byte stream: no way to find the frame boundary again
        finally:
            io.finish()

    reader = asyncio.get_running_loop().create_task(read())

    async def write_out() -> None:
        while (chunk := await outbox.get()) is not None:
            await send({"type": "http.response.body", "body": chunk, "more_body": True})
        # Let the runtime end the request body first (see `bidi_close_wait`).
        if not reader.done():
            await asyncio.wait({reader}, timeout=close_wait)
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    writer = asyncio.get_running_loop().create_task(write_out())
    try:
        await run_bidi(
            h.fn,
            endpoint.gate_bidi(opened, io, end),
            endpoint.wire_of(headers.get("content-type")),
            endpoint.values,
            clock=endpoint.clock,
            log_level=svc.log_level(h.name),
            input_type=h.input_type,
            takes_input=h.takes_input,
        )
    except Exception:  # noqa: BLE001 - an SDK bug; no channel left to report on
        log.exception("cairnflow: run_bidi failed")
    finally:
        end()
        await writer
        reader.cancel()
