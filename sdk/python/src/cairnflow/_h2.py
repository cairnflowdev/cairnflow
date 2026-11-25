# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""``BIDI_STREAM`` transport: h2c with prior knowledge, on the same port as HTTP/1.1.

``/discover`` always runs over HTTP/1.1 (section 4), so one port serves both: the first
bytes of a connection are compared with the 24-byte HTTP/2 preface. Needs the optional
dependency ``h2`` (``pip install cairnflow[bidi]``).
"""

from __future__ import annotations

import asyncio
import json
import logging
import zlib
from typing import Any

from ._bidi import FrameSplitter, QueueStream, run_bidi
from ._wire import FrameError
from .endpoint import DISCOVERY_V1, INVOCATION_V1, MAX_REQUEST_BODY, Endpoint
from .identity import DISCOVER_JTI

try:
    from h2.config import H2Configuration
    from h2.connection import H2Connection
    from h2.errors import ErrorCodes
    from h2.events import (
        ConnectionTerminated,
        DataReceived,
        RequestReceived,
        StreamEnded,
        StreamReset,
        WindowUpdated,
    )
    from h2.exceptions import ProtocolError, StreamClosedError
except ImportError as e:  # pragma: no cover - exercised only without the extra
    raise ImportError(
        "BIDI_STREAM needs the optional dependency h2: pip install 'cairnflow[bidi]' "
        "(or serve with protocol_mode='REQUEST_RESPONSE')"
    ) from e

log = logging.getLogger("cairnflow")

H2_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"


class _Stream:
    def __init__(self, session: _Session, sid: int, gzipped: bool) -> None:
        self.session = session
        self.sid = sid
        self.out = bytearray()
        self.want_end = False
        self.closed = False
        #: The runtime ended (or reset) its side.
        self.remote_ended = False
        self.split = FrameSplitter()
        self.gunzip = zlib.decompressobj(wbits=31) if gzipped else None
        self.received = 0
        self.io = QueueStream(self.write, self.end)
        self.task: asyncio.Task[None] | None = None

    def write(self, data: bytes) -> None:
        if self.closed:
            return
        self.out += data
        self.session.pump(self)

    def end(self) -> None:
        if self.closed:
            return
        self.want_end = True
        self.session.pump(self)

    def feed(self, data: bytes) -> None:
        if self.gunzip is not None:
            data = self.gunzip.decompress(data)
        self.received += len(data)
        if self.received > MAX_REQUEST_BODY and self.gunzip is not None:
            raise FrameError(f"stream exceeds {MAX_REQUEST_BODY} bytes after decompression")
        for f in self.split.push(data):
            self.io.offer(f)


class _Session:
    def __init__(self, endpoint: Endpoint, writer: asyncio.StreamWriter) -> None:
        self.endpoint = endpoint
        self.writer = writer
        self.conn = H2Connection(H2Configuration(client_side=False, header_encoding="utf-8"))
        self.streams: dict[int, _Stream] = {}

    def flush(self) -> None:
        data = self.conn.data_to_send()
        if data and not self.writer.is_closing():
            self.writer.write(data)

    def pump(self, s: _Stream) -> None:
        """Sends what flow control allows; ends the stream once everything is out."""
        try:
            while s.out:
                window = min(self.conn.local_flow_control_window(s.sid), self.conn.max_outbound_frame_size)
                if window <= 0:
                    break
                chunk = bytes(s.out[:window])
                del s.out[:window]
                self.conn.send_data(s.sid, chunk)
            if not s.out and s.want_end and not s.closed:
                s.closed = True
                self.conn.end_stream(s.sid)
        except (StreamClosedError, ProtocolError):
            s.closed = True
            s.out.clear()
        self.forget(s)
        self.flush()

    def forget(self, s: _Stream) -> None:
        """Drops a stream both sides have finished with (long-lived connections)."""
        if s.closed and s.remote_ended:
            self.streams.pop(s.sid, None)

    async def run(self, reader: asyncio.StreamReader, prefix: bytes) -> None:
        self.conn.initiate_connection()
        self.flush()
        data = prefix
        try:
            while True:
                if data:
                    try:
                        events = self.conn.receive_data(data)
                    except ProtocolError:
                        self.flush()
                        return
                    for ev in events:
                        self.handle(ev)
                    self.flush()
                    await self.writer.drain()
                data = await reader.read(65536)
                if not data:
                    return
        except (ConnectionError, asyncio.IncompleteReadError):
            return
        finally:
            for s in self.streams.values():
                s.closed = True
                s.io.finish()
            self.writer.close()

    def respond(self, sid: int, status: int, content_type: str, body: bytes) -> None:
        self.conn.send_headers(sid, [(":status", str(status)), ("content-type", content_type)])
        self.conn.send_data(sid, body, end_stream=True)

    def handle(self, ev: Any) -> None:
        if isinstance(ev, RequestReceived):
            self.request(ev)
        elif isinstance(ev, DataReceived):
            self.conn.acknowledge_received_data(ev.flow_controlled_length, ev.stream_id)
            s = self.streams.get(ev.stream_id)
            if s is not None and not s.closed:
                try:
                    s.feed(ev.data)
                except (FrameError, zlib.error):
                    # A broken byte stream: no way to find the frame boundary again.
                    s.closed = True
                    s.io.finish()
                    self.conn.reset_stream(ev.stream_id, ErrorCodes.PROTOCOL_ERROR)
        elif isinstance(ev, StreamEnded):
            s = self.streams.get(ev.stream_id)
            if s is not None:
                s.remote_ended = True
                s.io.finish()
                self.forget(s)
        elif isinstance(ev, StreamReset):
            # The runtime closing the stream is how it says "turn over"; not an error.
            s = self.streams.pop(ev.stream_id, None)
            if s is not None:
                s.closed = True
                s.remote_ended = True
                s.io.finish()
        elif isinstance(ev, WindowUpdated):
            targets = list(self.streams.values()) if ev.stream_id == 0 else [self.streams.get(ev.stream_id)]
            for s in targets:
                if s is not None and (s.out or s.want_end):
                    self.pump(s)
        elif isinstance(ev, ConnectionTerminated):
            for s in self.streams.values():
                s.closed = True
                s.io.finish()

    def request(self, ev: RequestReceived) -> None:
        headers: dict[str, str] = {}
        for k, v in ev.headers:
            headers[str(k).lower()] = str(v)
        sid = ev.stream_id
        method = headers.get(":method", "GET")
        path = headers.get(":path", "/").split("?", 1)[0]
        ep = self.endpoint
        if method == "GET" and path == "/discover":
            who = ep.identify(headers, path, DISCOVER_JTI)
            if isinstance(who, str):
                self.respond(sid, 401, "application/json", json.dumps({"error": who}).encode())
                return
            self.respond(sid, 200, DISCOVERY_V1, json.dumps(ep.manifest(), separators=(",", ":")).encode())
            return
        routed = ep.route(method, path)
        if routed is None:
            self.respond(sid, 404, "application/json", json.dumps({"error": f"no route for {method} {path}"}).encode())
            return
        svc, h = routed
        opened = ep.open_bidi(headers, path)
        if isinstance(opened, tuple):
            status, message = opened
            self.respond(sid, status, "application/json", json.dumps({"error": message}).encode())
            return
        # Response headers go out at once, before reading any byte of the body: the
        # runtime takes them as "the stream is open" (section 4).
        self.conn.send_headers(sid, [(":status", "200"), ("content-type", INVOCATION_V1)])
        s = _Stream(self, sid, headers.get("content-encoding", "").strip().lower() == "gzip")
        self.streams[sid] = s
        if ev.stream_ended is not None:
            s.remote_ended = True
            s.io.finish()
        wire = ep.wire_of(headers.get("content-type"))
        io = ep.gate_bidi(opened, s.io, lambda: self.refuse(s))

        async def go() -> None:
            try:
                await run_bidi(
                    h.fn,
                    io,
                    wire,
                    ep.values,
                    clock=ep.clock,
                    log_level=svc.log_level(h.name),
                    input_type=h.input_type,
                    takes_input=h.takes_input,
                )
            except Exception:  # noqa: BLE001 - an SDK bug; no channel left to report on
                log.exception("cairnflow: run_bidi failed")
                self.refuse(s)
            finally:
                self.forget(s)

        s.task = asyncio.get_running_loop().create_task(go())

    def refuse(self, s: _Stream) -> None:
        if s.closed:
            return
        s.closed = True
        s.remote_ended = True
        s.io.finish()
        self.streams.pop(s.sid, None)
        try:
            self.conn.reset_stream(s.sid, ErrorCodes.REFUSED_STREAM)
        except (StreamClosedError, ProtocolError):
            pass
        self.flush()


async def _connection(endpoint: Endpoint, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    from .serve import _connection as h1

    seen = b""
    # One differing byte is enough to conclude h1: "PRI " and "POST" differ at byte 2.
    while len(seen) < len(H2_PREFACE) and H2_PREFACE.startswith(seen):
        chunk = await reader.read(65536)
        if not chunk:
            writer.close()
            return
        seen += chunk
    if seen.startswith(H2_PREFACE):
        await _Session(endpoint, writer).run(reader, seen)
        return
    # HTTP/1.1: hand over a reader that replays what was already read.
    replay = asyncio.StreamReader(limit=64 << 10)
    replay.feed_data(seen)

    async def pump() -> None:
        try:
            while chunk := await reader.read(65536):
                replay.feed_data(chunk)
        except ConnectionError:
            pass
        finally:
            replay.feed_eof()

    task = asyncio.get_running_loop().create_task(pump())
    try:
        await h1(endpoint, replay, writer)
    finally:
        task.cancel()


async def start_bidi(endpoint: Endpoint, *, host: str, port: int) -> asyncio.Server:
    return await asyncio.start_server(lambda r, w: _connection(endpoint, r, w), host, port, limit=64 << 10)
