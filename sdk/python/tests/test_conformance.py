# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""The conformance runner: a fake runtime that replays every trace of ``conformance/``
into this SDK over real HTTP and compares every frame the SDK sends, by the rules of
``conformance/README.md``.

The response is split into frames by this file's own parser, not the SDK's codec, so a
codec bug cannot hide itself.
"""

from __future__ import annotations

import asyncio
import base64
import json
import unittest
from pathlib import Path
from urllib.parse import urlsplit
from typing import TYPE_CHECKING, Any

import _path  # noqa: F401  (src/ on sys.path)
import conformance_handlers
from cairnflow import MAX_PROTOCOL_VERSION, Endpoint, app, start
from cairnflow._wire import SuspensionTree, encode_suspension, tree_satisfied
from cairnflow.aws_lambda import async_lambda_handler
from conformance_handlers import conformance
from signer import Signer

#: The fake runtime signs every request, as the real one does: the replay cache (wire v5) is
#: only served by an endpoint with ``identity_keys``, wire v6 by one with ``identity_keys`` or
#: ``segment_origins``. ``REQUEST_RESPONSE`` endpoints verify identity; ``BIDI_STREAM`` ones (no
#: replay cache) list the trace's segment origins instead -- a BIDI endpoint opens one stream
#: per JWT, and two connections of one invocation within a second carry identical JWTs.
RUNTIME = Signer()

if TYPE_CHECKING:  # bound for the type checker; at run time the optional imports below decide
    import hypercorn.asyncio as hypercorn_asyncio
    from h2.config import H2Configuration
    from h2.connection import H2Connection
    from h2.events import DataReceived, ResponseReceived, StreamEnded, StreamReset
    from hypercorn.config import Config as HypercornConfig

try:
    from h2.config import H2Configuration
    from h2.connection import H2Connection
    from h2.events import DataReceived, ResponseReceived, StreamEnded, StreamReset

    HAVE_H2 = True
except ImportError:  # the optional dependency of BIDI_STREAM
    HAVE_H2 = False

try:
    import hypercorn.asyncio as hypercorn_asyncio
    from hypercorn.config import Config as HypercornConfig

    HAVE_HYPERCORN = True
except ImportError:  # optional: the ASGI app under a real ASGI server
    HAVE_HYPERCORN = False

ROOT = Path(__file__).resolve().parents[3] / "conformance"
TRACES = sorted((ROOT / "traces").glob("*.json"))

#: Wire versions this SDK does not implement yet, with the reason; every skipped trace is
#: listed by name in ``test_skips_are_known`` so none slips out of the runner silently.
NOT_YET: dict[int, str] = {}
BIDI_REASON = "BIDI_STREAM needs the optional dependency h2 (pip install 'cairnflow[bidi]')"


def load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def skip_reason(t: dict[str, Any]) -> str | None:
    if t["mode"] == "BIDI_STREAM" and not HAVE_H2:
        return BIDI_REASON
    if t["wire_version"] > MAX_PROTOCOL_VERSION and "sdk_max_protocol_version" not in t:
        return NOT_YET.get(t["wire_version"], f"wire v{t['wire_version']} is newer than this SDK")
    return None


# ─────────────────────────────────────────────────── frames, by hand


def split(data: bytes) -> list[tuple[int, int, bytes]]:
    """``(type, flags, whole frame)``: an 8-byte big-endian header (u16, u16, u32)."""
    out = []
    at = 0
    while at < len(data):
        assert len(data) - at >= 8, "half a frame header at the end of the SDK response"
        ty = int.from_bytes(data[at : at + 2], "big")
        flags = int.from_bytes(data[at + 2 : at + 4], "big")
        size = int.from_bytes(data[at + 4 : at + 8], "big")
        assert len(data) - at >= 8 + size, "half a frame at the end of the SDK response"
        out.append((ty, flags, data[at : at + 8 + size]))
        at += 8 + size
    return out


def is_side(ty: int) -> bool:
    """Keepalive (v3), StreamAppend and ArtifactPut (v4), the custom range."""
    return ty in (0x0008, 0x000B, 0x000C) or ty >= 0xFC00


def varint_field(payload: bytes, n: int) -> int:
    at = 0
    found = 0

    def varint() -> int:
        nonlocal at
        r, shift = 0, 0
        while True:
            b = payload[at]
            at += 1
            r |= (b & 0x7F) << shift
            if not b & 0x80:
                return r
            shift += 7

    while at < len(payload):
        key = varint()
        wire = key & 7
        if wire == 0:
            v = varint()
            if key >> 3 == n:
                found = v
        elif wire == 2:
            size = varint()
            at += size
        elif wire == 1:
            at += 8
        elif wire == 5:
            at += 4
        else:
            raise AssertionError(f"unknown wire type {wire}")
    return found


def mismatch(want: dict[str, Any], got: bytes) -> str | None:
    head = got[:4].hex()
    match = want.get("match", "bytes")
    if match == "bytes":
        return None if got.hex() == want["hex"] else "bytes differ"
    if match == "type":
        return None if head == want["hex"][:8] else "type/flags differ"
    if head != want["hex"][:8]:
        return "type/flags differ"
    code = varint_field(got[8:], 1)
    behavior = varint_field(got[8:], 9)
    if code == want["error_code"] and behavior == want["error_behavior"]:
        return None
    return (
        f"ErrorMessage code={code} behavior={behavior}, "
        f"the trace demands code={want['error_code']} behavior={want['error_behavior']}"
    )


# ─────────────────────────────────────────────────── a tiny HTTP/1.1 client


async def http(port: int, method: str, path: str, headers: dict[str, str], body: bytes = b"") -> tuple[int, dict[str, str], bytes]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        head = [f"{method} {path} HTTP/1.1", "host: 127.0.0.1", f"content-length: {len(body)}", "connection: close"]
        head += [f"{k}: {v}" for k, v in headers.items()]
        writer.write(("\r\n".join(head) + "\r\n\r\n").encode() + body)
        await writer.drain()
        raw = await reader.readuntil(b"\r\n\r\n")
        lines = raw.decode("latin-1").split("\r\n")
        status = int(lines[0].split(" ")[1])
        hs = {}
        for line in lines[1:]:
            if line:
                k, _, v = line.partition(":")
                hs[k.strip().lower()] = v.strip()
        data = await reader.readexactly(int(hs.get("content-length", "0")))
        return status, hs, data
    finally:
        writer.close()


# ─────────────────────────────────────────────────── an h2c client (BIDI_STREAM)


class RawInbox:
    """SDK frames in arrival order, with waiting (5 s of silence is a failure)."""

    def __init__(self) -> None:
        self.buf = b""
        self.queue: list[tuple[int, int, bytes]] = []
        self.ended = False
        self.event = asyncio.Event()

    def push(self, data: bytes) -> None:
        self.buf += data
        while len(self.buf) >= 8:
            size = int.from_bytes(self.buf[4:8], "big")
            if len(self.buf) < 8 + size:
                break
            frame, self.buf = self.buf[: 8 + size], self.buf[8 + size :]
            self.queue.append((int.from_bytes(frame[:2], "big"), int.from_bytes(frame[2:4], "big"), frame))
        self.event.set()

    def end(self) -> None:
        self.ended = True
        self.event.set()

    async def next(self, what: str) -> tuple[int, int, bytes] | None:
        while True:
            if self.queue:
                return self.queue.pop(0)
            if self.ended:
                assert not self.buf, "half a frame left at the end of the SDK stream"
                return None
            self.event.clear()
            try:
                await asyncio.wait_for(self.event.wait(), 5)
            except TimeoutError:
                raise AssertionError(f"SDK silent for over 5 s while the runner waits for {what}") from None

    async def rest(self) -> list[str]:
        out = []
        while (f := await self.next("the SDK closing its side")) is not None:
            if not is_side(f[0]):
                out.append(f"0x{f[0]:04x} {f[2].hex()}")
        return out


class H2Client:
    def __init__(self) -> None:
        self.conn = H2Connection(H2Configuration(client_side=True, header_encoding="utf-8"))
        self.inboxes: dict[int, RawInbox] = {}
        self.headers: dict[int, asyncio.Future[dict[str, str]]] = {}

    async def open(self, port: int) -> None:
        self.reader, self.writer = await asyncio.open_connection("127.0.0.1", port)
        self.conn.initiate_connection()
        self.flush()
        self.task = asyncio.get_running_loop().create_task(self.read())

    def flush(self) -> None:
        data = self.conn.data_to_send()
        if data:
            self.writer.write(data)

    async def read(self) -> None:
        try:
            while data := await self.reader.read(65536):
                for ev in self.conn.receive_data(data):
                    sid = getattr(ev, "stream_id", 0)
                    if isinstance(ev, ResponseReceived):
                        self.headers[sid].set_result({str(k): str(v) for k, v in ev.headers})
                    elif isinstance(ev, DataReceived):
                        self.conn.acknowledge_received_data(ev.flow_controlled_length, sid)
                        self.inboxes[sid].push(ev.data)
                    elif isinstance(ev, (StreamEnded, StreamReset)):
                        if sid in self.inboxes:
                            self.inboxes[sid].end()
                        if isinstance(ev, StreamReset) and not self.headers[sid].done():
                            self.headers[sid].set_exception(AssertionError("stream reset before headers"))
                self.flush()
        except ConnectionError:
            pass
        finally:
            for i in self.inboxes.values():
                i.end()

    def request(self, path: str, content_type: str) -> int:
        sid = self.conn.get_next_available_stream_id()
        self.inboxes[sid] = RawInbox()
        self.headers[sid] = asyncio.get_running_loop().create_future()
        self.conn.send_headers(
            sid,
            [(":method", "POST"), (":path", path), (":scheme", "http"), (":authority", "127.0.0.1"),
             ("content-type", content_type)],
        )
        self.flush()
        return sid

    def write(self, sid: int, data: bytes) -> None:
        self.conn.send_data(sid, data)
        self.flush()

    def end(self, sid: int) -> None:
        self.conn.end_stream(sid)
        self.flush()

    def close(self) -> None:
        self.task.cancel()
        self.writer.close()


def runs(frames: list[dict[str, Any]]) -> list[Any]:
    """Consecutive runtime frames become one write, as a real runtime flushes a batch."""
    out: list[Any] = []
    batch = b""
    for f in frames:
        if f["from"] == "runtime":
            batch += bytes.fromhex(f["hex"])
            continue
        if batch:
            out.append(batch)
            batch = b""
        out.append(f)
    if batch:
        out.append(batch)
    return out


# ─────────────────────────────────────────────────── running one trace


class Runner:
    def __init__(self, t: dict[str, Any], max_wire: int, via: str = "serve") -> None:
        self.t = t
        self.max_wire = max_wire
        self.fetches = 0
        #: ``serve`` = real HTTP; ``lambda`` = the AWS Lambda adapter, event in, result out.
        self.via = via
        self.lambdas: dict[int, Any] = {}

    async def fetch(self, url: str, max_bytes: int) -> bytes:
        """Wire v6: serves exactly the trace's segment bytes for its URLs."""
        for seg in self.t.get("segments", []):
            if seg["url"] == url:
                self.fetches += 1
                return bytes.fromhex(seg["hex"])[: max_bytes + 1]
        raise AssertionError(f"the SDK fetched a URL the trace does not serve: {url}")

    async def fresh(self) -> tuple[asyncio.Server, int]:
        t = self.t
        origins = sorted({"{0.scheme}://{0.netloc}".format(urlsplit(seg["url"])) for seg in t.get("segments", [])})
        identity: dict[str, Any] = (
            {"identity_keys": [RUNTIME.kid]}
            if t["mode"] == "REQUEST_RESPONSE"
            else {"segment_origins": origins or ["https://segments.conformance.invalid"]}
        )
        endpoint = Endpoint(
            [conformance],
            protocol_mode=t["mode"],
            max_protocol_version=self.max_wire,
            **identity,
            clock=lambda: t["clock_ms"],
            # A cache per endpoint, so every v6 trace really downloads its segments.
            value_cache_bytes=64 << 20,
            segment_fetch=self.fetch,
        )
        if self.via == "hypercorn":
            return await self.hypercorn(endpoint)
        if self.via == "lambda":
            server = await asyncio.start_server(lambda r, w: w.close(), "127.0.0.1", 0)
            port = server.sockets[0].getsockname()[1]
            self.lambdas[port] = async_lambda_handler(endpoint)
            return server, port
        server = await start(endpoint, port=0)
        return server, server.sockets[0].getsockname()[1]

    async def hypercorn(self, endpoint: Endpoint) -> tuple[Any, int]:
        """The ASGI app (``cairnflow.app``) under hypercorn: h1 and h2c on one port."""
        import socket

        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        config = HypercornConfig()
        config.bind = [f"127.0.0.1:{port}"]
        config.accesslog = None
        config.errorlog = None
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        errors: list[str] = []
        before = loop.get_exception_handler()
        # hypercorn reports a connection it dropped only here: collect, fail at stop.
        loop.set_exception_handler(lambda _l, ctx: errors.append(f"{ctx.get('message')} {ctx.get('exception')!r}"))
        task = asyncio.get_running_loop().create_task(
            hypercorn_asyncio.serve(app(endpoint, bidi_close_wait=0.2), config, shutdown_trigger=stop.wait)  # type: ignore[arg-type]
        )
        for _ in range(100):
            try:
                _r, w = await asyncio.open_connection("127.0.0.1", port)
                w.close()
                break
            except OSError:
                await asyncio.sleep(0.02)
        return _Hyper(stop, task, errors, lambda: loop.set_exception_handler(before)), port

    @staticmethod
    async def stop(server: Any) -> None:
        server.close()
        await server.wait_closed()

    async def run(self) -> None:
        t = self.t
        conformance_handlers.slow_release = asyncio.Event()
        server, port = await self.fresh()
        try:
            await self.negotiate(port)
            for i, conn in enumerate(t["connections"]):
                at = f"{t['name']} connection {i}"
                replay = conn.get("replay", {})
                if replay.get("sdk_restart"):
                    await self.stop(server)
                    server, port = await self.fresh()
                if replay.get("stale_sdk"):
                    # The second replica: received every earlier connection except the last 200 one.
                    prior = t["connections"][:i]
                    ok = [j for j, c in enumerate(prior) if not c.get("replay", {}).get("miss")]
                    last = ok[-1] if ok else -1
                    stale, stale_port = await self.fresh()
                    for j, c in enumerate(prior):
                        if j == last or c.get("replay", {}).get("miss"):
                            continue
                        await self.request_response(stale_port, c, f"{t['name']} stale replica, connection {j}")
                    await self.stop(server)
                    server, port = stale, stale_port
                if t["mode"] == "BIDI_STREAM":
                    await self.bidi_stream(port, conn, at)
                else:
                    await self.request_response(port, conn, at)
        finally:
            await self.stop(server)

    async def post(self, port: int, method: str, path: str, headers: dict[str, str], body: bytes = b"") -> tuple[int, dict[str, str], bytes]:
        if self.via != "lambda":
            return await http(port, method, path, headers, body)
        event = {
            "rawPath": path,
            "headers": headers,
            "requestContext": {"http": {"method": method}},
            "body": base64.b64encode(body).decode(),
            "isBase64Encoded": True,
        }
        r = await self.lambdas[port](event, None)
        return r["statusCode"], r["headers"], base64.b64decode(r["body"])

    async def negotiate(self, port: int) -> None:
        t = self.t
        status, _h, body = await self.post(
            port, "GET", "/discover", {"accept": "application/vnd.cairnflow.endpointmanifest.v1+json", **RUNTIME.headers("/discover")}
        )
        assert status == 200, status
        m = json.loads(body)
        assert m["protocolMode"] == t["mode"]
        if "sdk_max_protocol_version" in t:
            assert m["maxProtocolVersion"] == t["sdk_max_protocol_version"], f"{t['name']}: SDK cap"
            return
        chosen = min(m["maxProtocolVersion"], t["wire_version"])
        assert chosen >= max(1, m["minProtocolVersion"]), f"{t['name']}: no overlap"
        assert chosen == t["wire_version"], f"{t['name']}: negotiated v{chosen}"

    async def expect(self, inbox: RawInbox, want: dict[str, Any], at: str) -> None:
        while True:
            got = await inbox.next(f"{want['name']} ({at})")
            assert got is not None, f"{at}: the SDK closed, the trace still expects {want['name']}: {want['summary']}"
            ty, flags, frame = got
            why = mismatch(want, frame)
            if why is None:
                return
            if is_side(ty) and not (want.get("channel") == "side" and ty == int(want["type"], 16)):
                continue
            raise AssertionError(
                f"{at}: {why}\n  trace: {want['name']} {want['hex']}\n         {want['summary']}\n"
                f"  SDK:   0x{ty:04x} flags=0x{flags:x} {frame.hex()}"
            )

    async def bidi_stream(self, port: int, conn: dict[str, Any], at: str) -> None:
        t = self.t
        client = H2Client()
        await client.open(port)
        try:
            sid = client.request(
                f"/invoke/{t['handler']['service']}/{t['handler']['name']}",
                f"application/vnd.cairnflow.invocation.v{t['wire_version']}",
            )
            inbox = client.inboxes[sid]
            if "status" in conn:
                for step in runs(conn["frames"]):
                    if isinstance(step, bytes):
                        client.write(sid, step)
                h = await asyncio.wait_for(client.headers[sid], 5)
                assert int(h[":status"]) == conn["status"], f"{at}: status {h[':status']}"
                assert not h.get("content-type", "").startswith("application/vnd.cairnflow.invocation"), at
                return
            for step in runs(conn["frames"]):
                if isinstance(step, bytes):
                    client.write(sid, step)
                    continue
                await self.expect(inbox, step, at)
                if step["type"] == "0x0008" and conformance_handlers.slow_release is not None:
                    conformance_handlers.slow_release.set()
            h = await asyncio.wait_for(client.headers[sid], 5)
            media = h.get("content-type", "").split(";")[0].strip()
            prefix = "application/vnd.cairnflow.invocation.v"
            assert media.startswith(prefix) and int(media[len(prefix) :]) <= t["wire_version"], f"{at}: {media}"
            if conn["close"] == "runtime":
                client.end(sid)
                extra = await inbox.rest()
                assert not extra, f"{at}: the SDK kept sending after the runtime closed: {extra}"
            elif self.via == "hypercorn":
                # The SDK has sent the turn's last frame. Under ASGI it waits for the
                # runtime to end the request body before ending its own side (hypercorn
                # drops the whole connection on data for a stream whose response ended):
                # end first, as the real runtime does right after reading that frame.
                client.end(sid)
                extra = await inbox.rest()
                assert not extra, f"{at}: the SDK sent extra frames: {extra}"
            else:
                extra = await inbox.rest()
                assert not extra, f"{at}: the SDK sent extra frames: {extra}"
                client.end(sid)
        finally:
            client.close()

    async def request_response(self, port: int, conn: dict[str, Any], at: str) -> None:
        t = self.t
        body = b"".join(bytes.fromhex(f["hex"]) for f in conn["frames"] if f["from"] == "runtime")
        ct = f"application/vnd.cairnflow.invocation.v{t['wire_version']}"
        path = f"/invoke/{t['handler']['service']}/{t['handler']['name']}"
        status, headers, data = await self.post(
            port, "POST", path, {"content-type": ct, "accept": ct, **RUNTIME.headers(path, body)}, body
        )
        if "status" in conn:
            assert status == conn["status"], f"{at}: status {status}, the trace demands {conn['status']}"
            return
        replay = conn.get("replay")
        if replay is not None and replay.get("miss"):
            # Wire v5: a miss is 409 + `cairn-replay: miss`, an empty body.
            assert status == 409, f"{at}: must miss, got {status}"
            assert headers.get("cairn-replay") == "miss", at
            assert data == b"", f"{at}: a miss body must be empty"
            if "held" in replay:
                assert headers.get("cairn-replay-held") == f"{replay['held']}:{replay['held_digest']}", (
                    f"{at}: cairn-replay-held {headers.get('cairn-replay-held')}"
                )
            return
        assert status == 200, f"{at}: status {status}: {data[:300]!r}"
        if replay is not None:
            cache = headers.get("cairn-replay-cache")
            assert (None if cache is None else int(cache)) == replay.get("cache"), f"{at}: cairn-replay-cache {cache}"
        media = headers.get("content-type", "").split(";")[0].strip()
        prefix = "application/vnd.cairnflow.invocation.v"
        assert media.startswith(prefix) and int(media[len(prefix) :]) <= t["wire_version"], f"{at}: content-type {media}"
        got = split(data)
        i = 0
        for want in (f for f in conn["frames"] if f["from"] == "sdk"):
            while True:
                assert i < len(got), f"{at}: the SDK ended, the trace still expects {want['name']}: {want['summary']}"
                ty, flags, frame = got[i]
                i += 1
                why = mismatch(want, frame)
                if why is None:
                    break
                if is_side(ty) and not (want.get("channel") == "side" and ty == int(want["type"], 16)):
                    continue
                raise AssertionError(
                    f"{at}: {why}\n  trace: {want['name']} {want['hex']}\n         {want['summary']}\n"
                    f"  SDK:   0x{ty:04x} flags=0x{flags:x} {frame.hex()}"
                )
        extra = [f"0x{ty:04x} {fr.hex()}" for ty, _f, fr in got[i:] if not is_side(ty)]
        assert not extra, f"{at}: the SDK sent extra frames: {extra}"


class _Hyper:
    def __init__(
        self, stop: asyncio.Event, task: asyncio.Task[Any], errors: list[str], restore: Any
    ) -> None:
        self._stop = stop
        self._task = task
        self._errors = errors
        self._restore = restore

    def close(self) -> None:
        self._stop.set()

    async def wait_closed(self) -> None:
        try:
            await asyncio.wait_for(self._task, 10)
        finally:
            self._restore()
        assert not self._errors, f"hypercorn dropped a connection: {self._errors[:2]}"


def check_journal(t: dict[str, Any]) -> None:
    """The commands the SDK emitted beyond the durable prefix are exactly the command
    part of the final journal; a verify connection replays the whole journal."""
    sent = [
        f
        for c in t["connections"]
        if not c.get("verify")
        for f in c["frames"]
        if f["from"] == "sdk" and f["type"].startswith("0x04")
    ]
    commands = [e for e in t["journal"][t["initial_journal_len"] :] if e["type"].startswith("0x04")]
    assert len(sent) == len(commands), f"{t['name']}: number of SDK commands differs from the journal"
    for i, (f, e) in enumerate(zip(sent, commands, strict=True)):
        assert f["type"] == e["type"], f"{t['name']}: command {i}"
        if e.get("match", "bytes") == "bytes":
            assert f["hex"][16:] == e["payload_hex"], f"{t['name']}: payload of command {i} differs from the journal"
    verify = next((c for c in t["connections"] if c.get("verify")), None)
    if verify is not None:
        replayed = [f["hex"][16:] for f in verify["frames"] if f["from"] == "runtime"][1:]
        assert replayed == [e["payload_hex"] for e in t["journal"]], f"{t['name']}: verify does not replay the journal"


class ConformanceTest(unittest.IsolatedAsyncioTestCase):
    def test_traces_exist(self) -> None:
        self.assertGreater(len(TRACES), 0, f"no traces under {ROOT}")

    def test_journals_are_self_consistent(self) -> None:
        for path in TRACES:
            t = load(path)
            self.assertEqual(f"{t['name']}.json", path.name)
            check_journal(t)

    def test_skips_are_known(self) -> None:
        """Every trace this SDK does not run yet, by name: a new REQUEST_RESPONSE trace at
        a wire this SDK speaks can never be skipped silently."""
        skipped = sorted(load(p)["name"] for p in TRACES if skip_reason(load(p)) is not None and load(p)["mode"] != "BIDI_STREAM")
        self.assertEqual(skipped, [])

    async def test_traces(self) -> None:
        ran = 0
        for path in TRACES:
            t = load(path)
            reason = skip_reason(t)
            if reason is not None:
                continue
            pins = [t["sdk_max_protocol_version"]] if "sdk_max_protocol_version" in t else [MAX_PROTOCOL_VERSION]
            if "sdk_max_protocol_version" not in t and t["wire_version"] < MAX_PROTOCOL_VERSION:
                pins.append(t["wire_version"])
            for pin in pins:
                with self.subTest(trace=t["name"], sdk_max=pin):
                    runner = Runner(t, pin)
                    await asyncio.wait_for(runner.run(), timeout=30)
                    if t.get("segments"):
                        self.assertGreater(runner.fetches, 0, f"{t['name']}: no segment download at all")
                    ran += 1
                if HAVE_HYPERCORN:
                    with self.subTest(trace=t["name"], sdk_max=pin, via="hypercorn"):
                        await asyncio.wait_for(Runner(t, pin, "hypercorn").run(), timeout=30)
                        ran += 1
                if t["mode"] == "REQUEST_RESPONSE":
                    with self.subTest(trace=t["name"], sdk_max=pin, via="lambda"):
                        await asyncio.wait_for(Runner(t, pin, "lambda").run(), timeout=30)
                        ran += 1
        self.assertGreaterEqual(ran, 74 if HAVE_H2 else 58)


class SuspensionVectorsTest(unittest.TestCase):
    """The SDK encodes each tree to exactly the bytes in the file, and its satisfaction
    rule reaches the runtime's verdict on every combination of leaves (CG-4)."""

    def test_vectors(self) -> None:
        doc = json.loads((ROOT / "suspension-vectors.json").read_text())
        self.assertGreaterEqual(len(doc["vectors"]), 10)
        checked = 0

        def tree(j: dict[str, Any]) -> SuspensionTree:
            return SuspensionTree(
                j["combinator"],
                tuple(j["completions"]),
                tuple(j["signals"]),
                tuple(j["names"]),
                tuple(tree(n) for n in j.get("nested", [])),
            )

        for v in doc["vectors"]:
            t = tree(v["tree"])
            self.assertEqual(encode_suspension(t).hex(), v["hex"], v["name"])
            for s in v["states"]:
                got = tree_satisfied(
                    t, lambda c: c in s["completions"], lambda i: i in s["signals"], lambda n: n in s["names"]
                )
                self.assertEqual(got, s["satisfied"], f"{v['name']}: {s}")
                checked += 1
        self.assertGreater(checked, 70)


if __name__ == "__main__":
    unittest.main()
