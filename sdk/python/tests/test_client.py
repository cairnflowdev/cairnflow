# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""The client (``cairnflow.client``) and its transport (``cairnflow._http``) against a fake
ingress started in-process: the idempotency key kept across retries, the ``leader`` of a
``503``, the ``Retry-After`` of a ``429``, ``202`` => attach by id, the error mapping,
schedules, and ``read_stream`` (``503`` + ``leader``, ``moved``, ``reset``, a cut
connection, ``410 stream_expired``).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import unittest
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

import _path  # noqa: F401  (src/ on sys.path)
from cairnflow._http import NetworkError, Response, Transport, fetch
from cairnflow.client import (
    AlreadyInvoked,
    AttachTimeout,
    CairnHttpError,
    InvocationFailed,
    InvocationPaused,
    OutputResult,
    ScheduleTarget,
    StreamError,
    StreamEvent,
    connect,
    read_stream,
)
from cairnflow.errors import TerminalError

ID = "ab" * 24


@dataclass
class Req:
    method: str
    target: str
    headers: dict[str, str]
    body: bytes

    @property
    def path(self) -> str:
        return unquote(urlsplit(self.target).path)

    @property
    def raw_path(self) -> str:
        return urlsplit(self.target).path

    @property
    def query(self) -> dict[str, str]:
        return {k: v[0] for k, v in parse_qs(urlsplit(self.target).query).items()}

    def json(self) -> Any:
        return json.loads(self.body) if self.body else None


Handler = Callable[[Req, asyncio.StreamWriter], Awaitable[None]]


@dataclass
class Fake:
    """One fake node: every request is recorded in ``seen``, then handed to ``handler``."""

    handler: Handler
    seen: list[Req] = field(default_factory=list)
    server: asyncio.Server | None = None
    url: str = ""

    async def start(self) -> Fake:
        self.server = await asyncio.start_server(self._conn, "127.0.0.1", 0)
        port = self.server.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{port}"
        return self

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            self.server = None

    async def _conn(self, r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        try:
            head = await r.readuntil(b"\r\n\r\n")
            lines = head.decode("latin-1").split("\r\n")
            method, target, _ = lines[0].split(" ", 2)
            headers: dict[str, str] = {}
            for line in lines[1:]:
                if line:
                    k, _, v = line.partition(":")
                    headers[k.strip().lower()] = v.strip()
            body = await r.readexactly(int(headers.get("content-length", "0")))
            req = Req(method, target, headers, body)
            self.seen.append(req)
            await self.handler(req, w)
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            if not w.is_closing():
                w.close()


def reply(w: asyncio.StreamWriter, status: int, body: Any = None, headers: Mapping[str, str] | None = None) -> None:
    data = b"" if body is None else json.dumps(body).encode()
    head = f"HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {len(data)}\r\n"
    for k, v in (headers or {}).items():
        head += f"{k}: {v}\r\n"
    w.write(head.encode() + b"\r\n" + data)


def _chunk(text: str) -> bytes:
    data = text.encode()
    return f"{len(data):x}\r\n".encode() + data + b"\r\n"


Ev = tuple[str, Any] | tuple[str, Any, int]


def sse(w: asyncio.StreamWriter, events: list[Ev], end: bool = True) -> None:
    """Writes an SSE response (chunked); ``end=False`` leaves the body open."""
    w.write(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n")
    w.write(_chunk(": keepalive\n\n"))
    for ev in events:
        idl = f"id: {ev[2]}\n" if len(ev) == 3 else ""
        w.write(_chunk(f"event: {ev[0]}\n{idl}data: {json.dumps(ev[1])}\n\n"))
    if end:
        w.write(b"0\r\n\r\n")


async def die(w: asyncio.StreamWriter, events: list[Ev]) -> None:
    """Breaks the connection like a node under ``kill -9``: no terminating chunk."""
    sse(w, events, end=False)
    await w.drain()
    await asyncio.sleep(0.02)
    w.transport.abort()


async def collect(gen: AsyncIterator[StreamEvent], limit: int = 50) -> list[StreamEvent]:
    out: list[StreamEvent] = []
    async with contextlib.aclosing(gen):  # type: ignore[type-var]
        async for e in gen:
            out.append(e)
            if len(out) >= limit:
                break
    return out


def completed(output: Any, inv_id: str = ID) -> dict[str, Any]:
    return {"id": inv_id, "status": "completed", "output": json.dumps(output)}


class FakeCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.fakes: list[Fake] = []

    async def asyncTearDown(self) -> None:
        for f in self.fakes:
            await f.close()

    async def fake(self, handler: Handler) -> Fake:
        f = await Fake(handler).start()
        self.fakes.append(f)
        return f


# ---------------------------------------------------------------------------
# The HTTP layer
# ---------------------------------------------------------------------------


class HttpTest(FakeCase):
    async def test_content_length_chunked_and_read_until_close(self) -> None:
        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            if req.path == "/cl":
                reply(w, 200, {"a": 1})
            elif req.path == "/chunked":
                w.write(b"HTTP/1.1 100 Continue\r\n\r\n")
                w.write(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\nx-a: 1\r\nx-a: 2\r\n\r\n")
                w.write(_chunk('{"b":') + _chunk("2}") + b"0\r\nx-trailer: t\r\n\r\n")
            else:
                w.write(b'HTTP/1.1 200 OK\r\n\r\n{"c":3}')

        f = await self.fake(h)
        r = await fetch("GET", f"{f.url}/cl", {})
        self.assertEqual((r.status, r.json()), (200, {"a": 1}))
        r = await fetch("POST", f"{f.url}/chunked", {"x-b": "1"}, b"{}")
        self.assertEqual(r.json(), {"b": 2})
        self.assertEqual(r.header("X-A"), "1, 2")
        self.assertEqual(f.seen[1].headers["content-length"], "2")
        self.assertEqual(f.seen[1].headers["x-b"], "1")
        r = await fetch("GET", f"{f.url}/eof", {})
        self.assertEqual(r.json(), {"c": 3})

    async def test_refused_cut_and_timeout_are_network_errors(self) -> None:
        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            if req.path == "/cut":
                w.write(b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nabc")
                await w.drain()
                w.transport.abort()
            else:
                await asyncio.sleep(1)

        f = await self.fake(h)
        with self.assertRaises(NetworkError):
            await fetch("GET", f"{f.url}/cut", {})
        with self.assertRaises(NetworkError):
            await fetch("GET", f"{f.url}/slow", {}, timeout=0.05)
        port = urlsplit(f.url).port
        await f.close()
        with self.assertRaises(NetworkError):
            await fetch("GET", f"http://127.0.0.1:{port}/x", {})

    async def test_header_injection_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            await fetch("GET", "http://127.0.0.1:1/x", {"x": "a\r\nb: c"})


# ---------------------------------------------------------------------------
# The transport, on a fake fetch
# ---------------------------------------------------------------------------


Reply = Response | Exception


def fake_fetch(replies: list[Reply]) -> tuple[Any, list[str]]:
    calls: list[str] = []

    async def f(method: str, url: str, headers: Mapping[str, str], body: bytes | None, stream: bool) -> Response:
        calls.append(url)
        if not replies:
            raise AssertionError("out of fake replies")
        r = replies.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    return f, calls


def jres(status: int, body: Any, headers: Mapping[str, str] | None = None) -> Response:
    return Response(status, dict(headers or {}), json.dumps(body).encode())


class TransportTest(unittest.IsolatedAsyncioTestCase):
    async def test_leader_one_hop(self) -> None:
        f, calls = fake_fetch([jres(503, {"error": "x", "leader": "http://b:2"}), jres(200, {"ok": 1})])
        sent = await Transport("a:1", fetch=f).request("/invoke/S/h", method="POST")
        self.assertEqual((sent.res.status, sent.node), (200, "http://b:2"))
        self.assertEqual(calls, ["http://a:1/invoke/S/h", "http://b:2/invoke/S/h"])

    async def test_other_scheme_not_followed(self) -> None:
        f, calls = fake_fetch([jres(503, {"error": "x", "leader": "http://b:2"}), jres(200, {})])
        sent = await Transport("https://a", fetch=f, max_attempts=2).request("/x")
        self.assertEqual(sent.res.status, 200)
        self.assertEqual(calls, ["https://a/x", "https://a/x"])

    async def test_follow_leader_off(self) -> None:
        f, calls = fake_fetch([jres(503, {"error": "x", "leader": "http://b:2"}), jres(200, {})])
        await Transport("http://a", fetch=f, follow_leader=False).request("/x")
        self.assertEqual(calls, ["http://a/x", "http://a/x"])

    async def test_no_second_hop(self) -> None:
        f, calls = fake_fetch(
            [jres(503, {"error": "x", "leader": "http://b"}), jres(503, {"leader": "http://c"}), jres(200, {})]
        )
        await Transport("http://a", fetch=f).request("/x")
        self.assertEqual(calls, ["http://a/x", "http://b/x", "http://a/x"])

    async def test_503_until_the_cap_returns_the_last(self) -> None:
        f, calls = fake_fetch([jres(503, {"error": "x"})] * 3)
        sent = await Transport("http://a", fetch=f, max_attempts=3).request("/x")
        self.assertEqual(sent.res.status, 503)
        self.assertEqual(len(calls), 3)

    async def test_time_cap(self) -> None:
        f, calls = fake_fetch([jres(503, {"error": "x"})] * 20)
        sent = await Transport("http://a", fetch=f, max_elapsed=timedelta(milliseconds=250)).request("/x")
        self.assertEqual(sent.res.status, 503)
        self.assertLess(len(calls), 6)

    async def test_429(self) -> None:
        f, calls = fake_fetch([jres(429, {"code": "range_overloaded"}, {"retry-after": "0"}), jres(200, {})])
        self.assertEqual((await Transport("http://a", fetch=f).request("/x")).res.status, 200)
        self.assertEqual(len(calls), 2)
        g, gcalls = fake_fetch([jres(429, {"status": "throttled", "id": "ab"}, {"retry-after": "1"})])
        self.assertEqual((await Transport("http://a", fetch=g).request("/x")).res.status, 429)
        self.assertEqual(len(gcalls), 1)

    async def test_network_errors(self) -> None:
        f, calls = fake_fetch([jres(503, {"leader": "http://b"}), NetworkError("down"), jres(200, {})])
        self.assertEqual((await Transport("http://a", fetch=f).request("/x")).res.status, 200)
        self.assertEqual(calls, ["http://a/x", "http://b/x", "http://a/x"])
        g, _ = fake_fetch([NetworkError("down"), jres(200, {})])
        sent = await Transport("http://a", fetch=g).request("/x")
        self.assertEqual(sent.res.status, 200)
        self.assertTrue(sent.resent)
        h, _ = fake_fetch([NetworkError("down")])
        with self.assertRaises(NetworkError):
            await Transport("http://a", fetch=h, retry_network=False).request("/x")

    async def test_dead_leader_not_followed_again(self) -> None:
        f, calls = fake_fetch(
            [
                jres(503, {"leader": "http://b"}),
                NetworkError("down"),
                jres(503, {"leader": "http://b"}),
                jres(503, {"leader": "http://c"}),
                jres(200, {}),
            ]
        )
        self.assertEqual((await Transport("http://a", fetch=f).request("/x")).res.status, 200)
        self.assertEqual(calls, ["http://a/x", "http://b/x", "http://a/x", "http://a/x", "http://c/x"])

    async def test_cancelling_stops_the_wait(self) -> None:
        seen: list[Mapping[str, str]] = []

        async def f(method: str, url: str, headers: Mapping[str, str], body: bytes | None, stream: bool) -> Response:
            seen.append(dict(headers))
            return jres(503, {"error": "x"})

        t = Transport("http://a", headers={"Authorization": "Bearer k"}, fetch=f)
        task = asyncio.create_task(t.request("/x", headers={"Idempotency-Key": "i"}))
        await asyncio.sleep(0.02)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(seen[0], {"authorization": "Bearer k", "idempotency-key": "i"})


# ---------------------------------------------------------------------------
# connect()
# ---------------------------------------------------------------------------


class ClientTest(FakeCase):
    async def test_call_waits_and_sends_the_contract_headers(self) -> None:
        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            reply(w, 200, completed({"ok": True, "n": req.json()}), {"x-cairn-invocation-id": ID})

        f = await self.fake(h)
        c = connect(f.url, "cf_key")
        out = await c.service("Orders").call("submit", {"amount": 5}, limit_key="t1", log_level="debug")
        self.assertEqual(out, {"ok": True, "n": {"amount": 5}})
        r = f.seen[0]
        self.assertEqual((r.method, r.target), ("POST", "/invoke/Orders/submit"))
        self.assertEqual(r.headers["authorization"], "Bearer cf_key")
        self.assertEqual(r.headers["content-type"], "application/json")
        self.assertEqual(r.headers["x-cairn-limit-key"], "t1")
        self.assertEqual(r.headers["cairnflow-log-level"], "debug")
        self.assertRegex(r.headers["idempotency-key"], r"^[0-9a-f-]{36}$")

    async def test_paths_keys_scopes_and_bodies(self) -> None:
        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            reply(w, 200, {"id": ID, "status": "completed", "output": ""})

        f = await self.fake(h)
        c = connect(f.url)
        self.assertIsNone(await c.object("Cart", "a/b c", scope="acme").call("add"))
        self.assertIsNone(await c.service("S").call("h", None))
        self.assertEqual(f.seen[0].target, "/scope/acme/invoke/Cart/add?key=a%2Fb%20c")
        self.assertEqual(f.seen[0].body, b"")
        self.assertNotIn("content-type", f.seen[0].headers)
        self.assertNotIn("authorization", f.seen[0].headers)
        self.assertEqual(f.seen[1].body, b"null")

    async def test_idempotency_key_off_explicit_and_workflow_run(self) -> None:
        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            reply(w, 200, completed("ok"))

        f = await self.fake(h)
        c = connect(f.url)
        await c.service("S").call("h", 1, idempotency_key=None)
        await c.service("S").call("h", 1, idempotency_key="mine")
        await c.workflow("W", "k").run(1)
        await c.workflow("W", "k").call("click", 1)
        await c.workflow("W", "k").run(1, idempotency_key="r1")
        keys = [r.headers.get("idempotency-key") for r in f.seen]
        self.assertEqual(keys[:3], [None, "mine", None])
        self.assertIsNotNone(keys[3])
        self.assertEqual(keys[4], "r1")
        self.assertEqual(f.seen[2].target, "/invoke/W/run?key=k")

    async def test_the_key_is_kept_across_503_and_network_errors(self) -> None:
        n = 0

        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            nonlocal n
            n += 1
            if n == 1:
                reply(w, 503, {"error": "electing"})
            elif n == 2:
                w.transport.abort()  # the response is lost
            else:
                reply(w, 200, completed(n))

        f = await self.fake(h)
        self.assertEqual(await connect(f.url).service("S").call("h", 1), 3)
        keys = {r.headers["idempotency-key"] for r in f.seen}
        self.assertEqual(len(f.seen), 3)
        self.assertEqual(len(keys), 1)

    async def test_503_leader_is_followed_with_the_same_key(self) -> None:
        async def leader(req: Req, w: asyncio.StreamWriter) -> None:
            reply(w, 200, completed("from b"))

        b = await self.fake(leader)

        async def follower(req: Req, w: asyncio.StreamWriter) -> None:
            reply(w, 503, {"error": "not the leader", "leader": b.url.removeprefix("http://")})

        a = await self.fake(follower)
        self.assertEqual(await connect(a.url).service("S").call("h", 1), "from b")
        self.assertEqual((len(a.seen), len(b.seen)), (1, 1))
        self.assertEqual(a.seen[0].headers["idempotency-key"], b.seen[0].headers["idempotency-key"])

    async def test_a_leader_with_another_scheme_is_not_followed(self) -> None:
        n = 0

        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            nonlocal n
            n += 1
            if n == 1:
                reply(w, 503, {"error": "x", "leader": "https://127.0.0.1:1"})
            else:
                reply(w, 200, completed("home"))

        f = await self.fake(h)
        self.assertEqual(await connect(f.url).service("S").call("h"), "home")
        self.assertEqual(len(f.seen), 2)

    async def test_429_without_id_waits_retry_after(self) -> None:
        n = 0

        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            nonlocal n
            n += 1
            if n == 1:
                reply(w, 429, {"error": "overloaded", "code": "range_overloaded"}, {"retry-after": "0"})
            else:
                reply(w, 200, completed("ok"))

        f = await self.fake(h)
        self.assertEqual(await connect(f.url).service("S").call("h"), "ok")
        self.assertEqual(len(f.seen), 2)
        self.assertEqual(f.seen[0].headers["idempotency-key"], f.seen[1].headers["idempotency-key"])

    async def test_202_and_429_throttled_attach_by_id(self) -> None:
        attaches = 0

        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            nonlocal attaches
            if req.path.startswith("/invoke/S/slow"):
                reply(w, 202, {"id": ID, "status": "waiting"}, {"x-cairn-invocation-id": ID})
            elif req.path.startswith("/invoke/S/throttled"):
                reply(w, 429, {"id": ID, "status": "throttled", "retry_after_ms": 5}, {"retry-after": "1"})
            elif req.path == f"/invocations/{ID}/attach":
                attaches += 1
                if attaches % 2 == 1:
                    reply(w, 202, {"id": ID, "status": "waiting"})
                else:
                    reply(w, 200, completed("late"))

        f = await self.fake(h)
        c = connect(f.url, attach_wait=timedelta(milliseconds=1500))
        self.assertEqual(await c.service("S").call("slow"), "late")
        self.assertEqual(f.seen[1].target, f"/invocations/{ID}/attach?wait_ms=1500")
        self.assertEqual(f.seen[1].method, "POST")
        self.assertNotIn("idempotency-key", f.seen[1].headers)
        self.assertEqual(await c.service("S").call("throttled"), "late")
        self.assertEqual(len(f.seen), 6, [r.target for r in f.seen])

    async def test_attach_timeout(self) -> None:
        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            wait = int(req.query.get("wait_ms", "0"))
            await asyncio.sleep(min(wait, 50) / 1000)
            reply(w, 202, {"id": ID, "status": "waiting"}, {"x-cairn-invocation-id": ID})

        f = await self.fake(h)
        c = connect(f.url)
        with self.assertRaises(AttachTimeout) as cm:
            await c.service("S").call("h", timeout=0.2)
        self.assertEqual(cm.exception.invocation_id, ID)
        self.assertTrue(all(int(r.query["wait_ms"]) <= 200 for r in f.seen[1:]))
        with self.assertRaises(AttachTimeout):
            await c.invocation(ID).attach(timeout=timedelta(milliseconds=100))

    async def test_attach_retries_503_and_429_without_id(self) -> None:
        n = 0

        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            nonlocal n
            n += 1
            if n == 1:
                reply(w, 429, {"error": "too many", "code": "too_many_open_streams"}, {"retry-after": "0"})
            elif n == 2:
                reply(w, 503, {"error": "x"})
            else:
                reply(w, 200, completed(7))

        f = await self.fake(h)
        c = connect(f.url, max_attempts=1)
        self.assertEqual(await c.invocation(ID).attach(), 7)
        self.assertEqual(len(f.seen), 3)

    async def test_error_mapping(self) -> None:
        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            hdr = {"x-cairn-invocation-id": ID}
            match req.path:
                case "/invoke/S/failed":
                    reply(w, 500, {"id": ID, "status": "failed", "code": 422, "message": "bad amount"}, hdr)
                case "/invoke/S/paused":
                    reply(w, 409, {"id": ID, "status": "paused", "code": 503, "message": "out of attempts"}, hdr)
                case "/invoke/W/run":
                    reply(w, 409, {"id": ID, "status": "already_invoked"}, hdr)
                case "/invoke/S/bad":
                    reply(w, 400, {"error": "bad input", "code": "bad_request"})
                case "/invoke/S/text":
                    w.write(b"HTTP/1.1 403 Forbidden\r\ncontent-length: 4\r\n\r\nnope")
                case _:
                    reply(w, 404, {"message": "no such thing"})

        f = await self.fake(h)
        c = connect(f.url)
        with self.assertRaises(InvocationFailed) as failed:
            await c.service("S").call("failed")
        e = failed.exception
        self.assertIsInstance(e, TerminalError)
        self.assertEqual((e.code, e.message, e.invocation_id), (422, "bad amount", ID))
        with self.assertRaises(InvocationPaused) as paused:
            await c.service("S").call("paused")
        self.assertEqual((paused.exception.code, paused.exception.message), (503, "out of attempts"))
        with self.assertRaises(AlreadyInvoked) as already:
            await c.workflow("W", "k").run({})
        self.assertEqual(already.exception.invocation_id, ID)
        with self.assertRaises(CairnHttpError) as bad:
            await c.service("S").call("bad")
        self.assertEqual((bad.exception.status, bad.exception.code, str(bad.exception)), (400, "bad_request", "bad input"))
        with self.assertRaises(CairnHttpError) as text:
            await c.service("S").call("text")
        self.assertEqual((text.exception.status, text.exception.code, str(text.exception)), (403, None, "HTTP 403"))
        with self.assertRaises(CairnHttpError) as missing:
            await c.invocation(ID).status()
        self.assertEqual((missing.exception.status, str(missing.exception)), (404, "no such thing"))

    async def test_a_resent_workflow_run_waits_for_its_own_run(self) -> None:
        """A workflow's ``run`` carries no key: the first send reached the node but its
        response was lost, so the resend gets ``409 already_invoked``. That run is the
        caller's own: wait for it instead of raising :class:`AlreadyInvoked`."""
        n = 0

        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            nonlocal n
            n += 1
            if n == 1:
                w.transport.abort()
            elif req.path == "/invoke/W/run":
                reply(w, 409, {"id": ID, "status": "already_invoked"}, {"x-cairn-invocation-id": ID})
            else:
                reply(w, 200, completed("mine"))

        f = await self.fake(h)
        self.assertEqual(await connect(f.url).workflow("W", "k").run({}), "mine")
        self.assertEqual(f.seen[-1].target.split("?")[0], f"/invocations/{ID}/attach")

    async def test_send_handle(self) -> None:
        state = {"done": False}

        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            hdr = {"x-cairn-invocation-id": ID}
            p = req.path
            if p == "/invoke/S/slow/send":
                reply(w, 202, {"id": ID, "status": "waiting"}, hdr)
            elif p == "/invoke/W/run/send":
                reply(w, 409, {"id": ID, "status": "already_invoked"}, hdr)
            elif p == "/invoke/S/paused/send":
                reply(w, 409, {"id": ID, "status": "paused"}, hdr)
            elif p == "/invoke/S/nope/send":
                reply(w, 400, {"error": "bad"})
            elif p == f"/invocations/{ID}/attach":
                if state["done"]:
                    reply(w, 200, completed("late"))
                else:
                    reply(w, 202, {"id": ID, "status": "waiting"})
            elif p == f"/invocations/{ID}":
                reply(w, 200, {"id": ID, "status": "completed", "createdAt": 1, "endedAt": 2})
            elif p == f"/invocations/{ID}/signal":
                reply(w, 200, {"id": ID, "status": "delivered"})
            elif p == f"/invocations/{ID}/cancel":
                reply(w, 200, {"id": ID, "cancelled": True, "status": "running"})

        f = await self.fake(h)
        c = connect(f.url)
        handle = await c.service("S").send("slow", 800)
        self.assertEqual(f.seen[0].target, "/invoke/S/slow/send")
        self.assertEqual(handle.id, ID)
        self.assertEqual(await handle.output(), OutputResult(False))
        self.assertEqual(f.seen[1].target, f"/invocations/{ID}/attach?wait_ms=0")
        state["done"] = True
        out = await handle.output()
        self.assertEqual((out.ready, out.value), (True, "late"))
        self.assertEqual(await handle.attach(), "late")
        status = await handle.status()
        self.assertEqual((status["status"], status.get("createdAt")), ("completed", 1))
        self.assertEqual((await handle.signal("approve", {"by": "boss"}))["status"], "delivered")
        await handle.signal(2)
        self.assertEqual(f.seen[-2].json(), {"signal": "approve", "value": {"by": "boss"}})
        self.assertEqual(f.seen[-1].json(), {"signal": 2})
        self.assertTrue((await handle.cancel())["cancelled"])
        self.assertEqual(f.seen[-1].target, f"/invocations/{ID}/cancel?mode=signal")
        await handle.cancel(mode="hard")
        self.assertEqual(f.seen[-1].target, f"/invocations/{ID}/cancel")
        self.assertEqual((await c.service("S").send("paused")).id, ID)
        with self.assertRaises(AlreadyInvoked):
            await c.workflow("W", "k").send("run", {})
        with self.assertRaises(CairnHttpError):
            await c.service("S").send("nope")

    async def test_workflow_attach_output_awakeables_promises(self) -> None:
        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            p = req.path
            if p.endswith("/attach"):
                reply(w, 200, completed("a@b:123", "wf"))
            elif p.startswith("/awakeables/"):
                reply(w, 200, {"id": "awk", "status": "resolved"})
            elif req.method == "GET":
                reply(w, 200, {"service": "Signup", "key": "u1", "name": "email", "completed": False})
            else:
                reply(w, 200, {"service": "Signup", "key": "u1", "name": "email", "completed": True})

        f = await self.fake(h)
        c = connect(f.url)
        wf = c.workflow("Signup", "u 1", scope="acme")
        self.assertEqual(await wf.attach(timeout=5), "a@b:123")
        # `wait_ms` is what is left of the 5 s timeout, capped at `attach_wait`.
        self.assertRegex(f.seen[-1].target, r"^/scope/acme/workflow/Signup/u%201/attach\?wait_ms=(4[0-9]{3}|5000)$")
        out = await wf.output()
        self.assertEqual((out.ready, out.value), (True, "a@b:123"))
        self.assertEqual(f.seen[-1].target, "/scope/acme/workflow/Signup/u%201/attach?wait_ms=0")

        await c.awakeable("sign_1/x").resolve({"ok": True})
        self.assertEqual(f.seen[-1].raw_path, "/awakeables/sign_1%2Fx/resolve")
        self.assertEqual(f.seen[-1].json(), {"ok": True})
        await c.awakeable("a").resolve()
        self.assertEqual(f.seen[-1].body, b"")
        await c.awakeable("a").reject("denied", 403)
        self.assertEqual(f.seen[-1].json(), {"message": "denied", "code": 403})
        await c.awakeable("a").reject("denied")
        self.assertEqual(f.seen[-1].json(), {"message": "denied"})

        p = c.promise("Signup", "u1", "email")
        self.assertFalse((await p.peek())["completed"])
        self.assertEqual((f.seen[-1].method, f.seen[-1].target), ("GET", "/promises/Signup/u1/email"))
        self.assertTrue((await p.resolve("456"))["completed"])
        self.assertEqual((f.seen[-1].target, f.seen[-1].json()), ("/promises/Signup/u1/email/resolve", "456"))
        await c.promise("Signup", "u1", "email", scope="acme").reject("broken", 400)
        self.assertEqual(f.seen[-1].target, "/scope/acme/promises/Signup/u1/email/reject")
        self.assertEqual(f.seen[-1].json(), {"message": "broken", "code": 400})

    async def test_schedules(self) -> None:
        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            p = req.path
            if req.method == "PUT":
                reply(w, 200, {"name": "nightly", "created": True, **req.json()})
            elif p.endswith("/trigger"):
                reply(w, 200, {"id": ID, "created": True})
            elif p.endswith("/gone"):
                reply(w, 404, {"error": "no such schedule"})
            elif p.endswith("/pause") or p.endswith("/resume"):
                reply(w, 200, {"name": "nightly", "paused": p.endswith("/pause")})
            elif req.method == "DELETE":
                reply(w, 200, {"name": "nightly", "deleted": True})
            elif p.endswith("/schedules"):
                reply(w, 200, {"schedules": [], "partial": False, "missing": [], "missingRanges": []})
            else:
                reply(w, 200, {"name": "nightly", "fired": 1})

        f = await self.fake(h)
        s = connect(f.url).schedules
        v = await s.put(
            "nightly",
            every=timedelta(minutes=1),
            target=ScheduleTarget("Report", "build"),
            input={"full": True},
            paused=True,
            jitter=0.5,
        )
        self.assertIs(v.get("created"), True)
        self.assertEqual((f.seen[-1].method, f.seen[-1].target), ("PUT", "/schedules/nightly"))
        self.assertEqual(
            f.seen[-1].json(),
            {
                "everyMs": 60_000,
                "target": {"service": "Report", "handler": "build"},
                "input": {"full": True},
                "paused": True,
                "jitterMs": 500,
            },
        )
        await s.put(
            "vo",
            cron="0 2 * * *",
            time_zone="Europe/Berlin",
            target={"service": "CronObject", "handler": "tick", "key": "k1"},
            limit_key="l",
            overlap="allow",
            scope="acme",
        )
        self.assertEqual(f.seen[-1].target, "/scope/acme/schedules/vo")
        self.assertEqual(
            f.seen[-1].json(),
            {
                "cron": "0 2 * * *",
                "timeZone": "Europe/Berlin",
                "target": {"service": "CronObject", "handler": "tick", "key": "k1"},
                "limitKey": "l",
                "overlap": "allow",
            },
        )
        got = await s.get("nightly")
        self.assertEqual(got and got["fired"], 1)
        self.assertIsNone(await s.get("gone"))
        self.assertTrue(await s.delete("nightly"))
        self.assertFalse(await s.delete("gone"))
        self.assertTrue((await s.pause("nightly"))["paused"])
        self.assertFalse((await s.resume("nightly"))["paused"])
        self.assertEqual(f.seen[-1].target, "/schedules/nightly/resume")
        run = await s.trigger("nightly", idempotency_key="r1")
        self.assertEqual(run.id, ID)
        self.assertEqual(f.seen[-1].headers["idempotency-key"], "r1")
        await s.trigger("nightly", scope="acme")
        self.assertEqual(f.seen[-1].target, "/scope/acme/schedules/nightly/trigger")
        self.assertRegex(f.seen[-1].headers["idempotency-key"], r"^[0-9a-f-]{36}$")
        self.assertFalse((await s.list())["partial"])
        await s.list(scope="acme")
        self.assertEqual((f.seen[-1].method, f.seen[-1].target), ("GET", "/scope/acme/schedules"))


# ---------------------------------------------------------------------------
# read_stream
# ---------------------------------------------------------------------------


class ReadStreamTest(FakeCase):
    def seen(self, f: Fake) -> list[str]:
        return [f"{r.target}|{r.headers.get('last-event-id', '')}" for r in f.seen]

    async def test_503_leader_then_moved_and_reset(self) -> None:
        """``503`` + ``leader`` => ask the leader; ``moved`` + ``reset`` => the transient
        stream is read again from 0 at the new leader."""
        calls = 0
        b_url = ""

        async def leader(req: Req, w: asyncio.StreamWriter) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                sse(
                    w,
                    [
                        ("record", {"offset": 0, "durable": False, "data": {"tok": 0}}, 0),
                        ("record", {"offset": 1, "durable": False, "data": {"tok": 1}}, 1),
                        ("moved", {"leader": b_url}),
                        ("reset", {}),
                    ],
                )
            else:
                sse(w, [("record", {"offset": 0, "durable": False, "data": {"tok": "a"}}, 0), ("end", {"next": 1})])

        b = await self.fake(leader)
        b_url = b.url

        async def follower(req: Req, w: asyncio.StreamWriter) -> None:
            reply(w, 503, {"error": "not the leader", "leader": b.url.removeprefix("http://")})

        a = await self.fake(follower)
        got = await collect(read_stream(a.url, ID, "ui"))
        self.assertEqual([e.kind for e in got], ["record", "record", "moved", "reset", "record", "end"])
        self.assertEqual(got[4].data, {"tok": "a"})
        self.assertEqual(got[0].fields, {"offset": 0, "durable": False, "data": {"tok": 0}})
        self.assertEqual(len(a.seen), 1)
        self.assertIn("from=0", self.seen(b)[1])
        self.assertEqual(a.seen[0].headers["accept"], "text/event-stream")

    async def test_durable_stream_reconnects_from_the_next_offset(self) -> None:
        calls = 0
        url = ""

        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                sse(w, [("record", {"offset": 0, "durable": True, "data": 1}, 0), ("moved", {"leader": url})])
            elif calls == 2:
                # The connection's maximum lifetime: the server closes without `end`.
                sse(w, [("record", {"offset": 1, "durable": True, "data": 2}, 1)])
            else:
                sse(w, [("record", {"offset": 2, "durable": True, "data": 3}, 2), ("end", {"next": 3})])

        s = await self.fake(h)
        url = s.url
        got = await collect(read_stream(s.url, ID, "$log", headers={"Authorization": "Bearer k"}))
        self.assertEqual([e.offset for e in got if e.kind == "record"], [0, 1, 2])
        self.assertEqual(got[-1].kind, "end")
        self.assertEqual(got[-1].next, 3)
        self.assertIn("from=1", self.seen(s)[1])
        self.assertIn("from=2", self.seen(s)[2])
        self.assertEqual(s.seen[0].target, f"/invocations/{ID}/streams/%24log?from=0")
        self.assertEqual(s.seen[0].headers["authorization"], "Bearer k")

    async def test_410_stream_expired(self) -> None:
        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            reply(w, 410, {"error": "expired", "code": "stream_expired"})

        s = await self.fake(h)
        with self.assertRaises(StreamError) as cm:
            await collect(read_stream(s.url, ID, "ui"))
        self.assertEqual((cm.exception.status, cm.exception.code, str(cm.exception)), (410, "stream_expired", "expired"))

    async def test_broken_connection_same_generation_no_duplicates(self) -> None:
        calls = 0
        recs: list[Ev] = [
            ("record", {"offset": i, "durable": False, "step": 3, "attempt": 0, "k": i, "data": d}, i)
            for i, d in enumerate("abc")
        ]

        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                await die(w, recs[:2])
            else:
                sse(w, [*recs[int(req.query["from"]) :], ("end", {"next": 3})])

        s = await self.fake(h)
        got = await collect(read_stream(s.url, ID, "ui"))
        self.assertEqual([e.kind for e in got], ["record", "record", "record", "end"])
        self.assertEqual([e.data for e in got if e.kind == "record"], ["a", "b", "c"])
        self.assertIn("from=1", self.seen(s)[1])

    async def test_new_generation_after_a_break_resets(self) -> None:
        calls = 0

        def rec(i: int, attempt: int, d: str) -> Ev:
            return ("record", {"offset": i, "durable": False, "step": 3, "attempt": attempt, "k": i, "data": d}, i)

        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                await die(w, [rec(0, 0, "a"), rec(1, 0, "b")])
            else:
                fresh = [rec(0, 1, "x"), rec(1, 1, "y")]
                sse(w, [*fresh[int(req.query["from"]) :], ("end", {"next": 2})])

        s = await self.fake(h)
        got = await collect(read_stream(s.url, ID, "ui"))
        self.assertEqual([e.kind for e in got], ["record", "record", "reset", "record", "record", "end"])
        self.assertEqual([e.data for e in got[3:5]], ["x", "y"])

    async def test_dead_leader_goes_back_to_the_origin_node(self) -> None:
        def rec(i: int, d: str) -> Ev:
            return ("record", {"offset": i, "durable": True, "step": 1, "attempt": 0, "k": i, "data": d}, i)

        async def new_leader(req: Req, w: asyncio.StreamWriter) -> None:
            sse(w, [*[rec(0, "a"), rec(1, "b")][int(req.query["from"]) :], ("end", {"next": 2})])

        c = await self.fake(new_leader)
        holder: list[Fake] = []

        async def dying(req: Req, w: asyncio.StreamWriter) -> None:
            await holder[0].close()
            await die(w, [rec(0, "a")])

        b = await self.fake(dying)
        holder.append(b)
        asked = 0

        async def origin(req: Req, w: asyncio.StreamWriter) -> None:
            nonlocal asked
            asked += 1
            to = b.url if asked == 1 else c.url
            reply(w, 503, {"error": "not the leader", "leader": to.removeprefix("http://")})

        a = await self.fake(origin)
        got = await collect(read_stream(a.url, ID, "$log"))
        self.assertEqual([e.kind for e in got], ["record", "record", "end"])
        self.assertEqual(asked, 2, "back to the origin node once more")

    async def test_moved_to_another_scheme_is_not_followed(self) -> None:
        """A ``moved`` to another scheme is dropped: no connection to it, reading
        continues at the origin node (a Bearer key never goes to an address with another
        scheme)."""
        foreign = 0

        async def count(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
            nonlocal foreign
            foreign += 1
            w.close()

        other = await asyncio.start_server(count, "127.0.0.1", 0)
        port = other.sockets[0].getsockname()[1]
        calls = 0

        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                sse(w, [("moved", {"leader": f"https://127.0.0.1:{port}"})])
            else:
                sse(w, [("end", {"next": 0})])

        try:
            home = await self.fake(h)
            got = await collect(read_stream(home.url, ID, "ui"))
            self.assertEqual([e.kind for e in got], ["moved", "end"])
            self.assertEqual(foreign, 0, "opened a connection to a leader with another scheme")
        finally:
            other.close()

    async def test_gap_over_the_last_record_is_not_a_reset(self) -> None:
        """Reconnecting after a break when the last record has left the buffer: the server
        sends a ``gap`` covering it, then a later record. That proves no new generation.
        Must see: no ``reset``; the ``gap`` reaches the reader; reading continues."""
        calls = 0

        def rec(i: int, d: str) -> Ev:
            return ("record", {"offset": i, "durable": False, "step": 1, "attempt": 0, "k": i, "data": d}, i)

        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                await die(w, [rec(0, "a"), rec(1, "b")])
            else:
                sse(w, [("gap", {"offset": 1, "dropped": 3}), rec(4, "e"), ("end", {"next": 5})])

        s = await self.fake(h)
        got = await collect(read_stream(s.url, ID, "ui"))
        self.assertEqual([e.kind for e in got], ["record", "record", "gap", "record", "end"])
        self.assertEqual(got[3].offset, 4)

    async def test_gives_up_after_retries(self) -> None:
        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            reply(w, 503, {"error": "electing"})

        s = await self.fake(h)
        with self.assertRaises(StreamError) as cm:
            await collect(read_stream(s.url, ID, "ui", retries=1))
        self.assertEqual(cm.exception.status, 503)

    async def test_breaking_out_closes_the_connection(self) -> None:
        closed = asyncio.Event()

        async def h(req: Req, w: asyncio.StreamWriter) -> None:
            sse(w, [("record", {"offset": 0, "data": 1}, 0)], end=False)
            await w.drain()
            with contextlib.suppress(ConnectionError):
                for _ in range(500):
                    if w.is_closing():
                        break
                    w.write(_chunk(": keepalive\n\n"))
                    await w.drain()
                    await asyncio.sleep(0.01)
                else:
                    return  # the reader never closed
            closed.set()

        s = await self.fake(h)
        got = await collect(read_stream(s.url, ID, "ui"), limit=1)
        self.assertEqual(len(got), 1)
        await asyncio.wait_for(closed.wait(), 2)


if __name__ == "__main__":
    unittest.main()
