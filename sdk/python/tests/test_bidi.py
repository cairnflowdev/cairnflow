# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""``BIDI_STREAM`` specifics the traces do not pin: request identity on h2c (the ``jti``
is only known once ``Start`` arrives), a mid-turn CANCEL reaching a running closure,
and HTTP/1.1 discovery on the same port."""

from __future__ import annotations

import asyncio
import json
import unittest
from typing import Any

import frames as F
from cairnflow import Context, Endpoint, Service, TerminalError, start
from cairnflow._wire import MT, decode_frames
from cairnflow.identity import JWT_HEADER, SCHEME_HEADER, expected_jti
from test_conformance import HAVE_H2, H2Client, http
from test_endpoint import Signer

svc = Service("S")


@svc.handler()
async def hello(ctx: Context, name: str) -> str:
    return f"hi {name}"


closure_cancelled = asyncio.Event()


@svc.handler()
async def long(ctx: Context) -> Any:
    async def work() -> str:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            closure_cancelled.set()
            raise
        return "finished"

    try:
        return await ctx.run("work", work, cancellable=True)
    except TerminalError as e:
        return f"cancelled {e.code}"


CT = "application/vnd.cairnflow.invocation.v4"


@unittest.skipUnless(HAVE_H2, "BIDI_STREAM needs the optional dependency h2")
class BidiTest(unittest.IsolatedAsyncioTestCase):
    async def serve(self, **opts: Any) -> int:
        self.server = await start(Endpoint([svc], protocol_mode="BIDI_STREAM", **opts), port=0)
        self.addAsyncCleanup(self.stop)
        return self.server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        self.server.close()
        await self.server.wait_closed()

    async def open(self, port: int, path: str, extra: list[tuple[str, str]] | None = None) -> tuple[H2Client, int]:
        c = H2Client()
        await c.open(port)
        self.addCleanup(c.close)
        sid = c.conn.get_next_available_stream_id()
        from test_conformance import RawInbox

        c.inboxes[sid] = RawInbox()
        c.headers[sid] = asyncio.get_running_loop().create_future()
        c.conn.send_headers(
            sid,
            [(":method", "POST"), (":path", path), (":scheme", "http"), (":authority", "127.0.0.1"), ("content-type", CT)]
            + (extra or []),
        )
        c.flush()
        return c, sid

    async def drain(self, c: H2Client, sid: int) -> list[int]:
        out = []
        while (f := await c.inboxes[sid].next("the SDK")) is not None:
            out.append(f[0])
        return out

    async def test_discovery_over_http1_on_the_same_port(self) -> None:
        port = await self.serve()
        status, _h, body = await http(port, "GET", "/discover", {})
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["protocolMode"], "BIDI_STREAM")

    async def test_a_turn(self) -> None:
        port = await self.serve()
        c, sid = await self.open(port, "/invoke/S/hello")
        c.write(sid, F.start(1) + F.input_("bo"))
        c.end(sid)
        self.assertEqual(await asyncio.wait_for(self.drain(c, sid), 5), [MT.OUTPUT, MT.END])

    async def test_finished_streams_are_forgotten(self) -> None:
        """A runtime keeps one h2 connection for many turns: finished streams must not pile up."""
        port = await self.serve()
        c = H2Client()
        await c.open(port)
        self.addCleanup(c.close)
        for _ in range(20):
            sid = c.request("/invoke/S/hello", CT)
            c.write(sid, F.start(1) + F.input_("x"))
            c.end(sid)
            self.assertEqual(await asyncio.wait_for(self.drain(c, sid), 5), [MT.OUTPUT, MT.END])
        await asyncio.sleep(0.05)
        from cairnflow import _h2

        sessions = [o for o in __import__("gc").get_objects() if isinstance(o, _h2._Session)]
        self.assertTrue(sessions)
        self.assertEqual(max(len(x.streams) for x in sessions), 0)

    async def test_identity(self) -> None:
        s = Signer()
        port = await self.serve(identity_keys=[s.kid])
        signed = [(SCHEME_HEADER, "v1")]
        jti = expected_jti(F.ID, 0)

        c, sid = await self.open(port, "/invoke/S/hello")
        self.assertEqual((await asyncio.wait_for(c.headers[sid], 5))[":status"], "401")

        token = s.jwt(jti=jti)
        c, sid = await self.open(port, "/invoke/S/hello", signed + [(JWT_HEADER, token)])
        self.assertEqual((await asyncio.wait_for(c.headers[sid], 5))[":status"], "200")
        c.write(sid, F.start(1) + F.input_("id"))
        c.end(sid)
        self.assertEqual(await asyncio.wait_for(self.drain(c, sid), 5), [MT.OUTPUT, MT.END])

        # One JWT, one stream.
        c, sid = await self.open(port, "/invoke/S/hello", signed + [(JWT_HEADER, token)])
        self.assertEqual((await asyncio.wait_for(c.headers[sid], 5))[":status"], "401")

        # A JWT issued for another attempt: the stream closes without a single frame.
        c, sid = await self.open(port, "/invoke/S/hello", signed + [(JWT_HEADER, s.jwt(jti=jti[:-1] + "9"))])
        self.assertEqual((await asyncio.wait_for(c.headers[sid], 5))[":status"], "200")
        c.write(sid, F.start(1) + F.input_("id"))
        self.assertEqual(await asyncio.wait_for(self.drain(c, sid), 5), [])

    async def test_mid_turn_cancel_aborts_a_cancellable_closure(self) -> None:
        closure_cancelled.clear()
        port = await self.serve()
        c, sid = await self.open(port, "/invoke/S/long")
        # With a keepalive interval (wire v3+) the SDK flushes `Run` before the closure ends.
        c.write(sid, F.start(1, keepalive_ms=1000) + F.input_())
        first = await asyncio.wait_for(c.inboxes[sid].next("Run"), 5)
        assert first is not None
        self.assertEqual(first[0], MT.RUN)
        c.write(sid, F.signal(idx=1))
        await asyncio.wait_for(closure_cancelled.wait(), 5)
        # No result is proposed for the aborted step; the turn suspends on it... and the
        # runtime would then close the stream. The rerun sees CANCEL and returns 409.
        c.end(sid)
        rest = await asyncio.wait_for(self.drain(c, sid), 5)
        self.assertNotIn(MT.PROPOSE_RUN_COMPLETION, rest)


class CancelReplayTest(unittest.IsolatedAsyncioTestCase):
    async def test_rerun_after_cancel_returns_409(self) -> None:
        from cairnflow import run_turn
        from cairnflow._wire import encode_frame, encode_run

        run = encode_frame(MT.RUN, 0, encode_run(1, "work"))
        out = await run_turn(long, F.start(3) + F.input_() + run + F.signal(idx=1), takes_input=False)
        types = [f.type for f in decode_frames(out)]
        self.assertEqual(types, [MT.OUTPUT, MT.END])


if __name__ == "__main__":
    unittest.main()
