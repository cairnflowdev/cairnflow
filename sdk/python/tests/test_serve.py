# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""The built-in HTTP/1.1 server of ``serve()``: what it reads before answering."""

from __future__ import annotations

import asyncio
import unittest
import warnings

import _path  # noqa: F401  (src/ on sys.path)
from cairnflow import Context, Endpoint, Service
from cairnflow.serve import start

svc = Service("S")


@svc.handler()
async def hello(ctx: Context, name: str) -> str:
    return f"hi {name}"


async def _status_and_head(reader: asyncio.StreamReader) -> tuple[int, str]:
    head = (await reader.readuntil(b"\r\n\r\n")).decode("latin-1")
    return int(head.split(" ", 2)[1]), head.lower()


class UnreadBodyTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.server = await start(Endpoint([svc]), port=0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def asyncTearDown(self) -> None:
        self.server.close()
        await self.server.wait_closed()

    async def test_an_unread_body_closes_the_connection_instead_of_being_drained(self) -> None:
        """A request the endpoint answers without reading its body (an unknown route, a
        refused wire version) used to have its body drained, up to MAX_REQUEST_BODY
        (256 MiB), so the connection could be reused: anyone who can reach the port
        makes the server read 256 MiB per request, before any request identity check.

        Must see: the 404 arrives at once with ``connection: close`` while the client
        has sent only a few bytes of a declared 100 MiB body, and the server closes."""
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        try:
            writer.write(b"POST /nowhere HTTP/1.1\r\nhost: x\r\ncontent-length: 104857600\r\n\r\nabc")
            await writer.drain()
            status, head = await asyncio.wait_for(_status_and_head(reader), 5)
            self.assertEqual(status, 404)
            self.assertIn("connection: close", head)
            rest = await asyncio.wait_for(reader.read(), 5)
            self.assertLess(len(rest), 1024, "the server kept the connection open")
        finally:
            writer.close()

    async def test_an_early_answer_without_a_body_keeps_the_connection(self) -> None:
        """No body to skip: the connection stays usable for the next request."""
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        try:
            for _ in range(2):
                writer.write(b"POST /nowhere HTTP/1.1\r\nhost: x\r\ncontent-length: 0\r\n\r\n")
                await writer.drain()
                status, head = await asyncio.wait_for(_status_and_head(reader), 5)
                self.assertEqual(status, 404)
                self.assertIn("connection: keep-alive", head)
                length = int(head.split("content-length:", 1)[1].split("\r\n", 1)[0])
                await reader.readexactly(length)
        finally:
            writer.close()


if __name__ == "__main__":
    unittest.main()
