# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""Segmented values (wire v6): what a caller of the endpoint can make it fetch, and learn."""

from __future__ import annotations

import asyncio
import hashlib
import re
import struct
import unittest
import urllib.error
from email.message import Message
from typing import Any

import _path  # noqa: F401  (src/ on sys.path)
import frames as F
from cairnflow import Context, Endpoint, VirtualObject, _values
from cairnflow._pb import Writer
from cairnflow._values import SegmentCache, Segments, ValueOptions, url_key
from cairnflow._wire import MT, SegmentRef, SegmentUrl, WireValue, encode_frame, encode_get_state, write_value
from cairnflow.errors import SegmentedValueError


def segmented(data: bytes, cut: int, salt: int = 0) -> tuple[WireValue, list[SegmentUrl], str]:
    head = data[:cut]
    ref = SegmentRef(hashlib.sha256(head).digest(), len(head), 0xFEDC + salt)
    url = f"https://store.example/values/{salt}?X-Secret-Signature=hunter2"
    return WireValue(data[cut:], (ref,), len(data)), [SegmentUrl(ref.sha256, ref.gen, url)], url


def tiny_refs(n: int, salt: int) -> tuple[WireValue, list[SegmentUrl]]:
    """``n`` one-byte refs with distinct shas, each with its own URL."""
    refs = []
    urls = []
    for i in range(n):
        sha = struct.pack(">II", i, salt) + bytes(24)
        refs.append(SegmentRef(sha, 1, i))
        urls.append(SegmentUrl(sha, i, f"https://store.example/{salt}/{i}"))
    return WireValue(b"", tuple(refs), n), urls


async def assemble(value: WireValue, urls: list[SegmentUrl], fetch: _values.Fetch) -> bytes:
    segs = Segments(ValueOptions(cache=SegmentCache(), fetch=fetch))
    segs.add_urls(urls)
    return await segs._assemble(value)


class SegmentReadTest(unittest.IsolatedAsyncioTestCase):
    async def test_a_failed_read_is_one_generic_text(self) -> None:
        """What a failed segment read returns to the runtime is the turn's error text, which
        the caller of an unverified endpoint reads back: distinct texts for a status, a body
        length or a dial error make the endpoint a port and size scanner of its own network.
        Must see: one text for every outcome after the request went out."""
        value, urls, url = segmented(b"q" * 300, 100)

        def status(code: int) -> _values.Fetch:
            async def f(u: str, n: int) -> bytes:
                raise urllib.error.HTTPError(u, code, "nope", Message(), None)

            return f

        def body(b: bytes) -> _values.Fetch:
            async def f(u: str, n: int) -> bytes:
                return b

            return f

        async def refused(u: str, n: int) -> bytes:
            raise ConnectionRefusedError(61, "Connection refused")

        cases: list[tuple[str, _values.Fetch]] = [
            ("404", status(404)),
            ("500", status(500)),
            ("short body", body(b"x" * 10)),
            ("wrong bytes", body(bytes(100))),
            ("connection refused", refused),
        ]
        texts = set()
        for what, f in cases:
            with self.subTest(what):
                with self.assertRaises(SegmentedValueError) as e:
                    await assemble(value, urls, f)
                msg = str(e.exception)
                self.assertIsNone(re.search(r"HTTP|\b(404|500|10|100)\b|Connection|Refused|bytes", msg), msg)
                self.assertNotIn("hunter2", msg)
                texts.add(msg)
        self.assertEqual(len(texts), 1, f"distinct texts tell the caller what is behind the URL: {texts}")

    async def test_refs_per_value_are_capped_before_any_fetch(self) -> None:
        """The runtime seals segments of at least 64 KiB, so a value of ``max_value_bytes``
        holds at most ``max_value_bytes / 64 KiB`` refs; thousands of one-byte refs cost the
        sender nothing and one outbound request each. Must see: refused before any fetch."""
        calls: list[str] = []

        async def f(u: str, n: int) -> bytes:
            calls.append(u)
            return b"?"

        value, urls = tiny_refs((64 << 20) // (64 << 10) + 1, 1)
        with self.assertRaises(SegmentedValueError):
            await assemble(value, urls, f)
        self.assertEqual(calls, [], "refs were fetched before the count was checked")

    async def test_fetches_in_flight_across_the_process_are_capped(self) -> None:
        """Each assembly fetched with 8 workers, but nothing bounded the turns running at
        once: 8 turns of 8 refs meant 64 requests in flight. Must see: the process cap holds."""
        cap = getattr(_values, "MAX_PROCESS_FETCHES", 32)
        state = {"in": 0, "max": 0}
        gate = asyncio.Event()

        async def slow(u: str, n: int) -> bytes:
            state["in"] += 1
            state["max"] = max(state["max"], state["in"])
            await gate.wait()
            state["in"] -= 1
            raise urllib.error.HTTPError(u, 404, "nope", Message(), None)

        async def one(k: int) -> None:
            value, urls = tiny_refs(8, 100 + k)
            try:
                await assemble(value, urls, slow)
            except SegmentedValueError:
                pass

        tasks = [asyncio.ensure_future(one(k)) for k in range(8)]
        for _ in range(20):
            await asyncio.sleep(0)
        gate.set()
        await asyncio.gather(*tasks)
        self.assertLessEqual(state["max"], cap, f"{state['max']} fetches in flight, the process cap is {cap}")



capped = VirtualObject("Capped")


@capped.handler()
async def read(ctx: Context) -> Any:
    return await ctx.get("h")


class WireCapTest(unittest.IsolatedAsyncioTestCase):
    async def test_an_endpoint_capped_at_v5_fetches_nothing(self) -> None:
        """The v6 gate used to change only the manifest: an endpoint capped at v5 (no
        identity_keys, no segment_origins) still assembled every segmented ref, at whatever
        wire the request said.

        1. The endpoint declares v5; a caller sends a turn labelled v5 whose
           ``Start.segment_urls`` points at a host of its choosing, and a ``GetState``
           completion carrying a ref.
        2. The SDK fetched the URL from its own network position.

        Must see: no fetch at all.
        """
        calls: list[str] = []

        async def f(u: str, n: int) -> bytes:
            calls.append(u)
            return b""

        value, urls, _url = segmented(b"r" * 1500, 1000)
        url = "http://169.254.169.254/latest/meta-data/"
        w = Writer().bytes(1, F.ID).uint32(3, 3).string(6, "k").uint64(9, 7)
        for u in urls:
            w.message(20, Writer().bytes(1, u.sha256).fixed64(2, u.gen).string(3, url))
        body = (
            encode_frame(MT.START, 0, w.finish())
            + F.input_()
            + encode_frame(MT.GET_STATE, 0, encode_get_state("h", 1, "h"))
            + encode_frame(
                MT.GET_STATE_COMPLETION, 0, Writer().uint32(1, 1).message(5, write_value(value.content, value.segments, value.len)).finish()
            )
        )
        ep = Endpoint([capped], segment_fetch=f)
        self.assertEqual(ep.manifest()["maxProtocolVersion"], 5)
        for wire in (5, 1):

            async def data() -> bytes:
                return body

            await ep.answer("POST", "/invoke/Capped/read", {"content-type": f"application/vnd.cairnflow.invocation.v{wire}"}, data)
        self.assertEqual(calls, [], "the endpoint fetched a URL from the request")


if __name__ == "__main__":
    unittest.main()
