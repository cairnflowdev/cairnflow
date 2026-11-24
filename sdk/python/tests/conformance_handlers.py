# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""The conformance handlers of ``conformance/README.md`` (section Handlers): same names,
same behaviour as the reference SDK and ``sdk/typescript/test/conformance.test.ts``."""

from __future__ import annotations

import asyncio
from typing import Any

import _path  # noqa: F401  (src/ on sys.path)
from cairnflow import Context, VirtualObject

conformance = VirtualObject("Conformance")

#: The ``slowRun`` closure waits on this; the runner releases it at the first Keepalive.
slow_release: asyncio.Event | None = None


@conformance.handler()
async def counter(ctx: Context) -> str:
    n = await ctx.get("count")
    await ctx.sleep(1)
    await ctx.run("charge", lambda: "charged")
    ctx.set("count", (n or 0) + 1)
    return "done"


@conformance.handler(name="counterRenamed")
async def counter_renamed(ctx: Context) -> str:
    n = await ctx.get("count")
    await ctx.sleep(1)
    await ctx.run("charge-v2", lambda: "charged")
    ctx.set("count", (n or 0) + 1)
    return "done"


@conformance.handler(name="counterChanged")
async def counter_changed(ctx: Context) -> str:
    await ctx.get("count")
    ctx.set("count", 9)
    return "other"


@conformance.handler(name="outOfOrder")
async def out_of_order(ctx: Context) -> str:
    a = ctx.sleep(1)
    b = ctx.sleep(2)
    await a
    await b
    return "both"


@conformance.handler(name="sleepRunSleep")
async def sleep_run_sleep(ctx: Context) -> str:
    a = ctx.sleep(1)
    b = ctx.sleep(2)
    await ctx.run("pay", lambda: "result")
    await a
    await b
    return "done"


@conformance.handler(name="eagerCount")
async def eager_count(ctx: Context) -> str:
    n = await ctx.get("count")
    ctx.set("count", (n or 0) + 1)
    return "done"


@conformance.handler(name="stateKeys")
async def state_keys(ctx: Context) -> list[str]:
    return await ctx.state_keys()


@conformance.handler()
async def promise(ctx: Context) -> Any:
    await ctx.promise("approve").resolve("ok")
    return await ctx.promise("approve").get()


@conformance.handler(name="raceSignal")
async def race_signal(ctx: Context) -> Any:
    v = await ctx.race([ctx.sleep(60), ctx.signal("approve")])
    return "sleep" if v is None else v


@conformance.handler(name="raceAwakeable")
async def race_awakeable(ctx: Context) -> Any:
    _id, fut = ctx.awakeable()
    v = await ctx.race([fut, ctx.sleep(60)])
    return "sleep" if v is None else v


@conformance.handler(name="allSignal")
async def all_signal(ctx: Context) -> Any:
    _, s = await ctx.all([ctx.sleep(1), ctx.signal(4)])
    return s


@conformance.handler(name="allOf")
async def all_of(ctx: Context) -> str:
    a = ctx.sleep(1)
    b = ctx.sleep(2)
    await ctx.all([a, b])
    return "both"


@conformance.handler(name="scopedCall")
async def scoped_call(ctx: Context) -> Any:
    return await ctx.scoped("tenant-a", "k1").call("Callee", "work", 1)


@conformance.handler(name="spreadCall")
async def spread_call(ctx: Context) -> Any:
    return await ctx.call("Callee", "work", 1, placement="spread")


@conformance.handler(name="streamRun")
async def stream_run(ctx: Context) -> str:
    ui = ctx.stream("ui")

    def llm() -> str:
        ui.append({"tok": "a"})
        ui.append({"tok": "b"})
        return "ab"

    await ctx.run("llm", llm)
    return "done"


@conformance.handler(name="parallelBranches")
async def parallel_branches(ctx: Context) -> Any:
    async def branch(n: str) -> list[Any]:
        return [await ctx.run(f"{n}1", lambda: f"{n}1"), await ctx.run(f"{n}2", lambda: f"{n}2")]

    return await asyncio.gather(branch("a"), branch("b"))


@conformance.handler(name="callRetargeted")
async def call_retargeted(ctx: Context) -> str:
    await ctx.call("Callee", "b", 1)
    return "x"


@conformance.handler(name="stateKeyChanged")
async def state_key_changed(ctx: Context) -> str:
    await ctx.get("total")
    return "x"


@conformance.handler(name="segmentedGet")
async def segmented_get(ctx: Context) -> Any:
    return await ctx.get("history")


@conformance.handler(name="segmentedSetReuseTail")
async def segmented_set_reuse_tail(ctx: Context) -> str:
    h = await ctx.get("history")
    ctx.set("history", f"{h}b")
    return "ok"


@conformance.handler(name="segmentedRunResult")
async def segmented_run_result(ctx: Context) -> str:
    h = await ctx.get("history")
    await ctx.run("extend", lambda: f"{h}c")
    return "ok"


@conformance.handler(name="slowRun")
async def slow_run(ctx: Context) -> Any:
    async def slow() -> str:
        assert slow_release is not None
        await slow_release.wait()
        return "done"

    return await ctx.run("slow", slow)
