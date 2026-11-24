# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""End to end: the real ``cairnflow`` binary drives this SDK over real HTTP.

Two independent implementations of the protocol, two languages: the only check that
catches drift in the header, the message numbers or the protobuf layout that the
conformance traces do not cover. Runs in ``REQUEST_RESPONSE`` and, with the ``h2``
extra installed, in ``BIDI_STREAM``.

Needs the binary: ``cargo build --bin cairnflow`` (or ``CAIRNFLOW_BINARY=<path>``);
skipped without it.
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import shutil
import subprocess
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import _path  # noqa: F401  (src/ on sys.path)
from cairnflow import Context, Endpoint, Service, TerminalError, VirtualObject, Workflow, start

ROOT = Path(__file__).resolve().parents[3]
BINARY = Path(os.environ.get("CAIRNFLOW_BINARY", ROOT / "target" / "debug" / "cairnflow"))
ADMIN_KEY = f"cf_e2e_{secrets.token_hex(16)}"

try:
    import h2  # noqa: F401

    MODES = ["REQUEST_RESPONSE", "BIDI_STREAM"]
except ImportError:
    MODES = ["REQUEST_RESPONSE"]

# ─────────────────────────────────────────────────── the services under test

side_effects: dict[str, int] = {}

orders = VirtualObject("PyOrders")


@orders.handler()
async def submit(ctx: Context, order: dict[str, Any]) -> dict[str, Any]:
    total = (await ctx.get("total")) or 0
    await ctx.sleep(0.05)

    def charge() -> str:
        side_effects[ctx.key] = side_effects.get(ctx.key, 0) + 1
        return f"receipt-{order['amount']}"

    receipt = await ctx.run("charge", charge)
    total += order["amount"]
    ctx.set("total", total)
    return {"receipt": receipt, "total": total, "key": ctx.key}


@orders.handler(kind="shared")
async def peek(ctx: Context) -> Any:
    return {"total": await ctx.get("total"), "keys": await ctx.state_keys()}


@orders.handler()
async def keys_across_sleep(ctx: Context) -> Any:
    """Eager state_keys, a new key, then a suspension: the turn after the sleep gets a snapshot
    that already holds ``b`` and replays the journaled ``state_keys`` (``[total]``) without a 570."""
    keys = await ctx.state_keys()
    ctx.set("b", 2)
    await ctx.sleep(0.01)
    return {"keys": keys, "after": await ctx.state_keys()}


@orders.handler()
async def checkout(ctx: Context, order: dict[str, Any]) -> Any:
    paid = await ctx.call("PyPayments", "pay", order["amount"])
    ctx.send("PyPayments", "note", {"paid": paid})
    return {"paid": paid}


payments = Service("PyPayments")


@payments.handler()
async def pay(ctx: Context, amount: int) -> str:
    if amount > 1000:
        raise TerminalError("over quota", 402)
    return await ctx.run("pay", lambda: f"paid-{amount}")


notes: list[Any] = []


@payments.handler()
async def note(ctx: Context, n: Any) -> None:
    await ctx.run("note", lambda: notes.append(n))


@payments.handler()
async def fanout(ctx: Context, n: int) -> list[int]:
    async def step(i: int) -> int:
        return await ctx.run(f"step-{i}", lambda: i * i)

    return list(await asyncio.gather(*(step(i) for i in range(n))))


@payments.handler()
async def wait_awakeable(ctx: Context) -> Any:
    awk_id, fut = ctx.awakeable()
    await ctx.run("publish", lambda: awakeables.put_nowait(awk_id))
    return await fut


awakeables: asyncio.Queue[str] = asyncio.Queue()

signup = Workflow("PySignup")


@signup.main()
async def run(ctx: Context, user: str) -> Any:
    ctx.set("stage", "waiting")
    email = await ctx.promise("email").get()
    ctx.set("stage", "done")
    return {"user": user, "email": email}


@signup.handler()
async def confirm(ctx: Context, email: str) -> None:
    await ctx.promise("email").resolve(email)


@signup.handler()
async def stage(ctx: Context) -> Any:
    return await ctx.get("stage")


SERVICES = [orders, payments, signup]


# ─────────────────────────────────────────────────── plumbing


def _call(url: str, method: str = "GET", body: Any = None) -> tuple[int, Any]:
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("authorization", f"Bearer {ADMIN_KEY}")
    if data is not None:
        req.add_header("content-type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as res:
            return res.status, _parse(res.read())
    except urllib.error.HTTPError as e:
        return e.code, _parse(e.read())


def _parse(raw: bytes) -> Any:
    try:
        return json.loads(raw) if raw else None
    except ValueError:
        return raw.decode(errors="replace")


async def call(url: str, method: str = "GET", body: Any = None) -> tuple[int, Any]:
    return await asyncio.to_thread(_call, url, method, body)


@unittest.skipUnless(BINARY.exists(), f"missing binary {BINARY}: cargo build --bin cairnflow")
class EndToEnd(unittest.IsolatedAsyncioTestCase):
    mode = "REQUEST_RESPONSE"
    #: ``serve`` = the built-in server; ``hypercorn`` = the ASGI app under hypercorn.
    via = "serve"

    async def asyncSetUp(self) -> None:
        if self.via == "hypercorn":
            await self.start_hypercorn()
        else:
            self.server = await start(Endpoint(SERVICES, protocol_mode=self.mode), port=0)
            self.sdk_url = f"http://127.0.0.1:{self.server.sockets[0].getsockname()[1]}"
        await self.start_runtime()

    async def start_hypercorn(self) -> None:
        import logging
        import socket

        import hypercorn.asyncio
        from hypercorn.config import Config

        from cairnflow import app

        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        config = Config()
        config.bind = [f"127.0.0.1:{port}"]
        config.accesslog = None
        self.hyper_errors: list[str] = []
        errors = self.hyper_errors

        class Catch(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                if record.levelno >= logging.ERROR:
                    errors.append(record.getMessage())

        for name in ("hypercorn.error", "asyncio"):
            logging.getLogger(name).addHandler(Catch())
        loop = asyncio.get_running_loop()
        default = loop.get_exception_handler()

        def on_error(lp: Any, ctx: dict[str, Any]) -> None:
            errors.append(str(ctx.get("message")) + " " + repr(ctx.get("exception")))

        loop.set_exception_handler(on_error)
        self.addCleanup(loop.set_exception_handler, default)
        self.stop_hyper = asyncio.Event()
        self.hyper = loop.create_task(
            hypercorn.asyncio.serve(app(Endpoint(SERVICES, protocol_mode=self.mode)), config, shutdown_trigger=self.stop_hyper.wait)  # type: ignore[arg-type]
        )
        for _ in range(100):
            try:
                _r, w = await asyncio.open_connection("127.0.0.1", port)
                w.close()
                break
            except OSError:
                await asyncio.sleep(0.02)
        self.sdk_url = f"http://127.0.0.1:{port}"

    async def start_runtime(self) -> None:
        self.data_dir = tempfile.mkdtemp(prefix="cairnflow-py-e2e-")
        # `CAIRNFLOW_E2E_ARGS` runs this very suite in another shape, e.g. a 4-partition
        # cluster: "--main-store memory:// --cluster --partitions 4 --tick-interval-ms 100".
        extra = os.environ.get("CAIRNFLOW_E2E_ARGS")
        args = extra.split() if extra else ["--data-dir", self.data_dir, "--tick-interval-ms", "50"]
        # A shared bucket: each test gets its own prefix, never another run's leases.
        args = [f"{a.rstrip('/')}/{secrets.token_hex(4)}" if a.startswith("s3://") else a for a in args]
        self.cluster = "--cluster" in args
        self.proc = subprocess.Popen(
            [str(BINARY), *args, "--port", "0", "--node-id", "py-e2e"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env={**os.environ, "CAIRNFLOW_ADMIN_KEY": ADMIN_KEY},
        )
        self.url = await asyncio.to_thread(self._read_url)
        for _ in range(100):
            try:
                status, _ = await call(f"{self.url}/health")
                if status == 200:
                    break
            except OSError:
                pass
            await asyncio.sleep(0.1)
        if self.cluster:
            # `/health` only says HTTP is up; wait until every partition has a leader.
            deadline = time.monotonic() + 30
            while True:
                _s, c = await call(f"{self.url}/cluster")
                if isinstance(c, dict) and c.get("partitions") and all(p.get("leader") for p in c["partitions"]):
                    break
                self.assertLess(time.monotonic(), deadline, f"a partition has no leader yet: {c}")
                await asyncio.sleep(0.1)
        status, reg = await call(f"{self.url}/deployments", "POST", {"uri": self.sdk_url})
        self.assertEqual(status, 200, reg)
        self.registration = reg

    def _read_url(self) -> str:
        assert self.proc.stdout is not None
        deadline = time.monotonic() + 30
        seen = ""
        while time.monotonic() < deadline:
            line = self.proc.stdout.readline().decode(errors="replace")
            if not line:
                raise AssertionError(f"runtime exited early:\n{seen}")
            seen += line
            if "listening on" in line:
                return "http://" + line.split("http://", 1)[1].split()[0].rstrip("/")
        raise AssertionError(f"runtime did not report a port:\n{seen}")

    async def asyncTearDown(self) -> None:
        self.proc.kill()
        self.proc.wait()
        if self.proc.stdout is not None:
            self.proc.stdout.close()
        if self.via == "hypercorn":
            self.stop_hyper.set()
            await asyncio.wait_for(self.hyper, 10)
        else:
            self.server.close()
            await self.server.wait_closed()
        shutil.rmtree(self.data_dir, ignore_errors=True)

    async def invoke(self, path: str, body: Any = None) -> dict[str, Any]:
        # A 503 (no leader for the partition yet, a lease changing hands) creates nothing:
        # back off and ask again, as the cluster-aware fetch of the TS e2e does.
        deadline = time.monotonic() + 20
        while True:
            status, r = await call(f"{self.url}/invoke/{path}", "POST", body)
            if status != 503 or time.monotonic() > deadline:
                break
            await asyncio.sleep(0.2)
        # 500 = the invocation finished failed (the body says so); anything else is an error.
        self.assertTrue(status in (200, 202) or (status == 500 and r.get("status") == "failed"), (status, r))
        return r

    async def settle(self, inv: dict[str, Any], timeout: float = 20) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        last: Any = inv
        while time.monotonic() < deadline:
            if last.get("status") in ("completed", "failed"):
                return last
            await asyncio.sleep(0.1)
            _s, last = await call(f"{self.url}/invocations/{inv['id']}")
        raise AssertionError(f"invocation did not finish: {last}")

    @staticmethod
    def output(st: dict[str, Any]) -> Any:
        out = st.get("output")
        return json.loads(out) if isinstance(out, str) else out

    async def test_end_to_end(self) -> None:
        reg = self.registration
        self.assertEqual(reg["protocolMode"], self.mode)
        self.assertEqual(sorted(reg["services"]), ["PyOrders", "PyPayments", "PySignup"])

        with self.subTest("state, durable sleep, ctx.run exactly once"):
            side_effects.clear()
            st = await self.settle(await self.invoke("PyOrders/submit?key=o-1", {"amount": 100}))
            self.assertEqual(st["status"], "completed", st)
            self.assertEqual(self.output(st), {"receipt": "receipt-100", "total": 100, "key": "o-1"})
            st = await self.settle(await self.invoke("PyOrders/submit?key=o-1", {"amount": 50}))
            self.assertEqual(self.output(st)["total"], 150)
            self.assertEqual(side_effects["o-1"], 2, "each invocation runs its own side effect exactly once")
            st = await self.settle(await self.invoke("PyOrders/peek?key=o-1"))
            self.assertEqual(self.output(st), {"total": 150, "keys": ["total"]})

        with self.subTest("eager state_keys, then a set and a sleep: replay without a journal mismatch"):
            st = await self.settle(await self.invoke("PyOrders/keys_across_sleep?key=o-1"))
            self.assertEqual(st["status"], "completed", st)
            self.assertEqual(self.output(st), {"keys": ["total"], "after": ["b", "total"]})

        with self.subTest("ctx.call and ctx.send"):
            st = await self.settle(await self.invoke("PyOrders/checkout?key=o-2", {"amount": 250}))
            self.assertEqual(self.output(st), {"paid": "paid-250"})
            deadline = time.monotonic() + 10
            while {"paid": "paid-250"} not in notes and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
            self.assertIn({"paid": "paid-250"}, notes)

        with self.subTest("a callee's TerminalError fails the caller"):
            st = await self.settle(await self.invoke("PyOrders/checkout?key=o-3", {"amount": 5000}))
            self.assertEqual(st["status"], "failed", st)
            self.assertIn("over quota", json.dumps(st))
            self.assertEqual(st.get("code"), 402, st)

        with self.subTest("parallel ctx.run under asyncio.gather"):
            st = await self.settle(await self.invoke("PyPayments/fanout", 4))
            self.assertEqual(self.output(st), [0, 1, 4, 9])

        with self.subTest("workflow: a shared handler resolves the main handler's promise"):
            first = await self.invoke("PySignup/run?key=u-1", "alice")
            await asyncio.sleep(0.3)
            st = await self.settle(await self.invoke("PySignup/stage?key=u-1"))
            self.assertEqual(self.output(st), "waiting")
            await self.settle(await self.invoke("PySignup/confirm?key=u-1", "a@example.com"))
            st = await self.settle(first)
            self.assertEqual(self.output(st), {"user": "alice", "email": "a@example.com"})

        with self.subTest("awakeable resolved from outside"):
            inv = await self.invoke("PyPayments/wait_awakeable")
            awk = await asyncio.wait_for(awakeables.get(), 10)
            status, r = await call(f"{self.url}/awakeables/{awk}/resolve", "POST", {"ok": True})
            self.assertIn(status, (200, 202), r)
            st = await self.settle(inv)
            self.assertEqual(self.output(st), {"ok": True})

        if self.via == "hypercorn":
            # No connection the runtime holds may have been dropped by the server.
            self.assertEqual(self.hyper_errors, [])


@unittest.skipUnless(BINARY.exists(), f"missing binary {BINARY}: cargo build --bin cairnflow")
@unittest.skipUnless("BIDI_STREAM" in MODES, "BIDI_STREAM needs the optional dependency h2")
class EndToEndBidi(EndToEnd):
    mode = "BIDI_STREAM"


try:
    import hypercorn  # noqa: F401

    HAVE_HYPERCORN = True
except ImportError:
    HAVE_HYPERCORN = False


@unittest.skipUnless(BINARY.exists(), f"missing binary {BINARY}: cargo build --bin cairnflow")
@unittest.skipUnless(HAVE_HYPERCORN, "the ASGI app under hypercorn needs hypercorn")
class EndToEndHypercorn(EndToEnd):
    via = "hypercorn"


@unittest.skipUnless(BINARY.exists(), f"missing binary {BINARY}: cargo build --bin cairnflow")
@unittest.skipUnless(HAVE_HYPERCORN and "BIDI_STREAM" in MODES, "BIDI over ASGI needs hypercorn and h2")
class EndToEndHypercornBidi(EndToEnd):
    via = "hypercorn"
    mode = "BIDI_STREAM"


if __name__ == "__main__":
    unittest.main()
