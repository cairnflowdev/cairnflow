# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""The turn state machine through ``run_turn``: pure, no network, every replay situation
built by hand."""

from __future__ import annotations

import asyncio
import json
import unittest
from dataclasses import dataclass
from typing import Any

import frames as F
from cairnflow import Context, PauseError, RunFailure, TerminalError, run_turn
from cairnflow._pb import fields
from cairnflow._wire import (
    ERROR_PAUSE,
    ERROR_RETRY,
    JOURNAL_MISMATCH,
    MT,
    decode_command_name,
    decode_suspension,
    encode_frame,
    encode_get_eager_state_keys,
    encode_get_state,
    encode_output,
    encode_run,
    encode_set_state,
    encode_sleep,
)


def output_value(out: bytes) -> object:
    for f in F.frames(out):
        if f.type == MT.OUTPUT:
            for n, _w, v in fields(f.payload):
                if n == 14:
                    for m, _x, c in fields(v):  # type: ignore[arg-type]
                        if m == 1:
                            return json.loads(c)  # type: ignore[arg-type]
                    return None
                if n == 15:
                    return ("failure", dict((m, c) for m, _x, c in fields(v)))  # type: ignore[arg-type]
    raise AssertionError("no Output frame")


class TurnTest(unittest.IsolatedAsyncioTestCase):
    async def test_output_then_end(self) -> None:
        async def h(ctx: Context, name: str) -> str:
            return f"hello {name}"

        out = await run_turn(h, F.start(1) + F.input_("ada"))
        self.assertEqual(F.types(out), [MT.OUTPUT, MT.END])
        self.assertEqual(output_value(out), "hello ada")

    async def test_handler_without_input(self) -> None:
        async def h(ctx: Context) -> int:
            return 42

        out = await run_turn(h, F.start(1) + F.input_(None), takes_input=False)
        self.assertEqual(output_value(out), 42)

    async def test_lazy_get_suspends_then_resumes(self) -> None:
        async def h(ctx: Context) -> object:
            return await ctx.get("count")

        out = await run_turn(h, F.start(1) + F.input_(), takes_input=False)
        self.assertEqual(F.types(out), [MT.GET_STATE, MT.SUSPENSION])
        self.assertEqual(decode_suspension(F.frames(out)[1].payload).completions, (1,))
        get = encode_frame(MT.GET_STATE, 0, encode_get_state("count", 1, "count"))
        body = F.start(3) + F.input_() + get + F.completion(MT.GET_STATE_COMPLETION, 1, 5)
        out = await run_turn(h, body, takes_input=False)
        self.assertEqual(F.types(out), [MT.OUTPUT, MT.END])
        self.assertEqual(output_value(out), 5)

    async def test_eager_state_answers_in_place(self) -> None:
        async def h(ctx: Context) -> object:
            n = await ctx.get("count")
            ctx.set("count", n + 1)
            return await ctx.state_keys()

        out = await run_turn(h, F.start(1, state={"count": 1}) + F.input_(), takes_input=False)
        self.assertEqual(
            F.types(out), [MT.GET_EAGER_STATE, MT.SET_STATE, MT.GET_EAGER_STATE_KEYS, MT.OUTPUT, MT.END]
        )
        self.assertEqual(output_value(out), ["count"])

    async def test_replayed_eager_keys_after_a_set_is_not_a_mismatch(self) -> None:
        """The runtime captures the eager snapshot when it loads the turn, so it already holds
        every key the journal set before the suspension; the key list embedded in a journaled
        ``GetEagerStateKeys`` is the one read back then.

        1. Turn 1: ``state_keys()`` answers ``[a]`` from the complete snapshot and journals it,
           then ``set("b")``, then ``sleep`` suspends.
        2. Turn 2: the snapshot is now ``{a, b}``; replay rebuilds ``GetEagerStateKeys`` from it.

        Must see: no 570 -- the embedded list (field 14) is a recorded result, not a parameter;
        replay returns the journaled ``[a]`` and the turn runs to ``Output``.
        """

        async def h(ctx: Context) -> object:
            keys = await ctx.state_keys()
            ctx.set("b", 2)
            await ctx.sleep(10)
            return keys

        body = (
            F.start(5, key="obj", state={"a": 1, "b": 2})
            + F.input_()
            + encode_frame(MT.GET_EAGER_STATE_KEYS, 0, encode_get_eager_state_keys([b"a"], "getKeys"))
            + encode_frame(MT.SET_STATE, 0, encode_set_state("b", b"2", "b"))
            + encode_frame(MT.SLEEP, 0, encode_sleep(1, 1, "sleep"))
            + F.completion(MT.SLEEP_COMPLETION, 1, void=True)
        )
        out = await run_turn(h, body, takes_input=False)
        self.assertEqual(F.types(out), [MT.OUTPUT, MT.END], out)
        self.assertEqual(output_value(out), ["a"])

    async def test_journal_mismatch_is_570_pause(self) -> None:
        async def h(ctx: Context) -> None:
            await ctx.sleep(1)

        get = encode_frame(MT.GET_STATE, 0, encode_get_state("count", 1, "count"))
        out = await run_turn(h, F.start(2) + F.input_() + get, takes_input=False)
        code, msg, behavior = F.error_of(out)
        self.assertEqual((code, behavior), (JOURNAL_MISMATCH, ERROR_PAUSE))
        self.assertIn("journal has GET_STATE", msg)

    async def test_swallowed_mismatch_still_570(self) -> None:
        async def h(ctx: Context) -> str:
            try:
                await ctx.sleep(1)
            except Exception:
                pass
            return "ok"

        get = encode_frame(MT.GET_STATE, 0, encode_get_state("count", 1, "count"))
        out = await run_turn(h, F.start(2) + F.input_() + get, takes_input=False)
        self.assertEqual(F.error_of(out)[0], JOURNAL_MISMATCH)
        self.assertNotIn(MT.OUTPUT, F.types(out))

    async def test_terminal_error_fails_the_invocation(self) -> None:
        async def h(ctx: Context) -> None:
            raise TerminalError("bad input", 400)

        out = await run_turn(h, F.start(1) + F.input_(), takes_input=False)
        self.assertEqual(F.types(out), [MT.OUTPUT, MT.END])
        self.assertEqual(output_value(out)[0], "failure")  # type: ignore[index]

    async def test_pause_error(self) -> None:
        async def h(ctx: Context) -> None:
            raise PauseError("restock needed")

        out = await run_turn(h, F.start(1) + F.input_(), takes_input=False)
        self.assertEqual(F.error_of(out)[2], ERROR_PAUSE)

    async def test_ordinary_error_is_retried(self) -> None:
        async def h(ctx: Context) -> None:
            raise RuntimeError("boom")

        out = await run_turn(h, F.start(1) + F.input_(), takes_input=False)
        code, msg, behavior = F.error_of(out)
        self.assertEqual((code, behavior), (500, ERROR_RETRY))
        self.assertIn("boom", msg)

    async def test_run_proposes_without_ack_in_rr_then_replays(self) -> None:
        calls = []

        async def h(ctx: Context) -> object:
            return await ctx.run("charge", lambda: calls.append(1) or "charged")

        out = await run_turn(h, F.start(1) + F.input_(), takes_input=False)
        fs = F.frames(out)
        self.assertEqual([f.type for f in fs], [MT.RUN, MT.PROPOSE_RUN_COMPLETION, MT.SUSPENSION])
        self.assertEqual(fs[1].flags, 0, "REQUEST_RESPONSE must not ask for an ack")
        run = encode_frame(MT.RUN, 0, encode_run(1, "charge"))
        body = F.start(3) + F.input_() + run + F.completion(MT.RUN_COMPLETION, 1, "charged")
        out = await run_turn(h, body, takes_input=False)
        self.assertEqual(output_value(out), "charged")
        self.assertEqual(calls, [1], "the closure must not rerun once its result is journaled")

    async def test_run_failure_is_a_durable_result(self) -> None:
        def fail() -> None:
            raise RunFailure("card declined", 402)

        async def h(ctx: Context) -> object:
            try:
                return await ctx.run("charge", fail)
            except TerminalError as e:
                return f"caught {e.code}"

        out = await run_turn(h, F.start(1) + F.input_(), takes_input=False)
        self.assertEqual(F.types(out), [MT.RUN, MT.PROPOSE_RUN_COMPLETION, MT.SUSPENSION])
        run = encode_frame(MT.RUN, 0, encode_run(1, "charge"))
        body = F.start(3) + F.input_() + run + F.completion(MT.RUN_COMPLETION, 1, failure=(402, "card declined"))
        self.assertEqual(output_value(await run_turn(h, body, takes_input=False)), "caught 402")

    async def test_transient_closure_error_wins_even_if_swallowed(self) -> None:
        def flaky() -> None:
            raise ConnectionError("vendor down")

        async def h(ctx: Context) -> str:
            try:
                await ctx.run("call vendor", flaky)
            except ConnectionError:
                pass
            return "carried on"

        out = await run_turn(h, F.start(1) + F.input_(), takes_input=False)
        self.assertNotIn(MT.PROPOSE_RUN_COMPLETION, F.types(out))
        self.assertNotIn(MT.OUTPUT, F.types(out))
        code, msg, behavior = F.error_of(out)
        self.assertEqual(behavior, ERROR_RETRY)
        self.assertIn("vendor down", msg)

    async def test_output_already_journaled_sends_only_end(self) -> None:
        async def h(ctx: Context) -> str:
            return "done"

        journaled = encode_frame(MT.OUTPUT, 0, encode_output(b'"done"', "output"))
        out = await run_turn(h, F.start(2) + F.input_() + journaled, takes_input=False)
        self.assertEqual(F.types(out), [MT.END])

    async def test_replay_only_stops_before_the_first_new_command(self) -> None:
        ran = []

        async def h(ctx: Context) -> None:
            await ctx.sleep(1)
            await ctx.run("side effect", lambda: ran.append(1))

        sleep = encode_frame(MT.SLEEP, 0, encode_sleep(5, 1, "sleep"))
        body = F.start(3, replay_only=True) + F.input_() + sleep + F.completion(MT.SLEEP_COMPLETION, 1, void=True)
        out = await run_turn(h, body, 3, takes_input=False)
        self.assertEqual(F.types(out), [MT.REPLAY_REPORT])
        self.assertEqual(ran, [])

    async def test_continue_as_new(self) -> None:
        async def h(ctx: Context, n: int) -> None:
            ctx.continue_as_new(n + 1)

        out = await run_turn(h, F.start(1) + F.input_(1), 3)
        self.assertEqual(F.types(out), [MT.CONTINUE_AS_NEW])

    async def test_parallel_sleeps_suspend_on_both(self) -> None:
        async def h(ctx: Context) -> str:
            await asyncio.gather(ctx.sleep(1), ctx.sleep(2))
            return "both"

        out = await run_turn(h, F.start(1) + F.input_(), takes_input=False)
        self.assertEqual(F.types(out), [MT.SLEEP, MT.SLEEP, MT.SUSPENSION])
        self.assertEqual(decode_suspension(F.frames(out)[2].payload).completions, (1, 2))

    async def test_race_picks_the_earliest_in_the_journal(self) -> None:
        async def h(ctx: Context) -> object:
            return await ctx.race([ctx.signal("a"), ctx.signal("b")])

        body = F.start(3) + F.input_() + F.signal(name="b", value="B") + F.signal(name="a", value="A")
        self.assertEqual(output_value(await run_turn(h, body, takes_input=False)), "B")

    async def test_cancel_signal_fails_the_next_wait_with_409(self) -> None:
        async def h(ctx: Context) -> object:
            try:
                await ctx.sleep(10)
            except TerminalError as e:
                return f"cleanup after {e.code}"
            return "slept"

        body = F.start(2) + F.input_() + F.signal(idx=1)
        self.assertEqual(output_value(await run_turn(h, body, takes_input=False)), "cleanup after 409")

    async def test_random_is_deterministic(self) -> None:
        seen: list[list[float]] = []

        async def h(ctx: Context) -> None:
            seen.append([ctx.random() for _ in range(3)])

        await run_turn(h, F.start(1, seed=42) + F.input_(), takes_input=False)
        await run_turn(h, F.start(1, seed=42) + F.input_(), takes_input=False)
        self.assertEqual(seen[0], seen[1])
        self.assertTrue(all(0 <= x < 1 for x in seen[0]))

    async def test_random_matches_the_typescript_sdk(self) -> None:
        """xorshift64* from seed 42, values produced by the TS SDK's ``makeRandom``."""
        got: list[float] = []

        async def h(ctx: Context) -> None:
            got.extend(ctx.random() for _ in range(3))

        await run_turn(h, F.start(1, seed=42) + F.input_(), takes_input=False)
        self.assertEqual(got, TS_RANDOM_SEED_42)

    async def test_asyncio_sleep_in_handler_does_not_hang(self) -> None:
        async def h(ctx: Context) -> str:
            await asyncio.sleep(0.01)
            return "ok"

        out = await asyncio.wait_for(run_turn(h, F.start(1) + F.input_(), takes_input=False), 5)
        self.assertEqual(output_value(out), "ok")

    async def test_io_outside_ctx_run_is_refused(self) -> None:
        async def h(ctx: Context) -> None:
            await asyncio.open_connection("127.0.0.1", 9)

        out = await run_turn(h, F.start(1) + F.input_(), takes_input=False)
        self.assertIn("must go through ctx.run", F.error_of(out)[1])

    async def test_async_closure_runs_on_the_real_loop(self) -> None:
        async def fetch() -> str:
            await asyncio.sleep(0.01)
            return "fetched"

        async def h(ctx: Context) -> object:
            return await ctx.run("fetch", fetch)

        out = await run_turn(h, F.start(1) + F.input_(), takes_input=False)
        self.assertEqual(F.types(out), [MT.RUN, MT.PROPOSE_RUN_COMPLETION, MT.SUSPENSION])

    async def test_input_becomes_a_dataclass(self) -> None:
        @dataclass
        class Order:
            amount: int

        async def h(ctx: Context, order: Order) -> int:
            assert isinstance(order, Order)
            return order.amount * 2

        out = await run_turn(h, F.start(1) + F.input_({"amount": 21}), input_type=Order)
        self.assertEqual(output_value(out), 42)

    async def test_call_names_and_scope_needs_wire_2(self) -> None:
        async def h(ctx: Context) -> object:
            return await ctx.scoped("tenant-a").call("Callee", "work", 1)

        out = await run_turn(h, F.start(1) + F.input_(), 1, takes_input=False)
        self.assertEqual(F.types(out), [MT.OUTPUT, MT.END])
        self.assertEqual(output_value(out)[0], "failure")  # type: ignore[index]
        out = await run_turn(h, F.start(1) + F.input_(), 2, takes_input=False)
        call = F.frames(out)[0]
        self.assertEqual(call.type, MT.CALL)
        self.assertEqual(decode_command_name(call.payload), "Callee/work")


class SideChannelTest(unittest.IsolatedAsyncioTestCase):
    """Wire v4 side channel: ``$log``/``$trace`` records and artifacts never become entries."""

    @staticmethod
    def appends(out: bytes) -> dict[str, list[dict[str, object]]]:
        streams: dict[str, list[dict[str, object]]] = {}
        for f in F.frames(out):
            if f.type != MT.STREAM_APPEND:
                continue
            name = ""
            for n, _w, v in fields(f.payload):
                if n == 1:
                    name = v.decode()  # type: ignore[union-attr]
                elif n == 3:
                    rec = {m: c for m, _x, c in fields(v)}  # type: ignore[arg-type]
                    streams.setdefault(name, []).append(json.loads(rec[4]))  # type: ignore[arg-type]
        return streams

    async def test_log_and_trace(self) -> None:
        async def h(ctx: Context) -> str:
            ctx.log.debug("hidden below info")
            ctx.log.info("hello", {"n": 1})
            async with ctx.trace.span("plan", {"model": "m"}) as span:
                span.set({"tokens": 3})
            return "ok"

        out = await run_turn(h, F.start(1) + F.input_(), 4, takes_input=False, clock=lambda: 1000)
        s = self.appends(out)
        self.assertEqual(s["$log"], [{"ts": 1000, "level": "info", "msg": "hello", "attrs": {"n": 1}}])
        start, end = s["$trace"]
        self.assertEqual((start["kind"], end["kind"], end["status"]), ("start", "end", "ok"))
        self.assertEqual(start["span_id"], end["span_id"])
        self.assertEqual(end["attrs"], {"tokens": 3})
        self.assertEqual(F.types(out)[-2:], [MT.OUTPUT, MT.END])

    async def test_streams_are_dropped_below_wire_4(self) -> None:
        async def h(ctx: Context) -> str:
            ctx.stream("ui").append({"tok": "a"})
            return "ok"

        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = await run_turn(h, F.start(1) + F.input_(), 3, takes_input=False)
        self.assertNotIn(MT.STREAM_APPEND, F.types(out))

    async def test_artifact_put_goes_before_the_command_that_carries_the_ref(self) -> None:
        async def h(ctx: Context) -> object:
            ref = ctx.artifact.put(b"big bytes", "text/plain")
            ctx.set("ref", ref.sha256)
            return ref.size

        out = await run_turn(h, F.start(1, state={"x": 1}) + F.input_(), 4, takes_input=False)
        self.assertEqual(F.types(out), [MT.ARTIFACT_PUT, MT.SET_STATE, MT.OUTPUT, MT.END])


class _Source:
    """A local artifact source for ``configure_artifacts``: ``route(path) -> (status, headers, body)``;
    a body of ``None`` streams zeros until the client hangs up."""

    def __init__(self, route: Any) -> None:
        import http.server
        import threading

        self.seen: list[str] = []
        self.streamed = 0
        outer = self

        class H(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                outer.seen.append(self.path)
                status, hdrs, body = route(self.path)
                self.send_response(status)
                for k, v in hdrs.items():
                    self.send_header(k, v)
                if body is None:
                    self.end_headers()
                    chunk = bytes(1 << 20)
                    try:
                        while outer.streamed < 256 << 20:
                            self.wfile.write(chunk)
                            outer.streamed += len(chunk)
                    except OSError:
                        pass
                    return
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - the base name
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


async def _get_all(refs: list[Any]) -> list[Any]:
    got: list[Any] = []

    async def h(ctx: Context) -> int:
        for r in refs:
            try:
                got.append(await ctx.artifact.get(r))
            except Exception as e:  # noqa: BLE001
                got.append(e)
        return 1

    await run_turn(h, F.start(1) + F.input_(), 4, takes_input=False)
    return got


def _sha(b: bytes) -> str:
    import hashlib

    return hashlib.sha256(b).hexdigest()


class ArtifactGetTest(unittest.IsolatedAsyncioTestCase):
    async def test_get_checks_the_download_against_the_ref(self) -> None:
        """``get`` trusted whatever the source answered: other bytes than the ref's sha256,
        another size, or an endless body read whole into memory; the bytes were also
        remembered under the sha. Must see: each is an error, and nothing is remembered."""
        from cairnflow import configure_artifacts
        from cairnflow.context import ArtifactRef

        real = b"the real artifact"
        want = _sha(real)

        def route(path: str) -> tuple[int, dict[str, str], bytes | None]:
            if path.endswith("/" + want):
                return 200, {}, b"tampered bytes!!!"
            if path.endswith("/" + _sha(b"sized")):
                return 200, {}, b"sized"
            if path.endswith("/" + _sha(b"endless")):
                return 200, {}, None
            return 404, {}, b""

        src = _Source(route)
        try:
            configure_artifacts(src.url, {"authorization": "Bearer k"})
            got = await _get_all(
                [
                    want,
                    ArtifactRef(want, len(real), "text/plain"),
                    ArtifactRef(_sha(b"sized"), 99, "text/plain"),
                    _sha(b"endless"),
                ]
            )
        finally:
            src.close()
        self.assertIsInstance(got[0], Exception, f"sha256 mismatch accepted: {got[0]!r}")
        self.assertIsInstance(got[1], Exception, "a mismatched body was remembered under the sha")
        self.assertIsInstance(got[2], Exception, "a size other than the ref's was accepted")
        self.assertIsInstance(got[3], Exception, "a body over the artifact limit was accepted")
        self.assertLess(src.streamed, 64 << 20, f"the SDK read {src.streamed} bytes of an endless body")

    async def test_get_refuses_a_malformed_sha_and_follows_no_redirect(self) -> None:
        """A ref that is not a sha256 builds another path at the source, with the tenant's
        key; a redirect would carry that key elsewhere. Must see: no request for a malformed
        sha, no redirect followed."""
        from cairnflow import configure_artifacts

        elsewhere = _Source(lambda path: (200, {}, b"elsewhere"))
        src = _Source(lambda path: (302, {"location": f"{elsewhere.url}/x"}, b""))
        try:
            configure_artifacts(src.url, {"authorization": "Bearer k"})
            got = await _get_all([_sha(b"redirected"), "../../admin/keys", "AB" * 32])
        finally:
            src.close()
            elsewhere.close()
        self.assertIsInstance(got[0], Exception, "a redirect was followed")
        self.assertEqual(elsewhere.seen, [], "the key reached another origin")
        self.assertIsInstance(got[1], Exception)
        self.assertIsInstance(got[2], Exception)
        self.assertEqual(len(src.seen), 1, f"requests for malformed shas: {src.seen}")


#: The TS SDK's makeRandom(42n) — computed with the same xorshift64* in Node.
TS_RANDOM_SEED_42 = [0.33908526400192196, 0.7822558479199243, 0.7901370452687786]


if __name__ == "__main__":
    unittest.main()
