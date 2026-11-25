# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""The HTTP surface: manifest, routing, compression, request identity, the replay cache."""

from __future__ import annotations

import gzip
import unittest
from typing import Any

import frames as F
from cairnflow import Context, Endpoint, HandlerOptions, RetryPolicy, Service, VirtualObject, Workflow, app
from cairnflow import _ed25519
from cairnflow._replay import ReplayCache, chain_root, chain_step
from cairnflow._wire import MT
from signer import Signer
from cairnflow.identity import (
    JWT_HEADER,
    SCHEME_HEADER,
    IdentityError,
    IdentityVerifier,
    body_hash,
    expected_jti,
)

svc = Service("S", options=HandlerOptions(retry_policy=RetryPolicy(max_attempts=3, initial_interval=0.5)))


@svc.handler()
async def hello(ctx: Context, name: str) -> str:
    return f"hi {name}"


@svc.handler()
async def nap(ctx: Context) -> str:
    await ctx.sleep(1)
    return "done"


obj = VirtualObject("O")


@obj.handler(kind="shared")
async def look(ctx: Context) -> Any:
    return await ctx.get("x")


wf = Workflow("W")


@wf.main()
async def run(ctx: Context) -> None:
    return None


@wf.handler()
async def poke(ctx: Context) -> None:
    return None


def headers(wire: int = 4, /, **extra: str) -> dict[str, str]:
    return {"content-type": f"application/vnd.cairnflow.invocation.v{wire}", **extra}


async def body_of(data: bytes) -> bytes:
    return data


async def answer(ep: Endpoint, path: str, data: bytes = b"", hs: dict[str, str] | None = None, method: str = "POST") -> Any:
    async def body() -> bytes:
        return data

    return await ep.answer(method, path, hs if hs is not None else headers(), body)


class ManifestTest(unittest.IsolatedAsyncioTestCase):
    def test_manifest_shape(self) -> None:
        m = Endpoint([svc, obj, wf]).manifest()
        self.assertEqual(m["protocolMode"], "REQUEST_RESPONSE")
        self.assertEqual((m["minProtocolVersion"], m["maxProtocolVersion"]), (1, 5), "no identity_keys, no segment_origins")
        self.assertEqual(m["requestCompression"], "gzip")
        self.assertNotIn("replayCache", m, "no identity_keys: no replay cache")
        s, o, w = m["services"]
        self.assertEqual((s["ty"], o["ty"], w["ty"]), ("SERVICE", "VIRTUAL_OBJECT", "WORKFLOW"))
        self.assertEqual(s["inactivityTimeout"], 900000)
        self.assertEqual((s["retryPolicyMaxAttempts"], s["retryPolicyInitialInterval"]), (3, 500))
        self.assertEqual(s["handlers"], [{"name": "hello"}, {"name": "nap"}])
        self.assertEqual(o["handlers"], [{"name": "look", "ty": "SHARED"}])
        self.assertEqual(w["handlers"], [{"name": "run", "ty": "WORKFLOW"}, {"name": "poke", "ty": "SHARED"}])

    def test_bidi_has_no_replay_cache(self) -> None:
        self.assertNotIn("replayCache", Endpoint([svc], protocol_mode="BIDI_STREAM").manifest())

    def test_definition_errors(self) -> None:
        with self.assertRaises(ValueError):
            Endpoint([Service("Empty")])
        with self.assertRaises(ValueError):
            Endpoint([svc, svc])
        bad = Workflow("NoMain")
        bad.handler()(poke)
        with self.assertRaises(ValueError):
            Endpoint([bad])
        with self.assertRaises(ValueError):
            Endpoint([svc], max_protocol_version=7)


class RoutingTest(unittest.IsolatedAsyncioTestCase):
    async def test_discover(self) -> None:
        a = await answer(Endpoint([svc]), "/discover", method="GET", hs={})
        self.assertEqual(a.status, 200)
        self.assertEqual(a.content_type, "application/vnd.cairnflow.endpointmanifest.v1+json")

    async def test_invoke(self) -> None:
        a = await answer(Endpoint([svc]), "/invoke/S/hello", F.start(1) + F.input_("bo"))
        self.assertEqual((a.status, a.content_type), (200, "application/vnd.cairnflow.invocation.v1"))
        self.assertEqual(F.types(a.body), [MT.OUTPUT, MT.END])

    async def test_unknown_route_is_404(self) -> None:
        for path in ("/invoke/S/nope", "/invoke/Nope/hello", "/other"):
            self.assertEqual((await answer(Endpoint([svc]), path)).status, 404, path)

    async def test_wire_too_new_is_415(self) -> None:
        a = await answer(Endpoint([svc], max_protocol_version=3), "/invoke/S/hello", b"", headers(4))
        self.assertEqual(a.status, 415)

    async def test_gzip_request_and_response(self) -> None:
        ep = Endpoint([svc])
        big = "x" * 20000
        hs = headers(**{"content-encoding": "gzip", "accept-encoding": "gzip"})
        a = await answer(ep, "/invoke/S/hello", gzip.compress(F.start(1) + F.input_(big)), hs)
        self.assertEqual(a.status, 200)
        self.assertEqual(a.headers.get("content-encoding"), "gzip")
        self.assertEqual(F.types(gzip.decompress(a.body)), [MT.OUTPUT, MT.END])
        self.assertEqual((await answer(ep, "/invoke/S/hello", b"\x1f\x8bnot gzip", hs)).status, 400)
        no = Endpoint([svc], compression=False)
        self.assertEqual((await answer(no, "/invoke/S/hello", gzip.compress(b"x"), hs)).status, 415)

    async def test_asgi(self) -> None:
        application = app([svc])
        sent: list[dict[str, Any]] = []
        msgs = [{"type": "http.request", "body": F.start(1) + F.input_("asgi"), "more_body": False}]

        async def receive() -> dict[str, Any]:
            return msgs.pop(0)

        async def send(m: dict[str, Any]) -> None:
            sent.append(m)

        scope = {
            "type": "http",
            "method": "POST",
            "path": "/invoke/S/hello",
            "headers": [(b"content-type", b"application/vnd.cairnflow.invocation.v4")],
        }
        await application(scope, receive, send)
        self.assertEqual(sent[0]["status"], 200)
        self.assertEqual(F.types(sent[1]["body"]), [MT.OUTPUT, MT.END])


# ─────────────────────────────────────────────────── request identity

# The runtime's golden vector (crates/cairn-server/src/identity.rs, golden_vector_is_stable):
# master cf_test_golden_vector_master_key, tenant acme, host sdk.example:8080, body golden-body,
# nonce golden-nonce-16b. The nonce claim is ignored: it only makes each signed request's JWT distinct.
GOLDEN_KID = "publickeyv1_3CPYZuphHAqFNsix9y91xhW8L6Cn9sTuL2tfTVDhZJdr"
GOLDEN_JWT = (
    "eyJhbGciOiJFZERTQSIsImtpZCI6InB1YmxpY2tleXYxXzNDUFladXBoSEFxRk5zaXg5eTkxeGhXOEw2Q245c1R1TDJ0ZlRWRGhaSmRyIiwidHlwIjoiSldUIn0"
    ".eyJhdWQiOiIvaW52b2tlL1MvaCIsImJzaCI6IlpUUDBpVUVFWDhCT1otdzdycVh0RzFwZHVMSDJWdXFWUGtpQ1o0OXR1ZzAiLCJleHAiOjE3MDAwMDAwNjAsImhvc3QiOiJzZGsuZXhhbXBsZTo4MDgwIiwiaWF0IjoxNzAwMDAwMDAwLCJqdGkiOiJhYjowIiwibmJmIjoxNjk5OTk5OTQwLCJub25jZSI6IloyOXNaR1Z1TFc1dmJtTmxMVEUyWWciLCJzdWIiOiJhY21lIn0"
    ".ye5aaOY8bgpblEegb7oSCNtv_Ue0Lx56LK40CuSZC9XaXOb9IwBKHxiLhzf2y3UQkNyjf2rHDErF4DBul47yAg"
)
GOLDEN_NOW = 1_700_000_000_123


class IdentityTest(unittest.IsolatedAsyncioTestCase):
    def test_golden_vector(self) -> None:
        c = IdentityVerifier([GOLDEN_KID]).verify({SCHEME_HEADER: "v1", JWT_HEADER: GOLDEN_JWT}.get, "/invoke/S/h", GOLDEN_NOW)
        self.assertEqual((c.aud, c.sub, c.jti, c.iat, c.nbf, c.exp), ("/invoke/S/h", "acme", "ab:0", 1700000000, 1699999940, 1700000060))
        self.assertEqual((c.host, c.bsh), ("sdk.example:8080", body_hash(b"golden-body")))

    def test_golden_vector_rejections(self) -> None:
        v = IdentityVerifier([GOLDEN_KID])
        ok = {SCHEME_HEADER: "v1", JWT_HEADER: GOLDEN_JWT}
        cases = [
            (ok, "/invoke/S/other", GOLDEN_NOW),
            (ok, "/invoke/S/h", GOLDEN_NOW + 61_000),
            (ok, "/invoke/S/h", GOLDEN_NOW - 61_000),
            ({**ok, JWT_HEADER: GOLDEN_JWT[:-4] + ("BBBB" if GOLDEN_JWT.endswith("AAAA") else "AAAA")}, "/invoke/S/h", GOLDEN_NOW),
            ({JWT_HEADER: GOLDEN_JWT}, "/invoke/S/h", GOLDEN_NOW),
            ({SCHEME_HEADER: "v1"}, "/invoke/S/h", GOLDEN_NOW),
        ]
        for h, path, now in cases:
            with self.assertRaises(IdentityError):
                v.verify(h.get, path, now)
        other = IdentityVerifier(["publickeyv1_2rAxB2eNFycA6hgt7trqW5UXrbn2yQvaK6TYfsZq3aBV"])
        with self.assertRaises(IdentityError):
            other.verify(ok.get, "/invoke/S/h", GOLDEN_NOW)
        with self.assertRaises(ValueError):
            IdentityVerifier(["not-a-kid"])

    def test_pure_python_and_accelerated_verify_agree(self) -> None:
        s = Signer()
        msg = b"payload"
        sig = _ed25519.sign(s.secret, msg)
        pub = _ed25519.public_key(s.secret)
        self.assertTrue(_ed25519._verify_pure(pub, msg, sig))
        self.assertFalse(_ed25519._verify_pure(pub, msg + b"!", sig))
        self.assertEqual(_ed25519.verify(pub, msg, sig), True)

    async def test_invoke_requires_a_signature_bound_to_the_start(self) -> None:
        s = Signer()
        ep = Endpoint([svc], identity_keys=[s.kid])
        self.assertEqual(ep.manifest()["requestIdentity"], "v1")
        body = F.start(1) + F.input_("id")
        jti = expected_jti(F.ID, 0)
        signed = {SCHEME_HEADER: "v1", "host": "127.0.0.1"}
        unsigned = await answer(ep, "/invoke/S/hello", body)
        self.assertEqual(unsigned.status, 401)
        good = await answer(ep, "/invoke/S/hello", body, headers(**signed, **{JWT_HEADER: s.jwt(jti=jti)}))
        self.assertEqual(good.status, 200, good.body)
        other_attempt = await answer(ep, "/invoke/S/hello", body, headers(**signed, **{JWT_HEADER: s.jwt(jti=jti[:-1] + "1")}))
        self.assertEqual(other_attempt.status, 401)
        bound = await answer(
            ep, "/invoke/S/hello", body, headers(**signed, **{JWT_HEADER: s.jwt(jti=jti, bsh=body_hash(b"other"))})
        )
        self.assertEqual(bound.status, 401)
        wrong_host = await answer(ep, "/invoke/S/hello", body, headers(**signed, **{JWT_HEADER: s.jwt(jti=jti, host="evil")}))
        self.assertEqual(wrong_host.status, 401)
        disc = await ep.answer("GET", "/discover", {**signed, JWT_HEADER: s.jwt(aud="/discover", jti="discover")}, lambda: body_of(b""))
        self.assertEqual(disc.status, 200)


# ─────────────────────────────────────────────────── replay cache (wire v5)


class ReplayTest(unittest.IsolatedAsyncioTestCase):
    def test_golden_vector(self) -> None:
        """Same values as ``cairn_runtime::replay`` and ``sdk/typescript/test/replay.test.ts``."""
        h0 = chain_root(F.ID)
        self.assertEqual(h0.hex(), "1f64388c3fc28f0042a5c57665b4b9a00b95f335a30260a54323acffdd41bc74")
        inp = bytes.fromhex("040000000000000972070a052272657122")
        self.assertEqual(chain_step(h0, inp).hex(), "caedb1a740dbe983c6a7f0a4be6a7f94e8f0b48fd0324664f1b51c6d3994367a")

    async def test_delta_hit_and_miss(self) -> None:
        rt = Signer()
        ep = Endpoint([svc], identity_keys=[rt.kid])

        def signed(body: bytes) -> dict[str, str]:
            return headers(5, **rt.headers("/invoke/S/nap", body))

        inp = F.input_()
        first = await answer(ep, "/invoke/S/nap", F.start(1) + inp, signed(F.start(1) + inp))
        self.assertEqual(first.headers.get("cairn-replay-cache"), "1")
        h1 = chain_step(chain_root(F.ID), inp)
        sleep = F.frames(first.body)[0]
        from cairnflow._wire import encode_frame

        delta = encode_frame(sleep.type, 0, sleep.payload) + F.completion(MT.SLEEP_COMPLETION, 1, void=True)
        body = F.start(3, replay_from=1, digest=h1) + delta
        hit = await answer(ep, "/invoke/S/nap", body, signed(body))
        self.assertEqual(hit.status, 200)
        self.assertEqual(F.types(hit.body), [MT.OUTPUT, MT.END])
        self.assertNotIn("cairn-replay-cache", hit.headers, "an ended invocation keeps nothing")
        miss = await answer(ep, "/invoke/S/nap", body, signed(body))
        self.assertEqual((miss.status, miss.headers.get("cairn-replay"), miss.body), (409, "miss", b""))

    def test_cache_is_bounded(self) -> None:
        c = ReplayCache(max_bytes=200)
        self.assertTrue(c.put("a", [b"x" * 50], [b"0" * 32, b"1" * 32]))
        self.assertTrue(c.put("b", [b"y" * 50], [b"0" * 32, b"1" * 32]))
        self.assertIsNone(c.held("a", 5))
        self.assertFalse(c.put("big", [b"z" * 500], [b"0" * 32]))



# ─────────────────────────────────────────────────── an endpoint without identity_keys

victim = Service("V")
seen: list[Any] = []


@victim.handler()
async def leak(ctx: Context, data: Any) -> str:
    seen.append(data)
    await ctx.sleep(1)
    return "done"


class UnverifiedTest(unittest.IsolatedAsyncioTestCase):
    async def test_no_identity_keeps_no_journal_to_splice(self) -> None:
        """The replay cache on an endpoint that does not verify request identity.

        1. The runtime runs a turn of invocation ``V`` (input ``victim-secret``), full
           replay; the endpoint keeps the journal prefix.
        2. A caller sends ``Start{id V, replay_from 1, prefix_digest garbage}``: ``409`` with
           ``cairn-replay-held: 1:h_1`` -- the id is running, and here is its digest.
        3. The caller sends ``Start{id V, replay_from 1, prefix_digest h_1}``: the endpoint
           splices the victim's prefix and runs the handler on ``victim-secret``.

        Must see: no ``replayCache`` in the manifest, no ``409``, and the handler never sees
        the victim's input a second time.
        """
        seen.clear()
        ep = Endpoint([victim])
        self.assertNotIn("replayCache", ep.manifest())
        secret = F.input_("victim-secret")
        first = await answer(ep, "/invoke/V/leak", F.start(1) + secret, headers(5))
        self.assertEqual(first.status, 200)
        self.assertEqual(seen, ["victim-secret"])
        probe = await answer(ep, "/invoke/V/leak", F.start(1, replay_from=1, digest=bytes(32)), headers(5))
        self.assertNotEqual(probe.status, 409, "a 409 tells the caller that the id is running")
        self.assertNotIn("cairn-replay-held", probe.headers, "the held digest lets the caller splice the prefix")
        h1 = chain_step(chain_root(F.ID), secret)
        await answer(ep, "/invoke/V/leak", F.start(1, replay_from=1, digest=h1), headers(5))
        self.assertEqual(seen.count("victim-secret"), 1, "the handler ran again on the victim's input, for the caller")

    def test_replay_cache_without_identity_is_a_configuration_error(self) -> None:
        with self.assertRaisesRegex(ValueError, "replay_cache needs identity_keys"):
            Endpoint([svc], replay_cache=True)
        with self.assertRaisesRegex(ValueError, "replay_cache needs identity_keys"):
            Endpoint([svc], replay_cache=ReplayCache(max_bytes=1 << 20))
        Endpoint([svc], replay_cache=False)
        self.assertEqual(Endpoint([svc], identity_keys=[Signer().kid]).manifest().get("replayCache"), True)

    def test_no_identity_and_no_segment_origins_serves_no_wire_v6(self) -> None:
        """Wire v6 makes the endpoint fetch the URLs in ``Start.segment_urls``; without
        ``identity_keys`` that body is the caller's, and without ``segment_origins`` any
        http(s) URL is fetched. Must see: v5 declared, pinning v6 refused, either option
        brings v6 back."""
        self.assertEqual(Endpoint([svc]).manifest()["maxProtocolVersion"], 5)
        self.assertEqual(Endpoint([svc], max_protocol_version=4).manifest()["maxProtocolVersion"], 4)
        with self.assertRaisesRegex(ValueError, "needs identity_keys or segment_origins"):
            Endpoint([svc], max_protocol_version=6)
        with_origins = Endpoint([svc], segment_origins=["https://values.cairn.example"])
        self.assertEqual(with_origins.manifest()["maxProtocolVersion"], 6)
        self.assertEqual(Endpoint([svc], identity_keys=[Signer().kid]).manifest()["maxProtocolVersion"], 6)
        self.assertEqual(Endpoint([svc], protocol_mode="BIDI_STREAM").manifest()["maxProtocolVersion"], 5)


if __name__ == "__main__":
    unittest.main()
