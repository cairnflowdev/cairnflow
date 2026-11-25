# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""The HTTP surface of a deployment, independent of any server: ``GET /discover`` and
``POST /invoke/{service}/{handler}`` (sections 3 and 4 of ``docs/protocol-v1.md``).

``asgi.py`` and ``serve.py`` connect it to a server; tests call :meth:`Endpoint.answer`
directly.
"""

from __future__ import annotations

import json
import os
import re
import urllib.parse
import zlib
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from ._invoke import MAX_PROTOCOL_VERSION, MIN_PROTOCOL_VERSION, run_turn
from ._replay import ReplayCache, chain_root, extend_chain
from ._values import DEFAULT_MAX_TURN_VALUE_BYTES, DEFAULT_MAX_VALUE_BYTES, WIRE_SEGMENTED, Fetch, SegmentCache, ValueOptions
from ._wire import MT, Frame, FrameError, decode_frames, decode_start, encode_frame, frames_to_bytes
from .identity import DISCOVER_JTI, JWT_HEADER, SCHEME_V1, IdentityClaims, IdentityVerifier, UsedTokens, body_hash, check, expected_jti
from .service import DEFAULT_INACTIVITY_TIMEOUT_MS, AnyService, HandlerDef

DISCOVERY_V1 = "application/vnd.cairnflow.endpointmanifest.v1+json"
INVOCATION_V1 = "application/vnd.cairnflow.invocation.v1"
#: Bodies above this are refused (413).
MAX_REQUEST_BODY = 256 << 20
#: Responses from this size are gzip-compressed when the runtime accepts it.
COMPRESS_MIN = 8 << 10
#: Wire v5: incremental replay for REQUEST_RESPONSE.
WIRE_REPLAY_DELTA = 5

_INVOKE = re.compile(r"^/invoke/([^/]+)/([^/]+)$")
_WIRE = re.compile(r"^application/vnd\.cairnflow\.invocation\.v(\d+)$")


def wire_version_of(content_type: str | None) -> int:
    """The wire version a content-type asks for. Unreadable = v1, the safe side."""
    media = (content_type or "").split(";")[0].strip().lower()
    m = _WIRE.match(media)
    return int(m.group(1)) if m else MIN_PROTOCOL_VERSION


def accepts_gzip(v: str | None) -> bool:
    return any(p.strip().split(";")[0].strip().lower() == "gzip" for p in (v or "").split(","))


class BodyTooLarge(Exception):
    pass


@dataclass
class Answer:
    status: int
    content_type: str
    body: bytes
    headers: dict[str, str] = field(default_factory=dict)


def _fly_machine_id() -> str | None:
    """Affinity hint (section 9.3): the runtime routes the next turn to this machine."""
    v = os.environ.get("FLY_MACHINE_ID")
    return v if v is not None and re.fullmatch(r"[A-Za-z0-9]{1,64}", v) else None


def _json_answer(status: int, body: Any, content_type: str = "application/json") -> Answer:
    return Answer(status, content_type, json.dumps(body, separators=(",", ":")).encode())


def _gzip(data: bytes) -> bytes:
    c = zlib.compressobj(wbits=31)
    return c.compress(data) + c.flush()


def _gzip_frames(data: bytes) -> bytes | None:
    """One gzip member per frame, concatenated (the runtime reads multi-member gzip)."""
    parts: list[bytes] = []
    at = 0
    while at < len(data):
        if len(data) - at < 8:
            return None
        end = at + 8 + int.from_bytes(data[at + 4 : at + 8], "big")
        if end > len(data):
            return None
        parts.append(_gzip(data[at:end]))
        at = end
    return b"".join(parts) if parts else None


def _gunzip(data: bytes, limit: int) -> bytes | None:
    """``None`` when the result exceeds ``limit``; raises ``zlib.error`` when corrupt."""
    out = bytearray()
    d = zlib.decompressobj(wbits=31)
    rest = data
    while rest:
        out += d.decompress(rest, limit + 1 - len(out))
        if len(out) > limit:
            return None
        if d.unconsumed_tail:
            rest = d.unconsumed_tail
            continue
        if d.eof and d.unused_data:
            rest = d.unused_data  # the next gzip member
            d = zlib.decompressobj(wbits=31)
            continue
        break
    if not d.eof:
        raise zlib.error("truncated gzip body")
    return bytes(out)


class Endpoint:
    """A set of services served under one protocol mode.

    :param protocol_mode: ``"REQUEST_RESPONSE"`` (HTTP/1.1, works behind any proxy or on
        serverless) or ``"BIDI_STREAM"`` (h2c, one long stream per invocation).
    :param max_protocol_version: pin the wire version below this SDK's maximum. Default: the
        maximum when ``identity_keys`` or ``segment_origins`` is set, otherwise 5 -- wire v6
        (segmented values) makes the endpoint fetch URLs from the request, so an endpoint that
        verifies neither who sends them nor where they point does not serve it, and pinning v6
        there is a ``ValueError``.
    :param compression: accept gzip request bodies and compress large responses.
    :param replay_cache: keep the journals of running invocations in memory and accept
        deltas (wire v5, ``REQUEST_RESPONSE`` only): ``True``, ``False`` or a
        :class:`ReplayCache` with its own limits. Default: on exactly when
        ``identity_keys`` is set. Asking for it without ``identity_keys`` is a
        ``ValueError``: a delta names an invocation id, so an unverified caller could probe
        for running ids and run the handler on another invocation's held journal.
    :param identity_keys: the ``kid`` values (``GET /identity`` of the runtime) whose
        signature this endpoint requires; ``None`` = no verification (warned once).
    :param identity_hosts: hosts a signed ``host`` claim may name; default: the request's
        own ``Host``.
    :param segment_origins: allowed origins of segment read URLs (wire v6), e.g. the
        runtime's value read origin; empty = any http(s) origin, which only ``identity_keys``
        makes safe (an endpoint with neither does not serve wire v6).
    :param value_cache_bytes: size of the process-wide segment cache.
    :param segment_fetch: ``async (url, max_bytes) -> bytes``, replaces the default reader.
    :param clock: the SDK's wall clock in ms (tests fix it).
    """

    def __init__(
        self,
        services: Iterable[AnyService],
        *,
        protocol_mode: str = "REQUEST_RESPONSE",
        max_protocol_version: int | None = None,
        compression: bool = True,
        replay_cache: bool | ReplayCache | None = None,
        identity_keys: Iterable[str] | None = None,
        identity_hosts: Iterable[str] = (),
        segment_origins: Iterable[str] = (),
        value_cache_bytes: int | None = None,
        segment_fetch: Fetch | None = None,
        max_value_bytes: int = DEFAULT_MAX_VALUE_BYTES,
        max_turn_value_bytes: int = DEFAULT_MAX_TURN_VALUE_BYTES,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self.services = list(services)
        if protocol_mode not in ("REQUEST_RESPONSE", "BIDI_STREAM"):
            raise ValueError(f"protocol_mode must be REQUEST_RESPONSE or BIDI_STREAM, got {protocol_mode!r}")
        origins = tuple(segment_origins)
        verified = identity_keys is not None
        if max_protocol_version is None:
            max_protocol_version = MAX_PROTOCOL_VERSION if verified or origins else WIRE_SEGMENTED - 1
        if not MIN_PROTOCOL_VERSION <= max_protocol_version <= MAX_PROTOCOL_VERSION:
            raise ValueError(
                f"max_protocol_version {max_protocol_version} is outside "
                f"[{MIN_PROTOCOL_VERSION}, {MAX_PROTOCOL_VERSION}]"
            )
        if max_protocol_version >= WIRE_SEGMENTED and not verified and not origins:
            raise ValueError(
                f"max_protocol_version {max_protocol_version} (segmented values) needs identity_keys or "
                "segment_origins: without them anyone who reaches this endpoint can make it fetch URLs of their choosing"
            )
        names: dict[str, AnyService] = {}
        for s in self.services:
            if not s.handlers:
                raise ValueError(f"service {s.name!r} has no handlers")
            if s.name in names:
                raise ValueError(f"service {s.name!r} is defined twice")
            if s.type == "WORKFLOW" and sum(h.ty == "WORKFLOW" for h in s.handlers.values()) != 1:
                raise ValueError(f"workflow {s.name!r} must have exactly one main handler")
            names[s.name] = s
        self._by_name = names
        self.mode = protocol_mode
        self.max_wire = max_protocol_version
        self.compression = compression
        self.clock = clock
        self.verifier = IdentityVerifier(identity_keys) if identity_keys is not None else None
        self.identity_hosts = tuple(identity_hosts)
        self.used_tokens = UsedTokens()
        self.values = ValueOptions(
            cache=None if value_cache_bytes is None else SegmentCache(value_cache_bytes),
            fetch=segment_fetch,
            origins=origins,
            max_value_bytes=max_value_bytes,
            max_turn_value_bytes=max_turn_value_bytes,
        )
        # The cache needs request identity: a delta names an invocation id and a prefix
        # digest, and a 409 reports what is held -- without identity anyone who reaches the
        # endpoint could probe for running ids and run the handler on a held journal.
        self.replay: ReplayCache | None = None
        usable = protocol_mode == "REQUEST_RESPONSE" and self.max_wire >= WIRE_REPLAY_DELTA
        want = replay_cache if replay_cache is not None else self.verifier is not None
        if want is not False and usable and self.verifier is None:
            raise ValueError(
                "replay_cache needs identity_keys: without request identity anyone who reaches this endpoint "
                "can replay the journal of a running invocation it holds; set identity_keys or leave replay_cache off"
            )
        if want is not False and usable:
            self.replay = want if isinstance(want, ReplayCache) else ReplayCache()

    # ── discovery ────────────────────────────────────────────────────────

    def manifest(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "protocolMode": self.mode,
            "minProtocolVersion": MIN_PROTOCOL_VERSION,
            "maxProtocolVersion": self.max_wire,
        }
        if self.verifier is not None:
            out["requestIdentity"] = SCHEME_V1
        if self.compression:
            out["requestCompression"] = "gzip"
        if self.replay is not None:
            out["replayCache"] = True
        out["services"] = [
            {
                "name": s.name,
                "ty": s.type,
                "inactivityTimeout": DEFAULT_INACTIVITY_TIMEOUT_MS,
                **(s.options.contract() if s.options is not None else {}),
                "handlers": [
                    {
                        "name": h.name,
                        **({"ty": h.ty} if h.ty is not None else {}),
                        **(h.options.contract() if h.options is not None else {}),
                    }
                    for h in s.handlers.values()
                ],
            }
            for s in self.services
        ]
        return out

    # ── routing ──────────────────────────────────────────────────────────

    def wire_of(self, content_type: str | None) -> int:
        return min(wire_version_of(content_type), self.max_wire)

    def too_new(self, content_type: str | None) -> str | None:
        asked = wire_version_of(content_type)
        if asked <= self.max_wire:
            return None
        return (
            f"the runtime speaks wire v{asked}, this SDK supports up to v{self.max_wire}; "
            "register the deployment again"
        )

    def route(self, method: str, path: str) -> tuple[AnyService, HandlerDef] | None:
        m = _INVOKE.match(path)
        if method != "POST" or m is None:
            return None
        svc = self._by_name.get(urllib.parse.unquote(m.group(1)))
        if svc is None:
            return None
        h = svc.handlers.get(urllib.parse.unquote(m.group(2)))
        return None if h is None else (svc, h)

    def check_bidi(self, headers: Mapping[str, str]) -> tuple[int, str] | None:
        """Why a ``BIDI_STREAM`` request is refused before its stream opens, if it is."""
        enc = (headers.get("content-encoding") or "").strip().lower()
        if enc not in ("", "identity") and not (enc == "gzip" and self.compression):
            return 415, f"content-encoding {enc} is not enabled (requestCompression)"
        too_new = self.too_new(headers.get("content-type"))
        if too_new is not None:
            return 415, too_new
        return None

    def identify(
        self,
        headers: Mapping[str, str],
        signed_path: str,
        jti: str | Callable[[str], bool],
        body: bytes | None = None,
    ) -> IdentityClaims | str | None:
        """Claims, ``None`` without verification, or why the request is refused."""
        host = headers.get("host") or headers.get(":authority")
        return check(self.verifier, headers.get, signed_path, jti, host, self.identity_hosts, body)

    def open_bidi(self, headers: Mapping[str, str], signed_path: str) -> tuple[int, str] | IdentityClaims | None:
        """Whether a ``BIDI_STREAM`` request may open its stream: a refusal, or the claims
        the first frames must still match (``jti`` is only known once ``Start`` arrives)."""
        refusal = self.check_bidi(headers)
        if refusal is not None:
            return refusal
        claims = self.identify(headers, signed_path, lambda _jti: True)
        if isinstance(claims, str):
            return 401, claims
        if claims is not None:
            sig = (headers.get(JWT_HEADER) or "").split(".")[-1]
            if not self.used_tokens.claim(sig, claims.exp):
                return 401, "this request identity token has already opened a stream"
        return claims

    def gate_bidi(self, claims: IdentityClaims | None, io: Any, reject: Callable[[], None]) -> Any:
        """The frame stream a ``BIDI_STREAM`` turn loop reads: with verification, the
        first frame must be the ``Start`` the JWT was issued for."""
        return io if claims is None else JtiGate(io, claims.jti, claims.bsh, reject)

    async def answer(
        self,
        method: str,
        path: str,
        headers: Mapping[str, str],
        body: Callable[[], Awaitable[bytes]],
        signed_path: str | None = None,
    ) -> Answer:
        """Answers one request. ``signed_path`` is the path the runtime signed, including
        any mount prefix (defaults to ``path``). ``headers`` has lower-case names; ``body`` reads the whole
        request body (raising :class:`BodyTooLarge` past ``MAX_REQUEST_BODY``)."""
        signed = signed_path or path
        if method == "GET" and path == "/discover":
            who = self.identify(headers, signed, DISCOVER_JTI)
            if isinstance(who, str):
                return _json_answer(401, {"error": who})
            return _json_answer(200, self.manifest(), DISCOVERY_V1)
        routed = self.route(method, path)
        if routed is None:
            return _json_answer(404, {"error": f"no route for {method} {path}"})
        svc, h = routed
        too_new = self.too_new(headers.get("content-type"))
        if too_new is not None:
            return _json_answer(415, {"error": too_new})
        # The header signature BEFORE reading the body: a stranger cannot push a body of
        # any size into memory. jti and bsh are checked against the body afterwards.
        pre = self.identify(headers, signed, lambda _jti: True)
        if isinstance(pre, str):
            return _json_answer(401, {"error": pre})
        declared = headers.get("content-length")
        if declared is not None and declared.isdigit() and int(declared) > MAX_REQUEST_BODY:
            return _json_answer(413, {"error": f"body exceeds {MAX_REQUEST_BODY} bytes"})
        try:
            data = await body()
        except BodyTooLarge:
            return _json_answer(413, {"error": f"body exceeds {MAX_REQUEST_BODY} bytes"})
        enc = (headers.get("content-encoding") or "").strip().lower()
        if enc not in ("", "identity"):
            if enc != "gzip" or not self.compression:
                return _json_answer(415, {"error": f"content-encoding {enc} is not enabled (requestCompression)"})
            # A platform that already decompressed the body leaves the header as is.
            if data[:2] == b"\x1f\x8b":
                try:
                    raw = _gunzip(data, MAX_REQUEST_BODY)
                except zlib.error:
                    return _json_answer(400, {"error": "corrupt gzip body"})
                if raw is None:
                    return _json_answer(413, {"error": f"body exceeds {MAX_REQUEST_BODY} bytes after decompression"})
                data = raw
        if self.verifier is not None:

            def same_start(claimed: str) -> bool:
                try:
                    first = decode_frames(data)[0]
                except (FrameError, IndexError):
                    return False
                if first.type != MT.START:
                    return False
                st = decode_start(first.payload)
                return expected_jti(st.id, st.retry_count) == claimed

            who = self.identify(headers, signed, same_start, data)
            if isinstance(who, str):
                return _json_answer(401, {"error": who})
        wire = self.wire_of(headers.get("content-type"))
        keep: tuple[str, list[bytes], list[bytes]] | None = None
        if self.replay is not None and wire >= WIRE_REPLAY_DELTA:
            joined = self._rejoin(data)
            if isinstance(joined, Answer):
                return joined
            data, keep = joined
        out = await run_turn(
            h.fn,
            data,
            wire,
            self.values,
            clock=self.clock,
            log_level=svc.log_level(h.name),
            input_type=h.input_type,
            takes_input=h.takes_input,
        )
        extra: dict[str, str] = {}
        if keep is not None:
            k = self._keep(keep, out)
            if k > 0:
                extra["cairn-replay-cache"] = str(k)
                fly = _fly_machine_id()
                if fly is not None:
                    extra["cairn-affinity"] = f"fly={fly}"
        if self.compression and len(out) >= COMPRESS_MIN and accepts_gzip(headers.get("accept-encoding")):
            z = _gzip_frames(out)
            if z is not None and len(z) < len(out):
                return Answer(200, INVOCATION_V1, z, {"content-encoding": "gzip", **extra})
        return Answer(200, INVOCATION_V1, out, extra)

    def _rejoin(self, data: bytes) -> Answer | tuple[bytes, tuple[str, list[bytes], list[bytes]] | None]:
        """Joins the cached prefix with a delta body (section 9.3), or answers a miss."""
        cache = self.replay
        assert cache is not None
        try:
            frames = decode_frames(data)
        except FrameError:
            return data, None  # the turn reports the broken body itself
        if not frames or frames[0].type != MT.START:
            return data, None
        start = decode_start(frames[0].payload)
        n, b, key = start.known_entries, start.replay_from, start.id.hex()
        raw = [encode_frame(f.type, f.flags, f.payload) for f in frames]
        rest = raw[1:]
        if b == 0:
            # Full replay: missing entries make the turn fail by itself; keep nothing.
            if len(rest) < n:
                return data, None
            chain = [chain_root(start.id)]
            extend_chain(chain, rest[:n])
            return data, (key, rest[:n], chain)
        held = cache.take(key, b, start.prefix_digest) if b <= n and len(rest) >= n - b else None
        if held is None:
            miss = {"cairn-replay": "miss"}
            have = cache.held(key, n)
            if have is not None:
                miss["cairn-replay-held"] = f"{have[0]}:{have[1].hex()}"
            return Answer(409, INVOCATION_V1, b"", miss)
        prefix, chain = held
        delta = rest[: n - b]
        chain = list(chain)
        extend_chain(chain, delta)
        return raw[0] + b"".join(prefix) + b"".join(rest), (key, prefix + delta, chain)

    def _keep(self, keep: tuple[str, list[bytes], list[bytes]], out: bytes) -> int:
        """Keeps the entries received from the runtime; drops them when the turn ended the
        invocation. Returns how many entries are kept."""
        cache = self.replay
        assert cache is not None
        key, entries, chain = keep
        ended = any(f.type in (MT.END, MT.ERROR) for f in decode_frames(out))
        if ended or not entries:
            cache.delete(key)
            return 0
        return len(entries) if cache.put(key, entries, chain) else 0


class JtiGate:
    """Wraps a ``BIDI_STREAM`` frame stream: the first frame must be the ``Start`` the JWT
    was issued for (``jti``), and with ``bsh`` the opening frames must hash to it."""

    def __init__(self, inner: Any, jti: str, bsh: str | None, reject: Callable[[], None]) -> None:
        self._inner = inner
        self._jti = jti
        self._bsh = bsh
        self._reject = reject
        self._first = True
        self._pending: list[Frame] = []

    @property
    def watcher(self) -> Any:
        return self._inner.watcher

    @watcher.setter
    def watcher(self, w: Any) -> None:
        self._inner.watcher = w

    async def next(self) -> Frame | None:
        if self._pending:
            return self._pending.pop(0)
        f = await self._inner.next()
        if f is None or not self._first:
            return f
        self._first = False
        if f.type != MT.START:
            self._reject()
            return None
        start = decode_start(f.payload)
        if expected_jti(start.id, start.retry_count) != self._jti:
            self._reject()
            return None
        if self._bsh is None:
            return f
        frames = [f]
        size = HEADER + len(f.payload)
        for _ in range(start.known_entries):
            g = await self._inner.next()
            if g is None:
                self._reject()
                return None
            size += HEADER + len(g.payload)
            if size > MAX_REQUEST_BODY:
                self._reject()
                return None
            frames.append(g)
        if body_hash(frames_to_bytes(frames)) != self._bsh:
            self._reject()
            return None
        self._pending = frames[1:]
        return f

    def buffered(self) -> list[Frame]:
        out, self._pending = self._pending, []
        return out + self._inner.buffered()

    def write(self, data: bytes) -> None:
        self._inner.write(data)

    def end(self) -> None:
        self._inner.end()


HEADER = 8
