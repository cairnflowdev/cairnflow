# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""Request identity (section 4 of ``docs/protocol-v1.md``): the runtime signs every
``GET /discover`` and ``POST /invoke/...`` with the Ed25519 key of the tenant that owns
the deployment; an endpoint with ``identity_keys`` refuses anything else with 401
before running a handler.

Set ``identity_keys`` to the ``kid`` listed at the runtime's ``GET /identity``.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from . import _ed25519

SCHEME_HEADER = "x-cairn-signature-scheme"
SCHEME_V1 = "v1"
JWT_HEADER = "x-cairn-jwt-v1"
KID_PREFIX = "publickeyv1_"
DISCOVER_JTI = "discover"

_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


class IdentityError(Exception):
    """A request whose identity does not verify."""


def base58_decode(s: str) -> bytes | None:
    zeros = len(s) - len(s.lstrip("1"))
    n = 0
    for ch in s[zeros:]:
        i = _ALPHABET.find(ch)
        if i < 0:
            return None
        n = n * 58 + i
    body = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    return b"\x00" * zeros + body


def base58_encode(b: bytes) -> str:
    zeros = len(b) - len(b.lstrip(b"\x00"))
    n = int.from_bytes(b, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = _ALPHABET[r] + out
    return "1" * zeros + out


def b64url_encode(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def _b64url_decode(s: str) -> bytes:
    """Canonical base64url only: no padding, no other encodings of the same bytes."""
    if len(s) % 4 == 1 or any(c not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_" for c in s):
        raise ValueError("not canonical base64url")
    out = base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))
    if b64url_encode(out) != s:
        raise ValueError("not canonical base64url")
    return out


def body_hash(data: bytes) -> str:
    """The ``bsh`` claim: base64url (no padding) of SHA-256 of the request body."""
    return b64url_encode(hashlib.sha256(data).digest())


def expected_jti(invocation_id: bytes, retry_count: int) -> str:
    """The ``jti`` of ``/invoke``: binds the JWT to this very ``Start``."""
    return f"{invocation_id.hex()}:{retry_count}"


@dataclass(frozen=True)
class IdentityClaims:
    aud: str
    sub: str
    iat: int
    nbf: int
    exp: int
    jti: str
    kid: str
    host: str | None = None
    bsh: str | None = None


class IdentityVerifier:
    def __init__(self, kids: Iterable[str]) -> None:
        self.keys: dict[str, bytes] = {}
        for kid in kids:
            raw = base58_decode(kid[len(KID_PREFIX) :]) if kid.startswith(KID_PREFIX) else None
            if raw is None or len(raw) != 32:
                raise ValueError(f"identity_keys: {kid!r} is not {KID_PREFIX}<base58 of 32 bytes>")
            self.keys[kid] = raw
        if not self.keys:
            raise ValueError("identity_keys is empty")

    def verify(self, header: Callable[[str], str | None], path: str, now_ms: int | None = None) -> IdentityClaims:
        if header(SCHEME_HEADER) != SCHEME_V1:
            raise IdentityError(f"missing {SCHEME_HEADER}: {SCHEME_V1}")
        token = header(JWT_HEADER)
        if token is None:
            raise IdentityError(f"missing {JWT_HEADER}")
        parts = token.split(".")
        if len(parts) != 3:
            raise IdentityError("JWT does not have three parts")
        try:
            head = json.loads(_b64url_decode(parts[0]))
            claims = json.loads(_b64url_decode(parts[1]))
            sig = _b64url_decode(parts[2])
        except (ValueError, binascii.Error):
            raise IdentityError("JWT cannot be decoded") from None
        if not isinstance(head, dict) or not isinstance(claims, dict):
            raise IdentityError("JWT cannot be decoded")
        kid = head.get("kid")
        if head.get("alg") != "EdDSA" or not isinstance(kid, str):
            raise IdentityError("JWT is not EdDSA with a kid")
        key = self.keys.get(kid)
        if key is None:
            raise IdentityError(f"kid {kid} is not in identity_keys")
        if not _ed25519.verify(key, f"{parts[0]}.{parts[1]}".encode(), sig):
            raise IdentityError("invalid signature")

        def num(k: str) -> int:
            v = claims.get(k)
            if not isinstance(v, int) or isinstance(v, bool):
                raise IdentityError(f"claim {k} is missing")
            return v

        def text(k: str) -> str:
            v = claims.get(k)
            if not isinstance(v, str):
                raise IdentityError(f"claim {k} is missing")
            return v

        def opt(k: str) -> str | None:
            v = claims.get(k)
            if v is None:
                return None
            if not isinstance(v, str):
                raise IdentityError(f"claim {k} is not a string")
            return v

        c = IdentityClaims(
            text("aud"), text("sub"), num("iat"), num("nbf"), num("exp"), text("jti"), kid, opt("host"), opt("bsh")
        )
        if c.aud != path:
            raise IdentityError(f"aud {c.aud} is not {path}")
        now = (int(time.time() * 1000) if now_ms is None else now_ms) // 1000
        if now < c.nbf or now > c.exp:
            raise IdentityError("JWT is outside its nbf..exp window")
        return c


def check(
    verifier: IdentityVerifier | None,
    header: Callable[[str], str | None],
    path: str,
    jti: str | Callable[[str], bool],
    host: str | None = None,
    hosts: Iterable[str] = (),
    body: bytes | None = None,
) -> IdentityClaims | str | None:
    """Claims when the request verifies, ``None`` without a verifier, or why it does not."""
    if verifier is None:
        return None
    try:
        c = verifier.verify(header, path)
    except IdentityError as e:
        return str(e)
    if (c.jti != jti) if isinstance(jti, str) else not jti(c.jti):
        return "jti does not match the request"
    # A present claim must match (old runtimes do not sign it). Without identity_hosts,
    # compare with the request's own host; a request with no host is refused.
    if c.host is not None:
        trusted = list(hosts) or ([host] if host is not None else [])
        if not any(h.lower() == c.host.lower() for h in trusted):
            return f"JWT was signed for host {c.host}"
    if c.bsh is not None and body is not None and body_hash(body) != c.bsh:
        return "body does not match the signed hash"
    return c


class UsedTokens:
    """One JWT, one stream: a ``BIDI_STREAM`` token cannot open a second stream."""

    def __init__(self, max_entries: int = 100_000) -> None:
        self._seen: dict[str, int] = {}
        self._max = max_entries

    def claim(self, token: str, exp_s: int, now_ms: int | None = None) -> bool:
        if token in self._seen:
            return False
        now = int(time.time() * 1000) if now_ms is None else now_ms
        if len(self._seen) >= self._max:
            for k in [k for k, exp in self._seen.items() if exp * 1000 <= now]:
                del self._seen[k]
            if len(self._seen) >= self._max:
                return False
        self._seen[token] = exp_s
        return True
