# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""A signing key standing in for the runtime's request identity, for tests that need an
endpoint with ``identity_keys``: the replay cache and wire v6 are served only when the
endpoint verifies who is calling."""

from __future__ import annotations

import json
import time
from typing import Any

import _path  # noqa: F401  (src/ on sys.path)
from cairnflow import _ed25519
from cairnflow._wire import MT, decode_frames, decode_start
from cairnflow.identity import JWT_HEADER, SCHEME_HEADER, b64url_encode, base58_encode, expected_jti


class Signer:
    """An Ed25519 key and JWTs signed with it."""

    def __init__(self, secret: bytes = bytes(range(1, 33))) -> None:
        self.secret = secret
        self.kid = "publickeyv1_" + base58_encode(_ed25519.public_key(self.secret))

    def jwt(self, **claims: Any) -> str:
        now = int(time.time())
        c = {"aud": "/invoke/S/hello", "sub": "acme", "iat": now, "nbf": now - 60, "exp": now + 60, **claims}
        head = b64url_encode(json.dumps({"alg": "EdDSA", "typ": "JWT", "kid": self.kid}).encode())
        payload = b64url_encode(json.dumps(c).encode())
        sig = _ed25519.sign(self.secret, f"{head}.{payload}".encode())
        return f"{head}.{payload}.{b64url_encode(sig)}"

    def headers(self, path: str, body: bytes | None = None) -> dict[str, str]:
        """Identity headers for a request to ``path``: ``/discover``, or ``/invoke/S/h`` opening with ``body``."""
        jti = "discover"
        if body is not None:
            first = decode_frames(body)[0]
            assert first.type == MT.START, "the body does not open with Start"
            st = decode_start(first.payload)
            jti = expected_jti(st.id, st.retry_count)
        return {SCHEME_HEADER: "v1", JWT_HEADER: self.jwt(aud=path, jti=jti)}
