# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""A small async HTTP/1.1 client, plus the retry layer every ingress client shares.

The HTTP part runs on ``asyncio.open_connection`` (``ssl`` for ``https://``), one
connection per request (``Connection: close``). It reads ``Content-Length``, chunked and
read-until-close bodies, and has a streaming mode for Server-Sent Events.

The retry part (:class:`Transport`) implements the cluster's error contract
(``docs/admin-api.md``, the error contract section):

- ``503`` with ``leader`` => ask again at that leader, **one hop per attempt**, only when
  it has the same scheme as the original ``url`` (a Bearer key does not go to ``http://``
  from an ``https://``); can be turned off with ``follow_leader=False``;
- ``503`` without ``leader`` (the cluster is electing, a range is changing) => capped
  exponential backoff, then ask the original node again;
- ``429`` **without** ``id`` (``range_overloaded``, ``sse_limit``: nothing created yet) =>
  wait ``Retry-After`` and resend; ``429`` with ``id`` (``throttled``: already durable) =>
  return it to the caller, resending is its job (usually an attach by id);
- a network error at a followed node => back off and return to the original node, and do
  not follow that node again in this call: the original node keeps pointing at the old
  leader until its lease expires, so following again would burn every attempt;
- a cap on attempts **and** a cap on time; past the cap, return the last response or
  raise the last network error.

A request that creates an invocation must carry an ``Idempotency-Key`` so that a resend
points at that same invocation. The transport does not add one; :func:`cairnflow.client.connect`
does.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import re
import ssl
import time
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from urllib.parse import urlsplit

__all__ = [
    "Fetch",
    "NetworkError",
    "Response",
    "Sent",
    "Transport",
    "backoff_ms",
    "fetch",
    "normalize_url",
    "seconds",
]

#: Upper bound of one header line or chunk-size line.
_LINE_LIMIT = 64 * 1024
#: Upper bound of the response head (status line + headers).
_HEAD_LIMIT = 256 * 1024
_READ_SIZE = 64 * 1024


class NetworkError(ConnectionError):
    """The request did not get a complete response: refused, reset, timed out, cut midway
    or malformed. The request may still have reached the server."""


def seconds(d: float | timedelta) -> float:
    """A duration given as seconds or as a :class:`~datetime.timedelta`, in seconds."""
    return d.total_seconds() if isinstance(d, timedelta) else float(d)


class Response:
    """An HTTP response. A buffered response holds its whole body in :attr:`body`; a
    streaming one (``stream=True`` and a ``2xx`` status) is read with
    :meth:`iter_chunks` and must be closed with :meth:`aclose`."""

    def __init__(
        self,
        status: int,
        headers: Mapping[str, str],
        body: bytes = b"",
        *,
        chunks: AsyncGenerator[bytes, None] | None = None,
        writer: asyncio.StreamWriter | None = None,
    ) -> None:
        self.status = status
        #: Header names are lower case; repeated headers are joined with ``", "``.
        self.headers: dict[str, str] = {k.lower(): v for k, v in headers.items()}
        self.body = body
        self._chunks = chunks
        self._writer = writer

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300

    @property
    def streaming(self) -> bool:
        return self._chunks is not None

    def header(self, name: str) -> str | None:
        return self.headers.get(name.lower())

    def json(self) -> Any:
        """The body parsed as JSON; raises :class:`ValueError` if it is not JSON."""
        return json.loads(self.body.decode("utf-8"))

    def json_object(self) -> dict[str, Any]:
        """The body as a JSON object, ``{}`` when it is not one."""
        try:
            v = self.json()
        except ValueError:
            return {}
        return v if isinstance(v, dict) else {}

    async def iter_chunks(self) -> AsyncIterator[bytes]:
        """The body as it arrives. Raises :class:`NetworkError` when the connection breaks
        before the body's end."""
        if self._chunks is None:
            if self.body:
                yield self.body
            return
        async for chunk in self._chunks:
            yield chunk

    async def aclose(self) -> None:
        chunks, self._chunks = self._chunks, None
        if chunks is not None:
            with contextlib.suppress(RuntimeError):
                await chunks.aclose()
        w, self._writer = self._writer, None
        if w is not None:
            await _close(w)


#: Sends one request: ``(method, url, headers, body, stream) -> Response``. Raises
#: :class:`NetworkError` when no complete response arrives.
Fetch = Callable[[str, str, Mapping[str, str], bytes | None, bool], Awaitable[Response]]


async def _close(writer: asyncio.StreamWriter) -> None:
    writer.close()
    with contextlib.suppress(Exception):
        await writer.wait_closed()


def _check_header(name: str, value: str) -> None:
    if not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name):
        raise ValueError(f"invalid header name: {name!r}")
    if "\r" in value or "\n" in value or "\0" in value:
        raise ValueError(f"invalid value for header {name!r}")


async def fetch(
    method: str,
    url: str,
    headers: Mapping[str, str] | None = None,
    body: bytes | None = None,
    stream: bool = False,
    *,
    timeout: float | None = None,
    connect_timeout: float | None = 10.0,
    ssl_context: ssl.SSLContext | None = None,
) -> Response:
    """Sends one request on a fresh connection.

    ``timeout`` caps the whole exchange (``stream=False``) or the wait for the response
    head (``stream=True``); ``None`` waits as long as it takes. ``stream=True`` keeps a
    ``2xx`` body open to read with :meth:`Response.iter_chunks`; any other status is read
    in full, so its JSON can be inspected.
    """
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError(f"not an http(s) URL: {url!r}")
    host = parts.hostname
    port = parts.port or (443 if parts.scheme == "https" else 80)
    target = parts.path or "/"
    if parts.query:
        target += "?" + parts.query
    out: dict[str, str] = {}
    for k, v in (headers or {}).items():
        _check_header(k, v)
        out[k.lower()] = v
    out["host"] = parts.netloc.rpartition("@")[2]
    out["connection"] = "close"
    out.pop("transfer-encoding", None)
    if body is not None or method.upper() in ("POST", "PUT", "PATCH"):
        out["content-length"] = str(len(body or b""))
    head = f"{method.upper()} {target} HTTP/1.1\r\n" + "".join(f"{k}: {v}\r\n" for k, v in out.items())
    request = head.encode("latin-1") + b"\r\n" + (body or b"")

    tls: ssl.SSLContext | None = None
    if parts.scheme == "https":
        tls = ssl_context or ssl.create_default_context()
    writer: asyncio.StreamWriter | None = None
    try:
        try:
            async with asyncio.timeout(connect_timeout):
                reader, writer = await asyncio.open_connection(
                    host, port, ssl=tls, server_hostname=host if tls else None, limit=_LINE_LIMIT
                )
        except TimeoutError as e:
            raise NetworkError(f"connect to {host}:{port} timed out") from e
        async with asyncio.timeout(timeout) as head_timeout:
            writer.write(request)
            await writer.drain()
            status, resp_headers = await _read_head(reader)
            if stream and 200 <= status < 300 and _has_body(method, status):
                head_timeout.reschedule(None)
                return Response(
                    status,
                    resp_headers,
                    chunks=_body_chunks(reader, resp_headers),
                    writer=writer,
                )
            data = b""
            if _has_body(method, status):
                parts_: list[bytes] = []
                async for c in _body_chunks(reader, resp_headers):
                    parts_.append(c)
                data = b"".join(parts_)
        await _close(writer)
        writer = None
        return Response(status, resp_headers, data)
    except NetworkError:
        if writer is not None:
            await _close(writer)
        raise
    except (OSError, EOFError, ValueError, asyncio.LimitOverrunError, TimeoutError) as e:
        if writer is not None:
            await _close(writer)
        raise NetworkError(f"{method} {url}: {type(e).__name__}: {e}") from e
    except BaseException:
        if writer is not None:
            writer.close()
        raise


def _has_body(method: str, status: int) -> bool:
    return method.upper() != "HEAD" and status not in (204, 304) and not 100 <= status < 200


async def _read_head(reader: asyncio.StreamReader) -> tuple[int, dict[str, str]]:
    while True:
        size = 0
        line = await reader.readuntil(b"\n")
        size += len(line)
        m = re.match(rb"HTTP/1\.[01] (\d{3})(?: |\r?\n)", line)
        if m is None:
            raise ValueError(f"malformed status line: {line[:80]!r}")
        status = int(m.group(1))
        headers: dict[str, str] = {}
        while True:
            line = await reader.readuntil(b"\n")
            size += len(line)
            if size > _HEAD_LIMIT:
                raise ValueError("response head too large")
            text = line.decode("latin-1").rstrip("\r\n")
            if text == "":
                break
            name, sep, value = text.partition(":")
            if not sep or not name or name != name.strip():
                raise ValueError(f"malformed header line: {text[:80]!r}")
            key = name.lower()
            value = value.strip(" \t")
            headers[key] = f"{headers[key]}, {value}" if key in headers else value
        # Interim responses (`100 Continue`, `103 Early Hints`) precede the real one.
        if 100 <= status < 200 and status != 101:
            continue
        return status, headers


async def _body_chunks(reader: asyncio.StreamReader, headers: Mapping[str, str]) -> AsyncGenerator[bytes, None]:
    """The body framed by ``headers``; a cut connection raises :class:`NetworkError`."""
    try:
        te = headers.get("transfer-encoding", "").lower()
        if "chunked" in te:
            while True:
                line = await reader.readuntil(b"\n")
                size_text = line.split(b";", 1)[0].strip()
                if not re.fullmatch(rb"[0-9A-Fa-f]+", size_text):
                    raise ValueError(f"malformed chunk size: {line[:40]!r}")
                n = int(size_text, 16)
                if n == 0:
                    # Trailers, up to the empty line.
                    while (await reader.readuntil(b"\n")).strip() != b"":
                        pass
                    return
                data = await reader.readexactly(n)
                if (await reader.readexactly(2)) != b"\r\n":
                    raise ValueError("chunk not followed by CRLF")
                yield data
        elif "content-length" in headers:
            text = headers["content-length"].split(",")[0].strip()
            if not text.isdigit():
                raise ValueError(f"malformed Content-Length: {text!r}")
            left = int(text)
            while left > 0:
                data = await reader.read(min(left, _READ_SIZE))
                if not data:
                    raise EOFError(f"connection closed with {left} bytes of the body missing")
                left -= len(data)
                yield data
        else:
            while data := await reader.read(_READ_SIZE):
                yield data
    except (OSError, EOFError, ValueError, asyncio.LimitOverrunError) as e:
        raise NetworkError(f"response body cut: {type(e).__name__}: {e}") from e


def normalize_url(node: str) -> str:
    """``host:port`` => ``http://host:port``; drops a trailing ``/``."""
    n = node.rstrip("/")
    return n if re.match(r"https?://", n) else f"http://{n}"


def _scheme(url: str) -> str:
    return url[: url.find(":")]


def backoff_ms(n: int) -> float:
    """Exponential backoff for attempt ``n`` (from 1): 100 ms, 200 ms, ... capped at 2 s."""
    return min(2000.0, 100.0 * 2 ** max(0, n - 1))


def retry_after_s(value: str | None, default: float) -> float:
    """``Retry-After`` in seconds (``default`` when absent or not a finite number)."""
    if value is None:
        return default
    text = value.strip()
    if text == "":
        return 0.0
    try:
        v = float(text)
    except ValueError:
        return default
    return v if math.isfinite(v) else default


@dataclass(frozen=True)
class Sent:
    """One send: the last response and the node that answered it."""

    res: Response
    node: str
    #: An earlier send of the **same** request failed with a network error and was resent;
    #: that one may have reached the server, so a ``409 already_invoked`` may be the
    #: caller's own.
    resent: bool


class Transport:
    """Sends requests under the error contract at the top of this module."""

    def __init__(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        follow_leader: bool = True,
        max_attempts: int = 8,
        max_elapsed: float | timedelta = 30.0,
        retry_network: bool = True,
        timeout: float | timedelta | None = None,
        connect_timeout: float | timedelta | None = 10.0,
        ssl_context: ssl.SSLContext | None = None,
        fetch: Fetch | None = None,
    ) -> None:
        """
        :param url: ingress port of any node (``http://host:port`` or ``host:port``).
        :param headers: added to every request, e.g. ``authorization: Bearer <key>``.
        :param follow_leader: follow the ``leader`` of a ``503``.
        :param max_attempts: cap on the attempts of one request.
        :param max_elapsed: time cap of one request, waits included.
        :param retry_network: a network error at the original node is retried (``True``)
            or raised at once.
        :param timeout: cap on one HTTP exchange (``None``: no cap).
        :param connect_timeout: cap on opening one connection.
        :param fetch: replacement for the HTTP client (tests).
        """
        self.url = normalize_url(url)
        self.headers: dict[str, str] = {k.lower(): v for k, v in (headers or {}).items()}
        self.follow_leader = follow_leader
        self.max_attempts = max(1, max_attempts)
        self.max_elapsed = seconds(max_elapsed)
        self.retry_network = retry_network
        t = None if timeout is None else seconds(timeout)
        ct = None if connect_timeout is None else seconds(connect_timeout)
        if fetch is None:

            async def default_fetch(
                method: str, url: str, headers: Mapping[str, str], body: bytes | None, stream: bool
            ) -> Response:
                return await _fetch(
                    method,
                    url,
                    headers,
                    body,
                    stream,
                    timeout=t,
                    connect_timeout=ct,
                    ssl_context=ssl_context,
                )

            self._fetch: Fetch = default_fetch
        else:
            self._fetch = fetch

    async def request(
        self,
        path: str,
        *,
        method: str = "GET",
        headers: Mapping[str, str] | None = None,
        body: bytes | None = None,
        node: str | None = None,
        stream: bool = False,
    ) -> Sent:
        """Sends ``path`` (with query). ``node``: start at this node instead of the original
        ``url`` (a stream reader keeps the leader between reconnects)."""
        started = time.monotonic()
        merged = {**self.headers, **{k.lower(): v for k, v in (headers or {}).items()}}
        at = normalize_url(node if node is not None else self.url)
        followed = False
        #: Followed leaders that did not answer: not followed again in this call.
        dead: set[str] = set()
        resent = False
        attempt = 0
        while True:
            attempt += 1
            last = attempt >= self.max_attempts or time.monotonic() - started >= self.max_elapsed
            try:
                res = await self._fetch(method, f"{at}{path}", merged, body, stream)
            except NetworkError:
                if at != self.url:
                    # A followed node did not answer: back off and return to the original
                    # node; it learns the new leader when the old lease expires.
                    dead.add(at)
                    at = self.url
                    followed = False
                    if last:
                        raise
                    await self._wait(backoff_ms(attempt), started)
                    continue
                if not self.retry_network or last:
                    raise
                resent = True
                await self._wait(backoff_ms(attempt), started)
                continue
            if res.status == 503:
                leader = res.json_object().get("leader")
                if (
                    self.follow_leader
                    and not followed
                    and isinstance(leader, str)
                    and leader != ""
                    and _scheme(normalize_url(leader)) == _scheme(self.url)
                    and normalize_url(leader) != at
                    and normalize_url(leader) not in dead
                    and not last
                ):
                    await res.aclose()
                    at = normalize_url(leader)
                    followed = True
                    continue
                if last:
                    return Sent(res, at, resent)
                await res.aclose()
                await self._wait(backoff_ms(attempt), started)
                at = self.url
                followed = False
                continue
            if res.status == 429 and "id" not in res.json_object() and not last:
                after = retry_after_s(res.header("retry-after"), 1.0)
                await res.aclose()
                await self._wait(max(0.0, after) * 1000, started)
                continue
            return Sent(res, at, resent)

    async def _wait(self, ms: float, started: float) -> None:
        """Waits ``ms``, no longer than what is left of the time cap."""
        left = self.max_elapsed - (time.monotonic() - started)
        await asyncio.sleep(max(0.0, min(ms / 1000, left)))


_fetch = fetch
