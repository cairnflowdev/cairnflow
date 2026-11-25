# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""Errors user code raises or catches. Shared by the handler side and the client."""

from __future__ import annotations


def wire_code(code: object) -> int:
    """An error code that can travel on the wire (``uint32``); anything else becomes 500."""
    if isinstance(code, int) and not isinstance(code, bool) and 0 <= code < 2**32:
        return code
    return 500


class TerminalError(Exception):
    """Business error: ends the invocation, **no** retry (retrying cannot change it)."""

    def __init__(self, message: str, code: int = 500) -> None:
        super().__init__(message)
        self.message = message
        self.code = wire_code(code)


class PauseError(Exception):
    """Pauses the invocation, **waiting for a human** (protocol-v1 section 8.5).

    Unlike an ordinary exception (retried forever with backoff) and unlike
    :class:`TerminalError` (fails for good, throwing the work away), the journal is kept as
    is; the operator fixes the cause and calls ``POST /invocations/{id}/resume``.
    """

    def __init__(self, message: str, code: int = 500) -> None:
        super().__init__(message)
        self.message = message
        self.code = wire_code(code)


class RunFailure(Exception):
    """Raised inside a ``ctx.run`` closure: a durable failure of that step (not retried)."""

    def __init__(self, message: str, code: int = 500) -> None:
        super().__init__(message)
        self.message = message
        self.code = wire_code(code)


class JournalMismatch(Exception):
    """The journal does not match the current code (code 570). Never retried."""


class TurnFailed(Exception):
    """A ``ctx`` command after a ``ctx.run`` closure failed transiently: the turn is
    already dead, its outcome is a retry whatever the code does next."""

    def __init__(self, cause: BaseException | None) -> None:
        super().__init__(f"turn failed on a retryable ctx.run error: {cause}")
        self.__cause__ = cause


class Suspend(BaseException):
    """A ``ctx`` command called after the turn has suspended.

    A ``BaseException`` (like ``asyncio.CancelledError``) so that a broad
    ``except Exception`` in user code does not swallow it. The turn's outcome is decided by
    state anyway, not by how this propagates.
    """


class SegmentedValueError(Exception):
    """A segmented value could not be assembled (missing segment, wrong sha). Retried."""
