"""Typed MyVocal STT Realtime errors.

No error message, ``repr`` or attribute ever contains the access key, a ticket, an
authentication header or a signed URL.
"""
from typing import Any, Optional


class MyVocalError(Exception):
    """Base class of every error raised by this SDK."""


class ApiError(MyVocalError):
    """A MyVocal envelope with ``code != 1`` (HTTP 200 included)."""

    def __init__(self, code: int, error_code: Optional[str], message: str = "", *, retryable: bool = False,
                 action: Optional[str] = None, request_id: Optional[str] = None, details: Any = None):
        self.code = code
        self.error_code = error_code
        self.message = message
        self.retryable = bool(retryable)
        self.action = action
        self.request_id = request_id
        self.details = details
        super().__init__("MyVocal API error code=%s errorCode=%s retryable=%s requestId=%s"
                         % (code, error_code, self.retryable, request_id))


class AuthenticationError(ApiError):
    """Missing, unknown or disabled access key (envelope ``code = 401``)."""


class InputInvalidError(ApiError):
    """``INPUT_INVALID``: malformed or unsupported field or combination. Never retried."""


class IdempotencyConflictError(ApiError):
    """The Idempotency-Key was already bound to a different create request."""


class InsufficientCharactersError(ApiError):
    """``INSUFFICIENT_CHARACTERS``: the Characters balance is exhausted."""


class NotFoundError(ApiError):
    """``PROJECT_NOT_FOUND``: unknown session or not owned by this key."""


class TransportError(MyVocalError):
    """The request or socket failed below the MyVocal envelope (network, TLS, HTTP status)."""

    def __init__(self, message: str, *, status: Optional[int] = None):
        self.status = status
        super().__init__(message)


class AudioFormatError(MyVocalError):
    """Audio does not fit the session encoding (half sample at the end, WAV mismatch, encoding mismatch)."""


class SessionStateError(MyVocalError):
    """The operation is not allowed in the session's current local state (paused, finished, closed)."""


class ConnectionLostError(SessionStateError):
    """The socket ended before the session finished. No audio is accepted any more; the
    session is closed out over REST (see :class:`FinishResult.closeout_reason`)."""

    def __init__(self, message: str, session_id: Optional[str]):
        self.session_id = session_id
        super().__init__("%s (sessionId=%s)" % (message, session_id))


class BackpressureTimeoutError(MyVocalError):
    """The bounded send buffer stayed full (no acknowledgement progress) past the configured time."""


class AckTimeoutError(MyVocalError):
    """Unacknowledged audio made no acknowledgement progress within ``ack_timeout``."""


class ServerEventError(MyVocalError):
    """The socket reported ``session.error`` with a MyVocal code that stops the stream."""

    def __init__(self, error_code: str, session_id: Optional[str] = None):
        self.error_code = error_code
        self.session_id = session_id
        super().__init__("session.error %s (sessionId=%s)" % (error_code, session_id))


class FinishUnconfirmedError(MyVocalError):
    """The end of the session could not be confirmed (REST unreachable or no terminal state
    in time). Nothing is assumed settled. Call ``get_session``/``finish_session`` with
    :attr:`session_id` later."""

    def __init__(self, message: str, session_id: Optional[str], *, cause: Optional[BaseException] = None):
        self.session_id = session_id
        self.cause = cause
        super().__init__("%s (sessionId=%s)" % (message, session_id))


def api_error_from_envelope(body: dict) -> ApiError:
    code = body.get("code")
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    error_code = data.get("errorCode")
    kwargs = dict(retryable=bool(data.get("retryable", False)), action=data.get("action"),
                  request_id=data.get("requestId"), details=data.get("details"))
    message = body.get("message") or ""
    if code == 401:
        return AuthenticationError(401, error_code or "UNAUTHORIZED", message, **kwargs)
    cls = {
        "INPUT_INVALID": InputInvalidError,
        "IDEMPOTENCY_CONFLICT": IdempotencyConflictError,
        "INSUFFICIENT_CHARACTERS": InsufficientCharactersError,
        "PROJECT_NOT_FOUND": NotFoundError,
    }.get(error_code, ApiError)
    return cls(int(code) if isinstance(code, int) else -1, error_code, message, **kwargs)
