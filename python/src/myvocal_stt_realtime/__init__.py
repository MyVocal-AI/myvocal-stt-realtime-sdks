"""MyVocal Speech to Text Realtime SDK for Python (asyncio core + synchronous wrapper)."""
from ._client import AsyncRealtimeClient
from ._session import AsyncRealtimeSession, Cursors
from ._sync import RealtimeClient, RealtimeSession
from ._version import __version__
from .audio import ENCODINGS, AudioFormat, WavInfo, read_wav_header
from .config import ClientConfig, Timeouts
from .errors import (AckTimeoutError, ApiError, AudioFormatError, AuthenticationError, BackpressureTimeoutError,
                     ConnectionLostError, FinishUnconfirmedError, IdempotencyConflictError, InputInvalidError,
                     InsufficientCharactersError, MyVocalError, NotFoundError, ServerEventError, SessionStateError,
                     TransportError)
from .models import (EVENT_TYPES, TERMINAL_STATUSES, Event, FinishResult, Notice, SessionOptions, SessionRequest,
                     SessionView, Ticket, Transcript, TranscriptLine, Usage, Word)

__all__ = [
    "AsyncRealtimeClient", "AsyncRealtimeSession", "RealtimeClient", "RealtimeSession", "Cursors", "__version__",
    "ENCODINGS", "AudioFormat", "WavInfo", "read_wav_header", "ClientConfig", "Timeouts",
    "AckTimeoutError", "ApiError", "AudioFormatError", "AuthenticationError", "BackpressureTimeoutError",
    "ConnectionLostError", "FinishUnconfirmedError", "IdempotencyConflictError", "InputInvalidError",
    "InsufficientCharactersError", "MyVocalError", "NotFoundError", "ServerEventError", "SessionStateError",
    "TransportError", "EVENT_TYPES", "TERMINAL_STATUSES", "Event", "FinishResult", "Notice", "SessionOptions",
    "SessionRequest", "SessionView", "Ticket", "Transcript", "TranscriptLine", "Usage", "Word",
]
