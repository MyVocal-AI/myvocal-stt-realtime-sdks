"""Client configuration. Durations are seconds (float)."""
from dataclasses import dataclass
from typing import Optional


@dataclass
class Timeouts:
    #: one REST request (GET/pause/resume/tickets)
    request: float = 30.0
    #: the whole create call including its idempotent retries
    create: float = 30.0
    #: socket open + ``session.ready``
    connect: float = 15.0
    #: unacknowledged audio without acknowledgement progress (idle time without
    #: unacknowledged audio never counts)
    ack: float = 10.0
    #: after ``session.finish`` was sent, waiting for ``session.completed``
    finish: float = 60.0
    #: REST close-out after a disconnect / timeout (query + idempotent finish, with retries)
    closeout: float = 70.0
    #: local resource release in ``close()`` (socket close handshake, event drain)
    close: float = 5.0
    #: how long ``send_audio`` may wait for buffer room; ``None`` uses ``ack``
    backpressure: Optional[float] = None


@dataclass
class ClientConfig:
    base_url: str = "https://api.myvocal.ai"
    timeouts: Timeouts = None  # type: ignore[assignment]
    #: client WebSocket ping interval (seconds). Unrelated to ``heartbeatIntervalMs``.
    ping_interval: Optional[float] = 20.0
    ping_timeout: Optional[float] = 20.0
    #: approximate audio frame duration
    frame_ms: int = 100
    #: bound of audio held by the SDK (not yet confirmed by the server), in milliseconds
    max_buffered_ms: int = 30_000
    #: bound of events waiting for user handlers, and of the event iterator queue (sync and async)
    event_queue_size: int = 10_000
    #: idempotent create retries after a transport failure or a retryable envelope
    max_create_retries: int = 2
    #: on an unexpected disconnect, query the original session and finish it over REST
    closeout_on_disconnect: bool = True
    #: consecutive retryable ``session.error`` replays before the stream stops
    max_frame_retries: int = 3

    def __post_init__(self) -> None:
        if self.timeouts is None:
            self.timeouts = Timeouts()
        self.base_url = self.base_url.rstrip("/")
        if not (self.base_url.startswith("http://") or self.base_url.startswith("https://")):
            raise ValueError("base_url must be http:// or https://")
        if self.frame_ms <= 0 or self.max_buffered_ms < self.frame_ms:
            raise ValueError("frame_ms must be > 0 and max_buffered_ms >= frame_ms")
