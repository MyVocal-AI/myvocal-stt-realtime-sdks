"""AsyncRealtimeClient: REST helpers plus per-session connections."""
import logging
import ssl as _ssl
from typing import Any, Callable, Dict, Mapping, Optional, Union
from urllib.parse import urlsplit, urlunsplit

import httpx

from ._rest import USER_AGENT, RestTransport
from ._session import AsyncRealtimeSession
from .audio import AudioFormat
from .config import ClientConfig, Timeouts
from .errors import AudioFormatError, FinishUnconfirmedError, MyVocalError, TransportError
from .models import SessionRequest, SessionView, Ticket


class _WarningsOnly(logging.Filter):
    """websockets logs handshake headers (the accessKey) and frame text (audio, transcripts)
    at DEBUG. The SDK never forwards those records, whatever the application's log level."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno >= logging.WARNING


_TRANSPORT_LOG = logging.getLogger("myvocal_stt_realtime.transport")
_TRANSPORT_LOG.addFilter(_WarningsOnly())


class AsyncRealtimeClient:
    """MyVocal STT Realtime client (asyncio).

    ``proxy``: explicit proxy URL; otherwise ``trust_env`` decides whether HTTP(S)_PROXY /
    NO_PROXY from the environment apply. ``ssl_context``: custom trust (TLS verification is
    always on by default). ``http_client``: a borrowed ``httpx.AsyncClient`` that this client
    never closes.
    """

    def __init__(self, access_key: str, *, base_url: str = "https://api.myvocal.ai",
                 timeouts: Optional[Timeouts] = None, config: Optional[ClientConfig] = None,
                 proxy: Optional[str] = None, trust_env: bool = True,
                 ssl_context: Optional[_ssl.SSLContext] = None,
                 http_client: Optional[httpx.AsyncClient] = None, **config_kwargs: Any):
        if config is None:
            config = ClientConfig(base_url=base_url, timeouts=timeouts or Timeouts(), **config_kwargs)
        self.config = config
        self._proxy = proxy
        self._trust_env = trust_env
        self._ssl = ssl_context
        self._rest = RestTransport(access_key, config, http_client=http_client, proxy=proxy, trust_env=trust_env,
                                   verify=ssl_context if ssl_context is not None else True)
        self._key = access_key
        self._sessions = set()

    def __repr__(self) -> str:
        return "AsyncRealtimeClient(base_url=%r)" % self.config.base_url

    # ------------------------------------------------------------------ REST helpers
    async def create_session(self, request: Optional[SessionRequest] = None, *,
                             idempotency_key: Optional[str] = None) -> SessionView:
        """``POST /sessions``. A lost response is retried with the same Idempotency-Key and body."""
        return await self._rest.create(request or SessionRequest(), idempotency_key)

    async def get_session(self, session_id: str) -> SessionView:
        return await self._rest.get(session_id)

    async def pause_session(self, session_id: str, captured_samples: Optional[int] = None) -> SessionView:
        return await self._rest.pause(session_id, captured_samples)

    async def resume_session(self, session_id: str, captured_samples: Optional[int] = None) -> SessionView:
        return await self._rest.resume(session_id, captured_samples)

    async def finish_session(self, session_id: str, captured_samples: Optional[int] = None) -> SessionView:
        """Idempotent REST finish; a repeated finish returns the same task without a second charge."""
        return await self._rest.finish(session_id, captured_samples)

    async def create_ticket(self, session_id: str) -> Ticket:
        """Issues a single-use browser ticket (for your browser front end). This SDK never
        connects with a ticket."""
        return await self._rest.ticket(session_id)

    # ------------------------------------------------------------------ sessions
    async def connect(self, request: Optional[SessionRequest] = None, *, session_id: Optional[str] = None,
                      idempotency_key: Optional[str] = None,
                      handlers: Optional[Mapping[str, Callable]] = None, on_event: Optional[Callable] = None,
                      on_callback_error: Optional[Callable] = None,
                      iterate_events: bool = False) -> AsyncRealtimeSession:
        """Creates a session (or uses ``session_id``), opens its socket with the accessKey header
        and waits for ``session.ready``. Handlers are registered before the socket opens."""
        created_here = session_id is None
        view = await (self._rest.get(session_id) if session_id else
                      self._rest.create(request or SessionRequest(), idempotency_key))
        try:
            fmt = self._resolve_format(request, view)
        except AudioFormatError:
            if created_here:
                await self._quiet_finish(view.session_id)
            raise
        session = AsyncRealtimeSession(self, view, fmt, handlers=handlers, on_event=on_event,
                                       on_callback_error=on_callback_error, iterate_events=iterate_events,
                                       created_here=created_here)
        try:
            await session._open(self._stream_url(view.stream_url, view.session_id), self._ws_headers(),
                                self._ws_kwargs())
        except MyVocalError as exc:
            await session.close()
            if created_here:
                await self._quiet_finish(view.session_id)
            if isinstance(exc, TransportError):
                exc.session_id = view.session_id  # type: ignore[attr-defined]
            raise
        self._sessions.add(session)
        return session

    def _forget(self, session: AsyncRealtimeSession) -> None:
        self._sessions.discard(session)

    async def aclose(self) -> None:
        for s in list(self._sessions):
            await s.close()
        await self._rest.aclose()

    async def __aenter__(self) -> "AsyncRealtimeClient":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.aclose()

    # ------------------------------------------------------------------ internals
    @staticmethod
    def _resolve_format(request: Optional[SessionRequest], view: SessionView) -> AudioFormat:
        effective = view.effective_options.get("inputEncoding")
        requested = request.options.input_encoding if (request and request.options) else None
        if requested and effective and requested.strip().lower() != str(effective).lower():
            raise AudioFormatError("requested inputEncoding %s but the session uses %s" % (requested, effective))
        fmt = AudioFormat.of(effective or requested or "pcm_s16le_16000")
        rate = view.effective_options.get("sampleRateHz")
        if rate is not None and int(rate) != fmt.sample_rate_hz:
            raise AudioFormatError("session sampleRateHz %s does not match %s" % (rate, fmt.encoding))
        return fmt

    async def _quiet_finish(self, session_id: Optional[str]) -> None:
        if not session_id:
            return
        try:
            await self._rest.finish(session_id, 0, timeout=self.config.timeouts.request)
        except MyVocalError:
            pass

    def _stream_url(self, stream_url: Optional[str], session_id: Optional[str]) -> str:
        path = stream_url or "/sound_clone/api/v1/stt/realtime/sessions/%s/stream" % session_id
        if path.startswith("ws://") or path.startswith("wss://"):
            return path
        if path.startswith("http://") or path.startswith("https://"):
            full = path
        else:
            full = self.config.base_url + (path if path.startswith("/") else "/" + path)
        parts = urlsplit(full)
        scheme = "wss" if parts.scheme == "https" else "ws"
        return urlunsplit((scheme, parts.netloc, parts.path, parts.query, ""))

    def _ws_headers(self) -> Dict[str, str]:
        return {"accessKey": self._key}

    def _ws_kwargs(self) -> Dict[str, Any]:
        cfg = self.config
        kwargs: Dict[str, Any] = dict(user_agent_header=USER_AGENT, open_timeout=cfg.timeouts.connect,
                                      ping_interval=cfg.ping_interval, ping_timeout=cfg.ping_timeout,
                                      close_timeout=cfg.timeouts.close, max_size=4 * 1024 * 1024,
                                      logger=_TRANSPORT_LOG)
        if self.config.base_url.startswith("https://") and self._ssl is not None:
            kwargs["ssl"] = self._ssl
        if self._proxy:
            kwargs["proxy"] = self._proxy
        elif self._trust_env:
            kwargs["proxy"] = True
        else:
            kwargs["proxy"] = None
        return kwargs
