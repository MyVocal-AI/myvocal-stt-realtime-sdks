"""Synchronous wrapper. It runs the same asyncio engine on a private loop thread, so both
entry points share one protocol implementation. Synchronous handlers run on a dedicated
callback thread, never on the socket reader."""
import asyncio
import concurrent.futures
import threading
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional

from ._client import AsyncRealtimeClient
from ._session import AsyncRealtimeSession, Cursors
from .errors import SessionStateError
from .models import Event, FinishResult, SessionRequest, SessionView, Ticket, Transcript


class _LoopThread:
    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, name="myvocal-stt-loop", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def run(self, coro, timeout: Optional[float] = None) -> Any:
        if threading.current_thread() is self._thread:
            raise RuntimeError("synchronous SDK call from an SDK callback thread would deadlock")
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    def stop(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(timeout=5)
        if not self._thread.is_alive():
            self.loop.close()


class RealtimeClient:
    """Blocking MyVocal STT Realtime client. Same options as :class:`AsyncRealtimeClient`
    except ``http_client`` (an ``httpx.AsyncClient`` would be bound to another loop)."""

    def __init__(self, access_key: str, **kwargs: Any):
        if "http_client" in kwargs:
            raise TypeError("http_client is only supported by AsyncRealtimeClient")
        self._loop = _LoopThread()

        async def build():
            return AsyncRealtimeClient(access_key, **kwargs)
        try:
            self._async = self._loop.run(build())
        except BaseException:
            self._loop.stop()
            raise
        self.config = self._async.config
        self._closed = False

    def __repr__(self) -> str:
        return "RealtimeClient(base_url=%r)" % self.config.base_url

    def create_session(self, request: Optional[SessionRequest] = None, *,
                       idempotency_key: Optional[str] = None) -> SessionView:
        return self._loop.run(self._async.create_session(request, idempotency_key=idempotency_key))

    def get_session(self, session_id: str) -> SessionView:
        return self._loop.run(self._async.get_session(session_id))

    def pause_session(self, session_id: str, captured_samples: Optional[int] = None) -> SessionView:
        return self._loop.run(self._async.pause_session(session_id, captured_samples))

    def resume_session(self, session_id: str, captured_samples: Optional[int] = None) -> SessionView:
        return self._loop.run(self._async.resume_session(session_id, captured_samples))

    def finish_session(self, session_id: str, captured_samples: Optional[int] = None) -> SessionView:
        return self._loop.run(self._async.finish_session(session_id, captured_samples))

    def create_ticket(self, session_id: str) -> Ticket:
        return self._loop.run(self._async.create_ticket(session_id))

    def connect(self, request: Optional[SessionRequest] = None, *, session_id: Optional[str] = None,
                idempotency_key: Optional[str] = None, handlers: Optional[Mapping[str, Callable]] = None,
                on_event: Optional[Callable] = None, on_callback_error: Optional[Callable] = None,
                iterate_events: bool = False) -> "RealtimeSession":
        # iterate_events uses the engine's bounded iterator queue (event_queue_size): the socket
        # reader never waits for the iterating thread; an overflow is counted in dropped_events and
        # reported to on_callback_error, and the transcript/result snapshot stays complete.
        coro = self._async.connect(request, session_id=session_id, idempotency_key=idempotency_key,
                                   handlers=handlers, on_event=on_event, on_callback_error=on_callback_error,
                                   iterate_events=iterate_events)
        return RealtimeSession(self._loop, self._loop.run(coro))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._loop.run(self._async.aclose(), timeout=self.config.timeouts.close + 10)
        finally:
            self._loop.stop()

    def __enter__(self) -> "RealtimeClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class RealtimeSession:
    """Blocking handle of one session (wraps :class:`AsyncRealtimeSession`)."""

    def __init__(self, loop: _LoopThread, inner: AsyncRealtimeSession):
        self._loop = loop
        self._inner = inner
        self.session_id = inner.session_id
        self.transcription_id = inner.transcription_id
        self.format = inner.format

    def __repr__(self) -> str:
        return "RealtimeSession(session_id=%r, state=%s)" % (self.session_id, self._inner.state)

    @property
    def state(self) -> str:
        return self._inner.state

    @property
    def view(self) -> SessionView:
        return self._inner.view

    @property
    def cursors(self) -> Cursors:
        return self._inner.cursors

    @property
    def transcript(self) -> Transcript:
        return self._inner.transcript

    @property
    def latest_usage(self):
        return self._inner.latest_usage

    @property
    def server_errors(self):
        return list(self._inner.server_errors)

    def stats(self):
        return self._inner.stats()

    def send_audio(self, data: bytes, *, preceding_text: Optional[str] = None) -> None:
        self._loop.run(self._inner.send_audio(data, preceding_text=preceding_text))

    def send_file(self, path: str, *, paced: bool = True, wav: Optional[bool] = None) -> None:
        self._loop.run(self._inner.send_file(path, paced=paced, wav=wav))

    def send_chunks(self, chunks: Iterable[bytes], *, paced: bool = False) -> None:
        for chunk in self._paced(chunks) if paced else chunks:
            self.send_audio(chunk)

    def _paced(self, chunks: Iterable[bytes]) -> Iterator[bytes]:
        import time
        t0 = time.monotonic()
        pos = 0
        bps, rate = self.format.bytes_per_sample, self.format.sample_rate_hz
        for chunk in chunks:
            pos += len(chunk)
            due = t0 + (pos / bps) / rate - time.monotonic()
            if due > 0:
                time.sleep(due)
            yield chunk

    def commit(self) -> None:
        self._loop.run(self._inner.commit())

    def pause(self, *, via: str = "ws") -> None:
        self._loop.run(self._inner.pause(via=via))

    def resume(self, *, via: str = "ws") -> None:
        self._loop.run(self._inner.resume(via=via))

    def finish(self) -> FinishResult:
        return self._loop.run(self._inner.finish())

    def discard_partial_sample(self) -> bool:
        return self._inner.discard_partial_sample()

    def abort_connection(self) -> None:
        self._loop.run(self._inner.abort_connection())

    def events(self, timeout: Optional[float] = None) -> Iterator[Event]:
        """Blocking iterator over events (``connect(..., iterate_events=True)``). Ends after
        ``session.completed``, after close() once the queue is drained, or when ``timeout``
        passes without an event. The queue holds at most ``event_queue_size`` events; events
        that do not fit are counted in ``dropped_events`` and reported to ``on_callback_error``."""
        if self._inner._iter_queue is None:
            raise SessionStateError("connect(..., iterate_events=True) to iterate events")
        while True:
            try:
                ev = self._loop.run(self._inner._next_event(timeout))
            except (asyncio.TimeoutError, concurrent.futures.TimeoutError):
                return
            if ev is None:
                return
            yield ev
            if ev.type == "session.completed":
                return

    def close(self) -> None:
        self._loop.run(self._inner.close())

    def __enter__(self) -> "RealtimeSession":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
