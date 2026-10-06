"""Per-session realtime engine (asyncio).

One state machine per session: frames ~100 ms from arbitrary input chunks, keeps the
session-wide capture count and the per-epoch cursors, follows server rotation (resending
only unconfirmed bytes), serializes lifecycle operations, isolates user callbacks from the
socket reader, and closes out over REST after an unexpected disconnect (no reconnect, no
audio replay into a new task).
"""
import asyncio
import base64
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, AsyncIterator, Callable, Dict, List, Mapping, Optional

from .audio import AudioFormat, read_wav_header
from .errors import (AckTimeoutError, ApiError, AudioFormatError, BackpressureTimeoutError, ConnectionLostError,
                     FinishUnconfirmedError, InsufficientCharactersError, MyVocalError, ServerEventError,
                     SessionStateError, TransportError)
from .models import Event, FinishResult, SessionView, Transcript, Usage, parse_int

log = logging.getLogger("myvocal_stt_realtime")

#: session.error codes that stop the audio stream (finish is still attempted)
FATAL_STREAM_CODES = ("INPUT_INVALID", "PROJECT_NOT_FOUND", "INSUFFICIENT_CHARACTERS")
#: session.error codes after which unconfirmed frames are replayed at the same sampleOffset
RETRYABLE_STREAM_CODES = ("TEMPORARILY_UNAVAILABLE", "PROVIDER_UNAVAILABLE")

S_OPEN, S_PAUSED, S_FINISHING, S_COMPLETED, S_LOST, S_CLOSED = (
    "OPEN", "PAUSED", "FINISHING", "COMPLETED", "LOST", "CLOSED")


class Cursors:
    """Snapshot of the counters. ``sample_offset``: samples sent in the current epoch;
    ``sent_samples``: samples the server acknowledged in the current epoch;
    ``received_samples``: the server's session-wide transport receipt (not a model proof);
    ``captured_samples``: whole samples handed to this SDK so far."""

    def __init__(self, epoch, sample_offset, sent_samples, received_samples, captured_samples, confirmed_samples):
        self.epoch = epoch
        self.sample_offset = sample_offset
        self.sent_samples = sent_samples
        self.received_samples = received_samples
        self.captured_samples = captured_samples
        self.confirmed_samples = confirmed_samples

    def __repr__(self):
        return ("Cursors(epoch=%s, sample_offset=%s, sent_samples=%s, received_samples=%s, captured_samples=%s)"
                % (self.epoch, self.sample_offset, self.sent_samples, self.received_samples, self.captured_samples))


class AsyncRealtimeSession:
    """Handle of one realtime session. Create it with :meth:`AsyncRealtimeClient.connect`."""

    def __init__(self, client, view: SessionView, fmt: AudioFormat, *, handlers: Optional[Mapping[str, Callable]] = None,
                 on_event: Optional[Callable] = None, on_callback_error: Optional[Callable] = None,
                 iterate_events: bool = False, created_here: bool = True):
        cfg = client.config
        self._client = client
        self._rest = client._rest
        self._cfg = cfg
        self.session_id: str = view.session_id or ""
        self.transcription_id: Optional[str] = view.transcription_id
        self.view: SessionView = view
        self.format = fmt
        self._bps = fmt.bytes_per_sample
        self._frame_samples = max(1, fmt.sample_rate_hz * cfg.frame_ms // 1000)
        self._max_buffered = max(self._frame_samples, fmt.sample_rate_hz * cfg.max_buffered_ms // 1000)
        self._created_here = created_here
        # cursors
        self._buf = bytearray()
        self._buf_base = 0
        self._half = b""
        self._captured = 0
        self._epoch = view.epoch or 1
        self._epoch_start = 0
        self._sent = 0
        self._acked = 0
        self._server_received: Optional[int] = None
        self._rotating = False
        self._rotations = 0
        self._server_accepting = True
        self._user_paused = False
        self._state = S_OPEN
        self._fatal: Optional[BaseException] = None
        self._flush = False
        self._finish_sent = False
        self._last_progress = time.monotonic()
        self._preceding: Optional[str] = None
        self._frames_sent = 0
        self._peak_buffered = 0
        self._frame_retries = 0
        # sync
        self._changed = asyncio.Event()
        self._op_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._ws = None
        self._reader_task: Optional[asyncio.Task] = None
        self._tasks: List[asyncio.Task] = []
        self._ready = asyncio.Event()
        self._completed_view: Optional[SessionView] = None
        self._result: Optional[FinishResult] = None
        self._closeout_task: Optional[asyncio.Task] = None
        self._closing_out = False
        self._closed = False
        self._intentional_close = False
        self.latest_usage = view.usage
        self.transcript = Transcript()
        self.server_errors: List[str] = []
        self._connection = 0
        self._last_sequence: Optional[int] = None
        self.sequence_anomalies = 0
        # dispatch
        self._handlers: Dict[str, List[Callable]] = {}
        for etype, fn in (handlers or {}).items():
            self._handlers.setdefault(etype, []).append(fn)
        self._on_event = on_event
        self._on_callback_error = on_callback_error
        self._queue: "asyncio.Queue[Optional[Event]]" = asyncio.Queue(maxsize=cfg.event_queue_size)
        self._iter_queue: Optional["asyncio.Queue[Optional[Event]]"] = (
            asyncio.Queue(maxsize=cfg.event_queue_size) if iterate_events else None)
        self._iter_end = False
        self._executor: Optional[ThreadPoolExecutor] = None
        self._dispatcher: Optional[asyncio.Task] = None
        self.callback_errors = 0
        self.dropped_events = 0

    def __repr__(self) -> str:
        return "AsyncRealtimeSession(session_id=%r, state=%s, epoch=%s)" % (self.session_id, self._state, self._epoch)

    # ------------------------------------------------------------------ public read-only state
    @property
    def state(self) -> str:
        return self._state

    @property
    def cursors(self) -> Cursors:
        return Cursors(self._epoch, self._sent, self._acked, self._server_received, self._captured,
                       self._epoch_start + self._acked)

    def stats(self) -> Dict[str, Any]:
        return {"captured_samples": self._captured, "confirmed_samples": self._epoch_start + self._acked,
                "buffered_samples": self._captured - (self._epoch_start + self._acked),
                "peak_buffered_samples": self._peak_buffered, "max_buffered_samples": self._max_buffered,
                "frame_samples": self._frame_samples, "frames_sent": self._frames_sent, "rotations": self._rotations,
                "epoch": self._epoch, "connections": self._connection, "callback_errors": self.callback_errors,
                "dropped_events": self.dropped_events, "sequence_anomalies": self.sequence_anomalies}

    # ------------------------------------------------------------------ connection
    async def _open(self, url: str, headers: Dict[str, str], connect_kwargs: Dict[str, Any]) -> None:
        from websockets.asyncio.client import connect as ws_connect
        self._dispatcher = asyncio.ensure_future(self._dispatch_loop())
        timeout = self._cfg.timeouts.connect
        try:
            self._ws = await asyncio.wait_for(ws_connect(url, additional_headers=headers, **connect_kwargs), timeout)
        except asyncio.TimeoutError as exc:
            raise TransportError("socket open timed out") from exc
        except Exception as exc:  # handshake refused, TLS, DNS, proxy
            raise TransportError("socket open failed (%s)" % type(exc).__name__) from exc
        self._connection += 1
        self._last_sequence = None
        self._reader_task = asyncio.ensure_future(self._reader(self._ws))
        try:
            await asyncio.wait_for(self._ready.wait(), timeout)
        except asyncio.TimeoutError as exc:
            await self._abort_ws()
            raise TransportError("session.ready not received in time") from exc
        if self._state == S_LOST:
            raise ConnectionLostError("socket closed before session.ready", self.session_id)

    async def _reader(self, ws) -> None:
        from websockets.exceptions import ConnectionClosed
        try:
            async for raw in ws:
                if isinstance(raw, bytes):
                    continue
                try:
                    msg = json.loads(raw)
                except ValueError:
                    continue
                if isinstance(msg, dict):
                    self._on_message(msg)
        except ConnectionClosed:
            pass
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - defensive
            log.debug("socket reader stopped: %s", type(exc).__name__)
        finally:
            if self._ws is ws:
                self._on_socket_end()

    def _touch(self) -> None:
        self._changed.set()

    async def _wait_change(self, timeout: float) -> None:
        self._changed.clear()
        try:
            await asyncio.wait_for(self._changed.wait(), max(0.001, timeout))
        except asyncio.TimeoutError:
            pass

    # ------------------------------------------------------------------ inbound events
    def _on_message(self, msg: Dict[str, Any]) -> None:
        etype = msg.get("eventType") or ""
        payload = msg.get("payload") if isinstance(msg.get("payload"), dict) else {}
        seq = msg.get("sequence") if isinstance(msg.get("sequence"), int) else None
        if seq is not None:
            if self._last_sequence is not None and seq <= self._last_sequence:
                self.sequence_anomalies += 1  # recorded, never used to drop another connection's events
            self._last_sequence = seq
        # 1) network layer updates state first
        try:
            if etype == "usage.updated":
                self._on_usage(payload)
            elif etype == "session.ready":
                self._on_ready(payload)
            elif etype == "session.completed":
                view = SessionView.from_wire(payload)
                self._completed_view = view
                self.view = view
                self.latest_usage = view.usage
            elif etype == "session.error":
                self._on_server_error(str(payload.get("errorCode") or "UNKNOWN"))
            self.transcript.apply(etype, payload)
        except (ValueError, TypeError) as exc:
            log.debug("malformed %s payload: %s", etype, type(exc).__name__)
        self._touch()
        # 2) then dispatch
        self._enqueue(Event(etype, payload, seq, msg.get("eventId"), msg.get("sessionId"), self._connection))

    def _on_ready(self, p: Dict[str, Any]) -> None:
        view = SessionView.from_wire(p)
        self.view = view
        self._epoch = view.epoch or self._epoch
        received = view.usage.received_samples or 0
        sent = view.usage.sent_samples or 0
        self._server_received = view.usage.received_samples
        if self._captured == 0 and not self._buf:
            self._epoch_start = max(0, received - sent)
            self._sent = self._acked = sent
            self._captured = self._buf_base = received
        self._server_accepting = bool(view.accepting)
        if view.is_terminal:
            self._completed_view = view
            self._state = S_COMPLETED
        elif not view.accepting and view.status == "PAUSED":
            self._user_paused = True
            self._state = S_PAUSED
        self._ready.set()

    def _on_usage(self, p: Dict[str, Any]) -> None:
        usage = Usage.from_wire(p)
        ep = usage.epoch
        if usage.received_samples is not None and (self._server_received is None or ep is None or ep >= self._epoch):
            self._server_received = usage.received_samples
        if not isinstance(ep, int):
            return
        if ep >= self._epoch:
            self.latest_usage = usage
        if ep == self._epoch:
            if usage.sent_samples is not None and usage.sent_samples > self._acked:
                self._acked = usage.sent_samples
                if self._acked > self._sent:
                    self._sent = self._acked
                self._progress()
                self._frame_retries = 0
            if usage.accepting is not None:
                self._server_accepting = bool(usage.accepting)
            if usage.rotate and not self._rotating:
                self._rotating = True
                self._rotations += 1
                self._spawn(self._rotate())
            if usage.error_code:
                self._set_fatal_code(usage.error_code)
        elif ep > self._epoch and usage.accepting:
            self._adopt_epoch(ep)
        # ep < current epoch: a late answer for an older epoch never moves current cursors back

    def _adopt_epoch(self, epoch: int) -> None:
        self._epoch_start += self._acked
        self._epoch = epoch
        self._sent = self._acked = 0
        self._rotating = False
        self._server_accepting = True
        self._progress()
        self._schedule_pump()

    def _progress(self) -> None:
        self._last_progress = time.monotonic()
        confirmed = self._epoch_start + self._acked
        drop = confirmed - self._buf_base
        if drop > 0:
            del self._buf[: drop * self._bps]
            self._buf_base = confirmed

    def _on_server_error(self, code: str) -> None:
        self.server_errors.append(code)
        if code in FATAL_STREAM_CODES:
            self._set_fatal_code(code)
        elif code in RETRYABLE_STREAM_CODES and self._sent > self._acked and not self._finish_sent:
            self._frame_retries += 1
            if self._frame_retries > self._cfg.max_frame_retries:
                self._fatal = ServerEventError(code, self.session_id)
            else:
                self._sent = self._acked  # replay unconfirmed frames at the confirmed sampleOffset
                self._schedule_pump()

    def _set_fatal_code(self, code: str) -> None:
        if self._fatal is not None:
            return
        if code == "INSUFFICIENT_CHARACTERS":
            self._fatal = InsufficientCharactersError(47210, code, "Characters balance exhausted during the stream")
        else:
            self._fatal = ServerEventError(code, self.session_id)

    def _on_socket_end(self) -> None:
        self._touch()
        if self._completed_view is not None or self._intentional_close or self._closing_out:
            return
        if not self._ready.is_set():
            self._state = S_LOST
            self._ready.set()
            return
        self._state = S_LOST
        log.debug("socket ended before the session finished (sessionId=%s)", self.session_id)
        if self._cfg.closeout_on_disconnect or self._finish_sent:
            self._start_closeout("DISCONNECTED")

    # ------------------------------------------------------------------ outbound
    def _spawn(self, coro) -> asyncio.Task:
        task = asyncio.ensure_future(coro)
        self._tasks.append(task)
        task.add_done_callback(lambda t: self._tasks.remove(t) if t in self._tasks else None)
        task.add_done_callback(_swallow)
        return task

    async def _send_json(self, event_type: str, payload: Dict[str, Any]) -> None:
        ws = self._ws
        if ws is None:
            raise ConnectionLostError("socket is not open", self.session_id)
        msg = json.dumps({"eventType": event_type, "epoch": self._epoch, "payload": payload})
        try:
            await ws.send(msg)
        except Exception as exc:
            raise ConnectionLostError("socket send failed (%s)" % type(exc).__name__, self.session_id) from exc

    async def _rotate(self) -> None:
        cap = {"capturedSamples": str(self._captured)}
        try:
            async with self._send_lock:
                await self._send_json("session.pause", cap)
                await self._send_json("session.resume", cap)
        except ConnectionLostError:
            pass

    def _schedule_pump(self) -> None:
        if self._can_send():
            self._spawn(self._pump_safe())

    async def _pump_safe(self) -> None:
        try:
            await self._pump()
        except ConnectionLostError:
            pass

    def _can_send(self) -> bool:
        return (self._ws is not None and self._state in (S_OPEN, S_FINISHING) and not self._finish_sent
                and self._server_accepting and not self._rotating and not self._user_paused and self._fatal is None
                and not self._closing_out)

    async def _pump(self) -> None:
        async with self._send_lock:
            while self._can_send():
                pos = self._epoch_start + self._sent
                avail = self._captured - pos
                if avail <= 0:
                    break
                n = min(self._frame_samples, avail)
                if n < self._frame_samples and not self._flush:
                    break
                start = (pos - self._buf_base) * self._bps
                chunk = bytes(self._buf[start:start + n * self._bps])
                if self._sent == self._acked:
                    self._last_progress = time.monotonic()  # the no-ack clock starts with unconfirmed audio
                payload: Dict[str, Any] = {"audioBase64": base64.b64encode(chunk).decode("ascii"),
                                           "sampleOffset": str(self._sent), "capturedSamples": str(self._captured)}
                if self._preceding is not None:
                    payload["precedingText"] = self._preceding
                    self._preceding = None
                epoch = self._epoch
                self._sent += n
                self._frames_sent += 1
                ws = self._ws
                try:
                    await ws.send(json.dumps({"eventType": "audio.append", "epoch": epoch, "payload": payload}))
                except Exception as exc:
                    raise ConnectionLostError("socket send failed (%s)" % type(exc).__name__, self.session_id) from exc

    def _raise_if_broken(self) -> None:
        if self._fatal is not None:
            raise self._fatal
        if self._state == S_LOST:
            raise ConnectionLostError("connection lost; no more audio is accepted", self.session_id)
        if self._state in (S_CLOSED, S_COMPLETED):
            raise SessionStateError("session is %s" % self._state.lower())

    async def _wait_confirmed(self) -> None:
        while True:
            self._raise_if_broken()
            confirmed = self._epoch_start + self._acked
            if confirmed >= self._captured and not self._rotating:
                return
            if self._can_send() and self._epoch_start + self._sent < self._captured:
                await self._pump()
                continue
            idle = time.monotonic() - self._last_progress
            if idle > self._cfg.timeouts.ack:
                raise AckTimeoutError("no acknowledgement progress for %.1fs (sessionId=%s)"
                                      % (self._cfg.timeouts.ack, self.session_id))
            await self._wait_change(min(0.25, self._cfg.timeouts.ack - idle + 0.01))

    async def _wait_room(self, need: int) -> int:
        limit = self._cfg.timeouts.backpressure or self._cfg.timeouts.ack
        while True:
            self._raise_if_broken()
            if self._user_paused:
                raise SessionStateError("session is paused; resume() before sending audio")
            room = self._max_buffered - (self._captured - (self._epoch_start + self._acked))
            if room >= min(need, self._frame_samples) and room > 0:
                return room
            idle = time.monotonic() - self._last_progress
            if idle > limit:
                raise BackpressureTimeoutError("send buffer full for %.1fs without acknowledgement progress "
                                               "(sessionId=%s)" % (limit, self.session_id))
            await self._wait_change(min(0.25, limit - idle + 0.01))

    def _check_input(self) -> None:
        if self._state == S_PAUSED or self._user_paused:
            raise SessionStateError("session is paused; resume() before sending audio")
        if self._state != S_OPEN:
            self._raise_if_broken()
            raise SessionStateError("session is %s; no more audio is accepted" % self._state.lower())
        self._raise_if_broken()

    # ------------------------------------------------------------------ public operations
    async def send_audio(self, data: bytes, *, preceding_text: Optional[str] = None) -> None:
        """Queues audio in the session encoding. Any chunk size is accepted; complete ~100 ms
        frames are sent as soon as they exist. Blocks (asynchronously) while the bounded buffer
        is full. A trailing half PCM sample is kept for the next call."""
        async with self._op_lock:
            self._check_input()
            data = self._half + bytes(data)
            self._half = b""
            if self._bps == 2 and len(data) % 2:
                self._half, data = data[-1:], data[:-1]
            if preceding_text is not None:
                self._preceding = preceding_text
            off = 0
            total = len(data)
            while off < total:
                need = (total - off) // self._bps
                room = await self._wait_room(need)
                take = min(room * self._bps, total - off)
                self._buf += data[off:off + take]
                self._captured += take // self._bps
                off += take
                self._peak_buffered = max(self._peak_buffered, self._captured - (self._epoch_start + self._acked))
                await self._pump()

    async def send_file(self, path: str, *, paced: bool = True, wav: Optional[bool] = None) -> None:
        """Streams a raw file in the session encoding, or a WAV file whose header matches it.
        ``paced=True`` sends by sample time (one second of audio per second). The file is read
        incrementally, one frame at a time, as the send buffer has room."""
        with open(path, "rb") as fh:
            is_wav = wav if wav is not None else path.lower().endswith(".wav")
            remaining = None
            if is_wav:
                info = read_wav_header(fh)
                if info.encoding != self.format.encoding:
                    raise AudioFormatError("WAV is %s but the session encoding is %s"
                                           % (info.encoding, self.format.encoding))
                fh.seek(info.data_offset)
                remaining = info.data_size
            # read one frame at a time while sending: memory stays bounded by the send buffer
            # (and its backpressure), whatever the file size
            await self.send_chunks(_read_frames(fh, self._frame_samples * self._bps, remaining), paced=paced)

    async def send_chunks(self, chunks, *, paced: bool = False) -> None:
        """Sends an iterable / async iterable of byte chunks. ``paced`` sleeps to sample time;
        a live source that already arrives at capture speed should use ``paced=False``."""
        t0 = time.monotonic()
        pos_bytes = 0
        if hasattr(chunks, "__aiter__"):
            async for chunk in chunks:
                await self.send_audio(chunk)
            return
        for chunk in chunks:
            if paced:
                # a frame is sent when its audio would have been captured
                pos_bytes += len(chunk)
                due = t0 + (pos_bytes / self._bps) / self.format.sample_rate_hz - time.monotonic()
                if due > 0:
                    await asyncio.sleep(due)
            await self.send_audio(chunk)

    async def commit(self) -> None:
        """``segment.commit``: submits the audio so far and keeps recording (no new epoch)."""
        async with self._op_lock:
            self._check_input()
            await self._flush_and_confirm()
            await self._send_json("segment.commit", {"capturedSamples": str(self._captured)})

    async def pause(self, *, via: str = "ws") -> None:
        """Flushes and confirms buffered audio, then closes the current segment. ``via='rest'``
        uses ``POST /pause`` on the same serialized control path."""
        async with self._op_lock:
            self._check_input()
            await self._flush_and_confirm()
            if via == "rest":
                view = await self._rest_control("pause")
                self._apply_rest_view(view)
            else:
                await self._send_json("session.pause", {"capturedSamples": str(self._captured)})
                await self._wait_until(lambda: not self._server_accepting, "pause")
            self._user_paused = True
            self._state = S_PAUSED

    async def resume(self, *, via: str = "ws") -> None:
        """Opens the next segment (a new epoch). Sample offsets restart at 0 in that epoch;
        the session-wide captured count continues."""
        async with self._op_lock:
            if self._state != S_PAUSED:
                self._raise_if_broken()
                raise SessionStateError("resume() needs a paused session (state=%s)" % self._state.lower())
            old = self._epoch
            if via == "rest":
                view = await self._rest_control("resume")
                self._apply_rest_view(view)
                if (view.epoch or 0) > old and view.accepting:
                    self._adopt_epoch(view.epoch)
            else:
                await self._send_json("session.resume", {"capturedSamples": str(self._captured)})
                await self._wait_until(lambda: self._epoch > old, "resume")
            if self._epoch <= old:
                raise SessionStateError("server did not open a new epoch")
            self._user_paused = False
            self._state = S_OPEN

    async def finish(self) -> FinishResult:
        """Flushes the tail, waits until every sample is confirmed (handling rotation),
        sends ``session.finish`` and returns the server's terminal view. Repeated calls return
        the same result. On disconnect/timeout it closes out over REST (see
        :attr:`FinishResult.closeout_reason`) or raises :class:`FinishUnconfirmedError`."""
        if self._result is not None:
            return self._result
        if self._closeout_task is not None:
            return await asyncio.shield(self._closeout_task)
        async with self._op_lock:
            if self._result is not None:
                return self._result
            if self._closeout_task is not None:
                return await asyncio.shield(self._closeout_task)
            if self._half:
                raise AudioFormatError("one byte of an incomplete PCM sample remains; send the missing byte "
                                       "or call discard_partial_sample() before finish()")
            if self._state == S_COMPLETED and self._completed_view is not None:
                return self._finalize(self._completed_view, via_rest=False, reason=None)
            if self._state == S_CLOSED:
                raise SessionStateError("session handle is closed; use client.finish_session(session_id)")
            if self._state == S_LOST:
                return await self._ensure_closeout("DISCONNECTED")
            self._state = S_FINISHING
            if not self._user_paused and self._fatal is None:
                try:
                    self._flush = True
                    await self._wait_confirmed()
                except AckTimeoutError:
                    return await self._ensure_closeout("ACK_TIMEOUT")
                except ConnectionLostError:
                    return await self._ensure_closeout("DISCONNECTED")
                except (ServerEventError, InsufficientCharactersError):
                    pass  # stream stopped by the server; settle what it confirmed
                finally:
                    self._flush = False
            self._finish_sent = True
            try:
                await self._send_json("session.finish", {"capturedSamples": str(self._captured)})
            except ConnectionLostError:
                return await self._ensure_closeout("DISCONNECTED")
            deadline = time.monotonic() + self._cfg.timeouts.finish
            while self._completed_view is None:
                if self._closeout_task is not None:
                    return await asyncio.shield(self._closeout_task)
                if self._state == S_LOST:
                    return await self._ensure_closeout("DISCONNECTED")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return await self._ensure_closeout("FINISH_TIMEOUT")
                await self._wait_change(min(0.25, remaining))
            result = self._finalize(self._completed_view, via_rest=False, reason=None)
            self._state = S_COMPLETED
            self._intentional_close = True
            self._spawn(self._close_ws())
            return result

    def discard_partial_sample(self) -> bool:
        """Drops a pending half PCM sample (returns whether one was pending)."""
        had = bool(self._half)
        self._half = b""
        return had

    async def abort_connection(self) -> None:
        """Test/diagnostic helper: drops this client's socket without a close handshake or
        finish, exactly like a network failure. The disconnect path then runs."""
        ws = self._ws
        if ws is not None and getattr(ws, "transport", None) is not None:
            ws.transport.abort()

    async def close(self) -> None:
        """Releases local resources only (socket, tasks, callback thread). It is not a
        business finish; an unfinished session stays open server-side (paused on disconnect)."""
        if self._closed:
            return
        self._closed = True
        self._intentional_close = True
        if self._state not in (S_COMPLETED, S_LOST):
            self._state = S_CLOSED
        self._touch()
        await self._close_ws()
        for task in list(self._tasks):
            if task is not self._closeout_task:
                task.cancel()
        if self._reader_task is not None:
            self._reader_task.cancel()
        # let queued events reach handlers within the close timeout, then stop
        try:
            self._queue.put_nowait(None)
        except asyncio.QueueFull:
            pass
        if self._dispatcher is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self._dispatcher), self._cfg.timeouts.close)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._dispatcher.cancel()
        if self._iter_queue is not None:
            self._iter_end = True  # a full iterator queue still ends once it is drained
            try:
                self._iter_queue.put_nowait(None)
            except asyncio.QueueFull:
                pass
        if self._executor is not None:
            self._executor.shutdown(wait=False)
        self._client._forget(self)  # a closed session is no longer held by the client

    async def events(self) -> AsyncIterator[Event]:
        """``async for event in session.events()`` (requires ``iterate_events=True``)."""
        if self._iter_queue is None:
            raise SessionStateError("connect(..., iterate_events=True) to iterate events")
        while True:
            ev = await self._next_event(None)
            if ev is None:
                return
            yield ev

    async def _next_event(self, timeout: Optional[float]) -> Optional[Event]:
        """Next event of the bounded iterator queue; None once the session is closed and the
        queue is drained. Raises ``asyncio.TimeoutError`` after ``timeout`` without an event."""
        q = self._iter_queue
        if q is None:
            raise SessionStateError("connect(..., iterate_events=True) to iterate events")
        if self._iter_end and q.empty():
            return None
        if timeout is None:
            return await q.get()
        return await asyncio.wait_for(q.get(), timeout)

    async def __aenter__(self) -> "AsyncRealtimeSession":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    # ------------------------------------------------------------------ helpers
    async def _flush_and_confirm(self) -> None:
        self._flush = True
        try:
            await self._pump()
            await self._wait_confirmed()
        finally:
            self._flush = False

    async def _wait_until(self, predicate, what: str) -> None:
        deadline = time.monotonic() + self._cfg.timeouts.ack
        while not predicate():
            self._raise_if_broken()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AckTimeoutError("%s not acknowledged in %.1fs (sessionId=%s)"
                                      % (what, self._cfg.timeouts.ack, self.session_id))
            await self._wait_change(min(0.25, remaining))

    async def _rest_control(self, action: str) -> SessionView:
        """REST pause/resume. An unknown outcome is resolved by reading the session first; the
        call is repeated only when the read shows it did not take effect."""
        call = self._rest.pause if action == "pause" else self._rest.resume
        old_epoch = self._epoch
        try:
            return await call(self.session_id, self._captured)
        except TransportError:
            view = await self._rest.get(self.session_id)
            done = (not view.accepting) if action == "pause" else ((view.epoch or 0) > old_epoch and bool(view.accepting))
            if done:
                return view
            return await call(self.session_id, self._captured)

    def _apply_rest_view(self, view: SessionView) -> None:
        self.view = view
        self.latest_usage = view.usage
        self._server_accepting = bool(view.accepting)
        if view.usage.received_samples is not None:
            self._server_received = view.usage.received_samples
        self._touch()

    def _start_closeout(self, reason: str) -> "asyncio.Task":
        if self._closeout_task is None:
            self._closeout_task = asyncio.ensure_future(self._closeout(reason))
            self._closeout_task.add_done_callback(_swallow)
        return self._closeout_task

    async def _ensure_closeout(self, reason: str) -> FinishResult:
        return await asyncio.shield(self._start_closeout(reason))

    async def _closeout(self, reason: str) -> FinishResult:
        self._closing_out = True
        if self._state not in (S_CLOSED,):
            self._state = S_LOST
        self._touch()
        await self._abort_ws()
        # one budget for the whole close-out: every GET/finish gets only the time that is left
        deadline = time.monotonic() + self._cfg.timeouts.closeout
        last: Optional[BaseException] = None
        attempt = 0
        while time.monotonic() < deadline:
            attempt += 1
            try:
                view = await self._rest.get(self.session_id, deadline=deadline)
                if not view.is_terminal:
                    view = await self._rest.finish(self.session_id, self._captured, deadline=deadline)
                if view.is_terminal:
                    return self._finalize(view, via_rest=True, reason=reason)
                last = None
            except ApiError as err:
                if not err.retryable:
                    raise FinishUnconfirmedError("close-out was refused (%s)" % err.error_code, self.session_id,
                                                 cause=err)
                last = err
            except TransportError as err:
                last = err
            await asyncio.sleep(min(0.25 * (2 ** min(attempt, 4)), max(0.0, deadline - time.monotonic())))
        raise FinishUnconfirmedError("the end of the session is not confirmed", self.session_id, cause=last)

    def _finalize(self, view: SessionView, *, via_rest: bool, reason: Optional[str]) -> FinishResult:
        self.view = view
        self.latest_usage = view.usage
        self.transcript.merge_view(view)
        result = FinishResult(status=view.status, session_id=view.session_id or self.session_id,
                              transcription_id=view.transcription_id or self.transcription_id, usage=view.usage,
                              notices=view.notices, lines=self.transcript.lines, view=view, via_rest=via_rest,
                              closeout_reason=reason, server_errors=list(self.server_errors),
                              callback_errors=self.callback_errors, dropped_events=self.dropped_events)
        self._result = result
        if self._state != S_CLOSED:
            self._state = S_COMPLETED
        self._touch()
        return result

    async def _close_ws(self) -> None:
        ws = self._ws
        if ws is None:
            return
        try:
            await asyncio.wait_for(ws.close(), self._cfg.timeouts.close)
        except Exception:
            await self._abort_ws()

    async def _abort_ws(self) -> None:
        ws = self._ws
        if ws is not None and getattr(ws, "transport", None) is not None:
            try:
                ws.transport.abort()
            except Exception:
                pass

    # ------------------------------------------------------------------ dispatch
    def _has_consumers(self) -> bool:
        return bool(self._handlers) or self._on_event is not None

    def _enqueue(self, event: Event) -> None:
        # never blocks: the socket reader keeps processing acknowledgements whatever the consumers do
        if self._iter_queue is not None:
            try:
                self._iter_queue.put_nowait(event)
            except asyncio.QueueFull:
                self._dropped(OverflowError("event iterator queue full; event not delivered (state and "
                                            "transcript snapshot are still updated)"), event)
        if not self._has_consumers():
            return
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self._dropped(OverflowError("event handler queue full; event not delivered (state and transcript "
                                        "snapshot are still updated)"), event)

    def _dropped(self, exc: BaseException, event: Event) -> None:
        self.dropped_events += 1
        if self.dropped_events == 1:
            log.warning("event queue full (event_queue_size=%d); %s not delivered, further drops are counted "
                        "in dropped_events (sessionId=%s)", self._cfg.event_queue_size, event.type, self.session_id)
        self._report(exc, event)

    def _report(self, exc: BaseException, event: Optional[Event]) -> None:
        if self._on_callback_error is not None:
            try:
                self._on_callback_error(exc, event)
            except Exception:
                pass

    async def _dispatch_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            event = await self._queue.get()
            if event is None:
                return
            targets = list(self._handlers.get(event.type, ())) + list(self._handlers.get("*", ()))
            if self._on_event is not None:
                targets.append(self._on_event)
            for fn in targets:
                try:
                    if asyncio.iscoroutinefunction(fn):
                        await fn(event)
                    else:
                        if self._executor is None:
                            self._executor = ThreadPoolExecutor(max_workers=1,
                                                                thread_name_prefix="myvocal-stt-callback")
                        await loop.run_in_executor(self._executor, fn, event)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.callback_errors += 1
                    self._report(exc, event)


def _read_frames(fh, frame_bytes: int, remaining: Optional[int]):
    """Yields the file frame by frame; nothing is read ahead of the sender."""
    while True:
        size = frame_bytes if remaining is None else min(frame_bytes, remaining)
        if size <= 0:
            return
        chunk = fh.read(size)
        if not chunk:
            return
        if remaining is not None:
            remaining -= len(chunk)
        yield chunk


def _swallow(task: "asyncio.Future") -> None:
    if not task.cancelled():
        task.exception()
