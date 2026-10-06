# MyVocal STT Realtime SDK for Python

Server-side SDK for the MyVocal Speech to Text Realtime API
(`/sound_clone/api/v1/stt/realtime`). You configure a session, hand it audio, consume
events and call `finish()`. The SDK frames audio (~100 ms), tracks sample cursors, handles
in-connection rotation and the end of the session. On an unexpected disconnect it closes
out the **original** session over REST. It does not reconnect or resend audio.

- Python 3.10+; dependencies: `httpx`, `websockets`
- `RealtimeClient` (blocking) and `AsyncRealtimeClient` (asyncio) share one engine

## Install

```bash
pip install myvocal-stt-realtime==1.0.0     # PyPI: https://pypi.org/project/myvocal-stt-realtime/
```

If PyPI does not list 1.0.0 yet, use the same wheel and sdist attached to the GitHub Release
(https://github.com/MyVocal-AI/myvocal-stt-realtime-sdks/releases/tag/v1.0.0, checksums in `SHA256SUMS`):
`pip install https://github.com/MyVocal-AI/myvocal-stt-realtime-sdks/releases/download/v1.0.0/myvocal_stt_realtime-1.0.0-py3-none-any.whl`

## Quick start (blocking)

```python
import os
from myvocal_stt_realtime import RealtimeClient, SessionOptions, SessionRequest

with RealtimeClient(os.environ["MYVOCAL_ACCESS_KEY"]) as client:
    request = SessionRequest(title="meeting", language_hint="en",
                             options=SessionOptions(input_encoding="pcm_s16le_16000"))
    session = client.connect(request, handlers={"transcript.final": lambda e: print(e.text)})
    try:
        session.send_file("speech.wav")      # paced by sample time
        result = session.finish()            # terminal server result
    finally:
        session.close()                      # local resources only, not a business finish
print(result.status, result.transcription_id, result.usage.billable_characters)
```

## Quick start (asyncio)

```python
async with AsyncRealtimeClient(os.environ["MYVOCAL_ACCESS_KEY"]) as client:
    session = await client.connect(request, iterate_events=True)
    ...
    await session.send_audio(chunk)          # any chunk size; live sources are not re-paced
    result = await session.finish()
    await session.close()
```

## Session request

| Python | Wire | Notes |
|---|---|---|
| `SessionRequest.title`, `language_hint` | `title`, `languageHint` | top level |
| `SessionOptions.input_encoding` | `inputEncoding` | `pcm_s16le_8000/16000/22050/24000/44100/48000`, `mulaw_8000` |
| `additional_language_hints`, `segment_commit_mode` (`manual`/`vad`), `speech_detection_threshold`, `commit_silence_ms`, `minimum_speech_ms`, `minimum_silence_ms`, `include_alignment`, `include_detected_language`, `vocabulary_hints` (≤ 50), `remove_disfluencies`, `entity_categories`, `rewrite_instruction`, `suppress_background_speech`, `heartbeat_interval_ms`, `processing_content_storage` | camelCase of the same name under `options` | `None` = omitted; `False`/`0` are sent as given |

`send_audio(data, preceding_text=...)` attaches optional `precedingText` to the next frame.
Invalid combinations (`rewrite_instruction` with `entity_categories`,
`suppress_background_speech` with `include_alignment`, more than 50 hints) come back as
`InputInvalidError`. The SDK does not drop fields and does not retry.

## Session handle

| Method | Meaning |
|---|---|
| `send_audio(bytes)`, `send_chunks(iter, paced=False)`, `send_file(path, paced=True)` | buffer and frame audio; half PCM samples carry across calls |
| `commit()` | close the current line, keep recording (same epoch) |
| `pause(via="ws"/"rest")`, `resume(...)` | flush + confirm, then close the epoch / open a new epoch. Input while paused raises `SessionStateError` |
| `finish()` | flush the tail, wait for every sample to be confirmed (incl. rotation), send finish and return `FinishResult`. Repeated calls return the same result |
| `close()` | release socket, tasks and callback thread. It does **not** end the session |
| `cursors`, `transcript`, `latest_usage`, `stats()` | local state (sample cursors, merged lines, last usage) |

Client REST helpers: `create_session`, `get_session`, `pause_session`, `resume_session`,
`finish_session`, `create_ticket` (a single-use browser ticket for your own front end; this
SDK never connects with it, and you must never log `ticket.ticket`).

## Events

`session.ready`, `transcript.delta`, `transcript.final`, `transcript.alignment`,
`transcript.revision`, `transcript.entities`, `usage.updated`, `session.notice`,
`session.completed`, `session.error`. Register `handlers={"transcript.final": fn, "*": fn}`,
`on_event=fn`, or iterate (`iterate_events=True`). Handlers run off the network path. A slow
handler never delays acknowledgements. A handler exception goes to `on_callback_error` and
never counts as a server failure. Handler and iterator queues (sync and async) hold at most
`event_queue_size` events. When one overflows, the dropped event is reported there and counted
in `FinishResult.dropped_events`; the socket reader never waits for a consumer. The transcript
snapshot still has every line. `send_file` reads the file frame by frame as the send buffer has
room. `close()` also removes the session from the client.

`TranscriptLine.entities is None` means the entities result has not arrived.
`entities == []` with `entities_arrived` means it arrived empty. A supplement that never
came is reported by a `SUPPLEMENT_NOT_RECEIVED` notice naming the line and kind. The SDK
never infers it.

## Results and errors

`FinishResult.status` is the server's `COMPLETED`, `PARTIAL` or `FAILED`, unchanged.
`usage.billable_characters` is the server ledger value. Acknowledged samples
(`usage.updated`) prove transport receipt, not billing.

| Situation | What you get |
|---|---|
| socket drops / no acknowledgement progress (`Timeouts.ack`) / no `session.completed` (`Timeouts.finish`) | REST close-out of the same session: `result.via_rest`, `result.closeout_reason` = `DISCONNECTED` / `ACK_TIMEOUT` / `FINISH_TIMEOUT` |
| REST unreachable during close-out (`Timeouts.closeout`) | `FinishUnconfirmedError(session_id)`. Call `get_session` / `finish_session` later |
| HTTP 200 envelope with `code != 1` | `ApiError` subclasses with `error_code`, `retryable`, `request_id` |
| invalid key | `AuthenticationError` (not retried) |
| balance exhausted | `InsufficientCharactersError` (not retried) |
| residual half sample at finish | `AudioFormatError` (`discard_partial_sample()` to drop it) |

Create is retried only on transport errors / retryable envelopes, with the same
`Idempotency-Key` and body, so a lost response never creates a second session.

## Network

`proxy=` explicit proxy, otherwise `trust_env=True` honours `HTTP(S)_PROXY`/`NO_PROXY`.
TLS verification is always on; pass `ssl_context=` for a private CA. `http_client=` (async
only) borrows your `httpx.AsyncClient`, which the SDK never closes. Client WebSocket pings
every `ping_interval=20` s keep idle sockets open. They are not audio, and they are
unrelated to the `heartbeat_interval_ms` processing option. The access key travels only in
the `accessKey` header and never in a URL. The SDK does not log audio, transcript text,
keys or tickets.

## Examples

`examples/file_stream.py`, `byte_stream_async.py`, `manual_vad.py`, `pause_resume.py`
(ticket helper included), `abnormal_finish.py`, `adapters.md` (MP3/URL/microphone via
third-party tools). Run with `MYVOCAL_ACCESS_KEY` (and optional `MYVOCAL_API_BASE`) set.

## Development

```bash
python -m pytest            # runs against tools/fixture/server.py (local protocol fixture)
python -m build             # wheel + sdist
```
