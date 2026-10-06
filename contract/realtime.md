# Shared behaviour contract (Java / Go / Python, v1.0.0)

The one behaviour spec every SDK implements and every local test checks against
`tools/fixture/server.py` + `fixtures/scenarios.json`. It models the existing public MyVocal
protocol (myvocal-java `81e70985`, `docs/public-api/stt-contract.md`); it adds no endpoint.

## Wire

| Item | Rule |
|---|---|
| REST base | `{base}/sound_clone/api/v1/stt/realtime`; `POST /sessions` (header `Idempotency-Key`), `GET /sessions/{id}`, `POST /sessions/{id}/tickets`, `POST /sessions/{id}/pause|resume|finish` (optional body `{"capturedSamples":"<decimal>"}`) |
| Envelope | success only when `code == 1`; HTTP 200 with `code != 1` is an error carrying `data.errorCode/retryable/action/requestId/details` |
| Identity | `accessKey` header on REST and WebSocket. Never in a URL. Ticket = helper only (SDK never connects with it) |
| Stream URL | the session's `streamUrl`; relative paths resolve against the base; http→ws, https→wss |
| Client events | `{eventType, epoch, payload}`: `audio.append` (`audioBase64`, `sampleOffset` string, `capturedSamples` string, optional `precedingText`), `segment.commit`, `session.pause`, `session.resume`, `session.finish` (each with `capturedSamples` string) |
| Server events | `session.ready`, `transcript.delta`, `transcript.final`, `transcript.alignment`, `transcript.revision`, `transcript.entities`, `usage.updated`, `session.notice`, `session.completed`, `session.error`; envelope `sequence` is a JSON number, per connection |
| 64-bit fields | sample / usage / ms counters are written as decimal strings and read as integers (`long`, `int64`, `int`); `null` stays null; epoch / booleans / indexes keep protocol types |

## Cursors

- `capturedSamples`: whole samples handed to the SDK in this session (bytes ÷ bytes-per-sample), cumulative.
- `sampleOffset`: position of the next frame in the current epoch; `sentSamples`: acknowledged in the current epoch (`usage.updated`).
- `receivedSamples`: server transport receipt, session-wide. Not a billing or model-result proof.
- Confirmed position = epoch start + `sentSamples`. Buffered = captured − confirmed (bounded by `maxBufferedMs`; the sender waits, then raises a backpressure error; audio is never dropped). Confirmed bytes are released.
- Format and rate come from `effectiveOptions`; a mismatch with the request or a WAV header is an `AudioFormat` error, never sent.
- Frames are ~`frameMs` (default 100 ms) of whole samples; a PCM half sample carries to the next call; a half sample left at finish is an error (`discardPartialSample`).

## Rotation and epochs

`usage.updated` with `rotate=true` for the current epoch: the triggering frame was **not**
stored. The SDK sends `session.pause` + `session.resume`, adopts the new epoch when a
`usage.updated` with a higher epoch and `accepting=true` arrives, restarts `sampleOffset` at 0
and resends only the unconfirmed bytes. Captured is not counted twice. Usage for an older epoch
never moves the current cursors. Finish waits until every captured sample is confirmed and no
rotation is in progress.

## Lifecycle

| Operation | Rule |
|---|---|
| create | retried only on transport failure / retryable envelope, same `Idempotency-Key` and body; auth / input / balance errors are not retried |
| commit | flush + confirm, then `segment.commit`; same epoch |
| pause / resume (WS or REST) | flush + confirm, close the epoch; input while paused is a state error (paused time is never audio). Resume opens a new epoch. An unknown REST outcome is resolved by GET before any repeat |
| finish | flush tail → confirm → `session.finish` → `session.completed`; repeated calls return the same result; server status returned as is (`COMPLETED`/`PARTIAL`/`FAILED`) with notices, usage, transcriptionId |
| close | local resources only (socket, callback worker); idempotent; never a business finish; borrowed HTTP clients are not closed |
| serialization | one session's sends and lifecycle operations are serialized; sessions are independent (cursors, transcript, idempotency keys) |

## Disconnect close-out (no reconnect, no resend)

Unexpected socket end, no acknowledgement progress within `ack` (only while unconfirmed audio
exists; idle is not a timeout), or no `session.completed` within `finish`:

1. stop accepting audio (`ConnectionLost` on further input);
2. `GET /sessions/{id}` of the **original** session; terminal → return it;
3. otherwise `POST .../finish` with the captured count, retried within `closeout`;
4. return the server's real terminal result with `viaRest=true` and `closeoutReason`
   (`DISCONNECTED`, `ACK_TIMEOUT`, `FINISH_TIMEOUT`);
5. not confirmable → `FinishUnconfirmed(sessionId)`; the caller may `getSession` / `finishSession` later.

`closeout` is one overall budget: each GET, finish and retry wait inside the close-out is bounded
by the time left (the global `request` setting is unchanged), and no request starts after it is
used up. Running out is reported as `FinishUnconfirmed`, never as a server status.

Unconfirmed buffered audio is never presented as received; no new (chargeable) session is
created; nothing is replayed.

## Events and results

The network path updates state, then the transcript snapshot, then queues the event for
callbacks (bounded queue, dedicated worker). Handlers are registered before the socket opens.
A slow handler never blocks acknowledgements; a handler exception is reported via
`onCallbackError` and counted, never treated as a server failure; an overflowed queue reports
each undelivered event, and the final result still has every line. `delta` replaces partial
text; `final`/`alignment`/`revision`/`entities` merge by line id. Entities `null` = not arrived,
`[]` + arrived flag = arrived empty. Missing supplements come only from `SUPPLEMENT_NOT_RECEIVED`
notices. `sequence` is checked only within one connection.

## Defaults

frame 100 ms · buffer 30 s · client ping 20 s (unrelated to `heartbeatIntervalMs`) · create
retries 2 · frame replay on retryable `session.error` 3 · timeouts: request 30 s, create 30 s,
connect 15 s, ack 10 s, finish 60 s, close-out 70 s, close 5 s; backpressure wait = ack. Timeouts are configuration, not
latency SLAs.
