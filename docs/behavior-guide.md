# MyVocal STT Realtime SDKs: shared behaviour guide

Applies to Java (`ai.myvocal:myvocal-stt-realtime`), Go
(`github.com/MyVocal-AI/myvocal-stt-realtime-sdks/go`) and Python (`myvocal-stt-realtime`).
Names follow each language's conventions; the behaviour is identical. Normative details:
`contract/realtime.md`.

## 1. The normal path

1. Create a `RealtimeClient` with your access key (server side only).
2. `connect` with a session request (title, languageHint, options). Register event handlers here.
3. Send audio: a file (`sendFile`, paced by sample time), a stream, or arbitrary byte chunks
   (`sendAudio`). The SDK builds ~100 ms frames, joins half PCM samples across calls and tracks
   cursors. A live source that already arrives at capture speed is not paced again.
4. Optional: `commit` (line boundary, keep recording), `pause` / `resume` (new epoch).
5. `finish`: flushes the tail, waits until every sample is acknowledged (including any server
   rotation), ends the session and returns the server result: `status`
   (`COMPLETED` / `PARTIAL` / `FAILED`, unchanged), `transcriptionId`, `usage`, `notices`, lines.
6. `close` releases all local resources. It is **not** a business end. Call `finish` first; in a
   `finally` block a bounded `finish` then `close` never hangs (all waits have timeouts).

## 2. close vs finish

| | `finish` | `close` |
|---|---|---|
| ends the session server-side | yes (idempotent, same result on repeat) | no; an unfinished session is paused by the server when the socket goes away |
| settles usage | yes, by the server | no |
| returns the result | yes | no |
| releases local resources | closes the session's socket only | yes, completely: socket, threads / tasks, event queues; the client stops holding the session |

## 3. Acknowledgement is not billing

`usage.updated` acknowledgements (`sentSamples`, `receivedSamples`) prove transport receipt. The
billable amount is the server's `usage.billableCharacters` in the terminal result. The SDK never
computes prices.

## 4. Empty vs missing results

- entities `null` / `None` / nil: the entities result has not arrived.
- entities `[]` with `entitiesArrived=true`: it arrived and is empty (a valid model result).
- a supplement that never arrives is reported by the server as a `SUPPLEMENT_NOT_RECEIVED` notice
  naming the line and kind. The SDK does not guess from silence, and does not turn a
  `COMPLETED` session into `FAILED` because a supplement is missing.

## 5. Disconnects and timeouts

The SDK does **not** reconnect or resend audio after a network failure. When the socket drops
unexpectedly, acknowledgements stop progressing while audio is outstanding, or the completion
event does not arrive in time, it:

1. stops accepting audio (further input raises a connection-lost error);
2. reads the original session over REST; if terminal, returns it;
3. otherwise finishes it over REST (idempotent) within the close-out timeout. That timeout is
   one budget for the whole close-out: every REST read, finish and retry wait inside it gets only
   the time that is left, and nothing new starts once it is used up;
4. returns the real server result marked `viaRest` with a `closeoutReason`.

If REST is unreachable, you get a *finish unconfirmed* error carrying the `sessionId`; call
`getSession` / `finishSession` later. Audio the server never acknowledged is not counted as
received, and no new session is created. Cross-process or cross-server lossless recovery is
not provided.

## 6. Pause and resume

`pause` flushes and confirms buffered audio, then closes the current epoch; sending while
paused is a state error (the pause duration is never sent as audio). `resume` opens a new epoch:
`sampleOffset` restarts at 0 while `capturedSamples` keeps counting. Both work over the socket
or REST (`Via.REST` / `ViaREST` / `via="rest"`).

## 7. Backpressure, callbacks, concurrency

- The SDK holds at most `maxBufferedMs` (default 30 s) of unacknowledged audio. When full,
  `sendAudio` waits; without acknowledgement progress it raises a backpressure error. Audio is
  never silently dropped. `sendFile` reads the file frame by frame as the buffer has room, so
  memory does not grow with the file size.
- Handlers and event iteration run off the network path and use bounded queues (event queue
  size, default 10 000). Slow consumers do not delay acknowledgements; exceptions go to
  `onCallbackError`. An event that does not fit is never dropped silently: it is counted
  (`droppedEvents`) and reported to `onCallbackError`, and the transcript snapshot and the result
  still keep every line.
- One session's operations are serialized; any number of sessions may run in parallel with
  independent cursors and results.

## 8. Observed service behaviour (root cause not confirmed)

These were seen in real-API testing. The SDK reports them as the service returns them; read
`status`, `notices` and `usage` of the result, the service's final state is authoritative.

- **Long idle.** After an audio gap of about 75 s or more, the service may stop acknowledging
  further audio. The SDK then closes the session out after the acknowledgement timeout and the
  session may end `PARTIAL`. The client WebSocket ping keeps the connection open, but it does not
  guarantee that the session keeps processing audio.
- **Manual segment mode.** In manual mode the service may return `PARTIAL` even when all audio was
  received (the receipt counters show every sample). Manual mode remains supported.

## 9. Network and security

- `accessKey` header only; never in URLs. Ticket helper: `createTicket` issues a single-use
  ticket for your own browser client. These SDKs never connect with tickets.
- TLS verification is always on; private CAs can be added. Explicit proxies are supported, and a
  borrowed HTTP client is never closed by the SDK.
- Client WebSocket ping every 20 s keeps idle connections open; it is not audio and is unrelated
  to the `heartbeatIntervalMs` processing option.
- The SDKs never log audio, transcript text, keys, tickets, auth headers or signed URLs.

## 10. Timeouts (defaults)

request 30 s · create 30 s (with idempotent retries) · connect 15 s · ack 10 s (only while
audio is unacknowledged) · finish 60 s · close-out 70 s · close 5 s. These are configuration
values, not service latency guarantees.

## 11. Audio input

Encodings: `pcm_s16le_8000/16000/22050/24000/44100/48000` (16-bit little-endian mono) and
`mulaw_8000`. WAV headers are read by the SDK (`sendFile(*.wav)` / WAV helpers) and must match the
session encoding. MP3, URLs and microphones are handled outside the SDK (see each language's
`adapters.md`); the core packages have no FFmpeg or sound-card dependency.
