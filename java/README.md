# MyVocal STT Realtime SDK for Java

Server-side SDK for the MyVocal Speech to Text Realtime API
(`/sound_clone/api/v1/stt/realtime`). You configure a session, hand it audio, consume events
and call `finish()`. The SDK frames audio (~100 ms), tracks sample cursors, handles
in-connection rotation and the end of the session. On an unexpected disconnect it closes out
the **original** session over REST. It does not reconnect or resend audio.

- Java 8+ (verified on 8, 11, 13, 17, 21); no Spring. Dependencies: OkHttp 5 (see `DEPENDENCIES.md`)
- Blocking calls, `CompletableFuture` variants (`connectAsync`, `finishAsync`) and a `RealtimeListener`

## Install

```xml
<dependency>
  <groupId>ai.myvocal</groupId>
  <artifactId>myvocal-stt-realtime</artifactId>
  <version>1.0.0</version>
</dependency>
```

The coordinates are those of Maven Central
(https://central.sonatype.com/artifact/ai.myvocal/myvocal-stt-realtime). The same JAR, sources,
Javadoc and POM are attached to the GitHub Release
(https://github.com/MyVocal-AI/myvocal-stt-realtime-sdks/releases/tag/v1.0.0, checksums in `SHA256SUMS`). If Central does not list 1.0.0 yet (or you have no Central access), install them into
your local repository:
`mvn install:install-file -Dfile=myvocal-stt-realtime-1.0.0.jar -DpomFile=myvocal-stt-realtime-1.0.0.pom -Dsources=myvocal-stt-realtime-1.0.0-sources.jar -Djavadoc=myvocal-stt-realtime-1.0.0-javadoc.jar`.

## Quick start (blocking)

```java
try (RealtimeClient client = RealtimeClient.builder(System.getenv("MYVOCAL_ACCESS_KEY")).build()) {
    SessionRequest request = new SessionRequest().title("meeting").languageHint("en")
            .options(new SessionOptions().inputEncoding("pcm_s16le_16000"));
    RealtimeSession session = client.connect(new ConnectOptions().request(request)
            .on(RealtimeEvent.TRANSCRIPT_FINAL, e -> System.out.println(e.getText())));
    try {
        session.sendFile(Paths.get("speech.wav"), true); // paced by sample time
        FinishResult result = session.finish();          // terminal server result
        System.out.println(result.getStatus() + " " + result.getTranscriptionId());
    } finally {
        session.close();                                 // local resources only, not a business finish
    }
}
```

## Future / listener

```java
RealtimeSession session = client.connectAsync(new ConnectOptions().request(request).listener(new RealtimeListener() {
    @Override public void onTranscriptFinal(TranscriptLine line, RealtimeEvent e) { ... }
    @Override public void onNotice(Notice notice) { ... }
})).get();
session.sendAudio(chunk);                                // any chunk size; live sources are not re-paced
FinishResult result = session.finishAsync().get();       // cancel(true) interrupts the wait
```

## Session request

| Java | Wire | Notes |
|---|---|---|
| `SessionRequest.title`, `languageHint` | `title`, `languageHint` | top level |
| `SessionOptions.inputEncoding` | `inputEncoding` | `pcm_s16le_8000/16000/22050/24000/44100/48000`, `mulaw_8000` |
| `additionalLanguageHints`, `segmentCommitMode` (`manual`/`vad`), `speechDetectionThreshold`, `commitSilenceMs`, `minimumSpeechMs`, `minimumSilenceMs`, `includeAlignment`, `includeDetectedLanguage`, `vocabularyHints` (≤ 50), `removeDisfluencies`, `entityCategories`, `rewriteInstruction`, `suppressBackgroundSpeech`, `heartbeatIntervalMs`, `processingContentStorage` | same name under `options` | `null` = omitted; `false`/`0` are sent as given |

`sendAudio(data, precedingText)` attaches optional `precedingText` to the next frame. Invalid
combinations (`rewriteInstruction` with `entityCategories`, `suppressBackgroundSpeech` with
`includeAlignment`, more than 50 hints) raise `InputInvalidException`. The SDK does not drop
fields and does not retry.

## Session handle (`RealtimeSession`)

| Method | Meaning |
|---|---|
| `sendAudio(byte[])`, `sendStream(InputStream, paced)`, `sendFile(Path, paced)` | buffer and frame audio; half PCM samples carry across calls; blocks only when the bounded buffer (`maxBufferedMs`) is full |
| `commit()` | close the current line, keep recording (same epoch) |
| `pause(Via.WS/REST)`, `resume(...)` | flush + confirm, then close the epoch / open a new epoch. Input while paused throws `SessionStateException` |
| `finish()` / `finishAsync()` | flush the tail, wait for every sample to be confirmed (including rotation), send finish, return `FinishResult`. Repeated calls return the same result |
| `close()` | release socket and callback thread. It does **not** end the session |
| `getCursors()`, `getTranscript()`, `getLatestUsage()`, `getStats()` | local state |

Client REST helpers: `createSession`, `getSession`, `pauseSession`, `resumeSession`,
`finishSession`, `createTicket` (a single-use browser ticket for your own front end; this SDK
never connects with it; never log `ticket.getTicket()`).

## Events

`session.ready`, `transcript.delta`, `transcript.final`, `transcript.alignment`,
`transcript.revision`, `transcript.entities`, `usage.updated`, `session.notice`,
`session.completed`, `session.error` (`RealtimeEvent.TYPES`). Register handlers before the
connection opens with `on(type, fn)`, `onEvent(fn)` or `listener(...)`. Handlers run on a
dedicated callback thread, so a slow handler never delays acknowledgements. A handler
exception goes to `onCallbackError` and never counts as a server failure. When the bounded
handler queue (`eventQueueSize`) overflows, the dropped event is reported there and counted
in `FinishResult.getDroppedEvents()`. The transcript snapshot still has every line.

`TranscriptLine.getEntities() == null` means the entities result has not arrived; an empty
list with `isEntitiesArrived()` means it arrived empty. A supplement that never came is
reported by a `SUPPLEMENT_NOT_RECEIVED` notice naming the line and kind.

## Results and errors

`FinishResult.getStatus()` is the server's `COMPLETED`, `PARTIAL` or `FAILED`, unchanged.
`getUsage().getBillableCharacters()` is the server ledger value. Acknowledged samples prove
transport receipt, not billing. 64-bit counters are `long`/`Long`; `null` stays `null`.

| Situation | What you get |
|---|---|
| socket drops / no acknowledgement progress (`Timeouts.ackMillis`) / no `session.completed` (`finishMillis`) | REST close-out of the same session: `isViaRest()`, `getCloseoutReason()` = `DISCONNECTED` / `ACK_TIMEOUT` / `FINISH_TIMEOUT` |
| REST unreachable during close-out (`closeoutMillis`) | `FinishUnconfirmedException.getSessionId()`. Call `getSession` / `finishSession` later |
| HTTP 200 envelope with `code != 1` | `ApiException` subclasses with `getErrorCode()`, `isRetryable()`, `getRequestId()` |
| invalid key | `AuthenticationException` (not retried) |
| balance exhausted | `InsufficientCharactersException` (not retried) |
| residual half sample at finish | `AudioFormatException` (`discardPartialSample()` drops it) |
| interrupted blocking call | `CancellationException` |

Create is retried only on transport errors / retryable envelopes, with the same
`Idempotency-Key` and body, so a lost response never creates a second session.

## Network

`proxy(Proxy)` sets an explicit proxy (`Proxy.NO_PROXY` disables); otherwise the JVM
`ProxySelector` applies. TLS verification is always on; `sslSocketFactory(factory, trustManager)`
adds a private CA. `httpClient(OkHttpClient)` borrows your client, which the SDK never shuts
down. Client WebSocket pings every `pingIntervalMillis` (20 s) keep idle sockets open. They
are not audio and are unrelated to `heartbeatIntervalMs`. The access key travels only in the
`accessKey` header. The SDK does not log audio, transcript text, keys or tickets.

## Examples

`examples/src/main/java/example/`: `FileStream` (blocking), `ByteStream` (Future + Listener),
`ManualVad`, `PauseResume` (ticket helper included), `AbnormalFinish`; `examples/adapters.md`
(MP3/URL/microphone). `examples/pom.xml` is a standalone consumer of the installed artifact:

```bash
cd examples && mvn -q compile exec:java -Dexec.mainClass=example.FileStream -Dexec.args=speech.wav
```

## Development

```bash
mvn test       # A01–A14 against tools/fixture/server.py (FIXTURE_PYTHON selects the interpreter)
mvn package    # JAR + sources JAR (release 8 on JDK 9+, source/target 1.8 on JDK 8)
```
