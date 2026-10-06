# MyVocal STT Realtime SDK for Go

Server-side SDK for the MyVocal Speech to Text Realtime API. Configure a session, hand it
audio, consume events, call `Finish`. The SDK frames audio (~100 ms), keeps the sample
cursors, follows in-connection rotation and the end of the session. After an unexpected
disconnect it closes out the **original** session over REST. It never reconnects or resends
audio into a new task.

- Go 1.22+; dependency: `github.com/gorilla/websocket`
- Module: `github.com/MyVocal-AI/myvocal-stt-realtime-sdks/go` (package `sttrealtime`)

## Install

```bash
go get github.com/MyVocal-AI/myvocal-stt-realtime-sdks/go@v1.0.0
```

The module lives in the `go/` directory of the repository; its versions are tagged `go/vX.Y.Z`.

## Quick start

```go
import sttrealtime "github.com/MyVocal-AI/myvocal-stt-realtime-sdks/go"

client, err := sttrealtime.NewClient(sttrealtime.Config{AccessKey: os.Getenv("MYVOCAL_ACCESS_KEY")})
if err != nil { log.Fatal(err) }
defer client.Close()

ctx := context.Background()
session, err := client.Connect(ctx, sttrealtime.ConnectOptions{
    Request: &sttrealtime.SessionRequest{Title: "meeting", LanguageHint: "en",
        Options: &sttrealtime.SessionOptions{InputEncoding: "pcm_s16le_16000"}},
    Handlers: map[string]func(sttrealtime.Event){
        sttrealtime.EventTranscriptFinal: func(e sttrealtime.Event) { fmt.Println(e.Text()) },
    },
})
if err != nil { log.Fatal(err) }
defer session.Close()                       // local resources only, not a business finish

if err := session.SendFile(ctx, "speech.wav", true); err != nil { log.Fatal(err) } // paced
result, err := session.Finish(ctx)          // terminal server result
fmt.Println(result.Status, result.TranscriptionID, *result.Usage.BillableCharacters)
```

## API

| Item | Meaning |
|---|---|
| `SessionRequest{LanguageHint, Title, Options}` | create body. `SessionOptions` has the 16 public options; nil / `""` = omitted, `Bool(false)` / `Int(0)` are sent as given |
| `Client.CreateSession / GetSession / PauseSession / ResumeSession / FinishSession / CreateTicket` | REST helpers. `FinishSession` is idempotent. `CreateTicket` issues a browser ticket for your front end; the SDK never connects with it. Never log `Ticket.Value` |
| `Client.Connect(ctx, ConnectOptions)` | create (or attach via `SessionID`) + socket with the `accessKey` header + `session.ready` |
| `Session.SendAudio / SendAudioWithPrecedingText / SendReader / SendFile` | any chunk size. Half PCM samples carry across calls. Bounded buffer (`MaxBufferedMs`) blocks, never drops |
| `Session.Commit(ctx)` | close the current line, keep recording (same epoch) |
| `Session.Pause(ctx, ViaWS / ViaREST)`, `Resume` | flush + confirm, then close the epoch / open a new one. Input while paused returns `*SessionStateError` |
| `Session.Finish(ctx)` | flush the tail, wait for confirmation (incl. rotation), finish, return `*FinishResult`. Repeated calls return the same result |
| `Session.Close()` | release socket and goroutines. Idempotent. Not a finish |
| `Cursors()`, `Stats()`, `Transcript()`, `LatestUsage()` | local state |

Events: `EventTypes` lists the ten public events. Use `Handlers` (by type, `"*"` for all),
`OnEvent`, or `EventChannel` + `Events()`. Handlers run on a dispatcher goroutine, so a slow
handler never delays acknowledgements. A panic or a queue overflow is reported to
`OnCallbackError` and counted (`FinishResult.DroppedEvents`). The transcript snapshot
still has every line. `Line.EntitiesArrived` distinguishes "arrived empty" from "not
arrived". A missing supplement is reported by a `SUPPLEMENT_NOT_RECEIVED` notice.

## Results and errors

`FinishResult.Status` is the server's `COMPLETED` / `PARTIAL` / `FAILED`, unchanged.

| Situation | Result |
|---|---|
| socket drop / no acknowledgement progress (`Timeouts.Ack`) / no `session.completed` (`Timeouts.Finish`) | REST close-out of the same session: `ViaREST`, `CloseoutReason` = `DISCONNECTED` / `ACK_TIMEOUT` / `FINISH_TIMEOUT` |
| REST unreachable during close-out (`Timeouts.Closeout`) | `*FinishUnconfirmedError{SessionID}`. Call `GetSession` / `FinishSession` later |
| HTTP 200 envelope with `code != 1` | `*APIError` (`errors.Is(err, ErrInputInvalid / ErrAuthentication / ErrInsufficientCharacters / ...)`) |
| residual half sample at `Finish` | `*AudioFormatError` (`DiscardPartialSample()`) |

Create retries reuse the same `Idempotency-Key` and body. Auth, input and balance errors are
never retried.

## Network

`Proxy` (default `http.ProxyFromEnvironment`; `NoProxy`, `ProxyURL(...)`) applies to REST
and WebSocket. `TLSConfig` adds private trust; verification is always on. `HTTPClient` is
borrowed and never closed. Client pings every `PingInterval` (20 s) keep idle sockets open.
They are not audio, and they are unrelated to `HeartbeatIntervalMs`. The SDK writes no logs.
`String()` of results and events never prints keys, tickets or transcript text.

## Examples

`examples/file_stream`, `byte_stream`, `manual_vad`, `pause_resume` (ticket helper),
`abnormal_finish`, plus `examples/adapters.md`. Run with `MYVOCAL_ACCESS_KEY` (and optional
`MYVOCAL_API_BASE`).

## Development

`go test ./...` starts `../tools/fixture/server.py` (needs Python 3.10+, or `FIXTURE_PYTHON`).
