# Optional audio adapters (not part of the module)

The SDK takes raw bytes in the session `InputEncoding`; the core module has no FFmpeg or
sound-card dependency.

## MP3 / any file or URL via FFmpeg

```go
cmd := exec.Command("ffmpeg", "-loglevel", "error", "-i", "input.mp3_or_https_url",
	"-f", "s16le", "-ac", "1", "-ar", "16000", "-")
out, _ := cmd.StdoutPipe()
_ = cmd.Start()
err := session.SendReader(ctx, out, true) // file: pace by sample time
_ = cmd.Wait()
result, err := session.Finish(ctx)
```

Use `paced=false` when the source already arrives at capture speed (for example FFmpeg `-re`
or a live capture pipe).

## Microphone

Use any capture library you trust (for example PortAudio bindings) to deliver 16-bit mono
PCM chunks, and call `session.SendAudio(ctx, chunk)` per chunk without extra pacing.
