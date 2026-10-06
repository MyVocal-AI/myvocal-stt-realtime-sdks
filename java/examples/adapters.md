# Optional audio adapters (not part of the JAR)

The SDK takes raw bytes in the session `inputEncoding`; the core JAR has no FFmpeg or
sound-card dependency. WAV headers are read by `WavInfo.read` / `session.sendFile(path.wav)`.

## MP3 / any file or URL via FFmpeg

```java
Process ffmpeg = new ProcessBuilder("ffmpeg", "-loglevel", "error", "-i", "input.mp3_or_https_url",
        "-f", "s16le", "-ac", "1", "-ar", "16000", "-").redirectError(ProcessBuilder.Redirect.INHERIT).start();
session.sendStream(ffmpeg.getInputStream(), true); // file: pace by sample time
ffmpeg.waitFor();
FinishResult result = session.finish();
```

Use `paced=false` when the source already arrives at capture speed (FFmpeg `-re`, a live pipe).

## Microphone (javax.sound.sampled)

```java
javax.sound.sampled.AudioFormat fmt = new javax.sound.sampled.AudioFormat(16000f, 16, 1, true, false);
TargetDataLine line = AudioSystem.getTargetDataLine(fmt);
line.open(fmt);
line.start();
byte[] buf = new byte[3200];
while (recording) {
    int n = line.read(buf, 0, buf.length);
    session.sendAudio(buf, 0, n, null); // already real-time: no extra pacing
}
```
