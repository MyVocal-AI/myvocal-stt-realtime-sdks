# Optional audio adapters (not part of the package)

The SDK takes raw bytes in the session `inputEncoding`. The core package has no FFmpeg or
sound-card dependency. These snippets show how to feed other sources.

## MP3 / any file or URL via FFmpeg

```python
import subprocess
proc = subprocess.Popen(["ffmpeg", "-loglevel", "error", "-i", "input.mp3_or_https_url",
                         "-f", "s16le", "-ac", "1", "-ar", "16000", "-"], stdout=subprocess.PIPE)
session.send_chunks(iter(lambda: proc.stdout.read(3200), b""), paced=True)   # file: pace by sample time
result = session.finish()
```

Use `paced=False` when the source already delivers audio at capture speed (for example an
FFmpeg `-re` live input).

## Microphone (third-party `sounddevice`, install it yourself)

```python
import queue, sounddevice as sd
q = queue.Queue()
with sd.RawInputStream(samplerate=16000, channels=1, dtype="int16", blocksize=1600,
                       callback=lambda data, frames, t, status: q.put(bytes(data))):
    while recording:
        session.send_audio(q.get())          # live: no extra pacing
result = session.finish()
```
