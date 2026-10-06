"""Manual segment commits versus server VAD.

    MYVOCAL_ACCESS_KEY=... python manual_vad.py speech.wav manual|vad
"""
import os
import sys

from myvocal_stt_realtime import RealtimeClient, SessionOptions, SessionRequest, read_wav_header


def main() -> None:
    path, mode = sys.argv[1], (sys.argv[2] if len(sys.argv) > 2 else "manual")
    with open(path, "rb") as fh:
        wav = read_wav_header(fh)
        fh.seek(wav.data_offset)
        pcm = fh.read(wav.data_size)
    options = SessionOptions(input_encoding=wav.encoding, segment_commit_mode=mode)
    if mode == "vad":
        options.commit_silence_ms = 600
    with RealtimeClient(os.environ["MYVOCAL_ACCESS_KEY"],
                        base_url=os.environ.get("MYVOCAL_API_BASE", "https://api.myvocal.ai")) as client:
        with client.connect(SessionRequest(title="manual/vad example", options=options),
                            handlers={"transcript.final": lambda e: print("final:", e.text)}) as session:
            half = len(pcm) // 2
            session.send_chunks([pcm[i:i + 4096] for i in range(0, half, 4096)], paced=True)
            if mode == "manual":
                session.commit()             # closes a line and keeps recording, same epoch
            session.send_chunks([pcm[i:i + 4096] for i in range(half, len(pcm), 4096)], paced=True)
            result = session.finish()
    print(mode, "->", result.status, len(result.lines), "lines |", result.text)


if __name__ == "__main__":
    main()
