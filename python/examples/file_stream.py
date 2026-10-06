"""Stream a WAV (16-bit PCM mono, or 8 kHz mu-law) file at real-time pace and print the transcript.

    MYVOCAL_ACCESS_KEY=... python file_stream.py speech.wav [languageHint]
"""
import os
import sys

from myvocal_stt_realtime import RealtimeClient, SessionOptions, SessionRequest, read_wav_header


def main() -> None:
    path = sys.argv[1]
    with open(path, "rb") as fh:
        wav = read_wav_header(fh)  # picks the matching inputEncoding, e.g. pcm_s16le_16000
    request = SessionRequest(title="file-stream example", language_hint=sys.argv[2] if len(sys.argv) > 2 else None,
                             options=SessionOptions(input_encoding=wav.encoding, segment_commit_mode="vad"))
    with RealtimeClient(os.environ["MYVOCAL_ACCESS_KEY"],
                        base_url=os.environ.get("MYVOCAL_API_BASE", "https://api.myvocal.ai")) as client:
        handlers = {"transcript.delta": lambda e: print("  ...", e.text),
                    "transcript.final": lambda e: print("final:", e.text)}
        session = client.connect(request, handlers=handlers)
        try:
            session.send_file(path)          # paced by sample time; the SDK frames ~100 ms
            result = session.finish()        # flush, wait for acknowledgements, terminal result
        finally:
            session.close()                  # local resources only
    print("status:", result.status, "| transcriptionId:", result.transcription_id)
    print("billableCharacters:", result.usage.billable_characters)
    print("text:", result.text)


if __name__ == "__main__":
    main()
