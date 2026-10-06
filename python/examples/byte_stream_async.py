"""Async byte stream: audio arrives in arbitrary-size chunks at capture speed (here simulated from
a raw PCM file), events are consumed with ``async for``.

    MYVOCAL_ACCESS_KEY=... python byte_stream_async.py audio.raw [inputEncoding]
"""
import asyncio
import os
import sys

from myvocal_stt_realtime import AsyncRealtimeClient, AudioFormat, SessionOptions, SessionRequest


async def capture(path: str, fmt: AudioFormat):
    """Stands in for a live source: 37 ms chunks delivered at capture speed."""
    chunk = fmt.sample_rate_hz * 37 // 1000 * fmt.bytes_per_sample + 1  # odd size: half samples are fine
    with open(path, "rb") as fh:
        while True:
            data = fh.read(chunk)
            if not data:
                return
            yield data
            await asyncio.sleep(len(data) / fmt.bytes_per_sample / fmt.sample_rate_hz)


async def main() -> None:
    path = sys.argv[1]
    encoding = sys.argv[2] if len(sys.argv) > 2 else "pcm_s16le_16000"
    async with AsyncRealtimeClient(os.environ["MYVOCAL_ACCESS_KEY"],
                                   base_url=os.environ.get("MYVOCAL_API_BASE", "https://api.myvocal.ai")) as client:
        session = await client.connect(SessionRequest(title="byte-stream example",
                                                      options=SessionOptions(input_encoding=encoding,
                                                                              segment_commit_mode="vad")),
                                       iterate_events=True)

        async def printer():
            async for event in session.events():
                if event.type == "transcript.final":
                    print("final:", event.text)
                elif event.type == "session.notice":
                    print("notice:", event.notice.code)
        consumer = asyncio.ensure_future(printer())
        try:
            # live source: already at capture speed, so the SDK does not sleep again
            await session.send_chunks(capture(path, session.format))
            result = await session.finish()
        finally:
            await session.close()
            await consumer
    print("status:", result.status, "| transcriptionId:", result.transcription_id, "| text:", result.text)


if __name__ == "__main__":
    asyncio.run(main())
