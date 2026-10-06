"""Lifecycle: create a session, issue a browser ticket (helper only), stream, pause, resume
(a new epoch), finish twice (same result), and read the session back over REST.

    MYVOCAL_ACCESS_KEY=... python pause_resume.py speech.wav
"""
import os
import sys
import time

from myvocal_stt_realtime import RealtimeClient, SessionOptions, SessionRequest, read_wav_header


def main() -> None:
    path = sys.argv[1]
    with open(path, "rb") as fh:
        wav = read_wav_header(fh)
        fh.seek(wav.data_offset)
        pcm = fh.read(wav.data_size)
    with RealtimeClient(os.environ["MYVOCAL_ACCESS_KEY"],
                        base_url=os.environ.get("MYVOCAL_API_BASE", "https://api.myvocal.ai")) as client:
        session = client.connect(SessionRequest(title="pause/resume example",
                                                options=SessionOptions(input_encoding=wav.encoding)))
        try:
            # Ticket helper: hand ticket.ticket to YOUR browser front end; never log it.
            ticket = client.create_ticket(session.session_id)
            print("ticket issued:", ticket.ticket_id, "expires in", ticket.expires_in_seconds, "s")

            third = len(pcm) // 3 // 2 * 2
            session.send_chunks([pcm[:third]], paced=True)
            session.pause()                   # flushes and confirms first; nothing is sent while paused
            print("paused at epoch", session.cursors.epoch)
            time.sleep(1.0)                   # paused time is never sent as audio
            session.resume()                  # or session.resume(via="rest")
            print("resumed at epoch", session.cursors.epoch)
            session.send_chunks([pcm[third:]], paced=True)
            result = session.finish()
            again = session.finish()          # same object, no second settlement
            assert again is result
        finally:
            session.close()
        view = client.get_session(result.session_id)
    print("status:", result.status, "/ GET:", view.status, "| epochs:", view.epoch,
          "| billable:", result.usage.billable_characters, "| text:", result.text)


if __name__ == "__main__":
    main()
