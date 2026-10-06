"""Abnormal finish: the socket drops mid-stream. The SDK stops taking audio, queries the ORIGINAL
session and finishes it over REST (no reconnect, no resend, no new session). If the end cannot be
confirmed, keep the session id and finish it later explicitly.

    MYVOCAL_ACCESS_KEY=... python abnormal_finish.py speech.wav
"""
import os
import sys

from myvocal_stt_realtime import (ConnectionLostError, FinishUnconfirmedError, RealtimeClient, SessionOptions,
                                  SessionRequest, read_wav_header)


def main() -> None:
    path = sys.argv[1]
    with open(path, "rb") as fh:
        wav = read_wav_header(fh)
        fh.seek(wav.data_offset)
        pcm = fh.read(wav.data_size)
    with RealtimeClient(os.environ["MYVOCAL_ACCESS_KEY"],
                        base_url=os.environ.get("MYVOCAL_API_BASE", "https://api.myvocal.ai")) as client:
        session = client.connect(SessionRequest(title="abnormal-finish example",
                                                options=SessionOptions(input_encoding=wav.encoding)))
        session_id = session.session_id
        try:
            half = len(pcm) // 2 // 2 * 2
            session.send_chunks([pcm[:half]], paced=True)
            session.commit()
            session.abort_connection()        # simulates a network failure on this client only
            try:
                session.send_audio(pcm[half:])
            except ConnectionLostError:
                print("socket lost; audio is no longer accepted")
            result = session.finish()         # REST close-out of the same session
            print("closed out:", result.status, "via REST:", result.via_rest, "reason:", result.closeout_reason)
        except FinishUnconfirmedError as err:
            print("end not confirmed yet for", err.session_id, "- finish it later:")
            view = client.get_session(session_id)
            if not view.is_terminal:
                view = client.finish_session(session_id)
            print("explicit finish:", view.status)
        finally:
            session.close()


if __name__ == "__main__":
    main()
