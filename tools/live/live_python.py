#!/usr/bin/env python3
"""Real-API validation with the Python SDK (R1 all 7 encodings + R2 option combinations, R3
lifecycle, R4 disconnect close-out + 75 s idle with ping, R5 examples run from the wheel).

    MYVOCAL_ACCESS_KEY=... python tools/live/live_python.py --audio <dir from make_audio.ps1> --out <dir>

Re-running resumes: scenarios already recorded in <out>/live-python.json are skipped.
"""
import argparse
import asyncio
import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from live_common import error_record, ENCODINGS, Recorder, env, raw_path, samples_of, summarize  # noqa: E402

from myvocal_stt_realtime import (AsyncRealtimeClient, ConnectionLostError, RealtimeClient,  # noqa: E402
                                  SessionOptions, SessionRequest)

# R1 x R2: encoding -> (segment mode, options, label)
R1 = [
    ("pcm_s16le_8000", "manual", dict(include_alignment=True, include_detected_language=True,
                                      vocabulary_hints=["MyVocal", "Characters"]), "language+alignment+hints"),
    ("pcm_s16le_16000", "vad", dict(include_detected_language=True, include_alignment=False), "language only"),
    ("pcm_s16le_22050", "manual", dict(entity_categories=["pii"]), "entities"),
    ("pcm_s16le_24000", "vad", dict(remove_disfluencies=True, rewrite_instruction="Use sentence case."), "cleanup"),
    ("pcm_s16le_44100", "manual", dict(suppress_background_speech=True, include_alignment=False),
     "background filter, no alignment"),
    ("pcm_s16le_48000", "vad", dict(), "defaults"),
    ("mulaw_8000", "manual", dict(), "defaults"),
]


def chunks(data, start, end, size=3200):
    """Chunks of data[start:end], never crossing ``end``."""
    return [data[i:min(i + size, end)] for i in range(start, end, size)]


def read(path):
    with open(path, "rb") as f:
        return f.read()


def r1(client, rec, audio, enc, mode, extra, label):
    scen = "R1-%s-%s" % (enc, mode)
    if rec.done(scen):
        return
    data = read(raw_path(audio, enc))
    req = SessionRequest(title="sdk-live py " + scen, language_hint="en",
                         options=SessionOptions(input_encoding=enc, segment_commit_mode=mode, **extra))
    s = client.connect(req)
    try:
        bps = ENCODINGS[enc][1]
        if mode == "manual":
            half = len(data) // 2 // bps * bps
            s.send_chunks(chunks(data, 0, half, 3200), paced=True)
            s.commit()
            s.send_chunks(chunks(data, half, len(data), 3200), paced=True)
        else:
            s.send_chunks(chunks(data, 0, len(data), 3200), paced=True)
        r = s.finish()
    finally:
        s.close()
    lines = r.lines
    supp = {"alignmentArrived": any(l.alignment_arrived for l in lines),
            "languageCodes": sorted({l.language_code for l in lines if l.language_code}),
            "entitiesArrived": any(l.entities_arrived for l in lines),
            "entitiesCount": sum(len(l.entities or []) for l in lines),
            "revisionArrived": any(l.revision_arrived for l in lines)}
    exp = samples_of(audio, enc)
    out = summarize(r, scen, enc, exp, mode=mode, options=label, supplements=supp)
    out["ok"] = (r.usage.captured_samples == exp and r.usage.received_samples == exp and r.status == "COMPLETED"
                 and len(r.text) > 0)
    rec.add(out)


def r3_lifecycle(client, rec, audio):
    scen = "R3-lifecycle-commit-pause-resume-ticket-refinish"
    if rec.done(scen):
        return
    enc = "pcm_s16le_16000"
    data = read(raw_path(audio, enc))
    q = len(data) // 4 // 2 * 2
    s = client.connect(SessionRequest(title="sdk-live py " + scen, language_hint="en",
                                      options=SessionOptions(input_encoding=enc, segment_commit_mode="manual")))
    try:
        ticket = client.create_ticket(s.session_id)
        ticket_ok = bool(ticket.ticket) and ticket.session_id == s.session_id and bool(ticket.ticket_id)
        s.send_chunks(chunks(data, 0, q, 3200), paced=True)
        s.commit()
        s.send_chunks(chunks(data, q, 2 * q, 3200), paced=True)  # continue after commit
        s.pause()
        e1 = s.cursors.epoch
        time.sleep(2)
        s.resume(via="rest")
        e2 = s.cursors.epoch
        s.send_chunks(chunks(data, 2 * q, len(data), 3200), paced=True)
        r = s.finish()
        r2 = s.finish()
        view1 = client.get_session(s.session_id)
        again = client.finish_session(s.session_id)
        view2 = client.get_session(s.session_id)
    finally:
        s.close()
    exp = samples_of(audio, enc)
    out = summarize(r, scen, enc, exp, ticketIssued=ticket_ok, ticketExpiresInSeconds=ticket.expires_in_seconds,
                    epochs=[e1, e2], repeatSameObject=r is r2,
                    restRefinish={"status": again.status, "transcriptionId": again.transcription_id,
                                  "billableCharacters": again.usage.billable_characters},
                    getBefore=view1.usage.billable_characters, getAfter=view2.usage.billable_characters)
    out["ok"] = (ticket_ok and e2 == e1 + 1 and r is r2 and r.status == "COMPLETED"
                 and again.transcription_id == r.transcription_id
                 and again.usage.billable_characters == r.usage.billable_characters == view2.usage.billable_characters
                 and r.usage.captured_samples == exp)
    rec.add(out)


def r4_disconnect(client, rec, audio):
    scen = "R4-client-disconnect-closeout"
    if rec.done(scen):
        return
    enc = "pcm_s16le_16000"
    data = read(raw_path(audio, enc))
    half = len(data) // 2 // 2 * 2
    s = client.connect(SessionRequest(title="sdk-live py " + scen, language_hint="en",
                                      options=SessionOptions(input_encoding=enc, segment_commit_mode="manual")))
    lost = False
    try:
        s.send_chunks(chunks(data, 0, half, 3200), paced=True)
        s.commit()
        s.abort_connection()  # only this client's socket
        try:
            s.send_audio(data[half:])
        except ConnectionLostError:
            lost = True
        r = s.finish()
    finally:
        s.close()
    view = client.get_session(s.session_id)
    out = summarize(r, scen, enc, half // 2, audioRejectedAfterLoss=lost, getStatus=view.status)
    out["ok"] = lost and r.via_rest and r.status == view.status and r.status in ("COMPLETED", "PARTIAL", "FAILED")
    rec.add(out)


def r4_idle(client, rec, audio, idle_seconds):
    scen = "R4-idle-%ds-ping" % idle_seconds
    if rec.done(scen):
        return
    enc = "pcm_s16le_16000"
    data = read(raw_path(audio, enc))
    head = 16000 * 2  # 1 s
    s = client.connect(SessionRequest(title="sdk-live py " + scen, language_hint="en",
                                      options=SessionOptions(input_encoding=enc, segment_commit_mode="manual")))
    try:
        s.send_audio(data[:head])
        s.commit()
        before = s.cursors.captured_samples
        time.sleep(idle_seconds)  # no audio traffic; client pings every 20 s keep the socket
        during = s.cursors.captured_samples
        state = s.state
        s.send_chunks(chunks(data, head, len(data), 3200), paced=True)
        r = s.finish()
        stats = s.stats()
    finally:
        s.close()
    exp = samples_of(audio, enc)
    out = summarize(r, scen, enc, exp, idleSeconds=idle_seconds, capturedBeforeIdle=before, capturedAfterIdle=during,
                    stateAfterIdle=state, connections=stats.get("connections"))
    out["ok"] = (before == during and not r.via_rest and r.status == "COMPLETED" and r.usage.received_samples == exp
                 and state == "OPEN")
    rec.add(out)


def r5_examples(rec, audio):
    """Shipped examples run unchanged from the installed wheel (sync file stream, async byte stream)."""
    here = os.path.dirname(os.path.abspath(__file__))
    ex = os.path.join(here, "..", "..", "python", "examples")
    for scen, script, arg in [("R5-example-sync-file_stream", "file_stream.py", "speech_pcm_s16le_16000.wav"),
                              ("R5-example-async-byte_stream", "byte_stream_async.py", "speech_pcm_s16le_16000.raw")]:
        if rec.done(scen):
            continue
        p = subprocess.run([sys.executable, os.path.join(ex, script), os.path.join(audio, arg)],
                           capture_output=True, text=True, timeout=300, cwd=os.path.dirname(audio))
        m = re.search(r"status: (\w+) \| transcriptionId: (stt_\w+)", p.stdout)
        err_tail = (p.stderr.strip().splitlines() or [""])[-1][:200]
        out = {"scenario": scen, "exitCode": p.returncode, "status": m.group(1) if m else None, "stderrTail": err_tail,
               "transcriptionId": m.group(2) if m else None, "stdoutChars": len(p.stdout)}
        out["ok"] = p.returncode == 0 and bool(m) and m.group(1) == "COMPLETED"
        rec.add(out)


async def r5_async_main(rec, audio, key, base):
    scen = "R5-async-main-flow"
    if rec.done(scen):
        return
    enc = "mulaw_8000"
    data = read(raw_path(audio, enc))
    async with AsyncRealtimeClient(key, base_url=base) as client:
        s = await client.connect(SessionRequest(title="sdk-live py " + scen, language_hint="en",
                                                options=SessionOptions(input_encoding=enc, segment_commit_mode="vad")))
        try:
            await s.send_chunks(chunks(data, 0, len(data), 1000), paced=True)
            r = await s.finish()
        finally:
            await s.close()
    exp = samples_of(audio, enc)
    out = summarize(r, scen, enc, exp)
    out["ok"] = r.status == "COMPLETED" and r.usage.captured_samples == exp and len(r.text) > 0
    rec.add(out)


def d_segment(client, rec, audio, scen, mode, commit_every_s, heartbeat_ms=None, idle_s=0):
    """Diagnostics for real-API outcomes (not new acceptance items): one uncommitted 20 s manual
    segment vs. periodic commits, and the 75 s idle with the processing-side heartbeat option."""
    enc = "pcm_s16le_16000"
    data = read(raw_path(audio, enc))
    opts = dict(input_encoding=enc, segment_commit_mode=mode)
    if heartbeat_ms:
        opts["heartbeat_interval_ms"] = heartbeat_ms
    s = client.connect(SessionRequest(title="sdk-live py " + scen, language_hint="en", options=SessionOptions(**opts)))
    try:
        if idle_s:
            head = 16000 * 2
            s.send_audio(data[:head])
            s.commit()
            time.sleep(idle_s)
            start = head
        else:
            start = 0
        step = int(commit_every_s * 16000 * 2) if commit_every_s else len(data)
        pos = start
        while pos < len(data):
            end = min(pos + step, len(data))
            s.send_chunks(chunks(data, pos, end, 3200), paced=True)
            pos = end
            if commit_every_s and pos < len(data):
                s.commit()
        r = s.finish()
    finally:
        s.close()
    exp = samples_of(audio, enc)
    out = summarize(r, scen, enc, exp, mode=mode, commitEverySeconds=commit_every_s, heartbeatIntervalMs=heartbeat_ms,
                    idleSeconds=idle_s)
    out["ok"] = r.status == "COMPLETED" and r.usage.received_samples == exp
    rec.add(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--audio", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--idle", type=int, default=75)
    ap.add_argument("--only", default="r1,r3,r4,r5", help="comma list of r1,r3,r4,r5,diag")
    ap.add_argument("--redo", default="", help="regex of scenarios to run again (earlier attempt kept as superseded)")
    a = ap.parse_args()
    only = set(a.only.split(","))
    key, base = env()
    rec = Recorder(a.out, "python")
    rec.doc["sdk"] = "myvocal-stt-realtime (Python) %s" % sys.version.split()[0]
    if a.redo:
        rec.redo = re.compile(a.redo)
    def guard(name, fn, *args):
        if rec.done(name):
            return
        try:
            fn(*args)
        except Exception as exc:  # record and continue with the next scenario
            rec.add(error_record(name, exc))

    with RealtimeClient(key, base_url=base) as client:
        if "r1" in only:
            for enc, mode, extra, label in R1:
                guard("R1-%s-%s" % (enc, mode), r1, client, rec, a.audio, enc, mode, extra, label)
        if "r3" in only:
            guard("R3-lifecycle-commit-pause-resume-ticket-refinish", r3_lifecycle, client, rec, a.audio)
        if "r4" in only:
            guard("R4-client-disconnect-closeout", r4_disconnect, client, rec, a.audio)
            guard("R4-idle-%ds-ping" % a.idle, r4_idle, client, rec, a.audio, a.idle)
        if "diag" in only:
            for scen, mode, every, hb, idle in [("D1-manual-single-20s-segment", "manual", 0, None, 0),
                                                ("D2-manual-commit-every-5s", "manual", 5, None, 0),
                                                ("D3-idle-%ds-heartbeat-10000ms-commit-5s" % a.idle, "manual", 5, 10000, a.idle),
                                                ("D4-idle-%ds-no-heartbeat-commit-5s" % a.idle, "manual", 5, None, a.idle)]:
                guard(scen, d_segment, client, rec, a.audio, scen, mode, every, hb, idle)
    if "r5" in only:
        guard("R5-async-main-flow", lambda: asyncio.run(r5_async_main(rec, a.audio, key, base)))
        r5_examples(rec, a.audio)
    bad = [r["scenario"] for r in rec.doc["records"] if not r.get("ok")]
    print("records: %d, needing review: %s" % (len(rec.doc["records"]), bad or "none"))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
