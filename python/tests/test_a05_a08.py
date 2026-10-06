"""A05 (pacing/backpressure/concurrency), A06 (lifecycle), A07 (rotation), A08 (events/supplements)."""
import asyncio
import os
import time
from unittest.mock import patch

import pytest

from conftest import AUDIO, KEY, audio, scenario, sha
from myvocal_stt_realtime import (AsyncRealtimeClient, EVENT_TYPES, RealtimeClient, SessionOptions,
                                  SessionRequest, SessionStateError)


def req(title, encoding="pcm_s16le_16000", **opts):
    return SessionRequest(title=title, options=SessionOptions(input_encoding=encoding, **opts))


def client(fx, **kw):
    return AsyncRealtimeClient(KEY, base_url=fx.base, trust_env=False, **kw)


# ---------------------------------------------------------------- A05
async def test_a05_file_is_paced_by_sample_time_live_is_not(fx):
    path = os.path.join(AUDIO, "tone_pcm_s16le_16000.raw")
    async with client(fx) as c:
        s = await c.connect(req("A05-pacing"))
        t0 = time.monotonic()
        await s.send_file(path, paced=True)
        paced = time.monotonic() - t0
        await s.finish()
        await s.close()
        frames = fx.record(s.session_id)["frames"]
        s2 = await c.connect(req("A05-pacing"))
        t0 = time.monotonic()
        await s2.send_audio(audio("pcm_s16le_16000"))
        live = time.monotonic() - t0
        await s2.finish()
        await s2.close()
    assert paced >= 1.1, paced  # 1.25 s of audio
    span = frames[-1]["t"] - frames[0]["t"]
    assert span >= 1.0, span
    assert live < 0.6, live
    assert fx.record(s.session_id)["storedSha256"] == fx.record(s2.session_id)["storedSha256"]


async def test_a05_bounded_buffer_backpressure(fx):
    data = audio("pcm_s16le_16000")
    async with client(fx, max_buffered_ms=300) as c:
        s = await c.connect(req("A05-backpressure"))
        for i in range(0, len(data), 5000):
            await s.send_audio(data[i:i + 5000])
        stats = s.stats()
        r = await s.finish()
        await s.close()
    assert stats["peak_buffered_samples"] <= stats["max_buffered_samples"] == 4800
    assert fx.record(s.session_id)["storedSha256"] == sha(data) and r.status == "COMPLETED"
    assert s.stats()["buffered_samples"] == 0  # released after confirmation


class _TrackingFile:
    """Wraps a real file and records how far reading runs ahead of what the session captured."""

    def __init__(self, fh, captured_bytes):
        self._fh, self._captured_bytes = fh, captured_bytes
        self.read_bytes = 0
        self.max_lead = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._fh.close()

    def seek(self, *a):
        return self._fh.seek(*a)

    def tell(self):
        return self._fh.tell()

    def read(self, n=-1):
        self.max_lead = max(self.max_lead, self.read_bytes - self._captured_bytes())
        chunk = self._fh.read(n)
        self.read_bytes += len(chunk)
        return chunk


def _big_raw(tmp_path, repeats):
    data = audio("pcm_s16le_16000") * repeats  # 40000 bytes each
    path = tmp_path / "big.raw"
    path.write_bytes(data)
    return str(path), data


def _track_open(captured_bytes, holder):
    real_open = open

    def fake(path, mode="r", *a, **kw):
        holder.append(_TrackingFile(real_open(path, mode, *a, **kw), captured_bytes))
        return holder[-1]
    return patch("myvocal_stt_realtime._session.open", create=True, new=fake)


async def test_a05_send_file_reads_incrementally(fx, tmp_path):
    path, data = _big_raw(tmp_path, 50)  # 2,000,000 bytes = 62.5 s of audio, far above the 300 ms buffer
    holder = []
    async with client(fx, max_buffered_ms=300) as c:
        s = await c.connect(req("L-basic"))
        with _track_open(lambda: s.cursors.captured_samples * 2, holder):
            await s.send_file(path, paced=False)
        stats = s.stats()
        r = await s.finish()
        await s.close()
    tracked = holder[0]
    assert tracked.read_bytes == len(data)
    assert tracked.max_lead <= 3200, tracked.max_lead  # never more than one 100 ms frame read ahead
    assert stats["peak_buffered_samples"] <= stats["max_buffered_samples"] == 4800
    assert fx.record(s.session_id)["storedSha256"] == sha(data) and r.status == "COMPLETED"


def test_a05_sync_send_file_reads_incrementally(fx, tmp_path):
    path, data = _big_raw(tmp_path, 15)
    holder = []
    with RealtimeClient(KEY, base_url=fx.base, trust_env=False, max_buffered_ms=300) as c:
        with c.connect(req("L-basic")) as s:
            with _track_open(lambda: s.cursors.captured_samples * 2, holder):
                s.send_file(path, paced=False)
            r = s.finish()
    assert holder[0].read_bytes == len(data) and holder[0].max_lead <= 3200, holder[0].max_lead
    assert fx.record(s.session_id)["storedSha256"] == sha(data) and r.status == "COMPLETED"


async def test_a05_two_sessions_are_isolated(fx):
    async with client(fx) as c:
        a = await c.connect(req("A05-concurrent"))
        b = await c.connect(req("A05-concurrent", "mulaw_8000"))
        da, db = audio("pcm_s16le_16000"), audio("mulaw_8000")

        async def run(s, d):
            for i in range(0, len(d), 999):
                await s.send_audio(d[i:i + 999])
            return await s.finish()
        ra, rb = await asyncio.gather(run(a, da), run(b, db))
        assert a.cursors.captured_samples == 20000 and b.cursors.captured_samples == 10000
        await a.close()
        await b.close()
    assert ra.session_id != rb.session_id and ra.transcription_id != rb.transcription_id
    assert fx.record(ra.session_id)["storedSha256"] == sha(da)
    assert fx.record(rb.session_id)["storedSha256"] == sha(db)


# ---------------------------------------------------------------- A06
async def test_a06_manual_commit_pause_resume_finish_ws(fx):
    await _lifecycle(fx, "A06-manual", "ws")


async def test_a06_manual_lifecycle_rest_control(fx):
    await _lifecycle(fx, "A06-rest-control", "rest")


async def _lifecycle(fx, title, via):
    data = audio("pcm_s16le_16000")
    q = len(data) // 4 // 2 * 2
    async with client(fx) as c:
        s = await c.connect(req(title))
        await s.send_audio(data[:q])
        await s.commit()
        assert s.cursors.epoch == 1
        await s.send_audio(data[q:2 * q])
        await s.pause(via=via)
        with pytest.raises(SessionStateError):
            await s.send_audio(data[2 * q:3 * q])  # paused: input is refused, never counted
        await s.resume(via=via)
        assert s.cursors.epoch == 2 and s.cursors.sample_offset == 0
        await s.send_audio(data[2 * q:])
        r = await s.finish()
        r2 = await s.finish()
        view = await c.get_session(s.session_id)
        rest = await c.finish_session(s.session_id)
        await s.close()
    rec = fx.record(s.session_id)
    assert r is r2
    assert rec["storedSha256"] == sha(data) and len(rec["epochs"]) == 2
    assert rec["epochs"][1]["sent"] == (len(data) - 2 * q) // 2
    assert rec["captured"] == len(data) // 2
    assert r.status == view.status == rest.status == "COMPLETED"
    assert r.transcription_id == view.transcription_id == rest.transcription_id
    assert r.usage.billable_characters == view.usage.billable_characters == rest.usage.billable_characters
    assert rec["finishCount"] == 2  # one socket finish + one REST finish; settled once
    assert rec["settled"] == r.usage.billable_characters
    assert len(r.lines) == 3 and [l.id for l in r.lines] == ["line_1", "line_2", "line_3"]


async def test_a06_vad(fx):
    data = audio("pcm_s16le_16000")
    async with client(fx) as c:
        s = await c.connect(req("A06-vad", segment_commit_mode="vad"))
        await s.send_audio(data)
        r = await s.finish()
        await s.close()
    assert r.status == "COMPLETED" and len(r.lines) >= 3
    assert fx.record(s.session_id)["effectiveOptions"]["segmentCommitMode"] == "vad"


# ---------------------------------------------------------------- A07
@pytest.mark.parametrize("sid,chunk", [("A07-rotate-mid", None), ("A07-rotate-last", None),
                                       ("A07-rotate-tail", None), ("A07-rotate-ack-delay", 777),
                                       ("A07-rotate-twice", 1600), ("A07-old-epoch-late", None)])
async def test_a07_rotation(fx, sid, chunk):
    exp = scenario(sid)["expect"]
    data = audio("pcm_s16le_16000", exp.get("inputSeconds"))
    async with client(fx) as c:
        s = await c.connect(req(sid))
        if chunk:
            for i in range(0, len(data), chunk):
                await s.send_audio(data[i:i + chunk])
        else:
            await s.send_audio(data)
        r = await s.finish()
        await s.close()
    rec = fx.record(s.session_id)
    total = len(data) // 2
    assert rec["storedSha256"] == sha(data), sid
    assert len(rec["epochs"]) == exp["epochs"] and sum(e["sent"] for e in rec["epochs"]) == total
    assert max(rec["capturedClaims"]) == total and rec["captured"] == total  # never counted twice
    assert r.status == "COMPLETED" and s.cursors.epoch == exp["epochs"]
    assert s.stats()["rotations"] == exp["epochs"] - 1


# ---------------------------------------------------------------- A08
async def test_a08_all_public_events_and_supplements(fx):
    events, errors = [], []
    async with client(fx) as c:
        s = await c.connect(req("A08-events-all"), on_event=events.append,
                            on_callback_error=lambda e, ev: errors.append(e))
        data = audio("pcm_s16le_16000")
        await s.send_audio(data[:16000])
        await s.commit()
        await s.send_audio(data[16000:])
        r = await s.finish()
        await s.close()
    types = {e.type for e in events}
    assert types == set(scenario("A08-events-all")["expect"]["events"]) == set(EVENT_TYPES)
    assert [l.id for l in r.lines] == ["line_1", "line_2"]  # supplements never open a new line
    l1 = r.lines[0]
    assert l1.alignment_arrived and l1.language_code == "en" and l1.words and l1.words[0].start_ms == 0
    assert l1.entities_arrived and l1.entities == []  # arrived empty
    assert l1.revision_arrived and l1.revised_text == "HELLO WORLD"
    codes = [n.code for n in r.notices]
    assert "SUPPLEMENT_NOT_RECEIVED" in codes and "PROCESSING_STORAGE_NOT_APPLIED" in codes
    missing = next(n for n in r.notices if n.code == "SUPPLEMENT_NOT_RECEIVED")
    assert missing.supplements == ["language"] and missing.line_ids == ["line_1"]
    assert r.status == "COMPLETED"  # a missing supplement never rewrites the server status
    assert "TEMPORARILY_UNAVAILABLE" in r.server_errors and not errors
    seqs = [e.sequence for e in events]
    assert seqs == sorted(seqs) and seqs[0] == 1


async def test_a08_entities_not_arrived_vs_arrived(fx):
    async with client(fx) as c:
        s = await c.connect(req("L-basic"))
        await s.send_audio(audio("pcm_s16le_16000"))
        r = await s.finish()
        await s.close()
        s2 = await c.connect(req("A08-entities-some"))
        await s2.send_audio(audio("pcm_s16le_16000"))
        r2 = await s2.finish()
        await s2.close()
    assert r.lines[0].entities is None and not r.lines[0].entities_arrived
    assert r2.lines[0].entities_arrived and r2.lines[0].entities[0]["text"] == "Alice"


async def test_a08_transport_close_after_terminal_is_not_a_failure(fx):
    async with client(fx) as c:
        s = await c.connect(req("A08-close-after-completed"))
        await s.send_audio(audio("pcm_s16le_16000"))
        r = await s.finish()
        await asyncio.sleep(0.2)
        await s.close()
    assert r.status == "COMPLETED" and not r.via_rest and r.closeout_reason is None
    assert fx.record(s.session_id)["restFinishCount"] == 0
