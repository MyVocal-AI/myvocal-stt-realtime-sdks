"""A01 (lifecycle, sync+async), A02 (REST/identity), A03 (parameters), A04 (encodings/framing)."""
import random

import httpx
import pytest

from conftest import KEY, audio, scenario, sha
from myvocal_stt_realtime import (ApiError, AsyncRealtimeClient, AudioFormatError, AuthenticationError,
                                  InputInvalidError, InsufficientCharactersError, RealtimeClient, SessionOptions,
                                  SessionRequest, Timeouts, ENCODINGS)


def req(title, encoding="pcm_s16le_16000", **opts):
    return SessionRequest(title=title, options=SessionOptions(input_encoding=encoding, **opts))


def client(fx, **kw):
    return AsyncRealtimeClient(KEY, base_url=fx.base, trust_env=False, **kw)


# ---------------------------------------------------------------- A01
async def test_a01_async_connect_audio_finish(fx):
    async with client(fx) as c:
        s = await c.connect(req("L-basic"))
        data = audio("pcm_s16le_16000")
        await s.send_audio(data)
        r = await s.finish()
        await s.close()
    rec = fx.record(r.session_id)
    assert r.status == "COMPLETED" and r.transcription_id == rec["transcriptionId"]
    assert rec["storedSha256"] == sha(data)


def test_a01_sync_connect_audio_finish(fx):
    seen = []
    with RealtimeClient(KEY, base_url=fx.base, trust_env=False) as c:
        with c.connect(req("L-basic"), handlers={"*": lambda e: seen.append(e.type)}) as s:
            data = audio("pcm_s16le_16000")
            s.send_audio(data[:3333])
            s.send_audio(data[3333:])
            r = s.finish()
    rec = fx.record(r.session_id)
    assert r.status == "COMPLETED" and rec["storedSha256"] == sha(data)
    assert "session.ready" in seen and "session.completed" in seen


def test_a01_sync_event_iterator(fx):
    with RealtimeClient(KEY, base_url=fx.base, trust_env=False) as c:
        s = c.connect(req("L-basic"), iterate_events=True)
        s.send_audio(audio("pcm_s16le_16000"))
        r = s.finish()
        types = [e.type for e in s.events(timeout=2)]
        s.close()
    assert types[0] == "session.ready" and types[-1] == "session.completed" and r.status == "COMPLETED"


async def test_a01_async_event_iterator(fx):
    async with client(fx) as c:
        s = await c.connect(req("L-basic"), iterate_events=True)
        await s.send_audio(audio("pcm_s16le_16000"))
        r = await s.finish()
        await s.close()
        types = [e.type async for e in s.events()]
    assert types[0] == "session.ready" and "session.completed" in types and r.status == "COMPLETED"


# ---------------------------------------------------------------- A02
async def test_a02_create_response_lost_reuses_key_and_body(fx):
    async with client(fx) as c:
        s = await c.connect(req("A02-create-lost"))
        await s.send_audio(audio("pcm_s16le_16000", 0.3))
        r = await s.finish()
        await s.close()
    sessions = fx.by_title("A02-create-lost")
    assert len(sessions) == 1
    creates = [x for x in sessions[0]["restCalls"] if x["path"].startswith("create")]
    assert [x["path"] for x in creates] == ["create", "create(replay)"]
    assert creates[0]["idempotencyKey"] == creates[1]["idempotencyKey"]
    assert r.session_id == sessions[0]["sessionId"]


async def test_a02_envelope_error_http200_nonretryable(fx):
    count = {"n": 0}

    async def hook(request):
        count["n"] += 1
    async with httpx.AsyncClient(event_hooks={"request": [hook]}, trust_env=False) as http:
        c = AsyncRealtimeClient(KEY, base_url=fx.base, http_client=http)
        with pytest.raises(InsufficientCharactersError) as err:
            await c.create_session(req("A02-envelope-nonretryable"))
        await c.aclose()
        assert not http.is_closed  # borrowed client is never closed
    assert err.value.code == 47210 and err.value.error_code == "INSUFFICIENT_CHARACTERS" and not err.value.retryable
    assert count["n"] == 1


async def test_a02_envelope_error_retryable_is_bounded(fx):
    count = {"n": 0}

    async def hook(request):
        count["n"] += 1
    async with httpx.AsyncClient(event_hooks={"request": [hook]}, trust_env=False) as http:
        c = AsyncRealtimeClient(KEY, base_url=fx.base, http_client=http, max_create_retries=2)
        with pytest.raises(ApiError) as err:
            await c.create_session(req("A02-envelope-retryable"))
    assert err.value.error_code == "TEMPORARILY_UNAVAILABLE" and err.value.retryable
    assert count["n"] == 3


async def test_a02_invalid_key_not_retried(fx):
    count = {"n": 0}

    async def hook(request):
        count["n"] += 1
    async with httpx.AsyncClient(event_hooks={"request": [hook]}, trust_env=False) as http:
        c = AsyncRealtimeClient("wrong-key", base_url=fx.base, http_client=http)
        with pytest.raises(AuthenticationError) as err:
            await c.connect(req("L-basic"))
    assert err.value.code == 401 and count["n"] == 1
    assert "wrong-key" not in str(err.value) and "wrong-key" not in repr(c)


async def test_a02_rest_helpers_and_identity(fx):
    async with client(fx) as c:
        view = await c.create_session(SessionRequest(language_hint="en", title="A02-rest-helpers",
                                                     options=SessionOptions(input_encoding="mulaw_8000")))
        assert view.session_id.startswith("rts_") and view.transcription_id.startswith("stt_")
        assert view.effective_options["inputEncoding"] == "mulaw_8000" and view.effective_options["sampleRateHz"] == 8000
        got = await c.get_session(view.session_id)
        assert got.transcription_id == view.transcription_id and got.status == "RECORDING"
        ticket = await c.create_ticket(view.session_id)
        assert ticket.ticket and ticket.session_id == view.session_id and ticket.expires_in_seconds == 60
        assert ticket.ticket not in repr(ticket)
        paused = await c.pause_session(view.session_id, 0)
        assert paused.accepting is False and paused.status == "PAUSED"
        resumed = await c.resume_session(view.session_id, 0)
        assert resumed.epoch == 2 and resumed.accepting is True
        done = await c.finish_session(view.session_id, 0)
        assert done.status == "FAILED"  # no audio: the server's real outcome is returned as-is
        again = await c.finish_session(view.session_id, 0)
        assert again.transcription_id == done.transcription_id and again.status == done.status
    rec = fx.record(view.session_id)
    assert rec["rawRequest"] == {"languageHint": "en", "title": "A02-rest-helpers", "options": {"inputEncoding": "mulaw_8000"}}
    for call in rec["restCalls"]:
        assert call["hasAccessKeyHeader"] and not call.get("queryHasKey")


async def test_a02_socket_identity_header_not_url(fx):
    async with client(fx) as c:
        s = await c.connect(req("L-basic"))
        await s.send_audio(audio("pcm_s16le_16000", 0.2))
        await s.finish()
        await s.close()
    conn = fx.record(s.session_id)["connections"][0]
    assert conn["hasAccessKeyHeader"] and not conn["queryHasKey"] and conn["query"] == ""
    assert conn["userAgent"].startswith("myvocal-stt-realtime-python/")


# ---------------------------------------------------------------- A03
@pytest.mark.parametrize("sid", ["A03-params-full-a", "A03-params-full-b", "A03-falsy"])
async def test_a03_parameters_reach_wire_exactly(fx, sid):
    body = scenario(sid)["request"]
    r = SessionRequest(language_hint=body.get("languageHint"), title=body["title"],
                       options=SessionOptions.from_wire(body["options"]))
    async with client(fx) as c:
        view = await c.create_session(r)
        await c.finish_session(view.session_id)
    rec = fx.record(view.session_id)
    assert rec["rawRequest"] == body
    for k, v in body["options"].items():
        if k != "inputEncoding":
            assert rec["effectiveOptions"][k] == v or (isinstance(v, str) and rec["effectiveOptions"][k] == v.strip())


@pytest.mark.parametrize("sid", ["A03-illegal-rewrite-entities", "A03-illegal-suppress-alignment", "A03-illegal-vocab-51"])
async def test_a03_illegal_combinations_are_input_errors_not_retried(fx, sid):
    body = scenario(sid)["request"]
    opts = dict(body["options"])
    if opts.get("vocabularyHints") == "51x":
        opts["vocabularyHints"] = ["term%d" % i for i in range(51)]
    count = {"n": 0}

    async def hook(request):
        count["n"] += 1
    async with httpx.AsyncClient(event_hooks={"request": [hook]}, trust_env=False) as http:
        c = AsyncRealtimeClient(KEY, base_url=fx.base, http_client=http)
        with pytest.raises(InputInvalidError):
            await c.connect(SessionRequest(title=sid, options=SessionOptions.from_wire(opts)))
    assert count["n"] == 1 and fx.by_title(sid) == []


async def test_a03_preceding_text_on_audio_event(fx):
    async with client(fx) as c:
        s = await c.connect(req("A03-preceding-text"))
        await s.send_audio(audio("pcm_s16le_16000", 0.2), preceding_text="Earlier context.")
        await s.finish()
        await s.close()
    frames = fx.record(s.session_id)["frames"]
    assert frames[0]["precedingText"] == "Earlier context." and frames[1]["precedingText"] is None


def test_a03_options_omitted_vs_false_and_zero():
    o = SessionOptions(include_alignment=False, minimum_speech_ms=0, speech_detection_threshold=0.0)
    assert o.to_wire() == {"includeAlignment": False, "minimumSpeechMs": 0, "speechDetectionThreshold": 0.0}
    assert SessionOptions().to_wire() == {}


# ---------------------------------------------------------------- A04
def chunkings(data, bps):
    rnd = random.Random(20261006)
    yield "whole", [data]
    yield "128-sample", [data[i:i + 128 * bps] for i in range(0, len(data), 128 * bps)]
    irregular, i = [], 0
    while i < len(data):
        n = rnd.choice([1, 3, 7, 77, 255, 1001, 4097])
        irregular.append(data[i:i + n])
        i += n
    yield "irregular", irregular


@pytest.mark.parametrize("encoding", list(ENCODINGS))
async def test_a04_encodings_and_chunkings(fx, encoding):
    data = audio(encoding)
    rate, bps = ENCODINGS[encoding]
    async with client(fx) as c:
        for name, chunks in chunkings(data, bps):
            s = await c.connect(req("A04-encoding", encoding))
            for ch in chunks:
                await s.send_audio(ch)
            r = await s.finish()
            await s.close()
            rec = fx.record(s.session_id)
            assert rec["storedSha256"] == sha(data), (encoding, name)
            assert rec["storedSamples"] == len(data) // bps
            assert rec["captured"] == len(data) // bps and max(rec["capturedClaims"]) == len(data) // bps
            sizes = [f["bytes"] for f in rec["frames"]]
            frame = rate // 10 * bps
            assert all(sz == frame for sz in sizes[:-1]) and 0 < sizes[-1] <= frame, (encoding, name, sizes)
            assert r.status == "COMPLETED"


async def test_a04_residual_half_sample_is_an_input_error(fx):
    data = audio("pcm_s16le_16000", 0.3)
    async with client(fx) as c:
        s = await c.connect(req("A04-encoding"))
        await s.send_audio(data + b"\x01")
        with pytest.raises(AudioFormatError):
            await s.finish()
        assert s.discard_partial_sample() is True
        r = await s.finish()
        await s.close()
    rec = fx.record(s.session_id)
    assert rec["storedSha256"] == sha(data) and r.status == "COMPLETED"



async def test_a04_wav_header_and_mismatch(fx):
    import os
    from conftest import AUDIO
    wav = os.path.join(AUDIO, "tone_pcm_s16le_16000.wav")
    async with client(fx) as c:
        s = await c.connect(req("A04-encoding", "pcm_s16le_8000"))
        with pytest.raises(AudioFormatError):
            await s.send_file(wav, paced=False)
        await s.finish()
        await s.close()
        s = await c.connect(req("A04-encoding", "pcm_s16le_16000"))
        await s.send_file(wav, paced=False)
        await s.finish()
        await s.close()
    assert fx.record(s.session_id)["storedSha256"] == sha(audio("pcm_s16le_16000"))
