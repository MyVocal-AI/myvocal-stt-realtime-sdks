"""A09 (64-bit/null/sequence), A10 (disconnect close-out), A11 (timeouts/errors),
A12 (callbacks/release/network), A13 (no secrets/audio/text in public surfaces)."""
import asyncio
import io
import logging
import ssl
import threading
import time

import httpx
import pytest

from conftest import CA, KEY, audio, scenario, sha
from myvocal_stt_realtime import (AsyncRealtimeClient, FinishUnconfirmedError, InsufficientCharactersError,
                                  RealtimeClient, SessionOptions, SessionRequest, Timeouts, TransportError)


def req(title, encoding="pcm_s16le_16000", **opts):
    return SessionRequest(title=title, options=SessionOptions(input_encoding=encoding, **opts))


def client(fx, **kw):
    kw.setdefault("trust_env", False)
    return AsyncRealtimeClient(KEY, base_url=fx.base, **kw)


SHORT = Timeouts(request=3, create=3, connect=3, ack=1.0, finish=1.5, closeout=4, close=2)


# ---------------------------------------------------------------- A09
async def test_a09_int64_values_null_times_and_wire_types(fx):
    big = scenario("A09-bigints")["expect"]["bigOffset"]
    async with client(fx) as c:
        s = await c.connect(req("A09-bigints"))
        assert s.cursors.received_samples == big and s.cursors.captured_samples == big
        await s.send_audio(audio("pcm_s16le_16000"))
        r = await s.finish()
        await s.close()
    rec = fx.record(s.session_id)
    assert r.usage.elapsed_ms == 9223372036854775807 and isinstance(r.usage.elapsed_ms, int)
    assert r.usage.received_samples == big + 20000
    assert max(rec["capturedClaims"]) == big + 20000  # > 2^53, exact on the wire
    assert all(f["sampleOffsetType"] == "str" and f["capturedType"] == "str" for f in rec["frames"])
    words = r.lines[0].words
    assert words[0].start_ms >= big and words[-1].start_ms is None and words[-1].end_ms is None


async def test_a09_sequence_is_per_connection(fx):
    seqs = {1: [], 2: []}
    async with client(fx) as c:
        s = await c.connect(req("L-basic"), on_event=lambda e: seqs[1].append(e.sequence))
        await s.send_audio(audio("pcm_s16le_16000", 0.5))
        await s.commit()
        await s.close()
        await asyncio.sleep(0.2)
        s2 = await c.connect(session_id=s.session_id, on_event=lambda e: seqs[2].append(e.sequence))
        assert s2.cursors.captured_samples == 8000  # cursors restored from the server view
        await s2.resume()
        await s2.send_audio(audio("pcm_s16le_16000")[16000:])
        r = await s2.finish()
        await s2.close()
    assert seqs[1][0] == 1 and seqs[2][0] == 1 and s2.sequence_anomalies == 0
    assert r.status == "COMPLETED" and r.usage.received_samples == 8000 + 12000


# ---------------------------------------------------------------- A10
async def test_a10_ack_lost_then_disconnect_closes_out_over_rest(fx):
    async with client(fx, timeouts=SHORT) as c:
        s = await c.connect(req("A10-ack-lost-disconnect"))
        data = audio("pcm_s16le_16000")
        with pytest.raises(Exception):
            for i in range(0, len(data), 3200):
                await s.send_audio(data[i:i + 3200])
                await asyncio.sleep(0.03)
        r = await s.finish()
        await s.close()
    rec = fx.record(s.session_id)
    assert r.via_rest and r.closeout_reason == "DISCONNECTED"
    assert r.session_id == s.session_id and len(rec["connections"]) == 1  # no reconnect, no resend
    assert len(fx.by_title("A10-ack-lost-disconnect")) == 1 and rec["restFinishCount"] == 1
    # the SDK claims only what it took in; the server settles only what it received
    assert r.status in ("COMPLETED", "PARTIAL") and r.status == rec["status"]


async def test_a10_already_terminal_on_query_returns_real_result(fx):
    async with client(fx, timeouts=SHORT) as c:
        s = await c.connect(req("A10-terminal-on-query"))
        data = audio("pcm_s16le_16000")
        try:
            for i in range(0, len(data), 3200):
                await s.send_audio(data[i:i + 3200])
                await asyncio.sleep(0.03)
        except Exception:
            pass
        r = await s.finish()
        await s.close()
    rec = fx.record(s.session_id)
    assert r.via_rest and rec["restFinishCount"] == 0 and r.status == rec["status"]


async def test_a10_rest_unreachable_keeps_session_id_then_explicit_closeout(fx):
    async with client(fx, timeouts=SHORT) as c:
        s = await c.connect(req("A10-rest-unreachable"))
        data = audio("pcm_s16le_16000")
        try:
            for i in range(0, len(data), 3200):
                await s.send_audio(data[i:i + 3200])
                await asyncio.sleep(0.03)
        except Exception:
            pass
        with pytest.raises(FinishUnconfirmedError) as ei:
            await s.finish()
        assert ei.value.session_id == s.session_id
        await s.close()
        fx.post("/sessions/%s/rest-up" % s.session_id)
        view = await c.get_session(s.session_id)
        assert not view.is_terminal
        done = await c.finish_session(s.session_id)
    rec = fx.record(s.session_id)
    assert done.is_terminal and len(fx.by_title("A10-rest-unreachable")) == 1
    assert len(rec["connections"]) == 1


async def test_a10_disconnect_without_finish_call_closes_out_in_background(fx):
    async with client(fx, timeouts=SHORT) as c:
        s = await c.connect(req("L-basic"))
        await s.send_audio(audio("pcm_s16le_16000", 0.5))
        await s.commit()
        await s.abort_connection()
        r = await s.finish()  # returns the background close-out result
        await s.close()
    assert r.via_rest and r.closeout_reason == "DISCONNECTED" and r.status == "COMPLETED"
    assert len(fx.record(s.session_id)["connections"]) == 1


# ---------------------------------------------------------------- A11
async def test_a11_no_ack_progress_times_out_and_closes_out(fx):
    async with client(fx, timeouts=SHORT) as c:
        s = await c.connect(req("A11-no-ack"))
        await s.send_audio(audio("pcm_s16le_16000", 0.3))
        t0 = time.monotonic()
        r = await s.finish()
        await s.close()
    assert r.closeout_reason == "ACK_TIMEOUT" and r.via_rest and time.monotonic() - t0 < 6


async def test_a11_idle_is_not_an_ack_timeout(fx):
    async with client(fx, timeouts=SHORT) as c:
        s = await c.connect(req("L-basic"))
        await s.send_audio(audio("pcm_s16le_16000", 0.3))
        await s.commit()
        await asyncio.sleep(1.6)  # > ack timeout with nothing unconfirmed
        await s.send_audio(audio("pcm_s16le_16000")[9600:])
        r = await s.finish()
        await s.close()
    assert r.status == "COMPLETED" and not r.via_rest


async def test_a11_completed_event_lost_uses_rest(fx):
    async with client(fx, timeouts=SHORT) as c:
        s = await c.connect(req("A11-completed-lost"))
        await s.send_audio(audio("pcm_s16le_16000"))
        r = await s.finish()
        await s.close()
    assert r.via_rest and r.status == "COMPLETED"
    assert r.transcription_id == fx.record(s.session_id)["transcriptionId"]


async def test_a11_finish_timeout_bounded(fx):
    async with client(fx, timeouts=SHORT) as c:
        s = await c.connect(req("A11-finish-timeout"))
        await s.send_audio(audio("pcm_s16le_16000"))
        t0 = time.monotonic()
        r = await s.finish()
        await s.close()
    assert r.closeout_reason == "FINISH_TIMEOUT" and time.monotonic() - t0 < 7 and r.status == "COMPLETED"


async def test_a11_closeout_deadline_bounds_every_rest_request(fx):
    # every REST answer of this session takes 1.2 s; the close-out budget is 0.3 s
    tight = Timeouts(request=3, create=3, connect=3, ack=1.0, finish=1.5, closeout=0.3, close=0.3)
    async with client(fx, timeouts=tight) as c:
        s = await c.connect(req("A11-slow-rest"))
        await s.send_audio(audio("pcm_s16le_16000", 0.1))
        await s.commit()
        t0 = time.monotonic()
        await s.abort_connection()
        with pytest.raises(FinishUnconfirmedError) as ei:
            await s.finish()
        took = time.monotonic() - t0
        await s.close()
    assert ei.value.session_id == s.session_id and took < 0.9, took  # not 1.2 s+ per request
    assert not fx.record(s.session_id)["status"] in ("COMPLETED", "PARTIAL", "FAILED")  # not turned into a result
    # the same slow REST inside an adequate budget still closes out normally
    roomy = Timeouts(request=3, create=3, connect=3, ack=1.0, finish=1.5, closeout=4, close=0.5)
    async with client(fx, timeouts=roomy) as c:
        s = await c.connect(req("A11-slow-rest"))
        await s.send_audio(audio("pcm_s16le_16000", 0.1))
        await s.commit()
        t0 = time.monotonic()
        await s.abort_connection()
        r = await s.finish()
        took = time.monotonic() - t0
        await s.close()
    assert r.via_rest and r.status == "COMPLETED" and r.closeout_reason == "DISCONNECTED" and took < 4.5, took


async def test_a11_rest_finish_response_lost_is_idempotent(fx):
    async with client(fx, timeouts=SHORT) as c:
        s = await c.connect(req("A11-rest-finish-lost"))
        data = audio("pcm_s16le_16000")
        try:
            for i in range(0, len(data), 3200):
                await s.send_audio(data[i:i + 3200])
                await asyncio.sleep(0.03)
        except Exception:
            pass
        r = await s.finish()
        await s.close()
    rec = fx.record(s.session_id)
    assert r.via_rest and r.status == rec["status"] and rec["settledCount"] == 1


async def test_a11_insufficient_balance_not_retried(fx):
    async with client(fx, timeouts=SHORT) as c:
        s = await c.connect(req("A11-insufficient"))
        with pytest.raises(InsufficientCharactersError):
            data = audio("pcm_s16le_16000")
            for i in range(0, len(data), 3200):
                await s.send_audio(data[i:i + 3200])
                await asyncio.sleep(0.03)
            await s.commit()
        r = await s.finish()
        await s.close()
    rec = fx.record(s.session_id)
    assert "INSUFFICIENT_CHARACTERS" in [u.get("errorCode") for u in rec["usageReplies"] if u.get("errorCode")]
    assert r.status in ("PARTIAL", "COMPLETED") and r.status == rec["status"]
    assert len([f for f in rec["frames"] if f["n"] >= 3 and f["accepted"]]) == 0  # nothing blindly retried


async def test_a11_retryable_frame_error_is_replayed_bounded(fx):
    data = audio("pcm_s16le_16000")
    async with client(fx) as c:
        s = await c.connect(req("A11-temp-error-frame"))
        await s.send_audio(data)
        r = await s.finish()
        await s.close()
    assert r.status == "COMPLETED" and fx.record(s.session_id)["storedSha256"] == sha(data)
    assert "TEMPORARILY_UNAVAILABLE" in r.server_errors


@pytest.mark.parametrize("sid,status", [("A11-partial", "PARTIAL"), ("A11-failed", "FAILED")])
async def test_a11_partial_failed_reported_as_is(fx, sid, status):
    async with client(fx) as c:
        s = await c.connect(req(sid))
        await s.send_audio(audio("pcm_s16le_16000"))
        r = await s.finish()
        await s.close()
    assert r.status == status


# ---------------------------------------------------------------- A12
async def test_a12_slow_callback_does_not_block_acks(fx):
    seen = []

    def slow(ev):
        time.sleep(0.3)
        seen.append(ev.type)
    async with client(fx) as c:
        s = await c.connect(req("A12-slow-callback"), handlers={"transcript.final": slow})
        t0 = time.monotonic()
        await s.send_audio(audio("pcm_s16le_16000"))
        r = await s.finish()
        took = time.monotonic() - t0
        await s.close()
    assert took < 1.5 and r.status == "COMPLETED" and seen  # delivered after, without stalling the stream


async def test_a12_callback_exception_is_observable_not_a_server_failure(fx):
    errs = []

    def boom(ev):
        raise RuntimeError("handler bug")
    async with client(fx) as c:
        s = await c.connect(req("A12-callback-throws"), handlers={"usage.updated": boom},
                            on_callback_error=lambda e, ev: errs.append((type(e).__name__, ev.type)))
        await s.send_audio(audio("pcm_s16le_16000"))
        r = await s.finish()
        await s.close()
    assert r.status == "COMPLETED" and errs and errs[0] == ("RuntimeError", "usage.updated")
    assert s.callback_errors == len(errs) and r.callback_errors <= len(errs) and not r.server_errors


async def test_a12_overflow_reports_and_result_still_complete(fx):
    errs = []
    gate = threading.Event()
    async with client(fx, event_queue_size=2) as c:
        s = await c.connect(req("A12-overflow"), on_event=lambda e: gate.wait(2),
                            on_callback_error=lambda e, ev: errs.append(e))
        await s.send_audio(audio("pcm_s16le_16000"))
        await s.commit()
        r = await s.finish()
        gate.set()
        await s.close()
    assert r.dropped_events > 0 and len(errs) >= r.dropped_events
    assert r.status == "COMPLETED" and r.lines and r.lines[0].text  # finals kept in the snapshot


def test_a12_sync_event_iterator_is_bounded(fx):
    errs = []
    with RealtimeClient(KEY, base_url=fx.base, trust_env=False, event_queue_size=2) as c:
        s = c.connect(req("L-basic"), iterate_events=True, on_callback_error=lambda e, ev: errs.append((e, ev)))
        for _ in range(10):  # nobody iterates yet: the queue fills up
            s.send_audio(audio("pcm_s16le_16000", 0.1))
            s.commit()
        r = s.finish()
        q = s._inner._iter_queue
        held = q.qsize()
        got = [e.type for e in s.events(timeout=0.5)]
        s.close()
    assert q.maxsize == 2 and held <= 2 and len(got) <= 2
    assert r.status == "COMPLETED" and not r.via_rest  # acknowledgements kept flowing
    dropped = s.stats()["dropped_events"]
    assert dropped > 0 and len(errs) == dropped and all(isinstance(e, OverflowError) for e, _ in errs)
    assert any(ev.type == "transcript.final" for _, ev in errs) or any(t == "transcript.final" for t in got)
    assert r.lines and r.lines[0].text  # finals kept in the result snapshot


def test_a12_sync_event_iterator_drained_then_ends_after_close(fx):
    with RealtimeClient(KEY, base_url=fx.base, trust_env=False) as c:
        s = c.connect(req("L-basic"), iterate_events=True)
        s.send_audio(audio("pcm_s16le_16000"))
        s.finish()
        s.close()
        types = [e.type for e in s.events()]  # no timeout: must still end
    assert types[0] == "session.ready" and types[-1] == "session.completed" and s.stats()["dropped_events"] == 0


async def test_a12_closed_sessions_are_released_by_client(fx):
    import gc
    import weakref
    refs = []
    async with client(fx) as c:
        for _ in range(3):
            s = await c.connect(req("L-basic"))
            await s.send_audio(audio("pcm_s16le_16000", 0.1))
            r = await s.finish()
            assert r.status == "COMPLETED"
            await s.close()
            refs.append(weakref.ref(s))
            del s
        await asyncio.sleep(0.05)
        gc.collect()
        assert len(c._sessions) == 0 and sum(x() is not None for x in refs) == 0
        assert r.transcription_id and r.lines  # the caller's result object is kept
        open_s = await c.connect(req("L-basic"))
        assert c._sessions == {open_s}  # an open session stays registered until it is closed
    assert len(c._sessions) == 0


def test_a12_sync_closed_sessions_are_released_by_client(fx):
    with RealtimeClient(KEY, base_url=fx.base, trust_env=False) as c:
        for _ in range(2):
            with c.connect(req("L-basic")) as s:
                s.send_audio(audio("pcm_s16le_16000", 0.1))
                s.finish()
        assert len(c._async._sessions) == 0


async def test_a12_idle_ping_keeps_socket_without_audio(fx):
    async with client(fx, ping_interval=0.2) as c:
        s = await c.connect(req("A12-idle-ping"))
        await asyncio.sleep(1.2)
        rec = fx.record(s.session_id)
        r = await s.finish()
        await s.close()
    assert rec["connections"][0]["pings"] >= 3 and rec["frames"] == [] and rec["captured"] in (None, 0)
    assert r.usage.received_samples == 0  # pings are never audio


async def test_a12_tls_verification_on_by_default(fx_tls):
    async with AsyncRealtimeClient(KEY, base_url=fx_tls.base, trust_env=False) as c:
        with pytest.raises(TransportError):
            await c.create_session(req("A12-network"))
    ctx = ssl.create_default_context(cafile=CA)
    async with AsyncRealtimeClient(KEY, base_url=fx_tls.base, trust_env=False, ssl_context=ctx) as c:
        s = await c.connect(req("A12-network"))
        await s.send_audio(audio("pcm_s16le_16000"))
        r = await s.finish()
        await s.close()
    assert r.status == "COMPLETED"


async def test_a12_proxy_and_borrowed_http_client(fx_tls):
    ctx = ssl.create_default_context(cafile=CA)
    proxy = "http://127.0.0.1:%d" % fx_tls.proxy_port
    before = fx_tls.get("/proxy")["tunnels"]
    async with AsyncRealtimeClient(KEY, base_url=fx_tls.base, trust_env=False, ssl_context=ctx, proxy=proxy) as c:
        s = await c.connect(req("A12-network"))
        await s.send_audio(audio("pcm_s16le_16000"))
        await s.finish()
        await s.close()
    assert fx_tls.get("/proxy")["tunnels"] >= before + 2  # REST and socket both tunnelled
    borrowed = httpx.AsyncClient(verify=ctx, trust_env=False)
    c = AsyncRealtimeClient(KEY, base_url=fx_tls.base, http_client=borrowed, ssl_context=ctx, trust_env=False)
    v = await c.create_session(req("A12-network"))
    await c.finish_session(v.session_id)
    await c.aclose()
    assert not borrowed.is_closed  # never closes a borrowed client
    await borrowed.aclose()


async def test_a12_cancel_finish_and_idempotent_close(fx):
    async with client(fx) as c:
        s = await c.connect(req("A12-cancel"))
        await s.send_audio(audio("pcm_s16le_16000"))
        t = asyncio.ensure_future(s.finish())
        await asyncio.sleep(0.5)
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
        await s.close()
        await s.close()
        assert s._tasks == [] or all(x.done() for x in s._tasks)
        await asyncio.sleep(3)
        done = await c.finish_session(s.session_id)  # explicit close-out by id is still possible
    assert done.is_terminal


def test_a12_sync_threads_released(fx):
    before = {t.name for t in threading.enumerate()}
    with RealtimeClient(KEY, base_url=fx.base, trust_env=False) as c:
        with c.connect(req("L-basic")) as s:
            s.send_audio(audio("pcm_s16le_16000"))
            s.finish()
    time.sleep(0.5)
    leftover = {t.name for t in threading.enumerate() if t.is_alive()} - before
    assert not [n for n in leftover if n.startswith("myvocal")], leftover


# ---------------------------------------------------------------- A13
async def test_a13_no_secret_audio_or_text_in_repr_errors_and_logs(fx):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    root = logging.getLogger()
    root.addHandler(handler)
    old = root.level
    root.setLevel(logging.DEBUG)
    try:
        async with client(fx) as c:
            s = await c.connect(req("A08-events-all"))
            t = await c.create_ticket(s.session_id)
            await s.send_audio(audio("pcm_s16le_16000"))
            r = await s.finish()
            await s.close()
            reprs = " ".join(map(repr, [c, s, t, r, r.usage]))
            bad = AsyncRealtimeClient("bad-SECRET-key", base_url=fx.base, trust_env=False)
            try:
                await bad.create_session(req("x"))
            except Exception as e:  # noqa: BLE001
                err_text = repr(e) + str(e)
            await bad.aclose()
    finally:
        root.removeHandler(handler)
        root.setLevel(old)
    logs = stream.getvalue()
    for blob in (reprs, err_text, logs):
        assert KEY not in blob and "bad-SECRET-key" not in blob and t.ticket not in blob
        assert "accessKey" not in blob or "accessKey=" not in blob
        assert "hello world" not in blob and "audioBase64" not in blob
