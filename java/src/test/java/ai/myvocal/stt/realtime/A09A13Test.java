package ai.myvocal.stt.realtime;

import static ai.myvocal.stt.realtime.Fx.audio;
import static ai.myvocal.stt.realtime.Fx.check;
import static ai.myvocal.stt.realtime.Fx.list;
import static ai.myvocal.stt.realtime.Fx.num;
import static ai.myvocal.stt.realtime.Fx.opts;
import static ai.myvocal.stt.realtime.Fx.req;
import static ai.myvocal.stt.realtime.Fx.sendAll;
import static ai.myvocal.stt.realtime.Fx.sha;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

class A09A13Test {
    private final Fx f = Fx.plain();
    static final Timeouts SHORT = new Timeouts().withRequestMillis(3000).withCreateMillis(3000).withConnectMillis(3000)
            .withAckMillis(1000).withFinishMillis(1500).withCloseoutMillis(4000).withCloseMillis(2000);

    private RealtimeClient shortClient() {
        return f.client(b -> b.timeouts(SHORT));
    }

    /** Streams with small pauses; returns the first error (the disconnect) or null. */
    static RuntimeException sendUntilError(RealtimeSession s, byte[] data) {
        try {
            for (int i = 0; i < data.length; i += 3200) {
                s.sendAudio(data, i, Math.min(3200, data.length - i), null);
                Fx.sleep(30);
            }
            return null;
        } catch (RuntimeException e) {
            return e;
        }
    }

    // ------------------------------------------------------------ A09

    @Test
    void a09Int64NullAndWireTypes() {
        long big = 9007199254740993L;
        try (RealtimeClient c = f.client()) {
            RealtimeSession s = c.connect(opts("A09-bigints"));
            check((Long) s.getCursors().get("receivedSamples") == big && (Long) s.getCursors().get("capturedSamples") == big, s.getCursors());
            sendAll(s, audio("pcm_s16le_16000", 0), 1 << 20);
            FinishResult r = s.finish();
            s.close();
            Map<String, Object> rec = f.record(s.getSessionId());
            check(r.getUsage().getElapsedMs() == Long.MAX_VALUE && r.getUsage().getReceivedSamples() == big + 20000
                    && Fx.maxClaim(rec) == big + 20000, r.getUsage(), Fx.maxClaim(rec));
            for (Map<String, Object> fr : list(rec, "frames")) {
                check("str".equals(fr.get("sampleOffsetType")) && "str".equals(fr.get("capturedType")), fr);
            }
            List<Word> w = r.getLines().get(0).getWords();
            check(w.get(0).getStartMs() >= big && w.get(w.size() - 1).getStartMs() == null && w.get(w.size() - 1).getEndMs() == null, w);
        }
    }

    @Test
    void a09SequencePerConnection() {
        Map<Integer, List<Long>> seqs = Collections.synchronizedMap(new HashMap<Integer, List<Long>>());
        seqs.put(1, Collections.synchronizedList(new ArrayList<Long>()));
        seqs.put(2, Collections.synchronizedList(new ArrayList<Long>()));
        try (RealtimeClient c = f.client()) {
            RealtimeSession s = c.connect(opts("L-basic").onEvent(e -> seqs.get(1).add(e.getSequence())));
            sendAll(s, audio("pcm_s16le_16000", 0.5), 1 << 20);
            s.commit();
            s.close();
            Fx.sleep(200);
            RealtimeSession s2 = c.connect(new ConnectOptions().sessionId(s.getSessionId()).onEvent(e -> seqs.get(2).add(e.getSequence())));
            assertEquals(8000L, s2.getCursors().get("capturedSamples"));
            s2.resume(RealtimeSession.Via.WS);
            byte[] all = audio("pcm_s16le_16000", 0);
            sendAll(s2, Arrays.copyOfRange(all, 16000, all.length), 1 << 20);
            FinishResult r = s2.finish();
            s2.close();
            check(seqs.get(1).get(0) == 1 && seqs.get(2).get(0) == 1 && num(s2.getStats().get("sequenceAnomalies")) == 0
                    && "COMPLETED".equals(r.getStatus()) && r.getUsage().getReceivedSamples() == 20000, seqs, r);
        }
    }

    // ------------------------------------------------------------ A10

    @Test
    @Tag("compat")
    void a10AckLostThenDisconnect() {
        try (RealtimeClient c = shortClient()) {
            RealtimeSession s = c.connect(opts("A10-ack-lost-disconnect"));
            check(sendUntilError(s, audio("pcm_s16le_16000", 0)) != null, "send path must report the lost connection");
            FinishResult r = s.finish();
            s.close();
            Map<String, Object> rec = f.record(s.getSessionId());
            check(r.isViaRest() && "DISCONNECTED".equals(r.getCloseoutReason()) && list(rec, "connections").size() == 1
                    && num(rec.get("restFinishCount")) == 1 && f.byTitle("A10-ack-lost-disconnect").size() == 1
                    && r.getStatus().equals(rec.get("status")), r, rec.get("status"));
        }
    }

    @Test
    void a10AlreadyTerminalOnQuery() {
        try (RealtimeClient c = shortClient()) {
            RealtimeSession s = c.connect(opts("A10-terminal-on-query"));
            sendUntilError(s, audio("pcm_s16le_16000", 0));
            FinishResult r = s.finish();
            s.close();
            Map<String, Object> rec = f.record(s.getSessionId());
            check(r.isViaRest() && num(rec.get("restFinishCount")) == 0 && r.getStatus().equals(rec.get("status")), r);
        }
    }

    @Test
    void a10RestUnreachableKeepsSessionId() {
        try (RealtimeClient c = shortClient()) {
            RealtimeSession s = c.connect(opts("A10-rest-unreachable"));
            sendUntilError(s, audio("pcm_s16le_16000", 0));
            FinishUnconfirmedException e = assertThrows(FinishUnconfirmedException.class, s::finish);
            assertEquals(s.getSessionId(), e.getSessionId());
            s.close();
            f.post("/sessions/" + s.getSessionId() + "/rest-up");
            SessionView v = c.getSession(s.getSessionId());
            SessionView done = c.finishSession(s.getSessionId(), null);
            check(!v.isTerminal() && done.isTerminal() && f.byTitle("A10-rest-unreachable").size() == 1
                    && list(f.record(s.getSessionId()), "connections").size() == 1, v, done);
        }
    }

    @Test
    void a10DisconnectWithoutFinishClosesOut() {
        try (RealtimeClient c = shortClient()) {
            RealtimeSession s = c.connect(opts("L-basic"));
            sendAll(s, audio("pcm_s16le_16000", 0.5), 1 << 20);
            s.commit();
            s.abortConnection();
            assertThrows(ConnectionLostException.class, () -> s.sendAudio(new byte[320]));
            FinishResult r = s.finish();
            s.close();
            check(r.isViaRest() && "DISCONNECTED".equals(r.getCloseoutReason()) && "COMPLETED".equals(r.getStatus())
                    && list(f.record(s.getSessionId()), "connections").size() == 1, r);
        }
    }

    // ------------------------------------------------------------ A11

    private static final class Run {
        RealtimeSession s;
        FinishResult r;
        long took;
    }

    private Run finishOf(RealtimeClient c, String title, byte[] data) {
        Run run = new Run();
        run.s = c.connect(opts(title));
        sendAll(run.s, data, 1 << 20);
        long t0 = System.nanoTime();
        run.r = run.s.finish();
        run.took = (System.nanoTime() - t0) / 1_000_000;
        run.s.close();
        return run;
    }

    @Test
    void a11NoAckProgress() {
        try (RealtimeClient c = shortClient()) {
            Run x = finishOf(c, "A11-no-ack", audio("pcm_s16le_16000", 0.3));
            check("ACK_TIMEOUT".equals(x.r.getCloseoutReason()) && x.r.isViaRest() && x.took <= 6000, x.r, x.took);
        }
    }

    @Test
    void a11IdleIsNotAckTimeout() {
        try (RealtimeClient c = shortClient()) {
            RealtimeSession s = c.connect(opts("L-basic"));
            sendAll(s, audio("pcm_s16le_16000", 0.3), 1 << 20);
            s.commit();
            Fx.sleep(1600);
            byte[] all = audio("pcm_s16le_16000", 0);
            sendAll(s, Arrays.copyOfRange(all, 9600, all.length), 1 << 20);
            FinishResult r = s.finish();
            s.close();
            check("COMPLETED".equals(r.getStatus()) && !r.isViaRest(), r);
        }
    }

    @Test
    void a11CompletedEventLost() {
        try (RealtimeClient c = shortClient()) {
            Run x = finishOf(c, "A11-completed-lost", audio("pcm_s16le_16000", 0));
            check(x.r.isViaRest() && "COMPLETED".equals(x.r.getStatus())
                    && x.r.getTranscriptionId().equals(f.record(x.s.getSessionId()).get("transcriptionId")), x.r);
        }
    }

    @Test
    void a11FinishTimeoutBounded() {
        try (RealtimeClient c = shortClient()) {
            Run x = finishOf(c, "A11-finish-timeout", audio("pcm_s16le_16000", 0));
            check("FINISH_TIMEOUT".equals(x.r.getCloseoutReason()) && x.took <= 7000 && "COMPLETED".equals(x.r.getStatus()), x.r, x.took);
        }
    }

    @Test
    void a11CloseoutDeadlineBoundsEveryRestRequest() {
        // every REST answer of this session takes 1.2 s; the close-out budget is 300 ms
        Timeouts tight = SHORT.withCloseoutMillis(300).withCloseMillis(300);
        try (RealtimeClient c = f.client(b -> b.timeouts(tight))) {
            RealtimeSession s = c.connect(opts("A11-slow-rest"));
            sendAll(s, audio("pcm_s16le_16000", 0.1), 1 << 20);
            s.commit();
            long t0 = System.nanoTime();
            s.abortConnection();
            FinishUnconfirmedException e = assertThrows(FinishUnconfirmedException.class, s::finish);
            long took = (System.nanoTime() - t0) / 1_000_000;
            s.close();
            assertEquals(s.getSessionId(), e.getSessionId());
            Object st = f.record(s.getSessionId()).get("status");
            check(took < 900 && !"COMPLETED".equals(st) && !"PARTIAL".equals(st) && !"FAILED".equals(st), took, st);
        }
        // the same slow REST inside an adequate budget still closes out normally
        try (RealtimeClient c = f.client(b -> b.timeouts(SHORT.withCloseMillis(500)))) {
            RealtimeSession s = c.connect(opts("A11-slow-rest"));
            sendAll(s, audio("pcm_s16le_16000", 0.1), 1 << 20);
            s.commit();
            long t0 = System.nanoTime();
            s.abortConnection();
            FinishResult r = s.finish();
            long took = (System.nanoTime() - t0) / 1_000_000;
            s.close();
            check(r.isViaRest() && "COMPLETED".equals(r.getStatus()) && "DISCONNECTED".equals(r.getCloseoutReason())
                    && took < 4500, r, took);
        }
    }

    @Test
    void a11RestFinishResponseLostIsIdempotent() {
        try (RealtimeClient c = shortClient()) {
            RealtimeSession s = c.connect(opts("A11-rest-finish-lost"));
            sendUntilError(s, audio("pcm_s16le_16000", 0));
            FinishResult r = s.finish();
            s.close();
            Map<String, Object> rec = f.record(s.getSessionId());
            check(r.isViaRest() && r.getStatus().equals(rec.get("status")) && num(rec.get("settledCount")) == 1, r, rec.get("settledCount"));
        }
    }

    @Test
    void a11InsufficientBalanceNotRetried() {
        try (RealtimeClient c = shortClient()) {
            RealtimeSession s = c.connect(opts("A11-insufficient"));
            RuntimeException err = sendUntilError(s, audio("pcm_s16le_16000", 0));
            if (err == null) {
                try {
                    s.commit();
                } catch (RuntimeException e) {
                    err = e;
                }
            }
            check(err instanceof InsufficientCharactersException, err);
            FinishResult r = s.finish();
            s.close();
            Map<String, Object> rec = f.record(s.getSessionId());
            boolean seen = false;
            for (Map<String, Object> u : list(rec, "usageReplies")) {
                seen |= "INSUFFICIENT_CHARACTERS".equals(u.get("errorCode"));
            }
            for (Map<String, Object> fr : list(rec, "frames")) {
                check(!(num(fr.get("n")) >= 3 && Boolean.TRUE.equals(fr.get("accepted"))), "frame retried after balance stop", fr);
            }
            check(seen && r.getStatus().equals(rec.get("status")), r);
        }
    }

    @Test
    void a11RetryableFrameErrorReplayedBounded() {
        byte[] data = audio("pcm_s16le_16000", 0);
        try (RealtimeClient c = f.client()) {
            Run x = finishOf(c, "A11-temp-error-frame", data);
            check("COMPLETED".equals(x.r.getStatus()) && sha(data).equals(f.record(x.s.getSessionId()).get("storedSha256"))
                    && x.r.getServerErrors().contains("TEMPORARILY_UNAVAILABLE"), x.r);
        }
    }

    @Test
    void a11PartialFailedAsIs() {
        try (RealtimeClient c = f.client()) {
            assertEquals("PARTIAL", finishOf(c, "A11-partial", audio("pcm_s16le_16000", 0)).r.getStatus());
            assertEquals("FAILED", finishOf(c, "A11-failed", audio("pcm_s16le_16000", 0)).r.getStatus());
        }
    }

    // ------------------------------------------------------------ A12

    @Test
    void a12SlowCallbackDoesNotBlockAcks() {
        AtomicInteger n = new AtomicInteger();
        try (RealtimeClient c = f.client()) {
            RealtimeSession s = c.connect(opts("A12-slow-callback").on(RealtimeEvent.TRANSCRIPT_FINAL, e -> {
                Fx.sleep(300);
                n.incrementAndGet();
            }));
            long t0 = System.nanoTime();
            sendAll(s, audio("pcm_s16le_16000", 0), 1 << 20);
            FinishResult r = s.finish();
            long took = (System.nanoTime() - t0) / 1_000_000;
            s.close();
            check(took <= 1500 && "COMPLETED".equals(r.getStatus()) && n.get() > 0, took, n);
        }
    }

    @Test
    void a12CallbackThrowsObservable() {
        List<String> errs = Collections.synchronizedList(new ArrayList<String>());
        try (RealtimeClient c = f.client()) {
            RealtimeSession s = c.connect(opts("A12-callback-throws")
                    .on(RealtimeEvent.USAGE_UPDATED, e -> { throw new IllegalStateException("handler bug"); })
                    .onCallbackError((e, ev) -> errs.add(ev.getType())));
            sendAll(s, audio("pcm_s16le_16000", 0), 1 << 20);
            FinishResult r = s.finish();
            s.close();
            check("COMPLETED".equals(r.getStatus()) && !errs.isEmpty() && RealtimeEvent.USAGE_UPDATED.equals(errs.get(0))
                    && s.getCallbackErrors() == errs.size() && r.getServerErrors().isEmpty(), r, errs);
        }
    }

    @Test
    void a12OverflowReportedResultComplete() {
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger reported = new AtomicInteger();
        try (RealtimeClient c = f.client(b -> b.eventQueueSize(2))) {
            RealtimeSession s = c.connect(opts("A12-overflow").onEvent(e -> {
                try {
                    gate.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }).onCallbackError((e, ev) -> reported.incrementAndGet()));
            sendAll(s, audio("pcm_s16le_16000", 0), 1 << 20);
            s.commit();
            FinishResult r = s.finish();
            gate.countDown();
            s.close();
            Fx.sleep(200);
            check(r.getDroppedEvents() > 0 && reported.get() >= r.getDroppedEvents() && "COMPLETED".equals(r.getStatus())
                    && !r.getLines().isEmpty() && !r.getLines().get(0).getText().isEmpty(), r, reported);
        }
    }

    @Test
    void a12IdlePingWithoutAudio() {
        try (RealtimeClient c = f.client(b -> b.pingIntervalMillis(200))) {
            RealtimeSession s = c.connect(opts("A12-idle-ping"));
            Fx.sleep(1200);
            Map<String, Object> rec = f.record(s.getSessionId());
            FinishResult r = s.finish();
            s.close();
            check(num(list(rec, "connections").get(0).get("pings")) >= 3 && list(rec, "frames").isEmpty()
                    && (rec.get("captured") == null || num(rec.get("captured")) == 0) && r.getUsage().getReceivedSamples() == 0,
                    list(rec, "connections").get(0));
        }
    }

    @Test
    void a12TlsVerificationDefaultAndCustomCa() {
        Fx t = Fx.tls();
        try (RealtimeClient plainTrust = RealtimeClient.builder(Fx.KEY).baseUrl(t.base).proxy(Proxy.NO_PROXY).build()) {
            assertThrows(TransportException.class, () -> plainTrust.createSession(req("A12-network")));
        }
        try (RealtimeClient c = t.client()) {
            RealtimeSession s = c.connect(opts("A12-network"));
            sendAll(s, audio("pcm_s16le_16000", 0), 1 << 20);
            FinishResult r = s.finish();
            s.close();
            assertEquals("COMPLETED", r.getStatus());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void a12ProxyAndBorrowedHttpClient() throws Exception {
        Fx t = Fx.tls();
        long before = num(((Map<String, Object>) t.get("/proxy")).get("tunnels"));
        String sid;
        try (RealtimeClient c = t.client(b -> b.proxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress("127.0.0.1", t.proxyPort))))) {
            RealtimeSession s = c.connect(opts("A12-network"));
            sendAll(s, audio("pcm_s16le_16000", 0), 1 << 20);
            s.finish();
            s.close();
            sid = s.getSessionId();
        }
        long after = num(((Map<String, Object>) t.get("/proxy")).get("tunnels"));
        check(after >= before + 2, before, after, sid);
        OkHttpClient borrowed = new OkHttpClient.Builder().proxy(Proxy.NO_PROXY)
                .sslSocketFactory(Fx.sslContext().getSocketFactory(), Fx.trustManager()).build();
        RealtimeClient c2 = RealtimeClient.builder(Fx.KEY).baseUrl(t.base).httpClient(borrowed).build();
        SessionView v = c2.createSession(req("A12-network"));
        c2.close();
        try (Response resp = borrowed.newCall(new Request.Builder().url(t.base + "/__fixture/health").build()).execute()) {
            assertTrue(resp.isSuccessful()); // never shut down by the SDK
        }
        try (RealtimeClient c3 = t.client()) {
            c3.finishSession(v.getSessionId(), null);
        }
    }

    @Test
    void a12CancelFinishAndIdempotentClose() throws Exception {
        try (RealtimeClient c = f.client()) {
            RealtimeSession s = c.connect(opts("A12-cancel"));
            sendAll(s, audio("pcm_s16le_16000", 0), 1 << 20);
            CompletableFuture<FinishResult> fut = s.finishAsync();
            Fx.sleep(500);
            fut.cancel(true);
            try {
                fut.get();
                fail("expected cancellation");
            } catch (CancellationException expected) {
                // ok
            }
            s.close();
            s.close();
            Fx.sleep(3000);
            SessionView done = c.finishSession(s.getSessionId(), null);
            assertTrue(done.isTerminal());
        }
    }

    @Test
    void a12CancelBlockingFinishByInterrupt() throws Exception {
        try (RealtimeClient c = f.client()) {
            RealtimeSession s = c.connect(opts("A12-cancel"));
            sendAll(s, audio("pcm_s16le_16000", 0), 1 << 20);
            Thread caller = Thread.currentThread();
            Thread interrupter = new Thread(() -> { Fx.sleep(500); caller.interrupt(); });
            interrupter.start();
            assertThrows(CancellationException.class, s::finish);
            Thread.interrupted();
            s.close();
            Fx.sleep(3000);
            assertTrue(c.finishSession(s.getSessionId(), null).isTerminal());
        }
    }

    private static int sdkThreads() {
        int n = 0;
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            String name = t.getName();
            if (t.isAlive() && (name.startsWith("myvocal-stt") || name.startsWith("OkHttp http") || name.startsWith("OkHttp ws"))) {
                n++;
            }
        }
        return n;
    }

    @Test
    void a12ThreadsReleased() {
        Fx.sleep(300);
        int before = sdkThreads();
        for (int i = 0; i < 3; i++) {
            RealtimeClient c = f.client();
            RealtimeSession s = c.connect(opts("L-basic"));
            sendAll(s, audio("pcm_s16le_16000", 0), 1 << 20);
            s.finish();
            s.close();
            c.close();
        }
        long deadline = System.currentTimeMillis() + 5000;
        while (sdkThreads() > before && System.currentTimeMillis() < deadline) {
            Fx.sleep(100);
        }
        check(sdkThreads() <= before, before, sdkThreads());
    }

    // ------------------------------------------------------------ A13

    @Test
    void a13NoSecretOrContentInStringsAndErrors() {
        try (RealtimeClient c = f.client()) {
            RealtimeSession s = c.connect(opts("A08-events-all"));
            Ticket tk = c.createTicket(s.getSessionId());
            sendAll(s, audio("pcm_s16le_16000", 0), 1 << 20);
            FinishResult r = s.finish();
            s.close();
            String err;
            try (RealtimeClient bad = RealtimeClient.builder("bad-SECRET-key").baseUrl(f.base).proxy(Proxy.NO_PROXY).build()) {
                try {
                    bad.createSession(req("x"));
                    err = "";
                } catch (RuntimeException e) {
                    err = e.toString() + " " + e.getMessage();
                }
                String blob = c + " " + bad + " " + s + " " + tk + " " + r + " " + r.getLines() + " " + r.getView() + " " + err;
                for (String secret : Arrays.asList(Fx.KEY, "bad-SECRET-key", tk.getTicket(), "hello world", "audioBase64")) {
                    check(!blob.contains(secret), "leaked", secret);
                }
            }
        }
    }
}
