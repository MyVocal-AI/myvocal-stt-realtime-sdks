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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class A05A08Test {
    private final Fx f = Fx.plain();

    // ------------------------------------------------------------ A05

    @Test
    void a05FilePacedLiveNot() throws Exception {
        Path raw = Fx.ROOT.resolve("fixtures/audio/tone_pcm_s16le_16000.raw");
        try (RealtimeClient c = f.client()) {
            RealtimeSession s = c.connect(opts("A05-pacing"));
            long t0 = System.nanoTime();
            s.sendFile(raw, true);
            long paced = (System.nanoTime() - t0) / 1_000_000;
            s.finish();
            s.close();
            List<Map<String, Object>> frames = list(f.record(s.getSessionId()), "frames");
            double span = ((Number) frames.get(frames.size() - 1).get("t")).doubleValue() - ((Number) frames.get(0).get("t")).doubleValue();
            RealtimeSession s2 = c.connect(opts("A05-pacing"));
            t0 = System.nanoTime();
            sendAll(s2, audio("pcm_s16le_16000", 0), 1 << 20);
            long live = (System.nanoTime() - t0) / 1_000_000;
            s2.finish();
            s2.close();
            check(paced >= 1100 && span >= 1.0 && live <= 600, paced, span, live);
            assertEquals(f.record(s.getSessionId()).get("storedSha256"), f.record(s2.getSessionId()).get("storedSha256"));
        }
    }

    @Test
    void a05BoundedBufferBackpressure() {
        byte[] data = audio("pcm_s16le_16000", 0);
        try (RealtimeClient c = f.client(b -> b.maxBufferedMs(300))) {
            RealtimeSession s = c.connect(opts("A05-backpressure"));
            sendAll(s, data, 5000);
            Map<String, Object> st = s.getStats();
            FinishResult r = s.finish();
            s.close();
            check(num(st.get("peakBufferedSamples")) <= num(st.get("maxBufferedSamples")) && num(st.get("maxBufferedSamples")) == 4800, st);
            check(sha(data).equals(f.record(s.getSessionId()).get("storedSha256")) && "COMPLETED".equals(r.getStatus())
                    && num(s.getStats().get("bufferedSamples")) == 0, r, s.getStats());
        }
    }

    @Test
    void a05TwoSessionsIsolated() throws Exception {
        try (RealtimeClient c = f.client()) {
            RealtimeSession a = c.connect(opts("A05-concurrent"));
            RealtimeSession b = c.connect(new ConnectOptions().request(req("A05-concurrent", "mulaw_8000")));
            byte[] da = audio("pcm_s16le_16000", 0);
            byte[] db = audio("mulaw_8000", 0);
            CompletableFuture<FinishResult> fa = CompletableFuture.supplyAsync(() -> { sendAll(a, da, 999); return a.finish(); });
            CompletableFuture<FinishResult> fb = CompletableFuture.supplyAsync(() -> { sendAll(b, db, 999); return b.finish(); });
            FinishResult ra = fa.get(60, TimeUnit.SECONDS);
            FinishResult rb = fb.get(60, TimeUnit.SECONDS);
            check(num(a.getCursors().get("capturedSamples")) == 20000 && num(b.getCursors().get("capturedSamples")) == 10000,
                    a.getCursors(), b.getCursors());
            a.close();
            b.close();
            check(!ra.getSessionId().equals(rb.getSessionId()) && !ra.getTranscriptionId().equals(rb.getTranscriptionId()));
            check(sha(da).equals(f.record(a.getSessionId()).get("storedSha256")) && sha(db).equals(f.record(b.getSessionId()).get("storedSha256")));
        }
    }

    // ------------------------------------------------------------ A06

    private void lifecycle(String title, RealtimeSession.Via via) {
        byte[] data = audio("pcm_s16le_16000", 0);
        int q = data.length / 4 / 2 * 2;
        try (RealtimeClient c = f.client()) {
            RealtimeSession s = c.connect(opts(title));
            sendAll(s, Arrays.copyOfRange(data, 0, q), 4096);
            s.commit();
            assertEquals(1L, s.getCursors().get("epoch"));
            sendAll(s, Arrays.copyOfRange(data, q, 2 * q), 4096);
            s.pause(via);
            assertThrows(SessionStateException.class, () -> s.sendAudio(Arrays.copyOfRange(data, 2 * q, 3 * q)));
            s.resume(via);
            check((Long) s.getCursors().get("epoch") == 2 && (Long) s.getCursors().get("sampleOffset") == 0, s.getCursors());
            sendAll(s, Arrays.copyOfRange(data, 2 * q, data.length), 4096);
            FinishResult r = s.finish();
            FinishResult r2 = s.finish();
            SessionView view = c.getSession(s.getSessionId());
            SessionView rest = c.finishSession(s.getSessionId(), null);
            s.close();
            Map<String, Object> rec = f.record(s.getSessionId());
            List<Map<String, Object>> epochs = list(rec, "epochs");
            assertSame(r, r2);
            check(sha(data).equals(rec.get("storedSha256")) && epochs.size() == 2
                    && num(epochs.get(1).get("sent")) == (data.length - 2 * q) / 2 && num(rec.get("captured")) == data.length / 2, epochs);
            check("COMPLETED".equals(r.getStatus()) && "COMPLETED".equals(view.getStatus()) && "COMPLETED".equals(rest.getStatus())
                    && r.getTranscriptionId().equals(view.getTranscriptionId()) && r.getTranscriptionId().equals(rest.getTranscriptionId())
                    && r.getUsage().getBillableCharacters().equals(view.getUsage().getBillableCharacters())
                    && r.getUsage().getBillableCharacters().equals(rest.getUsage().getBillableCharacters()), r, view, rest);
            check(num(rec.get("finishCount")) == 2 && num(rec.get("settled")) == r.getUsage().getBillableCharacters()
                    && num(rec.get("settledCount")) == 1 && r.getLines().size() == 3, rec.get("finishCount"), r.getLines().size());
        }
    }

    @Test
    @Tag("compat")
    void a06ManualLifecycleWs() {
        lifecycle("A06-manual", RealtimeSession.Via.WS);
    }

    @Test
    void a06ManualLifecycleRest() {
        lifecycle("A06-rest-control", RealtimeSession.Via.REST);
    }

    @Test
    void a06Vad() {
        try (RealtimeClient c = f.client()) {
            RealtimeSession s = c.connect(new ConnectOptions().request(new SessionRequest().title("A06-vad")
                    .options(new SessionOptions().inputEncoding("pcm_s16le_16000").segmentCommitMode("vad"))));
            sendAll(s, audio("pcm_s16le_16000", 0), 1 << 20);
            FinishResult r = s.finish();
            s.close();
            check("COMPLETED".equals(r.getStatus()) && r.getLines().size() >= 3
                    && "vad".equals(((Map<?, ?>) f.record(s.getSessionId()).get("effectiveOptions")).get("segmentCommitMode")), r);
        }
    }

    // ------------------------------------------------------------ A07

    @Test
    @Tag("compat")
    @SuppressWarnings("unchecked")
    void a07Rotation() {
        Object[][] cases = {{"A07-rotate-mid", 0}, {"A07-rotate-last", 0}, {"A07-rotate-tail", 0}, {"A07-rotate-ack-delay", 777},
            {"A07-rotate-twice", 1600}, {"A07-old-epoch-late", 0}};
        try (RealtimeClient c = f.client()) {
            for (Object[] tc : cases) {
                String id = (String) tc[0];
                Map<String, Object> exp = (Map<String, Object>) Fx.scenario(id).get("expect");
                double secs = exp.get("inputSeconds") == null ? 0 : ((Number) exp.get("inputSeconds")).doubleValue();
                byte[] data = audio("pcm_s16le_16000", secs);
                RealtimeSession s = c.connect(opts(id));
                int chunk = (Integer) tc[1] == 0 ? data.length : (Integer) tc[1];
                sendAll(s, data, chunk);
                FinishResult r = s.finish();
                s.close();
                Map<String, Object> rec = f.record(s.getSessionId());
                long total = data.length / 2;
                long sum = 0;
                for (Map<String, Object> e : list(rec, "epochs")) {
                    sum += num(e.get("sent"));
                }
                int epochs = (int) num(exp.get("epochs"));
                check(sha(data).equals(rec.get("storedSha256")) && list(rec, "epochs").size() == epochs && sum == total
                        && Fx.maxClaim(rec) == total && num(rec.get("captured")) == total && "COMPLETED".equals(r.getStatus())
                        && (Long) s.getCursors().get("epoch") == epochs && num(s.getStats().get("rotations")) == epochs - 1,
                        id, list(rec, "epochs").size(), sum, Fx.maxClaim(rec), s.getStats());
            }
        }
    }

    // ------------------------------------------------------------ A08

    @Test
    @Tag("compat")
    void a08AllEventsAndSupplements() {
        List<RealtimeEvent> events = Collections.synchronizedList(new ArrayList<RealtimeEvent>());
        List<Throwable> cbErrs = Collections.synchronizedList(new ArrayList<Throwable>());
        byte[] data = audio("pcm_s16le_16000", 0);
        try (RealtimeClient c = f.client()) {
            RealtimeSession s = c.connect(opts("A08-events-all").onEvent(events::add).onCallbackError((e, ev) -> cbErrs.add(e)));
            sendAll(s, Arrays.copyOf(data, 16000), 16000);
            s.commit();
            sendAll(s, Arrays.copyOfRange(data, 16000, data.length), 1 << 20);
            FinishResult r = s.finish();
            s.close();
            Set<String> types = new HashSet<String>();
            List<Long> seqs = new ArrayList<Long>();
            synchronized (events) {
                for (RealtimeEvent e : events) {
                    types.add(e.getType());
                    seqs.add(e.getSequence());
                }
            }
            check(types.containsAll(RealtimeEvent.TYPES) && types.size() == 10, types);
            List<Long> sorted = new ArrayList<Long>(seqs);
            Collections.sort(sorted);
            check(sorted.equals(seqs) && seqs.get(0) == 1, seqs);
            List<TranscriptLine> lines = r.getLines();
            check(lines.size() == 2 && "line_1".equals(lines.get(0).getId()) && "line_2".equals(lines.get(1).getId()), lines);
            TranscriptLine l1 = lines.get(0);
            check(l1.isAlignmentArrived() && "en".equals(l1.getLanguageCode()) && !l1.getWords().isEmpty()
                    && l1.getWords().get(0).getStartMs() == 0, l1);
            check(l1.isEntitiesArrived() && l1.getEntities().isEmpty() && l1.isRevisionArrived()
                    && "HELLO WORLD".equals(l1.getRevisedText()), l1);
            Notice missing = null;
            Set<String> codes = new HashSet<String>();
            for (Notice n : r.getNotices()) {
                codes.add(n.getCode());
                if ("SUPPLEMENT_NOT_RECEIVED".equals(n.getCode())) {
                    missing = n;
                }
            }
            check(codes.contains("PROCESSING_STORAGE_NOT_APPLIED") && missing != null
                    && "language".equals(missing.getSupplements().get(0)) && "line_1".equals(missing.getLineIds().get(0)), r.getNotices());
            check("COMPLETED".equals(r.getStatus()) && cbErrs.isEmpty() && "TEMPORARILY_UNAVAILABLE".equals(r.getServerErrors().get(0)),
                    r.getStatus(), cbErrs, r.getServerErrors());
        }
    }

    private FinishResult run(RealtimeClient c, String title) {
        RealtimeSession s = c.connect(opts(title));
        sendAll(s, audio("pcm_s16le_16000", 0), 1 << 20);
        FinishResult r = s.finish();
        s.close();
        return r;
    }

    @Test
    void a08EntitiesNotArrivedVersusArrived() {
        try (RealtimeClient c = f.client()) {
            FinishResult r1 = run(c, "L-basic");
            FinishResult r2 = run(c, "A08-entities-some");
            check(!r1.getLines().get(0).isEntitiesArrived() && r1.getLines().get(0).getEntities() == null, r1.getLines());
            check(r2.getLines().get(0).isEntitiesArrived() && "Alice".equals(r2.getLines().get(0).getEntities().get(0).get("text")));
        }
    }

    @Test
    void a08TransportCloseAfterTerminalIsNotFailure() {
        try (RealtimeClient c = f.client()) {
            RealtimeSession s = c.connect(opts("A08-close-after-completed"));
            sendAll(s, audio("pcm_s16le_16000", 0), 1 << 20);
            FinishResult r = s.finish();
            Fx.sleep(200);
            s.close();
            check("COMPLETED".equals(r.getStatus()) && !r.isViaRest() && r.getCloseoutReason() == null
                    && num(f.record(s.getSessionId()).get("restFinishCount")) == 0, r);
        }
    }
}
