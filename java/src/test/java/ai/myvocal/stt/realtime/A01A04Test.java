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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import okhttp3.Interceptor;
import okhttp3.OkHttpClient;

class A01A04Test {
    private final Fx f = Fx.plain();

    // ------------------------------------------------------------ A01

    @Test
    @Tag("compat")
    void a01BlockingConnectAudioFinish() {
        List<String> seen = Collections.synchronizedList(new ArrayList<String>());
        byte[] data = audio("pcm_s16le_16000", 0);
        try (RealtimeClient c = f.client()) {
            RealtimeSession s = c.connect(opts("L-basic").onEvent(e -> seen.add(e.getType())));
            sendAll(s, data, 3333);
            FinishResult r = s.finish();
            s.close();
            Map<String, Object> rec = f.record(r.getSessionId());
            check("COMPLETED".equals(r.getStatus()) && r.getTranscriptionId().equals(rec.get("transcriptionId"))
                    && sha(data).equals(rec.get("storedSha256")), r);
            Fx.sleep(100);
            check(seen.contains(RealtimeEvent.SESSION_READY) && seen.contains(RealtimeEvent.SESSION_COMPLETED), seen);
        }
    }

    @Test
    @Tag("compat")
    void a01FutureAndListener() throws Exception {
        List<String> finals = Collections.synchronizedList(new ArrayList<String>());
        AtomicInteger ready = new AtomicInteger();
        try (RealtimeClient c = f.client()) {
            CompletableFuture<RealtimeSession> fut = c.connectAsync(opts("L-basic").listener(new RealtimeListener() {
                @Override
                public void onReady(SessionView view) {
                    ready.incrementAndGet();
                }

                @Override
                public void onTranscriptFinal(TranscriptLine line, RealtimeEvent event) {
                    finals.add(line.getId());
                }
            }));
            RealtimeSession s = fut.get(30, TimeUnit.SECONDS);
            sendAll(s, audio("pcm_s16le_16000", 0), 32000);
            FinishResult r = s.finishAsync().get(30, TimeUnit.SECONDS);
            s.close();
            Fx.sleep(100);
            check("COMPLETED".equals(r.getStatus()) && ready.get() == 1 && !finals.isEmpty(), r, finals);
        }
    }

    // ------------------------------------------------------------ A02

    private static OkHttpClient counting(AtomicInteger n) {
        return new OkHttpClient.Builder().proxy(java.net.Proxy.NO_PROXY).addInterceptor((Interceptor) chain -> {
            n.incrementAndGet();
            return chain.proceed(chain.request());
        }).build();
    }

    @Test
    void a02CreateResponseLostReusesKeyAndBody() {
        try (RealtimeClient c = f.client()) {
            RealtimeSession s = c.connect(opts("A02-create-lost"));
            sendAll(s, audio("pcm_s16le_16000", 0.3), 9600);
            FinishResult r = s.finish();
            s.close();
            List<Map<String, Object>> recs = f.byTitle("A02-create-lost");
            assertEquals(1, recs.size());
            List<Map<String, Object>> creates = new ArrayList<Map<String, Object>>();
            for (Map<String, Object> call : list(recs.get(0), "restCalls")) {
                if (((String) call.get("path")).startsWith("create")) {
                    creates.add(call);
                }
            }
            check(creates.size() == 2 && "create(replay)".equals(creates.get(1).get("path"))
                    && creates.get(0).get("idempotencyKey").equals(creates.get(1).get("idempotencyKey")), creates);
            assertEquals(recs.get(0).get("sessionId"), r.getSessionId());
        }
    }

    @Test
    void a02EnvelopeErrorNonRetryable() {
        AtomicInteger n = new AtomicInteger();
        try (RealtimeClient c = f.client(b -> b.httpClient(counting(n)))) {
            InsufficientCharactersException e = assertThrows(InsufficientCharactersException.class,
                    () -> c.createSession(req("A02-envelope-nonretryable")));
            check(e.getCode() == 47210 && !e.isRetryable() && n.get() == 1, e, n);
        }
    }

    @Test
    void a02EnvelopeErrorRetryableBounded() {
        AtomicInteger n = new AtomicInteger();
        try (RealtimeClient c = f.client(b -> b.httpClient(counting(n)).maxCreateRetries(2))) {
            ApiException e = assertThrows(ApiException.class, () -> c.createSession(req("A02-envelope-retryable")));
            check("TEMPORARILY_UNAVAILABLE".equals(e.getErrorCode()) && e.isRetryable() && n.get() == 3, e, n);
        }
    }

    @Test
    void a02InvalidKeyNotRetried() {
        AtomicInteger n = new AtomicInteger();
        try (RealtimeClient c = RealtimeClient.builder("wrong-key").baseUrl(f.base).httpClient(counting(n)).build()) {
            AuthenticationException e = assertThrows(AuthenticationException.class, () -> c.connect(opts("L-basic")));
            check(e.getCode() == 401 && n.get() == 1, e, n);
            assertFalse(String.valueOf(e).contains("wrong-key") || c.toString().contains("wrong-key"));
        }
    }

    @Test
    void a02RestHelpersAndIdentity() {
        try (RealtimeClient c = f.client()) {
            SessionView v = c.createSession(new SessionRequest().languageHint("en").title("A02-rest-helpers")
                    .options(new SessionOptions().inputEncoding("mulaw_8000")));
            check(v.getSessionId().startsWith("rts_") && v.getTranscriptionId().startsWith("stt_")
                    && "mulaw_8000".equals(v.getEffectiveOptions().get("inputEncoding"))
                    && num(v.getEffectiveOptions().get("sampleRateHz")) == 8000, v);
            SessionView got = c.getSession(v.getSessionId());
            check(got.getTranscriptionId().equals(v.getTranscriptionId()) && "RECORDING".equals(got.getStatus()), got);
            Ticket tk = c.createTicket(v.getSessionId());
            check(!tk.getTicket().isEmpty() && tk.getSessionId().equals(v.getSessionId()) && tk.getExpiresInSeconds() == 60, tk);
            assertFalse(tk.toString().contains(tk.getTicket()));
            SessionView p = c.pauseSession(v.getSessionId(), 0L);
            check(!p.isAccepting() && "PAUSED".equals(p.getStatus()), p);
            SessionView rs = c.resumeSession(v.getSessionId(), 0L);
            check(rs.getEpoch() == 2 && rs.isAccepting(), rs);
            SessionView done = c.finishSession(v.getSessionId(), 0L);
            SessionView again = c.finishSession(v.getSessionId(), 0L);
            check("FAILED".equals(done.getStatus()) && again.getTranscriptionId().equals(done.getTranscriptionId())
                    && again.getStatus().equals(done.getStatus()), done, again);
            Map<String, Object> rec = f.record(v.getSessionId());
            Map<String, Object> want = new LinkedHashMap<String, Object>();
            want.put("languageHint", "en");
            want.put("title", "A02-rest-helpers");
            want.put("options", Collections.singletonMap("inputEncoding", "mulaw_8000"));
            check(deepEq(rec.get("rawRequest"), want), rec.get("rawRequest"));
            for (Map<String, Object> call : list(rec, "restCalls")) {
                check(Boolean.TRUE.equals(call.get("hasAccessKeyHeader")) && !Boolean.TRUE.equals(call.get("queryHasKey")), call);
            }
        }
    }

    @Test
    void a02SocketIdentityHeaderNotUrl() {
        try (RealtimeClient c = f.client()) {
            RealtimeSession s = c.connect(opts("L-basic"));
            sendAll(s, audio("pcm_s16le_16000", 0.2), 6400);
            s.finish();
            s.close();
            Map<String, Object> conn = list(f.record(s.getSessionId()), "connections").get(0);
            check(Boolean.TRUE.equals(conn.get("hasAccessKeyHeader")) && !Boolean.TRUE.equals(conn.get("queryHasKey"))
                    && "".equals(conn.get("query"))
                    && ((String) conn.get("userAgent")).startsWith("myvocal-stt-realtime-java/"), conn);
        }
    }

    // ------------------------------------------------------------ A03

    @SuppressWarnings("unchecked")
    static SessionOptions optionsFromWire(Map<String, Object> m) {
        SessionOptions o = new SessionOptions();
        for (Map.Entry<String, Object> e : m.entrySet()) {
            Object v = e.getValue();
            switch (e.getKey()) {
                case "inputEncoding": o.inputEncoding((String) v); break;
                case "additionalLanguageHints": o.additionalLanguageHints(strings(v)); break;
                case "segmentCommitMode": o.segmentCommitMode((String) v); break;
                case "speechDetectionThreshold": o.speechDetectionThreshold(((Number) v).doubleValue()); break;
                case "commitSilenceMs": o.commitSilenceMs(((Number) v).doubleValue()); break;
                case "minimumSpeechMs": o.minimumSpeechMs(((Number) v).intValue()); break;
                case "minimumSilenceMs": o.minimumSilenceMs(((Number) v).intValue()); break;
                case "includeAlignment": o.includeAlignment((Boolean) v); break;
                case "includeDetectedLanguage": o.includeDetectedLanguage((Boolean) v); break;
                case "vocabularyHints":
                    if ("51x".equals(v)) {
                        List<String> terms = new ArrayList<String>();
                        for (int i = 0; i < 51; i++) {
                            terms.add("term" + i);
                        }
                        o.vocabularyHints(terms);
                    } else {
                        o.vocabularyHints(strings(v));
                    }
                    break;
                case "removeDisfluencies": o.removeDisfluencies((Boolean) v); break;
                case "entityCategories": o.entityCategories(strings(v)); break;
                case "rewriteInstruction": o.rewriteInstruction((String) v); break;
                case "suppressBackgroundSpeech": o.suppressBackgroundSpeech((Boolean) v); break;
                case "heartbeatIntervalMs": o.heartbeatIntervalMs(((Number) v).intValue()); break;
                case "processingContentStorage": o.processingContentStorage((Boolean) v); break;
                default: throw new IllegalArgumentException(e.getKey());
            }
        }
        return o;
    }

    @SuppressWarnings("unchecked")
    private static List<String> strings(Object v) {
        List<String> out = new ArrayList<String>();
        for (Object x : (List<Object>) v) {
            out.add((String) x);
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    static boolean deepEq(Object a, Object b) {
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() == ((Number) b).doubleValue();
        }
        if (a instanceof Map && b instanceof Map) {
            Map<String, Object> x = (Map<String, Object>) a;
            Map<String, Object> y = (Map<String, Object>) b;
            if (!x.keySet().equals(y.keySet())) {
                return false;
            }
            for (String k : x.keySet()) {
                if (!deepEq(x.get(k), y.get(k))) {
                    return false;
                }
            }
            return true;
        }
        if (a instanceof List && b instanceof List) {
            List<Object> x = (List<Object>) a;
            List<Object> y = (List<Object>) b;
            if (x.size() != y.size()) {
                return false;
            }
            for (int i = 0; i < x.size(); i++) {
                if (!deepEq(x.get(i), y.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return a == null ? b == null : a.equals(b);
    }

    @Test
    @SuppressWarnings("unchecked")
    void a03ParametersReachWireExactly() {
        try (RealtimeClient c = f.client()) {
            for (String sid : Arrays.asList("A03-params-full-a", "A03-params-full-b", "A03-falsy")) {
                Map<String, Object> body = (Map<String, Object>) Fx.scenario(sid).get("request");
                SessionRequest r = new SessionRequest().title((String) body.get("title"))
                        .options(optionsFromWire((Map<String, Object>) body.get("options")));
                if (body.get("languageHint") != null) {
                    r.languageHint((String) body.get("languageHint"));
                }
                SessionView v = c.createSession(r);
                c.finishSession(v.getSessionId(), null);
                Map<String, Object> rec = f.record(v.getSessionId());
                check(deepEq(rec.get("rawRequest"), body), sid, rec.get("rawRequest"), body);
                Map<String, Object> eff = (Map<String, Object>) rec.get("effectiveOptions");
                for (Map.Entry<String, Object> e : ((Map<String, Object>) body.get("options")).entrySet()) {
                    if (!"inputEncoding".equals(e.getKey())) {
                        check(deepEq(eff.get(e.getKey()), e.getValue()), sid, e.getKey(), eff.get(e.getKey()));
                    }
                }
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void a03IllegalCombinationsAreInputErrorsNotRetried() {
        for (String sid : Arrays.asList("A03-illegal-rewrite-entities", "A03-illegal-suppress-alignment", "A03-illegal-vocab-51")) {
            AtomicInteger n = new AtomicInteger();
            try (RealtimeClient c = f.client(b -> b.httpClient(counting(n)))) {
                Map<String, Object> body = (Map<String, Object>) Fx.scenario(sid).get("request");
                assertThrows(InputInvalidException.class, () -> c.connect(new ConnectOptions().request(new SessionRequest()
                        .title(sid).options(optionsFromWire((Map<String, Object>) body.get("options"))))));
                check(n.get() == 1 && f.byTitle(sid).isEmpty(), sid, n);
            }
        }
    }

    @Test
    void a03PrecedingTextOnAudioEvent() {
        try (RealtimeClient c = f.client()) {
            RealtimeSession s = c.connect(opts("A03-preceding-text"));
            s.sendAudio(audio("pcm_s16le_16000", 0.2), "Earlier context.");
            s.finish();
            s.close();
            List<Map<String, Object>> frames = list(f.record(s.getSessionId()), "frames");
            check("Earlier context.".equals(frames.get(0).get("precedingText")) && frames.get(1).get("precedingText") == null,
                    frames.get(0).get("precedingText"));
        }
    }

    @Test
    void a03OmittedVersusFalseAndZero() {
        Map<String, Object> w = new SessionOptions().includeAlignment(false).minimumSpeechMs(0).speechDetectionThreshold(0.0).toWire();
        check(w.size() == 3 && Boolean.FALSE.equals(w.get("includeAlignment")) && num(w.get("minimumSpeechMs")) == 0
                && ((Number) w.get("speechDetectionThreshold")).doubleValue() == 0.0, w);
        assertTrue(new SessionOptions().toWire().isEmpty());
        assertEquals("{\"speechDetectionThreshold\":0,\"minimumSpeechMs\":0,\"includeAlignment\":false}", Json.write(w));
    }

    // ------------------------------------------------------------ A04

    static Map<String, List<byte[]>> chunkings(byte[] data, int bps) {
        Map<String, List<byte[]>> out = new LinkedHashMap<String, List<byte[]>>();
        out.put("whole", Collections.singletonList(data));
        List<byte[]> c128 = new ArrayList<byte[]>();
        for (int i = 0; i < data.length; i += 128 * bps) {
            c128.add(Arrays.copyOfRange(data, i, Math.min(i + 128 * bps, data.length)));
        }
        List<byte[]> irr = new ArrayList<byte[]>();
        Random rnd = new Random(20261006);
        int[] sizes = {1, 3, 7, 77, 255, 1001, 4097};
        for (int i = 0; i < data.length;) {
            int n = sizes[rnd.nextInt(sizes.length)];
            irr.add(Arrays.copyOfRange(data, i, Math.min(i + n, data.length)));
            i += n;
        }
        out.put("128-sample", c128);
        out.put("irregular", irr);
        return out;
    }

    @Test
    void a04EncodingsAndChunkings() {
        try (RealtimeClient c = f.client()) {
            for (AudioFormat fmt : AudioFormat.ENCODINGS.values()) {
                byte[] data = audio(fmt.getEncoding(), 0);
                for (Map.Entry<String, List<byte[]>> ch : chunkings(data, fmt.getBytesPerSample()).entrySet()) {
                    RealtimeSession s = c.connect(new ConnectOptions().request(req("A04-encoding", fmt.getEncoding())));
                    for (byte[] b : ch.getValue()) {
                        s.sendAudio(b);
                    }
                    FinishResult r = s.finish();
                    s.close();
                    Map<String, Object> rec = f.record(s.getSessionId());
                    long samples = data.length / fmt.getBytesPerSample();
                    String where = fmt.getEncoding() + "/" + ch.getKey();
                    check(sha(data).equals(rec.get("storedSha256")) && num(rec.get("captured")) == samples
                            && "COMPLETED".equals(r.getStatus()) && Fx.maxClaim(rec) == samples, where, rec.get("captured"));
                    long frame = fmt.getSampleRateHz() / 10 * fmt.getBytesPerSample();
                    List<Map<String, Object>> frames = list(rec, "frames");
                    for (int i = 0; i < frames.size(); i++) {
                        long b = num(frames.get(i).get("bytes"));
                        check(!(i < frames.size() - 1 && b != frame) && b > 0 && b <= frame, where, i, b);
                    }
                }
            }
        }
    }

    @Test
    void a04ResidualHalfSample() {
        byte[] data = audio("pcm_s16le_16000", 0.3);
        try (RealtimeClient c = f.client()) {
            RealtimeSession s = c.connect(opts("A04-encoding"));
            byte[] odd = Arrays.copyOf(data, data.length + 1);
            odd[data.length] = 1;
            s.sendAudio(odd);
            assertThrows(AudioFormatException.class, s::finish);
            assertTrue(s.discardPartialSample());
            FinishResult r = s.finish();
            s.close();
            check("COMPLETED".equals(r.getStatus()) && sha(data).equals(f.record(s.getSessionId()).get("storedSha256")), r);
        }
    }

    @Test
    void a04WavHeaderAndMismatch() throws IOException {
        Path wav = Fx.ROOT.resolve("fixtures/audio/tone_pcm_s16le_16000.wav");
        try (RealtimeClient c = f.client()) {
            RealtimeSession s = c.connect(new ConnectOptions().request(req("A04-encoding", "pcm_s16le_8000")));
            assertThrows(AudioFormatException.class, () -> s.sendFile(wav, false));
            s.finish();
            s.close();
            RealtimeSession s2 = c.connect(opts("A04-encoding"));
            s2.sendFile(wav, false);
            s2.finish();
            s2.close();
            assertEquals(sha(audio("pcm_s16le_16000", 0)), f.record(s2.getSessionId()).get("storedSha256"));
        }
    }
}
