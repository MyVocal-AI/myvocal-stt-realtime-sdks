import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import ai.myvocal.stt.realtime.ApiException;
import ai.myvocal.stt.realtime.ConnectOptions;
import ai.myvocal.stt.realtime.ConnectionLostException;
import ai.myvocal.stt.realtime.FinishResult;
import ai.myvocal.stt.realtime.Notice;
import ai.myvocal.stt.realtime.RealtimeClient;
import ai.myvocal.stt.realtime.RealtimeSession;
import ai.myvocal.stt.realtime.SessionOptions;
import ai.myvocal.stt.realtime.SessionRequest;
import ai.myvocal.stt.realtime.SessionView;
import ai.myvocal.stt.realtime.Ticket;
import ai.myvocal.stt.realtime.TranscriptLine;

/** Real-API validation with the Java SDK (plan v1.1 section 7): R1 pcm_s16le_16000 manual + mulaw_8000 VAD,
 * R2 language + alignment, R3 commit/continue, pause/resume, ticket helper, repeated finish,
 * R4 client-only disconnect close-out and 75 s idle with ping. Compiled and run against the INSTALLED JAR.
 * Reads MYVOCAL_ACCESS_KEY / MYVOCAL_API_BASE from the environment only; writes sanitized records.
 * <pre>java -cp <installed jar + deps>;. LiveJava <audio dir> <out dir> [idleSeconds]</pre> */
public final class LiveJava {
    static final List<Map<String, Object>> RECORDS = new ArrayList<Map<String, Object>>();
    static Path out;

    public static void main(String[] args) throws Exception {
        String key = System.getenv("MYVOCAL_ACCESS_KEY");
        if (key == null || key.isEmpty()) {
            System.err.println("MYVOCAL_ACCESS_KEY is not set");
            System.exit(2);
        }
        String base = System.getenv("MYVOCAL_API_BASE");
        Path audio = Paths.get(args[0]);
        out = Paths.get(args[1]).resolve("live-java.json");
        int idle = args.length > 2 ? Integer.parseInt(args[2]) : 75;
        String prod = "https://api.myvocal.ai";
        String b0 = (base == null || base.isEmpty()) ? prod : base.replaceAll("/+$", "");
        if (!b0.equals(prod) && !key.startsWith("fixture-")) {
            System.err.println("refusing: a non-fixture key may only be sent to " + prod);
            System.exit(2);
        }
        RealtimeClient.Builder b = RealtimeClient.builder(key).baseUrl(b0);
        try (final RealtimeClient client = b.build()) {
            final Path a = audio;
            final int idl = idle;
            guard("R1-pcm_s16le_16000-manual-language+alignment", () -> r1(client, a, "pcm_s16le_16000", "manual", 2, true));
            guard("R1-mulaw_8000-vad", () -> r1(client, a, "mulaw_8000", "vad", 1, false));
            guard("R3-lifecycle-commit-pause-resume-ticket-refinish", () -> r3(client, a));
            guard("R4-client-disconnect-closeout", () -> r4Disconnect(client, a));
            guard("R4-idle-" + idl + "s-ping", () -> r4Idle(client, a, idl));
        }
        int bad = 0;
        for (Map<String, Object> r : RECORDS) {
            if (!Boolean.TRUE.equals(r.get("ok"))) {
                bad++;
            }
        }
        System.out.println("records: " + RECORDS.size() + ", needing review: " + bad);
        System.exit(bad == 0 ? 0 : 1);
    }

    interface Step {
        void run() throws Exception;
    }

    /** Runs one scenario; an exception becomes a sanitized error record and the run continues. */
    static void guard(String scenario, Step step) throws IOException {
        try {
            step.run();
        } catch (Exception e) {
            Map<String, Object> m = new java.util.LinkedHashMap<String, Object>();
            m.put("scenario", scenario);
            m.put("ok", false);
            m.put("status", null);
            m.put("transcriptionId", null);
            m.put("error", e.getClass().getSimpleName());
            if (e instanceof ApiException) {
                m.put("errorCode", ((ApiException) e).getErrorCode());
            }
            String msg = String.valueOf(e.getMessage());
            m.put("message", msg.length() > 300 ? msg.substring(0, 300) : msg);
            add(m);
        }
    }

    static byte[] raw(Path audio, String enc) throws IOException {
        return Files.readAllBytes(audio.resolve("speech_" + enc + ".raw"));
    }

    static SessionRequest req(String scen, String enc, String mode) {
        return new SessionRequest().title("sdk-live java " + scen).languageHint("en")
                .options(new SessionOptions().inputEncoding(enc).segmentCommitMode(mode));
    }

    static void r1(RealtimeClient client, Path audio, String enc, String mode, int bps, boolean langAlign) throws IOException {
        String scen = "R1-" + enc + "-" + mode + (langAlign ? "-language+alignment" : "");
        byte[] data = raw(audio, enc);
        SessionRequest r = req(scen, enc, mode);
        if (langAlign) {
            r.getOptions().includeAlignment(true).includeDetectedLanguage(true);
        }
        try (RealtimeSession s = client.connect(new ConnectOptions().request(r))) {
            if ("manual".equals(mode)) {
                int half = data.length / 2 / bps * bps;
                s.sendStream(new ByteArrayInputStream(Arrays.copyOf(data, half)), true);
                s.commit();
                s.sendStream(new ByteArrayInputStream(Arrays.copyOfRange(data, half, data.length)), true);
            } else {
                s.sendStream(new ByteArrayInputStream(data), true);
            }
            FinishResult res = s.finish();
            long exp = data.length / bps;
            Map<String, Object> m = summarize(scen, enc, res, exp);
            boolean align = false;
            List<String> langs = new ArrayList<String>();
            for (TranscriptLine l : res.getLines()) {
                align |= l.isAlignmentArrived();
                if (l.getLanguageCode() != null && !langs.contains(l.getLanguageCode())) {
                    langs.add(l.getLanguageCode());
                }
            }
            m.put("alignmentArrived", align);
            m.put("languageCodes", langs);
            m.put("ok", eq(res.getUsage().getCapturedSamples(), exp) && eq(res.getUsage().getReceivedSamples(), exp)
                    && "COMPLETED".equals(res.getStatus()) && !res.getText().isEmpty());
            add(m);
        }
    }

    static void r3(RealtimeClient client, Path audio) throws Exception {
        String scen = "R3-lifecycle-commit-pause-resume-ticket-refinish";
        String enc = "pcm_s16le_16000";
        byte[] data = raw(audio, enc);
        int q = data.length / 4 / 2 * 2;
        try (RealtimeSession s = client.connect(new ConnectOptions().request(req(scen, enc, "manual")))) {
            Ticket t = client.createTicket(s.getSessionId());
            boolean ticketOk = t.getTicket() != null && !t.getTicket().isEmpty() && s.getSessionId().equals(t.getSessionId());
            s.sendStream(new ByteArrayInputStream(Arrays.copyOf(data, q)), true);
            s.commit();
            s.sendStream(new ByteArrayInputStream(Arrays.copyOfRange(data, q, 2 * q)), true);
            s.pause(RealtimeSession.Via.REST);
            long e1 = (Long) s.getCursors().get("epoch");
            Thread.sleep(2000);
            s.resume();
            long e2 = (Long) s.getCursors().get("epoch");
            s.sendStream(new ByteArrayInputStream(Arrays.copyOfRange(data, 2 * q, data.length)), true);
            FinishResult r = s.finish();
            FinishResult r2 = s.finish();
            SessionView again = client.finishSession(s.getSessionId(), null);
            SessionView view = client.getSession(s.getSessionId());
            Map<String, Object> m = summarize(scen, enc, r, data.length / 2);
            m.put("ticketIssued", ticketOk);
            m.put("epochs", Arrays.asList(e1, e2));
            m.put("repeatSameObject", r == r2);
            m.put("restRefinishBillable", again.getUsage().getBillableCharacters());
            m.put("getAfterBillable", view.getUsage().getBillableCharacters());
            m.put("ok", ticketOk && e2 == e1 + 1 && r == r2 && "COMPLETED".equals(r.getStatus())
                    && r.getTranscriptionId().equals(again.getTranscriptionId())
                    && eq(again.getUsage().getBillableCharacters(), r.getUsage().getBillableCharacters())
                    && eq(view.getUsage().getBillableCharacters(), r.getUsage().getBillableCharacters()));
            add(m);
        }
    }

    static void r4Disconnect(RealtimeClient client, Path audio) throws Exception {
        String scen = "R4-client-disconnect-closeout";
        String enc = "pcm_s16le_16000";
        byte[] data = raw(audio, enc);
        int half = data.length / 2 / 2 * 2;
        try (RealtimeSession s = client.connect(new ConnectOptions().request(req(scen, enc, "manual")))) {
            s.sendStream(new ByteArrayInputStream(Arrays.copyOf(data, half)), true);
            s.commit();
            s.abortConnection();
            boolean lost = false;
            try {
                s.sendAudio(Arrays.copyOfRange(data, half, data.length));
            } catch (ConnectionLostException e) {
                lost = true;
            }
            FinishResult r = s.finish();
            SessionView view = client.getSession(s.getSessionId());
            Map<String, Object> m = summarize(scen, enc, r, half / 2);
            m.put("audioRejectedAfterLoss", lost);
            m.put("getStatus", view.getStatus());
            m.put("ok", lost && r.isViaRest() && r.getStatus().equals(view.getStatus()));
            add(m);
        }
    }

    static void r4Idle(RealtimeClient client, Path audio, int idle) throws Exception {
        String scen = "R4-idle-" + idle + "s-ping";
        String enc = "pcm_s16le_16000";
        byte[] data = raw(audio, enc);
        int head = 32000;
        try (RealtimeSession s = client.connect(new ConnectOptions().request(req(scen, enc, "manual")))) {
            s.sendAudio(Arrays.copyOf(data, head));
            s.commit();
            Object before = s.getCursors().get("capturedSamples");
            Thread.sleep(idle * 1000L);
            Object after = s.getCursors().get("capturedSamples");
            String state = s.getState();
            s.sendStream(new ByteArrayInputStream(Arrays.copyOfRange(data, head, data.length)), true);
            FinishResult r = s.finish();
            Map<String, Object> m = summarize(scen, enc, r, data.length / 2);
            m.put("idleSeconds", idle);
            m.put("capturedBeforeIdle", before);
            m.put("capturedAfterIdle", after);
            m.put("stateAfterIdle", state);
            m.put("ok", before.equals(after) && "open".equals(state) && !r.isViaRest() && "COMPLETED".equals(r.getStatus())
                    && eq(r.getUsage().getReceivedSamples(), data.length / 2));
            add(m);
        }
    }

    static boolean eq(Long a, long b) {
        return a != null && a == b;
    }

    static boolean eq(Long a, Long b) {
        return a != null && a.equals(b);
    }

    static Map<String, Object> summarize(String scen, String enc, FinishResult r, long expected) {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("scenario", scen);
        m.put("encoding", enc);
        m.put("sessionId", r.getSessionId());
        m.put("transcriptionId", r.getTranscriptionId());
        m.put("status", r.getStatus());
        m.put("capturedSamples", r.getUsage().getCapturedSamples());
        m.put("receivedSamples", r.getUsage().getReceivedSamples());
        m.put("expectedSamples", expected);
        m.put("billableCharacters", r.getUsage().getBillableCharacters());
        m.put("lines", r.getLines().size());
        m.put("textChars", r.getText().length());
        List<String> codes = new ArrayList<String>();
        for (Notice n : r.getNotices()) {
            codes.add(n.getCode());
        }
        m.put("notices", codes);
        m.put("viaRest", r.isViaRest());
        m.put("closeoutReason", r.getCloseoutReason());
        return m;
    }

    static void add(Map<String, Object> m) throws IOException {
        RECORDS.add(m);
        System.out.println(m.get("scenario") + " " + (Boolean.TRUE.equals(m.get("ok")) ? "OK" : "CHECK") + " " + m.get("status")
                + " " + m.get("transcriptionId"));
        StringBuilder sb = new StringBuilder("{\"language\":\"java\",\"updated\":\"")
                .append(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(new Date())).append("\",\"sdk\":\"ai.myvocal:myvocal-stt-realtime:")
                .append(RealtimeClient.VERSION).append(" on Java ").append(System.getProperty("java.version")).append("\",\"records\":[");
        for (int i = 0; i < RECORDS.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            json(sb, RECORDS.get(i));
        }
        sb.append("]}");
        Files.createDirectories(out.getParent());
        try (Writer w = Files.newBufferedWriter(out, StandardCharsets.UTF_8)) {
            w.write(sb.toString());
        }
    }

    @SuppressWarnings("unchecked")
    static void json(StringBuilder sb, Object v) {
        if (v == null) {
            sb.append("null");
        } else if (v instanceof Map) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<String, Object> e : ((Map<String, Object>) v).entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                json(sb, e.getKey());
                sb.append(':');
                json(sb, e.getValue());
            }
            sb.append('}');
        } else if (v instanceof List) {
            sb.append('[');
            boolean first = true;
            for (Object o : (List<Object>) v) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                json(sb, o);
            }
            sb.append(']');
        } else if (v instanceof Number || v instanceof Boolean) {
            sb.append(v);
        } else {
            sb.append('"');
            for (char ch : v.toString().toCharArray()) {
                if (ch == '"' || ch == '\\') {
                    sb.append('\\').append(ch);
                } else if (ch < 0x20) {
                    sb.append(String.format("\\u%04x", (int) ch));
                } else {
                    sb.append(ch);
                }
            }
            sb.append('"');
        }
    }
}
