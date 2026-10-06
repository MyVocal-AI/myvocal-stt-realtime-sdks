package ai.myvocal.stt.realtime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** The REST/WS session resource. {@link #toString()} never prints transcript text. */
public final class SessionView {
    private final String sessionId;
    private final String transcriptionId;
    private final String status;
    private final String modelId;
    private final String streamUrl;
    private final long epoch;
    private final boolean accepting;
    private final String partial;
    private final List<TranscriptLine> lines;
    private final List<Notice> notices;
    private final Usage usage;
    private final Map<String, Object> effectiveOptions;
    private final String createdAt;
    private final String updatedAt;

    SessionView(Map<String, Object> p) {
        sessionId = Json.asString(p.get("sessionId"));
        transcriptionId = Json.asString(p.get("transcriptionId"));
        status = Json.asString(p.get("status"));
        modelId = Json.asString(p.get("modelId"));
        streamUrl = Json.asString(p.get("streamUrl"));
        epoch = Json.asLong(p.get("epoch"), 0L);
        accepting = Boolean.TRUE.equals(p.get("accepting"));
        partial = Json.asString(p.get("partial"));
        List<TranscriptLine> ls = new ArrayList<TranscriptLine>();
        for (Object o : Json.asList(p.get("lines"))) {
            if (o instanceof Map) {
                ls.add(TranscriptLine.fromWire(Json.asMap(o)));
            }
        }
        lines = Collections.unmodifiableList(ls);
        List<Notice> ns = new ArrayList<Notice>();
        for (Object o : Json.asList(p.get("notices"))) {
            if (o instanceof Map) {
                ns.add(new Notice(Json.asMap(o)));
            }
        }
        notices = Collections.unmodifiableList(ns);
        usage = new Usage(Json.asMap(p.get("usage")));
        effectiveOptions = Collections.unmodifiableMap(Json.asMap(p.get("effectiveOptions")));
        createdAt = Json.asString(p.get("createdAt"));
        updatedAt = Json.asString(p.get("updatedAt"));
    }

    public String getSessionId() { return sessionId; }
    public String getTranscriptionId() { return transcriptionId; }
    /** RECORDING, PAUSED, COMPLETED, PARTIAL or FAILED. */
    public String getStatus() { return status; }
    public String getModelId() { return modelId; }
    public String getStreamUrl() { return streamUrl; }
    public long getEpoch() { return epoch; }
    public boolean isAccepting() { return accepting; }
    public String getPartial() { return partial; }
    public List<TranscriptLine> getLines() { return lines; }
    public List<Notice> getNotices() { return notices; }
    public Usage getUsage() { return usage; }
    public Map<String, Object> getEffectiveOptions() { return effectiveOptions; }
    public String getCreatedAt() { return createdAt; }
    public String getUpdatedAt() { return updatedAt; }

    /** COMPLETED, PARTIAL or FAILED. */
    public boolean isTerminal() {
        return "COMPLETED".equals(status) || "PARTIAL".equals(status) || "FAILED".equals(status);
    }

    @Override
    public String toString() {
        return "SessionView{sessionId=" + sessionId + ", transcriptionId=" + transcriptionId + ", status=" + status
                + ", epoch=" + epoch + ", accepting=" + accepting + ", lines=" + lines.size() + ", usage=" + usage + "}";
    }
}
