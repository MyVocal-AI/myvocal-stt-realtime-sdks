package ai.myvocal.stt.realtime;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** One server event. {@link #getSequence()} is per socket connection ({@link #getConnection()}
 * counts the connections this SDK opened for the session); it is not a cross-connection cursor.
 * {@link #toString()} never prints the payload. */
public final class RealtimeEvent {
    public static final String SESSION_READY = "session.ready";
    public static final String TRANSCRIPT_DELTA = "transcript.delta";
    public static final String TRANSCRIPT_FINAL = "transcript.final";
    public static final String TRANSCRIPT_ALIGNMENT = "transcript.alignment";
    public static final String TRANSCRIPT_REVISION = "transcript.revision";
    public static final String TRANSCRIPT_ENTITIES = "transcript.entities";
    public static final String USAGE_UPDATED = "usage.updated";
    public static final String SESSION_NOTICE = "session.notice";
    public static final String SESSION_COMPLETED = "session.completed";
    public static final String SESSION_ERROR = "session.error";

    /** The ten public server events. */
    public static final List<String> TYPES = Collections.unmodifiableList(Arrays.asList(SESSION_READY, TRANSCRIPT_DELTA,
            TRANSCRIPT_FINAL, TRANSCRIPT_ALIGNMENT, TRANSCRIPT_REVISION, TRANSCRIPT_ENTITIES, USAGE_UPDATED,
            SESSION_NOTICE, SESSION_COMPLETED, SESSION_ERROR));

    private final String type;
    private final Map<String, Object> payload;
    private final Long sequence;
    private final String eventId;
    private final String sessionId;
    private final int connection;

    RealtimeEvent(String type, Map<String, Object> payload, Long sequence, String eventId, String sessionId, int connection) {
        this.type = type;
        this.payload = Collections.unmodifiableMap(payload);
        this.sequence = sequence;
        this.eventId = eventId;
        this.sessionId = sessionId;
        this.connection = connection;
    }

    public String getType() { return type; }
    public Map<String, Object> getPayload() { return payload; }
    public Long getSequence() { return sequence; }
    public String getEventId() { return eventId; }
    public String getSessionId() { return sessionId; }
    public int getConnection() { return connection; }

    /** Usage of {@code usage.updated}, else null. */
    public Usage getUsage() { return USAGE_UPDATED.equals(type) ? new Usage(payload) : null; }

    /** Line of {@code transcript.final}, else null. */
    public TranscriptLine getLine() { return TRANSCRIPT_FINAL.equals(type) ? TranscriptLine.fromWire(payload) : null; }

    /** View of {@code session.ready} / {@code session.completed}, else null. */
    public SessionView getView() {
        return SESSION_READY.equals(type) || SESSION_COMPLETED.equals(type) ? new SessionView(payload) : null;
    }

    /** Notice of {@code session.notice}, else null. */
    public Notice getNotice() { return SESSION_NOTICE.equals(type) ? new Notice(payload) : null; }

    /** errorCode of {@code session.error} (or usage carrying one). */
    public String getErrorCode() { return Json.asString(payload.get("errorCode")); }

    /** Text of {@code transcript.delta} / {@code transcript.final}. */
    public String getText() {
        return TRANSCRIPT_DELTA.equals(type) || TRANSCRIPT_FINAL.equals(type) ? Json.asString(payload.get("text")) : null;
    }

    @Override
    public String toString() {
        return "RealtimeEvent{type=" + type + ", sequence=" + sequence + ", connection=" + connection + "}";
    }
}
