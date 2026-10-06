package ai.myvocal.stt.realtime;

import java.util.Collections;
import java.util.List;

/** The server's real terminal outcome. {@link #getStatus()} is COMPLETED, PARTIAL or FAILED
 * exactly as returned. {@link #getCloseoutReason()} is set when the result came from the REST
 * close-out path ({@code DISCONNECTED}, {@code ACK_TIMEOUT}, {@code FINISH_TIMEOUT}).
 * {@link #toString()} never prints transcript text. */
public final class FinishResult {
    private final String status;
    private final String sessionId;
    private final String transcriptionId;
    private final Usage usage;
    private final List<Notice> notices;
    private final List<TranscriptLine> lines;
    private final SessionView view;
    private final boolean viaRest;
    private final String closeoutReason;
    private final List<String> serverErrors;
    private final long callbackErrors;
    private final long droppedEvents;

    FinishResult(SessionView view, String sessionId, String transcriptionId, List<TranscriptLine> lines, boolean viaRest,
                 String closeoutReason, List<String> serverErrors, long callbackErrors, long droppedEvents) {
        this.status = view.getStatus();
        this.sessionId = sessionId;
        this.transcriptionId = transcriptionId;
        this.usage = view.getUsage();
        this.notices = view.getNotices();
        this.lines = Collections.unmodifiableList(lines);
        this.view = view;
        this.viaRest = viaRest;
        this.closeoutReason = closeoutReason;
        this.serverErrors = Collections.unmodifiableList(serverErrors);
        this.callbackErrors = callbackErrors;
        this.droppedEvents = droppedEvents;
    }

    public String getStatus() { return status; }
    public String getSessionId() { return sessionId; }
    public String getTranscriptionId() { return transcriptionId; }
    public Usage getUsage() { return usage; }
    public List<Notice> getNotices() { return notices; }
    public List<TranscriptLine> getLines() { return lines; }
    public SessionView getView() { return view; }
    public boolean isViaRest() { return viaRest; }
    public String getCloseoutReason() { return closeoutReason; }
    public List<String> getServerErrors() { return serverErrors; }
    public long getCallbackErrors() { return callbackErrors; }
    public long getDroppedEvents() { return droppedEvents; }

    /** Revised (else original) line texts joined by spaces. */
    public String getText() { return Transcript.join(lines); }

    @Override
    public String toString() {
        return "FinishResult{status=" + status + ", sessionId=" + sessionId + ", transcriptionId=" + transcriptionId
                + ", viaRest=" + viaRest + ", closeoutReason=" + closeoutReason + ", lines=" + lines.size()
                + ", usage=" + usage + "}";
    }
}
