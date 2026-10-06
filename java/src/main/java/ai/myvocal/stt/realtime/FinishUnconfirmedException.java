package ai.myvocal.stt.realtime;

/** The end of the session could not be confirmed (REST unreachable or refused during close-out).
 * Keep {@link #getSessionId()} and call {@code getSession}/{@code finishSession} later. */
public class FinishUnconfirmedException extends MyVocalException {
    private static final long serialVersionUID = 1L;
    private final String sessionId;

    public FinishUnconfirmedException(String message, String sessionId, Throwable cause) {
        super(message + " (sessionId=" + sessionId + ")", cause);
        this.sessionId = sessionId;
    }

    public String getSessionId() { return sessionId; }
}
