package ai.myvocal.stt.realtime;

/** The socket ended before the session finished. No audio is accepted any more; the session is
 * closed out over REST (see {@link FinishResult#getCloseoutReason()}). */
public class ConnectionLostException extends SessionStateException {
    private static final long serialVersionUID = 1L;
    private final String sessionId;

    public ConnectionLostException(String message, String sessionId) {
        super(message + " (sessionId=" + sessionId + ")");
        this.sessionId = sessionId;
    }

    public String getSessionId() { return sessionId; }
}
