package ai.myvocal.stt.realtime;

/** A {@code session.error} (or usage errorCode) that stopped the audio stream. */
public class ServerEventException extends MyVocalException {
    private static final long serialVersionUID = 1L;
    private final String errorCode;
    private final String sessionId;

    public ServerEventException(String errorCode, String sessionId) {
        super("session.error " + errorCode + " (sessionId=" + sessionId + ")");
        this.errorCode = errorCode;
        this.sessionId = sessionId;
    }

    public String getErrorCode() { return errorCode; }
    public String getSessionId() { return sessionId; }
}
