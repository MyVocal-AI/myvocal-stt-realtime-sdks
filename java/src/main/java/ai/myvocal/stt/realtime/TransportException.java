package ai.myvocal.stt.realtime;

/** The request or socket failed below the MyVocal envelope (network, TLS, proxy, HTTP status). */
public class TransportException extends MyVocalException {
    private static final long serialVersionUID = 1L;
    private final int status;
    private volatile String sessionId;

    public TransportException(String message, int status, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    /** HTTP status when known, else 0. */
    public int getStatus() { return status; }

    /** Session created before the socket failed (closed out quietly), if any. */
    public String getSessionId() { return sessionId; }

    void setSessionId(String sessionId) { this.sessionId = sessionId; }
}
