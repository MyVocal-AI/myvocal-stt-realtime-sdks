package ai.myvocal.stt.realtime;

/** The operation is not allowed in the session's current local state (paused, finished, closed). */
public class SessionStateException extends MyVocalException {
    private static final long serialVersionUID = 1L;

    public SessionStateException(String message) { super(message); }
}
