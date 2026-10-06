package ai.myvocal.stt.realtime;

/** Unacknowledged audio made no acknowledgement progress within {@link Timeouts#getAckMillis()}. */
public class AckTimeoutException extends MyVocalException {
    private static final long serialVersionUID = 1L;

    public AckTimeoutException(String message) { super(message); }
}
