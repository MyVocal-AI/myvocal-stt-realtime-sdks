package ai.myvocal.stt.realtime;

/** The bounded send buffer stayed full (no acknowledgement progress) past the configured time. */
public class BackpressureTimeoutException extends MyVocalException {
    private static final long serialVersionUID = 1L;

    public BackpressureTimeoutException(String message) { super(message); }
}
