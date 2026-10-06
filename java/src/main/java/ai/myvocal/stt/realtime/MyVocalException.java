package ai.myvocal.stt.realtime;

/** Base class of every exception thrown by this SDK. Messages never contain the access key, a
 * ticket, an authentication header or a signed URL. */
public class MyVocalException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public MyVocalException(String message) {
        super(message);
    }

    public MyVocalException(String message, Throwable cause) {
        super(message, cause);
    }
}
