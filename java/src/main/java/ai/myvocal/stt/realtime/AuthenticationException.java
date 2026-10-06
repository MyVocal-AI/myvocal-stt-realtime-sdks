package ai.myvocal.stt.realtime;

/** Missing, unknown or disabled access key (envelope {@code code = 401}). Never retried. */
public class AuthenticationException extends ApiException {
    private static final long serialVersionUID = 1L;

    public AuthenticationException(long code, String errorCode, String message, String requestId) {
        super(code, errorCode, message, false, null, requestId, null);
    }
}
