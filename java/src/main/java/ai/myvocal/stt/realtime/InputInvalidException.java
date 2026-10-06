package ai.myvocal.stt.realtime;

import java.util.Map;

/** INPUT_INVALID: malformed or unsupported field or combination. Never retried. */
public class InputInvalidException extends ApiException {
    private static final long serialVersionUID = 1L;

    public InputInvalidException(long code, String errorCode, String message, boolean retryable, String action,
                    String requestId, Map<String, Object> details) {
        super(code, errorCode, message, retryable, action, requestId, details);
    }
}
