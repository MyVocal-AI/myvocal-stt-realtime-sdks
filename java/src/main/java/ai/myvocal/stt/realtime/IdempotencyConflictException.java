package ai.myvocal.stt.realtime;

import java.util.Map;

/** The Idempotency-Key was already bound to a different create request. */
public class IdempotencyConflictException extends ApiException {
    private static final long serialVersionUID = 1L;

    public IdempotencyConflictException(long code, String errorCode, String message, boolean retryable, String action,
                    String requestId, Map<String, Object> details) {
        super(code, errorCode, message, retryable, action, requestId, details);
    }
}
