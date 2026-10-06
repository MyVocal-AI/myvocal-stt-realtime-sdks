package ai.myvocal.stt.realtime;

import java.util.Map;

/** PROJECT_NOT_FOUND: unknown session or not owned by this key. */
public class NotFoundException extends ApiException {
    private static final long serialVersionUID = 1L;

    public NotFoundException(long code, String errorCode, String message, boolean retryable, String action,
                    String requestId, Map<String, Object> details) {
        super(code, errorCode, message, retryable, action, requestId, details);
    }
}
