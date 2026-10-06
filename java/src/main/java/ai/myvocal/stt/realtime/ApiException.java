package ai.myvocal.stt.realtime;

import java.util.Collections;
import java.util.Map;

/** A MyVocal envelope with {@code code != 1} (HTTP 200 included). */
public class ApiException extends MyVocalException {
    private static final long serialVersionUID = 1L;
    private final long code;
    private final String errorCode;
    private final boolean retryable;
    private final String action;
    private final String requestId;
    private final transient Map<String, Object> details;

    public ApiException(long code, String errorCode, String message, boolean retryable, String action,
                        String requestId, Map<String, Object> details) {
        super("MyVocal API error code=" + code + " errorCode=" + errorCode + " retryable=" + retryable
                + " requestId=" + requestId);
        this.code = code;
        this.errorCode = errorCode;
        this.retryable = retryable;
        this.action = action;
        this.requestId = requestId;
        this.details = details == null ? Collections.<String, Object>emptyMap() : details;
    }

    public long getCode() { return code; }
    public String getErrorCode() { return errorCode; }
    public boolean isRetryable() { return retryable; }
    public String getAction() { return action; }
    public String getRequestId() { return requestId; }
    public Map<String, Object> getDetails() { return details; }

    @SuppressWarnings("unchecked")
    static ApiException fromEnvelope(Map<String, Object> env) {
        long code = Json.asLong(env.get("code"), 0L);
        String message = Json.asString(env.get("message"));
        Map<String, Object> data = env.get("data") instanceof Map ? (Map<String, Object>) env.get("data")
                : Collections.<String, Object>emptyMap();
        String errorCode = Json.asString(data.get("errorCode"));
        boolean retryable = Boolean.TRUE.equals(data.get("retryable"));
        String action = Json.asString(data.get("action"));
        String requestId = Json.asString(data.get("requestId"));
        Map<String, Object> details = data.get("details") instanceof Map ? (Map<String, Object>) data.get("details") : null;
        if (code == 401) {
            return new AuthenticationException(code, errorCode == null ? "UNAUTHORIZED" : errorCode, message, requestId);
        }
        if ("INPUT_INVALID".equals(errorCode)) {
            return new InputInvalidException(code, errorCode, message, retryable, action, requestId, details);
        }
        if ("IDEMPOTENCY_CONFLICT".equals(errorCode)) {
            return new IdempotencyConflictException(code, errorCode, message, retryable, action, requestId, details);
        }
        if ("INSUFFICIENT_CHARACTERS".equals(errorCode)) {
            return new InsufficientCharactersException(code, errorCode, message, retryable, action, requestId, details);
        }
        if ("PROJECT_NOT_FOUND".equals(errorCode)) {
            return new NotFoundException(code, errorCode, message, retryable, action, requestId, details);
        }
        return new ApiException(code, errorCode, message, retryable, action, requestId, details);
    }
}
