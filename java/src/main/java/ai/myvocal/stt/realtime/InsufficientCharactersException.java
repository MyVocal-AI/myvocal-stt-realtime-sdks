package ai.myvocal.stt.realtime;

import java.util.Map;

/** INSUFFICIENT_CHARACTERS: the Characters balance is exhausted. Never retried. */
public class InsufficientCharactersException extends ApiException {
    private static final long serialVersionUID = 1L;

    public InsufficientCharactersException(long code, String errorCode, String message, boolean retryable, String action,
                    String requestId, Map<String, Object> details) {
        super(code, errorCode, message, retryable, action, requestId, details);
    }
}
