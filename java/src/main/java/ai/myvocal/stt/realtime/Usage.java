package ai.myvocal.stt.realtime;

import java.util.Map;

/** A {@code usage.updated} payload or a view's usage. 64-bit fields are decimal strings on the
 * wire and {@code Long} here; {@code null} means absent (never 0). */
public final class Usage {
    private final Long estimatedCharacters;
    private final Long billableCharacters;
    private final Long elapsedMs;
    private final Long sentSamples;
    private final Long receivedSamples;
    private final Long capturedSamples;
    private final Boolean accepting;
    private final Long epoch;
    private final boolean rotate;
    private final String errorCode;

    Usage(Map<String, Object> p) {
        estimatedCharacters = Json.asLongOrNull(p.get("estimatedCharacters"));
        billableCharacters = Json.asLongOrNull(p.get("billableCharacters"));
        elapsedMs = Json.asLongOrNull(p.get("elapsedMs"));
        sentSamples = Json.asLongOrNull(p.get("sentSamples"));
        receivedSamples = Json.asLongOrNull(p.get("receivedSamples"));
        capturedSamples = Json.asLongOrNull(p.get("capturedSamples"));
        accepting = Json.asBool(p.get("accepting"));
        epoch = Json.optLong(p.get("epoch"));
        rotate = Boolean.TRUE.equals(p.get("rotate"));
        errorCode = Json.asString(p.get("errorCode"));
    }

    public Long getEstimatedCharacters() { return estimatedCharacters; }
    public Long getBillableCharacters() { return billableCharacters; }
    public Long getElapsedMs() { return elapsedMs; }
    /** Samples acknowledged in the active epoch. */
    public Long getSentSamples() { return sentSamples; }
    /** Session-wide transport receipt (not a model proof). */
    public Long getReceivedSamples() { return receivedSamples; }
    public Long getCapturedSamples() { return capturedSamples; }
    public Boolean getAccepting() { return accepting; }
    public Long getEpoch() { return epoch; }
    public boolean isRotate() { return rotate; }
    public String getErrorCode() { return errorCode; }

    @Override
    public String toString() {
        return "Usage{billable=" + billableCharacters + ", estimated=" + estimatedCharacters + ", elapsedMs=" + elapsedMs
                + ", sent=" + sentSamples + ", received=" + receivedSamples + ", captured=" + capturedSamples
                + ", epoch=" + epoch + ", accepting=" + accepting + (rotate ? ", rotate" : "")
                + (errorCode != null ? ", errorCode=" + errorCode : "") + "}";
    }
}
