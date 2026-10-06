package ai.myvocal.stt.realtime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The 16 public {@code options} fields. {@code null} means omitted (server default);
 * {@code false} and {@code 0} are sent as given. Fluent setters return {@code this}. */
public final class SessionOptions {
    private String inputEncoding;
    private List<String> additionalLanguageHints;
    private String segmentCommitMode;
    private Double speechDetectionThreshold;
    private Double commitSilenceMs;
    private Integer minimumSpeechMs;
    private Integer minimumSilenceMs;
    private Boolean includeAlignment;
    private Boolean includeDetectedLanguage;
    private List<String> vocabularyHints;
    private Boolean removeDisfluencies;
    private List<String> entityCategories;
    private String rewriteInstruction;
    private Boolean suppressBackgroundSpeech;
    private Integer heartbeatIntervalMs;
    private Boolean processingContentStorage;

    public SessionOptions inputEncoding(String v) { this.inputEncoding = v; return this; }
    public SessionOptions additionalLanguageHints(List<String> v) { this.additionalLanguageHints = copy(v); return this; }
    public SessionOptions additionalLanguageHints(String... v) { return additionalLanguageHints(Arrays.asList(v)); }
    /** {@code "manual"} or {@code "vad"}. */
    public SessionOptions segmentCommitMode(String v) { this.segmentCommitMode = v; return this; }
    public SessionOptions speechDetectionThreshold(Double v) { this.speechDetectionThreshold = v; return this; }
    public SessionOptions commitSilenceMs(Double v) { this.commitSilenceMs = v; return this; }
    public SessionOptions minimumSpeechMs(Integer v) { this.minimumSpeechMs = v; return this; }
    public SessionOptions minimumSilenceMs(Integer v) { this.minimumSilenceMs = v; return this; }
    public SessionOptions includeAlignment(Boolean v) { this.includeAlignment = v; return this; }
    public SessionOptions includeDetectedLanguage(Boolean v) { this.includeDetectedLanguage = v; return this; }
    /** At most 50 hints. */
    public SessionOptions vocabularyHints(List<String> v) { this.vocabularyHints = copy(v); return this; }
    public SessionOptions vocabularyHints(String... v) { return vocabularyHints(Arrays.asList(v)); }
    public SessionOptions removeDisfluencies(Boolean v) { this.removeDisfluencies = v; return this; }
    public SessionOptions entityCategories(List<String> v) { this.entityCategories = copy(v); return this; }
    public SessionOptions entityCategories(String... v) { return entityCategories(Arrays.asList(v)); }
    public SessionOptions rewriteInstruction(String v) { this.rewriteInstruction = v; return this; }
    public SessionOptions suppressBackgroundSpeech(Boolean v) { this.suppressBackgroundSpeech = v; return this; }
    public SessionOptions heartbeatIntervalMs(Integer v) { this.heartbeatIntervalMs = v; return this; }
    public SessionOptions processingContentStorage(Boolean v) { this.processingContentStorage = v; return this; }

    public String getInputEncoding() { return inputEncoding; }
    public List<String> getAdditionalLanguageHints() { return additionalLanguageHints; }
    public String getSegmentCommitMode() { return segmentCommitMode; }
    public Double getSpeechDetectionThreshold() { return speechDetectionThreshold; }
    public Double getCommitSilenceMs() { return commitSilenceMs; }
    public Integer getMinimumSpeechMs() { return minimumSpeechMs; }
    public Integer getMinimumSilenceMs() { return minimumSilenceMs; }
    public Boolean getIncludeAlignment() { return includeAlignment; }
    public Boolean getIncludeDetectedLanguage() { return includeDetectedLanguage; }
    public List<String> getVocabularyHints() { return vocabularyHints; }
    public Boolean getRemoveDisfluencies() { return removeDisfluencies; }
    public List<String> getEntityCategories() { return entityCategories; }
    public String getRewriteInstruction() { return rewriteInstruction; }
    public Boolean getSuppressBackgroundSpeech() { return suppressBackgroundSpeech; }
    public Integer getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
    public Boolean getProcessingContentStorage() { return processingContentStorage; }

    private static List<String> copy(List<String> v) {
        return v == null ? null : new ArrayList<String>(v);
    }

    /** The wire object (camelCase keys, omitted fields absent). */
    public Map<String, Object> toWire() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        put(m, "inputEncoding", inputEncoding);
        put(m, "additionalLanguageHints", additionalLanguageHints);
        put(m, "segmentCommitMode", segmentCommitMode);
        put(m, "speechDetectionThreshold", speechDetectionThreshold);
        put(m, "commitSilenceMs", commitSilenceMs);
        put(m, "minimumSpeechMs", minimumSpeechMs);
        put(m, "minimumSilenceMs", minimumSilenceMs);
        put(m, "includeAlignment", includeAlignment);
        put(m, "includeDetectedLanguage", includeDetectedLanguage);
        put(m, "vocabularyHints", vocabularyHints);
        put(m, "removeDisfluencies", removeDisfluencies);
        put(m, "entityCategories", entityCategories);
        put(m, "rewriteInstruction", rewriteInstruction);
        put(m, "suppressBackgroundSpeech", suppressBackgroundSpeech);
        put(m, "heartbeatIntervalMs", heartbeatIntervalMs);
        put(m, "processingContentStorage", processingContentStorage);
        return m;
    }

    private static void put(Map<String, Object> m, String k, Object v) {
        if (v != null) {
            m.put(k, v);
        }
    }

    @Override
    public String toString() {
        return "SessionOptions" + toWire().keySet();
    }
}
