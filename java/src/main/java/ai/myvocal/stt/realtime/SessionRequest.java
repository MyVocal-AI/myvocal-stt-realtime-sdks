package ai.myvocal.stt.realtime;

import java.util.LinkedHashMap;
import java.util.Map;

/** Body of {@code POST /sessions}: top-level {@code languageHint}/{@code title} plus {@code options}. */
public final class SessionRequest {
    private String languageHint;
    private String title;
    private SessionOptions options;

    public SessionRequest languageHint(String v) { this.languageHint = v; return this; }
    public SessionRequest title(String v) { this.title = v; return this; }
    public SessionRequest options(SessionOptions v) { this.options = v; return this; }

    public String getLanguageHint() { return languageHint; }
    public String getTitle() { return title; }
    public SessionOptions getOptions() { return options; }

    String requestedEncoding() {
        return options == null ? null : options.getInputEncoding();
    }

    /** The wire body. */
    public Map<String, Object> toWire() {
        Map<String, Object> b = new LinkedHashMap<String, Object>();
        if (languageHint != null) {
            b.put("languageHint", languageHint);
        }
        if (title != null) {
            b.put("title", title);
        }
        if (options != null) {
            b.put("options", options.toWire());
        }
        return b;
    }
}
