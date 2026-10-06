package ai.myvocal.stt.realtime;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** A {@code session.notice} (also listed in the session view). */
public final class Notice {
    private final String code;
    private final String message;
    private final List<String> supplements;
    private final List<String> lineIds;
    private final Map<String, Object> raw;

    Notice(Map<String, Object> p) {
        code = Json.asString(p.get("code"));
        message = Json.asString(p.get("message"));
        supplements = strings(p.get("supplements"));
        lineIds = strings(p.get("lineIds"));
        raw = p;
    }

    private static List<String> strings(Object v) {
        List<String> out = new ArrayList<String>();
        for (Object o : Json.asList(v)) {
            out.add(String.valueOf(o));
        }
        return out;
    }

    public String getCode() { return code; }
    public String getMessage() { return message; }
    public List<String> getSupplements() { return supplements; }
    public List<String> getLineIds() { return lineIds; }
    public Map<String, Object> getRaw() { return raw; }

    @Override
    public String toString() {
        return "Notice{code=" + code + ", supplements=" + supplements + ", lineIds=" + lineIds + "}";
    }
}
