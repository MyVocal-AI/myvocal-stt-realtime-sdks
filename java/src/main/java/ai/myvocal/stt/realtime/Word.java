package ai.myvocal.stt.realtime;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** A word or character alignment. {@code null} times stay {@code null}. */
public final class Word {
    private final String text;
    private final Long startMs;
    private final Long endMs;

    Word(Map<String, Object> p) {
        text = Json.asString(p.get("text"));
        startMs = Json.asLongOrNull(p.get("startMs"));
        endMs = Json.asLongOrNull(p.get("endMs"));
    }

    public String getText() { return text; }
    public Long getStartMs() { return startMs; }
    public Long getEndMs() { return endMs; }

    static List<Word> list(Object raw) {
        List<Word> out = new ArrayList<Word>();
        for (Object o : Json.asList(raw)) {
            out.add(new Word(Json.asMap(o)));
        }
        return out;
    }

    @Override
    public String toString() {
        return "Word{startMs=" + startMs + ", endMs=" + endMs + "}";
    }
}
