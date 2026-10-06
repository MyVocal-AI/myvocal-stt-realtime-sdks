package ai.myvocal.stt.realtime;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Snapshot merged from events: delta replaces the partial text; final, alignment, revision and
 * entities update lines by id and never open a line for a supplement. Thread-safe. */
public final class Transcript {
    private final Map<String, TranscriptLine> lines = new LinkedHashMap<String, TranscriptLine>();
    private String partial = "";

    public synchronized String getPartial() { return partial; }

    /** Copies of the merged lines in commit order. */
    public synchronized List<TranscriptLine> getLines() {
        List<TranscriptLine> out = new ArrayList<TranscriptLine>();
        for (TranscriptLine l : lines.values()) {
            out.add(l.copy());
        }
        return out;
    }

    /** Revised (else original) line texts joined by spaces. */
    public String getText() { return join(getLines()); }

    static String join(List<TranscriptLine> ls) {
        StringBuilder b = new StringBuilder();
        for (TranscriptLine l : ls) {
            String t = l.getRevisedText() != null && !l.getRevisedText().isEmpty() ? l.getRevisedText() : l.getText();
            if (t != null && !t.isEmpty()) {
                if (b.length() > 0) {
                    b.append(' ');
                }
                b.append(t);
            }
        }
        return b.toString();
    }

    private TranscriptLine get(String id, Map<String, Object> p) {
        TranscriptLine l = lines.get(id);
        if (l == null) {
            l = new TranscriptLine(id);
            l.text = Json.asString(p.get("text"));
            lines.put(id, l);
        }
        return l;
    }

    synchronized void apply(String type, Map<String, Object> p) {
        if (RealtimeEvent.TRANSCRIPT_DELTA.equals(type)) {
            String t = Json.asString(p.get("text"));
            partial = t == null ? "" : t;
        } else if (RealtimeEvent.TRANSCRIPT_FINAL.equals(type)) {
            TranscriptLine n = TranscriptLine.fromWire(p);
            TranscriptLine old = lines.get(n.getId());
            if (old == null) {
                lines.put(n.getId(), n);
            } else {
                old.text = n.text;
            }
        } else if (RealtimeEvent.TRANSCRIPT_REVISION.equals(type)) {
            TranscriptLine l = get(String.valueOf(p.get("id")), p);
            l.revisedText = Json.asString(p.get("revisedText"));
            l.revisionArrived = true;
        } else if (RealtimeEvent.TRANSCRIPT_ENTITIES.equals(type)) {
            TranscriptLine l = get(String.valueOf(p.get("id")), p);
            List<Map<String, Object>> e = new ArrayList<Map<String, Object>>();
            for (Object o : Json.asList(p.get("entities"))) {
                e.add(Json.asMap(o));
            }
            l.entities = e;
            l.entitiesArrived = true;
        } else if (RealtimeEvent.TRANSCRIPT_ALIGNMENT.equals(type)) {
            TranscriptLine l = get(String.valueOf(p.get("id")), p);
            l.languageCode = Json.asString(p.get("languageCode"));
            l.languageProbability = Json.asDouble(p.get("languageProbability"));
            l.words = Word.list(p.get("words"));
            l.characterAlignments = Word.list(p.get("characterAlignments"));
            l.alignmentArrived = true;
        }
    }

    synchronized void merge(SessionView v) {
        for (TranscriptLine vl : v.getLines()) {
            TranscriptLine l = lines.get(vl.getId());
            if (l == null) {
                lines.put(vl.getId(), vl.copy());
                continue;
            }
            l.text = vl.text;
            if (vl.revisedText != null) {
                l.revisedText = vl.revisedText;
                l.revisionArrived = true;
            }
            if (vl.languageCode != null) {
                l.languageCode = vl.languageCode;
                l.languageProbability = vl.languageProbability;
            }
            if (!vl.words.isEmpty()) {
                l.words = vl.words;
            }
            if (!vl.characterAlignments.isEmpty()) {
                l.characterAlignments = vl.characterAlignments;
            }
            if (vl.entitiesArrived) {
                l.entities = vl.entities;
                l.entitiesArrived = true;
            }
        }
    }
}
