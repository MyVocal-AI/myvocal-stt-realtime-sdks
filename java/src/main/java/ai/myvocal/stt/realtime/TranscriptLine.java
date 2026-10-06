package ai.myvocal.stt.realtime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** One committed line, merged by id. {@link #getEntities()} is {@code null} until the entities
 * result arrives; an empty list with {@link #isEntitiesArrived()} means it arrived empty.
 * {@link #toString()} never prints transcript text. */
public final class TranscriptLine {
    private final String id;
    volatile String text;
    volatile String revisedText;
    volatile String languageCode;
    volatile Double languageProbability;
    volatile List<Map<String, Object>> entities;
    volatile List<Word> words = Collections.emptyList();
    volatile List<Word> characterAlignments = Collections.emptyList();
    volatile boolean entitiesArrived;
    volatile boolean revisionArrived;
    volatile boolean alignmentArrived;

    TranscriptLine(String id) {
        this.id = id;
    }

    @SuppressWarnings("unchecked")
    static TranscriptLine fromWire(Map<String, Object> p) {
        TranscriptLine l = new TranscriptLine(String.valueOf(p.get("id")));
        l.text = Json.asString(p.get("text"));
        l.revisedText = Json.asString(p.get("revisedText"));
        l.revisionArrived = l.revisedText != null;
        l.languageCode = Json.asString(p.get("languageCode"));
        l.languageProbability = Json.asDouble(p.get("languageProbability"));
        List<Object> ents = Json.asList(p.get("entities"));
        if (!ents.isEmpty()) {
            List<Map<String, Object>> e = new ArrayList<Map<String, Object>>();
            for (Object o : ents) {
                e.add(Json.asMap(o));
            }
            l.entities = e;
            l.entitiesArrived = true;
        }
        l.words = Word.list(p.get("words"));
        l.characterAlignments = Word.list(p.get("characterAlignments"));
        return l;
    }

    TranscriptLine copy() {
        TranscriptLine c = new TranscriptLine(id);
        c.text = text;
        c.revisedText = revisedText;
        c.languageCode = languageCode;
        c.languageProbability = languageProbability;
        c.entities = entities;
        c.words = words;
        c.characterAlignments = characterAlignments;
        c.entitiesArrived = entitiesArrived;
        c.revisionArrived = revisionArrived;
        c.alignmentArrived = alignmentArrived;
        return c;
    }

    public String getId() { return id; }
    public String getText() { return text; }
    public String getRevisedText() { return revisedText; }
    public String getLanguageCode() { return languageCode; }
    public Double getLanguageProbability() { return languageProbability; }
    public List<Map<String, Object>> getEntities() { return entities; }
    public List<Word> getWords() { return words; }
    public List<Word> getCharacterAlignments() { return characterAlignments; }
    public boolean isEntitiesArrived() { return entitiesArrived; }
    public boolean isRevisionArrived() { return revisionArrived; }
    public boolean isAlignmentArrived() { return alignmentArrived; }

    @Override
    public String toString() {
        return "TranscriptLine{id=" + id + ", entitiesArrived=" + entitiesArrived + ", revisionArrived=" + revisionArrived
                + ", alignmentArrived=" + alignmentArrived + "}";
    }
}
