"""Request, view, event and result models.

Wire rules: 64-bit sample/usage/millisecond fields are decimal strings on the wire and
Python ``int`` here (never float); ``None`` stays ``None`` (never 0). ``epoch``,
booleans and ``sequence`` keep their protocol JSON types.
"""
from dataclasses import dataclass, field, fields
from typing import Any, Dict, List, Optional

TERMINAL_STATUSES = ("COMPLETED", "PARTIAL", "FAILED")

EVENT_TYPES = (
    "session.ready", "transcript.delta", "transcript.final", "transcript.alignment",
    "transcript.revision", "transcript.entities", "usage.updated", "session.notice",
    "session.completed", "session.error",
)


def parse_int(value: Any) -> Optional[int]:
    """Decimal string (or JSON integer) -> int; ``None`` stays ``None``. Rejects floats."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("boolean is not an integer field")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text or not (text.isdigit() or (text[0] == "-" and text[1:].isdigit())):
            raise ValueError("not a decimal integer: %r" % value)
        return int(text)
    raise ValueError("not an integer field: %r" % type(value).__name__)


@dataclass
class SessionOptions:
    """The 16 public ``options`` fields. ``None`` means *omitted* (server default);
    ``False``/``0`` are sent as given."""
    input_encoding: Optional[str] = None
    additional_language_hints: Optional[List[str]] = None
    segment_commit_mode: Optional[str] = None
    speech_detection_threshold: Optional[float] = None
    commit_silence_ms: Optional[float] = None
    minimum_speech_ms: Optional[int] = None
    minimum_silence_ms: Optional[int] = None
    include_alignment: Optional[bool] = None
    include_detected_language: Optional[bool] = None
    vocabulary_hints: Optional[List[str]] = None
    remove_disfluencies: Optional[bool] = None
    entity_categories: Optional[List[str]] = None
    rewrite_instruction: Optional[str] = None
    suppress_background_speech: Optional[bool] = None
    heartbeat_interval_ms: Optional[int] = None
    processing_content_storage: Optional[bool] = None

    _WIRE = {
        "input_encoding": "inputEncoding", "additional_language_hints": "additionalLanguageHints",
        "segment_commit_mode": "segmentCommitMode", "speech_detection_threshold": "speechDetectionThreshold",
        "commit_silence_ms": "commitSilenceMs", "minimum_speech_ms": "minimumSpeechMs",
        "minimum_silence_ms": "minimumSilenceMs", "include_alignment": "includeAlignment",
        "include_detected_language": "includeDetectedLanguage", "vocabulary_hints": "vocabularyHints",
        "remove_disfluencies": "removeDisfluencies", "entity_categories": "entityCategories",
        "rewrite_instruction": "rewriteInstruction", "suppress_background_speech": "suppressBackgroundSpeech",
        "heartbeat_interval_ms": "heartbeatIntervalMs", "processing_content_storage": "processingContentStorage",
    }

    def to_wire(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for f in fields(self):
            value = getattr(self, f.name)
            if value is not None:
                out[self._WIRE[f.name]] = list(value) if isinstance(value, (list, tuple)) else value
        return out

    @classmethod
    def from_wire(cls, raw: Dict[str, Any]) -> "SessionOptions":
        reverse = {v: k for k, v in cls._WIRE.items()}
        unknown = [k for k in raw if k not in reverse]
        if unknown:
            raise ValueError("unknown option(s): %s" % ", ".join(unknown))
        return cls(**{reverse[k]: v for k, v in raw.items()})


@dataclass
class SessionRequest:
    """Body of ``POST /sessions``: top-level ``languageHint``/``title`` plus ``options``."""
    language_hint: Optional[str] = None
    title: Optional[str] = None
    options: Optional[SessionOptions] = None

    def to_wire(self) -> Dict[str, Any]:
        body: Dict[str, Any] = {}
        if self.language_hint is not None:
            body["languageHint"] = self.language_hint
        if self.title is not None:
            body["title"] = self.title
        if self.options is not None:
            body["options"] = self.options.to_wire()
        return body

    @property
    def input_encoding(self) -> str:
        enc = self.options.input_encoding if self.options else None
        return enc or "pcm_s16le_16000"


@dataclass
class Usage:
    estimated_characters: Optional[int] = None
    billable_characters: Optional[int] = None
    elapsed_ms: Optional[int] = None
    sent_samples: Optional[int] = None
    received_samples: Optional[int] = None
    captured_samples: Optional[int] = None
    accepting: Optional[bool] = None
    epoch: Optional[int] = None
    rotate: bool = False
    error_code: Optional[str] = None

    @classmethod
    def from_wire(cls, p: Dict[str, Any]) -> "Usage":
        return cls(
            estimated_characters=parse_int(p.get("estimatedCharacters")),
            billable_characters=parse_int(p.get("billableCharacters")),
            elapsed_ms=parse_int(p.get("elapsedMs")),
            sent_samples=parse_int(p.get("sentSamples")),
            received_samples=parse_int(p.get("receivedSamples")),
            captured_samples=parse_int(p.get("capturedSamples")),
            accepting=p.get("accepting"), epoch=p.get("epoch"), rotate=bool(p.get("rotate", False)),
            error_code=p.get("errorCode"),
        )


@dataclass
class Word:
    text: Optional[str]
    start_ms: Optional[int]
    end_ms: Optional[int]

    @classmethod
    def from_wire(cls, p: Dict[str, Any]) -> "Word":
        return cls(p.get("text"), parse_int(p.get("startMs")), parse_int(p.get("endMs")))


def _words(raw: Any) -> List[Word]:
    return [Word.from_wire(w) for w in raw] if isinstance(raw, list) else []


@dataclass
class TranscriptLine:
    """One committed line, merged by ``id``. ``entities is None`` means the entities result
    has not arrived; ``entities == []`` with ``entities_arrived`` means it arrived empty."""
    id: str
    # transcript content stays out of repr() so that logging a result never logs speech text
    text: Optional[str] = field(default=None, repr=False)
    revised_text: Optional[str] = field(default=None, repr=False)
    language_code: Optional[str] = None
    language_probability: Optional[float] = None
    entities: Optional[List[Dict[str, Any]]] = field(default=None, repr=False)
    words: List[Word] = field(default_factory=list, repr=False)
    character_alignments: List[Word] = field(default_factory=list, repr=False)
    entities_arrived: bool = False
    revision_arrived: bool = False
    alignment_arrived: bool = False

    @classmethod
    def from_wire(cls, p: Dict[str, Any]) -> "TranscriptLine":
        line = cls(id=str(p.get("id")))
        line.text = p.get("text")
        line.revised_text = p.get("revisedText")
        line.language_code = p.get("languageCode")
        prob = p.get("languageProbability")
        line.language_probability = float(prob) if isinstance(prob, (int, float)) and not isinstance(prob, bool) else None
        ents = p.get("entities")
        if isinstance(ents, list) and ents:
            line.entities, line.entities_arrived = list(ents), True
        line.words = _words(p.get("words"))
        line.character_alignments = _words(p.get("characterAlignments"))
        line.revision_arrived = line.revised_text is not None
        return line


@dataclass
class Notice:
    code: Optional[str]
    message: Optional[str]
    supplements: List[str] = field(default_factory=list)
    line_ids: List[str] = field(default_factory=list)
    raw: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_wire(cls, p: Dict[str, Any]) -> "Notice":
        return cls(p.get("code"), p.get("message"), list(p.get("supplements") or []),
                   [str(x) for x in (p.get("lineIds") or [])], dict(p))


@dataclass
class SessionView:
    session_id: Optional[str]
    transcription_id: Optional[str]
    status: Optional[str]
    model_id: Optional[str] = None
    stream_url: Optional[str] = None
    epoch: Optional[int] = None
    accepting: Optional[bool] = None
    partial: Optional[str] = field(default=None, repr=False)
    lines: List[TranscriptLine] = field(default_factory=list)
    notices: List[Notice] = field(default_factory=list)
    usage: Usage = field(default_factory=Usage)
    effective_options: Dict[str, Any] = field(default_factory=dict)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    @classmethod
    def from_wire(cls, p: Dict[str, Any]) -> "SessionView":
        return cls(
            session_id=p.get("sessionId"), transcription_id=p.get("transcriptionId"), status=p.get("status"),
            model_id=p.get("modelId"), stream_url=p.get("streamUrl"), epoch=p.get("epoch"),
            accepting=p.get("accepting"), partial=p.get("partial"),
            lines=[TranscriptLine.from_wire(l) for l in (p.get("lines") or []) if isinstance(l, dict)],
            notices=[Notice.from_wire(n) for n in (p.get("notices") or []) if isinstance(n, dict)],
            usage=Usage.from_wire(p.get("usage") or {}), effective_options=dict(p.get("effectiveOptions") or {}),
            created_at=p.get("createdAt"), updated_at=p.get("updatedAt"), raw=dict(p),
        )


@dataclass
class Ticket:
    """A single-use browser ticket. The SDK itself never connects with it. ``ticket`` is a
    secret: do not log it."""
    ticket_id: Optional[str]
    session_id: Optional[str]
    stream_url: Optional[str]
    expires_in_seconds: Optional[int]
    ticket: str = field(repr=False, default="")

    @classmethod
    def from_wire(cls, p: Dict[str, Any]) -> "Ticket":
        return cls(p.get("ticketId"), p.get("sessionId"), p.get("streamUrl"), p.get("expiresInSeconds"),
                   p.get("ticket") or "")


@dataclass
class Event:
    """One server event. ``sequence`` is per socket connection (``connection`` counts the
    connections this SDK opened for the session); it is not a cross-connection cursor."""
    type: str
    payload: Dict[str, Any] = field(repr=False)
    sequence: Optional[int]
    event_id: Optional[str]
    session_id: Optional[str]
    connection: int = 1

    @property
    def usage(self) -> Optional[Usage]:
        return Usage.from_wire(self.payload) if self.type == "usage.updated" else None

    @property
    def line(self) -> Optional[TranscriptLine]:
        return TranscriptLine.from_wire(self.payload) if self.type == "transcript.final" else None

    @property
    def view(self) -> Optional[SessionView]:
        if self.type in ("session.ready", "session.completed"):
            return SessionView.from_wire(self.payload)
        return None

    @property
    def notice(self) -> Optional[Notice]:
        return Notice.from_wire(self.payload) if self.type == "session.notice" else None

    @property
    def error_code(self) -> Optional[str]:
        if self.type == "session.error":
            return self.payload.get("errorCode")
        if self.type == "usage.updated":
            return self.payload.get("errorCode")
        return None

    @property
    def text(self) -> Optional[str]:
        return self.payload.get("text") if self.type in ("transcript.delta", "transcript.final") else None


class Transcript:
    """Snapshot merged from events: ``delta`` replaces ``partial``; final/alignment/
    revision/entities update lines by id and never open a new line for a supplement."""

    def __init__(self) -> None:
        self.partial: str = ""
        self._lines: Dict[str, TranscriptLine] = {}

    @property
    def lines(self) -> List[TranscriptLine]:
        return list(self._lines.values())

    def line(self, line_id: str) -> Optional[TranscriptLine]:
        return self._lines.get(line_id)

    @property
    def text(self) -> str:
        return " ".join((l.revised_text or l.text or "") for l in self._lines.values() if (l.text or l.revised_text))

    def _get(self, line_id: str, payload: Dict[str, Any]) -> TranscriptLine:
        line = self._lines.get(line_id)
        if line is None:
            line = TranscriptLine(id=line_id, text=payload.get("text"))
            self._lines[line_id] = line
        return line

    def apply(self, event_type: str, p: Dict[str, Any]) -> None:
        if event_type == "transcript.delta":
            self.partial = p.get("text") or ""
        elif event_type == "transcript.final":
            new = TranscriptLine.from_wire(p)
            old = self._lines.get(new.id)
            if old is None:
                self._lines[new.id] = new
            else:
                old.text = new.text
        elif event_type == "transcript.revision":
            line = self._get(str(p.get("id")), p)
            line.revised_text = p.get("revisedText")
            line.revision_arrived = True
        elif event_type == "transcript.entities":
            line = self._get(str(p.get("id")), p)
            line.entities = list(p.get("entities") or [])
            line.entities_arrived = True
        elif event_type == "transcript.alignment":
            line = self._get(str(p.get("id")), p)
            line.language_code = p.get("languageCode")
            prob = p.get("languageProbability")
            line.language_probability = float(prob) if isinstance(prob, (int, float)) and not isinstance(prob, bool) else None
            line.words = _words(p.get("words"))
            line.character_alignments = _words(p.get("characterAlignments"))
            line.alignment_arrived = True
        elif event_type == "session.completed":
            self.merge_view(SessionView.from_wire(p))

    def merge_view(self, view: SessionView) -> None:
        for vl in view.lines:
            line = self._lines.get(vl.id)
            if line is None:
                self._lines[vl.id] = vl
                continue
            line.text = vl.text
            if vl.revised_text is not None:
                line.revised_text = vl.revised_text
            if vl.language_code is not None:
                line.language_code, line.language_probability = vl.language_code, vl.language_probability
            if vl.words:
                line.words = vl.words
            if vl.character_alignments:
                line.character_alignments = vl.character_alignments
            if vl.entities_arrived:
                line.entities, line.entities_arrived = vl.entities, True


@dataclass
class FinishResult:
    """The server's real terminal outcome. ``status`` is COMPLETED, PARTIAL or FAILED exactly
    as returned; it is never rewritten. ``closeout_reason`` is set when the result came from
    the REST close-out path (``DISCONNECTED``, ``ACK_TIMEOUT``, ``FINISH_TIMEOUT``,
    ``SERVER_ERROR``)."""
    status: Optional[str]
    session_id: Optional[str]
    transcription_id: Optional[str]
    usage: Usage
    notices: List[Notice]
    lines: List[TranscriptLine]
    view: SessionView = field(repr=False)
    via_rest: bool = False
    closeout_reason: Optional[str] = None
    server_errors: List[str] = field(default_factory=list)
    callback_errors: int = 0
    dropped_events: int = 0

    @property
    def text(self) -> str:
        return " ".join((l.revised_text or l.text or "") for l in self.lines if (l.text or l.revised_text))
