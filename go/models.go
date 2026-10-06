package sttrealtime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// Public server event types.
const (
	EventSessionReady        = "session.ready"
	EventTranscriptDelta     = "transcript.delta"
	EventTranscriptFinal     = "transcript.final"
	EventTranscriptAlignment = "transcript.alignment"
	EventTranscriptRevision  = "transcript.revision"
	EventTranscriptEntities  = "transcript.entities"
	EventUsageUpdated        = "usage.updated"
	EventSessionNotice       = "session.notice"
	EventSessionCompleted    = "session.completed"
	EventSessionError        = "session.error"
)

// EventTypes lists the ten public server events.
var EventTypes = []string{EventSessionReady, EventTranscriptDelta, EventTranscriptFinal, EventTranscriptAlignment,
	EventTranscriptRevision, EventTranscriptEntities, EventUsageUpdated, EventSessionNotice, EventSessionCompleted,
	EventSessionError}

// Pointer helpers for optional fields.
func Bool(v bool) *bool        { return &v }
func Int(v int) *int           { return &v }
func Float(v float64) *float64 { return &v }
func String(v string) *string  { return &v }

// SessionOptions are the 16 public "options" fields. nil / "" means omitted (server default);
// false and 0 are sent as given.
type SessionOptions struct {
	InputEncoding            string
	AdditionalLanguageHints  []string
	SegmentCommitMode        string // "manual" or "vad"
	SpeechDetectionThreshold *float64
	CommitSilenceMs          *float64
	MinimumSpeechMs          *int
	MinimumSilenceMs         *int
	IncludeAlignment         *bool
	IncludeDetectedLanguage  *bool
	VocabularyHints          []string // at most 50
	RemoveDisfluencies       *bool
	EntityCategories         []string
	RewriteInstruction       *string
	SuppressBackgroundSpeech *bool
	HeartbeatIntervalMs      *int
	ProcessingContentStorage *bool
}

func (o *SessionOptions) wire() map[string]any {
	m := map[string]any{}
	if o == nil {
		return m
	}
	if o.InputEncoding != "" {
		m["inputEncoding"] = o.InputEncoding
	}
	if o.AdditionalLanguageHints != nil {
		m["additionalLanguageHints"] = o.AdditionalLanguageHints
	}
	if o.SegmentCommitMode != "" {
		m["segmentCommitMode"] = o.SegmentCommitMode
	}
	putPtr(m, "speechDetectionThreshold", o.SpeechDetectionThreshold)
	putPtr(m, "commitSilenceMs", o.CommitSilenceMs)
	putPtr(m, "minimumSpeechMs", o.MinimumSpeechMs)
	putPtr(m, "minimumSilenceMs", o.MinimumSilenceMs)
	putPtr(m, "includeAlignment", o.IncludeAlignment)
	putPtr(m, "includeDetectedLanguage", o.IncludeDetectedLanguage)
	if o.VocabularyHints != nil {
		m["vocabularyHints"] = o.VocabularyHints
	}
	putPtr(m, "removeDisfluencies", o.RemoveDisfluencies)
	if o.EntityCategories != nil {
		m["entityCategories"] = o.EntityCategories
	}
	putPtr(m, "rewriteInstruction", o.RewriteInstruction)
	putPtr(m, "suppressBackgroundSpeech", o.SuppressBackgroundSpeech)
	putPtr(m, "heartbeatIntervalMs", o.HeartbeatIntervalMs)
	putPtr(m, "processingContentStorage", o.ProcessingContentStorage)
	return m
}

func putPtr[T any](m map[string]any, key string, v *T) {
	if v != nil {
		m[key] = *v
	}
}

// SessionRequest is the body of POST /sessions.
type SessionRequest struct {
	LanguageHint string
	Title        string
	Options      *SessionOptions
}

// MarshalJSON writes the wire body (top-level languageHint/title plus options).
func (r SessionRequest) MarshalJSON() ([]byte, error) { return json.Marshal(r.wire()) }

func (r SessionRequest) wire() map[string]any {
	b := map[string]any{}
	if r.LanguageHint != "" {
		b["languageHint"] = r.LanguageHint
	}
	if r.Title != "" {
		b["title"] = r.Title
	}
	if r.Options != nil {
		b["options"] = r.Options.wire()
	}
	return b
}

func (r *SessionRequest) encoding() string {
	if r != nil && r.Options != nil && r.Options.InputEncoding != "" {
		return r.Options.InputEncoding
	}
	return ""
}

// Usage is a usage.updated payload or a view's usage. nil = absent (never 0).
type Usage struct {
	EstimatedCharacters *int64
	BillableCharacters  *int64
	ElapsedMs           *int64
	SentSamples         *int64
	ReceivedSamples     *int64
	CapturedSamples     *int64
	Accepting           *bool
	Epoch               *int64
	Rotate              bool
	ErrorCode           string
}

func usageFrom(p map[string]any) Usage {
	u := Usage{
		EstimatedCharacters: optInt(p["estimatedCharacters"]),
		BillableCharacters:  optInt(p["billableCharacters"]),
		ElapsedMs:           optInt(p["elapsedMs"]),
		SentSamples:         optInt(p["sentSamples"]),
		ReceivedSamples:     optInt(p["receivedSamples"]),
		CapturedSamples:     optInt(p["capturedSamples"]),
		Epoch:               optInt(p["epoch"]),
		ErrorCode:           str(p["errorCode"]),
	}
	if b, ok := p["accepting"].(bool); ok {
		u.Accepting = &b
	}
	u.Rotate, _ = p["rotate"].(bool)
	return u
}

// Word is a word or character alignment; nil times stay nil.
type Word struct {
	Text    string
	StartMs *int64
	EndMs   *int64
}

func wordsFrom(v any) []Word {
	arr, _ := v.([]any)
	out := make([]Word, 0, len(arr))
	for _, x := range arr {
		m, _ := x.(map[string]any)
		out = append(out, Word{Text: str(m["text"]), StartMs: optInt(m["startMs"]), EndMs: optInt(m["endMs"])})
	}
	return out
}

// Line is one committed transcript line merged by ID. EntitiesArrived distinguishes an
// entities result that arrived empty from one that has not arrived.
type Line struct {
	ID                  string
	Text                string
	RevisedText         *string
	LanguageCode        *string
	LanguageProbability *float64
	Entities            []map[string]any
	Words               []Word
	CharacterAlignments []Word
	EntitiesArrived     bool
	RevisionArrived     bool
	AlignmentArrived    bool
}

// String never prints transcript content.
func (l Line) String() string {
	return fmt.Sprintf("Line{ID:%s entities:%t revision:%t alignment:%t}", l.ID, l.EntitiesArrived,
		l.RevisionArrived, l.AlignmentArrived)
}

func lineFrom(p map[string]any) Line {
	l := Line{ID: str(p["id"]), Text: str(p["text"])}
	if s, ok := p["revisedText"].(string); ok {
		l.RevisedText, l.RevisionArrived = &s, true
	}
	if s, ok := p["languageCode"].(string); ok {
		l.LanguageCode = &s
	}
	l.LanguageProbability = optFloat(p["languageProbability"])
	if ents, ok := p["entities"].([]any); ok && len(ents) > 0 {
		l.Entities, l.EntitiesArrived = mapsOf(ents), true
	}
	l.Words = wordsFrom(p["words"])
	l.CharacterAlignments = wordsFrom(p["characterAlignments"])
	return l
}

// Notice is a session.notice (also listed in the view).
type Notice struct {
	Code        string
	Message     string
	Supplements []string
	LineIDs     []string
	Raw         map[string]any
}

func noticeFrom(p map[string]any) Notice {
	return Notice{Code: str(p["code"]), Message: str(p["message"]), Supplements: strs(p["supplements"]),
		LineIDs: strs(p["lineIds"]), Raw: p}
}

// SessionView is the REST/WS session resource.
type SessionView struct {
	SessionID        string
	TranscriptionID  string
	Status           string // RECORDING, PAUSED, COMPLETED, PARTIAL, FAILED
	ModelID          string
	StreamURL        string
	Epoch            int64
	Accepting        bool
	Partial          string
	Lines            []Line
	Notices          []Notice
	Usage            Usage
	EffectiveOptions map[string]any
	CreatedAt        string
	UpdatedAt        string
}

// IsTerminal reports COMPLETED, PARTIAL or FAILED.
func (v SessionView) IsTerminal() bool {
	return v.Status == "COMPLETED" || v.Status == "PARTIAL" || v.Status == "FAILED"
}

// String never prints transcript content.
func (v SessionView) String() string {
	return fmt.Sprintf("SessionView{SessionID:%s TranscriptionID:%s Status:%s Epoch:%d Accepting:%t Lines:%d}",
		v.SessionID, v.TranscriptionID, v.Status, v.Epoch, v.Accepting, len(v.Lines))
}

func viewFrom(p map[string]any) SessionView {
	v := SessionView{SessionID: str(p["sessionId"]), TranscriptionID: str(p["transcriptionId"]),
		Status: str(p["status"]), ModelID: str(p["modelId"]), StreamURL: str(p["streamUrl"]),
		Partial: str(p["partial"]), CreatedAt: str(p["createdAt"]), UpdatedAt: str(p["updatedAt"])}
	if n := optInt(p["epoch"]); n != nil {
		v.Epoch = *n
	}
	v.Accepting, _ = p["accepting"].(bool)
	if arr, ok := p["lines"].([]any); ok {
		for _, x := range arr {
			if m, ok := x.(map[string]any); ok {
				v.Lines = append(v.Lines, lineFrom(m))
			}
		}
	}
	if arr, ok := p["notices"].([]any); ok {
		for _, x := range arr {
			if m, ok := x.(map[string]any); ok {
				v.Notices = append(v.Notices, noticeFrom(m))
			}
		}
	}
	if u, ok := p["usage"].(map[string]any); ok {
		v.Usage = usageFrom(u)
	}
	v.EffectiveOptions, _ = p["effectiveOptions"].(map[string]any)
	if v.EffectiveOptions == nil {
		v.EffectiveOptions = map[string]any{}
	}
	return v
}

// Ticket is a single-use browser ticket. This SDK never connects with it. Ticket.Value is a
// secret: never log it. String/GoString hide it.
type Ticket struct {
	TicketID         string
	SessionID        string
	StreamURL        string
	ExpiresInSeconds int64
	Value            string
}

func (t Ticket) String() string {
	return fmt.Sprintf("Ticket{TicketID:%s SessionID:%s ExpiresInSeconds:%d}", t.TicketID, t.SessionID, t.ExpiresInSeconds)
}
func (t Ticket) GoString() string { return t.String() }

// Event is one server event. Sequence is per socket connection (Connection counts the
// connections this SDK opened for the session); it is not a cross-connection cursor.
type Event struct {
	Type       string
	Payload    map[string]any
	Sequence   *int64
	EventID    string
	SessionID  string
	Connection int
}

// String never prints the payload.
func (e Event) String() string {
	seq := int64(0)
	if e.Sequence != nil {
		seq = *e.Sequence
	}
	return fmt.Sprintf("Event{Type:%s Sequence:%d Connection:%d}", e.Type, seq, e.Connection)
}

// Usage returns the usage of a usage.updated event.
func (e Event) Usage() (Usage, bool) {
	if e.Type != EventUsageUpdated {
		return Usage{}, false
	}
	return usageFrom(e.Payload), true
}

// Line returns the line of a transcript.final event.
func (e Event) Line() (Line, bool) {
	if e.Type != EventTranscriptFinal {
		return Line{}, false
	}
	return lineFrom(e.Payload), true
}

// View returns the session view of session.ready / session.completed.
func (e Event) View() (SessionView, bool) {
	if e.Type != EventSessionReady && e.Type != EventSessionCompleted {
		return SessionView{}, false
	}
	return viewFrom(e.Payload), true
}

// Notice returns the notice of a session.notice event.
func (e Event) Notice() (Notice, bool) {
	if e.Type != EventSessionNotice {
		return Notice{}, false
	}
	return noticeFrom(e.Payload), true
}

// ErrorCode of session.error (or usage.updated carrying errorCode).
func (e Event) ErrorCode() string { return str(e.Payload["errorCode"]) }

// Text of transcript.delta / transcript.final.
func (e Event) Text() string {
	if e.Type == EventTranscriptDelta || e.Type == EventTranscriptFinal {
		return str(e.Payload["text"])
	}
	return ""
}

// Transcript is a snapshot merged from events: delta replaces Partial; final, alignment,
// revision and entities update lines by ID and never open a line for a supplement.
type Transcript struct {
	mu      sync.Mutex
	partial string
	order   []string
	lines   map[string]*Line
}

func newTranscript() *Transcript { return &Transcript{lines: map[string]*Line{}} }

// Partial is the latest delta text.
func (t *Transcript) Partial() string { t.mu.Lock(); defer t.mu.Unlock(); return t.partial }

// Lines returns copies of the merged lines in commit order.
func (t *Transcript) Lines() []Line {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Line, 0, len(t.order))
	for _, id := range t.order {
		out = append(out, *t.lines[id])
	}
	return out
}

// Text joins revised (else original) line texts.
func (t *Transcript) Text() string { return joinText(t.Lines()) }

func joinText(lines []Line) string {
	parts := []string{}
	for _, l := range lines {
		s := l.Text
		if l.RevisedText != nil && *l.RevisedText != "" {
			s = *l.RevisedText
		}
		if s != "" {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, " ")
}

func (t *Transcript) get(id string, p map[string]any) *Line {
	l, ok := t.lines[id]
	if !ok {
		l = &Line{ID: id, Text: str(p["text"])}
		t.lines[id] = l
		t.order = append(t.order, id)
	}
	return l
}

func (t *Transcript) apply(etype string, p map[string]any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch etype {
	case EventTranscriptDelta:
		t.partial = str(p["text"])
	case EventTranscriptFinal:
		n := lineFrom(p)
		if old, ok := t.lines[n.ID]; ok {
			old.Text = n.Text
		} else {
			t.lines[n.ID] = &n
			t.order = append(t.order, n.ID)
		}
	case EventTranscriptRevision:
		l := t.get(str(p["id"]), p)
		if s, ok := p["revisedText"].(string); ok {
			l.RevisedText = &s
		}
		l.RevisionArrived = true
	case EventTranscriptEntities:
		l := t.get(str(p["id"]), p)
		ents, _ := p["entities"].([]any)
		l.Entities, l.EntitiesArrived = mapsOf(ents), true
	case EventTranscriptAlignment:
		l := t.get(str(p["id"]), p)
		if s, ok := p["languageCode"].(string); ok {
			l.LanguageCode = &s
		} else {
			l.LanguageCode = nil
		}
		l.LanguageProbability = optFloat(p["languageProbability"])
		l.Words = wordsFrom(p["words"])
		l.CharacterAlignments = wordsFrom(p["characterAlignments"])
		l.AlignmentArrived = true
	}
}

func (t *Transcript) mergeView(v SessionView) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, vl := range v.Lines {
		vl := vl
		l, ok := t.lines[vl.ID]
		if !ok {
			t.lines[vl.ID] = &vl
			t.order = append(t.order, vl.ID)
			continue
		}
		l.Text = vl.Text
		if vl.RevisedText != nil {
			l.RevisedText, l.RevisionArrived = vl.RevisedText, true
		}
		if vl.LanguageCode != nil {
			l.LanguageCode, l.LanguageProbability = vl.LanguageCode, vl.LanguageProbability
		}
		if len(vl.Words) > 0 {
			l.Words = vl.Words
		}
		if len(vl.CharacterAlignments) > 0 {
			l.CharacterAlignments = vl.CharacterAlignments
		}
		if vl.EntitiesArrived {
			l.Entities, l.EntitiesArrived = vl.Entities, true
		}
	}
}

// FinishResult is the server's real terminal outcome. Status is COMPLETED, PARTIAL or FAILED
// exactly as returned. CloseoutReason is set when the result came from the REST close-out
// path: DISCONNECTED, ACK_TIMEOUT or FINISH_TIMEOUT.
type FinishResult struct {
	Status          string
	SessionID       string
	TranscriptionID string
	Usage           Usage
	Notices         []Notice
	Lines           []Line
	View            SessionView
	ViaREST         bool
	CloseoutReason  string
	ServerErrors    []string
	CallbackErrors  int64
	DroppedEvents   int64
}

// Text joins revised (else original) line texts.
func (r *FinishResult) Text() string { return joinText(r.Lines) }

// String never prints transcript content.
func (r *FinishResult) String() string {
	return fmt.Sprintf("FinishResult{Status:%s SessionID:%s TranscriptionID:%s ViaREST:%t CloseoutReason:%s Lines:%d}",
		r.Status, r.SessionID, r.TranscriptionID, r.ViaREST, r.CloseoutReason, len(r.Lines))
}

// ---------------------------------------------------------------- JSON helpers

func decodeObject(data []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

func asInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case json.Number:
		n, err := strconv.ParseInt(string(x), 10, 64)
		return n, err == nil
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		return n, err == nil
	case int:
		return int64(x), true
	case int64:
		return x, true
	}
	return 0, false
}

func optInt(v any) *int64 {
	if n, ok := asInt64(v); ok {
		return &n
	}
	return nil
}

func optFloat(v any) *float64 {
	if n, ok := v.(json.Number); ok {
		if f, err := n.Float64(); err == nil {
			return &f
		}
	}
	return nil
}

func str(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func strs(v any) []string {
	arr, _ := v.([]any)
	out := make([]string, 0, len(arr))
	for _, x := range arr {
		out = append(out, fmt.Sprint(x))
	}
	return out
}

func mapsOf(arr []any) []map[string]any {
	out := make([]map[string]any, 0, len(arr))
	for _, x := range arr {
		if m, ok := x.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}
