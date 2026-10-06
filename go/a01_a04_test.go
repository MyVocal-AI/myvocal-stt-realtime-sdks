package sttrealtime

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

// ---------------------------------------------------------------- A01

func TestA01ConnectAudioFinish(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	var seen []string
	var mu = make(chan struct{}, 1)
	s, err := c.Connect(ctxT(t), ConnectOptions{Request: req("L-basic", ""), Handlers: map[string]func(Event){
		"*": func(e Event) { mu <- struct{}{}; seen = append(seen, e.Type); <-mu }}})
	if err != nil {
		t.Fatal(err)
	}
	data := audio(t, "pcm_s16le_16000", 0)
	sendAll(t, s, data, 3333)
	r, err := s.Finish(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	rec := f.record(t, r.SessionID)
	if r.Status != "COMPLETED" || r.TranscriptionID != rec.TranscriptionID || rec.StoredSha256 != sha(data) {
		t.Fatalf("result %v rec %+v", r, rec.Status)
	}
	joined := strings.Join(seen, ",")
	if !strings.Contains(joined, EventSessionReady) || !strings.Contains(joined, EventSessionCompleted) {
		t.Fatalf("events %v", seen)
	}
}

func TestA01EventChannel(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	s, err := c.Connect(ctxT(t), ConnectOptions{Request: req("L-basic", ""), EventChannel: true})
	if err != nil {
		t.Fatal(err)
	}
	sendAll(t, s, audio(t, "pcm_s16le_16000", 0), 32000)
	r, err := s.Finish(ctxT(t))
	if err != nil || r.Status != "COMPLETED" {
		t.Fatal(err, r)
	}
	_ = s.Close()
	var types []string
	for e := range s.Events() {
		types = append(types, e.Type)
	}
	if types[0] != EventSessionReady || types[len(types)-1] != EventSessionCompleted {
		t.Fatalf("types %v", types)
	}
}

// ---------------------------------------------------------------- A02

type countingRT struct {
	n    atomic.Int64
	next http.RoundTripper
}

func (c *countingRT) RoundTrip(r *http.Request) (*http.Response, error) {
	c.n.Add(1)
	return c.next.RoundTrip(r)
}

func countingClient() (*http.Client, *countingRT) {
	rt := &countingRT{next: &http.Transport{Proxy: nil}}
	return &http.Client{Transport: rt}, rt
}

func TestA02CreateResponseLostReusesKeyAndBody(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	s, err := c.Connect(ctxT(t), ConnectOptions{Request: req("A02-create-lost", "")})
	if err != nil {
		t.Fatal(err)
	}
	sendAll(t, s, audio(t, "pcm_s16le_16000", 0.3), 9600)
	r, err := s.Finish(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	recs := f.byTitle(t, "A02-create-lost")
	if len(recs) != 1 {
		t.Fatalf("sessions %d", len(recs))
	}
	var creates []map[string]any
	for _, call := range recs[0].RestCalls {
		if strings.HasPrefix(call["path"].(string), "create") {
			creates = append(creates, call)
		}
	}
	if len(creates) != 2 || creates[1]["path"] != "create(replay)" || creates[0]["idempotencyKey"] != creates[1]["idempotencyKey"] {
		t.Fatalf("creates %v", creates)
	}
	if r.SessionID != recs[0].SessionID {
		t.Fatal("session id")
	}
}

func TestA02EnvelopeErrorNonRetryable(t *testing.T) {
	f := fx(t)
	hc, rt := countingClient()
	c := newClient(t, f, func(cfg *Config) { cfg.HTTPClient = hc })
	_, err := c.CreateSession(ctxT(t), *req("A02-envelope-nonretryable", ""), "")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || !errors.Is(err, ErrInsufficientCharacters) || apiErr.Code != 47210 || apiErr.Retryable {
		t.Fatalf("err %v", err)
	}
	if rt.n.Load() != 1 {
		t.Fatalf("requests %d", rt.n.Load())
	}
}

func TestA02EnvelopeErrorRetryableBounded(t *testing.T) {
	f := fx(t)
	hc, rt := countingClient()
	c := newClient(t, f, func(cfg *Config) { cfg.HTTPClient = hc; cfg.MaxCreateRetries = 2 })
	_, err := c.CreateSession(ctxT(t), *req("A02-envelope-retryable", ""), "")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode != "TEMPORARILY_UNAVAILABLE" || !apiErr.Retryable {
		t.Fatalf("err %v", err)
	}
	if rt.n.Load() != 3 {
		t.Fatalf("requests %d", rt.n.Load())
	}
}

func TestA02InvalidKeyNotRetried(t *testing.T) {
	f := fx(t)
	hc, rt := countingClient()
	c, _ := NewClient(Config{AccessKey: "wrong-key", BaseURL: f.Base, HTTPClient: hc})
	_, err := c.Connect(ctxT(t), ConnectOptions{Request: req("L-basic", "")})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || !errors.Is(err, ErrAuthentication) || apiErr.Code != 401 || rt.n.Load() != 1 {
		t.Fatalf("err %v n=%d", err, rt.n.Load())
	}
	if strings.Contains(err.Error(), "wrong-key") || strings.Contains(fmt.Sprintf("%v %#v", c, c), "wrong-key") {
		t.Fatal("key leaked")
	}
}

func TestA02RESTHelpersAndIdentity(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	ctx := ctxT(t)
	view, err := c.CreateSession(ctx, SessionRequest{LanguageHint: "en", Title: "A02-rest-helpers",
		Options: &SessionOptions{InputEncoding: "mulaw_8000"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(view.SessionID, "rts_") || !strings.HasPrefix(view.TranscriptionID, "stt_") ||
		view.EffectiveOptions["inputEncoding"] != "mulaw_8000" || fmt.Sprint(view.EffectiveOptions["sampleRateHz"]) != "8000" {
		t.Fatalf("view %v %v", view, view.EffectiveOptions)
	}
	got, _ := c.GetSession(ctx, view.SessionID)
	if got.TranscriptionID != view.TranscriptionID || got.Status != "RECORDING" {
		t.Fatal(got)
	}
	tk, err := c.CreateTicket(ctx, view.SessionID)
	if err != nil || tk.Value == "" || tk.SessionID != view.SessionID || tk.ExpiresInSeconds != 60 {
		t.Fatal(err, tk)
	}
	if strings.Contains(fmt.Sprintf("%v %+v %#v", tk, tk, tk), tk.Value) {
		t.Fatal("ticket printed")
	}
	zero := int64(0)
	p, _ := c.PauseSession(ctx, view.SessionID, &zero)
	if p.Accepting || p.Status != "PAUSED" {
		t.Fatal(p)
	}
	rs, _ := c.ResumeSession(ctx, view.SessionID, &zero)
	if rs.Epoch != 2 || !rs.Accepting {
		t.Fatal(rs)
	}
	done, _ := c.FinishSession(ctx, view.SessionID, &zero)
	again, _ := c.FinishSession(ctx, view.SessionID, &zero)
	if done.Status != "FAILED" || again.TranscriptionID != done.TranscriptionID || again.Status != done.Status {
		t.Fatal(done, again)
	}
	rec := f.record(t, view.SessionID)
	want := map[string]any{"languageHint": "en", "title": "A02-rest-helpers", "options": map[string]any{"inputEncoding": "mulaw_8000"}}
	if !reflect.DeepEqual(rec.RawRequest, want) {
		t.Fatalf("raw %v", rec.RawRequest)
	}
	for _, call := range rec.RestCalls {
		if call["hasAccessKeyHeader"] != true || call["queryHasKey"] == true {
			t.Fatalf("call %v", call)
		}
	}
}

func TestA02SocketIdentityHeaderNotURL(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	s, err := c.Connect(ctxT(t), ConnectOptions{Request: req("L-basic", "")})
	if err != nil {
		t.Fatal(err)
	}
	sendAll(t, s, audio(t, "pcm_s16le_16000", 0.2), 6400)
	_, _ = s.Finish(ctxT(t))
	_ = s.Close()
	conn := f.record(t, s.SessionID).Connections[0]
	if conn["hasAccessKeyHeader"] != true || conn["queryHasKey"] == true || conn["query"] != "" ||
		!strings.HasPrefix(conn["userAgent"].(string), "myvocal-stt-realtime-go/") {
		t.Fatalf("conn %v", conn)
	}
}

// ---------------------------------------------------------------- A03

func optionsFromWire(t testing.TB, m map[string]any) *SessionOptions {
	o := &SessionOptions{}
	for k, v := range m {
		switch k {
		case "inputEncoding":
			o.InputEncoding = v.(string)
		case "additionalLanguageHints":
			o.AdditionalLanguageHints = toStrings(v)
		case "segmentCommitMode":
			o.SegmentCommitMode = v.(string)
		case "speechDetectionThreshold":
			o.SpeechDetectionThreshold = Float(v.(float64))
		case "commitSilenceMs":
			o.CommitSilenceMs = Float(v.(float64))
		case "minimumSpeechMs":
			o.MinimumSpeechMs = Int(int(v.(float64)))
		case "minimumSilenceMs":
			o.MinimumSilenceMs = Int(int(v.(float64)))
		case "includeAlignment":
			o.IncludeAlignment = Bool(v.(bool))
		case "includeDetectedLanguage":
			o.IncludeDetectedLanguage = Bool(v.(bool))
		case "vocabularyHints":
			if v == "51x" {
				for i := 0; i < 51; i++ {
					o.VocabularyHints = append(o.VocabularyHints, fmt.Sprintf("term%d", i))
				}
			} else {
				o.VocabularyHints = toStrings(v)
			}
		case "removeDisfluencies":
			o.RemoveDisfluencies = Bool(v.(bool))
		case "entityCategories":
			o.EntityCategories = toStrings(v)
		case "rewriteInstruction":
			o.RewriteInstruction = String(v.(string))
		case "suppressBackgroundSpeech":
			o.SuppressBackgroundSpeech = Bool(v.(bool))
		case "heartbeatIntervalMs":
			o.HeartbeatIntervalMs = Int(int(v.(float64)))
		case "processingContentStorage":
			o.ProcessingContentStorage = Bool(v.(bool))
		default:
			t.Fatalf("unknown option %s", k)
		}
	}
	return o
}

func toStrings(v any) []string {
	var out []string
	for _, x := range v.([]any) {
		out = append(out, x.(string))
	}
	return out
}

func TestA03ParametersReachWireExactly(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	for _, sid := range []string{"A03-params-full-a", "A03-params-full-b", "A03-falsy"} {
		body := scenario(sid)["request"].(map[string]any)
		r := SessionRequest{Title: body["title"].(string), Options: optionsFromWire(t, body["options"].(map[string]any))}
		if lh, ok := body["languageHint"].(string); ok {
			r.LanguageHint = lh
		}
		view, err := c.CreateSession(ctxT(t), r, "")
		if err != nil {
			t.Fatal(sid, err)
		}
		_, _ = c.FinishSession(ctxT(t), view.SessionID, nil)
		rec := f.record(t, view.SessionID)
		if !reflect.DeepEqual(rec.RawRequest, body) {
			t.Fatalf("%s raw %v\nwant %v", sid, rec.RawRequest, body)
		}
		for k, v := range body["options"].(map[string]any) {
			if k != "inputEncoding" && !reflect.DeepEqual(rec.Effective[k], v) {
				t.Fatalf("%s effective %s=%v want %v", sid, k, rec.Effective[k], v)
			}
		}
	}
}

func TestA03IllegalCombinationsAreInputErrorsNotRetried(t *testing.T) {
	f := fx(t)
	for _, sid := range []string{"A03-illegal-rewrite-entities", "A03-illegal-suppress-alignment", "A03-illegal-vocab-51"} {
		hc, rt := countingClient()
		c := newClient(t, f, func(cfg *Config) { cfg.HTTPClient = hc })
		body := scenario(sid)["request"].(map[string]any)
		_, err := c.Connect(ctxT(t), ConnectOptions{Request: &SessionRequest{Title: sid,
			Options: optionsFromWire(t, body["options"].(map[string]any))}})
		if !errors.Is(err, ErrInputInvalid) || rt.n.Load() != 1 || len(f.byTitle(t, sid)) != 0 {
			t.Fatalf("%s err %v n=%d", sid, err, rt.n.Load())
		}
	}
}

func TestA03PrecedingTextOnAudioEvent(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	s, err := c.Connect(ctxT(t), ConnectOptions{Request: req("A03-preceding-text", "")})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SendAudioWithPrecedingText(ctxT(t), audio(t, "pcm_s16le_16000", 0.2), "Earlier context."); err != nil {
		t.Fatal(err)
	}
	_, _ = s.Finish(ctxT(t))
	_ = s.Close()
	frames := f.record(t, s.SessionID).Frames
	if frames[0]["precedingText"] != "Earlier context." || frames[1]["precedingText"] != nil {
		t.Fatalf("frames %v %v", frames[0]["precedingText"], frames[1]["precedingText"])
	}
}

func TestA03OmittedVersusFalseAndZero(t *testing.T) {
	o := &SessionOptions{IncludeAlignment: Bool(false), MinimumSpeechMs: Int(0), SpeechDetectionThreshold: Float(0)}
	want := map[string]any{"includeAlignment": false, "minimumSpeechMs": 0, "speechDetectionThreshold": 0.0}
	if !reflect.DeepEqual(o.wire(), want) || len((&SessionOptions{}).wire()) != 0 {
		t.Fatalf("%v", o.wire())
	}
}

// ---------------------------------------------------------------- A04

func chunkings(data []byte, bps int) map[string][][]byte {
	out := map[string][][]byte{"whole": {data}}
	var c128, irr [][]byte
	for i := 0; i < len(data); i += 128 * bps {
		c128 = append(c128, data[i:minInt(i+128*bps, len(data))])
	}
	rnd := rand.New(rand.NewSource(20261006))
	sizes := []int{1, 3, 7, 77, 255, 1001, 4097}
	for i := 0; i < len(data); {
		n := sizes[rnd.Intn(len(sizes))]
		irr = append(irr, data[i:minInt(i+n, len(data))])
		i += n
	}
	out["128-sample"], out["irregular"] = c128, irr
	return out
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func TestA04EncodingsAndChunkings(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	for enc, format := range Encodings {
		data := audio(t, enc, 0)
		for name, chunks := range chunkings(data, format.BytesPerSample) {
			s, err := c.Connect(ctxT(t), ConnectOptions{Request: req("A04-encoding", enc)})
			if err != nil {
				t.Fatal(err)
			}
			for _, ch := range chunks {
				if err := s.SendAudio(ctxT(t), ch); err != nil {
					t.Fatal(err)
				}
			}
			r, err := s.Finish(ctxT(t))
			if err != nil {
				t.Fatal(err)
			}
			_ = s.Close()
			rec := f.record(t, s.SessionID)
			samples := int64(len(data) / format.BytesPerSample)
			if rec.StoredSha256 != sha(data) || num(rec.Captured) != samples || r.Status != "COMPLETED" {
				t.Fatalf("%s/%s sha/captured %d", enc, name, num(rec.Captured))
			}
			var maxClaim int64
			for _, cl := range rec.CapturedClaims {
				if v, _ := cl.Int64(); v > maxClaim {
					maxClaim = v
				}
			}
			frame := int64(format.SampleRateHz / 10 * format.BytesPerSample)
			for i, fr := range rec.Frames {
				b := i64(fr["bytes"])
				if (i < len(rec.Frames)-1 && b != frame) || b <= 0 || b > frame {
					t.Fatalf("%s/%s frame %d bytes %d", enc, name, i, b)
				}
			}
			if maxClaim != samples {
				t.Fatalf("claims %d", maxClaim)
			}
		}
	}
}

func TestA04ResidualHalfSample(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	data := audio(t, "pcm_s16le_16000", 0.3)
	s, err := c.Connect(ctxT(t), ConnectOptions{Request: req("A04-encoding", "")})
	if err != nil {
		t.Fatal(err)
	}
	_ = s.SendAudio(ctxT(t), append(append([]byte{}, data...), 1))
	_, err = s.Finish(ctxT(t))
	var afe *AudioFormatError
	if !errors.As(err, &afe) || !s.DiscardPartialSample() {
		t.Fatalf("err %v", err)
	}
	r, err := s.Finish(ctxT(t))
	_ = s.Close()
	if err != nil || r.Status != "COMPLETED" || f.record(t, s.SessionID).StoredSha256 != sha(data) {
		t.Fatal(err, r)
	}
}

func TestA04WAVHeaderAndMismatch(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	wav := filepath.Join(repoRoot, "fixtures", "audio", "tone_pcm_s16le_16000.wav")
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("A04-encoding", "pcm_s16le_8000")})
	var afe *AudioFormatError
	if err := s.SendFile(ctxT(t), wav, false); !errors.As(err, &afe) {
		t.Fatalf("err %v", err)
	}
	_, _ = s.Finish(ctxT(t))
	_ = s.Close()
	s, _ = c.Connect(ctxT(t), ConnectOptions{Request: req("A04-encoding", "")})
	if err := s.SendFile(context.Background(), wav, false); err != nil {
		t.Fatal(err)
	}
	_, _ = s.Finish(ctxT(t))
	_ = s.Close()
	if f.record(t, s.SessionID).StoredSha256 != sha(audio(t, "pcm_s16le_16000", 0)) {
		t.Fatal("wav sha")
	}
}
