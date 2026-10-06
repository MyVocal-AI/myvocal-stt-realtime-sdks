package sttrealtime

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------- A05

func TestA05FilePacedLiveNot(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	path := filepath.Join(repoRoot, "fixtures", "audio", "tone_pcm_s16le_16000.raw")
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("A05-pacing", "")})
	t0 := time.Now()
	if err := s.SendFile(ctxT(t), path, true); err != nil {
		t.Fatal(err)
	}
	paced := time.Since(t0)
	_, _ = s.Finish(ctxT(t))
	_ = s.Close()
	frames := f.record(t, s.SessionID).Frames
	span := frames[len(frames)-1]["t"].(float64) - frames[0]["t"].(float64)
	s2, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("A05-pacing", "")})
	t0 = time.Now()
	sendAll(t, s2, audio(t, "pcm_s16le_16000", 0), 1<<20)
	live := time.Since(t0)
	_, _ = s2.Finish(ctxT(t))
	_ = s2.Close()
	if paced < 1100*time.Millisecond || span < 1.0 || live > 600*time.Millisecond {
		t.Fatalf("paced %v span %v live %v", paced, span, live)
	}
	if f.record(t, s.SessionID).StoredSha256 != f.record(t, s2.SessionID).StoredSha256 {
		t.Fatal("sha differs")
	}
}

func TestA05BoundedBufferBackpressure(t *testing.T) {
	f := fx(t)
	c := newClient(t, f, func(cfg *Config) { cfg.MaxBufferedMs = 300 })
	data := audio(t, "pcm_s16le_16000", 0)
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("A05-backpressure", "")})
	sendAll(t, s, data, 5000)
	st := s.Stats()
	r, err := s.Finish(ctxT(t))
	_ = s.Close()
	if err != nil || st.PeakBufferedSamples > st.MaxBufferedSamples || st.MaxBufferedSamples != 4800 {
		t.Fatalf("%v %+v", err, st)
	}
	if f.record(t, s.SessionID).StoredSha256 != sha(data) || r.Status != "COMPLETED" || s.Stats().BufferedSamples != 0 {
		t.Fatal("backpressure result")
	}
}

func TestA05TwoSessionsIsolated(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	a, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("A05-concurrent", "")})
	b, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("A05-concurrent", "mulaw_8000")})
	da, db := audio(t, "pcm_s16le_16000", 0), audio(t, "mulaw_8000", 0)
	var wg sync.WaitGroup
	results := make([]*FinishResult, 2)
	for i, pair := range []struct {
		s *Session
		d []byte
	}{{a, da}, {b, db}} {
		wg.Add(1)
		go func(i int, s *Session, d []byte) {
			defer wg.Done()
			sendAll(t, s, d, 999)
			results[i], _ = s.Finish(context.Background())
		}(i, pair.s, pair.d)
	}
	wg.Wait()
	if a.Cursors().CapturedSamples != 20000 || b.Cursors().CapturedSamples != 10000 {
		t.Fatal("captured")
	}
	_ = a.Close()
	_ = b.Close()
	if results[0].SessionID == results[1].SessionID || results[0].TranscriptionID == results[1].TranscriptionID {
		t.Fatal("ids shared")
	}
	if f.record(t, a.SessionID).StoredSha256 != sha(da) || f.record(t, b.SessionID).StoredSha256 != sha(db) {
		t.Fatal("sha")
	}
}

// ---------------------------------------------------------------- A06

func lifecycle(t *testing.T, title string, via Control) {
	f := fx(t)
	c := newClient(t, f)
	ctx := ctxT(t)
	data := audio(t, "pcm_s16le_16000", 0)
	q := len(data) / 4 / 2 * 2
	s, err := c.Connect(ctx, ConnectOptions{Request: req(title, "")})
	if err != nil {
		t.Fatal(err)
	}
	sendAll(t, s, data[:q], 4096)
	if err := s.Commit(ctx); err != nil || s.Cursors().Epoch != 1 {
		t.Fatal(err)
	}
	sendAll(t, s, data[q:2*q], 4096)
	if err := s.Pause(ctx, via); err != nil {
		t.Fatal(err)
	}
	var se *SessionStateError
	if err := s.SendAudio(ctx, data[2*q:3*q]); !errors.As(err, &se) {
		t.Fatalf("paused send %v", err)
	}
	if err := s.Resume(ctx, via); err != nil {
		t.Fatal(err)
	}
	if cur := s.Cursors(); cur.Epoch != 2 || cur.SampleOffset != 0 {
		t.Fatalf("%+v", cur)
	}
	sendAll(t, s, data[2*q:], 4096)
	r, err := s.Finish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r2, _ := s.Finish(ctx)
	view, _ := c.GetSession(ctx, s.SessionID)
	rest, _ := c.FinishSession(ctx, s.SessionID, nil)
	_ = s.Close()
	rec := f.record(t, s.SessionID)
	if r != r2 || rec.StoredSha256 != sha(data) || len(rec.Epochs) != 2 || i64(rec.Epochs[1]["sent"]) != int64((len(data)-2*q)/2) ||
		num(rec.Captured) != int64(len(data)/2) {
		t.Fatalf("lifecycle rec %+v", rec.Epochs)
	}
	if r.Status != "COMPLETED" || view.Status != "COMPLETED" || rest.Status != "COMPLETED" ||
		r.TranscriptionID != view.TranscriptionID || r.TranscriptionID != rest.TranscriptionID ||
		*r.Usage.BillableCharacters != *view.Usage.BillableCharacters || *r.Usage.BillableCharacters != *rest.Usage.BillableCharacters {
		t.Fatal("finish/GET mismatch")
	}
	if rec.FinishCount != 2 || num(rec.Settled) != *r.Usage.BillableCharacters || rec.SettledCount != 1 || len(r.Lines) != 3 {
		t.Fatalf("finish %d settled %d lines %d", rec.FinishCount, rec.SettledCount, len(r.Lines))
	}
}

func TestA06ManualLifecycleWS(t *testing.T)   { lifecycle(t, "A06-manual", ViaWS) }
func TestA06ManualLifecycleREST(t *testing.T) { lifecycle(t, "A06-rest-control", ViaREST) }

func TestA06VAD(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: &SessionRequest{Title: "A06-vad",
		Options: &SessionOptions{InputEncoding: "pcm_s16le_16000", SegmentCommitMode: "vad"}}})
	sendAll(t, s, audio(t, "pcm_s16le_16000", 0), 1<<20)
	r, _ := s.Finish(ctxT(t))
	_ = s.Close()
	if r.Status != "COMPLETED" || len(r.Lines) < 3 || f.record(t, s.SessionID).Effective["segmentCommitMode"] != "vad" {
		t.Fatal(r)
	}
}

// ---------------------------------------------------------------- A07

func TestA07Rotation(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	cases := []struct {
		id    string
		chunk int
	}{{"A07-rotate-mid", 0}, {"A07-rotate-last", 0}, {"A07-rotate-tail", 0}, {"A07-rotate-ack-delay", 777},
		{"A07-rotate-twice", 1600}, {"A07-old-epoch-late", 0}}
	for _, tc := range cases {
		exp := scenario(tc.id)["expect"].(map[string]any)
		secs, _ := exp["inputSeconds"].(float64)
		data := audio(t, "pcm_s16le_16000", secs)
		s, err := c.Connect(ctxT(t), ConnectOptions{Request: req(tc.id, "")})
		if err != nil {
			t.Fatal(err)
		}
		chunk := tc.chunk
		if chunk == 0 {
			chunk = len(data)
		}
		sendAll(t, s, data, chunk)
		r, err := s.Finish(ctxT(t))
		if err != nil {
			t.Fatal(tc.id, err)
		}
		_ = s.Close()
		rec := f.record(t, s.SessionID)
		total := int64(len(data) / 2)
		var sum, maxClaim int64
		for _, e := range rec.Epochs {
			sum += i64(e["sent"])
		}
		for _, cl := range rec.CapturedClaims {
			if v, _ := cl.Int64(); v > maxClaim {
				maxClaim = v
			}
		}
		epochs := int(exp["epochs"].(float64))
		if rec.StoredSha256 != sha(data) || len(rec.Epochs) != epochs || sum != total || maxClaim != total ||
			num(rec.Captured) != total || r.Status != "COMPLETED" || s.Cursors().Epoch != int64(epochs) ||
			s.Stats().Rotations != epochs-1 {
			t.Fatalf("%s epochs %d sum %d claim %d", tc.id, len(rec.Epochs), sum, maxClaim)
		}
	}
}

// ---------------------------------------------------------------- A08

func TestA08AllEventsAndSupplements(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	var mu sync.Mutex
	var events []Event
	var cbErrs []error
	s, err := c.Connect(ctxT(t), ConnectOptions{Request: req("A08-events-all", ""),
		OnEvent:         func(e Event) { mu.Lock(); events = append(events, e); mu.Unlock() },
		OnCallbackError: func(err error, e Event) { mu.Lock(); cbErrs = append(cbErrs, err); mu.Unlock() }})
	if err != nil {
		t.Fatal(err)
	}
	data := audio(t, "pcm_s16le_16000", 0)
	sendAll(t, s, data[:16000], 16000)
	_ = s.Commit(ctxT(t))
	sendAll(t, s, data[16000:], 1<<20)
	r, err := s.Finish(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	mu.Lock()
	defer mu.Unlock()
	types := map[string]bool{}
	var seqs []int64
	for _, e := range events {
		types[e.Type] = true
		seqs = append(seqs, *e.Sequence)
	}
	for _, et := range EventTypes {
		if !types[et] {
			t.Fatalf("missing %s", et)
		}
	}
	if len(types) != 10 || !sort.SliceIsSorted(seqs, func(i, j int) bool { return seqs[i] < seqs[j] }) || seqs[0] != 1 {
		t.Fatalf("types %v", types)
	}
	if len(r.Lines) != 2 || r.Lines[0].ID != "line_1" || r.Lines[1].ID != "line_2" {
		t.Fatalf("lines %v", r.Lines)
	}
	l1 := r.Lines[0]
	if !l1.AlignmentArrived || l1.LanguageCode == nil || *l1.LanguageCode != "en" || len(l1.Words) == 0 || *l1.Words[0].StartMs != 0 {
		t.Fatalf("alignment %+v", l1)
	}
	if !l1.EntitiesArrived || len(l1.Entities) != 0 || !l1.RevisionArrived || *l1.RevisedText != "HELLO WORLD" {
		t.Fatalf("supplements %+v", l1)
	}
	var missing *Notice
	codes := map[string]bool{}
	for i, n := range r.Notices {
		codes[n.Code] = true
		if n.Code == "SUPPLEMENT_NOT_RECEIVED" {
			missing = &r.Notices[i]
		}
	}
	if !codes["PROCESSING_STORAGE_NOT_APPLIED"] || missing == nil || missing.Supplements[0] != "language" || missing.LineIDs[0] != "line_1" {
		t.Fatalf("notices %+v", r.Notices)
	}
	if r.Status != "COMPLETED" || len(cbErrs) != 0 || r.ServerErrors[0] != "TEMPORARILY_UNAVAILABLE" {
		t.Fatal(r.Status, cbErrs, r.ServerErrors)
	}
}

func TestA08EntitiesNotArrivedVersusArrived(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	run := func(title string) *FinishResult {
		s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req(title, "")})
		sendAll(t, s, audio(t, "pcm_s16le_16000", 0), 1<<20)
		r, _ := s.Finish(ctxT(t))
		_ = s.Close()
		return r
	}
	r1, r2 := run("L-basic"), run("A08-entities-some")
	if r1.Lines[0].EntitiesArrived || r1.Lines[0].Entities != nil {
		t.Fatal("not arrived expected")
	}
	if !r2.Lines[0].EntitiesArrived || r2.Lines[0].Entities[0]["text"] != "Alice" {
		t.Fatal("arrived expected")
	}
}

func TestA08TransportCloseAfterTerminalIsNotFailure(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("A08-close-after-completed", "")})
	sendAll(t, s, audio(t, "pcm_s16le_16000", 0), 1<<20)
	r, _ := s.Finish(ctxT(t))
	time.Sleep(200 * time.Millisecond)
	_ = s.Close()
	if r.Status != "COMPLETED" || r.ViaREST || r.CloseoutReason != "" || f.record(t, s.SessionID).RestFinishCount != 0 {
		t.Fatal(r)
	}
}
