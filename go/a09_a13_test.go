package sttrealtime

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var short = Timeouts{Request: 3 * time.Second, Create: 3 * time.Second, Connect: 3 * time.Second, Ack: time.Second,
	Finish: 1500 * time.Millisecond, Closeout: 4 * time.Second, Close: 2 * time.Second}

func shortClient(t *testing.T, f *fixture) *Client {
	return newClient(t, f, func(c *Config) { c.Timeouts = short })
}

// sendUntilError streams with small pauses and tolerates the disconnect error.
func sendUntilError(s *Session, data []byte) error {
	for i := 0; i < len(data); i += 3200 {
		end := minInt(i+3200, len(data))
		if err := s.SendAudio(context.Background(), data[i:end]); err != nil {
			return err
		}
		time.Sleep(30 * time.Millisecond)
	}
	return nil
}

// ---------------------------------------------------------------- A09

func TestA09Int64NullAndWireTypes(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	big := int64(9007199254740993)
	s, err := c.Connect(ctxT(t), ConnectOptions{Request: req("A09-bigints", "")})
	if err != nil {
		t.Fatal(err)
	}
	if cur := s.Cursors(); *cur.ReceivedSamples != big || cur.CapturedSamples != big {
		t.Fatalf("%+v", cur)
	}
	sendAll(t, s, audio(t, "pcm_s16le_16000", 0), 1<<20)
	r, err := s.Finish(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	rec := f.record(t, s.SessionID)
	var maxClaim int64
	for _, cl := range rec.CapturedClaims {
		if v, _ := cl.Int64(); v > maxClaim {
			maxClaim = v
		}
	}
	if *r.Usage.ElapsedMs != 9223372036854775807 || *r.Usage.ReceivedSamples != big+20000 || maxClaim != big+20000 {
		t.Fatalf("usage %d %d claim %d", *r.Usage.ElapsedMs, *r.Usage.ReceivedSamples, maxClaim)
	}
	for _, fr := range rec.Frames {
		if fr["sampleOffsetType"] != "str" || fr["capturedType"] != "str" {
			t.Fatalf("wire types %v", fr)
		}
	}
	w := r.Lines[0].Words
	if *w[0].StartMs < big || w[len(w)-1].StartMs != nil || w[len(w)-1].EndMs != nil {
		t.Fatal("words")
	}
}

func TestA09SequencePerConnection(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	var mu sync.Mutex
	seqs := map[int][]int64{}
	rec := func(k int) func(Event) {
		return func(e Event) { mu.Lock(); seqs[k] = append(seqs[k], *e.Sequence); mu.Unlock() }
	}
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("L-basic", ""), OnEvent: rec(1)})
	sendAll(t, s, audio(t, "pcm_s16le_16000", 0.5), 1<<20)
	_ = s.Commit(ctxT(t))
	_ = s.Close()
	time.Sleep(200 * time.Millisecond)
	s2, err := c.Connect(ctxT(t), ConnectOptions{SessionID: s.SessionID, OnEvent: rec(2)})
	if err != nil {
		t.Fatal(err)
	}
	if s2.Cursors().CapturedSamples != 8000 {
		t.Fatalf("restored %+v", s2.Cursors())
	}
	if err := s2.Resume(ctxT(t), ViaWS); err != nil {
		t.Fatal(err)
	}
	sendAll(t, s2, audio(t, "pcm_s16le_16000", 0)[16000:], 1<<20)
	r, _ := s2.Finish(ctxT(t))
	_ = s2.Close()
	mu.Lock()
	defer mu.Unlock()
	if seqs[1][0] != 1 || seqs[2][0] != 1 || s2.Stats().SequenceAnomalies != 0 || r.Status != "COMPLETED" ||
		*r.Usage.ReceivedSamples != 20000 {
		t.Fatal(seqs[1][0], seqs[2][0], r)
	}
}

// ---------------------------------------------------------------- A10

func TestA10AckLostThenDisconnect(t *testing.T) {
	f := fx(t)
	c := shortClient(t, f)
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("A10-ack-lost-disconnect", "")})
	if err := sendUntilError(s, audio(t, "pcm_s16le_16000", 0)); err == nil {
		t.Fatal("expected the send path to report the lost connection")
	}
	r, err := s.Finish(ctxT(t))
	_ = s.Close()
	if err != nil {
		t.Fatal(err)
	}
	rec := f.record(t, s.SessionID)
	if !r.ViaREST || r.CloseoutReason != "DISCONNECTED" || len(rec.Connections) != 1 || rec.RestFinishCount != 1 ||
		len(f.byTitle(t, "A10-ack-lost-disconnect")) != 1 || r.Status != rec.Status {
		t.Fatalf("%v rec %s", r, rec.Status)
	}
}

func TestA10AlreadyTerminalOnQuery(t *testing.T) {
	f := fx(t)
	c := shortClient(t, f)
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("A10-terminal-on-query", "")})
	_ = sendUntilError(s, audio(t, "pcm_s16le_16000", 0))
	r, err := s.Finish(ctxT(t))
	_ = s.Close()
	rec := f.record(t, s.SessionID)
	if err != nil || !r.ViaREST || rec.RestFinishCount != 0 || r.Status != rec.Status {
		t.Fatal(err, r)
	}
}

func TestA10RESTUnreachableKeepsSessionID(t *testing.T) {
	f := fx(t)
	c := shortClient(t, f)
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("A10-rest-unreachable", "")})
	_ = sendUntilError(s, audio(t, "pcm_s16le_16000", 0))
	_, err := s.Finish(ctxT(t))
	var fu *FinishUnconfirmedError
	if !errors.As(err, &fu) || fu.SessionID != s.SessionID {
		t.Fatalf("err %v", err)
	}
	_ = s.Close()
	f.post(t, "/sessions/"+s.SessionID+"/rest-up")
	v, _ := c.GetSession(ctxT(t), s.SessionID)
	done, err := c.FinishSession(ctxT(t), s.SessionID, nil)
	if v.IsTerminal() || err != nil || !done.IsTerminal() || len(f.byTitle(t, "A10-rest-unreachable")) != 1 ||
		len(f.record(t, s.SessionID).Connections) != 1 {
		t.Fatal(v.Status, err, done.Status)
	}
}

func TestA10DisconnectWithoutFinishClosesOutInBackground(t *testing.T) {
	f := fx(t)
	c := shortClient(t, f)
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("L-basic", "")})
	sendAll(t, s, audio(t, "pcm_s16le_16000", 0.5), 1<<20)
	_ = s.Commit(ctxT(t))
	s.AbortConnection()
	r, err := s.Finish(ctxT(t))
	_ = s.Close()
	if err != nil || !r.ViaREST || r.CloseoutReason != "DISCONNECTED" || r.Status != "COMPLETED" ||
		len(f.record(t, s.SessionID).Connections) != 1 {
		t.Fatal(err, r)
	}
}

// ---------------------------------------------------------------- A11

func finishOf(t *testing.T, c *Client, title string, data []byte) (*Session, *FinishResult, time.Duration, error) {
	s, err := c.Connect(ctxT(t), ConnectOptions{Request: req(title, "")})
	if err != nil {
		t.Fatal(err)
	}
	sendAll(t, s, data, 1<<20)
	t0 := time.Now()
	r, err := s.Finish(ctxT(t))
	took := time.Since(t0)
	_ = s.Close()
	return s, r, took, err
}

func TestA11NoAckProgress(t *testing.T) {
	_, r, took, err := finishOf(t, shortClient(t, fx(t)), "A11-no-ack", audio(t, "pcm_s16le_16000", 0.3))
	if err != nil || r.CloseoutReason != "ACK_TIMEOUT" || !r.ViaREST || took > 6*time.Second {
		t.Fatal(err, r, took)
	}
}

func TestA11IdleIsNotAckTimeout(t *testing.T) {
	c := shortClient(t, fx(t))
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("L-basic", "")})
	sendAll(t, s, audio(t, "pcm_s16le_16000", 0.3), 1<<20)
	_ = s.Commit(ctxT(t))
	time.Sleep(1600 * time.Millisecond)
	sendAll(t, s, audio(t, "pcm_s16le_16000", 0)[9600:], 1<<20)
	r, err := s.Finish(ctxT(t))
	_ = s.Close()
	if err != nil || r.Status != "COMPLETED" || r.ViaREST {
		t.Fatal(err, r)
	}
}

func TestA11CompletedEventLost(t *testing.T) {
	f := fx(t)
	s, r, _, err := finishOf(t, shortClient(t, f), "A11-completed-lost", audio(t, "pcm_s16le_16000", 0))
	if err != nil || !r.ViaREST || r.Status != "COMPLETED" || r.TranscriptionID != f.record(t, s.SessionID).TranscriptionID {
		t.Fatal(err, r)
	}
}

func TestA11FinishTimeoutBounded(t *testing.T) {
	_, r, took, err := finishOf(t, shortClient(t, fx(t)), "A11-finish-timeout", audio(t, "pcm_s16le_16000", 0))
	if err != nil || r.CloseoutReason != "FINISH_TIMEOUT" || took > 7*time.Second || r.Status != "COMPLETED" {
		t.Fatal(err, r, took)
	}
}

// Every REST answer of this session takes 1.2 s. The close-out budget bounds the whole
// GET/finish sequence (context deadline), so a 300 ms budget ends in ~300 ms with the
// sessionId kept, and an adequate budget still closes out normally.
func TestA11CloseoutDeadlineBoundsEveryRESTRequest(t *testing.T) {
	f := fx(t)
	run := func(closeout time.Duration) (*Session, *FinishResult, time.Duration, error) {
		c := newClient(t, f, func(cfg *Config) {
			cfg.Timeouts = short
			cfg.Timeouts.Closeout = closeout
			cfg.Timeouts.Close = 500 * time.Millisecond
		})
		s, err := c.Connect(ctxT(t), ConnectOptions{Request: req("A11-slow-rest", "")})
		if err != nil {
			t.Fatal(err)
		}
		sendAll(t, s, audio(t, "pcm_s16le_16000", 0.1), 1<<20)
		if err := s.Commit(ctxT(t)); err != nil {
			t.Fatal(err)
		}
		t0 := time.Now()
		s.AbortConnection()
		r, err := s.Finish(ctxT(t))
		took := time.Since(t0)
		_ = s.Close()
		return s, r, took, err
	}
	s, _, took, err := run(300 * time.Millisecond)
	var fu *FinishUnconfirmedError
	st := f.record(t, s.SessionID).Status
	if !errors.As(err, &fu) || fu.SessionID != s.SessionID || took >= 900*time.Millisecond ||
		st == "COMPLETED" || st == "PARTIAL" || st == "FAILED" {
		t.Fatal(err, took, st)
	}
	_, r, took, err := run(4 * time.Second)
	if err != nil || !r.ViaREST || r.Status != "COMPLETED" || r.CloseoutReason != "DISCONNECTED" || took >= 4500*time.Millisecond {
		t.Fatal(err, r, took)
	}
}

func TestA11RESTFinishResponseLostIsIdempotent(t *testing.T) {
	f := fx(t)
	c := shortClient(t, f)
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("A11-rest-finish-lost", "")})
	_ = sendUntilError(s, audio(t, "pcm_s16le_16000", 0))
	r, err := s.Finish(ctxT(t))
	_ = s.Close()
	rec := f.record(t, s.SessionID)
	if err != nil || !r.ViaREST || r.Status != rec.Status || rec.SettledCount != 1 {
		t.Fatal(err, r, rec.SettledCount)
	}
}

func TestA11InsufficientBalanceNotRetried(t *testing.T) {
	f := fx(t)
	c := shortClient(t, f)
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("A11-insufficient", "")})
	err := sendUntilError(s, audio(t, "pcm_s16le_16000", 0))
	if err == nil {
		err = s.Commit(ctxT(t))
	}
	if !errors.Is(err, ErrInsufficientCharacters) {
		t.Fatalf("err %v", err)
	}
	r, err := s.Finish(ctxT(t))
	_ = s.Close()
	rec := f.record(t, s.SessionID)
	seen := false
	for _, u := range rec.UsageReplies {
		if u["errorCode"] == "INSUFFICIENT_CHARACTERS" {
			seen = true
		}
	}
	for _, fr := range rec.Frames {
		if i64(fr["n"]) >= 3 && fr["accepted"] == true {
			t.Fatal("frame retried after balance stop")
		}
	}
	if err != nil || !seen || r.Status != rec.Status {
		t.Fatal(err, r)
	}
}

func TestA11RetryableFrameErrorReplayedBounded(t *testing.T) {
	f := fx(t)
	data := audio(t, "pcm_s16le_16000", 0)
	s, r, _, err := finishOf(t, newClient(t, f), "A11-temp-error-frame", data)
	if err != nil || r.Status != "COMPLETED" || f.record(t, s.SessionID).StoredSha256 != sha(data) ||
		!strings.Contains(strings.Join(r.ServerErrors, ","), "TEMPORARILY_UNAVAILABLE") {
		t.Fatal(err, r)
	}
}

func TestA11PartialFailedAsIs(t *testing.T) {
	c := newClient(t, fx(t))
	for id, want := range map[string]string{"A11-partial": "PARTIAL", "A11-failed": "FAILED"} {
		_, r, _, err := finishOf(t, c, id, audio(t, "pcm_s16le_16000", 0))
		if err != nil || r.Status != want {
			t.Fatal(id, err, r)
		}
	}
}

// ---------------------------------------------------------------- A12

func TestA12SlowCallbackDoesNotBlockAcks(t *testing.T) {
	c := newClient(t, fx(t))
	var n atomic.Int64
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("A12-slow-callback", ""), Handlers: map[string]func(Event){
		EventTranscriptFinal: func(Event) { time.Sleep(300 * time.Millisecond); n.Add(1) }}})
	t0 := time.Now()
	sendAll(t, s, audio(t, "pcm_s16le_16000", 0), 1<<20)
	r, err := s.Finish(ctxT(t))
	took := time.Since(t0)
	_ = s.Close()
	if err != nil || took > 1500*time.Millisecond || r.Status != "COMPLETED" || n.Load() == 0 {
		t.Fatal(err, took, n.Load())
	}
}

func TestA12CallbackPanicObservable(t *testing.T) {
	c := newClient(t, fx(t))
	var mu sync.Mutex
	var errs []string
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("A12-callback-throws", ""),
		Handlers:        map[string]func(Event){EventUsageUpdated: func(Event) { panic("handler bug") }},
		OnCallbackError: func(err error, e Event) { mu.Lock(); errs = append(errs, e.Type); mu.Unlock() }})
	sendAll(t, s, audio(t, "pcm_s16le_16000", 0), 1<<20)
	r, err := s.Finish(ctxT(t))
	_ = s.Close()
	mu.Lock()
	defer mu.Unlock()
	if err != nil || r.Status != "COMPLETED" || len(errs) == 0 || errs[0] != EventUsageUpdated ||
		s.CallbackErrors() != int64(len(errs)) || len(r.ServerErrors) != 0 {
		t.Fatal(err, r, errs)
	}
}

func TestA12OverflowReportedResultComplete(t *testing.T) {
	c := newClient(t, fx(t), func(cfg *Config) { cfg.EventQueueSize = 2 })
	gate := make(chan struct{})
	var reported atomic.Int64
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("A12-overflow", ""),
		OnEvent: func(Event) {
			select {
			case <-gate:
			case <-time.After(2 * time.Second):
			}
		},
		OnCallbackError: func(error, Event) { reported.Add(1) }})
	sendAll(t, s, audio(t, "pcm_s16le_16000", 0), 1<<20)
	_ = s.Commit(ctxT(t))
	r, err := s.Finish(ctxT(t))
	close(gate)
	_ = s.Close()
	time.Sleep(100 * time.Millisecond)
	if err != nil || r.DroppedEvents == 0 || reported.Load() < r.DroppedEvents || r.Status != "COMPLETED" ||
		len(r.Lines) == 0 || r.Lines[0].Text == "" {
		t.Fatal(err, r, reported.Load())
	}
}

func TestA12IdlePingWithoutAudio(t *testing.T) {
	f := fx(t)
	c := newClient(t, f, func(cfg *Config) { cfg.PingInterval = 200 * time.Millisecond })
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("A12-idle-ping", "")})
	time.Sleep(1200 * time.Millisecond)
	rec := f.record(t, s.SessionID)
	r, _ := s.Finish(ctxT(t))
	_ = s.Close()
	if i64(rec.Connections[0]["pings"]) < 3 || len(rec.Frames) != 0 || (rec.Captured != nil && num(rec.Captured) != 0) ||
		*r.Usage.ReceivedSamples != 0 {
		t.Fatal(rec.Connections[0]["pings"], len(rec.Frames))
	}
}

func TestA12TLSVerificationDefaultAndCustomCA(t *testing.T) {
	f := fxTLS(t)
	c, _ := NewClient(Config{AccessKey: fixtureKey, BaseURL: f.Base, Proxy: NoProxy})
	_, err := c.CreateSession(ctxT(t), *req("A12-network", ""), "")
	var te *TransportError
	if !errors.As(err, &te) {
		t.Fatalf("default TLS must reject the private CA: %v", err)
	}
	c2 := newClient(t, f, func(cfg *Config) { cfg.TLSConfig = testTLS(t) })
	s, err := c2.Connect(ctxT(t), ConnectOptions{Request: req("A12-network", "")})
	if err != nil {
		t.Fatal(err)
	}
	sendAll(t, s, audio(t, "pcm_s16le_16000", 0), 1<<20)
	r, _ := s.Finish(ctxT(t))
	_ = s.Close()
	if r.Status != "COMPLETED" {
		t.Fatal(r)
	}
}

func TestA12ProxyAndBorrowedHTTPClient(t *testing.T) {
	f := fxTLS(t)
	var before, after struct {
		Tunnels int `json:"tunnels"`
	}
	f.getJSON(t, "/proxy", &before)
	proxy, _ := ProxyURL(fmt.Sprintf("http://127.0.0.1:%d", f.ProxyPort))
	c := newClient(t, f, func(cfg *Config) { cfg.TLSConfig = testTLS(t); cfg.Proxy = proxy })
	s, err := c.Connect(ctxT(t), ConnectOptions{Request: req("A12-network", "")})
	if err != nil {
		t.Fatal(err)
	}
	sendAll(t, s, audio(t, "pcm_s16le_16000", 0), 1<<20)
	_, _ = s.Finish(ctxT(t))
	_ = s.Close()
	f.getJSON(t, "/proxy", &after)
	if after.Tunnels < before.Tunnels+2 {
		t.Fatalf("tunnels %d -> %d", before.Tunnels, after.Tunnels)
	}
	tr := &http.Transport{TLSClientConfig: testTLS(t), Proxy: nil}
	borrowed := &http.Client{Transport: tr}
	c2, _ := NewClient(Config{AccessKey: fixtureKey, BaseURL: f.Base, HTTPClient: borrowed, TLSConfig: testTLS(t), Proxy: NoProxy})
	v, err := c2.CreateSession(ctxT(t), *req("A12-network", ""), "")
	if err != nil {
		t.Fatal(err)
	}
	_ = c2.Close()
	if _, err := borrowed.Get(f.Base + "/__fixture/health"); err != nil { // still usable: never closed by the SDK
		t.Fatal(err)
	}
	_, _ = c.FinishSession(ctxT(t), v.SessionID, nil)
}

func TestA12CancelFinishAndIdempotentClose(t *testing.T) {
	c := newClient(t, fx(t))
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("A12-cancel", "")})
	sendAll(t, s, audio(t, "pcm_s16le_16000", 0), 1<<20)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(500 * time.Millisecond); cancel() }()
	if _, err := s.Finish(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
	_ = s.Close()
	_ = s.Close()
	time.Sleep(3 * time.Second)
	done, err := c.FinishSession(ctxT(t), s.SessionID, nil)
	if err != nil || !done.IsTerminal() {
		t.Fatal(err, done)
	}
}

func TestA12GoroutinesReleased(t *testing.T) {
	f := fx(t)
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	before := runtime.NumGoroutine()
	for i := 0; i < 3; i++ {
		c, _ := NewClient(Config{AccessKey: fixtureKey, BaseURL: f.Base, Proxy: NoProxy})
		s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("L-basic", "")})
		sendAll(t, s, audio(t, "pcm_s16le_16000", 0), 1<<20)
		_, _ = s.Finish(ctxT(t))
		_ = s.Close()
		_ = c.Close()
	}
	time.Sleep(500 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before+2 {
		buf := make([]byte, 1<<16)
		t.Fatalf("goroutines %d -> %d\n%s", before, after, buf[:runtime.Stack(buf, true)])
	}
}

// ---------------------------------------------------------------- A13

func TestA13NoSecretOrContentInStringsAndErrors(t *testing.T) {
	f := fx(t)
	c := newClient(t, f)
	s, _ := c.Connect(ctxT(t), ConnectOptions{Request: req("A08-events-all", "")})
	tk, _ := c.CreateTicket(ctxT(t), s.SessionID)
	sendAll(t, s, audio(t, "pcm_s16le_16000", 0), 1<<20)
	r, _ := s.Finish(ctxT(t))
	_ = s.Close()
	bad, _ := NewClient(Config{AccessKey: "bad-SECRET-key", BaseURL: f.Base, Proxy: NoProxy})
	_, err := bad.CreateSession(ctxT(t), *req("x", ""), "")
	blob := fmt.Sprintf("%v %+v %v %v %v %v %v %v", c, c, s, tk, r, r.Lines, r.View, err)
	for _, bad := range []string{fixtureKey, "bad-SECRET-key", tk.Value, "hello world", "audioBase64"} {
		if strings.Contains(blob, bad) {
			t.Fatalf("leaked %q", bad)
		}
	}
}
