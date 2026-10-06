// Real-API validation with the Go SDK (plan v1.1 section 7): R1 pcm_s16le_16000 VAD + mulaw_8000 manual,
// R2 language + alignment, R3 commit/continue, pause/resume, ticket helper, repeated finish,
// R4 client-only disconnect close-out and 75 s idle with ping.
// Reads MYVOCAL_ACCESS_KEY / MYVOCAL_API_BASE from the environment only; writes sanitized records.
//
//	go run . <audio dir> <out dir> [idleSeconds]
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	stt "github.com/MyVocal-AI/myvocal-stt-realtime-sdks/go"
)

type rec map[string]any

var (
	records []rec
	outPath string
)

func main() {
	key := os.Getenv("MYVOCAL_ACCESS_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "MYVOCAL_ACCESS_KEY is not set")
		os.Exit(2)
	}
	audio, out := os.Args[1], os.Args[2]
	outPath = filepath.Join(out, "live-go.json")
	idle := 75
	if len(os.Args) > 3 {
		idle, _ = strconv.Atoi(os.Args[3])
	}
	const prod = "https://api.myvocal.ai"
	base := strings.TrimRight(os.Getenv("MYVOCAL_API_BASE"), "/")
	if base == "" {
		base = prod
	}
	if base != prod && !strings.HasPrefix(key, "fixture-") {
		fmt.Fprintln(os.Stderr, "refusing: a non-fixture key may only be sent to "+prod)
		os.Exit(2)
	}
	c, err := stt.NewClient(stt.Config{AccessKey: key, BaseURL: base})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(3)
	}
	defer c.Close()
	ctx := context.Background()
	guard("R1-pcm_s16le_16000-vad-language+alignment", func() { r1(ctx, c, audio, "pcm_s16le_16000", "vad", 2, true) })
	guard("R1-mulaw_8000-manual", func() { r1(ctx, c, audio, "mulaw_8000", "manual", 1, false) })
	guard("R3-lifecycle-commit-pause-resume-ticket-refinish", func() { r3(ctx, c, audio) })
	guard("R4-client-disconnect-closeout", func() { r4Disconnect(ctx, c, audio) })
	guard("R4-idle-"+strconv.Itoa(idle)+"s-ping", func() { r4Idle(ctx, c, audio, idle) })
	bad := 0
	for _, r := range records {
		if r["ok"] != true {
			bad++
		}
	}
	fmt.Printf("records: %d, needing review: %d\n", len(records), bad)
	if bad > 0 {
		os.Exit(1)
	}
}

func raw(audio, enc string) []byte {
	b, err := os.ReadFile(filepath.Join(audio, "speech_"+enc+".raw"))
	must(err)
	return b
}

func req(scen, enc, mode string) *stt.SessionRequest {
	return &stt.SessionRequest{Title: "sdk-live go " + scen, LanguageHint: "en",
		Options: &stt.SessionOptions{InputEncoding: enc, SegmentCommitMode: mode}}
}

func r1(ctx context.Context, c *stt.Client, audio, enc, mode string, bps int, langAlign bool) {
	scen := "R1-" + enc + "-" + mode
	if langAlign {
		scen += "-language+alignment"
	}
	data := raw(audio, enc)
	r := req(scen, enc, mode)
	if langAlign {
		r.Options.IncludeAlignment = stt.Bool(true)
		r.Options.IncludeDetectedLanguage = stt.Bool(true)
	}
	s, err := c.Connect(ctx, stt.ConnectOptions{Request: r})
	must(err)
	defer s.Close()
	if mode == "manual" {
		half := len(data) / 2 / bps * bps
		must(s.SendReader(ctx, bytes.NewReader(data[:half]), true))
		must(s.Commit(ctx))
		must(s.SendReader(ctx, bytes.NewReader(data[half:]), true))
	} else {
		must(s.SendReader(ctx, bytes.NewReader(data), true))
	}
	res, err := s.Finish(ctx)
	must(err)
	exp := int64(len(data) / bps)
	m := summarize(scen, enc, res, exp)
	align, langs := false, []string{}
	for _, l := range res.Lines {
		align = align || l.AlignmentArrived
		if l.LanguageCode != nil {
			langs = append(langs, *l.LanguageCode)
		}
	}
	m["alignmentArrived"], m["languageCodes"] = align, langs
	m["ok"] = eq(res.Usage.CapturedSamples, exp) && eq(res.Usage.ReceivedSamples, exp) && res.Status == "COMPLETED" && res.Text() != ""
	add(m)
}

func r3(ctx context.Context, c *stt.Client, audio string) {
	scen, enc := "R3-lifecycle-commit-pause-resume-ticket-refinish", "pcm_s16le_16000"
	data := raw(audio, enc)
	q := len(data) / 4 / 2 * 2
	s, err := c.Connect(ctx, stt.ConnectOptions{Request: req(scen, enc, "manual")})
	must(err)
	defer s.Close()
	t, err := c.CreateTicket(ctx, s.SessionID)
	must(err)
	ticketOK := t.Value != "" && t.SessionID == s.SessionID
	must(s.SendReader(ctx, bytes.NewReader(data[:q]), true))
	must(s.Commit(ctx))
	must(s.SendReader(ctx, bytes.NewReader(data[q:2*q]), true))
	must(s.Pause(ctx, stt.ViaWS))
	e1 := s.Cursors().Epoch
	time.Sleep(2 * time.Second)
	must(s.Resume(ctx, stt.ViaREST))
	e2 := s.Cursors().Epoch
	must(s.SendReader(ctx, bytes.NewReader(data[2*q:]), true))
	r, err := s.Finish(ctx)
	must(err)
	r2, _ := s.Finish(ctx)
	again, err := c.FinishSession(ctx, s.SessionID, nil)
	must(err)
	view, err := c.GetSession(ctx, s.SessionID)
	must(err)
	m := summarize(scen, enc, r, int64(len(data)/2))
	m["ticketIssued"], m["epochs"], m["repeatSameObject"] = ticketOK, []int64{e1, e2}, r == r2
	m["restRefinishBillable"], m["getAfterBillable"] = again.Usage.BillableCharacters, view.Usage.BillableCharacters
	m["ok"] = ticketOK && e2 == e1+1 && r == r2 && r.Status == "COMPLETED" && again.TranscriptionID == r.TranscriptionID &&
		eqp(again.Usage.BillableCharacters, r.Usage.BillableCharacters) && eqp(view.Usage.BillableCharacters, r.Usage.BillableCharacters)
	add(m)
}

func r4Disconnect(ctx context.Context, c *stt.Client, audio string) {
	scen, enc := "R4-client-disconnect-closeout", "pcm_s16le_16000"
	data := raw(audio, enc)
	half := len(data) / 2 / 2 * 2
	s, err := c.Connect(ctx, stt.ConnectOptions{Request: req(scen, enc, "manual")})
	must(err)
	defer s.Close()
	must(s.SendReader(ctx, bytes.NewReader(data[:half]), true))
	must(s.Commit(ctx))
	s.AbortConnection()
	var lost *stt.ConnectionLostError
	rejected := errors.As(s.SendAudio(ctx, data[half:]), &lost)
	r, err := s.Finish(ctx)
	must(err)
	view, err := c.GetSession(ctx, s.SessionID)
	must(err)
	m := summarize(scen, enc, r, int64(half/2))
	m["audioRejectedAfterLoss"], m["getStatus"] = rejected, view.Status
	m["ok"] = rejected && r.ViaREST && r.Status == view.Status
	add(m)
}

func r4Idle(ctx context.Context, c *stt.Client, audio string, idle int) {
	scen, enc := fmt.Sprintf("R4-idle-%ds-ping", idle), "pcm_s16le_16000"
	data := raw(audio, enc)
	head := 32000
	s, err := c.Connect(ctx, stt.ConnectOptions{Request: req(scen, enc, "manual")})
	must(err)
	defer s.Close()
	must(s.SendAudio(ctx, data[:head]))
	must(s.Commit(ctx))
	before := s.Cursors().CapturedSamples
	time.Sleep(time.Duration(idle) * time.Second)
	after := s.Cursors().CapturedSamples
	state := s.State()
	must(s.SendReader(ctx, bytes.NewReader(data[head:]), true))
	r, err := s.Finish(ctx)
	must(err)
	m := summarize(scen, enc, r, int64(len(data)/2))
	m["idleSeconds"], m["capturedBeforeIdle"], m["capturedAfterIdle"], m["stateAfterIdle"] = idle, before, after, state
	m["ok"] = before == after && state == "open" && !r.ViaREST && r.Status == "COMPLETED" && eq(r.Usage.ReceivedSamples, int64(len(data)/2))
	add(m)
}

func eq(p *int64, v int64) bool { return p != nil && *p == v }
func eqp(a, b *int64) bool      { return a != nil && b != nil && *a == *b }

func summarize(scen, enc string, r *stt.FinishResult, expected int64) rec {
	codes := []string{}
	for _, n := range r.Notices {
		codes = append(codes, n.Code)
	}
	return rec{"scenario": scen, "encoding": enc, "sessionId": r.SessionID, "transcriptionId": r.TranscriptionID,
		"status": r.Status, "capturedSamples": r.Usage.CapturedSamples, "receivedSamples": r.Usage.ReceivedSamples,
		"expectedSamples": expected, "billableCharacters": r.Usage.BillableCharacters, "lines": len(r.Lines),
		"textChars": len(r.Text()), "notices": codes, "viaRest": r.ViaREST, "closeoutReason": r.CloseoutReason}
}

func add(m rec) {
	records = append(records, m)
	status := "CHECK"
	if m["ok"] == true {
		status = "OK"
	}
	fmt.Println(m["scenario"], status, m["status"], m["transcriptionId"])
	doc := map[string]any{"language": "go", "updated": time.Now().Format(time.RFC3339),
		"sdk": "github.com/MyVocal-AI/myvocal-stt-realtime-sdks/go v1.0.0 on " + runtime.Version(), "records": records}
	b, _ := json.MarshalIndent(doc, "", "  ")
	_ = os.MkdirAll(filepath.Dir(outPath), 0o755)
	_ = os.WriteFile(outPath, b, 0o644)
}

// must aborts the current scenario; guard turns it into a sanitized error record.
func must(err error) {
	if err != nil {
		panic(err)
	}
}

func guard(scenario string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			m := rec{"scenario": scenario, "ok": false, "status": nil, "transcriptionId": nil,
				"error": fmt.Sprintf("%T", r)}
			if err, ok := r.(error); ok {
				var apiErr *stt.APIError
				if errors.As(err, &apiErr) {
					m["errorCode"] = apiErr.ErrorCode
				}
				msg := err.Error()
				if len(msg) > 300 {
					msg = msg[:300]
				}
				m["message"] = msg
			}
			add(m)
		}
	}()
	fn()
}
