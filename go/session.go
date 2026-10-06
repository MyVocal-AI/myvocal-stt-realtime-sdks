package sttrealtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type state int

const (
	stOpen state = iota
	stPaused
	stFinishing
	stCompleted
	stLost
	stClosed
)

func (s state) String() string {
	return [...]string{"open", "paused", "finishing", "completed", "lost", "closed"}[s]
}

var fatalStreamCodes = map[string]bool{"INPUT_INVALID": true, "PROJECT_NOT_FOUND": true, "INSUFFICIENT_CHARACTERS": true}
var retryableStreamCodes = map[string]bool{"TEMPORARILY_UNAVAILABLE": true, "PROVIDER_UNAVAILABLE": true}

// Cursors is a snapshot of the counters. SampleOffset: samples sent in the current epoch;
// SentSamples: samples the server acknowledged in the current epoch; ReceivedSamples: the
// server's session-wide transport receipt (not a model proof, nil before known);
// CapturedSamples: whole samples handed to the SDK.
type Cursors struct {
	Epoch            int64
	SampleOffset     int64
	SentSamples      int64
	ReceivedSamples  *int64
	CapturedSamples  int64
	ConfirmedSamples int64
}

// Stats are local counters.
type Stats struct {
	CapturedSamples, ConfirmedSamples, BufferedSamples, PeakBufferedSamples, MaxBufferedSamples int64
	FrameSamples, FramesSent                                                                    int64
	Rotations, Connections, SequenceAnomalies                                                   int
	Epoch, CallbackErrors, DroppedEvents                                                        int64
}

// Session is the handle of one realtime session. Methods are safe for concurrent use; audio
// and lifecycle operations are serialized.
type Session struct {
	c               *Client
	SessionID       string
	TranscriptionID string
	Format          Format
	bps             int64
	frameSamples    int64
	maxBuffered     int64

	op      chan struct{} // serializes public operations (ctx-aware lock)
	sendMu  sync.Mutex    // orders outbound audio frames and rotation control
	writeMu sync.Mutex    // single socket writer

	mu             sync.Mutex
	changed        chan struct{}
	buf            []byte
	bufBase        int64
	half           []byte
	captured       int64
	epoch          int64
	epochStart     int64
	sent, acked    int64
	serverReceived *int64
	rotating       bool
	rotations      int
	accepting      bool
	userPaused     bool
	st             state
	fatal          error
	flush          bool
	finishSent     bool
	lastProgress   time.Time
	preceding      *string
	framesSent     int64
	peakBuffered   int64
	frameRetries   int
	completedView  *SessionView
	view           SessionView
	latestUsage    Usage
	result         *FinishResult
	closeoutDone   chan struct{}
	closeoutRes    *FinishResult
	closeoutErr    error
	closingOut     bool
	closed         bool
	intentional    bool
	serverErrors   []string
	connection     int
	lastSeq        *int64
	seqAnomalies   int
	ready          chan struct{}
	readyOnce      sync.Once
	ws             *websocket.Conn
	lastPong       time.Time

	transcript *Transcript

	handlers       map[string]func(Event)
	onEvent        func(Event)
	onCallbackErr  func(error, Event)
	queue          chan Event
	iterCh         chan Event
	iterMu         sync.Mutex
	iterClosed     bool
	quit           chan struct{}
	dispatcherDone chan struct{}
	readerDone     chan struct{}
	callbackErrors atomic.Int64
	droppedEvents  atomic.Int64
	bg             sync.WaitGroup
}

func newSession(c *Client, v SessionView, f Format, o ConnectOptions) *Session {
	frame := int64(f.SampleRateHz * c.cfg.FrameMs / 1000)
	if frame < 1 {
		frame = 1
	}
	maxBuf := int64(f.SampleRateHz) * int64(c.cfg.MaxBufferedMs) / 1000
	if maxBuf < frame {
		maxBuf = frame
	}
	s := &Session{c: c, SessionID: v.SessionID, TranscriptionID: v.TranscriptionID, Format: f,
		bps: int64(f.BytesPerSample), frameSamples: frame, maxBuffered: maxBuf, op: make(chan struct{}, 1),
		changed: make(chan struct{}), epoch: v.Epoch, accepting: true, st: stOpen, lastProgress: time.Now(),
		view: v, latestUsage: v.Usage, ready: make(chan struct{}), transcript: newTranscript(),
		handlers: o.Handlers, onEvent: o.OnEvent, onCallbackErr: o.OnCallbackError,
		queue: make(chan Event, c.cfg.EventQueueSize), quit: make(chan struct{}), dispatcherDone: make(chan struct{})}
	if s.epoch == 0 {
		s.epoch = 1
	}
	if o.EventChannel {
		s.iterCh = make(chan Event, c.cfg.EventQueueSize)
	}
	go s.dispatchLoop()
	return s
}

// String never shows keys or content.
func (s *Session) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fmt.Sprintf("sttrealtime.Session{SessionID:%s State:%s Epoch:%d}", s.SessionID, s.st, s.epoch)
}

// State is the local state name.
func (s *Session) State() string { s.mu.Lock(); defer s.mu.Unlock(); return s.st.String() }

// View is the latest session view.
func (s *Session) View() SessionView { s.mu.Lock(); defer s.mu.Unlock(); return s.view }

// LatestUsage is the latest usage of the current epoch.
func (s *Session) LatestUsage() Usage { s.mu.Lock(); defer s.mu.Unlock(); return s.latestUsage }

// ServerErrors lists session.error codes seen.
func (s *Session) ServerErrors() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.serverErrors...)
}

// Transcript is the merged snapshot.
func (s *Session) Transcript() *Transcript { return s.transcript }

// Cursors returns a snapshot of the counters.
func (s *Session) Cursors() Cursors {
	s.mu.Lock()
	defer s.mu.Unlock()
	var rec *int64
	if s.serverReceived != nil {
		v := *s.serverReceived
		rec = &v
	}
	return Cursors{Epoch: s.epoch, SampleOffset: s.sent, SentSamples: s.acked, ReceivedSamples: rec,
		CapturedSamples: s.captured, ConfirmedSamples: s.epochStart + s.acked}
}

// Stats returns local counters.
func (s *Session) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	conf := s.epochStart + s.acked
	return Stats{CapturedSamples: s.captured, ConfirmedSamples: conf, BufferedSamples: s.captured - conf,
		PeakBufferedSamples: s.peakBuffered, MaxBufferedSamples: s.maxBuffered, FrameSamples: s.frameSamples,
		FramesSent: s.framesSent, Rotations: s.rotations, Connections: s.connection,
		SequenceAnomalies: s.seqAnomalies, Epoch: s.epoch, CallbackErrors: s.callbackErrors.Load(),
		DroppedEvents: s.droppedEvents.Load()}
}

// CallbackErrors counts handler panics and undeliverable events.
func (s *Session) CallbackErrors() int64 { return s.callbackErrors.Load() }

// Events returns the event channel (ConnectOptions.EventChannel); closed by Close.
func (s *Session) Events() <-chan Event { return s.iterCh }

// ---------------------------------------------------------------- connection

func (s *Session) open(ctx context.Context, wsURL string) error {
	octx, cancel := context.WithTimeout(ctx, s.c.cfg.Timeouts.Connect)
	defer cancel()
	conn, resp, err := s.c.dialer().DialContext(octx, wsURL, s.c.wsHeader())
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		st := 0
		if resp != nil {
			st = resp.StatusCode
		}
		return &TransportError{Op: "socket open", Status: st, Err: errors.New(safeErr(err))}
	}
	conn.SetReadLimit(4 << 20)
	s.mu.Lock()
	s.ws = conn
	s.connection++
	s.lastSeq = nil
	s.lastPong = time.Now()
	s.readerDone = make(chan struct{})
	s.mu.Unlock()
	conn.SetPongHandler(func(string) error {
		s.mu.Lock()
		s.lastPong = time.Now()
		s.mu.Unlock()
		return nil
	})
	go s.reader(conn)
	if s.c.cfg.PingInterval > 0 {
		s.bg.Add(1)
		go s.pinger(conn)
	}
	t := time.NewTimer(s.c.cfg.Timeouts.Connect)
	defer t.Stop()
	select {
	case <-s.ready:
	case <-t.C:
		s.abortWS()
		return &TransportError{Op: "session.ready not received in time"}
	case <-ctx.Done():
		s.abortWS()
		return ctx.Err()
	}
	s.mu.Lock()
	lost := s.st == stLost
	s.mu.Unlock()
	if lost {
		return &ConnectionLostError{SessionID: s.SessionID, Msg: "socket closed before session.ready"}
	}
	return nil
}

func (s *Session) pinger(conn *websocket.Conn) {
	defer s.bg.Done()
	tick := time.NewTicker(s.c.cfg.PingInterval)
	defer tick.Stop()
	for {
		select {
		case <-s.readerDone:
			return
		case <-tick.C:
			s.mu.Lock()
			stale := time.Since(s.lastPong) > s.c.cfg.PingInterval+s.c.cfg.PingTimeout
			s.mu.Unlock()
			if stale {
				_ = conn.NetConn().Close()
				return
			}
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(s.c.cfg.PingTimeout)); err != nil {
				return
			}
		}
	}
}

func (s *Session) reader(conn *websocket.Conn) {
	done := s.readerDone
	defer close(done)
	for {
		mt, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		if mt != websocket.TextMessage {
			continue
		}
		msg, err := decodeObject(data)
		if err != nil {
			continue
		}
		s.onMessage(msg)
	}
	s.mu.Lock()
	same := s.ws == conn
	s.mu.Unlock()
	if same {
		s.onSocketEnd()
	}
}

func (s *Session) touchLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// waitOn blocks until ch closes, the timeout passes or ctx ends.
func waitOn(ctx context.Context, ch chan struct{}, d time.Duration) error {
	if d <= 0 {
		d = time.Millisecond
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ch:
	case <-t.C:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// ---------------------------------------------------------------- inbound

func (s *Session) onMessage(msg map[string]any) {
	etype := str(msg["eventType"])
	payload, _ := msg["payload"].(map[string]any)
	if payload == nil {
		payload = map[string]any{}
	}
	seq := optInt(msg["sequence"])
	s.mu.Lock()
	if seq != nil {
		if s.lastSeq != nil && *seq <= *s.lastSeq {
			s.seqAnomalies++ // recorded; never used to drop another connection's events
		}
		s.lastSeq = seq
	}
	switch etype {
	case EventUsageUpdated:
		s.onUsageLocked(usageFrom(payload))
	case EventSessionReady:
		s.onReadyLocked(viewFrom(payload))
	case EventSessionCompleted:
		v := viewFrom(payload)
		s.completedView = &v
		s.view = v
		s.latestUsage = v.Usage
	case EventSessionError:
		code := str(payload["errorCode"])
		if code == "" {
			code = "UNKNOWN"
		}
		s.onServerErrorLocked(code)
	}
	conn := s.connection
	s.touchLocked()
	s.mu.Unlock()
	// network layer first, then the snapshot and dispatch
	if etype == EventSessionCompleted {
		s.transcript.mergeView(viewFrom(payload))
	} else {
		s.transcript.apply(etype, payload)
	}
	s.enqueue(Event{Type: etype, Payload: payload, Sequence: seq, EventID: str(msg["eventId"]),
		SessionID: str(msg["sessionId"]), Connection: conn})
}

func (s *Session) onReadyLocked(v SessionView) {
	s.view = v
	if v.Epoch > 0 {
		s.epoch = v.Epoch
	}
	var received, sent int64
	if v.Usage.ReceivedSamples != nil {
		received = *v.Usage.ReceivedSamples
		r := received
		s.serverReceived = &r
	}
	if v.Usage.SentSamples != nil {
		sent = *v.Usage.SentSamples
	}
	if s.captured == 0 && len(s.buf) == 0 {
		s.epochStart = received - sent
		if s.epochStart < 0 {
			s.epochStart = 0
		}
		s.sent, s.acked = sent, sent
		s.captured, s.bufBase = received, received
	}
	s.accepting = v.Accepting
	if v.IsTerminal() {
		vv := v
		s.completedView = &vv
		s.st = stCompleted
	} else if !v.Accepting && v.Status == "PAUSED" {
		s.userPaused = true
		s.st = stPaused
	}
	s.readyOnce.Do(func() { close(s.ready) })
}

func (s *Session) onUsageLocked(u Usage) {
	if u.ReceivedSamples != nil && (s.serverReceived == nil || u.Epoch == nil || *u.Epoch >= s.epoch) {
		r := *u.ReceivedSamples
		s.serverReceived = &r
	}
	if u.Epoch == nil {
		return
	}
	ep := *u.Epoch
	if ep >= s.epoch {
		s.latestUsage = u
	}
	switch {
	case ep == s.epoch:
		if u.SentSamples != nil && *u.SentSamples > s.acked {
			s.acked = *u.SentSamples
			if s.acked > s.sent {
				s.sent = s.acked
			}
			s.progressLocked()
			s.frameRetries = 0
		}
		if u.Accepting != nil {
			s.accepting = *u.Accepting
		}
		if u.Rotate && !s.rotating {
			s.rotating = true
			s.rotations++
			s.spawn(s.rotate)
		}
		if u.ErrorCode != "" {
			s.setFatalLocked(u.ErrorCode)
		}
	case ep > s.epoch && u.Accepting != nil && *u.Accepting:
		s.adoptEpochLocked(ep)
	}
	// ep < current epoch: a late answer for an older epoch never moves current cursors back
}

func (s *Session) adoptEpochLocked(ep int64) {
	s.epochStart += s.acked
	s.epoch = ep
	s.sent, s.acked = 0, 0
	s.rotating = false
	s.accepting = true
	s.progressLocked()
	if s.canSendLocked() {
		s.spawn(s.pumpQuiet)
	}
}

func (s *Session) progressLocked() {
	s.lastProgress = time.Now()
	confirmed := s.epochStart + s.acked
	if drop := confirmed - s.bufBase; drop > 0 {
		n := drop * s.bps
		if n > int64(len(s.buf)) {
			n = int64(len(s.buf))
		}
		s.buf = append(s.buf[:0:0], s.buf[n:]...)
		s.bufBase = confirmed
	}
}

func (s *Session) onServerErrorLocked(code string) {
	s.serverErrors = append(s.serverErrors, code)
	if fatalStreamCodes[code] {
		s.setFatalLocked(code)
	} else if retryableStreamCodes[code] && s.sent > s.acked && !s.finishSent {
		s.frameRetries++
		if s.frameRetries > s.c.cfg.MaxFrameRetries {
			if s.fatal == nil {
				s.fatal = &ServerEventError{ErrorCode: code, SessionID: s.SessionID}
			}
		} else {
			s.sent = s.acked // replay unconfirmed frames at the confirmed sampleOffset
			if s.canSendLocked() {
				s.spawn(s.pumpQuiet)
			}
		}
	}
}

func (s *Session) setFatalLocked(code string) {
	if s.fatal == nil {
		s.fatal = &ServerEventError{ErrorCode: code, SessionID: s.SessionID}
	}
}

func (s *Session) onSocketEnd() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.touchLocked()
	if s.completedView != nil || s.intentional || s.closingOut {
		return
	}
	select {
	case <-s.ready:
	default:
		s.st = stLost
		s.readyOnce.Do(func() { close(s.ready) })
		return
	}
	s.st = stLost
	if !s.c.cfg.DisableCloseoutOnDisconnect || s.finishSent {
		s.startCloseoutLocked("DISCONNECTED")
	}
}

// ---------------------------------------------------------------- outbound

func (s *Session) spawn(fn func()) {
	s.bg.Add(1)
	go func() {
		defer s.bg.Done()
		fn()
	}()
}

func (s *Session) writeJSON(v any) error {
	s.mu.Lock()
	ws := s.ws
	s.mu.Unlock()
	if ws == nil {
		return &ConnectionLostError{SessionID: s.SessionID, Msg: "socket is not open"}
	}
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_ = ws.SetWriteDeadline(time.Now().Add(s.c.cfg.Timeouts.Ack))
	if err := ws.WriteMessage(websocket.TextMessage, data); err != nil {
		return &ConnectionLostError{SessionID: s.SessionID, Msg: "socket send failed"}
	}
	return nil
}

type clientEvent struct {
	EventType string         `json:"eventType"`
	Epoch     int64          `json:"epoch"`
	Payload   map[string]any `json:"payload"`
}

func (s *Session) sendControl(etype string) error {
	s.mu.Lock()
	ev := clientEvent{EventType: etype, Epoch: s.epoch, Payload: map[string]any{"capturedSamples": i64s(s.captured)}}
	s.mu.Unlock()
	return s.writeJSON(ev)
}

func (s *Session) rotate() {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.sendControl("session.pause") == nil {
		_ = s.sendControl("session.resume")
	}
}

func (s *Session) canSendLocked() bool {
	return s.ws != nil && (s.st == stOpen || s.st == stFinishing) && !s.finishSent && s.accepting && !s.rotating &&
		!s.userPaused && s.fatal == nil && !s.closingOut
}

func (s *Session) pumpQuiet() { _ = s.pump() }

func (s *Session) pump() error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	for {
		s.mu.Lock()
		if !s.canSendLocked() {
			s.mu.Unlock()
			return nil
		}
		pos := s.epochStart + s.sent
		avail := s.captured - pos
		if avail <= 0 {
			s.mu.Unlock()
			return nil
		}
		n := avail
		if n > s.frameSamples {
			n = s.frameSamples
		}
		if n < s.frameSamples && !s.flush {
			s.mu.Unlock()
			return nil
		}
		start := (pos - s.bufBase) * s.bps
		chunk := s.buf[start : start+n*s.bps]
		if s.sent == s.acked {
			s.lastProgress = time.Now() // the no-ack clock starts with unconfirmed audio
		}
		payload := map[string]any{"audioBase64": base64.StdEncoding.EncodeToString(chunk),
			"sampleOffset": i64s(s.sent), "capturedSamples": i64s(s.captured)}
		if s.preceding != nil {
			payload["precedingText"] = *s.preceding
			s.preceding = nil
		}
		ev := clientEvent{EventType: "audio.append", Epoch: s.epoch, Payload: payload}
		s.sent += n
		s.framesSent++
		s.mu.Unlock()
		if err := s.writeJSON(ev); err != nil {
			return err
		}
	}
}

func (s *Session) brokenLocked() error {
	if s.fatal != nil {
		return s.fatal
	}
	switch s.st {
	case stLost:
		return &ConnectionLostError{SessionID: s.SessionID, Msg: "connection lost; no more audio is accepted"}
	case stClosed, stCompleted:
		return &SessionStateError{Msg: "session is " + s.st.String()}
	}
	return nil
}

func (s *Session) waitConfirmed(ctx context.Context) error {
	for {
		s.mu.Lock()
		if err := s.brokenLocked(); err != nil {
			s.mu.Unlock()
			return err
		}
		confirmed := s.epochStart + s.acked
		if confirmed >= s.captured && !s.rotating {
			s.mu.Unlock()
			return nil
		}
		if s.canSendLocked() && s.epochStart+s.sent < s.captured {
			s.mu.Unlock()
			if err := s.pump(); err != nil {
				return err
			}
			continue
		}
		idle := time.Since(s.lastProgress)
		ch := s.changed
		s.mu.Unlock()
		if idle > s.c.cfg.Timeouts.Ack {
			return &AckTimeoutError{SessionID: s.SessionID, What: "audio"}
		}
		if err := waitOn(ctx, ch, minDur(250*time.Millisecond, s.c.cfg.Timeouts.Ack-idle+10*time.Millisecond)); err != nil {
			return err
		}
	}
}

func (s *Session) waitRoom(ctx context.Context, need int64) (int64, error) {
	limit := s.c.cfg.Timeouts.Backpressure
	for {
		s.mu.Lock()
		if err := s.brokenLocked(); err != nil {
			s.mu.Unlock()
			return 0, err
		}
		if s.userPaused {
			s.mu.Unlock()
			return 0, &SessionStateError{Msg: "session is paused; Resume before sending audio"}
		}
		room := s.maxBuffered - (s.captured - (s.epochStart + s.acked))
		want := need
		if want > s.frameSamples {
			want = s.frameSamples
		}
		if room >= want && room > 0 {
			s.mu.Unlock()
			return room, nil
		}
		idle := time.Since(s.lastProgress)
		ch := s.changed
		s.mu.Unlock()
		if idle > limit {
			return 0, &BackpressureTimeoutError{SessionID: s.SessionID}
		}
		if err := waitOn(ctx, ch, minDur(250*time.Millisecond, limit-idle+10*time.Millisecond)); err != nil {
			return 0, err
		}
	}
}

func (s *Session) checkInputLocked() error {
	if s.st == stPaused || s.userPaused {
		return &SessionStateError{Msg: "session is paused; Resume before sending audio"}
	}
	if s.st != stOpen {
		if err := s.brokenLocked(); err != nil {
			return err
		}
		return &SessionStateError{Msg: "session is " + s.st.String() + "; no more audio is accepted"}
	}
	return s.brokenLocked()
}

func (s *Session) lockOp(ctx context.Context) error {
	select {
	case s.op <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Session) unlockOp() { <-s.op }

// ---------------------------------------------------------------- public operations

// SendAudio queues audio in the session encoding. Any chunk size is accepted; complete
// frames are sent as soon as they exist. It blocks while the bounded buffer is full. A
// trailing half PCM sample is kept for the next call.
func (s *Session) SendAudio(ctx context.Context, data []byte) error {
	return s.sendAudio(ctx, data, nil)
}

// SendAudioWithPrecedingText attaches optional precedingText to the next audio frame.
func (s *Session) SendAudioWithPrecedingText(ctx context.Context, data []byte, precedingText string) error {
	return s.sendAudio(ctx, data, &precedingText)
}

func (s *Session) sendAudio(ctx context.Context, data []byte, preceding *string) error {
	if err := s.lockOp(ctx); err != nil {
		return err
	}
	defer s.unlockOp()
	s.mu.Lock()
	if err := s.checkInputLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	in := make([]byte, 0, len(s.half)+len(data))
	in = append(append(in, s.half...), data...)
	s.half = nil
	if s.bps == 2 && len(in)%2 == 1 {
		s.half = []byte{in[len(in)-1]}
		in = in[:len(in)-1]
	}
	if preceding != nil {
		s.preceding = preceding
	}
	s.mu.Unlock()
	off, total := int64(0), int64(len(in))
	for off < total {
		room, err := s.waitRoom(ctx, (total-off)/s.bps)
		if err != nil {
			return err
		}
		take := room * s.bps
		if take > total-off {
			take = total - off
		}
		s.mu.Lock()
		s.buf = append(s.buf, in[off:off+take]...)
		s.captured += take / s.bps
		if b := s.captured - (s.epochStart + s.acked); b > s.peakBuffered {
			s.peakBuffered = b
		}
		s.mu.Unlock()
		off += take
		if err := s.pump(); err != nil {
			return err
		}
	}
	return nil
}

// SendReader streams r (raw audio in the session encoding) frame by frame until EOF.
// paced=true sends by sample time (files); use false for a live source that already arrives
// at capture speed.
func (s *Session) SendReader(ctx context.Context, r io.Reader, paced bool) error {
	frameBytes := int(s.frameSamples * s.bps)
	buf := make([]byte, frameBytes)
	t0 := time.Now()
	var pos int64
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			if paced {
				pos += int64(n)
				due := time.Duration(float64(pos/s.bps) / float64(s.Format.SampleRateHz) * float64(time.Second))
				if wait := due - time.Since(t0); wait > 0 {
					sleepCtx(ctx, wait)
					if ctx.Err() != nil {
						return ctx.Err()
					}
				}
			}
			if serr := s.SendAudio(ctx, buf[:n]); serr != nil {
				return serr
			}
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// SendFile streams a raw file in the session encoding, or a WAV file (".wav") whose header
// matches it. paced=true sends one second of audio per second.
func (s *Session) SendFile(ctx context.Context, path string, paced bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var r io.Reader = f
	if strings.HasSuffix(strings.ToLower(path), ".wav") {
		info, err := ReadWAVHeader(f)
		if err != nil {
			return err
		}
		enc, err := info.Encoding()
		if err != nil {
			return err
		}
		if enc != s.Format.Encoding {
			return &AudioFormatError{Msg: fmt.Sprintf("WAV is %s but the session encoding is %s", enc, s.Format.Encoding)}
		}
		r = io.LimitReader(f, info.DataSize)
	}
	return s.SendReader(ctx, r, paced)
}

func (s *Session) flushAndConfirm(ctx context.Context) error {
	s.mu.Lock()
	s.flush = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.flush = false
		s.mu.Unlock()
	}()
	if err := s.pump(); err != nil {
		return err
	}
	return s.waitConfirmed(ctx)
}

// Commit sends segment.commit: the audio so far becomes a line; recording continues in the
// same epoch.
func (s *Session) Commit(ctx context.Context) error {
	if err := s.lockOp(ctx); err != nil {
		return err
	}
	defer s.unlockOp()
	s.mu.Lock()
	err := s.checkInputLocked()
	s.mu.Unlock()
	if err != nil {
		return err
	}
	if err := s.flushAndConfirm(ctx); err != nil {
		return err
	}
	return s.sendControl("segment.commit")
}

// Pause flushes and confirms buffered audio, then closes the current epoch.
func (s *Session) Pause(ctx context.Context, via Control) error {
	if err := s.lockOp(ctx); err != nil {
		return err
	}
	defer s.unlockOp()
	s.mu.Lock()
	err := s.checkInputLocked()
	s.mu.Unlock()
	if err != nil {
		return err
	}
	if err := s.flushAndConfirm(ctx); err != nil {
		return err
	}
	if via == ViaREST {
		v, err := s.restControl(ctx, "pause")
		if err != nil {
			return err
		}
		s.applyRESTView(v)
	} else {
		if err := s.sendControl("session.pause"); err != nil {
			return err
		}
		if err := s.waitUntil(ctx, func() bool { return !s.accepting }, "pause"); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.userPaused = true
	s.st = stPaused
	s.touchLocked()
	s.mu.Unlock()
	return nil
}

// Resume opens the next epoch. Sample offsets restart at 0; the captured count continues.
func (s *Session) Resume(ctx context.Context, via Control) error {
	if err := s.lockOp(ctx); err != nil {
		return err
	}
	defer s.unlockOp()
	s.mu.Lock()
	if s.st != stPaused {
		err := s.brokenLocked()
		if err == nil {
			err = &SessionStateError{Msg: "Resume needs a paused session (state=" + s.st.String() + ")"}
		}
		s.mu.Unlock()
		return err
	}
	old := s.epoch
	s.mu.Unlock()
	if via == ViaREST {
		v, err := s.restControl(ctx, "resume")
		if err != nil {
			return err
		}
		s.applyRESTView(v)
		s.mu.Lock()
		if v.Epoch > old && v.Accepting {
			s.adoptEpochLocked(v.Epoch)
		}
		s.mu.Unlock()
	} else {
		if err := s.sendControl("session.resume"); err != nil {
			return err
		}
		if err := s.waitUntil(ctx, func() bool { return s.epoch > old }, "resume"); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.epoch <= old {
		return &SessionStateError{Msg: "server did not open a new epoch"}
	}
	s.userPaused = false
	s.st = stOpen
	s.touchLocked()
	return nil
}

// waitUntil polls pred (evaluated under the lock) until true, within Timeouts.Ack.
func (s *Session) waitUntil(ctx context.Context, pred func() bool, what string) error {
	deadline := time.Now().Add(s.c.cfg.Timeouts.Ack)
	for {
		s.mu.Lock()
		if pred() {
			s.mu.Unlock()
			return nil
		}
		if err := s.brokenLocked(); err != nil {
			s.mu.Unlock()
			return err
		}
		ch := s.changed
		s.mu.Unlock()
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return &AckTimeoutError{SessionID: s.SessionID, What: what}
		}
		if err := waitOn(ctx, ch, minDur(250*time.Millisecond, remaining)); err != nil {
			return err
		}
	}
}

// restControl: an unknown outcome is resolved by reading the session first; the call is
// repeated only when the read shows it did not take effect.
func (s *Session) restControl(ctx context.Context, action string) (SessionView, error) {
	s.mu.Lock()
	captured, old := s.captured, s.epoch
	s.mu.Unlock()
	call := s.c.PauseSession
	if action == "resume" {
		call = s.c.ResumeSession
	}
	v, err := call(ctx, s.SessionID, &captured)
	var tErr *TransportError
	if err == nil || !errors.As(err, &tErr) {
		return v, err
	}
	got, gerr := s.c.GetSession(ctx, s.SessionID)
	if gerr != nil {
		return SessionView{}, gerr
	}
	done := !got.Accepting
	if action == "resume" {
		done = got.Epoch > old && got.Accepting
	}
	if done {
		return got, nil
	}
	return call(ctx, s.SessionID, &captured)
}

func (s *Session) applyRESTView(v SessionView) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.view = v
	s.latestUsage = v.Usage
	s.accepting = v.Accepting
	if v.Usage.ReceivedSamples != nil {
		r := *v.Usage.ReceivedSamples
		s.serverReceived = &r
	}
	s.touchLocked()
}

// Finish flushes the tail, waits until every sample is confirmed (handling rotation), sends
// session.finish and returns the server's terminal result. Repeated calls return the same
// result. On disconnect/timeout it closes out over REST (FinishResult.CloseoutReason) or
// returns *FinishUnconfirmedError.
func (s *Session) Finish(ctx context.Context) (*FinishResult, error) {
	if r, done := s.cachedResult(); done != nil {
		return s.awaitCloseout(ctx, done)
	} else if r != nil {
		return r, nil
	}
	if err := s.lockOp(ctx); err != nil {
		return nil, err
	}
	defer s.unlockOp()
	if r, done := s.cachedResult(); done != nil {
		return s.awaitCloseout(ctx, done)
	} else if r != nil {
		return r, nil
	}
	s.mu.Lock()
	if len(s.half) > 0 {
		s.mu.Unlock()
		return nil, &AudioFormatError{Msg: "one byte of an incomplete PCM sample remains; send the missing byte or call DiscardPartialSample before Finish"}
	}
	if s.st == stCompleted && s.completedView != nil {
		v := *s.completedView
		s.mu.Unlock()
		return s.finalize(v, false, ""), nil
	}
	if s.st == stClosed {
		s.mu.Unlock()
		return nil, &SessionStateError{Msg: "session handle is closed; use Client.FinishSession(sessionID)"}
	}
	if s.st == stLost {
		s.mu.Unlock()
		return s.ensureCloseout(ctx, "DISCONNECTED")
	}
	s.st = stFinishing
	streaming := !s.userPaused && s.fatal == nil
	s.mu.Unlock()
	if streaming {
		err := s.flushAndConfirm(ctx)
		var ackErr *AckTimeoutError
		var lost *ConnectionLostError
		var srvErr *ServerEventError
		switch {
		case err == nil, errors.As(err, &srvErr):
			// a server stop: settle what it confirmed
		case errors.As(err, &ackErr):
			return s.ensureCloseout(ctx, "ACK_TIMEOUT")
		case errors.As(err, &lost):
			return s.ensureCloseout(ctx, "DISCONNECTED")
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			s.mu.Lock()
			if s.st == stFinishing {
				s.st = stOpen
			}
			s.mu.Unlock()
			return nil, err
		default:
			return nil, err
		}
	}
	s.mu.Lock()
	s.finishSent = true
	s.mu.Unlock()
	if err := s.sendControl("session.finish"); err != nil {
		return s.ensureCloseout(ctx, "DISCONNECTED")
	}
	deadline := time.Now().Add(s.c.cfg.Timeouts.Finish)
	for {
		s.mu.Lock()
		if s.completedView != nil {
			s.mu.Unlock()
			break
		}
		done := s.closeoutDone
		lost := s.st == stLost
		ch := s.changed
		s.mu.Unlock()
		if done != nil {
			return s.awaitCloseout(ctx, done)
		}
		if lost {
			return s.ensureCloseout(ctx, "DISCONNECTED")
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return s.ensureCloseout(ctx, "FINISH_TIMEOUT")
		}
		if err := waitOn(ctx, ch, minDur(250*time.Millisecond, remaining)); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	v := *s.completedView
	s.intentional = true
	s.mu.Unlock()
	res := s.finalize(v, false, "")
	s.spawn(s.closeWS)
	return res, nil
}

func (s *Session) cachedResult() (*FinishResult, chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.result != nil {
		return s.result, nil
	}
	return nil, s.closeoutDone
}

// DiscardPartialSample drops a pending half PCM sample (reports whether one was pending).
func (s *Session) DiscardPartialSample() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	had := len(s.half) > 0
	s.half = nil
	return had
}

// AbortConnection drops this client's socket without a close handshake or finish, like a
// network failure (test/diagnostic helper). The disconnect path then runs.
func (s *Session) AbortConnection() {
	s.mu.Lock()
	ws := s.ws
	s.mu.Unlock()
	if ws != nil {
		_ = ws.NetConn().Close()
	}
}

// Close releases local resources only (socket, goroutines). It is not a business finish; an
// unfinished session stays open server-side (paused on disconnect). Idempotent.
func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.intentional = true
	if s.st != stCompleted && s.st != stLost {
		s.st = stClosed
	}
	s.touchLocked()
	readerDone := s.readerDone
	s.mu.Unlock()
	s.closeWS()
	if readerDone != nil {
		select {
		case <-readerDone:
		case <-time.After(s.c.cfg.Timeouts.Close):
			s.abortWS()
		}
	}
	close(s.quit)
	select {
	case <-s.dispatcherDone:
	case <-time.After(s.c.cfg.Timeouts.Close):
	}
	waitGroupTimeout(&s.bg, s.c.cfg.Timeouts.Close)
	s.iterMu.Lock()
	if s.iterCh != nil && !s.iterClosed {
		s.iterClosed = true
		close(s.iterCh)
	}
	s.iterMu.Unlock()
	s.c.forget(s)
	return nil
}

func waitGroupTimeout(wg *sync.WaitGroup, d time.Duration) {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
	}
}

func (s *Session) closeWS() {
	s.mu.Lock()
	ws := s.ws
	s.mu.Unlock()
	if ws == nil {
		return
	}
	s.writeMu.Lock()
	err := ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(s.c.cfg.Timeouts.Close))
	s.writeMu.Unlock()
	if err != nil {
		s.abortWS()
		return
	}
	s.mu.Lock()
	done := s.readerDone
	s.mu.Unlock()
	if done != nil {
		select {
		case <-done:
		case <-time.After(s.c.cfg.Timeouts.Close):
		}
	}
	s.abortWS()
}

func (s *Session) abortWS() {
	s.mu.Lock()
	ws := s.ws
	s.mu.Unlock()
	if ws != nil {
		_ = ws.NetConn().Close()
	}
}

// ---------------------------------------------------------------- close-out

func (s *Session) startCloseoutLocked(reason string) chan struct{} {
	if s.closeoutDone == nil {
		s.closeoutDone = make(chan struct{})
		s.closingOut = true
		if s.st != stClosed {
			s.st = stLost
		}
		done := s.closeoutDone
		s.bg.Add(1)
		go func() {
			defer s.bg.Done()
			res, err := s.closeout(reason)
			s.mu.Lock()
			s.closeoutRes, s.closeoutErr = res, err
			s.touchLocked()
			s.mu.Unlock()
			close(done)
		}()
	}
	return s.closeoutDone
}

func (s *Session) ensureCloseout(ctx context.Context, reason string) (*FinishResult, error) {
	s.mu.Lock()
	done := s.startCloseoutLocked(reason)
	s.mu.Unlock()
	return s.awaitCloseout(ctx, done)
}

func (s *Session) awaitCloseout(ctx context.Context, done chan struct{}) (*FinishResult, error) {
	select {
	case <-done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeoutRes, s.closeoutErr
}

func (s *Session) closeout(reason string) (*FinishResult, error) {
	s.abortWS()
	s.mu.Lock()
	captured := s.captured
	s.mu.Unlock()
	deadline := time.Now().Add(s.c.cfg.Timeouts.Closeout)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	var last error
	for attempt := 1; time.Now().Before(deadline); attempt++ {
		v, err := s.c.GetSession(ctx, s.SessionID)
		if err == nil && !v.IsTerminal() {
			v, err = s.c.FinishSession(ctx, s.SessionID, &captured)
		}
		if err == nil && v.IsTerminal() {
			return s.finalize(v, true, reason), nil
		}
		var apiErr *APIError
		if errors.As(err, &apiErr) && !apiErr.Retryable {
			return nil, &FinishUnconfirmedError{SessionID: s.SessionID, Msg: "close-out was refused (" + apiErr.ErrorCode + ")", Cause: err}
		}
		last = err
		back := time.Duration(250*(1<<uint(min(attempt, 4)))) * time.Millisecond
		sleepCtx(ctx, minDur(back, time.Until(deadline)))
	}
	return nil, &FinishUnconfirmedError{SessionID: s.SessionID, Msg: "the end of the session is not confirmed", Cause: last}
}

func (s *Session) finalize(v SessionView, viaREST bool, reason string) *FinishResult {
	s.transcript.mergeView(v)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.view = v
	s.latestUsage = v.Usage
	sid, tid := v.SessionID, v.TranscriptionID
	if sid == "" {
		sid = s.SessionID
	}
	if tid == "" {
		tid = s.TranscriptionID
	}
	r := &FinishResult{Status: v.Status, SessionID: sid, TranscriptionID: tid, Usage: v.Usage, Notices: v.Notices,
		Lines: s.transcript.Lines(), View: v, ViaREST: viaREST, CloseoutReason: reason,
		ServerErrors: append([]string(nil), s.serverErrors...), CallbackErrors: s.callbackErrors.Load(),
		DroppedEvents: s.droppedEvents.Load()}
	s.result = r
	if s.st != stClosed {
		s.st = stCompleted
	}
	s.touchLocked()
	return r
}

// ---------------------------------------------------------------- dispatch

func (s *Session) hasConsumers() bool { return len(s.handlers) > 0 || s.onEvent != nil }

func (s *Session) enqueue(ev Event) {
	if s.iterCh != nil {
		s.iterMu.Lock()
		if !s.iterClosed {
			select {
			case s.iterCh <- ev:
			default:
				s.droppedEvents.Add(1)
				s.report(errors.New("myvocal: event channel full; event not delivered"), ev)
			}
		}
		s.iterMu.Unlock()
	}
	if !s.hasConsumers() {
		return
	}
	select {
	case s.queue <- ev:
	default:
		s.droppedEvents.Add(1)
		s.report(errors.New("myvocal: event handler queue full; event not delivered (state and transcript snapshot are still updated)"), ev)
	}
}

func (s *Session) report(err error, ev Event) {
	if s.onCallbackErr == nil {
		return
	}
	go func() {
		defer func() { _ = recover() }()
		s.onCallbackErr(err, ev)
	}()
}

func (s *Session) dispatchLoop() {
	defer close(s.dispatcherDone)
	for {
		select {
		case ev := <-s.queue:
			s.deliver(ev)
		case <-s.quit:
			// drain what is queued within the close timeout
			deadline := time.After(s.c.cfg.Timeouts.Close)
			for {
				select {
				case ev := <-s.queue:
					s.deliver(ev)
				case <-deadline:
					return
				default:
					return
				}
			}
		}
	}
}

func (s *Session) deliver(ev Event) {
	var targets []func(Event)
	if h := s.handlers[ev.Type]; h != nil {
		targets = append(targets, h)
	}
	if h := s.handlers["*"]; h != nil {
		targets = append(targets, h)
	}
	if s.onEvent != nil {
		targets = append(targets, s.onEvent)
	}
	for _, fn := range targets {
		func() {
			defer func() {
				if r := recover(); r != nil {
					s.callbackErrors.Add(1)
					if s.onCallbackErr != nil {
						func() {
							defer func() { _ = recover() }()
							s.onCallbackErr(fmt.Errorf("handler panic: %v", r), ev)
						}()
					}
				}
			}()
			fn(ev)
		}()
	}
}

func i64s(v int64) string { return strconv.FormatInt(v, 10) }
