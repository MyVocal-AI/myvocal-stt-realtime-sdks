package sttrealtime

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Config configures a Client. Only AccessKey is required.
type Config struct {
	AccessKey string
	// BaseURL defaults to https://api.myvocal.ai. http:// maps to ws://, https:// to wss://.
	BaseURL  string
	Timeouts Timeouts
	// PingInterval of client WebSocket pings (default 20s; negative disables). Unrelated to
	// the heartbeatIntervalMs processing option. Pings are never audio.
	PingInterval time.Duration
	// PingTimeout: a socket without a pong for this long after a ping is treated as lost (default 20s).
	PingTimeout time.Duration
	// FrameMs is the approximate audio frame duration (default 100).
	FrameMs int
	// MaxBufferedMs bounds audio held by the SDK and not yet confirmed (default 30000).
	MaxBufferedMs int
	// EventQueueSize bounds events waiting for handlers (default 10000).
	EventQueueSize int
	// MaxCreateRetries: idempotent create retries after transport errors / retryable envelopes (default 2; -1 = none).
	MaxCreateRetries int
	// MaxFrameRetries: consecutive retryable session.error replays before the stream stops (default 3).
	MaxFrameRetries int
	// DisableCloseoutOnDisconnect turns off the automatic REST close-out after an unexpected
	// disconnect (Finish still closes out).
	DisableCloseoutOnDisconnect bool
	// HTTPClient is borrowed for REST calls and never closed. When nil the SDK owns one built
	// from Proxy and TLSConfig.
	HTTPClient *http.Client
	// Proxy for REST (owned client) and WebSocket. Default http.ProxyFromEnvironment; use NoProxy to disable.
	Proxy func(*http.Request) (*url.URL, error)
	// TLSConfig for a private CA etc. TLS verification is on by default.
	TLSConfig *tls.Config
}

// NoProxy disables proxies.
func NoProxy(*http.Request) (*url.URL, error) { return nil, nil }

// ProxyURL returns a fixed proxy.
func ProxyURL(raw string) (func(*http.Request) (*url.URL, error), error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	return http.ProxyURL(u), nil
}

// Client creates sessions and opens per-session connections. Safe for concurrent use.
type Client struct {
	cfg      Config
	key      string
	base     string
	http     *http.Client
	owned    *http.Transport
	mu       sync.Mutex
	sessions map[*Session]struct{}
}

// NewClient validates the configuration.
func NewClient(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.AccessKey) == "" {
		return nil, errors.New("myvocal: AccessKey is required")
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://api.myvocal.ai"
	}
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if !strings.HasPrefix(cfg.BaseURL, "http://") && !strings.HasPrefix(cfg.BaseURL, "https://") {
		return nil, errors.New("myvocal: BaseURL must be http:// or https://")
	}
	cfg.Timeouts = cfg.Timeouts.withDefaults()
	if cfg.PingInterval == 0 {
		cfg.PingInterval = 20 * time.Second
	}
	if cfg.PingTimeout <= 0 {
		cfg.PingTimeout = 20 * time.Second
	}
	if cfg.FrameMs <= 0 {
		cfg.FrameMs = 100
	}
	if cfg.MaxBufferedMs <= 0 {
		cfg.MaxBufferedMs = 30000
	}
	if cfg.MaxBufferedMs < cfg.FrameMs {
		return nil, errors.New("myvocal: MaxBufferedMs must be >= FrameMs")
	}
	if cfg.EventQueueSize <= 0 {
		cfg.EventQueueSize = 10000
	}
	if cfg.MaxCreateRetries == 0 {
		cfg.MaxCreateRetries = 2
	} else if cfg.MaxCreateRetries < 0 {
		cfg.MaxCreateRetries = 0
	}
	if cfg.MaxFrameRetries <= 0 {
		cfg.MaxFrameRetries = 3
	}
	if cfg.Proxy == nil {
		cfg.Proxy = http.ProxyFromEnvironment
	}
	c := &Client{cfg: cfg, key: cfg.AccessKey, base: cfg.BaseURL, sessions: map[*Session]struct{}{}}
	cfg.AccessKey = ""
	c.cfg.AccessKey = "" // the key lives only in c.key
	if cfg.HTTPClient != nil {
		c.http = cfg.HTTPClient
	} else {
		c.owned = &http.Transport{Proxy: cfg.Proxy, TLSClientConfig: cfg.TLSConfig, ForceAttemptHTTP2: true,
			MaxIdleConns: 16, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 15 * time.Second}
		c.http = &http.Client{Transport: c.owned}
	}
	return c, nil
}

// String never shows the key.
func (c *Client) String() string   { return "sttrealtime.Client{BaseURL:" + c.base + "}" }
func (c *Client) GoString() string { return c.String() }

// Close closes this client's sessions (local resources only) and its owned HTTP transport.
// A borrowed HTTPClient is left untouched.
func (c *Client) Close() error {
	c.mu.Lock()
	list := make([]*Session, 0, len(c.sessions))
	for s := range c.sessions {
		list = append(list, s)
	}
	c.mu.Unlock()
	for _, s := range list {
		_ = s.Close()
	}
	if c.owned != nil {
		c.owned.CloseIdleConnections()
	}
	return nil
}

// ---------------------------------------------------------------- REST helpers

// CreateSession creates a session. idempotencyKey "" generates one; retries reuse the same key
// and body so a lost response never creates a second session.
func (c *Client) CreateSession(ctx context.Context, req SessionRequest, idempotencyKey string) (SessionView, error) {
	if idempotencyKey == "" {
		idempotencyKey = newID()
	}
	body, err := json.Marshal(req.wire())
	if err != nil {
		return SessionView{}, err
	}
	deadline := time.Now().Add(c.cfg.Timeouts.Create)
	attempts := 1 + c.cfg.MaxCreateRetries
	var last error
	for i := 0; i < attempts; i++ {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		data, err := c.call(ctx, http.MethodPost, "/sessions", body, idempotencyKey, minDur(remaining, c.cfg.Timeouts.Request))
		if err == nil {
			return viewFrom(data), nil
		}
		var apiErr *APIError
		var tErr *TransportError
		switch {
		case errors.As(err, &apiErr):
			if !apiErr.Retryable {
				return SessionView{}, err
			}
		case errors.As(err, &tErr):
			if tErr.Status != 0 && tErr.Status < 500 && tErr.Status != 429 {
				return SessionView{}, err
			}
		default:
			return SessionView{}, err
		}
		if ctx.Err() != nil {
			return SessionView{}, ctx.Err()
		}
		last = err
		if i+1 < attempts {
			sleepCtx(ctx, minDur(time.Duration(250<<uint(i))*time.Millisecond, 2*time.Second, time.Until(deadline)))
		}
	}
	if last == nil {
		last = &TransportError{Op: "create session timed out"}
	}
	return SessionView{}, last
}

// GetSession reads a session.
func (c *Client) GetSession(ctx context.Context, sessionID string) (SessionView, error) {
	return c.view(ctx, http.MethodGet, sessionID, "", nil, c.cfg.Timeouts.Request)
}

// PauseSession closes the current epoch over REST. capturedSamples nil = not declared.
func (c *Client) PauseSession(ctx context.Context, sessionID string, capturedSamples *int64) (SessionView, error) {
	return c.view(ctx, http.MethodPost, sessionID, "/pause", capturedSamples, c.cfg.Timeouts.Request)
}

// ResumeSession opens the next epoch over REST.
func (c *Client) ResumeSession(ctx context.Context, sessionID string, capturedSamples *int64) (SessionView, error) {
	return c.view(ctx, http.MethodPost, sessionID, "/resume", capturedSamples, c.cfg.Timeouts.Request)
}

// FinishSession is the idempotent REST finish; a repeated finish returns the same task
// without a second charge.
func (c *Client) FinishSession(ctx context.Context, sessionID string, capturedSamples *int64) (SessionView, error) {
	return c.view(ctx, http.MethodPost, sessionID, "/finish", capturedSamples, maxDur(c.cfg.Timeouts.Request, 70*time.Second))
}

// CreateTicket issues a single-use browser ticket for your own front end. This SDK never
// connects with a ticket. Never log Ticket.Value.
func (c *Client) CreateTicket(ctx context.Context, sessionID string) (Ticket, error) {
	if err := checkID(sessionID); err != nil {
		return Ticket{}, err
	}
	data, err := c.call(ctx, http.MethodPost, "/sessions/"+sessionID+"/tickets", []byte("{}"), "", c.cfg.Timeouts.Request)
	if err != nil {
		return Ticket{}, err
	}
	t := Ticket{TicketID: str(data["ticketId"]), SessionID: str(data["sessionId"]), StreamURL: str(data["streamUrl"]),
		Value: str(data["ticket"])}
	if n := optInt(data["expiresInSeconds"]); n != nil {
		t.ExpiresInSeconds = *n
	}
	return t, nil
}

func (c *Client) view(ctx context.Context, method, sessionID, action string, captured *int64, timeout time.Duration) (SessionView, error) {
	if err := checkID(sessionID); err != nil {
		return SessionView{}, err
	}
	var body []byte
	if method == http.MethodPost {
		body = capBody(captured)
	}
	data, err := c.call(ctx, method, "/sessions/"+sessionID+action, body, "", timeout)
	if err != nil {
		return SessionView{}, err
	}
	return viewFrom(data), nil
}

func capBody(captured *int64) []byte {
	if captured == nil {
		return []byte("{}")
	}
	return []byte(fmt.Sprintf(`{"capturedSamples":"%d"}`, *captured))
}

func (c *Client) call(ctx context.Context, method, path string, body []byte, idemKey string, timeout time.Duration) (map[string]any, error) {
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	op := method + " " + realtimePath + strings.SplitN(path, "?", 2)[0]
	req, err := http.NewRequestWithContext(rctx, method, c.base+realtimePath+path, rd)
	if err != nil {
		return nil, &TransportError{Op: op, Err: err}
	}
	req.Header.Set("accessKey", c.key)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &TransportError{Op: op, Err: errors.New(safeErr(err))}
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, &TransportError{Op: op, Err: errors.New(safeErr(err))}
	}
	env, err := decodeObject(raw)
	if err != nil || env["code"] == nil {
		return nil, &TransportError{Op: op + " (no MyVocal envelope)", Status: resp.StatusCode}
	}
	if code, ok := asInt64(env["code"]); !ok || code != 1 {
		return nil, apiErrorFromEnvelope(env)
	}
	if resp.StatusCode >= 400 {
		return nil, &TransportError{Op: op, Status: resp.StatusCode}
	}
	data, _ := env["data"].(map[string]any)
	if data == nil {
		data = map[string]any{}
	}
	return data, nil
}

// ---------------------------------------------------------------- sessions

// Control selects the control path of Pause/Resume.
type Control int

const (
	// ViaWS sends session.pause / session.resume on the socket.
	ViaWS Control = iota
	// ViaREST calls POST /pause or /resume on the same serialized control path.
	ViaREST
)

// ConnectOptions for Client.Connect. Handlers are registered before the socket opens.
type ConnectOptions struct {
	// Request creates a new session (ignored when SessionID is set).
	Request *SessionRequest
	// SessionID attaches to an existing session instead of creating one.
	SessionID      string
	IdempotencyKey string
	// Handlers by event type; "*" receives every event.
	Handlers map[string]func(Event)
	OnEvent  func(Event)
	// OnCallbackError observes handler panics and undeliverable events (queue overflow).
	OnCallbackError func(err error, ev Event)
	// EventChannel enables Session.Events().
	EventChannel bool
}

// Connect creates a session (or uses SessionID), opens its socket with the accessKey header
// and waits for session.ready.
func (c *Client) Connect(ctx context.Context, opts ConnectOptions) (*Session, error) {
	created := opts.SessionID == ""
	var view SessionView
	var err error
	if created {
		req := SessionRequest{}
		if opts.Request != nil {
			req = *opts.Request
		}
		view, err = c.CreateSession(ctx, req, opts.IdempotencyKey)
	} else {
		view, err = c.GetSession(ctx, opts.SessionID)
	}
	if err != nil {
		return nil, err
	}
	format, err := resolveFormat(opts.Request, view)
	if err != nil {
		if created {
			c.quietFinish(view.SessionID)
		}
		return nil, err
	}
	s := newSession(c, view, format, opts)
	if err := s.open(ctx, c.streamURL(view)); err != nil {
		_ = s.Close()
		if created {
			c.quietFinish(view.SessionID)
		}
		var tErr *TransportError
		if errors.As(err, &tErr) {
			tErr.SessionID = view.SessionID
		}
		return nil, err
	}
	c.mu.Lock()
	c.sessions[s] = struct{}{}
	c.mu.Unlock()
	return s, nil
}

func (c *Client) forget(s *Session) {
	c.mu.Lock()
	delete(c.sessions, s)
	c.mu.Unlock()
}

func (c *Client) quietFinish(sessionID string) {
	if sessionID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.Timeouts.Request)
	defer cancel()
	_, _ = c.FinishSession(ctx, sessionID, nil)
}

func (c *Client) streamURL(v SessionView) string {
	path := v.StreamURL
	if path == "" {
		path = realtimePath + "/sessions/" + v.SessionID + "/stream"
	}
	full := path
	if !strings.HasPrefix(path, "http://") && !strings.HasPrefix(path, "https://") &&
		!strings.HasPrefix(path, "ws://") && !strings.HasPrefix(path, "wss://") {
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		full = c.base + path
	}
	u, err := url.Parse(full)
	if err != nil {
		return full
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	}
	u.Fragment = ""
	return u.String()
}

func (c *Client) dialer() *websocket.Dialer {
	return &websocket.Dialer{Proxy: c.cfg.Proxy, TLSClientConfig: c.cfg.TLSConfig,
		HandshakeTimeout: c.cfg.Timeouts.Connect, ReadBufferSize: 64 << 10, WriteBufferSize: 64 << 10}
}

func (c *Client) wsHeader() http.Header {
	h := http.Header{}
	h.Set("accessKey", c.key)
	h.Set("User-Agent", userAgent)
	return h
}

func resolveFormat(req *SessionRequest, v SessionView) (Format, error) {
	effective := str(v.EffectiveOptions["inputEncoding"])
	requested := req.encoding()
	if requested != "" && effective != "" && !strings.EqualFold(strings.TrimSpace(requested), effective) {
		return Format{}, &AudioFormatError{Msg: fmt.Sprintf("requested inputEncoding %s but the session uses %s", requested, effective)}
	}
	enc := effective
	if enc == "" {
		enc = requested
	}
	f, err := FormatOf(enc)
	if err != nil {
		return Format{}, err
	}
	if rate := optInt(v.EffectiveOptions["sampleRateHz"]); rate != nil && int(*rate) != f.SampleRateHz {
		return Format{}, &AudioFormatError{Msg: fmt.Sprintf("session sampleRateHz %d does not match %s", *rate, f.Encoding)}
	}
	return f, nil
}

func checkID(id string) error {
	if id == "" || strings.ContainsAny(id, "/?#") {
		return errors.New("myvocal: invalid session id")
	}
	return nil
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func minDur(ds ...time.Duration) time.Duration {
	m := ds[0]
	for _, d := range ds[1:] {
		if d < m {
			m = d
		}
	}
	if m < 0 {
		return 0
	}
	return m
}

func maxDur(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func sleepCtx(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
