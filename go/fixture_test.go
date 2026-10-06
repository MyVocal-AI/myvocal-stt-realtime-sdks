package sttrealtime

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

var repoRoot = func() string { p, _ := filepath.Abs(".."); return p }()

type scenarioFile struct {
	AccessKey string           `json:"accessKey"`
	Scenarios []map[string]any `json:"scenarios"`
}

var scen = func() scenarioFile {
	var f scenarioFile
	data, err := os.ReadFile(filepath.Join(repoRoot, "fixtures", "scenarios.json"))
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(data, &f); err != nil {
		panic(err)
	}
	return f
}()

var fixtureKey = scen.AccessKey

func scenario(id string) map[string]any {
	for _, s := range scen.Scenarios {
		if s["id"] == id {
			return s
		}
	}
	panic("no scenario " + id)
}

type fixture struct {
	cmd       *exec.Cmd
	Base      string
	ProxyPort int
	TLS       bool
	client    *http.Client
}

func pythonExe() string {
	if p := os.Getenv("FIXTURE_PYTHON"); p != "" {
		return p
	}
	if runtime.GOOS == "windows" {
		return "python"
	}
	return "python3"
}

func startFixture(t testing.TB, flags ...string) *fixture {
	args := append([]string{filepath.Join(repoRoot, "tools", "fixture", "server.py")}, flags...)
	cmd := exec.Command(pythonExe(), args...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start fixture: %v", err)
	}
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil {
		t.Fatalf("fixture did not start: %v", err)
	}
	var info struct {
		HTTP  int  `json:"http"`
		Proxy *int `json:"proxy"`
		TLS   bool `json:"tls"`
	}
	if err := json.Unmarshal([]byte(line), &info); err != nil {
		t.Fatalf("fixture banner %q: %v", line, err)
	}
	f := &fixture{cmd: cmd, TLS: info.TLS}
	scheme := "http"
	tr := &http.Transport{Proxy: nil}
	if info.TLS {
		scheme = "https"
		tr.TLSClientConfig = testTLS(t)
	}
	f.client = &http.Client{Transport: tr, Timeout: 10 * time.Second}
	f.Base = fmt.Sprintf("%s://127.0.0.1:%d", scheme, info.HTTP)
	if info.Proxy != nil {
		f.ProxyPort = *info.Proxy
	}
	return f
}

func (f *fixture) stop() { _ = f.cmd.Process.Kill(); _ = f.cmd.Wait() }

func (f *fixture) getJSON(t testing.TB, path string, v any) {
	t.Helper()
	resp, err := f.client.Get(f.Base + "/__fixture" + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) post(t testing.TB, path string) {
	resp, err := f.client.Post(f.Base+"/__fixture"+path, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}

// record is the fixture's server-side view of one session.
type record struct {
	SessionID       string           `json:"sessionId"`
	TranscriptionID string           `json:"transcriptionId"`
	Status          string           `json:"status"`
	Epochs          []map[string]any `json:"epochs"`
	Captured        *json.Number     `json:"captured"`
	CapturedClaims  []json.Number    `json:"capturedClaims"`
	StoredSha256    string           `json:"storedSha256"`
	Frames          []map[string]any `json:"frames"`
	RestCalls       []map[string]any `json:"restCalls"`
	Connections     []map[string]any `json:"connections"`
	FinishCount     int              `json:"finishCount"`
	RestFinishCount int              `json:"restFinishCount"`
	Settled         *json.Number     `json:"settled"`
	SettledCount    int              `json:"settledCount"`
	UsageReplies    []map[string]any `json:"usageReplies"`
	RawRequest      map[string]any   `json:"rawRequest"`
	Effective       map[string]any   `json:"effectiveOptions"`
}

func (f *fixture) record(t testing.TB, id string) record {
	var r record
	f.getJSON(t, "/sessions/"+id, &r)
	return r
}

func (f *fixture) byTitle(t testing.TB, title string) []record {
	var r []record
	f.getJSON(t, "/by-title/"+title, &r)
	return r
}

var (
	sharedOnce sync.Once
	shared     *fixture
	sharedTLS  *fixture
)

func TestMain(m *testing.M) {
	code := m.Run()
	if shared != nil {
		shared.stop()
	}
	if sharedTLS != nil {
		sharedTLS.stop()
	}
	os.Exit(code)
}

func fx(t testing.TB) *fixture {
	sharedOnce.Do(func() { shared = startFixture(t) })
	return shared
}

var tlsOnce sync.Once

func fxTLS(t testing.TB) *fixture {
	tlsOnce.Do(func() { sharedTLS = startFixture(t, "--tls", "--proxy") })
	return sharedTLS
}

func testTLS(t testing.TB) *tls.Config {
	pem, err := os.ReadFile(filepath.Join(repoRoot, "fixtures", "tls", "ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	return &tls.Config{RootCAs: pool}
}

func newClient(t testing.TB, f *fixture, mod ...func(*Config)) *Client {
	cfg := Config{AccessKey: fixtureKey, BaseURL: f.Base, Proxy: NoProxy}
	for _, m := range mod {
		m(&cfg)
	}
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func audio(t testing.TB, enc string, seconds float64) []byte {
	data, err := os.ReadFile(filepath.Join(repoRoot, "fixtures", "audio", "tone_"+enc+".raw"))
	if err != nil {
		t.Fatal(err)
	}
	if seconds > 0 {
		f, _ := FormatOf(enc)
		n := int(float64(f.SampleRateHz)*seconds) * f.BytesPerSample
		data = data[:n]
	}
	return data
}

func sha(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func req(title string, enc string) *SessionRequest {
	if enc == "" {
		enc = "pcm_s16le_16000"
	}
	return &SessionRequest{Title: title, Options: &SessionOptions{InputEncoding: enc}}
}

func ctxT(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func num(n *json.Number) int64 {
	if n == nil {
		return -1
	}
	v, _ := n.Int64()
	return v
}

func i64(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	}
	return -1
}

func sendAll(t testing.TB, s *Session, data []byte, chunk int) {
	t.Helper()
	for i := 0; i < len(data); i += chunk {
		end := i + chunk
		if end > len(data) {
			end = len(data)
		}
		if err := s.SendAudio(context.Background(), data[i:end]); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
}
