package sttrealtime

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// A14: every shipped example builds and runs unchanged against the protocol fixture.
func TestA14Examples(t *testing.T) {
	if testing.Short() {
		t.Skip("examples run in the full suite")
	}
	f := fx(t)
	goBin := filepath.Join(runtime.GOROOT(), "bin", "go")
	bin := t.TempDir()
	build := exec.Command(goBin, "build", "-o", bin+string(os.PathSeparator), "./examples/...")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build examples: %v\n%s", err, out)
	}
	wav := filepath.Join(repoRoot, "fixtures", "audio", "tone_pcm_s16le_16000.wav")
	raw := filepath.Join(repoRoot, "fixtures", "audio", "tone_pcm_s16le_16000.raw")
	cases := []struct {
		name   string
		args   []string
		expect string
	}{
		{"file_stream", []string{wav}, "status: COMPLETED"},
		{"byte_stream", []string{raw}, "status: COMPLETED"},
		{"manual_vad", []string{wav, "manual"}, "manual -> COMPLETED"},
		{"manual_vad", []string{wav, "vad"}, "vad -> COMPLETED"},
		{"pause_resume", []string{wav}, "status: COMPLETED / GET: COMPLETED"},
		{"abnormal_finish", []string{wav}, "closed out: COMPLETED via REST: true reason: DISCONNECTED"},
	}
	for _, tc := range cases {
		exe := filepath.Join(bin, tc.name)
		if runtime.GOOS == "windows" {
			exe += ".exe"
		}
		cmd := exec.Command(exe, tc.args...)
		cmd.Env = append(os.Environ(), "MYVOCAL_ACCESS_KEY="+fixtureKey, "MYVOCAL_API_BASE="+f.Base,
			"NO_PROXY=127.0.0.1,localhost", "no_proxy=127.0.0.1,localhost")
		out, err := cmd.CombinedOutput()
		if err != nil || !strings.Contains(string(out), tc.expect) {
			t.Fatalf("%s %v: %v\n%s", tc.name, tc.args, err, out)
		}
		if strings.Contains(string(out), fixtureKey) {
			t.Fatalf("%s printed the key", tc.name)
		}
	}
}
