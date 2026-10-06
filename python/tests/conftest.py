import json
import os
import subprocess
import sys
import time
import urllib.request

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
FIXTURE = os.path.join(ROOT, "tools", "fixture", "server.py")
AUDIO = os.path.join(ROOT, "fixtures", "audio")
SCENARIOS = json.load(open(os.path.join(ROOT, "fixtures", "scenarios.json"), encoding="utf-8"))
KEY = SCENARIOS["accessKey"]
CA = os.path.join(ROOT, "fixtures", "tls", "ca.crt")


def scenario(sid):
    return next(s for s in SCENARIOS["scenarios"] if s["id"] == sid)


class FixtureProc:
    def __init__(self, *flags):
        py = os.environ.get("FIXTURE_PYTHON", sys.executable)
        self.proc = subprocess.Popen([py, FIXTURE, *flags], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     text=True)
        info = json.loads(self.proc.stdout.readline())
        self.port, self.proxy_port, self.tls = info["http"], info["proxy"], info["tls"]
        scheme = "https" if self.tls else "http"
        self.base = "%s://127.0.0.1:%d" % (scheme, self.port)
        self.control = "%s://127.0.0.1:%d/__fixture" % (scheme, self.port)

    def get(self, path):
        import ssl
        ctx = ssl.create_default_context(cafile=CA) if self.tls else None
        with urllib.request.urlopen(self.control + path, timeout=10, context=ctx) as r:
            return json.loads(r.read())

    def post(self, path):
        import ssl
        ctx = ssl.create_default_context(cafile=CA) if self.tls else None
        req = urllib.request.Request(self.control + path, data=b"{}", method="POST")
        with urllib.request.urlopen(req, timeout=10, context=ctx) as r:
            return json.loads(r.read())

    def record(self, session_id):
        return self.get("/sessions/" + session_id)

    def by_title(self, title):
        return self.get("/by-title/" + title)

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(5)
        except Exception:
            self.proc.kill()


@pytest.fixture(scope="session")
def fx():
    f = FixtureProc()
    yield f
    f.stop()


@pytest.fixture(scope="session")
def fx_tls():
    f = FixtureProc("--tls", "--proxy")
    yield f
    f.stop()


def audio(encoding, seconds=None):
    data = open(os.path.join(AUDIO, "tone_%s.raw" % encoding), "rb").read()
    if seconds is not None:
        from myvocal_stt_realtime import AudioFormat
        fmt = AudioFormat.of(encoding)
        data = data[: int(fmt.sample_rate_hz * seconds) * fmt.bytes_per_sample]
    return data


def sha(data):
    import hashlib
    return hashlib.sha256(data).hexdigest()
