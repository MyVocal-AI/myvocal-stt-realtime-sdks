"""A14: every shipped example runs unchanged against the protocol fixture (env-configured)."""
import os
import subprocess
import sys

import pytest

from conftest import AUDIO, KEY, ROOT

EX = os.path.join(ROOT, "python", "examples")
WAV = os.path.join(AUDIO, "tone_pcm_s16le_16000.wav")
RAW = os.path.join(AUDIO, "tone_pcm_s16le_16000.raw")


def run(fx, *args):
    env = dict(os.environ, MYVOCAL_ACCESS_KEY=KEY, MYVOCAL_API_BASE=fx.base, NO_PROXY="127.0.0.1,localhost",
               no_proxy="127.0.0.1,localhost")
    if os.environ.get("EXAMPLES_USE_INSTALLED") != "1":
        env["PYTHONPATH"] = os.path.join(ROOT, "python", "src")
    p = subprocess.run([sys.executable, *args], env=env, capture_output=True, text=True, timeout=90)
    assert p.returncode == 0, p.stdout + p.stderr
    assert KEY not in p.stdout + p.stderr
    return p.stdout


@pytest.mark.parametrize("script,args,expect", [
    ("file_stream.py", [WAV], "status: COMPLETED"),
    ("byte_stream_async.py", [RAW], "status: COMPLETED"),
    ("manual_vad.py", [WAV, "manual"], "manual -> COMPLETED"),
    ("manual_vad.py", [WAV, "vad"], "vad -> COMPLETED"),
    ("pause_resume.py", [WAV], "status: COMPLETED / GET: COMPLETED"),
    ("abnormal_finish.py", [WAV], "closed out: COMPLETED via REST: True reason: DISCONNECTED"),
])
def test_a14_example(fx, script, args, expect):
    out = run(fx, os.path.join(EX, script), *args)
    assert expect in out, out
