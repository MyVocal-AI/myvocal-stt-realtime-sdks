# MyVocal STT Realtime SDKs (Java · Go · Python)

Server-side SDKs for the existing MyVocal Speech to Text Realtime API
(`https://api.myvocal.ai/sound_clone/api/v1/stt/realtime`). Configure a session, hand it audio,
consume events and call `finish`. The SDKs manage framing, sample cursors, in-connection
rotation and the end of the session. After an unexpected disconnect they query and finish the
**original** session over REST. They do not reconnect or resend audio.

| Language | Package | Directory | Quick start |
|---|---|---|---|
| Java 8+ | `ai.myvocal:myvocal-stt-realtime:1.0.0` (JAR, sources, Javadoc, POM) | `java/` | `java/README.md` |
| Go 1.22+ | `github.com/MyVocal-AI/myvocal-stt-realtime-sdks/go` (tag `go/v1.0.0`) | `go/` | `go/README.md` |
| Python 3.10+ | `myvocal-stt-realtime==1.0.0` (wheel, sdist; sync + asyncio) | `python/` | `python/README.md` |

Shared behaviour: `docs/behavior-guide.md` (user guide) and `contract/realtime.md` (normative).
Documentation: https://docs.myvocal.ai.

## Releases

Version 1.0.0 is tagged `v1.0.0` (all three SDKs) and `go/v1.0.0` (Go module). The
[GitHub Release v1.0.0](https://github.com/MyVocal-AI/myvocal-stt-realtime-sdks/releases/tag/v1.0.0) carries the Java JAR, sources JAR, Javadoc JAR and POM and the Python
wheel and sdist, with `SHA256SUMS`. Registry listings use these same files:

- Go: `go get github.com/MyVocal-AI/myvocal-stt-realtime-sdks/go@v1.0.0`
- Python: PyPI project `myvocal-stt-realtime` (https://pypi.org/project/myvocal-stt-realtime/), or the
  release wheel: `pip install https://github.com/MyVocal-AI/myvocal-stt-realtime-sdks/releases/download/v1.0.0/myvocal_stt_realtime-1.0.0-py3-none-any.whl`
- Java: Maven Central `ai.myvocal:myvocal-stt-realtime:1.0.0`
  (https://central.sonatype.com/artifact/ai.myvocal/myvocal-stt-realtime), or the release files

Check the registry pages for current availability; see `RELEASING.md` for how releases are made.

## Layout

```text
contract/realtime.md     shared behaviour contract
fixtures/                scenarios.json (46 protocol scenarios), deterministic audio (7 encodings + WAV),
                         tls/ (local test CA + server cert for the fixture only, never packaged)
tools/fixture/server.py  local HTTP/WebSocket protocol fixture (stdlib Python, TLS + CONNECT proxy modes)
tools/matrix/            runtime matrix + clean-install checks (Windows PowerShell)
tools/live/              bounded real-API validation (reads MYVOCAL_ACCESS_KEY from the environment only)
tools/scan_public.py     A13 static scan of packages / public docs
java/ go/ python/        SDKs, tests (A01–A14), examples
docs/                    shared behaviour guide
```

## Tests

All three suites drive the same fixture and scenarios; nothing contacts the real API.

```bash
cd python && python -m pytest                 # installed package (wheel) or PYTHONPATH=src
cd go && go test ./...                        # FIXTURE_PYTHON=<python> if `python` is not on PATH
cd java && mvn test                           # FIXTURE_PYTHON as above
python tools/scan_public.py --terms <private rules file kept outside the repo> <jar|whl|tar.gz|dir> ...
```

Runtime matrix (Java 8/11/13/17/21 against one JAR, Go 1.22/1.25, Python 3.10/3.13) and clean
installs (isolated Maven repository + consumer project, wheel-only venv, Go consumer with
`replace` to a clean export):

```powershell
powershell -ExecutionPolicy Bypass -File tools\matrix\run-matrix.ps1 -Tools <isolated runtimes dir> -Out <log dir>
```

Real-API validation (internal test account only; see `tools/live/README.md`):

```bash
MYVOCAL_ACCESS_KEY=... python tools/live/live_python.py --out <dir>
```

The fixture's test key and TLS material are test-only values; they are not credentials and are
excluded from every package.

## Licence

Apache-2.0 (`LICENSE`, `NOTICE`).
