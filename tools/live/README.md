# Live (real API) validation — plan v1.1 §7

Bounded checks against the production entry with the **internal test account only**, using
project-owned synthetic speech (Windows SAPI, `make_audio.ps1`). No customer audio, no prod changes,
no long sessions, no balance exhaustion. Controlled failures (rotation, ACK delay, low balance, auth
errors) stay in the local fixture suites.

| Script | Covers |
|---|---|
| `live_python.py` | R1 all 7 encodings × R2 option combinations (language+alignment+hints, language only, entities, cleanup, background filter without alignment, defaults), R3 commit-continue / pause-resume / ticket helper / repeated finish, R4 client-only disconnect close-out + 75 s idle with ping, R5 async main flow and the sync/async examples from the wheel |
| `java/LiveJava.java` | R1 `pcm_s16le_16000` manual + `mulaw_8000` VAD, R2 language+alignment, R3, R4 (compiled against the installed JAR) |
| `go/main.go` | R1 `pcm_s16le_16000` VAD + `mulaw_8000` manual, R2 language+alignment, R3, R4 (replace → clean candidate export) |
| `run-live.ps1` | `-Phase prepare` (no credential: synthetic audio, compiles the Java/Go harnesses) and `-Phase run` (balance at start, the three harnesses from the installed candidates, R5 Java/Go file-stream examples, then `reconcile.py`) |
| `balance.py` | available Characters through the existing read-only `GET /stt/capabilities` (`data.account.availableCharacters`) |
| `reconcile.py` | R6 / A16: History scan of the window (catches sessions a harness did not record), per-ID session + History billing state incl. PARTIAL/FAILED, repeated finish with settled amounts and balance re-read (no second charge), settled sum vs. balance window |

Credentials: `MYVOCAL_ACCESS_KEY` must be present in the environment of the `-Phase run` process only,
supplied through the sanctioned secure entry (for example a process that reads the key from the
operator's secret store and starts `run-live.ps1` with it in the child environment). Never put it on a
command line, in a file, in a log or in a persistent user/machine environment variable. Scripts never
print or write it. A non-fixture key is refused for any base other than `https://api.myvocal.ai`, so a
real key can never reach the local protocol fixture. Records keep only ids, statuses and numbers
(`textChars` instead of transcript text).

Balance: `balance.py` reads it at the start; `reconcile.py` reads it at the end of the window and again
after the repeated finishes. If other consumption shares the account in the window, the History scan
lists it as `historyOnly` and the balance comparison says so; do not attribute a total balance
difference to the SDK.
