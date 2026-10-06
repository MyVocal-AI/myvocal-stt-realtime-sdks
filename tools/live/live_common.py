"""Shared helpers for the bounded real-API validation (plan v1.1 section 7).

Credentials come ONLY from the environment (MYVOCAL_ACCESS_KEY, optional MYVOCAL_API_BASE) and
are never printed or written. Records keep sanitized identifiers and numbers only: no keys,
tickets, signed URLs or transcript text (text is reduced to its length).
"""
import json
import os
import sys
import time

ENCODINGS = {"pcm_s16le_8000": (8000, 2), "pcm_s16le_16000": (16000, 2), "pcm_s16le_22050": (22050, 2),
             "pcm_s16le_24000": (24000, 2), "pcm_s16le_44100": (44100, 2), "pcm_s16le_48000": (48000, 2),
             "mulaw_8000": (8000, 1)}


PROD_BASE = "https://api.myvocal.ai"
FIXTURE_KEY_PREFIX = "fixture-"


def env():
    """(key, base). A real key is only ever sent to the production origin; any other base (the local
    protocol fixture for smoke tests) requires a fixture key, so a real key can never reach it."""
    key = os.environ.get("MYVOCAL_ACCESS_KEY", "")
    if not key:
        sys.exit("MYVOCAL_ACCESS_KEY is not set (supply it through the sanctioned secure entry)")
    base = (os.environ.get("MYVOCAL_API_BASE") or PROD_BASE).rstrip("/")
    if base != PROD_BASE and not key.startswith(FIXTURE_KEY_PREFIX):
        sys.exit("refusing: a non-fixture key may only be sent to " + PROD_BASE)
    return key, base


def error_record(scenario, exc, session=None):
    """Sanitized record for a scenario that raised (SDK errors never carry credentials)."""
    rec = {"scenario": scenario, "ok": False, "status": None, "transcriptionId": None,
           "error": type(exc).__name__, "errorCode": getattr(exc, "error_code", None), "message": str(exc)[:300]}
    if session is not None:
        rec["sessionId"] = getattr(session, "session_id", None)
    return rec


def raw_path(audio_dir, enc):
    return os.path.join(audio_dir, "speech_%s.raw" % enc)


def samples_of(audio_dir, enc):
    return os.path.getsize(raw_path(audio_dir, enc)) // ENCODINGS[enc][1]


class Recorder:
    def __init__(self, out_dir, language):
        os.makedirs(out_dir, exist_ok=True)
        self.path = os.path.join(out_dir, "live-%s.json" % language)
        self.doc = {"language": language, "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "records": []}
        if os.path.exists(self.path):  # resume: keep earlier records of this run
            with open(self.path, encoding="utf-8-sig") as f:
                self.doc = json.load(f)

    redo = None  # compiled regex: scenarios to run again even when already recorded

    def done(self, scenario):
        if self.redo is not None and self.redo.search(scenario):
            return False
        return any(r["scenario"] == scenario and r.get("ok") is not None for r in self.doc["records"])

    def add(self, rec):
        # a re-run keeps the earlier attempt (its session is still reconciled) under "superseded"
        for old in self.doc["records"]:
            if old["scenario"] == rec["scenario"] and (old.get("transcriptionId") or old.get("sessionId")):
                self.doc.setdefault("superseded", []).append(old)
        self.doc["records"] = [r for r in self.doc["records"] if r["scenario"] != rec["scenario"]] + [rec]
        self.doc["updated"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.doc, f, indent=2, ensure_ascii=False)
        print("%-34s %-5s %s %s" % (rec["scenario"], "OK" if rec.get("ok") else "CHECK", rec.get("status"),
                                     rec.get("transcriptionId")))


def summarize(result, scenario, enc, expected_samples, **extra):
    """FinishResult (Python SDK) -> sanitized record."""
    u = result.usage
    text = result.text or ""
    rec = {"scenario": scenario, "encoding": enc, "sessionId": result.session_id,
           "transcriptionId": result.transcription_id, "status": result.status,
           "capturedSamples": u.captured_samples, "receivedSamples": u.received_samples,
           "expectedSamples": expected_samples, "billableCharacters": u.billable_characters,
           "estimatedCharacters": u.estimated_characters, "lines": len(result.lines), "textChars": len(text),
           "notices": [n.code for n in result.notices], "viaRest": result.via_rest,
           "closeoutReason": result.closeout_reason, "serverErrors": list(result.server_errors)}
    rec.update(extra)
    return rec
