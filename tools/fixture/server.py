#!/usr/bin/env python3
"""MyVocal STT Realtime local protocol fixture (L layer).

Stdlib-only HTTP/1.1 + WebSocket server that models the *existing* public MyVocal
Realtime protocol (REST envelope, idempotent create, pause/resume/finish/tickets,
accessKey handshake, epochs, sampleOffset cursors, rotation, events) so the Java,
Go and Python SDK test suites can drive the same deterministic scenarios.

It adds no product endpoint. Behaviour switches come from fixtures/scenarios.json,
selected by the session ``title`` (the scenario id). Control endpoints live under
``/__fixture`` and exist only in this test tool.

Usage:
    python server.py --port 0 [--tls] [--proxy] [--scenarios ../../fixtures/scenarios.json]
Prints one JSON line on stdout: {"http": port, "proxy": port|null, "tls": bool}
"""
import argparse
import asyncio
import base64
import hashlib
import json
import os
import ssl
import struct
import sys
import time
import uuid

STT = "/sound_clone/api/v1/stt"
REALTIME = STT + "/realtime"
START_BALANCE = 1000000
VALID_KEY = os.environ.get("FIXTURE_ACCESS_KEY", "fixture-key-SECRET-0001")
GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
ENCODINGS = {
    "pcm_s16le_8000": (8000, 2), "pcm_s16le_16000": (16000, 2), "pcm_s16le_22050": (22050, 2),
    "pcm_s16le_24000": (24000, 2), "pcm_s16le_44100": (44100, 2), "pcm_s16le_48000": (48000, 2),
    "mulaw_8000": (8000, 1),
}
OPTION_KEYS = ["inputEncoding", "additionalLanguageHints", "segmentCommitMode", "speechDetectionThreshold",
               "commitSilenceMs", "minimumSpeechMs", "minimumSilenceMs", "includeAlignment",
               "includeDetectedLanguage", "vocabularyHints", "removeDisfluencies", "entityCategories",
               "rewriteInstruction", "suppressBackgroundSpeech", "heartbeatIntervalMs", "processingContentStorage"]
ERRORS = {
    "INPUT_INVALID": (47201, False, "FIX_INPUT"), "PROJECT_NOT_FOUND": (47202, False, "RETURN_TO_STT"),
    "INSUFFICIENT_CHARACTERS": (47210, False, "OPEN_BILLING"), "IDEMPOTENCY_CONFLICT": (47215, False, "REFRESH"),
    "PROVIDER_UNAVAILABLE": (47217, True, "RETRY"), "TEMPORARILY_UNAVAILABLE": (47224, True, "RETRY_LATER"),
}
TERMINAL = ("COMPLETED", "PARTIAL", "FAILED")
RATE_PER_MINUTE = 420


class ApiError(Exception):
    def __init__(self, code, field=None):
        super().__init__(code)
        self.code, self.field = code, field


def envelope_ok(data):
    return {"code": 1, "message": "success", "data": data}


def envelope_err(code, field=None):
    num, retryable, action = ERRORS[code]
    return {"code": num, "message": code.lower().replace("_", " "), "data": {
        "errorCode": code, "retryable": retryable, "action": action, "requestId": "req_" + uuid.uuid4().hex[:12],
        "details": ({"field": field} if field else None)}}


def ceil_div(a, b):
    return -(-a // b)


# ----------------------------------------------------------------------------- option validation

def validate_options(raw):
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ApiError("INPUT_INVALID", "options")
    for key in raw:
        if key not in OPTION_KEYS:
            raise ApiError("INPUT_INVALID", "options." + key)
    eff = {}
    enc = raw.get("inputEncoding") or "pcm_s16le_16000"
    if not isinstance(enc, str) or enc.strip().lower() not in ENCODINGS:
        raise ApiError("INPUT_INVALID", "inputEncoding")
    enc = enc.strip().lower()
    eff["inputEncoding"] = enc
    eff["sampleRateHz"] = ENCODINGS[enc][0]

    def strlist(name, max_items, max_len=128):
        v = raw.get(name)
        if v is None or v == []:
            return []
        if not isinstance(v, list) or len(v) > max_items:
            raise ApiError("INPUT_INVALID", name)
        out = []
        for item in v:
            if not isinstance(item, str) or not item.strip() or len(item.strip()) > max_len:
                raise ApiError("INPUT_INVALID", name)
            out.append(item.strip())
        return out

    def ranged(name, lo, hi, integer=False):
        v = raw.get(name)
        if v is None:
            return None
        if isinstance(v, bool) or not isinstance(v, (int, float)) or (integer and not float(v).is_integer()):
            raise ApiError("INPUT_INVALID", name)
        if v < lo or v > hi:
            raise ApiError("INPUT_INVALID", name)
        return v

    def boolean(name):
        v = raw.get(name)
        if v is None:
            return None
        if not isinstance(v, bool):
            raise ApiError("INPUT_INVALID", name)
        return v

    eff["additionalLanguageHints"] = strlist("additionalLanguageHints", 8)
    mode = raw.get("segmentCommitMode") or "manual"
    if not isinstance(mode, str) or mode.strip().lower() not in ("manual", "vad"):
        raise ApiError("INPUT_INVALID", "segmentCommitMode")
    eff["segmentCommitMode"] = mode.strip().lower()
    eff["speechDetectionThreshold"] = ranged("speechDetectionThreshold", 0, 1)
    eff["commitSilenceMs"] = ranged("commitSilenceMs", 0, 60000)
    eff["minimumSpeechMs"] = ranged("minimumSpeechMs", 0, 60000, True)
    eff["minimumSilenceMs"] = ranged("minimumSilenceMs", 0, 60000, True)
    eff["includeAlignment"] = boolean("includeAlignment")
    eff["includeDetectedLanguage"] = boolean("includeDetectedLanguage")
    eff["vocabularyHints"] = strlist("vocabularyHints", 50)
    eff["removeDisfluencies"] = boolean("removeDisfluencies")
    cats = raw.get("entityCategories")
    if cats is None or cats == []:
        eff["entityCategories"] = []
    else:
        if not isinstance(cats, list) or any(not isinstance(c, str) or not c.strip() for c in cats):
            raise ApiError("INPUT_INVALID", "entityCategories")
        eff["entityCategories"] = [c.strip().lower() for c in cats]
    ri = raw.get("rewriteInstruction")
    if ri is not None and (not isinstance(ri, str) or len(ri.strip()) > 2000):
        raise ApiError("INPUT_INVALID", "rewriteInstruction")
    eff["rewriteInstruction"] = ri.strip() if isinstance(ri, str) and ri.strip() else None
    eff["suppressBackgroundSpeech"] = boolean("suppressBackgroundSpeech")
    eff["heartbeatIntervalMs"] = ranged("heartbeatIntervalMs", 500, 10000, True)
    eff["processingContentStorage"] = boolean("processingContentStorage")
    if eff["rewriteInstruction"] is not None and eff["entityCategories"]:
        raise ApiError("INPUT_INVALID", "entityCategories")
    if eff["suppressBackgroundSpeech"] is True:
        if eff["includeAlignment"] is True:
            raise ApiError("INPUT_INVALID", "includeAlignment")
        if eff["includeAlignment"] is None:
            eff["includeAlignment"] = False
    return eff


# ----------------------------------------------------------------------------- session model

class Session:
    counter = 0

    def __init__(self, server, title, language, options, scenario, raw_request):
        Session.counter += 1
        self.server = server
        self.id = "rts_fx%06d%s" % (Session.counter, uuid.uuid4().hex[:8])
        self.transcription_id = "stt_fx%06d%s" % (Session.counter, uuid.uuid4().hex[:8])
        self.title, self.language, self.options = title, language, options
        self.raw_request = raw_request
        self.scenario = scenario or {}
        self.fx = self.scenario.get("fixture", {})
        self.rate, self.bps = ENCODINGS[options["inputEncoding"]]
        self.state = "RECORDING"
        self.project_status = "RECORDING"
        self.epochs = [{"index": 1, "state": "OPEN", "sent": 0, "committed": 0, "rotate_due": False,
                        "frames": 0, "line_start": 0}]
        self.captured = None
        self.stored = bytearray()
        self.lines = []
        self.partial = ""
        self.notices = []
        self.completed_view = None
        self.frame_counter = 0
        self.frames = []
        self.rest_calls = []
        self.connections = []
        self.captured_claims = []
        self.finish_count = 0
        self.rest_finish_count = 0
        self.created_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        self.lock = asyncio.Lock()
        self.socket = None
        self.rest_down = False
        self.settled = None
        self.settled_count = 0
        self.usage_replies = []
        self.dropped_finish_once = False
        self.error_frames_done = set()
        self.pending_supplements = []

    # -- helpers
    @property
    def active(self):
        return self.epochs[-1]

    def received(self):
        return sum(e["sent"] for e in self.epochs)

    def committed(self):
        return sum(e["committed"] for e in self.epochs)

    def big(self):
        return int(self.fx.get("bigOffset", 0))

    def billable(self):
        if self.settled is not None:
            return self.settled
        return ceil_div(self.committed() * RATE_PER_MINUTE, self.rate * 60) if self.committed() else 0

    def estimated(self):
        s = self.received()
        return ceil_div(s * RATE_PER_MINUTE, self.rate * 60) if s else 0

    def usage_numbers(self):
        b = self.big()
        elapsed = self.received() * 1000 // self.rate
        if self.fx.get("maxInt64Elapsed"):
            elapsed = 9223372036854775807
        return {
            "estimatedCharacters": self.estimated() + b,
            "billableCharacters": self.billable() + b,
            "elapsedMs": elapsed if self.fx.get("maxInt64Elapsed") else elapsed + b,
            "sentSamples": self.active["sent"],
            "receivedSamples": self.received() + b,
            "capturedSamples": self.captured,
        }

    def accepting(self, rotate=False):
        return self.state == "RECORDING" and self.active["state"] == "OPEN" and not rotate

    def status_payload(self, rotate=False, error_code=None):
        nums = self.usage_numbers()
        p = {k: (None if v is None else str(v)) for k, v in nums.items()}
        p["accepting"] = self.accepting(rotate)
        p["epoch"] = self.active["index"]
        if rotate:
            p["rotate"] = True
        if error_code:
            p["errorCode"] = error_code
        return p

    def view(self):
        nums = self.usage_numbers()
        return {
            "sessionId": self.id, "transcriptionId": self.transcription_id,
            "status": self.project_status, "modelId": "myvocal_stt_realtime_v1",
            "streamUrl": REALTIME + "/sessions/" + self.id + "/stream",
            "epoch": self.active["index"], "accepting": self.accepting(), "partial": self.partial,
            "lines": [dict(l) for l in self.lines], "notices": list(self.notices),
            "usage": {k: (None if v is None else str(v)) for k, v in nums.items()},
            "effectiveOptions": dict(self.options, languageHint=self.language),
            "createdAt": self.created_at, "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

    def claim(self, value):
        if value is None:
            return
        try:
            v = int(value)
        except (TypeError, ValueError):
            raise ApiError("INPUT_INVALID", "capturedSamples")
        if v < 0:
            raise ApiError("INPUT_INVALID", "capturedSamples")
        self.captured_claims.append(v)
        self.captured = v if self.captured is None else max(self.captured, v)

    def make_line(self, start, end):
        idx = len(self.lines) + 1
        texts = self.fx.get("lineTexts")
        text = texts[(idx - 1) % len(texts)] if texts else "segment %d samples %d-%d" % (idx, start, end)
        b = self.big()
        line = {"id": "line_%d" % idx, "text": text, "revisedText": None, "languageCode": None,
                "languageProbability": None, "entities": [],
                "words": [{"text": text.split(" ")[0] if text else "", "startMs": str(start * 1000 // self.rate + b),
                           "endMs": str(end * 1000 // self.rate + b)}],
                "characterAlignments": []}
        if self.fx.get("nullWordTime"):
            line["words"].append({"text": "untimed", "startMs": None, "endMs": None})
        if self.fx.get("finalWithLanguage"):
            line["languageCode"], line["languageProbability"] = "en", 0.98
        self.lines.append(line)
        return line

    def commit_open(self, events):
        e = self.active
        if e["state"] == "OPEN" and e["sent"] > e["committed"]:
            start = e["line_start"]
            e["committed"] = e["sent"]
            e["line_start"] = e["sent"]
            base = sum(x["sent"] for x in self.epochs[:-1])
            line = self.make_line(base + start, base + e["sent"])
            events.append(("transcript.final", dict(line)))
            self.schedule_supplements(line, events)
            if self.partial:
                self.partial = ""

    def schedule_supplements(self, line, events):
        sup = self.fx.get("supplements") or {}
        if not sup:
            return
        self.pending_supplements.append((line["id"], sup))

    def flush_supplements(self, events):
        for line_id, sup in self.pending_supplements:
            line = next(l for l in self.lines if l["id"] == line_id)
            if sup.get("alignment"):
                line["languageCode"], line["languageProbability"] = "en", 0.97
                line["words"] = [{"text": w, "startMs": str(i * 100), "endMs": str(i * 100 + 90)}
                                 for i, w in enumerate((line["text"] or "x").split(" "))]
                line["characterAlignments"] = [{"text": c, "startMs": str(i * 10), "endMs": str(i * 10 + 9)}
                                               for i, c in enumerate((line["text"] or "x")[:3])]
                events.append(("transcript.alignment", {"id": line_id, "languageCode": "en",
                                                        "languageProbability": 0.97, "words": line["words"],
                                                        "characterAlignments": line["characterAlignments"]}))
            ent = sup.get("entities")
            if ent == "empty":
                line["entities"] = []
                events.append(("transcript.entities", {"id": line_id, "entities": []}))
            elif ent == "some":
                line["entities"] = [{"text": line["text"].split(" ")[0], "entityType": "name", "startChar": 0,
                                     "endChar": len(line["text"].split(" ")[0])}]
                events.append(("transcript.entities", {"id": line_id, "entities": line["entities"]}))
            if sup.get("revision"):
                line["revisedText"] = line["text"].upper()
                events.append(("transcript.revision", {"id": line_id, "text": line["text"],
                                                       "revisedText": line["revisedText"]}))
        missing = self.fx.get("missingNotice")
        if missing and self.lines:
            ids = [l["id"] for l in self.lines][: missing.get("lines", 1)]
            notice = {"code": "SUPPLEMENT_NOT_RECEIVED", "message": "A requested supplement did not arrive.",
                      "supplements": missing.get("kinds", ["entities"]), "lineIds": ids}
            self.notices.append(notice)
            events.append(("session.notice", dict(notice)))
        if self.fx.get("storageNotice"):
            notice = {"code": "PROCESSING_STORAGE_NOT_APPLIED", "message": "Not applied."}
            self.notices.append(notice)
            events.append(("session.notice", dict(notice)))
        self.pending_supplements = []

    def pause_epoch(self, events):
        if self.state == "RECORDING":
            self.commit_open(events)
            self.active["state"] = "CLOSED"
            self.state = "PAUSED"
            self.project_status = "PAUSED"

    def resume(self, events):
        if self.state not in ("PAUSED", "RECORDING"):
            return
        e = self.active
        if self.state == "RECORDING" and e["state"] == "OPEN":
            if e["sent"] == 0:
                return
            self.pause_epoch(events)
        self.epochs.append({"index": e["index"] + 1, "state": "OPEN", "sent": 0, "committed": 0,
                            "rotate_due": False, "frames": 0, "line_start": 0})
        self.state = "RECORDING"
        self.project_status = "RECORDING"

    def finish(self, events):
        self.finish_count += 1
        if self.state == "CLOSED":
            return
        if self.state == "RECORDING":
            self.pause_epoch(events)
        self.flush_supplements(events)
        received = self.received()
        if received == 0:
            outcome = "FAILED"
        elif self.captured is not None and self.captured > received:
            outcome = "PARTIAL"
        else:
            outcome = "COMPLETED"
        outcome = self.fx.get("finishStatus", outcome)
        self.settled = ceil_div(self.committed() * RATE_PER_MINUTE, self.rate * 60) if self.committed() else 0
        self.settled_count += 1
        self.state = "CLOSED"
        self.project_status = outcome

    def record(self):
        return {
            "sessionId": self.id, "transcriptionId": self.transcription_id, "title": self.title,
            "languageHint": self.language, "rawRequest": self.raw_request,
            "state": self.state, "status": self.project_status, "epochs": self.epochs,
            "captured": self.captured, "capturedClaims": self.captured_claims,
            "storedSamples": len(self.stored) // self.bps, "storedBytes": len(self.stored),
            "storedSha256": hashlib.sha256(bytes(self.stored)).hexdigest(),
            "frames": self.frames, "restCalls": self.rest_calls, "connections": self.connections,
            "lines": self.lines, "notices": self.notices, "finishCount": self.finish_count,
            "restFinishCount": self.rest_finish_count, "settled": self.settled, "settledCount": self.settled_count,
            "usageReplies": self.usage_replies, "effectiveOptions": self.options,
        }


# ----------------------------------------------------------------------------- websocket framing

async def ws_read_frame(reader):
    head = await reader.readexactly(2)
    fin = head[0] & 0x80
    opcode = head[0] & 0x0F
    masked = head[1] & 0x80
    length = head[1] & 0x7F
    if length == 126:
        length = struct.unpack("!H", await reader.readexactly(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", await reader.readexactly(8))[0]
    mask = await reader.readexactly(4) if masked else None
    data = await reader.readexactly(length) if length else b""
    if mask:
        data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
    return fin, opcode, data


def ws_frame(opcode, payload):
    head = bytearray([0x80 | opcode])
    n = len(payload)
    if n < 126:
        head.append(n)
    elif n < 65536:
        head.append(126)
        head += struct.pack("!H", n)
    else:
        head.append(127)
        head += struct.pack("!Q", n)
    return bytes(head) + payload


class Connection:
    def __init__(self, session, writer, record):
        self.session, self.writer, self.record = session, writer, record
        self.sequence = 0
        self.closed = False
        self.send_lock = asyncio.Lock()

    async def emit(self, event_type, payload):
        if self.closed:
            return
        self.sequence += 1
        msg = {"eventId": "evt_" + uuid.uuid4().hex[:24], "sessionId": self.session.id,
               "sequence": self.sequence, "eventType": event_type, "payload": payload}
        self.record["events"].append({"sequence": self.sequence, "eventType": event_type})
        if event_type == "usage.updated":
            self.session.usage_replies.append(dict(payload))
        async with self.send_lock:
            try:
                self.writer.write(ws_frame(0x1, json.dumps(msg).encode()))
                await self.writer.drain()
            except Exception:
                self.closed = True

    async def emit_all(self, events):
        for et, p in events:
            await self.emit(et, p)

    def abort(self):
        self.closed = True
        try:
            self.writer.transport.abort()
        except Exception:
            pass


# ----------------------------------------------------------------------------- server

class Fixture:
    def __init__(self, scenarios):
        self.scenarios = scenarios
        self.sessions = {}
        self.idempotency = {}
        self.dropped_create = set()
        self.tickets = {}
        self.proxy_tunnels = 0
        self.proxy_targets = []

    def scenario(self, title):
        return self.scenarios.get(title or "", {})

    # ---------------- HTTP plumbing
    async def handle(self, reader, writer):
        try:
            while True:
                req = await self.read_request(reader)
                if req is None:
                    break
                method, path, headers, body = req
                if headers.get("upgrade", "").lower() == "websocket":
                    await self.websocket(reader, writer, path, headers)
                    return
                keep = await self.route(writer, method, path, headers, body)
                if not keep:
                    break
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        except Exception as exc:  # pragma: no cover - diagnostic
            print("fixture error:", repr(exc), file=sys.stderr)
        finally:
            try:
                writer.close()
            except Exception:
                pass

    async def read_request(self, reader):
        try:
            line = await reader.readline()
        except (ConnectionError, asyncio.IncompleteReadError):
            return None
        if not line:
            return None
        parts = line.decode("latin-1").strip().split(" ")
        if len(parts) < 2:
            return None
        method, target = parts[0], parts[1]
        headers = {}
        while True:
            h = await reader.readline()
            if h in (b"\r\n", b"\n", b""):
                break
            k, _, v = h.decode("latin-1").partition(":")
            headers[k.strip().lower()] = v.strip()
        body = b""
        n = int(headers.get("content-length", "0") or 0)
        if n:
            body = await reader.readexactly(n)
        return method, target, headers, body

    async def respond(self, writer, status, obj, extra_headers=None):
        data = json.dumps(obj).encode()
        reason = {200: "OK", 404: "Not Found", 503: "Service Unavailable", 401: "Unauthorized",
                  400: "Bad Request", 403: "Forbidden"}.get(status, "OK")
        head = "HTTP/1.1 %d %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n" % (status, reason, len(data))
        for k, v in (extra_headers or {}).items():
            head += "%s: %s\r\n" % (k, v)
        writer.write(head.encode() + b"\r\n" + data)
        await writer.drain()

    # -- shared History / account view of the existing STT REST surface (read-only)
    def billing_of(self, sess):
        status = sess.project_status
        if status in ("COMPLETED", "PARTIAL"):
            n = sess.billable()
            return {"state": "SETTLED", "reservedCharacters": str(n), "settledCharacters": str(n),
                    "releasedCharacters": "0", "billableCharacters": str(n)}
        if status == "FAILED":
            return {"state": "RELEASED", "reservedCharacters": "0", "settledCharacters": "0",
                    "releasedCharacters": "0", "billableCharacters": "0"}
        return {"state": "RESERVED", "reservedCharacters": str(sess.estimated()), "settledCharacters": "0",
                "releasedCharacters": "0", "billableCharacters": str(sess.estimated())}

    async def history(self, writer, method, path, query, headers):
        if headers.get("accesskey") != VALID_KEY:
            await self.respond(writer, 200, {"code": 401, "message": "Unauthorized", "data": None})
            return True
        if method != "GET":
            await self.respond(writer, 200, envelope_err("INPUT_INVALID"))
            return True
        by_tid = {s.transcription_id: s for s in self.sessions.values()}
        if path.startswith(STT + "/capabilities"):
            spent = sum(int(self.billing_of(s)["settledCharacters"]) for s in self.sessions.values())
            await self.respond(writer, 200, envelope_ok({"modelId": "myvocal_stt_v1", "account": {
                "state": "READY", "planKey": "fixture", "ratePerMinute": RATE_PER_MINUTE,
                "availableCharacters": str(START_BALANCE - spent)}}))
            return True
        tid = path[len(STT + "/transcriptions"):].strip("/")
        if tid:
            sess = by_tid.get(tid)
            if sess is None:
                await self.respond(writer, 200, envelope_err("PROJECT_NOT_FOUND"))
                return True
            await self.respond(writer, 200, envelope_ok({
                "transcriptionId": tid, "status": sess.project_status, "modelId": "myvocal_stt_realtime_v1",
                "title": sess.title, "createdAt": getattr(sess, "created_at_iso", None),
                "billing": self.billing_of(sess)}))
            return True
        q = dict(kv.split("=", 1) for kv in query.split("&") if "=" in kv)
        page, size = int(q.get("page", 1)), int(q.get("pageSize", 20))
        items = [{"transcriptionId": s.transcription_id, "modelId": "myvocal_stt_realtime_v1", "title": s.title,
                  "status": s.project_status, "characters": self.billing_of(s)["billableCharacters"],
                  "createdAt": getattr(s, "created_at_iso", None)} for s in reversed(list(self.sessions.values()))]
        await self.respond(writer, 200, envelope_ok({
            "page": page, "pageSize": size, "total": len(items), "totalPage": (len(items) + size - 1) // size,
            "items": items[(page - 1) * size: page * size]}))
        return True

    async def route(self, writer, method, target, headers, body):
        path, _, query = target.partition("?")
        if path.startswith("/__fixture"):
            return await self.control(writer, method, path, body)
        if path.startswith(STT + "/capabilities") or path.startswith(STT + "/transcriptions"):
            return await self.history(writer, method, path, query, headers)
        if not path.startswith(REALTIME + "/sessions"):
            await self.respond(writer, 404, {"error": "not found"})
            return True
        rest = path[len(REALTIME + "/sessions"):].strip("/")
        segs = [s for s in rest.split("/") if s]
        sess = self.sessions.get(segs[0]) if segs else None
        if sess is not None:
            sess.rest_calls.append({"method": method, "path": path, "hasAccessKeyHeader": "accesskey" in headers,
                                    "queryHasKey": VALID_KEY in query, "query": query,
                                    "idempotencyKey": headers.get("idempotency-key"), "t": time.time()})
            if sess.rest_down:
                await self.respond(writer, 503, {"error": "unavailable"})
                return False
        if headers.get("accesskey") != VALID_KEY:
            await self.respond(writer, 200, {"code": 401, "message": "Unauthorized", "data": None})
            return True
        try:
            payload = json.loads(body.decode() or "null") if body else None
        except ValueError:
            await self.respond(writer, 200, envelope_err("INPUT_INVALID"))
            return True
        try:
            if method == "POST" and not segs:
                return await self.create(writer, headers, payload)
            if sess is None:
                raise ApiError("PROJECT_NOT_FOUND")
            delay = sess.fx.get("restDelayMs", 0)
            if delay:
                await asyncio.sleep(delay / 1000)
            if method == "GET" and len(segs) == 1:
                await self.respond(writer, 200, envelope_ok(sess.view()))
                return True
            action = segs[1] if len(segs) > 1 else ""
            if method == "POST" and action == "tickets":
                tk = "tk_" + uuid.uuid4().hex
                self.tickets[tk] = sess.id
                await self.respond(writer, 200, envelope_ok({
                    "ticketId": "rtt_" + uuid.uuid4().hex[:12], "sessionId": sess.id,
                    "streamUrl": REALTIME + "/sessions/" + sess.id + "/stream", "expiresInSeconds": 60,
                    "ticket": tk}))
                return True
            if method == "POST" and action in ("pause", "resume", "finish"):
                if payload is not None and not isinstance(payload, dict):
                    raise ApiError("INPUT_INVALID")
                payload = payload or {}
                for k in payload:
                    if k != "capturedSamples":
                        raise ApiError("INPUT_INVALID", k)
                events = []
                async with sess.lock:
                    sess.claim(payload.get("capturedSamples"))
                    if action == "pause":
                        sess.pause_epoch(events)
                    elif action == "resume":
                        sess.resume(events)
                    else:
                        sess.rest_finish_count += 1
                        fdelay = sess.fx.get("finishDelayMs", 0)
                        if fdelay:
                            await asyncio.sleep(fdelay / 1000)
                        sess.finish(events)
                if sess.socket is not None and not sess.socket.closed:
                    await sess.socket.emit_all(events)
                if action == "finish" and sess.fx.get("dropRestFinishResponseOnce") and not sess.dropped_finish_once:
                    sess.dropped_finish_once = True
                    writer.transport.abort()
                    return False
                await self.respond(writer, 200, envelope_ok(sess.view()))
                return True
            await self.respond(writer, 404, {"error": "not found"})
            return True
        except ApiError as err:
            await self.respond(writer, 200, envelope_err(err.code, err.field))
            return True

    async def create(self, writer, headers, payload):
        key = headers.get("idempotency-key")
        if not key or len(key) > 128:
            raise ApiError("INPUT_INVALID", "Idempotency-Key")
        if payload is not None and not isinstance(payload, dict):
            raise ApiError("INPUT_INVALID")
        payload = payload or {}
        for k in payload:
            if k not in ("languageHint", "title", "options"):
                raise ApiError("INPUT_INVALID", k)
        scen = self.scenario(payload.get("title"))
        fx = scen.get("fixture", {})
        if fx.get("createEnvelopeError"):
            raise ApiError(fx["createEnvelopeError"])
        options = validate_options(payload.get("options"))
        fingerprint = json.dumps(payload, sort_keys=True)
        if key in self.idempotency:
            sid, fp = self.idempotency[key]
            if fp != fingerprint:
                raise ApiError("IDEMPOTENCY_CONFLICT")
            sess = self.sessions[sid]
            sess.rest_calls.append({"method": "POST", "path": "create(replay)", "idempotencyKey": key,
                                    "hasAccessKeyHeader": True, "t": time.time()})
        else:
            sess = Session(self, payload.get("title"), payload.get("languageHint"), options, scen, payload)
            sess.created_at_iso = time.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.sessions[sess.id] = sess
            self.idempotency[key] = (sess.id, fingerprint)
            sess.rest_calls.append({"method": "POST", "path": "create", "idempotencyKey": key,
                                    "hasAccessKeyHeader": True, "t": time.time()})
        if fx.get("dropCreateResponseOnce") and key not in self.dropped_create:
            self.dropped_create.add(key)
            writer.transport.abort()
            return False
        await self.respond(writer, 200, envelope_ok(sess.view()))
        return True

    async def control(self, writer, method, path, body):
        segs = [s for s in path.split("/") if s][1:]
        if segs == ["sessions"]:
            await self.respond(writer, 200, [s.record() for s in self.sessions.values()])
        elif len(segs) == 2 and segs[0] == "sessions" and segs[1] in self.sessions:
            await self.respond(writer, 200, self.sessions[segs[1]].record())
        elif len(segs) == 2 and segs[0] == "by-title":
            found = [s.record() for s in self.sessions.values() if s.title == segs[1]]
            await self.respond(writer, 200, found)
        elif len(segs) == 3 and segs[0] == "sessions" and segs[2] == "rest-up" and segs[1] in self.sessions:
            self.sessions[segs[1]].rest_down = False
            await self.respond(writer, 200, {"ok": True})
        elif segs == ["proxy"]:
            await self.respond(writer, 200, {"tunnels": self.proxy_tunnels, "targets": self.proxy_targets})
        elif segs == ["health"]:
            await self.respond(writer, 200, {"ok": True})
        else:
            await self.respond(writer, 404, {"error": "unknown control"})
        return True

    # ---------------- WebSocket
    async def websocket(self, reader, writer, target, headers):
        path, _, query = target.partition("?")
        prefix, suffix = REALTIME + "/sessions/", "/stream"
        sid = path[len(prefix):-len(suffix)] if path.startswith(prefix) and path.endswith(suffix) else None
        sess = self.sessions.get(sid)
        ok_key = headers.get("accesskey") == VALID_KEY
        ticket = None
        if "ticket=" in query:
            ticket = query.split("ticket=", 1)[1].split("&")[0]
        ok_ticket = ticket is not None and self.tickets.pop(ticket, None) == sid
        if sess is None or not (ok_key or ok_ticket) or (sess and sess.fx.get("rejectHandshake")):
            writer.write(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
            return
        accept = base64.b64encode(hashlib.sha1((headers["sec-websocket-key"] + GUID).encode()).digest()).decode()
        writer.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                      "Sec-WebSocket-Accept: %s\r\n\r\n" % accept).encode())
        await writer.drain()
        record = {"hasAccessKeyHeader": "accesskey" in headers, "queryHasKey": VALID_KEY in query,
                  "query": query, "pings": 0, "events": [], "openedAt": time.time(), "closedAt": None,
                  "userAgent": headers.get("user-agent")}
        sess.connections.append(record)
        conn = Connection(sess, writer, record)
        sess.socket = conn
        await conn.emit("session.ready", sess.view())
        if sess.fx.get("readyErrorCode"):
            await conn.emit("session.error", {"errorCode": sess.fx["readyErrorCode"]})
        try:
            buffer = b""
            while not conn.closed:
                fin, opcode, data = await ws_read_frame(reader)
                if opcode == 0x9:
                    record["pings"] += 1
                    async with conn.send_lock:
                        writer.write(ws_frame(0xA, data))
                        await writer.drain()
                    continue
                if opcode == 0xA:
                    continue
                if opcode == 0x8:
                    async with conn.send_lock:
                        try:
                            writer.write(ws_frame(0x8, data[:2]))
                            await writer.drain()
                        except Exception:
                            pass
                    break
                buffer += data
                if not fin:
                    continue
                text, buffer = buffer, b""
                await self.on_message(sess, conn, text)
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            conn.closed = True
            record["closedAt"] = time.time()
            if sess.socket is conn:
                sess.socket = None
            # The real service pauses a still-recording session when its socket detaches.
            async with sess.lock:
                if sess.state == "RECORDING":
                    sess.pause_epoch([])
            if sess.fx.get("finishOnDisconnect") and sess.state != "CLOSED":
                async with sess.lock:
                    sess.finish([])
            if sess.fx.get("restDownAfterDisconnect") and sess.state != "CLOSED":
                sess.rest_down = True
            try:
                writer.close()
            except Exception:
                pass

    async def on_message(self, sess, conn, text):
        fx = sess.fx
        try:
            node = json.loads(text)
            et = node.get("eventType", "")
            payload = node.get("payload") or {}
        except (ValueError, AttributeError):
            await conn.emit("session.error", {"errorCode": "INPUT_INVALID"})
            return
        events = []
        rotate = False
        error_code = None
        reply_usage = True
        try:
            async with sess.lock:
                if et == "audio.append":
                    try:
                        pcm = base64.b64decode(payload.get("audioBase64", ""), validate=True)
                    except Exception:
                        raise ApiError("INPUT_INVALID")
                    sess.frame_counter += 1
                    n = sess.frame_counter
                    try:
                        epoch = int(node.get("epoch") or 0) or sess.active["index"]
                        offset = int(payload.get("sampleOffset", 0))
                    except (TypeError, ValueError):
                        raise ApiError("INPUT_INVALID")
                    frame = {"n": n, "epoch": epoch, "sampleOffset": offset,
                             "sampleOffsetType": type(payload.get("sampleOffset")).__name__,
                             "capturedType": type(payload.get("capturedSamples")).__name__,
                             "bytes": len(pcm), "captured": payload.get("capturedSamples"), "t": time.time(),
                             "accepted": False, "precedingText": payload.get("precedingText")}
                    sess.frames.append(frame)
                    sess.claim(payload.get("capturedSamples"))
                    e = sess.active
                    if sess.state == "RECORDING" and epoch == e["index"] and e["state"] == "OPEN":
                        e["recv"] = e.get("recv", 0) + 1
                    if sess.state != "RECORDING" or epoch != e["index"] or e["state"] != "OPEN":
                        pass
                    elif e["rotate_due"] or self.rotate_now(fx, e):
                        e["rotate_due"] = True
                        rotate = True
                    elif len(pcm) == 0 or (sess.bps == 2 and len(pcm) % 2):
                        raise ApiError("INPUT_INVALID")
                    elif offset != e["sent"]:
                        frame["ignored"] = "offset"
                    elif str(n) in (fx.get("errorAtFrames") or {}) and n not in sess.error_frames_done:
                        sess.error_frames_done.add(n)
                        raise ApiError(fx["errorAtFrames"][str(n)])
                    elif n == fx.get("insufficientAtFrame"):
                        sess.pause_epoch(events)
                        error_code = "INSUFFICIENT_CHARACTERS"
                    else:
                        samples = len(pcm) // sess.bps
                        sess.stored += pcm
                        e["sent"] += samples
                        e["frames"] += 1
                        frame["accepted"] = True
                        if sess.options["segmentCommitMode"] == "vad":
                            every = fx.get("vadEveryFrames", 5)
                            if e["frames"] % every == 0:
                                sess.commit_open(events)
                        if e["frames"] % 3 == 1:
                            sess.partial = "partial %d" % e["sent"]
                            events.append(("transcript.delta", {"text": sess.partial}))
                    if n in fx.get("dropAckFrames", []) or fx.get("noAck"):
                        reply_usage = False
                    if fx.get("disconnectAfterFrames") and n >= fx["disconnectAfterFrames"]:
                        conn.abort()
                        return
                elif et == "segment.commit":
                    sess.claim(payload.get("capturedSamples"))
                    if sess.state == "RECORDING":
                        sess.commit_open(events)
                elif et == "session.pause":
                    sess.claim(payload.get("capturedSamples"))
                    sess.pause_epoch(events)
                elif et == "session.resume":
                    sess.claim(payload.get("capturedSamples"))
                    old = sess.active
                    sess.resume(events)
                    if fx.get("lateOldEpochUsage") and sess.active is not old:
                        late = sess.status_payload()
                        late["epoch"] = old["index"]
                        late["sentSamples"] = str(max(0, old["sent"] - 1))
                        late["accepting"] = False
                        await conn.emit_all(events)
                        await conn.emit("usage.updated", sess.status_payload())
                        await conn.emit("usage.updated", late)
                        return
                elif et == "session.finish":
                    sess.claim(payload.get("capturedSamples"))
                    if fx.get("disconnectOnFinish"):
                        conn.abort()
                        return
                    fdelay = fx.get("finishDelayMs", 0)
                    if fdelay:
                        await asyncio.sleep(fdelay / 1000)
                    sess.finish(events)
                    reply_usage = False
                    await conn.emit_all(events)
                    if fx.get("closeAfterCompletedWithoutEvent"):
                        conn.abort()
                        return
                    await conn.emit("session.completed", sess.view())
                    if fx.get("closeAfterCompleted"):
                        # transport dies right after the terminal event was delivered (an immediate RST
                        # could discard the event in the peer's receive buffer on some OSes; that case is
                        # closeAfterCompletedWithoutEvent)
                        await conn.writer.drain()
                        await asyncio.sleep(0.2)
                        conn.abort()
                    return
                else:
                    raise ApiError("INPUT_INVALID")
        except ApiError as err:
            await conn.emit_all(events)
            await conn.emit("session.error", {"errorCode": err.code})
            return
        delay = fx.get("ackDelayMs", 0)
        if delay:
            await asyncio.sleep(delay / 1000)
        await conn.emit_all(events)
        if reply_usage:
            await conn.emit("usage.updated", sess.status_payload(rotate, error_code))

    @staticmethod
    def rotate_now(fx, epoch_row):
        for item in fx.get("rotateAt", []):
            if item["epoch"] == epoch_row["index"] and item["frame"] == epoch_row.get("recv"):
                return True
        return False

    # ---------------- CONNECT proxy
    async def proxy(self, reader, writer):
        try:
            line = await reader.readline()
            parts = line.decode("latin-1").split(" ")
            while True:
                h = await reader.readline()
                if h in (b"\r\n", b"\n", b""):
                    break
            if parts[0] != "CONNECT":
                writer.write(b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n")
                await writer.drain()
                writer.close()
                return
            host, _, port = parts[1].rpartition(":")
            self.proxy_tunnels += 1
            self.proxy_targets.append(parts[1])
            up_r, up_w = await asyncio.open_connection(host.strip("[]"), int(port))
            writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
            await writer.drain()

            async def pipe(r, w):
                try:
                    while True:
                        data = await r.read(65536)
                        if not data:
                            break
                        w.write(data)
                        await w.drain()
                except Exception:
                    pass
                finally:
                    try:
                        w.close()
                    except Exception:
                        pass
            await asyncio.gather(pipe(reader, up_w), pipe(up_r, writer))
        except Exception:
            try:
                writer.close()
            except Exception:
                pass


def load_scenarios(path):
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    return {s["id"]: s for s in doc["scenarios"]}


async def main():
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.abspath(os.path.join(here, "..", ".."))
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--tls", action="store_true")
    ap.add_argument("--proxy", action="store_true")
    ap.add_argument("--scenarios", default=os.path.join(root, "fixtures", "scenarios.json"))
    args = ap.parse_args()
    fixture = Fixture(load_scenarios(args.scenarios))
    ctx = None
    if args.tls:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(os.path.join(root, "fixtures", "tls", "server.crt"),
                            os.path.join(root, "fixtures", "tls", "server.key"))
    server = await asyncio.start_server(fixture.handle, args.host, args.port, ssl=ctx)
    port = server.sockets[0].getsockname()[1]
    proxy_port = None
    if args.proxy:
        psrv = await asyncio.start_server(fixture.proxy, args.host, 0)
        proxy_port = psrv.sockets[0].getsockname()[1]
    print(json.dumps({"http": port, "proxy": proxy_port, "tls": bool(args.tls)}), flush=True)
    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
