#!/usr/bin/env python3
"""R6 / A16: per-ID terminal state and ledger read-back for every live test session (PARTIAL and
FAILED included), repeated finish without a second charge, and balance-window reconciliation.

    MYVOCAL_ACCESS_KEY=... python tools/live/reconcile.py --out <dir>

1. Collects every transcription/session id from <out>/live-*.json (error records with only a
   sessionId are resolved through GET session).
2. Scans the shared History (GET /stt/transcriptions, newest first) for every item created since
   <out>/balance-start.json, so a session that a harness failed to record is still reconciled.
3. Waits until no row is still reserved, then reads the balance (end of test window).
4. For every row: GET session (realtime) and GET transcription (History + billing).
5. Repeats finish (REST) for every session, reads billing again, then reads the balance again: a
   repeated finish must not change any settled amount or the balance.
6. Compares balance(start) - balance(end) with the sum of settled Characters of the window.
Writes <out>/reconcile.json; only ids and numbers, no keys, tickets or transcript text.
"""
import argparse
import datetime as dt
import glob
import json
import os
import re
import sys
import time

import httpx

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from live_common import env  # noqa: E402
from balance import read_balance  # noqa: E402


def unwrap(resp):
    try:
        body = resp.json()
    except ValueError:
        return None, "HTTP_%d" % resp.status_code
    if body.get("code") != 1:
        return None, (body.get("data") or {}).get("errorCode") or body.get("code")
    return body.get("data") or {}, None


def as_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def parse_ts(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v / 1000.0 if v > 1e11 else float(v)
    s = str(v).strip().replace("Z", "+00:00")
    if len(s) >= 5 and s[-5] in "+-" and s[-3] != ":":
        s = s[:-2] + ":" + s[-2:]
    try:
        d = dt.datetime.fromisoformat(s)
    except ValueError:
        return None
    if d.tzinfo is None:  # server-local wall clock; the account is operated in Asia/Shanghai
        d = d.replace(tzinfo=dt.timezone(dt.timedelta(hours=8)))
    return d.timestamp()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--no-refinish", action="store_true")
    ap.add_argument("--history-pages", type=int, default=20)
    a = ap.parse_args()
    key, base = env()
    rt = base + "/sound_clone/api/v1/stt/realtime"
    stt = base + "/sound_clone/api/v1/stt"
    start = None
    sp = os.path.join(a.out, "balance-start.json")
    if os.path.exists(sp):
        with open(sp, encoding="utf-8-sig") as f:
            start = json.load(f)
    rows, by_tid, pending_sids = [], {}, []
    for path in sorted(glob.glob(os.path.join(a.out, "live-*.json"))):
        with open(path, encoding="utf-8-sig") as f:
            doc = json.load(f)
        for r in doc.get("records", []) + doc.get("superseded", []):
            tid = r.get("transcriptionId")
            if tid and tid not in by_tid:
                row = {"language": doc.get("language"), "scenario": r["scenario"], "sessionId": r.get("sessionId"),
                       "transcriptionId": tid, "sdkStatus": r.get("status"), "sdkBillable": r.get("billableCharacters"),
                       "harnessOk": r.get("ok")}
                by_tid[tid] = row
                rows.append(row)
            elif not tid:
                sid = r.get("sessionId")
                if not sid:  # SDK errors name the session in their message
                    m = re.search(r"\b(rts_[0-9a-f]+)", str(r.get("message") or ""))
                    sid = m.group(1) if m else None
                if sid:
                    pending_sids.append((doc.get("language"), r["scenario"], sid))
    headers = {"accessKey": key, "Accept": "application/json"}
    with httpx.Client(headers=headers, timeout=30) as http:
        for lang, scen, sid in pending_sids:
            v, err = unwrap(http.get("%s/sessions/%s" % (rt, sid)))
            tid = (v or {}).get("transcriptionId")
            if tid and tid not in by_tid:
                row = {"language": lang, "scenario": scen, "sessionId": sid, "transcriptionId": tid,
                       "sdkStatus": None, "harnessOk": False}
                by_tid[tid] = row
                rows.append(row)
        # History window scan
        history_only, scanned = [], 0
        if start and start.get("epoch"):
            t0 = start["epoch"] - 5
            for page in range(1, a.history_pages + 1):
                d, err = unwrap(http.get(stt + "/transcriptions", params={"page": page, "pageSize": 50}))
                items = (d or {}).get("items") or []
                older = 0
                for it in items:
                    scanned += 1
                    ts = parse_ts(it.get("createdAt") or it.get("createTime") or it.get("created_at"))
                    if ts is None or ts < t0:
                        older += 1
                        continue
                    tid = it.get("transcriptionId")
                    if tid and tid not in by_tid:
                        row = {"language": "history-only", "scenario": it.get("title"), "sessionId": None,
                               "transcriptionId": tid, "sdkStatus": it.get("status"), "modelId": it.get("modelId")}
                        by_tid[tid] = row
                        rows.append(row)
                        history_only.append(tid)
                if not items or older == len(items):
                    break

        # close out any session that a failed scenario left open (normal REST finish of that same
        # session; never a new task)
        closed_out = []
        for row in rows:
            if row.get("sessionId"):
                v, err = unwrap(http.get("%s/sessions/%s" % (rt, row["sessionId"])))
                if v and v.get("status") not in ("COMPLETED", "PARTIAL", "FAILED"):
                    v2, err2 = unwrap(http.post("%s/sessions/%s/finish" % (rt, row["sessionId"]), json={}))
                    row["closedOutByReconcile"] = (v2 or {}).get("status") or err2
                    closed_out.append(row["transcriptionId"])

        def read_rows():
            for row in rows:
                sid, tid = row["sessionId"], row["transcriptionId"]
                if sid:
                    v, err = unwrap(http.get("%s/sessions/%s" % (rt, sid)))
                    row["sessionStatus"] = v.get("status") if v else err
                    u = (v or {}).get("usage") or {}
                    row["sessionBillable"] = as_int(u.get("billableCharacters"))
                    row["sessionReceivedSamples"] = as_int(u.get("receivedSamples"))
                    row["sessionCapturedSamples"] = as_int(u.get("capturedSamples"))
                    row["sessionEpoch"] = (v or {}).get("epoch")
                    row["sessionLines"] = len((v or {}).get("lines") or [])
                    row["sessionNotices"] = [n.get("code") if isinstance(n, dict) else n for n in ((v or {}).get("notices") or [])]
                d, err = unwrap(http.get("%s/transcriptions/%s" % (stt, tid)))
                bill = (d or {}).get("billing") or {}
                row["historyStatus"] = d.get("status") if d else err
                e = (d or {}).get("error") or {}
                if e:
                    row["historyError"] = {k: e.get(k) for k in ("errorCode", "code", "message", "retryable") if k in e}
                row["audioDurationMs"] = as_int((d or {}).get("audioDurationMs"))
                if not row.get("sessionId") and (d or {}).get("sessionId"):
                    row["sessionId"] = d.get("sessionId")
                row["billingState"] = bill.get("state")
                for k in ("reservedCharacters", "settledCharacters", "releasedCharacters", "billableCharacters"):
                    row[k] = as_int(bill.get(k))

        for _ in range(12):  # settlement is expected to be immediate; allow up to ~60 s
            read_rows()
            if all(r.get("billingState") in ("SETTLED", "RELEASED") for r in rows):
                break
            time.sleep(5)
        end = read_balance(http, base)
        end["at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        if not a.no_refinish:
            for row in rows:
                sid, tid = row["sessionId"], row["transcriptionId"]
                if not sid:
                    continue
                v2, err2 = unwrap(http.post("%s/sessions/%s/finish" % (rt, sid), json={}))
                d2, _ = unwrap(http.get("%s/transcriptions/%s" % (stt, tid)))
                b2 = (d2 or {}).get("billing") or {}
                row["refinishStatus"] = v2.get("status") if v2 else err2
                row["refinishSameTranscription"] = bool(v2) and v2.get("transcriptionId") == tid
                row["settledAfterRefinish"] = as_int(b2.get("settledCharacters"))
                row["billableAfterRefinish"] = as_int(b2.get("billableCharacters"))
            after = read_balance(http, base)
            after["at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        else:
            after = None
    for row in rows:
        terminal_ok = row.get("billingState") in ("SETTLED", "RELEASED")
        status_ok = (not row.get("sessionId")) or row.get("sdkStatus") is None or row.get("sessionStatus") == row.get("sdkStatus")
        refinish_ok = (a.no_refinish or not row.get("sessionId")
                       or (row.get("refinishSameTranscription") and row.get("settledAfterRefinish") == row.get("settledCharacters")
                           and row.get("billableAfterRefinish") == row.get("billableCharacters")))
        row["ok"] = bool(terminal_ok and status_ok and refinish_ok)
    settled = sum(r.get("settledCharacters") or 0 for r in rows if r.get("billingState") == "SETTLED")
    status_of = lambda r: r.get("sessionStatus") or r.get("historyStatus") or r.get("sdkStatus")  # noqa: E731
    summary = {
        "rows": len(rows), "historyOnly": history_only, "closedOutByReconcile": closed_out, "historyItemsScanned": scanned,
        "byLanguage": {l: sum(1 for r in rows if r["language"] == l) for l in sorted({r["language"] for r in rows})},
        "byStatus": {s: sum(1 for r in rows if status_of(r) == s) for s in sorted({str(status_of(r)) for r in rows})},
        "reservedLeft": [r["transcriptionId"] for r in rows if r.get("billingState") not in ("SETTLED", "RELEASED")],
        "settledSum": settled, "rowsNeedingReview": [r["transcriptionId"] for r in rows if not r["ok"]],
        "balanceStart": (start or {}).get("availableCharacters"), "balanceStartAt": (start or {}).get("at"),
        "balanceEnd": end.get("availableCharacters"), "balanceEndAt": end.get("at"),
        "balanceAfterRefinish": (after or {}).get("availableCharacters"),
    }
    if summary["balanceStart"] is not None and summary["balanceEnd"] is not None:
        summary["balanceDelta"] = summary["balanceStart"] - summary["balanceEnd"]
        summary["balanceMatchesSettled"] = summary["balanceDelta"] == settled
    if after is not None and summary["balanceEnd"] is not None:
        summary["refinishChangedBalance"] = summary["balanceAfterRefinish"] != summary["balanceEnd"]
    with open(os.path.join(a.out, "reconcile.json"), "w", encoding="utf-8") as f:
        json.dump({"summary": summary, "rows": rows}, f, indent=2)
    print(json.dumps(summary, indent=2))
    ok = (not summary["rowsNeedingReview"] and not summary["reservedLeft"] and summary.get("balanceMatchesSettled", False)
          and not summary.get("refinishChangedBalance", False))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
