#!/usr/bin/env python3
"""Read the account's available Characters through the existing read-only STT capabilities endpoint
(GET /sound_clone/api/v1/stt/capabilities -> data.account.availableCharacters) and write
<out>/balance-<label>.json. Only the number, plan key and a timestamp are written.

    python tools/live/balance.py --out <dir> --label start
"""
import argparse
import json
import os
import sys
import time

import httpx

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from live_common import env  # noqa: E402


def read_balance(http, base):
    body = http.get(base + "/sound_clone/api/v1/stt/capabilities").json()
    if body.get("code") != 1:
        return {"error": (body.get("data") or {}).get("errorCode") or body.get("code")}
    acc = (body.get("data") or {}).get("account") or {}
    v = acc.get("availableCharacters")
    return {"availableCharacters": int(v) if v is not None else None, "accountState": acc.get("state"),
            "planKey": acc.get("planKey"), "ratePerMinute": acc.get("ratePerMinute")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--label", required=True)
    a = ap.parse_args()
    key, base = env()
    with httpx.Client(headers={"accessKey": key, "Accept": "application/json"}, timeout=30) as http:
        doc = read_balance(http, base)
    doc["label"], doc["at"], doc["epoch"] = a.label, time.strftime("%Y-%m-%dT%H:%M:%S%z"), time.time()
    os.makedirs(a.out, exist_ok=True)
    with open(os.path.join(a.out, "balance-%s.json" % a.label), "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
    print("balance %s: %s (%s)" % (a.label, doc.get("availableCharacters"), doc.get("accountState") or doc.get("error")))
    return 0 if doc.get("availableCharacters") is not None else 1


if __name__ == "__main__":
    sys.exit(main())
