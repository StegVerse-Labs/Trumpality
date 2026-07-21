#!/usr/bin/env python3
"""Consume Executive Rhetoric Ledger acknowledgments without conferring acceptance authority."""
from __future__ import annotations
import base64, hashlib, json, subprocess
from datetime import datetime, timezone
from pathlib import Path

CONSUMER = "StegVerse-Labs/Executive_Rhetoric_Ledger"
PRODUCER = "StegVerse-Labs/Trumpality"
SOURCE = "producer_intake/acknowledgments/StegVerse-Labs__Trumpality"
OUT = Path("data/receipts/executive-rhetoric-ledger-acknowledgments")


def gh_list(path: str):
    raw = subprocess.check_output(["gh", "api", f"repos/{CONSUMER}/contents/{path}"], text=True)
    return json.loads(raw)


def gh_read(path: str) -> bytes:
    raw = subprocess.check_output(["gh", "api", f"repos/{CONSUMER}/contents/{path}"], text=True)
    return base64.b64decode(json.loads(raw)["content"])


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    consumed = 0
    try:
        entries = gh_list(SOURCE)
    except Exception:
        entries = []
    for entry in entries if isinstance(entries, list) else []:
        if not entry.get("name", "").endswith(".json"):
            continue
        raw = gh_read(entry["path"])
        ack = json.loads(raw)
        if ack.get("producer_repository") != PRODUCER:
            continue
        receipt = {
            "receipt_type": "erl-acknowledgment-consumption",
            "producer_repository": PRODUCER,
            "consumer_repository": CONSUMER,
            "acknowledgment_id": ack["acknowledgment_id"],
            "acknowledgment_sha256": hashlib.sha256(raw).hexdigest(),
            "consumed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "observed_state": ack["state"],
            "authority": {
                "may_confirm_observation": True,
                "may_accept_evidence": False,
                "may_classify_final": False,
                "may_promote": False,
                "may_publish": False
            }
        }
        (OUT / f'{ack["acknowledgment_id"]}.json').write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        consumed += 1
    print(f"Consumed acknowledgments: {consumed}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
