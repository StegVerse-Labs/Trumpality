#!/usr/bin/env python3
"""Consume ERL acknowledgments and reviewed projections without conferring acceptance authority."""
from __future__ import annotations

import base64
import hashlib
import json
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

CONSUMER = "StegVerse-Labs/Executive_Rhetoric_Ledger"
PRODUCER = "StegVerse-Labs/Trumpality"
ACK_SOURCE = "producer_intake/acknowledgments/StegVerse-Labs__Trumpality"
PROJECTION_SOURCE = "person_specific_projections/trumpality.json"
ACK_OUT = Path("data/receipts/executive-rhetoric-ledger-acknowledgments")
ROOT = Path(__file__).resolve().parents[1]
PROJECTION_IMPORTER = ROOT / "core" / "exports" / "import_reviewed_ledger_projection.py"


def gh_list(path: str):
    raw = subprocess.check_output(["gh", "api", f"repos/{CONSUMER}/contents/{path}"], text=True)
    return json.loads(raw)


def gh_read(path: str) -> bytes:
    raw = subprocess.check_output(["gh", "api", f"repos/{CONSUMER}/contents/{path}"], text=True)
    return base64.b64decode(json.loads(raw)["content"])


def consume_acknowledgments() -> int:
    ACK_OUT.mkdir(parents=True, exist_ok=True)
    consumed = 0
    try:
        entries = gh_list(ACK_SOURCE)
    except Exception:
        entries = []
    for entry in entries if isinstance(entries, list) else []:
        if not entry.get("name", "").endswith(".json"):
            continue
        raw = gh_read(entry["path"])
        acknowledgment = json.loads(raw)
        if acknowledgment.get("producer_repository") != PRODUCER:
            continue
        receipt = {
            "receipt_type": "erl-acknowledgment-consumption",
            "producer_repository": PRODUCER,
            "consumer_repository": CONSUMER,
            "acknowledgment_id": acknowledgment["acknowledgment_id"],
            "acknowledgment_sha256": hashlib.sha256(raw).hexdigest(),
            "consumed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "observed_state": acknowledgment["state"],
            "authority": {
                "may_confirm_observation": True,
                "may_accept_evidence": False,
                "may_classify_final": False,
                "may_promote": False,
                "may_publish": False,
            },
        }
        (ACK_OUT / f'{acknowledgment["acknowledgment_id"]}.json').write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        consumed += 1
    return consumed


def consume_reviewed_projection() -> str:
    try:
        raw = gh_read(PROJECTION_SOURCE)
    except Exception:
        return "not-available"
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as handle:
        handle.write(raw)
        temporary_path = Path(handle.name)
    try:
        subprocess.run(
            [sys.executable, str(PROJECTION_IMPORTER), str(temporary_path)],
            cwd=ROOT,
            check=True,
        )
    finally:
        temporary_path.unlink(missing_ok=True)
    return "recorded"


def main() -> int:
    acknowledgments = consume_acknowledgments()
    projection = consume_reviewed_projection()
    print(f"Consumed acknowledgments: {acknowledgments}")
    print(f"Reviewed projection: {projection}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
