#!/usr/bin/env python3
"""Import reviewed Executive Rhetoric Ledger projections without mutating native records."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

EXPECTED_SOURCE = "StegVerse-Labs/Executive_Rhetoric_Ledger"
EXPECTED_DESTINATION = "StegVerse-Labs/Trumpality"
EXPECTED_SCHEMA = "stegverse.executive_rhetoric_ledger.person_specific_projection.v1"
ROOT = Path(__file__).resolve().parents[2]
OBJECT_DIR = ROOT / "data" / "receipts" / "ledger_reviewed_projections"
APPEND_LOG = ROOT / "data" / "receipts" / "ledger_reviewed_projections.jsonl"
CURRENT_INDEX = ROOT / "data" / "receipts" / "ledger_reviewed_projection_current.json"


def canonical_sha256(value: dict) -> str:
    material = dict(value)
    expected = material.pop("projection_sha256", None)
    actual = hashlib.sha256(
        json.dumps(material, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    if expected != actual:
        raise ValueError("projection_sha256 mismatch")
    return actual


def load_projection(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("schema") != EXPECTED_SCHEMA:
        raise ValueError("unexpected projection schema")
    if value.get("source_repository") != EXPECTED_SOURCE:
        raise ValueError("unexpected source_repository")
    if value.get("destination_repository") != EXPECTED_DESTINATION:
        raise ValueError("unexpected destination_repository")
    if value.get("projection_status") != "reviewed-ledger-projection":
        raise ValueError("projection is not reviewed-ledger-projection")
    authority = value.get("authority") or {}
    required_false = (
        "may_include_candidates",
        "may_change_native_source_records",
        "may_change_destination_verification_labels",
        "may_establish_culpability",
        "may_claim_delivery",
        "may_claim_acknowledgment",
    )
    if authority.get("reviewed_only") is not True:
        raise ValueError("projection is not reviewed-only")
    if any(authority.get(name) is not False for name in required_false):
        raise ValueError("projection authority boundary invalid")
    entries = value.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ValueError("projection must contain at least one reviewed entry")
    for entry in entries:
        if entry.get("review_status") != "reviewed":
            raise ValueError("projection contains a non-reviewed entry")
        if not isinstance(entry.get("receipt_sha256"), str) or len(entry["receipt_sha256"]) != 64:
            raise ValueError("entry receipt_sha256 invalid")
        if not str(entry.get("receipt_path", "")).startswith("ledger_receipts/reviewed/"):
            raise ValueError("entry receipt_path is outside reviewed ledger receipts")
    canonical_sha256(value)
    return value


def append_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("projection", type=Path)
    args = parser.parse_args()

    projection = load_projection(args.projection.resolve())
    projection_id = projection["projection_id"]
    OBJECT_DIR.mkdir(parents=True, exist_ok=True)
    object_path = OBJECT_DIR / f"{projection_id}.json"

    if object_path.exists():
        existing = json.loads(object_path.read_text(encoding="utf-8"))
        if existing != projection:
            raise ValueError(f"projection_id {projection_id} already exists with different content")
        print(f"Reviewed projection already recorded: {object_path}")
        return 0

    object_path.write_text(json.dumps(projection, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    append_jsonl(
        APPEND_LOG,
        {
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "projection_id": projection_id,
            "projection_sha256": projection["projection_sha256"],
            "source_repository": projection["source_repository"],
            "destination_repository": projection["destination_repository"],
            "entry_ids": [entry["entry_id"] for entry in projection["entries"]],
            "object_path": str(object_path.relative_to(ROOT)),
            "native_records_mutated": False,
            "verification_labels_changed": False,
            "acknowledgment_status": "recorded-not-returned",
        },
    )
    current = {
        "schema": "trumpality.ledger_reviewed_projection_current.v1",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "projection_id": projection_id,
        "projection_sha256": projection["projection_sha256"],
        "object_path": str(object_path.relative_to(ROOT)),
        "entry_count": len(projection["entries"]),
        "native_records_mutated": False,
        "verification_labels_changed": False,
        "projection_is_factual_truth_authority": False,
    }
    CURRENT_INDEX.write_text(json.dumps(current, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Recorded reviewed ledger projection: {object_path}")
    print("Native source records and verification labels were not modified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
