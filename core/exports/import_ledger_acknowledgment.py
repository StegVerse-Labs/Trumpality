#!/usr/bin/env python3
"""Import an Executive Rhetoric Ledger acknowledgment without mutating source records."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

EXPECTED_PRODUCER = "StegVerse-Labs/Trumpality"
EXPECTED_LEDGER = "StegVerse-Labs/Executive_Rhetoric_Ledger"
ROOT = Path(__file__).resolve().parents[2]
OBJECT_DIR = ROOT / "data" / "receipts" / "ledger_acknowledgments"
APPEND_LOG = ROOT / "data" / "receipts" / "ledger_acknowledgments.jsonl"
CURRENT_INDEX = ROOT / "data" / "receipts" / "ledger_acknowledgment_current.json"

REQUIRED = {
    "acknowledgment_id",
    "acknowledgment_kind",
    "ingestion_id",
    "producer_repo",
    "producer_path",
    "producer_commit",
    "ledger_repo",
    "decision_at",
    "review_status",
    "evidence_effect",
    "receipt_status",
    "producer_return_path",
    "required_actions",
}


def load_object(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    missing = sorted(REQUIRED - set(data))
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")
    if data["producer_repo"] != EXPECTED_PRODUCER:
        raise ValueError(f"unexpected producer_repo: {data['producer_repo']}")
    if data["ledger_repo"] != EXPECTED_LEDGER:
        raise ValueError(f"unexpected ledger_repo: {data['ledger_repo']}")
    if data["acknowledgment_kind"] in {"correction", "supersession"}:
        if not data.get("supersedes_acknowledgment_id"):
            raise ValueError("correction or supersession requires supersedes_acknowledgment_id")
    return data


def load_current_index() -> dict:
    if not CURRENT_INDEX.exists():
        return {"current_by_ingestion_id": {}}
    return json.loads(CURRENT_INDEX.read_text(encoding="utf-8"))


def append_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("acknowledgment", type=Path)
    args = parser.parse_args()

    source_path = args.acknowledgment.resolve()
    record = load_object(source_path)
    OBJECT_DIR.mkdir(parents=True, exist_ok=True)

    object_path = OBJECT_DIR / f"{record['acknowledgment_id']}.json"
    if object_path.exists():
        existing = json.loads(object_path.read_text(encoding="utf-8"))
        if existing != record:
            raise ValueError(
                f"acknowledgment_id {record['acknowledgment_id']} already exists with different content"
            )
        print(f"Acknowledgment already recorded: {object_path}")
        return 0

    index = load_current_index()
    current_map = index.setdefault("current_by_ingestion_id", {})
    current_id = current_map.get(record["ingestion_id"])
    prior_id = record.get("supersedes_acknowledgment_id")

    if prior_id:
        if current_id and current_id != prior_id:
            raise ValueError(
                f"supersedes {prior_id}, but current acknowledgment is {current_id}"
            )
        prior_path = OBJECT_DIR / f"{prior_id}.json"
        if not prior_path.exists():
            raise ValueError(f"superseded acknowledgment not found: {prior_id}")
    elif current_id:
        raise ValueError(
            f"ingestion_id {record['ingestion_id']} already has current acknowledgment {current_id}; "
            "a correction or supersession is required"
        )

    object_path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    append_jsonl(
        APPEND_LOG,
        {
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "acknowledgment_id": record["acknowledgment_id"],
            "ingestion_id": record["ingestion_id"],
            "acknowledgment_kind": record["acknowledgment_kind"],
            "review_status": record["review_status"],
            "evidence_effect": record["evidence_effect"],
            "supersedes_acknowledgment_id": prior_id,
            "object_path": str(object_path.relative_to(ROOT)),
        },
    )
    current_map[record["ingestion_id"]] = record["acknowledgment_id"]
    index["updated_at"] = datetime.now(timezone.utc).isoformat()
    CURRENT_INDEX.write_text(json.dumps(index, indent=2) + "\n", encoding="utf-8")

    print(f"Recorded acknowledgment: {object_path}")
    print("Native source records and verification labels were not modified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
