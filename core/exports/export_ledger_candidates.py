#!/usr/bin/env python3
"""Export Trumpality records as pending Executive Rhetoric Ledger candidates.

The export is deliberately conservative:
- context-only
- pending review
- unknown source type/proximity unless independently classified later
- no evidentiary upgrade from repository origin, confidence score, archive status,
  or co-occurrence strength
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path("data/processed/records.sqlite")
EXPORT_DIR = Path("datasets/exports/executive-rhetoric-ledger")
RECEIPT_PATH = Path("data/receipts/export_receipts.jsonl")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def current_commit() -> str:
    env_sha = os.environ.get("GITHUB_SHA", "").strip()
    if env_sha:
        return env_sha
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
        ).strip()
    except Exception:
        return "unresolved-local-commit"


def confidence_bucket(score: float | None) -> str:
    value = float(score or 0.0)
    if value >= 0.8:
        return "high"
    if value >= 0.55:
        return "medium"
    if value > 0:
        return "low"
    return "unknown"


def verification_status(native: str | None) -> str:
    mapping = {
        "verified_primary": "verified-primary",
        "corroborated_secondary": "verified-secondary",
        "partially_verified": "partially-verified",
        "unverified": "unverified",
    }
    return mapping.get((native or "").strip(), "unverified")


def append_receipt(receipt: dict) -> None:
    RECEIPT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with RECEIPT_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(receipt, sort_keys=True) + "\n")


def export_row(row: sqlite3.Row, producer_commit: str) -> Path:
    native_id = row["id"]
    ingestion_id = "TRUMPALITY-" + hashlib.sha256(
        f"{native_id}|{row['updated_at']}|{producer_commit}".encode("utf-8")
    ).hexdigest()[:20]
    export = {
        "ingestion_id": ingestion_id,
        "producer_repo": "StegVerse-Labs/Trumpality",
        "producer_path": f"data/processed/records.sqlite#records/{native_id}",
        "producer_commit": producer_commit,
        "ingestion_date": utc_now(),
        "object_class": "source_receipt",
        "related_topic_id": "donald-j-trump-general",
        "related_topic_name": "Donald J. Trump — general monitored sources",
        "source_receipts": [
            {
                "source_id": f"trumpality:{native_id}",
                "title": row["title"] or "Untitled",
                "url": row["source_url"] or "",
                "archive_url": row["archive_wayback"] or "",
                "access_date": row["last_verified_at"] or row["updated_at"] or "",
                "publication_date": row["date_published"] or "",
                "source_type": "unknown-source-type",
                "institutional_proximity": "unknown-proximity",
                "evidence_role": "context-only-source",
                "verification_status": verification_status(row["verification_status"]),
                "admissibility_use": "admissible-for-context-only",
                "confidence": confidence_bucket(row["source_confidence_score"]),
                "red_flags": ["archive-needed"] if not row["archive_local_path"] else [],
                "notes": (
                    "Native Trumpality source candidate. Source role, institutional proximity, "
                    "claim relationship, and factual use require independent ledger review."
                ),
            }
        ],
        "content_summary": row["summary"] or row["title"] or "",
        "ledger_relevance": (
            "Candidate contextual source concerning the monitored public figure; no claim, "
            "action, causation, or influence relationship is asserted by this export."
        ),
        "claimed_use": "context-only",
        "admissibility_request": "context-only",
        "review_status": "pending",
        "notes": (
            f"native_record_id={native_id}; native_category={row['category'] or ''}; "
            f"native_verification={row['verification_status'] or ''}; "
            "export does not confer evidentiary standing"
        ),
    }

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    destination = EXPORT_DIR / f"{ingestion_id}.json"
    destination.write_text(json.dumps(export, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    append_receipt(
        {
            "receipt_type": "ledger-producer-export",
            "ingestion_id": ingestion_id,
            "native_record_id": native_id,
            "producer_commit": producer_commit,
            "export_path": str(destination),
            "created_at": export["ingestion_date"],
            "status": "candidate-emitted",
            "acknowledgment_status": "not-received",
            "evidence_effect": "none-until-ledger-review",
        }
    )
    return destination


def ensure_archive_columns(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(records)")}
    additions = {
        "archive_wayback": "TEXT",
        "archive_local_path": "TEXT",
        "last_verified_at": "TEXT",
    }
    for name, declaration in additions.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE records ADD COLUMN {name} {declaration}")
    conn.commit()


def main() -> int:
    if not DB_PATH.exists():
        print("No DB yet; run weekly-ingest first.")
        return 0

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    ensure_archive_columns(conn)
    rows = conn.execute(
        """
        SELECT id, title, summary, category, source_url, verification_status,
               source_confidence_score, date_published, created_at, updated_at,
               archive_wayback, archive_local_path, last_verified_at
          FROM records
         WHERE source_url IS NOT NULL AND source_url != ''
         ORDER BY updated_at DESC
        """
    ).fetchall()

    producer_commit = current_commit()
    destinations = [export_row(row, producer_commit) for row in rows]
    conn.close()
    print(f"Ledger producer candidates emitted: {len(destinations)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
