#!/usr/bin/env python3
"""Archive Trumpality source records and emit durable archival receipts.

This module preserves the repository's declared StegArchive boundary:
- local HTML snapshot
- SHA-256 content digest
- optional Internet Archive save request
- append-only JSONL receipt

Archive success does not upgrade the evidentiary status of a source.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests

DB_PATH = Path("data/processed/records.sqlite")
SNAPSHOT_DIR = Path("data/archive/html")
RECEIPT_PATH = Path("data/receipts/archive_receipts.jsonl")
USER_AGENT = "Trumpality-Archive/1.0"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_receipt(receipt: dict) -> None:
    RECEIPT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with RECEIPT_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(receipt, sort_keys=True) + "\n")


def ensure_columns(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(records)")}
    additions = {
        "archive_wayback": "TEXT",
        "archive_local_path": "TEXT",
        "checksum_sha256": "TEXT",
        "last_verified_at": "TEXT",
        "last_status_code": "INTEGER",
        "link_ok": "INTEGER DEFAULT 1",
    }
    for name, declaration in additions.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE records ADD COLUMN {name} {declaration}")
    conn.commit()


def request_wayback_save(url: str) -> str:
    """Request an Internet Archive save and return the submitted save URL.

    A submitted URL is a request receipt, not proof that archival completed.
    """
    save_url = f"https://web.archive.org/save/{quote(url, safe=':/?=&%#')}"
    response = requests.get(save_url, timeout=45, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return response.headers.get("Content-Location") or save_url


def archive_record(conn: sqlite3.Connection, row: sqlite3.Row) -> None:
    record_id = row["id"]
    url = row["source_url"]
    checked_at = utc_now()
    receipt = {
        "receipt_type": "source-archive",
        "record_id": record_id,
        "source_url": url,
        "checked_at": checked_at,
        "status": "failed",
        "evidence_effect": "none",
    }

    try:
        response = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
        response.raise_for_status()
        content = response.content
        digest = hashlib.sha256(content).hexdigest()
        SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
        snapshot_path = SNAPSHOT_DIR / f"{record_id}-{digest[:16]}.html"
        snapshot_path.write_bytes(content)

        wayback_ref = ""
        wayback_status = "not-requested"
        try:
            wayback_ref = request_wayback_save(url)
            wayback_status = "submitted"
        except Exception as exc:  # external archive failure must not discard local custody
            wayback_status = "request-failed"
            receipt["wayback_error"] = str(exc)[:500]

        conn.execute(
            """
            UPDATE records
               SET raw_local_path = ?, archive_local_path = ?, checksum_sha256 = ?,
                   archive_wayback = ?, last_verified_at = ?, last_status_code = ?,
                   link_ok = 1, updated_at = ?
             WHERE id = ?
            """,
            (
                str(snapshot_path),
                str(snapshot_path),
                digest,
                wayback_ref,
                checked_at,
                response.status_code,
                checked_at,
                record_id,
            ),
        )
        receipt.update(
            {
                "status": "archived-local",
                "http_status": response.status_code,
                "snapshot_path": str(snapshot_path),
                "sha256": digest,
                "wayback_status": wayback_status,
                "wayback_reference": wayback_ref,
            }
        )
    except Exception as exc:
        receipt["error"] = str(exc)[:1000]
        conn.execute(
            "UPDATE records SET last_verified_at = ?, link_ok = 0, updated_at = ? WHERE id = ?",
            (checked_at, checked_at, record_id),
        )
    finally:
        conn.commit()
        append_receipt(receipt)


def main() -> int:
    if not DB_PATH.exists():
        print("No DB yet; run weekly-ingest first.")
        return 0

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    ensure_columns(conn)
    rows = conn.execute(
        """
        SELECT id, source_url
          FROM records
         WHERE source_url IS NOT NULL AND source_url != ''
         ORDER BY created_at DESC
        """
    ).fetchall()

    for row in rows:
        archive_record(conn, row)

    conn.close()
    print(f"Archive receipts written to {RECEIPT_PATH}: {len(rows)} record(s) examined")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
