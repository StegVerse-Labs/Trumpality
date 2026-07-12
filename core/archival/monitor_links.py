#!/usr/bin/env python3
"""Monitor source URL health and emit append-only link-health receipts."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import requests

DB_PATH = Path("data/processed/records.sqlite")
RECEIPT_PATH = Path("data/receipts/link_health_receipts.jsonl")
USER_AGENT = "Trumpality-LinkMonitor/1.0"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_receipt(receipt: dict) -> None:
    RECEIPT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with RECEIPT_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(receipt, sort_keys=True) + "\n")


def ensure_columns(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(records)")}
    additions = {
        "last_verified_at": "TEXT",
        "last_status_code": "INTEGER",
        "link_ok": "INTEGER DEFAULT 1",
    }
    for name, declaration in additions.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE records ADD COLUMN {name} {declaration}")
    conn.commit()


def check_url(url: str) -> tuple[int | None, bool, str]:
    try:
        response = requests.head(
            url,
            timeout=20,
            allow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        )
        if response.status_code in {403, 405}:
            response = requests.get(
                url,
                timeout=25,
                allow_redirects=True,
                stream=True,
                headers={"User-Agent": USER_AGENT},
            )
        return response.status_code, 200 <= response.status_code < 400, ""
    except Exception as exc:
        return None, False, str(exc)[:1000]


def main() -> int:
    if not DB_PATH.exists():
        print("No DB yet; run weekly-ingest first.")
        return 0

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    ensure_columns(conn)
    rows = conn.execute(
        "SELECT id, source_url FROM records WHERE source_url IS NOT NULL AND source_url != '' ORDER BY created_at DESC"
    ).fetchall()

    for row in rows:
        checked_at = utc_now()
        status_code, link_ok, error = check_url(row["source_url"])
        conn.execute(
            """
            UPDATE records
               SET last_verified_at = ?, last_status_code = ?, link_ok = ?, updated_at = ?
             WHERE id = ?
            """,
            (checked_at, status_code, 1 if link_ok else 0, checked_at, row["id"]),
        )
        receipt = {
            "receipt_type": "link-health",
            "record_id": row["id"],
            "source_url": row["source_url"],
            "checked_at": checked_at,
            "http_status": status_code,
            "link_ok": link_ok,
            "evidence_effect": "availability-only",
        }
        if error:
            receipt["error"] = error
        append_receipt(receipt)

    conn.commit()
    conn.close()
    print(f"Link-health receipts written to {RECEIPT_PATH}: {len(rows)} record(s) examined")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
