#!/usr/bin/env python3
"""Generate a commit/path/SHA-256-bound candidate export manifest."""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

EXPORT_ROOT = Path("datasets/exports/executive-rhetoric-ledger")
MANIFEST = EXPORT_ROOT / "manifest.json"
REPOSITORY = "StegVerse-Labs/Trumpality"


def commit_sha() -> str:
    value = os.environ.get("GITHUB_SHA", "").strip()
    if value:
        return value
    return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()


def main() -> int:
    EXPORT_ROOT.mkdir(parents=True, exist_ok=True)
    records = []
    for path in sorted(EXPORT_ROOT.glob("*.json")):
        if path == MANIFEST:
            continue
        raw = path.read_bytes()
        document = json.loads(raw)
        records.append({
            "path": path.as_posix(),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "ingestion_id": document.get("ingestion_id"),
            "producer_commit": document.get("producer_commit"),
            "record_updated_at": document.get("record_updated_at") or document.get("ingestion_date"),
            "supersedes": document.get("supersedes", []),
            "corrects": document.get("corrects", []),
            "authority": {"candidate_only": True, "may_promote": False}
        })
    manifest = {
        "manifest_version": "1.0.0",
        "producer_repository": REPOSITORY,
        "producer_commit": commit_sha(),
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "records": records,
        "authority": {
            "candidate_only": True,
            "may_claim_truth": False,
            "may_classify_final": False,
            "may_promote": False,
            "requires_ledger_review": True
        }
    }
    MANIFEST.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Manifest records: {len(records)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
