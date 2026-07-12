import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone

DB_PATH = "data/processed/records.sqlite"


def utc_now():
  return datetime.now(timezone.utc).isoformat()


def ensure_db():
  os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
  conn = sqlite3.connect(DB_PATH)
  with open("core/schema.sql", "r", encoding="utf-8") as f:
    conn.executescript(f.read())
  return conn


def insert_record(conn, rec: dict):
  """Insert or refresh a record while preserving one identity per source URL.

  URL-level deduplication prevents each scheduled scan from creating a new UUID
  for the same source. It does not assert that two different URLs contain the
  same claim or that one URL contains only one claim.
  """
  source_url = rec["source_url"]
  existing = conn.execute(
    "SELECT id, created_at FROM records WHERE source_url = ? ORDER BY created_at ASC LIMIT 1",
    (source_url,),
  ).fetchone()

  now = utc_now()
  if existing:
    rec.setdefault("id", existing[0])
    rec.setdefault("created_at", existing[1] or now)
  else:
    rec.setdefault("id", str(uuid.uuid4()))
    rec.setdefault("created_at", now)
  rec["updated_at"] = now
  rec["replication_links"] = json.dumps(rec.get("replication_links", []))

  conn.execute("""
    INSERT OR REPLACE INTO records
    (id,subject,title,summary,category,topic_cluster,date_occurred,date_published,
     source_url,source_type,source_attribution,raw_local_path,verification_status,
     source_confidence_score,replication_links,tags,notes,created_at,updated_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  """, (
    rec["id"], rec["subject"], rec["title"], rec.get("summary", ""),
    rec.get("category", "other"), rec.get("topic_cluster", ""),
    rec.get("date_occurred", ""), rec.get("date_published", ""),
    source_url, rec.get("source_type", "html"), rec.get("source_attribution", ""),
    rec.get("raw_local_path", ""), rec.get("verification_status", "unverified"),
    rec.get("source_confidence_score", 0.0), rec["replication_links"],
    ",".join(rec.get("tags", [])), rec.get("notes", ""),
    rec["created_at"], rec["updated_at"]
  ))
  conn.commit()
  return rec["id"], bool(existing)
