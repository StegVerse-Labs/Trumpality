import os, sqlite3, uuid, json
from datetime import datetime

DB_PATH = "data/processed/records.sqlite"

def ensure_db():
  os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
  conn = sqlite3.connect(DB_PATH)
  with open("core/schema.sql", "r") as f:
    conn.executescript(f.read())
  return conn

def insert_record(conn, rec: dict):
  rec.setdefault("id", str(uuid.uuid4()))
  now = datetime.utcnow().isoformat()
  rec.setdefault("created_at", now)
  rec.setdefault("updated_at", now)
  rec["replication_links"] = json.dumps(rec.get("replication_links", []))
  conn.execute("""
    INSERT OR REPLACE INTO records
    (id,subject,title,summary,category,topic_cluster,date_occurred,date_published,
     source_url,source_type,source_attribution,raw_local_path,verification_status,
     source_confidence_score,replication_links,tags,notes,created_at,updated_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  """, (
    rec["id"], rec["subject"], rec["title"], rec.get("summary",""),
    rec.get("category","other"), rec.get("topic_cluster",""),
    rec.get("date_occurred",""), rec.get("date_published",""),
    rec["source_url"], rec.get("source_type","html"), rec.get("source_attribution",""),
    rec.get("raw_local_path",""), rec.get("verification_status","unverified"),
    rec.get("source_confidence_score",0.0), rec["replication_links"],
    ",".join(rec.get("tags",[])), rec.get("notes",""),
    rec["created_at"], rec["updated_at"]
  ))
  conn.commit()
