import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from core.ingest_pipeline.base_ingest import ensure_db, insert_record
from core.verification import source_confidence, verification_label

RECEIPT_PATH = Path("data/receipts/ingest_receipts.jsonl")


def utc_now():
  return datetime.now(timezone.utc).isoformat()


def append_receipt(receipt):
  RECEIPT_PATH.parent.mkdir(parents=True, exist_ok=True)
  with RECEIPT_PATH.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(receipt, sort_keys=True) + "\n")


def fetch(url):
  response = requests.get(url, timeout=30, headers={"User-Agent": "Trumpality/1.0"})
  response.raise_for_status()
  return response


def summarize(text):
  soup = BeautifulSoup(text, "lxml")
  title = (soup.title.string if soup.title and soup.title.string else "").strip()[:250]
  meta = soup.find("meta", {"name": "description"}) or soup.find("meta", {"property": "og:description"})
  desc = meta["content"].strip()[:1000] if meta and meta.get("content") else ""
  return title or "Untitled", desc


def main(subject, topic_cluster, urls_file):
  conn = ensure_db()
  with open(urls_file, "r", encoding="utf-8") as handle:
    urls = [url.strip() for url in handle if url.strip() and not url.startswith("#")]

  for url in urls:
    observed_at = utc_now()
    try:
      response = fetch(url)
      title, summary = summarize(response.text)
      host = urlparse(url).netloc
      confidence = source_confidence(url)
      verification = verification_label("html", confidence)
      record_id, refreshed_existing = insert_record(conn, {
        "subject": subject,
        "title": title,
        "summary": summary,
        "category": "source_candidate",
        "topic_cluster": topic_cluster,
        "source_url": url,
        "source_type": "html",
        "source_attribution": host,
        "verification_status": verification,
        "source_confidence_score": confidence,
        "tags": ["seed", "classification_pending"],
        "notes": "Seed URL ingest; source role requires governed review",
      })
      append_receipt({
        "receipt_type": "source-ingest",
        "status": "refreshed" if refreshed_existing else "created",
        "record_id": record_id,
        "source_url": url,
        "observed_at": observed_at,
        "http_status": response.status_code,
        "verification_label": verification,
        "source_confidence_score": confidence,
        "classification_state": "pending-governed-review",
        "evidence_effect": "candidate-only",
      })
      time.sleep(1)
    except Exception as exc:
      append_receipt({
        "receipt_type": "source-ingest",
        "status": "failed",
        "source_url": url,
        "observed_at": observed_at,
        "error": str(exc)[:1000],
        "retry_posture": "retain-for-next-scheduled-cycle",
        "evidence_effect": "none",
      })
      print("skip:", url, exc)

  conn.close()


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--subject", default="Donald J. Trump")
  parser.add_argument("--topic_cluster", default="general")
  parser.add_argument("--urls", required=True)
  arguments = parser.parse_args()
  main(arguments.subject, arguments.topic_cluster, arguments.urls)
