import argparse, requests, time
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from core.ingest_pipeline.base_ingest import ensure_db, insert_record
from core.verification import source_confidence, verification_label

def fetch(url):
  r = requests.get(url, timeout=30, headers={"User-Agent":"Trumpality/1.0"})
  r.raise_for_status(); return r

def summarize(text):
  soup = BeautifulSoup(text, "lxml")
  title = (soup.title.string if soup.title else "").strip()[:250]
  meta = soup.find("meta", {"name":"description"}) or soup.find("meta", {"property":"og:description"})
  desc = meta["content"].strip()[:1000] if meta and meta.get("content") else ""
  return title or "Untitled", desc

def main(subject, topic_cluster, urls_file):
  conn = ensure_db()
  with open(urls_file, "r", encoding="utf-8") as f:
    urls = [u.strip() for u in f if u.strip() and not u.startswith("#")]
  for url in urls:
    try:
      res = fetch(url)
      title, summary = summarize(res.text)
      host = urlparse(url).netloc
      conf = source_confidence(url)
      ver = verification_label("html", conf)
      insert_record(conn, {
        "subject": subject, "title": title, "summary": summary,
        "category": "investigative_report", "topic_cluster": topic_cluster,
        "source_url": url, "source_type": "html", "source_attribution": host,
        "verification_status": ver, "source_confidence_score": conf,
        "tags": ["seed"], "notes": "Seed URL ingest"
      })
      time.sleep(1)
    except Exception as e:
      print("skip:", url, e)

if __name__ == "__main__":
  ap = argparse.ArgumentParser()
  ap.add_argument("--subject", default="Donald J. Trump")
  ap.add_argument("--topic_cluster", default="general")
  ap.add_argument("--urls", required=True)
  args = ap.parse_args()
  main(args.subject, args.topic_cluster, args.urls)
