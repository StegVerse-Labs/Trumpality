#!/usr/bin/env python3
"""
AI Search Agent (Public OSINT Only)
- Reads <base>/data/sources/sources_whitelist.csv
- Scans <base>/data/master/*.csv for deep_search_*: pending
- Crawls whitelisted sources (RSS & allowed pages)
- Appends lead links into notes fields (non-destructive)
- Logs under <base>/data/logs/ai_agent/
"""

from __future__ import annotations
import csv, re, json, pathlib, hashlib, argparse, sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import List, Dict

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from core.http_fetch import fetch  # noqa: E402

USER_AGENT = "StegVerse-AI-Agent/1.0 (+public sources only)"

def normalize_spaces(s: str) -> str:
    return " ".join((s or "").split())

def hash_key(*parts) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode("utf-8", errors="ignore"))
    return h.hexdigest()[:16]

def safe_get(url: str, timeout: int = 20, content_type: str = "text/html") -> str:
    try:
        r = fetch(url, user_agent=USER_AGENT, timeout=timeout)
        ctype = {k.lower(): v for k, v in r.headers.items()}.get("content-type", "")
        if r.status_code == 200 and (not content_type or content_type in ctype):
            return r.text
        return ""
    except Exception:
        return ""

def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()

def parse_feed(xml_text: str) -> List[Dict[str, str]]:
    """Return RSS <item> / Atom <entry> objects as dicts of title, summary, link, published, tags."""
    entries = []
    root = ET.fromstring(xml_text)
    for node in root.iter():
        if _local(node.tag) not in ("item", "entry"):
            continue
        entry = {"title": "", "summary": "", "link": "", "published": "", "tags": ""}
        tags = []
        for child in node:
            name, text = _local(child.tag), (child.text or "").strip()
            if name == "title":
                entry["title"] = text
            elif name in ("description", "summary", "content") and not entry["summary"]:
                entry["summary"] = text
            elif name == "link":
                entry["link"] = entry["link"] or text or child.get("href", "")
            elif name in ("pubdate", "published", "updated") and not entry["published"]:
                entry["published"] = text
            elif name == "category":
                tags.append(text or child.get("term", ""))
        entry["tags"] = " ".join(tags)
        entries.append(entry)
    return entries

class _Anchors(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.anchors, self._href, self._text = [], None, []
    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self._href, self._text = dict(attrs).get("href"), []
    def handle_data(self, data):
        if self._href is not None:
            self._text.append(data)
    def handle_endtag(self, tag):
        if tag == "a" and self._href is not None:
            self.anchors.append((normalize_spaces(" ".join(self._text)), self._href))
            self._href = None

def read_whitelist(path: pathlib.Path) -> List[Dict[str,str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def search_rss(feeds: List[str], keywords: List[str], limit_per_feed: int = 30) -> List[Dict]:
    results = []
    kw = [k.lower() for k in keywords if k]
    for url in feeds:
        try:
            for entry in parse_feed(safe_get(url, content_type=""))[:limit_per_feed]:
                text = " ".join([entry["title"], entry["summary"], entry["tags"]]).lower()
                if all(k in text for k in kw):
                    results.append({
                        "feed": url,
                        "title": entry.get("title","").strip(),
                        "link": entry.get("link","").strip(),
                        "published": entry.get("published","").strip()
                    })
        except Exception:
            continue
    return results

def site_keyword_scan(pages: List[str], keywords: List[str], limit_per_site: int = 10) -> List[Dict]:
    out = []
    kw = [k.lower() for k in keywords if k]
    for base in pages:
        html = safe_get(base)
        if not html:
            continue
        parser = _Anchors()
        parser.feed(html)
        count = 0
        for title, href in parser.anchors:
            txt = title.lower()
            if all(k in txt for k in kw) and href.startswith("http"):
                out.append({"page": base, "title": title, "link": href})
                count += 1
                if count >= limit_per_site:
                    break
    return out

def load_csv(path: pathlib.Path) -> tuple[List[str], List[Dict[str, str]]]:
    if not path.exists():
        return [], []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)

def write_csv(path: pathlib.Path, fields: List[str], rows: List[Dict[str, str]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

def find_pending(fields: List[str], rows: List[Dict[str, str]], col: str) -> List[Dict[str, str]]:
    if col not in fields:
        return []
    return [r for r in rows if (r.get(col) or "").strip().lower() in ("", "pending")]

def mk_log(log_dir: pathlib.Path) -> pathlib.Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / f"agent_run_{ts}.jsonl"

def log_line(log_path: pathlib.Path, payload: Dict):
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")

def keywords_for_event(row: Dict[str, str]) -> List[str]:
    base = " ".join([str(row.get("event","")), str(row.get("location",""))])
    tokens = [t for t in re.split(r"[^A-Za-z0-9]+", base) if len(t) >= 3]
    return list(dict.fromkeys([t.lower() for t in tokens]))[:6]

def keywords_for_person(row: Dict[str, str]) -> List[str]:
    base = " ".join([str(row.get("person","")), str(row.get("event","")), str(row.get("location",""))])
    tokens = [t for t in re.split(r"[^A-Za-z0-9]+", base) if len(t) >= 3]
    return list(dict.fromkeys([t.lower() for t in tokens]))[:6]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=".", help="Scope base directory (term folder, case thread folder, or repo root)")
    args = ap.parse_args()

    BASE = pathlib.Path(args.base).resolve()
    DATA = BASE / "data"

    # Standard FREE-DOM-style paths
    MASTER = DATA / "master" / "master_timeline.csv"
    PEOPLE = DATA / "master" / "verified_people_events.csv"
    WHITELIST = DATA / "sources" / "sources_whitelist.csv"
    LOG_DIR = DATA / "logs" / "ai_agent"

    log_path = mk_log(LOG_DIR)
    wl = read_whitelist(WHITELIST)
    rss_feeds = [r["url"] for r in wl if (r.get("type","rss").lower() == "rss")]
    site_pages = [r["url"] for r in wl if (r.get("type","rss").lower() != "rss")]

    master_fields, master = load_csv(MASTER)
    people_fields, people = load_csv(PEOPLE)

    pending_events = find_pending(master_fields, master, "deep_search_event")
    pending_people = find_pending(people_fields, people, "deep_search_person")

    total_hits = 0

    for row in pending_events:
        kws = keywords_for_event(row)
        if not kws:
            continue
        hits = search_rss(rss_feeds, kws, 25)[:5] + site_keyword_scan(site_pages, kws, 8)[:5]
        if hits:
            total_hits += len(hits)
            notes = normalize_spaces(str(row.get("notes","")))
            append = " Leads: " + "; ".join([h["link"] for h in hits])
            if "notes" not in master_fields:
                master_fields.append("notes")
            row["notes"] = (notes + append).strip()
            log_line(log_path, {"type":"event", "base": str(BASE), "keywords": kws, "hits": hits})

    for row in pending_people:
        kws = keywords_for_person(row)
        if not kws:
            continue
        hits = search_rss(rss_feeds, kws, 25)[:5] + site_keyword_scan(site_pages, kws, 8)[:5]
        if hits:
            total_hits += len(hits)
            notes = normalize_spaces(str(row.get("deep_search_notes","")))
            append = " Leads: " + "; ".join([h["link"] for h in hits])
            if "deep_search_notes" not in people_fields:
                people_fields.append("deep_search_notes")
            row["deep_search_notes"] = (notes + append).strip()
            log_line(log_path, {"type":"person", "base": str(BASE), "keywords": kws, "hits": hits})

    if master:
        write_csv(MASTER, master_fields, master)
    if people:
        write_csv(PEOPLE, people_fields, people)

    log_line(log_path, {"summary": {"base": str(BASE), "total_hits": total_hits}})

if __name__ == "__main__":
    main()
