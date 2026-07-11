import sqlite3, json, datetime, math, hashlib, os
from urllib.parse import urlparse

DB = "data/processed/records.sqlite"
REPORT = "data/processed/cooccurrence_report.csv"

WINDOW_HOURS = 6  # +/- 3 hours around an anchor time

def isoparse(s):
    try:
        return datetime.datetime.fromisoformat(s.replace("Z","+00:00")).replace(tzinfo=None)
    except Exception:
        return None

def domain(u):
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return ""

def norm_place(s):
    if not s: return ""
    return " ".join(s.strip().lower().split())[:160]

def score_window(domains, verified_count, signals_count):
    # simple heuristic: independent domains and verified items matter most
    dscore = min(len(set(domains))*0.15, 0.45)
    vscore = min(verified_count*0.2, 0.4)
    sscore = min(signals_count*0.05, 0.15)
    return round(min(1.0, dscore + vscore + sscore), 3)

def main():
    if not os.path.exists(DB):
        print("No DB found; run weekly-ingest first."); return
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    # Pull recent records (limit for speed)
    recs = conn.execute("""
      SELECT id, subject, title, summary, topic_cluster, source_url,
             verification_status, date_occurred, date_published, notes, tags
      FROM records
      ORDER BY created_at DESC LIMIT 500
    """).fetchall()

    # Optional: pull quarantine (if any)
    try:
        qu = conn.execute("SELECT id, claim, extracted_entities, time_hint, place_hint, source_url, evidence, status FROM quarantine ORDER BY created_at DESC LIMIT 300").fetchall()
    except Exception:
        qu = []

    # Build time-place bins
    bins = []  # each bin: dict(window_start, window_end, place_id, items[])
    for r in recs:
        t = isoparse(r["date_occurred"] or r["date_published"] or "")
        if not t: continue
        # simple window edges
        ws = t - datetime.timedelta(hours=WINDOW_HOURS/2)
        we = t + datetime.timedelta(hours=WINDOW_HOURS/2)
        place = ""  # placeholder; upgrade later to venue/city (from notes/tags if present)
        item = {"id": r["id"], "url": r["source_url"], "ver": r["verification_status"], "place": place}
        # try to merge into an existing compatible bin (same place and overlapping window)
        merged = False
        for b in bins:
            if b["place_id"] == place and not (we < b["window_start"] or ws > b["window_end"]):
                # expand window to include this time
                if ws < b["window_start"]: b["window_start"] = ws
                if we > b["window_end"]: b["window_end"] = we
                b["items"].append(item); merged = True; break
        if not merged:
            bins.append({"window_start": ws, "window_end": we, "place_id": place, "items": [item]})

    # Incorporate quarantine items with time/place hints
    for r in qu:
        t = isoparse(r["time_hint"] or "")
        if not t: continue
        ws = t - datetime.timedelta(hours=WINDOW_HOURS/2)
        we = t + datetime.timedelta(hours=WINDOW_HOURS/2)
        place = norm_place(r["place_hint"] or "")
        item = {"id": f"Q:{r['id']}", "url": r["source_url"], "ver": "unverified", "place": place}
        merged = False
        for b in bins:
            same_place = (b["place_id"] == place) or (b["place_id"] == "" and place == "")
            if same_place and not (we < b["window_start"] or ws > b["window_end"]):
                if ws < b["window_start"]: b["window_start"] = ws
                if we > b["window_end"]: b["window_end"] = we
                b["items"].append(item); merged = True; break
        if not merged:
            bins.append({"window_start": ws, "window_end": we, "place_id": place, "items": [item]})

    # Compute strengths and persist
    # Ensure table
    conn.executescript(open("core/cooccur/schema_cooccurrence.sql").read())

    outf = []
    for b in bins:
        if len(b["items"]) < 2: continue  # need at least two
        domains = [domain(i["url"]) for i in b["items"] if i["url"]]
        verified_count = sum(1 for i in b["items"] if i["ver"] in ("verified_primary","corroborated_secondary"))
        signals_count = 0  # placeholder for future pHash/OCR signals
        strength = score_window(domains, verified_count, signals_count)
        co_id = hashlib.sha1((str(b["window_start"])+str(b["window_end"])+b["place_id"]+",".join(sorted(set(domains)))).encode()).hexdigest()[:16]
        urls = [i["url"] for i in b["items"] if i["url"]]
        ents = []  # optional: infer from subject/tags later
        conn.execute("""INSERT OR REPLACE INTO cooccurrence (id, window_start, window_end, place_id, signals, entities, source_urls, strength)
                        VALUES (?,?,?,?,?,?,?,?)""",
                     (co_id, b["window_start"].isoformat(), b["window_end"].isoformat(), b["place_id"],
                      json.dumps([]), json.dumps(ents), json.dumps(urls), strength))
        outf.append((co_id, b["window_start"].isoformat(), b["window_end"].isoformat(), b["place_id"], strength, len(set(domains)), verified_count, len(urls)))

    conn.commit()
    conn.close()

    # Write human-readable report
    os.makedirs("data/processed", exist_ok=True)
    with open(REPORT, "w", encoding="utf-8") as f:
        f.write("id,window_start,window_end,place_id,strength,unique_domains,verified_count,num_items\n")
        for row in outf:
            f.write(",".join(map(str,row)) + "\n")
    print(f"Co-occurrence windows: {len(outf)} written to {REPORT}")

if __name__ == "__main__":
    main()
