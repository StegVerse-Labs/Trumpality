"""Read-only record queries over the native SQLite store (standard library only).

Replaces the former FastAPI server: no process listens or waits for requests.
Each query is a one-shot read that prints JSON, e.g.

  python api/app.py records --limit 20 --category source_candidate
  python api/app.py record <id>
  python api/app.py stats
"""
import argparse
import json
import sqlite3

DB = "data/processed/records.sqlite"


def rows(sql, args=()):
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    cur = con.execute(sql, args)
    r = [dict(x) for x in cur.fetchall()]
    con.close()
    return r


def get_records(limit: int = 100, category: str | None = None):
    q = "SELECT * FROM records"
    a = []
    if category:
        q += " WHERE category=?"
        a.append(category)
    q += " ORDER BY date_published DESC LIMIT ?"
    a.append(limit)
    return rows(q, tuple(a))


def get_record(rid: str):
    r = rows("SELECT * FROM records WHERE id=?", (rid,))
    return r[0] if r else {}


def stats():
    by_cat = rows("SELECT category, COUNT(*) as n FROM records GROUP BY category")
    return {"by_category": by_cat, "total": sum(x["n"] for x in by_cat)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    records = sub.add_parser("records")
    records.add_argument("--limit", type=int, default=100)
    records.add_argument("--category")
    record = sub.add_parser("record")
    record.add_argument("rid")
    sub.add_parser("stats")
    args = parser.parse_args(argv)
    if args.command == "records":
        result = get_records(args.limit, args.category)
    elif args.command == "record":
        result = get_record(args.rid)
    else:
        result = stats()
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
