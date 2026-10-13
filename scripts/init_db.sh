#!/usr/bin/env bash
set -e
python - <<'PY'
import sqlite3, os
os.makedirs('data/processed', exist_ok=True)
conn = sqlite3.connect('data/processed/records.sqlite')
conn.executescript(open('core/schema.sql').read())
conn.commit(); conn.close()
print('DB initialized: data/processed/records.sqlite')
PY
