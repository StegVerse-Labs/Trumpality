#!/usr/bin/env python3
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "data" / "receipts" / "ledger_current_state_evaluation.json"
LEDGER = ROOT / "data" / "receipts" / "ledger_evidence_updates.jsonl"

ALLOWED_DIRECTIONS = {"strengthen", "weaken", "disambiguate", "contextualize", "no-update"}


def fail(msg: str) -> None:
    raise SystemExit(f"FAIL: {msg}")


def main() -> None:
    if not INDEX.exists() or not LEDGER.exists():
        fail("current-state index and evidence movement ledger are required")
    state = json.loads(INDEX.read_text())
    if state.get("evaluation_authority") != "StegVerse-Labs/Executive_Rhetoric_Ledger":
        fail("ERL must remain evaluation authority")
    if state.get("native_records_mutated") is not False or state.get("verification_labels_changed") is not False:
        fail("reviewed projection must not mutate native records or verification labels")
    if state.get("authority_effect") is not False:
        fail("current-state import cannot grant authority")
    events = []
    for line_no, line in enumerate(LEDGER.read_text().splitlines(), start=1):
        if not line.strip():
            continue
        event = json.loads(line)
        if event.get("direction") not in ALLOWED_DIRECTIONS:
            fail(f"line {line_no}: invalid direction")
        if event.get("authority_effect") is not False or event.get("publication_effect") is not False:
            fail(f"line {line_no}: imported evidence movement cannot grant authority/publication")
        events.append(event)
    if not events:
        fail("evidence movement ledger cannot be empty")
    print(f"PASS: {len(events)} evidence movement event(s); current-state index governed")


if __name__ == "__main__":
    main()
