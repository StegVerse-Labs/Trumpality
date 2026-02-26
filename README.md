# Trumpality

Trumpality is an evidence-first, append-only documentation system tracking:

1. Verbatim public statements made by Donald J. Trump.
2. Documented executive or political actions attributable to Trump.
3. Correlations between rhetoric, policy actions, judicial response, and outcomes.

This repository operates in two modes:

- **Biography Mode** – Subject-based documentation using reputable sources.
- **Rhetoric → Action Ledger Mode** – Structured audit of statements and corresponding actions.

No editorial commentary is included in record files.
All entries require verifiable sources.

---

## Core Question

Did Trump say X?  
Did Trump attempt or execute X?  
Did policy reflect X?  
Did courts intervene?  
What was the outcome?

---

## Repository Structure

### /records/
- statements/YYYY/
- actions/YYYY/
- links/YYYY/

### /freedom/
FREE-DOM involvement graph (entities, events, relations)

### /analysis/
Comparative and pattern analysis

---

## Evidence Standards

- Statements must be verbatim.
- Primary source required (video, transcript, official post).
- Secondary sources allowed only if they cite primary documentation.
- No memes or partisan commentary.
- All entries are append-only.

---

## FREE-DOM Layer

The FREE-DOM layer maps:

- Entities
- Events
- Dates
- Court filings
- Source URLs

Graph relations allow:

- Trump → Said → Statement
- Statement → Implements → Action
- Action → Challenged In → Court Case
- Court Case → Blocked → Action

---

## Quick Start

1. Upload repository contents.
2. Run **weekly-ingest** from Actions.
3. Add new records under `/records/`.
4. Update FREE-DOM CSV files as needed.
5. Rebuild graph via `build_graph.py` (optional via Codespaces).

---

## Governance Principles

- Evidence-first
- Verbatim-first
- Append-only
- Comparative fairness when fraud is cited
- No selective omission of disconfirming cases

---

## Related Repositories

- **Administrations** – Institutional executive branch records by term.
- **Executive_Rhetoric_Ledger** – Cross-administration comparative analysis.
