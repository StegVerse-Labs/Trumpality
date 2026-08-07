# Person/Event Evaluation Consumer Handoff

Trumpality conforms to ERL's `standards/person-event-current-state-evaluation.v1.md` as a governed person-specific consumer.

Canonical local surfaces:

- `contracts/executive-rhetoric-ledger-reviewed-projection.contract.yml`
- `data/receipts/ledger_evidence_updates.jsonl`
- `data/receipts/ledger_current_state_evaluation.json`
- `scripts/validate_current_state_evaluation.py`
- `.github/workflows/validate-current-state-evaluation.yml`
- `docs/OSINT_PROJECTION_MIRROR_HANDOFF.md`

Every newly imported reviewed ERL evidence/current-state change must append a proposition-relative movement before or with current-state refresh. Native records and verification labels remain unchanged. ERL remains evaluation authority.

Continuation is machine-owned by the existing scheduled reviewed-projection consumer. No chat-owned implementation claim remains after hosted validation is observed green.
