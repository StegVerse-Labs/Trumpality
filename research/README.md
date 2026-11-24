# Trumpality Multi-Trajectory Research Surface

Trumpality remains the Trump-specific discovery/collection surface. ERL remains research-frontier and evaluation authority.

This layer supplements the existing `scripts/search_agent.py` rather than replacing it. Existing native person/event searches can continue to populate local context. ERL trajectory-scoped requests emit only append-only `lead-only` candidates under `research/`.

## Append-only receipt custody

Research scan receipts are one logical JSONL stream stored as numbered files under
`research/research_receipts/`. Readers process segments in lexical order. The final
segment is the only writable segment and rotates before 90 MiB; completed segments are
immutable. This keeps every file below GitHub's 100 MiB hard limit without Git LFS or an
external storage dependency.

The legacy `research/research_receipts.jsonl` bytes were preserved exactly as
`research/research_receipts/research_receipts-000000.jsonl`. Failed Actions run
`44430113790` supplied the independently digest-verified recovery delta in
`research_receipts-000001.jsonl`.

Research is not binary. Every ACTIVE trajectory is eligible for acquisition, contradictory and null evidence must be preserved, and materially distinct explanations discovered during search must be proposed as new trajectories rather than forced into an originating hypothesis.

No GitHub token is research or evaluation authority. TV/TVC governs credentials where applicable.

