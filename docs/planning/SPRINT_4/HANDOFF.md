# Sprint 4 Handoff (Draft)

Prepared on: 2026-03-05

## Starting Baseline

Sprint 4 starts from a completed Sprint 3 sales-ingest foundation:

- RETR ingest pipeline is implemented and tested (`accessdane ingest-retr --file ...`)
- sales exclusions are explicit and auditable in `sales_exclusions`
- multi-step matcher is in place through legal-description fallback
- unresolved / ambiguous / low-confidence matches are routed to review queue status
- match-audit output is available via `accessdane report-sales-matches`

Reference artifacts:

- [Sprint 3 acceptance review](../SPRINT_3/ACCEPTANCE_REVIEW.md)
- [Sprint 3 RETR operations guide](../SPRINT_3/RETR_OPERATIONS.md)
- [Sprint 3 match audit report doc](../SPRINT_3/MATCH_AUDIT_REPORT.md)

## Highest-Priority Carryover

1. Build a durable analyst review workflow beyond enum fields:
   - explicit case queue views
   - review dispositions
   - reviewer notes with timestamps
2. Add reconciliation features for difficult matches:
   - document/deed reference signals where available
   - stronger address normalization for edge municipal formats
   - additional legal-description parsing patterns
3. Add downstream analysis-ready marts:
   - parcel-level sale history features
   - neighborhood-level transaction distributions
   - flags for suspicious sale/assessment divergence patterns
4. Add feedback-loop mechanics so reviewed outcomes can tune matching/exclusion rules.

## Recommended First Validation Pass

Run these before Sprint 4 changes:

```bash
make typecheck && make lint && make format && make test
.venv/bin/accessdane report-sales-matches --out data/sprint4_baseline_match_audit.json
```

If running on a newly exported RETR file:

```bash
.venv/bin/accessdane ingest-retr --file <retr_export.csv>
.venv/bin/accessdane match-sales
.venv/bin/accessdane report-sales-matches --out data/sprint4_baseline_match_audit.json
```

## What Should Not Be Redone

- Rebuild of Sprint 3 schema/table design decisions unless a concrete deficiency is identified.
- Re-litigating whether excluded transfers should be dropped (they should remain auditable).
- Re-implementing the existing matching tiers from scratch.

## Sprint 4 Success Definition

Sprint 4 should provide a practical analyst loop on top of Sprint 3 outputs:

- clear review queue lifecycle and durable decision tracking
- improved match precision on currently unresolved/ambiguous backlog
- first-pass parcel/neighborhood reporting that can prioritize real investigations
