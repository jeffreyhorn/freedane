# Sprint 3 Acceptance Review

Reviewed on: 2026-03-05

## Overall Status

Sprint 3 is acceptable to close.

Sprint 3 delivered the first usable market-sales ingestion and matching workflow:

- migration-managed `sales_transactions`, `sales_parcel_matches`, and `sales_exclusions`
- production `ingest-retr` CLI path for Wisconsin DOR RETR CSV files
- v1 exclusion rules with auditable exclusion rows
- multi-step sales matcher (parcel number, crosswalk, address, legal-description fallback)
- explicit review-queue routing for unresolved, ambiguous, and low-confidence links
- operator-facing match-audit report output

## Validation Evidence

Validation checks run for Sprint 3 closeout:

- `make typecheck`
- `make lint`
- `make format`
- `make test`
- `.venv/bin/python -m pytest tests/test_retr.py tests/test_migrations.py`
- `.venv/bin/accessdane match-sales`
- `.venv/bin/accessdane report-sales-matches --out data/sprint3_match_audit.json`

Observed results:

- project quality gate: `105 passed`
- focused RETR/migration tests: `34 passed`
- local match refresh summary: `selected=860 matched=549 needs_review=311 unresolved=311`
- match-audit artifact written: `data/sprint3_match_audit.json`
- audit snapshot highlights:
  - loaded transactions in scope: `860`
  - matched: `549`
  - unmatched: `311`
  - review status counts: `needs_review=311`, `unreviewed=549`
  - confidence tiers: `high=546`, `medium=3`, `low=0`, `unknown=0`

## Acceptance Criteria Status

### 1. RETR CSV Ingestion Is Operator-Usable

Status: Pass

Evidence:

- `accessdane ingest-retr --file ...` accepts RETR alias headers and imports rows with normalized matching keys.
- Rejected rows are preserved with `import_error` and file/row provenance.
- Same-file reload behavior is idempotent (`inserted` vs `updated` summaries).

### 2. Sale-To-Parcel Matching Is Implemented With Explainable Tiers

Status: Pass

Evidence:

- Matching tiers are implemented in order:
  - exact parcel number
  - parcel-number crosswalk correction
  - normalized address
  - legal-description fallback
- Match candidates persist method, confidence, rank, and review status.

### 3. Non-Comparable Transfers Are Explicitly Excluded, Not Dropped

Status: Pass

Evidence:

- Exclusion rules for non-arms-length, non-usable-sale, family-transfer, government-transfer, and corrective-deed paths are implemented.
- Exclusions are tracked in `sales_exclusions` with machine code + human reason and active/inactive state.

### 4. Ambiguous/Low-Confidence Cases Are Isolated Into Review Queues

Status: Pass

Evidence:

- Transaction review status routing marks unresolved/ambiguous/low-confidence outcomes as `needs_review`.
- Reviewed transactions remain stable and are not re-counted into active review-queue counters.
- `report-sales-matches` provides unresolved/ambiguous/low-confidence queue counts and sample transaction IDs.

### 5. Operator Documentation Is Present

Status: Pass

Evidence:

- RETR export/import workflow documented in [RETR_OPERATIONS.md](RETR_OPERATIONS.md).
- Match audit report shape and interpretation documented in [MATCH_AUDIT_REPORT.md](MATCH_AUDIT_REPORT.md).
- README updated with the new `report-sales-matches` command.

## Remaining Limits / Next Sprint Inputs

- Matching still relies on heuristic text normalization and does not yet include deed-reference reconciliation.
- Analyst workflow for resolving review queues is still lightweight (`review_status` fields) and should evolve into dedicated case/review tooling.
- Neighborhood/assessment-pattern analytics are not yet layered on top of the new sales graph.

## Recommendation

Close Sprint 3 and start Sprint 4 with a focus on analyst workflow, review-loop persistence, and cross-dataset contextual enrichments.
