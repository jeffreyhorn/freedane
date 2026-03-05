# Wisconsin DOR RETR Import Operations

Prepared on: 2026-03-05

## Goal

Document a repeatable workflow to export Wisconsin DOR RETR transfer data and load it into this project with `ingest-retr`.

## Prerequisites

- Local virtualenv with project dependencies installed (`pip install -e .[dev]`)
- Database initialized (`.venv/bin/accessdane init-db`)
- Access to the Wisconsin DOR RETR system and CSV export workflow

## Export Guidance (RETR Portal)

1. Run your normal RETR search/export workflow for the scope you need (for example county + municipality + transfer date range).
2. Export as CSV (not XLS/PDF).
3. Keep the source rows as-is; do not pre-normalize parcel numbers or addresses before import.
4. Ensure each row includes the v1-critical fields required by the importer:
   - `Transfer Date`
   - `Consideration` / sale price
   - at least one identifier:
     - parcel number, or
     - property/physical address, or
     - legal description
5. Save the file locally, for example: `~/Downloads/202603RETRReport.csv`.

## Load + Match Workflow

```bash
.venv/bin/accessdane ingest-retr --file ~/Downloads/202603RETRReport.csv
.venv/bin/accessdane match-sales
.venv/bin/accessdane report-sales-matches --out data/sprint3_match_audit.json
```

## Interpreting Import Output

`ingest-retr` prints:

- `total`: rows read from CSV
- `loaded`: rows accepted for downstream matching
- `rejected`: rows preserved but not match-eligible
- `inserted` / `updated`: same-file idempotent upsert behavior

Rejected rows are still stored in `sales_transactions` with `import_error` for auditability.

## Interpreting Match + Audit Output

- `match-sales` prints run-level counters (`matched`, `needs_review`, `unresolved`, `ambiguous`, `low_confidence`).
- `report-sales-matches` writes a JSON snapshot with confidence-tier breakdown and manual-review queue counts.
- Start manual triage with:
  - unresolved transactions
  - ambiguous candidate sets
  - low-confidence legal-description fallback links

## Re-import Behavior

- Re-running `ingest-retr` on the exact same file hash updates existing rows instead of duplicating them.
- Exclusion rules (`sales_exclusions`) are re-synced on same-file re-import and inactive rules are deactivated.

## Recommended Sprint 3 Closeout Loop

```bash
.venv/bin/accessdane ingest-retr --file <retr_export.csv>
.venv/bin/accessdane match-sales
.venv/bin/accessdane report-sales-matches --out data/sprint3_match_audit.json
make typecheck && make lint && make format && make test
```
