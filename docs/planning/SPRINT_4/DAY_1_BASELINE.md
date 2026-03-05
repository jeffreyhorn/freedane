# Sprint 4 Day 1 Baseline

Date: 2026-03-05

Branch: `sprint4/day-01-baseline`

## Commands Run

Day 1 baseline validation was executed with:

```bash
make typecheck && make lint && make format && make test
.venv/bin/accessdane report-sales-matches --out data/sprint4_baseline_match_audit.json
.venv/bin/alembic heads
.venv/bin/alembic current -v
```

## Validation Result

- `make typecheck`: passed (`mypy` clean for `src/`)
- `make lint`: passed (`ruff` + `mypy` clean)
- `make format`: passed (`black` no-op, import sorting clean)
- `make test`: passed (`108 passed in 11.50s`)
- baseline report artifact generated: `data/sprint4_baseline_match_audit.json`

## Current Sales-Match Baseline Metrics

From `data/sprint4_baseline_match_audit.json`:

- loaded transactions in scope: `860`
- matched transactions: `549`
- unmatched transactions: `311`
- review status counts:
  - `needs_review`: `311`
  - `unreviewed`: `549`
- confidence tiers:
  - `high`: `546`
  - `medium`: `3`
  - `low`: `0`
  - `unknown`: `0`
- best-match methods:
  - `exact_parcel_number`: `545`
  - `normalized_address`: `3`
  - `parcel_number_crosswalk`: `1`
- review queue:
  - unresolved: `311`
  - ambiguous: `0`
  - low confidence: `0`

## Schema And Migration Baseline

- Alembic head revision: `20260304_0006`
- Local runtime DB (`sqlite:///data/accessdane.sqlite`) currently has no recorded row in Alembic version tracking (`alembic current -v` reports no active revision)

Interpretation:

- Migration head is defined and stable in repo.
- Runtime DB lifecycle is currently not guaranteed to be Alembic-stamped, so Sprint 4 migration changes should continue validating both migration integrity and runtime bootstrap compatibility.

## Sprint 4 Execution Checklist (Days 2-5)

- [ ] Day 2: finalize permit import contract and normalization rules (`PERMIT_IMPORT_V1.md`)
- [ ] Day 3: add `permit_events` model + migration + migration tests
- [ ] Day 4: implement `accessdane ingest-permits --file ...` with row-level statuses
- [ ] Day 5: harden permit ingest for messy/manual records and document operations

Cross-cutting carryover to keep visible while implementing Day 2-5:

- [ ] keep importer behavior tolerant of missing/inconsistent manual data
- [ ] preserve auditability of ingestion outcomes (do not silently drop bad rows)
- [ ] avoid schema choices that block future records-request expansions

## Day 1 Exit Decision

Day 1 is complete enough to proceed to Sprint 4 Day 2.

The baseline validation is green, a fresh audit artifact is captured, and the next four days are concretely scoped around the permit import path.
