# Sprint 3 Handoff

Prepared on: 2026-03-02

## Starting Baseline

Sprint 3 starts from a stable Sprint 2 extraction baseline:

- tests are green (`52 passed`)
- migrations are in place through `20260301_0004`
- `parcel_characteristics` is now a migration-managed parse-time output
- `parcel_lineage_links` is now populated from AccessDane parent/child history modals
- richer structured tax-detail fields are now stored inside `TaxRecord.data` for `source == "detail"` rows
- the AccessDane report endpoints were investigated and explicitly rejected as a Sprint 2 fetch path because they are currently a PDF print flow

Latest validation evidence from Sprint 2 acceptance:

- automated tests: `52 passed in 26.21s`
- active DB schema: `20260301_0004`
- scoped acceptance set: `6` parcels
- scoped `parcel_year_facts`: `86` rows
- scoped `parcel_characteristics`: `6` rows
- scoped `parcel_lineage_links`: `14` rows
- scoped quality checks: all `0` issues

Reference artifacts:

- [Sprint 2 acceptance review](../SPRINT_2/ACCEPTANCE_REVIEW.md)
- [Sprint 2 acceptance checklist](../SPRINT_2/CHECKLIST.md)
- [Sprint 2 report-endpoint decision](../SPRINT_2/DAY_11_NOTES.md)

## Highest-Priority Carryover

Start Sprint 3 with these items, in this order:

1. Add a dedicated rebuild path for `parcel_characteristics` and `parcel_lineage_links` so those Sprint 2 outputs do not depend only on parse-time side effects.
2. Improve long-running parse ergonomics, especially the current full-corpus `parse --reparse` single-transaction behavior.
3. Add profiling / completeness reporting for the new Sprint 2 outputs:
   - `parcel_characteristics` coverage
   - lineage coverage
   - richer tax-detail field presence
4. Decide whether the structured tax-detail JSON should stay embedded in `TaxRecord.data` or be promoted into a normalized supporting table / mart.
5. Expand quality checks where the new fields make it useful, especially around exempt-style page classification, lineage consistency, and placeholder payment-history semantics.

## What Should Not Be Redone

Sprint 3 should assume these are already in place:

- `parcel_characteristics` schema and baseline extraction path
- parse-time lineage extraction into `parcel_lineage_links`
- structured modal-derived tax-detail fields in `TaxRecord.data`
- the report-endpoint evaluation and rejection decision
- fixture coverage for the current edge-case parcel categories

If these areas change, treat the work as iterative refinement or normalization, not as rediscovery work.

## Recommended First Validation Pass

Before starting Sprint 3 changes, re-establish the current baseline locally:

```bash
.venv/bin/python -m pytest
.venv/bin/accessdane init-db
```

Then use the Sprint 2 scoped acceptance set first:

```bash
.venv/bin/accessdane run-all --parse-only --reparse --parse-ids data/sprint2_acceptance_ids.txt --build-parcel-year-facts --skip-anomalies
.venv/bin/accessdane check-data-quality --ids data/sprint2_acceptance_ids.txt --out data/sprint3_baseline_quality.json
.venv/bin/accessdane profile-data --ids data/sprint2_acceptance_ids.txt --out data/sprint3_baseline_profile.json
```

Use a full-corpus parse refresh only when the scope of the Sprint 3 change justifies the runtime.

## Sprint 3 Success Definition

Sprint 3 should improve one or more of these without regressing the Sprint 2 baseline:

- better operational control over rebuilding the new Sprint 2 outputs
- clearer measurement of `parcel_characteristics`, lineage, and richer tax-detail completeness
- a more queryable downstream shape for the richer tax-detail data
- stronger quality guarantees around the new extraction layers
