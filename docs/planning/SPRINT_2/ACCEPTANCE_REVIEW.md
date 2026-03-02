# Sprint 2 Acceptance Review

Reviewed on: 2026-03-02

## Overall Status

Sprint 2 is acceptable to close with a small operational caveat.

The core Sprint 2 deliverables are present:

- migration-managed `parcel_characteristics`
- broadened characteristic extraction across normal and edge-case parcel shapes
- parse-time `parcel_lineage_links` extraction
- richer structured tax-detail fields inside `TaxRecord.data`
- a documented no-go decision on the AccessDane report endpoints
- updated contributor and acceptance documentation for the new Sprint 2 outputs

The remaining caveat is operational, not structural:

- a full-corpus `parse --reparse` pass is still a long-running workflow, so scoped parse validation remains the practical acceptance path

## Validation Evidence

Acceptance checks run during this review:

- `python -m compileall src tests`
- `.venv/bin/python -m pytest`
- `.venv/bin/accessdane init-db`
- `.venv/bin/accessdane run-all --parse-only --reparse --parse-ids data/sprint2_acceptance_ids.txt --build-parcel-year-facts --skip-anomalies`
- `.venv/bin/accessdane check-data-quality --ids data/sprint2_acceptance_ids.txt --out data/sprint2_acceptance_quality.json`
- `.venv/bin/accessdane profile-data --ids data/sprint2_acceptance_ids.txt --out data/sprint2_acceptance_profile.json`

Observed results:

- automated tests: `52 passed in 26.21s`
- the local Postgres DB was upgraded during this review from `20260301_0003` to `20260301_0004`
- scoped parse/facts rebuild: `6` fetched pages reparsed, `86` scoped `parcel_year_facts` rows rebuilt
- scoped quality snapshot: local artifact `data/sprint2_acceptance_quality.json`
- scoped profile snapshot: local artifact `data/sprint2_acceptance_profile.json`

Additional direct DB checks on the same 6-parcel scoped acceptance set:

- database URL in use: `postgresql+psycopg://USER:PASSWORD@localhost:5432/accessdane`
- `alembic_version = 20260301_0004`
- `parcel_characteristics` rows in scope: `6`
- `parcel_lineage_links` rows in scope: `14`
- `parcel_year_facts` rows in scope: `86`

Scoped profile highlights:

- `6` parcels
- `6` fetches
- `6` successful fetches
- `6` parsed fetches
- `0` parse errors
- `71` assessments
- `120` taxes
- `551` payments
- `6` `parcel_summaries`
- `86` `parcel_year_facts` rows
- `86` source parcel-years
- `parcel_year_fact_source_year_rate = 1.0`

Scoped missing-section highlights:

- `1` assessment-empty fetch
- `2` tax-empty fetches
- `1` payment-empty fetch

These are expected for the chosen acceptance subset because it intentionally includes the exempt-style empty-page and sparse-tax edge cases added to Sprint 2 coverage.

Scoped quality highlights:

- duplicate parcel summaries: `0` issues
- suspicious assessment dates: `0` issues
- impossible numeric values: `0` issues
- fetch/parse consistency: `0` issues

## Acceptance Criteria Status

### 1. `parcel_characteristics` Is A Stable Managed Output

Status: Pass

Evidence:

- `parcel_characteristics` is present in the schema at revision `20260301_0004`.
- Parse-time storage populates the current v1 field set and keeps a single `source_fetch_id`.
- Tests cover precedence behavior where a richer older snapshot beats a less complete newer snapshot.

### 2. Characteristic Coverage Is Broadened Across Common And Edge Shapes

Status: Pass

Evidence:

- Tests now cover normal residential, sparse legal-detail, vacant land, condo-like, and exempt-style page variants.
- The Day 8 and Day 12 fixtures prove the current characteristic fields persist across those shapes without breaking existing parse behavior.

### 3. `parcel_lineage_links` Extraction Works

Status: Pass

Evidence:

- Parent/child history modals are parsed and normalized into `parcel_lineage_links`.
- Tests cover:
  - a parcel with both parents and children
  - a parcel with only parents
- The scoped acceptance set persisted `14` lineage rows across the selected lineage-heavy parcels.

### 4. Richer Tax Detail Fields Are Extracted

Status: Pass

Evidence:

- `TaxRecord.data` detail rows now include structured fields such as `tax_credit_rows`, `special_charge_rows`, `installment_rows`, and `tax_amount_summary`.
- Tests cover both a normal residential parcel and a split-lot edge case with explicit special-charge rows.

### 5. Report Endpoints Are Explicitly Resolved

Status: Pass

Evidence:

- Day 11 confirmed that `Summary Report` is a direct PDF response and `Custom Report` is an HTML setup form that also terminates in PDF.
- The rejection decision is documented in [DAY_11_NOTES.md](DAY_11_NOTES.md) and reflected in [README.md](../../../README.md).

### 6. Deliberate Deferrals Are Documented

Status: Pass

Evidence:

- The Sprint 2 checklist explicitly records the accepted deferrals:
  - no dedicated full rebuild command yet for `parcel_characteristics`
  - no dedicated full rebuild command yet for `parcel_lineage_links`
  - no normalized richer tax-detail table yet beyond the current JSON payload
  - no PDF-based report fetch/parser path

### 7. Contributor Docs Reflect Sprint 2 Reality

Status: Pass

Evidence:

- [README.md](../../../README.md) now documents the current Sprint 2 parse-time outputs and the report-endpoint no-go.
- [CHECKLIST.md](CHECKLIST.md) defines explicit acceptance criteria and deferrals.

## Operational Caveat

The acceptance run surfaced one real operational issue and one continuing runtime caveat:

- The local Postgres database had not yet been migrated to `20260301_0004`, which caused an initial `parse --reparse` attempt to fail because `parcel_characteristics` did not exist.
- Running `accessdane init-db` corrected this immediately and the schema is now current.
- A full-corpus `parse --reparse` is still a long-running path, so this review used a representative scoped acceptance set instead of waiting for a full-corpus reparse to complete.

This does not block Sprint 2 acceptance, but it should inform Sprint 3 operational improvements.

## Recommendation

Sprint 2 should be treated as complete enough to hand off into Sprint 3 planning.

The implementation is stable, the full automated suite is green, the new extraction outputs are working on representative edge cases, and the remaining limitations are now explicit rather than ambiguous.
