# Sprint 3 Day 1 Baseline

Date: 2026-03-02

Branch: `sprint3/day-01-baseline`

## Commands Run

Day 1 baseline validation was executed with:

```bash
.venv/bin/python -m pytest
.venv/bin/accessdane init-db
.venv/bin/accessdane run-all --parse-only --reparse --parse-ids data/sprint2_acceptance_ids.txt --build-parcel-year-facts --skip-anomalies
.venv/bin/accessdane check-data-quality --ids data/sprint2_acceptance_ids.txt --out data/sprint3_baseline_quality.json
.venv/bin/accessdane profile-data --ids data/sprint2_acceptance_ids.txt --out data/sprint3_baseline_profile.json
```

## Validation Result

- automated tests: `53 passed in 21.88s`
- schema init: Postgres schema already current at `20260301_0004`
- scoped parse/facts rebuild: `6` fetched pages reparsed, `86` `parcel_year_facts` rows rebuilt
- quality snapshot refreshed: local artifact `data/sprint3_baseline_quality.json`
- profile snapshot refreshed: local artifact `data/sprint3_baseline_profile.json`

## Current Scoped Metrics

Scoped counts for the Sprint 2 acceptance parcel set:

- `6` parcels
- `6` fetches
- `6` successful fetches
- `6` parsed fetches
- `0` parse errors
- `71` assessments
- `120` taxes
- `551` payments
- `6` `parcel_summaries`
- `86` `parcel_year_facts`
- `5` parcels represented in `parcel_year_facts`
- `86` source parcel-years

Scoped Sprint 2 output checks that are not yet surfaced directly in `profile-data`:

- `6` `parcel_characteristics` rows
- `14` `parcel_lineage_links` rows
- `60` scoped tax detail rows
- `60` of `60` scoped tax detail rows already carry structured tax-detail fields (`tax_amount_summary` present)

Scoped missing-section counts:

- `1` assessment-empty fetch
- `2` tax-empty fetches
- `1` payment-empty fetch
- `0` parcels missing current parcel summaries

Scoped coverage highlights:

- `successful_fetch_rate = 1.0`
- `parsed_successful_fetch_rate = 1.0`
- `parse_error_successful_fetch_rate = 0.0`
- `parcel_summary_parcel_rate = 1.0`
- `parcel_year_fact_parcel_rate = 0.8333`
- `parcel_year_fact_source_year_rate = 1.0`

## Quality Snapshot

All current scoped checks passed:

- `duplicate_parcel_summaries`: `0`
- `suspicious_assessment_dates`: `0`
- `impossible_numeric_values`: `0`
- `fetch_parse_consistency`: `0`

Interpretation:

- The Sprint 2 scoped acceptance set is still a stable smoke-test baseline for Sprint 3 work.
- The known sparse / exempt-style parcels in that set still produce expected missing-section counts without surfacing false-positive quality failures.

## Drift From Sprint 3 Handoff

The Sprint 3 handoff assumptions still hold.

Observed drift:

- the automated test count is now `53` instead of `52`

Interpretation:

- this is coverage growth, not a baseline regression
- the schema revision, scoped counts, and zero-issue quality result still match the intended Sprint 3 starting state

## Ranked Follow-Up For Days 2 Through 4

### Day 2 Priority

Define the dedicated rebuild path for `parcel_characteristics` and `parcel_lineage_links`, including full vs scoped replacement semantics and CLI shape.

### Day 3 Priority

Implement the `parcel_characteristics` rebuild path first, because it is the simpler of the two derived Sprint 2 outputs and should establish the rebuild pattern.

### Day 4 Priority

Implement the `parcel_lineage_links` rebuild path second, reusing the same operational pattern while preserving normalized dedupe behavior and latest-fetch metadata rules.

## Day 1 Exit Decision

Day 1 is complete enough to move into the Sprint 3 rebuild design work.

The current baseline is stable, the acceptance smoke set is still valid, and the next three days are materially narrower than they were at handoff.
