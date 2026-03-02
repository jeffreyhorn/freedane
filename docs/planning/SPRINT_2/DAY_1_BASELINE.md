# Sprint 2 Day 1 Baseline Note

Date: 2026-03-01

## Commands Run

Day 1 baseline validation was executed with:

```bash
.venv/bin/python -m pytest
.venv/bin/accessdane check-data-quality --out data/sprint2_baseline_quality.json
.venv/bin/accessdane profile-data --out data/sprint2_baseline_profile.json
```

## Validation Result

- automated tests: `36 passed in 23.85s`
- quality snapshot refreshed: local artifact `data/sprint2_baseline_quality.json`
- profile snapshot refreshed: local artifact `data/sprint2_baseline_profile.json`

## Baseline Metrics

Current profile counts:

- `2,657` parcels
- `2,657` fetches
- `2,657` successful fetches
- `2,657` parsed fetches
- `0` parse errors
- `60,284` assessments
- `108,188` taxes
- `399,295` payments
- `2,657` parcel summaries
- `58,684` `parcel_year_facts` rows
- `2,650` parcels represented in `parcel_year_facts`
- `58,684` source parcel-years

Current missing-section counts:

- `7` successful fetches missing assessments
- `42` successful fetches missing tax rows
- `7` successful fetches missing payment rows
- `0` parcels missing current parcel summaries

Current coverage highlights:

- `successful_fetch_rate = 1.0`
- `parsed_successful_fetch_rate = 1.0`
- `parse_error_successful_fetch_rate = 0.0`
- `parcel_summary_parcel_rate = 1.0`
- `parcel_year_fact_parcel_rate = 0.9974`
- `parcel_year_fact_source_year_rate = 1.0`

## Quality Snapshot

Current issue counts by check:

- `duplicate_parcel_summaries`: `0`
- `suspicious_assessment_dates`: `338`
- `impossible_numeric_values`: `0`
- `fetch_parse_consistency`: `0`

Breakdown of `suspicious_assessment_dates`:

- `331` `stale_assessment_date`
- `7` `assessment_date_after_year`

Top repeated valuation dates among the flagged issues:

- `2000-06-30` (`106` issues)
- `1999-12-01` (`66` issues)
- `2006-02-13` (`44` issues)
- `2006-03-31` (`30` issues)
- `2002-01-03` (`20` issues)

Interpretation:

- The remaining date-quality noise is still concentrated in a small set of repeated historical valuation dates.
- This strongly suggests the next refinement should remain pattern-based rather than trying to broaden the rule globally.

## Missing-Section Investigation Seed List

The `7` parcels missing assessments are the same `7` parcels missing payments, and they are also in the missing-tax set:

- `061002140011`
- `061002140131`
- `061002140241`
- `061002191411`
- `061002430211`
- `061002430321`
- `061003305711`

This points to a likely shared source pattern rather than three unrelated parser failures.

The missing-tax set has an additional tax-only subgroup beyond those `7`, including:

- `061002275801`
- `061002275851`
- `061002275901`
- `061002275911`
- `061002275991`
- `061002320251`
- `061002320401`
- `061002480711`
- `061002495021`
- `061003102759`

Interpretation:

- Day 3 should inspect the shared all-missing group first, because it is the highest-signal cluster.
- After that, the tax-only subgroup is the next best candidate for a focused parser-gap review.

## Ranked Follow-Up For Days 2 Through 4

### Day 2 Priority

Refine `suspicious_assessment_dates` by targeting the repeated historical carry-forward patterns that dominate the remaining `331` stale-date findings.

### Day 3 Priority

Inspect the shared all-missing parcel cluster (`7` parcels missing assessment, tax, and payment rows) before looking at the broader tax-only set.

### Day 4 Priority

No new blocker was discovered in the baseline commands themselves, but the full-dataset quality pass remains materially slower than tests or profiling.
Keep the Day 4 ergonomics work focused on making targeted iteration paths cheaper than full-dataset scans.

## Day 1 Exit Status

Day 1 is complete:

- the Sprint 2 baseline was revalidated
- the starting metrics are refreshed
- the next two days now have narrower, evidence-driven targets
