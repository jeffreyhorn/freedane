# Sprint 2 Handoff

Prepared on: 2026-03-01

## Starting Baseline

Sprint 2 starts from a stable Sprint 1 baseline:

- tests are green (`36 passed`)
- migrations are in place through `20260301_0003`
- `parcel_year_facts` rebuilds successfully
- `check-data-quality` and `profile-data` both run against the full local dataset

Latest baseline metrics from Sprint 1 acceptance:

- `2,657` parcels
- `2,657` successful fetches
- `2,657` parsed fetches
- `0` parse errors
- `60,284` assessments
- `108,188` taxes
- `399,295` payments
- `58,684` source parcel-years
- `58,684` `parcel_year_facts` rows

Reference artifacts:

- [Sprint 1 closeout](../SPRINT_1/CLOSEOUT.md)
- [Sprint 1 acceptance review](../SPRINT_1/ACCEPTANCE_REVIEW.md)
- [Sprint 1 operations guide](../SPRINT_1/OPERATIONS.md)

## Highest-Priority Carryover

Start Sprint 2 with these items, in this order:

1. Refine `suspicious_assessment_dates` so the remaining `338` findings are better split into real anomalies vs expected historical carry-forward patterns.
2. Investigate the known completeness gaps: `7` successful fetches missing assessments, `42` missing tax rows, and `7` missing payment rows.
3. Improve long-running refresh ergonomics, especially the usability of full-corpus `run-all --parse-only --reparse`.
4. Make `run-all` smarter about scoped downstream actions so small parser iterations do not trigger unnecessary broad work.
5. Expand the data-quality layer beyond the first-pass integrity checks added in Sprint 1.

## What Should Not Be Redone

Sprint 2 should assume these are already in place:

- migration framework adoption
- parser fixture harness and broad regression coverage
- `parcel_year_facts` v1 table design
- baseline quality and profiling commands
- contributor bootstrap and basic operations documentation

If these areas change, treat the work as incremental improvements, not re-foundation tasks.

## Recommended First Validation Pass

Before making Sprint 2 changes, re-establish the baseline locally:

```bash
.venv/bin/python -m pytest
accessdane check-data-quality --out data/sprint2_baseline_quality.json
accessdane profile-data --out data/sprint2_baseline_profile.json
```

If parser logic is part of the first Sprint 2 task, use the scoped workflow first:

```bash
printf '061003330128\n' > data/parcel_ids.txt
accessdane parse --parcel-id 061003330128 --reparse
accessdane build-parcel-year-facts --id 061003330128
accessdane check-data-quality --ids data/parcel_ids.txt
accessdane profile-data --ids data/parcel_ids.txt
```

Use full refreshes only when the scope of the change justifies the runtime.

## Sprint 2 Success Definition

Sprint 2 should improve one or more of these without regressing the Sprint 1 baseline:

- fewer ambiguous or noisy data-quality findings
- better explanation of known missing-section parcels
- faster or clearer operational refresh paths
- stronger downstream trust in parcel-year analysis outputs
