# Sprint 2 Day 1 Execution Checklist

## Objective

Re-establish the Sprint 1 baseline, capture current Sprint 2 starting metrics, and convert the handoff into a concrete ranked issue list.

## Pre-Flight

- [ ] Confirm the virtualenv is available and dependencies are installed.
- [ ] Confirm the database is reachable through `DATABASE_URL`.
- [ ] Confirm the local repo is on the Sprint 1 closeout baseline.

Suggested commands:

```bash
source .venv/bin/activate
alembic current
```

## Step 1: Re-Run Baseline Validation

- [ ] Run the full automated test suite.
- [ ] Refresh the full-dataset quality snapshot.
- [ ] Refresh the full-dataset profile snapshot.

Commands:

```bash
.venv/bin/python -m pytest
.venv/bin/accessdane check-data-quality --out data/sprint2_baseline_quality.json
.venv/bin/accessdane profile-data --out data/sprint2_baseline_profile.json
```

Success criteria:

- pytest passes cleanly
- both JSON artifacts are written
- no command crashes on the current local dataset

## Step 2: Record Baseline Metrics

- [ ] Capture parcel, fetch, parsed-fetch, parse-error, and `parcel_year_facts` counts from the profile snapshot.
- [ ] Capture missing-section counts from the profile snapshot.
- [ ] Capture quality issue counts by check from the quality snapshot.

Primary files:

- local artifact `data/sprint2_baseline_profile.json`
- local artifact `data/sprint2_baseline_quality.json`

## Step 3: Review Suspicious Assessment Date Patterns

- [ ] Group the `suspicious_assessment_dates` issues by code and by repeated date pattern.
- [ ] Identify which patterns still look like expected carry-forward behavior.
- [ ] Identify which patterns still look truly suspicious and should remain flagged.

Questions to answer:

- Are most remaining issues concentrated in a few repeatable historical valuation-date patterns?
- Are there obvious thresholds or exemptions that would reduce noise without hiding real anomalies?

## Step 4: Review Missing-Section Parcels

- [ ] Confirm the current counts for successful fetches missing assessments, tax rows, and payment rows.
- [ ] Pull a small sample parcel list from each category for manual inspection.
- [ ] Classify each sample as likely parser gap vs likely source omission.

Minimum Day 1 sample target:

- 3 parcels from the missing-assessment set
- 3 parcels from the missing-tax set
- 3 parcels from the missing-payment set

## Step 5: Convert Findings Into Sprint 2 Priorities

- [ ] Rank the highest-value quality-rule fix for Day 2.
- [ ] Rank the most actionable missing-section parser investigation for Day 3.
- [ ] Note whether any operational friction in the Day 1 commands should move into the Day 4 ergonomics work.

Deliverables:

- a baseline metrics note
- a ranked issue list for Days 2 through 4
- refreshed `data/sprint2_baseline_quality.json`
- refreshed `data/sprint2_baseline_profile.json`

## End-Of-Day Exit Criteria

- [ ] Sprint 2 has a fresh validated starting baseline.
- [ ] The next parser or quality change can be justified by specific current findings, not assumptions.
- [ ] The Day 2 and Day 3 tasks are materially narrower than they were at handoff.
