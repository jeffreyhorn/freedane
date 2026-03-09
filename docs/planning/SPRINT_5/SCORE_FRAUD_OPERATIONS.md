# Sprint 5 Score Fraud Operations

Prepared on: 2026-03-09

## Goal

Run `accessdane score-fraud` reproducibly after `build-features`, and use ranked output surfaces for analyst triage.

## End-To-End Run Sequence

Build features first (same `feature_version` that scoring will read):

```bash
.venv/bin/accessdane build-features --feature-version feature_v1
```

Score all available feature rows for v1:

```bash
.venv/bin/accessdane score-fraud \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1
```

Scoped triage run for selected parcels/years:

```bash
.venv/bin/accessdane score-fraud \
  --ids data/parcel_scope.txt \
  --year 2025 \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --out data/score_fraud_scope.json
```

Pin scoring to a specific feature build run:

```bash
.venv/bin/accessdane score-fraud \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --feature-run-id 42
```

## Output Interpretation

`summary` reports run totals:

- `features_considered`
- `scores_inserted`
- `flags_inserted`
- `high_risk_count`
- `medium_risk_count`
- `low_risk_count`
- `skipped_feature_rows`

`top_flags` is a deterministic, strongest-signal-first list for immediate evidence review.

`rankings` adds triage-oriented surfaces:

- `top_parcels`: highest risk parcels sorted deterministically by score, reason count, parcel id, year
- `risk_band_breakdown`: parcel counts by `high` / `medium` / `low`
- `reason_code_breakdown`: triggered reason-code frequency counts
- `skipped_feature_breakdown`: skipped-row counts by safeguard reason (for example invalid year)

## Safeguards For Partial Feature Data

Scoring keeps rule-level missing-input handling, and additionally hardens structural input handling:

- rows with invalid structural keys (missing/blank parcel id or invalid year) are skipped and counted
- non-object `source_refs_json` values are tolerated and marked with `source_refs_json_invalid_shape` quality flag
- skipped row reasons are surfaced in `rankings.skipped_feature_breakdown`

## Triage Workflow

1. Run scoped scoring first for validation and analyst walkthrough.
2. Confirm `skipped_feature_rows = 0` (or explicitly accepted) before full run.
3. Start analyst review from `rankings.top_parcels` and `top_flags`.
4. Use `reason_code_breakdown` to identify dominant fraud-signal categories in scope.
5. Re-run with identical version/scope and verify deterministic ranked output ordering.
