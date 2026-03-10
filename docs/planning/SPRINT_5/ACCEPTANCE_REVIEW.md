# Sprint 5 Acceptance Review

Reviewed on: 2026-03-10

## Overall Status

Sprint 5 is acceptable to close.

Sprint 5 delivered the planned fraud-signal engine baseline:

- `accessdane sales-ratio-study` with deterministic year/class/area rollups and diagnostics
- `accessdane build-features` with persisted versioned `parcel_features`
- `accessdane score-fraud` with explainable `fraud_scores` / `fraud_flags`
- ranked triage outputs (`top_flags`, `top_parcels`, risk/reason breakdowns)
- calibration artifacts for threshold tuning:
  - `docs/planning/SPRINT_5/CALIBRATION_SQL_V1.sql`
  - `docs/planning/SPRINT_5/CALIBRATION_NOTES.md`

## Validation Evidence

Targeted Sprint 5 closeout checks run:

- `.venv/bin/python -m pytest tests/test_sales_ratio_study.py tests/test_build_features.py tests/test_score_fraud.py tests/test_migrations.py -n auto -m "not slow"`
- `.venv/bin/accessdane init-db`
- `.venv/bin/accessdane sales-ratio-study --version-tag sales_ratio_v1 --out /tmp/sprint5_day14_sales_ratio.json`
- `.venv/bin/accessdane build-features --feature-version feature_v1 --out /tmp/sprint5_day14_build_features.json`
- `.venv/bin/accessdane score-fraud --feature-version feature_v1 --ruleset-version scoring_rules_v1 --out /tmp/sprint5_day14_score_fraud.json`
- `psql "${DATABASE_URL/postgresql+psycopg/postgresql}" -v ON_ERROR_STOP=1 -f docs/planning/SPRINT_5/CALIBRATION_SQL_V1.sql`

Observed results:

- targeted Sprint 5 test set: `32 passed in 11.68s`
- migration path initialized cleanly on runtime PostgreSQL DB
- command run status:
  - run `4` (`sales_ratio_study`, `sales_ratio_v1`): `succeeded`
  - run `5` (`build_features`, `feature_v1`): `succeeded`
  - run `6` (`score_fraud`, `scoring_rules_v1`, `parent_run_id=5`): `succeeded`
- ratio-study summary:
  - `candidate_sales_count=349`
  - `included_sales_count=316`
  - `group_count=12`
  - `insufficient_group_count=3`
- feature-build summary:
  - `selected_parcel_years=58684`
  - `rows_deleted=58684`
  - `rows_inserted=58684`
  - `rows_skipped=0`
- scoring summary:
  - `features_considered=58684`
  - `scores_inserted=58684`
  - `flags_inserted=16344`
  - `high_risk_count=19`, `medium_risk_count=28`, `low_risk_count=58637`
  - `skipped_feature_rows=0`
- explainability sample from `top_flags[0]`:
  - parcel/year: `061002401001 / 2024`
  - reason code: `ratio__assessment_to_sale_below_floor`
  - explanation: `Assessment-to-sale ratio 0.075828 is below threshold 0.55.`
- calibration SQL executed successfully against current schema and produced threshold-sweep, reason-mix, borderline-review, and area-rollup outputs.

## Acceptance Criteria Status

### 1. Each Flagged Parcel Can Be Explained In Plain Language

Status: Pass

Evidence:

- `score-fraud` persists reason-coded flags with plain-language explanation text.
- `top_flags` and `top_parcels` include deterministic reason ordering and primary reason weights.
- Validation run produced explanatory outputs for high-risk rows (for example ratio-floor trigger language).

### 2. Analysts Can Reproduce Why A Parcel Was Scored From Underlying Data

Status: Pass

Evidence:

- Versioned run chain is persisted (`sales_ratio_study` -> `build_features` -> `score_fraud` with run IDs and parent linkage).
- Source feature rows and scored outputs are persisted in `parcel_features`, `fraud_scores`, and `fraud_flags`.
- Re-runs are deterministic for same version/scope and replace in-scope outputs.

### 3. Output Supports Parcel-Level Review And Neighborhood-Level Pattern Analysis

Status: Pass (with local-data caveat)

Evidence:

- Parcel-level review surfaces are present in scoring output (`top_flags`, `top_parcels`, `reason_code_breakdown`).
- Neighborhood pattern analysis surfaces are present in calibration query outputs (`TRS`-level area rollup).
- Local validation DB currently yields one area bucket (`trs_code = (missing)`), but the rollup/query surface executed successfully and is ready for richer segmented data.

## Remaining Limits / Sprint 6 Inputs

- Current reviewer workflow is still output-driven; there is no persisted `case_reviews` workflow yet.
- Local validation data remains sparse for some dimensions (for example TRS segmentation and appeal coverage), reducing analyst realism for dashboard previews.
- Threshold calibration is documented and queryable, but not yet tied to an explicit review-feedback loop that writes back to exclusion/threshold decisions.

## Recommendation

Close Sprint 5 and begin Sprint 6 using the handoff draft in `docs/planning/SPRINT_6/HANDOFF.md`.

Sprint 5 scope is implemented, validated against targeted closeout checks, and documented for repeatable operation and calibration.
