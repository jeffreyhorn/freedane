# Sprint 6 Operations Runbook

Prepared on: 2026-03-14

## Goal

Provide one repeatable Sprint 6 investigation workflow from scored outputs to analyst-ready queue, dossier, review outcomes, feedback artifacts, and static reporting.

## Prerequisites

- virtualenv dependencies installed: `pip install -e '.[dev]'`
- database URL configured (default SQLite: `sqlite:///data/accessdane.sqlite`)
- source data ingested for the target scope (RETR/sales, permits, appeals) before running Sprint 5/6 analysis commands
- schema initialized to migration head:

```bash
.venv/bin/accessdane init-db
```

## Sprint 6 Investigation Sequence

1. Build/refresh scoring baseline (if the source data changed).

```bash
.venv/bin/accessdane sales-ratio-study \
  --version-tag sales_ratio_v1 \
  --out data/sprint6_sales_ratio_study.json

.venv/bin/accessdane build-features \
  --feature-version feature_v1 \
  --out data/sprint6_build_features.json

.venv/bin/accessdane score-fraud \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --out data/sprint6_score_fraud.json
```

2. Generate ranked analyst queue and optional CSV export.

```bash
.venv/bin/accessdane review-queue \
  --top 100 \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --out data/sprint6_review_queue.json \
  --csv-out data/sprint6_review_queue.csv
```

3. Build parcel dossier for a queued parcel.

```bash
.venv/bin/accessdane parcel-dossier \
  --id <parcel_id> \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --out data/sprint6_parcel_dossier_<parcel_id>.json
```

4. Record analyst review outcomes.

```bash
.venv/bin/accessdane case-review create \
  --score-id <score_id> \
  --status in_review \
  --reviewer "<analyst_name>" \
  --assigned-reviewer "<analyst_name>" \
  --note "Initial triage"

.venv/bin/accessdane case-review update \
  --id <case_review_id> \
  --status resolved \
  --disposition false_positive \
  --set-evidence-link "kind=dossier,ref=<dossier_ref>"
```

5. Monitor review outcomes and feedback artifacts.

```bash
.venv/bin/accessdane case-review list \
  --status resolved \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --out data/sprint6_case_reviews_resolved.json

.venv/bin/accessdane review-feedback \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --out data/sprint6_review_feedback.json \
  --sql-out data/sprint6_review_feedback.sql
```

6. Build static investigation report surface.

```bash
.venv/bin/accessdane investigation-report \
  --top 100 \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --html-out data/sprint6_investigation_report.html \
  --out data/sprint6_investigation_report.json
```

## Scoped Validation Loop

Use this shorter loop for dry-runs before full refreshes:

```bash
.venv/bin/accessdane review-queue --top 10 --out data/sprint6_queue_smoke.json
.venv/bin/accessdane parcel-dossier --id <parcel_id> --out data/sprint6_dossier_smoke.json
.venv/bin/accessdane review-feedback --out data/sprint6_feedback_smoke.json
.venv/bin/accessdane investigation-report --top 10 --html-out data/sprint6_report_smoke.html
```

## Targeted Sprint 6 Validation Set

Run this test set for dossier/queue/review/report integration changes:

```bash
.venv/bin/python -m pytest \
  tests/test_parcel_dossier.py \
  tests/test_review_queue.py \
  tests/test_case_review.py \
  tests/test_review_feedback.py \
  tests/test_investigation_report.py \
  tests/test_investigation_workflow.py \
  -n auto -m "not slow"
```

## Troubleshooting

- `unsupported_version_selector`:
  - Confirm `--feature-version` and `--ruleset-version` match available runs and supported selectors.
- Empty queue output:
  - Re-run scoring for target scope; confirm `fraud_scores.requires_review` and filters (`--id`, `--year`, `--risk-band`) are not over-constrained.
- Dossier unavailable sections:
  - Check whether upstream source tables are populated for the requested parcel/year and version selectors.
- `duplicate_case_review`:
  - A case review already exists for (`score_id`, `feature_version`, `ruleset_version`); use `case-review update` instead of create.
- `invalid_transition` or `invalid_disposition_for_status`:
  - Validate status lifecycle rules in [CASE_REVIEW_V1.md](CASE_REVIEW_V1.md).
- Feedback artifact has low sample counts:
  - Review outcomes are sparse; treat feedback rates as directional until more resolved/closed cases accumulate.

## Framing Guardrails

Analyst outputs and stakeholder communication must follow:

- [METHODOLOGY_MEMO.md](METHODOLOGY_MEMO.md)

Always treat score output as triage signal, not proof.
