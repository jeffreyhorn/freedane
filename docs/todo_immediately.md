# TODO Immediately: Startup Checklist For Analyst Operations

Use this checklist before analysts begin active fraud/anomaly triage for McFarland, WI.

Important framing:

- Treat all model output as **triage signal**, not proof of fraud.
- Use dossier evidence + review workflow before any enforcement conclusion.

## 1. Environment And Database Migrations

Run once per environment (or after pulling schema changes):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
.venv/bin/accessdane init-db
```

Expected result:

- Database is at migration head.
- CLI commands can run without schema errors.

## 2. Initial Data Loads (When New Source Files Exist)

Run these when you have fresh exports (RETR, permits, appeals):

```bash
.venv/bin/accessdane ingest-retr --file <retr_export.csv>
.venv/bin/accessdane match-sales

.venv/bin/accessdane ingest-permits --file <permits.csv>
.venv/bin/accessdane ingest-appeals --file <appeals.csv>

.venv/bin/accessdane build-parcel-year-facts
```

Notes:

- Permits and appeals are optional inputs, but missing them can reduce analyst confidence.
- Re-run `build-parcel-year-facts` after ingest updates so derived analytics stay in sync.

## 3. Baseline Statistical Analysis Pipeline

Run in this order:

```bash
.venv/bin/accessdane sales-ratio-study \
  --version-tag sales_ratio_v1 \
  --out data/startup_sales_ratio_study.json

.venv/bin/accessdane build-features \
  --feature-version feature_v1 \
  --out data/startup_build_features.json

.venv/bin/accessdane score-fraud \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --out data/startup_score_fraud.json
```

## 4. Analyst-Ready Queue And Reporting Artifacts

Produce initial work surfaces:

```bash
.venv/bin/accessdane review-queue \
  --top 100 \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --out data/startup_review_queue.json \
  --csv-out data/startup_review_queue.csv

.venv/bin/accessdane review-feedback \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --out data/startup_review_feedback.json \
  --sql-out data/startup_review_feedback.sql

.venv/bin/accessdane investigation-report \
  --top 100 \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --html-out data/startup_investigation_report.html \
  --out data/startup_investigation_report.json
```

## 5. Monitoring/Diagnostics Baseline (Recommended)

Run after startup pipeline so operations health is visible:

```bash
.venv/bin/accessdane parser-drift-snapshot \
  --profile-name daily_refresh \
  --run-date <YYYYMMDD> \
  --out data/startup_parser_drift_snapshot.json

.venv/bin/accessdane load-monitor \
  --artifact-base-dir data/refresh_runs \
  --profile-name daily_refresh \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --out data/startup_load_monitor.json \
  --alert-out data/startup_load_monitor_alert.json

.venv/bin/accessdane benchmark-pack \
  --run-date <YYYYMMDD> \
  --run-id startup_benchmark_run \
  --profile-name daily_refresh \
  --feature-version feature_v1 \
  --ruleset-version scoring_rules_v1 \
  --top-n 100 \
  --artifact-base-dir data/benchmark_packs \
  --out data/startup_benchmark_pack.json \
  --trend-out data/startup_benchmark_pack_trend.json \
  --alert-out data/startup_benchmark_pack_alert.json
```

## 6. Go/No-Go Before Analyst Triage

Proceed to analyst work only when all are true:

- `init-db` succeeded
- load/ingest commands completed without blocking errors
- scoring pipeline completed (`sales-ratio-study` -> `build-features` -> `score-fraud`)
- review queue and investigation report artifacts exist
- no unresolved `critical` monitor/diagnostic alerts without owner assignment
