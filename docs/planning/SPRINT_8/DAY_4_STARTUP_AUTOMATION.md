# Sprint 8 Day 4 Startup Automation Notes

## Scope Delivered

Day 4 converts `docs/todo_immediately.md` into a repeatable bootstrap workflow:

- one-command startup execution via `scripts/run_startup_workflow.sh`
- deterministic startup artifact layout under `data/startup_runs/<run_date>/<run_id>/`
- machine-readable go/no-go decision that blocks analyst intake on failure (`NO_GO`)

## Startup Command

```bash
scripts/run_startup_workflow.sh \
  --run-date <YYYYMMDD> \
  --run-id startup_<YYYYMMDD>_daily_refresh_feature_v1_scoring_rules_v1 \
  --retr-file <retr_export.csv> \
  --permits-file <permits.csv> \
  --appeals-file <appeals.csv>
```

Optional file flags may be omitted; omitted ingest steps are recorded as skipped.

## Deterministic Artifact Layout

For `run_date=20260325` and `run_id=startup_20260325_daily_refresh_feature_v1_scoring_rules_v1`:

```text
data/startup_runs/20260325/startup_20260325_daily_refresh_feature_v1_scoring_rules_v1/
  logs/
    init_db.log
    ...
  outputs/
    startup_sales_ratio_study.json
    startup_build_features.json
    startup_score_fraud.json
    startup_review_queue.json
    startup_review_queue.csv
    startup_review_feedback.json
    startup_review_feedback.sql
    startup_investigation_report.html
    startup_investigation_report.json
    startup_parser_drift_snapshot.json
    startup_load_monitor.json
    startup_load_monitor_alert.json
    startup_benchmark_pack.json
    startup_benchmark_pack_trend.json
    startup_benchmark_pack_alert.json
    startup_go_no_go.json
    startup_run_summary.json
  step_results.tsv
```

## Go/No-Go Artifact Template

`startup_go_no_go.json` shape:

```json
{
  "run_id": "startup_20260325_daily_refresh_feature_v1_scoring_rules_v1",
  "run_date": "20260325",
  "decision": "GO",
  "generated_at_utc": "2026-03-25T20:00:00+00:00",
  "checks": [
    {
      "check_id": "init_db_succeeded",
      "passed": true,
      "details": "init-db completed at migration head."
    }
  ],
  "blocking_reasons": []
}
```

`decision` values:

- `GO`: analyst intake may proceed.
- `NO_GO`: analyst intake must remain blocked until `blocking_reasons` are resolved.

## Verification Notes

Minimum operator verification after each startup run:

1. Confirm script exit code:
   - `0` expected for `GO`
   - non-zero expected for `NO_GO`
2. Read `startup_go_no_go.json` and confirm:
   - all checks are `passed`
   - `blocking_reasons` is empty for `GO`
3. Confirm required analyst artifacts exist:
   - `startup_review_queue.json`
   - `startup_review_queue.csv`
   - `startup_investigation_report.json`
   - `startup_investigation_report.html`
4. If decision is `NO_GO`, assign remediation owner(s) from `blocking_reasons` before retrying startup.

## Validation Run (2026-03-25)

Executed:

```bash
scripts/run_startup_workflow.sh --run-date 20260325 --run-id startup_20260325_day4_validation --top 25
```

Observed:

- command exit code: `0`
- decision: `GO`
- go/no-go artifact:
  `data/startup_runs/20260325/startup_20260325_day4_validation/outputs/startup_go_no_go.json`
- summary artifact:
  `data/startup_runs/20260325/startup_20260325_day4_validation/outputs/startup_run_summary.json`
