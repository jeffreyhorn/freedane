#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

ACCESSDANE_BIN="${ACCESSDANE_BIN:-.venv/bin/accessdane}"
PYTHON_BIN="${ACCESSDANE_PYTHON_BIN:-.venv/bin/python}"
RUN_DATE="${ACCESSDANE_RUN_DATE:-$(date -u +%Y%m%d)}"
PROFILE_NAME="${ACCESSDANE_REFRESH_PROFILE:-daily_refresh}"
FEATURE_VERSION="${ACCESSDANE_FEATURE_VERSION:-feature_v1}"
RULESET_VERSION="${ACCESSDANE_RULESET_VERSION:-scoring_rules_v1}"
SALES_RATIO_BASE="${ACCESSDANE_SALES_RATIO_BASE:-sales_ratio_v1}"
TOP_N="${ACCESSDANE_REFRESH_TOP:-100}"
STARTUP_ARTIFACT_ROOT="${ACCESSDANE_STARTUP_ARTIFACT_ROOT:-data/startup_runs}"
REFRESH_ARTIFACT_BASE_DIR="${ACCESSDANE_ARTIFACT_BASE_DIR:-data/refresh_runs}"
BENCHMARK_ARTIFACT_BASE_DIR="${ACCESSDANE_BENCHMARK_BASE_DIR:-data/benchmark_packs}"
RETR_FILE="${ACCESSDANE_RETR_FILE:-}"
PERMITS_FILE="${ACCESSDANE_PERMITS_FILE:-}"
APPEALS_FILE="${ACCESSDANE_APPEALS_FILE:-}"
RUN_ID="${ACCESSDANE_STARTUP_RUN_ID:-}"

usage() {
  cat <<'EOF'
Usage: scripts/run_startup_workflow.sh [options]

Automates docs/todo_immediately.md startup steps and emits deterministic
go/no-go artifacts that block analyst intake on failure.

Options:
  --run-date YYYYMMDD                 Run date (default: UTC today)
  --run-id ID                         Startup run ID (default derived from date/profile/versions)
  --profile-name NAME                 Refresh profile (default: daily_refresh)
  --feature-version VERSION           Feature version (default: feature_v1)
  --ruleset-version VERSION           Ruleset version (default: scoring_rules_v1)
  --sales-ratio-base VERSION          Sales ratio version tag (default: sales_ratio_v1)
  --top N                             Top-N review/report count (default: ACCESSDANE_REFRESH_TOP or 100)
  --startup-artifact-root PATH        Startup artifact root (default: data/startup_runs)
  --refresh-artifact-base-dir PATH    Refresh artifact base dir (default: data/refresh_runs)
  --benchmark-artifact-base-dir PATH  Benchmark artifact base dir (default: data/benchmark_packs)
  --retr-file PATH                    Optional RETR CSV input
  --permits-file PATH                 Optional permits CSV input
  --appeals-file PATH                 Optional appeals CSV input
  --accessdane-bin PATH               accessdane executable (default: .venv/bin/accessdane)
  --python-bin PATH                   Python executable for summary generation
  -h, --help                          Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-date)
      RUN_DATE="$2"
      shift 2
      ;;
    --run-id)
      RUN_ID="$2"
      shift 2
      ;;
    --profile-name)
      PROFILE_NAME="$2"
      shift 2
      ;;
    --feature-version)
      FEATURE_VERSION="$2"
      shift 2
      ;;
    --ruleset-version)
      RULESET_VERSION="$2"
      shift 2
      ;;
    --sales-ratio-base)
      SALES_RATIO_BASE="$2"
      shift 2
      ;;
    --top)
      TOP_N="$2"
      shift 2
      ;;
    --startup-artifact-root)
      STARTUP_ARTIFACT_ROOT="$2"
      shift 2
      ;;
    --refresh-artifact-base-dir)
      REFRESH_ARTIFACT_BASE_DIR="$2"
      shift 2
      ;;
    --benchmark-artifact-base-dir)
      BENCHMARK_ARTIFACT_BASE_DIR="$2"
      shift 2
      ;;
    --retr-file)
      RETR_FILE="$2"
      shift 2
      ;;
    --permits-file)
      PERMITS_FILE="$2"
      shift 2
      ;;
    --appeals-file)
      APPEALS_FILE="$2"
      shift 2
      ;;
    --accessdane-bin)
      ACCESSDANE_BIN="$2"
      shift 2
      ;;
    --python-bin)
      PYTHON_BIN="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! [[ "${RUN_DATE}" =~ ^[0-9]{8}$ ]]; then
  echo "Invalid --run-date '${RUN_DATE}'; expected YYYYMMDD." >&2
  exit 2
fi

if ! [[ "${TOP_N}" =~ ^[0-9]+$ ]] || [[ "${TOP_N}" -lt 1 ]]; then
  echo "Invalid --top '${TOP_N}'; expected positive integer." >&2
  exit 2
fi

if [[ -z "${RUN_ID}" ]]; then
  RUN_ID="startup_${RUN_DATE}_${PROFILE_NAME}_${FEATURE_VERSION}_${RULESET_VERSION}"
fi
if [[ "${RUN_ID}" == "." || "${RUN_ID}" == ".." || "${RUN_ID}" == *".."* || "${RUN_ID}" == *"/"* || "${RUN_ID}" == *"\\"* ]]; then
  echo "Invalid --run-id '${RUN_ID}'; must be a safe single path component." >&2
  exit 2
fi
if ! [[ "${RUN_ID}" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "Invalid --run-id '${RUN_ID}'; only letters, digits, '.', '_' and '-' are allowed." >&2
  exit 2
fi

if [[ ! -x "${ACCESSDANE_BIN}" ]]; then
  echo "accessdane executable not found or not executable at '${ACCESSDANE_BIN}'." >&2
  exit 2
fi

if [[ ! -x "${PYTHON_BIN}" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  else
    echo "Python executable not found at '${PYTHON_BIN}' and python3 is unavailable." >&2
    exit 2
  fi
fi

for optional_file in "${RETR_FILE}" "${PERMITS_FILE}" "${APPEALS_FILE}"; do
  if [[ -n "${optional_file}" ]] && [[ ! -f "${optional_file}" ]]; then
    echo "Optional input file not found: ${optional_file}" >&2
    exit 2
  fi
done

RUN_DIR="${STARTUP_ARTIFACT_ROOT}/${RUN_DATE}/${RUN_ID}"
OUTPUT_DIR="${RUN_DIR}/outputs"
LOG_DIR="${RUN_DIR}/logs"
STEP_RESULTS_FILE="${RUN_DIR}/step_results.tsv"

mkdir -p "${OUTPUT_DIR}" "${LOG_DIR}"
: > "${STEP_RESULTS_FILE}"

SALES_RATIO_OUT="${OUTPUT_DIR}/startup_sales_ratio_study.json"
BUILD_FEATURES_OUT="${OUTPUT_DIR}/startup_build_features.json"
SCORE_FRAUD_OUT="${OUTPUT_DIR}/startup_score_fraud.json"
REVIEW_QUEUE_OUT="${OUTPUT_DIR}/startup_review_queue.json"
REVIEW_QUEUE_CSV_OUT="${OUTPUT_DIR}/startup_review_queue.csv"
REVIEW_FEEDBACK_OUT="${OUTPUT_DIR}/startup_review_feedback.json"
REVIEW_FEEDBACK_SQL_OUT="${OUTPUT_DIR}/startup_review_feedback.sql"
INVESTIGATION_HTML_OUT="${OUTPUT_DIR}/startup_investigation_report.html"
INVESTIGATION_OUT="${OUTPUT_DIR}/startup_investigation_report.json"
PARSER_SNAPSHOT_OUT="${OUTPUT_DIR}/startup_parser_drift_snapshot.json"
LOAD_MONITOR_OUT="${OUTPUT_DIR}/startup_load_monitor.json"
LOAD_MONITOR_ALERT_OUT="${OUTPUT_DIR}/startup_load_monitor_alert.json"
BENCHMARK_OUT="${OUTPUT_DIR}/startup_benchmark_pack.json"
BENCHMARK_TREND_OUT="${OUTPUT_DIR}/startup_benchmark_pack_trend.json"
BENCHMARK_ALERT_OUT="${OUTPUT_DIR}/startup_benchmark_pack_alert.json"
GO_NO_GO_OUT="${OUTPUT_DIR}/startup_go_no_go.json"
SUMMARY_OUT="${OUTPUT_DIR}/startup_run_summary.json"

run_step() {
  local step_id="$1"
  local required="$2"
  shift 2
  local log_path="${LOG_DIR}/${step_id}.log"
  local exit_code=0
  local status="succeeded"
  local cmd_display
  cmd_display="$(printf "%q " "$@")"

  echo "[startup] ${step_id}: running"
  {
    echo "$ ${cmd_display}"
    "$@"
  } >"${log_path}" 2>&1 || exit_code=$?

  if [[ "${exit_code}" -ne 0 ]]; then
    status="failed"
    echo "[startup] ${step_id}: FAILED (exit ${exit_code})"
  else
    echo "[startup] ${step_id}: succeeded"
  fi
  printf "%s\t%s\t%s\t%s\t%s\n" \
    "${step_id}" "${required}" "${status}" "${exit_code}" "${log_path}" >>"${STEP_RESULTS_FILE}"
}

record_skipped_step() {
  local step_id="$1"
  local required="$2"
  local reason="$3"
  echo "[startup] ${step_id}: skipped (${reason})"
  printf "%s\t%s\t%s\t%s\t%s\n" \
    "${step_id}" "${required}" "skipped" "0" "" >>"${STEP_RESULTS_FILE}"
}

run_step "init_db" "true" "${ACCESSDANE_BIN}" "init-db"

if [[ -n "${RETR_FILE}" ]]; then
  run_step "ingest_retr" "true" "${ACCESSDANE_BIN}" "ingest-retr" "--file" "${RETR_FILE}"
else
  record_skipped_step "ingest_retr" "false" "no --retr-file provided"
fi

run_step "match_sales" "true" "${ACCESSDANE_BIN}" "match-sales"

if [[ -n "${PERMITS_FILE}" ]]; then
  run_step "ingest_permits" "true" "${ACCESSDANE_BIN}" "ingest-permits" "--file" "${PERMITS_FILE}"
else
  record_skipped_step "ingest_permits" "false" "no --permits-file provided"
fi

if [[ -n "${APPEALS_FILE}" ]]; then
  run_step "ingest_appeals" "true" "${ACCESSDANE_BIN}" "ingest-appeals" "--file" "${APPEALS_FILE}"
else
  record_skipped_step "ingest_appeals" "false" "no --appeals-file provided"
fi

run_step "build_parcel_year_facts" "true" "${ACCESSDANE_BIN}" "build-parcel-year-facts"

run_step \
  "sales_ratio_study" \
  "true" \
  "${ACCESSDANE_BIN}" "sales-ratio-study" \
  "--version-tag" "${SALES_RATIO_BASE}" \
  "--out" "${SALES_RATIO_OUT}"

run_step \
  "build_features" \
  "true" \
  "${ACCESSDANE_BIN}" "build-features" \
  "--feature-version" "${FEATURE_VERSION}" \
  "--out" "${BUILD_FEATURES_OUT}"

run_step \
  "score_fraud" \
  "true" \
  "${ACCESSDANE_BIN}" "score-fraud" \
  "--feature-version" "${FEATURE_VERSION}" \
  "--ruleset-version" "${RULESET_VERSION}" \
  "--out" "${SCORE_FRAUD_OUT}"

run_step \
  "review_queue" \
  "true" \
  "${ACCESSDANE_BIN}" "review-queue" \
  "--top" "${TOP_N}" \
  "--feature-version" "${FEATURE_VERSION}" \
  "--ruleset-version" "${RULESET_VERSION}" \
  "--out" "${REVIEW_QUEUE_OUT}" \
  "--csv-out" "${REVIEW_QUEUE_CSV_OUT}"

run_step \
  "review_feedback" \
  "true" \
  "${ACCESSDANE_BIN}" "review-feedback" \
  "--feature-version" "${FEATURE_VERSION}" \
  "--ruleset-version" "${RULESET_VERSION}" \
  "--out" "${REVIEW_FEEDBACK_OUT}" \
  "--sql-out" "${REVIEW_FEEDBACK_SQL_OUT}"

run_step \
  "investigation_report" \
  "true" \
  "${ACCESSDANE_BIN}" "investigation-report" \
  "--top" "${TOP_N}" \
  "--feature-version" "${FEATURE_VERSION}" \
  "--ruleset-version" "${RULESET_VERSION}" \
  "--html-out" "${INVESTIGATION_HTML_OUT}" \
  "--out" "${INVESTIGATION_OUT}"

run_step \
  "parser_drift_snapshot" \
  "true" \
  "${ACCESSDANE_BIN}" "parser-drift-snapshot" \
  "--profile-name" "${PROFILE_NAME}" \
  "--run-date" "${RUN_DATE}" \
  "--feature-version" "${FEATURE_VERSION}" \
  "--ruleset-version" "${RULESET_VERSION}" \
  "--artifact-root" "${REFRESH_ARTIFACT_BASE_DIR}" \
  "--out" "${PARSER_SNAPSHOT_OUT}"

run_step \
  "load_monitor" \
  "true" \
  "${ACCESSDANE_BIN}" "load-monitor" \
  "--artifact-base-dir" "${REFRESH_ARTIFACT_BASE_DIR}" \
  "--profile-name" "${PROFILE_NAME}" \
  "--feature-version" "${FEATURE_VERSION}" \
  "--ruleset-version" "${RULESET_VERSION}" \
  "--out" "${LOAD_MONITOR_OUT}" \
  "--alert-out" "${LOAD_MONITOR_ALERT_OUT}"

run_step \
  "benchmark_pack" \
  "true" \
  "${ACCESSDANE_BIN}" "benchmark-pack" \
  "--run-date" "${RUN_DATE}" \
  "--run-id" "${RUN_ID}_benchmark" \
  "--profile-name" "${PROFILE_NAME}" \
  "--feature-version" "${FEATURE_VERSION}" \
  "--ruleset-version" "${RULESET_VERSION}" \
  "--top-n" "${TOP_N}" \
  "--artifact-base-dir" "${BENCHMARK_ARTIFACT_BASE_DIR}" \
  "--out" "${BENCHMARK_OUT}" \
  "--trend-out" "${BENCHMARK_TREND_OUT}" \
  "--alert-out" "${BENCHMARK_ALERT_OUT}"

"${PYTHON_BIN}" - \
  "${STEP_RESULTS_FILE}" \
  "${GO_NO_GO_OUT}" \
  "${SUMMARY_OUT}" \
  "${RUN_ID}" \
  "${RUN_DATE}" \
  "${PROFILE_NAME}" \
  "${FEATURE_VERSION}" \
  "${RULESET_VERSION}" \
  "${SALES_RATIO_BASE}" \
  "${TOP_N}" \
  "${RETR_FILE}" \
  "${PERMITS_FILE}" \
  "${APPEALS_FILE}" \
  "${SALES_RATIO_OUT}" \
  "${BUILD_FEATURES_OUT}" \
  "${SCORE_FRAUD_OUT}" \
  "${REVIEW_QUEUE_OUT}" \
  "${REVIEW_QUEUE_CSV_OUT}" \
  "${REVIEW_FEEDBACK_OUT}" \
  "${REVIEW_FEEDBACK_SQL_OUT}" \
  "${INVESTIGATION_OUT}" \
  "${INVESTIGATION_HTML_OUT}" \
  "${LOAD_MONITOR_OUT}" \
  "${LOAD_MONITOR_ALERT_OUT}" \
  "${BENCHMARK_OUT}" \
  "${BENCHMARK_TREND_OUT}" \
  "${BENCHMARK_ALERT_OUT}" \
  "${PARSER_SNAPSHOT_OUT}" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    step_results_path,
    go_no_go_path,
    summary_path,
    run_id,
    run_date,
    profile_name,
    feature_version,
    ruleset_version,
    sales_ratio_base,
    top_n,
    retr_file,
    permits_file,
    appeals_file,
    sales_ratio_out,
    build_features_out,
    score_fraud_out,
    review_queue_out,
    review_queue_csv_out,
    review_feedback_out,
    review_feedback_sql_out,
    investigation_out,
    investigation_html_out,
    load_monitor_out,
    load_monitor_alert_out,
    benchmark_out,
    benchmark_trend_out,
    benchmark_alert_out,
    parser_snapshot_out,
) = sys.argv[1:]


def _load_json(path_text: str) -> dict:
    path = Path(path_text)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


steps: list[dict[str, object]] = []
step_by_id: dict[str, dict[str, object]] = {}
for line in Path(step_results_path).read_text(encoding="utf-8").splitlines():
    if not line.strip():
        continue
    step_id, required, status, exit_code, log_path = line.split("\t", 4)
    item = {
        "step_id": step_id,
        "required": required == "true",
        "status": status,
        "exit_code": int(exit_code),
        "log_path": log_path,
    }
    steps.append(item)
    step_by_id[step_id] = item


def step_ok(step_id: str) -> bool:
    item = step_by_id.get(step_id)
    return bool(item and item.get("status") == "succeeded")


def required_step_ids(step_ids: list[str]) -> list[str]:
    selected: list[str] = []
    for step_id in step_ids:
        item = step_by_id.get(step_id)
        if item and item.get("required") is True:
            selected.append(step_id)
    return selected


checks: list[dict[str, object]] = []
blocking_reasons: list[str] = []

init_db_ok = step_ok("init_db")
checks.append(
    {
        "check_id": "init_db_succeeded",
        "passed": init_db_ok,
        "details": "init-db completed at migration head.",
    }
)
if not init_db_ok:
    blocking_reasons.append("init-db failed; database is not ready for analyst intake.")

data_load_candidates = [
    "ingest_retr",
    "match_sales",
    "ingest_permits",
    "ingest_appeals",
    "build_parcel_year_facts",
]
data_load_required = required_step_ids(data_load_candidates)
data_load_ok = all(step_ok(step_id) for step_id in data_load_required)
checks.append(
    {
        "check_id": "data_load_steps_succeeded",
        "passed": data_load_ok,
        "details": "Required ingest/build steps completed without blocking errors.",
        "required_steps": data_load_required,
    }
)
if not data_load_ok:
    failed = [step_id for step_id in data_load_required if not step_ok(step_id)]
    blocking_reasons.append(
        "Data load stage failed for required steps: " + ", ".join(sorted(failed))
    )

scoring_steps = ["sales_ratio_study", "build_features", "score_fraud"]
scoring_ok = all(step_ok(step_id) for step_id in scoring_steps)
checks.append(
    {
        "check_id": "scoring_pipeline_succeeded",
        "passed": scoring_ok,
        "details": "sales-ratio-study -> build-features -> score-fraud completed.",
        "required_steps": scoring_steps,
    }
)
if not scoring_ok:
    failed = [step_id for step_id in scoring_steps if not step_ok(step_id)]
    blocking_reasons.append(
        "Scoring pipeline failed for steps: " + ", ".join(sorted(failed))
    )

required_artifacts = [
    review_queue_out,
    review_queue_csv_out,
    investigation_out,
    investigation_html_out,
]
missing_artifacts = [path for path in required_artifacts if not Path(path).is_file()]
artifacts_ok = not missing_artifacts
checks.append(
    {
        "check_id": "required_queue_and_report_artifacts_exist",
        "passed": artifacts_ok,
        "details": "review-queue and investigation-report outputs exist.",
        "required_artifacts": required_artifacts,
    }
)
if not artifacts_ok:
    blocking_reasons.append(
        "Required startup artifacts missing: " + ", ".join(sorted(missing_artifacts))
    )

monitoring_required = ["parser_drift_snapshot", "load_monitor", "benchmark_pack"]
monitoring_steps_ok = all(step_ok(step_id) for step_id in monitoring_required)
load_monitor_payload = _load_json(load_monitor_out)
load_monitor_run_status = (
    load_monitor_payload.get("run", {}).get("status")
    if isinstance(load_monitor_payload.get("run"), dict)
    else None
)
load_monitor_severity = (
    load_monitor_payload.get("summary", {}).get("overall_severity")
    if isinstance(load_monitor_payload.get("summary"), dict)
    else None
)
benchmark_payload = _load_json(benchmark_out)
benchmark_overall_severity = (
    benchmark_payload.get("comparison", {}).get("overall_severity")
    if isinstance(benchmark_payload.get("comparison"), dict)
    else None
)
benchmark_run_status = (
    benchmark_payload.get("run", {}).get("status")
    if isinstance(benchmark_payload.get("run"), dict)
    else None
)
parser_payload = _load_json(parser_snapshot_out)
parser_run_status = (
    parser_payload.get("run", {}).get("status")
    if isinstance(parser_payload.get("run"), dict)
    else None
)
load_monitor_alert_payload = _load_json(load_monitor_alert_out)
load_monitor_alert_severity = (
    load_monitor_alert_payload.get("alert", {}).get("severity")
    if isinstance(load_monitor_alert_payload.get("alert"), dict)
    else None
)
benchmark_alert_payload = _load_json(benchmark_alert_out)
benchmark_alert_critical = False
alerts = benchmark_alert_payload.get("alerts")
if isinstance(alerts, list):
    for alert in alerts:
        if isinstance(alert, dict) and alert.get("severity") == "critical":
            benchmark_alert_critical = True
            break

critical_alerts = []
if load_monitor_severity == "critical":
    critical_alerts.append("load_monitor.summary.overall_severity=critical")
if load_monitor_alert_severity == "critical":
    critical_alerts.append("load_monitor.alert.severity=critical")
if benchmark_overall_severity == "critical":
    critical_alerts.append("benchmark_pack.comparison.overall_severity=critical")
if benchmark_alert_critical:
    critical_alerts.append("benchmark_pack.alerts contains critical severity")

monitoring_ok = (
    monitoring_steps_ok
    and load_monitor_run_status == "succeeded"
    and benchmark_run_status == "succeeded"
    and parser_run_status == "succeeded"
    and not critical_alerts
)
checks.append(
    {
        "check_id": "monitoring_has_no_unresolved_critical_alerts",
        "passed": monitoring_ok,
        "details": "parser/load/benchmark diagnostics succeeded with no critical alerts.",
        "required_steps": monitoring_required,
        "critical_alerts": critical_alerts,
    }
)
if not monitoring_ok:
    if not monitoring_steps_ok:
        failed = [step_id for step_id in monitoring_required if not step_ok(step_id)]
        blocking_reasons.append(
            "Monitoring stage failed for steps: " + ", ".join(sorted(failed))
        )
    if load_monitor_run_status != "succeeded":
        blocking_reasons.append(
            "load-monitor did not succeed; startup health is unresolved."
        )
    if benchmark_run_status != "succeeded":
        blocking_reasons.append(
            "benchmark-pack did not succeed; startup health is unresolved."
        )
    if parser_run_status != "succeeded":
        blocking_reasons.append(
            "parser-drift-snapshot did not succeed; parser diagnostics unresolved."
        )
    if critical_alerts:
        blocking_reasons.append(
            "Critical monitoring alerts detected: " + ", ".join(critical_alerts)
        )

required_failures = sorted(
    step["step_id"]
    for step in steps
    if step.get("required") is True and step.get("status") != "succeeded"
)
if required_failures:
    checks.append(
        {
            "check_id": "all_required_steps_succeeded",
            "passed": False,
            "details": "One or more required startup steps failed.",
            "failed_steps": required_failures,
        }
    )
else:
    checks.append(
        {
            "check_id": "all_required_steps_succeeded",
            "passed": True,
            "details": "All required startup steps succeeded.",
        }
    )

decision = "GO" if all(bool(check.get("passed")) for check in checks) else "NO_GO"
go_no_go_payload = {
    "run_id": run_id,
    "run_date": run_date,
    "decision": decision,
    "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    "checks": checks,
    "blocking_reasons": sorted(set(blocking_reasons)),
}
Path(go_no_go_path).write_text(json.dumps(go_no_go_payload, indent=2), encoding="utf-8")

summary_payload = {
    "run": {
        "run_id": run_id,
        "run_date": run_date,
        "profile_name": profile_name,
        "feature_version": feature_version,
        "ruleset_version": ruleset_version,
        "sales_ratio_base": sales_ratio_base,
        "top_n": int(top_n),
        "status": "succeeded" if decision == "GO" else "failed",
    },
    "inputs": {
        "retr_file": retr_file or None,
        "permits_file": permits_file or None,
        "appeals_file": appeals_file or None,
    },
    "steps": steps,
    "artifacts": {
        "sales_ratio_study": sales_ratio_out,
        "build_features": build_features_out,
        "score_fraud": score_fraud_out,
        "review_queue_json": review_queue_out,
        "review_queue_csv": review_queue_csv_out,
        "investigation_report_json": investigation_out,
        "investigation_report_html": investigation_html_out,
        "review_feedback_json": review_feedback_out,
        "review_feedback_sql": review_feedback_sql_out,
        "parser_drift_snapshot": parser_snapshot_out,
        "load_monitor": load_monitor_out,
        "load_monitor_alert": load_monitor_alert_out,
        "benchmark_pack": benchmark_out,
        "benchmark_trend": benchmark_trend_out,
        "benchmark_alert": benchmark_alert_out,
        "go_no_go": go_no_go_path,
    },
    "go_no_go": go_no_go_payload,
}
Path(summary_path).write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
PY

DECISION="$("${PYTHON_BIN}" - <<'PY' "${GO_NO_GO_OUT}"
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
print(str(payload.get("decision", "NO_GO")))
PY
)"

if [[ "${DECISION}" == "GO" ]]; then
  echo "STARTUP GO: analyst intake unblocked."
  echo "go/no-go artifact: ${GO_NO_GO_OUT}"
  echo "startup summary: ${SUMMARY_OUT}"
  exit 0
fi

echo "STARTUP NO-GO: analyst intake remains blocked." >&2
echo "go/no-go artifact: ${GO_NO_GO_OUT}" >&2
echo "startup summary: ${SUMMARY_OUT}" >&2
"${PYTHON_BIN}" - <<'PY' "${GO_NO_GO_OUT}" >&2
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
for reason in payload.get("blocking_reasons", []):
    print(f"- {reason}")
PY
exit 1
