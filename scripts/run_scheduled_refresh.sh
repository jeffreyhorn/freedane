#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

ACCESSDANE_BIN="${ACCESSDANE_BIN:-.venv/bin/accessdane}"
PROFILE_NAME="${ACCESSDANE_REFRESH_PROFILE:-daily_refresh}"
FEATURE_VERSION="${ACCESSDANE_FEATURE_VERSION:-feature_v1}"
RULESET_VERSION="${ACCESSDANE_RULESET_VERSION:-scoring_rules_v1}"
SALES_RATIO_BASE="${ACCESSDANE_SALES_RATIO_BASE:-sales_ratio_v1}"
TOP_N="${ACCESSDANE_REFRESH_TOP:-100}"
ARTIFACT_BASE_DIR="${ACCESSDANE_ARTIFACT_BASE_DIR:-data/refresh_runs}"
LOG_BASE_DIR="${ACCESSDANE_REFRESH_LOG_DIR:-${ARTIFACT_BASE_DIR}/scheduler_logs}"
RUN_DATE="${ACCESSDANE_RUN_DATE:-$(date -u +%Y%m%d)}"
RUN_TIME_UTC="$(date -u +%H%M%S)"
RUN_ID="${ACCESSDANE_RUN_ID:-${RUN_DATE}_${PROFILE_NAME}_${FEATURE_VERSION}_${RULESET_VERSION}_${RUN_TIME_UTC}}"
OUTPUT_JSON="${LOG_BASE_DIR}/${RUN_ID}.json"

mkdir -p "${LOG_BASE_DIR}"

CMD=(
  "${ACCESSDANE_BIN}"
  "refresh-runner"
  "--profile-name" "${PROFILE_NAME}"
  "--run-date" "${RUN_DATE}"
  "--run-id" "${RUN_ID}"
  "--feature-version" "${FEATURE_VERSION}"
  "--ruleset-version" "${RULESET_VERSION}"
  "--sales-ratio-base" "${SALES_RATIO_BASE}"
  "--top" "${TOP_N}"
  "--artifact-base-dir" "${ARTIFACT_BASE_DIR}"
  "--accessdane-bin" "${ACCESSDANE_BIN}"
  "--out" "${OUTPUT_JSON}"
)

if [[ -n "${ACCESSDANE_RETR_FILE:-}" ]]; then
  CMD+=("--retr-file" "${ACCESSDANE_RETR_FILE}")
fi
if [[ -n "${ACCESSDANE_PERMITS_FILE:-}" ]]; then
  CMD+=("--permits-file" "${ACCESSDANE_PERMITS_FILE}")
fi
if [[ -n "${ACCESSDANE_APPEALS_FILE:-}" ]]; then
  CMD+=("--appeals-file" "${ACCESSDANE_APPEALS_FILE}")
fi
if [[ -n "${ACCESSDANE_RETRY_STAGE:-}" ]]; then
  RETRY_ATTEMPT="${ACCESSDANE_RETRY_ATTEMPT:-2}"
  CMD+=("--attempt-count" "${RETRY_ATTEMPT}")
  CMD+=("--retried-from-stage-id" "${ACCESSDANE_RETRY_STAGE}")
fi

"${CMD[@]}"
echo "refresh-runner output written to ${OUTPUT_JSON}"
