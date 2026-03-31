#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/run_promotion_gate_ci.sh --request-dir <path> [options]

Run promotion-gate in CI/pipeline mode and fail fast on unsafe promotion requests.

Required:
  --request-dir <path>              Promotion request bundle directory.

Optional:
  --activation-started-at-utc <ts>  UTC ISO-8601 timestamp. Defaults to current UTC.
  --gate-run-id <id>                Optional deterministic gate run id.
  --out <path>                      Optional path to write gate JSON payload.
  --accessdane-bin <path>           accessdane binary path (default: .venv/bin/accessdane).
  --help                            Show this message.

Environment:
  ACCESSDANE_ENVIRONMENT and full environment profile keys must be configured.
EOF
}

REQUEST_DIR=""
ACTIVATION_STARTED_AT_UTC=""
GATE_RUN_ID=""
OUT_PATH=""
ACCESSDANE_BIN=".venv/bin/accessdane"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --request-dir)
      REQUEST_DIR="${2:-}"
      shift 2
      ;;
    --activation-started-at-utc)
      ACTIVATION_STARTED_AT_UTC="${2:-}"
      shift 2
      ;;
    --gate-run-id)
      GATE_RUN_ID="${2:-}"
      shift 2
      ;;
    --out)
      OUT_PATH="${2:-}"
      shift 2
      ;;
    --accessdane-bin)
      ACCESSDANE_BIN="${2:-}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "${REQUEST_DIR}" ]]; then
  echo "--request-dir is required." >&2
  usage >&2
  exit 2
fi

if [[ ! -d "${REQUEST_DIR}" ]]; then
  echo "request-dir does not exist or is not a directory: ${REQUEST_DIR}" >&2
  exit 2
fi

if [[ ! -x "${ACCESSDANE_BIN}" ]]; then
  echo "accessdane binary is not executable: ${ACCESSDANE_BIN}" >&2
  exit 2
fi

if [[ -z "${ACTIVATION_STARTED_AT_UTC}" ]]; then
  ACTIVATION_STARTED_AT_UTC="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
fi

cmd=(
  "${ACCESSDANE_BIN}"
  promotion-gate
  --request-dir "${REQUEST_DIR}"
  --activation-started-at-utc "${ACTIVATION_STARTED_AT_UTC}"
)

if [[ -n "${GATE_RUN_ID}" ]]; then
  cmd+=(--gate-run-id "${GATE_RUN_ID}")
fi

if [[ -n "${OUT_PATH}" ]]; then
  cmd+=(--out "${OUT_PATH}")
fi

echo "Running promotion-policy gate for request bundle: ${REQUEST_DIR}" >&2
"${cmd[@]}"
