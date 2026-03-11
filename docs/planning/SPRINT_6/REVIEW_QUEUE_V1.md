# Sprint 6 Review Queue + Batch Reporting v1 Contract

Prepared on: 2026-03-11

## Purpose

Define the first stable contract for ranked investigation queue output and export/report payloads before Day 6 implementation.

This contract locks v1 behavior for:

- top-N queue selection rules
- deterministic sorting and tie-breakers
- scope filters and pagination limits
- JSON and CSV export/report payload shape
- comparability metadata across reruns/version tags
- diagnostics for filtered/skipped rows

## Scope

In scope for v1:

- one command surface for ranked queue generation from `fraud_scores` + `fraud_flags`
- JSON payload for analyst/API handoff
- optional CSV export with stable column order
- queue rows that include enough identifiers for dossier drill-in
- deterministic results for unchanged source runs and request filters

Out of scope for v1:

- interactive dashboard rendering
- case-review writeback workflows (`case_reviews`, Day 8+)
- automated threshold tuning
- cross-jurisdiction custom ranking logic

## Design Goals

- Prioritize review candidates by transparent, deterministic ranking.
- Preserve traceability to concrete score rows and reason-code evidence.
- Keep exports stable enough for analyst handoff and rerun comparison.
- Make queue filtering/skipping explicit through diagnostics.

## Baseline Assumptions

- Sprint 5 scoring objects exist and are populated:
  - `scoring_runs`
  - `fraud_scores`
  - `fraud_flags`
  - `parcel_features`
  - `parcel_year_facts`
- Dossier command exists and can be run independently:
  - `accessdane parcel-dossier --id <parcel_id> ...`
- v1 queue generation is read-only (no DB writes required).

## Command Contract

## `accessdane review-queue`

### CLI shape

```bash
accessdane review-queue \
  [--top <n>] \
  [--page <page>] \
  [--page-size <size>] \
  [--id <parcel_id> ...] \
  [--ids <parcel_ids_file>] \
  [--year <year> ...] \
  [--feature-version <feature_version>] \
  [--ruleset-version <ruleset_version>] \
  [--risk-band <high|medium|low> ...] \
  [--requires-review-only / --all-scores] \
  [--out <json_path>] \
  [--csv-out <csv_path>]
```

### Options and defaults

- `--top` (optional; default `100`; min `1`; max `1000`)
  - returns first N ranked rows for requested scope
- `--page` and `--page-size` (optional pagination mode)
  - `page` default `1`, min `1`
  - `page-size` default `100`, min `1`, max `500`
  - `--top` cannot be combined with `--page` or `--page-size`
- `--id` / `--ids` (optional parcel scope)
  - same semantics as other commands; dedupe + preserve stable sorted output scope
- `--year` (optional repeatable filter)
  - normalized to sorted unique year list
- `--feature-version` (optional; default `feature_v1`)
- `--ruleset-version` (optional; default `scoring_rules_v1`)
- `--risk-band` (optional repeatable)
  - allowed values: `high`, `medium`, `low`
- `--requires-review-only` / `--all-scores`
  - default mode is `--requires-review-only`
- `--out` (optional JSON output path)
- `--csv-out` (optional CSV output path)

### Exit behavior

- Success (`exit 0`): payload emitted/written with required keys.
- Request failure (`exit 1`): payload includes `run.status = failed` and `error`.
- Invalid CLI option values (`exit 2`): Typer/Click validation error.

Failure codes (v1):

- `unsupported_version_selector`
- `invalid_scope`
- `source_query_error`

## Ranking Contract

Queue candidate rows are pulled from `fraud_scores` filtered by requested scope/options.

Deterministic ranking key (highest priority first):

1. `score_value DESC`
2. `reason_code_count DESC`
3. risk-band precedence: `high`, `medium`, `low`
4. `year DESC`
5. `parcel_id ASC`
6. `score_id ASC`

`queue_rank` is 1-based and assigned after filtering and sorting.

## Output Payload Contract (JSON)

Canonical top-level key order:

1. `run`
2. `request`
3. `summary`
4. `rows`
5. `diagnostics`
6. `error`

## `run`

- `run_id`: `null` (read-only v1 command)
- `run_persisted`: `false`
- `run_type`: `review_queue`
- `version_tag`: `review_queue_v1`
- `status`: `succeeded` or `failed`

## `request`

- `top`
- `page`
- `page_size`
- `parcel_ids` (normalized list or `null`)
- `years` (normalized list; empty list means unscoped)
- `feature_version`
- `ruleset_version`
- `risk_bands` (normalized list; empty list means unscoped)
- `requires_review_only` (bool)

## `summary`

- `candidate_count` (pre-filter score rows in selected run/version scope)
- `filtered_count`
- `returned_count`
- `truncated` (bool)
- `page`
- `page_size`
- `total_pages`

## `rows`

Each row contains:

- `queue_rank`
- `score_id`
- `score_run_id`
- `feature_run_id`
- `parcel_id`
- `year`
- `score_value` (decimal string)
- `risk_band`
- `requires_review`
- `reason_code_count`
- `primary_reason_code` (nullable string)
- `primary_reason_weight` (nullable decimal string)
- `municipality_name` (nullable string)
- `valuation_classification` (nullable string)
- `dossier_args` (object)
  - `parcel_id`
  - `year`
  - `feature_version`
  - `ruleset_version`

## `diagnostics`

- `filtered_reason_counts` (object)
  - keys may include:
    - `filtered_by_parcel_id`
    - `filtered_by_year`
    - `filtered_by_risk_band`
    - `filtered_requires_review`
    - `filtered_by_feature_version`
    - `filtered_by_ruleset_version`
- `skipped_row_counts` (object)
  - keys may include:
    - `missing_primary_reason_code`
    - `missing_parcel_year_fact`
- `comparability` (object)
  - `comparable` (bool)
  - `queue_contract_version` (`review_queue_v1`)
  - `comparison_key` (stable object)
    - `feature_version`
    - `ruleset_version`
    - `requires_review_only`
    - `risk_bands`
    - `years`
    - `sort_key_version` (`review_queue_sort_v1`)

## `error`

- `null` when run succeeded
- object when failed:
  - `code`
  - `message`

## CSV Export Contract

`--csv-out` writes one row per queue row with fixed column order:

1. `queue_rank`
2. `score_id`
3. `score_run_id`
4. `feature_run_id`
5. `parcel_id`
6. `year`
7. `score_value`
8. `risk_band`
9. `requires_review`
10. `reason_code_count`
11. `primary_reason_code`
12. `primary_reason_weight`
13. `municipality_name`
14. `valuation_classification`
15. `dossier_parcel_id`
16. `dossier_year`
17. `dossier_feature_version`
18. `dossier_ruleset_version`

CSV and JSON row counts must match for the same command invocation.

## Comparability Semantics

Two queue exports are comparable only if all are true:

- same `queue_contract_version`
- same `sort_key_version`
- same `feature_version`
- same `ruleset_version`
- same filter scope (`parcel_ids`, `years`, `risk_bands`, review-only mode)

If any differ, output is valid but not directly comparable for rank drift interpretation.

## Null And Empty Semantics

- Decimal values serialize as strings in JSON and CSV.
- Arrays are never `null`; use `[]` for empty.
- Empty successful queue:
  - `run.status = succeeded`
  - `rows = []`
  - `summary.returned_count = 0`
  - `error = null`

## Scoped Validation Fixtures (Day 6 Test Targets)

### Fixture A: Deterministic tie-break ordering

- same score value/reason count across multiple parcels
- verify full deterministic tie-break key and stable rank assignment

### Fixture B: Filters + pagination

- apply `--year`, `--risk-band`, and `--requires-review-only`
- verify `filtered_reason_counts`, `returned_count`, `total_pages`

### Fixture C: JSON + CSV parity

- run with `--out` and `--csv-out`
- verify row-count parity and field-level agreement on core columns

### Fixture D: Comparability metadata

- run twice with identical filters and once with changed version selector
- verify comparability block flips as expected

### Fixture E: Empty scope behavior

- filters that return no rows
- verify success payload with empty rows and non-error status

## Non-Goals For Day 5

- Implementing queue/report code paths.
- Adding migrations or persistence for case reviews.
- Implementing analyst UI surface (Day 7).
