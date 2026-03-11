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
  [--requires-review-only | --all-scores] \
  [--out <json_path>] \
  [--csv-out <csv_path>]
```

### Options and defaults

- `--top` (optional; top-N mode; default `100` when pagination flags are omitted; min `1`; max `1000`)
  - In top-N mode (default), neither `--page` nor `--page-size` is provided and the command returns the first N ranked rows for the requested scope.
  - `--top` may be set explicitly, or omitted to use the default `100`, but only when pagination flags are not used.
  - Providing `--top` together with `--page` or `--page-size` is an invalid CLI option combination (`exit 2`, no JSON payload).
- `--page` and `--page-size` (optional; pagination mode)
  - Pagination mode is activated when either `--page` or `--page-size` is provided.
  - `page` default `1`, min `1`
  - `page-size` default `100`, min `1`, max `500`
  - In pagination mode, `--top` must be omitted and treated as `null`; if provided, fail as the invalid CLI combination above.
- `--id` / `--ids` (optional parcel scope)
  - normalization: collect all IDs from `--id` and `--ids`, trim whitespace, drop empty values, de-duplicate by normalized ID, then sort lexicographically for stable scope evaluation/output
  - if `--id` and/or `--ids` was explicitly provided but normalized IDs are empty, treat as CLI validation error (`exit 2`, no JSON payload)
- `--year` (optional repeatable filter)
  - normalized to sorted unique year list
- `--feature-version` (optional; default `feature_v1`)
- `--ruleset-version` (optional; default `scoring_rules_v1`)
- `--risk-band` (optional repeatable)
  - allowed values: `high`, `medium`, `low`
- `--requires-review-only` / `--all-scores` (mutually exclusive)
  - default mode is `--requires-review-only`
  - if both flags are provided, treat as invalid CLI option combination (`exit 2`, no JSON payload)
- `--out` (optional JSON output path)
- `--csv-out` (optional CSV output path)

### Exit behavior

- Success (`exit 0`): payload emitted/written with required keys.
- Request failure (`exit 1`): payload includes `run.status = failed` and `error`.
  - These failures are emitted only after CLI parsing/validation succeeds.
- Invalid CLI option values (`exit 2`): Typer/Click validation error; no JSON payload is emitted (only usage/error text from Typer/Click).
  - Includes mutually exclusive/conflicting flag combinations such as:
    - providing both `--requires-review-only` and `--all-scores`
    - providing `--top` together with `--page` or `--page-size`
  - Also includes other Typer/Click validation failures at parse time.
  - These conditions are never mapped to JSON `error.code` values (including `invalid_scope`).
  - Fixture expectation: assert exit code and CLI error text only; no JSON failure payload and no `error.code`.

Failure codes (v1):

- All failure codes below correspond to `exit 1` and a JSON failure payload.
- `unsupported_version_selector`
- `invalid_scope`
  - Request passed CLI parsing/validation but defines a logically invalid/unsupported scope.
  - Must not be used for Typer/Click validation errors or flag-conflict parse failures.
- `source_query_error`

## Ranking Contract

Queue rows originate from `fraud_scores` where `feature_version` and
`ruleset_version` match the request. This version-matched set is the
`candidate_count` baseline before optional request filters are applied
(`parcel_ids`, `years`, `risk_bands`, `requires_review_only`).

Deterministic ranking key (highest priority first):

1. `score_value DESC`
2. `reason_code_count DESC`
3. `risk_band` precedence: `high`, `medium`, `low` (defensive tie-break only; with current
   `scoring_rules_v1` this is functionally redundant because `risk_band` is derived
   from `score_value`, but retained for forward compatibility with future rulesets)
4. `year DESC`
5. `parcel_id ASC`
6. `score_id ASC`

`queue_rank` is 1-based global rank within the fully filtered and sorted result set,
assigned before top/page slicing. It does not reset per page.

## Output Payload Contract (JSON)

Canonical top-level key order:

1. `run`
2. `request`
3. `summary`
4. `rows`
5. `diagnostics`
6. `error`

### Presence Requirements By `run.status`

The key order above is canonical ordering only. Presence rules:

- When `run.status = "succeeded"`:
  - MUST include: `run`, `request`, `summary`, `rows`, `diagnostics`, `error`
  - `error` MUST be `null`
- When `run.status = "failed"`:
  - MUST include: `run`, `request`, `error`
  - MAY include: `diagnostics`
  - MUST NOT include: `summary`, `rows`

Producers MUST NOT emit any other `run.status` values in v1.

## `run`

- `run_id`: `null` (read-only v1 command)
- `run_persisted`: `false`
- `run_type`: `review_queue`
- `version_tag`: `review_queue_v1`
- `status`: `succeeded` or `failed`

## `request`

- `top` (nullable int)
  - top-N mode: integer value (explicit or default `100`)
  - pagination mode: `null`
- `page` (nullable int)
  - top-N mode: `null`
  - pagination mode: integer value (default `1`)
- `page_size` (nullable int)
  - top-N mode: `null`
  - pagination mode: integer value (default `100`)
- `parcel_ids` (normalized list; empty list means unscoped)
- `years` (normalized list; empty list means unscoped)
- `feature_version`
- `ruleset_version`
- `risk_bands` (normalized list; empty list means unscoped)
- `requires_review_only` (bool)

## `summary`

- `candidate_count` (version-matched baseline rows before optional request filters)
- `filtered_count` (rows excluded by optional request filters from `candidate_count`)
- `skipped_count` (rows excluded after filtering due to hard-skip checks, e.g., invalid identifiers or duplicate join artifacts)
- `returned_count` (rows emitted after filtering, hard-skip checks, and top/page slicing)
- `truncated` (bool; `true` when `returned_count < (candidate_count - filtered_count - skipped_count)`, else `false`)
- `page` (nullable int)
  - top-N mode: `null`
  - pagination mode: integer value for current page (default `1`)
- `page_size` (nullable int)
  - top-N mode: `null`
  - pagination mode: integer value for effective page size (default `100`)
- `total_pages` (nullable int)
  - top-N mode: `null`
  - pagination mode: integer value computed from `(candidate_count - filtered_count - skipped_count)` and `page_size`

## `rows`

Each row contains:

- `queue_rank`
- `score_id`
- `run_id` (`fraud_scores.run_id`; scoring run identifier for this row)
- `feature_run_id`
- `parcel_id`
- `year`
- `score_value` (decimal string; never `null`)
- `risk_band`
- `requires_review`
- `reason_code_count`
- `primary_reason_code` (nullable string)
- `primary_reason_weight` (decimal string; nullable when there is no primary reason or severity is unknown; when present, formatted with exactly 4 digits after the decimal point)
  - when `primary_reason_code` is `null`: emit `null`
  - when `primary_reason_code` is present and severity is known: emit decimal string quantized to 4 decimal places
  - when `primary_reason_code` is present but severity is unknown: emit `null`
- `municipality_name` (nullable string)
- `valuation_classification` (nullable string)
- `dossier_args` (object)
  - `parcel_id`
  - `year`
  - `feature_version`
  - `ruleset_version`

Rows with missing optional enrichment fields are retained and not dropped.
`municipality_name` and `valuation_classification` emit `null` when unavailable.

## `diagnostics`

- On succeeded runs, `diagnostics` MUST be present and MUST include `comparability`.
  `filtered_reason_counts` and `skipped_row_counts` MAY be empty objects when no rows were filtered or skipped.
- `filtered_reason_counts` (object)
  - keys may include:
    - `filtered_by_parcel_id`
    - `filtered_by_year`
    - `filtered_by_risk_band`
    - `filtered_requires_review`
- `skipped_row_counts` (object)
  - keys may include:
    - `invalid_required_identifiers`
    - `duplicate_score_id_after_join`
  - `summary.skipped_count` MUST equal the sum of values in `skipped_row_counts`.
- `comparability` (object)
  - `comparable` (bool)
  - `queue_contract_version` (`review_queue_v1`)
  - `comparison_key` (stable object)
    - `feature_version`
    - `ruleset_version`
    - `requires_review_only`
    - `risk_bands`
    - `years`
    - `parcel_ids`
    - `sort_key_version` (`review_queue_sort_v1`)
    - `slice_mode` (`top` | `page`)
    - `top` (nullable int; populated when `slice_mode = "top"`)
    - `page` (nullable int; populated when `slice_mode = "page"`)
    - `page_size` (nullable int; populated when `slice_mode = "page"`)

## `error`

- `null` when run succeeded
- object when failed:
  - `code`
  - `message`

## CSV Export Contract

`--csv-out` writes one row per queue row with fixed column order:

1. `queue_rank`
2. `score_id`
3. `run_id`
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
`run_id` in both JSON and CSV maps to `fraud_scores.run_id`.

### CSV Dialect (v1, Stable Contract)

- File encoding: UTF-8 without BOM.
- Newline: `\n` (LF) on all platforms.
- Delimiter: comma (`,`).
- Header row: required, with column names in the exact order listed above.
- Quoting: RFC 4180-style minimal quoting using `"` when a field contains comma, quote, or newline, with one exception:
  non-null empty strings are always emitted as `""` to distinguish them from nulls.
- Quote escaping: embedded `"` characters are escaped by doubling (`"` -> `""`).
- Nulls: nullable fields emit as empty unquoted fields; non-null empty strings emit as `""`.
- Boolean columns (for example, `requires_review`) emit lowercase `true` / `false`.

## Comparability Semantics

Two queue exports are comparable only if all are true:

- same `queue_contract_version`
- same `sort_key_version`
- same `feature_version`
- same `ruleset_version`
- same filter scope (`parcel_ids`, `years`, `risk_bands`, review-only mode)
- same slice settings (`slice_mode`, `top`, `page`, `page_size`)

If any differ, output is valid but not directly comparable for rank drift interpretation.

## Null And Empty Semantics

- Decimal values, when present, serialize as strings in JSON and CSV.
- Fields explicitly documented as nullable may emit `null` instead of a decimal string.
  In v1 queue rows, `primary_reason_weight` is the only decimal-typed field that may be `null`.
- Arrays are never `null`; use `[]` for empty.
- In `request`, scope arrays (`parcel_ids`, `years`, `risk_bands`) always emit as arrays.
  Use `[]` to represent unscoped/default selection.
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

## Non-Goals For Sprint 6 Day 6 Contract

- Implementing queue/report code paths.
- Adding migrations or persistence for case reviews.
- Implementing analyst UI surface (Day 7).
