# Sprint 5 Build Features Operations

Prepared on: 2026-03-09

## Goal

Run `accessdane build-features` reproducibly, validate feature output quality, and triage hard-validation skips before downstream scoring.

## Command Usage

Full rebuild:

```bash
.venv/bin/accessdane build-features --feature-version feature_v1
```

Scoped rebuild by file + year:

```bash
.venv/bin/accessdane build-features \
  --ids data/parcel_scope.txt \
  --year 2025 \
  --feature-version feature_v1_scope \
  --out data/build_features_scope.json
```

Scoped rebuild by repeated IDs:

```bash
.venv/bin/accessdane build-features \
  --id 061001391511 \
  --id 061002275801 \
  --feature-version feature_v1_triage
```

## Output Interpretation

`summary` provides run-level write outcomes:

- `selected_parcels`: unique parcels selected by resolved scope
- `selected_parcel_years`: total selected parcel-year rows considered
- `rows_deleted`: in-scope existing rows removed for the same `feature_version`
- `rows_inserted`: rows persisted to `parcel_features`
- `rows_skipped`: rows blocked by hard validation checks
- `quality_warning_count`: total warning-flag count across inserted rows

`diagnostics` provides operator triage details:

- `rows_with_quality_flags`: inserted rows with one or more quality warning flags
- `quality_flag_counts`: counts by warning flag code
- `validation_error_counts`: counts by hard-validation error code
- `skipped_rows`: deterministic sample of skipped rows with `parcel_id`, `year`, and `error_codes`

## Rebuild Workflow

1. Run scoped rebuild first (`--ids` + `--year`) for validation.
2. Confirm `rows_skipped = 0` before full-corpus rebuild.
3. If `rows_skipped > 0`, inspect `validation_error_counts` and `skipped_rows`, then correct source/context issues or logic defects.
4. Re-run the same scope and confirm deterministic output.
5. Execute full rebuild only after scoped output is clean.

## Operational Notes

- Hard-validation failures are surfaced and skipped intentionally so invalid feature states are not persisted.
- Quality-warning flags are non-fatal; they indicate incomplete context and should be reviewed by downstream scoring rules.
- For deterministic reruns, keep source data, scope, and `feature_version` fixed.
- `scoring_runs.output_summary_json` stores both `summary` and `diagnostics` for run auditability.

## Next Step

After feature output is accepted, continue with scoring workflow operations in:

- `docs/planning/SPRINT_5/SCORE_FRAUD_OPERATIONS.md`
