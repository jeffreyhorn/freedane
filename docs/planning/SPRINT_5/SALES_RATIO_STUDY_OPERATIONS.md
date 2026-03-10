# Sprint 5 Sales Ratio Study Operations

Prepared on: 2026-03-07

## Goal

Run `accessdane sales-ratio-study` in a repeatable way and interpret diagnostics safely, especially for sparse years and small peer groups.

## Command Usage

Full study:

```bash
.venv/bin/accessdane sales-ratio-study --version-tag sales_ratio_v1
```

Scoped study:

```bash
.venv/bin/accessdane sales-ratio-study \
  --ids <parcel_ids.txt> \
  --year 2025 \
  --municipality mcfarland \
  --class residential \
  --version-tag sales_ratio_v1_scope \
  --out data/sales_ratio_study_scope.json
```

Repeatable direct IDs:

```bash
.venv/bin/accessdane sales-ratio-study --id 061001391511 --id 061002275801
```

## Output Interpretation

`summary` highlights coverage and skip reasons:

- `candidate_sales_count`: candidate matched loaded sales in scope
- `included_sales_count`: sales included in ratio metrics
- `excluded_sales_count`: sales removed by active exclusions
- `skipped_scope_filter_count`: rows filtered out by municipality/class scope
- `skipped_missing_group_dimension_count`: rows skipped due to missing municipality/class grouping values
- `skipped_missing_parcel_year_fact_count`: rows with missing parcel-year context
- `skipped_missing_assessment_count`: rows with missing or invalid assessment values
- `insufficient_group_count`: groups with too few included sales for stable peer interpretation

`groups` are deterministic and include:

- `is_insufficient_sample`: `true` when `sale_count < minimum_required_sale_count`
- `minimum_required_sale_count`: current threshold for sufficient peer size
- `cod` / `prd`: set to `null` for insufficient samples

`diagnostics` provide operator triage signals:

- `requested_years_without_candidate_sales`: requested years with no candidate sales
- `insufficient_groups`: group keys with insufficient peer size
- `groups_with_exclusions`: group keys with non-zero exclusion counts

## Operational Notes

- The command is deterministic for the same source data and same resolved scope/config.
- Sparse scopes can still succeed with zero or low group coverage; inspect `diagnostics` before using outputs downstream.
- For analyst interpretation, treat `is_insufficient_sample = true` groups as directional only, not calibration-grade metrics.

## See Also

- consolidated Sprint 5 runbook: `docs/planning/SPRINT_5/OPERATIONS.md`
