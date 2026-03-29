# Manual Import CSV Templates

These templates are compatible with:

- `accessdane ingest-permits --file ...`
- `accessdane ingest-appeals --file ...`

Use them as starter headers for records-request cleanup and manual curation.

## Files

- `permit_events_template.csv`
- `appeal_events_template.csv`
- `alert_route_config_v1.template.json`

## Value Guidance

### Date fields

Accepted date examples:

- `01/15/2025`
- `01-15-2025`
- `2025-01-15`
- `2025/01/15`
- `01/15/25`
- `2025-01-15T14:30:00Z`

### Currency / numeric fields

Accepted amount examples:

- `12345.67`
- `$12,345.67`
- `(12345.67)` for negative values

### Null / missing tokens

These are treated as missing values:

- `N/A`
- `NA`
- `NULL`
- `none`
- `not available`
- `not applicable`
- `TBD`

## Minimum row-shape rules

Permit rows must include at least:

- one parcel locator: `Parcel Number` or `Address`
- one temporal anchor: one of `Applied Date`, `Issued Date`, `Finaled Date`, `Status Date`, `Permit Year`

Appeal rows must include at least:

- one parcel locator: `Parcel Number` or `Address`
- one temporal anchor: one of `Filing Date`, `Hearing Date`, `Decision Date`, `Tax Year`
- one appeal signal: one of `Appeal Number`, `Docket Number`, `Outcome`, `Requested Assessed Value`, `Decided Assessed Value`

## Example ingest commands

```bash
accessdane ingest-permits --file docs/templates/permit_events_template.csv
accessdane ingest-appeals --file docs/templates/appeal_events_template.csv
.venv/bin/accessdane alert-transport \
  --alert-file path/to/load_monitor_alert.json \
  --alert-file path/to/benchmark_pack_alert.json \
  --route-config docs/templates/alert_route_config_v1.template.json
```
