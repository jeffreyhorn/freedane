# Sprint 2 Day 7 Notes

Date: 2026-03-01

## Focus

Implement the first extractor/storage path for the initial `parcel_characteristics` field set.

## Implemented

The parse-time storage path now upserts `parcel_characteristics` during `_store_parsed(...)` in [cli.py](../../../src/accessdane_audit/cli.py).

The new behavior:

- derives a `parcel_characteristics` candidate from the current raw HTML and parsed page
- compares that candidate against the existing stored row for the parcel
- keeps the more complete characteristic snapshot, using recency only as a tie-breaker
- updates `source_fetch_id` and `built_at` when a new winner is stored

## Initial Field Set Now Stored

The first implemented field set includes:

- `formatted_parcel_number`
- `state_municipality_code`
- `township`
- `range`
- `section`
- `quarter_quarter`
- `has_dcimap_link`
- `has_google_map_link`
- `has_bing_map_link`
- `current_assessment_year`
- `current_valuation_classification`
- `current_assessment_acres`
- `current_assessment_ratio`
- `current_estimated_fair_market_value`
- `current_tax_info_available`
- `current_payment_history_available`
- `tax_jurisdiction_count`
- `is_exempt_style_page`
- `has_empty_valuation_breakout`
- `has_empty_tax_section`

## Important Implementation Detail

The winner selection is currently parcel-level and incremental:

- a newly parsed fetch can replace the existing row only if it is more complete
- if completeness ties, the newer fetch wins

This matches the Day 5 design well enough for the first storage path without requiring a separate full-table rebuild command yet.

## Not Implemented Yet

Day 7 intentionally does **not** implement:

- `parcel_lineage_links` extraction
- a dedicated `parcel_characteristics` rebuild command

Those remain later Sprint 2 work.

## Test Coverage

New end-to-end storage coverage was added in [test_parse.py](../../../tests/test_parse.py):

- a full real parcel persists the initial characteristics fields
- an explicit empty-tax page persists the expected availability flags
- a richer older snapshot is preserved over a less-complete newer snapshot

## Validation

Validation run:

```bash
python -m compileall src tests
.venv/bin/python -m pytest tests/test_parse.py -q
.venv/bin/python -m pytest
```

Observed results:

- compile step passed
- parse tests: `17 passed`
- full suite: `41 passed in 17.25s`

## Next Step

Day 8 can now broaden characteristic coverage across more common residential parcel shapes using this storage path as the base.
