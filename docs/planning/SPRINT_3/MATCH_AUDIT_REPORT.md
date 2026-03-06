# Sprint 3 Match Audit Report

Prepared on: 2026-03-05

## Purpose

Provide an operator-facing summary of RETR sales matching quality with explicit visibility into:

- confidence tiers for matched transactions
- unresolved transactions
- ambiguous candidate sets
- low-confidence links that still require manual review

## Command

Generate the report for all loaded RETR transactions:

```bash
.venv/bin/accessdane report-sales-matches --out data/sprint3_match_audit.json
```

Generate the report for a scoped transaction-ID subset:

```bash
.venv/bin/accessdane report-sales-matches --ids data/sales_transaction_ids.txt --out data/sprint3_match_audit_scoped.json
```

## Output Shape

`report-sales-matches` emits JSON with these top-level fields:

- `scope`
  - `scoped_transaction_id_count`
  - `loaded_transactions_in_scope`
- `totals`
  - `matched_transactions`
  - `unmatched_transactions`
- `review_status_counts`
  - transaction-level review-state counts (`unreviewed`, `needs_review`, `reviewed`)
- `confidence_tiers`
  - `high`: confidence `>= 0.9500`
  - `medium`: confidence `>= 0.8500` and `< 0.9500`
  - `low`: confidence `< 0.8500`
  - `unknown`: no confidence score available
- `best_match_method_counts`
  - count of the best candidate match method per matched transaction
- `review_queue`
  - `unresolved_count`
  - `ambiguous_count`
  - `low_confidence_count`
  - `sample_transaction_ids` for each queue category
- `context_signals`
  - matched-sale context coverage and first-pass permit/appeal + sale/assessment
    divergence flags using transfer-year `parcel_year_facts`
- `parcel_timeline_samples`
  - parcel-year timeline snippets for sampled matched transactions
  - includes nearby year context for assessment/tax/permit/appeal signals
- `neighborhood_context_aggregates`
  - municipality-level first-pass aggregates for matched transactions with transfer-year
    parcel context
  - includes permit/appeal signal rates and sale/assessment gap-flag rates

## Interpretation

- Use `unresolved_count` as the first triage queue (no candidate match rows).
- Use `ambiguous_count` to review transactions with multiple candidate parcels.
- Use `low_confidence_count` to review fallback links even when a candidate exists.
- A decrease in `needs_review` and unresolved/ambiguous/low-confidence queue counts across imports indicates matcher quality is improving.
