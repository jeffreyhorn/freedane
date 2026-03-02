# Sprint 2 Day 10 Notes

Day 10 extended tax parsing beyond the current summary-row shape without changing the schema.

The existing `TaxRecord.data` payload for `source == "detail"` records now keeps the raw modal `rows` and also includes structured tax-detail fields derived from those rows:

- `tax_value_rows`
- `tax_rate_rows`
- `tax_jurisdiction_rows`
- `tax_credit_rows`
- `special_charge_rows`
- `other_tax_item_rows`
- `installment_rows`
- `tax_amount_summary`
- `has_tax_credits`
- `has_special_charges`
- `has_other_tax_items`

This makes the detail payload much easier to query for:

- school levy and lottery/first-dollar credits
- special-charge indicators and named special charges
- presence of non-placeholder "other tax items"
- installment schedules
- modal-only totals like `total_taxes_less_credits`

Current implementation boundaries:

- the existing summary-level tax rows are unchanged
- `tax_detail_payments` parsing is unchanged
- no new tax columns or tables were added yet; the richer fields live inside the existing JSON detail payload
