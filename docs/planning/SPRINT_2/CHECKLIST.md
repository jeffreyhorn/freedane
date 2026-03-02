# Sprint 2 Acceptance Checklist

## Purpose

Use this checklist to determine whether Sprint 2 is complete and ready for acceptance review.

Sprint 2 target:

- Expand the AccessDane extraction layer so the project captures richer parcel characteristics, parcel lineage, and tax-detail context while keeping deliberate parser/operational deferrals explicit.

## Acceptance Criteria

### 1. `parcel_characteristics` Is A Stable Managed Output

- `parcel_characteristics` exists as a migration-managed table.
- The table remains one row per `parcel_id`.
- Parse-time storage populates the current v1 characteristic fields from AccessDane HTML.
- The stored row keeps a single winning `source_fetch_id`.
- Winner selection remains deterministic:
  - more complete characteristic snapshots beat less complete ones
  - recency breaks completeness ties

Pass condition:

- A contributor can parse the same parcel from multiple fetches and predict which `parcel_characteristics` row will persist.

### 2. Characteristic Coverage Is Broadened Across Common And Edge Shapes

- Tests cover normal residential pages and label variants.
- Tests cover at least the following edge-case parcel categories:
  - vacant land
  - condo or condo-like parcel
  - exempt-style empty page
  - sparse legal-detail shape
- The current characteristic extraction tolerates sparse field presence without breaking the broader parse.

Pass condition:

- The current characteristic fields persist across representative AccessDane page variants, not only the default residential layout.

### 3. `parcel_lineage_links` Extraction Works

- Parent/child parcel history modals are parsed when present.
- Lineage rows normalize to:
  - `parcel_id`
  - `related_parcel_id`
  - `relationship_type`
  - `related_parcel_status`
  - `relationship_note`
  - `source_fetch_id`
- Duplicate lineage links are deduplicated by (`parcel_id`, `related_parcel_id`, `relationship_type`).
- If the same lineage link appears in multiple fetches, the latest fetch wins for metadata fields.
- Tests cover:
  - a parcel with both parents and children
  - a parcel with only parents or only children

Pass condition:

- Explicit parcel lineage exposed by AccessDane is persisted predictably and without duplicate link rows.

### 4. Richer Tax Detail Fields Are Extracted

- `TaxRecord.data` for `source == "detail"` includes structured modal-derived fields in addition to the raw `rows`.
- The structured detail payload covers, when present:
  - tax values
  - tax rates
  - jurisdictions
  - tax credits
  - special charges
  - other tax items
  - installment rows
  - modal-level totals
- Boolean indicators exist for:
  - `has_tax_credits`
  - `has_special_charges`
  - `has_other_tax_items`
- Tests cover at least one normal parcel and one edge-case parcel with special-charge rows.

Pass condition:

- Downstream code can inspect richer tax-detail signals without re-deriving them from raw modal row arrays.

### 5. Report Endpoints Are Explicitly Resolved

- The AccessDane `Summary Report` / `Custom Report` flow has been investigated.
- The outcome is documented as either:
  - an implemented optional fetch path
  - or a deliberate rejection decision
- If rejected, the reason is concrete and technical rather than implied.

Pass condition:

- Sprint 2 does not leave the report path as an open ambiguity.

### 6. Deliberate Deferrals Are Documented

- Any major Sprint 2 omission that was consciously deferred is listed explicitly.
- At minimum, the documentation addresses whether these remain deferred:
  - a dedicated rebuild command for `parcel_characteristics`
  - a dedicated full-scope rebuild command for `parcel_lineage_links`
  - a normalized tax-detail table beyond the current JSON payload
  - a PDF-based report fetch/parser path
- The docs distinguish between a true parser gap and an accepted source limitation where relevant.

Pass condition:

- Another contributor can tell the difference between “not built yet on purpose” and “accidental missing work.”

### 7. Contributor Docs Reflect Sprint 2 Reality

- Contributor-facing docs mention the new Sprint 2 outputs.
- The operational docs explain how the new outputs are refreshed.
- The docs make clear that current report endpoints are not part of the ingestion path if they were rejected.
- The docs remain aligned with the actual CLI behavior and current parser architecture.

Pass condition:

- A contributor can use the repository without assuming nonexistent rebuild commands or hidden report-fetch behavior.

## Recommended Review Sequence

Review Sprint 2 in this order:

1. Run the automated test suite.
2. Confirm the schema is at the current head revision.
3. Reparse a scoped parcel subset and inspect `parcel_characteristics` / `parcel_lineage_links`.
4. Review the richer `TaxRecord.data` detail payload on a known fixture.
5. Review the report-endpoint decision and deliberate deferrals.

## Known Acceptable Deferrals

Sprint 2 should not be considered failed if these are still deferred:

- A dedicated full rebuild command for `parcel_characteristics`.
- A dedicated full rebuild command for `parcel_lineage_links`.
- A normalized table for richer tax detail beyond the existing JSON payload.
- A PDF fetch / PDF parsing path for AccessDane report endpoints.

These are acceptable as long as the deferrals remain explicit and the current parse-time behavior is documented.
