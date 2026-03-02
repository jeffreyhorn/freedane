# Sprint 2 Day 9 Notes

Day 9 implemented the first parcel-lineage extraction path into `parcel_lineage_links`.

What now happens during parsing:

- `_store_parsed(...)` reads the parent/child parcel history modals from the raw AccessDane HTML
- explicit history rows from `#modalParcelHistoryParents` and `#modalParcelHistoryChildren` are normalized into:
  - `parcel_id`
  - `related_parcel_id`
  - `relationship_type` (`parent` or `child`)
  - `related_parcel_status`
  - `relationship_note`
  - `source_fetch_id`
- duplicate lineage links are deduplicated by (`parcel_id`, `related_parcel_id`, `relationship_type`)
- if the same link is seen across multiple fetches, the latest fetch wins for status, note, and source provenance

Current implementation choice:

- lineage is maintained as an incremental deduplicated union during parse-time storage
- this matches the v1 merge rule well enough for normal ingest, but there is not yet a dedicated full-scope lineage rebuild command

Coverage added:

- real fixture `061002320401`, which confirms both parent and child modal rows persist as 5 normalized links
- a synthetic precedence case proving the latest fetch wins when the same parent link appears with newer metadata
