# Sprint 2 Day 8 Notes

Day 8 broadened `parcel_characteristics` coverage without changing the schema.

The extraction layer now handles more residential page shapes by:

- deriving `state_municipality_code`, `township`, `range`, and `section` from the formatted parcel number when the parcel-detail table is sparse
- accepting sparse two-cell legal-detail rows such as `Township/Range` and `Township and Range`, not only the original 3-column grid
- tolerating township/range strings with separators or spaces (for example `T 6 N / R 10 E`)
- falling back to `Parcel Description` patterns like `SW1/4NE1/4` to populate `quarter_quarter` when the explicit quarter row is missing
- detecting DCiMap, Google, and Bing links by URL pattern as well as exact button text

Coverage added in tests:

- real mixed-class residential fixture `061001285911`, which confirms the current characteristic row still captures mixed residential/agricultural classification and map-link availability
- a synthetic sparse parcel-detail shape that proves the new fallback path fills the core legal-location fields and link flags even when the page does not use the standard AccessDane button labels or 3-column legal grid

Early completeness result:

- across the Day 7 full fixture, the Day 8 mixed-class residential fixture, and the new sparse-shape test, the core identity/location characteristic fields (`formatted_parcel_number`, `state_municipality_code`, `township`, `range`, `section`, `quarter_quarter`) are now covered for all three representative residential shapes
