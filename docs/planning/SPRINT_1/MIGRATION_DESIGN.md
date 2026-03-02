# Sprint 1 Migration Design Note

## Objective

Replace the current implicit schema-management behavior with a managed, repeatable migration workflow that can support future analytics tables and external data imports.

## Current State

Today the repository defines schema in SQLAlchemy models and creates it through runtime ORM setup:

- `Base.metadata.create_all(engine)` creates the tables.
- `init_db()` also applies ad hoc schema repair logic for `assessments`.
- `init_db()` creates a unique index for `parcel_summaries.parcel_id` outside the model definition.

This works for initial experimentation, but it has three problems:

1. It hides schema drift in application startup logic.
2. It makes the actual database shape harder to audit.
3. It does not provide a durable history for future schema changes.

## Recommended Strategy

Use Alembic as the migration system.

Recommended implementation:

- Add a repository-local Alembic configuration.
- Create a baseline migration that fully represents the current schema.
- Make `accessdane init-db` apply migrations to `head`.
- Move future schema changes into forward-only migration revisions.

## Baseline Schema To Capture

The baseline migration should create and preserve the current operational schema:

- `parcels`
- `fetches`
- `assessments`
- `taxes`
- `payments`
- `parcel_summaries`

It must also include:

- Existing indexes used by the current application.
- The unique parcel summary constraint.
- The typed assessment columns currently patched in by runtime logic.

## Runtime Schema Logic To Remove

The following runtime schema behaviors should be phased out or reduced:

- Hidden `ALTER TABLE` additions for `assessments`.
- Out-of-band index creation for `parcel_summaries`.

During transition, temporary compatibility guards are acceptable only if they are clearly marked and removable after migration rollout.

## Expected Near-Term Benefits

- Empty databases can be initialized deterministically.
- Future tables such as `parcel_year_facts`, `sales_transactions`, and `parcel_features` can be introduced safely.
- Schema reviews become explicit and auditable.
- Developer onboarding becomes easier because database setup is no longer implicit.

## Immediate Risks

- Existing databases created by older code may not perfectly match the baseline migration.
- SQLite and Postgres differences may surface once migrations are authoritative.
- Alembic CLI usage from the repo root must be able to import the `src/` package layout cleanly.

## Mitigations

- Keep the baseline migration aligned with the current SQLAlchemy model definitions.
- Add a migration environment that inserts `src/` into `sys.path`.
- Validate schema creation against an empty database first before attempting upgrades on older databases.

## Follow-On Work

After the baseline is in place:

1. Add a migration for `parcel_year_facts`.
2. Add tests that validate migration-driven schema creation.
3. Remove any remaining runtime schema repair code once no longer needed.
