# Spatial Guardrails (Sprint 4 Day 13)

Prepared on: 2026-03-07

## Purpose

Add future-friendly spatial support without requiring PostGIS for normal operation.

## What Was Added

- Optional parcel spatial fields on `parcel_characteristics`:
  - `centroid_latitude` (`Numeric(9,6)`, nullable)
  - `centroid_longitude` (`Numeric(9,6)`, nullable)
  - `geometry_wkt` (`Text`, nullable)
- Runtime spatial capability probe:
  - `accessdane spatial-support`
  - detects DB dialect and PostGIS extension availability
  - reports effective mode and safe operator guidance

## Runtime Modes

### Geometry-present mode (`geometry_postgis`)

Triggered when:

- database dialect is PostgreSQL
- PostGIS extension probe succeeds
- PostGIS is installed

Behavior:

- geometry-enabled workflows may be used when geometry data is present
- point/coordinate fields continue to work

### Geometry-absent mode (`point_only`)

Triggered when:

- database is not PostgreSQL, or
- PostGIS is not installed, or
- PostGIS probe fails

Behavior:

- system remains fully functional
- point/coordinate fields are still available
- geometry-specific workflows stay disabled

## Operator Commands

Inspect current runtime mode:

```bash
accessdane spatial-support
```

Write status JSON:

```bash
accessdane spatial-support --out data/spatial_support.json
```

## Guardrail Intent

- Do not hard-fail non-PostGIS environments.
- Keep SQLite and non-geometry Postgres deployments fully operational.
- Enable geometry functionality only when PostGIS is verifiably available.
