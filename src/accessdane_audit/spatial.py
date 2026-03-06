from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session


@dataclass(frozen=True)
class SpatialSupportStatus:
    database_dialect: str
    mode: str
    postgis_available: bool
    postgis_full_version: Optional[str]
    geometry_columns_available: bool
    note: str


def detect_spatial_support(session: Session) -> SpatialSupportStatus:
    bind = session.get_bind()
    dialect_name = bind.dialect.name if bind is not None else "unknown"

    if dialect_name != "postgresql":
        return SpatialSupportStatus(
            database_dialect=dialect_name,
            mode="point_only",
            postgis_available=False,
            postgis_full_version=None,
            geometry_columns_available=False,
            note=(
                "Non-PostgreSQL backend detected; "
                "point/coordinate fields are available but "
                "PostGIS geometry workflows remain disabled."
            ),
        )

    try:
        postgis_available = bool(
            session.execute(
                text(
                    "SELECT EXISTS("
                    "SELECT 1 FROM pg_extension WHERE extname = 'postgis'"
                    ")"
                )
            ).scalar()
        )
    except Exception as exc:
        return SpatialSupportStatus(
            database_dialect=dialect_name,
            mode="point_only",
            postgis_available=False,
            postgis_full_version=None,
            geometry_columns_available=False,
            note=(
                "PostGIS extension probe failed; keeping geometry workflows disabled "
                f"({exc.__class__.__name__})."
            ),
        )

    if not postgis_available:
        return SpatialSupportStatus(
            database_dialect=dialect_name,
            mode="point_only",
            postgis_available=False,
            postgis_full_version=None,
            geometry_columns_available=False,
            note=(
                "PostGIS extension is not installed; point/coordinate fields remain "
                "usable and geometry workflows stay disabled."
            ),
        )

    postgis_full_version: Optional[str] = None
    try:
        version_value = session.execute(text("SELECT PostGIS_Full_Version()")).scalar()
        if version_value is not None:
            postgis_full_version = str(version_value)
    except Exception:
        postgis_full_version = None

    geometry_columns_available = False
    try:
        geometry_columns_available = bool(
            session.execute(
                text("SELECT to_regclass('public.geometry_columns') IS NOT NULL")
            ).scalar()
        )
    except Exception:
        geometry_columns_available = False

    return SpatialSupportStatus(
        database_dialect=dialect_name,
        mode="geometry_postgis",
        postgis_available=True,
        postgis_full_version=postgis_full_version,
        geometry_columns_available=geometry_columns_available,
        note=(
            "PostGIS is available; geometry-enabled workflows may be used when "
            "geometry data is present."
        ),
    )


def spatial_support_status_to_dict(status: SpatialSupportStatus) -> dict[str, object]:
    return {
        "database_dialect": status.database_dialect,
        "mode": status.mode,
        "postgis_available": status.postgis_available,
        "postgis_full_version": status.postgis_full_version,
        "geometry_columns_available": status.geometry_columns_available,
        "note": status.note,
    }
