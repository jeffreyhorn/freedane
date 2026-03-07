from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

BASELINE_REVISION = "20260301_0001"
HEAD_REVISION = "20260307_0011"
EXPECTED_SCHEMA: dict[str, set[str]] = {
    "parcels": {"id", "trs_code", "section", "subsection", "created_at"},
    "fetches": {
        "id",
        "parcel_id",
        "url",
        "status_code",
        "fetched_at",
        "raw_path",
        "raw_sha256",
        "raw_size",
        "parsed_at",
        "parse_error",
    },
    "assessments": {
        "id",
        "parcel_id",
        "fetch_id",
        "year",
        "valuation_classification",
        "assessment_acres",
        "land_value",
        "improved_value",
        "total_value",
        "average_assessment_ratio",
        "estimated_fair_market_value",
        "valuation_date",
        "data",
        "created_at",
    },
    "taxes": {"id", "parcel_id", "fetch_id", "year", "data", "created_at"},
    "payments": {"id", "parcel_id", "fetch_id", "year", "data", "created_at"},
    "parcel_summaries": {
        "id",
        "parcel_id",
        "fetch_id",
        "municipality_name",
        "parcel_description",
        "owner_name",
        "primary_address",
        "billing_address",
        "created_at",
    },
}


class Base(DeclarativeBase):
    pass


def get_engine(database_url: str):
    return create_engine(database_url, future=True)


def get_session_factory(database_url: str):
    engine = get_engine(database_url)
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)


def init_db(database_url: str) -> None:
    from alembic import command

    config = _get_alembic_config(database_url)
    schema_state = _classify_schema_state(database_url)
    if schema_state == "legacy":
        # Keep this only as a transition path for databases created before Alembic
        # became the source of truth. Stamp the last legacy-compatible revision,
        # then apply forward migrations normally.
        command.stamp(config, BASELINE_REVISION)
    elif schema_state == "incompatible":
        raise RuntimeError(
            "Found a pre-Alembic database that does not match the expected "
            "legacy schema. "
            "Back up the database and migrate it manually before running init-db."
        )
    if schema_state == "legacy":
        # After stamping the baseline, continue upgrading so future
        # revisions are applied.
        command.upgrade(config, "head")
        return
    command.upgrade(config, "head")


def _get_alembic_config(database_url: str):
    from alembic.config import Config

    project_root = Path(__file__).resolve().parents[2]
    config_path = project_root / "alembic.ini"
    if not config_path.exists():
        raise FileNotFoundError(f"Alembic config not found: {config_path}")

    config = Config(str(config_path))
    config.set_main_option("sqlalchemy.url", database_url)
    return config


def _classify_schema_state(database_url: str) -> str:
    engine = get_engine(database_url)
    try:
        inspector = inspect(engine)
        tables = set(inspector.get_table_names())
        table_columns = {
            table_name: {column["name"] for column in inspector.get_columns(table_name)}
            for table_name in tables
        }
    finally:
        engine.dispose()

    if "alembic_version" in tables:
        return "versioned"
    if not tables:
        return "empty"

    if tables == set(EXPECTED_SCHEMA):
        for table_name, expected_columns in EXPECTED_SCHEMA.items():
            if not expected_columns.issubset(table_columns.get(table_name, set())):
                return "incompatible"
        return "legacy"

    return "incompatible"


@contextmanager
def session_scope(database_url: str) -> Iterator[Session]:
    SessionFactory = get_session_factory(database_url)
    session = SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
