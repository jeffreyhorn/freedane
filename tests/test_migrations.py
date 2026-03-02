from __future__ import annotations

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

from accessdane_audit.db import BASELINE_REVISION, HEAD_REVISION, init_db


def _db_url(path: Path) -> str:
    return f"sqlite:///{path}"


def _alembic_config(database_url: str) -> Config:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    return config


def _create_legacy_baseline_schema(database_url: str) -> None:
    config = _alembic_config(database_url)
    command.upgrade(config, BASELINE_REVISION)

    engine = create_engine(database_url, future=True)
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE alembic_version"))
    finally:
        engine.dispose()


def test_init_db_applies_migrations_to_empty_database(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.sqlite"

    init_db(_db_url(db_path))

    engine = create_engine(_db_url(db_path), future=True)
    try:
        inspector = inspect(engine)
        tables = set(inspector.get_table_names())
        assert "alembic_version" in tables
        assert {
            "parcels",
            "fetches",
            "assessments",
            "taxes",
            "payments",
            "parcel_summaries",
            "parcel_year_facts",
            "parcel_characteristics",
            "parcel_lineage_links",
        }.issubset(tables)

        with engine.connect() as conn:
            revision = conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
        assert revision == HEAD_REVISION

        fetch_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("fetches")
        }
        assert fetch_indexes["ix_fetches_status_code_parsed_at"] == (
            "status_code",
            "parsed_at",
        )
        parcel_year_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("parcel_year_facts")
        }
        assert parcel_year_indexes["ix_parcel_year_facts_year"] == ("year",)
        assert parcel_year_indexes["ix_parcel_year_facts_current_owner_name"] == (
            "current_owner_name",
        )
        parcel_characteristic_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("parcel_characteristics")
        }
        assert parcel_characteristic_indexes["ix_parcel_characteristics_source_fetch_id"] == (
            "source_fetch_id",
        )
        assert (
            parcel_characteristic_indexes[
                "ix_parcel_characteristics_current_valuation_classification"
            ]
            == ("current_valuation_classification",)
        )
        assert (
            parcel_characteristic_indexes[
                "ix_parcel_characteristics_state_municipality_code"
            ]
            == ("state_municipality_code",)
        )
        parcel_lineage_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("parcel_lineage_links")
        }
        assert parcel_lineage_indexes["ix_parcel_lineage_links_related_parcel_id"] == (
            "related_parcel_id",
        )
        assert parcel_lineage_indexes["ix_parcel_lineage_links_relationship_type"] == (
            "relationship_type",
        )
    finally:
        engine.dispose()


def test_init_db_stamps_compatible_legacy_schema_before_upgrading(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.sqlite"
    database_url = _db_url(db_path)

    _create_legacy_baseline_schema(database_url)

    init_db(database_url)

    engine = create_engine(database_url, future=True)
    try:
        inspector = inspect(engine)
        assert "alembic_version" in set(inspector.get_table_names())
        with engine.connect() as conn:
            revision = conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
        assert revision == HEAD_REVISION
        fetch_indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspector.get_indexes("fetches")
        }
        assert fetch_indexes["ix_fetches_status_code_parsed_at"] == (
            "status_code",
            "parsed_at",
        )
        assert "parcel_year_facts" in set(inspector.get_table_names())
        assert "parcel_characteristics" in set(inspector.get_table_names())
        assert "parcel_lineage_links" in set(inspector.get_table_names())
    finally:
        engine.dispose()


def test_init_db_rejects_incompatible_preexisting_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "partial.sqlite"
    database_url = _db_url(db_path)

    engine = create_engine(database_url, future=True)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE parcels (id VARCHAR PRIMARY KEY)"))
    finally:
        engine.dispose()

    with pytest.raises(RuntimeError, match="does not match the expected legacy schema"):
        init_db(database_url)


def test_init_db_is_idempotent_on_already_versioned_database(tmp_path: Path) -> None:
    db_path = tmp_path / "versioned.sqlite"
    database_url = _db_url(db_path)

    init_db(database_url)
    init_db(database_url)

    engine = create_engine(database_url, future=True)
    try:
        inspector = inspect(engine)
        assert "parcel_year_facts" in set(inspector.get_table_names())
        assert "parcel_characteristics" in set(inspector.get_table_names())
        assert "parcel_lineage_links" in set(inspector.get_table_names())
        with engine.connect() as conn:
            revision = conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
        assert revision == HEAD_REVISION
    finally:
        engine.dispose()
