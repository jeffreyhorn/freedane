"""reconcile sales parcel match index name

Revision ID: 20260304_0006
Revises: 20260304_0005
Create Date: 2026-03-04 00:10:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260304_0006"
down_revision = "20260304_0005"
branch_labels = None
depends_on = None

_TABLE_NAME = "sales_parcel_matches"
_DESIRED_INDEX_NAME = "ux_sales_parcel_matches_txn_parcel_method"
_INDEX_COLUMNS = ("sales_transaction_id", "parcel_id", "match_method")


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    indexes = inspector.get_indexes(_TABLE_NAME)

    desired_present = False
    legacy_names: list[str] = []
    for index in indexes:
        name = index["name"]
        columns = tuple(index.get("column_names") or ())
        unique = bool(index.get("unique"))
        if not unique or columns != _INDEX_COLUMNS:
            continue
        if name == _DESIRED_INDEX_NAME:
            desired_present = True
            continue
        legacy_names.append(name)

    for legacy_name in legacy_names:
        op.drop_index(legacy_name, table_name=_TABLE_NAME)

    if not desired_present:
        op.create_index(
            _DESIRED_INDEX_NAME,
            _TABLE_NAME,
            list(_INDEX_COLUMNS),
            unique=True,
        )


def downgrade() -> None:
    # This revision only reconciles pre-existing index names to the canonical
    # 20260304_0005 definition. Downgrading to 0005 does not require changes.
    pass
