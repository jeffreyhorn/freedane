"""add optional spatial fields to parcel_characteristics

Revision ID: 20260307_0010
Revises: 20260306_0009
Create Date: 2026-03-07 00:05:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260307_0010"
down_revision = "20260306_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("parcel_characteristics") as batch:
        batch.add_column(
            sa.Column("centroid_latitude", sa.Numeric(9, 6), nullable=True)
        )
        batch.add_column(
            sa.Column("centroid_longitude", sa.Numeric(9, 6), nullable=True)
        )
        batch.add_column(sa.Column("geometry_wkt", sa.Text(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("parcel_characteristics") as batch:
        batch.drop_column("geometry_wkt")
        batch.drop_column("centroid_longitude")
        batch.drop_column("centroid_latitude")
