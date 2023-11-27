"""create catalog tables.

Revision ID: 44da61dbd8d2
Revises:
Create Date: 2023-11-27 11:42:05.074940

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "44da61dbd8d2"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add the following tables to the database.

    * sources
    * schemata
    * default_schema
    * tables
    * columns
    """
    op.create_table(
        "sources",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("uri", sa.String(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP),
        sa.Column("updated_at", sa.TIMESTAMP),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "schemata",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("source_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP),
        sa.Column("updated_at", sa.TIMESTAMP),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_id", "name", name="unique_schema_name"),
    )
    op.create_table(
        "default_schema",
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column("schema_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP),
        sa.Column("updated_at", sa.TIMESTAMP),
        sa.ForeignKeyConstraint(
            ["schema_id"],
            ["schemata.id"],
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["sources.id"],
        ),
        sa.PrimaryKeyConstraint("source_id"),
    )
    op.create_table(
        "tables",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("schema_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP),
        sa.Column("updated_at", sa.TIMESTAMP),
        sa.ForeignKeyConstraint(
            ["schema_id"],
            ["schemata.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("schema_id", "name", name="unique_table_name"),
    )
    op.create_table(
        "columns",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("data_type", sa.String(), nullable=True),
        sa.Column("sort_order", sa.Integer(), nullable=True),
        sa.Column("table_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP),
        sa.Column("updated_at", sa.TIMESTAMP),
        sa.ForeignKeyConstraint(
            ["table_id"],
            ["tables.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("table_id", "name", name="unique_column_name"),
    )


def downgrade() -> None:
    """Drop the following tables from the database.

    * columns
    * tables
    * default_schema
    * schemata
    * sources
    """
    op.drop_table("columns")
    op.drop_table("tables")
    op.drop_table("default_schema")
    op.drop_table("schemata")
    op.drop_table("sources")
