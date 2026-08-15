"""Add store_conversations column to entities

Revision ID: b6c7d8e9f0a1
Revises: d5e6f7a8b9c0
Create Date: 2026-08-14 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

revision = "b6c7d8e9f0a1"
down_revision = "d5e6f7a8b9c0"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("entities", sa.Column("store_conversations", sa.Boolean(), nullable=False, server_default=sa.true(),
                  comment="Whether webchat conversations are persisted for this user"))


def downgrade():
    op.drop_column("entities", "store_conversations")
