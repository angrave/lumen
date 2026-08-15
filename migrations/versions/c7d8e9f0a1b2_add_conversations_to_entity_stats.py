"""Add conversations counter to entity_stats

Revision ID: c7d8e9f0a1b2
Revises: b6c7d8e9f0a1
Create Date: 2026-08-14 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

revision = "c7d8e9f0a1b2"
down_revision = "b6c7d8e9f0a1"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("entity_stats", sa.Column("conversations", sa.Integer(), nullable=False, server_default="0",
                  comment="Total webchat conversations started; retained even when conversations are deleted or storage is disabled"))
    # Backfill from the currently stored conversations. Entities that have
    # conversations but no entity_stats row yet get one first.
    op.execute("""
        INSERT INTO entity_stats (entity_id, requests, input_tokens, output_tokens, audio_seconds, cost, conversations)
        SELECT DISTINCT c.entity_id, 0, 0, 0, 0, 0, 0
        FROM conversations c
        WHERE NOT EXISTS (SELECT 1 FROM entity_stats es WHERE es.entity_id = c.entity_id)
    """)
    op.execute("""
        UPDATE entity_stats SET conversations = (
            SELECT COUNT(*) FROM conversations c WHERE c.entity_id = entity_stats.entity_id
        )
    """)


def downgrade():
    op.drop_column("entity_stats", "conversations")
