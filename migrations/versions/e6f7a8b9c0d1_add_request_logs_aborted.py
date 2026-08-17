"""Add aborted flag to request_logs

Revision ID: e6f7a8b9c0d1
Revises: d5e6f7a8b9c0
Create Date: 2026-08-17 00:00:00.000000

A stream the client abandons used to be recognised by the convention
"cost == 0 means aborted": the upstream reports usage only in its terminal
chunk, so an abandoned stream had no tokens and therefore no cost. Aborted
streams are now billed for what they actually consumed (estimated from the
content deltas received, or exact when the usage chunk had already arrived),
so a zero cost no longer identifies them. Hence this explicit boolean.

No backfill of historical rows — deliberately. ``aborted`` is only useful if
it is trustworthy, and "cost = 0" cannot be turned into a trustworthy abort
flag after the fact:

  * ``request_logs`` has no column saying whether a request was streamed, so
    the backfill could not even be restricted to streaming rows.
  * Completed requests legitimately log cost 0 — a model configured with zero
    ``input_cost_per_million``/``output_cost_per_million`` (local/dummy
    models), an audio request against a model with no ``audio_cost_per_hour``
    (``_do_audio`` logs it as zero cost and only warns), an upstream that
    returned no usage object at all, and any request small enough that the
    cost rounds to 0.000000 at ``Numeric(12, 6)``.

Mislabelling those as aborts would poison every future query on the column,
whereas leaving old aborts at ``false`` is benign: historical rows simply keep
the old convention, which anyone analysing pre-migration data can still apply
by hand. False negatives in the past are cheaper than false positives forever.

"""

import sqlalchemy as sa
from alembic import op

revision = "e6f7a8b9c0d1"
down_revision = "d5e6f7a8b9c0"
branch_labels = None
depends_on = None


def _is_postgresql():
    return op.get_bind().dialect.name == "postgresql"


def upgrade():
    # NOT NULL in the same statement is safe *because* a server default is
    # supplied: PostgreSQL fills existing rows from it, and TimescaleDB
    # propagates the ADD COLUMN down to every chunk of the hypertable. It is
    # NOT NULL *without* a default that a populated hypertable rejects (see
    # y9z0a1b2c3d4). Ordering therefore matters: default first, never a bare
    # ADD COLUMN followed by SET NOT NULL.
    with op.batch_alter_table("request_logs") as batch_op:
        batch_op.add_column(
            sa.Column(
                "aborted",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            )
        )

    if _is_postgresql():
        op.execute(
            "COMMENT ON COLUMN request_logs.aborted IS "
            "$comment$Client disconnected before the stream completed; "
            "token counts may be estimated$comment$"
        )


def downgrade():
    with op.batch_alter_table("request_logs") as batch_op:
        batch_op.drop_column("aborted")
