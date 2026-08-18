"""Add per-request timing columns to request_logs

Revision ID: f7a8b9c0d1e2
Revises: e6f7a8b9c0d1
Create Date: 2026-08-18 00:00:00.000000

``request_logs`` has had exactly one timing column, ``duration``, and it does
not mean what an operator assumes. It starts *after* the model lookup, endpoint
selection and coin-budget checks have already run, and it ends before the
billing commit. ``time`` is stamped after that commit. So the row records how
long the upstream call took, and says nothing about how long the *user* waited.

That gap is why a class-start incident is currently undiagnosable: "queued
behind other students for a worker thread", "waiting on a contended connection
pool", and "the model itself is slow" all produce the same row.

Seven columns, all riding the INSERT that already happens in ``update_stats``,
so the hot path gains no statement:

  started_at    absolute arrival at the ASGI bridge (T0)
  queue_wait    T1-T0, waiting for a WSGI worker thread
  preflight     T2-T1, auth/lookup/budget/pool checkout
  ttft          first upstream chunk of any kind
  ttft_visible  first visible content delta
  send_blocked  time blocked handing chunks to the server
  outcome       ok | disconnect

``started_at`` is stored rather than derived. The obvious reconstruction,
``time - duration - queue_wait``, is wrong: it silently assumes preflight and
the billing commit take zero time, and both are largest exactly during the
burst the column exists to explain, so the error is worst when the data matters
most.

``started_at`` is TIMESTAMPTZ, unlike every other timestamp column in the
schema (naive UTC, per CLAUDE.md). It exists to be compared and subtracted
against ``time`` on the same row, and mixing naive with aware in that
arithmetic is a Postgres footgun. It must be written with
``datetime.now(timezone.utc)``.

``outcome`` carries only the two values the code can actually write. In
particular there is deliberately no ``billing_error``: the row is created by
``update_stats``, which only flushes, so a failing commit rolls back the very
row that would have recorded the failure. A value that can never be written
would make the column disagree with the abort counter beside it. Add values
together with the code that writes them, never ahead of it.

No backfill, following ``e6f7a8b9c0d1``. Historical rows have no arrival time
to recover — nothing recorded one — and inventing one from ``time - duration``
would produce a column that looks authoritative and is quietly wrong for every
pre-migration row. NULL means "not measured", which is honest and easy to
filter. That is also why the float columns carry ``server_default='0'`` for
Timescale's benefit while the ORM leaves them nullable: a request that genuinely
did not pass through the ASGI bridge (dev server, test client) writes NULL
rather than a fictitious zero.

"""

import sqlalchemy as sa
from alembic import op

revision = "f7a8b9c0d1e2"
down_revision = "e6f7a8b9c0d1"
branch_labels = None
depends_on = None


def _is_postgresql():
    return op.get_bind().dialect.name == "postgresql"


# (name, type, server_default) — nullable in every case.
_COLUMNS = [
    ("started_at", sa.DateTime(timezone=True), None),
    ("queue_wait", sa.Float(), "0"),
    ("preflight", sa.Float(), "0"),
    ("ttft", sa.Float(), "0"),
    ("ttft_visible", sa.Float(), "0"),
    ("send_blocked", sa.Float(), "0"),
    ("outcome", sa.String(16), None),
]


def upgrade():
    # Nullable WITH a server default on the numeric columns. A populated
    # TimescaleDB hypertable rejects a NOT NULL column that has no default
    # (see y9z0a1b2c3d4); supplying one lets the ADD COLUMN propagate to every
    # existing chunk. These stay nullable regardless, because "not measured"
    # has to remain distinguishable from "measured as zero".
    with op.batch_alter_table("request_logs") as batch_op:
        for name, type_, default in _COLUMNS:
            batch_op.add_column(
                sa.Column(name, type_, nullable=True, server_default=default)
            )

    if _is_postgresql():
        # The operator queries all filter by model and time; this is the
        # composite they need, and it is absent today.
        op.create_index(
            "ix_request_logs_model_config_id_time",
            "request_logs",
            ["model_config_id", sa.text("time DESC")],
        )
        for name, comment in [
            ("started_at", "UTC instant the request arrived at the ASGI bridge (T0). TIMESTAMPTZ to match `time` for same-row arithmetic; null when the request did not pass through the bridge"),
            ("queue_wait", "Seconds waiting for a WSGI worker thread (T1-T0)"),
            ("preflight", "Seconds from worker pickup to the upstream call (T2-T1). Composition differs by path: audio is dominated by upload parsing, the chat stream spans two preflights"),
            ("ttft", "Seconds to the first upstream chunk of any kind, including reasoning deltas"),
            ("ttft_visible", "Seconds to the first visible content delta; trails ttft by the thinking phase on a reasoning model"),
            ("send_blocked", "Seconds blocked handing chunks to the server; a large share means a slow client, not a slow backend"),
            ("outcome", "How the request ended: ok | disconnect. Null means the row predates this column"),
        ]:
            op.execute(
                f"COMMENT ON COLUMN request_logs.{name} IS '{comment}'"
            )


def downgrade():
    if _is_postgresql():
        op.drop_index("ix_request_logs_model_config_id_time", table_name="request_logs")
    with op.batch_alter_table("request_logs") as batch_op:
        for name, _type, _default in reversed(_COLUMNS):
            batch_op.drop_column(name)
