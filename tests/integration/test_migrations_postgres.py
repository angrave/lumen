"""The migration chain, run against a real PostgreSQL + TimescaleDB.

These are the tests the SQLite suite structurally cannot be: they prove that
``request_logs`` really is a hypertable, that the continuous aggregate exists and
refreshes, and that the extension the container cannot boot without is actually
required. If someone replaces the hypertable with a plain table, at least one
test here must fail — otherwise this file is decoration.
"""

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.postgres


def test_timescaledb_extension_is_installed(pg_migrated):
    """The migration runs CREATE EXTENSION unconditionally on PostgreSQL.

    This is not a nice-to-have: ``entrypoint.sh`` runs ``flask db upgrade``
    before ``exec uvicorn``, so a database without the extension fails the
    migration and the container never starts.
    """
    with pg_migrated.connect() as conn:
        version = conn.execute(text(
            "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
        )).scalar()
    assert version is not None, "timescaledb extension is not installed"


def test_request_logs_is_a_hypertable(pg_migrated):
    """The gate: this fails if request_logs is ever demoted to a plain table."""
    with pg_migrated.connect() as conn:
        row = conn.execute(text(
            "SELECT hypertable_name FROM timescaledb_information.hypertables "
            "WHERE hypertable_name = 'request_logs'"
        )).scalar()
    assert row == "request_logs", (
        "request_logs is not a hypertable — the analytics queries, the chunk "
        "interval and any future retention/compression policy all depend on it"
    )


def test_request_logs_chunk_interval_is_seven_days(pg_migrated):
    with pg_migrated.connect() as conn:
        interval = conn.execute(text(
            "SELECT time_interval FROM timescaledb_information.dimensions "
            "WHERE hypertable_name = 'request_logs' AND column_name = 'time'"
        )).scalar()
    assert interval is not None and interval.days == 7, f"unexpected chunk interval: {interval!r}"


def test_continuous_aggregate_exists(pg_migrated):
    with pg_migrated.connect() as conn:
        name = conn.execute(text(
            "SELECT view_name FROM timescaledb_information.continuous_aggregates "
            "WHERE view_name = 'request_counts_hourly'"
        )).scalar()
    assert name == "request_counts_hourly"


def test_continuous_aggregate_reflects_inserted_rows(pg_migrated):
    """Insert, refresh, read back.

    The aggregate's own policy lags real time by at least an hour, so the
    refresh has to be explicit — the same reason ``seed_analytics.py`` calls it
    directly. Rows are dated well into the past so they fall outside the
    policy's ``end_offset`` window and are eligible for materialisation.
    """
    with pg_migrated.begin() as conn:
        conn.execute(text("""
            INSERT INTO request_logs (time, source, input_tokens, output_tokens, cost, duration)
            VALUES (now() - INTERVAL '3 days', 'api', 100, 200, 0.5, 1.5),
                   (now() - INTERVAL '3 days', 'api', 300, 400, 1.5, 2.5)
        """))

    # refresh_continuous_aggregate cannot run inside a transaction block.
    with pg_migrated.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(
            "CALL refresh_continuous_aggregate('request_counts_hourly', NULL, NULL)"
        ))

    with pg_migrated.connect() as conn:
        requests, in_tok, out_tok = conn.execute(text(
            "SELECT SUM(requests), SUM(input_tokens), SUM(output_tokens) "
            "FROM request_counts_hourly"
        )).one()

    assert requests == 2
    assert in_tok == 400
    assert out_tok == 600


def test_migration_is_at_a_single_head(pg_migrated):
    """A merge that leaves two heads makes `db upgrade` ambiguous in production."""
    with pg_migrated.connect() as conn:
        heads = conn.execute(text("SELECT version_num FROM alembic_version")).scalars().all()
    assert len(heads) == 1, f"expected one alembic head, got {heads}"
