"""The Phase 8 operator commands, run as an operator runs them.

These are subprocess tests on purpose. ``backfill-aggregate`` exists because
``CALL refresh_continuous_aggregate`` cannot run inside a transaction block and a
``flask`` command using ``db.session`` is already in one; the only way to prove the
command opens its own AUTOCOMMIT connection is to run the real command against a
real TimescaleDB and see it succeed. A version that "looks right" fails with
``cannot run inside a transaction block`` in the middle of a maintenance window,
which is exactly the failure these tests exist to catch — so they invoke
``uv run flask ...`` the same way ``tests/integration/conftest.py`` invokes
``flask db upgrade``.

These commands mutate state that is global to the database — they add and remove
retention policies and they move every aggregate's watermark — so this module runs
against ``pg_migrated_isolated`` rather than the shared ``pg_migrated``.

``request_counts_hourly_by_entity`` is created by a migration owned by another
change; the tests that need it skip cleanly while it is absent.
"""

import os
import subprocess
import threading
import time
from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from tests.integration.conftest import TEST_CONFIG

pytestmark = pytest.mark.postgres

ENTITY_AGGREGATE = "request_counts_hourly_by_entity"
# request_logs.source is VARCHAR(8); this tags the rows these tests insert so the
# cleanup fixture can remove exactly them between tests.
SOURCE = "clitest"


def _run(pg_url, *args):
    env = {
        **os.environ,
        "DATABASE_URL": pg_url,
        "CONFIG_YAML": TEST_CONFIG,
        "BACKGROUND_WORKER": "false",
    }
    return subprocess.run(
        ["uv", "run", "flask", "--app", "run", *args],
        capture_output=True, text=True, env=env, timeout=300,
    )


def _autocommit(engine):
    return engine.connect().execution_options(isolation_level="AUTOCOMMIT")


def _retention_drop_after(engine):
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT config->>'drop_after' FROM timescaledb_information.jobs "
            "WHERE proc_name = 'policy_retention' AND hypertable_name = 'request_logs'"
        )).scalar()


def _aggregate_exists(engine, name):
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT COUNT(*) FROM timescaledb_information.continuous_aggregates "
            "WHERE view_name = :v"
        ), {"v": name}).scalar() == 1


def _months_ago(n):
    """A YYYY-MM string at least ``n`` months back, for --from."""
    now = datetime.now(timezone.utc)
    total = now.year * 12 + (now.month - 1) - n
    return f"{total // 12:04d}-{total % 12 + 1:02d}"


@pytest.fixture(scope="module")
def pg_url(pg_migrated_isolated):
    return pg_migrated_isolated[0]


@pytest.fixture(scope="module")
def pg_migrated(pg_migrated_isolated):
    return pg_migrated_isolated[1]


@pytest.fixture
def lifecycle_db(pg_migrated):
    """Leave the database exactly as this test found it.

    Every test here asserts on unqualified aggregate totals and on whether a
    retention policy exists, so one test's rows and policies would fail the next.
    Raw rows are deleted and every aggregate is then recomputed over the window
    they occupied, which drops their materialised rows. The window is bounded and
    ends at ``now()`` on purpose: ``NULL, NULL`` would move each aggregate's
    watermark past the current bucket and disable real-time aggregation for the
    tests that run after this one.
    """
    yield pg_migrated
    with _autocommit(pg_migrated) as conn:
        if conn.execute(text(
            "SELECT COUNT(*) FROM timescaledb_information.jobs "
            "WHERE proc_name = 'policy_retention' AND hypertable_name = 'request_logs'"
        )).scalar():
            conn.execute(text("SELECT remove_retention_policy('request_logs')"))
        conn.execute(text("DELETE FROM request_logs WHERE source = :s"), {"s": SOURCE})
        for view in conn.execute(text(
            "SELECT view_name FROM timescaledb_information.continuous_aggregates"
        )).scalars().all():
            conn.execute(text(
                f"CALL refresh_continuous_aggregate('{view}', now() - INTERVAL '30 days', now())"
            ))


@pytest.fixture
def recent_row(lifecycle_db):
    with lifecycle_db.begin() as conn:
        conn.execute(text(
            "INSERT INTO request_logs (time, source, input_tokens, output_tokens, cost, duration) "
            "VALUES (now() - INTERVAL '3 days', :s, 11, 22, 0.25, 1.0)"
        ), {"s": SOURCE})
    return lifecycle_db


def test_refresh_inside_a_transaction_block_is_rejected(pg_migrated):
    """The premise of the whole command: this is what db.session would do.

    If a future TimescaleDB ever allows the CALL inside a transaction this test
    fails, and the AUTOCOMMIT connection can be reconsidered deliberately rather
    than removed on a hunch.
    """
    with pytest.raises(Exception) as exc:
        with pg_migrated.begin() as conn:
            conn.execute(text(
                "CALL refresh_continuous_aggregate('request_counts_hourly', NULL, NULL)"
            ))
    assert "transaction block" in str(exc.value)


def test_backfill_aggregate_runs_outside_a_transaction_block(pg_url, recent_row):
    """Fails if the command is ever changed to refresh through ``db.session``."""
    result = _run(pg_url, "backfill-aggregate",
                  "--name", "request_counts_hourly", "--from", _months_ago(1))
    output = result.stdout + result.stderr
    assert "cannot run inside a transaction block" not in output, output
    assert result.returncode == 0, output
    assert "request_logs rows" in result.stdout, output
    assert "Backfill complete:" in result.stdout, output

    with recent_row.connect() as conn:
        materialised = conn.execute(text(
            "SELECT SUM(requests) FROM request_counts_hourly"
        )).scalar()
    assert materialised == 1


def test_backfill_aggregate_refuses_window_before_retention_boundary(pg_url, recent_row):
    with _autocommit(recent_row) as conn:
        conn.execute(text(
            "SELECT add_retention_policy('request_logs', drop_after => INTERVAL '13 months')"
        ))

    result = _run(pg_url, "backfill-aggregate",
                  "--name", "request_counts_hourly", "--from", _months_ago(15))
    assert result.returncode != 0, result.stdout + result.stderr
    assert "refusing to refresh" in result.stdout
    assert "EMPTY and DELETES" in result.stdout


def test_backfill_aggregate_force_overrides_the_retention_refusal(pg_url, recent_row):
    with _autocommit(recent_row) as conn:
        conn.execute(text(
            "SELECT add_retention_policy('request_logs', drop_after => INTERVAL '13 months')"
        ))

    result = _run(pg_url, "backfill-aggregate", "--name", "request_counts_hourly",
                  "--from", _months_ago(15), "--force")
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert "WARNING" in result.stdout
    assert "outside retention" in result.stdout
    assert "Backfill complete:" in result.stdout


def test_enable_retention_dry_run_adds_no_policy(pg_url, lifecycle_db):
    """The default is a report. Nothing about the database may change."""
    result = _run(pg_url, "enable-retention")
    assert "Retention window: 13 months" in result.stdout, result.stdout + result.stderr
    assert "rows that would eventually be dropped" in result.stdout
    assert "request_counts_hourly: earliest bucket" in result.stdout
    assert _retention_drop_after(lifecycle_db) is None, "dry run added a retention policy"


def test_enable_retention_dry_run_reports_and_exits_clean_once_backfilled(pg_url, recent_row):
    if not _aggregate_exists(recent_row, ENTITY_AGGREGATE):
        pytest.skip(f"{ENTITY_AGGREGATE} migration is not on disk yet")

    backfill = _run(pg_url, "backfill-aggregate", "--from", _months_ago(1))
    assert backfill.returncode == 0, backfill.stdout + backfill.stderr

    result = _run(pg_url, "enable-retention")
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert f"{ENTITY_AGGREGATE} holds" in result.stdout
    assert "Dry run: no policy added." in result.stdout
    assert _retention_drop_after(recent_row) is None, "dry run added a retention policy"


def test_enable_retention_force_refuses_without_a_backfilled_aggregate(pg_url, lifecycle_db):
    """Retention over an empty entity aggregate destroys history held nowhere else."""
    result = _run(pg_url, "enable-retention", "--force")
    assert result.returncode != 0, result.stdout + result.stderr
    if _aggregate_exists(lifecycle_db, ENTITY_AGGREGATE):
        assert "is empty" in result.stdout
        assert "flask backfill-aggregate" in result.stdout
    else:
        assert "does not exist" in result.stdout
    assert _retention_drop_after(lifecycle_db) is None, "a refused run added a retention policy"


def test_enable_retention_force_enables_the_policy_after_a_backfill(pg_url, recent_row):
    if not _aggregate_exists(recent_row, ENTITY_AGGREGATE):
        pytest.skip(f"{ENTITY_AGGREGATE} migration is not on disk yet")

    backfill = _run(pg_url, "backfill-aggregate", "--from", _months_ago(1))
    assert backfill.returncode == 0, backfill.stdout + backfill.stderr

    result = _run(pg_url, "enable-retention", "--window", "13 months", "--force")
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert "Retention enabled on request_logs" in result.stdout
    assert _retention_drop_after(recent_row) == "1 year 1 mon"


def test_backfill_leaves_real_time_aggregation_working_for_the_current_bucket(pg_url, recent_row):
    """The backfill must not hide requests made after it runs.

    Refreshing past ``now`` materialises the current bucket and moves the aggregate's
    watermark beyond it, which switches real-time aggregation off for that bucket: a
    student running requests right after a backfill would see zero on /usage until a
    scheduled refresh caught up. Measured on 2.27.2 — this fails if the month loop
    ever stops clamping its window end to ``now()``.

    Uses the entity aggregate because it is the one with
    ``materialized_only = false``; the older ``request_counts_hourly`` has no
    real-time aggregation to lose.
    """
    if not _aggregate_exists(recent_row, ENTITY_AGGREGATE):
        pytest.skip(f"{ENTITY_AGGREGATE} migration is not on disk yet")

    result = _run(pg_url, "backfill-aggregate", "--from", _months_ago(1))
    assert result.returncode == 0, result.stdout + result.stderr

    with recent_row.begin() as conn:
        conn.execute(text(
            "INSERT INTO request_logs (time, source, input_tokens, output_tokens, cost, duration) "
            "VALUES (now(), :s, 1, 1, 0.01, 0.1)"
        ), {"s": SOURCE})
    with recent_row.connect() as conn:
        total = conn.execute(text(
            f"SELECT SUM(requests) FROM {ENTITY_AGGREGATE} WHERE source = :s"
        ), {"s": SOURCE}).scalar()
    assert total == 2, "a request logged after the backfill is invisible in the aggregate"


def _wait_for(predicate, timeout=60.0, interval=0.25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_backfill_retries_a_concurrent_refresh_instead_of_aborting(pg_url, recent_row):
    """A real SQLSTATE 55P03, not an injected one.

    TimescaleDB refuses an overlapping refresh rather than queueing behind it, and the
    aggregates ship with policies firing every minute and every hour — so a long backfill
    will meet one. Forcing it deterministically: a holder session takes ACCESS EXCLUSIVE
    on the aggregate's materialisation hypertable, then a second session starts a genuine
    refresh, which acquires TimescaleDB's concurrency guard and *then* blocks on the table
    lock. While it sits there, every other refresh — including the CLI's — fails with
    55P03 in about 20ms. The holder is released as soon as the CLI is seen to have tried,
    which lets the blocked refresh finish and the CLI's next retry succeed.

    Without the retry the command exits non-zero partway through the run.
    """
    engine = recent_row
    with engine.connect() as conn:
        mat = conn.execute(text(
            "SELECT materialization_hypertable_schema || '.' || materialization_hypertable_name "
            "FROM timescaledb_information.continuous_aggregates "
            "WHERE view_name = 'request_counts_hourly'"
        )).scalar()

    holder = engine.connect()
    holder.execute(text(f"LOCK TABLE {mat} IN ACCESS EXCLUSIVE MODE"))

    blocked = {}

    def blocked_refresh():
        with _autocommit(engine) as conn:
            try:
                conn.execute(text(
                    "CALL refresh_continuous_aggregate('request_counts_hourly', "
                    "now() - INTERVAL '400 days', now())"
                ))
                blocked["outcome"] = "ok"
            except Exception as exc:
                blocked["outcome"] = repr(exc)

    refresher = threading.Thread(target=blocked_refresh)
    refresher.start()

    def refreshers_running(minimum):
        with engine.connect() as conn:
            return conn.execute(text(
                "SELECT COUNT(*) FROM pg_stat_activity WHERE pid <> pg_backend_pid() "
                "AND datname = current_database() "
                "AND query LIKE 'CALL refresh_continuous_aggregate%'"
            )).scalar() >= minimum

    assert _wait_for(lambda: refreshers_running(1), timeout=30), \
        "the blocking refresh never started"

    # Release only once a second backend has attempted a refresh — that is the CLI
    # hitting 55P03. Releasing on a fixed timer would make the test a race.
    releaser = threading.Thread(
        target=lambda: (_wait_for(lambda: refreshers_running(2), timeout=90),
                        holder.rollback()),
        daemon=True,
    )
    releaser.start()
    try:
        result = _run(pg_url, "backfill-aggregate",
                      "--name", "request_counts_hourly", "--from", _months_ago(1))
    finally:
        releaser.join(timeout=120)
        holder.rollback()
        holder.close()
        refresher.join(timeout=120)

    output = result.stdout + result.stderr
    assert blocked.get("outcome") == "ok", blocked
    assert result.returncode == 0, output
    assert "lock retries" in result.stdout, output
    assert "Backfill complete:" in result.stdout, output


def test_backfill_is_idempotent_so_from_can_resume_mid_run(pg_url, recent_row):
    """Re-running an already-refreshed month recomputes it from raw and overwrites.

    That is what makes the resume hint on a failed month safe: an operator can re-run
    from any month without double-counting or losing rows, as long as the raw chunks are
    still there — which the retention guard is what enforces.
    """
    totals = []
    for _ in range(2):
        result = _run(pg_url, "backfill-aggregate",
                      "--name", "request_counts_hourly", "--from", _months_ago(1))
        assert result.returncode == 0, result.stdout + result.stderr
        with recent_row.connect() as conn:
            totals.append(conn.execute(text(
                "SELECT SUM(requests), SUM(input_tokens), SUM(output_tokens) "
                "FROM request_counts_hourly"
            )).one())
    assert totals[0] == totals[1], f"re-running a month changed the aggregate: {totals}"
