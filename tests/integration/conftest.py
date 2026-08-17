"""Fixtures for tests that need a real PostgreSQL + TimescaleDB.

The rest of the suite runs on SQLite via ``db.create_all()`` (see the root
``conftest.py``), which never executes a line of Alembic and therefore never
creates the ``request_logs`` hypertable, its continuous aggregate, or any of the
``/api/usage/*`` SQL — those endpoints short-circuit on the dialect check before
reaching a query. Anything Timescale-shaped is untested without this.

Migrations run through ``flask db upgrade`` in a subprocess rather than through
Alembic's Python API, because that is verbatim what ``entrypoint.sh`` does in
production: the thing under test is the command the container actually runs.
"""

import os
import subprocess
import uuid

import pytest
from sqlalchemy import create_engine, text

# Set by CI (see .github/workflows/test.yml). Absent locally unless a developer
# exports it, in which case these tests skip rather than fail.
PG_URL_ENV = "LUMEN_TEST_POSTGRES_URL"


def _admin_engine(url: str):
    # AUTOCOMMIT: CREATE DATABASE cannot run inside a transaction block.
    return create_engine(url, isolation_level="AUTOCOMMIT")


@pytest.fixture(scope="session")
def pg_url():
    """A freshly created, disposable database on the CI PostgreSQL service."""
    base = os.environ.get(PG_URL_ENV)
    if not base:
        pytest.skip(f"{PG_URL_ENV} is not set; skipping PostgreSQL/TimescaleDB tests")

    # A per-run database so a failed run never poisons the next one, and so
    # these tests cannot touch anything a developer pointed the variable at.
    name = f"lumen_test_{uuid.uuid4().hex[:12]}"
    admin = _admin_engine(base)
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    admin.dispose()

    url = base.rsplit("/", 1)[0] + "/" + name
    yield url

    admin = _admin_engine(base)
    with admin.connect() as conn:
        # Terminate stragglers first; DROP DATABASE fails while anything is connected.
        conn.execute(text(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = :n"
        ), {"n": name})
        conn.execute(text(f'DROP DATABASE IF EXISTS "{name}"'))
    admin.dispose()


@pytest.fixture(scope="session")
def pg_migrated(pg_url):
    """Run the real migration chain against the disposable database.

    Returns an engine bound to it. The migration is the artifact under test, so
    a failure here is a test failure, not a fixture error — hence the explicit
    check on the return code with the output attached.
    """
    env = {**os.environ, "DATABASE_URL": pg_url}
    result = subprocess.run(
        ["uv", "run", "flask", "--app", "run", "db", "upgrade"],
        capture_output=True, text=True, env=env, timeout=300,
    )
    assert result.returncode == 0, (
        f"flask db upgrade failed against PostgreSQL:\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
    engine = create_engine(pg_url)
    yield engine
    engine.dispose()
