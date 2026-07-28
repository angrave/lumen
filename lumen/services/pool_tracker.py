"""Connection-pool checkout tracking, for attributing leaked DB connections.

The ``lumen_db_pool_connections`` gauges show *how many* connections are checked
out, not *who* holds them. A leak — a connection checked out and never returned,
so the pool creates a replacement and its ``checked_out`` count climbs and never
falls back — is invisible in the metrics beyond the climb itself.

This module records the endpoint, thread and stack of every checkout and drops
the record on check-in, so whatever is still outstanding after minutes is the
leak, named by call site. It is always on: a checkout happens a handful of times
per request and capturing a bounded stack costs microseconds against LLM calls
measured in seconds.

Exposed through ``/metrics/debug`` (see the metrics blueprint) and logged
automatically by :func:`watchdog` when the pool sits near capacity.
"""

import logging
import sys
import threading
import time
import traceback
from typing import NamedTuple

from flask import has_request_context, request
from sqlalchemy import event
from sqlalchemy.pool import Pool

logger = logging.getLogger(__name__)

# Stack frames captured per checkout: deep enough to cross the SQLAlchemy and
# Flask-SQLAlchemy plumbing and reach the application frame that triggered it.
_STACK_DEPTH = 25
# A checkout held longer than this is reported as stranded — no legitimate call
# site holds a connection across an LLM call (they all release it first).
STRANDED_AFTER = 300.0
# Consecutive near-capacity scrapes before the watchdog dumps stacks.
_PRESSURE_SCRAPES = 3

_lock = threading.Lock()
_outstanding: dict = {}
_registered = False
_pressure_count = 0


class Checkout(NamedTuple):
    """One outstanding pool checkout."""

    endpoint: str
    thread: str
    at: float  # time.monotonic() when the connection was checked out
    stack: str

    def age(self, now: float = None) -> float:
        return (now if now is not None else time.monotonic()) - self.at


def _on_checkout(dbapi_connection, connection_record, connection_proxy):
    endpoint = "no-request-context"
    if has_request_context():
        endpoint = request.endpoint or request.path
    record = Checkout(
        endpoint=endpoint,
        thread=threading.current_thread().name,
        at=time.monotonic(),
        stack="".join(traceback.format_stack(limit=_STACK_DEPTH)[:-1]),
    )
    with _lock:
        _outstanding[id(connection_record)] = record


def _on_checkin(dbapi_connection, connection_record):
    with _lock:
        _outstanding.pop(id(connection_record), None)


def init_pool_tracking():
    """Register the checkout/check-in listeners. Idempotent.

    Listens on the ``Pool`` class rather than a specific engine so no app context
    (and no engine creation) is needed at registration time.
    """
    global _registered
    if _registered:
        return
    event.listen(Pool, "checkout", _on_checkout)
    event.listen(Pool, "checkin", _on_checkin)
    _registered = True


def outstanding(min_age: float = 0.0) -> list:
    """Outstanding checkouts held at least ``min_age`` seconds, oldest first."""
    now = time.monotonic()
    with _lock:
        records = list(_outstanding.values())
    return sorted((r for r in records if r.age(now) >= min_age), key=lambda r: r.at)


def stranded_count() -> int:
    """Number of checkouts held long enough to be considered leaked."""
    return len(outstanding(STRANDED_AFTER))


def format_outstanding(min_age: float = 0.0, limit: int = 25) -> str:
    records = outstanding(min_age)
    if not records:
        return "(none)\n"
    now = time.monotonic()
    lines = [f"{len(records)} outstanding checkout(s), oldest first"]
    if len(records) > limit:
        lines.append(f"(showing the {limit} oldest)")
    for r in records[:limit]:
        lines.append(
            f"\n--- held {r.age(now):.0f}s  endpoint={r.endpoint}  thread={r.thread} ---\n{r.stack}"
        )
    return "\n".join(lines) + "\n"


def thread_dump() -> str:
    """Stacks of every live thread — identifies a worker wedged mid-request."""
    names = {t.ident: t.name for t in threading.enumerate()}
    parts = []
    for ident, frame in sys._current_frames().items():
        parts.append(f"\n--- thread {names.get(ident, 'unknown')} ({ident}) ---\n")
        parts.extend(traceback.format_stack(frame))
    return "".join(parts)


def watchdog(checked_out: float, limit: float):
    """Log outstanding-checkout stacks once the pool stays near capacity.

    Called from the metrics collector on every scrape, so "consecutive scrapes"
    is measured in scrape intervals. Logs once per episode (not on every scrape)
    to keep an exhausted pool from flooding the log, and resets when the pool
    recovers.
    """
    global _pressure_count
    if not limit or checked_out < 0.9 * limit:
        _pressure_count = 0
        return
    _pressure_count += 1
    if _pressure_count != _PRESSURE_SCRAPES:
        return
    logger.error(
        "DB pool at %d/%d checked out for %d consecutive scrapes; %d checkout(s) "
        "held over %.0fs. Outstanding checkouts:\n%s",
        checked_out, limit, _PRESSURE_SCRAPES, stranded_count(), STRANDED_AFTER,
        format_outstanding(),
    )
