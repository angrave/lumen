"""Tests for connection-pool checkout tracking and the pool watchdog."""
import logging

from lumen.services import pool_tracker


def _fake_record():
    """Stand-in for a SQLAlchemy _ConnectionRecord; only its identity is used."""
    return object()


def test_checkout_is_tracked_and_released(app):
    record = _fake_record()
    pool_tracker._on_checkout(None, record, None)
    try:
        entry = pool_tracker._outstanding[id(record)]
        assert entry.thread
        assert "test_checkout_is_tracked_and_released" in entry.stack
    finally:
        pool_tracker._on_checkin(None, record)
    assert id(record) not in pool_tracker._outstanding


def test_checkout_records_request_endpoint(app):
    with app.test_request_context("/chat"):
        record = _fake_record()
        pool_tracker._on_checkout(None, record, None)
        try:
            assert pool_tracker._outstanding[id(record)].endpoint == "chat.chat_page"
        finally:
            pool_tracker._on_checkin(None, record)


def test_stranded_count_only_counts_old_checkouts(app):
    baseline = pool_tracker.stranded_count()
    record = _fake_record()
    pool_tracker._on_checkout(None, record, None)
    try:
        assert pool_tracker.stranded_count() == baseline
        # Age the checkout past the stranded threshold.
        entry = pool_tracker._outstanding[id(record)]
        pool_tracker._outstanding[id(record)] = entry._replace(
            at=entry.at - pool_tracker.STRANDED_AFTER - 1
        )
        assert pool_tracker.stranded_count() == baseline + 1
        assert "held" in pool_tracker.format_outstanding(min_age=pool_tracker.STRANDED_AFTER)
    finally:
        pool_tracker._on_checkin(None, record)


def test_format_outstanding_empty():
    # No checkout is a day old, so the filtered listing is empty.
    assert pool_tracker.format_outstanding(min_age=86400).strip() == "(none)"


def test_thread_dump_includes_current_thread():
    dump = pool_tracker.thread_dump()
    assert "--- thread" in dump
    assert "test_thread_dump_includes_current_thread" in dump


def test_watchdog_logs_after_consecutive_near_capacity_scrapes(caplog):
    pool_tracker._pressure_count = 0
    record = _fake_record()
    pool_tracker._on_checkout(None, record, None)
    try:
        with caplog.at_level(logging.ERROR, logger="lumen.services.pool_tracker"):
            for _ in range(pool_tracker._PRESSURE_SCRAPES - 1):
                pool_tracker.watchdog(75, 80)
            assert caplog.records == []

            pool_tracker.watchdog(75, 80)
            assert len(caplog.records) == 1
            assert "DB pool at 75/80" in caplog.records[0].getMessage()

            # Logs once per episode, not on every subsequent scrape.
            pool_tracker.watchdog(75, 80)
            assert len(caplog.records) == 1
    finally:
        pool_tracker._on_checkin(None, record)
        pool_tracker._pressure_count = 0


def test_watchdog_resets_when_pool_recovers(caplog):
    pool_tracker._pressure_count = 0
    try:
        with caplog.at_level(logging.ERROR, logger="lumen.services.pool_tracker"):
            pool_tracker.watchdog(75, 80)
            pool_tracker.watchdog(10, 80)  # recovered — counter resets
            assert pool_tracker._pressure_count == 0
            for _ in range(pool_tracker._PRESSURE_SCRAPES - 1):
                pool_tracker.watchdog(75, 80)
            assert caplog.records == []
    finally:
        pool_tracker._pressure_count = 0


def test_watchdog_noop_without_limit():
    pool_tracker._pressure_count = 0
    pool_tracker.watchdog(75, 0)
    assert pool_tracker._pressure_count == 0
