"""Tests for make_metrics_middleware and _normalize_path."""


# ---------------------------------------------------------------------------
# _normalize_path
# ---------------------------------------------------------------------------

def test_normalize_path_no_ids():
    from lumen.blueprints.metrics.middleware import _normalize_path
    assert _normalize_path("/admin/groups") == "/admin/groups"


def test_normalize_path_single_id():
    from lumen.blueprints.metrics.middleware import _normalize_path
    assert _normalize_path("/admin/groups/42") == "/admin/groups/{id}"


def test_normalize_path_multiple_ids():
    from lumen.blueprints.metrics.middleware import _normalize_path
    assert _normalize_path("/admin/users/7/access/99/delete") == "/admin/users/{id}/access/{id}/delete"


def test_normalize_path_root():
    from lumen.blueprints.metrics.middleware import _normalize_path
    assert _normalize_path("/") == "/"


def test_normalize_path_empty():
    from lumen.blueprints.metrics.middleware import _normalize_path
    assert _normalize_path("") == ""


def test_normalize_path_leading_id_only():
    from lumen.blueprints.metrics.middleware import _normalize_path
    assert _normalize_path("/123") == "/{id}"


# ---------------------------------------------------------------------------
# make_metrics_middleware
# ---------------------------------------------------------------------------

def _fake_environ(path="/", method="GET"):
    return {"PATH_INFO": path, "REQUEST_METHOD": method}


def test_middleware_passes_through_response():
    from lumen.blueprints.metrics.middleware import make_metrics_middleware

    def fake_app(environ, start_response):
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"hello"]

    wrapped = make_metrics_middleware(fake_app)
    status_seen = []

    result = wrapped(_fake_environ("/chat"), lambda s, h, *_: status_seen.append(s))
    assert list(result) == [b"hello"]
    assert status_seen == ["200 OK"]


def test_middleware_captures_4xx_status():
    from lumen.blueprints.metrics.middleware import make_metrics_middleware

    def fake_app(environ, start_response):
        start_response("404 Not Found", [])
        return [b""]

    wrapped = make_metrics_middleware(fake_app)
    status_seen = []
    wrapped(_fake_environ("/missing"), lambda s, h, *_: status_seen.append(s))
    assert status_seen == ["404 Not Found"]


def test_middleware_normalizes_path_label(monkeypatch):
    """Numeric path segments are collapsed before being recorded as a label."""
    from lumen.blueprints.metrics import middleware as mw

    recorded = []
    orig = mw._http_requests.labels

    def spy(**kwargs):
        recorded.append(kwargs.get("path_template"))
        return orig(**kwargs)

    monkeypatch.setattr(mw._http_requests, "labels", spy)

    def fake_app(environ, start_response):
        start_response("200 OK", [])
        return []

    mw.make_metrics_middleware(fake_app)(
        _fake_environ("/admin/groups/123"), lambda *a: None
    )
    assert recorded and recorded[-1] == "/admin/groups/{id}"


def test_middleware_warns_when_a_context_survives_the_request(app, caplog):
    """A request that leaves an app context pushed past the response body's
    close() has escaped teardown; the middleware names it at that moment."""
    import logging

    from lumen.blueprints.metrics.middleware import make_metrics_middleware

    leaked = []

    def leaking_app(environ, start_response):
        ctx = app.app_context()
        ctx.push()  # never popped — the leak under investigation
        leaked.append(ctx)
        start_response("200 OK", [])
        return [b"ok"]

    wrapped = make_metrics_middleware(leaking_app)
    with caplog.at_level(logging.WARNING, logger="lumen.blueprints.metrics.middleware"):
        body = wrapped(_fake_environ("/v1/models"), lambda *a: None)
        list(body)
        # Only close() is the end of the request (the fixture's own ambient
        # context may additionally be reported at request start).
        assert not any("still current after" in r.getMessage() for r in caplog.records)
        body.close()
    leftover = [r.getMessage() for r in caplog.records if "still current after" in r.getMessage()]
    assert len(leftover) == 1
    assert "still current after GET /v1/models" in leftover[0]
    assert "stranded" in leftover[0]
    # Also kept for /metrics/debug, keyed to the leaked context's id.
    from lumen.services.ctx_probe import format_context_anomalies
    report = format_context_anomalies()
    assert f"leftover-context  ctx=0x{id(leaked[0]):x}" in report
    assert "left behind by GET /v1/models" in report
    leaked[0].pop()  # clean up for the other tests


def test_middleware_is_silent_for_a_balanced_request(app, caplog):
    """An ambient context around the request (test fixtures, nested dispatch)
    is the close()-check baseline, not a leftover — though it is reported once
    as ambient-at-start, since in production it means a poisoned thread."""
    import logging

    from lumen.blueprints.metrics.middleware import make_metrics_middleware

    def clean_app(environ, start_response):
        ctx = app.app_context()
        ctx.push()
        ctx.pop()
        start_response("200 OK", [])
        return [b"ok"]

    wrapped = make_metrics_middleware(clean_app)
    with app.app_context():  # ambient context present the whole time
        with caplog.at_level(logging.WARNING, logger="lumen.blueprints.metrics.middleware"):
            body = wrapped(_fake_environ("/v1/models"), lambda *a: None)
            list(body)
            body.close()
            # A second request on the same poisoned baseline reports nothing new.
            body2 = wrapped(_fake_environ("/v1/models"), lambda *a: None)
            list(body2)
            body2.close()
    messages = [r.getMessage() for r in caplog.records]
    assert not any("still current after" in m for m in messages)  # no leftover
    ambient = [m for m in messages if "already current at the start" in m]
    assert len(ambient) == 1  # reported once per context, not per request


def test_middleware_checks_the_exception_path(app, caplog):
    """A request that raises never gets a body close(); the leftover check
    must run on the exception path instead."""
    import logging

    import pytest

    from lumen.blueprints.metrics.middleware import make_metrics_middleware
    from lumen.services.ctx_probe import format_context_anomalies

    leaked = []

    def exploding_leaking_app(environ, start_response):
        ctx = app.app_context()
        ctx.push()  # never popped
        leaked.append(ctx)
        raise RuntimeError("boom")

    wrapped = make_metrics_middleware(exploding_leaking_app)
    with caplog.at_level(logging.WARNING, logger="lumen.blueprints.metrics.middleware"):
        with pytest.raises(RuntimeError, match="boom"):
            wrapped(_fake_environ("/v1/models"), lambda *a: None)
    assert any("raised" in r.getMessage() for r in caplog.records)
    assert f"leftover-context-after-exception  ctx=0x{id(leaked[0]):x}" in format_context_anomalies()
    leaked[0].pop()  # clean up


def test_middleware_heals_a_poisoned_thread(caplog):
    """Outside tests, an ambient context at request start is neutralized: the
    session registered under its key is closed and the contextvar cleared, so
    this and every later request on the thread pushes a fresh context."""
    import logging
    from unittest.mock import MagicMock

    from flask import Flask
    from flask.globals import _cv_app

    from lumen.blueprints.metrics.middleware import make_metrics_middleware
    from lumen.extensions import db

    prod_app = Flask("prod-like")  # testing defaults to False → heal path
    prior = _cv_app.get(None)  # the test fixture's own context, restored below
    ctx = prod_app.app_context()
    ctx.push()
    stuck_session = MagicMock()
    db.session.registry.registry[id(ctx)] = stuck_session

    def clean_app(environ, start_response):
        start_response("200 OK", [])
        return [b"ok"]

    wrapped = make_metrics_middleware(clean_app)
    try:
        with caplog.at_level(logging.WARNING, logger="lumen.blueprints.metrics.middleware"):
            body = wrapped(_fake_environ("/v1/models"), lambda *a: None)
            list(body)
            body.close()
        assert _cv_app.get(None) is None  # contextvar cleared
        assert id(ctx) not in db.session.registry.registry
        stuck_session.close.assert_called_once()
        assert any("(healing)" in r.getMessage() for r in caplog.records)
    finally:
        # The heal cleared the contextvar; drop the orphaned push token rather
        # than popping (pop would assert on the mismatched current context),
        # and restore the fixture's own context so its teardown pops cleanly.
        ctx._cv_tokens.clear()
        db.session.registry.registry.pop(id(ctx), None)
        if prior is not None:
            _cv_app.set(prior)


def test_middleware_records_500_on_app_exception():
    """If the wrapped app raises, status defaults to '500' and the exception propagates."""
    import pytest

    from lumen.blueprints.metrics.middleware import make_metrics_middleware

    def exploding_app(environ, start_response):
        raise RuntimeError("boom")

    wrapped = make_metrics_middleware(exploding_app)
    with pytest.raises(RuntimeError, match="boom"):
        wrapped(_fake_environ("/crash"), lambda *a: None)


# ---------------------------------------------------------------------------
# observe_stream_abort / lumen_stream_aborts_total
# ---------------------------------------------------------------------------

def _abort_count(source, reason):
    from prometheus_client import REGISTRY
    return REGISTRY.get_sample_value(
        "lumen_stream_aborts_total", {"source": source, "reason": reason}) or 0.0


def test_observe_stream_abort_increments_per_label_pair():
    """Each (source, reason) is its own series — 'clients are leaving' and 'the
    backend is broken' must not be summed into one number."""
    from lumen.blueprints.metrics.middleware import observe_stream_abort

    before = {
        ("chat", "disconnect"): _abort_count("chat", "disconnect"),
        ("api", "disconnect"): _abort_count("api", "disconnect"),
        ("api", "upstream_error"): _abort_count("api", "upstream_error"),
    }
    observe_stream_abort("api", "upstream_error")
    observe_stream_abort("api", "upstream_error")
    observe_stream_abort("chat", "disconnect")

    assert _abort_count("api", "upstream_error") == before[("api", "upstream_error")] + 2
    assert _abort_count("chat", "disconnect") == before[("chat", "disconnect")] + 1
    assert _abort_count("api", "disconnect") == before[("api", "disconnect")]


def test_stream_abort_counter_is_on_the_default_registry():
    """It must live on the default registry like the HTTP counters, so
    prometheus_client's multiprocess mode picks it up and /metrics exposes it."""
    from prometheus_client import REGISTRY, generate_latest

    from lumen.blueprints.metrics.middleware import observe_stream_abort

    observe_stream_abort("chat", "disconnect")
    scrape = generate_latest(REGISTRY).decode()
    assert "# TYPE lumen_stream_aborts_total counter" in scrape
    assert 'lumen_stream_aborts_total{reason="disconnect",source="chat"}' in scrape


# ---------------------------------------------------------------------------
# Streaming latency — the histogram must time the response the user waited for,
# not the microseconds it took to build a generator.
# ---------------------------------------------------------------------------

def _latency_sum(path):
    """Observed seconds for a path label, or 0.0 before any observation."""
    from prometheus_client import REGISTRY
    return REGISTRY.get_sample_value(
        "lumen_http_request_duration_seconds_sum",
        {"method": "POST", "path_template": path},
    ) or 0.0


def test_streaming_latency_is_measured_over_the_whole_body(monkeypatch):
    """A streaming view returns its generator instantly; the work happens later.

    Timing `wsgi_app()` therefore measured generator construction, so every SSE
    request — the ones this service exists to serve — recorded a near-zero
    duration however long the client actually waited. The observation belongs at
    close(), the last moment the request runs on this thread.
    """
    import time

    from lumen.blueprints.metrics.middleware import make_metrics_middleware

    body_time = 0.25
    path = "/stream-latency-probe"

    def streaming_app(environ, start_response):
        start_response("200 OK", [("Content-Type", "text/event-stream")])

        def generate():
            time.sleep(body_time)      # the part the user waits for
            yield b"data: done\n\n"

        return generate()

    before = _latency_sum(path)
    wrapped = make_metrics_middleware(streaming_app)
    body = wrapped(_fake_environ(path, "POST"), lambda s, h, *_: None)
    assert list(body) == [b"data: done\n\n"]
    body.close()

    observed = _latency_sum(path) - before
    assert observed >= body_time, (
        f"observed {observed:.3f}s for a response whose body took {body_time}s — "
        "the histogram is timing generator construction, not the request"
    )


def test_latency_is_still_recorded_when_the_app_raises(monkeypatch):
    """No body is returned, so nothing will ever call close() — the failure path
    has to observe its own latency or 500s vanish from the histogram."""
    from lumen.blueprints.metrics.middleware import make_metrics_middleware

    path = "/raising-latency-probe"

    def exploding_app(environ, start_response):
        raise RuntimeError("boom")

    import pytest

    before = _latency_sum(path)
    wrapped = make_metrics_middleware(exploding_app)
    with pytest.raises(RuntimeError):
        wrapped(_fake_environ(path, "POST"), lambda s, h, *_: None)
    assert _latency_sum(path) > before


def test_latency_is_observed_once_per_request():
    """close() can be called more than once; the histogram must not double-count."""
    from prometheus_client import REGISTRY

    from lumen.blueprints.metrics.middleware import make_metrics_middleware

    path = "/double-close-probe"

    def fake_app(environ, start_response):
        start_response("200 OK", [])
        return [b"x"]

    def count():
        return REGISTRY.get_sample_value(
            "lumen_http_request_duration_seconds_count",
            {"method": "POST", "path_template": path},
        ) or 0.0

    before = count()
    body = make_metrics_middleware(fake_app)(_fake_environ(path, "POST"), lambda s, h, *_: None)
    list(body)
    body.close()
    body.close()
    assert count() - before == 1
