"""How a rate-limited request answers the client.

Split out of test_api_auth.py so the rate-limit response contract has one
obvious home: two different conditions return 429 in this app (the limiter, and
an exhausted coin budget) and they need opposite reactions from the caller.
"""
from http import HTTPStatus

import pytest

from tests.routes.test_api_auth import (  # noqa: F401 - fixtures are used by name
    _allow_model,
    _capturing_openai,
    _chat_post,
    _NonStreamResponse,
    api_key,
    fresh_rate_limit,
)

# ---------------------------------------------------------------------------
# Rate-limit response shape
# ---------------------------------------------------------------------------

def test_rate_limited_response_carries_retry_after(
    app, client, monkeypatch, test_user, test_model, test_model_endpoint, api_key,
    fresh_rate_limit,
):
    """A bare 429 lets 300 clients retry on 300 independent schedules.

    The OpenAI SDK honours Retry-After, so supplying it is what stops a
    class-start burst from re-synchronising into a retry storm.
    """
    from lumen.blueprints.api import routes
    token, _ = api_key
    _allow_model(app, test_user, test_model)
    _capturing_openai(monkeypatch, routes, lambda **kw: _NonStreamResponse())

    # Spend the per-key budget, then one more.
    last = None
    for _ in range(40):
        last = _chat_post(client, token, test_model["model_name"], False)
        if last.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            break

    assert last.status_code == HTTPStatus.TOO_MANY_REQUESTS, "expected to hit the limiter"
    retry_after = last.headers.get("Retry-After")
    assert retry_after is not None, "429 must tell the client when to come back"
    assert retry_after.isdigit() and int(retry_after) >= 1
    # Derived from the limit's own window rather than hardcoded.
    assert int(retry_after) <= 3600

    body = last.get_json()
    assert body["error"]["type"] == "rate_limit_error"
    assert body["error"]["code"] == "rate_limit_exceeded"


def test_rate_limit_rejection_is_counted(
    app, client, monkeypatch, test_user, test_model, test_model_endpoint, api_key,
    fresh_rate_limit,
):
    """The rejection taxonomy has to distinguish 'slow down' from 'out of coins'.

    Both are 429s today and were indistinguishable in metrics; the model label
    is legitimately empty here because the request body is never parsed.
    """
    from lumen.blueprints.api import routes
    recorded = []
    monkeypatch.setattr(
        "lumen.blueprints.metrics.middleware.observe_rejection",
        lambda reason, source, model="": recorded.append((reason, source, model)),
    )
    token, _ = api_key
    _allow_model(app, test_user, test_model)
    _capturing_openai(monkeypatch, routes, lambda **kw: _NonStreamResponse())

    for _ in range(40):
        if _chat_post(client, token, test_model["model_name"], False).status_code == HTTPStatus.TOO_MANY_REQUESTS:
            break

    assert ("rate_limit", "api", "") in recorded
