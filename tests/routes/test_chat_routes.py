"""Tests for chat routes (conversations, stream validation, access control)."""
from datetime import datetime, timezone
from http import HTTPStatus


def _grant_unlimited_pool(app, entity_id):
    from lumen.extensions import db
    from lumen.models.entity_limit import EntityLimit
    db.session.add(EntityLimit(
        entity_id=entity_id, max_coins=-2, refresh_coins=0, starting_coins=0,
    ))
    db.session.commit()


def test_list_conversations_empty(auth_client):
    resp = auth_client.get("/chat/conversations")
    assert resp.status_code == HTTPStatus.OK
    assert resp.get_json()["conversations"] == []


def test_list_conversations_with_data(app, auth_client, test_user):
    with app.app_context():
        from lumen.extensions import db
        from lumen.models.conversation import Conversation
        conv = Conversation(entity_id=test_user["id"], title="Test Chat", model="test-model")
        db.session.add(conv)
        db.session.commit()

    resp = auth_client.get("/chat/conversations")
    data = resp.get_json()
    assert resp.status_code == HTTPStatus.OK
    assert len(data["conversations"]) == 1
    assert data["conversations"][0]["title"] == "Test Chat"
    assert data["conversations"][0]["model"] == "test-model"


def test_get_conversation_messages_not_found(auth_client):
    resp = auth_client.get("/chat/conversations/9999/messages")
    assert resp.status_code == HTTPStatus.NOT_FOUND


def test_get_conversation_messages(app, auth_client, test_user):
    with app.app_context():
        from lumen.extensions import db
        from lumen.models.conversation import Conversation
        from lumen.models.message import Message
        conv = Conversation(entity_id=test_user["id"], title="Test", model="test-model")
        db.session.add(conv)
        db.session.flush()
        db.session.add(Message(conversation_id=conv.id, role="user", content="hello"))
        db.session.add(Message(
            conversation_id=conv.id, role="assistant", content="hi",
            input_tokens=5, output_tokens=3,
        ))
        db.session.commit()
        conv_id = conv.id

    resp = auth_client.get(f"/chat/conversations/{conv_id}/messages")
    assert resp.status_code == HTTPStatus.OK
    data = resp.get_json()
    assert len(data["messages"]) == 2
    assert data["messages"][0]["role"] == "user"
    assert "meta" in data["messages"][1]


def test_delete_conversation_not_found(auth_client):
    resp = auth_client.delete("/chat/conversations/9999")
    assert resp.status_code == HTTPStatus.NOT_FOUND


def test_delete_conversation(app, auth_client, test_user):
    with app.app_context():
        from lumen.extensions import db
        from lumen.models.conversation import Conversation
        from lumen.models.message import Message
        conv = Conversation(entity_id=test_user["id"], title="Gone", model="test-model")
        db.session.add(conv)
        db.session.flush()
        db.session.add(Message(conversation_id=conv.id, role="user", content="hello"))
        db.session.commit()
        conv_id = conv.id

    resp = auth_client.delete(f"/chat/conversations/{conv_id}")
    assert resp.status_code == HTTPStatus.OK
    assert resp.get_json()["ok"] is True

    with app.app_context():
        from lumen.extensions import db
        from lumen.models.conversation import Conversation
        assert db.session.get(Conversation, conv_id) is None


def test_chat_stream_no_body(auth_client):
    resp = auth_client.post("/chat/stream", content_type="application/json", data="")
    assert resp.status_code == HTTPStatus.BAD_REQUEST


def test_chat_stream_missing_model_and_messages(auth_client):
    resp = auth_client.post("/chat/stream", json={})
    assert resp.status_code == HTTPStatus.BAD_REQUEST


def test_chat_stream_missing_model(auth_client):
    resp = auth_client.post("/chat/stream", json={"messages": [{"role": "user", "content": "hi"}]})
    assert resp.status_code == HTTPStatus.BAD_REQUEST


def test_chat_stream_model_but_no_messages(auth_client, test_model):
    """model provided but messages list omitted → 400."""
    resp = auth_client.post("/chat/stream", json={"model": test_model["model_name"]})
    assert resp.status_code == HTTPStatus.BAD_REQUEST


def test_chat_stream_unknown_model(auth_client):
    resp = auth_client.post("/chat/stream", json={
        "messages": [{"role": "user", "content": "hi"}],
        "model": "no-such-model",
    })
    assert resp.status_code == HTTPStatus.BAD_REQUEST


# ── Access control ────────────────────────────────────────────────────────────

def test_chat_stream_blacklisted_model_403(app, auth_client, test_user, test_model):
    with app.app_context():
        from lumen.extensions import db
        from lumen.models.entity_model_access import EntityModelAccess
        _grant_unlimited_pool(app, test_user["id"])
        db.session.add(EntityModelAccess(
            entity_id=test_user["id"],
            model_config_id=test_model["id"],
            access_type="blocked",
        ))
        db.session.commit()

    resp = auth_client.post("/chat/stream", json={
        "messages": [{"role": "user", "content": "hi"}],
        "model": test_model["model_name"],
    })
    assert resp.status_code == HTTPStatus.FORBIDDEN


def test_chat_stream_graylist_no_consent_403(app, auth_client, test_user, test_model):
    with app.app_context():
        from lumen.extensions import db
        from lumen.models.entity_model_access import EntityModelAccess
        from lumen.models.model_config import ModelConfig
        _grant_unlimited_pool(app, test_user["id"])
        db.session.get(ModelConfig, test_model["id"]).needs_ack = True
        db.session.add(EntityModelAccess(
            entity_id=test_user["id"],
            model_config_id=test_model["id"],
            access_type="allowed",
        ))
        db.session.commit()

    resp = auth_client.post("/chat/stream", json={
        "messages": [{"role": "user", "content": "hi"}],
        "model": test_model["model_name"],
    })
    assert resp.status_code == HTTPStatus.FORBIDDEN


def test_chat_stream_graylist_with_consent_passes_access(
    app, auth_client, test_user, test_model,
):
    """needs_ack + consent clears the access gate (stream starts, fails at LLM level)."""
    with app.app_context():
        from lumen.extensions import db
        from lumen.models.entity_model_access import EntityModelAccess
        from lumen.models.entity_model_consent import EntityModelConsent
        from lumen.models.model_config import ModelConfig
        _grant_unlimited_pool(app, test_user["id"])
        db.session.get(ModelConfig, test_model["id"]).needs_ack = True
        db.session.add(EntityModelAccess(
            entity_id=test_user["id"],
            model_config_id=test_model["id"],
            access_type="allowed",
        ))
        db.session.add(EntityModelConsent(
            entity_id=test_user["id"],
            model_config_id=test_model["id"],
            consented_at=datetime.now(timezone.utc).replace(tzinfo=None),
        ))
        db.session.commit()

    resp = auth_client.post("/chat/stream", json={
        "messages": [{"role": "user", "content": "hi"}],
        "model": test_model["model_name"],
    })
    assert resp.status_code != HTTPStatus.FORBIDDEN


def test_chat_stream_holds_no_connection_at_yields(app, auth_client, test_user, test_model, monkeypatch):
    """The streaming generator must not hold a DB connection while suspended at
    a yield. If the client disconnects while an event is in flight, the
    generator is never closed, teardown never runs, and any connection checked
    out at that point stays checked out until the process restarts."""
    with app.app_context():
        _grant_unlimited_pool(app, test_user["id"])
        from lumen.extensions import db
        pool = db.engine.pool

    def fake_stream(messages, model, entity_id=None, source="chat", effective=None):
        # The real send_message_stream runs context-free between yields; its
        # DB phases each push their own short-lived app context.
        yield "Hello", None, None
        yield None, None, {
            "reply": "Hello",
            "model": "test-model",
            "input_tokens": 1,
            "output_tokens": 1,
            "thinking": None,
            "thinking_tokens": None,
            "cost": 0.0,
            "duration": 0.1,
            "time_to_first_token": 0.05,
            "output_speed": 10.0,
        }

    from lumen.blueprints.chat import routes as chat_routes
    monkeypatch.setattr(chat_routes, "send_message_stream", fake_stream)

    resp = auth_client.post("/chat/stream", json={
        "messages": [{"role": "user", "content": "hi"}],
        "model": test_model["model_name"],
    })
    assert resp.status_code == HTTPStatus.OK
    # Laziness is required: a buffered response would already have run teardown
    # and hidden any leak.
    assert resp.is_streamed

    saw_final = False
    try:
        for raw in resp.response:
            if b'"done": true' in raw:
                saw_final = True
                # The generator is suspended at its final yield right now.
                assert pool.checkedout() == 0, (
                    "DB connection checked out while the final SSE event is in "
                    "flight — a client disconnect here leaks it permanently"
                )
        assert saw_final
    finally:
        resp.close()


def test_chat_stream_error_path_holds_no_connection(app, auth_client, test_user, test_model, monkeypatch):
    """Same invariant for the except-path yield: after rollback, the generator
    must hold no connection while the error event is in flight."""
    with app.app_context():
        _grant_unlimited_pool(app, test_user["id"])
        from lumen.extensions import db
        pool = db.engine.pool

    def fake_stream(messages, model, entity_id=None, source="chat", effective=None):
        yield "Hello", None, None
        # Missing "reply" key → KeyError inside the billing block, after the
        # conversation SELECT/flush has checked out a connection.
        yield None, None, {
            "model": "test-model",
            "input_tokens": 1,
            "output_tokens": 1,
        }

    from lumen.blueprints.chat import routes as chat_routes
    monkeypatch.setattr(chat_routes, "send_message_stream", fake_stream)

    resp = auth_client.post("/chat/stream", json={
        "messages": [{"role": "user", "content": "hi"}],
        "model": test_model["model_name"],
    })
    assert resp.status_code == HTTPStatus.OK
    assert resp.is_streamed

    saw_error = False
    try:
        for raw in resp.response:
            if b'"error"' in raw:
                saw_error = True
                # The generator is suspended at the except-path yield right now.
                assert pool.checkedout() == 0, (
                    "DB connection checked out while the error event is in flight"
                )
        assert saw_error
    finally:
        resp.close()


def test_chat_stream_skips_persistence_when_storing_disabled(app, auth_client, test_user, test_model, monkeypatch):
    """With store_conversations off, a stream persists nothing and the final
    event carries no conversation_id — even if the client sends one."""
    with app.app_context():
        from lumen.extensions import db
        from lumen.models.entity import Entity
        _grant_unlimited_pool(app, test_user["id"])
        db.session.get(Entity, test_user["id"]).store_conversations = False
        db.session.commit()

    def fake_stream(messages, model, entity_id=None, source="chat", effective=None):
        yield "Hello", None, None
        yield None, None, {
            "reply": "Hello",
            "model": "test-model",
            "input_tokens": 1,
            "output_tokens": 1,
            "thinking": None,
            "thinking_tokens": None,
            "cost": 0.0,
            "duration": 0.1,
            "time_to_first_token": 0.05,
            "output_speed": 10.0,
        }

    from lumen.blueprints.chat import routes as chat_routes
    monkeypatch.setattr(chat_routes, "send_message_stream", fake_stream)

    resp = auth_client.post("/chat/stream", json={
        "messages": [{"role": "user", "content": "hi"}],
        "model": test_model["model_name"],
        "conversation_id": 12345,
    })
    assert resp.status_code == HTTPStatus.OK

    body = resp.get_data(as_text=True)
    assert '"done": true' in body
    assert "conversation_id" not in body

    with app.app_context():
        from sqlalchemy import func, select

        from lumen.extensions import db
        from lumen.models.conversation import Conversation
        from lumen.models.message import Message
        assert db.session.scalar(select(func.count(Conversation.id))) == 0
        assert db.session.scalar(select(func.count(Message.id))) == 0


def _fake_stream(messages, model, entity_id=None, source="chat", effective=None):
    yield "Hello", None, None
    yield None, None, {
        "reply": "Hello",
        "model": "test-model",
        "input_tokens": 1,
        "output_tokens": 1,
        "thinking": None,
        "thinking_tokens": None,
        "cost": 0.0,
        "duration": 0.1,
        "time_to_first_token": 0.05,
        "output_speed": 10.0,
    }


def _conversation_counter(app, entity_id):
    with app.app_context():
        from sqlalchemy import select

        from lumen.extensions import db
        from lumen.models.entity_stat import EntityStat
        return db.session.scalar(
            select(EntityStat.conversations).filter_by(entity_id=entity_id)
        ) or 0


def test_chat_stream_counts_conversation_when_storing(app, auth_client, test_user, test_model, monkeypatch):
    """A new stored conversation bumps the persistent counter; continuing it does not."""
    with app.app_context():
        _grant_unlimited_pool(app, test_user["id"])

    from lumen.blueprints.chat import routes as chat_routes
    monkeypatch.setattr(chat_routes, "send_message_stream", _fake_stream)

    resp = auth_client.post("/chat/stream", json={
        "messages": [{"role": "user", "content": "hi"}],
        "model": test_model["model_name"],
    })
    body = resp.get_data(as_text=True)
    assert '"done": true' in body
    assert _conversation_counter(app, test_user["id"]) == 1

    import json as _json
    conv_id = _json.loads(body.strip().splitlines()[-1].removeprefix("data: "))["conversation_id"]

    resp = auth_client.post("/chat/stream", json={
        "messages": [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "Hello"},
            {"role": "user", "content": "more"},
        ],
        "model": test_model["model_name"],
        "conversation_id": conv_id,
    })
    assert '"done": true' in resp.get_data(as_text=True)
    assert _conversation_counter(app, test_user["id"]) == 1


def test_chat_stream_counts_conversation_when_storing_disabled(app, auth_client, test_user, test_model, monkeypatch):
    """With storage off, the first exchange bumps the counter; follow-ups don't."""
    with app.app_context():
        from lumen.extensions import db
        from lumen.models.entity import Entity
        _grant_unlimited_pool(app, test_user["id"])
        db.session.get(Entity, test_user["id"]).store_conversations = False
        db.session.commit()

    from lumen.blueprints.chat import routes as chat_routes
    monkeypatch.setattr(chat_routes, "send_message_stream", _fake_stream)

    resp = auth_client.post("/chat/stream", json={
        "messages": [{"role": "user", "content": "hi"}],
        "model": test_model["model_name"],
    })
    assert '"done": true' in resp.get_data(as_text=True)
    assert _conversation_counter(app, test_user["id"]) == 1

    resp = auth_client.post("/chat/stream", json={
        "messages": [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "Hello"},
            {"role": "user", "content": "more"},
        ],
        "model": test_model["model_name"],
    })
    assert '"done": true' in resp.get_data(as_text=True)
    assert _conversation_counter(app, test_user["id"]) == 1

    with app.app_context():
        from sqlalchemy import func, select

        from lumen.extensions import db
        from lumen.models.conversation import Conversation
        assert db.session.scalar(select(func.count(Conversation.id))) == 0


def test_chat_stream_whitelist_passes_access(app, auth_client, test_user, test_model):
    """Whitelist clears the access gate (stream starts, fails at LLM level)."""
    with app.app_context():
        from lumen.extensions import db
        from lumen.models.entity_model_access import EntityModelAccess
        _grant_unlimited_pool(app, test_user["id"])
        db.session.add(EntityModelAccess(
            entity_id=test_user["id"],
            model_config_id=test_model["id"],
            access_type="allowed",
        ))
        db.session.commit()

    resp = auth_client.post("/chat/stream", json={
        "messages": [{"role": "user", "content": "hi"}],
        "model": test_model["model_name"],
    })
    assert resp.status_code != HTTPStatus.FORBIDDEN


def test_chat_stream_disconnect_is_not_reported_as_empty_response(
    app, auth_client, test_user, test_model, monkeypatch,
):
    """A departed client is not an empty model response.

    send_message_stream stops without emitting its final result tuple when the
    disconnect flag is set, which leaves `result is None` — the same state as a
    genuinely empty response. Reporting the two identically is wrong in the log
    and, on a half-open connection where the socket is still live, delivers
    "Empty response from model" to a client that just received partial output.
    """
    import threading

    disconnected = threading.Event()
    with app.app_context():
        _grant_unlimited_pool(app, test_user["id"])

    def fake_stream(messages, model, entity_id=None, source="chat", effective=None):
        yield "Hello", None, None
        disconnected.set()  # client vanishes mid-stream
        yield " world", None, None

    from lumen.blueprints.chat import routes as chat_routes
    monkeypatch.setattr(chat_routes, "send_message_stream", fake_stream)
    monkeypatch.setattr(chat_routes, "client_disconnect_event", lambda: disconnected)

    resp = auth_client.post("/chat/stream", json={
        "messages": [{"role": "user", "content": "hi"}],
        "model": test_model["model_name"],
    })
    assert resp.status_code == HTTPStatus.OK
    body = b"".join(resp.response)
    resp.close()
    assert b"Empty response from model" not in body, (
        "a client disconnect was reported to the client as an empty model response"
    )


# ---------------------------------------------------------------------------
# Request timing columns
#
# The chat path is the only one where the LLM call lives in llm.py rather than
# in the view, so the marks have to travel view -> send_message_stream ->
# context-free generator. The real send_message_stream runs here (only the
# openai client is faked) so that journey is actually exercised.
# ---------------------------------------------------------------------------

_MAX_PLAUSIBLE_SPAN = 60 * 60  # seconds; a test request takes milliseconds
_QUEUE_WAIT = 0.05             # seconds of admission wait to stamp T0 behind
_SEND_BLOCKED = 0.25           # seconds the responder reports blocked in send


def _bridge_environ():
    """The environ keys ``asgi.py`` publishes; the test client bypasses it.

    ``lumen.queue_wait`` is left out on purpose — the real before_request hook
    derives it from the arrival mark.
    """
    import time
    from datetime import datetime, timezone
    from lumen.services.wsgi_disconnect import SendBlocked
    return {
        "lumen.t0_monotonic": time.monotonic() - _QUEUE_WAIT,
        "lumen.started_at": datetime.now(timezone.utc),
        "lumen.send_blocked": SendBlocked(),
    }


class _Chunk:
    """One upstream streaming chunk: a reasoning delta, a content delta, or usage."""

    def __init__(self, content=None, reasoning=None, usage=None):
        self.usage = usage
        delta = type("Delta", (), {
            "content": content, "reasoning_content": reasoning, "reasoning": None,
        })()
        self.choices = [type("Choice", (), {"delta": delta})()] if (content or reasoning) else []


class _Usage:
    prompt_tokens = 10
    completion_tokens = 5
    completion_tokens_details = None


def _fake_openai(chunks):
    from unittest.mock import MagicMock
    client = MagicMock()
    client.chat.completions.create.return_value = iter(chunks)
    client.__enter__ = MagicMock(return_value=client)
    client.__exit__ = MagicMock(return_value=False)
    return MagicMock(return_value=client)


def _allow_model(app, test_user, test_model):
    with app.app_context():
        from lumen.extensions import db
        from lumen.models.entity_model_access import EntityModelAccess
        _grant_unlimited_pool(app, test_user["id"])
        db.session.add(EntityModelAccess(
            entity_id=test_user["id"], model_config_id=test_model["id"],
            access_type="allowed",
        ))
        db.session.commit()


def _only_log(app):
    with app.app_context():
        from sqlalchemy import select
        from lumen.extensions import db
        from lumen.models.request_log import RequestLog
        return db.session.execute(select(RequestLog)).scalar_one()


def test_chat_stream_records_timing_columns(
    app, auth_client, test_user, test_model, test_model_endpoint,
):
    """A reasoning delta ahead of the content splits ttft from ttft_visible.

    send_blocked is mutated after the first event is out: a float captured in
    the view would still read 0.0 there, since nothing had been sent yet.
    """
    from unittest.mock import patch
    _allow_model(app, test_user, test_model)
    chunks = [_Chunk(reasoning="thinking"), _Chunk(content="hi"), _Chunk(usage=_Usage())]

    environ = _bridge_environ()
    with patch("lumen.services.llm.openai.OpenAI", _fake_openai(chunks)):
        resp = auth_client.post(
            "/chat/stream",
            json={"messages": [{"role": "user", "content": "hi"}],
                  "model": test_model["model_name"]},
            environ_base=environ,
        )
        assert resp.status_code == HTTPStatus.OK
        assert resp.is_streamed
        events = iter(resp.response)
        try:
            next(events)  # one event out; the generator is suspended mid-stream
            environ["lumen.send_blocked"].seconds = _SEND_BLOCKED
            for _ in events:
                pass
        finally:
            resp.close()

    log = _only_log(app)
    assert log.started_at is not None
    assert _QUEUE_WAIT <= log.queue_wait < _MAX_PLAUSIBLE_SPAN
    assert 0 <= log.preflight < _MAX_PLAUSIBLE_SPAN
    # The thinking phase sits between the two marks.
    assert 0 < log.ttft < log.ttft_visible < _MAX_PLAUSIBLE_SPAN
    assert log.send_blocked == _SEND_BLOCKED
    assert log.outcome == "ok"
    assert log.aborted is False


def test_chat_stream_without_the_bridge_records_nulls(
    app, auth_client, test_user, test_model, test_model_endpoint,
):
    """Absent marks are NULL — "not measured", not "measured as zero"."""
    from unittest.mock import patch
    _allow_model(app, test_user, test_model)
    chunks = [_Chunk(content="hi"), _Chunk(usage=_Usage())]

    with patch("lumen.services.llm.openai.OpenAI", _fake_openai(chunks)):
        resp = auth_client.post(
            "/chat/stream",
            json={"messages": [{"role": "user", "content": "hi"}],
                  "model": test_model["model_name"]},
        )
        assert resp.status_code == HTTPStatus.OK
        b"".join(resp.response)
        resp.close()

    log = _only_log(app)
    assert log.started_at is None
    assert log.queue_wait is None
    assert log.preflight is None
    assert log.send_blocked is None
    assert log.ttft is not None
    assert log.ttft_visible is not None
    assert log.outcome == "ok"
