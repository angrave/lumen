"""Guard the single-consumer rule of the ASGI→WSGI bridge.

Why this file exists — not what it asserts, but what it prevents:

``lumen/services/wsgi_disconnect.py`` needs to see the ASGI ``http.disconnect``
event, and the tempting way to get it is to bolt a small watcher task onto
a2wsgi's existing plumbing: leave ``Body`` reading ``receive()`` for the request
body, and run a second task that also awaits ``receive()`` looking for the
disconnect. That is wrong. ``receive()`` is a single-consumer channel — each
message goes to exactly one awaiting caller and is gone for the other — so the
two consumers steal from each other: the watcher eats ``http.request`` body
chunks and ``Body`` eats the ``http.disconnect``.

Nothing raises when that happens, which is what makes it dangerous. Requests
without a body have nothing to steal, so every GET keeps working and a smoke
test looks green. Requests *with* a body silently arrive truncated or with a
hole in the middle, and the damage only surfaces much later as a JSON decode
error, a short upload, or a read that blocks for bytes handed to the wrong
reader. Every endpoint this middleware exists for — ``/v1/chat/completions``,
``/chat/stream``, ``/v1/audio/*`` — is a POST with a body.

So the large-POST round trip below is the real enforcement: it fails loudly the
moment a second ``receive()`` consumer is introduced. The disconnect and
EOF-after-disconnect cases cover the behaviour the pump was added to provide,
including the deadlock a naive queue hand-off would introduce.
"""

import asyncio
import threading
import time

import flask

from lumen.services.wsgi_disconnect import (
    ENVIRON_KEY,
    DisconnectAwareWSGIMiddleware,
    client_disconnect_event,
)

# ~90 KB of distinctive, order-revealing payload, well over the 64 KB that
# guarantees delivery as several http.request chunks.
BODY = b"".join(b"%08d-lumen-payload;" % i for i in range(5000))
CHUNK_SIZE = 16 * 1024


def _scope(content_length, method="POST"):
    return {
        "type": "http",
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "root_path": "",
        "path": "/",
        "query_string": b"",
        "headers": [
            (b"content-length", str(content_length).encode()),
            (b"content-type", b"application/octet-stream"),
        ],
        "server": ("testserver", 80),
        "client": ("198.51.100.7", 4242),
    }


def _body_messages(payload, chunk_size=CHUNK_SIZE):
    chunks = [payload[i:i + chunk_size] for i in range(0, len(payload), chunk_size)]
    return [
        {"type": "http.request", "body": chunk, "more_body": i < len(chunks) - 1}
        for i, chunk in enumerate(chunks)
    ]


def _drive(app, scope, messages, timeout=10.0):
    """Run the middleware against a scripted receive()/send() pair.

    ``timeout`` turns a deadlock regression into a failing test rather than a
    hung suite; closing the loop also releases a WSGI thread stuck on a body
    read, because its pending cross-thread future is cancelled.
    """
    sent = []
    middleware = DisconnectAwareWSGIMiddleware(app, workers=2)

    async def main():
        pending = list(messages)

        async def receive():
            if pending:
                return pending.pop(0)
            # A real server simply says nothing more; the pump is cancelled
            # when the response finishes.
            await asyncio.Event().wait()

        async def send(message):
            sent.append(message)

        await asyncio.wait_for(middleware(scope, receive, send), timeout)

    try:
        asyncio.run(main())
    finally:
        middleware.executor.shutdown(wait=False)
    return sent


def test_large_post_body_arrives_intact():
    """A >64 KB POST body must round-trip byte for byte through the pump."""
    received = {}

    def app(environ, start_response):
        received["body"] = environ["wsgi.input"].read(int(environ["CONTENT_LENGTH"]))
        received["disconnected"] = environ[ENVIRON_KEY].is_set()
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"ok"]

    messages = _body_messages(BODY)
    assert len(messages) > 1, "payload must span multiple http.request chunks"

    sent = _drive(app, _scope(len(BODY)), messages)

    assert received["body"] == BODY
    assert received["disconnected"] is False
    assert sent[0]["type"] == "http.response.start"
    assert b"".join(m.get("body", b"") for m in sent[1:]) == b"ok"


def test_http_disconnect_sets_the_environ_event():
    """http.disconnect must set the threading.Event in environ[ENVIRON_KEY]."""
    observed = {}

    def app(environ, start_response):
        event = environ[ENVIRON_KEY]
        assert isinstance(event, threading.Event)
        observed["set"] = event.wait(timeout=5)
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"ok"]

    _drive(
        app,
        _scope(0),
        [
            {"type": "http.request", "body": b"", "more_body": False},
            {"type": "http.disconnect"},
        ],
    )

    assert observed["set"] is True


def test_body_read_after_disconnect_returns_eof_promptly():
    """A read still outstanding when the client vanishes must not hang.

    The scope promises 1 MB, the client delivers one chunk and disappears. The
    pump has to hand the reader an EOF; without it the WSGI thread blocks on a
    queue that will never receive another message.
    """
    first = BODY[:CHUNK_SIZE]
    observed = {}

    def app(environ, start_response):
        started = time.monotonic()
        observed["body"] = environ["wsgi.input"].read(1024 * 1024)
        observed["elapsed"] = time.monotonic() - started
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"ok"]

    _drive(
        app,
        _scope(1024 * 1024),
        [
            {"type": "http.request", "body": first, "more_body": True},
            {"type": "http.disconnect"},
        ],
        timeout=10.0,
    )

    assert observed["body"] == first
    assert observed["elapsed"] < 5.0


def test_client_disconnect_event_falls_back_when_key_absent():
    """The dev server and test client have no ENVIRON_KEY; they must still work."""
    app = flask.Flask(__name__)
    with app.test_request_context("/"):
        event = client_disconnect_event()
        assert isinstance(event, threading.Event)
        assert not event.is_set()
