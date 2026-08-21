"""Guard the send-side stall bound of the ASGI→WSGI bridge.

Why this file is separate from ``test_wsgi_disconnect_body.py``: that file is
about the *read* side — the single-consumer rule for ``receive()`` and the
request body it carries. This one is about the *write* side, which is a
different failure and a different fix, and the distinction is worth keeping
legible because an earlier draft of the plan confused the two and proposed
putting this timeout on the pump's (body) queue.

What it prevents: a client that opens a stream and stops reading — a half-open
TCP connection, or an app that never drains its socket — never produces an
``http.disconnect``, so the disconnect Event is never set. Worse, the streaming
view could not act on it even if it were: a blocked writer is suspended inside
``yield``, down in ``WSGIResponder.send``, and not in the loop body where the
flag is polled. Stock a2wsgi waits there with no timeout and uvicorn's
``flow.drain()`` has none either, so the worker thread is pinned for the life of
the dead connection. ``workers`` such clients is a total outage with nothing
logged.

The stall test below is only meaningful because it asserts on *elapsed time*.
Delete the timeout from ``send`` and the request still finishes once the fake
server eventually unsticks — it is the deadline, not the completion, that has to
be checked.
"""

import asyncio
import os
import threading
import time

import pytest

from lumen.services.wsgi_disconnect import (
    DEFAULT_SEND_TIMEOUT,
    ENVIRON_KEY,
    SEND_TIMEOUT_ENV,
    DisconnectAwareWSGIMiddleware,
    _send_timeout,
)

# a2wsgi's send queue holds 10 messages and the sender task holds one more, so
# the writer only blocks after a dozen or so. Yield comfortably past that.
CHUNKS = 40

# How long the fake server stays stuck. Bounded rather than infinite so that a
# regression fails on the elapsed-time assertion instead of wedging a worker
# thread that the interpreter would then try to join at exit.
STALL_RELEASE = 10.0

# The bound under test, shrunk so the test costs a fraction of a second.
TEST_SEND_TIMEOUT = "0.5"

SCOPE = {
    "type": "http",
    "http_version": "1.1",
    "method": "POST",
    "scheme": "http",
    "root_path": "",
    "path": "/v1/chat/completions",
    "query_string": b"",
    "headers": [(b"content-length", b"0")],
    "server": ("testserver", 80),
    "client": ("198.51.100.7", 4242),
}

EMPTY_BODY = [{"type": "http.request", "body": b"", "more_body": False}]


def _streaming_app(observed):
    """A WSGI app whose response body reports how it was torn down."""

    def app(environ, start_response):
        observed["event"] = environ[ENVIRON_KEY]
        start_response("200 OK", [("Content-Type", "text/plain")])

        def body():
            try:
                for i in range(CHUNKS):
                    yield b"chunk-%04d;" % i
            except GeneratorExit:
                observed["generator_exit"] = True
                raise

        return body()

    return app


def _drive(app, *, stall_first_send, guard=30.0):
    """Run the middleware against a scripted receive()/send() pair.

    ``stall_first_send`` makes the fake server accept the response headers and
    then go unresponsive, exactly as uvicorn does when the transport is paused
    and ``flow.drain()`` never resolves. ``guard`` turns an outright hang into a
    failure rather than a stuck suite.
    """
    sent = []
    result = {}
    middleware = DisconnectAwareWSGIMiddleware(app, workers=2)

    async def main():
        pending = list(EMPTY_BODY)
        stalled = [stall_first_send]
        never = asyncio.Event()

        async def receive():
            if pending:
                return pending.pop(0)
            await asyncio.Event().wait()

        async def send(message):
            sent.append(message)
            if stalled[0]:
                stalled[0] = False
                # Never actually set; the wait_for bounds the damage.
                await asyncio.wait_for(never.wait(), STALL_RELEASE)

        started = time.monotonic()
        try:
            await asyncio.wait_for(middleware(SCOPE, receive, send), guard)
        finally:
            result["elapsed"] = time.monotonic() - started

    try:
        asyncio.run(main())
    finally:
        middleware.executor.shutdown(wait=False)
    result["sent"] = sent
    return result


@pytest.fixture()
def short_send_timeout(monkeypatch):
    monkeypatch.setenv(SEND_TIMEOUT_ENV, TEST_SEND_TIMEOUT)


@pytest.mark.timeout(60)
def test_stalled_reader_releases_the_wsgi_thread(short_send_timeout):
    """A server that stops draining must not pin the worker thread.

    The elapsed-time assertion is the whole test: without the bound the request
    still completes, just ``STALL_RELEASE`` seconds later, which is precisely
    the outage this guards against.
    """
    observed = {}

    result = _drive(_streaming_app(observed), stall_first_send=True)

    assert result["elapsed"] < 4.0, (
        "the WSGI thread was not released within the send timeout: "
        f"{result['elapsed']:.1f}s"
    )
    assert observed.get("generator_exit") is True, (
        "the response generator must be closed so the abort accounting runs"
    )


@pytest.mark.timeout(60)
def test_stalled_reader_sets_the_disconnect_event(short_send_timeout):
    """A stall is reported through the same Event a clean disconnect uses."""
    observed = {}

    _drive(_streaming_app(observed), stall_first_send=True)

    event = observed["event"]
    assert isinstance(event, threading.Event)
    assert event.is_set(), "the stall must raise the one 'client is gone' signal"


@pytest.mark.timeout(60)
def test_healthy_client_streams_at_full_speed():
    """The happy path keeps the inherited behaviour, and its timing.

    Runs with the real 300 s default in force to prove the bound is a deadline
    and not a per-chunk delay or a poll loop.
    """
    observed = {}
    assert os.environ.get(SEND_TIMEOUT_ENV) is None
    assert DEFAULT_SEND_TIMEOUT >= 60.0, "the default must not cut off slow clients"

    result = _drive(_streaming_app(observed), stall_first_send=False)

    sent = result["sent"]
    assert sent[0]["type"] == "http.response.start"
    assert sent[0]["status"] == 200
    body = b"".join(m.get("body", b"") for m in sent[1:])
    assert body == b"".join(b"chunk-%04d;" % i for i in range(CHUNKS))
    assert sent[-1].get("more_body", False) is False
    assert observed.get("generator_exit") is None
    assert observed["event"].is_set() is False
    assert result["elapsed"] < 5.0


def test_send_timeout_env_override(monkeypatch):
    monkeypatch.setenv(SEND_TIMEOUT_ENV, "12.5")
    assert _send_timeout() == 12.5


@pytest.mark.parametrize("raw", ["", "   ", "0", "-1", "forever"])
def test_send_timeout_falls_back_to_the_default(monkeypatch, raw):
    """A typo must not silently restore the unbounded wait."""
    monkeypatch.setenv(SEND_TIMEOUT_ENV, raw)
    assert _send_timeout() == DEFAULT_SEND_TIMEOUT
