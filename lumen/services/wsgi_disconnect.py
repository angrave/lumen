"""ASGI→WSGI bridge that delivers client disconnects to the WSGI application.

Production serves Flask through ``a2wsgi.WSGIMiddleware`` under uvicorn (see
``asgi.py``). a2wsgi's ``WSGIResponder`` never watches for the ASGI
``http.disconnect`` event, and uvicorn's ``send()`` silently no-ops once the
peer is gone instead of raising. The consequence, verified empirically: when a
client disconnects mid-stream the WSGI response generator is never closed,
``GeneratorExit`` never fires, and the app streams an entire LLM response into a
dead socket — pinning a worker thread and burning upstream GPU time — while the
abort accounting that exists precisely to observe this never runs.

This module fixes the missing signal. It puts a :class:`threading.Event` into
the WSGI environ under :data:`ENVIRON_KEY`, set the moment the ASGI server
reports ``http.disconnect``. Streaming views capture it via
:func:`client_disconnect_event` while the request context is still live (the
response generators run context-free — see CLAUDE.md) and poll ``is_set()``
between chunks.

``receive()`` has EXACTLY ONE consumer, and it must stay that way
-----------------------------------------------------------------
``receive()`` is a single-consumer channel: every message it yields goes to
whoever happens to be awaiting it, and it is gone for everyone else. The
obvious implementation — leave ``a2wsgi.Body`` reading ``receive()`` for the
request body and add a small watcher task alongside it looking for
``http.disconnect`` — puts two consumers on that channel, and they steal each
other's messages at random: the watcher swallows ``http.request`` body chunks
that ``Body`` needed, and ``Body`` swallows the ``http.disconnect`` the watcher
was waiting for.

The failure mode is nasty because it is invisible in the obvious places. A GET
has no body to steal, so every GET keeps working and a smoke test passes. Only
requests with a body break, and they break *quietly*: the body arrives
truncated or with a hole in the middle rather than raising, so the symptom
surfaces far downstream as a JSON parse error, a short upload, or a request
that simply hangs waiting for bytes that were handed to the wrong reader. Every
endpoint that motivated this module (``/v1/chat/completions``, ``/chat/stream``,
``/v1/audio/*``) is a POST with a body.

So there is one pump task, and it is the only caller of ``receive()``. It
dispatches ``http.request`` messages into a queue that ``wsgi.input`` reads
from, and sets the disconnect Event on ``http.disconnect``. Never add a second
``receive()`` caller anywhere in this chain. Enforced by
``tests/unit/test_wsgi_disconnect_body.py``.

``_PumpedBody`` and ``_DisconnectAwareWSGIResponder`` are thin derivations of
a2wsgi 1.10.10's ``Body`` and ``WSGIResponder``; the send path, its bounded
queue and its backpressure semantics are inherited unchanged.
"""

import asyncio
import contextvars
import functools
import threading
import typing

from a2wsgi.wsgi import Body, WSGIMiddleware, WSGIResponder, build_environ
from flask import has_request_context, request

#: WSGI environ key holding the ``threading.Event`` set on client disconnect.
ENVIRON_KEY = "lumen.client_disconnected"

# The pump reads at most one message ahead of the WSGI thread, so the ASGI
# server's own read backpressure still governs how much of a large upload is
# buffered in this process.
_BODY_QUEUE_SIZE = 1

# (body, more_body) pushed to end the body stream when the client vanished
# mid-upload, so a blocked read returns EOF instead of waiting forever.
_EOF: typing.Tuple[bytes, bool] = (b"", False)


async def _no_receive() -> typing.NoReturn:
    """Placeholder for ``Body.receive`` — reaching it means a second consumer."""
    raise AssertionError(
        "wsgi.input is fed by the pump; receive() must have exactly one consumer"
    )


class _PumpedBody(Body):
    """``wsgi.input`` fed from the pump's queue instead of from ``receive()``."""

    def __init__(self, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue) -> None:
        super().__init__(loop, _no_receive)
        self._queue = queue

    def _receive_more_data(self) -> bytes:
        if not self._has_more:
            return b""
        future = asyncio.run_coroutine_threadsafe(self._queue.get(), loop=self.loop)
        body, more_body = future.result()
        self._has_more = more_body
        return body


async def _pump(
    receive: typing.Callable[[], typing.Awaitable[typing.Any]],
    queue: asyncio.Queue,
    disconnected: threading.Event,
) -> None:
    """The one and only consumer of ``receive()``. See the module docstring."""
    body_open = True
    while True:
        message = await receive()
        if message["type"] == "http.request":
            if body_open:
                more_body = message.get("more_body", False)
                await queue.put((message.get("body", b""), more_body))
                body_open = more_body
        elif message["type"] == "http.disconnect":
            # Set the flag before the put, which blocks while the queue is
            # full: an app that has stopped reading the body must still see
            # the disconnect.
            disconnected.set()
            if body_open:
                await queue.put(_EOF)
            return


class _DisconnectAwareWSGIResponder(WSGIResponder):
    """a2wsgi's responder with the ``receive()`` pump wired in.

    Only ``__call__`` differs from the base class: it starts the pump, feeds
    ``wsgi.input`` from the pump's queue, and publishes the disconnect Event in
    the environ. Everything else — ``send``, ``sender``, ``start_response`` and
    ``wsgi`` (including its ``finally: iterable.close()``) — is inherited.
    """

    async def __call__(self, scope: typing.Any, receive: typing.Any, send: typing.Any) -> None:
        disconnected = threading.Event()
        queue: asyncio.Queue = asyncio.Queue(_BODY_QUEUE_SIZE)
        environ = build_environ(scope, _PumpedBody(self.loop, queue))
        environ[ENVIRON_KEY] = disconnected
        sender = None
        pump = None
        try:
            pump = self.loop.create_task(_pump(receive, queue, disconnected))
            sender = self.loop.create_task(self.sender(send))
            context = contextvars.copy_context()
            func = functools.partial(context.run, self.wsgi)
            await self.loop.run_in_executor(
                self.executor, func, environ, self.start_response
            )
            await self.send_queue.put(None)
            # Sender may raise an exception, so we need to await it
            await sender
            if self.exc_info is not None:
                raise self.exc_info[0].with_traceback(
                    self.exc_info[1], self.exc_info[2]
                )
        finally:
            if pump and not pump.done():
                pump.cancel()
            if sender and not sender.done():
                sender.cancel()


class DisconnectAwareWSGIMiddleware(WSGIMiddleware):
    """Drop-in ``a2wsgi.WSGIMiddleware`` that reports client disconnects."""

    async def __call__(self, scope: typing.Any, receive: typing.Any, send: typing.Any) -> None:
        if scope["type"] == "http":
            responder = _DisconnectAwareWSGIResponder(
                self.app, self.executor, self.send_queue_size
            )
            return await responder(scope, receive, send)
        return await super().__call__(scope, receive, send)


def client_disconnect_event() -> threading.Event:
    """Return the current request's disconnect flag.

    Call this from view code while the request context is still live and
    capture the Event into the response generator's closure — the generators
    run context-free and must never touch ``request``.

    Falls back to an Event that is never set when there is no request context or
    the environ key is absent, so the Werkzeug dev server, the Flask test client,
    any non-ASGI deployment, and direct unit-test calls to the streaming helpers
    all keep working unchanged. Never raising matters: the callers are streaming
    views whose failure mode would otherwise be a 500 on a path that is supposed
    to be a pure observability improvement.
    """
    event = request.environ.get(ENVIRON_KEY) if has_request_context() else None
    return event if event is not None else threading.Event()
