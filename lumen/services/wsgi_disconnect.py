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

The other half: stalled readers, which no flag can catch
---------------------------------------------------------
The disconnect Event above handles a *clean* disconnect — the peer sent a FIN
and the server told us about it. It does nothing for a client that is simply not
reading: a half-open TCP connection (closed laptop, dropped NAT entry) or an app
that opened a stream and stopped consuming it. No ``http.disconnect`` is ever
delivered there, and — more fundamentally — the flag could not help even if it
were. Polling happens in the streaming view's loop body, but a blocked writer is
suspended *inside* ``yield``: a2wsgi's ``WSGIResponder.send`` pushes each chunk
across threads with ``asyncio.run_coroutine_threadsafe(...).result()``, and
upstream of it uvicorn's ``send`` awaits ``flow.drain()``. Neither has a timeout,
so once the socket buffer, uvicorn's write buffer and the 10-slot ``send_queue``
are all full, the WSGI thread parks in ``send`` forever. The generator never gets
control back, so it can never look at any flag. ``workers`` such clients is a
total request-serving outage with nothing logged.

So ``send`` is overridden here to bound that hand-off (:data:`DEFAULT_SEND_TIMEOUT`,
overridable with :data:`SEND_TIMEOUT_ENV`). On expiry it cancels the pending put,
sets the *same* disconnect Event the pump sets — one consistent "client is gone"
signal for the whole system — logs a warning, and raises. Raising is the point:
the exception unwinds a2wsgi's ``wsgi()`` into its ``finally: iterable.close()``,
which throws ``GeneratorExit`` at the suspended ``yield`` and so runs the
existing abort accounting. ``__call__`` then swallows that one exception rather
than letting it reach uvicorn's ``run_asgi``, which would log a full traceback
for what is a routine event; see the comment there for the rest of the trade.

Note this is the *write* side. It is deliberately not the pump's queue — that is
the *read* side (request body) and has nothing to do with a stalled reader.

``_PumpedBody`` and ``_DisconnectAwareWSGIResponder`` are thin derivations of
a2wsgi 1.10.10's ``Body`` and ``WSGIResponder``; the bounded send queue and its
backpressure semantics are inherited unchanged — only the unbounded wait on it
is replaced.
"""

import asyncio
import concurrent.futures
import contextvars
import functools
import logging
import os
import threading
import typing

from a2wsgi.wsgi import Body, WSGIMiddleware, WSGIResponder, build_environ
from flask import has_request_context, request

logger = logging.getLogger(__name__)

#: WSGI environ key holding the ``threading.Event`` set on client disconnect.
ENVIRON_KEY = "lumen.client_disconnected"

#: Environment variable overriding :data:`DEFAULT_SEND_TIMEOUT`, in seconds.
#: Follows the ``LUMEN_WSGI_WORKERS`` precedent in ``db_pool.py``: server
#: plumbing belongs in the environment, not in ``config.yaml``. Read once per
#: request, so a restart is not needed to change it. Non-numeric or non-positive
#: values warn and fall back to the default (there is deliberately no way to
#: disable the bound — an unbounded wait is the bug this exists to fix).
SEND_TIMEOUT_ENV = "LUMEN_WSGI_SEND_TIMEOUT"

#: Seconds a WSGI thread may wait to hand one response chunk to the server
#: before the client is declared gone.
#:
#: This is a stall detector, not a QoS policy, so it is set well past any real
#: client. What has to drain before a thread even begins waiting is the kernel
#: socket buffer plus uvicorn's 64 KiB high-water mark plus ten queued chunks;
#: a genuinely awful 20 kbit/s mobile link clears that in well under a minute,
#: so 300 s leaves more than an order of magnitude of headroom. It also sits
#: comfortably inside the 600 s gateway request budget (``chart/values.yaml``),
#: so a stalled thread is reclaimed before the request would have been cut off
#: anyway, and far below the ~15 min a Linux kernel takes to abandon
#: retransmissions to a vanished peer — which is the *best* case today, since a
#: peer that is alive but advertising a zero window is never abandoned at all.
DEFAULT_SEND_TIMEOUT = 300.0

# The pump reads at most one message ahead of the WSGI thread, so the ASGI
# server's own read backpressure still governs how much of a large upload is
# buffered in this process.
_BODY_QUEUE_SIZE = 1

# (body, more_body) pushed to end the body stream when the client vanished
# mid-upload, so a blocked read returns EOF instead of waiting forever.
_EOF: typing.Tuple[bytes, bool] = (b"", False)


class _StalledClient(Exception):
    """Raised in the WSGI thread when a chunk cannot be handed to the server.

    Private on purpose: it is caught by the responder that raised it and never
    escapes this module. Its only job is to unwind the WSGI thread out of the
    application so a2wsgi's ``finally: iterable.close()`` can fire.
    """


def _send_timeout() -> float:
    """Resolve :data:`SEND_TIMEOUT_ENV`, falling back to the default."""
    raw = os.environ.get(SEND_TIMEOUT_ENV, "").strip()
    if not raw:
        return DEFAULT_SEND_TIMEOUT
    try:
        value = float(raw)
    except ValueError:
        value = 0.0
    if value <= 0:
        logger.warning(
            "%s=%r is not a positive number of seconds; using %.0fs",
            SEND_TIMEOUT_ENV, raw, DEFAULT_SEND_TIMEOUT,
        )
        return DEFAULT_SEND_TIMEOUT
    return value


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
    try:
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
    except asyncio.CancelledError:
        # Normal teardown: the responder cancels the pump once the response is
        # done. Nobody is waiting on the queue by then.
        raise
    except BaseException:
        # Any other failure would otherwise be silent — the task's exception is
        # never retrieved — while the WSGI thread stays blocked forever in
        # _receive_more_data waiting on a queue nothing will fill again. That is
        # a permanently leaked worker thread per occurrence. Unblock the reader
        # and report the client as gone before propagating.
        disconnected.set()
        if body_open:
            try:
                await queue.put(_EOF)
            except BaseException:
                pass
        raise


class _DisconnectAwareWSGIResponder(WSGIResponder):
    """a2wsgi's responder with the ``receive()`` pump and a bounded ``send``.

    ``__call__`` starts the pump, feeds ``wsgi.input`` from the pump's queue, and
    publishes the disconnect Event in the environ. ``send`` bounds the
    cross-thread hand-off that a stalled reader would otherwise block forever.
    ``sender``, ``start_response`` and ``wsgi`` (including its
    ``finally: iterable.close()``) are inherited unchanged.

    One responder is built per request, so the Event and the resolved timeout
    are per-request state.
    """

    def __init__(self, app: typing.Any, executor: typing.Any, send_queue_size: int) -> None:
        super().__init__(app, executor, send_queue_size)
        self.disconnected = threading.Event()
        self.send_timeout = _send_timeout()
        self.description = "request"

    def send(self, message: typing.Optional[typing.Any]) -> None:
        """Hand one ASGI message to the sender task, bounded by a timeout.

        The happy path is exactly the inherited one — the same single blocking
        wait on the same future, with a deadline attached, so normal streaming
        neither busy-loops nor slows down.
        """
        future = asyncio.run_coroutine_threadsafe(
            self.send_queue.put(message), loop=self.loop
        )
        try:
            future.result(self.send_timeout)
        except concurrent.futures.TimeoutError:
            # Do not leave the put pending: it holds a reference to this
            # responder's queue and would deliver a chunk into a response that
            # is already being torn down.
            future.cancel()
            # The same signal the pump raises for a clean disconnect, so
            # everything downstream sees one notion of "client is gone".
            self.disconnected.set()
            logger.warning(
                "Client stopped reading %s after %.0fs (%s); aborting the response "
                "and releasing the WSGI worker thread. Raise %s if legitimate "
                "clients are being cut off.",
                self.description, self.send_timeout,
                "response body" if self.response_started else "response headers",
                SEND_TIMEOUT_ENV,
            )
            raise _StalledClient(self.description) from None

    async def __call__(self, scope: typing.Any, receive: typing.Any, send: typing.Any) -> None:
        queue: asyncio.Queue = asyncio.Queue(_BODY_QUEUE_SIZE)
        environ = build_environ(scope, _PumpedBody(self.loop, queue))
        environ[ENVIRON_KEY] = self.disconnected
        self.description = "%s %s from %s" % (
            scope.get("method", "?"),
            scope.get("path", "?"),
            (scope.get("client") or ("-",))[0],
        )
        sender = None
        pump = None
        try:
            pump = self.loop.create_task(_pump(receive, queue, self.disconnected))
            sender = self.loop.create_task(self.sender(send))
            context = contextvars.copy_context()
            func = functools.partial(context.run, self.wsgi)
            try:
                await self.loop.run_in_executor(
                    self.executor, func, environ, self.start_response
                )
            except _StalledClient:
                # ``send`` has already logged, set the disconnect Event and
                # cancelled its pending put; unwinding the WSGI thread ran
                # a2wsgi's ``finally: iterable.close()``, so the generator got
                # its GeneratorExit and did its abort accounting. Nothing is
                # left to do but let ``finally`` cancel the tasks.
                #
                # Returning rather than re-raising is deliberate. Re-raising
                # reaches uvicorn's ``run_asgi``, which logs "Exception in ASGI
                # application" with a full traceback and only then closes the
                # transport — an alarming stack dump for a routine dead client.
                # Returning leaves the response incomplete, which uvicorn
                # answers with a single "ASGI callable returned without
                # completing response." line and the same transport close. Same
                # cleanup, no traceback.
                #
                # And it must return *here*, before the two awaits below:
                # ``send_queue`` is full and the sender task is stuck in the
                # server's write drain, so ``put(None)`` and ``await sender``
                # would block for exactly as long as the send we just gave up on.
                return
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
