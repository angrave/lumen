import logging
import os
import re
import time
import weakref

from flask.globals import _cv_app

# Use the default registry (no registry= kwarg) so prometheus_client's multiprocess
# mode is automatically engaged when PROMETHEUS_MULTIPROC_DIR is set.
from prometheus_client import Counter, Histogram
from prometheus_client.multiprocess import mark_process_dead

from lumen.extensions import db
from lumen.services.ctx_probe import describe_push, record_context_anomaly

logger = logging.getLogger(__name__)

# Multiprocess invariant, for every metric defined in this file and anywhere
# else in lumen/:
#
#   Every ``Gauge`` MUST pass an explicit ``multiprocess_mode``. Under
#   PROMETHEUS_MULTIPROC_DIR each process writes its own mmap file and the
#   scrape merges them; a gauge with no declared mode gets the library default
#   ("all"), which exposes one series per pid — not the fleet number anyone
#   reading the dashboard thinks they are looking at. ``Counter`` and
#   ``Histogram`` need nothing: summing is the only correct merge for them.
#   Enforced statically by tests/unit/test_metrics_middleware.py.
#
#   The counter/gauge asymmetry is deliberate, and the two cases look alike:
#   a dead worker's COUNTER file is still summed into the scrape, and that is
#   CORRECT — those increments are real requests that really happened, and
#   dropping them would silently lose history. Only ``livesum``/``liveall``
#   gauges must exclude dead workers, because a gauge is a statement about
#   right now and a dead worker's last value is not true any more. That is what
#   ``reap_dead_workers()`` below (and ``mark_process_dead``) removes — gauge
#   files only; it never touches a counter file. Do not "fix" the asymmetry.

# App contexts already reported as ambient-at-request-start, so a poisoned
# worker thread is reported once rather than on every subsequent request it
# serves. Weak so dead contexts do not pin memory or block id() reuse detection.
_reported_ambient: "weakref.WeakSet" = weakref.WeakSet()

_http_requests = Counter(
    "lumen_http_requests_total",
    "Total HTTP requests",
    ["method", "path_template", "status"],
)
_http_latency = Histogram(
    "lumen_http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "path_template"],
    # Buckets run to 300s because this now times streaming responses too, and a
    # long generation is measured in minutes. With the old 10s ceiling every
    # stream landed in +Inf, which records that it was slow but never how slow.
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
             30.0, 60.0, 120.0, 300.0),
)
_stream_aborts = Counter(
    "lumen_stream_aborts_total",
    "Streaming LLM responses that ended before the client had the whole reply",
    ["source", "reason"],
)


def observe_stream_abort(source: str, reason: str) -> None:
    """Count one streaming response that ended early.

    ``source`` is the request source as recorded in ``request_logs`` ("chat" or
    "api"); ``reason`` is why the stream ended ("disconnect" for the polled
    client-disconnect flag or a ``GeneratorExit``, "upstream_error" for a
    failure from the backend).

    Mid-stream disconnects were structurally invisible in production before the
    ASGI bridge started delivering them (see ``services/wsgi_disconnect.py``);
    this is the metric that makes them observable, so it has to be incremented
    on every abort path rather than only defined.
    """
    _stream_aborts.labels(source=source, reason=reason).inc()


_PID_SUFFIXED = re.compile(r"_(\d+)\.db$")


def reap_dead_workers():
    """Mark PIDs with files in the multiproc dir that are no longer alive.

    ``mark_process_dead`` is only ever called on a clean shutdown, so it covers
    SIGTERM and nothing else. A worker killed by SIGKILL — uvicorn's
    post-grace-period kill, or the OOM killer — never runs it, and its
    ``gauge_livesum_<pid>.db`` keeps contributing its last value to every
    aggregate for the rest of the *pod's* life, because the dir is wiped once
    per pod start and not per worker respawn. A worker OOM-killed holding
    ``queue_depth=5`` leaves the fleet depth 5 too high indefinitely, during
    exactly the burst that killed it. So reconcile instead: list the dir, probe
    each pid with signal 0, and mark the dead ones.

    Every ``mark_process_dead`` call is wrapped. The library's implementation
    (``prometheus_client/multiprocess.py``) does an unguarded ``glob`` +
    ``os.remove``, so when two processes reap the same pid concurrently the
    loser raises ``FileNotFoundError`` — and this runs inside the ``/metrics``
    handler, so an unhandled raise is a 500 on the scrape. Concurrent reaps are
    routine (N workers plus a per-pod scrape) and they cluster right after a
    worker dies, which is precisely when the scrape must not break.

    PID reuse is handled separately by ``multiproc.clear_own_stale_gauges``,
    which must run before this module is imported at all — see that module for
    why it cannot live here.

    No-ops when PROMETHEUS_MULTIPROC_DIR is unset, and never raises.
    """
    path = os.environ.get("PROMETHEUS_MULTIPROC_DIR")
    if not path:
        return
    try:
        names = os.listdir(path)
    except OSError:
        return
    mine = os.getpid()
    dead = set()
    for name in names:
        match = _PID_SUFFIXED.search(name)
        if not match:
            continue
        pid = int(match.group(1))
        if pid == mine or pid in dead:
            continue
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            dead.add(pid)
        except OSError:
            # EPERM means the pid exists but belongs to someone else — alive.
            continue
    for pid in dead:
        try:
            mark_process_dead(pid, path)
        except (FileNotFoundError, OSError):
            # Another process reaped the same pid first; its files are gone,
            # which is the outcome we wanted anyway.
            pass


# Label values must come from a bounded set. A scanner sending PROPFIND, TRACK,
# FOOBARBAZ, ... would otherwise mint a new series per method (HTTP methods are
# RFC token grammar, so the set is unbounded), the same explosion as unbounded
# paths and amplified once per worker process.
_STANDARD_METHODS = frozenset(
    {"GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH"}
)


def _label_method(method):
    return method if method in _STANDARD_METHODS else "<other>"


def _label_path(environ):
    """The matched Flask url_rule, stashed into environ while the context lived.

    Routing has not happened when the middleware is entered, and Flask's
    ``wsgi_app`` runs ``ctx.pop(error)`` in its ``finally`` *before* returning
    the response iterable — so there is no request context in these closures
    either before or after the call, and ``request.url_rule`` cannot be read
    here at all. ``create_app``'s ``teardown_request`` hook writes the rule into
    environ instead. Anything unrouted (scanners hitting /.env, /wp-admin, ...)
    shares one bucket, which is what bounds the label set.
    """
    return environ.get("lumen.url_rule") or "<unmatched>"


class _ContextCheckingBody:
    """Wraps the response iterable to verify context hygiene after close().

    The WSGI server's ``close()`` on the response body is the last moment a
    request executes code on its thread. Flask pops its contexts before
    ``wsgi_app`` returns (streaming responses pop when the body generator is
    closed), so the current app context must be back to whatever it was when
    the request started — compared against a baseline rather than None, since
    an ambient context is legitimate (test fixtures, nested dispatch). A
    context left on top of the baseline has escaped teardown: its DB session
    stays registered forever and its connection is stranded. This names the
    request that did it, at the moment it happens.
    """

    def __init__(self, iterable, method, path, baseline_ctx, observe_latency=None):
        self._iterable = iterable
        self._method = method
        self._path = path
        self._baseline_ctx = baseline_ctx
        self._observe_latency = observe_latency
        self._observed = False

    def __iter__(self):
        return iter(self._iterable)

    def close(self):
        try:
            close = getattr(self._iterable, "close", None)
            if close is not None:
                close()
        finally:
            # Latency is observed here, not when wsgi_app() returned. A streaming
            # view returns its generator immediately, so timing the call measured
            # how long it took to *build* the generator — microseconds — while the
            # response the user actually waited for was still being produced.
            # close() is the last moment the request runs on this thread and the
            # WSGI server guarantees it, so it is the honest end of the request.
            if self._observe_latency is not None and not self._observed:
                self._observed = True
                self._observe_latency()
            ctx = _cv_app.get(None)
            if ctx is not None and ctx is not self._baseline_ctx:
                record_context_anomaly(
                    "leftover-context", id(ctx), len(ctx._cv_tokens),
                    f"left behind by {self._method} {self._path}",
                )
                logger.warning(
                    "app context 0x%x (push depth %d) still current after %s %s "
                    "finished; teardown never ran for it and the DB session "
                    "registered under its scope is stranded",
                    id(ctx), len(ctx._cv_tokens), self._method, self._path,
                )


def make_metrics_middleware(wsgi_app):
    def middleware(environ, start_response):
        # The raw-ish path and method are for the context-anomaly log messages,
        # which want to name the actual request. The metric labels are the
        # bounded values computed by _label_path()/_label_method() instead.
        path = _normalize_path(environ.get("PATH_INFO", ""))
        method = environ.get("REQUEST_METHOD", "")
        label_method = _label_method(method)
        status_holder = ["500"]

        def _start_response(status, headers, exc_info=None):
            status_holder[0] = status.split(" ", 1)[0]
            return start_response(status, headers, exc_info)

        baseline_ctx = _cv_app.get(None)
        # An app context already current when a request STARTS is a poisoned
        # worker thread: some earlier request pushed it and never popped, and
        # every session created while it is current is keyed to it — never torn
        # down. The close()-time check compares against this baseline and so is
        # blind to exactly this case; report it here instead, once per context.
        # (Legitimate in tests, where the client runs inside a fixture context.)
        if baseline_ctx is not None:
            # Self-heal outside tests (where an ambient context around the test
            # client is legitimate): release the session registered under the
            # stuck context's key and clear the contextvar, so this request —
            # and every later one on this thread — pushes a fresh context.
            heal = not baseline_ctx.app.testing
            if baseline_ctx not in _reported_ambient:
                _reported_ambient.add(baseline_ctx)
                record_context_anomaly(
                    "ambient-context-at-start", id(baseline_ctx), len(baseline_ctx._cv_tokens),
                    # app id distinguishes a second Flask app object's context (its
                    # sessions key elsewhere) from this app's (sessions key to it and
                    # leak); the push provenance names whoever left it behind.
                    f"already current when {method} {path} started; "
                    f"app=0x{id(baseline_ctx.app):x}; "
                    f"{'healed (session closed, context cleared); ' if heal else ''}"
                    f"{describe_push(baseline_ctx)}",
                )
                logger.warning(
                    "app context 0x%x already current at the start of %s %s — this "
                    "worker thread is poisoned; sessions keyed to it are never torn down%s",
                    id(baseline_ctx), method, path, " (healing)" if heal else "",
                )
            if heal:
                stuck = db.session.registry.registry.pop(id(baseline_ctx), None)
                if stuck is not None:
                    try:
                        stuck.close()  # rolls back and returns the connection
                    except Exception:
                        logger.exception("closing the stranded session failed")
                _cv_app.set(None)
                baseline_ctx = None
        start = time.time()

        def _observe_latency():
            _http_latency.labels(
                method=label_method, path_template=_label_path(environ),
            ).observe(time.time() - start)

        try:
            body = wsgi_app(environ, _start_response)
        except BaseException:
            # No body, so the close()-time check below will never run; verify
            # here that the raising request did not abandon a context.
            ctx = _cv_app.get(None)
            if ctx is not None and ctx is not baseline_ctx:
                record_context_anomaly(
                    "leftover-context-after-exception", id(ctx), len(ctx._cv_tokens),
                    f"left behind by {method} {path} raising",
                )
                logger.warning(
                    "app context 0x%x still current after %s %s raised; teardown "
                    "never ran for it and its DB session is stranded",
                    id(ctx), method, path,
                )
            # Nothing will close a body that was never returned, so the
            # close()-time observation cannot happen — record it here instead.
            _observe_latency()
            raise
        else:
            return _ContextCheckingBody(
                body, method, path, baseline_ctx, observe_latency=_observe_latency,
            )
        finally:
            _http_requests.labels(
                method=label_method,
                path_template=_label_path(environ),
                status=status_holder[0],
            ).inc()

    return middleware


def _normalize_path(path):
    """Collapse numeric path segments in the path used for log messages.

    No longer used for metric labels — see _label_path() — but the anomaly logs
    still want something close to the real path, with ids folded so the same
    endpoint reads the same way across requests.
    """
    return re.sub(r"/\d+", "/{id}", path)
