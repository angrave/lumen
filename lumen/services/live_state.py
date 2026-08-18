"""In-flight LLM requests, per model — the seam the Redis backend plugs into.

"Live" means **admitted → request completion**, not admitted → first token: a
generating request still holds a backend sequence slot, so a count that stops at
the first token counts nothing useful. The UI label must therefore read "in
flight", never "waiting"; the "N ahead of you" number is the backend's own queue
depth (Phase 6), not this.

``admit`` is called in the view, while the request context is live and *after*
every rejection path — a rejected request must never appear in flight. The
returned ticket is captured into the streaming generator's closure and
``release`` is called from a ``finally`` around the generator body.

``release`` runs context-free: it must never touch ``db.session`` or
``current_app``, because it runs on whichever thread iterates the response body,
outside any request context. Anything a backend needs from config is read at
``admit`` time and carried on the ticket.

The ``finally`` is the fast path, not the correctness argument — **the deadline
is**. Under uvicorn + a2wsgi a disconnected client's generator may never be
closed, so ``GeneratorExit`` and therefore ``finally`` may never run (see
``send_message_stream``'s docstring in ``lumen/services/llm.py``). An abandoned
ticket that is never released would inflate the count permanently, so every
ticket carries a deadline and expired tickets are excluded from every number
reported.
"""
import threading
import time
import uuid
from dataclasses import dataclass

from flask import current_app, has_app_context

from lumen.services.db_pool import detect_replicas, detect_workers

#: Wall-clock seconds a request is allowed to take before its ticket is presumed
#: abandoned. ``api.request_budget_seconds`` is Phase 2 item 6's key; until it
#: lands the fallback is 600s + the grace below.
_DEFAULT_REQUEST_BUDGET = 600.0

#: Added to the request budget so a request that is merely finishing late is not
#: dropped from the count while it is still running.
_DEADLINE_GRACE = 60.0


@dataclass(frozen=True)
class LiveTicket:
    """Handle returned by :meth:`LocalLiveState.admit`, needed to release."""

    model_key: str
    entity_id: int
    request_id: str
    deadline: float  # unix seconds


@dataclass(frozen=True)
class ModelLive:
    """One model's live numbers. A user with three concurrent requests is three
    in flight and one unique user."""

    inflight: int
    unique_users: int


def _request_budget() -> float:
    if not has_app_context():
        return _DEFAULT_REQUEST_BUDGET
    api_cfg = current_app.config.get("YAML_DATA", {}).get("api", {})
    try:
        return float(api_cfg.get("request_budget_seconds", _DEFAULT_REQUEST_BUDGET))
    except (TypeError, ValueError):
        return _DEFAULT_REQUEST_BUDGET


class LocalLiveState:
    """Per-process live state: a dict under a lock. Redis backend is Phase 5."""

    def __init__(self):
        self._lock = threading.Lock()
        self._tickets: dict[str, LiveTicket] = {}

    def admit(self, model_key: str, entity_id: int) -> LiveTicket:
        ticket = LiveTicket(
            model_key=model_key,
            entity_id=entity_id,
            request_id=uuid.uuid4().hex,
            deadline=time.time() + _request_budget() + _DEADLINE_GRACE,
        )
        with self._lock:
            self._prune(time.time())
            self._tickets[ticket.request_id] = ticket
        return ticket

    def release(self, ticket: LiveTicket) -> None:
        with self._lock:
            self._tickets.pop(ticket.request_id, None)

    def snapshot(self) -> dict[str, ModelLive]:
        now = time.time()
        with self._lock:
            self._prune(now)
            live = list(self._tickets.values())
        by_model: dict[str, list[LiveTicket]] = {}
        for ticket in live:
            by_model.setdefault(ticket.model_key, []).append(ticket)
        return {
            model_key: ModelLive(
                inflight=len(tickets),
                unique_users=len({t.entity_id for t in tickets}),
            )
            for model_key, tickets in by_model.items()
        }

    def topology(self) -> dict:
        """What the numbers cover, so a reader is not misled by a per-process
        count presented as a fleet-wide one."""
        return {
            "scope": "local",
            "processes": detect_workers(),
            "replicas": detect_replicas(),
        }

    def _prune(self, now: float) -> None:
        """Drop tickets past their deadline. Caller holds the lock."""
        expired = [rid for rid, t in self._tickets.items() if t.deadline <= now]
        for rid in expired:
            del self._tickets[rid]


_live_state = LocalLiveState()


def get_live_state() -> LocalLiveState:
    return _live_state
