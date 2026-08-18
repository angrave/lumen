"""Tests for the in-flight request seam (LocalLiveState).

Two properties carry the weight. Admit/release must balance to zero on *every*
exit path, or the count drifts up forever; and multiplicity must be preserved —
one user with three concurrent requests is three in flight and one unique user,
not one of each.
"""
import threading

import pytest

from lumen.services.live_state import LiveTicket, LocalLiveState, get_live_state


def test_admit_then_release_balances_to_zero():
    state = LocalLiveState()
    ticket = state.admit("gpt-4o", 7)
    assert state.snapshot()["gpt-4o"].inflight == 1
    state.release(ticket)
    assert state.snapshot() == {}


@pytest.mark.parametrize("exit_path", ["normal", "exception", "client_disconnected"])
def test_every_streaming_exit_path_releases(exit_path):
    """The release lives in a ``finally`` around the generator body, so each of
    these — a completed stream, an upstream error, a client that went away
    mid-stream — must leave the state at zero."""
    state = LocalLiveState()

    def generate():
        ticket = state.admit("gpt-4o", 7)
        try:
            if exit_path == "exception":
                raise RuntimeError("upstream blew up")
            yield "chunk"
        finally:
            state.release(ticket)

    gen = generate()
    if exit_path == "exception":
        with pytest.raises(RuntimeError):
            list(gen)
    elif exit_path == "client_disconnected":
        next(gen)
        gen.close()
    else:
        list(gen)

    assert state.snapshot() == {}


@pytest.mark.parametrize("raises", [False, True])
def test_the_non_streaming_path_releases(raises):
    """The API non-stream and audio paths wrap the upstream call in the view."""
    state = LocalLiveState()
    ticket = state.admit("gpt-4o", 7)
    try:
        if raises:
            raise RuntimeError("upstream blew up")
    except RuntimeError:
        pass
    finally:
        state.release(ticket)
    assert state.snapshot() == {}


def test_three_requests_from_one_user_are_three_in_flight_and_one_user():
    """The multiplicity property: in-flight counts requests, unique_users counts
    users. Collapsing them loses the number Phase 9 needs."""
    state = LocalLiveState()
    tickets = [state.admit("gpt-4o", 7) for _ in range(3)]
    tickets.append(state.admit("gpt-4o", 8))

    live = state.snapshot()["gpt-4o"]
    assert live.inflight == 4
    assert live.unique_users == 2

    for ticket in tickets:
        state.release(ticket)
    assert state.snapshot() == {}


def test_models_are_counted_separately():
    state = LocalLiveState()
    state.admit("gpt-4o", 1)
    state.admit("llama", 1)
    snapshot = state.snapshot()
    assert snapshot["gpt-4o"].inflight == 1
    assert snapshot["llama"].inflight == 1


def test_a_ticket_past_its_deadline_stops_being_counted(monkeypatch):
    """A disconnected client's generator may never be closed, so ``finally`` may
    never run. Without the deadline that ticket would inflate the count forever.
    """
    import lumen.services.live_state as live_state

    monkeypatch.setattr(live_state, "_request_budget", lambda: -live_state._DEADLINE_GRACE - 1)
    state = LocalLiveState()
    state.admit("gpt-4o", 7)
    assert state.snapshot() == {}


def test_expired_tickets_are_pruned_on_admit(monkeypatch):
    import lumen.services.live_state as live_state

    monkeypatch.setattr(live_state, "_request_budget", lambda: -live_state._DEADLINE_GRACE - 1)
    state = LocalLiveState()
    state.admit("gpt-4o", 7)
    monkeypatch.setattr(live_state, "_request_budget", lambda: 600.0)
    state.admit("gpt-4o", 8)
    assert len(state._tickets) == 1


def test_release_needs_no_app_or_request_context(app):
    """``release`` runs on whichever thread iterates the response body, outside
    any Flask context, so it must not touch ``current_app`` or ``db.session``."""
    state = LocalLiveState()
    with app.app_context():
        ticket = state.admit("gpt-4o", 7)
    state.release(ticket)  # no context here on purpose
    assert state.snapshot() == {}


def test_release_of_an_unknown_ticket_is_a_no_op():
    state = LocalLiveState()
    state.release(LiveTicket("gpt-4o", 7, "never-admitted", deadline=1e12))
    assert state.snapshot() == {}


def test_concurrent_admit_release_balances():
    state = LocalLiveState()

    def worker(entity_id):
        for _ in range(50):
            state.release(state.admit("gpt-4o", entity_id))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert state.snapshot() == {}


def test_topology_is_local_and_reports_the_multiplier(monkeypatch):
    monkeypatch.setenv("WEB_CONCURRENCY", "4")
    monkeypatch.setenv("LUMEN_REPLICAS", "2")
    assert LocalLiveState().topology() == {"scope": "local", "processes": 4, "replicas": 2}


def test_get_live_state_is_process_wide():
    assert get_live_state() is get_live_state()
