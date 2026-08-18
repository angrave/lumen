"""Tests for config_watcher._check_restart_required, _watcher, and start_config_watcher."""
import logging

import pytest


@pytest.fixture
def restore_config(app):
    """Snapshot app.config and restore it afterward.

    apply_hot_config mutates the shared session-scoped app.config; restoring
    prevents leakage into later tests (e.g. CONFIG_EDITOR / DEV_USER).
    """
    saved = dict(app.config)
    yield
    app.config.clear()
    app.config.update(saved)


def _check(old, new, caplog):
    from lumen.services.config_watcher import _check_restart_required
    with caplog.at_level(logging.WARNING, logger="lumen.services.config_watcher"):
        _check_restart_required(old, new)


def test_identical_data_no_warnings(caplog):
    data = {"app": {"secret_key": "abc", "database_url": "postgres://x"}}
    _check(data, data, caplog)
    assert caplog.records == []


def test_secret_key_change_warns(caplog):
    _check({"app": {"secret_key": "old"}}, {"app": {"secret_key": "new"}}, caplog)
    messages = " ".join(r.message for r in caplog.records)
    assert "secret_key" in messages
    assert "restart" in messages


def test_database_change_warns(caplog):
    _check(
        {"app": {"database": {"url": "sqlite:///a.db"}}},
        {"app": {"database": {"url": "sqlite:///b.db"}}},
        caplog,
    )
    assert any("database" in r.message for r in caplog.records)


def test_debug_change_warns(caplog):
    _check({"app": {"debug": False}}, {"app": {"debug": True}}, caplog)
    assert any("debug" in r.message for r in caplog.records)


def test_prometheus_enabled_change_warns(caplog):
    _check({"api": {"prometheus": {"enabled": False}}}, {"api": {"prometheus": {"enabled": True}}}, caplog)
    assert any("prometheus" in r.message for r in caplog.records)


def test_prometheus_multiproc_dir_change_warns(caplog):
    _check(
        {"api": {"prometheus": {"multiproc_dir": "/a"}}},
        {"api": {"prometheus": {"multiproc_dir": "/b"}}},
        caplog,
    )
    assert any("multiproc_dir" in r.message for r in caplog.records)


def test_oauth2_client_id_change_warns(caplog):
    _check({"oauth2": {"client_id": "aaa"}}, {"oauth2": {"client_id": "bbb"}}, caplog)
    assert any("oauth2" in r.message for r in caplog.records)


def test_oauth2_added_key_warns(caplog):
    _check({}, {"oauth2": {"client_secret": "xyz"}}, caplog)
    assert any("oauth2" in r.message for r in caplog.records)


def test_app_name_change_no_warning(caplog):
    """app.name is hot-reloadable — changing it must not emit a restart warning."""
    _check({"app": {"name": "Lumen"}}, {"app": {"name": "My Lumen"}}, caplog)
    assert caplog.records == []


def test_chat_config_change_no_warning(caplog):
    """chat.* keys are hot-reloadable — no restart warning."""
    _check({"chat": {"remove": "hide"}}, {"chat": {"remove": "delete"}}, caplog)
    assert caplog.records == []


def test_restart_keys_covered():
    """Smoke-check that the known restart-required keys are present in _RESTART_REQUIRED."""
    from lumen.services.config_watcher import _RESTART_REQUIRED
    keys = {tuple(p) for p in _RESTART_REQUIRED}
    assert ("app", "secret_key") in keys
    assert ("app", "database") in keys
    assert ("api", "prometheus", "enabled") in keys


def test_start_config_watcher_creates_daemon_thread(app, tmp_path):
    from unittest.mock import MagicMock, patch

    from lumen.services.config_watcher import start_config_watcher

    config_path = str(tmp_path / "config.yaml")
    with patch("lumen.services.config_watcher.threading.Thread") as mock_cls:
        mock_thread = MagicMock()
        mock_cls.return_value = mock_thread
        start_config_watcher(app, config_path)

    mock_cls.assert_called_once()
    assert mock_cls.call_args[1]["daemon"] is True
    mock_thread.start.assert_called_once()


def test_watcher_reloads_config_on_mtime_change(app, tmp_path, restore_config):
    from unittest.mock import patch

    import yaml

    from lumen.services.config_watcher import _watcher

    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump({"app": {"name": "Reloaded"}}))

    sleep_count = 0

    def fake_sleep(n):
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count >= 3:
            raise SystemExit("stop")

    mtime_values = [1.0, 2.0]
    mtime_idx = 0

    def fake_getmtime(path):
        nonlocal mtime_idx
        v = mtime_values[mtime_idx] if mtime_idx < len(mtime_values) else 2.0
        mtime_idx += 1
        return v

    with patch("lumen.services.config_watcher.time.sleep", side_effect=fake_sleep):
        with patch("lumen.services.config_watcher.os.path.getmtime", side_effect=fake_getmtime):
            try:
                _watcher(app, str(config_file))
            except SystemExit:
                pass

    with app.app_context():
        assert app.config.get("APP_NAME") == "Reloaded"


def test_watcher_reconciles_user_groups_on_reload(app, tmp_path, restore_config):
    from unittest.mock import patch

    import yaml

    from lumen.services.config_watcher import _watcher

    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump({"app": {"name": "Reloaded"}}))

    sleep_count = 0

    def fake_sleep(n):
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count >= 3:
            raise SystemExit("stop")

    mtime_values = [1.0, 2.0]
    mtime_idx = 0

    def fake_getmtime(path):
        nonlocal mtime_idx
        v = mtime_values[mtime_idx] if mtime_idx < len(mtime_values) else 2.0
        mtime_idx += 1
        return v

    with patch("lumen.services.config_watcher.time.sleep", side_effect=fake_sleep), \
         patch("lumen.services.config_watcher.sync_models_from_yaml"), \
         patch("lumen.services.config_watcher.sync_groups_from_yaml"), \
         patch("lumen.services.config_watcher.sync_projects_from_yaml"), \
         patch("lumen.services.config_watcher.sync_user_groups_from_yaml") as mock_sync_user_groups, \
         patch("lumen.services.config_watcher.sync_user_limits_from_yaml") as mock_sync_user_limits, \
         patch("lumen.services.config_watcher.os.path.getmtime", side_effect=fake_getmtime):
        try:
            _watcher(app, str(config_file))
        except SystemExit:
            pass

    mock_sync_user_groups.assert_called_once()
    mock_sync_user_limits.assert_called_once()


def test_watcher_skips_when_mtime_unchanged(app, tmp_path, restore_config):
    from unittest.mock import patch

    import yaml

    from lumen.services.config_watcher import _watcher

    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump({"app": {"name": "Original"}}))
    with app.app_context():
        app.config["APP_NAME"] = "Original"

    sleep_count = 0

    def fake_sleep(n):
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count >= 3:
            raise SystemExit("stop")

    with patch("lumen.services.config_watcher.time.sleep", side_effect=fake_sleep):
        with patch("lumen.services.config_watcher.os.path.getmtime", return_value=1.0):
            try:
                _watcher(app, str(config_file))
            except SystemExit:
                pass

    with app.app_context():
        assert app.config.get("APP_NAME") == "Original"


def test_watcher_handles_read_error_gracefully(app, tmp_path, restore_config):
    from unittest.mock import patch

    from lumen.services.config_watcher import _watcher

    config_file = tmp_path / "config.yaml"
    config_file.write_text("app:\n  name: Test\n")

    sleep_count = 0

    def fake_sleep(n):
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count >= 3:
            raise SystemExit("stop")

    mtime_values = [1.0, 2.0]
    mtime_idx = 0

    def fake_getmtime(path):
        nonlocal mtime_idx
        v = mtime_values[mtime_idx] if mtime_idx < len(mtime_values) else 2.0
        mtime_idx += 1
        return v

    with patch("lumen.services.config_watcher.time.sleep", side_effect=fake_sleep):
        with patch("lumen.services.config_watcher.os.path.getmtime", side_effect=fake_getmtime):
            with patch("builtins.open", side_effect=OSError("disk error")):
                try:
                    _watcher(app, str(config_file))
                except SystemExit:
                    pass


def test_dev_user_set_logs_warning(app, caplog, restore_config):
    """A configured dev_user emits a loud warning so an accidental prod setting is visible."""
    import logging

    from lumen.services.config_watcher import apply_hot_config
    with caplog.at_level(logging.WARNING, logger="lumen.services.config_watcher"):
        with app.app_context():
            apply_hot_config(app, {"app": {"dev_user": "dev@example.com"}})
    assert any("DEV LOGIN ENABLED" in r.message for r in caplog.records)


def test_no_dev_user_no_warning(app, caplog, restore_config):
    import logging

    from lumen.services.config_watcher import apply_hot_config
    with caplog.at_level(logging.WARNING, logger="lumen.services.config_watcher"):
        with app.app_context():
            apply_hot_config(app, {"app": {}})
    assert not any("DEV LOGIN ENABLED" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# apply_hot_config: unrecognised keys in the 'app' section
# ---------------------------------------------------------------------------

def test_unknown_app_key_warns(app, caplog, restore_config):
    """A key renamed by a schema change must not fail silently.

    Every read of the app section is a .get() with a default, so an orphaned key
    is simply ignored. `app.database_url` outlived the move to `app.database.url`
    by six weeks that way, which left the whole test suite running against the
    developer's dev database and dropping its tables on every run.
    """
    import logging

    from lumen.services.config_watcher import apply_hot_config
    with caplog.at_level(logging.WARNING, logger="lumen.services.config_watcher"):
        with app.app_context():
            apply_hot_config(app, {"app": {"database_url": "sqlite:///stale.db"}})
    messages = " ".join(r.getMessage() for r in caplog.records)
    assert "unrecognised key" in messages
    assert "database_url" in messages


def test_known_app_keys_do_not_warn(app, caplog, restore_config):
    import logging

    from lumen.services.config_watcher import KNOWN_APP_KEYS, apply_hot_config
    # Empty dicts are inert placeholders for every shape these keys can take
    # (str, bool, mapping); the point is only that the key name is recognised.
    yaml_data = {"app": {key: {} for key in KNOWN_APP_KEYS}}
    with caplog.at_level(logging.WARNING, logger="lumen.services.config_watcher"):
        with app.app_context():
            apply_hot_config(app, yaml_data)
    assert not any("unrecognised key" in r.getMessage() for r in caplog.records)


def test_shipped_configs_have_no_unknown_app_keys():
    """The fixture and the shipped configs must stay in step with the schema.

    This is the check that would have caught the database_url regression on the
    day the key moved, rather than six weeks later.
    """
    from pathlib import Path

    import yaml as _yaml

    from lumen.services.config_watcher import KNOWN_APP_KEYS
    root = Path(__file__).resolve().parents[2]
    for rel in ("tests/fixtures/test_config.yaml", "config.yaml.example", "dev.config.yaml"):
        data = _yaml.safe_load((root / rel).read_text()) or {}
        unknown = sorted(set(data.get("app") or {}) - KNOWN_APP_KEYS)
        assert not unknown, f"{rel} has unrecognised app key(s): {unknown}"


# ---------------------------------------------------------------------------
# apply_hot_config: global defaults and config-editor flag (orthogonal access)
# ---------------------------------------------------------------------------

def test_apply_hot_config_model_and_token_defaults(app, restore_config):
    from lumen.services.config_watcher import apply_hot_config
    yaml_data = {
        "version": 2,
        "defaults": {
            "models": {"access": "allowed", "ack_message": "please ack"},
            "tokens": {"max": 500, "refresh": 50, "starting": 250},
        },
    }
    with app.app_context():
        apply_hot_config(app, yaml_data)
        assert app.config["MODEL_DEFAULTS"] == {"access": "allowed", "ack_message": "please ack"}
        assert app.config["TOKEN_DEFAULTS"] == {"max": 500, "refresh": 50, "starting": 250}


def test_apply_hot_config_defaults_when_absent(app, restore_config):
    """With no defaults block, access defaults to blocked and token pool to zeros."""
    from lumen.services.config_watcher import apply_hot_config
    with app.app_context():
        apply_hot_config(app, {"version": 2})
        assert app.config["MODEL_DEFAULTS"]["access"] == "blocked"
        assert app.config["MODEL_DEFAULTS"]["ack_message"] is None
        assert app.config["TOKEN_DEFAULTS"] == {"max": 0, "refresh": 0, "starting": 0}


def test_apply_hot_config_token_starting_defaults_to_max(app, restore_config):
    """When 'starting' is omitted it falls back to 'max'."""
    from lumen.services.config_watcher import apply_hot_config
    with app.app_context():
        apply_hot_config(app, {"version": 2, "defaults": {"tokens": {"max": 300, "refresh": 30}}})
        assert app.config["TOKEN_DEFAULTS"] == {"max": 300, "refresh": 30, "starting": 300}


def test_apply_hot_config_legacy_graylist_notice_feeds_ack_message(app, restore_config):
    """Legacy app.graylist_default_notice maps to defaults.models.ack_message when unset."""
    from lumen.services.config_watcher import apply_hot_config
    with app.app_context():
        apply_hot_config(app, {"version": 2, "app": {"graylist_default_notice": "legacy notice"}})
        assert app.config["MODEL_DEFAULTS"]["ack_message"] == "legacy notice"


def test_apply_hot_config_config_editor_default_true(app, restore_config):
    from lumen.services.config_watcher import apply_hot_config
    with app.app_context():
        apply_hot_config(app, {"version": 2, "app": {}})
        assert app.config["CONFIG_EDITOR"] is True


def test_apply_hot_config_config_editor_disabled(app, restore_config):
    from lumen.services.config_watcher import apply_hot_config
    with app.app_context():
        apply_hot_config(app, {"version": 2, "app": {"config_editor": False}})
        assert app.config["CONFIG_EDITOR"] is False


# ---------------------------------------------------------------------------
# apply_hot_config: upstream LLM call bounds
# ---------------------------------------------------------------------------

def test_apply_hot_config_llm_bounds_from_yaml(app, restore_config):
    from lumen.services.config_watcher import apply_hot_config
    yaml_data = {
        "version": 2,
        "llm": {"connect_timeout": 3, "read_timeout": 45, "max_retries": 0},
    }
    with app.app_context():
        apply_hot_config(app, yaml_data)
        assert app.config["LLM_CONNECT_TIMEOUT"] == 3.0
        assert app.config["LLM_READ_TIMEOUT"] == 45.0
        assert app.config["LLM_MAX_RETRIES"] == 0


def test_apply_hot_config_llm_defaults_when_section_absent(app, restore_config):
    from lumen.services.config_watcher import apply_hot_config
    with app.app_context():
        apply_hot_config(app, {"version": 2})
        assert app.config["LLM_CONNECT_TIMEOUT"] == 5.0
        assert app.config["LLM_READ_TIMEOUT"] == 300.0
        assert app.config["LLM_MAX_RETRIES"] == 1


def test_apply_hot_config_llm_defaults_for_missing_keys(app, restore_config):
    """A partial llm section keeps the defaults for the keys it omits."""
    from lumen.services.config_watcher import apply_hot_config
    with app.app_context():
        apply_hot_config(app, {"version": 2, "llm": {"read_timeout": 30}})
        assert app.config["LLM_CONNECT_TIMEOUT"] == 5.0
        assert app.config["LLM_READ_TIMEOUT"] == 30.0
        assert app.config["LLM_MAX_RETRIES"] == 1


def test_apply_hot_config_llm_blank_key_uses_default(app, restore_config):
    """'read_timeout:' with no value parses as None and must fall back, not crash."""
    from lumen.services.config_watcher import apply_hot_config
    with app.app_context():
        apply_hot_config(app, {"version": 2, "llm": {"read_timeout": None}})
        assert app.config["LLM_READ_TIMEOUT"] == 300.0


def test_apply_hot_config_llm_values_are_floats_and_int(app, restore_config):
    """Values are coerced so the SDK never receives a yaml string."""
    from lumen.services.config_watcher import apply_hot_config
    with app.app_context():
        apply_hot_config(app, {"version": 2, "llm": {"connect_timeout": "2.5", "max_retries": "3"}})
        assert app.config["LLM_CONNECT_TIMEOUT"] == 2.5
        assert isinstance(app.config["LLM_CONNECT_TIMEOUT"], float)
        assert app.config["LLM_MAX_RETRIES"] == 3
        assert isinstance(app.config["LLM_MAX_RETRIES"], int)


def test_watcher_reloads_llm_bounds(app, tmp_path, restore_config):
    """A changed llm section is picked up by a hot reload, not just at startup."""
    from unittest.mock import patch

    import yaml

    from lumen.services.config_watcher import _watcher, apply_hot_config

    with app.app_context():
        apply_hot_config(app, {"version": 2, "llm": {"read_timeout": 10, "max_retries": 0}})
        assert app.config["LLM_READ_TIMEOUT"] == 10.0

    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump({"version": 2, "llm": {"read_timeout": 90, "max_retries": 2}}))

    sleep_count = 0

    def fake_sleep(n):
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count >= 3:
            raise SystemExit("stop")

    mtime_values = [1.0, 2.0]
    mtime_idx = 0

    def fake_getmtime(path):
        nonlocal mtime_idx
        v = mtime_values[mtime_idx] if mtime_idx < len(mtime_values) else 2.0
        mtime_idx += 1
        return v

    with patch("lumen.services.config_watcher.time.sleep", side_effect=fake_sleep), \
         patch("lumen.services.config_watcher.sync_models_from_yaml"), \
         patch("lumen.services.config_watcher.sync_groups_from_yaml"), \
         patch("lumen.services.config_watcher.sync_projects_from_yaml"), \
         patch("lumen.services.config_watcher.sync_user_groups_from_yaml"), \
         patch("lumen.services.config_watcher.sync_user_limits_from_yaml"), \
         patch("lumen.services.config_watcher.os.path.getmtime", side_effect=fake_getmtime):
        try:
            _watcher(app, str(config_file))
        except SystemExit:
            pass

    with app.app_context():
        assert app.config["LLM_READ_TIMEOUT"] == 90.0
        assert app.config["LLM_MAX_RETRIES"] == 2


def test_shipped_config_example_has_llm_section(app, restore_config):
    """config.yaml.example documents the section and it loads to the stated defaults."""
    from pathlib import Path

    import yaml as _yaml

    from lumen.services.config_watcher import apply_hot_config
    root = Path(__file__).resolve().parents[2]
    data = _yaml.safe_load((root / "config.yaml.example").read_text())
    assert set(data["llm"]) == {"connect_timeout", "read_timeout", "request_timeout", "max_retries"}
    with app.app_context():
        apply_hot_config(app, data)
        assert app.config["LLM_CONNECT_TIMEOUT"] == 5.0
        assert app.config["LLM_READ_TIMEOUT"] == 300.0
        assert app.config["LLM_REQUEST_TIMEOUT"] == 600.0
        assert app.config["LLM_MAX_RETRIES"] == 1


def test_apply_hot_config_v1_emits_version_deprecation_warning(app, caplog, restore_config):
    import logging

    import lumen.services.config_watcher as cw
    cw._version_warned = False
    try:
        with caplog.at_level(logging.WARNING, logger="lumen.services.config_watcher"):
            with app.app_context():
                cw.apply_hot_config(app, {"app": {}})  # no 'version' key -> treated as v1
        assert any("version: 2" in r.message for r in caplog.records)
    finally:
        cw._version_warned = False


def test_apply_hot_config_v2_no_version_warning(app, caplog, restore_config):
    import logging

    import lumen.services.config_watcher as cw
    cw._version_warned = False
    try:
        with caplog.at_level(logging.WARNING, logger="lumen.services.config_watcher"):
            with app.app_context():
                cw.apply_hot_config(app, {"version": 2, "app": {}})
        assert not any("version: 2" in r.message for r in caplog.records)
    finally:
        cw._version_warned = False
