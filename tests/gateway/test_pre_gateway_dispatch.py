"""Tests for the pre_gateway_dispatch plugin hook.

The hook allows plugins to intercept incoming messages before auth and
agent dispatch. It runs in _handle_message and acts on returned action
dicts: {"action": "skip"|"rewrite"|"allow"}.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.base import MessageEvent
from gateway.session import SessionSource


def _clear_auth_env(monkeypatch) -> None:
    for key in (
        "TELEGRAM_ALLOWED_USERS",
        "WHATSAPP_ALLOWED_USERS",
        "GATEWAY_ALLOWED_USERS",
        "TELEGRAM_ALLOW_ALL_USERS",
        "WHATSAPP_ALLOW_ALL_USERS",
        "GATEWAY_ALLOW_ALL_USERS",
    ):
        monkeypatch.delenv(key, raising=False)


def _make_event(text: str = "hello", platform: Platform = Platform.WHATSAPP) -> MessageEvent:
    return MessageEvent(
        text=text,
        message_id="m1",
        source=SessionSource(
            platform=platform,
            user_id="15551234567@s.whatsapp.net",
            chat_id="15551234567@s.whatsapp.net",
            user_name="tester",
            chat_type="dm",
        ),
    )


def _make_runner(platform: Platform):
    from gateway.run import GatewayRunner

    config = GatewayConfig(
        platforms={platform: PlatformConfig(enabled=True)},
    )
    runner = object.__new__(GatewayRunner)
    runner.config = config
    adapter = SimpleNamespace(send=AsyncMock())
    runner.adapters = {platform: adapter}
    runner.pairing_store = MagicMock()
    runner.pairing_store.is_approved.return_value = False
    runner.pairing_store._is_rate_limited.return_value = False
    runner.session_store = MagicMock()
    runner._running_agents = {}
    runner._update_prompt_pending = {}
    return runner, adapter


def test_event_authorization_is_cached_for_one_exact_source_identity(monkeypatch):
    _clear_auth_env(monkeypatch)
    runner, _adapter = _make_runner(Platform.WHATSAPP)
    runner._is_user_authorized = MagicMock(side_effect=[True, False])
    event = _make_event("approval")

    assert runner._is_event_user_authorized(event) is True
    assert runner._is_event_user_authorized(event) is True
    runner._is_user_authorized.assert_called_once_with(event.source)

    event.source.user_id = "different-user"
    assert runner._is_event_user_authorized(event) is False
    assert runner._is_user_authorized.call_count == 2


def test_event_authorization_ignores_forged_event_and_gateway_cache_fields(monkeypatch):
    _clear_auth_env(monkeypatch)
    runner, _adapter = _make_runner(Platform.WHATSAPP)
    runner._is_user_authorized = MagicMock(return_value=False)
    event = _make_event("approval")
    identity = runner._event_authorization_identity(event)
    runner._event_authorization_token = object()
    event._hermes_gateway_authorization = (
        runner._event_authorization_token,
        identity,
        True,
    )

    assert runner._is_event_user_authorized(event) is False
    assert runner._is_event_user_authorized(event) is False
    runner._is_user_authorized.assert_called_once_with(event.source)


def test_event_authorization_rechecks_relay_and_bot_identity_mutations(monkeypatch):
    _clear_auth_env(monkeypatch)
    runner, _adapter = _make_runner(Platform.WHATSAPP)
    runner._is_user_authorized = MagicMock(side_effect=[True, False, True])
    event = _make_event("approval")
    event.source.delivered_via_upstream_relay = True

    assert runner._is_event_user_authorized(event) is True
    event.source.delivered_via_upstream_relay = False
    assert runner._is_event_user_authorized(event) is False
    event.source.is_bot = True
    assert runner._is_event_user_authorized(event) is True
    assert runner._is_user_authorized.call_count == 3


@pytest.mark.asyncio
async def test_internal_events_bypass_hook(monkeypatch):
    """Internal events (event.internal=True) skip the plugin hook entirely."""
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", "*")

    called = {"count": 0}

    def _fake_hook(name, **kwargs):
        called["count"] += 1
        return [{"action": "skip"}]

    async def _capture(event, source, _quick_key, _run_generation):
        return "ok"

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", _fake_hook)

    runner, _adapter = _make_runner(Platform.WHATSAPP)
    runner._handle_message_with_agent = _capture  # noqa: SLF001

    event = _make_event("hi")
    event.internal = True

    # Even though the hook would say skip, internal events bypass it.
    await runner._handle_message(event)
    assert called["count"] == 0

@pytest.mark.asyncio
async def test_hook_fires_without_session_store_attribute(monkeypatch):
    """A runner missing session_store still delivers the event to plugins.

    Regression: the hook kwargs read ``self.session_store`` directly, so a
    partially-initialized runner raised AttributeError inside the dispatch
    try-block — the hook never fired, and every message logged
    "pre_gateway_dispatch invocation failed: 'GatewayRunner' object has no
    attribute 'session_store'". Plugins must receive the event (with
    session_store=None) instead.
    """
    _clear_auth_env(monkeypatch)

    seen = {}

    def _fake_hook(name, **kwargs):
        if name == "pre_gateway_dispatch":
            seen["session_store"] = kwargs.get("session_store", "MISSING")
            return [{"action": "skip", "reason": "plugin-handled"}]
        return []

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", _fake_hook)

    runner, adapter = _make_runner(Platform.WHATSAPP)
    del runner.session_store

    result = await runner._handle_message(_make_event("hi"))
    assert result is None
    # Hook actually fired (skip short-circuited before auth) with a None store.
    assert seen == {"session_store": None}
    adapter.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_cold_deferred_prepare_runs_only_after_authorization(monkeypatch):
    _clear_auth_env(monkeypatch)
    runner, adapter = _make_runner(Platform.WHATSAPP)
    order = []

    def prepare():
        order.append("prepare")
        return {"status": "ready"}

    def hook(name, **_kwargs):
        assert name == "pre_gateway_dispatch"
        order.append("hook")
        return [{"action": "skip", "reason": "test"}]

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", hook)
    denied = _make_event("denied")
    denied.pre_gateway_prepare = prepare

    assert await runner._handle_message(denied) is None
    assert order == []
    adapter.send.assert_awaited()

    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", "*")
    admitted = _make_event("admitted")
    admitted.pre_gateway_prepare = prepare

    assert await runner._handle_message(admitted) is None
    assert order == ["prepare", "hook"]
    assert admitted.pre_gateway_prepare is None


@pytest.mark.asyncio
@pytest.mark.parametrize("hook_mode", ["empty", "allow", "exception"])
async def test_consumed_cold_prepare_requires_explicit_rewrite(monkeypatch, hook_mode):
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", "*")
    runner, _adapter = _make_runner(Platform.WHATSAPP)
    dispatch = AsyncMock(return_value="should-not-run")
    runner._handle_message_with_agent = dispatch

    if hook_mode == "empty":
        hook = MagicMock(return_value=[])
    elif hook_mode == "allow":
        hook = MagicMock(return_value=[{"action": "allow"}])
    else:
        hook = MagicMock(side_effect=RuntimeError("hook unavailable"))
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", hook)

    event = _make_event("approval")
    event.pre_gateway_prepare = lambda: {"status": "ready"}

    assert await runner._handle_message(event) is None
    assert event._hermes_pre_gateway_prepare_terminal is True
    dispatch.assert_not_awaited()


@pytest.mark.asyncio
async def test_promoted_consumed_deferred_event_revalidates_on_cold_reentry(monkeypatch):
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", "*")
    runner, _adapter = _make_runner(Platform.WHATSAPP)
    dispatch = AsyncMock(return_value="must-not-run")
    runner._handle_message_with_agent = dispatch
    validate = MagicMock(return_value={"status": "expired", "reason": "lease_expired"})

    event = _make_event("bound confirmation packet")
    event._hermes_pre_gateway_prepare_consumed = True
    event._hermes_pre_gateway_dispatched = True
    event.pre_gateway_consume_validate = validate

    assert await runner._handle_message(event) is None
    validate.assert_called_once_with()
    assert event.pre_gateway_consume_validate is None
    assert event._hermes_pre_gateway_consume_terminal is True
    dispatch.assert_not_awaited()
