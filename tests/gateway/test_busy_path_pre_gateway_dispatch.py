"""Busy-session follow-ups must still run pre_gateway_dispatch.

Live Feishu confirmation failed when an Admin DM was already mid-turn: the
busy path steered/redirected the next user reply without invoking
pre_gateway_dispatch. That skipped runtime-intake recording and admin-reply
matching, so free-form semantic approvals never bound to the waiting Frame.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import AsyncMock, MagicMock

import pytest

# Minimal telegram stubs so gateway imports cleanly (mirrors sibling tests).
_tg = types.ModuleType("telegram")
_tg.constants = types.ModuleType("telegram.constants")
_ct = MagicMock()
_ct.SUPERGROUP = "supergroup"
_ct.GROUP = "group"
_ct.PRIVATE = "private"
_tg.constants.ChatType = _ct
sys.modules.setdefault("telegram", _tg)
sys.modules.setdefault("telegram.constants", _tg.constants)
sys.modules.setdefault("telegram.ext", types.ModuleType("telegram.ext"))

from gateway.platforms.base import MessageEvent, MessageType, SessionSource  # noqa: E402
from gateway.run import GatewayRunner  # noqa: E402


def _event(text: str = "按这个方案继续执行吧", *, internal: bool = False) -> MessageEvent:
    source = SessionSource(
        platform=MagicMock(value="feishu"),
        chat_id="oc_admin",
        chat_type="dm",
        user_id="ou_user",
        user_name="tester",
    )
    return MessageEvent(
        text=text,
        message_type=MessageType.TEXT,
        source=source,
        message_id="om_semantic_1",
        media_urls=[],
        media_types=[],
        internal=internal,
    )


@pytest.mark.asyncio
async def test_busy_path_runs_pre_gateway_and_skips_steer_on_plugin_skip(monkeypatch):
    runner = object.__new__(GatewayRunner)
    runner._is_user_authorized = MagicMock(return_value=True)
    runner._draining = False
    runner._adapter_for_source = MagicMock(return_value=MagicMock())
    runner._busy_input_mode = "interrupt"
    runner._busy_text_mode = "interrupt"
    redirect = MagicMock(return_value=True)
    runner._peek_session_state = MagicMock(
        return_value=MagicMock(turn=MagicMock(agent=MagicMock(redirect=redirect)))
    )
    runner._agent_has_active_subagents = MagicMock(return_value=False)
    runner._session_has_compression_in_flight = AsyncMock(return_value=False)
    runner._queue_or_replace_pending_event = MagicMock()
    runner.session_store = None

    def fake_invoke(name, **kwargs):
        assert name == "pre_gateway_dispatch"
        assert kwargs["event"].message_id == "om_semantic_1"
        return [{"action": "skip", "reason": "hck_admin_reply_matcher"}]

    monkeypatch.setattr("hermes_cli.lifecycle.invoke_hook", fake_invoke)

    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, _event(), "session-key"
    )

    assert handled is True
    runner._queue_or_replace_pending_event.assert_not_called()
    redirect.assert_not_called()


@pytest.mark.asyncio
async def test_busy_path_queues_rewritten_confirmation_instead_of_steer(monkeypatch):
    runner = object.__new__(GatewayRunner)
    runner._is_user_authorized = MagicMock(return_value=True)
    runner._draining = False
    runner._adapter_for_source = MagicMock(return_value=MagicMock())
    runner._busy_input_mode = "steer"
    runner._busy_text_mode = "interrupt"
    steered: list[str] = []

    class Agent:
        def steer(self, text):
            steered.append(text)
            return True

    runner._peek_session_state = MagicMock(
        return_value=MagicMock(turn=MagicMock(agent=Agent(), busy_ack_ts=0))
    )
    runner._agent_has_active_subagents = MagicMock(return_value=False)
    runner._session_has_compression_in_flight = AsyncMock(return_value=False)
    runner._queue_or_replace_pending_event = MagicMock()
    runner.session_store = None

    def fake_invoke(name, **kwargs):
        assert name == "pre_gateway_dispatch"
        return [{"action": "rewrite", "text": "[bound frame] execute approved action"}]

    monkeypatch.setattr("hermes_cli.lifecycle.invoke_hook", fake_invoke)

    event = _event("按这个方案继续执行吧")
    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, event, "session-key"
    )
    assert handled is True
    assert steered == []
    runner._queue_or_replace_pending_event.assert_called_once()
    queued = runner._queue_or_replace_pending_event.call_args.args[1]
    assert queued.text == "[bound frame] execute approved action"


def test_busy_path_internal_events_skip_pre_gateway(monkeypatch):
    runner = object.__new__(GatewayRunner)
    called: list[str] = []

    def fake_invoke(name, **kwargs):
        called.append(name)
        return []

    monkeypatch.setattr("hermes_cli.lifecycle.invoke_hook", fake_invoke)
    event = _event(internal=True)
    out_event, disposition = GatewayRunner._apply_pre_gateway_dispatch(runner, event)
    assert disposition == "continue"
    assert called == []
    assert out_event is event


def test_rewritten_busy_event_does_not_run_hook_again_on_cold_replay(monkeypatch):
    runner = object.__new__(GatewayRunner)
    calls: list[str] = []

    def fake_invoke(name, **kwargs):
        calls.append(kwargs["event"].text)
        return [{"action": "rewrite", "text": "[bound frame] execute once"}]

    monkeypatch.setattr("hermes_cli.lifecycle.invoke_hook", fake_invoke)
    rewritten, first = GatewayRunner._apply_pre_gateway_dispatch(runner, _event())
    replayed, second = GatewayRunner._apply_pre_gateway_dispatch(runner, rewritten)

    assert first == "queue"
    assert second == "continue"
    assert replayed.text == "[bound frame] execute once"
    assert calls == ["按这个方案继续执行吧"]


def test_failed_busy_hook_remains_retryable_on_cold_path(monkeypatch):
    runner = object.__new__(GatewayRunner)
    calls = 0

    def fake_invoke(_name, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("transient hook failure")
        return []

    monkeypatch.setattr("hermes_cli.lifecycle.invoke_hook", fake_invoke)
    event = _event()
    first_event, first = GatewayRunner._apply_pre_gateway_dispatch(runner, event)
    second_event, second = GatewayRunner._apply_pre_gateway_dispatch(runner, first_event)

    assert (first, second) == ("continue", "continue")
    assert second_event is event
    assert calls == 2
