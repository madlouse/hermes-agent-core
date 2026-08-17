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


def _queue_state(events=None):
    return MagicMock(conversation=MagicMock(queued_events=list(events or [])))


def test_fifo_admission_reports_unavailable_adapter_or_pending_slot():
    runner = object.__new__(GatewayRunner)
    event = _event()

    assert GatewayRunner._enqueue_fifo(runner, "session-key", event, None) is False
    assert (
        GatewayRunner._enqueue_fifo(
            runner, "session-key", event, MagicMock(spec=[])
        )
        is False
    )


def test_non_forced_admission_reports_queue_cap():
    runner = object.__new__(GatewayRunner)
    session_key = "session-key"
    adapter = MagicMock()
    adapter._pending_messages = {session_key: _event("already queued")}
    state = _queue_state([_event(f"queued-{index}") for index in range(31)])
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner._peek_session_state = MagicMock(return_value=state)

    admitted = GatewayRunner._queue_or_replace_pending_event(
        runner, session_key, _event("over cap")
    )

    assert admitted is False
    assert len(state.conversation.queued_events) == 31


def test_photo_merge_reports_success(monkeypatch):
    runner = object.__new__(GatewayRunner)
    session_key = "session-key"
    existing = _event("photo")
    existing.message_type = MessageType.PHOTO
    adapter = MagicMock()
    adapter._pending_messages = {session_key: existing}
    runner._adapter_for_source = MagicMock(return_value=adapter)
    merge = MagicMock()
    monkeypatch.setattr("gateway.run.merge_pending_message_event", merge)
    incoming = _event("caption")

    admitted = GatewayRunner._queue_or_replace_pending_event(
        runner, session_key, incoming
    )

    assert admitted is True
    merge.assert_called_once_with(
        adapter._pending_messages,
        session_key,
        incoming,
        merge_text=True,
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
    runner._queue_or_replace_pending_event = MagicMock(return_value=True)
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
    runner._queue_or_replace_pending_event = MagicMock(return_value=True)
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
    queue_call = runner._queue_or_replace_pending_event.call_args
    assert queue_call.args[0] == "session-key"
    assert queue_call.kwargs == {"force": True}
    queued = queue_call.args[1]
    assert queued.text == "[bound frame] execute approved action"


@pytest.mark.asyncio
async def test_busy_rewrite_force_admission_bypasses_pending_cap(monkeypatch):
    runner = object.__new__(GatewayRunner)
    session_key = "session-key"
    existing = _event("already queued")
    adapter = MagicMock()
    adapter._pending_messages = {session_key: existing}
    state = _queue_state([_event(f"queued-{index}") for index in range(31)])
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner._peek_session_state = MagicMock(return_value=state)
    runner._session_state = MagicMock(return_value=state)
    runner.session_store = None

    monkeypatch.setattr(
        "hermes_cli.lifecycle.invoke_hook",
        lambda _name, **_kwargs: [{"action": "rewrite", "text": "bound confirmation"}],
    )

    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, _event(), session_key
    )

    assert handled is True
    assert adapter._pending_messages[session_key] is existing
    assert len(state.conversation.queued_events) == 32
    assert state.conversation.queued_events[-1].text == "bound confirmation"


@pytest.mark.asyncio
@pytest.mark.parametrize("adapter", [None, MagicMock(spec=[])])
async def test_busy_rewrite_returns_false_when_internal_admission_is_unavailable(
    monkeypatch, adapter
):
    runner = object.__new__(GatewayRunner)
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner.session_store = None
    monkeypatch.setattr(
        "hermes_cli.lifecycle.invoke_hook",
        lambda _name, **_kwargs: [{"action": "rewrite", "text": "bound confirmation"}],
    )

    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, _event(), "session-key"
    )

    assert handled is False


@pytest.mark.asyncio
async def test_busy_rewrite_returns_false_when_fifo_enqueue_raises(monkeypatch):
    runner = object.__new__(GatewayRunner)
    adapter = MagicMock()
    adapter._pending_messages = {}
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner._enqueue_fifo = MagicMock(side_effect=RuntimeError("queue unavailable"))
    runner.session_store = None
    monkeypatch.setattr(
        "hermes_cli.lifecycle.invoke_hook",
        lambda _name, **_kwargs: [{"action": "rewrite", "text": "bound confirmation"}],
    )

    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, _event(), "session-key"
    )

    assert handled is False


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

    assert (first, second) == ("retry", "continue")
    assert second_event is event
    assert calls == 2


@pytest.mark.asyncio
async def test_busy_hook_exception_queues_unmarked_event_for_cold_retry(monkeypatch):
    runner = object.__new__(GatewayRunner)
    session_key = "session-key"
    adapter = MagicMock()
    adapter._pending_messages = {}
    state = _queue_state()
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner._peek_session_state = MagicMock(return_value=state)
    runner._session_state = MagicMock(return_value=state)
    runner.session_store = None
    calls = 0

    def fake_invoke(_name, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("transient hook failure")
        return [{"action": "rewrite", "text": "bound after retry"}]

    monkeypatch.setattr("hermes_cli.lifecycle.invoke_hook", fake_invoke)
    event = _event()

    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, event, session_key
    )

    assert handled is True
    queued = adapter._pending_messages.pop(session_key)
    assert queued is event
    assert not hasattr(queued, "_hermes_pre_gateway_dispatched")

    replayed, disposition = GatewayRunner._apply_pre_gateway_dispatch(runner, queued)
    assert disposition == "queue"
    assert replayed.text == "bound after retry"
    assert replayed._hermes_pre_gateway_dispatched is True
    assert calls == 2
