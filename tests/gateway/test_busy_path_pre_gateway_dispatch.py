"""Busy-session follow-ups must still run pre_gateway_dispatch.

Live Feishu confirmation failed when an Admin DM was already mid-turn: the
busy path steered/redirected the next user reply without invoking
pre_gateway_dispatch. That skipped runtime-intake recording and admin-reply
matching, so free-form semantic approvals never bound to the waiting Frame.
"""

from __future__ import annotations

import asyncio
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

from gateway.config import Platform, PlatformConfig  # noqa: E402
from gateway.platforms.base import (  # noqa: E402
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    SessionSource,
)
from gateway.run import GatewayRunner  # noqa: E402
from gateway.session import build_session_key  # noqa: E402


@pytest.fixture(autouse=True)
def _isolate_gateway_runtime_status(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))


def _event(text: str = "按这个方案继续执行吧", *, internal: bool = False) -> MessageEvent:
    source = SessionSource(
        platform=Platform.FEISHU,
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


class _FallbackAdapter(BasePlatformAdapter):
    def __init__(self):
        super().__init__(PlatformConfig(enabled=True, token="test"), Platform.FEISHU)

    async def connect(self, *, is_reconnect: bool = False):
        return True

    async def disconnect(self):
        return None

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        return SendResult(success=True, message_id="sent")

    async def get_chat_info(self, chat_id):
        return {"id": chat_id, "type": "dm"}


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


def test_ordinary_admission_reports_queue_cap():
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
    runner._adapter_for_source = MagicMock(return_value=MagicMock(_pending_messages={}))
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
    runner._adapter_for_source = MagicMock(return_value=MagicMock(_pending_messages={}))
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
    assert queue_call.kwargs == {"emergency": True}
    queued = queue_call.args[1]
    assert queued is event
    assert queued.text == "[bound frame] execute approved action"


@pytest.mark.asyncio
async def test_busy_rewrite_uses_single_emergency_slot_at_ordinary_cap(monkeypatch):
    runner = object.__new__(GatewayRunner)
    session_key = "session-key"
    existing = _event("already queued")
    adapter = MagicMock()
    adapter._pending_messages = {session_key: existing}
    state = _queue_state([_event(f"queued-{index}") for index in range(31)])
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner._peek_session_state = MagicMock(return_value=state)
    runner._session_state = MagicMock(return_value=state)
    runner._is_user_authorized = MagicMock(return_value=True)
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
async def test_deferred_prepare_runs_after_busy_admission_before_hook_and_enqueue(monkeypatch):
    runner = object.__new__(GatewayRunner)
    session_key = "session-key"
    adapter = MagicMock()
    adapter._pending_messages = {}
    state = _queue_state()
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner._peek_session_state = MagicMock(return_value=state)
    runner._session_state = MagicMock(return_value=state)
    runner._is_user_authorized = MagicMock(return_value=True)
    runner.session_store = None
    order = []

    def prepare():
        order.append("prepare")
        return {"status": "ready"}

    def hook(_name, **_kwargs):
        order.append("hook")
        return [{"action": "rewrite", "text": "bound confirmation"}]

    monkeypatch.setattr("hermes_cli.lifecycle.invoke_hook", hook)
    event = _event()
    event.pre_gateway_prepare = prepare

    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, event, session_key
    )

    assert handled is True
    assert order == ["prepare", "hook"]
    assert adapter._pending_messages[session_key] is event
    assert event.text == "bound confirmation"
    assert event.pre_gateway_prepare is None


@pytest.mark.asyncio
@pytest.mark.parametrize("hook_result", [[], [{"action": "allow"}]])
async def test_consumed_busy_prepare_requires_explicit_rewrite(monkeypatch, hook_result):
    runner = object.__new__(GatewayRunner)
    adapter = MagicMock(_pending_messages={})
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner._is_user_authorized = MagicMock(return_value=True)
    runner._peek_session_state = MagicMock(return_value=_queue_state())
    runner._queue_or_replace_pending_event = MagicMock(return_value=True)
    runner.session_store = None
    monkeypatch.setattr(
        "hermes_cli.lifecycle.invoke_hook", MagicMock(return_value=hook_result)
    )
    event = _event()
    event.pre_gateway_prepare = lambda: {"status": "ready"}

    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, event, "session-key"
    )

    assert handled is True
    assert event._hermes_pre_gateway_prepare_terminal is True
    runner._queue_or_replace_pending_event.assert_not_called()


@pytest.mark.asyncio
async def test_consumed_busy_prepare_hook_exception_is_terminal(monkeypatch):
    runner = object.__new__(GatewayRunner)
    adapter = MagicMock(_pending_messages={})
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner._is_user_authorized = MagicMock(return_value=True)
    runner._peek_session_state = MagicMock(return_value=_queue_state())
    runner._queue_or_replace_pending_event = MagicMock(return_value=True)
    runner.session_store = None
    monkeypatch.setattr(
        "hermes_cli.lifecycle.invoke_hook",
        MagicMock(side_effect=RuntimeError("hook unavailable")),
    )
    event = _event()
    event.pre_gateway_prepare = lambda: {"status": "ready"}

    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, event, "session-key"
    )

    assert handled is True
    assert event._hermes_pre_gateway_prepare_terminal is True
    runner._queue_or_replace_pending_event.assert_not_called()


@pytest.mark.asyncio
async def test_consumed_busy_prepare_enqueue_exception_is_terminal(monkeypatch):
    runner = object.__new__(GatewayRunner)
    adapter = MagicMock(_pending_messages={})
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner._is_user_authorized = MagicMock(return_value=True)
    runner._peek_session_state = MagicMock(return_value=_queue_state())
    runner._queue_or_replace_pending_event = MagicMock(
        side_effect=RuntimeError("queue unavailable")
    )
    runner.session_store = None
    monkeypatch.setattr(
        "hermes_cli.lifecycle.invoke_hook",
        MagicMock(return_value=[{"action": "rewrite", "text": "bound"}]),
    )
    event = _event()
    event.pre_gateway_prepare = lambda: {"status": "ready"}

    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, event, "session-key"
    )

    assert handled is True
    assert adapter._pending_messages == {}
    assert event.text == "bound"
    assert event._hermes_pre_gateway_prepare_terminal is True


@pytest.mark.asyncio
async def test_prepared_confirmation_never_merges_into_media_head(monkeypatch):
    runner = object.__new__(GatewayRunner)
    session_key = "session-key"
    head = _event("photo head")
    head.message_type = MessageType.PHOTO
    adapter = MagicMock()
    adapter._pending_messages = {session_key: head}
    state = _queue_state()
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner._peek_session_state = MagicMock(return_value=state)
    runner._session_state = MagicMock(return_value=state)
    runner._is_user_authorized = MagicMock(return_value=True)
    runner.session_store = None
    monkeypatch.setattr(
        "hermes_cli.lifecycle.invoke_hook",
        lambda _name, **_kwargs: [{"action": "rewrite", "text": "bound"}],
    )
    event = _event()
    event.pre_gateway_prepare = lambda: {"status": "ready"}

    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, event, session_key
    )

    assert handled is True
    assert adapter._pending_messages[session_key] is head
    assert state.conversation.queued_events == [event]
    assert event.text == "bound"


@pytest.mark.asyncio
async def test_busy_full_never_runs_deferred_prepare_or_ordinary_fallback(monkeypatch):
    adapter = _FallbackAdapter()
    adapter.set_message_handler(AsyncMock(return_value=None))
    runner = object.__new__(GatewayRunner)
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner._is_user_authorized = MagicMock(return_value=True)
    runner.session_store = None
    session_key = build_session_key(_event().source)
    head = _event("existing head")
    adapter._pending_messages[session_key] = head
    state = _queue_state([_event(f"queued-{index}") for index in range(32)])
    runner._peek_session_state = MagicMock(return_value=state)
    runner._session_state = MagicMock(return_value=state)
    adapter.set_busy_session_handler(
        GatewayRunner._handle_active_session_busy_message.__get__(runner, GatewayRunner)
    )
    adapter._active_sessions[session_key] = asyncio.Event()
    adapter._session_tasks[session_key] = asyncio.current_task()
    prepare = MagicMock(return_value={"status": "ready"})
    hook = MagicMock(return_value=[{"action": "rewrite", "text": "claimed"}])
    monkeypatch.setattr("hermes_cli.lifecycle.invoke_hook", hook)
    event = _event("approve at full capacity")
    event.pre_gateway_prepare = prepare

    await adapter.handle_message(event)

    prepare.assert_not_called()
    hook.assert_not_called()
    assert adapter._pending_messages[session_key] is head
    assert len(state.conversation.queued_events) == 32
    assert event.text == "approve at full capacity"


@pytest.mark.asyncio
@pytest.mark.parametrize("adapter", [None, MagicMock(spec=[])])
async def test_missing_busy_adapter_slot_terminally_blocks_deferred_prepare(
    monkeypatch, adapter
):
    runner = object.__new__(GatewayRunner)
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner._is_user_authorized = MagicMock(return_value=True)
    runner.session_store = None
    prepare = MagicMock(return_value={"status": "ready"})
    hook = MagicMock()
    monkeypatch.setattr("hermes_cli.lifecycle.invoke_hook", hook)
    event = _event()
    event.pre_gateway_prepare = prepare

    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, event, "session-key"
    )

    assert handled is True
    prepare.assert_not_called()
    hook.assert_not_called()


@pytest.mark.asyncio
async def test_deferred_prepare_race_is_terminal_before_hook_or_queue(monkeypatch):
    runner = object.__new__(GatewayRunner)
    adapter = MagicMock(_pending_messages={})
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner._is_user_authorized = MagicMock(return_value=True)
    runner._peek_session_state = MagicMock(return_value=_queue_state())
    runner.session_store = None
    hook = MagicMock()
    monkeypatch.setattr("hermes_cli.lifecycle.invoke_hook", hook)
    event = _event()
    event.pre_gateway_prepare = MagicMock(
        return_value={"status": "handled", "reason": "claim_identity_conflict"}
    )

    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, event, "session-key"
    )

    assert handled is True
    hook.assert_not_called()
    assert adapter._pending_messages == {}


@pytest.mark.asyncio
async def test_busy_rewrite_refuses_admission_at_emergency_cap(monkeypatch):
    runner = object.__new__(GatewayRunner)
    session_key = "session-key"
    adapter = MagicMock()
    adapter._pending_messages = {session_key: _event("already queued")}
    state = _queue_state([_event(f"queued-{index}") for index in range(32)])
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner._peek_session_state = MagicMock(return_value=state)
    runner._is_user_authorized = MagicMock(return_value=True)
    runner.session_store = None
    hook = MagicMock(
        return_value=[{"action": "rewrite", "text": "bound confirmation"}]
    )
    monkeypatch.setattr(
        "hermes_cli.lifecycle.invoke_hook",
        hook,
    )

    event = _event()
    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, event, session_key
    )

    assert handled is False
    assert len(state.conversation.queued_events) == 32
    assert event.text == "按这个方案继续执行吧"
    assert not hasattr(event, "_hermes_pre_gateway_dispatched")
    assert not hasattr(event, "_hermes_busy_fallback_preserve_identity")
    hook.assert_not_called()


@pytest.mark.asyncio
async def test_durable_claim_marker_cannot_bypass_emergency_traffic_cap(monkeypatch):
    runner = object.__new__(GatewayRunner)
    session_key = "session-key"
    head = _event("already queued")
    adapter = MagicMock()
    adapter._pending_messages = {session_key: head}
    state = _queue_state([_event(f"queued-{index}") for index in range(32)])
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner._peek_session_state = MagicMock(return_value=state)
    runner._session_state = MagicMock(return_value=state)
    runner._is_user_authorized = MagicMock(return_value=True)
    runner.session_store = None
    hook = MagicMock(
        return_value=[{
            "action": "rewrite",
            "text": "durable bound confirmation",
            "durable_claim": True,
        }]
    )
    monkeypatch.setattr(
        "hermes_cli.lifecycle.invoke_hook",
        hook,
    )

    event = _event("original approval")
    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, event, session_key
    )

    assert handled is False
    assert adapter._pending_messages[session_key] is head
    assert len(state.conversation.queued_events) == 32
    assert event.text == "original approval"
    assert not hasattr(event, "_hermes_pre_gateway_dispatched")
    assert not hasattr(event, "_hermes_durable_claimed_dispatch")
    assert not hasattr(event, "_hermes_busy_fallback_preserve_identity")
    hook.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("adapter", [None, MagicMock(spec=[])])
async def test_busy_rewrite_returns_false_when_internal_admission_is_unavailable(
    monkeypatch, adapter
):
    runner = object.__new__(GatewayRunner)
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner._is_user_authorized = MagicMock(return_value=True)
    runner.session_store = None
    hook = MagicMock(
        return_value=[{"action": "rewrite", "text": "bound confirmation"}]
    )
    monkeypatch.setattr(
        "hermes_cli.lifecycle.invoke_hook",
        hook,
    )

    event = _event()
    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, event, "session-key"
    )

    assert handled is False
    assert event.text == "按这个方案继续执行吧"
    assert not hasattr(event, "_hermes_pre_gateway_dispatched")
    assert not hasattr(event, "_hermes_busy_fallback_preserve_identity")
    hook.assert_not_called()


@pytest.mark.asyncio
async def test_busy_rewrite_returns_false_when_fifo_enqueue_raises(monkeypatch):
    runner = object.__new__(GatewayRunner)
    adapter = MagicMock()
    adapter._pending_messages = {}
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner._enqueue_fifo = MagicMock(side_effect=RuntimeError("queue unavailable"))
    runner._is_user_authorized = MagicMock(return_value=True)
    runner.session_store = None
    monkeypatch.setattr(
        "hermes_cli.lifecycle.invoke_hook",
        lambda _name, **_kwargs: [{"action": "rewrite", "text": "bound confirmation"}],
    )

    event = _event()
    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, event, "session-key"
    )

    assert handled is False
    assert event.text == "bound confirmation"
    assert event._hermes_pre_gateway_dispatched is True
    assert event._hermes_busy_fallback_preserve_identity is True


@pytest.mark.asyncio
async def test_busy_hook_retry_enqueue_exception_drops_without_claim_fallback(monkeypatch):
    runner = object.__new__(GatewayRunner)
    runner._adapter_for_source = MagicMock(return_value=MagicMock(_pending_messages={}))
    runner._queue_or_replace_pending_event = MagicMock(side_effect=RuntimeError("queue unavailable"))
    runner._is_user_authorized = MagicMock(return_value=True)
    runner.session_store = None
    monkeypatch.setattr(
        "hermes_cli.lifecycle.invoke_hook",
        MagicMock(side_effect=RuntimeError("transient hook failure")),
    )

    event = _event()
    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, event, "session-key"
    )

    assert handled is True
    assert not hasattr(event, "_hermes_busy_fallback_preserve_identity")


@pytest.mark.asyncio
async def test_busy_allow_continues_into_existing_drain_path(monkeypatch):
    runner = object.__new__(GatewayRunner)
    runner._is_user_authorized = MagicMock(return_value=True)
    runner._draining = True
    runner._restart_requested = False
    runner._busy_input_mode = "interrupt"
    adapter = MagicMock(_pending_messages={})
    adapter._send_with_retry = AsyncMock(return_value=SendResult(success=True))
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner.session_store = None
    monkeypatch.setattr(
        "hermes_cli.lifecycle.invoke_hook",
        lambda _name, **_kwargs: [{"action": "allow"}],
    )

    event = _event()
    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, event, "session-key"
    )

    assert handled is True
    assert event._hermes_pre_gateway_dispatched is True


@pytest.mark.asyncio
async def test_unauthorized_shared_channel_is_dropped_before_hook(monkeypatch):
    runner = object.__new__(GatewayRunner)
    runner._is_user_authorized = MagicMock(return_value=False)
    runner._queue_or_replace_pending_event = MagicMock()
    hook = MagicMock()
    monkeypatch.setattr("hermes_cli.lifecycle.invoke_hook", hook)
    event = _event()
    event.source.chat_type = "group"

    handled = await GatewayRunner._handle_active_session_busy_message(
        runner, event, "session-key"
    )

    assert handled is True
    hook.assert_not_called()
    runner._queue_or_replace_pending_event.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_mode", ["missing_adapter", "missing_slot", "enqueue_error"]
)
async def test_rewritten_claim_uses_empty_adapter_fallback_with_same_event(
    monkeypatch, failure_mode
):
    adapter = _FallbackAdapter()
    runner = object.__new__(GatewayRunner)
    runner._is_user_authorized = MagicMock(return_value=True)
    runner.session_store = None
    if failure_mode == "missing_adapter":
        runner._adapter_for_source = MagicMock(return_value=None)
    else:
        internal_adapter = (
            MagicMock(spec=[]) if failure_mode == "missing_slot" else MagicMock()
        )
        if failure_mode == "enqueue_error":
            internal_adapter._pending_messages = {}
            runner._enqueue_fifo = MagicMock(side_effect=RuntimeError("queue unavailable"))
        runner._adapter_for_source = MagicMock(return_value=internal_adapter)

    hook = MagicMock(
        return_value=[{"action": "rewrite", "text": "bound continuation"}]
    )
    monkeypatch.setattr("hermes_cli.lifecycle.invoke_hook", hook)
    adapter.set_message_handler(AsyncMock(return_value=None))
    adapter.set_busy_session_handler(
        GatewayRunner._handle_active_session_busy_message.__get__(runner, GatewayRunner)
    )
    event = _event()
    session_key = build_session_key(event.source)
    adapter._active_sessions[session_key] = asyncio.Event()
    adapter._session_tasks[session_key] = asyncio.current_task()

    await adapter.handle_message(event)

    fallback = adapter._pending_messages[session_key]
    assert fallback is event
    if failure_mode == "enqueue_error":
        assert fallback.text == "bound continuation"
        assert fallback._hermes_pre_gateway_dispatched is True
        hook.assert_called_once()
    else:
        assert fallback.text == "按这个方案继续执行吧"
        assert not hasattr(fallback, "_hermes_pre_gateway_dispatched")
        hook.assert_not_called()
    replayed, disposition = GatewayRunner._apply_pre_gateway_dispatch(runner, fallback)
    assert replayed is event
    assert disposition == ("continue" if failure_mode == "enqueue_error" else "queue")


@pytest.mark.asyncio
async def test_claimed_busy_fallback_never_overwrites_earlier_claim():
    adapter = _FallbackAdapter()
    adapter.set_message_handler(AsyncMock(return_value=None))
    adapter.set_busy_session_handler(AsyncMock(return_value=False))
    session_key = build_session_key(_event().source)
    adapter._active_sessions[session_key] = asyncio.Event()
    adapter._session_tasks[session_key] = asyncio.current_task()

    earlier_claim = _event("first claimed confirmation")
    earlier_claim.message_id = "om_claimed_first"
    earlier_claim._hermes_pre_gateway_dispatched = True
    earlier_claim._hermes_busy_fallback_preserve_identity = True
    later_claim = _event("second claimed confirmation")
    later_claim.message_id = "om_claimed_second"
    later_claim._hermes_pre_gateway_dispatched = True
    later_claim._hermes_busy_fallback_preserve_identity = True

    await adapter.handle_message(earlier_claim)
    await adapter.handle_message(later_claim)

    assert adapter._pending_messages[session_key] is earlier_claim
    assert adapter._pending_messages[session_key].message_id == "om_claimed_first"
    assert adapter.get_pending_message(session_key) is earlier_claim
    assert session_key not in adapter._pending_messages
    adapter._message_handler.assert_not_awaited()


@pytest.mark.asyncio
async def test_unclaimed_busy_follow_up_keeps_normal_adapter_queue_path():
    adapter = _FallbackAdapter()
    adapter.set_message_handler(AsyncMock(return_value=None))
    adapter.set_busy_session_handler(AsyncMock(return_value=False))
    event = _event("photo follow-up")
    event.message_type = MessageType.PHOTO
    session_key = build_session_key(event.source)
    adapter._active_sessions[session_key] = asyncio.Event()
    adapter._session_tasks[session_key] = asyncio.current_task()

    await adapter.handle_message(event)

    assert adapter._pending_messages[session_key] is event


@pytest.mark.asyncio
async def test_repeated_hook_exception_flood_stops_at_emergency_cap(monkeypatch):
    runner = object.__new__(GatewayRunner)
    session_key = "session-key"
    adapter = MagicMock()
    adapter._pending_messages = {session_key: _event("already queued")}
    state = _queue_state([_event(f"queued-{index}") for index in range(31)])
    runner._adapter_for_source = MagicMock(return_value=adapter)
    runner._peek_session_state = MagicMock(return_value=state)
    runner._session_state = MagicMock(return_value=state)
    runner._is_user_authorized = MagicMock(return_value=True)
    runner.session_store = None
    hook = MagicMock(side_effect=RuntimeError("persistent hook failure"))
    monkeypatch.setattr("hermes_cli.lifecycle.invoke_hook", hook)

    handled = [
        await GatewayRunner._handle_active_session_busy_message(
            runner, _event(f"flood-{index}"), session_key
        )
        for index in range(20)
    ]

    assert handled == [True] + [False] * 19
    assert len(state.conversation.queued_events) == 32
    assert state.conversation.queued_events[-1].text == "flood-0"
    assert hook.call_count == 1


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
    runner._is_user_authorized = MagicMock(return_value=True)
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
