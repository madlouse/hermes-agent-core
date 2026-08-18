import base64
import importlib
import sys
import types
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult
from gateway.session import SessionSource


_ONE_BY_ONE_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO6L2ioAAAAASUVORK5CYII="
)


class CaptureAdapter(BasePlatformAdapter):
    def __init__(self):
        super().__init__(PlatformConfig(enabled=True, token="***"), Platform.TELEGRAM)
        self.sent = []
        self.typing = []

    async def connect(self) -> bool:
        return True

    async def disconnect(self) -> None:
        return None

    async def send(self, chat_id, content, reply_to=None, metadata=None) -> SendResult:
        self.sent.append(
            {
                "chat_id": chat_id,
                "content": content,
                "reply_to": reply_to,
                "metadata": metadata,
            }
        )
        return SendResult(success=True, message_id="sent-1")

    async def send_typing(self, chat_id, metadata=None) -> None:
        self.typing.append({"chat_id": chat_id, "metadata": metadata})

    async def stop_typing(self, chat_id) -> None:
        return None

    async def get_chat_info(self, chat_id: str):
        return {"id": chat_id}


class CaptureQueuedNativeImageAgent:
    calls = []

    def __init__(self, **kwargs):
        self.tools = []
        self.tool_progress_callback = kwargs.get("tool_progress_callback")

    def run_conversation(self, message, conversation_history=None, task_id=None):
        type(self).calls.append(message)
        return {
            "final_response": f"done-{len(type(self).calls)}",
            "messages": [],
            "api_calls": 1,
        }


def _make_runner(adapter):
    gateway_run = importlib.import_module("gateway.run")
    runner = object.__new__(gateway_run.GatewayRunner)
    runner.adapters = {adapter.platform: adapter}
    runner._voice_mode = {}
    runner._prefill_messages = []
    runner._ephemeral_system_prompt = ""
    runner._reasoning_config = None
    runner._provider_routing = {}
    runner._fallback_model = None
    runner._session_db = None
    runner._running_agents = {}
    runner._session_run_generation = {}
    runner.hooks = SimpleNamespace(loaded_hooks=False)
    runner.config = SimpleNamespace(
        thread_sessions_per_user=False,
        group_sessions_per_user=False,
        stt_enabled=False,
    )
    runner._model = "openai/gpt-4.1-mini"
    runner._base_url = None
    runner._decide_image_input_mode = lambda **_kw: "native"
    return runner


@pytest.mark.asyncio
async def test_queued_followup_uses_pending_event_session_key_for_native_images(monkeypatch, tmp_path):
    CaptureQueuedNativeImageAgent.calls = []

    fake_dotenv = types.ModuleType("dotenv")
    fake_dotenv.load_dotenv = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "dotenv", fake_dotenv)

    fake_run_agent = types.ModuleType("run_agent")
    fake_run_agent.AIAgent = CaptureQueuedNativeImageAgent
    monkeypatch.setitem(sys.modules, "run_agent", fake_run_agent)

    gateway_run = importlib.import_module("gateway.run")
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run, "_resolve_runtime_agent_kwargs", lambda: {"api_key": "***"})

    adapter = CaptureAdapter()
    runner = _make_runner(adapter)

    image_path = tmp_path / "queued-image.png"
    image_path.write_bytes(_ONE_BY_ONE_PNG)

    source = SessionSource(
        platform=Platform.TELEGRAM,
        chat_id="-1001",
        chat_type="group",
    )
    pending_source = SessionSource(
        platform=Platform.TELEGRAM,
        chat_id="-1001",
        chat_type="group",
        thread_id="17585",
    )

    adapter._pending_messages["agent:main:telegram:group:-1001"] = MessageEvent(
        text="describe this",
        message_type=MessageType.PHOTO,
        source=pending_source,
        media_urls=[str(image_path)],
        media_types=["image/png"],
        message_id="queued-1",
    )

    result = await runner._run_agent(
        message="hello",
        context_prompt="",
        history=[],
        source=source,
        session_id="sess-native-image-followup",
        session_key="agent:main:telegram:group:-1001",
    )

    assert result["final_response"] == "done-2"
    assert len(CaptureQueuedNativeImageAgent.calls) == 2
    queued_message = CaptureQueuedNativeImageAgent.calls[1]
    assert isinstance(queued_message, list)
    assert queued_message[0]["type"] == "text"
    assert queued_message[0]["text"].startswith("describe this")
    assert any(part.get("type") == "image_url" for part in queued_message)


@pytest.mark.asyncio
async def test_queued_deferred_event_revalidates_lease_after_awaits_before_model(monkeypatch, tmp_path):
    CaptureQueuedNativeImageAgent.calls = []

    fake_dotenv = types.ModuleType("dotenv")
    fake_dotenv.load_dotenv = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "dotenv", fake_dotenv)

    fake_run_agent = types.ModuleType("run_agent")
    fake_run_agent.AIAgent = CaptureQueuedNativeImageAgent
    monkeypatch.setitem(sys.modules, "run_agent", fake_run_agent)

    gateway_run = importlib.import_module("gateway.run")
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run, "_resolve_runtime_agent_kwargs", lambda: {"api_key": "***"})

    adapter = CaptureAdapter()
    runner = _make_runner(adapter)
    source = SessionSource(platform=Platform.TELEGRAM, chat_id="-1001", chat_type="group")
    pending = MessageEvent(
        text="bound confirmation packet",
        message_type=MessageType.VOICE,
        source=source,
        media_urls=[str(tmp_path / "untrusted-voice.ogg")],
        media_types=["audio/ogg"],
        message_id="queued-confirmation-1",
    )
    pending._hermes_pre_gateway_prepare_consumed = True
    validate = MagicMock(side_effect=[
        {"status": "ok"},
        {"status": "expired", "reason": "lease_expired"},
    ])
    pending.pre_gateway_consume_validate = validate
    runner._is_event_user_authorized = MagicMock(return_value=True)
    runner._transcribe_and_echo_pending_voice = AsyncMock()
    runner._prepare_profile_scoped_inbound_message_text = AsyncMock()
    adapter._pending_messages["agent:main:telegram:group:-1001"] = pending

    result = await runner._run_agent(
        message="first turn",
        context_prompt="",
        history=[],
        source=source,
        session_id="sess-expired-confirmation",
        session_key="agent:main:telegram:group:-1001",
    )

    assert result["final_response"] in {"", "done-1"}
    assert CaptureQueuedNativeImageAgent.calls == ["first turn"]
    assert validate.call_count == 2
    assert pending.pre_gateway_consume_validate is None
    assert pending._hermes_pre_gateway_consume_terminal is True
    runner._transcribe_and_echo_pending_voice.assert_not_awaited()
    runner._prepare_profile_scoped_inbound_message_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_rejected_fifo_head_validates_and_consumes_successor_without_new_traffic(
    monkeypatch, tmp_path
):
    CaptureQueuedNativeImageAgent.calls = []
    fake_dotenv = types.ModuleType("dotenv")
    fake_dotenv.load_dotenv = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "dotenv", fake_dotenv)
    fake_run_agent = types.ModuleType("run_agent")
    fake_run_agent.AIAgent = CaptureQueuedNativeImageAgent
    monkeypatch.setitem(sys.modules, "run_agent", fake_run_agent)
    gateway_run = importlib.import_module("gateway.run")
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run, "_resolve_runtime_agent_kwargs", lambda: {"api_key": "***"})

    adapter = CaptureAdapter()
    runner = _make_runner(adapter)
    source = SessionSource(platform=Platform.TELEGRAM, chat_id="-1001", chat_type="group")
    session_key = "agent:main:telegram:group:-1001"
    rejected = MessageEvent(
        text="expired packet", message_type=MessageType.TEXT, source=source,
        message_id="queued-expired-head",
    )
    rejected._hermes_pre_gateway_prepare_consumed = True
    rejected_validate = MagicMock(return_value={"status": "expired"})
    rejected.pre_gateway_consume_validate = rejected_validate
    successor = MessageEvent(
        text="valid successor packet", message_type=MessageType.TEXT, source=source,
        message_id="queued-valid-successor",
    )
    successor._hermes_pre_gateway_prepare_consumed = True
    successor_validate = MagicMock(return_value={"status": "ok"})
    successor.pre_gateway_consume_validate = successor_validate
    runner._is_event_user_authorized = MagicMock(return_value=True)
    adapter._pending_messages[session_key] = rejected
    runner._session_state(session_key).conversation.queued_events.append(successor)

    result = await runner._run_agent(
        message="first turn", context_prompt="", history=[], source=source,
        session_id="sess-successor", session_key=session_key,
    )

    assert result["final_response"] == "done-2"
    assert CaptureQueuedNativeImageAgent.calls == ["first turn", "valid successor packet"]
    rejected_validate.assert_called_once_with()
    assert successor_validate.call_count == 2
    assert session_key not in adapter._pending_messages


@pytest.mark.asyncio
async def test_recursion_limit_requeues_deferred_event_before_one_shot_validation(
    monkeypatch, tmp_path
):
    CaptureQueuedNativeImageAgent.calls = []
    fake_dotenv = types.ModuleType("dotenv")
    fake_dotenv.load_dotenv = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "dotenv", fake_dotenv)
    fake_run_agent = types.ModuleType("run_agent")
    fake_run_agent.AIAgent = CaptureQueuedNativeImageAgent
    monkeypatch.setitem(sys.modules, "run_agent", fake_run_agent)

    gateway_run = importlib.import_module("gateway.run")
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(gateway_run, "_resolve_runtime_agent_kwargs", lambda: {"api_key": "***"})

    adapter = CaptureAdapter()
    runner = _make_runner(adapter)
    source = SessionSource(platform=Platform.TELEGRAM, chat_id="-1001", chat_type="group")
    session_key = "agent:main:telegram:group:-1001"
    pending = MessageEvent(
        text="bound confirmation packet",
        message_type=MessageType.TEXT,
        source=source,
        message_id="queued-confirmation-depth",
    )
    pending._hermes_pre_gateway_prepare_consumed = True
    validate = MagicMock(return_value={"status": "ok"})
    pending.pre_gateway_consume_validate = validate
    adapter._pending_messages[session_key] = pending
    successor = MessageEvent(
        text="later successor", message_type=MessageType.TEXT, source=source,
        message_id="queued-confirmation-after-depth",
    )
    runner._session_state(session_key).conversation.queued_events.append(successor)

    result = await runner._run_agent(
        message="first turn",
        context_prompt="",
        history=[],
        source=source,
        session_id="sess-depth-confirmation",
        session_key=session_key,
        _interrupt_depth=runner._MAX_INTERRUPT_DEPTH,
    )

    assert result["final_response"] == "done-1"
    assert CaptureQueuedNativeImageAgent.calls == ["first turn"]
    validate.assert_not_called()
    assert pending.pre_gateway_consume_validate is validate
    assert adapter._pending_messages[session_key] is pending
    assert runner._session_state(session_key).conversation.queued_events == [successor]


def test_consumed_queued_event_without_validator_fails_closed():
    gateway_run = importlib.import_module("gateway.run")
    runner = object.__new__(gateway_run.GatewayRunner)
    event = MessageEvent(
        text="unverified packet",
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.TELEGRAM,
            chat_id="-1001",
            chat_type="group",
        ),
        message_id="queued-confirmation-missing-validator",
    )
    event._hermes_pre_gateway_prepare_consumed = True

    assert runner._revalidate_queued_deferred_event(event) is False
    assert event._hermes_pre_gateway_consume_terminal is True


def test_consumed_queued_event_preflight_retains_validator_until_final_consume():
    gateway_run = importlib.import_module("gateway.run")
    runner = object.__new__(gateway_run.GatewayRunner)
    event = MessageEvent(
        text="verified packet",
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.TELEGRAM,
            chat_id="-1001",
            chat_type="group",
        ),
        message_id="queued-confirmation-valid",
    )
    event._hermes_pre_gateway_prepare_consumed = True
    validate = MagicMock(return_value={"status": "ok"})
    event.pre_gateway_consume_validate = validate
    runner._is_event_user_authorized = MagicMock(return_value=True)

    assert runner._revalidate_queued_deferred_event(event) is True
    validate.assert_called_once_with()
    assert event.pre_gateway_consume_validate is validate
    assert not bool(getattr(event, "_hermes_pre_gateway_consume_revalidated", False))

    assert runner._revalidate_queued_deferred_event(event, consume=True) is True
    assert validate.call_count == 2
    assert event.pre_gateway_consume_validate is None
    assert event._hermes_pre_gateway_consume_revalidated is True


@pytest.mark.parametrize("mutation", ["replace_source", "mutate_frozen_identity"])
def test_consumed_queued_event_rejects_authorization_identity_drift(mutation):
    gateway_run = importlib.import_module("gateway.run")
    runner = object.__new__(gateway_run.GatewayRunner)
    adapter = object()
    runner._registered_transport_adapter = MagicMock(return_value=adapter)
    runner._adapter_profile_for_source = MagicMock(return_value="atlas")
    runner._is_user_authorized = MagicMock(return_value=True)
    event = MessageEvent(
        text="verified packet",
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.TELEGRAM,
            chat_id="-1001",
            user_id="operator-1",
            chat_type="group",
        ),
        message_id="queued-confirmation-auth-drift",
    )
    event._hermes_pre_gateway_prepare_consumed = True
    validate = MagicMock(return_value={"status": "ok"})
    event.pre_gateway_consume_validate = validate

    assert runner._is_event_user_authorized(event) is True
    if mutation == "replace_source":
        event.source = SessionSource(
            platform=Platform.TELEGRAM,
            chat_id="-1001",
            user_id="operator-2",
            chat_type="group",
        )
    else:
        vars(event.source)["chat_id"] = "-2002"

    assert runner._revalidate_queued_deferred_event(event, consume=True) is False
    validate.assert_not_called()
    assert event.pre_gateway_consume_validate is None
    assert event._hermes_pre_gateway_consume_terminal is True


@pytest.mark.asyncio
async def test_deferred_event_is_rejected_before_session_or_media_preprocessing():
    gateway_run = importlib.import_module("gateway.run")
    runner = object.__new__(gateway_run.GatewayRunner)
    runner._revalidate_queued_deferred_event = MagicMock(return_value=False)
    runner._async_session_store = MagicMock()
    runner._prepare_profile_scoped_inbound_message_text = MagicMock()
    source = SessionSource(
        platform=Platform.TELEGRAM,
        chat_id="-1001",
        chat_type="group",
        user_id="operator-1",
    )
    event = MessageEvent(
        text="untrusted deferred packet",
        message_type=MessageType.TEXT,
        source=source,
        message_id="deferred-preprocess-reject",
    )
    event._hermes_pre_gateway_prepare_consumed = True
    event.pre_gateway_consume_validate = MagicMock(return_value=False)

    result = await runner._handle_message_with_agent(
        event, source, "agent:main:telegram:group:-1001", 1
    )

    assert result is None
    runner._revalidate_queued_deferred_event.assert_called_once_with(event)
    runner._async_session_store.get_or_create_session.assert_not_called()
    runner._prepare_profile_scoped_inbound_message_text.assert_not_called()


@pytest.mark.asyncio
async def test_deferred_packet_log_preview_is_redacted_before_final_consume(monkeypatch):
    gateway_run = importlib.import_module("gateway.run")
    runner = object.__new__(gateway_run.GatewayRunner)
    runner._revalidate_queued_deferred_event = MagicMock(return_value=True)
    runner._recover_telegram_topic_thread_id = MagicMock(
        side_effect=RuntimeError("stop after preview")
    )
    log_info = MagicMock()
    monkeypatch.setattr(gateway_run.logger, "info", log_info)
    source = SessionSource(
        platform=Platform.TELEGRAM,
        chat_id="-1001",
        chat_type="group",
        user_id="operator-1",
    )
    event = MessageEvent(
        text="secret bound confirmation packet",
        message_type=MessageType.TEXT,
        source=source,
        message_id="deferred-log-redaction",
    )
    event._hermes_pre_gateway_prepare_consumed = True

    with pytest.raises(RuntimeError, match="stop after preview"):
        await runner._handle_message_with_agent(
            event, source, "agent:main:telegram:group:-1001", 1
        )

    preview = log_info.call_args.args[4]
    assert preview == "[deferred confirmation pending final validation]"
    assert "secret bound" not in repr(log_info.call_args)
