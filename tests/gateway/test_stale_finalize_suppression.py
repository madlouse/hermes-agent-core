"""Regression coverage for #71643 — stale streamed finalize suppression.

A *successful* Telegram finalize edit can carry only the last streamed
preview snapshot: deltas generated between the last preview edit and stream
completion never reach any Bot API call, yet ``final_response_sent`` /
``final_content_delivered`` are set from the call's success and suppress the
gateway's normal final send. The missing tail is then lost with no retry.

These tests exercise the real gateway boundary (``GatewayRunner._run_agent``
with a live ``GatewayStreamConsumer``), per the review guidance on #71643:

1. fake agent emits a visible prefix through ``stream_delta_callback``;
2. the consumer successfully finalizes that prefix;
3. the agent returns a longer ``final_response`` containing a missing tail;
4. the result must NOT silently suppress — the complete final response must
   reach the platform (reconciliation edit or normal final send);
5. control: when the streamed text exactly equals the final text, the
   suppression still occurs (no duplicate delivery).

Plus unit coverage for ``GatewayStreamConsumer.delivered_final_matches``.
"""

import importlib
import sys
import types
from types import SimpleNamespace

import pytest

from gateway.config import Platform, PlatformConfig, StreamingConfig
from gateway.hooks import HookRegistry
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult
from gateway.session import SessionSource
from gateway.stream_consumer import GatewayStreamConsumer, StreamConsumerConfig


# ---------------------------------------------------------------------------
# Boundary-test fakes
# ---------------------------------------------------------------------------


class FinalizeCaptureAdapter(BasePlatformAdapter):
    """Adapter that records every send/edit with its finalize flag."""

    def __init__(self, platform=Platform.TELEGRAM):
        super().__init__(
            PlatformConfig(enabled=True, token="***", typing_indicator=False),
            platform,
        )
        self.sent = []
        self.edits = []
        self._next_id = 0

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        return True

    async def disconnect(self) -> None:
        return None

    def _mint_id(self) -> str:
        self._next_id += 1
        return f"m-{self._next_id}"

    async def send(self, chat_id, content, reply_to=None, metadata=None) -> SendResult:
        self.sent.append({"chat_id": chat_id, "content": content, "metadata": metadata})
        return SendResult(success=True, message_id=self._mint_id())

    async def edit_message(
        self, chat_id, message_id, content, *, finalize: bool = False, metadata=None
    ) -> SendResult:
        self.edits.append(
            {
                "chat_id": chat_id,
                "message_id": message_id,
                "content": content,
                "finalize": finalize,
            }
        )
        return SendResult(success=True, message_id=message_id)

    async def send_typing(self, chat_id, metadata=None) -> None:
        return None

    async def stop_typing(self, chat_id) -> None:
        return None

    async def get_chat_info(self, chat_id: str):
        return {"id": chat_id}


STREAMED_PREFIX = "The photo shows a dog on a beach"
MISSING_TAIL = " with a red frisbee in its mouth, mid-leap over the surf."
FULL_RESPONSE = STREAMED_PREFIX + MISSING_TAIL


class StalePrefixAgent:
    """Streams only a prefix; the completed response carries a longer tail.

    Models the #71643 incident shape: the tail generated between the last
    preview edit and stream completion never reaches the stream callback, so
    the consumer's successful finalize edit carries stale preview text while
    ``final_response`` holds the complete answer.
    """

    def __init__(self, **kwargs):
        self.stream_delta_callback = kwargs.get("stream_delta_callback")
        self.tools = []

    def run_conversation(self, message, conversation_history=None, task_id=None):
        if self.stream_delta_callback:
            self.stream_delta_callback(STREAMED_PREFIX)
        return {
            "final_response": FULL_RESPONSE,
            "response_previewed": False,
            "messages": [],
            "api_calls": 1,
        }


class CompleteStreamAgent:
    """Control: the streamed text exactly equals the final response."""

    def __init__(self, **kwargs):
        self.stream_delta_callback = kwargs.get("stream_delta_callback")
        self.tools = []

    def run_conversation(self, message, conversation_history=None, task_id=None):
        if self.stream_delta_callback:
            self.stream_delta_callback(FULL_RESPONSE)
        return {
            "final_response": FULL_RESPONSE,
            "response_previewed": False,
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
    runner.session_store = SimpleNamespace(_entries={}, _save=lambda: None)
    hooks = HookRegistry()

    def screening(_event_type, context):
        return {
            "decision": "allow",
            "content": context["content"],
            "reason": "test_complete_response_screened",
        }

    hooks._handlers["outbound:before_send"] = [screening]
    hooks._handler_owners[id(screening)] = "test-output-screen"
    hooks._handler_capabilities[id(screening)] = frozenset({"output-screening"})
    runner.hooks = hooks
    runner.config = SimpleNamespace(
        thread_sessions_per_user=False,
        group_sessions_per_user=False,
        stt_enabled=False,
        streaming=StreamingConfig.from_dict(
            {"enabled": True, "edit_interval": 0.01, "buffer_threshold": 1}
        ),
    )
    return runner


async def _run_streaming_turn(monkeypatch, tmp_path, agent_cls, session_id):
    import yaml

    (tmp_path / "config.yaml").write_text(
        yaml.dump(
            {
                "display": {"tool_progress": "off", "interim_assistant_messages": False},
                "streaming": {
                    "enabled": True,
                    "edit_interval": 0.01,
                    "buffer_threshold": 1,
                },
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "channel_policy.toml").write_text(
        "operator_enforce_enabled = false\n",
        encoding="utf-8",
    )

    fake_dotenv = types.ModuleType("dotenv")
    fake_dotenv.load_dotenv = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "dotenv", fake_dotenv)

    fake_run_agent = types.ModuleType("run_agent")
    fake_run_agent.AIAgent = agent_cls
    monkeypatch.setitem(sys.modules, "run_agent", fake_run_agent)

    adapter = FinalizeCaptureAdapter()
    runner = _make_runner(adapter)
    runner._resolve_profile_home_for_source = lambda _source: tmp_path
    gateway_run = importlib.import_module("gateway.run")
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    monkeypatch.setattr(
        gateway_run, "_resolve_runtime_agent_kwargs", lambda: {"api_key": "***"}
    )

    source = SessionSource(
        platform=Platform.TELEGRAM,
        chat_id="-1001",
        chat_type="group",
    )
    session_key = "agent:main:telegram:group:-1001"
    results = []

    async def handle_message(_event):
        result = await runner._run_agent(
            message="describe this photo",
            context_prompt="",
            history=[],
            source=source,
            session_id=session_id,
            session_key=session_key,
        )
        results.append(result)
        if result.get("already_sent") and not result.get("failed"):
            return None
        return result["final_response"]

    adapter._message_handler = handle_message
    monkeypatch.setattr(gateway_run, "_gateway_runner_ref", lambda: runner)
    event = MessageEvent(
        text="describe this photo",
        message_type=MessageType.TEXT,
        source=source,
        message_id="inbound-71643",
    )
    await adapter._process_message_background(event, session_key)
    assert len(results) == 1
    return adapter, results[0]


# ---------------------------------------------------------------------------
# Gateway-boundary regression (#71643)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stale_finalize_does_not_suppress_complete_response(
    monkeypatch, tmp_path
):
    """Mandatory screening must buffer stale preview text and send the
    complete response through the ordinary final-delivery boundary."""
    adapter, result = await _run_streaming_turn(
        monkeypatch, tmp_path, StalePrefixAgent, "sess-71643-stale-finalize"
    )

    assert result["final_response"] == FULL_RESPONSE
    # Group A requires model deltas to stay buffered until the complete reply
    # is screened. The complete response must then reach the platform exactly
    # once through the ordinary final-send path.
    all_payloads = [c["content"] for c in adapter.sent] + [
        e["content"] for e in adapter.edits
    ]
    assert any(FULL_RESPONSE in payload for payload in all_payloads), (
        f"complete response never reached the platform; payloads: {all_payloads!r}"
    )
    assert adapter.edits == []
    assert [call["content"] for call in adapter.sent] == [FULL_RESPONSE]
    assert result.get("already_sent") is not True


@pytest.mark.asyncio
async def test_equal_text_control_still_suppresses_duplicate_send(
    monkeypatch, tmp_path
):
    """Equal delta/final text still produces one screened delivery only."""
    adapter, result = await _run_streaming_turn(
        monkeypatch, tmp_path, CompleteStreamAgent, "sess-71643-control-equal"
    )

    assert result["final_response"] == FULL_RESPONSE
    assert result.get("already_sent") is not True
    assert adapter.edits == []
    full_sends = [c for c in adapter.sent if FULL_RESPONSE in c["content"]]
    assert len(full_sends) == 1, f"duplicate or missing final delivery: {full_sends!r}"


# ---------------------------------------------------------------------------
# Consumer unit coverage: delivered_final_matches tri-state
# ---------------------------------------------------------------------------


def _consumer():
    adapter = FinalizeCaptureAdapter()
    return GatewayStreamConsumer(
        adapter, "chat-1", StreamConsumerConfig(cursor=" ▉")
    )


class TestDeliveredFinalMatches:
    def test_no_record_returns_none(self):
        consumer = _consumer()
        assert consumer.delivered_final_matches("anything") is None

    def test_matching_record_returns_true(self):
        consumer = _consumer()
        consumer._record_turn_final_payload(FULL_RESPONSE)
        assert consumer.delivered_final_matches(FULL_RESPONSE) is True

    def test_stale_prefix_record_returns_false(self):
        consumer = _consumer()
        consumer._record_turn_final_payload(STREAMED_PREFIX)
        assert consumer.delivered_final_matches(FULL_RESPONSE) is False

    def test_split_delivery_returns_none(self):
        consumer = _consumer()
        consumer._turn_split_delivery = True
        consumer._record_turn_final_payload(STREAMED_PREFIX)
        assert consumer.delivered_final_matches(FULL_RESPONSE) is None

    def test_empty_final_text_returns_none(self):
        consumer = _consumer()
        consumer._record_turn_final_payload(STREAMED_PREFIX)
        assert consumer.delivered_final_matches("") is None

    def test_segment_delivered_text_still_matches(self):
        consumer = _consumer()
        consumer._record_turn_final_payload(STREAMED_PREFIX)
        # A prior segment delivered the exact final text.
        consumer._delivered_segment_texts.append(FULL_RESPONSE)
        assert consumer.delivered_final_matches(FULL_RESPONSE) is True

    def test_reset_segment_state_clears_record(self):
        consumer = _consumer()
        consumer._record_turn_final_payload(STREAMED_PREFIX)
        consumer._reset_segment_state()
        assert consumer._delivered_final_text is None
        assert consumer._turn_split_delivery is False
