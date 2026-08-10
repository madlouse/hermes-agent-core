import asyncio
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway import outbound_boundary as ob
from gateway.config import Platform, PlatformConfig
from gateway.hooks import HookRegistry
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.session import SessionSource


def Hooks(*handlers):
    registry = HookRegistry()
    for event_type in (ob.BEFORE_SEND, ob.AFTER_SEND):
        registry._handlers[event_type] = list(handlers)
    for handler in handlers:
        owner = getattr(handler, "_test_loader_owner", "")
        if owner:
            registry._handler_owners[id(handler)] = owner
        capabilities = getattr(handler, "_test_loader_capabilities", ())
        if capabilities:
            registry._handler_capabilities[id(handler)] = frozenset(capabilities)
    return registry


def named_handler(handler, name="outbound-actionable", capabilities=None):
    handler._test_loader_owner = name
    handler._test_loader_capabilities = tuple(
        capabilities
        if capabilities is not None
        else ([ob.REQUIRED_SCREENING_CAPABILITY] if name == "outbound-actionable" else [])
    )
    return handler


class StubAdapter(BasePlatformAdapter):
    async def connect(self, *, is_reconnect=False):
        return None

    async def disconnect(self):
        return None

    async def send(self, chat_id, text, **kwargs):
        return SendResult(success=True, message_id="sent")

    async def get_chat_info(self, chat_id):
        return {}


def run(coro):
    return asyncio.run(coro)


def ctx(**overrides):
    base = ob.build_outbound_context(
        source_kind="send_message",
        content="plain status update",
        platform="qihu360teams",
        chat_id="JK-SA",
    )
    base.update(overrides)
    return base


def process_gateway_reply(
    monkeypatch,
    *,
    hooks,
    response,
    send_result=None,
    send_voice_result=None,
    send_document_result=None,
    profile_id="atlas",
    source_profile=None,
    profile_config=None,
    enforced_channel=False,
):
    from gateway import run as gateway_run

    profile_temp = tempfile.TemporaryDirectory()
    profile_home = Path(profile_temp.name)
    (profile_home / "config.yaml").write_text(
        profile_config if profile_config is not None else f"profile_id: {profile_id}\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        gateway_run,
        "_gateway_runner_ref",
        lambda: SimpleNamespace(
            hooks=hooks,
            _resolve_profile_home_for_source=lambda _source: profile_home,
            _profile_name_for_source=lambda source: source.profile or profile_id,
            _active_profile_name=lambda: profile_id,
        ),
    )
    monkeypatch.setattr(
        gateway_run,
        "_operator_enforce_streaming_boundary_source_armed",
        lambda _profile_home, _source: enforced_channel,
    )
    adapter = StubAdapter(
        PlatformConfig(enabled=True, token="test", typing_indicator=False),
        Platform.TELEGRAM,
    )
    adapter._message_handler = AsyncMock(return_value=response)
    adapter._send_with_retry = AsyncMock(
        return_value=(
            send_result
            if send_result is not None
            else SendResult(success=True, message_id="sent")
        )
    )
    adapter.supports_transport_authority = True
    adapter.send_authorized = AsyncMock(
        return_value=(
            send_result
            if send_result is not None
            else SendResult(success=True, message_id="sent")
        )
    )
    adapter._run_processing_hook = AsyncMock()
    adapter._stop_typing_refresh = AsyncMock()
    adapter._flush_text_debounce_now = AsyncMock(return_value=False)
    adapter._notify_media_delivery_failure = AsyncMock()
    adapter._test_profile_temp = profile_temp
    adapter._test_profile_home = profile_home
    if send_voice_result is not None:
        adapter.send_voice = AsyncMock(return_value=send_voice_result)
    if send_document_result is not None:
        adapter.send_document = AsyncMock(return_value=send_document_result)
    event = MessageEvent(
        text="生成机构日报",
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.TELEGRAM,
            chat_id="admin-dm",
            chat_type="dm",
            profile=source_profile,
        ),
        message_id="inbound",
    )
    run(adapter._process_message_background(event, "telegram:admin-dm"))
    return adapter


def test_non_actionable_without_hook_allows():
    decision = run(ob.outbound_before_send(None, ctx()))
    assert decision.transmit is True
    assert decision.reason == "not_actionable"


def test_actionable_without_hook_denies():
    decision = run(ob.outbound_before_send(None, ctx(content="请回复 1 确认继续", looks_actionable=True)))
    assert decision.transmit is False
    assert decision.decision == "deny"
    assert decision.reason == "no_boundary_decision"


def test_operator_enforced_without_hook_holds():
    decision = run(ob.outbound_before_send(None, ctx(source_kind="operator_enforce", content="[[ACTION_SPEC]] x")))
    assert decision.transmit is False
    assert decision.decision == "hold"


def test_gateway_reply_source_kind_only_escalates_explicit_action_spec():
    assert ob.gateway_reply_source_kind("请回复 1 或 2 选择下一步") == "gateway_reply"
    assert (
        ob.gateway_reply_source_kind(
            "已完成。两项均已确认，可靠投递回写成功：2/2 completed。",
            enforced_channel=True,
        )
        == "gateway_reply"
    )
    assert ob.gateway_reply_source_kind("[[ACTION_SPEC]] approve") == "operator_enforce"


def test_gateway_reply_context_does_not_force_text_heuristic():
    context = ob.build_outbound_context(
        source_kind="gateway_reply",
        content="请回复 1 或 2 选择下一步",
        platform="qihu360teams",
        chat_id="JK-SA",
    )
    assert context["looks_actionable"] is False
    decision = run(ob.outbound_before_send(None, context))
    assert decision.transmit is True
    assert decision.reason == "not_actionable"


def test_enforced_gateway_result_is_screened_without_becoming_actionable():
    context = ob.build_outbound_context(
        source_kind=ob.gateway_reply_source_kind(
            "已完成。两项均已确认，可靠投递回写成功：2/2 completed。",
            enforced_channel=True,
        ),
        content="已完成。两项均已确认，可靠投递回写成功：2/2 completed。",
        platform="feishu",
        chat_id="oc_test",
        enforced_channel=True,
        output_screening_required=True,
    )

    assert context["source_kind"] == "gateway_reply"
    assert context["looks_actionable"] is False
    assert ob.requires_boundary(context) is True


def test_enforced_gateway_reply_with_explicit_actionability_stays_actionable():
    context = ob.build_outbound_context(
        source_kind="gateway_reply",
        content="请回复 1 继续。",
        platform="feishu",
        chat_id="oc_test",
        enforced_channel=True,
        output_screening_required=True,
        actionability={"requires_user_reply": True, "intent": "confirmation"},
    )

    assert context["looks_actionable"] is True
    assert ob.requires_boundary(context) is True


def test_gateway_reply_screening_requires_the_existing_hook_decision():
    context = ob.build_outbound_context(
        source_kind="gateway_reply",
        content="日报已生成。",
        platform="qihu360teams",
        chat_id="JK-SA",
        output_screening_required=True,
    )

    missing = run(ob.outbound_before_send(None, context))
    allowed = run(
        ob.outbound_before_send(
            Hooks(named_handler(lambda event_type, payload: {"decision": "allow", "reason": "screened"})),
            context,
        )
    )

    assert missing.transmit is False
    assert missing.reason == "required_output_screening_hook_missing"
    assert allowed.transmit is True
    assert allowed.reason == "screened"


def test_required_screening_uses_a_discovered_loader_owned_hook(tmp_path, monkeypatch):
    hook_dir = tmp_path / "hooks" / "policy-screen"
    hook_dir.mkdir(parents=True)
    (hook_dir / "HOOK.yaml").write_text(
        "name: policy-screen\n"
        "events: [outbound:before_send, outbound:after_send]\n"
        "capabilities: [output-screening]\n",
        encoding="utf-8",
    )
    (hook_dir / "handler.py").write_text(
        "def handle(event_type, context):\n"
        "    return {'decision': 'allow', 'reason': 'discovered_screening'}\n",
        encoding="utf-8",
    )
    registry = HookRegistry()
    monkeypatch.setattr("gateway.hooks.HOOKS_DIR", tmp_path / "hooks")
    registry.discover_and_load()

    decision = run(
        ob.outbound_before_send(
            registry,
            ctx(content="business result", output_screening_required=True),
        )
    )

    assert decision.transmit is True
    assert decision.reason == "discovered_screening"


def test_adapter_screens_plain_gateway_reply_before_existing_send(monkeypatch):
    calls = []

    def boundary(event_type, payload):
        calls.append((event_type, dict(payload)))
        if event_type == "outbound:before_send":
            return {
                "decision": "rewrite",
                "reason": "user_visible_projection",
                "content": "机构日报已完成，共 12 项。",
            }
        return {"decision": "allow", "reason": "delivery_recorded"}

    adapter = process_gateway_reply(
        monkeypatch,
        hooks=Hooks(named_handler(boundary)),
        response="读取 /private/tmp/internal.log 后，机构日报已完成，共 12 项。",
    )

    assert adapter._send_with_retry.await_args.kwargs["content"] == "机构日报已完成，共 12 项。"
    assert [event_type for event_type, _ in calls] == [
        "outbound:before_send",
        "outbound:after_send",
    ]
    assert calls[0][1]["source_kind"] == "gateway_reply"
    assert calls[0][1]["output_screening_required"] is True
    assert calls[0][1]["profile_id"] == "atlas"
    assert calls[0][1]["profile_path"] == str(adapter._test_profile_home)


def test_gateway_final_reply_hook_authority_uses_core_executor(monkeypatch):
    content = "请回复 1 确认"
    now = datetime.now(timezone.utc)
    route = {
        "transport_id": "telegram",
        "channel_id": "admin-dm",
        "thread_id": "",
    }
    request = {
        "request_id": "request-gateway-reply",
        "profile_id": "default",
        "frame_id": "frame-gateway-reply",
        "notification_claim_id": "claim-gateway-reply",
        "decision_route": route,
        "notification_route": route,
        "items_content_hash": "sha256:items",
        "visible_content_sha256": hashlib.sha256(content.encode()).hexdigest(),
        "claim_created_at": now.isoformat(),
        "claim_expires_at": (now + timedelta(hours=1)).isoformat(),
    }
    authority = {
        "schema_version": ob.DELIVERY_AUTHORITY_SCHEMA_VERSION,
        "required": True,
        "business_profile_id": "atlas",
        "request": request,
    }
    executions = []

    def boundary(_event_type, _payload):
        return {
            "decision": "allow",
            "reason": "registered",
            "delivery_authority": authority,
        }

    async def execute(**kwargs):
        executions.append(kwargs)
        provider_result = await kwargs["send"]()
        return ob.AuthorizedOutboundExecution(
            result=provider_result,
            outcome="confirmed",
            request=request,
            receipt={"receipt_id": "receipt-gateway-reply"},
            provider_called=True,
        )

    monkeypatch.setattr(ob, "execute_authorized_outbound_send", execute)
    monkeypatch.setattr("gateway.delivery_ledger.ledger_enabled", lambda: True)
    monkeypatch.setattr(
        "gateway.delivery_ledger.record_obligation",
        lambda **kwargs: pytest.fail("authority send must not enter legacy ledger"),
    )
    adapter = process_gateway_reply(
        monkeypatch,
        hooks=Hooks(
            named_handler(
                boundary,
                capabilities={
                    ob.REQUIRED_SCREENING_CAPABILITY,
                    ob.TRANSPORT_OUTBOX_AUTHORITY_CAPABILITY,
                },
            )
        ),
        response=content,
    )

    assert len(executions) == 1
    adapter.send_authorized.assert_awaited_once()
    adapter._send_with_retry.assert_not_awaited()
    assert executions[0]["decision"].delivery_authority == authority


def test_gateway_notice_hook_authority_uses_core_executor(monkeypatch):
    from gateway import run as gateway_run

    authority = {
        "schema_version": ob.DELIVERY_AUTHORITY_SCHEMA_VERSION,
        "required": True,
        "business_profile_id": "atlas",
        "request": {"request_id": "request-gateway-notice"},
    }
    decision = SimpleNamespace(
        content="最终通知内容",
        delivery_authority=authority,
    )
    provider = lambda: SendResult(success=True, message_id="notice-1")
    executions = []

    def execute(**kwargs):
        executions.append(kwargs)
        result = kwargs["send"]()
        return ob.AuthorizedOutboundExecution(
            result=result,
            outcome="confirmed",
            request={"request_id": "request-gateway-notice"},
            receipt={"receipt_id": "receipt-gateway-notice"},
            provider_called=True,
        )

    monkeypatch.setattr(ob, "execute_authorized_outbound_send_sync", execute)
    monkeypatch.setattr(
        gateway_run,
        "_operator_enforce_outbound_after_send",
        lambda *args: pytest.fail("legacy after_send must not run for authority"),
    )

    result = gateway_run._send_screened_gateway_notice(
        Hooks(),
        {"source_kind": "gateway_notice"},
        decision,
        provider,
    )

    assert result.message_id == "notice-1"
    assert len(executions) == 1
    assert executions[0]["context"]["content"] == "最终通知内容"


def test_armed_adapter_screens_terminal_result_without_actionable_escalation(monkeypatch):
    calls = []

    def boundary(event_type, payload):
        calls.append((event_type, dict(payload)))
        return {"decision": "allow", "reason": "screened"}

    process_gateway_reply(
        monkeypatch,
        hooks=Hooks(named_handler(boundary)),
        response="已完成。两项均已确认，可靠投递回写成功：2/2 completed。",
        enforced_channel=True,
    )

    assert calls[0][0] == "outbound:before_send"
    assert calls[0][1]["source_kind"] == "gateway_reply"
    assert calls[0][1]["enforced_channel"] is True
    assert calls[0][1]["output_screening_required"] is True
    assert calls[0][1]["looks_actionable"] is False


def test_adapter_binds_named_profile_owner_before_screening(monkeypatch):
    calls = []

    def boundary(event_type, payload):
        calls.append((event_type, dict(payload)))
        return {"decision": "allow", "reason": "screened"}

    adapter = process_gateway_reply(
        monkeypatch,
        hooks=Hooks(named_handler(boundary)),
        response="猿哥任务已完成。",
        profile_id="yuange",
        source_profile="yuange",
    )

    assert calls[0][0] == "outbound:before_send"
    assert calls[0][1]["profile_id"] == "yuange"
    assert calls[0][1]["profile_path"] == str(adapter._test_profile_home)


def test_profile_id_from_home_uses_business_owner_not_core_default(tmp_path):
    (tmp_path / "config.yaml").write_text("profile_id: atlas\n", encoding="utf-8")

    assert ob.profile_id_from_home(tmp_path) == "atlas"


def test_profile_id_from_home_does_not_reuse_last_known_good_owner(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("profile_id: atlas\n", encoding="utf-8")
    assert ob.profile_id_from_home(tmp_path) == "atlas"

    config_path.write_text("profile_id: [\n", encoding="utf-8")
    assert ob.profile_id_from_home(tmp_path) == ""
    assert ob.profile_id_from_home(tmp_path) == ""

    config_path.write_text("profile_id: yuange\n", encoding="utf-8")
    assert ob.profile_id_from_home(tmp_path) == "yuange"


def test_profile_id_from_home_does_not_expand_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("PROFILE_OWNER", "atlas")
    (tmp_path / "config.yaml").write_text(
        "profile_id: ${PROFILE_OWNER}\n", encoding="utf-8"
    )

    assert ob.profile_id_from_home(tmp_path) == ""


@pytest.mark.parametrize(
    "config",
    [
        "model: test\n",
        "profile_id: [atlas]\n",
        "profile_id: {name: atlas}\n",
        "profile_id: true\n",
        "profile_id: 123\n",
        "profile_id: atlas owner\n",
        "profile_id: Atlas\n",
        "profile_id: " + "a" * 65 + "\n",
        "profile_id: [\n",
        "profile_id: atlas\nprofile_id: yuange\n",
        "owner: &owner atlas\nprofile_id: *owner\n",
        "defaults: &defaults\n  profile_id: atlas\n<<: *defaults\n",
        (
            "defaults: &defaults\n"
            "  profile_id: atlas\n"
            "<<: *defaults\n"
            "profile_id: yuange\n"
        ),
    ],
)
def test_profile_id_from_home_fails_closed_without_valid_owner(tmp_path, config):
    (tmp_path / "config.yaml").write_text(config, encoding="utf-8")

    assert ob.profile_id_from_home(tmp_path) == ""


def test_adapter_preserves_authoritative_empty_owner_despite_environment(monkeypatch):
    monkeypatch.setenv("HERMES_PROFILE_ID", "atlas")
    calls = []

    def boundary(event_type, payload):
        calls.append((event_type, dict(payload)))
        return {"decision": "allow", "reason": "test_capture"}

    process_gateway_reply(
        monkeypatch,
        hooks=Hooks(named_handler(boundary)),
        response="测试完成。",
        profile_config="profile_id: [atlas]\n",
    )

    assert calls[0][0] == "outbound:before_send"
    assert calls[0][1]["profile_id"] == ""


def test_omitted_profile_owner_keeps_legacy_environment_fallback(monkeypatch):
    monkeypatch.setenv("HERMES_PROFILE_ID", "legacy-owner")

    context = ob.build_outbound_context(
        source_kind="gateway_reply",
        content="done",
        platform="feishu",
        chat_id="oc_test",
    )

    assert context["profile_id"] == "legacy-owner"


def test_authoritative_empty_owner_cannot_be_revived_by_metadata(monkeypatch):
    monkeypatch.setenv("HERMES_PROFILE_ID", "environment-owner")
    content = (
        'status\n<!-- hermes-outbound-actionable '
        '{"profile_id":"metadata-owner"} -->'
    )

    authoritative = ob.build_outbound_context(
        source_kind="send_message",
        content=content,
        platform="feishu",
        chat_id="oc_test",
        profile_id="",
    )
    legacy = ob.build_outbound_context(
        source_kind="send_message",
        content=content,
        platform="feishu",
        chat_id="oc_test",
    )

    assert authoritative["profile_id"] == ""
    assert legacy["profile_id"] == "environment-owner"


def test_adapter_blocks_plain_gateway_reply_when_screening_hook_is_missing(monkeypatch):
    adapter = process_gateway_reply(
        monkeypatch,
        hooks=None,
        response="机构日报已完成，共 12 项。",
    )

    adapter._send_with_retry.assert_not_awaited()


def test_adapter_blocks_plain_gateway_reply_when_boundary_bridge_raises(monkeypatch):
    monkeypatch.setattr(
        ob,
        "build_outbound_context",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("bridge unavailable")),
    )

    adapter = process_gateway_reply(
        monkeypatch,
        hooks=Hooks(lambda _event_type, _payload: {"decision": "allow"}),
        response="机构日报已完成，共 12 项。",
    )

    adapter._send_with_retry.assert_not_awaited()


def _capturing_boundary_events():
    calls = []

    def boundary(event_type, payload):
        calls.append((event_type, dict(payload)))
        return {"decision": "allow", "reason": "screened"}

    return calls, Hooks(named_handler(boundary))


def test_adapter_after_send_reports_failed_text_result(monkeypatch):
    calls, hooks = _capturing_boundary_events()

    process_gateway_reply(
        monkeypatch,
        hooks=hooks,
        response="delivery fails",
        send_result=SendResult(success=False, error="transport rejected"),
    )

    after_send = [payload for event_type, payload in calls if event_type == ob.AFTER_SEND]
    assert len(after_send) == 1
    assert after_send[0]["success"] is False
    assert after_send[0]["send_result"]["success"] is False
    assert after_send[0]["send_result"]["error"] == "transport rejected"


def test_adapter_after_send_reports_failed_voice_media_result(
    tmp_path, monkeypatch
):
    media = tmp_path / "failed.ogg"
    media.write_bytes(b"OggS")
    calls, hooks = _capturing_boundary_events()
    adapter = process_gateway_reply(
        monkeypatch,
        hooks=hooks,
        response=f"[[audio_as_voice]]\nMEDIA:{media}",
        send_voice_result=SendResult(success=False, error="voice rejected"),
    )
    adapter.send_voice.assert_awaited_once()

    after_send = [payload for event_type, payload in calls if event_type == ob.AFTER_SEND]
    assert len(after_send) == 1
    assert after_send[0]["success"] is False
    assert after_send[0]["send_result"]["success"] is False
    assert after_send[0]["send_result"]["error"] == "voice rejected"


def test_adapter_after_send_reports_failed_document_attachment_result(
    tmp_path, monkeypatch
):
    attachment = tmp_path / "failed.pdf"
    attachment.write_bytes(b"%PDF-1.4")
    calls, hooks = _capturing_boundary_events()
    adapter = process_gateway_reply(
        monkeypatch,
        hooks=hooks,
        response=f"MEDIA:{attachment}",
        send_document_result=SendResult(
            success=False,
            error="document rejected",
        ),
    )
    adapter.send_document.assert_awaited_once()

    after_send = [payload for event_type, payload in calls if event_type == ob.AFTER_SEND]
    assert len(after_send) == 1
    assert after_send[0]["success"] is False
    assert after_send[0]["send_result"]["success"] is False
    assert after_send[0]["send_result"]["error"] == "document rejected"


def test_hidden_outbound_metadata_is_stripped_and_forwarded():
    context = ob.build_outbound_context(
        source_kind="send_message",
        content=(
            "Visible choice\n"
            '<!-- hermes-outbound-actionable {"action_spec": {"items": [{"item_id": "a"}], '
            '"actions": [{"action_id": "send", "aliases": ["发送 1"]}]}, '
            '"actionability": {"requires_user_reply": true, "intent": "confirmation"}, '
            '"gate_mode": "enforce"} -->'
        ),
        platform="qihu360teams",
        chat_id="JK-SA",
    )
    assert context["content"] == "Visible choice"
    assert context["action_spec"]["items"][0]["item_id"] == "a"
    assert context["actionability"]["requires_user_reply"] is True
    assert context["looks_actionable"] is True


def test_empty_optional_actionable_fields_do_not_make_plain_send_actionable():
    context = ob.build_outbound_context(
        source_kind="send_message",
        content="plain status update",
        platform="qihu360teams",
        chat_id="JK-SA",
        action_spec=None,
        action_specs=None,
        actionability=None,
    )
    assert "action_spec" not in context
    assert "action_specs" not in context
    assert "actionability" not in context
    assert context["looks_actionable"] is False
    decision = run(ob.outbound_before_send(None, context))
    assert decision.transmit is True
    assert decision.reason == "not_actionable"


def test_action_spec_envelope_is_stripped_and_escalates_operator_source():
    content = 'Reply text\n[[ACTION_SPEC]] {"items": [], "actions": []} [[/ACTION_SPEC]]'
    assert ob.gateway_reply_source_kind(content) == "operator_enforce"
    context = ob.build_outbound_context(
        source_kind="operator_enforce",
        content=content,
        platform="qihu360teams",
        chat_id="JK-SA",
    )
    assert context["content"] == "Reply text"
    assert context["action_spec"] == {"items": [], "actions": []}


def test_invalid_actionable_envelope_fails_closed_before_hooks():
    context = ob.build_outbound_context(
        source_kind="send_message",
        content='Visible <!-- hermes-outbound-actionable {"action_spec": -->',
        platform="qihu360teams",
        chat_id="JK-SA",
    )
    decision = run(ob.outbound_before_send(None, context))
    assert decision.transmit is False
    assert decision.reason == "invalid_actionable_envelope"


def test_gate_mode_off_allows_actionable_without_hook(monkeypatch):
    monkeypatch.setenv("HERMES_OUTBOUND_GATE_MODE", "off")
    context = ctx(content="请回复 1 确认继续", looks_actionable=True)
    decision = run(ob.outbound_before_send(None, context))
    assert ob.requires_boundary(context) is False
    assert decision.transmit is True
    assert decision.reason == "gate_mode_off"


def test_boundary_disabled_allows_actionable_without_hook():
    context = ctx(content="请回复 1 确认继续", looks_actionable=True, boundary_enabled=False)
    decision = run(
        ob.outbound_before_send(
            None,
            context,
        )
    )
    assert ob.requires_boundary(context) is False
    assert decision.transmit is True
    assert decision.reason == "boundary_disabled"


def test_required_output_screening_cannot_be_disabled_by_runtime_overrides(monkeypatch):
    contexts = [
        ctx(content="业务结果", output_screening_required=True, boundary_enabled=False),
        ctx(content="业务结果", output_screening_required=True, gate_mode="off"),
    ]
    monkeypatch.setenv("HERMES_OUTBOUND_BOUNDARY_ENABLED", "false")
    monkeypatch.setenv("HERMES_OUTBOUND_GATE_MODE", "off")

    for context in contexts:
        decision = run(ob.outbound_before_send(None, context))
        assert ob.requires_boundary(context) is True
        assert decision.transmit is False
        assert decision.reason == "required_output_screening_hook_missing"


def test_required_output_screening_rejects_unrelated_allow_handler():
    unrelated = named_handler(
        lambda _event_type, _context: {"decision": "allow", "reason": "unrelated"},
        name="metrics",
    )
    context = ctx(content="业务结果", output_screening_required=True)

    decision = run(ob.outbound_before_send(Hooks(unrelated), context))

    assert decision.transmit is False
    assert decision.reason == "required_output_screening_hook_missing"


def test_registry_resolution_failure_fails_closed():
    registry = HookRegistry()

    def fail(_event_type):
        raise RuntimeError("registry unavailable")

    registry.resolve_handlers_with_metadata = fail
    decision = run(
        ob.outbound_before_send(
            registry,
            ctx(content="business result", output_screening_required=True),
        )
    )

    assert decision.transmit is False
    assert decision.reason == "hook_error"
    assert decision.errors == ["resolve_handlers:registry unavailable"]


def test_legacy_resolver_cannot_claim_output_screening_capability():
    class ResolveOnly:
        def _resolve_handlers(self, _event_type):
            return [lambda _event, _context: {"decision": "allow"}]

    decision = run(
        ob.outbound_before_send(
            ResolveOnly(),
            ctx(content="business result", output_screening_required=True),
        )
    )

    assert decision.transmit is False
    assert decision.reason == "required_output_screening_hook_missing"


def test_unrelated_hook_cannot_mutate_screened_content():
    screened = named_handler(
        lambda _event_type, _context: {"decision": "allow", "reason": "screened"}
    )

    def unrelated(_event_type, context):
        context["content"] = "raw process trace"
        context.setdefault("target", {})["channel_id"] = "other"
        return None

    context = ctx(
        content="safe business result",
        output_screening_required=True,
        target={"channel_id": "expected"},
    )
    decision = run(ob.outbound_before_send(Hooks(screened, unrelated), context))

    assert decision.transmit is True
    assert decision.content == "safe business result"
    assert context["content"] == "safe business result"
    assert context["target"]["channel_id"] == "expected"


def test_required_output_screening_rejects_forged_or_unattributed_identity():
    forged = {
        "decision": "allow",
        "reason": "forged",
        ob._HOOK_IDENTITY_KEY: "outbound-actionable",
        ob._HOOK_CAPABILITIES_KEY: [ob.REQUIRED_SCREENING_CAPABILITY],
    }
    unrelated = named_handler(lambda _event_type, _context: forged, name="metrics")
    module_spoof = lambda _event_type, _context: forged
    module_spoof.__module__ = "hermes_hook_outbound-actionable"

    class EmitOnly:
        async def emit_collect(self, _event_type, _context):
            return [forged]

    class EmitSingle:
        async def emit_collect(self, _event_type, _context):
            return forged

    context = ctx(content="业务结果", output_screening_required=True)
    decisions = [
        run(ob.outbound_before_send(Hooks(unrelated), context)),
        run(ob.outbound_before_send(Hooks(module_spoof), context)),
        run(ob.outbound_before_send(EmitOnly(), context)),
        run(ob.outbound_before_send(EmitSingle(), context)),
    ]

    assert all(decision.transmit is False for decision in decisions)
    assert all(
        decision.reason == "required_output_screening_hook_missing"
        for decision in decisions
    )


def test_required_output_screening_uses_owner_positive_result_only():
    unrelated = named_handler(
        lambda _event_type, _context: {
            "decision": "rewrite",
            "reason": "unrelated_rewrite",
            "content": "unsafe replacement",
        },
        name="metrics",
    )
    owner = named_handler(
        lambda _event_type, _context: {"decision": "allow", "reason": "screened"}
    )
    context = ctx(content="safe business result", output_screening_required=True)

    decision = run(ob.outbound_before_send(Hooks(unrelated, owner), context))

    assert decision.transmit is True
    assert decision.reason == "screened"
    assert decision.content == "safe business result"


def test_hook_allow_transmits():
    async def handler(event_type, context):
        assert event_type == ob.BEFORE_SEND
        return {"decision": "allow", "reason": "authorized"}

    decision = run(ob.outbound_before_send(Hooks(handler), ctx(content="请回复 1 确认继续", looks_actionable=True)))
    assert decision.transmit is True
    assert decision.reason == "authorized"


def test_hook_deny_blocks():
    async def handler(event_type, context):
        return {"decision": "deny", "reason": "missing_continuation"}

    decision = run(ob.outbound_before_send(Hooks(handler), ctx(content="请回复 1 确认继续", looks_actionable=True)))
    assert decision.transmit is False
    assert decision.decision == "deny"
    assert decision.reason == "missing_continuation"


def test_hook_rewrite_changes_content():
    def handler(event_type, context):
        return {"decision": "rewrite", "content": "safe visible frame", "reason": "framed"}

    decision = run(ob.outbound_before_send(Hooks(handler), ctx(content="请回复 1 确认继续", looks_actionable=True)))
    assert decision.transmit is True
    assert decision.decision == "rewrite"
    assert decision.content == "safe visible frame"


def test_deny_wins_over_rewrite_from_later_handler_order():
    def rewrite(event_type, context):
        return {"decision": "rewrite", "content": "safe visible frame", "reason": "framed"}

    def deny(event_type, context):
        return {"decision": "deny", "reason": "policy_block"}

    decision = run(
        ob.outbound_before_send(
            Hooks(rewrite, deny),
            ctx(content="请回复 1 确认继续", looks_actionable=True),
        )
    )
    assert decision.transmit is False
    assert decision.reason == "policy_block"


def test_malformed_hook_result_fails_closed_for_actionable():
    def handler(event_type, context):
        return {"decision": "surprise"}

    decision = run(ob.outbound_before_send(Hooks(handler), ctx(content="请回复 1 确认继续", looks_actionable=True)))
    assert decision.transmit is False
    assert decision.reason == "malformed_handler_result"


def test_non_object_hook_result_fails_closed_for_actionable():
    decision = run(
        ob.outbound_before_send(
            Hooks(lambda _event_type, _context: ["allow"]),
            ctx(content="请回复 1 确认继续", looks_actionable=True),
        )
    )

    assert decision.transmit is False
    assert decision.reason == "malformed_handler_result"


def test_missing_decision_dict_fails_closed_for_actionable():
    def handler(event_type, context):
        return {"status": "failed", "error": "malformed"}

    decision = run(ob.outbound_before_send(Hooks(handler), ctx(content="请回复 1 确认继续", looks_actionable=True)))
    assert decision.transmit is False
    assert decision.reason == "malformed_handler_result"


def test_hook_crash_fails_closed_for_actionable():
    def handler(event_type, context):
        raise RuntimeError("boom")

    decision = run(ob.outbound_before_send(Hooks(handler), ctx(content="请回复 1 确认继续", looks_actionable=True)))
    assert decision.transmit is False
    assert decision.reason == "hook_error"
    assert decision.errors


def test_hook_timeout_fails_closed_for_actionable():
    async def handler(event_type, context):
        await asyncio.sleep(0.05)
        return {"decision": "allow"}

    decision = run(
        ob.outbound_before_send(
            Hooks(handler),
            ctx(content="请回复 1 确认继续", looks_actionable=True),
            timeout_seconds=0.001,
        )
    )
    assert decision.transmit is False
    assert decision.reason == "hook_error"
    assert "timeout" in decision.errors


def test_after_send_collects_results_and_never_requires_decision():
    async def handler(event_type, context):
        assert event_type == ob.AFTER_SEND
        return {"status": "ok", "notified": True}

    results = run(ob.outbound_after_send(Hooks(handler), ctx(send_result={"outbox_id": "ob-1"})))
    assert results == [{"status": "ok", "notified": True}]


def test_required_screening_after_send_ignores_runtime_opt_outs():
    calls = []

    def handler(event_type, context):
        calls.append((event_type, context))
        return {"status": "ok", "notified": True}

    for overrides in ({"gate_mode": "off"}, {"boundary_enabled": False}):
        context = ctx(output_screening_required=True, **overrides)
        results = run(ob.outbound_after_send(Hooks(named_handler(handler)), context))
        assert results == [{"status": "ok", "notified": True}]

    assert [event_type for event_type, _ in calls] == [ob.AFTER_SEND, ob.AFTER_SEND]


def test_required_after_send_reports_missing_owner_hook():
    unrelated = named_handler(
        lambda _event_type, _context: {"status": "ok", "notified": False},
        name="metrics",
    )

    result = run(
        ob.outbound_after_send(
            Hooks(unrelated),
            ctx(output_screening_required=True),
        )
    )

    assert result == [{"status": "failed", "reason": "required_output_screening_hook_missing"}]


def test_non_required_after_send_preserves_runtime_opt_outs():
    calls = []
    hooks = Hooks(lambda *args: calls.append(args) or {"status": "ok"})

    for overrides in ({"gate_mode": "off"}, {"boundary_enabled": False}):
        assert run(ob.outbound_after_send(hooks, ctx(**overrides))) == []

    assert calls == []


def test_sync_before_send_wrapper_runs_from_plain_thread():
    def handler(event_type, context):
        return {"decision": "allow", "reason": "ok"}

    decision = ob.outbound_before_send_sync(
        Hooks(handler),
        ctx(content="请回复 1 确认继续", looks_actionable=True),
    )
    assert decision.transmit is True
    assert decision.reason == "ok"
