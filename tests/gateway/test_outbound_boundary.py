import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

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


def test_named_envelope_preserves_existing_frame_evidence():
    metadata = {
        "actionability": {"requires_user_reply": True, "intent": "approval", "risk": "high"},
        "continuation_evidence": {
            "kind": "decision_frame",
            "frame_id": "df-existing",
            "items_content_hash": "sha256:" + "a" * 64,
            "phase": "pre_send",
        },
    }
    content = (
        "管理员通知\n<!-- hermes-outbound-actionable\n"
        + __import__("json").dumps(metadata)
        + "\n-->"
    )

    context = ob.build_outbound_context(
        source_kind="send_message",
        content=content,
        platform="feishu",
        chat_id="admin-dm",
    )

    assert context["content"] == "管理员通知"
    assert context["continuation_evidence"] == metadata["continuation_evidence"]


def process_gateway_reply(monkeypatch, *, hooks, response):
    from gateway import run as gateway_run

    monkeypatch.setattr(
        gateway_run,
        "_gateway_runner_ref",
        lambda: SimpleNamespace(hooks=hooks),
    )
    adapter = StubAdapter(
        PlatformConfig(enabled=True, token="test", typing_indicator=False),
        Platform.TELEGRAM,
    )
    adapter._message_handler = AsyncMock(return_value=response)
    adapter._send_with_retry = AsyncMock(
        return_value=SendResult(success=True, message_id="sent")
    )
    adapter._run_processing_hook = AsyncMock()
    adapter._stop_typing_refresh = AsyncMock()
    adapter._flush_text_debounce_now = AsyncMock(return_value=False)
    event = MessageEvent(
        text="生成机构日报",
        message_type=MessageType.TEXT,
        source=SessionSource(
            platform=Platform.TELEGRAM,
            chat_id="admin-dm",
            chat_type="dm",
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
    assert ob.gateway_reply_source_kind("[[ACTION_SPEC]] approve") == "operator_enforce"


def test_gateway_reply_context_requires_output_screening_without_creating_an_action():
    context = ob.build_outbound_context(
        source_kind="gateway_reply",
        content="请回复 1 或 2 选择下一步",
        platform="qihu360teams",
        chat_id="JK-SA",
    )
    assert context["looks_actionable"] is False
    assert context["output_screening_required"] is True
    assert "action_spec" not in context
    decision = run(ob.outbound_before_send(None, context))
    assert decision.transmit is False
    assert decision.reason == "required_output_screening_hook_missing"


def test_gateway_reply_screening_allow_keeps_non_action_context():
    observed = []

    async def handler(event_type, context):
        observed.append((event_type, dict(context)))
        return {"decision": "allow", "reason": "output_safe"}

    context = ob.build_outbound_context(
        source_kind="gateway_reply",
        content="今日结果已整理完成。",
        platform="qihu360teams",
        chat_id="JK-SA",
    )
    decision = run(ob.outbound_before_send(Hooks(named_handler(handler)), context))

    assert decision.transmit is True
    assert decision.reason == "output_safe"
    assert observed[0][0] == ob.BEFORE_SEND
    assert observed[0][1]["looks_actionable"] is False
    assert "action_spec" not in observed[0][1]


def test_required_delivery_receipts_never_turn_partial_into_success():
    summary = ob.summarize_delivery_receipts(
        [
            ob.delivery_receipt({"success": False, "error": "private detail"}, kind="text"),
            ob.delivery_receipt({"success": True, "message_id": "msg-2"}, kind="document"),
        ]
    )

    assert summary["status"] == "partial"
    assert summary["success"] is False
    assert summary["send_result"]["message_id"] == "msg-2"
    assert summary["receipts"][0]["error_kind"] == "delivery_failed"
    assert "error" not in summary["receipts"][0]


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


def test_report_contract_suppresses_text_only_confirmation_heuristic():
    context = ob.build_outbound_context(
        source_kind="cron",
        content="日报已生成；如需展开明细，请回复 1。",
        platform="feishu",
        chat_id="admin-dm",
        legacy_actionable_output={
            "mode": "not_actionable",
            "requires_user_reply": False,
        },
    )

    assert context["looks_actionable"] is False


def test_action_spec_overrides_report_contract():
    context = ob.build_outbound_context(
        source_kind="cron",
        content="请回复 授权 继续。",
        platform="feishu",
        chat_id="admin-dm",
        legacy_actionable_output={
            "mode": "not_actionable",
            "requires_user_reply": False,
        },
        action_spec={"items": [], "actions": []},
    )

    assert context["looks_actionable"] is True


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


def test_hook_suppress_is_successful_non_transmission():
    async def suppress(_event_type, _context):
        return {"decision": "suppress", "reason": "intentional_silence"}

    decision = run(
        ob.outbound_before_send(
            Hooks(named_handler(suppress)),
            ctx(source_kind="cron", output_screening_required=True),
        )
    )

    assert decision.transmit is False
    assert decision.decision == "suppress"
    assert decision.content == ""
    assert decision.reason == "intentional_silence"


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
