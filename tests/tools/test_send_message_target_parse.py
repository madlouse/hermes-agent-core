"""Parser-only and lightweight routing tests for send_message targets.

These stay separate from ``test_send_message_tool.py`` because that module
skips wholesale when optional Telegram dependencies are not installed.
"""

import asyncio
import hashlib
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from gateway.config import Platform
from gateway.hooks import HookRegistry
from gateway.platform_registry import PlatformEntry, platform_registry
from tools.send_message_tool import _parse_target_ref, send_message_tool


def _run_async_immediately(coro):
    return asyncio.run(coro)


def _runner_with_boundary_result(result, *, owner="", capabilities=()):
    registry = HookRegistry()

    def handler(_event_type, _context):
        return dict(result)

    for event_type in ("outbound:before_send", "outbound:after_send"):
        registry._handlers[event_type] = [handler]
    if owner:
        registry._handler_owners[id(handler)] = owner
    if capabilities:
        registry._handler_capabilities[id(handler)] = frozenset(capabilities)
    return SimpleNamespace(hooks=registry)


def test_photon_e164_target_is_explicit() -> None:
    chat_id, thread_id, is_explicit = _parse_target_ref("photon", "+15551234567")

    assert chat_id == "+15551234567"
    assert thread_id is None
    assert is_explicit is True


def test_e164_target_still_requires_phone_platform() -> None:
    assert _parse_target_ref("matrix", "+15551234567")[2] is False


def test_registered_plugin_target_is_explicit_native_id() -> None:
    platform_registry.register(
        PlatformEntry(
            name="fakeim",
            label="Fake IM",
            adapter_factory=lambda cfg: None,
            check_fn=lambda: True,
        )
    )
    try:
        assert _parse_target_ref("fakeim", "dm-alpha") == (
            "dm-alpha",
            None,
            True,
        )
    finally:
        platform_registry.unregister("fakeim")


def test_unregistered_plugin_like_target_still_requires_resolution() -> None:
    assert _parse_target_ref("not-a-real-im", "dm-alpha")[2] is False


def test_send_message_routes_whatsapp_group_jid_without_home_fallback() -> None:
    whatsapp_cfg = SimpleNamespace(enabled=True, token=None, extra={"api_url": "http://bridge"})
    config = SimpleNamespace(
        platforms={Platform.WHATSAPP: whatsapp_cfg},
        get_home_channel=lambda _platform: SimpleNamespace(chat_id="15551234567@s.whatsapp.net"),
    )
    runner = _runner_with_boundary_result(
        {"decision": "allow", "reason": "screened"},
        owner="outbound-actionable",
        capabilities={"output-screening"},
    )

    with patch("gateway.config.load_gateway_config", return_value=config), \
         patch("tools.interrupt.is_interrupted", return_value=False), \
         patch("gateway.channel_directory.resolve_channel_name", side_effect=AssertionError("raw JID should not resolve via directory")), \
         patch("gateway.run._gateway_runner_ref", return_value=runner), \
         patch("model_tools._run_async", side_effect=_run_async_immediately), \
         patch("tools.send_message_tool._send_to_platform", new=AsyncMock(return_value={"success": True})) as send_mock, \
         patch("gateway.mirror.mirror_to_session", return_value=True):
        result = json.loads(
            send_message_tool(
                {
                    "action": "send",
                    "target": "whatsapp:120363408391911677@g.us",
                    "message": "hello group",
                }
            )
        )

    assert result["success"] is True
    assert "note" not in result
    send_mock.assert_awaited_once_with(
        Platform.WHATSAPP,
        whatsapp_cfg,
        "120363408391911677@g.us",
        "hello group",
        thread_id=None,
        media_files=[],
        force_document=False,
    )


def test_send_message_routes_plugin_native_id_without_home_fallback() -> None:
    platform_registry.register(
        PlatformEntry(
            name="fakeim",
            label="Fake IM",
            adapter_factory=lambda cfg: None,
            check_fn=lambda: True,
        )
    )
    try:
        platform = Platform("fakeim")
        fake_cfg = SimpleNamespace(enabled=True, token=None, extra={})
        config = SimpleNamespace(
            platforms={platform: fake_cfg},
            get_home_channel=lambda _platform: SimpleNamespace(chat_id="home-id"),
        )
        runner = _runner_with_boundary_result(
            {"decision": "allow", "reason": "screened"},
            owner="outbound-actionable",
            capabilities={"output-screening"},
        )
        with (
            patch("gateway.config.load_gateway_config", return_value=config),
            patch("tools.interrupt.is_interrupted", return_value=False),
            patch(
                "gateway.channel_directory.resolve_channel_name",
                side_effect=AssertionError("plugin native id used directory"),
            ),
            patch("gateway.run._gateway_runner_ref", return_value=runner),
            patch("model_tools._run_async", side_effect=_run_async_immediately),
            patch(
                "tools.send_message_tool._send_to_platform",
                new=AsyncMock(return_value={"success": True}),
            ) as send_mock,
            patch("gateway.mirror.mirror_to_session", return_value=True),
        ):
            result = json.loads(
                send_message_tool(
                    {
                        "action": "send",
                        "target": "fakeim:dm-alpha",
                        "message": "hello plugin",
                    }
                )
            )
    finally:
        platform_registry.unregister("fakeim")

    assert result["success"] is True
    assert "note" not in result
    send_mock.assert_awaited_once_with(
        platform,
        fake_cfg,
        "dm-alpha",
        "hello plugin",
        thread_id=None,
        media_files=[],
        force_document=False,
    )


def test_send_message_requires_output_screening_hook() -> None:
    whatsapp_cfg = SimpleNamespace(enabled=True, token=None, extra={"api_url": "http://bridge"})
    config = SimpleNamespace(
        platforms={Platform.WHATSAPP: whatsapp_cfg},
        get_home_channel=lambda _platform: None,
    )

    with patch("gateway.config.load_gateway_config", return_value=config), \
         patch("tools.interrupt.is_interrupted", return_value=False), \
         patch("gateway.run._gateway_runner_ref", return_value=None), \
         patch("model_tools._run_async", side_effect=_run_async_immediately), \
         patch("tools.send_message_tool._send_to_platform", new=AsyncMock()) as send_mock:
        result = json.loads(
            send_message_tool(
                {
                    "action": "send",
                    "target": "whatsapp:120363408391911677@g.us",
                    "message": "business result",
                }
            )
        )

    assert result["success"] is False
    assert "required_output_screening_hook_missing" in result["error"]
    send_mock.assert_not_awaited()


def test_send_message_rejects_unrelated_allow_hook() -> None:
    whatsapp_cfg = SimpleNamespace(enabled=True, token=None, extra={"api_url": "http://bridge"})
    config = SimpleNamespace(
        platforms={Platform.WHATSAPP: whatsapp_cfg},
        get_home_channel=lambda _platform: None,
    )
    runner = _runner_with_boundary_result(
        {"decision": "allow", "reason": "unrelated"},
        owner="metrics",
    )

    with patch("gateway.config.load_gateway_config", return_value=config), \
         patch("tools.interrupt.is_interrupted", return_value=False), \
         patch("gateway.run._gateway_runner_ref", return_value=runner), \
         patch("model_tools._run_async", side_effect=_run_async_immediately), \
         patch("tools.send_message_tool._send_to_platform", new=AsyncMock()) as send_mock:
        result = json.loads(
            send_message_tool(
                {
                    "action": "send",
                    "target": "whatsapp:120363408391911677@g.us",
                    "message": "business result",
                }
            )
        )

    assert result["success"] is False
    assert "required_output_screening_hook_missing" in result["error"]
    send_mock.assert_not_awaited()


def test_send_message_allow_rebuilds_media_from_screened_content() -> None:
    whatsapp_cfg = SimpleNamespace(enabled=True, token=None, extra={"api_url": "http://bridge"})
    config = SimpleNamespace(
        platforms={Platform.WHATSAPP: whatsapp_cfg},
        get_home_channel=lambda _platform: None,
    )
    decision = SimpleNamespace(
        transmit=True,
        decision="allow",
        content="safe business result",
        raw={"decision": "allow"},
        reason="screened",
    )

    observed = {}

    def allow(_hooks, context):
        observed.update(context)
        return decision

    with patch.dict("os.environ", {"HERMES_PROFILE_ID": "atlas"}), \
         patch("gateway.config.load_gateway_config", return_value=config), \
         patch("tools.interrupt.is_interrupted", return_value=False), \
         patch("model_tools._run_async", side_effect=_run_async_immediately), \
         patch("gateway.outbound_boundary.outbound_before_send_sync", side_effect=allow), \
         patch("gateway.outbound_boundary.outbound_after_send_sync"), \
         patch("tools.send_message_tool._send_to_platform", new=AsyncMock(return_value={"success": True})) as send_mock, \
         patch("gateway.mirror.mirror_to_session", return_value=True):
        result = json.loads(
            send_message_tool(
                {
                    "action": "send",
                    "target": "whatsapp:120363408391911677@g.us",
                    "message": "raw process\nMEDIA:/tmp/private.png",
                }
            )
        )

    assert result["success"] is True
    assert observed["profile_id"] == "atlas"
    assert send_mock.await_args.kwargs["media_files"] == []
    assert send_mock.await_args.args[3] == "safe business result"


def test_send_message_rewrite_drops_media_from_the_unscreened_source() -> None:
    whatsapp_cfg = SimpleNamespace(enabled=True, token=None, extra={"api_url": "http://bridge"})
    config = SimpleNamespace(
        platforms={Platform.WHATSAPP: whatsapp_cfg},
        get_home_channel=lambda _platform: None,
    )
    runner = _runner_with_boundary_result(
        {
            "decision": "rewrite",
            "content": "safe business result",
            "reason": "safe_projection",
        },
        owner="outbound-actionable",
        capabilities={"output-screening"},
    )

    with patch("gateway.config.load_gateway_config", return_value=config), \
         patch("tools.interrupt.is_interrupted", return_value=False), \
         patch("model_tools._run_async", side_effect=_run_async_immediately), \
         patch("gateway.run._gateway_runner_ref", return_value=runner), \
         patch("tools.send_message_tool._send_to_platform", new=AsyncMock(return_value={"success": True})) as send_mock, \
         patch("gateway.mirror.mirror_to_session", return_value=True):
        result = json.loads(
            send_message_tool(
                {
                    "action": "send",
                    "target": "whatsapp:120363408391911677@g.us",
                    "message": "raw process\nMEDIA:/tmp/private.png\n[[as_document]]",
                }
            )
        )

    assert result["success"] is True
    send_mock.assert_awaited_once_with(
        Platform.WHATSAPP,
        whatsapp_cfg,
        "120363408391911677@g.us",
        "safe business result",
        thread_id=None,
        media_files=[],
        force_document=False,
    )


def test_send_message_hook_authority_uses_core_executor_once() -> None:
    from gateway.outbound_boundary import AuthorizedOutboundExecution

    content = "请回复 1 确认"
    now = datetime.now(timezone.utc)
    route = {
        "transport_id": "whatsapp",
        "channel_id": "120363408391911677@g.us",
        "thread_id": "",
    }
    request = {
        "request_id": "request-send-message-hook",
        "profile_id": "atlas",
        "frame_id": "frame-send-message-hook",
        "notification_claim_id": "claim-send-message-hook",
        "decision_route": route,
        "notification_route": route,
        "items_content_hash": "sha256:items",
        "visible_content_sha256": hashlib.sha256(content.encode()).hexdigest(),
        "claim_created_at": now.isoformat(),
        "claim_expires_at": (now + timedelta(hours=1)).isoformat(),
    }
    authority = {
        "schema_version": "transport-outbox-hook/v1",
        "required": True,
        "request": request,
    }
    runner = _runner_with_boundary_result(
        {"decision": "allow", "reason": "registered", "delivery_authority": authority},
        owner="outbound-actionable",
        capabilities={"output-screening", "transport-outbox-authority"},
    )
    whatsapp_cfg = SimpleNamespace(enabled=True, token=None, extra={"api_url": "http://bridge"})
    config = SimpleNamespace(
        platforms={Platform.WHATSAPP: whatsapp_cfg},
        get_home_channel=lambda _platform: None,
    )
    executions = []

    async def execute(**kwargs):
        executions.append(kwargs)
        provider_result = await kwargs["send"]()
        return AuthorizedOutboundExecution(
            result=provider_result,
            outcome="confirmed",
            request=request,
            receipt={"receipt_id": "receipt-send-message-hook"},
            provider_called=True,
        )

    with patch.dict("os.environ", {"HERMES_PROFILE_ID": "atlas"}), \
         patch("gateway.config.load_gateway_config", return_value=config), \
         patch("tools.interrupt.is_interrupted", return_value=False), \
         patch("model_tools._run_async", side_effect=_run_async_immediately), \
         patch("gateway.run._gateway_runner_ref", return_value=runner), \
         patch("gateway.outbound_boundary.execute_authorized_outbound_send", side_effect=execute), \
         patch("tools.send_message_tool._send_to_platform", new=AsyncMock(return_value={"success": True, "message_id": "om-1"})) as send_mock, \
         patch("gateway.mirror.mirror_to_session", return_value=True):
        result = json.loads(
            send_message_tool(
                {
                    "action": "send",
                    "target": "whatsapp:120363408391911677@g.us",
                    "message": content,
                }
            )
        )

    assert len(executions) == 1
    send_mock.assert_awaited_once()
    assert result["transport_request_id"] == request["request_id"]
    assert result["transport_receipt_id"] == "receipt-send-message-hook"
