"""Parser-only and lightweight routing tests for send_message targets.

These stay separate from ``test_send_message_tool.py`` because that module
skips wholesale when optional Telegram dependencies are not installed.
"""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from gateway.config import Platform
from gateway.hooks import HookRegistry
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

    with patch("gateway.config.load_gateway_config", return_value=config), \
         patch("tools.interrupt.is_interrupted", return_value=False), \
         patch("model_tools._run_async", side_effect=_run_async_immediately), \
         patch("gateway.outbound_boundary.outbound_before_send_sync", return_value=decision), \
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
