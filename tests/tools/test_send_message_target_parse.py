"""Parser-only and lightweight routing tests for send_message targets.

These stay separate from ``test_send_message_tool.py`` because that module
skips wholesale when optional Telegram dependencies are not installed.
"""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from gateway.config import Platform
from gateway.platform_registry import PlatformEntry, platform_registry
from tools.send_message_tool import _parse_target_ref, send_message_tool


def _run_async_immediately(coro):
    return asyncio.run(coro)


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

    with patch("gateway.config.load_gateway_config", return_value=config), \
         patch("tools.interrupt.is_interrupted", return_value=False), \
         patch("gateway.channel_directory.resolve_channel_name", side_effect=AssertionError("raw JID should not resolve via directory")), \
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
        with (
            patch("gateway.config.load_gateway_config", return_value=config),
            patch("tools.interrupt.is_interrupted", return_value=False),
            patch(
                "gateway.channel_directory.resolve_channel_name",
                side_effect=AssertionError("plugin native id used directory"),
            ),
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
