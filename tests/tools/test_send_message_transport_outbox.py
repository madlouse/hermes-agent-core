from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform
from gateway.transport_outbox import visible_content_sha256
from tools import send_message_tool as send_module


CONTENT = "Approve this exact operation"


def _transport_request(request_id: str = "request-send-1") -> dict:
    return {
        "request_id": request_id,
        "profile_id": "atlas",
        "frame_id": "frame-send-1",
        "notification_claim_id": "notification-claim:send-1",
        "decision_route": {
            "transport_id": "feishu",
            "channel_id": "oc_admin",
            "thread_id": "",
        },
        "notification_route": {
            "transport_id": "feishu",
            "channel_id": "oc_admin",
            "thread_id": "",
        },
        "items_content_hash": "sha256:send-items",
        "visible_content_sha256": visible_content_sha256(CONTENT),
        "claim_created_at": "2020-01-01T00:00:00+00:00",
        "claim_expires_at": "2099-01-01T00:00:00+00:00",
    }


@pytest.fixture
def standalone_send(tmp_path, monkeypatch):
    home = tmp_path / "profiles" / "atlas"
    home.mkdir(parents=True)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_PROFILE_ID", "atlas")
    pconfig = SimpleNamespace(enabled=True, token="test-token", extra={})
    config = SimpleNamespace(
        platforms={Platform.FEISHU: pconfig},
        get_home_channel=lambda _platform: None,
    )
    monkeypatch.setattr("gateway.config.load_gateway_config", lambda: config)
    monkeypatch.setattr("tools.interrupt.is_interrupted", lambda: False)
    monkeypatch.setattr("model_tools._run_async", lambda coro: asyncio.run(coro))
    return home


def _send(request_id: str = "request-send-1", *, after_send=None) -> dict:
    return json.loads(
        send_module.send_message_tool(
            {
                "action": "send",
                "target": "feishu:oc_admin",
                "message": CONTENT,
                "transport_request": _transport_request(request_id),
            },
            after_send=after_send,
        )
    )


def test_trusted_standalone_send_orders_request_send_receipt_after_send_and_mirror(
    standalone_send, monkeypatch
):
    import gateway.transport_outbox as outbox

    events = []
    original_begin = outbox.begin_transport_request
    original_commit = outbox.commit_transport_receipt

    def begin(*args, **kwargs):
        events.append("request_commit")
        return original_begin(*args, **kwargs)

    def commit(*args, **kwargs):
        events.append("receipt_commit")
        return original_commit(*args, **kwargs)

    async def provider_send(*_args, **_kwargs):
        events.append("send")
        return {"success": True, "message_id": "om_native"}

    monkeypatch.setattr(outbox, "begin_transport_request", begin)
    monkeypatch.setattr(outbox, "commit_transport_receipt", commit)
    monkeypatch.setattr(send_module, "_send_to_platform", provider_send)
    monkeypatch.setattr(
        "gateway.mirror.mirror_to_session",
        lambda *_args, **_kwargs: events.append("mirror") or True,
    )

    result = _send(after_send=lambda _context: events.append("after_send"))

    assert result["success"] is True
    assert result["transport_outcome"] == "confirmed"
    assert result["message_id"] == "om_native"
    assert result["transport_request_id"] == "request-send-1"
    assert result["transport_receipt_id"].startswith("transport-receipt:")
    assert result["mirrored"] is True
    assert events == ["request_commit", "send", "receipt_commit", "after_send", "mirror"]


def test_confirmed_send_with_receipt_commit_failure_is_indeterminate_and_skips_after_send(
    standalone_send, monkeypatch
):
    events = []
    send = AsyncMock(return_value={"success": True, "message_id": "om_native"})
    monkeypatch.setattr(send_module, "_send_to_platform", send)
    monkeypatch.setattr(
        "gateway.transport_outbox.commit_transport_receipt",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("disk full")),
    )
    monkeypatch.setattr(
        "gateway.mirror.mirror_to_session",
        lambda *_args, **_kwargs: events.append("mirror") or True,
    )

    result = _send(after_send=lambda _context: events.append("after_send"))

    assert result["success"] is False
    assert result["indeterminate"] is True
    assert result["transport_request_id"] == "request-send-1"
    assert "after send outcome confirmed" in result["error"]
    assert result["transport_outcome"] == "indeterminate"
    send.assert_awaited_once()
    assert events == []


def test_request_commit_failure_prevents_transport(standalone_send, monkeypatch):
    send = AsyncMock(return_value={"success": True})
    monkeypatch.setattr(send_module, "_send_to_platform", send)
    monkeypatch.setattr(
        "gateway.transport_outbox.begin_transport_request",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("read only")),
    )

    result = _send()

    assert result["success"] is False
    assert "request commit failed" in result["error"].lower()
    send.assert_not_awaited()


def test_duplicate_confirmed_request_returns_receipt_without_resending(
    standalone_send, monkeypatch
):
    send = AsyncMock(return_value={"success": True, "message_id": "om_once"})
    monkeypatch.setattr(send_module, "_send_to_platform", send)
    monkeypatch.setattr("gateway.mirror.mirror_to_session", lambda *_args, **_kwargs: False)

    first = _send()
    duplicate = _send()

    assert first["success"] is True
    assert duplicate["success"] is True
    assert duplicate["idempotent"] is True
    assert duplicate["message_id"] == "om_once"
    assert duplicate["transport_receipt_id"] == first["transport_receipt_id"]
    assert send.await_count == 1


def test_provider_definitive_rejection_appends_receipt_and_skips_after_send(
    standalone_send, monkeypatch
):
    events = []
    send = AsyncMock(
        return_value={
            "success": False,
            "error": "provider rejected",
            "transport_outcome": "definitively_rejected",
        }
    )
    monkeypatch.setattr(send_module, "_send_to_platform", send)

    result = _send(after_send=lambda _context: events.append("after_send"))

    assert result["error"] == "provider rejected"
    assert result["transport_outcome"] == "definitively_rejected"
    assert events == []
    from gateway.transport_outbox import verify_transport_receipt

    verified = verify_transport_receipt(_transport_request(), home=standalone_send)
    assert verified["status"] == "definitively_rejected"
    assert verified["verified"] is False


def test_provider_exception_leaves_indeterminate_request_and_cannot_resend(
    standalone_send, monkeypatch
):
    send = AsyncMock(side_effect=TimeoutError("provider timeout"))
    monkeypatch.setattr(send_module, "_send_to_platform", send)

    first = _send()
    duplicate = _send()

    assert first["success"] is False
    assert first["indeterminate"] is True
    assert first["transport_outcome"] == "indeterminate"
    assert first["transport_receipt_id"].startswith("transport-receipt:")
    assert first["transport_request_id"] == "request-send-1"
    assert duplicate["success"] is False
    assert duplicate["indeterminate"] is True
    assert send.await_count == 1


def test_unclassified_provider_error_is_indeterminate_not_rejected(
    standalone_send, monkeypatch
):
    send = AsyncMock(return_value={"success": False, "error": "provider timeout"})
    monkeypatch.setattr(send_module, "_send_to_platform", send)

    result = _send()

    assert result["success"] is False
    assert result["transport_outcome"] == "indeterminate"
    assert result["indeterminate"] is True


def test_partial_chunk_failure_commits_indeterminate_ids_and_skips_after_send(
    standalone_send, monkeypatch
):
    events = []
    send = AsyncMock(
        return_value={
            "success": False,
            "error": "chunk 3 failed",
            "transport_outcome": "indeterminate",
            "chunk_results": [
                {"success": True, "message_id": "chunk-1"},
                {"success": True, "message_id": "chunk-2"},
                {"success": False, "error": "chunk 3 failed"},
            ],
        }
    )
    monkeypatch.setattr(send_module, "_send_to_platform", send)

    result = _send(after_send=lambda _context: events.append("after_send"))

    assert result["success"] is False
    assert result["indeterminate"] is True
    assert result["transport_outcome"] == "indeterminate"
    assert {
        item["value"] for item in result["transport_receipt"]["native_ids"]
    } == {"chunk-1", "chunk-2"}
    assert events == []


def test_transport_neutral_chunk_router_retains_ids_before_partial_failure(
    monkeypatch
):
    from gateway.platform_registry import platform_registry
    from gateway.platforms.base import BasePlatformAdapter

    platform = Platform.MATTERMOST
    pconfig = SimpleNamespace(enabled=True, token="token", extra={})
    entry = SimpleNamespace(max_message_length=2)
    send = AsyncMock(
        side_effect=[
            {"success": True, "outbox_id": "native-1"},
            {"success": True, "message_id": "native-2"},
            {"success": False, "error": "chunk rejected"},
        ]
    )
    monkeypatch.setattr(platform_registry, "get", lambda _name: entry)
    monkeypatch.setattr(
        BasePlatformAdapter,
        "truncate_message",
        staticmethod(lambda *_args, **_kwargs: ["one", "two", "three"]),
    )
    monkeypatch.setattr(send_module, "_send_via_adapter", send)

    result = asyncio.run(
        send_module._send_to_platform(platform, pconfig, "admin", "long message")
    )

    assert result["success"] is False
    assert result["transport_outcome"] == "indeterminate"
    assert [attempt.get("outbox_id") or attempt.get("message_id") for attempt in result["chunk_results"][:2]] == [
        "native-1",
        "native-2",
    ]
    assert send.await_count == 3
