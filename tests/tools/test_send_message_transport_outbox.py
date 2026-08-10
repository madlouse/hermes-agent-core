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
    monkeypatch.setattr(send_module, "_send_authorized_to_platform", provider_send)
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
    monkeypatch.setattr(send_module, "_send_authorized_to_platform", send)
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
    monkeypatch.setattr(send_module, "_send_authorized_to_platform", send)
    monkeypatch.setattr(
        "gateway.transport_outbox.begin_transport_request",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("read only")),
    )

    result = _send()

    assert result["success"] is False
    assert "request commit failed" in result["error"].lower()
    send.assert_not_awaited()


def test_trusted_transport_request_rejects_media_before_provider(
    standalone_send, monkeypatch, tmp_path
):
    media = tmp_path / "attachment.txt"
    media.write_text("not authorized", encoding="utf-8")
    send = AsyncMock(return_value={"success": True})
    monkeypatch.setattr(send_module, "_send_authorized_to_platform", send)

    result = json.loads(
        send_module.send_message_tool(
            {
                "action": "send",
                "target": "feishu:oc_admin",
                "message": f"{CONTENT}\nMEDIA:{media}",
                "transport_request": _transport_request("request-media"),
            }
        )
    )

    assert "one non-empty text send" in result["error"]
    send.assert_not_awaited()


def test_duplicate_confirmed_request_returns_receipt_without_resending(
    standalone_send, monkeypatch
):
    send = AsyncMock(return_value={"success": True, "message_id": "om_once"})
    monkeypatch.setattr(send_module, "_send_authorized_to_platform", send)
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
    monkeypatch.setattr(send_module, "_send_authorized_to_platform", send)

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
    monkeypatch.setattr(send_module, "_send_authorized_to_platform", send)

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
    monkeypatch.setattr(send_module, "_send_authorized_to_platform", send)

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
    monkeypatch.setattr(send_module, "_send_authorized_to_platform", send)

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


def test_strict_transport_request_uses_registered_out_of_process_authority_sender(
    monkeypatch,
):
    from gateway.platform_registry import platform_registry

    pconfig = SimpleNamespace(enabled=True, token="token", extra={})
    strict = AsyncMock(return_value={"success": True, "message_id": "om-strict"})
    monkeypatch.setattr("gateway.run._gateway_runner_ref", lambda: None)
    monkeypatch.setattr(
        platform_registry,
        "get",
        lambda _name: SimpleNamespace(standalone_authorized_sender_fn=strict),
    )

    result = asyncio.run(
        send_module._send_authorized_to_platform(
            Platform.FEISHU,
            pconfig,
            "oc_admin",
            CONTENT,
            transport_request_id="request-strict",
        )
    )

    assert result == {"success": True, "message_id": "om-strict"}
    strict.assert_awaited_once_with(
        pconfig,
        "oc_admin",
        CONTENT,
        thread_id=None,
        transport_request_id="request-strict",
    )


def test_strict_transport_request_rejects_unregistered_standalone_sender(monkeypatch):
    from gateway.platform_registry import platform_registry

    monkeypatch.setattr("gateway.run._gateway_runner_ref", lambda: None)
    monkeypatch.setattr(
        platform_registry,
        "get",
        lambda _name: SimpleNamespace(standalone_authorized_sender_fn=None),
    )

    result = asyncio.run(
        send_module._send_authorized_to_platform(
            Platform.TELEGRAM,
            SimpleNamespace(),
            "admin",
            CONTENT,
            transport_request_id="request-unsupported",
        )
    )

    assert result["success"] is False
    assert "conforming adapter" in result["error"]


def test_strict_transport_live_adapter_fail_closed_and_object_result(monkeypatch):
    unsupported = SimpleNamespace(supports_transport_authority=False)
    monkeypatch.setattr(
        "gateway.run._gateway_runner_ref",
        lambda: SimpleNamespace(adapters={Platform.FEISHU: unsupported}),
    )
    rejected = asyncio.run(
        send_module._send_authorized_to_platform(
            Platform.FEISHU,
            SimpleNamespace(),
            "oc_admin",
            CONTENT,
            transport_request_id="request-unsupported-live",
        )
    )
    assert rejected["success"] is False

    sender = AsyncMock(
        return_value=SimpleNamespace(
            success=True,
            message_id="om-live",
            error=None,
            raw_response={"code": 0},
            transport_outcome="confirmed",
        )
    )
    adapter = SimpleNamespace(
        supports_transport_authority=True,
        send_authorized=sender,
    )
    monkeypatch.setattr(
        "gateway.run._gateway_runner_ref",
        lambda: SimpleNamespace(adapters={Platform.FEISHU: adapter}),
    )
    result = asyncio.run(
        send_module._send_authorized_to_platform(
            Platform.FEISHU,
            SimpleNamespace(),
            "oc_admin",
            CONTENT,
            thread_id="omt-1",
            transport_request_id="request-live",
        )
    )
    assert result["message_id"] == "om-live"
    sender.assert_awaited_once_with(
        "oc_admin",
        CONTENT,
        metadata={"thread_id": "omt-1"},
        transport_request_id="request-live",
    )


def test_strict_transport_registry_lookup_exception_fails_closed(monkeypatch):
    from gateway.platform_registry import platform_registry

    monkeypatch.setattr(
        "gateway.run._gateway_runner_ref",
        lambda: (_ for _ in ()).throw(RuntimeError("runner unavailable")),
    )
    monkeypatch.setattr(
        platform_registry,
        "get",
        lambda _name: (_ for _ in ()).throw(RuntimeError("registry unavailable")),
    )
    result = asyncio.run(
        send_module._send_authorized_to_platform(
            Platform.FEISHU,
            SimpleNamespace(),
            "oc_admin",
            CONTENT,
            transport_request_id="request-registry-error",
        )
    )
    assert result["success"] is False
    assert "conforming adapter" in result["error"]


def test_trusted_sdk_response_is_json_safe_before_receipt_commit(
    standalone_send, monkeypatch
):
    sdk_response = SimpleNamespace(
        code=0,
        msg="ok",
        data=SimpleNamespace(message_id="om-sdk-direct"),
    )
    monkeypatch.setattr(
        send_module,
        "_send_authorized_to_platform",
        AsyncMock(
            return_value={
                "success": True,
                "message_id": "om-sdk-direct",
                "raw_response": sdk_response,
            }
        ),
    )

    result = _send("request-sdk-direct")

    assert result["success"] is True
    assert result["raw_response"] == {
        "code": 0,
        "msg": "ok",
        "data": {"message_id": "om-sdk-direct"},
    }
    assert result["transport_outcome"] == "confirmed"


def test_resident_authority_indeterminate_result_is_json_safe_and_unsuccessful(
    standalone_send, monkeypatch
):
    from gateway import outbound_boundary as ob

    request = _transport_request("request-resident-indeterminate")
    request["profile_id"] = "default"
    authority = {
        "schema_version": ob.DELIVERY_AUTHORITY_SCHEMA_VERSION,
        "required": True,
        "business_profile_id": "atlas",
        "request": request,
    }
    decision = ob.BoundaryDecision(
        transmit=True,
        decision="allow",
        content=CONTENT,
        reason="authorized",
        delivery_authority=authority,
    )
    monkeypatch.setattr(ob, "outbound_before_send_sync", lambda *_args, **_kwargs: decision)

    async def execute(**_kwargs):
        return ob.AuthorizedOutboundExecution(
            result={
                "success": True,
                "raw_response": SimpleNamespace(code=0, msg="accepted"),
            },
            outcome="indeterminate",
            request=request,
            receipt=None,
            provider_called=True,
        )

    monkeypatch.setattr(ob, "execute_authorized_outbound_send", execute)
    result = json.loads(
        send_module.send_message_tool(
            {"action": "send", "target": "feishu:oc_admin", "message": CONTENT}
        )
    )

    assert result["success"] is False
    assert result["transport_outcome"] == "indeterminate"
    assert result["raw_response"] == {"code": 0, "msg": "accepted"}
    assert "transport_receipt_id" not in result
