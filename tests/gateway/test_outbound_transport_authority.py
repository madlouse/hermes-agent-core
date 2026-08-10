from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timedelta, timezone

import pytest

from gateway import outbound_boundary as ob
from gateway.hooks import HookRegistry


def _selector(content: str = "请回复 1 确认") -> dict:
    now = datetime.now(timezone.utc)
    route = {
        "transport_id": "feishu",
        "channel_id": "chat-1",
        "thread_id": "",
    }
    return {
        "request_id": "request-1",
        "profile_id": "atlas",
        "frame_id": "frame-1",
        "notification_claim_id": "claim-1",
        "decision_route": route,
        "notification_route": route,
        "items_content_hash": "sha256:" + hashlib.sha256(b"items").hexdigest(),
        "visible_content_sha256": hashlib.sha256(content.encode()).hexdigest(),
        "claim_created_at": now.isoformat(),
        "claim_expires_at": (now + timedelta(hours=1)).isoformat(),
    }


def _context(content: str = "请回复 1 确认") -> dict:
    return ob.build_outbound_context(
        source_kind="gateway_notice",
        content=content,
        platform="feishu",
        chat_id="chat-1",
        profile_id="atlas",
        profile_path="/tmp/atlas",
        output_screening_required=True,
        looks_actionable=True,
    )


def Hooks(result: dict, capabilities: tuple[str, ...]) -> HookRegistry:
    registry = HookRegistry()

    def handler(_event_type, _context):
        return result

    registry._handlers[ob.BEFORE_SEND] = [handler]
    registry._handlers[ob.AFTER_SEND] = [handler]
    registry._handler_owners[id(handler)] = "outbound-actionable"
    registry._handler_capabilities[id(handler)] = frozenset(capabilities)
    return registry


def _authority_result(selector: dict | None = None) -> dict:
    return {
        "decision": "allow",
        "reason": "registered",
        "delivery_authority": {
            "schema_version": "transport-outbox-hook/v1",
            "required": True,
            "request": selector or _selector(),
        },
    }


def _post_send_result(*, authority: dict | None = None) -> dict:
    result = {
        "decision": "allow",
        "reason": "registered",
        "post_send": {"required": True},
    }
    if authority is not None:
        result["delivery_authority"] = authority
    return result


def _decision(result: dict, capabilities: tuple[str, ...]) -> ob.BoundaryDecision:
    return asyncio.run(ob.outbound_before_send(Hooks(result, capabilities), _context()))


def test_before_send_rejects_delivery_authority_without_capability():
    decision = _decision(_authority_result(), ("output-screening",))

    assert decision.transmit is False
    assert decision.reason == "untrusted_delivery_authority"


def test_before_send_rejects_required_post_send_without_delivery_authority():
    decision = _decision(
        _post_send_result(),
        ("output-screening", "transport-outbox-authority"),
    )

    assert decision.transmit is False
    assert decision.reason == "missing_delivery_authority"


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda request: request.pop("frame_id"), "invalid_delivery_authority"),
        (lambda request: request.__setitem__("frame_ids", ["frame-1", "frame-2"]), "multiple_delivery_frames"),
        (lambda request: request.__setitem__("profile_id", "yuange"), "delivery_authority_profile_mismatch"),
        (
            lambda request: request["notification_route"].__setitem__("channel_id", "chat-2"),
            "delivery_authority_route_mismatch",
        ),
        (
            lambda request: request.__setitem__("visible_content_sha256", "0" * 64),
            "delivery_authority_content_mismatch",
        ),
    ],
)
def test_before_send_rejects_invalid_or_tampered_authority(mutate, reason):
    request = _selector()
    mutate(request)

    decision = _decision(
        _authority_result(request),
        ("output-screening", "transport-outbox-authority"),
    )

    assert decision.transmit is False
    assert decision.reason == reason


def test_authorized_send_begin_failure_never_calls_provider(monkeypatch):
    decision = _decision(
        _authority_result(),
        ("output-screening", "transport-outbox-authority"),
    )
    calls: list[str] = []
    monkeypatch.setattr(
        "gateway.transport_outbox.begin_transport_request",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("disk unavailable")),
    )

    with pytest.raises(ob.OutboundDeliveryAuthorityError, match="begin failed"):
        ob.execute_authorized_outbound_send_sync(
            hooks=Hooks({}, ()),
            context=_context(),
            decision=decision,
            send=lambda: calls.append("provider"),
        )

    assert calls == []


def test_confirmed_duplicate_calls_after_send_without_provider(monkeypatch):
    decision = _decision(
        _authority_result(),
        ("output-screening", "transport-outbox-authority"),
    )
    calls: list[str] = []
    receipt = {
        "receipt_id": "receipt-1",
        "request_id": "request-1",
        "status": "confirmed",
        "native_ids": [{"kind": "message_id", "value": "om-1"}],
    }
    monkeypatch.setattr(
        "gateway.transport_outbox.begin_transport_request",
        lambda *args, **kwargs: {
            "state": "confirmed",
            "request": _selector(),
            "receipt": receipt,
        },
    )
    monkeypatch.setattr(
        ob,
        "outbound_after_send_sync",
        lambda hooks, context: calls.append(context),
    )

    execution = ob.execute_authorized_outbound_send_sync(
        hooks=Hooks({}, ()),
        context=_context(),
        decision=decision,
        send=lambda: calls.append("provider"),
    )

    assert execution.provider_called is False
    assert execution.recovered is True
    assert calls[0]["transport_request_id"] == "request-1"
    assert calls[0]["transport_receipt_id"] == "receipt-1"
    assert calls[0]["delivery_authority"] == decision.delivery_authority
    assert calls[0]["delivery_authority_selector"] == execution.request


@pytest.mark.parametrize("outcome", ["indeterminate", "definitively_rejected"])
def test_existing_non_confirmed_request_never_resends(monkeypatch, outcome):
    decision = _decision(
        _authority_result(),
        ("output-screening", "transport-outbox-authority"),
    )
    calls: list[str] = []
    monkeypatch.setattr(
        "gateway.transport_outbox.begin_transport_request",
        lambda *args, **kwargs: {"state": outcome, "request": _selector()},
    )

    with pytest.raises(ob.OutboundDeliveryAuthorityError, match=outcome):
        ob.execute_authorized_outbound_send_sync(
            hooks=Hooks({}, ()),
            context=_context(),
            decision=decision,
            send=lambda: calls.append("provider"),
        )

    assert calls == []


def test_provider_rejection_commits_receipt_but_skips_after_send(monkeypatch):
    decision = _decision(
        _authority_result(),
        ("output-screening", "transport-outbox-authority"),
    )
    calls: list[object] = []
    monkeypatch.setattr(
        "gateway.transport_outbox.begin_transport_request",
        lambda *args, **kwargs: {"state": "new", "request": _selector()},
    )
    monkeypatch.setattr(
        "gateway.transport_outbox.classify_transport_outcome",
        lambda result: "definitively_rejected",
    )
    monkeypatch.setattr(
        "gateway.transport_outbox.commit_transport_receipt",
        lambda request_id, result, outcome, **kwargs: calls.append((request_id, outcome))
        or {"receipt_id": "receipt-rejected", "status": outcome},
    )
    monkeypatch.setattr(
        ob,
        "outbound_after_send_sync",
        lambda hooks, context: calls.append("after"),
    )

    execution = ob.execute_authorized_outbound_send_sync(
        hooks=Hooks({}, ()),
        context=_context(),
        decision=decision,
        send=lambda: {"success": False, "transport_outcome": "definitively_rejected"},
    )

    assert execution.outcome == "definitively_rejected"
    assert calls == [("request-1", "definitively_rejected")]


def test_receipt_commit_failure_skips_after_send(monkeypatch):
    decision = _decision(
        _authority_result(),
        ("output-screening", "transport-outbox-authority"),
    )
    calls: list[str] = []
    monkeypatch.setattr(
        "gateway.transport_outbox.begin_transport_request",
        lambda *args, **kwargs: {"state": "new", "request": _selector()},
    )
    monkeypatch.setattr(
        "gateway.transport_outbox.commit_transport_receipt",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("commit failed")),
    )
    monkeypatch.setattr(
        ob,
        "outbound_after_send_sync",
        lambda hooks, context: calls.append("after"),
    )

    with pytest.raises(ob.OutboundDeliveryAuthorityError, match="receipt commit failed"):
        ob.execute_authorized_outbound_send_sync(
            hooks=Hooks({}, ()),
            context=_context(),
            decision=decision,
            send=lambda: {"success": True, "message_id": "om-1"},
        )

    assert calls == []


def test_confirmed_send_passes_full_authority_to_after_send(monkeypatch):
    decision = _decision(
        _authority_result(),
        ("output-screening", "transport-outbox-authority"),
    )
    contexts: list[dict] = []
    monkeypatch.setattr(
        "gateway.transport_outbox.begin_transport_request",
        lambda *args, **kwargs: {"state": "new", "request": _selector()},
    )
    monkeypatch.setattr(
        "gateway.transport_outbox.commit_transport_receipt",
        lambda request_id, result, outcome, **kwargs: {
            "receipt_id": "receipt-1",
            "request_id": request_id,
            "status": outcome,
            "native_ids": [{"kind": "message_id", "value": "om-1"}],
        },
    )
    monkeypatch.setattr(
        ob,
        "outbound_after_send_sync",
        lambda hooks, context: contexts.append(context),
    )

    execution = ob.execute_authorized_outbound_send_sync(
        hooks=Hooks({}, ()),
        context=_context(),
        decision=decision,
        send=lambda: {"success": True, "message_id": "om-1"},
    )

    assert execution.outcome == "confirmed"
    assert contexts[0]["transport_request_id"] == "request-1"
    assert contexts[0]["transport_receipt_id"] == "receipt-1"
    assert contexts[0]["delivery_authority"] == decision.delivery_authority
    assert contexts[0]["delivery_authority_selector"] == execution.request
    assert contexts[0]["send_result"]["message_id"] == "om-1"
