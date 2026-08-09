import asyncio
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import PlatformConfig
from gateway.config import Platform
from gateway.platforms.base import MessageEvent, RecoveryDeliveryContext, SendResult
from gateway.run import GatewayRunner
from gateway.session import SessionSource, build_session_key
from hermes_cli.plugins import VALID_HOOKS
from plugins.platforms.feishu import adapter as feishu


def item(
    message_id,
    position_ms,
    *,
    text="hello",
    channel_id="oc_chat",
    sender_id="ou_user",
    sender_id_type="open_id",
    chat_type="p2p",
):
    return SimpleNamespace(
        message_id=message_id,
        chat_id=channel_id,
        create_time=str(position_ms),
        msg_type="text",
        body=SimpleNamespace(content=json.dumps({"text": text})),
        sender=SimpleNamespace(id=sender_id, id_type=sender_id_type, sender_type="user"),
        chat_type=chat_type,
    )


def response(items=(), *, has_more=False, page_token="", success=True, code=0):
    return SimpleNamespace(
        success=lambda: success,
        code=code,
        data=SimpleNamespace(items=list(items), has_more=has_more, page_token=page_token),
    )


def recovery_adapter(responses):
    adapter = feishu.FeishuAdapter(PlatformConfig())
    adapter._app_id = "cli_recovery"
    adapter._client = SimpleNamespace(im=SimpleNamespace(v1=SimpleNamespace(message=SimpleNamespace(list=object()))))
    adapter._run_blocking = AsyncMock(side_effect=list(responses))
    adapter.get_chat_info = AsyncMock(return_value={"name": "Admin DM", "type": "dm"})
    adapter._recovery_temp_dir = tempfile.TemporaryDirectory()
    adapter._recovery_state_path = Path(adapter._recovery_temp_dir.name) / "recovery-state.json"
    adapter._dedup_state_path = Path(adapter._recovery_temp_dir.name) / "feishu-seen-message-ids.json"
    return adapter


_AUTHORIZATION_FINGERPRINT = "sha256:" + "a" * 64


async def fetch_history(adapter, **kwargs):
    checkpoint = str(kwargs.get("after_cursor") or "")
    if checkpoint and not adapter._recovery_state_path.exists():
        channel_id = str(kwargs.get("channel_id") or "")
        channel_key = feishu.hashlib.sha256(channel_id.encode("utf-8")).hexdigest()
        adapter._recovery_state_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "channels": {
                        channel_key: {
                            "cursor": checkpoint,
                            "revision": 1,
                            "consumed_execution_ids": [],
                            "profile_fingerprint": feishu.hashlib.sha256(b"yuange").hexdigest(),
                            "source_instance_fingerprint": feishu.hashlib.sha256(b"cli_recovery").hexdigest(),
                            "config_fingerprint": adapter._recovery_config_fingerprint(),
                            "authorization_fingerprint": _AUTHORIZATION_FINGERPRINT,
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
    return await adapter.fetch_recovery_history(
        profile_id="yuange",
        authorization_fingerprint=_AUTHORIZATION_FINGERPRINT,
        **kwargs,
    )


def recovery_runner(adapter, *, profile="yuange"):
    return SimpleNamespace(
        _adapter_for_source=lambda _source: adapter,
        _resolve_profile_home_for_source=lambda _source: Path(
            f"/tmp/hermes-profile-{profile}"
        ),
        _profile_name_for_source=lambda _source: profile,
        _active_profile_name=lambda: profile,
    )


def bind_adapter_platform(adapter):
    adapter.platform = Platform.FEISHU
    return adapter


def test_fetch_recovery_history_pages_and_applies_strict_total_order():
    checkpoint = feishu._recovery_cursor(1_700_000_000_000, "om_1")
    adapter = recovery_adapter(
        [
            response(
                [
                    item("om_0", 1_700_000_000_000, text="before tie"),
                    item("om_2", 1_700_000_000_000, text="after tie"),
                ],
                has_more=True,
                page_token="next",
            ),
            response([item("om_3", 1_700_000_001_000, text="later")]),
        ]
    )

    result = asyncio.run(
        fetch_history(adapter,
            channel_id="oc_chat",
            after_cursor=checkpoint,
            now_ms=1_700_000_002_000,
        )
    )

    assert result["status"] == "ok" and result["complete"] is True
    assert result["chat_type"] == "direct"
    assert [message["inbound_uid"] for message in result["messages"]] == ["om_2", "om_3"]
    assert result["next_cursor"] == feishu._recovery_cursor(1_700_000_001_000, "om_3")
    assert adapter._run_blocking.await_count == 2


def test_first_scan_recovers_the_bounded_bootstrap_window():
    adapter = recovery_adapter([response([item("offline", 1_699_999_999_500)])])

    result = asyncio.run(fetch_history(adapter, channel_id="oc_chat", now_ms=1_700_000_000_000))

    assert result["status"] == "ok"
    assert [value["inbound_uid"] for value in result["messages"]] == ["offline"]
    assert result["next_cursor"] == feishu._recovery_cursor(1_699_999_999_500, "offline")


def test_first_scan_excludes_messages_before_the_bounded_bootstrap_window():
    too_old = 1_700_000_000_000 - feishu._FEISHU_RECOVERY_BOOTSTRAP_LOOKBACK_MS - 1
    adapter = recovery_adapter([response([item("old", too_old)])])

    result = asyncio.run(fetch_history(adapter, channel_id="oc_chat", now_ms=1_700_000_000_000))

    assert result["messages"] == []
    assert result["next_cursor"] == feishu._recovery_cursor(1_700_000_000_000)


@pytest.mark.parametrize(
    ("responses", "reason"),
    [
        ([response(success=False, code=99991672)], "history_source_permission_or_api_failure"),
        ([response([], has_more=True, page_token="")], "history_pagination_incomplete"),
    ],
)
def test_incomplete_or_unauthorized_scan_never_returns_a_checkpoint(responses, reason):
    adapter = recovery_adapter(responses)

    result = asyncio.run(
        fetch_history(adapter,
            channel_id="oc_chat",
            after_cursor=feishu._recovery_cursor(1_700_000_000_000),
            now_ms=1_700_000_001_000,
        )
    )

    assert result == {
        **result,
        "status": "blocked",
        "complete": False,
        "reason": reason,
    }
    assert "next_cursor" not in result


def test_conflicting_duplicate_message_id_blocks_scan():
    adapter = recovery_adapter(
        [response([item("om_dup", 1_700_000_001_000, text="first"), item("om_dup", 1_700_000_001_000, text="changed")])]
    )

    result = asyncio.run(
        fetch_history(adapter,
            channel_id="oc_chat",
            after_cursor=feishu._recovery_cursor(1_700_000_000_000),
            now_ms=1_700_000_002_000,
        )
    )

    assert result["reason"] == "duplicate_message_conflict"
    assert "next_cursor" not in result


def test_source_identity_drift_blocks_scan():
    adapter = recovery_adapter([response([])])

    async def drift(_call, _request):
        adapter._app_id = "cli_changed"
        return response([])

    adapter._run_blocking = drift
    result = asyncio.run(
        fetch_history(adapter,
            channel_id="oc_chat",
            after_cursor=feishu._recovery_cursor(1_700_000_000_000),
            now_ms=1_700_000_001_000,
        )
    )

    assert result["reason"] == "history_source_identity_drift"
    assert "next_cursor" not in result


def test_source_config_drift_blocks_scan():
    adapter = recovery_adapter([response([])])

    async def drift(_call, _request):
        adapter._group_policy = "changed-during-scan"
        return response([])

    adapter._run_blocking = drift
    result = asyncio.run(
        fetch_history(adapter,
            channel_id="oc_chat",
            after_cursor=feishu._recovery_cursor(1_700_000_000_000),
            now_ms=1_700_000_001_000,
        )
    )

    assert result["reason"] == "history_source_config_drift"
    assert "next_cursor" not in result


def test_history_fact_marks_self_and_unsupported_messages():
    adapter = recovery_adapter([])
    unsupported = item("om_image", 1_700_000_001_000, sender_id="cli_recovery")
    unsupported.msg_type = "image"
    unsupported.body = SimpleNamespace(content=json.dumps({"image_key": "img_1"}))
    unsupported.sender.sender_type = "app"

    fact = adapter._recovery_fact(
        unsupported,
        expected_channel_id="oc_chat",
        container_chat_type="direct",
    )

    assert fact["is_bot_self"] is True
    assert fact["supported"] is False


def test_history_fact_infers_bot_from_app_id_when_sender_type_is_absent():
    adapter = recovery_adapter([])
    bot_message = item("om_shutdown", 1_700_000_001_000, sender_id="cli_recovery")
    bot_message.sender.id_type = "app_id"
    bot_message.sender.sender_type = ""

    fact = adapter._recovery_fact(
        bot_message,
        expected_channel_id="oc_chat",
        container_chat_type="direct",
    )

    assert fact["is_bot"] is True
    assert fact["is_bot_self"] is True


def test_recovery_event_preserves_native_identity_and_channel():
    adapter = recovery_adapter([])
    adapter.get_chat_info = AsyncMock(return_value={"name": "Admin DM", "type": "dm"})
    adapter._resolve_sender_profile = AsyncMock(
        return_value={"user_id": "ou_user", "user_name": "Admin", "user_id_alt": None}
    )
    fact = {
        "inbound_uid": "om_recovered",
        "channel_id": "oc_chat",
        "sender_uid": "ou_user",
        "text": "recover me",
        "chat_type": "p2p",
        "is_bot": False,
        "position_ms": 1_700_000_000_000,
    }

    event = asyncio.run(adapter.recovery_event(fact))

    assert event.message_id == "om_recovered"
    assert event.source.chat_id == "oc_chat"
    assert event.text == "recover me"
    assert event.raw_message["_hermes_history_recovery"]["inbound_uid"] == "om_recovered"


def test_gateway_waits_for_recovered_turn_before_checkpoint_owner_can_continue():
    source = SessionSource(platform=Platform.FEISHU, chat_id="oc_chat", user_id="ou_user")
    event = MessageEvent(text="recover me", source=source, message_id="om_recovered")
    adapter = bind_adapter_platform(
        SimpleNamespace(config=SimpleNamespace(extra={}), _session_tasks={})
    )
    completed = []

    async def complete_delivery():
        await asyncio.sleep(0)
        completed.append("delivered")

    async def handle_message(recovered_event):
        async def finish():
            await asyncio.sleep(0)
            await recovered_event.recovery_delivery.complete()
            recovered_event.recovery_delivery.future.set_result({"status": "completed"})

        key = build_session_key(source)
        adapter._session_tasks[key] = asyncio.create_task(finish())

    adapter.handle_message = handle_message
    result = asyncio.run(
        GatewayRunner.dispatch_recovered_message(
            recovery_runner(adapter),
            adapter,
            event,
            on_delivery_complete=complete_delivery,
        )
    )

    assert completed == ["delivered"]
    assert result == {"status": "completed", "message_id": "om_recovered"}
    assert getattr(event, "_hermes_startup_restore_replay") is True
    assert event.source.force_final_delivery is True
    assert "gateway_startup_recovery" in VALID_HOOKS


def test_gateway_recovered_dispatch_without_background_task_blocks_checkpoint():
    source = SessionSource(platform=Platform.FEISHU, chat_id="oc_chat", user_id="ou_user")
    event = MessageEvent(text="recover me", source=source, message_id="om_recovered")
    adapter = bind_adapter_platform(
        SimpleNamespace(
            config=SimpleNamespace(extra={}),
            _session_tasks=object(),
            handle_message=AsyncMock(return_value=None),
        )
    )
    result = asyncio.run(
        GatewayRunner.dispatch_recovered_message(
            recovery_runner(adapter), adapter, event, on_delivery_complete=lambda: None
        )
    )

    adapter.handle_message.assert_awaited_once_with(event)
    assert result == {"status": "blocked", "reason": "recovered_turn_not_started"}


def test_gateway_recovered_dispatch_requires_delivery_completion_owner():
    source = SessionSource(platform=Platform.FEISHU, chat_id="oc_chat", user_id="ou_user")
    event = MessageEvent(text="recover me", source=source, message_id="om_recovered")
    adapter = bind_adapter_platform(
        SimpleNamespace(
            config=SimpleNamespace(extra={}),
            _session_tasks={},
            handle_message=AsyncMock(return_value=None),
        )
    )
    result = asyncio.run(
        GatewayRunner.dispatch_recovered_message(
            recovery_runner(adapter), adapter, event, on_delivery_complete=None
        )
    )

    assert result == {"status": "blocked", "reason": "history_delivery_completion_missing"}
    adapter.handle_message.assert_not_awaited()


def test_gateway_recovered_dispatch_requires_confirmed_delivery_after_turn():
    source = SessionSource(platform=Platform.FEISHU, chat_id="oc_chat", user_id="ou_user")
    event = MessageEvent(text="recover me", source=source, message_id="om_recovered")
    adapter = bind_adapter_platform(
        SimpleNamespace(config=SimpleNamespace(extra={}), _session_tasks={})
    )
    async def handle_message(_event):
        key = build_session_key(source)
        adapter._session_tasks[key] = asyncio.create_task(asyncio.sleep(0))

    adapter.handle_message = handle_message
    result = asyncio.run(
        GatewayRunner.dispatch_recovered_message(
            recovery_runner(adapter), adapter, event, on_delivery_complete=lambda: None
        )
    )

    assert result == {"status": "blocked", "reason": "recovered_delivery_not_confirmed"}


def test_gateway_recovered_dispatch_fails_closed_on_adapter_owner_mismatch():
    source = SessionSource(
        platform=Platform.FEISHU, chat_id="oc_chat", user_id="ou_user"
    )
    event = MessageEvent(text="recover me", source=source, message_id="om_recovered")
    adapter = bind_adapter_platform(
        SimpleNamespace(
            config=SimpleNamespace(extra={}),
            _session_tasks={},
            handle_message=AsyncMock(return_value=None),
        )
    )
    runner = recovery_runner(object())

    result = asyncio.run(
        GatewayRunner.dispatch_recovered_message(
            runner, adapter, event, on_delivery_complete=lambda: None
        )
    )

    assert result == {
        "status": "blocked",
        "reason": "history_delivery_owner_mismatch",
    }
    adapter.handle_message.assert_not_awaited()


def test_gateway_recovery_idempotency_binds_profile_home_and_is_stable():
    adapter = bind_adapter_platform(
        SimpleNamespace(config=SimpleNamespace(extra={}), _session_tasks={})
    )
    observed = []

    async def handle_message(event):
        observed.append(event.recovery_delivery)

        async def finish():
            event.recovery_delivery.future.set_result({"status": "completed"})

        adapter._session_tasks[build_session_key(event.source)] = asyncio.create_task(
            finish()
        )

    adapter.handle_message = handle_message

    async def dispatch(profile):
        source = SessionSource(
            platform=Platform.FEISHU,
            chat_id="oc_chat",
            user_id="ou_user",
        )
        event = MessageEvent(
            text="recover me", source=source, message_id="om_recovered"
        )
        return await GatewayRunner.dispatch_recovered_message(
            recovery_runner(adapter, profile=profile),
            adapter,
            event,
            on_delivery_complete=lambda: None,
        )

    assert asyncio.run(dispatch("atlas"))["status"] == "completed"
    assert asyncio.run(dispatch("atlas"))["status"] == "completed"
    assert asyncio.run(dispatch("yuange"))["status"] == "completed"
    assert observed[0].idempotency_key == observed[1].idempotency_key
    assert observed[0].idempotency_key != observed[2].idempotency_key
    assert observed[0].profile_home_sha256 != observed[2].profile_home_sha256


def test_recovered_turn_completes_only_after_confirmed_platform_delivery(monkeypatch):
    async def allow(_hooks, context):
        return SimpleNamespace(
            transmit=True,
            content=context["content"],
            raw={"decision": "allow"},
            decision="allow",
            reason="test_output_safe",
        )

    monkeypatch.setattr("gateway.outbound_boundary.outbound_before_send", allow)
    adapter = feishu.FeishuAdapter(PlatformConfig())
    adapter.config.typing_indicator = False
    adapter.set_message_handler(lambda _event: asyncio.sleep(0, result="business result"))
    adapter.send = AsyncMock(return_value=SendResult(success=True, message_id="om_reply"))
    source = SessionSource(platform=Platform.FEISHU, chat_id="oc_chat", user_id="ou_user")
    event = MessageEvent(text="recover me", source=source, message_id="om_recovered")
    completed = []

    async def scenario():
        async def complete_delivery():
            await asyncio.sleep(0)
            completed.append("durable")

        event.recovery_delivery = RecoveryDeliveryContext(
            complete=complete_delivery,
            idempotency_key="stable-key",
            future=asyncio.get_running_loop().create_future(),
        )
        await adapter._process_message_background(event, build_session_key(source))
        return event.recovery_delivery.future.result()

    assert asyncio.run(scenario()) == {"status": "completed"}
    assert completed == ["durable"]
    assert adapter.send.await_args.kwargs["metadata"]["hermes_delivery_idempotency_key"] == "stable-key"


def test_recovered_turn_send_failure_never_completes_history(monkeypatch):
    async def allow(_hooks, context):
        return SimpleNamespace(
            transmit=True,
            content=context["content"],
            raw={"decision": "allow"},
            decision="allow",
            reason="test_output_safe",
        )

    monkeypatch.setattr("gateway.outbound_boundary.outbound_before_send", allow)
    adapter = feishu.FeishuAdapter(PlatformConfig())
    adapter.config.typing_indicator = False
    adapter.set_message_handler(lambda _event: asyncio.sleep(0, result="business result"))
    adapter.send = AsyncMock(return_value=SendResult(success=False, error="network"))
    source = SessionSource(platform=Platform.FEISHU, chat_id="oc_chat", user_id="ou_user")
    event = MessageEvent(text="recover me", source=source, message_id="om_recovered")
    completed = []

    async def scenario():
        event.recovery_delivery = RecoveryDeliveryContext(
            complete=lambda: completed.append("durable"),
            idempotency_key="stable-key",
            future=asyncio.get_running_loop().create_future(),
        )
        await adapter._process_message_background(event, build_session_key(source))
        return event.recovery_delivery.future.result()

    result = asyncio.run(scenario())
    assert result == {"status": "blocked", "reason": "recovered_delivery_failed"}
    assert completed == []


def test_recovered_turn_blocks_when_durable_completion_fails(monkeypatch):
    async def allow(_hooks, context):
        return SimpleNamespace(
            transmit=True,
            content=context["content"],
            raw={"decision": "allow"},
            decision="allow",
            reason="test_output_safe",
        )

    monkeypatch.setattr("gateway.outbound_boundary.outbound_before_send", allow)
    adapter = feishu.FeishuAdapter(PlatformConfig())
    adapter.config.typing_indicator = False
    adapter.set_message_handler(lambda _event: asyncio.sleep(0, result="business result"))
    adapter.send = AsyncMock(return_value=SendResult(success=True, message_id="om_reply"))
    source = SessionSource(platform=Platform.FEISHU, chat_id="oc_chat", user_id="ou_user")
    event = MessageEvent(text="recover me", source=source, message_id="om_recovered")

    def fail_completion():
        raise RuntimeError("inbox unavailable")

    async def scenario():
        event.recovery_delivery = RecoveryDeliveryContext(
            complete=fail_completion,
            idempotency_key="stable-key",
            future=asyncio.get_running_loop().create_future(),
        )
        await adapter._process_message_background(event, build_session_key(source))
        return event.recovery_delivery.future.result()

    assert asyncio.run(scenario()) == {
        "status": "blocked",
        "reason": "history_delivery_completion_failed",
        "error_type": "RuntimeError",
    }


def test_feishu_recovery_message_uuid_is_stable_per_delivery_part():
    adapter = recovery_adapter([])
    adapter._client.im.v1.message.reply = object()
    captured = []
    adapter._build_reply_message_body = lambda **kwargs: captured.append(kwargs) or kwargs
    adapter._build_reply_message_request = lambda message_id, body: (message_id, body)
    adapter._run_blocking = AsyncMock(return_value=SimpleNamespace(success=lambda: True))

    async def send(part, text="business result"):
        await adapter._send_raw_message(
            chat_id="oc_chat",
            msg_type="text",
            payload=json.dumps({"text": text}),
            reply_to="om_source",
            metadata={
                "hermes_delivery_idempotency_key": "stable-key",
                "hermes_delivery_part": part,
            },
        )

    asyncio.run(send("text:0"))
    asyncio.run(send("text:0", "regenerated business result"))
    asyncio.run(send("text:1"))

    assert captured[0]["uuid_value"] == captured[1]["uuid_value"]
    assert captured[0]["uuid_value"] != captured[2]["uuid_value"]


def test_feishu_multi_chunk_send_fails_if_any_required_chunk_fails():
    adapter = recovery_adapter([])
    adapter.truncate_message = lambda _content, _limit: ["first", "second"]
    failed = SimpleNamespace(success=lambda: False, code=500, msg="failed")
    succeeded = SimpleNamespace(success=lambda: True, data=SimpleNamespace(message_id="om_2"))
    adapter._feishu_send_with_retry = AsyncMock(side_effect=[failed, succeeded])

    result = asyncio.run(
        adapter.send(
            "oc_chat",
            "business result",
            metadata={"hermes_delivery_idempotency_key": "stable-key"},
        )
    )

    assert result.success is False
    assert adapter._feishu_send_with_retry.await_count == 1


def test_feishu_multi_chunk_send_confirms_every_required_chunk():
    adapter = recovery_adapter([])
    adapter.truncate_message = lambda _content, _limit: ["first", "second"]
    adapter._feishu_send_with_retry = AsyncMock(
        side_effect=[
            SimpleNamespace(success=lambda: True, data=SimpleNamespace(message_id="om_1")),
            SimpleNamespace(success=lambda: True, data=SimpleNamespace(message_id="om_2")),
        ]
    )

    result = asyncio.run(adapter.send("oc_chat", "business result", metadata=None))

    assert result.success is True
    assert result.message_id == "om_2"
    assert adapter._feishu_send_with_retry.await_count == 2


def test_confirmed_recovery_is_persisted_for_live_transport_dedup():
    adapter = recovery_adapter([])

    adapter.remember_recovered_message("om_recovered")
    adapter.remember_recovered_message("om_recovered")

    assert adapter._is_duplicate("om_recovered") is True
    persisted = json.loads(adapter._dedup_state_path.read_text(encoding="utf-8"))
    assert "om_recovered" in persisted["message_ids"]

    adapter._dedup_cache_size = 1
    adapter.remember_recovered_message("om_newer")
    assert "om_recovered" not in adapter._seen_message_ids
    with pytest.raises(ValueError, match="message_id is required"):
        adapter.remember_recovered_message("")


def test_confirmed_recovery_fails_closed_when_dedup_cannot_persist(monkeypatch):
    adapter = recovery_adapter([])
    monkeypatch.setattr(feishu, "atomic_json_write", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("disk full")))

    with pytest.raises(OSError, match="disk full"):
        adapter.remember_recovered_message("om_recovered")


def test_live_transport_keeps_existing_best_effort_dedup_persistence(monkeypatch):
    adapter = recovery_adapter([])
    monkeypatch.setattr(feishu, "atomic_json_write", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("disk full")))

    assert adapter._is_duplicate("om_live") is False
    assert adapter._is_duplicate("om_live") is True


def test_feishu_regular_send_keeps_random_uuid_path():
    adapter = recovery_adapter([])
    adapter._client.im.v1.message.reply = object()
    captured = []
    adapter._build_reply_message_body = lambda **kwargs: captured.append(kwargs) or kwargs
    adapter._build_reply_message_request = lambda message_id, body: (message_id, body)
    adapter._run_blocking = AsyncMock(return_value=SimpleNamespace(success=lambda: True))

    asyncio.run(
        adapter._send_raw_message(
            chat_id="oc_chat",
            msg_type="text",
            payload=json.dumps({"text": "ordinary reply"}),
            reply_to="om_source",
            metadata=None,
        )
    )

    assert captured[0]["uuid_value"]


def test_recovered_delivery_parts_cover_media_and_local_files(monkeypatch, tmp_path):
    async def allow(_hooks, context):
        return SimpleNamespace(
            transmit=True,
            content=context["content"],
            raw={"decision": "allow"},
            decision="allow",
            reason="test_output_safe",
        )

    monkeypatch.setattr("gateway.outbound_boundary.outbound_before_send", allow)
    media = tmp_path / "voice.ogg"
    local = tmp_path / "report.pdf"
    media.write_bytes(b"audio")
    local.write_bytes(b"report")
    adapter = feishu.FeishuAdapter(PlatformConfig())
    adapter.config.typing_indicator = False
    adapter.set_message_handler(
        lambda _event: asyncio.sleep(0, result=f"Done\nMEDIA:{media}\n{local}")
    )
    adapter.send = AsyncMock(return_value=SendResult(success=True, message_id="om_text"))
    adapter.send_voice = AsyncMock(return_value=SendResult(success=True, message_id="om_voice"))
    adapter.send_document = AsyncMock(return_value=SendResult(success=True, message_id="om_file"))
    source = SessionSource(platform=Platform.FEISHU, chat_id="oc_chat", user_id="ou_user")
    event = MessageEvent(text="recover me", source=source, message_id="om_recovered")

    async def scenario():
        event.recovery_delivery = RecoveryDeliveryContext(
            complete=lambda: None,
            idempotency_key="stable-key",
            future=asyncio.get_running_loop().create_future(),
        )
        await adapter._process_message_background(event, build_session_key(source))
        return event.recovery_delivery.future.result()

    assert asyncio.run(scenario()) == {"status": "completed"}
    assert adapter.send_voice.await_args.kwargs["metadata"]["hermes_delivery_part"] == "media:0"
    assert adapter.send_document.await_args.kwargs["metadata"]["hermes_delivery_part"] == "local-file:0"


def test_recovered_final_parts_use_the_reconnected_adapter_owner(
    monkeypatch, tmp_path
):
    async def allow(_hooks, context):
        return SimpleNamespace(
            transmit=True,
            content=context["content"],
            raw={"decision": "allow"},
            decision="allow",
            reason="test_output_safe",
        )

    monkeypatch.setattr("gateway.outbound_boundary.outbound_before_send", allow)
    report = tmp_path / "report.pdf"
    report.write_bytes(b"report")
    original = feishu.FeishuAdapter(PlatformConfig())
    replacement = feishu.FeishuAdapter(PlatformConfig())
    original.config.typing_indicator = False
    original.set_message_handler(
        lambda _event: asyncio.sleep(0, result=f"Done\n{report}")
    )
    original.send = AsyncMock(
        return_value=SendResult(success=False, error="stale adapter")
    )
    original.send_document = AsyncMock(
        return_value=SendResult(success=False, error="stale adapter")
    )
    replacement.send = AsyncMock(
        return_value=SendResult(success=True, message_id="om_text")
    )
    replacement.send_document = AsyncMock(
        return_value=SendResult(success=True, message_id="om_file")
    )
    original.gateway_runner = SimpleNamespace(
        _adapter_for_source=lambda _source: replacement
    )
    source = SessionSource(
        platform=Platform.FEISHU, chat_id="oc_chat", user_id="ou_user"
    )
    event = MessageEvent(text="recover me", source=source, message_id="om_recovered")

    async def scenario():
        event.recovery_delivery = RecoveryDeliveryContext(
            complete=lambda: None,
            idempotency_key="stable-key",
            future=asyncio.get_running_loop().create_future(),
        )
        await original._process_message_background(event, build_session_key(source))
        return event.recovery_delivery.future.result()

    assert asyncio.run(scenario()) == {"status": "completed"}
    original.send.assert_not_awaited()
    original.send_document.assert_not_awaited()
    replacement.send.assert_awaited_once()
    replacement.send_document.assert_awaited_once()


def test_gateway_startup_recovery_hook_awaits_results_and_contains_failure(monkeypatch):
    observed = []

    async def success():
        observed.append("success")

    async def failure():
        observed.append("failure")
        raise RuntimeError("blocked source")

    runner = SimpleNamespace(adapters={Platform.FEISHU: object()})
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *_args, **_kwargs: [None, success(), failure()],
    )

    asyncio.run(GatewayRunner._run_startup_recovery_hooks(runner))

    assert observed == ["success", "failure"]


def test_gateway_startup_recovery_hook_accepts_no_registered_results(monkeypatch):
    runner = SimpleNamespace(adapters={Platform.FEISHU: object()})
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *_args, **_kwargs: [])

    asyncio.run(GatewayRunner._run_startup_recovery_hooks(runner))


def test_recovery_scope_and_source_preconditions_fail_closed():
    adapter = recovery_adapter([])
    assert asyncio.run(fetch_history(adapter, channel_id=""))["reason"] == "invalid_recovery_scope"
    assert asyncio.run(fetch_history(adapter, channel_id="oc_chat", after_cursor="bad"))["reason"] == "invalid_recovery_scope"
    assert (
        asyncio.run(
            fetch_history(adapter,
                channel_id="oc_chat",
                after_cursor="feishu:v1:not-a-number",
            )
        )["reason"]
        == "invalid_recovery_scope"
    )
    adapter._client = None
    assert asyncio.run(fetch_history(adapter, channel_id="oc_chat"))["reason"] == "feishu_client_unavailable"
    adapter._client = SimpleNamespace(im=SimpleNamespace(v1=SimpleNamespace(message=SimpleNamespace(list=object()))))
    adapter._app_id = ""
    assert asyncio.run(fetch_history(adapter, channel_id="oc_chat"))["reason"] == "history_source_identity_missing"


def test_recovery_baseline_is_source_owned_and_fails_closed_on_invalid_or_ahead_state():
    adapter = recovery_adapter([response([])])
    adapter._recovery_state_path.write_text("not-json", encoding="utf-8")
    assert asyncio.run(fetch_history(adapter, channel_id="oc_chat"))["reason"] == "history_baseline_invalid"

    adapter = recovery_adapter([response([])])
    requested = feishu._recovery_cursor(1_700_000_002_000, "om_2")
    result = asyncio.run(adapter.fetch_recovery_history(
        channel_id="oc_chat", after_cursor=requested, profile_id="yuange",
        authorization_fingerprint=_AUTHORIZATION_FINGERPRINT,
    ))
    assert result["reason"] == "history_baseline_unbound"

    adapter = recovery_adapter([response([])])
    channel_key = feishu.hashlib.sha256(b"oc_chat").hexdigest()
    adapter._recovery_state_path.write_text(json.dumps({
        "schema_version": 1,
        "channels": {channel_key: {
            "cursor": feishu._recovery_cursor(1_700_000_001_000, "om_1"),
            "revision": 1,
            "consumed_execution_ids": [],
            "profile_fingerprint": feishu.hashlib.sha256(b"yuange").hexdigest(),
            "source_instance_fingerprint": feishu.hashlib.sha256(b"cli_recovery").hexdigest(),
            "config_fingerprint": adapter._recovery_config_fingerprint(),
            "authorization_fingerprint": _AUTHORIZATION_FINGERPRINT,
        }},
    }), encoding="utf-8")
    result = asyncio.run(adapter.fetch_recovery_history(
        channel_id="oc_chat", after_cursor=requested, profile_id="yuange",
        authorization_fingerprint=_AUTHORIZATION_FINGERPRINT,
    ))
    assert result["reason"] == "history_checkpoint_ahead_of_source"

    adapter._recovery_state_path.write_text('{"schema_version": 9, "channels": {}}', encoding="utf-8")
    assert asyncio.run(fetch_history(adapter, channel_id="oc_chat"))["reason"] == "history_baseline_invalid"


def test_source_exception_and_malformed_item_fail_closed():
    adapter = recovery_adapter([])
    adapter._run_blocking = AsyncMock(side_effect=RuntimeError("network"))
    assert asyncio.run(fetch_history(adapter, channel_id="oc_chat"))["reason"] == "history_source_error"

    adapter = recovery_adapter([response([item("", 1_700_000_001_000)])])
    result = asyncio.run(
        fetch_history(adapter,
            channel_id="oc_chat",
            after_cursor=feishu._recovery_cursor(1_700_000_000_000),
            now_ms=1_700_000_002_000,
        )
    )
    assert result["reason"] == "history_item_malformed"


def test_exact_duplicate_is_collapsed_and_seconds_timestamp_is_normalized():
    duplicate = item("om_same", 1_700_000_001, text="same")
    adapter = recovery_adapter([response([duplicate, duplicate])])
    result = asyncio.run(
        fetch_history(adapter,
            channel_id="oc_chat",
            after_cursor=feishu._recovery_cursor(1_700_000_000_000),
            now_ms=1_700_000_002_000,
        )
    )

    assert len(result["messages"]) == 1
    assert result["messages"][0]["position_ms"] == 1_700_000_001_000


def test_page_limit_and_chat_type_failures_are_bounded(monkeypatch):
    monkeypatch.setattr(feishu, "_FEISHU_RECOVERY_MAX_PAGES", 1)
    adapter = recovery_adapter([response([], has_more=True, page_token="next")])
    assert asyncio.run(fetch_history(adapter, channel_id="oc_chat"))["reason"] == "history_page_limit_exceeded"

    adapter = recovery_adapter([response([])])
    adapter.get_chat_info = AsyncMock(return_value={})
    assert asyncio.run(fetch_history(adapter, channel_id="oc_chat"))["reason"] == "history_chat_type_unavailable"

    adapter = recovery_adapter(
        [response([item("dm", 1_700_000_001_000), item("group", 1_700_000_002_000, chat_type="group")])]
    )
    container_scoped = asyncio.run(
        fetch_history(adapter,
            channel_id="oc_chat",
            after_cursor=feishu._recovery_cursor(1_700_000_000_000),
            now_ms=1_700_000_003_000,
        )
    )
    assert container_scoped["status"] == "ok"
    assert {fact["chat_type"] for fact in container_scoped["messages"]} == {"direct"}


def test_recovery_chat_lookup_failure_never_defaults_to_direct():
    adapter = recovery_adapter([response([])])
    adapter.get_chat_info = feishu.FeishuAdapter.get_chat_info.__get__(adapter)
    adapter._client.im.v1.chat = SimpleNamespace(get=object())
    adapter._run_blocking = AsyncMock(return_value=response(success=False, code=999))

    strict = asyncio.run(adapter.get_chat_info("oc_group", strict=True))
    ordinary = asyncio.run(adapter.get_chat_info("oc_group"))
    result = asyncio.run(fetch_history(adapter, channel_id="oc_group"))

    assert strict["type"] == "unknown"
    assert ordinary["type"] == "dm"
    assert result["reason"] == "history_chat_type_unavailable"


def test_group_container_never_defaults_history_items_to_direct():
    adapter = recovery_adapter(
        [response([item("group", 1_700_000_001_000, channel_id="oc_group")])]
    )
    adapter.get_chat_info = AsyncMock(return_value={"name": "Test Group", "type": "group"})

    result = asyncio.run(fetch_history(adapter, channel_id="oc_group", now_ms=1_700_000_002_000))

    assert result["chat_type"] == "group"
    assert result["messages"][0]["chat_type"] == "group"


def test_adapter_owned_recovery_baseline_commits_with_cas_and_rejects_tampering():
    adapter = recovery_adapter([response([item("om_1", 1_700_000_001_000)])])
    scan = asyncio.run(fetch_history(adapter, channel_id="oc_chat", now_ms=1_700_000_002_000))
    receipt = scan["admission_receipt"]
    facts = adapter.recovery_admission_facts(receipt)
    assert facts["status"] == "ok"
    assert facts["expected_receipt"] == receipt
    assert facts["expected_receipt"] is not receipt

    tampered = json.loads(json.dumps(receipt))
    tampered["position"]["fingerprint"] = "sha256:" + "f" * 64
    assert adapter.commit_recovery_history(tampered)["reason"] == "history_admission_arm_missing"
    committed = adapter.commit_recovery_history(receipt)
    assert committed == {"status": "committed", "cursor": scan["next_cursor"]}
    assert adapter.commit_recovery_history(receipt)["reason"] == "history_admission_arm_missing"

    stored = json.loads(adapter._recovery_state_path.read_text(encoding="utf-8"))
    record = next(iter(stored["channels"].values()))
    assert record["cursor"] == scan["next_cursor"]
    assert record["revision"] == 1
    assert receipt["evidence"]["execution_id"] in record["consumed_execution_ids"]
    assert record["profile_fingerprint"] == feishu.hashlib.sha256(b"yuange").hexdigest()
    assert record["source_instance_fingerprint"] == feishu.hashlib.sha256(b"cli_recovery").hexdigest()
    assert record["config_fingerprint"] == adapter._recovery_config_fingerprint()
    assert record["authorization_fingerprint"] == _AUTHORIZATION_FINGERPRINT
    assert adapter._recovery_state_path.stat().st_mode & 0o777 == 0o600


def test_adapter_owned_recovery_baseline_rejects_identity_config_and_authorization_drift():
    def committed_adapter():
        instance = recovery_adapter([response([item("om_1", 1_700_000_001_000)])])
        scan = asyncio.run(fetch_history(instance, channel_id="oc_chat", now_ms=1_700_000_002_000))
        assert instance.commit_recovery_history(scan["admission_receipt"])["status"] == "committed"
        instance._run_blocking = AsyncMock(return_value=response([]))
        return instance, scan["next_cursor"]

    adapter, cursor = committed_adapter()
    adapter._app_id = "other_app"
    result = asyncio.run(fetch_history(adapter, channel_id="oc_chat", after_cursor=cursor))
    assert result["reason"] == "history_source_identity_drift"

    adapter, cursor = committed_adapter()
    adapter._domain_name = "other-domain"
    result = asyncio.run(fetch_history(adapter, channel_id="oc_chat", after_cursor=cursor))
    assert result["reason"] == "history_source_config_drift"

    adapter, cursor = committed_adapter()
    result = asyncio.run(adapter.fetch_recovery_history(
        channel_id="oc_chat", after_cursor=cursor, profile_id="other-profile",
        authorization_fingerprint=_AUTHORIZATION_FINGERPRINT,
    ))
    assert result["reason"] == "history_profile_identity_drift"

    adapter, cursor = committed_adapter()
    result = asyncio.run(adapter.fetch_recovery_history(
        channel_id="oc_chat", after_cursor=cursor, profile_id="yuange",
        authorization_fingerprint="sha256:" + "b" * 64,
    ))
    assert result["reason"] == "history_authorization_drift"


def test_concurrent_adapter_arms_commit_with_one_baseline_cas_winner():
    fact = item("om_1", 1_700_000_001_000)
    adapter = recovery_adapter([response([fact]), response([fact])])
    first = asyncio.run(fetch_history(adapter, channel_id="oc_chat", now_ms=1_700_000_002_000))
    second = asyncio.run(fetch_history(adapter, channel_id="oc_chat", now_ms=1_700_000_002_000))

    assert adapter.commit_recovery_history(first["admission_receipt"])["status"] == "committed"
    assert adapter.commit_recovery_history(second["admission_receipt"])["reason"] == "history_baseline_cas_conflict"


def test_adapter_commit_rechecks_source_binding_after_scan():
    adapter = recovery_adapter([response([item("om_1", 1_700_000_001_000)])])
    scan = asyncio.run(fetch_history(adapter, channel_id="oc_chat", now_ms=1_700_000_002_000))

    adapter._domain_name = "changed-after-scan"

    assert adapter.commit_recovery_history(scan["admission_receipt"])["reason"] == "history_source_binding_drift"
    assert not adapter._recovery_state_path.exists()


def test_adapter_facts_and_commit_reject_missing_or_drifted_baseline():
    adapter = recovery_adapter([response([item("om_1", 1_700_000_001_000)])])
    assert adapter.recovery_admission_facts({})["reason"] == "history_admission_arm_missing"
    scan = asyncio.run(fetch_history(adapter, channel_id="oc_chat", now_ms=1_700_000_002_000))
    receipt = scan["admission_receipt"]
    adapter._recovery_state_path.write_text("bad-json", encoding="utf-8")
    assert adapter.commit_recovery_history(receipt)["reason"] == "history_baseline_invalid"

    adapter = recovery_adapter([response([item("om_1", 1_700_000_001_000)])])
    scan = asyncio.run(fetch_history(adapter, channel_id="oc_chat", now_ms=1_700_000_002_000))
    receipt = scan["admission_receipt"]
    channel_key = feishu.hashlib.sha256(b"oc_chat").hexdigest()
    adapter._recovery_state_path.write_text(json.dumps({
        "schema_version": 1,
        "channels": {channel_key: {
            "cursor": "", "revision": 0,
            "profile_fingerprint": "wrong",
        }},
    }), encoding="utf-8")
    assert adapter.commit_recovery_history(receipt)["reason"] == "history_baseline_binding_drift"


def test_multi_image_delivery_returns_receipt_and_uses_distinct_stable_parts():
    adapter = recovery_adapter([])
    adapter.send_image = AsyncMock(
        side_effect=[
            SendResult(success=True, message_id="om_image_1"),
            SendResult(success=True, message_id="om_image_2"),
        ]
    )
    metadata = {
        "hermes_delivery_idempotency_key": "stable-key",
        "hermes_delivery_part": "remote-image-batch",
    }

    result = asyncio.run(
        adapter.send_multiple_images(
            "oc_chat",
            [("https://example.test/1.png", "one"), ("https://example.test/2.png", "two")],
            metadata=metadata,
        )
    )

    assert result == SendResult(success=True, message_id="om_image_2")
    parts = [call.kwargs["metadata"]["hermes_delivery_part"] for call in adapter.send_image.await_args_list]
    assert parts == ["remote-image-batch:0", "remote-image-batch:1"]


def test_multi_image_delivery_propagates_failure_exception_and_empty_success():
    adapter = recovery_adapter([])
    adapter.send_image = AsyncMock(return_value=SendResult(success=False, error="rejected"))
    failed = asyncio.run(adapter.send_multiple_images("oc_chat", [("https://example.test/1.png", "one")]))
    assert failed == SendResult(success=False, error="rejected")
    adapter.send_image = AsyncMock(side_effect=RuntimeError("send crashed"))
    crashed = asyncio.run(adapter.send_multiple_images("oc_chat", [("https://example.test/2.png", "two")]))
    assert crashed == SendResult(success=False, error="send crashed")
    assert asyncio.run(adapter.send_multiple_images("oc_chat", [])) == SendResult(success=True)


def test_recovered_delivery_without_idempotency_key_uses_plain_final_metadata(monkeypatch):
    async def allow(_hooks, context):
        return SimpleNamespace(
            transmit=True, content=context["content"], raw={"decision": "allow"},
            decision="allow", reason="test_output_safe",
        )

    monkeypatch.setattr("gateway.outbound_boundary.outbound_before_send", allow)
    adapter = feishu.FeishuAdapter(PlatformConfig())
    adapter.config.typing_indicator = False
    adapter.set_message_handler(lambda _event: asyncio.sleep(0, result="business result"))
    adapter.send = AsyncMock(return_value=SendResult(success=True, message_id="om_reply"))
    source = SessionSource(platform=Platform.FEISHU, chat_id="oc_chat", user_id="ou_user")
    event = MessageEvent(text="recover me", source=source, message_id="om_recovered")

    async def scenario():
        event.recovery_delivery = RecoveryDeliveryContext(
            complete=lambda: None,
            idempotency_key="",
            future=asyncio.get_running_loop().create_future(),
        )
        await adapter._process_message_background(event, build_session_key(source))
        return event.recovery_delivery.future.result()

    assert asyncio.run(scenario()) == {"status": "completed"}
    assert "hermes_delivery_part" not in adapter.send.await_args.kwargs["metadata"]


def test_list_request_sdk_builder_carries_optional_page_token(monkeypatch):
    class Builder:
        def __init__(self):
            self.values = {}

        def __getattr__(self, name):
            if name == "build":
                return lambda: SimpleNamespace(**self.values)
            return lambda value: self.values.__setitem__(name, value) or self

    class Request:
        @staticmethod
        def builder():
            return Builder()

    monkeypatch.setattr(feishu, "ListMessageRequest", Request, raising=False)
    request = feishu.FeishuAdapter._build_list_message_request(
        channel_id="oc_chat", start_time=1, end_time=2, page_token="next",
    )
    assert request.container_id == "oc_chat"
    assert request.page_token == "next"
    request_without_page = feishu.FeishuAdapter._build_list_message_request(
        channel_id="oc_chat", start_time=1, end_time=2, page_token="",
    )
    assert not hasattr(request_without_page, "page_token")


def test_list_request_fallback_and_recovery_event_channel_fallback(monkeypatch):
    monkeypatch.delattr(feishu, "ListMessageRequest", raising=False)
    request = feishu.FeishuAdapter._build_list_message_request(
        channel_id="oc_chat", start_time=1, end_time=2, page_token="next"
    )
    assert request.container_id == "oc_chat" and request.page_token == "next"

    adapter = recovery_adapter([])
    adapter.get_chat_info = AsyncMock(return_value={"name": "Admin DM", "type": "dm"})
    adapter._resolve_sender_profile = AsyncMock(
        return_value={"user_id": "ou_user", "user_name": "Admin", "user_id_alt": None}
    )
    event = asyncio.run(
        adapter.recovery_event(
            {
                "recovery_channel_id": "oc_chat",
                "inbound_uid": "om_recovered",
                "sender_uid": "ou_user",
                "text": "recover me",
                "position_ms": 1_700_000_000_000,
            }
        )
    )
    assert event.source.chat_id == "oc_chat"
