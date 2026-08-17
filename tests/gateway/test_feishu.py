"""Tests for the Feishu gateway integration."""

import asyncio
import concurrent.futures
import json
import os
import socket
import tempfile
import threading
import time
import unittest
from collections import OrderedDict
from pathlib import Path
from types import SimpleNamespace
from typing import Dict
from unittest.mock import AsyncMock, Mock, patch

from gateway.platforms.base import ProcessingOutcome

try:
    import lark_oapi
    _HAS_LARK_OAPI = True
except ImportError:
    _HAS_LARK_OAPI = False


class _FakeRequestContent:
    def __init__(self, body: bytes):
        self.body = body
        self.read_sizes: list[int] = []

    async def readexactly(self, size: int) -> bytes:
        self.read_sizes.append(size)
        if len(self.body) < size:
            raise asyncio.IncompleteReadError(self.body, size)
        return self.body[:size]


def _mock_event_dispatcher_builder(mock_handler_class):
    mock_builder = Mock()
    mock_builder.register_p2_im_message_message_read_v1 = Mock(return_value=mock_builder)
    mock_builder.register_p2_im_message_receive_v1 = Mock(return_value=mock_builder)
    mock_builder.register_p2_im_message_reaction_created_v1 = Mock(return_value=mock_builder)
    mock_builder.register_p2_im_message_reaction_deleted_v1 = Mock(return_value=mock_builder)
    mock_builder.register_p2_card_action_trigger = Mock(return_value=mock_builder)
    mock_builder.build = Mock(return_value=object())
    mock_handler_class.builder = Mock(return_value=mock_builder)
    return mock_builder


class TestConfigEnvOverrides(unittest.TestCase):
    @patch.dict(os.environ, {
        "FEISHU_APP_ID": "cli_xxx",
        "FEISHU_APP_SECRET": "secret_xxx",
        "FEISHU_CONNECTION_MODE": "websocket",
        "FEISHU_DOMAIN": "feishu",
    }, clear=False)
    def test_feishu_config_loaded_from_env(self):
        from gateway.config import GatewayConfig, Platform, _apply_env_overrides

        config = GatewayConfig()
        _apply_env_overrides(config)

        self.assertIn(Platform.FEISHU, config.platforms)
        self.assertTrue(config.platforms[Platform.FEISHU].enabled)
        self.assertEqual(config.platforms[Platform.FEISHU].extra["app_id"], "cli_xxx")
        self.assertEqual(config.platforms[Platform.FEISHU].extra["connection_mode"], "websocket")


class TestFeishuMessageNormalization(unittest.TestCase):


    def test_normalize_interactive_card_preserves_title_body_and_actions(self):
        from plugins.platforms.feishu.adapter import normalize_feishu_message

        normalized = normalize_feishu_message(
            message_type="interactive",
            raw_content=json.dumps(
                {
                    "card": {
                        "header": {"title": {"tag": "plain_text", "content": "Build Failed"}},
                        "elements": [
                            {"tag": "div", "text": {"tag": "lark_md", "content": "Service: payments-api"}},
                            {"tag": "div", "text": {"tag": "plain_text", "content": "Branch: main"}},
                            {
                                "tag": "action",
                                "actions": [
                                    {"tag": "button", "text": {"tag": "plain_text", "content": "View Logs"}},
                                    {"tag": "button", "text": {"tag": "plain_text", "content": "Retry"}},
                                ],
                            },
                        ],
                    }
                }
            ),
        )

        self.assertEqual(normalized.relation_kind, "interactive")
        self.assertEqual(
            normalized.text_content,
            "Build Failed\nService: payments-api\nBranch: main\nView Logs\nRetry\nActions: View Logs, Retry",
        )


class TestFeishuAdapterMessaging(unittest.TestCase):
    @unittest.skipUnless(_HAS_LARK_OAPI, "lark-oapi not installed")
    def test_websocket_sdk_accepts_channel_ua_tag(self):
        """The shipped SDK must support the Channel signaling argument.

        Guarded on the pinned version: the repo pins lark-oapi==1.6.8 (the
        first release with ``extra_ua_tags``). Dev machines can carry an
        older lazy-installed lark-oapi that predates the argument — that is
        an environment artifact, not a product regression, so skip rather
        than fail there. Environments installing the pin (the feishu extra)
        still exercise the real assertion.
        """
        import inspect

        from importlib.metadata import version as _pkg_version

        installed = tuple(int(p) for p in _pkg_version("lark-oapi").split(".")[:3] if p.isdigit())
        if installed < (1, 6, 8):
            self.skipTest(
                f"lark-oapi {_pkg_version('lark-oapi')} predates extra_ua_tags; "
                "repo pin is 1.6.8 — stale local install"
            )

        from lark_oapi.ws import Client as FeishuWSClient

        signature = inspect.signature(FeishuWSClient)
        self.assertIn("extra_ua_tags", signature.parameters)


    def test_disconnect_sends_websocket_close_frame(self):
        """Regression test for #10202: disconnect() must call the WSS
        client's ``_disconnect()`` coroutine so a WebSocket CLOSE frame
        is sent to Feishu. Without this, Feishu's server continues
        routing to the stale connection, silencing the channel.
        """
        import threading
        from types import SimpleNamespace
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())

        # Real thread loop to schedule the close coroutine on.
        ws_thread_loop = asyncio.new_event_loop()
        ready = threading.Event()

        def _run_loop() -> None:
            asyncio.set_event_loop(ws_thread_loop)
            ready.set()
            ws_thread_loop.run_forever()

        thread = threading.Thread(target=_run_loop, daemon=True)
        thread.start()
        ready.wait()

        close_called = threading.Event()

        async def _fake_disconnect() -> None:
            close_called.set()

        ws_client = SimpleNamespace(_disconnect=_fake_disconnect, _auto_reconnect=True)
        adapter._ws_client = ws_client
        adapter._ws_thread_loop = ws_thread_loop
        adapter._ws_future = None

        try:
            asyncio.run(adapter.disconnect())
        finally:
            if not ws_thread_loop.is_closed():
                ws_thread_loop.call_soon_threadsafe(ws_thread_loop.stop)
            thread.join(timeout=2.0)
            if not ws_thread_loop.is_closed():
                ws_thread_loop.close()

        self.assertTrue(
            close_called.is_set(),
            "disconnect() must schedule ws_client._disconnect() on the ws thread loop",
        )
        # _disable_websocket_auto_reconnect() must still run.
        self.assertIsNone(adapter._ws_client)


    @patch.dict(os.environ, {
        "FEISHU_APP_ID": "cli_app",
        "FEISHU_APP_SECRET": "secret_app",
    }, clear=True)
    def test_connect_websocket_sets_channel_ua_tag(self):
        """Verify that FeishuWSClient receives extra_ua_tags=["channel"].

        Without this UA tag the Feishu server does not push group @mention
        events over the WebSocket transport.  See
        https://github.com/NousResearch/hermes-agent/issues/50656
        """
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        ws_client = SimpleNamespace()

        with (
            patch("plugins.platforms.feishu.adapter.FEISHU_AVAILABLE", True),
            patch("plugins.platforms.feishu.adapter.FEISHU_WEBSOCKET_AVAILABLE", True),
            patch("plugins.platforms.feishu.adapter.lark",
                  SimpleNamespace(LogLevel=SimpleNamespace(INFO="INFO", WARNING="WARNING"))),
            patch("plugins.platforms.feishu.adapter.EventDispatcherHandler") as mock_handler_class,
            patch("plugins.platforms.feishu.adapter.FeishuWSClient") as mock_ws_client,
            patch("plugins.platforms.feishu.adapter._run_official_feishu_ws_client"),
            patch("plugins.platforms.feishu.adapter.acquire_scoped_lock", return_value=(True, None)),
            patch("plugins.platforms.feishu.adapter.release_scoped_lock"),
            patch.object(adapter, "_hydrate_bot_identity", new=AsyncMock()),
            patch.object(adapter, "_build_lark_client", return_value=SimpleNamespace()),
        ):
            _mock_event_dispatcher_builder(mock_handler_class)

            loop = asyncio.new_event_loop()
            future = loop.create_future()
            future.set_result(None)

            class _Loop:
                def run_in_executor(self, *_args, **_kwargs):
                    return future
                def is_closed(self):
                    return False

            try:
                with patch("plugins.platforms.feishu.adapter.asyncio.get_running_loop",
                           return_value=_Loop()):
                    connected = asyncio.run(adapter.connect())
            finally:
                loop.close()

        self.assertTrue(connected)
        # Verify the Channel SDK UA tag is present — this is the fix for
        # group @mention message delivery over WebSocket.
        mock_ws_client.assert_called_once()
        call_kwargs = mock_ws_client.call_args.kwargs
        self.assertIn("extra_ua_tags", call_kwargs,
                      "FeishuWSClient must receive extra_ua_tags for group @mention delivery")
        self.assertEqual(call_kwargs["extra_ua_tags"], ["channel"],
                         "extra_ua_tags must be ['channel'] to enable group event routing")


    @patch.dict(os.environ, {}, clear=True)
    def test_edit_message_falls_back_to_text_when_post_update_is_rejected(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        captured = {"calls": []}

        class _MessageAPI:
            def update(self, request):
                captured["calls"].append(request)
                if len(captured["calls"]) == 1:
                    return SimpleNamespace(success=lambda: False, code=230001, msg="content format of the post type is incorrect")
                return SimpleNamespace(success=lambda: True)

        adapter._client = SimpleNamespace(
            im=SimpleNamespace(
                v1=SimpleNamespace(
                    message=_MessageAPI(),
                )
            )
        )

        async def _direct(func, *args, **kwargs):
            return func(*args, **kwargs)

        with patch("plugins.platforms.feishu.adapter.asyncio.to_thread", side_effect=_direct):
            result = asyncio.run(
                adapter.edit_message(
                    chat_id="oc_chat",
                    message_id="om_progress",
                    content="可以用 **粗体** 和 *斜体*。",
                )
            )

        self.assertTrue(result.success)
        self.assertEqual(captured["calls"][0].request_body.msg_type, "post")
        self.assertEqual(captured["calls"][1].request_body.msg_type, "text")
        self.assertEqual(
            captured["calls"][1].request_body.content,
            json.dumps({"text": "可以用 粗体 和 斜体。"}, ensure_ascii=False),
        )

    def test_async_send_sdk_call_has_outer_deadline(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import (
            FeishuAdapter,
            FeishuDeliveryTimeoutError,
        )

        adapter = FeishuAdapter(PlatformConfig())
        release = threading.Event()

        async def _never_returns(_request):
            while not release.is_set():
                await asyncio.sleep(0.002)

        try:
            with patch(
                "plugins.platforms.feishu.adapter._FEISHU_SEND_HARD_TIMEOUT_SECONDS",
                0.01,
            ):
                with self.assertRaisesRegex(
                    FeishuDeliveryTimeoutError, "delivery status unknown",
                ):
                    asyncio.run(
                        adapter._run_send_sdk(Mock(), _never_returns, object())
                    )
            self.assertIsNotNone(adapter._sdk_executor)
        finally:
            release.set()
            adapter._shutdown_sdk_executor()

    def test_send_sdk_uses_async_api_without_sync_fallback(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        sync_send = Mock(side_effect=AssertionError("sync path must not run"))
        async_send = AsyncMock(return_value="sent")

        try:
            result = asyncio.run(
                adapter._run_send_sdk(sync_send, async_send, object())
            )
        finally:
            adapter._shutdown_sdk_executor()

        self.assertEqual(result, "sent")
        async_send.assert_awaited_once()
        sync_send.assert_not_called()

    def test_async_send_preawait_block_has_wall_clock_deadline(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu import adapter as feishu_module

        adapter = feishu_module.FeishuAdapter(PlatformConfig())
        sync_send = Mock(side_effect=AssertionError("sync path must not run"))
        entered = threading.Event()
        release = threading.Event()
        cancelled = threading.Event()
        transport_entered = threading.Event()
        delivered = threading.Event()
        calls = 0

        async def _blocks_before_first_await(_request):
            nonlocal calls
            calls += 1
            entered.set()
            release.wait(timeout=1)
            transport_entered.set()
            try:
                await asyncio.sleep(1)
                delivered.set()
                return "late result"
            except asyncio.CancelledError:
                cancelled.set()
                raise

        async def _exercise():
            heartbeats = 0

            async def _heartbeat():
                nonlocal heartbeats
                while True:
                    heartbeats += 1
                    await asyncio.sleep(0.002)

            heartbeat = asyncio.create_task(_heartbeat())
            try:
                started = time.monotonic()
                with self.assertRaisesRegex(
                    feishu_module.FeishuDeliveryTimeoutError,
                    "delivery status unknown",
                ):
                    await adapter._run_send_sdk(
                        sync_send, _blocks_before_first_await, object(),
                    )
                elapsed = time.monotonic() - started
                self.assertTrue(entered.is_set())
                self.assertLess(elapsed, 0.2)
                self.assertGreater(heartbeats, 1)

                second_started = time.monotonic()
                with self.assertRaisesRegex(
                    feishu_module.FeishuDeliveryTimeoutError,
                    "still blocked",
                ):
                    await adapter._run_send_sdk(
                        sync_send, _blocks_before_first_await, object(),
                    )
                self.assertLess(time.monotonic() - second_started, 0.05)
                self.assertEqual(calls, 1)
                self.assertLessEqual(len(adapter._get_sdk_executor()._threads), 1)

                release.set()
                cancellation_observed = await asyncio.to_thread(
                    cancelled.wait, 0.2,
                )
                self.assertTrue(cancellation_observed)
                self.assertTrue(transport_entered.is_set())
                self.assertFalse(delivered.is_set())
            finally:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)

        try:
            with patch.object(
                feishu_module, "_FEISHU_SEND_HARD_TIMEOUT_SECONDS", 0.02,
            ):
                asyncio.run(_exercise())
        finally:
            release.set()
            adapter._shutdown_sdk_executor()

        self.assertEqual(calls, 1)
        sync_send.assert_not_called()

    def test_send_endpoint_uses_primed_token_without_shared_config_mutation(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu import adapter as feishu_module

        adapter = feishu_module.FeishuAdapter(PlatformConfig())
        shared_config = SimpleNamespace(enable_set_token=False)
        adapter._client = SimpleNamespace(config=shared_config)
        observed = {}

        class _RequestOption:
            def __init__(self):
                self.tenant_access_token = None

        class _TokenManager:
            @staticmethod
            def get_self_tenant_token(config):
                self.assertIs(config, shared_config)
                return "primed-token"

        class _MessageResource:
            def __init__(self, config):
                self.config = config

            async def acreate(self, _request, option=None):
                observed["enable_set_token"] = self.config.enable_set_token
                observed["token"] = option.tenant_access_token
                await asyncio.sleep(0)
                return "sent"

        resource = _MessageResource(shared_config)
        try:
            with (
                patch.object(feishu_module, "LarkTokenManager", _TokenManager),
                patch.object(feishu_module, "LarkRequestOption", _RequestOption),
            ):
                result = asyncio.run(
                    adapter._run_send_sdk(Mock(), resource.acreate, object())
                )
        finally:
            adapter._shutdown_sdk_executor()

        self.assertEqual(result, "sent")
        self.assertEqual(observed["token"], "primed-token")
        self.assertTrue(observed["enable_set_token"])
        self.assertFalse(shared_config.enable_set_token)

    @unittest.skipUnless(_HAS_LARK_OAPI, "lark_oapi is not installed")
    def test_pinned_sdk_create_and_reply_use_primed_token(self):
        from gateway.config import PlatformConfig
        from lark_oapi.api.im.v1 import (
            CreateMessageRequest,
            CreateMessageRequestBody,
            ReplyMessageRequest,
            ReplyMessageRequestBody,
        )
        from lark_oapi.api.im.v1.resource import message as message_resource
        from lark_oapi.core.model import Config
        from lark_oapi.core.model.raw_response import RawResponse
        from lark_oapi.core.token import auth as token_auth
        from plugins.platforms.feishu import adapter as feishu_module

        self.assertTrue(feishu_module._load_lark_oapi())
        adapter = feishu_module.FeishuAdapter(PlatformConfig())
        shared_config = Config()
        shared_config.app_id = "cli_test"
        shared_config.app_secret = "secret_test"
        adapter._client = SimpleNamespace(config=shared_config)
        resource = message_resource.Message(shared_config)
        observations = []

        class _PrimingTokenManager:
            @staticmethod
            def get_self_tenant_token(config):
                self.assertIs(config, shared_config)
                return "primed-token"

        async def _fake_transport(config, request, option=None):
            observations.append(
                (request.uri, config.enable_set_token, option.tenant_access_token)
            )
            response = RawResponse()
            response.status_code = 200
            response.headers = {}
            response.content = b'{"code":0,"msg":"success","data":{}}'
            return response

        create_request = (
            CreateMessageRequest.builder()
            .receive_id_type("chat_id")
            .request_body(
                CreateMessageRequestBody.builder()
                .receive_id("oc_test")
                .msg_type("text")
                .content('{"text":"test"}')
                .uuid("stable-create")
                .build()
            )
            .build()
        )
        reply_request = (
            ReplyMessageRequest.builder()
            .message_id("om_test")
            .request_body(
                ReplyMessageRequestBody.builder()
                .msg_type("text")
                .content('{"text":"test"}')
                .uuid("stable-reply")
                .build()
            )
            .build()
        )

        async def _exercise():
            await adapter._run_send_sdk(Mock(), resource.acreate, create_request)
            await adapter._run_send_sdk(Mock(), resource.areply, reply_request)

        try:
            with (
                patch.object(
                    feishu_module,
                    "LarkTokenManager",
                    _PrimingTokenManager,
                ),
                patch.object(
                    token_auth.TokenManager,
                    "get_self_tenant_token",
                    side_effect=AssertionError("SDK verify must use primed token"),
                ),
                patch.object(
                    message_resource.Transport,
                    "aexecute",
                    new=staticmethod(_fake_transport),
                ),
            ):
                asyncio.run(_exercise())
        finally:
            adapter._shutdown_sdk_executor()

        self.assertEqual(len(observations), 2)
        self.assertTrue(all(item[1] for item in observations))
        self.assertTrue(all(item[2] == "primed-token" for item in observations))
        self.assertFalse(shared_config.enable_set_token)

    def test_late_send_worker_failure_is_logged(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu import adapter as feishu_module

        adapter = feishu_module.FeishuAdapter(PlatformConfig())
        release = threading.Event()

        async def _late_failure(_request):
            release.wait(timeout=1)
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError as exc:
                raise RuntimeError("late worker failure") from exc

        async def _exercise():
            with self.assertRaises(feishu_module.FeishuDeliveryTimeoutError):
                await adapter._run_send_sdk(Mock(), _late_failure, object())
            release.set()
            for _ in range(20):
                if not adapter._send_executions:
                    return
                await asyncio.sleep(0.01)
            self.fail("late send worker did not exit")

        try:
            with (
                patch.object(
                    feishu_module, "_FEISHU_SEND_HARD_TIMEOUT_SECONDS", 0.02,
                ),
                self.assertLogs(
                    "plugins.platforms.feishu.adapter", level="WARNING",
                ) as captured,
            ):
                asyncio.run(_exercise())
        finally:
            release.set()
            adapter._shutdown_sdk_executor()

        self.assertTrue(
            any("late worker failure" in line for line in captured.output)
        )

    def test_completed_worker_failure_is_logged_when_caller_detaches_late(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu import adapter as feishu_module

        adapter = feishu_module.FeishuAdapter(PlatformConfig())

        async def _exercise():
            future = concurrent.futures.Future()
            future.set_exception(RuntimeError("completed before detach"))
            wrapped = asyncio.wrap_future(future)
            adapter._track_send_execution(future, None, wrapped)
            adapter._quarantine_send_execution(future)

        with self.assertLogs(
            "plugins.platforms.feishu.adapter", level="WARNING",
        ) as captured:
            asyncio.run(_exercise())

        self.assertTrue(
            any("completed before detach" in line for line in captured.output)
        )
        self.assertFalse(adapter._detached_send_futures)

    def test_deadline_logs_worker_failure_that_completes_before_handler(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu import adapter as feishu_module

        adapter = feishu_module.FeishuAdapter(PlatformConfig())
        worker_future = concurrent.futures.Future()
        executor = SimpleNamespace(submit=Mock(return_value=worker_future))

        async def _deadline_then_worker_finishes(wrapped, timeout):
            self.assertEqual(timeout, 0.02)
            wrapped.cancel()
            worker_future.set_exception(RuntimeError("deadline completion race"))
            raise asyncio.TimeoutError

        try:
            with (
                patch.object(adapter, "_get_sdk_executor", return_value=executor),
                patch.object(
                    feishu_module, "_FEISHU_SEND_HARD_TIMEOUT_SECONDS", 0.02,
                ),
                patch.object(
                    feishu_module.asyncio,
                    "wait_for",
                    side_effect=_deadline_then_worker_finishes,
                ),
                self.assertLogs(
                    "plugins.platforms.feishu.adapter", level="WARNING",
                ) as captured,
            ):
                with self.assertRaises(feishu_module.FeishuDeliveryTimeoutError):
                    asyncio.run(
                        adapter._run_send_sdk(Mock(), None, object())
                    )
        finally:
            adapter._shutdown_sdk_executor()

        self.assertTrue(
            any("deadline completion race" in line for line in captured.output)
        )
        self.assertFalse(adapter._detached_send_futures)

    def test_normal_endpoint_failure_is_not_logged_as_late_delivery(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu import adapter as feishu_module

        adapter = feishu_module.FeishuAdapter(PlatformConfig())

        async def _immediate_failure(_request):
            raise ValueError("ordinary endpoint failure")

        try:
            with self.assertNoLogs(
                "plugins.platforms.feishu.adapter", level="WARNING",
            ):
                with self.assertRaisesRegex(ValueError, "ordinary endpoint failure"):
                    asyncio.run(
                        adapter._run_send_sdk(
                            Mock(), _immediate_failure, object(),
                        )
                    )
        finally:
            adapter._shutdown_sdk_executor()

        self.assertFalse(adapter._detached_send_futures)

    def test_shutdown_cancels_preawait_send_before_reconnect(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu import adapter as feishu_module

        adapter = feishu_module.FeishuAdapter(PlatformConfig())
        entered = threading.Event()
        release = threading.Event()
        cancelled = threading.Event()

        async def _blocked_send(_request):
            entered.set()
            release.wait(timeout=1)
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        async def _exercise():
            send_task = asyncio.create_task(
                adapter._run_send_sdk(Mock(), _blocked_send, object())
            )
            self.assertTrue(await asyncio.to_thread(entered.wait, 0.2))
            adapter._shutdown_sdk_executor()
            with self.assertRaises(asyncio.CancelledError):
                await send_task

            adapter._rearm_sdk_executor()
            with self.assertRaisesRegex(
                feishu_module.FeishuDeliveryTimeoutError,
                "still blocked",
            ):
                await adapter._run_send_sdk(
                    Mock(), AsyncMock(return_value="must not start"), object(),
                )

            release.set()
            self.assertTrue(await asyncio.to_thread(cancelled.wait, 0.2))
            for _ in range(20):
                if not adapter._send_executions:
                    break
                await asyncio.sleep(0.01)

            self.assertEqual(
                await adapter._run_send_sdk(
                    Mock(side_effect=AssertionError("sync path must not run")),
                    AsyncMock(return_value="reconnected"),
                    object(),
                ),
                "reconnected",
            )

        try:
            asyncio.run(_exercise())
        finally:
            release.set()
            adapter._shutdown_sdk_executor()

    def test_blocked_token_refresh_does_not_freeze_loop_or_fill_pool(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu import adapter as feishu_module

        adapter = feishu_module.FeishuAdapter(PlatformConfig())
        adapter._client = SimpleNamespace(config=object())
        release = threading.Event()

        class _BlockingTokenManager:
            @staticmethod
            def get_self_tenant_token(_config):
                release.wait(timeout=1)
                return "token"

        async_send = AsyncMock(return_value="sent")
        with (
            patch.object(
                feishu_module, "LarkTokenManager", _BlockingTokenManager,
            ),
            patch.object(
                feishu_module, "_FEISHU_SEND_HARD_TIMEOUT_SECONDS", 0.01,
            ),
        ):
            started = time.monotonic()
            with self.assertRaisesRegex(
                feishu_module.FeishuDeliveryTimeoutError,
                "token refresh timed out",
            ):
                asyncio.run(
                    adapter._run_send_sdk(Mock(), async_send, object())
                )
            self.assertLess(time.monotonic() - started, 0.2)

            with self.assertRaisesRegex(
                feishu_module.FeishuDeliveryTimeoutError,
                "still blocked",
            ):
                asyncio.run(
                    adapter._run_send_sdk(Mock(), async_send, object())
                )

        async_send.assert_not_awaited()
        self.assertLessEqual(len(adapter._get_sdk_executor()._threads), 1)
        release.set()
        adapter._shutdown_sdk_executor()

    def test_send_timeout_is_not_retried(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        adapter._send_raw_message = AsyncMock(
            side_effect=TimeoutError(
                "Feishu SDK call timed out after 25s; delivery status unknown"
            )
        )
        with self.assertRaises(TimeoutError):
            asyncio.run(
                adapter._feishu_send_with_retry(
                    chat_id="oc_chat",
                    msg_type="text",
                    payload='{"text":"hello"}',
                    reply_to=None,
                    metadata=None,
                )
            )
        adapter._send_raw_message.assert_awaited_once()

    def test_authorized_send_is_one_exact_attempt_with_stable_request_key(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        adapter._client = object()
        response = SimpleNamespace(
            success=lambda: True,
            data=SimpleNamespace(message_id="om-authorized"),
        )
        adapter._send_raw_message = AsyncMock(return_value=response)

        result = asyncio.run(
            adapter.send_authorized(
                "oc_chat",
                "请回复 1 确认",
                metadata={"thread_id": "omt_thread"},
                transport_request_id="request-authorized",
            )
        )

        self.assertTrue(result.success)
        adapter._send_raw_message.assert_awaited_once()
        metadata = adapter._send_raw_message.await_args.kwargs["metadata"]
        self.assertEqual(metadata["thread_id"], "omt_thread")
        self.assertEqual(
            metadata["hermes_delivery_idempotency_key"],
            "request-authorized",
        )
        self.assertTrue(metadata["hermes_transport_authority_strict"])

    def test_authorized_send_rejects_chunking_before_provider(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        adapter._client = object()
        adapter._send_raw_message = AsyncMock()
        adapter.truncate_message = Mock(return_value=["first", "second"])

        result = asyncio.run(
            adapter.send_authorized(
                "oc_chat",
                "too long",
                transport_request_id="request-chunked",
            )
        )

        self.assertFalse(result.success)
        adapter._send_raw_message.assert_not_awaited()

    def test_authorized_send_rejects_disconnected_or_provider_exception(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        disconnected = asyncio.run(
            adapter.send_authorized(
                "oc_chat",
                "notice",
                transport_request_id="request-disconnected",
            )
        )
        self.assertFalse(disconnected.success)
        self.assertEqual(disconnected.error, "Not connected")

        adapter._client = object()
        adapter._send_raw_message = AsyncMock(side_effect=RuntimeError("provider down"))
        failed = asyncio.run(
            adapter.send_authorized(
                "oc_chat",
                "notice",
                transport_request_id="request-provider-error",
            )
        )
        self.assertFalse(failed.success)
        self.assertFalse(failed.retryable)
        self.assertEqual(failed.error, "provider down")

    def test_standalone_authorized_send_reuses_strict_adapter_seam(self):
        from gateway.config import PlatformConfig
        from gateway.platforms.base import SendResult
        from plugins.platforms.feishu import adapter as feishu_module

        strict_send = AsyncMock(
            return_value=SendResult(success=True, message_id="om-standalone-strict")
        )
        with patch.object(feishu_module, "_load_lark_oapi", return_value=True), patch.object(
            feishu_module.FeishuAdapter,
            "_build_lark_client",
            return_value=object(),
        ), patch.object(
            feishu_module.FeishuAdapter,
            "send_authorized",
            strict_send,
        ):
            result = asyncio.run(
                feishu_module._standalone_send_authorized(
                    PlatformConfig(),
                    "oc_chat",
                    "请回复 1 确认",
                    transport_request_id="request-standalone-strict",
                )
            )

        self.assertTrue(result["success"])
        self.assertEqual(result["message_id"], "om-standalone-strict")
        strict_send.assert_awaited_once_with(
            "oc_chat",
            "请回复 1 确认",
            metadata=None,
            transport_request_id="request-standalone-strict",
        )

    def test_standalone_authorized_send_fail_closed_branches(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu import adapter as feishu_module

        with patch.object(feishu_module, "_load_lark_oapi", return_value=False):
            missing = asyncio.run(
                feishu_module._standalone_send_authorized(
                    PlatformConfig(),
                    "oc_chat",
                    "notice",
                    transport_request_id="request-missing-deps",
                )
            )
        self.assertFalse(missing["success"])

        with patch.object(feishu_module, "_load_lark_oapi", return_value=True), patch.object(
            feishu_module.FeishuAdapter,
            "_build_lark_client",
            side_effect=RuntimeError("client failed"),
        ):
            failed = asyncio.run(
                feishu_module._standalone_send_authorized(
                    PlatformConfig(),
                    "oc_chat",
                    "notice",
                    transport_request_id="request-client-error",
                )
            )
        self.assertEqual(failed["error"], "Feishu strict send failed: client failed")

        async def cancelled():
            with patch.object(feishu_module, "_load_lark_oapi", return_value=True), patch.object(
                feishu_module.FeishuAdapter,
                "_build_lark_client",
                side_effect=asyncio.CancelledError,
            ):
                await feishu_module._standalone_send_authorized(
                    PlatformConfig(),
                    "oc_chat",
                    "notice",
                    transport_request_id="request-cancelled",
                )

        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(cancelled())

    def test_outer_send_retry_does_not_duplicate_timeout(self):
        import httpx
        import requests

        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        timeout_errors = (
            requests.exceptions.ReadTimeout(),
            requests.exceptions.Timeout(),
            httpx.ReadTimeout(""),
            asyncio.TimeoutError(),
        )
        for timeout_error in timeout_errors:
            with self.subTest(error_type=type(timeout_error).__name__):
                adapter = FeishuAdapter(PlatformConfig())
                adapter._client = object()
                adapter._send_raw_message = AsyncMock(side_effect=timeout_error)

                result = asyncio.run(
                    adapter._send_with_retry(chat_id="oc_chat", content="hello")
                )

                self.assertFalse(result.success)
                self.assertIn("timed out", (result.error or "").lower())
                adapter._send_raw_message.assert_awaited_once()

    def test_sdk_client_uses_explicit_request_timeout(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu import adapter as feishu_module

        adapter = feishu_module.FeishuAdapter(PlatformConfig())
        adapter._app_id = "cli_app"
        adapter._app_secret = "secret_app"
        builder = Mock()
        builder.app_id.return_value = builder
        builder.app_secret.return_value = builder
        builder.domain.return_value = builder
        builder.timeout.return_value = builder
        builder.log_level.return_value = builder
        builder.build.return_value = object()
        fake_lark = SimpleNamespace(
            Client=SimpleNamespace(builder=Mock(return_value=builder)),
            LogLevel=SimpleNamespace(WARNING="WARNING"),
        )

        with patch.object(feishu_module, "lark", fake_lark):
            adapter._build_lark_client("https://open.feishu.cn")

        builder.timeout.assert_called_once_with(
            feishu_module._FEISHU_SDK_REQUEST_TIMEOUT_SECONDS
        )


class TestAdapterModule(unittest.TestCase):
    def test_load_settings_uses_sdk_defaults_for_invalid_ws_reconnect_values(self):
        from plugins.platforms.feishu.adapter import FeishuAdapter

        settings = FeishuAdapter._load_settings(
            {
                "ws_reconnect_nonce": -1,
                "ws_reconnect_interval": "bad",
            }
        )

        self.assertEqual(settings.ws_reconnect_nonce, 30)
        self.assertEqual(settings.ws_reconnect_interval, 120)


    def test_runtime_ws_overrides_reapply_after_sdk_configure(self):
        import sys
        from types import ModuleType

        class _FakeWSClient:
            def __init__(self):
                self._reconnect_nonce = 30
                self._reconnect_interval = 120
                self._ping_interval = 120
                self.configure_calls = []

            def _configure(self, conf):
                self.configure_calls.append(conf)
                self._reconnect_nonce = conf.ReconnectNonce
                self._reconnect_interval = conf.ReconnectInterval
                self._ping_interval = conf.PingInterval

            def start(self):
                conf = SimpleNamespace(ReconnectNonce=99, ReconnectInterval=88, PingInterval=77)
                self._configure(conf)
                raise RuntimeError("stop test client")

        fake_client = _FakeWSClient()
        fake_adapter = SimpleNamespace(
            _ws_thread_loop=None,
            _ws_reconnect_nonce=2,
            _ws_reconnect_interval=3,
            _ws_ping_interval=4,
            _ws_ping_timeout=5,
        )
        fake_client_module = ModuleType("lark_oapi.ws.client")
        fake_client_module.loop = None
        fake_client_module.websockets = SimpleNamespace(connect=AsyncMock())
        fake_ws_module = ModuleType("lark_oapi.ws")
        fake_ws_module.client = fake_client_module
        fake_root_module = ModuleType("lark_oapi")
        fake_root_module.ws = fake_ws_module

        original_modules = sys.modules.copy()
        sys.modules["lark_oapi"] = fake_root_module
        sys.modules["lark_oapi.ws"] = fake_ws_module
        sys.modules["lark_oapi.ws.client"] = fake_client_module
        try:
            from plugins.platforms.feishu.adapter import _run_official_feishu_ws_client

            _run_official_feishu_ws_client(fake_client, fake_adapter)
        finally:
            sys.modules.clear()
            sys.modules.update(original_modules)

        self.assertEqual(len(fake_client.configure_calls), 1)
        self.assertEqual(fake_client._reconnect_nonce, 2)
        self.assertEqual(fake_client._reconnect_interval, 3)
        self.assertEqual(fake_client._ping_interval, 4)


def _admits_group(adapter, message, sender_id, chat_id=""):
    """Group-path shim: run a message through ``_admit`` and return a bool."""
    sender = SimpleNamespace(sender_type="user", sender_id=sender_id)
    if not hasattr(message, "chat_type"):
        message.chat_type = "group"
    if chat_id:
        message.chat_id = chat_id
    return adapter._admit(sender, message) is None


class TestAdapterBehavior(unittest.TestCase):
    @patch.dict(os.environ, {}, clear=True)
    def test_build_event_handler_registers_reaction_and_card_processors(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        calls = []

        class _Builder:
            def register_p2_im_message_message_read_v1(self, _handler):
                calls.append("message_read")
                return self

            def register_p2_im_message_receive_v1(self, _handler):
                calls.append("message_receive")
                return self

            def register_p2_im_message_reaction_created_v1(self, _handler):
                calls.append("reaction_created")
                return self

            def register_p2_im_message_reaction_deleted_v1(self, _handler):
                calls.append("reaction_deleted")
                return self

            def register_p2_card_action_trigger(self, _handler):
                calls.append("card_action")
                return self

            def register_p2_im_chat_member_bot_added_v1(self, _handler):
                calls.append("bot_added")
                return self

            def register_p2_im_chat_member_bot_deleted_v1(self, _handler):
                calls.append("bot_deleted")
                return self

            def register_p2_im_chat_access_event_bot_p2p_chat_entered_v1(self, _handler):
                calls.append("p2p_chat_entered")
                return self

            def register_p2_im_message_recalled_v1(self, _handler):
                calls.append("message_recalled")
                return self

            def register_p2_customized_event(self, event_key, _handler):
                calls.append(f"customized:{event_key}")
                return self

            def build(self):
                calls.append("build")
                return "handler"

        class _Dispatcher:
            @staticmethod
            def builder(_encrypt_key, _verification_token):
                calls.append("builder")
                return _Builder()

        with patch("plugins.platforms.feishu.adapter.EventDispatcherHandler", _Dispatcher):
            handler = adapter._build_event_handler()

        self.assertEqual(handler, "handler")
        self.assertEqual(
            calls,
            [
                "builder",
                "message_read",
                "message_receive",
                "reaction_created",
                "reaction_deleted",
                "card_action",
                "bot_added",
                "bot_deleted",
                "p2p_chat_entered",
                "message_recalled",
                "customized:drive.notice.comment_add_v1",
                "customized:vc.bot.meeting_invited_v1",
                "build",
            ],
        )

    @patch.dict(os.environ, {}, clear=True)
    def test_bot_origin_reactions_are_dropped_to_avoid_feedback_loops(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        adapter._loop = object()

        for emoji in ("Typing", "CrossMark"):
            event = SimpleNamespace(
                message_id="om_msg",
                operator_type="bot",
                reaction_type=SimpleNamespace(emoji_type=emoji),
            )
            data = SimpleNamespace(event=event)
            with patch(
                "plugins.platforms.feishu.adapter.asyncio.run_coroutine_threadsafe"
            ) as run_threadsafe:
                adapter._on_reaction_event("im.message.reaction.created_v1", data)
            run_threadsafe.assert_not_called()

    @patch.dict(os.environ, {}, clear=True)
    def test_user_reaction_with_managed_emoji_is_still_routed(self):
        # Operator-origin filter is enough to prevent feedback loops; we must
        # not additionally swallow user-origin reactions just because their
        # emoji happens to collide with a lifecycle emoji.
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        adapter._loop = SimpleNamespace(is_closed=lambda: False)

        event = SimpleNamespace(
            message_id="om_msg",
            operator_type="user",
            reaction_type=SimpleNamespace(emoji_type="Typing"),
        )
        data = SimpleNamespace(event=event)

        def _close_coro_and_return_future(coro, _loop):
            coro.close()
            return SimpleNamespace(add_done_callback=lambda _: None)

        with patch(
            "plugins.platforms.feishu.adapter.asyncio.run_coroutine_threadsafe",
            side_effect=_close_coro_and_return_future,
        ) as run_threadsafe:
            adapter._on_reaction_event("im.message.reaction.created_v1", data)
        run_threadsafe.assert_called_once()

    def _build_reaction_adapter(self, *, msg_sender_id: str):
        """Build a FeishuAdapter wired up to return a single GET-message result."""
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        adapter._app_id = "cli_self_app"
        adapter._bot_open_id = "ou_self_bot"
        adapter._bot_user_id = "u_self_bot"

        msg = SimpleNamespace(
            sender=SimpleNamespace(sender_type="app", id=msg_sender_id, id_type="app_id"),
            chat_id="oc_chat",
            chat_type="group",
        )
        response = SimpleNamespace(success=lambda: True, data=SimpleNamespace(items=[msg]))
        adapter._client = SimpleNamespace(
            im=SimpleNamespace(
                v1=SimpleNamespace(message=SimpleNamespace(get=Mock(return_value=response)))
            )
        )
        adapter._build_get_message_request = Mock(return_value=object())
        adapter._handle_message_with_guards = AsyncMock()
        adapter._resolve_sender_profile = AsyncMock(
            return_value={"user_id": "u_human", "user_name": "Human", "user_id_alt": None}
        )
        adapter.get_chat_info = AsyncMock(return_value={"name": "Test Chat"})
        return adapter

    @patch.dict(os.environ, {}, clear=True)
    def test_reaction_on_peer_bot_message_is_not_routed(self):
        # GET im/v1/messages sender for bot messages carries id=app_id; a peer
        # bot's message has a different app_id than ours, so it must be dropped.
        adapter = self._build_reaction_adapter(msg_sender_id="cli_peer_app")

        event = SimpleNamespace(
            message_id="om_peer_msg",
            user_id=SimpleNamespace(open_id="ou_human", user_id=None, union_id=None),
            reaction_type=SimpleNamespace(emoji_type="THUMBSUP"),
        )
        data = SimpleNamespace(event=event)
        asyncio.run(
            adapter._handle_reaction_event("im.message.reaction.created_v1", data)
        )
        adapter._handle_message_with_guards.assert_not_awaited()


    def test_per_group_allowlist_policy_gates_by_sender(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        config = PlatformConfig(
            extra={
                "group_rules": {
                    "oc_chat_a": {
                        "policy": "allowlist",
                        "allowlist": ["ou_alice", "ou_bob"],
                    }
                }
            }
        )
        adapter = FeishuAdapter(config)
        adapter._bot_open_id = "ou_bot"

        message = SimpleNamespace(
            mentions=[SimpleNamespace(name="Bot", id=SimpleNamespace(open_id="ou_bot", user_id=None))]
        )

        self.assertTrue(
            _admits_group(adapter,
                message,
                SimpleNamespace(open_id="ou_alice", user_id=None),
                "oc_chat_a",
            )
        )
        self.assertFalse(
            _admits_group(adapter,
                message,
                SimpleNamespace(open_id="ou_charlie", user_id=None),
                "oc_chat_a",
            )
        )


    def test_global_admins_bypass_all_group_rules(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        config = PlatformConfig(
            extra={
                "admins": ["ou_admin"],
                "group_rules": {
                    "oc_chat_e": {
                        "policy": "allowlist",
                        "allowlist": ["ou_alice"],
                    }
                },
            }
        )
        adapter = FeishuAdapter(config)
        adapter._bot_open_id = "ou_bot"

        message = SimpleNamespace(
            mentions=[SimpleNamespace(name="Bot", id=SimpleNamespace(open_id="ou_bot", user_id=None))]
        )

        self.assertTrue(
            _admits_group(adapter,
                message,
                SimpleNamespace(open_id="ou_admin", user_id=None),
                "oc_chat_e",
            )
        )

    def test_default_group_policy_fallback_for_chats_without_explicit_rule(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        config = PlatformConfig(
            extra={
                "default_group_policy": "open",
            }
        )
        adapter = FeishuAdapter(config)
        adapter._bot_open_id = "ou_bot"

        message = SimpleNamespace(
            mentions=[SimpleNamespace(name="Bot", id=SimpleNamespace(open_id="ou_bot", user_id=None))]
        )

        self.assertTrue(
            _admits_group(adapter,
                message,
                SimpleNamespace(open_id="ou_anyone", user_id=None),
                "oc_chat_unknown",
            )
        )


    @patch.dict(os.environ, {"FEISHU_GROUP_POLICY": "open"}, clear=True)
    def test_group_message_matches_bot_name_when_only_name_available(self):
        """Name fallback engages when either side lacks an open_id. When BOTH
        the mention and the bot carry open_ids, IDs are authoritative — a
        same-name human with a different open_id must NOT admit."""
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        # Case 1: bot has only a name (open_id not hydrated / not configured).
        # Name fallback is the only available signal for any mention.
        adapter = FeishuAdapter(PlatformConfig())
        adapter._bot_name = "Hermes Bot"
        sender_id = SimpleNamespace(open_id="ou_any", user_id=None)

        name_only_mention = SimpleNamespace(
            name="Hermes Bot",
            id=SimpleNamespace(open_id=None, user_id=None),
        )
        different_mention = SimpleNamespace(
            name="Another Bot",
            id=SimpleNamespace(open_id=None, user_id=None),
        )

        self.assertTrue(
            _admits_group(adapter, SimpleNamespace(mentions=[name_only_mention]), sender_id, "")
        )
        self.assertFalse(
            _admits_group(adapter, SimpleNamespace(mentions=[different_mention]), sender_id, "")
        )

        # Case 2: bot's open_id IS known — a same-name human with different
        # open_id must NOT admit (IDs override names).
        adapter2 = FeishuAdapter(PlatformConfig())
        adapter2._bot_open_id = "ou_bot"
        adapter2._bot_name = "Hermes Bot"

        same_name_other_id_mention = SimpleNamespace(
            name="Hermes Bot",
            id=SimpleNamespace(open_id="ou_other", user_id="u_other"),
        )
        bot_mention = SimpleNamespace(
            name="Hermes Bot",
            id=SimpleNamespace(open_id="ou_bot", user_id=None),
        )

        self.assertFalse(
            _admits_group(
                adapter2,
                SimpleNamespace(mentions=[same_name_other_id_mention]),
                sender_id,
                "",
            )
        )
        self.assertTrue(
            _admits_group(adapter2, SimpleNamespace(mentions=[bot_mention]), sender_id, "")
        )


    @patch.dict(os.environ, {}, clear=True)
    def test_extract_post_message_downloads_embedded_resources(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        adapter._download_feishu_image = AsyncMock(return_value=("/tmp/feishu-image.png", "image/png"))
        adapter._download_feishu_message_resource = AsyncMock(return_value=("/tmp/spec.pdf", "application/pdf"))
        message = SimpleNamespace(
            message_type="post",
            content=(
                '{"en_us":{"title":"Rich message","content":['
                '[{"tag":"img","image_key":"img_123","alt":"diagram"}],'
                '[{"tag":"media","file_key":"file_123","file_name":"spec.pdf"}]'
                ']}}'
            ),
            message_id="om_post_media",
        )

        text, msg_type, media_urls, media_types, _mentions = asyncio.run(adapter._extract_message_content(message))

        self.assertEqual(text, "Rich message\n[Image: diagram]\n[Attachment: spec.pdf]")
        self.assertEqual(msg_type.value, "text")
        self.assertEqual(media_urls, ["/tmp/feishu-image.png", "/tmp/spec.pdf"])
        self.assertEqual(media_types, ["image/png", "application/pdf"])
        adapter._download_feishu_image.assert_awaited_once_with(
            message_id="om_post_media",
            image_key="img_123",
        )
        adapter._download_feishu_message_resource.assert_awaited_once_with(
            message_id="om_post_media",
            file_key="file_123",
            resource_type="file",
            fallback_filename="spec.pdf",
        )


    @patch.dict(os.environ, {}, clear=True)
    def test_extract_audio_message_downloads_and_caches(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        adapter._download_feishu_message_resource = AsyncMock(
            return_value=("/tmp/feishu-audio.ogg", "audio/ogg")
        )
        message = SimpleNamespace(
            message_type="audio",
            content='{"file_key":"file_audio","file_name":"voice.ogg"}',
            message_id="om_audio",
        )

        text, msg_type, media_urls, media_types, _mentions = asyncio.run(adapter._extract_message_content(message))

        self.assertEqual(text, "")
        # Lark "audio" msg_type is a native voice recording (the fixture is
        # literally voice.ogg) — it must classify as VOICE so the gateway
        # auto-transcribes it, not AUDIO (a non-transcribed file attachment).
        # See the #28993 follow-up fix in _resolve_normalized_message_type.
        self.assertEqual(msg_type.value, "voice")
        self.assertEqual(media_urls, ["/tmp/feishu-audio.ogg"])
        self.assertEqual(media_types, ["audio/ogg"])


    @patch.dict(os.environ, {}, clear=True)
    def test_extract_text_message_starting_with_slash_becomes_command(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        adapter._dispatch_inbound_event = AsyncMock()
        adapter.get_chat_info = AsyncMock(
            return_value={"chat_id": "oc_chat", "name": "Feishu DM", "type": "dm"}
        )
        adapter._resolve_sender_profile = AsyncMock(
            return_value={"user_id": "ou_user", "user_name": "张三", "user_id_alt": None}
        )
        message = SimpleNamespace(
            chat_id="oc_chat",
            thread_id=None,
            parent_id=None,
            upper_message_id=None,
            message_type="text",
            content='{"text":"/help test"}',
            message_id="om_command",
        )

        asyncio.run(
            adapter._process_inbound_message(
                data=SimpleNamespace(event=SimpleNamespace(message=message)),
                message=message,
                sender_id=SimpleNamespace(open_id="ou_user", user_id=None, union_id=None),
                is_bot=False,
                chat_type="p2p",
                message_id="om_command",
            )
        )

        event = adapter._dispatch_inbound_event.await_args.args[0]
        self.assertEqual(event.message_type.value, "command")
        self.assertEqual(event.text, "/help test")

    @patch.dict(os.environ, {}, clear=True)
    def test_extract_text_file_injects_content(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as tmp:
            tmp.write("hello from feishu")
            path = tmp.name

        try:
            text = asyncio.run(adapter._maybe_extract_text_document(path, "text/plain"))
        finally:
            os.unlink(path)

        self.assertIn("hello from feishu", text)
        self.assertIn("[Content of", text)

    @patch.dict(os.environ, {}, clear=True)
    def test_message_event_submits_to_adapter_loop(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())

        class _Loop:
            def is_closed(self):
                return False

        adapter._loop = _Loop()

        message = SimpleNamespace(
            message_id="om_text",
            chat_type="p2p",
            chat_id="oc_chat",
            message_type="text",
            content='{"text":"hello"}',
        )
        sender_id = SimpleNamespace(open_id="ou_user", user_id=None, union_id=None)
        sender = SimpleNamespace(sender_id=sender_id, sender_type="user")
        data = SimpleNamespace(event=SimpleNamespace(message=message, sender=sender))

        future = SimpleNamespace(add_done_callback=lambda *_args, **_kwargs: None)

        def _submit(coro, _loop):
            coro.close()
            return future

        with patch("plugins.platforms.feishu.adapter.asyncio.run_coroutine_threadsafe", side_effect=_submit) as submit:
            adapter._on_message_event(data)

        self.assertTrue(submit.called)

    @patch.dict(os.environ, {}, clear=True)
    def test_webhook_request_uses_same_message_dispatch_path(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        adapter._on_message_event = Mock()

        body = json.dumps({
            "header": {"event_type": "im.message.receive_v1"},
            "event": {"message": {"message_id": "om_test"}},
        }).encode("utf-8")
        request = SimpleNamespace(
            remote="127.0.0.1",
            content_length=None,
            headers={},
            content=_FakeRequestContent(body),
        )

        response = asyncio.run(adapter._handle_webhook_request(request))

        self.assertEqual(response.status, 200)
        adapter._on_message_event.assert_called_once()

    @patch.dict(os.environ, {"FEISHU_VERIFICATION_TOKEN": "expected-token"}, clear=True)
    def test_url_verification_requires_configured_verification_token(self):
        """url_verification must be rejected when token is set but mismatched.

        Regression: previously the challenge was reflected before the token
        check, so an unauthenticated remote could prove endpoint control by
        sending an attacker-controlled challenge string.
        """
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        body = json.dumps({
            "type": "url_verification",
            "token": "wrong-token",
            "challenge": "attacker-controlled-challenge",
        }).encode("utf-8")
        request = SimpleNamespace(
            remote="203.0.113.10",
            content_length=None,
            headers={},
            content=_FakeRequestContent(body),
        )

        response = asyncio.run(adapter._handle_webhook_request(request))

        self.assertEqual(response.status, 401)

    @patch.dict(os.environ, {}, clear=True)
    def test_process_inbound_message_uses_event_sender_identity_only(self):
        from gateway.config import PlatformConfig
        from gateway.platforms.base import MessageType
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        adapter._dispatch_inbound_event = AsyncMock()
        # Sender name now comes from the contact API; mock it to return a known value.
        adapter._resolve_sender_name_from_api = AsyncMock(return_value="张三")
        adapter.get_chat_info = AsyncMock(
            return_value={"chat_id": "oc_chat", "name": "Feishu DM", "type": "dm"}
        )
        message = SimpleNamespace(
            chat_id="oc_chat",
            thread_id=None,
            message_type="text",
            content='{"text":"hello"}',
            message_id="om_text",
        )
        sender_id = SimpleNamespace(
            open_id="ou_user",
            user_id="u_user",
            union_id="on_union",
        )
        sender = SimpleNamespace(sender_type="user", sender_id=sender_id)
        data = SimpleNamespace(event=SimpleNamespace(message=message, sender=sender))

        asyncio.run(
            adapter._process_inbound_message(
                data=data,
                message=message,
                sender_id=sender.sender_id,
                chat_type="p2p",
                message_id="om_text",
            )
        )

        adapter._dispatch_inbound_event.assert_awaited_once()
        event = adapter._dispatch_inbound_event.await_args.args[0]
        self.assertEqual(event.message_type, MessageType.TEXT)
        self.assertEqual(event.source.user_id, "u_user")  # tenant-scoped user_id preferred over app-scoped open_id
        self.assertEqual(event.source.user_name, "张三")
        self.assertEqual(event.source.user_id_alt, "on_union")
        self.assertEqual(event.source.chat_name, "Feishu DM")


    @patch.dict(
        os.environ,
        {
            "HERMES_FEISHU_TEXT_BATCH_MAX_MESSAGES": "2",
        },
        clear=True,
    )
    def test_text_batch_flushes_when_message_count_limit_is_hit(self):
        from gateway.config import PlatformConfig
        from gateway.platforms.base import MessageEvent, MessageType
        from plugins.platforms.feishu.adapter import FeishuAdapter
        from gateway.session import SessionSource

        adapter = FeishuAdapter(PlatformConfig())
        adapter.handle_message = AsyncMock()
        source = SessionSource(
            platform=adapter.platform,
            chat_id="oc_chat",
            chat_name="Feishu DM",
            chat_type="dm",
            user_id="ou_user",
            user_name="张三",
        )

        async def _sleep(_delay):
            return None

        async def _run() -> None:
            with patch("plugins.platforms.feishu.adapter.asyncio.sleep", side_effect=_sleep):
                await adapter._dispatch_inbound_event(
                    MessageEvent(text="A", message_type=MessageType.TEXT, source=source, message_id="om_1")
                )
                await adapter._dispatch_inbound_event(
                    MessageEvent(text="B", message_type=MessageType.TEXT, source=source, message_id="om_2")
                )
                await adapter._dispatch_inbound_event(
                    MessageEvent(text="C", message_type=MessageType.TEXT, source=source, message_id="om_3")
                )
                pending = list(adapter._pending_text_batch_tasks.values())
                self.assertEqual(len(pending), 1)
                await asyncio.gather(*pending, return_exceptions=True)

        asyncio.run(_run())

        self.assertEqual(adapter.handle_message.await_count, 2)
        first = adapter.handle_message.await_args_list[0].args[0]
        second = adapter.handle_message.await_args_list[1].args[0]
        self.assertEqual(first.text, "A\nB")
        self.assertEqual(second.text, "C")

    @patch.dict(os.environ, {}, clear=True)
    def test_media_batch_merges_rapid_photo_messages(self):
        from gateway.config import PlatformConfig
        from gateway.platforms.base import MessageEvent, MessageType
        from plugins.platforms.feishu.adapter import FeishuAdapter
        from gateway.session import SessionSource

        adapter = FeishuAdapter(PlatformConfig())
        adapter.handle_message = AsyncMock()
        source = SessionSource(
            platform=adapter.platform,
            chat_id="oc_chat",
            chat_name="Feishu DM",
            chat_type="dm",
            user_id="ou_user",
            user_name="张三",
        )

        async def _sleep(_delay):
            return None

        async def _run() -> None:
            with patch("plugins.platforms.feishu.adapter.asyncio.sleep", side_effect=_sleep):
                await adapter._dispatch_inbound_event(
                    MessageEvent(
                        text="第一张",
                        message_type=MessageType.PHOTO,
                        source=source,
                        message_id="om_p1",
                        media_urls=["/tmp/a.png"],
                        media_types=["image/png"],
                    )
                )
                await adapter._dispatch_inbound_event(
                    MessageEvent(
                        text="第二张",
                        message_type=MessageType.PHOTO,
                        source=source,
                        message_id="om_p2",
                        media_urls=["/tmp/b.png"],
                        media_types=["image/png"],
                    )
                )
                pending = list(adapter._pending_media_batch_tasks.values())
                self.assertEqual(len(pending), 1)
                await asyncio.gather(*pending, return_exceptions=True)

        asyncio.run(_run())

        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.await_args.args[0]
        self.assertEqual(event.media_urls, ["/tmp/a.png", "/tmp/b.png"])
        self.assertIn("第一张", event.text)
        self.assertIn("第二张", event.text)


    def test_download_remote_document_reads_response_before_httpx_client_closes(self):
        """#18451 — snapshot Content-Type + body while the httpx.AsyncClient
        context is still active so pooled connections fully release on
        exit.  Otherwise the response is only readable because httpx
        eagerly buffers it; a future refactor to .stream() would silently
        read-after-close."""
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        events: list[str] = []

        class _FakeResponse:
            headers = {"Content-Type": "application/octet-stream"}

            def raise_for_status(self) -> None:
                events.append("raise_for_status")

            @property
            def content(self) -> bytes:
                events.append("content_read")
                return b"doc-bytes"

        class _FakeAsyncClient:
            def __init__(self, *_a: object, **_k: object) -> None:
                pass

            async def __aenter__(self) -> "_FakeAsyncClient":
                events.append("client_enter")
                return self

            async def __aexit__(self, *exc: object) -> None:
                events.append("client_exit")

            async def get(self, *_a: object, **_k: object) -> _FakeResponse:
                events.append("get")
                return _FakeResponse()

        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, {"HERMES_HOME": tmp}, clear=False):
                adapter = FeishuAdapter(PlatformConfig())

                async def _run() -> tuple[str, str]:
                    with patch("tools.url_safety.is_safe_url", return_value=True):
                        with patch(
                            "tools.url_safety.create_ssrf_safe_async_client",
                            side_effect=lambda **_kwargs: _FakeAsyncClient(),
                        ):
                            with patch(
                                "plugins.platforms.feishu.adapter.cache_document_from_bytes",
                                return_value="/tmp/cached-doc.bin",
                            ):
                                return await adapter._download_remote_document(
                                    "https://example.com/doc.bin",
                                    default_ext=".bin",
                                    preferred_name="doc",
                                )

                path, filename = asyncio.run(_run())

        self.assertEqual(path, "/tmp/cached-doc.bin")
        self.assertTrue(filename)
        # content_read MUST happen before client_exit — otherwise we're
        # reading response body after the connection pool has been torn
        # down, which only works by accident (httpx's eager buffering).
        self.assertLess(events.index("content_read"), events.index("client_exit"))

    def test_download_remote_document_blocks_connect_time_rebind(self):
        import httpcore
        from httpcore._backends.auto import AutoBackend
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter
        from tools.url_safety import SSRFConnectionBlocked

        adapter = FeishuAdapter(PlatformConfig())
        answers = iter(("93.184.216.34", "169.254.169.254"))

        def fake_getaddrinfo(_host, port, *_args, **_kwargs):
            ip = next(answers)
            return [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, port or 0))
            ]

        connect_attempts = []

        async def fake_connect_tcp(
            _self,
            host,
            port,
            timeout=None,
            local_address=None,
            socket_options=None,
        ):
            connect_attempts.append((host, port))
            raise httpcore.ConnectError("stop before network")

        proxy_vars = {
            name: ""
            for name in (
                "HTTP_PROXY",
                "HTTPS_PROXY",
                "ALL_PROXY",
                "http_proxy",
                "https_proxy",
                "all_proxy",
            )
        }
        with (
            patch.dict(os.environ, proxy_vars, clear=False),
            patch("socket.getaddrinfo", side_effect=fake_getaddrinfo),
            patch.object(AutoBackend, "connect_tcp", new=fake_connect_tcp),
            self.assertRaises(SSRFConnectionBlocked),
        ):
            asyncio.run(
                adapter._download_remote_document(
                    "http://rebind.example/doc.bin",
                    default_ext=".bin",
                    preferred_name="doc",
                )
            )

        self.assertEqual(connect_attempts, [])

    def test_dedup_state_persists_across_adapter_restart(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        with tempfile.TemporaryDirectory() as temp_home:
            with patch.dict(os.environ, {"HERMES_HOME": temp_home}, clear=False):
                first = FeishuAdapter(PlatformConfig())
                self.assertFalse(first._is_duplicate("om_same"))
                second = FeishuAdapter(PlatformConfig())
                self.assertTrue(second._is_duplicate("om_same"))


    @patch.dict(os.environ, {}, clear=True)
    def test_send_document_reply_uses_thread_flag(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        captured = {}

        class _FileAPI:
            def create(self, request):
                return SimpleNamespace(
                    success=lambda: True,
                    data=SimpleNamespace(file_key="file_123"),
                )

        class _MessageAPI:
            def reply(self, request):
                captured["request"] = request
                return SimpleNamespace(
                    success=lambda: True,
                    data=SimpleNamespace(message_id="om_file_reply"),
                )

        adapter._client = SimpleNamespace(
            im=SimpleNamespace(
                v1=SimpleNamespace(
                    file=_FileAPI(),
                    message=_MessageAPI(),
                )
            )
        )

        async def _direct(func, *args, **kwargs):
            return func(*args, **kwargs)

        with tempfile.NamedTemporaryFile("wb", suffix=".pdf", delete=False) as tmp:
            tmp.write(b"%PDF-1.4 test")
            file_path = tmp.name

        try:
            with patch("plugins.platforms.feishu.adapter.asyncio.to_thread", side_effect=_direct):
                result = asyncio.run(
                    adapter.send_document(
                        chat_id="oc_chat",
                        file_path=file_path,
                        reply_to="om_parent",
                        metadata={"thread_id": "omt-thread"},
                    )
                )
        finally:
            os.unlink(file_path)

        self.assertTrue(result.success)
        self.assertTrue(captured["request"].request_body.reply_in_thread)


    @patch.dict(os.environ, {}, clear=True)
    def test_send_uses_post_for_every_chunk_of_multi_chunk_markdown(self):
        """Regression for #26841: when a long Markdown message is split
        across multiple chunks, every chunk must go out as
        ``msg_type=post`` — including chunk 1.  The bug was that the
        first chunk often had only plain prose (the per-chunk regex
        didn't match) and was sent as ``text``, so users saw literal
        ``**bold``/``## heading``/code fences while later chunks
        rendered correctly.
        """
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        captured = []

        class _MessageAPI:
            def create(self, request):
                captured.append(request)
                return SimpleNamespace(
                    success=lambda: True,
                    data=SimpleNamespace(
                        message_id=f"om_chunk_{len(captured)}",
                    ),
                )

        adapter._client = SimpleNamespace(
            im=SimpleNamespace(
                v1=SimpleNamespace(
                    message=_MessageAPI(),
                )
            )
        )

        async def _direct(func, *args, **kwargs):
            return func(*args, **kwargs)

        # Force a deterministic split so the test doesn't depend on the
        # exact 8000-char limit.  Chunk 1 is plain prose; chunk 2 has
        # the markdown markers.  Without the fix, chunk 1 went out as
        # ``msg_type=text``.
        first_chunk = "Here is a short intro that has no markdown markers at all."
        second_chunk = "## Heading\nAnd then some **bold** text."

        with patch.object(
            adapter, "truncate_message", return_value=[first_chunk, second_chunk],
        ), patch("plugins.platforms.feishu.adapter.asyncio.to_thread", side_effect=_direct):
            result = asyncio.run(
                adapter.send(
                    chat_id="oc_chat",
                    content=first_chunk + "\n" + second_chunk,
                )
            )

        self.assertTrue(result.success)
        self.assertEqual(len(captured), 2)
        msg_types = [r.request_body.msg_type for r in captured]
        self.assertEqual(msg_types, ["post", "post"])


    @patch.dict(os.environ, {}, clear=True)
    def test_send_splits_fenced_code_blocks_into_separate_post_rows(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        captured = {}

        class _MessageAPI:
            def create(self, request):
                captured["request"] = request
                return SimpleNamespace(
                    success=lambda: True,
                    data=SimpleNamespace(message_id="om_codeblock"),
                )

        adapter._client = SimpleNamespace(
            im=SimpleNamespace(
                v1=SimpleNamespace(
                    message=_MessageAPI(),
                )
            )
        )

        async def _direct(func, *args, **kwargs):
            return func(*args, **kwargs)

        content = (
            "确认已入库 ✓\n"
            "文件路径：`/root/.hermes/profiles/agent_cto/cron/jobs.json`\n"
            "**解码后的内容：**\n"
            "```json\n"
            '{"cron": "list"}\n'
            "```\n"
            "后续说明仍应保留。"
        )

        with patch("plugins.platforms.feishu.adapter.asyncio.to_thread", side_effect=_direct):
            result = asyncio.run(
                adapter.send(
                    chat_id="oc_chat",
                    content=content,
                )
            )

        self.assertTrue(result.success)
        self.assertEqual(captured["request"].request_body.msg_type, "post")
        payload = json.loads(captured["request"].request_body.content)
        rows = payload["zh_cn"]["content"]
        self.assertEqual(
            rows,
            [
                [
                    {
                        "tag": "md",
                        "text": "确认已入库 ✓\n文件路径：`/root/.hermes/profiles/agent_cto/cron/jobs.json`\n**解码后的内容：**",
                    }
                ],
                [{"tag": "md", "text": "```json\n{\"cron\": \"list\"}\n```"}],
                [{"tag": "md", "text": "后续说明仍应保留。"}],
            ],
        )


@unittest.skipUnless(_HAS_LARK_OAPI, "lark-oapi not installed")
class TestHydrateBotIdentity(unittest.TestCase):
    """Hydration of bot identity via ``/open-apis/bot/v3/info``.

    Covers the manual-setup path where ``FEISHU_BOT_OPEN_ID`` /
    ``FEISHU_BOT_NAME`` are not configured — hydration populates them so
    self-echo protection and group @mention gating both have something to
    match against.
    """

    def _make_adapter(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        return FeishuAdapter(PlatformConfig())

    @patch.dict(os.environ, {}, clear=True)
    def test_hydration_populates_open_id_from_bot_info(self):
        adapter = self._make_adapter()
        adapter._client = Mock()
        payload = json.dumps(
            {
                "code": 0,
                "bot": {
                    "bot_name": "Hermes Bot",
                    "open_id": "ou_hermes_hydrated",
                },
            }
        ).encode("utf-8")
        response = SimpleNamespace(raw=SimpleNamespace(content=payload))
        adapter._client.request = Mock(return_value=response)

        asyncio.run(adapter._hydrate_bot_identity())

        self.assertEqual(adapter._bot_open_id, "ou_hermes_hydrated")
        self.assertEqual(adapter._bot_name, "Hermes Bot")

    @patch.dict(
        os.environ,
        {
            "FEISHU_BOT_OPEN_ID": "ou_env",
            "FEISHU_BOT_NAME": "Env Hermes",
        },
        clear=True,
    )
    def test_hydration_refreshes_env_values_when_bot_info_available(self):
        adapter = self._make_adapter()
        adapter._client = Mock()
        payload = json.dumps(
            {
                "code": 0,
                "bot": {
                    "bot_name": "Hydrated Hermes",
                    "open_id": "ou_hydrated",
                },
            }
        ).encode("utf-8")
        adapter._client.request = Mock(return_value=SimpleNamespace(raw=SimpleNamespace(content=payload)))

        asyncio.run(adapter._hydrate_bot_identity())

        # PR #16993 semantics: /bot/v3/info probe runs unconditionally
        # and hydrated values win over env vars so a stale FEISHU_BOT_*
        # from an old app registration doesn't break @mention gating.
        adapter._client.request.assert_called_once()
        self.assertEqual(adapter._bot_open_id, "ou_hydrated")
        self.assertEqual(adapter._bot_name, "Hydrated Hermes")


@unittest.skipUnless(_HAS_LARK_OAPI, "lark-oapi not installed")
class TestPendingInboundQueue(unittest.TestCase):
    """Tests for the loop-not-ready race (#5499): inbound events arriving
    before or during adapter loop transitions must be queued for replay
    rather than silently dropped."""

    @patch.dict(os.environ, {}, clear=True)
    def test_event_queued_when_loop_not_ready(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        adapter._loop = None  # Simulate "before start()" or "during reconnect"

        with patch("plugins.platforms.feishu.adapter.threading.Thread") as thread_cls:
            adapter._on_message_event(SimpleNamespace(tag="evt-1"))
            adapter._on_message_event(SimpleNamespace(tag="evt-2"))
            adapter._on_message_event(SimpleNamespace(tag="evt-3"))

        # All three queued, none dropped.
        self.assertEqual(len(adapter._pending_inbound_events), 3)
        # Only ONE drainer thread scheduled, not one per event.
        self.assertEqual(thread_cls.call_count, 1)
        # Drain scheduled flag set.
        self.assertTrue(adapter._pending_drain_scheduled)

    @patch.dict(os.environ, {}, clear=True)
    def test_drainer_replays_queued_events_when_loop_becomes_ready(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        adapter._loop = None
        adapter._running = True

        class _ReadyLoop:
            def is_closed(self):
                return False

        # Queue three events while loop is None (simulate the race).
        events = [SimpleNamespace(tag=f"evt-{i}") for i in range(3)]
        with patch("plugins.platforms.feishu.adapter.threading.Thread"):
            for ev in events:
                adapter._on_message_event(ev)

        self.assertEqual(len(adapter._pending_inbound_events), 3)

        # Now the loop becomes ready; run the drainer inline (not as a thread)
        # to verify it replays the queue.
        adapter._loop = _ReadyLoop()

        future = SimpleNamespace(add_done_callback=lambda *_a, **_kw: None)
        submitted: list = []

        def _submit(coro, _loop):
            submitted.append(coro)
            coro.close()
            return future

        with patch(
            "plugins.platforms.feishu.adapter.asyncio.run_coroutine_threadsafe",
            side_effect=_submit,
        ) as submit:
            adapter._drain_pending_inbound_events()

        # All three events dispatched to the loop.
        self.assertEqual(submit.call_count, 3)
        # Queue emptied.
        self.assertEqual(len(adapter._pending_inbound_events), 0)
        # Drain flag reset so a future race can schedule a new drainer.
        self.assertFalse(adapter._pending_drain_scheduled)


@unittest.skipUnless(_HAS_LARK_OAPI, "lark-oapi not installed")
class TestWebhookSecurity(unittest.TestCase):
    """Tests for webhook signature verification, rate limiting, and body size limits."""

    def _make_adapter(self, encrypt_key: str = "") -> "FeishuAdapter":
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        with patch.dict(os.environ, {"FEISHU_APP_ID": "cli", "FEISHU_APP_SECRET": "sec", "FEISHU_ENCRYPT_KEY": encrypt_key}, clear=True):
            return FeishuAdapter(PlatformConfig())

    def test_signature_valid_passes(self):
        import hashlib

        encrypt_key = "test_secret"
        adapter = self._make_adapter(encrypt_key)
        body = b'{"type":"event"}'
        timestamp = "1700000000"
        nonce = "abc123"
        content = f"{timestamp}{nonce}{encrypt_key}" + body.decode("utf-8")
        sig = hashlib.sha256(content.encode("utf-8")).hexdigest()
        headers = {"x-lark-request-timestamp": timestamp, "x-lark-request-nonce": nonce, "x-lark-signature": sig}
        self.assertTrue(adapter._is_webhook_signature_valid(headers, body))


    def test_rate_limit_resets_after_window_expires(self):
        from plugins.platforms.feishu.adapter import _FEISHU_WEBHOOK_RATE_LIMIT_MAX, _FEISHU_WEBHOOK_RATE_WINDOW_SECONDS
        adapter = self._make_adapter()
        ip = "10.0.0.3"
        for _ in range(_FEISHU_WEBHOOK_RATE_LIMIT_MAX):
            adapter._check_webhook_rate_limit(ip)
        self.assertFalse(adapter._check_webhook_rate_limit(ip))
        # Simulate window expiry by backdating the stored entry.
        count, window_start = adapter._webhook_rate_counts[ip]
        adapter._webhook_rate_counts[ip] = (count, window_start - _FEISHU_WEBHOOK_RATE_WINDOW_SECONDS - 1)
        self.assertTrue(adapter._check_webhook_rate_limit(ip))


    def test_webhook_request_rejects_oversized_chunked_body_while_reading(self):
        from gateway.config import PlatformConfig
        from hermes_constants import reset_hermes_home_override, set_hermes_home_override
        from plugins.platforms.feishu.adapter import FeishuAdapter, _FEISHU_WEBHOOK_MAX_BODY_BYTES

        with tempfile.TemporaryDirectory() as tmpdir:
            token = set_hermes_home_override(tmpdir)
            try:
                adapter = FeishuAdapter(PlatformConfig())
            finally:
                reset_hermes_home_override(token)
            content = _FakeRequestContent(b"A" * (_FEISHU_WEBHOOK_MAX_BODY_BYTES + 2))
            request = SimpleNamespace(
                remote="127.0.0.1",
                content_length=None,
                headers={},
                content=content,
            )

            response = asyncio.run(adapter._handle_webhook_request(request))

            self.assertEqual(response.status, 413)
            self.assertEqual(content.read_sizes, [_FEISHU_WEBHOOK_MAX_BODY_BYTES + 1])


    @patch.dict(os.environ, {}, clear=True)
    def test_webhook_connect_requires_inbound_auth_secret(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(
            PlatformConfig(
                enabled=True,
                extra={"app_id": "cli_app", "app_secret": "secret_app", "connection_mode": "webhook"},
            )
        )
        self.assertFalse(asyncio.run(adapter.connect()))

    @patch.dict(os.environ, {}, clear=True)
    def test_webhook_loads_auth_secrets_from_platform_extra(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(
            PlatformConfig(
                enabled=True,
                extra={
                    "app_id": "cli_app",
                    "app_secret": "secret_app",
                    "connection_mode": "webhook",
                    "verification_token": "token_from_extra",
                    "encrypt_key": "encrypt_from_extra",
                },
            )
        )
        self.assertEqual(adapter._verification_token, "token_from_extra")
        self.assertEqual(adapter._encrypt_key, "encrypt_from_extra")


class TestDedupTTL(unittest.TestCase):
    """Tests for TTL-aware deduplication."""

    @patch.dict(os.environ, {}, clear=True)
    def test_duplicate_within_ttl_is_rejected(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        with patch.object(adapter, "_persist_seen_message_ids"):
            adapter._seen_message_ids = {"om_dup": time.time()}
            adapter._seen_message_order = ["om_dup"]
            self.assertTrue(adapter._is_duplicate("om_dup"))


    @patch.dict(os.environ, {}, clear=True)
    def test_load_tolerates_malformed_timestamp_values(self):
        """Regression #13632 — a non-numeric timestamp in the persisted
        dedup state must not crash adapter startup.  The bad key is
        skipped; the rest of the state loads.
        """
        import tempfile
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        with tempfile.TemporaryDirectory() as temp_home:
            with patch.dict(os.environ, {"HERMES_HOME": temp_home}, clear=True):
                adapter = FeishuAdapter(PlatformConfig())
                adapter._dedup_state_path.parent.mkdir(parents=True, exist_ok=True)
                adapter._dedup_state_path.write_text(
                    json.dumps(
                        {
                            "message_ids": {
                                "om_good": time.time(),
                                "om_bad_str": "not-a-timestamp",
                                "om_bad_null": None,
                            }
                        }
                    ),
                    encoding="utf-8",
                )
                adapter._load_seen_message_ids()
                assert "om_good" in adapter._seen_message_ids
                assert "om_bad_str" not in adapter._seen_message_ids
                assert "om_bad_null" not in adapter._seen_message_ids


class TestGroupMentionAtAll(unittest.TestCase):
    """Tests for @_all (Feishu @everyone) group mention routing."""


    @patch.dict(os.environ, {"FEISHU_GROUP_POLICY": "allowlist", "FEISHU_ALLOWED_USERS": "ou_allowed"}, clear=True)
    def test_at_all_still_requires_policy_gate(self):
        """@_all bypasses mention gating but NOT the allowlist policy."""
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        message = SimpleNamespace(content='{"text":"@_all attention"}', mentions=[])
        # Non-allowlisted user — should be blocked even with @_all.
        blocked_sender = SimpleNamespace(open_id="ou_blocked", user_id=None)
        self.assertFalse(_admits_group(adapter, message, blocked_sender, ""))
        # Allowlisted user — should pass.
        allowed_sender = SimpleNamespace(open_id="ou_allowed", user_id=None)
        self.assertTrue(_admits_group(adapter, message, allowed_sender, ""))


@unittest.skipUnless(_HAS_LARK_OAPI, "lark-oapi not installed")
class TestSenderNameResolution(unittest.TestCase):
    """Tests for _resolve_sender_name_from_api (contact API + cache)."""


    @patch.dict(os.environ, {}, clear=True)
    def test_returns_cached_name_within_ttl(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        adapter._client = SimpleNamespace()
        future_expire = time.time() + 600
        adapter._sender_name_cache["ou_cached"] = ("Alice", future_expire)
        result = asyncio.run(adapter._resolve_sender_name_from_api("ou_cached"))
        self.assertEqual(result, "Alice")

    @patch.dict(os.environ, {}, clear=True)
    def test_fetches_and_caches_name_from_api(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        user_obj = SimpleNamespace(name="Bob", display_name=None, nickname=None, en_name=None)
        mock_response = SimpleNamespace(
            success=lambda: True,
            data=SimpleNamespace(user=user_obj),
        )

        async def _direct(func, *args, **kwargs):
            return func(*args, **kwargs)

        class _ContactAPI:
            def get(self, request):
                return mock_response

        adapter._client = SimpleNamespace(
            contact=SimpleNamespace(v3=SimpleNamespace(user=_ContactAPI()))
        )

        with patch("plugins.platforms.feishu.adapter.asyncio.to_thread", side_effect=_direct):
            result = asyncio.run(adapter._resolve_sender_name_from_api("ou_bob"))

        self.assertEqual(result, "Bob")
        self.assertIn("ou_bob", adapter._sender_name_cache)


@unittest.skipUnless(_HAS_LARK_OAPI, "lark-oapi not installed")
class TestBotNameResolution(unittest.TestCase):
    """Tests for the bot branch of _resolve_sender_name_from_api (basic_batch API + shared cache)."""

    @staticmethod
    def _batch_payload(bots: Dict[str, str]):
        import json as _json
        body = {
            oid: {"bot_id": oid, "name": name, "i18n_names": {"en_us": name}}
            for oid, name in bots.items()
        }
        return _json.dumps({"code": 0, "msg": "", "data": {"bots": body, "failed_bots": {}}}).encode()

    def _build_adapter_with_bots(self, bots: Dict[str, str]):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        calls = []

        def _fake_request(request):
            calls.append(request)
            return SimpleNamespace(raw=SimpleNamespace(content=self._batch_payload(bots)))

        adapter._client = SimpleNamespace(request=_fake_request)
        return adapter, calls

    @patch.dict(os.environ, {}, clear=True)
    def test_returns_cached_bot_name_without_api_call(self):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        adapter._sender_name_cache["ou_peer"] = ("Peer Bot", time.time() + 600)
        adapter._client = SimpleNamespace(
            request=lambda _r: (_ for _ in ()).throw(RuntimeError("should not fetch"))
        )
        result = asyncio.run(adapter._resolve_sender_name_from_api("ou_peer", is_bot=True))
        self.assertEqual(result, "Peer Bot")

    @patch.dict(os.environ, {}, clear=True)
    def test_fetches_and_caches_bot_name(self):
        adapter, calls = self._build_adapter_with_bots({"ou_peer": "Peer Bot"})

        async def _direct(func, *args, **kwargs):
            return func(*args, **kwargs)

        with patch("plugins.platforms.feishu.adapter.asyncio.to_thread", side_effect=_direct):
            result = asyncio.run(adapter._resolve_sender_name_from_api("ou_peer", is_bot=True))

        self.assertEqual(result, "Peer Bot")
        self.assertEqual(adapter._sender_name_cache["ou_peer"][0], "Peer Bot")
        self.assertEqual(len(calls), 1)
        self.assertIn("/open-apis/bot/v3/bots/basic_batch", calls[0].uri)
        # Feishu expects repeated ?bot_ids= params, not comma-joined.
        self.assertEqual(calls[0].queries, [("bot_ids", "ou_peer")])


@unittest.skipUnless(_HAS_LARK_OAPI, "lark-oapi not installed")
class TestProcessingReactions(unittest.TestCase):
    """Typing on start → removed on SUCCESS, swapped for CrossMark on FAILURE,
    removed (no replacement) on CANCELLED."""

    @staticmethod
    def _run(coro):
        return asyncio.run(coro)

    def _build_adapter(
        self,
        create_success: bool = True,
        delete_success: bool = True,
        delete_raises: bool = False,
        next_reaction_id: str = "r1",
    ):
        from gateway.config import PlatformConfig
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        tracker = SimpleNamespace(
            create_calls=[],
            delete_calls=[],
            next_reaction_id=next_reaction_id,
            create_success=create_success,
            delete_success=delete_success,
            delete_raises=delete_raises,
        )

        def _create(request):
            tracker.create_calls.append(
                request.request_body.reaction_type["emoji_type"]
            )
            if tracker.create_success:
                return SimpleNamespace(
                    success=lambda: True,
                    data=SimpleNamespace(reaction_id=tracker.next_reaction_id),
                )
            return SimpleNamespace(
                success=lambda: False, code=99, msg="rejected", data=None,
            )

        def _delete(request):
            tracker.delete_calls.append(request.reaction_id)
            if tracker.delete_raises:
                raise OSError("TLS transport failed")
            return SimpleNamespace(
                success=lambda: tracker.delete_success,
                code=0 if tracker.delete_success else 99,
                msg="success" if tracker.delete_success else "rejected",
            )

        adapter._client = SimpleNamespace(
            im=SimpleNamespace(
                v1=SimpleNamespace(
                    message_reaction=SimpleNamespace(create=_create, delete=_delete),
                ),
            ),
        )
        return adapter, tracker

    @staticmethod
    def _event(message_id: str = "om_msg"):
        return SimpleNamespace(message_id=message_id)

    def _patch_to_thread(self):
        async def _direct(func, *args, **kwargs):
            return func(*args, **kwargs)

        return patch("plugins.platforms.feishu.adapter.asyncio.to_thread", side_effect=_direct)

    # ------------------------------------------------------------------ start
    @patch.dict(os.environ, {}, clear=True)
    def test_start_adds_typing_and_caches_reaction_id(self):
        adapter, tracker = self._build_adapter(next_reaction_id="r_typing")
        with self._patch_to_thread():
            self._run(adapter.on_processing_start(self._event()))
        self.assertEqual(tracker.create_calls, ["Typing"])
        self.assertEqual(adapter._pending_processing_reactions["om_msg"], "r_typing")


    # --------------------------------------------------------------- complete
    @patch.dict(os.environ, {}, clear=True)
    def test_success_removes_typing_and_adds_nothing(self):
        adapter, tracker = self._build_adapter(next_reaction_id="r_typing")
        with self._patch_to_thread():
            self._run(adapter.on_processing_start(self._event()))
            self._run(
                adapter.on_processing_complete(self._event(), ProcessingOutcome.SUCCESS)
            )
        self.assertEqual(tracker.create_calls, ["Typing"])
        self.assertEqual(tracker.delete_calls, ["r_typing"])
        self.assertNotIn("om_msg", adapter._pending_processing_reactions)

    @patch.dict(os.environ, {}, clear=True)
    def test_failure_removes_typing_then_adds_cross_mark(self):
        adapter, tracker = self._build_adapter(next_reaction_id="r_typing")
        with self._patch_to_thread():
            self._run(adapter.on_processing_start(self._event()))
            self._run(
                adapter.on_processing_complete(self._event(), ProcessingOutcome.FAILURE)
            )
        self.assertEqual(tracker.create_calls, ["Typing", "CrossMark"])
        self.assertEqual(tracker.delete_calls, ["r_typing"])


    # ------------------------- delete failure: don't stack badges -----------
    @patch.dict(os.environ, {}, clear=True)
    def test_delete_failure_on_failure_outcome_skips_cross_mark(self):
        # Removing Typing is best-effort — but if it fails, we must NOT
        # additionally add CrossMark, or the UI would show two contradictory
        # badges. The handle stays in the cache for LRU to clean up later.
        adapter, tracker = self._build_adapter(
            next_reaction_id="r_typing", delete_success=False,
        )
        with self._patch_to_thread():
            self._run(adapter.on_processing_start(self._event()))
            self._run(
                adapter.on_processing_complete(self._event(), ProcessingOutcome.FAILURE)
            )
        self.assertEqual(tracker.create_calls, ["Typing"])  # CrossMark NOT added
        self.assertEqual(tracker.delete_calls, ["r_typing"])  # delete was attempted
        self.assertEqual(
            adapter._pending_processing_reactions["om_msg"], "r_typing",
        )  # handle retained

    @patch.dict(os.environ, {}, clear=True)
    def test_delete_transport_error_is_best_effort_and_keeps_identity(self):
        adapter, tracker = self._build_adapter(
            next_reaction_id="r_typing",
            delete_raises=True,
        )
        event = self._event("om_semantic_confirmation")
        with self._patch_to_thread():
            self._run(adapter.on_processing_start(event))
            self._run(adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS))

        self.assertEqual(tracker.delete_calls, ["r_typing"])
        self.assertEqual(
            adapter._pending_processing_reactions["om_semantic_confirmation"],
            "r_typing",
        )


    # ------------------------------------------------------------- env toggle

    # ------------------------------------------------------------- LRU bounds


class TestFeishuMentionMap(unittest.TestCase):

    def test_build_mentions_map_marks_self_by_open_id(self):
        from plugins.platforms.feishu.adapter import _build_mentions_map, _FeishuBotIdentity

        mention = SimpleNamespace(
            key="@_user_1",
            id=SimpleNamespace(open_id="ou_bot", user_id=""),
            name="Hermes",
        )
        ref = _build_mentions_map([mention], _FeishuBotIdentity(open_id="ou_bot"))["@_user_1"]
        self.assertTrue(ref.is_self)
        self.assertEqual(ref.open_id, "ou_bot")
        self.assertEqual(ref.name, "Hermes")


    def test_build_mentions_map_name_match_does_not_override_mismatching_open_id(self):
        """Regression: a human user whose display name matches the bot must
        NOT be flagged as self when their open_id differs. Before the fix,
        name-match fired even when open_id was present and different, causing
        their messages to be silently stripped/dropped."""
        from plugins.platforms.feishu.adapter import _build_mentions_map, _FeishuBotIdentity

        human_with_same_name = SimpleNamespace(
            key="@_user_1",
            id=SimpleNamespace(open_id="ou_human", user_id=""),
            name="Hermes Bot",
        )
        result = _build_mentions_map(
            [human_with_same_name],
            _FeishuBotIdentity(open_id="ou_bot", name="Hermes Bot"),
        )
        self.assertFalse(result["@_user_1"].is_self)

    def test_build_mentions_map_falls_back_to_name_when_bot_open_id_not_hydrated(self):
        """Regression: right after gateway startup, _hydrate_bot_identity may
        not have populated _bot_open_id yet. During that window, a mention
        carrying a real open_id should still match via name — otherwise
        @bot messages silently fail admission."""
        from plugins.platforms.feishu.adapter import _build_mentions_map, _FeishuBotIdentity

        bot_mention = SimpleNamespace(
            key="@_user_1",
            id=SimpleNamespace(open_id="ou_bot_actual", user_id=""),
            name="Hermes Bot",
        )
        # Bot identity has name but no open_id yet (hydration pending).
        result = _build_mentions_map(
            [bot_mention],
            _FeishuBotIdentity(open_id="", name="Hermes Bot"),
        )
        self.assertTrue(result["@_user_1"].is_self)


class TestFeishuMentionHint(unittest.TestCase):
    def test_hint_single_user(self):
        from plugins.platforms.feishu.adapter import FeishuMentionRef, _build_mention_hint

        refs = [FeishuMentionRef(name="Alice", open_id="ou_alice")]
        self.assertEqual(
            _build_mention_hint(refs),
            "[Mentioned: Alice (open_id=ou_alice)]",
        )

    def test_hint_multiple_users(self):
        from plugins.platforms.feishu.adapter import FeishuMentionRef, _build_mention_hint

        refs = [
            FeishuMentionRef(name="Alice", open_id="ou_alice"),
            FeishuMentionRef(name="Bob", open_id="ou_bob"),
        ]
        self.assertEqual(
            _build_mention_hint(refs),
            "[Mentioned: Alice (open_id=ou_alice), Bob (open_id=ou_bob)]",
        )


    def test_hint_filters_self_mentions(self):
        from plugins.platforms.feishu.adapter import FeishuMentionRef, _build_mention_hint

        refs = [
            FeishuMentionRef(name="Hermes", open_id="ou_bot", is_self=True),
            FeishuMentionRef(name="Alice", open_id="ou_alice"),
        ]
        self.assertEqual(
            _build_mention_hint(refs),
            "[Mentioned: Alice (open_id=ou_alice)]",
        )


    def test_hint_dedupes_repeated_user(self):
        from plugins.platforms.feishu.adapter import FeishuMentionRef, _build_mention_hint

        refs = [
            FeishuMentionRef(name="Alice", open_id="ou_alice"),
            FeishuMentionRef(name="Alice", open_id="ou_alice"),
            FeishuMentionRef(name="Bob", open_id="ou_bob"),
        ]
        self.assertEqual(
            _build_mention_hint(refs),
            "[Mentioned: Alice (open_id=ou_alice), Bob (open_id=ou_bob)]",
        )


class TestFeishuStripLeadingSelf(unittest.TestCase):
    def _make_refs(self, *, self_name="Hermes", other_name=None):
        from plugins.platforms.feishu.adapter import FeishuMentionRef

        refs = [FeishuMentionRef(name=self_name, open_id="ou_bot", is_self=True)]
        if other_name:
            refs.append(FeishuMentionRef(name=other_name, open_id="ou_alice"))
        return refs


    def test_stops_at_first_non_self_token(self):
        from plugins.platforms.feishu.adapter import _strip_edge_self_mentions

        result = _strip_edge_self_mentions(
            "@Hermes @Alice make a group", self._make_refs(other_name="Alice")
        )
        self.assertEqual(result, "@Alice make a group")


    def test_strips_trailing_self_with_terminal_punct(self):
        from plugins.platforms.feishu.adapter import _strip_edge_self_mentions

        # Terminal punct after the mention — strip the mention, keep the punct.
        result = _strip_edge_self_mentions("look up docs @Hermes.", self._make_refs())
        self.assertEqual(result, "look up docs.")

    def test_preserves_trailing_self_before_non_terminal_char(self):
        from plugins.platforms.feishu.adapter import _strip_edge_self_mentions

        # Non-terminal char (here a Chinese particle) follows — preserve.
        result = _strip_edge_self_mentions(
            "please don't @Hermes anymore", self._make_refs()
        )
        self.assertEqual(result, "please don't @Hermes anymore")


    def test_returns_input_when_no_self_refs(self):
        from plugins.platforms.feishu.adapter import _strip_edge_self_mentions, FeishuMentionRef

        refs = [FeishuMentionRef(name="Alice", open_id="ou_alice")]
        self.assertEqual(_strip_edge_self_mentions("@Alice hi", refs), "@Alice hi")


class TestFeishuNormalizeText(unittest.TestCase):

    def test_renders_self_mention_with_name(self):
        from plugins.platforms.feishu.adapter import _normalize_feishu_text, FeishuMentionRef

        refs = {"@_user_1": FeishuMentionRef(name="Hermes", open_id="ou_bot", is_self=True)}
        self.assertEqual(
            _normalize_feishu_text("stop pinging @_user_1 please", refs),
            "stop pinging @Hermes please",
        )

    def test_at_all_rendered_as_english_literal(self):
        from plugins.platforms.feishu.adapter import _normalize_feishu_text

        self.assertEqual(_normalize_feishu_text("@_all notice", None), "@all notice")


class TestFeishuPostMentionParsing(unittest.TestCase):
    def test_post_at_tag_renders_via_mentions_map(self):
        """Post <at>.user_id is a placeholder ('@_user_N'); the real display
        name comes from the mentions_map lookup. Confirmed via live
        im.v1.message.get payload."""
        from plugins.platforms.feishu.adapter import parse_feishu_post_payload, FeishuMentionRef

        payload = {
            "en_us": {
                "content": [[
                    {"tag": "at", "user_id": "@_user_1", "user_name": "ignored"},
                    {"tag": "text", "text": " hello"},
                ]]
            }
        }
        mentions_map = {
            "@_user_1": FeishuMentionRef(name="Alice", open_id="ou_alice"),
        }
        result = parse_feishu_post_payload(payload, mentions_map=mentions_map)
        self.assertEqual(result.text_content, "@Alice hello")


class TestFeishuNormalizeWithMentions(unittest.TestCase):
    def test_text_message_renders_mention_by_name(self):
        from plugins.platforms.feishu.adapter import normalize_feishu_message, _FeishuBotIdentity

        mention = SimpleNamespace(
            key="@_user_1",
            id=SimpleNamespace(open_id="ou_alice", user_id=""),
            name="Alice",
        )
        normalized = normalize_feishu_message(
            message_type="text",
            raw_content=json.dumps({"text": "@_user_1 hello"}),
            mentions=[mention],
            bot=_FeishuBotIdentity(open_id="ou_bot"),
        )
        self.assertEqual(normalized.text_content, "@Alice hello")
        self.assertEqual(len(normalized.mentions), 1)
        self.assertEqual(normalized.mentions[0].open_id, "ou_alice")
        self.assertFalse(normalized.mentions[0].is_self)


    def test_post_message_marks_self_via_mentions_map_lookup(self):
        """Real Feishu post: <at user_id="@_user_N"> + top-level mentions array
        resolves to open_id via placeholder lookup, not direct tag fields."""
        from plugins.platforms.feishu.adapter import normalize_feishu_message, _FeishuBotIdentity

        raw = json.dumps({
            "en_us": {
                "content": [
                    [
                        {"tag": "at", "user_id": "@_user_1", "user_name": "Hermes"},
                        {"tag": "text", "text": " check this"},
                    ]
                ]
            }
        })
        bot_mention = SimpleNamespace(
            key="@_user_1",
            id=SimpleNamespace(open_id="ou_bot", user_id=""),
            name="Hermes",
        )
        normalized = normalize_feishu_message(
            message_type="post",
            raw_content=raw,
            mentions=[bot_mention],
            bot=_FeishuBotIdentity(open_id="ou_bot"),
        )
        self.assertEqual(len(normalized.mentions), 1)
        self.assertTrue(normalized.mentions[0].is_self)
        self.assertEqual(normalized.mentions[0].open_id, "ou_bot")


class TestFeishuPostMentionsBot(unittest.TestCase):
    def _build_adapter(self, bot_open_id="ou_bot", bot_user_id="", bot_name=""):
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter.__new__(FeishuAdapter)
        adapter._bot_open_id = bot_open_id
        adapter._bot_user_id = bot_user_id
        adapter._bot_name = bot_name
        return adapter

    def test_post_mentions_bot_uses_is_self_flag(self):
        from plugins.platforms.feishu.adapter import FeishuMentionRef

        adapter = self._build_adapter()
        self.assertTrue(
            adapter._post_mentions_bot(
                [FeishuMentionRef(name="Hermes", open_id="ou_bot", is_self=True)]
            )
        )
        self.assertFalse(
            adapter._post_mentions_bot(
                [FeishuMentionRef(name="Alice", open_id="ou_alice")]
            )
        )


class TestFeishuExtractMessageContent(unittest.TestCase):
    def _build_adapter(self):
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter.__new__(FeishuAdapter)
        adapter._bot_open_id = "ou_bot"
        adapter._bot_user_id = ""
        adapter._bot_name = "Hermes"
        adapter._download_feishu_message_resources = AsyncMock(return_value=([], []))
        return adapter

    def test_returns_five_tuple_with_mentions(self):
        adapter = self._build_adapter()
        message = SimpleNamespace(
            content=json.dumps({"text": "@_user_1 hello"}),
            message_type="text",
            message_id="m1",
            mentions=[
                SimpleNamespace(
                    key="@_user_1",
                    id=SimpleNamespace(open_id="ou_alice", user_id=""),
                    name="Alice",
                )
            ],
        )

        text, inbound_type, media_urls, media_types, mentions = asyncio.run(
            adapter._extract_message_content(message)
        )
        self.assertEqual(text, "@Alice hello")
        self.assertEqual(len(mentions), 1)
        self.assertEqual(mentions[0].open_id, "ou_alice")


class TestFeishuProcessInboundMessage(unittest.TestCase):
    def _build_adapter(self):
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter.__new__(FeishuAdapter)
        adapter._bot_open_id = "ou_bot"
        adapter._bot_user_id = ""
        adapter._bot_name = "Hermes"
        adapter._download_feishu_message_resources = AsyncMock(return_value=([], []))
        adapter._fetch_message_text = AsyncMock(return_value=None)
        adapter.get_chat_info = AsyncMock(return_value={"name": "Test Chat"})
        adapter._resolve_sender_profile = AsyncMock(
            return_value={"user_id": "u1", "user_name": "Alice", "user_id_alt": None}
        )
        adapter._resolve_source_chat_type = Mock(return_value="group")
        adapter.build_source = Mock(return_value=SimpleNamespace(thread_id=None))
        adapter._dispatch_inbound_event = AsyncMock()
        return adapter


    def test_non_command_message_with_mentions_injects_hint(self):
        from gateway.platforms.base import MessageType

        adapter = self._build_adapter()
        alice = SimpleNamespace(
            key="@_user_1",
            id=SimpleNamespace(open_id="ou_alice", user_id=""),
            name="Alice",
        )
        bob = SimpleNamespace(
            key="@_user_2",
            id=SimpleNamespace(open_id="ou_bob", user_id=""),
            name="Bob",
        )
        message = SimpleNamespace(
            content=json.dumps({"text": "@_user_1 @_user_2 make a group"}),
            message_type="text",
            message_id="m2",
            mentions=[alice, bob],
            chat_id="oc_chat",
            parent_id=None,
            upper_message_id=None,
            thread_id=None,
        )
        asyncio.run(
            adapter._process_inbound_message(
                data=message,
                message=message,
                sender_id=None,
                chat_type="group",
                message_id="m2",
            )
        )
        event = adapter._dispatch_inbound_event.call_args.args[0]
        self.assertEqual(event.message_type, MessageType.TEXT)
        self.assertIn("[Mentioned: Alice (open_id=ou_alice), Bob (open_id=ou_bob)]", event.text)
        self.assertIn("@Alice @Bob make a group", event.text)

    def test_command_message_never_injects_hint(self):
        adapter = self._build_adapter()
        bot_mention = SimpleNamespace(
            key="@_user_1",
            id=SimpleNamespace(open_id="ou_bot", user_id=""),
            name="Hermes",
        )
        alice = SimpleNamespace(
            key="@_user_2",
            id=SimpleNamespace(open_id="ou_alice", user_id=""),
            name="Alice",
        )
        message = SimpleNamespace(
            content=json.dumps({"text": "@_user_1 /model @_user_2"}),
            message_type="text",
            message_id="m3",
            mentions=[bot_mention, alice],
            chat_id="oc_chat",
            parent_id=None,
            upper_message_id=None,
            thread_id=None,
        )
        asyncio.run(
            adapter._process_inbound_message(
                data=message,
                message=message,
                sender_id=None,
                chat_type="group",
                message_id="m3",
            )
        )
        event = adapter._dispatch_inbound_event.call_args.args[0]
        self.assertNotIn("[Mentioned:", event.text)
        self.assertTrue(event.text.startswith("/model"))


class TestFeishuFetchMessageText(unittest.TestCase):
    def _build_adapter(self):
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter.__new__(FeishuAdapter)
        adapter._bot_open_id = "ou_bot"
        adapter._bot_user_id = ""
        adapter._bot_name = "Hermes"
        adapter._message_text_cache = OrderedDict()
        adapter._client = Mock()
        adapter._build_get_message_request = Mock(return_value=object())
        return adapter


    def test_fetch_message_text_marks_is_self_via_string_id_shape(self):
        """History-path Mention objects carry id as str + id_type; is_self must still work."""
        adapter = self._build_adapter()
        # bot_name is empty — is_self must be detected via open_id alone
        adapter._bot_name = ""

        bot_mention = SimpleNamespace(
            key="@_user_1",
            id="ou_bot",
            id_type="open_id",
            name="Hermes",
        )
        parent = SimpleNamespace(
            body=SimpleNamespace(content=json.dumps({"text": "@_user_1 hi"})),
            msg_type="text",
            mentions=[bot_mention],
        )
        response = Mock()
        response.success = Mock(return_value=True)
        response.data = SimpleNamespace(items=[parent])
        adapter._client.im.v1.message.get = Mock(return_value=response)

        # The rendered text should still have the bot name substituted.
        result = asyncio.run(adapter._fetch_message_text("m_parent"))
        self.assertEqual(result, "@Hermes hi")


class TestFeishuMentionEndToEnd(unittest.TestCase):
    """High-level scenarios from the design spec — verify the full pipeline."""

    def _build_adapter(self):
        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = FeishuAdapter.__new__(FeishuAdapter)
        adapter._bot_open_id = "ou_bot"
        adapter._bot_user_id = ""
        adapter._bot_name = "Hermes"
        adapter._download_feishu_message_resources = AsyncMock(return_value=([], []))
        adapter._fetch_message_text = AsyncMock(return_value=None)
        adapter.get_chat_info = AsyncMock(return_value={"name": "Test Chat"})
        adapter._resolve_sender_profile = AsyncMock(
            return_value={"user_id": "u1", "user_name": "Alice", "user_id_alt": None}
        )
        adapter._resolve_source_chat_type = Mock(return_value="group")
        adapter.build_source = Mock(return_value=SimpleNamespace(thread_id=None))
        adapter._dispatch_inbound_event = AsyncMock()
        return adapter

    def _run(self, adapter, text, mentions):
        raw_mentions = [
            SimpleNamespace(
                key=m["key"],
                id=SimpleNamespace(open_id=m.get("open_id", ""), user_id=m.get("user_id", "")),
                name=m.get("name", ""),
            )
            for m in mentions
        ]
        message = SimpleNamespace(
            content=json.dumps({"text": text}),
            message_type="text",
            message_id="m",
            mentions=raw_mentions,
            chat_id="oc_chat",
            parent_id=None,
            upper_message_id=None,
            thread_id=None,
        )
        asyncio.run(
            adapter._process_inbound_message(
                data=message, message=message, sender_id=None, chat_type="group", message_id="m",
            )
        )
        return adapter._dispatch_inbound_event.call_args.args[0]


    def test_scenario_no_mentions_zero_regression(self):
        adapter = self._build_adapter()
        event = self._run(adapter, "plain message", [])
        self.assertEqual(event.text, "plain message")
        self.assertNotIn("[Mentioned:", event.text)


    def test_scenario_post_bot_plus_alice_filters_self_from_hint(self):
        """Post-type message @-ing both the bot and Alice: leading bot is
        stripped from the body, self is filtered from the [Mentioned: ...]
        hint, and Alice's real open_id is surfaced for the agent."""
        adapter = self._build_adapter()
        bot_mention = SimpleNamespace(
            key="@_user_1",
            id=SimpleNamespace(open_id="ou_bot", user_id=""),
            name="Hermes",
        )
        alice_mention = SimpleNamespace(
            key="@_user_2",
            id=SimpleNamespace(open_id="ou_alice", user_id=""),
            name="Alice",
        )
        post_content = json.dumps({
            "zh_cn": {
                "content": [[
                    {"tag": "at", "user_id": "@_user_1", "user_name": "Hermes"},
                    {"tag": "at", "user_id": "@_user_2", "user_name": "Alice"},
                    {"tag": "text", "text": " review the spec with Alice"},
                ]]
            }
        })
        message = SimpleNamespace(
            content=post_content,
            message_type="post",
            message_id="m_post_both",
            mentions=[bot_mention, alice_mention],
            chat_id="oc_chat",
            parent_id=None,
            upper_message_id=None,
            thread_id=None,
        )
        asyncio.run(
            adapter._process_inbound_message(
                data=message, message=message, sender_id=None,
                chat_type="group", message_id="m_post_both",
            )
        )
        event = adapter._dispatch_inbound_event.call_args.args[0]
        # Hint surfaces Alice; bot excluded because is_self=True.
        self.assertIn("[Mentioned: Alice (open_id=ou_alice)]", event.text)
        self.assertNotIn("Hermes (open_id=", event.text)
        # Body: leading @Hermes stripped, Alice preserved, trailing text intact.
        self.assertIn("@Alice review the spec with Alice", event.text)
        self.assertNotIn("@Hermes @Alice", event.text)


class TestChatLockEviction(unittest.TestCase):
    """_get_chat_lock is LRU-bounded so _chat_locks cannot grow unbounded."""

    def _make_adapter(self, max_size=5):
        import collections as _collections

        from plugins.platforms.feishu.adapter import FeishuAdapter

        adapter = object.__new__(FeishuAdapter)
        adapter._chat_locks = _collections.OrderedDict()
        adapter.CHAT_LOCK_MAX_SIZE = max_size
        return adapter

    def test_chat_locks_is_ordered_dict(self):
        import collections as _collections

        adapter = self._make_adapter()
        self.assertIsInstance(adapter._chat_locks, _collections.OrderedDict)
