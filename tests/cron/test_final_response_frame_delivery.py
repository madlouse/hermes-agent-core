import contextlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cron import scheduler
from gateway.config import Platform
from gateway.hooks import HookRegistry
from gateway.response_filters import extract_explicit_final_response


def _screening_hooks(seen, *, rewrite_frames=False):
    async def screen(_event_type, context):
        candidate = context["content"]
        seen.append(candidate)
        projected = extract_explicit_final_response(candidate)
        if rewrite_frames and projected != candidate:
            return {
                "decision": "rewrite",
                "content": f"screened::{projected}",
                "reason": "frame_projected",
            }
        return {"decision": "allow", "reason": "screened"}

    registry = HookRegistry()
    registry._handlers["outbound:before_send"] = [screen]
    registry._handler_owners[id(screen)] = "outbound-actionable"
    registry._handler_capabilities[id(screen)] = frozenset(
        {"output-screening"}
    )
    return registry


def _run_real_delivery(
    tmp_path,
    agent_result,
    *,
    rewrite_frames=False,
    wrap_response=False,
):
    seen = []
    hooks = _screening_hooks(seen, rewrite_frames=rewrite_frames)
    sender = AsyncMock(return_value={"success": True, "message_id": "sent-1"})
    fake_db = MagicMock()
    fake_db.get_compression_tip.side_effect = lambda session_id: session_id
    agent = MagicMock()
    agent.run_conversation.return_value = dict(agent_result)
    platform_config = SimpleNamespace(enabled=True, extra={})
    gateway_config = SimpleNamespace(
        platforms={Platform.TELEGRAM: platform_config},
        get_home_channel=lambda _platform: None,
    )
    job = {
        "id": "frame-e2e",
        "execution_id": "execution-frame-e2e",
        "name": "Frame E2E",
        "prompt": "produce the report",
        "deliver": "origin",
        "origin": {"platform": "telegram", "chat_id": "123"},
    }
    mark_run = MagicMock()
    finish_execution = MagicMock()
    patches = [
        patch("cron.scheduler._hermes_home", tmp_path),
        patch("hermes_cli.env_loader.load_hermes_dotenv"),
        patch("hermes_cli.env_loader.reset_secret_source_cache"),
        patch("hermes_state.SessionDB", return_value=fake_db),
        patch(
            "hermes_cli.runtime_provider.resolve_runtime_provider",
            return_value={
                "api_key": "test-key",
                "base_url": "https://example.invalid/v1",
                "provider": "openrouter",
                "api_mode": "chat_completions",
            },
        ),
        patch("run_agent.AIAgent", return_value=agent),
        patch("cron.jobs._apply_cron_runtime_governance"),
        patch(
            "cron.scheduler.load_config",
            return_value={"cron": {"wrap_response": wrap_response}},
        ),
        patch("gateway.config.load_gateway_config", return_value=gateway_config),
        patch("cron.scheduler._active_outbound_hooks", return_value=hooks),
        patch("tools.send_message_tool._send_to_platform", new=sender),
        patch("cron.scheduler._set_running_job_state", return_value=False),
        patch("cron.scheduler.begin_job_run_outcome", return_value=None),
        patch(
            "cron.scheduler._claim_dispatch_with_running_state",
            return_value=(True, False),
        ),
        patch("cron.scheduler.mark_execution_running"),
        patch("cron.scheduler.save_job_output", return_value="/tmp/frame-e2e.md"),
        patch("cron.scheduler._is_interrupted", return_value=False),
        patch("cron.scheduler._consume_interrupted_flag", return_value=False),
        patch("cron.scheduler.mark_job_run", mark_run),
        patch("cron.scheduler.finish_execution", finish_execution),
    ]
    with contextlib.ExitStack() as stack:
        for candidate in patches:
            stack.enter_context(candidate)
        processed = scheduler.run_one_job(job)

    sent_content = (
        sender.await_args.args[3]
        if sender.await_count
        else None
    )
    return {
        "processed": processed,
        "seen": seen,
        "sent_content": sent_content,
        "sender": sender,
        "mark_run": mark_run,
        "finish_execution": finish_execution,
    }


def test_closed_business_frame_reaches_real_boundary_and_can_be_rewritten(tmp_path):
    frame = (
        "internal notes: /Users/alice/.hermes/private\n"
        "## Response\nBusiness result\n## End Response"
    )

    result = _run_real_delivery(
        tmp_path,
        {"final_response": frame, "completed": True, "failed": False},
        rewrite_frames=True,
    )

    assert result["processed"] is True
    assert result["seen"] == [frame]
    assert result["sent_content"] == "screened::Business result"
    assert "internal notes" not in result["sent_content"]


def test_unchanged_frame_is_projected_after_boundary_before_sender(tmp_path):
    frame = "internal trace\n## Response\nSafe body\n## End Response"

    result = _run_real_delivery(
        tmp_path,
        {"final_response": frame, "completed": True, "failed": False},
    )

    assert result["seen"] == [frame]
    assert result["sent_content"] == "Safe body"
    assert "internal trace" not in result["sent_content"]


def test_wrapping_happens_after_original_frame_reaches_boundary(tmp_path):
    frame = "internal trace\n## Response\nSafe wrapped body\n## End Response"

    result = _run_real_delivery(
        tmp_path,
        {"final_response": frame, "completed": True, "failed": False},
        wrap_response=True,
    )

    assert result["seen"] == [frame]
    assert "Cronjob Response: Frame E2E" in result["sent_content"]
    assert "Safe wrapped body" in result["sent_content"]
    assert "internal trace" not in result["sent_content"]


def test_framed_silence_is_suppressed_before_outbound(tmp_path):
    frame = "## Response\n[SILENT]\n## End Response"

    result = _run_real_delivery(
        tmp_path,
        {"final_response": frame, "completed": True, "failed": False},
    )

    assert result["processed"] is True
    assert result["seen"] == []
    result["sender"].assert_not_awaited()


@pytest.mark.parametrize(
    "response",
    [
        "ordinary nonframe response",
        (
            "Documentation example:\n```markdown\n## Response\n[SILENT]\n"
            "## End Response\n```\nThree real changes."
        ),
    ],
    ids=["nonframe", "fenced-example"],
)
def test_nonframe_responses_reach_boundary_and_sender_unchanged(tmp_path, response):
    result = _run_real_delivery(
        tmp_path,
        {"final_response": response, "completed": True, "failed": False},
    )

    assert result["seen"] == [response]
    assert result["sent_content"] == response


def test_failure_uses_compact_notice_without_raw_internal_response(tmp_path):
    raw_internal = "RAW_INTERNAL provider payload with /Users/alice/.hermes"
    result = _run_real_delivery(
        tmp_path,
        {
            "final_response": raw_internal,
            "error": "provider exploded with secret diagnostic",
            "completed": False,
            "failed": True,
        },
    )

    assert result["processed"] is True
    assert len(result["seen"]) == 1
    assert "Frame E2E" in result["seen"][0]
    assert "failed" in result["seen"][0]
    assert raw_internal not in result["seen"][0]
    assert result["sent_content"] == result["seen"][0]


def test_typed_delivery_frame_and_public_legacy_tuple_remain_compatible(tmp_path):
    frame = "internal\n## Response\nBusiness body\n## End Response"
    fake_db = MagicMock()
    fake_db.get_compression_tip.side_effect = lambda session_id: session_id
    agent = MagicMock()
    agent.run_conversation.return_value = {
        "final_response": frame,
        "completed": True,
        "failed": False,
    }
    job = {
        "id": "legacy-frame",
        "name": "Legacy frame",
        "prompt": "produce the report",
    }
    patches = [
        patch("cron.scheduler._hermes_home", tmp_path),
        patch("hermes_cli.env_loader.load_hermes_dotenv"),
        patch("hermes_cli.env_loader.reset_secret_source_cache"),
        patch("hermes_state.SessionDB", return_value=fake_db),
        patch(
            "hermes_cli.runtime_provider.resolve_runtime_provider",
            return_value={
                "api_key": "test-key",
                "base_url": "https://example.invalid/v1",
                "provider": "openrouter",
                "api_mode": "chat_completions",
            },
        ),
        patch("run_agent.AIAgent", return_value=agent),
        patch("cron.jobs._apply_cron_runtime_governance"),
        patch("cron.scheduler.load_config", return_value={}),
    ]
    with contextlib.ExitStack() as stack:
        for candidate in patches:
            stack.enter_context(candidate)
        typed = scheduler._run_job_result(job)
        legacy = scheduler.run_job(job)

    assert typed.delivery_frame == frame
    assert typed.final_response == "Business body"
    assert legacy[0] is True
    assert legacy[2:] == ("Business body", None)
    assert len(legacy) == 4
