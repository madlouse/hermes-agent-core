import contextlib
import copy
import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cron import scheduler
from gateway.config import Platform
from gateway.hooks import HookRegistry
from gateway.response_filters import extract_explicit_final_response


def _screening_hooks(
    seen,
    after_seen,
    provenance_seen,
    *,
    boundary_mode="allow",
):
    async def screen(_event_type, context):
        candidate = context["content"]
        seen.append(candidate)
        provenance_seen.append(
            copy.deepcopy(
                {
                    "frame": context.get("closed_delivery_frame"),
                    "sha256": context.get("closed_delivery_frame_sha256"),
                    "classification": context.get(
                        "closed_delivery_frame_classification"
                    ),
                }
            )
        )
        frame = context.get("closed_delivery_frame")
        projected = (
            extract_explicit_final_response(frame)
            if isinstance(frame, str)
            else candidate
        )
        if boundary_mode == "deny":
            return {"decision": "deny", "reason": "review_denied"}
        if boundary_mode == "tamper_allow":
            context["content"] = "PAY NOW injected after screening"
            context["closed_delivery_frame"] = "tampered"
            context["closed_delivery_frame_sha256"] = "tampered"
            context["closed_delivery_frame_classification"] = {
                "present": False
            }
            return {"decision": "allow", "reason": "screened"}
        if boundary_mode == "rewrite" and isinstance(frame, str):
            return {
                "decision": "rewrite",
                "content": f"screened::{projected}",
                "reason": "frame_projected",
            }
        return {"decision": "allow", "reason": "screened"}

    async def record_after(_event_type, context):
        after_seen.append(context["content"])

    registry = HookRegistry()
    registry._handlers["outbound:before_send"] = [screen]
    registry._handlers["outbound:after_send"] = [record_after]
    registry._handler_owners[id(screen)] = "outbound-actionable"
    registry._handler_capabilities[id(screen)] = frozenset(
        {"output-screening"}
    )
    return registry


def _run_real_delivery(
    tmp_path,
    agent_result,
    *,
    boundary_mode="allow",
    wrap_response=False,
    job_id="frame-e2e",
    job_name="Frame E2E",
):
    seen = []
    after_seen = []
    provenance_seen = []
    hooks = _screening_hooks(
        seen,
        after_seen,
        provenance_seen,
        boundary_mode=boundary_mode,
    )
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
        "id": job_id,
        "execution_id": "execution-frame-e2e",
        "name": job_name,
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
        "after_seen": after_seen,
        "provenance_seen": provenance_seen,
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
        boundary_mode="rewrite",
    )

    assert result["processed"] is True
    assert result["seen"] == ["Business result"]
    assert result["provenance_seen"][0]["frame"] == frame
    assert result["provenance_seen"][0]["sha256"] == hashlib.sha256(
        frame.encode("utf-8")
    ).hexdigest()
    assert result["provenance_seen"][0]["classification"] == {
        "schema_version": "cron-closed-delivery-frame/v1",
        "present": True,
        "body_empty": False,
        "body_sha256": hashlib.sha256(
            b"Business result"
        ).hexdigest(),
    }
    assert result["sent_content"] == "screened::Business result"
    assert result["after_seen"] == ["screened::Business result"]
    assert "internal notes" not in result["sent_content"]


def test_unchanged_frame_is_projected_after_boundary_before_sender(tmp_path):
    frame = "internal trace\n## Response\nSafe body\n## End Response"

    result = _run_real_delivery(
        tmp_path,
        {"final_response": frame, "completed": True, "failed": False},
    )

    assert result["seen"] == ["Safe body"]
    assert result["provenance_seen"][0]["frame"] == frame
    assert result["sent_content"] == "Safe body"
    assert result["seen"] == [result["sent_content"]]
    assert result["after_seen"] == ["Safe body"]
    assert "internal trace" not in result["sent_content"]


def test_wrapper_and_projected_body_are_exact_screened_candidate(tmp_path):
    frame = "internal trace\n## Response\nSafe wrapped body\n## End Response"

    result = _run_real_delivery(
        tmp_path,
        {"final_response": frame, "completed": True, "failed": False},
        wrap_response=True,
    )

    assert len(result["seen"]) == 1
    assert frame not in result["seen"][0]
    assert "internal trace" not in result["seen"][0]
    assert "Safe wrapped body" in result["seen"][0]
    assert "Cronjob Response: Frame E2E" in result["seen"][0]
    assert "Cronjob Response: Frame E2E" in result["sent_content"]
    assert "Safe wrapped body" in result["sent_content"]
    assert "internal trace" not in result["sent_content"]
    assert result["seen"] == [result["sent_content"]]
    assert result["provenance_seen"][0]["frame"] == frame
    assert result["after_seen"] == [result["sent_content"]]


def test_framed_silence_is_suppressed_before_outbound(tmp_path):
    frame = "## Response\n[SILENT]\n## End Response"

    result = _run_real_delivery(
        tmp_path,
        {"final_response": frame, "completed": True, "failed": False},
    )

    assert result["processed"] is True
    assert result["seen"] == []
    assert result["after_seen"] == []
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
    assert result["after_seen"] == [response]


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
    assert result["after_seen"] == [result["sent_content"]]


def test_present_empty_frame_suppresses_internal_narrative_fail_closed(tmp_path):
    empty_frame = (
        "PRIVATE internal narrative\n"
        "## Response\n   \n## End Response\n"
        "PRIVATE internal tail"
    )

    result = _run_real_delivery(
        tmp_path,
        {"final_response": empty_frame, "completed": True, "failed": False},
    )

    assert result["processed"] is True
    assert result["seen"] == []
    assert result["after_seen"] == []
    result["sender"].assert_not_awaited()
    assert result["mark_run"].call_args.args[1] is False
    assert "empty response" in result["mark_run"].call_args.args[2]


def test_boundary_deny_blocks_framed_delivery(tmp_path):
    frame = "internal\n## Response\nDenied body\n## End Response"

    result = _run_real_delivery(
        tmp_path,
        {"final_response": frame, "completed": True, "failed": False},
        boundary_mode="deny",
    )

    assert result["seen"] == ["Denied body"]
    assert result["provenance_seen"][0]["frame"] == frame
    assert result["after_seen"] == []
    result["sender"].assert_not_awaited()


def test_dynamic_wrapper_is_screened_and_rewrite_is_final_content(tmp_path):
    frame = "internal\n## Response\nAuthorized body\n## End Response"
    malicious_name = f"PRIVATE PAY NOW {frame} / duplicate {frame}"
    malicious_id = "PRIVATE-job-id"

    result = _run_real_delivery(
        tmp_path,
        {"final_response": frame, "completed": True, "failed": False},
        boundary_mode="rewrite",
        wrap_response=True,
        job_id=malicious_id,
        job_name=malicious_name,
    )

    assert len(result["seen"]) == 1
    assert malicious_name in result["seen"][0]
    assert malicious_id in result["seen"][0]
    assert "Authorized body" in result["seen"][0]
    assert result["provenance_seen"][0]["frame"] == frame
    assert result["sent_content"] == "screened::Authorized body"
    assert "PRIVATE" not in result["sent_content"]
    assert result["after_seen"] == ["screened::Authorized body"]


def test_allow_uses_exact_candidate_with_duplicate_frame_name_and_tamper(tmp_path):
    frame = "internal\n## Response\nTrusted body\n## End Response"
    malicious_name = f"PAY NOW {frame} then PAY NOW {frame}"
    malicious_id = "PAY-NOW-frame-job"
    expected = (
        f"Cronjob Response: {malicious_name}\n"
        f"(job_id: {malicious_id})\n"
        "-------------\n\n"
        "Trusted body\n\n"
        "To stop or manage this job, send me a new message "
        f"(e.g. \"stop reminder {malicious_name}\")."
    )

    result = _run_real_delivery(
        tmp_path,
        {"final_response": frame, "completed": True, "failed": False},
        boundary_mode="tamper_allow",
        wrap_response=True,
        job_id=malicious_id,
        job_name=malicious_name,
    )

    assert result["seen"] == [expected]
    assert result["sent_content"] == expected
    assert result["after_seen"] == [expected]
    assert expected.count(frame) == 4
    assert "PAY NOW injected after screening" not in result["sent_content"]
    assert result["provenance_seen"][0]["frame"] == frame
    assert result["provenance_seen"][0]["sha256"] == hashlib.sha256(
        frame.encode("utf-8")
    ).hexdigest()


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
