import contextvars
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from agent.auxiliary_client import _relay_sync_completion
from agent.request_budget import (
    reset_provider_request_budget,
    set_provider_request_budget,
)
from cron.scheduler import _CronRunControl
from gateway.session_context import (
    get_cron_runtime_context,
    reset_cron_authorization,
    set_cron_authorization,
)
from run_agent import AIAgent


def _bare_agent():
    agent = object.__new__(AIAgent)
    agent.provider = "custom"
    agent.model = "test-model"
    return agent


def test_provider_request_timeout_is_capped_by_total_deadline_cleanup_reserve():
    agent = _bare_agent()
    agent.configure_request_timeout_budget(
        deadline_monotonic=110.0,
        cleanup_grace_seconds=2.0,
    )

    with patch("run_agent.get_provider_request_timeout", return_value=120.0), patch(
        "agent.request_budget.time.monotonic", return_value=100.0
    ):
        assert agent._resolved_api_call_timeout() == 8.0


def test_provider_request_fails_before_network_after_execution_budget_expires():
    agent = _bare_agent()
    agent.configure_request_timeout_budget(
        deadline_monotonic=100.0,
        cleanup_grace_seconds=2.0,
    )

    with patch("run_agent.get_provider_request_timeout", return_value=120.0), patch(
        "agent.request_budget.time.monotonic", return_value=99.0
    ), pytest.raises(TimeoutError, match="deadline exhausted"):
        agent._resolved_api_call_timeout()


def test_each_auxiliary_physical_attempt_refreshes_remaining_cron_budget():
    calls = []
    client = SimpleNamespace(
        chat=SimpleNamespace(
            completions=SimpleNamespace(
                create=lambda **kwargs: calls.append(kwargs) or SimpleNamespace()
            )
        )
    )
    token = set_provider_request_budget(
        deadline_monotonic=110.0,
        cleanup_grace_seconds=2.0,
    )
    try:
        with patch(
            "agent.request_budget.time.monotonic", side_effect=[100.0, 103.0]
        ):
            _relay_sync_completion(client, {"model": "m", "timeout": 120.0})
            _relay_sync_completion(client, {"model": "m", "timeout": 120.0})
    finally:
        reset_provider_request_budget(token)

    assert [call["timeout"] for call in calls] == [8.0, 5.0]


def test_shorter_helper_timeout_still_wins_over_cron_budget():
    calls = []
    client = SimpleNamespace(
        chat=SimpleNamespace(
            completions=SimpleNamespace(
                create=lambda **kwargs: calls.append(kwargs) or SimpleNamespace()
            )
        )
    )
    token = set_provider_request_budget(
        deadline_monotonic=110.0,
        cleanup_grace_seconds=2.0,
    )
    try:
        with patch("agent.request_budget.time.monotonic", return_value=100.0):
            _relay_sync_completion(client, {"model": "m", "timeout": 3.0})
    finally:
        reset_provider_request_budget(token)

    assert calls[0]["timeout"] == 3.0


def test_total_deadline_reserves_bounded_cleanup_window():
    with patch("cron.scheduler.time.monotonic", return_value=100.0):
        control = _CronRunControl(
            {"id": "proof-0039", "run_timeout_seconds": 10},
            started_at=100.0,
        )
        assert control.hard_deadline() == 110.0
        assert control.cleanup_grace() == 1.0
        assert control.provider_deadline() == 109.0
        assert control.execution_remaining() == 9.0


def test_timeout_revokes_authority_in_already_copied_provider_context():
    control = _CronRunControl({"id": "proof-0039", "run_timeout_seconds": 1})
    tokens = set_cron_authorization(
        {
            "HERMES_CRON_JOB_ID": "proof-0039",
            "HERMES_CRON_AUTHORIZED_BEHAVIOR_REF": "behavior.proof-0039",
        },
        lease=control.lease,
    )
    try:
        copied = contextvars.copy_context()
        assert copied.run(get_cron_runtime_context).job_id == "proof-0039"
        control.interrupt("deadline")
        assert copied.run(get_cron_runtime_context) is None
    finally:
        reset_cron_authorization(tokens)


def test_timeout_waits_for_the_single_owned_script_cleanup_fence():
    process = SimpleNamespace()
    control = _CronRunControl({"id": "proof-0039"})
    assert control.begin_script_spawn() is True
    cleanup = control.attach_script_process(process)

    with patch("cron.scheduler._kill_cron_process_group") as kill_group:
        control.interrupt("deadline")

    kill_group.assert_called_once_with(process)
    assert cleanup.done.is_set() is True
    assert control._script_process is None
    assert control._script_cleanup is None
