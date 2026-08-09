import contextvars
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from agent.auxiliary_client import _relay_sync_completion
from agent.request_budget import (
    reset_provider_request_budget,
    set_provider_request_budget,
)
from cron.scheduler import (
    _CRON_INTERRUPT_THREAD_PREFIX,
    _CRON_INTERRUPT_WORKER_LIMIT,
    _CronRunControl,
    _RunJobResult,
    _cron_bounded_interrupt_runner,
    _run_job_impl,
    _run_job_result,
)
from gateway.session_context import (
    get_cron_runtime_context,
    reset_cron_authorization,
    set_cron_authorization,
)
from run_agent import AIAgent


def _wait_until(predicate, *, timeout=1.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


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


def test_interrupt_is_bounded_when_process_group_kill_never_returns():
    process = SimpleNamespace()
    control = _CronRunControl(
        {"id": "proof-stuck-cleanup", "run_timeout_seconds": 0.1}
    )
    assert control.begin_script_spawn() is True
    cleanup = control.attach_script_process(process)
    kill_started = threading.Event()
    never_release = threading.Event()
    tokens = set_cron_authorization(
        {
            "HERMES_CRON_JOB_ID": "proof-stuck-cleanup",
            "HERMES_CRON_AUTHORIZED_BEHAVIOR_REF": "behavior.stuck-cleanup",
        },
        lease=control.lease,
    )
    try:
        copied = contextvars.copy_context()

        def stuck_kill(candidate):
            assert candidate is process
            kill_started.set()
            never_release.wait()

        with patch(
            "cron.scheduler._kill_cron_process_group", side_effect=stuck_kill
        ) as kill_group:
            started = time.monotonic()
            result = control.interrupt("deadline")
            elapsed = time.monotonic() - started

        assert kill_started.is_set()
        assert elapsed < 0.15
        assert result.cleanup_incomplete is True
        assert control.cleanup_incomplete() is True
        assert cleanup.done.is_set() is False
        assert copied.run(get_cron_runtime_context) is None
        kill_group.assert_called_once_with(process)
    finally:
        never_release.set()
        assert cleanup.done.wait(timeout=1)
        reset_cron_authorization(tokens)


def test_rejected_hard_interrupt_is_sticky_cleanup_incomplete():
    control = _CronRunControl({"id": "proof-rejected-interrupt"})
    control.set_agent(SimpleNamespace())

    result = control.interrupt("deadline", cleanup_timeout=0.1)

    assert result.cleanup_complete is True
    assert result.agent_interrupt_complete is False
    assert result.cleanup_incomplete is True
    assert control.cleanup_incomplete() is True
    assert _wait_until(
        lambda: _cron_bounded_interrupt_runner.active_count() == 0
    )


def test_worker_result_preserves_rejected_interrupt_status():
    control = _CronRunControl({"id": "proof-rejected-worker-result"})
    control.set_agent(SimpleNamespace())
    interrupt_result = control.interrupt("deadline", cleanup_timeout=0.1)
    assert interrupt_result.cleanup_incomplete is True

    with patch(
        "cron.scheduler._run_job_body",
        return_value=(True, "would-be success", "would-be success", None),
    ):
        result = _run_job_impl({}, _run_control=control)

    assert result.success is False
    assert result.cleanup_incomplete is True
    assert "cleanup_incomplete" in (result.error or "")


def test_interrupt_worker_capacity_is_fixed_nonqueued_and_reclaimed():
    assert _wait_until(
        lambda: _cron_bounded_interrupt_runner.active_count() == 0
    )
    release = threading.Event()
    calls_lock = threading.Lock()
    agent_calls = []
    blocked_cleanups = []
    rejected_cleanups = []

    class BlockingAgent:
        def __init__(self, identifier):
            self.identifier = identifier

        def hard_interrupt(self, message=None):
            with calls_lock:
                agent_calls.append(self.identifier)
            release.wait(timeout=2)

    def blocking_kill(candidate):
        release.wait(timeout=2)

    def live_bounded_workers():
        return [
            thread
            for thread in threading.enumerate()
            if thread.name.startswith(_CRON_INTERRUPT_THREAD_PREFIX)
        ]

    kill_slots = _CRON_INTERRUPT_WORKER_LIMIT // 2
    agent_slots = _CRON_INTERRUPT_WORKER_LIMIT - kill_slots
    with patch(
        "cron.scheduler._kill_cron_process_group", side_effect=blocking_kill
    ) as kill_group:
        try:
            for index in range(kill_slots):
                control = _CronRunControl({"id": f"capacity-kill-{index}"})
                assert control.begin_script_spawn() is True
                cleanup = control.attach_script_process(SimpleNamespace())
                blocked_cleanups.append(cleanup)
                result = control.interrupt("deadline", cleanup_timeout=0.01)
                assert result.cleanup_incomplete is True

            for index in range(agent_slots):
                control = _CronRunControl({"id": f"capacity-agent-{index}"})
                control.set_agent(BlockingAgent(f"blocked-{index}"))
                result = control.interrupt("deadline", cleanup_timeout=0.01)
                assert result.cleanup_incomplete is True

            assert _wait_until(
                lambda: _cron_bounded_interrupt_runner.active_count()
                == _CRON_INTERRUPT_WORKER_LIMIT
            )
            assert len(live_bounded_workers()) == _CRON_INTERRUPT_WORKER_LIMIT

            overflow_count = _CRON_INTERRUPT_WORKER_LIMIT * 3
            started = time.monotonic()
            for index in range(overflow_count):
                control = _CronRunControl({"id": f"capacity-overflow-{index}"})
                if index % 2:
                    control.set_agent(BlockingAgent(f"rejected-{index}"))
                else:
                    assert control.begin_script_spawn() is True
                    cleanup = control.attach_script_process(SimpleNamespace())
                    rejected_cleanups.append(cleanup)
                result = control.interrupt("deadline", cleanup_timeout=0.01)
                assert result.cleanup_incomplete is True
                assert control.cleanup_incomplete() is True

            assert time.monotonic() - started < 0.2
            time.sleep(0.02)
            assert (
                _cron_bounded_interrupt_runner.active_count()
                == _CRON_INTERRUPT_WORKER_LIMIT
            )
            assert len(live_bounded_workers()) == _CRON_INTERRUPT_WORKER_LIMIT
            assert kill_group.call_count == kill_slots
            assert len(agent_calls) == agent_slots
            assert all(cleanup.done.is_set() for cleanup in rejected_cleanups)
            assert all(not cleanup.completed for cleanup in rejected_cleanups)
        finally:
            release.set()
            assert _wait_until(
                lambda: _cron_bounded_interrupt_runner.active_count() == 0
            )
            assert _wait_until(lambda: not live_bounded_workers())

        assert all(cleanup.done.wait(timeout=1) for cleanup in blocked_cleanups)
        assert kill_group.call_count == kill_slots
        assert len(agent_calls) == agent_slots

        recovered = _CronRunControl({"id": "capacity-recovered"})
        recovered.set_agent(BlockingAgent("recovered"))
        assert recovered.begin_script_spawn() is True
        recovered_cleanup = recovered.attach_script_process(SimpleNamespace())
        recovered_result = recovered.interrupt(
            "deadline", cleanup_timeout=0.2
        )

        assert recovered_result.cleanup_incomplete is False
        assert recovered_cleanup.completed is True
        assert recovered.cleanup_incomplete() is False
        assert _wait_until(
            lambda: _cron_bounded_interrupt_runner.active_count() == 0
        )
        assert kill_group.call_count == kill_slots + 1
        assert len(agent_calls) == agent_slots + 1


def test_cleanup_owner_waiter_and_late_completion_share_one_kill():
    process = SimpleNamespace()
    control = _CronRunControl(
        {"id": "proof-late-cleanup", "run_timeout_seconds": 0.1}
    )
    assert control.begin_script_spawn() is True
    cleanup = control.attach_script_process(process)
    kill_started = threading.Event()
    release_kill = threading.Event()
    waiter_result = []
    tokens = set_cron_authorization(
        {
            "HERMES_CRON_JOB_ID": "proof-late-cleanup",
            "HERMES_CRON_AUTHORIZED_BEHAVIOR_REF": "behavior.late-cleanup",
        },
        lease=control.lease,
    )
    copied = contextvars.copy_context()

    def blocking_kill(candidate):
        assert candidate is process
        kill_started.set()
        assert release_kill.wait(timeout=2)

    try:
        with patch(
            "cron.scheduler._kill_cron_process_group", side_effect=blocking_kill
        ) as kill_group:
            owner_result = control.interrupt("deadline", cleanup_timeout=0.02)
            assert kill_started.wait(timeout=1)

            waiter = threading.Thread(
                target=lambda: waiter_result.append(
                    control.cleanup_script_process(
                        cleanup, kill=True, timeout=0.02
                    )
                )
            )
            waiter.start()
            waiter.join(timeout=0.2)

            assert not waiter.is_alive()
            assert owner_result.cleanup_incomplete is True
            assert waiter_result == [False]
            assert cleanup.done.is_set() is False
            assert control.cleanup_incomplete() is True
            assert copied.run(get_cron_runtime_context) is None
            kill_group.assert_called_once_with(process)

            release_kill.set()
            assert cleanup.done.wait(timeout=1)
            assert cleanup.completed is True
            assert control.cleanup_script_process(
                cleanup, kill=True, timeout=0.01
            ) is True
            kill_group.assert_called_once_with(process)

        assert copied.run(get_cron_runtime_context) is None
        assert control.cleanup_incomplete() is True
        assert control._script_process is None
        assert control._script_cleanup is None
    finally:
        release_kill.set()
        assert cleanup.done.wait(timeout=1)
        reset_cron_authorization(tokens)


def test_run_result_reports_cleanup_incomplete_with_stuck_cleanup():
    process = SimpleNamespace()
    cleanup_attached = threading.Event()
    release_worker = threading.Event()
    release_kill = threading.Event()
    result_holder = {}
    cleanup_holder = {}

    def fake_run_job_impl(
        job,
        *,
        defer_agent_teardown=None,
        _run_control,
        script_snapshot=None,
        run_outcome_claim=None,
    ):
        assert _run_control.begin_script_spawn() is True
        cleanup_holder["cleanup"] = _run_control.attach_script_process(process)
        cleanup_attached.set()
        release_worker.wait(timeout=2)
        return _RunJobResult(True, "late", "late", None)

    def blocking_kill(candidate):
        assert candidate is process
        release_kill.wait(timeout=2)

    def run_candidate():
        result_holder["result"] = _run_job_result(
            {
                "id": "proof-result-cleanup",
                "name": "proof result cleanup",
                "run_timeout_seconds": 0.1,
            }
        )

    with patch("cron.scheduler._run_job_impl", side_effect=fake_run_job_impl), patch(
        "cron.scheduler._kill_cron_process_group", side_effect=blocking_kill
    ):
        started = time.monotonic()
        runner = threading.Thread(target=run_candidate)
        runner.start()
        try:
            assert cleanup_attached.wait(timeout=1)
            runner.join(timeout=0.25)
            elapsed = time.monotonic() - started

            assert not runner.is_alive()
            assert elapsed < 0.2
            result = result_holder["result"]
            assert result.success is False
            assert result.cleanup_incomplete is True
            assert "cleanup_incomplete" in (result.error or "")
        finally:
            release_worker.set()
            release_kill.set()
            runner.join(timeout=1)
            cleanup = cleanup_holder.get("cleanup")
            if cleanup is not None:
                assert cleanup.done.wait(timeout=1)
