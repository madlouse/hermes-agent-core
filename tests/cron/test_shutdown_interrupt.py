"""Tests for #60432: cron jobs must not be silently invisible to gateway
shutdown, and a job whose tool subprocess got killed by shutdown must
never be reported as a successful run.

Covers the cron/scheduler.py primitives directly:
  - get_running_job_ids() -- thread-safe snapshot the gateway drain reads
  - mark_running_jobs_interrupted() -- called by the gateway right after
    it force-kills tool subprocesses
  - the interrupted-flag race guard in run_one_job(), which must win over
    the job's own thread finishing normally with a plausible-looking
    result AFTER its tool was already killed out from under it
"""

from unittest.mock import patch
import threading

import pytest


@pytest.fixture(autouse=True)
def _reset_scheduler_state():
    """Every test starts from a clean slate and leaves one behind, since
    these sets are module-level globals shared across the test process."""
    import cron.scheduler as sched

    sched._running_job_ids.clear()
    sched._running_job_states.clear()
    sched._interrupted_job_ids.clear()
    yield
    sched._running_job_ids.clear()
    sched._running_job_states.clear()
    sched._interrupted_job_ids.clear()


class TestGetRunningJobIds:
    def test_empty_when_nothing_running(self):
        import cron.scheduler as sched

        assert sched.get_running_job_ids() == frozenset()

    def test_reflects_in_flight_jobs(self):
        import cron.scheduler as sched

        sched._running_job_ids.add("job-1")
        sched._running_job_ids.add("job-2")

        result = sched.get_running_job_ids()

        assert result == frozenset({"job-1", "job-2"})

    def test_snapshot_is_immutable_and_independent(self):
        """Mutating _running_job_ids after the call must not change the
        already-returned snapshot -- callers (the gateway drain loop) rely
        on this to safely count in a tight polling loop."""
        import cron.scheduler as sched

        sched._running_job_ids.add("job-1")
        snapshot = sched.get_running_job_ids()
        sched._running_job_ids.add("job-2")

        assert snapshot == frozenset({"job-1"})


class TestMarkRunningJobsInterrupted:
    def test_no_op_when_nothing_running(self):
        import cron.scheduler as sched

        with patch("cron.scheduler.mark_job_run") as mock_mark:
            marked = sched.mark_running_jobs_interrupted("shutdown")

        assert marked == []
        mock_mark.assert_not_called()

    def test_marks_every_in_flight_job(self):
        import cron.scheduler as sched

        sched._running_job_ids.update({"job-1", "job-2"})

        with patch("cron.scheduler.mark_job_run") as mock_mark:
            marked = sched.mark_running_jobs_interrupted("gateway shutdown (final-cleanup)")

        assert sorted(marked) == ["job-1", "job-2"]
        assert mock_mark.call_count == 2
        called_ids = {c.args[0] for c in mock_mark.call_args_list}
        assert called_ids == {"job-1", "job-2"}
        for c in mock_mark.call_args_list:
            # success must be False -- an interrupted run is never "ok".
            assert c.args[1] is False
            assert "gateway shutdown" in c.args[2]

    def test_sets_interrupted_flag_for_consumption_by_run_one_job(self):
        import cron.scheduler as sched

        sched._running_job_ids.add("job-1")

        with patch("cron.scheduler.mark_job_run"):
            sched.mark_running_jobs_interrupted("shutdown")

        assert "job-1" in sched._interrupted_job_ids

    def test_one_job_marking_failure_does_not_block_the_others(self):
        """mark_job_run raising for one job (e.g. a jobs.json write race)
        must not prevent the rest from being marked -- this runs during
        shutdown, there's no retry window."""
        import cron.scheduler as sched

        sched._running_job_ids.update({"job-1", "job-2"})

        def _side_effect(job_id, success, reason, **kwargs):
            if job_id == "job-1":
                raise OSError("disk full")
            return True

        with patch("cron.scheduler.mark_job_run", side_effect=_side_effect):
            marked = sched.mark_running_jobs_interrupted("shutdown")

        assert marked == ["job-2"]

    def test_only_reports_jobs_whose_terminal_write_succeeded(self):
        import cron.scheduler as sched

        sched._running_job_ids.add("job-1")

        with patch("cron.scheduler.mark_job_run", return_value=False):
            marked = sched.mark_running_jobs_interrupted("shutdown")

        assert marked == []

    def test_queued_job_is_flagged_without_writing_a_false_run(self):
        import cron.scheduler as sched

        sched._running_job_ids.add("job-1")
        sched._running_job_states["job-1"] = {"phase": "queued"}

        with patch("cron.scheduler.mark_job_run") as mock_mark:
            marked = sched.mark_running_jobs_interrupted("shutdown")

        assert marked == []
        assert "job-1" in sched._interrupted_job_ids
        mock_mark.assert_not_called()

    def test_signed_pre_dispatch_claim_is_abandoned_exactly(self):
        import cron.scheduler as sched

        claim = {"run_id": "cron-run:" + "1" * 32}
        sched._running_job_ids.add("job-1")
        sched._running_job_states["job-1"] = {
            "phase": "preflight",
            "run_outcome_claim": claim,
            "run_claim": {"by": "scheduler"},
            "fire_claim": None,
        }

        with patch("cron.scheduler.abandon_job_run_outcome", return_value=True) as abandon, \
             patch("cron.scheduler.mark_job_run") as mock_mark:
            marked = sched.mark_running_jobs_interrupted("shutdown")

        assert marked == []
        abandon.assert_called_once_with(
            "job-1",
            claim,
            reason_code="run_outcome_interrupted_before_dispatch",
            run_claim={"by": "scheduler"},
            fire_claim=None,
        )
        mock_mark.assert_not_called()

    def test_unsigned_pre_dispatch_state_needs_no_persistent_write(self):
        import cron.scheduler as sched

        sched._running_job_ids.add("job-1")
        sched._running_job_states["job-1"] = {
            "phase": "attempting",
            "run_outcome_claim": None,
        }

        with patch("cron.scheduler.abandon_job_run_outcome") as abandon, \
             patch("cron.scheduler.mark_job_run") as mock_mark:
            marked = sched.mark_running_jobs_interrupted("shutdown")

        assert marked == []
        abandon.assert_not_called()
        mock_mark.assert_not_called()

    def test_signed_committed_run_uses_exact_claim_for_terminal_write(self):
        import cron.scheduler as sched

        claim = {"run_id": "cron-run:" + "2" * 32}
        sched._running_job_ids.add("job-1")
        sched._running_job_states["job-1"] = {
            "phase": "committed",
            "run_outcome_claim": claim,
        }

        with patch("cron.scheduler.mark_job_run", return_value=True) as mock_mark:
            marked = sched.mark_running_jobs_interrupted("shutdown")

        assert marked == ["job-1"]
        mock_mark.assert_called_once_with(
            "job-1",
            False,
            "shutdown",
            run_outcome_claim=claim,
        )

    def test_real_signed_claim_has_no_shutdown_wedge_before_or_after_dispatch(
        self,
        tmp_path,
    ):
        import cron.jobs as jobs
        import cron.scheduler as sched

        def signed_oneshot(job_id):
            return {
                "id": job_id,
                "name": "signed shutdown race",
                "prompt": "report",
                "schedule": {
                    "kind": "once",
                    "run_at": "2099-01-01T00:00:00+00:00",
                },
                "repeat": {"times": 1, "completed": 0},
                "enabled": True,
                "state": "scheduled",
                "creation_governance_receipt": {
                    "schema_version": "cron-creation-governance/v1",
                    "profile_id": "default",
                    "cron_job_id": job_id,
                    "receipt_id": "sha256:" + "1" * 64,
                },
            }

        with jobs.use_cron_store(tmp_path):
            before = signed_oneshot("before-dispatch")
            jobs.save_jobs([before])
            before_claim = jobs.begin_job_run_outcome(before)
            assert before_claim is not None
            sched._running_job_ids.add(before["id"])
            sched._running_job_states[before["id"]] = {
                "phase": "preflight",
                "run_outcome_claim": before_claim,
                "run_claim": None,
                "fire_claim": None,
            }

            assert sched.mark_running_jobs_interrupted("shutdown") == []
            persisted = jobs.get_job(before["id"])
            assert persisted["active_run_outcome_claim"] is None
            assert persisted["repeat"]["completed"] == 0

            sched._running_job_ids.clear()
            sched._running_job_states.clear()
            sched._interrupted_job_ids.clear()

            after = signed_oneshot("after-dispatch")
            jobs.save_jobs([after])
            after_claim = jobs.begin_job_run_outcome(after)
            assert after_claim is not None
            assert jobs.claim_dispatch(
                after["id"],
                run_outcome_claim=after_claim,
            ) is True
            sched._running_job_ids.add(after["id"])
            sched._running_job_states[after["id"]] = {
                "phase": "committed",
                "run_outcome_claim": after_claim,
            }

            assert sched.mark_running_jobs_interrupted("shutdown") == [after["id"]]
            persisted = jobs.get_job(after["id"])
            assert persisted["state"] == "completed"
            assert persisted["active_run_outcome_claim"] is None


class TestIsInterrupted:
    """Peek-only check used at the delivery gate -- must NOT clear the
    flag, unlike _consume_interrupted_flag."""

    def test_false_when_not_marked(self):
        import cron.scheduler as sched

        assert sched._is_interrupted("job-1") is False

    def test_true_when_marked(self):
        import cron.scheduler as sched

        sched._interrupted_job_ids.add("job-1")

        assert sched._is_interrupted("job-1") is True

    def test_does_not_clear_the_flag(self):
        import cron.scheduler as sched

        sched._interrupted_job_ids.add("job-1")

        sched._is_interrupted("job-1")

        # Still set -- the later, authoritative check before mark_job_run
        # must still see it.
        assert "job-1" in sched._interrupted_job_ids
        assert sched._is_interrupted("job-1") is True


class TestConsumeInterruptedFlag:

    def test_true_and_clears_when_marked(self):
        import cron.scheduler as sched

        sched._interrupted_job_ids.add("job-1")

        assert sched._consume_interrupted_flag("job-1") is True
        # Consumed -- a second check (e.g. a later, unrelated fire of the
        # same recurring job ID) must not still read as interrupted.
        assert sched._consume_interrupted_flag("job-1") is False


class TestRunOneJobHonoursInterruptedFlag:
    """run_one_job() must not let a job's own completion overwrite a
    status the shutdown path already wrote for the same run."""

    def _make_job(self, job_id="job-1"):
        return {"id": job_id, "name": "test job", "prompt": "do work"}

    @pytest.mark.parametrize("signed", [False, True])
    def test_shutdown_after_claim_creation_aborts_before_dispatch(self, signed):
        import cron.scheduler as sched

        job = self._make_job()
        claim = {"run_id": "cron-run:" + "1" * 32} if signed else None
        if signed:
            job["creation_governance_receipt"] = {
                "receipt_id": "sha256:" + "1" * 64,
            }

        with patch(
            "cron.scheduler._set_running_job_state",
            side_effect=[False, True],
        ), patch(
            "cron.scheduler.begin_job_run_outcome",
            return_value=claim,
        ), patch(
            "cron.scheduler.abandon_job_run_outcome",
            return_value=True,
        ) as abandon, patch(
            "cron.scheduler.claim_dispatch",
        ) as dispatch:
            assert sched.run_one_job(job) is False

        dispatch.assert_not_called()
        if signed:
            abandon.assert_called_once_with(
                job["id"],
                claim,
                reason_code="run_outcome_interrupted_before_dispatch",
                run_claim=None,
                fire_claim=None,
            )
        else:
            abandon.assert_not_called()

    @pytest.mark.parametrize("signed", [False, True])
    def test_shutdown_before_dispatch_cas_aborts_without_execution(self, signed):
        import cron.scheduler as sched

        job = self._make_job()
        claim = {"run_id": "cron-run:" + "2" * 32} if signed else None
        if signed:
            job["creation_governance_receipt"] = {
                "receipt_id": "sha256:" + "2" * 64,
            }

        with patch(
            "cron.scheduler._set_running_job_state",
            side_effect=[False, False, True],
        ), patch(
            "cron.scheduler.begin_job_run_outcome",
            return_value=claim,
        ), patch(
            "cron.scheduler._cron_run_script_snapshot",
            return_value=(True, None),
        ), patch(
            "cron.scheduler.abandon_job_run_outcome",
            return_value=True,
        ) as abandon, patch(
            "cron.scheduler.claim_dispatch",
        ) as dispatch:
            assert sched.run_one_job(job) is False

        dispatch.assert_not_called()
        assert abandon.call_count == (1 if signed else 0)

    @pytest.mark.parametrize(
        ("signed", "interrupt_phase"),
        [(False, "committed"), (True, "committed"), (False, "executing"), (True, "executing")],
    )
    def test_shutdown_after_dispatch_persists_failure_before_execution(
        self,
        signed,
        interrupt_phase,
    ):
        import cron.scheduler as sched

        job = self._make_job()
        claim = {"run_id": "cron-run:" + "3" * 32} if signed else None
        if signed:
            job["creation_governance_receipt"] = {
                "receipt_id": "sha256:" + "3" * 64,
            }
        states = [False, False, False, interrupt_phase == "committed"]
        if interrupt_phase == "executing":
            states.append(True)

        with patch(
            "cron.scheduler._set_running_job_state",
            side_effect=states,
        ), patch(
            "cron.scheduler.begin_job_run_outcome",
            return_value=claim,
        ), patch(
            "cron.scheduler._cron_run_script_snapshot",
            return_value=(True, None),
        ), patch(
            "cron.scheduler.claim_dispatch",
            return_value=True,
        ), patch(
            "cron.scheduler.mark_job_run",
            return_value=True,
        ) as mark, patch(
            "cron.scheduler.run_job",
        ) as run:
            assert sched.run_one_job(job) is False

        expected_kwargs = {"run_outcome_claim": claim} if signed else {}
        mark.assert_called_once_with(
            job["id"],
            False,
            "Interrupted by gateway shutdown before execution started.",
            **expected_kwargs,
        )
        run.assert_not_called()

    def test_shutdown_cannot_abandon_a_dispatch_that_committed_behind_a_barrier(
        self,
        tmp_path,
    ):
        import cron.jobs as jobs
        import cron.scheduler as sched

        job = {
            "id": "dispatch-barrier",
            "name": "signed dispatch barrier",
            "prompt": "report",
            "schedule": {
                "kind": "once",
                "run_at": "2099-01-01T00:00:00+00:00",
            },
            "repeat": {"times": 1, "completed": 0},
            "enabled": True,
            "state": "scheduled",
            "creation_governance_receipt": {
                "schema_version": "cron-creation-governance/v1",
                "profile_id": "default",
                "cron_job_id": "dispatch-barrier",
                "receipt_id": "sha256:" + "4" * 64,
            },
        }
        dispatch_persisted = threading.Event()
        release_dispatch = threading.Event()
        executing_reached = threading.Event()
        release_executing = threading.Event()
        shutdown_started = threading.Event()
        shutdown_done = threading.Event()
        run_result = {}
        shutdown_result = {}

        with jobs.use_cron_store(tmp_path):
            jobs.save_jobs([job])
            claim = jobs.begin_job_run_outcome(job)
            assert claim is not None
            sched._running_job_ids.add(job["id"])
            real_claim_dispatch = sched.claim_dispatch
            real_set_state = sched._set_running_job_state

            def claim_with_barrier(*args, **kwargs):
                allowed = real_claim_dispatch(*args, **kwargs)
                dispatch_persisted.set()
                assert release_dispatch.wait(5)
                return allowed

            def state_with_execution_gate(job_id, phase, **kwargs):
                if phase == "executing":
                    executing_reached.set()
                    assert release_executing.wait(5)
                return real_set_state(job_id, phase, **kwargs)

            def run():
                with jobs.use_cron_store(tmp_path):
                    run_result["value"] = sched.run_one_job(job)

            def shutdown():
                shutdown_started.set()
                with jobs.use_cron_store(tmp_path):
                    shutdown_result["value"] = sched.mark_running_jobs_interrupted(
                        "shutdown"
                    )
                shutdown_done.set()

            with patch(
                "cron.scheduler.begin_job_run_outcome",
                return_value=claim,
            ), patch(
                "cron.scheduler.claim_dispatch",
                side_effect=claim_with_barrier,
            ), patch(
                "cron.scheduler._set_running_job_state",
                side_effect=state_with_execution_gate,
            ), patch("cron.scheduler.run_job") as run_job:
                run_thread = threading.Thread(target=run)
                run_thread.start()
                assert dispatch_persisted.wait(5)
                assert jobs.get_job(job["id"])["repeat"]["completed"] == 1

                shutdown_thread = threading.Thread(target=shutdown)
                shutdown_thread.start()
                assert shutdown_started.wait(5)
                assert not shutdown_done.wait(0.05)

                release_dispatch.set()
                assert executing_reached.wait(5)
                assert shutdown_done.wait(5)
                release_executing.set()
                shutdown_thread.join(5)
                run_thread.join(5)

            assert not shutdown_thread.is_alive()
            assert not run_thread.is_alive()
            assert shutdown_result["value"] == [job["id"]]
            assert run_result["value"] is False
            persisted = jobs.get_job(job["id"])
            assert persisted["state"] == "completed"
            assert persisted["active_run_outcome_claim"] is None
            run_job.assert_not_called()

    def test_dispatch_transition_does_not_invert_jobs_and_running_locks(
        self,
        tmp_path,
    ):
        import cron.jobs as jobs
        import cron.scheduler as sched

        job = {
            "id": "lock-order",
            "name": "signed lock order",
            "schedule": {
                "kind": "once",
                "run_at": "2099-01-01T00:00:00+00:00",
            },
            "repeat": {"times": 1, "completed": 0},
            "creation_governance_receipt": {
                "schema_version": "cron-creation-governance/v1",
                "profile_id": "default",
                "cron_job_id": "lock-order",
                "receipt_id": "sha256:" + "5" * 64,
            },
        }
        jobs_lock_held = threading.Event()
        release_running_snapshot = threading.Event()
        claim_entered = threading.Event()
        running_snapshot_done = threading.Event()
        dispatch_result = {}

        with jobs.use_cron_store(tmp_path):
            jobs.save_jobs([job])
            claim = jobs.begin_job_run_outcome(job)
            assert claim is not None
            sched._running_job_ids.add(job["id"])
            real_claim_dispatch = sched.claim_dispatch

            def claim_after_signal(*args, **kwargs):
                claim_entered.set()
                return real_claim_dispatch(*args, **kwargs)

            def hold_jobs_then_read_running():
                with jobs.use_cron_store(tmp_path):
                    with jobs._jobs_lock():
                        jobs_lock_held.set()
                        assert release_running_snapshot.wait(5)
                        assert job["id"] in sched.get_running_job_ids()
                running_snapshot_done.set()

            def dispatch():
                with jobs.use_cron_store(tmp_path):
                    dispatch_result["value"] = sched._claim_dispatch_with_running_state(
                        job["id"],
                        claim,
                    )

            with patch(
                "cron.scheduler.claim_dispatch",
                side_effect=claim_after_signal,
            ):
                scan_thread = threading.Thread(
                    target=hold_jobs_then_read_running,
                    daemon=True,
                )
                scan_thread.start()
                assert jobs_lock_held.wait(5)

                dispatch_thread = threading.Thread(target=dispatch, daemon=True)
                dispatch_thread.start()
                assert claim_entered.wait(5)

                release_running_snapshot.set()
                assert running_snapshot_done.wait(5)
                scan_thread.join(5)
                dispatch_thread.join(5)

            assert not scan_thread.is_alive()
            assert not dispatch_thread.is_alive()
            assert dispatch_result["value"] == (True, False)
            assert jobs.get_job(job["id"])["repeat"]["completed"] == 1
            assert sched._running_job_states[job["id"]]["phase"] == "committed"

    def test_success_path_skipped_when_interrupted(self):
        import cron.scheduler as sched

        job = self._make_job()

        def finish_after_shutdown(*_args, **_kwargs):
            sched._interrupted_job_ids.add(job["id"])
            return True, "full output", "final response", None

        with patch("cron.scheduler.claim_dispatch", return_value=True), \
             patch("agent.secret_scope.set_secret_scope", return_value=None), \
             patch("agent.secret_scope.build_profile_secret_scope", return_value=None), \
             patch("agent.secret_scope.reset_secret_scope"), \
             patch(
                 "cron.scheduler.run_job",
                 side_effect=finish_after_shutdown,
             ), \
             patch("cron.scheduler.save_job_output", return_value="/tmp/out.md"), \
             patch("cron.scheduler._is_cron_silence_response", return_value=False), \
             patch("cron.scheduler._deliver_result", return_value=None), \
             patch("cron.scheduler.mark_job_run") as mock_mark:
            result = sched.run_one_job(job)

        assert result is True
        # The would-be "success" write must NOT happen -- the shutdown
        # path already wrote the authoritative interrupted status.
        mock_mark.assert_not_called()
        # Flag is consumed so a later, unrelated fire of the same job ID
        # isn't permanently silenced.
        assert job["id"] not in sched._interrupted_job_ids

    def test_interrupted_job_delivers_failure_summary_not_raw_response(self):
        """The status-write guard alone isn't enough: delivery happens
        BEFORE mark_job_run in run_one_job's own flow, so a job that kept
        running post-kill and produced a plausible-looking final_response
        must not have that response sent to the user just because the
        eventual status write gets suppressed. Interrupted jobs must route
        through the same failure-summary delivery path a real failure
        would."""
        import cron.scheduler as sched

        job = self._make_job()

        def finish_after_shutdown(*_args, **_kwargs):
            sched._interrupted_job_ids.add(job["id"])
            return True, "full output", "a plausible final response", None

        with patch("cron.scheduler.claim_dispatch", return_value=True), \
             patch("agent.secret_scope.set_secret_scope", return_value=None), \
             patch("agent.secret_scope.build_profile_secret_scope", return_value=None), \
             patch("agent.secret_scope.reset_secret_scope"), \
             patch(
                 "cron.scheduler.run_job",
                 side_effect=finish_after_shutdown,
             ), \
             patch("cron.scheduler.save_job_output", return_value="/tmp/out.md"), \
             patch(
                 "cron.scheduler._summarize_cron_failure_for_delivery",
                 return_value="This run was interrupted.",
             ) as mock_summarize, \
             patch("cron.scheduler._is_cron_silence_response", return_value=False), \
             patch("cron.scheduler._deliver_result", return_value=None) as mock_deliver, \
             patch("cron.scheduler.mark_job_run"):
            result = sched.run_one_job(job)

        assert result is True
        mock_summarize.assert_called_once()
        # The summarizer's error argument must mention the interruption,
        # not be silently None / the agent's own (possibly absent) error.
        assert "interrupt" in mock_summarize.call_args.args[1].lower()
        delivered_content = mock_deliver.call_args.args[1]
        assert delivered_content == "This run was interrupted."
        assert "plausible final response" not in delivered_content


    def test_exception_path_also_honours_interrupted_flag(self):
        import cron.scheduler as sched

        job = self._make_job()
        sched._interrupted_job_ids.add(job["id"])

        with patch("cron.scheduler.claim_dispatch", return_value=True), \
             patch("agent.secret_scope.set_secret_scope", return_value=None), \
             patch("agent.secret_scope.build_profile_secret_scope", return_value=None), \
             patch("agent.secret_scope.reset_secret_scope"), \
             patch("cron.scheduler.run_job", side_effect=RuntimeError("boom")), \
             patch("cron.scheduler.mark_job_run") as mock_mark:
            result = sched.run_one_job(job)

        assert result is False
        mock_mark.assert_not_called()
