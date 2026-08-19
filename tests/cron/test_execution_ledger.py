"""Durable cron execution-ledger behavior."""

from __future__ import annotations

import json
import copy
import os
import sqlite3
import subprocess
import sys
from pathlib import Path


def _point_ledger(monkeypatch, tmp_path):
    import cron.executions as executions

    monkeypatch.setattr(executions, "EXECUTIONS_FILE", tmp_path / "cron" / "executions.db")
    return executions


def test_execution_transitions_are_durable(monkeypatch, tmp_path):
    executions = _point_ledger(monkeypatch, tmp_path)

    claimed = executions.create_execution("job-1", source="builtin")
    assert claimed["status"] == "claimed"
    assert claimed["claimed_at"]
    assert claimed["started_at"] is None
    assert claimed["finished_at"] is None

    running = executions.mark_execution_running(claimed["id"], job_id="job-1")
    assert running["status"] == "running"
    assert running["started_at"]

    completed = executions.finish_execution(
        claimed["id"], job_id="job-1", success=True
    )
    assert completed["status"] == "completed"
    assert completed["finished_at"]
    assert completed["error"] is None

    persisted = executions.list_executions(job_id="job-1")
    assert persisted == [completed]


def test_terminal_execution_cannot_be_rewritten(monkeypatch, tmp_path):
    executions = _point_ledger(monkeypatch, tmp_path)
    record = executions.create_execution("immutable", source="builtin")
    executions.mark_execution_running(record["id"], job_id="immutable")
    executions.finish_execution(record["id"], job_id="immutable", success=True)

    assert executions.finish_execution(
        record["id"], job_id="immutable", success=False, error="late writer"
    ) is None
    assert executions.latest_execution("immutable")["status"] == "completed"


def test_execution_transitions_require_exact_job_binding(monkeypatch, tmp_path):
    executions = _point_ledger(monkeypatch, tmp_path)
    record = executions.create_execution("job-a", source="builtin")

    assert executions.mark_execution_running(
        record["id"], job_id="job-b"
    ) is None
    assert executions.finish_execution(
        record["id"], job_id="job-b", success=True
    ) is None
    assert executions.latest_execution("job-a")["status"] == "claimed"

    running = executions.mark_execution_running(record["id"], job_id="job-a")
    assert running["status"] == "running"
    assert executions.finish_execution(
        record["id"], job_id="job-a", success=True
    )["status"] == "completed"
    assert executions.mark_execution_running(
        record["id"], job_id="job-a"
    ) is None
    assert executions.finish_execution(
        record["id"], job_id="job-a", success=False
    ) is None


def test_retention_bounds_terminal_history_but_preserves_inflight(monkeypatch, tmp_path):
    executions = _point_ledger(monkeypatch, tmp_path)
    monkeypatch.setattr(executions, "MAX_TERMINAL_EXECUTIONS", 3)
    inflight = executions.create_execution("live", source="builtin")
    executions.mark_execution_running(inflight["id"], job_id="live")
    for index in range(8):
        row = executions.create_execution(f"done-{index}", source="builtin")
        executions.finish_execution(row["id"], job_id=f"done-{index}", success=True)

    records = executions.list_executions(limit=100)
    assert len([row for row in records if row["status"] == "completed"]) == 3
    assert executions.latest_execution("live")["status"] == "running"


def test_corrupt_store_fails_closed_without_overwrite(monkeypatch, tmp_path):
    executions = _point_ledger(monkeypatch, tmp_path)
    executions.EXECUTIONS_FILE.parent.mkdir(parents=True)
    executions.EXECUTIONS_FILE.write_bytes(b"not a sqlite database")

    with __import__("pytest").raises(sqlite3.DatabaseError):
        executions.create_execution("new", source="builtin")
    assert executions.EXECUTIONS_FILE.read_bytes() == b"not a sqlite database"


def test_cron_runs_cli_prints_execution_history(monkeypatch, tmp_path, capsys):
    executions = _point_ledger(monkeypatch, tmp_path)
    row = executions.create_execution("cli-job", source="builtin")
    executions.finish_execution(
        row["id"], job_id="cli-job", success=False, error="boom"
    )
    from hermes_cli.cron import cron_runs

    cron_runs("cli-job", limit=10)

    output = capsys.readouterr().out
    assert row["id"] in output
    assert "failed" in output
    assert "boom" in output


def test_quick_backup_includes_execution_ledger():
    from hermes_cli.backup import _QUICK_STATE_FILES

    assert "cron/executions.db" in _QUICK_STATE_FILES


def test_failed_execution_keeps_error(monkeypatch, tmp_path):
    executions = _point_ledger(monkeypatch, tmp_path)

    record = executions.create_execution("job-2", source="external")
    failed = executions.finish_execution(
        record["id"], job_id="job-2", success=False, error="provider exploded"
    )

    assert failed["status"] == "failed"
    assert failed["error"] == "provider exploded"


def test_recovery_does_not_mark_live_process_execution_unknown(monkeypatch, tmp_path):
    executions = _point_ledger(monkeypatch, tmp_path)
    record = executions.create_execution("still-live", source="builtin")
    executions.mark_execution_running(record["id"], job_id="still-live")

    assert executions.recover_interrupted_executions() == 0
    assert executions.latest_execution("still-live")["status"] == "running"


def test_restart_marks_interrupted_execution_unknown_without_requeue(tmp_path):
    """Real temp-HERMES_HOME subprocess restart: in-flight is audit-only unknown."""
    home = tmp_path / "home"
    repo = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["HERMES_HOME"] = str(home)
    env["PYTHONPATH"] = str(repo)

    create = subprocess.run(
        [
            sys.executable,
            "-c",
            "from cron.executions import create_execution, mark_execution_running; "
            "r=create_execution('restart-job', source='builtin'); "
            "mark_execution_running(r['id'], job_id='restart-job'); print(r['id'])",
        ],
        cwd=repo,
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    execution_id = create.stdout.strip()

    recover = subprocess.run(
        [
            sys.executable,
            "-c",
            "import json; from cron.executions import recover_interrupted_executions, list_executions; "
            "print(recover_interrupted_executions()); "
            "print(json.dumps(list_executions(job_id='restart-job'))) ",
        ],
        cwd=repo,
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    lines = recover.stdout.strip().splitlines()
    assert lines[0] == "1"
    records = json.loads(lines[1])
    assert len(records) == 1
    assert records[0]["id"] == execution_id
    assert records[0]["status"] == "unknown"
    assert records[0]["finished_at"]
    assert "restart" in records[0]["error"].lower()
    # Recovery only classifies the old attempt. It must not manufacture a new
    # claimed record (which would imply an automatic retry).
    assert [r["status"] for r in records] == ["unknown"]


def test_generic_submit_failure_finishes_attempt_and_releases_guard(monkeypatch):
    import cron.scheduler as scheduler

    class BrokenPool:
        def submit(self, _callable):
            raise ValueError("executor rejected")

    finished = []
    monkeypatch.setattr(
        scheduler, "create_execution",
        lambda *_args, **_kwargs: {"id": "exec-submit-fail"},
    )
    monkeypatch.setattr(
        scheduler, "finish_execution",
        lambda execution_id, **kwargs: finished.append((execution_id, kwargs)),
    )
    monkeypatch.setattr(scheduler, "get_due_jobs", lambda: [{"id": "submit-fail"}])
    monkeypatch.setattr(scheduler, "advance_next_runs", lambda _ids: 0)
    monkeypatch.setattr(scheduler, "_get_parallel_pool", lambda _workers: BrokenPool())

    assert scheduler.tick(verbose=False, sync=False) == 0
    assert finished == [
            ("exec-submit-fail", {
                "job_id": "submit-fail",
                "success": False,
            "error": "Executor dispatch failed: executor rejected",
        })
    ]
    assert "submit-fail" not in scheduler.get_running_job_ids()


def test_run_one_job_records_running_then_terminal(monkeypatch):
    import cron.scheduler as scheduler

    events = []
    monkeypatch.setattr(
        scheduler,
        "mark_execution_running",
        lambda execution_id, *, job_id: events.append(
            ("running", execution_id, job_id)
        ) or {"id": execution_id, "job_id": job_id},
        raising=False,
    )
    monkeypatch.setattr(
        scheduler,
        "finish_execution",
        lambda execution_id, **kwargs: events.append(
            ("finish", execution_id, kwargs)
        ) or {"id": execution_id, "status": "completed"},
        raising=False,
    )
    monkeypatch.setattr(scheduler, "claim_dispatch", lambda _job_id: True)
    monkeypatch.setattr(
        scheduler,
        "_run_job_result",
        lambda job, **_kwargs: scheduler._RunJobResult(
            True, "output", "response", None
        ),
    )
    monkeypatch.setattr(scheduler, "save_job_output", lambda *_args: None)
    monkeypatch.setattr(scheduler, "_deliver_result", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scheduler, "mark_job_run", lambda *_args, **_kwargs: None)

    job = {"id": "job-3", "execution_id": "forged-persisted"}
    before = copy.deepcopy(job)
    assert scheduler.run_one_job(
        job,
        execution_id="exec-3",
    ) is True
    assert job == before
    assert events[0] == ("running", "exec-3", "job-3")
    assert events[-1][0:2] == ("finish", "exec-3")
    assert events[-1][2]["success"] is True


def test_builtin_dispatch_passes_execution_out_of_band_without_mutating_job(monkeypatch):
    import cron.scheduler as scheduler

    job = {
        "id": "builtin-job",
        "name": "builtin",
        "execution_id": "persisted-forgery",
    }
    before = copy.deepcopy(job)
    seen = []
    monkeypatch.setattr(scheduler, "get_due_jobs", lambda: [job])
    monkeypatch.setattr(scheduler, "advance_next_runs", lambda _ids: 1)
    monkeypatch.setattr(
        scheduler,
        "create_execution",
        lambda job_id, *, source: {
            "id": "builtin-execution",
            "job_id": job_id,
            "source": source,
        },
    )
    monkeypatch.setattr(
        scheduler,
        "run_one_job",
        lambda candidate, **kwargs: seen.append((candidate, kwargs)) or True,
    )

    assert scheduler.tick(verbose=False, sync=True) == 1
    assert job == before
    assert seen[0][0] is job
    assert seen[0][1]["execution_id"] == "builtin-execution"


def test_run_one_job_invalid_execution_id_has_no_job_side_effect(monkeypatch):
    import cron.scheduler as scheduler

    side_effects = []
    monkeypatch.setattr(
        scheduler,
        "mark_execution_running",
        lambda _execution_id, *, job_id: None,
    )
    monkeypatch.setattr(
        scheduler,
        "begin_job_run_outcome",
        lambda _job: side_effects.append("outcome"),
    )
    monkeypatch.setattr(
        scheduler,
        "finish_execution",
        lambda *_args, **_kwargs: side_effects.append("finish"),
    )

    assert scheduler.run_one_job(
        {"id": "job-a"}, execution_id="missing-or-cross-job"
    ) is False
    assert side_effects == []


def test_run_one_job_early_return_terminalizes_attempt_exactly_once(monkeypatch):
    import cron.scheduler as scheduler

    terminal = []
    monkeypatch.setattr(
        scheduler,
        "mark_execution_running",
        lambda execution_id, *, job_id: {
            "id": execution_id,
            "job_id": job_id,
            "status": "running",
        },
    )
    monkeypatch.setattr(
        scheduler,
        "_set_running_job_state",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        scheduler,
        "finish_execution",
        lambda execution_id, **kwargs: terminal.append((execution_id, kwargs))
        or {"id": execution_id, "status": "failed"},
    )

    assert scheduler.run_one_job(
        {"id": "early-return"}, execution_id="early-execution"
    ) is False
    assert len(terminal) == 1
    assert terminal[0][1]["job_id"] == "early-return"
    assert terminal[0][1]["success"] is False


def test_run_one_job_preflight_exception_terminalizes_attempt_exactly_once(monkeypatch):
    import cron.scheduler as scheduler

    terminal = []
    monkeypatch.setattr(
        scheduler,
        "mark_execution_running",
        lambda execution_id, *, job_id: {
            "id": execution_id,
            "job_id": job_id,
            "status": "running",
        },
    )
    monkeypatch.setattr(
        scheduler,
        "_set_running_job_state",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        scheduler,
        "begin_job_run_outcome",
        lambda _job: (_ for _ in ()).throw(RuntimeError("preflight exploded")),
    )
    monkeypatch.setattr(
        scheduler,
        "finish_execution",
        lambda execution_id, **kwargs: terminal.append((execution_id, kwargs))
        or {"id": execution_id, "status": "failed"},
    )

    assert scheduler.run_one_job(
        {"id": "preflight-error"}, execution_id="preflight-execution"
    ) is False
    assert len(terminal) == 1
    assert terminal[0][1]["job_id"] == "preflight-error"
    assert terminal[0][1]["error"] == "preflight exploded"


def test_run_one_job_dispatch_refusal_terminalizes_attempt_exactly_once(monkeypatch):
    import cron.scheduler as scheduler

    terminal = []
    monkeypatch.setattr(
        scheduler,
        "mark_execution_running",
        lambda execution_id, *, job_id: {
            "id": execution_id,
            "job_id": job_id,
            "status": "running",
        },
    )
    monkeypatch.setattr(
        scheduler,
        "_set_running_job_state",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(scheduler, "begin_job_run_outcome", lambda _job: None)
    monkeypatch.setattr(
        scheduler,
        "_claim_dispatch_with_running_state",
        lambda *_args, **_kwargs: (False, False),
    )
    monkeypatch.setattr(
        scheduler,
        "finish_execution",
        lambda execution_id, **kwargs: terminal.append((execution_id, kwargs))
        or {"id": execution_id, "status": "failed"},
    )

    assert scheduler.run_one_job(
        {"id": "dispatch-refused"}, execution_id="dispatch-execution"
    ) is True
    assert len(terminal) == 1
    assert terminal[0][1]["job_id"] == "dispatch-refused"


def test_run_one_job_dispatch_exception_terminalizes_attempt_exactly_once(monkeypatch):
    import cron.scheduler as scheduler

    terminal = []
    monkeypatch.setattr(
        scheduler,
        "mark_execution_running",
        lambda execution_id, *, job_id: {
            "id": execution_id,
            "job_id": job_id,
            "status": "running",
        },
    )
    monkeypatch.setattr(
        scheduler,
        "_set_running_job_state",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(scheduler, "begin_job_run_outcome", lambda _job: None)
    monkeypatch.setattr(
        scheduler,
        "_claim_dispatch_with_running_state",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("dispatch commit exploded")
        ),
    )
    monkeypatch.setattr(
        scheduler,
        "finish_execution",
        lambda execution_id, **kwargs: terminal.append((execution_id, kwargs))
        or {"id": execution_id, "status": "failed"},
    )

    assert scheduler.run_one_job(
        {"id": "dispatch-error"}, execution_id="dispatch-error-execution"
    ) is False
    assert len(terminal) == 1
    assert terminal[0][1]["error"] == "dispatch commit exploded"


def test_run_one_job_retries_terminal_write_after_transient_ledger_error(monkeypatch):
    import cron.scheduler as scheduler

    attempts = []
    monkeypatch.setattr(
        scheduler,
        "mark_execution_running",
        lambda execution_id, *, job_id: {
            "id": execution_id,
            "job_id": job_id,
            "status": "running",
        },
    )
    monkeypatch.setattr(scheduler, "claim_dispatch", lambda _job_id: True)
    monkeypatch.setattr(
        scheduler,
        "_run_job_result",
        lambda job, **_kwargs: scheduler._RunJobResult(
            True, "output", "response", None
        ),
    )
    monkeypatch.setattr(scheduler, "save_job_output", lambda *_args: None)
    monkeypatch.setattr(scheduler, "_deliver_result", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scheduler, "mark_job_run", lambda *_args, **_kwargs: None)

    def finish(execution_id, **kwargs):
        attempts.append((execution_id, kwargs))
        if len(attempts) == 1:
            raise OSError("transient ledger write failure")
        return {"id": execution_id, "status": "failed"}

    monkeypatch.setattr(scheduler, "finish_execution", finish)

    assert scheduler.run_one_job(
        {"id": "terminal-retry"}, execution_id="terminal-retry-execution"
    ) is False
    assert len(attempts) == 2
    assert attempts[1][1]["job_id"] == "terminal-retry"
    assert attempts[1][1]["error"] == "transient ledger write failure"


def test_run_one_job_running_transition_exception_terminalizes_claimed_attempt(monkeypatch):
    import cron.scheduler as scheduler

    terminal = []
    side_effects = []
    monkeypatch.setattr(
        scheduler,
        "mark_execution_running",
        lambda _execution_id, *, job_id: (_ for _ in ()).throw(
            OSError("running transition failed")
        ),
    )
    monkeypatch.setattr(
        scheduler,
        "begin_job_run_outcome",
        lambda _job: side_effects.append("business-side-effect"),
    )
    monkeypatch.setattr(
        scheduler,
        "finish_execution",
        lambda execution_id, **kwargs: terminal.append((execution_id, kwargs))
        or {"id": execution_id, "status": "failed"},
    )

    assert scheduler.run_one_job(
        {"id": "transition-error"}, execution_id="transition-execution"
    ) is False
    assert side_effects == []
    assert len(terminal) == 1
    assert terminal[0][1]["job_id"] == "transition-error"
    assert terminal[0][1]["error"] == "running transition failed"


def test_authoritative_reload_exception_terminalizes_claimed_attempt_once(monkeypatch):
    import cron.scheduler as scheduler

    terminal = []
    running = []
    business = []
    monkeypatch.setattr(
        scheduler,
        "get_persisted_job",
        lambda _job_id: (_ for _ in ()).throw(OSError("persisted job unavailable")),
    )
    monkeypatch.setattr(
        scheduler,
        "mark_execution_running",
        lambda *_args, **_kwargs: running.append("running"),
    )
    monkeypatch.setattr(
        scheduler,
        "begin_job_run_outcome",
        lambda _job: business.append("business"),
    )
    monkeypatch.setattr(scheduler, "mark_job_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scheduler,
        "finish_execution",
        lambda execution_id, **kwargs: terminal.append((execution_id, kwargs))
        or {"id": execution_id, "status": "failed"},
    )

    assert scheduler.run_one_job(
        {"id": "reload-error"}, execution_id="reload-attempt"
    ) is False
    assert running == []
    assert business == []
    assert len(terminal) == 1
    assert terminal[0][0] == "reload-attempt"
    assert terminal[0][1]["job_id"] == "reload-error"
    assert terminal[0][1]["error"] == "persisted job unavailable"


def test_provider_start_recovers_interrupted_records_before_tick(monkeypatch):
    import cron.scheduler_provider as provider

    events = []
    stop = __import__("threading").Event()
    stop.set()
    monkeypatch.setattr(
        "cron.executions.recover_interrupted_executions",
        lambda: events.append("recover") or 0,
        raising=False,
    )
    monkeypatch.setattr("cron.jobs.record_ticker_heartbeat", lambda **_kwargs: events.append("heartbeat"))

    provider.InProcessCronScheduler().start(stop, interval=1)

    assert events[:2] == ["recover", "heartbeat"]


def test_external_provider_start_recovers_interrupted_records(monkeypatch):
    from plugins.cron_providers.chronos import ChronosCronScheduler

    provider = ChronosCronScheduler()
    provider._client = type("Client", (), {"arm": lambda self, **kwargs: None})()
    events = []
    monkeypatch.setattr(
        "cron.executions.recover_interrupted_executions",
        lambda: events.append("recover") or 0,
    )
    monkeypatch.setattr(provider, "reconcile", lambda: events.append("reconcile"))

    provider.start(__import__("threading").Event())

    assert events == ["recover", "reconcile"]


class _TrackingConnection:
    """Delegates to a real sqlite3.Connection while recording close() calls.

    sqlite3.Connection is a static C type: it has no per-instance __dict__
    and its class methods can't be monkeypatched, so open/close tracking is
    done via a delegating wrapper returned in place of the real connection.
    """

    def __init__(self, real, closed_ids):
        object.__setattr__(self, "_real", real)
        object.__setattr__(self, "_closed_ids", closed_ids)

    def close(self):
        self._closed_ids.append(id(self._real))
        self._real.close()

    def __enter__(self):
        self._real.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._real.__exit__(exc_type, exc, tb)

    def __getattr__(self, name):
        return getattr(self._real, name)

    def __setattr__(self, name, value):
        setattr(self._real, name, value)


def _count_open_connections(executions, monkeypatch):
    """Wrap sqlite3.connect to track open/close balance for the ledger module."""
    opened_ids = []
    closed_ids = []
    real_connect = sqlite3.connect

    def tracking_connect(*args, **kwargs):
        conn = real_connect(*args, **kwargs)
        opened_ids.append(id(conn))
        return _TrackingConnection(conn, closed_ids)

    monkeypatch.setattr(executions.sqlite3, "connect", tracking_connect)
    return opened_ids, closed_ids


def test_ledger_operations_close_every_connection(monkeypatch, tmp_path):
    """Regression for #69567: every ledger call must close its connection
    deterministically instead of relying on garbage collection."""
    executions = _point_ledger(monkeypatch, tmp_path)
    opened, closed = _count_open_connections(executions, monkeypatch)

    record = executions.create_execution("leak-check", source="builtin")
    executions.mark_execution_running(record["id"], job_id="leak-check")
    executions.finish_execution(record["id"], job_id="leak-check", success=True)
    executions.list_executions(job_id="leak-check")
    executions.latest_executions(["leak-check"])
    executions.recover_interrupted_executions()

    assert len(opened) == 6
    assert len(closed) == 6
    assert set(opened) == set(closed)


def test_early_return_still_closes_connection(monkeypatch, tmp_path):
    """mark_execution_running returns None mid-block on a bad transition;
    the connection must still be closed rather than leaked."""
    executions = _point_ledger(monkeypatch, tmp_path)
    opened, closed = _count_open_connections(executions, monkeypatch)

    assert executions.mark_execution_running(
        "does-not-exist", job_id="missing-job"
    ) is None

    assert len(opened) == 1
    assert len(closed) == 1


def test_exception_during_operation_still_closes_connection(monkeypatch, tmp_path):
    """A failing statement inside the transaction must roll back and close,
    not leak the connection."""
    executions = _point_ledger(monkeypatch, tmp_path)
    opened, closed = _count_open_connections(executions, monkeypatch)

    with __import__("pytest").raises(sqlite3.IntegrityError):
        with executions._transaction() as conn:
            conn.execute(
                "INSERT INTO executions (id, job_id, source, process_id, pid, "
                "status, claimed_at) VALUES ('x', 'x', 'x', 'x', 1, 'bogus-status', 'now')"
            )

    assert len(opened) == 1
    assert len(closed) == 1


def test_schema_init_failure_still_closes_connection(monkeypatch, tmp_path):
    """If PRAGMA/DDL setup in _connect() fails after sqlite3.connect()
    succeeds, the partially-initialized connection must still be closed."""
    executions = _point_ledger(monkeypatch, tmp_path)
    opened_ids = []
    closed_ids = []
    real_connect = sqlite3.connect

    class _FailingSchemaConnection(_TrackingConnection):
        def execute(self, sql, *args, **kwargs):
            if "CREATE TABLE" in sql:
                raise sqlite3.OperationalError("simulated schema init failure")
            return self._real.execute(sql, *args, **kwargs)

    def tracking_connect(*args, **kwargs):
        conn = real_connect(*args, **kwargs)
        opened_ids.append(id(conn))
        return _FailingSchemaConnection(conn, closed_ids)

    monkeypatch.setattr(executions.sqlite3, "connect", tracking_connect)

    with __import__("pytest").raises(sqlite3.OperationalError):
        executions.create_execution("init-fail", source="builtin")

    assert len(opened_ids) == 1
    assert len(closed_ids) == 1


def test_job_listing_exposes_latest_execution(monkeypatch, tmp_path):
    import cron.jobs as jobs

    monkeypatch.setattr(jobs, "CRON_DIR", tmp_path / "cron")
    monkeypatch.setattr(jobs, "JOBS_FILE", tmp_path / "cron" / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", tmp_path / "cron" / "output")
    executions = _point_ledger(monkeypatch, tmp_path)

    job = jobs.create_job(prompt="audit me", schedule="every 1h", name="audit")
    record = executions.create_execution(job["id"], source="builtin")
    executions.mark_execution_running(record["id"], job_id=job["id"])

    listed = jobs.list_jobs(include_disabled=True)
    assert listed[0]["latest_execution"]["id"] == record["id"]
    assert listed[0]["latest_execution"]["status"] == "running"
