"""Characterization + unit tests for the `run_one_job` shared helper (Phase 4A).

`tick`'s per-job body (`_process_job`) is the execute → save → deliver → mark
sequence that fires ONE due job. Phase 4A extracts it into a module-level
`run_one_job(job, *, adapters=None, loop=None, verbose=False)` so the external
Chronos provider's `fire_due` can reuse the IDENTICAL body — no duplicated
correctness.

The first test characterizes the sequence as driven through `tick()` (proving
the extraction didn't change `tick`'s behavior); the rest unit-test the
extracted helper directly.
"""
import cron.scheduler as s
import copy


def _patch_pipeline(monkeypatch, *, success=True, output="out", final="final response",
                    error=None, silent_marker_in=None):
    """Patch the job pipeline primitives and record the call order."""
    calls = []

    def fake_run_job(job, *, defer_agent_teardown=None, **_kwargs):
        calls.append(("run_job", job["id"]))
        fr = final if silent_marker_in is None else silent_marker_in
        return (success, output, fr, error)

    def fake_save(jid, out):
        calls.append(("save", jid))
        return f"/tmp/{jid}.txt"

    def fake_deliver(job, content, adapters=None, loop=None, **_kwargs):
        calls.append(("deliver", job["id"]))
        return None

    def fake_mark(jid, ok, err=None, delivery_error=None, **_kwargs):
        calls.append(("mark", jid, ok))

    monkeypatch.setattr(s, "_run_job_result", fake_run_job)
    monkeypatch.setattr(s, "save_job_output", fake_save)
    monkeypatch.setattr(s, "_deliver_result", fake_deliver)
    monkeypatch.setattr(s, "mark_job_run", fake_mark)
    return calls


def test_tick_process_job_sequence(monkeypatch):
    """Characterization: a single due job driven through tick() runs the
    sequence run_job → save → deliver → mark, in that order."""
    calls = _patch_pipeline(monkeypatch)
    monkeypatch.setattr(s, "get_due_jobs", lambda: [{"id": "j1", "name": "t"}])
    monkeypatch.setattr(s, "advance_next_runs", lambda ids: 1)

    s.tick(verbose=False, sync=True)

    assert [c[0] for c in calls] == ["run_job", "save", "deliver", "mark"]
    assert calls[-1] == ("mark", "j1", True)


def test_run_one_job_success_sequence(monkeypatch):
    """The extracted helper runs the same execute→save→deliver→mark sequence
    for a successful job."""
    calls = _patch_pipeline(monkeypatch)

    ok = s.run_one_job({"id": "j2", "name": "t"})

    assert ok is True
    assert [c[0] for c in calls] == ["run_job", "save", "deliver", "mark"]
    assert calls[-1] == ("mark", "j2", True)


def test_direct_run_creates_attempt_out_of_band_and_preserves_job(monkeypatch):
    calls = _patch_pipeline(monkeypatch)
    job = {"id": "direct-job", "name": "direct", "execution_id": "forged"}
    before = copy.deepcopy(job)
    attempts = []
    ledger = []
    monkeypatch.setattr(s, "get_persisted_job", lambda _job_id: None)
    monkeypatch.setattr(
        s,
        "create_execution",
        lambda job_id, *, source: attempts.append((job_id, source))
        or {"id": "direct-execution"},
    )
    monkeypatch.setattr(
        s,
        "mark_execution_running",
        lambda execution_id, *, job_id: ledger.append(
            ("running", execution_id, job_id)
        ) or {"id": execution_id, "job_id": job_id},
    )
    monkeypatch.setattr(
        s,
        "finish_execution",
        lambda execution_id, **kwargs: ledger.append(
            ("terminal", execution_id, kwargs)
        ) or {"id": execution_id, "status": "completed"},
    )

    assert s.run_one_job(job) is True
    assert job == before
    assert attempts == [("direct-job", "direct")]
    assert ledger[0] == ("running", "direct-execution", "direct-job")
    assert ledger[-1][0:2] == ("terminal", "direct-execution")
    assert [item[0] for item in calls] == ["run_job", "save", "deliver", "mark"]


def test_direct_run_reloads_authoritative_persisted_job(monkeypatch):
    stored = {
        "id": "direct-stored",
        "name": "Stored",
        "authorized_behavior_ref": "behavior.direct",
        "future_semantic_field": {"mode": "strict"},
    }
    caller_view = {
        **stored,
        "prompt": "",
        "skill": None,
        "skills": [],
    }
    seen = []
    monkeypatch.setattr(s, "get_persisted_job", lambda _job_id: copy.deepcopy(stored))
    monkeypatch.setattr(
        s,
        "mark_execution_running",
        lambda execution_id, *, job_id: {"id": execution_id, "job_id": job_id},
    )
    monkeypatch.setattr(s, "finish_execution", lambda execution_id, **kwargs: {"id": execution_id})
    monkeypatch.setattr(s, "_set_running_job_state", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(s, "begin_job_run_outcome", lambda _job: None)
    monkeypatch.setattr(s, "claim_dispatch", lambda _job_id: True)
    monkeypatch.setattr(
        s,
        "_run_job_result",
        lambda job, **_kwargs: seen.append(copy.deepcopy(job))
        or s._RunJobResult(True, "out", "final", None),
    )
    monkeypatch.setattr(s, "save_job_output", lambda *_args: "/tmp/direct-stored.md")
    monkeypatch.setattr(s, "_deliver_result", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(s, "mark_job_run", lambda *_args, **_kwargs: None)

    assert s.run_one_job(caller_view, execution_id="direct-attempt") is True
    assert seen == [stored]
    assert caller_view["prompt"] == ""


def test_run_one_job_passes_ledger_context_without_mutating_job(monkeypatch):
    job = {"id": "manual-job", "name": "manual", "prompt": "report"}
    before = copy.deepcopy(job)
    seen = []
    monkeypatch.setattr(s, "get_persisted_job", lambda _job_id: None)
    monkeypatch.setattr(
        s,
        "mark_execution_running",
        lambda execution_id, *, job_id: {
            "id": execution_id,
            "job_id": job_id,
            "source": "manual",
            "started_at": "2026-08-22T16:24:00+08:00",
        },
    )
    monkeypatch.setattr(
        s,
        "finish_execution",
        lambda *_args, **_kwargs: {"status": "completed"},
    )
    monkeypatch.setattr(
        s,
        "list_executions",
        lambda **_kwargs: [
            {"id": "manual-attempt", "source": "manual", "status": "running"},
            {
                "id": "scheduled-4",
                "source": "builtin",
                "status": "completed",
                "finished_at": "2026-08-21T23:04:31+08:00",
            },
            {
                "id": "scheduled-3",
                "source": "builtin",
                "status": "completed",
                "finished_at": "2026-08-20T23:05:20+08:00",
            },
            {"id": "scheduled-failure", "source": "builtin", "status": "failed"},
        ],
    )
    monkeypatch.setattr(s, "_set_running_job_state", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(s, "begin_job_run_outcome", lambda _job: None)
    monkeypatch.setattr(s, "claim_dispatch", lambda _job_id: True)
    monkeypatch.setattr(
        s,
        "_run_job_result",
        lambda candidate, **kwargs: seen.append((copy.deepcopy(candidate), kwargs))
        or s._RunJobResult(True, "out", "final", None),
    )
    monkeypatch.setattr(s, "save_job_output", lambda *_args: "/tmp/manual-job.md")
    monkeypatch.setattr(s, "_deliver_result", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(s, "mark_job_run", lambda *_args, **_kwargs: None)

    assert s.run_one_job(job, execution_id="manual-attempt") is True
    assert job == before
    assert seen[0][0] == before
    assert seen[0][1]["execution_context"]["source"] == "manual"
    assert seen[0][1]["execution_context"]["prior_builtin_success_streak"] == 2
    assert seen[0][1]["execution_context"]["prior_builtin_success_times"] == [
        "2026-08-21T23:04:31+08:00",
        "2026-08-20T23:05:20+08:00",
    ]


def test_run_one_job_installs_secret_scope_under_multiplex(monkeypatch, tmp_path):
    """Regression: under profile isolation (multiplex active), run_one_job must
    execute run_job inside a profile secret scope so credential reads
    (resolve_runtime_provider -> get_secret) don't fail-close with
    UnscopedSecretError, and must tear the scope down afterward.

    Behavior contract: a scope is present during run_job and absent after,
    regardless of the concrete secret values.
    """
    from agent import secret_scope as ss

    # Point cron's home resolution at a profile whose .env carries a secret.
    (tmp_path / ".env").write_text("OPENROUTER_BASE_URL=https://openrouter.ai/api/v1\n")
    monkeypatch.setattr(s, "_get_hermes_home", lambda: tmp_path)

    scope_during_run = {}

    def fake_run_job(job, *, defer_agent_teardown=None, **_kwargs):
        # This is where resolve_runtime_provider() would read a secret. Prove a
        # scope is installed and the profile's secret resolves without raising.
        scope_during_run["scope"] = ss.current_secret_scope()
        scope_during_run["base_url"] = ss.get_secret("OPENROUTER_BASE_URL")
        return (True, "out", "final", None)

    monkeypatch.setattr(s, "_run_job_result", fake_run_job)
    monkeypatch.setattr(s, "save_job_output", lambda jid, out: f"/tmp/{jid}.txt")
    monkeypatch.setattr(s, "_deliver_result", lambda *a, **k: None)
    monkeypatch.setattr(s, "mark_job_run", lambda *a, **k: None)

    ss.set_multiplex_active(True)
    try:
        ok = s.run_one_job({"id": "j7", "name": "t"})
    finally:
        ss.set_multiplex_active(False)

    assert ok is True
    # Scope was installed during run_job and the profile secret resolved.
    assert scope_during_run["scope"] is not None
    assert scope_during_run["base_url"] == "https://openrouter.ai/api/v1"
    # And it was torn down after run_one_job returned (no leak).
    assert ss.current_secret_scope() is None
