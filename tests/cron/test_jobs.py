"""Tests for cron/jobs.py — schedule parsing, job CRUD, and due-job detection."""

import hashlib
import json
import threading
import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cron.jobs import (
    _cron_checkpoint_invariant_hash,
    _cron_delivery_receipt_hash,
    _cron_interpreter_artifact_hash,
    _cron_run_artifact_hash,
    _cron_run_outcome_claim,
    _cron_run_outcome_receipt,
    _cron_run_script_snapshot,
    _cron_script_artifact_hash,
    _cron_support_artifact_hash,
    _validated_run_outcome_claim,
    _validated_run_outcome_receipt,
    CronJobGovernanceError,
    abandon_job_run_outcome,
    begin_job_run_outcome,
    parse_duration,
    parse_schedule,
    compute_next_run,
    create_job,
    load_jobs,
    save_jobs,
    get_job,
    heartbeat_job_run_outcome,
    list_jobs,
    update_job,
    pause_job,
    record_job_run_preflight_denial,
    resume_job,
    remove_job,
    mark_job_run,
    mark_job_delivery_recovered,
    advance_next_run,
    claim_dispatch,
    heartbeat_run_claim,
    get_due_jobs,
    save_job_output,
)


def _signed_job_revision(job_id="job.receipt", profile_id="profile-custom"):
    return {
        "id": job_id,
        "creation_governance_receipt": {
            "schema_version": "cron-creation-governance/v1",
            "profile_id": profile_id,
            "cron_job_id": job_id,
            "receipt_id": "sha256:" + "1" * 64,
            "candidate_hash": "sha256:" + "2" * 64,
            "job_semantic_hash": "sha256:" + "3" * 64,
        },
    }


# =========================================================================
# parse_duration
# =========================================================================

class TestParseDuration:
    def test_minutes(self):
        assert parse_duration("30m") == 30
        assert parse_duration("1min") == 1
        assert parse_duration("5mins") == 5
        assert parse_duration("10minute") == 10
        assert parse_duration("120minutes") == 120

    def test_hours(self):
        assert parse_duration("2h") == 120
        assert parse_duration("1hr") == 60
        assert parse_duration("3hrs") == 180
        assert parse_duration("1hour") == 60
        assert parse_duration("24hours") == 1440

    def test_days(self):
        assert parse_duration("1d") == 1440
        assert parse_duration("7day") == 7 * 1440
        assert parse_duration("2days") == 2 * 1440

    def test_whitespace_tolerance(self):
        assert parse_duration("  30m  ") == 30
        assert parse_duration("2 h") == 120

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            parse_duration("abc")
        with pytest.raises(ValueError):
            parse_duration("30x")
        with pytest.raises(ValueError):
            parse_duration("")
        with pytest.raises(ValueError):
            parse_duration("m30")


# =========================================================================
# parse_schedule
# =========================================================================

class TestParseSchedule:
    def test_duration_becomes_once(self):
        result = parse_schedule("30m")
        assert result["kind"] == "once"
        assert "run_at" in result
        # run_at should be a valid ISO timestamp string ~30 minutes from now
        run_at_str = result["run_at"]
        assert isinstance(run_at_str, str)
        run_at = datetime.fromisoformat(run_at_str)
        now = datetime.now().astimezone()
        assert run_at > now
        assert run_at < now + timedelta(minutes=31)

    def test_every_becomes_interval(self):
        result = parse_schedule("every 2h")
        assert result["kind"] == "interval"
        assert result["minutes"] == 120

    def test_every_case_insensitive(self):
        result = parse_schedule("Every 30m")
        assert result["kind"] == "interval"
        assert result["minutes"] == 30

    def test_cron_expression(self):
        pytest.importorskip("croniter")
        result = parse_schedule("0 9 * * *")
        assert result["kind"] == "cron"
        assert result["expr"] == "0 9 * * *"

    def test_iso_timestamp(self):
        result = parse_schedule("2030-01-15T14:00:00")
        assert result["kind"] == "once"
        assert "2030-01-15" in result["run_at"]

    def test_invalid_schedule_raises(self):
        with pytest.raises(ValueError):
            parse_schedule("not_a_schedule")

    def test_invalid_cron_raises(self):
        pytest.importorskip("croniter")
        with pytest.raises(ValueError):
            parse_schedule("99 99 99 99 99")

    def test_naive_iso_anchors_to_configured_tz_not_server_local(self, monkeypatch):
        """A naive ISO timestamp must be interpreted in the CONFIGURED Hermes
        timezone, NOT the server's local timezone (#51021).

        Regression: when the configured zone differs from the server's local
        zone (common on cloud hosts running UTC), parse_schedule used
        ``dt.astimezone()`` (server-local), baking in the wrong offset. The
        due-check compares against ``_hermes_now()`` (configured zone), so the
        stored instant landed hours off the user's wall-clock intent — far
        enough that one-shots never became due. This asserts the parsed offset
        matches the configured-now offset, the invariant that keeps the stored
        instant on the same clock the scheduler checks against.
        """
        configured_now = datetime(2026, 6, 22, 20, 0, 0, tzinfo=timezone(timedelta(hours=5, minutes=30)))
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: configured_now)

        result = parse_schedule("2026-06-22T20:07:00")  # naive, user wall-clock

        assert result["kind"] == "once"
        parsed = datetime.fromisoformat(result["run_at"])
        assert parsed.utcoffset() == configured_now.utcoffset()
        # Same wall-clock the user typed, on the configured clock.
        assert parsed.replace(tzinfo=None) == datetime(2026, 6, 22, 20, 7, 0)


# =========================================================================
# Timezone-divergence regression (#51021)
# =========================================================================

class TestNaiveScheduleTimezoneDivergence:
    """End-to-end: a one-shot created with a naive recent-past timestamp must
    become due even when the configured Hermes timezone differs from the
    server's local timezone. Before #51021 the naive value was anchored to
    server-local, so the job never fired."""

    def test_recent_past_oneshot_is_due_under_diverging_tz(self, tmp_cron_dir, monkeypatch):
        # Configured zone: a fixed +05:30 offset. The server's actual local
        # zone is irrelevant to the parse now — that is the whole point.
        configured = timezone(timedelta(hours=5, minutes=30))
        now = datetime(2026, 6, 22, 20, 7, 30, tzinfo=configured)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)

        # 30s ago in the configured wall clock, supplied as a NAIVE string.
        naive_str = (now - timedelta(seconds=30)).replace(tzinfo=None).isoformat()
        job = create_job(prompt="test message", schedule=naive_str, deliver="local")

        due = get_due_jobs()
        assert any(d["id"] == job["id"] for d in due), (
            f"one-shot should be due; next_run_at={job['next_run_at']}"
        )


# =========================================================================
# compute_next_run
# =========================================================================

class TestComputeNextRun:
    def test_once_future_returns_time(self):
        future = (datetime.now() + timedelta(hours=1)).isoformat()
        schedule = {"kind": "once", "run_at": future}
        assert compute_next_run(schedule) == future

    def test_once_recent_past_within_grace_returns_time(self, monkeypatch):
        now = datetime(2026, 3, 18, 4, 22, 3, tzinfo=timezone.utc)
        run_at = "2026-03-18T04:22:00+00:00"
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)

        schedule = {"kind": "once", "run_at": run_at}

        assert compute_next_run(schedule) == run_at

    def test_once_past_returns_none(self):
        past = (datetime.now() - timedelta(hours=1)).isoformat()
        schedule = {"kind": "once", "run_at": past}
        assert compute_next_run(schedule) is None

    def test_once_with_last_run_returns_none_even_within_grace(self, monkeypatch):
        now = datetime(2026, 3, 18, 4, 22, 3, tzinfo=timezone.utc)
        run_at = "2026-03-18T04:22:00+00:00"
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)

        schedule = {"kind": "once", "run_at": run_at}

        assert compute_next_run(schedule, last_run_at=now.isoformat()) is None

    def test_interval_first_run(self):
        schedule = {"kind": "interval", "minutes": 60}
        result = compute_next_run(schedule)
        next_dt = datetime.fromisoformat(result)
        # Should be ~60 minutes from now
        assert next_dt > datetime.now().astimezone() + timedelta(minutes=59)

    def test_interval_subsequent_run(self):
        schedule = {"kind": "interval", "minutes": 30}
        last = datetime.now().astimezone().isoformat()
        result = compute_next_run(schedule, last_run_at=last)
        next_dt = datetime.fromisoformat(result)
        # Should be ~30 minutes from last run
        assert next_dt > datetime.now().astimezone() + timedelta(minutes=29)

    def test_cron_returns_future(self):
        pytest.importorskip("croniter")
        schedule = {"kind": "cron", "expr": "* * * * *"}  # every minute
        result = compute_next_run(schedule)
        assert isinstance(result, str), f"Expected ISO timestamp string, got {type(result)}"
        assert len(result) > 0
        next_dt = datetime.fromisoformat(result)
        assert isinstance(next_dt, datetime)
        assert next_dt > datetime.now().astimezone()

    def test_unknown_kind_returns_none(self):
        assert compute_next_run({"kind": "unknown"}) is None


# =========================================================================
# Job CRUD (with tmp file storage)
# =========================================================================

@pytest.fixture()
def tmp_cron_dir(tmp_path, monkeypatch):
    """Redirect cron storage to a temp directory."""
    monkeypatch.setattr("cron.jobs.CRON_DIR", tmp_path / "cron")
    monkeypatch.setattr("cron.jobs.JOBS_FILE", tmp_path / "cron" / "jobs.json")
    monkeypatch.setattr("cron.jobs.OUTPUT_DIR", tmp_path / "cron" / "output")
    import cron.jobs as jobs_mod
    jobs_mod.ensure_dirs()
    return tmp_path


class TestJobCRUD:
    def test_create_and_get(self, tmp_cron_dir):
        job = create_job(prompt="Check server status", schedule="30m")
        assert job["id"]
        assert job["prompt"] == "Check server status"
        assert job["enabled"] is True
        assert job["schedule"]["kind"] == "once"

        fetched = get_job(job["id"])
        assert fetched is not None
        assert fetched["prompt"] == "Check server status"

    def test_list_jobs(self, tmp_cron_dir):
        create_job(prompt="Job 1", schedule="every 1h")
        create_job(prompt="Job 2", schedule="every 2h")
        jobs = list_jobs()
        assert len(jobs) == 2

    def test_list_jobs_normalizes_partial_legacy_records(self, tmp_cron_dir):
        save_jobs([
            {
                "id": "abc123deadbe",
                "name": None,
                "prompt": None,
                "schedule_display": None,
                "schedule": {"kind": "interval", "minutes": 60, "display": "every 60m"},
                "enabled": True,
            }
        ])

        jobs = list_jobs()

        assert jobs[0]["id"] == "abc123deadbe"
        assert jobs[0]["name"] == "abc123deadbe"
        assert jobs[0]["prompt"] == ""
        assert jobs[0]["schedule_display"] == "every 60m"
        assert jobs[0]["state"] == "scheduled"

    def test_remove_job(self, tmp_cron_dir):
        job = create_job(prompt="Temp job", schedule="30m")
        output_dir = tmp_cron_dir / "cron" / "output" / job["id"]
        output_dir.mkdir(parents=True)
        (output_dir / "result.md").write_text("done", encoding="utf-8")
        assert remove_job(job["id"]) is True
        assert get_job(job["id"]) is None
        assert not output_dir.exists()

    def test_remove_job_rejects_unsafe_legacy_id_before_output_cleanup(self, tmp_cron_dir):
        """Legacy unsafe IDs left over from before the create-time guard
        must fail closed without half-applying the removal."""
        job = create_job(prompt="Legacy unsafe", schedule="every 1h")
        job["id"] = "../escape"
        save_jobs([job])
        outside = tmp_cron_dir / "escape"
        outside.mkdir()
        (outside / "keep.txt").write_text("keep", encoding="utf-8")

        with pytest.raises(ValueError, match="output path"):
            remove_job("../escape")

        # Job should still be in the store and the escape dir untouched.
        assert load_jobs()[0]["id"] == "../escape"
        assert (outside / "keep.txt").exists()

    def test_remove_job_does_not_follow_a_swapped_output_directory(self, tmp_cron_dir, monkeypatch):
        import cron.jobs as jobs_mod

        job = create_job(prompt="safe cleanup", schedule="30m")
        cron_dir = tmp_cron_dir / "cron"
        output_dir = cron_dir / "output"
        job_dir = output_dir / job["id"]
        job_dir.mkdir(parents=True)
        (job_dir / "result.md").write_text("local", encoding="utf-8")
        external = tmp_cron_dir / "external-output"
        external_job_dir = external / job["id"]
        external_job_dir.mkdir(parents=True)
        sentinel = external_job_dir / "keep.txt"
        sentinel.write_text("unchanged", encoding="utf-8")
        original_save = jobs_mod._save_jobs_unlocked

        def swap_output_after_save(retained):
            original_save(retained)
            output_dir.rename(tmp_cron_dir / "output-before-swap")
            output_dir.symlink_to(external, target_is_directory=True)

        monkeypatch.setattr(jobs_mod, "_save_jobs_unlocked", swap_output_after_save)

        with pytest.raises(OSError, match="Cron output path is not a directory"):
            remove_job(job["id"])

        assert sentinel.read_text(encoding="utf-8") == "unchanged"
        assert external_job_dir.exists()

    def test_remove_nonexistent_returns_false(self, tmp_cron_dir):
        assert remove_job("nonexistent") is False

    def test_remove_rejects_active_signed_job_between_begin_and_dispatch(
        self,
        tmp_cron_dir,
    ):
        signed = {
            "name": "active signed removal",
            "schedule": {"kind": "once", "run_at": "2099-01-01T00:00:00+00:00"},
            "repeat": {"times": 1, "completed": 0},
            **_signed_job_revision("active-remove"),
        }
        save_jobs([signed])
        claim = begin_job_run_outcome(signed)
        assert claim is not None

        with pytest.raises(CronJobGovernanceError, match="signed run is active"):
            remove_job(signed["id"])

        persisted = load_jobs()[0]
        assert persisted["active_run_outcome_claim"] == claim
        assert claim_dispatch(signed["id"], run_outcome_claim=claim) is True
        assert load_jobs()[0]["repeat"]["completed"] == 1

    def test_auto_repeat_for_once(self, tmp_cron_dir):
        job = create_job(prompt="One-shot", schedule="1h")
        assert job["repeat"]["times"] == 1

    def test_rejects_stale_past_one_shot_at_creation(self, tmp_cron_dir, monkeypatch):
        now = datetime(2026, 3, 18, 4, 30, 0, tzinfo=timezone.utc)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)
        stale = (now - timedelta(minutes=5)).isoformat()

        with pytest.raises(ValueError, match="past and cannot be scheduled"):
            create_job(prompt="Too late", schedule=stale)

        assert load_jobs() == []

    def test_recent_past_one_shot_within_grace_still_creates(self, tmp_cron_dir, monkeypatch):
        now = datetime(2026, 3, 18, 4, 30, 30, tzinfo=timezone.utc)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)
        recent = (now - timedelta(seconds=30)).isoformat()

        job = create_job(prompt="Still valid", schedule=recent)

        assert job["next_run_at"] == recent
        assert load_jobs()[0]["id"] == job["id"]

    def test_interval_no_auto_repeat(self, tmp_cron_dir):
        job = create_job(prompt="Recurring", schedule="every 1h")
        assert job["repeat"]["times"] is None

    def test_default_delivery_origin(self, tmp_cron_dir):
        job = create_job(
            prompt="Test", schedule="30m",
            origin={"platform": "telegram", "chat_id": "123"},
        )
        assert job["deliver"] == "origin"

    def test_default_delivery_local_no_origin(self, tmp_cron_dir):
        job = create_job(prompt="Test", schedule="30m")
        assert job["deliver"] == "local"


class TestUpdateJob:
    def test_update_name(self, tmp_cron_dir):
        job = create_job(prompt="Check server status", schedule="every 1h", name="Old Name")
        assert job["name"] == "Old Name"
        updated = update_job(job["id"], {"name": "New Name"})
        assert updated is not None
        assert isinstance(updated, dict)
        assert updated["name"] == "New Name"
        # Verify other fields are preserved
        assert updated["prompt"] == "Check server status"
        assert updated["id"] == job["id"]
        assert updated["schedule"] == job["schedule"]
        # Verify persisted to disk
        fetched = get_job(job["id"])
        assert fetched["name"] == "New Name"

    def test_update_schedule(self, tmp_cron_dir):
        job = create_job(prompt="Daily report", schedule="every 1h")
        assert job["schedule"]["kind"] == "interval"
        assert job["schedule"]["minutes"] == 60
        old_next_run = job["next_run_at"]
        new_schedule = parse_schedule("every 2h")
        updated = update_job(job["id"], {"schedule": new_schedule, "schedule_display": new_schedule["display"]})
        assert updated is not None
        assert updated["schedule"]["kind"] == "interval"
        assert updated["schedule"]["minutes"] == 120
        assert updated["schedule_display"] == "every 120m"
        assert updated["next_run_at"] != old_next_run
        # Verify persisted to disk
        fetched = get_job(job["id"])
        assert fetched["schedule"]["minutes"] == 120
        assert fetched["schedule_display"] == "every 120m"

    def test_update_to_past_oneshot_rejected(self, tmp_cron_dir, monkeypatch):
        """Updating a job's schedule to a one-shot >ONESHOT_GRACE_SECONDS in the
        past must raise ValueError — otherwise the ghost-job bug (#59395) re-enters
        through the update door (next_run_at=None stored with state='scheduled').
        The original job must be left unchanged on disk."""
        now = datetime(2026, 7, 6, 12, 0, 0, tzinfo=timezone.utc)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)
        job = create_job(prompt="Recurring", schedule="every 1h", deliver="local")
        past = parse_schedule((now - timedelta(minutes=10)).isoformat())
        with pytest.raises(ValueError, match="past and cannot be scheduled"):
            update_job(job["id"], {"schedule": past})
        # Original job unchanged — still the recurring interval, still scheduled.
        fetched = get_job(job["id"])
        assert fetched["schedule"]["kind"] == "interval"
        assert fetched["next_run_at"] is not None

    def test_update_to_future_oneshot_accepted(self, tmp_cron_dir, monkeypatch):
        """Updating to a FUTURE one-shot still works — only past ones are rejected."""
        now = datetime(2026, 7, 6, 12, 0, 0, tzinfo=timezone.utc)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)
        job = create_job(prompt="Recurring", schedule="every 1h", deliver="local")
        future = parse_schedule((now + timedelta(hours=2)).isoformat())
        updated = update_job(job["id"], {"schedule": future})
        assert updated is not None
        assert updated["schedule"]["kind"] == "once"
        assert updated["next_run_at"] is not None

    def test_update_enable_disable(self, tmp_cron_dir):
        job = create_job(prompt="Toggle me", schedule="every 1h")
        assert job["enabled"] is True
        updated = update_job(job["id"], {"enabled": False})
        assert updated["enabled"] is False
        fetched = get_job(job["id"])
        assert fetched["enabled"] is False

    def test_update_nonexistent_returns_none(self, tmp_cron_dir):
        result = update_job("nonexistent_id", {"name": "X"})
        assert result is None

    def test_update_rejects_id_change(self, tmp_cron_dir):
        """Job IDs are filesystem path components — must be immutable."""
        job = create_job(prompt="Original", schedule="every 1h")

        with pytest.raises(ValueError, match="id"):
            update_job(job["id"], {"id": "../escape"})

        # Original job still resolvable, no rename happened.
        assert get_job(job["id"]) is not None
        assert get_job("../escape") is None


class TestPauseResumeJob:
    def test_pause_sets_state(self, tmp_cron_dir):
        job = create_job(prompt="Pause me", schedule="every 1h")
        paused = pause_job(job["id"], reason="user paused")
        assert paused is not None
        assert paused["enabled"] is False
        assert paused["state"] == "paused"
        assert paused["paused_reason"] == "user paused"

    def test_resume_reenables_job(self, tmp_cron_dir):
        job = create_job(prompt="Resume me", schedule="every 1h")
        pause_job(job["id"], reason="user paused")
        resumed = resume_job(job["id"])
        assert resumed is not None
        assert resumed["enabled"] is True
        assert resumed["state"] == "scheduled"
        assert resumed["paused_at"] is None
        assert resumed["paused_reason"] is None

    def test_resume_preserves_legacy_null_skill_shape(self, tmp_cron_dir):
        """A runtime-only resume must not rewrite the stored skill pair.

        Jobs authorized before multi-skill normalization can carry
        ``skill: null`` plus a populated ``skills`` list.  Unconditional
        ``_apply_skill_fields`` during pause/resume/trigger re-derived
        ``skill = skills[0]``, which changed the governed material hash and
        made an already-authorized runtime transition fail closed as
        candidate_hash_mismatch.  Runtime transitions must leave definition
        fields byte-for-byte intact.
        """
        job = create_job(prompt="legacy skills", schedule="every 1h")
        legacy = {
            **job,
            "skill": None,
            "skills": ["work/yuange-learning-retrospective", "work/yuange-knowledge-recall"],
            "enabled": False,
            "state": "paused",
            "paused_reason": "test",
        }
        save_jobs([legacy])
        stored_before = load_jobs()[0]
        definition_before = {
            key: value
            for key, value in stored_before.items()
            if key not in {"enabled", "state", "paused_at", "paused_reason", "next_run_at"}
        }

        resumed = resume_job(job["id"])
        assert resumed is not None
        assert resumed["enabled"] is True
        assert resumed["state"] == "scheduled"

        stored = load_jobs()[0]
        assert stored["skill"] is None
        assert stored["skills"] == legacy["skills"]
        definition_after = {
            key: value
            for key, value in stored.items()
            if key not in {"enabled", "state", "paused_at", "paused_reason", "next_run_at"}
        }
        assert definition_after == definition_before

    def test_resume_rejects_past_oneshot(self, tmp_cron_dir, monkeypatch):
        """Resuming a paused one-shot whose time is now in the past must raise
        ValueError — the revived job would silently never fire."""
        now = datetime(2026, 7, 6, 12, 0, 0, tzinfo=timezone.utc)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)
        # Create directly — bypass create_job's past-oneshot guard so we can
        # test the resume path independently.
        job = {
            "id": "test-resume-past",
            "name": "test-resume-past",
            "prompt": "Past one-shot",
            "schedule": {"kind": "once", "run_at": (now - timedelta(minutes=5)).isoformat(), "display": "once"},
            "repeat": {"times": 1, "completed": 0},
            "enabled": False,
            "state": "paused",
            "paused_at": now.isoformat(),
            "paused_reason": "test",
            "next_run_at": None,
            "last_run_at": None,
            "last_status": None,
            "last_error": None,
            "last_delivery_error": None,
            "created_at": (now - timedelta(hours=1)).isoformat(),
            "deliver": "local",
        }
        save_jobs([job])
        with pytest.raises(ValueError, match="in the past"):
            resume_job("test-resume-past")


class TestResolveJobRef:
    """Name-based job lookup for CLI/tool callers (PR #2627, @buntingszn)."""

    def test_resolve_by_exact_id(self, tmp_cron_dir):
        from cron.jobs import resolve_job_ref

        job = create_job(prompt="A", schedule="1h", name="alpha")
        assert resolve_job_ref(job["id"])["id"] == job["id"]

    def test_resolve_by_name(self, tmp_cron_dir):
        from cron.jobs import resolve_job_ref

        job = create_job(prompt="A", schedule="1h", name="alpha")
        assert resolve_job_ref("alpha")["id"] == job["id"]

    def test_resolve_by_name_case_insensitive(self, tmp_cron_dir):
        from cron.jobs import resolve_job_ref

        job = create_job(prompt="A", schedule="1h", name="MyJob")
        assert resolve_job_ref("myjob")["id"] == job["id"]
        assert resolve_job_ref("MYJOB")["id"] == job["id"]

    def test_resolve_returns_none_when_not_found(self, tmp_cron_dir):
        from cron.jobs import resolve_job_ref

        create_job(prompt="A", schedule="1h", name="alpha")
        assert resolve_job_ref("does-not-exist") is None
        assert resolve_job_ref("") is None

    def test_resolve_id_wins_over_name(self, tmp_cron_dir):
        """If a job's name happens to equal another job's ID, ID match wins."""
        from cron.jobs import resolve_job_ref

        j1 = create_job(prompt="A", schedule="1h")
        # Create a second job whose name is j1's ID
        j2 = create_job(prompt="B", schedule="1h", name=j1["id"])
        # Looking up j1["id"] must return j1, not the colliding-name job j2
        assert resolve_job_ref(j1["id"])["id"] == j1["id"]
        assert resolve_job_ref(j1["id"])["id"] != j2["id"]

    def test_resolve_ambiguous_name_raises(self, tmp_cron_dir):
        """Two jobs sharing a name → refuse to pick, surface both IDs."""
        from cron.jobs import AmbiguousJobReference, resolve_job_ref

        j1 = create_job(prompt="A", schedule="1h", name="dup")
        j2 = create_job(prompt="B", schedule="1h", name="dup")
        with pytest.raises(AmbiguousJobReference) as exc_info:
            resolve_job_ref("dup")
        ids = {m["id"] for m in exc_info.value.matches}
        assert ids == {j1["id"], j2["id"]}
        # Error message mentions both IDs so the user can pick one
        assert j1["id"] in str(exc_info.value)
        assert j2["id"] in str(exc_info.value)

    def test_trigger_by_name(self, tmp_cron_dir):
        from cron.jobs import trigger_job

        job = create_job(prompt="A", schedule="1h", name="alpha")
        result = trigger_job("alpha")
        assert result is not None
        assert result["id"] == job["id"]

    def test_pause_by_name(self, tmp_cron_dir):
        job = create_job(prompt="A", schedule="1h", name="alpha")
        result = pause_job("alpha", reason="manual")
        assert result is not None
        assert result["id"] == job["id"]
        assert result["state"] == "paused"

    def test_remove_by_name(self, tmp_cron_dir):
        job = create_job(prompt="A", schedule="1h", name="alpha")
        assert remove_job("alpha") is True
        assert get_job(job["id"]) is None

    def test_mutations_refuse_ambiguous_name(self, tmp_cron_dir):
        """pause/resume/trigger/remove must refuse to act on an ambiguous name."""
        from cron.jobs import AmbiguousJobReference, trigger_job

        create_job(prompt="A", schedule="1h", name="dup")
        create_job(prompt="B", schedule="1h", name="dup")
        for fn in (pause_job, resume_job, trigger_job):
            with pytest.raises(AmbiguousJobReference):
                fn("dup")
        with pytest.raises(AmbiguousJobReference):
            remove_job("dup")


class TestMarkJobRun:
    def test_increments_completed(self, tmp_cron_dir):
        job = create_job(prompt="Test", schedule="every 1h")
        mark_job_run(job["id"], success=True)
        updated = get_job(job["id"])
        assert updated["repeat"]["completed"] == 1
        assert updated["last_status"] == "ok"

    def test_repeat_limit_removes_job(self, tmp_cron_dir):
        job = create_job(prompt="Once", schedule="30m", repeat=1)
        mark_job_run(job["id"], success=True)
        # Job should be removed after hitting repeat limit
        assert get_job(job["id"]) is None

    def test_repeat_negative_one_is_infinite(self, tmp_cron_dir):
        # LLMs often pass repeat=-1 to mean "infinite/forever".
        # The job must NOT be deleted after runs when repeat <= 0.
        job = create_job(prompt="Forever", schedule="every 1h", repeat=-1)
        # -1 should be normalised to None (infinite) at create time
        assert job["repeat"]["times"] is None
        # Running it multiple times should never delete it
        for _ in range(3):
            mark_job_run(job["id"], success=True)
            assert get_job(job["id"]) is not None, "job was deleted after run despite infinite repeat"

    def test_repeat_zero_is_infinite(self, tmp_cron_dir):
        # repeat=0 should also be treated as None (infinite), not "run zero times".
        job = create_job(prompt="ZeroRepeat", schedule="every 1h", repeat=0)
        assert job["repeat"]["times"] is None
        mark_job_run(job["id"], success=True)
        assert get_job(job["id"]) is not None

    def test_error_status(self, tmp_cron_dir):
        job = create_job(prompt="Fail", schedule="every 1h")
        mark_job_run(job["id"], success=False, error="timeout")
        updated = get_job(job["id"])
        assert updated["last_status"] == "error"
        assert updated["last_error"] == "timeout"

    def test_delivery_error_tracked_separately(self, tmp_cron_dir):
        """Agent succeeds but delivery fails — both tracked independently."""
        job = create_job(prompt="Report", schedule="every 1h")
        mark_job_run(job["id"], success=True, delivery_error="platform 'telegram' not configured")
        updated = get_job(job["id"])
        assert updated["last_status"] == "ok"
        assert updated["last_error"] is None
        assert updated["last_delivery_error"] == "platform 'telegram' not configured"

    def test_delivery_error_cleared_on_success(self, tmp_cron_dir):
        """Successful delivery clears the previous delivery error."""
        job = create_job(prompt="Report", schedule="every 1h")
        mark_job_run(job["id"], success=True, delivery_error="network timeout")
        updated = get_job(job["id"])
        assert updated["last_delivery_error"] == "network timeout"
        # Next run delivers successfully
        mark_job_run(job["id"], success=True, delivery_error=None)
        updated = get_job(job["id"])
        assert updated["last_delivery_error"] is None

    def test_delivery_recovery_clears_only_delivery_health(self, tmp_cron_dir):
        job = create_job(prompt="Report", schedule="every 1h")
        mark_job_run(job["id"], success=True, delivery_error="transient audit failure")

        assert mark_job_delivery_recovered(job["id"]) is True
        updated = get_job(job["id"])
        assert updated["last_status"] == "ok"
        assert updated["last_delivery_error"] is None
        assert updated["last_delivery_recovered_at"]

    def test_delivery_receipt_persists_only_aggregate_terminal_facts(self, tmp_cron_dir):
        job = create_job(prompt="Report", schedule="every 1h")
        receipt = {
            "schema_version": "cron-delivery/v1",
            "status": "partial",
            "required_count": 2,
            "confirmed_count": 1,
            "failed_count": 1,
            "unconfirmed_count": 0,
            "receipts_truncated": False,
        }

        mark_job_run(job["id"], success=True, delivery_receipt=receipt)

        updated = get_job(job["id"])
        assert updated["last_delivery_receipt"] == receipt
        serialized = json.dumps(updated["last_delivery_receipt"])
        assert "message_id" not in serialized
        assert "chat_id" not in serialized
        assert "outbox_id" not in serialized

    def test_delivery_receipt_rejects_transport_identifiers(self, tmp_cron_dir):
        job = create_job(prompt="Report", schedule="every 1h")
        unsafe_receipt = {
            "schema_version": "cron-delivery/v1",
            "status": "success",
            "required_count": 1,
            "confirmed_count": 1,
            "failed_count": 0,
            "unconfirmed_count": 0,
            "receipts_truncated": False,
            "message_id": "must-not-persist",
        }

        with pytest.raises(ValueError, match="invalid cron delivery receipt"):
            mark_job_run(job["id"], success=True, delivery_receipt=unsafe_receipt)

        assert get_job(job["id"])["last_delivery_receipt"] is None

    def test_run_outcome_receipt_persists_revision_bound_terminal_fact(self, tmp_cron_dir):
        job = create_job(prompt="Report", schedule="every 1h")
        job.update(_signed_job_revision(job["id"]))
        save_jobs([job])
        claim = begin_job_run_outcome(job)
        assert claim is not None
        receipt = _cron_run_outcome_receipt(
            job,
            success=True,
            run_outcome_claim=claim,
        )
        assert receipt is not None

        assert mark_job_run(
            job["id"],
            success=True,
            run_outcome_receipt=receipt,
            run_outcome_claim=claim,
        ) is True

        assert get_job(job["id"])["last_run_outcome_receipt"] == receipt
        next_claim = begin_job_run_outcome(job)
        assert next_claim is not None
        mark_job_run(
            job["id"],
            success=False,
            error="next run failed",
            run_outcome_claim=next_claim,
        )
        assert get_job(job["id"])["last_run_outcome_receipt"] is None

    def test_run_outcome_writer_enforces_claim_and_terminal_binding(self, tmp_cron_dir):
        job = create_job(prompt="Report", schedule="every 1h")
        job.update(_signed_job_revision(job["id"]))
        save_jobs([job])

        claim = begin_job_run_outcome(job)
        assert claim is not None
        delivery = {
            "schema_version": "cron-delivery/v1",
            "status": "success",
            "required_count": 1,
            "confirmed_count": 1,
            "failed_count": 0,
            "unconfirmed_count": 0,
            "receipts_truncated": False,
        }
        receipt = _cron_run_outcome_receipt(
            job,
            success=True,
            run_outcome_claim=claim,
            delivery_receipt=delivery,
        )
        assert receipt is not None
        assert mark_job_run(
            job["id"],
            success=True,
            delivery_receipt=delivery,
            run_outcome_receipt=receipt,
            run_outcome_claim=claim,
        ) is True

        claim = begin_job_run_outcome(job)
        assert claim is not None
        before = get_job(job["id"])
        assert mark_job_run(
            job["id"],
            success=False,
            error="claim-less writer must not win",
        ) is False
        assert get_job(job["id"]) == before

        with pytest.raises(ValueError, match="requires its pre-run claim"):
            mark_job_run(job["id"], success=True, run_outcome_receipt=receipt)

        assert mark_job_run(
            job["id"],
            success=False,
            error="failed before receipt construction",
            run_outcome_claim=claim,
        ) is True
        assert get_job(job["id"])["last_run_outcome_receipt"] is None

        claim = begin_job_run_outcome(job)
        receipt = _cron_run_outcome_receipt(
            job,
            success=True,
            run_outcome_claim=claim,
            delivery_receipt=delivery,
        )
        assert receipt is not None

        with pytest.raises(ValueError, match="does not match its claim"):
            mark_job_run(
                job["id"],
                success=True,
                run_outcome_receipt={**receipt, "profile_id": "other-profile"},
                run_outcome_claim=claim,
            )

        with pytest.raises(ValueError, match="does not match its claim"):
            mark_job_run(
                job["id"],
                success=True,
                delivery_receipt=None,
                run_outcome_receipt=receipt,
                run_outcome_claim=claim,
            )

        with pytest.raises(ValueError, match="does not match its claim"):
            mark_job_run(
                job["id"],
                success=False,
                run_outcome_receipt=receipt,
                run_outcome_claim=claim,
            )

        assert mark_job_run("missing-job", success=False) is False

    def test_run_outcome_receipt_rejects_unredacted_or_malformed_data(self, tmp_cron_dir):
        job = create_job(prompt="Report", schedule="every 1h")
        receipt = {
            "schema_version": "cron-run-outcome/v1",
            "profile_id": "profile-custom",
            "job_id": job["id"],
            "job_revision": "sha256:" + "1" * 64,
            "run_id": "cron-run:" + "2" * 32,
            "terminal_state": "success",
            "implementation_hash": "sha256:" + "3" * 64,
            "checkpoint_invariant_hash": "sha256:" + "4" * 64,
            "raw_output": "must-not-persist",
        }

        with pytest.raises(ValueError, match="invalid cron run outcome receipt"):
            mark_job_run(job["id"], success=True, run_outcome_receipt=receipt)

        assert get_job(job["id"])["last_run_outcome_receipt"] is None

    def test_run_outcome_builder_executes_only_claimed_script_snapshot(
        self, tmp_path, monkeypatch
    ):
        scripts = tmp_path / "scripts"
        scripts.mkdir()
        task = scripts / "task.py"
        task.write_text("import helper\nprint('task-v1')\n", encoding="utf-8")
        helper = scripts / "helper.py"
        helper.write_text("VALUE = 'v1'\n", encoding="utf-8")
        interpreter = tmp_path / "python-runtime"
        interpreter.write_bytes(b"runtime-v1")
        monkeypatch.setattr(
            "cron.jobs._cron_script_interpreter",
            lambda _path: interpreter,
        )
        job = {
            **_signed_job_revision(profile_id="任意-profile"),
            "no_agent": True,
            "script": "task.py",
            "workdir": str(scripts),
            "deterministic_no_agent_exception": True,
        }
        claim = _cron_run_outcome_claim(
            job,
            run_id="cron-run:" + "a" * 32,
            profile_home=tmp_path,
        )
        assert claim is not None
        matches, snapshot = _cron_run_script_snapshot(
            job,
            claim,
            profile_home=tmp_path,
        )
        assert matches is True
        assert snapshot["script_bytes"] == b"import helper\nprint('task-v1')\n"
        assert snapshot["interpreter_bytes"] == b"runtime-v1"
        assert any(
            item["root"] == "script_root"
            and item["path"] == "helper.py"
            and item["content"] == b"VALUE = 'v1'\n"
            and item["mode"] == 0o644
            for item in snapshot["support_files"]
        )
        with monkeypatch.context() as scoped:
            scoped.setitem(
                _cron_run_script_snapshot.__globals__,
                "_cron_script_artifact",
                lambda *_args: (
                    claim["script_artifact_hash"],
                    claim["interpreter_artifact_hash"],
                    b"captured",
                ),
            )
            scoped.setitem(
                _cron_run_script_snapshot.__globals__,
                "_cron_support_artifact",
                lambda *_args: (claim["support_artifact_hash"], []),
            )
            scoped.setitem(
                _cron_run_script_snapshot.__globals__,
                "_cron_script_interpreter",
                lambda _path: None,
            )
            assert _cron_run_script_snapshot(
                job,
                claim,
                profile_home=tmp_path,
            ) == (False, None)

        helper.write_text("VALUE = 'v2'\n", encoding="utf-8")
        assert _cron_run_script_snapshot(
            job,
            claim,
            profile_home=tmp_path,
        )[0] is False
        assert _cron_run_outcome_receipt(
            job,
            success=True,
            run_outcome_claim=claim,
            profile_home=tmp_path,
        ) is None
        helper.write_text("VALUE = 'v1'\n", encoding="utf-8")

        receipt = _cron_run_outcome_receipt(
            job,
            success=True,
            run_outcome_claim=claim,
            profile_home=tmp_path,
        )

        assert receipt is not None
        assert receipt["profile_id"] == "任意-profile"
        assert receipt["job_revision"] == "sha256:" + "1" * 64
        assert receipt["run_id"] == "cron-run:" + "a" * 32
        assert receipt["terminal_state"] == "success"
        assert receipt["implementation_hash"].startswith("sha256:")
        assert receipt["checkpoint_invariant_hash"].startswith("sha256:")
        assert receipt["delivery_receipt_hash"] == _cron_delivery_receipt_hash(
            claim["run_id"],
            None,
        )

        task.write_text("import helper\nprint('task-v2')\n", encoding="utf-8")
        assert _cron_run_script_snapshot(
            job,
            claim,
            profile_home=tmp_path,
        ) == (False, None)
        assert _cron_run_outcome_receipt(
            job,
            success=True,
            run_outcome_claim=claim,
            profile_home=tmp_path,
        ) is None

        task.write_text("import helper\nprint('task-v1')\n", encoding="utf-8")
        interpreter.write_bytes(b"runtime-v2")
        changed_claim = _cron_run_outcome_claim(
            job,
            run_id=claim["run_id"],
            profile_home=tmp_path,
        )
        assert changed_claim["implementation_hash"] != claim["implementation_hash"]
        assert (
            changed_claim["interpreter_artifact_hash"]
            != claim["interpreter_artifact_hash"]
        )
        assert _cron_run_outcome_receipt(
            job,
            success=True,
            run_outcome_claim=claim,
            profile_home=tmp_path,
        ) is None

    def test_run_executes_captured_helper_and_interpreter_bytes(
        self, tmp_path, monkeypatch
    ):
        from cron import scheduler

        profile = tmp_path / "profile"
        scripts = profile / "scripts"
        scripts.mkdir(parents=True)
        task = scripts / "task.py"
        helper = scripts / "helper.py"
        task.write_text("import helper\nprint(helper.VALUE)\n", encoding="utf-8")
        helper.write_text("VALUE = 'claimed'\n", encoding="utf-8")
        job = {
            **_signed_job_revision(profile_id="profile-custom"),
            "no_agent": True,
            "script": "task.py",
            "deterministic_no_agent_exception": True,
        }
        claim = _cron_run_outcome_claim(
            job,
            run_id="cron-run:" + "c" * 32,
            profile_home=profile,
        )
        assert claim is not None
        matches, snapshot = _cron_run_script_snapshot(
            job,
            claim,
            profile_home=profile,
        )
        assert matches is True
        assert snapshot is not None

        helper.write_text("VALUE = 'mutated-during-run'\n", encoding="utf-8")
        monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: profile)
        success, output = scheduler._run_job_script(
            "task.py",
            script_snapshot=snapshot,
        )
        assert success is True, output
        assert output == "claimed"

        helper.write_text("VALUE = 'claimed'\n", encoding="utf-8")
        assert _cron_run_outcome_receipt(
            job,
            success=True,
            run_outcome_claim=claim,
            profile_home=profile,
        ) is not None

    def test_run_executes_captured_extensionless_helper_mode(
        self, tmp_path, monkeypatch
    ):
        from cron import scheduler

        profile = tmp_path / "profile"
        scripts = profile / "scripts"
        scripts.mkdir(parents=True)
        task = scripts / "task.sh"
        helper = scripts / "helper"
        task.write_text("./helper\n", encoding="utf-8")
        helper.write_text("#!/bin/sh\nprintf claimed\n", encoding="utf-8")
        helper.chmod(0o755)
        job = {
            **_signed_job_revision(profile_id="profile-custom"),
            "no_agent": True,
            "script": "task.sh",
            "deterministic_no_agent_exception": True,
        }
        claim = _cron_run_outcome_claim(
            job,
            run_id="cron-run:" + "d" * 32,
            profile_home=profile,
        )
        assert claim is not None
        matches, snapshot = _cron_run_script_snapshot(
            job,
            claim,
            profile_home=profile,
        )
        assert matches is True
        assert snapshot is not None

        helper.write_text("#!/bin/sh\nprintf live\n", encoding="utf-8")
        helper.chmod(0o644)
        monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: profile)
        success, output = scheduler._run_job_script(
            "task.sh",
            script_snapshot=snapshot,
        )

        assert success is True, output
        assert output == "claimed"

    @pytest.mark.parametrize(
        "declaration",
        [
            {"verification_command": "python verify.py"},
            {"verification_command_mode": "read_only"},
            {
                "verification_command_mode": "read_only",
                "verification_command": "python verify.py",
            },
        ],
    )
    def test_declared_verification_fails_closed_without_os_sandbox(
        self, tmp_path, declaration
    ):
        scripts = tmp_path / "scripts"
        scripts.mkdir()
        (scripts / "task.py").write_text("print('task')\n", encoding="utf-8")
        (tmp_path / "verify.py").write_text("print('{}')\n", encoding="utf-8")
        job = {
            **_signed_job_revision(),
            "no_agent": True,
            "script": "task.py",
            "workdir": str(tmp_path),
            "deterministic_no_agent_exception": True,
            **declaration,
        }
        assert _cron_run_outcome_claim(job, profile_home=tmp_path) is None

    def test_declared_checkpoint_policy_stays_unproven_until_runtime_observation_exists(self):
        job = {
            **_signed_job_revision(),
            "checkpoint_policy": {"mode": "phase_commit", "required": True},
        }
        claim = _cron_run_outcome_claim(job)
        assert claim is not None

        assert _cron_run_outcome_receipt(
            job,
            success=True,
            run_outcome_claim=claim,
        ) is None
        failed = _cron_run_outcome_receipt(
            job,
            success=False,
            run_outcome_claim=claim,
        )
        assert failed is not None
        assert failed["terminal_state"] == "failed"

    def test_runtime_budget_is_implementation_material_not_checkpoint_policy(self):
        job = {
            **_signed_job_revision(),
            "max_turns": 12,
            "run_timeout_seconds": 300,
        }
        claim = _cron_run_outcome_claim(job)
        assert claim is not None
        receipt = _cron_run_outcome_receipt(
            job,
            success=True,
            run_outcome_claim=claim,
        )
        assert receipt is not None

        changed = {**job, "run_timeout_seconds": 301}
        assert _cron_run_outcome_receipt(
            changed,
            success=True,
            run_outcome_claim=claim,
        ) is None

    @pytest.mark.parametrize(
        "mutation",
        [
            lambda job: job.pop("creation_governance_receipt"),
            lambda job: job["creation_governance_receipt"].update(schema_version="legacy"),
            lambda job: job["creation_governance_receipt"].update(cron_job_id="other"),
            lambda job: job["creation_governance_receipt"].update(profile_id=" bad "),
            lambda job: job["creation_governance_receipt"].update(profile_id="\ud800"),
            lambda job: job.update(id="bad\njob"),
            lambda job: job["creation_governance_receipt"].update(receipt_id="not-a-hash"),
        ],
    )
    def test_run_outcome_builder_refuses_unbound_revision(self, mutation):
        job = _signed_job_revision()
        mutation(job)
        assert _cron_run_outcome_claim(job) is None

    def test_run_outcome_builder_requires_boolean_terminal_state(self):
        job = _signed_job_revision()
        claim = _cron_run_outcome_claim(job)
        assert claim is not None
        with pytest.raises(ValueError, match="must be boolean"):
            _cron_run_outcome_receipt(job, success=1, run_outcome_claim=claim)

    @pytest.mark.parametrize(
        "field,value",
        [
            ("schema_version", "legacy"),
            ("profile_id", " bad "),
            ("job_id", "bad\njob"),
            ("run_id", "run-opaque"),
            ("terminal_state", "unknown"),
            ("job_revision", "not-a-hash"),
            ("implementation_hash", "not-a-hash"),
            ("checkpoint_invariant_hash", "not-a-hash"),
            ("delivery_receipt_hash", "not-a-hash"),
        ],
    )
    def test_run_outcome_validator_rejects_invalid_fields(self, field, value):
        job = _signed_job_revision()
        claim = _cron_run_outcome_claim(
            job,
            run_id="cron-run:" + "a" * 32,
        )
        assert claim is not None
        receipt = _cron_run_outcome_receipt(
            job,
            success=True,
            run_outcome_claim=claim,
        )
        assert receipt is not None
        receipt[field] = value
        with pytest.raises(ValueError, match="invalid cron run outcome receipt"):
            _validated_run_outcome_receipt(receipt)

    def test_run_outcome_validator_rejects_unversioned_extra_fields(self):
        job = _signed_job_revision()
        claim = _cron_run_outcome_claim(
            job,
            run_id="cron-run:" + "a" * 32,
        )
        assert claim is not None
        receipt = _cron_run_outcome_receipt(
            job,
            success=True,
            run_outcome_claim=claim,
        )
        assert receipt is not None
        receipt["verification_command"] = {"mode": "read_only"}
        with pytest.raises(ValueError, match="invalid cron run outcome receipt"):
            _validated_run_outcome_receipt(receipt)

    def test_begin_clears_stale_success_and_rejects_concurrent_attempt(self, tmp_cron_dir):
        job = create_job(prompt="Report", schedule="every 1h")
        job.update(_signed_job_revision(job["id"]))
        job["last_run_outcome_receipt"] = {"stale": True}
        save_jobs([job])

        first = begin_job_run_outcome(job)
        assert first is not None
        assert get_job(job["id"])["last_run_outcome_receipt"] is None
        second = begin_job_run_outcome(job)
        assert second is None
        stale = {**first, "run_id": "cron-run:" + "9" * 32}
        assert mark_job_run(
            job["id"],
            success=False,
            run_outcome_claim=stale,
        ) is False
        first_receipt = _cron_run_outcome_receipt(
            job,
            success=True,
            run_outcome_claim=first,
        )
        assert first_receipt is not None

        assert mark_job_run(
            job["id"],
            success=True,
            run_outcome_receipt=first_receipt,
            run_outcome_claim=first,
        ) is True
        current = get_job(job["id"])
        assert current["active_run_outcome_claim"] is None
        assert current["last_run_outcome_receipt"] == first_receipt

    def test_begin_recovers_expired_claim_and_rejects_old_owner(
        self, tmp_cron_dir, monkeypatch
    ):
        job = create_job(prompt="Report", schedule="every 1h")
        job.update(_signed_job_revision(job["id"]))
        save_jobs([job])

        monkeypatch.setattr("cron.jobs.time.time", lambda: 1_000_000)
        first = begin_job_run_outcome(job)
        assert first is not None
        assert begin_job_run_outcome(get_job(job["id"])) is None

        original_expiry = first["claim_expires_at_epoch"]
        monkeypatch.setattr("cron.jobs.time.time", lambda: original_expiry - 60)
        refreshed = heartbeat_job_run_outcome(job["id"], first)
        assert refreshed is not None
        assert refreshed["claim_expires_at_epoch"] > original_expiry
        assert heartbeat_job_run_outcome("missing-job", refreshed) is None
        mismatched = {
            **refreshed,
            "run_id": "cron-run:" + "8" * 32,
        }
        assert heartbeat_job_run_outcome(job["id"], mismatched) is None

        monkeypatch.setattr("cron.jobs.time.time", lambda: original_expiry + 10)
        assert begin_job_run_outcome(get_job(job["id"])) is None

        monkeypatch.setattr(
            "cron.jobs.time.time",
            lambda: refreshed["claim_expires_at_epoch"],
        )
        assert heartbeat_job_run_outcome(job["id"], refreshed) is None
        assert mark_job_run(
            job["id"],
            success=False,
            run_outcome_claim=refreshed,
        ) is False
        replacement = begin_job_run_outcome(get_job(job["id"]))
        assert replacement is not None
        assert replacement["run_id"] != first["run_id"]
        assert get_job(job["id"])["active_run_outcome_claim"] == replacement
        assert mark_job_run(
            job["id"],
            success=False,
            run_outcome_claim=refreshed,
        ) is False

        receipt = _cron_run_outcome_receipt(
            job,
            success=True,
            run_outcome_claim=replacement,
        )
        assert receipt is not None
        assert mark_job_run(
            job["id"],
            success=True,
            run_outcome_receipt=receipt,
            run_outcome_claim=replacement,
        ) is True

    def test_begin_does_not_replace_malformed_persisted_claim(self, tmp_cron_dir):
        job = create_job(prompt="Report", schedule="every 1h")
        job.update(_signed_job_revision(job["id"]))
        malformed = {"schema_version": "cron-run-claim/v1"}
        job["active_run_outcome_claim"] = malformed
        save_jobs([job])

        assert begin_job_run_outcome(job) is None
        assert get_job(job["id"])["active_run_outcome_claim"] == malformed

    def test_claim_validator_rejects_surrogate_without_throwing_from_builder(self):
        job = _signed_job_revision(profile_id="\ud800")
        assert _cron_run_outcome_claim(job) is None

        valid = _cron_run_outcome_claim(_signed_job_revision())
        assert valid is not None
        valid["profile_id"] = "\ud800"
        with pytest.raises(ValueError, match="invalid cron run outcome"):
            _validated_run_outcome_claim(valid)

    def test_run_artifact_and_script_hashes_fail_closed(self, tmp_path, monkeypatch):
        profile = tmp_path / "profile"
        scripts = profile / "scripts"
        scripts.mkdir(parents=True)
        outside = tmp_path / "outside.py"
        outside.write_text("print('outside')\n", encoding="utf-8")
        script = scripts / "task.py"
        script.write_text("print('ok')\n", encoding="utf-8")

        assert _cron_run_artifact_hash(scripts) is None
        assert _cron_run_artifact_hash(tmp_path / "missing.py") is None
        with monkeypatch.context() as scoped:
            original_read = type(script).read_bytes

            def oversized_read(path):
                if path.resolve(strict=False) == script.resolve(strict=False):
                    return b"x" * 1001
                return original_read(path)

            scoped.setattr(type(script), "read_bytes", oversized_read)
            scoped.setattr("cron.jobs._CRON_RUN_ARTIFACT_MAX_BYTES", 1000)
            assert _cron_run_artifact_hash(script) is None
        monkeypatch.setattr("cron.jobs._CRON_RUN_ARTIFACT_MAX_BYTES", 0)
        assert _cron_run_artifact_hash(script) is None
        monkeypatch.setattr("cron.jobs._CRON_RUN_ARTIFACT_MAX_BYTES", 8 * 1024 * 1024)
        assert _cron_script_artifact_hash({"script": str(outside)}, profile) is None
        assert _cron_script_artifact_hash({"script": "missing.py"}, profile) is None
        assert _cron_script_artifact_hash({"script": "task.py"}, profile).startswith("sha256:")
        shell = scripts / "task.sh"
        shell.write_text("printf ok\n", encoding="utf-8")
        assert _cron_script_artifact_hash({"script": "task.sh"}, profile).startswith(
            "sha256:"
        )

    def test_support_artifact_hash_fail_closed_edges(self, tmp_path, monkeypatch):
        profile = tmp_path / "profile"
        scripts = profile / "scripts"
        scripts.mkdir(parents=True)
        script = scripts / "task.py"
        script.write_text("print('ok')\n", encoding="utf-8")

        outside = tmp_path / "outside.py"
        outside.write_text("print('outside')\n", encoding="utf-8")
        assert _cron_support_artifact_hash({"script": str(outside)}, profile) is None

        same_root = {
            "script": "task.py",
            "workdir": str(scripts),
        }
        assert _cron_support_artifact_hash(same_root, profile).startswith("sha256:")

        ignored = scripts / "__pycache__"
        ignored.mkdir()
        (ignored / "cached.py").write_text("VALUE = 1\n", encoding="utf-8")
        (scripts / "lib").mkdir()
        assert _cron_support_artifact_hash(same_root, profile).startswith("sha256:")

        extensionless = scripts / "sourced-helper"
        extensionless.write_text("VALUE=one\n", encoding="utf-8")
        extensionless.chmod(0o644)
        before = _cron_support_artifact_hash(same_root, profile)
        extensionless.chmod(0o755)
        after_mode_change = _cron_support_artifact_hash(same_root, profile)
        assert after_mode_change != before
        extensionless.write_text("VALUE=two\n", encoding="utf-8")
        assert _cron_support_artifact_hash(same_root, profile) != after_mode_change

        external_root = tmp_path / "external-workdir"
        external_root.mkdir()
        (external_root / "helper.py").write_text("VALUE = 1\n", encoding="utf-8")
        linked_root = tmp_path / "linked-workdir"
        linked_root.symlink_to(external_root, target_is_directory=True)
        assert _cron_support_artifact_hash(
            {"script": "task.py", "workdir": str(linked_root)}, profile
        ) is None

        linked_file = scripts / "linked.py"
        linked_file.symlink_to(script)
        assert _cron_support_artifact_hash(same_root, profile) is None
        linked_file.unlink()

        monkeypatch.setattr("cron.jobs._cron_run_artifact", lambda _path: (None, None))
        assert _cron_support_artifact_hash(same_root, profile) is None

    def test_shell_interpreter_ignores_caller_path(self, tmp_path, monkeypatch):
        from cron import jobs

        caller_bin = tmp_path / "caller-bin"
        caller_bin.mkdir()
        caller_bash = caller_bin / "bash"
        caller_bash.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
        caller_bash.chmod(0o755)
        trusted_bash = tmp_path / "trusted-bash"
        trusted_bash.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        trusted_bash.chmod(0o755)
        monkeypatch.setenv("PATH", str(caller_bin))
        monkeypatch.setattr(
            jobs, "_CRON_TRUSTED_BASH_PATHS", (str(trusted_bash),)
        )

        assert jobs._cron_script_interpreter(Path("task.sh")) == trusted_bash

        monkeypatch.setattr(
            jobs, "_CRON_TRUSTED_BASH_PATHS", (str(tmp_path / "missing"),)
        )
        assert jobs._cron_script_interpreter(Path("task.sh")) is None

        monkeypatch.setattr(jobs.sys, "platform", "win32")
        assert jobs._cron_script_interpreter(Path("task.py")) is None

    def test_support_artifact_hash_enforces_tree_bounds(self, tmp_path, monkeypatch):
        profile = tmp_path / "profile"
        scripts = profile / "scripts"
        scripts.mkdir(parents=True)
        (scripts / "task.py").write_text("print('ok')\n", encoding="utf-8")
        job = {"script": "task.py", "workdir": str(scripts)}

        monkeypatch.setattr("cron.jobs._CRON_SUPPORT_ARTIFACT_MAX_FILES", 0)
        assert _cron_support_artifact_hash(job, profile) is None

        monkeypatch.setattr("cron.jobs._CRON_SUPPORT_ARTIFACT_MAX_FILES", 512)
        monkeypatch.setattr("cron.jobs._CRON_SUPPORT_ARTIFACT_MAX_BYTES", 0)
        assert _cron_support_artifact_hash(job, profile) is None

    def test_interpreter_hash_streams_beyond_script_snapshot_limit(
        self, tmp_path, monkeypatch
    ):
        interpreter = tmp_path / "python-runtime"
        interpreter.write_bytes(b"runtime-binary-v1")

        monkeypatch.setattr("cron.jobs._CRON_RUN_ARTIFACT_MAX_BYTES", 4)
        monkeypatch.setattr("cron.jobs._CRON_INTERPRETER_ARTIFACT_MAX_BYTES", 64)
        assert _cron_run_artifact_hash(interpreter) is None
        assert _cron_interpreter_artifact_hash(interpreter) == (
            "sha256:" + hashlib.sha256(interpreter.read_bytes()).hexdigest()
        )

        monkeypatch.setattr("cron.jobs._CRON_INTERPRETER_ARTIFACT_MAX_BYTES", 4)
        assert _cron_interpreter_artifact_hash(interpreter) is None
        assert _cron_interpreter_artifact_hash(tmp_path) is None
        assert _cron_interpreter_artifact_hash(tmp_path / "missing-python") is None

    def test_claim_and_receipt_fail_closed_edges(self, tmp_path):
        invalid_script = {**_signed_job_revision(), "script": "missing.py"}
        assert _cron_run_outcome_claim(invalid_script, profile_home=tmp_path) is None
        assert _cron_run_outcome_receipt(
            _signed_job_revision(), success=False, run_outcome_claim=None
        ) is None

        claim = _cron_run_outcome_claim(_signed_job_revision())
        assert claim is not None
        with pytest.raises(ValueError, match="invalid cron run outcome claim"):
            _validated_run_outcome_claim({**claim, "extra": True})
        with pytest.raises(ValueError, match="invalid cron run outcome claim"):
            _validated_run_outcome_claim({**claim, "schema_version": "legacy"})

        malformed_claim = dict(claim)
        malformed_claim["script_artifact_hash"] = "not-a-hash"
        with pytest.raises(ValueError, match="invalid cron run outcome claim"):
            _validated_run_outcome_claim(malformed_claim)

        checkpoint_job = _signed_job_revision()
        checkpoint_claim = _cron_run_outcome_claim(checkpoint_job)
        assert checkpoint_claim is not None
        checkpoint_job["checkpoint_policy"] = {"mode": "changed-after-claim"}
        assert _cron_checkpoint_invariant_hash(
            checkpoint_job,
            checkpoint_claim,
            success=False,
        ) is None

    def test_begin_run_outcome_fail_closed_edges(self, tmp_cron_dir):
        original = create_job(prompt="Report", schedule="every 1h")
        signed = {**original, **_signed_job_revision(original["id"])}

        save_jobs([{**signed, "id": "other"}, signed])
        claim = begin_job_run_outcome(signed)
        assert claim is not None

        save_jobs([{**signed, "creation_governance_receipt": {**signed["creation_governance_receipt"], "receipt_id": "sha256:" + "9" * 64}}])
        assert begin_job_run_outcome(signed) is None

        invalid_script = {**signed, "script": "missing.py"}
        save_jobs([invalid_script])
        assert begin_job_run_outcome(invalid_script) is None
        persisted = get_job(invalid_script["id"])
        assert persisted["last_run_outcome_receipt"] is None
        assert persisted["active_run_outcome_claim"] is None

        save_jobs([])
        assert begin_job_run_outcome(signed) is None

    def test_abandon_run_outcome_clears_only_the_matching_active_claim(
        self, tmp_cron_dir
    ):
        original = create_job(prompt="Report", schedule="every 1h")
        signed = {**original, **_signed_job_revision(original["id"])}
        save_jobs([signed])
        first = begin_job_run_outcome(signed)
        assert first is not None
        stale = {**first, "run_id": "cron-run:" + "9" * 32}

        assert abandon_job_run_outcome(signed["id"], stale) is False
        assert abandon_job_run_outcome(
            signed["id"], first, run_claim=None, fire_claim=None
        ) is True
        persisted = get_job(signed["id"])
        assert persisted["active_run_outcome_claim"] is None
        assert persisted["last_run_outcome_receipt"] is None
        assert abandon_job_run_outcome("missing", first) is False

        save_jobs([signed])
        next_claim = begin_job_run_outcome(signed)
        assert next_claim is not None
        assert abandon_job_run_outcome(signed["id"], next_claim) is True

    def test_preflight_denial_releases_matching_scheduler_claims_without_run_count(
        self, tmp_cron_dir
    ):
        original = create_job(prompt="Report", schedule="every 1h")
        run_claim = {"at": "2026-07-22T00:00:00+00:00", "by": "scheduler-a"}
        fire_claim = {"at": "2026-07-22T00:00:00+00:00", "by": "scheduler-a"}
        signed = {
            **original,
            **_signed_job_revision(original["id"]),
            "run_claim": run_claim,
            "fire_claim": fire_claim,
        }
        save_jobs([signed])

        assert record_job_run_preflight_denial(
            signed["id"],
            job_revision=signed["creation_governance_receipt"]["receipt_id"],
            reason_code="unsupported_verification_contract",
            run_claim=run_claim,
            fire_claim=fire_claim,
        ) is True

        persisted = get_job(signed["id"])
        assert persisted["run_claim"] is None
        assert persisted["fire_claim"] is None
        assert persisted["repeat"]["completed"] == 0
        assert persisted["last_runtime_admission_receipt"]["reason_code"] == (
            "unsupported_verification_contract"
        )
        assert record_job_run_preflight_denial(
            "missing",
            job_revision=signed["creation_governance_receipt"]["receipt_id"],
            reason_code="missing",
        ) is False

        fire_only = {
            **signed,
            "run_claim": None,
            "fire_claim": fire_claim,
            "last_runtime_admission_receipt": None,
        }
        save_jobs([fire_only])
        assert record_job_run_preflight_denial(
            signed["id"],
            job_revision=signed["creation_governance_receipt"]["receipt_id"],
            reason_code="run_outcome_claim_unavailable",
            run_claim=None,
            fire_claim=fire_claim,
        ) is True
        assert get_job(signed["id"])["fire_claim"] is None

        no_scheduler_claims = {
            **signed,
            "run_claim": None,
            "fire_claim": None,
            "last_runtime_admission_receipt": None,
        }
        save_jobs([no_scheduler_claims])
        assert record_job_run_preflight_denial(
            signed["id"],
            job_revision=signed["creation_governance_receipt"]["receipt_id"],
            reason_code="run_outcome_claim_unavailable",
        ) is True

    def test_preflight_denial_and_abandon_are_claim_bound(self, tmp_cron_dir):
        original = create_job(prompt="Report", schedule="every 1h")
        run_claim = {"at": "2026-07-22T00:00:00+00:00", "by": "scheduler-a"}
        fire_claim = {"at": "2026-07-22T00:00:00+00:00", "by": "scheduler-a"}
        signed = {
            **original,
            **_signed_job_revision(original["id"]),
            "run_claim": run_claim,
            "fire_claim": fire_claim,
        }
        save_jobs([signed])
        claim = begin_job_run_outcome(signed)
        assert claim is not None

        assert record_job_run_preflight_denial(
            signed["id"],
            job_revision=claim["job_revision"],
            reason_code="must-not-overwrite-active",
            run_claim=run_claim,
        ) is False
        assert abandon_job_run_outcome(
            signed["id"],
            claim,
            run_claim={**run_claim, "by": "scheduler-b"},
        ) is False
        assert abandon_job_run_outcome(
            signed["id"],
            claim,
            reason_code="run_outcome_script_changed",
            run_claim=run_claim,
            fire_claim=fire_claim,
        ) is True
        persisted = get_job(signed["id"])
        assert persisted["run_claim"] is None
        assert persisted["fire_claim"] is None
        assert persisted["last_runtime_admission_receipt"]["reason_code"] == (
            "run_outcome_script_changed"
        )

    def test_both_agent_and_delivery_error(self, tmp_cron_dir):
        """Agent fails AND delivery fails — both errors recorded."""
        job = create_job(prompt="Report", schedule="every 1h")
        mark_job_run(job["id"], success=False, error="model timeout",
                     delivery_error="platform 'discord' not enabled")
        updated = get_job(job["id"])
        assert updated["last_status"] == "error"
        assert updated["last_error"] == "model timeout"
        assert updated["last_delivery_error"] == "platform 'discord' not enabled"

    def test_recurring_cron_not_disabled_when_croniter_missing(self, tmp_cron_dir, monkeypatch):
        """Regression test for issue #16265.

        If the gateway runs in an env where `croniter` went missing after a
        recurring cron job was persisted, `compute_next_run()` returns None.
        `mark_job_run()` must NOT treat that as terminal completion — the job
        has to stay enabled with state=error so the user notices, rather than
        silently flipping to enabled=false, state=completed.
        """
        pytest.importorskip("croniter")  # need it to create the job
        job = create_job(prompt="Recurring", schedule="0 7,15,23 * * *")
        assert job["schedule"]["kind"] == "cron"

        # Simulate the runtime env having lost croniter between job creation
        # and this run.
        monkeypatch.setattr("cron.jobs.HAS_CRONITER", False)

        mark_job_run(job["id"], success=True)

        updated = get_job(job["id"])
        assert updated is not None, "recurring cron job was deleted"
        assert updated["enabled"] is True, (
            "recurring cron job was disabled despite croniter-missing being "
            "a runtime dep issue, not a terminal completion"
        )
        assert updated["state"] == "error"
        assert updated["state"] != "completed"
        assert updated["next_run_at"] is None
        assert updated["last_error"]
        assert "croniter" in updated["last_error"].lower()

    def test_recurring_interval_not_disabled_when_next_run_is_none(self, tmp_cron_dir, monkeypatch):
        """Defensive sibling of the cron test — any recurring schedule that
        somehow yields next_run_at=None must stay enabled with state=error.
        """
        job = create_job(prompt="Recurring", schedule="every 1h")
        assert job["schedule"]["kind"] == "interval"

        # Force compute_next_run to return None for this call — simulates
        # any future regression where a recurring schedule loses its
        # next-run computation (missing dep, corrupt schedule, etc.).
        monkeypatch.setattr("cron.jobs.compute_next_run", lambda *a, **kw: None)

        mark_job_run(job["id"], success=True)

        updated = get_job(job["id"])
        assert updated is not None
        assert updated["enabled"] is True
        assert updated["state"] == "error"
        assert updated["state"] != "completed"

    def test_oneshot_still_completes_when_next_run_is_none(self, tmp_cron_dir):
        """One-shot jobs must still flip to enabled=false, state=completed
        when next_run_at cannot be computed — the #16265 fix must not
        regress this path. We bypass create_job and craft a minimal
        one-shot record directly so that the repeat-limit branch doesn't
        pop the job before we observe the terminal-completion branch.
        """
        jobs = [{
            "id": "oneshot-test",
            "prompt": "Once",
            "schedule": {"kind": "once", "run_at": "2020-01-01T00:00:00+00:00", "display": "once"},
            "repeat": {"times": None, "completed": 0},
            "enabled": True,
            "state": "scheduled",
            "next_run_at": "2020-01-01T00:00:00+00:00",
            "last_run_at": None,
            "last_status": None,
            "last_error": None,
            "last_delivery_error": None,
            "created_at": "2020-01-01T00:00:00+00:00",
        }]
        save_jobs(jobs)

        mark_job_run("oneshot-test", success=True)

        updated = get_job("oneshot-test")
        assert updated is not None
        assert updated["next_run_at"] is None
        assert updated["enabled"] is False
        assert updated["state"] == "completed"


class TestAdvanceNextRun:
    """Tests for advance_next_run() — crash-safety for recurring jobs."""

    def test_advances_interval_job(self, tmp_cron_dir):
        """Interval jobs should have next_run_at bumped to the next future occurrence."""
        job = create_job(prompt="Recurring check", schedule="every 1h")
        # Force next_run_at to 5 minutes ago (i.e. the job is due)
        jobs = load_jobs()
        old_next = (datetime.now() - timedelta(minutes=5)).isoformat()
        jobs[0]["next_run_at"] = old_next
        save_jobs(jobs)

        result = advance_next_run(job["id"])
        assert result is True

        updated = get_job(job["id"])
        from cron.jobs import _ensure_aware, _hermes_now
        new_next_dt = _ensure_aware(datetime.fromisoformat(updated["next_run_at"]))
        assert new_next_dt > _hermes_now(), "next_run_at should be in the future after advance"

    def test_advances_cron_job(self, tmp_cron_dir):
        """Cron-expression jobs should have next_run_at bumped to the next occurrence."""
        pytest.importorskip("croniter")
        job = create_job(prompt="Daily wakeup", schedule="15 6 * * *")
        # Force next_run_at to 30 minutes ago
        jobs = load_jobs()
        old_next = (datetime.now() - timedelta(minutes=30)).isoformat()
        jobs[0]["next_run_at"] = old_next
        save_jobs(jobs)

        result = advance_next_run(job["id"])
        assert result is True

        updated = get_job(job["id"])
        from cron.jobs import _ensure_aware, _hermes_now
        new_next_dt = _ensure_aware(datetime.fromisoformat(updated["next_run_at"]))
        assert new_next_dt > _hermes_now(), "next_run_at should be in the future after advance"

    def test_skips_oneshot_job(self, tmp_cron_dir):
        """One-shot jobs should NOT be advanced — they need to retry on restart."""
        job = create_job(prompt="Run once", schedule="30m")
        original_next = get_job(job["id"])["next_run_at"]

        result = advance_next_run(job["id"])
        assert result is False

        updated = get_job(job["id"])
        assert updated["next_run_at"] == original_next, "one-shot next_run_at should be unchanged"

    def test_nonexistent_job_returns_false(self, tmp_cron_dir):
        result = advance_next_run("nonexistent-id")
        assert result is False

    def test_already_future_stays_future(self, tmp_cron_dir):
        """If next_run_at is already in the future, advance keeps it in the future (no harm)."""
        job = create_job(prompt="Future job", schedule="every 1h")
        # next_run_at is already set to ~1h from now by create_job
        advance_next_run(job["id"])
        # Regardless of return value, the job should still be in the future
        updated = get_job(job["id"])
        from cron.jobs import _ensure_aware, _hermes_now
        new_next_dt = _ensure_aware(datetime.fromisoformat(updated["next_run_at"]))
        assert new_next_dt > _hermes_now(), "next_run_at should remain in the future"

    def test_crash_safety_scenario(self, tmp_cron_dir):
        """Simulate the crash-loop scenario: after advance, the job should NOT be due."""
        job = create_job(prompt="Crash test", schedule="every 1h")
        # Force next_run_at to 5 minutes ago (job is due)
        jobs = load_jobs()
        jobs[0]["next_run_at"] = (datetime.now() - timedelta(minutes=5)).isoformat()
        save_jobs(jobs)

        # Job should be due before advance
        due_before = get_due_jobs()
        assert len(due_before) == 1

        # Advance (simulating what tick() does before run_job)
        advance_next_run(job["id"])

        # Now the job should NOT be due (simulates restart after crash)
        due_after = get_due_jobs()
        assert len(due_after) == 0, "Job should not be due after advance_next_run"


class TestGetDueJobs:
    def test_past_due_within_window_returned(self, tmp_cron_dir):
        """Jobs within the dynamic grace window are still considered due (not stale).

        For an hourly job, grace = 30 min (half the period, clamped to [120s, 2h]).
        """
        job = create_job(prompt="Due now", schedule="every 1h")
        # Force next_run_at to 10 minutes ago (within the 30-min grace for hourly)
        jobs = load_jobs()
        jobs[0]["next_run_at"] = (datetime.now() - timedelta(minutes=10)).isoformat()
        save_jobs(jobs)

        due = get_due_jobs()
        assert len(due) == 1
        assert due[0]["id"] == job["id"]

    def test_active_run_outcome_claim_is_not_dispatched_again(self, tmp_cron_dir):
        job = create_job(prompt="Already running", schedule="every 1h")
        jobs = load_jobs()
        jobs[0]["next_run_at"] = (datetime.now() - timedelta(minutes=10)).isoformat()
        jobs[0]["active_run_outcome_claim"] = {"run_id": "cron-run:" + "1" * 32}
        save_jobs(jobs)

        assert get_due_jobs() == []
        assert get_job(job["id"])["active_run_outcome_claim"] is not None

    def test_expired_run_outcome_claim_is_due_after_restart(
        self, tmp_cron_dir, monkeypatch
    ):
        job = create_job(prompt="Recover after crash", schedule="every 1h")
        signed = _signed_job_revision(job["id"])
        job.update(signed)
        jobs = [job]
        jobs[0]["next_run_at"] = (datetime.now() - timedelta(minutes=10)).isoformat()
        monkeypatch.setattr("cron.jobs.time.time", lambda: 1_000_000)
        claim = _cron_run_outcome_claim(jobs[0])
        assert claim is not None
        jobs[0]["active_run_outcome_claim"] = claim
        save_jobs(jobs)

        assert get_due_jobs() == []
        monkeypatch.setattr(
            "cron.jobs.time.time",
            lambda: claim["claim_expires_at_epoch"],
        )
        due = get_due_jobs()
        assert [item["id"] for item in due] == [job["id"]]

    def test_stale_past_due_runs_once_and_fast_forwards(self, tmp_cron_dir):
        """Recurring jobs past their grace window run once now and fast-forward next_run_at.

        For an hourly job, grace = 30 min. Setting 35 min late exceeds the window.
        The job should be returned as due (execute once) with next_run_at in the future.
        """
        job = create_job(prompt="Stale", schedule="every 1h")
        # Force next_run_at to 35 minutes ago (beyond the 30-min grace for hourly)
        jobs = load_jobs()
        jobs[0]["next_run_at"] = (datetime.now() - timedelta(minutes=35)).isoformat()
        save_jobs(jobs)

        due = get_due_jobs()
        # Job is returned as due — execute once now instead of skipping
        assert len(due) == 1
        assert due[0]["id"] == job["id"]
        # next_run_at should be fast-forwarded to the future (accumulated slots skipped)
        updated = get_job(job["id"])
        from cron.jobs import _ensure_aware, _hermes_now
        next_dt = _ensure_aware(datetime.fromisoformat(updated["next_run_at"]))
        assert next_dt > _hermes_now()

    def test_idless_job_does_not_crash_or_block_sibling_jobs(self, tmp_cron_dir):
        """A job missing its 'id' key must not crash the tick or freeze siblings.

        Regression: jobs authored by a direct jobs.json edit (bypassing
        create_job) sometimes used the key 'job_id' instead of 'id'. The logging
        helpers evaluated ``job.get("name", job["id"])`` -- Python evaluates the
        default argument ``job["id"]`` eagerly, so an id-less job raised
        ``KeyError: 'id'`` mid-tick. That exception aborted
        ``_get_due_jobs_locked()`` BEFORE ``save_jobs()`` ran, so every healthy
        job's fast-forwarded next_run_at was computed in memory then discarded --
        the whole profile's scheduler froze in a per-minute loop.
        """
        healthy = create_job(prompt="Healthy", schedule="every 1h")

        jobs = load_jobs()
        # Push the healthy job beyond its grace window so the fast-forward path
        # (one of the id-less-crash sites) runs.
        jobs[0]["next_run_at"] = (datetime.now() - timedelta(minutes=35)).isoformat()
        # A malformed record: no 'id' key, mirroring the real corruption.
        jobs.append({
            "name": "idless-job",
            "schedule": {"kind": "cron", "expr": "0 4 * * *"},
            "enabled": True,
            "no_agent": True,
            "next_run_at": None,
        })
        save_jobs(jobs)

        # Must not raise KeyError.
        due = get_due_jobs()

        # The healthy sibling is still discovered despite the malformed neighbor.
        assert any(d.get("id") == healthy["id"] for d in due)


    def test_long_execution_does_not_perpetually_defer(self, tmp_cron_dir, monkeypatch):
        """#33315: a recurring job whose runtime exceeds interval+grace must still
        run once when the tick comes back, not skip forever.

        Reproduces the production loop: a 5-min interval job whose previous run
        overran the interval, leaving next_run_at ~11 min in the past — beyond
        the 150s grace for a 5m interval. The job must be returned as due (run
        once) AND have next_run_at fast-forwarded (so accumulated missed slots
        don't all fire)."""
        from cron.jobs import _ensure_aware, _hermes_now
        job = create_job(prompt="Long job", schedule="every 5m")
        jobs = load_jobs()
        # 11 minutes ago: > grace (150s for a 5m interval) — the "still running" miss.
        stale = (_hermes_now() - timedelta(minutes=11)).isoformat()
        jobs[0]["next_run_at"] = stale
        jobs[0]["last_run_at"] = (_hermes_now() - timedelta(minutes=1)).isoformat()
        save_jobs(jobs)

        due = get_due_jobs()
        assert [j["id"] for j in due] == [job["id"]], "long-execution job was skipped (perpetual-defer bug)"
        # next_run_at fast-forwarded into the future (no burst of missed slots).
        nxt = _ensure_aware(datetime.fromisoformat(get_job(job["id"])["next_run_at"]))
        assert nxt > _hermes_now()


    def test_stale_repeat_limited_job_consumes_one_run_on_catchup(self, tmp_cron_dir, monkeypatch):
        """#33315 behavior note: a stale recurring job with a repeat.times limit
        fires ONCE on catch-up and consumes one of its runs (it is no longer
        silently skipped). Pins the documented repeat-count interaction so it
        isn't changed accidentally."""
        from cron.jobs import _hermes_now
        job = create_job(prompt="Limited", schedule="every 5m", repeat=3)
        jobs = load_jobs()
        jobs[0]["next_run_at"] = (_hermes_now() - timedelta(minutes=11)).isoformat()
        jobs[0]["last_run_at"] = (_hermes_now() - timedelta(minutes=11)).isoformat()
        save_jobs(jobs)

        # The stale job is returned to fire once (not skipped).
        due = get_due_jobs()
        assert [j["id"] for j in due] == [job["id"]]
        # Simulate the run completing: mark_job_run increments completed.
        mark_job_run(job["id"], True)
        survived = get_job(job["id"])
        assert survived is not None, "job should survive (3 > 1 completed)"
        assert survived["repeat"]["completed"] == 1

    def test_future_not_returned(self, tmp_cron_dir):
        create_job(prompt="Not yet", schedule="every 1h")
        due = get_due_jobs()
        assert len(due) == 0

    def test_disabled_not_returned(self, tmp_cron_dir):
        job = create_job(prompt="Disabled", schedule="every 1h")
        jobs = load_jobs()
        jobs[0]["enabled"] = False
        jobs[0]["next_run_at"] = (datetime.now() - timedelta(minutes=5)).isoformat()
        save_jobs(jobs)

        due = get_due_jobs()
        assert len(due) == 0

    def test_broken_recent_one_shot_without_next_run_is_recovered(self, tmp_cron_dir, monkeypatch):
        now = datetime(2026, 3, 18, 4, 22, 30, tzinfo=timezone.utc)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)

        run_at = "2026-03-18T04:22:00+00:00"
        save_jobs(
            [{
                "id": "oneshot-recover",
                "name": "Recover me",
                "prompt": "Word of the day",
                "schedule": {"kind": "once", "run_at": run_at, "display": "once at 2026-03-18 04:22"},
                "schedule_display": "once at 2026-03-18 04:22",
                "repeat": {"times": 1, "completed": 0},
                "enabled": True,
                "state": "scheduled",
                "paused_at": None,
                "paused_reason": None,
                "created_at": "2026-03-18T04:21:00+00:00",
                "next_run_at": None,
                "last_run_at": None,
                "last_status": None,
                "last_error": None,
                "deliver": "local",
                "origin": None,
            }]
        )

        due = get_due_jobs()

        assert [job["id"] for job in due] == ["oneshot-recover"]
        # Recovery restores next_run_at to the original run time; the
        # cross-process double-exec guard (#59229) is a separate run_claim
        # stamped under the lock, not a next_run_at mutation.
        recovered = get_job("oneshot-recover")
        assert recovered["next_run_at"] == run_at
        assert recovered.get("run_claim") is not None
        assert recovered["run_claim"]["at"] == now.isoformat()

    def test_broken_stale_one_shot_without_next_run_is_not_recovered(self, tmp_cron_dir, monkeypatch):
        now = datetime(2026, 3, 18, 4, 30, 0, tzinfo=timezone.utc)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)

        save_jobs(
            [{
                "id": "oneshot-stale",
                "name": "Too old",
                "prompt": "Word of the day",
                "schedule": {"kind": "once", "run_at": "2026-03-18T04:22:00+00:00", "display": "once at 2026-03-18 04:22"},
                "schedule_display": "once at 2026-03-18 04:22",
                "repeat": {"times": 1, "completed": 0},
                "enabled": True,
                "state": "scheduled",
                "paused_at": None,
                "paused_reason": None,
                "created_at": "2026-03-18T04:21:00+00:00",
                "next_run_at": None,
                "last_run_at": None,
                "last_status": None,
                "last_error": None,
                "deliver": "local",
                "origin": None,
            }]
        )

        assert get_due_jobs() == []
        assert get_job("oneshot-stale")["next_run_at"] is None

    def test_one_shot_not_redispatched_while_running(self, tmp_cron_dir, monkeypatch):
        """#59229: two concurrent schedulers must not double-execute a one-shot.

        Reproduces the reported failure with a job whose run OUTLIVES the tick
        interval (a ~2.5-min research prompt). Process A's tick returns it as
        due and stamps a run_claim; while A is still running, every later tick
        (process B, or A's own next tick) must see the fresh claim and skip —
        not just for one tick window but for the whole run.
        """
        from cron.jobs import _hermes_now
        t0 = _hermes_now()
        run_at = (t0 - timedelta(seconds=5)).isoformat()
        save_jobs([{
            "id": "long-oneshot", "name": "R", "prompt": "2.5min research",
            "schedule": {"kind": "once", "run_at": run_at},
            "next_run_at": run_at, "enabled": True, "state": "scheduled",
        }])

        # Process A tick: picks it up + claims it.
        dueA = get_due_jobs()
        assert [j["id"] for j in dueA] == ["long-oneshot"]
        assert get_job("long-oneshot").get("run_claim") is not None

        # Process B (and A's own subsequent ticks) while A is still running:
        # 28s later (the exact gap in the report) AND 61s later (past any
        # fixed +60s window) — both must skip.
        for gap in (28, 61, 130):
            monkeypatch.setattr("cron.jobs._hermes_now",
                                lambda t0=t0, g=gap: t0 + timedelta(seconds=g))
            assert get_due_jobs() == [], f"double-dispatched at +{gap}s"

    def test_one_shot_run_claim_expires_after_ttl(self, tmp_cron_dir, monkeypatch):
        """A claiming tick that DIED mid-run must not wedge the one-shot forever:
        once the run_claim is older than the TTL it is re-dispatched (recovered)."""
        # Pin the inactivity timeout unset so the derived TTL is deterministic.
        monkeypatch.delenv("HERMES_CRON_TIMEOUT", raising=False)
        from cron.jobs import _hermes_now, _oneshot_run_claim_ttl_seconds
        ttl = _oneshot_run_claim_ttl_seconds()
        t0 = _hermes_now()
        run_at = (t0 - timedelta(seconds=5)).isoformat()
        save_jobs([{
            "id": "wedged", "name": "R", "prompt": "x",
            "schedule": {"kind": "once", "run_at": run_at},
            "next_run_at": run_at, "enabled": True, "state": "scheduled",
        }])
        assert [j["id"] for j in get_due_jobs()] == ["wedged"]  # A claims, then dies

        # Just inside the TTL: still claimed → skipped.
        monkeypatch.setattr("cron.jobs._hermes_now",
                            lambda: t0 + timedelta(seconds=ttl - 10))
        assert get_due_jobs() == []

        # Just past the TTL: stale claim → re-dispatched (recovered), re-claimed.
        monkeypatch.setattr("cron.jobs._hermes_now",
                            lambda: t0 + timedelta(seconds=ttl + 10))
        recovered = get_due_jobs()
        assert [j["id"] for j in recovered] == ["wedged"]

    def test_run_claim_ttl_derived_from_cron_timeout(self, tmp_cron_dir, monkeypatch):
        """The stale-recovery TTL tracks HERMES_CRON_TIMEOUT (3x headroom), with
        the fixed constant as a floor, and falls back to the constant when runs
        are unbounded (timeout=0)."""
        from cron.jobs import (
            _oneshot_run_claim_ttl_seconds as ttl,
            ONESHOT_RUN_CLAIM_TTL_SECONDS as FLOOR,
        )
        # Unset → default 600s inactivity → 1800s (== the historical constant).
        monkeypatch.delenv("HERMES_CRON_TIMEOUT", raising=False)
        assert ttl() == 1800.0

        # A large custom timeout scales the TTL up (3x headroom).
        monkeypatch.setenv("HERMES_CRON_TIMEOUT", "1200")
        assert ttl() == 3600.0

        # A tiny timeout is floored so a claim can never expire mid-run.
        monkeypatch.setenv("HERMES_CRON_TIMEOUT", "30")
        assert ttl() == float(FLOOR)

        # Unlimited runs (0) → no finite bound → fall back to the floor.
        monkeypatch.setenv("HERMES_CRON_TIMEOUT", "0")
        assert ttl() == float(FLOOR)

        # Invalid value → treated as the default 600s → 1800s.
        monkeypatch.setenv("HERMES_CRON_TIMEOUT", "not-a-number")
        assert ttl() == 1800.0


    def test_mark_job_run_clears_one_shot_run_claim(self, tmp_cron_dir, monkeypatch):
        """mark_job_run() clears the run_claim on completion so a re-dispatched
        one-shot (e.g. a stale-recovered retry) is claimable again."""
        from cron.jobs import _hermes_now
        t0 = _hermes_now()
        run_at = (t0 - timedelta(seconds=5)).isoformat()
        # Give it repeat headroom so mark_job_run keeps the job around.
        save_jobs([{
            "id": "claimclear", "name": "R", "prompt": "x",
            "schedule": {"kind": "once", "run_at": run_at},
            "next_run_at": run_at, "enabled": True, "state": "scheduled",
            "repeat": {"times": 2, "completed": 0},
        }])
        assert [j["id"] for j in get_due_jobs()] == ["claimclear"]
        assert get_job("claimclear").get("run_claim") is not None
        mark_job_run("claimclear", True)
        assert get_job("claimclear")["run_claim"] is None

    def test_stale_maxed_oneshot_kept_while_running_in_this_process(
        self, tmp_cron_dir, monkeypatch
    ):
        """#62002: a live run must never have its job record deleted underneath it.

        A one-shot whose run outlives the run_claim TTL (stream stall, laptop
        asleep mid-run) satisfies the same completed >= times + expired-claim
        condition as a dead tick. When the scheduler in this process still has
        the job in its running set, the stale-entry recovery must keep the
        record so the in-flight run's mark_job_run() can land its outcome —
        and remove it only once the run is actually gone.
        """
        import cron.scheduler as scheduler_mod
        from cron.jobs import _hermes_now, _oneshot_run_claim_ttl_seconds
        monkeypatch.delenv("HERMES_CRON_TIMEOUT", raising=False)
        ttl = _oneshot_run_claim_ttl_seconds()
        t0 = _hermes_now()
        run_at = (t0 - timedelta(seconds=ttl + 300)).isoformat()
        # Mid-run store shape: claim_dispatch committed completed=1 and the
        # run_claim was stamped at fire time; next_run_at is only resolved by
        # mark_job_run, so it still points at the (past) fire time.
        save_jobs([{
            "id": "inflight", "name": "flight check", "prompt": "x",
            "schedule": {"kind": "once", "run_at": run_at},
            "next_run_at": run_at, "enabled": True, "state": "scheduled",
            "repeat": {"times": 1, "completed": 1},
            "run_claim": {"at": run_at, "by": "this-machine"},
        }])

        # Run still alive in this process → keep the record, dispatch nothing.
        monkeypatch.setattr(
            scheduler_mod, "get_running_job_ids", lambda: frozenset({"inflight"})
        )
        assert get_due_jobs() == []
        assert get_job("inflight") is not None  # still visible to list/run

        # The claiming tick really died (running set empty) → recovered as before.
        monkeypatch.setattr(
            scheduler_mod, "get_running_job_ids", lambda: frozenset()
        )
        assert get_due_jobs() == []
        assert get_job("inflight") is None  # stale entry cleaned up

    def test_run_claim_heartbeat_keeps_long_run_claimed_past_ttl(
        self, tmp_cron_dir, monkeypatch
    ):
        """#62002 cross-process leg: a heartbeat-refreshed claim never expires
        while the run is alive, so no other tick re-dispatches or stale-removes
        the job even when the run outlives the original TTL horizon."""
        monkeypatch.delenv("HERMES_CRON_TIMEOUT", raising=False)
        from cron.jobs import _hermes_now, _oneshot_run_claim_ttl_seconds
        ttl = _oneshot_run_claim_ttl_seconds()
        t0 = _hermes_now()
        run_at = (t0 - timedelta(seconds=5)).isoformat()
        save_jobs([{
            "id": "slowrun", "name": "R", "prompt": "x",
            "schedule": {"kind": "once", "run_at": run_at},
            "next_run_at": run_at, "enabled": True, "state": "scheduled",
            "repeat": {"times": 1, "completed": 0},
        }])

        # Tick claims + dispatches the job.
        assert [j["id"] for j in get_due_jobs()] == ["slowrun"]
        assert claim_dispatch("slowrun") is True

        # Mid-run heartbeat before the TTL horizon refreshes the claim.
        monkeypatch.setattr("cron.jobs._hermes_now",
                            lambda: t0 + timedelta(seconds=ttl - 60))
        owner = get_job("slowrun")["run_claim"]["by"]
        assert heartbeat_run_claim("slowrun", expected_owner=owner) is True

        # Past the ORIGINAL claim's TTL horizon: without the heartbeat this
        # tick would stale-remove the maxed one-shot; with it the claim is
        # fresh, so the job is skipped and the record survives.
        monkeypatch.setattr("cron.jobs._hermes_now",
                            lambda: t0 + timedelta(seconds=ttl + 10))
        assert get_due_jobs() == []
        assert get_job("slowrun") is not None

        # Run completes → outcome lands on a record that still exists
        # (times=1 reached, so mark_job_run retires the job normally).
        mark_job_run("slowrun", True)
        assert get_job("slowrun") is None

    def test_heartbeat_run_claim_noop_without_claim(self, tmp_cron_dir):
        """heartbeat_run_claim is a safe no-op when there is nothing to refresh
        (manual run that never stamped a claim, or the job is gone)."""
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        save_jobs([{
            "id": "noclaim", "name": "R", "prompt": "x",
            "schedule": {"kind": "once", "run_at": future},
            "next_run_at": future, "enabled": True, "state": "scheduled",
        }])
        assert heartbeat_run_claim("noclaim", expected_owner="owner") is False
        assert heartbeat_run_claim("missing-job", expected_owner="owner") is False
        assert get_job("noclaim").get("run_claim") is None

    def test_heartbeat_run_claim_rejects_replaced_owner(self, tmp_cron_dir):
        """A resumed stale runner must not keep a newer owner's claim alive."""
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        original_at = datetime.now(timezone.utc).isoformat()
        save_jobs([{
            "id": "reclaimed", "name": "R", "prompt": "x",
            "schedule": {"kind": "once", "run_at": future},
            "next_run_at": future, "enabled": True, "state": "scheduled",
            "run_claim": {"at": original_at, "by": "new-owner"},
        }])

        assert heartbeat_run_claim("reclaimed", expected_owner="old-owner") is False
        assert get_job("reclaimed")["run_claim"] == {
            "at": original_at,
            "by": "new-owner",
        }

    def test_heartbeat_run_claim_rejects_non_oneshot(self, tmp_cron_dir):
        """Heartbeat ownership applies only to one-shot dispatch claims."""
        original_at = datetime.now(timezone.utc).isoformat()
        save_jobs([{
            "id": "recurring", "name": "R", "prompt": "x",
            "schedule": {"kind": "interval", "seconds": 60},
            "enabled": True,
            "run_claim": {"at": original_at, "by": "owner"},
        }])

        assert heartbeat_run_claim("recurring", expected_owner="owner") is False
        assert get_job("recurring")["run_claim"]["at"] == original_at


    def test_broken_cron_without_next_run_is_recovered(self, tmp_cron_dir, monkeypatch):
        now = datetime(2026, 3, 18, 10, 0, 0, tzinfo=timezone.utc)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)

        save_jobs(
            [{
                "id": "cron-recover",
                "name": "AI Daily Digest",
                "prompt": "...",
                "schedule": {"kind": "cron", "expr": "0 12 * * *", "display": "0 12 * * *"},
                "schedule_display": "0 12 * * *",
                "repeat": {"times": None, "completed": 0},
                "enabled": True,
                "state": "scheduled",
                "paused_at": None,
                "paused_reason": None,
                "created_at": "2026-03-18T09:00:00+00:00",
                "next_run_at": None,
                "last_run_at": None,
                "last_status": None,
                "last_error": None,
                "deliver": "local",
                "origin": None,
            }]
        )

        assert get_due_jobs() == []
        recovered = get_job("cron-recover")["next_run_at"]
        assert recovered is not None
        recovered_dt = datetime.fromisoformat(recovered)
        if recovered_dt.tzinfo is None:
            recovered_dt = recovered_dt.replace(tzinfo=timezone.utc)
        assert recovered_dt > now

    def test_broken_interval_without_next_run_is_recovered(self, tmp_cron_dir, monkeypatch):
        now = datetime(2026, 3, 18, 10, 0, 0, tzinfo=timezone.utc)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)

        save_jobs(
            [{
                "id": "interval-recover",
                "name": "Hourly heartbeat",
                "prompt": "...",
                "schedule": {"kind": "interval", "minutes": 60, "display": "every 60m"},
                "schedule_display": "every 1h",
                "repeat": {"times": None, "completed": 0},
                "enabled": True,
                "state": "scheduled",
                "paused_at": None,
                "paused_reason": None,
                "created_at": "2026-03-18T09:00:00+00:00",
                "next_run_at": None,
                "last_run_at": None,
                "last_status": None,
                "last_error": None,
                "deliver": "local",
                "origin": None,
            }]
        )

        assert get_due_jobs() == []
        recovered = get_job("interval-recover")["next_run_at"]
        assert recovered is not None
        recovered_dt = datetime.fromisoformat(recovered)
        if recovered_dt.tzinfo is None:
            recovered_dt = recovered_dt.replace(tzinfo=timezone.utc)
        assert recovered_dt > now


    def test_cron_next_run_offset_migration_is_rescheduled_not_fired(self, tmp_cron_dir, monkeypatch):
        current_tz = timezone(timedelta(hours=2))
        now = datetime(2026, 5, 19, 13, 2, 0, tzinfo=current_tz)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)

        # A 21:00 cron was stored while Hermes/system local time was UTC+10.
        # After the host moves to UTC+02, that absolute timestamp converts to
        # 13:00+02.  At 13:02+02 the old code considered it due and fired, even
        # though the user's local wall-clock cron intent is still 21:00.
        save_jobs(
            [{
                "id": "cron-tz-migrate",
                "name": "Migrated local cron",
                "prompt": "...",
                "schedule": {"kind": "cron", "expr": "0 21 * * 2", "display": "0 21 * * 2"},
                "schedule_display": "0 21 * * 2",
                "repeat": {"times": None, "completed": 0},
                "enabled": True,
                "state": "scheduled",
                "paused_at": None,
                "paused_reason": None,
                "created_at": "2026-05-12T21:00:00+10:00",
                "next_run_at": "2026-05-19T21:00:00+10:00",
                "last_run_at": "2026-05-12T21:00:00+10:00",
                "last_status": "ok",
                "last_error": None,
                "deliver": "local",
                "origin": None,
            }]
        )

        assert get_due_jobs() == []
        repaired = datetime.fromisoformat(get_job("cron-tz-migrate")["next_run_at"])
        assert repaired == datetime(2026, 5, 19, 21, 0, 0, tzinfo=current_tz)

    def test_cron_offset_migration_does_not_repair_already_passed_wall_time(self, tmp_cron_dir, monkeypatch):
        current_tz = timezone(timedelta(hours=2))
        now = datetime(2026, 5, 19, 13, 2, 0, tzinfo=current_tz)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)

        save_jobs(
            [{
                "id": "cron-tz-missed",
                "name": "Migrated missed cron",
                "prompt": "...",
                "schedule": {"kind": "cron", "expr": "0 9 * * 2", "display": "0 9 * * 2"},
                "schedule_display": "0 9 * * 2",
                "repeat": {"times": None, "completed": 0},
                "enabled": True,
                "state": "scheduled",
                "paused_at": None,
                "paused_reason": None,
                "created_at": "2026-05-12T09:00:00+10:00",
                "next_run_at": "2026-05-19T09:00:00+10:00",
                "last_run_at": "2026-05-12T09:00:00+10:00",
                "last_status": "ok",
                "last_error": None,
                "deliver": "local",
                "origin": None,
            }]
        )

        # The wall-clock time has already passed, so this does NOT take the
        # timezone-migration repair path (which is for still-future wall-clock
        # runs). It falls through to the stale-grace path, which — since #33315
        # — runs the job once now and fast-forwards next_run_at (rather than
        # skipping). The key assertion for THIS test is that the repaired
        # next_run_at is the normal next cron occurrence, not the migration
        # path's same-day rebase.
        due = get_due_jobs()
        assert [j["id"] for j in due] == ["cron-tz-missed"]  # runs once now (#33315)
        repaired = datetime.fromisoformat(get_job("cron-tz-missed")["next_run_at"])
        assert repaired == datetime(2026, 5, 26, 9, 0, 0, tzinfo=current_tz)

    def test_same_tz_due_cron_still_fires(self, tmp_cron_dir, monkeypatch):
        """Guard must NOT over-fire: a due cron in the SAME offset fires normally."""
        current_tz = timezone(timedelta(hours=2))
        now = datetime(2026, 5, 19, 21, 0, 30, tzinfo=current_tz)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)
        save_jobs([{
            "id": "cron-same-tz", "name": "same tz", "prompt": "...",
            "schedule": {"kind": "cron", "expr": "0 21 * * 2", "display": "0 21 * * 2"},
            "schedule_display": "0 21 * * 2",
            "repeat": {"times": None, "completed": 0},
            "enabled": True, "state": "scheduled", "paused_at": None, "paused_reason": None,
            "created_at": "2026-05-12T21:00:00+02:00",
            "next_run_at": "2026-05-19T21:00:00+02:00",  # same offset as now
            "last_run_at": "2026-05-12T21:00:00+02:00",
            "last_status": "ok", "last_error": None, "deliver": "local", "origin": None,
        }])
        # offset matches -> guard skips -> the genuinely-due job is returned to fire.
        due = get_due_jobs()
        assert [j["id"] for j in due] == ["cron-same-tz"]

    def test_interval_job_with_stale_offset_is_unaffected(self, tmp_cron_dir, monkeypatch):
        """The offset-repair guard is cron-only; interval jobs never take it.

        A stale-offset interval job whose converted instant is well past the
        grace window is handled by the pre-existing stale fast-forward path
        (not the cron repair path). Verify it fast-forwards via interval math
        (next = now + interval), proving the cron-only guard didn't touch it.
        """
        current_tz = timezone(timedelta(hours=2))
        now = datetime(2026, 5, 19, 13, 2, 0, tzinfo=current_tz)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)
        save_jobs([{
            "id": "interval-stale-tz", "name": "interval", "prompt": "...",
            "schedule": {"kind": "interval", "minutes": 60, "display": "every 1h"},
            "schedule_display": "every 1h",
            "repeat": {"times": None, "completed": 0},
            "enabled": True, "state": "scheduled", "paused_at": None, "paused_reason": None,
            "created_at": "2026-05-19T10:00:00+10:00",
            "next_run_at": "2026-05-19T12:00:00+10:00",  # stale offset, instant 04:00+02 (well past)
            "last_run_at": "2026-05-19T11:00:00+10:00",
            "last_status": "ok", "last_error": None, "deliver": "local", "origin": None,
        }])
        get_due_jobs()
        # The cron-only repair path would have produced a cron occurrence; instead
        # the interval stale fast-forward recomputes next = now + 60m (interval
        # math), confirming the guard did not intercept this interval job.
        nr = datetime.fromisoformat(get_job("interval-stale-tz")["next_run_at"])
        assert nr == now + timedelta(minutes=60)

    def test_offset_migration_at_wall_clock_equal_now_falls_through(self, tmp_cron_dir, monkeypatch):
        """Boundary: stored wall-clock == now wall-clock (strict >) does NOT take
        the repair path — it falls through to the existing due/fast-forward logic."""
        current_tz = timezone(timedelta(hours=2))
        now = datetime(2026, 5, 19, 13, 0, 0, tzinfo=current_tz)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)
        save_jobs([{
            "id": "cron-wall-equal", "name": "wall equal", "prompt": "...",
            "schedule": {"kind": "cron", "expr": "0 13 * * 2", "display": "0 13 * * 2"},
            "schedule_display": "0 13 * * 2",
            "repeat": {"times": None, "completed": 0},
            "enabled": True, "state": "scheduled", "paused_at": None, "paused_reason": None,
            "created_at": "2026-05-12T13:00:00+10:00",
            # stored naive wall-clock 13:00 == now naive wall-clock 13:00 -> strict > is False
            "next_run_at": "2026-05-19T13:00:00+10:00",
            "last_run_at": "2026-05-12T13:00:00+10:00",
            "last_status": "ok", "last_error": None, "deliver": "local", "origin": None,
        }])
        # _stored_wall_clock_is_future is strict (>), so 13:00 == 13:00 is False
        # -> repair guard skipped -> existing logic handles it (does not raise).
        get_due_jobs()  # must not raise / must not take the repair branch
        # next_run_at must NOT have been rewritten to a future cron occurrence by
        # the repair path (it either fires or fast-forwards via the normal path).
        nr = get_job("cron-wall-equal")["next_run_at"]
        assert nr is None or datetime.fromisoformat(nr).utcoffset() == now.utcoffset() or "+10:00" in nr


class TestEnabledToolsets:
    def test_enabled_toolsets_stored(self, tmp_cron_dir):
        job = create_job(prompt="monitor", schedule="every 1h", enabled_toolsets=["web", "terminal"])
        assert job["enabled_toolsets"] == ["web", "terminal"]

    def test_enabled_toolsets_persisted(self, tmp_cron_dir):
        job = create_job(prompt="monitor", schedule="every 1h", enabled_toolsets=["web", "file"])
        fetched = get_job(job["id"])
        assert fetched["enabled_toolsets"] == ["web", "file"]

    def test_enabled_toolsets_none_when_omitted(self, tmp_cron_dir):
        job = create_job(prompt="monitor", schedule="every 1h")
        assert job["enabled_toolsets"] is None

    def test_enabled_toolsets_empty_list_normalizes_to_none(self, tmp_cron_dir):
        job = create_job(prompt="monitor", schedule="every 1h", enabled_toolsets=[])
        assert job["enabled_toolsets"] is None

    def test_enabled_toolsets_whitespace_entries_stripped(self, tmp_cron_dir):
        job = create_job(prompt="monitor", schedule="every 1h", enabled_toolsets=["web", " ", "file"])
        assert job["enabled_toolsets"] == ["web", "file"]

    def test_enabled_toolsets_updated_via_update_job(self, tmp_cron_dir):
        job = create_job(prompt="monitor", schedule="every 1h")
        update_job(job["id"], {"enabled_toolsets": ["web", "delegation"]})
        fetched = get_job(job["id"])
        assert fetched["enabled_toolsets"] == ["web", "delegation"]


class TestMarkJobRunConcurrency:
    """Regression tests for concurrent parallel job state writes.

    tick() dispatches multiple jobs to separate threads simultaneously.
    Without _jobs_file_lock protecting the load→modify→save cycle in
    mark_job_run(), concurrent writes can clobber each other's updates
    (last-writer-wins), leaving some jobs with stale last_status / last_run_at.
    """

    def test_three_concurrent_mark_job_run_no_overwrites(self, tmp_cron_dir):
        """Run mark_job_run() for 3 jobs in parallel threads; all must land correctly."""
        # Create 3 distinct recurring jobs
        job_a = create_job(prompt="Job A", schedule="every 1h")
        job_b = create_job(prompt="Job B", schedule="every 1h")
        job_c = create_job(prompt="Job C", schedule="every 1h")

        errors: list = []

        def run_mark(job_id: str, success: bool, error_msg=None):
            try:
                mark_job_run(job_id, success=success, error=error_msg)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        # Fire all three concurrently
        threads = [
            threading.Thread(target=run_mark, args=(job_a["id"], True)),
            threading.Thread(target=run_mark, args=(job_b["id"], False, "timeout")),
            threading.Thread(target=run_mark, args=(job_c["id"], True)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Unexpected exceptions in worker threads: {errors}"

        # Verify each job has the correct state — no overwrites
        a = get_job(job_a["id"])
        b = get_job(job_b["id"])
        c = get_job(job_c["id"])

        assert a is not None, "Job A was unexpectedly deleted"
        assert b is not None, "Job B was unexpectedly deleted"
        assert c is not None, "Job C was unexpectedly deleted"

        assert a["last_status"] == "ok", f"Job A last_status wrong: {a['last_status']}"
        assert a["last_run_at"] is not None, "Job A last_run_at not set"
        assert a["repeat"]["completed"] == 1, f"Job A completed count wrong: {a['repeat']['completed']}"

        assert b["last_status"] == "error", f"Job B last_status wrong: {b['last_status']}"
        assert b["last_error"] == "timeout", f"Job B last_error wrong: {b['last_error']}"
        assert b["last_run_at"] is not None, "Job B last_run_at not set"
        assert b["repeat"]["completed"] == 1, f"Job B completed count wrong: {b['repeat']['completed']}"

        assert c["last_status"] == "ok", f"Job C last_status wrong: {c['last_status']}"
        assert c["last_run_at"] is not None, "Job C last_run_at not set"
        assert c["repeat"]["completed"] == 1, f"Job C completed count wrong: {c['repeat']['completed']}"

    def test_repeated_concurrent_runs_accumulate_completed_count(self, tmp_cron_dir):
        """Stress test: 10 threads each call mark_job_run on a different job once.

        The completed count for every job must be exactly 1 after all threads finish,
        confirming no thread's write was silently dropped.
        """
        n = 10
        jobs = [create_job(prompt=f"Stress job {i}", schedule="every 1h") for i in range(n)]
        errors: list = []

        def run_mark(job_id: str):
            try:
                mark_job_run(job_id, success=True)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=run_mark, args=(j["id"],)) for j in jobs]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Unexpected exceptions: {errors}"

        for job in jobs:
            updated = get_job(job["id"])
            assert updated is not None, f"Job {job['id']} was deleted"
            assert updated["last_status"] == "ok", (
                f"Job {job['id']} has wrong last_status: {updated['last_status']}"
            )
            assert updated["repeat"]["completed"] == 1, (
                f"Job {job['id']} completed count is {updated['repeat']['completed']}, expected 1"
            )


class TestBadNextRunAtRecovery:
    """Regression: malformed next_run_at must not crash the due scan or starve siblings.

    Mirrors the id-less and non-dict-schedule patterns: a single bad persisted
    record in jobs.json must not abort _get_due_jobs_locked before save.
    """

    def test_bad_next_run_at_does_not_crash_or_block_sibling_jobs(self, tmp_cron_dir):
        """One job with unparseable next_run_at + one healthy due sibling.

        get_due_jobs must succeed and return the healthy job; the bad record
        must be repaired (next_run_at cleared so recovery can set a sane value).
        """
        from datetime import timezone, timedelta as td
        now = datetime.now(timezone.utc)
        past = (now - td(seconds=30)).isoformat()
        future = (now + td(days=1)).isoformat()

        # Bad record: next_run_at is not a valid ISO string (e.g. from hand-edit or corruption)
        # Healthy sibling is past due with good schedule.
        bad_job = {
            "id": "bad-next",
            "schedule": {"kind": "interval", "minutes": 60},
            "next_run_at": "not-a-valid-iso-timestamp!!!",
            "enabled": True,
            "created_at": past,
        }
        good_job = {
            "id": "good-sibling",
            "schedule": {"kind": "interval", "minutes": 5},
            "next_run_at": past,
            "enabled": True,
            "created_at": past,
        }
        save_jobs([bad_job, good_job])

        # Must not raise
        due = get_due_jobs()

        # The healthy job must still be returned
        ids = [j["id"] for j in due]
        assert "good-sibling" in ids, f"healthy sibling missing from due jobs: {ids}"
        assert "bad-next" not in ids  # bad one may be repaired and/or not yet due after repair

        # Bad job should have been auto-repaired (next_run_at stripped or fixed)
        repaired = get_job("bad-next")
        assert repaired is not None
        nr = repaired.get("next_run_at")
        if nr is not None:
            # If still present it must now be parseable
            datetime.fromisoformat(nr)

        # Calling again must remain stable (no crash on re-scan)
        due2 = get_due_jobs()
        assert any(j["id"] == "good-sibling" for j in due2)


class TestPerJobScanContainment:
    """Structural guard: ANY per-job exception in the due scan must degrade to
    skipping that one job for the tick — never abort the scan and starve
    healthy siblings (the freeze class behind bad id / schedule / next_run_at).
    """

    def test_unforeseen_per_job_exception_does_not_starve_siblings(self, tmp_cron_dir):
        """Simulate a FUTURE malformed-field variant none of the shape
        normalizers repair, by making grace computation raise for one job
        only. The per-job guard must skip it and still return the sibling."""
        from datetime import timezone, timedelta as td
        from unittest.mock import patch as mock_patch

        now = datetime.now(timezone.utc)
        past = (now - td(seconds=30)).isoformat()

        poison = {
            "id": "poison",
            # minutes=7 tags this schedule so the patched helper can target it
            "schedule": {"kind": "interval", "minutes": 7},
            "next_run_at": past,
            "enabled": True,
            "created_at": past,
        }
        good = {
            "id": "good-sibling",
            "schedule": {"kind": "interval", "minutes": 5},
            "next_run_at": past,
            "enabled": True,
            "created_at": past,
        }
        save_jobs([poison, good])

        import cron.jobs as jobs_mod
        real_grace = jobs_mod._compute_grace_seconds

        def selective_grace(schedule):
            if schedule.get("minutes") == 7:
                raise RuntimeError("simulated unforeseen malformed field")
            return real_grace(schedule)

        with mock_patch.object(jobs_mod, "_compute_grace_seconds", selective_grace):
            due = get_due_jobs()  # must not raise

        ids = [j["id"] for j in due]
        assert "good-sibling" in ids, f"healthy sibling starved: {ids}"
        assert "poison" not in ids

        # Scheduler stays alive on subsequent ticks too.
        with mock_patch.object(jobs_mod, "_compute_grace_seconds", selective_grace):
            due2 = get_due_jobs()
        assert any(j["id"] == "good-sibling" for j in due2)


class TestSaveJobOutput:
    def test_creates_output_file(self, tmp_cron_dir):
        output_file = save_job_output("test123", "# Results\nEverything ok.")
        assert output_file.exists()
        assert output_file.read_text() == "# Results\nEverything ok."
        assert "test123" in str(output_file)

    @pytest.mark.parametrize("bad_job_id", ["../escape", "nested/escape", ".", "..", ""])
    def test_rejects_unsafe_job_id(self, tmp_cron_dir, bad_job_id):
        """Path-escape attempts must fail closed and never create dirs."""
        with pytest.raises(ValueError, match="output path"):
            save_job_output(bad_job_id, "# Results")
        assert not (tmp_cron_dir / "escape").exists()

    def test_rejects_absolute_job_id(self, tmp_cron_dir):
        """Absolute paths as job IDs must fail closed."""
        with pytest.raises(ValueError, match="output path"):
            save_job_output(str(tmp_cron_dir / "outside"), "# Results")
        assert not (tmp_cron_dir / "outside").exists()

    def test_rejects_symlinked_output_directory_without_external_write(self, tmp_cron_dir):
        external = tmp_cron_dir.parent / "external-output"
        external.mkdir()
        external_sentinel = external / "sentinel.txt"
        external_sentinel.write_text("unchanged", encoding="utf-8")
        output_dir = tmp_cron_dir / "cron" / "output"
        if output_dir.exists():
            output_dir.rmdir()
        output_dir.symlink_to(external, target_is_directory=True)

        with pytest.raises(OSError, match="Cron output path is not a directory"):
            save_job_output("safe-job", "must not escape")

        assert external_sentinel.read_text(encoding="utf-8") == "unchanged"
        assert not (external / "safe-job").exists()

    def test_output_path_swap_after_open_never_writes_the_replacement(self, tmp_cron_dir, monkeypatch):
        import cron.jobs as jobs_mod

        external = tmp_cron_dir.parent / "external-cron"
        external.mkdir()
        external_sentinel = external / "sentinel.txt"
        external_sentinel.write_text("unchanged", encoding="utf-8")
        cron_dir = tmp_cron_dir / "cron"
        original_cron = tmp_cron_dir.parent / "cron-original"
        original_open = jobs_mod._open_pinned_cron_output_dir

        def replace_after_open():
            cron_fd, output_fd = original_open()
            cron_dir.rename(original_cron)
            cron_dir.symlink_to(external, target_is_directory=True)
            return cron_fd, output_fd

        monkeypatch.setattr(jobs_mod, "_open_pinned_cron_output_dir", replace_after_open)

        with pytest.raises(OSError, match="Cron directory changed during output persistence"):
            save_job_output("safe-job", "pinned output")

        assert external_sentinel.read_text(encoding="utf-8") == "unchanged"
        assert not (external / "output").exists()
        assert any((original_cron / "output" / "safe-job").glob("*.md"))


class TestCronOutputRetention:
    """Per-run cron output must self-prune so long deploys don't fill the disk (#52383)."""

    @staticmethod
    def _seed(d, count):
        d.mkdir(parents=True, exist_ok=True)
        names = [f"2026-06-25_10-00-{i:02d}.md" for i in range(count)]
        for n in names:
            (d / n).write_text("x")
        return names

    def test_prune_keeps_newest_n(self, tmp_path):
        from cron.jobs import _prune_job_output
        d = tmp_path / "job"
        names = self._seed(d, 10)
        assert _prune_job_output(d, keep=3) == 7
        assert sorted(p.name for p in d.glob("*.md")) == names[-3:]

    def test_prune_noop_when_under_cap(self, tmp_path):
        from cron.jobs import _prune_job_output
        d = tmp_path / "job"
        self._seed(d, 3)
        assert _prune_job_output(d, keep=5) == 0
        assert len(list(d.glob("*.md"))) == 3

    def test_prune_disabled_when_keep_non_positive(self, tmp_path):
        from cron.jobs import _prune_job_output
        d = tmp_path / "job"
        self._seed(d, 5)
        assert _prune_job_output(d, keep=0) == 0
        assert _prune_job_output(d, keep=-1) == 0
        assert len(list(d.glob("*.md"))) == 5

    def test_prune_ignores_non_md_and_temp_files(self, tmp_path):
        from cron.jobs import _prune_job_output
        d = tmp_path / "job"
        self._seed(d, 4)
        (d / ".output_abc.tmp").write_text("partial")
        (d / "manifest.json").write_text("{}")
        _prune_job_output(d, keep=2)
        assert (d / ".output_abc.tmp").exists()
        assert (d / "manifest.json").exists()
        assert len(list(d.glob("*.md"))) == 2

    def test_save_job_output_prunes_old_runs(self, tmp_cron_dir, monkeypatch):
        from cron.jobs import save_job_output, _job_output_dir
        monkeypatch.setattr("cron.jobs._cron_output_keep", lambda: 3)
        seq = iter(
            datetime(2026, 6, 25, 10, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=i)
            for i in range(8)
        )
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: next(seq))
        for _ in range(8):
            save_job_output("job1", "report")
        files = sorted(_job_output_dir("job1").glob("*.md"))
        assert len(files) == 3  # only the 3 most-recent runs survive

    def test_cron_output_keep_reads_config(self, monkeypatch):
        import cron.jobs as jobs
        monkeypatch.setattr(
            "hermes_cli.config.load_config", lambda: {"cron": {"output_retention": 7}}
        )
        assert jobs._cron_output_keep() == 7

    def test_cron_output_keep_defaults_on_bad_config(self, monkeypatch):
        import cron.jobs as jobs
        monkeypatch.setattr(
            "hermes_cli.config.load_config", lambda: {"cron": {"output_retention": "oops"}}
        )
        assert jobs._cron_output_keep() == jobs._CRON_OUTPUT_DEFAULT_KEEP


# =========================================================================
# claim_dispatch — pre-run one-shot crash safety (issue #38758)
# =========================================================================

class TestClaimDispatch:
    """One-shot jobs must commit their dispatch BEFORE the side effect runs, so
    a tick that dies mid-execution (gateway kill, OOM, hard-timeout) can re-fire
    the job at most ``repeat.times`` times instead of infinitely."""

    def _oneshot(self, times=1, completed=0):
        return {
            "id": "os1",
            "name": "one-shot",
            "enabled": True,
            "schedule": {"kind": "once", "run_at": "2026-01-01T00:00:00+00:00"},
            "repeat": {"times": times, "completed": completed},
        }

    def test_claim_increments_and_persists(self, tmp_cron_dir):
        save_jobs([self._oneshot(times=1, completed=0)])
        assert claim_dispatch("os1") is True
        # Persisted BEFORE any side effect — survives a crash.
        assert load_jobs()[0]["repeat"]["completed"] == 1

    def test_signed_dispatch_requires_the_exact_active_revision_claim(
        self,
        tmp_cron_dir,
    ):
        signed = {
            **self._oneshot(times=1, completed=0),
            **_signed_job_revision("os1"),
        }
        save_jobs([signed])
        claim = begin_job_run_outcome(signed)
        assert claim is not None
        stale = {**claim, "run_id": "cron-run:" + "9" * 32}

        assert claim_dispatch("os1", run_outcome_claim=stale) is False
        assert load_jobs()[0]["repeat"]["completed"] == 0
        assert claim_dispatch("os1", run_outcome_claim=claim) is True
        assert load_jobs()[0]["repeat"]["completed"] == 1

    def test_signed_dispatch_without_claim_is_refused_without_consuming_count(
        self,
        tmp_cron_dir,
    ):
        signed = {
            **self._oneshot(times=1, completed=0),
            **_signed_job_revision("os1"),
        }
        save_jobs([signed])

        assert claim_dispatch("os1") is False
        persisted = load_jobs()[0]
        assert persisted["repeat"]["completed"] == 0
        assert persisted.get("active_run_outcome_claim") is None

    def test_already_dispatched_oneshot_is_removed(self, tmp_cron_dir):
        # A prior tick claimed (completed==times) then died before mark_job_run
        # could remove the job.  The next claim must refuse AND clean up.
        save_jobs([self._oneshot(times=1, completed=1)])
        assert claim_dispatch("os1") is False
        assert load_jobs() == []  # removed, will not re-fire

    def test_recurring_job_is_not_claimed(self, tmp_cron_dir):
        job = {
            "id": "rec",
            "schedule": {"kind": "interval", "minutes": 5},
            "repeat": {"times": 3, "completed": 0},
        }
        save_jobs([job])
        assert claim_dispatch("rec") is True
        # Recurring jobs use advance_next_run(); claim must NOT touch completed.
        assert load_jobs()[0]["repeat"]["completed"] == 0

    def test_infinite_oneshot_not_claimed(self, tmp_cron_dir):
        job = self._oneshot(times=0, completed=0)  # times<=0 means infinite
        save_jobs([job])
        assert claim_dispatch("os1") is True
        assert load_jobs()[0]["repeat"]["completed"] == 0

    def test_no_repeat_block_not_claimed(self, tmp_cron_dir):
        job = {"id": "os1", "schedule": {"kind": "once", "run_at": "2026-01-01T00:00:00+00:00"}}
        save_jobs([job])
        assert claim_dispatch("os1") is True
        assert "repeat" not in load_jobs()[0]

    def test_missing_job_proceeds(self, tmp_cron_dir):
        # A handed-in job dict not persisted in the store (external provider /
        # direct caller) can't be claimed — proceed rather than suppress it.
        save_jobs([])
        assert claim_dispatch("ghost") is True

    def test_missing_job_with_exact_claim_is_refused(self, tmp_cron_dir):
        signed = {
            **self._oneshot(times=1, completed=0),
            **_signed_job_revision("os1"),
        }
        save_jobs([signed])
        claim = begin_job_run_outcome(signed)
        assert claim is not None
        save_jobs([])

        assert claim_dispatch("os1", run_outcome_claim=claim) is False

    def test_mark_job_run_does_not_double_count_preclaimed_oneshot(self, tmp_cron_dir):
        # Full lifecycle: claim bumps completed to times, then mark_job_run must
        # NOT increment again — it recognizes the pre-claim and removes the job.
        save_jobs([self._oneshot(times=1, completed=0)])
        assert claim_dispatch("os1") is True
        assert load_jobs()[0]["repeat"]["completed"] == 1
        mark_job_run("os1", success=True)
        assert load_jobs() == []  # completed once, removed — not fired twice

    def test_mark_job_run_still_increments_recurring(self, tmp_cron_dir):
        # The double-count guard is one-shot-specific; recurring jobs keep the
        # legacy post-run increment.
        job = {
            "id": "rec",
            "schedule": {"kind": "interval", "minutes": 5},
            "repeat": {"times": 3, "completed": 1},
        }
        save_jobs([job])
        mark_job_run("rec", success=True)
        assert load_jobs()[0]["repeat"]["completed"] == 2

    def test_get_due_jobs_removes_stale_maxed_oneshot(self, tmp_cron_dir):
        # A claimed one-shot whose tick died leaves completed>=times with
        # last_run_at still unset, so the recovery helper re-arms it as due.
        # get_due_jobs must drop it instead of returning it for another fire.
        past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
        save_jobs([{
            "id": "os1",
            "name": "one-shot",
            "enabled": True,
            "schedule": {"kind": "once", "run_at": past},
            "repeat": {"times": 1, "completed": 1},
            "next_run_at": None,
        }])
        due = get_due_jobs()
        assert due == []
        assert load_jobs() == []  # cleaned up

    def test_bad_schedule_does_not_crash_or_block_sibling_jobs(self, tmp_cron_dir):
        """Regression for a job with non-dict 'schedule' (null / string / etc.

        from direct jobs.json edit or old writer).

        Such a record must not raise in _get_due_jobs_locked and must not
        prevent healthy sibling jobs from being returned or having their
        next_run_at advanced+persisted. Mirrors the id-less job P1 pattern.
        """
        past = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
        future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()

        bad = {
            "id": "bad-sched",
            "name": "bad",
            "enabled": True,
            "schedule": None,  # poison: not a dict
            "next_run_at": future,  # not due
        }
        good = {
            "id": "good",
            "name": "good",
            "enabled": True,
            "schedule": {"kind": "interval", "minutes": 5},
            "next_run_at": past,
        }
        save_jobs([bad, good])

        due = get_due_jobs()
        due_ids = [j["id"] for j in due]
        assert "good" in due_ids
        assert "bad-sched" not in due_ids  # bad one ignored, no crash

        # At minimum, the good job's record is still intact (no corruption from the bad neighbor)
        loaded = {j["id"]: j for j in load_jobs()}
        assert "good" in loaded


class TestLateEnvRepointScopesStore:
    """A HERMES_HOME set AFTER cron.jobs import must scope the store even
    without use_cron_store(): fixtures that patch the environment too late
    previously read/wrote the import-time jobs.json — the user's real file."""

    def test_late_env_repoint_scopes_store(self, tmp_path, monkeypatch):
        import cron.jobs as jobs

        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        store = jobs._current_cron_store()
        expected = tmp_path.resolve() / "cron"
        assert store.cron_dir == expected
        assert store.jobs_file == expected / "jobs.json"
        assert store.output_dir == expected / "output"
        # the import-time compatibility constants are untouched
        assert jobs.JOBS_FILE != store.jobs_file

    def test_unchanged_home_returns_import_time_constants(self, monkeypatch):
        import cron.jobs as jobs

        monkeypatch.setenv("HERMES_HOME", str(jobs.HERMES_DIR))
        store = jobs._current_cron_store()
        assert store.jobs_file is jobs.JOBS_FILE

    def test_use_cron_store_override_still_wins(self, tmp_path, monkeypatch):
        import cron.jobs as jobs

        monkeypatch.setenv("HERMES_HOME", str(tmp_path / "env-home"))
        with jobs.use_cron_store(tmp_path / "override-home"):
            store = jobs._current_cron_store()
            assert store.jobs_file == (tmp_path / "override-home").resolve() / "cron" / "jobs.json"

    def test_patched_compatibility_constants_beat_env(self, tmp_path, monkeypatch):
        """Deliberately re-pointed module constants are the documented
        process-wide escape hatch — they win over a repointed HERMES_HOME."""
        import cron.jobs as jobs

        patched_dir = tmp_path / "patched-cron"
        monkeypatch.setattr(jobs, "CRON_DIR", patched_dir)
        monkeypatch.setattr(jobs, "JOBS_FILE", patched_dir / "jobs.json")
        monkeypatch.setattr(jobs, "OUTPUT_DIR", patched_dir / "output")
        monkeypatch.setenv("HERMES_HOME", str(tmp_path / "env-home"))
        store = jobs._current_cron_store()
        assert store.jobs_file == patched_dir / "jobs.json"

    def test_public_io_after_late_env_repoint_leaves_old_file_untouched(
        self, tmp_path, monkeypatch
    ):
        """The public API, not the store internals: save_jobs()/load_jobs()
        called after a post-import HERMES_HOME repoint must operate on the NEW
        home's jobs.json and leave the import-time file byte-identical.

        The "import-time home" is SIMULATED at a tmp location by patching the
        module constants and the import-time snapshot together (so they still
        compare equal and the deliberate-repoint branch does not fire). The
        test must never touch the real import-time jobs.json: if this module
        was first imported before the suite's env isolation applied, that
        path IS the developer's live file — writing a sentinel there is
        exactly the incident this PR exists to prevent."""
        import cron.jobs as jobs

        sim_old_home = tmp_path / "import-time-home"
        sim_cron = sim_old_home / "cron"
        monkeypatch.setattr(jobs, "HERMES_DIR", sim_old_home)
        monkeypatch.setattr(jobs, "CRON_DIR", sim_cron)
        monkeypatch.setattr(jobs, "JOBS_FILE", sim_cron / "jobs.json")
        monkeypatch.setattr(jobs, "OUTPUT_DIR", sim_cron / "output")
        monkeypatch.setattr(
            jobs, "_IMPORT_STORE",
            jobs._CronStorePaths(jobs.CRON_DIR, jobs.JOBS_FILE, jobs.OUTPUT_DIR),
        )

        # Plant a sentinel at the (simulated) import-time location — the file
        # a late-patching fixture used to clobber.
        old_file = jobs.JOBS_FILE
        old_file.parent.mkdir(parents=True, exist_ok=True)
        sentinel = '[{"id": "sentinel-do-not-touch"}]'
        old_file.write_text(sentinel, encoding="utf-8")

        new_home = tmp_path / "late-home"
        monkeypatch.setenv("HERMES_HOME", str(new_home))

        job = {
            "id": "lateenvjob01",
            "name": "late-env",
            "prompt": None,
            "schedule_display": None,
            "schedule": {"kind": "interval", "minutes": 60, "display": "every 60m"},
            "enabled": True,
        }
        save_jobs([job])

        # public read round-trips from the NEW home...
        loaded = load_jobs()
        assert [j["id"] for j in loaded] == ["lateenvjob01"]
        new_file = new_home.resolve() / "cron" / "jobs.json"
        assert new_file.is_file()
        # ...and the import-time file is byte-identical to the sentinel.
        assert old_file.read_text(encoding="utf-8") == sentinel
