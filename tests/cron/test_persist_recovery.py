"""Core contracts for stale Cron resume recovery (#1569)."""

from __future__ import annotations

import copy
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from agent.skill_resolution import resolve_skill_refs
from cron.jobs import (
    CronJobGovernanceError,
    _cron_persist_spec_hash,
    _cron_resume_precondition_hash,
    _cron_stable_hash,
    create_job,
    cron_persist_resume_identity,
    get_cron_persist_recovery,
    load_jobs,
    update_job,
)


NOW = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)


@pytest.fixture()
def recovery_store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / "profile"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_PROFILE_ID", "default")
    monkeypatch.setenv("HERMES_CRON_CREATION_GOVERNANCE_REQUIRED", "1")
    monkeypatch.setattr("cron.jobs.CRON_DIR", home / "cron")
    monkeypatch.setattr("cron.jobs.JOBS_FILE", home / "cron" / "jobs.json")
    monkeypatch.setattr("cron.jobs.OUTPUT_DIR", home / "cron" / "output")
    monkeypatch.setattr("cron.jobs._hermes_now", lambda: NOW)
    monkeypatch.setattr("hermes_cli.plugins.discover_plugins", lambda: None)
    return home


def _write_skill(home: Path, body: str = "First revision.") -> Path:
    path = home / "skills" / "work" / "governed" / "SKILL.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "---\nname: governed\ndescription: recovery fixture\n---\n\n" + body + "\n",
        encoding="utf-8",
    )
    return path


def _candidate(home: Path, *, job_id: str = "abc123def456") -> dict[str, Any]:
    binding = resolve_skill_refs(home, ["governed"])[0]
    return {
        "id": job_id,
        "name": "Pending digest",
        "prompt": "Summarize the approved scope.",
        "skills": ["governed"],
        "skill": "governed",
        "skill_bindings": [binding],
        "schedule": {"kind": "interval", "minutes": 60, "display": "every 60m"},
        "schedule_display": "every 60m",
        "repeat": {"times": None, "completed": 0},
        "enabled": True,
        "state": "scheduled",
        "created_at": NOW.isoformat(),
        "next_run_at": (NOW + timedelta(hours=1)).isoformat(),
        "deliver": "origin",
        "origin": {
            "platform": "feishu",
            "chat_id": "group-source",
            "chat_type": "group",
        },
        "source_route": {
            "transport_id": "feishu",
            "channel_id": "group-source",
            "chat_type": "group",
        },
        "authorized_behavior_ref": "behavior.test",
    }


def _resume_package(
    candidate: dict[str, Any],
    operation: str,
    *,
    prior_job_hash: str = "",
) -> dict[str, Any]:
    frozen = copy.deepcopy(candidate)
    identity = cron_persist_resume_identity(operation, frozen)
    candidate_hash = f"sha256:{'a' * 64}"
    persist_spec_hash = _cron_persist_spec_hash(operation, frozen)
    receipt = {
        "schema_version": "cron-persist-resume/v2",
        "profile_id": identity["profile_id"],
        "frame_id": "cpf_rejected000000000000000",
        "action_id": "approve",
        "pending_id": "cpa_rejected000000000000000",
        "operation": operation,
        "candidate_hash": candidate_hash,
        "persist_spec_hash": persist_spec_hash,
        "cron_job_id": frozen["id"],
        "behavior_id": "behavior.test",
        "process_id": "process.test",
        "approval_id": "approval-old",
        "admin_actor_uid": "admin.test",
        "prior_job_hash": prior_job_hash,
        "issued_at": (NOW - timedelta(minutes=5)).isoformat(),
        "expires_at": (NOW + timedelta(hours=1)).isoformat(),
        "request_id": identity["request_id"],
        "request_hash": identity["request_hash"],
        "source_route_hash": identity["source_route_hash"],
    }
    receipt["receipt_id"] = _cron_stable_hash(receipt)
    return {
        "schema_version": "cron-persist-resume/v2",
        "operation": operation,
        "job_id": frozen["id"],
        "candidate_hash": candidate_hash,
        "persist_spec_hash": persist_spec_hash,
        "authorized_behavior_ref": "behavior.test",
        "scope_immutable": True,
        "receipt": receipt,
        "job": frozen,
        "instruction": "Resubmit this immutable job through cron governance.",
    }


def _report(*results: dict[str, Any]) -> dict[str, Any]:
    return {"callback_count": len(results), "results": list(results), "failures": []}


def _allow(*, resume_id: str = "") -> dict[str, Any]:
    patch: dict[str, Any] = {
        "creation_governance_receipt": {
            "schema_version": "cron-creation-governance/v1",
            "receipt_id": "sha256:creation",
        }
    }
    if resume_id:
        patch["creation_governance_receipt"]["resume_receipt_id"] = resume_id
        patch.update(
            enabled=False,
            state="paused",
            paused_reason="admin_authorized_pending_explicit_enable",
        )
    return {"action": "allow", "persist_disposition": "allow_write", "job_patch": patch}


def _fresh_block(context: dict[str, Any]) -> dict[str, Any]:
    suffix = str(context["recovery_id"]).split(":", 1)[1][:24]
    frame_id = f"cpf_{suffix}"
    pending_id = f"cpa_{suffix}"
    return {
        "action": "block",
        "reason": "group_source_requires_admin_dm",
        "state": "frozen_candidate_required",
        "pending_action": {
            "schema_version": "cron-admin-pending-action/v1",
            "status": "pending_admin_dm",
            "pending_id": pending_id,
            "frame": {"frame_id": frame_id, "state": "created"},
        },
        "notification_effect": {
            "kind": "cron-admin-pending-notification/v1",
            "frame_id": frame_id,
        },
    }


def test_real_create_stale_v2_derives_one_blocked_recovery_and_replays_once(
    recovery_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    skill = _write_skill(recovery_store)
    package = _resume_package(_candidate(recovery_store), "create")
    skill.write_text(
        skill.read_text(encoding="utf-8") + "Second revision.\n", encoding="utf-8"
    )
    pre_calls: list[dict[str, Any]] = []
    post_calls: list[dict[str, Any]] = []

    def invoke_mandatory(name: str, **kwargs: Any) -> dict[str, Any]:
        assert name == "pre_cron_job_persist"
        assert (
            getattr(
                __import__("cron.jobs", fromlist=["_jobs_lock_state"])._jobs_lock_state,
                "depth",
                0,
            )
            > 0
        )
        context = copy.deepcopy(kwargs["recovery_context"])
        pre_calls.append(context)
        assert "cron_persist_resume_receipt" not in kwargs["candidate"]
        return _report(_fresh_block(context))

    def invoke_post(name: str, **kwargs: Any) -> list[Any]:
        assert name == "post_cron_job_persist"
        assert (
            getattr(
                __import__("cron.jobs", fromlist=["_jobs_lock_state"])._jobs_lock_state,
                "depth",
                0,
            )
            == 0
        )
        post_calls.append(copy.deepcopy(kwargs))
        return []

    monkeypatch.setattr("hermes_cli.plugins.invoke_mandatory_hook", invoke_mandatory)
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", invoke_post)

    errors = []
    for _ in range(2):
        with pytest.raises(CronJobGovernanceError) as exc_info:
            create_job(prompt=None, schedule="", governance_resume=package)
        errors.append(exc_info.value)

    assert load_jobs() == []
    assert len(pre_calls) == len(post_calls) == 1
    recovery = errors[0].payload()["recovery"]
    assert errors[1].payload()["recovery"] == recovery
    assert recovery["disposition"] == "fresh_blocked_candidate"
    assert recovery["rejected_receipt_id"] == package["receipt"]["receipt_id"]
    assert recovery["fresh_persist_spec_hash"] != package["persist_spec_hash"]
    stored = get_cron_persist_recovery(recovery["recovery_id"])
    assert stored is not None
    assert stored["pending_id"] == recovery["pending_id"]
    assert stored["frame_id"] == recovery["frame_id"]
    with sqlite3.connect(recovery_store / "cron" / "persist-recovery.sqlite3") as conn:
        assert (
            conn.execute("SELECT count(*) FROM cron_persist_recoveries").fetchone()[0]
            == 1
        )


def test_concurrent_stale_create_registers_one_recovery_frame_and_effect(
    recovery_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    skill = _write_skill(recovery_store)
    package = _resume_package(_candidate(recovery_store), "create")
    skill.write_text(skill.read_text(encoding="utf-8") + "Drift.\n", encoding="utf-8")
    counts = {"pre": 0, "post": 0}
    count_lock = threading.Lock()

    def invoke_mandatory(_name: str, **kwargs: Any) -> dict[str, Any]:
        with count_lock:
            counts["pre"] += 1
        return _report(_fresh_block(kwargs["recovery_context"]))

    def invoke_post(_name: str, **_kwargs: Any) -> list[Any]:
        with count_lock:
            counts["post"] += 1
        return []

    monkeypatch.setattr("hermes_cli.plugins.invoke_mandatory_hook", invoke_mandatory)
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", invoke_post)

    def attempt() -> str:
        with pytest.raises(CronJobGovernanceError) as exc_info:
            create_job(prompt=None, schedule="", governance_resume=package)
        return str(exc_info.value.payload()["recovery"]["recovery_id"])

    with ThreadPoolExecutor(max_workers=8) as pool:
        recovery_ids = list(pool.map(lambda _index: attempt(), range(16)))

    assert len(set(recovery_ids)) == 1
    assert counts == {"pre": 1, "post": 1}
    assert load_jobs() == []


def test_stale_update_binds_prior_revision_and_does_not_mutate_job(
    recovery_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    skill = _write_skill(recovery_store)
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda _name, **_kwargs: _report(_allow()),
    )
    original = create_job(
        prompt="Original",
        schedule="every 1h",
        skills=["governed"],
        origin={"platform": "feishu", "chat_id": "group-source", "chat_type": "group"},
        deliver="origin",
        authorized_behavior_ref="behavior.test",
    )
    candidate = {**copy.deepcopy(original), "prompt": "Approved update"}
    package = _resume_package(
        candidate,
        "update",
        prior_job_hash=_cron_resume_precondition_hash(original),
    )
    skill.write_text(
        skill.read_text(encoding="utf-8") + "Updated digest.\n", encoding="utf-8"
    )
    contexts: list[dict[str, Any]] = []

    def block(_name: str, **kwargs: Any) -> dict[str, Any]:
        contexts.append(copy.deepcopy(kwargs["recovery_context"]))
        return _report(_fresh_block(kwargs["recovery_context"]))

    monkeypatch.setattr("hermes_cli.plugins.invoke_mandatory_hook", block)
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *_args, **_kwargs: [])

    with pytest.raises(CronJobGovernanceError) as exc_info:
        update_job(original["id"], {}, governance_resume=package)

    assert load_jobs() == [original]
    recovery = exc_info.value.payload()["recovery"]
    assert recovery["operation"] == "update"
    assert recovery["prior_job_hash"] == _cron_resume_precondition_hash(original)
    assert len(contexts) == 1


def test_stale_update_wrong_prior_revision_fails_before_hook_and_store(
    recovery_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    skill = _write_skill(recovery_store)
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda _name, **_kwargs: _report(_allow()),
    )
    original = create_job(
        prompt="Original",
        schedule="every 1h",
        skills=["governed"],
        authorized_behavior_ref="behavior.test",
    )
    package = _resume_package(
        {**copy.deepcopy(original), "prompt": "Approved update"},
        "update",
        prior_job_hash=f"sha256:{'f' * 64}",
    )
    skill.write_text(skill.read_text(encoding="utf-8") + "Drift.\n", encoding="utf-8")
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: pytest.fail("bad precondition reached hook"),
    )

    with pytest.raises(CronJobGovernanceError) as exc_info:
        update_job(original["id"], {}, governance_resume=package)

    assert exc_info.value.decision["reason"] == "resume_update_precondition_mismatch"
    assert load_jobs() == [original]
    assert not (recovery_store / "cron" / "persist-recovery.sqlite3").exists()


def test_same_rejected_receipt_with_second_fresh_spec_fails_lineage_closed(
    recovery_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    skill = _write_skill(recovery_store)
    package = _resume_package(_candidate(recovery_store), "create")
    skill.write_text(
        skill.read_text(encoding="utf-8") + "First drift.\n", encoding="utf-8"
    )
    calls = 0

    def block(_name: str, **kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return _report(_fresh_block(kwargs["recovery_context"]))

    monkeypatch.setattr("hermes_cli.plugins.invoke_mandatory_hook", block)
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *_args, **_kwargs: [])

    with pytest.raises(CronJobGovernanceError):
        create_job(prompt=None, schedule="", governance_resume=package)
    skill.write_text(
        skill.read_text(encoding="utf-8") + "Second drift.\n", encoding="utf-8"
    )
    with pytest.raises(CronJobGovernanceError) as exc_info:
        create_job(prompt=None, schedule="", governance_resume=package)

    assert exc_info.value.decision["reason"] == "resume_recovery_lineage_conflict"
    assert calls == 1
    with sqlite3.connect(recovery_store / "cron" / "persist-recovery.sqlite3") as conn:
        assert (
            conn.execute("SELECT count(*) FROM cron_persist_recoveries").fetchone()[0]
            == 1
        )


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("profile_id", "other", "resume_request_or_route_mismatch"),
        ("source_route_hash", f"sha256:{'b' * 64}", "resume_request_or_route_mismatch"),
        ("request_hash", f"sha256:{'c' * 64}", "resume_request_or_route_mismatch"),
    ],
)
def test_v2_identity_mismatch_fails_before_hook_or_recovery_store(
    recovery_store: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: str,
    reason: str,
) -> None:
    _write_skill(recovery_store)
    package = _resume_package(_candidate(recovery_store), "create")
    package["receipt"][field] = value
    package["receipt"]["receipt_id"] = _cron_stable_hash({
        key: value for key, value in package["receipt"].items() if key != "receipt_id"
    })
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: pytest.fail("identity mismatch reached hook"),
    )

    with pytest.raises(CronJobGovernanceError) as exc_info:
        create_job(prompt=None, schedule="", governance_resume=package)

    assert exc_info.value.decision["reason"] == reason
    assert not (recovery_store / "cron" / "persist-recovery.sqlite3").exists()


def test_missing_skill_records_one_unreconstructable_disposition_without_hook(
    recovery_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    skill = _write_skill(recovery_store)
    package = _resume_package(_candidate(recovery_store), "create")
    skill.unlink()
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: pytest.fail("unreconstructable resume reached hook"),
    )

    with pytest.raises(CronJobGovernanceError) as exc_info:
        create_job(prompt=None, schedule="", governance_resume=package)

    recovery = exc_info.value.payload()["recovery"]
    assert recovery["disposition"] == "blocked_unreconstructable"
    assert recovery["reason"] == "skill_unavailable_in_active_profile"
    assert load_jobs() == []


def test_v2_exact_resume_remains_the_normal_paused_create_path(
    recovery_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_skill(recovery_store)
    package = _resume_package(_candidate(recovery_store), "create")
    resume_id = package["receipt"]["receipt_id"]

    def allow(_name: str, **kwargs: Any) -> dict[str, Any]:
        assert "recovery_context" not in kwargs
        return _report(_allow(resume_id=resume_id))

    monkeypatch.setattr("hermes_cli.plugins.invoke_mandatory_hook", allow)

    created = create_job(prompt=None, schedule="", governance_resume=package)

    assert created["state"] == "paused"
    assert created["enabled"] is False
    assert len(load_jobs()) == 1
    assert not (recovery_store / "cron" / "persist-recovery.sqlite3").exists()


def test_recovery_database_symlink_fails_closed_before_hook(
    recovery_store: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    skill = _write_skill(recovery_store)
    package = _resume_package(_candidate(recovery_store), "create")
    skill.write_text(skill.read_text(encoding="utf-8") + "Drift.\n", encoding="utf-8")
    cron_dir = recovery_store / "cron"
    cron_dir.mkdir()
    target = tmp_path / "outside.sqlite3"
    target.write_bytes(b"")
    (cron_dir / "persist-recovery.sqlite3").symlink_to(target)
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: pytest.fail("unsafe store reached hook"),
    )

    with pytest.raises(CronJobGovernanceError) as exc_info:
        create_job(prompt=None, schedule="", governance_resume=package)

    assert exc_info.value.decision["reason"] == "resume_recovery_store_unavailable"


def test_corrupt_recovery_database_fails_closed_before_hook(
    recovery_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    skill = _write_skill(recovery_store)
    package = _resume_package(_candidate(recovery_store), "create")
    skill.write_text(skill.read_text(encoding="utf-8") + "Drift.\n", encoding="utf-8")
    cron_dir = recovery_store / "cron"
    cron_dir.mkdir()
    database = cron_dir / "persist-recovery.sqlite3"
    database.write_bytes(b"not a sqlite database")
    database.chmod(0o600)
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: pytest.fail("corrupt store reached hook"),
    )

    with pytest.raises(CronJobGovernanceError) as exc_info:
        create_job(prompt=None, schedule="", governance_resume=package)

    assert exc_info.value.decision["reason"] == "resume_recovery_store_unavailable"


def test_recovery_record_failure_does_not_dispatch_registered_effect(
    recovery_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cron.persist_recovery import CronPersistRecoveryStoreError

    skill = _write_skill(recovery_store)
    package = _resume_package(_candidate(recovery_store), "create")
    skill.write_text(skill.read_text(encoding="utf-8") + "Drift.\n", encoding="utf-8")
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda _name, **kwargs: _report(_fresh_block(kwargs["recovery_context"])),
    )

    def fail_record(*_args: Any, **_kwargs: Any) -> None:
        raise CronPersistRecoveryStoreError("write failed")

    monkeypatch.setattr(
        "cron.persist_recovery.record_recovery",
        fail_record,
    )
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *_args, **_kwargs: pytest.fail("undurable recovery dispatched effect"),
    )

    with pytest.raises(CronJobGovernanceError) as exc_info:
        create_job(prompt=None, schedule="", governance_resume=package)

    assert exc_info.value.decision["reason"] == "resume_recovery_store_unavailable"
