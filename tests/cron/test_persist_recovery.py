"""Core contracts for stale Cron resume recovery (#1569)."""

from __future__ import annotations

import copy
import os
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

import cron.persist_recovery as persist_recovery
from agent.skill_resolution import resolve_skill_refs
from cron.jobs import (
    CronJobGovernanceError,
    _cron_persist_spec_hash,
    _cron_resume_precondition_hash,
    _cron_stable_hash,
    create_job,
    cron_persist_recovery_dispatch_key,
    cron_persist_resume_identity,
    get_cron_persist_recovery,
    load_jobs,
    update_job,
    use_cron_store,
)


NOW = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)


def test_dispatch_ack_schema_matches_durable_cas_contract() -> None:
    assert (
        persist_recovery.DISPATCH_ACK_SCHEMA_VERSION
        == "cron-persist-recovery-dispatch-ack/v2"
    )


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
        "profile_home_sha256": identity["profile_home_sha256"],
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
    effect = {
        "kind": "cron-admin-pending-notification/v1",
        "frame_id": frame_id,
    }
    registration = {
        "schema_version": "cron-persist-recovery-registration/v1",
        "issuer": {"id": "test-governance", "version": "1"},
        "recovery_id": context["recovery_id"],
        "pending_id": pending_id,
        "frame_id": frame_id,
        "effect_hash": _cron_stable_hash(effect),
    }
    registration["dispatch_key"] = cron_persist_recovery_dispatch_key(
        context["recovery_id"],
        registration["issuer"],
        effect,
    )
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
        "recovery_registration": registration,
        "notification_effect": effect,
    }


def _dispatch_ack(kwargs: dict[str, Any]) -> dict[str, Any]:
    registration = kwargs["recovery_registration"]
    dispatch = kwargs["recovery_dispatch"]
    return {
        "schema_version": "cron-persist-recovery-dispatch-ack/v2",
        "issuer": registration["issuer"],
        "recovery_id": registration["recovery_id"],
        "dispatch_key": dispatch["dispatch_key"],
        "disposition": "durably_accepted",
        "durable_cas": {
            "schema_version": "cron-persist-recovery-durable-cas/v1",
            "dispatch_key": dispatch["dispatch_key"],
            "owner_id": "test-outbox",
            "cas_version": 1,
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
        return [_dispatch_ack(kwargs)]

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
    assert stored["dispatch"]["disposition"] == "dispatched"
    with sqlite3.connect(recovery_store / "cron" / "persist-recovery.sqlite3") as conn:
        assert (
            conn.execute("SELECT count(*) FROM cron_persist_recoveries").fetchone()[0]
            == 1
        )


def test_slow_recovery_dispatch_heartbeat_stays_in_named_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    default_home = tmp_path / "default"
    named_home = tmp_path / "profiles" / "atlas"
    default_home.mkdir()
    named_home.mkdir(parents=True)
    monkeypatch.setenv("HERMES_HOME", str(default_home))
    monkeypatch.setenv("HERMES_PROFILE_ID", "atlas")
    monkeypatch.setenv("HERMES_CRON_CREATION_GOVERNANCE_REQUIRED", "1")
    monkeypatch.setattr("cron.jobs._hermes_now", lambda: NOW)
    monkeypatch.setattr("hermes_cli.plugins.discover_plugins", lambda: None)
    skill = _write_skill(named_home)
    with use_cron_store(named_home):
        package = _resume_package(_candidate(named_home), "create")
        skill.write_text(
            skill.read_text(encoding="utf-8") + "Named profile revision.\n",
            encoding="utf-8",
        )

    real_claim = persist_recovery.claim_recovery_dispatch

    def short_claim(*args: Any, **kwargs: Any) -> dict[str, Any] | None:
        return real_claim(*args, **kwargs, lease_seconds=0.06)

    def invoke_mandatory(_name: str, **kwargs: Any) -> dict[str, Any]:
        return _report(_fresh_block(copy.deepcopy(kwargs["recovery_context"])))

    def invoke_post(_name: str, **kwargs: Any) -> list[Any]:
        time.sleep(0.12)
        return [_dispatch_ack(kwargs)]

    monkeypatch.setattr(
        "cron.persist_recovery.claim_recovery_dispatch", short_claim
    )
    monkeypatch.setattr("hermes_cli.plugins.invoke_mandatory_hook", invoke_mandatory)
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", invoke_post)

    with use_cron_store(named_home), pytest.raises(CronJobGovernanceError) as exc_info:
        create_job(prompt=None, schedule="", governance_resume=package)

    payload = exc_info.value.payload()
    assert "recovery" in payload, payload
    recovery_id = payload["recovery"]["recovery_id"]
    stored = get_cron_persist_recovery(recovery_id, profile_home=named_home)
    assert stored is not None
    assert stored["dispatch"]["disposition"] == "dispatched"
    assert not (default_home / "cron" / "persist-recovery.sqlite3").exists()


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
        return [_dispatch_ack(_kwargs)]

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


@pytest.mark.parametrize(
    ("mutate_report", "reason"),
    [
        (
            lambda block: _report(
                block,
                {"action": "block", "reason": "second_policy_block"},
            ),
            "resume_recovery_registration_ambiguous",
        ),
        (
            lambda block: _report({
                **block,
                "post_persist_effects": [
                    {"kind": "second-effect", "frame_id": "other"}
                ],
            }),
            "resume_recovery_registration_invalid",
        ),
        (
            lambda block: _report({
                **block,
                "recovery_registration": {
                    **block["recovery_registration"],
                    "recovery_id": f"sha256:{'f' * 64}",
                },
            }),
            "resume_recovery_registration_invalid",
        ),
        (
            lambda block: _report({
                **block,
                "recovery_registration": {
                    **block["recovery_registration"],
                    "issuer": {"id": "unversioned"},
                },
            }),
            "resume_recovery_registration_invalid",
        ),
    ],
)
def test_recovery_registration_counterexamples_fail_closed(
    recovery_store: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutate_report,
    reason: str,
) -> None:
    skill = _write_skill(recovery_store)
    package = _resume_package(_candidate(recovery_store), "create")
    skill.write_text(skill.read_text(encoding="utf-8") + "Drift.\n", encoding="utf-8")

    def invalid_registration(_name: str, **kwargs: Any) -> dict[str, Any]:
        return mutate_report(_fresh_block(kwargs["recovery_context"]))

    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook", invalid_registration
    )
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *_args, **_kwargs: pytest.fail("invalid registration dispatched"),
    )

    with pytest.raises(CronJobGovernanceError) as exc_info:
        create_job(prompt=None, schedule="", governance_resume=package)

    assert exc_info.value.decision["reason"] == reason
    assert not (recovery_store / "cron" / "persist-recovery.sqlite3").exists()


def test_recovery_dispatch_survives_both_crash_windows_with_stable_outbox_key(
    recovery_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import cron.persist_recovery as recovery_store_module

    CronPersistRecoveryStoreError = recovery_store_module.CronPersistRecoveryStoreError

    skill = _write_skill(recovery_store)
    package = _resume_package(_candidate(recovery_store), "create")
    skill.write_text(skill.read_text(encoding="utf-8") + "Drift.\n", encoding="utf-8")
    pre_calls = 0
    observer_calls = 0
    transport_keys: set[tuple[str, str]] = set()
    transport_calls = 0

    def block(_name: str, **kwargs: Any) -> dict[str, Any]:
        nonlocal pre_calls
        pre_calls += 1
        return _report(_fresh_block(kwargs["recovery_context"]))

    def observer(_name: str, **kwargs: Any) -> list[Any]:
        nonlocal observer_calls, transport_calls
        observer_calls += 1
        key = (
            kwargs["recovery_registration"]["recovery_id"],
            kwargs["notification_effect"]["frame_id"],
        )
        if key not in transport_keys:
            transport_keys.add(key)
            transport_calls += 1
        return [_dispatch_ack(kwargs)]

    monkeypatch.setattr("hermes_cli.plugins.invoke_mandatory_hook", block)
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", observer)
    real_dispatch = __import__(
        "cron.jobs", fromlist=["_dispatch_claimed_cron_recovery_effects"]
    )._dispatch_claimed_cron_recovery_effects
    monkeypatch.setattr(
        "cron.jobs._dispatch_claimed_cron_recovery_effects",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(CronJobGovernanceError) as first_error:
        create_job(prompt=None, schedule="", governance_resume=package)
    recovery_id = first_error.value.payload()["recovery"]["recovery_id"]
    database = recovery_store / "cron" / "persist-recovery.sqlite3"
    with sqlite3.connect(database) as conn:
        conn.execute(
            "UPDATE cron_persist_recovery_dispatches SET claim_expires_at = 0 "
            "WHERE recovery_id = ?",
            (recovery_id,),
        )
    monkeypatch.setattr(
        "cron.jobs._dispatch_claimed_cron_recovery_effects", real_dispatch
    )

    def fail_complete(*_args: Any, **_kwargs: Any) -> None:
        raise CronPersistRecoveryStoreError("simulated crash after transport")

    real_complete = recovery_store_module.complete_recovery_dispatch
    monkeypatch.setattr(
        "cron.persist_recovery.complete_recovery_dispatch", fail_complete
    )
    with pytest.raises(CronJobGovernanceError):
        create_job(prompt=None, schedule="", governance_resume=package)
    with sqlite3.connect(database) as conn:
        conn.execute(
            "UPDATE cron_persist_recovery_dispatches SET claim_expires_at = 0 "
            "WHERE recovery_id = ?",
            (recovery_id,),
        )
    monkeypatch.setattr(
        "cron.persist_recovery.complete_recovery_dispatch", real_complete
    )
    with pytest.raises(CronJobGovernanceError):
        create_job(prompt=None, schedule="", governance_resume=package)

    stored = get_cron_persist_recovery(recovery_id, profile_home=recovery_store)
    assert stored is not None
    assert stored["dispatch"]["disposition"] == "dispatched"
    assert pre_calls == 1
    assert observer_calls == 2
    assert transport_calls == 1


def test_distinct_root_profiles_have_distinct_canonical_home_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = [tmp_path / "root-a", tmp_path / "root-b"]
    for root in roots:
        root.mkdir()
    candidate = {
        "id": "same-job-id",
        "prompt": "same request",
        "deliver": "local",
    }
    identities = []
    monkeypatch.setenv("HERMES_PROFILE_ID", "default")
    for root in roots:
        with use_cron_store(root):
            identities.append(cron_persist_resume_identity("create", candidate))

    assert {identity["profile_id"] for identity in identities} == {"default"}
    assert identities[0]["profile_home_sha256"] != identities[1]["profile_home_sha256"]
    assert identities[0]["request_id"] != identities[1]["request_id"]


def _minimal_recovery_record() -> dict[str, Any]:
    return {
        "schema_version": "cron-persist-recovery/v1",
        "recovery_id": f"sha256:{'1' * 64}",
        "rejected_receipt_id": f"sha256:{'2' * 64}",
        "request_id": f"sha256:{'3' * 64}",
        "profile_id": "default",
        "operation": "create",
        "candidate": {},
        "decision": {"action": "block"},
    }


def _dispatch_recovery_record() -> dict[str, Any]:
    record = _minimal_recovery_record()
    effect = {"kind": "test-effect/v1", "frame_id": "frame-1"}
    issuer = {"id": "test-governance", "version": "1"}
    record["registration"] = {
        "schema_version": "cron-persist-recovery-registration/v1",
        "issuer": issuer,
        "recovery_id": record["recovery_id"],
        "pending_id": "pending-1",
        "frame_id": "frame-1",
        "effect_hash": _cron_stable_hash(effect),
        "dispatch_key": cron_persist_recovery_dispatch_key(
            record["recovery_id"], issuer, effect
        ),
    }
    record["notification_effect"] = effect
    return record


def test_dispatch_heartbeat_fences_reentry_after_original_lease(
    tmp_path: Path,
) -> None:
    from cron.persist_recovery import (
        claim_recovery_dispatch,
        heartbeat_recovery_dispatch,
        record_recovery,
    )

    profile = tmp_path / "profile"
    profile.mkdir()
    record = _dispatch_recovery_record()
    cron_dir = profile / "cron"
    record_recovery(cron_dir, record, profile_home=profile)
    first = claim_recovery_dispatch(
        cron_dir,
        record["recovery_id"],
        profile_home=profile,
        now=10.0,
        lease_seconds=0.03,
    )
    assert first is not None
    assert heartbeat_recovery_dispatch(
        cron_dir,
        record["recovery_id"],
        first["claim_id"],
        first["fence_token"],
        profile_home=profile,
        now=10.02,
        lease_seconds=0.03,
    )
    assert claim_recovery_dispatch(
        cron_dir,
        record["recovery_id"],
        profile_home=profile,
        now=10.04,
        lease_seconds=0.03,
    ) is None


def test_read_recovers_hot_delete_journal(tmp_path: Path) -> None:
    from cron.persist_recovery import get_recovery, record_recovery

    profile = tmp_path / "profile"
    profile.mkdir()
    cron_dir = profile / "cron"
    record = _minimal_recovery_record()
    record_recovery(cron_dir, record, profile_home=profile)
    database = cron_dir / "persist-recovery.sqlite3"
    script = """
import os, sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
conn.execute('PRAGMA journal_mode=DELETE')
conn.execute('BEGIN IMMEDIATE')
conn.execute("UPDATE cron_persist_recovery_dispatches SET disposition='claimed'")
os._exit(0)
"""
    subprocess.run([sys.executable, "-c", script, str(database)], check=True)

    recovered = get_recovery(
        cron_dir,
        record["recovery_id"],
        profile_home=profile,
    )

    assert recovered is not None
    assert recovered["dispatch"]["disposition"] == "not_required"


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS root aliases")
def test_darwin_var_profile_alias_is_canonicalized(tmp_path: Path) -> None:
    from cron.persist_recovery import get_recovery, record_recovery

    profile = tmp_path / "profile"
    profile.mkdir()
    resolved = profile.resolve()
    if not str(resolved).startswith("/private/var/"):
        pytest.skip("pytest temp root is not under /private/var")
    alias = Path("/var") / resolved.relative_to("/private/var")
    record = _minimal_recovery_record()

    record_recovery(alias / "cron", record, profile_home=alias)
    recovered = get_recovery(
        resolved / "cron",
        record["recovery_id"],
        profile_home=resolved,
    )

    assert recovered is not None


def test_recovery_store_rejects_out_of_profile_cron_directory(tmp_path: Path) -> None:
    from cron.persist_recovery import (
        CronPersistRecoveryStoreError,
        record_recovery,
    )

    profile = tmp_path / "profile"
    outside = tmp_path / "outside"
    profile.mkdir()
    outside.mkdir()

    with pytest.raises(CronPersistRecoveryStoreError, match="outside the profile"):
        record_recovery(
            outside / "cron",
            _minimal_recovery_record(),
            profile_home=profile,
        )


def test_recovery_store_rejects_symlinked_profile_ancestor(tmp_path: Path) -> None:
    from cron.persist_recovery import (
        CronPersistRecoveryStoreError,
        record_recovery,
    )

    profile = tmp_path / "profile"
    profile.mkdir()
    linked = tmp_path / "linked-profile"
    linked.symlink_to(profile, target_is_directory=True)

    with pytest.raises(CronPersistRecoveryStoreError, match="symbolic link"):
        record_recovery(
            linked / "cron",
            _minimal_recovery_record(),
            profile_home=linked,
        )


def test_recovery_store_detects_final_file_swap_during_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import cron.persist_recovery as recovery_store_module

    profile = tmp_path / "profile"
    profile.mkdir()
    cron_dir = profile / "cron"
    stored = recovery_store_module.record_recovery(
        cron_dir,
        _minimal_recovery_record(),
        profile_home=profile,
    )
    database = cron_dir / "persist-recovery.sqlite3"
    replacement = cron_dir / "replacement.sqlite3"
    shutil.copy2(database, replacement)
    real_connect = recovery_store_module.sqlite3.connect
    swapped = False

    def swapping_connect(*args: Any, **kwargs: Any):
        nonlocal swapped
        if not swapped:
            swapped = True
            os.replace(replacement, database)
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(recovery_store_module.sqlite3, "connect", swapping_connect)

    with pytest.raises(
        recovery_store_module.CronPersistRecoveryStoreError,
        match="changed while opening",
    ):
        recovery_store_module.get_recovery(
            cron_dir,
            stored["recovery_id"],
            profile_home=profile,
        )
