"""Cron persistence governance foundation contracts."""

from __future__ import annotations

import copy
import contextlib
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from cron.jobs import (
    CronJobGovernanceError,
    _cron_persist_spec_hash,
    _cron_stable_hash,
    create_job,
    load_jobs,
    save_jobs,
    update_job,
)


@pytest.fixture()
def governed_store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / "profile"
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_CRON_CREATION_GOVERNANCE_REQUIRED", "1")
    monkeypatch.setattr("cron.jobs.CRON_DIR", home / "cron")
    monkeypatch.setattr("cron.jobs.JOBS_FILE", home / "cron" / "jobs.json")
    monkeypatch.setattr("cron.jobs.OUTPUT_DIR", home / "cron" / "output")
    monkeypatch.setattr("hermes_cli.plugins.discover_plugins", lambda: None)
    return home


def _receipt(
    *,
    resume_id: str = "",
    receipt_id: str = "sha256:creation",
) -> dict[str, Any]:
    receipt = {
        "schema_version": "cron-creation-governance/v1",
        "receipt_id": receipt_id,
    }
    if resume_id:
        receipt["resume_receipt_id"] = resume_id
    return receipt


def _allow_decision(
    *,
    resume_id: str = "",
    receipt_id: str = "sha256:creation",
) -> dict[str, Any]:
    patch: dict[str, Any] = {
        "creation_governance_receipt": _receipt(
            resume_id=resume_id,
            receipt_id=receipt_id,
        )
    }
    if resume_id:
        patch.update(
            enabled=False,
            state="paused",
            paused_reason="admin_authorized_pending_explicit_enable",
        )
    return {
        "action": "allow",
        "persist_disposition": "allow_write",
        "job_patch": patch,
    }


def _mandatory_report(
    results: list[dict[str, Any]],
    *,
    failures: list[dict[str, Any]] | None = None,
    callback_count: int | None = None,
) -> dict[str, Any]:
    return {
        "callback_count": callback_count if callback_count is not None else len(results),
        "results": results,
        "failures": failures or [],
    }


def _resume_package(job: dict[str, Any], operation: str) -> dict[str, Any]:
    fill = "c" if operation == "create" else "d"
    candidate_hash = f"sha256:{'a' * 64}"
    frozen_job = copy.deepcopy(job)
    frozen_job["authorized_behavior_ref"] = "behavior.test"
    persist_spec_hash = _cron_persist_spec_hash(operation, frozen_job)
    receipt = {
        "schema_version": "cron-persist-resume/v1",
        "profile_id": "default",
        "frame_id": f"cpf_{fill * 24}",
        "action_id": "approve",
        "pending_id": f"cpa_{fill * 24}",
        "operation": operation,
        "candidate_hash": candidate_hash,
        "persist_spec_hash": persist_spec_hash,
        "cron_job_id": frozen_job["id"],
        "behavior_id": "behavior.test",
        "process_id": "process.test",
        "approval_id": f"approval-{fill}",
        "admin_actor_uid": "admin.test",
        "prior_job_hash": "" if operation == "create" else f"sha256:{'e' * 64}",
        "issued_at": "2026-08-09T00:00:00+00:00",
        "expires_at": "2026-08-10T00:00:00+00:00",
    }
    receipt["receipt_id"] = _cron_stable_hash(receipt)
    return {
        "schema_version": "cron-persist-resume/v1",
        "operation": operation,
        "job_id": frozen_job["id"],
        "candidate_hash": candidate_hash,
        "persist_spec_hash": persist_spec_hash,
        "authorized_behavior_ref": "behavior.test",
        "scope_immutable": True,
        "receipt": receipt,
        "job": frozen_job,
        "instruction": "Resubmit this immutable job through cron governance.",
    }


def _candidate(job_id: str = "abc123def456") -> dict[str, Any]:
    return {
        "id": job_id,
        "name": "Pending digest",
        "prompt": "Summarize the approved scope.",
        "skills": [],
        "skill": None,
        "schedule": {"kind": "interval", "minutes": 60, "display": "every 60m"},
        "schedule_display": "every 60m",
        "repeat": {"times": None, "completed": 0},
        "enabled": True,
        "state": "scheduled",
        "created_at": "2026-08-09T00:00:00+00:00",
        "next_run_at": "2026-08-09T01:00:00+00:00",
        "deliver": "local",
    }


def _store_bytes(home: Path) -> bytes | None:
    path = home / "cron" / "jobs.json"
    return path.read_bytes() if path.exists() else None


@pytest.mark.parametrize(
    ("operation", "resume"),
    [
        ("create", False),
        ("create", True),
        ("update", False),
        ("update", True),
    ],
)
def test_denied_paths_preserve_store_and_dispatch_all_unique_effects_post_lock(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    resume: bool,
) -> None:
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda name, **kwargs: _mandatory_report([_allow_decision()]),
    )
    existing = None
    if operation == "update":
        existing = create_job(prompt="original", schedule="every 1h")

    before = _store_bytes(governed_store)
    effect_a = {"kind": "test/v1", "frame_id": "frame-a"}
    effect_b = {"kind": "test/v1", "frame_id": "frame-b"}
    observed: list[dict[str, Any]] = []

    def invoke_mandatory(name: str, **kwargs: Any) -> dict[str, Any]:
        assert name == "pre_cron_job_persist"
        return _mandatory_report(
            [
                {"action": "block", "reason": "first_blocker", "state": "review_required"},
                {"action": "block", "reason": "effect_owner_a", "notification_effect": effect_a},
                {"action": "block", "reason": "duplicate_a", "notification_effect": effect_a},
                {"action": "block", "reason": "effect_owner_b", "notification_effect": effect_b},
            ]
        )

    def invoke_post(name: str, **kwargs: Any) -> list[dict[str, Any]]:
        assert name == "post_cron_job_persist"
        assert getattr(__import__("cron.jobs", fromlist=["_jobs_lock_state"])._jobs_lock_state, "depth", 0) == 0
        probe = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import fcntl, sys; "
                    "h=open(sys.argv[1], 'a+b'); "
                    "fcntl.flock(h, fcntl.LOCK_EX | fcntl.LOCK_NB)"
                ),
                str(governed_store / "cron" / ".jobs.lock"),
            ],
            check=False,
        )
        assert probe.returncode == 0
        assert _store_bytes(governed_store) == before
        observed.append(copy.deepcopy(kwargs))
        return []

    monkeypatch.setattr("hermes_cli.plugins.invoke_mandatory_hook", invoke_mandatory)
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", invoke_post)

    with pytest.raises(CronJobGovernanceError) as exc_info:
        if operation == "create":
            if resume:
                package = _resume_package(_candidate(), "create")
                create_job(prompt=None, schedule="", governance_resume=package)
            else:
                create_job(prompt="denied", schedule="every 1h")
        else:
            assert existing is not None
            if resume:
                candidate = {**existing, "prompt": "resumed update"}
                package = _resume_package(candidate, "update")
                update_job(existing["id"], {}, governance_resume=package)
            else:
                update_job(existing["id"], {"prompt": "denied update"})

    assert exc_info.value.decision["reason"] == "first_blocker"
    assert exc_info.value.decision["post_persist_effects"] == [effect_a, effect_b]
    assert _store_bytes(governed_store) == before
    assert observed == [
        {"operation": operation, "notification_effect": effect_a},
        {"operation": operation, "notification_effect": effect_b},
    ]


@pytest.mark.parametrize(
    ("operation", "resume"),
    [
        ("create", False),
        ("create", True),
        ("update", False),
        ("update", True),
    ],
)
def test_ordinary_and_resume_paths_persist_one_governed_candidate(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    resume: bool,
) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    def invoke(name: str, **kwargs: Any) -> dict[str, Any]:
        assert name == "pre_cron_job_persist"
        calls.append((kwargs["operation"], copy.deepcopy(kwargs["candidate"])))
        receipt = kwargs["candidate"].get("cron_persist_resume_receipt")
        resume_id = str(receipt.get("receipt_id") or "") if isinstance(receipt, dict) else ""
        return _mandatory_report([_allow_decision(resume_id=resume_id)])

    monkeypatch.setattr("hermes_cli.plugins.invoke_mandatory_hook", invoke)

    if operation == "create" and not resume:
        result = create_job(prompt="ordinary create", schedule="every 1h")
    elif operation == "create":
        result = create_job(
            prompt=None,
            schedule="",
            governance_resume=_resume_package(_candidate(), "create"),
        )
    else:
        original = create_job(prompt="original", schedule="every 1h")
        calls.clear()
        if resume:
            candidate = {**original, "prompt": "resumed update"}
            result = update_job(
                original["id"],
                {},
                governance_resume=_resume_package(candidate, "update"),
            )
        else:
            result = update_job(original["id"], {"prompt": "ordinary update"})

    assert result is not None
    assert len(load_jobs()) == 1
    assert "cron_persist_resume_receipt" not in load_jobs()[0]
    assert calls[0][0] == operation
    assert ("cron_persist_resume_receipt" in calls[0][1]) is resume
    if resume:
        assert result["enabled"] is False
        assert result["state"] == "paused"
        assert result["paused_reason"] == "admin_authorized_pending_explicit_enable"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda package: package.update(operation="update"),
        lambda package: package.update(scope_immutable=False),
        lambda package: package.update(candidate_hash="sha256:changed"),
        lambda package: package["receipt"].update(cron_job_id="different-job"),
        lambda package: (
            package["job"].update(id="../escape"),
            package.update(job_id="../escape"),
            package["receipt"].update(cron_job_id="../escape"),
        ),
    ],
)
def test_resume_envelope_mismatch_fails_before_hook_or_store(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutate,
) -> None:
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *args, **kwargs: pytest.fail("invalid resume reached governance hook"),
    )
    package = _resume_package(_candidate(), "create")
    mutate(package)

    with pytest.raises(CronJobGovernanceError, match="invalid resume package"):
        create_job(prompt=None, schedule="", governance_resume=package)

    assert _store_bytes(governed_store) is None


def test_resume_package_candidate_overrides_parallel_caller_fields(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _candidate()
    package = _resume_package(candidate, "create")

    def invoke(name: str, **kwargs: Any) -> dict[str, Any]:
        assert kwargs["candidate"]["name"] == candidate["name"]
        assert kwargs["candidate"]["prompt"] == candidate["prompt"]
        return _mandatory_report([
            _allow_decision(resume_id=package["receipt"]["receipt_id"])
        ])

    monkeypatch.setattr("hermes_cli.plugins.invoke_mandatory_hook", invoke)

    created = create_job(
        prompt="caller override",
        schedule="every 5m",
        name="caller override",
        governance_resume=package,
    )

    assert created["name"] == candidate["name"]
    assert created["prompt"] == candidate["prompt"]
    assert len(load_jobs()) == 1


def test_v1_exact_update_preserves_empty_prior_hash_compatibility(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda _name, **_kwargs: _mandatory_report([_allow_decision()]),
    )
    original = create_job(prompt="original", schedule="every 1h")
    package = _resume_package({**original, "prompt": "v1 resumed"}, "update")
    package["receipt"]["prior_job_hash"] = ""
    package["receipt"]["receipt_id"] = _cron_stable_hash({
        field: copy.deepcopy(package["receipt"].get(field))
        for field in package["receipt"]
        if field != "receipt_id"
    })
    resume_id = package["receipt"]["receipt_id"]
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda _name, **_kwargs: _mandatory_report([
            _allow_decision(resume_id=resume_id)
        ]),
    )

    updated = update_job(original["id"], {}, governance_resume=package)

    assert updated is not None
    assert updated["prompt"] == "v1 resumed"
    assert updated["state"] == "paused"


def test_generic_non_hak_plugin_can_block_persistence(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("HERMES_CRON_CREATION_GOVERNANCE_REQUIRED")
    monkeypatch.setattr("hermes_cli.plugins.has_hook", lambda name: True)
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda name, **kwargs: _mandatory_report([
            {"action": "block", "reason": "generic_policy", "state": "rejected"}
        ]),
    )

    with pytest.raises(CronJobGovernanceError, match="generic_policy"):
        create_job(prompt="generic plugin candidate", schedule="every 1h")

    assert _store_bytes(governed_store) is None


def test_governed_write_fails_closed_without_cross_process_lock(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("cron.jobs.fcntl", None)
    monkeypatch.setattr("cron.jobs.msvcrt", None)
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda name, **kwargs: _mandatory_report([_allow_decision()]),
    )

    with pytest.raises(CronJobGovernanceError, match="strict jobs lock unavailable"):
        create_job(prompt="strict lock candidate", schedule="every 1h")

    assert _store_bytes(governed_store) is None


def test_exact_resume_replay_returns_existing_job_once(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _candidate()
    package = _resume_package(candidate, "create")
    resume_id = package["receipt"]["receipt_id"]

    def invoke(name: str, **kwargs: Any) -> dict[str, Any]:
        assert name == "pre_cron_job_persist"
        if kwargs["existing_jobs"]:
            return _mandatory_report([
                {"action": "allow", "persist_disposition": "already_persisted"}
            ])
        return _mandatory_report([_allow_decision(resume_id=resume_id)])

    monkeypatch.setattr("hermes_cli.plugins.invoke_mandatory_hook", invoke)

    first = create_job(prompt=None, schedule="", governance_resume=package)
    second = create_job(prompt=None, schedule="", governance_resume=package)

    assert first["id"] == second["id"] == candidate["id"]
    assert len(load_jobs()) == 1


def test_mandatory_callback_failure_fails_closed_with_provenance_and_effect(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    effect = {"kind": "test/v1", "frame_id": "frame-callback-failure"}
    failure = {
        "plugin": "policy/crashing",
        "plugin_name": "crashing",
        "source": "user",
        "callback": "decide",
        "module": "policy.crashing",
        "hook": "pre_cron_job_persist",
        "exception_class": "RuntimeError",
    }
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: _mandatory_report(
            [{"action": "block", "reason": "secondary", "notification_effect": effect}],
            failures=[failure],
            callback_count=2,
        ),
    )
    observed: list[dict[str, Any]] = []
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda name, **kwargs: observed.append({"name": name, **copy.deepcopy(kwargs)}) or [],
    )

    with pytest.raises(CronJobGovernanceError) as exc_info:
        create_job(prompt="must not persist", schedule="every 1h")

    assert exc_info.value.decision == {
        "action": "block",
        "reason": "governance_callback_failed",
        "state": "review_required",
        "callback_failures": [failure],
        "post_persist_effects": [effect],
    }
    assert exc_info.value.payload()["callback_failures"] == [failure]
    assert _store_bytes(governed_store) is None
    assert observed == [{
        "name": "post_cron_job_persist",
        "operation": "create",
        "notification_effect": effect,
    }]


@pytest.mark.parametrize("tamper", ["job_only", "job_and_spec", "extra_field"])
def test_resume_hashes_and_exact_shape_are_bound_before_governance(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
    tamper: str,
) -> None:
    package = _resume_package(_candidate(), "create")
    if tamper == "extra_field":
        package["parallel_override"] = "not-authorized"
    else:
        package["job"]["prompt"] = "mutated after approval"
        if tamper == "job_and_spec":
            new_hash = _cron_persist_spec_hash("create", package["job"])
            package["persist_spec_hash"] = new_hash
            package["receipt"]["persist_spec_hash"] = new_hash
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: pytest.fail("tampered resume reached governance"),
    )

    with pytest.raises(CronJobGovernanceError, match="invalid resume package"):
        create_job(prompt=None, schedule="", governance_resume=package)

    assert _store_bytes(governed_store) is None


def test_resume_does_not_repair_store_before_governance(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = governed_store / "cron" / "jobs.json"
    path.parent.mkdir(parents=True)
    raw = json.dumps([{"id": "legacy", "name": "legacy"}]).encode()
    path.write_bytes(raw)
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: pytest.fail("repair-required resume reached governance"),
    )

    with pytest.raises(CronJobGovernanceError, match="jobs store requires shape repair"):
        create_job(
            prompt=None,
            schedule="",
            governance_resume=_resume_package(_candidate(), "create"),
        )

    assert path.read_bytes() == raw


def test_resume_create_does_not_repair_empty_control_character_store(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = governed_store / "cron" / "jobs.json"
    path.parent.mkdir(parents=True)
    raw = b'{"jobs":[],"note":"bad\x01metadata"}'
    path.write_bytes(raw)
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: pytest.fail("repair-required resume reached governance"),
    )

    with pytest.raises(CronJobGovernanceError, match="control-character repair"):
        create_job(
            prompt=None,
            schedule="",
            governance_resume=_resume_package(_candidate(), "create"),
        )

    assert path.read_bytes() == raw


@pytest.mark.parametrize(
    "field",
    ["operation", "source_route", "join_keys", "creation_governance_receipt"],
)
def test_hook_owned_binding_fields_cannot_be_mutated_by_callers(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: _mandatory_report([_allow_decision()]),
    )
    job = create_job(prompt="approved", schedule="every 1h")
    before = _store_bytes(governed_store)

    with pytest.raises(ValueError, match="cannot be updated"):
        update_job(job["id"], {field: {"forged": True}})

    assert _store_bytes(governed_store) == before


def test_caller_binding_field_addition_is_governed_not_plain_mutation(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("HERMES_CRON_CREATION_GOVERNANCE_REQUIRED")
    monkeypatch.setattr("hermes_cli.plugins.has_hook", lambda _name: False)
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: _mandatory_report([], callback_count=0),
    )
    job = create_job(prompt="plain", schedule="every 1h")
    before = _store_bytes(governed_store)

    with pytest.raises(CronJobGovernanceError, match="missing or ambiguous"):
        update_job(job["id"], {"authorized_behavior_ref": "behavior.injected"})

    assert _store_bytes(governed_store) == before


def test_update_lock_requirement_uses_merged_candidate(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: _mandatory_report([_allow_decision()]),
    )
    job = create_job(prompt="before", schedule="every 1h")
    import cron.jobs as jobs_module

    original_lock = jobs_module._jobs_lock
    lock_modes: list[bool] = []

    @contextlib.contextmanager
    def recording_lock(*, require_cross_process: bool = False):
        lock_modes.append(require_cross_process)
        with original_lock(require_cross_process=require_cross_process):
            yield

    monkeypatch.setattr(jobs_module, "_jobs_lock", recording_lock)
    updated = update_job(job["id"], {"prompt": "after"})
    assert updated is not None
    assert lock_modes == [False, True]

    lock_modes.clear()
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: pytest.fail("semantic no-op reached governance"),
    )
    update_job(job["id"], {"prompt": "after"})
    assert lock_modes == [False]


def test_governance_refresh_preserves_legacy_skill_shape_and_reissues_receipt(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: _mandatory_report([_allow_decision()]),
    )
    job = create_job(prompt="approved", schedule="every 1h")
    legacy = {**job, "skill": "legacy-skill", "next_run_at": None}
    legacy.pop("skills", None)
    save_jobs([legacy])
    seen: dict[str, Any] = {}

    def refresh(name: str, **kwargs: Any) -> dict[str, Any]:
        assert name == "pre_cron_job_persist"
        seen.update(copy.deepcopy(kwargs["candidate"]))
        return _mandatory_report([
            _allow_decision(receipt_id="sha256:refreshed")
        ])

    monkeypatch.setattr("hermes_cli.plugins.invoke_mandatory_hook", refresh)
    updated = update_job(job["id"], {}, governance_refresh=True)

    assert seen["skill"] == "legacy-skill"
    assert "skills" not in seen
    assert updated is not None
    assert updated["creation_governance_receipt"]["receipt_id"] == "sha256:refreshed"
    stored = json.loads((governed_store / "cron" / "jobs.json").read_text())["jobs"][0]
    assert stored["skill"] == "legacy-skill"
    assert "skills" not in stored
    assert stored["next_run_at"] is None


@pytest.mark.parametrize("shape", ["bare_list", "control_character"])
def test_governance_refresh_never_repairs_store_before_precondition(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
    shape: str,
) -> None:
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: _mandatory_report([_allow_decision()]),
    )
    job = create_job(prompt="approved", schedule="every 1h")
    path = governed_store / "cron" / "jobs.json"
    raw = (
        json.dumps([job], ensure_ascii=False).encode()
        if shape == "bare_list"
        else json.dumps({"jobs": [job]}, ensure_ascii=False)
        .replace("approved", "bad\x01")
        .encode()
    )
    path.write_bytes(raw)
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: pytest.fail("repair-required store reached governance"),
    )

    with pytest.raises(CronJobGovernanceError, match="jobs store requires"):
        update_job(job["id"], {}, governance_refresh=True)

    assert path.read_bytes() == raw


def test_verification_retirement_requires_exact_precondition_and_governance(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: _mandatory_report([_allow_decision()]),
    )
    job = create_job(prompt="approved", schedule="every 1h")
    command = f"HERMES_HOME={governed_store.resolve()} hermes cron status {job['id']}"
    job["verification_command"] = command
    job["verification_command_mode"] = None
    job["creation_governance_receipt"].update({
        "profile_id": "default",
        "cron_job_id": job["id"],
    })
    save_jobs([job])
    request = {
        "schema_version": "cron-verification-retirement/v1",
        "profile_id": "default",
        "job_revision": job["creation_governance_receipt"]["receipt_id"],
        "command_sha256": "sha256:" + hashlib.sha256(command.encode()).hexdigest(),
    }
    observed: dict[str, Any] = {}

    def retire(name: str, **kwargs: Any) -> dict[str, Any]:
        observed.update(copy.deepcopy(kwargs["candidate"]))
        return _mandatory_report([_allow_decision(receipt_id="sha256:retired")])

    monkeypatch.setattr("hermes_cli.plugins.invoke_mandatory_hook", retire)
    updated = update_job(
        job["id"],
        {},
        deprecated_verification_retirement=request,
    )
    assert "verification_command" not in observed
    assert updated is not None and "verification_command" not in updated

    save_jobs([job])
    before = _store_bytes(governed_store)
    request["command_sha256"] = "sha256:" + "0" * 64
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: pytest.fail("mismatched retirement reached governance"),
    )
    with pytest.raises(CronJobGovernanceError, match="precondition mismatch"):
        update_job(
            job["id"],
            {},
            deprecated_verification_retirement=request,
        )
    assert _store_bytes(governed_store) == before


def test_verification_retirement_uses_active_named_profile_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cron.jobs import use_cron_store

    named_profile = tmp_path / "profiles" / "atlas"
    process_home = tmp_path / "process-default"
    monkeypatch.setenv("HERMES_HOME", str(process_home))
    monkeypatch.setenv("HERMES_CRON_CREATION_GOVERNANCE_REQUIRED", "1")
    monkeypatch.setattr("hermes_cli.plugins.discover_plugins", lambda: None)
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: _mandatory_report([_allow_decision()]),
    )

    with use_cron_store(named_profile):
        job = create_job(prompt="approved", schedule="every 1h")
        command = (
            f"HERMES_HOME={named_profile.resolve()} hermes cron status {job['id']}"
        )
        job["verification_command"] = command
        job["verification_command_mode"] = None
        job["creation_governance_receipt"].update(
            {"profile_id": "atlas", "cron_job_id": job["id"]}
        )
        save_jobs([job])
        request = {
            "schema_version": "cron-verification-retirement/v1",
            "profile_id": "atlas",
            "job_revision": job["creation_governance_receipt"]["receipt_id"],
            "command_sha256": "sha256:"
            + hashlib.sha256(command.encode()).hexdigest(),
        }
        updated = update_job(
            job["id"],
            {},
            deprecated_verification_retirement=request,
        )

    assert updated is not None
    assert "verification_command" not in updated


def test_governance_refresh_cannot_replace_active_signed_revision(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: _mandatory_report([_allow_decision()]),
    )
    job = create_job(prompt="approved", schedule="every 1h")
    save_jobs([{**job, "active_run_outcome_claim": {"run_id": "cron-run:active"}}])
    before = _store_bytes(governed_store)
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: pytest.fail("active revision reached governance"),
    )

    with pytest.raises(CronJobGovernanceError, match="signed run is active"):
        update_job(job["id"], {}, governance_refresh=True)

    assert _store_bytes(governed_store) == before


def test_ordinary_material_update_cannot_replace_active_signed_revision(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: _mandatory_report([_allow_decision()]),
    )
    job = create_job(prompt="approved", schedule="every 1h")
    save_jobs([{**job, "active_run_outcome_claim": {"run_id": "cron-run:active"}}])
    before = _store_bytes(governed_store)
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: pytest.fail("active revision reached governance"),
    )

    with pytest.raises(CronJobGovernanceError, match="signed run is active"):
        update_job(job["id"], {"prompt": "changed while active"})

    assert _store_bytes(governed_store) == before


def test_resume_update_cannot_replace_active_signed_revision(
    governed_store: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: _mandatory_report([_allow_decision()]),
    )
    job = create_job(prompt="approved", schedule="every 1h")
    save_jobs([{**job, "active_run_outcome_claim": {"run_id": "cron-run:active"}}])
    before = _store_bytes(governed_store)
    package = _resume_package({**job, "prompt": "resumed change"}, "update")
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: pytest.fail("active resume reached governance"),
    )

    with pytest.raises(CronJobGovernanceError, match="signed run is active"):
        update_job(job["id"], {}, governance_resume=package)

    assert _store_bytes(governed_store) == before


@pytest.mark.parametrize("no_agent", [True, False])
def test_runtime_governance_precedes_script_and_agent_paths(
    monkeypatch: pytest.MonkeyPatch,
    no_agent: bool,
) -> None:
    from cron.jobs import CronRuntimeAdmissionError
    from cron.scheduler import run_job

    def deny(_job: dict[str, Any]) -> None:
        raise CronRuntimeAdmissionError(
            "denied before execution",
            decision={"action": "block", "reason": "runtime_denied"},
        )

    monkeypatch.setattr("cron.jobs._apply_cron_runtime_governance", deny)
    monkeypatch.setattr(
        "cron.scheduler._run_job_script_with_claim_heartbeat",
        lambda *_args, **_kwargs: pytest.fail("script ran before admission"),
    )
    job = {"id": "runtime-job", "name": "runtime", "no_agent": no_agent}
    if no_agent:
        job["script"] = "never.py"

    success, document, response, error = run_job(job)
    assert success is False
    assert response == ""
    assert error == "denied before execution"
    assert "reason: runtime_denied" in document


def test_runtime_mandatory_callback_failure_keeps_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cron.jobs import CronRuntimeAdmissionError, _apply_cron_runtime_governance

    failure = {
        "plugin": "policy/runtime",
        "source": "user",
        "callback": "admit",
        "hook": "pre_cron_job_run",
        "exception_class": "TimeoutError",
    }
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_mandatory_hook",
        lambda *_args, **_kwargs: _mandatory_report(
            [], failures=[failure], callback_count=1
        ),
    )

    with pytest.raises(CronRuntimeAdmissionError) as exc_info:
        _apply_cron_runtime_governance({
            "id": "runtime-job",
            "creation_governance_receipt": _receipt(),
        })

    assert exc_info.value.decision["reason"] == "runtime_governance_callback_failed"
    assert exc_info.value.decision["callback_failures"] == [failure]
