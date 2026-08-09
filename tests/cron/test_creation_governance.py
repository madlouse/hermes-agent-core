"""Cron persistence governance regression tests (#694)."""

from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import json
import os
import threading
from types import SimpleNamespace

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from fastapi import HTTPException

from cron.jobs import (
    _cron_governance_material_changed,
    _cron_update_may_change_governance_material,
    CronJobGovernanceError,
    CronRuntimeAdmissionError,
    create_job,
    load_jobs,
    save_jobs,
    update_job,
    use_cron_store,
)
from gateway.config import PlatformConfig
from gateway.platforms.api_server import APIServerAdapter, cors_middleware
from hermes_cli.cron import cron_create, cron_edit
from tools.blueprints import (
    BlueprintSpec,
    create_blueprint_job,
    register_blueprint_suggestion,
)
import tools.cronjob_tools as cronjob_tools_module
from tools.cronjob_tools import cronjob
from tools.registry import registry


@pytest.fixture()
def governed_store(tmp_path, monkeypatch):
    monkeypatch.setattr("cron.jobs.CRON_DIR", tmp_path / "cron")
    monkeypatch.setattr("cron.jobs.JOBS_FILE", tmp_path / "cron" / "jobs.json")
    monkeypatch.setattr("cron.jobs.OUTPUT_DIR", tmp_path / "cron" / "output")
    (tmp_path / "cron").mkdir()
    (tmp_path / "cron" / ".jobs.lock").touch()
    monkeypatch.setenv("HERMES_CRON_CREATION_GOVERNANCE_REQUIRED", "1")
    monkeypatch.setattr("hermes_cli.plugins.discover_plugins", lambda: None)
    return tmp_path


def allow_decision(**overrides):
    receipt = {
        "schema_version": "cron-creation-governance/v1",
        "receipt_id": "sha256:receipt",
        "candidate_hash": "sha256:candidate",
    }
    patch = {
        "operation": "new",
        "authorized_behavior_ref": "behavior.test",
        "process_charter_ref": "process.test",
        "approval_evidence_ref": "approval.test",
        "read_scope_ref": "read.test",
        "disclosure_policy_ref": "disclosure.test",
        "risk_tier": "low",
        "implementation_categories": ["cron"],
        "source_route": {"kind": "operator"},
        "join_keys": {"candidate_hash": "sha256:candidate"},
        "creation_governance_receipt": receipt,
    }
    patch.update(overrides)
    return {"action": "allow", "job_patch": patch}


def block_decision(
    reason="missing_behavior_binding",
    state="unbound_job_review",
    **overrides,
):
    decision = {"action": "block", "reason": reason, "state": state}
    decision.update(overrides)
    return [decision]


def api_jobs_app(adapter):
    app = web.Application(middlewares=[cors_middleware])
    app.router.add_post("/api/jobs", adapter._handle_create_job)
    app.router.add_patch("/api/jobs/{job_id}", adapter._handle_update_job)
    return app


def test_direct_create_blocked_before_jobs_json_changes(governed_store, monkeypatch):
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *args, **kwargs: [{"action": "block", "reason": "missing_behavior_binding", "state": "unbound_job_review"}],
    )

    with pytest.raises(CronJobGovernanceError, match="was not saved"):
        create_job(prompt="blocked", schedule="every 1h")

    assert load_jobs() == []
    assert not (governed_store / "cron" / "jobs.json").exists()


def test_agent_cronjob_tool_cannot_bypass_same_gate(governed_store, monkeypatch):
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *args, **kwargs: [{"action": "block", "reason": "group_source_requires_admin_dm", "state": "frozen_candidate_required"}],
    )

    result = json.loads(cronjob(action="create", prompt="blocked", schedule="every 1h"))

    assert result["success"] is False
    assert "was not saved" in result["error"]
    assert load_jobs() == []


def test_agent_cronjob_update_cannot_bypass_same_gate(governed_store, monkeypatch):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    before = (governed_store / "cron" / "jobs.json").read_bytes()
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *args, **kwargs: block_decision("candidate_hash_mismatch", "frozen_candidate_required"),
    )

    result = json.loads(cronjob(action="update", job_id=job["id"], schedule="every 2h"))

    assert result["success"] is False
    assert "was not saved" in result["error"]
    assert (governed_store / "cron" / "jobs.json").read_bytes() == before


def test_valid_guarded_job_persists_hook_minted_receipt(governed_store, monkeypatch):
    observed = {}

    def invoke_hook(*args, **kwargs):
        capability = kwargs["core_jobs_lock_capability"]
        snapshot = __import__("cron.jobs", fromlist=["jobs"])._validate_jobs_lock_capability(
            capability
        )
        assert snapshot is not None
        observed["capability"] = capability
        return [allow_decision()]

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", invoke_hook)

    job = create_job(
        prompt="approved",
        schedule="every 1h",
        authorized_behavior_ref="behavior.test",
        implementation_categories=["cron"],
    )

    assert job["creation_governance_receipt"]["receipt_id"] == "sha256:receipt"
    assert load_jobs()[0]["authorized_behavior_ref"] == "behavior.test"
    assert __import__("cron.jobs", fromlist=["jobs"])._validate_jobs_lock_capability(
        observed["capability"]
    ) is None


def test_rejected_material_update_leaves_original_job_unchanged(governed_store, monkeypatch):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    before = (governed_store / "cron" / "jobs.json").read_bytes()
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *args, **kwargs: [{"action": "block", "reason": "candidate_hash_mismatch", "state": "frozen_candidate_required"}],
    )

    with pytest.raises(CronJobGovernanceError):
        update_job(job["id"], {"schedule": "every 2h"})

    assert (governed_store / "cron" / "jobs.json").read_bytes() == before


def test_semantically_unchanged_material_update_preserves_receipt_without_hook_churn(
    governed_store,
    monkeypatch,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    receipt = json.loads(json.dumps(job["creation_governance_receipt"]))
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *args, **kwargs: pytest.fail("semantic no-op called pre_cron_job_persist"),
    )

    updated = update_job(job["id"], {"schedule": "every 1h"})

    assert updated["creation_governance_receipt"] == receipt
    assert load_jobs()[0]["creation_governance_receipt"] == receipt


def test_explicit_governance_refresh_reissues_unchanged_job_receipt(
    governed_store,
    monkeypatch,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    refreshed_receipt = {
        "schema_version": "cron-creation-governance/v1",
        "receipt_id": "sha256:refreshed",
        "candidate_hash": "sha256:candidate",
    }
    observed = {}

    def invoke_hook(name, **kwargs):
        observed["name"] = name
        observed["candidate"] = kwargs["candidate"]
        return [allow_decision(creation_governance_receipt=refreshed_receipt)]

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", invoke_hook)

    updated = update_job(job["id"], {}, governance_refresh=True)

    assert observed["name"] == "pre_cron_job_persist"
    assert observed["candidate"]["id"] == job["id"]
    assert updated["creation_governance_receipt"] == refreshed_receipt
    assert load_jobs()[0]["creation_governance_receipt"] == refreshed_receipt


def test_blocked_governance_refresh_leaves_job_store_unchanged(
    governed_store,
    monkeypatch,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    before = (governed_store / "cron" / "jobs.json").read_bytes()
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *args, **kwargs: block_decision("candidate_refresh_requires_admin_dm", "frozen_candidate_required"),
    )

    with pytest.raises(CronJobGovernanceError, match="candidate_refresh_requires_admin_dm"):
        update_job(job["id"], {}, governance_refresh=True)

    assert (governed_store / "cron" / "jobs.json").read_bytes() == before


def test_governance_refresh_rejects_caller_supplied_resume_package(governed_store):
    with pytest.raises(CronJobGovernanceError, match="mutually exclusive"):
        update_job(
            "job-1",
            {},
            governance_resume={"job": {"id": "job-1"}, "receipt": {"receipt_id": "resume"}},
            governance_refresh=True,
        )


def test_governance_refresh_cannot_replace_active_signed_revision(
    governed_store,
    monkeypatch,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    save_jobs([{**job, "active_run_outcome_claim": {"run_id": "cron-run:" + "4" * 32}}])
    before = (governed_store / "cron" / "jobs.json").read_bytes()
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *args, **kwargs: pytest.fail("active refresh reached governance hook"),
    )

    with pytest.raises(CronJobGovernanceError, match="signed run is active"):
        update_job(job["id"], {}, governance_refresh=True)

    assert (governed_store / "cron" / "jobs.json").read_bytes() == before


def test_cronjob_tool_forwards_explicit_governance_refresh(monkeypatch):
    captured = {}
    job = {"id": "job-1", "name": "job", "schedule": {"display": "every 1h"}}
    monkeypatch.setattr("tools.cronjob_tools.resolve_job_ref", lambda _job_id, **_kwargs: job)
    monkeypatch.setattr("tools.cronjob_tools.update_job", lambda *args, **kwargs: captured.update(kwargs) or job)
    monkeypatch.setattr("tools.cronjob_tools._notify_provider_jobs_changed_safe", lambda: None)

    result = json.loads(cronjob(action="update", job_id="job-1", governance_refresh=True))

    assert result["success"] is True
    assert captured["governance_refresh"] is True


def test_cronjob_tool_still_rejects_empty_update_without_refresh(monkeypatch):
    job = {"id": "job-1", "name": "job", "schedule": {"display": "every 1h"}}
    monkeypatch.setattr("tools.cronjob_tools.resolve_job_ref", lambda _job_id, **_kwargs: job)
    monkeypatch.setattr(
        "tools.cronjob_tools.update_job",
        lambda *_args, **_kwargs: pytest.fail("empty update reached persistence"),
    )

    result = json.loads(cronjob(action="update", job_id="job-1"))

    assert result == {"success": False, "error": "No updates provided."}


def _verification_retirement(job, command):
    return {
        "schema_version": "cron-verification-retirement/v1",
        "profile_id": job["creation_governance_receipt"]["profile_id"],
        "job_revision": job["creation_governance_receipt"]["receipt_id"],
        "command_sha256": "sha256:" + hashlib.sha256(command.encode("utf-8")).hexdigest(),
    }


def _job_with_deprecated_verification(job, command):
    return {
        **job,
        "verification_command": command,
        "creation_governance_receipt": {
            **job["creation_governance_receipt"],
            "profile_id": "default",
            "cron_job_id": job["id"],
        },
    }


def test_exact_deprecated_verification_retirement_uses_existing_governance(
    governed_store,
    monkeypatch,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    command = f"HERMES_HOME={governed_store} hermes cron status {job['id']}"
    job = _job_with_deprecated_verification(job, command)
    save_jobs([job])
    observed = {}

    def invoke_hook(name, **kwargs):
        observed["candidate"] = kwargs["candidate"]
        return [allow_decision()]

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", invoke_hook)
    updated = update_job(
        job["id"],
        {},
        deprecated_verification_retirement=_verification_retirement(job, command),
    )

    assert "verification_command" not in observed["candidate"]
    assert "verification_command" not in updated
    assert "verification_command" not in load_jobs()[0]


@pytest.mark.parametrize("field", ["profile_id", "job_revision", "command_sha256"])
def test_deprecated_verification_retirement_mismatch_is_zero_write(
    governed_store,
    monkeypatch,
    field,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    command = f"HERMES_HOME={governed_store} hermes cron status {job['id']}"
    job = _job_with_deprecated_verification(job, command)
    save_jobs([job])
    before = (governed_store / "cron" / "jobs.json").read_bytes()
    request = _verification_retirement(job, command)
    request[field] = "sha256:" + "0" * 64 if field != "profile_id" else "other"
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *_args, **_kwargs: pytest.fail("mismatched retirement reached governance"),
    )

    with pytest.raises(CronJobGovernanceError, match="precondition mismatch"):
        update_job(job["id"], {}, deprecated_verification_retirement=request)

    assert (governed_store / "cron" / "jobs.json").read_bytes() == before


@pytest.mark.parametrize("variant", ["custom_command", "cross_job_receipt"])
def test_deprecated_verification_retirement_rejects_unknown_legacy_shape(
    governed_store,
    monkeypatch,
    variant,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    command = f"HERMES_HOME={governed_store} hermes cron status {job['id']}"
    if variant == "custom_command":
        command = "python verify.py"
    job = _job_with_deprecated_verification(job, command)
    if variant == "cross_job_receipt":
        job["creation_governance_receipt"]["cron_job_id"] = "other-job"
    save_jobs([job])
    before = (governed_store / "cron" / "jobs.json").read_bytes()
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *_args, **_kwargs: pytest.fail("unknown verifier reached governance"),
    )

    with pytest.raises(CronJobGovernanceError, match="precondition mismatch"):
        update_job(
            job["id"],
            {},
            deprecated_verification_retirement=_verification_retirement(job, command),
        )

    assert (governed_store / "cron" / "jobs.json").read_bytes() == before


def test_governance_refresh_fails_closed_when_governance_is_unavailable(
    governed_store,
    monkeypatch,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    save_jobs([{key: value for key, value in job.items() if key != "creation_governance_receipt"}])
    before = (governed_store / "cron" / "jobs.json").read_bytes()
    monkeypatch.delenv("HERMES_CRON_CREATION_GOVERNANCE_REQUIRED", raising=False)

    with pytest.raises(CronJobGovernanceError, match="governance unavailable"):
        update_job(job["id"], {}, governance_refresh=True)

    assert (governed_store / "cron" / "jobs.json").read_bytes() == before


@pytest.mark.parametrize("shape", ["bare_list", "control_character"])
def test_governance_refresh_does_not_auto_repair_store_before_decision(
    governed_store,
    monkeypatch,
    shape,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    path = governed_store / "cron" / "jobs.json"
    if shape == "bare_list":
        raw = json.dumps([job], ensure_ascii=False).encode("utf-8")
    else:
        raw = json.dumps({"jobs": [job]}, ensure_ascii=False).replace("approved", "bad\x01").encode("utf-8")
    path.write_bytes(raw)
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *_args, **_kwargs: pytest.fail("repair-required store reached governance"),
    )

    with pytest.raises(CronJobGovernanceError, match="jobs store requires"):
        update_job(job["id"], {}, governance_refresh=True)

    assert path.read_bytes() == raw


def test_cronjob_tool_refresh_does_not_repair_store_before_resolution(
    governed_store,
):
    path = governed_store / "cron" / "jobs.json"
    raw = json.dumps([{"id": "legacy-job", "name": "legacy"}]).encode("utf-8")
    path.write_bytes(raw)

    result = json.loads(
        cronjob(action="update", job_id="legacy-job", governance_refresh=True)
    )

    assert result["success"] is False
    assert "jobs store requires shape repair" in result["error"]
    assert path.read_bytes() == raw


def test_registered_cronjob_refresh_does_not_repair_store_before_resolution(
    governed_store,
):
    path = governed_store / "cron" / "jobs.json"
    raw = json.dumps([{"id": "legacy-job", "name": "legacy"}]).encode("utf-8")
    path.write_bytes(raw)

    result = json.loads(
        cronjob_tools_module.registry._tools["cronjob"].handler(
            {
                "action": "update",
                "job_id": "legacy-job",
                "governance_refresh": True,
            },
            task_id="task-1",
        )
    )

    assert result["success"] is False
    assert "jobs store requires shape repair" in result["error"]
    assert path.read_bytes() == raw


def test_ordinary_load_still_repairs_bare_list_store(governed_store):
    path = governed_store / "cron" / "jobs.json"
    raw_job = {"id": "legacy-job", "prompt": "legacy"}
    path.write_text(json.dumps([raw_job]), encoding="utf-8")

    assert load_jobs() == [raw_job]
    repaired = json.loads(path.read_text(encoding="utf-8"))
    assert repaired["jobs"] == [raw_job]
    assert repaired["updated_at"]


def test_deprecated_verification_retirement_rejects_refresh_combination(governed_store):
    with pytest.raises(CronJobGovernanceError, match="mutually exclusive"):
        update_job(
            "job-1",
            {},
            governance_refresh=True,
            deprecated_verification_retirement={"schema_version": "cron-verification-retirement/v1"},
        )


@pytest.mark.parametrize("operation", ["refresh", "retirement"])
def test_special_governance_operation_rejects_ordinary_updates_zero_write(
    governed_store,
    monkeypatch,
    operation,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    before = (governed_store / "cron" / "jobs.json").read_bytes()
    kwargs = {"governance_refresh": True}
    if operation == "retirement":
        kwargs = {
            "deprecated_verification_retirement": {
                "schema_version": "cron-verification-retirement/v1"
            }
        }

    with pytest.raises(CronJobGovernanceError, match="otherwise unchanged Job"):
        update_job(job["id"], {"prompt": "changed"}, **kwargs)

    assert (governed_store / "cron" / "jobs.json").read_bytes() == before


@pytest.mark.parametrize("operation", ["refresh", "retirement"])
def test_special_governance_operation_skips_ordinary_normalization(
    governed_store,
    monkeypatch,
    operation,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    legacy = {**job, "skill": "legacy-skill", "next_run_at": None}
    legacy.pop("skills", None)
    kwargs = {"governance_refresh": True}
    if operation == "retirement":
        command = f"HERMES_HOME={governed_store} hermes cron status {job['id']}"
        legacy = _job_with_deprecated_verification(legacy, command)
        kwargs = {
            "deprecated_verification_retirement": _verification_retirement(legacy, command)
        }
    save_jobs([legacy])

    updated = update_job(job["id"], {}, **kwargs)
    stored_payload = json.loads((governed_store / "cron" / "jobs.json").read_text())
    stored = stored_payload[0] if isinstance(stored_payload, list) else stored_payload["jobs"][0]

    assert updated["skill"] == "legacy-skill"
    assert "skills" not in stored
    assert stored["next_run_at"] is None
    assert stored["enabled"] is legacy["enabled"]
    assert stored["state"] == legacy["state"]
    if operation == "retirement":
        assert "verification_command" not in stored


def test_ordinary_update_still_computes_missing_next_run(
    governed_store,
    monkeypatch,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    save_jobs([{**job, "next_run_at": None}])
    monkeypatch.setattr("cron.jobs.compute_next_run", lambda _schedule: "2026-08-06T01:00:00+00:00")

    updated = update_job(job["id"], {"prompt": "changed"})

    assert updated["next_run_at"] == "2026-08-06T01:00:00+00:00"


def test_cronjob_tool_forwards_verification_retirement(monkeypatch):
    captured = {}
    job = {"id": "job-1", "name": "job", "schedule": {"display": "every 1h"}}
    request = {
        "schema_version": "cron-verification-retirement/v1",
        "profile_id": "yuange",
        "job_revision": "sha256:" + "1" * 64,
        "command_sha256": "sha256:" + "2" * 64,
    }
    monkeypatch.setattr("tools.cronjob_tools.resolve_job_ref", lambda _job_id, **_kwargs: job)
    monkeypatch.setattr("tools.cronjob_tools.update_job", lambda *args, **kwargs: captured.update(kwargs) or job)
    monkeypatch.setattr("tools.cronjob_tools._notify_provider_jobs_changed_safe", lambda: None)

    result = json.loads(
        cronjob(action="update", job_id="job-1", deprecated_verification_retirement=request)
    )

    assert result["success"] is True
    assert captured["deprecated_verification_retirement"] == request


def test_registered_cronjob_handler_forwards_governance_controls(monkeypatch):
    captured = {}
    request = {
        "schema_version": "cron-verification-retirement/v1",
        "profile_id": "yuange",
        "job_revision": "sha256:" + "1" * 64,
        "command_sha256": "sha256:" + "2" * 64,
    }
    monkeypatch.setattr(
        cronjob_tools_module,
        "cronjob",
        lambda **kwargs: captured.update(kwargs) or json.dumps({"success": True}),
    )

    result = json.loads(
        cronjob_tools_module.registry._tools["cronjob"].handler(
            {
                "action": "update",
                "job_id": "job-1",
                "governance_refresh": True,
                "deprecated_verification_retirement": request,
            },
            task_id="task-1",
        )
    )

    assert result["success"] is True
    assert captured["governance_refresh"] is True
    assert captured["deprecated_verification_retirement"] == request


@pytest.mark.parametrize("operation", ["refresh", "retirement"])
def test_special_cronjob_operation_does_not_reconcile_provider(monkeypatch, operation):
    job = {"id": "job-1", "name": "job", "schedule": {"display": "every 1h"}}
    monkeypatch.setattr("tools.cronjob_tools.resolve_job_ref", lambda _job_id, **_kwargs: job)
    monkeypatch.setattr("tools.cronjob_tools.update_job", lambda *_args, **_kwargs: job)
    monkeypatch.setattr(
        "tools.cronjob_tools._notify_provider_jobs_changed_safe",
        lambda: pytest.fail("special operation reconciled provider schedules"),
    )
    kwargs = {"governance_refresh": True}
    if operation == "retirement":
        kwargs = {
            "deprecated_verification_retirement": {
                "schema_version": "cron-verification-retirement/v1",
                "profile_id": "yuange",
                "job_revision": "sha256:" + "1" * 64,
                "command_sha256": "sha256:" + "2" * 64,
            }
        }

    result = json.loads(cronjob(action="update", job_id="job-1", **kwargs))

    assert result["success"] is True


def test_ordinary_cronjob_update_still_reconciles_provider(monkeypatch):
    calls = []
    job = {"id": "job-1", "name": "job", "schedule": {"display": "every 1h"}}
    monkeypatch.setattr("tools.cronjob_tools.resolve_job_ref", lambda _job_id, **_kwargs: job)
    monkeypatch.setattr("tools.cronjob_tools.update_job", lambda *_args, **_kwargs: job)
    monkeypatch.setattr(
        "tools.cronjob_tools._notify_provider_jobs_changed_safe",
        lambda: calls.append("changed"),
    )

    result = json.loads(cronjob(action="update", job_id="job-1", prompt="changed"))

    assert result["success"] is True
    assert calls == ["changed"]


def test_material_update_cannot_replace_revision_during_active_signed_run(
    governed_store,
    monkeypatch,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    active = {**job, "active_run_outcome_claim": {"run_id": "cron-run:" + "1" * 32}}
    save_jobs([active])
    before = (governed_store / "cron" / "jobs.json").read_bytes()
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *args, **kwargs: pytest.fail("active revision reached governance hook"),
    )

    with pytest.raises(CronJobGovernanceError, match="signed run is active"):
        update_job(job["id"], {"schedule": "every 2h"})

    assert (governed_store / "cron" / "jobs.json").read_bytes() == before


@pytest.mark.parametrize(
    "updates",
    [
        {"risk_tier": "high"},
        {"process_charter_ref": "process.changed"},
        {"future_definition": {"mode": "strict"}},
    ],
)
def test_v2_definition_update_cannot_bypass_active_signed_revision(
    governed_store,
    monkeypatch,
    updates,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    save_jobs([
        {**job, "active_run_outcome_claim": {"run_id": "cron-run:" + "3" * 32}}
    ])
    before = (governed_store / "cron" / "jobs.json").read_bytes()
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *args, **kwargs: pytest.fail("active v2 revision reached governance hook"),
    )

    with pytest.raises(CronJobGovernanceError, match="signed run is active"):
        update_job(job["id"], updates)

    assert (governed_store / "cron" / "jobs.json").read_bytes() == before


def test_v2_unknown_definition_update_reaches_governance_hook(
    governed_store,
    monkeypatch,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    seen = []

    def allow_update(_name, **kwargs):
        seen.append(kwargs["candidate"]["future_definition"])
        return [allow_decision()]

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", allow_update)

    updated = update_job(job["id"], {"future_definition": {"mode": "strict"}})

    assert seen == [{"mode": "strict"}]
    assert updated["future_definition"] == {"mode": "strict"}


def test_v2_material_comparison_excludes_only_explicit_runtime_state():
    base = {
        "name": "bound",
        "repeat": {"times": 2, "completed": 0, "future_policy": "strict"},
        "last_status": "ok",
        "join_keys": {"candidate_hash": "sha256:old"},
    }

    assert _cron_governance_material_changed(
        base,
        {
            **base,
            "repeat": {**base["repeat"], "completed": 1},
            "last_status": "error",
            "join_keys": {"candidate_hash": "sha256:new"},
        },
    ) is False
    assert _cron_governance_material_changed(
        base,
        {**base, "repeat": {**base["repeat"], "future_policy": "relaxed"}},
    ) is True
    assert _cron_governance_material_changed({}, {}) is False
    assert _cron_update_may_change_governance_material(
        {"last_status": "error", "join_keys": {}, "repeat": {"completed": 1}}
    ) is False
    assert _cron_update_may_change_governance_material(
        {"future_definition": {"mode": "strict"}}
    ) is True


def test_signed_material_update_fails_when_governance_is_unavailable(
    governed_store,
    monkeypatch,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    before = (governed_store / "cron" / "jobs.json").read_bytes()
    monkeypatch.setattr("cron.jobs._cron_creation_governance_expected", lambda: False)

    with pytest.raises(CronJobGovernanceError, match="governance is unavailable"):
        update_job(job["id"], {"risk_tier": "high"})

    assert (governed_store / "cron" / "jobs.json").read_bytes() == before


def test_governance_resume_cannot_replace_revision_during_active_signed_run(
    governed_store,
    monkeypatch,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    active = {**job, "active_run_outcome_claim": {"run_id": "cron-run:" + "2" * 32}}
    save_jobs([active])
    before = (governed_store / "cron" / "jobs.json").read_bytes()
    package = {
        "schema_version": "cron-persist-resume/v1",
        "receipt": {
            "schema_version": "cron-persist-resume/v1",
            "receipt_id": "sha256:resume",
        },
        "job": {**job, "schedule": {"kind": "interval", "seconds": 7200}},
    }
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *args, **kwargs: pytest.fail("active revision reached governance hook"),
    )

    with pytest.raises(CronJobGovernanceError, match="signed run is active"):
        update_job(job["id"], {}, governance_resume=package)

    assert (governed_store / "cron" / "jobs.json").read_bytes() == before


def test_stale_resume_update_fails_before_jobs_json_or_receipt_changes(
    governed_store,
    monkeypatch,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    before = (governed_store / "cron" / "jobs.json").read_bytes()
    seen = []

    def reject_stale(_name, **kwargs):
        seen.append(kwargs["candidate"]["id"])
        return block_decision(
            "resume_update_precondition_mismatch",
            "resume_review_required",
            suppress_pending_action=True,
        )

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", reject_stale)
    package = {
        "schema_version": "cron-persist-resume/v1",
        "receipt": {
            "schema_version": "cron-persist-resume/v1",
            "receipt_id": "sha256:stale-resume",
        },
        "job": {**job, "schedule": {"kind": "interval", "seconds": 7200, "display": "every 2h"}},
    }

    with pytest.raises(CronJobGovernanceError) as exc_info:
        update_job(job["id"], {}, governance_resume=package)

    assert exc_info.value.decision["suppress_pending_action"] is True
    assert seen == [job["id"]]
    assert (governed_store / "cron" / "jobs.json").read_bytes() == before


def test_cli_create_cannot_bypass_shared_persist_gate(governed_store, monkeypatch):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: block_decision())

    result = cron_create(SimpleNamespace(prompt="blocked", schedule="every 1h"))

    assert result == 1
    assert load_jobs() == []
    assert not (governed_store / "cron" / "jobs.json").exists()


def test_cli_edit_cannot_bypass_shared_persist_gate(governed_store, monkeypatch):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    before = (governed_store / "cron" / "jobs.json").read_bytes()
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *args, **kwargs: block_decision("candidate_hash_mismatch", "frozen_candidate_required"),
    )

    result = cron_edit(SimpleNamespace(job_id=job["id"], schedule="every 2h"))

    assert result == 1
    assert (governed_store / "cron" / "jobs.json").read_bytes() == before


def test_cli_governance_refresh_reaches_shared_gate_without_definition_change(
    governed_store,
    monkeypatch,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    before = (governed_store / "cron" / "jobs.json").read_bytes()
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *args, **kwargs: block_decision("refresh_requires_admin_dm", "frozen_candidate_required"),
    )

    result = cron_edit(
        SimpleNamespace(job_id=job["id"], schedule=None, refresh_governance=True)
    )

    assert result == 1
    assert (governed_store / "cron" / "jobs.json").read_bytes() == before


def test_cli_governance_refresh_does_not_repair_store_before_resolution(
    governed_store,
):
    path = governed_store / "cron" / "jobs.json"
    raw = json.dumps([{"id": "legacy-job", "name": "legacy"}]).encode("utf-8")
    path.write_bytes(raw)

    result = cron_edit(
        SimpleNamespace(job_id="legacy-job", schedule=None, refresh_governance=True)
    )

    assert result == 1
    assert path.read_bytes() == raw


def test_cli_verification_retirement_requires_complete_precondition(monkeypatch, capsys):
    job = {"id": "job-1", "name": "job", "schedule": {"display": "every 1h"}}
    monkeypatch.setattr("cron.jobs.resolve_job_ref", lambda _job_id, **_kwargs: job)
    monkeypatch.setattr(
        "hermes_cli.cron._cron_api",
        lambda **_kwargs: pytest.fail("partial retirement reached API"),
    )

    result = cron_edit(
        SimpleNamespace(
            job_id="job-1",
            retire_verification_profile_id="yuange",
            retire_verification_job_revision=None,
            retire_verification_command_sha256=None,
        )
    )

    assert result == 1
    assert "All verification retirement preconditions are required" in capsys.readouterr().out


def test_cli_verification_retirement_forwards_exact_precondition(monkeypatch):
    job = {"id": "job-1", "name": "job", "schedule": {"display": "every 1h"}}
    captured = {}
    monkeypatch.setattr("cron.jobs.resolve_job_ref", lambda _job_id, **_kwargs: job)

    def fake_api(**kwargs):
        captured.update(kwargs)
        return {"success": True, "job": {"job_id": "job-1", "name": "job", "schedule": "every 1h"}}

    monkeypatch.setattr("hermes_cli.cron._cron_api", fake_api)
    revision = "sha256:" + "1" * 64
    command_hash = "sha256:" + "2" * 64

    result = cron_edit(
        SimpleNamespace(
            job_id="job-1",
            retire_verification_profile_id="yuange",
            retire_verification_job_revision=revision,
            retire_verification_command_sha256=command_hash,
        )
    )

    assert result == 0
    assert captured["deprecated_verification_retirement"] == {
        "schema_version": "cron-verification-retirement/v1",
        "profile_id": "yuange",
        "job_revision": revision,
        "command_sha256": command_hash,
    }


def test_gateway_api_create_cannot_bypass_shared_persist_gate(governed_store, monkeypatch):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: block_decision())
    adapter = APIServerAdapter(PlatformConfig(enabled=True))

    async def request_status():
        async with TestClient(TestServer(api_jobs_app(adapter))) as client:
            response = await client.post(
                "/api/jobs",
                json={"name": "blocked", "prompt": "blocked", "schedule": "every 1h"},
            )
            return response.status

    assert asyncio.run(request_status()) == 500
    assert load_jobs() == []
    assert not (governed_store / "cron" / "jobs.json").exists()


def test_gateway_api_update_cannot_bypass_shared_persist_gate(governed_store, monkeypatch):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    before = (governed_store / "cron" / "jobs.json").read_bytes()
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *args, **kwargs: block_decision("candidate_hash_mismatch", "frozen_candidate_required"),
    )
    adapter = APIServerAdapter(PlatformConfig(enabled=True))

    async def request_status():
        async with TestClient(TestServer(api_jobs_app(adapter))) as client:
            response = await client.patch(f"/api/jobs/{job['id']}", json={"schedule": "every 2h"})
            return response.status

    assert asyncio.run(request_status()) == 500
    assert (governed_store / "cron" / "jobs.json").read_bytes() == before


def test_dashboard_create_cannot_bypass_shared_persist_gate(governed_store, monkeypatch):
    from hermes_cli import web_server

    profile_home = governed_store / "dashboard-profile"
    (profile_home / "cron").mkdir(parents=True)
    (profile_home / "cron" / ".jobs.lock").touch()
    monkeypatch.setattr(web_server, "_cron_profile_home", lambda _profile: ("default", profile_home))
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: block_decision())

    with pytest.raises(HTTPException, match="was not saved"):
        web_server._create_cron_job_sync(
            web_server.CronJobCreate(prompt="blocked", schedule="every 1h", name="blocked")
        )

    assert not (profile_home / "cron" / "jobs.json").exists()


def test_dashboard_update_cannot_bypass_shared_persist_gate(governed_store, monkeypatch):
    from hermes_cli import web_server

    profile_home = governed_store / "dashboard-profile"
    (profile_home / "cron").mkdir(parents=True)
    (profile_home / "cron" / ".jobs.lock").touch()
    monkeypatch.setattr(web_server, "_cron_profile_home", lambda _profile: ("default", profile_home))
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    with use_cron_store(profile_home):
        job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    jobs_file = profile_home / "cron" / "jobs.json"
    before = jobs_file.read_bytes()
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *args, **kwargs: block_decision("candidate_hash_mismatch", "frozen_candidate_required"),
    )

    with pytest.raises(CronJobGovernanceError):
        web_server._update_cron_job_sync(
            job["id"],
            web_server.CronJobUpdate(updates={"schedule": "every 2h"}),
            profile="default",
        )

    assert jobs_file.read_bytes() == before


def test_blueprint_helper_cannot_bypass_shared_persist_gate(governed_store, monkeypatch):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: block_decision())
    spec = BlueprintSpec(
        skill_name="guarded-blueprint",
        schedule="every 1h",
        deliver="local",
        prompt="blocked",
    )

    with pytest.raises(CronJobGovernanceError):
        create_blueprint_job(spec)

    assert load_jobs() == []
    assert not (governed_store / "cron" / "jobs.json").exists()


def test_blueprint_command_cannot_bypass_shared_persist_gate(governed_store, monkeypatch):
    from cron import blueprint_catalog
    from hermes_cli import blueprint_cmd

    blueprint = SimpleNamespace(key="guarded", title="Guarded blueprint")
    monkeypatch.setattr(blueprint_cmd, "match_blueprint", lambda _query: (blueprint, []))
    monkeypatch.setattr(
        blueprint_catalog,
        "fill_blueprint",
        lambda *_args, **_kwargs: {
            "name": "guarded",
            "prompt": "blocked",
            "schedule": "every 1h",
            "deliver": "local",
        },
    )
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: block_decision())

    result = blueprint_cmd.handle_blueprint_command("guarded target=test")

    assert "Failed to create the job" in result.text
    assert load_jobs() == []
    assert not (governed_store / "cron" / "jobs.json").exists()


def test_blueprint_suggestion_acceptance_cannot_bypass_shared_persist_gate(
    governed_store,
    monkeypatch,
):
    from cron import suggestions

    monkeypatch.setattr(suggestions, "CRON_DIR", governed_store / "cron")
    monkeypatch.setattr(suggestions, "SUGGESTIONS_FILE", governed_store / "cron" / "suggestions.json")
    spec = BlueprintSpec(
        skill_name="suggested-blueprint",
        schedule="every 1h",
        deliver="local",
        prompt="blocked",
    )
    suggestion = register_blueprint_suggestion(spec)
    assert suggestion is not None
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: block_decision())

    with pytest.raises(CronJobGovernanceError):
        suggestions.accept_suggestion(suggestion["id"])

    assert suggestions.get_suggestion(suggestion["id"])["status"] == "pending"
    assert load_jobs() == []
    assert not (governed_store / "cron" / "jobs.json").exists()


def test_no_agent_job_cannot_bypass_runtime_admission(monkeypatch):
    from cron import jobs as cron_jobs
    from cron import scheduler

    seen = []

    def block(job):
        seen.append(job["id"])
        raise CronRuntimeAdmissionError({
            "schema_version": "cron-runtime-admission/v1",
            "receipt_id": "cron-runtime-admission:" + "0" * 32,
            "stage": "pre_cron_job_run",
            "status": "blocked",
            "reason_code": "missing_behavior_binding",
            "state": "unbound_job_review",
            "exception_class": "runtime_admission_blocked",
            "retryable": False,
            "job_fingerprint": "sha256:" + "a" * 64,
        })

    monkeypatch.setattr(cron_jobs, "_apply_cron_runtime_governance", block)
    monkeypatch.setattr(scheduler, "_run_job_script", lambda _path: pytest.fail("script bypassed runtime gate"))

    success, output, final_response, error = scheduler.run_job(
        {"id": "unbound-script", "name": "script", "no_agent": True, "script": "run.sh"}
    )

    assert seen == ["unbound-script"]
    assert success is False
    assert final_response == ""
    assert error == "Cron job was not run: missing_behavior_binding."
    assert "blocked before execution" in output


def test_parallel_runtime_admission_is_profile_scoped(monkeypatch):
    from cron import jobs as cron_jobs

    marker = "HERMES_CRON_RUNTIME_BINDING_REQUIRED"
    barrier = threading.Barrier(2)
    monkeypatch.delenv(marker, raising=False)
    monkeypatch.setattr(cron_jobs, "_cron_creation_governance_expected", lambda: True)
    monkeypatch.setattr("hermes_cli.plugins.discover_plugins", lambda: None)

    def invoke_hook(event, *, job):
        assert event == "pre_cron_job_run"
        assert marker not in os.environ
        barrier.wait(timeout=2)
        if job["id"] == "bound-job":
            return [{"action": "allow", "reason": "runtime_binding_verified"}]
        return [{"action": "block", "reason": "missing_behavior_binding"}]

    def admit(job):
        try:
            cron_jobs._apply_cron_runtime_governance(job)
            return "allow"
        except CronRuntimeAdmissionError as exc:
            return str(exc)

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", invoke_hook)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(admit, [{"id": "bound-job"}, {"id": "unbound-job"}]))

    assert results == ["allow", "Cron job was not run: missing_behavior_binding."]
    assert marker not in os.environ


def test_runtime_admission_receipt_is_persisted_without_execution_material(
    governed_store,
    monkeypatch,
):
    from cron import scheduler

    def invoke_hook(event, **_kwargs):
        if event == "pre_cron_job_persist":
            return [allow_decision()]
        assert event == "pre_cron_job_run"
        return [{
            "action": "block",
            "reason": "missing_behavior_binding",
            "state": "unbound_job_review",
            "untrusted_detail": "prompt=do-not-persist /private/route",
        }]

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", invoke_hook)
    delivered = []
    monkeypatch.setattr(
        scheduler,
        "_deliver_result",
        lambda _job, content, **_kwargs: delivered.append(content),
    )
    job = create_job(
        prompt="do-not-persist",
        schedule="every 1h",
        authorized_behavior_ref="behavior.test",
    )
    job["creation_governance_receipt"].update(
        {
            "profile_id": "profile-test",
            "cron_job_id": job["id"],
            "receipt_id": "sha256:" + "1" * 64,
        }
    )
    save_jobs([job])

    assert scheduler.run_one_job(job) is True

    persisted = load_jobs()[0]
    receipt = persisted["last_runtime_admission_receipt"]
    assert receipt["schema_version"] == "cron-runtime-admission/v1"
    assert receipt["stage"] == "pre_cron_job_run"
    assert receipt["reason_code"] == "missing_behavior_binding"
    assert receipt["state"] == "unbound_job_review"
    assert receipt["retryable"] is False
    assert receipt["job_fingerprint"].startswith("sha256:")
    assert "do-not-persist" not in json.dumps(receipt)
    assert "/private/route" not in json.dumps(receipt)
    assert persisted["last_error"] == "Cron job was not run: missing_behavior_binding."
    assert delivered == [
        "⚠️ Cron 'do-not-persist' failed: Cron job was not run: missing_behavior_binding."
    ]


def test_callers_cannot_inject_their_own_receipt(governed_store):
    with pytest.raises(ValueError, match="cannot be updated"):
        update_job("missing", {"creation_governance_receipt": {"receipt_id": "forged"}})


def test_exact_resume_package_persists_original_job_once(governed_store, monkeypatch):
    resume_id = "sha256:resume"
    package = {
        "schema_version": "cron-persist-resume/v1",
        "receipt": {"schema_version": "cron-persist-resume/v1", "receipt_id": resume_id},
        "job": {
            "id": "abc123def456",
            "name": "approved resume",
            "prompt": "approved",
            "skills": [],
            "skill": None,
            "schedule": {"kind": "interval", "seconds": 3600, "display": "every 1h"},
            "schedule_display": "every 1h",
            "repeat": {"times": None, "completed": 0},
            "enabled": True,
            "state": "scheduled",
            "deliver": "local",
        },
    }

    def invoke_hook(_name, **kwargs):
        if kwargs["existing_jobs"]:
            return [{
                "action": "allow",
                "reason": "authorized_job_already_persisted",
                "persist_disposition": "already_persisted",
                "existing_job_id": "abc123def456",
            }]
        decision = allow_decision(
            creation_governance_receipt={
                "schema_version": "cron-creation-governance/v1",
                "receipt_id": "sha256:creation",
                "resume_receipt_id": resume_id,
            },
            enabled=False,
            state="paused",
            paused_reason="admin_authorized_pending_explicit_enable",
        )
        decision["persist_disposition"] = "allow_write"
        return [decision]

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", invoke_hook)

    first = create_job(prompt=None, schedule="", governance_resume=package)
    second = create_job(prompt=None, schedule="", governance_resume=package)

    assert first["id"] == second["id"] == "abc123def456"
    assert first["enabled"] is False
    assert first["state"] == "paused"
    assert first["paused_reason"] == "admin_authorized_pending_explicit_enable"
    assert first["creation_governance_receipt"]["resume_receipt_id"] == resume_id
    assert len(load_jobs()) == 1
    assert "cron_persist_resume_receipt" not in load_jobs()[0]


def test_resume_update_accepts_hook_controlled_explicit_enable_pause(governed_store, monkeypatch):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    resume_id = "sha256:resume-update"
    package = {
        "schema_version": "cron-persist-resume/v1",
        "receipt": {"schema_version": "cron-persist-resume/v1", "receipt_id": resume_id},
        "job": job,
    }

    decision = allow_decision(
        creation_governance_receipt={
            "schema_version": "cron-creation-governance/v1",
            "receipt_id": "sha256:resumed-creation",
            "resume_receipt_id": resume_id,
        },
        enabled=False,
        state="paused",
        paused_reason="admin_authorized_pending_explicit_enable",
    )
    decision["persist_disposition"] = "allow_write"
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [decision])

    updated = update_job(job["id"], {}, governance_resume=package)

    assert updated["enabled"] is False
    assert updated["state"] == "paused"
    assert updated["paused_reason"] == "admin_authorized_pending_explicit_enable"
    assert updated["creation_governance_receipt"]["resume_receipt_id"] == resume_id
    assert "cron_persist_resume_receipt" not in load_jobs()[0]


@pytest.mark.parametrize(
    "state_patch",
    [
        {"enabled": True, "state": "paused", "paused_reason": "admin_authorized_pending_explicit_enable"},
        {"enabled": False, "state": "scheduled", "paused_reason": "admin_authorized_pending_explicit_enable"},
        {"enabled": False, "state": "paused", "paused_reason": "different"},
        {"enabled": False, "state": "paused"},
    ],
)
def test_resume_update_rejects_noncanonical_pause_tuple(
    governed_store,
    monkeypatch,
    state_patch,
):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    before = (governed_store / "cron" / "jobs.json").read_bytes()
    resume_id = "sha256:resume-update"
    package = {
        "schema_version": "cron-persist-resume/v1",
        "receipt": {"schema_version": "cron-persist-resume/v1", "receipt_id": resume_id},
        "job": job,
    }
    decision = allow_decision(
        creation_governance_receipt={
            "schema_version": "cron-creation-governance/v1",
            "receipt_id": "sha256:resumed-creation",
            "resume_receipt_id": resume_id,
        },
        **state_patch,
    )
    decision["persist_disposition"] = "allow_write"
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [decision])

    with pytest.raises(CronJobGovernanceError, match="invalid resume pause result"):
        update_job(job["id"], {}, governance_resume=package)

    assert (governed_store / "cron" / "jobs.json").read_bytes() == before


def test_ordinary_update_rejects_governance_state_patch(governed_store, monkeypatch):
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])
    job = create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")
    before = (governed_store / "cron" / "jobs.json").read_bytes()
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *args, **kwargs: [
            allow_decision(
                enabled=False,
                state="paused",
                paused_reason="admin_authorized_pending_explicit_enable",
            )
        ],
    )

    with pytest.raises(CronJobGovernanceError, match="unexpected governance state patch"):
        update_job(job["id"], {"schedule": "every 2h"})

    assert (governed_store / "cron" / "jobs.json").read_bytes() == before


def test_ordinary_create_rejects_governance_state_patch(governed_store, monkeypatch):
    monkeypatch.setattr(
        "hermes_cli.plugins.invoke_hook",
        lambda *args, **kwargs: [
            allow_decision(
                enabled=False,
                state="paused",
                paused_reason="admin_authorized_pending_explicit_enable",
            )
        ],
    )

    with pytest.raises(CronJobGovernanceError, match="unexpected governance state patch"):
        create_job(prompt="approved", schedule="every 1h", authorized_behavior_ref="behavior.test")

    assert load_jobs() == []
    assert not (governed_store / "cron" / "jobs.json").exists()


def test_registry_forwards_governance_resume_to_cronjob(monkeypatch):
    resume = {
        "job": {"id": "approved-job"},
        "receipt": {"receipt_id": "sha256:resume"},
    }
    captured = {}

    def fake_cronjob(**kwargs):
        captured.update(kwargs)
        return json.dumps({"success": True})

    monkeypatch.setattr("tools.cronjob_tools.cronjob", fake_cronjob)

    result = json.loads(registry.dispatch("cronjob", {
        "action": "create",
        "attach_to_session": False,
        "authorized_behavior_ref": "behavior.approved",
        "implementation_categories": ["cron", "reliable_delivery"],
        "governance_resume": resume,
    }))

    assert result["success"] is True
    assert captured["attach_to_session"] is False
    assert captured["authorized_behavior_ref"] == "behavior.approved"
    assert captured["implementation_categories"] == ["cron", "reliable_delivery"]
    assert captured["governance_resume"] == resume


def test_cronjob_create_formats_minimal_governance_resume_job(monkeypatch):
    resume = {
        "job": {"id": "approved-job"},
        "receipt": {"receipt_id": "sha256:resume"},
    }
    persisted_job = {
        "id": "approved-job",
        "name": "approved resume",
        "prompt": "approved",
        "skills": [],
        "schedule": {
            "kind": "cron",
            "expr": "0 0 1 1 *",
            "display": "0 0 1 1 *",
        },
        "repeat": {"times": None, "completed": 0},
        "enabled": False,
        "state": "paused",
        "deliver": "local",
    }

    monkeypatch.setattr("tools.cronjob_tools.create_job", lambda **_kwargs: persisted_job)
    monkeypatch.setattr("tools.cronjob_tools._notify_provider_jobs_changed_safe", lambda: None)

    result = json.loads(cronjob(action="create", governance_resume=resume))

    assert result["success"] is True
    assert result["job_id"] == "approved-job"
    assert result["schedule"] == "0 0 1 1 *"
    assert result["next_run_at"] is None
    assert result["job"]["state"] == "paused"


def test_governed_write_fails_closed_when_cross_process_lock_is_unavailable(governed_store, monkeypatch):
    class BusyFcntl:
        LOCK_EX = 1
        LOCK_NB = 2
        LOCK_UN = 8

        @staticmethod
        def flock(*_args):
            raise OSError("injected busy lock")

    monkeypatch.setattr("cron.jobs.fcntl", BusyFcntl)
    monkeypatch.setattr("cron.jobs._JOBS_LOCK_TIMEOUT_SECONDS", 0)
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", lambda *args, **kwargs: [allow_decision()])

    with pytest.raises(CronJobGovernanceError, match="strict jobs lock unavailable"):
        create_job(prompt="approved", schedule="every 1h")

    assert load_jobs() == []
