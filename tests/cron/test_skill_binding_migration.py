from __future__ import annotations

import json
from pathlib import Path

import pytest

from cron.jobs import apply_skill_binding_migration, plan_skill_binding_migration


def _write_skill(profile: Path, relative_dir: str, name: str) -> None:
    skill_dir = profile / "skills" / relative_dir
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(
        f"---\nname: {name}\ndescription: test\n---\n\nBody.\n",
        encoding="utf-8",
    )


def _write_legacy_jobs(profile: Path, skills: list[str]) -> Path:
    jobs_file = profile / "cron" / "jobs.json"
    jobs_file.parent.mkdir(parents=True, exist_ok=True)
    jobs_file.write_text(
        json.dumps(
            {
                "jobs": [
                    {
                        "id": "legacy-job",
                        "name": "Legacy",
                        "prompt": "Run",
                        "skill": skills[0] if skills else None,
                        "skills": skills,
                    }
                ],
                "updated_at": "before",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return jobs_file


def test_plan_is_read_only_and_aliases_canonicalize(tmp_path):
    profile = tmp_path / "atlas"
    _write_skill(profile, "work/cron-task-force", "cron-task-force")
    jobs_file = _write_legacy_jobs(profile, ["work:cron-task-force"])
    before = jobs_file.read_bytes()
    mtime = jobs_file.stat().st_mtime_ns

    plan = plan_skill_binding_migration(profile)

    assert plan["errors"] == []
    assert plan["applicable"] is True
    assert len(plan["changes"]) == 1
    assert plan["changes"][0]["after"]["skills"] == ["cron-task-force"]
    assert jobs_file.read_bytes() == before
    assert jobs_file.stat().st_mtime_ns == mtime


def test_apply_is_backup_first_and_idempotent(tmp_path):
    profile = tmp_path / "atlas"
    _write_skill(profile, "work/cron-task-force", "cron-task-force")
    jobs_file = _write_legacy_jobs(profile, ["cron-task-force"])
    original = jobs_file.read_bytes()
    plan = plan_skill_binding_migration(profile)

    first = apply_skill_binding_migration(plan)
    second = apply_skill_binding_migration(plan)

    assert first["status"] == "applied"
    assert first["jobs_updated"] == 1
    backup = Path(first["backup_path"])
    assert backup.read_bytes() == original
    migrated = json.loads(jobs_file.read_text(encoding="utf-8"))["jobs"][0]
    assert migrated["skills"] == ["cron-task-force"]
    assert migrated["skill_bindings"][0]["relative_path"] == (
        "skills/work/cron-task-force/SKILL.md"
    )
    assert second == {
        "status": "already_applied",
        "jobs_updated": 0,
        "backup_path": None,
    }


def test_apply_keeps_backup_when_atomic_save_fails(tmp_path, monkeypatch):
    profile = tmp_path / "atlas"
    _write_skill(profile, "work/cron-task-force", "cron-task-force")
    jobs_file = _write_legacy_jobs(profile, ["cron-task-force"])
    original = jobs_file.read_bytes()
    plan = plan_skill_binding_migration(profile)
    digest_token = plan["store_digest"].split(":", 1)[-1][:16]
    backup = jobs_file.with_name(
        f"{jobs_file.name}.skill-bindings.{digest_token}.bak"
    )
    monkeypatch.setattr(
        "cron.jobs._save_jobs_unlocked",
        lambda jobs: (_ for _ in ()).throw(OSError("save failed")),
    )

    with pytest.raises(OSError, match="save failed"):
        apply_skill_binding_migration(plan)

    assert jobs_file.read_bytes() == original
    assert backup.read_bytes() == original


def test_apply_restores_original_bytes_after_partial_save_failure(tmp_path, monkeypatch):
    profile = tmp_path / "atlas"
    _write_skill(profile, "work/cron-task-force", "cron-task-force")
    jobs_file = _write_legacy_jobs(profile, ["cron-task-force"])
    original = jobs_file.read_bytes()
    plan = plan_skill_binding_migration(profile)

    def corrupt_then_fail(_jobs):
        jobs_file.write_bytes(b"partial")
        raise OSError("post-write failure")

    monkeypatch.setattr("cron.jobs._save_jobs_unlocked", corrupt_then_fail)

    with pytest.raises(OSError, match="post-write failure"):
        apply_skill_binding_migration(plan)

    assert jobs_file.read_bytes() == original


def test_apply_rejects_store_drift_before_backup(tmp_path):
    profile = tmp_path / "atlas"
    _write_skill(profile, "work/cron-task-force", "cron-task-force")
    jobs_file = _write_legacy_jobs(profile, ["cron-task-force"])
    plan = plan_skill_binding_migration(profile)
    jobs_file.write_text(jobs_file.read_text() + "\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="changed after"):
        apply_skill_binding_migration(plan)

    assert list(jobs_file.parent.glob("*.skill-bindings.*.bak")) == []


def test_apply_rejects_skill_drift_before_backup(tmp_path):
    profile = tmp_path / "atlas"
    _write_skill(profile, "work/cron-task-force", "cron-task-force")
    jobs_file = _write_legacy_jobs(profile, ["cron-task-force"])
    plan = plan_skill_binding_migration(profile)
    skill_md = profile / "skills" / "work" / "cron-task-force" / "SKILL.md"
    skill_md.write_text(skill_md.read_text() + "changed\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="skill resolution changed"):
        apply_skill_binding_migration(plan)

    assert jobs_file.exists()
    assert list(jobs_file.parent.glob("*.skill-bindings.*.bak")) == []


def test_missing_profile_skill_blocks_plan_and_apply(tmp_path):
    profile = tmp_path / "yuange"
    (profile / "skills").mkdir(parents=True)
    _write_legacy_jobs(profile, ["cron-task-force"])

    plan = plan_skill_binding_migration(profile)

    assert plan["applicable"] is False
    assert plan["errors"][0]["reason"] == "skill_unavailable_in_active_profile"
    with pytest.raises(ValueError, match="contains resolution errors"):
        apply_skill_binding_migration(plan)
