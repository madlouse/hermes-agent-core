from __future__ import annotations

from pathlib import Path

import pytest

from agent.skill_resolution import (
    SkillResolutionError,
    resolve_skill_ref,
    resolve_skill_refs,
)


def _write_skill(
    profile: Path,
    relative_dir: str,
    *,
    name: str,
    body: str = "Follow this skill.",
) -> Path:
    skill_dir = profile / "skills" / relative_dir
    skill_dir.mkdir(parents=True, exist_ok=True)
    skill_md = skill_dir / "SKILL.md"
    skill_md.write_text(
        f"---\nname: {name}\ndescription: test\n---\n\n{body}\n",
        encoding="utf-8",
    )
    return skill_md


def test_bare_path_and_colon_selectors_converge(tmp_path):
    profile = tmp_path / "atlas"
    skill_md = _write_skill(
        profile,
        "work/cron-task-force",
        name="cron-task-force",
    )

    bindings = [
        resolve_skill_ref(profile, selector)
        for selector in (
            "cron-task-force",
            "work/cron-task-force",
            "work:cron-task-force",
        )
    ]

    assert bindings[0] == bindings[1] == bindings[2]
    assert bindings[0]["canonical_name"] == "cron-task-force"
    assert bindings[0]["relative_path"] == "skills/work/cron-task-force/SKILL.md"
    assert bindings[0]["source_kind"] == "profile_local"
    assert bindings[0]["content_digest"].startswith("sha256:")
    assert skill_md.read_text(encoding="utf-8").endswith("Follow this skill.\n")


def test_missing_skill_is_profile_local_failure(tmp_path):
    atlas = tmp_path / "atlas"
    yuange = tmp_path / "yuange"
    _write_skill(atlas, "work/cron-task-force", name="cron-task-force")
    (yuange / "skills").mkdir(parents=True)

    with pytest.raises(SkillResolutionError) as excinfo:
        resolve_skill_ref(yuange, "cron-task-force")

    assert excinfo.value.code == "skill_unavailable_in_active_profile"


def test_bare_name_ambiguity_fails_closed(tmp_path):
    profile = tmp_path / "profile"
    first = _write_skill(profile, "work/first", name="duplicate")
    second = _write_skill(profile, "research/second", name="duplicate")

    with pytest.raises(SkillResolutionError) as excinfo:
        resolve_skill_ref(profile, "duplicate")

    assert excinfo.value.code == "skill_ambiguous_in_active_profile"
    assert set(excinfo.value.matches) == {str(first), str(second)}


def test_external_skill_is_not_visible_in_profile(tmp_path):
    profile = tmp_path / "profile"
    external = tmp_path / "external"
    (profile / "skills").mkdir(parents=True)
    _write_skill(external, "external-only", name="external-only")

    with pytest.raises(SkillResolutionError) as excinfo:
        resolve_skill_ref(profile, "external-only")

    assert excinfo.value.code == "skill_unavailable_in_active_profile"


def test_profile_skill_symlink_cannot_escape_profile(tmp_path):
    profile = tmp_path / "profile"
    external = tmp_path / "external"
    external_skill = _write_skill(external, "escaped", name="escaped")
    linked_dir = profile / "skills" / "escaped"
    linked_dir.parent.mkdir(parents=True)
    linked_dir.symlink_to(external_skill.parent, target_is_directory=True)

    with pytest.raises(SkillResolutionError) as excinfo:
        resolve_skill_ref(profile, "escaped")

    assert excinfo.value.code == "skill_symlink_unsupported"


def test_skill_digest_covers_behavior_files(tmp_path):
    profile = tmp_path / "profile"
    skill_md = _write_skill(profile, "work/task", name="task")
    script = skill_md.parent / "scripts" / "run.sh"
    script.parent.mkdir()
    script.write_text("echo first\n", encoding="utf-8")
    first = resolve_skill_ref(profile, "task")

    script.write_text("echo changed\n", encoding="utf-8")
    second = resolve_skill_ref(profile, "task")

    assert first["content_digest"] != second["content_digest"]


def test_skill_digest_covers_executable_mode(tmp_path):
    profile = tmp_path / "profile"
    skill_md = _write_skill(profile, "work/task", name="task")
    script = skill_md.parent / "scripts" / "run.sh"
    script.parent.mkdir()
    script.write_text("echo run\n", encoding="utf-8")
    script.chmod(0o600)
    first = resolve_skill_ref(profile, "task")

    script.chmod(0o700)
    second = resolve_skill_ref(profile, "task")

    assert first["content_digest"] != second["content_digest"]


def test_skill_tree_rejects_nested_symlink(tmp_path):
    profile = tmp_path / "profile"
    skill_md = _write_skill(profile, "work/task", name="task")
    external = tmp_path / "external-script.sh"
    external.write_text("echo external\n", encoding="utf-8")
    scripts = skill_md.parent / "scripts"
    scripts.mkdir()
    (scripts / "run.sh").symlink_to(external)

    with pytest.raises(SkillResolutionError) as excinfo:
        resolve_skill_ref(profile, "task")

    assert excinfo.value.code == "skill_symlink_unsupported"


def test_skill_rejects_symlinked_category_ancestor(tmp_path):
    profile = tmp_path / "profile"
    _write_skill(profile, "real/category/task", name="task")
    alias = profile / "skills" / "alias"
    alias.symlink_to(profile / "skills" / "real" / "category", target_is_directory=True)

    with pytest.raises(SkillResolutionError) as excinfo:
        resolve_skill_ref(profile, "alias/task")

    assert excinfo.value.code == "skill_symlink_unsupported"


def test_distinct_targets_with_same_canonical_name_fail_closed(tmp_path):
    profile = tmp_path / "profile"
    _write_skill(profile, "work/first", name="duplicate")
    _write_skill(profile, "work/second", name="duplicate")

    with pytest.raises(SkillResolutionError) as excinfo:
        resolve_skill_refs(profile, ["work/first", "work/second"])

    assert excinfo.value.code == "skill_canonical_name_collision"


def test_legacy_flat_skill_is_bound_profile_locally(tmp_path):
    profile = tmp_path / "profile"
    skill = profile / "skills" / "legacy.md"
    skill.parent.mkdir(parents=True)
    skill.write_text("---\nname: legacy\ndescription: old\n---\n\nLegacy body.\n")

    binding = resolve_skill_ref(profile, "legacy")

    assert binding["canonical_name"] == "legacy"
    assert binding["relative_path"] == "skills/legacy.md"


def test_exact_plugin_local_collision_fails_closed(tmp_path):
    profile = tmp_path / "profile"
    local = _write_skill(profile, "work/task", name="task")
    plugin = tmp_path / "plugin" / "SKILL.md"
    plugin.parent.mkdir(parents=True)
    plugin.write_text("---\nname: task\ndescription: plugin\n---\n", encoding="utf-8")

    with pytest.raises(SkillResolutionError) as excinfo:
        resolve_skill_ref(
            profile,
            "work:task",
            plugin_skill_path=plugin,
        )

    assert excinfo.value.code == "skill_plugin_local_conflict"
    assert set(excinfo.value.matches) == {str(local), str(plugin.resolve())}
