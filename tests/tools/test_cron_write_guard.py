import json
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cron.scheduler import _cron_authorization_values
from gateway.session_context import get_cron_runtime_context, scoped_cron_authorization
from tools.cron_write_guard import authorize_cron_tool_call, write_scope_ref


def _scope(root: Path, operations=("create", "update")):
    return {
        "schema_version": "cron-write-scope/v1",
        "roots": [{"path": str(root), "operations": list(operations)}],
    }


@contextmanager
def _active_cron(scope=None, ref=None):
    values = {"HERMES_CRON_JOB_ID": "job-1624"}
    if scope is not None:
        values["HERMES_CRON_WRITE_SCOPE"] = scope
    if ref is not None:
        values["HERMES_CRON_WRITE_SCOPE_REF"] = ref
    with scoped_cron_authorization(values):
        yield


def _successful_file_ops():
    file_ops = MagicMock()
    result = MagicMock()
    result.to_dict.return_value = {"status": "ok"}
    file_ops.write_file.return_value = result
    file_ops.patch_v4a.return_value = result
    file_ops.patch_replace.return_value = result
    return file_ops


def test_scheduler_projects_only_verified_runtime_write_scope(tmp_path):
    scope = _scope(tmp_path)
    ref = write_scope_ref(scope)

    values = _cron_authorization_values(
        {"id": "job-1624"},
        runtime_admission={"write_scope_ref": ref, "write_scope": scope},
    )

    assert values["HERMES_CRON_WRITE_SCOPE_REF"] == ref
    assert values["HERMES_CRON_WRITE_SCOPE"] == json.dumps(
        scope, sort_keys=True, separators=(",", ":")
    )
    with scoped_cron_authorization(values):
        runtime = get_cron_runtime_context()
        assert runtime is not None
        assert runtime.write_scope_ref == ref
        assert runtime.write_scope == scope


def test_scheduler_ignores_forged_persisted_write_scope_without_verified_admission(tmp_path):
    scope = _scope(tmp_path)
    values = _cron_authorization_values({
        "id": "job-1624",
        "write_scope_ref": write_scope_ref(scope),
        "write_scope": scope,
    })

    assert values["HERMES_CRON_WRITE_SCOPE_REF"] == ""
    assert values["HERMES_CRON_WRITE_SCOPE"] == ""


def test_missing_scope_denies_direct_write_before_file_ops(tmp_path):
    target = tmp_path / "missing.txt"
    with patch("tools.file_tools._get_file_ops") as get_file_ops, _active_cron():
        from tools.file_tools import write_file_tool

        result = json.loads(write_file_tool(str(target), "blocked"))

    assert "missing write scope" in result["error"]
    get_file_ops.assert_not_called()
    assert not target.exists()


def test_forged_process_environment_cannot_activate_cron_guard(monkeypatch, tmp_path):
    target = tmp_path / "ordinary.txt"
    scope = _scope(tmp_path)
    monkeypatch.setenv("HERMES_CRON_JOB_ID", "forged-job")
    monkeypatch.setenv("HERMES_CRON_WRITE_SCOPE_REF", write_scope_ref(scope))
    monkeypatch.setenv("HERMES_CRON_WRITE_SCOPE", json.dumps(scope))
    file_ops = _successful_file_ops()

    with patch("tools.file_tools._get_file_ops", return_value=file_ops):
        from tools.file_tools import write_file_tool

        result = json.loads(write_file_tool(str(target), "allowed"))

    assert result["status"] == "ok"
    file_ops.write_file.assert_called_once()


def test_ref_mismatch_denies_direct_write(tmp_path):
    target = tmp_path / "mismatch.txt"
    scope = _scope(tmp_path)
    with patch("tools.file_tools._get_file_ops") as get_file_ops, _active_cron(
        scope, "sha256:" + "0" * 64
    ):
        from tools.file_tools import write_file_tool

        result = json.loads(write_file_tool(str(target), "blocked"))

    assert "reference mismatch" in result["error"]
    get_file_ops.assert_not_called()


def test_invalid_scope_schema_denies_direct_write(tmp_path):
    target = tmp_path / "invalid.txt"
    scope = {"schema_version": "cron-write-scope/v2", "roots": []}
    with patch("tools.file_tools._get_file_ops") as get_file_ops, _active_cron(
        scope, write_scope_ref(scope)
    ):
        from tools.file_tools import write_file_tool

        result = json.loads(write_file_tool(str(target), "blocked"))

    assert "invalid write scope" in result["error"]
    get_file_ops.assert_not_called()


@pytest.mark.parametrize(
    ("exists", "operation"),
    [(False, "create"), (True, "update")],
)
def test_direct_write_requires_the_effective_operation(tmp_path, exists, operation):
    target = tmp_path / "target.txt"
    if exists:
        target.write_text("old", encoding="utf-8")
    scope = _scope(tmp_path, (operation,))
    file_ops = _successful_file_ops()

    with patch("tools.file_tools._get_file_ops", return_value=file_ops), _active_cron(
        scope, write_scope_ref(scope)
    ):
        from tools.file_tools import write_file_tool

        result = json.loads(write_file_tool(str(target), "new"))

    assert result["status"] == "ok"
    assert result["operation"] == operation
    assert target.read_text(encoding="utf-8") == "new"
    file_ops.write_file.assert_not_called()


def test_path_escape_denied_before_file_ops(tmp_path):
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    target = tmp_path / "sibling.txt"
    scope = _scope(allowed)

    with patch("tools.file_tools._get_file_ops") as get_file_ops, _active_cron(
        scope, write_scope_ref(scope)
    ):
        from tools.file_tools import write_file_tool

        result = json.loads(write_file_tool(str(target), "blocked"))

    assert "outside write scope" in result["error"]
    get_file_ops.assert_not_called()
    assert not target.exists()


def test_traversal_denied_even_when_normalized_target_is_in_scope(tmp_path):
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    raw_target = str(allowed / "nested" / ".." / "target.txt")
    scope = _scope(allowed)

    with patch("tools.file_tools._get_file_ops") as get_file_ops, _active_cron(
        scope, write_scope_ref(scope)
    ):
        from tools.file_tools import write_file_tool

        result = json.loads(write_file_tool(raw_target, "blocked"))

    assert "traversal" in result["error"]
    get_file_ops.assert_not_called()


def test_symlink_component_denied_before_file_ops(tmp_path):
    allowed = tmp_path / "allowed"
    outside = tmp_path / "outside"
    allowed.mkdir()
    outside.mkdir()
    (allowed / "link").symlink_to(outside, target_is_directory=True)
    target = allowed / "link" / "escaped.txt"
    scope = _scope(allowed)

    with patch("tools.file_tools._get_file_ops") as get_file_ops, _active_cron(
        scope, write_scope_ref(scope)
    ):
        from tools.file_tools import write_file_tool

        result = json.loads(write_file_tool(str(target), "blocked"))

    assert "symlink component" in result["error"]
    get_file_ops.assert_not_called()
    assert not (outside / "escaped.txt").exists()


def test_update_rejects_target_inode_swap_before_atomic_commit(tmp_path):
    target = tmp_path / "target.txt"
    target.write_text("old", encoding="utf-8")
    scope = _scope(tmp_path, ("update",))
    real_stat = __import__("os").stat
    observed = 0

    def swap_after_first_target_stat(path, *args, **kwargs):
        nonlocal observed
        result = real_stat(path, *args, **kwargs)
        if path == target.name and kwargs.get("dir_fd") is not None:
            observed += 1
            if observed == 1:
                replacement = tmp_path / "replacement.txt"
                replacement.write_text("raced", encoding="utf-8")
                replacement.replace(target)
        return result

    with _active_cron(scope, write_scope_ref(scope)), patch(
        "tools.cron_write_guard.os.stat", side_effect=swap_after_first_target_stat
    ):
        from tools.file_tools import write_file_tool

        result = json.loads(write_file_tool(str(target), "new"))

    assert "target changed before commit" in result["error"]
    assert target.read_text(encoding="utf-8") == "raced"


def test_active_cron_remote_backend_fails_closed_before_file_ops(tmp_path):
    target = tmp_path / "remote.txt"
    scope = _scope(tmp_path, ("create",))
    with _active_cron(scope, write_scope_ref(scope)), patch(
        "tools.file_tools._terminal_env_type_for_task", return_value="ssh"
    ), patch("tools.file_tools._get_file_ops") as get_file_ops:
        from tools.file_tools import write_file_tool

        result = json.loads(write_file_tool(str(target), "blocked"))

    assert "remote file backends are unavailable" in result["error"]
    get_file_ops.assert_not_called()
    assert not target.exists()


@pytest.mark.parametrize("exists", [False, True])
def test_post_commit_directory_fsync_failure_reports_success_with_warning(tmp_path, exists):
    target = tmp_path / "target.txt"
    operation = "update" if exists else "create"
    if exists:
        target.write_text("old", encoding="utf-8")
    scope = _scope(tmp_path, (operation,))

    with _active_cron(scope, write_scope_ref(scope)), patch(
        "tools.cron_write_guard.os.fsync",
        side_effect=[None, OSError("directory fsync failed")],
    ):
        from tools.file_tools import write_file_tool

        result = json.loads(write_file_tool(str(target), "committed"))

    assert result["status"] == "ok"
    assert result["operation"] == operation
    assert "directory fsync failed after commit" in result["durability_warning"]
    assert target.read_text(encoding="utf-8") == "committed"


def test_create_file_close_failure_reports_committed_success(tmp_path):
    target = tmp_path / "target.txt"
    scope = _scope(tmp_path, ("create",))
    real_close = __import__("os").close
    real_fstat = __import__("os").fstat

    def close_then_fail_for_regular_file(fd):
        is_regular = __import__("stat").S_ISREG(real_fstat(fd).st_mode)
        real_close(fd)
        if is_regular:
            raise OSError("regular file close failed")

    with _active_cron(scope, write_scope_ref(scope)), patch(
        "tools.cron_write_guard.os.close", side_effect=close_then_fail_for_regular_file
    ):
        from tools.file_tools import write_file_tool

        result = json.loads(write_file_tool(str(target), "committed"))

    assert result["status"] == "ok"
    assert "file close failed after commit" in result["durability_warning"]
    assert target.read_text(encoding="utf-8") == "committed"


def test_update_temp_file_close_failure_keeps_target_and_cleans_temp(tmp_path):
    target = tmp_path / "target.txt"
    target.write_text("old", encoding="utf-8")
    scope = _scope(tmp_path, ("update",))
    real_close = __import__("os").close
    real_fstat = __import__("os").fstat

    def close_then_fail_for_regular_file(fd):
        is_regular = __import__("stat").S_ISREG(real_fstat(fd).st_mode)
        real_close(fd)
        if is_regular:
            raise OSError("temp file close failed")

    with _active_cron(scope, write_scope_ref(scope)), patch(
        "tools.cron_write_guard.os.close", side_effect=close_then_fail_for_regular_file
    ):
        from tools.file_tools import write_file_tool

        result = json.loads(write_file_tool(str(target), "not-committed"))

    assert "secure local write failed" in result["error"]
    assert target.read_text(encoding="utf-8") == "old"
    assert list(tmp_path.glob(".*.hermes-cron-*.tmp")) == []


def test_v4a_multi_target_is_all_or_nothing(tmp_path):
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    inside = allowed / "inside.txt"
    outside = tmp_path / "outside.txt"
    scope = _scope(allowed, ("create",))
    patch_text = (
        "*** Begin Patch\n"
        f"*** Add File: {inside}\n"
        "+inside\n"
        f"*** Add File: {outside}\n"
        "+outside\n"
        "*** End Patch"
    )

    with patch("tools.file_tools._get_file_ops") as get_file_ops, _active_cron(
        scope, write_scope_ref(scope)
    ):
        from tools.file_tools import patch_tool

        result = json.loads(patch_tool(mode="patch", patch=patch_text))

    assert "patch has no atomic local sink" in result["error"]
    get_file_ops.assert_not_called()
    assert not inside.exists()
    assert not outside.exists()


def test_v4a_move_authorizes_both_endpoints(tmp_path):
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    source = allowed / "source.txt"
    destination = tmp_path / "outside.txt"
    scope = _scope(allowed, ("move",))
    patch_text = (
        "*** Begin Patch\n"
        f"*** Move File: {source} -> {destination}\n"
        "*** End Patch"
    )

    with patch("tools.file_tools._get_file_ops") as get_file_ops, _active_cron(
        scope, write_scope_ref(scope)
    ):
        from tools.file_tools import patch_tool

        result = json.loads(patch_tool(mode="patch", patch=patch_text))

    assert "patch has no atomic local sink" in result["error"]
    get_file_ops.assert_not_called()


def test_patch_replace_requires_update_authority(tmp_path):
    target = tmp_path / "target.txt"
    target.write_text("old", encoding="utf-8")
    scope = _scope(tmp_path, ("create",))

    with patch("tools.file_tools._get_file_ops") as get_file_ops, _active_cron(
        scope, write_scope_ref(scope)
    ):
        from tools.file_tools import patch_tool

        result = json.loads(
            patch_tool(
                mode="replace",
                path=str(target),
                old_string="old",
                new_string="new",
            )
        )

    assert "patch has no atomic local sink" in result["error"]
    get_file_ops.assert_not_called()
    assert target.read_text(encoding="utf-8") == "old"


def test_v4a_all_targets_still_fail_closed_without_atomic_patch_sink(tmp_path):
    first = tmp_path / "first.txt"
    second = tmp_path / "second.txt"
    scope = _scope(tmp_path, ("create",))
    patch_text = (
        "*** Begin Patch\n"
        f"*** Add File: {first}\n"
        "+first\n"
        f"*** Add File: {second}\n"
        "+second\n"
        "*** End Patch"
    )
    file_ops = _successful_file_ops()

    with patch("tools.file_tools._get_file_ops", return_value=file_ops), _active_cron(
        scope, write_scope_ref(scope)
    ):
        from tools.file_tools import patch_tool

        result = json.loads(patch_tool(mode="patch", patch=patch_text))

    assert "patch has no atomic local sink" in result["error"]
    file_ops.patch_v4a.assert_not_called()
    assert not first.exists()
    assert not second.exists()


def test_terminal_denied_for_any_active_cron_context():
    with patch("tools.terminal_tool._get_env_config") as get_config, _active_cron():
        from tools.terminal_tool import terminal_tool

        result = json.loads(terminal_tool(command={"forged": "prompt"}))

    assert result["status"] == "blocked"
    assert "terminal is unavailable" in result["error"]
    get_config.assert_not_called()


def test_execute_code_denied_for_any_active_cron_context():
    with patch("tools.approval.check_execute_code_guard") as approval, _active_cron():
        from tools.code_execution_tool import execute_code

        result = json.loads(execute_code("print('should not run')"))

    assert "execute_code is unavailable" in result["error"]
    approval.assert_not_called()


def test_pre_dispatch_classifier_requires_scope_and_denies_managed_mutation(tmp_path):
    scope = _scope(tmp_path)
    ref = write_scope_ref(scope)

    assert authorize_cron_tool_call("write_file", {}, write_scope=None, write_scope_ref="")["allowed"] is False
    patch_decision = authorize_cron_tool_call("patch", {}, write_scope=scope, write_scope_ref=ref)
    assert patch_decision == {"allowed": False, "reason": "cron_patch_sink_unavailable"}
    for tool_name in ("terminal_exec", "execute_code", "skill_manage", "memory"):
        assert authorize_cron_tool_call(tool_name, {}, write_scope=scope, write_scope_ref=ref) == {
            "allowed": False,
            "reason": "cron_generic_execute_denied",
        }


def test_memory_and_skill_manage_have_active_cron_sink_backstops():
    with _active_cron():
        from tools.memory_tool import memory_tool
        from tools.skill_manager_tool import skill_manage

        memory_result = json.loads(memory_tool(action="add", content="blocked", store=MagicMock()))
        skill_result = json.loads(skill_manage(action="create", name="blocked", content="# Skill"))

    assert "memory is unavailable" in memory_result["error"]
    assert "skill_manage is unavailable" in skill_result["error"]


def test_non_cron_terminal_and_execute_code_keep_existing_entry_behavior():
    with patch(
        "tools.terminal_tool._get_env_config",
        side_effect=RuntimeError("terminal-existing-path"),
    ):
        from tools.terminal_tool import terminal_tool

        terminal_result = json.loads(terminal_tool("echo unchanged"))
    assert "terminal-existing-path" in terminal_result["error"]

    with patch("tools.code_execution_tool.SANDBOX_AVAILABLE", False):
        from tools.code_execution_tool import execute_code

        code_result = json.loads(execute_code("print('unchanged')"))
    assert "sandbox is unavailable" in code_result["error"]
