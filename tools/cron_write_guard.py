"""Fail-closed filesystem authority for active Cron executions."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path, PurePath
from typing import Any, Iterable, Mapping

from gateway.session_context import get_cron_runtime_context


WRITE_SCOPE_SCHEMA_VERSION = "cron-write-scope/v1"
WRITE_OPERATIONS = frozenset({"create", "update"})


@dataclass(frozen=True)
class WriteTarget:
    path: str
    operation: str
    raw_path: str | None = None


def canonical_write_scope_json(scope: Mapping[str, Any]) -> str:
    return json.dumps(
        scope,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def write_scope_ref(scope: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(canonical_write_scope_json(scope).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _has_traversal(path: str) -> bool:
    normalized = path.replace("\\", "/")
    return ".." in PurePath(normalized).parts


def _has_symlink_component(path: Path) -> bool:
    current = Path(path.anchor)
    for part in path.parts[1:] if path.anchor else path.parts:
        current = current / part
        if current.is_symlink():
            return True
    return False


def _validate_scope(
    scope: Any,
    expected_ref: str,
) -> tuple[list[tuple[Path, frozenset[str]]], str | None]:
    if not expected_ref or not isinstance(scope, Mapping):
        return [], "Cron write denied: missing write scope"
    if set(scope) != {"schema_version", "roots"}:
        return [], "Cron write denied: invalid write scope schema"
    if scope.get("schema_version") != WRITE_SCOPE_SCHEMA_VERSION:
        return [], "Cron write denied: invalid write scope schema version"
    roots = scope.get("roots")
    if not isinstance(roots, list) or not roots:
        return [], "Cron write denied: invalid write scope roots"
    if write_scope_ref(scope) != expected_ref:
        return [], "Cron write denied: write scope reference mismatch"

    validated = []
    for root in roots:
        if not isinstance(root, Mapping) or set(root) != {"path", "operations"}:
            return [], "Cron write denied: invalid write scope root"
        raw_path = root.get("path")
        operations = root.get("operations")
        if not isinstance(raw_path, str) or not raw_path or not os.path.isabs(raw_path):
            return [], "Cron write denied: write scope root must be absolute"
        if _has_traversal(raw_path):
            return [], "Cron write denied: traversal in write scope root"
        if not isinstance(operations, list) or not operations:
            return [], "Cron write denied: invalid write scope operations"
        operation_set = frozenset(operations)
        if (
            any(not isinstance(item, str) for item in operations)
            or not operation_set.issubset(WRITE_OPERATIONS)
            or len(operation_set) != len(operations)
        ):
            return [], "Cron write denied: invalid write scope operation"
        root_path = Path(os.path.normpath(raw_path))
        if _has_symlink_component(root_path):
            return [], "Cron write denied: symlink component in write scope root"
        validated.append((root_path, operation_set))
    return validated, None


def authorize_cron_writes(targets: Iterable[WriteTarget]) -> str | None:
    """Return a denial message for active Cron writes, otherwise ``None``."""
    context = get_cron_runtime_context()
    if context is None:
        return None

    try:
        roots, error = _validate_scope(context.write_scope, context.write_scope_ref)
    except (AttributeError, OSError, TypeError, ValueError):
        return "Cron write denied: invalid write scope"
    if error:
        return error

    target_list = list(targets)
    if not target_list:
        return "Cron write denied: no write targets"
    for target in target_list:
        if (
            not isinstance(target.operation, str)
            or target.operation not in WRITE_OPERATIONS
        ):
            return "Cron write denied: invalid write operation"
        raw_path = target.raw_path if target.raw_path is not None else target.path
        if not isinstance(target.path, str) or not isinstance(raw_path, str):
            return "Cron write denied: invalid target path"
        if _has_traversal(raw_path):
            return f"Cron write denied: traversal in target path: {raw_path}"
        if not os.path.isabs(target.path):
            return f"Cron write denied: unresolved target path: {target.path}"
        target_path = Path(os.path.normpath(target.path))
        if _has_symlink_component(target_path):
            return f"Cron write denied: symlink component in target path: {raw_path}"

        allowed = False
        for root_path, operations in roots:
            try:
                target_path.relative_to(root_path)
            except ValueError:
                continue
            if target.operation in operations:
                allowed = True
                break
        if not allowed:
            return (
                f"Cron write denied: {target.operation} target is outside write scope: "
                f"{target.path}"
            )
    return None


def _open_absolute_directory(path: Path) -> int:
    """Open an absolute directory chain without following any symlink."""
    if not path.is_absolute() or not hasattr(os, "O_NOFOLLOW"):
        raise PermissionError("Cron write denied: secure local path operations unavailable")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    current = os.open(path.anchor, flags)
    try:
        for part in path.parts[1:]:
            child = os.open(part, flags, dir_fd=current)
            os.close(current)
            current = child
        return current
    except Exception:
        os.close(current)
        raise


def _write_all(fd: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise OSError("short write")
        view = view[written:]


def write_active_cron_file(
    target: str,
    content: str,
    *,
    raw_path: str | None = None,
) -> dict[str, Any] | None:
    """Atomically enforce and perform one active-Cron local file write.

    Returns ``None`` outside Cron. Active Cron callers must use this sink rather
    than a shell/remote backend, whose path state cannot be bound atomically.
    """
    context = get_cron_runtime_context()
    if context is None:
        return None
    try:
        roots, error = _validate_scope(context.write_scope, context.write_scope_ref)
    except (AttributeError, OSError, TypeError, ValueError) as exc:
        raise PermissionError("Cron write denied: invalid write scope") from exc
    if error:
        raise PermissionError(error)
    raw = raw_path if raw_path is not None else target
    if not isinstance(target, str) or not isinstance(raw, str) or _has_traversal(raw):
        raise PermissionError("Cron write denied: traversal in target path")
    if not os.path.isabs(target):
        raise PermissionError(f"Cron write denied: unresolved target path: {target}")

    target_path = Path(os.path.normpath(target))
    candidates: list[tuple[Path, frozenset[str], Path]] = []
    for root_path, operations in roots:
        try:
            relative = target_path.relative_to(root_path)
        except ValueError:
            continue
        if relative.parts:
            candidates.append((root_path, operations, relative))
    candidates.sort(key=lambda item: len(item[0].parts), reverse=True)
    if not candidates:
        raise PermissionError(f"Cron write denied: target is outside write scope: {target}")

    payload = content.encode("utf-8")
    last_denial = f"Cron write denied: target is outside write scope: {target}"
    for root_path, operations, relative in candidates:
        parent_fd = None
        committed_result: dict[str, Any] | None = None
        try:
            parent_fd = _open_absolute_directory(root_path)
            directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
            for part in relative.parts[:-1]:
                child = os.open(part, directory_flags, dir_fd=parent_fd)
                os.close(parent_fd)
                parent_fd = child
            leaf = relative.parts[-1]
            try:
                before = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
                operation = "update"
                if not stat.S_ISREG(before.st_mode):
                    raise PermissionError("Cron write denied: target must be a regular file")
            except FileNotFoundError:
                before = None
                operation = "create"
            if operation not in operations:
                last_denial = (
                    f"Cron write denied: {operation} target is outside write scope: {target}"
                )
                continue

            if operation == "create":
                flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
                fd = os.open(leaf, flags, 0o600, dir_fd=parent_fd)
                try:
                    _write_all(fd, payload)
                    os.fsync(fd)
                except Exception:
                    try:
                        os.close(fd)
                    finally:
                        os.unlink(leaf, dir_fd=parent_fd)
                    raise
                committed_result = {
                    "status": "ok",
                    "path": target,
                    "resolved_path": target,
                    "files_modified": [target],
                    "bytes": len(payload),
                    "operation": operation,
                }
                try:
                    os.close(fd)
                except OSError as exc:
                    committed_result["durability_warning"] = (
                        f"file close failed after commit: {type(exc).__name__}"
                    )
            else:
                temp_name = f".{leaf}.hermes-cron-{uuid.uuid4().hex}.tmp"
                flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
                temp_fd = os.open(temp_name, flags, stat.S_IMODE(before.st_mode), dir_fd=parent_fd)
                try:
                    _write_all(temp_fd, payload)
                    os.fsync(temp_fd)
                except Exception:
                    try:
                        os.close(temp_fd)
                    finally:
                        os.unlink(temp_name, dir_fd=parent_fd)
                    raise
                try:
                    os.close(temp_fd)
                except OSError:
                    try:
                        os.unlink(temp_name, dir_fd=parent_fd)
                    except FileNotFoundError:
                        pass
                    raise
                try:
                    current = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
                    if not stat.S_ISREG(current.st_mode) or (
                        current.st_dev,
                        current.st_ino,
                    ) != (before.st_dev, before.st_ino):
                        raise PermissionError("Cron write denied: target changed before commit")
                    os.replace(
                        temp_name,
                        leaf,
                        src_dir_fd=parent_fd,
                        dst_dir_fd=parent_fd,
                    )
                except Exception:
                    try:
                        os.unlink(temp_name, dir_fd=parent_fd)
                    except FileNotFoundError:
                        pass
                    raise
                committed_result = {
                    "status": "ok",
                    "path": target,
                    "resolved_path": target,
                    "files_modified": [target],
                    "bytes": len(payload),
                    "operation": operation,
                }
            try:
                os.fsync(parent_fd)
            except OSError as exc:
                committed_result["durability_warning"] = (
                    f"directory fsync failed after commit: {type(exc).__name__}"
                )
            return committed_result
        except (FileNotFoundError, NotADirectoryError, OSError) as exc:
            if isinstance(exc, PermissionError):
                raise
            if isinstance(exc, NotADirectoryError):
                last_denial = "Cron write denied: symlink component in target path"
            else:
                last_denial = f"Cron write denied: secure local write failed: {type(exc).__name__}"
        finally:
            if parent_fd is not None:
                try:
                    os.close(parent_fd)
                except OSError as exc:
                    if committed_result is None:
                        raise
                    committed_result.setdefault(
                        "durability_warning",
                        f"directory close failed after commit: {type(exc).__name__}",
                    )
    raise PermissionError(last_denial)


def deny_active_cron_execution(tool_name: str) -> str | None:
    if get_cron_runtime_context() is None:
        return None
    return f"Cron execution denied: {tool_name} is unavailable in scheduled runs"


def _generic_execute_or_managed_mutation(tool_name: str) -> bool:
    name = str(tool_name or "").strip().lower()
    if name in {"terminal", "execute_code", "skill_manage", "memory", "process"}:
        return True
    return any(
        name.startswith(prefix)
        for prefix in (
            "terminal_",
            "shell_",
            "bash_",
            "exec_",
            "subprocess_",
            "process_",
            "skill_manage_",
        )
    ) or name in {"read_terminal", "close_terminal"}


def authorize_cron_tool_call(
    tool_name: str,
    args: Mapping[str, Any] | None = None,
    *,
    write_scope: Any = None,
    write_scope_ref: str = "",
) -> dict[str, Any]:
    """Classify one Cron tool call before dispatch; sinks still check paths."""
    name = str(tool_name or "").strip().lower()
    if _generic_execute_or_managed_mutation(name):
        return {"allowed": False, "reason": "cron_generic_execute_denied"}
    if name == "patch" or name.startswith("patch_"):
        return {"allowed": False, "reason": "cron_patch_sink_unavailable"}
    if name == "write_file" or name.startswith("write_file_"):
        try:
            _roots, error = _validate_scope(write_scope, write_scope_ref)
        except (OSError, TypeError, ValueError):
            error = "Cron write denied: invalid write scope"
        if error:
            return {"allowed": False, "reason": "cron_write_scope_denied", "detail": error}
        return {"allowed": True, "reason": "cron_write_scope_bound_sink_check_required"}
    return {"allowed": True, "reason": "cron_read_or_adapter_tool"}
