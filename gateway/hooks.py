"""
Event Hook System

A lightweight event-driven system that fires handlers at key lifecycle points.
Hooks are discovered from ~/.hermes/hooks/ directories, each containing:
  - HOOK.yaml  (metadata: name, description, events list)
  - handler.py (Python handler with async def handle(event_type, context))

Events:
  - gateway:startup     -- Gateway process starts
  - session:start       -- New session created (first message of a new session)
  - session:end         -- Session ends (user ran /new or /reset)
  - session:reset       -- Session reset completed (new session entry created)
  - agent:start         -- Agent begins processing a message
  - agent:step          -- Each turn in the tool-calling loop
  - agent:end           -- Agent finishes processing
  - command:*           -- Any slash command executed (wildcard match)

Errors in hooks are caught and logged but never block the main pipeline.

Context dict passed to ``agent:start`` / ``agent:end`` handlers:
  platform     -- source platform name (e.g. "telegram", "matrix", "slack")
  user_id      -- platform user id of the sender
  chat_id      -- platform chat id (group/DM identifier)
  thread_id    -- Telegram forum-topic id / thread root id (string; empty
                  when not in a thread / topic)
  chat_type    -- "dm" | "group" | "forum" (empty if unknown)
  session_id   -- Hermes session id
  message      -- inbound message text (truncated to 500 chars)

``agent:end`` adds:
  response     -- agent response text (truncated to 500 chars)

Handlers posting a follow-up into the same Telegram forum-topic should
include ``message_thread_id=int(thread_id)`` when ``chat_type == "forum"``
and ``thread_id`` is non-empty.
"""

import asyncio
import hashlib
import importlib.util
import logging
import os
import stat
import sys
import types
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import yaml

from hermes_cli.config import get_hermes_home


HOOKS_DIR = get_hermes_home() / "hooks"
logger = logging.getLogger(__name__)


class HookRegistry:
    """
    Discovers, loads, and fires event hooks.

    Usage:
        registry = HookRegistry()
        registry.discover_and_load()
        await registry.emit("agent:start", {"platform": "telegram", ...})
    """

    def __init__(
        self,
        hooks_dir: Path | None = None,
        *,
        strict_discovery: bool = False,
        quiet: bool = False,
    ):
        # event_type -> [handler_fn, ...]
        self._handlers: Dict[str, List[Callable]] = {}
        self._handler_owners: Dict[int, str] = {}
        self._handler_capabilities: Dict[int, frozenset[str]] = {}
        self._loaded_hooks: List[dict] = []  # metadata for listing
        self._hooks_dir = hooks_dir
        self._strict_discovery = strict_discovery
        self._quiet = quiet

    def _announce(self, message: str) -> None:
        if not self._quiet:
            print(message, flush=True)

    @staticmethod
    def _read_regular_file_at(directory_fd: int, name: str) -> bytes:
        descriptor = os.open(
            name,
            os.O_RDONLY
            | getattr(os, "O_NONBLOCK", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=directory_fd,
        )
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise OSError(f"{name} is not a regular file")
            with os.fdopen(descriptor, "rb", closefd=False) as handle:
                return handle.read()
        finally:
            os.close(descriptor)

    @staticmethod
    def _supports_dirfd_discovery() -> bool:
        return (
            hasattr(os, "O_DIRECTORY")
            and hasattr(os, "O_NOFOLLOW")
            and os.open in os.supports_dir_fd
            and os.listdir in os.supports_fd
        )

    @staticmethod
    def _open_directory_chain(path: Path) -> int:
        """Open an absolute directory without following any path component."""
        if not path.is_absolute() or any(part == ".." for part in path.parts):
            raise OSError("strict hook discovery requires an absolute lexical path")

        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        descriptor = os.open(path.anchor, flags)
        try:
            for part in path.parts[1:]:
                next_descriptor = os.open(part, flags, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = next_descriptor
            if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
                raise OSError(f"{path} is not a directory")
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    @staticmethod
    def _identity(metadata: os.stat_result) -> tuple[int, int, int]:
        return (metadata.st_dev, metadata.st_ino, metadata.st_mode)

    @staticmethod
    def _is_reparse_point(metadata: os.stat_result) -> bool:
        attributes = getattr(metadata, "st_file_attributes", 0)
        reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
        return bool(reparse_flag and attributes & reparse_flag)

    @classmethod
    def _snapshot_directory_chain(
        cls, path: Path
    ) -> tuple[tuple[int, int, int], ...]:
        if not path.is_absolute() or any(part == ".." for part in path.parts):
            raise OSError("strict hook discovery requires an absolute lexical path")
        current = Path(path.anchor)
        snapshot: list[tuple[int, int, int]] = []
        for part in path.parts[1:]:
            current = current / part
            metadata = os.lstat(current)
            if (
                stat.S_ISLNK(metadata.st_mode)
                or cls._is_reparse_point(metadata)
                or not stat.S_ISDIR(metadata.st_mode)
            ):
                raise OSError(f"unsafe hook directory component: {current}")
            snapshot.append(cls._identity(metadata))
        return tuple(snapshot)

    @classmethod
    def _read_regular_file_portable(
        cls,
        path: Path,
        expected_parent: tuple[tuple[int, int, int], ...],
    ) -> bytes:
        before_parent = cls._snapshot_directory_chain(path.parent)
        before = os.lstat(path)
        if before_parent != expected_parent or (
            stat.S_ISLNK(before.st_mode)
            or cls._is_reparse_point(before)
            or not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
        ):
            raise OSError(f"unsafe hook file: {path}")

        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NONBLOCK", 0))
        try:
            opened = os.fstat(descriptor)
            after_parent = cls._snapshot_directory_chain(path.parent)
            after = os.lstat(path)
            if (
                after_parent != expected_parent
                or cls._identity(before) != cls._identity(after)
                or cls._identity(opened) != cls._identity(after)
                or stat.S_ISLNK(after.st_mode)
                or cls._is_reparse_point(after)
                or not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1
                or after.st_nlink != 1
            ):
                raise OSError(f"hook file identity changed while opening: {path}")
            with os.fdopen(descriptor, "rb", closefd=False) as handle:
                return handle.read()
        finally:
            os.close(descriptor)

    @classmethod
    def _read_portable_hook(
        cls,
        hook_dir: Path,
        expected_root: tuple[tuple[int, int, int], ...],
    ) -> tuple[bytes, bytes]:
        hook_snapshot = cls._snapshot_directory_chain(hook_dir)
        if hook_snapshot[: len(expected_root)] != expected_root:
            raise OSError(f"hook root identity changed: {hook_dir}")
        manifest = cls._read_regular_file_portable(
            hook_dir / "HOOK.yaml", hook_snapshot
        )
        handler = cls._read_regular_file_portable(
            hook_dir / "handler.py", hook_snapshot
        )
        if cls._snapshot_directory_chain(hook_dir) != hook_snapshot:
            raise OSError(f"hook directory identity changed: {hook_dir}")
        return manifest, handler

    def _read_strict_hook(self, root_fd: int, name: str) -> tuple[bytes, bytes]:
        hook_fd = os.open(
            name,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=root_fd,
        )
        try:
            if not stat.S_ISDIR(os.fstat(hook_fd).st_mode):
                raise OSError(f"{name} is not a directory")
            return (
                self._read_regular_file_at(hook_fd, "HOOK.yaml"),
                self._read_regular_file_at(hook_fd, "handler.py"),
            )
        finally:
            os.close(hook_fd)

    @property
    def loaded_hooks(self) -> List[dict]:
        """Return metadata about all loaded hooks."""
        return list(self._loaded_hooks)

    @property
    def hooks_dir(self) -> Path:
        """Return the Profile hook root owned by this registry."""
        return self._hooks_dir or HOOKS_DIR

    def _register_builtin_hooks(self) -> None:
        """Register built-in hooks that are always active.

        Currently empty — no shipped built-in hooks. Kept as the extension
        point for future always-on gateway hooks so they drop in without
        re-plumbing discover_and_load().
        """
        return

    def discover_and_load(self) -> None:
        """
        Scan the hooks directory for hook directories and load their handlers.

        Also registers built-in hooks that are always active.

        Each hook directory must contain:
          - HOOK.yaml with at least 'name' and 'events' keys
          - handler.py with a top-level 'handle' function (sync or async)
        """
        self._register_builtin_hooks()

        hooks_dir = self.hooks_dir
        root_fd: int | None = None
        portable_root: tuple[tuple[int, int, int], ...] | None = None
        if self._strict_discovery:
            if self._supports_dirfd_discovery():
                root_fd = self._open_directory_chain(hooks_dir)
            else:
                portable_root = self._snapshot_directory_chain(hooks_dir)
        elif not hooks_dir.exists():
            return

        if root_fd is not None and not stat.S_ISDIR(os.fstat(root_fd).st_mode):
            os.close(root_fd)
            return

        try:
            names = sorted(os.listdir(root_fd if root_fd is not None else hooks_dir))
            for hook_dir_name in names:
                hook_dir = hooks_dir / hook_dir_name
                if not self._strict_discovery and not hook_dir.is_dir():
                    continue

                manifest_path = hook_dir / "HOOK.yaml"
                handler_path = hook_dir / "handler.py"

                try:
                    if root_fd is not None:
                        manifest_bytes, handler_bytes = self._read_strict_hook(
                            root_fd, hook_dir_name
                        )
                        manifest_text = manifest_bytes.decode("utf-8")
                    elif portable_root is not None:
                        manifest_bytes, handler_bytes = self._read_portable_hook(
                            hook_dir, portable_root
                        )
                        manifest_text = manifest_bytes.decode("utf-8")
                    else:
                        if not manifest_path.exists() or not handler_path.exists():
                            continue
                        manifest_text = manifest_path.read_text(encoding="utf-8")
                        handler_bytes = b""

                    manifest = yaml.safe_load(manifest_text)
                    if not manifest or not isinstance(manifest, dict):
                        self._announce(
                            f"[hooks] Skipping {hook_dir.name}: invalid HOOK.yaml"
                        )
                        continue

                    hook_name = manifest.get("name", hook_dir.name)
                    events = manifest.get("events", [])
                    if not events:
                        self._announce(
                            f"[hooks] Skipping {hook_name}: no events declared"
                        )
                        continue

                    module_name = f"hermes_hook_{hook_name}"
                    if root_fd is not None or portable_root is not None:
                        path_identity = hashlib.sha256(
                            str(hook_dir).encode("utf-8")
                        ).hexdigest()[:16]
                        module_name = f"{module_name}_{path_identity}"
                    if root_fd is not None or portable_root is not None:
                        module = types.ModuleType(module_name)
                        module.__file__ = str(handler_path)
                        sys.modules[module_name] = module
                        try:
                            exec(
                                compile(handler_bytes, str(handler_path), "exec"),
                                module.__dict__,
                            )
                        except Exception:
                            sys.modules.pop(module_name, None)
                            raise
                    else:
                        spec = importlib.util.spec_from_file_location(
                            module_name, handler_path
                        )
                        if spec is None or spec.loader is None:
                            self._announce(
                                f"[hooks] Skipping {hook_name}: could not load handler.py"
                            )
                            continue

                        module = importlib.util.module_from_spec(spec)
                        sys.modules[module_name] = module
                        try:
                            spec.loader.exec_module(module)
                        except Exception:
                            sys.modules.pop(module_name, None)
                            raise

                    handle_fn = getattr(module, "handle", None)
                    if handle_fn is None:
                        self._announce(
                            f"[hooks] Skipping {hook_name}: no 'handle' function found"
                        )
                        continue

                    for event in events:
                        self._handlers.setdefault(event, []).append(handle_fn)
                    self._handler_owners[id(handle_fn)] = hook_dir.name
                    raw_capabilities = manifest.get("capabilities", [])
                    capabilities = (
                        frozenset(
                            value.strip()
                            for value in raw_capabilities
                            if isinstance(value, str) and value.strip()
                        )
                        if isinstance(raw_capabilities, list)
                        else frozenset()
                    )
                    self._handler_capabilities[id(handle_fn)] = capabilities

                    self._loaded_hooks.append({
                        "name": hook_name,
                        "description": manifest.get("description", ""),
                        "events": events,
                        "path": str(hook_dir),
                    })

                    self._announce(
                        f"[hooks] Loaded hook '{hook_name}' for events: {events}"
                    )

                except (FileNotFoundError, NotADirectoryError):
                    # The hook root may also contain helper modules or ordinary
                    # directories. They are not malformed hook declarations.
                    continue
                except Exception as e:
                    if self._strict_discovery and self._quiet:
                        logger.warning(
                            "Strict hook discovery rejected %s",
                            hook_dir,
                            exc_info=True,
                        )
                    else:
                        self._announce(
                            f"[hooks] Error loading hook {hook_dir.name}: {e}"
                        )
        finally:
            if root_fd is not None:
                os.close(root_fd)

    def _resolve_handlers(self, event_type: str) -> List[Callable]:
        """Return all handlers that should fire for ``event_type``.

        Exact matches fire first, followed by wildcard matches (e.g.
        ``command:*`` matches ``command:reset``).
        """
        handlers = list(self._handlers.get(event_type, []))
        if ":" in event_type:
            base = event_type.split(":")[0]
            wildcard_key = f"{base}:*"
            handlers.extend(self._handlers.get(wildcard_key, []))
        return handlers

    def resolve_handlers_with_metadata(
        self,
        event_type: str,
    ) -> List[tuple[Callable, Dict[str, Any]]]:
        """Resolve handlers with loader-owned identity and capabilities."""
        return [
            (
                handler,
                {
                    "owner": self._handler_owners.get(id(handler), ""),
                    "capabilities": sorted(self._handler_capabilities.get(id(handler), ())),
                },
            )
            for handler in self._resolve_handlers(event_type)
        ]

    async def emit(self, event_type: str, context: Optional[Dict[str, Any]] = None) -> None:
        """
        Fire all handlers registered for an event, discarding return values.

        Supports wildcard matching: handlers registered for "command:*" will
        fire for any "command:..." event. Handlers registered for a base type
        like "agent" won't fire for "agent:start" -- only exact matches and
        explicit wildcards.

        Args:
            event_type: The event identifier (e.g. "agent:start").
            context:    Optional dict with event-specific data.
        """
        if context is None:
            context = {}

        for fn in self._resolve_handlers(event_type):
            try:
                result = fn(event_type, context)
                # Support both sync and async handlers
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                print(f"[hooks] Error in handler for '{event_type}': {e}", flush=True)

    async def emit_collect(
        self,
        event_type: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> List[Any]:
        """Fire handlers and return their non-None return values in order.

        Like :meth:`emit` but captures each handler's return value. Used for
        decision-style hooks (e.g. ``command:<name>`` policies that want to
        allow/deny/rewrite the command before normal dispatch).

        Exceptions from individual handlers are logged but do not abort the
        remaining handlers.
        """
        if context is None:
            context = {}

        results: List[Any] = []
        for fn in self._resolve_handlers(event_type):
            try:
                result = fn(event_type, context)
                if asyncio.iscoroutine(result):
                    result = await result
                if result is not None:
                    results.append(result)
            except Exception as e:
                print(f"[hooks] Error in handler for '{event_type}': {e}", flush=True)
        return results
