"""
Cron job storage and management.

Jobs are stored in ~/.hermes/cron/jobs.json
Output is saved to ~/.hermes/cron/output/{job_id}/{timestamp}.md
"""

import contextlib
import copy
from contextvars import ContextVar
from dataclasses import dataclass
import hashlib
import json
import logging
import shutil
import tempfile
import threading
import time
import os
import re
import stat
import sys
import uuid

# Cross-process advisory file locking for jobs.json critical sections.
# fcntl is Unix-only; on Windows fall back to msvcrt. Non-strict callers may
# degrade to in-process locking for read-only liveness checks; every durable
# jobs.json mutation must require the cross-process capability.
try:
    import fcntl
except ImportError:  # pragma: no cover - non-Unix
    fcntl = None
try:
    import msvcrt
except ImportError:  # pragma: no cover - non-Windows
    msvcrt = None
from datetime import datetime, timedelta
from pathlib import Path
from hermes_constants import get_hermes_home
from typing import Optional, Dict, List, Any, Set, Tuple, Union

logger = logging.getLogger(__name__)

from hermes_time import now as _hermes_now
from utils import atomic_replace

try:
    from croniter import croniter
    HAS_CRONITER = True
except ImportError:
    HAS_CRONITER = False

# =============================================================================
# Configuration
# =============================================================================

# Cron is per-profile by design (issue #4707). Each profile owns its own cron
# store under its own HERMES_HOME, and a profile-scoped gateway runs that
# profile's jobs under that same HERMES_HOME — so a job authored in profile
# `coder` lives in `~/.hermes/profiles/coder/cron/jobs.json` and executes with
# `coder`'s `.env`, `config.yaml`, and skills. We deliberately anchor on
# `get_hermes_home()` (the active profile home), NOT `get_default_hermes_root()`
# (the shared root). Anchoring at the root would funnel every profile's jobs
# into one shared `jobs.json` and run them under whatever HERMES_HOME the
# ticker process happens to have — leaking config/credentials/skills across
# profiles (the security boundary #4707 was filed for). Do NOT change this to
# the default root: that re-breaks per-profile isolation. See also the dynamic
# `_get_hermes_home()` / `_get_lock_paths()` resolution in cron/scheduler.py.
HERMES_DIR = Path(
    os.path.abspath(os.fspath(get_hermes_home().expanduser()))
)
# These constants remain the default-profile fallback and a compatibility
# surface for existing callers/tests. Cross-profile callers must scope paths
# with use_cron_store() instead of mutating them process-wide.
CRON_DIR = HERMES_DIR / "cron"
JOBS_FILE = CRON_DIR / "jobs.json"
# Heartbeat file the in-process ticker touches on every loop iteration. The
# gateway process and the (separate) ``hermes cron status`` process share it
# so status can tell whether the ticker THREAD is alive, not just whether the
# gateway PROCESS exists — a ticker that dies silently inside a live gateway
# would otherwise report healthy (#32612, #32895).
TICKER_HEARTBEAT_FILE = CRON_DIR / "ticker_heartbeat"
# Last tick that completed WITHOUT raising. Distinguishing this from the plain
# heartbeat lets status detect a ticker that is alive but failing every tick.
TICKER_SUCCESS_FILE = CRON_DIR / "ticker_last_success"
# Default ticker loop interval (seconds). The single source of truth shared by
# the in-process ticker (cron/scheduler_provider.py) and the staleness
# threshold in `hermes cron status` (hermes_cli/cron.py), so the two never
# drift apart.
TICKER_INTERVAL_SECONDS = 60

# In-process lock protecting load_jobs→modify→save_jobs cycles.
# Required when tick() runs jobs in parallel threads — without this,
# concurrent mark_job_run / advance_next_run calls can clobber each other.
_jobs_file_lock = threading.RLock()
_jobs_lock_state = threading.local()


def _build_jobs_lock_capability_owner():
    """Keep active capability identity and nonce private to Core's lock owner."""
    owner_nonce = object()
    active_by_thread: Dict[int, Any] = {}

    class JobsLockCapability:
        __slots__ = (
            "_JobsLockCapability__active",
            "_JobsLockCapability__handle",
            "_JobsLockCapability__lock_device",
            "_JobsLockCapability__lock_inode",
            "_JobsLockCapability__owner_nonce",
            "_JobsLockCapability__owner_thread",
            "_JobsLockCapability__cron_device",
            "_JobsLockCapability__cron_fd",
            "_JobsLockCapability__cron_inode",
        )

        def __init__(self, nonce: object, handle: Any, cron_fd: int):
            if nonce is not owner_nonce:
                raise TypeError("Core jobs lock capability is internal")
            opened = os.fstat(handle.fileno())
            opened_cron = os.fstat(cron_fd)
            self.__active = True
            self.__lock_device = opened.st_dev
            self.__lock_inode = opened.st_ino
            self.__owner_nonce = nonce
            self.__owner_thread = threading.get_ident()
            self.__handle = handle
            self.__cron_device = opened_cron.st_dev
            self.__cron_fd = cron_fd
            self.__cron_inode = opened_cron.st_ino

        def snapshot(self, nonce: object) -> Dict[str, Any] | None:
            if (
                nonce is not owner_nonce
                or self.__owner_nonce is not owner_nonce
                or not self.__active
                or threading.get_ident() != self.__owner_thread
            ):
                return None
            try:
                opened = os.fstat(self.__handle.fileno())
                opened_cron = os.fstat(self.__cron_fd)
                lock_entry = os.stat(
                    ".jobs.lock",
                    dir_fd=self.__cron_fd,
                    follow_symlinks=False,
                )
            except (OSError, ValueError):
                return None
            if (
                not stat.S_ISDIR(opened_cron.st_mode)
                or opened_cron.st_dev != self.__cron_device
                or opened_cron.st_ino != self.__cron_inode
                or not stat.S_ISREG(lock_entry.st_mode)
                or opened.st_dev != self.__lock_device
                or opened.st_ino != self.__lock_inode
                or lock_entry.st_dev != self.__lock_device
                or lock_entry.st_ino != self.__lock_inode
            ):
                return None
            return {
                "owner_thread_id": self.__owner_thread,
                "cron_device": self.__cron_device,
                "cron_inode": self.__cron_inode,
                "lock_device": self.__lock_device,
                "lock_inode": self.__lock_inode,
            }

        def invalidate(self, nonce: object) -> None:
            if nonce is not owner_nonce or self.__owner_nonce is not owner_nonce:
                raise TypeError("Core jobs lock capability is internal")
            self.__active = False

    def activate(handle: Any, cron_fd: int) -> object:
        owner_thread = threading.get_ident()
        if owner_thread in active_by_thread:
            raise RuntimeError("Core jobs lock capability owner is already active")
        capability = JobsLockCapability(owner_nonce, handle, cron_fd)
        active_by_thread[owner_thread] = capability
        return capability

    def issue() -> object:
        capability = active_by_thread.get(threading.get_ident())
        if capability is None or capability.snapshot(owner_nonce) is None:
            raise CronJobGovernanceError(
                "Cron job persistence needs administrator review "
                "(Core jobs lock capability unavailable)."
            )
        return capability

    def validate(capability: object) -> Dict[str, Any] | None:
        current = active_by_thread.get(threading.get_ident())
        if capability is not current:
            return None
        return current.snapshot(owner_nonce)

    def invalidate(capability: object | None) -> None:
        owner_thread = threading.get_ident()
        current = active_by_thread.get(owner_thread)
        if capability is None or capability is not current:
            return
        active_by_thread.pop(owner_thread, None)
        current.invalidate(owner_nonce)

    def duplicate_cron_fd() -> int:
        current = active_by_thread.get(threading.get_ident())
        if current is None or current.snapshot(owner_nonce) is None:
            raise CronJobGovernanceError(
                "Cron job persistence needs administrator review "
                "(Core jobs lock capability unavailable)."
            )
        return os.dup(current._JobsLockCapability__cron_fd)

    return activate, issue, validate, invalidate, duplicate_cron_fd


(
    _activate_jobs_lock_capability,
    _issue_jobs_lock_capability,
    _validate_jobs_lock_capability,
    _invalidate_jobs_lock_capability,
    _duplicate_active_jobs_lock_cron_fd,
) = _build_jobs_lock_capability_owner()

# Upper bound on waiting for the cross-process .jobs.lock flock (#60703).
# Every cron function in the process funnels through _jobs_lock(), and the
# flock is taken while holding the process-wide RLock — so an unbounded wait
# on a lock held by a wedged sibling process silently freezes the ticker
# heartbeat and every job forever.  30s is orders of magnitude above any
# legitimate critical section (field updates only) while keeping the ticker's
# worst-case stall well under one status-alarm threshold.
_JOBS_LOCK_TIMEOUT_SECONDS = 30.0
OUTPUT_DIR = CRON_DIR / "output"
ONESHOT_GRACE_SECONDS = 120


@dataclass(frozen=True)
class _CronStorePaths:
    cron_dir: Path
    jobs_file: Path
    output_dir: Path


_cron_store_override: ContextVar[Optional[_CronStorePaths]] = ContextVar(
    "cron_store_override",
    default=None,
)


# Import-time snapshot of the compatibility constants, so deliberate
# re-pointing of the module surface (monkeypatched CRON_DIR/JOBS_FILE/
# OUTPUT_DIR — the documented escape hatch existing tests/embedders use)
# is distinguishable from the constants merely being stale.
_IMPORT_STORE = _CronStorePaths(CRON_DIR, JOBS_FILE, OUTPUT_DIR)


def _current_cron_store() -> _CronStorePaths:
    """Return paths pinned to this execution context's profile.

    Precedence, most explicit first:

    1. an active use_cron_store() override (ContextVar);
    2. deliberately re-pointed module constants — if CRON_DIR/JOBS_FILE/
       OUTPUT_DIR no longer match their import-time values, someone chose
       the documented process-wide compatibility surface; honor it;
    3. the ACTIVE profile home, resolved fresh via get_hermes_home()
       (context-local override, then the HERMES_HOME env var) — so a test
       or embedder that re-points HERMES_HOME after this module was
       imported reads/writes ITS OWN store, not whatever jobs.json the
       import happened to freeze (the filed incident: fixtures that patched
       the env too late silently rewrote the user's real jobs file);
    4. the import-time constants (home unchanged since import — the common
       path, returned unchanged).
    """
    override = _cron_store_override.get()
    if override is not None:
        return override
    live_constants = _CronStorePaths(CRON_DIR, JOBS_FILE, OUTPUT_DIR)
    if live_constants != _IMPORT_STORE:
        return live_constants
    home = get_hermes_home().resolve()
    if home == HERMES_DIR:
        return live_constants
    cron_dir = home / "cron"
    return _CronStorePaths(cron_dir, cron_dir / "jobs.json", cron_dir / "output")


@contextlib.contextmanager
def use_cron_store(home: Union[str, Path]):
    """Route cron storage to ``home`` without mutating process globals."""
    cron_dir = Path(os.path.abspath(os.fspath(Path(home).expanduser()))) / "cron"
    token = _cron_store_override.set(
        _CronStorePaths(
            cron_dir=cron_dir,
            jobs_file=cron_dir / "jobs.json",
            output_dir=cron_dir / "output",
        )
    )
    try:
        yield
    finally:
        _cron_store_override.reset(token)


def get_cron_output_dir() -> Path:
    """Return the output directory for the active cron store context."""
    return _current_cron_store().output_dir


# Fallback stale-recovery window for a one-shot's running-claim (#59229) when
# the cron inactivity timeout is disabled (HERMES_CRON_TIMEOUT=0 → unlimited),
# in which case no finite run bound exists to derive from. Also acts as the
# floor for the derived value so a very short configured timeout can't make the
# claim expire mid-run.
ONESHOT_RUN_CLAIM_TTL_SECONDS = 1800
OPERATIONAL_NOTICE_RECEIPT_LIMIT = 64

# The derived TTL is the cron inactivity timeout times this headroom multiplier.
# A healthy run clears its claim via mark_job_run() long before the TTL; the
# TTL only recovers a claim left by a tick that DIED mid-run. HERMES_CRON_TIMEOUT
# is an *inactivity* limit, not a wall-clock cap — a job that keeps producing
# output legitimately runs past it — so the multiplier gives comfortable
# headroom over any healthy run before we treat a claim as stale.
_ONESHOT_RUN_CLAIM_TTL_HEADROOM = 3

_DEFAULT_CRON_INACTIVITY_TIMEOUT = 600.0


def _oneshot_run_claim_ttl_seconds() -> float:
    """Resolve the one-shot running-claim stale-recovery TTL.

    Derived from ``HERMES_CRON_TIMEOUT`` (the cron inactivity timeout the
    scheduler enforces on each run) so the safety valve tracks how long a run
    is actually allowed to go quiet, instead of a magic constant:

    - unset / invalid → default 600s inactivity limit → TTL = 1800s
    - ``0`` (unlimited runs) → no finite bound to derive from → fall back to
      ``ONESHOT_RUN_CLAIM_TTL_SECONDS``
    - positive N → ``max(N * headroom, ONESHOT_RUN_CLAIM_TTL_SECONDS)`` so a
      tiny configured timeout can never expire a claim mid-run.
    """
    raw = os.getenv("HERMES_CRON_TIMEOUT", "").strip()
    timeout = _DEFAULT_CRON_INACTIVITY_TIMEOUT
    if raw:
        try:
            timeout = float(raw)
        except (ValueError, TypeError):
            timeout = _DEFAULT_CRON_INACTIVITY_TIMEOUT
    if timeout <= 0:
        # Unlimited runs — cannot bound; use the fixed fallback floor.
        return float(ONESHOT_RUN_CLAIM_TTL_SECONDS)
    return max(
        timeout * _ONESHOT_RUN_CLAIM_TTL_HEADROOM,
        float(ONESHOT_RUN_CLAIM_TTL_SECONDS),
    )


def _job_running_in_this_process(job_id: str) -> bool:
    """Return True when the scheduler in THIS process is still running ``job_id``.

    Direct liveness signal for stale-entry recovery (#62002): the run_claim
    TTL alone cannot distinguish "the claiming tick died" from "the run is
    alive but slow" — a run stalled on network I/O (or a laptop that slept
    mid-run) legitimately outlives the TTL. The in-process ticker and the run
    share this process, so the scheduler's running set settles the common
    single-gateway case without any claim-age guesswork.

    Imported lazily: the scheduler imports this module at load, so a
    module-level import here would be circular.
    """
    try:
        from cron.scheduler import get_running_job_ids
        return job_id in get_running_job_ids()
    except Exception:
        return False


def _jobs_lock_file() -> Path:
    """Return the advisory lock path for the current cron directory."""
    return _current_cron_store().cron_dir / ".jobs.lock"


def _open_existing_directory_chain(path: Path) -> int:
    """Open one absolute directory through its lexical non-symlink chain."""
    absolute = Path(os.path.abspath(os.fspath(path.expanduser())))
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0)
    directory_flags |= getattr(os, "O_NONBLOCK", 0)
    # A ProfileAnchor on Darwin exposes a stable ``/.vol/<dev>/<ino>``
    # directory identity.  It is not a lexical component tree, so traversing
    # it one component at a time fails even though the kernel can open the
    # identity atomically.  Accept only the exact numeric identity form.
    parts = absolute.parts
    if (
        len(parts) == 4
        and parts[0] == os.sep
        and parts[1] == ".vol"
        and parts[2].isdigit()
        and parts[3].isdigit()
    ):
        descriptor = os.open(absolute, directory_flags)
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISDIR(opened.st_mode):
                raise OSError("Cron identity path is not a directory")
            return descriptor
        except Exception:
            os.close(descriptor)
            raise
    descriptor = os.open(absolute.anchor or os.sep, directory_flags)
    try:
        for component in absolute.parts[1:]:
            entry = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            if not stat.S_ISDIR(entry.st_mode):
                raise OSError("Cron path contains a symlink or non-directory")
            child_fd = os.open(component, directory_flags, dir_fd=descriptor)
            opened = os.fstat(child_fd)
            if (
                not stat.S_ISDIR(opened.st_mode)
                or opened.st_dev != entry.st_dev
                or opened.st_ino != entry.st_ino
            ):
                os.close(child_fd)
                raise OSError("Cron directory changed while opening")
            os.close(descriptor)
            descriptor = child_fd
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _open_existing_cron_directory() -> int:
    """Open the configured cron directory without creating or following it."""
    cron_dir = _current_cron_store().cron_dir
    parent_fd = _open_existing_directory_chain(cron_dir.parent)
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0)
    directory_flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        entry = os.stat(cron_dir.name, dir_fd=parent_fd, follow_symlinks=False)
        if not stat.S_ISDIR(entry.st_mode):
            raise OSError("Cron directory is not an existing regular directory")
        cron_fd = os.open(cron_dir.name, directory_flags, dir_fd=parent_fd)
        opened = os.fstat(cron_fd)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or opened.st_dev != entry.st_dev
            or opened.st_ino != entry.st_ino
        ):
            os.close(cron_fd)
            raise OSError("Cron directory changed while opening")
        return cron_fd
    finally:
        os.close(parent_fd)


def _open_existing_jobs_lock():
    """Open Core's existing lock without creating or following Profile state."""
    cron_fd = -1
    descriptor = -1
    try:
        cron_fd = _open_existing_cron_directory()
        lock_entry = os.stat(
            ".jobs.lock",
            dir_fd=cron_fd,
            follow_symlinks=False,
        )
        if not stat.S_ISREG(lock_entry.st_mode):
            raise OSError("Cron jobs lock is not an existing regular file")
        descriptor = os.open(
            ".jobs.lock",
            os.O_RDWR
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
            dir_fd=cron_fd,
        )
        opened_lock = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened_lock.st_mode)
            or opened_lock.st_dev != lock_entry.st_dev
            or opened_lock.st_ino != lock_entry.st_ino
        ):
            raise OSError("Cron jobs lock changed while opening")
        handle = os.fdopen(descriptor, "r+", encoding="utf-8")
        descriptor = -1
        retained_cron_fd = cron_fd
        cron_fd = -1
        return handle, retained_cron_fd
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if cron_fd >= 0:
            os.close(cron_fd)


def _acquire_bounded_flock(descriptor: int, *, label: str) -> None:
    """Take one POSIX advisory lock without wedging the cron writer path."""
    if fcntl is None:
        raise OSError(f"{label} advisory lock is unavailable")
    deadline = time.monotonic() + _JOBS_LOCK_TIMEOUT_SECONDS
    while True:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except (OSError, IOError):
            if time.monotonic() >= deadline:
                raise TimeoutError(f"timed out waiting for {label}")
            time.sleep(0.1)


@contextlib.contextmanager
def _jobs_lock(*, require_cross_process: bool = False):
    """Serialize a load_jobs→modify→save_jobs critical section.

    Combines the in-process threading lock (cheap mutual exclusion between
    the gateway's parallel tick threads) with a cross-process advisory file
    lock on ``<cron dir>/.jobs.lock`` (mutual exclusion between the gateway process
    and standalone ``hermes`` CLI invocations, which previously shared no lock
    at all — a `cron pause` could be silently clobbered by a concurrent
    gateway write, leaving a "paused" job still firing).

    The flock is bounded because every critical section is short (field
    updates only — no agent execution). Non-strict callers may retain the
    historical in-process-only fallback for read-only liveness checks;
    persistence callers must set ``require_cross_process=True`` and fail
    closed when the capability is unavailable.

    Nested calls in the same thread reuse the held lock so legacy callers that
    invoke save_jobs() inside a broader mutation section don't deadlock or try
    to reacquire the advisory file lock.
    """
    depth = getattr(_jobs_lock_state, "depth", 0)
    if depth:
        if require_cross_process and not getattr(_jobs_lock_state, "cross_process_acquired", False):
            raise CronJobGovernanceError(
                "Cron job persistence needs administrator review (strict jobs lock unavailable)."
            )
        _jobs_lock_state.depth = depth + 1
        try:
            yield
        finally:
            _jobs_lock_state.depth -= 1
        return

    with _jobs_file_lock:
        _jobs_lock_state.depth = 1
        _jobs_lock_state.cross_process_acquired = False
        lock_fd = None
        lock_dir_fd = -1
        cron_directory_guard_acquired = False
        lock_capability = None
        try:
            try:
                lock_fd, lock_dir_fd = _open_existing_jobs_lock()
            except (OSError, IOError) as e:
                raise CronJobGovernanceError(
                    "Cron job persistence needs administrator review "
                    "(strict jobs lock unavailable)."
                ) from e
            try:
                if fcntl is not None:
                    # Bounded acquisition (#60703): a plain blocking
                    # fcntl.flock(LOCK_EX) here has NO timeout, and it is
                    # taken while holding the process-wide _jobs_file_lock
                    # RLock above.  If another process wedges while holding
                    # .jobs.lock (e.g. an old gateway draining through a
                    # restart), a single blocked acquirer freezes EVERY cron
                    # function in this process — including the ticker's
                    # get_due_jobs() — silently and forever: the heartbeat
                    # file stops updating and all jobs stop firing with no
                    # error logged.  Poll LOCK_NB against a deadline instead;
                    # on timeout, non-strict read-only callers log loudly and
                    # retain in-process protection. Durable mutations require
                    # the capability and fail closed below.
                    _acquire_bounded_flock(lock_fd.fileno(), label="strict cron jobs lock")
                    # The lock filename can be atomically replaced after the
                    # file lock is acquired. Retaining an advisory lock on the
                    # already-open cron directory keeps every replacement
                    # contender in one writer domain until this transaction
                    # exits, rather than allowing two lock inodes to diverge.
                    _acquire_bounded_flock(lock_dir_fd, label="strict cron directory guard")
                    cron_directory_guard_acquired = True
                    _jobs_lock_state.cross_process_acquired = True
                    lock_capability = _activate_jobs_lock_capability(
                        lock_fd,
                        lock_dir_fd,
                    )
                elif msvcrt is not None:
                    raise OSError("strict cron directory guard is unavailable on this platform")
                elif require_cross_process:
                    raise OSError("cross-process file locking is unavailable")
            except (OSError, IOError) as e:
                if cron_directory_guard_acquired and fcntl is not None:
                    try:
                        fcntl.flock(lock_dir_fd, fcntl.LOCK_UN)
                    except (OSError, IOError):
                        pass
                    cron_directory_guard_acquired = False
                if lock_fd is not None and fcntl is not None:
                    try:
                        fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    except (OSError, IOError):
                        pass
                if lock_fd is not None:
                    try:
                        lock_fd.close()
                    except OSError:
                        pass
                    lock_fd = None
                if lock_dir_fd >= 0:
                    try:
                        os.close(lock_dir_fd)
                    except OSError:
                        pass
                    finally:
                        lock_dir_fd = -1
                if require_cross_process:
                    raise CronJobGovernanceError(
                        "Cron job persistence needs administrator review (strict jobs lock unavailable)."
                    ) from e
                # Non-strict callers are read-only/liveness probes. Durable
                # mutations always require the cross-process capability.
                logger.error(
                    "Timed out or failed to acquire jobs.json cross-process lock (%s); "
                    "proceeding with in-process lock only",
                    e,
                )
            try:
                yield
            finally:
                _invalidate_jobs_lock_capability(lock_capability)
                if cron_directory_guard_acquired and fcntl is not None:
                    try:
                        fcntl.flock(lock_dir_fd, fcntl.LOCK_UN)
                    except (OSError, IOError):
                        pass
                if lock_fd is not None:
                    try:
                        if fcntl is not None:
                            fcntl.flock(lock_fd, fcntl.LOCK_UN)
                        elif msvcrt is not None:
                            getattr(msvcrt, "locking")(lock_fd.fileno(), getattr(msvcrt, "LK_UNLCK"), 1)
                    except (OSError, IOError):
                        pass
                    finally:
                        lock_fd.close()
                if lock_dir_fd >= 0:
                    os.close(lock_dir_fd)
        finally:
            _jobs_lock_state.depth = 0
            _jobs_lock_state.cross_process_acquired = False

# Fields on a cron job that must never change after creation. ``id`` is used
# as a filesystem path component under ``OUTPUT_DIR``; allowing it to be
# updated lets an unsafe value (``../escape``, absolute path, nested) leak
# into output writes/deletes.
_IMMUTABLE_JOB_FIELDS = frozenset({
    "id",
    "creation_governance_receipt",
    "last_runtime_admission_receipt",
    "last_delivery_receipt",
    "last_run_outcome_receipt",
    "active_run_outcome_claim",
})
_CRON_GOVERNANCE_ENV = "HERMES_CRON_CREATION_GOVERNANCE_REQUIRED"
_CRON_GOVERNANCE_PLUGIN = "hck-tool-boundary"
_CRON_RESUME_RECEIPT_FIELD = "cron_persist_resume_receipt"
_CRON_RUNTIME_ADMISSION_RECEIPT_SCHEMA = "cron-runtime-admission/v1"
_CRON_RUNTIME_ADMISSION_RECEIPT_FIELD = "last_runtime_admission_receipt"
_CRON_DELIVERY_RECEIPT_SCHEMA = "cron-delivery/v1"
_CRON_DELIVERY_RECEIPT_FIELD = "last_delivery_receipt"
_CRON_RUN_OUTCOME_RECEIPT_SCHEMA = "cron-run-outcome/v1"
_CRON_RUN_OUTCOME_RECEIPT_FIELD = "last_run_outcome_receipt"
_CRON_RUN_OUTCOME_CLAIM_SCHEMA = "cron-run-claim/v1"
_CRON_RUN_OUTCOME_CLAIM_FIELD = "active_run_outcome_claim"
_CRON_RUN_OUTCOME_CLAIM_MAX_TTL_SECONDS = 24 * 60 * 60
_CRON_RUN_IMPLEMENTATION_SCHEMA = "cron-run-implementation/v1"
_CRON_SUPPORT_ARTIFACT_SCHEMA = "cron-support-artifacts/v1"
_CRON_SCRIPT_EXECUTION_SNAPSHOT_SCHEMA = "cron-script-execution-snapshot/v1"
_CRON_CHECKPOINT_INVARIANT_SCHEMA = "cron-checkpoint-invariant/v1"
_CRON_DELIVERY_BINDING_SCHEMA = "cron-run-delivery-binding/v1"
_CRON_RUN_ARTIFACT_MAX_BYTES = 8 * 1024 * 1024
_CRON_INTERPRETER_ARTIFACT_MAX_BYTES = 256 * 1024 * 1024
_CRON_ARTIFACT_HASH_CHUNK_BYTES = 1024 * 1024
_CRON_SUPPORT_ARTIFACT_MAX_FILES = 512
_CRON_SUPPORT_ARTIFACT_MAX_BYTES = 8 * 1024 * 1024
_CRON_SUPPORT_IGNORED_DIRS = frozenset(
    {".git", ".venv", "venv", "node_modules", "__pycache__"}
)
_CRON_TRUSTED_BASH_PATHS = (
    "/bin/bash",
    "/usr/bin/bash",
    "C:/Program Files/Git/bin/bash.exe",
    "C:/Program Files/Git/usr/bin/bash.exe",
)
_CRON_CLAIM_UNSET = object()
_CRON_RUN_IMPLEMENTATION_FIELDS = (
    "prompt",
    "skills",
    "skill",
    "model",
    "provider",
    "base_url",
    "script",
    "no_agent",
    "context_from",
    "enabled_toolsets",
    "workdir",
    "max_turns",
    "run_timeout_seconds",
)
_CRON_CHECKPOINT_CONTRACT_FIELDS = (
    "checkpoint_policy",
    "checkpoint_invariant",
)
_CRON_GOVERNANCE_PATCH_FIELDS = frozenset({
    "operation",
    "authorized_behavior_ref",
    "process_charter_ref",
    "approval_evidence_ref",
    "read_scope_ref",
    "disclosure_policy_ref",
    "risk_tier",
    "implementation_categories",
    "implementation_path_evidence_ref",
    "actionable_output",
    "source_route",
    "join_keys",
    "creation_governance_receipt",
    "enabled",
    "state",
    "paused_reason",
})
_CRON_GOVERNANCE_RUNTIME_FIELDS = frozenset({
    "created_at",
    "enabled",
    "last_delivery_error",
    "last_delivery_recovered_at",
    "last_error",
    "last_output",
    "last_run_at",
    "last_run_id",
    "last_status",
    "next_run_at",
    "operational_notice_receipts",
    "paused_at",
    "paused_reason",
    "run_claim",
    "fire_claim",
    "state",
    _CRON_RUNTIME_ADMISSION_RECEIPT_FIELD,
    _CRON_DELIVERY_RECEIPT_FIELD,
    _CRON_RUN_OUTCOME_RECEIPT_FIELD,
    _CRON_RUN_OUTCOME_CLAIM_FIELD,
})
_CRON_GOVERNANCE_SELF_REFERENTIAL_FIELDS = frozenset({
    "candidate_hash",
    "creation_governance_receipt",
    _CRON_RESUME_RECEIPT_FIELD,
    "join_keys",
})


def _cron_governance_material(job: Dict[str, Any]) -> Dict[str, Any]:
    """Bind every definition field except explicit runtime/self references."""
    material = {
        key: copy.deepcopy(value)
        for key, value in job.items()
        if key not in _CRON_GOVERNANCE_RUNTIME_FIELDS
        and key not in _CRON_GOVERNANCE_SELF_REFERENTIAL_FIELDS
    }
    repeat = material.get("repeat")
    if isinstance(repeat, dict):
        material["repeat"] = {
            key: copy.deepcopy(value)
            for key, value in repeat.items()
            if key != "completed"
        }
    return material


def _cron_governance_material_changed(
    before: Dict[str, Any],
    after: Dict[str, Any],
) -> bool:
    """Return whether normalized authorization-bound job material changed."""
    return _cron_governance_material(before) != _cron_governance_material(after)


def _cron_update_may_change_governance_material(updates: Dict[str, Any]) -> bool:
    """Conservatively select the strict lock before the stored Job is loaded."""
    for key, value in (updates or {}).items():
        if key in _CRON_GOVERNANCE_RUNTIME_FIELDS:
            continue
        if key in _CRON_GOVERNANCE_SELF_REFERENTIAL_FIELDS:
            continue
        if key == "repeat" and isinstance(value, dict) and set(value) <= {"completed"}:
            continue
        return True
    return False


class CronJobGovernanceError(PermissionError):
    """Raised when a managed profile refuses a cron persistence candidate."""

    def __init__(self, message: str, *, decision: Dict[str, Any] | None = None):
        super().__init__(message)
        self.decision = copy.deepcopy(decision or {})

    def payload(self) -> Dict[str, Any]:
        pending = self.decision.get("pending_action")
        return {
            "schema_version": "cron-admin-pending-action/v1",
            "action": "blocked",
            "reason": str(self.decision.get("reason") or "review_required"),
            "state": str(self.decision.get("state") or "review_required"),
            "pending_action": copy.deepcopy(pending) if isinstance(pending, dict) else {},
        }


def _safe_runtime_admission_code(value: Any, fallback: str) -> str:
    """Keep persisted admission metadata machine-readable and non-sensitive."""
    code = str(value or "").strip().lower()
    if re.fullmatch(r"[a-z][a-z0-9_.:-]{0,95}", code):
        return code
    return fallback


def _runtime_admission_job_fingerprint(job: Dict[str, Any]) -> str:
    """Return a stable job link without persisting prompt, route, or credentials."""
    creation_receipt = job.get("creation_governance_receipt")
    join_keys = job.get("join_keys")
    schedule = job.get("schedule")
    material = {
        "job_id": str(job.get("id") or ""),
        "authorized_behavior_ref": str(job.get("authorized_behavior_ref") or ""),
        "process_charter_ref": str(job.get("process_charter_ref") or ""),
        "candidate_hash": (
            str(creation_receipt.get("candidate_hash") or "")
            if isinstance(creation_receipt, dict)
            else str(join_keys.get("candidate_hash") or "")
            if isinstance(join_keys, dict)
            else ""
        ),
        "risk_tier": str(job.get("risk_tier") or ""),
        "schedule_kind": str(schedule.get("kind") or "") if isinstance(schedule, dict) else "",
    }
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _runtime_admission_receipt(
    job: Dict[str, Any],
    *,
    reason_code: Any,
    state: Any,
    exception_class: str,
    retryable: bool,
) -> Dict[str, Any]:
    """Build the small durable record shared by scheduler status and delivery."""
    return {
        "schema_version": _CRON_RUNTIME_ADMISSION_RECEIPT_SCHEMA,
        "receipt_id": f"cron-runtime-admission:{uuid.uuid4().hex}",
        "stage": "pre_cron_job_run",
        "status": "blocked",
        "reason_code": _safe_runtime_admission_code(reason_code, "runtime_binding_required"),
        "state": _safe_runtime_admission_code(state, "review_required"),
        "exception_class": _safe_runtime_admission_code(
            exception_class, "runtime_admission_error"
        ),
        "retryable": bool(retryable),
        "job_fingerprint": _runtime_admission_job_fingerprint(job),
    }


def _validated_runtime_admission_receipt(receipt: Dict[str, Any]) -> Dict[str, Any]:
    """Reject forged or free-form data before it can enter durable job state."""
    expected_keys = {
        "schema_version", "receipt_id", "stage", "status", "reason_code", "state",
        "exception_class", "retryable", "job_fingerprint",
    }
    if not isinstance(receipt, dict) or set(receipt) != expected_keys:
        raise ValueError("invalid runtime admission receipt")
    if (
        receipt.get("schema_version") != _CRON_RUNTIME_ADMISSION_RECEIPT_SCHEMA
        or receipt.get("stage") != "pre_cron_job_run"
        or receipt.get("status") != "blocked"
        or not isinstance(receipt.get("retryable"), bool)
        or not re.fullmatch(r"cron-runtime-admission:[0-9a-f]{32}", str(receipt.get("receipt_id") or ""))
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", str(receipt.get("job_fingerprint") or ""))
    ):
        raise ValueError("invalid runtime admission receipt")
    for field, fallback in {
        "reason_code": "runtime_binding_required",
        "state": "review_required",
        "exception_class": "runtime_admission_error",
    }.items():
        value = str(receipt.get(field) or "")
        if _safe_runtime_admission_code(value, fallback) != value:
            raise ValueError("invalid runtime admission receipt")
    return copy.deepcopy(receipt)


def _validated_delivery_receipt(receipt: Dict[str, Any]) -> Dict[str, Any]:
    """Accept only the aggregate delivery fact safe for ``jobs.json``.

    Transport-level receipts may contain message identifiers or provider
    metadata. Cron persistence deliberately retains only cardinalities, so the
    scheduler and Kit health surfaces can prove terminal delivery without
    turning the job store into an outbound-message archive.
    """
    expected_keys = {
        "schema_version",
        "status",
        "required_count",
        "confirmed_count",
        "failed_count",
        "unconfirmed_count",
        "receipts_truncated",
    }
    if not isinstance(receipt, dict) or set(receipt) != expected_keys:
        raise ValueError("invalid cron delivery receipt")
    if receipt.get("schema_version") != _CRON_DELIVERY_RECEIPT_SCHEMA:
        raise ValueError("invalid cron delivery receipt")
    if receipt.get("status") not in {"not_attempted", "success", "partial", "failed"}:
        raise ValueError("invalid cron delivery receipt")
    if not isinstance(receipt.get("receipts_truncated"), bool):
        raise ValueError("invalid cron delivery receipt")
    counts = {
        field: receipt.get(field)
        for field in ("required_count", "confirmed_count", "failed_count", "unconfirmed_count")
    }
    if not all(isinstance(value, int) and not isinstance(value, bool) and value >= 0 for value in counts.values()):
        raise ValueError("invalid cron delivery receipt")
    if counts["confirmed_count"] + counts["failed_count"] + counts["unconfirmed_count"] != counts["required_count"]:
        raise ValueError("invalid cron delivery receipt")
    if counts["required_count"] == 0:
        expected_status = "not_attempted"
    elif counts["confirmed_count"] == counts["required_count"]:
        expected_status = "success"
    elif counts["confirmed_count"]:
        expected_status = "partial"
    else:
        expected_status = "failed"
    if receipt.get("status") != expected_status:
        raise ValueError("invalid cron delivery receipt")
    return copy.deepcopy(receipt)


def _safe_run_outcome_identity(value: Any) -> bool:
    """Accept opaque profile/Job ids without allowing control data."""
    if not isinstance(value, str) or value != value.strip():
        return False
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return bool(encoded) and len(encoded) <= 256 and not any(
        ord(char) < 32 or 127 <= ord(char) <= 159 for char in value
    )


def _cron_run_hash(material: Any) -> str:
    encoded = json.dumps(
        material,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _cron_run_artifact(path: Path) -> tuple[Optional[str], Optional[bytes]]:
    try:
        resolved = path.expanduser().resolve(strict=True)
        if not resolved.is_file() or resolved.stat().st_size > _CRON_RUN_ARTIFACT_MAX_BYTES:
            return None, None
        content = resolved.read_bytes()
        if len(content) > _CRON_RUN_ARTIFACT_MAX_BYTES:
            return None, None
        return "sha256:" + hashlib.sha256(content).hexdigest(), content
    except (OSError, RuntimeError):
        return None, None


def _cron_run_artifact_hash(path: Path) -> Optional[str]:
    return _cron_run_artifact(path)[0]


def _cron_interpreter_artifact(
    path: Path,
) -> tuple[Optional[str], Optional[bytes], Optional[Path]]:
    """Capture one bounded interpreter binary for exact snapshot execution."""
    try:
        resolved = path.expanduser().resolve(strict=True)
        if not resolved.is_file():
            return None, None, None
        digest = hashlib.sha256()
        total = 0
        chunks: list[bytes] = []
        with resolved.open("rb") as handle:
            while chunk := handle.read(_CRON_ARTIFACT_HASH_CHUNK_BYTES):
                total += len(chunk)
                if total > _CRON_INTERPRETER_ARTIFACT_MAX_BYTES:
                    return None, None, None
                digest.update(chunk)
                chunks.append(chunk)
        return "sha256:" + digest.hexdigest(), b"".join(chunks), resolved
    except (OSError, RuntimeError):
        return None, None, None


def _cron_interpreter_artifact_hash(path: Path) -> Optional[str]:
    return _cron_interpreter_artifact(path)[0]


def _cron_profile_home() -> Path:
    return _current_cron_store().cron_dir.parent


def _cron_script_interpreter(path: Path) -> Optional[Path]:
    if path.suffix.lower() in {".sh", ".bash"}:
        for raw_candidate in _CRON_TRUSTED_BASH_PATHS:
            candidate = Path(raw_candidate)
            if candidate.is_file() and os.access(candidate, os.X_OK):
                return candidate
        return None
    if sys.platform == "win32":
        return None
    return Path(sys.executable)


def _cron_script_artifact(
    job: Dict[str, Any],
    profile_home: Path,
) -> tuple[Optional[str], Optional[str], Optional[bytes]]:
    script = job.get("script")
    if not isinstance(script, str) or not script.strip():
        return (
            _cron_run_hash({"state": "absent"}),
            _cron_run_hash({"state": "not_applicable"}),
            None,
        )
    scripts_root = (profile_home / "scripts").resolve(strict=False)
    raw = Path(script).expanduser()
    path = raw.resolve(strict=False) if raw.is_absolute() else (scripts_root / raw).resolve(strict=False)
    try:
        path.relative_to(scripts_root)
    except ValueError:
        return None, None, None
    artifact_hash, snapshot = _cron_run_artifact(path)
    interpreter = _cron_script_interpreter(path)
    interpreter_hash = (
        _cron_interpreter_artifact_hash(interpreter)
        if interpreter is not None
        else None
    )
    if artifact_hash is None or snapshot is None or interpreter_hash is None:
        return None, None, None
    return _cron_run_hash(
        {
            "state": "loaded",
            "execution_mode": "stdin-snapshot/v1",
            "interpreter": "bash" if path.suffix.lower() in {".sh", ".bash"} else "python",
            "content_hash": artifact_hash,
        }
    ), interpreter_hash, snapshot


def _cron_script_artifact_hash(job: Dict[str, Any], profile_home: Path) -> Optional[str]:
    return _cron_script_artifact(job, profile_home)[0]


def _cron_support_artifact(
    job: Dict[str, Any], profile_home: Path
) -> tuple[Optional[str], Optional[list[dict[str, Any]]]]:
    """Capture the bounded local tree that a script can import or execute."""
    script = job.get("script")
    if not isinstance(script, str) or not script.strip():
        return _cron_run_hash({"state": "not_applicable"}), []
    scripts_root = (profile_home / "scripts").resolve(strict=False)
    raw = Path(script).expanduser()
    script_path = (
        raw.resolve(strict=False)
        if raw.is_absolute()
        else (scripts_root / raw).resolve(strict=False)
    )
    try:
        script_path.relative_to(scripts_root)
    except ValueError:
        return None, None

    roots: list[tuple[str, Path]] = [("script_root", script_path.parent)]
    workdir = str(job.get("workdir") or "").strip()
    if workdir:
        workdir_input = Path(workdir).expanduser()
        workdir_path = workdir_input.resolve(strict=False)
        if workdir_input.is_symlink() or workdir_path != script_path.parent:
            return None, None

    records: list[dict[str, Any]] = []
    snapshots: list[dict[str, Any]] = []
    total_bytes = 0
    try:
        for root_kind, raw_root in roots:
            root = raw_root.resolve(strict=True)
            for candidate in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
                relative = candidate.relative_to(root)
                if set(relative.parts) & _CRON_SUPPORT_IGNORED_DIRS:
                    continue
                if candidate.is_dir():
                    continue
                if candidate.is_symlink() or not candidate.is_file():
                    return None, None
                content_hash, content = _cron_run_artifact(candidate)
                if content_hash is None or content is None:
                    return None, None
                mode = candidate.stat().st_mode & 0o777
                total_bytes += len(content)
                if (
                    len(records) >= _CRON_SUPPORT_ARTIFACT_MAX_FILES
                    or total_bytes > _CRON_SUPPORT_ARTIFACT_MAX_BYTES
                ):
                    return None, None
                records.append(
                    {
                        "root": root_kind,
                        "path": relative.as_posix(),
                        "content_hash": content_hash,
                        "mode": mode,
                    }
                )
                snapshots.append(
                    {
                        "root": root_kind,
                        "path": relative.as_posix(),
                        "content": content,
                        "mode": mode,
                    }
                )
    except (OSError, RuntimeError, ValueError):
        return None, None
    return (
        _cron_run_hash(
            {"schema_version": _CRON_SUPPORT_ARTIFACT_SCHEMA, "files": records}
        ),
        snapshots,
    )


def _cron_support_artifact_hash(
    job: Dict[str, Any], profile_home: Path
) -> Optional[str]:
    return _cron_support_artifact(job, profile_home)[0]


def _cron_run_script_snapshot(
    job: Dict[str, Any],
    claim: Dict[str, Any],
    *,
    profile_home: Optional[Path] = None,
) -> tuple[bool, Optional[dict[str, Any]]]:
    """Capture the bounded Profile-controlled files after validating the claim."""
    home = (profile_home or _cron_profile_home()).expanduser().resolve(strict=False)
    artifact_hash, interpreter_hash, snapshot = _cron_script_artifact(job, home)
    support_hash, support_snapshot = _cron_support_artifact(job, home)
    if snapshot is None:
        return (
            artifact_hash == claim.get("script_artifact_hash")
            and interpreter_hash == claim.get("interpreter_artifact_hash")
            and support_hash == claim.get("support_artifact_hash")
        ), None
    scripts_root = (home / "scripts").resolve(strict=False)
    raw = Path(str(job.get("script") or "")).expanduser()
    script_path = (
        raw.resolve(strict=False)
        if raw.is_absolute()
        else (scripts_root / raw).resolve(strict=False)
    )
    interpreter = _cron_script_interpreter(script_path)
    if interpreter is None:
        return False, None
    captured_interpreter_hash, interpreter_snapshot, interpreter_path = (
        _cron_interpreter_artifact(interpreter)
    )
    matches = (
        artifact_hash == claim.get("script_artifact_hash")
        and interpreter_hash == claim.get("interpreter_artifact_hash")
        and captured_interpreter_hash == interpreter_hash
        and support_hash == claim.get("support_artifact_hash")
        and interpreter_snapshot is not None
        and interpreter_path is not None
        and support_snapshot is not None
    )
    if not matches:
        return False, None
    return True, {
        "schema_version": _CRON_SCRIPT_EXECUTION_SNAPSHOT_SCHEMA,
        "script_name": script_path.name,
        "script_suffix": script_path.suffix.lower(),
        "script_bytes": snapshot,
        "interpreter_path": str(interpreter_path),
        "interpreter_bytes": interpreter_snapshot,
        "support_files": support_snapshot,
    }


def _cron_run_outcome_claim(
    job: Dict[str, Any],
    *,
    run_id: Optional[str] = None,
    profile_home: Optional[Path] = None,
    claim_started_at_epoch: Optional[int] = None,
    claim_heartbeat_at_epoch: Optional[int] = None,
    claim_expires_at_epoch: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """Freeze the actual executable and checkpoint contract before a run."""
    creation = job.get("creation_governance_receipt")
    if not isinstance(creation, dict):
        return None
    profile_id = creation.get("profile_id")
    job_id = job.get("id")
    revision = creation.get("receipt_id")
    if (
        creation.get("schema_version") != "cron-creation-governance/v1"
        or creation.get("cron_job_id") != job_id
        or not _safe_run_outcome_identity(profile_id)
        or not _safe_run_outcome_identity(job_id)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", str(revision or ""))
    ):
        return None
    home = (profile_home or _cron_profile_home()).expanduser().resolve(strict=False)
    script_hash, interpreter_hash, _snapshot = _cron_script_artifact(job, home)
    support_hash = _cron_support_artifact_hash(job, home)
    if script_hash is None or interpreter_hash is None or support_hash is None or any(
        job.get(field) not in (None, "")
        for field in ("verification_command", "verification_command_mode")
    ):
        # An arbitrary subprocess cannot be proven read-only without an OS
        # sandbox. A future fixed verifier must use a new versioned contract.
        return None
    implementation_hash = _cron_run_hash(
        {
            "schema_version": _CRON_RUN_IMPLEMENTATION_SCHEMA,
            "mode": "no_agent" if job.get("no_agent") is True else "agent",
            "execution": {
                field: copy.deepcopy(job.get(field))
                for field in _CRON_RUN_IMPLEMENTATION_FIELDS
            },
            "script_artifact_hash": script_hash,
            "interpreter_artifact_hash": interpreter_hash,
            "support_artifact_hash": support_hash,
        }
    )
    checkpoint_contract = {
        field: copy.deepcopy(job.get(field))
        for field in _CRON_CHECKPOINT_CONTRACT_FIELDS
        if field in job
    }
    checkpoint_policy_hash = _cron_run_hash(
        {
            "schema_version": "cron-checkpoint-policy/v1",
            "job_revision": revision,
            "checkpoint_contract": checkpoint_contract or {"mode": "not_declared"},
        }
    )
    if (
        claim_started_at_epoch is None
        and claim_heartbeat_at_epoch is None
        and claim_expires_at_epoch is None
    ):
        claim_started_at_epoch = int(time.time())
        claim_heartbeat_at_epoch = claim_started_at_epoch
        claim_expires_at_epoch = claim_heartbeat_at_epoch + min(
            int(_oneshot_run_claim_ttl_seconds()),
            _CRON_RUN_OUTCOME_CLAIM_MAX_TTL_SECONDS,
        )
    claim: Dict[str, Any] = {
        "schema_version": _CRON_RUN_OUTCOME_CLAIM_SCHEMA,
        "profile_id": profile_id,
        "job_id": job_id,
        "job_revision": revision,
        "run_id": run_id or f"cron-run:{uuid.uuid4().hex}",
        "claim_started_at_epoch": claim_started_at_epoch,
        "claim_heartbeat_at_epoch": claim_heartbeat_at_epoch,
        "claim_expires_at_epoch": claim_expires_at_epoch,
        "script_artifact_hash": script_hash,
        "interpreter_artifact_hash": interpreter_hash,
        "support_artifact_hash": support_hash,
        "implementation_hash": implementation_hash,
        "checkpoint_policy_hash": checkpoint_policy_hash,
    }
    return _validated_run_outcome_claim(claim)


def _validated_run_outcome_claim(claim: Dict[str, Any]) -> Dict[str, Any]:
    required = {
        "schema_version",
        "profile_id",
        "job_id",
        "job_revision",
        "run_id",
        "claim_started_at_epoch",
        "claim_heartbeat_at_epoch",
        "claim_expires_at_epoch",
        "script_artifact_hash",
        "interpreter_artifact_hash",
        "support_artifact_hash",
        "implementation_hash",
        "checkpoint_policy_hash",
    }
    if not isinstance(claim, dict) or set(claim) != required:
        raise ValueError("invalid cron run outcome claim")
    started_at = claim.get("claim_started_at_epoch")
    heartbeat_at = claim.get("claim_heartbeat_at_epoch")
    expires_at = claim.get("claim_expires_at_epoch")
    if (
        claim.get("schema_version") != _CRON_RUN_OUTCOME_CLAIM_SCHEMA
        or not _safe_run_outcome_identity(claim.get("profile_id"))
        or not _safe_run_outcome_identity(claim.get("job_id"))
        or not re.fullmatch(r"cron-run:[0-9a-f]{32}", str(claim.get("run_id") or ""))
        or not all(
            re.fullmatch(r"sha256:[0-9a-f]{64}", str(claim.get(field) or ""))
            for field in (
                "job_revision",
                "script_artifact_hash",
                "interpreter_artifact_hash",
                "support_artifact_hash",
                "implementation_hash",
                "checkpoint_policy_hash",
            )
        )
        or isinstance(started_at, bool)
        or not isinstance(started_at, int)
        or isinstance(heartbeat_at, bool)
        or not isinstance(heartbeat_at, int)
        or isinstance(expires_at, bool)
        or not isinstance(expires_at, int)
        or started_at <= 0
        or heartbeat_at < started_at
        or not (
            ONESHOT_RUN_CLAIM_TTL_SECONDS
            <= expires_at - heartbeat_at
            <= _CRON_RUN_OUTCOME_CLAIM_MAX_TTL_SECONDS
        )
    ):
        raise ValueError("invalid cron run outcome claim")
    return copy.deepcopy(claim)


def _run_outcome_claim_is_active(
    claim: Dict[str, Any],
    *,
    now_epoch: Optional[int] = None,
) -> bool:
    """Return whether a validated claim still owns its bounded run window."""
    validated = _validated_run_outcome_claim(claim)
    now = int(time.time()) if now_epoch is None else now_epoch
    return (
        validated["claim_heartbeat_at_epoch"]
        <= now
        < validated["claim_expires_at_epoch"]
    )


def heartbeat_job_run_outcome(
    job_id: str,
    claim: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Refresh the exact active claim without changing its run identity."""
    claim = _validated_run_outcome_claim(claim)
    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs()
        for job in jobs:
            if job.get("id") != job_id:
                continue
            if job.get(_CRON_RUN_OUTCOME_CLAIM_FIELD) != claim:
                return None
            if not _run_outcome_claim_is_active(claim):
                return None
            heartbeat_at = max(
                int(time.time()),
                claim["claim_heartbeat_at_epoch"],
            )
            refreshed = {
                **claim,
                "claim_heartbeat_at_epoch": heartbeat_at,
                "claim_expires_at_epoch": heartbeat_at
                + min(
                    int(_oneshot_run_claim_ttl_seconds()),
                    _CRON_RUN_OUTCOME_CLAIM_MAX_TTL_SECONDS,
                ),
            }
            refreshed = _validated_run_outcome_claim(refreshed)
            job[_CRON_RUN_OUTCOME_CLAIM_FIELD] = copy.deepcopy(refreshed)
            _save_jobs_unlocked(jobs)
            return refreshed
    return None


def _validated_run_outcome_receipt(receipt: Dict[str, Any]) -> Dict[str, Any]:
    """Validate the redacted, revision-bound terminal execution fact."""
    required_keys = {
        "schema_version",
        "profile_id",
        "job_id",
        "job_revision",
        "run_id",
        "terminal_state",
        "implementation_hash",
        "checkpoint_invariant_hash",
        "delivery_receipt_hash",
    }
    if not isinstance(receipt, dict) or set(receipt) != required_keys:
        raise ValueError("invalid cron run outcome receipt")
    if (
        receipt.get("schema_version") != _CRON_RUN_OUTCOME_RECEIPT_SCHEMA
        or receipt.get("terminal_state") not in {"success", "failed"}
        or not _safe_run_outcome_identity(receipt.get("profile_id"))
        or not _safe_run_outcome_identity(receipt.get("job_id"))
        or not re.fullmatch(r"cron-run:[0-9a-f]{32}", str(receipt.get("run_id") or ""))
    ):
        raise ValueError("invalid cron run outcome receipt")
    for field in (
        "job_revision",
        "implementation_hash",
        "checkpoint_invariant_hash",
        "delivery_receipt_hash",
    ):
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", str(receipt.get(field) or "")):
            raise ValueError("invalid cron run outcome receipt")
    return copy.deepcopy(receipt)


def _cron_delivery_receipt_hash(
    run_id: str,
    delivery_receipt: Optional[Dict[str, Any]],
) -> str:
    normalized = (
        _validated_delivery_receipt(delivery_receipt)
        if delivery_receipt is not None
        else {"state": "not_recorded"}
    )
    return _cron_run_hash(
        {
            "schema_version": _CRON_DELIVERY_BINDING_SCHEMA,
            "run_id": run_id,
            "delivery_receipt": normalized,
        }
    )


def _cron_checkpoint_invariant_hash(
    job: Dict[str, Any],
    claim: Dict[str, Any],
    *,
    success: bool,
) -> Optional[str]:
    checkpoint_contract = {
        field: copy.deepcopy(job.get(field))
        for field in _CRON_CHECKPOINT_CONTRACT_FIELDS
        if field in job
    }
    policy_hash = _cron_run_hash(
        {
            "schema_version": "cron-checkpoint-policy/v1",
            "job_revision": claim["job_revision"],
            "checkpoint_contract": checkpoint_contract or {"mode": "not_declared"},
        }
    )
    if policy_hash != claim.get("checkpoint_policy_hash"):
        return None
    if checkpoint_contract and success:
        # #1424 owns the actual checkpoint observation. A declaration alone
        # cannot authorize a successful terminal outcome.
        return None
    terminal_state = (
        "unobserved_due_to_failure"
        if checkpoint_contract
        else "not_applicable"
    )
    return _cron_run_hash(
        {
            "schema_version": _CRON_CHECKPOINT_INVARIANT_SCHEMA,
            "run_id": claim["run_id"],
            "policy_hash": policy_hash,
            "implementation_hash": claim["implementation_hash"],
            "terminal_state": terminal_state,
            "pending_stage_count": None if checkpoint_contract else 0,
        }
    )


def _cron_run_outcome_receipt(
    job: Dict[str, Any],
    *,
    success: bool,
    run_outcome_claim: Optional[Dict[str, Any]] = None,
    delivery_receipt: Optional[Dict[str, Any]] = None,
    profile_home: Optional[Path] = None,
) -> Optional[Dict[str, Any]]:
    """Build a terminal fact only when the pre-run implementation stayed exact."""
    if not isinstance(success, bool):
        raise ValueError("cron run outcome success must be boolean")
    if run_outcome_claim is None:
        return None
    claim = _validated_run_outcome_claim(run_outcome_claim)
    current = _cron_run_outcome_claim(
        job,
        run_id=claim["run_id"],
        profile_home=profile_home,
        claim_started_at_epoch=claim["claim_started_at_epoch"],
        claim_heartbeat_at_epoch=claim["claim_heartbeat_at_epoch"],
        claim_expires_at_epoch=claim["claim_expires_at_epoch"],
    )
    if current != claim:
        return None
    checkpoint_hash = _cron_checkpoint_invariant_hash(
        job,
        claim,
        success=success,
    )
    if checkpoint_hash is None:
        return None
    receipt: Dict[str, Any] = {
        "schema_version": _CRON_RUN_OUTCOME_RECEIPT_SCHEMA,
        "profile_id": claim["profile_id"],
        "job_id": claim["job_id"],
        "job_revision": claim["job_revision"],
        "run_id": claim["run_id"],
        "terminal_state": "success" if success else "failed",
        "implementation_hash": claim["implementation_hash"],
        "checkpoint_invariant_hash": checkpoint_hash,
        "delivery_receipt_hash": _cron_delivery_receipt_hash(
            claim["run_id"],
            delivery_receipt,
        ),
    }
    return _validated_run_outcome_receipt(receipt)


def begin_job_run_outcome(job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Clear stale success and atomically claim this signed Job revision."""
    snapshot_creation = job.get("creation_governance_receipt")
    if not isinstance(snapshot_creation, dict):
        return None
    snapshot_revision = snapshot_creation.get("receipt_id")
    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs()
        for stored in jobs:
            if stored.get("id") != job.get("id"):
                continue
            creation = stored.get("creation_governance_receipt")
            if not isinstance(creation, dict) or creation.get("receipt_id") != snapshot_revision:
                return None
            existing_claim = stored.get(_CRON_RUN_OUTCOME_CLAIM_FIELD)
            if existing_claim is not None:
                try:
                    if _run_outcome_claim_is_active(existing_claim):
                        return None
                except ValueError:
                    return None
            claim = _cron_run_outcome_claim(stored)
            if claim is None:
                stored[_CRON_RUN_OUTCOME_CLAIM_FIELD] = None
                stored[_CRON_RUN_OUTCOME_RECEIPT_FIELD] = None
                _save_jobs_unlocked(jobs)
                return None
            stored[_CRON_RUN_OUTCOME_CLAIM_FIELD] = copy.deepcopy(claim)
            stored[_CRON_RUN_OUTCOME_RECEIPT_FIELD] = None
            _save_jobs_unlocked(jobs)
            return claim
    return None


def record_job_run_preflight_denial(
    job_id: str,
    *,
    job_revision: str,
    reason_code: str,
    run_claim: Any = _CRON_CLAIM_UNSET,
    fire_claim: Any = _CRON_CLAIM_UNSET,
) -> bool:
    """Persist a zero-side-effect denial without consuming a finite dispatch."""
    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs()
        for stored in jobs:
            if stored.get("id") != job_id:
                continue
            creation = stored.get("creation_governance_receipt")
            if (
                not isinstance(creation, dict)
                or creation.get("receipt_id") != job_revision
                or stored.get(_CRON_RUN_OUTCOME_CLAIM_FIELD) is not None
                or (
                    run_claim is not _CRON_CLAIM_UNSET
                    and stored.get("run_claim") != run_claim
                )
                or (
                    fire_claim is not _CRON_CLAIM_UNSET
                    and stored.get("fire_claim") != fire_claim
                )
            ):
                return False
            if run_claim is not _CRON_CLAIM_UNSET:
                stored["run_claim"] = None
            if fire_claim is not _CRON_CLAIM_UNSET:
                stored["fire_claim"] = None
            stored[_CRON_RUN_OUTCOME_RECEIPT_FIELD] = None
            stored[_CRON_RUNTIME_ADMISSION_RECEIPT_FIELD] = _runtime_admission_receipt(
                stored,
                reason_code=reason_code,
                state="review_required",
                exception_class="run_outcome_preflight_denied",
                retryable=False,
            )
            _save_jobs_unlocked(jobs)
            return True
    return False


def abandon_job_run_outcome(
    job_id: str,
    claim: Dict[str, Any],
    *,
    reason_code: Optional[str] = None,
    run_claim: Any = _CRON_CLAIM_UNSET,
    fire_claim: Any = _CRON_CLAIM_UNSET,
) -> bool:
    """Clear an unexecuted claim without advancing schedule or run status."""
    claim = _validated_run_outcome_claim(claim)
    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs()
        for job in jobs:
            if job.get("id") != job_id:
                continue
            if (
                job.get(_CRON_RUN_OUTCOME_CLAIM_FIELD) != claim
                or (
                    run_claim is not _CRON_CLAIM_UNSET
                    and job.get("run_claim") != run_claim
                )
                or (
                    fire_claim is not _CRON_CLAIM_UNSET
                    and job.get("fire_claim") != fire_claim
                )
            ):
                return False
            job[_CRON_RUN_OUTCOME_CLAIM_FIELD] = None
            job[_CRON_RUN_OUTCOME_RECEIPT_FIELD] = None
            if run_claim is not _CRON_CLAIM_UNSET:
                job["run_claim"] = None
            if fire_claim is not _CRON_CLAIM_UNSET:
                job["fire_claim"] = None
            if reason_code is not None:
                job[_CRON_RUNTIME_ADMISSION_RECEIPT_FIELD] = _runtime_admission_receipt(
                    job,
                    reason_code=reason_code,
                    state="review_required",
                    exception_class="run_outcome_preflight_denied",
                    retryable=False,
                )
            _save_jobs_unlocked(jobs)
            return True
    return False


class CronRuntimeAdmissionError(PermissionError):
    """Fail-closed runtime admission result with a safe durable receipt."""

    def __init__(self, receipt: Dict[str, Any]):
        self.receipt = _validated_runtime_admission_receipt(receipt)
        super().__init__(f"Cron job was not run: {self.receipt['reason_code']}.")


def _cron_creation_governance_expected() -> bool:
    if str(os.environ.get(_CRON_GOVERNANCE_ENV, "")).strip().lower() in {"1", "true", "yes", "on"}:
        return True
    plugin_dir = get_hermes_home() / "plugins" / _CRON_GOVERNANCE_PLUGIN
    managed_profile = (get_hermes_home() / ".hermes-agent-kit").exists()
    if not (plugin_dir / "plugin.yaml").is_file() or not (plugin_dir / "cron_creation_governance.py").is_file():
        # A managed Hermes Agent Kit profile must fail closed if its governance
        # plugin is missing. Unmanaged upstream profiles retain compatibility.
        return managed_profile
    try:
        import yaml
        config_path = get_hermes_home() / "config.yaml"
        config = yaml.safe_load(config_path.read_text(encoding="utf-8")) if config_path.is_file() else {}
        plugins = config.get("plugins") if isinstance(config, dict) else {}
        enabled = plugins.get("enabled") if isinstance(plugins, dict) else []
        disabled = plugins.get("disabled") if isinstance(plugins, dict) else []
        return _CRON_GOVERNANCE_PLUGIN in (enabled or []) and _CRON_GOVERNANCE_PLUGIN not in (disabled or [])
    except Exception:
        return managed_profile


def _apply_cron_runtime_governance(job: Dict[str, Any]) -> None:
    """Admit a scheduled job before its script or Agent side effect."""
    if not _cron_creation_governance_expected():
        return
    try:
        from hermes_cli.plugins import discover_plugins, invoke_hook
        discover_plugins()
        results = invoke_hook("pre_cron_job_run", job=copy.deepcopy(job))
    except Exception as exc:
        raise CronRuntimeAdmissionError(
            _runtime_admission_receipt(
                job,
                reason_code="runtime_governance_unavailable",
                state="review_required",
                exception_class=exc.__class__.__name__,
                retryable=True,
            )
        ) from exc
    decisions = [item for item in results if isinstance(item, dict) and item.get("action") in {"allow", "block"}]
    blocked = next((item for item in decisions if item.get("action") == "block"), None)
    if blocked is not None:
        raise CronRuntimeAdmissionError(
            _runtime_admission_receipt(
                job,
                reason_code=blocked.get("reason"),
                state=blocked.get("state"),
                exception_class="runtime_admission_blocked",
                retryable=False,
            )
        )
    allowed = [item for item in decisions if item.get("action") == "allow"]
    if len(allowed) != 1:
        raise CronRuntimeAdmissionError(
            _runtime_admission_receipt(
                job,
                reason_code="runtime_admission_ambiguous",
                state="review_required",
                exception_class="runtime_admission_ambiguous",
                retryable=False,
            )
        )


def _apply_cron_creation_governance(
    operation: str,
    candidate: Dict[str, Any],
    existing_jobs: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], str]:
    if not _cron_creation_governance_expected():
        if _CRON_RESUME_RECEIPT_FIELD in candidate:
            raise CronJobGovernanceError(
                "Cron job persistence needs administrator review (resume governance unavailable)."
            )
        return candidate, "allow_write"
    try:
        from hermes_cli.plugins import discover_plugins, invoke_hook
        discover_plugins()
        results = invoke_hook(
            "pre_cron_job_persist",
            operation=operation,
            candidate=copy.deepcopy(candidate),
            existing_jobs=copy.deepcopy(existing_jobs),
            core_jobs_lock_capability=_issue_jobs_lock_capability(),
        )
    except Exception as exc:
        raise CronJobGovernanceError("Cron job persistence needs administrator review (governance unavailable).") from exc

    decisions = [item for item in results if isinstance(item, dict) and item.get("action") in {"allow", "block"}]
    blocked = next((item for item in decisions if item.get("action") == "block"), None)
    if blocked is not None:
        reason = str(blocked.get("reason") or "review_required")
        state = str(blocked.get("state") or "review_required")
        raise CronJobGovernanceError(f"Cron job was not saved: {reason} ({state}).", decision=blocked)
    allowed = [item for item in decisions if item.get("action") == "allow"]
    if len(allowed) != 1:
        raise CronJobGovernanceError("Cron job persistence needs administrator review (missing or ambiguous authorization).")
    decision = allowed[0]
    disposition = str(decision.get("persist_disposition") or "allow_write")
    if disposition == "already_persisted":
        resume = candidate.get(_CRON_RESUME_RECEIPT_FIELD)
        resume_id = str(resume.get("receipt_id") or "").strip() if isinstance(resume, dict) else ""
        matching = [
            job for job in existing_jobs
            if str(job.get("id") or "") == str(candidate.get("id") or "")
            and isinstance(job.get("creation_governance_receipt"), dict)
            and str(job["creation_governance_receipt"].get("resume_receipt_id") or "") == resume_id
        ]
        if not resume_id or len(matching) != 1:
            raise CronJobGovernanceError(
                "Cron job persistence needs administrator review (invalid already-persisted result)."
            )
        return copy.deepcopy(matching[0]), "already_persisted"
    if disposition != "allow_write":
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review (invalid persistence disposition)."
        )

    patch = decision.get("job_patch")
    if not isinstance(patch, dict) or set(patch) - _CRON_GOVERNANCE_PATCH_FIELDS:
        raise CronJobGovernanceError("Cron job persistence needs administrator review (invalid governance result).")
    resume = candidate.get(_CRON_RESUME_RECEIPT_FIELD)
    resume_pause = {
        "enabled": False,
        "state": "paused",
        "paused_reason": "admin_authorized_pending_explicit_enable",
    }
    if resume is not None:
        if {key: patch.get(key) for key in resume_pause} != resume_pause:
            raise CronJobGovernanceError(
                "Cron job persistence needs administrator review (invalid resume pause result)."
            )
    elif set(patch).intersection(resume_pause):
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review (unexpected governance state patch)."
        )
    receipt = patch.get("creation_governance_receipt")
    if not isinstance(receipt, dict) or not str(receipt.get("receipt_id") or "").strip():
        raise CronJobGovernanceError("Cron job persistence needs administrator review (missing governance receipt).")
    if resume is not None:
        resume_id = str(resume.get("receipt_id") or "").strip() if isinstance(resume, dict) else ""
        if not resume_id or str(receipt.get("resume_receipt_id") or "").strip() != resume_id:
            raise CronJobGovernanceError(
                "Cron job persistence needs administrator review (resume receipt was not consumed)."
            )
    governed = {**candidate, **copy.deepcopy(patch)}
    governed.pop(_CRON_RESUME_RECEIPT_FIELD, None)
    return governed, "allow_write"


def _resume_candidate(package: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(package, dict):
        raise CronJobGovernanceError("Cron job persistence needs administrator review (invalid resume package).")
    job = package.get("job")
    receipt = package.get("receipt")
    if not isinstance(job, dict) or not isinstance(receipt, dict):
        raise CronJobGovernanceError("Cron job persistence needs administrator review (invalid resume package).")
    if not str(job.get("id") or "").strip() or not str(receipt.get("receipt_id") or "").strip():
        raise CronJobGovernanceError("Cron job persistence needs administrator review (incomplete resume package).")
    return {**copy.deepcopy(job), _CRON_RESUME_RECEIPT_FIELD: copy.deepcopy(receipt)}


def _job_output_dir(job_id: str) -> Path:
    """Resolve a job's output directory, rejecting any path-escape attempt.

    Job IDs are filesystem path components under ``OUTPUT_DIR``. A legacy or
    crafted ID containing ``..``, absolute paths, or nested separators would
    allow output writes/deletes to escape the cron output sandbox. Reject
    anything that isn't a single safe path component.
    """
    text = str(job_id or "").strip()
    if not text or text in {".", ".."} or "/" in text or "\\" in text:
        raise ValueError(f"Invalid cron job id for output path: {job_id!r}")
    if Path(text).is_absolute() or Path(text).drive:
        raise ValueError(f"Invalid cron job id for output path: {job_id!r}")
    return _current_cron_store().output_dir / text


def _remove_pinned_job_output(job_id: str) -> None:
    """Remove one output tree through the active Core cron identity only."""
    # Keep the existing safe-component guard, but do not use its lexical result
    # for any filesystem operation after Core's lock capability is active.
    output_name = _job_output_dir(job_id).name
    cron_fd = -1
    output_fd = -1
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        cron_fd = _duplicate_active_jobs_lock_cron_fd()
        try:
            output_entry = os.stat("output", dir_fd=cron_fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        if not stat.S_ISDIR(output_entry.st_mode):
            raise OSError("Cron output path is not a directory")
        output_fd = os.open("output", directory_flags, dir_fd=cron_fd)
        output_open = os.fstat(output_fd)
        if (
            not stat.S_ISDIR(output_open.st_mode)
            or output_open.st_dev != output_entry.st_dev
            or output_open.st_ino != output_entry.st_ino
        ):
            raise OSError("Cron output directory changed during cleanup")
        try:
            job_entry = os.stat(output_name, dir_fd=output_fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        if not stat.S_ISDIR(job_entry.st_mode):
            raise OSError("Cron job output path is not a directory")
        if not getattr(shutil.rmtree, "avoids_symlink_attacks", False):
            raise OSError("Secure recursive Cron output cleanup is unavailable")
        shutil.rmtree(output_name, dir_fd=output_fd)
        os.fsync(output_fd)
    finally:
        if output_fd >= 0:
            os.close(output_fd)
        if cron_fd >= 0:
            os.close(cron_fd)


def _normalize_skill_list(skill: Optional[str] = None, skills: Optional[Any] = None) -> List[str]:
    """Normalize legacy/single-skill and multi-skill inputs into a unique ordered list."""
    if skills is None:
        raw_items = [skill] if skill else []
    elif isinstance(skills, str):
        raw_items = [skills]
    else:
        raw_items = list(skills)

    normalized: List[str] = []
    for item in raw_items:
        text = str(item or "").strip()
        if text and text not in normalized:
            normalized.append(text)
    return normalized


def _apply_skill_fields(job: Dict[str, Any]) -> Dict[str, Any]:
    """Return a job dict with canonical `skills` and legacy `skill` fields aligned."""
    normalized = dict(job)
    skills = _normalize_skill_list(normalized.get("skill"), normalized.get("skills"))
    normalized["skills"] = skills
    normalized["skill"] = skills[0] if skills else None
    return normalized


def _coerce_job_text(value: Any, fallback: str = "") -> str:
    """Coerce legacy/hand-edited nullable cron fields to strings for readers."""
    if value is None:
        return fallback
    return str(value)


def _schedule_display_for_job(job: Dict[str, Any]) -> str:
    display = _coerce_job_text(job.get("schedule_display")).strip()
    if display:
        return display

    schedule = job.get("schedule")
    if isinstance(schedule, dict):
        for key in ("display", "value", "expr", "run_at"):
            text = _coerce_job_text(schedule.get(key)).strip()
            if text:
                return text
    elif schedule is not None:
        return str(schedule)

    return "?"


def _normalize_job_record(job: Dict[str, Any]) -> Dict[str, Any]:
    """Return a read-safe cron job shape for UI/API/tool/scheduler consumers.

    Older or hand-edited jobs can have nullable fields like ``prompt``,
    ``name``, or ``schedule_display``.  Keep storage untouched on read, but
    ensure consumers never crash while formatting or running those records.
    """
    normalized = _apply_skill_fields(job)
    job_id = _coerce_job_text(normalized.get("id"), "unknown")
    prompt = _coerce_job_text(normalized.get("prompt"))
    normalized["id"] = job_id
    normalized["prompt"] = prompt

    name = _coerce_job_text(normalized.get("name")).strip()
    if not name:
        script = _coerce_job_text(normalized.get("script")).strip()
        label_source = (
            prompt
            or (normalized["skills"][0] if normalized.get("skills") else "")
            or script
            or job_id
            or "cron job"
        )
        name = label_source[:50].strip() or "cron job"
    normalized["name"] = name
    normalized["schedule_display"] = _schedule_display_for_job(normalized)

    state = _coerce_job_text(normalized.get("state")).strip()
    if not state:
        state = "scheduled" if normalized.get("enabled", True) else "paused"
    normalized["state"] = state

    return normalized


def _secure_dir(path: Path):
    """Set directory to owner-only access (0700). No-op on Windows."""
    try:
        os.chmod(path, 0o700)
    except (OSError, NotImplementedError):
        pass  # Windows or other platforms where chmod is not supported


def _secure_file(path: Path):
    """Set file to owner-only read/write (0600). No-op on Windows."""
    try:
        if path.exists():
            os.chmod(path, 0o600)
    except (OSError, NotImplementedError):
        pass


def _open_cron_directory_for_provisioning() -> int:
    """Explicitly provision cron only through a checked parent directory chain."""
    cron_dir = _current_cron_store().cron_dir
    parent_fd = _open_existing_directory_chain(cron_dir.parent)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        try:
            entry = os.stat(cron_dir.name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            try:
                os.mkdir(cron_dir.name, 0o700, dir_fd=parent_fd)
                os.fsync(parent_fd)
            except FileExistsError:
                pass
            entry = os.stat(cron_dir.name, dir_fd=parent_fd, follow_symlinks=False)
        if not stat.S_ISDIR(entry.st_mode):
            raise OSError("Cron directory is not an existing regular directory")
        descriptor = os.open(cron_dir.name, flags, dir_fd=parent_fd)
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or opened.st_dev != entry.st_dev
            or opened.st_ino != entry.st_ino
        ):
            os.close(descriptor)
            raise OSError("Cron directory changed while provisioning")
        return descriptor
    finally:
        os.close(parent_fd)


def _provision_jobs_lock(cron_fd: int) -> None:
    """Create Core's lock during explicit store initialization, never at acquire time."""
    name = ".jobs.lock"
    try:
        entry = os.stat(name, dir_fd=cron_fd, follow_symlinks=False)
    except FileNotFoundError:
        descriptor = os.open(
            name,
            os.O_RDWR
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
            0o600,
            dir_fd=cron_fd,
        )
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode):
                raise OSError("Cron jobs lock is not a regular file")
            os.fchmod(descriptor, 0o600)
            os.fsync(descriptor)
            os.fsync(cron_fd)
        finally:
            os.close(descriptor)
        return
    if not stat.S_ISREG(entry.st_mode):
        raise OSError("Cron jobs lock is not an existing regular file")
    descriptor = os.open(
        name,
        os.O_RDWR
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0),
        dir_fd=cron_fd,
    )
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_dev != entry.st_dev
            or opened.st_ino != entry.st_ino
        ):
            raise OSError("Cron jobs lock changed while provisioning")
    finally:
        os.close(descriptor)


def _provision_cron_output_dir(cron_fd: int) -> None:
    """Create the output directory through the same checked cron identity."""
    name = "output"
    try:
        entry = os.stat(name, dir_fd=cron_fd, follow_symlinks=False)
    except FileNotFoundError:
        os.mkdir(name, 0o700, dir_fd=cron_fd)
        os.fsync(cron_fd)
        entry = os.stat(name, dir_fd=cron_fd, follow_symlinks=False)
    if not stat.S_ISDIR(entry.st_mode):
        raise OSError("Cron output path is not a directory")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    descriptor = os.open(name, flags, dir_fd=cron_fd)
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or opened.st_dev != entry.st_dev
            or opened.st_ino != entry.st_ino
        ):
            raise OSError("Cron output directory changed while provisioning")
        os.fchmod(descriptor, 0o700)
    finally:
        os.close(descriptor)


def _open_pinned_cron_output_dir() -> tuple[int, int]:
    """Open the cron/output pair through one no-follow cron directory handle."""
    cron_fd = -1
    output_fd = -1
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        cron_fd = _open_cron_directory_for_provisioning()
        _provision_cron_output_dir(cron_fd)
        entry = os.stat("output", dir_fd=cron_fd, follow_symlinks=False)
        if not stat.S_ISDIR(entry.st_mode):
            raise OSError("Cron output path is not a directory")
        output_fd = os.open("output", directory_flags, dir_fd=cron_fd)
        opened = os.fstat(output_fd)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or opened.st_dev != entry.st_dev
            or opened.st_ino != entry.st_ino
        ):
            raise OSError("Cron output directory changed while opening")
        return cron_fd, output_fd
    except Exception:
        if output_fd >= 0:
            os.close(output_fd)
        if cron_fd >= 0:
            os.close(cron_fd)
        raise


def _open_pinned_job_output_dir(output_fd: int, job_name: str) -> int:
    """Provision and open one job output directory without following a path."""
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        entry = os.stat(job_name, dir_fd=output_fd, follow_symlinks=False)
    except FileNotFoundError:
        try:
            os.mkdir(job_name, 0o700, dir_fd=output_fd)
            os.fsync(output_fd)
        except FileExistsError:
            pass
        entry = os.stat(job_name, dir_fd=output_fd, follow_symlinks=False)
    if not stat.S_ISDIR(entry.st_mode):
        raise OSError("Cron job output path is not a directory")
    descriptor = os.open(job_name, directory_flags, dir_fd=output_fd)
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or opened.st_dev != entry.st_dev
            or opened.st_ino != entry.st_ino
        ):
            raise OSError("Cron job output directory changed while opening")
        os.fchmod(descriptor, 0o700)
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _write_pinned_job_output(job_fd: int, filename: str, output: str) -> None:
    """Atomically replace one output file through its already-pinned directory."""
    descriptor = -1
    temporary = ""
    try:
        for _attempt in range(16):
            candidate = f".output_{os.getpid()}_{os.urandom(8).hex()}.tmp"
            try:
                descriptor = os.open(
                    candidate,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=job_fd,
                )
                temporary = candidate
                break
            except FileExistsError:
                continue
        if descriptor < 0:
            raise OSError("cannot create cron output temporary file")
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            handle.write(output)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, filename, src_dir_fd=job_fd, dst_dir_fd=job_fd)
        temporary = ""
        os.fsync(job_fd)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary:
            try:
                os.unlink(temporary, dir_fd=job_fd)
            except OSError:
                pass


def _prune_pinned_job_output(job_fd: int, keep: int) -> int:
    """Prune retained output files without resolving or globbing their path."""
    if keep <= 0:
        return 0
    try:
        files = []
        for name in os.listdir(job_fd):
            if not name.endswith(".md"):
                continue
            entry = os.stat(name, dir_fd=job_fd, follow_symlinks=False)
            if stat.S_ISREG(entry.st_mode):
                files.append(name)
        files.sort(reverse=True)
    except OSError:
        return 0
    deleted = 0
    for stale in files[keep:]:
        try:
            os.unlink(stale, dir_fd=job_fd)
            deleted += 1
        except OSError as exc:
            logger.debug("Failed to prune cron output %s: %s", stale, exc)
    return deleted


def _validate_pinned_cron_path(cron_fd: int) -> None:
    """Do not report a mutable path as the destination of a pinned write."""
    try:
        entry = os.stat(_current_cron_store().cron_dir, follow_symlinks=False)
    except OSError as exc:
        raise OSError("Cron directory changed during output persistence") from exc
    opened = os.fstat(cron_fd)
    if (
        not stat.S_ISDIR(entry.st_mode)
        or not stat.S_ISDIR(opened.st_mode)
        or entry.st_dev != opened.st_dev
        or entry.st_ino != opened.st_ino
    ):
        raise OSError("Cron directory changed during output persistence")


def ensure_dirs():
    """Provision one cron store before any runtime lock acquisition.

    This is the only Core-owned creation path for ``cron/.jobs.lock``.  The
    lock manager itself intentionally opens an already-provisioned regular
    file, so governed writes fail closed if later path state is untrusted.
    """
    cron_fd = _open_cron_directory_for_provisioning()
    try:
        os.fchmod(cron_fd, 0o700)
        _provision_jobs_lock(cron_fd)
        _provision_cron_output_dir(cron_fd)
    finally:
        os.close(cron_fd)


# =============================================================================
# Schedule Parsing
# =============================================================================

def parse_duration(s: str) -> int:
    """
    Parse duration string into minutes.
    
    Examples:
        "30m" → 30
        "2h" → 120
        "1d" → 1440
    """
    s = s.strip().lower()
    match = re.match(r'^(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)$', s)
    if not match:
        raise ValueError(f"Invalid duration: '{s}'. Use format like '30m', '2h', or '1d'")
    
    value = int(match.group(1))
    unit = match.group(2)[0]  # First char: m, h, or d
    
    multipliers = {'m': 1, 'h': 60, 'd': 1440}
    return value * multipliers[unit]


def parse_schedule(schedule: str) -> Dict[str, Any]:
    """
    Parse schedule string into structured format.
    
    Returns dict with:
        - kind: "once" | "interval" | "cron"
        - For "once": "run_at" (ISO timestamp)
        - For "interval": "minutes" (int)
        - For "cron": "expr" (cron expression)
    
    Examples:
        "30m"              → once in 30 minutes
        "2h"               → once in 2 hours
        "every 30m"        → recurring every 30 minutes
        "every 2h"         → recurring every 2 hours
        "0 9 * * *"        → cron expression
        "2026-02-03T14:00" → once at timestamp
    """
    schedule = schedule.strip()
    original = schedule
    schedule_lower = schedule.lower()
    
    # "every X" pattern → recurring interval
    if schedule_lower.startswith("every "):
        duration_str = schedule[6:].strip()
        minutes = parse_duration(duration_str)
        return {
            "kind": "interval",
            "minutes": minutes,
            "display": f"every {minutes}m"
        }
    
    # Check for cron expression (5 or 6 space-separated fields)
    # Cron fields: minute hour day month weekday [year]
    parts = schedule.split()
    if len(parts) >= 5 and all(
        re.match(r'^[\d\*\-,/]+$', p) for p in parts[:5]
    ):
        if not HAS_CRONITER:
            raise ValueError("Cron expressions require 'croniter' package. Install with: pip install croniter")
        # Validate cron expression
        try:
            croniter(schedule)
        except Exception as e:
            raise ValueError(f"Invalid cron expression '{schedule}': {e}")
        return {
            "kind": "cron",
            "expr": schedule,
            "display": schedule
        }
    
    # ISO timestamp (contains T or looks like date)
    if 'T' in schedule or re.match(r'^\d{4}-\d{2}-\d{2}', schedule):
        try:
            # Parse and validate
            dt = datetime.fromisoformat(schedule.replace('Z', '+00:00'))
            # Make naive timestamps timezone-aware at parse time so the stored
            # value doesn't depend on the system timezone matching at check time.
            #
            # Anchor to the CONFIGURED Hermes timezone, not the server's local
            # timezone. The due-check (`get_due_jobs`) compares `next_run_at`
            # against `hermes_time.now()`, which uses the configured zone. If a
            # naive "20:07" were interpreted as server-local (e.g. UTC) while
            # now() runs in Asia/Kolkata, the stored instant would land hours
            # off from the user's wall-clock intent — far enough that one-shots
            # never become due and recurring jobs fire at the wrong time. Using
            # the configured zone makes "20:07" mean 20:07 on the same clock the
            # scheduler checks against (#51021).
            if dt.tzinfo is None:
                hermes_tz = _hermes_now().tzinfo
                dt = dt.replace(tzinfo=hermes_tz)
            return {
                "kind": "once",
                "run_at": dt.isoformat(),
                "display": f"once at {dt.strftime('%Y-%m-%d %H:%M')}"
            }
        except ValueError as e:
            raise ValueError(f"Invalid timestamp '{schedule}': {e}")
    
    # Duration like "30m", "2h", "1d" → one-shot from now
    try:
        minutes = parse_duration(schedule)
        run_at = _hermes_now() + timedelta(minutes=minutes)
        return {
            "kind": "once",
            "run_at": run_at.isoformat(),
            "display": f"once in {original}"
        }
    except ValueError:
        pass
    
    raise ValueError(
        f"Invalid schedule '{original}'. Use:\n"
        f"  - Duration: '30m', '2h', '1d' (one-shot)\n"
        f"  - Interval: 'every 30m', 'every 2h' (recurring)\n"
        f"  - Cron: '0 9 * * *' (cron expression)\n"
        f"  - Timestamp: '2026-02-03T14:00:00' (one-shot at time)"
    )


def _ensure_aware(dt: datetime) -> datetime:
    """Return a timezone-aware datetime in Hermes configured timezone.

    Backward compatibility:
    - Older stored timestamps may be naive.
    - Naive values are interpreted as *system-local wall time* (the timezone
      `datetime.now()` used when they were created), then converted to the
      configured Hermes timezone.

    This preserves relative ordering for legacy naive timestamps across
    timezone changes and avoids false not-due results.
    """
    target_tz = _hermes_now().tzinfo
    if dt.tzinfo is None:
        local_tz = datetime.now().astimezone().tzinfo
        return dt.replace(tzinfo=local_tz).astimezone(target_tz)
    return dt.astimezone(target_tz)


def _timezone_offset_mismatch(stored: datetime, current: datetime) -> bool:
    """Return True when a stored aware timestamp uses a different UTC offset.

    Naive stored timestamps return False: they carry no offset to compare, and
    are normalized by ``_ensure_aware`` instead — they intentionally never take
    the offset-repair path.
    """
    if stored.tzinfo is None or current.tzinfo is None:
        return False
    return stored.utcoffset() != current.utcoffset()


def _stored_wall_clock_is_future(stored: datetime, current: datetime) -> bool:
    """Return True when the stored local wall-clock time has not arrived yet.

    Cron schedules express local wall-clock intent. If Hermes/system local time
    changes after next_run_at was persisted, an old offset can make a future
    wall-clock run look due at the converted absolute time (for example
    21:00+10 becomes 13:00+02). Comparing naive wall-clock values lets us
    distinguish that migration case from a genuinely missed run whose scheduled
    wall time has already passed.
    """
    return stored.replace(tzinfo=None) > current.replace(tzinfo=None)


def _recoverable_oneshot_run_at(
    schedule: Dict[str, Any],
    now: datetime,
    *,
    last_run_at: Optional[str] = None,
) -> Optional[str]:
    """Return a one-shot run time if it is still eligible to fire.

    One-shot jobs get a small grace window so jobs created a few seconds after
    their requested minute still run on the next tick. Once a one-shot has
    already run, it is never eligible again.
    """
    if not isinstance(schedule, dict) or schedule.get("kind") != "once":
        return None
    if last_run_at:
        return None

    run_at = schedule.get("run_at")
    if not run_at:
        return None

    try:
        run_at_dt = _ensure_aware(datetime.fromisoformat(run_at))
    except Exception:
        return None
    if run_at_dt >= now - timedelta(seconds=ONESHOT_GRACE_SECONDS):
        return run_at
    return None


def _compute_grace_seconds(schedule: dict) -> int:
    """Compute how late a job can be and still catch up instead of fast-forwarding.

    Uses half the schedule period, clamped between 120 seconds and 2 hours.
    This ensures daily jobs can catch up if missed by up to 2 hours,
    while frequent jobs (every 5-10 min) still fast-forward quickly.
    """
    MIN_GRACE = 120
    MAX_GRACE = 7200  # 2 hours

    kind = schedule.get("kind")

    if kind == "interval":
        period_seconds = schedule.get("minutes", 1) * 60
        grace = period_seconds // 2
        return max(MIN_GRACE, min(grace, MAX_GRACE))

    if kind == "cron" and HAS_CRONITER:
        expr = schedule.get("expr")
        if expr:
            try:
                now = _hermes_now()
                cron = croniter(expr, now)
                first = cron.get_next(datetime)
                second = cron.get_next(datetime)
                period_seconds = int((second - first).total_seconds())
                grace = period_seconds // 2
                return max(MIN_GRACE, min(grace, MAX_GRACE))
            except Exception:
                pass

    return MIN_GRACE


def compute_next_run(schedule: Dict[str, Any], last_run_at: Optional[str] = None) -> Optional[str]:
    """
    Compute the next run time for a schedule.

    Returns ISO timestamp string, or None if no more runs.
    """
    now = _hermes_now()

    if not isinstance(schedule, dict):
        return None
    kind = schedule.get("kind")
    if kind is None:
        return None

    if kind == "once":
        return _recoverable_oneshot_run_at(schedule, now, last_run_at=last_run_at)

    elif kind == "interval":
        minutes = schedule.get("minutes")
        if minutes is None:
            return None
        if last_run_at:
            try:
                last = _ensure_aware(datetime.fromisoformat(last_run_at))
                next_run = last + timedelta(minutes=minutes)
            except Exception:
                next_run = now + timedelta(minutes=minutes)
        else:
            # First run is now + interval
            next_run = now + timedelta(minutes=minutes)
        return next_run.isoformat()

    elif kind == "cron":
        expr = schedule.get("expr")
        if not expr:
            return None
        if not HAS_CRONITER:
            logger.warning(
                "Cannot compute next run for cron schedule %r: 'croniter' is "
                "not installed. croniter is a core dependency as of v0.9.x; "
                "reinstall hermes-agent or run 'pip install croniter' in your "
                "runtime env.",
                expr,
            )
            return None
        # Use last_run_at as the croniter base when available, consistent
        # with interval jobs.  This ensures that after a crash/restart,
        # the next run is anchored to the actual last execution time
        # rather than to an arbitrary restart time.
        base_time = now
        if last_run_at:
            try:
                base_time = _ensure_aware(datetime.fromisoformat(last_run_at))
            except Exception:
                base_time = now
        cron = croniter(expr, base_time)
        next_run = cron.get_next(datetime)
        return next_run.isoformat()

    return None


# =============================================================================
# Ticker heartbeat (liveness signal for `hermes cron status`)
# =============================================================================

def _atomic_write_epoch(path: Path) -> None:
    """Atomically write the current epoch time to ``path``.

    Uses the same tmpfile + ``atomic_replace`` pattern as ``save_jobs`` so a
    concurrent reader in another process (``hermes cron status``) never sees a
    torn/truncated file. Best-effort: failures are swallowed by callers.
    """
    fd, tmp_path = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp", prefix=".hb_")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(str(time.time()))
            f.flush()
            os.fsync(f.fileno())
        atomic_replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def record_ticker_heartbeat(success: bool = False) -> None:
    """Record a ticker liveness signal, and optionally a successful-tick signal.

    The ticker calls this once per loop iteration. ``success=True`` additionally
    bumps the *last successful tick* marker. We track two distinct signals so
    `hermes cron status` can tell a thread that is merely *alive and looping*
    (heartbeat fresh, success stale) from one that is actually *firing jobs*
    (both fresh) — a ticker stuck failing every tick would otherwise keep the
    plain heartbeat fresh and falsely report healthy (#32612, #32895).

    Best-effort: a write failure must never disrupt the tick loop.
    """
    try:
        _atomic_write_epoch(TICKER_HEARTBEAT_FILE)
    except Exception:
        pass
    if success:
        try:
            _atomic_write_epoch(TICKER_SUCCESS_FILE)
        except Exception:
            pass


def _epoch_file_age(path: Path) -> Optional[float]:
    try:
        raw = path.read_text(encoding="utf-8").strip()
        return max(0.0, time.time() - float(raw))
    except Exception:
        return None


def get_ticker_heartbeat_age() -> Optional[float]:
    """Seconds since the ticker loop last iterated, or None if unknown.

    None = heartbeat file missing/unreadable (older build, never ran, or a
    torn read). Callers treat None as "cannot determine", not "dead".
    """
    return _epoch_file_age(TICKER_HEARTBEAT_FILE)


def get_ticker_success_age() -> Optional[float]:
    """Seconds since the ticker last completed a tick WITHOUT raising, or None."""
    return _epoch_file_age(TICKER_SUCCESS_FILE)


# =============================================================================
# Job CRUD Operations
# =============================================================================

def _read_jobs_payload_from_text(raw: str) -> tuple[Any, bool]:
    """Parse jobs store text, returning ``(payload, used_strict_false)``."""
    try:
        return json.loads(raw), False
    except json.JSONDecodeError:
        try:
            return json.loads(raw, strict=False), True
        except Exception as e:
            logger.error("Failed to auto-repair jobs.json: %s", e)
            raise RuntimeError(f"Cron database corrupted and unrepairable: {e}") from e


def _read_jobs_payload_bytes(raw: bytes) -> tuple[Any, bool]:
    try:
        text = raw.decode("utf-8")
    except UnicodeError as e:
        logger.error("IOError reading jobs.json: %s", e)
        raise RuntimeError(f"Failed to read cron database: {e}") from e
    return _read_jobs_payload_from_text(text)


def _read_jobs_payload_from_path(jobs_file: Path) -> tuple[Any, bool]:
    try:
        with open(jobs_file, "r", encoding="utf-8") as handle:
            raw = handle.read()
    except IOError as e:
        logger.error("IOError reading jobs.json: %s", e)
        raise RuntimeError(f"Failed to read cron database: {e}") from e
    return _read_jobs_payload_from_text(raw)


def _read_jobs_payload_from_cron_fd(cron_fd: int) -> tuple[Any, bool] | None:
    """Read jobs.json through the active Core lock's pinned cron directory."""
    descriptor = -1
    try:
        try:
            entry = os.stat("jobs.json", dir_fd=cron_fd, follow_symlinks=False)
        except FileNotFoundError:
            return None
        if not stat.S_ISREG(entry.st_mode):
            raise RuntimeError(
                "Cron database corrupted and unrepairable: jobs store is not a regular file"
            )
        descriptor = os.open(
            "jobs.json",
            os.O_RDONLY
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
            dir_fd=cron_fd,
        )
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_dev != entry.st_dev
            or opened.st_ino != entry.st_ino
        ):
            raise RuntimeError(
                "Cron database corrupted and unrepairable: jobs store changed while opening"
            )
        chunks: list[bytes] = []
        while True:
            piece = os.read(descriptor, 1024 * 1024)
            if not piece:
                break
            chunks.append(piece)
        return _read_jobs_payload_bytes(b"".join(chunks))
    except OSError as e:
        logger.error("IOError reading jobs.json: %s", e)
        raise RuntimeError(f"Failed to read cron database: {e}") from e
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _normalize_loaded_jobs_payload(
    data: Any,
    *,
    strict_retry: bool,
    repair_recoverable: bool = True,
) -> List[Dict[str, Any]]:
    """Validate the top-level jobs store shape and repair recoverable layouts."""
    # Validate the top-level JSON shape: accept a dict (expected) or a bare
    # list (auto-repair). Anything else (str/number/null) is corruption that
    # would otherwise raise an uncaught AttributeError on ``.get()`` and take
    # down the whole cron subsystem.
    if isinstance(data, dict):
        jobs = data.get("jobs", [])
        if strict_retry and not repair_recoverable:
            raise CronJobGovernanceError(
                "Cron job persistence needs administrator review "
                "(jobs store requires control-character repair)."
            )
        if strict_retry and jobs:
            # Hit control-character corruption — rewrite with proper escaping.
            # Prefer the already-held Core lock identity when present so a path
            # swap cannot redirect the auto-repair write.
            try:
                capability = _issue_jobs_lock_capability()
            except CronJobGovernanceError:
                capability = None
            if capability is not None and _validate_jobs_lock_capability(capability) is not None:
                _save_jobs_unlocked(jobs)
            else:
                save_jobs(jobs)
            logger.warning("Auto-repaired jobs.json (had invalid control characters)")
        return jobs
    if isinstance(data, list):
        if not repair_recoverable:
            raise CronJobGovernanceError(
                "Cron job persistence needs administrator review "
                "(jobs store requires shape repair)."
            )
        # Bare array — likely saved/edited outside save_jobs(). Wrap it back
        # into the expected {"jobs": [...]} structure.
        if data:
            try:
                capability = _issue_jobs_lock_capability()
            except CronJobGovernanceError:
                capability = None
            if capability is not None and _validate_jobs_lock_capability(capability) is not None:
                _save_jobs_unlocked(data)
            else:
                save_jobs(data)
            logger.warning("Auto-repaired jobs.json (bare list wrapped as dict)")
        return data

    raise RuntimeError(
        f"Cron database corrupted: expected {{'jobs': [...]}}, got {type(data).__name__}"
    )


def load_jobs(*, repair_recoverable: bool = True) -> List[Dict[str, Any]]:
    """Load all jobs from storage.

    When Core's jobs lock is already held, the read stays on the same pinned
    cron directory identity used by ``_save_jobs_unlocked``. Path-based reads
    remain only for unlocked observers so a rename/swap cannot split load and
    save across two trees inside one governed mutation.
    """
    held_cross_process = bool(
        getattr(_jobs_lock_state, "depth", 0)
        and getattr(_jobs_lock_state, "cross_process_acquired", False)
    )
    try:
        capability = _issue_jobs_lock_capability()
    except CronJobGovernanceError:
        capability = None
    if capability is not None and _validate_jobs_lock_capability(capability) is not None:
        cron_fd = _duplicate_active_jobs_lock_cron_fd()
        try:
            payload = _read_jobs_payload_from_cron_fd(cron_fd)
        finally:
            os.close(cron_fd)
        if payload is None:
            return []
        data, strict_retry = payload
        return _normalize_loaded_jobs_payload(
            data,
            strict_retry=strict_retry,
            repair_recoverable=repair_recoverable,
        )
    if held_cross_process:
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review "
            "(Core jobs lock capability unavailable)."
        )

    jobs_file = _current_cron_store().jobs_file
    if not jobs_file.exists():
        return []
    data, strict_retry = _read_jobs_payload_from_path(jobs_file)
    return _normalize_loaded_jobs_payload(
        data,
        strict_retry=strict_retry,
        repair_recoverable=repair_recoverable,
    )


def _save_jobs_unlocked(jobs: List[Dict[str, Any]]):
    """Save through the active Core lock's pinned cron directory identity."""
    cron_fd = _duplicate_active_jobs_lock_cron_fd()
    descriptor = -1
    temp_name = ""
    try:
        try:
            existing = os.stat("jobs.json", dir_fd=cron_fd, follow_symlinks=False)
        except FileNotFoundError:
            existing = None
        if existing is not None and not stat.S_ISREG(existing.st_mode):
            raise CronJobGovernanceError(
                "Cron job persistence needs administrator review "
                "(jobs store is not a regular file)."
            )
        for _attempt in range(16):
            candidate = f".jobs_{os.getpid()}_{os.urandom(8).hex()}.tmp"
            try:
                descriptor = os.open(
                    candidate,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=cron_fd,
                )
                temp_name = candidate
                break
            except FileExistsError:
                continue
        if descriptor < 0:
            raise CronJobGovernanceError(
                "Cron job persistence needs administrator review "
                "(cannot create pinned jobs store temporary file)."
            )
        with os.fdopen(descriptor, 'w', encoding='utf-8') as f:
            descriptor = -1
            json.dump({"jobs": jobs, "updated_at": _hermes_now().isoformat()}, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        validation_fd = _duplicate_active_jobs_lock_cron_fd()
        os.close(validation_fd)
        os.replace(temp_name, "jobs.json", src_dir_fd=cron_fd, dst_dir_fd=cron_fd)
        temp_name = ""
        os.fsync(cron_fd)
    except BaseException:
        raise
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temp_name:
            try:
                os.unlink(temp_name, dir_fd=cron_fd)
            except OSError:
                pass
        os.close(cron_fd)


def save_jobs(jobs: List[Dict[str, Any]]):
    """Save all jobs to storage."""
    with _jobs_lock(require_cross_process=True):
        _save_jobs_unlocked(jobs)


def _normalize_workdir(workdir: Optional[str]) -> Optional[str]:
    """Normalize and validate a cron job workdir.

    Rules:
      - Empty / None → None (feature off, preserves old behaviour).
      - ``~`` is expanded.  Relative paths are rejected — cron jobs run detached
        from any shell cwd, so relative paths have no stable meaning.
      - The path must exist and be a directory at create/update time.  We do
        NOT re-check at run time (a user might briefly unmount the dir; the
        scheduler will just fall back to old behaviour with a logged warning).

    Returns the absolute path string, or None when disabled.
    Raises ValueError on invalid input.
    """
    if workdir is None:
        return None
    raw = str(workdir).strip()
    if not raw:
        return None
    expanded = Path(raw).expanduser()
    if not expanded.is_absolute():
        raise ValueError(
            f"Cron workdir must be an absolute path (got {raw!r}). "
            f"Cron jobs run detached from any shell cwd, so relative paths are ambiguous."
        )
    resolved = expanded.resolve()
    if not resolved.exists():
        raise ValueError(f"Cron workdir does not exist: {resolved}")
    if not resolved.is_dir():
        raise ValueError(f"Cron workdir is not a directory: {resolved}")
    return str(resolved)


def _resolve_default_model_snapshot() -> Optional[str]:
    """Resolve the global default model the same way the cron ticker does.

    Mirrors the unpinned-model resolution in ``cron/scheduler.py`` ``run_job``:
    read ``config.yaml`` ``model.default`` (or the ``model`` alias / bare string
    form), applying the managed-scope overlay and env expansion. Used by
    ``create_job`` to snapshot the default model for unpinned jobs so a later
    swap of the global default is detected at fire time (#44585).

    Returns the resolved model string, or ``None`` if config is missing/empty
    or resolution fails (fail-open — caller treats ``None`` as "no snapshot").
    """
    try:
        import yaml
        from hermes_cli.config import _expand_env_vars

        cfg_path = get_hermes_home() / "config.yaml"
        if not cfg_path.exists():
            return None
        with cfg_path.open(encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        try:
            from hermes_cli import managed_scope
            cfg = managed_scope.apply_managed_overlay(cfg)
        except Exception:
            pass
        cfg = _expand_env_vars(cfg)
        model_cfg = cfg.get("model") or {}
        if isinstance(model_cfg, str):
            return model_cfg.strip() or None
        if isinstance(model_cfg, dict):
            default = model_cfg.get("default") or model_cfg.get("model")
            if isinstance(default, str):
                return default.strip() or None
        return None
    except Exception:
        return None


def _normalize_job_optional_text(value: Any, *, strip_trailing_slash: bool = False) -> Optional[str]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if strip_trailing_slash:
        text = text.rstrip("/")
    return text or None


def _compute_provider_model_snapshots(
    *,
    provider: Any,
    model: Any,
    base_url: Any,
    no_agent: Any,
) -> Tuple[Optional[str], Optional[str]]:
    """Snapshot unpinned inference axes for the provider/model drift guard.

    Agent cron jobs with unpinned provider/model follow global config at fire
    time. Capture the current resolution for each unpinned axis so a later
    global switch fails closed instead of silently changing spend. Pinned axes
    and no-agent script jobs intentionally carry no snapshot.
    """
    normalized_provider = _normalize_job_optional_text(provider)
    normalized_model = _normalize_job_optional_text(model)
    normalized_base_url = _normalize_job_optional_text(
        base_url,
        strip_trailing_slash=True,
    )
    if bool(no_agent):
        return None, None

    provider_snapshot: Optional[str] = None
    model_snapshot: Optional[str] = None
    if normalized_provider is None:
        try:
            from hermes_cli.runtime_provider import resolve_runtime_provider

            runtime_kwargs = {"requested": None}
            if normalized_base_url:
                runtime_kwargs["explicit_base_url"] = normalized_base_url
            snap = resolve_runtime_provider(**runtime_kwargs)
            snap_provider = str(snap.get("provider") or "").strip().lower()
            provider_snapshot = snap_provider or None
        except Exception:
            provider_snapshot = None
    if normalized_model is None:
        try:
            model_snapshot = _resolve_default_model_snapshot() or None
        except Exception:
            model_snapshot = None
    return provider_snapshot, model_snapshot


def _normalized_inference_axes(job: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], bool]:
    """Return the stored inference-routing fields in their semantic form."""
    return (
        _normalize_job_optional_text(job.get("provider")),
        _normalize_job_optional_text(job.get("model")),
        _normalize_job_optional_text(job.get("base_url"), strip_trailing_slash=True),
        bool(job.get("no_agent")),
    )


def create_job(
    prompt: Optional[str],
    schedule: str,
    name: Optional[str] = None,
    repeat: Optional[int] = None,
    deliver: Optional[str] = None,
    origin: Optional[Dict[str, Any]] = None,
    skill: Optional[str] = None,
    skills: Optional[List[str]] = None,
    model: Optional[str] = None,
    provider: Optional[str] = None,
    base_url: Optional[str] = None,
    script: Optional[str] = None,
    context_from: Optional[Union[str, List[str]]] = None,
    enabled_toolsets: Optional[List[str]] = None,
    workdir: Optional[str] = None,
    no_agent: bool = False,
    attach_to_session: Optional[bool] = None,
    authorized_behavior_ref: Optional[str] = None,
    implementation_categories: Optional[List[str]] = None,
    governance_resume: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Create a new cron job.

    Args:
        prompt: The prompt to run (must be self-contained, or a task instruction when skill is set).
                Ignored when ``no_agent=True`` except as an optional name hint.
        schedule: Schedule string (see parse_schedule)
        name: Optional friendly name
        repeat: How many times to run (None = forever, 1 = once)
        deliver: Where to deliver output ("origin", "local", "telegram", etc.)
        origin: Source info where job was created (for "origin" delivery)
        skill: Optional legacy single skill name to load before running the prompt
        skills: Optional ordered list of skills to load before running the prompt
        model: Optional per-job model override
        provider: Optional per-job provider override
        base_url: Optional per-job base URL override
        script: Optional path to a script whose stdout feeds the job. With
                ``no_agent=True`` the script IS the job — its stdout is
                delivered verbatim. Without ``no_agent``, its stdout is
                injected into the agent's prompt as context (data-collection /
                change-detection pattern). Paths resolve under
                ~/.hermes/scripts/; ``.sh`` / ``.bash`` files run via bash,
                anything else via Python.
        context_from: Optional job ID (or list of job IDs) whose most recent output
                      is injected into the prompt as context before each run.
                      Useful for chaining cron jobs: job A finds data, job B processes it.
        enabled_toolsets: Optional list of toolset names to restrict the agent to.
                          When set, only tools from these toolsets are loaded, reducing
                          token overhead. When omitted, all default tools are loaded.
                          Ignored when ``no_agent=True``.
        workdir: Optional absolute path.  When set, the job runs as if launched
                from that directory: AGENTS.md / CLAUDE.md / .cursorrules from
                that directory are injected into the system prompt, and the
                terminal/file/code_exec tools use it as their working directory
                (via TERMINAL_CWD).  When unset, the old behaviour is preserved
                (no context files injected, tools use the scheduler's cwd).
                With ``no_agent=True``, ``workdir`` is still applied as the
                script's cwd so relative paths inside the script behave
                predictably.
        no_agent: When True, skip the agent entirely — run ``script`` on schedule
                and deliver its stdout directly. Empty stdout = silent (no
                delivery). Requires ``script`` to be set. Ideal for classic
                watchdogs and periodic alerts that don't need LLM reasoning.

    Returns:
        The created job dict
    """
    if governance_resume is not None:
        candidate = _resume_candidate(governance_resume)
        with _jobs_lock(require_cross_process=True):
            jobs = load_jobs()
            governed, disposition = _apply_cron_creation_governance("create", candidate, jobs)
            if disposition == "already_persisted":
                return _normalize_job_record(governed)
            if any(str(item.get("id") or "") == str(governed.get("id") or "") for item in jobs):
                raise CronJobGovernanceError(
                    "Cron job persistence needs administrator review (resume job id conflict)."
                )
            jobs.append(governed)
            _save_jobs_unlocked(jobs)
        return governed

    parsed_schedule = parse_schedule(schedule)

    # Normalize repeat: treat 0 or negative values as None (infinite)
    if repeat is not None and repeat <= 0:
        repeat = None

    # Auto-set repeat=1 for one-shot schedules if not specified
    if parsed_schedule["kind"] == "once" and repeat is None:
        repeat = 1

    # Default delivery to origin if available, otherwise local
    if deliver is None:
        deliver = "origin" if origin else "local"

    job_id = uuid.uuid4().hex[:12]
    now = _hermes_now().isoformat()

    normalized_skills = _normalize_skill_list(skill, skills)
    normalized_model = _normalize_job_optional_text(model)
    normalized_provider = _normalize_job_optional_text(provider)
    normalized_base_url = _normalize_job_optional_text(base_url, strip_trailing_slash=True)
    normalized_script = str(script).strip() if isinstance(script, str) else None
    normalized_script = normalized_script or None
    normalized_toolsets = [str(t).strip() for t in enabled_toolsets if str(t).strip()] if enabled_toolsets else None
    normalized_toolsets = normalized_toolsets or None
    normalized_workdir = _normalize_workdir(workdir)
    normalized_no_agent = bool(no_agent)
    normalized_attach = attach_to_session if isinstance(attach_to_session, bool) else None

    # no_agent jobs are meaningless without a script — the script IS the job.
    # Surface this as a clear ValueError at create time so bad configs never
    # reach the scheduler.
    if normalized_no_agent and not normalized_script:
        raise ValueError(
            "no_agent=True requires a script — with no agent and no script "
            "there is nothing for the job to run."
        )

    # Normalize context_from: accept str or list of str, store as list or None
    if isinstance(context_from, str):
        context_from = [context_from.strip()] if context_from.strip() else None
    elif isinstance(context_from, list):
        context_from = [str(j).strip() for j in context_from if str(j).strip()] or None
    else:
        context_from = None

    prompt_text = _coerce_job_text(prompt)

    # Reject cron jobs that schedule gateway-lifecycle commands. Prevents
    # agent-driven SIGTERM-respawn loops under launchd/systemd KeepAlive
    # (#30719). Enforced here (not only in the CLI layer) so the agent's
    # `cronjob` model tool — which calls create_job directly — is also
    # covered, not just `hermes cron create`.
    from cron.lifecycle_guard import check_gateway_lifecycle
    check_gateway_lifecycle(prompt_text, normalized_script)

    label_source = (prompt_text or (normalized_skills[0] if normalized_skills else None) or (normalized_script if normalized_no_agent else None)) or "cron job"

    provider_snapshot, model_snapshot = _compute_provider_model_snapshots(
        provider=normalized_provider,
        model=normalized_model,
        base_url=normalized_base_url,
        no_agent=normalized_no_agent,
    )

    next_run_at = compute_next_run(parsed_schedule)
    if parsed_schedule.get("kind") == "once" and next_run_at is None:
        run_at = parsed_schedule.get("run_at") or schedule
        logger.warning(
            "Rejecting one-shot cron job '%s': run_at %s is outside the %ss grace window",
            name or label_source[:50].strip(),
            run_at,
            ONESHOT_GRACE_SECONDS,
        )
        raise ValueError(
            f"Requested one-shot time {run_at} is more than "
            f"{ONESHOT_GRACE_SECONDS}s in the past and cannot be scheduled."
        )

    job = {
        "id": job_id,
        "name": name or label_source[:50].strip(),
        "prompt": prompt_text,
        "skills": normalized_skills,
        "skill": normalized_skills[0] if normalized_skills else None,
        "model": normalized_model,
        "provider": normalized_provider,
        # Provider/model resolution captured at creation for unpinned jobs
        # (#44585). None for pinned axes, no_agent jobs, resolution failures, and
        # any pre-existing job written before these fields existed (back-compat).
        "provider_snapshot": provider_snapshot,
        "model_snapshot": model_snapshot,
        "base_url": normalized_base_url,
        "script": normalized_script,
        "no_agent": normalized_no_agent,
        "context_from": context_from,
        "schedule": parsed_schedule,
        "schedule_display": parsed_schedule.get("display", schedule),
        "repeat": {
            "times": repeat,  # None = forever
            "completed": 0
        },
        "enabled": True,
        "state": "scheduled",
        "paused_at": None,
        "paused_reason": None,
        "created_at": now,
        "next_run_at": next_run_at,
        "last_run_at": None,
        "last_status": None,
        "last_error": None,
        "last_delivery_error": None,
        _CRON_DELIVERY_RECEIPT_FIELD: None,
        _CRON_RUN_OUTCOME_RECEIPT_FIELD: None,
        "last_runtime_admission_receipt": None,
        # Delivery configuration
        "deliver": deliver,
        "origin": origin,  # Tracks where job was created for "origin" delivery
        "enabled_toolsets": normalized_toolsets,
        "workdir": normalized_workdir,
    }
    if authorized_behavior_ref is not None:
        job["authorized_behavior_ref"] = _normalize_job_optional_text(authorized_behavior_ref)
    if implementation_categories is not None:
        job["implementation_categories"] = [
            str(item).strip() for item in implementation_categories if str(item).strip()
        ]
    # Only persist attach_to_session when explicitly set, so existing jobs and
    # the common case stay byte-identical (absent key => fall back to the
    # global cron.mirror_delivery config, default off).
    if normalized_attach is not None:
        job["attach_to_session"] = normalized_attach

    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs()
        job, disposition = _apply_cron_creation_governance("create", job, jobs)
        if disposition != "allow_write":
            return _normalize_job_record(job)
        jobs.append(job)
        _save_jobs_unlocked(jobs)

    return job


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Get a job by ID."""
    jobs = load_jobs()
    for job in jobs:
        if job["id"] == job_id:
            return _normalize_job_record(job)
    return None


class AmbiguousJobReference(LookupError):
    """Raised when a job name matches more than one job."""

    def __init__(self, ref: str, matches: List[Dict[str, Any]]):
        self.ref = ref
        self.matches = matches
        ids = ", ".join(m["id"] for m in matches)
        super().__init__(
            f"Job name '{ref}' is ambiguous — matches {len(matches)} jobs: {ids}. "
            f"Use the job ID instead."
        )


def resolve_job_ref(
    ref: str,
    *,
    repair_recoverable: bool = True,
) -> Optional[Dict[str, Any]]:
    """Resolve a job reference (ID or name) to a job record.

    - Exact ID match wins (works even if a different job's name equals this ID).
    - Otherwise, case-insensitive name match.
    - If a name matches more than one job, raises AmbiguousJobReference so the
      caller can surface the matching IDs rather than silently picking one.
    """
    if not ref:
        return None
    jobs = load_jobs(repair_recoverable=repair_recoverable)
    for job in jobs:
        if job["id"] == ref:
            return _normalize_job_record(job)
    ref_lower = ref.lower()
    name_matches = [j for j in jobs if (j.get("name") or "").lower() == ref_lower]
    if not name_matches:
        return None
    if len(name_matches) > 1:
        raise AmbiguousJobReference(
            ref, [_normalize_job_record(j) for j in name_matches]
        )
    return _normalize_job_record(name_matches[0])


def list_jobs(include_disabled: bool = False) -> List[Dict[str, Any]]:
    """List all jobs, optionally including disabled ones."""
    jobs = [_normalize_job_record(j) for j in load_jobs()]
    if not include_disabled:
        jobs = [j for j in jobs if j.get("enabled", True)]
    return jobs


def update_job(
    job_id: str,
    updates: Dict[str, Any],
    governance_resume: Optional[Dict[str, Any]] = None,
    governance_refresh: bool = False,
    deprecated_verification_retirement: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Update a job by ID, refreshing derived schedule fields when needed."""
    special_requests = sum(
        request is not None and request is not False
        for request in (
            governance_resume,
            governance_refresh,
            deprecated_verification_retirement,
        )
    )
    if special_requests > 1:
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review "
            "(refresh, resume, and verification retirement are mutually exclusive)."
        )
    if (governance_refresh or deprecated_verification_retirement is not None) and updates:
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review "
            "(refresh and verification retirement require an otherwise unchanged Job)."
        )
    if governance_resume is not None:
        candidate = _resume_candidate(governance_resume)
        if str(candidate.get("id") or "") != str(job_id or ""):
            raise CronJobGovernanceError(
                "Cron job persistence needs administrator review (resume job id mismatch)."
            )
        with _jobs_lock(require_cross_process=True):
            jobs = load_jobs()
            target = next((index for index, item in enumerate(jobs) if item.get("id") == job_id), None)
            if target is None:
                return None
            if jobs[target].get(_CRON_RUN_OUTCOME_CLAIM_FIELD) is not None:
                raise CronJobGovernanceError(
                    "Cron job authorization cannot change while a signed run is active."
                )
            governed, disposition = _apply_cron_creation_governance("update", candidate, jobs)
            if disposition == "already_persisted":
                return _normalize_job_record(governed)
            jobs[target] = governed
            _save_jobs_unlocked(jobs)
            return _normalize_job_record(governed)

    # Block mutation of immutable fields. ``id`` in particular is a filesystem
    # path component under OUTPUT_DIR — letting an update change it leaks
    # path-escape values into output writes/deletes.
    bad_fields = _IMMUTABLE_JOB_FIELDS.intersection(updates or {})
    if bad_fields:
        raise ValueError(
            f"Cron job field(s) cannot be updated: {', '.join(sorted(bad_fields))}"
        )

    governance_expected = _cron_creation_governance_expected()
    if (
        (governance_refresh or deprecated_verification_retirement is not None)
        and not governance_expected
    ):
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review (governance unavailable)."
        )
    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs(
            repair_recoverable=not (
                governance_refresh or deprecated_verification_retirement is not None
            )
        )
        for i, job in enumerate(jobs):
            if job["id"] != job_id:
                continue

            if deprecated_verification_retirement is not None:
                request = deprecated_verification_retirement
                receipt = job.get("creation_governance_receipt")
                command = job.get("verification_command")
                expected_keys = {
                    "schema_version",
                    "profile_id",
                    "job_revision",
                    "command_sha256",
                }
                command_sha256 = (
                    "sha256:" + hashlib.sha256(command.encode("utf-8")).hexdigest()
                    if isinstance(command, str) and command
                    else ""
                )
                expected_command = (
                    f"HERMES_HOME={_cron_profile_home()} hermes cron status {job_id}"
                )
                if (
                    not isinstance(request, dict)
                    or set(request) != expected_keys
                    or request.get("schema_version") != "cron-verification-retirement/v1"
                    or not isinstance(receipt, dict)
                    or receipt.get("cron_job_id") != job_id
                    or request.get("profile_id") != receipt.get("profile_id")
                    or request.get("job_revision") != receipt.get("receipt_id")
                    or request.get("command_sha256") != command_sha256
                    or command != expected_command
                    or job.get("verification_command_mode") not in (None, "")
                ):
                    raise CronJobGovernanceError(
                        "Cron job persistence needs administrator review "
                        "(deprecated verification retirement precondition mismatch)."
                    )

            # Validate / normalize workdir if present in updates.  Empty string
            # or None both mean "clear the field" (restore old behaviour).
            if "workdir" in updates:
                _wd = updates["workdir"]
                if _wd in {None, "", False}:
                    updates["workdir"] = None
                else:
                    updates["workdir"] = _normalize_workdir(_wd)

            special_operation = governance_refresh or deprecated_verification_retirement is not None
            # Only re-derive the canonical skill/skills pair when the caller
            # actually changes one of those fields. Runtime-only updates
            # (pause/resume/trigger) must preserve the stored definition so a
            # governed Job's candidate/material hash stays exact; unconditional
            # normalization here rewrites legacy ``skill: null`` + ``skills``
            # shapes and turns an authorized runtime transition into a
            # candidate_hash_mismatch.
            normalize_skills = "skills" in updates or "skill" in updates
            previous_inference_axes = _normalized_inference_axes(job)
            updated = (
                copy.deepcopy(job)
                if special_operation
                else ({**job, **updates}
                      if not normalize_skills
                      else _apply_skill_fields({**job, **updates}))
            )
            if deprecated_verification_retirement is not None:
                updated.pop("verification_command", None)
                updated.pop("verification_command_mode", None)
            schedule_changed = "schedule" in updates
            inference_fields_changed = bool(
                {"provider", "model", "base_url", "no_agent"}.intersection(updates)
            ) and _normalized_inference_axes(updated) != previous_inference_axes

            if normalize_skills:
                normalized_skills = _normalize_skill_list(updated.get("skill"), updated.get("skills"))
                updated["skills"] = normalized_skills
                updated["skill"] = normalized_skills[0] if normalized_skills else None

            if schedule_changed:
                updated_schedule = updated["schedule"]
                # The API may pass schedule as a raw string (e.g. "every 10m")
                # instead of a pre-parsed dict.  Normalize it the same way
                # create_job() does so downstream code can call .get() safely.
                if isinstance(updated_schedule, str):
                    updated_schedule = parse_schedule(updated_schedule)
                    updated["schedule"] = updated_schedule
                updated["schedule_display"] = updates.get(
                    "schedule_display",
                    updated_schedule.get("display", updated.get("schedule_display")),
                )
                if updated.get("state") != "paused":
                    updated_next_run = compute_next_run(updated_schedule)
                    # Same guard as create_job: an UPDATE that sets a one-shot
                    # to a time >ONESHOT_GRACE_SECONDS in the past would store
                    # next_run_at=None with state="scheduled", re-creating the
                    # ghost job that never fires (#59395). Reject it here too so
                    # the bug can't re-enter through the update door.
                    if (
                        updated_next_run is None
                        and updated_schedule.get("kind") == "once"
                    ):
                        run_at = updated_schedule.get("run_at") or updated_schedule
                        logger.warning(
                            "Rejecting one-shot cron job update '%s': run_at %s "
                            "is outside the %ss grace window",
                            updated.get("name", job_id),
                            run_at,
                            ONESHOT_GRACE_SECONDS,
                        )
                        raise ValueError(
                            f"Requested one-shot time {run_at} is more than "
                            f"{ONESHOT_GRACE_SECONDS}s in the past and cannot be scheduled."
                        )
                    updated["next_run_at"] = updated_next_run

            if inference_fields_changed:
                provider_snapshot, model_snapshot = _compute_provider_model_snapshots(
                    provider=updated.get("provider"),
                    model=updated.get("model"),
                    base_url=updated.get("base_url"),
                    no_agent=updated.get("no_agent"),
                )
                updated["provider_snapshot"] = provider_snapshot
                updated["model_snapshot"] = model_snapshot

            if (
                not special_operation
                and updated.get("enabled", True)
                and updated.get("state") != "paused"
                and not updated.get("next_run_at")
            ):
                next_run = compute_next_run(updated["schedule"])
                if next_run is None and updated["schedule"].get("kind") == "once":
                    run_at = updated["schedule"].get("run_at", "unknown")
                    raise ValueError(
                        f"Requested one-shot time {run_at} is in the past "
                        f"(grace window: {ONESHOT_GRACE_SECONDS}s) and cannot be scheduled."
                    )
                updated["next_run_at"] = next_run

            if governance_refresh or _cron_governance_material_changed(job, updated):
                if job.get(_CRON_RUN_OUTCOME_CLAIM_FIELD) is not None:
                    raise CronJobGovernanceError(
                        "Cron job authorization cannot change while a signed run is active."
                    )
                if (
                    job.get("creation_governance_receipt") is not None
                    and not governance_expected
                ):
                    raise CronJobGovernanceError(
                        "Cron job authorization cannot change while governance is unavailable."
                    )
                updated, disposition = _apply_cron_creation_governance("update", updated, jobs)
                if disposition != "allow_write":
                    return _normalize_job_record(updated)

            jobs[i] = updated
            _save_jobs_unlocked(jobs)
            return _normalize_job_record(jobs[i])
    return None


def pause_job(job_id: str, reason: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Pause a job without deleting it. Accepts a job ID or name."""
    job = resolve_job_ref(job_id)
    if not job:
        return None
    return update_job(
        job["id"],
        {
            "enabled": False,
            "state": "paused",
            "paused_at": _hermes_now().isoformat(),
            "paused_reason": reason,
        },
    )


def resume_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Resume a paused job and compute the next future run from now. Accepts a job ID or name."""
    job = resolve_job_ref(job_id)
    if not job:
        return None

    next_run_at = compute_next_run(job["schedule"])
    if next_run_at is None and job["schedule"].get("kind") == "once":
        run_at = job["schedule"].get("run_at", "unknown")
        raise ValueError(
            f"Cannot resume: one-shot time {run_at} is in the past "
            f"(grace window: {ONESHOT_GRACE_SECONDS}s) and will never fire."
        )
    return update_job(
        job["id"],
        {
            "enabled": True,
            "state": "scheduled",
            "paused_at": None,
            "paused_reason": None,
            "next_run_at": next_run_at,
        },
    )


def trigger_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Schedule a job to run on the next scheduler tick. Accepts a job ID or name."""
    job = resolve_job_ref(job_id)
    if not job:
        return None
    return update_job(
        job["id"],
        {
            "enabled": True,
            "state": "scheduled",
            "paused_at": None,
            "paused_reason": None,
            "next_run_at": _hermes_now().isoformat(),
        },
    )


def remove_job(job_id: str) -> bool:
    """Remove a job by ID or name."""
    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs()
        job = next((item for item in jobs if item.get("id") == job_id), None)
        if job is None:
            name_matches = [
                item
                for item in jobs
                if str(item.get("name") or "").lower() == str(job_id or "").lower()
            ]
            if len(name_matches) > 1:
                raise AmbiguousJobReference(
                    job_id,
                    [_normalize_job_record(item) for item in name_matches],
                )
            job = name_matches[0] if name_matches else None
        if job is None:
            return False
        if job.get(_CRON_RUN_OUTCOME_CLAIM_FIELD) is not None:
            raise CronJobGovernanceError(
                "Cron job cannot be removed while a signed run is active."
            )
        canonical_id = job["id"]
        # Resolve the output dir BEFORE saving so a legacy unsafe ID (e.g.
        # left over from before the create-time guard) fails closed without
        # half-applying the removal.
        _job_output_dir(canonical_id)
        retained = [item for item in jobs if item["id"] != canonical_id]
        _save_jobs_unlocked(retained)
        # Clean up output directory to prevent orphaned dirs accumulating.
        _remove_pinned_job_output(canonical_id)
        return True


def mark_job_run(job_id: str, success: bool, error: Optional[str] = None,
                 delivery_error: Optional[str] = None,
                 runtime_admission_receipt: Optional[Dict[str, Any]] = None,
                 delivery_receipt: Optional[Dict[str, Any]] = None,
                 run_outcome_receipt: Optional[Dict[str, Any]] = None,
                 run_outcome_claim: Optional[Dict[str, Any]] = None):
    """
    Mark a job as having been run.
    
    Updates last_run_at, last_status, increments completed count,
    computes next_run_at, and auto-deletes if repeat limit reached.

    ``delivery_error`` is tracked separately from the agent error — a job
    can succeed (agent produced output) but fail delivery (platform down).
    ``runtime_admission_receipt`` records a fail-closed pre-execution decision
    without putting raw prompts, routes, or exception text in jobs.json.
    ``delivery_receipt`` records only aggregate terminal delivery proof; it
    never includes transport identifiers, targets, content, or raw errors.
    ``run_outcome_receipt`` binds the technical terminal state to the signed
    Job revision. ``run_outcome_claim`` is the pre-run CAS token that cleared
    stale proof before side effects. It is not a business-semantic judgment.
    """
    if runtime_admission_receipt is not None:
        runtime_admission_receipt = _validated_runtime_admission_receipt(
            runtime_admission_receipt
        )
    if delivery_receipt is not None:
        delivery_receipt = _validated_delivery_receipt(delivery_receipt)
    if run_outcome_receipt is not None:
        run_outcome_receipt = _validated_run_outcome_receipt(
            run_outcome_receipt
        )
    if run_outcome_claim is not None:
        run_outcome_claim = _validated_run_outcome_claim(run_outcome_claim)
    if run_outcome_receipt is not None and run_outcome_claim is None:
        raise ValueError("cron run outcome receipt requires its pre-run claim")
    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs()
        for i, job in enumerate(jobs):
            if job["id"] == job_id:
                creation = job.get("creation_governance_receipt")
                if isinstance(creation, dict) and run_outcome_claim is None:
                    logger.warning(
                        "mark_job_run: signed job_id %s requires its active run outcome claim; skipping save",
                        job_id,
                    )
                    return False
                if run_outcome_claim is not None:
                    if (
                        not isinstance(creation, dict)
                        or creation.get("receipt_id") != run_outcome_claim.get("job_revision")
                        or job.get(_CRON_RUN_OUTCOME_CLAIM_FIELD) != run_outcome_claim
                        or not _run_outcome_claim_is_active(run_outcome_claim)
                    ):
                        logger.warning(
                            "mark_job_run: stale run outcome claim for job_id %s; skipping save",
                            job_id,
                        )
                        return False
                    if run_outcome_receipt is not None and any(
                        run_outcome_receipt.get(field) != run_outcome_claim.get(field)
                        for field in (
                            "profile_id",
                            "job_id",
                            "job_revision",
                            "run_id",
                            "implementation_hash",
                        )
                    ):
                        raise ValueError("cron run outcome receipt does not match its claim")
                    if run_outcome_receipt is not None:
                        expected_checkpoint = _cron_checkpoint_invariant_hash(
                            job,
                            run_outcome_claim,
                            success=success,
                        )
                        expected_delivery = _cron_delivery_receipt_hash(
                            run_outcome_claim["run_id"],
                            delivery_receipt,
                        )
                        if (
                            run_outcome_receipt.get("terminal_state")
                            != ("success" if success else "failed")
                            or expected_checkpoint != run_outcome_receipt.get("checkpoint_invariant_hash")
                            or expected_delivery != run_outcome_receipt.get("delivery_receipt_hash")
                        ):
                            raise ValueError("cron run outcome receipt does not match its claim")
                now = _hermes_now().isoformat()
                job["last_run_at"] = now
                job["last_status"] = "ok" if success else "error"
                job["last_error"] = error if not success else None
                # Track delivery failures separately — cleared on successful delivery
                job["last_delivery_error"] = delivery_error
                job[_CRON_DELIVERY_RECEIPT_FIELD] = (
                    copy.deepcopy(delivery_receipt)
                    if delivery_receipt is not None
                    else None
                )
                job[_CRON_RUN_OUTCOME_RECEIPT_FIELD] = (
                    copy.deepcopy(run_outcome_receipt)
                    if run_outcome_receipt is not None
                    else None
                )
                job[_CRON_RUN_OUTCOME_CLAIM_FIELD] = None
                if runtime_admission_receipt is None:
                    job[_CRON_RUNTIME_ADMISSION_RECEIPT_FIELD] = None
                else:
                    job[_CRON_RUNTIME_ADMISSION_RECEIPT_FIELD] = copy.deepcopy(
                        runtime_admission_receipt
                    )
                # Clear any external-fire claim so a re-armed recurring job can
                # be claimed again on its next fire (Phase 4C CAS).
                job["fire_claim"] = None
                # Clear the one-shot running-claim (#59229): the run is over, so
                # a re-armed recurring job or a re-dispatched one-shot recovery
                # is claimable again. No-op if the job never carried a claim.
                if job.get("run_claim") is not None:
                    job["run_claim"] = None
                
                # Increment completed count.  Finite one-shot jobs are
                # pre-claimed by claim_dispatch() BEFORE the side effect runs
                # (issue #38758), which already incremented completed — do not
                # double-count them here.  Recurring jobs and direct callers
                # with no pre-run claim still get the legacy increment.
                if job.get("repeat"):
                    repeat = job["repeat"]
                    times = repeat.get("times")
                    completed = repeat.get("completed", 0)
                    kind = job.get("schedule", {}).get("kind")
                    preclaimed_oneshot = (
                        kind == "once"
                        and times is not None
                        and times > 0
                        and completed > 0
                    )
                    if not preclaimed_oneshot:
                        completed += 1
                        repeat["completed"] = completed

                    # Check if we've hit the repeat limit
                    if times is not None and times > 0 and completed >= times:
                        # Remove the job (limit reached)
                        jobs.pop(i)
                        save_jobs(jobs)
                        return True
                
                # Compute next run
                job["next_run_at"] = compute_next_run(job["schedule"], now)

                # If no next run, decide whether this is terminal completion
                # (one-shot) or a transient failure (recurring schedule couldn't
                # compute — e.g. 'croniter' missing from the runtime env).
                # Recurring jobs must NEVER be silently disabled: that turns a
                # missing runtime dep into "job completed" and the user's
                # schedule quietly goes off. See issue #16265.
                if job["next_run_at"] is None:
                    kind = job.get("schedule", {}).get("kind")
                    if kind in {"cron", "interval"}:
                        job["state"] = "error"
                        if not job.get("last_error"):
                            job["last_error"] = (
                                "Failed to compute next run for recurring "
                                "schedule (is the 'croniter' package "
                                "installed in the gateway's Python env?)"
                            )
                        logger.error(
                            "Job '%s' (%s) could not compute next_run_at; "
                            "leaving enabled and marking state=error so the "
                            "job is not silently disabled.",
                            job.get("name", job.get("id", "?")),
                            kind,
                        )
                    else:
                        job["enabled"] = False
                        job["state"] = "completed"
                elif job.get("state") != "paused":
                    job["state"] = "scheduled"

                save_jobs(jobs)
                return True

        logger.warning("mark_job_run: job_id %s not found, skipping save", job_id)
        return False


def mark_job_delivery_recovered(job_id: str) -> bool:
    """Clear delivery health after a confirmed recovery without changing run state."""
    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs()
        for job in jobs:
            if job.get("id") != job_id:
                continue
            job["last_delivery_error"] = None
            job["last_delivery_recovered_at"] = _hermes_now().isoformat()
            _save_jobs_unlocked(jobs)
            return True
    logger.warning(
        "mark_job_delivery_recovered: job_id %s not found, skipping save",
        job_id,
    )
    return False


def claim_operational_notice_delivery(job_id: str, idempotency_key: str) -> Dict[str, Any]:
    """Durably claim one supplemental notice before its adapter send.

    The receipt intentionally provides at-most-once automatic delivery. A
    restart after a claimed but uncertain send is recorded for explicit
    operator recovery instead of guessing and duplicating an admin message.
    """
    key = str(idempotency_key or "").strip()
    if not key or len(key) > 512:
        return {"status": "invalid_key", "claimed": False}
    started = time.monotonic()
    # A duplicate admin diagnostic is still an external side effect.  This
    # receipt must not degrade to process-local protection under contention.
    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs()
        for job in jobs:
            if job.get("id") != job_id:
                continue
            receipts = job.get("operational_notice_receipts")
            receipts = dict(receipts) if isinstance(receipts, dict) else {}
            existing = receipts.get(key)
            if isinstance(existing, dict):
                return {"status": str(existing.get("status") or "claimed"), "claimed": False}
            if len(receipts) >= OPERATIONAL_NOTICE_RECEIPT_LIMIT:
                oldest = sorted(receipts, key=lambda item: str(receipts[item].get("updated_at") or ""))
                for stale_key in oldest[: len(receipts) - OPERATIONAL_NOTICE_RECEIPT_LIMIT + 1]:
                    receipts.pop(stale_key, None)
            now = _hermes_now().isoformat()
            receipts[key] = {
                "status": "claimed",
                "claimed_at": now,
                "updated_at": now,
                "caller": "cron_scheduler",
                "parameters": {"idempotency_key": key},
                "result": {"status": "claimed"},
                "duration_ms": int((time.monotonic() - started) * 1000),
            }
            job["operational_notice_receipts"] = receipts
            _save_jobs_unlocked(jobs)
            return {"status": "claimed", "claimed": True}
    return {"status": "job_not_found", "claimed": False}


def mark_operational_notice_delivery(job_id: str, idempotency_key: str, status: str) -> Dict[str, Any]:
    """Record a safe terminal result for an already claimed notice."""
    if status not in {"sent", "failed", "uncertain"}:
        return {"status": "invalid_status"}
    key = str(idempotency_key or "").strip()
    started = time.monotonic()
    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs()
        for job in jobs:
            if job.get("id") != job_id:
                continue
            receipts = job.get("operational_notice_receipts")
            if not isinstance(receipts, dict) or not isinstance(receipts.get(key), dict):
                return {"status": "not_claimed"}
            receipts[key] = {
                **receipts[key],
                "status": status,
                "updated_at": _hermes_now().isoformat(),
                "result": {"status": status},
                "duration_ms": int((time.monotonic() - started) * 1000),
            }
            job["operational_notice_receipts"] = receipts
            _save_jobs_unlocked(jobs)
            return {"status": status}
    return {"status": "job_not_found"}


def claim_dispatch(
    job_id: str,
    *,
    run_outcome_claim: Optional[Dict[str, Any]] = None,
) -> bool:
    """Atomically claim a finite one-shot job dispatch BEFORE execution.

    Increments ``repeat.completed`` under the cross-process jobs lock and
    persists the claim immediately, so that if the tick dies mid-execution
    (gateway kill, OOM, segfault, hard-timeout) the dispatch is not lost.
    This converts finite one-shot jobs from *at-least-once* to *at-most-times*
    semantics — a job that self-destructs fires at most ``repeat.times`` times
    instead of infinitely (issue #38758).

    Returns ``True`` if the caller may proceed to run the job, ``False`` if the
    dispatch limit is already reached (in which case the stale job is removed).

    Only claims jobs with ``schedule.kind == "once"`` and ``repeat.times > 0``.
    Recurring jobs (they use ``advance_next_run``) and infinite-repeat / no-repeat
    jobs are left unchanged and always allowed to proceed.
    """
    if run_outcome_claim is not None:
        run_outcome_claim = _validated_run_outcome_claim(run_outcome_claim)
    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs()
        for i, job in enumerate(jobs):
            if job["id"] != job_id:
                continue
            creation = job.get("creation_governance_receipt")
            if creation is not None and run_outcome_claim is None:
                logger.warning(
                    "claim_dispatch: signed job_id %s requires an exact run outcome claim; refusing dispatch",
                    job_id,
                )
                return False
            if run_outcome_claim is not None:
                if (
                    not isinstance(creation, dict)
                    or creation.get("receipt_id")
                    != run_outcome_claim.get("job_revision")
                    or job.get(_CRON_RUN_OUTCOME_CLAIM_FIELD)
                    != run_outcome_claim
                ):
                    logger.warning(
                        "claim_dispatch: stale run outcome claim for job_id %s; refusing dispatch",
                        job_id,
                    )
                    return False
            if job.get("schedule", {}).get("kind") != "once":
                return True  # recurring jobs use advance_next_run(), not dispatch claims
            repeat = job.get("repeat")
            if not repeat:
                return True  # no repeat limit — always dispatch
            times = repeat.get("times")
            if times is None or times <= 0:
                return True  # infinite — always dispatch
            completed = repeat.get("completed", 0)
            if completed >= times:
                # Already dispatched the max number of times (e.g. a prior
                # tick claimed then died before mark_job_run could remove it).
                # Clean up so it stops appearing as due on every tick.
                jobs.pop(i)
                save_jobs(jobs)
                logger.info(
                    "Job '%s': dispatch limit reached (%d/%d) — removing",
                    job.get("name", job.get("id", "?")),
                    completed,
                    times,
                )
                return False
            # Claim this dispatch before the side effect runs.
            repeat["completed"] = completed + 1
            save_jobs(jobs)
            logger.debug(
                "Job '%s': claimed dispatch %d/%d",
                job.get("name", job.get("id", "?")),
                repeat["completed"],
                times,
            )
            return True

        if run_outcome_claim is not None:
            logger.warning(
                "claim_dispatch: exact run outcome claim has no stored job_id %s; refusing dispatch",
                job_id,
            )
            return False
        logger.debug(
            "claim_dispatch: unsigned job_id %s not in store — proceeding "
            "(handed-in job dict; nothing to persist a claim against)",
            job_id,
        )
        return True


def heartbeat_run_claim(job_id: str, *, expected_owner: str) -> bool:
    """Refresh a one-shot's ``run_claim`` timestamp while its run is alive.

    Called periodically from the scheduler's run monitor (#62002) so a
    legitimately long run keeps its claim fresh: an expired claim then really
    does mean "the claiming process died", and neither another process's tick
    nor this process's own next tick will re-dispatch or stale-remove the job
    while the run is in flight. mark_job_run() clears the claim on completion.

    ``expected_owner`` is the stable owner copied from the dispatched job. The
    compare-and-refresh prevents a stale runner that resumes after a long sleep
    from extending a claim another scheduler process has since taken over.

    Returns True if this owner's one-shot claim was refreshed; False when the
    job, claim, or ownership no longer matches.
    """
    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs()
        for job in jobs:
            if job.get("id") != job_id:
                continue
            if job.get("schedule", {}).get("kind") != "once":
                return False
            claim = job.get("run_claim")
            if not isinstance(claim, dict) or claim.get("by") != expected_owner:
                return False
            claim["at"] = _hermes_now().isoformat()
            save_jobs(jobs)
            return True
    return False


def advance_next_run(job_id: str) -> bool:
    """Preemptively advance next_run_at for a recurring job before execution.

    Call this BEFORE run_job() so that if the process crashes mid-execution,
    the job won't re-fire on the next gateway restart.  This converts the
    scheduler from at-least-once to at-most-once for recurring jobs — missing
    one run is far better than firing dozens of times in a crash loop.

    One-shot jobs are left unchanged so they can still retry on restart.

    Returns True if next_run_at was advanced, False otherwise.
    """
    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs()
        for job in jobs:
            if job["id"] == job_id:
                kind = job.get("schedule", {}).get("kind")
                if kind not in {"cron", "interval"}:
                    return False
                now = _hermes_now().isoformat()
                new_next = compute_next_run(job["schedule"], now)
                if new_next and new_next != job.get("next_run_at"):
                    job["next_run_at"] = new_next
                    save_jobs(jobs)
                    return True
                return False
        return False


def _machine_id() -> str:
    """Stable-ish identifier for claim attribution/debugging (NOT correctness).

    Uses ``HERMES_MACHINE_ID`` if set, else hostname + pid. The CAS correctness
    comes from the file lock + the fresh-claim check, not from this value.
    """
    explicit = os.getenv("HERMES_MACHINE_ID", "").strip()
    if explicit:
        return explicit
    try:
        import socket
        host = socket.gethostname()
    except Exception:
        host = "unknown"
    return f"{host}:{os.getpid()}"


def claim_job_for_fire(job_id: str, *, claim_ttl_seconds: int = 300) -> bool:
    """Atomically claim a job for a single external 'fire' (multi-machine
    at-most-once). Returns True iff THIS caller won the claim.

    Used by the external-provider fire path (``CronScheduler.fire_due``) when an
    external scheduler (Chronos) signals a job is due across N gateway replicas:
    exactly one wins. Single-machine deployments always win.

    Under the file lock: reject if the job is missing/disabled/paused. If a
    fresh claim (younger than ``claim_ttl_seconds``) already exists, lose.
    Otherwise stamp a ``fire_claim`` and, for recurring jobs, advance
    ``next_run_at`` (mirrors ``advance_next_run``'s at-most-once bump so a stale
    re-delivery for the old time can't re-fire). One-shots keep ``next_run_at``
    but the fresh ``fire_claim`` blocks a duplicate retry for the same fire.
    ``mark_job_run`` clears the claim on completion so a re-armed recurring job
    is claimable again next fire.

    The stale-claim TTL means a machine that crashed after claiming but before
    completing doesn't wedge the job forever — after the TTL another fire can
    reclaim it.
    """
    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs()
        for job in jobs:
            if job["id"] != job_id:
                continue
            if not job.get("enabled", True) or job.get("state") == "paused":
                return False
            now = _hermes_now()
            existing = job.get("fire_claim")
            if existing:
                try:
                    claimed_at = _ensure_aware(datetime.fromisoformat(existing["at"]))
                    # Bounded on BOTH sides (#60703): a claim stamped in the
                    # future (clock/TZ skew across a restart, or a corrupted
                    # timestamp) would otherwise have a negative age and stay
                    # "fresh" forever — the job becomes permanently unfireable
                    # and every manual `cron run` reports "already being
                    # fired". Treat future-dated claims as stale/overwritable.
                    _age = (now - claimed_at).total_seconds()
                    if 0 <= _age < claim_ttl_seconds:
                        return False  # someone holds a fresh claim
                except Exception:
                    pass  # malformed claim → overwrite
            job["fire_claim"] = {"at": now.isoformat(), "by": _machine_id()}
            kind = job.get("schedule", {}).get("kind")
            if kind in {"cron", "interval"}:
                nxt = compute_next_run(job["schedule"], now.isoformat())
                if nxt:
                    job["next_run_at"] = nxt
            save_jobs(jobs)
            return True
        return False


def get_due_jobs() -> List[Dict[str, Any]]:
    """Get all jobs that are due to run now.

    For recurring jobs (cron/interval), if the scheduled time is stale (more
    than one period in the past, e.g. because the gateway was down OR because a
    long-running previous execution overran the interval), the accumulated
    missed runs are collapsed — ``next_run_at`` is fast-forwarded to the next
    future occurrence so a backlog does NOT burst-fire on restart — but the job
    still fires ONCE now. This prevents the perpetual-defer loop (#33315) where
    a job whose runtime exceeds ``interval + grace`` would be skipped forever.

    Note: firing once on catch-up flows through ``mark_job_run``, so a job with
    a ``repeat.times`` limit consumes one of its runs on that catch-up fire.
    """
    with _jobs_lock(require_cross_process=True):
        return _get_due_jobs_locked()


def _get_due_jobs_locked() -> List[Dict[str, Any]]:
    """Inner implementation of get_due_jobs(); must be called with _jobs_lock held."""
    now = _hermes_now()
    raw_jobs = load_jobs()
    needs_save = False

    # Repair id-less records BEFORE anything keys off ``job["id"]``. A direct
    # jobs.json edit that bypassed add_job() can leave a record without an "id"
    # (older writers used "job_id"). Every downstream site — the logging
    # helpers and the ``for rj in raw_jobs: if rj["id"] == job["id"]``
    # persistence loops — indexes job["id"] eagerly, so a single malformed
    # record raised KeyError mid-tick, aborting the whole scan before
    # save_jobs() ran. That froze the entire profile's scheduler in a
    # per-minute fast-forward loop (healthy jobs recomputed in memory, then
    # discarded when the exception unwound). Recover the id from the drifted
    # "job_id" key when present, else synthesize one, and persist.
    for rj in raw_jobs:
        if not rj.get("id"):
            rj["id"] = rj.pop("job_id", None) or uuid.uuid4().hex[:12]
            needs_save = True

    jobs = [_apply_skill_fields(j) for j in copy.deepcopy(raw_jobs)]
    due = []

    # Normalize malformed "schedule" records (direct jobs.json edit, old writers,
    # corruption, etc.). "schedule" must be a dict; a null/string/etc. value
    # makes `schedule.get("kind")` or direct `schedule["kind"]` / ["expr"] /
    # ["minutes"] later raise and abort the entire scan *before* save_jobs().
    # Healthy jobs then lose their fast-forwarded next_run_at (exactly the
    # failure mode of the id-less job bug fixed above). Repair early at the
    # source so the rest of the tick can proceed and persist progress for
    # siblings.
    for j in jobs:
        if not isinstance(j.get("schedule"), dict):
            j["schedule"] = {}
            needs_save = True
    for rj in raw_jobs:
        if not isinstance(rj.get("schedule"), dict):
            rj["schedule"] = {}
            needs_save = True

    # Normalize malformed "next_run_at" records (direct jobs.json edit,
    # corruption, migration, or buggy writer). If present but not a valid
    # ISO string, datetime.fromisoformat(next_run) later raises and aborts
    # the entire scan *before* save_jobs(). Healthy siblings then lose any
    # fast-forwarded next_run_at (same class of bug as bad "id" or "schedule").
    # Strip the bad value so the existing "no next_run_at" recovery path
    # recomputes a sane value and persists it for this job.
    for j in jobs:
        nr = j.get("next_run_at")
        if nr is not None:
            if not isinstance(nr, str):
                j.pop("next_run_at", None)
                needs_save = True
            else:
                try:
                    datetime.fromisoformat(nr)
                except Exception:
                    j.pop("next_run_at", None)
                    needs_save = True
    for rj in raw_jobs:
        nr = rj.get("next_run_at")
        if nr is not None:
            if not isinstance(nr, str):
                rj.pop("next_run_at", None)
                needs_save = True
            else:
                try:
                    datetime.fromisoformat(nr)
                except Exception:
                    rj.pop("next_run_at", None)
                    needs_save = True

    # Same treatment for last_run_at (used as base in recovery / compute_next_run).
    for j in jobs:
        lr = j.get("last_run_at")
        if lr is not None and not isinstance(lr, str):
            j.pop("last_run_at", None)
            needs_save = True
        elif isinstance(lr, str):
            try:
                datetime.fromisoformat(lr)
            except Exception:
                j.pop("last_run_at", None)
                needs_save = True
    for rj in raw_jobs:
        lr = rj.get("last_run_at")
        if lr is not None and not isinstance(lr, str):
            rj.pop("last_run_at", None)
            needs_save = True
        elif isinstance(lr, str):
            try:
                datetime.fromisoformat(lr)
            except Exception:
                rj.pop("last_run_at", None)
                needs_save = True

    # Resolve the one-shot running-claim stale-recovery TTL once per scan
    # (derived from HERMES_CRON_TIMEOUT). See _oneshot_run_claim_ttl_seconds.
    _run_claim_ttl = _oneshot_run_claim_ttl_seconds()

    for job in jobs:
        # Per-job containment (structural guard): one malformed or
        # unexpected job record must never abort the whole scan. The id /
        # schedule / timestamp normalizations above repair the known shapes;
        # this guard catches every FUTURE variant, degrading to "skip this
        # job this tick" so healthy siblings still run and their recovered
        # state still reaches save_jobs() below.
        try:
            if not job.get("enabled", True):
                continue
            active_outcome_claim = job.get(_CRON_RUN_OUTCOME_CLAIM_FIELD)
            if active_outcome_claim is not None:
                try:
                    if _run_outcome_claim_is_active(active_outcome_claim):
                        continue
                except ValueError:
                    # Invalid durable evidence belongs to the existing HAK
                    # repair path and must never be dispatched automatically.
                    continue

            # Cross-process running-claim guard (#59229): if another scheduler
            # process already claimed this one-shot and its run is still in flight
            # (claim younger than the TTL), skip it — do NOT re-dispatch. The
            # claim is stamped just before we return the job as due (below) and
            # cleared by mark_job_run() on completion. A claim older than the TTL
            # is treated as stale (the claiming tick died mid-run) and allowed
            # through so the job is recovered rather than wedged forever.
            existing_claim = job.get("run_claim")
            if existing_claim and job.get("schedule", {}).get("kind") == "once":
                try:
                    claimed_at = _ensure_aware(
                        datetime.fromisoformat(existing_claim["at"])
                    )
                    # 0 <= age: a future-dated claim (clock/TZ skew across a
                    # restart) must be treated as stale, not eternally fresh,
                    # or the one-shot is skipped forever (#60703).
                    _age = (now - claimed_at).total_seconds()
                    if 0 <= _age < _run_claim_ttl:
                        continue  # a fresh claim is held by an in-flight run
                except (KeyError, ValueError, TypeError):
                    pass  # malformed claim → fall through and (re)claim

            next_run = job.get("next_run_at")
            if not next_run:
                schedule = job.get("schedule", {})
                kind = schedule.get("kind")

                # One-shot jobs use a small grace window via the dedicated helper.
                recovered_next = _recoverable_oneshot_run_at(
                    schedule,
                    now,
                    last_run_at=job.get("last_run_at"),
                )
                recovery_kind = "one-shot" if recovered_next else None

                # Recurring jobs reach here only when something — typically a
                # direct jobs.json edit that bypassed add_job() — left
                # next_run_at unset.  Without this branch, such jobs are
                # silently skipped forever; recompute next_run_at from the
                # schedule so they pick up at their next scheduled tick.
                if not recovered_next and kind in {"cron", "interval"}:
                    recovered_next = compute_next_run(schedule, now.isoformat())
                    if recovered_next:
                        recovery_kind = kind

                if not recovered_next:
                    continue

                job["next_run_at"] = recovered_next
                next_run = recovered_next
                logger.info(
                    "Job '%s' had no next_run_at; recovering %s run at %s",
                    job.get("name", job.get("id", "?")),
                    recovery_kind,
                    recovered_next,
                )
                for rj in raw_jobs:
                    if rj["id"] == job["id"]:
                        rj["next_run_at"] = recovered_next
                        needs_save = True
                        break

            raw_next_run_dt = datetime.fromisoformat(next_run)
            schedule = job.get("schedule", {})
            kind = schedule.get("kind")

            next_run_dt = _ensure_aware(raw_next_run_dt)
            # Migration repair: a cron job persists next_run_at as an absolute
            # instant, but the cron expr describes local wall-clock intent. If the
            # configured/system timezone changed after persistence, the stored
            # instant's offset no longer matches now's, and its converted time can
            # look due hours early (21:00+10 -> 13:00+02). When the stored *wall
            # clock* is still in the future, recompute from the schedule so we fire
            # at the intended local time instead of early-then-again.
            #
            # TRADE-OFF: this cannot distinguish a config/host TZ migration from a
            # legitimate DST offset change. A DST boundary that satisfies all four
            # conditions will recompute (and thus SKIP the pending occurrence, no
            # catch-up) rather than fire it. Accepted: in the pure-migration case
            # the recompute lands on the same wall-clock time later the same period,
            # and DST-boundary collisions with a still-future stored wall clock are
            # rare relative to the double-fire bug this prevents (#28934).
            if (
                kind == "cron"
                and next_run_dt <= now
                and _timezone_offset_mismatch(raw_next_run_dt, now)
                and _stored_wall_clock_is_future(raw_next_run_dt, now)
            ):
                new_next = compute_next_run(schedule, now.isoformat())
                if new_next:
                    logger.info(
                        "Job '%s' next_run_at offset changed (%s -> %s). "
                        "Recomputing cron run to preserve local wall-clock intent: %s",
                        job.get("name", job.get("id", "?")),
                        raw_next_run_dt.utcoffset(),
                        now.utcoffset(),
                        new_next,
                    )
                    for rj in raw_jobs:
                        if rj["id"] == job["id"]:
                            rj["next_run_at"] = new_next
                            needs_save = True
                            break
                    continue

            if next_run_dt <= now:

                # For recurring jobs, check if the scheduled time is stale
                # (gateway was down and missed the window). Fast-forward to
                # the next future occurrence instead of firing a stale run.
                grace = _compute_grace_seconds(schedule)
                if kind in {"cron", "interval"} and (now - next_run_dt).total_seconds() > grace:
                    # Job is past its catch-up grace window — skip accumulated
                    # missed runs but still execute once now to avoid deferring
                    # indefinitely (e.g. a long-running job just finished).
                    new_next = compute_next_run(schedule, now.isoformat())
                    if new_next:
                        logger.info(
                            "Job '%s' missed its scheduled time (%s, grace=%ds). "
                            "Running now; next run provisionally set to: %s "
                            "(re-anchored on completion)",
                            job.get("name", job.get("id", "?")),
                            next_run,
                            grace,
                            new_next,
                        )
                        # Persist the fast-forward to storage now (skip accumulated
                        # slots). In the built-in ticker path this is shortly
                        # overwritten by advance_next_run + mark_job_run, but it is
                        # NOT redundant: it (a) protects the crash window between
                        # here and mark_job_run, and (b) covers the external
                        # fire_due provider path, which does not call
                        # advance_next_run. mark_job_run re-anchors next_run_at off
                        # the actual completion time, so this value is provisional.
                        for rj in raw_jobs:
                            if rj["id"] == job["id"]:
                                rj["next_run_at"] = new_next
                                needs_save = True
                                break
                        # Fall through to due.append(job) — execute once now

                # One-shot dispatch-limit guard (issue #38758): a finite one-shot
                # claimed via claim_dispatch() but whose tick died before
                # mark_job_run could remove it will have completed >= times while
                # still looking due (last_run_at was never written, so the
                # recovery helper re-armed it). Remove it instead of re-firing.
                if kind == "once":
                    repeat = job.get("repeat")
                    if repeat:
                        times = repeat.get("times")
                        completed = repeat.get("completed", 0)
                        if times is not None and times > 0 and completed >= times:
                            # A live run must never have its job record deleted
                            # underneath it (#62002): a run that outlives the
                            # run_claim TTL (stream stall, laptop asleep
                            # mid-run) satisfies the same completed >= times +
                            # expired-claim condition as a dead tick, but
                            # mark_job_run() still needs the record to land
                            # last_run_at / last_status / last_delivery_error.
                            # If this process is still running the job, it is
                            # slow, not stale — keep the entry and skip.
                            if _job_running_in_this_process(job.get("id", "")):
                                logger.info(
                                    "Job '%s': dispatch limit reached (%d/%d) "
                                    "but its run is still in flight in this "
                                    "process — keeping entry",
                                    job.get("name", job.get("id", "?")),
                                    completed,
                                    times,
                                )
                                continue
                            logger.info(
                                "Job '%s': one-shot dispatch limit reached (%d/%d) "
                                "— removing stale due entry",
                                job.get("name", job.get("id", "?")),
                                completed,
                                times,
                            )
                            for rj in raw_jobs:
                                if rj["id"] == job["id"]:
                                    raw_jobs.remove(rj)
                                    needs_save = True
                                    break
                            continue

                # Durably claim a one-shot for the DURATION of its run before
                # returning it as due, so a second scheduler process (gateway +
                # desktop both run in-process 60s tickers on one HERMES_HOME)
                # cannot re-dispatch it while the first run is still in flight
                # (#59229). A plain one-shot's due-state is not resolved until
                # mark_job_run() completes it minutes later, so advancing
                # next_run_at by a fixed window is not enough — a job that outlives
                # one tick (e.g. a 2.5-min research prompt) would simply re-fire on
                # the next tick after the window. Instead we stamp a run_claim under
                # the same lock get_due_jobs already holds; the other process reads
                # a fresh claim on its next tick and skips (handled at the top of
                # this loop). mark_job_run() clears the claim on completion. The TTL
                # is only a safety valve: a claiming tick that DIES mid-run leaves a
                # stale claim that expires after the resolved run-claim TTL
                # (_oneshot_run_claim_ttl_seconds, derived from HERMES_CRON_TIMEOUT),
                # so the job is re-dispatched rather than wedged forever.
                if kind == "once":
                    claim = {"at": now.isoformat(), "by": _machine_id()}
                    job["run_claim"] = claim
                    for rj in raw_jobs:
                        if rj["id"] == job["id"]:
                            rj["run_claim"] = claim
                            needs_save = True
                            break

                due.append(job)
        except Exception:
            logger.exception(
                "Skipping malformed cron job %r during due scan",
                job.get("name") or job.get("id") or "?",
            )
            continue

    if needs_save:
        save_jobs(raw_jobs)

    return due


# Per-run cron output (`cron/output/<job>/<timestamp>.md`) is written once per
# execution. Unlike the quick-snapshot store (`hermes_cli.backup`, capped at 20)
# it had no retention, so a frequently-scheduled job on a long-running deploy
# accumulated one file per run forever and could fill the disk (#52383). Keep the
# most recent N files per job; a non-positive value disables pruning (opt-out).
_CRON_OUTPUT_DEFAULT_KEEP = 50


def _cron_output_keep() -> int:
    """Resolve the per-job output-file retention cap from config (``cron.output_retention``)."""
    try:
        from hermes_cli.config import load_config
        cfg = load_config() or {}
        cron_cfg = cfg.get("cron", {}) if isinstance(cfg, dict) else {}
        return int(cron_cfg.get("output_retention", _CRON_OUTPUT_DEFAULT_KEEP))
    except Exception:
        return _CRON_OUTPUT_DEFAULT_KEEP


def _prune_job_output(job_output_dir: Path, keep: int) -> int:
    """Remove the oldest ``*.md`` run-output files beyond *keep*. Returns count deleted.

    Mirrors the quick-snapshot retention in ``hermes_cli.backup._prune_quick_snapshots``:
    output filenames are timestamp-based (``%Y-%m-%d_%H-%M-%S.md``) so a reverse
    lexical sort orders newest-first, and everything past *keep* is the tail to
    drop. A non-positive *keep* disables pruning. Pruning failures are swallowed
    so they can never break output saving.
    """
    if keep <= 0:
        return 0
    try:
        files = sorted(
            (f for f in job_output_dir.glob("*.md") if f.is_file()),
            key=lambda f: f.name,
            reverse=True,
        )
    except OSError:
        return 0
    deleted = 0
    for stale in files[keep:]:
        try:
            stale.unlink()
            deleted += 1
        except OSError as exc:
            logger.debug("Failed to prune cron output %s: %s", stale.name, exc)
    return deleted


def save_job_output(job_id: str, output: str):
    """Save job output to file."""
    job_name = _job_output_dir(job_id).name
    timestamp = _hermes_now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{timestamp}.md"
    cron_fd = -1
    output_fd = -1
    job_fd = -1
    try:
        cron_fd, output_fd = _open_pinned_cron_output_dir()
        job_fd = _open_pinned_job_output_dir(output_fd, job_name)
        _write_pinned_job_output(job_fd, filename, output)
        _prune_pinned_job_output(job_fd, _cron_output_keep())
        _validate_pinned_cron_path(cron_fd)
        return _current_cron_store().output_dir / job_name / filename
    finally:
        if job_fd >= 0:
            os.close(job_fd)
        if output_fd >= 0:
            os.close(output_fd)
        if cron_fd >= 0:
            os.close(cron_fd)


# =============================================================================
# Skill reference rewriting (curator integration)
# =============================================================================

def referenced_skill_names() -> Set[str]:
    """Return the set of skill names referenced by ANY cron job.

    Includes paused and disabled jobs deliberately: a paused job never
    fires, so its skills never get a ``bump_use`` from the scheduler, yet
    resuming it must still find its skills present. The curator uses this
    set to protect referenced skills from inactivity archival — a skill a
    live job depends on is "in use" regardless of when it was last loaded.

    Best-effort: a corrupt/unreadable jobs store returns an empty set
    rather than raising, so a cron issue can never break the curator.
    """
    try:
        jobs = load_jobs()
    except Exception:
        logger.debug("referenced_skill_names: failed to load cron jobs", exc_info=True)
        return set()

    names: Set[str] = set()
    for job in jobs:
        if not isinstance(job, dict):
            continue
        for name in _normalize_skill_list(job.get("skill"), job.get("skills")):
            cleaned = str(name).strip().lstrip("/")
            if cleaned:
                names.add(cleaned)
    return names


def rewrite_skill_refs(
    consolidated: Optional[Dict[str, str]] = None,
    pruned: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Rewrite cron job skill references after a curator consolidation pass.

    When the curator consolidates a skill X into umbrella Y (or archives X
    as pruned), any cron job that lists ``X`` in its ``skills`` field will
    fail to load ``X`` at run time — the scheduler logs a warning and
    skips the skill, so the job runs without the instructions it was
    scheduled to follow. See cron/scheduler.py where ``skill_view`` is
    called per skill name.

    This function repairs cron jobs in-place:

    - A skill listed in ``consolidated`` is replaced with its umbrella
      target (the ``into`` value). If the umbrella is already in the
      job's skill list, the stale name is dropped without duplication.
    - A skill listed in ``pruned`` is dropped outright — there is no
      forwarding target.
    - Ordering and other skills in the list are preserved.
    - The legacy ``skill`` field is realigned via ``_apply_skill_fields``.

    Args:
        consolidated: mapping of ``old_skill_name -> umbrella_skill_name``.
        pruned: list of skill names that were archived with no forwarding
            target.

    Returns a report dict::

        {
            "rewrites": [
                {
                    "job_id": ...,
                    "job_name": ...,
                    "before": [...],
                    "after": [...],
                    "mapped": {"old": "new", ...},
                    "dropped": ["old", ...],
                },
                ...
            ],
            "jobs_updated": N,
            "jobs_scanned": M,
        }

    Best-effort: exceptions from loading/saving propagate to the caller so
    tests can assert behaviour; the curator invocation site wraps this
    call in a try/except so a failure here never breaks the curator.
    """
    consolidated = dict(consolidated or {})
    pruned_set = set(pruned or [])
    # A skill listed in both wins as "consolidated" — it has a target,
    # which is the more useful of the two outcomes.
    pruned_set -= set(consolidated.keys())

    if not consolidated and not pruned_set:
        return {"rewrites": [], "jobs_updated": 0, "jobs_scanned": 0}

    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs()
        rewrites: List[Dict[str, Any]] = []
        changed = False

        for job in jobs:
            skills_before = _normalize_skill_list(job.get("skill"), job.get("skills"))
            if not skills_before:
                continue

            mapped: Dict[str, str] = {}
            dropped: List[str] = []
            new_skills: List[str] = []

            for name in skills_before:
                if name in consolidated:
                    target = consolidated[name]
                    mapped[name] = target
                    if target and target not in new_skills:
                        new_skills.append(target)
                elif name in pruned_set:
                    dropped.append(name)
                elif name not in new_skills:
                    new_skills.append(name)

            if not mapped and not dropped:
                continue

            job["skills"] = new_skills
            job["skill"] = new_skills[0] if new_skills else None
            changed = True

            rewrites.append({
                "job_id": job.get("id"),
                "job_name": job.get("name") or job.get("id"),
                "before": list(skills_before),
                "after": list(new_skills),
                "mapped": mapped,
                "dropped": dropped,
            })

        if changed:
            save_jobs(jobs)
            logger.info(
                "Curator rewrote skill references in %d cron job(s)", len(rewrites)
            )

        return {
            "rewrites": rewrites,
            "jobs_updated": len(rewrites),
            "jobs_scanned": len(jobs),
        }
