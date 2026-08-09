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
import sys
import uuid

# Cross-process advisory file locking for jobs.json critical sections.
# fcntl is Unix-only; on Windows fall back to msvcrt. Either may be absent,
# in which case _jobs_lock() degrades to in-process locking only (the old
# behaviour) rather than failing.
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
from utils import atomic_replace, atomic_write_text

# ``croniter`` compiles ~15 ms of regexes at import and only matters for
# 5-field cron expressions. Resolve lazily; ``HAS_CRONITER`` stays a module
# attribute (tests monkeypatch it, and a monkeypatched value wins because
# ``_ensure_croniter`` only probes while it's still None).
croniter = None
HAS_CRONITER: Optional[bool] = None


def _ensure_croniter() -> bool:
    """Import croniter on first use; honor a pre-set HAS_CRONITER override."""
    global croniter, HAS_CRONITER
    if HAS_CRONITER is None:
        try:
            from croniter import croniter as _croniter
            croniter = _croniter
            HAS_CRONITER = True
        except ImportError:
            HAS_CRONITER = False
    return bool(HAS_CRONITER)

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
HERMES_DIR = get_hermes_home().resolve()
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
    cron_dir = Path(home).expanduser().resolve() / "cron"
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


def get_cron_profile_home() -> Path:
    """Return the canonical profile home owning the active cron store."""
    return _current_cron_store().cron_dir.parent.expanduser().resolve(strict=False)


# Fallback stale-recovery window for a one-shot's running-claim (#59229) when
# the cron inactivity timeout is disabled (HERMES_CRON_TIMEOUT=0 → unlimited),
# in which case no finite run bound exists to derive from. Also acts as the
# floor for the derived value so a very short configured timeout can't make the
# claim expire mid-run.
ONESHOT_RUN_CLAIM_TTL_SECONDS = 1800
OPERATIONAL_NOTICE_CLAIM_LEASE_SECONDS = 300

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
        logger.warning(
            "Cron running-set liveness check failed for job %r; keeping the "
            "entry to avoid deleting a possibly live one-shot run",
            job_id,
            exc_info=True,
        )
        return True


def _jobs_lock_file() -> Path:
    """Return the advisory lock path for the current cron directory."""
    return _current_cron_store().cron_dir / ".jobs.lock"


@contextlib.contextmanager
def _jobs_lock(*, require_cross_process: bool = False):
    """Serialize a load_jobs→modify→save_jobs critical section.

    Combines the in-process threading lock (cheap mutual exclusion between
    the gateway's parallel tick threads) with a cross-process advisory file
    lock on ``<cron dir>/.jobs.lock`` (mutual exclusion between the gateway process
    and standalone ``hermes`` CLI invocations, which previously shared no lock
    at all — a `cron pause` could be silently clobbered by a concurrent
    gateway write, leaving a "paused" job still firing).

    The flock is blocking, but every critical section that uses it is short
    (field updates only — no agent execution), so contention resolves in
    milliseconds. If neither fcntl nor msvcrt is available the manager still
    provides in-process locking, matching the historical behaviour.

    Nested calls in the same thread reuse the held lock so legacy callers that
    invoke save_jobs() inside a broader mutation section don't deadlock or try
    to reacquire the advisory file lock.
    """
    depth = getattr(_jobs_lock_state, "depth", 0)
    if depth:
        if require_cross_process and not getattr(
            _jobs_lock_state, "cross_process_acquired", False
        ):
            raise CronJobGovernanceError(
                "Cron job persistence needs administrator review "
                "(strict jobs lock unavailable)."
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
        try:
            try:
                ensure_dirs()
                lock_fd = open(_jobs_lock_file(), "a+", encoding="utf-8")
                lock_fd.seek(0)
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
                    # on timeout, log loudly and fall through to the same
                    # in-process-only degraded mode used when locking is
                    # unavailable.  A briefly-torn cross-process write is
                    # strictly better than a permanently dead scheduler.
                    _deadline = time.monotonic() + _JOBS_LOCK_TIMEOUT_SECONDS
                    while True:
                        try:
                            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                            _jobs_lock_state.cross_process_acquired = True
                            break
                        except (OSError, IOError):
                            if time.monotonic() >= _deadline:
                                if require_cross_process:
                                    raise TimeoutError(
                                        f"timed out waiting for strict cron jobs lock: {_jobs_lock_file()}"
                                    )
                                logger.error(
                                    "Timed out after %.0fs waiting for the cron "
                                    "jobs lock (%s) — another process is holding "
                                    "it. Proceeding with in-process locking only "
                                    "so the scheduler stays alive (#60703).",
                                    _JOBS_LOCK_TIMEOUT_SECONDS,
                                    _jobs_lock_file(),
                                )
                                try:
                                    lock_fd.close()
                                except OSError:
                                    pass
                                lock_fd = None
                                break
                            time.sleep(0.1)
                elif msvcrt is not None:
                    getattr(msvcrt, "locking")(lock_fd.fileno(), getattr(msvcrt, "LK_LOCK"), 1)
                    _jobs_lock_state.cross_process_acquired = True
                elif require_cross_process:
                    raise OSError("cross-process file locking is unavailable")
            except (OSError, IOError) as e:
                if require_cross_process:
                    raise CronJobGovernanceError(
                        "Cron job persistence needs administrator review "
                        "(strict jobs lock unavailable)."
                    ) from e
                # Never let a locking failure take down cron writes — fall back to
                # in-process-only protection (still held via _jobs_file_lock).
                logger.warning("jobs.json cross-process lock unavailable (%s); "
                               "proceeding with in-process lock only", e)
            try:
                yield
            finally:
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
        finally:
            _jobs_lock_state.depth = 0
            _jobs_lock_state.cross_process_acquired = False

# Fields on a cron job that must never change after creation. ``id`` is used
# as a filesystem path component under ``OUTPUT_DIR``; allowing it to be
# updated lets an unsafe value (``../escape``, absolute path, nested) leak
# into output writes/deletes.
_CRON_GOVERNANCE_HOOK_OWNED_FIELDS = frozenset({
    "operation",
    "process_charter_ref",
    "approval_evidence_ref",
    "read_scope_ref",
    "disclosure_policy_ref",
    "risk_tier",
    "implementation_path_evidence_ref",
    "actionable_output",
    "source_route",
    "join_keys",
    "creation_governance_receipt",
})
_IMMUTABLE_JOB_FIELDS = frozenset({
    "id",
    "skill_bindings",
    *_CRON_GOVERNANCE_HOOK_OWNED_FIELDS,
    "last_runtime_admission_receipt",
    "last_delivery_receipt",
    "last_run_outcome_receipt",
    "active_run_outcome_claim",
})
_CRON_GOVERNANCE_ENV = "HERMES_CRON_CREATION_GOVERNANCE_REQUIRED"
_CRON_GOVERNANCE_PLUGIN = "hck-tool-boundary"
_CRON_RESUME_SCHEMA = "cron-persist-resume/v1"
_CRON_RESUME_SCHEMA_V2 = "cron-persist-resume/v2"
_CRON_RECOVERY_SCHEMA = "cron-persist-recovery/v1"
_CRON_RECOVERY_REGISTRATION_SCHEMA = "cron-persist-recovery-registration/v1"
_CRON_RECOVERY_DISPATCH_ACK_SCHEMA = "cron-persist-recovery-dispatch-ack/v2"
_CRON_RECOVERY_DURABLE_CAS_SCHEMA = "cron-persist-recovery-durable-cas/v1"
_CRON_RESUME_RECEIPT_FIELD = "cron_persist_resume_receipt"
_CRON_RESUME_PACKAGE_FIELDS = frozenset({
    "schema_version",
    "operation",
    "job_id",
    "candidate_hash",
    "persist_spec_hash",
    "authorized_behavior_ref",
    "scope_immutable",
    "receipt",
    "job",
    "instruction",
})
_CRON_RESUME_RECEIPT_CORE_FIELDS = (
    "schema_version",
    "profile_id",
    "frame_id",
    "action_id",
    "pending_id",
    "operation",
    "candidate_hash",
    "persist_spec_hash",
    "cron_job_id",
    "behavior_id",
    "process_id",
    "approval_id",
    "admin_actor_uid",
    "prior_job_hash",
    "issued_at",
    "expires_at",
)
_CRON_RESUME_RECEIPT_V2_CORE_FIELDS = (
    *_CRON_RESUME_RECEIPT_CORE_FIELDS,
    "request_id",
    "request_hash",
    "source_route_hash",
    "profile_home_sha256",
)
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
    "fire_claim",
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
_CRON_GOVERNANCE_CALLER_BINDING_FIELDS = frozenset({
    "authorized_behavior_ref",
    "implementation_categories",
})


def _cron_governance_material(job: Dict[str, Any]) -> Dict[str, Any]:
    """Return definition material while excluding operational state."""
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
    return _cron_governance_material(before) != _cron_governance_material(after)


def _cron_candidate_requires_governance(candidate: Dict[str, Any]) -> bool:
    """Return whether a candidate claims or retains governance authority."""
    if _CRON_RESUME_RECEIPT_FIELD in candidate:
        return True
    if isinstance(candidate.get("creation_governance_receipt"), dict):
        return True
    return any(
        candidate.get(field) not in (None, "", [])
        for field in _CRON_GOVERNANCE_CALLER_BINDING_FIELDS
    )


def _cron_stable_hash(value: Any) -> str:
    body = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return f"sha256:{hashlib.sha256(body.encode('utf-8')).hexdigest()}"


def _cron_persist_spec_hash(operation: str, candidate: Dict[str, Any]) -> str:
    normalized = {
        key: copy.deepcopy(value)
        for key, value in candidate.items()
        if key != _CRON_RESUME_RECEIPT_FIELD
    }
    return _cron_stable_hash({"operation": operation, "job": normalized})


def _cron_request_material(candidate: Dict[str, Any]) -> Dict[str, Any]:
    """Return normalized user intent, excluding mutable and derived state."""
    fields = (
        "id",
        "name",
        "prompt",
        "skills",
        "skill",
        "model",
        "provider",
        "base_url",
        "script",
        "no_agent",
        "context_from",
        "schedule",
        "repeat",
        "deliver",
        "origin",
        "enabled_toolsets",
        "workdir",
        "attach_to_session",
        "authorized_behavior_ref",
        "implementation_categories",
        "source_route",
    )
    material = {
        field: copy.deepcopy(candidate.get(field))
        for field in fields
        if field in candidate
    }
    repeat = material.get("repeat")
    if isinstance(repeat, dict):
        material["repeat"] = {
            key: copy.deepcopy(value)
            for key, value in repeat.items()
            if key != "completed"
        }
    return material


def _cron_request_hash(candidate: Dict[str, Any]) -> str:
    return _cron_stable_hash(_cron_request_material(candidate))


def _cron_source_route_hash(candidate: Dict[str, Any]) -> str:
    return _cron_stable_hash({
        "source_route": copy.deepcopy(candidate.get("source_route")),
        "origin": copy.deepcopy(candidate.get("origin")),
        "deliver": copy.deepcopy(candidate.get("deliver")),
    })


def _cron_candidate_definition_hash(candidate: Dict[str, Any]) -> str:
    return _cron_stable_hash(_cron_governance_material(candidate))


def cron_persist_recovery_dispatch_key(
    recovery_id: str,
    issuer: Dict[str, Any],
    notification_effect: Dict[str, Any],
) -> str:
    """Bind one stable HAK/outbox idempotency key to recovery issuer and effect."""
    if (
        not str(recovery_id or "").strip()
        or not isinstance(issuer, dict)
        or set(issuer) != {"id", "version"}
        or not str(issuer.get("id") or "").strip()
        or not str(issuer.get("version") or "").strip()
        or not isinstance(notification_effect, dict)
    ):
        raise ValueError("invalid Cron recovery dispatch key material")
    return _cron_stable_hash({
        "schema_version": _CRON_RECOVERY_REGISTRATION_SCHEMA,
        "recovery_id": recovery_id,
        "issuer": copy.deepcopy(issuer),
        "effect_hash": _cron_stable_hash(notification_effect),
    })


def _active_cron_profile_identity() -> Dict[str, str]:
    """Derive profile name and canonical-home digest from the Cron store path."""
    home = _active_profile_home()
    try:
        resolved = home.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review "
            "(active profile is unavailable)."
        ) from exc
    if not resolved.is_dir():
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review "
            "(active profile is unavailable)."
        )
    profile_id = resolved.name if resolved.parent.name == "profiles" else "default"
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", profile_id):
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review "
            "(active profile identity is invalid)."
        )
    asserted = str(os.environ.get("HERMES_PROFILE_ID") or "").strip()
    if asserted and asserted != profile_id:
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review "
            "(active profile identity mismatch)."
        )
    return {
        "profile_id": profile_id,
        "profile_home_sha256": _cron_stable_hash(str(resolved)),
    }


def _active_cron_profile_id() -> str:
    return _active_cron_profile_identity()["profile_id"]


class CronCreationProfileBindingError(ValueError):
    """A signed Job cannot enter Group C without canonical-home authority."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.migration_action = "refresh_creation_governance_receipt"


def cron_creation_profile_identity(job: Dict[str, Any]) -> Dict[str, str]:
    """Read Profile authority from the persisted creation receipt and verify it."""
    receipt = job.get("creation_governance_receipt")
    job_id = str(job.get("id") or "")
    if not isinstance(receipt, dict):
        raise CronCreationProfileBindingError(
            "creation_receipt_missing",
            "cron creation governance receipt is required",
        )
    profile_id = str(receipt.get("profile_id") or "").strip()
    if (
        receipt.get("schema_version") != "cron-creation-governance/v1"
        or receipt.get("cron_job_id") != job_id
        or not _safe_run_outcome_identity(profile_id)
        or not re.fullmatch(
            r"sha256:[0-9a-f]{64}", str(receipt.get("receipt_id") or "")
        )
    ):
        raise CronCreationProfileBindingError(
            "creation_receipt_profile_binding_invalid",
            "invalid cron creation governance profile binding",
        )
    active = _active_cron_profile_identity()
    if profile_id != active["profile_id"]:
        raise CronCreationProfileBindingError(
            "creation_receipt_profile_mismatch",
            "cron creation governance profile does not match active Profile",
        )
    asserted_home = str(receipt.get("profile_home_sha256") or "").strip()
    if not asserted_home:
        raise CronCreationProfileBindingError(
            "creation_receipt_profile_home_migration_required",
            "legacy creation governance receipt must be refreshed with profile_home_sha256",
        )
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", asserted_home):
        raise CronCreationProfileBindingError(
            "creation_receipt_profile_home_invalid",
            "creation governance receipt has an invalid profile_home_sha256",
        )
    if asserted_home != active["profile_home_sha256"]:
        raise CronCreationProfileBindingError(
            "creation_receipt_profile_home_mismatch",
            "cron creation governance Profile home does not match active Profile",
        )
    return active


def cron_persist_resume_identity(
    operation: str,
    candidate: Dict[str, Any],
) -> Dict[str, str]:
    """Return the Core-owned v2 request and route identities for a candidate."""
    op = str(operation or "").strip().lower()
    if op not in {"create", "update"} or not isinstance(candidate, dict):
        raise ValueError("Cron persist resume identity requires create/update candidate")
    profile_identity = _active_cron_profile_identity()
    profile_id = profile_identity["profile_id"]
    profile_home_sha256 = profile_identity["profile_home_sha256"]
    request_hash = _cron_request_hash(candidate)
    source_route_hash = _cron_source_route_hash(candidate)
    request_id = _cron_stable_hash({
        "profile_id": profile_id,
        "profile_home_sha256": profile_home_sha256,
        "operation": op,
        "job_id": str(candidate.get("id") or "").strip(),
        "request_hash": request_hash,
    })
    return {
        "profile_id": profile_id,
        "profile_home_sha256": profile_home_sha256,
        "request_id": request_id,
        "request_hash": request_hash,
        "source_route_hash": source_route_hash,
    }


@dataclass(frozen=True)
class CronResumeResolution:
    """Typed classification of one structurally valid Cron resume package."""

    disposition: str
    candidate: Dict[str, Any]
    reason: str = ""
    recovery_context: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class _ParsedCronResume:
    schema_version: str
    operation: str
    job: Dict[str, Any]
    receipt: Dict[str, Any]


class CronJobGovernanceError(PermissionError):
    """Raised when a cron persistence candidate is not authorized."""

    def __init__(self, message: str, *, decision: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.decision = copy.deepcopy(decision or {})

    def payload(self) -> Dict[str, Any]:
        pending = self.decision.get("pending_action")
        payload = {
            "schema_version": "cron-admin-pending-action/v1",
            "action": "blocked",
            "reason": str(self.decision.get("reason") or "review_required"),
            "state": str(self.decision.get("state") or "review_required"),
            "pending_action": (
                copy.deepcopy(pending) if isinstance(pending, dict) else {}
            ),
        }
        failures = self.decision.get("callback_failures")
        if isinstance(failures, list):
            payload["callback_failures"] = [
                copy.deepcopy(item) for item in failures if isinstance(item, dict)
            ]
        recovery = self.decision.get("recovery")
        if isinstance(recovery, dict):
            payload["recovery"] = copy.deepcopy(recovery)
        return payload

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
    try:
        profile_id = cron_creation_profile_identity(job)["profile_id"]
    except (CronJobGovernanceError, ValueError):
        return None
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




def _cron_creation_governance_expected() -> bool:
    if str(os.environ.get(_CRON_GOVERNANCE_ENV, "")).strip().lower() in {
        "1", "true", "yes", "on",
    }:
        return True
    home = get_hermes_home()
    plugin_dir = home / "plugins" / _CRON_GOVERNANCE_PLUGIN
    managed_profile = (home / ".hermes-agent-kit").exists()
    if not (plugin_dir / "plugin.yaml").is_file():
        return managed_profile
    try:
        from hermes_cli.config import read_user_config_raw

        config_path = home / "config.yaml"
        config = (
            read_user_config_raw(config_path)
            if config_path.is_file()
            else {}
        )
        plugins = config.get("plugins") if isinstance(config, dict) else {}
        enabled = plugins.get("enabled") if isinstance(plugins, dict) else []
        disabled = plugins.get("disabled") if isinstance(plugins, dict) else []
        return (
            _CRON_GOVERNANCE_PLUGIN in (enabled or [])
            and _CRON_GOVERNANCE_PLUGIN not in (disabled or [])
        )
    except Exception:
        return managed_profile


def _cron_persist_governance_active(
    candidate: Optional[Dict[str, Any]] = None,
) -> bool:
    """Return whether writes need the strict cross-process lock."""
    expected = _cron_creation_governance_expected() or (
        isinstance(candidate, dict) and _cron_candidate_requires_governance(candidate)
    )
    try:
        from hermes_cli.plugins import discover_plugins, has_hook

        discover_plugins()
        return expected or has_hook("pre_cron_job_persist")
    except Exception:
        return expected


def _validated_recovery_blocker(
    blocked_decisions: List[Dict[str, Any]],
    recovery_context: Dict[str, Any],
) -> Dict[str, Any]:
    """Require one generic issuer-bound registration and one sealed effect."""
    if len(blocked_decisions) != 1:
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review "
            "(ambiguous resume recovery registration).",
            decision={
                "action": "block",
                "reason": "resume_recovery_registration_ambiguous",
                "state": "resume_review_required",
            },
        )
    blocked = copy.deepcopy(blocked_decisions[0])
    registration = blocked.get("recovery_registration")
    effect = blocked.get("notification_effect")
    pending = blocked.get("pending_action")
    frame = pending.get("frame") if isinstance(pending, dict) else None
    issuer = registration.get("issuer") if isinstance(registration, dict) else None
    expected_registration_fields = {
        "schema_version",
        "issuer",
        "recovery_id",
        "pending_id",
        "frame_id",
        "effect_hash",
        "dispatch_key",
    }
    valid = (
        isinstance(registration, dict)
        and set(registration) == expected_registration_fields
        and registration.get("schema_version")
        == _CRON_RECOVERY_REGISTRATION_SCHEMA
        and isinstance(issuer, dict)
        and set(issuer) == {"id", "version"}
        and bool(str(issuer.get("id") or "").strip())
        and bool(str(issuer.get("version") or "").strip())
        and registration.get("recovery_id") == recovery_context.get("recovery_id")
        and isinstance(pending, dict)
        and registration.get("pending_id") == pending.get("pending_id")
        and isinstance(frame, dict)
        and registration.get("frame_id") == frame.get("frame_id")
        and isinstance(effect, dict)
        and registration.get("effect_hash") == _cron_stable_hash(effect)
        and registration.get("dispatch_key")
        == cron_persist_recovery_dispatch_key(
            str(recovery_context.get("recovery_id") or ""),
            issuer,
            effect,
        )
        and "post_persist_effects" not in blocked
    )
    if not valid:
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review "
            "(invalid resume recovery registration).",
            decision={
                "action": "block",
                "reason": "resume_recovery_registration_invalid",
                "state": "resume_review_required",
            },
        )
    blocked["post_persist_effects"] = [copy.deepcopy(effect)]
    return blocked


def _apply_cron_persist_governance(
    operation: str,
    candidate: Dict[str, Any],
    existing_jobs: List[Dict[str, Any]],
    *,
    recovery_context: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], str]:
    """Run the generic persistence decision hook inside the jobs lock."""
    expected = (
        _cron_creation_governance_expected()
        or _cron_candidate_requires_governance(candidate)
    )
    try:
        from hermes_cli.plugins import discover_plugins, invoke_mandatory_hook

        discover_plugins()
        hook_kwargs = {
            "operation": operation,
            "candidate": copy.deepcopy(candidate),
            "existing_jobs": copy.deepcopy(existing_jobs),
        }
        if recovery_context is not None:
            hook_kwargs["recovery_context"] = copy.deepcopy(recovery_context)
        report = invoke_mandatory_hook("pre_cron_job_persist", **hook_kwargs)
    except Exception as exc:
        if not expected:
            logger.warning("pre_cron_job_persist discovery failed", exc_info=True)
            return candidate, "allow_write"
        decision = {
            "action": "block",
            "reason": "governance_unavailable",
            "state": "review_required",
            "callback_failures": [{
                "hook": "pre_cron_job_persist",
                "stage": "discovery",
                "exception_class": type(exc).__name__,
            }],
        }
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review (governance unavailable).",
            decision=decision,
        ) from exc

    results = report.get("results") if isinstance(report, dict) else []
    failures = report.get("failures") if isinstance(report, dict) else []
    callback_count = report.get("callback_count") if isinstance(report, dict) else 0
    results = results if isinstance(results, list) else []
    failures = failures if isinstance(failures, list) else []
    callback_count = callback_count if isinstance(callback_count, int) else 0
    decisions = [
        item for item in results
        if isinstance(item, dict) and item.get("action") in {"allow", "block"}
    ]
    blocked_decisions = [item for item in decisions if item.get("action") == "block"]
    if recovery_context is not None and failures:
        raise CronJobGovernanceError(
            "Cron job was not saved: governance_callback_failed (review_required).",
            decision={
                "action": "block",
                "reason": "governance_callback_failed",
                "state": "review_required",
                "callback_failures": copy.deepcopy(failures),
            },
        )
    if recovery_context is not None and blocked_decisions:
        blocked = _validated_recovery_blocker(blocked_decisions, recovery_context)
        reason = str(blocked.get("reason") or "review_required")
        state = str(blocked.get("state") or "review_required")
        raise CronJobGovernanceError(
            f"Cron job was not saved: {reason} ({state}).",
            decision=blocked,
        )
    post_effects: List[Dict[str, Any]] = []
    for blocked_decision in blocked_decisions:
        effect = blocked_decision.get("notification_effect")
        if isinstance(effect, dict) and effect not in post_effects:
            post_effects.append(copy.deepcopy(effect))

    if failures:
        blocked = {
            "action": "block",
            "reason": "governance_callback_failed",
            "state": "review_required",
            "callback_failures": copy.deepcopy(failures),
        }
        if post_effects:
            blocked["post_persist_effects"] = post_effects
        raise CronJobGovernanceError(
            "Cron job was not saved: governance_callback_failed (review_required).",
            decision=blocked,
        )

    if blocked_decisions:
        blocked = copy.deepcopy(blocked_decisions[0])
        if post_effects:
            blocked["post_persist_effects"] = post_effects
        reason = str(blocked.get("reason") or "review_required")
        state = str(blocked.get("state") or "review_required")
        raise CronJobGovernanceError(
            f"Cron job was not saved: {reason} ({state}).",
            decision=blocked,
        )

    allowed = [item for item in decisions if item.get("action") == "allow"]
    if not allowed and not expected and callback_count == 0:
        return candidate, "allow_write"
    if len(allowed) != 1:
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review "
            "(missing or ambiguous authorization)."
        )

    decision = allowed[0]
    disposition = str(decision.get("persist_disposition") or "allow_write")
    if disposition == "already_persisted":
        resume = candidate.get(_CRON_RESUME_RECEIPT_FIELD)
        resume_id = (
            str(resume.get("receipt_id") or "").strip()
            if isinstance(resume, dict)
            else ""
        )
        matching = [
            job for job in existing_jobs
            if str(job.get("id") or "") == str(candidate.get("id") or "")
            and isinstance(job.get("creation_governance_receipt"), dict)
            and str(
                job["creation_governance_receipt"].get("resume_receipt_id") or ""
            ) == resume_id
        ]
        if not resume_id or len(matching) != 1:
            raise CronJobGovernanceError(
                "Cron job persistence needs administrator review "
                "(invalid already-persisted result)."
            )
        return copy.deepcopy(matching[0]), "already_persisted"
    if disposition != "allow_write":
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review "
            "(invalid persistence disposition)."
        )

    patch = decision.get("job_patch")
    if not isinstance(patch, dict) or set(patch) - _CRON_GOVERNANCE_PATCH_FIELDS:
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review (invalid governance result)."
        )
    resume = candidate.get(_CRON_RESUME_RECEIPT_FIELD)
    resume_pause = {
        "enabled": False,
        "state": "paused",
        "paused_reason": "admin_authorized_pending_explicit_enable",
    }
    if resume is not None:
        if {key: patch.get(key) for key in resume_pause} != resume_pause:
            raise CronJobGovernanceError(
                "Cron job persistence needs administrator review "
                "(invalid resume pause result)."
            )
    elif set(patch).intersection(resume_pause):
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review "
            "(unexpected governance state patch)."
        )
    receipt = patch.get("creation_governance_receipt")
    if not isinstance(receipt, dict) or not str(receipt.get("receipt_id") or "").strip():
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review (missing governance receipt)."
        )
    profile_identity = _active_cron_profile_identity()
    if str(receipt.get("profile_id") or "").strip() != profile_identity["profile_id"]:
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review "
            "(governance receipt Profile mismatch)."
        )
    receipt = copy.deepcopy(receipt)
    asserted_home = str(receipt.get("profile_home_sha256") or "").strip()
    if asserted_home and asserted_home != profile_identity["profile_home_sha256"]:
        raise CronJobGovernanceError(
            "Cron job persistence needs administrator review "
            "(governance receipt Profile home mismatch)."
        )
    # Core owns the canonical filesystem boundary. Bind every newly authorized
    # receipt before persistence so Group C never has to infer a home from a
    # reusable profile name. Existing unbound receipts fail closed and require
    # an explicit governance refresh to pass through this migration point.
    receipt["profile_home_sha256"] = profile_identity["profile_home_sha256"]
    patch = {**patch, "creation_governance_receipt": receipt}
    if resume is not None:
        resume_id = (
            str(resume.get("receipt_id") or "").strip()
            if isinstance(resume, dict)
            else ""
        )
        if not resume_id or str(receipt.get("resume_receipt_id") or "").strip() != resume_id:
            raise CronJobGovernanceError(
                "Cron job persistence needs administrator review "
                "(resume receipt was not consumed)."
            )
    governed = {**candidate, **copy.deepcopy(patch)}
    governed.pop(_CRON_RESUME_RECEIPT_FIELD, None)
    return governed, "allow_write"


def _post_cron_persist_effects(
    error: CronJobGovernanceError,
) -> List[Dict[str, Any]]:
    """Extract opaque effects sealed into a denied decision."""
    effects = error.decision.get("post_persist_effects")
    if not isinstance(effects, list):
        return []
    return [copy.deepcopy(effect) for effect in effects if isinstance(effect, dict)]


class CronRuntimeAdmissionError(PermissionError):
    """Raised before any cron script or Agent side effect may begin."""

    def __init__(
        self,
        message: str,
        *,
        decision: Optional[Dict[str, Any]] = None,
        job: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.decision = copy.deepcopy(decision or {})
        self.receipt = _runtime_admission_receipt(
            job or {},
            reason_code=self.decision.get("reason"),
            state=self.decision.get("state"),
            exception_class="runtime_admission_blocked",
            retryable=bool(self.decision.get("retryable", False)),
        )


def _apply_cron_runtime_governance(job: Dict[str, Any]) -> None:
    """Run mandatory runtime admission before both cron execution paths."""
    required = (
        _cron_creation_governance_expected()
        or _cron_candidate_requires_governance(job)
    )
    try:
        from hermes_cli.plugins import discover_plugins, invoke_mandatory_hook

        discover_plugins()
        report = invoke_mandatory_hook(
            "pre_cron_job_run",
            job=copy.deepcopy(job),
        )
    except Exception as exc:
        if not required:
            logger.warning("pre_cron_job_run discovery failed", exc_info=True)
            return
        decision = {
            "action": "block",
            "reason": "runtime_governance_unavailable",
            "state": "runtime_review_required",
            "callback_failures": [{
                "hook": "pre_cron_job_run",
                "stage": "discovery",
                "exception_class": type(exc).__name__,
            }],
        }
        raise CronRuntimeAdmissionError(
            "Cron job was not run: runtime_governance_unavailable.",
            decision=decision,
            job=job,
        ) from exc

    results = report.get("results") if isinstance(report, dict) else []
    failures = report.get("failures") if isinstance(report, dict) else []
    callback_count = report.get("callback_count") if isinstance(report, dict) else 0
    results = results if isinstance(results, list) else []
    failures = failures if isinstance(failures, list) else []
    callback_count = callback_count if isinstance(callback_count, int) else 0
    if failures:
        decision = {
            "action": "block",
            "reason": "runtime_governance_callback_failed",
            "state": "runtime_review_required",
            "callback_failures": copy.deepcopy(failures),
        }
        raise CronRuntimeAdmissionError(
            "Cron job was not run: runtime_governance_callback_failed.",
            decision=decision,
            job=job,
        )

    decisions = [
        item for item in results
        if isinstance(item, dict) and item.get("action") in {"allow", "block"}
    ]
    blocked = next(
        (item for item in decisions if item.get("action") == "block"),
        None,
    )
    if blocked is not None:
        reason = str(blocked.get("reason") or "runtime_binding_required")
        raise CronRuntimeAdmissionError(
            f"Cron job was not run: {reason}.",
            decision=copy.deepcopy(blocked),
            job=job,
        )
    allowed = [item for item in decisions if item.get("action") == "allow"]
    if not allowed and not required and callback_count == 0:
        return
    if len(allowed) != 1:
        decision = {
            "action": "block",
            "reason": "runtime_admission_ambiguous",
            "state": "runtime_review_required",
        }
        raise CronRuntimeAdmissionError(
            "Cron job was not run: runtime_admission_ambiguous.",
            decision=decision,
            job=job,
        )


def _dispatch_post_cron_persist_effects(
    operation: str,
    effects: List[Dict[str, Any]],
) -> None:
    """Dispatch denied-write effects after the jobs lock has been released."""
    if not effects:
        return
    try:
        from hermes_cli.plugins import discover_plugins, invoke_hook

        discover_plugins()
        for effect in effects:
            invoke_hook(
                "post_cron_job_persist",
                operation=operation,
                notification_effect=copy.deepcopy(effect),
            )
    except Exception:
        logger.warning(
            "post_cron_job_persist observer failed after rejected %s",
            operation,
            exc_info=True,
        )


def _invalid_resume(reason: str) -> CronJobGovernanceError:
    return CronJobGovernanceError(
        "Cron job persistence needs administrator review (invalid resume package).",
        decision={
            "action": "block",
            "reason": reason,
            "state": "resume_review_required",
        },
    )


def _parse_resume_package(
    package: Dict[str, Any], *, operation: str
) -> _ParsedCronResume:
    """Verify an immutable v1/v2 envelope without consulting mutable state."""
    if not isinstance(package, dict):
        raise _invalid_resume("resume_package_invalid")
    schema = str(package.get("schema_version") or "").strip()
    receipt_fields = (
        _CRON_RESUME_RECEIPT_CORE_FIELDS
        if schema == _CRON_RESUME_SCHEMA
        else _CRON_RESUME_RECEIPT_V2_CORE_FIELDS
        if schema == _CRON_RESUME_SCHEMA_V2
        else ()
    )
    job = package.get("job")
    receipt = package.get("receipt")
    if not receipt_fields or not isinstance(job, dict) or not isinstance(receipt, dict):
        raise _invalid_resume("resume_package_schema_invalid")
    job_id = str(job.get("id") or "").strip()
    receipt_id = str(receipt.get("receipt_id") or "").strip()
    candidate_hash = str(package.get("candidate_hash") or "").strip()
    persist_spec_hash = str(package.get("persist_spec_hash") or "").strip()
    behavior_ref = str(package.get("authorized_behavior_ref") or "").strip()
    receipt_core = {
        field: copy.deepcopy(receipt.get(field)) for field in receipt_fields
    }
    sha256_pattern = r"sha256:[0-9a-f]{64}"
    safe_job_id = bool(
        job_id
        and job_id not in {".", ".."}
        and "/" not in job_id
        and "\\" not in job_id
        and not Path(job_id).is_absolute()
        and not Path(job_id).drive
    )
    valid = (
        set(package) == _CRON_RESUME_PACKAGE_FIELDS
        and set(receipt) == {*receipt_fields, "receipt_id"}
        and receipt.get("schema_version") == schema
        and package.get("scope_immutable") is True
        and isinstance(package.get("instruction"), str)
        and bool(str(package.get("instruction") or "").strip())
        and str(package.get("operation") or "").strip().lower() == operation
        and str(package.get("job_id") or "").strip() == job_id
        and all(
            str(receipt.get(field) or "").strip()
            for field in receipt_fields
            if field != "prior_job_hash"
        )
        and str(receipt.get("operation") or "").strip().lower() == operation
        and str(receipt.get("cron_job_id") or "").strip() == job_id
        and behavior_ref
        and behavior_ref == str(receipt.get("behavior_id") or "").strip()
        and behavior_ref == str(job.get("authorized_behavior_ref") or "").strip()
        and safe_job_id
        and re.fullmatch(sha256_pattern, receipt_id) is not None
        and receipt_id == _cron_stable_hash(receipt_core)
        and re.fullmatch(sha256_pattern, candidate_hash) is not None
        and candidate_hash == str(receipt.get("candidate_hash") or "").strip()
        and re.fullmatch(sha256_pattern, persist_spec_hash) is not None
        and persist_spec_hash == str(receipt.get("persist_spec_hash") or "").strip()
        and persist_spec_hash == _cron_persist_spec_hash(operation, job)
    )
    if not valid:
        raise _invalid_resume("resume_package_integrity_mismatch")
    if (
        schema == _CRON_RESUME_SCHEMA_V2
        and operation == "update"
        and not str(receipt.get("prior_job_hash") or "").strip()
    ):
        raise _invalid_resume("resume_update_precondition_missing")
    if schema == _CRON_RESUME_SCHEMA_V2:
        identity = cron_persist_resume_identity(operation, job)
        if any(receipt.get(field) != identity[field] for field in identity):
            raise _invalid_resume("resume_request_or_route_mismatch")
        try:
            issued_at = datetime.fromisoformat(
                str(receipt["issued_at"]).replace("Z", "+00:00")
            )
            expires_at = datetime.fromisoformat(
                str(receipt["expires_at"]).replace("Z", "+00:00")
            )
            now = _hermes_now()
            if now.tzinfo is None:
                now = now.astimezone()
        except (TypeError, ValueError):
            raise _invalid_resume("resume_receipt_time_invalid") from None
        if (
            issued_at.tzinfo is None
            or expires_at.tzinfo is None
            or issued_at >= expires_at
            or issued_at > now
            or expires_at <= now
        ):
            raise _invalid_resume("resume_receipt_expired")
    return _ParsedCronResume(
        schema_version=schema,
        operation=operation,
        job=copy.deepcopy(job),
        receipt=copy.deepcopy(receipt),
    )


def _cron_resume_precondition_hash(job: Dict[str, Any]) -> str:
    return _cron_stable_hash({
        key: copy.deepcopy(value)
        for key, value in job.items()
        if key != _CRON_RESUME_RECEIPT_FIELD
    })


def _recovery_context(
    parsed: _ParsedCronResume,
    *,
    disposition: str,
    reason: str,
    fresh_candidate: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    receipt = parsed.receipt
    fresh_candidate_hash = (
        _cron_candidate_definition_hash(fresh_candidate) if fresh_candidate else ""
    )
    fresh_spec_hash = (
        _cron_persist_spec_hash(parsed.operation, fresh_candidate)
        if fresh_candidate
        else ""
    )
    core = {
        "schema_version": _CRON_RECOVERY_SCHEMA,
        "disposition": disposition,
        "reason": reason,
        "profile_id": str(receipt.get("profile_id") or ""),
        "profile_home_sha256": str(receipt.get("profile_home_sha256") or ""),
        "operation": parsed.operation,
        "job_id": str(parsed.job.get("id") or ""),
        "request_id": str(receipt.get("request_id") or ""),
        "request_hash": str(receipt.get("request_hash") or ""),
        "source_route_hash": str(receipt.get("source_route_hash") or ""),
        "rejected_receipt_id": str(receipt.get("receipt_id") or ""),
        "rejected_receipt_hash": _cron_stable_hash(receipt),
        "rejected_frame_id": str(receipt.get("frame_id") or ""),
        "rejected_pending_id": str(receipt.get("pending_id") or ""),
        "rejected_candidate_hash": str(receipt.get("candidate_hash") or ""),
        "rejected_persist_spec_hash": str(receipt.get("persist_spec_hash") or ""),
        "prior_job_hash": str(receipt.get("prior_job_hash") or ""),
        "fresh_candidate_hash": fresh_candidate_hash,
        "fresh_persist_spec_hash": fresh_spec_hash,
    }
    core["recovery_id"] = _cron_stable_hash(core)
    return core


def _resolve_resume(
    parsed: _ParsedCronResume,
    *,
    existing_jobs: List[Dict[str, Any]],
) -> CronResumeResolution:
    """Classify exact, recoverable drift, and unsafe resume envelopes."""
    candidate = copy.deepcopy(parsed.job)
    receipt = parsed.receipt
    job_id = str(candidate.get("id") or "")
    matches = [job for job in existing_jobs if str(job.get("id") or "") == job_id]
    if len(matches) > 1:
        raise _invalid_resume("resume_job_id_ambiguous")
    if parsed.operation == "create" and matches:
        stored_receipt = matches[0].get("creation_governance_receipt")
        if not (
            isinstance(stored_receipt, dict)
            and str(stored_receipt.get("resume_receipt_id") or "")
            == str(receipt.get("receipt_id") or "")
        ):
            raise _invalid_resume("resume_job_id_conflict")
    if parsed.operation == "update":
        if len(matches) != 1:
            raise _invalid_resume("resume_update_target_missing")
        if matches[0].get("active_run_outcome_claim") is not None:
            raise CronJobGovernanceError(
                "Cron job authorization cannot change while a signed run is active."
            )
        if parsed.schema_version == _CRON_RESUME_SCHEMA_V2 and (
            _cron_resume_precondition_hash(matches[0])
            != str(receipt.get("prior_job_hash") or "")
        ):
            raise _invalid_resume("resume_update_precondition_mismatch")
    if parsed.schema_version == _CRON_RESUME_SCHEMA:
        return CronResumeResolution(
            disposition="exact",
            candidate={
                **candidate,
                _CRON_RESUME_RECEIPT_FIELD: copy.deepcopy(receipt),
            },
        )

    selectors = _normalize_skill_list(candidate.get("skill"), candidate.get("skills"))
    try:
        normalized_skills, bindings = _resolve_skill_fields(selectors)
    except Exception as exc:
        from agent.skill_resolution import SkillResolutionError

        if not isinstance(exc, SkillResolutionError):
            raise
        context = _recovery_context(
            parsed,
            disposition="blocked_unreconstructable",
            reason=str(exc.code),
            fresh_candidate=None,
        )
        return CronResumeResolution(
            disposition="blocked_unreconstructable",
            candidate=candidate,
            reason=str(exc.code),
            recovery_context=context,
        )
    fresh = copy.deepcopy(candidate)
    fresh["skills"] = normalized_skills
    fresh["skill"] = normalized_skills[0] if normalized_skills else None
    fresh["skill_bindings"] = bindings
    fresh_identity = cron_persist_resume_identity(parsed.operation, fresh)
    if any(
        fresh_identity[field] != str(receipt.get(field) or "")
        for field in (
            "profile_id",
            "profile_home_sha256",
            "request_id",
            "request_hash",
            "source_route_hash",
        )
    ):
        raise _invalid_resume("resume_fresh_request_or_route_mismatch")
    fresh_spec_hash = _cron_persist_spec_hash(parsed.operation, fresh)
    if fresh_spec_hash == str(receipt.get("persist_spec_hash") or ""):
        return CronResumeResolution(
            disposition="exact",
            candidate={
                **fresh,
                _CRON_RESUME_RECEIPT_FIELD: copy.deepcopy(receipt),
            },
        )
    context = _recovery_context(
        parsed,
        disposition="fresh_blocked_candidate",
        reason="resume_persist_spec_drift",
        fresh_candidate=fresh,
    )
    return CronResumeResolution(
        disposition="recoverable_spec_drift",
        candidate=fresh,
        reason="resume_persist_spec_drift",
        recovery_context=context,
    )


def get_cron_persist_recovery(
    recovery_id: str, *, profile_home: Optional[Union[str, Path]] = None
) -> Optional[Dict[str, Any]]:
    """Return one verified profile-local recovery disposition."""
    from cron.persist_recovery import get_recovery

    cron_dir = (
        Path(profile_home).expanduser().resolve(strict=False) / "cron"
        if profile_home is not None
        else _current_cron_store().cron_dir
    )
    return get_recovery(
        cron_dir,
        str(recovery_id or "").strip(),
        profile_home=(
            Path(profile_home).expanduser().resolve(strict=False)
            if profile_home is not None
            else _active_profile_home()
        ),
    )


def _recovery_store_failure(exc: Exception) -> CronJobGovernanceError:
    return CronJobGovernanceError(
        "Cron job persistence needs administrator review "
        "(resume recovery store unavailable).",
        decision={
            "action": "block",
            "reason": "resume_recovery_store_unavailable",
            "state": "resume_review_required",
            "callback_failures": [{
                "stage": "recovery_store",
                "exception_class": type(exc).__name__,
            }],
        },
    )


def _load_recovery_replay(
    context: Dict[str, Any], candidate: Dict[str, Any]
) -> Optional[CronJobGovernanceError]:
    from cron.persist_recovery import (
        CronPersistRecoveryStoreError,
        load_by_rejected_receipt,
    )

    try:
        stored = load_by_rejected_receipt(
            _current_cron_store().cron_dir,
            str(context["rejected_receipt_id"]),
            profile_home=_active_profile_home(),
        )
    except CronPersistRecoveryStoreError as exc:
        raise _recovery_store_failure(exc) from exc
    if stored is None:
        return None
    lineage_fields = (
        "recovery_id",
        "disposition",
        "reason",
        "profile_id",
        "profile_home_sha256",
        "operation",
        "job_id",
        "request_id",
        "request_hash",
        "source_route_hash",
        "rejected_receipt_id",
        "rejected_receipt_hash",
        "rejected_frame_id",
        "rejected_pending_id",
        "rejected_candidate_hash",
        "rejected_persist_spec_hash",
        "prior_job_hash",
        "fresh_candidate_hash",
        "fresh_persist_spec_hash",
    )
    if (
        any(stored.get(field) != context.get(field) for field in lineage_fields)
        or stored.get("candidate") != candidate
        or not isinstance(stored.get("decision"), dict)
    ):
        raise _invalid_resume("resume_recovery_lineage_conflict")
    decision = copy.deepcopy(stored["decision"])
    # Registration and transport are owned by the sealed HAK Frame. Ordinary
    # replay returns the same pending action without re-emitting its observer.
    decision.pop("post_persist_effects", None)
    return CronJobGovernanceError(
        "Cron job was not saved: stale authorization requires fresh review.",
        decision=decision,
    )


def _record_recovery_error(
    resolution: CronResumeResolution,
    decision: Dict[str, Any],
) -> CronJobGovernanceError:
    from cron.persist_recovery import (
        CronPersistRecoveryStoreError,
        record_recovery,
    )

    context = copy.deepcopy(resolution.recovery_context or {})
    if resolution.disposition == "recoverable_spec_drift":
        registration = decision.get("recovery_registration")
        effects = decision.get("post_persist_effects")
        if not isinstance(registration, dict) or not (
            isinstance(effects, list)
            and len(effects) == 1
            and isinstance(effects[0], dict)
        ):
            raise CronJobGovernanceError(
                "Cron job persistence needs administrator review "
                "(invalid resume recovery governance result)."
            )
        context["pending_id"] = str(registration.get("pending_id") or "")
        context["frame_id"] = str(registration.get("frame_id") or "")
        context["registration"] = copy.deepcopy(registration)
        context["notification_effect"] = copy.deepcopy(effects[0])
    public_recovery = {
        key: copy.deepcopy(value)
        for key, value in context.items()
        if key not in {"rejected_receipt_hash"}
    }
    stored_decision = copy.deepcopy(decision)
    stored_decision.pop("post_persist_effects", None)
    stored_decision["recovery"] = public_recovery
    record = {
        **context,
        "candidate": copy.deepcopy(resolution.candidate),
        "decision": stored_decision,
        "created_at": _hermes_now().isoformat(),
    }
    try:
        record_recovery(
            _current_cron_store().cron_dir,
            record,
            profile_home=_active_profile_home(),
        )
    except CronPersistRecoveryStoreError as exc:
        raise _recovery_store_failure(exc) from exc
    reason = str(stored_decision.get("reason") or resolution.reason)
    state = str(stored_decision.get("state") or "resume_review_required")
    return CronJobGovernanceError(
        f"Cron job was not saved: {reason} ({state}).",
        decision=stored_decision,
    )


def _unreconstructable_recovery_error(
    resolution: CronResumeResolution,
) -> CronJobGovernanceError:
    decision = {
        "action": "block",
        "reason": resolution.reason or "resume_candidate_unreconstructable",
        "state": "resume_recovery_unreconstructable",
    }
    return _record_recovery_error(resolution, decision)


def _claim_cron_recovery_dispatch(recovery_id: str) -> Optional[Dict[str, Any]]:
    from cron.persist_recovery import (
        CronPersistRecoveryStoreError,
        claim_recovery_dispatch,
    )

    try:
        return claim_recovery_dispatch(
            _current_cron_store().cron_dir,
            recovery_id,
            profile_home=_active_profile_home(),
        )
    except CronPersistRecoveryStoreError as exc:
        raise _recovery_store_failure(exc) from exc


def _valid_recovery_dispatch_ack(
    result: Any,
    claim: Dict[str, Any],
) -> bool:
    registration = claim.get("registration")
    issuer = registration.get("issuer") if isinstance(registration, dict) else None
    durable_cas = result.get("durable_cas") if isinstance(result, dict) else None
    return bool(
        isinstance(result, dict)
        and set(result)
        == {
            "schema_version",
            "issuer",
            "recovery_id",
            "dispatch_key",
            "disposition",
            "durable_cas",
        }
        and result.get("schema_version") == _CRON_RECOVERY_DISPATCH_ACK_SCHEMA
        and result.get("issuer") == issuer
        and result.get("recovery_id") == claim.get("recovery_id")
        and result.get("dispatch_key") == claim.get("dispatch_key")
        and result.get("disposition") == "durably_accepted"
        and isinstance(durable_cas, dict)
        and set(durable_cas)
        == {"schema_version", "dispatch_key", "owner_id", "cas_version"}
        and durable_cas.get("schema_version") == _CRON_RECOVERY_DURABLE_CAS_SCHEMA
        and durable_cas.get("dispatch_key") == claim.get("dispatch_key")
        and bool(str(durable_cas.get("owner_id") or "").strip())
        and isinstance(durable_cas.get("cas_version"), int)
        and durable_cas.get("cas_version") >= 1
    )


def _dispatch_claimed_cron_recovery_effects(
    operation: str,
    claims: List[Dict[str, Any]],
) -> None:
    """Deliver durable recovery effects after the jobs lock is released."""
    if not claims:
        return
    from cron.persist_recovery import (
        CronPersistRecoveryStoreError,
        complete_recovery_dispatch,
        heartbeat_recovery_dispatch,
        release_recovery_dispatch,
    )

    # ContextVar-backed profile routing does not propagate into a plain
    # threading.Thread. Pin the owning store for the whole dispatch lifecycle.
    dispatch_store = _current_cron_store()
    dispatch_profile_home = dispatch_store.cron_dir.parent.resolve(strict=False)

    for claim in claims:
        acknowledged = False
        heartbeat_stop = threading.Event()
        heartbeat_lost = threading.Event()
        heartbeat_interval = max(
            min(float(claim.get("lease_seconds") or 30.0) / 3.0, 5.0),
            0.05,
        )

        def maintain_claim() -> None:
            while not heartbeat_stop.wait(heartbeat_interval):
                try:
                    alive = heartbeat_recovery_dispatch(
                        dispatch_store.cron_dir,
                        str(claim["recovery_id"]),
                        str(claim["claim_id"]),
                        int(claim["fence_token"]),
                        profile_home=dispatch_profile_home,
                        lease_seconds=float(claim["lease_seconds"]),
                    )
                except CronPersistRecoveryStoreError:
                    logger.warning(
                        "Cron resume recovery dispatch heartbeat failed",
                        exc_info=True,
                    )
                    alive = False
                if not alive:
                    heartbeat_lost.set()
                    return

        heartbeat_thread: Optional[threading.Thread] = None
        try:
            from hermes_cli.plugins import discover_plugins, invoke_hook

            discover_plugins()
            if not heartbeat_recovery_dispatch(
                dispatch_store.cron_dir,
                str(claim["recovery_id"]),
                str(claim["claim_id"]),
                int(claim["fence_token"]),
                profile_home=dispatch_profile_home,
                lease_seconds=float(claim["lease_seconds"]),
            ):
                heartbeat_lost.set()
                raise CronPersistRecoveryStoreError(
                    "Cron resume recovery dispatch claim was fenced before observer entry."
                )
            heartbeat_thread = threading.Thread(
                target=maintain_claim,
                name="cron-recovery-dispatch-heartbeat",
                daemon=True,
            )
            heartbeat_thread.start()
            results = invoke_hook(
                "post_cron_job_persist",
                operation=operation,
                notification_effect=copy.deepcopy(claim["notification_effect"]),
                recovery_registration=copy.deepcopy(claim["registration"]),
                recovery_dispatch={
                    "recovery_id": claim["recovery_id"],
                    "dispatch_key": claim["dispatch_key"],
                },
            )
            acknowledgements = [
                result
                for result in results
                if _valid_recovery_dispatch_ack(result, claim)
            ]
            acknowledged = len(acknowledgements) == 1 and not heartbeat_lost.is_set()
        except Exception:
            logger.warning(
                "post_cron_job_persist recovery observer failed after rejected %s",
                operation,
                exc_info=True,
            )
        finally:
            heartbeat_stop.set()
            if heartbeat_thread is not None:
                heartbeat_thread.join(timeout=max(heartbeat_interval * 2.0, 0.2))
        try:
            if acknowledged:
                complete_recovery_dispatch(
                    dispatch_store.cron_dir,
                    str(claim["recovery_id"]),
                    str(claim["claim_id"]),
                    int(claim["fence_token"]),
                    profile_home=dispatch_profile_home,
                )
            else:
                release_recovery_dispatch(
                    dispatch_store.cron_dir,
                    str(claim["recovery_id"]),
                    str(claim["claim_id"]),
                    int(claim["fence_token"]),
                    profile_home=dispatch_profile_home,
                )
        except CronPersistRecoveryStoreError:
            logger.warning(
                "Cron resume recovery dispatch disposition update failed",
                exc_info=True,
            )


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


def _active_profile_home() -> Path:
    """Return the profile that owns the active Cron store."""
    return _current_cron_store().cron_dir.parent.resolve(strict=False)


def _registered_plugin_skill_paths(
    selectors: List[str], profile_home: Path
) -> Dict[str, Path]:
    qualified = [selector for selector in selectors if ":" in selector]
    if not qualified:
        return {}
    if profile_home.resolve(strict=False) != get_hermes_home().resolve(strict=False):
        return {}
    try:
        from hermes_cli.plugins import discover_plugins, get_plugin_manager

        discover_plugins()
        manager = get_plugin_manager()
        return {
            selector: path
            for selector in qualified
            if (path := manager.find_plugin_skill(selector)) is not None
        }
    except Exception as exc:
        from agent.skill_resolution import SkillResolutionError

        raise SkillResolutionError(
            "skill_plugin_resolution_failed",
            qualified[0],
            "Plugin skill discovery failed before Cron persistence.",
        ) from exc


def _resolve_skill_fields(
    selectors: List[str],
    *,
    profile_home: Optional[Path] = None,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    if not selectors:
        return [], []
    from agent.skill_resolution import resolve_skill_refs

    profile = (profile_home or _active_profile_home()).resolve(strict=False)
    bindings = resolve_skill_refs(
        profile,
        selectors,
        plugin_skill_paths=_registered_plugin_skill_paths(selectors, profile),
    )
    return [str(binding["canonical_name"]) for binding in bindings], bindings


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


def _preserve_file_ownership(path: Path, before: Optional[os.stat_result]) -> None:
    """Restore a rewritten file's previous owner (POSIX, privileged writer only).

    The atomic-write pattern (mkstemp + replace) makes the rewritten file owned
    by the *writer's* euid. When a root shell runs a state-writing cron CLI
    command (``docker exec hermes hermes cron create ...`` — ``docker exec``
    defaults to root) against a store owned by the unprivileged gateway user,
    the replace flips ``jobs.json`` to ``root:root`` mode 600 and the gateway's
    ticker (uid 1000) is silently locked out of every subsequent tick (#68483).

    Root can always hand ownership back, so do exactly that: when the euid is 0
    and the pre-replace owner differs, chown the new file to the previous
    uid/gid. Unprivileged writers are a no-op (their own rewrite already heals
    a root-owned file back to their uid, and they couldn't chown anyway).
    No-op on Windows. Best-effort: a failure must never break the save.
    """
    if before is None or os.name != "posix":
        return
    geteuid = getattr(os, "geteuid", None)
    getegid = getattr(os, "getegid", None)
    if geteuid is None or getegid is None:
        return
    try:
        euid = geteuid()
        if euid != 0:
            return  # unprivileged writer — nothing to (or we could) restore
        if (before.st_uid, before.st_gid) == (euid, getegid()):
            return  # already ours before the rewrite — nothing changed
        os.chown(path, before.st_uid, before.st_gid)
    except OSError as e:
        logger.warning(
            "Could not restore ownership of %s to uid=%s gid=%s after rewrite: %s "
            "— if the gateway runs as a different user, its cron ticker may now "
            "be locked out (see issue #68483).",
            path, before.st_uid, before.st_gid, e,
        )


def ensure_dirs():
    """Ensure cron directories exist with secure permissions."""
    store = _current_cron_store()
    store.cron_dir.mkdir(parents=True, exist_ok=True)
    store.output_dir.mkdir(parents=True, exist_ok=True)
    _secure_dir(store.cron_dir)
    _secure_dir(store.output_dir)


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
        if not _ensure_croniter():
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

    if kind == "cron" and _ensure_croniter():
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
        if not _ensure_croniter():
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

    Delegates to :func:`utils.atomic_write_text` (tmpfile + fsync +
    ``atomic_replace``, same pattern as ``save_jobs``) so a concurrent reader
    in another process (``hermes cron status``) never sees a torn/truncated
    file. Best-effort: failures are swallowed by callers.
    """
    ensure_dirs()
    atomic_write_text(path, str(time.time()), tmp_prefix=".hb_")


def _atomic_write_counter(path: Path, value: int) -> None:
    """Atomically persist a non-negative integer counter."""
    ensure_dirs()
    fd, tmp_path = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp", prefix=".count_")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(str(max(0, value)))
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

    Resolution uses ``_current_cron_store()`` so the heartbeat is correctly
    scoped to the active profile's store — critical under multiplex_profiles
    where each profile needs its own liveness signal (#69377).

    Best-effort: a write failure must never disrupt the tick loop.
    """
    store = _current_cron_store()
    try:
        _atomic_write_epoch(store.cron_dir / "ticker_heartbeat")
    except Exception:
        pass
    if success:
        try:
            _atomic_write_epoch(store.cron_dir / "ticker_last_success")
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

    Resolution uses ``_current_cron_store()`` so the heartbeat is correctly
    scoped to the active profile — critical under multiplex_profiles where
    ``hermes cron status`` must report per-profile liveness (#69377).
    """
    store = _current_cron_store()
    return _epoch_file_age(store.cron_dir / "ticker_heartbeat")


def get_ticker_success_age() -> Optional[float]:
    """Seconds since the ticker last completed a tick WITHOUT raising, or None.

    Resolution uses ``_current_cron_store()`` so the heartbeat is correctly
    scoped to the active profile — critical under multiplex_profiles where
    ``hermes cron status`` must report per-profile liveness (#69377).
    """
    store = _current_cron_store()
    return _epoch_file_age(store.cron_dir / "ticker_last_success")


def record_catch_up_occurrence() -> None:
    """Increment the profile-local stale-schedule catch-up counter, best effort."""
    path = _current_cron_store().cron_dir / "catch_up_occurrences"
    try:
        try:
            value = int(path.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            value = 0
        _atomic_write_counter(path, max(0, value) + 1)
    except Exception:
        pass


def record_ticker_error(message: str) -> None:
    """Persist the most recent tick failure so other processes can surface it.

    The ticker thread lives inside the gateway process; ``hermes cron
    status``/``list`` run in a separate process and previously could only
    infer "ticks may be failing" from marker staleness, with no clue WHY.
    A root-owned ``jobs.json`` (#68483) failed every tick for ~14h with the
    reason visible only in the gateway's errors.log. Writing the last error
    next to the heartbeat markers gives the CLI something concrete to show.

    Best-effort: a write failure must never disrupt the tick loop.
    """
    store = _current_cron_store()
    path = store.cron_dir / "ticker_last_error"
    try:
        ensure_dirs()
        fd, tmp_path = tempfile.mkstemp(
            dir=str(path.parent), suffix=".tmp", prefix=".terr_"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(f"{time.time()}\n{message.strip()}\n")
                f.flush()
                os.fsync(f.fileno())
            atomic_replace(tmp_path, path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    except Exception:
        pass


def get_catch_up_occurrence_count() -> int:
    """Return the profile-local stale-schedule catch-up count."""
    path = _current_cron_store().cron_dir / "catch_up_occurrences"
    try:
        return max(0, int(path.read_text(encoding="utf-8").strip()))
    except (OSError, ValueError):
        return 0


def clear_ticker_error() -> None:
    """Remove the last-tick-error marker after a successful tick. Best-effort."""
    store = _current_cron_store()
    try:
        (store.cron_dir / "ticker_last_error").unlink()
    except OSError:
        pass


def get_ticker_last_error() -> Optional[str]:
    """Return the most recent recorded tick error message, or None."""
    store = _current_cron_store()
    try:
        raw = (store.cron_dir / "ticker_last_error").read_text(encoding="utf-8")
    except Exception:
        return None
    lines = raw.splitlines()
    if len(lines) < 2:
        return None
    message = "\n".join(lines[1:]).strip()
    return message or None


# =============================================================================
# Job CRUD Operations
# =============================================================================

def load_jobs(*, repair_recoverable: bool = True) -> List[Dict[str, Any]]:
    """Load all jobs from storage."""
    jobs_file = _current_cron_store().jobs_file
    ensure_dirs()
    if not jobs_file.exists():
        return []

    _strict_retry = False  # track whether we used the strict=False fallback

    try:
        # utf-8-sig: Windows Notepad / PowerShell 5.1 Set-Content -Encoding UTF8
        # write a leading BOM; json.load under plain utf-8 raises
        # JSONDecodeError("Unexpected UTF-8 BOM") and takes down cron.
        with open(jobs_file, 'r', encoding='utf-8-sig') as f:
            data = json.load(f)
    except json.JSONDecodeError:
        # Retry with strict=False to handle bare control chars in string values
        _strict_retry = True
        try:
            with open(jobs_file, 'r', encoding='utf-8-sig') as f:
                data = json.loads(f.read(), strict=False)
        except Exception as e:
            logger.error("Failed to auto-repair jobs.json: %s", e)
            raise RuntimeError(f"Cron database corrupted and unrepairable: {e}") from e
    except IOError as e:
        logger.error("IOError reading jobs.json: %s", e)
        raise RuntimeError(f"Failed to read cron database: {e}") from e

    # Validate the top-level JSON shape: accept a dict (expected) or a bare
    # list (auto-repair). Anything else (str/number/null) is corruption that
    # would otherwise raise an uncaught AttributeError on ``.get()`` and take
    # down the whole cron subsystem.
    if isinstance(data, dict):
        jobs = data.get("jobs", [])
        if _strict_retry:
            if not repair_recoverable:
                raise CronJobGovernanceError(
                    "Cron job persistence needs administrator review "
                    "(jobs store requires control-character repair)."
                )
            # Hit control-character corruption — rewrite with proper escaping.
            if jobs:
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
            save_jobs(data)
            logger.warning("Auto-repaired jobs.json (bare list wrapped as dict)")
        return data

    raise RuntimeError(
        f"Cron database corrupted: expected {{'jobs': [...]}}, got {type(data).__name__}"
    )


def _save_jobs_unlocked(jobs: List[Dict[str, Any]]):
    """Save all jobs to storage. Caller must hold _jobs_lock()."""
    jobs_file = _current_cron_store().jobs_file
    ensure_dirs()
    # Snapshot the current owner BEFORE the atomic replace so a privileged
    # writer (root CLI in Docker) can hand ownership back to the gateway user
    # afterwards instead of locking its ticker out (#68483). When the file is
    # being created for the first time, inherit the cron dir's owner — in the
    # Docker image that is the PUID/PGID gateway user who must be able to
    # read the store on the next tick.
    try:
        _stat_before = os.stat(jobs_file)
    except OSError:
        try:
            _stat_before = os.stat(jobs_file.parent)
        except OSError:
            _stat_before = None
    fd, tmp_path = tempfile.mkstemp(dir=str(jobs_file.parent), suffix='.tmp', prefix='.jobs_')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump({"jobs": jobs, "updated_at": _hermes_now().isoformat()}, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        atomic_replace(tmp_path, jobs_file)
        _secure_file(jobs_file)
        _preserve_file_ownership(jobs_file, _stat_before)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def save_jobs(jobs: List[Dict[str, Any]]):
    """Save all jobs to storage."""
    with _jobs_lock():
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
        from hermes_cli.config import _expand_env_vars, read_user_config_raw

        cfg_path = get_hermes_home() / "config.yaml"
        if not cfg_path.exists():
            return None
        cfg = read_user_config_raw(cfg_path)
        try:
            from hermes_cli import managed_scope
            cfg = managed_scope.apply_managed_overlay(cfg)
        except Exception:
            pass
        cfg = _expand_env_vars(cfg)
        # Mirror run_job's precedence: the explicit cron-fleet default
        # (cron.model) beats the global chat model for unpinned cron jobs.
        cron_cfg = cfg.get("cron") or {}
        if isinstance(cron_cfg, dict):
            cron_model = cron_cfg.get("model")
            if isinstance(cron_model, str) and cron_model.strip():
                return cron_model.strip()
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
        parsed_resume = _parse_resume_package(governance_resume, operation="create")
        post_effects: List[Dict[str, Any]] = []
        recovery_claims: List[Dict[str, Any]] = []
        with contextlib.ExitStack() as stack:
            stack.callback(
                _dispatch_post_cron_persist_effects,
                "create",
                post_effects,
            )
            stack.callback(
                _dispatch_claimed_cron_recovery_effects,
                "create",
                recovery_claims,
            )
            stack.enter_context(_jobs_lock(require_cross_process=True))
            jobs = load_jobs(repair_recoverable=False)
            resolution = _resolve_resume(parsed_resume, existing_jobs=jobs)
            candidate = resolution.candidate
            if resolution.recovery_context is not None:
                replay = _load_recovery_replay(
                    resolution.recovery_context,
                    resolution.candidate,
                )
                if replay is not None:
                    claim = _claim_cron_recovery_dispatch(
                        str(resolution.recovery_context["recovery_id"])
                    )
                    if claim is not None:
                        recovery_claims.append(claim)
                    raise replay
            if resolution.disposition == "blocked_unreconstructable":
                raise _unreconstructable_recovery_error(resolution)
            try:
                governed, disposition = _apply_cron_persist_governance(
                    "create",
                    candidate,
                    jobs,
                    recovery_context=resolution.recovery_context,
                )
            except CronJobGovernanceError as exc:
                if (
                    resolution.disposition == "recoverable_spec_drift"
                    and isinstance(exc.decision.get("recovery_registration"), dict)
                ):
                    recovery_error = _record_recovery_error(resolution, exc.decision)
                    claim = _claim_cron_recovery_dispatch(
                        str(resolution.recovery_context["recovery_id"])
                    )
                    if claim is not None:
                        recovery_claims.append(claim)
                    raise recovery_error from exc
                post_effects.extend(_post_cron_persist_effects(exc))
                raise
            if resolution.disposition == "recoverable_spec_drift":
                raise CronJobGovernanceError(
                    "Cron job persistence needs administrator review "
                    "(resume recovery was not blocked)."
                )
            if disposition == "already_persisted":
                return _normalize_job_record(governed)
            if any(
                str(item.get("id") or "") == str(governed.get("id") or "")
                for item in jobs
            ):
                raise CronJobGovernanceError(
                    "Cron job persistence needs administrator review "
                    "(resume job id conflict)."
                )
            jobs.append(governed)
            _save_jobs_unlocked(jobs)
        return _normalize_job_record(governed)

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

    requested_skills = _normalize_skill_list(skill, skills)
    normalized_skills, skill_bindings = _resolve_skill_fields(requested_skills)
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
        "skill_bindings": skill_bindings,
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
        _CRON_RUNTIME_ADMISSION_RECEIPT_FIELD: None,
        _CRON_DELIVERY_RECEIPT_FIELD: None,
        _CRON_RUN_OUTCOME_RECEIPT_FIELD: None,
        _CRON_RUN_OUTCOME_CLAIM_FIELD: None,
        # Delivery configuration
        "deliver": deliver,
        "origin": origin,  # Tracks where job was created for "origin" delivery
        "enabled_toolsets": normalized_toolsets,
        "workdir": normalized_workdir,
    }
    # Only persist attach_to_session when explicitly set, so existing jobs and
    # the common case stay byte-identical (absent key => fall back to the
    # global cron.mirror_delivery config, default off).
    if normalized_attach is not None:
        job["attach_to_session"] = normalized_attach

    if authorized_behavior_ref is not None:
        job["authorized_behavior_ref"] = _normalize_job_optional_text(
            authorized_behavior_ref
        )
    if implementation_categories is not None:
        job["implementation_categories"] = [
            str(item).strip()
            for item in implementation_categories
            if str(item).strip()
        ]

    governance_active = _cron_persist_governance_active(job)
    post_effects: List[Dict[str, Any]] = []
    with contextlib.ExitStack() as stack:
        stack.callback(_dispatch_post_cron_persist_effects, "create", post_effects)
        lock_context = (
            _jobs_lock(require_cross_process=True)
            if governance_active
            else _jobs_lock()
        )
        stack.enter_context(lock_context)
        jobs = load_jobs()
        try:
            job, disposition = _apply_cron_persist_governance("create", job, jobs)
        except CronJobGovernanceError as exc:
            post_effects.extend(_post_cron_persist_effects(exc))
            raise
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
    try:
        from cron.executions import latest_executions

        latest = latest_executions([job.get("id", "") for job in jobs])
    except Exception:
        latest = {}
    for job in jobs:
        job["latest_execution"] = latest.get(job.get("id", ""))
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
        parsed_resume = _parse_resume_package(governance_resume, operation="update")
        if str(parsed_resume.job.get("id") or "") != str(job_id or ""):
            raise CronJobGovernanceError(
                "Cron job persistence needs administrator review "
                "(resume job id mismatch)."
            )
        post_effects: List[Dict[str, Any]] = []
        recovery_claims: List[Dict[str, Any]] = []
        with contextlib.ExitStack() as stack:
            stack.callback(
                _dispatch_post_cron_persist_effects,
                "update",
                post_effects,
            )
            stack.callback(
                _dispatch_claimed_cron_recovery_effects,
                "update",
                recovery_claims,
            )
            stack.enter_context(_jobs_lock(require_cross_process=True))
            jobs = load_jobs(repair_recoverable=False)
            resolution = _resolve_resume(parsed_resume, existing_jobs=jobs)
            candidate = resolution.candidate
            if resolution.recovery_context is not None:
                replay = _load_recovery_replay(
                    resolution.recovery_context,
                    resolution.candidate,
                )
                if replay is not None:
                    claim = _claim_cron_recovery_dispatch(
                        str(resolution.recovery_context["recovery_id"])
                    )
                    if claim is not None:
                        recovery_claims.append(claim)
                    raise replay
            if resolution.disposition == "blocked_unreconstructable":
                raise _unreconstructable_recovery_error(resolution)
            target = next(
                (index for index, item in enumerate(jobs) if item.get("id") == job_id),
                None,
            )
            if target is None:
                return None
            if jobs[target].get("active_run_outcome_claim") is not None:
                raise CronJobGovernanceError(
                    "Cron job authorization cannot change while a signed run is active."
                )
            try:
                governed, disposition = _apply_cron_persist_governance(
                    "update",
                    candidate,
                    jobs,
                    recovery_context=resolution.recovery_context,
                )
            except CronJobGovernanceError as exc:
                if (
                    resolution.disposition == "recoverable_spec_drift"
                    and isinstance(exc.decision.get("recovery_registration"), dict)
                ):
                    recovery_error = _record_recovery_error(resolution, exc.decision)
                    claim = _claim_cron_recovery_dispatch(
                        str(resolution.recovery_context["recovery_id"])
                    )
                    if claim is not None:
                        recovery_claims.append(claim)
                    raise recovery_error from exc
                post_effects.extend(_post_cron_persist_effects(exc))
                raise
            if resolution.disposition == "recoverable_spec_drift":
                raise CronJobGovernanceError(
                    "Cron job persistence needs administrator review "
                    "(resume recovery was not blocked)."
                )
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

    post_effects: List[Dict[str, Any]] = []
    with contextlib.ExitStack() as stack:
        stack.callback(_dispatch_post_cron_persist_effects, "update", post_effects)
        stack.enter_context(_jobs_lock())
        jobs = load_jobs(
            repair_recoverable=not (
                governance_refresh
                or deprecated_verification_retirement is not None
            )
        )
        for i, job in enumerate(jobs):
            if job["id"] != job_id:
                continue

            special_operation = (
                governance_refresh
                or deprecated_verification_retirement is not None
            )
            if special_operation and not _cron_persist_governance_active(job):
                raise CronJobGovernanceError(
                    "Cron job persistence needs administrator review "
                    "(governance unavailable)."
                )
            if special_operation and job.get("active_run_outcome_claim") is not None:
                raise CronJobGovernanceError(
                    "Cron job authorization cannot change while a signed run is active."
                )

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
                    f"HERMES_HOME={_current_cron_store().cron_dir.parent.resolve()} "
                    f"hermes cron status {job_id}"
                )
                if (
                    not isinstance(request, dict)
                    or set(request) != expected_keys
                    or request.get("schema_version")
                    != "cron-verification-retirement/v1"
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

            skill_fields_changed = "skills" in updates or "skill" in updates
            previous_inference_axes = _normalized_inference_axes(job)
            if special_operation:
                updated = copy.deepcopy(job)
            else:
                updated = (
                    _apply_skill_fields({**job, **updates})
                    if skill_fields_changed
                    else {**job, **updates}
                )
            if deprecated_verification_retirement is not None:
                updated.pop("verification_command", None)
                updated.pop("verification_command_mode", None)
            schedule_changed = "schedule" in updates
            inference_fields_changed = bool(
                {"provider", "model", "base_url", "no_agent"}.intersection(updates)
            ) and _normalized_inference_axes(updated) != previous_inference_axes

            if skill_fields_changed:
                requested_skills = _normalize_skill_list(
                    updates.get("skill") if "skill" in updates else None,
                    updates.get("skills") if "skills" in updates else None,
                )
                normalized_skills, skill_bindings = _resolve_skill_fields(
                    requested_skills
                )
                updated["skills"] = normalized_skills
                updated["skill"] = normalized_skills[0] if normalized_skills else None
                updated["skill_bindings"] = skill_bindings

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

            material_changed = (
                governance_refresh
                or _cron_governance_material_changed(job, updated)
            )
            if material_changed:
                if job.get("active_run_outcome_claim") is not None:
                    raise CronJobGovernanceError(
                        "Cron job authorization cannot change while a signed run is active."
                    )
                governance_active = _cron_persist_governance_active(updated)
                strict_context = (
                    _jobs_lock(require_cross_process=True)
                    if governance_active
                    else contextlib.nullcontext()
                )
                with strict_context:
                    try:
                        updated, disposition = _apply_cron_persist_governance(
                            "update", updated, jobs
                        )
                    except CronJobGovernanceError as exc:
                        post_effects.extend(_post_cron_persist_effects(exc))
                        raise
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
        job_output_dir = _job_output_dir(canonical_id)
        retained = [item for item in jobs if item["id"] != canonical_id]
        _save_jobs_unlocked(retained)
        # Clean up output directory to prevent orphaned dirs accumulating.
        if job_output_dir.exists():
            shutil.rmtree(job_output_dir)
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
    ``runtime_admission_receipt`` records a fail-closed pre-execution decision.
    ``delivery_receipt`` and ``run_outcome_receipt`` contain only redacted,
    revision-bound terminal proof. The claim is the CAS owner for that write.
    """
    if runtime_admission_receipt is not None:
        runtime_admission_receipt = _validated_runtime_admission_receipt(
            runtime_admission_receipt
        )
    if delivery_receipt is not None:
        delivery_receipt = _validated_delivery_receipt(delivery_receipt)
    if run_outcome_receipt is not None:
        run_outcome_receipt = _validated_run_outcome_receipt(run_outcome_receipt)
    if run_outcome_claim is not None:
        run_outcome_claim = _validated_run_outcome_claim(run_outcome_claim)
    if run_outcome_receipt is not None and run_outcome_claim is None:
        raise ValueError("cron run outcome receipt requires its pre-run claim")
    with _jobs_lock(require_cross_process=run_outcome_claim is not None):
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
                job[_CRON_RUNTIME_ADMISSION_RECEIPT_FIELD] = (
                    copy.deepcopy(runtime_admission_receipt)
                    if runtime_admission_receipt is not None
                    else None
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
                        # Limit reached: retain the record as a terminal
                        # completion instead of popping it. Deleting the job
                        # here discarded the last_status / last_error /
                        # last_delivery_error written above — a finished
                        # one-shot vanished from `cronjob list` with no
                        # inspectable outcome, and a failed delivery was
                        # invisible. Mirror the terminal shape of the
                        # next_run_at-is-None branch below; the retention
                        # sweep prunes these after
                        # COMPLETED_ONESHOT_RETENTION_DAYS.
                        job["enabled"] = False
                        job["state"] = "completed"
                        job["next_run_at"] = None
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


def _write_wedged_oneshot_diagnostic(job: Dict[str, Any]) -> None:
    """Leave an operator-visible trace when a wedged one-shot is removed.

    A finite one-shot whose dispatch was claimed (``repeat.completed`` >=
    ``repeat.times``) but which never reached ``mark_job_run`` (``last_run_at``
    is null) was interrupted mid-run — scheduler restart, gateway kill, or a
    non-Exception escape (#73973). The recovery guards remove such jobs so
    they stop appearing due, but a silent removal leaves the user with no
    output, no error, and no job record. Write a small diagnostic file into
    the job's output directory so the removal is observable and debuggable.

    Best-effort: diagnostics must never break the removal itself.
    """
    if job.get("last_run_at") is not None:
        return  # a prior run was recorded — normal completion race, not a wedge
    try:
        repeat = job.get("repeat") or {}
        claim = job.get("run_claim") or {}
        text = (
            "# Cron job removed without producing output\n\n"
            f"- job id: {job.get('id')}\n"
            f"- name: {job.get('name')}\n"
            f"- dispatch claimed: {repeat.get('completed', '?')}/{repeat.get('times', '?')}\n"
            f"- run claimed at: {claim.get('at', 'unknown')} by {claim.get('by', 'unknown')}\n"
            f"- removed at: {_hermes_now().isoformat()}\n\n"
            "This one-shot job's dispatch was claimed, but the run never "
            "completed (`last_run_at` was never written) — the scheduler "
            "process was most likely killed or restarted mid-execution. The "
            "job has been removed to stop it re-firing; recreate it to run "
            "again.\n"
        )
        save_job_output(job.get("id", ""), text)
        logger.warning(
            "Job '%s': removed without a completed run — diagnostic written to "
            "its output directory",
            job.get("name", job.get("id", "?")),
        )
    except Exception as e:
        logger.debug(
            "Failed to write wedged-oneshot diagnostic for job %r: %s",
            job.get("id"), e,
        )


def claim_operational_notice_delivery(
    job_id: str,
    idempotency_key: str,
    *,
    now_epoch: Optional[int] = None,
    lease_seconds: int = OPERATIONAL_NOTICE_CLAIM_LEASE_SECONDS,
) -> Dict[str, Any]:
    """Claim one notice with cross-process lease recovery and no capacity eviction."""
    key = str(idempotency_key or "").strip()
    if (
        not key
        or len(key) > 512
        or isinstance(lease_seconds, bool)
        or not isinstance(lease_seconds, int)
        or lease_seconds <= 0
        or lease_seconds > 86400
    ):
        return {"status": "invalid_key", "claimed": False}
    started = time.monotonic()
    epoch = int(time.time()) if now_epoch is None else int(now_epoch)
    owner = f"operational-notice-owner:{uuid.uuid4().hex}"
    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs()
        for job in jobs:
            if job.get("id") != job_id:
                continue
            receipts = job.get("operational_notice_receipts")
            receipts = dict(receipts) if isinstance(receipts, dict) else {}
            existing = receipts.get(key)
            if isinstance(existing, dict):
                status = str(existing.get("status") or "claimed")
                lease_expires = existing.get("lease_expires_at_epoch")
                if status not in {"claimed", "uncertain"} or (
                    status == "claimed"
                    and
                    isinstance(lease_expires, int) and epoch < lease_expires
                ):
                    return {"status": status, "claimed": False}
                recovery_count = int(existing.get("recovery_count") or 0) + 1
                transport_request_id = str(
                    existing.get("transport_request_id") or ""
                )
            else:
                recovery_count = 0
                transport_request_id = ""
            now = _hermes_now().isoformat()
            receipts[key] = {
                "status": "claimed",
                "claimed_at": now,
                "updated_at": now,
                "claim_owner": owner,
                "claim_heartbeat_at_epoch": epoch,
                "lease_expires_at_epoch": epoch + lease_seconds,
                "recovery_count": recovery_count,
                "transport_request_id": transport_request_id,
                "caller": "cron_scheduler",
                "parameters": {"idempotency_key": key},
                "result": {"status": "claimed"},
                "duration_ms": int((time.monotonic() - started) * 1000),
            }
            job["operational_notice_receipts"] = receipts
            _save_jobs_unlocked(jobs)
            return {
                "status": "claimed",
                "claimed": True,
                "claim_owner": owner,
                "recovered": recovery_count > 0,
                "transport_request_id": transport_request_id,
            }
    return {"status": "job_not_found", "claimed": False}


def bind_operational_notice_transport_request(
    job_id: str,
    idempotency_key: str,
    *,
    claim_owner: str,
    transport_request_id: str,
) -> Dict[str, Any]:
    """CAS-bind one stable outbox request to the current notice owner."""
    key = str(idempotency_key or "").strip()
    request_id = str(transport_request_id or "").strip()
    if not key or not request_id or len(request_id) > 512:
        return {"status": "invalid_request"}
    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs()
        for job in jobs:
            if job.get("id") != job_id:
                continue
            receipts = job.get("operational_notice_receipts")
            if not isinstance(receipts, dict) or not isinstance(receipts.get(key), dict):
                return {"status": "not_claimed"}
            current = receipts[key]
            if current.get("status") != "claimed" or current.get("claim_owner") != claim_owner:
                return {"status": "ownership_lost"}
            existing = str(current.get("transport_request_id") or "")
            if existing and existing != request_id:
                return {"status": "request_conflict"}
            current = {
                **current,
                "transport_request_id": request_id,
                "updated_at": _hermes_now().isoformat(),
            }
            receipts[key] = current
            job["operational_notice_receipts"] = receipts
            _save_jobs_unlocked(jobs)
            return {"status": "bound", "transport_request_id": request_id}
    return {"status": "job_not_found"}


def heartbeat_operational_notice_delivery(
    job_id: str,
    idempotency_key: str,
    *,
    claim_owner: str,
    transport_request_id: str = "",
    now_epoch: Optional[int] = None,
    lease_seconds: int = OPERATIONAL_NOTICE_CLAIM_LEASE_SECONDS,
) -> Dict[str, Any]:
    """Renew the current owner without permitting request identity drift."""
    key = str(idempotency_key or "").strip()
    request_id = str(transport_request_id or "").strip()
    epoch = int(time.time()) if now_epoch is None else int(now_epoch)
    if not key or lease_seconds <= 0 or lease_seconds > 86400:
        return {"status": "invalid_claim"}
    with _jobs_lock(require_cross_process=True):
        jobs = load_jobs()
        for job in jobs:
            if job.get("id") != job_id:
                continue
            receipts = job.get("operational_notice_receipts")
            if not isinstance(receipts, dict) or not isinstance(receipts.get(key), dict):
                return {"status": "not_claimed"}
            current = receipts[key]
            if current.get("status") != "claimed" or current.get("claim_owner") != claim_owner:
                return {"status": "ownership_lost"}
            bound_request = str(current.get("transport_request_id") or "")
            if request_id and bound_request and request_id != bound_request:
                return {"status": "request_conflict"}
            current = {
                **current,
                "claim_heartbeat_at_epoch": epoch,
                "lease_expires_at_epoch": epoch + lease_seconds,
                "updated_at": _hermes_now().isoformat(),
            }
            receipts[key] = current
            job["operational_notice_receipts"] = receipts
            _save_jobs_unlocked(jobs)
            return {
                "status": "claimed",
                "claim_owner": claim_owner,
                "lease_expires_at_epoch": current["lease_expires_at_epoch"],
                "transport_request_id": bound_request,
            }
    return {"status": "job_not_found"}


def mark_operational_notice_delivery(
    job_id: str,
    idempotency_key: str,
    status: str,
    *,
    claim_owner: str,
    transport_request_id: str = "",
    confirmed_transport_receipt_id: str = "",
) -> Dict[str, Any]:
    """CAS a terminal fact; uncertain remains recoverable and cannot hide sent."""
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
            if receipts[key].get("status") == "sent":
                return {"status": "sent"}
            if (
                receipts[key].get("status") != "claimed"
                or receipts[key].get("claim_owner") != claim_owner
            ):
                return {"status": "ownership_lost"}
            bound_request = str(receipts[key].get("transport_request_id") or "")
            supplied_request = str(transport_request_id or "")
            if status == "sent" and (
                not bound_request or bound_request != supplied_request
            ):
                return {"status": "request_mismatch"}
            if status != "sent" and (bound_request or supplied_request) and (
                bound_request != supplied_request
            ):
                return {"status": "request_mismatch"}
            if status == "sent" and not str(confirmed_transport_receipt_id or "").strip():
                return {"status": "confirmed_receipt_required"}
            if status == "uncertain":
                receipts[key] = {
                    **receipts[key],
                    "updated_at": _hermes_now().isoformat(),
                    "result": {"status": "uncertain"},
                    "duration_ms": int((time.monotonic() - started) * 1000),
                }
                job["operational_notice_receipts"] = receipts
                _save_jobs_unlocked(jobs)
                return {"status": "uncertain", "terminal": False}
            receipts[key] = {
                **receipts[key],
                "status": status,
                "updated_at": _hermes_now().isoformat(),
                "lease_expires_at_epoch": None,
                "result": {"status": status},
                "confirmed_transport_receipt_id": (
                    str(confirmed_transport_receipt_id)
                    if status == "sent"
                    else ""
                ),
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
    with _jobs_lock(require_cross_process=run_outcome_claim is not None):
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
                # Already dispatched the max number of times.
                if job.get("last_run_at") is not None:
                    # A prior run completed normally (e.g. mark_job_run raced
                    # with this tick). Retain the terminal record — same shape
                    # as mark_job_run's repeat-limit branch — instead of
                    # deleting the job and its final status/delivery error.
                    job["enabled"] = False
                    job["state"] = "completed"
                    job["next_run_at"] = None
                    save_jobs(jobs)
                    logger.info(
                        "Job '%s': dispatch limit reached (%d/%d) — marking completed",
                        job.get("name", job.get("id", "?")),
                        completed,
                        times,
                    )
                    return False
                # A prior tick claimed the dispatch then died before the run
                # completed (#73973) — a genuinely wedged claim. Remove it so
                # it stops appearing as due, and leave an operator-visible
                # diagnostic instead of vanishing silently.
                jobs.pop(i)
                save_jobs(jobs)
                _write_wedged_oneshot_diagnostic(job)
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
    with _jobs_lock():
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


def advance_next_runs(job_ids) -> int:
    """Batch form of :func:`advance_next_run` for the due-dispatch loop.

    One ``load_jobs()`` + at most one ``save_jobs()`` for the whole due
    set, instead of one of each per job — the per-job form costs
    O(N loads + N saves) for N due jobs (~110 ms at N=50, measured), the
    batch form O(1 + 1) (~2 ms). ``job_ids`` may contain ids of one-shot
    or unknown jobs; they are skipped exactly as the per-job form skips
    them. Returns the number of jobs whose ``next_run_at`` was advanced.

    Crash semantics: the batch persists once at the end, so a crash
    mid-batch re-fires the whole set on restart (at-least-once burst)
    rather than advancing a prefix — acceptable given the sub-10ms window,
    and identical to the per-job form once the batch completes.
    """
    ids = set(job_ids)
    if not ids:
        return 0
    with _jobs_lock():
        jobs = load_jobs()
        now = _hermes_now().isoformat()
        advanced = 0
        for job in jobs:
            if job["id"] not in ids:
                continue
            kind = job.get("schedule", {}).get("kind")
            if kind not in {"cron", "interval"}:
                continue
            new_next = compute_next_run(job["schedule"], now)
            if new_next and new_next != job.get("next_run_at"):
                job["next_run_at"] = new_next
                advanced += 1
        if advanced:
            save_jobs(jobs)
        return advanced


def advance_next_run(job_id: str) -> bool:
    """Preemptively advance next_run_at for a recurring job before execution.

    Call this BEFORE run_job() so that if the process crashes mid-execution,
    the job won't re-fire on the next gateway restart.  This converts the
    scheduler from at-least-once to at-most-once for recurring jobs — missing
    one run is far better than firing dozens of times in a crash loop.

    One-shot jobs are left unchanged so they can still retry on restart.

    Returns True if next_run_at was advanced, False otherwise.
    """
    # >= 1 (not == 1): a corrupted jobs file with duplicate ids advances
    # every matching record; the wrapper still reports the advance.
    return advance_next_runs([job_id]) >= 1


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
    with _jobs_lock():
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


# Completed one-shot job records are retained in jobs.json (final status +
# delivery error stay inspectable via `cronjob list`) instead of being deleted
# at completion, then pruned by _sweep_completed_oneshots once they age out.
COMPLETED_ONESHOT_RETENTION_DAYS = 7


def _completed_oneshot_retention_days() -> float:
    """Resolve the completed one-shot retention window from config.

    ``cron.completed_retention_days`` (number, default
    ``COMPLETED_ONESHOT_RETENTION_DAYS``). A non-positive value disables the
    sweep, retaining completed one-shot records indefinitely.
    """
    try:
        from hermes_cli.config import load_config
        cfg = load_config() or {}
        cron_cfg = cfg.get("cron", {}) if isinstance(cfg, dict) else {}
        return float(
            cron_cfg.get(
                "completed_retention_days", COMPLETED_ONESHOT_RETENTION_DAYS
            )
        )
    except Exception:
        return float(COMPLETED_ONESHOT_RETENTION_DAYS)


def _sweep_completed_oneshots(raw_jobs: List[Dict[str, Any]], now: datetime) -> bool:
    """Prune terminal ``state == "completed"`` one-shot records past retention.

    Mutates *raw_jobs* in place; returns True when anything was removed (the
    caller persists). Only one-shot (``schedule.kind == "once"``) records in
    the terminal completed state are candidates; recurring jobs and non-
    terminal one-shots are never touched. Age is measured from
    ``last_run_at`` — a completed record without a parseable ``last_run_at``
    is kept (never guess a record into deletion).
    """
    retention_days = _completed_oneshot_retention_days()
    if retention_days <= 0:
        return False
    cutoff = now - timedelta(days=retention_days)
    removed = False
    for rj in list(raw_jobs):
        try:
            if rj.get("state") != "completed":
                continue
            schedule = rj.get("schedule")
            kind = schedule.get("kind") if isinstance(schedule, dict) else None
            if kind != "once":
                continue
            last_run = rj.get("last_run_at")
            if not isinstance(last_run, str):
                continue
            try:
                last_run_dt = _ensure_aware(datetime.fromisoformat(last_run))
            except Exception:
                continue
            if last_run_dt >= cutoff:
                continue
            raw_jobs.remove(rj)
            removed = True
            logger.info(
                "Job '%s': pruning completed one-shot record "
                "(finished %s, retention %.1f days)",
                rj.get("name", rj.get("id", "?")),
                last_run,
                retention_days,
            )
        except Exception:
            logger.debug(
                "Retention sweep skipped malformed job record %r",
                rj.get("id", "?"),
                exc_info=True,
            )
    return removed


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
    with _jobs_lock():
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

    # Retention sweep: completed one-shots are retained (so their final
    # status / delivery error stay inspectable via `cronjob list`) instead of
    # being deleted on completion, but they must not accumulate in jobs.json
    # forever. Prune terminal one-shot records older than the retention
    # window each scan.
    if _sweep_completed_oneshots(raw_jobs, now):
        needs_save = True
        jobs = [j for j in jobs if any(rj.get("id") == j.get("id") for rj in raw_jobs)]

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
                        record_catch_up_occurrence()
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
                            # The claimed run never completed here by
                            # definition (last_run_at unwritten is what made
                            # the entry look due) — leave an operator-visible
                            # diagnostic instead of vanishing silently (#73973).
                            _write_wedged_oneshot_diagnostic(job)
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
    ensure_dirs()
    job_output_dir = _job_output_dir(job_id)
    job_output_dir.mkdir(parents=True, exist_ok=True)
    _secure_dir(job_output_dir)

    timestamp = _hermes_now().strftime("%Y-%m-%d_%H-%M-%S")
    output_file = job_output_dir / f"{timestamp}.md"

    fd, tmp_path = tempfile.mkstemp(dir=str(job_output_dir), suffix='.tmp', prefix='.output_')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(output)
            f.flush()
            os.fsync(f.fileno())
        atomic_replace(tmp_path, output_file)
        _secure_file(output_file)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    # Bound per-job output growth so long-running deploys don't fill the disk (#52383).
    _prune_job_output(job_output_dir, _cron_output_keep())

    return output_file


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

    with _jobs_lock():
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

            canonical_skills, skill_bindings = _resolve_skill_fields(new_skills)
            job["skills"] = canonical_skills
            job["skill"] = canonical_skills[0] if canonical_skills else None
            job["skill_bindings"] = skill_bindings
            changed = True

            rewrites.append({
                "job_id": job.get("id"),
                "job_name": job.get("name") or job.get("id"),
                "before": list(skills_before),
                "after": list(canonical_skills),
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


_SKILL_BINDING_MIGRATION_SCHEMA = "cron-skill-binding-migration/v1"


def _read_jobs_document(path: Path) -> Tuple[bytes, Dict[str, Any]]:
    if not path.exists():
        return b"", {"jobs": []}
    raw = path.read_bytes()
    try:
        document = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Cron database corrupted: {path} is not valid JSON") from exc
    if isinstance(document, list):
        document = {"jobs": document}
    if not isinstance(document, dict) or not isinstance(document.get("jobs"), list):
        raise RuntimeError(
            "Cron database corrupted: expected {'jobs': [...] } for skill migration"
        )
    return raw, document


def _skill_binding_state(job: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "skill": copy.deepcopy(job.get("skill")),
        "skills": copy.deepcopy(job.get("skills")),
        "skill_bindings": copy.deepcopy(job.get("skill_bindings")),
    }


def _resolved_skill_binding_state(
    job: Dict[str, Any], profile_home: Path
) -> Dict[str, Any]:
    selectors = _normalize_skill_list(job.get("skill"), job.get("skills"))
    canonical, bindings = _resolve_skill_fields(
        selectors,
        profile_home=profile_home,
    )
    return {
        "skill": canonical[0] if canonical else None,
        "skills": canonical,
        "skill_bindings": bindings,
    }


def plan_skill_binding_migration(
    profile_home: str | Path | None = None,
) -> Dict[str, Any]:
    """Build a read-only canonical-binding migration plan for one profile."""
    profile = Path(profile_home or _active_profile_home()).expanduser().resolve(
        strict=False
    )
    jobs_file = profile / "cron" / "jobs.json"
    raw, document = _read_jobs_document(jobs_file)
    changes: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for job in document["jobs"]:
        if not isinstance(job, dict):
            errors.append({"job_id": "", "reason": "job_record_invalid"})
            continue
        before = _skill_binding_state(job)
        selectors = _normalize_skill_list(job.get("skill"), job.get("skills"))
        if not selectors and "skill_bindings" not in job:
            continue
        try:
            after = _resolved_skill_binding_state(job, profile)
        except Exception as exc:
            errors.append(
                {
                    "job_id": str(job.get("id") or ""),
                    "reason": getattr(exc, "code", type(exc).__name__),
                    "detail": str(exc),
                }
            )
            continue
        if before != after:
            changes.append(
                {
                    "job_id": str(job.get("id") or ""),
                    "before": before,
                    "after": after,
                }
            )

    return {
        "schema_version": _SKILL_BINDING_MIGRATION_SCHEMA,
        "profile_home": str(profile),
        "jobs_file": str(jobs_file),
        "store_digest": "sha256:" + hashlib.sha256(raw).hexdigest(),
        "jobs_scanned": len(document["jobs"]),
        "changes": changes,
        "errors": errors,
        "applicable": bool(changes) and not errors,
    }


def _migration_already_applied(
    jobs: List[Dict[str, Any]], changes: List[Dict[str, Any]]
) -> bool:
    by_id = {
        str(job.get("id") or ""): job for job in jobs if isinstance(job, dict)
    }
    return bool(changes) and all(
        change.get("job_id") in by_id
        and _skill_binding_state(by_id[change["job_id"]]) == change.get("after")
        for change in changes
    )


def apply_skill_binding_migration(plan: Dict[str, Any]) -> Dict[str, Any]:
    """Apply one unchanged, error-free plan after a durable store backup."""
    if not isinstance(plan, dict) or plan.get("schema_version") != _SKILL_BINDING_MIGRATION_SCHEMA:
        raise ValueError("invalid skill binding migration plan")
    if plan.get("errors"):
        raise ValueError("skill binding migration plan contains resolution errors")

    profile = Path(str(plan.get("profile_home") or "")).expanduser().resolve(
        strict=False
    )
    jobs_file = profile / "cron" / "jobs.json"
    if str(jobs_file) != str(plan.get("jobs_file") or ""):
        raise ValueError("skill binding migration plan path mismatch")
    changes = plan.get("changes")
    if not isinstance(changes, list):
        raise ValueError("skill binding migration plan changes are invalid")
    if not changes:
        return {
            "status": "noop",
            "jobs_updated": 0,
            "backup_path": None,
        }

    with use_cron_store(profile), _jobs_lock():
        raw, document = _read_jobs_document(jobs_file)
        jobs = document["jobs"]
        if _migration_already_applied(jobs, changes):
            return {
                "status": "already_applied",
                "jobs_updated": 0,
                "backup_path": None,
            }

        current_digest = "sha256:" + hashlib.sha256(raw).hexdigest()
        if current_digest != plan.get("store_digest"):
            raise RuntimeError("cron jobs changed after the skill binding migration plan")

        changes_by_id = {change["job_id"]: change for change in changes}
        seen: set[str] = set()
        for job in jobs:
            if not isinstance(job, dict):
                continue
            job_id = str(job.get("id") or "")
            change = changes_by_id.get(job_id)
            if change is None:
                continue
            if _skill_binding_state(job) != change.get("before"):
                raise RuntimeError(f"cron job {job_id} changed after migration planning")
            if _resolved_skill_binding_state(job, profile) != change.get("after"):
                raise RuntimeError(
                    f"cron job {job_id} skill resolution changed after migration planning"
                )
            job.update(copy.deepcopy(change["after"]))
            seen.add(job_id)
        if seen != set(changes_by_id):
            raise RuntimeError("one or more planned cron jobs are missing")

        digest_token = str(plan["store_digest"]).split(":", 1)[-1][:16]
        backup_path = jobs_file.with_name(
            f"{jobs_file.name}.skill-bindings.{digest_token}.bak"
        )
        if backup_path.exists():
            if backup_path.read_bytes() != raw:
                raise RuntimeError("existing skill binding migration backup does not match")
        else:
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(backup_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                with os.fdopen(fd, "wb") as backup:
                    backup.write(raw)
                    backup.flush()
                    os.fsync(backup.fileno())
            except BaseException:
                try:
                    backup_path.unlink()
                except OSError:
                    pass
                raise

        original_stat = os.stat(jobs_file)
        try:
            _save_jobs_unlocked(jobs)
        except BaseException as save_exc:
            fd, restore_tmp = tempfile.mkstemp(
                dir=str(jobs_file.parent), suffix=".tmp", prefix=".jobs_restore_"
            )
            try:
                with os.fdopen(fd, "wb") as restored:
                    restored.write(raw)
                    restored.flush()
                    os.fsync(restored.fileno())
                atomic_replace(restore_tmp, jobs_file)
                _secure_file(jobs_file)
                _preserve_file_ownership(jobs_file, original_stat)
            except BaseException as restore_exc:
                try:
                    os.unlink(restore_tmp)
                except OSError:
                    pass
                raise RuntimeError(
                    "skill binding migration failed and rollback could not restore jobs.json"
                ) from restore_exc
            raise save_exc
        return {
            "status": "applied",
            "jobs_updated": len(changes),
            "backup_path": str(backup_path),
        }
