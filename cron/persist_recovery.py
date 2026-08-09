"""Durable, profile-local lineage for rejected Cron resume recovery."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote


SCHEMA_VERSION = "cron-persist-recovery/v1"
REGISTRATION_SCHEMA_VERSION = "cron-persist-recovery-registration/v1"
DISPATCH_ACK_SCHEMA_VERSION = "cron-persist-recovery-dispatch-ack/v2"
_DB_FILENAME = "persist-recovery.sqlite3"
_LOCK = threading.RLock()
_DISPATCH_LEASE_SECONDS = 30.0
_DARWIN_ROOT_ALIASES = {
    Path("/var"): Path("/private/var"),
    Path("/tmp"): Path("/private/tmp"),
    Path("/etc"): Path("/private/etc"),
}


class CronPersistRecoveryStoreError(RuntimeError):
    """Recovery lineage could not be safely recorded or verified."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _payload_hash(payload_json: str) -> str:
    return f"sha256:{hashlib.sha256(payload_json.encode('utf-8')).hexdigest()}"


def _reject_symlink_components(path: Path) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        try:
            info = current.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode):
            raise CronPersistRecoveryStoreError(
                "Cron persist recovery path contains a symbolic link."
            )


def _canonicalize_trusted_root_alias(path: Path) -> Path:
    absolute = path.expanduser().absolute()
    if sys.platform != "darwin":
        return absolute
    for alias, target in _DARWIN_ROOT_ALIASES.items():
        try:
            relative = absolute.relative_to(alias)
        except ValueError:
            continue
        try:
            if alias.resolve(strict=True) != target.resolve(strict=True):
                return absolute
        except (OSError, RuntimeError):
            return absolute
        return target / relative
    return absolute


def _validated_paths(cron_dir: Path, profile_home: Path | None) -> tuple[Path, Path]:
    raw_home = _canonicalize_trusted_root_alias(Path(profile_home or cron_dir.parent))
    raw_cron = _canonicalize_trusted_root_alias(Path(cron_dir))
    _reject_symlink_components(raw_home)
    _reject_symlink_components(raw_cron)
    try:
        home = raw_home.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery profile home is unavailable."
        ) from exc
    if not home.is_dir():
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery profile home is not a directory."
        )
    if raw_cron.name != "cron":
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery directory is outside the profile store."
        )
    try:
        resolved_parent = raw_cron.parent.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery directory parent is unavailable."
        ) from exc
    if resolved_parent != home:
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery directory is outside the profile store."
        )
    if raw_cron.exists():
        try:
            resolved_cron = raw_cron.resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise CronPersistRecoveryStoreError(
                "Cron persist recovery directory is unavailable."
            ) from exc
        if resolved_cron.parent != home or resolved_cron.name != "cron":
            raise CronPersistRecoveryStoreError(
                "Cron persist recovery directory is outside the profile store."
            )
    return home, raw_cron


def _db_path(cron_dir: Path) -> Path:
    return cron_dir / _DB_FILENAME


def _owner(path: Path) -> tuple[int, int] | None:
    if os.name != "posix":
        return None
    info = path.stat()
    return info.st_uid, info.st_gid


def _validate_file(path: Path, cron_dir: Path) -> os.stat_result | None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(info.st_mode):
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery database must not be a symbolic link."
        )
    if not stat.S_ISREG(info.st_mode):
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery database is not a regular file."
        )
    try:
        expected_owner = _owner(cron_dir)
    except OSError as exc:
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery directory owner is unavailable."
        ) from exc
    if expected_owner is not None and (info.st_uid, info.st_gid) != expected_owner:
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery database has the wrong profile owner."
        )
    if info.st_mode & 0o077:
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery database permissions are too broad."
        )
    return info


def _open_guard(path: Path, *, writable: bool) -> tuple[int, os.stat_result]:
    flags = (os.O_RDWR if writable else os.O_RDONLY) | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
        info = os.fstat(fd)
    except OSError as exc:
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery database could not be safely opened."
        ) from exc
    if not stat.S_ISREG(info.st_mode):
        os.close(fd)
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery database is not a regular file."
        )
    return fd, info


def _verify_guard(path: Path, expected: os.stat_result) -> None:
    try:
        actual = path.lstat()
    except OSError as exc:
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery database changed while opening."
        ) from exc
    if (
        stat.S_ISLNK(actual.st_mode)
        or actual.st_dev != expected.st_dev
        or actual.st_ino != expected.st_ino
    ):
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery database changed while opening."
        )


def _ensure_database_file(
    cron_dir: Path, profile_home: Path | None
) -> tuple[Path, Path]:
    home, cron_dir = _validated_paths(cron_dir, profile_home)
    cron_dir.mkdir(parents=True, exist_ok=True)
    _reject_symlink_components(cron_dir)
    if cron_dir.resolve(strict=True).parent != home:
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery directory is outside the profile store."
        )
    path = _db_path(cron_dir)
    _validate_file(path, cron_dir)
    if not path.exists():
        flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(path, flags, 0o600)
        except FileExistsError:
            _validate_file(path, cron_dir)
        except OSError as exc:
            raise CronPersistRecoveryStoreError(
                "Cron persist recovery database could not be created."
            ) from exc
        else:
            try:
                os.fchmod(fd, 0o600)
            except (OSError, AttributeError) as exc:
                if os.name == "posix":
                    raise CronPersistRecoveryStoreError(
                        "Cron persist recovery database permissions could not be secured."
                    ) from exc
            finally:
                os.close(fd)
    _validate_file(path, cron_dir)
    return cron_dir, path


def _initialize(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA synchronous=FULL")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS cron_persist_recoveries (
            recovery_id TEXT PRIMARY KEY,
            rejected_receipt_id TEXT NOT NULL UNIQUE,
            request_id TEXT NOT NULL,
            profile_id TEXT NOT NULL,
            operation TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            payload_sha256 TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS cron_persist_recovery_dispatches (
            recovery_id TEXT PRIMARY KEY,
            disposition TEXT NOT NULL CHECK(
                disposition IN ('not_required', 'pending', 'claimed', 'dispatched')
            ),
            registration_json TEXT,
            registration_sha256 TEXT,
            effect_json TEXT,
            effect_sha256 TEXT,
            dispatch_key TEXT,
            claim_id TEXT,
            fence_token INTEGER NOT NULL DEFAULT 0,
            claimed_at REAL,
            claim_expires_at REAL,
            dispatched_at REAL,
            FOREIGN KEY(recovery_id) REFERENCES cron_persist_recoveries(recovery_id)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS cron_persist_recovery_request_receipt
            ON cron_persist_recoveries(
                profile_id, operation, request_id, rejected_receipt_id
            );
        CREATE TRIGGER IF NOT EXISTS cron_persist_recoveries_no_update
        BEFORE UPDATE ON cron_persist_recoveries BEGIN
            SELECT RAISE(ABORT, 'cron persist recovery rows are append-only');
        END;
        CREATE TRIGGER IF NOT EXISTS cron_persist_recoveries_no_delete
        BEFORE DELETE ON cron_persist_recoveries BEGIN
            SELECT RAISE(ABORT, 'cron persist recovery rows are append-only');
        END;
        """
    )
    columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(cron_persist_recovery_dispatches)")
    }
    if "dispatch_key" not in columns:
        conn.execute(
            "ALTER TABLE cron_persist_recovery_dispatches ADD COLUMN dispatch_key TEXT"
        )
    if "fence_token" not in columns:
        conn.execute(
            "ALTER TABLE cron_persist_recovery_dispatches "
            "ADD COLUMN fence_token INTEGER NOT NULL DEFAULT 0"
        )


def _connect_write(
    cron_dir: Path, profile_home: Path | None = None
) -> sqlite3.Connection:
    cron_dir, path = _ensure_database_file(cron_dir, profile_home)
    guard_fd, guard_info = _open_guard(path, writable=True)
    try:
        uri = f"file:{quote(str(path))}?mode=rw&nofollow=1"
        conn = sqlite3.connect(uri, uri=True, timeout=10)
        conn.row_factory = sqlite3.Row
        _verify_guard(path, guard_info)
        _initialize(conn)
        _validate_file(path, cron_dir)
        return conn
    except (OSError, sqlite3.DatabaseError, CronPersistRecoveryStoreError) as exc:
        try:
            conn.close()
        except UnboundLocalError:
            pass
        if isinstance(exc, CronPersistRecoveryStoreError):
            raise
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery database is unavailable or corrupt."
        ) from exc
    finally:
        os.close(guard_fd)


def _connect_read_only(
    cron_dir: Path, profile_home: Path | None = None
) -> sqlite3.Connection:
    _, cron_dir = _validated_paths(cron_dir, profile_home)
    path = _db_path(cron_dir)
    _validate_file(path, cron_dir)
    if not path.exists():
        raise FileNotFoundError(path)
    # Open read paths read-write so SQLite can acquire locks and roll back a
    # hot DELETE journal left by a crashed writer. query_only below prevents
    # application-level mutations after recovery completes.
    guard_fd, guard_info = _open_guard(path, writable=True)
    uri = f"file:{quote(str(path))}?mode=rw&nofollow=1"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=10)
        conn.row_factory = sqlite3.Row
        _verify_guard(path, guard_info)
        conn.execute("PRAGMA query_only=ON")
        return conn
    except (sqlite3.DatabaseError, CronPersistRecoveryStoreError) as exc:
        try:
            conn.close()
        except UnboundLocalError:
            pass
        if isinstance(exc, CronPersistRecoveryStoreError):
            raise
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery database is unavailable or corrupt."
        ) from exc
    finally:
        os.close(guard_fd)


def _verified_payload(row: sqlite3.Row) -> dict[str, Any]:
    payload_json = str(row["payload_json"])
    if _payload_hash(payload_json) != str(row["payload_sha256"]):
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery lineage integrity verification failed."
        )
    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError as exc:
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery lineage payload is invalid."
        ) from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery lineage payload is invalid."
        )
    selectors = {
        "recovery_id": row["recovery_id"],
        "rejected_receipt_id": row["rejected_receipt_id"],
        "request_id": row["request_id"],
        "profile_id": row["profile_id"],
        "operation": row["operation"],
    }
    if any(payload.get(key) != value for key, value in selectors.items()):
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery lineage selector mismatch."
        )
    return payload


def _dispatch_projection(conn: sqlite3.Connection, recovery_id: str) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM cron_persist_recovery_dispatches WHERE recovery_id = ?",
        (recovery_id,),
    ).fetchone()
    if row is None:
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery dispatch disposition is missing."
        )
    projection = {
        "disposition": str(row["disposition"]),
        "dispatch_key": str(row["dispatch_key"] or ""),
        "claim_id": str(row["claim_id"] or ""),
        "fence_token": int(row["fence_token"] or 0),
        "claimed_at": row["claimed_at"],
        "claim_expires_at": row["claim_expires_at"],
        "dispatched_at": row["dispatched_at"],
    }
    registration_json = row["registration_json"]
    effect_json = row["effect_json"]
    if registration_json is not None:
        if _payload_hash(str(registration_json)) != str(row["registration_sha256"]):
            raise CronPersistRecoveryStoreError(
                "Cron persist recovery registration integrity verification failed."
            )
        try:
            projection["registration"] = json.loads(str(registration_json))
        except json.JSONDecodeError as exc:
            raise CronPersistRecoveryStoreError(
                "Cron persist recovery registration is invalid."
            ) from exc
    if effect_json is not None:
        if _payload_hash(str(effect_json)) != str(row["effect_sha256"]):
            raise CronPersistRecoveryStoreError(
                "Cron persist recovery effect integrity verification failed."
            )
        try:
            projection["notification_effect"] = json.loads(str(effect_json))
        except json.JSONDecodeError as exc:
            raise CronPersistRecoveryStoreError(
                "Cron persist recovery effect is invalid."
            ) from exc
    return projection


def load_by_rejected_receipt(
    cron_dir: Path,
    rejected_receipt_id: str,
    *,
    profile_home: Path | None = None,
) -> dict[str, Any] | None:
    """Return the immutable recovery bound to a rejected receipt, if any."""
    with _LOCK:
        try:
            conn = _connect_read_only(cron_dir, profile_home)
        except FileNotFoundError:
            return None
        try:
            row = conn.execute(
                "SELECT * FROM cron_persist_recoveries WHERE rejected_receipt_id = ?",
                (rejected_receipt_id,),
            ).fetchone()
            payload = _verified_payload(row) if row is not None else None
            if payload is not None:
                payload["dispatch"] = _dispatch_projection(
                    conn, str(payload["recovery_id"])
                )
        except sqlite3.DatabaseError as exc:
            raise CronPersistRecoveryStoreError(
                "Cron persist recovery database is unavailable or corrupt."
            ) from exc
        finally:
            conn.close()
    return payload


def get_recovery(
    cron_dir: Path, recovery_id: str, *, profile_home: Path | None = None
) -> dict[str, Any] | None:
    """Read one verified recovery by its deterministic identity."""
    with _LOCK:
        try:
            conn = _connect_read_only(cron_dir, profile_home)
        except FileNotFoundError:
            return None
        try:
            row = conn.execute(
                "SELECT * FROM cron_persist_recoveries WHERE recovery_id = ?",
                (recovery_id,),
            ).fetchone()
            payload = _verified_payload(row) if row is not None else None
            if payload is not None:
                payload["dispatch"] = _dispatch_projection(conn, recovery_id)
        except sqlite3.DatabaseError as exc:
            raise CronPersistRecoveryStoreError(
                "Cron persist recovery database is unavailable or corrupt."
            ) from exc
        finally:
            conn.close()
    return payload


def record_recovery(
    cron_dir: Path,
    payload: Mapping[str, Any],
    *,
    profile_home: Path | None = None,
) -> dict[str, Any]:
    """Insert one immutable recovery or return its byte-identical replay."""
    normalized = dict(payload)
    if normalized.get("schema_version") != SCHEMA_VERSION:
        raise CronPersistRecoveryStoreError("Cron persist recovery schema is invalid.")
    required = (
        "recovery_id",
        "rejected_receipt_id",
        "request_id",
        "profile_id",
        "operation",
    )
    if any(not str(normalized.get(field) or "").strip() for field in required):
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery lineage is incomplete."
        )
    payload_json = _canonical_json(normalized)
    payload_sha256 = _payload_hash(payload_json)
    registration = normalized.get("registration")
    effect = normalized.get("notification_effect")
    if (registration is None) != (effect is None):
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery dispatch registration is incomplete."
        )
    if registration is not None and (
        not isinstance(registration, Mapping) or not isinstance(effect, Mapping)
    ):
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery dispatch registration is invalid."
        )
    registration_json = (
        _canonical_json(registration) if isinstance(registration, Mapping) else None
    )
    effect_json = _canonical_json(effect) if isinstance(effect, Mapping) else None
    dispatch_key = (
        str(registration.get("dispatch_key") or "").strip()
        if isinstance(registration, Mapping)
        else None
    )
    if registration_json is not None and not dispatch_key:
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery dispatch key is missing."
        )
    with _LOCK:
        conn = _connect_write(cron_dir, profile_home)
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT * FROM cron_persist_recoveries WHERE rejected_receipt_id = ?",
                (normalized["rejected_receipt_id"],),
            ).fetchone()
            if existing is not None:
                verified = _verified_payload(existing)
                if _canonical_json(verified) != payload_json:
                    raise CronPersistRecoveryStoreError(
                        "Rejected Cron resume receipt has conflicting recovery lineage."
                    )
                conn.rollback()
                return verified
            conn.execute(
                """
                INSERT INTO cron_persist_recoveries(
                    recovery_id, rejected_receipt_id, request_id,
                    profile_id, operation, payload_json, payload_sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized["recovery_id"],
                    normalized["rejected_receipt_id"],
                    normalized["request_id"],
                    normalized["profile_id"],
                    normalized["operation"],
                    payload_json,
                    payload_sha256,
                ),
            )
            conn.execute(
                """
                INSERT INTO cron_persist_recovery_dispatches(
                    recovery_id, disposition,
                    registration_json, registration_sha256,
                    effect_json, effect_sha256, dispatch_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized["recovery_id"],
                    "pending" if registration_json is not None else "not_required",
                    registration_json,
                    _payload_hash(registration_json) if registration_json else None,
                    effect_json,
                    _payload_hash(effect_json) if effect_json else None,
                    dispatch_key,
                ),
            )
            conn.commit()
        except CronPersistRecoveryStoreError:
            conn.rollback()
            raise
        except sqlite3.DatabaseError as exc:
            conn.rollback()
            raise CronPersistRecoveryStoreError(
                "Cron persist recovery database rejected the lineage record."
            ) from exc
        finally:
            conn.close()
        _validate_file(_db_path(cron_dir), cron_dir)
    return normalized


def claim_recovery_dispatch(
    cron_dir: Path,
    recovery_id: str,
    *,
    profile_home: Path | None = None,
    now: float | None = None,
    lease_seconds: float = _DISPATCH_LEASE_SECONDS,
) -> dict[str, Any] | None:
    """Claim a pending or expired recovery observer dispatch."""
    claimed_at = float(time.time() if now is None else now)
    expires_at = claimed_at + max(float(lease_seconds), 0.001)
    claim_id = f"crd_{uuid.uuid4().hex}"
    with _LOCK:
        conn = _connect_write(cron_dir, profile_home)
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM cron_persist_recovery_dispatches WHERE recovery_id = ?",
                (recovery_id,),
            ).fetchone()
            if row is None:
                raise CronPersistRecoveryStoreError(
                    "Cron persist recovery dispatch disposition is missing."
                )
            disposition = str(row["disposition"])
            if disposition in {"not_required", "dispatched"}:
                conn.rollback()
                return None
            if (
                disposition == "claimed"
                and float(row["claim_expires_at"] or 0.0) > claimed_at
            ):
                conn.rollback()
                return None
            conn.execute(
                """
                UPDATE cron_persist_recovery_dispatches
                SET disposition = 'claimed', claim_id = ?, claimed_at = ?,
                    claim_expires_at = ?, dispatched_at = NULL,
                    fence_token = fence_token + 1
                WHERE recovery_id = ?
                """,
                (claim_id, claimed_at, expires_at, recovery_id),
            )
            projection = _dispatch_projection(conn, recovery_id)
            conn.commit()
        except CronPersistRecoveryStoreError:
            conn.rollback()
            raise
        except sqlite3.DatabaseError as exc:
            conn.rollback()
            raise CronPersistRecoveryStoreError(
                "Cron persist recovery dispatch could not be claimed."
            ) from exc
        finally:
            conn.close()
    registration = projection.get("registration")
    effect = projection.get("notification_effect")
    dispatch_key = str(projection.get("dispatch_key") or "")
    fence_token = int(projection.get("fence_token") or 0)
    if (
        not isinstance(registration, dict)
        or not isinstance(effect, dict)
        or not dispatch_key
        or fence_token < 1
    ):
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery claimed dispatch is incomplete."
        )
    return {
        "recovery_id": recovery_id,
        "dispatch_key": dispatch_key,
        "claim_id": claim_id,
        "fence_token": fence_token,
        "claimed_at": claimed_at,
        "claim_expires_at": expires_at,
        "lease_seconds": max(float(lease_seconds), 0.001),
        "registration": registration,
        "notification_effect": effect,
    }


def heartbeat_recovery_dispatch(
    cron_dir: Path,
    recovery_id: str,
    claim_id: str,
    fence_token: int,
    *,
    profile_home: Path | None = None,
    now: float | None = None,
    lease_seconds: float = _DISPATCH_LEASE_SECONDS,
) -> bool:
    """Extend one live fenced claim so a slow observer cannot be re-entered."""
    heartbeat_at = float(time.time() if now is None else now)
    expires_at = heartbeat_at + max(float(lease_seconds), 0.001)
    with _LOCK:
        conn = _connect_write(cron_dir, profile_home)
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """
                UPDATE cron_persist_recovery_dispatches
                SET claim_expires_at = ?
                WHERE recovery_id = ? AND disposition = 'claimed'
                    AND claim_id = ? AND fence_token = ?
                """,
                (expires_at, recovery_id, claim_id, int(fence_token)),
            )
            conn.commit()
            return cursor.rowcount == 1
        except sqlite3.DatabaseError as exc:
            conn.rollback()
            raise CronPersistRecoveryStoreError(
                "Cron persist recovery dispatch heartbeat failed."
            ) from exc
        finally:
            conn.close()


def complete_recovery_dispatch(
    cron_dir: Path,
    recovery_id: str,
    claim_id: str,
    fence_token: int,
    *,
    profile_home: Path | None = None,
    now: float | None = None,
) -> None:
    """Seal one explicitly acknowledged recovery observer dispatch."""
    completed_at = float(time.time() if now is None else now)
    with _LOCK:
        conn = _connect_write(cron_dir, profile_home)
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """
                UPDATE cron_persist_recovery_dispatches
                SET disposition = 'dispatched', dispatched_at = ?,
                    claim_expires_at = NULL
                WHERE recovery_id = ? AND disposition = 'claimed'
                    AND claim_id = ? AND fence_token = ?
                """,
                (completed_at, recovery_id, claim_id, int(fence_token)),
            )
            if cursor.rowcount != 1:
                raise CronPersistRecoveryStoreError(
                    "Cron persist recovery dispatch claim no longer matches."
                )
            conn.commit()
        except CronPersistRecoveryStoreError:
            conn.rollback()
            raise
        except sqlite3.DatabaseError as exc:
            conn.rollback()
            raise CronPersistRecoveryStoreError(
                "Cron persist recovery dispatch could not be completed."
            ) from exc
        finally:
            conn.close()


def release_recovery_dispatch(
    cron_dir: Path,
    recovery_id: str,
    claim_id: str,
    fence_token: int,
    *,
    profile_home: Path | None = None,
) -> None:
    """Return an unacknowledged claim to pending for deterministic retry."""
    with _LOCK:
        conn = _connect_write(cron_dir, profile_home)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                UPDATE cron_persist_recovery_dispatches
                SET disposition = 'pending', claim_id = NULL,
                    claimed_at = NULL, claim_expires_at = NULL
                WHERE recovery_id = ? AND disposition = 'claimed'
                    AND claim_id = ? AND fence_token = ?
                """,
                (recovery_id, claim_id, int(fence_token)),
            )
            conn.commit()
        except sqlite3.DatabaseError as exc:
            conn.rollback()
            raise CronPersistRecoveryStoreError(
                "Cron persist recovery dispatch claim could not be released."
            ) from exc
        finally:
            conn.close()
