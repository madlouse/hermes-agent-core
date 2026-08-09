"""Durable, profile-local lineage for rejected Cron resume recovery."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
import threading
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote


SCHEMA_VERSION = "cron-persist-recovery/v1"
_DB_FILENAME = "persist-recovery.sqlite3"
_LOCK = threading.RLock()


class CronPersistRecoveryStoreError(RuntimeError):
    """Recovery lineage could not be safely recorded or verified."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _payload_hash(payload_json: str) -> str:
    return f"sha256:{hashlib.sha256(payload_json.encode('utf-8')).hexdigest()}"


def _db_path(cron_dir: Path) -> Path:
    return cron_dir / _DB_FILENAME


def _owner(path: Path) -> tuple[int, int] | None:
    if os.name != "posix":
        return None
    info = path.stat()
    return info.st_uid, info.st_gid


def _validate_file(path: Path, cron_dir: Path) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
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


def _ensure_database_file(cron_dir: Path) -> Path:
    cron_dir.mkdir(parents=True, exist_ok=True)
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
            os.close(fd)
    try:
        os.chmod(path, 0o600)
    except (OSError, NotImplementedError) as exc:
        if os.name == "posix":
            raise CronPersistRecoveryStoreError(
                "Cron persist recovery database permissions could not be secured."
            ) from exc
    _validate_file(path, cron_dir)
    return path


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


def _connect_write(cron_dir: Path) -> sqlite3.Connection:
    path = _ensure_database_file(cron_dir)
    try:
        conn = sqlite3.connect(path, timeout=10)
        conn.row_factory = sqlite3.Row
        _initialize(conn)
        _validate_file(path, cron_dir)
        return conn
    except (OSError, sqlite3.DatabaseError) as exc:
        try:
            conn.close()
        except UnboundLocalError:
            pass
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery database is unavailable or corrupt."
        ) from exc


def _connect_read_only(cron_dir: Path) -> sqlite3.Connection:
    path = _db_path(cron_dir)
    _validate_file(path, cron_dir)
    if not path.exists():
        raise FileNotFoundError(path)
    uri = f"file:{quote(str(path.resolve()))}?mode=ro&immutable=1"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        return conn
    except sqlite3.DatabaseError as exc:
        raise CronPersistRecoveryStoreError(
            "Cron persist recovery database is unavailable or corrupt."
        ) from exc


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


def load_by_rejected_receipt(
    cron_dir: Path, rejected_receipt_id: str
) -> dict[str, Any] | None:
    """Return the immutable recovery bound to a rejected receipt, if any."""
    with _LOCK:
        try:
            conn = _connect_read_only(cron_dir)
        except FileNotFoundError:
            return None
        try:
            row = conn.execute(
                "SELECT * FROM cron_persist_recoveries WHERE rejected_receipt_id = ?",
                (rejected_receipt_id,),
            ).fetchone()
        except sqlite3.DatabaseError as exc:
            raise CronPersistRecoveryStoreError(
                "Cron persist recovery database is unavailable or corrupt."
            ) from exc
        finally:
            conn.close()
    return _verified_payload(row) if row is not None else None


def get_recovery(cron_dir: Path, recovery_id: str) -> dict[str, Any] | None:
    """Read one verified recovery by its deterministic identity."""
    with _LOCK:
        try:
            conn = _connect_read_only(cron_dir)
        except FileNotFoundError:
            return None
        try:
            row = conn.execute(
                "SELECT * FROM cron_persist_recoveries WHERE recovery_id = ?",
                (recovery_id,),
            ).fetchone()
        except sqlite3.DatabaseError as exc:
            raise CronPersistRecoveryStoreError(
                "Cron persist recovery database is unavailable or corrupt."
            ) from exc
        finally:
            conn.close()
    return _verified_payload(row) if row is not None else None


def record_recovery(cron_dir: Path, payload: Mapping[str, Any]) -> dict[str, Any]:
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
    with _LOCK:
        conn = _connect_write(cron_dir)
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
