"""Core-owned durable receipts for claim-bound outbound transport sends.

The request and receipt tables are append-only.  Every row is authenticated
with a profile-local key stored outside the database, so editing the SQLite
file cannot manufacture a receipt that passes :func:`verify_transport_receipt`.
Verification is deliberately read-only: caller values select an exact stored
tuple; they never become evidence by themselves.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import sqlite3
import stat
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote

from hermes_constants import get_hermes_home


SCHEMA_VERSION = "transport-outbox/v1"
RECOVERY_SCHEMA_VERSION = "transport-outbox-recovery/v1"
OUTCOME_CONFIRMED = "confirmed"
OUTCOME_DEFINITIVELY_REJECTED = "definitively_rejected"
OUTCOME_INDETERMINATE = "indeterminate"
TRANSPORT_OUTCOMES = frozenset(
    {
        OUTCOME_CONFIRMED,
        OUTCOME_DEFINITIVELY_REJECTED,
        OUTCOME_INDETERMINATE,
    }
)
SELECTOR_FIELDS = frozenset(
    {
        "request_id",
        "profile_id",
        "frame_id",
        "notification_claim_id",
        "decision_route",
        "notification_route",
        "items_content_hash",
        "visible_content_sha256",
        "claim_created_at",
        "claim_expires_at",
    }
)

_DB_FILENAME = "transport-outbox.sqlite3"
_KEY_FILENAME = ".transport-outbox.key"
_LOCK = threading.RLock()
_NATIVE_ID_KEYS = (
    "outbox_id",
    "message_id",
    "messageId",
    "native_id",
    "native_ids",
    "message_ids",
    "continuation_message_ids",
    "id",
)
_NATIVE_ID_CONTAINERS = (
    "raw_response",
    "messages",
    "results",
    "items",
    "chunk_results",
    "transport_attempts",
)
_PROFILE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


class TransportOutboxError(RuntimeError):
    """A trusted transport record could not be safely created or read."""


def _db_path(home: Path | None = None) -> Path:
    return (home or get_hermes_home()) / _DB_FILENAME


def _key_path(home: Path | None = None) -> Path:
    return (home or get_hermes_home()) / _KEY_FILENAME


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def visible_content_sha256(content: str) -> str:
    """Return the hash Core binds to the exact user-visible send content."""
    return _sha256_text(str(content))


def _utc_now(now: datetime | None = None) -> datetime:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _canonical_timestamp(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise TransportOutboxError(f"{field} is required")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TransportOutboxError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise TransportOutboxError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def _clean_identifier(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise TransportOutboxError(f"{field} is required")
    if len(text) > 512 or "\x00" in text:
        raise TransportOutboxError(f"{field} is invalid")
    return text


def _route(value: Any, field: str) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise TransportOutboxError(f"{field} must be an object")
    transport_id = _clean_identifier(
        value.get("transport_id") or value.get("platform"),
        f"{field}.transport_id",
    ).lower()
    channel_id = _clean_identifier(
        value.get("channel_id") or value.get("chat_id"),
        f"{field}.channel_id",
    )
    thread_id = str(value.get("thread_id") or "").strip()
    if len(thread_id) > 512 or "\x00" in thread_id:
        raise TransportOutboxError(f"{field}.thread_id is invalid")
    return {
        "transport_id": transport_id,
        "channel_id": channel_id,
        "thread_id": thread_id,
    }


def _canonical_profile_id(home: Path) -> str:
    """Derive one profile identity from the canonical profile home path.

    ``HERMES_PROFILE_ID`` is only a consistency assertion. It must never be
    able to relabel a different home, because both request creation and receipt
    recovery use this identity as part of their trust boundary.
    """
    try:
        resolved = home.expanduser().resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise TransportOutboxError("active Hermes profile home is unavailable") from exc
    if not resolved.is_dir():
        raise TransportOutboxError("active Hermes profile home is not a directory")

    if resolved.parent.name == "profiles":
        profile_id = resolved.name
        if not _PROFILE_ID_RE.fullmatch(profile_id):
            raise TransportOutboxError("active Hermes profile path is not canonical")
    else:
        profile_id = "default"

    configured = os.getenv("HERMES_PROFILE_ID", "").strip()
    if configured and configured != profile_id:
        raise TransportOutboxError(
            "HERMES_PROFILE_ID does not match the canonical Hermes profile home"
        )
    return profile_id


def _profile_home_sha256(home: Path) -> str:
    return _sha256_text(str(home.expanduser().resolve()))


def _profile_owner(home: Path) -> tuple[int, int] | None:
    if os.name != "posix":
        return None
    try:
        info = home.expanduser().resolve(strict=True).stat()
    except OSError as exc:
        raise TransportOutboxError("active Hermes profile owner is unavailable") from exc
    return info.st_uid, info.st_gid


def _validate_profile_file(path: Path, home: Path, *, label: str) -> os.stat_result:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise TransportOutboxError(f"{label} is missing") from exc
    if stat.S_ISLNK(info.st_mode):
        raise TransportOutboxError(f"{label} must not be a symbolic link")
    if not stat.S_ISREG(info.st_mode):
        raise TransportOutboxError(f"{label} is not a regular file")
    owner = _profile_owner(home)
    if owner is not None and (info.st_uid, info.st_gid) != owner:
        raise TransportOutboxError(f"{label} has the wrong profile owner")
    if info.st_mode & 0o077:
        raise TransportOutboxError(f"{label} permissions are too broad")
    return info


def _secure_profile_file(path: Path, home: Path, *, label: str) -> None:
    """Make a Core state file private and usable by the profile owner."""
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISLNK(info.st_mode):
        raise TransportOutboxError(f"{label} must not be a symbolic link")
    if not stat.S_ISREG(info.st_mode):
        raise TransportOutboxError(f"{label} is not a regular file")
    owner = _profile_owner(home)
    if owner is not None and (info.st_uid, info.st_gid) != owner:
        geteuid = getattr(os, "geteuid", None)
        if geteuid is None or geteuid() != 0:
            raise TransportOutboxError(f"{label} has the wrong profile owner")
        try:
            os.chown(path, owner[0], owner[1])
        except OSError as exc:
            raise TransportOutboxError(
                f"{label} could not be assigned to the profile owner"
            ) from exc
    try:
        os.chmod(path, 0o600)
    except (OSError, NotImplementedError) as exc:
        if os.name == "posix":
            raise TransportOutboxError(f"{label} permissions could not be secured") from exc


def _secure_outbox_files(home: Path) -> None:
    for suffix, label in (
        ("", "transport outbox database"),
        ("-wal", "transport outbox WAL"),
        ("-shm", "transport outbox shared-memory file"),
    ):
        _secure_profile_file(Path(str(_db_path(home)) + suffix), home, label=label)


def _validate_existing_outbox_files(home: Path) -> None:
    for suffix, label in (
        ("", "transport outbox database"),
        ("-wal", "transport outbox WAL"),
        ("-shm", "transport outbox shared-memory file"),
    ):
        path = Path(str(_db_path(home)) + suffix)
        if path.exists() or path.is_symlink():
            _validate_profile_file(path, home, label=label)


def _normalize_request(
    request: Mapping[str, Any],
    *,
    visible_content: str,
    notification_route: Mapping[str, Any],
    home: Path,
    now: datetime,
) -> dict[str, Any]:
    if not isinstance(request, Mapping):
        raise TransportOutboxError("transport_request must be an object")
    normalized = {
        "schema_version": SCHEMA_VERSION,
        "request_id": _clean_identifier(request.get("request_id"), "request_id"),
        "profile_id": _clean_identifier(request.get("profile_id"), "profile_id"),
        "profile_home_sha256": _profile_home_sha256(home),
        "frame_id": _clean_identifier(request.get("frame_id"), "frame_id"),
        "notification_claim_id": _clean_identifier(
            request.get("notification_claim_id"), "notification_claim_id"
        ),
        "decision_route": _route(request.get("decision_route"), "decision_route"),
        "notification_route": _route(
            request.get("notification_route"), "notification_route"
        ),
        "items_content_hash": _clean_identifier(
            request.get("items_content_hash"), "items_content_hash"
        ),
        "visible_content_sha256": _clean_identifier(
            request.get("visible_content_sha256"), "visible_content_sha256"
        ).lower(),
        "claim_created_at": _canonical_timestamp(
            request.get("claim_created_at"), "claim_created_at"
        ),
        "claim_expires_at": _canonical_timestamp(
            request.get("claim_expires_at"), "claim_expires_at"
        ),
    }
    if normalized["profile_id"] != _canonical_profile_id(home):
        raise TransportOutboxError("profile_id does not match the active Hermes profile")
    actual_route = _route(notification_route, "resolved_notification_route")
    if normalized["notification_route"] != actual_route:
        raise TransportOutboxError("notification_route does not match the resolved send target")
    computed_visible_hash = visible_content_sha256(visible_content)
    if normalized["visible_content_sha256"] != computed_visible_hash:
        raise TransportOutboxError("visible_content_sha256 does not match the send content")
    claim_created = _parse_timestamp(normalized["claim_created_at"])
    claim_expires = _parse_timestamp(normalized["claim_expires_at"])
    if claim_created >= claim_expires:
        raise TransportOutboxError("claim timestamp interval is invalid")
    if now < claim_created or now >= claim_expires:
        raise TransportOutboxError("transport request is outside the claim timestamp interval")
    normalized["created_at"] = now.isoformat(timespec="microseconds")
    return normalized


def _secure_key(path: Path, home: Path, *, create: bool) -> bytes:
    created = False
    if create:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        try:
            fd = os.open(temporary, flags, 0o600)
            secret = os.urandom(32).hex().encode("ascii")
            try:
                os.write(fd, secret)
                os.fsync(fd)
            finally:
                os.close(fd)
            try:
                os.link(temporary, path)
                created = True
            except FileExistsError:
                pass
            try:
                parent_fd = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(parent_fd)
                finally:
                    os.close(parent_fd)
            except OSError:
                pass
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
    if created:
        _secure_profile_file(
            path, home, label="transport outbox integrity key"
        )
    _validate_profile_file(path, home, label="transport outbox integrity key")
    try:
        encoded = path.read_bytes()
        secret = bytes.fromhex(encoded.decode("ascii"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise TransportOutboxError("transport outbox integrity key is unreadable") from exc
    if len(secret) != 32:
        raise TransportOutboxError("transport outbox integrity key is invalid")
    return secret


def _initialize_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS transport_outbox_requests (
            request_id TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL,
            payload_sha256 TEXT NOT NULL,
            key_id TEXT NOT NULL,
            integrity_mac TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS transport_outbox_receipts (
            receipt_id TEXT PRIMARY KEY,
            request_id TEXT NOT NULL UNIQUE,
            payload_json TEXT NOT NULL,
            payload_sha256 TEXT NOT NULL,
            key_id TEXT NOT NULL,
            integrity_mac TEXT NOT NULL,
            FOREIGN KEY(request_id) REFERENCES transport_outbox_requests(request_id)
        );
        CREATE TRIGGER IF NOT EXISTS transport_outbox_requests_no_update
        BEFORE UPDATE ON transport_outbox_requests BEGIN
            SELECT RAISE(ABORT, 'transport outbox requests are append-only');
        END;
        CREATE TRIGGER IF NOT EXISTS transport_outbox_requests_no_delete
        BEFORE DELETE ON transport_outbox_requests BEGIN
            SELECT RAISE(ABORT, 'transport outbox requests are append-only');
        END;
        CREATE TRIGGER IF NOT EXISTS transport_outbox_receipts_no_update
        BEFORE UPDATE ON transport_outbox_receipts BEGIN
            SELECT RAISE(ABORT, 'transport outbox receipts are append-only');
        END;
        CREATE TRIGGER IF NOT EXISTS transport_outbox_receipts_no_delete
        BEFORE DELETE ON transport_outbox_receipts BEGIN
            SELECT RAISE(ABORT, 'transport outbox receipts are append-only');
        END;
        """
    )


def _connect_write(home: Path) -> sqlite3.Connection:
    path = _db_path(home)
    path.parent.mkdir(parents=True, exist_ok=True)
    _validate_existing_outbox_files(home)
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        _initialize_schema(conn)
        _secure_outbox_files(home)
    except Exception:
        conn.close()
        raise
    return conn


def _connect_read_only(home: Path) -> sqlite3.Connection:
    path = _db_path(home)
    _validate_profile_file(path, home, label="transport outbox database")
    _validate_existing_outbox_files(home)
    uri = f"file:{quote(str(path.resolve()))}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def _signed_row(payload: Mapping[str, Any], secret: bytes) -> tuple[str, str, str, str]:
    payload_json = _canonical_json(payload)
    payload_hash = _sha256_text(payload_json)
    key_id = hashlib.sha256(secret).hexdigest()[:16]
    mac = hmac.new(secret, payload_json.encode("utf-8"), hashlib.sha256).hexdigest()
    return payload_json, payload_hash, key_id, mac


def _verified_payload(row: sqlite3.Row, secret: bytes) -> dict[str, Any]:
    payload_json = str(row["payload_json"])
    payload_hash = _sha256_text(payload_json)
    key_id = hashlib.sha256(secret).hexdigest()[:16]
    mac = hmac.new(secret, payload_json.encode("utf-8"), hashlib.sha256).hexdigest()
    if not (
        hmac.compare_digest(payload_hash, str(row["payload_sha256"]))
        and hmac.compare_digest(key_id, str(row["key_id"]))
        and hmac.compare_digest(mac, str(row["integrity_mac"]))
    ):
        raise TransportOutboxError("transport outbox integrity verification failed")
    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError as exc:
        raise TransportOutboxError("transport outbox payload is invalid") from exc
    if not isinstance(payload, dict):
        raise TransportOutboxError("transport outbox payload is invalid")
    return payload


def _request_row(conn: sqlite3.Connection, request_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM transport_outbox_requests WHERE request_id = ?", (request_id,)
    ).fetchone()


def _receipt_row(conn: sqlite3.Connection, request_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM transport_outbox_receipts WHERE request_id = ?", (request_id,)
    ).fetchone()


def begin_transport_request(
    request: Mapping[str, Any],
    *,
    visible_content: str,
    notification_route: Mapping[str, Any],
    now: datetime | None = None,
    home: Path | None = None,
) -> dict[str, Any]:
    """Durably append one request before transport starts.

    A repeated exact request is never sent again automatically.  If it already
    has a confirmed receipt, callers may return that receipt idempotently;
    otherwise the prior attempt remains indeterminate or rejected.
    """
    resolved_home = home or get_hermes_home()
    moment = _utc_now(now)
    payload = _normalize_request(
        request,
        visible_content=visible_content,
        notification_route=notification_route,
        home=resolved_home,
        now=moment,
    )
    secret = _secure_key(_key_path(resolved_home), resolved_home, create=True)
    payload_json, payload_hash, key_id, mac = _signed_row(payload, secret)
    with _LOCK:
        conn = _connect_write(resolved_home)
        try:
            with conn:
                existing = _request_row(conn, payload["request_id"])
                if existing is None:
                    conn.execute(
                        "INSERT INTO transport_outbox_requests VALUES (?, ?, ?, ?, ?)",
                        (payload["request_id"], payload_json, payload_hash, key_id, mac),
                    )
                    return {"state": "new", "request": payload}
                stored = _verified_payload(existing, secret)
                comparable = dict(payload)
                comparable["created_at"] = stored.get("created_at")
                if stored != comparable:
                    raise TransportOutboxError("request_id already belongs to a different tuple")
                receipt_row = _receipt_row(conn, payload["request_id"])
                if receipt_row is None:
                    return {"state": "indeterminate", "request": stored}
                receipt = _verified_payload(receipt_row, secret)
                return {"state": receipt["status"], "request": stored, "receipt": receipt}
        finally:
            conn.close()
            _secure_outbox_files(resolved_home)


def extract_native_ids(result: Any) -> list[dict[str, str]]:
    """Normalize known provider result identifiers without transport coupling."""
    found: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def append(kind: str, value: Any) -> None:
        if isinstance(value, (str, int)) and not isinstance(value, bool):
            text = str(value).strip()
            pair = (kind, text)
            if text and pair not in seen:
                seen.add(pair)
                found.append({"kind": kind, "value": text})
        elif isinstance(value, (list, tuple)):
            for item in value:
                if isinstance(item, Mapping):
                    walk(item)
                else:
                    append(kind, item)

    def walk(value: Any) -> None:
        if isinstance(value, Mapping):
            for key in _NATIVE_ID_KEYS:
                if key in value:
                    append(key, value[key])
            for key in _NATIVE_ID_CONTAINERS:
                if key in value:
                    walk(value[key])
        elif isinstance(value, (list, tuple)):
            for item in value:
                walk(item)

    walk(result)
    return found


def classify_transport_outcome(result: Any) -> str:
    """Return the explicit Core outcome for one completed send attempt.

    Providers may set ``transport_outcome`` directly. Otherwise only a complete
    success confirms delivery; failures, timeouts, malformed results, and
    partial native-ID evidence remain indeterminate. A definitive rejection
    must be declared explicitly by the transport implementation.
    """
    if not isinstance(result, Mapping):
        return OUTCOME_INDETERMINATE
    explicit = str(result.get("transport_outcome") or "").strip()
    if explicit:
        if explicit not in TRANSPORT_OUTCOMES:
            raise TransportOutboxError("transport_outcome is invalid")
        return explicit
    native_ids = extract_native_ids(result)
    if result.get("success") is True:
        return OUTCOME_CONFIRMED
    if native_ids:
        return OUTCOME_INDETERMINATE
    return OUTCOME_INDETERMINATE


def commit_transport_receipt(
    request_id: str,
    result: Mapping[str, Any],
    *,
    outcome: str,
    now: datetime | None = None,
    home: Path | None = None,
) -> dict[str, Any]:
    """Append one explicit transport outcome after the provider returned."""
    resolved_home = home or get_hermes_home()
    selected_request_id = _clean_identifier(request_id, "request_id")
    outcome_value = str(outcome or "").strip()
    if outcome_value not in TRANSPORT_OUTCOMES:
        raise TransportOutboxError("transport outcome is invalid")
    if not isinstance(result, Mapping):
        raise TransportOutboxError("transport result must be an object")
    secret = _secure_key(
        _key_path(resolved_home), resolved_home, create=False
    )
    result_payload = dict(result) if isinstance(result, Mapping) else {}
    declared_outcome = str(result_payload.get("transport_outcome") or "").strip()
    if declared_outcome and declared_outcome != outcome_value:
        raise TransportOutboxError(
            "transport result outcome does not match the receipt outcome"
        )
    result_payload["transport_outcome"] = outcome_value
    native_ids = extract_native_ids(result_payload)
    with _LOCK:
        conn = _connect_write(resolved_home)
        try:
            with conn:
                request_row = _request_row(conn, selected_request_id)
                if request_row is None:
                    raise TransportOutboxError("transport request is missing")
                request_payload = _verified_payload(request_row, secret)
                receipt_moment = _utc_now(now)
                if outcome_value == OUTCOME_CONFIRMED and (
                    receipt_moment < _parse_timestamp(request_payload["created_at"])
                    or receipt_moment >= _parse_timestamp(request_payload["claim_expires_at"])
                ):
                    raise TransportOutboxError(
                        "confirmed transport receipt is outside the claim timestamp interval"
                    )
                receipt = {
                    "schema_version": SCHEMA_VERSION,
                    "receipt_id": f"transport-receipt:{uuid.uuid4().hex}",
                    "request_id": selected_request_id,
                    "request_payload_sha256": str(request_row["payload_sha256"]),
                    "status": outcome_value,
                    "native_ids": native_ids,
                    "transport_result_sha256": _sha256_text(_canonical_json(result_payload)),
                    "request_created_at": request_payload["created_at"],
                    "created_at": receipt_moment.isoformat(timespec="microseconds"),
                }
                existing = _receipt_row(conn, selected_request_id)
                if existing is not None:
                    stored = _verified_payload(existing, secret)
                    comparable_fields = (
                        "request_id",
                        "request_payload_sha256",
                        "status",
                        "native_ids",
                        "transport_result_sha256",
                        "request_created_at",
                    )
                    if all(stored.get(key) == receipt.get(key) for key in comparable_fields):
                        return {**stored, "idempotent": True}
                    raise TransportOutboxError("transport request already has a different receipt")
                payload_json, payload_hash, key_id, mac = _signed_row(receipt, secret)
                conn.execute(
                    "INSERT INTO transport_outbox_receipts VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        receipt["receipt_id"],
                        selected_request_id,
                        payload_json,
                        payload_hash,
                        key_id,
                        mac,
                    ),
                )
                return receipt
        finally:
            conn.close()
            _secure_outbox_files(resolved_home)


def _normalize_selector(selector: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(selector, Mapping):
        raise TransportOutboxError("selector must be an object")
    missing = SELECTOR_FIELDS - set(selector)
    if missing:
        raise TransportOutboxError(
            "selector is missing required fields: " + ", ".join(sorted(missing))
        )
    return {
        "request_id": _clean_identifier(selector.get("request_id"), "request_id"),
        "profile_id": _clean_identifier(selector.get("profile_id"), "profile_id"),
        "frame_id": _clean_identifier(selector.get("frame_id"), "frame_id"),
        "notification_claim_id": _clean_identifier(
            selector.get("notification_claim_id"), "notification_claim_id"
        ),
        "decision_route": _route(selector.get("decision_route"), "decision_route"),
        "notification_route": _route(
            selector.get("notification_route"), "notification_route"
        ),
        "items_content_hash": _clean_identifier(
            selector.get("items_content_hash"), "items_content_hash"
        ),
        "visible_content_sha256": _clean_identifier(
            selector.get("visible_content_sha256"), "visible_content_sha256"
        ).lower(),
        "claim_created_at": _canonical_timestamp(
            selector.get("claim_created_at"), "claim_created_at"
        ),
        "claim_expires_at": _canonical_timestamp(
            selector.get("claim_expires_at"), "claim_expires_at"
        ),
    }


def verify_transport_receipt(
    selector: Mapping[str, Any],
    *,
    now: datetime | None = None,
    home: Path | None = None,
) -> dict[str, Any]:
    """Read and verify one exact confirmed request/receipt tuple without writes."""
    resolved_home = home or get_hermes_home()
    try:
        expected = _normalize_selector(selector)
        canonical_profile_id = _canonical_profile_id(resolved_home)
        if expected["profile_id"] != canonical_profile_id:
            return {
                "status": "selector_mismatch",
                "verified": False,
                "reason": "wrong_profile_id",
            }
        secret = _secure_key(
            _key_path(resolved_home), resolved_home, create=False
        )
    except TransportOutboxError as exc:
        return {"status": "missing", "verified": False, "reason": str(exc)}
    try:
        conn = _connect_read_only(resolved_home)
    except TransportOutboxError as exc:
        return {"status": "missing", "verified": False, "reason": str(exc)}
    except (OSError, sqlite3.DatabaseError) as exc:
        return {"status": "integrity_failure", "verified": False, "reason": str(exc)}
    try:
        request_row = _request_row(conn, expected["request_id"])
        if request_row is None:
            return {"status": "missing", "verified": False, "reason": "request_not_found"}
        try:
            request = _verified_payload(request_row, secret)
        except TransportOutboxError as exc:
            return {"status": "integrity_failure", "verified": False, "reason": str(exc)}
        if request.get("profile_home_sha256") != _profile_home_sha256(resolved_home):
            return {
                "status": "integrity_failure",
                "verified": False,
                "reason": "wrong_profile_home",
            }
        for field, value in expected.items():
            if request.get(field) != value:
                return {
                    "status": "selector_mismatch",
                    "verified": False,
                    "reason": f"wrong_{field}",
                }
        moment = _utc_now(now)
        if moment >= _parse_timestamp(request["claim_expires_at"]):
            return {"status": "stale", "verified": False, "reason": "claim_expired"}
        receipt_row = _receipt_row(conn, expected["request_id"])
        if receipt_row is None:
            return {
                "status": OUTCOME_INDETERMINATE,
                "verified": False,
                "reason": "receipt_not_found",
                "request": request,
            }
        try:
            receipt = _verified_payload(receipt_row, secret)
        except TransportOutboxError as exc:
            return {"status": "integrity_failure", "verified": False, "reason": str(exc)}
        if receipt.get("request_id") != request["request_id"]:
            return {"status": "integrity_failure", "verified": False, "reason": "wrong_request_id"}
        if receipt.get("request_payload_sha256") != request_row["payload_sha256"]:
            return {
                "status": "integrity_failure",
                "verified": False,
                "reason": "request_receipt_binding_mismatch",
            }
        if receipt.get("request_created_at") != request.get("created_at"):
            return {
                "status": "integrity_failure",
                "verified": False,
                "reason": "request_timestamp_mismatch",
            }
        receipt_status = receipt.get("status")
        if receipt_status not in TRANSPORT_OUTCOMES:
            return {
                "status": "integrity_failure",
                "verified": False,
                "reason": "transport_outcome_invalid",
            }
        if receipt_status != OUTCOME_CONFIRMED:
            return {
                "status": receipt_status,
                "verified": False,
                "reason": "send_not_confirmed",
                "request": request,
                "receipt": receipt,
            }
        return {
            "status": "verified",
            "verified": True,
            "request": request,
            "receipt": receipt,
        }
    except (KeyError, TypeError, ValueError, OSError, sqlite3.DatabaseError) as exc:
        return {"status": "integrity_failure", "verified": False, "reason": str(exc)}
    finally:
        conn.close()


def recover_transport_request(
    selector: Mapping[str, Any],
    *,
    now: datetime | None = None,
    home: Path | None = None,
) -> dict[str, Any]:
    """Project verification into a stable, read-only HAK recovery contract."""
    verification = verify_transport_receipt(selector, now=now, home=home)
    status = str(verification.get("status") or "")
    if verification.get("verified"):
        outcome = OUTCOME_CONFIRMED
    elif status in {OUTCOME_DEFINITIVELY_REJECTED, OUTCOME_INDETERMINATE}:
        outcome = status
    else:
        outcome = "unavailable"
    result = {
        "schema_version": RECOVERY_SCHEMA_VERSION,
        "request_id": str(selector.get("request_id") or "")
        if isinstance(selector, Mapping)
        else "",
        "outcome": outcome,
        "trusted_confirmed_receipt": outcome == OUTCOME_CONFIRMED,
        "verification_status": status or "unavailable",
        "reason": verification.get("reason"),
    }
    if "request" in verification:
        result["request"] = verification["request"]
    if "receipt" in verification:
        result["receipt"] = verification["receipt"]
    return result


__all__ = [
    "SCHEMA_VERSION",
    "RECOVERY_SCHEMA_VERSION",
    "SELECTOR_FIELDS",
    "OUTCOME_CONFIRMED",
    "OUTCOME_DEFINITIVELY_REJECTED",
    "OUTCOME_INDETERMINATE",
    "TRANSPORT_OUTCOMES",
    "TransportOutboxError",
    "begin_transport_request",
    "classify_transport_outcome",
    "commit_transport_receipt",
    "extract_native_ids",
    "recover_transport_request",
    "verify_transport_receipt",
    "visible_content_sha256",
]
