from __future__ import annotations

import copy
import json
import os
import shutil
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from gateway.transport_outbox import (
    OUTCOME_CONFIRMED,
    OUTCOME_DEFINITIVELY_REJECTED,
    OUTCOME_INDETERMINATE,
    TransportOutboxError,
    begin_transport_request,
    classify_transport_outcome,
    commit_transport_receipt,
    recover_transport_request,
    verify_transport_receipt,
    visible_content_sha256,
)


NOW = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)
CONTENT = "Approve the exact pending operation"


def _request(request_id: str = "request-1", *, platform: str = "feishu") -> dict:
    return {
        "request_id": request_id,
        "profile_id": "atlas",
        "frame_id": "frame-1567",
        "notification_claim_id": "notification-claim:abc123",
        "decision_route": {
            "transport_id": platform,
            "channel_id": "admin-dm",
            "thread_id": "decision-thread",
        },
        "notification_route": {
            "transport_id": platform,
            "channel_id": "admin-dm",
            "thread_id": "notice-thread",
        },
        "items_content_hash": "sha256:items-1567",
        "visible_content_sha256": visible_content_sha256(CONTENT),
        "claim_created_at": (NOW - timedelta(minutes=1)).isoformat(),
        "claim_expires_at": (NOW + timedelta(minutes=30)).isoformat(),
    }


def _begin(home, request: dict | None = None, *, now: datetime = NOW):
    selected = request or _request()
    return begin_transport_request(
        selected,
        visible_content=CONTENT,
        notification_route=selected["notification_route"],
        now=now,
        home=home,
    )


@pytest.fixture
def profile_home(tmp_path, monkeypatch):
    home = tmp_path / "profiles" / "atlas"
    home.mkdir(parents=True)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_PROFILE_ID", "atlas")
    return home


@pytest.mark.parametrize(
    ("platform", "provider_result", "expected_ids"),
    [
        ("feishu", {"success": True, "message_id": "om_feishu"}, [("message_id", "om_feishu")]),
        ("qihu360teams", {"success": True, "outbox_id": "ob_360"}, [("outbox_id", "ob_360")]),
        ("telegram", {"success": True}, []),
        (
            "discord",
            {
                "success": True,
                "message_id": "discord-final",
                "continuation_message_ids": ["discord-1", "discord-2"],
            },
            [
                ("message_id", "discord-final"),
                ("continuation_message_ids", "discord-1"),
                ("continuation_message_ids", "discord-2"),
            ],
        ),
    ],
)
def test_transport_neutral_confirmed_receipts_support_native_id_shapes(
    profile_home, platform, provider_result, expected_ids
):
    request = _request(f"request-{platform}", platform=platform)
    assert _begin(profile_home, request)["state"] == "new"

    receipt = commit_transport_receipt(
        request["request_id"], provider_result, outcome=OUTCOME_CONFIRMED, now=NOW, home=profile_home
    )
    verified = verify_transport_receipt(request, now=NOW, home=profile_home)

    assert verified["status"] == "verified"
    assert verified["verified"] is True
    assert verified["receipt"]["receipt_id"] == receipt["receipt_id"]
    assert [(item["kind"], item["value"]) for item in receipt["native_ids"]] == expected_ids
    assert verified["request"]["profile_id"] == "atlas"
    assert verified["request"]["frame_id"] == "frame-1567"
    assert verified["request"]["notification_claim_id"] == "notification-claim:abc123"
    assert verified["request"]["decision_route"] == request["decision_route"]
    assert verified["request"]["notification_route"] == request["notification_route"]
    assert verified["request"]["items_content_hash"] == request["items_content_hash"]
    assert verified["request"]["visible_content_sha256"] == request["visible_content_sha256"]
    assert verified["request"]["created_at"]
    assert verified["receipt"]["created_at"]


@pytest.mark.parametrize(
    ("field", "wrong_value"),
    [
        ("profile_id", "yuange"),
        ("frame_id", "wrong-frame"),
        ("notification_claim_id", "notification-claim:wrong"),
        ("items_content_hash", "sha256:wrong-items"),
        ("visible_content_sha256", "0" * 64),
        ("claim_created_at", (NOW - timedelta(minutes=2)).isoformat()),
        ("claim_expires_at", (NOW + timedelta(minutes=31)).isoformat()),
    ],
)
def test_verification_rejects_wrong_exact_tuple_fields(profile_home, field, wrong_value):
    request = _request()
    _begin(profile_home, request)
    commit_transport_receipt("request-1", {"success": True}, outcome=OUTCOME_CONFIRMED, now=NOW, home=profile_home)
    selector = copy.deepcopy(request)
    selector[field] = wrong_value

    result = verify_transport_receipt(selector, now=NOW, home=profile_home)

    assert result["verified"] is False
    assert result["status"] == "selector_mismatch"
    assert result["reason"] == f"wrong_{field}"


@pytest.mark.parametrize("route_name", ["decision_route", "notification_route"])
def test_verification_rejects_wrong_routes(profile_home, route_name):
    request = _request()
    _begin(profile_home, request)
    commit_transport_receipt("request-1", {"success": True}, outcome=OUTCOME_CONFIRMED, now=NOW, home=profile_home)
    selector = copy.deepcopy(request)
    selector[route_name]["channel_id"] = "wrong-channel"

    result = verify_transport_receipt(selector, now=NOW, home=profile_home)

    assert result["status"] == "selector_mismatch"
    assert result["reason"] == f"wrong_{route_name}"


@pytest.mark.parametrize("table", ["transport_outbox_requests", "transport_outbox_receipts"])
def test_tampered_rows_fail_integrity_verification(profile_home, table):
    request = _request()
    _begin(profile_home, request)
    commit_transport_receipt("request-1", {"success": True}, outcome=OUTCOME_CONFIRMED, now=NOW, home=profile_home)
    db_path = profile_home / "transport-outbox.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"DROP TRIGGER {table}_no_update")
        row = conn.execute(f"SELECT rowid, payload_json FROM {table} LIMIT 1").fetchone()
        payload = json.loads(row[1])
        payload["profile_id" if table.endswith("requests") else "status"] = "forged"
        conn.execute(
            f"UPDATE {table} SET payload_json = ? WHERE rowid = ?",
            (json.dumps(payload, sort_keys=True), row[0]),
        )

    result = verify_transport_receipt(request, now=NOW, home=profile_home)

    assert result["status"] == "integrity_failure"
    assert result["verified"] is False


def test_append_only_triggers_reject_update_and_delete(profile_home):
    _begin(profile_home)
    db_path = profile_home / "transport-outbox.sqlite3"
    with sqlite3.connect(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            conn.execute(
                "UPDATE transport_outbox_requests SET payload_json = '{}' WHERE request_id = 'request-1'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            conn.execute("DELETE FROM transport_outbox_requests WHERE request_id = 'request-1'")


def test_missing_and_stale_receipts_never_verify(profile_home):
    request = _request()
    missing = verify_transport_receipt(request, now=NOW, home=profile_home)
    assert missing == {
        "status": "missing",
        "verified": False,
        "reason": "transport outbox integrity key is missing",
    }
    assert list(profile_home.iterdir()) == []

    _begin(profile_home, request)
    no_receipt = verify_transport_receipt(request, now=NOW, home=profile_home)
    assert no_receipt["status"] == OUTCOME_INDETERMINATE
    assert no_receipt["reason"] == "receipt_not_found"

    commit_transport_receipt("request-1", {"success": True}, outcome=OUTCOME_CONFIRMED, now=NOW, home=profile_home)
    stale = verify_transport_receipt(
        request, now=NOW + timedelta(hours=1), home=profile_home
    )
    assert stale["status"] == "stale"
    assert stale["verified"] is False


def test_corrupt_database_fails_closed_as_integrity_failure(profile_home):
    request = _request()
    _begin(profile_home, request)
    (profile_home / "transport-outbox.sqlite3").write_bytes(b"not a sqlite database")

    result = verify_transport_receipt(request, now=NOW, home=profile_home)

    assert result["status"] == "integrity_failure"
    assert result["verified"] is False


def test_symlinked_database_is_not_trusted(profile_home, tmp_path):
    request = _request()
    _begin(profile_home, request)
    real_db = tmp_path / "copied.sqlite3"
    shutil.copy2(profile_home / "transport-outbox.sqlite3", real_db)
    (profile_home / "transport-outbox.sqlite3").unlink()
    (profile_home / "transport-outbox.sqlite3").symlink_to(real_db)

    result = verify_transport_receipt(request, now=NOW, home=profile_home)

    assert result["status"] == "missing"
    assert result["verified"] is False
    assert "symbolic link" in result["reason"]


def test_duplicate_request_and_receipt_are_exactly_idempotent(profile_home):
    request = _request()
    first = _begin(profile_home, request)
    receipt = commit_transport_receipt(
        "request-1", {"success": True, "message_id": "m1"}, outcome=OUTCOME_CONFIRMED, now=NOW, home=profile_home
    )

    duplicate = _begin(profile_home, request, now=NOW + timedelta(seconds=1))
    duplicate_receipt = commit_transport_receipt(
        "request-1",
        {"success": True, "message_id": "m1"},
        outcome=OUTCOME_CONFIRMED,
        now=NOW + timedelta(seconds=2),
        home=profile_home,
    )

    assert first["state"] == "new"
    assert duplicate["state"] == "confirmed"
    assert duplicate["receipt"]["receipt_id"] == receipt["receipt_id"]
    assert duplicate_receipt["receipt_id"] == receipt["receipt_id"]
    assert duplicate_receipt["idempotent"] is True


def test_duplicate_pending_request_is_indeterminate_and_tuple_reuse_conflicts(profile_home):
    request = _request()
    _begin(profile_home, request)

    assert _begin(profile_home, request, now=NOW + timedelta(seconds=1))["state"] == "indeterminate"
    changed = copy.deepcopy(request)
    changed["frame_id"] = "different-frame"
    with pytest.raises(TransportOutboxError, match="different tuple"):
        _begin(profile_home, changed, now=NOW + timedelta(seconds=2))


def test_rejected_receipt_is_signed_but_not_verified_as_delivery(profile_home):
    request = _request()
    _begin(profile_home, request)
    commit_transport_receipt(
        "request-1",
        {"success": False, "error": "provider rejected"},
        outcome=OUTCOME_DEFINITIVELY_REJECTED,
        now=NOW,
        home=profile_home,
    )

    result = verify_transport_receipt(request, now=NOW, home=profile_home)

    assert result["status"] == OUTCOME_DEFINITIVELY_REJECTED
    assert result["verified"] is False


def test_request_validation_binds_active_profile_route_content_and_time(profile_home):
    request = _request()
    wrong_profile = copy.deepcopy(request)
    wrong_profile["profile_id"] = "yuange"
    with pytest.raises(TransportOutboxError, match="active Hermes profile"):
        _begin(profile_home, wrong_profile)

    wrong_route = copy.deepcopy(request)
    with pytest.raises(TransportOutboxError, match="resolved send target"):
        begin_transport_request(
            wrong_route,
            visible_content=CONTENT,
            notification_route={"transport_id": "feishu", "channel_id": "other"},
            now=NOW,
            home=profile_home,
        )

    wrong_hash = copy.deepcopy(request)
    wrong_hash["visible_content_sha256"] = "f" * 64
    with pytest.raises(TransportOutboxError, match="send content"):
        _begin(profile_home, wrong_hash)

    with pytest.raises(TransportOutboxError, match="outside the claim"):
        _begin(profile_home, request, now=NOW + timedelta(hours=1))


def test_confirmed_receipt_after_claim_expiry_is_not_committed(profile_home):
    request = _request()
    _begin(profile_home, request)

    with pytest.raises(TransportOutboxError, match="outside the claim"):
        commit_transport_receipt(
            "request-1",
            {"success": True, "message_id": "too-late"},
            outcome=OUTCOME_CONFIRMED,
            now=NOW + timedelta(hours=1),
            home=profile_home,
        )

    result = verify_transport_receipt(request, now=NOW, home=profile_home)
    assert result["reason"] == "receipt_not_found"
    assert result["status"] == OUTCOME_INDETERMINATE


def test_receipt_database_and_key_copied_to_another_profile_do_not_verify(
    profile_home, tmp_path, monkeypatch
):
    request = _request()
    _begin(profile_home, request)
    commit_transport_receipt(
        "request-1", {"success": True}, outcome=OUTCOME_CONFIRMED, now=NOW, home=profile_home
    )
    other = tmp_path / "profiles" / "yuange"
    other.mkdir()
    shutil.copy2(profile_home / "transport-outbox.sqlite3", other)
    shutil.copy2(profile_home / ".transport-outbox.key", other)
    monkeypatch.setenv("HERMES_HOME", str(other))
    monkeypatch.setenv("HERMES_PROFILE_ID", "yuange")

    copied_selector = copy.deepcopy(request)
    copied_selector["profile_id"] = "yuange"
    result = verify_transport_receipt(copied_selector, now=NOW, home=other)

    assert result["status"] == "integrity_failure"
    assert result["reason"] == "wrong_profile_home"


def test_environment_cannot_relabel_canonical_profile_on_begin_or_verify(
    profile_home, monkeypatch
):
    request = _request()
    _begin(profile_home, request)

    monkeypatch.setenv("HERMES_PROFILE_ID", "yuange")
    relabeled = copy.deepcopy(request)
    relabeled["profile_id"] = "yuange"
    with pytest.raises(TransportOutboxError, match="canonical Hermes profile home"):
        _begin(profile_home, relabeled)

    result = verify_transport_receipt(request, now=NOW, home=profile_home)
    assert result["verified"] is False
    assert "canonical Hermes profile home" in result["reason"]


def test_noncanonical_profile_path_is_rejected(tmp_path, monkeypatch):
    home = tmp_path / "profiles" / "Atlas"
    home.mkdir(parents=True)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_PROFILE_ID", "Atlas")
    request = _request()
    request["profile_id"] = "Atlas"

    with pytest.raises(TransportOutboxError, match="path is not canonical"):
        _begin(home, request)


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        ({"success": True}, OUTCOME_CONFIRMED),
        (
            {
                "success": False,
                "error": "forbidden",
                "transport_outcome": OUTCOME_DEFINITIVELY_REJECTED,
            },
            OUTCOME_DEFINITIVELY_REJECTED,
        ),
        (
            {
                "success": False,
                "error": "chunk 2 failed",
                "chunk_results": [
                    {"success": True, "message_id": "native-1"},
                    {"success": False, "error": "chunk 2 failed"},
                ],
            },
            OUTCOME_INDETERMINATE,
        ),
        ({"error": "timeout", "message_ids": ["native-1"]}, OUTCOME_INDETERMINATE),
        ({"success": False, "error": "timeout"}, OUTCOME_INDETERMINATE),
        ({}, OUTCOME_INDETERMINATE),
    ],
)
def test_transport_outcomes_are_explicit_and_partial_evidence_is_indeterminate(
    result, expected
):
    assert classify_transport_outcome(result) == expected


def test_indeterminate_partial_receipt_preserves_all_native_ids(profile_home):
    request = _request()
    _begin(profile_home, request)
    provider_result = {
        "success": False,
        "error": "final chunk failed",
        "chunk_results": [
            {"success": True, "message_id": "chunk-1"},
            {"success": True, "outbox_id": "chunk-2"},
            {"success": False, "error": "final chunk failed"},
        ],
    }

    receipt = commit_transport_receipt(
        request["request_id"],
        provider_result,
        outcome=OUTCOME_INDETERMINATE,
        now=NOW,
        home=profile_home,
    )
    recovered = recover_transport_request(request, now=NOW, home=profile_home)

    assert receipt["status"] == OUTCOME_INDETERMINATE
    assert {(item["kind"], item["value"]) for item in receipt["native_ids"]} == {
        ("message_id", "chunk-1"),
        ("outbox_id", "chunk-2"),
    }
    assert recovered["outcome"] == OUTCOME_INDETERMINATE
    assert recovered["trusted_confirmed_receipt"] is False
    assert recovered["receipt"]["receipt_id"] == receipt["receipt_id"]


@pytest.mark.parametrize(
    ("outcome", "trusted"),
    [
        (OUTCOME_CONFIRMED, True),
        (OUTCOME_DEFINITIVELY_REJECTED, False),
        (OUTCOME_INDETERMINATE, False),
    ],
)
def test_recovery_contract_only_trusts_confirmed_receipt(
    profile_home, outcome, trusted
):
    request = _request(f"request-recovery-{outcome}")
    _begin(profile_home, request)
    commit_transport_receipt(
        request["request_id"],
        {"success": outcome == OUTCOME_CONFIRMED},
        outcome=outcome,
        now=NOW,
        home=profile_home,
    )

    recovered = recover_transport_request(request, now=NOW, home=profile_home)

    assert recovered["schema_version"] == "transport-outbox-recovery/v1"
    assert recovered["outcome"] == outcome
    assert recovered["trusted_confirmed_receipt"] is trusted


def test_outbox_files_are_private_and_owned_by_profile_owner(profile_home):
    _begin(profile_home)
    paths = [
        profile_home / ".transport-outbox.key",
        profile_home / "transport-outbox.sqlite3",
    ]

    for path in paths:
        assert path.stat().st_mode & 0o777 == 0o600
        if hasattr(os, "getuid"):
            assert path.stat().st_uid == profile_home.stat().st_uid
            assert path.stat().st_gid == profile_home.stat().st_gid


@pytest.mark.skipif(os.name != "posix", reason="POSIX ownership and modes")
def test_wal_and_shm_are_secured_for_profile_owner(profile_home):
    import gateway.transport_outbox as outbox

    _begin(profile_home)
    db_path = profile_home / "transport-outbox.sqlite3"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("BEGIN IMMEDIATE")
        wal = profile_home / "transport-outbox.sqlite3-wal"
        shm = profile_home / "transport-outbox.sqlite3-shm"
        assert wal.exists()
        assert shm.exists()
        wal.chmod(0o644)
        shm.chmod(0o644)

        outbox._secure_outbox_files(profile_home)

        for path in (wal, shm):
            assert path.stat().st_mode & 0o777 == 0o600
            assert path.stat().st_uid == profile_home.stat().st_uid
            assert path.stat().st_gid == profile_home.stat().st_gid
    finally:
        conn.rollback()
        conn.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX ownership and modes")
def test_privileged_writer_assigns_files_to_profile_owner(
    profile_home, monkeypatch
):
    import gateway.transport_outbox as outbox

    path = profile_home / "owner-probe"
    path.write_text("probe")
    calls = []
    monkeypatch.setattr(outbox, "_profile_owner", lambda _home: (1234, 5678))
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    monkeypatch.setattr(
        os, "chown", lambda selected, uid, gid: calls.append((selected, uid, gid))
    )

    outbox._secure_profile_file(path, profile_home, label="owner probe")

    assert calls == [(path, 1234, 5678)]
    assert path.stat().st_mode & 0o777 == 0o600


def test_broad_database_permissions_fail_closed(profile_home):
    request = _request()
    _begin(profile_home, request)
    (profile_home / "transport-outbox.sqlite3").chmod(0o644)

    result = verify_transport_receipt(request, now=NOW, home=profile_home)

    assert result["verified"] is False
    assert "permissions are too broad" in result["reason"]
