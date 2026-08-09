from __future__ import annotations

import asyncio
import json
import sqlite3
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from cron import jobs, scheduler
from gateway.config import Platform
from gateway.outbound_boundary import BoundaryDecision
from tools import send_message_tool as send_module


def _job(*, profile_id: str = "default") -> dict:
    return {
        "id": "job-1",
        "profile_id": "untrusted-top-level-value",
        "deliver": "local",
        "schedule": {"kind": "interval", "minutes": 60},
        "repeat": {"times": None, "completed": 0},
        "enabled": True,
        "state": "scheduled",
        "creation_governance_receipt": {
            "schema_version": "cron-creation-governance/v1",
            "profile_id": profile_id,
            "cron_job_id": "job-1",
            "receipt_id": "sha256:" + "a" * 64,
        },
    }


def _notice(*, profile_id: str = "default") -> dict:
    return {
        "status": "ready",
        "schema_version": "operational-notice/v1",
        "purpose": "operational_notification",
        "profile_id": profile_id,
        "classification": "restricted_operational_raw",
        "evidence_ref": "oe-0123456789abcdef0123",
        "fingerprint": "0123456789abcdef",
        "persist": True,
        "local_retrieval": "raw_owner_local",
        "expires_at": "2099-07-16T00:00:00+00:00",
        "source_target": {"transport_id": "feishu", "channel_id": "oc_group"},
        "target": {
            "transport_id": "feishu",
            "channel_id": "ou_admin",
            "chat_type": "dm",
        },
        "target_source": "runtime_safety.approval",
        "idempotency_key": (
            "operational-notice:oe-0123456789abcdef0123:0123456789abcdef"
        ),
        "admin_content": (
            "Restricted operational diagnostic; run run-1; "
            "ref oe-0123456789abcdef0123."
        ),
        "user_content": "The restricted diagnostic was redacted.",
    }


@pytest.fixture
def operational_profile(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.delenv("HERMES_PROFILE_ID", raising=False)
    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: tmp_path)
    monkeypatch.setattr(
        scheduler,
        "_active_outbound_hooks",
        lambda: object(),
    )

    def allow(_hooks, context):
        return BoundaryDecision(
            transmit=True,
            decision="allow",
            content=str(context["content"]),
            reason="operational_notice_revalidated",
            raw={"decision": "allow", "reason": "operational_notice_revalidated"},
        )

    monkeypatch.setattr("gateway.outbound_boundary.outbound_before_send_sync", allow)
    pconfig = SimpleNamespace(enabled=True, token="test-token", extra={})
    config = SimpleNamespace(
        platforms={Platform.FEISHU: pconfig},
        get_home_channel=lambda _platform: None,
    )
    monkeypatch.setattr("gateway.config.load_gateway_config", lambda: config)
    monkeypatch.setattr("tools.interrupt.is_interrupted", lambda: False)
    monkeypatch.setattr("model_tools._run_async", lambda coro: asyncio.run(coro))
    monkeypatch.setattr(
        "gateway.mirror.mirror_to_session", lambda *_args, **_kwargs: False
    )
    with jobs.use_cron_store(tmp_path):
        jobs.save_jobs([_job()])
        yield tmp_path


def test_operational_notice_claim_has_owner_lease_and_stale_recovery(
    operational_profile,
):
    key = _notice()["idempotency_key"]
    with jobs.use_cron_store(operational_profile):
        first = jobs.claim_operational_notice_delivery(
            "job-1", key, now_epoch=1000, lease_seconds=10
        )
        duplicate = jobs.claim_operational_notice_delivery(
            "job-1", key, now_epoch=1005, lease_seconds=10
        )
        recovered = jobs.claim_operational_notice_delivery(
            "job-1", key, now_epoch=1011, lease_seconds=10
        )
        stale_mark = jobs.mark_operational_notice_delivery(
            "job-1", key, "sent", claim_owner=first["claim_owner"]
        )
        terminal = jobs.mark_operational_notice_delivery(
            "job-1", key, "uncertain", claim_owner=recovered["claim_owner"]
        )
        stored = jobs.load_jobs()[0]["operational_notice_receipts"][key]

    assert duplicate == {"status": "claimed", "claimed": False}
    assert recovered["recovered"] is True
    assert stale_mark == {"status": "ownership_lost"}
    assert terminal == {"status": "uncertain"}
    assert stored["recovery_count"] == 1
    assert "admin_content" not in json.dumps(stored)


def test_operational_notice_uses_screened_sender_and_transport_outbox_once(
    operational_profile, monkeypatch
):
    provider_send = AsyncMock(
        return_value={"success": True, "message_id": "om_notice_once"}
    )
    monkeypatch.setattr(send_module, "_send_to_platform", provider_send)

    with jobs.use_cron_store(operational_profile):
        first = scheduler._deliver_operational_notice(
            _job(), {"operational_notice": _notice()}
        )
        replay = scheduler._deliver_operational_notice(
            _job(), {"operational_notice": _notice()}
        )
        stored = jobs.load_jobs()[0]["operational_notice_receipts"]

    assert first == "sent"
    assert replay == "sent"
    provider_send.assert_awaited_once()
    assert stored[_notice()["idempotency_key"]]["status"] == "sent"
    conn = sqlite3.connect(operational_profile / "transport-outbox.sqlite3")
    try:
        assert conn.execute("SELECT count(*) FROM transport_outbox_requests").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM transport_outbox_receipts").fetchone()[0] == 1
    finally:
        conn.close()


def test_crash_after_confirmed_send_recovers_without_resending(
    operational_profile, monkeypatch
):
    provider_send = AsyncMock(
        return_value={"success": True, "message_id": "om_crash_once"}
    )
    monkeypatch.setattr(send_module, "_send_to_platform", provider_send)
    epoch = [1000]
    monkeypatch.setattr(jobs.time, "time", lambda: epoch[0])
    real_mark = scheduler.mark_operational_notice_delivery
    monkeypatch.setattr(
        scheduler,
        "mark_operational_notice_delivery",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("crash")),
    )

    with jobs.use_cron_store(operational_profile):
        first = scheduler._deliver_operational_notice(
            _job(), {"operational_notice": _notice()}
        )
        epoch[0] = 1400
        monkeypatch.setattr(scheduler, "mark_operational_notice_delivery", real_mark)
        recovered = scheduler._deliver_operational_notice(
            _job(), {"operational_notice": _notice()}
        )

    assert first == "uncertain"
    assert recovered == "sent"
    provider_send.assert_awaited_once()


def test_operational_notice_claims_are_not_capacity_evicted(operational_profile):
    with jobs.use_cron_store(operational_profile):
        for index in range(1000):
            result = jobs.claim_operational_notice_delivery(
                "job-1", f"notice-{index}", now_epoch=1000, lease_seconds=10
            )
            assert result["claimed"] is True
            jobs.mark_operational_notice_delivery(
                "job-1",
                f"notice-{index}",
                "sent",
                claim_owner=result["claim_owner"],
            )
        receipts = jobs.load_jobs()[0]["operational_notice_receipts"]

    assert len(receipts) == 1000
    assert receipts["notice-0"]["status"] == "sent"
    assert receipts["notice-999"]["status"] == "sent"


def test_operational_notice_claim_is_atomic_across_processes(operational_profile):
    code = """
import json, sys
from cron import jobs
with jobs.use_cron_store(sys.argv[1]):
    print(json.dumps(jobs.claim_operational_notice_delivery(
        'job-1', 'cross-process', now_epoch=1000, lease_seconds=30
    )))
"""
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", code, str(operational_profile)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    results = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=20)
        assert process.returncode == 0, stderr
        results.append(json.loads(stdout))

    assert sum(result["claimed"] is True for result in results) == 1
    assert sum(result["claimed"] is False for result in results) == 1


def test_operational_notice_rejects_top_level_or_receipt_profile_spoof(
    operational_profile, monkeypatch
):
    provider_send = AsyncMock(return_value={"success": True})
    monkeypatch.setattr(send_module, "_send_to_platform", provider_send)

    spoofed = _job(profile_id="atlas")
    spoofed["profile_id"] = "default"
    result = scheduler._deliver_operational_notice(
        spoofed, {"operational_notice": _notice(profile_id="atlas")}
    )

    assert result == "profile_unverified"
    provider_send.assert_not_awaited()


def test_named_profile_identity_comes_from_creation_receipt(tmp_path, monkeypatch):
    profile = tmp_path / "profiles" / "atlas"
    profile.mkdir(parents=True)
    monkeypatch.setenv("HERMES_PROFILE_ID", "atlas")
    signed = _job(profile_id="atlas")
    signed["profile_id"] = "spoofed-top-level"

    with jobs.use_cron_store(profile):
        jobs.save_jobs([signed])
        identity = jobs.cron_creation_profile_identity(signed)
        claim = jobs.begin_job_run_outcome(signed)

    assert identity["profile_id"] == "atlas"
    assert claim is not None
    assert claim["profile_id"] == "atlas"
