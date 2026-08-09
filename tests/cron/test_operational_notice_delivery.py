from __future__ import annotations

import asyncio
import json
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from cron import jobs, scheduler
from gateway.config import Platform
from gateway.outbound_boundary import BoundaryDecision
from tools import send_message_tool as send_module


def _job(*, profile_id: str = "default", profile_home: Path | None = None) -> dict:
    receipt = {
        "schema_version": "cron-creation-governance/v1",
        "profile_id": profile_id,
        "cron_job_id": "job-1",
        "receipt_id": "sha256:" + "a" * 64,
    }
    if profile_home is not None:
        receipt["profile_home_sha256"] = jobs._cron_stable_hash(
            str(profile_home.resolve())
        )
    return {
        "id": "job-1",
        "profile_id": "untrusted-top-level-value",
        "deliver": "local",
        "schedule": {"kind": "interval", "minutes": 60},
        "repeat": {"times": None, "completed": 0},
        "enabled": True,
        "state": "scheduled",
        "creation_governance_receipt": receipt,
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
        jobs.save_jobs([_job(profile_home=tmp_path)])
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
    assert terminal == {"status": "uncertain", "terminal": False}
    assert stored["recovery_count"] == 1
    assert stored["status"] == "claimed"
    assert "admin_content" not in json.dumps(stored)


def test_confirmed_sent_truth_cannot_be_overwritten_by_uncertain(
    operational_profile,
):
    key = "confirmed-wins"
    with jobs.use_cron_store(operational_profile):
        claim = jobs.claim_operational_notice_delivery("job-1", key)
        request_id = "transport-request-confirmed-wins"
        jobs.bind_operational_notice_transport_request(
            "job-1",
            key,
            claim_owner=claim["claim_owner"],
            transport_request_id=request_id,
        )
        sent = jobs.mark_operational_notice_delivery(
            "job-1",
            key,
            "sent",
            claim_owner=claim["claim_owner"],
            transport_request_id=request_id,
            confirmed_transport_receipt_id="transport-receipt-confirmed-wins",
        )
        late_uncertain = jobs.mark_operational_notice_delivery(
            "job-1",
            key,
            "uncertain",
            claim_owner=claim["claim_owner"],
            transport_request_id=request_id,
        )
        stored = jobs.get_job("job-1")["operational_notice_receipts"][key]

    assert sent == {"status": "sent"}
    assert late_uncertain == {"status": "sent"}
    assert stored["status"] == "sent"
    assert stored["confirmed_transport_receipt_id"] == (
        "transport-receipt-confirmed-wins"
    )


def test_operational_notice_uses_screened_sender_and_transport_outbox_once(
    operational_profile, monkeypatch
):
    provider_send = AsyncMock(
        return_value={"success": True, "message_id": "om_notice_once"}
    )
    monkeypatch.setattr(send_module, "_send_to_platform", provider_send)

    with jobs.use_cron_store(operational_profile):
        first = scheduler._deliver_operational_notice(
            _job(profile_home=operational_profile), {"operational_notice": _notice()}
        )
        replay = scheduler._deliver_operational_notice(
            _job(profile_home=operational_profile), {"operational_notice": _notice()}
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
            _job(profile_home=operational_profile), {"operational_notice": _notice()}
        )
        epoch[0] = 1400
        monkeypatch.setattr(scheduler, "mark_operational_notice_delivery", real_mark)
        recovered = scheduler._deliver_operational_notice(
            _job(profile_home=operational_profile), {"operational_notice": _notice()}
        )

    assert first == "uncertain"
    assert recovered == "sent"
    provider_send.assert_awaited_once()


def test_cross_original_lease_two_thread_probe_converges_confirmed_sent(
    operational_profile, monkeypatch
):
    epoch = [1000]
    provider_started = threading.Event()
    release_provider = threading.Event()
    provider_calls = []
    monkeypatch.setattr(jobs.time, "time", lambda: epoch[0])
    monkeypatch.setattr(scheduler, "_OPERATIONAL_NOTICE_CLAIM_LEASE_SECONDS", 10)
    monkeypatch.setattr(scheduler, "_OPERATIONAL_NOTICE_HEARTBEAT_SECONDS", 0.01)

    async def provider_send(*args, **kwargs):
        provider_calls.append((args, kwargs))
        provider_started.set()
        while not release_provider.is_set():
            await asyncio.sleep(0.005)
        return {"success": True, "message_id": "om-cross-lease"}

    monkeypatch.setattr(send_module, "_send_to_platform", provider_send)
    results = []

    def deliver():
        with jobs.use_cron_store(operational_profile):
            results.append(
                scheduler._deliver_operational_notice(
                    _job(profile_home=operational_profile),
                    {"operational_notice": _notice()},
                )
            )

    first = threading.Thread(target=deliver)
    first.start()
    assert provider_started.wait(timeout=5)
    epoch[0] = 1011
    time.sleep(0.05)
    second = threading.Thread(target=deliver)
    second.start()
    second.join(timeout=5)
    release_provider.set()
    first.join(timeout=5)

    assert sorted(results) == ["claimed", "sent"]
    assert len(provider_calls) == 1
    with jobs.use_cron_store(operational_profile):
        stored = jobs.get_job("job-1")["operational_notice_receipts"]
    assert stored[_notice()["idempotency_key"]]["status"] == "sent"
    conn = sqlite3.connect(operational_profile / "transport-outbox.sqlite3")
    try:
        payload = conn.execute(
            "SELECT payload_json FROM transport_outbox_receipts"
        ).fetchone()[0]
    finally:
        conn.close()
    assert json.loads(payload)["status"] == "confirmed"


def test_notice_freezes_scoped_store_across_heartbeat_and_reconciliation(
    tmp_path, monkeypatch
):
    process_home = tmp_path / "process-home-a"
    owning_profile = tmp_path / "scoped-store-b"
    process_home.mkdir()
    owning_profile.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(process_home))
    monkeypatch.delenv("HERMES_PROFILE_ID", raising=False)
    monkeypatch.setattr(scheduler, "_active_outbound_hooks", lambda: object())

    def allow(_hooks, context):
        assert Path(context["profile_path"]) == owning_profile.resolve()
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

    epoch = [1000]
    provider_started = threading.Event()
    release_provider = threading.Event()
    provider_calls = []
    monkeypatch.setattr(jobs.time, "time", lambda: epoch[0])
    monkeypatch.setattr(scheduler, "_OPERATIONAL_NOTICE_CLAIM_LEASE_SECONDS", 10)
    monkeypatch.setattr(scheduler, "_OPERATIONAL_NOTICE_HEARTBEAT_SECONDS", 0.01)

    async def provider_send(*args, **kwargs):
        provider_calls.append((args, kwargs))
        provider_started.set()
        while not release_provider.is_set():
            await asyncio.sleep(0.005)
        return {"success": True, "message_id": "om-scoped-store-b"}

    monkeypatch.setattr(send_module, "_send_to_platform", provider_send)
    with jobs.use_cron_store(owning_profile):
        jobs.save_jobs([_job(profile_home=owning_profile)])

    results = []

    def deliver():
        with jobs.use_cron_store(owning_profile):
            results.append(
                scheduler._deliver_operational_notice(
                    _job(profile_home=owning_profile),
                    {"operational_notice": _notice()},
                )
            )

    first = threading.Thread(target=deliver)
    first.start()
    assert provider_started.wait(timeout=5)
    epoch[0] = 1011

    heartbeat_advanced = False
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        with jobs.use_cron_store(owning_profile):
            receipt = jobs.get_job("job-1")["operational_notice_receipts"][
                _notice()["idempotency_key"]
            ]
        if receipt["claim_heartbeat_at_epoch"] >= 1011:
            heartbeat_advanced = True
            break
        time.sleep(0.01)
    assert heartbeat_advanced is True

    second = threading.Thread(target=deliver)
    second.start()
    second.join(timeout=5)
    release_provider.set()
    first.join(timeout=5)

    assert first.is_alive() is False
    assert second.is_alive() is False
    assert sorted(results) == ["claimed", "sent"]
    assert len(provider_calls) == 1
    with jobs.use_cron_store(owning_profile):
        stored = jobs.get_job("job-1")["operational_notice_receipts"]
    assert stored[_notice()["idempotency_key"]]["status"] == "sent"
    assert not (process_home / "cron").exists()
    assert not (process_home / "transport-outbox.sqlite3").exists()
    assert (owning_profile / "transport-outbox.sqlite3").is_file()


def test_operational_notice_claims_are_not_capacity_evicted(operational_profile):
    with jobs.use_cron_store(operational_profile):
        for index in range(1000):
            result = jobs.claim_operational_notice_delivery(
                "job-1", f"notice-{index}", now_epoch=1000, lease_seconds=10
            )
            assert result["claimed"] is True
            request_id = f"transport-request-{index}"
            jobs.bind_operational_notice_transport_request(
                "job-1",
                f"notice-{index}",
                claim_owner=result["claim_owner"],
                transport_request_id=request_id,
            )
            jobs.mark_operational_notice_delivery(
                "job-1",
                f"notice-{index}",
                "sent",
                claim_owner=result["claim_owner"],
                transport_request_id=request_id,
                confirmed_transport_receipt_id=f"transport-receipt-{index}",
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

    spoofed = _job(profile_id="atlas", profile_home=operational_profile)
    spoofed["profile_id"] = "default"
    result = scheduler._deliver_operational_notice(
        spoofed, {"operational_notice": _notice(profile_id="atlas")}
    )

    assert result == "creation_receipt_profile_mismatch"
    provider_send.assert_not_awaited()


def test_named_profile_identity_comes_from_creation_receipt(tmp_path, monkeypatch):
    profile = tmp_path / "profiles" / "atlas"
    profile.mkdir(parents=True)
    monkeypatch.setenv("HERMES_PROFILE_ID", "atlas")
    signed = _job(profile_id="atlas", profile_home=profile)
    signed["profile_id"] = "spoofed-top-level"

    with jobs.use_cron_store(profile):
        jobs.save_jobs([signed])
        identity = jobs.cron_creation_profile_identity(signed)
        claim = jobs.begin_job_run_outcome(signed)

    assert identity["profile_id"] == "atlas"
    assert claim is not None
    assert claim["profile_id"] == "atlas"


def test_legacy_receipt_without_home_digest_requires_explicit_migration(
    tmp_path, monkeypatch
):
    monkeypatch.delenv("HERMES_PROFILE_ID", raising=False)
    legacy = _job()
    with jobs.use_cron_store(tmp_path):
        jobs.save_jobs([legacy])
        with pytest.raises(jobs.CronCreationProfileBindingError) as caught:
            jobs.cron_creation_profile_identity(legacy)
        claim = jobs.begin_job_run_outcome(legacy)

    assert caught.value.code == "creation_receipt_profile_home_migration_required"
    assert caught.value.migration_action == "refresh_creation_governance_receipt"
    assert claim is None


def test_same_default_profile_name_cannot_cross_canonical_roots(tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_PROFILE_ID", raising=False)
    root_a = tmp_path / "root-a"
    root_b = tmp_path / "root-b"
    root_a.mkdir()
    root_b.mkdir()
    signed = _job(profile_home=root_a)

    with jobs.use_cron_store(root_b):
        jobs.save_jobs([signed])
        with pytest.raises(jobs.CronCreationProfileBindingError) as caught:
            jobs.cron_creation_profile_identity(signed)
        claim = jobs.begin_job_run_outcome(signed)
        notice_status = scheduler._deliver_operational_notice(
            signed,
            {"operational_notice": _notice()},
        )

    assert caught.value.code == "creation_receipt_profile_home_mismatch"
    assert claim is None
    assert notice_status == "creation_receipt_profile_home_mismatch"
