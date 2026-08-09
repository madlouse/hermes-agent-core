import json

from cron import jobs
from cron import scheduler


def _job():
    return {"id": "job-1", "profile_id": "atlas", "deliver": "local"}


def _notice():
    return {
        "status": "ready",
        "schema_version": "operational-notice/v1",
        "purpose": "operational_notification",
        "profile_id": "atlas",
        "classification": "restricted_operational_raw",
        "evidence_ref": "oe-0123456789abcdef0123",
        "fingerprint": "0123456789abcdef",
        "persist": True,
        "local_retrieval": "raw_owner_local",
        "expires_at": "2026-07-16T00:00:00+00:00",
        "source_target": {"transport_id": "feishu", "channel_id": "oc_group"},
        "target": {"transport_id": "feishu", "channel_id": "ou_admin", "chat_type": "dm"},
        "target_source": "runtime_safety.approval",
        "idempotency_key": "operational-notice:oe-0123456789abcdef0123:0123456789abcdef",
        "admin_content": "Restricted operational diagnostic; run run-1; ref oe-0123456789abcdef0123.",
        "user_content": "运行诊断包含受限内部信息，已脱敏。",
    }


def test_operational_notice_receipt_is_claimed_once_and_keeps_safe_state(tmp_path):
    with jobs.use_cron_store(tmp_path):
        jobs.save_jobs([_job()])
        key = _notice()["idempotency_key"]

        first = jobs.claim_operational_notice_delivery("job-1", key)
        duplicate = jobs.claim_operational_notice_delivery("job-1", key)
        terminal = jobs.mark_operational_notice_delivery("job-1", key, "uncertain")
        stored = jobs.load_jobs()[0]["operational_notice_receipts"][key]

    assert first == {"status": "claimed", "claimed": True}
    assert duplicate == {"status": "claimed", "claimed": False}
    assert terminal == {"status": "uncertain"}
    assert stored["status"] == "uncertain"
    assert stored["caller"] == "cron_scheduler"
    assert stored["parameters"] == {"idempotency_key": key}
    assert stored["result"] == {"status": "uncertain"}
    assert isinstance(stored["duration_ms"], int)
    assert "admin_content" not in json.dumps(stored)


def test_operational_notice_reuses_cron_delivery_once_without_raw_content(tmp_path, monkeypatch):
    sent = []
    notice = _notice()
    monkeypatch.setattr(
        scheduler,
        "_deliver_result",
        lambda job, content, adapters=None, loop=None: sent.append((job, content)) or None,
    )

    with jobs.use_cron_store(tmp_path):
        jobs.save_jobs([_job()])
        first = scheduler._deliver_operational_notice(_job(), {"operational_notice": notice})
        replay = scheduler._deliver_operational_notice(_job(), {"operational_notice": notice})
        stored = jobs.load_jobs()[0]["operational_notice_receipts"][notice["idempotency_key"]]

    assert first == "sent"
    assert replay == "sent"
    assert len(sent) == 1
    supplemental_job, content = sent[0]
    assert supplemental_job["deliver"] == "feishu:ou_admin"
    assert supplemental_job["_operational_notice_delivery"] == notice
    assert supplemental_job["_operational_notice_no_wrap"] is True
    assert supplemental_job["_operational_notice_no_mirror"] is True
    assert content == notice["admin_content"]
    assert "raw_text" not in json.dumps(supplemental_job)
    assert stored["status"] == "sent"


def test_operational_notice_does_not_send_when_receipt_cannot_be_claimed(monkeypatch):
    sent = []
    monkeypatch.setattr(
        scheduler,
        "claim_operational_notice_delivery",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("lock unavailable")),
    )
    monkeypatch.setattr(
        scheduler,
        "_deliver_result",
        lambda *args, **kwargs: sent.append((args, kwargs)),
    )

    result = scheduler._deliver_operational_notice(_job(), {"operational_notice": _notice()})

    assert result == "receipt_unavailable"
    assert sent == []
