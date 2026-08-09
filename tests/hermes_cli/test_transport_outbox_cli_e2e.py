from __future__ import annotations

import argparse
import json
from unittest.mock import AsyncMock

import pytest

from gateway.transport_outbox import visible_content_sha256
from hermes_cli import send_cmd, transport_outbox_cmd


CONTENT = "Approve this exact operation"


@pytest.fixture
def close_real_cli_loop():
    yield
    import model_tools

    loop = model_tools._tool_loop
    if loop is not None and not loop.is_closed():
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
    model_tools._tool_loop = None


def _send_args(request: dict):
    parser = argparse.ArgumentParser(prog="hermes")
    subparsers = parser.add_subparsers(dest="command")
    send_cmd.register_send_subparser(subparsers)
    return parser.parse_args(
        [
            "send",
            "--to",
            "feishu:oc_admin",
            CONTENT,
            "--transport-request-json",
            json.dumps(request),
            "--json",
        ]
    )


def _recover_args(request: dict):
    parser = argparse.ArgumentParser(prog="hermes")
    subparsers = parser.add_subparsers(dest="command")
    transport_outbox_cmd.register_transport_outbox_subparser(subparsers)
    return parser.parse_args(
        [
            "transport-outbox",
            "recover",
            "--selector-json",
            json.dumps(request),
            "--json",
        ]
    )


def test_real_cli_send_recover_and_duplicate_suppress_transport(
    tmp_path, monkeypatch, capsys, close_real_cli_loop
):
    home = tmp_path / "profiles" / "atlas"
    home.mkdir(parents=True)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_PROFILE_ID", "atlas")
    monkeypatch.setenv("FEISHU_APP_ID", "cli-e2e-app")
    monkeypatch.setenv("FEISHU_APP_SECRET", "cli-e2e-secret")
    final_transport = AsyncMock(
        return_value={"success": True, "message_id": "om_cli_native"}
    )
    monkeypatch.setattr("tools.send_message_tool._send_to_platform", final_transport)
    request = {
        "request_id": "request-cli-e2e",
        "profile_id": "atlas",
        "frame_id": "frame-cli-e2e",
        "notification_claim_id": "notification-claim:cli-e2e",
        "decision_route": {
            "transport_id": "feishu",
            "channel_id": "oc_admin",
            "thread_id": "",
        },
        "notification_route": {
            "transport_id": "feishu",
            "channel_id": "oc_admin",
            "thread_id": "",
        },
        "items_content_hash": "sha256:cli-e2e-items",
        "visible_content_sha256": visible_content_sha256(CONTENT),
        "claim_created_at": "2020-01-01T00:00:00+00:00",
        "claim_expires_at": "2099-01-01T00:00:00+00:00",
    }

    with pytest.raises(SystemExit) as first_exit:
        send_cmd.cmd_send(_send_args(request))
    first = json.loads(capsys.readouterr().out)
    assert first_exit.value.code == 0
    assert first["transport_outcome"] == "confirmed"

    with pytest.raises(SystemExit) as recover_exit:
        transport_outbox_cmd.cmd_transport_outbox(_recover_args(request))
    recovered = json.loads(capsys.readouterr().out)
    assert recover_exit.value.code == 0
    assert recovered["outcome"] == "confirmed"
    assert recovered["trusted_confirmed_receipt"] is True
    assert recovered["receipt"]["native_ids"] == [
        {"kind": "message_id", "value": "om_cli_native"}
    ]

    with pytest.raises(SystemExit) as duplicate_exit:
        send_cmd.cmd_send(_send_args(request))
    duplicate = json.loads(capsys.readouterr().out)
    assert duplicate_exit.value.code == 0
    assert duplicate["idempotent"] is True
    assert duplicate["message_id"] == "om_cli_native"
    final_transport.assert_awaited_once()
