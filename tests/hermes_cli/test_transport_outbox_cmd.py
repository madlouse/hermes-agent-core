from __future__ import annotations

import argparse
import json

import pytest

from hermes_cli import transport_outbox_cmd


def _parse(argv):
    parser = argparse.ArgumentParser(prog="hermes")
    subparsers = parser.add_subparsers(dest="command")
    transport_outbox_cmd.register_transport_outbox_subparser(subparsers)
    return parser.parse_args(["transport-outbox", *argv])


def test_verify_cli_passes_selector_to_read_only_api(monkeypatch, capsys):
    selector = {
        "request_id": "request-1",
        "profile_id": "atlas",
    }
    observed = []
    monkeypatch.setattr(
        "gateway.transport_outbox.verify_transport_receipt",
        lambda value: observed.append(value)
        or {
            "status": "verified",
            "verified": True,
            "receipt": {"receipt_id": "transport-receipt:1"},
        },
    )

    args = _parse(["verify", "--selector-json", json.dumps(selector), "--json"])
    with pytest.raises(SystemExit) as exc:
        transport_outbox_cmd.cmd_transport_outbox(args)

    assert exc.value.code == 0
    assert observed == [selector]
    assert json.loads(capsys.readouterr().out)["verified"] is True


def test_verify_cli_returns_nonzero_for_unverified_selector(monkeypatch, capsys):
    monkeypatch.setattr(
        "gateway.transport_outbox.verify_transport_receipt",
        lambda _value: {"status": "missing", "verified": False, "reason": "receipt_not_found"},
    )
    args = _parse(["verify", "--selector-json", "{}"])

    with pytest.raises(SystemExit) as exc:
        transport_outbox_cmd.cmd_transport_outbox(args)

    assert exc.value.code == 1
    assert "receipt_not_found" in capsys.readouterr().err


def test_verify_cli_accepts_stdin_and_rejects_non_object(monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin.read", lambda: "[]")
    args = _parse(["verify", "--selector-json", "-"])

    with pytest.raises(SystemExit) as exc:
        transport_outbox_cmd.cmd_transport_outbox(args)

    assert exc.value.code == 2
    assert "selector must be a JSON object" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("outcome", "exit_code"),
    [
        ("confirmed", 0),
        ("definitively_rejected", 3),
        ("indeterminate", 4),
        ("unavailable", 1),
    ],
)
def test_recover_cli_exposes_stable_core_outcome_contract(
    monkeypatch, capsys, outcome, exit_code
):
    selector = {"request_id": "request-1", "profile_id": "atlas"}
    observed = []
    monkeypatch.setattr(
        "gateway.transport_outbox.recover_transport_request",
        lambda value: observed.append(value)
        or {
            "schema_version": "transport-outbox-recovery/v1",
            "request_id": "request-1",
            "outcome": outcome,
            "trusted_confirmed_receipt": outcome == "confirmed",
            "verification_status": outcome,
            "reason": None,
        },
    )
    args = _parse(["recover", "--selector-json", json.dumps(selector), "--json"])

    with pytest.raises(SystemExit) as exc:
        transport_outbox_cmd.cmd_transport_outbox(args)

    assert exc.value.code == exit_code
    assert observed == [selector]
    payload = json.loads(capsys.readouterr().out)
    assert payload["outcome"] == outcome
    assert payload["trusted_confirmed_receipt"] is (outcome == "confirmed")
