"""Read-only CLI for exact Core transport receipt verification."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def _read_selector(value: str) -> dict[str, Any]:
    if value == "-":
        raw = sys.stdin.read()
    elif value.startswith("@"):
        raw = Path(value[1:]).expanduser().read_text(encoding="utf-8")
    else:
        raw = value
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("selector must be a JSON object")
    return payload


def cmd_transport_outbox(args: argparse.Namespace) -> None:
    try:
        selector = _read_selector(args.selector_json)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"hermes transport-outbox verify: invalid selector: {exc}", file=sys.stderr)
        sys.exit(2)

    from gateway.transport_outbox import (
        recover_transport_request,
        verify_transport_receipt,
    )

    action = getattr(args, "transport_outbox_action", "verify")
    if action == "recover":
        result = recover_transport_request(selector)
        if args.json:
            print(json.dumps(result, indent=2, sort_keys=True))
        else:
            print(
                f"{result.get('outcome', 'unavailable')} "
                f"{result.get('request_id', '')}".rstrip()
            )
        exit_codes = {
            "confirmed": 0,
            "definitively_rejected": 3,
            "indeterminate": 4,
            "unavailable": 1,
        }
        sys.exit(exit_codes.get(str(result.get("outcome")), 1))

    result = verify_transport_receipt(selector)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    elif result.get("verified"):
        receipt = result.get("receipt") or {}
        print(f"verified {receipt.get('receipt_id', '')}")
    else:
        print(
            f"not verified: {result.get('reason') or result.get('status') or 'unknown'}",
            file=sys.stderr,
        )
    sys.exit(0 if result.get("verified") else 1)


def register_transport_outbox_subparser(subparsers) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "transport-outbox",
        help="Verify a Core-owned transport receipt without modifying state.",
    )
    actions = parser.add_subparsers(dest="transport_outbox_action", required=True)
    verify = actions.add_parser(
        "verify",
        help="Verify one exact profile, Frame, claim, route, hash, and request tuple.",
    )
    verify.add_argument(
        "--selector-json",
        required=True,
        metavar="JSON_OR_@PATH_OR_-",
        help="Exact selector JSON, @path, or '-' for stdin.",
    )
    verify.add_argument("--json", action="store_true", help="Emit structured JSON.")
    verify.set_defaults(func=cmd_transport_outbox)
    recover = actions.add_parser(
        "recover",
        help="Read Core's authoritative outcome for HAK crash recovery.",
    )
    recover.add_argument(
        "--selector-json",
        required=True,
        metavar="JSON_OR_@PATH_OR_-",
        help="Exact selector JSON, @path, or '-' for stdin.",
    )
    recover.add_argument("--json", action="store_true", help="Emit structured JSON.")
    recover.set_defaults(func=cmd_transport_outbox)
    return parser


__all__ = ["cmd_transport_outbox", "register_transport_outbox_subparser"]
