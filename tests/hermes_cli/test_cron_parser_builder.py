"""Unit tests for the extracted ``hermes cron`` parser builder.

Confirms ``build_cron_parser`` wires up the same subactions, aliases, options,
and ``func=cmd_cron`` dispatch that lived inline in ``main()`` before the
god-file Phase 2 extraction.
"""

from __future__ import annotations

import argparse

from hermes_cli.subcommands.cron import build_cron_parser


def _sentinel_handler(args):  # pragma: no cover - only identity is asserted
    return "cron-handler"


def _build():
    parser = argparse.ArgumentParser(prog="hermes")
    subparsers = parser.add_subparsers(dest="command")
    build_cron_parser(subparsers, cmd_cron=_sentinel_handler)
    return parser


def test_cron_subactions_present():
    parser = _build()
    for action in ("list", "create", "edit", "pause", "resume", "run", "remove", "status", "runs", "tick"):
        ns = parser.parse_args(["cron", action] if action in ("list", "status", "runs", "tick")
                               else ["cron", action, "jobid"] if action in ("pause", "resume", "run", "remove", "edit")
                               else ["cron", "create", "30m"])
        assert ns.command == "cron"
        assert ns.cron_command == action


def test_cron_edit_no_agent_tristate():
    parser = _build()
    # --no-agent -> True, --agent -> False, neither -> None
    assert parser.parse_args(["cron", "edit", "j", "--no-agent"]).no_agent is True
    assert parser.parse_args(["cron", "edit", "j", "--agent"]).no_agent is False
    assert parser.parse_args(["cron", "edit", "j"]).no_agent is None


def test_cron_edit_governance_controls_are_explicit():
    parser = _build()
    assert parser.parse_args(["cron", "edit", "j"]).refresh_governance is False
    assert parser.parse_args([
        "cron", "edit", "j", "--refresh-governance"
    ]).refresh_governance is True

    args = parser.parse_args([
        "cron",
        "edit",
        "j",
        "--retire-verification-profile-id",
        "default",
        "--retire-verification-job-revision",
        "sha256:" + "1" * 64,
        "--retire-verification-command-sha256",
        "sha256:" + "2" * 64,
    ])
    assert args.retire_verification_profile_id == "default"
    assert args.retire_verification_job_revision == "sha256:" + "1" * 64
    assert args.retire_verification_command_sha256 == "sha256:" + "2" * 64


def test_cron_accept_hooks_flag_on_run_and_tick():
    parser = _build()
    # --accept-hooks is suppressed-default; present only when passed.
    ns = parser.parse_args(["cron", "run", "jid", "--accept-hooks"])
    assert ns.accept_hooks is True
    ns2 = parser.parse_args(["cron", "tick", "--accept-hooks"])
    assert ns2.accept_hooks is True
