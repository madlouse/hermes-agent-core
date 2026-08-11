import asyncio
import os
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cron import scheduler
from gateway.outbound_boundary import outbound_before_send
from gateway.hooks import HookRegistry
from hermes_constants import (
    reset_hermes_home_override,
    set_hermes_home_override,
)


def _write_screening_hook(profile, *, reason="screened"):
    hook = profile / "hooks" / "outbound-actionable"
    hook.mkdir(parents=True)
    (hook / "HOOK.yaml").write_text(
        "\n".join(
            (
                "name: outbound-actionable",
                "events:",
                "  - outbound:before_send",
                "capabilities:",
                "  - output-screening",
                "",
            )
        ),
        encoding="utf-8",
    )
    (hook / "handler.py").write_text(
        "async def handle(event_type, context):\n"
        f"    return {{'decision': 'allow', 'reason': {reason!r}}}\n",
        encoding="utf-8",
    )


def _reset_standalone_cache(monkeypatch, profile):
    monkeypatch.setattr(scheduler, "get_hermes_home", lambda: profile)
    scheduler._standalone_outbound_hook_registries.clear()


def _load_with_profile_override(profile):
    token = set_hermes_home_override(profile)
    try:
        return scheduler._active_outbound_hooks()
    finally:
        reset_hermes_home_override(token)


def _required_context(profile):
    return {
        "source_kind": "cron",
        "profile_path": str(profile),
        "profile_id": "test",
        "platform": "feishu",
        "channel_id": "oc_test",
        "content": "Business result",
        "output_screening_required": True,
    }


def test_standalone_cron_loads_profile_screening_hook(monkeypatch, tmp_path):
    profile = tmp_path / "profile"
    _write_screening_hook(profile)
    _reset_standalone_cache(monkeypatch, profile)
    monkeypatch.setattr("gateway.run._gateway_runner_ref", lambda: None)

    first = scheduler._active_outbound_hooks()
    second = scheduler._active_outbound_hooks()
    decision = asyncio.run(outbound_before_send(first, _required_context(profile)))

    assert first is second
    assert decision.transmit is True
    assert decision.reason == "screened"


def test_standalone_cron_without_screening_hook_remains_closed(
    monkeypatch, tmp_path
):
    profile = tmp_path / "profile"
    (profile / "hooks").mkdir(parents=True)
    _reset_standalone_cache(monkeypatch, profile)
    monkeypatch.setattr("gateway.run._gateway_runner_ref", lambda: None)

    decision = asyncio.run(
        outbound_before_send(
            scheduler._active_outbound_hooks(),
            _required_context(profile),
        )
    )

    assert decision.transmit is False
    assert decision.reason == "required_output_screening_hook_missing"


def test_gateway_registry_is_preferred_without_standalone_discovery(
    monkeypatch, tmp_path
):
    profile = tmp_path / "profile"
    _reset_standalone_cache(monkeypatch, profile)
    live_hooks = HookRegistry(profile / "hooks")
    monkeypatch.setattr(
        "gateway.run._gateway_runner_ref",
        lambda: SimpleNamespace(hooks=live_hooks),
    )
    monkeypatch.setattr(
        scheduler,
        "_standalone_outbound_hooks",
        lambda: (_ for _ in ()).throw(AssertionError("standalone load")),
    )

    assert scheduler._active_outbound_hooks() is live_hooks


def test_standalone_missing_hook_root_fails_closed(monkeypatch, tmp_path):
    profile = tmp_path / "missing-profile"
    _reset_standalone_cache(monkeypatch, profile)
    monkeypatch.setattr("gateway.run._gateway_runner_ref", lambda: None)

    decision = asyncio.run(
        outbound_before_send(
            scheduler._active_outbound_hooks(),
            _required_context(profile),
        )
    )

    assert decision.transmit is False
    assert decision.reason == "required_output_screening_hook_missing"


@pytest.mark.skipif(
    sys.platform == "win32", reason="Symlinks require elevated privileges on Windows"
)
def test_standalone_cron_rejects_symlinked_hook_directory(
    monkeypatch, tmp_path
):
    profile = tmp_path / "profile"
    external = tmp_path / "external-hook"
    _write_screening_hook(external.parent)
    external_source = external.parent / "hooks" / "outbound-actionable"
    (profile / "hooks").mkdir(parents=True)
    (profile / "hooks" / "outbound-actionable").symlink_to(
        external_source,
        target_is_directory=True,
    )
    _reset_standalone_cache(monkeypatch, profile)
    monkeypatch.setattr("gateway.run._gateway_runner_ref", lambda: None)

    decision = asyncio.run(
        outbound_before_send(
            scheduler._active_outbound_hooks(),
            _required_context(profile),
        )
    )

    assert decision.transmit is False
    assert decision.reason == "required_output_screening_hook_missing"


@pytest.mark.skipif(
    sys.platform == "win32", reason="Symlinks require elevated privileges on Windows"
)
def test_standalone_cron_rejects_symlinked_profile_parent(tmp_path):
    external = tmp_path / "external" / "profile"
    _write_screening_hook(external)
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(external.parent, target_is_directory=True)
    selected_profile = linked_parent / "profile"
    scheduler._standalone_outbound_hook_registries.clear()

    hooks = _load_with_profile_override(selected_profile)
    decision = asyncio.run(
        outbound_before_send(hooks, _required_context(selected_profile))
    )

    assert decision.transmit is False
    assert decision.reason == "required_output_screening_hook_missing"


@pytest.mark.skipif(sys.platform == "win32", reason="FIFO is POSIX-only")
def test_standalone_cron_rejects_fifo_manifest_without_blocking(tmp_path):
    profile = tmp_path / "profile"
    hook = profile / "hooks" / "outbound-actionable"
    hook.mkdir(parents=True)
    os.mkfifo(hook / "HOOK.yaml")
    (hook / "handler.py").write_text(
        "async def handle(event_type, context):\n    return {'decision': 'allow'}\n",
        encoding="utf-8",
    )
    scheduler._standalone_outbound_hook_registries.clear()

    hooks = _load_with_profile_override(profile)
    decision = asyncio.run(outbound_before_send(hooks, _required_context(profile)))

    assert decision.transmit is False
    assert decision.reason == "required_output_screening_hook_missing"


@pytest.mark.skipif(
    sys.platform == "win32", reason="Symlinks require elevated privileges on Windows"
)
def test_standalone_cron_rejects_symlinked_manifest(tmp_path):
    profile = tmp_path / "profile"
    hook = profile / "hooks" / "outbound-actionable"
    hook.mkdir(parents=True)
    external_manifest = tmp_path / "external-HOOK.yaml"
    external_manifest.write_text(
        "name: outbound-actionable\nevents: [outbound:before_send]\n"
        "capabilities: [output-screening]\n",
        encoding="utf-8",
    )
    (hook / "HOOK.yaml").symlink_to(external_manifest)
    (hook / "handler.py").write_text(
        "async def handle(event_type, context):\n"
        "    return {'decision': 'allow'}\n",
        encoding="utf-8",
    )
    scheduler._standalone_outbound_hook_registries.clear()

    hooks = _load_with_profile_override(profile)
    decision = asyncio.run(outbound_before_send(hooks, _required_context(profile)))

    assert decision.transmit is False
    assert decision.reason == "required_output_screening_hook_missing"


@pytest.mark.skipif(not hasattr(os, "link"), reason="Hard links unavailable")
@pytest.mark.parametrize("linked_name", ["HOOK.yaml", "handler.py"])
@pytest.mark.parametrize("portable", [False, True])
def test_standalone_cron_rejects_hard_linked_hook_files(
    monkeypatch, tmp_path, linked_name, portable
):
    profile = tmp_path / "profile"
    _write_screening_hook(profile)
    hook = profile / "hooks" / "outbound-actionable"
    original = hook / linked_name
    external = tmp_path / f"external-{linked_name}"
    original.replace(external)
    os.link(external, original)
    scheduler._standalone_outbound_hook_registries.clear()
    if portable:
        monkeypatch.setattr(
            HookRegistry,
            "_supports_dirfd_discovery",
            staticmethod(lambda: False),
        )

    hooks = _load_with_profile_override(profile)
    decision = asyncio.run(outbound_before_send(hooks, _required_context(profile)))

    assert decision.transmit is False
    assert decision.reason == "required_output_screening_hook_missing"


def test_standalone_cron_uses_portable_secure_discovery_without_dirfd(
    monkeypatch, tmp_path
):
    profile = tmp_path / "profile"
    _write_screening_hook(profile)
    scheduler._standalone_outbound_hook_registries.clear()
    monkeypatch.setattr(
        HookRegistry,
        "_supports_dirfd_discovery",
        staticmethod(lambda: False),
    )

    hooks = _load_with_profile_override(profile)
    decision = asyncio.run(outbound_before_send(hooks, _required_context(profile)))

    assert decision.transmit is True
    assert decision.reason == "screened"


@pytest.mark.skipif(
    sys.platform == "win32", reason="Symlinks require elevated privileges on Windows"
)
def test_portable_discovery_rejects_symlinked_profile_parent(
    monkeypatch, tmp_path
):
    external = tmp_path / "external" / "profile"
    _write_screening_hook(external)
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(external.parent, target_is_directory=True)
    scheduler._standalone_outbound_hook_registries.clear()
    monkeypatch.setattr(
        HookRegistry,
        "_supports_dirfd_discovery",
        staticmethod(lambda: False),
    )

    hooks = _load_with_profile_override(linked_parent / "profile")
    decision = asyncio.run(
        outbound_before_send(hooks, _required_context(linked_parent / "profile"))
    )

    assert decision.transmit is False
    assert decision.reason == "required_output_screening_hook_missing"


def test_portable_discovery_executes_only_verified_handler_bytes(
    monkeypatch, tmp_path
):
    profile = tmp_path / "profile"
    _write_screening_hook(profile, reason="verified")
    handler = profile / "hooks" / "outbound-actionable" / "handler.py"
    scheduler._standalone_outbound_hook_registries.clear()
    monkeypatch.setattr(
        HookRegistry,
        "_supports_dirfd_discovery",
        staticmethod(lambda: False),
    )
    original_reader = HookRegistry._read_portable_hook.__func__

    def read_then_replace(cls, hook_dir, expected_root):
        payload = original_reader(cls, hook_dir, expected_root)
        handler.write_text(
            "async def handle(event_type, context):\n"
            "    return {'decision': 'allow', 'reason': 'replaced'}\n",
            encoding="utf-8",
        )
        return payload

    monkeypatch.setattr(
        HookRegistry,
        "_read_portable_hook",
        classmethod(read_then_replace),
    )

    hooks = _load_with_profile_override(profile)
    decision = asyncio.run(outbound_before_send(hooks, _required_context(profile)))

    assert decision.transmit is True
    assert decision.reason == "verified"


def test_standalone_discovery_is_quiet(monkeypatch, tmp_path, capsys):
    profile = tmp_path / "profile"
    _write_screening_hook(profile)
    _reset_standalone_cache(monkeypatch, profile)
    monkeypatch.setattr("gateway.run._gateway_runner_ref", lambda: None)

    assert scheduler._active_outbound_hooks() is not None
    assert capsys.readouterr().out == ""


def test_context_selected_profiles_fail_closed_after_first_profile_binding(
    monkeypatch, tmp_path
):
    profile_a = tmp_path / "profile-a"
    profile_b = tmp_path / "profile-b"
    _write_screening_hook(profile_a, reason="profile-a")
    _write_screening_hook(profile_b, reason="profile-b")
    scheduler._standalone_outbound_hook_registries.clear()
    monkeypatch.delitem(sys.modules, "gateway.run", raising=False)

    hooks_a = _load_with_profile_override(profile_a)
    hooks_b = _load_with_profile_override(profile_b)
    decision_a = asyncio.run(
        outbound_before_send(hooks_a, _required_context(profile_a))
    )
    decision_b = asyncio.run(
        outbound_before_send(hooks_b, _required_context(profile_b))
    )

    assert hooks_b is None
    assert decision_a.reason == "profile-a"
    assert decision_b.reason == "required_output_screening_hook_missing"
    assert list(scheduler._standalone_outbound_hook_registries) == [profile_a]


def test_second_profile_handler_is_not_loaded_after_process_binding(tmp_path):
    profile_a = tmp_path / "profile-a"
    profile_b = tmp_path / "profile-b"
    for profile, reason in ((profile_a, "profile-a"), (profile_b, "profile-b")):
        hook = profile / "hooks" / "outbound-actionable"
        hook.mkdir(parents=True)
        (hook / "HOOK.yaml").write_text(
            "name: outbound-actionable\nevents: [outbound:before_send]\n"
            "capabilities: [output-screening]\n",
            encoding="utf-8",
        )
        (hook / "handler.py").write_text(
            "import sys\n"
            f"PROFILE_REASON = {reason!r}\n"
            "async def handle(event_type, context):\n"
            "    module = sys.modules[__name__]\n"
            "    return {'decision': 'allow', 'reason': module.PROFILE_REASON}\n",
            encoding="utf-8",
        )
    scheduler._standalone_outbound_hook_registries.clear()

    hooks_a = _load_with_profile_override(profile_a)
    hooks_b = _load_with_profile_override(profile_b)
    decision_a = asyncio.run(
        outbound_before_send(hooks_a, _required_context(profile_a))
    )
    decision_b = asyncio.run(
        outbound_before_send(hooks_b, _required_context(profile_b))
    )

    assert decision_a.reason == "profile-a"
    assert hooks_b is None
    assert decision_b.reason == "required_output_screening_hook_missing"
    assert not any(
        getattr(module, "PROFILE_REASON", "") == "profile-b"
        for module in tuple(sys.modules.values())
    )


def test_mismatched_gateway_registry_does_not_cross_profiles(
    monkeypatch, tmp_path
):
    profile_a = tmp_path / "profile-a"
    profile_b = tmp_path / "profile-b"
    _write_screening_hook(profile_a, reason="profile-a")
    _write_screening_hook(profile_b, reason="profile-b")
    scheduler._standalone_outbound_hook_registries.clear()
    token = set_hermes_home_override(profile_a)
    try:
        hooks_a = scheduler._standalone_outbound_hooks()
    finally:
        reset_hermes_home_override(token)
    monkeypatch.setitem(
        sys.modules,
        "gateway.run",
        SimpleNamespace(_gateway_runner_ref=lambda: SimpleNamespace(hooks=hooks_a)),
    )

    hooks_b = _load_with_profile_override(profile_b)
    decision = asyncio.run(
        outbound_before_send(hooks_b, _required_context(profile_b))
    )

    assert hooks_b is None
    assert decision.reason == "required_output_screening_hook_missing"


def test_standalone_cron_delivery_discovers_hook_before_sender(
    monkeypatch, tmp_path
):
    from gateway.config import Platform

    profile = tmp_path / "profile"
    _write_screening_hook(profile)
    scheduler._standalone_outbound_hook_registries.clear()
    monkeypatch.setattr("gateway.run._gateway_runner_ref", lambda: None)
    platform_config = MagicMock(enabled=True)
    gateway_config = MagicMock(platforms={Platform.TELEGRAM: platform_config})
    sender = AsyncMock(return_value={"success": True})

    with (
        patch("gateway.config.load_gateway_config", return_value=gateway_config),
        patch(
            "cron.scheduler.load_config",
            return_value={"cron": {"wrap_response": False}},
        ),
        patch("tools.send_message_tool._send_to_platform", new=sender),
    ):
        token = set_hermes_home_override(profile)
        try:
            result = scheduler._deliver_result(
                {
                    "id": "standalone-screened",
                    "deliver": "origin",
                    "origin": {"platform": "telegram", "chat_id": "123"},
                },
                "Business result",
            )
        finally:
            reset_hermes_home_override(token)

    assert result is None
    assert sender.await_count == 1
    assert sender.await_args.args[3] == "Business result"
