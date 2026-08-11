import asyncio
import os
import sys
import threading
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
    assert scheduler._standalone_outbound_hook_registries == {profile: live_hooks}


def test_gateway_profile_binding_blocks_later_standalone_profile(
    monkeypatch, tmp_path
):
    profile_a = tmp_path / "profile-a"
    profile_b = tmp_path / "profile-b"
    _write_screening_hook(profile_b, reason="profile-b")
    scheduler._standalone_outbound_hook_registries.clear()
    live_hooks = HookRegistry(profile_a / "hooks")
    monkeypatch.setattr(
        "gateway.run._gateway_runner_ref",
        lambda: SimpleNamespace(hooks=live_hooks),
    )

    hooks_a = _load_with_profile_override(profile_a)
    hooks_b = _load_with_profile_override(profile_b)
    decision_b = asyncio.run(
        outbound_before_send(hooks_b, _required_context(profile_b))
    )

    assert hooks_a is live_hooks
    assert hooks_b is None
    assert decision_b.reason == "required_output_screening_hook_missing"
    assert scheduler._standalone_outbound_hook_registries == {profile_a: live_hooks}
    assert not any(
        getattr(module, "PROFILE_REASON", "") == "profile-b"
        for module in tuple(sys.modules.values())
    )


def test_failed_standalone_profile_cannot_switch_to_live_gateway_registry(
    monkeypatch, tmp_path
):
    profile = tmp_path / "profile"
    scheduler._standalone_outbound_hook_registries.clear()
    monkeypatch.setattr("gateway.run._gateway_runner_ref", lambda: None)

    assert _load_with_profile_override(profile) is None
    live_hooks = HookRegistry(profile / "hooks")
    monkeypatch.setattr(
        "gateway.run._gateway_runner_ref",
        lambda: SimpleNamespace(hooks=live_hooks),
    )

    hooks = _load_with_profile_override(profile)
    decision = asyncio.run(outbound_before_send(hooks, _required_context(profile)))

    assert hooks is None
    assert decision.reason == "required_output_screening_hook_missing"
    assert scheduler._standalone_outbound_hook_registries == {profile: None}


def test_standalone_profile_cannot_switch_to_second_live_registry(
    monkeypatch, tmp_path
):
    profile = tmp_path / "profile"
    _write_screening_hook(profile, reason="standalone")
    scheduler._standalone_outbound_hook_registries.clear()
    monkeypatch.setattr("gateway.run._gateway_runner_ref", lambda: None)
    standalone_hooks = _load_with_profile_override(profile)
    live_hooks = HookRegistry(profile / "hooks")
    monkeypatch.setattr(
        "gateway.run._gateway_runner_ref",
        lambda: SimpleNamespace(hooks=live_hooks),
    )

    selected = _load_with_profile_override(profile)

    assert selected is standalone_hooks
    assert selected is not live_hooks
    assert scheduler._standalone_outbound_hook_registries == {
        profile: standalone_hooks
    }


def test_first_selected_profile_owns_before_gateway_lookup(monkeypatch, tmp_path):
    profile_a = tmp_path / "profile-a"
    profile_b = tmp_path / "profile-b"
    scheduler._standalone_outbound_hook_registries.clear()
    live_hooks = HookRegistry(profile_a / "hooks")
    lookup_entered = threading.Event()
    release_lookup = threading.Event()
    lookup_calls = []
    results = {}

    def blocking_runner_ref():
        lookup_calls.append(threading.current_thread().name)
        lookup_entered.set()
        assert release_lookup.wait(timeout=2)
        return SimpleNamespace(hooks=live_hooks)

    monkeypatch.setattr("gateway.run._gateway_runner_ref", blocking_runner_ref)

    def load(profile, key):
        results[key] = _load_with_profile_override(profile)

    thread_a = threading.Thread(target=load, args=(profile_a, "a"), name="profile-a")
    thread_b = threading.Thread(target=load, args=(profile_b, "b"), name="profile-b")
    thread_a.start()
    assert lookup_entered.wait(timeout=2)
    thread_b.start()
    thread_b.join(timeout=2)

    assert not thread_b.is_alive()
    assert results["b"] is None
    assert lookup_calls == ["profile-a"]

    release_lookup.set()
    thread_a.join(timeout=2)

    assert not thread_a.is_alive()
    assert results["a"] is live_hooks
    assert scheduler._standalone_outbound_hook_registries == {profile_a: live_hooks}


def test_hook_discovery_reentry_fails_closed_without_deadlock(monkeypatch, tmp_path):
    profile = tmp_path / "profile"
    _write_screening_hook(profile)
    scheduler._standalone_outbound_hook_registries.clear()
    monkeypatch.setattr("gateway.run._gateway_runner_ref", lambda: None)
    original_discover = HookRegistry.discover_and_load
    reentrant_results = []

    def discover_with_reentry(registry):
        reentrant_results.append(_load_with_profile_override(profile))
        return original_discover(registry)

    monkeypatch.setattr(HookRegistry, "discover_and_load", discover_with_reentry)

    hooks = _load_with_profile_override(profile)

    assert hooks is not None
    assert reentrant_results == [None]
    assert scheduler._standalone_outbound_hook_registries == {profile: hooks}


def test_live_registry_cannot_commit_after_claim_is_lost(monkeypatch, tmp_path):
    profile = tmp_path / "profile"
    scheduler._standalone_outbound_hook_registries.clear()
    live_hooks = HookRegistry(profile / "hooks")

    def clear_claim_before_return():
        scheduler._standalone_outbound_hook_registries.clear()
        return SimpleNamespace(hooks=live_hooks)

    monkeypatch.setattr("gateway.run._gateway_runner_ref", clear_claim_before_return)

    hooks = _load_with_profile_override(profile)

    assert hooks is None
    assert scheduler._standalone_outbound_hook_registries == {}


def test_live_registry_inspection_failure_terminalizes_exact_claim(
    monkeypatch, tmp_path
):
    profile = tmp_path / "profile"
    scheduler._standalone_outbound_hook_registries.clear()

    class BrokenHooks:
        @property
        def hooks_dir(self):
            raise OSError("hooks directory unavailable")

    monkeypatch.setattr(
        "gateway.run._gateway_runner_ref",
        lambda: SimpleNamespace(hooks=BrokenHooks()),
    )

    hooks = _load_with_profile_override(profile)

    assert hooks is None
    assert scheduler._standalone_outbound_hook_registries == {profile: None}


def test_live_registry_lookup_failure_does_not_fallback(monkeypatch, tmp_path):
    profile = tmp_path / "profile"
    _write_screening_hook(profile)
    scheduler._standalone_outbound_hook_registries.clear()
    monkeypatch.setattr(
        "gateway.run._gateway_runner_ref",
        lambda: (_ for _ in ()).throw(OSError("runner unavailable")),
    )

    hooks = _load_with_profile_override(profile)

    assert hooks is None
    assert scheduler._standalone_outbound_hook_registries == {profile: None}


def test_mismatched_live_registry_does_not_fallback(monkeypatch, tmp_path):
    profile_a = tmp_path / "profile-a"
    profile_b = tmp_path / "profile-b"
    _write_screening_hook(profile_b)
    scheduler._standalone_outbound_hook_registries.clear()
    live_hooks = HookRegistry(profile_a / "hooks")
    monkeypatch.setattr(
        "gateway.run._gateway_runner_ref",
        lambda: SimpleNamespace(hooks=live_hooks),
    )

    hooks = _load_with_profile_override(profile_b)

    assert hooks is None
    assert scheduler._standalone_outbound_hook_registries == {profile_b: None}


def test_lost_claim_cannot_consume_replacement_registry(monkeypatch, tmp_path):
    profile = tmp_path / "profile"
    scheduler._standalone_outbound_hook_registries.clear()
    replacement = HookRegistry(profile / "replacement-hooks")
    live_hooks = HookRegistry(profile / "hooks")

    def replace_claim_before_return():
        scheduler._standalone_outbound_hook_registries[profile] = replacement
        return SimpleNamespace(hooks=live_hooks)

    monkeypatch.setattr("gateway.run._gateway_runner_ref", replace_claim_before_return)

    hooks = _load_with_profile_override(profile)

    assert hooks is None
    assert scheduler._standalone_outbound_hook_registries == {profile: replacement}


def test_profile_home_is_captured_once_for_claim_and_discovery(monkeypatch, tmp_path):
    profile_a = tmp_path / "profile-a"
    profile_b = tmp_path / "profile-b"
    _write_screening_hook(profile_a)
    scheduler._standalone_outbound_hook_registries.clear()
    profiles = iter((profile_a, profile_b))
    calls = []

    def drifting_profile_home():
        selected = next(profiles)
        calls.append(selected)
        return selected

    monkeypatch.setattr(scheduler, "get_hermes_home", drifting_profile_home)
    monkeypatch.setattr("gateway.run._gateway_runner_ref", lambda: None)

    hooks = scheduler._active_outbound_hooks()

    assert hooks is not None
    assert calls == [profile_a]
    assert scheduler._standalone_outbound_hook_registries == {profile_a: hooks}


def test_discovery_runtime_error_after_lost_claim_fails_closed(
    monkeypatch, tmp_path
):
    profile = tmp_path / "profile"
    scheduler._standalone_outbound_hook_registries.clear()
    replacement = HookRegistry(profile / "replacement-hooks")
    monkeypatch.setattr("gateway.run._gateway_runner_ref", lambda: None)

    def replace_claim_then_fail(registry):
        scheduler._standalone_outbound_hook_registries[profile] = replacement
        raise RuntimeError("discovery failed after ownership changed")

    monkeypatch.setattr(HookRegistry, "discover_and_load", replace_claim_then_fail)

    hooks = _load_with_profile_override(profile)

    assert hooks is None
    assert scheduler._standalone_outbound_hook_registries == {profile: replacement}


def test_gateway_control_signal_terminalizes_exact_claim_and_propagates(
    monkeypatch, tmp_path
):
    profile = tmp_path / "profile"
    scheduler._standalone_outbound_hook_registries.clear()
    monkeypatch.setattr(
        "gateway.run._gateway_runner_ref",
        lambda: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    with pytest.raises(KeyboardInterrupt):
        _load_with_profile_override(profile)

    assert scheduler._standalone_outbound_hook_registries == {profile: None}
    live_hooks = HookRegistry(profile / "hooks")
    monkeypatch.setattr(
        "gateway.run._gateway_runner_ref",
        lambda: SimpleNamespace(hooks=live_hooks),
    )
    assert _load_with_profile_override(profile) is None


def test_gateway_control_signal_preserves_replacement_claim(monkeypatch, tmp_path):
    profile = tmp_path / "profile"
    scheduler._standalone_outbound_hook_registries.clear()
    replacement = HookRegistry(profile / "replacement-hooks")

    def replace_then_interrupt():
        scheduler._standalone_outbound_hook_registries[profile] = replacement
        raise KeyboardInterrupt

    monkeypatch.setattr("gateway.run._gateway_runner_ref", replace_then_interrupt)

    with pytest.raises(KeyboardInterrupt):
        _load_with_profile_override(profile)

    assert scheduler._standalone_outbound_hook_registries == {profile: replacement}


def test_discovery_control_signal_terminalizes_exact_claim_and_propagates(
    monkeypatch, tmp_path
):
    profile = tmp_path / "profile"
    scheduler._standalone_outbound_hook_registries.clear()
    monkeypatch.setattr("gateway.run._gateway_runner_ref", lambda: None)
    monkeypatch.setattr(
        HookRegistry,
        "discover_and_load",
        lambda registry: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    with pytest.raises(KeyboardInterrupt):
        _load_with_profile_override(profile)

    assert scheduler._standalone_outbound_hook_registries == {profile: None}
    assert _load_with_profile_override(profile) is None


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


def test_failed_first_profile_load_still_owns_process_binding(monkeypatch, tmp_path):
    profile_a = tmp_path / "profile-a"
    profile_b = tmp_path / "profile-b"
    _write_screening_hook(profile_a, reason="profile-a")
    _write_screening_hook(profile_b, reason="profile-b")
    scheduler._standalone_outbound_hook_registries.clear()
    original_discover = HookRegistry.discover_and_load
    calls = []

    def fail_first(registry):
        calls.append(registry.hooks_dir)
        raise OSError("first Profile unavailable")

    monkeypatch.setattr(HookRegistry, "discover_and_load", fail_first)
    assert _load_with_profile_override(profile_a) is None
    monkeypatch.setattr(HookRegistry, "discover_and_load", original_discover)

    hooks_b = _load_with_profile_override(profile_b)
    decision_b = asyncio.run(
        outbound_before_send(hooks_b, _required_context(profile_b))
    )

    assert hooks_b is None
    assert decision_b.reason == "required_output_screening_hook_missing"
    assert calls == [profile_a / "hooks"]
    assert scheduler._standalone_outbound_hook_registries == {profile_a: None}


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
