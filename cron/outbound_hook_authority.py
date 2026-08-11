"""Process-wide authority for standalone Cron outbound Hook selection."""

import threading
from collections.abc import Iterator, MutableMapping
from pathlib import Path
from typing import Any


_UNRESOLVED_OUTBOUND_HOOK_RESULT = object()
_MISSING_OUTBOUND_HOOK_REGISTRY = object()


class _OutboundHookWaiter:
    """Thread-safe cancellation marker for one same-Profile waiter."""

    def __init__(self) -> None:
        self.abandoned = threading.Event()


class _UnresolvedOutboundHooks:
    """Identity token for the one Profile lookup allowed to resolve."""

    def __init__(self) -> None:
        self.owner_thread_id = threading.get_ident()
        self.result: Any = _UNRESOLVED_OUTBOUND_HOOK_RESULT
        self.revoked = False
        self.waiters: set[_OutboundHookWaiter] = set()
        self.done = threading.Event()


class _OutboundHookRegistryStore(MutableMapping[Path, Any | None]):
    """One lock-owned Profile authority projected as a mapping."""

    __slots__ = ("__lock", "__profile", "__state")

    def __init__(self) -> None:
        if hasattr(self, "_OutboundHookRegistryStore__lock"):
            raise RuntimeError("Outbound Hook registry store is already initialized")
        self.__lock = threading.RLock()
        self.__profile: Path | None = None
        self.__state: Any = _MISSING_OUTBOUND_HOOK_REGISTRY

    @property
    def lock(self):
        return self.__lock

    def _active_waiters_locked(
        self,
        claim_token: _UnresolvedOutboundHooks,
    ) -> set[_OutboundHookWaiter]:
        claim_token.waiters = {
            waiter
            for waiter in claim_token.waiters
            if not waiter.abandoned.is_set()
        }
        return claim_token.waiters

    def _finalize_claim_locked(
        self,
        key: Path,
        claim_token: _UnresolvedOutboundHooks,
    ) -> None:
        if self.__profile != key or self.__state is not claim_token:
            return
        if self._active_waiters_locked(claim_token):
            return
        result = claim_token.result
        if result is _UNRESOLVED_OUTBOUND_HOOK_RESULT:
            return
        self.__state = None if claim_token.revoked else result

    def _prepare_mutation_locked(self) -> None:
        if not isinstance(self.__state, _UnresolvedOutboundHooks):
            return
        claim_token = self.__state
        self._reject_active_waiters_locked()
        claim_token.revoked = True

    def _normalize_abandoned_waiters_locked(self) -> bool:
        previous = self.__state
        if isinstance(self.__state, _UnresolvedOutboundHooks):
            self._finalize_claim_locked(self.__profile, self.__state)
        return self.__state is not previous

    def _guard_mutation_start_locked(self) -> None:
        if self._normalize_abandoned_waiters_locked():
            raise ValueError("Outbound Hook registry authority was finalized")
        self._reject_active_waiters_locked()

    def _reject_active_waiters_locked(self) -> None:
        if not isinstance(self.__state, _UnresolvedOutboundHooks):
            return
        if self._active_waiters_locked(self.__state):
            raise ValueError("Outbound Hook registry claim has active waiters")

    def __getitem__(self, key: Path) -> Any | None:
        with self.__lock:
            if key != self.__profile or self.__state is _MISSING_OUTBOUND_HOOK_REGISTRY:
                raise KeyError(key)
            return self.__state

    def __setitem__(self, key: Path, value: Any | None) -> None:
        with self.__lock:
            self._setitem_locked(key, value)

    def _setitem_locked(self, key: Path, value: Any | None) -> None:
        self._guard_mutation_start_locked()
        if key == self.__profile and self.__state is value:
            return
        if self.__profile is not None and key != self.__profile:
            raise ValueError("Outbound Hook registry store is already bound")
        if self.__state is not _MISSING_OUTBOUND_HOOK_REGISTRY and not isinstance(
            self.__state,
            _UnresolvedOutboundHooks,
        ):
            raise ValueError("Outbound Hook registry authority is immutable")
        self._prepare_mutation_locked()
        self.__profile = key
        self.__state = value

    def __delitem__(self, key: Path) -> None:
        with self.__lock:
            self._guard_mutation_start_locked()
            if key != self.__profile or self.__state is _MISSING_OUTBOUND_HOOK_REGISTRY:
                raise KeyError(key)
            self._prepare_mutation_locked()
            self.__profile = None
            self.__state = _MISSING_OUTBOUND_HOOK_REGISTRY

    def __iter__(self) -> Iterator[Path]:
        with self.__lock:
            if self.__profile is None or self.__state is _MISSING_OUTBOUND_HOOK_REGISTRY:
                return iter(())
            return iter((self.__profile,))

    def __len__(self) -> int:
        with self.__lock:
            return int(
                self.__profile is not None
                and self.__state is not _MISSING_OUTBOUND_HOOK_REGISTRY
            )

    def __contains__(self, key: object) -> bool:
        with self.__lock:
            return (
                key == self.__profile
                and self.__state is not _MISSING_OUTBOUND_HOOK_REGISTRY
            )

    def _resolve_claim(
        self,
        key: Path,
        claim_token: _UnresolvedOutboundHooks,
        result: Any,
    ) -> bool:
        with self.__lock:
            if self.__profile != key or self.__state is not claim_token:
                return False
            if claim_token.revoked:
                return False
            if claim_token.result is not _UNRESOLVED_OUTBOUND_HOOK_RESULT:
                return False
            claim_token.result = result
            claim_token.done.set()
            self._finalize_claim_locked(key, claim_token)
            return True

    def _fail_claim(
        self,
        key: Path,
        claim_token: _UnresolvedOutboundHooks,
    ) -> bool:
        with self.__lock:
            if self.__profile != key or self.__state is not claim_token:
                return False
            if claim_token.result is not _UNRESOLVED_OUTBOUND_HOOK_RESULT:
                return False
            claim_token.result = None
            claim_token.done.set()
            self._finalize_claim_locked(key, claim_token)
            return True

    def _consume_waiter(
        self,
        key: Path,
        claim_token: _UnresolvedOutboundHooks,
        waiter: _OutboundHookWaiter,
    ) -> tuple[Any, bool]:
        with self.__lock:
            if waiter not in claim_token.waiters:
                raise RuntimeError("Outbound Hook claim waiter count underflow")
            claim_token.waiters.remove(waiter)
            result = claim_token.result
            revoked = claim_token.revoked
            if result is _UNRESOLVED_OUTBOUND_HOOK_RESULT:
                result = None
                claim_token.result = None
            self._finalize_claim_locked(key, claim_token)
            return result, revoked

    def _abandon_waiter_during_control_signal(
        self,
        key: Path,
        claim_token: _UnresolvedOutboundHooks,
        waiter: _OutboundHookWaiter,
    ) -> None:
        waiter.abandoned.set()
        try:
            acquired = self.__lock.acquire(False)
        except BaseException:
            return
        if not acquired:
            return
        try:
            self._finalize_claim_locked(key, claim_token)
        finally:
            try:
                self.__lock.release()
            except BaseException:
                pass

    def __ior__(self, other: Any) -> "_OutboundHookRegistryStore":
        self.update(other)
        return self

    def update(self, *args: Any, **kwargs: Any) -> None:
        candidate = dict(*args, **kwargs)
        if len(candidate) > 1:
            raise ValueError("Outbound Hook registry store accepts one Profile")
        with self.__lock:
            if not candidate:
                self._guard_mutation_start_locked()
                return
            key, value = next(iter(candidate.items()))
            self._setitem_locked(key, value)

    def setdefault(self, key: Path, default: Any | None = None) -> Any | None:
        with self.__lock:
            self._guard_mutation_start_locked()
            if key == self.__profile and self.__state is not _MISSING_OUTBOUND_HOOK_REGISTRY:
                return self.__state
            self._setitem_locked(key, default)
            return default

    def pop(self, key: Path, *default: Any) -> Any:
        if len(default) > 1:
            raise TypeError(f"pop expected at most 2 arguments, got {len(default) + 1}")
        with self.__lock:
            self._guard_mutation_start_locked()
            if key != self.__profile or self.__state is _MISSING_OUTBOUND_HOOK_REGISTRY:
                if default:
                    return default[0]
                raise KeyError(key)
            result = self.__state
            self._prepare_mutation_locked()
            self.__profile = None
            self.__state = _MISSING_OUTBOUND_HOOK_REGISTRY
            return result

    def popitem(self) -> tuple[Path, Any | None]:
        with self.__lock:
            self._guard_mutation_start_locked()
            if self.__profile is None or self.__state is _MISSING_OUTBOUND_HOOK_REGISTRY:
                raise KeyError("popitem(): mapping is empty")
            key = self.__profile
            return key, self.pop(key)


_standalone_outbound_hook_registries = _OutboundHookRegistryStore()
_standalone_outbound_hooks_lock = _standalone_outbound_hook_registries.lock
