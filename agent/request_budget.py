"""Caller-owned provider request deadlines.

Cron installs one budget for the complete worker context.  Main-model and
auxiliary requests consult it immediately before each physical provider
attempt, so retries cannot restart a full provider timeout after the owning
run's cleanup window has begun.
"""

from __future__ import annotations

import contextvars
from dataclasses import dataclass
import time
from typing import Optional


@dataclass(frozen=True)
class ProviderRequestBudget:
    deadline_monotonic: float
    cleanup_grace_seconds: float = 0.0

    def remaining(self) -> float:
        remaining = (
            float(self.deadline_monotonic)
            - time.monotonic()
            - max(0.0, float(self.cleanup_grace_seconds))
        )
        if remaining <= 0.0:
            raise TimeoutError("Cron run deadline exhausted before provider request")
        return remaining

    def cap(self, configured_timeout: float) -> float:
        return min(float(configured_timeout), self.remaining())


_PROVIDER_REQUEST_BUDGET: contextvars.ContextVar[
    Optional[ProviderRequestBudget]
] = contextvars.ContextVar("provider_request_budget", default=None)


def set_provider_request_budget(
    *, deadline_monotonic: float, cleanup_grace_seconds: float
) -> contextvars.Token:
    return _PROVIDER_REQUEST_BUDGET.set(
        ProviderRequestBudget(
            deadline_monotonic=float(deadline_monotonic),
            cleanup_grace_seconds=max(0.0, float(cleanup_grace_seconds)),
        )
    )


def reset_provider_request_budget(token: contextvars.Token) -> None:
    _PROVIDER_REQUEST_BUDGET.reset(token)


def get_provider_request_budget() -> Optional[ProviderRequestBudget]:
    return _PROVIDER_REQUEST_BUDGET.get()


def cap_provider_request_timeout(configured_timeout: float) -> float:
    budget = get_provider_request_budget()
    if budget is None:
        return float(configured_timeout)
    return budget.cap(float(configured_timeout))
