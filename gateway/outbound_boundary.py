"""Durable outbound delivery boundary for user-visible sends.

The runtime owns a thin, generic hook bridge only. Policy stays in hooks (for
example Hermes Agent Kit's ``outbound-actionable`` handler). This module gives
cron, send_message, and gateway final replies one common before/after event
shape and fail-closed behavior for actionable or enforced output.
"""

from __future__ import annotations

import asyncio
import copy
import concurrent.futures
import hashlib
import inspect
import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from gateway.hooks import HookRegistry

logger = logging.getLogger(__name__)

BEFORE_SEND = "outbound:before_send"
AFTER_SEND = "outbound:after_send"
EVENT_SCHEMA_VERSION = "durable-output-boundary/v1"

DEFAULT_TIMEOUT_SECONDS = 5.0
DECISIONS = {"allow", "deny", "hold", "rewrite", "downgrade"}
OPERATOR_SOURCE_KINDS = {"operator_enforce", "streaming_final_reply"}
REQUIRED_SCREENING_CAPABILITY = "output-screening"
_HOOK_IDENTITY_KEY = "_hermes_hook_name"
_HOOK_CAPABILITIES_KEY = "_hermes_hook_capabilities"

_ACTIONABLE_TEXT_RE = re.compile(
    r"(回复|发送|选择|确认|同意|通过|继续|批准|拒绝|不发|全发|发).{0,12}([A-Za-z]\d+|\d+|这[一二两三四五六七八九十\d]+)",
    re.IGNORECASE,
)
_ACTIONABLE_ENVELOPE_NAMES = {
    "cron": "hermes-cron-actionable",
    "send_message": "hermes-outbound-actionable",
    "gateway_notice": "hermes-outbound-actionable",
}
_ACTION_SPEC_ENVELOPE_RE = re.compile(
    r"\[\[ACTION_SPEC\]\]\s*(\{.*?\})\s*\[\[/ACTION_SPEC\]\]",
    re.DOTALL,
)
_FALSE_VALUES = {"0", "false", "no", "off", "disabled"}
_PROFILE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


@dataclass
class BoundaryDecision:
    """Normalized before-send decision used by runtime call sites."""

    transmit: bool
    decision: str
    content: str
    reason: str
    raw: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        payload = {
            "transmit": self.transmit,
            "decision": self.decision,
            "content": self.content,
            "reason": self.reason,
            "raw": self.raw,
        }
        if self.errors:
            payload["errors"] = list(self.errors)
        return payload


def _string(value: Any) -> str:
    return str(value or "").strip()


def profile_id_from_home(profile_path: Any) -> str:
    """Read the outbound owner from the already-resolved Profile home.

    Core's internal root Profile name is ``default`` and is not necessarily
    the business owner id used by policy plugins (for example ``atlas``).
    The resolved Profile config is therefore the authority. Missing or invalid
    identity stays empty so capability hooks can make the existing fail-closed
    decision. Core itself remains compatible with Profiles that do not use an
    owner-aware output-screening hook.
    """
    try:
        from hermes_cli.config import read_profile_id_literal

        raw_profile_id = read_profile_id_literal(
            Path(profile_path).expanduser().resolve() / "config.yaml"
        )
    except (OSError, ValueError, TypeError):
        return ""
    if not isinstance(raw_profile_id, str):
        return ""
    profile_id = raw_profile_id.strip()
    return profile_id if _PROFILE_ID_RE.fullmatch(profile_id) else ""


def _object(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _list_of_objects(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


def _source_kind(context: dict[str, Any]) -> str:
    value = _string(context.get("source_kind") or context.get("producer_kind"))
    if value in {"cron_notice", "cron_job"}:
        return "cron"
    if value in {"message", "send"}:
        return "send_message"
    return value


def content_text(context: dict[str, Any]) -> str:
    content = context.get("content")
    if isinstance(content, dict):
        return _string(content.get("text") or content.get("body") or content.get("preview"))
    return _string(content)


def gate_mode(context: dict[str, Any]) -> str:
    mode = _string(
        context.get("gate_mode")
        or os.getenv("HERMES_OUTBOUND_GATE_MODE")
        or "enforce"
    ).lower()
    return mode if mode in {"observe", "warn", "downgrade", "enforce", "off"} else "enforce"


def boundary_enabled(context: dict[str, Any]) -> bool:
    if context.get("boundary_enabled") is False:
        return False
    raw = os.getenv("HERMES_OUTBOUND_BOUNDARY_ENABLED", "").strip().lower()
    if raw in _FALSE_VALUES:
        return False
    return True


def _declares_actionable_metadata(context: dict[str, Any]) -> bool:
    if "action_spec" in context or "action_specs" in context:
        return True
    if _object(context.get("actionability")):
        return True
    content = content_text(context)
    return "[[ACTION_SPEC" in content or '"action_spec"' in content or "'action_spec'" in content


def looks_actionable(context: dict[str, Any]) -> bool:
    if context.get("looks_actionable") is True:
        return True
    if _declares_actionable_metadata(context):
        return True
    return bool(_ACTIONABLE_TEXT_RE.search(content_text(context) or ""))


def requires_boundary(context: dict[str, Any]) -> bool:
    screening_required = context.get("output_screening_required") is True
    if not screening_required and (
        not boundary_enabled(context) or gate_mode(context) == "off"
    ):
        return False
    source = _source_kind(context)
    if context.get("looks_actionable") is True:
        actionability = True
    elif context.get("looks_actionable") is False:
        actionability = _declares_actionable_metadata(context)
    else:
        actionability = looks_actionable(context)
    return bool(
        screening_required
        or source in OPERATOR_SOURCE_KINDS
        or context.get("enforced_channel") is True
        or actionability
    )


def gateway_reply_source_kind(content: str, *, enforced_channel: bool = False) -> str:
    ctx = {"source_kind": "gateway_reply", "content": content, "enforced_channel": enforced_channel}
    if _declares_actionable_metadata(ctx):
        return "operator_enforce"
    return "gateway_reply"


def _json_envelope_patterns(name: str) -> tuple[re.Pattern[str], re.Pattern[str]]:
    return (
        re.compile(r"<!--\s*" + re.escape(name) + r"\s*(.*?)\s*-->", re.DOTALL),
        re.compile(r"```" + re.escape(name) + r"\s*(.*?)\s*```", re.DOTALL),
    )


def _strip_and_parse_named_envelope(content: str, source_kind: str) -> tuple[str, dict[str, Any], str]:
    name = _ACTIONABLE_ENVELOPE_NAMES.get(source_kind)
    if not name:
        return content, {}, ""
    matches = [
        match
        for pattern in _json_envelope_patterns(name)
        for match in pattern.finditer(content)
    ]
    if not matches:
        return content, {}, ""
    if len(matches) > 1:
        return content, {}, f"multiple {name} metadata blocks are not allowed"
    match = matches[0]
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        return content, {}, f"invalid {name} metadata: {exc}"
    if not isinstance(payload, dict):
        return content, {}, f"{name} metadata must be a JSON object"
    visible = (content[: match.start()] + content[match.end() :]).strip()
    return visible, payload, ""


def _strip_and_parse_action_spec_envelope(content: str) -> tuple[str, dict[str, Any], str]:
    matches = list(_ACTION_SPEC_ENVELOPE_RE.finditer(content))
    if not matches:
        if "[[ACTION_SPEC" in content:
            return content, {}, "invalid ACTION_SPEC envelope"
        return content, {}, ""
    if len(matches) > 1:
        return content, {}, "multiple ACTION_SPEC envelopes are not allowed"
    match = matches[0]
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        return content, {}, f"invalid ACTION_SPEC envelope: {exc}"
    if not isinstance(payload, dict):
        return content, {}, "ACTION_SPEC envelope must be a JSON object"
    visible = (content[: match.start()] + content[match.end() :]).strip()
    return visible, {"action_spec": payload}, ""


def _apply_actionable_metadata(ctx: dict[str, Any], metadata: dict[str, Any]) -> None:
    action_specs = _list_of_objects(metadata.get("action_specs"))
    action_spec = _object(metadata.get("action_spec"))
    if not action_specs and not action_spec and isinstance(metadata.get("items"), list) and isinstance(metadata.get("actions"), list):
        action_spec = dict(metadata)
    if action_specs and "action_specs" not in ctx:
        ctx["action_specs"] = action_specs
    if action_spec and "action_spec" not in ctx and "action_specs" not in ctx:
        ctx["action_spec"] = action_spec
    for key in (
        "actionability",
        "decision_route",
        "result_route",
        "producer_id",
        "run_id",
        "job_id",
        "allowed_actor_uid",
        "resume_prompt",
        "ttl_hours",
    ):
        value = metadata.get(key)
        if value not in (None, "", [], {}) and key not in ctx:
            ctx[key] = value
    if metadata.get("gate_mode") and not ctx.get("gate_mode"):
        ctx["gate_mode"] = _string(metadata.get("gate_mode"))
    if metadata.get("profile_id") and not ctx.get("profile_id"):
        ctx["profile_id"] = _string(metadata.get("profile_id"))


def build_outbound_context(
    *,
    source_kind: str,
    content: str,
    platform: Any = "",
    chat_id: Any = "",
    thread_id: Any = "",
    profile_id: str | None = None,
    profile_path: str = "",
    gate_mode_value: str = "",
    boundary_enabled_value: bool | None = None,
    **extra: Any,
) -> dict[str, Any]:
    authoritative_profile_id = (
        None if profile_id is None else _string(profile_id)
    )
    transport_id = _string(getattr(platform, "value", platform))
    channel_id = _string(chat_id)
    source_kind = _source_kind({"source_kind": source_kind})
    text = _string(content)
    named_metadata: dict[str, Any] = {}
    action_spec_metadata: dict[str, Any] = {}
    envelope_errors: list[str] = []
    text, named_metadata, error = _strip_and_parse_named_envelope(text, source_kind)
    if error:
        envelope_errors.append(error)
    text, action_spec_metadata, error = _strip_and_parse_action_spec_envelope(text)
    if error:
        envelope_errors.append(error)
    event_seed = {
        "source_kind": source_kind,
        "transport_id": transport_id,
        "channel_id": channel_id,
        "thread_id": _string(thread_id),
        "content_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
    }
    ctx: dict[str, Any] = {
        "schema_version": EVENT_SCHEMA_VERSION,
        "event_id": "dob-" + hashlib.sha256(
            json.dumps(event_seed, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()[:24],
        "source_kind": source_kind,
        "transport_id": transport_id,
        "platform": transport_id,
        "channel_id": channel_id,
        "chat_id": channel_id,
        "thread_id": _string(thread_id),
        "profile_id": (
            os.getenv("HERMES_PROFILE_ID", "")
            if profile_id is None
            else _string(profile_id)
        ),
        "profile_path": profile_path or os.getenv("HERMES_PROFILE", ""),
        "gate_mode": gate_mode_value or "",
        "content": text,
        "content_sha256": event_seed["content_sha256"],
    }
    if boundary_enabled_value is not None:
        ctx["boundary_enabled"] = bool(boundary_enabled_value)
    _apply_actionable_metadata(ctx, named_metadata)
    _apply_actionable_metadata(ctx, action_spec_metadata)
    _apply_actionable_metadata(ctx, extra)
    for key, value in extra.items():
        if key not in ctx and value not in (None, "", [], {}):
            ctx[key] = value
    if authoritative_profile_id is not None:
        # An explicit value, including empty, is the result of Profile-home
        # authority resolution. Environment and content metadata cannot revive
        # or replace it.
        ctx["profile_id"] = authoritative_profile_id
    if envelope_errors:
        ctx["envelope_errors"] = envelope_errors
        ctx.setdefault(
            "actionability",
            {
                "requires_user_reply": True,
                "intent": "confirmation",
                "risk": "normal",
                "detected_by": "invalid_actionable_envelope",
            },
        )
    if not ctx.get("gate_mode"):
        ctx["gate_mode"] = os.getenv("HERMES_OUTBOUND_GATE_MODE", "enforce")
    if "looks_actionable" not in ctx:
        if source_kind == "gateway_reply":
            ctx["looks_actionable"] = _declares_actionable_metadata(ctx)
        else:
            ctx["looks_actionable"] = looks_actionable(ctx)
    return ctx


def _closed_decision(context: dict[str, Any], reason: str, errors: Iterable[str] = ()) -> BoundaryDecision:
    decision = "hold" if _source_kind(context) in OPERATOR_SOURCE_KINDS or context.get("enforced_channel") is True else "deny"
    raw = {
        "schema_version": EVENT_SCHEMA_VERSION,
        "decision": decision,
        "reason": reason,
        "audit": {"gate": "runtime-backstop", "source_kind": _source_kind(context)},
    }
    return BoundaryDecision(
        transmit=False,
        decision=decision,
        content=content_text(context),
        reason=reason,
        raw=raw,
        errors=list(errors),
    )


def _allow_decision(context: dict[str, Any], reason: str = "allow", raw: dict[str, Any] | None = None) -> BoundaryDecision:
    return BoundaryDecision(
        transmit=True,
        decision="allow",
        content=content_text(context),
        reason=reason,
        raw=raw or {"decision": "allow", "reason": reason},
    )


async def _call_handler(fn: Any, event_type: str, context: dict[str, Any], timeout_seconds: float) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await asyncio.wait_for(fn(event_type, context), timeout=timeout_seconds)
    result = await asyncio.wait_for(
        asyncio.to_thread(fn, event_type, context),
        timeout=timeout_seconds,
    )
    if inspect.isawaitable(result):
        return await asyncio.wait_for(result, timeout=timeout_seconds)
    return result


def _with_hook_identity(
    result: Any,
    hook_name: str,
    capabilities: Iterable[str] = (),
) -> Any:
    if not isinstance(result, dict):
        return result
    payload = _without_hook_identity(result)
    normalized_capabilities = sorted(
        value for value in capabilities if isinstance(value, str) and value
    )
    if not hook_name and not normalized_capabilities:
        return payload
    return {
        **payload,
        _HOOK_IDENTITY_KEY: hook_name,
        _HOOK_CAPABILITIES_KEY: normalized_capabilities,
    }


def _is_required_screening_result(result: Any) -> bool:
    return bool(
        isinstance(result, dict)
        and REQUIRED_SCREENING_CAPABILITY in result.get(_HOOK_CAPABILITIES_KEY, [])
    )


def _without_hook_identity(result: Any) -> Any:
    if not isinstance(result, dict):
        return result
    return {
        key: value
        for key, value in result.items()
        if key not in {_HOOK_IDENTITY_KEY, _HOOK_CAPABILITIES_KEY}
    }


async def _emit_collect_strict(
    hooks: Any,
    event_type: str,
    context: dict[str, Any],
    *,
    timeout_seconds: float,
) -> tuple[list[Any], list[str]]:
    if hooks is None:
        return [], []

    resolve_with_metadata = getattr(hooks, "resolve_handlers_with_metadata", None)
    resolve = getattr(hooks, "_resolve_handlers", None)
    if callable(resolve_with_metadata) and type(hooks) is HookRegistry:
        try:
            handler_entries = list(resolve_with_metadata(event_type))
        except Exception as exc:  # noqa: BLE001 - hook registry failure is a boundary fault
            return [], [f"resolve_handlers:{exc}"]
    elif callable(resolve):
        try:
            handler_entries = [(handler, {}) for handler in resolve(event_type)]
        except Exception as exc:  # noqa: BLE001 - hook registry failure is a boundary fault
            return [], [f"resolve_handlers:{exc}"]
    else:
        handler_entries = []

    if callable(resolve_with_metadata) or callable(resolve):
        results: list[Any] = []
        errors: list[str] = []
        for fn, metadata in handler_entries:
            try:
                handler_context = copy.deepcopy(context)
                result = await _call_handler(
                    fn,
                    event_type,
                    handler_context,
                    timeout_seconds,
                )
            except asyncio.TimeoutError:
                errors.append("timeout")
                continue
            except Exception as exc:  # noqa: BLE001 - fail closed when boundary is required
                errors.append(type(exc).__name__ or "hook_error")
                continue
            if result is not None:
                results.append(
                    _with_hook_identity(
                        result,
                        _string(metadata.get("owner")),
                        metadata.get("capabilities", []),
                    )
                )
        return results, errors

    emit_collect = getattr(hooks, "emit_collect", None)
    if not callable(emit_collect):
        return [], []
    try:
        result = emit_collect(event_type, copy.deepcopy(context))
        if inspect.isawaitable(result):
            result = await asyncio.wait_for(result, timeout=timeout_seconds)
    except asyncio.TimeoutError:
        return [], ["timeout"]
    except Exception as exc:  # noqa: BLE001
        return [], [type(exc).__name__ or "hook_error"]
    if result is None:
        return [], []
    if isinstance(result, list):
        return [_without_hook_identity(item) for item in result], []
    return [_without_hook_identity(result)], []


def _normalize_result(context: dict[str, Any], result: Any) -> BoundaryDecision | None:
    if not isinstance(result, dict):
        if requires_boundary(context):
            return _closed_decision(context, "malformed_handler_result")
        return None
    decision = _string(result.get("decision")).lower()
    if not decision:
        if requires_boundary(context):
            return _closed_decision(context, "malformed_handler_result")
        return None
    if decision == "allow":
        return _allow_decision(context, _string(result.get("reason") or "authorized"), result)
    if decision not in DECISIONS:
        if requires_boundary(context):
            return _closed_decision(context, "malformed_handler_result")
        return None
    if decision in {"deny", "hold"}:
        return BoundaryDecision(
            transmit=False,
            decision=decision,
            content=content_text(context),
            reason=_string(result.get("reason") or decision),
            raw=result,
        )
    if decision == "rewrite":
        rewritten = _string(result.get("content"))
        if not rewritten:
            return _closed_decision(context, "missing_rewrite_content")
        return BoundaryDecision(
            transmit=True,
            decision="rewrite",
            content=rewritten,
            reason=_string(result.get("reason") or "rewrite"),
            raw=result,
        )
    if decision == "downgrade":
        return BoundaryDecision(
            transmit=True,
            decision="downgrade",
            content=_string(result.get("content")) or content_text(context),
            reason=_string(result.get("reason") or "downgrade"),
            raw=result,
        )
    return None


async def outbound_before_send(
    hooks: Any,
    context: dict[str, Any],
    *,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> BoundaryDecision:
    ctx = context if isinstance(context, dict) else {}
    screening_required = ctx.get("output_screening_required") is True
    if not screening_required and not boundary_enabled(ctx):
        return _allow_decision(ctx, "boundary_disabled")
    if not screening_required and gate_mode(ctx) == "off":
        return _allow_decision(ctx, "gate_mode_off")
    if ctx.get("envelope_errors") and requires_boundary(ctx):
        return _closed_decision(
            ctx,
            "invalid_actionable_envelope",
            [str(item) for item in ctx.get("envelope_errors", [])],
        )

    results, errors = await _emit_collect_strict(
        hooks,
        "outbound:before_send",
        ctx,
        timeout_seconds=timeout_seconds,
    )
    if errors and requires_boundary(ctx):
        return _closed_decision(ctx, "hook_error", errors)
    if screening_required and not any(_is_required_screening_result(result) for result in results):
        return _closed_decision(ctx, "required_output_screening_hook_missing", errors)
    if not results:
        if requires_boundary(ctx):
            return _closed_decision(ctx, "no_boundary_decision", errors)
        return _allow_decision(ctx, "not_actionable")

    normalized_results = [
        (result, normalized)
        for result in results
        if (normalized := _normalize_result(ctx, _without_hook_identity(result))) is not None
    ]
    for _, normalized in normalized_results:
        if not normalized.transmit:
            return normalized
    positive_results = [
        normalized
        for result, normalized in normalized_results
        if not screening_required or _is_required_screening_result(result)
    ]
    for normalized in positive_results:
        if normalized.decision in {"rewrite", "downgrade"}:
            return normalized
    best_allow: BoundaryDecision | None = None
    for normalized in positive_results:
        best_allow = normalized

    if best_allow is not None:
        return best_allow
    if requires_boundary(ctx):
        return _closed_decision(ctx, "no_boundary_decision", errors)
    return _allow_decision(ctx, "not_actionable")


async def outbound_after_send(
    hooks: Any,
    context: dict[str, Any],
    *,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> list[Any]:
    ctx = context if isinstance(context, dict) else {}
    screening_required = ctx.get("output_screening_required") is True
    if hooks is None or (
        not screening_required
        and (not boundary_enabled(ctx) or gate_mode(ctx) == "off")
    ):
        return []
    results, errors = await _emit_collect_strict(
        hooks,
        "outbound:after_send",
        ctx,
        timeout_seconds=timeout_seconds,
    )
    if errors:
        logger.warning("outbound:after_send hook error(s): %s", ", ".join(errors))
    if screening_required and not any(_is_required_screening_result(result) for result in results):
        return [{"status": "failed", "reason": "required_output_screening_hook_missing"}]
    return [_without_hook_identity(result) for result in results]


def _run_coro_sync(coro: Any) -> Any:
    def _run_in_fresh_loop() -> Any:
        runner_type = getattr(asyncio, "Runner", None)
        if runner_type is not None:
            with runner_type() as runner:
                return runner.run(coro)
        loop = asyncio.new_event_loop()  # pragma: no cover - Python < 3.11
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return _run_in_fresh_loop()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(_run_in_fresh_loop)
        return future.result()


def outbound_before_send_sync(
    hooks: Any,
    context: dict[str, Any],
    *,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> BoundaryDecision:
    return _run_coro_sync(
        outbound_before_send(hooks, context, timeout_seconds=timeout_seconds)
    )


def outbound_after_send_sync(
    hooks: Any,
    context: dict[str, Any],
    *,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> list[Any]:
    return _run_coro_sync(
        outbound_after_send(hooks, context, timeout_seconds=timeout_seconds)
    )


def send_result_payload(result: Any) -> dict[str, Any]:
    if isinstance(result, dict):
        return dict(result)
    if result is None:
        return {}
    payload: dict[str, Any] = {}
    for key in ("success", "message_id", "outbox_id", "error", "raw_response"):
        if hasattr(result, key):
            value = getattr(result, key)
            if value is not None:
                payload[key] = value
    return payload
