"""Canonical, profile-explicit skill resolution for durable references."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Iterable, Mapping, Optional

from agent.skill_utils import is_excluded_skill_path, iter_skill_index_files, parse_frontmatter


SKILL_BINDING_SCHEMA_VERSION = "skill-binding/v1"
SKILL_RESOLVER_VERSION = "profile-skill-resolver/v1"


class SkillResolutionError(ValueError):
    """A selector cannot be bound to one skill visible in the active profile."""

    def __init__(
        self,
        code: str,
        selector: str,
        message: str,
        *,
        matches: Optional[Iterable[str]] = None,
    ) -> None:
        self.code = code
        self.selector = selector
        self.matches = tuple(matches or ())
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class SkillBinding:
    canonical_name: str
    relative_path: Optional[str]
    source_kind: str
    content_digest: str
    resolver_version: str = SKILL_RESOLVER_VERSION
    schema_version: str = SKILL_BINDING_SCHEMA_VERSION

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "canonical_name": self.canonical_name,
            "relative_path": self.relative_path,
            "source_kind": self.source_kind,
            "content_digest": self.content_digest,
            "resolver_version": self.resolver_version,
        }


def _selector_error(selector: Any) -> Optional[str]:
    if not isinstance(selector, str):
        return "Skill selector must be a string."
    candidate = selector.strip()
    if not candidate:
        return "Skill selector must not be empty."
    if (
        PurePosixPath(candidate).is_absolute()
        or PureWindowsPath(candidate).is_absolute()
        or PureWindowsPath(candidate).drive
    ):
        return "Skill selector must be relative to the active profile."
    if any(part == ".." for part in PurePosixPath(candidate.replace("\\", "/")).parts):
        return "Skill selector cannot contain '..' path traversal components."
    return None


def _skill_tree_snapshot(skill_md: Path) -> tuple[str, bytes]:
    skill_dir = skill_md.parent
    digest = hashlib.sha256()
    try:
        entries = (
            [skill_md]
            if skill_md.name != "SKILL.md"
            else sorted(
                skill_dir.rglob("*"),
                key=lambda item: item.relative_to(skill_dir).as_posix(),
            )
        )
        skill_raw: bytes | None = None
        for entry in entries:
            relative = entry.name if skill_md.name != "SKILL.md" else entry.relative_to(skill_dir).as_posix()
            if entry.is_symlink():
                raise SkillResolutionError(
                    "skill_symlink_unsupported",
                    str(skill_md),
                    f"Skill tree entry {relative!r} is a symbolic link.",
                )
            if not entry.is_file():
                continue
            raw = entry.read_bytes()
            if entry == skill_md:
                skill_raw = raw
            encoded_name = relative.encode("utf-8")
            digest.update(len(encoded_name).to_bytes(8, "big"))
            digest.update(encoded_name)
            digest.update((entry.stat(follow_symlinks=False).st_mode & 0o7777).to_bytes(4, "big"))
            digest.update(len(raw).to_bytes(8, "big"))
            digest.update(raw)
    except SkillResolutionError:
        raise
    except OSError as exc:
        raise SkillResolutionError(
            "skill_unreadable",
            str(skill_md),
            f"Skill tree could not be read from {skill_dir}.",
        ) from exc
    if skill_raw is None:
        raise SkillResolutionError(
            "skill_unreadable",
            str(skill_md),
            f"Skill metadata could not be read from {skill_md}.",
        )
    return "sha256:" + digest.hexdigest(), skill_raw


def _read_skill(path: Path, *, canonical_override: Optional[str] = None) -> SkillBinding:
    try:
        content_digest, raw = _skill_tree_snapshot(path)
        frontmatter, _ = parse_frontmatter(raw.decode("utf-8-sig"))
    except (OSError, UnicodeError) as exc:
        raise SkillResolutionError(
            "skill_unreadable",
            str(path),
            f"Skill metadata could not be read from {path}.",
        ) from exc

    canonical_name = str(
        canonical_override
        or frontmatter.get("name")
        or (path.parent.name if path.name == "SKILL.md" else path.stem)
    ).strip()
    if not canonical_name:
        raise SkillResolutionError(
            "skill_identity_missing",
            str(path),
            f"Skill at {path} has no canonical name.",
        )
    return SkillBinding(
        canonical_name=canonical_name,
        relative_path=None,
        source_kind="profile_local",
        content_digest=content_digest,
    )


def _reject_symlink_components(path: Path, root: Path, selector: str) -> None:
    current = path
    while True:
        if current.is_symlink():
            raise SkillResolutionError(
                "skill_symlink_unsupported",
                selector,
                f"Skill path component {current} is a symbolic link.",
            )
        if current == root:
            return
        parent = current.parent
        if parent == current:
            raise SkillResolutionError(
                "skill_not_profile_local",
                selector,
                f"Skill path {path} is not beneath profile {root}.",
            )
        current = parent


def _profile_candidates(profile_home: Path, selector: str) -> list[Path]:
    skills_root = profile_home / "skills"
    if not skills_root.is_dir():
        return []

    local_selector = selector
    if ":" in selector:
        namespace, separator, bare = selector.partition(":")
        if not separator or not namespace or not bare or ":" in bare:
            raise SkillResolutionError(
                "skill_selector_invalid",
                selector,
                f"Qualified skill selector {selector!r} is malformed.",
            )
        local_selector = f"{namespace}/{bare}"

    candidates: list[Path] = []
    seen: set[Path] = set()

    def record(path: Path) -> None:
        if not path.is_file() or (path.name != "SKILL.md" and path.suffix != ".md"):
            return
        try:
            key = path.resolve()
        except OSError:
            key = path.absolute()
        if key not in seen:
            seen.add(key)
            candidates.append(path)

    direct = skills_root / local_selector
    if direct.is_dir():
        record(direct / "SKILL.md")
    elif direct.name == "SKILL.md" or direct.suffix == ".md":
        record(direct)
    else:
        record(direct.with_suffix(".md"))

    # Qualified/category paths are exact. Bare selectors also match directory
    # and frontmatter names so the same aliases accepted by skill_view converge.
    if "/" not in local_selector:
        for skill_md in iter_skill_index_files(skills_root, "SKILL.md"):
            if skill_md.parent.name == local_selector:
                record(skill_md)
                continue
            try:
                frontmatter, _ = parse_frontmatter(
                    skill_md.read_text(encoding="utf-8-sig")
                )
            except (OSError, UnicodeError):
                continue
            if str(frontmatter.get("name") or "").strip() == local_selector:
                record(skill_md)
        for flat_md in skills_root.rglob(f"{local_selector}.md"):
            if flat_md.name != "SKILL.md" and not is_excluded_skill_path(
                flat_md, root=skills_root
            ):
                record(flat_md)

    return candidates


def _plugin_binding(selector: str, plugin_skill_path: Path) -> SkillBinding:
    binding = _read_skill(plugin_skill_path, canonical_override=selector)
    return SkillBinding(
        canonical_name=selector,
        relative_path=None,
        source_kind="plugin",
        content_digest=binding.content_digest,
    )


def resolve_skill_ref(
    profile_home: str | Path,
    selector: str,
    *,
    plugin_skill_path: str | Path | None = None,
) -> dict[str, Any]:
    """Resolve one selector without mutating profile, plugin, or Cron state."""
    error = _selector_error(selector)
    if error:
        raise SkillResolutionError("skill_selector_invalid", str(selector or ""), error)

    normalized_selector = selector.strip()
    profile = Path(profile_home).expanduser().resolve(strict=False)
    local_candidates = _profile_candidates(profile, normalized_selector)
    plugin_path = (
        Path(plugin_skill_path).expanduser().resolve(strict=False)
        if plugin_skill_path is not None
        else None
    )
    plugin_exists = bool(plugin_path and plugin_path.is_file())

    if plugin_skill_path is not None and not plugin_exists:
        raise SkillResolutionError(
            "skill_plugin_target_missing",
            normalized_selector,
            f"Registered plugin skill {normalized_selector!r} is missing.",
        )

    if local_candidates and plugin_exists:
        matches = [str(path) for path in local_candidates]
        matches.append(str(plugin_path))
        raise SkillResolutionError(
            "skill_plugin_local_conflict",
            normalized_selector,
            f"Selector {normalized_selector!r} matches both a plugin and a profile-local skill.",
            matches=matches,
        )
    if len(local_candidates) > 1:
        raise SkillResolutionError(
            "skill_ambiguous_in_active_profile",
            normalized_selector,
            f"Selector {normalized_selector!r} matches multiple profile-local skills.",
            matches=[str(path) for path in local_candidates],
        )
    if plugin_exists:
        return _plugin_binding(normalized_selector, plugin_path).as_dict()
    if not local_candidates:
        raise SkillResolutionError(
            "skill_unavailable_in_active_profile",
            normalized_selector,
            f"Skill {normalized_selector!r} is not visible in profile {profile}.",
        )

    path = local_candidates[0]
    _reject_symlink_components(path, profile, normalized_selector)
    try:
        resolved_path = path.resolve(strict=True)
        resolved_path.relative_to(profile)
    except (OSError, ValueError) as exc:
        raise SkillResolutionError(
            "skill_not_profile_local",
            normalized_selector,
            f"Skill {normalized_selector!r} resolves outside profile {profile}.",
        ) from exc
    binding = _read_skill(path)
    try:
        relative_path = path.relative_to(profile).as_posix()
    except ValueError as exc:
        raise SkillResolutionError(
            "skill_not_profile_local",
            normalized_selector,
            f"Skill {normalized_selector!r} is outside profile {profile}.",
        ) from exc
    return SkillBinding(
        canonical_name=binding.canonical_name,
        relative_path=relative_path,
        source_kind="profile_local",
        content_digest=binding.content_digest,
    ).as_dict()


def resolve_skill_refs(
    profile_home: str | Path,
    selectors: Iterable[str],
    *,
    plugin_skill_paths: Optional[Mapping[str, str | Path]] = None,
) -> list[dict[str, Any]]:
    """Resolve and identity-deduplicate an ordered selector collection."""
    plugin_paths = plugin_skill_paths or {}
    bindings: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    identities_by_name: dict[str, tuple[Any, ...]] = {}
    for selector in selectors:
        binding = resolve_skill_ref(
            profile_home,
            selector,
            plugin_skill_path=plugin_paths.get(selector),
        )
        identity = (
            binding["canonical_name"],
            binding["relative_path"],
            binding["source_kind"],
            binding["content_digest"],
        )
        prior = identities_by_name.get(binding["canonical_name"])
        if prior is not None and prior != identity:
            raise SkillResolutionError(
                "skill_canonical_name_collision",
                binding["canonical_name"],
                f"Canonical skill name {binding['canonical_name']!r} resolves to multiple targets.",
            )
        identities_by_name[binding["canonical_name"]] = identity
        if identity not in seen:
            seen.add(identity)
            bindings.append(binding)
    return bindings


def validate_skill_binding(
    profile_home: str | Path,
    binding: Mapping[str, Any],
    *,
    plugin_skill_path: str | Path | None = None,
) -> Path:
    """Validate a persisted binding and return its exact SKILL.md path."""
    required = {
        "schema_version",
        "canonical_name",
        "relative_path",
        "source_kind",
        "content_digest",
        "resolver_version",
    }
    if set(binding) != required:
        raise SkillResolutionError(
            "skill_binding_invalid",
            str(binding.get("canonical_name") or ""),
            "Persisted skill binding has an invalid shape.",
        )
    if (
        binding.get("schema_version") != SKILL_BINDING_SCHEMA_VERSION
        or binding.get("resolver_version") != SKILL_RESOLVER_VERSION
    ):
        raise SkillResolutionError(
            "skill_binding_version_unsupported",
            str(binding.get("canonical_name") or ""),
            "Persisted skill binding uses an unsupported version.",
        )
    source_kind = binding.get("source_kind")
    if source_kind not in {"profile_local", "plugin"}:
        raise SkillResolutionError(
            "skill_binding_source_invalid",
            str(binding.get("canonical_name") or ""),
            "Persisted skill binding has an invalid source kind.",
        )

    profile = Path(profile_home).expanduser().resolve(strict=False)
    if source_kind == "plugin":
        if binding.get("relative_path") is not None:
            raise SkillResolutionError(
                "skill_binding_path_invalid",
                str(binding.get("canonical_name") or ""),
                "Persisted plugin skill binding must not contain a profile path.",
            )
        resolved = resolve_skill_ref(
            profile,
            str(binding.get("canonical_name") or ""),
            plugin_skill_path=plugin_skill_path,
        )
        if resolved != dict(binding):
            raise SkillResolutionError(
                "skill_binding_mismatch",
                str(binding.get("canonical_name") or ""),
                "Persisted plugin skill binding no longer matches its registered target.",
            )
        return Path(plugin_skill_path).expanduser().resolve(strict=False)

    relative = str(binding.get("relative_path") or "")
    error = _selector_error(relative)
    if error or not relative.startswith("skills/") or not relative.endswith(".md"):
        raise SkillResolutionError(
            "skill_binding_path_invalid",
            str(binding.get("canonical_name") or ""),
            "Persisted skill binding path is not profile-local.",
        )
    path = profile / relative
    if not path.is_file():
        raise SkillResolutionError(
            "skill_binding_target_missing",
            str(binding.get("canonical_name") or ""),
            f"Persisted skill binding target {relative!r} is missing.",
        )
    _reject_symlink_components(
        path,
        profile,
        str(binding.get("canonical_name") or ""),
    )
    try:
        resolved_path = path.resolve(strict=True)
        resolved_path.relative_to(profile)
    except (OSError, ValueError) as exc:
        raise SkillResolutionError(
            "skill_not_profile_local",
            str(binding.get("canonical_name") or ""),
            f"Persisted skill binding target {relative!r} resolves outside the profile.",
        ) from exc
    selector = relative.removeprefix("skills/")
    selector = (
        selector.removesuffix("/SKILL.md")
        if selector.endswith("/SKILL.md")
        else selector.removesuffix(".md")
    )
    resolved = resolve_skill_ref(profile, selector)
    if resolved != dict(binding):
        raise SkillResolutionError(
            "skill_binding_mismatch",
            str(binding.get("canonical_name") or ""),
            f"Persisted skill binding target {relative!r} changed after authorization.",
        )
    return resolved_path


def load_verified_skill_content(
    profile_home: str | Path,
    binding: Mapping[str, Any],
    *,
    plugin_skill_path: str | Path | None = None,
) -> tuple[Path, str]:
    """Return the exact SKILL.md bytes whose complete tree matches a binding."""
    path = validate_skill_binding(
        profile_home,
        binding,
        plugin_skill_path=plugin_skill_path,
    )
    verified, raw = _skill_tree_snapshot(path)
    if verified != binding.get("content_digest"):
        raise SkillResolutionError(
            "skill_binding_mismatch",
            str(binding.get("canonical_name") or ""),
            "Persisted skill binding changed while it was being loaded.",
        )
    try:
        return path, raw.decode("utf-8-sig")
    except UnicodeError as exc:
        raise SkillResolutionError(
            "skill_unreadable",
            str(binding.get("canonical_name") or ""),
            "Persisted skill binding is not valid UTF-8.",
        ) from exc
