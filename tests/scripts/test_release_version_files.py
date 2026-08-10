from __future__ import annotations

import importlib.util
import json
from pathlib import Path


RELEASE_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "release.py"
REPO_ROOT = RELEASE_SCRIPT.parent.parent


def _load_release_module():
    spec = importlib.util.spec_from_file_location("release_version_test", RELEASE_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_repository_release_versions_are_identical():
    import re

    init_text = (REPO_ROOT / "hermes_cli" / "__init__.py").read_text()
    pyproject_text = (REPO_ROOT / "pyproject.toml").read_text()
    uv_lock_text = (REPO_ROOT / "uv.lock").read_text()
    desktop = json.loads(
        (REPO_ROOT / "apps" / "desktop" / "package.json").read_text()
    )
    package_lock = json.loads((REPO_ROOT / "package-lock.json").read_text())

    versions = {
        re.search(r'^__version__ = "([^"]+)"$', init_text, re.MULTILINE).group(1),
        re.search(
            r'^version = "([^"]+)"$', pyproject_text, re.MULTILINE
        ).group(1),
        re.search(
            r'\[\[package\]\]\nname = "hermes-agent"\nversion = "([^"]+)"',
            uv_lock_text,
        ).group(1),
        desktop["version"],
        package_lock["packages"]["apps/desktop"]["version"],
    }
    release_date = re.search(
        r'^__release_date__ = "([^"]+)"$', init_text, re.MULTILINE
    ).group(1)

    assert len(versions) == 1
    assert release_date == "2026.8.10.3"


def test_version_update_and_stage_list_cover_desktop_lockfile(tmp_path, monkeypatch):
    release = _load_release_module()
    version_file = tmp_path / "hermes_cli" / "__init__.py"
    pyproject = tmp_path / "pyproject.toml"
    desktop_package = tmp_path / "apps" / "desktop" / "package.json"
    package_lock = tmp_path / "package-lock.json"
    uv_lock = tmp_path / "uv.lock"
    version_file.parent.mkdir()
    desktop_package.parent.mkdir(parents=True)
    version_file.write_text(
        '__version__ = "0.20.0"\n__release_date__ = "2026.8.3"\n',
        encoding="utf-8",
    )
    pyproject.write_text('[project]\nversion = "0.20.0"\n', encoding="utf-8")
    desktop_package.write_text(
        json.dumps({"name": "hermes", "version": "0.17.0"}, indent=2) + "\n",
        encoding="utf-8",
    )
    package_lock.write_text(
        json.dumps(
            {"packages": {"": {"version": "1.0.0"}, "apps/desktop": {"version": "0.17.0"}}},
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    uv_lock.write_text(
        'version = 1\n\n[[package]]\nname = "hermes-agent"\nversion = "0.20.0"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(release, "VERSION_FILE", version_file)
    monkeypatch.setattr(release, "PYPROJECT_FILE", pyproject)
    monkeypatch.setattr(release, "DESKTOP_PACKAGE_FILE", desktop_package)
    monkeypatch.setattr(release, "PACKAGE_LOCK_FILE", package_lock)
    monkeypatch.setattr(release, "UV_LOCK_FILE", uv_lock)

    release.update_version_files("0.20.1", "2026.8.9")

    assert '__version__ = "0.20.1"' in version_file.read_text(encoding="utf-8")
    assert '__release_date__ = "2026.8.9"' in version_file.read_text(encoding="utf-8")
    assert 'version = "0.20.1"' in pyproject.read_text(encoding="utf-8")
    assert json.loads(desktop_package.read_text(encoding="utf-8"))["version"] == "0.20.1"
    lock = json.loads(package_lock.read_text(encoding="utf-8"))
    assert lock["packages"]["apps/desktop"]["version"] == "0.20.1"
    assert 'version = "0.20.1"' in uv_lock.read_text(encoding="utf-8")
    assert release.version_files_to_stage() == [
        str(version_file),
        str(pyproject),
        str(desktop_package),
        str(package_lock),
        str(uv_lock),
    ]
