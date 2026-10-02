"""Contract tests for the public package-version docs hook."""

from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DOCS = ROOT / "docs"
HOOK_PATH = DOCS / "scripts" / "inject_public_version.py"


def _load_hook():
    spec = importlib.util.spec_from_file_location("datacoolie_public_version", HOOK_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_version_is_loaded_once_per_build_and_refreshed(monkeypatch) -> None:
    hook = _load_hook()
    versions = iter(("0.2.0", "0.3.0"))
    calls: list[str] = []

    def fake_current_version() -> str:
        version = next(versions)
        calls.append(version)
        return version

    monkeypatch.setattr(hook, "current_version", fake_current_version)

    hook.on_pre_build({})
    assert hook.on_page_markdown("Version {{ datacoolie_version }}", None, None, None) == (
        "Version 0.2.0"
    )
    assert hook.on_page_markdown("No token here", None, None, None) == "No token here"

    hook.on_pre_build({})
    assert hook.on_page_markdown("Version {{ datacoolie_version }}", None, None, None) == (
        "Version 0.3.0"
    )
    assert calls == ["0.2.0", "0.3.0"]


def test_token_free_content_does_not_resolve_version(monkeypatch) -> None:
    hook = _load_hook()

    def fail_current_version() -> str:
        raise AssertionError("version should not be resolved for token-free content")

    monkeypatch.setattr(hook, "current_version", fail_current_version)
    hook._BUILD_VERSION = None

    assert hook._render_version("plain content") == "plain content"


def test_post_build_replaces_declared_text_asset_only(tmp_path) -> None:
    hook = _load_hook()
    site_dir = tmp_path / "site"
    site_dir.mkdir()
    (site_dir / "llms.txt").write_text(
        "Version: {{ datacoolie_version }}\n", encoding="utf-8"
    )
    unrelated_html = site_dir / "index.html"
    unrelated_html.write_text(
        "{{ datacoolie_version }}", encoding="utf-8"
    )
    unrelated_text = site_dir / "llms-full.txt"
    unrelated_text.write_text(
        "{{ datacoolie_version }}", encoding="utf-8"
    )

    hook._BUILD_VERSION = "0.2.0"
    hook.on_post_build({"site_dir": site_dir})

    assert (site_dir / "llms.txt").read_text(encoding="utf-8") == "Version: 0.2.0\n"
    assert unrelated_html.read_text(encoding="utf-8") == "{{ datacoolie_version }}"
    assert unrelated_text.read_text(encoding="utf-8") == "{{ datacoolie_version }}"


def test_versioned_text_asset_inventory_matches_hook_owner() -> None:
    hook = _load_hook()
    source_assets = {
        path.relative_to(DOCS).as_posix()
        for path in DOCS.rglob("*.txt")
        if hook.VERSION_TOKEN in path.read_text(encoding="utf-8")
    }

    assert source_assets == set(hook.VERSIONED_TEXT_ASSETS)
