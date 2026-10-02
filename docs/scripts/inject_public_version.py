"""ProperDocs hook: render the package version from ``pyproject.toml``."""

from __future__ import annotations

import tomllib
from pathlib import Path


VERSION_TOKEN = "{{ datacoolie_version }}"
REPO_ROOT = Path(__file__).resolve().parents[2]
VERSIONED_TEXT_ASSETS = ("llms.txt",)
_BUILD_VERSION: str | None = None


def current_version() -> str:
    """Return the public package version from the project manifest."""
    manifest = REPO_ROOT / "pyproject.toml"
    with manifest.open("rb") as stream:
        project = tomllib.load(stream)["project"]
    version = project.get("version")
    if not isinstance(version, str) or not version:
        raise ValueError(f"Missing project version in {manifest}")
    return version


def _render_version(content: str) -> str:
    if VERSION_TOKEN not in content:
        return content
    version = _BUILD_VERSION or current_version()
    return content.replace(VERSION_TOKEN, version)


def on_pre_build(config) -> None:  # noqa: ANN001
    """Refresh the package version for each clean or serve rebuild."""
    global _BUILD_VERSION
    _BUILD_VERSION = current_version()


def on_page_markdown(markdown: str, page, config, files) -> str:  # noqa: ANN001
    """Replace the version token in Markdown pages before rendering."""
    return _render_version(markdown)


def on_post_build(config) -> None:  # noqa: ANN001
    """Replace the token in declared text assets copied directly to the site."""
    site_dir = Path(config["site_dir"])
    if not site_dir.exists():
        return

    for relative_path in VERSIONED_TEXT_ASSETS:
        path = site_dir / relative_path
        if not path.is_file():
            continue
        content = path.read_text(encoding="utf-8")
        rendered = _render_version(content)
        if rendered != content:
            path.write_text(rendered, encoding="utf-8")
