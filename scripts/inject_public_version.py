"""ProperDocs hook: render the package version from ``pyproject.toml``."""

from __future__ import annotations

import tomllib
from pathlib import Path


VERSION_TOKEN = "{{ datacoolie_version }}"
REPO_ROOT = Path(__file__).resolve().parents[2]


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
    return content.replace(VERSION_TOKEN, current_version())


def on_page_markdown(markdown: str, page, config, files) -> str:  # noqa: ANN001
    """Replace the version token in Markdown pages before rendering."""
    return _render_version(markdown)


def on_post_build(config) -> None:  # noqa: ANN001
    """Replace the token in text assets copied directly to the site."""
    site_dir = Path(config["site_dir"])
    if not site_dir.exists():
        return

    for path in site_dir.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in {".html", ".txt"}:
            continue
        content = path.read_text(encoding="utf-8")
        rendered = _render_version(content)
        if rendered != content:
            path.write_text(rendered, encoding="utf-8")
