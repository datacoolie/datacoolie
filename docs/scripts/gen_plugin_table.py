"""Generate `reference/plugin-entry-points.md` from pyproject.toml.

Runs as an mkdocs-gen-files script at docs-build time so the plugin table
never drifts from the canonical entry-point declarations.
"""

from __future__ import annotations

from pathlib import Path

import mkdocs_gen_files

try:
    import tomllib  # py3.11+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]


PYPROJECT = Path(__file__).resolve().parents[2] / "pyproject.toml"

GROUPS = [
    ("datacoolie.engines", "Engines"),
    ("datacoolie.platforms", "Platforms"),
    ("datacoolie.sources", "Sources"),
    ("datacoolie.destinations", "Destinations"),
    ("datacoolie.transformers", "Transformers"),
    ("datacoolie.resolvers", "Secret resolvers"),
]


def _load_plugins() -> dict[str, dict[str, str]]:
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    project_entry_points = data.get("project", {}).get("entry-points", {})
    if project_entry_points:
        return project_entry_points
    return data.get("tool", {}).get("poetry", {}).get("plugins", {})


def _render(plugins: dict[str, dict[str, str]]) -> str:
    out: list[str] = []
    out.append("---\n")
    out.append("title: Plugin Entry Points Reference | DataCoolie\n")
    out.append(
        "description: Generated reference for DataCoolie Python entry-point groups covering engines, platforms, sources, destinations, transformers, and secret resolvers.\n"
    )
    out.append("---\n\n")
    out.append("# Plugin entry points\n")
    out.append(
        "DataCoolie registers built-ins in-process and also discovers installed plugins through "
        "[Python entry points](https://packaging.python.org/en/latest/specifications/entry-points/). "
        "The tables below are **generated from this repository's `pyproject.toml`** at docs-build "
        "time and show its packaged entry-point declarations.\n"
    )
    out.append(
        "To ship your own plugin, declare the matching entry-point group in your package's "
        "`pyproject.toml`; DataCoolie discovers it on the first registry lookup.\n"
    )
    for group, title in GROUPS:
        entries = plugins.get(group, {})
        out.append(f"\n## {title}\n")
        out.append(f"Entry-point group: `{group}`\n")
        if not entries:
            out.append("\n_(none)_\n")
            continue
        out.append("\n| Name | Target |\n|---|---|")
        for name in sorted(entries):
            target = entries[name]
            out.append(f"\n| `{name}` | `{target}` |")
        out.append("\n")
    out.append(
        "\n## In-process-only built-ins\n\n"
        "`row_filter` is registered directly by `datacoolie.__init__` and is part of the "
        "default transformer pipeline, but it is not currently declared in the packaged "
        "`datacoolie.transformers` entry-point group above.\n"
    )
    return "".join(out)


plugins = _load_plugins()
with mkdocs_gen_files.open("reference/plugin-entry-points.md", "w") as fp:
    fp.write(_render(plugins))
