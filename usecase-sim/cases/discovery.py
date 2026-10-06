"""Filesystem-only discovery used by the simulator case catalog facade."""

from __future__ import annotations

import sys
from pathlib import Path


_MODULE_DIR = str(Path(__file__).resolve().parents[1])
if _MODULE_DIR not in sys.path:
    sys.path.insert(0, _MODULE_DIR)


def discover_authored_json(root: str | Path | None = None) -> list[str]:
    """Discover authored JSON outside runtime, catalog, and registry roots."""

    from cases.catalog import _EXCLUDED_PARTS, _slash, simulator_root

    sim_root = simulator_root(root)
    files: list[str] = []
    if not sim_root.exists():
        return files
    for path in sorted(sim_root.rglob("*.json")):
        relative = path.relative_to(sim_root)
        if any(part in _EXCLUDED_PARTS for part in relative.parts):
            continue
        files.append(_slash(relative))
    return files


def discover_flows(root: str | Path | None = None) -> list[dict[str, str]]:
    """Return every authored ``(fixture, flow)`` pair in simulator JSON."""

    from cases.catalog import _read_json, simulator_root

    sim_root = simulator_root(root)
    result: list[dict[str, str]] = []
    for relative in discover_authored_json(sim_root):
        path = sim_root / relative
        try:
            value = _read_json(path)
        except Exception:
            # Parse diagnostics are emitted by ``audit_catalog`` where the
            # inventory exclusion list is available.
            continue
        if not isinstance(value, dict) or not isinstance(value.get("dataflows"), list):
            continue
        for flow in value["dataflows"]:
            if isinstance(flow, dict) and isinstance(flow.get("name"), str) and flow["name"]:
                result.append({"fixture": relative, "flow": flow["name"]})
    return result


def discover_scripts(root: str | Path | None = None) -> list[str]:
    """Discover authored Python and notebook routes in the simulator."""

    from cases.catalog import _relative, simulator_root

    sim_root = simulator_root(root)
    scripts_root = sim_root / "scripts"
    if not scripts_root.is_dir():
        return []
    result: list[str] = []
    for path in sorted(scripts_root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        result.append(_relative(sim_root, path))
    for parent in (sim_root / "metadata", sim_root / "platforms"):
        if not parent.is_dir():
            continue
        for suffix in ("*.py", "*.ipynb"):
            for path in sorted(parent.rglob(suffix)):
                if "__pycache__" not in path.parts:
                    result.append(_relative(sim_root, path))
    return result


__all__ = ["discover_authored_json", "discover_flows", "discover_scripts"]
