#!/usr/bin/env python3
"""Emit deterministic capability evidence from the installed DataCoolie runtime."""

from __future__ import annotations

import argparse
import json
import re
import sys
from importlib.metadata import PackageNotFoundError, entry_points, requires, version
from pathlib import Path
from typing import Any


REGISTRY_GROUPS = {
    "engines": "datacoolie.engines",
    "platforms": "datacoolie.platforms",
    "sources": "datacoolie.sources",
    "destinations": "datacoolie.destinations",
    "transformers": "datacoolie.transformers",
    "resolvers": "datacoolie.resolvers",
}
_DISTRIBUTION_NAME = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")


def _entry_point_records(group: str) -> list[dict[str, str | None]]:
    records: list[dict[str, str | None]] = []
    for entry_point in entry_points(group=group):
        distribution = getattr(entry_point, "dist", None)
        records.append(
            {
                "name": entry_point.name,
                "value": entry_point.value,
                "distribution": getattr(distribution, "name", None),
                "version": getattr(distribution, "version", None),
            }
        )
    return sorted(
        records,
        key=lambda item: (
            item["name"] or "",
            item["value"] or "",
            item["distribution"] or "",
            item["version"] or "",
        ),
    )


def collect_capabilities() -> dict[str, Any]:
    """Collect installed registrations without instantiating plugins."""
    try:
        import datacoolie
        from datacoolie import (
            destination_registry,
            engine_registry,
            platform_registry,
            resolver_registry,
            source_registry,
            transformer_registry,
        )
    except ImportError as exc:
        raise RuntimeError("datacoolie must be installed before capability inspection") from exc

    try:
        distribution_version = version("datacoolie")
    except PackageNotFoundError as exc:
        raise RuntimeError("installed datacoolie distribution metadata is unavailable") from exc

    declared_requirements = sorted(requires("datacoolie") or [])
    dependency_status: dict[str, dict[str, Any]] = {}
    for declaration in declared_requirements:
        match = _DISTRIBUTION_NAME.match(declaration)
        if match is None:
            continue
        dependency = match.group(1).lower().replace("_", "-")
        record = dependency_status.setdefault(
            dependency,
            {"declarations": [], "installed_version": None},
        )
        record["declarations"].append(declaration)
        if record["installed_version"] is None:
            try:
                record["installed_version"] = version(dependency)
            except PackageNotFoundError:
                pass

    registries = {
        "engines": sorted(engine_registry.list_plugins()),
        "platforms": sorted(platform_registry.list_plugins()),
        "sources": sorted(source_registry.list_plugins()),
        "destinations": sorted(destination_registry.list_plugins()),
        "transformers": sorted(transformer_registry.list_plugins()),
        "resolvers": sorted(resolver_registry.list_plugins()),
    }
    return {
        "schema_version": 1,
        "distribution": {
            "name": "datacoolie",
            "version": distribution_version,
            "module_version": getattr(datacoolie, "__version__", None),
            "version_match": getattr(datacoolie, "__version__", None)
            in (None, distribution_version),
            "module_path": str(Path(datacoolie.__file__).resolve()),
        },
        "requirements": declared_requirements,
        "dependency_status": dict(sorted(dependency_status.items())),
        "registries": registries,
        "entry_points": {
            name: _entry_point_records(group)
            for name, group in REGISTRY_GROUPS.items()
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="Optional JSON evidence output path")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON")
    args = parser.parse_args()
    try:
        payload = collect_capabilities()
        text = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":") if args.compact else None,
            indent=None if args.compact else 2,
        ) + "\n"
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(text, encoding="utf-8")
        else:
            print(text, end="")
    except (OSError, RuntimeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
