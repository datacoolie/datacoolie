#!/usr/bin/env python3
"""Resolve canonical modular DataCoolie metadata for one environment."""

from __future__ import annotations

import argparse
import json
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any


SECTION_KEYS = ("connections", "dataflows", "schema_hints")
ALLOWED_OVERLAY_KEYS = {"$schema", *SECTION_KEYS}


def _load_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path}: {exc}") from exc


def _section_items(data: Any, key: str, path: Path) -> list[dict[str, Any]]:
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict) and key in data:
        items = data[key]
    elif key == "dataflows" and isinstance(data, dict):
        items = [data]
    else:
        raise ValueError(f"{path} must be an array or an object containing '{key}'")
    if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
        raise ValueError(f"{path} section '{key}' must be an array of objects")
    return deepcopy(items)


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in overlay.items():
        if isinstance(result.get(key), dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def _merge_named(
    base_items: list[dict[str, Any]],
    overlay_items: list[dict[str, Any]],
    section: str,
) -> list[dict[str, Any]]:
    def keyed(items: list[dict[str, Any]], label: str) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for index, item in enumerate(items):
            name = item.get("name")
            if not isinstance(name, str) or not name:
                raise ValueError(f"{label}[{index}] requires a non-empty name")
            if name in result:
                raise ValueError(f"Duplicate {section} name: {name}")
            result[name] = item
        return result

    base_map = keyed(base_items, f"base {section}")
    overlay_map = keyed(overlay_items, f"overlay {section}")
    merged = {
        name: _deep_merge(item, overlay_map[name]) if name in overlay_map else item
        for name, item in base_map.items()
    }
    for name, item in overlay_map.items():
        if name not in merged:
            merged[name] = item
    return list(merged.values())


def _validate_dataflow_identity(item: dict[str, Any], label: str) -> tuple[str, str]:
    name = item.get("name")
    stage = item.get("stage")
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"{label} requires a non-empty name")
    if not isinstance(stage, str) or not stage.strip():
        raise ValueError(f"{label} requires a non-empty stage")
    return name, stage


def _hint_group_key(item: dict[str, Any], label: str) -> tuple[str, str | None, str]:
    connection = item.get("connection_name") or item.get("connection_id")
    table = item.get("table_name")
    if not connection or not table:
        raise ValueError(f"{label} requires connection_name/connection_id and table_name")
    schema = item.get("schema_name")
    return str(connection), str(schema) if schema not in (None, "") else None, str(table)


def _hint_columns(items: Any, label: str) -> dict[str, dict[str, Any]]:
    if not isinstance(items, list):
        raise ValueError(f"{label}.hints must be an array")
    result: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(items):
        if not isinstance(item, dict) or not item.get("column_name"):
            raise ValueError(f"{label}.hints[{index}] requires column_name")
        column = str(item["column_name"])
        if column in result:
            raise ValueError(f"Duplicate schema hint column {column} in {label}")
        result[column] = item
    return result


def _merge_hint_group(
    base: dict[str, Any], overlay: dict[str, Any], label: str
) -> dict[str, Any]:
    base_columns = _hint_columns(base.get("hints"), f"base {label}")
    overlay_columns = _hint_columns(overlay.get("hints"), f"overlay {label}")
    merged_columns = {
        name: _deep_merge(item, overlay_columns[name]) if name in overlay_columns else item
        for name, item in base_columns.items()
    }
    for name, item in overlay_columns.items():
        if name not in merged_columns:
            merged_columns[name] = item
    merged = _deep_merge(
        {key: value for key, value in base.items() if key != "hints"},
        {key: value for key, value in overlay.items() if key != "hints"},
    )
    merged["hints"] = list(merged_columns.values())
    return merged


def _merge_schema_hints(
    base_items: list[dict[str, Any]], overlay_items: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    def keyed(items: list[dict[str, Any]], label: str) -> dict[tuple[str, str | None, str], dict[str, Any]]:
        result: dict[tuple[str, str | None, str], dict[str, Any]] = {}
        for item in items:
            key = _hint_group_key(item, label)
            if key in result:
                raise ValueError(f"Duplicate schema_hints group in {label}: {key}")
            _hint_columns(item.get("hints"), label)
            result[key] = item
        return result

    base_map = keyed(base_items, "base schema_hints")
    overlay_map = keyed(overlay_items, "overlay schema_hints")
    merged = {
        key: _merge_hint_group(item, overlay_map[key], str(key)) if key in overlay_map else item
        for key, item in base_map.items()
    }
    for key, item in overlay_map.items():
        if key not in merged:
            merged[key] = item
    return list(merged.values())


def _load_dataflows(metadata_dir: Path) -> list[dict[str, Any]]:
    paths: list[Path] = []
    root_file = metadata_dir / "dataflows.json"
    if root_file.is_file():
        paths.append(root_file)

    dataflows_dir = metadata_dir / "dataflows"
    if dataflows_dir.is_dir():
        paths.extend(path for path in dataflows_dir.rglob("*.json") if path.is_file())
    paths = sorted(paths, key=lambda path: path.relative_to(metadata_dir).as_posix())
    if not paths:
        raise ValueError(
            f"No canonical dataflow JSON found at {root_file} or under {dataflows_dir}"
        )

    result: list[dict[str, Any]] = []
    sources_by_name: dict[str, Path] = {}
    for path in paths:
        for index, item in enumerate(
            _section_items(_load_json(path), "dataflows", path)
        ):
            name, _ = _validate_dataflow_identity(item, f"{path} dataflows[{index}]")
            if name in sources_by_name:
                raise ValueError(
                    f"Duplicate dataflow name {name!r} in {sources_by_name[name]} and {path}"
                )
            sources_by_name[name] = path
            result.append(item)
    return result


def merge_metadata(metadata_dir: Path, environment: str) -> dict[str, Any]:
    """Return one resolved metadata document from canonical modular JSON sources."""
    metadata_dir = metadata_dir.resolve()
    connections_path = metadata_dir / "connections.json"
    if not connections_path.is_file():
        raise ValueError(f"Canonical connections file not found: {connections_path}")
    connections_data = _load_json(connections_path)
    connections = _section_items(connections_data, "connections", connections_path)
    dataflows = _load_dataflows(metadata_dir)

    schema_hints_path = metadata_dir / "schema_hints.json"
    schema_hints = (
        _section_items(_load_json(schema_hints_path), "schema_hints", schema_hints_path)
        if schema_hints_path.is_file()
        else []
    )

    overlay_path = metadata_dir / "environments" / f"{environment}.json"
    overlay: dict[str, Any] = {}
    if overlay_path.is_file():
        loaded = _load_json(overlay_path)
        if not isinstance(loaded, dict):
            raise ValueError(f"Environment overlay must be an object: {overlay_path}")
        unknown = sorted(set(loaded) - ALLOWED_OVERLAY_KEYS)
        if unknown:
            raise ValueError(f"Unsupported overlay keys in {overlay_path}: {', '.join(unknown)}")
        overlay = loaded

    resolved: dict[str, Any] = {}
    if isinstance(connections_data, dict) and "$schema" in connections_data:
        resolved["$schema"] = connections_data["$schema"]
    if "$schema" in overlay:
        resolved["$schema"] = overlay["$schema"]
    resolved["connections"] = _merge_named(
        connections,
        _section_items(overlay.get("connections", []), "connections", overlay_path),
        "connection",
    )
    resolved_dataflows = _merge_named(
        dataflows,
        _section_items(overlay.get("dataflows", []), "dataflows", overlay_path),
        "dataflow",
    )
    for index, item in enumerate(resolved_dataflows):
        _validate_dataflow_identity(item, f"resolved dataflows[{index}]")
    resolved["dataflows"] = resolved_dataflows
    merged_hints = _merge_schema_hints(
        schema_hints,
        _section_items(overlay.get("schema_hints", []), "schema_hints", overlay_path),
    )
    if merged_hints:
        resolved["schema_hints"] = merged_hints
    return resolved


def write_metadata(path: Path, metadata: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True, help="Canonical metadata directory")
    parser.add_argument("--env", required=True, help="Environment overlay name")
    parser.add_argument("--output", type=Path, help="Resolved metadata JSON output")
    args = parser.parse_args()
    try:
        resolved = merge_metadata(args.base, args.env)
        if args.output:
            write_metadata(args.output, resolved)
            print(f"OK: resolved {args.env} metadata -> {args.output}")
        else:
            print(json.dumps(resolved, indent=2, ensure_ascii=False))
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
