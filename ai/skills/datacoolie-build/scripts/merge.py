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
PATCH_TYPES = frozenset(SECTION_KEYS)
ALLOWED_OVERLAY_KEYS = {"$schema", "patches", *SECTION_KEYS}
IDENTITY_FIELDS = {
    "connections": frozenset({"name"}),
    "dataflows": frozenset({"name"}),
    "schema_hints": frozenset(
        {
            "connection_name",
            "connection_id",
            "schema_name",
            "table_name",
            "column_name",
        }
    ),
}


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


def _validate_selector(value: Any, label: str) -> None:
    if isinstance(value, list):
        raise ValueError(f"{label} must not contain arrays")
    if isinstance(value, dict):
        if not value:
            raise ValueError(f"{label} must not contain empty objects")
        for key, nested in value.items():
            if not isinstance(key, str) or not key:
                raise ValueError(f"{label} keys must be non-empty strings")
            _validate_selector(nested, f"{label}.{key}")


def _matches_selector(candidate: Any, selector: Any) -> bool:
    if isinstance(selector, dict):
        return isinstance(candidate, dict) and all(
            key in candidate and _matches_selector(candidate[key], value)
            for key, value in selector.items()
        )
    return candidate == selector


def _validated_patches(overlay: dict[str, Any], path: Path) -> list[dict[str, Any]]:
    patches = overlay.get("patches", [])
    if not isinstance(patches, list):
        raise ValueError(f"{path} patches must be an array")
    result: list[dict[str, Any]] = []
    for index, item in enumerate(patches):
        label = f"{path} patches[{index}]"
        if not isinstance(item, dict):
            raise ValueError(f"{label} must be an object")
        unknown = sorted(set(item) - {"match", "patch"})
        missing = sorted({"match", "patch"} - set(item))
        if unknown or missing:
            detail = []
            if missing:
                detail.append(f"missing {', '.join(missing)}")
            if unknown:
                detail.append(f"unsupported {', '.join(unknown)}")
            raise ValueError(f"{label} has invalid keys: {'; '.join(detail)}")

        match = item["match"]
        patch = item["patch"]
        if not isinstance(match, dict):
            raise ValueError(f"{label}.match must be an object")
        match_unknown = sorted(set(match) - {"type", "where"})
        match_missing = sorted({"type", "where"} - set(match))
        if match_unknown or match_missing:
            detail = []
            if match_missing:
                detail.append(f"missing {', '.join(match_missing)}")
            if match_unknown:
                detail.append(f"unsupported {', '.join(match_unknown)}")
            raise ValueError(f"{label}.match has invalid keys: {'; '.join(detail)}")

        patch_type = match["type"]
        if not isinstance(patch_type, str) or patch_type not in PATCH_TYPES:
            supported = ", ".join(sorted(PATCH_TYPES))
            raise ValueError(
                f"{label}.match.type must be one of {supported}; got {patch_type!r}"
            )
        where = match["where"]
        if not isinstance(where, dict) or not where:
            raise ValueError(f"{label}.match.where must be a non-empty object")
        _validate_selector(where, f"{label}.match.where")
        if not isinstance(patch, dict) or not patch:
            raise ValueError(f"{label}.patch must be a non-empty object")
        forbidden = sorted(set(patch) & IDENTITY_FIELDS[patch_type])
        if forbidden:
            raise ValueError(
                f"{label}.patch must not contain immutable identity fields: "
                f"{', '.join(forbidden)}"
            )
        result.append(item)
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


def _merge_hint_items(
    base_items: list[dict[str, Any]],
    overlay_items: list[dict[str, Any]],
    label: str,
) -> list[dict[str, Any]]:
    base_columns = _hint_columns(base_items, f"base {label}")
    overlay_columns = _hint_columns(overlay_items, f"overlay {label}")
    merged = {
        name: _deep_merge(item, overlay_columns[name])
        if name in overlay_columns
        else item
        for name, item in base_columns.items()
    }
    for name, item in overlay_columns.items():
        if name not in merged:
            merged[name] = item
    return list(merged.values())


def _merge_dataflow_patch(
    base: dict[str, Any], patch: dict[str, Any], label: str
) -> dict[str, Any]:
    merged = _deep_merge(base, patch)
    patch_transform = patch.get("transform")
    if not isinstance(patch_transform, dict) or "schema_hints" not in patch_transform:
        return merged

    base_transform = base.get("transform", {})
    if not isinstance(base_transform, dict):
        raise ValueError(f"{label} canonical transform must be an object")
    base_hints = base_transform.get("schema_hints", [])
    patch_hints = patch_transform["schema_hints"]
    if not isinstance(base_hints, list) or not isinstance(patch_hints, list):
        raise ValueError(f"{label}.patch.transform.schema_hints must be an array")
    merged["transform"]["schema_hints"] = _merge_hint_items(
        base_hints,
        patch_hints,
        f"{label}.transform.schema_hints",
    )
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


def _global_hint_records(
    groups: list[dict[str, Any]],
) -> list[tuple[int, int, dict[str, Any]]]:
    records: list[tuple[int, int, dict[str, Any]]] = []
    for group_index, group in enumerate(groups):
        _hint_group_key(group, f"schema_hints[{group_index}]")
        hints = group.get("hints")
        _hint_columns(hints, f"schema_hints[{group_index}]")
        group_fields = {
            key: deepcopy(value) for key, value in group.items() if key != "hints"
        }
        group_fields["schema_name"] = (
            None if group.get("schema_name") in (None, "") else str(group["schema_name"])
        )
        for hint_index, hint in enumerate(hints):
            record = deepcopy(hint)
            record.update(group_fields)
            records.append((group_index, hint_index, record))
    return records


def _apply_selector_patches(
    connections: list[dict[str, Any]],
    dataflows: list[dict[str, Any]],
    schema_hints: list[dict[str, Any]],
    patches: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    canonical_connections = deepcopy(connections)
    canonical_dataflows = deepcopy(dataflows)
    canonical_hints = deepcopy(schema_hints)
    resolved_connections = deepcopy(connections)
    resolved_dataflows = deepcopy(dataflows)
    resolved_hints = deepcopy(schema_hints)
    canonical_hint_records = _global_hint_records(canonical_hints)

    for index, item in enumerate(patches):
        patch_type = item["match"]["type"]
        where = item["match"]["where"]
        patch = item["patch"]
        label = f"patches[{index}] ({patch_type})"

        if patch_type == "connections":
            matches = [
                item_index
                for item_index, candidate in enumerate(canonical_connections)
                if _matches_selector(candidate, where)
            ]
            for item_index in matches:
                resolved_connections[item_index] = _deep_merge(
                    resolved_connections[item_index], patch
                )
        elif patch_type == "dataflows":
            matches = [
                item_index
                for item_index, candidate in enumerate(canonical_dataflows)
                if _matches_selector(candidate, where)
            ]
            for item_index in matches:
                resolved_dataflows[item_index] = _merge_dataflow_patch(
                    resolved_dataflows[item_index], patch, label
                )
        else:
            hint_matches = [
                (group_index, hint_index)
                for group_index, hint_index, candidate in canonical_hint_records
                if _matches_selector(candidate, where)
            ]
            matches = hint_matches
            for group_index, hint_index in hint_matches:
                resolved_hints[group_index]["hints"][hint_index] = _deep_merge(
                    resolved_hints[group_index]["hints"][hint_index], patch
                )

        if not matches:
            raise ValueError(f"{label} matched zero canonical entities")

    return resolved_connections, resolved_dataflows, resolved_hints


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

    patches = _validated_patches(overlay, overlay_path)
    connections, dataflows, schema_hints = _apply_selector_patches(
        connections,
        dataflows,
        schema_hints,
        patches,
    )

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
