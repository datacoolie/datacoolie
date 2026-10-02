"""Deterministic environment overlays for project metadata."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Any

from .documents import SECTION_KEYS, MetadataSnapshot
from .errors import ProjectValidationError


_ALLOWED_KEYS = {"$schema", "patches", *SECTION_KEYS}
_PATCH_TYPES = set(SECTION_KEYS)
_IDENTITY_FIELDS = {
    "connections": {"name", "connection_id"},
    "dataflows": {"name", "dataflow_id"},
    "schema_hints": {
        "connection_name",
        "connection_id",
        "schema_name",
        "table_name",
        "column_name",
    },
}


def _deep_merge(base: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in update.items():
        if isinstance(result.get(key), dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def _selector(value: Any, label: str) -> None:
    if isinstance(value, list):
        raise ProjectValidationError(f"{label} must not contain arrays")
    if isinstance(value, dict):
        if not value:
            raise ProjectValidationError(f"{label} must not contain empty objects")
        for key, nested in value.items():
            if not isinstance(key, str) or not key:
                raise ProjectValidationError(f"{label} keys must be non-empty strings")
            _selector(nested, f"{label}.{key}")


def _matches(candidate: Any, selector: Any) -> bool:
    if isinstance(selector, dict):
        return isinstance(candidate, dict) and all(
            key in candidate and _matches(candidate[key], value)
            for key, value in selector.items()
        )
    return candidate == selector


def _validate_overlay(value: Any, path: Path) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProjectValidationError(f"Environment overlay must be an object: {path}")
    unknown = sorted(str(key) for key in set(value) - _ALLOWED_KEYS)
    if unknown:
        raise ProjectValidationError(
            f"Unsupported overlay keys in {path}: {', '.join(unknown)}"
        )
    for section in SECTION_KEYS:
        if section in value and not isinstance(value[section], list):
            raise ProjectValidationError(f"Overlay section {section!r} must be an array: {path}")
        for index, item in enumerate(value.get(section, [])):
            if not isinstance(item, dict):
                raise ProjectValidationError(f"Overlay {section}[{index}] must be an object: {path}")
    patches = value.get("patches", [])
    if not isinstance(patches, list):
        raise ProjectValidationError(f"Overlay patches must be an array: {path}")
    for index, item in enumerate(patches):
        label = f"{path} patches[{index}]"
        if not isinstance(item, dict) or set(item) != {"match", "patch"}:
            raise ProjectValidationError(f"{label} must contain only match and patch")
        match = item["match"]
        patch = item["patch"]
        if not isinstance(match, dict) or set(match) != {"type", "where"}:
            raise ProjectValidationError(f"{label}.match must contain type and where")
        if match["type"] not in _PATCH_TYPES:
            raise ProjectValidationError(
                f"{label}.match.type must be one of {sorted(_PATCH_TYPES)}"
            )
        if not isinstance(match["where"], dict) or not match["where"]:
            raise ProjectValidationError(f"{label}.match.where must be a non-empty object")
        _selector(match["where"], f"{label}.match.where")
        if not isinstance(patch, dict) or not patch:
            raise ProjectValidationError(f"{label}.patch must be a non-empty object")
        forbidden = sorted(set(patch) & _IDENTITY_FIELDS[match["type"]])
        if forbidden:
            raise ProjectValidationError(
                f"{label}.patch must not change identity fields: {', '.join(forbidden)}"
            )
    return value


def _item_key(section: str, item: dict[str, Any], index: int) -> Any:
    if section == "connections":
        key = item.get("name") or item.get("connection_id")
        if not isinstance(key, str) or not key.strip():
            raise ProjectValidationError(f"{section}[{index}] requires a stable name/id")
        return key
    if section == "dataflows":
        key = item.get("name") or item.get("dataflow_id")
        if not isinstance(key, str) or not key.strip():
            raise ProjectValidationError(f"{section}[{index}] requires a stable name/id")
        return key
    connection = item.get("connection_name") or item.get("connection_id")
    table = item.get("table_name")
    schema = item.get("schema_name") or None
    if not connection or not table:
        raise ProjectValidationError(
            f"schema_hints[{index}] requires connection_name/connection_id and table_name"
        )
    return (str(connection), str(schema) if schema else None, str(table))


def _hint_columns(group: dict[str, Any], label: str) -> dict[str, dict[str, Any]]:
    hints = group.get("hints", [])
    if not isinstance(hints, list):
        raise ProjectValidationError(f"{label}.hints must be an array")
    result: dict[str, dict[str, Any]] = {}
    for index, hint in enumerate(hints):
        if not isinstance(hint, dict) or not isinstance(hint.get("column_name"), str):
            raise ProjectValidationError(f"{label}.hints[{index}] requires column_name")
        column = hint["column_name"]
        if column in result:
            raise ProjectValidationError(f"Duplicate schema hint column {column!r} in {label}")
        result[column] = hint
    return result


def _merge_schema_group(base: dict[str, Any], update: dict[str, Any], label: str) -> dict[str, Any]:
    base_columns = _hint_columns(base, f"base {label}")
    update_columns = _hint_columns(update, f"overlay {label}")
    merged = _deep_merge(
        {key: value for key, value in base.items() if key != "hints"},
        {key: value for key, value in update.items() if key != "hints"},
    )
    merged["hints"] = [
        _deep_merge(item, update_columns[column]) if column in update_columns else deepcopy(item)
        for column, item in base_columns.items()
    ]
    merged["hints"].extend(
        deepcopy(item) for column, item in update_columns.items() if column not in base_columns
    )
    return merged


def _merge_local_hints(
    base: list[dict[str, Any]],
    update: list[dict[str, Any]],
    label: str,
) -> list[dict[str, Any]]:
    """Merge a dataflow-local ``transform.schema_hints`` list by column.

    Local hints are a separate scope from global ``schema_hints``.  A selector
    patch targeting a dataflow may change one local column while retaining
    unmentioned columns; other transform arrays continue to use ordinary
    replacement semantics through :func:`_deep_merge`.
    """

    base_columns = _hint_columns({"hints": base}, f"base {label}")
    update_columns = _hint_columns({"hints": update}, f"overlay {label}")
    merged = [
        _deep_merge(item, update_columns[column]) if column in update_columns else deepcopy(item)
        for column, item in base_columns.items()
    ]
    merged.extend(
        deepcopy(item)
        for column, item in update_columns.items()
        if column not in base_columns
    )
    return merged


def _merge_dataflow_patch(
    base: dict[str, Any],
    patch: dict[str, Any],
    label: str,
) -> dict[str, Any]:
    """Deep-merge one dataflow patch, preserving local hint columns."""

    merged = _deep_merge(base, patch)
    patch_transform = patch.get("transform")
    if not isinstance(patch_transform, dict) or "schema_hints" not in patch_transform:
        return merged
    base_transform = base.get("transform", {})
    if not isinstance(base_transform, dict):
        raise ProjectValidationError(f"{label} canonical transform must be an object")
    base_hints = base_transform.get("schema_hints", [])
    patch_hints = patch_transform["schema_hints"]
    if not isinstance(base_hints, list) or not isinstance(patch_hints, list):
        raise ProjectValidationError(f"{label}.transform.schema_hints must be an array")
    if not isinstance(merged.get("transform"), dict):
        raise ProjectValidationError(f"{label}.transform must be an object")
    merged["transform"]["schema_hints"] = _merge_local_hints(
        base_hints,
        patch_hints,
        f"{label}.transform.schema_hints",
    )
    return merged


def _merge_section(base: list[dict[str, Any]], update: list[dict[str, Any]], section: str) -> list[dict[str, Any]]:
    base_map: dict[Any, dict[str, Any]] = {}
    update_map: dict[Any, dict[str, Any]] = {}
    for index, item in enumerate(base):
        key = _item_key(section, item, index)
        if key in base_map:
            raise ProjectValidationError(f"Duplicate {section} identity: {key!r}")
        base_map[key] = item
    for index, item in enumerate(update):
        key = _item_key(section, item, index)
        if key in update_map:
            raise ProjectValidationError(f"Duplicate overlay {section} identity: {key!r}")
        update_map[key] = item
    result: list[dict[str, Any]] = []
    for key, item in base_map.items():
        if key in update_map:
            if section == "schema_hints":
                result.append(_merge_schema_group(item, update_map[key], str(key)))
            else:
                result.append(_deep_merge(item, update_map[key]))
        else:
            result.append(deepcopy(item))
    for key, item in update_map.items():
        if key not in base_map:
            result.append(deepcopy(item))
    return result


def _apply_patches(metadata: dict[str, Any], patches: list[dict[str, Any]]) -> None:
    canonical = deepcopy(metadata)
    for index, item in enumerate(patches):
        kind = item["match"]["type"]
        where = item["match"]["where"]
        patch = item["patch"]
        matches: list[tuple[int, dict[str, Any]]] = []
        if kind == "schema_hints":
            for group_index, group in enumerate(canonical["schema_hints"]):
                for hint_index, hint in enumerate(group.get("hints", [])):
                    candidate = {**{key: value for key, value in group.items() if key != "hints"}, **hint}
                    if _matches(candidate, where):
                        matches.append((group_index, {"hint_index": hint_index}))
            for group_index, marker in matches:
                hint_index = marker["hint_index"]
                metadata["schema_hints"][group_index]["hints"][hint_index] = _deep_merge(
                    metadata["schema_hints"][group_index]["hints"][hint_index], patch
                )
        else:
            for item_index, candidate in enumerate(canonical[kind]):
                if _matches(candidate, where):
                    matches.append((item_index, {}))
            for item_index, _ in matches:
                if kind == "dataflows":
                    metadata[kind][item_index] = _merge_dataflow_patch(
                        metadata[kind][item_index],
                        patch,
                        f"patches[{index}] ({kind})",
                    )
                else:
                    metadata[kind][item_index] = _deep_merge(metadata[kind][item_index], patch)
        if not matches:
            raise ProjectValidationError(
                f"patches[{index}] ({kind}) matched zero canonical entities"
            )


def load_overlay(metadata_root: Path, environment: str) -> tuple[dict[str, Any], Path | None]:
    path = metadata_root / "environments" / f"{environment}.json"
    if not path.is_file():
        return {}, None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProjectValidationError(f"Cannot read environment overlay: {path}") from exc
    return _validate_overlay(value, path), path


def resolve_environment(snapshot: MetadataSnapshot, environment: str) -> tuple[dict[str, Any], Path | None]:
    """Return common metadata with one optional environment overlay applied."""

    metadata = snapshot.merged()
    overlay, path = load_overlay(snapshot.root if snapshot.root.is_dir() else snapshot.root.parent, environment)
    if not overlay:
        return metadata, path
    _apply_patches(metadata, overlay.get("patches", []))
    for section in SECTION_KEYS:
        metadata[section] = _merge_section(
            metadata.get(section, []),
            overlay.get(section, []),
            section,
        )
    if "$schema" in overlay:
        metadata["$schema"] = overlay["$schema"]
    return metadata, path


__all__ = ["load_overlay", "resolve_environment"]
