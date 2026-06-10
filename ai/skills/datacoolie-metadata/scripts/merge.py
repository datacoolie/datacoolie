"""Merge base metadata + environment overlay into fully resolved metadata.

Usage:
    python merge.py --base <dir> --env <name> [--output <path>]
    python merge.py --base {project_name}_dcws/metadata/ --env prod
    python merge.py --base {project_name}_dcws/metadata/ --env dev --output {project_name}_dcws/generated/dev/metadata.json
    python merge.py --base {project_name}_dcws/metadata/ --env dev --output-layout split
    python merge.py --base {project_name}_dcws/metadata/ --env dev --emit-stage-manifest

The base directory should contain either:
    - metadata.json (or .yaml) — unified layout (connections + dataflows in one file)
    - connections.json + dataflows.json (or .yaml) — split layout
    - connections.json + dataflows/*.json — modular stage-file layout
    - connections.json + dataflows/{stage}/{group}.json — nested modular layout

Plus:
    - environments/{env}.yaml (overlay)

Auto-detects which layout is present. Split/modular layouts are checked first.

Exit codes:
    0 = success
    1 = merge/validation error
    2 = input error
"""

import argparse
import json
import sys
from pathlib import Path

import yaml

from _loaders import load_file

STAGE_ORDER = ("source2bronze", "bronze2silver", "silver2gold")
SUPPORTED_EXTENSIONS = (".json", ".yaml", ".yml")
OVERLAY_SECTION_KEYS = ("connections", "dataflows", "schema_hints")
ALLOWED_OVERLAY_KEYS = {"$schema", *OVERLAY_SECTION_KEYS}


def find_file(directory: Path, basename: str) -> Path | None:
    """Find a file with any supported extension."""
    for ext in SUPPORTED_EXTENSIONS:
        candidate = directory / f"{basename}{ext}"
        if candidate.exists():
            return candidate
    return None


def deep_merge(base: dict, overlay: dict) -> dict:
    """Deep merge overlay into base. Overlay values win for scalars, dicts merge recursively."""
    result = base.copy()
    for key, value in overlay.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def merge_list_by_name(base_list: list, overlay_list: list) -> list:
    """Merge two lists of objects by 'name' field. Overlay overrides matching items."""
    base_map = {item["name"]: item for item in base_list if isinstance(item, dict) and "name" in item}
    overlay_map = {item["name"]: item for item in overlay_list if isinstance(item, dict) and "name" in item}

    # Merge matching items
    for name, overlay_item in overlay_map.items():
        if name in base_map:
            base_map[name] = deep_merge(base_map[name], overlay_item)
        else:
            base_map[name] = overlay_item

    # Preserve original order from base, append new items from overlay
    result = []
    seen = set()
    for item in base_list:
        if isinstance(item, dict) and "name" in item:
            name = item["name"]
            result.append(base_map[name])
            seen.add(name)
        else:
            result.append(item)

    for name, item in overlay_map.items():
        if name not in seen:
            result.append(item)

    return result


def _error(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(2)


def _normalize_schema_name(value: object) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _schema_hint_group_key(item: object, label: str, index: int) -> tuple[str, str | None, str]:
    if not isinstance(item, dict):
        _error(f"{label}[{index}] must be an object.")

    connection_ref = item.get("connection_name") or item.get("connection_id")
    table_name = item.get("table_name")
    if not connection_ref or not table_name:
        _error(
            f"{label}[{index}] must include connection_name or connection_id, "
            "plus table_name."
        )

    return (str(connection_ref), _normalize_schema_name(item.get("schema_name")), str(table_name))


def _schema_hint_key_label(key: tuple[str, str | None, str]) -> str:
    connection_ref, schema_name, table_name = key
    schema_display = schema_name if schema_name is not None else "<none>"
    return f"connection={connection_ref}, schema={schema_display}, table={table_name}"


def _schema_hint_column_key(item: object, label: str, index: int) -> str:
    if not isinstance(item, dict):
        _error(f"{label}.hints[{index}] must be an object.")

    column_name = item.get("column_name")
    if not column_name:
        _error(f"{label}.hints[{index}] must include column_name.")
    return str(column_name)


def _validate_schema_hint_group(group: dict, label: str) -> list:
    hints = group.get("hints")
    if hints is None:
        _error(f"{label} must include hints.")
    if not isinstance(hints, list):
        _error(f"{label}.hints must be a list.")
    return hints


def _schema_hint_group_map(items: list, label: str) -> dict[tuple[str, str | None, str], dict]:
    result = {}
    for index, item in enumerate(items):
        key = _schema_hint_group_key(item, label, index)
        if key in result:
            _error(f"Duplicate schema_hints group in {label}: {_schema_hint_key_label(key)}.")
        hints = _validate_schema_hint_group(item, f"{label}[{index}]")
        _schema_hint_column_map(hints, f"{label}[{index}]")
        result[key] = item
    return result


def _schema_hint_column_map(items: list, label: str) -> dict[str, dict]:
    result = {}
    for index, item in enumerate(items):
        column_name = _schema_hint_column_key(item, label, index)
        if column_name in result:
            _error(f"Duplicate schema_hints column_name '{column_name}' in {label}.hints.")
        result[column_name] = item
    return result


def merge_schema_hint_columns(base_hints: list, overlay_hints: list, label: str) -> list:
    """Merge schema hint columns by column_name, preserving base order."""
    base_map = _schema_hint_column_map(base_hints, f"{label} base")
    overlay_map = _schema_hint_column_map(overlay_hints, f"{label} overlay")

    for column_name, overlay_hint in overlay_map.items():
        if column_name in base_map:
            base_map[column_name] = deep_merge(base_map[column_name], overlay_hint)
        else:
            base_map[column_name] = overlay_hint

    result = []
    seen = set()
    for item in base_hints:
        column_name = item["column_name"]
        result.append(base_map[column_name])
        seen.add(column_name)

    for column_name, item in overlay_map.items():
        if column_name not in seen:
            result.append(item)

    return result


def merge_schema_hint_group(base_group: dict, overlay_group: dict, label: str) -> dict:
    """Merge one schema_hints group, using column_name as the nested hints key."""
    base_hints = _validate_schema_hint_group(base_group, f"{label} base")
    overlay_hints = _validate_schema_hint_group(overlay_group, f"{label} overlay")

    merged = deep_merge(
        {k: v for k, v in base_group.items() if k != "hints"},
        {k: v for k, v in overlay_group.items() if k != "hints"},
    )
    merged["hints"] = merge_schema_hint_columns(base_hints, overlay_hints, label)
    return merged


def merge_schema_hints(base_list: list, overlay_list: list) -> list:
    """Merge shared schema_hints by connection + schema + table, then column_name."""
    base_map = _schema_hint_group_map(base_list, "base schema_hints")
    overlay_map = _schema_hint_group_map(overlay_list, "overlay schema_hints")

    for key, overlay_group in overlay_map.items():
        if key in base_map:
            base_map[key] = merge_schema_hint_group(
                base_map[key],
                overlay_group,
                f"schema_hints[{_schema_hint_key_label(key)}]",
            )
        else:
            base_map[key] = overlay_group

    result = []
    seen = set()
    for item in base_list:
        key = _schema_hint_group_key(item, "base schema_hints", len(seen))
        result.append(base_map[key])
        seen.add(key)

    for key, item in overlay_map.items():
        if key not in seen:
            result.append(item)

    return result


def validate_overlay_root(overlay: object, env_file: Path) -> dict:
    if not isinstance(overlay, dict):
        _error(f"Environment overlay {env_file} must be a YAML/JSON object.")

    unknown_keys = sorted(set(overlay) - ALLOWED_OVERLAY_KEYS)
    if unknown_keys:
        _error(f"Environment overlay {env_file} has unsupported top-level keys: {', '.join(unknown_keys)}.")

    for key in OVERLAY_SECTION_KEYS:
        if key in overlay and not isinstance(overlay[key], list):
            _error(f"Environment overlay {env_file} field '{key}' must be a list.")

    return overlay


def _items(data: object, key: str) -> list:
    """Normalize list or object-with-list to a list."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        value = data.get(key, data.get("items", []))
        return value if isinstance(value, list) else []
    return []


def _load(path: Path) -> object:
    try:
        return load_file(path)
    except (json.JSONDecodeError, yaml.YAMLError) as e:
        print(f"ERROR: Failed to parse {path}: {e}", file=sys.stderr)
        sys.exit(2)


def _stage_sort_key(path: Path) -> tuple[int, str]:
    parts = path.parts
    stage = path.stem
    if len(parts) >= 2:
        parent = parts[-2]
        if parent == "dataflows":
            stage = path.stem
        else:
            stage = parent
    try:
        stage_index = STAGE_ORDER.index(stage)
    except ValueError:
        stage_index = len(STAGE_ORDER)
    return (stage_index, str(path).lower())


def discover_dataflow_files(dataflows_dir: Path) -> list[Path]:
    if not dataflows_dir.is_dir():
        return []
    candidates = [
        path
        for path in dataflows_dir.rglob("*")
        if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS
    ]
    return sorted(candidates, key=_stage_sort_key)


def infer_stage_from_path(dataflows_dir: Path, path: Path) -> tuple[str, bool]:
    relative = path.relative_to(dataflows_dir)
    parts = relative.parts
    if len(parts) == 1:
        return (Path(parts[0]).stem, False)
    if len(parts) == 2:
        parent = parts[0]
        group = Path(parts[1]).stem
        return (f"{parent}_{group}", True)
    print(
        f"ERROR: Unsupported dataflow path depth: {path}. "
        "Expected dataflows/{stage}.json or dataflows/{stage}/{group}.json",
        file=sys.stderr,
    )
    sys.exit(2)


def apply_stage_inference(dataflows: list, inferred_stage: str, exact: bool, source_path: Path) -> list:
    parent_stage = inferred_stage.split("_", 1)[0] if exact else inferred_stage
    result = []
    for index, item in enumerate(dataflows):
        if not isinstance(item, dict):
            result.append(item)
            continue

        dataflow = item.copy()
        explicit_stage = dataflow.get("stage")
        name = dataflow.get("name", f"index {index}")

        if not explicit_stage:
            dataflow["stage"] = inferred_stage
        elif exact and explicit_stage != inferred_stage:
            print(
                f"ERROR: Dataflow '{name}' in {source_path} has stage '{explicit_stage}', "
                f"but nested path requires '{inferred_stage}'.",
                file=sys.stderr,
            )
            sys.exit(2)
        elif not exact and explicit_stage != parent_stage and not str(explicit_stage).startswith(f"{parent_stage}_"):
            print(
                f"ERROR: Dataflow '{name}' in {source_path} has stage '{explicit_stage}', "
                f"but file path requires '{parent_stage}' or '{parent_stage}_*'.",
                file=sys.stderr,
            )
            sys.exit(2)
        result.append(dataflow)
    return result


def load_modular_dataflows(dataflows_dir: Path) -> list:
    merged = []
    for path in discover_dataflow_files(dataflows_dir):
        data = _load(path)
        dataflows = _items(data, "dataflows")
        inferred_stage, exact = infer_stage_from_path(dataflows_dir, path)
        merged.extend(apply_stage_inference(dataflows, inferred_stage, exact, path))
    return merged


def assert_unique_dataflow_names(dataflows: list) -> None:
    seen = {}
    for index, item in enumerate(dataflows):
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        if not name:
            continue
        if name in seen:
            print(
                f"ERROR: Duplicate dataflow name '{name}' at index {index}; first seen at index {seen[name]}.",
                file=sys.stderr,
            )
            sys.exit(2)
        seen[name] = index


def stage_manifest(dataflows: list) -> dict:
    stages: dict[str, list[str]] = {}
    for item in dataflows:
        if not isinstance(item, dict):
            continue
        stage = item.get("stage", "unassigned")
        name = item.get("name")
        if name:
            stages.setdefault(stage, []).append(name)
    return {"stages": [{"stage": stage, "dataflows": names} for stage, names in stages.items()]}


def write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")


def default_generated_dir(base_dir: Path, env: str) -> Path:
    """Default output follows the workspace that owns the metadata directory."""
    if base_dir.name.lower() == "metadata":
        return base_dir.parent / "generated" / env
    return Path("generated") / env


def main():
    parser = argparse.ArgumentParser(
        description="Merge base metadata with environment overlay."
    )
    parser.add_argument("--base", type=Path, required=True, help="Base metadata directory (contains connections + dataflows files).")
    parser.add_argument("--env", required=True, help="Environment name (looks for environments/{env}.yaml).")
    parser.add_argument("--output", type=Path, default=None, help="Output file path for unified layout or output directory for split layout.")
    parser.add_argument("--output-layout", choices=("unified", "split"), default="unified", help="Generated output layout (default: unified).")
    parser.add_argument("--emit-stage-manifest", action="store_true", help="Also write stage_manifest.json as a generated review index.")
    args = parser.parse_args()

    if not args.base.is_dir():
        print(f"ERROR: Base directory not found: {args.base}", file=sys.stderr)
        sys.exit(2)

    # Load base files — detect layout: split/modular or unified.
    connections_file = find_file(args.base, "connections")
    dataflows_file = find_file(args.base, "dataflows")
    schema_hints_file = find_file(args.base, "schema_hints")
    dataflows_dir = args.base / "dataflows"
    unified_file = None
    base_schema_hints = []

    if connections_file and (dataflows_file or dataflows_dir.is_dir()):
        # Split or modular layout.
        connections_data = _load(connections_file)
        if dataflows_file:
            dataflows_data = _load(dataflows_file)
        else:
            dataflows_data = {"dataflows": load_modular_dataflows(dataflows_dir)}
        if schema_hints_file:
            schema_hints_data = _load(schema_hints_file)
            base_schema_hints = _items(schema_hints_data, "schema_hints")
    else:
        # Try unified layout
        unified_file = find_file(args.base, "metadata")
        if not unified_file:
            print(
                f"ERROR: No metadata file found in {args.base} "
                f"(expected metadata.json, connections.json + dataflows.json, "
                f"or connections.json + dataflows/*.json)",
                file=sys.stderr,
            )
            sys.exit(2)
        unified_data = _load(unified_file)
        connections_data = unified_data
        dataflows_data = unified_data
        base_schema_hints = unified_data.get("schema_hints", []) if isinstance(unified_data, dict) else []

    # Normalize: accept both raw arrays and objects with a key
    connections = _items(connections_data, "connections")
    dataflows = _items(dataflows_data, "dataflows")
    assert_unique_dataflow_names(dataflows)

    # Load environment overlay
    env_file = find_file(args.base / "environments", args.env)

    overlay_connections = []
    overlay_dataflows = []
    overlay_schema_hints = []
    overlay_top_level = {}
    has_overlay_schema_hints = False

    if env_file and env_file.exists():
        try:
            overlay = load_file(env_file)
        except (json.JSONDecodeError, yaml.YAMLError) as e:
            print(f"ERROR: Failed to parse overlay {env_file}: {e}", file=sys.stderr)
            sys.exit(2)

        overlay = validate_overlay_root(overlay, env_file)
        overlay_connections = overlay.get("connections", [])
        overlay_dataflows = overlay.get("dataflows", [])
        if "schema_hints" in overlay:
            overlay_schema_hints = overlay.get("schema_hints", [])
            has_overlay_schema_hints = True
        overlay_top_level = {
            k: v for k, v in overlay.items() if k not in OVERLAY_SECTION_KEYS
        }
    else:
        print(f"WARNING: No environment overlay found for '{args.env}' in {args.base / 'environments'}", file=sys.stderr)

    # Merge
    merged_connections = merge_list_by_name(connections, overlay_connections)
    merged_dataflows = merge_list_by_name(dataflows, overlay_dataflows)
    merged_schema_hints = (
        merge_schema_hints(base_schema_hints, overlay_schema_hints)
        if has_overlay_schema_hints
        else base_schema_hints
    )
    assert_unique_dataflow_names(merged_dataflows)

    # Build output — preserve top-level keys from base (e.g., $schema)
    merged = {}

    # Carry over $schema and other top-level metadata from base file(s)
    if isinstance(connections_data, dict):
        for k, v in connections_data.items():
            if k not in ("connections", "dataflows", "schema_hints", "items"):
                merged[k] = v
    if isinstance(dataflows_data, dict):
        for k, v in dataflows_data.items():
            if k not in ("connections", "dataflows", "schema_hints", "items") and k not in merged:
                merged[k] = v
    merged = deep_merge(merged, overlay_top_level)

    merged["connections"] = merged_connections
    merged["dataflows"] = merged_dataflows

    if merged_schema_hints:
        merged["schema_hints"] = merged_schema_hints

    if args.output_layout == "split":
        output_dir = args.output or default_generated_dir(args.base, args.env)
        write_json(output_dir / "connections.json", merged_connections)
        write_json(output_dir / "dataflows.json", merged_dataflows)
        if merged_schema_hints:
            write_json(output_dir / "schema_hints.json", merged_schema_hints)
        if args.emit_stage_manifest:
            write_json(output_dir / "stage_manifest.json", stage_manifest(merged_dataflows))
        hints_msg = f" + {len(merged_schema_hints)} schema_hints" if merged_schema_hints else ""
        print(f"✓ Merged {len(merged_connections)} connections + {len(merged_dataflows)} dataflows{hints_msg} → {output_dir}")
    else:
        output_path = args.output or default_generated_dir(args.base, args.env) / "metadata.json"
        write_json(output_path, merged)
        if args.emit_stage_manifest:
            write_json(output_path.parent / "stage_manifest.json", stage_manifest(merged_dataflows))
        hints_msg = f" + {len(merged_schema_hints)} schema_hints" if merged_schema_hints else ""
        print(f"✓ Merged {len(merged_connections)} connections + {len(merged_dataflows)} dataflows{hints_msg} → {output_path}")
    sys.exit(0)


if __name__ == "__main__":
    main()
