"""Merge base metadata + environment overlay into a fully resolved metadata file.

Usage:
    python merge.py --base <dir> --env <name> [--output <path>]
    python merge.py --base metadata/ --env prod
    python merge.py --base metadata/ --env dev --output .datacoolie/generated/metadata.dev.json

The base directory should contain either:
    - metadata.json (or .yaml) — unified layout (connections + dataflows in one file)
    - connections.json + dataflows.json (or .yaml) — split layout

Plus:
    - environments/{env}.yaml (overlay)

Auto-detects which layout is present. Split layout is checked first.

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


def find_file(directory: Path, basename: str) -> Path | None:
    """Find a file with any supported extension."""
    for ext in (".json", ".yaml", ".yml"):
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


def main():
    parser = argparse.ArgumentParser(
        description="Merge base metadata with environment overlay."
    )
    parser.add_argument("--base", type=Path, required=True, help="Base metadata directory (contains connections + dataflows files).")
    parser.add_argument("--env", required=True, help="Environment name (looks for environments/{env}.yaml).")
    parser.add_argument("--output", type=Path, default=None, help="Output file path (default: .datacoolie/generated/metadata.{env}.json).")
    args = parser.parse_args()

    if not args.base.is_dir():
        print(f"ERROR: Base directory not found: {args.base}", file=sys.stderr)
        sys.exit(2)

    # Load base files — detect layout: split (connections + dataflows) or unified (metadata)
    connections_file = find_file(args.base, "connections")
    dataflows_file = find_file(args.base, "dataflows")
    unified_file = None
    base_schema_hints = []

    if connections_file and dataflows_file:
        # Split layout
        try:
            connections_data = load_file(connections_file)
            dataflows_data = load_file(dataflows_file)
        except (json.JSONDecodeError, yaml.YAMLError) as e:
            print(f"ERROR: Failed to parse base files: {e}", file=sys.stderr)
            sys.exit(2)
    else:
        # Try unified layout
        unified_file = find_file(args.base, "metadata")
        if not unified_file:
            print(
                f"ERROR: No metadata file found in {args.base} "
                f"(expected metadata.json or connections.json + dataflows.json)",
                file=sys.stderr,
            )
            sys.exit(2)
        try:
            unified_data = load_file(unified_file)
        except (json.JSONDecodeError, yaml.YAMLError) as e:
            print(f"ERROR: Failed to parse {unified_file}: {e}", file=sys.stderr)
            sys.exit(2)
        connections_data = unified_data
        dataflows_data = unified_data
        base_schema_hints = unified_data.get("schema_hints", []) if isinstance(unified_data, dict) else []

    # Normalize: accept both raw arrays and objects with a key
    if isinstance(connections_data, list):
        connections = connections_data
    elif isinstance(connections_data, dict):
        connections = connections_data.get("connections", connections_data.get("items", []))
    else:
        connections = []

    if isinstance(dataflows_data, list):
        dataflows = dataflows_data
    elif isinstance(dataflows_data, dict):
        dataflows = dataflows_data.get("dataflows", dataflows_data.get("items", []))
    else:
        dataflows = []

    # Load environment overlay
    env_file = find_file(args.base / "environments", args.env)

    overlay_connections = []
    overlay_dataflows = []

    if env_file and env_file.exists():
        try:
            overlay = load_file(env_file)
        except (json.JSONDecodeError, yaml.YAMLError) as e:
            print(f"ERROR: Failed to parse overlay {env_file}: {e}", file=sys.stderr)
            sys.exit(2)

        if isinstance(overlay, dict):
            overlay_connections = overlay.get("connections", [])
            overlay_dataflows = overlay.get("dataflows", [])
    else:
        print(f"WARNING: No environment overlay found for '{args.env}' in {args.base / 'environments'}", file=sys.stderr)

    # Merge
    merged_connections = merge_list_by_name(connections, overlay_connections)
    merged_dataflows = merge_list_by_name(dataflows, overlay_dataflows)

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

    merged["connections"] = merged_connections
    merged["dataflows"] = merged_dataflows

    # Preserve schema_hints from base (overlay doesn't modify schema_hints)
    if base_schema_hints:
        merged["schema_hints"] = base_schema_hints

    # Determine output path
    if args.output:
        output_path = args.output
    else:
        output_path = Path(f".datacoolie/generated/metadata.{args.env}.json")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
        f.write("\n")

    hints_msg = f" + {len(base_schema_hints)} schema_hints" if base_schema_hints else ""
    print(f"✓ Merged {len(merged_connections)} connections + {len(merged_dataflows)} dataflows{hints_msg} → {output_path}")
    sys.exit(0)


if __name__ == "__main__":
    main()
