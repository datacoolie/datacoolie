"""Validate DataCoolie metadata files against the versioned JSON Schema.

Usage:
    python validate.py <metadata_file> [options]
    python validate.py metadata/dataflows.json
    python validate.py metadata/config.yaml --schema-version 0.1.0
    python validate.py --fetch-latest               # refresh bundled schema from GitHub, no file
    python validate.py --fetch-latest config.json   # refresh then validate

Schema resolution order:
    1. --schemas-dir <path>
    2. DATACOOLIE_SCHEMAS_DIR env var
    3. Skill-bundled schemas/ (offline-first)
    4. Installed datacoolie package
    5. User cache (~/.datacoolie/schemas/ via --fetch-latest)
    6. Dev-repo fallback

Exit codes:
    0 = valid  (or schema refreshed successfully when no file given)
    1 = validation errors found
    2 = input/schema loading error
"""

import argparse
import json
import sys
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

from _loaders import load_metadata
from _schema_resolver import (
    USER_CACHE_DIR,
    fetch_latest_from_github,
    find_schemas_dir,
    load_schema,
    resolve_schema_version,
)


def format_error_path(error) -> str:
    """Format JSON path from validation error for readability."""
    parts = []
    for p in error.absolute_path:
        parts.append(f"[{p}]" if isinstance(p, int) else (f".{p}" if parts else str(p)))
    return "".join(parts) or "$"


def validate_metadata(metadata: dict, schema: dict) -> list[dict]:
    """Validate metadata against schema. Returns list of error dicts."""
    validator = Draft202012Validator(schema)
    errors = []
    for error in sorted(validator.iter_errors(metadata), key=lambda e: list(e.absolute_path)):
        errors.append({
            "path": format_error_path(error),
            "message": error.message,
            "schema_path": ".".join(str(p) for p in error.schema_path),
        })
    return errors


def main():
    parser = argparse.ArgumentParser(
        description="Validate DataCoolie metadata against JSON Schema.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "metadata_file",
        type=Path,
        nargs="?",
        help="Path to metadata JSON or YAML file. Omit when using --fetch-latest alone.",
    )
    parser.add_argument(
        "--schema-version",
        default=None,
        help="Schema version to validate against (default: auto-detect from $schema or latest).",
    )
    parser.add_argument(
        "--schemas-dir",
        default=None,
        metavar="PATH",
        help="Path to schemas directory (overrides DATACOOLIE_SCHEMAS_DIR and bundled defaults).",
    )
    parser.add_argument(
        "--fetch-latest",
        action="store_true",
        help=(
            "Fetch the latest schema from GitHub and update the user cache "
            f"({USER_CACHE_DIR}). Falls back to installed package schema on network failure."
        ),
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress output; exit code only (for CI).",
    )
    args = parser.parse_args()

    # Require at least --fetch-latest or a file
    if not args.fetch_latest and args.metadata_file is None:
        parser.error("metadata_file is required unless --fetch-latest is used.")

    # -- Fetch from GitHub (updates bundled cache) --
    if args.fetch_latest:
        ok = fetch_latest_from_github(
            version=args.schema_version,
            verbose=True,
        )
        if not ok:
            print("  Continuing with bundled schema.", file=sys.stderr)
        if args.metadata_file is None:
            sys.exit(0)  # schema-refresh-only invocation

    # -- Locate schemas dir --
    schemas_dir = find_schemas_dir(explicit=args.schemas_dir)

    # -- Check file exists --
    if not args.metadata_file.exists():
        print(f"ERROR: File not found: {args.metadata_file}", file=sys.stderr)
        sys.exit(2)

    # -- Load metadata --
    try:
        metadata = load_metadata(args.metadata_file)
    except (json.JSONDecodeError, yaml.YAMLError) as e:
        print(f"ERROR: Failed to parse {args.metadata_file}: {e}", file=sys.stderr)
        sys.exit(2)
    except ImportError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)

    # -- Resolve and load schema --
    version = resolve_schema_version(metadata, schemas_dir, args.schema_version)
    schema = load_schema(version, schemas_dir)

    # -- Validate --
    errors = validate_metadata(metadata, schema)

    if not errors:
        if not args.quiet:
            print(f"✓ {args.metadata_file} is valid (schema v{version})")
        sys.exit(0)
    else:
        if not args.quiet:
            print(f"✗ {args.metadata_file} has {len(errors)} validation error(s) (schema v{version}):\n")
            for err in errors:
                print(f"  {err['path']}: {err['message']}")
        sys.exit(1)



if __name__ == "__main__":
    main()
