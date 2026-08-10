"""Validate DataCoolie metadata files against the versioned JSON Schema.

Usage:
    python validate.py <metadata_file>
    python validate.py {project_name}_dcws/metadata/dataflows.json
    python validate.py {project_name}_dcws/metadata/metadata.json

Schemas are resolved only from the versioned ``schemas/`` directory bundled
beside this tooling. ``$schema`` selects a bundled version; otherwise the
bundled compatibility default is used.

Exit codes:
    0 = valid
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
from _schema_resolver import find_schemas_dir, load_schema, resolve_schema_version


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
        help="Path to metadata JSON, YAML, or Excel file.",
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress output; exit code only (for CI).",
    )
    args = parser.parse_args()

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
    except (ImportError, ValueError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)

    # -- Resolve and load schema --
    try:
        schemas_dir = find_schemas_dir()
        version = resolve_schema_version(metadata, schemas_dir)
        schema = load_schema(version, schemas_dir)
    except (OSError, ValueError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)

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
