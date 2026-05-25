"""Scaffold a new DataCoolie project from user choices.

Usage:
    python scaffold.py --name my_project [options]
    python scaffold.py --name my_project --source-type file --platform local --engine polars
    python scaffold.py --name my_project --source-type database --platform aws --engine spark
    python scaffold.py --name my_project --source-type file,api --platform local,aws --engine mixed --layers source2bronze,bronze2silver

Options:
    --name            Project name (required)
    --source-type     Source types: file, database, api (comma-separated, default: file)
    --platform        Target platforms: local, aws, fabric, databricks (comma-separated, default: local)
    --engine          Engine strategy: polars, spark, mixed (default: polars)
    --layers          Medallion layers: source2bronze, bronze2silver, silver2gold (comma-separated, default: source2bronze)
    --output          Output directory (default: ./{name})
    --dest-format     Destination format: delta, parquet, iceberg (default: delta)
    --metadata-layout Metadata file layout: combined (single metadata.json) or split (connections.json + dataflows.json) (default: combined)

Exit codes:
    0 = success
    1 = validation error
    2 = input error
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from _templates import (
    CONFIG_YAML,
    CONNECTIONS_JSON_API,
    CONNECTIONS_JSON_DB,
    CONNECTIONS_JSON_FILE,
    DATAFLOWS_JSON,
    ENV_DEV_YAML,
    ENV_PROD_YAML,
    FUNCTIONS_INIT_PY,
    FUNCTIONS_PYPROJECT_TOML,
    FUNCTIONS_SOURCES_PY,
    GITIGNORE,
    REQUIREMENTS_TXT,
    RUN_LOCAL_PY,
    RUN_LOCAL_PY_SPLIT,
)

# ---------------------------------------------------------------------------
# Engine advisory logic (Phase 2.3)
# ---------------------------------------------------------------------------

ADVISORY_TABLE = """\
┌─────────────────────────────────┬──────────────┬──────────────────────────────────────┐
│ Stage                           │ Recommended  │ Reason                               │
├─────────────────────────────────┼──────────────┼──────────────────────────────────────┤
{rows}└─────────────────────────────────┴──────────────┴──────────────────────────────────────┘"""

ADVISORY_ROW = "│ {stage:<32}│ {engine:<13}│ {reason:<37}│\n"


def recommend_engine(layer: str, source_type: str) -> tuple[str, str]:
    """Return (engine, reason) recommendation for a given layer + source."""
    if layer == "source2bronze":
        if source_type in ("file", "api"):
            return "polars", "File I/O ingest < 10GB typical"
        return "spark", "Database extract may require distributed reads"
    elif layer == "bronze2silver":
        return "spark", "Transforms with joins benefit from distribution"
    elif layer == "silver2gold":
        return "spark", "Aggregation/gold stages often large-scale"
    return "polars", "Default for unknown stages"


def build_advisory(layers: list[str], source_types: list[str], engine: str) -> str:
    """Build engine recommendation table. Returns formatted string."""
    if engine != "mixed":
        return f"  Engine strategy: {engine} (user-selected, no advisory needed)\n"

    rows = ""
    for layer in layers:
        for src in source_types:
            stage_name = f"{layer} ({src})"
            rec_engine, reason = recommend_engine(layer, src)
            rows += ADVISORY_ROW.format(stage=stage_name, engine=rec_engine, reason=reason)
    return ADVISORY_TABLE.format(rows=rows)


def build_engine_overrides_yaml(layers: list[str], source_types: list[str], engine: str) -> str:
    """Build engine_strategy overrides section for config.yaml."""
    if engine != "mixed":
        return ""
    lines = ["  overrides:\n"]
    for layer in layers:
        for src in source_types:
            rec, _ = recommend_engine(layer, src)
            lines.append(f'    "{layer}__{src}": "{rec}"\n')
    return "".join(lines)


# ---------------------------------------------------------------------------
# Scaffold generation
# ---------------------------------------------------------------------------


def generate_project(
    name: str,
    source_types: list[str],
    platforms: list[str],
    engine: str,
    layers: list[str],
    output_dir: Path,
    dest_format: str,
    metadata_layout: str = "combined",
) -> list[str]:
    """Generate project directory structure. Returns list of created files."""
    created: list[str] = []

    # Determine default engine for templates
    default_engine = engine if engine != "mixed" else "polars"

    # Build format-specific connection template args
    source_format = "csv"  # default for file sources
    if "database" in source_types:
        source_format = "sql"
    elif "api" in source_types:
        source_format = "api"

    source_conn_name = f"{name}_source"
    dest_conn_name = f"{name}_destination"
    dest_connection_type = "lakehouse" if dest_format in ("delta", "iceberg") else "file"

    template_vars = {
        "project_name": name,
        "source_conn_name": source_conn_name,
        "dest_conn_name": dest_conn_name,
        "source_format": source_format,
        "dest_format": dest_format,
        "dest_connection_type": dest_connection_type,
        "default_engine": default_engine,
    }

    # --- Directory structure ---
    dirs = [
        output_dir / "metadata" / "environments",
        output_dir / "functions",
        output_dir / "data" / "eval" / "input",
        output_dir / "data" / "eval" / "output",
        output_dir / "scripts",
        output_dir / ".datacoolie",
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)

    # --- .datacoolie/config.yaml ---
    platforms_yaml = "\n".join(f'  - "{p}"' for p in platforms)
    layers_yaml = "\n".join(f'  - "{lyr}"' for lyr in layers)
    engine_overrides = build_engine_overrides_yaml(layers, source_types, engine)

    config_content = CONFIG_YAML.format(
        project_name=name,
        default_engine=default_engine,
        engine_overrides=engine_overrides,
        platforms_yaml=platforms_yaml,
        layers_yaml=layers_yaml,
    )
    _write(output_dir / ".datacoolie" / "config.yaml", config_content, created)

    # --- metadata/connections.json ---
    primary_source = source_types[0]
    if primary_source == "database":
        conn_template = CONNECTIONS_JSON_DB
    elif primary_source == "api":
        conn_template = CONNECTIONS_JSON_API
    else:
        conn_template = CONNECTIONS_JSON_FILE

    connections_content = conn_template.format(**template_vars)
    connections_list = json.loads(connections_content)

    # --- metadata/dataflows.json ---
    dataflows_list = []
    for i, layer in enumerate(layers):
        df_vars = {
            **template_vars,
            "dataflow_name": f"{name}_{layer}",
            "stage": layer,
        }
        dataflows_list.append(json.loads(DATAFLOWS_JSON.format(**df_vars))[0])

    # --- Write metadata files based on layout ---
    if metadata_layout == "combined":
        combined_metadata = {
            "connections": connections_list,
            "dataflows": dataflows_list,
        }
        combined_json = json.dumps(combined_metadata, indent=2, ensure_ascii=False) + "\n"
        _write(output_dir / "metadata" / "metadata.json", combined_json, created)
    else:
        # Split layout: separate connections + dataflows files
        _write(output_dir / "metadata" / "connections.json",
               json.dumps(connections_list, indent=2, ensure_ascii=False) + "\n", created)
        _write(output_dir / "metadata" / "dataflows.json",
               json.dumps(dataflows_list, indent=2, ensure_ascii=False) + "\n", created)

    # --- metadata/environments/ ---
    _write(output_dir / "metadata" / "environments" / "dev.yaml",
           ENV_DEV_YAML.format(**template_vars), created)
    _write(output_dir / "metadata" / "environments" / "prod.yaml",
           ENV_PROD_YAML.format(**template_vars), created)

    # --- functions/ ---
    _write(output_dir / "functions" / "__init__.py",
           FUNCTIONS_INIT_PY.format(**template_vars), created)
    _write(output_dir / "functions" / "sources.py",
           FUNCTIONS_SOURCES_PY.format(**template_vars), created)
    _write(output_dir / "functions" / "pyproject.toml",
           FUNCTIONS_PYPROJECT_TOML.format(**template_vars), created)

    # --- scripts/run_local.py ---
    runner_template = RUN_LOCAL_PY_SPLIT if metadata_layout == "split" else RUN_LOCAL_PY
    _write(output_dir / "scripts" / "run_local.py",
           runner_template.format(**template_vars), created)

    # --- Root files ---
    _write(output_dir / ".gitignore", GITIGNORE, created)
    _write(output_dir / "requirements.txt", REQUIREMENTS_TXT, created)

    return created


def _write(path: Path, content: str, created: list[str]) -> None:
    """Write content to path, creating parent dirs as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    created.append(str(path))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Scaffold a new DataCoolie project.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--name", required=True, help="Project name.")
    parser.add_argument(
        "--source-type", default="file",
        help="Source types (comma-separated): file, database, api. Default: file.",
    )
    parser.add_argument(
        "--platform", default="local",
        help="Target platforms (comma-separated): local, aws, fabric, databricks. Default: local.",
    )
    parser.add_argument(
        "--engine", default="polars", choices=["polars", "spark", "mixed"],
        help="Engine strategy. Default: polars.",
    )
    parser.add_argument(
        "--layers", default="source2bronze",
        help="Medallion layers (comma-separated): source2bronze, bronze2silver, silver2gold. Default: source2bronze.",
    )
    parser.add_argument(
        "--output", type=Path, default=None,
        help="Output directory. Default: ./{name}",
    )
    parser.add_argument(
        "--dest-format", default="delta", choices=["delta", "parquet", "iceberg"],
        help="Destination format. Default: delta.",
    )
    parser.add_argument(
        "--metadata-layout", default="combined", choices=["combined", "split"],
        help="Metadata file layout. combined: single metadata.json. split: separate connections.json + dataflows.json. Default: combined.",
    )
    args = parser.parse_args()

    # Parse comma-separated values
    source_types = [s.strip() for s in args.source_type.split(",")]
    platforms = [p.strip() for p in args.platform.split(",")]
    layers = [lyr.strip() for lyr in args.layers.split(",")]

    # Validate
    valid_sources = {"file", "database", "api"}
    valid_platforms = {"local", "aws", "fabric", "databricks"}
    valid_layers = {"source2bronze", "bronze2silver", "silver2gold"}

    for s in source_types:
        if s not in valid_sources:
            print(f"ERROR: Invalid source type '{s}'. Valid: {sorted(valid_sources)}", file=sys.stderr)
            sys.exit(2)
    for p in platforms:
        if p not in valid_platforms:
            print(f"ERROR: Invalid platform '{p}'. Valid: {sorted(valid_platforms)}", file=sys.stderr)
            sys.exit(2)
    for lyr in layers:
        if lyr not in valid_layers:
            print(f"ERROR: Invalid layer '{lyr}'. Valid: {sorted(valid_layers)}", file=sys.stderr)
            sys.exit(2)

    output_dir = args.output or Path(f"./{args.name}")

    if output_dir.exists() and any(output_dir.iterdir()):
        print(f"WARNING: Output directory '{output_dir}' already exists. Files may be overwritten.", file=sys.stderr)

    # Show engine advisory for mixed mode
    if args.engine == "mixed":
        advisory = build_advisory(layers, source_types, args.engine)
        print("Engine Advisory:\n")
        print(advisory)
        print()

    # Generate
    created = generate_project(
        name=args.name,
        source_types=source_types,
        platforms=platforms,
        engine=args.engine,
        layers=layers,
        output_dir=output_dir,
        dest_format=args.dest_format,
        metadata_layout=args.metadata_layout,
    )

    print(f"✓ Scaffolded project '{args.name}' at {output_dir}")
    print(f"  {len(created)} files created:")
    for f in created:
        rel = Path(f).relative_to(output_dir) if Path(f).is_relative_to(output_dir) else f
        print(f"    {rel}")

    print(f"\nNext steps:")
    print(f"  cd {output_dir}")
    print(f"  pip install datacoolie")
    print(f"  python scripts/run_local.py --dry-run")
    sys.exit(0)


if __name__ == "__main__":
    main()
