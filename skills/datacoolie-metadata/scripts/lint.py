"""Lint DataCoolie metadata for best-practice warnings beyond schema validation.

Usage:
    python lint.py <metadata_file> [--engine polars|spark] [--env dev|test|prod]

Exit codes:
    0 = no warnings
    1 = warnings found
    2 = input error
"""

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

import yaml

from _loaders import load_metadata


@dataclass
class LintWarning:
    """A single lint finding."""
    severity: str  # "WARN" or "INFO"
    path: str
    rule: str
    message: str
    suggestion: str

    def __str__(self) -> str:
        return f"  [{self.severity}] {self.path}: {self.message}\n         \u2192 {self.suggestion}"


@dataclass
class LintContext:
    """Context passed to every lint rule function."""
    dataflows: list
    connections: dict
    engine: str
    env: str


def build_connection_map(metadata: dict) -> dict:
    """Build a lookup of connection name → connection object."""
    return {c["name"]: c for c in metadata.get("connections", []) if "name" in c}


def lint_merge_keys_missing(ctx: LintContext) -> list[LintWarning]:
    """WARN: merge/scd2 load_type without merge_keys."""
    warnings = []
    merge_types = {"merge_upsert", "merge_overwrite", "scd2"}
    for i, df in enumerate(ctx.dataflows):
        dest = df.get("destination", {})
        load_type = dest.get("load_type", "")
        merge_keys = dest.get("merge_keys", [])
        if load_type in merge_types and not merge_keys:
            warnings.append(LintWarning(
                severity="WARN",
                path=f"dataflows[{i}].destination",
                rule="merge-keys-required",
                message=f"load_type is '{load_type}' but merge_keys is empty.",
                suggestion="Add merge_keys to define the join condition for merge/scd2 operations.",
            ))
    return warnings


def lint_incremental_without_watermark(ctx: LintContext) -> list[LintWarning]:
    """WARN: non-full-load destination but source has no watermark_columns."""
    warnings = []
    full_load_types = {"full_load", "overwrite"}
    for i, df in enumerate(ctx.dataflows):
        dest = df.get("destination", {})
        source = df.get("source", {})
        load_type = dest.get("load_type", "full_load")
        watermark = source.get("watermark_columns", [])
        if load_type not in full_load_types and not watermark:
            warnings.append(LintWarning(
                severity="WARN",
                path=f"dataflows[{i}].source",
                rule="watermark-for-incremental",
                message=f"Incremental load_type '{load_type}' but source has no watermark_columns.",
                suggestion="Add watermark_columns to enable incremental extraction and avoid full scans.",
            ))
    return warnings


def lint_infer_schema_in_prod(ctx: LintContext) -> list[LintWarning]:
    """WARN: inferSchema in read_options when env is not dev."""
    warnings = []
    if ctx.env == "dev":
        return warnings
    for i, df in enumerate(ctx.dataflows):
        source = df.get("source", {})
        configure = source.get("configure", {})
        read_options = configure.get("read_options", {})
        if isinstance(read_options, dict) and read_options.get("inferSchema") in (True, "true", "True"):
            warnings.append(LintWarning(
                severity="WARN",
                path=f"dataflows[{i}].source.configure.read_options",
                rule="no-infer-schema-prod",
                message="inferSchema is enabled in non-dev environment.",
                suggestion="Use schema_hints instead of inferSchema for stable, predictable typing in test/prod.",
            ))
    return warnings


def lint_dot_notation_filter(ctx: LintContext) -> list[LintWarning]:
    """WARN: filter_expression uses dot notation (Spark-only, not portable)."""
    warnings = []
    if ctx.engine == "spark":
        return warnings  # Dot notation is valid in Spark context
    for i, df in enumerate(ctx.dataflows):
        source = df.get("source", {})
        filter_expr = source.get("filter_expression", "")
        if filter_expr and "col." in filter_expr:
            warnings.append(LintWarning(
                severity="WARN",
                path=f"dataflows[{i}].source.filter_expression",
                rule="portable-filter-expression",
                message="filter_expression uses dot notation (col.) which is Spark-specific.",
                suggestion="Use SQL-style expressions for engine-portable filters.",
            ))
        transform = df.get("transform", {})
        t_filter = transform.get("filter_expression", "")
        if t_filter and "col." in t_filter:
            warnings.append(LintWarning(
                severity="WARN",
                path=f"dataflows[{i}].transform.filter_expression",
                rule="portable-filter-expression",
                message="filter_expression uses dot notation (col.) which is Spark-specific.",
                suggestion="Use SQL-style expressions for engine-portable filters.",
            ))
    return warnings


def lint_undefined_connection(ctx: LintContext) -> list[LintWarning]:
    """WARN: dataflow references a connection_name not defined in connections[]."""
    warnings = []
    for i, df in enumerate(ctx.dataflows):
        for endpoint in ("source", "destination"):
            ep = df.get(endpoint, {})
            conn_name = ep.get("connection_name", "")
            if conn_name and conn_name not in ctx.connections:
                warnings.append(LintWarning(
                    severity="WARN",
                    path=f"dataflows[{i}].{endpoint}.connection_name",
                    rule="undefined-connection",
                    message=f"References connection '{conn_name}' which is not defined in connections[].",
                    suggestion="Add the connection to connections[] or fix the connection_name typo.",
                ))
    return warnings


def lint_inactive_connection_used(ctx: LintContext) -> list[LintWarning]:
    """WARN: dataflow references a connection that is marked inactive."""
    warnings = []
    for i, df in enumerate(ctx.dataflows):
        for endpoint in ("source", "destination"):
            ep = df.get(endpoint, {})
            conn_name = ep.get("connection_name", "")
            conn = ctx.connections.get(conn_name)
            if conn and conn.get("is_active") is False:
                warnings.append(LintWarning(
                    severity="WARN",
                    path=f"dataflows[{i}].{endpoint}.connection_name",
                    rule="inactive-connection",
                    message=f"References connection '{conn_name}' which is marked is_active: false.",
                    suggestion="Either activate the connection or remove the dataflow.",
                ))
    return warnings


def lint_duplicate_dataflow_names(ctx: LintContext) -> list[LintWarning]:
    """WARN: duplicate dataflow names."""
    warnings = []
    seen = {}
    for i, df in enumerate(ctx.dataflows):
        name = df.get("name", "")
        if name in seen:
            warnings.append(LintWarning(
                severity="WARN",
                path=f"dataflows[{i}].name",
                rule="unique-dataflow-name",
                message=f"Duplicate dataflow name '{name}' (first at index {seen[name]}).",
                suggestion="Use unique names for each dataflow to avoid ambiguity in orchestration.",
            ))
        else:
            seen[name] = i
    return warnings


def lint_scd2_missing_effective_column(ctx: LintContext) -> list[LintWarning]:
    """WARN: scd2 load_type without scd2_effective_column in destination.configure."""
    warnings = []
    for i, df in enumerate(ctx.dataflows):
        dest = df.get("destination", {})
        if dest.get("load_type") != "scd2":
            continue
        configure = dest.get("configure", {})
        if not configure.get("scd2_effective_column"):
            warnings.append(LintWarning(
                severity="WARN",
                path=f"dataflows[{i}].destination.configure",
                rule="scd2-effective-column-required",
                message="load_type is 'scd2' but scd2_effective_column is not set.",
                suggestion="Set destination.configure.scd2_effective_column to the column used as __valid_from (e.g. 'modified_at').",
            ))
    return warnings



# All lint rules
LINT_RULES = [
    lint_merge_keys_missing,
    lint_incremental_without_watermark,
    lint_infer_schema_in_prod,
    lint_dot_notation_filter,
    lint_undefined_connection,
    lint_inactive_connection_used,
    lint_duplicate_dataflow_names,
    lint_scd2_missing_effective_column,
]


def run_lint(metadata: dict, engine: str, env: str) -> list[LintWarning]:
    """Run all lint rules and return warnings."""
    ctx = LintContext(
        dataflows=metadata.get("dataflows", []),
        connections=build_connection_map(metadata),
        engine=engine,
        env=env,
    )
    return [w for rule in LINT_RULES for w in rule(ctx)]


def main():
    parser = argparse.ArgumentParser(
        description="Lint DataCoolie metadata for best-practice warnings."
    )
    parser.add_argument("metadata_file", type=Path, help="Path to metadata JSON or YAML file.")
    parser.add_argument("--engine", choices=["polars", "spark"], default="polars", help="Target engine.")
    parser.add_argument("--env", choices=["dev", "test", "prod"], default="dev", help="Target environment.")
    parser.add_argument("--quiet", "-q", action="store_true", help="Suppress output; exit code only (for CI).")
    args = parser.parse_args()

    if not args.metadata_file.exists():
        print(f"ERROR: File not found: {args.metadata_file}", file=sys.stderr)
        sys.exit(2)

    try:
        metadata = load_metadata(args.metadata_file)
    except (json.JSONDecodeError, yaml.YAMLError) as e:
        print(f"ERROR: Failed to parse {args.metadata_file}: {e}", file=sys.stderr)
        sys.exit(2)
    except ImportError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)

    warnings = run_lint(metadata, args.engine, args.env)

    if not warnings:
        if not args.quiet:
            print(f"✓ {args.metadata_file}: no lint warnings ({args.engine}/{args.env})")
        sys.exit(0)
    else:
        if not args.quiet:
            print(f"⚠ {args.metadata_file}: {len(warnings)} warning(s) ({args.engine}/{args.env}):\n")
            for w in warnings:
                print(w)
                print()
        sys.exit(1)


if __name__ == "__main__":
    main()
