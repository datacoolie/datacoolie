"""introspect_lakehouse.py — Lakehouse catalog introspection.

Introspects catalog-based lakehouses (Iceberg REST, Hive Metastore, Unity
Catalog, AWS Glue) and outputs the standard discovery observations CSV.

Usage:
    python introspect_lakehouse.py --iceberg http://localhost:8181 --source lakehouse
    python introspect_lakehouse.py --iceberg http://localhost:8181 --source lh --namespaces sales,analytics
    python introspect_lakehouse.py --hive localhost:10000 --source hive-dw --databases staging
    python introspect_lakehouse.py --unity --catalog main --schemas default --source unity-prod
    python introspect_lakehouse.py --glue --database analytics --source glue-dw --region us-east-1
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from _observation_contract import (
    CSV_HEADER as CSV_HEADER,  # noqa: F401 - public cross-probe contract
    atomic_write_observations,
    make_observation,
    write_observations,
)
from _input_safety import validate_nonsecret_locator
from _probe_status import PARTIAL_EXIT_CODE, write_probe_status

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Iceberg REST catalog
# ---------------------------------------------------------------------------

ICEBERG_TYPE_MAP = {
    "boolean": ("boolean", "", "", ""),
    "int": ("integer", "", "", ""),
    "long": ("long", "", "", ""),
    "float": ("float", "", "", ""),
    "double": ("double", "", "", ""),
    "string": ("string", "", "", ""),
    "binary": ("binary", "", "", ""),
    "date": ("date", "", "", ""),
    "time": ("time", "", "", ""),
    "timestamp": ("timestamp", "", "", ""),
    "timestamptz": ("timestamp_tz", "", "", ""),
    "uuid": ("string", "uuid", "", ""),
    "fixed": ("binary", "", "", ""),
}


def _map_iceberg_type(type_obj: Any) -> tuple[str, str, str, str]:
    """Map an Iceberg type (from REST API JSON) to canonical tuple."""
    if isinstance(type_obj, str):
        return ICEBERG_TYPE_MAP.get(type_obj, (type_obj, "", "", ""))

    if isinstance(type_obj, dict):
        type_id = type_obj.get("type", "")

        # Decimal
        if type_id == "decimal":
            p = type_obj.get("precision", 38)
            s = type_obj.get("scale", 0)
            return f"decimal({p},{s})", "", str(p), str(s)

        # Fixed
        if type_id == "fixed":
            length = type_obj.get("length", "")
            return "binary", "", str(length), ""

        # List
        if type_id == "list":
            elem = type_obj.get("element", "")
            elem_type, _, _, _ = _map_iceberg_type(elem)
            return f"array<{elem_type}>", "", "", ""

        # Map
        if type_id == "map":
            key_type, _, _, _ = _map_iceberg_type(type_obj.get("key", "string"))
            val_type, _, _, _ = _map_iceberg_type(type_obj.get("value", "string"))
            return f"struct<map<{key_type},{val_type}>>", "", "", ""

        # Struct
        if type_id == "struct":
            return "struct", str(type_obj), "", ""

        # Nested primitives in {"type": "string"} form
        if type_id in ICEBERG_TYPE_MAP:
            return ICEBERG_TYPE_MAP[type_id]

        return type_id, "", "", ""

    return str(type_obj), "", "", ""


def introspect_iceberg(
    catalog_url: str,
    source: str,
    namespaces: list[str] | None = None,
    tables_filter: list[str] | None = None,
    headers: dict | None = None,
    issues: list[str] | None = None,
) -> list[dict[str, str]]:
    """Introspect Iceberg REST catalog and return CSV rows."""
    import requests

    base = catalog_url.rstrip("/")
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)

    # Discover namespaces
    if namespaces:
        ns_list = [ns.split(".") for ns in namespaces]
    else:
        try:
            resp = requests.get(f"{base}/v1/namespaces", headers=hdrs, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            ns_list = data.get("namespaces", [])
        except requests.RequestException as exc:
            print(
                f"ERROR: Cannot list namespaces ({type(exc).__name__})",
                file=sys.stderr,
            )
            sys.exit(1)

    rows: list[dict[str, str]] = []
    issue_list = issues if issues is not None else []

    for ns_parts in ns_list:
        if isinstance(ns_parts, str):
            ns_parts = [ns_parts]
        ns_name = ".".join(ns_parts)
        ns_path = "\x1f".join(ns_parts)  # Iceberg REST uses \x1f as separator

        # List tables in namespace
        try:
            resp = requests.get(
                f"{base}/v1/namespaces/{ns_path}/tables",
                headers=hdrs, timeout=15,
            )
            resp.raise_for_status()
            table_list = resp.json().get("identifiers", [])
        except requests.RequestException as exc:
            issue = f"Cannot list tables in namespace '{ns_name}': {type(exc).__name__}"
            issue_list.append(issue)
            print(f"WARNING: {issue}", file=sys.stderr)
            continue

        for tbl_ident in table_list:
            tbl_name = tbl_ident.get("name", "")
            tbl_ns = ".".join(tbl_ident.get("namespace", ns_parts))

            if tables_filter and tbl_name not in tables_filter:
                continue

            # Get table metadata
            try:
                resp = requests.get(
                    f"{base}/v1/namespaces/{ns_path}/tables/{tbl_name}",
                    headers=hdrs, timeout=15,
                )
                resp.raise_for_status()
                table_meta = resp.json()
            except requests.RequestException as exc:
                issue = f"Cannot load table '{tbl_ns}.{tbl_name}': {type(exc).__name__}"
                issue_list.append(issue)
                print(f"WARNING: {issue}", file=sys.stderr)
                continue

            schema = table_meta.get("metadata", {}).get("current-schema", {})
            if not schema:
                # Try schemas list and pick last
                schemas_list = table_meta.get("metadata", {}).get("schemas", [])
                if schemas_list:
                    schema = schemas_list[-1]

            fields = schema.get("fields", [])

            # Detect partition columns for notes
            partition_spec = table_meta.get("metadata", {}).get("default-partition-spec", {})
            partition_fields = partition_spec.get("fields", []) if partition_spec else []
            partition_source_ids = {pf.get("source-id") for pf in partition_fields}

            # Build identifier fields set (if any)
            identifier_field_ids = set(schema.get("identifier-field-ids", []))

            for ordinal, field in enumerate(fields, start=1):
                col_name = field.get("name", "")
                type_obj = field.get("type", "string")
                required = field.get("required", False)
                field_id = field.get("id", -1)

                canonical, fmt, prec, scl = _map_iceberg_type(type_obj)

                is_pk = "true" if field_id in identifier_field_ids else ""
                nullable = "false" if required else "true"

                notes_parts = []
                if field_id in partition_source_ids:
                    # Find the transform
                    for pf in partition_fields:
                        if pf.get("source-id") == field_id:
                            transform = pf.get("transform", "identity")
                            notes_parts.append(f"partition:{transform}")
                            break

                rows.append(make_observation(
                    source=source,
                    object_type="table",
                    schema=tbl_ns,
                    object=tbl_name,
                    column=col_name,
                    native_type=json.dumps(type_obj, sort_keys=True) if isinstance(type_obj, dict) else type_obj,
                    data_type=canonical,
                    format=fmt,
                    precision=prec,
                    scale=scl,
                    nullable=nullable,
                    ordinal=ordinal,
                    key="primary" if is_pk else "",
                    notes="; ".join(notes_parts),
                ))

    return rows


# ---------------------------------------------------------------------------
# Hive Metastore (via beeline / PyHive)
# ---------------------------------------------------------------------------

HIVE_TYPE_MAP = {
    "boolean": ("boolean", "", "", ""),
    "tinyint": ("byte", "", "", ""),
    "smallint": ("short", "", "", ""),
    "int": ("integer", "", "", ""),
    "bigint": ("long", "", "", ""),
    "float": ("float", "", "", ""),
    "double": ("double", "", "", ""),
    "string": ("string", "", "", ""),
    "binary": ("binary", "", "", ""),
    "date": ("date", "", "", ""),
    "timestamp": ("timestamp", "", "", ""),
    "void": ("string", "", "", ""),
    "char": ("string", "", "", ""),
    "varchar": ("string", "", "", ""),
}


def _map_hive_type(type_str: str) -> tuple[str, str, str, str]:
    """Map a Hive type string to canonical tuple."""
    t = type_str.strip().lower()

    # decimal(p,s)
    m = re.match(r"decimal\((\d+),\s*(\d+)\)", t)
    if m:
        p, s = m.group(1), m.group(2)
        return f"decimal({p},{s})", "", p, s

    # char(n) / varchar(n)
    m = re.match(r"(char|varchar)\((\d+)\)", t)
    if m:
        return "string", "", m.group(2), ""

    # array<...>
    if t.startswith("array<"):
        return "array", t, "", ""

    # map<...>
    if t.startswith("map<"):
        return "struct", t, "", ""

    # struct<...>
    if t.startswith("struct<"):
        return "struct", t, "", ""

    return HIVE_TYPE_MAP.get(t, (t, "", "", ""))


def introspect_hive(
    host_port: str,
    source: str,
    databases: list[str] | None = None,
    tables_filter: list[str] | None = None,
    issues: list[str] | None = None,
) -> list[dict[str, str]]:
    """Introspect Hive Metastore via PyHive and return CSV rows."""
    try:
        from pyhive import hive
    except ImportError:
        print("ERROR: Install pyhive for Hive support: pip install 'pyhive[hive]'", file=sys.stderr)
        sys.exit(1)

    parts = host_port.split(":")
    host = parts[0]
    port = int(parts[1]) if len(parts) > 1 else 10000

    try:
        conn = hive.connect(host=host, port=port)
    except Exception as exc:
        print(
            f"ERROR: Cannot connect to Hive at {host_port} ({type(exc).__name__})",
            file=sys.stderr,
        )
        sys.exit(1)

    cursor = conn.cursor()

    # Discover databases
    if databases:
        db_list = databases
    else:
        cursor.execute("SHOW DATABASES")
        db_list = [row[0] for row in cursor.fetchall()]
        # Filter out system databases
        db_list = [db for db in db_list if db not in ("default", "sys", "information_schema")]

    rows: list[dict[str, str]] = []
    issue_list = issues if issues is not None else []

    for db_name in db_list:
        try:
            cursor.execute(f"SHOW TABLES IN `{db_name}`")
            table_list = [row[0] for row in cursor.fetchall()]
        except Exception as exc:
            issue = f"Cannot list tables in '{db_name}': {type(exc).__name__}"
            issue_list.append(issue)
            print(f"WARNING: {issue}", file=sys.stderr)
            continue

        for tbl_name in table_list:
            if tables_filter and tbl_name not in tables_filter:
                continue

            try:
                cursor.execute(f"DESCRIBE `{db_name}`.`{tbl_name}`")
                columns = cursor.fetchall()
            except Exception as exc:
                issue = f"Cannot describe '{db_name}.{tbl_name}': {type(exc).__name__}"
                issue_list.append(issue)
                print(f"WARNING: {issue}", file=sys.stderr)
                continue

            # DESCRIBE output: (col_name, data_type, comment)
            # After the columns, there may be partition info rows preceded by a blank line
            ordinal = 0
            in_partition_section = False
            for col_row in columns:
                col_name = (col_row[0] or "").strip()
                col_type = (col_row[1] or "").strip()

                # Skip empty lines and section headers
                if not col_name or col_name.startswith("#"):
                    if col_name and "partition" in col_name.lower():
                        in_partition_section = True
                    continue

                ordinal += 1
                canonical, fmt, prec, scl = _map_hive_type(col_type)

                notes = "partition column" if in_partition_section else ""

                rows.append(make_observation(
                    source=source,
                    object_type="table",
                    schema=db_name,
                    object=tbl_name,
                    column=col_name,
                    native_type=col_type,
                    data_type=canonical,
                    format=fmt,
                    precision=prec,
                    scale=scl,
                    nullable="true",
                    ordinal=ordinal,
                    notes=notes,
                ))

    cursor.close()
    conn.close()
    return rows


# ---------------------------------------------------------------------------
# Databricks Unity Catalog (via CLI)
# ---------------------------------------------------------------------------

def introspect_unity(
    source: str,
    catalog: str,
    schemas: list[str] | None = None,
    tables_filter: list[str] | None = None,
    issues: list[str] | None = None,
) -> list[dict[str, str]]:
    """Introspect Unity Catalog via databricks CLI and return CSV rows."""
    try:
        preflight = subprocess.run(
            ["databricks", "schemas", "list", "--help"],
            capture_output=True, text=True, timeout=15,
        )
    except FileNotFoundError:
        print("ERROR: current 'databricks' CLI is not available", file=sys.stderr)
        sys.exit(1)
    except subprocess.TimeoutExpired:
        print("ERROR: databricks CLI preflight timed out", file=sys.stderr)
        sys.exit(1)
    if preflight.returncode != 0:
        print("ERROR: databricks CLI does not support the required schemas/tables commands", file=sys.stderr)
        sys.exit(1)

    # List schemas
    if schemas:
        schema_list = schemas
    else:
        try:
            result = subprocess.run(
                ["databricks", "schemas", "list", catalog, "--output", "json"],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode != 0:
                print(
                    f"ERROR: databricks CLI failed with exit {result.returncode}",
                    file=sys.stderr,
                )
                sys.exit(1)
            data = json.loads(result.stdout)
            schema_list = [
                s["name"] for s in data.get("schemas", [])
                if s.get("name") not in ("information_schema",)
            ]
        except Exception as exc:
            print(f"ERROR: Cannot list schemas ({type(exc).__name__})", file=sys.stderr)
            sys.exit(1)

    rows: list[dict[str, str]] = []
    issue_list = issues if issues is not None else []

    for schema_name in schema_list:
        # List tables
        try:
            result = subprocess.run(
                ["databricks", "tables", "list", catalog, schema_name, "--output", "json"],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode != 0:
                issue_list.append(f"Cannot list tables in Unity schema '{schema_name}'")
                continue
            data = json.loads(result.stdout)
            table_names = [t["name"] for t in data.get("tables", [])]
        except Exception as exc:
            issue_list.append(f"Cannot list tables in Unity schema '{schema_name}': {type(exc).__name__}")
            continue

        for tbl_name in table_names:
            if tables_filter and tbl_name not in tables_filter:
                continue

            try:
                result = subprocess.run(
                    ["databricks", "tables", "get",
                     f"{catalog}.{schema_name}.{tbl_name}", "--output", "json"],
                    capture_output=True, text=True, timeout=30,
                )
                if result.returncode != 0:
                    issue_list.append(f"Cannot load Unity table '{catalog}.{schema_name}.{tbl_name}'")
                    continue
                table_data = json.loads(result.stdout)
            except Exception as exc:
                issue_list.append(
                    f"Cannot load Unity table '{catalog}.{schema_name}.{tbl_name}': {type(exc).__name__}"
                )
                continue

            columns = table_data.get("columns", [])
            for ordinal, col in enumerate(columns, start=1):
                col_name = col.get("name", "")
                col_type = col.get("type_text", col.get("type_name", "string")).lower()
                nullable = "true" if col.get("nullable", True) else "false"

                canonical, fmt, prec, scl = _map_hive_type(col_type)

                notes = ""
                if col.get("partition_index") is not None:
                    notes = "partition column"

                rows.append(make_observation(
                    source=source,
                    object_type="table",
                    catalog=catalog,
                    schema=schema_name,
                    object=tbl_name,
                    column=col_name,
                    native_type=col_type,
                    data_type=canonical,
                    format=fmt,
                    precision=prec,
                    scale=scl,
                    nullable=nullable,
                    ordinal=ordinal,
                    notes=notes,
                ))

    return rows


# ---------------------------------------------------------------------------
# AWS Glue Data Catalog
# ---------------------------------------------------------------------------

def introspect_glue(
    source: str,
    database: str | None = None,
    tables_filter: list[str] | None = None,
    region: str | None = None,
    issues: list[str] | None = None,
) -> list[dict[str, str]]:
    """Introspect AWS Glue Data Catalog via AWS CLI and return CSV rows."""
    base_cmd = ["aws", "glue"]
    if region:
        base_cmd += ["--region", region]

    # List databases
    if database:
        db_list = [database]
    else:
        try:
            result = subprocess.run(
                base_cmd + ["get-databases", "--output", "json"],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode != 0:
                print(
                    f"ERROR: aws glue failed with exit {result.returncode}",
                    file=sys.stderr,
                )
                sys.exit(1)
            data = json.loads(result.stdout)
            db_list = [d["Name"] for d in data.get("DatabaseList", [])]
        except FileNotFoundError:
            print("ERROR: 'aws' CLI not found. Install AWS CLI.", file=sys.stderr)
            sys.exit(1)
        except Exception as exc:
            print(f"ERROR: Cannot list databases ({type(exc).__name__})", file=sys.stderr)
            sys.exit(1)

    rows: list[dict[str, str]] = []
    issue_list = issues if issues is not None else []

    for db_name in db_list:
        try:
            result = subprocess.run(
                base_cmd + ["get-tables", "--database-name", db_name, "--output", "json"],
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode != 0:
                issue = f"Cannot list tables in Glue database '{db_name}'"
                issue_list.append(issue)
                print(f"WARNING: {issue}", file=sys.stderr)
                continue
            data = json.loads(result.stdout)
        except Exception as exc:
            issue = f"Cannot list tables in Glue database '{db_name}': {type(exc).__name__}"
            issue_list.append(issue)
            print(f"WARNING: {issue}", file=sys.stderr)
            continue

        for table in data.get("TableList", []):
            tbl_name = table.get("Name", "")
            if tables_filter and tbl_name not in tables_filter:
                continue

            # Partition keys
            partition_keys = {pk["Name"] for pk in table.get("PartitionKeys", [])}

            columns = table.get("StorageDescriptor", {}).get("Columns", [])
            # Add partition keys as columns too
            all_columns = columns + table.get("PartitionKeys", [])

            for ordinal, col in enumerate(all_columns, start=1):
                col_name = col.get("Name", "")
                col_type = col.get("Type", "string").lower()

                canonical, fmt, prec, scl = _map_hive_type(col_type)

                notes = ""
                if col_name in partition_keys:
                    notes = "partition column"

                rows.append(make_observation(
                    source=source,
                    object_type="table",
                    schema=db_name,
                    object=tbl_name,
                    column=col_name,
                    native_type=col_type,
                    data_type=canonical,
                    format=fmt,
                    precision=prec,
                    scale=scl,
                    nullable="true",
                    ordinal=ordinal,
                    notes=notes,
                ))

    return rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        allow_abbrev=False,
        description="Introspect lakehouse catalogs and output a standardised schema CSV.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--iceberg", metavar="URL", help="Iceberg REST catalog URL")
    group.add_argument("--hive", metavar="HOST:PORT", help="HiveServer2 host:port")
    group.add_argument("--unity", action="store_true", help="Databricks Unity Catalog (via CLI)")
    group.add_argument("--glue", action="store_true", help="AWS Glue Data Catalog (via CLI)")

    parser.add_argument("--source", required=True, help="Source name for CSV output")
    parser.add_argument("--output", default=None, help="Output CSV file (default: stdout)")

    # Iceberg-specific
    parser.add_argument("--namespaces", default=None, help="Comma-separated namespace filter")
    parser.add_argument("--headers-env", default=None, help="Environment variable containing JSON headers")

    # Hive-specific
    parser.add_argument("--databases", default=None, help="Comma-separated database filter (Hive/Glue)")

    # Unity-specific
    parser.add_argument("--catalog", default=None, help="Unity Catalog name")
    parser.add_argument("--schemas", default=None, help="Comma-separated schema filter (Unity)")

    # Glue-specific
    parser.add_argument("--database", default=None, help="Single Glue database name")
    parser.add_argument("--region", default=None, help="AWS region (Glue)")

    # Common
    parser.add_argument("--tables", default=None, help="Comma-separated table filter")
    parser.add_argument("--status-output", default=None, help="Optional probe status JSON path")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    if args.iceberg:
        try:
            validate_nonsecret_locator(args.iceberg, "Iceberg catalog URL")
        except ValueError as exc:
            raise SystemExit(f"ERROR: {exc}") from exc

    tables_filter = args.tables.split(",") if args.tables else None

    rows: list[dict[str, str]] = []
    issues: list[str] = []

    if args.iceberg:
        hdrs = None
        if args.headers_env:
            raw_headers = os.environ.get(args.headers_env)
            if raw_headers is None:
                raise SystemExit(f"ERROR: Environment variable '{args.headers_env}' is not set")
            try:
                hdrs = json.loads(raw_headers)
            except json.JSONDecodeError as exc:
                raise SystemExit("ERROR: Headers environment value is not valid JSON") from exc
            if not isinstance(hdrs, dict) or not all(
                isinstance(key, str) and isinstance(value, str) for key, value in hdrs.items()
            ):
                raise SystemExit("ERROR: Headers environment value must be a JSON object of strings")
        ns_filter = args.namespaces.split(",") if args.namespaces else None
        rows = introspect_iceberg(
            args.iceberg, args.source, ns_filter, tables_filter, hdrs, issues,
        )
        probe = "iceberg-rest-catalog"
    elif args.hive:
        db_filter = args.databases.split(",") if args.databases else None
        rows = introspect_hive(args.hive, args.source, db_filter, tables_filter, issues)
        probe = "hive-metastore"
    elif args.unity:
        if not args.catalog:
            print("ERROR: --catalog is required for Unity Catalog", file=sys.stderr)
            sys.exit(1)
        schema_filter = args.schemas.split(",") if args.schemas else None
        rows = introspect_unity(args.source, args.catalog, schema_filter, tables_filter, issues)
        probe = "unity-catalog"
    elif args.glue:
        rows = introspect_glue(
            args.source, args.database, tables_filter, args.region, issues,
        )
        probe = "aws-glue-catalog"

    if not rows:
        issues.append("No schema fields extracted from the requested lakehouse scope")
        print(f"WARNING: {issues[-1]}", file=sys.stderr)

    if args.output:
        atomic_write_observations(Path(args.output), rows)
    else:
        write_observations(sys.stdout, rows)
    status = write_probe_status(
        Path(args.status_output) if args.status_output else None,
        source=args.source,
        probe=probe,
        row_count=len(rows),
        issues=issues,
    )
    if status == "partial":
        raise SystemExit(PARTIAL_EXIT_CODE)


if __name__ == "__main__":
    main()
