"""Introspect data sources and generate DataCoolie metadata.

Modes:
    1. Folder scan — infer schema from files in a directory
    2. DDL parse  — extract columns from CREATE TABLE statements
    3. Manual     — accept column definitions as key:type pairs

Usage:
    python introspect.py --mode folder --path ./data/input/csv --connection-name my_csv
    python introspect.py --mode folder --path ./data/input/parquet --connection-name my_pq
    python introspect.py --mode ddl --ddl-file schema.sql --connection-name my_db
    python introspect.py --mode manual --columns "id:int,name:string,amount:decimal,created_at:timestamp"
    python introspect.py --mode folder --path ./data --connection-name src --dest-connection dest --dest-format delta

Options:
    --mode              folder, ddl, or manual (required)
    --path              Directory to scan (folder mode)
    --connection-name   Name for generated source connection
    --dest-connection   Name for destination connection (default: {connection_name}_dest)
    --dest-format       Destination format: delta, parquet, iceberg (default: delta)
    --ddl-file          SQL DDL file path (ddl mode)
    --columns           Comma-separated col:type pairs (manual mode)
    --output            Output file path (default: stdout as JSON)

Exit codes:
    0 = success
    1 = introspection error
    2 = input error
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# SQL type → DataCoolie data_type mapping
# ---------------------------------------------------------------------------

SQL_TYPE_MAP: dict[str, str] = {
    # Integers
    "int": "int",
    "integer": "int",
    "bigint": "int",
    "smallint": "int",
    "tinyint": "int",
    "serial": "int",
    "bigserial": "int",
    # Strings
    "varchar": "string",
    "char": "string",
    "text": "string",
    "nvarchar": "string",
    "nchar": "string",
    "string": "string",
    "str": "string",
    # Decimals
    "decimal": "decimal",
    "numeric": "decimal",
    "float": "decimal",
    "double": "decimal",
    "real": "decimal",
    "money": "decimal",
    # Timestamps
    "timestamp": "timestamp",
    "datetime": "timestamp",
    "datetime2": "timestamp",
    "timestamptz": "timestamp",
    # Dates
    "date": "date",
    # Booleans
    "boolean": "boolean",
    "bool": "boolean",
    "bit": "boolean",
}


def map_sql_type(raw_type: str) -> str:
    """Map a SQL/file type to DataCoolie data_type value."""
    base = raw_type.lower().strip().split("(")[0].strip()
    return SQL_TYPE_MAP.get(base, "string")


# ---------------------------------------------------------------------------
# Folder scan
# ---------------------------------------------------------------------------


def infer_csv_schema(file_path: Path) -> list[dict]:
    """Read CSV headers and infer types from first few rows."""
    schema_hints = []
    with open(file_path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        # Read up to 100 rows for type inference
        rows = []
        for i, row in enumerate(reader):
            rows.append(row)
            if i >= 99:
                break

    for i, col in enumerate(headers):
        inferred = _infer_column_type(col, [r.get(col, "") for r in rows])
        schema_hints.append({
            "column_name": col,
            "data_type": inferred,
            "ordinal_position": i + 1,
        })
    return schema_hints


def _infer_column_type(col_name: str, values: list[str]) -> str:
    """Heuristic type inference from column name and sample values."""
    # Name-based hints
    name_lower = col_name.lower()
    if any(k in name_lower for k in ("_at", "_date", "timestamp", "created", "updated", "modified")):
        return "timestamp"
    if any(k in name_lower for k in ("_id", "_count", "quantity", "qty", "number", "num")):
        return "int"
    if any(k in name_lower for k in ("amount", "price", "cost", "total", "rate", "salary", "balance")):
        return "decimal"
    if any(k in name_lower for k in ("is_", "has_", "flag", "active", "enabled")):
        return "boolean"

    # Value-based inference
    non_empty = [v for v in values if v.strip()]
    if not non_empty:
        return "string"

    # Try int
    if all(_is_int(v) for v in non_empty[:20]):
        return "int"
    # Try decimal
    if all(_is_decimal(v) for v in non_empty[:20]):
        return "decimal"
    # Try boolean
    if all(v.lower() in ("true", "false", "0", "1", "yes", "no") for v in non_empty[:20]):
        return "boolean"

    return "string"


def _is_int(v: str) -> bool:
    try:
        int(v.replace(",", ""))
        return True
    except ValueError:
        return False


def _is_decimal(v: str) -> bool:
    try:
        float(v.replace(",", ""))
        return "." in v or "e" in v.lower()
    except ValueError:
        return False


def infer_json_schema(file_path: Path) -> list[dict]:
    """Read JSON and infer schema from keys of first record."""
    with open(file_path, encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list) and data:
        record = data[0]
    elif isinstance(data, dict):
        record = data
    else:
        return []

    schema_hints = []
    for i, (key, value) in enumerate(record.items()):
        dtype = _python_type_to_datacoolie(value, key)
        schema_hints.append({
            "column_name": key,
            "data_type": dtype,
            "ordinal_position": i + 1,
        })
    return schema_hints


def _python_type_to_datacoolie(value, key: str = "") -> str:
    """Map a Python value to DataCoolie data_type."""
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "decimal"
    if isinstance(value, str):
        name_lower = key.lower()
        if any(k in name_lower for k in ("_at", "_date", "timestamp", "created", "updated")):
            return "timestamp"
        if any(k in name_lower for k in ("amount", "price", "cost", "total")):
            return "decimal"
        return "string"
    return "string"


def infer_parquet_schema(file_path: Path) -> list[dict]:
    """Read Parquet schema metadata. Requires pyarrow or fastparquet."""
    try:
        import pyarrow.parquet as pq
        schema = pq.read_schema(file_path)
        hints = []
        for i, field in enumerate(schema):
            dtype = _arrow_type_to_datacoolie(str(field.type))
            hints.append({
                "column_name": field.name,
                "data_type": dtype,
                "ordinal_position": i + 1,
            })
        return hints
    except ImportError:
        pass

    # Fallback: try fastparquet
    try:
        import fastparquet
        pf = fastparquet.ParquetFile(str(file_path))
        hints = []
        for i, col in enumerate(pf.columns):
            hints.append({
                "column_name": col,
                "data_type": "string",  # can't infer without pyarrow
                "ordinal_position": i + 1,
            })
        return hints
    except ImportError:
        print("WARNING: Neither pyarrow nor fastparquet installed. Cannot infer Parquet schema.", file=sys.stderr)
        return []


def _arrow_type_to_datacoolie(arrow_type: str) -> str:
    """Map Arrow type string to DataCoolie data_type."""
    t = arrow_type.lower()
    if "int" in t:
        return "int"
    if "float" in t or "double" in t or "decimal" in t:
        return "decimal"
    if "timestamp" in t:
        return "timestamp"
    if "date" in t:
        return "date"
    if "bool" in t:
        return "boolean"
    return "string"


def scan_folder(directory: Path, connection_name: str) -> tuple[list[dict], list[dict], list[dict]]:
    """Scan a directory and generate connections, dataflows, and schema_hints.

    Returns (connections, dataflows, schema_hints_per_table).
    """
    directory = directory.resolve()
    if not directory.is_dir():
        raise ValueError(f"Not a directory: {directory}")

    # Detect format from files in directory
    tables: list[dict] = []

    # Check if directory contains subdirectories (each = table)
    subdirs = [d for d in directory.iterdir() if d.is_dir() and not d.name.startswith(".")]

    if subdirs:
        for subdir in sorted(subdirs):
            table_info = _scan_table_dir(subdir)
            if table_info:
                tables.append(table_info)
    else:
        # Flat directory — each file group is a table
        table_info = _scan_table_dir(directory)
        if table_info:
            tables.append(table_info)

    if not tables:
        raise ValueError(f"No recognizable data files found in {directory}")

    # Detect format from first table
    detected_format = tables[0]["format"]

    # Build connection
    connection = {
        "name": connection_name,
        "connection_type": "file",
        "format": detected_format,
        "configure": {
            "base_path": str(directory),
        },
    }

    # Build dataflows
    dataflows = []
    all_schema_hints = []
    for table in tables:
        dataflow = {
            "name": f"ingest_{table['name']}",
            "stage": "source2bronze",
            "source": {
                "connection_name": connection_name,
                "table": table["name"],
            },
            "destination": {
                "connection_name": f"{connection_name}_dest",
                "table": table["name"],
                "load_type": "full_load",
            },
        }
        if table.get("schema_hints"):
            dataflow["transform"] = {
                "schema_hints": table["schema_hints"],
            }
            all_schema_hints.extend(table["schema_hints"])
        dataflows.append(dataflow)

    return [connection], dataflows, all_schema_hints


def _scan_table_dir(directory: Path) -> dict | None:
    """Scan a single directory for data files and return table info."""
    files = list(directory.iterdir())
    data_files = [f for f in files if f.is_file() and not f.name.startswith(".")]

    if not data_files:
        return None

    # Detect format from file extension
    first_file = data_files[0]
    ext = first_file.suffix.lower()
    format_map = {
        ".csv": "csv",
        ".tsv": "csv",
        ".json": "json",
        ".jsonl": "jsonl",
        ".parquet": "parquet",
        ".avro": "avro",
        ".xlsx": "excel",
        ".xls": "excel",
    }
    detected_format = format_map.get(ext)
    if not detected_format:
        return None

    # Infer schema from first file
    schema_hints = []
    if detected_format == "csv":
        schema_hints = infer_csv_schema(first_file)
    elif detected_format == "json":
        schema_hints = infer_json_schema(first_file)
    elif detected_format == "parquet":
        schema_hints = infer_parquet_schema(first_file)

    return {
        "name": directory.name,
        "format": detected_format,
        "schema_hints": schema_hints,
    }


# ---------------------------------------------------------------------------
# DDL parse
# ---------------------------------------------------------------------------

_DDL_PATTERN = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`\"\[]?(\w+)[`\"\]]?\s*\((.*?)\)\s*;",
    re.IGNORECASE | re.DOTALL,
)

_COL_PATTERN = re.compile(
    r"^\s*[`\"\[]?(\w+)[`\"\]]?\s+(\w+(?:\([^)]*\))?)",
    re.MULTILINE,
)


def parse_ddl(ddl_text: str, connection_name: str) -> tuple[list[dict], list[dict]]:
    """Parse CREATE TABLE statements and generate metadata.

    Returns (dataflows, schema_hints_per_table).
    """
    dataflows = []

    for match in _DDL_PATTERN.finditer(ddl_text):
        table_name = match.group(1)
        columns_text = match.group(2)

        schema_hints = []
        for i, col_match in enumerate(_COL_PATTERN.finditer(columns_text)):
            col_name = col_match.group(1)
            col_type = col_match.group(2)
            # Skip constraint keywords
            if col_name.upper() in ("PRIMARY", "FOREIGN", "UNIQUE", "CHECK", "CONSTRAINT", "INDEX"):
                continue
            schema_hints.append({
                "column_name": col_name,
                "data_type": map_sql_type(col_type),
                "ordinal_position": i + 1,
            })

        dataflow = {
            "name": f"ingest_{table_name}",
            "stage": "source2bronze",
            "source": {
                "connection_name": connection_name,
                "table": table_name,
            },
            "destination": {
                "connection_name": f"{connection_name}_dest",
                "table": table_name,
                "load_type": "full_load",
            },
        }
        if schema_hints:
            dataflow["transform"] = {"schema_hints": schema_hints}
        dataflows.append(dataflow)

    return dataflows, []


# ---------------------------------------------------------------------------
# Manual columns
# ---------------------------------------------------------------------------


def parse_manual_columns(columns_str: str) -> list[dict]:
    """Parse col:type,col:type string into schema_hints."""
    schema_hints = []
    for i, pair in enumerate(columns_str.split(",")):
        pair = pair.strip()
        if ":" in pair:
            col_name, col_type = pair.split(":", 1)
        else:
            col_name, col_type = pair, "string"
        schema_hints.append({
            "column_name": col_name.strip(),
            "data_type": map_sql_type(col_type.strip()),
            "ordinal_position": i + 1,
        })
    return schema_hints


# ---------------------------------------------------------------------------
# Output builder
# ---------------------------------------------------------------------------


def build_metadata(
    connections: list[dict],
    dataflows: list[dict],
    dest_connection_name: str,
    dest_format: str,
) -> dict:
    """Build complete metadata dict with destination connection added."""
    # Add destination connection if not already present
    conn_names = {c["name"] for c in connections}
    if dest_connection_name not in conn_names:
        dest_connection_type = "lakehouse" if dest_format in ("delta", "iceberg") else "file"
        connections.append({
            "name": dest_connection_name,
            "connection_type": dest_connection_type,
            "format": dest_format,
            "configure": {
                "base_path": f"./data/output/{dest_format}",
            },
        })

    # Ensure all dataflows point to dest connection
    for df in dataflows:
        if df.get("destination", {}).get("connection_name", "").endswith("_dest"):
            df["destination"]["connection_name"] = dest_connection_name

    return {
        "connections": connections,
        "dataflows": dataflows,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Introspect data sources and generate DataCoolie metadata.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--mode", required=True, choices=["folder", "ddl", "manual"],
                        help="Introspection mode.")
    parser.add_argument("--path", type=Path, default=None,
                        help="Directory to scan (folder mode).")
    parser.add_argument("--connection-name", default="source",
                        help="Name for generated source connection.")
    parser.add_argument("--dest-connection", default=None,
                        help="Destination connection name (default: {connection-name}_dest).")
    parser.add_argument("--dest-format", default="delta", choices=["delta", "parquet", "iceberg"],
                        help="Destination format. Default: delta.")
    parser.add_argument("--ddl-file", type=Path, default=None,
                        help="SQL DDL file (ddl mode).")
    parser.add_argument("--columns", default=None,
                        help="Column definitions as col:type,... (manual mode).")
    parser.add_argument("--output", type=Path, default=None,
                        help="Output file path (default: stdout).")
    args = parser.parse_args()

    dest_conn = args.dest_connection or f"{args.connection_name}_dest"

    try:
        if args.mode == "folder":
            if not args.path:
                print("ERROR: --path is required for folder mode.", file=sys.stderr)
                sys.exit(2)
            if not args.path.is_dir():
                print(f"ERROR: Not a directory: {args.path}", file=sys.stderr)
                sys.exit(2)

            connections, dataflows, _ = scan_folder(args.path, args.connection_name)

        elif args.mode == "ddl":
            if not args.ddl_file:
                print("ERROR: --ddl-file is required for ddl mode.", file=sys.stderr)
                sys.exit(2)
            if not args.ddl_file.exists():
                print(f"ERROR: File not found: {args.ddl_file}", file=sys.stderr)
                sys.exit(2)

            ddl_text = args.ddl_file.read_text(encoding="utf-8")
            dataflows, _ = parse_ddl(ddl_text, args.connection_name)
            connections = [{
                "name": args.connection_name,
                "connection_type": "database",
                "format": "sql",
                "configure": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "mydb",
                },
            }]

        elif args.mode == "manual":
            if not args.columns:
                print("ERROR: --columns is required for manual mode.", file=sys.stderr)
                sys.exit(2)

            schema_hints = parse_manual_columns(args.columns)
            connections = [{
                "name": args.connection_name,
                "connection_type": "file",
                "format": "csv",
                "configure": {"base_path": "./data/input"},
            }]
            dataflows = [{
                "name": f"ingest_{args.connection_name}",
                "stage": "source2bronze",
                "source": {"connection_name": args.connection_name, "table": "manual"},
                "destination": {
                    "connection_name": dest_conn,
                    "table": "manual",
                    "load_type": "full_load",
                },
                "transform": {"schema_hints": schema_hints},
            }]

    except (ValueError, OSError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    # Build final metadata
    metadata = build_metadata(connections, dataflows, dest_conn, args.dest_format)

    # Output
    output_json = json.dumps(metadata, indent=2, ensure_ascii=False) + "\n"

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output_json, encoding="utf-8")
        print(f"✓ Generated metadata → {args.output}")
        print(f"  {len(metadata['connections'])} connections, {len(metadata['dataflows'])} dataflows")
    else:
        sys.stdout.write(output_json)

    sys.exit(0)


if __name__ == "__main__":
    main()
