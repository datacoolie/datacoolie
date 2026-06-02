"""introspect_files.py — File and folder introspection via fsspec + pyarrow.

Two subcommands:
  structure  — Recursively list a path, detect formats/partitioning, output Markdown.
  schema     — Extract column schema from a file or table, output CSV.

Usage:
    python introspect_files.py structure --path ./data/ --output structure.md
    python introspect_files.py schema --path ./data/orders/ --format delta --source erp --table orders
    python introspect_files.py schema --path s3://bucket/sales.parquet --source warehouse --table sales --output schema.csv
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from pathlib import PurePosixPath
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CSV_HEADER = [
    "source", "schema", "table", "column", "type", "format",
    "precision", "scale", "nullable", "pk", "fk",
    "ordinal_position", "row_estimate", "notes",
]

KNOWN_EXTENSIONS = {
    ".parquet", ".csv", ".json", ".jsonl", ".ndjson",
    ".avro", ".orc", ".xlsx", ".xls", ".tsv",
}

# ---------------------------------------------------------------------------
# Arrow type mapping
# ---------------------------------------------------------------------------

def _map_arrow_type(arrow_type) -> tuple[str, str, str, str]:
    """Map a PyArrow type to (canonical, format, precision, scale)."""
    import pyarrow as pa

    t = arrow_type

    # Null
    if pa.types.is_null(t):
        return "string", "", "", ""

    # Boolean
    if pa.types.is_boolean(t):
        return "boolean", "", "", ""

    # Integer types
    if pa.types.is_int8(t):
        return "byte", "", "", ""
    if pa.types.is_int16(t):
        return "short", "", "", ""
    if pa.types.is_int32(t):
        return "integer", "", "", ""
    if pa.types.is_int64(t):
        return "long", "", "", ""
    if pa.types.is_uint8(t):
        return "short", "", "", ""
    if pa.types.is_uint16(t):
        return "integer", "", "", ""
    if pa.types.is_uint32(t):
        return "long", "", "", ""
    if pa.types.is_uint64(t):
        return "long", "", "", ""

    # Float types
    if pa.types.is_float16(t) or pa.types.is_float32(t):
        return "float", "", "", ""
    if pa.types.is_float64(t):
        return "double", "", "", ""

    # Decimal
    if pa.types.is_decimal(t):
        p = t.precision
        s = t.scale
        return f"decimal({p},{s})", "", str(p), str(s)

    # String / large string / utf8
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "string", "", "", ""

    # Binary
    if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t):
        size = getattr(t, "byte_width", None)
        prec = str(size) if size and size > 0 else ""
        return "binary", "", prec, ""

    # Date
    if pa.types.is_date(t):
        return "date", "", "", ""

    # Timestamp
    if pa.types.is_timestamp(t):
        tz = t.tz
        return ("timestamp_tz" if tz else "timestamp"), "", "", ""

    # Time
    if pa.types.is_time(t):
        return "time", "", "", ""

    # Duration
    if pa.types.is_duration(t):
        return "long", "duration", "", ""

    # List / large list
    if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
        return "array", str(t), "", ""

    # Struct
    if pa.types.is_struct(t):
        return "struct", str(t), "", ""

    # Map
    if pa.types.is_map(t):
        return "struct", str(t), "", ""

    # Dictionary (categorical)
    if pa.types.is_dictionary(t):
        return _map_arrow_type(t.value_type)

    # Fallback
    return str(t), str(t), "", ""


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------

def _detect_format_from_path(path: str) -> str | None:
    """Detect file format from extension."""
    ext = PurePosixPath(path).suffix.lower()
    mapping = {
        ".parquet": "parquet",
        ".csv": "csv",
        ".tsv": "csv",
        ".json": "json",
        ".jsonl": "json",
        ".ndjson": "json",
        ".avro": "avro",
        ".orc": "orc",
        ".xlsx": "excel",
        ".xls": "excel",
    }
    return mapping.get(ext)


def _detect_delta(fs, path: str) -> bool:
    """Check if path contains a _delta_log directory."""
    try:
        entries = fs.ls(path, detail=False)
        return any(e.rstrip("/").endswith("_delta_log") for e in entries)
    except Exception:
        return False


def _detect_hive_partitions(fs, path: str) -> list[str]:
    """Detect Hive-style partition columns (key=value dirs)."""
    partition_re = re.compile(r"^([^=]+)=.+$")
    cols: list[str] = []
    try:
        entries = fs.ls(path, detail=True)
        for entry in entries:
            if entry.get("type") == "directory":
                name = PurePosixPath(entry["name"]).name
                m = partition_re.match(name)
                if m and m.group(1) not in cols:
                    cols.append(m.group(1))
    except Exception:
        pass
    return cols


# ---------------------------------------------------------------------------
# Subcommand: structure
# ---------------------------------------------------------------------------

def _human_size(nbytes: int | float) -> str:
    """Convert bytes to human-readable size."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def cmd_structure(path: str, output_path: str | None, storage_options: dict | None) -> None:
    """Recursively list a path and output a Markdown structure report."""
    import fsspec

    so = storage_options or {}
    fs, root = fsspec.core.url_to_fs(path, **so)

    # Collect all entries
    try:
        all_files = fs.find(root, detail=True)
    except Exception as exc:
        print(f"ERROR: Cannot list {path}: {exc}", file=sys.stderr)
        sys.exit(1)

    if not all_files:
        print(f"WARNING: No files found at {path}", file=sys.stderr)
        return

    # Group by top-level directory relative to root
    groups: dict[str, dict] = defaultdict(lambda: {
        "files": [], "total_size": 0, "formats": set(),
        "is_delta": False, "hive_cols": [], "detected_as": "plain",
    })

    for fpath, info in all_files.items():
        if info.get("type") == "directory":
            continue
        rel = fpath[len(root):].lstrip("/")
        parts = rel.split("/")
        top_dir = parts[0] if len(parts) > 1 else "."
        size = info.get("size", 0) or 0

        groups[top_dir]["files"].append(rel)
        groups[top_dir]["total_size"] += size

        ext = PurePosixPath(fpath).suffix.lower()
        if ext in KNOWN_EXTENSIONS:
            groups[top_dir]["formats"].add(ext.lstrip("."))

        # Skip internal files
        if "_delta_log" in rel:
            groups[top_dir]["is_delta"] = True
            groups[top_dir]["detected_as"] = "Delta table"

    # Detect hive partitioning per group
    for group_name, gdata in groups.items():
        group_path = f"{root}/{group_name}" if group_name != "." else root
        hive_cols = _detect_hive_partitions(fs, group_path)
        if hive_cols:
            gdata["hive_cols"] = hive_cols
            if gdata["detected_as"] == "plain":
                gdata["detected_as"] = "Hive partitioned"

        # Delta detection for group
        if not gdata["is_delta"]:
            if _detect_delta(fs, group_path):
                gdata["is_delta"] = True
                gdata["detected_as"] = "Delta table"

    # Build simple tree (2 levels deep)
    tree_lines: list[str] = []
    root_name = PurePosixPath(root).name or root
    tree_lines.append(f"{root_name}/")
    group_names = sorted(groups.keys())
    for i, gname in enumerate(group_names):
        gdata = groups[gname]
        is_last = i == len(group_names) - 1
        prefix = "└── " if is_last else "├── "
        file_count = len([f for f in gdata["files"] if "_delta_log" not in f])
        fmts = ", ".join(sorted(gdata["formats"])) if gdata["formats"] else "unknown"
        size_str = _human_size(gdata["total_size"])
        suffix_parts = []
        if gdata["is_delta"]:
            suffix_parts.append("Delta table detected")
        if gdata["hive_cols"]:
            suffix_parts.append(f"partitioned by: {', '.join(gdata['hive_cols'])}")
        suffix = f"  ← {'; '.join(suffix_parts)}" if suffix_parts else ""
        tree_lines.append(f"{prefix}{gname}/  ({file_count} files, {size_str}, {fmts}){suffix}")

    # Summary table
    summary_rows: list[list[str]] = []
    for gname in group_names:
        gdata = groups[gname]
        file_count = len([f for f in gdata["files"] if "_delta_log" not in f])
        fmts = ", ".join(sorted(gdata["formats"])) if gdata["formats"] else "unknown"
        partitioned = "yes" if gdata["hive_cols"] else "no"
        part_cols = ", ".join(gdata["hive_cols"]) if gdata["hive_cols"] else "—"
        summary_rows.append([
            gname + "/", fmts, partitioned, part_cols,
            str(file_count), _human_size(gdata["total_size"]),
            gdata["detected_as"],
        ])

    # Build markdown
    protocol = path.split("://")[0] if "://" in path else "local"
    md_lines = [
        f"# Folder Structure — {path}",
        "",
        f"**Platform:** {protocol}",
        f"**Root:** {path}",
        f"**Scanned:** {_today()}",
        "",
        "## Tree",
        "",
        "```",
        *tree_lines,
        "```",
        "",
        "## Summary",
        "",
        "| Path | Format | Partitioned | Partition Columns | File Count | Total Size | Detected As |",
        "|---|---|---|---|---|---|---|",
    ]
    for row in summary_rows:
        md_lines.append("| " + " | ".join(row) + " |")

    md_content = "\n".join(md_lines) + "\n"

    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(md_content)
    else:
        print(md_content)


def _today() -> str:
    from datetime import date
    return date.today().isoformat()


# ---------------------------------------------------------------------------
# Subcommand: schema
# ---------------------------------------------------------------------------

def _detect_format(fs, path: str) -> str:
    """Auto-detect the file format at a path."""
    if _detect_delta(fs, path):
        return "delta"
    # Check for Iceberg metadata
    try:
        entries = fs.ls(path, detail=False)
        for e in entries:
            if "metadata" in e and e.endswith(".metadata.json"):
                return "iceberg"
    except Exception:
        pass
    # Check individual file extension
    try:
        files = fs.glob(path.rstrip("/") + "/*")
        for f in files:
            detected = _detect_format_from_path(f)
            if detected:
                return detected
    except Exception:
        pass
    # Single file
    detected = _detect_format_from_path(path)
    if detected:
        return detected
    return "unknown"


def _schema_parquet(fs, path: str) -> list[tuple[str, Any]]:
    """Extract schema from Parquet file(s)."""
    import pyarrow.parquet as pq
    # Find a parquet file
    target = path
    try:
        if fs.isdir(path):
            files = [f for f in fs.glob(path.rstrip("/") + "/**/*.parquet") or fs.glob(path.rstrip("/") + "/*.parquet")]
            if files:
                target = files[0]
    except Exception:
        pass
    with fs.open(target, "rb") as f:
        schema = pq.read_schema(f)
    return [(field.name, field.type) for field in schema]


def _schema_csv(fs, path: str) -> list[tuple[str, Any]]:
    """Infer schema from CSV by reading first rows."""
    import pyarrow.csv as pcsv
    target = path
    try:
        if fs.isdir(path):
            files = fs.glob(path.rstrip("/") + "/*.csv") + fs.glob(path.rstrip("/") + "/*.tsv")
            if files:
                target = files[0]
    except Exception:
        pass
    with fs.open(target, "rb") as f:
        reader = pcsv.open_csv(f, read_options=pcsv.ReadOptions(block_size=1 << 20))
        batch = next(reader)
    return [(field.name, field.type) for field in batch.schema]


def _schema_json(fs, path: str) -> list[tuple[str, Any]]:
    """Infer schema from JSON/JSONL by reading first rows."""
    import pyarrow.json as pjson
    target = path
    try:
        if fs.isdir(path):
            files = (
                fs.glob(path.rstrip("/") + "/*.json")
                + fs.glob(path.rstrip("/") + "/*.jsonl")
                + fs.glob(path.rstrip("/") + "/*.ndjson")
            )
            if files:
                target = files[0]
    except Exception:
        pass
    with fs.open(target, "rb") as f:
        table = pjson.read_json(f)
    return [(field.name, field.type) for field in table.schema]


def _schema_delta(path: str, storage_options: dict | None) -> list[tuple[str, Any]]:
    """Extract schema from a Delta table."""
    from deltalake import DeltaTable
    dt = DeltaTable(path, storage_options=storage_options)
    schema = dt.schema()
    fields = []
    for field in schema.fields:
        fields.append((field.name, field.type))
    return fields


def _map_delta_type(dt_type) -> tuple[str, str, str, str]:
    """Map a deltalake Field type to (canonical, format, precision, scale)."""
    # PrimitiveType has a .type attribute with the plain string
    type_str = getattr(dt_type, "type", str(dt_type)).lower()

    if type_str == "boolean":
        return "boolean", "", "", ""
    if type_str == "byte":
        return "byte", "", "", ""
    if type_str in ("short", "smallint"):
        return "short", "", "", ""
    if type_str in ("integer", "int"):
        return "integer", "", "", ""
    if type_str in ("long", "bigint"):
        return "long", "", "", ""
    if type_str == "float":
        return "float", "", "", ""
    if type_str == "double":
        return "double", "", "", ""
    if type_str == "string":
        return "string", "", "", ""
    if type_str == "binary":
        return "binary", "", "", ""
    if type_str == "date":
        return "date", "", "", ""
    if type_str == "timestamp" or type_str == "timestamp_ntz":
        return "timestamp", "", "", ""
    if type_str == "timestamp_tz":
        return "timestamp_tz", "", "", ""

    # decimal(p,s)
    m = re.match(r"decimal\((\d+),\s*(\d+)\)", type_str)
    if m:
        p, s = m.group(1), m.group(2)
        return f"decimal({p},{s})", "", p, s

    # array / struct / map
    if type_str.startswith("array"):
        return "array", type_str, "", ""
    if type_str.startswith("struct"):
        return "struct", type_str, "", ""
    if type_str.startswith("map"):
        return "struct", type_str, "", ""

    return type_str, type_str, "", ""


def _schema_avro(fs, path: str) -> list[tuple[str, Any]]:
    """Extract schema from an Avro file."""
    import fastavro
    target = path
    try:
        if fs.isdir(path):
            files = fs.glob(path.rstrip("/") + "/*.avro")
            if files:
                target = files[0]
    except Exception:
        pass
    with fs.open(target, "rb") as f:
        reader = fastavro.reader(f)
        schema = reader.writer_schema
    fields = []
    for field_def in schema.get("fields", []):
        fields.append((field_def["name"], field_def["type"]))
    return fields


def _map_avro_type(avro_type) -> tuple[str, str, str, str]:
    """Map an Avro type to (canonical, format, precision, scale)."""
    if isinstance(avro_type, str):
        mapping = {
            "null": "string", "boolean": "boolean", "int": "integer",
            "long": "long", "float": "float", "double": "double",
            "bytes": "binary", "string": "string",
        }
        return mapping.get(avro_type, avro_type), "", "", ""

    if isinstance(avro_type, list):
        # Union — pick the non-null type
        non_null = [t for t in avro_type if t != "null"]
        if non_null:
            return _map_avro_type(non_null[0])
        return "string", "", "", ""

    if isinstance(avro_type, dict):
        logical = avro_type.get("logicalType", "")
        base = avro_type.get("type", "")
        if logical == "decimal":
            p = avro_type.get("precision", 38)
            s = avro_type.get("scale", 0)
            return f"decimal({p},{s})", "", str(p), str(s)
        if logical == "date":
            return "date", "", "", ""
        if logical in ("timestamp-millis", "timestamp-micros"):
            return "timestamp", "", "", ""
        if logical in ("time-millis", "time-micros"):
            return "time", "", "", ""
        if logical == "uuid":
            return "string", "uuid", "", ""
        if base == "array":
            return "array", str(avro_type), "", ""
        if base == "record":
            return "struct", str(avro_type.get("name", "record")), "", ""
        if base == "map":
            return "struct", str(avro_type), "", ""
        if base == "enum":
            return "string", "enum", "", ""
        if base == "fixed":
            size = avro_type.get("size", "")
            return "binary", "", str(size) if size else "", ""
        return _map_avro_type(base)

    return str(avro_type), str(avro_type), "", ""


def _schema_orc(fs, path: str) -> list[tuple[str, Any]]:
    """Extract schema from ORC file(s)."""
    import pyarrow.orc as porc
    target = path
    try:
        if fs.isdir(path):
            files = fs.glob(path.rstrip("/") + "/*.orc")
            if files:
                target = files[0]
    except Exception:
        pass
    with fs.open(target, "rb") as f:
        orc_file = porc.ORCFile(f)
        schema = orc_file.schema
    return [(field.name, field.type) for field in schema]


def _schema_excel(path: str) -> list[tuple[str, str]]:
    """Extract column names from an Excel file header row."""
    import openpyxl
    wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(max_row=2, values_only=True))
    wb.close()
    if not rows:
        return []
    headers = rows[0]
    # Try to infer types from second row
    sample = rows[1] if len(rows) > 1 else [None] * len(headers)
    fields = []
    for h, val in zip(headers, sample):
        name = str(h) if h is not None else f"col_{len(fields)}"
        if isinstance(val, bool):
            typ = "boolean"
        elif isinstance(val, int):
            typ = "integer"
        elif isinstance(val, float):
            typ = "double"
        else:
            typ = "string"
        fields.append((name, typ))
    return fields


def cmd_schema(
    path: str,
    fmt: str,
    source: str,
    table: str,
    output_path: str | None,
    storage_options: dict | None,
) -> None:
    """Extract schema from files and output CSV."""
    import fsspec

    so = storage_options or {}
    fs, resolved = fsspec.core.url_to_fs(path, **so)

    # Detect format
    if fmt == "auto":
        fmt = _detect_format(fs, resolved)
        if fmt == "unknown":
            print(f"ERROR: Cannot detect format at {path}. Use --format.", file=sys.stderr)
            sys.exit(1)

    # Extract fields as list of (name, type_obj)
    notes_suffix = ""
    fields: list[tuple[str, Any]] = []
    is_delta = fmt == "delta"
    is_avro = fmt == "avro"
    is_excel = fmt == "excel"

    try:
        if fmt == "parquet":
            fields = _schema_parquet(fs, resolved)
        elif fmt == "csv":
            fields = _schema_csv(fs, resolved)
            notes_suffix = "inferred from sample rows"
        elif fmt == "json":
            fields = _schema_json(fs, resolved)
            notes_suffix = "inferred from sample rows"
        elif fmt == "delta":
            fields = _schema_delta(path, so or None)
        elif fmt == "avro":
            fields = _schema_avro(fs, resolved)
        elif fmt == "orc":
            fields = _schema_orc(fs, resolved)
        elif fmt == "excel":
            # Excel needs local path
            fields = _schema_excel(resolved)
        elif fmt == "iceberg":
            print("ERROR: Iceberg schema extraction requires catalog access. Use AI-native fallback.", file=sys.stderr)
            sys.exit(1)
        else:
            print(f"ERROR: Unsupported format '{fmt}'. Use --format.", file=sys.stderr)
            sys.exit(1)
    except ImportError as exc:
        lib = str(exc).split("'")[1] if "'" in str(exc) else str(exc)
        print(f"ERROR: Install '{lib}' for {fmt} support: pip install {lib}", file=sys.stderr)
        sys.exit(1)

    # Write CSV
    out_file = open(output_path, "w", newline="", encoding="utf-8") if output_path else sys.stdout
    try:
        writer = csv.writer(out_file, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(CSV_HEADER)

        for ordinal, (name, type_obj) in enumerate(fields, start=1):
            if is_delta:
                canonical, type_fmt, prec, scl = _map_delta_type(type_obj)
            elif is_avro:
                canonical, type_fmt, prec, scl = _map_avro_type(type_obj)
            elif is_excel:
                canonical, type_fmt, prec, scl = str(type_obj), "", "", ""
            else:
                canonical, type_fmt, prec, scl = _map_arrow_type(type_obj)

            notes = notes_suffix
            if not type_fmt:
                type_fmt = fmt

            writer.writerow([
                source, "", table, name,
                canonical, type_fmt, prec, scl,
                "true",  # files generally lack nullable info — default true
                "", "",  # no PK/FK for files
                ordinal, "",
                notes,
            ])
    finally:
        if output_path and out_file is not sys.stdout:
            out_file.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Introspect file sources — folder structure and schema extraction.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # structure
    p_struct = sub.add_parser("structure", help="List folder structure as Markdown")
    p_struct.add_argument("--path", required=True, help="Root path (local, s3://, abfss://, gs://)")
    p_struct.add_argument("--output", default=None, help="Output .md file (default: stdout)")
    p_struct.add_argument("--storage-options", default=None, help="JSON string of storage options")

    # schema
    p_schema = sub.add_parser("schema", help="Extract file schema as CSV")
    p_schema.add_argument("--path", required=True, help="Path to file or directory")
    p_schema.add_argument("--format", default="auto", choices=[
        "auto", "parquet", "csv", "json", "delta", "iceberg", "avro", "orc", "excel",
    ], help="File format (default: auto-detect)")
    p_schema.add_argument("--source", required=True, help="Source name for CSV output")
    p_schema.add_argument("--table", required=True, help="Table name for CSV output")
    p_schema.add_argument("--output", default=None, help="Output .csv file (default: stdout)")
    p_schema.add_argument("--storage-options", default=None, help="JSON string of storage options")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    # Parse storage options
    so = None
    if getattr(args, "storage_options", None):
        import json
        so = json.loads(args.storage_options)

    if args.command == "structure":
        cmd_structure(args.path, args.output, so)
    elif args.command == "schema":
        cmd_schema(args.path, args.format, args.source, args.table, args.output, so)


if __name__ == "__main__":
    main()
