"""introspect_files.py — File and folder introspection via fsspec + pyarrow.

Two subcommands:
  structure  — Recursively list a path, detect formats/partitioning, output Markdown.
  schema     — Extract column schema from a file or table, output CSV.

Usage:
    python introspect_files.py structure --path ./data/ --source files --output structure.md
    python introspect_files.py schema --path ./data/orders/ --format delta --source erp --table orders
    python introspect_files.py schema --path s3://bucket/sales.parquet --source warehouse --table sales --output schema.csv
"""
from __future__ import annotations

import argparse
import heapq
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path, PurePosixPath
from typing import Any

from _artifact_io import atomic_write_text
from _input_safety import validate_nonsecret_locator
from _observation_contract import (
    CSV_HEADER as CSV_HEADER,  # noqa: F401 - public cross-probe contract
    atomic_write_observations,
    make_observation,
    write_observations,
)
from _probe_status import PARTIAL_EXIT_CODE, write_probe_status

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

KNOWN_EXTENSIONS = {
    ".parquet", ".csv", ".json", ".jsonl", ".ndjson",
    ".avro", ".orc", ".xlsx", ".tsv",
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
    }
    return mapping.get(ext)


def _detect_delta(fs, path: str) -> bool:
    """Check if path contains a _delta_log directory."""
    try:
        return fs.isdir(path.rstrip("/") + "/_delta_log")
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


def _bounded_file_listing(
    fs,
    root: str,
    max_entries: int,
    max_depth: int,
) -> tuple[dict[str, dict], list[str]]:
    """List files deterministically while bounding directory entries and depth."""
    files: dict[str, dict] = {}
    issues: list[str] = []
    queue: list[tuple[int, str]] = [(0, root)]
    seen: set[str] = set()
    inspected = 0

    while queue:
        depth, current = heapq.heappop(queue)
        if current in seen:
            continue
        seen.add(current)
        entries = sorted(fs.ls(current, detail=True), key=lambda item: str(item.get("name", "")))
        for info in entries:
            inspected += 1
            if inspected > max_entries:
                issues.append(
                    f"Stopped after --max-entries={max_entries}; directory scope is partial"
                )
                return files, issues
            name = str(info.get("name", ""))
            if info.get("type") == "directory":
                if depth >= max_depth:
                    if not any("--max-depth" in issue for issue in issues):
                        issues.append(
                            f"Stopped below --max-depth={max_depth}; directory scope is partial"
                        )
                    continue
                heapq.heappush(queue, (depth + 1, name))
            else:
                files[name] = info
    return files, issues


def cmd_structure(
    path: str,
    output_path: str | None,
    storage_options: dict | None,
    max_entries: int = 10000,
    max_depth: int = 8,
) -> tuple[int, list[str]]:
    """Write a bounded Markdown layout summary for scratch evidence."""
    import fsspec

    validate_nonsecret_locator(path, "File path")
    so = storage_options or {}
    fs, root = fsspec.core.url_to_fs(path, **so)

    try:
        all_files, issues = _bounded_file_listing(fs, root, max_entries, max_depth)
    except Exception as exc:
        raise ValueError(f"Cannot list file scope ({type(exc).__name__})") from exc

    if not all_files:
        issues.append("No files found in the requested scope")

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
        atomic_write_text(Path(output_path), md_content)
    else:
        print(md_content)
    return len(all_files), issues


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


def _schema_parquet(fs, path: str) -> list[tuple[str, Any, bool]]:
    """Extract schema from Parquet file(s)."""
    import pyarrow.parquet as pq

    with fs.open(path, "rb") as handle:
        schema = pq.read_schema(handle)
    return [(field.name, field.type, field.nullable) for field in schema]


def _schema_csv(fs, path: str, sample_bytes: int = 1 << 20) -> list[tuple[str, Any]]:
    """Infer schema from CSV by reading first rows."""
    import pyarrow.csv as pcsv

    target = path
    with fs.open(target, "rb") as f:
        reader = pcsv.open_csv(
            f,
            read_options=pcsv.ReadOptions(block_size=sample_bytes),
            parse_options=pcsv.ParseOptions(
                delimiter="\t" if path.lower().endswith(".tsv") else ","
            ),
        )
        batch = next(reader)
    return [(field.name, field.type) for field in batch.schema]


def _schema_json(
    fs,
    path: str,
    sample_bytes: int = 1 << 20,
    sample_records: int = 100,
) -> list[tuple[str, Any]]:
    """Infer schema from a bounded sample of JSON objects."""
    import pyarrow as pa

    target = path
    with fs.open(target, "rb") as f:
        sample = f.read(sample_bytes)
    text_sample = sample.decode("utf-8")
    stripped = text_sample.lstrip()
    decoder = json.JSONDecoder()
    records: list[dict[str, Any]] = []
    if stripped.startswith("["):
        remainder = stripped[1:].lstrip()
        while remainder and len(records) < sample_records:
            try:
                value, end = decoder.raw_decode(remainder)
            except json.JSONDecodeError:
                break
            if isinstance(value, dict):
                records.append(value)
            remainder = remainder[end:].lstrip()
            if remainder.startswith(","):
                remainder = remainder[1:].lstrip()
            elif remainder.startswith("]"):
                break
    else:
        for line in text_sample.splitlines()[:sample_records]:
            line = line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                if not records:
                    value, _ = decoder.raw_decode(stripped)
                else:
                    break
            if isinstance(value, dict):
                records.append(value)
    if not records:
        raise ValueError("No complete JSON objects found within the bounded sample")
    table = pa.Table.from_pylist(records)
    return [(field.name, field.type) for field in table.schema]


def _schema_delta(
    path: str, storage_options: dict | None
) -> list[tuple[str, Any, bool]]:
    """Extract schema from a Delta table."""
    from deltalake import DeltaTable

    dt = DeltaTable(path, storage_options=storage_options)
    schema = dt.schema()
    fields = []
    for field in schema.fields:
        fields.append((field.name, field.type, field.nullable))
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


def _schema_avro(fs, path: str) -> list[tuple[str, Any, bool]]:
    """Extract schema from an Avro file."""
    import fastavro

    target = path
    with fs.open(target, "rb") as f:
        reader = fastavro.reader(f)
        schema = reader.writer_schema
    fields = []
    for field_def in schema.get("fields", []):
        field_type = field_def["type"]
        branches = field_type if isinstance(field_type, list) else [field_type]
        nullable = any(
            branch == "null"
            or isinstance(branch, dict)
            and branch.get("type") == "null"
            for branch in branches
        )
        fields.append((field_def["name"], field_type, nullable))
    return fields


def _map_avro_type(avro_type) -> tuple[str, str, str, str]:
    """Map an Avro type to (canonical, format, precision, scale)."""
    if isinstance(avro_type, str):
        mapping = {
            "null": "string",
            "boolean": "boolean",
            "int": "integer",
            "long": "long",
            "float": "float",
            "double": "double",
            "bytes": "binary",
            "string": "string",
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


def _schema_orc(fs, path: str) -> list[tuple[str, Any, bool]]:
    """Extract schema from ORC file(s)."""
    import pyarrow.orc as porc

    target = path
    with fs.open(target, "rb") as f:
        orc_file = porc.ORCFile(f)
        schema = orc_file.schema
    return [(field.name, field.type, field.nullable) for field in schema]


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
    sample_bytes: int = 1 << 20,
    sample_records: int = 100,
    *,
    max_entries: int = 10000,
    max_depth: int = 8,
    max_files: int = 1,
    issues: list[str] | None = None,
    details: dict | None = None,
) -> int:
    """Read bounded representative schemas; never infer dataset completeness from one file."""
    import fsspec

    validate_nonsecret_locator(path, "File path")
    problems = issues if issues is not None else []
    evidence = details if details is not None else {}
    so = storage_options or {}
    fs, resolved = fsspec.core.url_to_fs(path, **so)
    selected = [resolved]
    directory = fs.isdir(resolved)
    if fmt == "auto" and _detect_delta(fs, resolved):
        fmt = "delta"
    if directory and fmt != "delta":
        listing, limitations = _bounded_file_listing(
            fs, resolved, max_entries, max_depth
        )
        problems.extend(limitations)
        candidates = sorted(
            name for name in listing if _detect_format_from_path(name) is not None
        )
        excluded = sorted(set(listing) - set(candidates))
        evidence["excluded_files"] = excluded
        if excluded:
            problems.append(
                "Unrecognized files were excluded from directory schema coverage"
            )
        if fmt == "auto":
            fmt = _detect_format_from_path(candidates[0]) if candidates else "unknown"
        matching = [
            name for name in candidates if _detect_format_from_path(name) == fmt
        ]
        if len(matching) != len(candidates):
            problems.append(
                "Directory contains other formats outside the selected schema scope"
            )
        selected = matching[:max_files]
        if len(matching) > len(selected):
            problems.append("Unexamined files remain outside the bounded schema sample")
    elif fmt == "auto":
        fmt = _detect_format_from_path(resolved) or "unknown"
    if not selected:
        problems.append("No schema files found within the requested scope")
    if fmt in {"unknown", "iceberg"}:
        raise ValueError(
            "Unsupported file schema scope; select a format or lakehouse catalog probe"
        )
    evidence.update(
        {
            "selected_files": selected,
            "format": fmt,
            "schema_scope": "table"
            if fmt == "delta"
            else "directory_sample"
            if directory
            else "file",
            "bounds": {
                "max_entries": max_entries,
                "max_depth": max_depth,
                "max_files": max_files,
                "sample_bytes": sample_bytes,
                "sample_records": sample_records,
            },
            "listing_limitation": "Backend ls responses may be buffered before traversal limits apply",
        }
    )
    if output_path and fs.protocol in ("file", "local", ("file", "local")):
        destination = Path(output_path).resolve()
        source_path = Path(resolved).resolve()
        if (
            destination in {Path(name).resolve() for name in selected}
            or directory
            and destination.is_relative_to(source_path)
        ):
            raise ValueError("Schema output must be outside source files")
    representative = None
    for selected_path in selected:
        inferred = fmt in {"csv", "json", "excel"}
        if fmt == "parquet":
            fields = _schema_parquet(fs, selected_path)
        elif fmt == "delta":
            fields = _schema_delta(path, so or None)
        elif fmt == "avro":
            fields = _schema_avro(fs, selected_path)
        elif fmt == "orc":
            fields = _schema_orc(fs, selected_path)
        elif fmt == "csv":
            fields = [
                (name, typ, None)
                for name, typ in _schema_csv(fs, selected_path, sample_bytes)
            ]
        elif fmt == "json":
            fields = [
                (name, typ, None)
                for name, typ in _schema_json(
                    fs, selected_path, sample_bytes, sample_records
                )
            ]
        elif fmt == "excel":
            if fs.protocol not in ("file", "local", ("file", "local")):
                raise ValueError("Excel schema requires a local file")
            fields = [(name, typ, None) for name, typ in _schema_excel(selected_path)]
        else:
            raise ValueError("Unsupported schema format")
        observations = []
        for ordinal, (name, type_obj, nullable) in enumerate(fields, start=1):
            mapper = (
                _map_delta_type
                if fmt == "delta"
                else _map_avro_type
                if fmt == "avro"
                else _map_arrow_type
            )
            canonical, type_fmt, prec, scl = (
                (str(type_obj), "", "", "") if fmt == "excel" else mapper(type_obj)
            )
            observations.append(
                make_observation(
                    source=source,
                    object_type="file",
                    object=table,
                    column=name,
                    native_type=str(type_obj),
                    data_type=canonical,
                    format=type_fmt or fmt,
                    precision=prec,
                    scale=scl,
                    nullable="" if nullable is None else str(nullable).lower(),
                    ordinal=ordinal,
                    notes="inferred from sample rows; nullability unknown"
                    if inferred
                    else "",
                )
            )
        signature = [
            (r["column"], r["native_type"], r["nullable"]) for r in observations
        ]
        if representative is None:
            representative = observations
        elif signature != [
            (r["column"], r["native_type"], r["nullable"]) for r in representative
        ]:
            problems.append(
                "Selected files have conflicting name/type/nullability schemas"
            )
    observations = representative or []
    if not observations and selected:
        problems.append("No schema fields extracted from selected files")
    evidence["schema_complete"] = not problems
    if output_path:
        atomic_write_observations(Path(output_path), observations)
    else:
        write_observations(sys.stdout, observations)
    return len(observations)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        allow_abbrev=False,
        description="Introspect file sources — folder structure and schema extraction.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # structure
    p_struct = sub.add_parser(
        "structure",
        help="List folder structure as Markdown",
        allow_abbrev=False,
    )
    p_struct.add_argument(
        "--path", required=True, help="Root path (local, s3://, abfss://, gs://)"
    )
    p_struct.add_argument(
        "--source", required=True, help="Source name for status evidence"
    )
    p_struct.add_argument(
        "--output", default=None, help="Output .md file (default: stdout)"
    )
    p_struct.add_argument("--storage-options-env", default=None)
    p_struct.add_argument("--max-entries", type=int, default=10000)
    p_struct.add_argument("--max-depth", type=int, default=8)
    p_struct.add_argument("--status-output", default=None)

    # schema
    p_schema = sub.add_parser(
        "schema",
        help="Extract file schema as CSV",
        allow_abbrev=False,
    )
    p_schema.add_argument("--path", required=True, help="Path to file or directory")
    p_schema.add_argument(
        "--format",
        default="auto",
        choices=[
            "auto",
            "parquet",
            "csv",
            "json",
            "delta",
            "iceberg",
            "avro",
            "orc",
            "excel",
        ],
        help="File format (default: auto-detect)",
    )
    p_schema.add_argument("--source", required=True, help="Source name for CSV output")
    p_schema.add_argument("--table", required=True, help="Table name for CSV output")
    p_schema.add_argument(
        "--output", default=None, help="Output .csv file (default: stdout)"
    )
    p_schema.add_argument("--storage-options-env", default=None)
    p_schema.add_argument("--sample-bytes", type=int, default=1 << 20)
    p_schema.add_argument("--sample-records", type=int, default=100)
    p_schema.add_argument("--status-output", default=None)
    p_schema.add_argument("--max-entries", type=int, default=10000)
    p_schema.add_argument("--max-depth", type=int, default=8)
    p_schema.add_argument("--max-files", type=int, default=1)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    try:
        if (
            args.output
            and args.status_output
            and Path(args.output).resolve() == Path(args.status_output).resolve()
        ):
            raise ValueError(
                "Probe status output must be distinct from observation output"
            )
        validate_nonsecret_locator(args.path, "File path")
        if args.status_output and (
            "://" not in args.path or args.path.startswith(("file://", "local://"))
        ):
            import fsspec

            guard_fs, guard_root = fsspec.core.url_to_fs(args.path)
            if guard_fs.protocol in ("file", "local", ("file", "local")):
                input_path, status_path = (
                    Path(guard_root).resolve(),
                    Path(args.status_output).resolve(),
                )
                inside = input_path.is_dir() and status_path.is_relative_to(input_path)
                if (
                    status_path == input_path
                    or inside
                    and (args.command == "schema" or status_path.exists())
                ):
                    raise ValueError("Probe status output must be outside source files")
    except ValueError as exc:
        raise SystemExit(f"ERROR: {exc}") from exc

    so = None
    storage_options_env = getattr(args, "storage_options_env", None)
    if storage_options_env:
        raw = os.environ.get(storage_options_env)
        if raw is None:
            raise SystemExit(
                f"ERROR: Environment variable '{storage_options_env}' is not set"
            )
        try:
            so = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SystemExit(
                "ERROR: Storage options environment value is not valid JSON"
            ) from exc
        if not isinstance(so, dict):
            raise SystemExit(
                "ERROR: Storage options environment value must be a JSON object"
            )

    if args.command == "structure":
        if args.max_entries < 1 or args.max_entries > 1000000:
            raise SystemExit("ERROR: --max-entries must be between 1 and 1000000")
        if args.max_depth < 1 or args.max_depth > 64:
            raise SystemExit("ERROR: --max-depth must be between 1 and 64")
        try:
            row_count, issues = cmd_structure(
                args.path,
                args.output,
                so,
                args.max_entries,
                args.max_depth,
            )
        except (OSError, ValueError) as exc:
            raise SystemExit(f"ERROR: {exc}") from exc
        status = write_probe_status(
            Path(args.status_output) if args.status_output else None,
            source=args.source,
            probe="file-layout",
            row_count=row_count,
            issues=issues,
        )
        if status == "partial":
            raise SystemExit(PARTIAL_EXIT_CODE)
    elif args.command == "schema":
        if args.sample_bytes < 1024 or args.sample_bytes > 64 << 20:
            raise SystemExit("ERROR: --sample-bytes must be between 1024 and 67108864")
        if args.sample_records < 1 or args.sample_records > 10000:
            raise SystemExit("ERROR: --sample-records must be between 1 and 10000")
        if (
            not 1 <= args.max_entries <= 1000000
            or not 1 <= args.max_depth <= 64
            or not 1 <= args.max_files <= 10000
        ):
            raise SystemExit("ERROR: schema traversal/sample bounds out of range")
        issues, details = [], {}
        try:
            row_count = cmd_schema(
                args.path,
                args.format,
                args.source,
                args.table,
                args.output,
                so,
                args.sample_bytes,
                args.sample_records,
                max_entries=args.max_entries,
                max_depth=args.max_depth,
                max_files=args.max_files,
                issues=issues,
                details=details,
            )
        except (OSError, ValueError, ImportError) as exc:
            raise SystemExit(
                f"ERROR: File schema probe failed ({type(exc).__name__})"
            ) from exc
        status = write_probe_status(
            Path(args.status_output) if args.status_output else None,
            source=args.source,
            probe="file-schema",
            row_count=row_count,
            issues=issues,
            output_path=Path(args.output) if args.output else None,
            scope={
                "kind": "objects",
                "filters": {"path": args.path, "table": args.table},
                "objects": [[args.source, "file", "", "", args.table, ""]]
                if row_count
                else [],
            },
            details=details,
        )
        if status == "partial":
            raise SystemExit(PARTIAL_EXIT_CODE)


if __name__ == "__main__":
    main()
