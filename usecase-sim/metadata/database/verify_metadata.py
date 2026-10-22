"""Verify that metadata in the database matches the source JSON file.

Compares connections, dataflows, and schema hints field by field.

Usage (from the datacoolie/ directory):
    python usecase-sim/metadata/database/verify_metadata.py \\
        --json-path usecase-sim/scenarios/workflows/local_use_cases.json \\
        --connection-string "postgresql+psycopg2://datacoolie:datacoolie@localhost:5432/datacoolie" \\
        --workspace-id local-workspace
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from runner.runtime_paths import DATACOOLIE_ROOT, USECASE_SIM_DIR as SIM_ROOT, sqlite_metadata_url
from runner.metadata_documents import load_metadata_files

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, text

from datacoolie.utils.identity import (
    name_to_uuid as _name_to_uuid,
    normalize_workspace_id,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("verify_metadata")


# Column names that collide with reserved words in one or more target dialects.
_RESERVED_COLS = {"format", "database", "precision"}


def quote_col(name: str, dialect: str) -> str:
    """Quote column *name* when it is a reserved word in *dialect*."""
    if name not in _RESERVED_COLS:
        return name
    if dialect == "mysql":
        return f"`{name}`"
    if dialect == "mssql":
        return f"[{name}]"
    if dialect == "oracle":
        # Oracle up-folds unquoted identifiers; quoting lowercase would break
        # case-matching. FORMAT/DATABASE aren't reserved in Oracle, and
        # PRECISION is accepted as a DML reference.
        return name
    return f'"{name}"'  # postgresql, sqlite


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load(v: Any) -> Any:
    """Parse a JSON string into a Python object; return as-is if already parsed."""
    if isinstance(v, str):
        try:
            return json.loads(v)
        except (ValueError, TypeError):
            return v
    return v


def _norm(v: Any) -> Any:
    """Normalise a value for comparison: parse JSON strings, sort list keys."""
    v = _load(v)
    if isinstance(v, list):
        # Sort lists of dicts by their repr so order doesn't matter
        try:
            return sorted(v, key=lambda x: json.dumps(x, sort_keys=True) if isinstance(x, dict) else str(x))
        except Exception:
            return v
    if isinstance(v, dict):
        return {k: _norm(val) for k, val in sorted(v.items())}
    return v


def _diff(label: str, expected: Any, actual: Any) -> List[str]:
    """Return a list of difference messages, empty if equal."""
    e = _norm(expected)
    a = _norm(actual)
    if e == a:
        return []
    return [f"  {label}: expected={e!r}, actual={a!r}"]


@dataclass
class Report:
    mismatches: List[str] = field(default_factory=list)
    missing_in_db: List[str] = field(default_factory=list)
    extra_in_db: List[str] = field(default_factory=list)

    def ok(self) -> bool:
        return not (self.mismatches or self.missing_in_db or self.extra_in_db)


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------

def _expected_connections(meta: dict, workspace_id: str) -> Dict[str, dict]:
    """Build expected connection rows from JSON, keyed by ID and workspace."""
    out = {}
    for c in meta.get("connections", []):
        if normalize_workspace_id(c.get("workspace_id")) not in (None, workspace_id):
            continue
        connection_id = str(c.get("connection_id") or _name_to_uuid(
                c["name"], c.get("workspace_id") or workspace_id
            ))
        out[connection_id] = {
            "name": c["name"],
            "connection_id": connection_id,
            "connection_type": c.get("connection_type"),
            "format": c.get("format", "") or "",
            "catalog": c.get("catalog"),
            "database": c.get("database"),
            "configure": _load(c.get("configure")),
            "is_active": c.get("is_active", True),
        }
    return out


def _actual_connections(engine, workspace_id: str) -> Dict[str, dict]:
    d = engine.dialect.name
    q = lambda c: quote_col(c, d)  # noqa: E731
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT name, connection_id, connection_type, {q('format')}, catalog, {q('database')}, "
                "configure, is_active FROM dc_framework_connections "
                "WHERE workspace_id = :ws"
            ),
            {"ws": workspace_id},
        ).mappings().all()
    return {
        str(r["connection_id"]): {
            "name": r["name"],
            "connection_id": str(r["connection_id"]),
            "connection_type": r["connection_type"],
            "format": r["format"] or "",
            "catalog": r["catalog"],
            "database": r["database"],
            "configure": _load(r["configure"]),
            "is_active": bool(r["is_active"]),
        }
        for r in rows
    }


def verify_connections(meta: dict, engine, workspace_id: str) -> Report:
    report = Report()
    expected = _expected_connections(meta, workspace_id)
    actual   = _actual_connections(engine, workspace_id)

    for connection_id in sorted(expected):
        if connection_id not in actual:
            report.missing_in_db.append(
                f"connection '{expected[connection_id]['name']}' ({connection_id})"
            )
            continue
        exp = expected[connection_id]
        act = actual[connection_id]
        diffs = []
        for f_name in ("name", "connection_id", "connection_type", "format", "catalog", "database",
                       "configure", "is_active"):
            diffs += _diff(f_name, exp[f_name], act[f_name])
        if diffs:
            report.mismatches.append(
                f"connection '{exp['name']}' ({connection_id}):\n" + "\n".join(diffs)
            )

    for connection_id in sorted(set(actual) - set(expected)):
        report.extra_in_db.append(
            f"connection '{actual[connection_id]['name']}' ({connection_id})"
        )

    return report


# ---------------------------------------------------------------------------
# Dataflows
# ---------------------------------------------------------------------------

def _build_dst_configure(dst: dict, d: dict) -> Any:
    """Mirror the scripts/bootstrap/setup_metadata.py seeding logic for destination_configure."""
    dst_configure = dst.get("configure") or d.get("destination_configure") or {}
    if isinstance(dst_configure, str):
        try:
            dst_configure = json.loads(dst_configure)
        except (ValueError, TypeError):
            dst_configure = {}
    if not isinstance(dst_configure, dict):
        dst_configure = {}
    dst_partitions = dst.get("partition_columns") or d.get("destination_partition_columns")
    if dst_partitions and "partition_columns" not in dst_configure:
        dst_configure = dict(dst_configure)
        dst_configure["partition_columns"] = dst_partitions
    return dst_configure or None


def _connection_id(meta: dict, endpoint: dict, dataflow: dict, role: str, workspace_id: str) -> str:
    """Resolve an authored endpoint reference using the bound workspace."""
    explicit = endpoint.get("connection_id") or dataflow.get(f"{role}_connection_id")
    inline = endpoint.get("connection")
    if isinstance(inline, dict):
        explicit = explicit or inline.get("connection_id")
        reference = inline.get("name")
    else:
        reference = endpoint.get("connection_name") or dataflow.get(f"{role}_connection")
    if explicit:
        return str(explicit)
    connections = meta.get("connections", [])
    candidates = [
        row for row in connections
        if row.get("name") == reference
        and (normalize_workspace_id(row.get("workspace_id")) or workspace_id) == workspace_id
    ]
    if len(candidates) == 1:
        row = candidates[0]
        return str(row.get("connection_id") or _name_to_uuid(row["name"], workspace_id))
    return str(_name_to_uuid(reference or "", workspace_id))


def _expected_dataflows(meta: dict, workspace_id: str) -> Dict[str, dict]:
    out = {}
    for d in meta.get("dataflows", []):
        if normalize_workspace_id(d.get("workspace_id")) not in (None, workspace_id):
            continue
        src = d.get("source", {})
        dst = d.get("destination", {})
        src_cid = str(
            d.get("source_connection_id")
            or _connection_id(meta, src, d, "source", workspace_id)
        )
        dst_cid = str(
            d.get("destination_connection_id")
            or _connection_id(meta, dst, d, "destination", workspace_id)
        )
        dataflow_id = str(d.get("dataflow_id") or _name_to_uuid(
                d["name"], d.get("workspace_id") or workspace_id
            ))
        out[dataflow_id] = {
            "name": d["name"],
            "dataflow_id": dataflow_id,
            "stage": d.get("stage"),
            "group_number": d.get("group_number"),
            "execution_order": d.get("execution_order"),
            "processing_mode": d.get("processing_mode"),
            "source_connection_id": src_cid,
            "source_schema": src.get("schema") or src.get("schema_name") or d.get("source_schema"),
            "source_table": src.get("table") or d.get("source_table"),
            "source_query": src.get("query") or d.get("source_query"),
            "source_python_function": src.get("python_function") or d.get("source_python_function"),
            "source_filter_expression": src["filter_expression"] if "filter_expression" in src else d.get("source_filter_expression"),
            "source_watermark_columns": _load(src.get("watermark_columns") or d.get("source_watermark_columns")),
            "source_configure": _load(src.get("configure") or d.get("source_configure")),
            "transform": _load(d.get("transform")),
            "destination_connection_id": dst_cid,
            "destination_schema": dst.get("schema") or dst.get("schema_name") or d.get("destination_schema"),
            "destination_table": dst.get("table") or d.get("destination_table"),
            "destination_load_type": dst.get("load_type") or d.get("destination_load_type"),
            "destination_merge_keys": _load(dst.get("merge_keys") or d.get("destination_merge_keys")),
            "destination_configure": _load(_build_dst_configure(dst, d)),
            "configure": _load(d.get("configure")),
            "is_active": d.get("is_active", True),
        }
    return out


def _actual_dataflows(engine, workspace_id: str) -> Dict[str, dict]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT name, dataflow_id, stage, group_number, execution_order, processing_mode, "
                "source_connection_id, source_schema, source_table, source_query, "
                "source_python_function, source_filter_expression, source_watermark_columns, source_configure, "
                "transform, "
                "destination_connection_id, destination_schema, destination_table, "
                "destination_load_type, destination_merge_keys, destination_configure, "
                "configure, is_active "
                "FROM dc_framework_dataflows WHERE workspace_id = :ws"
            ),
            {"ws": workspace_id},
        ).mappings().all()
    return {
        str(r["dataflow_id"]): {
            "name": r["name"],
            "dataflow_id": str(r["dataflow_id"]),
            "stage": r["stage"],
            "group_number": r["group_number"],
            "execution_order": r["execution_order"],
            "processing_mode": r["processing_mode"],
            "source_connection_id": str(r["source_connection_id"]) if r["source_connection_id"] else None,
            "source_schema": r["source_schema"],
            "source_table": r["source_table"],
            "source_query": r["source_query"],
            "source_python_function": r["source_python_function"],
            "source_filter_expression": r["source_filter_expression"],
            "source_watermark_columns": _load(r["source_watermark_columns"]),
            "source_configure": _load(r["source_configure"]),
            "transform": _load(r["transform"]),
            "destination_connection_id": str(r["destination_connection_id"]) if r["destination_connection_id"] else None,
            "destination_schema": r["destination_schema"],
            "destination_table": r["destination_table"],
            "destination_load_type": r["destination_load_type"],
            "destination_merge_keys": _load(r["destination_merge_keys"]),
            "destination_configure": _load(r["destination_configure"]),
            "configure": _load(r["configure"]),
            "is_active": bool(r["is_active"]),
        }
        for r in rows
    }


def verify_dataflows(meta: dict, engine, workspace_id: str) -> Report:
    report = Report()
    expected = _expected_dataflows(meta, workspace_id)
    actual   = _actual_dataflows(engine, workspace_id)

    _FIELDS = [
        "name", "dataflow_id", "stage", "group_number", "execution_order", "processing_mode",
        "source_connection_id", "source_schema", "source_table", "source_query",
        "source_python_function", "source_filter_expression", "source_watermark_columns", "source_configure",
        "transform",
        "destination_connection_id", "destination_schema", "destination_table",
        "destination_load_type", "destination_merge_keys", "destination_configure",
        "configure", "is_active",
    ]

    for dataflow_id in sorted(expected):
        if dataflow_id not in actual:
            report.missing_in_db.append(
                f"dataflow '{expected[dataflow_id]['name']}' ({dataflow_id})"
            )
            continue
        exp = expected[dataflow_id]
        act = actual[dataflow_id]
        diffs = []
        for f_name in _FIELDS:
            diffs += _diff(f_name, exp[f_name], act[f_name])
        if diffs:
            report.mismatches.append(
                f"dataflow '{exp['name']}' ({dataflow_id}):\n" + "\n".join(diffs)
            )

    for dataflow_id in sorted(set(actual) - set(expected)):
        report.extra_in_db.append(
            f"dataflow '{actual[dataflow_id]['name']}' ({dataflow_id})"
        )

    return report


# ---------------------------------------------------------------------------
# Schema hints
# ---------------------------------------------------------------------------

def _hint_key(
    connection_id: str,
    schema_name: object,
    table_name: object,
    column_name: str,
    dataflow_id: object = None,
) -> str:
    """Build the generated hint identity from authoritative references."""
    values = [connection_id, schema_name or "", table_name or "", column_name]
    if isinstance(dataflow_id, str) and dataflow_id.strip():
        values.append(dataflow_id)
    return json.dumps(values, ensure_ascii=False, separators=(",", ":"))


def _expected_hints(meta: dict, workspace_id: str) -> Dict[str, dict]:
    out = {}
    for group in meta.get("schema_hints", []):
        if normalize_workspace_id(group.get("workspace_id")) not in (None, workspace_id):
            continue
        conn_id = group.get("connection_id")
        conn_name = group.get("connection_name", "")
        if not conn_name and conn_id:
            conn_name = next(
                (
                    row.get("name", "")
                    for row in meta.get("connections", [])
                    if row.get("connection_id") == conn_id
                ),
                "",
            )
        if not conn_id:
            conn_id = _connection_id(meta, group, {}, "source", workspace_id)
        table_name = group.get("table_name", "")
        for col in group.get("hints", []) or group.get("columns", []):
            key = _hint_key(
                str(conn_id),
                group.get("schema_name"),
                table_name,
                col["column_name"],
                group.get("dataflow_id"),
            )
            hint_id = str(col.get("schema_hint_id") or _name_to_uuid(
                key, group.get("workspace_id") or workspace_id
            ))
            out[hint_id] = {
                "schema_hint_id": hint_id,
                "connection_name": conn_name,
                "connection_id": str(conn_id),
                "schema_name": group.get("schema_name"),
                "table_name": table_name,
                "column_name": col["column_name"],
                "data_type": col["data_type"],
                "format": col.get("format"),
                "precision": col.get("precision"),
                "scale": col.get("scale"),
                "is_active": col.get("is_active", True),
            }
    return out


def _actual_hints(engine, workspace_id: str) -> Dict[str, dict]:
    d = engine.dialect.name
    q = lambda c: quote_col(c, d)  # noqa: E731
    # MSSQL and Oracle don't support/tolerate USING(); use ON for all dialects.
    join_clause = (
        "JOIN dc_framework_connections c ON sh.connection_id = c.connection_id"
    )
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT sh.schema_hint_id, sh.connection_id, c.name as conn_name, "
                f"sh.dataflow_id, sh.schema_name, sh.table_name, sh.column_name, sh.data_type, "
                f"sh.{q('format')}, sh.{q('precision')}, sh.scale, sh.is_active "
                f"FROM dc_framework_schema_hints sh "
                f"{join_clause} WHERE c.workspace_id = :ws"
            ),
            {"ws": workspace_id},
        ).mappings().all()
    out = {}
    for r in rows:
        out[str(r["schema_hint_id"])] = {
            "schema_hint_id": str(r["schema_hint_id"]),
            "connection_name": r["conn_name"],
            "connection_id": str(r["connection_id"]),
            "schema_name": r["schema_name"],
            "table_name": r["table_name"],
            "column_name": r["column_name"],
            "data_type": r["data_type"],
            "format": r["format"],
            "precision": r["precision"],
            "scale": r["scale"],
            "is_active": bool(r["is_active"]),
        }
    return out


def verify_hints(meta: dict, engine, workspace_id: str) -> Report:
    report = Report()
    expected = _expected_hints(meta, workspace_id)
    actual   = _actual_hints(engine, workspace_id)

    _FIELDS = [
        "schema_hint_id", "connection_name", "connection_id", "schema_name", "table_name",
        "column_name", "data_type", "format", "precision", "scale", "is_active",
    ]

    for hint_id in sorted(expected):
        if hint_id not in actual:
            report.missing_in_db.append(
                f"schema_hint '{expected[hint_id]['connection_name']}' ({hint_id})"
            )
            continue
        exp = expected[hint_id]
        act = actual[hint_id]
        diffs = []
        for f_name in _FIELDS:
            diffs += _diff(f_name, exp.get(f_name), act.get(f_name))
        if diffs:
            report.mismatches.append(
                f"schema_hint '{exp['connection_name']}' ({hint_id}):\n"
                + "\n".join(diffs)
            )

    for hint_id in sorted(set(actual) - set(expected)):
        report.extra_in_db.append(
            f"schema_hint '{actual[hint_id]['connection_name']}' ({hint_id})"
        )

    return report


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _print_report(name: str, report: Report) -> int:
    """Print report, return 1 if any issues found, else 0."""
    total = len(report.mismatches) + len(report.missing_in_db) + len(report.extra_in_db)
    if report.ok():
        logger.info("%-20s PASS  (0 issues)", name)
        return 0

    logger.error("%-20s FAIL  (%d issue(s))", name, total)
    for m in report.missing_in_db:
        logger.error("  MISSING in DB:  %s", m)
    for m in report.extra_in_db:
        logger.error("  EXTRA in DB:    %s", m)
    for m in report.mismatches:
        logger.error("  MISMATCH:\n%s", m)
    return 1


def main() -> None:
    os.chdir(DATACOOLIE_ROOT)
    parser = argparse.ArgumentParser(
        description="Verify that DB metadata matches the source JSON file."
    )
    parser.add_argument(
        "--json-path",
        action="append",
        help="Exact source JSON; repeat for multiple group files",
    )
    parser.add_argument("--connections-path", help="Exact shared connections file")
    parser.add_argument("--schema-hints-path", help="Exact shared schema hints file")
    parser.add_argument(
        "--connection-string",
        default=sqlite_metadata_url(),
        help="SQLAlchemy connection string for the metadata DB",
    )
    parser.add_argument(
        "--workspace-id",
        default="local-workspace",
        help="Workspace ID used during seeding",
    )
    args = parser.parse_args()

    sources = args.json_path or [str(SIM_ROOT / "scenarios/workflows/local_use_cases.json")]
    meta = load_metadata_files(sources, connections_path=args.connections_path,
                               schema_hints_path=args.schema_hints_path, root=SIM_ROOT)

    engine = create_engine(args.connection_string)

    failed = 0
    logger.info("=== Metadata verification: %s ===", sources)

    conn_report = verify_connections(meta, engine, args.workspace_id)
    failed += _print_report("connections", conn_report)

    df_report = verify_dataflows(meta, engine, args.workspace_id)
    failed += _print_report("dataflows", df_report)

    hint_report = verify_hints(meta, engine, args.workspace_id)
    failed += _print_report("schema_hints", hint_report)

    if failed == 0:
        logger.info("=== All checks PASSED ===")
        sys.exit(0)
    else:
        logger.error("=== %d section(s) FAILED ===", failed)
        sys.exit(1)


if __name__ == "__main__":
    main()
