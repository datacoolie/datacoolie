"""Fan-out metadata from a single use-cases JSON into multiple targets.

Targets:
    file              : export JSON/YAML/XLSX at the declared output directory
    db:<dialect>      : apply schema + seed connections/dataflows/schema_hints
                        (dialects: sqlite, postgresql, mysql, mssql, oracle)
    api-db            : alias for db:postgresql (what pg_api_metadata_server reads)

Examples:
    python usecase-sim/scripts/bootstrap/setup_metadata.py
        # → default: file + db:sqlite + db:postgresql (local workspace)

    python usecase-sim/scripts/bootstrap/setup_metadata.py \
        --json usecase-sim/scenarios/workflows/aws_use_cases.json \
        --workspace-id aws-workspace --targets db:postgresql --truncate

    python usecase-sim/scripts/bootstrap/setup_metadata.py \
        --targets file,db:postgresql,db:mysql,db:mssql,db:oracle --truncate
"""
from __future__ import annotations

import os

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from runner.runtime_paths import DATACOOLIE_ROOT, USECASE_SIM_DIR as SIM_ROOT
from runner.metadata_documents import load_metadata_files
from datacoolie.utils.identity import (  # noqa: E402
    is_usable_identifier,
    name_to_uuid,
    normalize_document_workspace,
    normalize_workspace_id,
)
sys.path.insert(0, str(SIM_ROOT / 'scripts'))


from _common import (  # noqa: E402
    DB_URLS,
    OUTPUT_DIR,
    LOCAL_USE_CASES_JSON,
    METADATA_DB_DIR,
    setup_logging,
)

logger = setup_logging("setup_metadata")

# Dialect → schema SQL file (kept in metadata/database alongside the DB)
_DIALECT_SCHEMA = {
    "sqlite":     METADATA_DB_DIR / "schema.sql",
    "postgresql": METADATA_DB_DIR / "schema_postgres.sql",
    "mysql":      METADATA_DB_DIR / "schema_mysql.sql",
    "mssql":      METADATA_DB_DIR / "schema_mssql.sql",
    "oracle":     METADATA_DB_DIR / "schema_oracle.sql",
}

_RESERVED_COLS = {"format", "database", "precision"}

ALL_DIALECTS = ["sqlite", "postgresql", "mysql", "mssql", "oracle"]

_REQUIRED_METADATA_TABLES = {
    "dc_framework_connections",
    "dc_framework_dataflows",
    "dc_framework_watermarks",
    "dc_framework_schema_hints",
}


# ===========================================================================
# DDL helpers (inlined from metadata/database/_shared.py)
# ===========================================================================

def quote_col(name: str, dialect: str) -> str:
    if name not in _RESERVED_COLS:
        return name
    if dialect == "mysql":
        return f"`{name}`"
    if dialect == "mssql":
        return f"[{name}]"
    if dialect == "oracle":
        return name
    return f'"{name}"'


def apply_schema(engine) -> None:
    from sqlalchemy import text  # noqa: PLC0415
    dialect = engine.dialect.name
    schema_path = _DIALECT_SCHEMA.get(dialect)
    if schema_path is None:
        raise ValueError(f"Unsupported dialect: {dialect}")
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file missing: {schema_path}")
    sql = re.sub(r"--[^\n]*", "", schema_path.read_text(encoding="utf-8"))
    with engine.begin() as conn:
        if dialect == "oracle":
            for block in re.split(r"^\s*/\s*$", sql, flags=re.MULTILINE):
                block = block.strip()
                if block:
                    conn.execute(text(block))
        else:
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))
    _validate_required_tables(engine)
    logger.info("Applied schema: %s (%s)", schema_path.name, dialect)


def _validate_required_tables(engine) -> None:
    from sqlalchemy import inspect  # noqa: PLC0415

    actual = {name.lower() for name in inspect(engine).get_table_names()}
    missing = sorted(_REQUIRED_METADATA_TABLES - actual)
    if missing:
        raise RuntimeError(
            "Metadata schema bootstrap incomplete; missing tables: "
            + ", ".join(missing)
        )


# ===========================================================================
# Seed helpers (condensed from metadata/database/seed_from_json.py)
# ===========================================================================

def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _json_str(obj):
    if obj is None:
        return None
    if isinstance(obj, str):
        return obj
    return json.dumps(obj)


def _name_to_uuid(name: str, workspace_id: str | None = None) -> str:
    """Keep the bootstrap call sites on the canonical identity contract."""
    return name_to_uuid(name, workspace_id)


def _connections_sql(dialect: str) -> str:
    q = lambda c: quote_col(c, dialect)  # noqa: E731
    return f"""
    INSERT INTO dc_framework_connections
        (connection_id, workspace_id, name, description,
         connection_type, {q("format")}, catalog, {q("database")},
         configure, secrets_ref, is_active, version,
         created_at, updated_at, created_by, updated_by)
    VALUES
        (:cid, :ws, :name, :desc,
         :ctype, :fmt, :catalog, :db,
         :configure, :secrets_ref, :active, 1,
         :now, :now, 'setup', 'setup')
    """


_DATAFLOWS_SQL = """
INSERT INTO dc_framework_dataflows
    (dataflow_id, workspace_id, name, description,
     stage, group_number, execution_order, processing_mode,
     source_connection_id, source_schema, source_table,
     source_query, source_python_function,
     source_filter_expression, source_watermark_columns, source_configure,
     transform,
     destination_connection_id, destination_schema, destination_table,
     destination_load_type, destination_merge_keys, destination_configure,
     configure, is_active, version,
     created_at, updated_at, created_by, updated_by)
VALUES
    (:did, :ws, :name, :desc,
     :stage, :gnum, :eorder, :pmode,
     :src_cid, :src_schema, :src_table,
     :src_query, :src_pyfunc,
     :src_filter, :src_wm, :src_conf,
     :transform,
     :dst_cid, :dst_schema, :dst_table,
     :dst_load, :dst_merge, :dst_conf,
     :conf, :active, 1,
     :now, :now, 'setup', 'setup')
"""


def _schema_hints_sql(dialect: str) -> str:
    q = lambda c: quote_col(c, dialect)  # noqa: E731
    return f"""
    INSERT INTO dc_framework_schema_hints
        (schema_hint_id, connection_id, dataflow_id,
         schema_name, table_name, column_name,
         data_type, {q("format")}, {q("precision")}, scale,
         default_value, ordinal_position, is_active,
         created_at, updated_at)
    VALUES
        (:hid, :cid, :dfid,
         :sname, :tname, :cname,
         :dtype, :fmt, :prec, :scale,
         :defval, :ordinal, :active,
         :now, :now)
    """


def _insert_or_skip(conn, sql, params, nested: bool):
    from sqlalchemy import text  # noqa: PLC0415
    from sqlalchemy.exc import IntegrityError  # noqa: PLC0415
    try:
        if nested:
            with conn.begin_nested():
                conn.execute(text(sql), params)
        else:
            conn.execute(text(sql), params)
    except IntegrityError:
        pass


def _truncate_workspace(engine, workspace_id: str) -> None:
    from sqlalchemy import text  # noqa: PLC0415
    with engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM dc_framework_schema_hints "
                "WHERE dataflow_id IN "
                "  (SELECT dataflow_id FROM dc_framework_dataflows WHERE workspace_id = :ws) "
                "OR connection_id IN "
                "  (SELECT connection_id FROM dc_framework_connections WHERE workspace_id = :ws)"
            ),
            {"ws": workspace_id},
        )
        conn.execute(text("DELETE FROM dc_framework_dataflows WHERE workspace_id = :ws"),
                     {"ws": workspace_id})
        conn.execute(text("DELETE FROM dc_framework_connections WHERE workspace_id = :ws"),
                     {"ws": workspace_id})
    logger.info("Truncated workspace_id=%s", workspace_id)


def _seed_document(meta: dict, workspace_id: str) -> dict:
    """Normalize scope and reject foreign rows before touching the database."""
    workspace_id = normalize_workspace_id(workspace_id)
    if workspace_id is None:
        raise ValueError("workspace_id must be a non-empty string")
    try:
        document = normalize_document_workspace(meta, default_workspace=workspace_id)
    except ValueError as exc:
        raise ValueError(f"Invalid metadata workspace: {exc}") from exc
    for section in ("connections", "dataflows"):
        for row in document.get(section, []):
            if not isinstance(row, dict) or normalize_workspace_id(row.get("workspace_id")) != workspace_id:
                raise ValueError(
                    f"{section} row is not bound to workspace {workspace_id!r}"
                )
    for group in document.get("schema_hints", []):
        if isinstance(group, dict):
            scope = normalize_workspace_id(group.get("workspace_id"))
            if scope is not None and scope != workspace_id:
                raise ValueError(
                    f"schema_hints row is not bound to workspace {workspace_id!r}"
                )
            if scope is None:
                group["workspace_id"] = workspace_id
    return document


def _seed_records(meta: dict, workspace_id: str) -> dict[str, list[dict]]:
    """Resolve every identity/reference without performing I/O."""
    document = _seed_document(meta, workspace_id)
    connections: list[dict] = []
    by_id: dict[str, dict] = {}
    by_name: dict[str, list[dict]] = {}

    def register_connection(row: dict, *, inline: bool = False) -> dict:
        """Register one canonical connection, checking ID/name ownership."""
        if not isinstance(row, dict):
            raise ValueError("Connection definitions must be objects")
        name = row.get("name")
        if not is_usable_identifier(name):
            raise ValueError("Connection name must be a non-empty string")
        scope = normalize_workspace_id(row.get("workspace_id"))
        if scope != workspace_id:
            raise ValueError(
                f"Connection {name!r} is not bound to workspace {workspace_id!r}"
            )
        connection_id = row.get("connection_id")
        if not is_usable_identifier(connection_id):
            connection_id = _name_to_uuid(name, workspace_id)
        existing = by_id.get(connection_id)
        if existing is not None:
            if existing.get("name") != name or normalize_workspace_id(
                existing.get("workspace_id")
            ) != workspace_id:
                raise ValueError(
                    f"Connection ID {connection_id!r} has conflicting name/workspace"
                )
            return existing
        # Connection models supply these defaults for inline definitions. The
        # database target stores a normalized connection row, so make the
        # defaults explicit before writing instead of silently dropping an
        # inline endpoint's identity/configuration.
        row["connection_id"] = connection_id
        row["workspace_id"] = workspace_id
        row.setdefault("connection_type", "file")
        row.setdefault("format", "parquet")
        by_id[connection_id] = row
        by_name.setdefault(name, []).append(row)
        connections.append(row)
        if inline:
            document.setdefault("connections", []).append(row)
        return row

    for row in document.get("connections", []):
        before = row.get("connection_id") if isinstance(row, dict) else None
        owner = register_connection(row)
        if owner is not row:
            raise ValueError(f"Duplicate connection_id: {before or owner['connection_id']}")

    def hint_identity_key(
        connection_id: str,
        schema_name: object,
        table_name: object,
        column_name: str,
        dataflow_id: object = None,
    ) -> str:
        # IDs are derived from the authoritative connection identity.  The
        # optional dataflow grain keeps scoped hints distinct when a catalog
        # intentionally publishes a dataflow-specific contract.
        values = [connection_id, schema_name or "", table_name or "", column_name]
        if is_usable_identifier(dataflow_id):
            values.append(dataflow_id)
        return json.dumps(values, ensure_ascii=False, separators=(",", ":"))

    def resolve_endpoint(dataflow: dict, role: str) -> tuple[str, dict]:
        endpoint = dataflow.get(role) or {}
        if not isinstance(endpoint, dict):
            raise ValueError(f"Dataflow {dataflow.get('name')!r} {role} must be an object")
        inline = endpoint.get("connection")
        if isinstance(inline, dict):
            explicit_id = inline.get("connection_id")
            name = inline.get("name")
            inline_scope = normalize_workspace_id(inline.get("workspace_id"))
            if inline_scope is not None and inline_scope != workspace_id:
                raise ValueError(f"Dataflow {dataflow.get('name')!r} {role} crosses workspaces")
            if not is_usable_identifier(name):
                raise ValueError(
                    f"Dataflow {dataflow.get('name')!r} {role} inline connection "
                    "requires a non-empty name"
                )
            if is_usable_identifier(explicit_id) and explicit_id in by_id:
                owner = by_id[explicit_id]
                if owner.get("name") != name:
                    raise ValueError(
                        f"Dataflow {dataflow.get('name')!r} {role} connection_id "
                        f"{explicit_id!r} conflicts with inline name {name!r}"
                    )
                if normalize_workspace_id(owner.get("workspace_id")) != workspace_id:
                    raise ValueError(
                        f"Dataflow {dataflow.get('name')!r} {role} crosses workspaces"
                    )
                return explicit_id, owner
            inline = dict(inline)
            inline["workspace_id"] = workspace_id
            owner = register_connection(inline, inline=True)
            return owner["connection_id"], owner
        else:
            explicit_id = endpoint.get("connection_id")
            name = endpoint.get("connection_name")
        explicit_id = explicit_id or dataflow.get(f"{role}_connection_id")
        name = name or dataflow.get(f"{role}_connection")
        if is_usable_identifier(explicit_id):
            connection = by_id.get(explicit_id)
            if connection is None:
                raise ValueError(
                    f"Dataflow {dataflow.get('name')!r} {role} references unknown connection_id {explicit_id!r}"
                )
            if is_usable_identifier(name) and connection.get("name") != name:
                raise ValueError(
                    f"Dataflow {dataflow.get('name')!r} {role} connection_id "
                    f"{explicit_id!r} conflicts with connection name {name!r}"
                )
            return explicit_id, connection
        if not is_usable_identifier(name):
            raise ValueError(f"Dataflow {dataflow.get('name')!r} {role} has no connection reference")
        candidates = by_name.get(name, [])
        if len(candidates) != 1:
            raise ValueError(
                f"Dataflow {dataflow.get('name')!r} {role} connection name is ambiguous or unknown: {name!r}"
            )
        return candidates[0]["connection_id"], candidates[0]

    dataflows: list[dict] = []
    flow_ids: dict[str, dict] = {}
    for row in document.get("dataflows", []):
        name = row.get("name")
        if not is_usable_identifier(name):
            raise ValueError("Dataflow name must be a non-empty string")
        dataflow_id = row.get("dataflow_id")
        if not is_usable_identifier(dataflow_id):
            dataflow_id = _name_to_uuid(name, workspace_id)
        if dataflow_id in flow_ids:
            raise ValueError(f"Duplicate dataflow_id: {dataflow_id}")
        source_id, _ = resolve_endpoint(row, "source")
        destination_id, _ = resolve_endpoint(row, "destination")
        row["dataflow_id"] = dataflow_id
        row["workspace_id"] = workspace_id
        row["source_connection_id"] = source_id
        row["destination_connection_id"] = destination_id
        flow_ids[dataflow_id] = row
        dataflows.append(row)

    hints: list[dict] = []
    hint_ids: set[str] = set()
    for group in document.get("schema_hints", []):
        if not isinstance(group, dict):
            raise ValueError("Schema hint groups must be objects")
        reference = group.get("connection_id") or group.get("connection_name")
        connection = None
        if is_usable_identifier(group.get("connection_id")):
            connection = by_id.get(group["connection_id"])
        elif is_usable_identifier(group.get("connection_name")):
            candidates = by_name.get(group["connection_name"], [])
            if len(candidates) == 1:
                connection = candidates[0]
        if connection is None:
            raise ValueError(f"Schema hint references unknown or ambiguous connection: {reference!r}")
        group["connection_id"] = connection["connection_id"]
        group["connection_name"] = group.get("connection_name") or connection["name"]
        if group.get("dataflow_id") is not None and group["dataflow_id"] not in flow_ids:
            raise ValueError(f"Schema hint references unknown dataflow_id: {group['dataflow_id']!r}")
        for col in group.get("hints", []) or group.get("columns", []):
            if not isinstance(col, dict) or not is_usable_identifier(col.get("column_name")):
                raise ValueError("Schema hint columns require a non-empty column_name")
            hint_key = (
                f"{group['connection_name']}__{group.get('table_name', '')}__"
                f"{col['column_name']}"
            )
            hint_identity = hint_identity_key(
                group["connection_id"],
                group.get("schema_name"),
                group.get("table_name"),
                col["column_name"],
                group.get("dataflow_id"),
            )
            col["schema_hint_id"] = (
                col.get("schema_hint_id") if is_usable_identifier(col.get("schema_hint_id"))
                else _name_to_uuid(hint_identity, workspace_id)
            )
            if col["schema_hint_id"] in hint_ids:
                raise ValueError(f"Duplicate schema_hint_id: {col['schema_hint_id']}")
            hint_ids.add(col["schema_hint_id"])
            hints.append({"group": group, "column": col, "hint_key": hint_key})
    return {
        "document": document,
        "connections": connections,
        "dataflows": dataflows,
        "schema_hints": hints,
    }


def _expected_seed_records(meta: dict, workspace_id: str) -> dict[str, dict[str, str]]:
    records = _seed_records(meta, workspace_id)
    return {
        "connections": {str(row["connection_id"]): row["name"] for row in records["connections"]},
        "dataflows": {str(row["dataflow_id"]): row["name"] for row in records["dataflows"]},
        "schema_hints": {
            str(item["column"]["schema_hint_id"]): item["hint_key"]
            for item in records["schema_hints"]
        },
    }


def _validate_seeded_metadata(engine, meta: dict, workspace_id: str) -> None:
    from sqlalchemy import text  # noqa: PLC0415

    with engine.connect() as conn:
        actual = {
            "connections": {
                str(row[0])
                for row in conn.execute(
                    text(
                        "SELECT connection_id FROM dc_framework_connections "
                        "WHERE workspace_id = :ws"
                    ),
                    {"ws": workspace_id},
                )
            },
            "dataflows": {
                str(row[0])
                for row in conn.execute(
                    text(
                        "SELECT dataflow_id FROM dc_framework_dataflows "
                        "WHERE workspace_id = :ws"
                    ),
                    {"ws": workspace_id},
                )
            },
            "schema_hints": {
                str(row[0])
                for row in conn.execute(
                    text(
                        "SELECT sh.schema_hint_id FROM dc_framework_schema_hints sh "
                        "WHERE sh.connection_id IN "
                        "(SELECT connection_id FROM dc_framework_connections "
                        " WHERE workspace_id = :ws) "
                        "OR sh.dataflow_id IN "
                        "(SELECT dataflow_id FROM dc_framework_dataflows "
                        " WHERE workspace_id = :ws)"
                    ),
                    {"ws": workspace_id},
                )
            },
        }

    expected = _expected_seed_records(meta, workspace_id)
    missing = {
        category: [labels[item_id] for item_id in sorted(set(labels) - actual[category])]
        for category, labels in expected.items()
    }
    missing = {category: names for category, names in missing.items() if names}
    if missing:
        details = "; ".join(
            f"{category}: {', '.join(names)}" for category, names in missing.items()
        )
        raise RuntimeError(f"Metadata seed incomplete for workspace {workspace_id!r}; {details}")


def seed_db(connection_string: str, meta: dict, workspace_id: str, truncate: bool) -> None:
    from sqlalchemy import create_engine, make_url  # noqa: PLC0415

    workspace_id = normalize_workspace_id(workspace_id)
    if workspace_id is None:
        raise ValueError("workspace_id must be a non-empty string")
    records = _seed_records(meta, workspace_id)
    meta = records["document"]

    # SQLite does not create missing parent directories for a file database.
    # A fresh checkout must create the selected database parent before SQLAlchemy
    # opens the connection. Do not touch remote databases or
    # in-memory SQLite URLs.
    url = make_url(connection_string)
    if url.get_backend_name() == "sqlite" and url.database not in (None, ":memory:"):
        database_path = Path(url.database).expanduser().resolve()
        database_path.parent.mkdir(parents=True, exist_ok=True)

    engine = create_engine(connection_string)
    dialect = engine.dialect.name
    nested = (dialect == "postgresql")

    apply_schema(engine)
    if truncate:
        _truncate_workspace(engine, workspace_id)

    now = _now()
    conn_sql  = _connections_sql(dialect)
    hints_sql = _schema_hints_sql(dialect)

    connections = records["connections"]
    with engine.begin() as conn:
        for c in connections:
            cid = c["connection_id"]
            _insert_or_skip(conn, conn_sql, {
                "cid": cid, "ws": workspace_id,
                "name": c["name"], "desc": c.get("description"),
                "ctype": c["connection_type"],
                "fmt": c.get("format", ""),
                "catalog": c.get("catalog"), "db": c.get("database"),
                "configure": _json_str(c.get("configure")),
                "secrets_ref": _json_str(c.get("secrets_ref")),
                "active": c.get("is_active", True),
                "now": now,
            }, nested)

    dataflows = records["dataflows"]
    with engine.begin() as conn:
        for d in dataflows:
            did = d["dataflow_id"]
            src = d.get("source", {}) or {}
            dst = d.get("destination", {}) or {}
            src_cid = d["source_connection_id"]
            dst_cid = d["destination_connection_id"]

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

            _insert_or_skip(conn, _DATAFLOWS_SQL, {
                "did": did, "ws": workspace_id,
                "name": d["name"], "desc": d.get("description"),
                "stage": d.get("stage"),
                "gnum": d.get("group_number"),
                "eorder": d.get("execution_order"),
                "pmode": d.get("processing_mode"),
                "src_cid": src_cid,
                "src_schema": src.get("schema") or src.get("schema_name") or d.get("source_schema"),
                "src_table":  src.get("table")  or d.get("source_table"),
                "src_query":  src.get("query")  or d.get("source_query"),
                "src_pyfunc": src.get("python_function") or d.get("source_python_function"),
                "src_filter": src["filter_expression"] if "filter_expression" in src else d.get("source_filter_expression"),
                "src_wm":   _json_str(src.get("watermark_columns") or d.get("source_watermark_columns")),
                "src_conf": _json_str(src.get("configure") or d.get("source_configure")),
                "transform": _json_str(d.get("transform")),
                "dst_cid": dst_cid,
                "dst_schema": dst.get("schema") or dst.get("schema_name") or d.get("destination_schema"),
                "dst_table":  dst.get("table")  or d.get("destination_table"),
                "dst_load":   dst.get("load_type") or d.get("destination_load_type"),
                "dst_merge":  _json_str(dst.get("merge_keys") or d.get("destination_merge_keys")),
                "dst_conf":   _json_str(dst_configure or None),
                "conf": _json_str(d.get("configure")),
                "active": d.get("is_active", True),
                "now": now,
            }, nested)

    schema_hints = records["schema_hints"]
    with engine.begin() as conn:
        for item in schema_hints:
            group = item["group"]
            col = item["column"]
            hid = col["schema_hint_id"]
            _insert_or_skip(conn, hints_sql, {
                "hid": hid, "cid": group["connection_id"], "dfid": group.get("dataflow_id"),
                "sname": group.get("schema_name"),
                "tname": group.get("table_name"),
                "cname": col["column_name"],
                "dtype": col["data_type"],
                "fmt": col.get("format"),
                "prec": col.get("precision"),
                "scale": col.get("scale"),
                "defval": col.get("default_value"),
                "ordinal": col.get("ordinal_position"),
                "active": col.get("is_active", True),
                "now": now,
            }, nested)

    _validate_seeded_metadata(engine, meta, workspace_id)
    logger.info("Seeded %s (%s): %d connections, %d dataflows, %d hint groups",
                dialect, workspace_id,
                len(connections), len(dataflows), len(schema_hints))


# ===========================================================================
# File sibling emitters (YAML + XLSX) — condensed from build_metadata_siblings.py
# ===========================================================================

def _json_cell(value):
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _list_to_csv(value):
    if not value:
        return ""
    if isinstance(value, list):
        parts = [json.dumps(i, ensure_ascii=False) if isinstance(i, dict) else str(i) for i in value]
        return ",".join(parts)
    return str(value)


def emit_yaml(data: dict, out_path: Path) -> None:
    try:
        import yaml  # noqa: PLC0415
    except ImportError:
        logger.warning("pyyaml not installed — skipping %s", out_path.name)
        return
    with out_path.open("w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
    logger.info("Wrote %s", out_path)


def emit_xlsx(data: dict, out_path: Path) -> None:
    try:
        import openpyxl  # noqa: PLC0415
    except ImportError:
        logger.warning("openpyxl not installed — skipping %s", out_path.name)
        return
    wb = openpyxl.Workbook()

    ws_c = wb.active
    ws_c.title = "connections"
    ws_c.append(["name", "connection_id", "workspace_id", "connection_type", "format",
                 "catalog", "database", "configure", "secrets_ref", "is_active"])
    for c in data.get("connections", []):
        ws_c.append([
            c.get("name", ""), c.get("connection_id", ""), c.get("workspace_id", ""),
            c.get("connection_type", ""), c.get("format", ""),
            c.get("catalog", ""), c.get("database", ""),
            _json_cell(c.get("configure")), _json_cell(c.get("secrets_ref")),
            "" if c.get("is_active", True) else False,
        ])

    ws_d = wb.create_sheet("dataflows")
    ws_d.append([
        "name", "dataflow_id", "workspace_id", "description", "stage", "group_number", "execution_order", "processing_mode",
        "is_active", "configure",
        "source_connection_name", "source_connection_id", "source_schema_name", "source_table",
        "source_query", "source_python_function", "source_watermark_columns",
        "source_filter_expression", "source_configure",
        "destination_connection_name", "destination_connection_id", "destination_schema_name", "destination_table",
        "destination_load_type", "destination_merge_keys",
        "destination_partition_columns", "destination_configure", "transform",
    ])
    for d in data.get("dataflows", []):
        src = d.get("source", {}) or {}
        dst = d.get("destination", {}) or {}
        tx  = d.get("transform", {}) or {}
        part = dst.get("partition_columns", [])
        part_str = json.dumps(part, ensure_ascii=False) if part and isinstance(part[0], dict) else _list_to_csv(part)
        ws_d.append([
            d.get("name", ""), d.get("dataflow_id", ""), d.get("workspace_id", ""),
            d.get("description", ""), d.get("stage", ""),
            d.get("group_number", ""), d.get("execution_order", ""),
            d.get("processing_mode", "batch"),
            "" if d.get("is_active", True) else False,
            _json_cell(d.get("configure")),
            src.get("connection_name", ""), src.get("connection_id", "") or d.get("source_connection_id", ""), src.get("schema_name", ""),
            src.get("table", ""), src.get("query", ""),
            src.get("python_function", ""),
            _list_to_csv(src.get("watermark_columns", [])),
            src.get("filter_expression", ""),
            _json_cell(src.get("configure")),
            dst.get("connection_name", ""), dst.get("connection_id", "") or d.get("destination_connection_id", ""), dst.get("schema_name", ""),
            dst.get("table", ""), dst.get("load_type", "append"),
            _list_to_csv(dst.get("merge_keys", [])),
            part_str, _json_cell(dst.get("configure")),
            _json_cell(tx) if tx else "",
        ])

    ws_h = wb.create_sheet("schema_hints")
    ws_h.append(["connection_name", "connection_id", "workspace_id", "dataflow_id",
                 "table_name", "schema_name", "column_name", "schema_hint_id",
                 "data_type", "format", "precision", "scale", "is_active"])
    for group in data.get("schema_hints", []):
        cn = group.get("connection_name", "")
        tn = group.get("table_name", "")
        sn = group.get("schema_name", "")
        for h in group.get("hints", []):
            ws_h.append([
                cn, group.get("connection_id", ""), group.get("workspace_id", ""),
                group.get("dataflow_id", ""), tn, sn, h.get("column_name", ""),
                h.get("schema_hint_id", ""), h.get("data_type", ""),
                h.get("format", ""), h.get("precision", ""), h.get("scale", ""),
                "" if h.get("is_active", True) else False,
            ])

    wb.save(out_path)
    logger.info("Wrote %s", out_path)


# ===========================================================================
# Driver
# ===========================================================================

def _parse_targets(raw: str) -> list[str]:
    items = [t.strip().lower() for t in raw.split(",") if t.strip()]
    valid: list[str] = []
    for t in items:
        if t == "file":
            valid.append("file")
        elif t == "api-db":
            valid.append("db:postgresql")
        elif t.startswith("db:"):
            dialect = t[3:]
            if dialect not in ALL_DIALECTS:
                raise ValueError(f"Unknown dialect: {dialect}. Known: {ALL_DIALECTS}")
            valid.append(t)
        else:
            raise ValueError(f"Unknown target: {t}")
    # dedupe preserving order
    seen: set[str] = set()
    out: list[str] = []
    for t in valid:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _default_targets() -> str:
    return "file," + ",".join(f"db:{d}" for d in ["sqlite", "postgresql"])


def main() -> int:
    os.chdir(DATACOOLIE_ROOT)
    parser = argparse.ArgumentParser(description="Fan-out metadata from JSON to multiple targets")
    parser.add_argument("--json", action="append",
                        help="Exact source JSON; repeat for multiple group files")
    parser.add_argument("--connections-path", help="Exact shared connections section file")
    parser.add_argument("--schema-hints-path", help="Exact shared schema_hints section file")
    parser.add_argument("--workspace-id", default="local-workspace")
    parser.add_argument("--targets", default=_default_targets(),
                        help="Comma-separated: file, db:<dialect>, api-db. "
                             "Dialects: " + ", ".join(ALL_DIALECTS))
    parser.add_argument("--truncate", action="store_true",
                        help="Delete existing rows for the workspace before seeding")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR / "metadata_exports",
                        help="Destination for explicit file exports; unused for database targets")
    parser.add_argument("--db-url", action="append", default=[],
                        metavar="DIALECT=URL",
                        help="Override a dialect's connection string (repeatable)")
    args = parser.parse_args()

    targets = _parse_targets(args.targets)
    logger.info("Targets: %s", targets)

    # Merge --db-url overrides
    db_urls = dict(DB_URLS)
    for kv in args.db_url:
        if "=" not in kv:
            logger.error("Bad --db-url %r (want DIALECT=URL)", kv)
            return 2
        k, v = kv.split("=", 1)
        if k not in ALL_DIALECTS:
            logger.error("Unknown dialect in --db-url: %s", k)
            return 2
        db_urls[k] = v

    sources = args.json or [str(LOCAL_USE_CASES_JSON)]
    source_paths = []
    for value in sources:
        source_path = Path(value)
        if not source_path.is_absolute():
            source_path = (DATACOOLIE_ROOT / source_path).resolve()
        if not source_path.is_file():
            logger.error("JSON not found: %s", source_path)
            return 2
        source_paths.append(source_path)

    # Seed backends from one in-memory snapshot. Only the explicit file target
    # needs an export; a database seed must not create a JSON intermediate.
    try:
        meta = load_metadata_files(source_paths, connections_path=args.connections_path,
                                   schema_hints_path=args.schema_hints_path, root=SIM_ROOT)
    except (OSError, ValueError) as exc:
        logger.error("Cannot load metadata %s: %s", source_paths, exc)
        return 2

    rc = 0
    export_path = None
    if "file" in targets:
        basename = source_paths[0].name if len(source_paths) == 1 else "metadata.json"
        export_path = args.output_dir.resolve() / basename
        inputs = set(source_paths)
        for value in (args.connections_path, args.schema_hints_path):
            if value:
                inputs.add(Path(value).resolve())
        outputs = {export_path.with_suffix(suffix) for suffix in (".json", ".yaml", ".xlsx")}
        # Protect every authored source, including manifests' referenced files.
        authored_root = (SIM_ROOT / "metadata").resolve()
        if outputs & inputs or any(path.is_relative_to(authored_root) for path in outputs):
            logger.error("Export destination would overwrite authored metadata: %s", export_path)
            return 2
    for target in targets:
        if target == "file":
            logger.info("--- target: file ---")
            args.output_dir.mkdir(parents=True, exist_ok=True)
            json_path = export_path
            json_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n",
                                 encoding="utf-8")
            emit_yaml(meta, json_path.with_suffix(".yaml"))
            emit_xlsx(meta, json_path.with_suffix(".xlsx"))
        elif target.startswith("db:"):
            dialect = target[3:]
            url = db_urls[dialect]
            logger.info("--- target: db:%s ---", dialect)
            try:
                seed_db(url, meta, args.workspace_id, truncate=args.truncate)
            except Exception:
                logger.exception("db:%s failed", dialect)
                rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
