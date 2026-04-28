"""PostgreSQL-backed metadata API server for DataCoolie APIClient.

Serves the ``dc_framework_*`` tables from PostgreSQL via REST endpoints
compatible with ``datacoolie.metadata.api_client.APIClient``.  Replaces the
JSON-file-backed :mod:`api_metadata_server` for Docker deployments.

Usage:
    DATABASE_URL=postgresql+psycopg2://datacoolie:datacoolie@localhost:5432/datacoolie \
    python pg_api_metadata_server.py --port 8000

Endpoints (scoped under ``/workspaces/{workspace_id}``):
    GET  /workspaces/{ws}/connections                 — paginated, ?name= ?active_only=
    GET  /workspaces/{ws}/connections/{connection_id} — single connection
    GET  /workspaces/{ws}/dataflows                   — paginated, ?stage= ?active_only=
    GET  /workspaces/{ws}/dataflows/{dataflow_id}     — single dataflow (nested conns)
    GET  /workspaces/{ws}/schema-hints                — ?connection_id= ?table_name= ?schema_name=
    GET  /workspaces/{ws}/watermarks/{dataflow_id}    — current watermark
    PUT  /workspaces/{ws}/watermarks/{dataflow_id}    — upsert watermark
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    from flask import Flask, jsonify, request
except ImportError:
    raise SystemExit("Flask is required: pip install flask")

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


logger = logging.getLogger("pg_api_metadata_server")


# ---------------------------------------------------------------------------
# Row mappers — convert DB rows to the JSON shape APIClient expects
# ---------------------------------------------------------------------------

def _parse_json(raw: Any) -> Any:
    """Parse a TEXT column holding JSON, returning ``None`` on empty/invalid."""
    if raw is None or raw == "":
        return None
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def _row_to_connection(row: Any) -> Dict[str, Any]:
    """Map a ``dc_framework_connections`` row to the API response dict."""
    return {
        "connection_id": row.connection_id,
        "workspace_id": row.workspace_id,
        "name": row.name,
        "description": row.description,
        "connection_type": row.connection_type,
        "format": row.format,
        "catalog": row.catalog,
        "database": row.database,
        "configure": _parse_json(row.configure) or {},
        "secrets_ref": _parse_json(row.secrets_ref),
        "is_active": bool(row.is_active) if row.is_active is not None else True,
    }


def _row_to_dataflow(
    row: Any,
    src_conn: Optional[Dict[str, Any]],
    dest_conn: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Map a ``dc_framework_dataflows`` row to the nested API response dict.

    The server returns the ``source.connection`` / ``destination.connection``
    objects inline so that ``APIClient._dict_to_dataflow`` can consume them
    in a single request without a follow-up lookup.
    """
    src_wm = _parse_json(row.source_watermark_columns) or []
    src_conf = _parse_json(row.source_configure) or {}
    dest_merge = _parse_json(row.destination_merge_keys) or []
    dest_conf = _parse_json(row.destination_configure) or {}
    # partition_columns are stored inside destination_configure by the seeder
    partition_cols = dest_conf.get("partition_columns") or []
    return {
        "dataflow_id": row.dataflow_id,
        "workspace_id": row.workspace_id,
        "name": row.name,
        "description": row.description,
        "stage": row.stage,
        "group_number": row.group_number,
        "execution_order": row.execution_order,
        "processing_mode": row.processing_mode or "batch",
        "is_active": bool(row.is_active) if row.is_active is not None else True,
        "configure": _parse_json(row.configure) or {},
        "transform": _parse_json(row.transform) or {},
        "source": {
            "connection": src_conn or {},
            "schema_name": row.source_schema,
            "table": row.source_table,
            "query": row.source_query,
            "python_function": row.source_python_function,
            "watermark_columns": src_wm,
            "configure": src_conf,
        },
        "destination": {
            "connection": dest_conn or {},
            "schema_name": row.destination_schema,
            "table": row.destination_table,
            "load_type": row.destination_load_type,
            "merge_keys": dest_merge,
            "partition_columns": partition_cols,
            "configure": dest_conf,
        },
    }


def _row_to_schema_hint(row: Any) -> Dict[str, Any]:
    return {
        "schema_hint_id": row.schema_hint_id,
        "connection_id": row.connection_id,
        "dataflow_id": row.dataflow_id,
        "schema_name": row.schema_name,
        "table_name": row.table_name,
        "column_name": row.column_name,
        "data_type": row.data_type,
        "format": row.format,
        "precision": row.precision,
        "scale": row.scale,
        "default_value": row.default_value,
        # SchemaHint.ordinal_position is a non-nullable int with default 0;
        # Coerce DB nulls so the APIClient model mapper accepts the row.
        "ordinal_position": row.ordinal_position if row.ordinal_position is not None else 0,
        "is_active": bool(row.is_active) if row.is_active is not None else True,
    }


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------

def _paginate(items: List[Any], page: int, page_size: int) -> Dict[str, Any]:
    total = len(items)
    total_pages = max(1, math.ceil(total / page_size)) if page_size > 0 else 1
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size if page_size > 0 else total
    return {
        "data": items[start:end],
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
        },
    }


# ---------------------------------------------------------------------------
# SQL helpers — all queries honour workspace_id + deleted_at IS NULL
# ---------------------------------------------------------------------------

_CONN_SELECT = """
    SELECT connection_id, workspace_id, name, description, connection_type,
           format, catalog, database, configure, secrets_ref, is_active
      FROM dc_framework_connections
     WHERE workspace_id = :ws AND deleted_at IS NULL
"""

_DF_SELECT = """
    SELECT dataflow_id, workspace_id, name, description, stage,
           group_number, execution_order, processing_mode,
           source_connection_id, source_schema, source_table,
           source_query, source_python_function,
           source_watermark_columns, source_configure,
           transform,
           destination_connection_id, destination_schema, destination_table,
           destination_load_type, destination_merge_keys, destination_configure,
           configure, is_active
      FROM dc_framework_dataflows
     WHERE workspace_id = :ws AND deleted_at IS NULL
"""

_HINT_SELECT = """
    SELECT schema_hint_id, connection_id, dataflow_id, schema_name, table_name,
           column_name, data_type, format, precision, scale,
           default_value, ordinal_position, is_active
      FROM dc_framework_schema_hints
     WHERE deleted_at IS NULL
"""


def _fetch_connections(engine: Engine, workspace_id: str) -> Dict[str, Dict[str, Any]]:
    """Load all workspace connections into a ``{connection_id: dict}`` map."""
    with engine.connect() as conn:
        rows = conn.execute(text(_CONN_SELECT), {"ws": workspace_id}).all()
    return {r.connection_id: _row_to_connection(r) for r in rows}


# ---------------------------------------------------------------------------
# Flask app factory
# ---------------------------------------------------------------------------

def create_app(database_url: str) -> Flask:
    app = Flask(__name__)
    engine = create_engine(database_url, pool_pre_ping=True)
    app.config["ENGINE"] = engine

    # ---- connections ---------------------------------------------------

    @app.route("/workspaces/<workspace_id>/connections", methods=["GET"])
    def list_connections(workspace_id: str):
        page = request.args.get("page", 1, type=int)
        page_size = request.args.get("page_size", 50, type=int)
        name_filter = request.args.get("name")
        active_only = request.args.get("active_only", "false").lower() == "true"

        sql = _CONN_SELECT
        params: Dict[str, Any] = {"ws": workspace_id}
        if name_filter:
            sql += " AND name = :name"
            params["name"] = name_filter
        if active_only:
            sql += " AND is_active = TRUE"
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).all()
        items = [_row_to_connection(r) for r in rows]
        return jsonify(_paginate(items, page, page_size))

    @app.route("/workspaces/<workspace_id>/connections/<connection_id>", methods=["GET"])
    def get_connection(workspace_id: str, connection_id: str):
        sql = _CONN_SELECT + " AND connection_id = :cid"
        with engine.connect() as conn:
            row = conn.execute(
                text(sql), {"ws": workspace_id, "cid": connection_id}
            ).first()
        if row is None:
            return jsonify({"error": "not found"}), 404
        return jsonify(_row_to_connection(row))

    # ---- dataflows -----------------------------------------------------

    @app.route("/workspaces/<workspace_id>/dataflows", methods=["GET"])
    def list_dataflows(workspace_id: str):
        page = request.args.get("page", 1, type=int)
        page_size = request.args.get("page_size", 50, type=int)
        stage_filter = request.args.get("stage")
        active_only = request.args.get("active_only", "false").lower() == "true"

        sql = _DF_SELECT
        params: Dict[str, Any] = {"ws": workspace_id}
        if stage_filter:
            stages = [s.strip() for s in stage_filter.split(",") if s.strip()]
            if stages:
                placeholders = ", ".join(f":stage{i}" for i in range(len(stages)))
                sql += f" AND stage IN ({placeholders})"
                for i, s in enumerate(stages):
                    params[f"stage{i}"] = s
        if active_only:
            sql += " AND is_active = TRUE"

        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).all()

        conns = _fetch_connections(engine, workspace_id)
        items = [
            _row_to_dataflow(
                r,
                conns.get(r.source_connection_id),
                conns.get(r.destination_connection_id),
            )
            for r in rows
        ]
        return jsonify(_paginate(items, page, page_size))

    @app.route("/workspaces/<workspace_id>/dataflows/<dataflow_id>", methods=["GET"])
    def get_dataflow(workspace_id: str, dataflow_id: str):
        sql = _DF_SELECT + " AND dataflow_id = :did"
        with engine.connect() as conn:
            row = conn.execute(
                text(sql), {"ws": workspace_id, "did": dataflow_id}
            ).first()
        if row is None:
            return jsonify({"error": "not found"}), 404
        conns = _fetch_connections(engine, workspace_id)
        return jsonify(_row_to_dataflow(
            row,
            conns.get(row.source_connection_id),
            conns.get(row.destination_connection_id),
        ))

    # ---- schema hints --------------------------------------------------

    @app.route("/workspaces/<workspace_id>/schema-hints", methods=["GET"])
    def list_schema_hints(workspace_id: str):
        page = request.args.get("page", 1, type=int)
        page_size = request.args.get("page_size", 50, type=int)
        cid = request.args.get("connection_id")
        table = request.args.get("table_name")
        schema_name = request.args.get("schema_name")

        sql = _HINT_SELECT
        params: Dict[str, Any] = {}
        # Scope to workspace via connection table join.
        sql += (
            " AND connection_id IN ("
            "   SELECT connection_id FROM dc_framework_connections"
            "    WHERE workspace_id = :ws AND deleted_at IS NULL"
            " )"
        )
        params["ws"] = workspace_id
        if cid:
            sql += " AND connection_id = :cid"
            params["cid"] = cid
        if table:
            sql += " AND LOWER(table_name) = LOWER(:tname)"
            params["tname"] = table
        if schema_name:
            sql += " AND LOWER(schema_name) = LOWER(:sname)"
            params["sname"] = schema_name
        sql += " ORDER BY ordinal_position NULLS LAST"

        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).all()
        items = [_row_to_schema_hint(r) for r in rows]
        return jsonify(_paginate(items, page, page_size))

    # ---- watermarks ----------------------------------------------------

    @app.route("/workspaces/<workspace_id>/watermarks/<dataflow_id>", methods=["GET"])
    def get_watermark(workspace_id: str, dataflow_id: str):
        sql = """
            SELECT watermark_id, dataflow_id, current_value, previous_value,
                   job_id, dataflow_run_id, updated_at
              FROM dc_framework_watermarks
             WHERE dataflow_id = :did
        """
        with engine.connect() as conn:
            row = conn.execute(text(sql), {"did": dataflow_id}).first()
        if row is None:
            return jsonify({"current_value": None})
        return jsonify({
            "watermark_id": row.watermark_id,
            "dataflow_id": row.dataflow_id,
            "current_value": row.current_value,
            "previous_value": row.previous_value,
            "job_id": row.job_id,
            "dataflow_run_id": row.dataflow_run_id,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        })

    @app.route("/workspaces/<workspace_id>/watermarks/<dataflow_id>", methods=["PUT"])
    def update_watermark(workspace_id: str, dataflow_id: str):
        body = request.get_json(force=True) or {}
        current_value = body.get("current_value")
        job_id = body.get("job_id")
        run_id = body.get("dataflow_run_id")
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        # Rotate previous_value from the current value in a single UPDATE;
        # on rowcount=0 fall back to INSERT.  Matches DatabaseProvider logic.
        update_sql = """
            UPDATE dc_framework_watermarks
               SET previous_value = current_value,
                   current_value  = :cv,
                   job_id         = :jid,
                   dataflow_run_id = :rid,
                   updated_at     = :now
             WHERE dataflow_id = :did
        """
        insert_sql = """
            INSERT INTO dc_framework_watermarks
                (watermark_id, dataflow_id, current_value, previous_value,
                 job_id, dataflow_run_id, updated_at)
            VALUES (:wid, :did, :cv, NULL, :jid, :rid, :now)
        """
        with engine.begin() as conn:
            result = conn.execute(text(update_sql), {
                "cv": current_value, "jid": job_id, "rid": run_id,
                "now": now, "did": dataflow_id,
            })
            if result.rowcount == 0:
                conn.execute(text(insert_sql), {
                    "wid": str(uuid.uuid4()), "did": dataflow_id,
                    "cv": current_value, "jid": job_id, "rid": run_id, "now": now,
                })
        return jsonify({"status": "ok"})

    # ---- health --------------------------------------------------------

    @app.route("/health", methods=["GET"])
    def health():
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return jsonify({"status": "ok"})
        except Exception as exc:
            return jsonify({"status": "error", "detail": str(exc)}), 503

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="DataCoolie PostgreSQL metadata API server")
    parser.add_argument(
        "--database-url",
        default=os.environ.get(
            "DATABASE_URL",
            "postgresql+psycopg2://datacoolie:datacoolie@localhost:5432/datacoolie",
        ),
        help="SQLAlchemy database URL (env: DATABASE_URL)",
    )
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Translate bare ``postgresql://`` URLs (libpq style) to SQLAlchemy's
    # explicit ``postgresql+psycopg2://`` driver spec.
    db_url = args.database_url
    if db_url.startswith("postgresql://"):
        db_url = "postgresql+psycopg2://" + db_url[len("postgresql://"):]

    logger.info("Starting PG metadata API server on %s:%d", args.host, args.port)
    logger.info("Database URL: %s", db_url.split("@", 1)[-1])  # hide credentials
    app = create_app(db_url)
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
