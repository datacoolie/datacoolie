"""Simulated metadata API server for DataCoolie APIClient.

Reads a JSON metadata file and serves it via REST endpoints
compatible with the datacoolie.metadata.api_client.APIClient.

Usage:
    python api_metadata_server.py --json-path ../file/local_use_cases.json --port 8000

Endpoints (all scoped under /workspaces/{workspace_id}):
    GET  /workspaces/{ws}/connections                  — paginated list
    GET  /workspaces/{ws}/connections/{connection_id}   — single connection
    GET  /workspaces/{ws}/dataflows                    — paginated list (?stage=...)
    GET  /workspaces/{ws}/dataflows/{dataflow_id}      — single dataflow
    GET  /workspaces/{ws}/schema-hints                 — filtered by connection_id, table_name
    GET  /workspaces/{ws}/watermarks/{dataflow_id}     — current watermark
    PUT  /workspaces/{ws}/watermarks/{dataflow_id}     — update watermark
"""

import argparse
import json
import math
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from flask import Flask, jsonify, request
except ImportError:
    raise SystemExit("Flask is required: pip install flask")

from datacoolie.utils.helpers import name_to_uuid as _name_to_uuid


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_metadata(json_path: str) -> Dict[str, Any]:
    with open(json_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    connections: List[Dict[str, Any]] = []
    conn_map: Dict[str, Dict[str, Any]] = {}

    for c in raw.get("connections", []):
        conn_id = _name_to_uuid(c["name"])
        enriched = {**c, "connection_id": conn_id}
        connections.append(enriched)
        conn_map[c["name"]] = enriched

    dataflows: List[Dict[str, Any]] = []
    for df in raw.get("dataflows", []):
        df_id = _name_to_uuid(df.get("name", ""))
        src = dict(df.get("source", {}))
        dest = dict(df.get("destination", {}))

        # Resolve connection_name to full connection object
        src_conn_name = src.pop("connection_name", "")
        src["connection"] = conn_map.get(src_conn_name, {})
        dest_conn_name = dest.pop("connection_name", "")
        dest["connection"] = conn_map.get(dest_conn_name, {})

        enriched = {
            **df,
            "dataflow_id": df_id,
            "source": src,
            "destination": dest,
        }
        dataflows.append(enriched)

    schema_hints: List[Dict[str, Any]] = []
    for group in raw.get("schema_hints", []):
        conn_name = group.get("connection_name", "")
        conn_id = _name_to_uuid(conn_name) if conn_name else ""
        table_name = group.get("table_name", "")
        schema_name = group.get("schema_name", "")
        for hint in group.get("hints", []):
            schema_hints.append({
                **hint,
                "connection_id": conn_id,
                "table_name": table_name,
                "schema_name": schema_name,
            })

    return {
        "connections": connections,
        "conn_map": conn_map,
        "dataflows": dataflows,
        "schema_hints": schema_hints,
    }


# ---------------------------------------------------------------------------
# Pagination helper
# ---------------------------------------------------------------------------

def _paginate(items: List[Any], page: int, page_size: int) -> Dict[str, Any]:
    total = len(items)
    total_pages = max(1, math.ceil(total / page_size))
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size
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
# Flask app factory
# ---------------------------------------------------------------------------

def create_app(json_path: str, watermark_dir: Optional[Path] = None) -> Flask:
    app = Flask(__name__)
    meta = _load_metadata(json_path)

    if watermark_dir is None:
        watermark_dir = Path(__file__).resolve().parent / "watermarks"
    watermark_dir.mkdir(parents=True, exist_ok=True)
    app.logger.info("Watermarks persisted under: %s", watermark_dir)

    def _wm_path(dataflow_id: str) -> Path:
        # dataflow_id is a UUID string from _name_to_uuid(); safe as filename.
        return watermark_dir / f"{dataflow_id}.json"

    def _load_wm(dataflow_id: str) -> Optional[Dict[str, Any]]:
        p = _wm_path(dataflow_id)
        if not p.exists():
            return None
        try:
            with p.open("r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return None

    def _save_wm(dataflow_id: str, wm: Dict[str, Any]) -> None:
        p = _wm_path(dataflow_id)
        tmp = p.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(wm, f, indent=2, sort_keys=True)
        tmp.replace(p)  # atomic on POSIX; best-effort on Windows

    @app.route("/workspaces/<workspace_id>/connections", methods=["GET"])
    def list_connections(workspace_id: str):
        page = request.args.get("page", 1, type=int)
        page_size = request.args.get("page_size", 50, type=int)
        name_filter = request.args.get("name")
        active_only = request.args.get("active_only", "true").lower() == "true"

        items = meta["connections"]
        if active_only:
            items = [c for c in items if c.get("is_active", True)]
        if name_filter:
            items = [c for c in items if c.get("name") == name_filter]

        return jsonify(_paginate(items, page, page_size))

    @app.route("/workspaces/<workspace_id>/connections/<connection_id>", methods=["GET"])
    def get_connection(workspace_id: str, connection_id: str):
        for c in meta["connections"]:
            if c.get("connection_id") == connection_id:
                return jsonify(c)
        return jsonify({"error": "not found"}), 404

    @app.route("/workspaces/<workspace_id>/dataflows", methods=["GET"])
    def list_dataflows(workspace_id: str):
        page = request.args.get("page", 1, type=int)
        page_size = request.args.get("page_size", 50, type=int)
        stage_filter = request.args.get("stage")
        active_only = request.args.get("active_only", "true").lower() == "true"

        items = meta["dataflows"]
        if active_only:
            items = [d for d in items if d.get("is_active", True)]
        if stage_filter:
            stages = [s.strip() for s in stage_filter.split(",")]
            items = [d for d in items if d.get("stage") in stages]

        return jsonify(_paginate(items, page, page_size))

    @app.route("/workspaces/<workspace_id>/dataflows/<dataflow_id>", methods=["GET"])
    def get_dataflow(workspace_id: str, dataflow_id: str):
        for d in meta["dataflows"]:
            if d.get("dataflow_id") == dataflow_id:
                return jsonify(d)
        return jsonify({"error": "not found"}), 404

    @app.route("/workspaces/<workspace_id>/schema-hints", methods=["GET"])
    def list_schema_hints(workspace_id: str):
        page = request.args.get("page", 1, type=int)
        page_size = request.args.get("page_size", 50, type=int)
        conn_id = request.args.get("connection_id")
        table = request.args.get("table_name")
        schema_name = request.args.get("schema_name")

        items = meta["schema_hints"]
        if conn_id:
            items = [h for h in items if h.get("connection_id") == conn_id]
        if table:
            items = [h for h in items if h.get("table_name") == table]
        if schema_name:
            items = [h for h in items if h.get("schema_name") == schema_name]

        return jsonify(_paginate(items, page, page_size))

    @app.route("/workspaces/<workspace_id>/watermarks/<dataflow_id>", methods=["GET"])
    def get_watermark(workspace_id: str, dataflow_id: str):
        wm = _load_wm(dataflow_id)
        if wm is None:
            return jsonify({"current_value": None})
        return jsonify(wm)

    @app.route("/workspaces/<workspace_id>/watermarks/<dataflow_id>", methods=["PUT"])
    def update_watermark(workspace_id: str, dataflow_id: str):
        body = request.get_json(force=True)
        existing = _load_wm(dataflow_id) or {}
        wm = {
            "dataflow_id": dataflow_id,
            "current_value": body.get("current_value"),
            "previous_value": existing.get("current_value"),
            "job_id": body.get("job_id"),
            "dataflow_run_id": body.get("dataflow_run_id"),
        }
        _save_wm(dataflow_id, wm)
        return jsonify({"status": "ok"})

    return app


def main():
    parser = argparse.ArgumentParser(description="DataCoolie simulated metadata API server")
    parser.add_argument(
        "--json-path",
        default=str(Path(__file__).resolve().parent.parent / "file" / "local_use_cases.json"),
        help="Path to source JSON metadata file",
    )
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument(
        "--watermark-dir",
        default=str(Path(__file__).resolve().parent / "watermarks"),
        help="Directory where watermarks are persisted (one JSON file per dataflow_id)",
    )
    args = parser.parse_args()

    app = create_app(args.json_path, watermark_dir=Path(args.watermark_dir))
    print(f"Starting metadata API server on {args.host}:{args.port}")
    print(f"  Source:     {args.json_path}")
    print(f"  Watermarks: {args.watermark_dir}")
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
