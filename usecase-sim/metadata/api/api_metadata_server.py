"""Simulated metadata API server for DataCoolie APIProvider.

Reads a JSON metadata file and serves it via REST endpoints
compatible with the datacoolie.metadata.api_provider.APIProvider.

Usage:
    python api_metadata_server.py --json-path ../file/workflows/local_use_cases.json --port 8000

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
import copy
import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

SIM_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SIM_ROOT))

from runner.metadata_documents import load_metadata_files  # noqa: E402

try:
    from flask import Flask, jsonify, request
except ImportError:
    raise SystemExit("Flask is required: pip install flask")

from datacoolie.metadata.documents.mapping import (  # noqa: E402
    build_connections,
    build_dataflows,
    build_grouped_schema_hints,
)
from datacoolie.utils.identity import (  # noqa: E402
    normalize_document_workspace,
    normalize_workspace_id,
)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_metadata(json_path: str | list[str], *, connections_path=None,
                   schema_hints_path=None, workspace_id: str | None = None) -> Dict[str, Any]:
    raw = load_metadata_files(json_path, connections_path=connections_path,
                              schema_hints_path=schema_hints_path, root=SIM_ROOT)

    raw = normalize_document_workspace(raw, default_workspace=workspace_id)
    connection_models = build_connections(raw.get("connections", []))
    connection_payloads = {
        model.connection_id: model.model_dump() for model in connection_models
    }
    # Make explicit endpoint IDs authoritative before the shared mapper sees
    # named refs. This also keeps ID-only simulator documents valid while the
    # mapper remains responsible for normal validation and model construction.
    for flow in raw.get("dataflows", []):
        if not isinstance(flow, dict):
            continue
        for role in ("source", "destination"):
            endpoint = flow.get(role)
            if not isinstance(endpoint, dict):
                continue
            inline = endpoint.get("connection")
            reference = endpoint.get("connection_id") or flow.get(
                f"{role}_connection_id"
            )
            if isinstance(inline, dict):
                reference = reference or inline.get("connection_id")
            if reference and reference in connection_payloads:
                authored_name = endpoint.get("connection_name")
                if isinstance(inline, dict):
                    authored_name = authored_name or inline.get("name")
                owner_name = connection_payloads[reference].get("name")
                if authored_name is not None and authored_name != owner_name:
                    raise ValueError(
                        f"Dataflow {flow.get('name')!r} {role} connection_id "
                        f"{reference!r} conflicts with connection name {authored_name!r}"
                    )
                endpoint["connection"] = copy.deepcopy(connection_payloads[reference])
                endpoint.pop("connection_name", None)
                endpoint.pop("connection_id", None)
    dataflow_models = build_dataflows(raw.get("dataflows", []), connection_models)
    grouped_hints = build_grouped_schema_hints(raw.get("schema_hints", []), connection_models)

    connections = [model.model_dump() for model in connection_models]
    connection_by_id = {row["connection_id"]: row for row in connections}
    dataflows = [model.model_dump() for model in dataflow_models]
    schema_hints: List[Dict[str, Any]] = []
    for (connection_id, schema_name, table_name), hints in grouped_hints.items():
        connection = connection_by_id.get(connection_id, {})
        for hint in hints:
            schema_hints.append({
                **hint.model_dump(),
                "connection_id": connection_id,
                "workspace_id": connection.get("workspace_id"),
                "table_name": table_name,
                "schema_name": schema_name,
            })

    return {
        "connections": connections,
        "conn_map": connection_by_id,
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

def create_app(json_path: str | list[str], watermark_dir: Optional[Path] = None,
               *, connections_path=None, schema_hints_path=None,
               workspace_id: str | None = None) -> Flask:
    app = Flask(__name__)
    meta = _load_metadata(json_path, connections_path=connections_path,
                          schema_hints_path=schema_hints_path,
                          workspace_id=workspace_id)
    bound_workspace = normalize_workspace_id(workspace_id)
    if bound_workspace is None:
        unscoped = [
            row for section in ("connections", "dataflows")
            for row in meta[section]
            if normalize_workspace_id(row.get("workspace_id")) is None
        ]
        if unscoped:
            raise ValueError(
                "workspace_id is required when the metadata document is unscoped"
            )

    if watermark_dir is None:
        # Keep generated API-provider state outside the metadata source tree.
        # ``metadata/api`` remains source code and fixtures; the simulator's
        # mutable state has one explicit runtime boundary.
        watermark_dir = (
            Path(__file__).resolve().parents[2]
            / ".runtime"
            / "watermarks"
            / "api"
        )
    watermark_dir.mkdir(parents=True, exist_ok=True)
    app.logger.info("Watermarks persisted under: %s", watermark_dir)

    def _wm_path(workspace: str, dataflow_id: str) -> Path:
        # Keep state separated by route workspace even for explicit IDs.
        return watermark_dir / workspace / f"{dataflow_id}.json"

    def _load_wm(workspace: str, dataflow_id: str) -> Optional[Dict[str, Any]]:
        p = _wm_path(workspace, dataflow_id)
        if not p.exists():
            return None
        try:
            with p.open("r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return None

    def _save_wm(workspace: str, dataflow_id: str, wm: Dict[str, Any]) -> None:
        p = _wm_path(workspace, dataflow_id)
        p.parent.mkdir(parents=True, exist_ok=True)
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

        items = [c for c in meta["connections"] if c.get("workspace_id") == workspace_id]
        if active_only:
            items = [c for c in items if c.get("is_active", True)]
        if name_filter:
            items = [c for c in items if c.get("name") == name_filter]

        return jsonify(_paginate(items, page, page_size))

    @app.route("/workspaces/<workspace_id>/connections/<connection_id>", methods=["GET"])
    def get_connection(workspace_id: str, connection_id: str):
        for c in meta["connections"]:
            if c.get("workspace_id") == workspace_id and c.get("connection_id") == connection_id:
                return jsonify(c)
        return jsonify({"error": "not found"}), 404

    @app.route("/workspaces/<workspace_id>/dataflows", methods=["GET"])
    def list_dataflows(workspace_id: str):
        page = request.args.get("page", 1, type=int)
        page_size = request.args.get("page_size", 50, type=int)
        stage_filter = request.args.get("stage")
        active_only = request.args.get("active_only", "true").lower() == "true"

        items = [d for d in meta["dataflows"] if d.get("workspace_id") == workspace_id]
        if active_only:
            items = [d for d in items if d.get("is_active", True)]
        if stage_filter:
            stages = [s.strip() for s in stage_filter.split(",")]
            items = [d for d in items if d.get("stage") in stages]

        return jsonify(_paginate(items, page, page_size))

    @app.route("/workspaces/<workspace_id>/dataflows/<dataflow_id>", methods=["GET"])
    def get_dataflow(workspace_id: str, dataflow_id: str):
        for d in meta["dataflows"]:
            if d.get("workspace_id") == workspace_id and d.get("dataflow_id") == dataflow_id:
                return jsonify(d)
        return jsonify({"error": "not found"}), 404

    @app.route("/workspaces/<workspace_id>/schema-hints", methods=["GET"])
    def list_schema_hints(workspace_id: str):
        page = request.args.get("page", 1, type=int)
        page_size = request.args.get("page_size", 50, type=int)
        conn_id = request.args.get("connection_id")
        table = request.args.get("table_name")
        schema_name = request.args.get("schema_name")

        items = [h for h in meta["schema_hints"] if h.get("workspace_id") == workspace_id]
        if conn_id:
            items = [h for h in items if h.get("connection_id") == conn_id]
        if table:
            items = [h for h in items if h.get("table_name") == table]
        if schema_name:
            items = [h for h in items if h.get("schema_name") == schema_name]

        return jsonify(_paginate(items, page, page_size))

    @app.route("/workspaces/<workspace_id>/watermarks/<dataflow_id>", methods=["GET"])
    def get_watermark(workspace_id: str, dataflow_id: str):
        valid = any(
            d.get("workspace_id") == workspace_id and d.get("dataflow_id") == dataflow_id
            for d in meta["dataflows"]
        )
        if not valid:
            return jsonify({"current_value": None})
        wm = _load_wm(workspace_id, dataflow_id)
        if wm is None:
            return jsonify({"current_value": None})
        return jsonify(wm)

    @app.route("/workspaces/<workspace_id>/watermarks/<dataflow_id>", methods=["PUT"])
    def update_watermark(workspace_id: str, dataflow_id: str):
        body = request.get_json(force=True)
        valid = any(
            d.get("workspace_id") == workspace_id and d.get("dataflow_id") == dataflow_id
            for d in meta["dataflows"]
        )
        if not valid:
            return jsonify({"error": "not found"}), 404
        existing = _load_wm(workspace_id, dataflow_id) or {}
        wm = {
            "dataflow_id": dataflow_id,
            "current_value": body.get("current_value"),
            "previous_value": existing.get("current_value"),
            "job_id": body.get("job_id"),
            "dataflow_run_id": body.get("dataflow_run_id"),
        }
        _save_wm(workspace_id, dataflow_id, wm)
        return jsonify({"status": "ok"})

    return app


def main():
    parser = argparse.ArgumentParser(description="DataCoolie simulated metadata API server")
    parser.add_argument(
        "--json-path",
        action="append",
        help="Exact source JSON metadata file; repeat for multiple groups",
    )
    parser.add_argument("--connections-path", help="Exact shared connections file")
    parser.add_argument("--schema-hints-path", help="Exact shared schema hints file")
    parser.add_argument(
        "--workspace-id", default="local-workspace",
        help="Default workspace for an unscoped metadata document",
    )
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument(
        "--watermark-dir",
        default=str(
            Path(__file__).resolve().parents[2]
            / ".runtime"
            / "watermarks"
            / "api"
        ),
        help="Directory where watermarks are persisted (one JSON file per dataflow_id)",
    )
    args = parser.parse_args()

    paths = args.json_path or [str(SIM_ROOT / "scenarios/workflows/local_use_cases.json")]
    app = create_app(paths, watermark_dir=Path(args.watermark_dir),
                     connections_path=args.connections_path,
                     schema_hints_path=args.schema_hints_path,
                     workspace_id=args.workspace_id)
    print(f"Starting metadata API server on {args.host}:{args.port}")
    print(f"  Source:     {args.json_path}")
    print(f"  Watermarks: {args.watermark_dir}")
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
