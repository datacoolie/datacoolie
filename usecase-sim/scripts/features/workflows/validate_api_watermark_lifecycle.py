"""Normal Driver watermark lifecycle against the PostgreSQL-backed API service."""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _stateful_support import (  # noqa: E402
    PolarsEngine,
    assert_exact_rows,
    assert_successful_flow,
    business_rows,
    materialize_fixture,
    read_delta_rows,
    receipt_payload,
    resolve_case_root,
    root_argument,
    run_driver,
    write_parquet,
    write_receipt,
)
from datacoolie.metadata.api_provider import APIProvider  # noqa: E402
from datacoolie.utils.identity import name_to_uuid  # noqa: E402


def _database():
    import psycopg2

    return psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="datacoolie",
        user=os.environ.get("DATACOOLIE_PG_USER", "datacoolie"),
        password=os.environ.get("DATACOOLIE_PG_PASS", "datacoolie"),
    )


def _runtime_flow_id(workspace: str, authored_name: str) -> str:
    """Return the globally unique persisted ID for one workspace/name pair."""

    return name_to_uuid(f"{workspace}:{authored_name}")


def _seed(case, workspace: str, runtime_name: str):
    """Insert only this execution's records into the existing metadata schema."""
    document = case.document
    connections = {row["name"]: row for row in document["connections"]}
    flow = document["dataflows"][0]
    ids = {name: str(uuid.uuid4()) for name in connections}
    flow_id = _runtime_flow_id(workspace, runtime_name)
    with _database() as db, db.cursor() as cursor:
        cursor.execute(
            "SELECT to_regclass('dc_framework_watermarks'), to_regclass('dc_framework_schema_hints')"
        )
        assert all(cursor.fetchone()), (
            "Existing metadata schema is required; do not reinitialize it"
        )
        for name, row in connections.items():
            cursor.execute(
                """INSERT INTO dc_framework_connections
                (connection_id,workspace_id,name,connection_type,format,configure)
                VALUES (%s,%s,%s,%s,%s,%s)""",
                (
                    ids[name],
                    workspace,
                    name,
                    row["connection_type"],
                    row["format"],
                    json.dumps(row["configure"]),
                ),
            )
        source, destination = flow["source"], flow["destination"]
        cursor.execute(
            """INSERT INTO dc_framework_dataflows
            (dataflow_id,workspace_id,name,stage,processing_mode,source_connection_id,
             source_table,source_watermark_columns,destination_connection_id,destination_table,
             destination_load_type,transform,configure)
            VALUES (%s,%s,%s,%s,'batch',%s,%s,%s,%s,%s,%s,'{}','{}')""",
            (
                flow_id,
                workspace,
                runtime_name,
                flow["stage"],
                ids[source["connection_name"]],
                source["table"],
                json.dumps(source["watermark_columns"]),
                ids[destination["connection_name"]],
                destination["table"],
                destination["load_type"],
            ),
        )
    return flow_id


def assert_watermark(payload, current, previous):
    for field, expected in [("current_value", current), ("previous_value", previous)]:
        actual = payload.get(field)
        actual = json.loads(actual) if isinstance(actual, str) else actual
        if actual != expected:
            raise AssertionError(f"{field}: {actual!r} != {expected!r}")


def validate(root=None):
    import httpx

    case = resolve_case_root("workflows", root)
    materialize_fixture(case, "api_watermark_lifecycle.json")
    workspace = str(uuid.uuid4())
    authored = "api-watermark-lifecycle"
    # The authored flow identity is stable; the fresh case root and workspace
    # already isolate this run from prior lifecycle observations.
    runtime_name = authored
    flow_id = _seed(case, workspace, runtime_name)
    url = os.environ.get("DATACOOLIE_METADATA_API_URL", "http://127.0.0.1:8000")
    endpoint = f"{url}/workspaces/{workspace}/watermarks/{flow_id}"
    assert httpx.get(f"{url}/health").json() == {"status": "ok"}
    first = httpx.get(endpoint).json()
    assert_watermark(first, None, None)
    trace = []

    def provider_with_trace():
        provider = APIProvider(
            base_url=url, api_key="", workspace_id=workspace, max_workers=1
        )

        def capture(response):
            trace.append(
                {
                    "method": response.request.method,
                    "path": response.request.url.path,
                    "status": response.status_code,
                }
            )

        provider._client = httpx.Client(
            base_url=url, event_hooks={"response": [capture]}
        )
        provider._client_owned = True
        return provider

    engine = PolarsEngine()
    source = case.input / "rows"
    rows = [
        {"id": 1, "value": "first", "modified_at": datetime(2024, 1, 1, 10)},
        {"id": 2, "value": "second", "modified_at": datetime(2024, 1, 2, 10)},
    ]
    runs, checkpoints = [], []
    for iteration, row in enumerate(rows, 1):
        write_parquet(engine, source, [row])
        provider = provider_with_trace()
        run = run_driver(
            case,
            stage="api_watermark_lifecycle",
            iteration=iteration,
            metadata_provider=provider,
        )
        assert_successful_flow(
            run,
            runtime_name,
            expected_dataflow_id=flow_id,
        )
        runs.append(run)
        checkpoint = httpx.get(endpoint).json()
        expected = {"modified_at": {"__datetime__": row["modified_at"].isoformat()}}
        previous = (
            None
            if iteration == 1
            else {"modified_at": {"__datetime__": rows[0]["modified_at"].isoformat()}}
        )
        assert_watermark(checkpoint, expected, previous)
        fresh = APIProvider(base_url=url, api_key="", workspace_id=workspace)
        try:
            assert json.loads(fresh.get_watermark(flow_id)) == expected
        finally:
            fresh.close()
        with _database() as db, db.cursor() as cursor:
            cursor.execute(
                "SELECT current_value,previous_value FROM dc_framework_watermarks WHERE dataflow_id=%s",
                (flow_id,),
            )
            persisted = cursor.fetchone()
        assert persisted == (checkpoint["current_value"], checkpoint["previous_value"])
        checkpoints.append(checkpoint)
        assert_exact_rows(
            business_rows(read_delta_rows(engine, case.output / "orders")),
            rows[:iteration],
            key="id",
        )
    # Corrupt only this case-owned input. The reader must fail before a target write.
    for path in source.glob("*.parquet"):
        path.write_bytes(b"invalid parquet pre-write failure")
    provider = provider_with_trace()
    failure = run_driver(
        case, stage="api_watermark_lifecycle", iteration=3, metadata_provider=provider
    )
    assert {key: failure.result[key] for key in ("succeeded", "failed", "skipped")} == {
        "succeeded": 0,
        "failed": 1,
        "skipped": 0,
    }
    assert httpx.get(endpoint).json() == checkpoints[-1], (
        "Failure advanced API watermark/history"
    )
    assert_exact_rows(
        business_rows(read_delta_rows(engine, case.output / "orders")), rows, key="id"
    )
    watermark_trace = [
        item for item in trace if item["path"].endswith("/watermarks/" + flow_id)
    ]
    assert [item["method"] for item in watermark_trace].count("PUT") == 2, (
        watermark_trace
    )
    assert [item["method"] for item in watermark_trace].count("GET") >= 3, (
        watermark_trace
    )
    assert all(item["status"] == 200 for item in watermark_trace), watermark_trace
    payload = receipt_payload(
        case,
        runs=[*runs, failure],
        script=Path(__file__),
        details={
            "identity_mapping": {
                authored: {
                    "name": runtime_name,
                    "workspace_id": workspace,
                    "dataflow_id": flow_id,
                    "id_strategy": "name_to_uuid(workspace_id + ':' + authored_name)",
                }
            },
            "workspace": workspace,
            "provenance_id_scope": (
                "metadata_provenance.dataflows identifies the authored source "
                "using its name-derived ID; details.identity_mapping maps that "
                "authored name to the workspace-scoped persisted dataflow_id "
                "used by Driver logs and API watermarks"
            ),
            "backend": "postgresql-backed-metadata-api",
            "checkpoints": checkpoints,
            "sanitized_driver_request_trace": watermark_trace,
            "failure_guard": "invalid-input-before-write",
            "request_contract": "Driver APIProvider GET/PUT; fresh client GET; PostgreSQL readback",
        },
    )
    payload["profile"]["qualification_scope"] = (
        "host-polars-docker-postgresql-metadata-api"
    )
    return write_receipt(case, payload)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    root_argument(parser)
    args = parser.parse_args()
    print(validate(args.root))
