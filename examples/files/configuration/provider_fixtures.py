"""Run metadata-provider startup and an optional local dataflow fixture.

The default command exercises provider hydration only.  ``--run-dataflow``
adds a small, isolated Parquet source/destination run for each selected
provider.  It uses SQLite and a loopback HTTP service to prove the provider to
Driver handoff without connecting to a production metadata or data service.
"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
from pathlib import Path
from typing import Any


WORKSPACE = "example-workspace"
SOURCE_ID = "source-connection"
DESTINATION_ID = "destination-connection"
DATAFLOW_ID = "orders-dataflow"


def _rows(base_path: Path | None = None) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Return provider rows, optionally rooted under an isolated run folder."""
    input_path = str(base_path / "input") if base_path else "./input"
    output_path = str(base_path / "output") if base_path else "./output"
    source = {
        "connection_id": SOURCE_ID,
        "workspace_id": WORKSPACE,
        "name": "source",
        "connection_type": "file",
        "format": "parquet",
        "configure": {"base_path": input_path},
    }
    destination = {
        "connection_id": DESTINATION_ID,
        "workspace_id": WORKSPACE,
        "name": "destination",
        "connection_type": "file",
        "format": "parquet",
        "configure": {"base_path": output_path},
    }
    dataflow = {
        "dataflow_id": DATAFLOW_ID,
        "workspace_id": WORKSPACE,
        "name": "orders",
        "stage": "bronze2silver",
        "source_connection_id": SOURCE_ID,
        "source_table": "orders",
        "destination_connection_id": DESTINATION_ID,
        "destination_table": "orders",
        "destination_load_type": "overwrite",
    }
    return source, destination, dataflow


def run_sqlite() -> tuple[int, int]:
    """Create and hydrate a workspace-scoped in-memory DatabaseProvider."""
    from sqlalchemy import create_engine, text

    from datacoolie.metadata.database_provider import DatabaseProvider

    source, destination, dataflow = _rows()
    engine = create_engine("sqlite:///:memory:")
    provider = DatabaseProvider(engine=engine, workspace_id=WORKSPACE)
    provider.create_tables()
    with engine.begin() as connection:
        for row in (source, destination):
            connection.execute(
                text(
                    """
                    INSERT INTO dc_framework_connections
                    (connection_id, workspace_id, name, connection_type, format, configure, is_active)
                    VALUES (:connection_id, :workspace_id, :name, :connection_type, :format, :configure, 1)
                    """
                ),
                {**row, "configure": json.dumps(row["configure"])},
            )
        connection.execute(
            text(
                """
                INSERT INTO dc_framework_dataflows
                (dataflow_id, workspace_id, name, stage, source_connection_id, source_table,
                 destination_connection_id, destination_table, destination_load_type, is_active)
                VALUES (:dataflow_id, :workspace_id, :name, :stage, :source_connection_id,
                        :source_table, :destination_connection_id, :destination_table,
                        :destination_load_type, 1)
                """
            ),
            dataflow,
        )
    try:
        provider.initialize()
        return len(provider.get_connections()), len(provider.get_dataflows(stage="bronze2silver"))
    finally:
        provider.close()


def _write_input(base_path: Path) -> None:
    """Create the tiny Parquet input used by the opt-in execution check."""
    import polars as pl

    input_path = base_path / "input" / "orders" / "orders.parquet"
    input_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "order_id": [1, 2, 3],
            "amount": [19.99, 29.00, 5.50],
        }
    ).write_parquet(input_path)


def _execute_provider(
    provider: Any,
    *,
    base_path: Path,
    provider_name: str,
) -> tuple[int, int, int]:
    """Inspect and execute one already-hydrated provider."""
    from datacoolie.core.models.run_config import DataCoolieRunConfig
    from datacoolie.engines.polars_engine import PolarsEngine
    from datacoolie.orchestration.driver import DataCoolieDriver
    from datacoolie.platforms.local_platform import LocalPlatform

    connections = provider.get_connections(active_only=False)
    dataflows = provider.get_dataflows(
        stage="bronze2silver",
        active_only=False,
        attach_schema_hints=False,
    )
    if len(connections) != 2 or len(dataflows) != 1:
        raise RuntimeError(
            f"Unexpected {provider_name} preflight scope: "
            f"connections={len(connections)} dataflows={len(dataflows)}"
        )

    platform = LocalPlatform()
    engine = PolarsEngine(platform=platform)
    with DataCoolieDriver(
        engine=engine,
        platform=platform,
        metadata_provider=provider,
        state_base_path=str(base_path / "state"),
        config=DataCoolieRunConfig(job_id=f"provider-fixture-{provider_name}"),
    ) as driver:
        result = driver.run(stage="bronze2silver")
    return len(connections), len(dataflows), result.succeeded


class _FixtureHandler(BaseHTTPRequestHandler):
    """Serve the minimal paginated APIProvider response contract."""

    fixture_rows: tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None = None

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
        source, destination, dataflow = self.fixture_rows or _rows()
        if self.path.split("?", 1)[0].endswith("/connections"):
            payload = {"data": [source, destination], "pagination": {"page": 1, "total_pages": 1}}
        elif self.path.split("?", 1)[0].endswith("/dataflows"):
            payload = {
                "data": [
                    {
                        "dataflow_id": dataflow["dataflow_id"],
                        "workspace_id": WORKSPACE,
                        "name": dataflow["name"],
                        "stage": dataflow["stage"],
                        "source": {"connection": source, "table": "orders"},
                        "destination": {
                            "connection": destination,
                            "table": "orders",
                            "load_type": "overwrite",
                        },
                        "transform": {},
                    }
                ],
                "pagination": {"page": 1, "total_pages": 1},
            }
        elif self.path.split("?", 1)[0].endswith("/schema-hints"):
            payload = {"data": [], "pagination": {"page": 1, "total_pages": 1}}
        else:
            self.send_error(404)
            return
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


def run_api() -> tuple[int, int]:
    """Hydrate APIProvider from a local HTTP server with no external network."""
    from datacoolie.metadata.api_provider import APIProvider

    _FixtureHandler.fixture_rows = _rows()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FixtureHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        provider = APIProvider(
            base_url=f"http://127.0.0.1:{server.server_port}",
            api_key="fixture-key",
            workspace_id=WORKSPACE,
            max_retries=0,
        )
        try:
            provider.initialize()
            return len(provider.get_connections()), len(provider.get_dataflows(stage="bronze2silver"))
        finally:
            provider.close()
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def run_sqlite_dataflow(base_path: Path) -> tuple[int, int, int]:
    """Run the opt-in Parquet flow through an in-memory DatabaseProvider."""
    from sqlalchemy import create_engine, text

    from datacoolie.metadata.database_provider import DatabaseProvider

    _write_input(base_path)
    source, destination, dataflow = _rows(base_path)
    engine = create_engine("sqlite:///:memory:")
    provider = DatabaseProvider(engine=engine, workspace_id=WORKSPACE)
    provider.create_tables()
    with engine.begin() as connection:
        for row in (source, destination):
            connection.execute(
                text(
                    """
                    INSERT INTO dc_framework_connections
                    (connection_id, workspace_id, name, connection_type, format, configure, is_active)
                    VALUES (:connection_id, :workspace_id, :name, :connection_type, :format, :configure, 1)
                    """
                ),
                {**row, "configure": json.dumps(row["configure"])},
            )
        connection.execute(
            text(
                """
                INSERT INTO dc_framework_dataflows
                (dataflow_id, workspace_id, name, stage, source_connection_id, source_table,
                 destination_connection_id, destination_table, destination_load_type, is_active)
                VALUES (:dataflow_id, :workspace_id, :name, :stage, :source_connection_id,
                        :source_table, :destination_connection_id, :destination_table,
                        :destination_load_type, 1)
                """
            ),
            dataflow,
        )
    try:
        provider.initialize()
        return _execute_provider(provider, base_path=base_path, provider_name="sqlite")
    finally:
        # The provider is injected into Driver, so this fixture owns its lifecycle.
        provider.close()


def run_api_dataflow(base_path: Path) -> tuple[int, int, int]:
    """Run the opt-in Parquet flow through a loopback APIProvider."""
    from datacoolie.metadata.api_provider import APIProvider

    _write_input(base_path)
    _FixtureHandler.fixture_rows = _rows(base_path)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FixtureHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        provider = APIProvider(
            base_url=f"http://127.0.0.1:{server.server_port}",
            api_key="fixture-key",
            workspace_id=WORKSPACE,
            max_retries=0,
        )
        try:
            provider.initialize()
            return _execute_provider(provider, base_path=base_path, provider_name="api")
        finally:
            # The provider is injected into Driver, so this fixture owns its lifecycle.
            provider.close()
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", choices=("sqlite", "api", "both"), default="both")
    parser.add_argument(
        "--run-dataflow",
        action="store_true",
        help="Execute a local Parquet dataflow after provider preflight",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        help="New empty path for --run-dataflow; existing paths are rejected",
    )
    args = parser.parse_args()
    if args.run_dataflow and args.work_dir is None:
        parser.error("--work-dir is required with --run-dataflow")
    if args.run_dataflow and args.work_dir.exists():
        parser.error(f"--work-dir must not already exist: {args.work_dir}")
    if args.run_dataflow:
        args.work_dir.mkdir(parents=True)
    results: dict[str, tuple[int, int]] = {}
    if args.run_dataflow:
        execution_results: dict[str, tuple[int, int, int]] = {}
        if args.provider in {"sqlite", "both"}:
            execution_results["sqlite"] = run_sqlite_dataflow(args.work_dir / "sqlite")
        if args.provider in {"api", "both"}:
            execution_results["api"] = run_api_dataflow(args.work_dir / "api")
        for name, (connections, dataflows, succeeded) in execution_results.items():
            print(
                f"provider={name} executed={succeeded} "
                f"connections={connections} dataflows={dataflows}"
            )
        return 0 if all(value[2] == 1 for value in execution_results.values()) else 1
    if args.provider in {"sqlite", "both"}:
        results["sqlite"] = run_sqlite()
    if args.provider in {"api", "both"}:
        results["api"] = run_api()
    for name, (connections, dataflows) in results.items():
        print(f"provider={name} connections={connections} dataflows={dataflows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
