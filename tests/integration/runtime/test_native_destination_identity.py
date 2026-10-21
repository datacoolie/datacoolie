"""Native physical-identity evidence. All records/catalogs are synthetic."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.runtime_qualification]


def test_pyiceberg_sql_catalog_case_prefix_and_catalog_scope(tmp_path):
    pytest.importorskip("pyiceberg")
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField
    from datacoolie.engines._polars.iceberg.schema import table_id

    schema = Schema(NestedField(1, "id", LongType(), required=False))
    catalogs = []
    for name in ("first", "second"):
        warehouse = tmp_path / name
        warehouse.mkdir()
        warehouse_uri = warehouse.as_uri()
        if os.name == "nt":
            warehouse_uri = "file://" + warehouse_uri.removeprefix("file:///")
        catalog = SqlCatalog(name, uri=f"sqlite:///{(tmp_path / (name + '.db')).as_posix()}",
                             warehouse=warehouse_uri)
        catalogs.append(catalog)
        catalog.create_namespace("ns")
        catalog.create_table("ns.Orders", schema=schema)
        catalog.create_table("ns.orders", schema=schema)
    first, second = catalogs
    upper = first.load_table("ns.Orders")
    lower = first.load_table("ns.orders")
    assert upper.metadata.table_uuid != lower.metadata.table_uuid
    for alias in ("ns.Orders", "first.ns.Orders", "authored_alias.ns.Orders"):
        assert first.load_table(table_id(alias)).metadata.table_uuid == upper.metadata.table_uuid
    assert second.load_table("ns.Orders").metadata.table_uuid != upper.metadata.table_uuid
    from datacoolie.engines.polars_engine import PolarsEngine
    engine = PolarsEngine(iceberg_catalog=first)
    keys = [engine.target_name_identity(fmt="iceberg", table_name=name) for name in (
        "ns.Orders", "first.ns.Orders", "authored_alias.ns.Orders", "ns.orders")]
    assert keys[0] is not None and keys[0] == keys[1] == keys[2] != keys[3]
    assert PolarsEngine(iceberg_catalog=second).target_name_identity(fmt="iceberg", table_name="ns.Orders") != keys[0]
    # Properties may drift; projection follows the attached operative engine.
    first.properties["uri"] = "sqlite:///:memory:"
    assert engine.target_name_identity(fmt="iceberg", table_name="ns.Orders") == keys[0]
    for memory in (SqlCatalog("same", uri="sqlite:///:memory:"), SqlCatalog("same", uri="sqlite:///:memory:")):
        assert PolarsEngine(iceberg_catalog=memory).target_name_identity(fmt="iceberg", table_name="ns.Orders") is None
        memory.engine.dispose()
    from types import SimpleNamespace
    from unittest.mock import patch
    from sqlalchemy.engine import make_url
    from datacoolie.engines._polars.iceberg.schema import sqlite_name_identity
    rejected = ["sqlite:///relative.db", "sqlite:////tmp/alias/../catalog.db"]
    if os.name == "nt":
        rejected.append("sqlite:////catalog.db")
    for url in rejected:
        with patch.object(first, "engine", SimpleNamespace(url=make_url(url))):
            assert sqlite_name_identity(first, "ns.Orders") is None
    database = first.engine.url.database
    with patch.object(first, "engine", SimpleNamespace(url=make_url(f"sqlite:///{database.upper()}"))):
        assert sqlite_name_identity(first, "ns.Orders") != keys[0]
    # Aliases are scheduled once; native table probes/history stay real while
    # observing Polars' unsupported compaction hook (no compaction claim).
    from datacoolie.core.models.connection import Connection
    from datacoolie.core.models.dataflow import DataFlow
    from datacoolie.core.models.destination import Destination
    from datacoolie.core.models.source import Source
    from datacoolie.destinations.iceberg_writer import IcebergWriter
    from datacoolie.orchestration.maintenance import dedupe_by_destination
    flows = [DataFlow(dataflow_id=str(index), source=Source(connection=Connection(name="unused", format="iceberg"), table="unused"),
        destination=Destination(connection=Connection(name=prefix, format="iceberg", catalog=prefix), schema_name="ns", table=name))
        for index, (prefix, name) in enumerate((("first", "Orders"), ("first", "orders"), ("authored_alias", "Orders")))]
    unique = dedupe_by_destination(flows, engine=engine)
    assert len(unique) == 2
    with patch.object(engine, "compact_by_name", wraps=engine.compact_by_name) as compact:
        for flow in unique:
            assert IcebergWriter(engine).run_maintenance(flow, do_compact=True, do_cleanup=False).status == "succeeded"
    assert [call.args[0] for call in compact.call_args_list] == [flow.destination.full_table_name for flow in unique]
    assert first.load_table("ns.Orders").metadata.table_uuid == upper.metadata.table_uuid
    assert first.load_table("ns.orders").metadata.table_uuid == lower.metadata.table_uuid
    for catalog in catalogs:
        catalog.engine.dispose()


@pytest.mark.parametrize("selection", ["explicit", "metadata"])
def test_native_polars_delta_two_paths_write_and_maintenance_identity(tmp_path, selection):
    import polars as pl
    from unittest.mock import patch
    from datacoolie.core.models.connection import Connection
    from datacoolie.core.models.destination import Destination
    from datacoolie.core.models.dataflow import DataFlow
    from datacoolie.core.models.source import Source
    from datacoolie.core.models.run_config import DataCoolieRunConfig
    from datacoolie.destinations.delta_writer import DeltaWriter
    from datacoolie.destinations.resolution.target import resolve_destination_target
    from datacoolie.engines.polars_engine import PolarsEngine
    from datacoolie.metadata.file_provider import FileProvider
    from datacoolie.orchestration.driver import DataCoolieDriver
    from datacoolie.platforms.local_platform import LocalPlatform

    engine = PolarsEngine()
    source = Connection(name="source", connection_type="file", format="parquet", configure={"base_path": str(tmp_path / "source")})
    connections = [Connection(name=name, format="delta", catalog="logical", database="db",
        configure={"base_path": str(tmp_path / root)}) for name, root in (("left", "left"), ("right", "right"), ("alias", "left"))]
    flows = [DataFlow(dataflow_id=name, source=Source(connection=source, table="input"),
        destination=Destination(connection=conn, table="orders", load_type="append")) for name, conn in zip(("a-left", "b-right", "z-alias"), connections)]
    for flow, value in zip(flows[:2], (101, 202)):
        DeltaWriter(engine).write(pl.DataFrame({"id": [value]}).lazy(), flow)
    targets = [resolve_destination_target(flow.destination, engine=engine, require_identity=True) for flow in flows]
    assert targets[0].identity == targets[2].identity != targets[1].identity
    assert [engine.read_delta(target.path).collect().to_dicts() for target in targets[:2]] == [[{"id": 101}], [{"id": 202}]]
    metadata = tmp_path / "metadata.json"
    metadata.write_text(json.dumps({"connections": [{"name": c.name, "connection_type": c.connection_type,
        "format": c.format, "catalog": c.catalog, "database": c.database, "configure": c.configure}
        for c in [source] + connections],
        "dataflows": [{"dataflow_id": f.dataflow_id, "source": {"connection_name": "source", "table": "input"},
            "destination": {"connection_name": c.name, "table": "orders", "load_type": "append"}}
            for f, c in zip(flows, connections)]}), encoding="utf-8")
    platform = LocalPlatform()
    provider = FileProvider(config_path=str(metadata), platform=platform)
    with DataCoolieDriver(engine=engine, platform=platform, metadata_provider=provider,
        state_base_path=str(tmp_path / "state"), log_base_path=str(tmp_path / "logs"),
        config=DataCoolieRunConfig(retry_count=0, retry_delay=0, max_workers=1)) as driver:
        with patch.object(engine, "compact_by_path", wraps=engine.compact_by_path) as compact:
            result = driver.run_maintenance(dataflows=flows if selection == "explicit" else None, do_cleanup=False)
        assert (result.total, result.succeeded, result.failed) == (2, 2, 0), result.errors
        assert [c.args[0] for c in compact.call_args_list] == [t.path for t in targets[:2]]
    assert [engine.read_delta(target.path).collect().to_dicts() for target in targets[:2]] == [[{"id": 101}], [{"id": 202}]]


@pytest.mark.spark
@pytest.mark.xdist_group("spark")
def test_spark35_isolated_delta_and_hadoop_catalog_identity():
    from tests.support.datatype_config import require_docker_services

    require_docker_services(("spark",))
    result = subprocess.run(["docker", "exec", "-w", "/datacoolie", "datacoolie-spark",
        "python3", "/datacoolie/tests/support/spark_destination_identity_worker.py"],
        capture_output=True, text=True, timeout=240, cwd=Path(__file__).resolve().parents[3])
    assert result.returncode == 0, result.stdout + result.stderr
    observation = json.loads(result.stdout.splitlines()[-1])
    assert observation["status"] == "passed", observation
    assert observation["spark_version"].startswith("3.5."), observation
    print(json.dumps(observation, sort_keys=True))
