"""Opt-in observation qualification against fresh local REST test tables."""
import json
import os
import subprocess
from uuid import uuid4

import pytest

from tests.support.datatype_config import require_docker_services

pytestmark = [pytest.mark.integration, pytest.mark.runtime_qualification]


def test_polars_snapshot_expiration_retains_null_file_measurements():
    require_docker_services(("minio", "iceberg-rest"))
    from pyiceberg.catalog.rest import RestCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField
    import pyarrow as pa
    from datacoolie.core.models.connection import Connection
    from datacoolie.core.models.dataflow import DataFlow
    from datacoolie.core.models.destination import Destination
    from datacoolie.core.models.source import Source
    from datacoolie.destinations.iceberg_writer import IcebergWriter
    from datacoolie.engines.contracts.maintenance import MaintenanceTableAddress, RestMaintenanceDeployment
    from datacoolie.engines.polars_engine import PolarsEngine
    from datacoolie.logging.persistence.projection import build_dataflow_entry
    from datacoolie.orchestration.execution.runtime import AttemptRuntimeInfo, DataFlowRuntimeInfo
    from datacoolie.orchestration.runtime import JobRuntimeInfo

    catalog = RestCatalog("catalog", uri="http://localhost:8181", **{
        "s3.endpoint": "http://localhost:9000", "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin", "s3.region": "us-east-1"})
    namespace = "observed_polars_" + uuid4().hex
    catalog.create_namespace(namespace)
    table_schema = Schema(NestedField(1, "id", LongType(), required=False))
    sentinel = catalog.create_table((namespace, "sentinel"), schema=table_schema)
    table = catalog.create_table((namespace, "target"), schema=table_schema)
    for value in range(2):
        table.append(pa.table({"id": pa.array([value], type=pa.int64())}))
    deployment = RestMaintenanceDeployment(("qualified-docker", "datacoolie-postgres-catalog-instance"),
        ("http://localhost:8181",), "s3://datacoolie-test/iceberg-warehouse",
        (namespace, "sentinel"), str(sentinel.metadata.table_uuid), "catalog")
    engine = PolarsEngine(iceberg_catalog=catalog, maintenance_deployment=deployment)
    prepared = engine.prepare_maintenance_target(fmt="iceberg", table_name=f"catalog.{namespace}.target",
        table_address=MaintenanceTableAddress("catalog", (namespace,), "target"))
    connection = Connection(name="native", connection_type="lakehouse", format="iceberg",
                            configure={"catalog": "catalog", "database": namespace})
    flow = DataFlow(dataflow_id="polars-v7", source=Source(connection=connection, table="unused"),
                    destination=Destination(connection=connection, table="target"))
    writer = IcebergWriter(engine)
    for expected in (1, 0):
        info = writer.run_selected_maintenance(flow, prepared=prepared, do_compact=False,
            cleanup_options={"expire_snapshots": True, "remove_orphan_files": False}, retention_hours=0)
        assert info.status == "succeeded", info.message
        action = info.operation_details[0]["actions"][0]
        assert action["metrics"]["snapshots_expired"] == expected
        assert action["file_reclamation"] is False
        runtime = DataFlowRuntimeInfo(dataflow_id=flow.dataflow_id, status="succeeded",
            operation_type="maintenance", attempts=[AttemptRuntimeInfo(status="succeeded", destination=info)])
        entry = build_dataflow_entry(flow, runtime, job_info=JobRuntimeInfo(job_id="native-v7"))
        assert all(value is None for value in entry["metrics"].values())
        assert "metric_eligibility" not in entry
    assert sorted(row["id"] for row in catalog.load_table((namespace, "target")).scan().to_arrow().to_pylist()) == [0, 1]


@pytest.mark.spark
def test_spark_observed_zero_and_unentered_maintenance():
    require_docker_services(("spark", "minio", "iceberg-rest"))
    manifest = os.environ["DATACOOLIE_GUARD_MANIFEST"]
    result = subprocess.run(["docker", "exec", "-e", f"DATACOOLIE_GUARD_MANIFEST={manifest}",
        "datacoolie-spark", "python3", "/datacoolie/tests/support/spark_observed_metrics_worker.py"],
        capture_output=True, text=True, timeout=180)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "OBSERVED_METRICS_V7_VERIFIED rows_preserved=true" in result.stdout
    receipt = next(line.split("=", 1)[1] for line in result.stdout.splitlines()
                   if line.startswith("OBSERVED_METRICS_V7="))
    assert len(json.loads(receipt)["runs"]) >= 4
