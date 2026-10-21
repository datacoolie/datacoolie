"""Native T2 preparation uses verified scope and the original table incarnation."""
from dataclasses import replace
from uuid import uuid4
import os
import json
import subprocess

import pytest

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.engines.contracts.maintenance import MaintenanceTableAddress, RestMaintenanceDeployment
from tests.support.datatype_config import require_docker_services

pytestmark = [pytest.mark.integration, pytest.mark.runtime_qualification]


@pytest.mark.spark
def test_spark_rest_driver_uses_canonical_guard_and_truthful_native_results():
    require_docker_services(("spark", "minio", "iceberg-rest"))
    manifest = os.getenv("DATACOOLIE_GUARD_MANIFEST")
    assert manifest, "Pass a container-visible explicitly built canonical guard manifest"
    result = subprocess.run(["docker", "exec", "-w", "/datacoolie", "-e",
        f"DATACOOLIE_GUARD_MANIFEST={manifest}", "datacoolie-spark", "python3",
        "tests/support/spark_rest_driver_worker.py"], capture_output=True, text=True, timeout=360)
    assert result.returncode == 0, result.stdout + result.stderr
    receipts = [json.loads(line.split("SPARK_REST_DRIVER=", 1)[1]) for line in result.stdout.splitlines()
                if line.startswith("SPARK_REST_DRIVER=")]
    assert len(receipts) == 1
    receipt = receipts[0]
    assert receipt["deduped_invocations"] == 1 and receipt["v6_runs"] == 2
    assert receipt["replacement_unchanged"] and receipt["rows_preserved"] and receipt["zero_selection_unchanged"]
    assert len(receipt["actions"]) == 5
    partial = receipt["callback_failure_detail"]
    assert partial["status"] == "failed" and partial["confirmed_deleted_count"] == 0
    assert partial["delete_callback_error_count"] > 0
    diagnostic = receipt["post_action_diagnostic"]
    assert diagnostic["status"] == "succeeded" and diagnostic["confirmed_deleted_count"] > 0
    assert "post_snapshot_diagnostic" in diagnostic
    orphan = receipt["orphan_effect"]
    assert orphan["action"]["confirmed_deleted_count"] == len(orphan["removed_paths"]) == 1
    assert orphan["old_created_at_ms"] < orphan["cutoff_ms"] < orphan["recent_created_at_ms"]
    assert orphan["recent_preserved"]
    print("SPARK_REST_DRIVER=" + json.dumps(receipt, sort_keys=True))


def test_polars_prepared_rest_target_keeps_scope_identifier_and_uuid():
    require_docker_services(("minio", "iceberg-rest"))
    import pyarrow as pa
    from pyiceberg.catalog.rest import RestCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField
    from datacoolie.engines.polars_engine import PolarsEngine
    from datacoolie.engines._polars.iceberg.maintenance import validate_target

    catalog = RestCatalog("catalog", uri="http://localhost:8181", **{
        "s3.endpoint": "http://localhost:9000", "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin", "s3.region": "us-east-1"})
    namespace = f"maintenance_prepare_{uuid4().hex}"
    catalog.create_namespace(namespace)
    schema = Schema(NestedField(1, "id", LongType(), required=False))
    sentinel = catalog.create_table((namespace, "sentinel"), schema=schema)
    table = catalog.create_table((namespace, "Orders"), schema=schema)
    deployment = RestMaintenanceDeployment(
        ("qualified-docker", "datacoolie-postgres-catalog-instance"),
        ("http://localhost:8181", "http://iceberg-rest:8181"),
        "s3://datacoolie-test/iceberg-warehouse", (namespace, "sentinel"),
        str(sentinel.metadata.table_uuid), "catalog")
    engine = PolarsEngine(iceberg_catalog=catalog, maintenance_deployment=deployment)
    nested_namespace = ("catalog", namespace)
    catalog.create_namespace(nested_namespace)
    nested = catalog.create_table((*nested_namespace, "Orders"), schema=schema)
    nested_prepared = engine.prepare_maintenance_target(fmt="iceberg", table_name=f"catalog.{namespace}.Orders",
        table_address=MaintenanceTableAddress(None, nested_namespace, "Orders"))
    assert nested_prepared.expected_uuid == str(nested.metadata.table_uuid)
    with pytest.raises(ConfigurationError, match="requested catalog"):
        engine.prepare_maintenance_target(fmt="iceberg", table_name=f"other.{namespace}.Orders",
            table_address=MaintenanceTableAddress("other", (namespace,), "Orders"))
    prepared = engine.prepare_maintenance_target(fmt="iceberg", table_name=f"`catalog`.`{namespace}`.`Orders`",
        table_address=MaintenanceTableAddress("catalog", (namespace,), "Orders"))
    alias = engine.prepare_maintenance_target(fmt="iceberg", table_name=f"{namespace}.Orders")
    assert prepared.identity == alias.identity
    assert prepared.expected_uuid == str(table.metadata.table_uuid)
    assert prepared.capabilities == frozenset({"expire_snapshots"})
    assert validate_target(prepared) is prepared.binding
    before_noop = prepared.binding.metadata.model_dump_json()
    detail = engine.execute_maintenance_action(prepared, action="expire_snapshots")
    assert detail["metrics"] == {"snapshots_expired": 0}
    assert prepared.binding.metadata.model_dump_json() == before_noop
    for value in (101, 102, 103):
        prepared.binding.append(pa.table({"id": pa.array([value], type=pa.int64())}))
    snapshots = [item.snapshot_id for item in prepared.binding.snapshots()]
    prepared.binding.manage_snapshots().create_tag(snapshots[0], "protected").commit()
    # Zero hours is scoped only to this freshly created table to exercise
    # metadata expiration; no files are deleted by PyIceberg.
    import time
    time.sleep(0.01)
    effects = engine.execute_maintenance_action(prepared, action="expire_snapshots", retention_hours=0)
    assert effects["expired_snapshot_ids"] == [snapshots[1]]
    assert effects["file_reclamation"] is False
    assert {item.snapshot_id for item in prepared.binding.snapshots()} == {snapshots[0], snapshots[-1]}
    assert sorted(row["id"] for row in prepared.binding.scan().to_arrow().to_pylist()) == [101, 102, 103]
    with pytest.raises(ConfigurationError, match="support"):
        engine.execute_maintenance_action(prepared, action="remove_orphan_files")
    assert {item.snapshot_id for item in prepared.binding.snapshots()} == {snapshots[0], snapshots[-1]}
    assert engine.prepare_maintenance_target(fmt="iceberg", table_name=f"{namespace}.missing").missing
    with pytest.raises(ConfigurationError, match="binding"):
        PolarsEngine(iceberg_catalog=catalog).prepare_maintenance_target(fmt="iceberg", table_name=f"{namespace}.Orders")
    with pytest.raises(ConfigurationError, match="sentinel"):
        PolarsEngine(iceberg_catalog=catalog, maintenance_deployment=replace(deployment, sentinel_uuid=str(uuid4()))).prepare_maintenance_target(fmt="iceberg", table_name=f"{namespace}.Orders")
    catalog.drop_table((namespace, "Orders"))
    replacement = catalog.create_table((namespace, "Orders"), schema=schema)
    before = replacement.metadata.model_dump_json()
    with pytest.raises(ValueError, match="UUID"):
        validate_target(prepared)
    assert catalog.load_table((namespace, "Orders")).metadata.model_dump_json() == before
    assert prepared.expected_uuid != str(replacement.metadata.table_uuid)


def test_polars_rest_expiration_runs_through_driver_and_persists_truthful_v6(tmp_path):
    require_docker_services(("minio", "iceberg-rest"))
    import json
    import pyarrow as pa
    from pyiceberg.catalog.rest import RestCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField
    from datacoolie.core.models.connection import Connection
    from datacoolie.core.models.destination import Destination
    from datacoolie.core.models.source import Source
    from datacoolie.core.models.dataflow import DataFlow
    from datacoolie.core.models.run_config import DataCoolieRunConfig
    from datacoolie.engines.polars_engine import PolarsEngine
    from datacoolie.platforms.local_platform import LocalPlatform
    from datacoolie.orchestration.driver import DataCoolieDriver
    catalog = RestCatalog("catalog", uri="http://localhost:8181", **{
        "s3.endpoint": "http://localhost:9000", "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin", "s3.region": "us-east-1"})
    namespace = f"maintenance_driver_{uuid4().hex}"
    catalog.create_namespace(namespace)
    schema = Schema(NestedField(1, "id", LongType(), required=False))
    sentinel = catalog.create_table((namespace, "sentinel"), schema=schema)
    table = catalog.create_table((namespace, "Orders"), schema=schema)
    for value in (101, 102, 103):
        table.append(pa.table({"id": pa.array([value], type=pa.int64())}))
    snapshots = [item.snapshot_id for item in table.snapshots()]
    table.manage_snapshots().create_tag(snapshots[0], "protected").commit()
    deployment = RestMaintenanceDeployment(("qualified-docker", "datacoolie-postgres-catalog-instance"),
        ("http://localhost:8181",), "s3://datacoolie-test/iceberg-warehouse",
        (namespace, "sentinel"), str(sentinel.metadata.table_uuid), "catalog")
    connection = Connection(name="native", connection_type="lakehouse", format="iceberg",
                            configure={"catalog": "catalog", "database": namespace})
    flow = DataFlow(dataflow_id="native-driver", source=Source(connection=connection, table="unused"),
                    destination=Destination(connection=connection, table="Orders"))
    platform = LocalPlatform()
    engine = PolarsEngine(platform=platform, iceberg_catalog=catalog, maintenance_deployment=deployment)
    driver = DataCoolieDriver(engine=engine, platform=platform,
        config=DataCoolieRunConfig(retention_hours=0, retry_count=0), log_base_path=str(tmp_path))
    try:
        result = driver.run_maintenance(dataflows=[flow], do_compact=False,
                    cleanup_options={"expire_snapshots": True, "remove_orphan_files": False})
        assert (result.total, result.succeeded, result.failed) == (1, 1, 0)
        after = catalog.load_table((namespace, "Orders"))
        assert {item.snapshot_id for item in after.snapshots()} == {snapshots[0], snapshots[-1]}
        assert sorted(row["id"] for row in after.scan().to_arrow().to_pylist()) == [101, 102, 103]
        before_unsupported = after.metadata.model_dump_json()
        result = driver.run_maintenance(dataflows=[flow])
        assert result.failed == 1
        assert catalog.load_table((namespace, "Orders")).metadata.model_dump_json() == before_unsupported
    finally:
        driver.close()
    records = [json.loads(line) for path in tmp_path.rglob("*.json") for line in path.read_text().splitlines()]
    succeeded = [record for record in records if record.get("_type") == "dataflow_run_log" and record.get("status") == "succeeded"]
    assert len(succeeded) == 1
    destination = succeeded[0]["attempts"][-1]["destination"]
    group = destination["operation_details"][0]
    assert group["operation"] == "cleanup"
    assert group["actions"][0]["expired_snapshot_ids"] == [snapshots[1]]
    assert destination["files_removed"] is None and group["metrics"]["files_removed"] is None
