"""Canonical REST adapter and actual Driver qualification on fresh test tables."""
from __future__ import annotations

from dataclasses import replace
import json
import hashlib
import os
from pathlib import Path
import tempfile
from uuid import uuid4


def main():
    from pyspark.sql import SparkSession
    from datacoolie.core.models.connection import Connection
    from datacoolie.core.models.destination import Destination
    from datacoolie.core.models.source import Source
    from datacoolie.core.models.dataflow import DataFlow
    from datacoolie.core.models.run_config import DataCoolieRunConfig
    from datacoolie.engines.contracts.maintenance import MaintenanceTableAddress, RestMaintenanceDeployment
    from datacoolie.engines.spark_engine import SparkEngine
    from datacoolie.platforms.local_platform import LocalPlatform
    from datacoolie.orchestration.driver import DataCoolieDriver
    from datacoolie.core.exceptions import ConfigurationError

    manifest = Path(os.environ["DATACOOLIE_GUARD_MANIFEST"])
    product = Path(__file__).resolve().parents[2]
    dependencies = product / "usecase-sim/.runtime/dependencies/spark/spark35/jars"
    if not dependencies.is_dir():
        dependencies = Path("/tmp/datacoolie-usecase-sim/dependencies/spark35/jars")
    jars = [manifest.with_name("guard.jar"),
            dependencies / "org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.10.1.jar",
            dependencies / "software.amazon.awssdk_bundle-2.29.51.jar"]
    probe_sources = sorted((product / "tests/support/native_iceberg").glob("*.java"))
    probe_fingerprint = hashlib.sha256(b"".join(path.read_bytes() for path in probe_sources)).hexdigest()
    import shutil
    probe_dir = product / "usecase-sim/.runtime/dependencies/native-iceberg-qualification" / probe_fingerprint
    # Spark requires unique basenames for distributed JARs.
    shutil.copyfile(probe_dir / "guard.jar", probe_dir / "qualification-probes.jar")
    jars.append(probe_dir / "qualification-probes.jar")
    builder = (SparkSession.builder.master("local[1]").appName("REST-maintenance-driver-qualification")
        .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.jars", ",".join(map(str, jars))))
    for key, value in {"": "org.apache.iceberg.spark.SparkCatalog", "type": "rest",
        "uri": "http://iceberg-rest:8181", "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "s3.endpoint": "http://minio:9000", "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin", "s3.path-style-access": "true",
        "client.region": "us-east-1", "cache-enabled": "false"}.items():
        builder = builder.config("spark.sql.catalog.catalog" + (f".{key}" if key else ""), value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    namespace = "maintenance_driver_" + uuid4().hex
    load = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable
    platform = LocalPlatform()
    driver = None
    try:
        spark.sql(f"CREATE NAMESPACE catalog.{namespace}")
        for name in ("sentinel", "Orders", "orders"):
            spark.sql(f"CREATE TABLE catalog.{namespace}.{name} (id BIGINT) USING iceberg")
        full_name = f"catalog.{namespace}.Orders"
        for value in range(6):
            spark.sql(f"INSERT INTO {full_name} VALUES ({value})")
        sentinel = load(spark._jsparkSession, f"catalog.{namespace}.sentinel")
        deployment = RestMaintenanceDeployment(("qualified-docker", "datacoolie-postgres-catalog-instance"),
            ("http://iceberg-rest:8181",), "s3://datacoolie-test/iceberg-warehouse",
            (namespace, "sentinel"), str(sentinel.uuid()), "catalog")
        engine = SparkEngine(spark_session=spark, platform=platform,
            maintenance_deployment=deployment, maintenance_guard_manifest=manifest)
        spark.sql(f"CREATE NAMESPACE catalog.catalog.{namespace}")
        spark.sql(f"CREATE TABLE catalog.catalog.{namespace}.Orders (id BIGINT) USING iceberg")
        nested = engine.prepare_maintenance_target(fmt="iceberg", table_name=f"catalog.{namespace}.Orders",
            table_address=MaintenanceTableAddress(None, ("catalog", namespace), "Orders"))
        assert nested.expected_uuid == str(load(spark._jsparkSession, f"catalog.catalog.{namespace}.Orders").uuid())
        try:
            engine.prepare_maintenance_target(fmt="iceberg", table_name=f"other.{namespace}.Orders",
                table_address=MaintenanceTableAddress("other", (namespace,), "Orders"))
        except ConfigurationError:
            pass
        else:
            raise AssertionError("Foreign catalog admitted for maintenance")
        prepare = engine.maintenance_preparation()
        address = MaintenanceTableAddress("catalog", (namespace,), "Orders")
        target = prepare(fmt="iceberg", table_name=full_name, table_address=address)
        alias = prepare(fmt="iceberg", table_name=f"`catalog`.`{namespace}`.`Orders`", table_address=address)
        lower = prepare(fmt="iceberg", table_name=f"catalog.{namespace}.orders",
            table_address=replace(address, name="orders"))
        assert target.identity == alias.identity and lower.identity != target.identity
        assert prepare(fmt="iceberg", table_name=f"catalog.{namespace}.missing",
            table_address=replace(address, name="missing")).missing
        try:
            SparkEngine(spark_session=spark, maintenance_deployment=replace(deployment, sentinel_uuid=str(uuid4())),
                maintenance_guard_manifest=manifest).prepare_maintenance_target(fmt="iceberg", table_name=full_name,
                    table_address=address)
        except ConfigurationError:
            pass
        else:
            raise AssertionError("Changed sentinel was accepted")
        connection = Connection(name="native", connection_type="lakehouse", format="iceberg",
            configure={"catalog": "catalog", "database": namespace})
        def flow(identifier, name="Orders"):
            return DataFlow(dataflow_id=identifier, source=Source(connection=connection, table="unused"),
                destination=Destination(connection=connection, table=name))
        before_files = spark.sql(f"SELECT file_path FROM {full_name}.files").collect()
        actions = []
        execute = engine.execute_maintenance_action
        def observed(prepared, **kwargs):
            detail = execute(prepared, **kwargs)
            actions.append({"uuid": prepared.expected_uuid, **detail})
            return detail
        engine.execute_maintenance_action = observed
        logs = tempfile.mkdtemp(prefix="datacoolie-rest-maintenance-")
        driver = DataCoolieDriver(engine=engine, platform=platform,
            config=DataCoolieRunConfig(retry_count=0), log_base_path=logs)
        result = driver.run_maintenance(dataflows=[flow("b"), flow("a")])
        assert (result.total, result.succeeded, result.failed) == (1, 1, 0), result
        assert [item["action"] for item in actions] == ["rewrite_data_files", "rewrite_position_delete_files",
            "rewrite_manifests", "expire_snapshots", "remove_orphan_files"]
        assert actions[0]["rewritten_data_files"] == len(before_files) == 6
        assert len(spark.sql(f"SELECT file_path FROM {full_name}.files").collect()) == 1
        assert [row.id for row in spark.table(full_name).orderBy("id").collect()] == list(range(6))
        assert all(item["uuid"] == target.expected_uuid for item in actions)
        delete_name = f"catalog.{namespace}.position_delete_effect"
        spark.sql(f"CREATE TABLE {delete_name} (id BIGINT) USING iceberg "
                  "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')")
        for value in range(5):
            spark.sql(f"INSERT INTO {delete_name} VALUES ({value * 10}), ({value * 10 + 1}), ({value * 10 + 2})")
        for value in range(5):
            spark.sql(f"DELETE FROM {delete_name} WHERE id = {value * 10}")
        delete_before = spark.sql(f"SELECT file_path FROM {delete_name}.delete_files").collect()
        assert len(delete_before) == 5
        delete_prepared = prepare(fmt="iceberg", table_name=delete_name,
            table_address=replace(address, name="position_delete_effect"))
        delete_result = execute(delete_prepared, action="rewrite_position_delete_files")
        delete_after = spark.sql(f"SELECT file_path FROM {delete_name}.delete_files").collect()
        assert delete_result["status"] == "succeeded" and delete_result["rewritten_delete_files"] == 5
        assert len(delete_after) == delete_result["added_delete_files"]
        assert {row.file_path for row in delete_before}.isdisjoint(row.file_path for row in delete_after)
        assert [row.id for row in spark.table(delete_name).orderBy("id").collect()] == [
            value * 10 + offset for value in range(5) for offset in (1, 2)]
        # Native expiration commits but every injected delete callback fails.
        # Its suppressed callback errors must still fail with honest zero counts.
        failure_name = f"catalog.{namespace}.delete_failure"
        spark.sql(f"CREATE TABLE {failure_name} (id BIGINT) USING iceberg")
        for value in (1, 2, 3):
            spark.sql(f"INSERT INTO {failure_name} VALUES ({value})")
        failing_native = load(spark._jsparkSession, failure_name)
        probe = spark._jvm.org.datacoolie.qualification.FailureProbe(failing_native)
        failing_guard = spark._jvm.org.datacoolie.iceberg.GuardedTableOperations.bind(
            probe.nativeTable(), str(failing_native.uuid()))
        from time import time
        from datacoolie.engines._spark.iceberg.maintenance import _plain
        try:
            spark._jvm.org.datacoolie.iceberg.NativeMaintenance.execute(
                failing_guard, spark._jsparkSession, "expire_snapshots", int(time() * 1000) + 1000)
        except Exception as error:
            partial = _plain(error.java_exception.detail())
            assert partial["status"] == "failed" and partial["delete_callback_error_count"] > 0
            assert partial["confirmed_deleted_count"] == partial["metrics"]["files_removed"] == 0
            assert sum(partial[key] for key in partial if key.startswith("deleted_")) == 0
        else:
            raise AssertionError("Suppressed native deletion errors became success")
        assert spark.sql(f"SELECT COUNT(*) n FROM {failure_name}.snapshots").first().n == 1
        assert [row.id for row in spark.table(failure_name).orderBy("id").collect()] == [1, 2, 3]
        diagnostic_name = f"catalog.{namespace}.diagnostic_failure"
        spark.sql(f"CREATE TABLE {diagnostic_name} (id BIGINT) USING iceberg")
        for value in (1, 2, 3):
            spark.sql(f"INSERT INTO {diagnostic_name} VALUES ({value})")
        diagnostic_native = load(spark._jsparkSession, diagnostic_name)
        diagnostic_probe = spark._jvm.org.datacoolie.qualification.FailureProbe(diagnostic_native, True)
        diagnostic_guard = spark._jvm.org.datacoolie.iceberg.GuardedTableOperations.bind(
            diagnostic_probe.nativeTable(), str(diagnostic_native.uuid()))
        diagnostic_result = _plain(spark._jvm.org.datacoolie.iceberg.NativeMaintenance.execute(
            diagnostic_guard, spark._jsparkSession, "expire_snapshots", int(time() * 1000) + 1000))
        assert diagnostic_result["status"] == "succeeded" and "post_snapshot_diagnostic" in diagnostic_result
        assert diagnostic_result["delete_callback_error_count"] == 0
        assert spark.sql(f"SELECT COUNT(*) n FROM {diagnostic_name}.snapshots").first().n == 1
        assert [row.id for row in spark.table(diagnostic_name).orderBy("id").collect()] == [1, 2, 3]
        # Native prefix listing uses genuine S3 object timestamps. A fixed
        # test-owned UTC cutoff between uploads proves old/recent eligibility.
        orphan_name = f"catalog.{namespace}.orphan_effect"
        spark.sql(f"CREATE TABLE {orphan_name} (id BIGINT) USING iceberg")
        spark.sql(f"INSERT INTO {orphan_name} VALUES (701), (702)")
        orphan_native = load(spark._jsparkSession, orphan_name)
        orphan_guard = spark._jvm.org.datacoolie.iceberg.GuardedTableOperations.bind(
            orphan_native, str(orphan_native.uuid()))
        io = orphan_native.io()
        old_path = str(orphan_native.location()) + "/data/old-orphan.bin"
        recent_path = str(orphan_native.location()) + "/data/recent-orphan.bin"
        stream = io.newOutputFile(old_path).createOrOverwrite()
        stream.write(17)
        stream.close()
        def inventory():
            iterator = io.listPrefix(str(orphan_native.location()) + "/").iterator()
            result = {}
            while iterator.hasNext():
                item = iterator.next()
                result[str(item.location())] = int(item.createdAtMillis())
            return result
        old_time = inventory()[old_path]
        from time import sleep
        sleep(2)
        stream = io.newOutputFile(recent_path).createOrOverwrite()
        stream.write(23)
        stream.close()
        before_orphan = inventory()
        orphan_cutoff = old_time + 1
        assert before_orphan[recent_path] > orphan_cutoff
        orphan_result = _plain(spark._jvm.org.datacoolie.iceberg.NativeMaintenance.execute(
            orphan_guard, spark._jsparkSession, "remove_orphan_files", orphan_cutoff))
        after_orphan = inventory()
        assert orphan_result["status"] == "succeeded" and orphan_result["confirmed_deleted_count"] == 1
        assert set(before_orphan) - set(after_orphan) == {old_path}
        assert recent_path in after_orphan
        assert [row.id for row in spark.table(orphan_name).orderBy("id").collect()] == [701, 702]
        before_noop = str(load(spark._jsparkSession, full_name).operations().current().metadataFileLocation())
        zero = driver.run_maintenance(dataflows=[flow("no-work")],
            compact_options={key: False for key in ("rewrite_data_files", "rewrite_position_delete_files", "rewrite_manifests")},
            cleanup_options={key: False for key in ("expire_snapshots", "remove_orphan_files")})
        assert zero.succeeded == 1 and len(actions) == 5
        assert str(load(spark._jsparkSession, full_name).operations().current().metadataFileLocation()) == before_noop
        # Replacement after prepare must reject without changing its rows or metadata.
        spark.sql(f"DROP TABLE {full_name}")
        spark.sql(f"CREATE TABLE {full_name} (id BIGINT) USING iceberg")
        spark.sql(f"INSERT INTO {full_name} VALUES (901), (902)")
        replacement = load(spark._jsparkSession, full_name)
        replacement_metadata = str(replacement.operations().current().metadataFileLocation())
        try:
            execute(target, action="rewrite_data_files")
        except Exception as error:
            assert "UUID" in str(error), str(error)
        else:
            raise AssertionError("Stale prepared incarnation executed")
        assert str(load(spark._jsparkSession, full_name).operations().current().metadataFileLocation()) == replacement_metadata
        assert [row.id for row in spark.table(full_name).orderBy("id").collect()] == [901, 902]
        driver.close()
        records = [json.loads(line) for path in Path(logs).rglob("*.json") for line in path.read_text().splitlines()]
        runs = [item for item in records if item.get("_type") == "dataflow_run_log"]
        assert len(runs) == 2 and all(item["status"] == "succeeded" for item in runs)
        print("SPARK_REST_DRIVER=" + json.dumps({"namespace": namespace, "expected_uuid": target.expected_uuid,
            "replacement_uuid": str(replacement.uuid()), "actions": actions,
            "deduped_invocations": 1, "log_runs": len(runs), "rows_preserved": True,
            "callback_failure_detail": partial,
            "position_delete_effect": {"action": delete_result,
                "delete_files_before": len(delete_before), "delete_files_after": len(delete_after), "rows_preserved": True},
            "post_action_diagnostic": diagnostic_result,
            "orphan_effect": {"action": orphan_result, "cutoff_ms": orphan_cutoff,
                "old_created_at_ms": old_time, "recent_created_at_ms": before_orphan[recent_path],
                "removed_paths": sorted(set(before_orphan) - set(after_orphan)), "recent_preserved": True},
            "zero_selection_unchanged": True, "replacement_unchanged": True, "logs": logs}, sort_keys=True))
    finally:
        if driver is not None:
            driver.close()
        # Driver borrowed the session and must leave it usable.
        assert spark.range(1).count() == 1
        spark.stop()


if __name__ == "__main__":
    main()
