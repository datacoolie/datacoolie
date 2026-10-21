"""Opt-in native observation qualification on newly created REST test tables."""
from __future__ import annotations

import json
import os
from pathlib import Path
from uuid import uuid4


def main():
    from pyspark.sql import SparkSession
    from datacoolie.core.models.connection import Connection
    from datacoolie.core.models.dataflow import DataFlow
    from datacoolie.core.models.destination import Destination
    from datacoolie.core.models.source import Source
    from datacoolie.destinations.iceberg_writer import IcebergWriter
    from datacoolie.engines.contracts.maintenance import MaintenanceTableAddress, RestMaintenanceDeployment
    from datacoolie.engines.spark_engine import SparkEngine
    from datacoolie.logging.persistence.projection import build_dataflow_entry
    from datacoolie.orchestration.execution.runtime import AttemptRuntimeInfo, DataFlowRuntimeInfo
    from datacoolie.orchestration.runtime import JobRuntimeInfo

    product = Path(__file__).resolve().parents[2]
    manifest = Path(os.environ["DATACOOLIE_GUARD_MANIFEST"])
    dependencies = product / "usecase-sim/.runtime/dependencies/spark/spark35/jars"
    if not dependencies.is_dir():
        dependencies = Path("/tmp/datacoolie-usecase-sim/dependencies/spark35/jars")
    jars = [manifest.with_name("guard.jar"),
            dependencies / "org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.10.1.jar",
            dependencies / "software.amazon.awssdk_bundle-2.29.51.jar"]
    builder = (SparkSession.builder.master("local[1]").appName("observed-metrics-v7")
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
    namespace = "observed_metrics_" + uuid4().hex
    try:
        spark.sql(f"CREATE NAMESPACE catalog.{namespace}")
        for name in ("sentinel", "target"):
            spark.sql(f"CREATE TABLE catalog.{namespace}.{name} (id BIGINT) USING iceberg")
        table = f"catalog.{namespace}.target"
        for value in range(6):
            spark.sql(f"INSERT INTO {table} VALUES ({value})")
        sentinel = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
            spark._jsparkSession, f"catalog.{namespace}.sentinel")
        deployment = RestMaintenanceDeployment(("qualified-docker", "datacoolie-postgres-catalog-instance"),
            ("http://iceberg-rest:8181",), "s3://datacoolie-test/iceberg-warehouse",
            (namespace, "sentinel"), str(sentinel.uuid()), "catalog")
        engine = SparkEngine(spark_session=spark, maintenance_deployment=deployment,
                             maintenance_guard_manifest=manifest)
        prepared = engine.prepare_maintenance_target(fmt="iceberg", table_name=table,
            table_address=MaintenanceTableAddress("catalog", (namespace,), "target"))
        connection = Connection(name="native", connection_type="lakehouse", format="iceberg",
                                configure={"catalog": "catalog", "database": namespace})
        flow = DataFlow(dataflow_id="native-v7", source=Source(connection=connection, table="unused"),
                        destination=Destination(connection=connection, table="target"))
        writer = IcebergWriter(engine)
        observed = []
        compact = {"rewrite_data_files": True, "rewrite_position_delete_files": False, "rewrite_manifests": False}
        cleanup = {"expire_snapshots": False, "remove_orphan_files": True}
        for label, options in [
            ("compact-effect-with-cleanup", dict(compact_options=compact, cleanup_options=cleanup)),
            ("compact-no-work", dict(do_cleanup=False, compact_options=compact)),
            ("cleanup-no-work", dict(do_compact=False, cleanup_options=cleanup)),
            ("disabled", dict(do_compact=False, do_cleanup=False)),
        ]:
            info = writer.run_selected_maintenance(flow, prepared=prepared, **options)
            assert info.status == "succeeded", info.message
            assert info.rows_written is None
            runtime = DataFlowRuntimeInfo(dataflow_id=flow.dataflow_id, status="succeeded",
                operation_type="maintenance", attempts=[AttemptRuntimeInfo(status="succeeded", destination=info)])
            entry = build_dataflow_entry(flow, runtime, job_info=JobRuntimeInfo(job_id="native-v7"))
            observed.append({"label": label, "metrics": entry["metrics"]})
        print("OBSERVED_METRICS_V7=" + json.dumps({"namespace": namespace, "runs": observed}, sort_keys=True))
        assert observed[0]["metrics"]["files_removed"] == 6
        assert observed[0]["metrics"]["files_added"] == 1
        assert observed[1]["metrics"]["files_removed"] == 0
        assert observed[1]["metrics"]["files_added"] == 0
        assert observed[2]["metrics"]["files_added"] is None
        assert observed[2]["metrics"]["files_removed"] == 0
        assert observed[2]["metrics"]["files_added"] is None
        assert all(value is None for value in observed[3]["metrics"].values())
        assert [row.id for row in spark.table(table).orderBy("id").collect()] == list(range(6))
        print("OBSERVED_METRICS_V7_VERIFIED rows_preserved=true")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
