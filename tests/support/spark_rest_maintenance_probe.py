"""T1 native feasibility evidence; this does not qualify Driver maintenance."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4


def main() -> None:
    from pyspark.sql import SparkSession

    product = Path(__file__).resolve().parents[2]
    jars = product / "usecase-sim/.runtime/dependencies/spark/spark35/jars"
    selected = [jars / name for name in (
        "org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.10.1.jar",
        "software.amazon.awssdk_bundle-2.29.51.jar",
    )]
    assert all(path.is_file() for path in selected), selected
    builder = (SparkSession.builder.master("local[1]")
        .appName("iceberg-rest-maintenance-feasibility")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", ",".join(map(str, selected)))
        .config("spark.sql.shuffle.partitions", "1"))
    for alias in ("probe", "alias"):
        options = {
            "": "org.apache.iceberg.spark.SparkCatalog", "type": "rest",
            "uri": os.getenv("DATACOOLIE_ICEBERG_URI", "http://iceberg-rest:8181"),
            "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "s3.endpoint": os.getenv("DATACOOLIE_MINIO_ENDPOINT", "http://minio:9000"),
            "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "s3.path-style-access": "true", "client.region": "us-east-1",
            "cache-enabled": "false",
        }
        for key, value in options.items():
            builder = builder.config(f"spark.sql.catalog.{alias}" + (f".{key}" if key else ""), value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    namespace = f"maintenance_probe_{uuid4().hex}"
    name = f"probe.{namespace}.Orders"
    try:
        assert spark.version == "3.5.9", spark.version
        spark.sql(f"CREATE NAMESPACE probe.{namespace}")
        spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg")
        spark.sql(f"INSERT INTO {name} VALUES (101)")
        # Public JVM APIs only: initialize/load through Spark catalog, then
        # obtain the attached Iceberg table. No private field reflection.
        jvm = spark._jvm
        catalog = spark._jsparkSession.sessionState().catalogManager().catalog("probe")
        assert catalog.icebergCatalog().getClass().getName() == "org.apache.iceberg.rest.RESTCatalog"
        ns_array = spark.sparkContext._gateway.new_array(jvm.java.lang.String, 1)
        ns_array[0] = namespace
        identifier = jvm.org.apache.spark.sql.connector.catalog.Identifier.of(ns_array, "Orders")
        prepared = catalog.loadTable(identifier).table()
        original_uuid = str(prepared.uuid())
        alias_catalog = spark._jsparkSession.sessionState().catalogManager().catalog("alias")
        assert str(alias_catalog.loadTable(identifier).table().uuid()) == original_uuid
        spark.sql(f"CREATE TABLE probe.{namespace}.orders (id BIGINT) USING iceberg")
        lower_identifier = jvm.org.apache.spark.sql.connector.catalog.Identifier.of(ns_array, "orders")
        assert str(catalog.loadTable(lower_identifier).table().uuid()) != original_uuid

        # Drop only this run's unique table registration; keep old files.
        # Replacement rows/snapshots then expose whether a borrowed table
        # can silently adopt a different incarnation during an action.
        spark.sql(f"DROP TABLE {name}")
        spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg")
        for value in (201, 202):
            spark.sql(f"INSERT INTO {name} VALUES ({value})")
        replacement = catalog.loadTable(identifier).table()
        replacement_uuid = str(replacement.uuid())
        assert replacement_uuid != original_uuid
        before_snapshot = replacement.currentSnapshot().snapshotId()
        before_files = spark.sql(f"SELECT COUNT(*) AS n FROM {name}.files").first().n
        assert before_files == 2, before_files

        # The result is an observed API limitation, not A2 acceptance.
        prepared.refresh()
        refreshed_uuid = str(prepared.uuid())
        assert refreshed_uuid == replacement_uuid
        result = (jvm.org.apache.iceberg.spark.actions.SparkActions.get(spark._jsparkSession)
            .rewriteDataFiles(prepared).option("rewrite-all", "true").execute())
        replacement.refresh()
        after_snapshot = replacement.currentSnapshot().snapshotId()
        after_files = spark.sql(f"SELECT COUNT(*) AS n FROM {name}.files").first().n
        assert result.rewrittenDataFilesCount() == 2
        assert after_files == 1, after_files
        assert after_snapshot != before_snapshot
        assert sorted(row.id for row in spark.table(name).collect()) == [201, 202]
        evidence = {
            "claim": "unguarded REST Table.refresh adopts replacement; bound action can mutate it",
            "driver_maintenance_qualified": False,
            "runtime": {"spark": spark.version, "iceberg": "1.10.1"},
            "namespace": namespace, "original_uuid": original_uuid,
            "replacement_uuid": replacement_uuid, "refreshed_uuid": refreshed_uuid,
            "replacement_snapshots": [before_snapshot, after_snapshot],
            "replacement_files": [before_files, after_files],
            "rewritten_data_files": result.rewrittenDataFilesCount(),
            "protected_rows": [201, 202],
            "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "jars": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in selected},
        }
        print("REST_MAINTENANCE_PROBE=" + json.dumps(evidence, sort_keys=True))
    finally:
        # Retain synthetic resources for review; never purge or reset services.
        spark.stop()


if __name__ == "__main__":
    main()
