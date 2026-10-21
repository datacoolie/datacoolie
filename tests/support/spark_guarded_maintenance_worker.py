"""Real REST action/refresh/commit/retry qualification of experimental JVM guard."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
import shutil
from pathlib import Path
from uuid import uuid4


def main() -> None:
    from pyspark.sql import SparkSession

    product = Path(__file__).resolve().parents[2]
    sources = sorted((product / "tests/support/native_iceberg").glob("*.java"))
    fingerprint = hashlib.sha256(b"".join(p.read_bytes() for p in sources)).hexdigest()
    guard_jar = product / "usecase-sim/.runtime/dependencies/native-iceberg-qualification" / fingerprint / "guard.jar"
    assert guard_jar.is_file(), "Run tests/support/build_iceberg_guard.py explicitly before qualification"
    jars = product / "usecase-sim/.runtime/dependencies/spark/spark35/jars"
    selected = [guard_jar, jars / "org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.10.1.jar",
                jars / "software.amazon.awssdk_bundle-2.29.51.jar"]
    manifest_path = os.getenv("DATACOOLIE_GUARD_MANIFEST")
    if manifest_path:
        probe_jar = guard_jar.with_name("qualification-probes.jar")
        shutil.copyfile(guard_jar, probe_jar)
        selected[0] = probe_jar
        selected.append(Path(manifest_path).with_name("guard.jar"))
    builder = (SparkSession.builder.master("local[1]")
        .appName("guarded-native-maintenance-qualification")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", ",".join(map(str, selected)))
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"))
    options = {
        "": "org.apache.iceberg.spark.SparkCatalog", "type": "rest",
        "uri": os.getenv("DATACOOLIE_ICEBERG_URI", "http://iceberg-rest:8181"),
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO", "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "s3.path-style-access": "true", "client.region": "us-east-1", "cache-enabled": "false",
    }
    for key, value in options.items():
        builder = builder.config("spark.sql.catalog.probe" + (f".{key}" if key else ""), value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    jvm = spark._jvm
    if manifest_path:
        from datacoolie.engines._spark.iceberg.native_build import verify_runtime_provenance
        verify_runtime_provenance(spark, manifest_path)
    def bind(probe, expected_uuid):
        if manifest_path:
            return jvm.org.datacoolie.iceberg.GuardedTableOperations.bind(probe.nativeTable(), expected_uuid)
        return probe.guarded()
    load = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable
    namespace = f"maintenance_guard_{uuid4().hex}"
    actions = ("rewrite_data_files", "rewrite_position_delete_files", "rewrite_manifests",
               "expire_snapshots", "remove_orphan_files")
    cutoff = int(datetime.now(timezone.utc).timestamp() * 1000) - 168 * 3600 * 1000
    records = []
    reused = []
    positive = []
    try:
        spark.sql(f"CREATE NAMESPACE probe.{namespace}")
        for action in actions:
            # Orphan deletion has no commit; its pre-action refresh guard is
            # required. The operational external-DDL exclusion still applies.
            boundaries = ("before", "refresh", "commit", "retry") if action != "remove_orphan_files" else ("before",)
            if action == "expire_snapshots":
                boundaries += ("postcommit",)
            for root_mode, boundary in ((mode, boundary) for mode in ("fresh", "reused") for boundary in boundaries):
                name = f"probe.{namespace}.{action}_{root_mode}_{boundary}"
                spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg TBLPROPERTIES ('write.delete.mode'='merge-on-read')")
                for value in (101, 102, 103):
                    spark.sql(f"INSERT INTO {name} VALUES ({value}), ({value + 1000})")
                if action == "rewrite_position_delete_files":
                    spark.sql(f"DELETE FROM {name} WHERE id=101")
                    spark.sql(f"DELETE FROM {name} WHERE id=102")
                    assert spark.sql(f"SELECT COUNT(*) AS n FROM {name}.delete_files").first().n > 0
                native = load(spark._jsparkSession, name)
                original_uuid = str(native.uuid())
                trigger = boundary if root_mode == "fresh" else "reused_" + boundary
                probe = jvm.org.datacoolie.qualification.ReplacementProbe(native, spark._jsparkSession, name, trigger)
                guarded = bind(probe, original_uuid)
                if boundary == "before":
                    probe.replace()
                # All action dispatch must revalidate; refresh injection tests
                # action-internal refresh instead, after pre-action validation.
                try:
                    if boundary == "before":
                        guarded.refresh()
                    if action == "expire_snapshots":
                        # Use native expiration's real commit with a future cutoff
                        # only on these fresh case-owned append snapshots.
                        action_cutoff = int(datetime.now(timezone.utc).timestamp() * 1000) + 1000
                    else:
                        action_cutoff = cutoff
                    probe.execute(guarded, action, action_cutoff)
                except Exception as exc:
                    error = str(exc)
                    assert "UUID" in error or "uuid" in error or "ValidationException" in error or "commit rewrite" in error.lower(), error
                else:
                    raise AssertionError(f"Expected stale-binding rejection: {action}/{boundary}")
                metadata_location = probe.replacementMetadata()
                assert metadata_location, f"Boundary never activated: {action}/{boundary}"
                replacement = load(spark._jsparkSession, name)
                assert str(replacement.uuid()) != original_uuid
                assert replacement.operations().current().metadataFileLocation() == metadata_location
                assert sorted(row.id for row in spark.table(name).collect()) == [901, 902]
                assert spark.sql(f"SELECT COUNT(*) AS n FROM {name}.snapshots").first().n == 1
                assert probe.deletions() == 0
                if root_mode == "fresh":
                    assert probe.replacementFileHashes() == probe.replacementFilesBefore(), (
                        action, boundary, probe.replacementFilesBefore(), probe.replacementFileHashes())
                    old_staged_deleted = 0
                else:
                    old_staged_deleted = probe.verifyReusedRootOwnership()
                (records if root_mode == "fresh" else reused).append({"action": action, "boundary": boundary, "old_uuid": original_uuid,
                    "replacement_uuid": str(replacement.uuid()), "refreshes": probe.refreshes(),
                    "commits": probe.commits(), "replacement_metadata_unchanged": True,
                    "replacement_objects_unchanged": True, "cleanup_callback_deletions": 0,
                    "old_uncommitted_outputs_deleted": old_staged_deleted})
                print(f"QUALIFIED {action}/{root_mode}/{boundary}", flush=True)
        for action in actions:
            name = f"probe.{namespace}.{action}_positive"
            spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg TBLPROPERTIES ('write.delete.mode'='merge-on-read')")
            for value in (101, 102, 103):
                spark.sql(f"INSERT INTO {name} VALUES ({value}), ({value + 1000})")
            expected_rows = [101, 102, 103, 1101, 1102, 1103]
            if action == "rewrite_position_delete_files":
                spark.sql(f"DELETE FROM {name} WHERE id IN (101, 102)")
                expected_rows = [103, 1101, 1102, 1103]
            native = load(spark._jsparkSession, name)
            probe = jvm.org.datacoolie.qualification.ReplacementProbe(native, spark._jsparkSession, name, "none")
            guarded = bind(probe, str(native.uuid()))
            guarded.refresh()
            snapshots_before = list(guarded.snapshots())
            probe.execute(guarded, action, cutoff)
            guarded.refresh()
            assert str(guarded.uuid()) == str(native.uuid())
            assert sorted(row.id for row in spark.table(name).collect()) == expected_rows
            positive.append({"action": action, "uuid": str(guarded.uuid()),
                "rows": expected_rows, "snapshot_count_before": len(snapshots_before),
                "snapshot_count_after": len(list(guarded.snapshots())), "commits": probe.commits()})
            print(f"QUALIFIED {action}/positive", flush=True)
        name = f"probe.{namespace}.paths_and_latch"
        spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg")
        spark.sql(f"INSERT INTO {name} VALUES (101), (102)")
        native = load(spark._jsparkSession, name)
        probe = jvm.org.datacoolie.qualification.ReplacementProbe(native, spark._jsparkSession, name, "none")
        probe.verifyPathsAndLatch(bind(probe, str(native.uuid())))
        assert probe.replacementFileHashes() == probe.replacementFilesBefore()
        print("QUALIFIED immutable_paths_and_sticky_latch", flush=True)
        name = f"probe.{namespace}.same_uuid_retry"
        spark.sql(f"CREATE TABLE {name} (id BIGINT) USING iceberg")
        spark.sql(f"INSERT INTO {name} VALUES (101), (102)")
        spark.sql(f"INSERT INTO {name} VALUES (103), (104)")
        native = load(spark._jsparkSession, name)
        probe = jvm.org.datacoolie.qualification.ReplacementProbe(native, spark._jsparkSession, name, "same_uuid_retry")
        guarded = bind(probe, str(native.uuid()))
        probe.execute(guarded, "rewrite_data_files", cutoff)
        assert probe.commits() == 2
        assert str(guarded.uuid()) == str(native.uuid())
        assert sorted(row.id for row in spark.table(name).collect()) == [101, 102, 103, 104]
        print("QUALIFIED same_uuid_native_retry", flush=True)
        print("GUARDED_MAINTENANCE_PROBE=" + json.dumps({
            "namespace": namespace, "cases": records, "reused_root_cases": reused, "positive": positive, "spark": spark.version,
            "immutable_paths_and_sticky_latch": True, "same_uuid_native_retry": True,
            "canonical_helper": bool(manifest_path),
            "guard_source_fingerprint": fingerprint, "driver_qualified": False,
        }, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
