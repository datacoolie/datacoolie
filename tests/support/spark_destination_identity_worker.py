"""Isolated native Spark catalog identity qualification; cached JARs only."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path


def main():
    from pyspark.sql import SparkSession

    product = Path(__file__).resolve().parents[2]
    jars = product / "usecase-sim/.runtime/dependencies/spark/spark35/jars"
    selected = [p for p in jars.glob("*.jar") if p.name.startswith((
        "io.delta_", "org.antlr_", "org.apache.iceberg_iceberg-spark-runtime-",
    ))]
    assert len(selected) == 4, [p.name for p in selected]
    with tempfile.TemporaryDirectory(prefix="datacoolie-identity-") as temp:
        root = Path(temp)
        session = (SparkSession.builder.master("local[1]").appName("bounded-destination-identity")
            .config("spark.ui.enabled", "false")
            .config("spark.jars", ",".join(str(p) for p in selected))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.warehouse.dir", str(root / "session"))
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.databricks.delta.snapshotPartitions", "1")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", str(root / "iceberg"))
            .config("spark.sql.catalog.other", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.other.type", "hadoop")
            .config("spark.sql.catalog.other.warehouse", str(root / "other"))
            .config("spark.sql.catalog.alias", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.alias.type", "hadoop")
            .config("spark.sql.catalog.alias.warehouse", str(root / "iceberg"))
            .getOrCreate())
        session.sparkContext.setLogLevel("ERROR")
        try:
            from datacoolie.engines.spark_engine import SparkEngine
            from datacoolie.engines._spark.target_identity import _field
            from datacoolie.core.models.connection import Connection
            from datacoolie.core.models.destination import Destination
            from datacoolie.destinations.resolution.target import resolve_destination_target
            from datacoolie.core.models.dataflow import DataFlow
            from datacoolie.core.models.source import Source
            from datacoolie.orchestration.maintenance import dedupe_by_destination
            from datacoolie.destinations.delta_writer import DeltaWriter
            from datacoolie.destinations.iceberg_writer import IcebergWriter
            from unittest.mock import patch
            engine = SparkEngine(spark_session=session)
            # No lazy session/catalog creation on an uninitialized profile.
            fresh = session.newSession()
            fresh_engine = SparkEngine(spark_session=fresh)
            fresh_state = _field(fresh._jsparkSession, "sessionState")
            assert fresh_engine.target_name_identity(fmt="iceberg", table_name="local.ns.Orders") is None
            assert _field(fresh._jsparkSession, "sessionState") == fresh_state
            assert session.version.startswith("3.5."), session.version
            session.sql("CREATE DATABASE MixedDB")
            session.sql(f"CREATE TABLE MixedDB.Orders (id BIGINT) USING delta LOCATION '{root / 'delta'}'")
            session.sql("INSERT INTO MixedDB.Orders VALUES (101)")
            delta_aliases = ["MixedDB.Orders", "mixeddb.orders", "spark_catalog.MixedDB.Orders"]
            for alias in delta_aliases:
                assert [r.id for r in session.table(alias).collect()] == [101], alias
            delta_keys = [engine.target_name_identity(fmt="delta", table_name=name) for name in (
                "spark_catalog.MixedDB.Orders", "SPARK_CATALOG.mixeddb.orders")]
            assert delta_keys[0] is not None and delta_keys[0] == delta_keys[1], delta_keys
            session.conf.set("spark.sql.caseSensitive", "true")
            assert engine.target_name_identity(fmt="delta", table_name="spark_catalog.MixedDB.Orders") is None
            session.conf.set("spark.sql.caseSensitive", "false")
            for catalog in ("local", "other"):
                session.sql(f"CREATE NAMESPACE {catalog}.ns")
            session.sql("CREATE TABLE local.ns.Orders (id BIGINT) USING iceberg")
            session.sql("CREATE TABLE local.ns.orders (id BIGINT) USING iceberg")
            session.sql("CREATE TABLE other.ns.Orders (id BIGINT) USING iceberg")
            session.sql("INSERT INTO local.ns.Orders VALUES (201)")
            session.sql("INSERT INTO local.ns.orders VALUES (202)")
            session.sql("INSERT INTO other.ns.Orders VALUES (203)")
            iceberg_targets = {name: [r.id for r in session.table(name).collect()] for name in (
                "local.ns.Orders", "local.ns.orders", "other.ns.Orders",
            )}
            assert iceberg_targets == {"local.ns.Orders": [201], "local.ns.orders": [202],
                                       "other.ns.Orders": [203]}, iceberg_targets
            assert [r.id for r in session.table("alias.ns.Orders").collect()] == [201]
            iceberg_keys = {name: engine.target_name_identity(fmt="iceberg", table_name=name) for name in (
                "local.ns.Orders", "local.ns.orders", "other.ns.Orders", "alias.ns.Orders")}
            assert all(iceberg_keys.values()), iceberg_keys
            assert iceberg_keys["local.ns.Orders"] == iceberg_keys["alias.ns.Orders"], iceberg_keys
            assert len(set(iceberg_keys.values())) == 3, iceberg_keys
            session.conf.set("spark.sql.catalog.local.warehouse", str(root / "other"))
            assert engine.target_name_identity(fmt="iceberg", table_name="local.ns.Orders") == iceberg_keys["local.ns.Orders"]
            # Missing cached catalog must stay missing after projection.
            session.conf.set("spark.sql.catalog.unloaded", "org.apache.iceberg.spark.SparkCatalog")
            session.conf.set("spark.sql.catalog.unloaded.type", "hadoop")
            session.conf.set("spark.sql.catalog.unloaded.warehouse", str(root / "unloaded"))
            manager = _field(_field(_field(session._jsparkSession, "sessionState"), "analyzer"), "catalogManager")
            cache = _field(manager, "catalogs")
            assert cache.get("unloaded").isEmpty()
            assert engine.target_name_identity(fmt="iceberg", table_name="unloaded.ns.Orders") is None
            assert cache.get("unloaded").isEmpty()
            assert not (root / "unloaded").exists()
            destination = Destination(connection=Connection(name="delta", format="delta", catalog="spark_catalog",
                configure={"base_path": str(root / "auxiliary")}), schema_name="mixeddb", table="orders")
            resolved = resolve_destination_target(destination, engine=engine, require_identity=True)
            assert resolved.path is None and resolved.storage_path is not None
            assert engine.exists(table_name=resolved.table_name, path=resolved.path, fmt="delta")
            # Native probe/history/compaction uses the catalog table despite an
            # auxiliary path. No fixture path may be silently used instead.
            delta_flow = DataFlow(dataflow_id="delta", source=Source(connection=destination.connection, table="unused"), destination=destination)
            delta_info = DeltaWriter(engine).run_maintenance(delta_flow, do_compact=True, do_cleanup=False)
            assert delta_info.status == "succeeded", delta_info
            assert [r.id for r in session.table("spark_catalog.mixeddb.orders").collect()] == [101]
            iceberg_flows = [DataFlow(dataflow_id=str(index),
                source=Source(connection=Connection(name="unused", format="iceberg"), table="unused"),
                destination=Destination(connection=Connection(name=catalog, format="iceberg", catalog=catalog),
                    schema_name="ns", table=table)) for index, (catalog, table) in enumerate((
                        ("local", "Orders"), ("local", "orders"), ("other", "Orders"), ("alias", "Orders")))]
            unique = dedupe_by_destination(iceberg_flows, engine=engine)
            assert len(unique) == 3, unique
            # Hadoop does not implement SparkCatalog procedures. Observe writer
            # invocation targets while preserving real existence/history reads;
            # this proves scheduling, not unsupported procedure capability.
            with patch.object(engine, "compact_by_name") as compact:
                for flow in unique:
                    info = IcebergWriter(engine).run_maintenance(flow, do_compact=True, do_cleanup=False)
                    assert info.status == "succeeded", info
            assert [call.args[0] for call in compact.call_args_list] == [flow.destination.full_table_name for flow in unique]
            for name, rows in iceberg_targets.items():
                assert [r.id for r in session.table(name).collect()] == rows
            session.catalog.setCurrentCatalog("local")
            assert [r.id for r in session.table("ns.Orders").collect()] == [201]
            # Catalog names are configuration keys, independent of SQL column case.
            try:
                session.table("LOCAL.ns.Orders").collect()
            except Exception:
                uppercase_catalog_is_alias = False
            else:
                uppercase_catalog_is_alias = True
            assert not uppercase_catalog_is_alias
            print(json.dumps({"status": "passed", "spark_version": session.version,
                "delta_aliases": delta_aliases, "iceberg_targets": iceberg_targets,
                "uppercase_catalog_is_alias": uppercase_catalog_is_alias}, sort_keys=True))
        finally:
            session.stop()


if __name__ == "__main__":
    main()
