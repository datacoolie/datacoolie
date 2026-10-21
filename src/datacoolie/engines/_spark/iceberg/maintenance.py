"""REST preparation through public native Spark catalog APIs and JVM guard."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from datacoolie.core.exceptions import ConfigurationError, EngineError
from datacoolie.engines.contracts.maintenance import ICEBERG_ACTIONS, PreparedMaintenanceTarget, native_identifier
from datacoolie.engines._spark.iceberg.native_build import verify_runtime_provenance


@dataclass(frozen=True, slots=True)
class SparkMaintenanceBinding:
    spark: Any
    table: Any


def _identifier(spark, parts):
    array = spark.sparkContext._gateway.new_array(spark._jvm.java.lang.String, len(parts) - 1)
    for index, part in enumerate(parts[:-1]):
        array[index] = part
    return spark._jvm.org.apache.spark.sql.connector.catalog.Identifier.of(array, parts[-1])


def preparation(spark, deployment, guard_manifest):
    verified = False
    plugin = None
    def prepare(*, fmt, table_name=None, path=None, table_address=None):
        nonlocal verified, plugin
        if fmt != "iceberg" or not table_name:
            raise ConfigurationError("Spark REST maintenance requires a named Iceberg table")
        if deployment is None or guard_manifest is None:
            raise ConfigurationError("Spark REST maintenance requires deployment binding and native guard manifest")
        parts = native_identifier(table_name, catalog_label=deployment.catalog_label,
                                  table_address=table_address)
        if not verified:
            verify_runtime_provenance(spark, guard_manifest)
            plugin = spark._jsparkSession.sessionState().catalogManager().catalog(deployment.catalog_label)
            if plugin.getClass().getName() != "org.apache.iceberg.spark.SparkCatalog":
                raise ConfigurationError("Unqualified native Spark maintenance catalog")
            catalog = plugin.icebergCatalog()
            if catalog.getClass().getName() != "org.apache.iceberg.rest.RESTCatalog":
                raise ConfigurationError("Spark maintenance deployment requires a native REST catalog")
            deployment.validate_properties(dict(catalog.properties()))
            sentinel = plugin.loadTable(_identifier(spark, deployment.sentinel_identifier)).table()
            if str(sentinel.uuid()) != deployment.sentinel_uuid:
                raise ConfigurationError("REST deployment sentinel UUID changed")
            deployment.validate_location(sentinel.location())
            verified = True
        try:
            table = plugin.loadTable(_identifier(spark, parts)).table()
        except Exception as exc:
            # JVM missing is distinct from authorization/transport failures.
            java = getattr(exc, "java_exception", None)
            from pyspark.errors import AnalysisException
            captured_missing = isinstance(exc, AnalysisException) and exc.getErrorClass() == "TABLE_OR_VIEW_NOT_FOUND"
            if captured_missing or (java is not None and java.getClass().getName() in {
                "org.apache.spark.sql.catalyst.analysis.NoSuchTableException",
                "org.apache.iceberg.exceptions.NoSuchTableException"}):
                return PreparedMaintenanceTarget("iceberg", table_name, None, None, missing=True)
            raise
        expected_uuid = str(table.uuid())
        from uuid import UUID
        try:
            UUID(expected_uuid)
        except ValueError as exc:
            raise ConfigurationError("Spark REST target requires native UUID metadata") from exc
        deployment.validate_location(table.location())
        guarded = spark._jvm.org.datacoolie.iceberg.GuardedTableOperations.bind(table, expected_uuid)
        return PreparedMaintenanceTarget("iceberg", table_name, None,
            f"{deployment.scope}:{expected_uuid}", expected_uuid=expected_uuid,
            capabilities=ICEBERG_ACTIONS, binding=SparkMaintenanceBinding(spark, guarded))
    return prepare


def _plain(value):
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    # Py4J maps/lists implement Python protocols even when their concrete
    # native class is Collections.UnmodifiableMap/List.
    from py4j.java_collections import JavaMap, JavaList, JavaSet
    if isinstance(value, JavaMap):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (JavaList, JavaSet)):
        return [_plain(item) for item in value]
    return str(value)


def execute_action(prepared, *, action, retention_hours):
    if action not in prepared.capabilities:
        raise ConfigurationError(f"Unsupported Spark maintenance action: {action}")
    if type(retention_hours) is not int or retention_hours < 0:
        raise ConfigurationError("Maintenance retention must be nonnegative integer hours")
    binding = prepared.binding
    cutoff = int((datetime.now(timezone.utc) - timedelta(hours=retention_hours)).timestamp() * 1000)
    try:
        result = binding.spark._jvm.org.datacoolie.iceberg.NativeMaintenance.execute(
            binding.table, binding.spark._jsparkSession, action, cutoff)
        return _plain(result)
    except Exception as exc:
        failure = EngineError(str(exc) or type(exc).__name__)
        java = getattr(exc, "java_exception", None)
        if java is not None and java.getClass().getName() == "org.datacoolie.iceberg.NativeMaintenance$ActionFailure":
            failure.action_detail = _plain(java.detail())
        raise failure from exc


def history(prepared, *, limit=5, start_time=None, end_time=None):
    binding = prepared.binding
    def millis(value):
        return None if value is None else binding.spark._jvm.java.lang.Long(int(value.timestamp() * 1000))
    return _plain(binding.spark._jvm.org.datacoolie.iceberg.NativeMaintenance.history(
        binding.table, limit, millis(start_time), millis(end_time)))
