"""Lazy runtime registry state and built-in plugin registration."""

from __future__ import annotations

import logging as _logging
from threading import RLock
from typing import Any

_logger = _logging.getLogger(__name__)

_RuntimeRegistry = dict[str, Any]
_runtime_registries: _RuntimeRegistry | None = None
_builtins_registered = False
_runtime_lock = RLock()

def _runtime_state() -> _RuntimeRegistry:
    """Create global plugin registries and built-ins on first runtime use."""
    global _runtime_registries, _builtins_registered
    with _runtime_lock:
        if _runtime_registries is None:
            from datacoolie.core.registry import PluginRegistry
            from datacoolie.core.secrets.resolver import BaseSecretResolver
            from datacoolie.destinations.base import BaseDestinationWriter
            from datacoolie.engines.base import BaseEngine
            from datacoolie.platforms.base import BasePlatform
            from datacoolie.sources.base import BaseSourceReader
            from datacoolie.transformers.base import BaseTransformer

            _runtime_registries = {
                "engine_registry": PluginRegistry("datacoolie.engines", BaseEngine),
                "platform_registry": PluginRegistry("datacoolie.platforms", BasePlatform),
                "source_registry": PluginRegistry("datacoolie.sources", BaseSourceReader),
                "destination_registry": PluginRegistry("datacoolie.destinations", BaseDestinationWriter),
                "transformer_registry": PluginRegistry("datacoolie.transformers", BaseTransformer),
                "resolver_registry": PluginRegistry("datacoolie.resolvers", BaseSecretResolver),
            }
        if not _builtins_registered:
            _register_builtins(_runtime_registries)
            _builtins_registered = True
        return _runtime_registries

def _register_builtins(registries: _RuntimeRegistry | None = None) -> None:
    """Register all built-in plugins.

    Uses try/except for each registration so that missing optional
    dependencies (e.g. PySpark, Polars) don't break runtime initialization.

    ``registries`` is an internal hook used while the lazy runtime state is
    being constructed.  The public no-argument form remains useful for tests
    and for applications that explicitly want to retry built-in discovery.
    """
    state = registries if registries is not None else _runtime_state()
    engine_registry = state["engine_registry"]
    platform_registry = state["platform_registry"]
    source_registry = state["source_registry"]
    destination_registry = state["destination_registry"]
    transformer_registry = state["transformer_registry"]
    resolver_registry = state["resolver_registry"]
    # -- Engines --
    try:
        from datacoolie.engines.spark_engine import SparkEngine
        engine_registry.register("spark", SparkEngine)
    except Exception as exc:
        _logger.debug("Skipping built-in 'spark' engine: %s", exc)

    try:
        from datacoolie.engines.polars_engine import PolarsEngine
        engine_registry.register("polars", PolarsEngine)
    except Exception as exc:
        _logger.debug("Skipping built-in 'polars' engine: %s", exc)

    # -- Platforms --
    try:
        from datacoolie.platforms.local_platform import LocalPlatform
        platform_registry.register("local", LocalPlatform)
    except Exception as exc:
        _logger.debug("Skipping built-in 'local' platform: %s", exc)

    try:
        from datacoolie.platforms.fabric_platform import FabricPlatform
        platform_registry.register("fabric", FabricPlatform)
    except Exception as exc:
        _logger.debug("Skipping built-in 'fabric' platform: %s", exc)

    try:
        from datacoolie.platforms.databricks_platform import DatabricksPlatform
        platform_registry.register("databricks", DatabricksPlatform)
    except Exception as exc:
        _logger.debug("Skipping built-in 'databricks' platform: %s", exc)

    try:
        from datacoolie.platforms.aws_platform import AWSPlatform
        platform_registry.register("aws", AWSPlatform)
    except Exception as exc:
        _logger.debug("Skipping built-in 'aws' platform: %s", exc)

    # -- Sources --
    try:
        from datacoolie.sources.delta_reader import DeltaReader
        source_registry.register("delta", DeltaReader)
    except Exception as exc:
        _logger.debug("Skipping built-in 'delta' source: %s", exc)

    try:
        from datacoolie.sources.file_reader import FileReader
        source_registry.register("parquet", FileReader)
        source_registry.register("csv", FileReader)
        source_registry.register("json", FileReader)
        source_registry.register("jsonl", FileReader)
        source_registry.register("avro", FileReader)
        source_registry.register("excel", FileReader)
    except Exception as exc:
        _logger.debug("Skipping built-in file source readers: %s", exc)

    try:
        from datacoolie.sources.python_function_reader import PythonFunctionReader
        source_registry.register("function", PythonFunctionReader)
    except Exception as exc:
        _logger.debug("Skipping built-in 'function' source: %s", exc)

    try:
        from datacoolie.sources.database_reader import DatabaseReader
        source_registry.register("sql", DatabaseReader)
    except Exception as exc:
        _logger.debug("Skipping built-in 'sql' source: %s", exc)

    try:
        from datacoolie.sources.iceberg_reader import IcebergReader
        source_registry.register("iceberg", IcebergReader)
    except Exception as exc:
        _logger.debug("Skipping built-in 'iceberg' source: %s", exc)

    try:
        from datacoolie.sources.api_reader import APIReader
        source_registry.register("api", APIReader)
    except Exception as exc:
        _logger.debug("Skipping built-in 'api' source: %s", exc)

    # -- Destinations --
    try:
        from datacoolie.destinations.delta_writer import DeltaWriter
        destination_registry.register("delta", DeltaWriter)
    except Exception as exc:
        _logger.debug("Skipping built-in 'delta' destination: %s", exc)

    try:
        from datacoolie.destinations.file_writer import FileWriter
        destination_registry.register("parquet", FileWriter)
        destination_registry.register("csv", FileWriter)
        destination_registry.register("json", FileWriter)
        destination_registry.register("jsonl", FileWriter)
        destination_registry.register("avro", FileWriter)
    except Exception as exc:
        _logger.debug("Skipping built-in file destination writers: %s", exc)

    try:
        from datacoolie.destinations.iceberg_writer import IcebergWriter
        destination_registry.register("iceberg", IcebergWriter)
    except Exception as exc:
        _logger.debug("Skipping built-in 'iceberg' destination: %s", exc)

    # -- Transformers --
    try:
        from datacoolie.transformers.column_value_transformer import ColumnValueTransformer
        transformer_registry.register("column_value_transformer", ColumnValueTransformer)
    except Exception as exc:
        _logger.debug("Skipping built-in 'column_value_transformer' transformer: %s", exc)

    try:
        from datacoolie.transformers.schema_converter import SchemaConverter
        transformer_registry.register("schema_converter", SchemaConverter)
    except Exception as exc:
        _logger.debug("Skipping built-in 'schema_converter' transformer: %s", exc)

    try:
        from datacoolie.transformers.hash_column_adder import HashColumnAdder
        transformer_registry.register("hash_column_adder", HashColumnAdder)
    except Exception as exc:
        _logger.debug("Skipping built-in 'hash_column_adder' transformer: %s", exc)

    try:
        from datacoolie.transformers.deduplicator import Deduplicator
        transformer_registry.register("deduplicator", Deduplicator)
    except Exception as exc:
        _logger.debug("Skipping built-in 'deduplicator' transformer: %s", exc)

    try:
        from datacoolie.transformers.column_adder import ColumnAdder, SCD2ColumnAdder, SystemColumnAdder
        transformer_registry.register("column_adder", ColumnAdder)
        transformer_registry.register("scd2_column_adder", SCD2ColumnAdder)
        transformer_registry.register("system_column_adder", SystemColumnAdder)
    except Exception as exc:
        _logger.debug("Skipping built-in column adder transformers: %s", exc)

    try:
        from datacoolie.transformers.row_filter import RowFilter
        transformer_registry.register("row_filter", RowFilter)
    except Exception as exc:
        _logger.debug("Skipping built-in 'row_filter' transformer: %s", exc)

    try:
        from datacoolie.transformers.partition_handler import PartitionHandler
        transformer_registry.register("partition_handler", PartitionHandler)
    except Exception as exc:
        _logger.debug("Skipping built-in 'partition_handler' transformer: %s", exc)

    try:
        from datacoolie.transformers.column_name_sanitizer import ColumnNameSanitizer
        transformer_registry.register("column_name_sanitizer", ColumnNameSanitizer)
    except Exception as exc:
        _logger.debug("Skipping built-in 'column_name_sanitizer' transformer: %s", exc)

    try:
        from datacoolie.transformers.data_masker import DataMasker
        from datacoolie.transformers.column_projector import ColumnProjector
        transformer_registry.register("data_masker", DataMasker)
        transformer_registry.register("column_projector", ColumnProjector)
    except Exception as exc:
        _logger.debug("Skipping built-in masking/projection transformers: %s", exc)

    # -- Secret Resolvers --
    try:
        from datacoolie.core.secrets.resolver import EnvResolver
        resolver_registry.register("env", EnvResolver)
    except Exception as exc:
        _logger.debug("Skipping built-in 'env' resolver: %s", exc)

__all__ = ["_runtime_state", "_register_builtins"]
