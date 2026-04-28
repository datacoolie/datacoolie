"""DataCoolie — Metadata-driven, engine-unified, cloud-agnostic ETL framework."""

__version__ = "0.1.0"

__all__ = [
    # Base classes
    "PluginRegistry",
    "BaseSecretResolver",
    "BaseEngine",
    "BasePlatform",
    "BaseSourceReader",
    "BaseDestinationWriter",
    "BaseTransformer",
    # Registries
    "engine_registry",
    "platform_registry",
    "source_registry",
    "destination_registry",
    "transformer_registry",
    "resolver_registry",
    # Factory functions
    "create_engine",
    "create_platform",
    "create_source",
    "create_destination",
    "create_transformer",
    "create_resolver",
]

import logging as _logging

from datacoolie.core.registry import PluginRegistry
from datacoolie.core.secret_resolver import BaseSecretResolver
from datacoolie.destinations.base import BaseDestinationWriter
from datacoolie.engines.base import BaseEngine
from datacoolie.platforms.base import BasePlatform
from datacoolie.sources.base import BaseSourceReader
from datacoolie.transformers.base import BaseTransformer

_logger = _logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global plugin registries
# ---------------------------------------------------------------------------

engine_registry: PluginRegistry[BaseEngine] = PluginRegistry("datacoolie.engines", BaseEngine)
platform_registry: PluginRegistry[BasePlatform] = PluginRegistry("datacoolie.platforms", BasePlatform)
source_registry: PluginRegistry[BaseSourceReader] = PluginRegistry("datacoolie.sources", BaseSourceReader)
destination_registry: PluginRegistry[BaseDestinationWriter] = PluginRegistry("datacoolie.destinations", BaseDestinationWriter)
transformer_registry: PluginRegistry[BaseTransformer] = PluginRegistry("datacoolie.transformers", BaseTransformer)
resolver_registry: PluginRegistry[BaseSecretResolver] = PluginRegistry("datacoolie.resolvers", BaseSecretResolver)


# ---------------------------------------------------------------------------
# Factory functions
# ---------------------------------------------------------------------------

def create_engine(name: str, **kwargs: object) -> BaseEngine:
    """Create an engine instance by name (e.g. ``"spark"``, ``"polars"``)."""
    return engine_registry.get(name, **kwargs)


def create_platform(name: str, **kwargs: object) -> BasePlatform:
    """Create a platform instance by name (e.g. ``"local"``, ``"fabric"``)."""
    return platform_registry.get(name, **kwargs)


def create_source(name: str, **kwargs: object) -> BaseSourceReader:
    """Create a source reader by name (e.g. ``"delta"``, ``"csv"``)."""
    return source_registry.get(name, **kwargs)


def create_destination(name: str, **kwargs: object) -> BaseDestinationWriter:
    """Create a destination writer by name (e.g. ``"delta"``, ``"parquet"``)."""
    return destination_registry.get(name, **kwargs)


def create_transformer(name: str, **kwargs: object) -> BaseTransformer:
    """Create a transformer by name (e.g. ``"schema_converter"``)."""
    return transformer_registry.get(name, **kwargs)


def create_resolver(name: str, **kwargs: object) -> BaseSecretResolver:
    """Create a secret resolver by name (e.g. ``"env"``)."""
    return resolver_registry.get(name, **kwargs)


# ---------------------------------------------------------------------------
# Built-in registrations (after all packages are initialised)
# ---------------------------------------------------------------------------

def _register_builtins() -> None:
    """Register all built-in plugins.

    Uses try/except for each registration so that missing optional
    dependencies (e.g. PySpark, Polars) don't break the import.
    """
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
        from datacoolie.transformers.schema_converter import SchemaConverter
        transformer_registry.register("schema_converter", SchemaConverter)
    except Exception as exc:
        _logger.debug("Skipping built-in 'schema_converter' transformer: %s", exc)

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
        from datacoolie.transformers.partition_handler import PartitionHandler
        transformer_registry.register("partition_handler", PartitionHandler)
    except Exception as exc:
        _logger.debug("Skipping built-in 'partition_handler' transformer: %s", exc)

    try:
        from datacoolie.transformers.column_name_sanitizer import ColumnNameSanitizer
        transformer_registry.register("column_name_sanitizer", ColumnNameSanitizer)
    except Exception as exc:
        _logger.debug("Skipping built-in 'column_name_sanitizer' transformer: %s", exc)

    # -- Secret Resolvers --
    try:
        from datacoolie.core.secret_resolver import EnvResolver
        resolver_registry.register("env", EnvResolver)
    except Exception as exc:
        _logger.debug("Skipping built-in 'env' resolver: %s", exc)


_register_builtins()
