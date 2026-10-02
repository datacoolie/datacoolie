"""DataCoolie core — domain models, constants, exceptions, and plugin registry."""

from datacoolie.core.constants import (
    ConnectionType,
    DataFlowStatus,
    DatabaseAuthType,
    ExecutionType,
    DatabaseType,
    Format,
    LoadType,
    MaintenanceType,
    ProcessingMode,
    ColumnCaseMode,
    SystemColumn,
    FileInfoColumn,
)
from datacoolie.core.exceptions import (
    ConfigurationError,
    DataCoolieError,
    DataFlowError,
    DestinationError,
    EngineError,
    MetadataError,
    PlatformError,
    SourceError,
    TransformError,
    WatermarkError,
)
from datacoolie.core.models.destination import Destination, PartitionColumn
from datacoolie.core.models.transform import AdditionalColumn, SchemaHint, Transform
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.source import Source
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.run_config import DataCoolieRunConfig, ReplayConfig
from datacoolie.core.registry import PluginRegistry

__all__ = [
    # Enums
    "ConnectionType",
    "DataFlowStatus",
    "ExecutionType",
    "DatabaseAuthType",
    "DatabaseType",
    "Format",
    "LoadType",
    "MaintenanceType",
    "ProcessingMode",
    "ColumnCaseMode",
    "SystemColumn",
    "FileInfoColumn",
    # Exceptions
    "ConfigurationError",
    "DataCoolieError",
    "DataFlowError",
    "DestinationError",
    "EngineError",
    "MetadataError",
    "PlatformError",
    "SourceError",
    "TransformError",
    "WatermarkError",
    # Models
    "SchemaHint",
    "PartitionColumn",
    "AdditionalColumn",
    "Connection",
    "Source",
    "Destination",
    "Transform",
    "DataFlow",
    "DataCoolieRunConfig",
    "ReplayConfig",
    # Registry
    "PluginRegistry",
]
