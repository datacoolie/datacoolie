"""DataCoolie core — domain models, constants, exceptions, and plugin registry."""

from datacoolie.core.constants import (
    ConnectionType,
    DataFlowStatus,
    ExecutionType,
    DatabaseType,
    Format,
    LoadType,
    LogPurpose,
    LogType,
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
from datacoolie.core.models import (
    SchemaHint,
    PartitionColumn,
    AdditionalColumn,
    Connection,
    Source,
    Destination,
    Transform,
    DataFlow,
    DataCoolieRunConfig,
)
from datacoolie.core.registry import PluginRegistry

__all__ = [
    # Enums
    "ConnectionType",
    "DataFlowStatus",
    "ExecutionType",
    "DatabaseType",
    "Format",
    "LoadType",
    "LogPurpose",
    "LogType",
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
    # Registry
    "PluginRegistry",
]
