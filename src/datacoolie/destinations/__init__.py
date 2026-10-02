"""Destination writer components.

Provides :class:`BaseDestinationWriter`, :class:`BaseLoadStrategy`, and
concrete writers: :class:`DeltaWriter`, :class:`IcebergWriter`.

Load strategies are available via :func:`get_load_strategy` and the
:data:`LOAD_STRATEGIES` registry.
"""

from datacoolie.destinations.base import BaseDestinationWriter, BaseLoadStrategy
from datacoolie.destinations.delta_writer import DeltaWriter
from datacoolie.destinations.file_writer import FileWriter
from datacoolie.destinations.iceberg_writer import IcebergWriter
from datacoolie.destinations.strategies.load import (
    LOAD_STRATEGIES,
    AppendStrategy,
    MergeOverwriteStrategy,
    MergeUpsertStrategy,
    OverwriteStrategy,
    SCD2Strategy,
    get_load_strategy,
)
from datacoolie.destinations.resolution.target import (
    ResolvedDestination,
    resolve_destination_target,
)

__all__ = [
    "BaseDestinationWriter",
    "BaseLoadStrategy",
    "DeltaWriter",
    "FileWriter",
    "IcebergWriter",
    "LOAD_STRATEGIES",
    "AppendStrategy",
    "MergeOverwriteStrategy",
    "MergeUpsertStrategy",
    "OverwriteStrategy",
    "SCD2Strategy",
    "get_load_strategy",
    "ResolvedDestination",
    "resolve_destination_target",
]
