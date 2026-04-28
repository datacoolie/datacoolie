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
from datacoolie.destinations.load_strategies import (
    LOAD_STRATEGIES,
    AppendStrategy,
    MergeOverwriteStrategy,
    MergeUpsertStrategy,
    OverwriteStrategy,
    SCD2Strategy,
    get_load_strategy,
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
]
