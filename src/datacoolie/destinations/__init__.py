"""Destination components with lazy public exports.

Importing the destination runtime model must not initialize writers, load
strategies, logging, or optional engine integrations.  The public component
exports remain available through lazy attribute resolution for callers that
explicitly request them.
"""

from importlib import import_module
from typing import Any

from datacoolie.destinations.runtime import DestinationRuntimeInfo


_LAZY_EXPORTS = {
    "BaseDestinationWriter": ("datacoolie.destinations.base", "BaseDestinationWriter"),
    "BaseLoadStrategy": ("datacoolie.destinations.base", "BaseLoadStrategy"),
    "DeltaWriter": ("datacoolie.destinations.delta_writer", "DeltaWriter"),
    "FileWriter": ("datacoolie.destinations.file_writer", "FileWriter"),
    "IcebergWriter": ("datacoolie.destinations.iceberg_writer", "IcebergWriter"),
    "LOAD_STRATEGIES": ("datacoolie.destinations.strategies.load", "LOAD_STRATEGIES"),
    "AppendStrategy": ("datacoolie.destinations.strategies.load", "AppendStrategy"),
    "MergeOverwriteStrategy": (
        "datacoolie.destinations.strategies.load",
        "MergeOverwriteStrategy",
    ),
    "MergeUpsertStrategy": (
        "datacoolie.destinations.strategies.load",
        "MergeUpsertStrategy",
    ),
    "OverwriteStrategy": ("datacoolie.destinations.strategies.load", "OverwriteStrategy"),
    "SCD2Strategy": ("datacoolie.destinations.strategies.load", "SCD2Strategy"),
    "get_load_strategy": ("datacoolie.destinations.strategies.load", "get_load_strategy"),
    "ResolvedDestination": (
        "datacoolie.destinations.resolution.target",
        "ResolvedDestination",
    ),
    "resolve_destination_target": (
        "datacoolie.destinations.resolution.target",
        "resolve_destination_target",
    ),
}


def __getattr__(name: str) -> Any:
    """Load a destination component only when it is explicitly requested."""

    try:
        module_name, attribute_name = _LAZY_EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    value = getattr(import_module(module_name), attribute_name)
    globals()[name] = value
    return value

__all__ = [
    "BaseDestinationWriter",
    "BaseLoadStrategy",
    "DestinationRuntimeInfo",
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
