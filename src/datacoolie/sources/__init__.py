"""Source contracts and lazily loaded reader components."""

from __future__ import annotations

from importlib import import_module
from typing import Any

from datacoolie.sources.contracts import (
    SourceReadMode,
    SourceReadRange,
    SourceReadRequest,
    SourceReadResult,
    SourceReadSelection,
    SourceReadUpperOrigin,
)
from datacoolie.sources.runtime import SourceRuntimeInfo

_READER_EXPORTS = {
    "APIReader": ("datacoolie.sources.api_reader", "APIReader"),
    "BaseSourceReader": ("datacoolie.sources.base", "BaseSourceReader"),
    "DatabaseReader": ("datacoolie.sources.database_reader", "DatabaseReader"),
    "DeltaReader": ("datacoolie.sources.delta_reader", "DeltaReader"),
    "FileReader": ("datacoolie.sources.file_reader", "FileReader"),
    "IcebergReader": ("datacoolie.sources.iceberg_reader", "IcebergReader"),
    "PythonFunctionReader": (
        "datacoolie.sources.python_function_reader",
        "PythonFunctionReader",
    ),
}

__all__ = [
    "APIReader",
    "BaseSourceReader",
    "SourceReadRange",
    "SourceReadResult",
    "SourceReadSelection",
    "DatabaseReader",
    "DeltaReader",
    "FileReader",
    "IcebergReader",
    "PythonFunctionReader",
    "SourceReadMode",
    "SourceReadRequest",
    "SourceReadUpperOrigin",
    "SourceRuntimeInfo",
]


def __getattr__(name: str) -> Any:
    """Load a concrete reader only when it is requested."""

    target = _READER_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attribute = target
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value
