"""Pure source-datatype interpretation shared by engine adapters.

This package has no optional dataframe imports, sessions, connectors, or I/O.
It turns an authored source declaration into an immutable logical description;
Spark and Polars adapters consume that description using native APIs.
"""

from datacoolie.engines.data_types.models import (
    LogicalKind,
    ResolvedDataType,
    TimestampKind,
    TypeSystem,
)
from datacoolie.engines.data_types.resolver import (
    infer_type_system,
    normalize_type_system,
    resolve_schema_hint,
)

__all__ = [
    "LogicalKind",
    "ResolvedDataType",
    "TimestampKind",
    "TypeSystem",
    "infer_type_system",
    "normalize_type_system",
    "resolve_schema_hint",
]

