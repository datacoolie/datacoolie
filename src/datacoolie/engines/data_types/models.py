"""Small, dependency-free logical datatype model.

The model is intentionally narrower than any one engine's type system.  Its
job is to capture the semantics that must remain stable when the same
metadata is executed by Spark or Polars, not to expose a second user-facing
SQL dialect.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from datacoolie.core.exceptions import ConfigurationError


class TypeSystem(str, Enum):
    """Named source conventions understood by the resolver."""

    SPARK_SQL = "spark_sql"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    MSSQL = "mssql"
    ORACLE = "oracle"
    SQLITE = "sqlite"


class LogicalKind(str, Enum):
    """Portable logical categories used by engine adapters."""

    BOOLEAN = "boolean"
    SIGNED_INTEGER = "signed_integer"
    UNSIGNED_INTEGER = "unsigned_integer"
    FLOAT = "float"
    DECIMAL = "decimal"
    STRING = "string"
    BINARY = "binary"
    DATE = "date"
    TIMESTAMP = "timestamp"


class TimestampKind(str, Enum):
    """Whether a timestamp represents an instant or a wall-clock value."""

    INSTANT = "instant"
    NAIVE = "timestamp_ntz"


@dataclass(frozen=True, slots=True)
class ResolvedDataType:
    """Resolved logical type plus source provenance.

    ``source_type`` and ``source_type_system`` are retained for diagnostics.
    Native adapters decide how this logical description is represented; no
    engine-specific target string is part of this contract.
    """

    kind: LogicalKind
    source_type: str
    source_type_system: str
    bit_width: int | None = None
    precision: int | None = None
    scale: int | None = None
    timestamp_kind: TimestampKind | None = None

    @property
    def unsigned(self) -> bool:
        """Whether this logical kind represents an unsigned integer."""
        return self.kind is LogicalKind.UNSIGNED_INTEGER

    def __post_init__(self) -> None:
        if self.kind in {LogicalKind.SIGNED_INTEGER, LogicalKind.UNSIGNED_INTEGER}:
            if self.bit_width not in {8, 16, 32, 64}:
                raise ConfigurationError(
                    "Integer bit_width must be one of 8, 16, 32, or 64",
                    details={
                        "bit_width": self.bit_width,
                        "source_type": self.source_type,
                    },
                )
        elif self.kind is LogicalKind.FLOAT:
            if self.bit_width not in {32, 64}:
                raise ConfigurationError(
                    "Float bit_width must be 32 or 64",
                    details={
                        "bit_width": self.bit_width,
                        "source_type": self.source_type,
                    },
                )
        elif self.bit_width is not None:
            raise ConfigurationError(
                "bit_width is only valid for integer datatypes",
                details={"source_type": self.source_type},
            )

        if self.kind is LogicalKind.DECIMAL:
            if self.precision is None or self.scale is None:
                raise ConfigurationError(
                    "Decimal types require explicit precision and scale",
                    details={"source_type": self.source_type},
                )
            if not 1 <= self.precision <= 38:
                raise ConfigurationError(
                    "Decimal precision must be between 1 and 38",
                    details={
                        "precision": self.precision,
                        "source_type": self.source_type,
                    },
                )
            if not 0 <= self.scale <= self.precision:
                raise ConfigurationError(
                    "Decimal scale must be between 0 and precision",
                    details={
                        "precision": self.precision,
                        "scale": self.scale,
                        "source_type": self.source_type,
                    },
                )
        elif self.precision is not None or self.scale is not None:
            raise ConfigurationError(
                "precision and scale are only valid for decimal datatypes",
                details={"source_type": self.source_type},
            )

        if self.kind is LogicalKind.TIMESTAMP and self.timestamp_kind is None:
            raise ConfigurationError(
                "Timestamp types require an explicit timestamp kind",
                details={"source_type": self.source_type},
            )
        if self.kind is not LogicalKind.TIMESTAMP and self.timestamp_kind is not None:
            raise ConfigurationError(
                "timestamp_kind is only valid for timestamp datatypes",
                details={"source_type": self.source_type},
            )


