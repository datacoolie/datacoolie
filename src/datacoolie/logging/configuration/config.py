"""Configuration and intrinsic validation for framework logging."""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import Optional, TypeVar

from datacoolie.logging.configuration.constants import (
    ConsoleColor,
    DEFAULT_PARTITION_PATTERN,
    LogLevel,
    PersistenceMode,
    StorageMode,
)
from datacoolie.logging.persistence.layout import validate_partition_pattern
from datacoolie.utils.path_utils import normalize_path

_EnumT = TypeVar("_EnumT", bound=Enum)


def _normalize_enum_string(
    value: object,
    *,
    enum_type: type[_EnumT],
    field_name: str,
    uppercase: bool,
) -> str:
    if isinstance(value, enum_type):
        return str(value.value)
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string or {enum_type.__name__}")
    normalized = value.upper() if uppercase else value.lower()
    supported = {str(member.value) for member in enum_type}
    if normalized not in supported:
        choices = ", ".join(sorted(supported))
        raise ValueError(f"{field_name} must be one of: {choices}")
    return normalized


def normalize_log_level(value: object, *, field_name: str = "log_level") -> str:
    """Normalize a supported framework log level or fail fast."""
    return _normalize_enum_string(
        value,
        enum_type=LogLevel,
        field_name=field_name,
        uppercase=True,
    )


def normalize_storage_mode(value: object) -> str:
    """Normalize the local capture storage mode."""
    return _normalize_enum_string(
        value,
        enum_type=StorageMode,
        field_name="storage_mode",
        uppercase=False,
    )


def normalize_persistence_mode(value: object) -> str:
    """Normalize the remote persistence mode."""
    return _normalize_enum_string(
        value,
        enum_type=PersistenceMode,
        field_name="persistence_mode",
        uppercase=False,
    )


def normalize_console_color(value: object) -> str:
    """Normalize the dependency-free console color policy."""
    return _normalize_enum_string(
        value,
        enum_type=ConsoleColor,
        field_name="console_color",
        uppercase=False,
    )


@dataclass
class LogConfig:
    """Declared configuration for framework loggers."""

    log_level: str = LogLevel.INFO.value
    file_level: str = LogLevel.DEBUG.value
    storage_mode: str = StorageMode.MEMORY.value
    output_path: Optional[str] = None
    partition_by_date: bool = True
    partition_pattern: str = DEFAULT_PARTITION_PATTERN
    persistence_mode: str = PersistenceMode.SNAPSHOT.value
    flush_interval_seconds: float = 300.0
    flush_batch_bytes: int = 4 * 1024 * 1024
    buffer_memory_bytes: int = 64 * 1024 * 1024
    spool_max_bytes: int = 512 * 1024 * 1024
    spool_directory: Optional[str] = None
    close_timeout_seconds: float = 10.0
    console_color: str = ConsoleColor.AUTO.value

    def __post_init__(self) -> None:
        self.log_level = normalize_log_level(self.log_level, field_name="log_level")
        self.file_level = normalize_log_level(self.file_level, field_name="file_level")
        self.storage_mode = normalize_storage_mode(self.storage_mode)
        self.persistence_mode = normalize_persistence_mode(self.persistence_mode)
        self.console_color = normalize_console_color(self.console_color)
        validate_partition_pattern(self.partition_pattern)

        if not isinstance(self.partition_by_date, bool):
            raise ValueError("partition_by_date must be a boolean")
        for field_name in ("output_path", "spool_directory"):
            value = getattr(self, field_name)
            if value is None:
                continue
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string or None")
            setattr(self, field_name, normalize_path(value))

        if isinstance(self.flush_interval_seconds, bool):
            raise ValueError(
                "flush_interval_seconds must be zero or a positive finite number"
            )
        try:
            interval = float(self.flush_interval_seconds)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "flush_interval_seconds must be zero or a positive finite number"
            ) from exc
        if not math.isfinite(interval) or interval < 0:
            raise ValueError(
                "flush_interval_seconds must be zero or a positive finite number"
            )
        self.flush_interval_seconds = interval

        for field_name in (
            "flush_batch_bytes",
            "buffer_memory_bytes",
            "spool_max_bytes",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{field_name} must be a positive integer")
        if self.spool_max_bytes < self.buffer_memory_bytes:
            raise ValueError("spool_max_bytes must be at least buffer_memory_bytes")

        if isinstance(self.close_timeout_seconds, bool):
            raise ValueError("close_timeout_seconds must be a positive finite number")
        try:
            close_timeout = float(self.close_timeout_seconds)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "close_timeout_seconds must be a positive finite number"
            ) from exc
        if not math.isfinite(close_timeout) or close_timeout <= 0:
            raise ValueError("close_timeout_seconds must be a positive finite number")
        self.close_timeout_seconds = close_timeout
