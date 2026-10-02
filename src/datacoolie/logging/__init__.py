"""Public logging API for system and execution logging.

The runtime manager, capture handler, context propagation, and persistence
helpers are implementation details. Applications should use
:class:`SystemLogger` for operational records and :class:`ExecutionLogger`
for structured run records.
"""

from datacoolie.logging.base import BaseLogger
from datacoolie.logging.configuration.config import LogConfig
from datacoolie.logging.configuration.constants import (
    ConsoleColor,
    LogCategory,
    LogLevel,
    LogType,
    PersistenceMode,
    StorageMode,
)
from datacoolie.logging.execution_logger import ExecutionLogger, create_execution_logger
from datacoolie.logging.system_logger import SystemLogger, create_system_logger

__all__ = [
    "BaseLogger",
    "ConsoleColor",
    "ExecutionLogger",
    "LogCategory",
    "LogConfig",
    "LogLevel",
    "LogType",
    "PersistenceMode",
    "StorageMode",
    "SystemLogger",
    "create_execution_logger",
    "create_system_logger",
]
