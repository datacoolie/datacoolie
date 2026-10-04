"""Public logging API for system and execution logging.

The package initializer intentionally stays cold-import friendly.  Execution
logging imports the canonical aggregate runtime, which in turn imports source
and destination runtime packages; importing those packages while this module
is still initializing can re-enter the logging manager.  Public classes that
need those dependencies are therefore resolved lazily.
"""

from datacoolie.logging.configuration.constants import (
    ConsoleColor,
    LogCategory,
    LogLevel,
    LogType,
    PersistenceMode,
    StorageMode,
)


def __getattr__(name: str):
    """Resolve heavier public logging classes only when requested."""

    if name == "BaseLogger":
        from datacoolie.logging.base import BaseLogger

        return BaseLogger
    if name == "LogConfig":
        from datacoolie.logging.configuration.config import LogConfig

        return LogConfig
    if name == "ExecutionLogger":
        from datacoolie.logging.execution_logger import ExecutionLogger

        return ExecutionLogger
    if name == "create_execution_logger":
        from datacoolie.logging.execution_logger import create_execution_logger

        return create_execution_logger
    if name == "SystemLogger":
        from datacoolie.logging.system_logger import SystemLogger

        return SystemLogger
    if name == "create_system_logger":
        from datacoolie.logging.system_logger import create_system_logger

        return create_system_logger
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

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
