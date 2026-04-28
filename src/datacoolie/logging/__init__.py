"""Logging package — base infrastructure, system logger, ETL logger.

Provides:

* :class:`LogManager` — singleton for configuring Python logging.
* :class:`BaseLogger` — ABC for persistent loggers.
* :class:`SystemLogger` — captures and uploads Python log output.
* :class:`ETLLogger` — structured dataflow/maintenance execution logs.
* :func:`get_logger` — convenience factory for child loggers.
"""

from datacoolie.logging.base import (
    BaseLogger,
    CaptureHandler,
    DataflowContextFilter,
    LogConfig,
    LogLevel,
    LogManager,
    LogRecord,
    StorageMode,
    get_logger,
)
from datacoolie.logging.context import (
    clear_dataflow_id,
    get_dataflow_id,
    set_dataflow_id,
)
from datacoolie.logging.etl_logger import ETLLogger, create_etl_logger
from datacoolie.logging.system_logger import SystemLogger, create_system_logger

__all__ = [
    "BaseLogger",
    "CaptureHandler",
    "DataflowContextFilter",
    "ETLLogger",
    "LogConfig",
    "LogLevel",
    "LogManager",
    "LogRecord",
    "StorageMode",
    "SystemLogger",
    "clear_dataflow_id",
    "create_etl_logger",
    "create_system_logger",
    "get_dataflow_id",
    "get_logger",
    "set_dataflow_id",
]
