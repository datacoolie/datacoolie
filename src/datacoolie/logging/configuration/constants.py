"""Logging-owned enum values and shared constants."""

from __future__ import annotations

from enum import Enum


class LogCategory(str, Enum):
    """Top-level log directories under a shared ``log_base_path``."""

    EXECUTION = "execution_logs"
    SYSTEM = "system_logs"


class LogType(str, Enum):
    """Structured log record kind identifiers."""

    JOB_RUN_LOG = "job_run_log"
    DATAFLOW_RUN_LOG = "dataflow_run_log"
    SYSTEM_LOG = "system_log"


class LogLevel(str, Enum):
    """Supported framework logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class StorageMode(str, Enum):
    """Local fallback storage used by captured Python records."""

    MEMORY = "memory"
    FILE = "file"


class PersistenceMode(str, Enum):
    """Remote JSON Lines persistence strategy."""

    SNAPSHOT = "snapshot"
    BATCH = "batch"


class ConsoleColor(str, Enum):
    """Console color policy for human-readable logging output."""

    AUTO = "auto"
    ALWAYS = "always"
    NEVER = "never"


class LogEvent(str, Enum):
    """Stable names for framework diagnostic lifecycle events."""

    SESSION_STARTING = "session.starting"
    SESSION_READY = "session.ready"
    SESSION_STARTUP_FAILED = "session.startup_failed"
    SESSION_FINISHING = "session.finishing"
    OPERATION_STARTED = "operation.started"
    OPERATION_FINISHED = "operation.finished"
    OPERATION_FAILED = "operation.failed"
    DATAFLOW_STARTED = "dataflow.started"
    DATAFLOW_FINISHED = "dataflow.finished"
    REPLAY_STARTED = "replay.started"
    REPLAY_FINISHED = "replay.finished"
    SCHEDULER_ADMISSION_STOPPED = "scheduler.admission_stopped"
    SCHEDULER_EXECUTION_FAILED = "scheduler.execution_failed"
    METADATA_INITIALIZED = "metadata.initialized"
    QUERY_FILE_RESOLVED = "query.file_resolved"
    PREPARATION_FINISHED = "preparation.finished"
    RETRY_SCHEDULED = "retry.scheduled"


class FlushResult(str, Enum):
    """Outcome of a non-raising writer flush attempt.

    ``WRITTEN`` is the only outcome that represents a completed remote write.
    ``NO_WORK`` and ``IN_FLIGHT`` are deliberately distinct so lifecycle code
    cannot report a skipped operation as successful.
    """

    WRITTEN = "written"
    NO_WORK = "no_work"
    IN_FLIGHT = "in_flight"

    def __bool__(self) -> bool:
        """Preserve the old boolean writer contract for successful writes."""

        return self is FlushResult.WRITTEN


DEFAULT_PARTITION_PATTERN = "__run_date={year}-{month}-{day}"
# Removing the second, random logging identity is a persisted envelope change.
# Consumers can therefore distinguish the job-owned identity contract from the
# previous v2 files without a dual-write compatibility path.
LOG_SCHEMA_VERSION = 4
INTERNAL_LOGGER_NAME = "datacoolie.logging.internal"
