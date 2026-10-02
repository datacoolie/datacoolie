---
title: Logging — Python API Reference | DataCoolie
description: Python API reference for DataCoolie system and execution logging.
---

# Logging

`datacoolie.logging` exposes two application-facing logger types: `SystemLogger` for operational
records and `ExecutionLogger` for structured execution records. Runtime capture, context
propagation, formatting, and persistence modules are internal implementation details.

`LogCategory.EXECUTION` and `LogCategory.SYSTEM` define the two default top-level folders used by
the Driver when it creates loggers from a shared `log_base_path`.

::: datacoolie.logging.configuration.constants
    options:
      members:
        - LogCategory
        - LogType
        - LogLevel
        - PersistenceMode
        - StorageMode
        - ConsoleColor
        - LogEvent
        - FlushResult
        - LOG_SCHEMA_VERSION

::: datacoolie.logging.execution_logger
    options:
      members:
        - ExecutionLogger
        - create_execution_logger

::: datacoolie.logging.system_logger
    options:
      members:
        - SystemLogger
        - create_system_logger

Console formatters render the independent correlation fields as optional context segments. The
`dataflow_id` segment is separate, while the second segment is
`[dataflow_run_id:event_name]` when both values exist (or contains whichever one is present).
For example: `[orders] [run-123:dataflow.started]`. These identifiers are not appended to the
semantic `message`; use the structured fields for filtering and persistence.

`DataCoolieRunConfig.run_attributes` is documented with the [runtime configuration API · Run configuration](../runtime-configuration.md#run-configuration). It is persisted
only in the job-runtime JSON record as the deterministic `run_attributes` string. Persisted
records set `log_schema_version=4`. A logger's `log_session_id` correlates records from one activated
Driver logging session; `job_id` identifies the job lifecycle, while `dataflow_run_id` identifies
an individual execution instance (`dataflow_id` identifies the logical flow). `LogConfig.console_color`
accepts `"auto"`, `"always"`, or `"never"` and affects only the owned console handler; capture and
persisted JSON remain plain.
