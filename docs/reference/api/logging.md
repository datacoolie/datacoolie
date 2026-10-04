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

Default console formatting shows optional `dataflow_id` and `event_name` in separate brackets
after the logger name, for example `[orders] [dataflow.started]`. Missing values are omitted.
The default console omits `dataflow_run_id`; persisted records retain it as a separate field,
and custom formats can explicitly render it with `%(dataflow_run_id)s`.

Dataflow lifecycle and replay messages include the authored name and logical route:
`name | source_full_table_name → destination_full_table_name | details`.
The emitter builds this message without adding name or route fields to logging context.
Use the structured ID and event fields for filtering and persistence.

`DataCoolieRunConfig.run_attributes` is documented with the [runtime configuration API · Run configuration](../runtime-configuration.md#run-configuration). It is persisted
only in the job-runtime JSON record as the deterministic `run_attributes` string. Persisted
records use `log_schema_version=6`. A logger's `log_session_id` correlates records from one
activated Driver logging session; `job_id` identifies the job lifecycle, while `dataflow_run_id`
identifies an individual execution or replay-chunk instance (`dataflow_id` identifies the logical
flow). `LogConfig.console_color` accepts `"auto"`, `"always"`, or `"never"` and affects only the
owned console handler; capture and persisted JSON remain plain.

### v6 runtime projection

The v6 dataflow record keeps authored metadata fields and adds an explicit
runtime shape:

- `read_request` is a nullable native object containing the original source
  request. Watermark-shaped bounds use the tagged scalar representation;
  `reference_time` remains an ISO timestamp.
- `attempts` is a native array of detached terminal attempt objects. It is not
  a JSON-encoded string and it is empty when no pipeline attempt was admitted.
- Each attempt contains `source`, `transform`, `destination`, and `watermark`
  phase objects. Source `selection` and watermark `candidate` are native nested
  objects; a candidate is `{ "values": ..., "kind": ... }` or `null`.
- `metrics` is a native object with nullable counts and byte measurements.
  `null` means the measurement is unavailable; it is not an implicit zero.
  `retry_attempts` and the root identity/timing/status fields remain derived
  root fields.

Diagnostic detail is bounded per persisted dataflow or system record by the shared
`LogConfig.diagnostic_max_bytes` budget (65,536 bytes by default). The encoder keeps a valid
UTF-8 prefix, omits a value when it cannot preserve a valid projection, and may add at most 16
`diagnostic_issues` markers with `field` and `reason` (`truncated`, `omitted`, or `invalid`).
The complete JSON Lines record is capped by `LogConfig.max_record_bytes` (1 MiB by default).
See [diagnostic limits and loss accounting](../concepts/logging.md#diagnostic-limits-and-loss-accounting)
for the shared message/detail budget, marker semantics and exact scope of job drop counters.

The root does not duplicate phase fields such as `source_read_range`,
`source_watermark_end`, flattened source/transform/destination counts, or
checkpoint aliases. System records may carry an optional positive
  `attempt_number` for diagnostic correlation. Historical v3/v4/v5 records remain
  historical evidence; the v6 writer does not rewrite them or emit a dual shape.
