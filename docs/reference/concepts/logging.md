---
title: Logging Model — DataCoolie Concepts
description: System and execution logging contracts, JSON Lines persistence, run attributes, and lifecycle.
---

# Logging

DataCoolie has two independent streams:

- `SystemLogger` captures framework Python records for operational diagnosis.
- `ExecutionLogger` records terminal dataflow observations and one mutable job summary.

Both streams use the same bounded JSON Lines persistence writer. The default is a snapshot: one
remote `.json` file per stream is replaced with the latest complete snapshot. `persistence_mode="batch"`
publishes immutable `*_part_00000001.json` files after the configured size or time trigger. Files use
the `.json` extension for platform preview compatibility, but contain one compact JSON object per line.

## SystemLogger

`SystemLogger` is inert until `activate()`. Driver activates it (and then `ExecutionLogger`) after
pure configuration/provider binding but before provider metadata I/O, so provider startup records are
captured. `log_level` controls console output; `file_level` controls captured records. Capture
diagnostics remain console-only so a storage failure cannot recursively write to the same sink.

Each system record includes `log_schema_version`, then `_type="system_log"`,
then `datacoolie_version` (the installed producer package version), followed by
`log_session_id`, `job_id`, `job_num`, and `job_index`, then the `LogRecord` projection (`ts`, `level`, `logger`, `msg`, and optional source location,
exception, `event_name`, `dataflow_id`, and `dataflow_run_id`). Empty streams do not create a file.

Framework lifecycle anchors use stable dotted event names, including `session.starting`,
`session.ready`, `session.startup_failed`, `session.finishing`, `operation.started`,
`operation.finished`, `dataflow.started`, and `dataflow.finished`. The two dataflow identifiers are
bound only for the active execution scope and are restored when work returns; rejected operations do
not emit a started event. Event production never forces a remote flush.

The persisted record keeps `dataflow_id`, `dataflow_run_id`, and `event_name` as independent fields.
Console formatting adds them only as an optional display suffix: `dataflow_id` is shown in its own
brackets and `dataflow_run_id:event_name` in a second bracket, omitting any value that is absent.
For example, a fully correlated record is rendered as `[orders] [run-123:dataflow.started]`.
This is presentation-only; the original `message` remains the semantic message and is not rewritten
with identifiers, so structured consumers and persistence can query each field independently.

## ExecutionLogger

Job and dataflow records also include the automatic `datacoolie_version` field.
All three record kinds carry `log_session_id`. Driver assigns the same value
to both loggers for its session, so reuse of a caller-supplied `job_id` does not
erase session identity. Correlate individual executions with `dataflow_run_id`;
replay chunks have their own execution IDs.
Package releases and log schema versions evolve independently: readers ignore
unknown fields, and compatible optional additions keep the same schema version.
Readers of historical logs must tolerate an absent producer version.

Execution logging accepts only terminal `succeeded`, `failed`, or `skipped` runtime observations. Preparation
failures are terminal failed rows. An execution killed before it is admitted to a dataflow/chunk
boundary has no fabricated row; if the scheduler itself fails after admitting a dataflow, the
boundary creates one failed runtime for that dataflow so the job accounting remains truthful.
File persistence requires an activated logger with a platform and output path.
`activate()` supplies a default `DataCoolieRunConfig` when a standalone logger
has not been given one; Driver-owned loggers receive the Driver's validated
configuration before activation. Uploads and final close are bounded
best-effort operations, so a job summary may be absent or stale after a storage
failure. See [Logging layout](../../guide/operations/logging.md).
The dataflow record keeps declarative metadata, including the original `source_query`. The flattened
`source_action` JSON string may contain the exact SQL sent to the engine, including runtime predicates.

The job record uses `_type="job_run_log"`; dataflow records use `_type="dataflow_run_log"`. The job
record is always a replace-one snapshot, even when dataflow persistence uses batch mode. It
contains aggregate counters, component names, lifecycle status, `log_records_dropped`,
`log_bytes_dropped`, `message_truncated`, and the optional caller-owned `run_attributes`
string. Call `ExecutionLogger.finish_job(...)` to provide the
business outcome when using the logger without a Driver; `close()` alone does not invent success.

Every terminal dataflow and phase runtime uses one optional `message` field. For failed terminal
observations, the JobRuntime `message` is a compact index of failed dataflow identities (`name [id]`,
with an ID-only fallback), joined with `; `. For skipped observations, the same field carries the
human-readable skip reason. The dataflow row retains the full phase details. Driver-owned session
failures (for example startup,
scheduler-contract, or provider-teardown failures) are appended only when they are independent
of an already observed terminal dataflow, so one failure is not counted twice across the two
summary owners. System records retain the event context and complete exception chain; a concise
message plus the traceback's exception line is intentional when both aid diagnosis.

In flattened dataflow records, phase explanations are exposed as
`source_message`, `transform_message`, and `destination_message`. The top-level
`message` remains the explanation for the dataflow outcome; the phase fields
retain more specific partial-failure evidence.

## Layout

When created by Driver, `log_base_path` is split into the two component roots below. For standalone
loggers, `LogConfig.output_path` is already that logger's component root.

```text
execution_logs/
├── job_run_log/<partition>/job_<stem>.json
└── dataflow_run_log/<partition>/dataflow_<stem>.json
system_logs/<partition>/system_<stem>.json
```

Batch system/dataflow streams add `_part_<sequence:08d>` before `.json`. The stem contains the
job-start timestamp, job number/index, and a filesystem-safe job-id token. The raw job id remains
in each record alongside the session `log_session_id`. Date partitioning follows
`LogConfig.partition_pattern` and batch parts use their sealing time. Snapshot paths are frozen at
activation.

## Run attributes

`DataCoolieRunConfig.run_attributes` is an optional JSON object supplied by the caller for correlation
with an external orchestrator, for example:

```python
DataCoolieRunConfig(
    job_id="orders",
    run_attributes={"data_factory_run_id": "adf-123", "glue_job_run_id": "jr-456"},
)
```

Keys are caller-defined and are not interpreted by the framework. Objects, arrays, finite numbers,
booleans, strings, and null are accepted; raw JSON strings, non-string keys, cycles, and non-finite
numbers fail configuration validation. Attributes are serialized once, deterministically, in the job
summary and are not repeated in every dataflow/system record.

## Configuration

Applications configure logging through the two public logger types. The system logger owns
operational capture and the execution logger owns structured run records:

```python
from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.logging import ExecutionLogger, LogConfig, SystemLogger
from datacoolie.platforms.local_platform import LocalPlatform

platform = LocalPlatform()
run_config = DataCoolieRunConfig(
    job_id="standalone-logging",
    run_attributes={"owner": "example"},
)
log_config = LogConfig(output_path="logs")
system_logger = SystemLogger(log_config, platform)
execution_logger = ExecutionLogger(log_config, platform)
system_logger.set_run_config(run_config)
execution_logger.set_run_config(run_config)

# Nested contexts activate system capture first and close execution first.
with system_logger:
    with execution_logger:
        # Run the standalone work here and emit terminal dataflow rows as needed.
        execution_logger.finish_job("succeeded")
```

`SystemLogger.activate()` claims the process-wide capture session. Driver activates an injected or
auto-created `SystemLogger` before provider initialization, then activates the execution logger and
emits the session-starting anchor. An `ExecutionLogger` used without a `SystemLogger` does not
implicitly configure console/capture output.
Invalid levels or modes fail before handlers or an active capture owner are changed.

Important `LogConfig` fields:

- `persistence_mode`: `"snapshot"` (default) or `"batch"` for system/dataflow streams.
- `flush_interval_seconds`: `300` by default; `0` disables time-triggered flushes (batch size wakeups and final close still work).
- `flush_batch_bytes`: approximately 4 MiB by default in batch mode.
- `buffer_memory_bytes` / `spool_max_bytes`: bounded encoded-data budgets (64 MiB / 512 MiB defaults). The writer reserves space for both retained and temporary upload copies, so the practical retained limit is lower than `spool_max_bytes`.
- `spool_directory`: optional local spool directory.
- `close_timeout_seconds`: bounded terminal sink wait (10 seconds by default).
- `storage_mode`: controls only the internal capture buffer (`"memory"` or `"file"`).

At capacity, new records are dropped and counted in persistence statistics; business execution does not
fail. A failed upload keeps the exact frozen payload and destination for a later retry; records admitted
after that failure form a later batch. Startup and close waits are bounded, and a timed-out or skipped
write is never reported as successful. Terminal close drains already-admitted parts while its shared
deadline allows; a sink that remains in flight is reported as timed out. The platform owns cloud
replacement semantics, so cross-file atomicity and exactly-once delivery are not claimed.

The logging contract is framework-first. DataCoolie Studio consumers migrate after framework fixtures
and schema/layout verification are complete.
