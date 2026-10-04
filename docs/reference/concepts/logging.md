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

System records emitted inside an admitted retry attempt may also contain the
optional positive `attempt_number`. It is diagnostic correlation only: records
outside an attempt omit it, and it does not replace `dataflow_run_id`.

The persisted record keeps `dataflow_id`, `dataflow_run_id`, and `event_name` as independent fields.
Default console formatting shows `dataflow_id` and `event_name` in separate brackets after the
logger name, omitting any value that is absent. `dataflow_run_id` stays in persisted records
and is available to custom formats, but is omitted from the default console prefix.

Dataflow lifecycle and replay messages show the authored name, logical source and destination,
and event details. For example:

```text
2026-10-03 04:43:31,003 [INFO] datacoolie.orchestration.execution.lifecycle [orders-id] [dataflow.finished] - orders_to_bronze | `sales`.`public`.`orders` → `lakehouse`.`bronze`.`orders` | operation=etl, status=succeeded, duration=1.014s
```

Routes use the models' `full_table_name` properties, preserving their quoting; they do not resolve
storage paths or print SQL. Query-only and Python-function-only sources use `<query>` and
`<python_function>` labels. Missing names/tables use `<unnamed>` and `<unknown>`.
Control characters in labels are escaped. The emitter creates the message, which is also
captured in system logs; name and route are not added to `LogRecord` context.
Identifiers and events remain separate persisted fields for correlation. Session/job messages
and third-party output keep their own shapes.

## ExecutionLogger

Job and dataflow records also include the automatic `datacoolie_version` field.
All three record kinds carry `log_session_id`. Driver assigns the same value
to both loggers for its session, so reuse of a caller-supplied `job_id` does not
erase session identity. Correlate individual executions with `dataflow_run_id`;
replay chunks have their own execution IDs.
Package releases and log schema versions evolve independently. Current records
use `log_schema_version=6`; readers of historical logs must tolerate older
records and an absent producer version. The v6 writer has one explicit shape
per record type and does not rewrite historical files.

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
The dataflow record keeps declarative metadata, including the original
`source_query`. Runtime `source_action` is a native nested object and may
contain the exact SQL sent to the engine, including runtime predicates.

The job record uses `_type="job_run_log"`; dataflow records use `_type="dataflow_run_log"`. The job
record is always a replace-one snapshot, even when dataflow persistence uses batch mode. It
contains aggregate counters, component names, lifecycle status, `log_records_dropped`,
`log_bytes_dropped`, `message_truncated`, and the optional caller-owned `run_attributes`
string. Call `ExecutionLogger.finish_job(...)` to provide the
business outcome when using the logger without a Driver; `close()` alone does not invent success.

Every terminal dataflow and phase runtime uses one optional `message` field. For failed terminal
observations, the JobRuntime `message` is a compact index of failed dataflow identities (`name [id]`,
with an ID-only fallback), joined with `; `. For skipped observations, the same field carries the
human-readable skip reason. The dataflow row retains phase details within its diagnostic budget;
messages and optional details may be truncated or omitted. Driver-owned session
failures (for example startup,
scheduler-contract, or provider-teardown failures) are appended only when they are independent
of an already observed terminal dataflow, so one failure is not counted twice across the two
summary owners. System records retain event context and diagnostic message/exception text within
their budget. Exception text can be truncated or omitted; inspect `diagnostic_issues` before
assuming a persisted traceback is complete. A concise message plus the traceback's exception
line is intentional when both are retained and aid diagnosis.

The v6 dataflow root keeps identity, operation, timing, status, message, and
derived `retry_attempts`. It also contains:

- `read_request`: a nullable native object with the caller's original
  `SourceReadRequest`; `reference_time` is an ISO timestamp and watermark-shaped
  bounds use tagged values.
- `attempts`: a native array of detached terminal `AttemptRuntimeInfo` objects.
  It is `[]` when no pipeline attempt was admitted; it is never a JSON string.
- `metrics`: native `ExecutionMetrics` for the final attempt. Counts and byte
  measurements are nullable, so unknown remains `null` rather than `0`.

Each attempt contains `attempt_number`, timing/status/message, and optional
`source`, `transform`, `destination`, and `watermark` objects. The source phase
contains nullable `rows_read`, native `source_action`, and native `selection`
with `mode`, `lower`, `upper`, operators, `combine_operator`, and
`upper_origin`. It does not contain the removed root-level source watermark or
range fields. The watermark phase contains `load_status`, `loaded_watermark`,
`merge_base`, `candidate`, `watermark_to_save`, `save_status`, and
`saved_watermark`; `candidate` is `{values, kind}` or `null`.

Transform and destination phase fields remain nested and explicit. Their counts
and bytes preserve unavailable measurements as `null`. A diagnostic capture
failure cannot retry an acknowledged business phase; a write adapter that
raises before acknowledging its physical write remains a business failure.

`ReplayRuntimeInfo` summarizes an outer replay with `chunk_run_ids`, aggregate
`metrics`, and aggregate `retry_attempts`; it does not invent an outer attempt.
Admitted chunks retain separate run IDs and histories. `JobRuntimeInfo` keeps
status counters exact and applies per-metric null propagation to its ingestion
totals; maintenance row metrics do not contribute to those totals, while file
and byte metrics do.

Watermark-shaped values in `read_request`, read-range bounds, source selection,
candidate values, and watermark phase fields use the canonical tagged scalar
codec. Dates, datetimes, times, decimals, and bytes are represented as nested
objects with `__date__`, `__datetime__`, `__time__`, `__decimal__`, or
`__binary__` keys. Native null, booleans, finite numbers, strings, lists, and
string-keyed mappings remain native. Tagged values are never JSON-encoded
strings; authored metadata keeps its existing encoding.

The serializer is an allowlist. Adding a Python runtime attribute cannot add a
log field, and removed v4 root fields or checkpoint aliases are not reintroduced
as compatibility projections. Bounded persistence, drop accounting, and
protected job-summary behavior remain best effort and do not repeat business
work.

### Consumer migration from v4

Read `attempts` directly as an array and select its final element for phase
diagnostics; use root `metrics` for run totals. Do not sum retries as separate
runs. Replace flattened source/transform/destination fields with their nested
phase fields. Read persistence progress from `attempts[-1].watermark.save_status`
and selection from `attempts[-1].source.selection`; neither is an execution
instruction. An empty attempt array means execution never entered the pipeline.

Unknown metrics are `null`; phases not entered contribute zero to run metrics.
Only the final attempt contributes to job metric totals. Optional diagnostic
payloads (`source_action`, `transformers_applied`, `operation_details`) may be
omitted when their JSON encoding fails; consumers must not require them.
Nested timings remain ISO timestamps and durations remain numeric seconds.
Decode tagged bound/watermark values when exact temporal, decimal or binary
types matter. Existing metadata payload encoding is unchanged.

### Diagnostic limits and loss accounting

`LogConfig.diagnostic_max_bytes` defaults to 65,536 encoded UTF-8 bytes for diagnostic values
in one persisted dataflow or system record. Dataflow root, attempt and phase messages share
this allowance with `source_action`, `transformers_applied` and `operation_details`; each
attempt does not receive a separate allowance. System `msg` and `exc_info` share one allowance.
Oversized text keeps a valid UTF-8 prefix. Unsupported, cyclic, overly deep or oversized
container values may be omitted. Arbitrary custom objects are omitted rather than converted
through their string hooks; supported temporal, Decimal, bytes and masked-secret values retain
their diagnostic JSON representation within the budget. Diagnostic scalar encoding is separate
from the tagged watermark encoding described above.

The optional `diagnostic_issues` array reports affected fields as JSON-pointer paths and reasons
`truncated`, `omitted` or `invalid`. It is absent when there are no issues and contains at most
16 entries, with paths bounded to 192 UTF-8 bytes. The final `{"field":"","reason":"omitted"}`
marker means additional issue details were omitted; it does not mean the entire root was omitted.
Consumers must tolerate missing optional detail and use these markers to detect incomplete text.

`LogConfig.max_record_bytes` defaults to 1 MiB for the complete encoded JSON Lines record,
including its newline, for all three record types. Identity, status, metrics, read requests,
selection, watermark values and authored metadata are never truncated. A record that cannot
fit is rejected as a whole. These limits bound logging normalization/encoding; they do not bound
caller-owned allocations, earlier runtime snapshots or Python message/traceback formatting.
The job message keeps its existing separate truncation policy and `message_truncated` flag.

When dataflow-detail persistence is enabled, job `log_records_dropped` counts terminal
dataflow/maintenance observations rejected during projection, encoding or writer admission,
once per submitted observation. Business totals still include a valid terminal observation
whose detail record is rejected. Disabled detail persistence, an admitted record with omitted
diagnostics, repeated job-snapshot rejection and an upload waiting for retry do not increment
this counter. System capture/writer drop statistics are separate from these execution counters.

`log_bytes_dropped` sums only known full encoded byte lengths. A rejection before that length
is known adds no bytes, so zero does not prove that no records were lost; it is not a total
estimate of all lost bytes. Use it alongside `log_records_dropped` and persistence health.

If the latest job snapshot is rejected, the previous immutable payload can still retry within
the existing deadlines. Publishing that older snapshot does not publish the latest summary:
the latest summary remains dirty. Protected job capacity does not make an individually
oversized job record publishable, and persisted job counters can be absent or stale after
publication failure.

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
- `diagnostic_max_bytes`: shared UTF-8 diagnostic budget per persisted dataflow/system record (65,536 bytes by default).
- `max_record_bytes`: maximum encoded JSON Lines record size including its newline (1 MiB by default).

At capacity, new records are dropped and counted in persistence statistics; business execution does not
fail. A failed upload keeps the exact frozen payload and destination for a later retry; records admitted
after that failure form a later batch. Startup and close waits are bounded, and a timed-out or skipped
write is never reported as successful. Terminal close drains already-admitted parts while its shared
deadline allows; a sink that remains in flight is reported as timed out. The platform owns cloud
replacement semantics, so cross-file atomicity and exactly-once delivery are not claimed.

The logging contract is framework-first. DataCoolie Studio consumers migrate after framework fixtures
and schema/layout verification are complete.
