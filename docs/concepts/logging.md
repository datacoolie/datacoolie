---
title: Logging Model — DataCoolie Concepts
description: Understand SystemLogger and ETLLogger outputs, file layouts, event shapes, and how DataCoolie separates operator logs from analyst logs.
---

# Logging

**TL;DR** Two orthogonal loggers. `SystemLogger` captures framework Python logs
as structured JSONL for operators. `ETLLogger` writes structured execution logs
in two forms: debug JSONL (appended per run) and analyst outputs (JSONL for job
summaries, Parquet for dataflow detail).

## `SystemLogger`

- Captures framework Python logs through `LogManager`.
- Uses `datacoolie` as the framework root namespace. `get_logger(__name__)`
  preserves the full module logger name, and standard `datacoolie.*` child
  loggers propagate into the same capture pipeline.
- Preserves records emitted before and during Driver/SystemLogger
  reconfiguration. When capture remains enabled, `LogManager` keeps the same
  `CaptureHandler` attached and changes its level, formatter, or memory/file
  storage under a lock. Records already accepted remain ordered and are not
  re-filtered by a later `file_level`.
- Keeps LogManager's own diagnostic messages console-only so capture and flush
  failures cannot recursively write into the same buffer.
- **Two independent levels:**
    - `log_level` — controls what is printed to the console (default `INFO`).
    - `file_level` — controls what is captured to the file (default `DEBUG`,
      capturing all framework messages regardless of console level).
- Writes one JSON object per line using `LogRecord.to_dict()`. Core keys are
  `ts`, `level`, `logger`, and `msg`; source-location, `dataflow_id`, and
  exception fields are included when available.
- Filename: `system_log_<YYYYMMDD_HHMMSS>_<job_id>.jsonl` under the configured
  `output_path`, optionally date-partitioned.
- **Periodic flush** via background timer — appends new records to the remote
  file using `platform.append_file()` at each interval.  Final remaining
  records are appended on `close()`.
- Intended for operators reading runtime logs and troubleshooting failures.

## `ETLLogger`

- Writes two outputs for the same run session:
  - Debug JSONL for full-fidelity troubleshooting.
  - Analyst outputs for query-friendly reporting and dashboards.
- Uses `_type` values `"dataflow_run_log"` (one per dataflow execution) and
  `"job_run_log"` (one per job run summary).
- Stores files under `{output_path}/{purpose}/{log_type}/__run_date=yyyy-mm-dd/`
  by default.

### Debug JSONL

- Single JSONL file per session: `debug_json/job_run_log/__run_date=.../job_<stem>.jsonl`
- **Periodic flush**: new bytes appended via `platform.append_file()` at each
  flush interval.  Job summary line appended as the final line on `close()`.
- Per-dataflow entries followed by a final `job_run_log` summary line.

### Analyst Outputs

| Log type | Format | Path | Flush strategy |
|---|---|---|---|
| `job_run_log` | JSONL (one line per job run) | `analyst/job_run_log/__run_date=.../job_<stem>.jsonl` | Immutable `write_file` on close |
| `dataflow_run_log` | Parquet (one row per dataflow) | `analyst/dataflow_run_log/__run_date=.../dataflow_<stem>.parquet` | `upload_file` on close |

Each job run creates one immutable `job_run_log` file. Jobs never read or
rewrite another run's summary, so concurrent writers can safely target the
same date partition on local, Fabric, Databricks, and AWS storage. Consumers
read all `*.jsonl` files in the partition; legacy shared daily files remain
valid inputs during rollout.

Row shape (dataflow entry):

```json
{
  "_type": "dataflow_run_log",
  "job_id": "job-1",
  "dataflow_id": "orders_bronze_to_silver",
  "stage": "bronze2silver",
  "processing_mode": "batch",
  "operation_type": null,
  "status": "succeeded",
  "source_rows_read": 12345,
  "destination_rows_written": 12345,
  "transformers_applied": ["SchemaConverter", "Deduplicator", "SystemColumnAdder", "PartitionHandler"],
  "start_time": "2026-04-20T08:00:00+00:00",
  "end_time": "2026-04-20T08:00:09+00:00",
  "duration_seconds": 9.0,
  "overhead_duration_seconds": 0.2,
  "destination_load_type": "merge",
  "destination_operation_type": null
}
```

`job_run_log` summary rows aggregate session totals such as
`total_dataflows`, `total_succeeded`, `total_failed`, `total_running`,
`total_pending`, `total_rows_written`, and `operation_types`. `job_id` uniquely
identifies the job run. Job and dataflow artifacts use the same
`<timestamp>_<job_num>_<job_index>_<job_id>` filename stem.

Each dataflow entry also projects transformer metadata for analysis:
`transform_select_columns`, `transform_drop_columns`,
`transform_rename_columns`, `transform_value_rules`,
`transform_hash_columns`, `transform_masking_rules`, and
`transform_configure`. Configuration, collection, and rule fields are JSON
strings in analyst Parquet and JSON-encoded values in the debug entry. Masking
replacement values and value-rule mappings are retained as metadata; protect
log storage with the same access controls used for pipeline metadata.

If a source, transformer, or destination fails, the dataflow row keeps the
available phase-level status, error, and partial timing information collected
before the exception.

An expected-failure test remains a real ETL failure in both log streams. A
scenario runner may report that scenario as passed after matching its expected
exit code and error text, but `SystemLogger` still contains the `ERROR` record
and `ETLLogger` still records the failed dataflow.

## `LogPurpose`

Enum that controls the output folder and intended audience:

| Enum | `.value` | Meaning |
|---|---|---|
| `DEBUG` | `debug_json` | JSONL debug output for troubleshooting |
| `ANALYST` | `analyst` | Analyst outputs for dashboards and analysis |

`ETLLogger` uses `DEBUG/job_run_log` for the JSONL debug session file,
`ANALYST/job_run_log` for the immutable per-run job summary JSONL, and
`ANALYST/dataflow_run_log` for the per-run Parquet.

## `ExecutionType` → `operation_type`

`operation_type` records the runtime operation:

- ETL runs typically leave `operation_type` as `etl`.
- Maintenance runs set `operation_type` to `maintenance`.

## Partitioning

`LogConfig` fields that affect storage:

- `output_path` — root directory
- `log_level` — console stream level (default `INFO`)
- `file_level` — capture / file level for `SystemLogger` (default `DEBUG`)
- `partition_by_date` — append a partition folder to output paths
- `partition_pattern` — override the partition folder layout
  (default: `__run_date={year}-{month}-{day}`). Supported placeholders are
  `{year}`, `{month}`, `{day}`, and `{hour}`. The placeholders must form one
  ordered prefix—year, year/month, year/month/day, or all four—and every
  directory level must contain at least one placeholder. Literal text and
  separators may vary but cannot contain digits or `%`. Invalid patterns fail
  when `LogConfig` is created. For example,
  `__run_date={year}-{month}-{day}/__run_hour={hour}` creates hourly
  partitions that DataCoolie Studio can discover incrementally.
- `flush_interval_seconds` — how often to upload pending buffers
- `storage_mode` — `memory` / `file` for temporary buffering before upload

`LogConfig` normalises path separators in `output_path` when it is created.
Keep one partition layout per log root. To move an existing daily stream to
hourly partitions, start writing to a new empty log root; daily and hourly
history are not combined into one Studio source.

## Related

- [How-to · Logging layout operations](../operations/logging-layout.md)
- [`reference/api/logging`](../reference/api/logging.md)
