---
title: Logging Model — DataCoolie Concepts
description: Understand SystemLogger and ETLLogger outputs, file layouts, event shapes, and how DataCoolie separates operator logs from analyst logs.
---

# Logging

**TL;DR** Two orthogonal loggers. `SystemLogger` captures framework Python logs
as plain-text `.log` files for operators. `ETLLogger` writes structured execution
logs in two forms: debug JSONL (appended per run) and analyst outputs (JSONL for
job summaries, Parquet for dataflow detail).

## `SystemLogger`

- Captures framework Python logs through `LogManager`.
- **Two independent levels:**
    - `log_level` — controls what is printed to the console (default `INFO`).
    - `file_level` — controls what is captured to the file (default `DEBUG`,
      capturing all framework messages regardless of console level).
- Writes plain-text `.log` files: one line per record,
  format `timestamp - LEVEL - logger [dataflow_id] - message`.
- Filename: `system_log_<job_id>.log` under the configured `output_path`,
  optionally date-partitioned.
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
| `job_run_log` | JSONL (one line per job run) | `analyst/job_run_log/__run_date=.../job_run_log.jsonl` | `append_file` on close |
| `dataflow_run_log` | Parquet (one row per dataflow) | `analyst/dataflow_run_log/__run_date=.../dataflow_<stem>.parquet` | `upload_file` on close |

The `job_run_log` file is a **shared daily file** — multiple job runs on the
same day append their summary to the same file, making it easy to query
recent job history without listing many small per-run files.

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
  "destination_load_type": "merge",
  "destination_operation_type": null
}
```

`job_run_log` summary rows aggregate session totals such as
`total_dataflows`, `total_succeeded`, `total_failed`, and
`total_rows_written`.

## `LogPurpose`

Enum that controls the output folder and intended audience:

| Enum | `.value` | Meaning |
|---|---|---|
| `DEBUG` | `debug_json` | JSONL debug output for troubleshooting |
| `ANALYST` | `analyst` | Analyst outputs for dashboards and analysis |

`ETLLogger` uses `DEBUG/job_run_log` for the JSONL debug session file,
`ANALYST/job_run_log` for the appended job summary JSONL, and
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
  (default: `__run_date={year}-{month}-{day}`)
- `flush_interval_seconds` — how often to upload pending buffers
- `storage_mode` — `memory` / `file` for temporary buffering before upload

## Related

- [How-to · Logging layout operations](../operations/logging-layout.md)
- [`reference/api/logging`](../reference/api/logging.md)
