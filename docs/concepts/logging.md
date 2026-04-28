# Logging

**TL;DR** Two orthogonal loggers. `SystemLogger` captures framework Python logs
as JSONL for operators. `ETLLogger` writes structured execution logs in two
forms: debug JSONL and analyst Parquet.

## `SystemLogger`

- Captures framework Python logs through `LogManager`.
- JSONL output, `INFO` by default, configurable.
- Writes `system_log_<timestamp>_<job_num>_<job_index>_<job_id>.jsonl` under the
  configured `output_path`, optionally date-partitioned.
- Intended for operators reading runtime logs and troubleshooting failures.

## `ETLLogger`

- Writes two outputs for the same run session:
  - Debug JSONL for full-fidelity troubleshooting.
  - Analyst Parquet for query-friendly reporting and dashboards.
- Uses `_type` values `"dataflow_run_log"` (one per dataflow execution) and
  `"job_run_log"` (one per job run summary).
- Stores files under `{output_path}/{purpose}/{log_type}/__run_date=yyyy-mm-dd/`
  by default.

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
| `ANALYST` | `analyst` | Parquet output for dashboards and analysis |

`ETLLogger` uses `DEBUG/job_run_log` for the JSONL session file and
`ANALYST/job_run_log` plus `ANALYST/dataflow_run_log` for Parquet outputs.

## `ExecutionType` → `operation_type`

`operation_type` records the runtime operation:

- ETL runs typically leave `operation_type` as `etl`.
- Maintenance runs set `operation_type` to `maintenance`.

## Partitioning

`LogConfig` fields that affect storage:

- `output_path` — root directory
- `partition_by_date` — append a partition folder to output paths
- `partition_pattern` — override the partition folder layout
  (default: `__run_date={year}-{month}-{day}`)
- `flush_interval_seconds` — how often to upload pending buffers
- `storage_mode` — `memory` / `file` for temporary buffering before upload

## Related

- [How-to · Logging layout operations](../operations/logging-layout.md)
- [`reference/api/logging`](../reference/api/logging.md)
