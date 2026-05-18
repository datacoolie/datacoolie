---
title: Logging Layout — DataCoolie Operations
description: See the DataCoolie log folder structure, debug and analyst outputs, partitioning rules, and how to trace job runs in production.
---

# Logging layout

DataCoolie produces two independent log streams.

## Directory layout

```
<base_log_path>/
├── etl_logs/
│   ├── debug_json/
│   │   └── job_run_log/
│   │       └── __run_date=yyyy-mm-dd/job_<stem>.jsonl          ← appended
│   └── analyst/
│       ├── job_run_log/
│       │   └── __run_date=yyyy-mm-dd/job_run_log.jsonl         ← shared daily file, appended
│       └── dataflow_run_log/
│           └── __run_date=yyyy-mm-dd/dataflow_<stem>.parquet   ← per-run file
└── system_logs/
    └── __run_date=yyyy-mm-dd/system_log_<job_id>.log          ← plain text, appended
```

## Two loggers, two purposes

| | `ETLLogger` | `SystemLogger` |
|---|---|---|
| Written by | Driver, Stage, DataFlow, Watermark manager | Everywhere — platform, engines, sources, destinations, transformers |
| Format | Debug JSONL + analyst JSONL/Parquet | Plain text `.log`, one line per record |
| Purpose | Execution analytics, dashboards, troubleshooting | Operational debugging |
| Retention | Long-term (feeds dashboards) | Short-term (rotate aggressively) |
| Flush | Periodic `append_file` + final on close | Periodic `append_file` (timer) + final on close |

## SystemLogger levels

`SystemLogger` supports two independent log levels:

- **`log_level`** (default `INFO`) — what is printed to the console.  Set by
  the Driver configuration.
- **`file_level`** (default `DEBUG`) — what is captured to the `.log` file.
  Captures all framework messages regardless of the console level, acting as a
  "black box recorder" for post-mortem diagnosis.

## Analyst outputs

| Log type | Format | File per … | Query |
|---|---|---|---|
| `job_run_log` | JSONL | Day (shared, appended) | Read one file per day for job history |
| `dataflow_run_log` | Parquet (Snappy) | Job run | Scan with Spark / Polars / Athena |

The `job_run_log.jsonl` is a **shared daily file**: every job run on the same
date appends its summary line to the same file.  This makes it efficient to
query recent job history without listing many small per-run files.  It is also
hive-partition compatible (`__run_date=yyyy-mm-dd`) so Spark / Polars can
discover the `run_date` column automatically.

## Partitioning

ETL logs are partitioned by **purpose** and **log type**, then by run date:

```
etl_logs/analyst/dataflow_run_log/__run_date=2026-01-03/dataflow_<stem>.parquet
etl_logs/analyst/job_run_log/__run_date=2026-01-03/job_run_log.jsonl
```

Query them directly with Spark / Polars / Athena.

## Configuring

```python
driver = DataCoolieDriver(
    engine=engine,
    metadata_provider=metadata,
    base_log_path="s3://my-bucket/logs",  # or local path
)
```

Use `log_config=LogConfig(...)` when you need to override partition pattern,
flush interval, temporary storage mode, or the `file_level` for `SystemLogger`.

## Debug mode

When ETL logging is enabled, debug JSONL is written under the `debug_json`
purpose folder. `LogPurpose.DEBUG.value == "debug_json"`.

## Downstream use

- Build a dashboard from `etl_logs/analyst/dataflow_run_log/` and
  `etl_logs/analyst/job_run_log/`.
- Alert on `dataflow_run_log.status = "failed"`.
- If you run negative tests on purpose, suppress them with your own scenario or
  job naming convention; the current runner does not automatically mark a
  failure as expected.
