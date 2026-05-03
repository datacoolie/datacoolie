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
│   │       └── __run_date=yyyy-mm-dd/job_<stem>.jsonl
│   └── analyst/
│       ├── job_run_log/
│       │   └── __run_date=yyyy-mm-dd/job_<stem>.parquet
│       └── dataflow_run_log/
│           └── __run_date=yyyy-mm-dd/dataflow_<stem>.parquet
└── system_logs/
    └── __run_date=yyyy-mm-dd/system_log_<ts>_<job_num>_<job_index>_<job_id>.jsonl
```

## Two loggers, two purposes

| | `ETLLogger` | `SystemLogger` |
|---|---|---|
| Written by | Driver, Stage, DataFlow, Watermark manager | Everywhere — platform, engines, sources, destinations, transformers |
| Format | Debug JSONL plus analyst Parquet | JSONL, one event per line |
| Purpose | Execution analytics, dashboards, troubleshooting | Operational debugging |
| Retention | Long-term (feeds dashboards) | Short-term (rotate aggressively) |

## Partitioning

ETL logs are partitioned by **purpose** and **log type**, then by run date:

```
etl_logs/analyst/dataflow_run_log/__run_date=2026-01-03/dataflow_<stem>.parquet
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
flush interval, or temporary storage mode.

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
