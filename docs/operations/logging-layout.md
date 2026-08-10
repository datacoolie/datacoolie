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
│       │   └── __run_date=yyyy-mm-dd/job_<stem>.jsonl           ← per-run file
│       └── dataflow_run_log/
│           └── __run_date=yyyy-mm-dd/dataflow_<stem>.parquet   ← per-run file
└── system_logs/
    └── __run_date=yyyy-mm-dd/system_log_<timestamp>_<job_id>.jsonl
```

## Two loggers, two purposes

| | `ETLLogger` | `SystemLogger` |
|---|---|---|
| Written by | Driver, Stage, DataFlow, Watermark manager | Everywhere — platform, engines, sources, destinations, transformers |
| Format | Debug JSONL + analyst JSONL/Parquet | JSONL, one `LogRecord` per line |
| Purpose | Execution analytics, dashboards, troubleshooting | Operational debugging |
| Retention | Long-term (feeds dashboards) | Short-term (rotate aggressively) |
| Flush | Periodic debug append + immutable analyst files on close | Periodic `append_file` (timer) + final on close |

## SystemLogger levels

`SystemLogger` supports two independent log levels:

- **`log_level`** (default `INFO`) — what is printed to the console.  Set by
  the Driver configuration.
- **`file_level`** (default `DEBUG`) — what is captured to the JSONL file.
  Captures all framework messages regardless of the console level, acting as a
  "black box recorder" for post-mortem diagnosis.

## Framework namespace and startup capture

Framework loggers live under the `datacoolie` namespace. Calls such as
`get_logger(__name__)` retain their module name, while ordinary
`logging.getLogger("datacoolie...")` children propagate to the same capture
handler. Records emitted before the Driver finishes configuring logging are
preserved and transferred to `SystemLogger`, subject to the configured
`file_level`. LogManager's own diagnostics use a console-only handler to avoid
recursive capture.

## Analyst outputs

| Log type | Format | File per … | Query |
|---|---|---|---|
| `job_run_log` | JSONL | Job run (immutable) | Scan all JSONL files in the date partition |
| `dataflow_run_log` | Parquet (Snappy) | Job run | Scan with Spark / Polars / Athena |

Each `job_<stem>.jsonl` contains one complete job summary and is created once.
It shares the `<timestamp>_<job_num>_<job_index>_<job_id>` stem with the
corresponding `dataflow_<stem>.parquet` artifact. Concurrent jobs therefore
never coordinate around or rewrite a shared object. The containing
`__run_date=yyyy-mm-dd` folder remains
Hive-partition compatible so Spark and Polars can discover the `run_date`
column automatically. Readers should include every `*.jsonl` file in the
partition; this also keeps existing shared daily files readable during
rollout.

## Partitioning

ETL logs are partitioned by **purpose** and **log type**, then by run date:

```
etl_logs/analyst/dataflow_run_log/__run_date=2026-01-03/dataflow_<stem>.parquet
etl_logs/analyst/job_run_log/__run_date=2026-01-03/job_<stem>.jsonl
```

Query them directly with Spark / Polars / Athena.

Hourly partitioning is opt-in. The default remains one partition directory
per day. To make each hour independently discoverable, configure:

```python
LogConfig(
    partition_pattern="__run_date={year}-{month}-{day}/__run_hour={hour}",
)
```

Studio-compatible patterns use one ordered prefix of the time tokens:

```text
{year}
{year} {month}
{year} {month} {day}
{year} {month} {day} {hour}
```

The tokens may be adjacent or separated by arbitrary non-numeric literal
text, and can share a directory level. Each directory level must contain a
token. For example, `logs_{year}--m_{month}__d_{day}++h_{hour}` and
`y={year}/m={month}/d={day}/h={hour}` are valid. Reversed/skipped tokens,
duplicate or unknown placeholders, numeric literals, `%`, and tokenless
directory levels fail when `LogConfig` is created.

This produces paths such as:

```text
etl_logs/analyst/job_run_log/__run_date=2026-01-03/__run_hour=09/job_<stem>.jsonl
```

DataCoolie Studio recognizes year, month, day, and hour layouts. A date-based
lookback over an hourly stream includes all 24 hourly partitions for each
selected day. Normal incremental sync revisits the current and previous hour,
then uses the file manifest to skip unchanged files; use explicit lookback for
longer late-arrival windows. Keep a single layout within each log root; use a
new empty root when changing an existing daily stream to hourly partitions.

Dataflow rows include dedicated transformer metadata columns for select, drop,
rename, value rules, hash columns, masking rules, and the resolved
missing-column policy. Structured fields are JSON-serialized strings in
Parquet. Metadata values are retained without redaction, so analyst log access
must be governed as metadata access.

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
- Failed dataflow rows preserve any source, transformer, and destination
  runtime information available before the exception.
- An expected-failure scenario still writes a real `ERROR` and a failed ETL
  record. The usecase runner marks the scenario passed only after its child
  process matches `expected_exit_code` and required error text. Filter alerts
  by your scenario or job naming convention when running negative tests.
