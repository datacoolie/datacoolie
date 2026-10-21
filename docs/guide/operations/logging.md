---
title: Logging Layout — DataCoolie User Guide
description: Production layout and persistence behavior for system and execution logs.
---

# Logging layout

For exact `LogConfig` fields and defaults, use the
[logging configuration reference](../../reference/runtime-configuration.md#logging-configuration).
For standalone logger construction, activation and close, use the
[logging API](../../reference/api/logging.md).

The Driver treats its `log_base_path` as the root for both framework streams:

```text
<log_base_path>/
├── execution_logs/
│   ├── job_run_log/__run_date=yyyy-mm-dd/job_<stem>.json
│   └── dataflow_run_log/__run_date=yyyy-mm-dd/dataflow_<stem>.json
└── system_logs/__run_date=yyyy-mm-dd/system_<stem>.json
```

All files are UTF-8 JSON Lines with a `.json` suffix. A complete line is one JSON object and every
file ends with a newline. Snapshot dataflow/system files are replaced on flush. Batch mode adds
`_part_00000001.json`, `_part_00000002.json`, and so on; each part is uploaded through
`BasePlatform.upload_file` and is never appended through a read-modify-write cycle.

Every terminal dataflow record has one optional `message` field. It carries the
failure detail for `failed` records and the human-readable reason for `skipped`
records; there are no separate `error_message` or `skip_reason` fields. Known
skip messages include inactive dataflow or connection flags, an empty source
read, a successful dry-run validation (no pipeline execution), and maintenance
with no operation to perform. Replay does not skip a requested chunk because
its stored watermark is high; a replay aggregate can be skipped only when all
admitted chunks return no data or the activation check excludes the dataflow.
If an extension returns `skipped` without a message, the record says
that no detailed reason was provided instead of guessing. The correlated system
log explains activation skips. An inactive dataflow excluded during normal
metadata selection creates no execution row.

The v6 dataflow record keeps phase details inside native `attempts` objects;
there are no duplicate root-level `source_message`, `transform_message`, or
`destination_message` projections. The job snapshot uses `message` for its
compact summary and `message_truncated` when that summary exceeds the configured
UTF-8 bound.

When execution-log persistence is configured, the job file is a single
replace-one snapshot. The activated ExecutionLogger requires a platform,
`LogConfig.output_path` and the session RunConfig to create its writers. The
Driver creates default loggers when a log root resolves from `log_base_path`,
`log_config.output_path` or `state_base_path`; an injected logger retains its
own output configuration. Without an execution logger or its persistence
prerequisites, there is no persisted job file.

The logger attempts a job snapshot at activation, when the summary changes,
and during final close. A session with no dataflow rows can therefore persist
a job summary, while empty system/dataflow streams create no empty files.
Uploads are best effort: storage failure, interruption or a bounded close
timeout can leave a missing or stale file. Check logger health and storage
alongside business results.

## Linking records

Every newly written system, job, and dataflow record includes
`datacoolie_version`, the installed package version that produced it. The value
is automatic and cannot be configured through `LogConfig`. It follows the first
two header fields, `log_schema_version` and `_type`, in both snapshot and batch
output. Historical v3 records may lack this field; readers should tolerate its
absence and ignore unknown fields. Current writers use schema version 7 with
explicit nested runtime projections and observed measurements. Historical v6
payloads remain readable evidence and are not rewritten.

Every current record carries `log_schema_version=7` followed immediately by `_type`:
`system_log`, `job_run_log`, or `dataflow_run_log`. Job/dataflow/system rows also carry the configured
`job_id`, `job_num`, and `job_index`, plus `log_session_id`. The raw job id is retained in the row;
only the filename token is sanitized. The same stem is used by the job and dataflow snapshot streams.

The Driver creates one `log_session_id` per Driver instance and hands it to
both loggers before activation. Use it to correlate system, job and dataflow
rows within that session, including when a caller reuses `job_id` in a later
session. `dataflow_id` identifies the configured flow; `dataflow_run_id`
identifies a particular execution or replay chunk. `run_attributes` supplies
caller correlation on the JobRuntime record. A dataflow record's
`read_request`, `attempts`, and `metrics` fields are native objects/arrays, not
JSON strings. Measured zero stays zero; unentered and unmeasured work stays null.
Job and replay totals sum observed values independently per metric. A missing
contribution does not erase a measured value; all-null populations remain null.
These totals describe observed work and do not claim complete measurement.

Diagnostic values share a 65,536-byte encoded UTF-8 allowance per dataflow or system
record, configured by `diagnostic_max_bytes`. Root, attempt and phase messages share
this budget with optional phase details; system messages share it with exception text.
Oversized strings retain a valid prefix; unsupported, cyclic, deep, or
oversized containers are omitted. The optional bounded `diagnostic_issues`
array records only a JSON-pointer field and one of `truncated`, `omitted`, or
`invalid`, with at most 16 markers. `max_record_bytes` defaults to 1 MiB and
limits the complete encoded JSON Lines record, including its newline; records
that exceed it are rejected as a whole.
Use the markers before assuming diagnostic text is complete. For marker overflow,
scalar handling and authoritative fields that are never truncated, see
[diagnostic limits and loss accounting](../../reference/concepts/logging.md#diagnostic-limits-and-loss-accounting).

For a batch stream, the sequence is stable across retries. Date partitions for batch parts use sealing
time; a retry never relocates a part. Snapshot paths are fixed at session activation and may remain in
an earlier date partition for a long-running process.

## Flush and failure behavior

The default snapshot timer is five minutes. Batch mode flushes when the encoded pending bytes reach
`flush_batch_bytes` (4 MiB by default) or when the timer fires, even if the batch is below the byte
threshold. A zero interval disables only the time trigger. `close()` drains admitted pending parts
within `close_timeout_seconds` (or records a bounded timeout when a sink remains in flight).

Each stream has one writer in flight. A failed upload retains the frozen payload and retries the same
bytes/path; a timed-out worker is considered ambiguous and no newer write is started for that stream.
Records admitted while a failed batch is waiting remain in a separate later batch. Other streams and
business execution continue. Buffer and local-spool limits include temporary upload materialisation;
new records are dropped and counted when capacity is exhausted rather than failing business execution.

`SystemLogger` keeps global capture ownership and console/file level separation. `ExecutionLogger` updates
aggregate counters when a terminal observation is received, even if the corresponding dataflow record
is dropped. With dataflow-detail persistence enabled, job `log_records_dropped` counts
terminal dataflow/maintenance records rejected during projection, encoding or writer admission,
once per observation. It excludes omitted diagnostics in an admitted record, disabled detail
persistence, repeated job-snapshot rejection and uploads awaiting retry. System capture/writer
drop statistics are separate. `log_bytes_dropped` counts only full encoded sizes that are known;
a rejection before encoding finishes can increase the record counter without increasing bytes.
Neither counter is a complete measure of remote publication failures.

The job's existing `message_truncated` flag applies to its compact message summary. A rejected
latest job snapshot leaves that summary dirty and does not prevent retry of a prior immutable
payload within the existing deadlines. An older snapshot's successful upload does not mean the
latest counters were published. Protected capacity cannot admit an individually oversized job
record; check logger health when the persisted summary is missing or stale.

## Driver setup

```python
driver = DataCoolieDriver(
    engine=engine,
    metadata_provider=metadata,
    log_base_path="s3://bucket/jobs/logs",
    config=DataCoolieRunConfig(
        job_id="orders",
        run_attributes={"factory_run_id": "adf-123"},
    ),
)
```

An injected logger keeps its own `LogConfig.output_path`; `log_base_path` is the default root for
auto-created loggers. The Driver passes the same lifecycle start timestamp and RunConfig job
identity to both auto-created/injected loggers before activation. It activates SystemLogger first,
emits `session.starting`, initializes the metadata provider, and emits `session.ready` only after
startup succeeds. On close it emits `session.finishing`, cleans Driver-owned components while
capture remains available, commits the JobRuntime terminal status, then closes ExecutionLogger and
SystemLogger. A startup failure emits `session.startup_failed` when capture was active and preserves
the original exception. If close is called while another exception is active, that primary exception
is preserved; otherwise the first provider/contract interruption is raised after all close attempts.
Importing DataCoolie and obtaining a module logger do not configure handlers. Driver startup
configures capture only when a SystemLogger is present; standalone callers activate their
SystemLogger explicitly.

## Diagnostic anchors

System records carry optional `event_name`, `dataflow_id`, and `dataflow_run_id` fields. Execution,
preparation, replay, scheduler, retry, and provider owners emit only boundary or failure anchors;
terminal dataflow rows remain the sole input to ExecutionLogger counters. A preparation or scheduler
failure is logged with one traceback at its owning boundary. Returned failed runtimes are summarized
without a second traceback in the Driver session summary. These records are observational and do
not change retries, status, watermarks, or flush timing; a logging-handler failure cannot replace a
business exception or prevent cleanup.

## Platform notes

The logging writer calls `upload_file` with `overwrite=True` for snapshots and batch retries. Local
publishing copies to a sibling temporary file and uses `os.replace`. Cloud SDKs may expose different
visibility/interruption guarantees; DataCoolie does not claim global atomic replacement or exactly-once
request delivery. Existing `append_file` remains available to other platform callers but is not used by
the new logger persistence path.
