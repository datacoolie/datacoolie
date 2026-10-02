---
title: Troubleshooting — DataCoolie User Guide
description: Diagnose common DataCoolie failures across metadata loading, engines, platforms, plugins, and destination writes.
---

# Troubleshooting

Start with the terminal dataflow `status` and `message`, then correlate its
`dataflow_run_id` and session `log_session_id` with system diagnostics. Check
the selected metadata, resolved paths and engine before changing state or
rerunning writes. See [logging](logging.md) for persistence prerequisites.

## Run reports no failures but expected data is missing

**Check:** inspect `total`, `succeeded`, `skipped` and `pending`, compare the
selected dataflow IDs with the required stage inventory, and read skip reasons.
Check activation on both connections and the flow, stage/connection filters,
shard assignment and source eligibility.

**Action:** correct selection/configuration, rerun the intended scope and
validate output freshness and completeness. A legitimately empty shard differs
from missing required work across the job; wait for all upstream shards before
releasing dependent stages. See [run checks](run-stage.md#single-stage).

## Logs or the job summary are missing

**Check:** confirm that an ExecutionLogger exists and is activated, has a
platform and output path, and received the RunConfig. Check credentials,
upload warnings, dropped-record counters and whether Driver close completed.

**Action:** configure a writable log/state root or inject configured loggers,
use the Driver context manager, and verify files after close. Business success
does not guarantee every log upload succeeded. See
[flush and failure behavior](logging.md#flush-and-failure-behavior).

## "`pl.sql_expr` unknown function …"

**Polars** uses a SQL subset. Common offenders:

| Doesn't work in Polars | Use |
|---|---|
| `current_timestamp()` | Literal cast, or framework-added `__updated_at`. |
| `date_format(col, 'yyyy-MM-dd')` | `CAST(col AS DATE)`. |
| `year(col)` | `EXTRACT(YEAR FROM col)`. |

See [Partition expression portability](../metadata/destination-and-load-patterns.md#partition-expression-portability).

## Row count mismatch for multi-line JSON

**Check:** distinguish a JSON array/document from JSON Lines. JSONL requires
one complete JSON value per physical line; escaped `\n` inside a string is
valid, but a pretty-printed record spanning lines is not JSONL. Literal
unescaped newlines inside JSON strings are invalid JSON.

**Action:** use `format: "json"` for a JSON document or array, and
`format: "jsonl"` for one-record-per-line input. Fix malformed input and
compare IDs/row counts after parsing rather than assuming file-line count is
record count.

## Excel `is_active` column loads everything as inactive

**Check:** inspect the flow and both connection activation values in the
loaded metadata. A blank value uses the default active behavior; explicit
`FALSE` means inactive and may be deliberate.

**Action:** set `TRUE` only for flows and connections that should run. Keep
intentional inactive flags. See [activation and selection](../metadata/dataflows.md#activation-and-selection).

## "dead lock detected" on Delta optimize

**Check:** inspect overlapping job invocations and resolved destination paths
or catalog identities. Deduplication covers one maintenance invocation; it
does not coordinate external jobs. Also check concurrent ingestion/writes and
the backend conflict message.

**Action:** serialize conflicting operations for the same physical target and
retry according to the backend's conflict policy. See
[deduplication scope](maintenance.md#deduplication).

## Spark or a source service is unavailable

**Check:** use the failing operation's actual runtime coordinates: Java/Spark
versions, connector packages, endpoint, catalog and credentials. Resolve
network/DNS/authentication failures before investigating row selection.

**Action:** configure the host using the [platform guides](../platforms/index.md)
and choose a supported dependency profile from [installation](../getting-started/installation.md).
Repository simulation setup belongs to [testing](../../project/testing.md);
production runners should use their deployed services and project configuration.

## Iceberg writes do not appear in the expected catalog

**Check:** confirm `format: "iceberg"`, the catalog implementation and endpoint,
warehouse, namespace, table identifier and storage credentials used by both
writer and reader. An AWSPlatform with an S3-compatible endpoint does not by
itself imply a Glue catalog. Check engine/catalog initialization and permissions
for the configured REST, Glue or other supported catalog.

**Action:** align the writer/reader catalog and target, then query that target
and inspect its snapshots. Delta symlink options `generate_manifest` and
`register_symlink_table` do not register Iceberg tables.

## `WatermarkManager` throws on first run

**Check:** distinguish missing stored state from a provider/storage error or
invalid source configuration. A missing state is normally an initial read;
it does not guarantee every source can issue an unbounded request. For
example, an API using incremental range splitting may require
`watermark_range_start` when no lower state exists.

**Action:** configure the source's documented first-read behavior and a valid
provider-owned state root. Custom readers must handle absent state according
to their contract or reject an unsupported initial read clearly. Use an
explicit bounded replay for a history load when supported; do not fabricate
watermark state to hide a storage failure. See
[API source configuration](../metadata/source-patterns.md#push-down-and-split-watermark-ranges).

## Maintenance succeeded but compaction or cleanup had no effect

**Check:** read warning logs and `destination_operation_details`, then inspect
the selected table's history, snapshots and files. A requested backend action
can be unavailable, skipped or have no eligible work. Polars Iceberg warns
about unsupported compaction/orphan removal and can warn on snapshot-expiry
failure without failing the aggregate.

**Action:** use the [capability matrix](maintenance.md#engine-and-format-capabilities)
to select an engine/catalog that implements the required action. Review the
retention policy and target identity before any cleanup retry.

## Replay wrote rows but the watermark did not advance

Replay writes the destination before saving the reader-produced watermark
candidate. A process interruption or provider failure in that gap can leave
committed rows with the previous watermark. The next invocation still reads
every requested `[start, end)` chunk; the saved value is never a replay
checkpoint. Rerun the complete range with a keyed or otherwise idempotent
destination strategy when repeated delivery would create duplicates. An empty
or all-null observation does not advance state, while a real zero is a valid
watermark value.
