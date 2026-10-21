---
title: File sources | DataCoolie
description: Configure file sources with metadata, examples and backend-specific guidance.
---

# File sources

[All source recipes](source-patterns.md).

## File source (CSV, Parquet, JSON, JSONL, Avro, Excel)

Use when your data is in flat files on local disk, cloud object storage (S3,
ADLS, GCS), or a Fabric/Databricks lakehouse path.

```json
{
  "name":            "raw_files",
  "connection_type": "file",
  "format":          "csv",
  "configure": {
    "base_path":             "data/input",
    "read_options":          { "separator": ";" }
  }
}
```

| Option | Required | Notes |
|--------|----------|-------|
| `base_path` | yes | Root folder. For S3: `s3://my-bucket/raw`. For ADLS: `abfss://container@account.dfs.core.windows.net/raw` |
| `read_options` | no | Reader defaults for this connection. `source.configure.read_options` can override them per dataflow |
| `use_hive_partitioning` | no | Enables partition-folder discovery like `country=VN/year=2026/` |
| `date_folder_partitions` | no | Date-folder pattern such as `{year}/{month}/{day}` for folder pruning |
| `backward_days` / `backward` | no | Re-read a historical window behind the last watermark |
| `use_schema_hint` | no | Defaults to `true`; disable to ignore `schema_hints` for this connection |
| `format` | yes | One of `csv`, `parquet`, `json`, `jsonl`, `avro`, `excel` |

**Dataflow source block:**

```json
"source": {
  "connection_name": "raw_files",
  "schema_name": "sales",
  "table": "orders",
  "watermark_columns": ["__file_modification_time"]
}
```

Reads the folder at `data/input/sales/orders`. For incremental ingestion of
new or modified files, `__file_modification_time` is a common watermark: the
reader lists files and selects those newer than the saved modification time.
It is not an automatic default; omit `watermark_columns` for a full read, or
use a reliable row column such as `updated_at` when the requirement is to
filter records rather than files. A modified file is read again as a whole,
so the destination load strategy must handle reprocessed rows appropriately.
The normal lower bound is strict (`>`): a file arriving later with the same or
an older modification time can be missed. If this is possible, use a deliberate
[look-back window](source-incremental.md#incremental-windows-and-look-back) and an idempotent
destination load.

When a source does not use `__file_modification_time` as a watermark, a file
whose platform listing has no modification time is still read; its
`__file_modification_time` lineage value is null. When the field is configured
as a watermark, every discovered file must provide a modification time. A
missing value fails the source before file reading or watermark advancement,
including on the first run. Fix the platform metadata or source listing and
retry from the unchanged watermark.

### Per-dataflow file overrides

If one dataset needs special reader settings, override the connection-level
defaults in `source.configure`:

```json
"source": {
  "connection_name": "raw_files",
  "schema_name": "sales",
  "table": "orders",
  "configure": {
    "read_options": {
      "separator": "|",
      "encoding": "utf8-lossy"
    }
  }
}
```

### File-source incremental cases

File readers support more than simple row-level watermarks:

| Pattern | How it works |
|---------|--------------|
| Row-column watermark | Use a trustworthy column in the file data; the reader filters rows after reading |
| File modification watermark | Use `__file_modification_time` in `watermark_columns` to select new/modified files before reading; requires file listing with modification times |
| Date-folder pruning | `date_folder_partitions` lets the reader prune old folders before reading |
| Historical replay window | `backward_days` / `backward` re-opens a look-back window |
| Independent bounded read | Replay may use `chunk_column="__file_modification_time"` even when mtime is not a persisted watermark; the reader still selects files by actual mtime |

For a partitioned dataset under folders such as
`data/input/sales/orders/year=2026/month=09/day=23/`, configure both options
on its connection:

```json
{
  "name": "partitioned_files",
  "connection_type": "file",
  "format": "parquet",
  "configure": {
    "base_path": "data/input",
    "use_hive_partitioning": true,
    "date_folder_partitions": "year={year}/month={month}/day={day}"
  }
}
```

Then use a source with `connection_name: "partitioned_files"`,
`schema_name: "sales"`, and `table: "orders"`. If you also use
`__file_modification_time`, folder pruning happens first: a file modified in
an older, pruned date folder will not be discovered unless the folder window
is reopened.

File reads also inject file lineage columns such as `__file_name`,
`__file_path`, and `__file_modification_time`.

!!! note "Internal folder watermark"
    When you use `date_folder_partitions`, DataCoolie stores the folder-level
    watermark internally as `__date_folder_partition__`. You normally do not
    need to author that field yourself; the reader maintains it. It is used
    only for conservative folder discovery. Folder boundaries are inclusive;
    the file's modification time decides whether the file is read. The
    internal folder key cannot be a replay `chunk_column`. The reader also
    reloads this internal frontier on later ordinary runs when the source has
    no authored `watermark_columns`, so folders older than the saved frontier
    are not scanned again.

!!! note "Excel is read-only"
    Excel (`format: "excel"`) can only be a **source**. You cannot write Excel
    as a destination.
