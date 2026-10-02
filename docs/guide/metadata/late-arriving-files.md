---
title: Late-arriving and updated files — DataCoolie User Guide
description: Combine date-folder discovery and file modification watermarks without promising missed older files are reread.
---

# Late-arriving and updated files

Use this pattern for files stored under dated folders when a delivery may
arrive later than the folder date. Configure the ordinary
[file source](source-patterns.md#file-source-csv-parquet-json-jsonl-avro-excel)
and [look-back](source-patterns.md#incremental-windows-and-look-back) first. The
[Connection](../../reference/metadata-schema.md#connection) and
[Source](../../reference/metadata-schema.md#source) reference sections give
exact field shapes.

## Reopen folders and admit recent files

These are separate metadata fragments. Assume the file connection's
`base_path` addresses `landing/` and the source path selects `orders`.
Adapt the folder pattern and format to the files the platform lists:

```json
{
  "name": "orders_files",
  "connection_type": "file",
  "format": "parquet",
  "configure": {
    "base_path": "landing",
    "date_folder_partitions": "{year}/{month}/{day}",
    "use_hive_partitioning": false
  }
}
```

```json
{
  "source": {
    "connection_name": "orders_files",
    "table": "orders",
    "watermark_columns": ["__file_modification_time"],
    "configure": {"backward_days": 3}
  }
}
```

For Hive-style partitioned paths use the matching folder pattern and set
`use_hive_partitioning` when its reader needs recursive discovery. In a
dated-folder read, folder pruning happens before modification-time filtering.
If yesterday a new file was delivered into a two-day-old folder and its mtime
is newer than the saved mtime, the three-day folder look-back can reopen the
folder and the mtime filter can admit the new file. Choose the look-back for
the actual delivery delay, including timezone and folder naming conventions.

## A reopened folder can still contain an excluded file

With a date-folder watermark present, `backward_days` moves **only the folder
watermark**. It does not move `__file_modification_time` backwards. A new file
whose mtime equals or predates the saved mtime is still excluded, even when
its folder is reopened. There is no separate authored mtime look-back option
for this reader path. For a known historical interval use the bounded
[replay procedure](../operations/replay-and-backfill.md), checking both bounds
and the source listing; do not assume routine metadata look-back recovers it.

The reader ingests selected files as whole files, not as isolated changed
rows. For immutable, newly named files, an append destination may be suitable.
For corrected files that are read again, choose a write strategy that handles
their business keys or complete replacement scope. A key-based upsert updates
rows it receives, but does not remove rows deleted inside a corrected file.
Window replacement requires a *complete* bounded source window and a usable
row watermark; see [that recipe](watermark-window-replacement.md) before
combining the patterns.
