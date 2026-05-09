---
title: Use Your Own Data After the Quickstart — DataCoolie
description: Replace the sample quickstart input with your own local data while keeping the same runner, engine, and basic metadata shape.
---

# Use your own data after the quickstart

This page is for the most common next step: the sample pipeline worked, and now
you want to point DataCoolie at your own data without changing engines,
platforms, or orchestration code.

**Start from**

- Completed [Quickstart · Polars](quickstart-polars.md) or [Quickstart · Spark](quickstart-spark.md).
- A local file or folder you want to ingest first.

**Keep these the same**

- Your chosen engine (`PolarsEngine` or `SparkEngine`)
- Your platform (`LocalPlatform` in the quickstarts)
- Your `run.py` structure

## Change only three parts first

1. The input connection
2. The source selector
3. The destination behavior

That is enough for most first real runs.

## 1. Put your input in a predictable folder

Start simple. For file sources, DataCoolie builds the read path from
`base_path`, optional `schema_name`, and `table`.

```text
my-pipeline/
  data/
    input/
      customers/
        customers.csv
    output/
  metadata/
    customers.json
  run.py
```

If your input is not CSV, keep the same folder idea and change the connection
`format` to match the file type you are reading.

## 2. Update the input connection

Use the same shape as the quickstart and change only what is yours:

```json
{
  "name": "local_input",
  "connection_type": "file",
  "format": "csv",
  "configure": {
    "base_path": "data/input"
  }
}
```

If you later need reader-specific options such as headers or delimiter changes,
add them under `configure.read_options`.

## 3. Point the source at your folder or table

The smallest useful source block is:

```json
{
  "connection_name": "local_input",
  "table": "customers"
}
```

That reads from `data/input/customers` when the connection `base_path` is
`data/input`.

If you have a reliable incremental column, add it now:

```json
{
  "connection_name": "local_input",
  "table": "customers",
  "watermark_columns": ["updated_at"]
}
```

If you do not have a reliable timestamp or sequence column yet, skip watermarks
for the first run and add them later.

## 4. Pick the simplest destination first

When moving from the sample to your own data, prefer the easiest load behavior
that matches what you know:

- Use `overwrite` when you just need a clean successful run.
- Use `merge_upsert` only when you already have stable business keys.
- For flat-file destinations such as CSV, Parquet, JSON, or JSONL, stay with
  `append`, `overwrite`, or `full_load`.

Example destination for a first real run:

```json
{
  "connection_name": "local_bronze",
  "schema_name": "sales",
  "table": "customers",
  "load_type": "overwrite"
}
```

When you are ready for keyed incremental merges:

```json
{
  "connection_name": "local_bronze",
  "schema_name": "sales",
  "table": "customers",
  "load_type": "merge_upsert",
  "merge_keys": ["customer_id"]
}
```

## 5. Keep the runner unchanged

Do not redesign `run.py` yet. Reuse the quickstart runner and only point
`FileProvider` at the new metadata file.

```python
platform = LocalPlatform()
engine = PolarsEngine(platform=platform)
provider = FileProvider(config_path="metadata/customers.json", platform=platform)

with DataCoolieDriver(engine=engine, metadata_provider=provider) as driver:
    result = driver.run(stage="ingest2bronze")
```

If your quickstart already ran, keeping the runner fixed makes the next failure
much easier to diagnose because only the metadata changed.

## 6. Add transforms only when you need them

Common first upgrades:

- Add `deduplicate_columns` and `latest_data_columns` when the source can send duplicate business keys.
- Add `schema_hints` when decimals, timestamps, or IDs need stable typing.
- Add `additional_columns` only for business columns you derive before the write.

Use the detailed guides when you hit those needs:

- [Metadata guide · Build your first metadata file](../how-to/metadata-guide/first-metadata-file.md)
- [Metadata guide · Source patterns](../how-to/metadata-guide/source-patterns.md)
- [Metadata guide · Destination & load patterns](../how-to/metadata-guide/destination-and-load-patterns.md)
- [Metadata guide · Transform patterns](../how-to/metadata-guide/transform-patterns.md)

## Common mistakes on the first real run

- Using `merge_upsert` before you have stable `merge_keys`
- Expecting flat-file outputs to support merge-style loads
- Adding watermarks before you have a reliable incremental column
- Changing the engine, platform, and metadata all at once

## Next

- [Your first dataflow](first-dataflow.md) — add a second stage and explicit ordering.
- [Metadata guide for new users](../how-to/metadata-guide/index.md) — field-by-field coverage of the full metadata model.
- [Quickstart · Spark](quickstart-spark.md) — keep the same metadata shape and validate it on Spark.