---
title: Validate Metadata Before First Run — DataCoolie How-to
description: DataCoolie metadata checklist before the first pipeline run — providers, naming, sources, destinations, transforms, secrets, and common failure cases.
---

# Validation checklist

**Prerequisites** · You have authored a metadata JSON file.  
**End state** · Confidence that your metadata is correct before you press run.

Use this checklist before the first run on a new pipeline. You can also return
to it whenever a run fails with an unexpected error.

---

## 1. Document / provider preflight

- [ ] If you use the file provider, JSON is your canonical source and any YAML
      or Excel sibling has been regenerated after the latest edit.
- [ ] If you use the database or API provider, you know which `workspace_id`
      the run should target.
- [ ] Every connection has a **unique `name`** within the metadata set.
- [ ] Every dataflow has a **unique `name`** within the metadata set.
- [ ] Any nested JSON stored in Excel cells (`configure`, `secrets_ref`,
      `source_configure`, `destination_configure`, `transform`) is valid JSON.
- [ ] You are not expecting `connection_type: "streaming"` to work yet; that
      model value exists, but no built-in formats are mapped to it.

---

## 2. Connection basics

- [ ] `connection_type` and `format` are a valid pair:

    | `connection_type` | Valid `format` |
    |-------------------|----------------|
    | `file` | `csv` `parquet` `json` `jsonl` `avro` `excel` |
    | `lakehouse` | `delta` `iceberg` |
    | `database` | `sql` |
    | `api` | `api` |
    | `function` | `function` |

- [ ] If `connection_type` is omitted, `format` alone still identifies the
      intended connection family.
- [ ] `configure.base_path` exists on disk (or the cloud path is reachable)
      for `file` and `lakehouse` connections.
- [ ] Lakehouse connections using metastore registration have the right
      `catalog` / `database` values.
- [ ] Database connections have either `configure.url` or a valid combination
      of `database_type`, `host`, `port`, and `database`.
- [ ] API connections use `configure.base_url` rather than `configure.url`.
- [ ] `secrets_ref` only lists field names that actually exist in `configure`.
- [ ] No `configure` field appears under two different `secrets_ref` sources.

Quick database connectivity check:

    ```python
    from sqlalchemy import create_engine, text
    engine = create_engine("postgresql+psycopg2://user:pass@host:5432/db")
    with engine.connect() as conn:
        print(conn.execute(text("SELECT 1")).fetchone())
    ```

---

## 3. Dataflow envelope

- [ ] Every `source.connection_name` matches a `name` in `connections`.
- [ ] Every `destination.connection_name` matches a `name` in `connections`.
- [ ] `stage` is set — it is the filter you pass to `driver.run(stage=…)`.
      All dataflows in the same logical step should share the same `stage` string.
- [ ] If execution order matters, `group_number` and `execution_order` are set
      explicitly instead of relying on file order.
- [ ] `processing_mode` is left as `batch` unless you intentionally need a
      specialized mode.
- [ ] `is_active` was not accidentally set to `false` on the dataflow.

---

## 4. Source

- [ ] Each source uses the right selector style:
      - file / lakehouse / database table mode → `source.table`
      - database query mode → `source.query`
      - function source → `source.python_function`
- [ ] If the source is in a sub-folder/schema, `source.schema_name` is set.
- [ ] If you want incremental loads, `source.watermark_columns` is set and the
      column actually exists in the source data.
- [ ] For database sources: the SQL schema (`source.schema_name`) and table
      (`source.table`) exist in the target database.
- [ ] For database query sources: `source.query` runs successfully by itself.
- [ ] For API sources: `connection.configure.base_url` and
      `source.configure.endpoint` together form the correct URL.
- [ ] For API sources: pagination keys (`pagination_type`, `page_size`,
      `cursor_path`, `next_link_path`, `total_path`) match the actual response.
- [ ] For function sources: `source.python_function` is a dotted path like
      `mypkg.loaders.load_orders` and is allowed by runtime prefix rules if you
      use `allowed_function_prefixes`.
- [ ] Any `source.configure.read_options` override is intentional and engine-valid.
- [ ] If the file source uses `date_folder_partitions` or backward replay, you
      have verified the folder layout matches the pattern.

---

## 5. Destination

- [ ] The destination format is supported by a built-in writer:
      `parquet`, `csv`, `json`, `jsonl`, `avro`, `delta`, or `iceberg`.
- [ ] `destination.load_type` is set to one of:
      `append`, `overwrite`, `full_load`, `merge_upsert`, `merge_overwrite`, `scd2`.
- [ ] If the destination is a flat-file writer (`parquet`, `csv`, `json`,
      `jsonl`, `avro`), the load type is only `append`, `overwrite`, or `full_load`.
- [ ] If `load_type` is `merge_upsert`, `merge_overwrite`, or `scd2`:
      - [ ] `destination.merge_keys` is set and is a list.
      - [ ] Every column in `merge_keys` exists in the source data.
- [ ] If `load_type` is `scd2`:
      - [ ] `destination.configure.scd2_effective_column` is set.
      - [ ] The column named in `scd2_effective_column` exists in the source data.
- [ ] If `destination.partition_columns` are used:
      - [ ] Each `column` either already exists in the source data, or its
            `expression` references columns that do.
- [ ] If `connection.configure.date_folder_partitions` is used for a flat-file
      destination, you understand that `partition_columns` takes precedence when both are present.
- [ ] Any `destination.configure.write_options` override is intentional and engine-valid.
- [ ] If you use `catalog` / `database` registration, the resulting qualified
      name resolves to the intended lakehouse table.

---

## 6. Transform

- [ ] `schema_hints` entries use a supported `data_type`:
      `int`, `long`, `float`, `double`, `decimal`, `string`, `boolean`,
      `date`, `timestamp`, `binary`.
- [ ] `decimal` hints have both `precision` and `scale` set.
- [ ] `source.connection.use_schema_hint` is not disabled if you expect schema
      hints to take effect.
- [ ] `deduplicate_columns` and `latest_data_columns` reference columns that
      actually exist in the source data.
- [ ] If you rely on deduplication but left `latest_data_columns` empty, you
      intentionally want ordering to fall back to `source.watermark_columns`.
- [ ] SQL expressions in `additional_columns` are valid for your engine:
      - Polars: use `EXTRACT(YEAR FROM col)`, not `year(col)`.
      - Polars: use `CAST(col AS DATE)`, not `date(col)`.
      - Both: standard SQL arithmetic, `CASE WHEN`, string concatenation work.
- [ ] You have not configured `__created_at`, `__updated_at`, or `__updated_by`
      in `additional_columns` — these are added automatically.
- [ ] You are not trying to reference system columns inside `additional_columns`;
      they are added later in the pipeline.
- [ ] `transform.configure.convert_timestamp_ntz` and
      `transform.configure.deduplicate_by_rank` are only set when you want those behaviors.
- [ ] Downstream expectations account for final lowercase column names after
      `ColumnNameSanitizer` runs.

---

## 7. Secrets

- [ ] Credentials are **not hardcoded** in `configure.url` or `configure.password`.
- [ ] `secrets_ref` lists the correct field names from `configure`.
- [ ] The current value of each `configure` field listed in `secrets_ref` is a
      **vault key or environment variable name**, not the real credential.
- [ ] The vault/environment variables are available in the execution environment.
- [ ] The same `configure` field is not listed under two different secret sources.

Environment-variable example:

```json
{
  "configure": {
    "url": "DC_POSTGRES_URL"
  },
  "secrets_ref": {
    "env": ["url"]
  }
}
```

Quick check — environment-variable secrets:

```python
import os
# Every variable referenced indirectly by secrets_ref must be set:
print(os.environ.get("DC_POSTGRES_URL"))   # should not be None
```

See [Concepts · Secrets](../../concepts/secrets.md) for the full `secrets_ref`
schema.

---

## 8. Load and run a quick smoke test

Before a full production run, test with a small subset. The quickest way is to
use `dry_run=True` — the driver validates and plans without writing anything:

```python
from datacoolie.core.models import DataCoolieRunConfig
from datacoolie.orchestration.driver import DataCoolieDriver

with DataCoolieDriver(engine=engine, metadata_provider=metadata) as driver:
    result = driver.run(stage="ingest", run_config=DataCoolieRunConfig(dry_run=True))

print(result)
# ExecutionResult(total=1, succeeded=0, failed=0, skipped=1)
# Skipped = dry run; no write attempted.
```

Or validate the metadata load alone:

```python
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.platforms.local_platform import LocalPlatform

provider = FileProvider(config_path="metadata.json", platform=LocalPlatform())
flows = provider.get_dataflows(stage="ingest")
print(flows)     # list of DataFlow objects — inspect fields here
conns = provider.get_connections()
print(conns)     # list of Connection objects
```

If either call raises, the error message points directly at the invalid field.

For merge-style destinations, remember that the **first** successful run may
create the table with overwrite-style behavior before later runs switch to true
merge semantics.

---

## 9. Common errors quick-reference

| Error message | Root cause | Fix |
|---------------|------------|-----|
| `Format 'delta' is not valid for connection_type 'file'` | Type/format mismatch | Use the valid pairs table in section 2 |
| `APIReader requires 'base_url' in connection.configure` | API connection used the wrong key | Put the root URL in `connection.configure.base_url` |
| `PythonFunctionReader requires source.python_function` | Function path is missing or was put on the connection | Put a dotted path on `source.python_function` |
| `connection 'X' not found` | `connection_name` typo in source or destination | Check spelling against `connections[].name` |
| `Field 'url' listed in secrets_ref is missing from configure` | `secrets_ref` points at a non-existent config field | Add `configure.url` first, then resolve it via `secrets_ref` |
| `MergeUpsertStrategy requires merge_keys` | Merge load type requires business keys | Add `"merge_keys": [...]` to destination |
| `SCD2Strategy requires scd2_effective_column` | SCD2 without effective date | Add `"configure": {"scd2_effective_column": "..."}` to destination |
| `FileWriter only supports ['append', 'full_load', 'overwrite']` | Merge or SCD2 was configured on a flat-file destination | Use Delta/Iceberg for merge-style writes or switch the load type |
| `Column not found: updated_at` | Watermark or dedup column doesn't exist | Check actual column names in source data |
| `year()` / `date()` fails on Polars | Unsupported SQL helper was used in metadata expressions | Use `EXTRACT(...)` or `CAST(... AS DATE)` |
| `JSONDecodeError` in Excel cell | `configure` cell contains invalid JSON | Fix the JSON in that cell; ensure it's a valid object |
| All dataflows skipped / 0 loaded | `is_active` is false, the stage filter did not match, or the source legitimately returned zero rows | Check `is_active`, `driver.run(stage=...)`, and the source query/path |

---

## Ready to run

If all boxes above are checked, run your first stage:

```python
with DataCoolieDriver(engine=engine, metadata_provider=metadata) as driver:
    result = driver.run(stage="ingest")

assert result.failed == 0, f"Pipeline failed: {result}"
print(f"Processed {result.total} dataflows, {result.succeeded} succeeded")
```

→ Back to [Metadata guide overview](index.md)
