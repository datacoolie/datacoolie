---
title: Validate Metadata Before First Run — DataCoolie User Guide
description: DataCoolie metadata checklist before the first pipeline run — providers, naming, sources, destinations, transforms, secrets, and common failure cases.
---

# Validation checklist

**Prerequisites** · You have authored a metadata JSON file.  
**End state** · Confidence that your metadata is correct before you press run.

Use this checklist before the first run on a new pipeline. You can also return
to it whenever a run fails with an unexpected error.
The links beside relevant checks open the field shapes, allowed values, and
defaults in the Metadata reference.

---

## 1. Document / provider preflight

- [ ] If you use the file provider, JSON is your canonical source and any YAML
      or Excel sibling has been regenerated after the latest edit.
- [ ] If you use the database or API provider, you know which
      `connection.workspace_id`
      or `dataflow.workspace_id` the run should target.
- [ ] Every connection has a **nonblank `name`**. Keep names unique within the
      active document/provider scope for name-based references. Distinct
      explicit `connection_id` values may share a display name, but a name
      reference then fails as ambiguous and must be replaced with the ID.
      If a workspace is supplied, this scope is that `workspace_id`.
- [ ] Every name-based dataflow reference resolves to one dataflow name in its
      scope. A dataflow may use an explicit `dataflow_id` without a name.
- [ ] If explicit `connection_id`
      or `dataflow_id` values are used, they are
      stable, unique, and intentionally owned by an external identity contract.
      Duplicate connection display names require distinct explicit IDs because
      omitted IDs derive from the name.
- [ ] If `$schema` is present, it follows the [Metadata document](../../reference/metadata-schema.md#metadata-document)
      contract: use the current `latest` alias for ordinary
      authoring or the exact compatible version for reproducible artifacts;
      `dc validate` still reports the local framework-resolved version.
- [ ] Root `extensions` values are project-owned annotations and are not being
      relied on as framework runtime settings.
- [ ] Any nested JSON stored in Excel cells (`configure`, `secrets_ref`,
      `source_configure`, `destination_configure`, `transform`) is valid JSON.
- [ ] Excel rows include `name` and `connection_type`; each source row includes
      a connection plus `source_table`, `source_query`, or
      `source_python_function`. JSON/YAML may derive `connection_type` from an
      unambiguous `format`, but Excel does not.
- [ ] If metadata is exported to Excel, `source_filter_expression` is present
      as its own source column and survives an Excel → FileProvider round-trip.
- [ ] You are not expecting `connection_type`: `"streaming"` to work yet; that
      model value exists, but no built-in formats are mapped to it.

---

## 2. Connection basics

- [ ] `connection_type` and `format` are a valid pair in [Connection](../../reference/metadata-schema.md#connection):

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
      for `file` and `lakehouse` connections. See [`connections[].configure`](../../reference/metadata-schema.md#connectionsconfigure)
      for shared endpoint settings.
- [ ] Lakehouse connections using metastore registration have the right
      `catalog` /
      `database` values.
- [ ] Database connections have either `configure.url` or a valid combination
      of `database_type`,
      `host`,
      `port`, and
      `database`; see [Connection settings by endpoint type](../../reference/metadata-schema.md#connection-settings-by-endpoint-type).
- [ ] API connections use `configure.base_url` rather than `configure.url`.
- [ ] `secrets_ref` only lists field names that actually exist in `configure`.
- [ ] No `configure` field appears under two different `secrets_ref` sources.
- [ ] For API auth, the required fields for the selected
      `auth_type` are
      present and the runtime has its optional dependency (for example
      `botocore` for `aws_sigv4`). See [API authentication](connections.md#api-authentication).
- [ ] Database transport-specific options such as MSSQL TLS flags match the
      selected engine/driver; open configure maps do not guarantee portability.

Quick database connectivity check:

    ```python
    from sqlalchemy import create_engine, text
    engine = create_engine("postgresql+psycopg2://user:pass@host:5432/db")
    with engine.connect() as conn:
        print(conn.execute(text("SELECT 1")).fetchone())
    ```

---

## 3. Dataflow envelope

- [ ] In the [Dataflow](../../reference/metadata-schema.md#dataflow) envelope,
      every `source.connection_name` matches a `connection.name`.
- [ ] Every `destination.connection_name` matches a `connection.name`.
- [ ] `stage` is set — it is the filter you pass to `driver.run(stage=…)`.
      All dataflows in the same logical step should share the same `stage` string.
- [ ] If execution order matters, `group_number` and `execution_order` are set
      explicitly instead of relying on file order.
- [ ] `processing_mode` is `batch` for the built-in driver. The model accepts
      `microbatch` and `streaming` values for specialized or future runtimes,
      but the normal built-in ETL path does not implement those modes.
- [ ] `dataflow.is_active` was not accidentally set to `false` on the dataflow.
- [ ] The source and destination connections' `is_active` fields are `true` for every dataflow
      expected to run. Inactive connections remain in metadata but cause a
      selected dataflow to be skipped.

---

## 4. Source

- [ ] Each [Source](../../reference/metadata-schema.md#source) uses the right selector style:
      - file / lakehouse / database table mode → `source.table`
      - database query mode → `source.query`
      - function source → `source.python_function`
- [ ] If a query source also has `source.table`, treat it as a logical alias,
      not a limit on the SQL read. If a function source has `source.table`,
      check how that function uses it and other Source fields. If either uses
      [Shared schema hint](../../reference/metadata-schema.md#shared-schema-hint)
      entries, ensure the matching hints describe the output.
- [ ] If the source is in a sub-folder/schema, `source.schema_name` is set.
- [ ] If you want incremental loads, `source.watermark_columns` is set and the
      column actually exists in the source data.
- [ ] For database table sources: the SQL schema (`source.schema_name`) and
      table (`source.table`) exist in the target database.
- [ ] For inline database queries: `source.query` runs successfully by itself.
- [ ] For SQL-file queries: the `.sql` path is resolved with the runner's
      `sql_base_path` or `artifact_base_path`; check the single-root or
      multiple-root prefix rules in [Source patterns](source-patterns.md#read-a-sql-file).
- [ ] For SQL-file queries: the resolved SQL text runs successfully and the
      selected query returns every column named by `watermark_columns` or later
      transform/destination rules.
- [ ] For API sources: `connection.configure.base_url` and
      `source.configure.endpoint` together form the correct URL.
- [ ] For API sources: pagination keys (`pagination_type`, `page_size`,
      `cursor_path`, `next_link_path`, `total_path`) match the actual response.
      See [`source.configure`](../../reference/metadata-schema.md#dataflowssourceconfigure)
      for the request options.
- [ ] For API sources: `data_path` resolves to the response records list (or
      one object); a wrong path otherwise looks like an empty result.
- [ ] For API cursor/offset pagination, custom parameter names (`cursor_param`,
      `offset_param`, `limit_param`) match the provider contract. Built-in
      offset mode sends record offsets, not 1-based page numbers.
- [ ] For API offset pagination with `total_path`, its value is numeric,
      `offset_max_workers` respects the provider's rate limit, and `max_pages`
      cannot silently truncate the intended result. `rate_limit_delay` does
      not throttle those parallel requests.
- [ ] For the **legacy incremental API split**,
      `watermark_range_interval_unit` has `watermark_to_param` and
      `watermark_param_mapping`, and the first run has
      `watermark_range_start`; the API accepts both lower and upper bounds.
      These fields are not required for canonical bounded replay.
- [ ] For API bounded reads or replay, prefer `range_param_mapping` with an
      explicit lower and upper binding for the selected field. Check each
      binding's location, operator, wire format, and `response_column`; use
      `format: integer` for numeric bounds so values are not stringified.
- [ ] For API `range_param_mapping`, choose one `watermark_value` meaning per
      active request: `observed_max` requires a returned response field, while
      `request_end` requires an exact covered end and complete pagination.
      Mixed active meanings are rejected before HTTP.
- [ ] For canonical bounded reads or replay, the selected field has a
      `range_param_mapping` entry with explicit lower and upper operators. The
      endpoint wire format preserves the authored precision: `date` values are
      calendar dates, `datetime` values are whole seconds, and millisecond
      formats require millisecond-aligned values. Unrepresentable fractional
      precision fails before the request.
- [ ] For API `next_link` pagination, treat the continuation URL as opaque by
      default. Configure `next_link_bound_mode: repeat_query_bounds` only when
      the endpoint contract requires query bounds on every page; matching,
      missing, duplicate, and conflicting bounds have distinct outcomes.
- [ ] A bounded API replay `chunk_column` has a matching
      `range_param_mapping` entry with both lower and upper bindings. The
      selected field may be outside `source.watermark_columns`; legacy
      `watermark_param_mapping` plus `watermark_to_param` is not sufficient
      for an exact `[start, end)` read.
- [ ] For API next-link pagination, returned links remain on the configured
      HTTP(S) origin. The reader rejects a foreign host/port, scheme downgrade,
      userinfo, or non-HTTP(S) continuation before sending the next request.
- [ ] For a legacy endpoint whose upper bound is inclusive,
      `watermark_range_to_exclusive_offset` is intentionally set and its
      precision matches the API parameter. Do not use it to emulate an
      exclusive canonical range; use `range_param_mapping` operators.
- [ ] If `watermark_to_param_timezone` is set, the source-level value is
      intentional; it overrides the connection-level value.
- [ ] For function sources: `source.python_function` is a dotted path like
      `mypkg.loaders.load_orders` and is allowed by runtime prefix rules if you
      use `allowed_function_prefixes`.
- [ ] Any `source.configure.read_options` override is intentional and engine-valid.
- [ ] If `source.filter_expression` is set, the SQL predicate references
      columns in the reader output (including aliases returned by `source.query`),
      not columns added later by transforms.
- [ ] For API sources, `source.filter_expression` is understood as a local
      DataFrame filter after response materialization. Put endpoint push-down
      parameters in `source.configure.params` or `source.configure.body`.
- [ ] If the file source uses `date_folder_partitions` or backward replay, you
      have verified the folder layout matches the pattern.
- [ ] If a look-back is configured, it uses a supported shorthand or nested
      `backward` key (`hours`, `days`, `months`, `years`, `closing_day`), and a source
      override contains the complete intended value rather than relying on a
      partial merge with the connection.

---

## 5. Destination

- [ ] The destination `format` is supported by a built-in writer:
      `parquet`, `csv`, `json`, `jsonl`, `avro`, `delta`, or `iceberg`.
- [ ] The [Destination](../../reference/metadata-schema.md#destination) block's
      `load_type` is set to one of:
      `append`, `overwrite`, `full_load`, `merge_upsert`, `merge_overwrite`, `scd2`.
- [ ] If the destination is a flat-file writer (`parquet`, `csv`, `json`,
      `jsonl`, `avro`), the load type is only `append`, `overwrite`, or `full_load`.
- [ ] If `load_type` is `merge_upsert` or `scd2`:
      - [ ] `destination.merge_keys` is set and is a list.
      - [ ] Every column in `merge_keys` exists in the source data.
- [ ] If `load_type` is `merge_overwrite` and
      `destination.configure.replace_by_watermark` is not using a usable
      replacement window, `destination.merge_keys` is set and every key column
      exists in the source data.
- [ ] If `load_type` is `scd2`:
      - [ ] `destination.configure.scd2_effective_column` is set.
      - [ ] The column named in `scd2_effective_column` exists in the source data.
- [ ] If `destination.configure.replace_by_watermark` is `true` (see
      [`destination.configure`](../../reference/metadata-schema.md#dataflowsdestinationconfigure)):
      - [ ] `destination.load_type` is `merge_overwrite`.
      - [ ] `source.watermark_columns` identifies the window column and either
            `source.configure` or the referenced connection `configure` contains
            a look-back option such as `backward_days` or `backward`.
      - [ ] You understand that `date_backward` is computed at runtime and is
            not an authored metadata field.
      - [ ] The source covers the complete replacement window, including rows
            that must be removed from the destination. See [Cross-boundary combinations](../../reference/metadata-schema.md#cross-boundary-combinations)
            and [Replace a watermark window](watermark-window-replacement.md).
- [ ] If `destination.partition_columns` are used:
      - [ ] Each item follows the [Partition column](../../reference/metadata-schema.md#partition-column)
            shape. Its `column` either already exists in the source data, or its
            `expression` references columns that do.
- [ ] If `connection.configure.date_folder_partitions` is used for a flat-file
      destination, you understand that `partition_columns` takes precedence when both are present.
- [ ] Any `destination.configure.write_options` override is intentional and engine-valid.
- [ ] If you use `catalog` / `database` registration, the resulting qualified
      name resolves to the intended lakehouse table.
- [ ] If an existing database/API metadata provider is upgraded, the additive
      `source_filter_expression` schema migration has run and passed its
      postflight check before the new runtime is started.

---

## 6. Transform

The [Transform](../../reference/metadata-schema.md#transform) section defines the top-level fields checked here.

- [ ] `transform.schema_hints` entries follow the [Schema hint](../../reference/metadata-schema.md#schema-hint)
      shape and use a supported `data_type` for the selected
      source type system. See [Datatypes and schema hints](data-types.md).
- [ ] Decimal hints use explicit `decimal(precision,scale)` or matching
      `precision` and
      `scale` fields; bare `decimal`/`number` is rejected.
- [ ] If a weak source reuses vendor hints, its source connection has
      `configure.schema_hint_type_system` set to the authored source dialect.
- [ ] `source.connection.use_schema_hint` is not disabled if you expect schema
      hints to take effect.
- [ ] `deduplicate_columns` and `latest_data_columns` reference columns that
      actually exist in the source data.
- [ ] Every `hash_columns` entry follows the [Hash column](../../reference/metadata-schema.md#hash-column)
      shape and declares a non-empty, ordered `columns` list;
      hash inputs do not fall back automatically to deduplication or merge keys.
- [ ] For a surrogate-key-style hash, the explicit hash columns match the
      intended business/natural key; do not assume deduplication and merge keys
      always mean the same thing.
- [ ] The selected `algorithm` matches the use case: `xxhash64` only when signed
      64-bit collision risk is acceptable, `sha256` when a larger digest is
      preferred, and neither as plain low-entropy PII protection.
- [ ] If a hash target or its input columns change, downstream type/value
      migration has been planned; changing SHA-256 to XXHash64 changes String
      output to signed BIGINT.
- [ ] If you rely on deduplication but left `latest_data_columns` empty, you
      intentionally want ordering to fall back to `source.watermark_columns`.
- [ ] The SQL `expression` values in [Additional column](../../reference/metadata-schema.md#additional-column)
      items are valid for your engine:
      - Polars: use `EXTRACT(YEAR FROM col)`, not `year(col)`.
      - Polars: use `CAST(col AS DATE)`, not `date(col)`.
      - Both: standard SQL arithmetic, `CASE WHEN`, string concatenation work.
- [ ] If `transform.filter_expression` is set:
      - [ ] The SQL predicate is valid for your engine.
      - [ ] It only references source columns or columns created by
            `additional_columns` (not system columns added later at order 70).
- [ ] You have not configured `__created_at`, `__updated_at`, `__updated_by`, or
      `__dataflow_run_id` in `additional_columns` — these are added automatically.
- [ ] You are not trying to reference system columns inside `additional_columns`;
      they are added later in the pipeline.
- [ ] In [`transform.configure`](../../reference/metadata-schema.md#dataflowstransformconfigure),
      `convert_timestamp_ntz` defaults to `false`; when it
      is `true`, `transform.configure.timestamp_timezone` is set deliberately.
- [ ] `transform.configure.deduplicate_by_rank` is only set when you want that behavior.
- [ ] Downstream expectations account for final lowercase column names after
      `ColumnNameSanitizer` runs.

---

## 7. Secrets

- [ ] Credentials are **not hardcoded** in `configure.url` or `configure.password`.
- [ ] The [Connection](../../reference/metadata-schema.md#connection) `secrets_ref`
      lists the correct field names from `configure`.
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
    "env:": ["url"]
  }
}
```

Quick check — environment-variable secrets:

```python
import os
# Every variable referenced indirectly by secrets_ref must be set:
print(os.environ.get("DC_POSTGRES_URL"))   # should not be None
```

See [Concepts · Secrets · `secrets_ref` schema](../../reference/concepts/secrets.md#secrets_ref-schema) for the full `secrets_ref`
schema.

---

## 8. Load and run a quick smoke test

Before a full production run, test with a small subset. The quickest way is to
use `dry_run=True` to confirm metadata loading, selection, SQL-file references,
and replay-window structure without touching business data. It deliberately
does not resolve secrets, construct readers/writers, or execute transforms:

```python
from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.orchestration.driver import DataCoolieDriver

with DataCoolieDriver(
    engine=engine,
    metadata_provider=metadata,
    config=DataCoolieRunConfig(dry_run=True),
) as driver:
    result = driver.run(stage="ingest")

print(result)
# Valid targets are skipped (validated only); malformed paths/ranges are failed.
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
| `MergeOverwriteStrategy requires merge_keys when no usable replacement window is available` | Key-based merge-overwrite was selected without keys or a usable watermark window | Add merge keys, or configure a valid watermark window with `replace_by_watermark` |
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
