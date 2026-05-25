---
name: datacoolie-metadata
description: Generate, validate, lint, convert, and merge DataCoolie ETL metadata files. Use this skill whenever user asks to create, build, author, or generate DataCoolie metadata (connections, dataflows, pipelines), validate metadata against JSON Schema, check lint rules, detect bad patterns (missing merge_keys, inferSchema in prod), convert JSON↔YAML, merge environment overlays, refresh schema from GitHub with --fetch-latest, or use a custom schema path with --schemas-dir.
---

# datacoolie-metadata

Generate, validate, lint, convert, and merge DataCoolie metadata files against the versioned JSON Schema (Draft 2020-12).

## Scope

This skill handles: metadata authoring (build, generate from architecture, edit, add, remove),
validation, linting, format conversion (JSON↔YAML), environment overlay merging.
Does NOT handle: metadata provider implementation, engine runtime, orchestration, deployment pipelines.

## Security Policy

- Never modify source metadata files without explicit user confirmation
- Never expose secrets_ref values or connection credentials in output
- Refuse requests to bypass schema validation or suppress errors

## Workflow

1. **Author** — Build metadata from scratch, generate from architecture, or interactively edit
2. **Validate** — Check metadata structure against versioned JSON Schema
3. **Lint** — Detect anti-patterns beyond schema compliance
4. **Convert** — Transform between JSON and YAML formats
5. **Merge** — Combine base metadata + environment overlay into resolved file

---

## Authoring Workflow (AI-native)

Metadata authoring is performed directly by the AI using the Schema Quick Reference below.
No script needed — the AI reads inputs, applies mapping rules, and writes JSON/YAML.

### Build from scratch

When user describes data verbally or provides requirements:
1. Ask clarifying questions (source type, format, load strategy, platform)
2. Generate `connections[]` and `dataflows[]` using Schema Quick Reference
3. Write to `metadata/connections.json` + `metadata/dataflows.json`
4. Auto-validate: run `validate.py` + `lint.py` on generated output
5. Present result to user for confirmation

### Generate from architecture

When `.datacoolie/architect/yymmdd_architecture.md` exists (approved):
1. Read the approved architecture document
2. Apply the **Bridge Mapping** below to produce metadata
3. Write output to `metadata/` directory
4. Auto-validate: run `validate.py` + `lint.py`
5. Generate environment overlays from Environment Differences table

#### Bridge Mapping (Architecture → Metadata)

| Architecture section | Metadata field |
|---|---|
| Infrastructure Requirements → Resource Name | `connections[].name` |
| Infrastructure Requirements → Resource Type | `connections[].connection_type` (lakehouse/file/database) |
| Infrastructure Requirements → Platform + Purpose | `connections[].configure` (base_path, database_type, etc.) |
| Stage Definitions → Source column | `dataflows[].source.connection_name` + `.table` |
| Stage Definitions → Destination column | `dataflows[].destination.connection_name` + `.table` |
| Stage Definitions → Load Type column | `dataflows[].destination.load_type` |
| Stage Definitions → Engine column | Runner generation config |
| Stage Definitions → Schedule column | Orchestration config (out of metadata scope) |
| Partitioning Strategy → Partition Columns | `dataflows[].destination.partition_columns` |
| Partitioning Strategy → Expression | `partition_columns[].expression` |
| Environment Differences → Dev/Test/Prod | `environments/{env}.yaml` overlays |

**Mapping rules:**
- Each unique resource in Infrastructure Requirements → one Connection object
- Each row in Stage Definitions → one DataFlow object
- Stage name pattern `{source}_source2bronze` → `dataflows[].stage: "source2bronze"`
- If Load Type is `merge_upsert` / `merge_overwrite` / `scd2` → set `merge_keys` from discovery PKs
- If architecture specifies watermark → set `source.watermark_columns`
- Bronze partition by `_ingest_date` → `destination.partition_columns: [{"column": "_ingest_date", "expression": "current_date()"}]`
- Use `secrets_ref` for all credentials — never hardcode in configure

### Edit

When user requests a change to existing metadata:
1. Read the existing metadata file
2. Apply the targeted change to the correct field
3. Re-validate after change (`validate.py` + `lint.py`)
4. Show diff to user

### Add

When user adds a new source/connection/dataflow:
1. Generate the new object(s) using Schema Quick Reference
2. Append to the correct array in metadata
3. Validate cross-references (`connection_name` must exist)
4. Re-validate

### Remove

When user removes a connection or dataflow:
1. Confirm no other dataflows reference the removed connection
2. Remove the object or set `is_active: false`
3. Re-validate

---

## Scripts

All scripts at `ai/skills/datacoolie-metadata/scripts/`.

### validate.py — Schema validation

```bash
python scripts/validate.py <metadata_file> [--schema-version 0.1.0] [--schemas-dir PATH] [--fetch-latest] [--quiet]
python scripts/validate.py --fetch-latest                 # refresh schema only
python scripts/validate.py --fetch-latest config.json    # refresh then validate
python scripts/validate.py config.json --quiet           # CI mode: exit code only
```

- Auto-detects schema version from `$schema` field or falls back to latest
- Accepts JSON, YAML, or Excel (.xlsx) input
- Reports errors with JSON path (e.g., `dataflows[2].source: 'connection_name' is a required property`)
- `--fetch-latest`: downloads latest schema from GitHub into `~/.datacoolie/schemas/`; falls back to bundled copy on network failure
- `--schemas-dir PATH`: override schema location (useful in CI/CD with custom paths or `DATACOOLIE_SCHEMAS_DIR` env var)
- `--quiet` / `-q`: suppress all output; only exit code (for CI pipelines)
- Exit 0 = valid, Exit 1 = errors found, Exit 2 = input/schema error

### lint.py — Best-practice checks

```bash
python scripts/lint.py <metadata_file> [--engine polars|spark] [--env dev|test|prod] [--quiet]
```

- Accepts JSON, YAML, or Excel (.xlsx) input

Rules detected:
- `merge-keys-required`: merge/scd2 load_type without merge_keys
- `watermark-for-incremental`: incremental load without watermark_columns
- `no-infer-schema-prod`: inferSchema enabled in non-dev env
- `portable-filter-expression`: Spark-only dot notation in filters
- `undefined-connection`: dataflow references a connection_name not in connections[]
- `inactive-connection`: dataflow references inactive connection
- `unique-dataflow-name`: duplicate dataflow names
- `scd2-effective-column-required`: scd2 load_type without scd2_effective_column

### convert.py — Format conversion

```bash
python scripts/convert.py <input_file> --to json|yaml|excel [--output <path>]
python scripts/convert.py metadata.json --to excel
python scripts/convert.py metadata.xlsx --to json
python scripts/convert.py metadata.xlsx --to yaml
```

- Bidirectional: JSON ↔ YAML ↔ Excel (all 6 directions supported)
- Round-trip safe: JSON → Excel → JSON produces schema-valid output
- Preserves field order, readable indentation
- Excel format matches native datacoolie style (`local_use_cases.xlsx`): three sheets (`connections`, `dataflows`, `schema_hints`) with `source_*` / `destination_*` prefixed columns, compatible with `FileProvider`
- `partition_columns` serialized as comma-separated names (simple) or JSON array (with expressions)
- Boolean fields (`is_active`, `inferSchema`, etc.) correctly coerced from Excel numeric values
- Requires `pip install openpyxl` (for Excel)

### merge.py — Environment overlay merge

```bash
python scripts/merge.py --base <dir> --env <name> [--output <path>]
```

- Base directory must contain `connections.json` + `dataflows.json`
- Overlay at `environments/{env}.yaml` matched by `name` field
- Deep merges nested objects (e.g., `configure.base_path`)
- Unmentioned items pass through unchanged
- Default output: `.datacoolie/generated/metadata.{env}.json`

## Schema Location

- Single source of truth: `ai/skills/datacoolie-metadata/schemas/{version}/metadata.schema.json`
- Compatibility map: `ai/skills/datacoolie-metadata/schemas/compatibility.json`
- Latest: v0.1.0 (Draft 2020-12, conditional configure per connection_type)

## Schema Quick Reference (for AI metadata generation)

When generating metadata JSON/YAML, follow this structure exactly.

### Top-level structure

```json
{
  "$schema": "https://raw.githubusercontent.com/datacoolie/datacoolie/main/ai/skills/datacoolie-metadata/schemas/0.1.0/metadata.schema.json",
  "connections": [ ... ],
  "dataflows": [ ... ],
  "schema_hints": [ ... ]
}
```

At least one of `connections` or `dataflows` is required. `schema_hints` is optional.

### Connection object

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | **yes** | Unique identifier |
| `connection_type` | enum | no | `file` \| `lakehouse` \| `database` \| `api` \| `function` \| `streaming` |
| `format` | enum | no | `delta` \| `iceberg` \| `parquet` \| `csv` \| `json` \| `jsonl` \| `avro` \| `excel` \| `sql` \| `api` \| `function` |
| `catalog` | string\|null | no | Unity/Iceberg catalog |
| `database` | string\|null | no | Database/schema namespace |
| `configure` | object | no | Type-specific settings — common + type-conditional fields (see below) |
| `secrets_ref` | object\|null | no | `{ "vault_arn_or_url": ["field1", "field2"] }` — empty string key = env vars |
| `is_active` | boolean | no | Default `true` |

**Connection configure — common fields (all types):**
`read_options` (object), `write_options` (object), `use_schema_hint` (boolean, default true — set false to skip type casting), `backward_days`, `backward_months`, `backward_hours`, `backward_years`, `backward_closing_day` (integers), `backward` (nested object: `{days, months, hours, years, closing_day}`)

**Connection configure — by connection_type:**
- **file**: `base_path` (string), `use_hive_partitioning` (boolean, default false), `date_folder_partitions` (string, e.g. `{year}/{month}/{day}`), backward fields above
- **lakehouse**: `base_path` (string), `catalog`, `database` (override top-level), `athena_output_location`, `generate_manifest` (boolean, default false), `register_symlink_table` (boolean), `symlink_database_prefix` (string, default `symlink_`)
- **database**: `database_type` (postgresql\|mysql\|mssql\|oracle\|sqlite), `auth_type` (password\|service_principal\|managed_identity\|access_token — default `password`), `host`, `port` (int), `database`, `username` (or SPN client_id), `password` (or SPN client_secret), `tenant_id` (Azure AD tenant — SPN only), `token` (pre-fetched token — access_token only), `driver`, `url`, `encrypt`
- **api**: `base_url`, `timeout` (number), `auth_type` (bearer\|api_key\|basic\|oauth2_client_credentials\|aws_sigv4), `auth_token`, `api_key_header`, `api_key_value`, `username`, `password` (basic auth), `default_headers` (object), `watermark_to_param_timezone` (IANA or ±HH:MM); OAuth2: `token_url`, `client_id`, `client_secret`, `token_auth_method`, `scope`, `token_request_body_format` (form\|json), `token_request_extras`; AWS SigV4: `aws_region`, `aws_service`, `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`

### DataFlow object

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | **yes** | Unique dataflow name |
| `description` | string\|null | no | Human-readable description |
| `stage` | string\|null | no | `<source_name>2<destination_name>`, e.g. `source2bronze`, `source_sap2bronze`, `bronze2silver` |
| `group_number` | int\|null | no | Parallel execution group |
| `execution_order` | int\|null | no | Order within group (lower = first) |
| `processing_mode` | enum | no | `batch` \| `microbatch` \| `streaming` (default: batch) |
| `is_active` | boolean | no | Default `true` |
| `source` | Source | **yes** | Read-side config |
| `destination` | Destination | **yes** | Write-side config |
| `transform` | Transform | no | Transformation rules |
| `configure` | object | no | Dataflow-level free-form config |

### Source object

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `connection_name` | string | **yes** | Must match a Connection `name` |
| `schema_name` | string\|null | no | Schema namespace |
| `table` | string\|null | no | Table/object to read |
| `query` | string\|null | no | SQL query (alternative to table) |
| `python_function` | string\|null | no | Dotted path for function sources |
| `watermark_columns` | string[] | no | Columns for incremental reads |
| `filter_expression` | string\|null | no | SQL WHERE applied after watermark filter |
| `configure` | object | no | Look-back overrides, API pagination, watermark push-down |

**Source configure — look-back (common fields):** `read_options` (object), `backward_days`, `backward_months`, `backward_hours`, `backward_years`, `backward_closing_day` (integers — override connection-level), `backward` (nested object: `{days, months, hours, years, closing_day}`)

**Source configure — API:** `endpoint`, `method` (default GET), `params` (object), `body` (object), `data_path` (dot-path to records array, e.g. `data.items`); pagination: `pagination_type` (offset\|cursor\|next_link), `page_size` (default 100), `max_pages` (default 1000), `next_link_path` (default `next`), `cursor_path` (default `next_cursor`), `cursor_param`, `offset_param`, `limit_param`, `total_path`, `offset_max_workers` (default 4); rate limiting: `rate_limit_delay` (seconds), `max_retries` (default 10)

**Source configure — watermark push-down (API):** `watermark_param_mapping` (object: `{"updated_at": "updated_since"}`), `watermark_to_param` (string), `watermark_param_location` (params\|body, default params), `watermark_param_format` (iso\|date\|timestamp\|timestamp_ms\|datetime\|datetime_ms, default iso), `watermark_to_param_timezone`

**Source configure — watermark range splitting (API):** `watermark_range_interval_unit` (hour\|day\|month\|year), `watermark_range_interval_amount` (int, default 1), `watermark_range_start` (ISO-8601, required if unit set), `watermark_range_max_workers` (default 4), `watermark_range_to_exclusive_offset` (1ms\|1s\|1day\|null)

### Destination object

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `connection_name` | string | **yes** | Must match a Connection `name` |
| `schema_name` | string\|null | no | Schema namespace |
| `table` | string | **yes** | Target table name |
| `load_type` | enum | no | `full_load` \| `overwrite` \| `append` \| `merge_upsert` \| `merge_overwrite` \| `scd2` (default: append) |
| `merge_keys` | string[] | no | Key columns for merge/scd2 — required when load_type is merge or scd2 |
| `partition_columns` | PartitionColumn[] | no | `[{"column": "col", "expression": "year(col)"}]` — expression optional; can also be specified inside `configure.partition_columns` |
| `configure` | object | no | `scd2_effective_column` (string), `replace_by_watermark` (boolean, default false), `write_options` (object), `partition_columns` (PartitionColumn[] — alternative to top-level `partition_columns`) |

### Transform object

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `deduplicate_columns` | string[] | no | Dedup key columns |
| `latest_data_columns` | string[] | no | Tiebreaker columns to pick latest row when deduplicating |
| `filter_expression` | string\|null | no | SQL WHERE applied after additional_columns |
| `additional_columns` | AdditionalColumn[] | no | Computed columns: `[{"column": "etl_loaded_at", "expression": "current_timestamp()"}]` |
| `schema_hints` | SchemaHint[] | no | Column-level type casting applied at load |
| `configure` | object | no | `convert_timestamp_ntz` (boolean, default true), `deduplicate_by_rank` (boolean, default false) |

### SchemaHint object

Required: `column_name`, `data_type`

| Field | Type | Description |
|-------|------|-------------|
| `column_name` | string | Target column name |
| `data_type` | string | Target type: `int`, `string`, `decimal`, `timestamp`, `date`, `boolean`, `float`, `double`, `long`, `short`, `byte`, etc. |
| `format` | string\|null | Date/timestamp pattern e.g. `yyyy-MM-dd`, `yyyy-MM-dd HH:mm:ss` |
| `precision` | int\|null | Decimal total digits |
| `scale` | int\|null | Decimal digits after decimal point |
| `default_value` | string\|null | Fallback value for nulls |
| `ordinal_position` | int\|null | Column ordering (default 0) |
| `is_active` | boolean | Default `true` |

### SharedSchemaHint object (top-level `schema_hints` items)

Top-level `schema_hints` apply across dataflows by connection+table match — distinct from `transform.schema_hints`.

Required: `connection_name`, `table_name`, `hints`

```json
{
  "connection_name": "raw_db",
  "schema_name": null,
  "table_name": "orders",
  "hints": [
    { "column_name": "amount", "data_type": "decimal", "precision": 18, "scale": 2 }
  ]
}
```

### Minimal valid example

```json
{
  "$schema": "https://raw.githubusercontent.com/datacoolie/datacoolie/main/ai/skills/datacoolie-metadata/schemas/0.1.0/metadata.schema.json",
  "connections": [
    { "name": "raw_csv", "connection_type": "file", "format": "csv" },
    { "name": "bronze_lake", "connection_type": "lakehouse", "format": "delta", "configure": { "base_path": "/mnt/lake/bronze" } }
  ],
  "dataflows": [
    {
      "name": "ingest_orders",
      "stage": "source2bronze",
      "source": { "connection_name": "raw_csv", "table": "orders" },
      "destination": { "connection_name": "bronze_lake", "table": "orders", "load_type": "append" },
      "transform": {
        "schema_hints": [
          { "column_name": "order_id", "data_type": "int" },
          { "column_name": "amount", "data_type": "decimal", "precision": 18, "scale": 2 },
          { "column_name": "created_at", "data_type": "timestamp", "format": "yyyy-MM-dd HH:mm:ss" }
        ]
      }
    }
  ]
}
```

### Common patterns

#### Load strategies

- **Full load / overwrite**: `destination.load_type: "full_load"` or `"overwrite"` — no `watermark_columns` needed; replaces all data
- **Append**: `source.watermark_columns: ["modified_at"]` + `destination.load_type: "append"` — only new rows added since last watermark
- **Incremental upsert**: `source.watermark_columns: ["modified_at"]` + `destination.load_type: "merge_upsert"` + `destination.merge_keys: ["order_id"]`
- **Merge upsert without watermark**: no `watermark_columns` — reads entire source every run; combine with dedup to resolve duplicates in source: `transform.deduplicate_columns: ["order_id"]` + `transform.latest_data_columns: ["modified_at"]`
- **SCD2**: `load_type: "scd2"` + `merge_keys` + `destination.configure.scd2_effective_column: "modified_at"` — tracks history via effective date column
- **Merge overwrite (key-based, pure)**: `load_type: "merge_overwrite"` + `merge_keys: ["id"]` — deletes rows matching the merge keys (including partition columns) then re-inserts the new batch; handles source-side deletes within the fetched key set; no watermark window needed
- **Merge overwrite by watermark window**: `load_type: "merge_overwrite"` + `destination.configure.replace_by_watermark: true` — deletes ALL rows within the watermark window (not by key), then appends; handles source-side deletions within the time window; requires `source.configure.backward` for look-back scope; both `merge_keys` and `source.watermark_columns` must be set

#### Watermark & filtering

- **File system watermark**: `source.watermark_columns: ["__file_modification_time"]` — built-in virtual column tracking file mtime; used with file/lakehouse sources
- **Backward look-back window**: `source.configure.backward: {"days": 7}` — shifts watermark back by N days/hours/months/years; `closing_day` anchor for monthly: `{"months": 1, "closing_day": 10}`
- **Source-side filter** (before watermark push-down): `source.configure.endpoint: "..."` applies at fetch time; for files/DB use `source.filter_expression: "amount > 0"` — evaluated before writing
- **Transform-side filter** (post-load): `transform.filter_expression: "status != 'cancelled' AND amount > 0"` — SQL predicate applied to the loaded DataFrame

#### Transforms

- **Partition col resolution (all load types)**: uses `deduplicate_columns` if explicitly set; auto-falls back to `merge_keys` otherwise; dedup skipped if both empty
- **Order col resolution (all load types)**: uses `latest_data_columns` if set; falls back to `source.watermark_columns`; dedup skipped if both empty
- **RANK mode**: triggers when (`load_type == "merge_overwrite"` + `merge_keys` non-empty + `deduplicate_columns` NOT explicitly set) — OR — `transform.configure.deduplicate_by_rank: true` (any load type); keeps ALL tied rows
- **ROW_NUMBER mode**: all other cases (including explicit `deduplicate_columns` with `merge_overwrite`); keeps ONE latest row per key
- **Composite dedup key**: `transform.deduplicate_columns: ["order_id", "region"]`
- **Add computed columns**: `transform.additional_columns: [{"column": "order_year", "expression": "EXTRACT(YEAR FROM order_date)"}]` — SQL expression evaluated against loaded DataFrame
- **Schema hints**: `transform.schema_hints: [{"column_name": "amount", "data_type": "DECIMAL", "precision": 18, "scale": 2}]` — supported types: `DATE`, `TIMESTAMP`, `DATETIME` (NTZ), `DECIMAL`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `BOOLEAN`, `STRING`
- **Disable a hint temporarily**: add `"is_active": false` to the hint object
- **Disable NTZ conversion**: `transform.configure.convert_timestamp_ntz: false` — keeps TIMESTAMP as zoned
- **Partitioned destination**: `destination.partition_columns: [{"column": "order_year", "expression": "EXTRACT(YEAR FROM order_date)"}]` — also accepted inside `destination.configure.partition_columns`
- **Write options**: `connection.configure.write_options: {"compression": "snappy"}` — passed to the Spark writer

#### File / lakehouse connections

- **Date-folder partitions**: `connection.configure.date_folder_partitions: "{year}/{month}/{day}"` — source reads the folder matching the watermark date; dest writes into dated sub-folder
- **Hive partitioning source**: `connection.configure.use_hive_partitioning: true` — reads column=value directory layout
- **Disable global schema hint for one connection**: `connection.configure.use_schema_hint: false`

#### SQL query source (instead of table name)

- **DB or lakehouse custom query**: replace `source.table` with `source.query: "SELECT order_id, amount FROM orders WHERE amount > 100"` — no `table` key needed

#### Python function source

- **Custom loader**: set `connection.connection_type: "function"` + `source.python_function: "functions.sources.load_orders_custom"` — function returns a DataFrame; `source.table` is passed as argument when provided

#### Database authentication

| `auth_type` | Required fields | Use case |
|-------------|-----------------|----------|
| `password` (default) | `username`, `password` | All databases — standard SQL auth |
| `service_principal` | `username` (= client_id), `password` (= client_secret), `tenant_id` | Azure SQL, Fabric SQL via Entra ID |
| `managed_identity` | none (or `username` = client_id for user-assigned MI) | Azure-hosted runtimes (AKS, Fabric, App Service) |
| `access_token` | `token` (+ optional `username` for non-MSSQL) | Pre-fetched bearer token (Azure, AWS RDS IAM, GCP Cloud SQL IAM) |

- **Service principal**: `auth_type: "service_principal"` + `username: "AZURE_CLIENT_ID"` + `password: "AZURE_CLIENT_SECRET"` + `tenant_id: "AZURE_TENANT_ID"` — `secrets_ref` all three via env
- **Managed identity (system-assigned)**: `auth_type: "managed_identity"` only — no credentials needed
- **Managed identity (user-assigned)**: add `username: "msi-client-id"` to identify the specific MI
- **Access token (Azure SQL)**: `auth_type: "access_token"` + `token: "AZURE_SQL_TOKEN"` via `secrets_ref` — token injected into JDBC `accessToken` property (MSSQL) or as password (PG/MySQL)
- **Access token (AWS RDS IAM / GCP Cloud SQL IAM)**: same pattern — set `username` to the IAM DB user + `token` to the short-lived token generated externally
- **Fabric SQL endpoint**: host `*.datawarehouse.fabric.microsoft.com` — `auth_type: "password"` is rejected at validation time; must use `service_principal`, `managed_identity`, or `access_token`
- **Backward compat**: omitting `auth_type` is identical to `auth_type: "password"` — all existing configs work unchanged

#### API connections

- **Auth types** (connection-level): `auth_type: "bearer"` + `auth_token`; `"api_key"` + `api_key_header` + `api_key_value`; `"basic"` + `username`/`password`; `"oauth2_client_credentials"` + `token_url` + `client_id` + `client_secret` (optional `token_auth_method: "client_secret_basic"` + `scope`)
- **Pagination: none** — `source.configure.endpoint: "/api/orders/simple"` — single-page response; wrap with `data_path` if records are nested
- **Pagination: offset** — `pagination_type: "offset"` + `page_size: 5` + `data_path: "data"`; add `total_path: "total"` + `offset_max_workers: 4` for concurrent fetching
- **Pagination: cursor** — `pagination_type: "cursor"` + `cursor_path: "next_cursor"` + `data_path: "items"`
- **Pagination: next_link** — `pagination_type: "next_link"` + `next_link_path: "_links.next"` + `data_path: "records"` — follows `@odata.nextLink` style
- **Nested data path**: `data_path: "response.payload.orders"` — dot-notation traversal into response JSON
- **POST request with body**: `source.configure.method: "POST"` + `source.configure.body: {"status": "completed"}`
- **Static query-string params**: `source.configure.params: {"api_version": "v2"}` — merged with watermark params on every request
- **Rate limiting**: `source.configure.rate_limit_delay: 0.5` — seconds between page fetches
- **Cap page count**: `source.configure.max_pages: 2`
- **API incremental (watermark push-down)**: `source.watermark_columns: ["modified_at"]` + `source.configure.watermark_param_mapping: {"modified_at": "modified_since"}` + `watermark_param_format: "iso"` — formats: `iso`, `date`, `timestamp`, `timestamp_ms`, `datetime`, `datetime_ms`
- **API to-param (range end)**: `source.configure.watermark_to_param: "modified_until"` — sends both from and to bounds; add `watermark_range_to_exclusive_offset: "1s"` to subtract 1 second from end bound (avoids double-counting on inclusive APIs)
- **Watermark in POST body**: `watermark_param_location: "body"` — injects from/to into the JSON body instead of query string
- **API timezone**: `connection.configure.watermark_to_param_timezone: "+07:00"` — override per-source with `source.configure.watermark_to_param_timezone: "UTC"`
- **API range-split fetch** (backfill): `watermark_range_interval_unit: "day"|"month"|"year"` + `watermark_range_interval_amount: 7` + `watermark_range_start: "2024-01-01T00:00:00"` + `watermark_range_max_workers: 4`

#### Secrets (never hardcode)

- `secrets_ref: {"env:": ["username", "password"]}` — `env:` prefix = EnvResolver; env var name = field name as-is (e.g. resolves `username` → `$username`)
- `secrets_ref: {"env:APP_": ["username", "password"]}` — env var name = `APP_username`, `APP_password` (prefix prepended)
- `secrets_ref: {"": ["username", "password"]}` — empty string key = platform-native provider (AWS Secrets Manager, Azure Key Vault, Databricks secret scope, etc.)
- `secrets_ref: {"arn:aws:secretsmanager:us-east-1:123:secret:mydb": ["password"]}` — AWS Secrets Manager
- Multiple sources allowed: `{"env:APP_": ["host", "port"], "arn:...": ["password"]}` — each field must appear under exactly one source

#### Orchestration

- **Job assignment (`group_number` + `job_num > 1`)**: `group_number % job_num == job_index` — dataflows with the same `group_number` are guaranteed to run in the same job; different group_numbers are distributed across jobs by modulo; omit `group_number` (null) for hash-based distribution (no co-location guarantee)
- **Execution order within a group**: dataflows in the same group run sequentially sorted by `execution_order` (lower first, nulls = 0)
- **Cross-group independence**: groups with different `group_number` values run independently; no ordering guarantee between groups across jobs
- **Dependencies**: place prerequisite dataflows in a lower `group_number`; consumer dataflows in a higher one — within the same job the groups run in `group_number` ascending order

### Validation rules to remember

1. Every `source.connection_name` and `destination.connection_name` must match a Connection `name`
2. `merge_upsert` / `merge_overwrite` / `scd2` require non-empty `merge_keys`
3. Incremental source should have `watermark_columns`
4. Don't use `inferSchema: true` in production (lint will flag it)
5. Dataflow `name` must be unique across the file
6. `additionalProperties: false` on Connection, DataFlow, Source, Destination, Transform, SchemaHint, PartitionColumn — no unknown fields allowed at those levels

## Dependencies

- `jsonschema` (pip) — Draft 2020-12 validator
- `pyyaml` (pip) — YAML parsing/serialization

## Examples

```bash
# Validate single file
python scripts/validate.py metadata/file/local_use_cases.json

# Validate all metadata in directory
Get-ChildItem metadata/file/*.json | ForEach-Object { python scripts/validate.py $_ }

# Lint for production with Spark engine
python scripts/lint.py metadata.json --engine spark --env prod

# Convert JSON to YAML for editing
python scripts/convert.py metadata.json --to yaml

# Merge base + prod overlay
python scripts/merge.py --base metadata/ --env prod --output .datacoolie/generated/metadata.prod.json
```
