# Scenarios reference

Full field-level dump of every entry in [scenarios.json](scenarios.json).
Consult [../README.md](../README.md) for narrative context and usage.

## Field glossary

| Field | Required | Meaning |
|---|---|---|
| `engine` | ✓ | `polars` or `spark` |
| `metadata_type` | ✓ | `file`, `database`, `api`, or `maintenance` |
| `platform` | | `local` (default) or `aws` — sets `is_aws` in runners |
| `metadata_path` | file / maintenance | Path to JSON / YAML / XLSX metadata file |
| `metadata_db_connection_string` | database | SQLAlchemy URL |
| `metadata_api_url` | api | Base URL of metadata REST API |
| `metadata_api_key` | | Bearer token / API key |
| `metadata_workspace_id` | database / api | Workspace ID filter |
| `stage` | non-maintenance | Stage name(s); `""` runs every stage |
| `column_name_mode` | | `lower` (default) or `snake` |
| `connection` | maintenance | Connection name filter |
| `skip_api_sources` | | Skip dataflows with `connection_type=api` |
| `dry_run` | | Driver-level dry-run |
| `max_workers` | | Parallel dataflow workers (forwarded to `DataCoolieRunConfig`) |
| `timeout_seconds` | | Override dispatcher timeout |
| `pre_clean_paths` | | Repository-relative output directories removed before the scenario |
| `services` | | Docker Compose services ensured before setup and execution |
| `setup` | | Repository-local setup script, optional args, and timeout |
| `engine_setup` | | Same-process repository-local function and args invoked after engine creation |
| `validation` | | Expected exit code, required console text, and optional output-validator script |
| `priority` | | `P0`, `P1`, or `P2` (for `--priority` filter) |
| `notes` | | Free-form description |

`validation` supports `expected_exit_code` (default `0`),
`required_console_text` (string or list), `script` (repository-relative Python
file), optional `args`, and `timeout_seconds` for that script. A scenario is
reported as PASS only when every configured assertion succeeds.

`setup` supports `script`, optional `args`, and `timeout_seconds`. The script
must resolve inside the repository and writes to a separate scenario setup
log. A setup failure prevents the ETL child from starting.

`engine_setup` supports `python_function` and optional `args`. Unlike `setup`,
it runs inside the ETL child after engine construction, so it can register
relations in the active Polars SQLContext. It is simulator configuration, not
DataCoolie source metadata.

## Dataflow authoring invariant

One dataflow must represent one primary case or feature and write to a unique
output. It may use other features as supporting setup when they are necessary
to exercise the primary behavior. Supporting features must not become separate
assertion targets or make the main purpose ambiguous; behavior that matters
independently belongs in its own dataflow. Multiple operations may also form
the primary case when their interaction is explicit, such as the stable
value-rule ordering case with two ordered rules.

---

## P0 — Local filesystem, no Docker

### `local_polars_transform_features` / `local_spark_transform_features`

Runs the dedicated `transformer_features.json` metadata fixture. Both variants
assert the aggregate missing-schema-hint warning and then validate persisted
Parquet schemas and values across 25 independent, single-case outputs covering
normalization, literal replacement, mapping, rule order, value-rule/schema-cast
order, schema hints, portable SHA-256 and signed XXHash64 parity, PII masking,
select/drop, multi-column rename, and missing-column policies.

### `local_polars_transform_dedup_strict` / `local_spark_transform_dedup_strict`

Expected-failure scenarios proving that a missing configured dedup order
column exits with code `2`, even when `missing_column_policy` is `ignore`.

### Typed-literal and sanitizer expected failures

The paired `local_{polars,spark}_transform_invalid_fill` and
`local_{polars,spark}_transform_invalid_redact` scenarios prove that string
literals cannot be applied to integer columns. The paired
`local_{polars,spark}_transform_sanitizer_collision` scenarios prove that two
distinct source names cannot collapse to the same sanitized name. Every case
runs in its own one-dataflow stage and requires exit code `2` plus its stable
diagnostic text.

### `local_polars_qualified_sql_delta`

Runs seven independent dataflows from `polars_qualified_sql.json`. The cases
prove Delta 4/3/2/1-part references, component include/exclude filters, lazy
indexing, and reuse within one query. A scoped setup script recreates the
nested Delta fixtures; the same-process engine setup registers them; normal
DeltaReader instances execute `source.query`; and a validator reconciles every
unique Parquet result.

### `local_polars_qualified_sql_delta_ambiguity`

Runs one isolated expected-failure dataflow. Two different Delta tables share
the suffix `shared.orders_ambiguous`; the scenario requires exit code `2` and
both fully qualified candidates in the diagnostic.

### `local_polars_file`

| Field | Value |
|---|---|
| engine | polars |
| metadata_type | file |
| platform | local |
| metadata_path | `./usecase-sim/metadata/file/local_use_cases.json` |
| stage | `""` (all) |
| column_name_mode | lower |
| priority | P0 |
| Docker needs | optional `mock-api` (for `api_*` stages) |

### `local_polars_file_yaml`

| Field | Value |
|---|---|
| engine | polars |
| metadata_type | file |
| platform | local |
| metadata_path | `./usecase-sim/metadata/file/local_use_cases.json` |
| stage | `""` |
| column_name_mode | lower |
| priority | P0 |
| Notes | Uses YAML-emitted metadata (generated by `setup_metadata.py --targets file`); API-source dataflows skipped because only JSON supports them |

### `local_polars_file_excel`

| Field | Value |
|---|---|
| engine | polars |
| metadata_type | file |
| platform | local |
| metadata_path | `./usecase-sim/metadata/file/local_use_cases.json` |
| stage | `""` |
| column_name_mode | lower |
| priority | P0 |
| Notes | Uses XLSX-emitted metadata; API-source dataflows skipped |

### `local_spark_file`

| Field | Value |
|---|---|
| engine | spark |
| metadata_type | file |
| platform | local |
| metadata_path | `./usecase-sim/metadata/file/local_use_cases.json` |
| stage | `""` |
| column_name_mode | lower |
| skip_api_sources | `true` |
| priority | P0 |

---

## P1 — Docker-backed metadata + lakehouse maintenance

### `local_polars_database`

| Field | Value |
|---|---|
| engine | polars |
| metadata_type | database |
| platform | local |
| metadata_db_connection_string | `sqlite:///./usecase-sim/metadata/database/datacoolie_metadata.db` |
| metadata_workspace_id | `local-workspace` |
| stage | `""` |
| priority | P1 |
| Docker needs | none (SQLite file) |

### `local_polars_database_postgres`

| Field | Value |
|---|---|
| engine | polars |
| metadata_type | database |
| platform | local |
| metadata_db_connection_string | `postgresql+psycopg2://datacoolie:datacoolie@localhost:5432/datacoolie` |
| metadata_workspace_id | `local-workspace` |
| priority | P1 |
| Docker needs | `postgres` (seeded via `setup_metadata.py --targets db:postgresql`) |

### `local_polars_database_mysql`

| Field | Value |
|---|---|
| engine | polars |
| metadata_type | database |
| platform | local |
| metadata_db_connection_string | `mysql+pymysql://datacoolie:datacoolie@localhost:3306/datacoolie` |
| metadata_workspace_id | `local-workspace` |
| priority | P1 |
| Docker needs | `mysql` |

### `local_polars_database_mssql`

| Field | Value |
|---|---|
| engine | polars |
| metadata_type | database |
| platform | local |
| metadata_db_connection_string | `mssql+pymssql://sa:Datacoolie%401@localhost:1433/datacoolie` |
| metadata_workspace_id | `local-workspace` |
| priority | P1 |
| Docker needs | `mssql` |

### `local_polars_database_oracle`

| Field | Value |
|---|---|
| engine | polars |
| metadata_type | database |
| platform | local |
| metadata_db_connection_string | `oracle+oracledb://datacoolie:datacoolie@localhost:1521/?service_name=FREEPDB1` |
| metadata_workspace_id | `local-workspace` |
| priority | P1 |
| Docker needs | `oracle` |

### `local_spark_database`

| Field | Value |
|---|---|
| engine | spark |
| metadata_type | database |
| platform | local |
| metadata_db_connection_string | `sqlite:///./usecase-sim/metadata/database/datacoolie_metadata.db` |
| metadata_workspace_id | `local-workspace` |
| skip_api_sources | `true` |
| priority | P1 |

### `local_polars_api`

| Field | Value |
|---|---|
| engine | polars |
| metadata_type | api |
| platform | local |
| metadata_api_url | `http://localhost:8000` |
| metadata_workspace_id | `local-workspace` |
| priority | P1 |
| Docker needs | `metadata-api` (+ `postgres`) |

### `local_spark_api`

| Field | Value |
|---|---|
| engine | spark |
| metadata_type | api |
| platform | local |
| metadata_api_url | `http://localhost:8000` |
| metadata_workspace_id | `local-workspace` |
| priority | P1 |
| Docker needs | `metadata-api` (+ `postgres`) |

### `local_polars_delta_maintenance`

| Field | Value |
|---|---|
| engine | polars |
| metadata_type | maintenance |
| platform | local |
| metadata_path | `./usecase-sim/metadata/file/local_use_cases.json` |
| connection | `local_delta_dest` |
| priority | P1 |

### `local_polars_iceberg_maintenance`

| Field | Value |
|---|---|
| engine | polars |
| metadata_type | maintenance |
| platform | local |
| metadata_path | `./usecase-sim/metadata/file/local_use_cases.json` |
| connection | `local_iceberg_dest` |
| priority | P1 |
| Docker needs | `minio` + `iceberg-rest` |

### `local_spark_delta_maintenance`

| Field | Value |
|---|---|
| engine | spark |
| metadata_type | maintenance |
| platform | local |
| metadata_path | `./usecase-sim/metadata/file/local_use_cases.json` |
| connection | `local_delta_dest` |
| priority | P1 |

### `local_spark_iceberg_maintenance`

| Field | Value |
|---|---|
| engine | spark |
| metadata_type | maintenance |
| platform | local |
| metadata_path | `./usecase-sim/metadata/file/local_use_cases.json` |
| connection | `local_iceberg_dest` |
| priority | P1 |
| Docker needs | `minio` + `iceberg-rest` |

### Qualified Iceberg SQL scenarios

`local_polars_qualified_sql_iceberg` runs six independent dataflows for
default catalog mapping, structured `logical_prefix` replacement, short-name
resolution, include/exclude filters, and lazy reuse.
`local_polars_qualified_sql_iceberg_ambiguity` contains exactly one expected
failure dataflow. Both use explicit `minio` and `iceberg-rest` services and
scoped `qsql_*` namespaces in the local REST catalog. Registration is runner
setup; each dataflow uses a normal Iceberg source and metadata `source.query`.

---

## P2 — AWS platform (MinIO + Iceberg REST)

### `aws_polars_file`

| Field | Value |
|---|---|
| engine | polars |
| metadata_type | file |
| platform | aws |
| metadata_path | `./usecase-sim/metadata/file/aws_use_cases.json` |
| stage | `""` |
| priority | P2 |
| Docker needs | `minio` + `iceberg-rest` |

### `aws_spark_file`

| Field | Value |
|---|---|
| engine | spark |
| metadata_type | file |
| platform | aws |
| metadata_path | `./usecase-sim/metadata/file/aws_use_cases.json` |
| stage | `""` |
| skip_api_sources | `true` |
| priority | P2 |
| Docker needs | `minio` + `iceberg-rest` |

### `aws_polars_delta_maintenance`

| Field | Value |
|---|---|
| engine | polars |
| metadata_type | maintenance |
| platform | aws |
| metadata_path | `./usecase-sim/metadata/file/aws_use_cases.json` |
| connection | `aws_delta_dest` |
| priority | P2 |
| Docker needs | `minio` |

### `aws_polars_iceberg_maintenance`

| Field | Value |
|---|---|
| engine | polars |
| metadata_type | maintenance |
| platform | aws |
| metadata_path | `./usecase-sim/metadata/file/aws_use_cases.json` |
| connection | `aws_iceberg_dest` |
| priority | P2 |
| Docker needs | `minio` + `iceberg-rest` |

### `aws_spark_delta_maintenance`

| Field | Value |
|---|---|
| engine | spark |
| metadata_type | maintenance |
| platform | aws |
| metadata_path | `./usecase-sim/metadata/file/aws_use_cases.json` |
| connection | `aws_delta_dest` |
| priority | P2 |
| Docker needs | `minio` |

### `aws_spark_iceberg_maintenance`

| Field | Value |
|---|---|
| engine | spark |
| metadata_type | maintenance |
| platform | aws |
| metadata_path | `./usecase-sim/metadata/file/aws_use_cases.json` |
| connection | `aws_iceberg_dest` |
| priority | P2 |
| Docker needs | `minio` + `iceberg-rest` |

---

## Updating this file

Scenarios are added or modified in [scenarios.json](scenarios.json). When you
change that file, update the matching entry here by hand — this reference is
intentionally hand-maintained so each row can carry human-written notes the
raw JSON cannot.
