# Scenarios reference

Full field-level dump of every entry in [scenarios.json](scenarios.json).
Consult [../README.md](../README.md) for narrative context and usage.

## Field glossary

| Field | Required | Meaning |
|---|---|---|
| `engine` | ✓ | `polars` or `spark` |
| `spark_container` | | Docker container override for Spark scenarios; defaults to `datacoolie-spark`, use `datacoolie-spark4` for the opt-in Spark 4.1.0 service |
| `metadata_type` | ✓ | `file`, `database`, `api`, or `maintenance` |
| `platform` | | `local` (default) or `aws` — sets `is_aws` in runners |
| `metadata_path` | file / maintenance | Path to JSON / YAML / XLSX metadata file |
| `metadata_base_path` | file / maintenance | Directory of section-wrapped metadata documents; Driver creates the FileProvider |
| `artifact_base_path` | file / maintenance | Deployed artifact root; metadata defaults to `<root>/metadata` when no explicit metadata root is supplied |
| `sql_base_path` | | Optional SQL root for shorthand references such as `queries/orders.sql`; provider configuration is preferred and Driver input is the session fallback; repeatable roots use their folder prefix |
| `metadata_db_connection_string` | database | SQLAlchemy URL |
| `metadata_api_url` | api | Base URL of metadata REST API |
| `metadata_api_key` | | Bearer token / API key |
| `metadata_workspace_id` | database / api | Workspace ID filter |
| `stage` | non-maintenance | Stage name(s); `""` runs every stage |
| `column_name_mode` | | `lower` (default) or `snake` |
| `connection` | maintenance | Connection name filter |
| `skip_api_sources` | | Skip dataflows with `connection_type=api` |
| `dry_run` | | Driver-level dry-run |
| `state_base_path` | | Runtime state root; derives logs and file-provider watermarks when component roots are absent |
| `log_base_path` | | Explicit framework log root; overrides state-derived/default logs |
| `derive_log_paths_from_state` | | Omit `--log-path` so Driver derives logs below `state_base_path` |
| `job_id` | | Stable Driver session/job identifier used by log validators |
| `run_attributes` | | JSON object of caller-owned external correlation values persisted with JobRuntime |
| `log_persistence_mode` | | Structured logger mode: `snapshot` or `batch` |
| `log_flush_interval_seconds` | | Optional periodic batch flush override |
| `log_flush_batch_bytes` | | Optional batch byte threshold override |
| `log_console_color` | | Optional console color policy: `auto`, `always`, or `never` |
| `max_workers` | | Parallel dataflow workers (forwarded to `DataCoolieRunConfig`) |
| `timeout_seconds` | | Override dispatcher timeout |
| `pre_clean_paths` | | Repository-relative output directories removed before the scenario |
| `services` | | Docker Compose services ensured before setup and execution |
| `setup` | | Repository-local setup script, optional args, and timeout |
| `engine_setup` | | Same-process repository-local function and args invoked after engine creation |
| `needs_iceberg` | | Initialize the local Iceberg catalog even when the stage name is neutral |
| `invocations` | | Ordered child runs; put a negative child's `expected_exit_code` on its invocation |
| `validation` | | Final scenario console/output checks; keep its `expected_exit_code` at `0` |
| `priority` | | `P0`, `P1`, or `P2` (for `--priority` filter) |
| `notes` | | Free-form description |

`validation` supports `expected_exit_code` (default `0`),
`required_console_text` (string or list), `script` (repository-relative Python
file), optional `args`, and `timeout_seconds` for that script. A scenario is
reported as PASS only when every configured assertion succeeds.

The dispatcher first checks each invocation's exit, then calls final validation
with `0`. For negative tests, declare `invocations: [{"expected_exit_code": 2}]`
(or the actual expected child code) and leave `validation.expected_exit_code`
at `0`. A nonzero top-level validation value is used as a legacy single-child
fallback but subsequently fails the final check. See the public
[expected-failure guide](https://datacoolie.github.io/datacoolie/project/expected-failure-scenarios/)
for a runnable example and startup/dataflow logging distinctions.

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

### Artifact and path contracts

The following focused P0 scenarios keep one dataflow per primary contract.
Artifact scenarios consume checked-in deployed-artifact fixtures directly and
never mutate those fixtures during a run.

| Scenario | Primary contract | Important inputs / assertions |
|---|---|---|
| `local_polars_artifact_default_metadata` | Default artifact metadata | `artifact_base_path`; Driver-inferred `FileProvider`; output values |
| `local_polars_artifact_custom_metadata` | Explicit metadata root | `metadata_base_path` overrides the artifact default; nested section wrappers are discovered |
| `local_polars_query_artifact_relative` | Artifact-relative SQL | shorthand `queries/orders.sql` resolves directly below the artifact root when no SQL root is supplied |
| `local_polars_query_artifact_relative_nested` | Nested artifact-relative SQL | `shared/sql2/customers.sql` remains unchanged and resolves below the artifact root without a manifest |
| `local_polars_query_explicit_multiple_roots` | Explicit SQL routing | repeated `--sql-base-path` values route a prefixed reference through explicit roots |
| `local_polars_query_root_routing` | Component SQL root | shorthand `queries/orders.sql` resolves below `sql_base_path` |
| `local_polars_state_base_path` | Runtime root derivation | `state_base_path` produces `logs/` and `watermarks/` below the supplied root |
| `local_polars_runtime_log_contract` | Snapshot execution logging | v4 `message`, `source_query`, executed `source_action.query`, `job_id`, and `run_attributes` |
| `local_polars_logging_batch` | Batch execution logging | immutable `.json` part plus replace-one JobRuntime snapshot |
| `local_polars_dry_run_query_file` | Preparation-only execution | query file is resolved; no business output or watermark is written |
| `local_polars_startup_failure` | Fail-fast startup | malformed provider input emits startup lifecycle diagnostics before dataflow execution |

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

## Replay qualification pair

### `local_polars_replay` / `local_spark_replay`

These paired scenarios qualify the same bounded replay contract on both local
engines. Each run cleans its destination and isolated runtime state, executes
three sequential `[start, end)` chunks over `modified_at`, persists the source
observation after successful chunks, and validates six expected rows plus the
saved `2024-01-17T12:30:00` observation. The scenario receipt is a fresh
bounded replay because setup cleans its destination and isolated runtime state
before each invocation. Same-range rerun and replacement behavior are covered
by local regression tests and are not claimed by this pair.

| Field | Value |
|---|---|
| metadata | `metadata/file/local_use_cases.json` |
| stage | `replay_test` |
| replay range | `[2024-01-15T00:00:00, 2024-01-18T00:00:00)` |
| chunking | `modified_at`, `days=1` |
| persistence | `replay_save_watermark: true` |
| outputs | `.runtime/data/output/delta/orders_replay` plus isolated state under `.runtime/data/replay_validation/{polars,spark}` |
| assertions | `scripts/validate_replay_output.py` |
| Docker | optional for Spark dispatch; the case definition remains local-engine qualification |

---

### `local_polars_date_mtime_replay` / `local_spark_date_mtime_replay`

These profiles qualify the source-owned behavior for a date-partitioned file
source when replay selects by the internal file modification-time column. The
fixture puts one file exactly on the inclusive lower boundary, one file inside
the range, one file exactly on the exclusive upper boundary, and one older file
in another date folder. The expected output is therefore IDs `1,2`; date-folder
discovery limits the scan, while mtime decides which files are read.

| Field | Value |
|---|---|
| metadata | generated under `.runtime/data/replay_date_mtime_validation_{polars\|spark}.json` |
| stage | `date_mtime_replay_validation` |
| replay range | `[2024-01-15T00:00:00+00:00, 2024-01-17T00:00:00+00:00)` |
| chunking | `__file_modification_time`, `days=1` |
| persistence | `replay_save_watermark: true` |
| assertions | `scripts/validate_date_mtime_replay_output.py` checks IDs, boundary selection and saved mtime state |
| Docker | optional for Polars; Docker Spark is used when the Spark container is running |

The range is owned by `FileReader`: folder boundaries remain inclusive for
discovery and the exact file choice uses `[start,end)` mtime filtering.

---

### `local_polars_api_replay` / `local_spark_api_replay`

These paired scenarios qualify API bounded replay through the canonical
`range_param_mapping` contract. The API request selects `modified_at`, while
the source persists only `order_date`; this proves that the selected replay
field can be independent of the authored watermark columns. The mock API
implements the half-open bounds and the validator checks six rows plus the
final saved `order_date=2024-01-17` observation.

| Field | Value |
|---|---|
| metadata | `metadata/file/replay_api_validation.json` |
| stage | `replay_api_validation` |
| replay range | `[2024-01-15T00:00:00, 2024-01-18T00:00:00)` |
| chunking | `modified_at`, `days=1` |
| persisted watermark | `order_date` only |
| destination | Delta `api_replay_validation` |
| assertions | `scripts/validate_replay_output.py` validates rows, selected IDs, range and saved state |
| services | `mock-api` |
| Docker | required when the selected profile starts the service through Compose; Spark may use host fallback when its container is absent |

The current qualification receipt is PASS for both IDs. Polars ran on the
local runtime; Spark ran through the Docker Spark 3.5.9/Delta 3.3.3 profile.
Both runs made three API requests/chunks, selected six rows and persisted only
the authored `order_date=2024-01-17`. A prior host Spark 4.1 fallback attempt
failed during JVM heartbeat recovery and is not used as evidence for this pair.

---

### `local_{polars|spark}_api_recovery_fail` → `api_recovery` → `api_continuation`

This ordered trio qualifies the normal incremental API pagination boundary on
the Docker `mock-api` service. The first profile caps `next_link` pagination at
one page and must fail before destination or watermark commit. The recovery
profile removes the cap and persists all 30 fixture rows plus
`modified_at=2024-02-01T09:30:00`. The continuation profile adds one late row
through the same endpoint and reuses the saved state; it appends only ID `1028`
and advances the state to `2024-02-02T09:30:00`.

Run each engine in this order so the state and Delta output are shared by the
three profiles:

```text
python usecase-sim/runner/run_scenario.py --scenario local_polars_api_recovery_fail
python usecase-sim/runner/run_scenario.py --scenario local_polars_api_recovery
python usecase-sim/runner/run_scenario.py --scenario local_polars_api_continuation

python usecase-sim/runner/run_scenario.py --scenario local_spark_api_recovery_fail
python usecase-sim/runner/run_scenario.py --scenario local_spark_api_recovery
python usecase-sim/runner/run_scenario.py --scenario local_spark_api_continuation
```

| Field | Value |
|---|---|
| metadata | Polars uses `metadata/file/api_incremental_recovery_{fail,continuation}.json` plus `api_incremental_recovery.json`; Spark uses the corresponding `_spark` metadata files to isolate its output base |
| source | `mock-api` `/api/orders/next-link` with `modified_since` pushdown |
| pagination | opaque same-origin `next_link`, `limit=5`; failure uses `max_pages=1` |
| destination | Delta `api_incremental_recovery_validation` |
| state | `usecase-sim/.runtime/data/replay_validation/api_recovery_{polars,spark}` |
| assertions | failure exit `2` and no commit; recovery `30` rows; continuation `31` rows with ID `1028` once |
| Docker | `mock-api`; Spark uses the existing `datacoolie-spark` container |

Qualification receipt: both Polars and Docker Spark passed all three ordered
profiles. The receipt covers local/mock API pagination and incremental state
continuation; it does not claim behavior for a provider-specific production API
until that endpoint's contract is qualified.

---

### `local_polars_sql_replay` / `local_spark_sql_replay`

These paired scenarios qualify SQL predicate pushdown when the replay selection
column is independent of the persisted watermark list. The SQLite source stores
the generated 30-row sample locally; each run pushes a half-open
`modified_at` window into SQL and persists only the authored `order_date` key.

| Field | Value |
|---|---|
| metadata | `metadata/file/replay_sql_validation.json` |
| source | SQLite `orders` table from `.runtime/data/input/sqlite/orders.db` |
| replay range | `[2024-01-15T00:00:00, 2024-01-18T00:00:00)` |
| chunking | `modified_at`, `days=1` |
| persisted watermark | `order_date` only |
| destination | Delta `sql_replay_validation` |
| assertions | validator checks six rows, three chunks, range and saved `order_date=2024-01-17` |
| Docker | Polars local; Spark uses the Docker Spark 3.5.9/Delta 3.3.3 profile when available |

The current qualification receipt is PASS for both IDs. Exact Decimal scalar
comparison remains a source-code/local test concern; this SQLite fixture keeps
the generated amount representation as text and is not used to claim Decimal
precision parity.

---

## Datatype qualification pair

### `local_polars_datatype_qualification` / `local_spark_datatype_qualification`

These paired scenarios run the same eight dataflows from one canonical
metadata contract. The setup script creates a run-scoped fixture under
`.runtime/data/datatype_qualification`, then materializes only the physical
destination binding for each engine. Alongside the decimal/date and weak CSV
regressions, one matrix dataflow exercises the mapped scalar families for each
Spark SQL, PostgreSQL, MySQL, SQL Server, Oracle, and SQLite convention. The
scenario runner only checks execution exit status. Host integration tests own
persisted schema/value assertions and cross-engine comparison.

| Field | Value |
|---|---|
| canonical metadata | `metadata/file/datatype_qualification.json` |
| generated metadata | `.runtime/data/datatype_qualification/metadata/{engine}_{format}.json` |
| stage | `datatype_qualification` |
| priority | P1 |
| setup | `scripts/prepare_datatype_qualification.py` |
| assertions | `tests/integration/data_types/test_usecase_sim_cross_engine.py` |
| output roots | `.runtime/data/datatype_qualification/output/{format}/{polars,spark}` |

## P1 — Docker-backed metadata + lakehouse maintenance

### `local_polars_database`

| Field | Value |
|---|---|
| engine | polars |
| metadata_type | database |
| platform | local |
| metadata_db_connection_string | `sqlite:///./usecase-sim/.runtime/databases/metadata/datacoolie_metadata.db` |
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
| metadata_db_connection_string | `sqlite:///./usecase-sim/.runtime/databases/metadata/datacoolie_metadata.db` |
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

### Qualification receipt — 2026-09-29

These profiles were run with the existing Compose services `minio`, `postgres`,
`iceberg-rest`, `datacoolie-spark` and `mock-api`. The runner constructs
`AWSPlatform` with `endpoint_url=http://minio:9000`; therefore the receipt is for
the AWS-compatible S3/REST profile backed by MinIO. It must not be read as proof
of parity with real AWS S3, Glue or other AWS services.

- `aws_polars_file`: **82 total / 78 succeeded / 4 skipped / 0 failed**.
- `aws_spark_file`: **82 total / 66 succeeded / 16 skipped / 0 failed** in the
  Docker Spark 3.5.9/Delta 3.3.3 profile.
- Direct MinIO file-read smoke: **5/5** dataflows for Polars and **5/5** for
  Spark.
- `aws_polars_delta_maintenance` and `aws_spark_delta_maintenance`: **13/13**
  each.
- `aws_polars_iceberg_maintenance` and `aws_spark_iceberg_maintenance`:
  **11/11** each. Polars reported the expected compaction capability skip and
  a REST snapshot-expiry 500 handled by the capability path; Spark emitted a
  transient BlockManager heartbeat warning but completed.
- `aws_polars_replay` and `aws_spark_replay`: **1/1** dataflow each, three
  sequential chunks, six selected rows in `[2024-01-15T00:00:00,
  2024-01-18T00:00:00)`, and persisted `order_date=2024-01-17`. The source
  selects by independent `modified_at`; output and S3 state are validated
  through MinIO.

The first AWS file run exposed a simulator fixture hint using `DATETIME`, which
Spark SQL does not accept in this contract. The fixture was corrected to
`timestamp_ntz`, uploaded again, and the full profiles passed. This is a fixture
correction, not a replay implementation failure. The exact date-folder/mtime
and Iceberg replay/replacement gates are covered below. Real AWS execution
remains a separate optional deployment gate.

### `aws_polars_replay` and `aws_spark_replay`

| Field | Value |
|---|---|
| engine | polars / spark |
| metadata_type | file |
| platform | aws (`AWSPlatform` + MinIO endpoint) |
| metadata_path | engine-specific generated object: `s3://datacoolie-test/metadata/replay_aws_validation_{polars|spark}.json` |
| stage | `aws_replay_validation` |
| range | `[2024-01-15T00:00:00, 2024-01-18T00:00:00)` in three `days=1` chunks |
| selection column | `modified_at` |
| persisted watermark | authored `order_date=2024-01-17` |
| destination | S3-backed Delta `aws_replay_validation_{polars|spark}` (unique per case) |
| Docker needs | `minio` and, for Spark, the existing `datacoolie-spark` container |

The setup script reseeds the MinIO fixture, uploads the replay-only metadata,
and removes only the scenario's output/state prefixes. The validator checks
selected IDs, half-open bounds, and persisted state; a successful process exit
alone is not sufficient.

### `aws_polars_iceberg_replay` and `aws_spark_iceberg_replay`

These profiles use a setup-created Iceberg source table in the REST catalog and
an Iceberg `merge_overwrite` destination with `replace_by_watermark`. Each
profile executes three `[2024-01-15, 2024-01-18)` chunks, then the validator
executes the same replay range a second time. The destination must still contain
the six selected IDs after the second pass, proving that the Iceberg window is
replaced rather than appended again. The saved authored watermark is
`order_date=2024-01-17`.

| Field | Value |
|---|---|
| platform | `aws` (`AWSPlatform` + MinIO endpoint) |
| source | setup-created `default.replay_iceberg_source` |
| destination | engine-specific REST Iceberg table `default.iceberg_replay_validation_{polars\|spark}` |
| selection column | `order_date` |
| load | `merge_overwrite` + `replace_by_watermark: true` |
| assertions | `scripts/validate_iceberg_replay_output.py` checks rows, snapshots, rerun replacement and S3 state |
| Docker | `minio` + `iceberg-rest`; Spark uses `datacoolie-spark` |

This is an AWS-compatible MinIO/REST qualification. It does not claim parity
with real AWS S3, Glue, or Athena.

### `aws_polars_file`

| Field | Value |
|---|---|
| engine | polars |
| metadata_type | file |
| platform | aws |
| metadata_path | `s3://datacoolie-test/metadata/aws_use_cases.json` |
| stage | `""` |
| priority | P2 |
| Docker needs | `minio` + `iceberg-rest` |

### `aws_spark_file`

| Field | Value |
|---|---|
| engine | spark |
| metadata_type | file |
| platform | aws |
| metadata_path | `s3://datacoolie-test/metadata/aws_use_cases.json` |
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
| metadata_path | `s3://datacoolie-test/metadata/aws_use_cases.json` |
| connection | `aws_delta_dest` |
| priority | P2 |
| Docker needs | `minio` |

### `aws_polars_iceberg_maintenance`

| Field | Value |
|---|---|
| engine | polars |
| metadata_type | maintenance |
| platform | aws |
| metadata_path | `s3://datacoolie-test/metadata/aws_use_cases.json` |
| connection | `aws_iceberg_dest` |
| priority | P2 |
| Docker needs | `minio` + `iceberg-rest` |

### `aws_spark_delta_maintenance`

| Field | Value |
|---|---|
| engine | spark |
| metadata_type | maintenance |
| platform | aws |
| metadata_path | `s3://datacoolie-test/metadata/aws_use_cases.json` |
| connection | `aws_delta_dest` |
| priority | P2 |
| Docker needs | `minio` |

### `aws_spark_iceberg_maintenance`

| Field | Value |
|---|---|
| engine | spark |
| metadata_type | maintenance |
| platform | aws |
| metadata_path | `s3://datacoolie-test/metadata/aws_use_cases.json` |
| connection | `aws_iceberg_dest` |
| priority | P2 |
| Docker needs | `minio` + `iceberg-rest` |

---

## Updating this file

Scenarios are added or modified in [scenarios.json](scenarios.json). The
registry currently contains 61 entries; when the JSON changes, update the
matching entry here by hand so each row can carry human-written notes the raw
JSON cannot.
