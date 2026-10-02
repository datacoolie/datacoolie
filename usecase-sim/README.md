# usecase-sim — DataCoolie Testbed & Scenarios

End-to-end integration testbed and executable demo for the `datacoolie` ETL framework.
Exercises the local `{polars, spark}` × `{file, database, api}` matrix,
Polars qualified SQL over Delta/Iceberg, selected AWS-platform file scenarios,
and lakehouse maintenance
(compact / cleanup). A companion `docker-compose` stack provides realistic
backends (Postgres/MySQL/MSSQL/Oracle, MinIO, Iceberg REST, Trino, a mock REST
API, and a metadata REST API).

> Examples use PowerShell syntax. Bash users: drop the leading `.\` on venv
> paths and swap `\` for `/` in file arguments. All commands assume the working
> directory is the standalone repository root.

---

## 1. What is usecase-sim?

- **Library under test:** the `datacoolie` package in `src/datacoolie/`.
- **What it exercises:** 51 named scenarios covering 2 engines, 3 metadata
  sources, 2 storage platforms, and lakehouse maintenance. The set is
  representative; it is not a complete engine × source × platform cross-product.
- **Why it exists:** one-command regression coverage for the selected ETL
  behaviours, plus a teaching surface for new contributors.

---

## 2. Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | ≥ 3.11, < 4.0 | Matches package metadata |
| Virtualenv | — | Recommended at the active checkout root as `.venv` |
| `datacoolie` extras | `[all]` recommended | Installed editable from repo root |
| Docker Desktop | required for the checked-in full scenario suite | Provides external stores/APIs and the recommended Windows Spark runtime |
| Java 17 | for host Spark | Not needed when Spark runs in the provided container |

Install into a fresh checkout-root venv:

```powershell
# From the checkout root
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[all]"
```

---

## 3. Directory map

```
usecase-sim/
├── scripts/       # Bootstrap, reset, fixture generation, and output validation
├── runner/        # ETL runners (run.py, maintenance.py) + dispatcher (run_scenario.py)
├── scenarios/     # scenarios.json (multi entries) + SCENARIOS.md (field reference)
├── metadata/
│   ├── file/      # JSON use-case files (local, aws, perf) + generated yaml/xlsx
│   ├── database/  # DDL per dialect + verify_metadata.py
│   └── api/       # Standalone dev metadata server (reads JSON)
├── docker/        # docker-compose.yml, mock_api_server.py, pg_api_metadata_server.py
├── functions/     # Custom Python source functions (sources.py)
├── artifacts/     # Checked-in deployed-artifact fixtures used by artifact scenarios
├── .runtime/      # Generated data, logs, watermarks, and databases (gitignored)
└── platforms/
    ├── aws/          # Glue/local AWS samples; dispatcher integration remains separate
    ├── databricks/  # Prepared metadata + notebook samples
    └── fabric/      # Prepared metadata + notebook samples
```

---

## 4. Full start

The simplest reliable path: bring up the entire stack, seed every metadata
target, generate every input dataset, and run every scenario. Takes a few
minutes cold but has no "is service X up?" guesswork.

```powershell
# From the repository root

# 1. Start all 11 services and wait for exposed ports (≈ 60 s cold, 10 s warm)
python usecase-sim/scripts/setup_platform.py

# 2. Seed metadata into file + every DB dialect + api-db
python usecase-sim/scripts/setup_metadata.py `
    --targets file,db:sqlite,db:postgresql,db:mysql,db:mssql,db:oracle `
    --truncate

# 3. Generate the 30-row sample set plus focused transformer fixture
python usecase-sim/scripts/generate_data.py

# 4. Run every scenario
python usecase-sim/runner/run_scenario.py --all
```

**Expected result** (≈ a few minutes, depending on cold/warm JVM):

```
Total: 51 | PASS: 51 | FAIL: 0
```

Or, for a first sanity check after step 3:

```powershell
python usecase-sim/runner/run_scenario.py --scenario local_polars_file
# Result — total: <selected>, succeeded: <selected>, failed: 0, skipped: 0
# Total: 1 | PASS: 1 | FAIL: 0
```

Outputs land in `usecase-sim/.runtime/data/output/` (Delta tables, Parquet, JSON, …)
and watermarks in `usecase-sim/.runtime/watermarks/`.

Checked-in metadata/provider source remains under `usecase-sim/metadata/`.
Artifact-backed scenarios consume immutable fixtures from `usecase-sim/artifacts/`;
generated data, logs, and watermarks remain isolated under `.runtime/`.

Tear everything down:

```powershell
python usecase-sim/scripts/setup_platform.py --down --volumes
```

### Running only what you need

Once the full path works, you can trim by looking at the **selected dataflows**,
not only the scenario priority. Most file/database/API scenarios use
`stage: ""`, so they execute the broad `local_use_cases` metadata set and can
touch database sources, the mock API, MinIO, and Iceberg in one run. Starting
the full stack is the reliable default.

Narrow-stage replay scenarios and connection-filtered maintenance scenarios
need only the backends their selected dataflows touch. Iceberg maintenance
needs `minio` + `iceberg-rest`; API metadata needs `postgres` +
`metadata-api`. Spark scenarios can run on a host JVM, but on Windows the
dispatcher uses the running `spark` container when available.

`run.py` builds an Iceberg REST catalog for Polars only when the selected stage
name contains `iceberg` or the empty stage selects all dataflows. Focused
non-Iceberg stages do not require `iceberg-rest`; Iceberg stages still require
`iceberg-rest` and `minio` with the default local catalog. Spark follows the
same stage gate for its Iceberg catalog configuration, extension, and JARs.

> `setup_metadata.py` with no args defaults to `file,db:sqlite,db:postgresql`
> and therefore needs the `postgres` container. Narrow `--targets` when
> running it standalone against a smaller stack.

---

## 5. Running scenarios

```mermaid
flowchart LR
    A[scenarios.json] -->|--scenario / --priority| B[run_scenario.py]
    B -->|metadata_type == maintenance| C[maintenance.py]
    B -->|otherwise| D[run.py]
    D --> E[DataCoolieDriver.run]
    C --> F[DataCoolieDriver.run_maintenance]
```

### Invocation modes

```powershell
# Single scenario
python usecase-sim/runner/run_scenario.py --scenario local_polars_file

# All scenarios at a given priority
python usecase-sim/runner/run_scenario.py --priority P0

# Everything (long; P1/P2 need Docker)
python usecase-sim/runner/run_scenario.py --all
```

The MinIO target also publishes `metadata/file/aws_use_cases.json` to
`s3://datacoolie-test/metadata/aws_use_cases.json`, which is the metadata URI
used by the AWS-platform scenarios.

Artifact-backed scenarios are run with the same dispatcher as every other
framework scenario. The dispatcher receives explicit metadata, artifact, SQL,
and runtime roots. CLI lifecycle tests live under `tests/unit/cli` and
`tests/integration/cli`.

### Datatype cross-engine qualification

The normal scenario suite is representative and does not claim a complete
source × engine × format matrix. For the bounded persisted-format cell, start
the Spark, MinIO and Iceberg REST services, then run the opt-in integration
test from the product root:

```powershell
python usecase-sim/scripts/setup_platform.py --services minio iceberg-rest spark
..\.venv\Scripts\python.exe -m pytest --datatype-qualification `
  -m datatype_qualification tests/integration/data_types/test_spark_container_cross_engine.py

# Run the full six-source scalar matrix through both engines and all formats.
..\.venv\Scripts\python.exe -m pytest --datatype-qualification `
  -m datatype_qualification tests/integration/data_types/test_usecase_sim_cross_engine.py
```

The test-owned container worker reports its resolved coordinates and structured
observations (Spark 3.5.9, Delta 3.3.3, Iceberg 1.10.1, `deltalake` 1.6.3,
PyIceberg 0.12.0). Host assertions compare both Spark↔Polars directions and
the cleanup receipt. The full matrix observer reads Parquet through PyArrow,
Delta through `deltalake.DeltaTable`, and Iceberg through the REST catalog;
reading raw Parquet files under a Delta directory is not considered Delta
qualification. This is evidence for that coordinate, not a blanket Spark 4.x
or boundary-value compatibility claim.

Spark 4.1 is a separate opt-in coordinate. Build and start the isolated
service beside the default Spark 3.5 service, then run its dedicated gate:

```powershell
docker compose -f usecase-sim/docker/docker-compose.yml build spark4
python usecase-sim/scripts/setup_platform.py --services minio iceberg-rest spark4
..\.venv\Scripts\python.exe -m pytest `
  tests/integration/data_types/test_spark4_container_cross_engine.py `
  --datatype-qualification --spark4-qualification `
  -m "spark4_qualification and datatype_qualification" -n 0 -q
```

The `spark4` image pins PySpark 4.1.0, Delta Lake 4.2.0 and resolves
`iceberg-spark-runtime-4.1_2.13:1.11.0`. Its mutable state lives below
`.runtime/spark4/`, so it can run next to `datacoolie-spark`. A scenario may
select it with `"spark_container": "datacoolie-spark4"`; the default remains
`datacoolie-spark`. A passing gate is evidence only for this pinned coordinate.

#### Qualification metadata locations

These paths have different ownership and lifecycles:

| Path | Role | Used by |
|---|---|---|
| `metadata/file/datatype_qualification.json` | Checked-in canonical logical metadata contract | Fixture preparation and contract-coverage checks |
| `.runtime/data/datatype_qualification/metadata/{engine}_{format}.json` | Generated execution metadata for manually invoked named scenarios | `run_scenario.py --scenario local_*_datatype_qualification*` |
| `.runtime/data/datatype_qualification/runs/<run-id>/metadata/{engine}_{format}.json` | Run-isolated execution metadata with unique output/table bindings | `tests/integration/data_types/test_usecase_sim_cross_engine.py` |

The generated files are materialized from the canonical contract. They only
change physical execution bindings such as output root, format, catalog/table
name and run suffix; the source, transform and datatype declarations remain
the same. The `.runtime/` copies are disposable and must not become a second
source of truth.

`usecase-sim/` is verified through its executable runner and scenario exit
status. It intentionally has no pytest suite; test-owned integration checks
under the core repository boundary invoke it as a black box.

### Dispatcher behaviour

| Concern | Handling |
|---|---|
| Spark JVM cooldown | 6 s pause + stale `.runtime/spark/warehouse/` + `.runtime/spark/metastore_db/` wipe between host Spark scenarios; container profiles use isolated state roots |
| Timeouts | 450 s (spark), 300 s (polars), 600 s (maintenance); override per scenario via `"timeout_seconds"` |
| Cancellation | On timeout, signal the child, allow 120 s for `driver.close()` and log flush, then hard-kill if needed |
| Exit code | `0` iff every scenario passed |
| Explicit services | `services` entries are started before setup for either engine |
| Scenario setup | Optional repository-local script runs before the ETL child and has a separate log |
| Engine setup | Optional repository-local function runs in the ETL process after engine construction |

### Direct runner invocation (advanced)

When debugging one metadata row, bypass the dispatcher and call `run.py`
directly — all flags documented in section 7.

```powershell
python usecase-sim/runner/run.py `
    --engine polars --metadata-source file --platform local `
    --metadata-path usecase-sim/metadata/file/local_use_cases.json `
    --stage local_csv2parquet
```

---

## 6. Scenarios reference

Full field-level reference: [scenarios/SCENARIOS.md](scenarios/SCENARIOS.md).

### P0 — fast regression set (not necessarily Docker-free)

| Key | Engine | Source | Notes |
|---|---|---|---|
| `local_polars_file` | polars | file (JSON) | all stages including `transform_filter` |
| `local_polars_file_yaml` | polars | file (YAML) | |
| `local_polars_file_excel` | polars | file (XLSX) | |
| `local_spark_file` | spark | file (JSON) | api-source dataflows skipped |
| `local_polars_replay` | polars | file (JSON) | three sequential half-open chunks; validates output and saved source watermark |
| `local_spark_replay` | spark | file (JSON) | same replay contract via Spark; api-source dataflows skipped |
| `local_polars_api_replay` | polars | ranged API + mock-api | independent `modified_at` selection; persists authored `order_date` only |
| `local_spark_api_replay` | spark | ranged API + mock-api | same independent API replay contract through the Spark profile |
| `local_polars_sql_replay` | polars | SQLite | SQL-pushed independent `modified_at` range; persists authored `order_date` only |
| `local_spark_sql_replay` | spark | SQLite | same SQL replay contract through the Spark profile |
| `local_polars_transform_features` | polars | focused file metadata | validates transformer output values and schema |
| `local_spark_transform_features` | spark | focused file metadata | same assertions as Polars |
| `local_polars_transform_dedup_strict` | polars | focused file metadata | expected failure for missing dedup order column |
| `local_spark_transform_dedup_strict` | spark | focused file metadata | same strict failure assertion as Polars |
| `local_{polars,spark}_transform_invalid_fill` | both | focused file metadata | expected failure for an incompatible typed fill literal |
| `local_{polars,spark}_transform_invalid_redact` | both | focused file metadata | expected failure for an incompatible typed redact literal |
| `local_{polars,spark}_transform_sanitizer_collision` | both | focused file metadata | expected failure for colliding sanitized names |
| `local_polars_datatype_qualification` | polars | typed/weak file metadata matrix | executes the decimal/CSV checks plus one scalar matrix for each Spark SQL, PostgreSQL, MySQL, SQL Server, Oracle, and SQLite convention; parity assertions live in the opt-in integration test |
| `local_spark_datatype_qualification` | spark | typed/weak file metadata matrix | executes the same eight dataflows in the simulator Spark container when available (otherwise the documented local fallback); parity assertions live in the opt-in integration test |
| `local_polars_qualified_sql_delta` | polars | focused file metadata | 4/3/2/1 names, filters, lazy reuse |
| `local_polars_qualified_sql_delta_ambiguity` | polars | focused file metadata | expected ambiguity failure |
| `local_polars_artifact_default_metadata` | polars | static artifact | Driver-inferred FileProvider, default metadata root, artifact SQL |
| `local_polars_artifact_custom_metadata` | polars | static artifact | explicit nested metadata root with arbitrary shard names |
| `local_polars_query_artifact_relative` | polars | static artifact | shorthand SQL resolved directly below the artifact root |
| `local_polars_query_artifact_relative_nested` | polars | static artifact | nested artifact-relative SQL path remains unchanged without a manifest |
| `local_polars_query_explicit_multiple_roots` | polars | exact file + SQL roots | repeated SQL root flags route a prefixed reference through explicit roots |
| `local_polars_query_root_routing` | polars | exact file + SQL root | shorthand SQL resolved from an explicit `sql_base_path` (including its root prefix) |
| `local_polars_state_base_path` | polars | exact file + SQL root | derived logs and file-watermark state below a state root |
| `local_polars_runtime_log_contract` | polars | static artifact | v4 runtime `message` plus metadata/runtime query fields and `run_attributes` |
| `local_polars_logging_batch` | polars | static artifact | immutable batch parts plus replace-one job snapshot |
| `local_polars_dry_run_query_file` | polars | exact file + SQL root | preparation-only query-file validation without business I/O |
| `local_polars_startup_failure` | polars | malformed exact file | provider startup failure diagnostics before dataflow execution |

The transformer feature scenarios require the local fixture generated by
`python usecase-sim/scripts/generate_data.py --targets local`. Positive
scenarios run an output validator after the ETL child exits; negative scenarios
assert both the expected exit code and stable error text.

The datatype qualification scenarios prepare their own deterministic fixture
with `prepare_datatype_qualification.py`.  The Parquet input contains one
typed scalar matrix per supported source convention, while the CSV input keeps
the weak-input/leading-zero regression.  Polars and Spark write below separate
`.runtime/data/datatype_qualification/output/<format>/<engine>` roots,
and return the framework process result. They are focused simulator execution
cases; persisted schema/value assertions and cross-engine comparison belong to
the opt-in pytest gate documented in `docs/project/testing.md`.

The positive validator reconciles 25 unique outputs (75 total rows), including
literal regex replacement, equal-order declaration stability, value rules
before schema casting, portable SHA-256 and signed XXHash64 output, and partial
masking of null and empty strings.

Each usecase-sim dataflow represents one primary case or feature and writes to
its own output. A dataflow may use other features as supporting setup when they
are needed to exercise that primary behavior. Do not turn supporting features
into additional assertions or let them obscure what the dataflow is testing;
give independently important behavior its own dataflow. Multiple operations
may also form the primary case when their interaction is what is being tested,
such as value-rule ordering.

### P1 — needs Docker services

| Key | Engine | Source | Requires |
|---|---|---|---|
| `local_polars_database` | polars | database (SQLite) | — |
| `local_polars_database_postgres` | polars | database | postgres |
| `local_polars_database_mysql` | polars | database | mysql |
| `local_polars_database_mssql` | polars | database | mssql |
| `local_polars_database_oracle` | polars | database | oracle |
| `local_spark_database` | spark | database (SQLite) | — |
| `local_polars_api` | polars | api | metadata-api |
| `local_spark_api` | spark | api | metadata-api |
| `local_polars_delta_maintenance` | polars | maintenance | — |
| `local_polars_iceberg_maintenance` | polars | maintenance | minio + iceberg-rest |
| `local_spark_delta_maintenance` | spark | maintenance | — |
| `local_spark_iceberg_maintenance` | spark | maintenance | minio + iceberg-rest |
| `local_{polars,spark}_api_recovery_fail` | both | mock-api next_link | expected pagination cap failure; no target/state commit |
| `local_{polars,spark}_api_recovery` | both | mock-api next_link | recovery after failed pagination; saves 30-row observed watermark |
| `local_{polars,spark}_api_continuation` | both | mock-api next_link | second run selects one late row from persisted state |
| `local_polars_qualified_sql_iceberg` | polars | focused file metadata | minio + iceberg-rest |
| `local_polars_qualified_sql_iceberg_ambiguity` | polars | focused file metadata | minio + iceberg-rest |

### P2 — AWS platform (MinIO + Iceberg REST)

| Key | Engine | Source | Requires |
|---|---|---|---|
| `aws_polars_file` | polars | file | minio + iceberg-rest |
| `aws_spark_file` | spark | file | minio + iceberg-rest |
| `aws_polars_delta_maintenance` | polars | maintenance | minio |
| `aws_polars_iceberg_maintenance` | polars | maintenance | minio + iceberg-rest |
| `aws_spark_delta_maintenance` | spark | maintenance | minio |
| `aws_spark_iceberg_maintenance` | spark | maintenance | minio + iceberg-rest |
| `aws_polars_replay` | polars | file replay | minio |
| `aws_spark_replay` | spark | file replay | minio + datacoolie-spark |
| `aws_polars_iceberg_replay` | polars | Iceberg replay/replacement | minio + iceberg-rest |
| `aws_spark_iceberg_replay` | spark | Iceberg replay/replacement | minio + iceberg-rest + datacoolie-spark |

> P0 is a priority label, not an isolation guarantee. Scenarios with
> `stage: ""` can touch the broad metadata set. `local_polars_file` and
> `local_polars_file_yaml` additionally need `mock-api` on port 8082 for their
> API-source dataflows; the `_excel` and `_spark_file` variants set
> `skip_api_sources: true` and therefore avoid that dependency.

---

## 7. The runners

Two runner scripts replace what used to be eight. They are thin shells over
`DataCoolieDriver` from the library.

### `runner/run.py`

Unified ETL runner. Dispatches any `(engine × metadata-source)` combination.

| Flag | Req | Default | Purpose |
|---|---|---|---|
| `--engine` | ✓ | — | `polars` \| `spark` |
| `--metadata-source` | ✓ | — | `file` \| `database` \| `api` |
| `--platform` | | `local` | `local` \| `aws` (chooses `LocalPlatform` or `AWSPlatform`) |
| `--metadata-path` | file | — | `.json` \| `.yaml` \| `.xlsx` |
| `--metadata-base-path` | file | `None` | Directory of section-wrapped metadata documents; Driver creates the FileProvider |
| `--artifact-base-path` | file | `None` | Deployed artifact root; metadata defaults to `<root>/metadata` |
| `--sql-base-path` | | `None` | Repeatable root(s) for shorthand SQL file references; multiple roots use their folder prefixes |
| `--metadata-db-connection-string` | db | — | SQLAlchemy URL |
| `--metadata-api-url` | api | — | Base URL of metadata API |
| `--metadata-api-key` | | `""` | Optional API key |
| `--metadata-workspace-id` | db/api | — | Workspace ID |
| `--stage` | ✓ | — | Stage name(s); `""` runs all stages |
| `--column-name-mode` | | `lower` | `lower` \| `snake` |
| `--dry-run` | | off | Load/select metadata only; skip reads, transforms, writes, and watermarks |
| `--storage-options KEY=VALUE` | | `[]` | Repeatable; passed to Polars / object store |
| `--iceberg-catalog-uri` | | `None` | Override Iceberg REST URI |
| `--catalog-preset` | | `local` | `local` \| `unity_catalog` |
| `--uc-token` / `--uc-credential` | | `""` | Unity Catalog auth |
| `--log-path` | | `None` | Directory for framework logs; driver writes `system_logs/` and `execution_logs/` under it |
| `--state-base-path` | | `None` | Runtime state root; derives log and file-watermark paths when component roots are absent |
| `--job-id` | | generated | Stable Driver session/job identifier (useful for deterministic scenario logs) |
| `--run-attributes JSON` | | `None` | Caller-owned JSON object persisted with JobRuntime for external correlation |
| `--log-persistence-mode` | | `None` | `snapshot` or `batch` structured log persistence |
| `--log-flush-interval-seconds` | | `None` | Periodic batch flush interval override |
| `--log-flush-batch-bytes` | | `None` | Batch size threshold override |
| `--log-console-color` | | `None` | Console color policy: `auto` \| `always` \| `never` |
| `--max-workers` | | `None` | Parallel dataflow workers |
| `--skip-api-sources` | | off | Skip dataflows with `connection_type=api` |
| `--engine-setup-function` | | `None` | Usecase-local callable invoked with the active engine before metadata execution |
| `--engine-setup-arg` | | `[]` | Repeatable argument forwarded to the engine-setup callable |
| `--app-name` | spark | `DataCoolie-UseCase` | Spark app name |
| `--spark-config KEY=VALUE` | spark | `[]` | Extra Spark configs |
| `--replay-start` | | `None` | Inclusive replay range start (ISO date/datetime or int); activates replay mode |
| `--replay-end` | | `None` | Exclusive replay range end |
| `--replay-chunk-interval KEY=VALUE` | | `[]` | Repeatable; e.g. `days=1`. Empty = single-shot replay |
| `--replay-save-watermark` | | off | Persist source watermark observation after each successful chunk; requested replay chunks always rerun |
| `--replay-chunk-column` | | `None` | Override auto-resolved chunk column |

When replay bounds come from this CLI, an unambiguous signed integer such as
`0` or `-10` is converted to an integer before `ReplayConfig` validation. Other
strings remain ISO date/datetime candidates. This keeps integer `step` replay
usable through the command line without changing the framework's direct API
normalization rules.

### `runner/maintenance.py`

Compact + cleanup for Delta and Iceberg tables.

| Flag | Req | Default | Purpose |
|---|---|---|---|
| `--engine` | ✓ | — | `polars` \| `spark` |
| `--platform` | | `local` | `local` \| `aws` |
| `--metadata-path` | one of roots | — | Exact metadata file |
| `--metadata-base-path` | one of roots | `None` | Directory of section-wrapped metadata documents |
| `--artifact-base-path` | one of roots | `None` | Deployed artifact root; metadata defaults to `<root>/metadata` |
| `--sql-base-path` | | `None` | Repeatable root(s) for shorthand SQL references; multiple roots use their folder prefixes |
| `--connection` | | `None` | Filter to a single connection name |
| `--do-compact` / `--no-compact` | | on | Enable/disable compaction |
| `--do-cleanup` / `--no-cleanup` | | on | Enable/disable cleanup |
| `--retention-hours` | | `168` | File retention window |
| `--dry-run` | | off | Accepted and passed into run config, but current `run_maintenance()` does not enforce it; do not use as a safety switch |
| `--storage-options KEY=VALUE` | | `[]` | Repeatable |
| `--catalog-preset` | | `local` | `local` \| `unity_catalog` |
| `--iceberg-catalog-uri` | | `None` | Override Iceberg REST URI |
| `--uc-token` / `--uc-credential` | | `""` | Unity Catalog auth |
| `--log-path` | | `None` | Directory for framework logs (same layout as `run.py`) |
| `--state-base-path` | | `None` | Runtime state root for derived logs/watermarks |
| `--job-id` | | generated | Stable Driver session/job identifier |
| `--run-attributes JSON` | | `None` | Caller-owned correlation object |
| `--log-persistence-mode` | | `None` | `snapshot` or `batch` |
| `--skip-api-sources` | | off | Skip api-source dataflows |
| `--app-name` | spark | `DataCoolie-Maintenance` | Spark app name |
| `--spark-config KEY=VALUE` | spark | `[]` | Extra Spark configs |

### `runner/run_scenario.py`

| Flag | Default | Purpose |
|---|---|---|
| `--scenario NAME` | — | Single scenario key |
| `--all` | off | Run every scenario |
| `--priority P0\|P1\|P2` | — | Run every scenario at a priority tier |
| `--scenarios-path PATH` | `scenarios/scenarios.json` | Override scenarios file |

The dispatcher writes three kinds of logs under `usecase-sim/.runtime/logs/`:

- `system_logs/` and `execution_logs/` — driver output (forwarded via `--log-path`).
- `scenarios/run_scenario.log` — dispatcher's own log (which scenarios ran, commands, pass/fail summary).
- `scenarios/<name>.console.log` — full stdout+stderr tee of each scenario's child process (also streamed live to the terminal).

On graceful cancellation the child handles `SIGINT`, `SIGTERM`, or Windows
`SIGBREAK`, closes the driver to flush framework logs, and exits. The
dispatcher still reports a timeout as exit code `124`; after the 120-second
grace period it hard-kills a child that has not exited. AWS scenarios send
framework logs to `s3://datacoolie-test/logs`.

### `runner/_runner_utils.py`

Shared factory module. Key exports:

- `build_spark_session(...)` — picks Scala 2.12 / 2.13, injects Delta + Iceberg + S3A + JDBC drivers.
- `build_iceberg_rest_catalog(...)` — builds a `pyiceberg` REST catalog for Polars.
- `setup_platform(is_aws, storage_opts, logger)` — returns `LocalPlatform` or `AWSPlatform`.
- `run_and_report(driver, stage, ...)` — runs the driver, logs the result, optionally stops Spark.
- `replay_and_report(driver, stage, ..., replay_start, replay_end, replay_chunk_interval, ...)` — runs `driver.run_replay()` with a `ReplayConfig` built from the supplied arguments.

Two catalog presets are supported; `unity_catalog` has two deployment/auth
forms:

| Preset | `--iceberg-catalog-uri` default | Auth |
|---|---|---|
| `local` | `http://localhost:8181` (tabulario/iceberg-rest) | none |
| `unity_catalog` (OSS) | `http://<host>:8080/api/2.1/unity-catalog/iceberg` | `--uc-credential client_id:secret` |
| `unity_catalog` (Databricks) | `https://<ws>.azuredatabricks.net/api/2.1/unity-catalog/iceberg` | `--uc-token <pat>` |

---

## 8. Bootstrap scripts

All scripts live in [`scripts/`](scripts/) and import shared helpers from
`scripts/_common.py`. Run from the `datacoolie/` directory.

### `setup_platform.py` — start / stop Docker stack

```powershell
python usecase-sim/scripts/setup_platform.py                         # up all, wait for ports
python usecase-sim/scripts/setup_platform.py --services postgres minio
python usecase-sim/scripts/setup_platform.py --down --volumes        # tear down + wipe data
```

| Flag | Default | Purpose |
|---|---|---|
| `--down` | off | Stop the stack (`docker compose down`) |
| `--volumes` | off | With `--down`, also remove named volumes |
| `--services s1 s2 …` | all | Bring up/down a subset |
| `--timeout SECONDS` | `180` | Port-readiness poll timeout |
| `--no-wait` | off | Skip readiness polling |

### `setup_metadata.py` — fan-out use-cases to targets

```powershell
# Default: file + db:sqlite + db:postgresql (local workspace)
python usecase-sim/scripts/setup_metadata.py

# Seed every dialect, truncating first
python usecase-sim/scripts/setup_metadata.py `
    --targets file,db:postgresql,db:mysql,db:mssql,db:oracle --truncate

# AWS workspace into postgres
python usecase-sim/scripts/setup_metadata.py `
    --json usecase-sim/metadata/file/aws_use_cases.json `
    --workspace-id aws-workspace --targets db:postgresql --truncate
```

| Flag | Default | Purpose |
|---|---|---|
| `--json PATH` | `local_use_cases.json` | Source JSON file |
| `--workspace-id STR` | `local-workspace` | Workspace ID written into DB rows |
| `--targets LIST` | `file,db:sqlite,db:postgresql` | Comma-separated targets |
| `--truncate` | off | Truncate tables before seeding |
| `--db-url DIALECT=URL` | `[]` | Override the default SQLAlchemy URL per dialect |

Targets: `file`, `db:sqlite`, `db:postgresql`, `db:mysql`, `db:mssql`,
`db:oracle`, `api-db` (alias for `db:postgresql`).

### `generate_data.py` — write sample inputs

30-row sample dataset into every requested target. Replaces the old
`generate_sample_data.py` + `bootstrap_{postgres,mysql,mssql,oracle,minio}.py`.

| Flag | Default | Purpose |
|---|---|---|
| `--targets LIST` | all reachable | `local`, `minio`, `pg`, `mysql`, `mssql`, `oracle` |

```powershell
python usecase-sim/scripts/generate_data.py --targets local,minio
python usecase-sim/scripts/generate_data.py --targets pg,mysql
```

### `reset_watermarks.py` — delete watermarks only

| Flag | Default | Purpose |
|---|---|---|
| `--dry-run` | off | Report without deleting |
| `--skip-local` / `--skip-minio` / `--skip-db` | off | Skip individual stores |
| `--dialects D1 D2 …` | all 5 | DB dialects to target |
| `--db-url DIALECT=URL` | `[]` | Override SQLAlchemy URL |

### `reset_data.py` — full reset

Wipes `.runtime/data/output/`, drops MinIO output + iceberg-warehouse prefixes, drops
the Iceberg `default` namespace, and calls `reset_watermarks.py`.

| Flag | Default | Purpose |
|---|---|---|
| `--dry-run` | off | Report without deleting |
| `--local-only` | off | Skip MinIO + Iceberg + DB cleanup |
| `--dialects D1 D2 …` | all 5 | DB dialects for watermark cleanup |

### `generate_perf_data.py` — perf benchmark inputs

| Flag | Default | Purpose |
|---|---|---|
| `--sizes LIST` | all 8 | `10k,50k,100k,500k,1m,5m,10m,50m` |
| `--formats LIST` | all | `jsonl,parquet,delta,iceberg` |
| `--targets LIST` | all | `local,minio,iceberg` |
| `--iceberg-uri URL` | `http://localhost:8181` | Iceberg REST URI |

Notes:

- JSONL inputs are only generated through `1m` rows. The script skips JSONL for `5m`, `10m`, and `50m`.
- `minio` is skipped automatically when `localhost:9000` is unavailable.
- `iceberg` requires the local REST catalog and is skipped when the catalog is unavailable.

### `reset_perf_data.py` — reset perf artifacts

| Flag | Default | Purpose |
|---|---|---|
| `--all` | off | Also reset inputs (`data/perf/input/` + `perf_src` namespace) |
| `--dry-run` | off | Report without deleting |

---

## 9. Docker stack (P1 / P2)

`usecase-sim/docker/docker-compose.yml` defines a 12-service stack (the
Spark 4.1 service is opt-in). All
credentials are hardcoded — intended for local dev only.

| Service | Port(s) | Credentials | Purpose |
|---|---|---|---|
| `postgres` | 5432 | `datacoolie/datacoolie` | Metadata DB + Iceberg JDBC catalog |
| `mysql` | 3306 | `datacoolie/datacoolie` | Metadata DB variant |
| `mssql` | 1433 | `sa / Datacoolie@1` | Metadata DB variant |
| `oracle` | 1521 | `datacoolie/datacoolie`, service `FREEPDB1` | Metadata DB variant |
| `minio` | 9000 / 9001 | `minioadmin/minioadmin` | S3-compatible object store |
| `iceberg-rest` | 8181 | — | Tabular Iceberg REST catalog |
| `trino` | 8080 | — | SQL engine over Iceberg |
| `mock-api` | 8082 | env-configured | Simulates REST API sources |
| `metadata-api` | 8000 | via `DATABASE_URL` | Flask API over postgres metadata |
| `sqlpad` | 3000 | `admin@datacoolie.local / admin` | Web SQL editor |
| `spark` | — | — | Pinned Spark 3.5 container; avoids Windows Spark issues |
| `spark4` | — | — | Opt-in Spark 4.1.0 + Delta 4.2.0 qualification container |

### UI endpoints

- MinIO console: <http://localhost:9001>
- Trino UI: <http://localhost:8080>
- SQLPad: <http://localhost:3000>
- Mock REST API: <http://localhost:8082>
- Metadata REST API: <http://localhost:8000>

### Bringing up only what you need

```powershell
# P1 database scenario on postgres only
python usecase-sim/scripts/setup_platform.py --services postgres

# P2 AWS iceberg scenario
python usecase-sim/scripts/setup_platform.py --services minio iceberg-rest

# P1 metadata-api scenario (postgres + api server)
python usecase-sim/scripts/setup_platform.py --services postgres metadata-api
```

### Windows: running Spark via Docker (recommended)

Running PySpark natively on Windows is fragile (JVM temp-dir cleanup,
`winutils.exe`, Python-path resolution). The `spark` Docker service solves
this by running Spark in `local[*]` mode inside a Linux container. Input and
output data are shared through a volume mount — no path changes needed.

**Start the service** (same pattern as any other service):

```powershell
# From the datacoolie/ directory — build image + start dependencies + spark
python usecase-sim/scripts/setup_platform.py --services minio iceberg-rest spark
```

On first start the container runs `pip install -e /datacoolie --no-deps`
automatically via its entrypoint (all deps are pre-baked into the image, so
this is fast). No separate setup script needed.

**Generate input data on the host (once per reset):**

```powershell
python usecase-sim/scripts/generate_data.py --targets local
```

**Run any Spark scenario via `docker exec`:**

```powershell
# Single stage
docker exec datacoolie-spark python usecase-sim/runner/run.py `
  --engine spark --metadata-source file --platform local `
  --metadata-path ./usecase-sim/metadata/file/local_use_cases.json `
  --stage load_delta --column-name-mode lower --skip-api-sources

# All stages
docker exec datacoolie-spark python usecase-sim/runner/run.py `
  --engine spark --metadata-source file --platform local `
  --metadata-path ./usecase-sim/metadata/file/local_use_cases.json `
  --stage "" --column-name-mode lower --skip-api-sources

# Interactive shell for debugging
docker exec -it datacoolie-spark bash
```

For Spark 4.1, start the `spark4` service and replace the container name with
`datacoolie-spark4`. The scenario dispatcher accepts the same selection via
the `spark_container` scenario field.

All relative paths (`./usecase-sim/.runtime/data/...`) resolve correctly because the
container's `WORKDIR` is `/datacoolie` (the mounted package directory).
Output tables written inside the container appear on the host immediately.

For local runs, the runner disables Hadoop checksum verification only on the
local filesystem instance. This is required because Spark and non-Hadoop
writers share the same generated Delta fixtures; AWS/S3 checksum behavior is
unchanged. The three broad Spark scenarios reset only stateless generated Delta
targets before each run so transaction history does not grow without bound.
Watermark-dependent targets remain intact. The runner rejects pre-clean targets
outside `usecase-sim/.runtime/data`.

| What works | Notes |
|---|---|
| P0 file scenarios (Delta, Parquet, CSV, Iceberg) | ✅ Full support — minio + iceberg-rest reached by container name |
| P0 replay scenarios | ✅ |
| P1 database scenarios | ⚠ `localhost` in metadata JSON resolves to the container, not the host. Use `docker exec ... --metadata-source database` only after verifying connectivity. |
| P1 API scenarios | ⚠ Same localhost caveat; pass `--skip-api-sources` to avoid. |

### MSSQL URL encoding

The SQL Server password `Datacoolie@1` must be URL-encoded in SQLAlchemy URLs
as `Datacoolie%401`. The default URL in `scenarios.json` already handles this.

---

## 10. Metadata sources

Three interchangeable ways of providing the same connection + dataflow + schema
hint definitions.

### File

Primary sources: [local_use_cases.json](metadata/file/local_use_cases.json),
[aws_use_cases.json](metadata/file/aws_use_cases.json),
[perf_test.json](metadata/file/perf_test.json). Running
`setup_metadata.py --targets file` emits YAML and XLSX siblings used by the
`*_yaml` / `*_excel` scenarios.

### Database

Schema files per dialect: `metadata/database/schema.sql` (SQLite),
`schema_postgres.sql`, `schema_mysql.sql`, `schema_mssql.sql`,
`schema_oracle.sql`. All create the same four `dc_framework_*` tables.
Seeded by `setup_metadata.py --targets db:<dialect>`.

For an existing database, run the reviewed additive
[`metadata/database/migrations/`](metadata/database/migrations/) script before
deploying a runtime that reads `source_filter_expression`; `create_tables()`
and the seeder do not upgrade an existing table.

Oracle setup is safe to repeat. To reset only the simulator's local metadata
workspace and then prove the non-truncating path is idempotent:

```powershell
python usecase-sim/scripts/setup_metadata.py --targets db:oracle --truncate
python usecase-sim/scripts/setup_metadata.py --targets db:oracle
```

Setup now fails fast if a required framework table or expected seeded metadata
ID is missing.

Verify DB matches JSON source:

```powershell
python usecase-sim/metadata/database/verify_metadata.py `
    --json-path usecase-sim/metadata/file/local_use_cases.json `
    --connection-string "postgresql+psycopg2://datacoolie:datacoolie@localhost:5432/datacoolie" `
    --workspace-id local-workspace
```

### REST API

- **Containerized (recommended):** `docker/pg_api_metadata_server.py`, running
  in the `metadata-api` service on port 8000. Reads from postgres.
- **Standalone dev:** `metadata/api/api_metadata_server.py` — reads a JSON file
  directly; useful when you don't want Docker.

Both serve the same REST contract consumed by `APIProvider`.

---

## 11. Custom Python sources

[`functions/sources.py`](functions/sources.py) hosts Python functions that are
callable from metadata via `source.python_function` dotted paths. Current
function:

- `sql_query_orders(engine, source, watermark)` — queries Delta tables via SQL;
  registers tables for Polars, queries the metastore for Spark. Supports Local,
  AWS, Fabric, and Databricks platforms via `base_path` lookups.

[`metadata/file/polars_qualified_sql.json`](metadata/file/polars_qualified_sql.json)
contains only normal Delta/Iceberg sources with portable SQL declared as
`source.query`. It has no Python-function source or test-only source
configuration. The scenario's same-process engine setup registers the fixture
relations before DeltaReader or IcebergReader executes each query. Every
dataflow owns one case: qualified name level, include/exclude selection, reuse,
or ambiguity. Run the local cases with:

```powershell
# Delta only; no Docker service required
python usecase-sim/runner/run_scenario.py --scenario local_polars_qualified_sql_delta

# Iceberg REST + MinIO are started from the scenario's explicit service list
python usecase-sim/runner/run_scenario.py --scenario local_polars_qualified_sql_iceberg
```

The repository recommendation `pip install -e ".[all]"` includes everything
needed. A minimal Delta-only environment needs `.[polars-sql,polars-delta]`;
add `polars-iceberg` for the Iceberg cases.

To add your own, define a function in `functions/sources.py` and reference it
from the metadata JSON:

```json
{
  "source": {
    "type": "python_function",
    "python_function": "sources.my_custom_reader"
  }
}
```

---

## 12. Maintenance runs

Maintenance scenarios call `DataCoolieDriver.run_maintenance(...)` to compact
(optimize file layout) and cleanup (vacuum expired files) Delta and Iceberg
tables listed in the metadata.

```powershell
python usecase-sim/runner/run_scenario.py --scenario local_polars_delta_maintenance
python usecase-sim/runner/run_scenario.py --scenario local_spark_iceberg_maintenance
```

Or directly:

```powershell
python usecase-sim/runner/maintenance.py `
    --engine polars --platform local `
    --metadata-path usecase-sim/metadata/file/local_use_cases.json `
    --connection local_delta_dest `
    --retention-hours 168
```

The driver deduplicates maintenance targets by physical destination within one
run. Separate scheduler invocations can still race on the same Delta path;
serialise those external jobs or use distinct targets.

---

## 13. Perf benchmarks

```powershell
# Generate full benchmark inputs.
# JSONL is only generated through 1m; parquet/delta/iceberg go through 50m.
python usecase-sim/scripts/generate_perf_data.py

# Run each engine from a clean output state for a fair comparison.
python usecase-sim/runner/run_perf_benchmark.py --engine polars --max-size 50m --reset
python usecase-sim/runner/run_perf_benchmark.py --engine spark  --max-size 50m --reset

# Regenerate the final merged report from the two JSON result files.
python usecase-sim/runner/run_perf_benchmark.py --report-only
```

Notes:

- Run all benchmark commands from the repository root.
- `--reset` calls `usecase-sim/scripts/reset_perf_data.py` and clears perf outputs only. It does not delete generated inputs or `.runtime/data/perf/benchmark_results/`.
- Each engine run writes one JSON file to `.runtime/data/perf/benchmark_results/` and also regenerates `perf_report.md` from whatever result files already exist.
- `--report-only` is the clean way to rebuild the final comparison after both engine runs finish.
- If you want to regenerate inputs as well, use `python usecase-sim/scripts/reset_perf_data.py --all` before `generate_perf_data.py`.
- If Docker-backed services are unavailable, use `--no-iceberg` and keep `--max-size 1m` because JSONL inputs stop at `1m`.

---

## 14. Troubleshooting

| Symptom | Fix |
|---|---|
| `ERROR DerbyLockFile` on second Spark run | `run_scenario.py` auto-wipes `.runtime/spark/warehouse/` + `.runtime/spark/metastore_db/`; if running `run.py` directly, delete them between runs |
| Local Spark raises `ChecksumException` for a shared Delta file | Run through `runner/run.py` or `run_scenario.py`; local runners apply the scoped Hadoop checksum policy after creating Spark. Direct framework callers keep checksum verification enabled. |
| Broad Spark scenarios slow down after many reruns | Use `run_scenario.py`; each affected scenario resets its stateless generated Delta targets while preserving watermark-dependent state. |
| `BucketAlreadyOwnedByYou` on MinIO | Benign; `_common.ensure_bucket` is idempotent |
| Database reader import error | Install `pip install "datacoolie[source-db-native-polars]"` for the default MySQL/MSSQL path, or `datacoolie[source-db-oracle-polars]` for Oracle. Use `database_read_engine: "connectorx"` only as an explicit fallback. |
| Oracle setup reports a missing `DC_FRAMEWORK_*` table or metadata ID | Rerun `setup_metadata.py --targets db:oracle`; use `--truncate` only when you intentionally want to reset `local-workspace`. The setup log now includes the full failing traceback. |
| MSSQL auth fails with `Datacoolie@1` | Use URL-encoded form `Datacoolie%401` in SQLAlchemy URLs |
| MSSQL `Login failed for user 'sa'` with state 38 | The `datacoolie` user database is missing — the MSSQL image has no auto-create env var. `setup_platform.py` creates it after the container is up; to do it manually: `docker exec datacoolie-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Datacoolie@1' -No -Q "IF DB_ID('datacoolie') IS NULL CREATE DATABASE datacoolie;"` |
| API-source dataflows fail in `local_polars_file` | `mock-api` container is down; either `setup_platform.py --services mock-api` or use `--skip-api-sources` |
| Iceberg scenarios fail with connection refused on `:8181` | `iceberg-rest` container is down: `setup_platform.py --services iceberg-rest` |
| `json.decoder.JSONDecodeError` loading scenarios.json | File likely corrupted; reseed from git and reapply edits |

---

## 15. Platform-specific assets

The central `scenarios.json` dispatcher covers local and AWS-compatible
scenarios. Native Fabric and Databricks execution remains notebook-based and
is not integrated into that dispatcher. AWS has separate Glue/local sample
scripts under `platforms/aws/`.

Fabric assets:

- [`platforms/fabric/README.md`](platforms/fabric/README.md)
- [`platforms/fabric/fabric_use_cases.json`](platforms/fabric/fabric_use_cases.json)
- [`platforms/fabric/sample_fabric_spark.ipynb`](platforms/fabric/sample_fabric_spark.ipynb)
- [`platforms/fabric/sample_fabric_polars.ipynb`](platforms/fabric/sample_fabric_polars.ipynb)

Databricks assets:

- [`platforms/databricks/README.md`](platforms/databricks/README.md)
- [`platforms/databricks/databricks_use_cases.json`](platforms/databricks/databricks_use_cases.json)
- [`platforms/databricks/sample_databricks_spark.ipynb`](platforms/databricks/sample_databricks_spark.ipynb)
- [`platforms/databricks/sample_databricks_polars.ipynb`](platforms/databricks/sample_databricks_polars.ipynb)

AWS assets:

- [`platforms/aws/README.md`](platforms/aws/README.md)
- [`platforms/aws/aws_glue_use_cases.json`](platforms/aws/aws_glue_use_cases.json)
- [`platforms/aws/sample_aws_glue_spark.py`](platforms/aws/sample_aws_glue_spark.py)
- [`platforms/aws/sample_aws_local_polars.py`](platforms/aws/sample_aws_local_polars.py)

Deferred status details:

- [`platforms/databricks/DEFERRED.md`](platforms/databricks/DEFERRED.md)
- [`platforms/fabric/DEFERRED.md`](platforms/fabric/DEFERRED.md)

---

## License

AGPL-3.0-or-later — same as the parent `datacoolie` package. See [../LICENSE](../LICENSE).
