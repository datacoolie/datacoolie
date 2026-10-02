---
title: Testing Strategy — DataCoolie Project
description: Understand DataCoolie testing layers, local validation patterns, coverage expectations, and how to keep pipelines safe to change.
---

# Testing strategy

DataCoolie uses plain `pytest` with `pytest-xdist`. The default repo behavior
is defined in `pyproject.toml`, not in a separate CLI wrapper.

## Default test run

```powershell
# From datacoolie/
python -m pytest
```

Use the [shared contributor setup](contributing.md#standard-local-environment)
and install `dev` plus the extras for your changed area before running tests.
Run this from the product root. It picks up the current default
pytest options from `pyproject.toml`:

- `-n auto`
- `--dist loadgroup`
- `-m "not spark"`
- `--strict-markers`
- `--tb=short`
- `-q`
- `--import-mode=importlib`

So the local default is an `xdist` run with automatic worker selection and
parallel **non-spark** test execution. It does not execute Spark-marked tests
unless you override the marker selection explicitly.

GitHub Actions synchronizes the locked development, documentation, and selected
capability profiles, then calls `scripts/verify_release.py`. That release gate
runs the non-Spark package suite and the release-contract suite serially with
`-n 0`; it does not use the local `-n auto` default. A restored virtual
environment cache never replaces `poetry sync`.

The packaging contract is covered separately so a normal test run does not
silently miss a renamed or incomplete extra:

```powershell
python -m pytest -c pyproject.toml scripts/tests/test_optional_dependencies.py -q -n 0
poetry check --lock
```

That test compares the PEP 621 and Poetry dependency declarations and asserts
that `all` is exactly the union of the capability profiles.

When diagnosing a skip-heavy collection, add `-rs` so pytest prints the skip
reasons. The datatype and runtime gates add skips during collection until their
matching command-line option is present. Individual tests also use
`pytest.importorskip(...)` for optional packages such as Polars, PySpark, and
database drivers; a missing import is therefore a skipped test, not a passed
qualification. A selected Docker-backed test calls the local service probe and
fails clearly when its required service is missing or unhealthy; pytest does
not start that service for you.

## Test ownership

The suites have deliberately separate owners:

- `tests/` tests the `src/datacoolie` package and owns only its fixtures and
  shared harnesses.
- `scripts/tests/` verifies release and packaging contracts.
- `ai/skills/tests/` verifies AI Skills and build-schema contracts.
- `usecase-sim/` is an executable scenario runner. It owns preparation and
  framework execution; selected feature-specific validation hooks remain
  runner-owned, while datatype parity assertions live in
  `tests/integration/data_types/`. It has no pytest suite of its own and is not
  collected by the core pytest configuration.

## Markers

| Marker | Description |
|---|---|
| default selection | `pytest` runs with `-m "not spark"`, so all non-spark tests are included by default. |
| `slow` | Defined marker. Still included by default unless you exclude it yourself. |
| `integration` | Defined marker. Still included by default unless you exclude it yourself. |
| `cloud_integration` | Tests against real cloud resources. Always skipped unless `--cloud-integration` is supplied. |
| `benchmark` | Opt-in performance benchmark. Requires `--run-benchmarks`; cloud benchmarks also require `--cloud-integration`. |
| `cloud_platform(name)` | Identifies the test platform as `fabric`, `aws`, or `databricks`; filter with `--cloud-platform`. |
| `datatype_qualification` | Source, engine and persisted-format datatype qualification. Always skipped unless `--datatype-qualification` is supplied. |
| `runtime_qualification` | Attempt-local window, replacement and persisted-format runtime qualification. Always skipped unless `--runtime-qualification` is supplied. |
| `spark` | Spark-specific tests. Excluded by default by the repo pytest config. When run directly, the Spark module also uses `pytest.importorskip(...)` for `pyspark` and `delta-spark`. |

```powershell
python -m pytest -m "not slow and not integration and not spark"
```

## Datatype qualification

Datatype qualification is a separate opt-in gate because later cases use Docker
databases, Spark, object storage and table catalogs. The default test command
does not start services or open those resources. The gate uses an independently
authored contract and has the following bounded qualification cells:

- PostgreSQL → Polars extraction, including `int8`, exact `NUMERIC(18,2)`,
  instant timestamps and wall-clock timestamps;
- Polars and Spark native decimal/`timestamp_ntz` casts;
- SQLite and PostgreSQL URI extraction plus precision-preserving MySQL, MSSQL
  and Oracle Docker extraction before schema-hint casting;
- Polars Parquet, Delta and Iceberg REST/MinIO round trips for a bounded
  decimal/temporal fixture;
- Spark ↔ Polars Parquet reads in both directions for decimal, instant
  timestamp and `timestamp_ntz`, using Spark's explicit `TIMESTAMP_MICROS`
  output contract.
- The opt-in usecase-sim parity test runs one scalar matrix dataflow for each
  mapped source convention (Spark SQL, PostgreSQL, MySQL, SQL Server, Oracle,
  and SQLite), then compares the same metadata and independent expected
  contract for Parquet, Delta, and Iceberg. This is source-convention and
  engine/output qualification; it does not replace live database extraction
  cells.
- `usecase-sim/metadata/file/datatype_qualification.json` is the checked-in
  logical contract. Qualification setup materializes disposable execution
  metadata under `.runtime/data/datatype_qualification/`; the integration test
  uses an additional run-scoped `<run-id>/metadata/` directory so engine and
  format output bindings cannot collide. These generated files are not a
  second metadata source of truth.
- Spark 3.5.9 ↔ Polars Delta and Iceberg reads in both directions through the
  checked-in `datacoolie-spark` container. The test-owned container worker uses
  Delta 3.3.3, Iceberg 1.10.1 and the REST/MinIO catalog cell; host pytest
  assertions own the result. The usecase-sim matrix reads Parquet datasets with
  PyArrow, Delta tables with `deltalake.DeltaTable`, and Iceberg tables through
  the REST catalog; it must not fall back to reading Delta's underlying files.
- Spark 4.1.0 ↔ Polars Delta and Iceberg reads in both directions are an
  additional opt-in coordinate. The `datacoolie-spark4` service uses Delta
  4.2.0 and `iceberg-spark-runtime-4.1_2.13:1.11.0`; its mutable JVM state is
  isolated under `.runtime/spark4/`.

These are bounded qualification cells, not a claim for every connector,
runtime or format version. The previously recorded Docker qualification passed the
usecase-sim Spark/Polars matrix for Parquet, Delta and Iceberg, plus the
native Spark-container Delta/Iceberg read-back cell. The exact
`decimal(18,2)` upper-bound value is still blocked by the installed
`deltalake 1.5.1` writer. Spark 4.1.0 is qualified only as the pinned
coordinate above; other Spark 4.x versions and connector combinations remain
separate unverified cells. Do not infer those claims from the inventory above.

```powershell
# The command does not start Docker; prepare selected usecase-sim services first.
poetry run pytest tests/integration/data_types `
  --datatype-qualification -m datatype_qualification -n 0 -q

# Select the PostgreSQL extraction cells while the runtime matrix is built.
poetry install --with dev -E source-db-polars -E metadata-db
poetry run python -m pip install psycopg2-binary
poetry run python usecase-sim/scripts/setup_platform.py --services postgres
poetry run pytest tests/integration/data_types/test_postgresql_extraction.py `
  --datatype-qualification -m "integration and datatype_qualification" -n 0 -q -rs

# The MySQL and MSSQL tests use the native DB-API readers but also create their
# fixtures through SQLAlchemy. The Oracle test uses the separate oracledb
# driver.
poetry install --with dev -E source-db-native-polars -E source-db-oracle-polars -E metadata-db
poetry run python usecase-sim/scripts/setup_platform.py --services mysql mssql
poetry run pytest tests/integration/data_types/test_mysql_mssql_extraction.py `
  --datatype-qualification -m "integration and datatype_qualification" -n 0 -q -rs
poetry run python usecase-sim/scripts/setup_platform.py --services oracle
poetry run pytest tests/integration/data_types/test_oracle_extraction.py `
  --datatype-qualification -m "integration and datatype_qualification" -n 0 -q -rs

# Run exactly the local Spark cast contract (the project default excludes
# Spark). The explicit marker keeps this cell separate from non-Spark tests.
poetry install --with dev -E spark-delta
poetry run pytest `
  tests/integration/data_types/test_engine_casts.py::test_spark_matches_shared_decimal_and_ntz_contract `
  --datatype-qualification -m "spark and datatype_qualification" -n 0 -q -rs

# Run the pinned Spark 3.5.9 container cross-engine Delta/Iceberg cell.
poetry run python usecase-sim/scripts/setup_platform.py --services minio iceberg-rest spark
poetry run pytest tests/integration/data_types/test_spark_container_cross_engine.py `
  --datatype-qualification -m datatype_qualification -n 0 -q

# Run the opt-in Spark 4.1.0 + Delta 4.2.0 coordinate.
docker compose -f usecase-sim/docker/docker-compose.yml build spark4
poetry run python usecase-sim/scripts/setup_platform.py --services minio iceberg-rest spark4
poetry run pytest tests/integration/data_types/test_spark4_container_cross_engine.py `
  --datatype-qualification --spark4-qualification `
  -m "spark4_qualification and datatype_qualification" -n 0 -q

# Run the usecase-sim six-source scalar matrix through both engines and all
# persisted formats.
poetry run pytest tests/integration/data_types/test_usecase_sim_cross_engine.py `
  --datatype-qualification -m datatype_qualification -n 0 -q
```

The PostgreSQL tests create fixtures through SQLAlchemy/psycopg2 and read
through ConnectorX; that fixture driver is separate from the source profile.
The host Spark cast cell also requires a compatible Java runtime; follow the
[Spark prerequisites](../guide/getting-started/installation.md#spark-prerequisites).

[The usecase-sim datatype qualification instructions](https://github.com/datacoolie/datacoolie/blob/main/usecase-sim/README.md#datatype-cross-engine-qualification)
describe the service layout and the container coordinates used by these cells.
The qualification harness does not call
`setup_metadata.py --truncate`, stop containers, or remove Docker volumes. A
selected service that is missing or unhealthy fails the explicit qualification
run; an unselected runtime cell remains unverified.

## Runtime replacement qualification

Runtime qualification is intentionally separate from datatype qualification.
It verifies the execution-local window contract, replacement safety and
persisted target observations without enabling Docker or Spark during normal
collection. The checked-in `usecase-sim/metadata/file/` tree remains the
scenario source of truth; generated logs, watermarks and data stay under
`usecase-sim/.runtime/`.

```powershell
# Contract checks only; no services are started by pytest.
python -m pytest tests/integration/runtime `
  --runtime-qualification -m runtime_qualification -n 0 -q

# Prepare simulator services separately before any native qualification cells.
python usecase-sim/scripts/setup_platform.py
```

Native Spark/Polars Delta and Iceberg persistence comparisons are an explicit
local gate, not part of the default CI suite. A missing runtime or service is
reported as an unverified qualification coordinate; it is never represented as
a passing mock result.

## Real-cloud integration tests

Real-cloud tests are explicitly opt-in and use resource coordinates from a
local env file. Credentials are deliberately excluded: authentication remains
with each SDK's normal chain, such as `DefaultAzureCredential`, boto3, or a
Databricks CLI/workload identity.

1. Copy `.env.integration.example` to `.env.integration.local`.
2. Fill only the platform coordinates you intend to test.
3. Authenticate with the platform's normal CLI, identity, or CI mechanism.
4. Select both the opt-in flag and platform.

```powershell
# Fabric / OneLake contract tests (benchmarks remain skipped)
python -m pytest tests/integration/platforms/fabric `
  --cloud-integration --cloud-platform fabric

# Databricks UC Volume contract tests; native-only checks skip on a laptop
python -m pytest tests/integration/platforms/databricks `
  --cloud-integration --cloud-platform databricks

# Use a non-default file. Relative paths resolve from the repository root.
python -m pytest tests/integration `
  --cloud-integration --cloud-platform aws `
  --integration-env-file .env.integration.aws.local
```

The AWS live contract also covers S3-compatible storage. Leave
`DATACOOLIE_AWS_ENDPOINT_URL` empty for AWS S3, or set it to a local MinIO
endpoint such as `http://localhost:9000`; the bucket must already exist. Keep
MinIO access keys in the normal boto3 environment/profile chain, not in the
committed integration file. For the existing local simulator, start its MinIO
service before running the AWS integration command.

The opt-in AWS benchmark uses a UUID-scoped tree of at least 1,024 small
objects and measures recursive/non-recursive listing, full reads, managed
download, append, and copy. Set `DATACOOLIE_AWS_LIST_BENCHMARK_ROOT` to a
relative root such as `metadata`; it is resolved below
`DATACOOLIE_AWS_TEST_PREFIX`. The benchmark verifies path sets on every run
and removes its UUID prefix in `finally`:

```powershell
python -m pytest tests/integration/platforms/aws/test_s3_benchmark.py `
  --cloud-integration --run-benchmarks --cloud-platform aws -s
```

The benchmark output includes the boto3 version, region, endpoint kind, run
count, p50/p95 samples, and failures. Compare runs on the same bucket,
region, machine, and tree shape; do not use one warm-cache sample as a
regression decision.

Fabric and Databricks benchmarks use the same explicit gate:

```powershell
python -m pytest tests/integration/platforms/fabric/test_notebookutils_benchmark.py `
  --cloud-integration --run-benchmarks --cloud-platform fabric -s

python -m pytest tests/integration/platforms/databricks/test_listing_benchmark.py `
  --cloud-integration --run-benchmarks --cloud-platform databricks -s
```

The LocalPlatform benchmark is filesystem-only and does not require cloud
coordinates. It creates a temporary 8,192-file tree, checks identical path
sets against a `pathlib` baseline, and reports p50/p95 listing times:

```powershell
python -m pytest tests/integration/platforms/local/test_listing_benchmark.py `
  --run-benchmarks -m benchmark -n 0 -s
```

`--run-benchmarks` is required even when `--cloud-integration` is already
present, so a broad contract command cannot start benchmark workloads by
accident. This also keeps the local benchmark out of ordinary integration and
default test runs. The `benchmark` marker can be combined with normal pytest
selection, for example `-m "benchmark"`.

`.env.integration.local` is ignored by Git. Values already present in the
process environment take precedence over the env file, which lets CI inject
resource coordinates without rewriting files. Missing variables skip only the
affected platform's tests. Each mutating test must create a UUID-scoped child
under its configured test root and remove only that child during cleanup.
The env file accepts comments, optional `export`, and plain or quoted
`KEY=VALUE` entries; shell expansion is intentionally not performed.

The Fabric test accepts either an explicit
`DATACOOLIE_FABRIC_ONELAKE_TEST_URI` or the workspace and Lakehouse GUID pair;
the latter is converted to an `abfss://` OneLake URI. The notebook benchmark
uses `DATACOOLIE_FABRIC_LIST_BENCHMARK_ROOT`, normally `Files`, because it runs
inside Fabric through `notebookutils`.

Databricks tests use `DATACOOLIE_DATABRICKS_HOST`, catalog, schema, volume,
and a test prefix to build `/Volumes/<catalog>/<schema>/<volume>/<prefix>`.
`DATACOOLIE_DATABRICKS_LIST_BENCHMARK_ROOT` is a root relative to that Volume,
for example `metadata`; the resolver expands it to
`/Volumes/<catalog>/<schema>/<volume>/metadata`. Existing fully qualified
`/Volumes/...` values remain accepted.
Authentication remains in a Databricks CLI profile or workload identity. The
external suite injects `WorkspaceClient(host=...)`; the native suite runs only
inside a Databricks notebook/job with `dbutils`. Optional secret scope/key and
cluster ID values are coordinates, not credentials. Recursive-listing
benchmarks warm each route, measure five repetitions for worker caps `1`, `4`,
`8`, and `16`, and report p50/p95, file/directory counts, directory-list calls,
failures, and throttling. They assert identical file sets before timing results
are considered. The native benchmark compares the POSIX `/Volumes` strategy
with the `dbutils.fs.ls` baseline; the serverless gate passed and mounted-Volume
listing now defaults to POSIX, with `dbutils` retained as the explicit
baseline/fallback. Recursive delete benchmarks use three repetitions of a
UUID-scoped tree for sequential and bounded worker caps before a default
changes.

## Coverage

The current `pyproject.toml` does **not** configure `pytest-cov`, branch
coverage, omissions, or a repository-wide failure threshold. Run coverage
explicitly when needed, and do not treat a normal `python -m pytest` result as
a coverage gate.

## Parallel execution contract

`pytest-xdist` distributes by **test group** (`--dist loadgroup`). Tests
that share fixtures use the `@pytest.mark.xdist_group(...)` marker to pin into
the same worker. The current Spark engine module is grouped this way so one JVM
is reused safely.

Spark remains a local-only release gate because JVM and Delta startup are too
expensive for the hosted CI job:

```powershell
poetry sync --with dev -E spark-delta
poetry run pytest tests/unit/engines/test_spark_engine.py -m spark -n auto --dist loadgroup
```

## Scope

The main source-package test surface is the pytest suite under `tests/`.
Release/package and AI Skills contracts are invoked by their owning gates.
Separately, `usecase-sim/` provides coarse-grained execution scenarios and
runner hooks for end-to-end execution. Datatype persisted-output assertions are
owned by the opt-in integration tests; no pytest suite is maintained inside the
separate simulator testbed.
