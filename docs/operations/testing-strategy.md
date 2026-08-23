---
title: Testing Strategy — DataCoolie Operations
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

This picks up the current default pytest options from `pyproject.toml`:

- `-n auto`
- `--dist loadgroup`
- `-m "not spark"`
- `--strict-markers`
- `--tb=short`
- `-q`
- `--import-mode=importlib`

So the default run is a parallel **non-spark** test run. It does not execute
the Spark-marked tests unless you override the marker selection explicitly.
GitHub Actions runs this non-Spark suite as a required job after synchronizing
the locked `polars-delta`, `polars-hash`, and `polars-sql` extras. A restored virtual
environment cache never replaces `poetry sync`.

The packaging contract is covered separately so a normal test run does not
silently miss a renamed or incomplete extra:

```powershell
python -m pytest tests/unit/test_optional_dependencies.py -q -n 0
poetry check --lock
```

That test compares the PEP 621 and Poetry dependency declarations and asserts
that `all` is exactly the union of the capability profiles.

## Markers

| Marker | Description |
|---|---|
| default selection | `pytest` runs with `-m "not spark"`, so all non-spark tests are included by default. |
| `slow` | Defined marker. Still included by default unless you exclude it yourself. |
| `integration` | Defined marker. Still included by default unless you exclude it yourself. |
| `cloud_integration` | Tests against real cloud resources. Always skipped unless `--cloud-integration` is supplied. |
| `benchmark` | Opt-in performance benchmark. Requires `--run-benchmarks`; cloud benchmarks also require `--cloud-integration`. |
| `cloud_platform(name)` | Identifies the test platform as `fabric`, `aws`, or `databricks`; filter with `--cloud-platform`. |
| `spark` | Spark-specific tests. Excluded by default by the repo pytest config. When run directly, the Spark module also uses `pytest.importorskip(...)` for `pyspark` and `delta-spark`. |

```powershell
python -m pytest -m "not slow and not integration and not spark"
```

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

The main automated test surface is the pytest suite under `tests/`.
Separately, `usecase-sim/` provides coarse-grained execution scenarios and
runner scripts for end-to-end validation outside the core pytest unit suite.
