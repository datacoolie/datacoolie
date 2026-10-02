---
title: Runner examples
description: Project-owned DataCoolie runners for local Python, Spark hosts, replay, maintenance and managed platforms.
---

# Runner examples

A runner is project code that owns host setup and calls the public
`DataCoolieDriver` API. DataCoolie does not provide a universal `dc run`
command: the project decides how tables are registered, how credentials are
attached and which stage is passed to the Driver.

The complete host/operation inventory is maintained on the [Examples
catalog](index.md#runners). Use the catalog's explicit `source` and
`raw` actions so the link target is unambiguous. The `.ipynb` files are the
source of truth for managed notebook runners; their generated source pages keep
Markdown/code order without executing cells or showing outputs. For a
multi-file project, use **project-files** to browse its complete tree and
**download** to obtain one `.zip` archive instead of collecting files
individually. The `project-files` action jumps to the complete project section
in the catalog; it does not download the archive.

## Platform smoke: prepare once, choose a host {#platform-smoke}

The [Platform smoke project](index.md#platform-smoke-project)
([download](downloads/platform-smoke.zip)) contains one `platform_smoke` stage
with one active flow, `orders_platform_smoke`. Its CSV has three business rows:

| order_id | customer_id | amount |
|---|---|---|
| 1 | 100 | 20 |
| 2 | 100 | 43 |
| 3 | 101 | 7 |

All three business columns become signed 64-bit integers. The destination uses
`overwrite`: choose a new sandbox path/table before the first run. Framework
system columns can appear alongside these business columns. This fixture has no
SQL files, functions, secrets, incremental watermark or database service.

### Download, adapt and build

Extract the archive into a new directory; it contains `platform-smoke/`. Save
[the local Polars runner](files/runners/local/run.py) as `run_local.py` beside
that directory. Use the matching source revision/version for both artifacts;
a local preview labeled unpublished is an explicit checkout handoff, not proof
that its files are available on the published site.

On the preparation machine:

```bash
pip install "datacoolie[cli,polars-delta]"
dc --project-dir platform-smoke validate --format json
dc --project-dir platform-smoke inspect metadata --env local --section connections --full --format json
dc --project-dir platform-smoke build --format json
```

Before building for a cloud host, edit `metadata/environments/<env>.json`:

| Environment | Replace in the overlay | Destination |
|---|---|---|
| `local` | Relative roots only if changing the local fixture layout | `data/output/orders_platform_smoke` |
| `fabric` | Workspace/lakehouse in **both** connection ABFSS roots | OneLake path-addressed Delta |
| `databricks` | Input Volume path; output `catalog` and `database` | Named UC table; empty `base_path` clears the local path |
| `aws` | Bucket/prefix in **both** S3 connection roots | S3 path-addressed Delta, without catalog registration |
| `aws-iceberg` | Input S3 root and output Glue database/catalog | Named Glue Iceberg table; warehouse is a Spark job setting |

Also replace the chosen runner's metadata, watermark and log paths. A runner
root does not rewrite connection paths inside metadata. Keep `schema_name`
unset for the three-part UC/Glue table names. Overlays merge by connection
identity; the Driver does not apply them. Inspect the effective document with
`dc --project-dir platform-smoke inspect metadata --env <env> --section connections --full --format json`
and require successful validation before using the build. Inspect
`--section dataflows --full` as well to check selection and transform settings.

Build creates all declared environments. Use the returned `data.current_path`
and its `<env>` directory, normally `platform-smoke/.builds/current/<env>`;
never invent an immutable build ID. Upload the selected environment's
`metadata/` directory to the host metadata root. Upload
`data/input/orders/orders.csv` **separately** to the source connection's
`<base_path>/orders/orders.csv`. CLI build includes configured components;
it does not include/upload `data/input` or run a cloud job.

### Rehearse locally

From the directory containing `run_local.py` and the extracted project:

```bash
python run_local.py --working-directory platform-smoke \
  --artifact-base-path .builds/current/local \
  --state-base-path .runtime --stage platform_smoke
```

Connection/artifact/state paths are resolved after the runner changes its
working directory. Expect one succeeded flow and Delta output under
`platform-smoke/data/output/orders_platform_smoke`. Read it independently:

```python
import polars as pl

rows = (
    pl.read_delta("platform-smoke/data/output/orders_platform_smoke")
    .select("order_id", "customer_id", "amount")
    .sort("order_id")
)
assert rows.rows() == [(1, 100, 20), (2, 100, 43), (3, 101, 7)]
assert rows.dtypes == [pl.Int64, pl.Int64, pl.Int64]
```

### Run on the selected host

Follow [Fabric](../guide/platforms/fabric.md),
[Databricks](../guide/platforms/databricks.md) or
[AWS Glue](../guide/platforms/aws-glue.md) for dependencies, identity, uploads
and exact host parameters. The archive deliberately does not duplicate these
[canonical runners](index.md#runners). Their generic failure handling raises
on failed dataflows; it does not enforce the required selection or prove output
freshness. For this **unsharded** three-row smoke task, replace the runner's
`result = driver.run(...)` line inside the Driver context with:

```python
selected = driver.load_dataflows(stage="platform_smoke", active_only=True)
if len(selected) != 1 or {flow.name for flow in selected} != {"orders_platform_smoke"}:
    raise RuntimeError("Platform smoke selection does not match the required flow")
result = driver.run(dataflows=selected)
```

After the run, inside the job:

```python
if (result.total, result.succeeded, result.failed, result.pending) != (1, 1, 0, 0):
    raise RuntimeError("Platform smoke did not complete its one required flow")
```

Read the three business rows from the exact destination. See the broader
[required-selection guard](../guide/operations/run-stage.md#single-stage)
for downstream scheduling. Do not copy this one-flow rule to empty shards or
normal incremental jobs.
Local fixture tests and simulated host adapters do not execute the cloud host.

## Getting-started project {#getting-started-project}

The [getting-started project](index.md#getting-started-project) is the
canonical local onboarding fixture. It keeps the same metadata while showing
three deliberately separate lessons: keyed orders incremental loading,
customer full refresh and a Bronze-to-Silver partitioned-detail continuation.
Its archive includes the input files, schema hints and both local runners.

From an extracted project root, use one lesson at a time:

```bash
python runners/local/run_polars.py --lesson orders --state-base-path .runtime
python runners/local/run_polars.py --lesson customers --state-base-path .runtime
python runners/local/run_polars.py --lesson multi-stage --state-base-path .runtime
```

Use a fresh project directory for a new engine. The runner preflights the CSV,
selects exactly one required flow, requires terminal completion and reads the
Delta result independently. It accepts an incremental no-change skip only when
the input exists, the persisted watermark proves there are no newer rows and
the previous output is readable. A stale output does not authorize a dependent
stage. The Spark runner exposes the same lesson names and owns its local
Delta-enabled `SparkSession`; it should be run in a separate workspace.

The first orders run retains three business rows after deduplicating the
duplicate ID 2. Appending ID 4 produces four rows. The customers lesson writes
two rows to its separate full-refresh destination. The multi-stage lesson runs
Bronze first, checks its output, then writes four typed detail rows after the
append; it does not aggregate them.

These guards are intentionally fixture-specific tutorial code. They do not
replace the generic runner's sharding or incremental skip policy. For reusable
stage selection and failure handling, see [Run a stage](../guide/operations/run-stage.md).

## Local artifact runner {#local-artifact-runner}

### Artifact project invocation {#artifact-project-runner}

The complete extracted-project recipe, expected rows and adaptation notes live
under [Dataflow examples](dataflows.md#artifact-project-recipe). From the
directory containing the extracted `artifact/` project's `metadata/`,
`queries/` and `runners/`, use its project-owned runner:

```bash
python runners/dev/run.py --state-base-path .runtime
```

That runner registers the `orders` and `order_categories` Polars relations
before reading `artifact:/queries/orders.sql`. The project CSV is a reference
fixture; changing it alone does not change this SQL demonstration.

The catalog's **runners/local/run_artifact_minimal.py**
([source](source/runners/local/run_artifact_minimal.py.md) ·
[raw](files/runners/local/run_artifact_minimal.py)) is a reusable artifact-root
template. It does not register the two relations used by this project, so it is
not the command for the Artifact SQL recipe above. It accepts one or more
explicit `--sql-base-path` values and returns a non-zero status when the Driver
reports failed dataflows.

The catalog's general **runners/local/run.py** exposes `--log-persistence-mode snapshot|batch` and the
batch thresholds `--log-flush-interval-seconds` and
`--log-flush-batch-bytes`. These configure the public `LogConfig`; they do not
change metadata, state or component roots. The default batch interval is five
minutes and console color is `auto`.

## Local Spark runner {#local-spark-runner}

For a local Spark process, use **runners/local/run_spark.py**
([source](source/runners/local/run_spark.py.md) ·
[raw](files/runners/local/run_spark.py)). It accepts the same artifact,
metadata, SQL-root, state/log and run-attribute options, but owns the
`SparkSession` lifecycle. CSV fields remain strings unless the metadata supplies
schema hints; register catalog/views in project code before using a Spark SQL
query that refers to them.

When an artifact runner receives an explicit `--watermark-base-path`, it creates
the FileProvider explicitly so that component-owned state is not discarded;
artifact-only invocation keeps Driver's automatic `<artifact>/metadata` fallback.

## Managed notebook runners {#managed-notebook-runners}

Managed-host notebooks and external SDK scripts are setup contracts. Their exact
host/path rules and verification boundaries are listed in the catalog entries.
The most common notebook source is **runners/databricks/run_spark.ipynb**
([source](source/runners/databricks/run_spark.ipynb.md) ·
[raw](files/runners/databricks/run_spark.ipynb)); replay is
**runners/databricks/replay_spark.ipynb**
([source](source/runners/databricks/replay_spark.ipynb.md) ·
[raw](files/runners/databricks/replay_spark.ipynb)) and maintenance is
**runners/databricks/maintenance_spark.ipynb**
([source](source/runners/databricks/maintenance_spark.ipynb.md) ·
[raw](files/runners/databricks/maintenance_spark.ipynb)).

## Host adaptation rules

- Keep `runners/<env>/...` project-owned and copy only the host setup that the
  environment needs.
- Pass `--job-num` and `--job-index` for sharding; a notebook or Glue job maps
  its host parameters to the same two values.
- Keep `log_base_path`, watermark/state roots and metadata roots explicit at the
  runner boundary. Do not put secrets or production paths in a published file.
- Use `allowed_function_prefixes=[]` when the example has no Python function
  source. A project with packaged functions replaces that empty list with its
  fixed import prefix before startup.
- Treat managed-host examples as setup contracts until a separate live-host
  record exists. Syntax and public-constructor checks do not imply cloud
  execution.
- External cloud runners fail fast on launcher-local roots: AWS uses
  `s3://`/`s3a://`, Fabric external uses qualified `abfs://`, `abfss://` or
  HTTPS paths, and Databricks external uses Unity Catalog `/Volumes/...`.
  These checks protect path ownership; they do not validate credentials or
  prove a live host run.

## Related guides

- [Runtime configuration and path ownership](../guide/operations/runtime-configuration.md)
- [Replay and backfill](../guide/operations/replay-and-backfill.md)
- [Maintenance](../guide/operations/maintenance.md)
- [Platform guides](../guide/platforms/aws-glue.md),
  [Databricks](../guide/platforms/databricks.md) and
  [Fabric](../guide/platforms/fabric.md)
