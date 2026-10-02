---
title: Deploy Python ETL to Microsoft Fabric | DataCoolie
description: Prepare and verify a DataCoolie Spark smoke run on OneLake, with native Python and external SDK alternatives.
---

# Deploy to Microsoft Fabric

Start with Spark + FileProvider and the shared [platform smoke project](../../examples/runners.md#platform-smoke).
It reads three CSV rows and overwrites a new sandbox Delta path. This guide is
a locally checked setup contract; it does not report a live Fabric run.

## 1. Choose the host and prepare access

| Host | Installation | Boundary |
|---|---|---|
| Fabric Spark notebook | Base `datacoolie`; reuse supplied `spark` | NotebookUtils for control files; Spark connector for business data |
| Fabric native Python | `datacoolie[polars-delta]`, Python 3.11+ | Attached lakehouse mount for this fixture |
| External Python 3.11+ | `datacoolie[fabric-external,polars-delta]` | Azure SDK control access; separate engine storage credentials |

Use Spark for distributed work, MERGE/SCD and Gold tables requiring Spark
Delta features. Use Polars for bounded single-node file work after measuring
memory/runtime. See [engine selection](../../blog/posts/2026-05-26-polars-vs-spark-for-etl.md).

Create a sandbox lakehouse, attach it as the notebook's default lakehouse, and
choose a runtime compatible with Python `>=3.11,<4.0`. Install a matching release:

```python
%pip install "datacoolie=={{ datacoolie_version }}"
```

For an unpublished checkout, install its matching wheel. Fabric supplies
Spark/Delta; avoid installing another PySpark runtime into the host.

The execution identity needs metadata/input read access and output/log/watermark
read/write access. Workspace Contributor is a convenient sandbox role with
broader access than a restricted production policy. See [OneLake security](https://learn.microsoft.com/en-us/fabric/onelake/security/get-started-security).
Interactive runs use the current user; schedules use their creator/last updater.
For pipelines, inspect the Notebook activity's configured connection identity;
Workspace Identity authentication is also available and needs access to the
notebook and data workspaces. Interactive success does not prove scheduler access.
[Notebook identities](https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook),
[pipeline Notebook activity](https://learn.microsoft.com/en-us/fabric/data-factory/notebook-activity).

## 2. Build and upload

Follow the shared [download/build recipe](../../examples/runners.md#platform-smoke).
Replace workspace/lakehouse identifiers in **both** roots in
`metadata/environments/fabric.json`; validate, inspect `--env fabric`, then build.
Use a qualified prefix:

```text
abfss://your-workspace@onelake.dfs.fabric.microsoft.com/your-lakehouse.Lakehouse/Files/datacoolie-example
```

Upload `.builds/current/fabric/metadata/` to `<root>/metadata/`. Upload the CSV
**separately** to `<root>/data/input/orders/orders.csv`. Output is
`<root>/data/output/orders_platform_smoke`; reserve it for this overwrite test.
Changing notebook parameters does not rewrite connection paths in metadata.

## 3. Import and run

Import canonical **fabric/run_spark.ipynb**
([source](../../examples/source/runners/fabric/run_spark.ipynb.md) ·
[raw](../../examples/files/runners/fabric/run_spark.ipynb)). It explicitly selects
`runtime="fabric"` and reuses the supplied Spark session.

| Parameter cell / pipeline parameter | Smoke value |
|---|---|
| `METADATA_PATH` | `<root>/metadata/metadata.json` (exact built file) |
| `CONNECTIONS_PATH`, `SCHEMA_HINTS_PATH` | `None`; included in built metadata |
| `WATERMARK_BASE_PATH` | `<root>/.runtime/watermarks` |
| `LOG_BASE_PATH` | `<root>/.runtime/logs` |
| `STAGE` | `platform_smoke` |
| `JOB_NUM`, `JOB_INDEX` | `1`, `0` |

Edit the parameter cell or pass these exact names through the pipeline activity.
Apply the shared smoke selection/result guards at its `driver.run` boundary
when gating downstream work. The generic runner raises on failed dataflows.

## 4. Verify output and logs

Require selection `orders_platform_smoke` and counts
`total=1, succeeded=1, failed=0, pending=0`. Read the actual Delta destination:

```python
output = spark.read.format("delta").load(
    "abfss://your-workspace@onelake.dfs.fabric.microsoft.com/"
    "your-lakehouse.Lakehouse/Files/datacoolie-example/data/output/orders_platform_smoke"
).select("order_id", "customer_id", "amount").orderBy("order_id")
assert [tuple(row) for row in output.collect()] == [(1, 100, 20), (2, 100, 43), (3, 101, 7)]
assert output.dtypes == [("order_id", "bigint"), ("customer_id", "bigint"), ("amount", "bigint")]
```

Inspect execution/system logs under `LOG_BASE_PATH` for job ID and diagnostics.
A successful notebook cell alone does not prove selection or fresh output.
This fixture writes under `Files/`; it does not register a lakehouse `Tables/` table.

## Native Python and external SDK

[NotebookUtils relative paths](https://learn.microsoft.com/en-us/fabric/data-engineering/notebookutils/notebookutils-file-system)
differ by kernel. The platform passes paths through to its backend.

| Path | Spark notebook | Native Python | External SDK |
|---|---|---|---|
| `Files/...` | Default lakehouse relative | Python working-directory relative; avoid here | No default lakehouse |
| `/lakehouse/default/Files/...` | Attached mount where available | Use for this fixture | Unavailable on laptop |
| Qualified `abfss://...` | Host connectors | Needs suitable engine connector/auth | Azure SDK for control files |

For native Python choose a supported Python 3.11+ kernel, install
`datacoolie[polars-delta]`, and use **fabric/run_polars.ipynb**
([source](../../examples/source/runners/fabric/run_polars.ipynb.md) ·
[raw](../../examples/files/runners/fabric/run_polars.ipynb)). Rebuild the Fabric
overlay with both business roots under
`/lakehouse/default/Files/datacoolie-example/data/{input,output}`. Use that mount
prefix for metadata/log/watermark parameters too. Put sizing before execution:

```python
%%configure -f
{"vCores": 4}
```

This is Fabric cell syntax. See [Python kernels and sizing](https://learn.microsoft.com/en-us/fabric/data-engineering/using-python-experience-on-notebook).

For external execution use **fabric/run_polars_azure_sdk.py**
([source](../../examples/source/runners/fabric/run_polars_azure_sdk.py.md) ·
[raw](../../examples/files/runners/fabric/run_polars_azure_sdk.py)); run `--help`
for its required qualified cloud paths and CLI parameters. It selects
`runtime="external"` and uses `DefaultAzureCredential`. Azure SDK credentials
cover platform metadata/log/secrets; they do not automatically configure Polars
CSV/Delta storage auth. Provide engine storage options or directly accessible
business paths before running.

## Optional secrets and Gold tables {#5-secrets}

No secrets are required by the fixture. For a secret-backed connection:

```json
{"configure": {"password": "sql-password"}, "secrets_ref": {"https://myvault.vault.azure.net/": ["password"]}}
```

The executing identity needs Key Vault secret Get separately from lakehouse
access. Native mode uses NotebookUtils; external mode uses Azure SecretClient.
See [credentials](https://learn.microsoft.com/en-us/fabric/data-engineering/notebookutils/notebookutils-credentials).

For later Direct Lake Gold workloads, explicitly choose
`spark.conf.set("spark.sql.parquet.vorder.default", "true")` before writing
when read benefits justify write costs. Review [V-Order](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-optimization-and-v-order)
and [Direct Lake storage](https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-understand-storage).
The smoke output alone does not create a semantic model.

## Troubleshooting and next steps

| Symptom | Check |
|---|---|
| Python cannot find `Files/...` | Attached mount and rebuilt business roots |
| Pipeline denied after interactive success | Pipeline connection identity and cross-workspace access |
| Empty selection | Built metadata root, stage, active flag and shard parameters |
| SDK metadata works, engine fails | Business connector credentials and path support |

Continue with [operations](../operations/index.md) for replay, maintenance,
sharding and functions. The larger [Fabric simulator](https://github.com/datacoolie/datacoolie/blob/main/usecase-sim/platforms/fabric/README.md)
and [WWI walkthrough](../../examples/wwi-medallion-multicloud.md#environment-matrix)
require their own input/dependency preparation.
