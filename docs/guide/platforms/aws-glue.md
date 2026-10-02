---
title: Deploy Python ETL to AWS Glue | DataCoolie
description: Configure a DataCoolie Glue Spark smoke job with S3 and bundled Delta, or choose a separately configured Glue Catalog Iceberg target.
---

# Deploy to AWS Glue

Start with Spark + FileProvider and the shared [platform smoke project](../../examples/runners.md#platform-smoke).
The baseline writes three rows to a new S3 Delta sandbox without Athena/catalog
registration. These are locally checked setup contracts, not a live Glue receipt.

## 1. Choose the runtime and package

| Runtime | Scope |
|---|---|
| Glue 5.0 Spark ETL | Chosen example baseline: Python 3.11, Spark 3.5.4, bundled Delta 3.3.0 |
| Python 3.11+ container/VM/local | Polars + S3 through AWSPlatform; separate engine installation |
| EMR / EMR Serverless Spark | Runtime/packaging must be adapted; no repo-backed deployment recipe here |
| Glue Python Shell (Python 3.9) | Incompatible with DataCoolie's Python `>=3.11,<4.0` requirement |

Glue 5.0 is a selected baseline, not the latest/default runtime. Do not infer
support for another Glue version from Python compatibility alone. Review
[Glue releases](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html)
and [job types](https://docs.aws.amazon.com/glue/latest/dg/glue-version-support-policy.html).

Attach the matching `datacoolie[aws]` package to the job. For an available
published release, the job argument value is:

```text
--additional-python-modules = datacoolie[aws]=={{ datacoolie_version }}
```

Use the matching wheel plus resolved dependencies for an unpublished checkout.
Freeze compatible Python wheels, including boto3/botocore, rather than resolving
unpinned packages on every production startup. Glue supplies Spark; do not add
a profile that installs another PySpark runtime. `pyiceberg` is for PyIceberg
operations, not a substitute for Spark Iceberg JARs.
[Glue Python libraries](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html).

## 2. Prepare IAM and S3

Use a Glue execution role trusted by Glue, with CloudWatch logging and access
to the script, wheels, temporary directories and fixture storage. The role needs
bucket listing and object reads for metadata/input and Delta transaction logs;
output/log/watermark roots also need writes and deletes as their operations
require. Narrow policies to the chosen prefixes. With SSE-KMS, reads require
Decrypt and writes GenerateDataKey (multipart operations can require both),
plus a permitting key policy. See [Glue role setup](https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html),
[minimum job access](https://docs.aws.amazon.com/glue/latest/dg/getting-started-min-privs-job.html)
and [S3 KMS permissions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html).

The baseline needs no Secrets Manager, Athena or Glue table-registration grants.
Spark S3 access and AWSPlatform's boto3 access must both work for the execution
role; successful control-file access alone does not prove engine access.

## 3. Build and upload the Delta fixture

Follow the [shared download/build recipe](../../examples/runners.md#platform-smoke).
Edit **both** S3 roots in `metadata/environments/aws.json`, validate and inspect
`--env aws`, then build. Example prefix: `s3://your-bucket/datacoolie-example`.

Upload `.builds/current/aws/metadata/` to `<root>/metadata/`. Upload input CSV
separately to `<root>/data/input/orders/orders.csv`. Output will be
`<root>/data/output/orders_platform_smoke`; reserve it for this overwrite test.
Upload canonical **aws/run_glue_spark.py** as the Glue job script
([source](../../examples/source/runners/aws/run_glue_spark.py.md) ·
[raw](../../examples/files/runners/aws/run_glue_spark.py)). The script reuses
GlueContext's session and uses DataCoolie state, not Glue bookmarks.

## 4. Set bundled Delta session and job arguments

Configure these **before** the Spark session starts. In Glue job parameters,
each row is a key and its complete value; repeated `--conf` segments belong
inside one `--conf` value:

| Key | Value |
|---|---|
| `--datalake-formats` | `delta` |
| `--conf` | `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore` |
| `--REGION` | Your bucket/job region, e.g. `us-east-1` |
| `--METADATA_PATH` | `<root>/metadata/metadata.json` |
| `--WATERMARK_BASE_PATH` | `<root>/.runtime/watermarks` |
| `--LOG_BASE_PATH` | `<root>/.runtime/logs` |
| `--STAGE` | `platform_smoke` |
| `--JOB_NUM`, `--JOB_INDEX` | `1`, `0` respectively |

`REGION`, metadata, watermark and log arguments are required by the script.
Omit optional `--CONNECTIONS_PATH`/`--SCHEMA_HINTS_PATH`; built metadata embeds
both. These uppercase script argument names differ from Glue's own lowercase
arguments. Apply shared smoke selection/result guards at `driver.run` if this
job gates downstream work; generic runner failure raises a job error.
The complete bundled Delta recipe comes from [AWS Delta setup](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-delta-lake.html).

### Custom Delta version alternative

Use this branch only when deliberately replacing the bundled runtime: omit
`delta` from `--datalake-formats`, provide matching Delta JARs through
`--extra-jars`, and set `--user-jars-first=true` on Glue 5.0+. Provide the matching
Python API through `--extra-py-files` using the artifact layout documented by
AWS (their Delta JAR includes the Python library), and retain the Delta extension,
catalog and S3 log-store settings. Match Spark/Scala/JAR/Python versions as one
set. Python `pip install delta-spark` alone does not provision this custom
Glue JVM runtime. Do not combine bundled and custom Delta JARs.

## 5. Verify Delta output and logs

Require selected name `orders_platform_smoke` and counts
`total=1, succeeded=1, failed=0, pending=0`. Independently read output using a
compatible Spark session, or place this verification after the run in the job:

```python
output = spark.read.format("delta").load(
    "s3://your-bucket/datacoolie-example/data/output/orders_platform_smoke"
).select("order_id", "customer_id", "amount").orderBy("order_id")
assert [tuple(row) for row in output.collect()] == [(1, 100, 20), (2, 100, 43), (3, 101, 7)]
assert output.dtypes == [("order_id", "bigint"), ("customer_id", "bigint"), ("amount", "bigint")]
```

Inspect DataCoolie execution/system logs under `LOG_BASE_PATH` and Glue's
CloudWatch error logs. Job success alone proves neither intended selection nor
Athena queryability.

## Bundled Iceberg alternative

Use environment `aws-iceberg`, replace the input bucket and output Glue database,
validate/inspect/build, and upload its metadata instead. Create the sandbox Glue
database and grant the role the required Glue catalog access; account for Lake
Formation permissions if that database is governed by it. Keep output
`format="iceberg"`, `catalog="glue_catalog"`, `database="datacoolie_example"`,
no `schema_name`, and empty `base_path`. This resolves a **named** Iceberg table.

Replace the Delta session settings with `--datalake-formats=iceberg` and this
single `--conf` value (substitute the warehouse bucket/prefix):

```text
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://your-bucket/datacoolie-example/warehouse --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

The remaining script parameters are unchanged. Verify with
`spark.table("glue_catalog.datacoolie_example.orders_platform_smoke")` and the
same three business rows/types. The warehouse is session configuration, not a
Volume-style connection path. For custom Iceberg, omit bundled `iceberg`, supply
matching JARs and use `--user-jars-first=true` on Glue 5.0+; qualify that version
set separately. See [AWS Iceberg setup](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html).

## Optional catalog handoff and secrets {#5-secrets}

For AWS path-addressed Delta, `athena_output_location` opts into native Delta
catalog registration via Athena; `register_symlink_table` requests the legacy
symlink route and implies manifest generation. Registration is conditional:
unchanged-schema writes can skip it, and registration exceptions are logged as
warnings. Successful data writes/job status therefore do not guarantee a new
or refreshed catalog entry. If downstream requires Athena, independently check
the expected Glue database/table, location and an Athena query result.

Grant optional Athena StartQueryExecution/GetQueryExecution in the chosen
workgroup, result-bucket access and Glue database/table permissions needed by
that DDL; these are additional to the smoke baseline.
[Workgroup policies](https://docs.aws.amazon.com/athena/latest/ug/example-policies-workgroup.html),
[Glue resource access](https://docs.aws.amazon.com/athena/latest/ug/fine-grained-access-to-glue-resources.html).

Secret references map configured field values to **JSON keys** in the secret:

```json
{"configure": {"username": "db_user", "password": "db_pass"}, "secrets_ref": {"arn:aws:secretsmanager:us-east-1:123456789012:secret:datacoolie/rds-example": ["username", "password"]}}
```

The secret payload must contain `db_user` and `db_pass`. Use your actual region,
ARN or exact secret name; grant GetSecretValue and, for a custom encryption key,
KMS Decrypt. [Secret retrieval permissions](https://docs.aws.amazon.com/cli/latest/reference/secretsmanager/get-secret-value.html).

## Polars, troubleshooting and next steps

Use **aws/run_polars_s3.py** in controlled Python 3.11+
([source](../../examples/source/runners/aws/run_polars_s3.py.md) ·
[raw](../../examples/files/runners/aws/run_polars_s3.py)); install
`datacoolie[aws,polars-delta]` and run `--help` for S3 parameters. This is not a
Glue Python Shell job. AWSPlatform uses boto3 credentials for control files;
this script does not copy those credentials into Polars storage options.
Configure compatible ambient engine credentials or explicit storage options
for CSV/Delta access, and verify format-specific auth before adding Iceberg.

| Symptom | Check |
|---|---|
| Delta class missing | Bundled flag or complete custom JAR recipe before session creation |
| Iceberg catalog missing | Extension, catalog, warehouse and Glue permissions |
| Metadata works, output denied | Spark S3 role access, output reads/writes/deletes and KMS |
| Job succeeds, Athena cannot see table | Conditional/warning-only registration and separate catalog/query check |
| Zero selected flows | Uploaded built environment, stage and shard arguments |

Continue with [operations](../operations/index.md). The larger [AWS simulator](https://github.com/datacoolie/datacoolie/blob/main/usecase-sim/platforms/aws/README.md)
and [WWI walkthrough](../../examples/wwi-medallion-multicloud.md#environment-matrix)
need their own input/dependency preparation.
