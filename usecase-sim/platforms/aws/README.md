# AWS Assets for usecase-sim

Prepared AWS assets for file + Delta + Iceberg scenarios using real S3 and
AWS Secrets Manager. The sample metadata currently targets the `s3://de-dev-0007/`
bucket, and the scripts below default to the same bucket.

Three execution paths are provided:

| Script | Runtime | Engine | Formats |
|---|---|---|---|
| `sample_aws_glue_spark.py` | AWS Glue Spark (cloud job) | `SparkEngine` | File, Delta, Iceberg |
| `sample_aws_glue_docker.py` | **Glue Docker on-prem** (hybrid) | `SparkEngine` | File, Delta, Iceberg |
| `sample_aws_local_polars.py` | **Local** (Python 3.11+, `aws configure`) | `PolarsEngine` | File, Delta, Iceberg |

Hybrid mode: `sample_aws_glue_docker.py` runs Spark compute inside the official
Glue 5.0 Docker image on-prem, but all AWS data services (S3, Glue Data Catalog,
Secrets Manager, Athena) remain in the real cloud — no mocking.

> **Why not Glue Python Shell for Polars?**  DataCoolie requires Python >= 3.11.
> AWS Glue Python Shell tops out at Python 3.9 and is incompatible with the
> current `datacoolie` package.  Use `sample_aws_local_polars.py` to run the
> Polars pipeline locally against real AWS resources.

## What is in this folder

- `aws_glue_use_cases.json`:
  - Shared metadata file used by both the Glue Spark and local Polars paths.
  - Paths currently point to `s3://de-dev-0007/`.
  - Stages use standard names (`read_file`, `write_file`, `load_delta`,
    `load_iceberg`, `read_lakehouse`, …) without platform prefixes.
  - Delta destination includes `database` and `configure.athena_output_location`
    to trigger Glue catalog registration after writes.
  - Iceberg connections include `catalog: "glue_catalog"` and `database: "datacoolie"`.
  - One `aws_secrets_example_source` connection demonstrates Secrets Manager
    `secrets_ref` with a Secrets Manager ARN as the outer key.
- `sample_aws_glue_spark.py`:
  - Glue Spark (PySpark) job script for cloud Glue jobs.
  - Uses `SparkEngine` with `max_workers=8`.
  - Full Delta + Iceberg + Athena/Glue catalog registration.
- `sample_aws_glue_docker.py`:
  - **Hybrid Docker version** — runs inside `public.ecr.aws/glue/aws-glue-libs:5` on-prem.
  - Mirrors `sample_aws_glue_spark.py` as closely as possible.
  - Key differences from the cloud script:
    - `sys.path` injects `src/` (datacoolie) from the volume mount — mirrors `--additional-python-modules`.
    - `functions/` is passed via `--py-files functions.zip` — mirrors `--extra-py-files`.
    - Arg parsing is **identical**: `getResolvedOptions(sys.argv, ["JOB_NAME", "REGION", "BUCKET", "STAGE"])`.
    - No Glue Job bookmark / Job.init() calls (not supported locally).
  - Use `run_glue_docker.py` to launch it (see **Running with Glue Docker** below).
- `run_glue_docker.py`:
  - Python CLI that builds and fires the `docker run` command (cross-platform).
  - Packs `functions/` as `functions.zip` before launch (mirrors `--extra-py-files`).
  - Passes the zip via spark-submit `--py-files` inside the container.
  - Mounts `~/.aws:ro` and the workspace; passes `--profile`.
  - Args: `--stage`, `--region`, `--bucket`, `--profile`, `--job-name`, `--workspace`.
  - **`--install`** (optional) — mirrors `--additional-python-modules` in cloud Glue:
    - Omit: load `datacoolie` from the volume-mounted `src/` tree (**src mode**, default).
    - `--install datacoolie` or `--install datacoolie==1.0.0`: `pip install` from PyPI.
    - `--install dist/datacoolie-1.0.0-py3-none-any.whl`: `pip install` a local wheel file.
      Path can be relative to workspace root or absolute (must be inside the workspace).
- `sample_aws_local_polars.py`:
  - Local runner for Polars pipelines — no Glue runtime or IAM execution role required.
  - Reads AWS credentials from `~/.aws/credentials` via `aws configure`.
  - Accepts `--PROFILE <name>` for named profiles (SSO, assumed roles, etc.).
  - All parameters are optional CLI flags (see script header for defaults).
- `functions/`:
  - Python package for `python_function` source handlers.
  - Must be on `sys.path` at job runtime so that `functions.sources.sql_query_orders`
    and sibling imports resolve.
- `README.md`: this file.
- `DEFERRED.md`: known gaps and deferred work.

## Running with Glue Docker (hybrid mode)

Compute runs in the official Glue 5.0 Docker image on-prem; all AWS data services
(S3, Glue Data Catalog, Secrets Manager, Athena) are real cloud resources.

### Prerequisites

1. Docker Desktop running in **Linux container** mode.
2. Python 3.6+ available (for `run_glue_docker.py`).
3. AWS named profile configured: `aws configure --profile <name>`
4. Pull the image once (~7 GB):
   ```bash
   docker pull public.ecr.aws/glue/aws-glue-libs:5
   ```

`datacoolie` install modes for Docker (all mirror `--additional-python-modules` in cloud Glue):

| Mode | Command | How `datacoolie` is loaded |
|---|---|---|
| **src** (default) | *(no `--install`)* | `sys.path` → volume-mounted `src/` |
| **PyPI package** | `--install datacoolie` | `pip install datacoolie` inside container |
| **wheel file** | `--install dist/datacoolie-*.whl` | `pip install <wheel>` inside container |

`functions/` is packaged as `functions.zip` by the Python helper and passed to
`spark-submit --py-files` (mirrors `--extra-py-files` in cloud Glue).

### Quick start

```bash
# Src mode — load directly from volume-mounted src/ (Docker dev default)
python usecase-sim/platforms/aws/run_glue_docker.py --stage read_file

# PyPI package mode — same as cloud Glue --additional-python-modules
python usecase-sim/platforms/aws/run_glue_docker.py --stage read_file --install datacoolie
python usecase-sim/platforms/aws/run_glue_docker.py --stage read_file --install datacoolie==1.0.0

# Wheel file mode — pip install a local wheel inside the container
python usecase-sim/platforms/aws/run_glue_docker.py --stage read_file --install dist/datacoolie-1.0.0-py3-none-any.whl

# Run multiple stages
python usecase-sim/platforms/aws/run_glue_docker.py --stage "read_file,load_delta"

# Run all stages
python usecase-sim/platforms/aws/run_glue_docker.py

# Use a non-default AWS profile or bucket
python usecase-sim/platforms/aws/run_glue_docker.py --profile my-sso --bucket my-bucket --stage read_file
```

Spark UI is available at `http://localhost:4050` while the job runs.


### Differences from cloud `sample_aws_glue_spark.py`

| | Cloud Glue | Glue Docker |
|---|---|---|
| Script | `sample_aws_glue_spark.py` | `sample_aws_glue_docker.py` |
| `datacoolie` import | `--additional-python-modules` (PyPI or wheel) | **`--install <pkg>`**: `pip install` from PyPI |
| | | **`--install <wheel>`**: `pip install` wheel file |
| | | **default (Docker only)**: `sys.path` → volume-mounted `src/` |
| `functions/` on path | `--extra-py-files functions.zip` | `--py-files functions.zip` (zipped by Python helper) |
| Arg parsing | `getResolvedOptions` | `getResolvedOptions` (identical) |
| Job bookmarks | ✅ Supported | ❌ Not supported (local limitation) |
| AWS services | Real cloud | Real cloud (identical) |

---

## S3 path convention

Root bucket: `s3://de-dev-0007/`

| Purpose | S3 path |
|---|---|
| Input files | `s3://de-dev-0007/input/{format}/` |
| File outputs | `s3://de-dev-0007/output/{format}/` |
| Delta tables | `s3://de-dev-0007/output/delta/` |
| Iceberg tables | `s3://de-dev-0007/output/iceberg/` |
| Metadata file | `s3://de-dev-0007/metadata/aws_glue_use_cases.json` |
| Athena results | `s3://de-dev-0007/athena-results/` |
| Logs | `s3://de-dev-0007/logs/datacoolie` |

## Prerequisites

1. AWS account with IAM role for Glue execution.
2. S3 bucket `de-dev-0007` created in the target region (default: `ap-southeast-1`).
3. AWS Glue Data Catalog database `datacoolie` created:
   ```bash
   aws glue create-database --database-input '{"Name":"datacoolie"}' --region ap-southeast-1
   ```
4. (Spark only) Delta Lake JAR available to the Glue job.
   Use Glue 4.0+ and add the Delta connector from the Glue marketplace, or supply
   the JAR via `--extra-jars`.
5. `datacoolie` wheel uploaded to S3 and referenced via
   `--additional-python-modules` or a Glue Python environment. `Note`: `datacoolie` wheel built and uploaded to S3 (see **Installing datacoolie on Glue** below).
6. Input files uploaded from the local repo folder `usecase-sim/data/input/`
   to the corresponding S3 paths.
7. Metadata file `aws_glue_use_cases.json` uploaded to
  `s3://de-dev-0007/metadata/aws_glue_use_cases.json`.
8. Keep the `BUCKET` job argument and the paths inside `aws_glue_use_cases.json`
  aligned. The current repo copy of the metadata already points at `de-dev-0007`.

### IAM permissions required

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:DeleteObjects",
        "s3:HeadObject",
        "s3:CopyObject"
      ],
      "Resource": "arn:aws:s3:::de-dev-0007/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::de-dev-0007"
    },
    {
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "arn:aws:secretsmanager:ap-southeast-1:*:secret:de-dev-0007/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:ap-southeast-1:*:log-group:/aws-glue/*"
    }
  ]
}
```

## Installing datacoolie on Glue

AWS Glue does not have `datacoolie` pre-installed.  You must build a wheel,
upload it to S3, and reference it via Glue job parameters.

### 1 — Build the wheel (run once locally)

```bash
cd datacoolie/          # repo sub-folder that contains pyproject.toml
pip install build
python -m build --wheel --outdir dist/
# produces: dist/datacoolie-0.1.1-py3-none-any.whl
```

### 2 — Upload wheel to S3

```bash
aws s3 cp dist/datacoolie-0.1.1-py3-none-any.whl \
    s3://de-dev-0007/libraries/datacoolie-0.1.1-py3-none-any.whl \
    --region ap-southeast-1
```

### 3 — Add Glue job parameters

#### Spark job (`sample_aws_glue_spark.py`)

`pyspark` and `delta-spark` are provided by the Glue 4.0 / 5.0 runtime — **do
not** include them in `--additional-python-modules`.

| Parameter | Value |
|---|---|
| `--extra-py-files` | `s3://de-dev-0007/libraries/datacoolie-0.1.1-py3-none-any.whl` |
| `--additional-python-modules` | `boto3>=1.28` |

> `boto3` is also pre-installed in Glue 4.0+; you can omit it if the
> pre-installed version is sufficient.

### Extras reference (`pyproject.toml`)

The `datacoolie` package ships platform bundle extras you can use for local
`pip install` when testing outside Lambda or Glue:

| Extra | Purpose |
|---|---|
| `datacoolie[aws-polars]` | Polars path — Polars + Delta + Iceberg + boto3 (Python 3.11+) |
| `datacoolie[aws]` | Same as `aws-polars`; convenience alias |
| `datacoolie[aws-spark]` | Spark job — only `boto3` (Spark/Delta supplied by Glue) |

Example install for the local Polars scenario:
```bash
pip install "datacoolie[aws-polars]"
```

---

## Setting up the `functions/` package (python_function sources)

Two dataflows — `python_fn__delta_sql` and `python_fn__iceberg_sql` — use the
`python_function` feature to run custom SQL via a user-supplied Python module.
The dotted paths `functions.sources.sql_query_orders` and
`functions.sources.sql_query_orders_iceberg` must be importable at job runtime.

### Directory layout

```
usecase-sim/platforms/aws/
├── sample_aws_glue_spark.py
├── aws_glue_use_cases.json
└── functions/
    ├── __init__.py
    └── sources/
        ├── __init__.py
        ├── sql_query_orders.py          # Delta SQL function
        └── sql_query_orders_iceberg.py  # Iceberg SQL function
```

### Function signature

Every python_function source must follow this signature:

```python
def my_loader(engine, source, watermark) -> DataFrame | None:
    ...
```

| Argument | Type | Description |
|---|---|---|
| `engine` | `SparkEngine` / `PolarsEngine` | Active engine instance |
| `source` | `Source` | Source model for the dataflow |
| `watermark` | `dict \| None` | Previous watermark values; `None` on first run |

### Packaging for Glue Spark

Glue does **not** auto-import local folders. Zip and upload the package, then
reference it via `--extra-py-files`:

```bash
# Zip the functions package (keep the functions/ folder as the root entry)
Compress-Archive -Path functions -DestinationPath functions.zip -Force

# Upload to S3
aws s3 cp functions.zip \
    s3://de-dev-0007/libraries/functions.zip \
    --region ap-southeast-1
```

| Glue parameter | Value |
|---|---|
| `--extra-py-files` | `s3://de-dev-0007/libraries/datacoolie-0.1.1-py3-none-any.whl,s3://de-dev-0007/libraries/functions.zip` |

Glue will unzip `functions.zip` and add it to `sys.path`, making
`functions.sources.sql_query_orders` importable.

> **Re-upload after any change.** Every time you edit a function file,
> re-zip and re-upload `functions.zip` before the next Glue job run.

---

## Spark Glue job setup

1. Create a Glue ETL job (type: Spark, Glue 4.0+; Glue 5.0+ recommended).
2. Set script path to `sample_aws_glue_spark.py` (upload to S3 first).
3. Add job parameters:
  ```text
  --REGION      ap-southeast-1
  --BUCKET      de-dev-0007
  --STAGE       read_file,load_delta
  ```

4. For a Delta-only run, configure the Glue job with:
  ```text
  --datalake-formats        delta
  --enable-glue-datacatalog true
  ```
  `--conf` is a **single unique parameter** in Glue — all Spark conf values are
  packed into one space-separated string:
  ```text
  --conf  spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
          --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
          --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore
  ```
5. For an Iceberg-only run, configure the Glue job with:
  ```text
  --datalake-formats        iceberg
  --enable-glue-datacatalog true
  ```
  `--conf` value (single parameter key, all confs space-separated):
  ```text
  --conf  spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
          --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
          --conf spark.sql.catalog.glue_catalog.warehouse=s3://de-dev-0007/output/iceberg/
          --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
          --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
  ```
6. For a combined Delta + Iceberg run, configure the Glue job with:
  ```text
  --datalake-formats        delta,iceberg
  --enable-glue-datacatalog true
  ```
  `--conf` value (single parameter key, all confs space-separated):
  ```text
  --conf  spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
          --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
          --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore
          --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
          --conf spark.sql.catalog.glue_catalog.warehouse=s3://de-dev-0007/output/iceberg/
          --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
          --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
  ```
7. Run with `STAGE=read_file,load_delta` first.
8. Then set `STAGE=` (empty) to run all stages.

Lake Formation note:
For Lake Formation registered tables, the Glue job role needs `SELECT` to read
and `SUPER` to write. In Glue 4.0 Iceberg jobs, add the Lake Formation-specific
catalog configuration documented by AWS if your catalog is LF-managed.

## Local Polars setup (`sample_aws_local_polars.py`)

Run the Polars pipeline directly from your machine against real S3 / Glue
resources.  No Glue job required.

### Prerequisites

```bash
pip install "datacoolie[aws-polars]"
aws configure            # sets up ~/.aws/credentials (default profile)
# — or —
aws configure --profile my-sso-profile
```

### Run

```bash
# All stages, default profile
python sample_aws_local_polars.py

# Named profile
python sample_aws_local_polars.py --PROFILE my-sso-profile

# Specific stages
python sample_aws_local_polars.py --STAGE read_file,load_delta

# Override region / bucket
python sample_aws_local_polars.py --REGION ap-southeast-1 --BUCKET de-dev-0007
```

### Parameters

| Flag | Default | Description |
|---|---|---|
| `--REGION` | `ap-southeast-1` | AWS region |
| `--BUCKET` | `de-dev-0007` | S3 bucket name |
| `--STAGE` | *(all stages)* | Comma-separated stage names |
| `--PROFILE` | *(default profile)* | AWS CLI named profile |
| `--METADATA_PATH` | `s3://{BUCKET}/metadata/aws_glue_use_cases.json` | Full S3 path to metadata |
| `--LOG_BASE_PATH` | `s3://{BUCKET}/logs/datacoolie` | S3 base path for logs |
| `--ICEBERG_CATALOG` | `glue_catalog` | pyiceberg catalog alias |
| `--ICEBERG_WAREHOUSE` | `s3://{BUCKET}/output/iceberg/` | pyiceberg warehouse root |
| `--MAX_WORKERS` | `4` | Parallel dataflow worker count |

> **Consistent table paths with Spark**: pyiceberg derives table locations as
> `{ICEBERG_WAREHOUSE}/{database}.db/{table_name}` — the same Hive-style layout
> that Glue Spark uses when the warehouse root is set in the Spark catalog conf.
> Keep `ICEBERG_WAREHOUSE` and the Spark `warehouse` conf value in sync so both
> engines write to the same S3 paths.

### IAM permissions for the local user

The IAM user or role referenced by the active profile needs the same S3, Glue,
Secrets Manager, and (optionally) Athena permissions listed in the **IAM
permissions required** section above.  Replace the Glue log group resource with
an appropriate resource if you are not using CloudWatch.

## Secrets Manager integration

The `aws_secrets_example_source` connection in the metadata demonstrates
how to wire a Secrets Manager secret:

```json
"secrets_ref": {
  "arn:aws:secretsmanager:ap-southeast-1:123456789012:secret:de-dev-0007/datacoolie/rds": [
    "username",
    "password"
  ]
}
```

- The outer key is the **SecretId** (ARN or name).
- The inner list contains the JSON field names to extract from the secret value.
- `AWSPlatform._fetch_secret()` calls `secretsmanager:GetSecretValue` at runtime
  and resolves the listed fields into the connection `configure` dict.

To create the secret for testing:

```bash
aws secretsmanager create-secret \
  --name de-dev-0007/datacoolie/rds \
  --secret-string '{"username":"myuser","password":"mypassword"}' \
  --region ap-southeast-1
```

## Iceberg notes

**SparkEngine (Glue Spark job)**:
- Full read/write via Spark + Glue catalog. The `glue_catalog` catalog name maps
  to the AWS Glue Data Catalog when the Glue catalog integration is enabled.
- Tables are created under `datacoolie.<table_name>` in the Glue catalog.
- This is the recommended path for production Delta + Iceberg jobs, especially
  when Athena or Lake Formation integration matters.

**PolarsEngine (local runner)**:
- Delta uses S3 path-based reads and writes via `deltalake` / `object_store`.
- Iceberg uses `pyiceberg` with a Glue-backed catalog initialised from local
  AWS CLI credentials.
- When `pyiceberg` cannot be initialised (missing package or Glue permissions),
  file and Delta stages continue normally and only Iceberg stages fail.

## Quick start (Spark job, recommended)

1. Upload input data from `usecase-sim/data/input/` to S3 input paths.
2. Upload `aws_glue_use_cases.json` to `s3://de-dev-0007/metadata/`.
3. Create the Glue Spark job as described above.
4. Run with `STAGE=read_file,load_delta` to validate file reads and Delta writes.
5. Re-run with `STAGE=load_iceberg,read_lakehouse` to validate Glue-catalog Iceberg writes.
6. Check outputs in `s3://de-dev-0007/output/` and catalog tables in Glue/Athena.

## Quick start (Local Polars)

1. Install dependencies and configure credentials:
   ```bash
   pip install "datacoolie[aws-polars]"
   aws configure    # enter Access Key ID, Secret, Region, output format
   ```
2. Upload input data from `usecase-sim/data/input/` to the corresponding S3 input paths.
3. Upload `aws_glue_use_cases.json` to `s3://de-dev-0007/metadata/`.
4. Run a single stage to validate:
   ```bash
   python sample_aws_local_polars.py --STAGE read_file
   ```
5. Run all stages:
   ```bash
   python sample_aws_local_polars.py
   ```
6. Check S3 outputs and Glue catalog tables in `datacoolie` database.

Expected outcomes:

- CSV/JSON/Parquet/Avro/JSONL are read from `s3://de-dev-0007/input/` and written
  to `s3://de-dev-0007/output/`.
- Delta tables are written under `s3://de-dev-0007/output/delta/` and registered
  in the Glue catalog under database `datacoolie`.
- Iceberg tables are written under `s3://de-dev-0007/output/iceberg/` and
  registered in the Glue catalog.
