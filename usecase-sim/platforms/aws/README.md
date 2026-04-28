# AWS Glue Assets for usecase-sim

Prepared AWS Glue assets for file + Delta + Iceberg scenarios using real S3 and
AWS Secrets Manager. Targets the `s3://de-dev-0001/` bucket.

Two execution modes are provided:

| Script | Glue job type | Engine | Formats |
|---|---|---|---|
| `sample_aws_glue_spark.py` | Spark (PySpark) | `SparkEngine` | File, Delta, Iceberg |
| `sample_aws_glue_polars.py` | Python Shell | `PolarsEngine` | File, Delta (path-only), Iceberg (pyiceberg) |

## What is in this folder

- `aws_glue_use_cases.json`:
  - Derived from the canonical `metadata/file/aws_use_cases.json` structure.
  - Paths rewritten to `s3://de-dev-0001/`.
  - Stages use standard names (`read_file`, `write_file`, `load_delta`,
    `load_iceberg`, `read_lakehouse`, …) without platform prefixes.
  - Delta destination includes `database` and `configure.athena_output_location`
    to trigger Glue catalog registration after Spark writes.
  - Iceberg connections include `catalog: "glue_catalog"` and `database: "datacoolie"`.
  - One `aws_secrets_example_source` connection demonstrates Secrets Manager
    `secrets_ref` with a Secrets Manager ARN as the outer key.
- `sample_aws_glue_spark.py`:
  - Glue Spark (PySpark) job script.
  - Uses `SparkEngine` with `max_workers=8`.
  - Full Delta + Iceberg + Athena/Glue catalog registration.
- `sample_aws_glue_polars.py`:
  - Glue Python Shell job script.
  - Uses `PolarsEngine` with `max_workers=4`.
  - Delta via path-only (`deltalake` / `object_store`).
  - Iceberg via pyiceberg Glue catalog (see notes below).
- `README.md`: this file.
- `DEFERRED.md`: known gaps and deferred work.

## S3 path convention

Root bucket: `s3://de-dev-0001/`

| Purpose | S3 path |
|---|---|
| Input files | `s3://de-dev-0001/input/{format}/` |
| File outputs | `s3://de-dev-0001/output/{format}/` |
| Delta tables | `s3://de-dev-0001/output/delta/` |
| Iceberg tables | `s3://de-dev-0001/output/iceberg/` |
| Metadata file | `s3://de-dev-0001/metadata/aws_glue_use_cases.json` |
| Athena results | `s3://de-dev-0001/athena-results/` |
| Logs | `s3://de-dev-0001/logs/datacoolie` |

## Prerequisites

1. AWS account with IAM role for Glue execution.
2. S3 bucket `de-dev-0001` created in the target region (default: `ap-southeast-1`).
3. AWS Glue Data Catalog database `datacoolie` created:
   ```bash
   aws glue create-database --database-input '{"Name":"datacoolie"}' --region ap-southeast-1
   ```
4. (Spark only) Delta Lake JAR available to the Glue job.
   Use Glue 4.0+ and add the Delta connector from the Glue marketplace, or supply
   the JAR via `--extra-jars`.
5. `datacoolie` wheel uploaded to S3 and referenced via
   `--additional-python-modules` or a Glue Python environment.
6. Input files uploaded from the local repo folder `usecase-sim/data/input/`
   to the corresponding S3 paths.
7. Metadata file `aws_glue_use_cases.json` uploaded to
   `s3://de-dev-0001/metadata/aws_glue_use_cases.json`.

### IAM permissions required

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::de-dev-0001/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::de-dev-0001"
    },
    {
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "arn:aws:secretsmanager:ap-southeast-1:*:secret:de-dev-0001/*"
    },
    {
      "Effect": "Allow",
      "Action": ["glue:CreateTable", "glue:UpdateTable", "glue:GetDatabase",
                 "glue:GetTable", "glue:GetTables"],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["athena:StartQueryExecution", "athena:GetQueryExecution",
                 "athena:GetQueryResults"],
      "Resource": "*"
    }
  ]
}
```

## Spark Glue job setup

1. Create a Glue ETL job (type: Spark, Glue 4.0+).
2. Set script path to `sample_aws_glue_spark.py` (upload to S3 first).
3. Add Spark configuration in **Job details → Advanced properties → Spark properties**:
   ```
   --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
   --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
   ```
4. Add job parameters:
   ```
   --REGION      ap-southeast-1
   --BUCKET      de-dev-0001
   --STAGE       read_file,load_delta
   ```
5. Run with `STAGE=read_file,load_delta` first.
6. Then set `STAGE=` (empty) to run all stages.

## Python Shell Glue job setup (Polars)

1. Create a Glue job (type: Python Shell, Python 3.9+).
2. Set script path to `sample_aws_glue_polars.py` (upload to S3 first).
3. Add job parameters:
   ```
   --REGION      ap-southeast-1
   --BUCKET      de-dev-0001
   --STAGE       read_file,load_delta
   ```
4. Ensure `polars`, `deltalake`, and `pyiceberg[glue]` are available.

## Secrets Manager integration

The `aws_secrets_example_source` connection in the metadata demonstrates
how to wire a Secrets Manager secret:

```json
"secrets_ref": {
  "arn:aws:secretsmanager:ap-southeast-1:123456789012:secret:de-dev-0001/datacoolie/rds": [
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
  --name de-dev-0001/datacoolie/rds \
  --secret-string '{"username":"myuser","password":"mypassword"}' \
  --region ap-southeast-1
```

## Iceberg notes

**SparkEngine (Glue Spark job)**:
- Full read/write via Spark + Glue catalog. The `glue_catalog` catalog name maps
  to the AWS Glue Data Catalog when the Glue catalog integration is enabled.
- Tables are created under `datacoolie.<table_name>` in the Glue catalog.

**PolarsEngine (Glue Python Shell job)**:
- Iceberg is supported via `pyiceberg` with the Glue catalog backend.
- Set the environment variable `PYICEBERG_CATALOG__GLUE__TYPE=glue` in the
  Glue job environment or configure via `~/.pyiceberg.yaml`.
- See `DEFERRED.md` for current limitations.

## Quick start (Spark job, recommended)

1. Upload input data from `usecase-sim/data/input/` to S3 input paths.
2. Upload `aws_glue_use_cases.json` to `s3://de-dev-0001/metadata/`.
3. Create the Glue Spark job as described above.
4. Run with `STAGE=read_file,load_delta` to validate file reads and Delta writes.
5. Check outputs in `s3://de-dev-0001/output/` and Delta tables in the Glue catalog.

Expected outcomes:

- CSV/JSON/Parquet/Avro/JSONL are read from `s3://de-dev-0001/input/` and written
  to `s3://de-dev-0001/output/`.
- Delta tables are written under `s3://de-dev-0001/output/delta/` and registered
  in the Glue catalog under database `datacoolie`.
- Iceberg tables are written under `s3://de-dev-0001/output/iceberg/` and
  registered in the Glue catalog.
