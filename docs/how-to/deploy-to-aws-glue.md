# Deploy to AWS Glue

**Prerequisites** · AWS account with Glue, S3, Secrets Manager · `datacoolie` installed in the Glue job's Python library path.
**End state** · DataCoolie pipeline running as a Glue Spark job writing Delta/Iceberg to S3 with Secrets Manager-backed credentials.

## 1. Package the library

Glue lets you provide a wheel via `--additional-python-modules`:

```
--additional-python-modules datacoolie==0.1.0
```

Then add only the extra Python packages your specific job needs, for example:

- `sqlalchemy` for database metadata.
- `httpx` for API metadata.
- `openpyxl` for Excel metadata files.
- `pyiceberg` for PyIceberg-based Iceberg workflows.

Glue already provides the Spark runtime, so treat DataCoolie extras as optional
convenience, not the default install path.

## 2. Job script

```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from datacoolie.engines.spark_engine import SparkEngine
from datacoolie.platforms.aws_platform import AWSPlatform
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver

spark = GlueContext(SparkContext.getOrCreate()).spark_session
platform = AWSPlatform(region="us-east-1")
engine = SparkEngine(spark, platform=platform)
metadata = FileProvider(config_path="s3://my-bucket/metadata/orders.json", platform=platform)

with DataCoolieDriver(engine=engine, metadata_provider=metadata,
                     base_log_path="s3://my-bucket/logs") as driver:
    driver.run(stage="ingest2bronze")
```

## 3. Delta + Athena handoff

`Connection.configure` options that matter on AWS:

| Option | Purpose |
|---|---|
| `athena_output_location` | S3 path for Athena DDL results. When set, DataCoolie registers a native Delta table via Athena `CREATE EXTERNAL TABLE ... TBLPROPERTIES('table_type'='DELTA')` after every write and maintenance. |
| `generate_manifest` | Write a `_symlink_format_manifest/` after writes (Presto/Athena-compatible). |
| `register_symlink_table` | Register a Glue `SymlinkTextInputFormat` table after writes. Implies `generate_manifest`. |
| `symlink_database_prefix` | Prefix for the symlink Glue database. Default `"symlink_"`. |

The checked-in `aws_glue_use_cases.json` uses this Delta path: Delta
destinations carry `athena_output_location`, and `AWSPlatform.register_delta_table(...)`
does the Glue Catalog registration through Athena after Spark writes.

## 4. Secrets

`AWSPlatform._fetch_secret` hits AWS Secrets Manager:

```json
{
    "configure": {
        "username": "db_user",
        "password": "db_pass"
    },
    "secrets_ref": {
        "arn:aws:secretsmanager:ap-southeast-1:123456789012:secret:de-dev-0001/datacoolie/rds": [
            "username",
            "password"
        ]
    }
}
```

`username` and `password` are the `configure` fields. Their current values
(`db_user`, `db_pass`) are the JSON keys fetched from the Secrets Manager
secret. This is the same pattern used by `aws_secrets_example_source` in the
usecase-sim AWS metadata.

If you prefer a shorter form in your own metadata, use the actual secret name
you created, such as `de-dev-0001/datacoolie/rds`. A bare value like
`datacoolie/rds` only works when that is literally the secret's name. Use the
full ARN when you want an unambiguous reference across environments or accounts.

## 5. Delta Lake config on Glue

Glue Spark needs Delta Lake extensions activated at session start. The
repo-backed `sample_aws_glue_spark.py` uses explicit Spark properties, so that
is the documented baseline:

```
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

Glue also offers job arguments such as `--datalake-formats delta`, but this
repo does not rely on them anywhere. Prefer the explicit Spark properties plus
the matching Delta runtime/JARs, because that is the path exercised by the
checked-in Glue sample.

## 6. Iceberg alternative

If you would rather use Iceberg than Delta on Glue, switch the connection to
`fmt="iceberg"`, set `catalog="glue_catalog"`, and keep a real Glue database
name (the AWS sample metadata uses `database="datacoolie"`). This skips the
Athena-native Delta registration path entirely and uses Glue Catalog-backed
Iceberg tables instead.

For Spark jobs, include the Glue Iceberg runtime/JARs. For Python Shell jobs,
the checked-in `sample_aws_glue_polars.py` uses `pyiceberg`, and Glue catalog
support requires `PYICEBERG_CATALOG__GLUE__TYPE=glue` plus the appropriate Glue
permissions.

## Reference assets

Use the AWS platform guide in usecase-sim for the current sample scripts,
metadata file, and setup notes:

- [`README.md`](https://github.com/datacoolie/datacoolie/blob/main/datacoolie/usecase-sim/platforms/aws/README.md)
