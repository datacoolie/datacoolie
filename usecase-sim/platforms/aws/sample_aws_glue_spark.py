"""
sample_aws_glue_spark.py — DataCoolie usecase-sim runner for AWS Glue (PySpark)

This script is designed to run as an AWS Glue ETL job using the PySpark engine.
It loads the shared metadata from S3, initialises AWSPlatform + SparkEngine,
and runs all dataflows matching the selected stage(s).

Glue job type: Spark (Glue 4.0+, Python 3)

Required Glue job parameters (--conf / job arguments):
  --REGION       AWS region, e.g. ap-southeast-1
  --BUCKET       S3 bucket name, e.g. de-dev-0001
  --STAGE        Comma-separated stage names to run, e.g. "read_file,load_delta"
                 Leave empty ("") to run ALL stages in the metadata file.

Required Glue Spark configuration (Job details → Spark properties):
  spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
  spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

Required Glue job libraries:
  - delta-core / delta-spark JAR matching your Spark version (via --extra-jars or
    Glue Data Catalog continuous integration)
  - datacoolie[aws] wheel uploaded to S3 and added via --additional-python-modules
    or an S3 path reference in the Glue job definition.

IAM permissions required for the Glue execution role:
  - s3:GetObject / s3:PutObject / s3:DeleteObject on s3://de-dev-0001/*
  - s3:ListBucket on s3://de-dev-0001
  - secretsmanager:GetSecretValue for any secrets used in connections
  - glue:CreateTable / glue:UpdateTable / glue:GetDatabase on the datacoolie database
  - athena:StartQueryExecution / athena:GetQueryExecution on the Athena workgroup
"""

import sys
import logging

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

from datacoolie.platforms.aws_platform import AWSPlatform
from datacoolie.engines.spark_engine import SparkEngine
from datacoolie.metadata import FileProvider
from datacoolie.orchestration import DataCoolieDriver
from datacoolie.core import DataCoolieRunConfig

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Glue job parameters
# ---------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "REGION", "BUCKET", "STAGE"],
)

REGION: str = args.get("REGION", "ap-southeast-1")
BUCKET: str = args.get("BUCKET", "de-dev-0001")
STAGE: str = args.get("STAGE", "")  # empty string = run all stages

METADATA_PATH = f"s3://{BUCKET}/metadata/aws_glue_use_cases.json"
LOG_BASE_PATH = f"s3://{BUCKET}/logs/datacoolie"

# ---------------------------------------------------------------------------
# Spark / Glue context
# ---------------------------------------------------------------------------
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

logger.info("Spark version: %s", spark.version)
logger.info("Running stage(s): '%s'", STAGE or "<all>")
logger.info("Metadata: %s", METADATA_PATH)

# ---------------------------------------------------------------------------
# DataCoolie setup
# ---------------------------------------------------------------------------
platform = AWSPlatform(region=REGION)
engine = SparkEngine(spark, platform=platform)

metadata = FileProvider(METADATA_PATH, platform=platform)

config = DataCoolieRunConfig(max_workers=8)

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
with DataCoolieDriver(
    engine=engine,
    metadata_provider=metadata,
    config=config,
    base_log_path=LOG_BASE_PATH,
) as driver:
    result = driver.run(stage=STAGE if STAGE else None)

logger.info("DataCoolie run complete. result=%s", result)
