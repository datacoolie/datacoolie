"""
sample_aws_glue_docker.py — DataCoolie usecase-sim runner for AWS Glue Docker (hybrid mode)

Compute: AWS Glue Docker container on-prem (public.ecr.aws/glue/aws-glue-libs:5)
Data:    Real AWS cloud services — S3, Glue Data Catalog, Secrets Manager, Athena

This script mirrors sample_aws_glue_spark.py as closely as possible.
The only structural differences from the cloud Glue script are:

  1. sys.path — injects src/ so that "import datacoolie" resolves from the
     volume-mounted source tree (mirrors --additional-python-modules in cloud Glue).
     functions/ is NOT added here; it is passed via --py-files to spark-submit
     (mirrors --extra-py-files in cloud Glue).
  2. No Job.init() / bookmark calls — not supported in local/Docker mode.

Arg parsing is identical to the cloud script: getResolvedOptions reads all four
required parameters (JOB_NAME, REGION, BUCKET, STAGE) from sys.argv.

Usage: see README.md — "Running with Glue Docker (hybrid mode)"
Launch via: python usecase-sim/platforms/aws/run_glue_docker.py --stage read_file
"""

from __future__ import annotations

import logging
import os
import sys

# ---------------------------------------------------------------------------
# sys.path — src/ only (dev mode / sys.path mode)
# Skipped when DATACOOLIE_PIP_MODE=1 (datacoolie installed via pip by run_glue_docker.py
# --install, which mirrors --additional-python-modules in cloud Glue).
# Adding via sys.path (not -e PYTHONPATH) preserves the Glue image's built-in
# PYTHONPATH that makes awsglue importable.
# ---------------------------------------------------------------------------
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if not os.environ.get("DATACOOLIE_PIP_MODE"):
    _SRC_DIR = os.path.normpath(os.path.join(_THIS_DIR, "../../../src"))
    if _SRC_DIR not in sys.path:
        sys.path.insert(0, _SRC_DIR)

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from datacoolie.core import DataCoolieRunConfig
from datacoolie.engines.spark_engine import SparkEngine
from datacoolie.metadata import FileProvider
from datacoolie.orchestration import DataCoolieDriver
from datacoolie.platforms.aws_platform import AWSPlatform

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Glue job parameters — identical to cloud script
# ---------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "REGION", "BUCKET", "STAGE"],
)

REGION: str = args.get("REGION", "ap-southeast-1")
BUCKET: str = args.get("BUCKET", "de-dev-0007")
STAGE: str = args.get("STAGE", "")  # empty string = run all stages

METADATA_PATH = f"s3://{BUCKET}/metadata/aws_glue_use_cases.json"
LOG_BASE_PATH = f"s3://{BUCKET}/logs/datacoolie"

# ---------------------------------------------------------------------------
# Spark / Glue context
# SparkContext is created by spark-submit; GlueContext wraps it so that
# DynamicFrame / awsglue-dependent code paths are available identically to
# a cloud Glue job.  The Spark conf (Delta + Iceberg catalogs) is supplied
# via --conf flags to spark-submit (see Usage above).
# ---------------------------------------------------------------------------
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

logger.info("Spark version: %s", spark.version)
logger.info("Running stage(s): '%s'", STAGE or "<all>")
logger.info("Metadata: %s", METADATA_PATH)

# ---------------------------------------------------------------------------
# DataCoolie setup — identical to cloud path
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
