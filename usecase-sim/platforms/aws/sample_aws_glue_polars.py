"""
sample_aws_glue_polars.py — DataCoolie usecase-sim runner for AWS Glue (Python Shell / Polars)

This script is designed to run as an AWS Glue Python Shell job.
It loads the shared metadata from S3, initialises AWSPlatform + PolarsEngine,
and runs all dataflows matching the selected stage(s).

Glue job type: Python Shell (Python 3.9+)

Required Glue job parameters (job arguments):
  --REGION       AWS region, e.g. ap-southeast-1
  --BUCKET       S3 bucket name, e.g. de-dev-0001
  --STAGE        Comma-separated stage names to run, e.g. "read_file,load_delta"
                 Leave empty ("") to run ALL stages in the metadata file.

Required Glue job libraries:
  - datacoolie[aws] wheel uploaded to S3 and referenced via --additional-python-modules
    or pre-installed in a Glue Python environment.
  - polars, deltalake must be installed (included in datacoolie[aws] extras or via
    --additional-python-modules).

IAM permissions required for the Glue execution role:
  - s3:GetObject / s3:PutObject / s3:DeleteObject on s3://de-dev-0001/*
  - s3:ListBucket on s3://de-dev-0001
  - secretsmanager:GetSecretValue for any secrets used in connections

Notes on Iceberg with PolarsEngine:
  - PolarsEngine reads/writes Iceberg via pyiceberg with a REST or Glue catalog.
  - For Glue catalog support, set PYICEBERG_CATALOG__GLUE__TYPE=glue in the
    Glue job environment variables and ensure the role has Glue catalog permissions.
  - See DEFERRED.md for known limitations.
"""

import sys
import logging

import boto3

from datacoolie.platforms.aws_platform import AWSPlatform
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata import FileProvider
from datacoolie.orchestration import DataCoolieDriver
from datacoolie.core import DataCoolieRunConfig

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Job parameters — read from sys.argv for Glue Python Shell compatibility
# ---------------------------------------------------------------------------
def _get_arg(name: str, default: str = "") -> str:
    """Read --NAME VALUE pairs from sys.argv (Glue Python Shell style)."""
    try:
        idx = sys.argv.index(f"--{name}")
        return sys.argv[idx + 1]
    except (ValueError, IndexError):
        return default


REGION: str = _get_arg("REGION", "ap-southeast-1")
BUCKET: str = _get_arg("BUCKET", "de-dev-0001")
STAGE: str = _get_arg("STAGE", "")  # empty string = run all stages

METADATA_PATH = f"s3://{BUCKET}/metadata/aws_glue_use_cases.json"
LOG_BASE_PATH = f"s3://{BUCKET}/logs/datacoolie"

logger.info("Running stage(s): '%s'", STAGE or "<all>")
logger.info("Metadata: %s", METADATA_PATH)

# ---------------------------------------------------------------------------
# Derive S3 storage_options from the active boto3 session so that Polars
# (via deltalake / object_store) can authenticate to S3 without additional
# credential configuration.  On AWS Glue the session credentials come from
# the job execution role via the instance metadata service.
# ---------------------------------------------------------------------------
session = boto3.Session(region_name=REGION)
creds = session.get_credentials().get_frozen_credentials()

storage_options: dict = {
    "aws_access_key_id": creds.access_key,
    "aws_secret_access_key": creds.secret_key,
    "aws_region": REGION,
}
# Session token is only present for temporary (STS) credentials, e.g. IAM roles
if creds.token:
    storage_options["aws_session_token"] = creds.token

# ---------------------------------------------------------------------------
# DataCoolie setup
# ---------------------------------------------------------------------------
platform = AWSPlatform(region=REGION)
engine = PolarsEngine(platform=platform, storage_options=storage_options)

metadata = FileProvider(METADATA_PATH, platform=platform)

config = DataCoolieRunConfig(max_workers=4)

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
